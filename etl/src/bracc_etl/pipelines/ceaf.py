from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)


class CeafPipeline(Pipeline):
    """ETL pipeline for CEAF (Cadastro de Expulsoes da Administracao Federal)."""

    name = "ceaf"
    source_id = "ceaf"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> pd.DataFrame:
        ceaf_dir = Path(self.data_dir) / "ceaf"
        return pd.read_csv(
            ceaf_dir / "ceaf.csv",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
            chunksize=self.chunk_size,
        )

    def transform(self, data) -> dict[str, list[dict[str, Any]]]:
        expulsions: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []

        for idx, row in data.iterrows():
            cpf_raw = str(row.get("cpf", ""))
            digits = strip_document(cpf_raw)

            nome = normalize_name(str(row.get("nome", "")))
            if not nome:
                continue

            position = str(row.get("cargo_efetivo", "")).strip()
            punishment_type = str(row.get("tipo_punicao", "")).strip()
            date = parse_date(str(row.get("data_publicacao", "")))
            decree = str(row.get("portaria", "")).strip()
            uf = str(row.get("uf", "")).strip()

            # Use full CPF when available, otherwise use partial + index
            if len(digits) == 11:
                cpf_formatted = format_cpf(cpf_raw)
                expulsion_id = f"ceaf_{digits}_{idx}"
            else:
                cpf_formatted = cpf_raw.strip()  # Keep masked format
                expulsion_id = f"ceaf_{digits}_{idx}"

            expulsions.append({
                "expulsion_id": expulsion_id,
                "cpf": cpf_formatted,
                "name": nome,
                "position": position,
                "punishment_type": punishment_type,
                "date": date,
                "decree": decree,
                "uf": uf,
                "source": "ceaf",
            })

            # Only create person relationships for full CPFs
            if len(digits) == 11:
                person_rels.append({
                    "source_key": cpf_formatted,
                    "target_key": expulsion_id,
                    "person_name": nome,
                })

        return {
            "expulsions": deduplicate_rows(expulsions, ["expulsion_id"]),
            "person_rels": person_rels
        }

    def load(self, data) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data["expulsions"]:
            loader.load_nodes("Expulsion", data["expulsions"], key_field="expulsion_id")

        # Ensure Person nodes exist
        for rel in data["person_rels"]:
            loader.load_nodes(
                "Person",
                [{"cpf": rel["source_key"], "name": rel["person_name"]}],
                key_field="cpf",
            )

        if data["person_rels"]:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (e:Expulsion {expulsion_id: row.target_key}) "
                "MERGE (p)-[:EXPULSO]->(e)"
            )
            loader.run_query_with_retry(query, data["person_rels"])
