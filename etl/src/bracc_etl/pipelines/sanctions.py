from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)


class SanctionsPipeline(Pipeline):
    """ETL pipeline for CEIS/CNEP sanctions data."""

    name = "sanctions"
    source_id = "ceis_cnep"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> dict[str, pd.DataFrame]:
        sanctions_dir = Path(self.data_dir) / "sanctions"
        raw_ceis: pd.DataFrame = pd.DataFrame()
        raw_cnep: pd.DataFrame = pd.DataFrame()
        if not sanctions_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, sanctions_dir)
            return {"raw_ceis": pd.DataFrame(), "raw_cnep": pd.DataFrame()}
        ceis_path = sanctions_dir / "ceis.csv"
        cnep_path = sanctions_dir / "cnep.csv"
        if not ceis_path.exists() or not cnep_path.exists():
            logger.warning("[%s] Required CSV files not found in %s", self.name, sanctions_dir)
            return {"raw_ceis": pd.DataFrame(), "raw_cnep": pd.DataFrame()}
        raw_ceis = pd.read_csv(
            ceis_path, dtype=str, encoding="latin-1", keep_default_na=False, chunksize=self.chunk_size
        )
        raw_cnep = pd.read_csv(
            cnep_path, dtype=str, encoding="latin-1", keep_default_na=False, chunksize=self.chunk_size
        )
        return {"raw_ceis": raw_ceis, "raw_cnep": raw_cnep}

    def _process_rows(self, df: pd.DataFrame, sanction_type: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        sanctions: list[dict[str, Any]] = []
        entities: list[dict[str, Any]] = []

        for idx, row in df.iterrows():
            doc_raw = str(row["cpf_cnpj"])
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["nome"]))
            is_company = len(digits) == 14

            if is_company:
                doc_formatted = format_cnpj(doc_raw)
            elif len(digits) == 11:
                doc_formatted = format_cpf(doc_raw)
            else:
                doc_formatted = digits

            sanction_id = f"{sanction_type}_{digits}_{idx}"
            date_start = parse_date(str(row["data_inicio"]))
            date_end_raw = parse_date(str(row["data_fim"]))
            date_end = date_end_raw if date_end_raw else None

            sanctions.append({
                "sanction_id": sanction_id,
                "type": sanction_type,
                "date_start": date_start,
                "date_end": date_end,
                "reason": str(row["motivo"]).strip(),
                "source": sanction_type,
            })

            entity_label = "Company" if is_company else "Person"
            entity_key_field = "cnpj" if is_company else "cpf"

            entities.append({
                "source_key": doc_formatted,
                "target_key": sanction_id,
                "entity_label": entity_label,
                "entity_key_field": entity_key_field,
                "entity_name": nome,
                "entity_doc": doc_formatted,
            })

        return sanctions, entities

    def transform(self, data: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
        raw_ceis = data["raw_ceis"]
        raw_cnep = data["raw_cnep"]

        ceis_sanctions, ceis_entities = self._process_rows(raw_ceis, "CEIS")
        cnep_sanctions, cnep_entities = self._process_rows(raw_cnep, "CNEP")

        all_sanctions = ceis_sanctions + cnep_sanctions
        all_entities = ceis_entities + cnep_entities

        return {
            "sanctions": deduplicate_rows(all_sanctions, ["sanction_id"]), 
            "entities": all_entities
        }

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data["sanctions"]:
            loader.load_nodes("Sanction", data["sanctions"], key_field="sanction_id")

        for ent in data["entities"]:
            label = ent["entity_label"]
            key_field = ent["entity_key_field"]
            doc = ent["entity_doc"]
            name = ent["entity_name"]

            node_row: dict[str, Any] = {key_field: doc, "name": name}
            if label == "Company":
                node_row["razao_social"] = name
            loader.load_nodes(label, [node_row], key_field=key_field)

        if data["sanctions"]:
            rel_rows = [
                {"source_key": e["source_key"], "target_key": e["target_key"]}
                for e in data["sanctions"]
            ]

            query = (
                "UNWIND $rows AS row "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "OPTIONAL MATCH (c:Company {cnpj: row.source_key}) "
                "OPTIONAL MATCH (p:Person {cpf: row.source_key}) "
                "WITH s, coalesce(c, p) AS entity "
                "WHERE entity IS NOT NULL "
                "MERGE (entity)-[:SANCIONADA]->(s)"
            )
            loader.run_query(query, rel_rows)
