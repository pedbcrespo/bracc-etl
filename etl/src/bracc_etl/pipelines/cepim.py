from __future__ import annotations

import hashlib
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
    normalize_name,
    strip_document,
)


def _generate_ngo_id(cnpj_digits: str, agreement_number: str) -> str:
    """Deterministic ID from CNPJ digits + agreement number."""
    raw = f"{cnpj_digits}:{agreement_number}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class CepimPipeline(Pipeline):
    """ETL pipeline for CEPIM (Cadastro de Entidades Privadas sem Fins Lucrativos Impedidas)."""

    name = "cepim"
    source_id = "cepim"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> pd.DataFrame:
        cepim_dir = Path(self.data_dir) / "cepim"
        return pd.read_csv(
            cepim_dir / "cepim.csv",
            sep=";",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
        )

    def transform(self, data: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        ngos: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []

        for _idx, row in data.iterrows():
            cnpj_raw = str(row.get("CNPJ ENTIDADE", ""))
            digits = strip_document(cnpj_raw)

            if len(digits) != 14:
                continue

            cnpj_formatted = format_cnpj(cnpj_raw)
            name = normalize_name(str(row.get("NOME ENTIDADE", "")))
            agreement_number = str(row.get("NÚMERO CONVÊNIO", "")).strip()
            agency = str(row.get("ÓRGÃO CONCEDENTE", "")).strip()
            reason = str(
                row.get("MOTIVO IMPEDIMENTO", row.get("MOTIVO DO IMPEDIMENTO", ""))
            ).strip()

            ngo_id = _generate_ngo_id(digits, agreement_number)

            ngos.append({
                "ngo_id": ngo_id,
                "cnpj": cnpj_formatted,
                "name": name,
                "reason": reason,
                "agreement_number": agreement_number,
                "agency": agency,
                "source": "cepim",
            })

            company_rels.append({
                "source_key": cnpj_formatted,
                "target_key": ngo_id,
            })

        return {
            "ngos": deduplicate_rows(ngos, ["ngo_id"]),
            "company_rels": company_rels
        }

    def load(self, transformed_data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if transformed_data["ngos"]:
            loader.load_nodes("BarredNGO", transformed_data["ngos"], key_field="ngo_id")

        # Ensure Company nodes exist for CNPJ linking
        if transformed_data["company_rels"]:
            companies = [
                {"cnpj": rel["source_key"]} for rel in transformed_data["company_rels"]
            ]
            loader.load_nodes("Company", deduplicate_rows(companies, ["cnpj"]), key_field="cnpj")

        if transformed_data["company_rels"]:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (b:BarredNGO {ngo_id: row.target_key}) "
                "MERGE (c)-[:IMPEDIDA]->(b)"
            )
            loader.run_query_with_retry(query, transformed_data["company_rels"])
