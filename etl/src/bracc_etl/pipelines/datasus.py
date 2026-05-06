from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    format_cnpj,
    normalize_name,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver


class DatasusPipeline(Pipeline):
    """ETL pipeline for CNES health facility data from DATASUS."""

    name = "datasus"
    source_id = "cnes"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.facilities: list[dict[str, Any]] = []
        self.company_links: list[dict[str, Any]] = []

    def extract(self) -> pd.DataFrame:
        datasus_dir = Path(self.data_dir) / "datasus"
        csv_path = datasus_dir / "cnes_all.csv"
        if not csv_path.exists():
            msg = f"CNES data not found at {csv_path}. Run scripts/download_datasus.py first."
            raise FileNotFoundError(msg)

        raw = pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,
        )
        if self.limit:
            raw = raw.head(self.limit)
        return raw

    def transform(self, data: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        facilities: list[dict[str, Any]] = []
        company_links: list[dict[str, Any]] = []

        for _, row in data.iterrows():
            cnes_code = str(row.get("codigo_cnes", "")).strip()
            if not cnes_code:
                continue

            # Get CNPJ — prefer numero_cnpj_entidade, fall back to numero_cnpj
            cnpj_raw = str(row.get("numero_cnpj_entidade", "")).strip()
            if not cnpj_raw:
                cnpj_raw = str(row.get("numero_cnpj", "")).strip()

            cnpj_digits = strip_document(cnpj_raw)
            cnpj_formatted = format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""

            razao = normalize_name(str(row.get("nome_razao_social", "")))
            fantasia = normalize_name(str(row.get("nome_fantasia", "")))
            facility_name = fantasia or razao

            tipo_unidade = str(row.get("codigo_tipo_unidade", "")).strip()
            esfera = str(row.get("descricao_esfera_administrativa", "")).strip()
            municipio = str(row.get("codigo_municipio", "")).strip()
            uf = str(row.get("codigo_uf", "")).strip()
            sus_key = "estabelecimento_faz_atendimento_ambulatorial_sus"
            atende_sus_raw = str(row.get(sus_key, "")).strip().upper()
            atende_sus = "1" if atende_sus_raw in ("1", "SIM", "S") else "0"
            hospitalar = str(row.get("estabelecimento_possui_atendimento_hospitalar", "")).strip()
            nat_juridica = str(row.get("descricao_natureza_juridica_estabelecimento", "")).strip()

            facility = {
                "cnes_code": cnes_code,
                "name": facility_name,
                "razao_social": razao,
                "tipo_unidade": tipo_unidade,
                "esfera": esfera,
                "municipio": municipio,
                "uf": uf,
                "atende_sus": atende_sus,
                "hospitalar": hospitalar,
                "natureza_juridica": nat_juridica,
                "source": "cnes",
            }
            facilities.append(facility)

            # Link to Company if CNPJ is valid
            if cnpj_formatted:
                company_links.append({
                    "source_key": cnpj_formatted,
                    "target_key": cnes_code,
                    "razao_social": razao,
                })

        return {
            "facilities": facilities,
            "company_links": company_links
        }

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # Create Health facility nodes
        if data["facilities"]:
            loader.load_nodes("Health", data["facilities"], key_field="cnes_code")

        # Ensure Company nodes exist and create relationships
        if data["company_links"]:
            # MERGE Company nodes (most should already exist from CNPJ pipeline)
            company_rows = [
                {"cnpj": link["source_key"], "razao_social": link["razao_social"]}
                for link in data["company_links"]
            ]
            loader.load_nodes("Company", company_rows, key_field="cnpj")

            # Create OPERA_UNIDADE relationships
            loader.load_relationships(
                rel_type="OPERA_UNIDADE",
                rows=data["company_links"],
                source_label="Company",
                source_key="cnpj",
                target_label="Health",
                target_key="cnes_code",
            )
