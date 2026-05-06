from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import format_cnpj, normalize_name, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# TP_DEPENDENCIA codes
ADMIN_TYPE = {
    "1": "federal",
    "2": "estadual",
    "3": "municipal",
    "4": "privada",
}

# TP_SITUACAO_FUNCIONAMENTO codes
STATUS_MAP = {
    "1": "em_atividade",
    "2": "paralisada",
    "3": "extinta",
}


class InepPipeline(Pipeline):
    """ETL pipeline for INEP Censo Escolar (school census) data."""

    name = "inep"
    source_id = "inep_censo_escolar"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> list[dict[str, str]]:
        inep_dir = Path(self.data_dir) / "inep"
        csv_path = inep_dir / "microdados_ed_basica_2022.csv"
        raw_rows: list[dict[str, str]] = []

        if not csv_path.exists():
            msg = f"INEP CSV not found at {csv_path}"
            raise FileNotFoundError(msg)

        logger.info("[inep] Reading %s ...", csv_path)

        with open(csv_path, encoding="latin-1", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for i, row in enumerate(reader):
                raw_rows.append(row)
                if self.limit and i + 1 >= self.limit:
                    break

        logger.info("[inep] Extracted %d rows", len(raw_rows))
        return raw_rows

    def _parse_int(self, value: str) -> int:
        """Parse an integer string, returning 0 for empty/invalid."""
        value = value.strip()
        if not value:
            return 0
        try:
            return int(value)
        except ValueError:
            return 0

    def transform(self, data: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
        schools: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []

        for row in data:
            school_id = row.get("CO_ENTIDADE", "").strip()
            if not school_id:
                continue

            name = normalize_name(row.get("NO_ENTIDADE", ""))
            municipality_code = row.get("CO_MUNICIPIO", "").strip()
            municipality_name = row.get("NO_MUNICIPIO", "").strip()
            uf = row.get("SG_UF", "").strip()
            admin_type_code = row.get("TP_DEPENDENCIA", "").strip()
            status_code = row.get("TP_SITUACAO_FUNCIONAMENTO", "").strip()

            enrollment = self._parse_int(row.get("QT_MAT_BAS", ""))
            staff = self._parse_int(row.get("QT_FUNCIONARIOS", ""))

            schools.append({
                "school_id": school_id,
                "name": name,
                "municipality_code": municipality_code,
                "municipality_name": municipality_name,
                "uf": uf,
                "admin_type": ADMIN_TYPE.get(admin_type_code, admin_type_code),
                "status": STATUS_MAP.get(status_code, status_code),
                "enrollment": enrollment,
                "staff": staff,
                "year": 2022,
                "source": "inep_censo_escolar",
            })

            # Link private schools to Company via CNPJ
            cnpj_raw = row.get("NU_CNPJ_ESCOLA_PRIVADA", "").strip()
            if cnpj_raw:
                digits = strip_document(cnpj_raw)
                if len(digits) == 14:
                    cnpj_formatted = format_cnpj(cnpj_raw)
                    links.append({
                        "source_key": cnpj_formatted,
                        "target_key": school_id,
                    })

            # Also link maintainer CNPJ if different
            cnpj_mant_raw = row.get("NU_CNPJ_MANTENEDORA", "").strip()
            if cnpj_mant_raw and cnpj_mant_raw != cnpj_raw:
                digits_mant = strip_document(cnpj_mant_raw)
                if len(digits_mant) == 14:
                    cnpj_mant_formatted = format_cnpj(cnpj_mant_raw)
                    links.append({
                        "source_key": cnpj_mant_formatted,
                        "target_key": school_id,
                    })

        schools = schools
        school_company_links = links
        logger.info(
            "[inep] Transformed %d schools, %d company links",
            len(schools),
            len(school_company_links),
        )
        return {
            "schools": schools,
            "school_company_links": school_company_links,
        }

    def load(self, data: dict[str, list[dict[str, str]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data["schools"]:
            loader.load_nodes("Education", data["schools"], key_field="school_id")
            logger.info("[inep] Loaded %d Education nodes", len(data["schools"]))

        if data["school_company_links"]:
            query = (
                "UNWIND $rows AS row "
                "MATCH (e:Education {school_id: row.target_key}) "
                "MERGE (c:Company {cnpj: row.source_key}) "
                "MERGE (c)-[:MANTEDORA_DE]->(e)"
            )
            loader.run_query(query, data["school_company_links"])
            logger.info(
                "[inep] Created %d MANTEDORA_DE relationships",
                len(data["school_company_links"]),
            )
