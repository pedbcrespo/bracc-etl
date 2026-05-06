from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver

from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import deduplicate_rows, normalize_name

logger = logging.getLogger(__name__)


def _generate_case_id(
    case_class: str, case_number: str, year: str,
) -> str:
    """Deterministic ID from case class + number + year."""
    raw = f"stj:{case_class}:{case_number}:{year}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class StjPipeline(Pipeline):
    """ETL pipeline for STJ (Superior Tribunal de Justiça) decisions.

    Data source: dadosabertos.stj.jus.br — CSV export of
    Superior Court decisions and proceedings.
    """

    name = "stj_dados_abertos"
    source_id = "stj_dados_abertos"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> pd.DataFrame:
        src_dir = Path(self.data_dir) / "stj_dados_abertos"
        csv_path = src_dir / "decisoes.csv"
        raw: pd.DataFrame = pd.DataFrame()
        if not csv_path.exists():
            msg = f"STJ CSV not found: {csv_path}"
            raise FileNotFoundError(msg)

        raw = pd.read_csv(csv_path, dtype=str, keep_default_na=False, chunksize=self.chunk_size)
        logger.info(
            "[stj] Extracted %d case records", len(raw),
        )
        return raw

    def transform(self, data: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        cases: list[dict[str, Any]] = []
        rapporteur_rels: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}
        for row in data.itertuples(index=False):
            case_class = str(
                getattr(row, "classe", "")
            ).strip()
            case_number = str(
                getattr(row, "numero", "")
            ).strip()
            year = str(getattr(row, "ano", "")).strip()

            if not case_class or not case_number:
                continue

            case_id = _generate_case_id(
                case_class, case_number, year,
            )
            rapporteur = normalize_name(
                str(getattr(row, "relator", ""))
            )
            decision_type = str(
                getattr(row, "tipo_decisao", "")
            ).strip()
            decision_date = str(
                getattr(row, "data_decisao", "")
            ).strip()
            subject = str(
                getattr(row, "assunto", "")
            ).strip()
            origin_state = str(
                getattr(row, "uf_origem", "")
            ).strip()

            cases.append({
                "case_id": case_id,
                "case_class": case_class,
                "case_number": case_number,
                "year": year,
                "rapporteur": rapporteur,
                "decision_type": decision_type,
                "decision_date": decision_date,
                "subject": subject,
                "origin_state": origin_state,
                "court": "STJ",
                "source": self.source_id,
            })

            if rapporteur:
                rapporteur_rels.append({
                    "source_key": rapporteur,
                    "target_key": case_id,
                })

            if self.limit and len(cases) >= self.limit:
                break

        dict_result["cases"] = deduplicate_rows(cases, ["case_id"])
        dict_result["rapporteur_rels"] = rapporteur_rels
        logger.info(
            "[stj] Transformed %d cases", len(dict_result["cases"]),
        )
        return dict_result

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data.get("cases"):
            loader.load_nodes(
                "LegalCase", data["cases"], key_field="case_id",
            )

        if data.get("rapporteur_rels"):
            query = (
                "UNWIND $rows AS row "
                "MERGE (p:Person {name: row.source_key}) "
                "WITH p, row "
                "MATCH (lc:LegalCase {case_id: row.target_key}) "
                "MERGE (p)-[:RELATOR_DE]->(lc)"
            )
            loader.run_query_with_retry(
                query, data["rapporteur_rels"],
            )
