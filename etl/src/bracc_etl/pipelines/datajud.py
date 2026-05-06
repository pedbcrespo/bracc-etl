from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _stable_id(*parts: str, length: int = 24) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _pick(row: pd.Series, *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


class DatajudPipeline(Pipeline):
    """ETL pipeline for CNJ DataJud judicial cases (when available)."""

    name = "datajud"
    source_id = "datajud"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> dict[str, pd.DataFrame]:
        src_dir = Path(self.data_dir) / "datajud"
        cases_csv = src_dir / "cases.csv"
        parties_csv = src_dir / "parties.csv"

        dict_results: dict[str, pd.DataFrame] = {}

        if cases_csv.exists():
            dict_results["raw_cases"] = pd.read_csv(cases_csv, dtype=str, keep_default_na=False)

        if parties_csv.exists():
            dict_results["raw_parties"] = pd.read_csv(parties_csv, dtype=str, keep_default_na=False)

        if dict_results["raw_cases"].empty and not cases_csv.exists():
            dry_run_manifest = src_dir / "dry_run_manifest.json"
            if dry_run_manifest.exists():
                try:
                    payload = json.loads(dry_run_manifest.read_text(encoding="utf-8"))
                    logger.info(
                        "[datajud] dry-run mode: %s",
                        payload.get("message", "manifest found"),
                    )
                except json.JSONDecodeError:
                    logger.info("[datajud] dry-run mode: manifest present")

        if self.limit:
            dict_results["raw_cases"] = dict_results["raw_cases"].head(self.limit)
            dict_results["raw_parties"] = dict_results["raw_parties"].head(self.limit)

        logger.info(
            "[datajud] extracted cases=%d parties=%d",
            len(dict_results["raw_cases"]),
            len(dict_results["raw_parties"]),
        )
        return dict_results

    def transform(self, data: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
        dict_results: dict[str, list[dict[str, Any]]] = {}
        if not data["raw_cases"].empty:
            dict_results["cases"] = self._transform_cases(data["raw_cases"])
        if not data["raw_parties"].empty:
            result = self._transform_parties(data["raw_parties"])
            dict_results.update(result)
        return dict_results

    def _transform_cases(self, data: pd.DataFrame) -> list[dict[str, Any]]:
        cases: list[dict[str, Any]] = []

        for _, row in data.iterrows():
            case_id = _pick(row, "judicial_case_id", "case_id", "id")
            case_number = _pick(row, "case_number", "numero_processo")
            court = _pick(row, "court", "tribunal")
            branch = _pick(row, "branch", "segmento")
            class_name = _pick(row, "class", "classe")
            subject = normalize_name(_pick(row, "subject", "assunto"))
            filed_at = parse_date(_pick(row, "filed_at", "data_ajuizamento"))
            status = _pick(row, "status", "situacao")
            source_url = _pick(row, "source_url", "url")

            if not case_id:
                case_id = _stable_id(case_number, court, filed_at)

            cases.append({
                "judicial_case_id": case_id,
                "case_number": case_number,
                "court": court,
                "branch": branch,
                "class": class_name,
                "subject": subject,
                "filed_at": filed_at,
                "status": status,
                "source_url": source_url,
                "source": "datajud",
            })

        return deduplicate_rows(cases, ["judicial_case_id"])

    def _transform_parties(self, data: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        persons: list[dict[str, Any]] = []
        companies: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []

        for _, row in data.iterrows():
            case_id = _pick(row, "judicial_case_id", "case_id", "id_processo")
            if not case_id:
                continue

            role = _pick(row, "role", "polo", "tipo_parte")
            party_name = normalize_name(_pick(row, "party_name", "nome", "parte"))

            cpf_digits = strip_document(_pick(row, "party_cpf", "cpf"))
            cnpj_digits = strip_document(_pick(row, "party_cnpj", "cnpj"))

            if len(cpf_digits) == 11:
                cpf = format_cpf(cpf_digits)
                persons.append({"cpf": cpf, "name": party_name})
                person_rels.append({
                    "source_key": cpf,
                    "target_key": case_id,
                    "role": role,
                })
            elif len(cnpj_digits) == 14:
                cnpj = format_cnpj(cnpj_digits)
                companies.append({"cnpj": cnpj, "razao_social": party_name})
                company_rels.append({
                    "source_key": cnpj,
                    "target_key": case_id,
                    "role": role,
                })

        persons = deduplicate_rows(persons, ["cpf"])
        companies = deduplicate_rows(companies, ["cnpj"])
        person_rels = deduplicate_rows(person_rels, ["source_key", "target_key", "role"])
        company_rels = deduplicate_rows(
            company_rels,
            ["source_key", "target_key", "role"],
        )
        return {
            "persons": persons,
            "companies": companies,
            "person_case_rels": person_rels,
            "company_case_rels": company_rels,
        }

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data.get("cases"):
            loader.load_nodes("JudicialCase", data["cases"], key_field="judicial_case_id")

        if data.get("persons"):
            loader.load_nodes("Person", data["persons"], key_field="cpf")

        if data.get("companies"):
            loader.load_nodes("Company", data["companies"], key_field="cnpj")

        if data.get("person_case_rels"):
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (j:JudicialCase {judicial_case_id: row.target_key}) "
                "MERGE (p)-[r:PARTE_PROCESSO]->(j) "
                "SET r.role = row.role"
            )
            loader.run_query_with_retry(query, data["person_case_rels"])

        if data.get("company_case_rels"):
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (j:JudicialCase {judicial_case_id: row.target_key}) "
                "MERGE (c)-[r:PARTE_PROCESSO]->(j) "
                "SET r.role = row.role"
            )
            loader.run_query_with_retry(query, data["company_case_rels"])
