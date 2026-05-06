from __future__ import annotations

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


class TcuPipeline(Pipeline):
    """ETL pipeline for TCU (Tribunal de Contas da Uniao) accountability data.

    Loads four datasets:
    - inabilitados: individuals barred from public office
    - licitantes inidoneos: companies declared unfit for public bidding
    - contas julgadas irregulares: persons with irregular accounts
    - contas irregulares com implicacao eleitoral: same with electoral context
    """

    name = "tcu"
    source_id = "tcu"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def _read_csv(self, path: Path) -> pd.DataFrame:
        return pd.read_csv(
            path,
            dtype=str,
            sep="|",
            encoding="utf-8",
            keep_default_na=False,
            quotechar='"',
            chunksize=self.chunk_size,
        )

    def extract(self) -> dict[str, pd.DataFrame]:
        tcu_dir = Path(self.data_dir) / "tcu"
        dict_result: dict[str, pd.DataFrame] = {}
        dict_result["raw_inabilitados"] = self._read_csv(
            tcu_dir / "inabilitados-funcao-publica.csv")
        dict_result["raw_inidoneos"] = self._read_csv(
            tcu_dir / "licitantes-inidoneos.csv"
        )
        dict_result["raw_irregulares"] = self._read_csv(
            tcu_dir / "resp-contas-julgadas-irregulares.csv"
        )
        dict_result["raw_irregulares_eleitorais"] = self._read_csv(
            tcu_dir / "resp-contas-julgadas-irreg-implicacao-eleitoral.csv"
        )

        logger.info(
            "[tcu] Extracted: %d inabilitados, %d inidoneos, "
            "%d irregulares, %d irregulares eleitorais",
            len(dict_result["raw_inabilitados"]),
            len(dict_result["raw_inidoneos"]),
            len(dict_result["raw_irregulares"]),
            len(dict_result["raw_irregulares_eleitorais"]),
        )

        return dict_result

    def _process_inabilitados(self, raw_inabilitados: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        """Persons barred from public office (CPF-only)."""
        sanctions: list[dict[str, Any]] = []
        sanctioned_persons: list[dict[str, Any]] = []
        for idx, row in raw_inabilitados.iterrows():
            cpf_raw = str(row["CPF"]).strip()
            digits = strip_document(cpf_raw)
            if len(digits) != 11:
                continue

            cpf = format_cpf(cpf_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            date_end = parse_date(str(row["DATA FINAL"]))
            date_acordao = parse_date(str(row["DATA ACORDAO"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()

            sanction_id = f"tcu_inabilitado_{digits}_{idx}"
            sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_inabilitado",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": date_end,
                "date_acordao": date_acordao,
                "uf": uf,
                "municipio": municipio,
                "cargo": "",
                "source": "tcu",
            })
            sanctioned_persons.append({
                "cpf": cpf,
                "name": nome,
                "sanction_id": sanction_id,
            })

        return {"sanctions": sanctions, "sanctioned_persons": sanctioned_persons}

    def _process_inidoneos(self, raw_inidoneos: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        """Companies declared unfit for public bidding (CNPJ-only)."""
        sanctions: list[dict[str, Any]] = []
        sanctioned_companies: list[dict[str, Any]] = []
        sanctioned_persons: list[dict[str, Any]] = []
        for idx, row in raw_inidoneos.iterrows():
            doc_raw = str(row["CPF_CNPJ"]).strip()
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            date_end = parse_date(str(row["DATA FINAL"]))
            date_acordao = parse_date(str(row["DATA ACORDAO"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()

            sanction_id = f"tcu_inidoneo_{digits}_{idx}"
            sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_inidoneo",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": date_end,
                "date_acordao": date_acordao,
                "uf": uf,
                "municipio": municipio,
                "cargo": "",
                "source": "tcu",
            })

            if len(digits) == 14:
                cnpj = format_cnpj(doc_raw)
                sanctioned_companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome,
                    "name": nome,
                    "sanction_id": sanction_id,
                })
            elif len(digits) == 11:
                cpf = format_cpf(doc_raw)
                sanctioned_persons.append({
                    "cpf": cpf,
                    "name": nome,
                    "sanction_id": sanction_id,
                })
        return {"sanctions": sanctions, "sanctioned_companies": sanctioned_companies, "sanctioned_persons": sanctioned_persons}

    def _process_irregulares(self, raw_irregulares: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        """Persons with accounts judged irregular (may have CPF or CNPJ)."""
        sanctions: list[dict[str, Any]] = []
        sanctioned_companies: list[dict[str, Any]] = []
        sanctioned_persons: list[dict[str, Any]] = []
        for idx, row in raw_irregulares.iterrows():
            doc_raw = str(row["CPF_CNPJ"]).strip()
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()

            sanction_id = f"tcu_irregular_{digits}_{idx}"
            sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_conta_irregular",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": "",
                "date_acordao": "",
                "uf": uf,
                "municipio": municipio,
                "cargo": "",
                "source": "tcu",
            })

            if len(digits) == 14:
                cnpj = format_cnpj(doc_raw)
                sanctioned_companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome,
                    "name": nome,
                    "sanction_id": sanction_id,
                })
            elif len(digits) == 11:
                cpf = format_cpf(doc_raw)
                sanctioned_persons.append({
                    "cpf": cpf,
                    "name": nome,
                    "sanction_id": sanction_id,
                })
        return {"sanctions": sanctions, "sanctioned_companies": sanctioned_companies, "sanctioned_persons": sanctioned_persons}

    def _process_irregulares_eleitorais(self, raw_irregulares_eleitorais: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        """Persons with irregular accounts and electoral implication (CPF-only)."""
        sanctions: list[dict[str, Any]] = []
        sanctioned_persons: list[dict[str, Any]] = []

        for idx, row in raw_irregulares_eleitorais.iterrows():
            cpf_raw = str(row["CPF"]).strip()
            digits = strip_document(cpf_raw)
            if len(digits) != 11:
                continue

            cpf = format_cpf(cpf_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            date_end = parse_date(str(row["DATA FINAL"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()
            cargo = str(row.get("CARGO/FUNCAO", "")).strip()

            sanction_id = f"tcu_irregular_eleitoral_{digits}_{idx}"
            sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_conta_irregular_eleitoral",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": date_end,
                "date_acordao": "",
                "uf": uf,
                "municipio": municipio,
                "cargo": cargo,
                "source": "tcu",
            })
            sanctioned_persons.append({
                "cpf": cpf,
                "name": nome,
                "sanction_id": sanction_id,
            })

        return {"sanctions": sanctions, "sanctioned_persons": sanctioned_persons}

    def transform(self, data: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
        raw_inabilitados: pd.DataFrame = data.get("raw_inabilitados", pd.DataFrame())
        raw_inidoneos: pd.DataFrame = data.get("raw_inidoneos", pd.DataFrame())
        raw_irregulares: pd.DataFrame = data.get("raw_irregulares", pd.DataFrame())
        raw_irregulares_eleitorais: pd.DataFrame = data.get("raw_irregulares_eleitorais", pd.DataFrame())
        dict_result: dict[str, list[dict[str, Any]]] = {}
        inabilitados_result = self._process_inabilitados(raw_inabilitados)
        inidoneos_result = self._process_inidoneos(raw_inidoneos)
        irregulares_result = self._process_irregulares(raw_irregulares)
        irregulares_eleitorais_result = self._process_irregulares_eleitorais(raw_irregulares_eleitorais)

        dict_result = {
            "sanctions": inabilitados_result["sanctions"] + inidoneos_result["sanctions"] + irregulares_result["sanctions"] + irregulares_eleitorais_result["sanctions"],
            "sanctioned_persons": inabilitados_result["sanctioned_persons"] + inidoneos_result.get("sanctioned_persons", []) + irregulares_result.get("sanctioned_persons", []) + irregulares_eleitorais_result.get("sanctioned_persons", []),
            "sanctioned_companies": inidoneos_result.get("sanctioned_companies", []) + irregulares_result.get("sanctioned_companies", []),
        }

        dict_result["sanctions"] = deduplicate_rows(dict_result["sanctions"], ["sanction_id"])

        logger.info(
            "[tcu] Transformed: %d sanctions, %d person links, %d company links",
            len(dict_result["sanctions"]),
            len(dict_result["sanctioned_persons"]),
            len(dict_result["sanctioned_companies"]),
        )

        return dict_result

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # Load Sanction nodes
        if data["sanctions"]:
            loader.load_nodes("Sanction", data["sanctions"], key_field="sanction_id")
            logger.info("[tcu] Loaded %d Sanction nodes", len(data["sanctions"]))

        # Merge Person nodes and create relationships
        if data["sanctioned_persons"]:
            person_nodes = deduplicate_rows(
                [{"cpf": p["cpf"], "name": p["name"]} for p in data["sanctioned_persons"]],
                ["cpf"],
            )
            loader.load_nodes("Person", person_nodes, key_field="cpf")
            logger.info("[tcu] Merged %d Person nodes", len(person_nodes))

            person_rels = [
                {"source_key": p["cpf"], "target_key": p["sanction_id"]}
                for p in data["sanctioned_persons"]
            ]
            query_person = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "MERGE (p)-[:SANCIONADA]->(s)"
            )
            loader.run_query(query_person, person_rels)
            logger.info("[tcu] Created %d Person-SANCIONADA->Sanction rels", len(person_rels))

        # Merge Company nodes and create relationships
        if data["sanctioned_companies"]:
            company_nodes = deduplicate_rows(
                [
                    {"cnpj": c["cnpj"], "razao_social": c["razao_social"], "name": c["name"]}
                    for c in data["sanctioned_companies"]
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", company_nodes, key_field="cnpj")
            logger.info("[tcu] Merged %d Company nodes", len(company_nodes))

            company_rels = [
                {"source_key": c["cnpj"], "target_key": c["sanction_id"]}
                for c in data["sanctioned_companies"]
            ]
            query_company = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "MERGE (c)-[:SANCIONADA]->(s)"
            )
            loader.run_query(query_company, company_rels)
            logger.info("[tcu] Created %d Company-SANCIONADA->Sanction rels", len(company_rels))
