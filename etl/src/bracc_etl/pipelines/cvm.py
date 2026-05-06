"""ETL pipeline for CVM (Comissao de Valores Mobiliarios) sanctions data.

Ingests PAS (Processo Administrativo Sancionador) results from CVM open data.
Creates CVMProceeding nodes linked to Company/Person nodes via CVM_SANCIONADA.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
    parse_date,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class CvmPipeline(Pipeline):
    """ETL pipeline for CVM PAS sanctions data."""

    name = "cvm"
    source_id = "cvm"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> dict[str, pd.DataFrame]:
        cvm_dir = Path(self.data_dir) / "cvm"
        raw_processos: pd.DataFrame = pd.DataFrame()
        raw_acusados: pd.DataFrame = pd.DataFrame()

        # New CVM format (processo_sancionador.zip contents)
        proc_path = cvm_dir / "processo_sancionador.csv"
        acusado_path = cvm_dir / "processo_sancionador_acusado.csv"

        if not proc_path.exists():
            msg = f"CVM proceedings file not found: {proc_path}"
            raise FileNotFoundError(msg)

        raw_processos = pd.read_csv(
            proc_path,
            sep=";",
            dtype=str,
            keep_default_na=False,
            encoding="latin-1",
            chunksize=self.chunk_size,
        )
        if acusado_path.exists():
            raw_acusados = pd.read_csv(
                acusado_path,
                sep=";",
                dtype=str,
                keep_default_na=False,
                encoding="latin-1",
                chunksize=self.chunk_size,
            )
        return {
            "raw_processos": raw_processos,
            "raw_acusados": raw_acusados
        }

    def transform(self, data: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
        # Build accused lookup by NUP
        accused_by_nup: dict[str, list[dict[str, str]]] = {}
        dict_results: dict[str, list[dict[str, Any]]] = {}

        if not data["raw_acusados"].empty:
            for _, row in data["raw_acusados"].iterrows():
                nup = str(row.get("NUP", "")).strip()
                if not nup:
                    continue
                nome = normalize_name(str(row.get("Nome_Acusado", "")))
                situacao = str(row.get("Situacao", "")).strip()
                data_sit = parse_date(str(row.get("Data_Situacao", "")))
                accused_by_nup.setdefault(nup, []).append({
                    "name": nome,
                    "status": situacao,
                    "date": data_sit,
                })

        proceedings: list[dict[str, Any]] = []
        entities: list[dict[str, Any]] = []

        for _, row in data["raw_processos"].iterrows():
            nup = str(row.get("NUP", "")).strip()
            if not nup:
                continue

            date = parse_date(str(row.get("Data_Abertura", "")))
            fase = str(row.get("Fase_Atual", "")).strip()
            objeto = str(row.get("Objeto", "")).strip()
            ementa = str(row.get("Ementa", "")).strip()

            proceedings.append({
                "pas_id": nup,
                "date": date,
                "penalty_type": "",
                "penalty_value": 0.0,
                "status": fase,
                "description": ementa or objeto,
                "numero_processo": nup,
                "relator": "",
                "data_instauracao": date,
                "source": "cvm",
            })

            # Link accused persons (name-based, no CPF/CNPJ in new format)
            for accused in accused_by_nup.get(nup, []):
                entities.append({
                    "target_key": nup,
                    "entity_name": accused["name"],
                    "accused_status": accused["status"],
                    "accused_date": accused["date"],
                })

        dict_results["proceedings"] = deduplicate_rows(proceedings, ["pas_id"])
        dict_results["accused_entities"] = entities

        if self.limit:
            dict_results["proceedings"] = dict_results["proceedings"][: self.limit]
            dict_results["accused_entities"] = dict_results["accused_entities"][: self.limit]

        logger.info(
            "Transformed: %d proceedings, %d accused entities",
            len(dict_results["proceedings"]),
            len(dict_results["accused_entities"]),
        )
        return dict_results

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)
        proceedings = data.get("proceedings", [])
        accused_entities = data.get("accused_entities", [])
        if proceedings:
            loader.load_nodes("CVMProceeding", proceedings, key_field="pas_id")

        # Name-based matching: find existing Person/Company by name
        if accused_entities:
            rel_rows = [
                {
                    "target_key": e["target_key"],
                    "entity_name": e["entity_name"],
                }
                for e in accused_entities
                if e["entity_name"]
            ]

            query = (
                "UNWIND $rows AS row "
                "MATCH (p:CVMProceeding {pas_id: row.target_key}) "
                "OPTIONAL MATCH (pe:Person) WHERE pe.name = row.entity_name "
                "OPTIONAL MATCH (c:Company) WHERE c.razao_social = row.entity_name "
                "WITH p, coalesce(pe, c) AS entity "
                "WHERE entity IS NOT NULL "
                "MERGE (entity)-[:CVM_SANCIONADA]->(p)"
            )
            loader.run_query(query, rel_rows)
