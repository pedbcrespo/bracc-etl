from __future__ import annotations

import logging
from difflib import SequenceMatcher
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
)

logger = logging.getLogger(__name__)

# Confidence thresholds for fuzzy name matching
EXACT_MATCH = 1.0
HIGH_CONFIDENCE = 0.85
MIN_CONFIDENCE = 0.7


def name_similarity(a: str, b: str) -> float:
    """Compute similarity ratio between two names (0.0-1.0)."""
    return SequenceMatcher(None, a.upper(), b.upper()).ratio()


class ICIJPipeline(Pipeline):
    """ETL pipeline for ICIJ OffshoreLeaks data (Panama/Paradise/Pandora Papers)."""

    name = "icij"
    source_id = "icij_offshore_leaks"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    @staticmethod
    def _is_brazilian(row: pd.Series) -> bool:
        """Check if a row has Brazilian connections."""
        jurisdiction = str(row.get("jurisdiction", "")).upper()
        country_codes = str(row.get("country_codes", "")).upper()
        countries = str(row.get("countries", "")).upper()
        address = str(row.get("address", "")).upper()

        brazil_terms = {"BRA", "BRAZIL", "BRASIL"}
        for field in (jurisdiction, country_codes, countries, address):
            for term in brazil_terms:
                if term in field:
                    return True
        return False

    def extract(self) -> dict[str, pd.DataFrame]:
        icij_dir = Path(self.data_dir) / "icij"

        entities_path = icij_dir / "nodes-entities.csv"
        officers_path = icij_dir / "nodes-officers.csv"
        intermediaries_path = icij_dir / "nodes-intermediaries.csv"
        rels_path = icij_dir / "relationships.csv"

        entities_raw: pd.DataFrame = pd.DataFrame()
        officers_raw: pd.DataFrame = pd.DataFrame()
        intermediaries_raw: pd.DataFrame = pd.DataFrame()
        relationships_raw: pd.DataFrame = pd.DataFrame()

        read_opts: dict[str, Any] = {
            "dtype": str,
            "keep_default_na": False,
        }

        if entities_path.exists():
            entities_raw = pd.read_csv(entities_path, **read_opts, chunksize=self.chunk_size)
            logger.info("[icij] Extracted %d entities", len(entities_raw))

        if officers_path.exists():
            officers_raw = pd.read_csv(officers_path, **read_opts, chunksize=self.chunk_size)
            logger.info("[icij] Extracted %d officers", len(officers_raw))

        if intermediaries_path.exists():
            intermediaries_raw = pd.read_csv(intermediaries_path, **read_opts, chunksize=self.chunk_size)
            logger.info("[icij] Extracted %d intermediaries", len(intermediaries_raw))

        if rels_path.exists():
            relationships_raw = pd.read_csv(rels_path, **read_opts, chunksize=self.chunk_size)
            logger.info("[icij] Extracted %d relationships", len(relationships_raw))

        return {
            "entities": entities_raw,
            "officers": officers_raw,
            "intermediaries": intermediaries_raw,
            "relationships": relationships_raw
        }

    def _transform_entities(self, entities_raw: pd.DataFrame) -> list[dict[str, Any]]:
        """Transform ICIJ entity nodes, filtering for Brazilian connections."""
        entities: list[dict[str, Any]] = []

        for _, row in  entities_raw.iterrows():
            if not self._is_brazilian(row):
                continue

            node_id = str(row.get("node_id", "")).strip()
            if not node_id:
                continue

            name_raw = str(row.get("name", "")).strip()
            if not name_raw:
                continue

            entities.append({
                "offshore_id": f"icij_{node_id}",
                "name": normalize_name(name_raw),
                "original_name": name_raw,
                "jurisdiction": str(row.get("jurisdiction", "")).strip(),
                "country_codes": str(row.get("country_codes", "")).strip(),
                "source_investigation": str(row.get("sourceID", "")).strip(),
                "status": str(row.get("status", "")).strip(),
                "incorporation_date": str(row.get("incorporation_date", "")).strip(),
                "address": str(row.get("address", "")).strip(),
                "source": "icij_offshore_leaks",
            })

        return entities

    def _transform_officers(self, officers_raw: pd.DataFrame) -> list[dict[str, Any]]:
        """Transform ICIJ officer nodes, filtering for Brazilian connections."""
        officers: list[dict[str, Any]] = []

        for _, row in officers_raw.iterrows():
            if not self._is_brazilian(row):
                continue

            node_id = str(row.get("node_id", "")).strip()
            if not node_id:
                continue

            name_raw = str(row.get("name", "")).strip()
            if not name_raw:
                continue

            officers.append({
                "offshore_officer_id": f"icij_{node_id}",
                "name": normalize_name(name_raw),
                "original_name": name_raw,
                "country_codes": str(row.get("country_codes", "")).strip(),
                "source_investigation": str(row.get("sourceID", "")).strip(),
                "source": "icij_offshore_leaks",
            })

        return officers

    def _transform_relationships(self, relationships_raw: pd.DataFrame, offshore_entities: list[dict[str, Any]], offshore_officers: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Build OFFICER_OF and INTERMEDIARY_OF relationships from the relationships CSV."""
        officer_rels: list[dict[str, Any]] = []
        intermediary_rels: list[dict[str, Any]] = []

        entity_ids = {e["offshore_id"] for e in offshore_entities}
        officer_ids = {o["offshore_officer_id"] for o in offshore_officers}

        for _, row in relationships_raw.iterrows():
            node_id_start = str(row.get("node_id_start", "")).strip()
            node_id_end = str(row.get("node_id_end", "")).strip()
            rel_type = str(row.get("rel_type", "")).strip().lower()
            link = str(row.get("link", "")).strip()

            start_key = f"icij_{node_id_start}"
            end_key = f"icij_{node_id_end}"

            if "officer" in rel_type or "officer" in link.lower():
                if start_key in officer_ids and end_key in entity_ids:
                    officer_rels.append({
                        "source_key": start_key,
                        "target_key": end_key,
                        "link": link,
                        "source_investigation": str(row.get("sourceID", "")).strip(),
                    })
            elif (
                "intermediary" in rel_type or "intermediary" in link.lower()
            ) and end_key in entity_ids:
                intermediary_rels.append({
                        "source_key": start_key,
                        "target_key": end_key,
                        "link": link,
                        "source_investigation": str(row.get("sourceID", "")).strip(),
                    })

        return officer_rels, intermediary_rels

    def transform(self, data: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
        dict_result: dict[str, list[dict[str, Any]]] = {
            "offshore_entities": deduplicate_rows(self._transform_entities(data["entities"]), ["offshore_id"]),
            "offshore_officers": deduplicate_rows(self._transform_officers(data["officers"]), ["offshore_officer_id"]),
        }
        dict_result["officer_of_rels"], dict_result["intermediary_of_rels"] = self._transform_relationships(data["relationships"], dict_result["offshore_entities"], dict_result["offshore_officers"])

        logger.info(
            "[icij] Transformed %d OffshoreEntity, %d OffshoreOfficer, "
            "%d OFFICER_OF, %d INTERMEDIARY_OF",
            len(dict_result["offshore_entities"]),
            len(dict_result["offshore_officers"]),
            len(dict_result["officer_of_rels"]),
            len(dict_result["intermediary_of_rels"]),
        )
        return dict_result

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data["offshore_entities"]:
            loaded = loader.load_nodes(
                "OffshoreEntity", data["offshore_entities"], key_field="offshore_id"
            )
            logger.info("[icij] Loaded %d OffshoreEntity nodes", loaded)

        if data["offshore_officers"]:
            loaded = loader.load_nodes(
                "OffshoreOfficer", data["offshore_officers"], key_field="offshore_officer_id"
            )
            logger.info("[icij] Loaded %d OffshoreOfficer nodes", loaded)

        if data["officer_of_rels"]:
            query = (
                "UNWIND $rows AS row "
                "MATCH (o:OffshoreOfficer {offshore_officer_id: row.source_key}) "
                "MATCH (e:OffshoreEntity {offshore_id: row.target_key}) "
                "MERGE (o)-[r:OFFICER_OF]->(e) "
                "SET r.link = row.link, "
                "    r.source_investigation = row.source_investigation"
            )
            loaded = loader.run_query_with_retry(query, data["officer_of_rels"])
            logger.info("[icij] Loaded %d OFFICER_OF relationships", loaded)

        if data["intermediary_of_rels"]:
            query = (
                "UNWIND $rows AS row "
                "MATCH (i:OffshoreOfficer {offshore_officer_id: row.source_key}) "
                "MATCH (e:OffshoreEntity {offshore_id: row.target_key}) "
                "MERGE (i)-[r:INTERMEDIARY_OF]->(e) "
                "SET r.link = row.link, "
                "    r.source_investigation = row.source_investigation"
            )
            loaded = loader.run_query_with_retry(query, data["intermediary_of_rels"])
            logger.info("[icij] Loaded %d INTERMEDIARY_OF relationships", loaded)
