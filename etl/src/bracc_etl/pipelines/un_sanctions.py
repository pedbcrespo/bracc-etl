from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
)

logger = logging.getLogger(__name__)

VALID_ENTITY_TYPES = {"individual", "entity"}


def _generate_sanction_id(reference_number: str, name: str) -> str:
    """Generate a deterministic 16-char hex ID from reference_number + name."""
    raw = f"{reference_number}|{name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class UnSanctionsPipeline(Pipeline):
    """ETL pipeline for UN Security Council consolidated sanctions list.

    Loads all INDIVIDUAL and ENTITY entries as InternationalSanction nodes.
    Name-based matching to existing Person/Company nodes is attempted
    via MERGE on normalized name.
    """

    name = "un_sanctions"
    source_id = "un_sanctions"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> list[dict[str, Any]]:
        un_dir = Path(self.data_dir) / "un_sanctions"
        json_path = un_dir / "un_sanctions.json"
        raw: list[dict[str, Any]] = []
        if not json_path.exists():
            logger.warning("[un_sanctions] un_sanctions.json not found at %s", json_path)
            return

        logger.info("[un_sanctions] Reading %s", json_path)
        with open(json_path, encoding="utf-8") as f:
            raw = json.load(f)

        if self.limit:
            raw = raw[: self.limit]

        logger.info("[un_sanctions] Extracted %d entries", len(raw))
        return raw

    def transform(self, data: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        sanctions: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []
        dict_results: dict[str, list[dict[str, Any]]] = {}

        for entry in data:
            reference_number = str(entry.get("reference_number", "")).strip()
            if not reference_number:
                continue

            entity_type = str(entry.get("entity_type", "")).strip().lower()
            if entity_type not in VALID_ENTITY_TYPES:
                continue

            name_raw = str(entry.get("name", "")).strip()
            if not name_raw:
                continue

            name_normalized = normalize_name(name_raw)
            sanction_id = _generate_sanction_id(reference_number, name_raw)

            sanction: dict[str, Any] = {
                "sanction_id": sanction_id,
                "name": name_normalized,
                "original_name": name_raw,
                "entity_type": entity_type,
                "reference_number": reference_number,
                "listed_date": str(entry.get("listed_date", "")).strip(),
                "un_list_type": str(entry.get("un_list_type", "")).strip(),
                "nationality": str(entry.get("nationality", "")).strip(),
                "source": "un_sanctions",
                "source_list": "UN",
            }

            aliases = entry.get("aliases", [])
            if aliases:
                sanction["aliases"] = "|".join(str(a) for a in aliases)

            sanctions.append(sanction)

            # Build name-match relationships
            if entity_type == "individual":
                person_rels.append({
                    "source_key": sanction_id,
                    "target_key": name_normalized,
                })
            elif entity_type == "entity":
                company_rels.append({
                    "source_key": sanction_id,
                    "target_key": name_normalized,
                })

        dict_results["sanctions"] = deduplicate_rows(sanctions, ["sanction_id"])

        # Filter rels to only include sanctions that survived dedup
        valid_ids = {s["sanction_id"] for s in dict_results["sanctions"]}
        dict_results["person_rels"] = [r for r in person_rels if r["source_key"] in valid_ids]
        dict_results["company_rels"] = [r for r in company_rels if r["source_key"] in valid_ids]

        logger.info(
            "[un_sanctions] Transformed %d InternationalSanction nodes "
            "(%d person matches, %d company matches)",
            len(dict_results["sanctions"]),
            len(dict_results["person_rels"]),
            len(dict_results["company_rels"]),
        )
        return dict_results

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data["sanctions"]:
            loaded = loader.load_nodes(
                "InternationalSanction", data["sanctions"], key_field="sanction_id"
            )
            logger.info("[un_sanctions] Loaded %d InternationalSanction nodes", loaded)

        if data["person_rels"]:
            person_query = (
                "UNWIND $rows AS row "
                "MATCH (s:InternationalSanction {sanction_id: row.source_key}) "
                "MATCH (p:Person {name: row.target_key}) "
                "MERGE (p)-[r:UN_SANCTIONED]->(s) "
                "SET r.matched_by = 'name'"
            )
            loaded = loader.run_query_with_retry(person_query, data["person_rels"])
            logger.info("[un_sanctions] Created %d Person UN_SANCTIONED rels", loaded)

        if data["company_rels"]:
            company_query = (
                "UNWIND $rows AS row "
                "MATCH (s:InternationalSanction {sanction_id: row.source_key}) "
                "MATCH (c:Company {razao_social: row.target_key}) "
                "MERGE (c)-[r:UN_SANCTIONED]->(s) "
                "SET r.matched_by = 'name'"
            )
            loaded = loader.run_query_with_retry(company_query, data["company_rels"])
            logger.info("[un_sanctions] Created %d Company UN_SANCTIONED rels", loaded)
