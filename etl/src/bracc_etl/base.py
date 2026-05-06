import pandas as pd
import logging
import psutil
import os
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

from neo4j import Driver

logger = logging.getLogger(__name__)

def calculate_dynamic_chunksize(row_size_estimate_kb: int = 2, target_memory_fraction: float = 0.1) -> int:
    try:
        available_mem_bytes = psutil.virtual_memory().available
        available_mem_kb = available_mem_bytes / 1024
        target_mem_kb = available_mem_kb * target_memory_fraction
        chunk_size = int(target_mem_kb / row_size_estimate_kb)
        chunk_size = max(10_000, min(chunk_size, 100_000))
        return chunk_size
        
    except Exception as e:
        logger.warning(f"Erro, default value 50.000: {e}")
        return 50_000


class Pipeline(ABC):
    """Base class for all ETL pipelines."""

    name: str
    source_id: str

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int | None = None,
        neo4j_database: str | None = None,
        history: bool = False,
    ) -> None:
        self.driver = driver
        self.data_dir = data_dir
        self.limit = limit
        self.chunk_size = chunk_size if chunk_size is not None else calculate_dynamic_chunksize()
        self.neo4j_database = neo4j_database or os.getenv("NEO4J_DATABASE", "neo4j")
        self.history = history
        self.rows_in: int = 0
        self.rows_loaded: int = 0
        source_key = getattr(self, "source_id", getattr(self, "name", "unknown_source"))
        self.run_id = f"{source_key}_{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S')}"

    @abstractmethod
    def extract(self) -> pd.DataFrame | list[Any] | dict[str, Any]:
        """Download raw data from source."""

    @abstractmethod
    def transform(self, data: pd.DataFrame | list[Any] | dict[str, Any]) -> list[Any] | dict[str, Any]:
        """Normalize, deduplicate, and prepare data for loading."""

    @abstractmethod
    def load(self, data: list[Any] | dict[str, Any]) -> None:
        """Load transformed data into Neo4j."""

    def cleanup(self) -> None:
        """Optional cleanup method to delete temporary attributes after run.
        
        Subclasses can override this to delete specific attributes (e.g., large DataFrames)
        to free memory. By default, does nothing to avoid breaking essential attributes.
        """
        pass  # Subclasses can implement custom cleanup logic

    def run(self) -> None:
        """Execute the full ETL pipeline."""
        started_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._upsert_ingestion_run(status="running", started_at=started_at)
        try:
            logger.info("[%s] Starting extraction...", self.name)
            data = self.extract()
            logger.info("[%s] Starting transformation...", self.name)
            transformed_data = self.transform(data)
            logger.info("[%s] Starting load...", self.name)
            self.load(transformed_data)
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="loaded",
                started_at=started_at,
                finished_at=finished_at,
            )
            logger.info("[%s] Pipeline complete.", self.name)
            self.cleanup()
        except Exception as exc:
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="quality_fail",
                started_at=started_at,
                finished_at=finished_at,
                error=str(exc)[:1000],
            )
            raise

    def _upsert_ingestion_run(
        self,
        *,
        status: str,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        """Persist ingestion run state for operational traceability."""
        source_id = getattr(self, "source_id", getattr(self, "name", "unknown_source"))
        query = (
            "MERGE (r:IngestionRun {run_id: $run_id}) "
            "SET r.source_id = $source_id, "
            "    r.status = $status, "
            "    r.started_at = coalesce($started_at, r.started_at), "
            "    r.finished_at = coalesce($finished_at, r.finished_at), "
            "    r.error = coalesce($error, r.error), "
            "    r.rows_in = $rows_in, "
            "    r.rows_loaded = $rows_loaded"
        )
        run_id = getattr(self, "run_id", f"{source_id}_manual")
        params = {
            "run_id": run_id,
            "source_id": source_id,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "error": error,
            "rows_in": self.rows_in,
            "rows_loaded": self.rows_loaded,
        }
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                session.run(query, params)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] failed to persist IngestionRun: %s", self.name, exc)
