from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import deduplicate_rows, normalize_name

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Column mapping: original CSV header -> safe attribute name
_COL_RENAME = {
    "OB": "ob",
    "Data": "data",
    "Ano": "ano",
    "Mês": "mes",
    "Nome Emenda": "nome_emenda",
    "Transferência Especial": "transferencia_especial",
    "Categoria Econômica Despesa": "categoria_economica",
    "Valor": "valor",
    "CNPJ do Favorecido": "cnpj_favorecido",
    "Nome Favorecido": "nome_favorecido",
}


def _parse_excel_date(date_val: str) -> str:
    """Convert Excel serial date (e.g. 42005) to ISO format."""
    if date_val.isdigit():
        with contextlib.suppress(Exception):
            dt = pd.to_datetime(
                int(date_val), unit="D", origin="1899-12-30"
            )
            return dt.strftime("%Y-%m-%d")
    return date_val


def _parse_brl_value(raw: str) -> float:
    """Parse a Brazilian-formatted value string to float."""
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return 0.0


class TesouroEmendasPipeline(Pipeline):
    """ETL pipeline for Tesouro Emendas."""

    name = "tesouro_emendas"
    source_id = "tesouro_emendas"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs,)

    def extract(self) -> pd.DataFrame:
        src_dir = Path(self.data_dir) / "tesouro_emendas"
        raw: pd.DataFrame = pd.DataFrame()
        csv_path = src_dir / "emendas_tesouro.csv"
        if not csv_path.exists():
            msg = f"Tesouro Emendas CSV not found: {csv_path}"
            raise FileNotFoundError(msg)

        raw = pd.read_csv(
            csv_path,
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
            chunksize=self.chunk_size,
        )
        logger.info(
            "[tesouro_emendas] Extracted %d records", len(raw),
        )
        return raw

    def transform(self, data: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        # Rename columns so itertuples() produces valid attributes
        df = data.rename(columns=_COL_RENAME)

        transfers: list[dict[str, Any]] = []
        companies: list[dict[str, Any]] = []
        transfer_rels: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}

        for row in df.itertuples(index=False):
            ob = str(getattr(row, "ob", "")).strip()
            if not ob:
                continue

            date_val = str(getattr(row, "data", "")).strip()
            formatted_date = _parse_excel_date(date_val)

            transfer_id = f"transfer_tesouro_{ob}"
            transfers.append({
                "transfer_id": transfer_id,
                "ob": ob,
                "date": formatted_date,
                "year": str(getattr(row, "ano", "")).strip(),
                "month": str(getattr(row, "mes", "")).strip(),
                "amendment_type": str(
                    getattr(row, "nome_emenda", "")
                ).strip(),
                "special_transfer": str(
                    getattr(row, "transferencia_especial", "")
                ).strip(),
                "economic_category": str(
                    getattr(row, "categoria_economica", "")
                ).strip(),
                "value": _parse_brl_value(
                    str(getattr(row, "valor", "")).strip()
                ),
                "source": self.source_id,
            })

            cnpj_raw = str(
                getattr(row, "cnpj_favorecido", "")
            ).strip()
            nome_fav = normalize_name(
                str(getattr(row, "nome_favorecido", ""))
            )

            cnpj = cnpj_raw.zfill(14) if cnpj_raw else ""
            if len(cnpj) == 14:
                companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome_fav,
                })
                transfer_rels.append({
                    "source_key": transfer_id,
                    "target_key": cnpj,
                })

            if self.limit and len(transfers) >= self.limit:
                break
        dict_result["transfers"] = deduplicate_rows(transfers, ["transfer_id"])
        dict_result["companies"] = deduplicate_rows(companies, ["cnpj"])
        dict_result["transfer_rels"] = transfer_rels

        logger.info(
            "[tesouro_emendas] Transformed %d transfers, %d companies",
            len(dict_result["transfers"]),
            len(dict_result["companies"]),
        )
        return dict_result

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data["transfers"]:
            loader.load_nodes(
                "Payment", data["transfers"], key_field="transfer_id",
            )

        if data["companies"]:
            loader.load_nodes(
                "Company", data["companies"], key_field="cnpj",
            )

        if data["transfer_rels"]:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Payment {transfer_id: row.source_key}) "
                "MATCH (c:Company {cnpj: row.target_key}) "
                "MERGE (p)-[:PAGO_PARA]->(c)"
            )
            loader.run_query(query, data["transfer_rels"])
