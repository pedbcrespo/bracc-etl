"""ETL pipeline for CPGF (Cartao de Pagamento do Governo Federal) data.

Ingests government credit card expense data from Portal da Transparencia.
Creates GovCardExpense nodes linked to Person (cardholder) via GASTOU_CARTAO.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Portal da Transparencia CPGF columns may have accented or unaccented names.
# Normalize to unaccented forms for reliable field access.
_COLUMN_ALIASES: dict[str, str] = {
    "CÓDIGO ÓRGÃO SUPERIOR": "CODIGO ORGAO SUPERIOR",
    "NOME ÓRGÃO SUPERIOR": "NOME ORGAO SUPERIOR",
    "CÓDIGO ÓRGÃO": "CODIGO ORGAO",
    "NOME ÓRGÃO": "NOME ORGAO",
    "CÓDIGO UNIDADE GESTORA": "CODIGO UNIDADE GESTORA",
    "MÊS EXTRATO": "MES EXTRATO",
    "TRANSAÇÃO": "TRANSACAO",
    "DATA TRANSAÇÃO": "DATA TRANSACAO",
    "VALOR TRANSAÇÃO": "VALOR TRANSACAO",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize accented column names to unaccented equivalents."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        stripped = col.strip()
        if stripped in _COLUMN_ALIASES:
            rename_map[col] = _COLUMN_ALIASES[stripped]
        elif stripped != col:
            rename_map[col] = stripped
    return df.rename(columns=rename_map) if rename_map else df


def _parse_brl_value(value: str) -> float:
    """Parse Brazilian numeric format (1.234,56) to float."""
    if not value or not value.strip():
        return 0.0
    cleaned = value.strip().replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _make_expense_id(cpf: str, date: str, amount: str, description: str) -> str:
    """Generate a stable expense ID from key fields."""
    raw = f"cpgf_{cpf}_{date}_{amount}_{description}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class CpgfPipeline(Pipeline):
    """ETL pipeline for CPGF (government credit card expenses)."""

    name = "cpgf"
    source_id = "cpgf"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.expenses: list[dict[str, Any]] = []
        self.cardholders: list[dict[str, Any]] = []
        self.gastou_cartao_rels: list[dict[str, Any]] = []

    def extract(self) -> pd.DataFrame:
        cpgf_dir = Path(self.data_dir) / "cpgf"
        csv_files = sorted(cpgf_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", cpgf_dir)
            return

        frames: list[pd.DataFrame] = []
        for f in csv_files:
            df = pd.read_csv(
                f,
                sep=";",
                dtype=str,
                encoding="latin-1",
                keep_default_na=False,
            )
            frames.append(df)
            logger.info("  Loaded %d rows from %s", len(df), f.name)

        raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        raw = _normalize_columns(raw)
        logger.info("Total raw rows: %d", len(raw))
        return raw

    def transform(self, data: pd.DataFrame) -> dict[str, Any]:
        if data:
            return {}

        expenses: list[dict[str, Any]] = []
        cardholders_map: dict[str, dict[str, Any]] = {}
        gastou_cartao: list[dict[str, Any]] = []
        skipped = 0

        for _, row in data.iterrows():
            cpf_raw = str(row.get("CPF PORTADOR", "")).strip()
            digits = strip_document(cpf_raw)

            cardholder_name = normalize_name(
                str(row.get("NOME PORTADOR", ""))
            )
            if not cardholder_name:
                skipped += 1
                continue

            # Use full CPF when available, otherwise keep masked format
            cpf_formatted = format_cpf(cpf_raw) if len(digits) == 11 else cpf_raw

            amount = _parse_brl_value(str(row.get("VALOR TRANSACAO", "")))
            if amount == 0.0:
                skipped += 1
                continue

            agency = str(row.get("NOME ORGAO SUPERIOR", "")).strip()
            date = parse_date(str(row.get("DATA TRANSACAO", "")))
            description = str(row.get("NOME FAVORECIDO", "")).strip()
            transaction_type = str(row.get("TRANSACAO", "")).strip()

            expense_id = _make_expense_id(
                cpf_formatted, date, str(amount), description
            )

            expenses.append({
                "expense_id": expense_id,
                "cardholder_name": cardholder_name,
                "cardholder_cpf": cpf_formatted,
                "agency": agency,
                "amount": amount,
                "date": date,
                "description": description,
                "transaction_type": transaction_type,
                "source": "cpgf",
            })

            # Only link to Person nodes when we have a full CPF
            if len(digits) == 11:
                cardholders_map[cpf_formatted] = {
                    "cpf": cpf_formatted,
                    "name": cardholder_name,
                }

                gastou_cartao.append({
                    "source_key": cpf_formatted,
                    "target_key": expense_id,
                })

            if self.limit and len(expenses) >= self.limit:
                break

        dict_result = {
            "expenses": deduplicate_rows(expenses, ["expense_id"]),
            "cardholders": list(cardholders_map.values()),
            "gastou_cartao_rels": gastou_cartao
        }

        logger.info(
            "Transformed: %d expenses, %d cardholders (skipped %d)",
            len(dict_result["expenses"]),
            len(dict_result["cardholders"]),
            skipped,
        )
        return dict_result

    def load(self, data:dict[str, Any]) -> None:
        expenses: list[dict[str, Any]] = data["expenses"]
        cardholders: list[dict[str, Any]] = data["cardholders"]
        gastou_cartao_rels: list[dict[str, Any]] = data["gastou_cartao_rels"]

        if not expenses:
            logger.warning("No expenses to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # Load GovCardExpense nodes
        count = loader.load_nodes(
            "GovCardExpense", expenses, key_field="expense_id"
        )
        logger.info("Loaded %d GovCardExpense nodes", count)

        # Merge Person nodes for cardholders
        if cardholders:
            count = loader.load_nodes("Person", cardholders, key_field="cpf")
            logger.info("Merged %d cardholder Person nodes", count)

        # GASTOU_CARTAO: Person -> GovCardExpense
        if gastou_cartao_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (e:GovCardExpense {expense_id: row.target_key}) "
                "MERGE (p)-[:GASTOU_CARTAO]->(e)"
            )
            count = loader.run_query_with_retry(query, gastou_cartao_rels)
            logger.info("Created %d GASTOU_CARTAO relationships", count)
