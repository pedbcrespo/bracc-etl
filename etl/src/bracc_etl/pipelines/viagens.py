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
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# Portal da Transparencia Viagens CSVs use semicolon separators and
# mixed-case Portuguese column headers. Map to canonical names.
_COLUMN_ALIASES: dict[str, str] = {
    # Actual Portal da Transparencia *_Viagem.csv headers (with accents)
    "CÓDIGO DO ÓRGÃO SUPERIOR": "cod_orgao_superior",
    "NOME DO ÓRGÃO SUPERIOR": "nome_orgao_superior",
    "CÓDIGO ÓRGÃO SOLICITANTE": "cod_orgao",
    "NOME ÓRGÃO SOLICITANTE": "nome_orgao",
    "CPF VIAJANTE": "cpf",
    "NOME": "nome",
    "CARGO": "cargo",
    "FUNÇÃO": "funcao",
    "DESCRIÇÃO FUNÇÃO": "descricao_funcao",
    "PERÍODO - DATA DE INÍCIO": "data_inicio",
    "PERÍODO - DATA DE FIM": "data_fim",
    "DESTINOS": "destinos",
    "MOTIVO": "motivo",
    "VALOR DIÁRIAS": "valor_diarias",
    "VALOR PASSAGENS": "valor_passagens",
    "VALOR DEVOLUÇÃO": "valor_devolucao",
    "VALOR OUTROS GASTOS": "valor_outros",
    # Variants without accents
    "CODIGO DO ORGAO SUPERIOR": "cod_orgao_superior",
    "NOME DO ORGAO SUPERIOR": "nome_orgao_superior",
    "CODIGO ORGAO SOLICITANTE": "cod_orgao",
    "NOME ORGAO SOLICITANTE": "nome_orgao",
    "FUNCAO": "funcao",
    "DESCRICAO FUNCAO": "descricao_funcao",
    "PERIODO - DATA DE INICIO": "data_inicio",
    "PERIODO - DATA DE FIM": "data_fim",
    "VALOR DEVOLUCAO": "valor_devolucao",
    # Legacy aliases (download script pre-mapped names, kept for compatibility)
    "CÓDIGO ÓRGÃO": "cod_orgao",
    "NOME ÓRGÃO": "nome_orgao",
    "CPF SERVIDOR": "cpf",
    "NOME SERVIDOR": "nome",
    "CODIGO ORGAO": "cod_orgao",
    "NOME ORGAO": "nome_orgao",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names using alias lookup (case-insensitive)."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        col_upper = col.strip().upper()
        if col_upper in _COLUMN_ALIASES:
            rename_map[col] = _COLUMN_ALIASES[col_upper]
        elif col.strip() in _COLUMN_ALIASES.values():
            rename_map[col] = col.strip()
    return df.rename(columns=rename_map)


def _parse_money(value: str) -> float:
    """Parse Brazilian money format (1.234,56) to float."""
    if not value or not value.strip():
        return 0.0
    cleaned = value.strip().replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _make_travel_id(cpf: str, destination: str, start_date: str, amount: float) -> str:
    """Generate deterministic travel_id from key fields."""
    raw = f"{cpf}|{destination}|{start_date}|{amount:.2f}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class ViagensPipeline(Pipeline):
    """ETL pipeline for Government Travel (Viagens a Servico) data."""

    name = "viagens"
    source_id = "portal_transparencia_viagens"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> pd.DataFrame:
        viagens_dir = Path(self.data_dir) / "viagens"
        csv_files = sorted(viagens_dir.glob("*.csv"))
        raw: pd.DataFrame = pd.DataFrame()
        if not csv_files:
            msg = f"No CSV files found in {viagens_dir}"
            raise FileNotFoundError(msg)

        frames: list[pd.DataFrame] = []
        for csv_path in csv_files:
            try:
                df = pd.read_csv(
                    csv_path,
                    dtype=str,
                    delimiter=";",
                    encoding="latin-1",
                    keep_default_na=False,
                    chunksize=self.chunk_size
                )
                df = _normalize_columns(df)
                frames.append(df)
                logger.info("[viagens] Read %d rows from %s", len(df), csv_path.name)
            except Exception:
                logger.warning("[viagens] Failed to read %s", csv_path.name)

        if frames:
            raw = pd.concat(frames, ignore_index=True)
        logger.info("[viagens] Extracted %d total rows", len(raw))
        return raw

    def transform(self, data: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        travels: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}
        for _, row in data.iterrows():
            cpf_raw = str(row.get("cpf", "")).strip()
            digits = strip_document(cpf_raw)

            nome = normalize_name(str(row.get("nome", "")))
            if not nome:
                continue

            # Use full CPF when available, otherwise keep masked format
            cpf_formatted = format_cpf(cpf_raw) if len(digits) == 11 else cpf_raw

            agency = str(row.get("nome_orgao", "")).strip()
            destination = str(row.get("destinos", "")).strip()
            start_date = parse_date(str(row.get("data_inicio", "")))
            end_date = parse_date(str(row.get("data_fim", "")))
            justification = str(row.get("motivo", "")).strip()

            valor_diarias = _parse_money(str(row.get("valor_diarias", "")))
            valor_passagens = _parse_money(str(row.get("valor_passagens", "")))
            valor_outros = _parse_money(str(row.get("valor_outros", "")))
            amount = round(valor_diarias + valor_passagens + valor_outros, 2)

            travel_id = _make_travel_id(cpf_formatted, destination, start_date, amount)

            travels.append({
                "travel_id": travel_id,
                "traveler_name": nome,
                "traveler_cpf": cpf_formatted,
                "agency": agency,
                "destination": destination,
                "start_date": start_date,
                "end_date": end_date,
                "amount": amount,
                "justification": justification,
                "source": "portal_transparencia_viagens",
            })

            # Only link to Person nodes when we have a full CPF
            if len(digits) == 11:
                person_rels.append({
                    "source_key": cpf_formatted,
                    "target_key": travel_id,
                    "person_name": nome,
                })

            if self.limit and len(travels) >= self.limit:
                break
        dict_result = {
            "travels": deduplicate_rows(travels, ["travel_id"]),
            "person_rels": person_rels,
        }
        logger.info(
            "[viagens] Transformed %d travel records, %d person links",
            len(dict_result["travels"]),
            len(dict_result["person_rels"]),
        )
        return dict_result

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if data["travels"]:
            loaded = loader.load_nodes("GovTravel", data["travels"], key_field="travel_id")
            logger.info("[viagens] Loaded %d GovTravel nodes", loaded)

        if data["person_rels"]:
            query = (
                "UNWIND $rows AS row "
                "MERGE (p:Person {cpf: row.source_key}) "
                "ON CREATE SET p.name = row.person_name "
                "WITH p, row "
                "MATCH (t:GovTravel {travel_id: row.target_key}) "
                "MERGE (p)-[:VIAJOU]->(t)"
            )
            loaded = loader.run_query_with_retry(query, data["person_rels"])
            logger.info("[viagens] Loaded %d VIAJOU relationships", loaded)
