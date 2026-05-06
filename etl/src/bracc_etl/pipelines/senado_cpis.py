from __future__ import annotations

import hashlib
import logging
import re
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

_CNPJ_FMT_RE = re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}")
_CNPJ_RAW_RE = re.compile(r"\d{14}")
_TEMPORAL_RULE = (
    "event_date>=inquiry.date_start and "
    "(inquiry.date_end is null or event_date<=inquiry.date_end)"
)


def _stable_id(*parts: str, length: int = 20) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _make_cpi_id(code: str, name: str) -> str:
    """Deterministic CPI ID for backward compatibility."""
    return _stable_id(code, name, length=16)


def _infer_kind(name: str, explicit_kind: str = "") -> str:
    kind = explicit_kind.strip().upper()
    if kind in {"CPI", "CPMI"}:
        return kind
    if "CPMI" in name.upper():
        return "CPMI"
    return "CPI"


def _extract_cnpjs(text: str) -> list[str]:
    formatted = _CNPJ_FMT_RE.findall(text)
    raw = _CNPJ_RAW_RE.findall(text)

    seen: set[str] = set()
    out: list[str] = []

    for match in formatted:
        digits = strip_document(match)
        if len(digits) == 14 and digits not in seen:
            seen.add(digits)
            out.append(format_cnpj(match))

    for match in raw:
        if len(match) == 14 and match not in seen:
            seen.add(match)
            out.append(format_cnpj(match))

    return out


def _temporal_status(event_date: str, start_date: str, end_date: str) -> str:
    if not event_date or not start_date:
        return "unknown"
    if event_date < start_date:
        return "invalid"
    if end_date and event_date > end_date:
        return "invalid"
    return "valid"


class SenadoCpisPipeline(Pipeline):
    """ETL pipeline for Senate inquiries (CPI/CPMI), v2.

    Input directory: data/senado_cpis/

    Supported files:
    - inquiries.csv (preferred v2)
    - cpis.csv (legacy fallback)
    - requirements.csv
    - sessions.csv
    - members.csv

    Compatibility:
    - Still creates :CPI nodes for existing consumers.
    - Adds richer model: :Inquiry, :InquiryRequirement, :InquirySession.
    """

    name = "senado_cpis"
    source_id = "senado_cpis"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)
        self.run_id = f"{self.name}_{pd.Timestamp.utcnow().strftime('%Y%m%d%H%M%S')}"

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=self.chunk_size)
        except pd.errors.EmptyDataError:
            logger.info("[senado_cpis] empty file (treated as no data): %s", path.name)
            return pd.DataFrame()

    def extract(self) -> dict[str, pd.DataFrame]:
        src_dir = Path(self.data_dir) / "senado_cpis"

        dict_result: dict[str, pd.DataFrame] = {}

        if not src_dir.exists():
            logger.warning("[senado_cpis] data dir not found: %s", src_dir)
            return {}

        inquiries_csv = src_dir / "inquiries.csv"
        legacy_csv = src_dir / "cpis.csv"

        if inquiries_csv.exists():
            dict_result["raw_inquiries"] = self._read_csv_optional(inquiries_csv)
        elif legacy_csv.exists():
            dict_result["raw_inquiries"] = self._read_csv_optional(legacy_csv)
        else:
            logger.warning("[senado_cpis] inquiries.csv/cpis.csv not found in %s", src_dir)
            return {}

        dict_result["raw_requirements"] = self._read_csv_optional(src_dir / "requirements.csv")
        dict_result["raw_sessions"] = self._read_csv_optional(src_dir / "sessions.csv")
        dict_result["raw_members"] = self._read_csv_optional(src_dir / "members.csv")
        dict_result["raw_history_sources"] = self._read_csv_optional(src_dir / "history_sources.csv")

        if self.limit:
            for key, df in dict_result.items():
                dict_result[key] = df.head(self.limit)
        dict_result["raw"] = dict_result["raw_inquiries"]

        logger.info(
            "[senado_cpis] extracted inquiries=%d requirements=%d sessions=%d members=%d",
            len(dict_result["raw_inquiries"]),
            len(dict_result["raw_requirements"]),
            len(dict_result["raw_sessions"]),
            len(dict_result["raw_members"]),
        )
        return dict_result

    def transform(self, data: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
        raw_inquiries = data.get("raw_inquiries", pd.DataFrame())
        raw = data.get("raw", pd.DataFrame())
        raw_requirements = data.get("raw_requirements", pd.DataFrame())
        dict_result = {}
        if raw_inquiries.empty and not raw.empty:
            # Legacy compatibility for tests/callers that pre-fill raw.
            raw_inquiries = raw

        if raw_inquiries.empty:
            return

        dict_result, inquiry_date_lookup = self._transform_inquiries(raw_inquiries)
        dict_result.update(self._transform_members(data, dict_result["cpis"]))
        dict_result.update(self._transform_requirements(raw_requirements, inquiry_date_lookup))
        dict_result.update(self._transform_sessions(data.get("raw_sessions", pd.DataFrame()), inquiry_date_lookup))
        dict_result.update(self._transform_source_documents(data.get("raw_history_sources", pd.DataFrame())))
        return dict_result

    def _get_inquiry_value(self, row: pd.Series, *keys: str) -> str:
        for key in keys:
            value = str(row.get(key, "")).strip()
            if value:
                return value
        return ""

    def _transform_inquiries(self, raw_inquiries: pd.DataFrame) -> tuple[dict[str, list[dict[str, Any]]], dict[str, tuple[str, str]]]:
        inquiries: list[dict[str, Any]] = []
        cpis: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}
        inquiry_date_lookup: dict[str, tuple[str, str]]

        for _, row in raw_inquiries.iterrows():
            code = self._get_inquiry_value(row, "inquiry_code", "codigo", "codigo_cpi")
            name = self._get_inquiry_value(row, "name", "nome", "nome_cpi")
            if not name:
                continue

            kind = _infer_kind(name, self._get_inquiry_value(row, "kind", "tipo"))
            house = self._get_inquiry_value(row, "house", "casa") or "senado"
            status = self._get_inquiry_value(row, "status", "situacao")
            subject = self._get_inquiry_value(row, "subject", "objeto")
            source_url = self._get_inquiry_value(row, "source_url", "url")
            source_system = self._get_inquiry_value(row, "source_system")
            extraction_method = self._get_inquiry_value(row, "extraction_method")
            source_ref = self._get_inquiry_value(row, "source_ref")
            date_precision = self._get_inquiry_value(row, "date_precision") or "unknown"
            date_start = parse_date(self._get_inquiry_value(row, "date_start", "data_inicio"))
            date_end = parse_date(self._get_inquiry_value(row, "date_end", "data_fim"))

            inquiry_id = self._get_inquiry_value(row, "inquiry_id")
            if not inquiry_id:
                inquiry_id = _stable_id(code, name)

            inquiry = {
                "inquiry_id": inquiry_id,
                "code": code,
                "name": name,
                "kind": kind,
                "house": house,
                "status": status,
                "subject": subject,
                "date_start": date_start,
                "date_end": date_end,
                "source_url": source_url,
                "source": "senado_cpis",
                "source_system": source_system,
                "extraction_method": extraction_method,
                "source_ref": source_ref,
                "date_precision": date_precision,
                "run_id": self.run_id,
            }
            inquiries.append(inquiry)

            cpi_id = _make_cpi_id(code or inquiry_id, name)
            cpis.append({
                "cpi_id": cpi_id,
                "code": code,
                "name": name,
                "date_start": date_start,
                "date_end": date_end,
                "subject": subject,
                "source": "senado_cpis",
                "inquiry_id": inquiry_id,
                "kind": kind,
                "house": house,
            })

        dict_result["inquiries"] = deduplicate_rows(inquiries, ["inquiry_id"])
        dict_result["cpis"] = deduplicate_rows(cpis, ["cpi_id"])
        inquiry_date_lookup = {
            str(row.get("inquiry_id", "")): (
                str(row.get("date_start", "")).strip(),
                str(row.get("date_end", "")).strip(),
            )
            for row in dict_result["inquiries"]
        }

        return dict_result, inquiry_date_lookup

    def _transform_members(self, data: dict[str, pd.DataFrame], cpis: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        rows: list[dict[str, Any]] = []
        raw_members = data.get("raw_members", pd.DataFrame())
        raw_inquiries = data.get("raw_inquiries", pd.DataFrame())
        dict_result: dict[str, list[dict[str, Any]]]
        # Legacy fallback: member info embedded in cpis.csv row.
        source = raw_members if not raw_members.empty else raw_inquiries

        for _, row in source.iterrows():
            inquiry_id = self._get_inquiry_value(row, "inquiry_id")
            if not inquiry_id:
                code = self._get_inquiry_value(row, "inquiry_code", "codigo", "codigo_cpi")
                name = self._get_inquiry_value(row, "name", "nome", "nome_cpi")
                if not name:
                    continue
                inquiry_id = _stable_id(code, name)

            person_name = normalize_name(
                self._get_inquiry_value(row, "member_name", "nome_parlamentar", "name")
            )
            if not person_name:
                continue

            role = self._get_inquiry_value(row, "role", "papel")

            rows.append({
                "inquiry_id": inquiry_id,
                "person_name": person_name,
                "role": role,
            })

        cpi_lookup = {c["inquiry_id"]: c["cpi_id"] for c in cpis}
        
        dict_result = {
            "inquiry_member_rels": rows,
            "senator_rels": [
                {
                    "senator_name": r["person_name"],
                    "cpi_id": cpi_lookup.get(r["inquiry_id"], ""),
                    "role": r["role"],
                }
                for r in rows
                if cpi_lookup.get(r["inquiry_id"])
            ]
        }
        return dict_result

    def _transform_requirements(self, raw_requirements: pd.DataFrame, inquiry_date_lookup: dict[str, tuple[str, str]]) -> dict[str, list[dict[str, Any]]]:
        if raw_requirements.empty:
            return {}

        requirements: list[dict[str, Any]] = []
        inquiry_rels: list[dict[str, Any]] = []
        author_cpf_rels: list[dict[str, Any]] = []
        author_name_rels: list[dict[str, Any]] = []
        mentions: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}
        temporal_violations: list[dict[str, Any]] = []
        for _, row in raw_requirements.iterrows():
            inquiry_id = self._get_inquiry_value(row, "inquiry_id")
            if not inquiry_id:
                continue

            requirement_id = self._get_inquiry_value(row, "requirement_id", "codigo", "id")
            req_type = self._get_inquiry_value(row, "type", "tipo")
            text = self._get_inquiry_value(row, "text", "texto", "ementa")
            status = self._get_inquiry_value(row, "status", "situacao")
            source_url = self._get_inquiry_value(row, "source_url", "url")
            source_system = self._get_inquiry_value(row, "source_system")
            extraction_method = self._get_inquiry_value(row, "extraction_method")
            source_ref = self._get_inquiry_value(row, "source_ref")
            date_precision = self._get_inquiry_value(row, "date_precision") or "unknown"
            date = parse_date(self._get_inquiry_value(row, "date", "data"))

            if not requirement_id:
                requirement_id = _stable_id(inquiry_id, req_type, text[:200])

            requirements.append({
                "requirement_id": requirement_id,
                "type": req_type,
                "date": date,
                "text": text,
                "status": status,
                "source_url": source_url,
                "source": "senado_cpis",
                "source_system": source_system,
                "extraction_method": extraction_method,
                "source_ref": source_ref,
                "date_precision": date_precision,
                "run_id": self.run_id,
            })
            start_date, end_date = inquiry_date_lookup.get(inquiry_id, ("", ""))
            temporal_status = _temporal_status(date, start_date, end_date)

            inquiry_rels.append({
                "source_key": inquiry_id,
                "target_key": requirement_id,
                "event_date": date,
                "temporal_status": temporal_status,
                "temporal_rule": _TEMPORAL_RULE,
            })
            if temporal_status == "invalid":
                temporal_violations.append({
                    "violation_id": _stable_id("req", inquiry_id, requirement_id, date),
                    "edge_type": "TEM_REQUERIMENTO",
                    "rule": _TEMPORAL_RULE,
                    "event_date": date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "source_id": self.source_id,
                    "run_id": self.run_id,
                })

            author_cpf_raw = self._get_inquiry_value(row, "author_cpf", "cpf_autor")
            author_digits = strip_document(author_cpf_raw)
            if len(author_digits) == 11:
                author_cpf_rels.append({
                    "source_key": format_cpf(author_digits),
                    "target_key": requirement_id,
                })
            # Do not infer factual author->requirement edges from name-only rows.
            # Name is preserved on InquiryRequirement for exploratory analysis.

            explicit_mentioned = self._get_inquiry_value(row, "mentioned_cnpj", "cnpj")
            explicit_digits = strip_document(explicit_mentioned)
            if len(explicit_digits) == 14:
                mentions.append({
                    "cnpj": format_cnpj(explicit_digits),
                    "target_key": requirement_id,
                    "method": "cnpj_explicit",
                    "confidence": 1.0,
                    "source_ref": source_url or requirement_id,
                    "run_id": self.run_id,
                })

            for cnpj in _extract_cnpjs(text):
                mentions.append({
                    "cnpj": cnpj,
                    "target_key": requirement_id,
                    "method": "text_cnpj_extract",
                    "confidence": 0.8,
                    "source_ref": source_url or requirement_id,
                    "run_id": self.run_id,
                })

        return {
            "inquiry_requirements": deduplicate_rows(requirements, ["requirement_id"]),
            "inquiry_requirement_rels": inquiry_rels,
            "requirement_author_cpf_rels": author_cpf_rels,
            "requirement_author_name_rels": author_name_rels,
            "requirement_company_mentions": deduplicate_rows(
                mentions,
                ["cnpj", "target_key", "method"],
            ),
            "temporal_violations": deduplicate_rows(temporal_violations, ["violation_id"]),
        }

    def _transform_sessions(self, raw_sessions: pd.DataFrame, inquiry_date_lookup: dict[str, tuple[str, str]]) -> dict[str, list[dict[str, Any]]]:
        if raw_sessions.empty:
            return

        sessions: list[dict[str, Any]] = []
        session_rels: list[dict[str, Any]] = []
        temporal_violations: list[dict[str, Any]] = []

        for _, row in raw_sessions.iterrows():
            inquiry_id = self._get_inquiry_value(row, "inquiry_id")
            if not inquiry_id:
                continue

            session_id = self._get_inquiry_value(row, "session_id", "id")
            date = parse_date(self._get_inquiry_value(row, "date", "data"))
            topic = self._get_inquiry_value(row, "topic", "assunto")
            source_url = self._get_inquiry_value(row, "source_url", "url")
            source_system = self._get_inquiry_value(row, "source_system")
            extraction_method = self._get_inquiry_value(row, "extraction_method")
            source_ref = self._get_inquiry_value(row, "source_ref")
            date_precision = self._get_inquiry_value(row, "date_precision") or "unknown"

            if not session_id:
                session_id = _stable_id(inquiry_id, date, topic[:200])

            sessions.append({
                "session_id": session_id,
                "date": date,
                "topic": topic,
                "source_url": source_url,
                "source": "senado_cpis",
                "source_system": source_system,
                "extraction_method": extraction_method,
                "source_ref": source_ref,
                "date_precision": date_precision,
                "run_id": self.run_id,
            })
            start_date, end_date = inquiry_date_lookup.get(inquiry_id, ("", ""))
            temporal_status = _temporal_status(date, start_date, end_date)

            session_rels.append({
                "source_key": inquiry_id,
                "target_key": session_id,
                "event_date": date,
                "temporal_status": temporal_status,
                "temporal_rule": _TEMPORAL_RULE,
            })
            if temporal_status == "invalid":
                temporal_violations.append({
                    "violation_id": _stable_id("sess", inquiry_id, session_id, date),
                    "edge_type": "REALIZOU_SESSAO",
                    "rule": _TEMPORAL_RULE,
                    "event_date": date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "source_id": self.source_id,
                    "run_id": self.run_id,
                })
        return {
            "inquiry_sessions": deduplicate_rows(sessions, ["session_id"]),
            "inquiry_session_rels": session_rels,
            "temporal_violations": deduplicate_rows(temporal_violations, ["violation_id"]),
        }

    def _transform_source_documents(self, raw_history_sources: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        if raw_history_sources.empty:
            return {"source_documents": []}

        documents: list[dict[str, Any]] = []
        for _, row in raw_history_sources.iterrows():
            url = self._get_inquiry_value(row, "source_url", "url")
            checksum = self._get_inquiry_value(row, "checksum")
            if not url:
                continue
            doc_id = _stable_id(url, checksum or "", length=24)
            documents.append({
                "doc_id": doc_id,
                "url": url,
                "checksum": checksum,
                "published_at": self._get_inquiry_value(row, "period_end"),
                "retrieved_at": self._get_inquiry_value(row, "retrieved_at_utc"),
                "content_type": self._get_inquiry_value(row, "doc_type") or "application/pdf",
                "source_id": self.source_id,
                "run_id": self.run_id,
            })
        return {
            "source_documents": deduplicate_rows(documents, ["doc_id"]),
        }

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)
        inquiries = data.get("inquiries", [])
        cpis = data.get("cpis", [])
        inquiry_requirements = data.get("inquiry_requirements", [])
        inquiry_sessions = data.get("inquiry_sessions", [])
        inquiry_requirement_rels = data.get("inquiry_requirement_rels", [])
        inquiry_session_rels = data.get("inquiry_session_rels", [])
        inquiry_member_rels = data.get("inquiry_member_rels", [])
        senator_rels = data.get("senator_rels", [])
        requirement_author_cpf_rels = data.get("requirement_author_cpf_rels", [])
        requirement_author_name_rels = data.get("requirement_author_name_rels", [])
        requirement_company_mentions = data.get("requirement_company_mentions", []) 
        source_documents = data.get("source_documents", [])
        temporal_violations = data.get("temporal_violations", [])

        if inquiries:
            count = loader.load_nodes("Inquiry", inquiries, key_field="inquiry_id")
            logger.info("[senado_cpis] loaded %d Inquiry nodes", count)

        if cpis:
            count = loader.load_nodes("CPI", cpis, key_field="cpi_id")
            logger.info("[senado_cpis] loaded %d CPI nodes", count)

            # Explicit compatibility bridge between old and new labels.
            bridge_rows = [
                {"source_key": row["cpi_id"], "target_key": row["inquiry_id"]}
                for row in cpis
                if row.get("inquiry_id")
            ]
            if bridge_rows:
                loader.load_relationships(
                    rel_type="EH_INQUIRY",
                    rows=bridge_rows,
                    source_label="CPI",
                    source_key="cpi_id",
                    target_label="Inquiry",
                    target_key="inquiry_id",
                )

        if inquiry_requirements:
            count = loader.load_nodes(
                "InquiryRequirement",
                inquiry_requirements,
                key_field="requirement_id",
            )
            logger.info("[senado_cpis] loaded %d InquiryRequirement nodes", count)

        if inquiry_sessions:
            count = loader.load_nodes(
                "InquirySession",
                inquiry_sessions,
                key_field="session_id",
            )
            logger.info("[senado_cpis] loaded %d InquirySession nodes", count)

        if inquiry_requirement_rels:
            loader.load_relationships(
                rel_type="TEM_REQUERIMENTO",
                rows=inquiry_requirement_rels,
                source_label="Inquiry",
                source_key="inquiry_id",
                target_label="InquiryRequirement",
                target_key="requirement_id",
                properties=["event_date", "temporal_status", "temporal_rule"],
            )

        if inquiry_session_rels:
            loader.load_relationships(
                rel_type="REALIZOU_SESSAO",
                rows=inquiry_session_rels,
                source_label="Inquiry",
                source_key="inquiry_id",
                target_label="InquirySession",
                target_key="session_id",
                properties=["event_date", "temporal_status", "temporal_rule"],
            )

        if inquiry_member_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person) WHERE p.name = row.person_name "
                "MATCH (i:Inquiry {inquiry_id: row.inquiry_id}) "
                "MERGE (p)-[r:PARTICIPA_INQUIRY]->(i) "
                "SET r.role = row.role"
            )
            loader.run_query_with_retry(query, inquiry_member_rels)

        if senator_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person) WHERE p.name = row.senator_name "
                "MATCH (c:CPI {cpi_id: row.cpi_id}) "
                "MERGE (p)-[r:PARTICIPOU_CPI]->(c) "
                "SET r.role = row.role"
            )
            loader.run_query_with_retry(query, senator_rels)

        if requirement_author_cpf_rels:
            loader.load_relationships(
                rel_type="PROPOS_REQUERIMENTO",
                rows=requirement_author_cpf_rels,
                source_label="Person",
                source_key="cpf",
                target_label="InquiryRequirement",
                target_key="requirement_id",
            )

        if requirement_author_name_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person) WHERE p.name = row.person_name "
                "MATCH (r:InquiryRequirement {requirement_id: row.target_key}) "
                "MERGE (p)-[:PROPOS_REQUERIMENTO]->(r)"
            )
            loader.run_query_with_retry(query, requirement_author_name_rels)

        if requirement_company_mentions:
            companies = deduplicate_rows(
                [
                    {
                        "cnpj": row["cnpj"],
                        "razao_social": row.get("cnpj", ""),
                    }
                    for row in requirement_company_mentions
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", companies, key_field="cnpj")

            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.cnpj}) "
                "MATCH (r:InquiryRequirement {requirement_id: row.target_key}) "
                "MERGE (c)-[m:MENCIONADA_EM]->(r) "
                "SET m.method = row.method, "
                "m.confidence = row.confidence, "
                "m.source_ref = row.source_ref, "
                "m.run_id = row.run_id"
            )
            loader.run_query_with_retry(query, requirement_company_mentions)

        if temporal_violations:
            count = loader.load_nodes(
                "TemporalViolation",
                deduplicate_rows(temporal_violations, ["violation_id"]),
                key_field="violation_id",
            )
            logger.info("[senado_cpis] loaded %d TemporalViolation nodes", count)

        if source_documents:
            count = loader.load_nodes(
                "SourceDocument",
                source_documents,
                key_field="doc_id",
            )
            logger.info("[senado_cpis] loaded %d SourceDocument nodes", count)
