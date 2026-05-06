from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_date,
    strip_document,
)


def _parse_brl(value: str | None) -> float:
    """Parse Brazilian monetary string to float (e.g. '1.234.567,89')."""
    if not value:
        return 0.0
    cleaned = str(value).strip()
    cleaned = re.sub(r"[R$\s]", "", cleaned)
    if not cleaned:
        return 0.0
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


class TransferegovPipeline(Pipeline):
    """ETL pipeline for TransfereGov parliamentary amendments data.

    Sources: Portal da Transparência emendas parlamentares bulk download.
    Three CSV files:
    - EmendasParlamentares.csv: amendments with authors, functions, municipalities
    - EmendasParlamentares_PorFavorecido.csv: who received the money (companies/persons)
    - EmendasParlamentares_Convenios.csv: convênios linked to amendments
    """

    name = "transferegov"
    source_id = "transferegov"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, **kwargs)

    def extract(self) -> dict[str, pd.DataFrame]:
        src_dir = Path(self.data_dir) / "transferegov"
        dict_result: dict[str, pd.DataFrame] = {}
        dict_result["raw_emendas"] = pd.read_csv(
            src_dir / "EmendasParlamentares.csv",
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
            chunksize=self.chunk_size,
        )
        dict_result["raw_favorecidos"] = pd.read_csv(
            src_dir / "EmendasParlamentares_PorFavorecido.csv",
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
            chunksize=self.chunk_size,
        )
        dict_result["raw_convenios"] = pd.read_csv(
            src_dir / "EmendasParlamentares_Convenios.csv",
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
            chunksize=self.chunk_size,
        )
        return dict_result

    def transform(self, data: dict[str, pd.DataFrame]) -> None:
        dict_result: dict[str, list[dict[str, Any]]] = {}

        dict_result.update(self._transform_amendments(data["raw_emendas"]))
        dict_result.update(self._transform_favorecidos(data["raw_favorecidos"]))
        dict_result.update(self._transform_convenios(data["raw_convenios"]))

        return dict_result

    def _transform_amendments(self, raw_emendas: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        """Transform main amendments file: Amendment nodes + Person authors."""
        amendments: list[dict[str, Any]] = []
        authors: list[dict[str, Any]] = []
        author_rels: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}

        # Group by amendment code to aggregate values
        grouped = raw_emendas.groupby("Código da Emenda")

        for code, group in grouped:
            code_str = str(code).strip()
            if not code_str or code_str == "Sem informação":
                continue

            first = group.iloc[0]
            author_code = str(first["Código do Autor da Emenda"]).strip()
            author_name = normalize_name(str(first["Nome do Autor da Emenda"]))
            emenda_type = str(first["Tipo de Emenda"]).strip()
            function_name = normalize_name(str(first["Nome Função"]))
            municipality = str(first["Município"]).strip()
            uf = str(first["UF"]).strip()

            # Sum values across all rows for this amendment
            value_empenhado = sum(
                _parse_brl(str(r["Valor Empenhado"]))
                for _, r in group.iterrows()
            )
            value_pago = sum(
                _parse_brl(str(r["Valor Pago"]))
                for _, r in group.iterrows()
            )

            amendments.append({
                "amendment_id": code_str,
                "type": emenda_type,
                "function": function_name,
                "municipality": municipality,
                "uf": uf,
                "value_committed": value_empenhado,
                "value_paid": value_pago,
            })

            # Author relationship
            if author_code and author_code != "S/I":
                authors.append({
                    "author_key": author_code,
                    "name": author_name,
                })
                author_rels.append({
                    "source_key": author_code,
                    "target_key": code_str,
                })
        dict_result = {
            "amendments": deduplicate_rows(amendments, ["amendment_id"]),
            "authors": deduplicate_rows(authors, ["author_key"]),
            "author_rels": author_rels,
        }
        return dict_result

    def _transform_favorecidos(self, raw_favorecidos: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        """Transform favorecidos: companies/persons receiving amendment funds."""
        companies: list[dict[str, Any]] = []
        persons: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}

        for _, row in raw_favorecidos.iterrows():
            emenda_code = str(row["Código da Emenda"]).strip()
            if not emenda_code or emenda_code == "Sem informação":
                continue

            doc_raw = str(row["Código do Favorecido"]).strip()
            digits = strip_document(doc_raw)
            tipo = str(row["Tipo Favorecido"]).strip()
            nome = normalize_name(str(row["Favorecido"]))
            valor = _parse_brl(str(row["Valor Recebido"]))
            municipio = str(row["Município Favorecido"]).strip()
            uf = str(row["UF Favorecido"]).strip()

            if tipo == "Pessoa Jurídica" and len(digits) == 14:
                cnpj = format_cnpj(doc_raw)
                companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome,
                })
                rels.append({
                    "amendment_id": emenda_code,
                    "doc": cnpj,
                    "entity_type": "Company",
                    "doc_field": "cnpj",
                    "value": valor,
                    "municipality": municipio,
                    "uf": uf,
                })
            elif tipo == "Pessoa Fisica" and len(digits) == 11:
                # Individual CPFs — we don't store raw CPFs for non-PEPs,
                # but we still create Person nodes for graph linkage
                from bracc_etl.transforms import format_cpf

                cpf = format_cpf(doc_raw)
                persons.append({
                    "cpf": cpf,
                    "name": nome,
                })
                rels.append({
                    "amendment_id": emenda_code,
                    "doc": cpf,
                    "entity_type": "Person",
                    "doc_field": "cpf",
                    "value": valor,
                    "municipality": municipio,
                    "uf": uf,
                })
            # Skip Unidade Gestora, Inscrição Genérica, Inválido
        dict_result = {
            "favorecido_companies": deduplicate_rows(companies, ["cnpj"]),
            "favorecido_persons": deduplicate_rows(persons, ["cpf"]),
            "favorecido_rels": rels,
        }

        return dict_result

    def _transform_convenios(self, raw_convenios: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        """Transform convênios linked to amendments."""
        convenios: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []
        dict_result: dict[str, list[dict[str, Any]]] = {}

        for _, row in raw_convenios.iterrows():
            emenda_code = str(row["Código da Emenda"]).strip()
            if not emenda_code or emenda_code == "Sem informação":
                continue

            numero = str(row["Número Convênio"]).strip()
            if not numero:
                continue

            convenente = normalize_name(str(row["Convenente"]))
            objeto = normalize_name(str(row["Objeto Convênio"]))
            valor = _parse_brl(str(row["Valor Convênio"]))
            data_pub = parse_date(str(row["Data Publicação Convênio"]))
            funcao = normalize_name(str(row["Nome Função"]))

            convenios.append({
                "convenio_id": numero,
                "convenente": convenente,
                "object": objeto,
                "value": valor,
                "date_published": data_pub,
                "function": funcao,
            })

            rels.append({
                "source_key": emenda_code,
                "target_key": numero,
            })
        dict_result = {
            "convenios": deduplicate_rows(convenios, ["convenio_id"]),
            "convenio_rels": rels,
        }
        return dict_result

    def load(self, data: dict[str, list[dict[str, Any]]]) -> None:
        loader = Neo4jBatchLoader(self.driver)
        amendments = data.get("amendments", [])
        authors = data.get("authors", [])
        author_rels = data.get("author_rels", [])
        favorecido_companies = data.get("favorecido_companies", [])
        favorecido_persons = data.get("favorecido_persons", [])
        favorecido_rels = data.get("favorecido_rels", [])
        convenios = data.get("convenios", [])
        convenio_rels = data.get("convenio_rels", [])

        # 1. Amendment nodes
        if amendments:
            loader.load_nodes("Amendment", amendments, key_field="amendment_id")

        # 2. Person nodes for authors (keyed by author_key for entity resolution)
        if authors:
            loader.load_nodes("Person", authors, key_field="author_key")

        # 3. Person -[:AUTOR_EMENDA]-> Amendment
        if author_rels:
            loader.load_relationships(
                rel_type="AUTOR_EMENDA",
                rows=author_rels,
                source_label="Person",
                source_key="author_key",
                target_label="Amendment",
                target_key="amendment_id",
            )

        # 4. Company nodes for favorecidos
        if favorecido_companies:
            loader.load_nodes(
                "Company", favorecido_companies, key_field="cnpj"
            )

        # 5. Person nodes for favorecidos
        if favorecido_persons:
            loader.load_nodes(
                "Person", favorecido_persons, key_field="cpf"
            )

        # 6. Amendment -[:BENEFICIOU]-> Company/Person
        if favorecido_rels:
            company_rels = [
                r for r in favorecido_rels if r["entity_type"] == "Company"
            ]
            person_rels = [
                r for r in favorecido_rels if r["entity_type"] == "Person"
            ]

            if company_rels:
                query = (
                    "UNWIND $rows AS row "
                    "MATCH (a:Amendment {amendment_id: row.amendment_id}) "
                    "MATCH (c:Company {cnpj: row.doc}) "
                    "MERGE (a)-[r:BENEFICIOU]->(c) "
                    "SET r.value = row.value, "
                    "r.municipality = row.municipality, "
                    "r.uf = row.uf"
                )
                loader.run_query(query, company_rels)

            if person_rels:
                query = (
                    "UNWIND $rows AS row "
                    "MATCH (a:Amendment {amendment_id: row.amendment_id}) "
                    "MATCH (p:Person {cpf: row.doc}) "
                    "MERGE (a)-[r:BENEFICIOU]->(p) "
                    "SET r.value = row.value, "
                    "r.municipality = row.municipality, "
                    "r.uf = row.uf"
                )
                loader.run_query(query, person_rels)

        # 7. Convenio nodes
        if convenios:
            loader.load_nodes("Convenio", convenios, key_field="convenio_id")

        # 8. Amendment -[:GEROU_CONVENIO]-> Convenio
        if convenio_rels:
            loader.load_relationships(
                rel_type="GEROU_CONVENIO",
                rows=convenio_rels,
                source_label="Amendment",
                source_key="amendment_id",
                target_label="Convenio",
                target_key="convenio_id",
            )
