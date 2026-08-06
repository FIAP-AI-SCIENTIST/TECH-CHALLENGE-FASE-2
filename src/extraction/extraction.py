"""Extração de entidades do BigQuery para a camada Bronze."""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
from collections import defaultdict
from collections.abc import Iterator

from google.cloud import bigquery

from bronze import reader as bronze_reader
from bronze import writer as bronze_writer
from common.retry import with_retry
from contracts.models import (
    DadosAlunosRecord,
    MetaAlfabetizacaoBrasilRecord,
    MetaAlfabetizacaoMunicipioRecord,
    MetaAlfabetizacaoUFRecord,
    MunicipioRecord,
    UFRecord,
)
from contracts.schema_mapper import to_pyarrow_schema
from contracts.serialization import to_pyarrow_table
from observability.logging import log_execution

PROJECT_ID = "useful-space-277919"
SOURCE_DATASET = "basedosdados.br_inep_avaliacao_alfabetizacao"
BATCH_THRESHOLD = 500_000
BATCH_SIZE = 50_000
TIMEOUT_SECONDS = 10

ENTITY_TABLE_MAP = {
    "uf": ("uf", UFRecord),
    "municipio": ("municipio", MunicipioRecord),
    "meta_alfabetizacao_brasil": ("meta_alfabetizacao_brasil", MetaAlfabetizacaoBrasilRecord),
    "meta_alfabetizacao_uf": ("meta_alfabetizacao_uf", MetaAlfabetizacaoUFRecord),
    "meta_alfabetizacao_municipio": ("meta_alfabetizacao_municipio", MetaAlfabetizacaoMunicipioRecord),
    "alunos": ("alunos", DadosAlunosRecord),
}


def compute_incremental_years(existing_years: set, source_years: set) -> list:
    """Retorna, ordenados, os anos de source_years que não estão em existing_years."""
    return sorted(source_years - existing_years)


def batched(iterable, size: int) -> Iterator[list]:
    """Itera ``iterable`` e produz lotes de até ``size`` elementos."""
    batch: list = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

@with_retry()
def _do_query(client: bigquery.Client, sql: str):
    """Operação atômica: roda a query e materializa o RowIterator (com timeout)."""
    return client.query(sql).result(timeout=TIMEOUT_SECONDS)


def extract_full(entidade: str) -> None:
    """Extrai todos os dados de uma entidade do BigQuery para Bronze."""
    tabela, modelo = ENTITY_TABLE_MAP[entidade]
    sql = f"SELECT * FROM `{SOURCE_DATASET}.{tabela}`"

    with log_execution(unit="Bronze_Ingestion", layer="Bronze") as run:
        client = bigquery.Client(project=PROJECT_ID)
        rows = _do_query(client, sql)

        total_rows = getattr(rows, "total_rows", None)
        if total_rows and total_rows > BATCH_THRESHOLD:
            row_batches = batched(rows, BATCH_SIZE)
        else:
            row_batches = [list(rows)]

        rows_read = 0
        rows_written = 0
        for row_batch in row_batches:
            instancias = []
            for row in row_batch:
                record = modelo(**dict(row))
                instancias.append(record)
                rows_read += 1

            # Agrupa por ano
            ano_groups: dict = defaultdict(list)
            for inst in instancias:
                ano = getattr(inst, "ano", None) or 0
                ano_groups[ano].append(inst)

            # Escreve uma partição por ano
            schema = to_pyarrow_schema(modelo)
            for ano, grupo in ano_groups.items():
                table = to_pyarrow_table(grupo, schema)
                written = bronze_writer.write_partition(entidade, ano, table)
                rows_written += written

        run.rows_read = rows_read
        run.rows_written = rows_written


def extract_incremental(entidade: str) -> None:
    """Extrai apenas os anos novos de uma entidade do BigQuery para Bronze."""
    tabela, modelo = ENTITY_TABLE_MAP[entidade]
    existing_years = bronze_reader.list_bronze_years(entidade)

    if not existing_years:
        extract_full(entidade)
        return

    max_existing = max(existing_years)
    sql = f"SELECT * FROM `{SOURCE_DATASET}.{tabela}` WHERE ano > {max_existing}"

    with log_execution(unit="Bronze_Ingestion", layer="Bronze") as run:
        client = bigquery.Client(project=PROJECT_ID)
        rows = _do_query(client, sql)

        total_rows = getattr(rows, "total_rows", None)
        if total_rows and total_rows > BATCH_THRESHOLD:
            row_batches = batched(rows, BATCH_SIZE)
        else:
            row_batches = [list(rows)]

        rows_read = 0
        rows_written = 0
        for row_batch in row_batches:
            instancias = []
            for row in row_batch:
                record = modelo(**dict(row))
                instancias.append(record)
                rows_read += 1

            # Agrupa por ano
            ano_groups: dict = defaultdict(list)
            for inst in instancias:
                ano = getattr(inst, "ano", None) or 0
                ano_groups[ano].append(inst)

            # Escreve uma partição por ano
            schema = to_pyarrow_schema(modelo)
            for ano, grupo in ano_groups.items():
                table = to_pyarrow_table(grupo, schema)
                written = bronze_writer.write_partition(entidade, ano, table)
                rows_written += written

        run.rows_read = rows_read
        run.rows_written = rows_written


def extract_entity(entidade: str) -> None:
    """Ponto de entrada único — extrai uma entidade (full ou incremental).

    Se não houver anos existentes, faz extração completa.
    Senão, faz extração incremental.
    """
    existing_years = bronze_reader.list_bronze_years(entidade)
    if not existing_years:
        extract_full(entidade)
    else:
        extract_incremental(entidade)
