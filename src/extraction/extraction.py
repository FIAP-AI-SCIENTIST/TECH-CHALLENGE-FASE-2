"""Extração de entidades do BigQuery para a camada Bronze."""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
from collections import defaultdict
from collections.abc import Iterable, Iterator

from google.cloud import bigquery

from bronze import reader as bronze_reader
from bronze import writer as bronze_writer
from common.lock import gcs_lock
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
# Cap de custo: query que estourar 10 GB de scan falha em vez de cobrar —
# folga de ~5-10x sobre a maior entidade (~4M linhas); aborta só scan acidental caro.
MAX_BYTES_BILLED = 10 * 2**30

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
def _do_query(client: bigquery.Client, sql: str) -> tuple:
    """Operação atômica: roda a query (com cap de bytes) e materializa o RowIterator.

    Retorna ``(rows, total_bytes_processed)`` — o contador do job alimenta a
    auditoria de custo por execução.
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED))
    return job.result(timeout=TIMEOUT_SECONDS), job.total_bytes_processed


def _split_into_batches(rows, total_rows) -> Iterable[list]:
    """Decide entre processar tudo de uma vez ou em lotes (batching seletivo)."""
    if total_rows and total_rows > BATCH_THRESHOLD:
        return batched(rows, BATCH_SIZE)
    return [list(rows)]


def _instantiate_records(row_batch, modelo) -> list:
    """Valida cada linha crua do BigQuery contra o Contrato (Pydantic) da entidade."""
    return [modelo(**dict(row)) for row in row_batch]


def _group_by_ano(instancias: list) -> dict:
    """Agrupa instâncias validadas por ano — cada grupo vira uma escrita de partição."""
    ano_groups: dict = defaultdict(list)
    for inst in instancias:
        ano = getattr(inst, "ano", None) or 0
        ano_groups[ano].append(inst)
    return ano_groups


def _write_ano_groups(
    entidade: str,
    ano_groups: dict,
    modelo,
    anos_limpos: set[int],
    parte_por_ano: dict[int, int],
) -> int:
    """Escreve um lote de grupos (ano -> instâncias) na Bronze.

    Limpa a partição na primeira vez que um ano aparece nesta execução
    (anos_limpos) e escreve cada lote em um arquivo part-{index}.parquet
    próprio (parte_por_ano) — nunca sobrescreve um lote anterior do mesmo
    ano (fix do B1).
    """
    schema = to_pyarrow_schema(modelo)
    rows_written = 0
    for ano, grupo in ano_groups.items():
        chave = f"ano={ano}"
        if ano not in anos_limpos:
            bronze_writer.clear_partition(entidade, chave)
            anos_limpos.add(ano)

        table = to_pyarrow_table(grupo, schema)
        written = bronze_writer.write_partition(
            entidade, chave, table, part_id=str(parte_por_ano[ano])
        )
        parte_por_ano[ano] += 1
        rows_written += written
    return rows_written


def _run_extraction(entidade: str, sql: str) -> None:
    """Núcleo compartilhado por extract_full/extract_incremental: query -> valida -> escreve -> audita.

    Protegido por um lock exclusivo (`common.lock.gcs_lock`) — duas execuções
    concorrentes da mesma entidade (ex: dois membros do grupo rodando
    `make bronze` ao mesmo tempo) poderiam intercalar `clear_partition`/
    `write_partition` e deixar a partição com uma mistura inconsistente de
    arquivos de dois runs diferentes.
    """
    _tabela, modelo = ENTITY_TABLE_MAP[entidade]

    with gcs_lock(bronze_writer.BUCKET_NAME, f"bronze/.locks/{entidade}.lock"):
        with log_execution(unit="Bronze_Ingestion", layer="Bronze") as run:
            client = bigquery.Client(project=PROJECT_ID)
            rows, bytes_processed = _do_query(client, sql)
            row_batches = _split_into_batches(rows, getattr(rows, "total_rows", None))

            rows_read = 0
            rows_written = 0
            anos_limpos: set[int] = set()
            parte_por_ano: dict[int, int] = defaultdict(int)
            for row_batch in row_batches:
                instancias = _instantiate_records(row_batch, modelo)
                rows_read += len(instancias)
                ano_groups = _group_by_ano(instancias)
                rows_written += _write_ano_groups(
                    entidade, ano_groups, modelo, anos_limpos, parte_por_ano
                )

            run.rows_read = rows_read
            run.rows_written = rows_written
            run.total_bytes_processed = bytes_processed


def extract_full(entidade: str) -> None:
    """Extrai todos os dados de uma entidade do BigQuery para Bronze."""
    tabela, _modelo = ENTITY_TABLE_MAP[entidade]
    sql = f"SELECT * FROM `{SOURCE_DATASET}.{tabela}`"
    _run_extraction(entidade, sql)


def extract_incremental(entidade: str) -> None:
    """Extrai apenas os anos novos de uma entidade do BigQuery para Bronze."""
    tabela, _modelo = ENTITY_TABLE_MAP[entidade]
    existing_years = bronze_reader.list_bronze_years(entidade)

    if not existing_years:
        extract_full(entidade)
        return

    max_existing = max(existing_years)
    sql = f"SELECT * FROM `{SOURCE_DATASET}.{tabela}` WHERE ano > {max_existing}"
    _run_extraction(entidade, sql)


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
