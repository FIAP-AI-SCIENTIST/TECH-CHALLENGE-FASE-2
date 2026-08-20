"""Extração de entidades do BigQuery para a camada Bronze."""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import NamedTuple

from google.cloud import bigquery
from pydantic import ValidationError

from bronze import reader as bronze_reader
from bronze import writer as bronze_writer
from common.lock import gcs_lock
from common.retry import with_retry
from config import get_settings
from contracts.registry import model_for
from contracts.schema_mapper import to_pyarrow_schema
from contracts.serialization import to_pyarrow_table
from observability.logging import log_execution, setup_logger

BATCH_THRESHOLD = 500_000
BATCH_SIZE = 50_000
TIMEOUT_SECONDS = 10
# Cap de custo: query que estourar 10 GB de scan falha em vez de cobrar —
# folga de ~5-10x sobre a maior entidade (~4M linhas); aborta só scan acidental caro.
MAX_BYTES_BILLED = 10 * 2**30

class SourceTable(NamedTuple):
    """Origem de uma entidade no BigQuery.

    `dataset` ausente significa "o dataset da fonte principal"
    (`settings.source_dataset`), que é o caso das 6 entidades do indicador. Uma
    fonte externa de enriquecimento (Censo Escolar, IBGE, Atlas, FUNDEB) vive em
    **outro** dataset do Base dos Dados, e é para isso que o campo existe: antes
    dele, o mapa guardava só o nome da tabela e a query era montada sempre contra
    `settings.source_dataset`, o que tornava qualquer fonte de fora inalcançável
    pela extração batch.

    O contrato da entidade **não** fica aqui — vem de `contracts.registry`, que é
    a fonte única. Este mapa carrega só o que é conhecimento de origem.
    """

    tabela: str
    dataset: str | None = None

    def reference(self, default_dataset: str) -> str:
        """Referência qualificada para a query: `dataset.tabela`."""
        return f"{self.dataset or default_dataset}.{self.tabela}"


ENTITY_TABLE_MAP: dict[str, SourceTable] = {
    "uf": SourceTable("uf"),
    "municipio": SourceTable("municipio"),
    "meta_alfabetizacao_brasil": SourceTable("meta_alfabetizacao_brasil"),
    "meta_alfabetizacao_uf": SourceTable("meta_alfabetizacao_uf"),
    "meta_alfabetizacao_municipio": SourceTable("meta_alfabetizacao_municipio"),
    "alunos": SourceTable("alunos"),
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


# Teto de logs individuais de rejeição por lote: problema sistêmico (lote
# inteiro sujo) não pode inundar o stdout — o resumo final sempre registra o total.
MAX_REJEITADAS_LOG = 10


def _instantiate_records(row_batch, modelo) -> list:
    """Valida cada linha crua do BigQuery contra o Contrato (Pydantic) da entidade.

    Isolamento por linha: uma linha fora do contrato (ex: valor fora da faixa
    declarada no modelo) é descartada e registrada, sem derrubar o lote —
    antes, a primeira ValidationError abortava a extração inteira da entidade.
    ``rows_read`` no audit passa a significar "linhas válidas" (as rejeitadas
    aparecem só no log), mantendo a comparação rows_read x rows_written íntegra.
    """
    logger = setup_logger()
    instancias = []
    rejeitadas = 0
    for row in row_batch:
        dados = dict(row)
        try:
            instancias.append(modelo(**dados))
        except ValidationError as exc:
            rejeitadas += 1
            if rejeitadas <= MAX_REJEITADAS_LOG:
                chave = {k: dados[k] for k in ("ano", "sigla_uf", "id_municipio", "id_aluno") if dados.get(k) is not None}
                logger.warning(
                    "Linha rejeitada pelo contrato %s %s: %s",
                    modelo.__name__, chave, exc.errors(include_url=False),
                )
    if rejeitadas:
        logger.warning(
            "%s: %d linha(s) descartada(s) no lote; %d válida(s) seguem para a Bronze.",
            modelo.__name__, rejeitadas, len(instancias),
        )
    return instancias


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
    modelo = model_for(entidade)
    settings = get_settings()

    with gcs_lock(settings.bucket_name, f"bronze/.locks/{entidade}.lock"):
        with log_execution(step="Bronze_Ingestion", layer="Bronze") as run:
            client = bigquery.Client(project=settings.project_id)
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
    origem = ENTITY_TABLE_MAP[entidade]
    sql = f"SELECT * FROM `{origem.reference(get_settings().source_dataset)}`"
    _run_extraction(entidade, sql)


def extract_incremental(entidade: str) -> None:
    """Extrai apenas os anos novos de uma entidade do BigQuery para Bronze."""
    origem = ENTITY_TABLE_MAP[entidade]
    existing_years = bronze_reader.list_bronze_years(entidade)

    if not existing_years:
        extract_full(entidade)
        return

    max_existing = max(existing_years)
    sql = (
        f"SELECT * FROM `{origem.reference(get_settings().source_dataset)}` "
        f"WHERE ano > {max_existing}"
    )
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
