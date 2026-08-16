"""Leitura de colunas de fato/dimensão da Gold para o check de integridade referencial.

Fronteira de dependência: importa só `google.cloud.bigquery` e `common.retry` — nunca `gold/`.
A verificação de FK roda de fora da Gold (mesma regra já declarada em
`U7-Gold/functional-design/business-rules.md`, "a Gold não valida a si mesma").
"""
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

from google.cloud import bigquery

from common.retry import with_retry

PROJECT_ID = "useful-space-277919"
DATASET_ID = "alfabetizacao_analytics"
TIMEOUT_SECONDS = 30
# Mesmo racional/valor de extraction.py e silver/reference.py: folga generosa sobre o volume
# real (a maior tabela lida aqui é fact_alunos, ~3,9M linhas de uma única coluna), aborta só
# scan acidental caro.
MAX_BYTES_BILLED = 10 * 2**30


@with_retry()
def _do_query(client: bigquery.Client, sql: str) -> tuple:
    """Operação atômica: roda a query (com cap de bytes) e materializa o RowIterator.

    Retorna ``(rows, total_bytes_processed)``.
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED))
    return job.result(timeout=TIMEOUT_SECONDS), job.total_bytes_processed


def read_column(tabela: str, coluna: str) -> tuple[list, int]:
    """Lê uma coluna de uma tabela/view da Gold (`fact_*`/`dim_*`).

    Sem `DISTINCT`: preserva a semântica por linha que `check_referential_integrity` espera
    (fração de linhas cuja FK resolve, não fração de valores distintos). Retorna
    ``(valores, bytes_processados)``.
    """
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"SELECT {coluna} FROM `{PROJECT_ID}.{DATASET_ID}.{tabela}`"
    rows, bytes_processed = _do_query(client, sql)
    return [row[coluna] for row in rows], bytes_processed
