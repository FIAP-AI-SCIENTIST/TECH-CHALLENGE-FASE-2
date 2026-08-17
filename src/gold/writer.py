"""Escrita de tabelas Gold no BigQuery (dataset analítico).

Cada tabela é sobrescrita por completo a cada execução (load job com
WRITE_TRUNCATE, schema inferido do próprio Parquet autodescritivo) — a Gold
não tem estado próprio, é sempre recomputada da Silver. Mesma filosofia de
posse total de partição usada na Bronze/Silver, aplicada aqui à tabela
inteira em vez de a uma partição. O dataset já existe via Terraform; as
tabelas fato são criadas antes do load por `gold.schema.ensure_table` (DDL com
particionamento/clustering/constraints) e as dimensões pelo próprio load job.
Como o load WRITE_TRUNCATE remove PK/FK (ele regrava o schema a partir do
Parquet), `gold.schema.ensure_constraints` re-aplica as constraints depois do
load.
"""

import io
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

from common.retry import with_retry
from config import get_settings
from gold import schema as gold_schema

TIMEOUT_SECONDS = 30


def write_table(nome_tabela: str, table: pa.Table) -> int:
    """Sobrescreve `nome_tabela` no dataset analítico a partir de `table`.
    Retorna o número de linhas escritas."""
    settings = get_settings()
    client = bigquery.Client(project=settings.project_id)
    table_ref = f"{settings.project_id}.{settings.dataset_id}.{nome_tabela}"

    # Garante a tabela antes do load: CREATE IF NOT EXISTS é no-op para
    # tabelas já criadas — o WRITE_TRUNCATE preserva partição/clustering.
    gold_schema.ensure_table(client, nome_tabela)

    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    _load(client, buffer, table_ref, job_config)

    # O load WRITE_TRUNCATE remove PK/FK — re-aplica depois da carga.
    gold_schema.ensure_constraints(client, nome_tabela)
    return table.num_rows


@with_retry()
def _load(client: bigquery.Client, buffer: io.BytesIO, table_ref: str, job_config: bigquery.LoadJobConfig) -> None:
    """Operação atômica: submete o load job e aguarda a conclusão."""
    buffer.seek(0)
    job = client.load_table_from_file(buffer, table_ref, job_config=job_config, timeout=TIMEOUT_SECONDS)
    job.result(timeout=TIMEOUT_SECONDS)
