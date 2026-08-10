"""Escrita de partições/tabelas Parquet no Cloud Storage (camada Silver).

Reaproveita as funções de I/O puro do Bronze (`_upload_blob`,
`_delete_blobs_under_prefix`) em vez de duplicar upload/download/list — mas
com um contrato de partição próprio: overwrite completo por `ano=` para as
entidades regulares, tabela cumulativa sem partição para as de meta (SCD2).
"""

import io

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from bronze.writer import BUCKET_NAME, _delete_blobs_under_prefix, _upload_blob

SILVER_PREFIX = "silver"


def _entity_partition_path(entidade: str, chave: str) -> str:
    return f"{SILVER_PREFIX}/{entidade}/{chave}/"


def scd2_path(entidade: str) -> str:
    return f"{SILVER_PREFIX}/{entidade}/data.parquet"


def clear_entity(entidade: str, chave: str) -> None:
    """Limpa a partição `ano=` de uma entidade regular antes de reescrever —
    a Silver é dona dessa partição (business-rules.md regra 13).
    """
    client = storage.Client()
    prefix = _entity_partition_path(entidade, chave)
    _delete_blobs_under_prefix(client, BUCKET_NAME, prefix)


def write_entity(entidade: str, chave: str, table: pa.Table) -> int:
    """Escreve a partição `ano=` de uma entidade regular.

    Overwrite completo — `clear_entity` já deve ter rodado antes para a
    mesma `chave` (mesmo padrão de posse de partição da Bronze batch).
    """
    client = storage.Client()
    prefix = _entity_partition_path(entidade, chave)
    bucket = client.bucket(BUCKET_NAME)

    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    blob = bucket.blob(f"{prefix}part-0.parquet")
    _upload_blob(blob, buffer)
    return table.num_rows


def write_scd2_table(entidade: str, table: pa.Table) -> int:
    """Sobrescreve a tabela SCD2 completa (todas as versões, vigentes e
    fechadas) — sem chave de partição (business-rules.md regra 14).
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    blob = bucket.blob(scd2_path(entidade))
    _upload_blob(blob, buffer)
    return table.num_rows
