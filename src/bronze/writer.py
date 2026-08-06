"""Escrita de partições Parquet no Cloud Storage (camada Bronze)."""

import io
import time

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from common.retry import with_retry

BUCKET_NAME = "useful-space-277919-datalake"
TIMEOUT_SECONDS = 10


def build_partition_path(entidade: str, ano: int) -> str:
    """Constrói o caminho GCS para uma partição."""
    return f"bronze/{entidade}/ano={ano}/"


def write_partition(entidade: str, ano: int, table: pa.Table) -> int:
    """Escreve uma partição Parquet no GCS, sobrescrevendo partições existentes.

    Apaga todos os blobs existentes sob o prefixo da partição,
    serializa a tabela para Parquet e faz upload como part-0.parquet.
    Retorna o número de linhas escritas.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    prefix = build_partition_path(entidade, ano)

    # Remove blobs existentes (overwrite)
    _delete_blobs_under_prefix(client, BUCKET_NAME, prefix)

    # Serializa para Parquet
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload
    blob = bucket.blob(f"{prefix}part-0.parquet")
    _upload_blob(blob, buffer)

    return table.num_rows


@with_retry()
def _delete_blobs_under_prefix(client: storage.Client, bucket_name: str, prefix: str) -> None:
    """Remove todos os blobs sob o prefixo especificado."""
    bucket = client.bucket(bucket_name)
    blobs = list(_list_blobs_with_prefix(client, bucket_name, prefix))
    for blob in blobs:
        blob.delete(timeout=TIMEOUT_SECONDS)


@with_retry()
def _list_blobs_with_prefix(client: storage.Client, bucket_name: str, prefix: str) -> list:
    """Lista blobs sob o prefixo especificado."""
    bucket = client.bucket(bucket_name)
    return list(bucket.list_blobs(prefix=prefix, timeout=TIMEOUT_SECONDS))


@with_retry()
def _upload_blob(blob: storage.Blob, buffer: io.BytesIO) -> None:
    """Faz upload de um buffer para o blob especificado."""
    blob.upload_from_string(buffer.read(), content_type="application/octet-stream", timeout=TIMEOUT_SECONDS)
