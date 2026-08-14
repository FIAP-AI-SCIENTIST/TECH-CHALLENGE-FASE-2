"""Escrita de partições Parquet no Cloud Storage (camada Bronze)."""

import io
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from common.retry import with_retry

BUCKET_NAME = "useful-space-277919-datalake"
TIMEOUT_SECONDS = 10


def build_partition_path(entidade: str, chave: str) -> str:
    """Constrói o caminho GCS para uma partição.

    ``chave`` já vem formatada pelo caller (ex: "ano=2023",
    "data_ingestao=2026-08-06") — esta função não sabe nem precisa saber
    o significado da chave, só concatena.
    """
    return f"bronze/{entidade}/{chave}/"


def write_partition(entidade: str, chave: str, table: pa.Table, part_id: str) -> int:
    """Escreve um arquivo de lote na partição, sem apagar nada existente.

    Serializa a tabela para Parquet e faz upload como part-{part_id}.parquet.
    ``part_id`` nomeia o lote dentro da partição e é escolhido pelo caller: a
    extração batch usa o índice sequencial do lote (a partição "ano=" é dela e
    é reescrita inteira a cada execução); o consumer de streaming usa o run_id
    da execução, porque a partição "data_ingestao=" é compartilhada entre
    execuções e um índice sequencial colidiria com o lote de um run anterior.
    Retorna o número de linhas escritas. Limpeza da partição é responsabilidade
    exclusiva de clear_partition, chamada pelo caller antes do primeiro lote.
    """
    client = storage.Client()
    prefix = build_partition_path(entidade, chave)
    bucket = client.bucket(BUCKET_NAME)

    # Serializa para Parquet
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload
    blob = bucket.blob(f"{prefix}part-{part_id}.parquet")
    _upload_blob(blob, buffer)

    return table.num_rows


def clear_partition(entidade: str, chave: str) -> None:
    """Remove todos os blobs existentes sob a partição (overwrite).

    Chamada uma única vez por (entidade, chave) por execução, antes do
    primeiro lote — nunca de dentro de write_partition.
    """
    client = storage.Client()
    prefix = build_partition_path(entidade, chave)
    _delete_blobs_under_prefix(client, BUCKET_NAME, prefix)


@with_retry()
def _delete_blobs_under_prefix(client: storage.Client, bucket_name: str, prefix: str) -> None:
    """Remove todos os blobs sob o prefixo especificado."""
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
