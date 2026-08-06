"""Leitura de partições Parquet do Cloud Storage (camada Bronze)."""

import io
import time
import re
from typing import Set

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.api_core.exceptions import NotFound

from common.retry import with_retry

BUCKET_NAME = "useful-space-277919-datalake"


def parse_partition_path(path: str) -> tuple:
    """Operação inversa de build_partition_path.

    Dado 'bronze/uf/ano=2023/' ou 'bronze/uf/ano=2023/part-0.parquet',
    retorna ('uf', 2023).
    """
    # Remove trailing slash se existir
    clean_path = path.rstrip("/")
    # Extrai entidade e ano via regex
    match = re.search(r"bronze/([^/]+)/ano=(\d+)", clean_path)
    if not match:
        raise ValueError(f"Caminho de partição inválido: {path}")
    return match.group(1), int(match.group(2))


def list_bronze_years(entidade: str) -> Set[int]:
    """Lista os anos já gravados no Bronze para uma entidade.

    Retorna set() vazio se não houver nada (nunca levanta exceção
    por ausência — bucket vazio é um estado válido).
    """
    years: Set[int] = set()
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blobs = _list_blobs_for_entity(client, BUCKET_NAME, entidade)
        for blob in blobs:
            _, ano = parse_partition_path(blob.name)
            years.add(ano)
    except NotFound:
        pass  # Bucket não existe — retorna set vazio
    return years


@with_retry()
def _list_blobs_for_entity(client: storage.Client, bucket_name: str, entidade: str):
    """Lista blobs sob o prefixo da entidade."""
    bucket = client.bucket(bucket_name)
    prefix = f"bronze/{entidade}/"
    return list(bucket.list_blobs(prefix=prefix))


def read_partition(entidade: str, ano: int | None = None) -> pa.Table:
    """Lê partições Parquet do GCS.

    Se ``ano`` for informado, lê só os Parquets daquela partição.
    Senão, lê e concatena todas as partições da entidade.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    if ano is not None:
        prefix = f"bronze/{entidade}/ano={ano}/"
    else:
        prefix = f"bronze/{entidade}/"

    tables = []
    blobs = _list_blobs_for_entity(client, BUCKET_NAME, entidade)
    if ano is not None:
        blobs = [b for b in blobs if b.name.startswith(prefix)]

    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        content = _download_blob(blob)
        table = pq.read_table(io.BytesIO(content))
        tables.append(table)

    if not tables:
        return pa.Table.from_pydict({})
    return pa.concat_tables(tables)


@with_retry()
def _download_blob(blob: storage.Blob) -> bytes:
    """Baixa o conteúdo de um blob."""
    return blob.download_as_bytes()
