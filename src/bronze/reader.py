"""Leitura de partições Parquet do Cloud Storage (camada Bronze)."""

import io
import re
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.api_core.exceptions import NotFound

from common.retry import with_retry
from config import get_settings

TIMEOUT_SECONDS = 10


def parse_partition_path(path: str) -> tuple[str, str]:
    """Operação inversa de build_partition_path.

    Dado 'bronze/uf/ano=2023/' ou 'bronze/alunos/data_ingestao=2026-08-06/part-0.parquet',
    retorna ('uf', 'ano=2023') / ('alunos', 'data_ingestao=2026-08-06').

    Genérica: não sabe o significado da chave, só extrai o segmento bruto.
    """
    clean_path = path.rstrip("/")
    match = re.search(r"bronze/([^/]+)/([^/]+)", clean_path)
    if not match:
        raise ValueError(f"Caminho de partição inválido: {path}")
    return match.group(1), match.group(2)


def list_bronze_years(entidade: str) -> set[int]:
    """Lista os anos já gravados no Bronze para uma entidade (partições "ano=").

    Ignora silenciosamente outras chaves de partição da mesma entidade
    (ex: "data_ingestao=..." do streaming) — não são anos, não devem
    quebrar o parsing. Retorna set() vazio se não houver nada.
    """
    years: set[int] = set()
    try:
        client = storage.Client()
        settings = get_settings()
        blobs = _list_blobs_for_entity(client, settings.bucket_name, entidade)
        for blob in blobs:
            _, chave = parse_partition_path(blob.name)
            match = re.fullmatch(r"ano=(\d+)", chave)
            if match:
                years.add(int(match.group(1)))
    except NotFound:
        pass  # Bucket não existe — retorna set vazio
    return years


@with_retry()
def _list_blobs_for_entity(client: storage.Client, bucket_name: str, entidade: str):
    """Lista blobs sob o prefixo da entidade."""
    bucket = client.bucket(bucket_name)
    prefix = f"bronze/{entidade}/"
    return list(bucket.list_blobs(prefix=prefix, timeout=TIMEOUT_SECONDS))


def read_partition(entidade: str, chave: str | None = None) -> pa.Table:
    """Lê partições Parquet do GCS.

    Se ``chave`` for informada (ex: "ano=2023"), lê só os Parquets daquela
    partição. Senão, lê e concatena todas as partições da entidade.
    """
    client = storage.Client()
    settings = get_settings()

    if chave is not None:
        prefix = f"bronze/{entidade}/{chave}/"
    else:
        prefix = f"bronze/{entidade}/"

    blobs = _list_blobs_for_entity(client, settings.bucket_name, entidade)
    if chave is not None:
        blobs = [b for b in blobs if b.name.startswith(prefix)]

    tables = []
    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        content = _download_blob(blob)
        table = pq.read_table(io.BytesIO(content))
        tables.append(table)

    if not tables:
        return pa.Table.from_pydict({})
    return pa.concat_tables(tables)


def count_partition_rows(entidade: str) -> int:
    """Conta linhas de todas as partições da entidade sem decodificar colunas.

    Usa o metadado do rodapé do Parquet (``ParquetFile(...).metadata.num_rows``) em vez de
    ``pq.read_table`` — evita decodificar colunas inteiras (proficiência, presença, etc.) só
    para descartar o conteúdo e devolver um inteiro. Reaproveita a mesma listagem/download de
    blobs (com retry) de `read_partition`.
    """
    client = storage.Client()
    settings = get_settings()
    blobs = _list_blobs_for_entity(client, settings.bucket_name, entidade)
    total = 0
    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        content = _download_blob(blob)
        total += pq.ParquetFile(io.BytesIO(content)).metadata.num_rows
    return total


@with_retry()
def _download_blob(blob: storage.Blob) -> bytes:
    """Baixa o conteúdo de um blob."""
    return blob.download_as_bytes(timeout=TIMEOUT_SECONDS)
