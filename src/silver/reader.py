"""Leitura de estado da camada Silver — hoje só a tabela SCD2 (estado atual
lido antes de `apply_scd2`, o único ponto desta unit que não é stateless).
"""

import io

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import storage

from bronze.reader import _download_blob
from bronze.writer import BUCKET_NAME
from silver.writer import scd2_path


def read_scd2_table(entidade: str, schema: pa.Schema) -> pa.Table:
    """Lê o estado atual da tabela SCD2 de uma entidade.

    Retorna uma tabela vazia (já com o `schema` esperado — incluindo
    `valid_from`/`valid_to`/`is_current`) se ainda não houver nenhum estado
    gravado (primeira execução da entidade).
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(scd2_path(entidade))

    try:
        content = _download_blob(blob)
    except NotFound:
        return pa.Table.from_pylist([], schema=schema)

    return pq.read_table(io.BytesIO(content))
