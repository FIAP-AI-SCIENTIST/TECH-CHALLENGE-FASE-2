"""Leitura de estado da camada Silver por consumidores a jusante — hoje só a
tabela SCD2 das entidades de meta.

Não é usada por `silver.pipeline`: desde a regra 11 do `business-rules.md` a
cadeia de versões é reconstruída do Bronze a cada execução, então a Silver não
lê o próprio estado para decidir nada. Quem consome daqui é a Gold (U7, modelo
dimensional) e a Data Quality (Data Quality, `make quality` sobre o estado atual).
"""

import io

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import storage

from bronze.reader import _download_blob
from bronze.writer import BUCKET_NAME
from silver.writer import SILVER_PREFIX, scd2_path


def read_scd2_table(entidade: str, schema: pa.Schema) -> pa.Table:
    """Lê a tabela SCD2 gravada de uma entidade de meta.

    Retorna uma tabela vazia (já com o `schema` esperado — incluindo
    `valid_from`/`valid_to`/`is_current`) se ainda não houver nada gravado,
    para que o caller não precise distinguir "entidade nunca processada" de
    "entidade sem versões".
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(scd2_path(entidade))

    try:
        content = _download_blob(blob)
    except NotFound:
        return pa.Table.from_pylist([], schema=schema)

    return pq.read_table(io.BytesIO(content))


def read_entity(entidade: str) -> pa.Table:
    """Lê e concatena todas as partições `ano=` de uma entidade regular da
    Silver — usada pela Gold, que consome a entidade inteira de uma vez
    (mesmo padrão de `bronze.reader.read_partition` sem `chave`).

    Retorna uma tabela sem colunas se a entidade ainda não foi processada.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    prefix = f"{SILVER_PREFIX}/{entidade}/"

    try:
        blobs = list(bucket.list_blobs(prefix=prefix))
    except NotFound:
        return pa.Table.from_pydict({})

    tables = []
    for blob in blobs:
        if not blob.name.endswith(".parquet") or blob.name == scd2_path(entidade):
            continue
        content = _download_blob(blob)
        tables.append(pq.read_table(io.BytesIO(content)))

    if not tables:
        return pa.Table.from_pydict({})
    return pa.concat_tables(tables, promote_options="default")


def read_scd2_table_raw(entidade: str) -> pa.Table | None:
    """Lê a tabela SCD2 completa (todas as versões, vigentes e fechadas)
    sem exigir o schema esperado de antemão — usada pela Gold, que só
    precisa do que já existe. Retorna ``None`` se a entidade ainda não foi
    processada pela Silver (nenhuma versão gravada ainda).
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(scd2_path(entidade))

    try:
        content = _download_blob(blob)
    except NotFound:
        return None

    return pq.read_table(io.BytesIO(content))
