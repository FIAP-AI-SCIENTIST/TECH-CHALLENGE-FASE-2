"""Leitura de estado da camada Silver por consumidores a jusante — hoje só a
tabela SCD2 das entidades de meta.

Não é usada por `silver.pipeline`: desde a regra 11 do `business-rules.md` a
cadeia de versões é reconstruída do Bronze a cada execução, então a Silver não
lê o próprio estado para decidir nada. Quem consome daqui é a Gold (U7, modelo
dimensional) e a Data Quality (U8, `make quality` sobre o estado atual).
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
