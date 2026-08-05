import pyarrow as pa
from pydantic import BaseModel
from typing import Iterable

def to_pyarrow_table(models: Iterable[BaseModel], schema: pa.Schema) -> pa.Table:
    data = [model.model_dump() for model in models]
    columns = schema.names
    col_dict = {column: [row[column] for row in data] for column in columns}
    pa_table = pa.Table.from_pydict(col_dict, schema)
    return pa_table
