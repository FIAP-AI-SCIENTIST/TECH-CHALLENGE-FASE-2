import pyarrow as pa
from pydantic import BaseModel
from typing import Optional

from contracts.serialization import to_pyarrow_table

class SimpleModel(BaseModel):
    id: int
    value: str
    is_valid: Optional[bool] = None

def test_to_pyarrow_table():
    data = [
        SimpleModel(id=1, value="a", is_valid=True),
        SimpleModel(id=2, value="b")
    ]
    
    # Construindo o schema na mão para isolar a dependência do schema_mapper
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("value", pa.string(), nullable=False),
        pa.field("is_valid", pa.bool_(), nullable=True)
    ])
    
    table = to_pyarrow_table(data, schema)
    
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table.num_columns == 3
    
    pydict = table.to_pydict()
    assert pydict["id"] == [1, 2]
    assert pydict["value"] == ["a", "b"]
    assert pydict["is_valid"] == [True, None]
