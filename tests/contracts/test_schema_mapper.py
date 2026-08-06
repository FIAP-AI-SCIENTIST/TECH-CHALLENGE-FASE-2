import pyarrow as pa
import pytest
from pydantic import BaseModel

from contracts.schema_mapper import to_pyarrow_schema

class DummyModel(BaseModel):
    name: str
    age: int
    score: float | None
    is_active: bool

def test_to_pyarrow_schema():
    schema = to_pyarrow_schema(DummyModel)
    
    assert isinstance(schema, pa.Schema)
    
    # Verifica os nomes das colunas
    assert schema.names == ["name", "age", "score", "is_active"]
    
    # Verifica os tipos (PyArrow)
    assert pa.types.is_string(schema.field("name").type)
    assert pa.types.is_integer(schema.field("age").type)
    assert pa.types.is_floating(schema.field("score").type)
    assert pa.types.is_boolean(schema.field("is_active").type)
    
    # Verifica nullability
    assert schema.field("name").nullable is False
    assert schema.field("score").nullable is True
