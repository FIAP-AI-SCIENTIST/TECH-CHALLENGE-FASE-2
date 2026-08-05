from typing import Optional
from pydantic import BaseModel
import pyarrow as pa
from hypothesis import given, strategies as st

from contracts.schema_mapper import to_pyarrow_schema
from contracts.serialization import to_pyarrow_table

class PbtModel(BaseModel):
    name: str
    value: int
    flag: Optional[bool]

# Gera instâncias aleatórias do PbtModel
@st.composite
def st_pbt_model(draw):
    return PbtModel(
        name=draw(st.text()),
        value=draw(st.integers(min_value=-100000, max_value=100000)),
        flag=draw(st.one_of(st.none(), st.booleans()))
    )

@given(st.lists(st_pbt_model(), max_size=100))
def test_serialization_roundtrip(models):
    """
    Property-Based Test (PBT-02: Round-trip).
    Garante que qualquer conversão Pydantic -> PyArrow -> Pydantic gera
    os mesmos dados sem corrupção e sem crash do PyArrow.
    """
    schema = to_pyarrow_schema(PbtModel)
    
    # 1. Pydantic -> PyArrow Table
    table = to_pyarrow_table(models, schema)
    
    # 2. PyArrow Table -> List of Dicts
    records = table.to_pylist()
    
    # 3. List of Dicts -> Pydantic
    recreated_models = [PbtModel(**rec) for rec in records]
    
    # Afirma a propriedade de Round-trip
    assert models == recreated_models
