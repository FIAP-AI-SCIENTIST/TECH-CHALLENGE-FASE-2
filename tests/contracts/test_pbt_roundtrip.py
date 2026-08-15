from pydantic import BaseModel
import pyarrow as pa
from hypothesis import given, strategies as st

from contracts.schema_mapper import to_pyarrow_schema
from contracts.serialization import to_pyarrow_table

class PbtModel(BaseModel):
    name: str
    value: int
    flag: bool | None

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
    Property-Based Test de round-trip.
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


# --- Round-trip com as 6 entidades reais do domínio (não só um modelo sintético) ---

from contracts.models import (
    UFRecord,
    MunicipioRecord,
    MetaAlfabetizacaoBrasilRecord,
    MetaAlfabetizacaoUFRecord,
    MetaAlfabetizacaoMunicipioRecord,
    DadosAlunosRecord,
)
from contracts.testing.strategies import (
    st_uf_record,
    st_municipio_record,
    st_meta_alfabetizacao_brasil_record,
    st_meta_alfabetizacao_uf_record,
    st_meta_alfabetizacao_municipio_record,
    st_dados_alunos_record,
)


def _assert_roundtrip(model_class, models):
    schema = to_pyarrow_schema(model_class)
    table = to_pyarrow_table(models, schema)
    records = table.to_pylist()
    recreated_models = [model_class(**rec) for rec in records]
    assert models == recreated_models


@given(st.lists(st_uf_record(), max_size=50))
def test_uf_record_roundtrip(models):
    _assert_roundtrip(UFRecord, models)


@given(st.lists(st_municipio_record(), max_size=50))
def test_municipio_record_roundtrip(models):
    _assert_roundtrip(MunicipioRecord, models)


@given(st.lists(st_meta_alfabetizacao_brasil_record(), max_size=50))
def test_meta_alfabetizacao_brasil_record_roundtrip(models):
    _assert_roundtrip(MetaAlfabetizacaoBrasilRecord, models)


@given(st.lists(st_meta_alfabetizacao_uf_record(), max_size=50))
def test_meta_alfabetizacao_uf_record_roundtrip(models):
    _assert_roundtrip(MetaAlfabetizacaoUFRecord, models)


@given(st.lists(st_meta_alfabetizacao_municipio_record(), max_size=50))
def test_meta_alfabetizacao_municipio_record_roundtrip(models):
    _assert_roundtrip(MetaAlfabetizacaoMunicipioRecord, models)


@given(st.lists(st_dados_alunos_record(), max_size=50))
def test_dados_alunos_record_roundtrip(models):
    _assert_roundtrip(DadosAlunosRecord, models)
