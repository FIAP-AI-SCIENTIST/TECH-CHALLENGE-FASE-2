"""Property-Based Testing (Hypothesis) das funções puras de gold.transform —
idempotência e determinismo de cada dimensão/fato.

Geradores reaproveitados de `contracts.testing.strategies` onde o schema bate 1:1 com o contrato
(fatos regulares e `fact_alunos` — `_select_existing` só projeta colunas já presentes no contrato
base). `dim_uf`/`dim_municipio`/`dim_rede`/`dim_serie` e o fato de meta usam estratégias próprias
porque leem colunas de enriquecimento da Silver (`sigla_uf_nome`, `nome_regiao`, `rede_desc`,
`serie_desc`, `valid_from`/`valid_to`/`is_current`) que não existem no contrato base — a
Gold consome a saída já enriquecida, não o registro cru.
"""

import pyarrow as pa
from hypothesis import given
from hypothesis import strategies as st

from contracts.models import DadosAlunosRecord, MunicipioRecord, UFRecord
from contracts.schema_mapper import to_pyarrow_schema
from contracts.serialization import to_pyarrow_table
from contracts.testing.strategies import (
    st_dados_alunos_record,
    st_municipio_record,
    st_uf_record,
)
from gold.transform import (
    FACT_ALUNOS_COLUMNS,
    FACT_UF_MEASURES,
    META_ANOS,
    build_dim_municipio,
    build_dim_rede,
    build_dim_serie,
    build_dim_uf,
    build_fact_alunos,
    build_fact_indicador_municipio,
    build_fact_indicador_uf,
    build_fact_meta_resultado,
)

_UF_SCHEMA = to_pyarrow_schema(UFRecord)
_MUNICIPIO_SCHEMA = to_pyarrow_schema(MunicipioRecord)
_ALUNOS_SCHEMA = to_pyarrow_schema(DadosAlunosRecord)


# --- Fatos regulares: _select_existing só projeta, nunca filtra linha (reaproveita contracts) ---


@given(st.lists(st_uf_record(), min_size=0, max_size=15))
def test_fact_indicador_uf_preserves_row_count(records):
    tabela = to_pyarrow_table(records, _UF_SCHEMA)
    fato = build_fact_indicador_uf(tabela)
    assert fato.num_rows == tabela.num_rows


@given(st.lists(st_municipio_record(), min_size=0, max_size=15))
def test_fact_indicador_municipio_preserves_row_count(records):
    tabela = to_pyarrow_table(records, _MUNICIPIO_SCHEMA)
    fato = build_fact_indicador_municipio(tabela)
    assert fato.num_rows == tabela.num_rows


@given(st.lists(st_uf_record(), min_size=0, max_size=15))
def test_fact_indicador_uf_columns_subset_of_measures_and_source(records):
    tabela = to_pyarrow_table(records, _UF_SCHEMA)
    fato = build_fact_indicador_uf(tabela)
    esperado = set(FACT_UF_MEASURES) | {"ano", "sigla_uf", "serie", "rede"}
    assert set(fato.column_names) <= esperado


@given(st.lists(st_dados_alunos_record(), min_size=0, max_size=15))
def test_fact_alunos_preserves_row_count_and_columns(records):
    tabela = to_pyarrow_table(records, _ALUNOS_SCHEMA)
    fato = build_fact_alunos(tabela)
    assert fato.num_rows == tabela.num_rows
    assert set(fato.column_names) <= set(FACT_ALUNOS_COLUMNS)


# --- Idempotência: mesma função, mesma tabela, resultado idêntico ---


@given(st.lists(st_uf_record(), min_size=0, max_size=10))
def test_fact_indicador_uf_is_idempotent(records):
    tabela = to_pyarrow_table(records, _UF_SCHEMA)
    primeira = build_fact_indicador_uf(tabela)
    segunda = build_fact_indicador_uf(tabela)
    assert primeira.to_pylist() == segunda.to_pylist()


# --- dim_uf: distinct por sigla_uf, cardinalidade nunca reduz ao duplicar linhas fonte ---


@st.composite
def st_uf_dim_source(draw):
    siglas = draw(st.lists(st.sampled_from(["SP", "RJ", "MG", "BA", "PR"]), min_size=1, max_size=10))
    nomes = {s: draw(st.text(min_size=1, max_size=10)) for s in set(siglas)}
    return pa.table({
        "sigla_uf": siglas,
        "sigla_uf_nome": [nomes[s] for s in siglas],
    })


@given(st_uf_dim_source())
def test_dim_uf_is_distinct_by_sigla(tabela):
    dim = build_dim_uf(tabela)
    valores = dim.column("sigla_uf").to_pylist()
    assert len(valores) == len(set(valores))


@given(st_uf_dim_source())
def test_dim_uf_duplicating_source_rows_keeps_same_cardinality(tabela):
    duplicada = pa.concat_tables([tabela, tabela])
    original = build_dim_uf(tabela)
    dobrada = build_dim_uf(duplicada)
    assert set(original.column("sigla_uf").to_pylist()) == set(dobrada.column("sigla_uf").to_pylist())
    assert original.num_rows == dobrada.num_rows


# --- dim_municipio: distinct por id_municipio ---


@st.composite
def st_municipio_dim_source(draw):
    ids = draw(st.lists(st.from_regex(r"[0-9]{7}", fullmatch=True), min_size=1, max_size=8, unique=True))
    linhas = []
    for id_mun in ids:
        n_versoes = draw(st.integers(min_value=1, max_value=3))
        for _ in range(n_versoes):
            linhas.append({
                "id_municipio": id_mun,
                "nome": draw(st.text(min_size=1, max_size=10)),
                "sigla_uf": draw(st.sampled_from(["SP", "RJ", "MG"])),
                "nome_regiao": draw(st.sampled_from(["Sudeste", "Sul", "Nordeste"])),
                "capital_uf": draw(st.integers(min_value=0, max_value=1)),
                "ano": draw(st.integers(min_value=2020, max_value=2026)),
            })
    return pa.Table.from_pylist(linhas)


@given(st_municipio_dim_source())
def test_dim_municipio_unique_per_id(tabela):
    dim = build_dim_municipio(tabela)
    valores = dim.column("id_municipio").to_pylist()
    assert len(valores) == len(set(valores)) == len(set(tabela.column("id_municipio").to_pylist()))


# --- dim_rede / dim_serie: união distinct entre fontes ---


@st.composite
def st_codigo_desc_source(draw, codigo_col, desc_col, codigos):
    pares = draw(st.lists(st.sampled_from(codigos), min_size=1, max_size=10))
    descs = {c: draw(st.text(min_size=1, max_size=10)) for c in set(pares)}
    return pa.table({codigo_col: pares, desc_col: [descs[c] for c in pares]})


@given(st_codigo_desc_source("rede", "rede_desc", ["0", "1", "2", "3", "4"]))
def test_dim_rede_is_distinct(tabela):
    dim = build_dim_rede(tabela)
    valores = dim.column("rede").to_pylist()
    assert len(valores) == len(set(valores))


@given(st_codigo_desc_source("serie", "serie_desc", ["2"]))
def test_dim_serie_is_distinct(tabela):
    dim = build_dim_serie(tabela)
    valores = dim.column("serie").to_pylist()
    assert len(valores) == len(set(valores))


# --- fact_meta_resultado: gap_pontos/atingiu_meta são sempre derivados do meta_indicador do próprio ano ---


@st.composite
def st_meta_resultado_source(draw):
    n = draw(st.integers(min_value=1, max_value=8))
    linhas = []
    for _ in range(n):
        ano = draw(st.sampled_from(META_ANOS))
        taxa = draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False))
        metas = {a: draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)) for a in META_ANOS}
        linha = {
            "rede": draw(st.sampled_from(["0", "1", "2"])),
            "ano": ano,
            "taxa_alfabetizacao": taxa,
            "percentual_participacao": draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)),
            "valid_from": ano,
            "valid_to": None,
            "is_current": True,
        }
        for a in META_ANOS:
            linha[f"meta_alfabetizacao_{a}"] = metas[a]
        linhas.append(linha)
    return pa.Table.from_pylist(linhas)


@given(st_meta_resultado_source())
def test_fact_meta_resultado_gap_and_atingiu_meta_are_derived(tabela):
    fato = build_fact_meta_resultado(tabela, ["rede"])
    for linha in fato.to_pylist():
        meta = linha["meta_indicador"]
        taxa = linha["taxa_alfabetizacao"]
        assert linha["gap_pontos"] == taxa - meta
        assert linha["atingiu_meta"] == (taxa >= meta)


@given(st_meta_resultado_source())
def test_fact_meta_resultado_is_idempotent(tabela):
    primeira = build_fact_meta_resultado(tabela, ["rede"])
    segunda = build_fact_meta_resultado(tabela, ["rede"])
    assert primeira.to_pylist() == segunda.to_pylist()
