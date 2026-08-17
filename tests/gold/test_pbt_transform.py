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
    build_dim_tempo,
    build_dim_uf,
    build_fact_alfabetizacao_municipio,
    build_fact_alunos,
    build_fact_indicador_municipio,
    build_fact_indicador_uf,
    build_fact_meta_resultado,
    surrogate_key,
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
    esperado = set(FACT_UF_MEASURES) | {"ano", "sigla_uf", "serie", "rede", "sk_uf", "sk_serie", "sk_rede", "sk_tempo"}
    assert set(fato.column_names) <= esperado


@given(st.lists(st_dados_alunos_record(), min_size=0, max_size=15))
def test_fact_alunos_preserves_row_count_and_columns(records):
    tabela = to_pyarrow_table(records, _ALUNOS_SCHEMA)
    fato = build_fact_alunos(tabela)
    assert fato.num_rows == tabela.num_rows
    assert set(fato.column_names) <= set(FACT_ALUNOS_COLUMNS) | {"sk_municipio", "sk_serie", "sk_rede", "sk_tempo"}


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
        "nome": [nomes[s] for s in siglas],
    })


@given(st_uf_dim_source())
def test_dim_uf_is_distinct_by_sigla(tabela):
    dim = build_dim_uf(tabela)
    valores = dim.column("sigla_uf").to_pylist()
    assert len(valores) == len(set(valores))


@given(st_uf_dim_source())
def test_dim_uf_keys_match_diretorio(tabela):
    """Completeness + no extras: chaves da dim == chaves do diretório."""
    dim = build_dim_uf(tabela)
    assert set(dim.column("sigla_uf").to_pylist()) == set(tabela.column("sigla_uf").to_pylist())
    assert dim.num_rows == len(set(tabela.column("sigla_uf").to_pylist()))


@given(st_uf_dim_source())
def test_dim_uf_is_idempotent(tabela):
    primeira = build_dim_uf(tabela)
    segunda = build_dim_uf(primeira)
    assert primeira.to_pylist() == segunda.to_pylist()


@given(st_uf_dim_source())
def test_dim_uf_duplicating_source_rows_keeps_same_cardinality(tabela):
    """Metamorphic: duplicar linhas no diretório não altera a dim (DISTINCT)."""
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
        n_dup = draw(st.integers(min_value=1, max_value=3))
        for _ in range(n_dup):
            linhas.append({
                "id_municipio": id_mun,
                "nome": f"Cidade {id_mun}",
                "sigla_uf": "SP",
                "nome_regiao": "Sudeste",
                "capital_uf": "0",
            })
    return pa.Table.from_pylist(linhas)


@given(st_municipio_dim_source())
def test_dim_municipio_unique_per_id(tabela):
    dim = build_dim_municipio(tabela)
    valores = dim.column("id_municipio").to_pylist()
    assert len(valores) == len(set(valores)) == len(set(tabela.column("id_municipio").to_pylist()))


@given(st_municipio_dim_source())
def test_dim_municipio_keys_match_diretorio(tabela):
    """Completeness + no extras: chaves da dim == chaves do diretório."""
    dim = build_dim_municipio(tabela)
    assert set(dim.column("id_municipio").to_pylist()) == set(tabela.column("id_municipio").to_pylist())


@given(st_municipio_dim_source())
def test_dim_municipio_is_idempotent(tabela):
    primeira = build_dim_municipio(tabela)
    segunda = build_dim_municipio(primeira)
    assert primeira.to_pylist() == segunda.to_pylist()


@given(st_municipio_dim_source())
def test_dim_municipio_duplicating_source_rows_keeps_same_dim(tabela):
    """Metamorphic: duplicar linhas no diretório não altera a dim (DISTINCT)."""
    duplicada = pa.concat_tables([tabela, tabela])
    assert build_dim_municipio(duplicada).to_pylist() == build_dim_municipio(tabela).to_pylist()


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


# --- surrogate keys: determinismo, unicidade por chave, nulidade ---


@given(st.text(min_size=1, max_size=20))
def test_surrogate_key_is_deterministic(chave):
    assert surrogate_key("municipio", chave) == surrogate_key("municipio", chave)


@given(st.integers(min_value=1900, max_value=2100))
def test_surrogate_key_accepts_int_natural_key(ano):
    """`dim_tempo` usa o ano (INT64) como chave natural — a SK não pode
    assumir que a chave é string."""
    assert surrogate_key("tempo", ano) == surrogate_key("tempo", ano)
    assert isinstance(surrogate_key("tempo", ano), int)


@given(st.lists(st.from_regex(r"[0-9]{7}", fullmatch=True), min_size=1, max_size=20, unique=True))
def test_surrogate_key_unique_per_distinct_key(chaves):
    sks = [surrogate_key("municipio", c) for c in chaves]
    assert len(set(sks)) == len(chaves)


@given(st_municipio_dim_source())
def test_dim_municipio_sk_unique_and_matches_natural_key(tabela):
    """Unicidade: uma SK por id_municipio distinto; e a SK é a função hash da
    chave natural (não um sequencial da ordem de leitura)."""
    dim = build_dim_municipio(tabela)
    sks = dim.column("sk_municipio").to_pylist()
    ids = dim.column("id_municipio").to_pylist()
    assert len(set(sks)) == len(set(ids)) == dim.num_rows
    for sk, id_mun in zip(sks, ids):
        assert sk == surrogate_key("municipio", id_mun)


# --- dim_tempo: completude, idempotência, metamorfose ---


_st_anos = st.lists(st.one_of(st.none(), st.integers(min_value=2010, max_value=2035)), max_size=20)


@given(_st_anos)
def test_dim_tempo_covers_every_input_year(anos):
    """Completude: todo ano não-nulo da entrada está na dimensão."""
    dim = build_dim_tempo(anos)
    cobertura = set(dim.column("ano").to_pylist())
    assert {a for a in anos if a is not None} <= cobertura
    assert set(META_ANOS) <= cobertura


@given(_st_anos)
def test_dim_tempo_grain_is_unique(anos):
    dim = build_dim_tempo(anos)
    valores = dim.column("ano").to_pylist()
    assert len(valores) == len(set(valores))


@given(_st_anos)
def test_dim_tempo_is_idempotent(anos):
    assert build_dim_tempo(anos).to_pylist() == build_dim_tempo(anos).to_pylist()


@given(_st_anos)
def test_dim_tempo_duplicating_years_changes_nothing(anos):
    """Metamórfica: duplicar os anos de entrada não altera a dimensão."""
    assert build_dim_tempo(anos + anos).to_pylist() == build_dim_tempo(anos).to_pylist()


@given(_st_anos)
def test_dim_tempo_order_of_input_is_irrelevant(anos):
    """Metamórfica: permutar a entrada não altera a saída (ela é ordenada)."""
    reverso = list(reversed(anos))
    assert build_dim_tempo(reverso).to_pylist() == build_dim_tempo(anos).to_pylist()


# --- Integridade referencial por construção: fato x dim da mesma fonte ---


@given(st.lists(st_municipio_record(), min_size=1, max_size=15))
def test_fact_indicador_municipio_sks_exist_in_dim_built_from_same_keys(records):
    """Toda sk_municipio/sk_tempo não-nula do fato existe na dimensão
    construída sobre as mesmas chaves naturais — a FK nunca fica órfã quando
    fato e dim derivam da mesma fonte."""
    tabela = to_pyarrow_table(records, _MUNICIPIO_SCHEMA)
    fato = build_fact_indicador_municipio(tabela)

    ids = [i for i in tabela.column("id_municipio").to_pylist() if i is not None]
    diretorio = pa.table({
        "id_municipio": ids,
        "nome": ["M"] * len(ids),
        "sigla_uf": ["SP"] * len(ids),
        "nome_regiao": ["Sudeste"] * len(ids),
        "capital_uf": pa.array([1] * len(ids), type=pa.int64()),
    })
    dim_municipio = build_dim_municipio(diretorio)
    dim_tempo = build_dim_tempo(tabela.column("ano").to_pylist())

    sks_municipio_validas = set(dim_municipio.column("sk_municipio").to_pylist())
    sks_tempo_validas = set(dim_tempo.column("sk_tempo").to_pylist())
    for linha in fato.to_pylist():
        if linha["sk_municipio"] is not None:
            assert linha["sk_municipio"] in sks_municipio_validas
        if linha["sk_tempo"] is not None:
            assert linha["sk_tempo"] in sks_tempo_validas


# --- fact_alfabetizacao_municipio: completude, idempotência, derivação ---


@st.composite
def st_integrada_source(draw):
    """Linhas no formato da tabela integrada da Silver (grão + medidas + meta)."""
    n = draw(st.integers(min_value=1, max_value=10))
    linhas = []
    for _ in range(n):
        ano = draw(st.integers(min_value=2019, max_value=2030))
        tem_meta = draw(st.booleans()) and ano in META_ANOS
        linhas.append({
            "ano": ano,
            "id_municipio": draw(st.from_regex(r"[0-9]{7}", fullmatch=True)),
            "rede": draw(st.sampled_from(["0", "1", "2"])),
            "serie": "2",
            "taxa_alfabetizacao": draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)),
            "media_portugues": draw(st.floats(min_value=400.0, max_value=900.0, allow_nan=False, allow_infinity=False)),
            "meta_indicador": draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)) if tem_meta else None,
            "percentual_participacao": draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)) if tem_meta else None,
            "nivel_alfabetizacao": draw(st.integers(min_value=0, max_value=3)) if tem_meta else None,
        })
    return pa.Table.from_pylist(linhas)


@given(st_integrada_source())
def test_fact_alfabetizacao_municipio_preserves_row_count(tabela):
    fato = build_fact_alfabetizacao_municipio(tabela)
    assert fato.num_rows == tabela.num_rows


@given(st_integrada_source())
def test_fact_alfabetizacao_municipio_gap_and_atingiu_meta_are_derived(tabela):
    fato = build_fact_alfabetizacao_municipio(tabela)
    for linha in fato.to_pylist():
        meta = linha["meta_indicador"]
        taxa = linha["taxa_alfabetizacao"]
        if meta is None:
            assert linha["gap_pontos"] is None
            assert linha["atingiu_meta"] is None
        else:
            assert linha["gap_pontos"] == taxa - meta
            assert linha["atingiu_meta"] == (taxa >= meta)


@given(st_integrada_source())
def test_fact_alfabetizacao_municipio_is_idempotent(tabela):
    primeira = build_fact_alfabetizacao_municipio(tabela)
    segunda = build_fact_alfabetizacao_municipio(tabela)
    assert primeira.to_pylist() == segunda.to_pylist()


@given(st_integrada_source())
def test_fact_alfabetizacao_municipio_row_order_is_irrelevant(tabela):
    """Metamórfica: permutar as linhas de entrada não altera o conjunto de
    saída (SKs incluídas) — só a ordem física das linhas."""
    invertida = tabela.take(pa.array(list(reversed(range(tabela.num_rows))), type=pa.int64()))
    conjunto = lambda t: sorted((tuple(sorted(r.items())) for r in t.to_pylist()), key=repr)
    assert conjunto(build_fact_alfabetizacao_municipio(tabela)) == conjunto(build_fact_alfabetizacao_municipio(invertida))
