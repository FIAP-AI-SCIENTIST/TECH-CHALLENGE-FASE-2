"""Property-Based Testing (Hypothesis) das funções puras de silver.transform —
round-trip e invariantes de normalização/limpeza."""

import io

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import given
from hypothesis import strategies as st

from silver.transform import clean, integrate_alfabetizacao_municipio, normalize_key

_META_ANOS = list(range(2024, 2031))


# --- Invariante: normalize_key sempre devolve 7 dígitos numéricos ---

@given(st.from_regex(r"[0-9]{1,7}", fullmatch=True))
def test_normalize_key_always_returns_7_digits_for_numeric_input(codigo):
    resultado = normalize_key(codigo)
    assert resultado is not None
    assert len(resultado) == 7
    assert resultado.isdigit()


@given(st.from_regex(r"[0-9]{1,7}", fullmatch=True))
def test_normalize_key_is_idempotent(codigo):
    """normalize_key(normalize_key(x)) == normalize_key(x)."""
    uma_vez = normalize_key(codigo)
    duas_vezes = normalize_key(uma_vez)
    assert uma_vez == duas_vezes


@given(st.text(alphabet="abcdefghij!@#", min_size=1, max_size=10))
def test_normalize_key_rejects_non_numeric(texto):
    assert normalize_key(texto) is None


@given(st.from_regex(r"[0-9]{8,15}", fullmatch=True))
def test_normalize_key_rejects_too_many_digits(codigo):
    assert normalize_key(codigo) is None


# --- Round-trip: saída da Silver serializa/desserializa em Parquet sem perda ---

@given(
    st.lists(
        st.fixed_dictionaries({
            "ano": st.integers(min_value=2020, max_value=2030),
            "sigla_uf": st.sampled_from(["SP", "RJ", "MG", "BA"]),
            "serie": st.just("2"),
            "rede": st.sampled_from(["0", "1", "2", "3", "4"]),
            "taxa_alfabetizacao": st.floats(min_value=0, max_value=100, allow_nan=False),
        }),
        min_size=0,
        max_size=20,
    )
)
def test_clean_output_roundtrips_through_parquet(linhas):
    if not linhas:
        tabela = pa.table({
            "ano": pa.array([], type=pa.int64()),
            "sigla_uf": pa.array([], type=pa.string()),
            "serie": pa.array([], type=pa.string()),
            "rede": pa.array([], type=pa.string()),
            "taxa_alfabetizacao": pa.array([], type=pa.float64()),
        })
    else:
        tabela = pa.Table.from_pylist(linhas)

    limpa, _ = clean("uf", tabela, {})

    buffer = io.BytesIO()
    pq.write_table(limpa, buffer)
    buffer.seek(0)
    relida = pq.read_table(buffer)

    assert relida.num_rows == limpa.num_rows
    assert set(relida.column_names) == set(limpa.column_names)


# --- Invariante de domínio: taxa_alfabetizacao permanece em [0, 100] após clean ---

@given(
    st.lists(
        st.floats(min_value=0, max_value=100, allow_nan=False),
        min_size=1,
        max_size=30,
    )
)
def test_clean_preserves_taxa_alfabetizacao_range(taxas):
    tabela = pa.table({
        "ano": [2023] * len(taxas),
        "sigla_uf": ["SP"] * len(taxas),
        "serie": ["2"] * len(taxas),
        "rede": ["0"] * len(taxas),
        "taxa_alfabetizacao": taxas,
    })

    limpa, rejeitadas = clean("uf", tabela, {})

    assert rejeitadas == 0
    valores = limpa.column("taxa_alfabetizacao").to_pylist()
    assert all(0 <= v <= 100 for v in valores)
    assert valores == taxas  # clean nao altera valores numericos ja validos


# --- Integração indicador × meta (SCD2): completude, unicidade, metamorfose ---


@st.composite
def st_indicador_municipio(draw):
    """Indicador observado com grão (ano, id_municipio, rede, serie) único —
    pré-condição que a Silver regular já garante por dedupe."""
    n = draw(st.integers(min_value=1, max_value=10))
    chaves = draw(st.lists(
        st.tuples(
            st.integers(min_value=2019, max_value=2030),
            st.from_regex(r"[0-9]{7}", fullmatch=True),
            st.sampled_from(["1", "2"]),
        ),
        min_size=n, max_size=n, unique=True,
    ))
    linhas = [{
        "ano": ano,
        "id_municipio": id_mun,
        "rede": rede,
        "serie": "2",
        "taxa_alfabetizacao": draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)),
    } for ano, id_mun, rede in chaves]
    return pa.Table.from_pylist(linhas)


@st.composite
def st_meta_scd2_uma_versao(draw):
    """SCD2 bem-formada: no máximo uma versão por (id_municipio, rede), sempre
    vigente a partir de `valid_from` (valid_to NULL) — o caso em que o JOIN
    temporal não pode multiplicar linhas."""
    n = draw(st.integers(min_value=0, max_value=5))
    if n == 0:
        return None
    chaves = draw(st.lists(
        st.tuples(
            st.from_regex(r"[0-9]{7}", fullmatch=True),
            st.sampled_from(["1", "2"]),
        ),
        min_size=n, max_size=n, unique=True,
    ))
    linhas = []
    for id_mun, rede in chaves:
        linha = {
            "ano": 2023,
            "id_municipio": id_mun,
            "rede": rede,
            "percentual_participacao": draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)),
            "nivel_alfabetizacao": draw(st.integers(min_value=0, max_value=3)),
            "valid_from": draw(st.integers(min_value=2019, max_value=2024)),
            "valid_to": None,
            "is_current": True,
        }
        for a in _META_ANOS:
            linha[f"meta_alfabetizacao_{a}"] = draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False))
        linhas.append(linha)
    return pa.Table.from_pylist(linhas)


def _conjunto(tabela: pa.Table) -> list:
    """Linhas como conjunto ordenável — JOIN não garante ordem física."""
    return sorted((tuple(sorted(r.items())) for r in tabela.to_pylist()), key=repr)


@given(st_indicador_municipio(), st_meta_scd2_uma_versao())
def test_integracao_preserva_row_count(indicador, meta):
    """Completude: LEFT JOIN nunca perde linha do indicador; com a SCD2
    bem-formada (uma versão vigente por chave), também nunca multiplica."""
    integrada = integrate_alfabetizacao_municipio(indicador, meta)
    assert integrada.num_rows == indicador.num_rows


@given(st_indicador_municipio(), st_meta_scd2_uma_versao())
def test_integracao_preserva_unicidade_do_grao(indicador, meta):
    integrada = integrate_alfabetizacao_municipio(indicador, meta)
    graos = [(r["ano"], r["id_municipio"], r["rede"], r["serie"]) for r in integrada.to_pylist()]
    assert len(graos) == len(set(graos))


@given(st_indicador_municipio(), st_meta_scd2_uma_versao())
def test_integracao_meta_indicador_eh_a_meta_do_proprio_ano(indicador, meta):
    """Regra de negócio central: quando há match temporal, meta_indicador é
    exatamente a coluna meta_alfabetizacao_{ano} da versão vigente; sem match
    ou fora do horizonte 2024–2030, é NULL."""
    integrada = integrate_alfabetizacao_municipio(indicador, meta)
    metas_por_chave = {}
    if meta is not None:
        for m in meta.to_pylist():
            metas_por_chave[(m["id_municipio"], m["rede"])] = m
    for row in integrada.to_pylist():
        versao = metas_por_chave.get((row["id_municipio"], row["rede"]))
        esperado = None
        if versao is not None and row["ano"] >= versao["valid_from"] and row["ano"] in _META_ANOS:
            esperado = versao[f"meta_alfabetizacao_{row['ano']}"]
        assert row["meta_indicador"] == esperado


@given(st_indicador_municipio(), st_meta_scd2_uma_versao())
def test_integracao_is_deterministic(indicador, meta):
    primeira = integrate_alfabetizacao_municipio(indicador, meta)
    segunda = integrate_alfabetizacao_municipio(indicador, meta)
    assert _conjunto(primeira) == _conjunto(segunda)


@given(st_indicador_municipio(), st_meta_scd2_uma_versao())
def test_integracao_row_order_is_irrelevant(indicador, meta):
    """Metamórfica: permutar as linhas do indicador não altera o conjunto de
    saída — o JOIN é função do conteúdo, não da ordem física."""
    permutado = indicador.take(pa.array(list(reversed(range(indicador.num_rows))), type=pa.int64()))
    assert _conjunto(integrate_alfabetizacao_municipio(permutado, meta)) == _conjunto(integrate_alfabetizacao_municipio(indicador, meta))
