"""Property-Based Testing (Hypothesis) das funções puras de silver.transform —
round-trip e invariantes de normalização/limpeza."""

import io

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import given
from hypothesis import strategies as st

from silver.transform import clean, normalize_key


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
