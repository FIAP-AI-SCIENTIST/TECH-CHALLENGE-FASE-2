"""Testes do módulo silver.transform — limpeza, dedup, agrupamento e SCD2."""

import pyarrow as pa

from silver.transform import (
    apply_scd2,
    clean,
    dedupe,
    group_by_ano,
    normalize_key,
)


class TestNormalizeKey:
    """Verifica normalização de id_municipio (7 dígitos IBGE)."""

    def test_pads_short_code(self):
        assert normalize_key("550308") == "0550308"

    def test_keeps_already_7_digits(self):
        assert normalize_key("3550308") == "3550308"

    def test_none_returns_none(self):
        assert normalize_key(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_key("") is None
        assert normalize_key("   ") is None

    def test_non_numeric_returns_none(self):
        assert normalize_key("abc1234") is None

    def test_too_many_digits_returns_none(self):
        assert normalize_key("123456789") is None


class TestClean:
    """Verifica tradução de código, normalização de chave e rejeição."""

    def test_normalizes_id_municipio_and_translates_rede(self):
        tabela = pa.table({
            "ano": [2023],
            "id_municipio": ["550308"],
            "serie": ["2"],
            "rede": ["2"],
            "taxa_alfabetizacao": [80.0],
        })
        referencias = {
            "rede": {"2": "Estadual"},
            "serie": {"2": "2° ano do Ensino Fundamental"},
            "diretorio_municipio": {
                "0550308": {"nome": "São Paulo", "sigla_uf": "SP", "nome_regiao": "Sudeste", "capital_uf": 1}
            },
        }

        limpa, rejeitadas = clean("municipio", tabela, referencias)

        assert rejeitadas == 0
        assert limpa.column("id_municipio").to_pylist() == ["0550308"]
        assert limpa.column("rede_desc").to_pylist() == ["Estadual"]
        assert limpa.column("nome").to_pylist() == ["São Paulo"]
        assert limpa.column("sigla_uf").to_pylist() == ["SP"]

    def test_rejects_invalid_id_municipio(self):
        tabela = pa.table({
            "ano": [2023, 2023],
            "id_municipio": ["550308", "abc"],
            "serie": ["2", "2"],
            "rede": ["2", "2"],
        })

        limpa, rejeitadas = clean("municipio", tabela, {})

        assert rejeitadas == 1
        assert limpa.num_rows == 1
        assert limpa.column("id_municipio").to_pylist() == ["0550308"]

    def test_empty_table_returns_empty(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64())})
        limpa, rejeitadas = clean("uf", tabela, {})
        assert limpa.num_rows == 0
        assert rejeitadas == 0

    def test_alunos_booleans_cast_from_code_strings(self):
        tabela = pa.table({
            "ano": [2023, 2023],
            "id_municipio": ["3550308", "3550308"],
            "id_aluno": ["a1", "a2"],
            "alfabetizado": ["1", "0"],
            "presenca": ["1", "1"],
            "preenchimento_caderno": ["1", "1"],
        })

        limpa, _ = clean("alunos", tabela, {})

        assert limpa.column("alfabetizado").to_pylist() == [True, False]
        assert limpa.column("presenca").to_pylist() == [True, True]


class TestDedupe:
    """Verifica deduplicação por chave natural."""

    def test_removes_duplicate_streaming_reentries(self):
        tabela = pa.table({
            "ano": [2023, 2023, 2023],
            "id_aluno": ["a1", "a1", "a2"],
            "proficiencia": [500.0, 500.0, 600.0],
        })

        result = dedupe("alunos", tabela)

        assert result.num_rows == 2
        assert sorted(result.column("id_aluno").to_pylist()) == ["a1", "a2"]

    def test_keeps_last_occurrence_on_tie(self):
        tabela = pa.table({
            "ano": [2023, 2023],
            "id_aluno": ["a1", "a1"],
            "proficiencia": [500.0, 999.0],
        })

        result = dedupe("alunos", tabela)

        assert result.num_rows == 1
        assert result.column("proficiencia").to_pylist() == [999.0]

    def test_empty_table_returns_empty(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64()), "id_aluno": pa.array([], type=pa.string())})
        result = dedupe("alunos", tabela)
        assert result.num_rows == 0


class TestGroupByAno:
    """Verifica reagrupamento por ano (batch + streaming misturados)."""

    def test_separates_multiple_years(self):
        tabela = pa.table({
            "ano": [2023, 2024, 2023],
            "sigla_uf": ["SP", "SP", "RJ"],
        })

        grupos = group_by_ano(tabela)

        assert set(grupos.keys()) == {2023, 2024}
        assert grupos[2023].num_rows == 2
        assert grupos[2024].num_rows == 1

    def test_empty_table_returns_empty_dict(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64())})
        assert group_by_ano(tabela) == {}


class TestApplyScd2:
    """Verifica versionamento SCD Tipo 2."""

    _SCHEMA = pa.schema([
        pa.field("sigla_uf", pa.string()),
        pa.field("rede", pa.string()),
        pa.field("meta_alfabetizacao_2024", pa.float64()),
        pa.field("meta_alfabetizacao_2025", pa.float64()),
        pa.field("meta_alfabetizacao_2026", pa.float64()),
        pa.field("meta_alfabetizacao_2027", pa.float64()),
        pa.field("meta_alfabetizacao_2028", pa.float64()),
        pa.field("meta_alfabetizacao_2029", pa.float64()),
        pa.field("meta_alfabetizacao_2030", pa.float64()),
        pa.field("percentual_participacao", pa.float64()),
    ])

    def _make_incoming(self, sigla_uf="SP", rede="0", meta_2024=50.0):
        return pa.Table.from_pylist([{
            "sigla_uf": sigla_uf,
            "rede": rede,
            "meta_alfabetizacao_2024": meta_2024,
            "meta_alfabetizacao_2025": 55.0,
            "meta_alfabetizacao_2026": 60.0,
            "meta_alfabetizacao_2027": 65.0,
            "meta_alfabetizacao_2028": 70.0,
            "meta_alfabetizacao_2029": 80.0,
            "meta_alfabetizacao_2030": 100.0,
            "percentual_participacao": 90.0,
        }], schema=self._SCHEMA)

    def test_new_key_opens_first_version(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        incoming = self._make_incoming()

        result = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, incoming, ano=2023)

        assert result.num_rows == 1
        row = result.to_pylist()[0]
        assert row["valid_from"] == 2023
        assert row["valid_to"] is None
        assert row["is_current"] is True

    def test_reextraction_with_same_values_does_not_version(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(), ano=2023)

        segunda = apply_scd2("meta_alfabetizacao_uf", primeira, self._make_incoming(), ano=2024)

        assert segunda.num_rows == 1  # nao duplicou versao
        row = segunda.to_pylist()[0]
        assert row["valid_from"] == 2023  # continua a versao original
        assert row["is_current"] is True

    def test_changed_value_closes_old_and_opens_new_version(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(meta_2024=50.0), ano=2023)

        segunda = apply_scd2(
            "meta_alfabetizacao_uf", primeira, self._make_incoming(meta_2024=99.0), ano=2024
        )

        assert segunda.num_rows == 2
        rows = sorted(segunda.to_pylist(), key=lambda r: r["valid_from"])
        assert rows[0]["valid_from"] == 2023
        assert rows[0]["valid_to"] == 2024
        assert rows[0]["is_current"] is False
        assert rows[1]["valid_from"] == 2024
        assert rows[1]["valid_to"] is None
        assert rows[1]["is_current"] is True
        assert rows[1]["meta_alfabetizacao_2024"] == 99.0

    def test_at_most_one_current_version_per_key(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(meta_2024=1.0), ano=2020)
        segunda = apply_scd2("meta_alfabetizacao_uf", primeira, self._make_incoming(meta_2024=2.0), ano=2021)
        terceira = apply_scd2("meta_alfabetizacao_uf", segunda, self._make_incoming(meta_2024=3.0), ano=2022)

        currents = [r for r in terceira.to_pylist() if r["is_current"]]
        assert len(currents) == 1
        assert currents[0]["meta_alfabetizacao_2024"] == 3.0

    def test_key_missing_from_incoming_stays_unchanged(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(sigla_uf="SP"), ano=2023)

        # incoming desta vez só traz RJ — SP não aparece
        outro_incoming = self._make_incoming(sigla_uf="RJ")
        segunda = apply_scd2("meta_alfabetizacao_uf", primeira, outro_incoming, ano=2024)

        siglas = {r["sigla_uf"] for r in segunda.to_pylist()}
        assert siglas == {"SP", "RJ"}
        sp_row = next(r for r in segunda.to_pylist() if r["sigla_uf"] == "SP")
        assert sp_row["is_current"] is True  # nao foi tocada
