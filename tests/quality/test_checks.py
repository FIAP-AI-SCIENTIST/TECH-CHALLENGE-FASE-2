"""Testes do módulo quality.checks — regras puras de qualidade de dados."""

import pyarrow as pa

from quality.checks import (
    check_duplicates,
    check_missing_values,
    check_referential_integrity,
    check_value_range,
)


class TestCheckDuplicates:
    def test_passes_when_no_duplicates(self):
        tabela = pa.table({"ano": [2023, 2024], "sigla_uf": ["SP", "SP"]})
        resultado = check_duplicates("uf", tabela, ["ano", "sigla_uf"])
        assert resultado.passou is True
        assert resultado.linhas_afetadas == 0

    def test_fails_when_key_repeats(self):
        tabela = pa.table({"ano": [2023, 2023], "sigla_uf": ["SP", "SP"]})
        resultado = check_duplicates("uf", tabela, ["ano", "sigla_uf"])
        assert resultado.passou is False
        assert resultado.linhas_afetadas == 1

    def test_empty_table_passes(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64()), "sigla_uf": pa.array([], type=pa.string())})
        resultado = check_duplicates("uf", tabela, ["ano", "sigla_uf"])
        assert resultado.passou is True

    def test_missing_key_column_passes_without_erroring(self):
        tabela = pa.table({"ano": [2023]})
        resultado = check_duplicates("uf", tabela, ["ano", "sigla_uf"])
        assert resultado.passou is True


class TestCheckMissingValues:
    def test_passes_when_no_nulls(self):
        tabela = pa.table({"ano": [2023, 2024], "sigla_uf": ["SP", "RJ"]})
        resultado = check_missing_values("uf", tabela, ["ano", "sigla_uf"])
        assert resultado.passou is True

    def test_fails_when_required_column_has_null(self):
        tabela = pa.table({"ano": [2023, None], "sigla_uf": ["SP", "RJ"]})
        resultado = check_missing_values("uf", tabela, ["ano"])
        assert resultado.passou is False
        assert resultado.linhas_afetadas == 1

    def test_empty_table_passes(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64())})
        resultado = check_missing_values("uf", tabela, ["ano"])
        assert resultado.passou is True


class TestCheckReferentialIntegrity:
    def test_passes_when_all_keys_resolve(self):
        tabela = pa.table({"sigla_uf": ["SP", "RJ"]})
        resultado = check_referential_integrity("fact_indicador_uf", tabela, "sigla_uf", {"SP", "RJ"})
        assert resultado.passou is True

    def test_fails_on_orphan_key(self):
        tabela = pa.table({"sigla_uf": ["SP", "XX"]})
        resultado = check_referential_integrity("fact_indicador_uf", tabela, "sigla_uf", {"SP"})
        assert resultado.passou is False
        assert resultado.linhas_afetadas == 1

    def test_null_values_are_not_orphans(self):
        tabela = pa.table({"sigla_uf": ["SP", None]})
        resultado = check_referential_integrity("fact_indicador_uf", tabela, "sigla_uf", {"SP"})
        assert resultado.passou is True


class TestCheckValueRange:
    def test_passes_within_range(self):
        tabela = pa.table({"taxa_alfabetizacao": [50.0, 90.0]})
        resultado = check_value_range("uf", tabela, "taxa_alfabetizacao", 0.0, 100.0)
        assert resultado.passou is True

    def test_fails_outside_range(self):
        tabela = pa.table({"taxa_alfabetizacao": [50.0, 150.0, -1.0]})
        resultado = check_value_range("uf", tabela, "taxa_alfabetizacao", 0.0, 100.0)
        assert resultado.passou is False
        assert resultado.linhas_afetadas == 2

    def test_nulls_do_not_count_as_out_of_range(self):
        tabela = pa.table({"taxa_alfabetizacao": [50.0, None]})
        resultado = check_value_range("uf", tabela, "taxa_alfabetizacao", 0.0, 100.0)
        assert resultado.passou is True
