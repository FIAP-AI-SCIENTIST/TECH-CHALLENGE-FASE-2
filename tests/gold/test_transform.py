"""Testes do módulo gold.transform — dimensões e fatos do modelo Kimball."""

import pyarrow as pa

from gold.transform import (
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
    with_surrogate_keys,
)


class TestBuildDimUf:
    def test_distinct_by_sigla(self):
        diretorio = pa.table({
            "sigla_uf": ["SP", "SP", "RJ"],
            "nome": ["São Paulo", "São Paulo", "Rio de Janeiro"],
        })
        dim = build_dim_uf(diretorio)
        assert dim.num_rows == 2
        assert set(dim.column("sigla_uf").to_pylist()) == {"SP", "RJ"}

    def test_missing_columns_returns_empty(self):
        diretorio = pa.table({"ano": [2023]})
        dim = build_dim_uf(diretorio)
        assert dim.num_rows == 0
        assert set(dim.column_names) == {"sk_uf", "sigla_uf", "nome"}


class TestBuildDimMunicipio:
    def test_distinct_by_id(self):
        diretorio = pa.table({
            "id_municipio": ["3550308", "3550308", "3304557"],
            "nome": ["São Paulo", "São Paulo", "Rio de Janeiro"],
            "sigla_uf": ["SP", "SP", "RJ"],
            "nome_regiao": ["Sudeste", "Sudeste", "Sudeste"],
            "capital_uf": [1, 1, 1],
        })
        dim = build_dim_municipio(diretorio)
        assert dim.num_rows == 2
        assert set(dim.column("id_municipio").to_pylist()) == {"3550308", "3304557"}

    def test_missing_id_returns_empty(self):
        diretorio = pa.table({"ano": [2023]})
        dim = build_dim_municipio(diretorio)
        assert dim.num_rows == 0


class TestBuildDimCodigo:
    def test_rede_distinct_across_sources(self):
        uf = pa.table({"rede": ["0", "2"], "rede_desc": ["Total", "Estadual"]})
        municipio = pa.table({"rede": ["0"], "rede_desc": ["Total"]})
        dim = build_dim_rede(uf, municipio)
        assert dim.num_rows == 2
        assert set(dim.column("rede").to_pylist()) == {"0", "2"}

    def test_serie_missing_from_all_sources_returns_empty(self):
        uf = pa.table({"ano": [2023]})
        dim = build_dim_serie(uf)
        assert dim.num_rows == 0
        assert set(dim.column_names) == {"sk_serie", "serie", "serie_desc"}


class TestBuildFactIndicador:
    def test_uf_projects_measures_only(self):
        uf = pa.table({
            "ano": [2023],
            "sigla_uf": ["SP"],
            "serie": ["2"],
            "rede": ["0"],
            "rede_desc": ["Total"],
            "sigla_uf_nome": ["São Paulo"],
            "taxa_alfabetizacao": [80.0],
            "media_portugues": [700.0],
        })
        fato = build_fact_indicador_uf(uf)
        assert set(fato.column_names) >= {"ano", "sigla_uf", "serie", "rede", "taxa_alfabetizacao"}
        assert "rede_desc" not in fato.column_names
        assert "sigla_uf_nome" not in fato.column_names

    def test_municipio_projects_measures_only(self):
        municipio = pa.table({
            "ano": [2023],
            "id_municipio": ["3550308"],
            "serie": ["2"],
            "rede": ["0"],
            "nome": ["São Paulo"],
            "taxa_alfabetizacao": [80.0],
        })
        fato = build_fact_indicador_municipio(municipio)
        assert "nome" not in fato.column_names
        assert fato.column("id_municipio").to_pylist() == ["3550308"]


class TestBuildFactAlunos:
    def test_projects_expected_columns(self):
        alunos = pa.table({
            "ano": [2023],
            "id_municipio": ["3550308"],
            "id_aluno": ["a1"],
            "proficiencia": [650.0],
            "caderno_extra_nao_usado": ["x"],
        })
        fato = build_fact_alunos(alunos)
        assert "caderno_extra_nao_usado" not in fato.column_names
        assert fato.column("proficiencia").to_pylist() == [650.0]


class TestBuildFactMetaResultado:
    _SCHEMA = pa.schema([
        pa.field("sigla_uf", pa.string()),
        pa.field("rede", pa.string()),
        pa.field("ano", pa.int64()),
        pa.field("taxa_alfabetizacao", pa.float64()),
        pa.field("meta_alfabetizacao_2024", pa.float64()),
        pa.field("meta_alfabetizacao_2025", pa.float64()),
        pa.field("meta_alfabetizacao_2026", pa.float64()),
        pa.field("meta_alfabetizacao_2027", pa.float64()),
        pa.field("meta_alfabetizacao_2028", pa.float64()),
        pa.field("meta_alfabetizacao_2029", pa.float64()),
        pa.field("meta_alfabetizacao_2030", pa.float64()),
        pa.field("percentual_participacao", pa.float64()),
        pa.field("valid_from", pa.int64()),
        pa.field("valid_to", pa.int64()),
        pa.field("is_current", pa.bool_()),
    ])

    def test_computes_gap_for_year_in_range(self):
        scd2 = pa.Table.from_pylist([{
            "sigla_uf": "SP", "rede": "0", "ano": 2024, "taxa_alfabetizacao": 60.0,
            "meta_alfabetizacao_2024": 70.0, "meta_alfabetizacao_2025": 75.0,
            "meta_alfabetizacao_2026": 80.0, "meta_alfabetizacao_2027": 85.0,
            "meta_alfabetizacao_2028": 90.0, "meta_alfabetizacao_2029": 95.0,
            "meta_alfabetizacao_2030": 100.0, "percentual_participacao": 90.0,
            "valid_from": 2024, "valid_to": None, "is_current": True,
        }], schema=self._SCHEMA)

        fato = build_fact_meta_resultado(scd2, ["sigla_uf", "rede"])

        assert fato.num_rows == 1
        row = fato.to_pylist()[0]
        assert row["meta_indicador"] == 70.0
        assert row["gap_pontos"] == -10.0
        assert row["atingiu_meta"] is False

    def test_year_without_target_column_returns_null_gap(self):
        scd2 = pa.Table.from_pylist([{
            "sigla_uf": "SP", "rede": "0", "ano": 2020, "taxa_alfabetizacao": 60.0,
            "meta_alfabetizacao_2024": 70.0, "meta_alfabetizacao_2025": 75.0,
            "meta_alfabetizacao_2026": 80.0, "meta_alfabetizacao_2027": 85.0,
            "meta_alfabetizacao_2028": 90.0, "meta_alfabetizacao_2029": 95.0,
            "meta_alfabetizacao_2030": 100.0, "percentual_participacao": 90.0,
            "valid_from": 2020, "valid_to": None, "is_current": True,
        }], schema=self._SCHEMA)

        fato = build_fact_meta_resultado(scd2, ["sigla_uf", "rede"])

        row = fato.to_pylist()[0]
        assert row["meta_indicador"] is None
        assert row["gap_pontos"] is None

    def test_empty_table_returns_empty(self):
        scd2 = pa.Table.from_pylist([], schema=self._SCHEMA)
        fato = build_fact_meta_resultado(scd2, ["sigla_uf", "rede"])
        assert fato.num_rows == 0

    def test_adds_surrogate_keys_for_natural_keys_and_tempo(self):
        scd2 = pa.Table.from_pylist([{
            "sigla_uf": "SP", "rede": "0", "ano": 2024, "taxa_alfabetizacao": 60.0,
            "meta_alfabetizacao_2024": 70.0, "meta_alfabetizacao_2025": 75.0,
            "meta_alfabetizacao_2026": 80.0, "meta_alfabetizacao_2027": 85.0,
            "meta_alfabetizacao_2028": 90.0, "meta_alfabetizacao_2029": 95.0,
            "meta_alfabetizacao_2030": 100.0, "percentual_participacao": 90.0,
            "valid_from": 2024, "valid_to": None, "is_current": True,
        }], schema=self._SCHEMA)

        fato = build_fact_meta_resultado(scd2, ["sigla_uf", "rede"])

        row = fato.to_pylist()[0]
        assert row["sk_uf"] == surrogate_key("uf", "SP")
        assert row["sk_rede"] == surrogate_key("rede", "0")
        assert row["sk_tempo"] == surrogate_key("tempo", 2024)


class TestSurrogateKey:
    def test_is_deterministic(self):
        assert surrogate_key("municipio", "3550308") == surrogate_key("municipio", "3550308")

    def test_none_returns_none(self):
        assert surrogate_key("municipio", None) is None

    def test_fits_int64_signed(self):
        for chave in ("3550308", "SP", "0", "2", 2024):
            sk = surrogate_key("municipio", chave)
            assert -(2**63) <= sk < 2**63

    def test_namespace_prevents_cross_dimension_collision(self):
        """O código "2" existe como rede e como série — sem namespace, as duas
        dimensões gerariam a mesma SK e um join errado passaria despercebido."""
        assert surrogate_key("rede", "2") != surrogate_key("serie", "2")

    def test_distinct_keys_give_distinct_sks(self):
        sks = {surrogate_key("municipio", f"{i:07d}") for i in range(1000)}
        assert len(sks) == 1000


class TestWithSurrogateKeys:
    def test_adds_sk_without_touching_existing_columns(self):
        tabela = pa.table({"sigla_uf": ["SP", "RJ"], "nome": ["São Paulo", "Rio de Janeiro"]})
        resultado = with_surrogate_keys(tabela, {"sk_uf": ("uf", "sigla_uf")})
        assert resultado.column("sigla_uf").to_pylist() == ["SP", "RJ"]
        assert resultado.column("sk_uf").to_pylist() == [surrogate_key("uf", "SP"), surrogate_key("uf", "RJ")]
        assert resultado.schema.field("sk_uf").type == pa.int64()

    def test_null_natural_key_gives_null_sk(self):
        tabela = pa.table({"sigla_uf": pa.array(["SP", None], type=pa.string())})
        resultado = with_surrogate_keys(tabela, {"sk_uf": ("uf", "sigla_uf")})
        assert resultado.column("sk_uf").to_pylist()[1] is None

    def test_missing_natural_key_column_is_skipped(self):
        tabela = pa.table({"ano": [2023]})
        resultado = with_surrogate_keys(tabela, {"sk_uf": ("uf", "sigla_uf")})
        assert "sk_uf" not in resultado.column_names

    def test_table_without_columns_passes_through(self):
        tabela = pa.Table.from_pydict({})
        assert with_surrogate_keys(tabela, {"sk_uf": ("uf", "sigla_uf")}).column_names == []


class TestBuildDimTempo:
    def test_covers_observed_years_and_goal_horizon(self):
        dim = build_dim_tempo([2022, 2023])
        anos = dim.column("ano").to_pylist()
        assert {2022, 2023} <= set(anos)
        assert set(range(2024, 2031)) <= set(anos)

    def test_empty_input_still_covers_goal_horizon(self):
        """Mesmo sem nenhum ano observado, o horizonte da meta nacional existe."""
        dim = build_dim_tempo([])
        assert set(dim.column("ano").to_pylist()) == set(range(2024, 2031))

    def test_grain_is_unique_and_sorted(self):
        dim = build_dim_tempo([2023, 2022, 2023, 2025])
        anos = dim.column("ano").to_pylist()
        assert anos == sorted(set(anos))

    def test_derived_attributes(self):
        dim = build_dim_tempo([2023, 2025])
        por_ano = {r["ano"]: r for r in dim.to_pylist()}
        assert por_ano[2023]["ano_tem_meta"] is False
        assert por_ano[2025]["ano_tem_meta"] is True
        assert por_ano[2025]["anos_para_meta_final"] == 5
        assert por_ano[2023]["decada"] == 2020

    def test_sk_matches_tempo_namespace(self):
        dim = build_dim_tempo([2023])
        por_ano = {r["ano"]: r["sk_tempo"] for r in dim.to_pylist()}
        assert por_ano[2023] == surrogate_key("tempo", 2023)

    def test_ignores_null_years(self):
        dim = build_dim_tempo([2023, None])
        assert None not in dim.column("ano").to_pylist()


class TestBuildFactAlfabetizacaoMunicipio:
    def _integrada(self) -> pa.Table:
        return pa.table({
            "ano": [2024, 2024, 2023],
            "id_municipio": ["3550308", "3304557", "3550308"],
            "rede": ["1", "1", "1"],
            "serie": ["2", "2", "2"],
            "taxa_alfabetizacao": [80.0, 60.0, 75.0],
            "media_portugues": [700.0, 650.0, 690.0],
            "meta_indicador": [75.0, 70.0, None],
            "percentual_participacao": [95.0, 90.0, None],
            "nivel_alfabetizacao": pa.array([2, 1, None], type=pa.int64()),
        })

    def test_meta_e_resultado_na_mesma_linha(self):
        fato = build_fact_alfabetizacao_municipio(self._integrada())
        por_chave = {(r["ano"], r["id_municipio"]): r for r in fato.to_pylist()}
        sp2024 = por_chave[(2024, "3550308")]
        assert sp2024["taxa_alfabetizacao"] == 80.0
        assert sp2024["meta_indicador"] == 75.0
        assert sp2024["gap_pontos"] == 5.0
        assert sp2024["atingiu_meta"] is True

    def test_gap_e_atingiu_meta_null_sem_meta(self):
        """Ano fora do horizonte da meta (ou município sem meta): ausência é
        NULL, nunca zero — zero fingiria meta inexistente cumprida/não cumprida."""
        fato = build_fact_alfabetizacao_municipio(self._integrada())
        sp2023 = next(r for r in fato.to_pylist() if r["ano"] == 2023)
        assert sp2023["meta_indicador"] is None
        assert sp2023["gap_pontos"] is None
        assert sp2023["atingiu_meta"] is None

    def test_preserva_chaves_naturais_e_adiciona_sks(self):
        fato = build_fact_alfabetizacao_municipio(self._integrada())
        row = fato.to_pylist()[0]
        assert row["id_municipio"] == "3550308"
        assert row["sk_municipio"] == surrogate_key("municipio", "3550308")
        assert row["sk_tempo"] == surrogate_key("tempo", row["ano"])
        assert row["sk_rede"] == surrogate_key("rede", "1")
        assert row["sk_serie"] == surrogate_key("serie", "2")

    def test_preserves_row_count(self):
        fato = build_fact_alfabetizacao_municipio(self._integrada())
        assert fato.num_rows == 3

    def test_empty_integrada_passes_through(self):
        vazia = pa.Table.from_pydict({})
        assert build_fact_alfabetizacao_municipio(vazia).column_names == []
