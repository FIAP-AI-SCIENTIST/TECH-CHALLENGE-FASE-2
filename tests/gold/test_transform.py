"""Testes do módulo gold.transform — dimensões e fatos do modelo Kimball."""

import pyarrow as pa

from gold.transform import (
    build_dim_municipio,
    build_dim_rede,
    build_dim_serie,
    build_dim_uf,
    build_fact_alunos,
    build_fact_indicador_municipio,
    build_fact_indicador_uf,
    build_fact_meta_resultado,
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
        assert set(dim.column_names) == {"sigla_uf", "nome"}


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
        assert set(dim.column_names) == {"serie", "serie_desc"}


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
