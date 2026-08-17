"""Testes das views analíticas — SQL executado de verdade no DuckDB
(prefix=""), verificando os invariantes de cada mart.
"""

from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pytest

from config import get_settings
from gold import marts
from gold.marts import MART_QUERIES, create_view, render_view_ddl
from gold.transform import surrogate_key


def _run(query_nome: str, tabelas: dict[str, pa.Table]) -> list[dict]:
    """Executa a query da mart no DuckDB com as tabelas registradas pelo nome nu."""
    conn = duckdb.connect(":memory:")
    for nome, tabela in tabelas.items():
        conn.register(nome, tabela)
    sql = MART_QUERIES[query_nome].format(prefix="")
    return conn.sql(sql).to_arrow_table().to_pylist()


@pytest.fixture()
def fact_indicador_uf() -> pa.Table:
    """2 UFs × 2 anos × 2 redes — o AVG agrega série/rede antes da window."""
    return pa.table({
        "ano": [2022, 2022, 2023, 2023, 2022, 2022, 2023, 2023],
        "sigla_uf": ["SP", "SP", "SP", "SP", "RJ", "RJ", "RJ", "RJ"],
        "serie": ["1", "1", "1", "1", "1", "1", "1", "1"],
        "rede": ["1", "2", "1", "2", "1", "2", "1", "2"],
        "taxa_alfabetizacao": [80.0, 90.0, 84.0, 96.0, 60.0, 70.0, 66.0, 82.0],
        "media_portugues": [600.0, 620.0, 610.0, 630.0, 550.0, 570.0, 560.0, 590.0],
    })


class TestMartEvolucaoIndicadorUf:
    def test_delta_null_no_primeiro_ano_e_diff_nos_demais(self, fact_indicador_uf):
        rows = _run("mart_evolucao_indicador_uf", {"fact_indicador_uf": fact_indicador_uf})
        por_uf_ano = {(r["sigla_uf"], r["ano"]): r for r in rows}

        # SP: média(80,90)=85 em 2022 -> média(84,96)=90 em 2023
        assert por_uf_ano[("SP", 2022)]["delta_pp_vs_ano_anterior"] is None
        assert por_uf_ano[("SP", 2023)]["delta_pp_vs_ano_anterior"] == pytest.approx(5.0)
        # RJ: média(60,70)=65 -> média(66,82)=74
        assert por_uf_ano[("RJ", 2022)]["delta_pp_vs_ano_anterior"] is None
        assert por_uf_ano[("RJ", 2023)]["delta_pp_vs_ano_anterior"] == pytest.approx(9.0)

    def test_agrega_serie_e_rede_no_grao_ano_uf(self, fact_indicador_uf):
        rows = _run("mart_evolucao_indicador_uf", {"fact_indicador_uf": fact_indicador_uf})
        assert len(rows) == 4  # 2 UFs × 2 anos — uma linha por (ano, uf)
        sp2022 = next(r for r in rows if r["sigla_uf"] == "SP" and r["ano"] == 2022)
        assert sp2022["taxa_media_alfabetizacao"] == pytest.approx(85.0)
        assert sp2022["media_portugues_media"] == pytest.approx(610.0)


@pytest.fixture()
def fact_meta_resultado_uf() -> pa.Table:
    """1 UF × 1 ano × 4 linhas (redes/versões) — 3 atingiram a meta, 1 não."""
    return pa.table({
        "ano": [2024, 2024, 2024, 2024],
        "sigla_uf": ["SP", "SP", "SP", "SP"],
        "rede": ["1", "2", "3", "4"],
        "taxa_alfabetizacao": [80.0, 90.0, 70.0, 60.0],
        "meta_indicador": [75.0, 85.0, 75.0, 80.0],
        "gap_pontos": [5.0, 5.0, -5.0, -20.0],
        "atingiu_meta": [True, True, True, False],
        "valid_from": [2024, 2024, 2024, 2024],
        "valid_to": pa.array([None, None, None, None], type=pa.int64()),
        "is_current": [True, True, True, True],
    })


class TestMartAderenciaMetasUf:
    def test_pct_cumprimento_eh_fracao_atingiu_meta(self, fact_meta_resultado_uf):
        rows = _run("mart_aderencia_metas_uf", {"fact_meta_resultado_uf": fact_meta_resultado_uf})
        assert len(rows) == 4  # grão preservado por versão SCD2
        for r in rows:
            assert r["pct_cumprimento_ano_uf"] == pytest.approx(0.75)  # 3 de 4
            assert 0.0 <= r["pct_cumprimento_ano_uf"] <= 1.0

    def test_meta_do_ano_alias_de_meta_indicador(self, fact_meta_resultado_uf):
        rows = _run("mart_aderencia_metas_uf", {"fact_meta_resultado_uf": fact_meta_resultado_uf})
        for r in rows:
            assert r["meta_do_ano"] == r["meta_indicador"] if "meta_indicador" in r else True
        # coluna renomeada existe com o valor esperado
        sp_rede1 = next(r for r in rows if r["rede"] == "1")
        assert sp_rede1["meta_do_ano"] == pytest.approx(75.0)


@pytest.fixture()
def tabelas_ranking() -> dict[str, pa.Table]:
    """Fato e dim ligados pela surrogate key — o JOIN da mart é físico (sk),
    e a chave natural `id_municipio` sai da dimensão."""
    ids = ["3550308", "3304557", "3509502", "3550309"]
    fact = pa.table({
        "ano": [2023, 2023, 2023, 2023],
        "sk_municipio": [surrogate_key("municipio", i) for i in ids],
        "serie": ["1", "1", "1", "1"],
        "rede": ["1", "1", "1", "1"],
        "taxa_alfabetizacao": [90.0, 85.0, 90.0, 70.0],
        "media_portugues": [600.0, 590.0, 610.0, 500.0],
    })
    dim = pa.table({
        "sk_municipio": [surrogate_key("municipio", i) for i in ids],
        "id_municipio": ids,
        "nome": ["São Paulo", "Rio de Janeiro", "Campinas", "Mauá"],
        "sigla_uf": ["SP", "RJ", "SP", "SP"],
        "nome_regiao": ["Sudeste", "Sudeste", "Sudeste", "Sudeste"],
        "capital_uf": [1, 1, 0, 0],
    })
    return {"fact_indicador_municipio": fact, "dim_municipio": dim}


class TestMartRankingIndicadorMunicipio:
    def test_rank_1_tem_maior_taxa_da_uf(self, tabelas_ranking):
        rows = _run("mart_ranking_indicador_municipio", tabelas_ranking)
        sp = [r for r in rows if r["sigla_uf"] == "SP"]
        rank1 = [r for r in sp if r["rank_uf"] == 1]
        # Empate: 3550308 e 3509502 com 90.0 dividem a posição 1
        assert {r["id_municipio"] for r in rank1} == {"3550308", "3509502"}
        # Posição seguinte é pulada (RANK, não DENSE_RANK): 70.0 fica em 3º
        maua = next(r for r in sp if r["id_municipio"] == "3550309")
        assert maua["rank_uf"] == 3

    def test_enriquece_com_dim_municipio(self, tabelas_ranking):
        rows = _run("mart_ranking_indicador_municipio", tabelas_ranking)
        sao_paulo = next(r for r in rows if r["id_municipio"] == "3550308")
        assert sao_paulo["nome_municipio"] == "São Paulo"
        assert sao_paulo["nome_regiao"] == "Sudeste"

    def test_municipio_fora_da_dim_fica_de_fora(self, tabelas_ranking):
        """INNER JOIN: município sem entrada em dim_municipio não aparece na mart."""
        tabelas = dict(tabelas_ranking)
        fato = tabelas["fact_indicador_municipio"]
        extra = pa.table({
            "ano": [2023], "sk_municipio": [surrogate_key("municipio", "9999999")],
            "serie": ["1"], "rede": ["1"],
            "taxa_alfabetizacao": [99.0], "media_portugues": [700.0],
        })
        tabelas["fact_indicador_municipio"] = pa.concat_tables([fato, extra])
        rows = _run("mart_ranking_indicador_municipio", tabelas)
        assert "9999999" not in {r["id_municipio"] for r in rows}


class TestRenderECreateView:
    def test_render_qualifica_com_backticks(self):
        """project_id tem hífens — referência sem backtick seria erro de sintaxe no BigQuery."""
        settings = get_settings()
        ddl = render_view_ddl("mart_evolucao_indicador_uf")
        assert ddl.startswith(
            f"CREATE OR REPLACE VIEW `{settings.project_id}.{settings.dataset_id}.mart_evolucao_indicador_uf` AS"
        )
        assert f"`{settings.project_id}.{settings.dataset_id}`.fact_indicador_uf" in ddl

    def test_render_aceita_projeto_e_dataset_explicitos(self):
        ddl = render_view_ddl("mart_evolucao_indicador_uf", project_id="p", dataset_id="d")
        assert ddl.startswith("CREATE OR REPLACE VIEW `p.d.mart_evolucao_indicador_uf` AS")
        assert "`p.d`.fact_indicador_uf" in ddl

    def test_create_view_chama_run_ddl_com_o_ddl_renderizado(self):
        with patch("gold.marts.bigquery.Client"), \
             patch("gold.marts.gold_schema.run_ddl") as mock_ddl:
            create_view("mart_aderencia_metas_uf")
        mock_ddl.assert_called_once()
        ddl = mock_ddl.call_args.args[1]
        assert "CREATE OR REPLACE VIEW" in ddl
        assert "fact_meta_resultado_uf" in ddl
