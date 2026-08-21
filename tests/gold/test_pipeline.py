"""Testes do módulo gold.pipeline — orquestração completa da materialização."""

from unittest.mock import MagicMock, patch

import pyarrow as pa

from gold.pipeline import run_gold


def _mock_create_view():
    return patch("gold.pipeline.marts.create_view")


def _mock_log_execution():
    mock_run = MagicMock()
    mock_log = MagicMock()
    mock_log.return_value.__enter__ = lambda self: mock_run
    mock_log.return_value.__exit__ = lambda self, *a: None
    return mock_log


def _mock_lock():
    mock_lock = MagicMock()
    mock_lock.return_value.__enter__ = MagicMock(return_value=None)
    mock_lock.return_value.__exit__ = MagicMock(return_value=False)
    return mock_lock


class TestRunGold:
    def test_materializes_dims_and_facts(self):
        uf = pa.table({
            "ano": [2023], "sigla_uf": ["SP"], "serie": ["2"], "rede": ["0"],
            "sigla_uf_nome": ["São Paulo"], "taxa_alfabetizacao": [80.0],
        })
        municipio = pa.table({
            "ano": [2023], "id_municipio": ["3550308"], "serie": ["2"], "rede": ["0"],
            "nome": ["São Paulo"], "sigla_uf": ["SP"], "nome_regiao": ["Sudeste"],
            "capital_uf": [1], "taxa_alfabetizacao": [80.0],
        })
        alunos = pa.table({"ano": [2023], "id_municipio": ["3550308"], "id_aluno": ["a1"], "proficiencia": [650.0]})
        integrada = pa.table({
            "ano": [2024], "id_municipio": ["3550308"], "rede": ["1"], "serie": ["2"],
            "taxa_alfabetizacao": [80.0], "media_portugues": [700.0],
            "meta_indicador": [75.0], "percentual_participacao": [95.0],
            "nivel_alfabetizacao": pa.array([2], type=pa.int64()),
        })

        with patch("gold.pipeline.silver_reader.read_entity", side_effect=lambda e: {
                "uf": uf, "municipio": municipio, "alunos": alunos,
                "alfabetizacao_municipio_integrado": integrada,
             }[e]), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.get_diretorio_uf", return_value=(
                 {"SP": "São Paulo", "RJ": "Rio de Janeiro", "DF": "Distrito Federal"}, 1024)), \
             patch("gold.pipeline.get_diretorio_municipio", return_value=({
                 "3550308": {"nome": "São Paulo", "sigla_uf": "SP", "nome_regiao": "Sudeste", "capital_uf": 1},
                 "3304557": {"nome": "Rio de Janeiro", "sigla_uf": "RJ", "nome_regiao": "Sudeste", "capital_uf": 1},
             }, 2048)), \
             patch("gold.pipeline.get_atlas_idhm", return_value=({
                 "3550308": {"idhm": 0.805, "idhm_educacao": 0.75, "idhm_renda": 0.80, "idhm_longevidade": 0.87},
             }, 512)), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             _mock_create_view() as mock_create_view, \
             patch("gold.pipeline.write_table", return_value=1) as mock_write:
            run_gold()

        tabelas_escritas = {c.args[0] for c in mock_write.call_args_list}
        assert tabelas_escritas == {
            "dim_uf", "dim_municipio", "dim_rede", "dim_serie", "dim_tempo",
            "fact_indicador_uf", "fact_indicador_municipio", "fact_alunos",
            "fact_alfabetizacao_municipio",
        }
        # O fato integrado carrega meta e resultado da mesma linha, com FKs.
        fato_integrado = next(c.args[1] for c in mock_write.call_args_list if c.args[0] == "fact_alfabetizacao_municipio")
        row = fato_integrado.to_pylist()[0]
        assert row["meta_indicador"] == 75.0
        assert row["gap_pontos"] == 5.0
        assert row["sk_municipio"] is not None and row["sk_tempo"] is not None
        # dim_municipio carrega o IDHM fundido do Atlas do Desenvolvimento Humano.
        dim_municipio = next(c.args[1] for c in mock_write.call_args_list if c.args[0] == "dim_municipio")
        linha_sp = next(r for r in dim_municipio.to_pylist() if r["id_municipio"] == "3550308")
        assert linha_sp["idhm"] == 0.805
        assert linha_sp["idhm_educacao"] == 0.75
        # dim_tempo cobre os anos observados e o horizonte da meta.
        dim_tempo = next(c.args[1] for c in mock_write.call_args_list if c.args[0] == "dim_tempo")
        anos_dim = set(dim_tempo.column("ano").to_pylist())
        assert {2023, 2024} <= anos_dim and set(range(2024, 2031)) <= anos_dim
        # As 3 views analíticas são criadas após os fatos.
        views_criadas = [c.args[0] for c in mock_create_view.call_args_list]
        assert views_criadas == [
            "mart_evolucao_indicador_uf", "mart_aderencia_metas_uf", "mart_ranking_indicador_municipio",
        ]

    def test_skips_facts_when_silver_not_processed_yet(self):
        # Dims sempre saem (fallback com schema vazio); fatos exigem colunas
        # da fonte, então são pulados quando a Silver ainda não rodou.
        # dim_tempo é a exceção intencional: mesmo sem ano observado, o
        # horizonte da meta nacional (2024-2030) sempre existe na dimensão.
        empty = pa.Table.from_pydict({})

        with patch("gold.pipeline.get_diretorio_uf", return_value=({}, 0)), \
             patch("gold.pipeline.get_diretorio_municipio", return_value=({}, 0)), \
             patch("gold.pipeline.get_atlas_idhm", return_value=({}, 0)), \
             patch("gold.pipeline.silver_reader.read_entity", return_value=empty), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             _mock_create_view(), \
             patch("gold.pipeline.write_table", return_value=0) as mock_write:
            run_gold()

        tabelas_escritas = {c.args[0] for c in mock_write.call_args_list}
        assert tabelas_escritas == {"dim_uf", "dim_municipio", "dim_rede", "dim_serie", "dim_tempo"}

    def test_view_failure_is_isolated_and_marks_run_failed(self):
        """Uma view falhando não impede as demais, mas o
        run termina com RuntimeError — falha nunca é silenciosa."""
        import pytest

        empty = pa.Table.from_pydict({})

        def falha_na_segunda(nome):
            if nome == "mart_aderencia_metas_uf":
                raise ValueError("view quebrada simulada")

        with patch("gold.pipeline.get_diretorio_uf", return_value=({}, 0)), \
             patch("gold.pipeline.get_diretorio_municipio", return_value=({}, 0)), \
             patch("gold.pipeline.get_atlas_idhm", return_value=({}, 0)), \
             patch("gold.pipeline.silver_reader.read_entity", return_value=empty), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             _mock_create_view() as mock_create_view, \
             patch("gold.pipeline.write_table"):
            mock_create_view.side_effect = falha_na_segunda
            with pytest.raises(RuntimeError):
                run_gold()

        # As 3 views foram tentadas, mesmo com a do meio falhando.
        assert mock_create_view.call_count == 3

    def test_build_failure_is_isolated_and_marks_run_failed(self):
        """Uma tabela que falha ao ser construída não impede a materialização
        das demais.

        Regressão: enquanto a lista de tabelas era um literal com as chamadas de
        construção já resolvidas, todas eram avaliadas antes do try/except do
        laço de escrita — uma exceção em qualquer construtor abortava a Gold
        inteira, inclusive as dimensões que já tinham sido computadas com
        sucesso. A falha ainda tem que ser fatal no fim da execução, mas só
        depois de tentar todas as outras tabelas.
        """
        import pytest

        uf = pa.table({
            "ano": [2023], "sigla_uf": ["SP"], "rede": ["1"], "serie": ["2"],
            "rede_desc": ["Federal"], "serie_desc": ["2o ano"], "sigla_uf_nome": ["São Paulo"],
            "taxa_alfabetizacao": [70.0],
        })

        with patch("gold.pipeline.get_diretorio_uf", return_value=({"SP": "São Paulo"}, 1024)), \
             patch("gold.pipeline.get_diretorio_municipio", return_value=({}, 0)), \
             patch("gold.pipeline.get_atlas_idhm", return_value=({}, 0)), \
             patch("gold.pipeline.silver_reader.read_entity", return_value=uf), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             _mock_create_view(), \
             patch("gold.pipeline.transform.build_fact_indicador_uf",
                   side_effect=ValueError("construtor quebrado simulado")), \
             patch("gold.pipeline.write_table", return_value=1) as mock_write:
            with pytest.raises(RuntimeError):
                run_gold()

        tabelas_escritas = {c.args[0] for c in mock_write.call_args_list}
        # A tabela cujo construtor falhou não é escrita...
        assert "fact_indicador_uf" not in tabelas_escritas
        # ...e nenhuma das outras é perdida por causa dela, nem as dimensões
        # computadas antes dela na ordem de materialização.
        assert {"dim_uf", "dim_rede", "dim_serie", "dim_tempo"} <= tabelas_escritas
        assert "fact_indicador_municipio" in tabelas_escritas

    def test_reads_diretorio_through_real_reference_contract(self):
        """Regressão: `get_diretorio_*` devolve `(dict, bytes)`, não `dict`.

        Aqui só o I/O (`_do_query`) é mockado — as funções reais de
        `silver.reference` rodam, então qualquer divergência de contrato entre
        elas e a Gold quebra este teste (o mock de `get_diretorio_*` usado nos
        demais casos não pega esse tipo de erro).
        """
        empty = pa.Table.from_pydict({})
        uf_rows = [
            {"sigla": "SP", "nome": "São Paulo"},
            {"sigla": "DF", "nome": "Distrito Federal"},
            {"sigla": "RR", "nome": "Roraima"},
        ]
        # `capital_uf` é INT64 no diretório da Base dos Dados — fixture com string
        # aqui esconderia o ArrowTypeError que estourou no run real.
        municipio_rows = [
            {"id_municipio": "5219308", "nome": "Santa Helena de Goiás",
             "sigla_uf": "GO", "nome_regiao": "Centro-Oeste", "capital_uf": 0},
            {"id_municipio": "5208707", "nome": "Goiânia",
             "sigla_uf": "GO", "nome_regiao": "Centro-Oeste", "capital_uf": 1},
        ]

        atlas_rows = [
            {"id_municipio": "5208707", "idhm": 0.799, "idhm_e": 0.7, "idhm_l": 0.85, "idhm_r": 0.78},
        ]

        def do_query(_client, sql):
            tabela = sql.split("`")[1]
            if "mundo_onu_adh" in tabela:
                return atlas_rows, 512
            if tabela.rsplit(".", 1)[-1] == "uf":
                return uf_rows, 512
            return municipio_rows, 512

        with patch("silver.reference.bigquery.Client"), \
             patch("silver.reference._do_query", side_effect=do_query), \
             patch("gold.pipeline.silver_reader.read_entity", return_value=empty), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             _mock_create_view(), \
             patch("gold.pipeline.write_table", return_value=1) as mock_write:
            run_gold()

        escritas = {c.args[0]: c.args[1] for c in mock_write.call_args_list}
        # F1: DF e RR presentes na dim mesmo sem linha de resultado na Silver.
        assert set(escritas["dim_uf"].column("sigla_uf").to_pylist()) == {"SP", "DF", "RR"}
        # F2: município que a entidade `municipio` do INEP omite.
        assert escritas["dim_municipio"].column("id_municipio").to_pylist() == ["5208707", "5219308"]
        # Tipo preservado da fonte: forçar string quebra o load da dim.
        assert escritas["dim_municipio"].schema.field("capital_uf").type == pa.int64()
        # G1: IDHM fundido só para o município presente no Atlas mockado;
        # o outro município do diretório sai com IDHM None, não descartado.
        idhm_por_id = dict(zip(
            escritas["dim_municipio"].column("id_municipio").to_pylist(),
            escritas["dim_municipio"].column("idhm").to_pylist(),
        ))
        assert idhm_por_id["5208707"] == 0.799
        assert idhm_por_id["5219308"] is None

