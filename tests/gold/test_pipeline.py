"""Testes do módulo gold.pipeline — orquestração completa da materialização."""

from unittest.mock import MagicMock, patch

import pyarrow as pa

from gold.pipeline import run_gold


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

        with patch("gold.pipeline.silver_reader.read_entity", side_effect=lambda e: {
                "uf": uf, "municipio": municipio, "alunos": alunos,
             }[e]), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             patch("gold.pipeline.write_table", return_value=1) as mock_write, \
             patch("gold.pipeline.write_quality_results"):
            run_gold()

        tabelas_escritas = {c.args[0] for c in mock_write.call_args_list}
        assert tabelas_escritas == {
            "dim_uf", "dim_municipio", "dim_rede", "dim_serie",
            "fact_indicador_uf", "fact_indicador_municipio", "fact_alunos",
        }

    def test_skips_facts_when_silver_not_processed_yet(self):
        # Dims sempre saem (fallback com schema vazio); fatos exigem colunas
        # da fonte, então são pulados quando a Silver ainda não rodou.
        empty = pa.Table.from_pydict({})

        with patch("gold.pipeline.silver_reader.read_entity", return_value=empty), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             patch("gold.pipeline.write_table") as mock_write:
            run_gold()

        tabelas_escritas = {c.args[0] for c in mock_write.call_args_list}
        assert tabelas_escritas == {"dim_uf", "dim_municipio", "dim_rede", "dim_serie"}

    def test_referential_check_runs_before_writing_fact(self):
        uf = pa.table({"ano": [2023], "sigla_uf": ["SP"], "sigla_uf_nome": ["São Paulo"], "taxa_alfabetizacao": [80.0]})
        empty = pa.Table.from_pydict({})

        with patch("gold.pipeline.silver_reader.read_entity", side_effect=lambda e: uf if e == "uf" else empty), \
             patch("gold.pipeline.silver_reader.read_scd2_table_raw", return_value=None), \
             patch("gold.pipeline.gcs_lock", new=_mock_lock()), \
             patch("gold.pipeline.log_execution", _mock_log_execution()), \
             patch("gold.pipeline.write_table", return_value=1), \
             patch("gold.pipeline.check_referential_integrity") as mock_check, \
             patch("gold.pipeline.write_quality_results"):
            run_gold()

        chamadas_fact_uf = [c for c in mock_check.call_args_list if c.args[0] == "fact_indicador_uf"]
        assert len(chamadas_fact_uf) == 1
        assert chamadas_fact_uf[0].args[2] == "sigla_uf"
