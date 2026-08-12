"""Testes do módulo quality.pipeline — orquestração dos checks registrados."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from quality.pipeline import run_all_quality_checks, run_quality_checks


class TestRunQualityChecks:
    def test_runs_registered_checks_for_entity(self):
        tabela = pa.table({
            "ano": [2023, 2023],
            "sigla_uf": ["SP", "SP"],
            "serie": ["2", "2"],
            "rede": ["0", "0"],
            "taxa_alfabetizacao": [50.0, 50.0],
        })

        with patch("quality.pipeline.write_quality_results") as mock_write:
            resultados = run_quality_checks("uf", tabela)

        checks_rodados = {r.check for r in resultados}
        assert "duplicidade" in checks_rodados  # chave (ano,sigla_uf,serie,rede) duplicada
        assert "valores_ausentes" in checks_rodados
        assert "consistencia_faixa" in checks_rodados
        mock_write.assert_called_once_with(resultados)

    def test_unknown_entity_runs_no_checks(self):
        tabela = pa.table({"x": [1]})
        with patch("quality.pipeline.write_quality_results") as mock_write:
            resultados = run_quality_checks("entidade_desconhecida", tabela)

        assert resultados == []
        mock_write.assert_called_once_with([])


class TestRunAllQualityChecks:
    def test_reads_regular_and_scd2_entities(self):
        tabela = pa.table({"ano": [2023]})

        with patch("quality.pipeline.run_quality_checks") as mock_checks:
            with patch("silver.reader.read_entity", return_value=tabela) as mock_read_entity, \
                 patch("silver.reader.read_scd2_table_raw", return_value=tabela) as mock_read_scd2:
                run_all_quality_checks()

        assert mock_read_entity.call_count == 3  # uf, municipio, alunos
        assert mock_read_scd2.call_count == 3  # as 3 entidades de meta
        assert mock_checks.call_count == 6

    def test_one_entity_failing_does_not_stop_others(self):
        def fake_run_quality_checks(entidade, tabela):
            if entidade == "municipio":
                raise ValueError("falha simulada")
            return []

        with patch("quality.pipeline.run_quality_checks", side_effect=fake_run_quality_checks), \
             patch("silver.reader.read_entity", return_value=pa.table({"ano": [2023]})), \
             patch("silver.reader.read_scd2_table_raw", return_value=None):
            with pytest.raises(RuntimeError):
                run_all_quality_checks()
