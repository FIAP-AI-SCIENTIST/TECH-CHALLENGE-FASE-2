"""Testes do módulo silver.pipeline — orquestração completa por entidade."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from silver.pipeline import run_all_silver, run_silver


def _mock_log_execution():
    mock_run = MagicMock()
    mock_log = MagicMock()
    mock_log.return_value.__enter__ = lambda self: mock_run
    mock_log.return_value.__exit__ = lambda self, *a: None
    return mock_log, mock_run


def _mock_lock():
    mock_lock = MagicMock()
    mock_lock.return_value.__enter__ = MagicMock(return_value=None)
    mock_lock.return_value.__exit__ = MagicMock(return_value=False)
    return mock_lock


class TestRunSilverRegularEntity:
    """Fluxo completo mockado ponta a ponta — entidade regular, múltiplos anos."""

    def test_writes_one_partition_per_year(self):
        bruta = pa.table({
            "ano": [2023, 2024],
            "sigla_uf": ["SP", "SP"],
            "serie": ["2", "2"],
            "rede": ["0", "0"],
        })

        mock_log, mock_run = _mock_log_execution()

        with patch("silver.pipeline.gcs_lock", new=_mock_lock()), \
             patch("silver.pipeline.log_execution", mock_log), \
             patch("silver.pipeline.bronze_reader.read_partition", return_value=bruta), \
             patch("silver.pipeline.reference.get_dicionario", return_value=({}, 0)), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value=({}, 0)), \
             patch("quality.pipeline.run_entity_quality_checks", return_value=[]) as mock_quality, \
             patch("silver.pipeline.silver_writer.clear_entity") as mock_clear, \
             patch("silver.pipeline.silver_writer.write_entity", return_value=1) as mock_write:
            run_silver("uf")

        # Hook Data Quality: quality roda sobre o frame deduplicado, uma vez por execução.
        mock_quality.assert_called_once()
        assert mock_quality.call_args.args[0] == "uf"

        assert mock_clear.call_count == 2

        assert mock_write.call_count == 2
        chaves_escritas = {c.args[1] for c in mock_write.call_args_list}
        assert chaves_escritas == {"ano=2023", "ano=2024"}
        assert mock_run.rows_read == 2
        assert mock_run.rows_written == 2


class TestRunSilverQualityHook:
    """Integração Data Quality: run_silver propaga falha CRITICA de Data Quality para a
    auditoria (SUCCESS_WITH_DQ_FAILURE) sem desfazer a escrita."""

    def test_critical_dq_failure_marks_run_status(self):
        from quality.translate import QualityResult

        bruta = pa.table({
            "ano": [2024], "sigla_uf": ["SP"], "serie": ["2"], "rede": ["0"],
        })
        falha_critica = QualityResult(
            check_id="uf.duplicidade", check="duplicidade", entidade="uf",
            dimensao="Unicidade", passou=False, valor_medido=0.5, limiar=1.0,
            severidade="CRITICA", linhas_afetadas=1,
        )
        mock_log, mock_run = _mock_log_execution()

        with patch("silver.pipeline.gcs_lock", new=_mock_lock()), \
             patch("silver.pipeline.log_execution", mock_log), \
             patch("silver.pipeline.bronze_reader.read_partition", return_value=bruta), \
             patch("silver.pipeline.reference.get_dicionario", return_value=({}, 0)), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value=({}, 0)), \
             patch("quality.pipeline.run_entity_quality_checks", return_value=[falha_critica]), \
             patch("silver.pipeline.silver_writer.clear_entity"), \
             patch("silver.pipeline.silver_writer.write_entity", return_value=1) as mock_write:
            run_silver("uf")

        assert mock_run.status == "SUCCESS_WITH_DQ_FAILURE"
        # A escrita aconteceu normalmente — DQ não desfaz partição.
        mock_write.assert_called_once()

    def test_warning_only_dq_keeps_success_status(self):
        from quality.translate import QualityResult

        bruta = pa.table({
            "ano": [2024], "sigla_uf": ["SP"], "serie": ["2"], "rede": ["0"],
        })
        aviso = QualityResult(
            check_id="uf.frescor_dado", check="frescor_dado", entidade="uf",
            dimensao="Atualidade", passou=False, valor_medido=0.0, limiar=1.0,
            severidade="AVISO", linhas_afetadas=0,
        )
        mock_log, mock_run = _mock_log_execution()

        with patch("silver.pipeline.gcs_lock", new=_mock_lock()), \
             patch("silver.pipeline.log_execution", mock_log), \
             patch("silver.pipeline.bronze_reader.read_partition", return_value=bruta), \
             patch("silver.pipeline.reference.get_dicionario", return_value=({}, 0)), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value=({}, 0)), \
             patch("quality.pipeline.run_entity_quality_checks", return_value=[aviso]), \
             patch("silver.pipeline.silver_writer.clear_entity"), \
             patch("silver.pipeline.silver_writer.write_entity", return_value=1):
            run_silver("uf")

        assert mock_run.status != "SUCCESS_WITH_DQ_FAILURE"


class TestRunSilverScd2Entity:
    """Fluxo completo mockado — entidade SCD2."""

    _BRUTA = pa.table({
        "ano": [2023, 2024],
        "sigla_uf": ["SP", "SP"],
        "rede": ["0", "0"],
        "meta_alfabetizacao_2024": [50.0, 99.0],
        "meta_alfabetizacao_2025": [55.0, 55.0],
        "meta_alfabetizacao_2026": [60.0, 60.0],
        "meta_alfabetizacao_2027": [65.0, 65.0],
        "meta_alfabetizacao_2028": [70.0, 70.0],
        "meta_alfabetizacao_2029": [80.0, 80.0],
        "meta_alfabetizacao_2030": [100.0, 100.0],
        "percentual_participacao": [90.0, 90.0],
    })

    def test_applies_scd2_chronologically(self):
        mock_log, mock_run = _mock_log_execution()
        with patch("silver.pipeline.gcs_lock", new=_mock_lock()), \
             patch("silver.pipeline.log_execution", mock_log), \
             patch("silver.pipeline.bronze_reader.read_partition", return_value=self._BRUTA), \
             patch("silver.pipeline.reference.get_dicionario", return_value=({}, 0)), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value=({}, 0)), \
             patch("quality.pipeline.run_entity_quality_checks", return_value=[]), \
             patch("silver.pipeline.silver_writer.write_scd2_table") as mock_write:
            run_silver("meta_alfabetizacao_uf")

        mock_write.assert_called_once()
        tabela_final = mock_write.call_args.args[1]
        # SP mudou de 50 -> 99 entre 2023 e 2024: 2 versoes (1 fechada, 1 vigente)
        assert tabela_final.num_rows == 2
        currents = [r for r in tabela_final.to_pylist() if r["is_current"]]
        assert len(currents) == 1
        assert currents[0]["meta_alfabetizacao_2024"] == 99.0

    def test_persisted_scd2_state_does_not_leak_into_the_output(self):
        """A tabela SCD2 é reconstruída do Bronze a cada execução.
        Um estado persistido divergente (resíduo de um run anterior) não pode
        influenciar a saída — era exatamente por aí que a cadeia de versões
        duplicava a cada `make silver`, com versões fechadas antes de abrir.

        O patch é em `silver.reader`, não em `silver.pipeline`: o pipeline não
        importa mais o reader, então o resíduo simplesmente não tem por onde
        entrar. Se alguém reintroduzir a leitura de estado, este teste quebra.
        """
        residuo = pa.Table.from_pylist([{
            "sigla_uf": "SP",
            "rede": "0",
            "meta_alfabetizacao_2024": 1.0,
            "meta_alfabetizacao_2025": 1.0,
            "meta_alfabetizacao_2026": 1.0,
            "meta_alfabetizacao_2027": 1.0,
            "meta_alfabetizacao_2028": 1.0,
            "meta_alfabetizacao_2029": 1.0,
            "meta_alfabetizacao_2030": 1.0,
            "percentual_participacao": 1.0,
            "valid_from": 1999,
            "valid_to": None,
            "is_current": True,
        }])

        mock_log, mock_run = _mock_log_execution()
        with patch("silver.pipeline.gcs_lock", new=_mock_lock()), \
             patch("silver.pipeline.log_execution", mock_log), \
             patch("silver.pipeline.bronze_reader.read_partition", return_value=self._BRUTA), \
             patch("silver.pipeline.reference.get_dicionario", return_value=({}, 0)), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value=({}, 0)), \
             patch("quality.pipeline.run_entity_quality_checks", return_value=[]), \
             patch("silver.reader.read_scd2_table", return_value=residuo), \
             patch("silver.pipeline.silver_writer.write_scd2_table") as mock_write:
            run_silver("meta_alfabetizacao_uf")

        tabela_final = mock_write.call_args.args[1]
        assert tabela_final.num_rows == 2
        assert 1999 not in {r["valid_from"] for r in tabela_final.to_pylist()}


class TestRunAllSilver:
    """Verifica isolamento de falha entre entidades."""

    def test_one_entity_failing_does_not_stop_others(self):
        call_order = []

        def fake_run_silver(entidade):
            call_order.append(entidade)
            if entidade == "municipio":
                raise ValueError("falha simulada")

        with patch("silver.pipeline.run_silver", side_effect=fake_run_silver), \
             patch("silver.pipeline.run_integracao") as mock_integracao:
            with pytest.raises(RuntimeError):
                run_all_silver()

        # Todas as 6 entidades foram tentadas, mesmo com a falha no meio.
        assert call_order == [
            "uf",
            "municipio",
            "meta_alfabetizacao_brasil",
            "meta_alfabetizacao_uf",
            "meta_alfabetizacao_municipio",
            "alunos",
        ]
        # Com entidade falhada, a integrada não é reconstruída — insumo
        # parcial não gera tabela derivada parcial.
        mock_integracao.assert_not_called()

    def test_all_succeed_does_not_raise(self):
        with patch("silver.pipeline.run_silver"), \
             patch("silver.pipeline.run_integracao") as mock_integracao:
            run_all_silver()  # nao levanta
        mock_integracao.assert_called_once()

    def test_integracao_failing_marks_run_failed(self):
        with patch("silver.pipeline.run_silver"), \
             patch("silver.pipeline.run_integracao", side_effect=ValueError("falha simulada")):
            with pytest.raises(RuntimeError):
                run_all_silver()


class TestRunIntegracao:
    """Fluxo da tabela integrada: lê o estado Silver das duas entidades de
    insumo, escreve uma partição por ano e roda o hook de qualidade."""

    def _municipio_silver(self) -> pa.Table:
        return pa.table({
            "ano": [2023, 2024],
            "id_municipio": ["3550308", "3550308"],
            "rede": ["1", "1"],
            "serie": ["2", "2"],
            "rede_desc": ["Pública", "Pública"],
            "serie_desc": ["2º ano", "2º ano"],
            "nome": ["São Paulo", "São Paulo"],
            "sigla_uf": ["SP", "SP"],
            "nome_regiao": ["Sudeste", "Sudeste"],
            "capital_uf": [1, 1],
            "taxa_alfabetizacao": [70.0, 80.0],
            "media_portugues": [650.0, 700.0],
        })

    def _meta_scd2(self) -> pa.Table:
        return pa.table({
            "ano": [2023],
            "id_municipio": ["3550308"],
            "rede": ["1"],
            "meta_alfabetizacao_2024": [75.0],
            "percentual_participacao": [95.0],
            "nivel_alfabetizacao": pa.array([2], type=pa.int64()),
            "valid_from": pa.array([2023], type=pa.int64()),
            "valid_to": pa.array([None], type=pa.int64()),
            "is_current": [True],
        })

    def _run(self, municipio, meta):
        from silver.pipeline import run_integracao

        mock_log, mock_run = _mock_log_execution()
        with patch("silver.pipeline.gcs_lock", new=_mock_lock()), \
             patch("silver.pipeline.log_execution", mock_log), \
             patch("silver.pipeline.silver_reader.read_entity", return_value=municipio), \
             patch("silver.pipeline.silver_reader.read_scd2_table_raw", return_value=meta), \
             patch("quality.pipeline.run_entity_quality_checks", return_value=[]) as mock_quality, \
             patch("silver.pipeline.silver_writer.clear_entity") as mock_clear, \
             patch("silver.pipeline.silver_writer.write_entity", side_effect=lambda *a: 1) as mock_write:
            run_integracao()
        return mock_run, mock_quality, mock_clear, mock_write

    def test_writes_one_partition_per_year(self):
        mock_run, mock_quality, mock_clear, mock_write = self._run(self._municipio_silver(), self._meta_scd2())

        assert {c.args[1] for c in mock_write.call_args_list} == {"ano=2023", "ano=2024"}
        assert mock_clear.call_count == 2
        assert mock_run.rows_read == 2
        assert mock_run.rows_written == 2
        # Hook de qualidade roda sobre a tabela integrada, uma vez por execução.
        mock_quality.assert_called_once()
        assert mock_quality.call_args.args[0] == "alfabetizacao_municipio_integrado"

    def test_meta_do_ano_vem_da_versao_vigente(self):
        _, _, _, mock_write = self._run(self._municipio_silver(), self._meta_scd2())
        escrita_2024 = next(c.args[2] for c in mock_write.call_args_list if c.args[1] == "ano=2024")
        row = escrita_2024.to_pylist()[0]
        assert row["meta_indicador"] == 75.0
        assert row["percentual_participacao"] == 95.0
        # 2023 está fora do horizonte de colunas de meta — meta_indicador NULL.
        escrita_2023 = next(c.args[2] for c in mock_write.call_args_list if c.args[1] == "ano=2023")
        assert escrita_2023.to_pylist()[0]["meta_indicador"] is None

    def test_without_meta_table_meta_columns_are_null(self):
        """Sem a Silver de meta processada, o JOIN é contra relação vazia —
        o indicador sobrevive com as colunas de meta NULL."""
        _, _, _, mock_write = self._run(self._municipio_silver(), None)
        escrita_2024 = next(c.args[2] for c in mock_write.call_args_list if c.args[1] == "ano=2024")
        row = escrita_2024.to_pylist()[0]
        assert row["meta_indicador"] is None
        assert row["nivel_alfabetizacao"] is None

    def test_empty_municipio_writes_nothing(self):
        mock_run, mock_quality, mock_clear, mock_write = self._run(pa.Table.from_pydict({}), self._meta_scd2())
        mock_write.assert_not_called()
        mock_clear.assert_not_called()
        mock_quality.assert_not_called()
        assert mock_run.rows_written == 0

    def test_critical_dq_failure_marks_run_status(self):
        from quality.translate import QualityResult

        falha_critica = QualityResult(
            check_id="alfabetizacao_municipio_integrado.duplicidade", check="duplicidade",
            entidade="alfabetizacao_municipio_integrado", dimensao="Unicidade",
            passou=False, valor_medido=0.5, limiar=1.0, severidade="CRITICA", linhas_afetadas=1,
        )
        from silver.pipeline import run_integracao

        mock_log, mock_run = _mock_log_execution()
        with patch("silver.pipeline.gcs_lock", new=_mock_lock()), \
             patch("silver.pipeline.log_execution", mock_log), \
             patch("silver.pipeline.silver_reader.read_entity", return_value=self._municipio_silver()), \
             patch("silver.pipeline.silver_reader.read_scd2_table_raw", return_value=self._meta_scd2()), \
             patch("quality.pipeline.run_entity_quality_checks", return_value=[falha_critica]), \
             patch("silver.pipeline.silver_writer.clear_entity"), \
             patch("silver.pipeline.silver_writer.write_entity", side_effect=lambda *a: 1):
            run_integracao()

        assert mock_run.status == "SUCCESS_WITH_DQ_FAILURE"
