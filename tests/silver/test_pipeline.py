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
             patch("silver.pipeline.reference.get_dicionario", return_value={}), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value={}), \
             patch("silver.pipeline.silver_writer.clear_entity") as mock_clear, \
             patch("silver.pipeline.silver_writer.write_entity", return_value=1) as mock_write:
            run_silver("uf")

        assert mock_clear.call_count == 2
        assert mock_write.call_count == 2
        chaves_escritas = {c.args[1] for c in mock_write.call_args_list}
        assert chaves_escritas == {"ano=2023", "ano=2024"}
        assert mock_run.rows_read == 2
        assert mock_run.rows_written == 2


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
             patch("silver.pipeline.reference.get_dicionario", return_value={}), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value={}), \
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
        """Regra 11: a tabela SCD2 é reconstruída do Bronze a cada execução.
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
             patch("silver.pipeline.reference.get_dicionario", return_value={}), \
             patch("silver.pipeline.reference.get_diretorio_uf", return_value={}), \
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

        with patch("silver.pipeline.run_silver", side_effect=fake_run_silver):
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

    def test_all_succeed_does_not_raise(self):
        with patch("silver.pipeline.run_silver"):
            run_all_silver()  # nao levanta
