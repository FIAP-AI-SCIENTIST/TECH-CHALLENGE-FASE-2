"""Testes do módulo quality.writer — gravação best-effort no BigQuery."""

from unittest.mock import MagicMock, patch

from quality.checks import QualityResult
from quality.writer import write_quality_results


class TestWriteQualityResults:
    def test_inserts_one_row_per_result(self):
        resultados = [
            QualityResult("duplicidade", "uf", True, 0, "sem duplicatas"),
            QualityResult("valores_ausentes", "uf", False, 2, "ano=2"),
        ]

        with patch("quality.writer.bigquery.Client") as mock_client_cls, \
             patch("quality.writer._do_insert") as mock_insert:
            mock_client_cls.return_value = MagicMock()
            write_quality_results(resultados)

        mock_insert.assert_called_once()
        payloads = mock_insert.call_args.args[2]
        assert len(payloads) == 2
        assert payloads[0]["check"] == "duplicidade"
        assert payloads[1]["passou"] is False

    def test_empty_results_does_not_call_bigquery(self):
        with patch("quality.writer.bigquery.Client") as mock_client_cls:
            write_quality_results([])
            mock_client_cls.assert_not_called()

    def test_failure_is_swallowed(self):
        resultados = [QualityResult("duplicidade", "uf", True, 0, "sem duplicatas")]

        with patch("quality.writer.bigquery.Client", side_effect=RuntimeError("boom")):
            write_quality_results(resultados)  # nao levanta
