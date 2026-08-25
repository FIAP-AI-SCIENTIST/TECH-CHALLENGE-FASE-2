"""Testes de ml.pipeline.run_ml — orquestração treino -> avaliação -> predição."""

from unittest.mock import MagicMock, patch

from ml import model as ml_model
from ml.pipeline import _run_query, run_ml


class TestRunQuery:
    def test_applies_maximum_bytes_billed_cap(self):
        """Mesmo espírito do cap de custo da extração: JOIN acidentalmente
        caro falha a query em vez de gerar fatura (gap encontrado na revisão)."""
        mock_client = MagicMock()
        _run_query(mock_client, "SELECT 1", timeout=30)
        _, kwargs = mock_client.query.call_args
        assert kwargs["job_config"].maximum_bytes_billed == ml_model.MAX_BYTES_BILLED


class TestRunMl:
    def test_treina_avalia_e_materializa_predicao_nessa_ordem(self):
        eventos: list[str] = []

        def fake_run_query(client, sql, timeout):
            if "CREATE OR REPLACE MODEL" in sql:
                eventos.append("train")
            elif "ML.EVALUATE" in sql:
                eventos.append("evaluate")
            elif "CREATE OR REPLACE VIEW" in sql:
                eventos.append("predict")
            return iter([])

        with patch("ml.pipeline.bigquery.Client"), \
             patch("ml.pipeline._run_query", side_effect=fake_run_query):
            run_ml()

        assert eventos == ["train", "evaluate", "predict"]

    def test_loga_metricas_de_avaliacao_quando_ha_resultado(self):
        with patch("ml.pipeline.bigquery.Client"), \
             patch("ml.pipeline._run_query") as mock_query, \
             patch("ml.pipeline.setup_logger") as mock_logger:
            mock_query.side_effect = [None, iter([{"roc_auc": 0.8}]), None]
            mock_log_instance = MagicMock()
            mock_logger.return_value = mock_log_instance
            run_ml()

        mock_log_instance.info.assert_any_call(
            "ML.EVALUATE modelo_risco_nao_alfabetizacao: {'roc_auc': 0.8}"
        )

    def test_nao_falha_quando_evaluate_retorna_vazio(self):
        with patch("ml.pipeline.bigquery.Client"), \
             patch("ml.pipeline._run_query") as mock_query:
            mock_query.side_effect = [None, iter([]), None]
            run_ml()  # não deve levantar exceção
