"""Testes do módulo streaming.producer — geração e publicação de eventos sintéticos."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from streaming.producer import (
    EVENT_TYPE_MODELS,
    _do_publish,
    gerar_evento_sintetico,
    produce_events,
)


class TestGerarEventoSintetico:
    """Verifica geração de eventos sintéticos (função pura, PBT-01)."""

    def test_medicao_returns_valid_instance(self):
        for _ in range(20):
            instancia, entidade = gerar_evento_sintetico("medicao")
            assert isinstance(instancia, BaseModel)
            assert entidade == "alunos"
            # Round-trip: instancia sempre serializa/desserializa sem erro
            assert type(instancia)(**instancia.model_dump()) == instancia

    def test_meta_returns_valid_instance(self):
        for _ in range(20):
            instancia, entidade = gerar_evento_sintetico("meta")
            assert isinstance(instancia, BaseModel)
            assert entidade in ("meta_alfabetizacao_uf", "meta_alfabetizacao_municipio")

    def test_indicador_returns_valid_instance(self):
        for _ in range(20):
            instancia, entidade = gerar_evento_sintetico("indicador")
            assert isinstance(instancia, BaseModel)
            assert entidade in ("uf", "municipio")

    def test_proficiencia_within_plausible_range(self):
        for _ in range(30):
            instancia, entidade = gerar_evento_sintetico("medicao")
            assert 0.0 <= instancia.proficiencia <= 1000.0

    def test_taxa_within_plausible_range(self):
        for _ in range(30):
            instancia, entidade = gerar_evento_sintetico("indicador")
            assert 0.0 <= instancia.taxa_alfabetizacao <= 100.0

    def test_unknown_tipo_evento_raises(self):
        with pytest.raises(ValueError):
            gerar_evento_sintetico("desconhecido")

    def test_all_declared_types_are_generatable(self):
        for tipo_evento in EVENT_TYPE_MODELS:
            instancia, entidade = gerar_evento_sintetico(tipo_evento)
            assert instancia is not None
            assert entidade


class TestDoPublish:
    """Verifica retry/timeout na publicação real."""

    def test_retries_on_transient_failure_then_succeeds(self):
        mock_client = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "message-id-123"
        mock_client.publish.side_effect = [ConnectionError("transient"), mock_future]

        with patch("streaming.producer.time"):
            result = _do_publish(mock_client, "topic-path", b"{}", {"tipo_evento": "meta"})

        assert result == "message-id-123"
        assert mock_client.publish.call_count == 2

    def test_passes_explicit_timeout(self):
        mock_client = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "id"

        _do_publish(mock_client, "topic-path", b"{}", {"tipo_evento": "meta"})

        _, kwargs = mock_client.publish.call_args
        assert kwargs.get("timeout") == 10


class TestProduceEvents:
    """Verifica orquestração single-shot do Producer."""

    def test_publishes_n_events_and_sets_rows_written(self):
        with patch("streaming.producer.pubsub_v1.PublisherClient") as mock_client_cls:
            mock_client_cls.return_value.topic_path.return_value = "projects/x/topics/y"
            mock_future = MagicMock()
            mock_future.result.return_value = "id"
            mock_client_cls.return_value.publish.return_value = mock_future

            with patch("streaming.producer.log_execution") as mock_log:
                mock_run = MagicMock()
                mock_log.return_value.__enter__ = lambda self: mock_run
                mock_log.return_value.__exit__ = lambda self, *a: None

                produce_events("meta", n=5)

        assert mock_client_cls.return_value.publish.call_count == 5
        assert mock_run.rows_written == 5

    def test_publish_includes_routing_attributes(self):
        with patch("streaming.producer.pubsub_v1.PublisherClient") as mock_client_cls:
            mock_client_cls.return_value.topic_path.return_value = "projects/x/topics/y"
            mock_future = MagicMock()
            mock_future.result.return_value = "id"
            mock_client_cls.return_value.publish.return_value = mock_future

            with patch("streaming.producer.log_execution") as mock_log:
                mock_run = MagicMock()
                mock_log.return_value.__enter__ = lambda self: mock_run
                mock_log.return_value.__exit__ = lambda self, *a: None

                produce_events("indicador", n=1)

        _, kwargs = mock_client_cls.return_value.publish.call_args
        assert "tipo_evento" in kwargs
        assert "entidade" in kwargs
