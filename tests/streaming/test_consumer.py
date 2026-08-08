"""Testes do módulo streaming.consumer — consumo de eventos e escrita em micro-batch na Bronze."""

import json
from unittest.mock import MagicMock, patch

import pytest

from streaming.consumer import _do_ack, _do_pull, consume_batch


def _make_message(entidade: str, payload: dict, message_id: str = "m1", ack_id: str = "a1"):
    msg = MagicMock()
    msg.ack_id = ack_id
    msg.message.message_id = message_id
    msg.message.attributes = {"tipo_evento": "indicador", "entidade": entidade}
    msg.message.data = json.dumps(payload).encode("utf-8")
    return msg


class TestDoPull:
    def test_retries_on_transient_failure_then_succeeds(self):
        mock_client = MagicMock()
        mock_response_ok = MagicMock()
        mock_response_ok.received_messages = ["msg1"]
        mock_client.pull.side_effect = [ConnectionError("transient"), mock_response_ok]

        with patch("streaming.consumer.time"):
            result = _do_pull(mock_client, "sub-path", 10)

        assert result == ["msg1"]
        assert mock_client.pull.call_count == 2

    def test_passes_explicit_timeout(self):
        mock_client = MagicMock()
        mock_client.pull.return_value.received_messages = []

        _do_pull(mock_client, "sub-path", 10)

        _, kwargs = mock_client.pull.call_args
        assert kwargs.get("timeout") == 10


class TestDoAck:
    def test_passes_explicit_timeout(self):
        mock_client = MagicMock()

        _do_ack(mock_client, "sub-path", ["a1"])

        _, kwargs = mock_client.acknowledge.call_args
        assert kwargs.get("timeout") == 10


class TestConsumeBatch:
    """Verifica orquestração do micro-batch: decodifica, escreve, acka, checa lag."""

    def _run_with_messages(self, messages, run_id: str = "run-1"):
        with patch("streaming.consumer.pubsub_v1.SubscriberClient") as mock_client_cls:
            mock_client_cls.return_value.subscription_path.return_value = "projects/x/subscriptions/y"
            with patch("streaming.consumer._do_pull", return_value=messages):
                with patch("streaming.consumer._do_ack") as mock_ack:
                    with patch("streaming.consumer.bronze_writer.clear_partition") as mock_clear:
                        with patch("streaming.consumer.bronze_writer.write_partition") as mock_write:
                            mock_write.return_value = 1
                            with patch("streaming.consumer.get_consumer_lag", return_value=0):
                                with patch("streaming.consumer.log_execution") as mock_log:
                                    mock_run = MagicMock()
                                    mock_run.run_id = run_id
                                    mock_log.return_value.__enter__ = lambda self: mock_run
                                    mock_log.return_value.__exit__ = lambda self, *a: None

                                    consume_batch(max_messages=10)

                                    return mock_ack, mock_clear, mock_write, mock_run

    def test_acks_only_after_successful_write(self):
        msg = _make_message("uf", {"ano": 2024, "sigla_uf": "SP"})
        mock_ack, mock_clear, mock_write, mock_run = self._run_with_messages([msg])

        mock_write.assert_called_once()
        mock_ack.assert_called_once()
        ack_ids = mock_ack.call_args.args[2]
        assert ack_ids == ["a1"]

    def test_does_not_ack_when_write_fails(self):
        msg = _make_message("uf", {"ano": 2024, "sigla_uf": "SP"})
        with patch("streaming.consumer.pubsub_v1.SubscriberClient") as mock_client_cls:
            mock_client_cls.return_value.subscription_path.return_value = "projects/x/subscriptions/y"
            with patch("streaming.consumer._do_pull", return_value=[msg]):
                with patch("streaming.consumer._do_ack") as mock_ack:
                    with patch("streaming.consumer.bronze_writer.clear_partition"):
                        with patch("streaming.consumer.bronze_writer.write_partition", side_effect=RuntimeError("boom")):
                            with patch("streaming.consumer.get_consumer_lag", return_value=0):
                                with patch("streaming.consumer.log_execution") as mock_log:
                                    mock_run = MagicMock()
                                    mock_log.return_value.__enter__ = lambda self: mock_run
                                    mock_log.return_value.__exit__ = lambda self, *a: None

                                    consume_batch(max_messages=10)

        mock_ack.assert_not_called()

    def test_isolates_bad_message_from_good_ones(self):
        """Regressão: uma mensagem malformada não impede o ack das demais."""
        good = _make_message("uf", {"ano": 2024, "sigla_uf": "SP"}, message_id="good", ack_id="ack-good")
        bad = _make_message("entidade_desconhecida", {"x": 1}, message_id="bad", ack_id="ack-bad")

        mock_ack, mock_clear, mock_write, mock_run = self._run_with_messages([good, bad])

        mock_ack.assert_called_once()
        ack_ids = mock_ack.call_args.args[2]
        assert ack_ids == ["ack-good"]
        assert "ack-bad" not in ack_ids

    def test_never_clears_the_daily_partition(self):
        """Regressão: a partição "data_ingestao=" é compartilhada por todos os
        micro-batches do dia — limpá-la apagaria o que runs anteriores gravaram."""
        msgs = [
            _make_message("uf", {"ano": 2024, "sigla_uf": "SP"}, message_id=f"m{i}", ack_id=f"a{i}")
            for i in range(3)
        ]
        _ack, mock_clear, _write, _run = self._run_with_messages(msgs)

        mock_clear.assert_not_called()

    def test_consecutive_runs_write_distinct_part_files(self):
        """Regressão: dois runs no mesmo dia nomeiam o arquivo pelo run_id,
        então o segundo não sobrescreve o Parquet do primeiro."""
        msg = _make_message("uf", {"ano": 2024, "sigla_uf": "SP"})

        _a1, _c1, write_1, _r1 = self._run_with_messages([msg], run_id="run-a")
        _a2, _c2, write_2, _r2 = self._run_with_messages([msg], run_id="run-b")

        assert write_1.call_args.kwargs["part_id"] == "run-a"
        assert write_2.call_args.kwargs["part_id"] == "run-b"
        assert write_1.call_args.args[1] == write_2.call_args.args[1]  # mesma partição do dia

    def test_sets_rows_read_and_written(self):
        msg = _make_message("uf", {"ano": 2024, "sigla_uf": "SP"})
        mock_ack, mock_clear, mock_write, mock_run = self._run_with_messages([msg])

        assert mock_run.rows_read == 1
        assert mock_run.rows_written == 1

    def test_warns_when_lag_above_threshold(self):
        msg = _make_message("uf", {"ano": 2024, "sigla_uf": "SP"})
        with patch("streaming.consumer.pubsub_v1.SubscriberClient") as mock_client_cls:
            mock_client_cls.return_value.subscription_path.return_value = "projects/x/subscriptions/y"
            with patch("streaming.consumer._do_pull", return_value=[msg]):
                with patch("streaming.consumer._do_ack"):
                    with patch("streaming.consumer.bronze_writer.clear_partition"):
                        with patch("streaming.consumer.bronze_writer.write_partition", return_value=1):
                            with patch("streaming.consumer.get_consumer_lag", return_value=101):
                                with patch("streaming.consumer.log_execution") as mock_log:
                                    mock_run = MagicMock()
                                    mock_log.return_value.__enter__ = lambda self: mock_run
                                    mock_log.return_value.__exit__ = lambda self, *a: None
                                    with patch("streaming.consumer.setup_logger") as mock_setup_logger:
                                        mock_logger = MagicMock()
                                        mock_setup_logger.return_value = mock_logger

                                        consume_batch(max_messages=10)

        mock_logger.warning.assert_called_once()

    def test_no_warning_when_lag_below_threshold(self):
        msg = _make_message("uf", {"ano": 2024, "sigla_uf": "SP"})
        with patch("streaming.consumer.pubsub_v1.SubscriberClient") as mock_client_cls:
            mock_client_cls.return_value.subscription_path.return_value = "projects/x/subscriptions/y"
            with patch("streaming.consumer._do_pull", return_value=[msg]):
                with patch("streaming.consumer._do_ack"):
                    with patch("streaming.consumer.bronze_writer.clear_partition"):
                        with patch("streaming.consumer.bronze_writer.write_partition", return_value=1):
                            with patch("streaming.consumer.get_consumer_lag", return_value=5):
                                with patch("streaming.consumer.log_execution") as mock_log:
                                    mock_run = MagicMock()
                                    mock_log.return_value.__enter__ = lambda self: mock_run
                                    mock_log.return_value.__exit__ = lambda self, *a: None
                                    with patch("streaming.consumer.setup_logger") as mock_setup_logger:
                                        mock_logger = MagicMock()
                                        mock_setup_logger.return_value = mock_logger

                                        consume_batch(max_messages=10)

        mock_logger.warning.assert_not_called()
