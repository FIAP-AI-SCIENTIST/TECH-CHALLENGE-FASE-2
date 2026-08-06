"""Testes do módulo observability.monitoring — leitura Consumer Lag."""

from unittest.mock import MagicMock, patch

import pytest

from observability.monitoring import (
    MAX_ATTEMPTS,
    _read_undelivered_count,
    get_consumer_lag,
)


def _fake_time_series(int_value: int = 42):
    """Cria um objeto mock de TimeSeries com um ponto."""
    series = MagicMock()
    point = MagicMock()
    point.value.int64_value = int_value
    series.points = [point]
    return series


class TestReadUndeliveredCount:
    """Verifica leitura da métrica de undelivered messages com retry."""

    def test_returns_value_on_success(self):
        client = MagicMock()
        client.list_time_series.return_value = [_fake_time_series(int_value=17)]
        result = _read_undelivered_count(client, "projects/p/subscriptions/s")
        assert result == 17

    def test_uses_most_recent_point_when_series_has_multiple(self):
        """A API do Cloud Monitoring retorna pontos em ordem reversa
        (mais recente primeiro) — regressao para bug onde o codigo lia
        points[-1] (o ponto mais antigo da janela) em vez de points[0]."""
        client = MagicMock()
        series = MagicMock()
        newest_point = MagicMock()
        newest_point.value.int64_value = 5
        oldest_point = MagicMock()
        oldest_point.value.int64_value = 200
        series.points = [newest_point, oldest_point]
        client.list_time_series.return_value = [series]

        result = _read_undelivered_count(client, "projects/p/subscriptions/s")

        assert result == 5

    def test_returns_none_when_no_series(self):
        client = MagicMock()
        client.list_time_series.return_value = []
        result = _read_undelivered_count(client, "projects/p/subscriptions/s")
        assert result is None

    def test_returns_none_when_empty_points(self):
        client = MagicMock()
        series = MagicMock()
        series.points = []
        client.list_time_series.return_value = [series]
        result = _read_undelivered_count(client, "projects/p/subscriptions/s")
        assert result is None

    def test_retries_on_failure_then_succeeds(self):
        client = MagicMock()
        client.list_time_series.side_effect = [
            ConnectionError("fail"),
            [_fake_time_series(int_value=5)],
        ]
        with patch("observability.monitoring.time.sleep"):
            result = _read_undelivered_count(client, "projects/p/subscriptions/s")
        assert result == 5
        assert client.list_time_series.call_count == 2

    def test_raises_after_max_attempts(self):
        client = MagicMock()
        client.list_time_series.side_effect = ConnectionError("unavailable")
        with patch("observability.monitoring.time.sleep"):
            with pytest.raises(ConnectionError):
                _read_undelivered_count(client, "projects/p/subscriptions/s")
        assert client.list_time_series.call_count == MAX_ATTEMPTS

    def test_returns_double_value(self):
        """Suporta double_value como fallback."""
        client = MagicMock()
        series = MagicMock()
        point = MagicMock()
        point.value.int64_value = None
        point.value.double_value = 99.5
        series.points = [point]
        client.list_time_series.return_value = [series]
        result = _read_undelivered_count(client, "projects/p/subscriptions/s")
        assert result == 99


class TestGetConsumerLag:
    """Verifica que get_consumer_lag retorna None em falha (best-effort)."""

    def test_returns_none_on_error(self):
        with patch("observability.monitoring.monitoring_v3") as mock_monitoring:
            mock_monitoring.MetricServiceClient.return_value.list_time_series.side_effect = (
                ConnectionError("network")
            )
            with patch("observability.monitoring.time.sleep"):
                result = get_consumer_lag()
            assert result is None

    def test_returns_value_on_success(self):
        with patch("observability.monitoring.monitoring_v3") as mock_monitoring:
            mock_client = mock_monitoring.MetricServiceClient.return_value
            mock_client.list_time_series.return_value = [_fake_time_series(int_value=10)]
            result = get_consumer_lag()
            assert result == 10

    def test_accepts_custom_subscription(self):
        with patch("observability.monitoring.monitoring_v3") as mock_monitoring:
            mock_client = mock_monitoring.MetricServiceClient.return_value
            mock_client.list_time_series.return_value = [_fake_time_series(int_value=0)]
            result = get_consumer_lag(subscription_name="custom-sub")
            assert result == 0
