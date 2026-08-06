"""Testes do módulo observability.audit — inserção BigQuery com retry."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from observability.audit import (
    MAX_ATTEMPTS,
    _build_payload,
    _insert_audit_row,
    write_audit_row,
)
from observability.logging import RunContext


def _make_run(**kwargs):
    defaults = dict(
        run_id="test-id",
        unit="test-unit",
        layer="bronze",
        timestamp=datetime(2025, 1, 1, 12, 0, 0),
    )
    defaults.update(kwargs)
    return RunContext(**defaults)


class TestBuildPayload:
    """Verifica montagem do payload para BigQuery."""

    def test_all_required_fields_present(self):
        run = _make_run()
        payload = _build_payload(run)

        assert payload["run_id"] == "test-id"
        assert payload["unit"] == "test-unit"
        assert payload["layer"] == "bronze"
        assert payload["status"] == "SUCCESS"
        assert payload["timestamp"] == "2025-01-01T12:00:00"

    def test_optional_fields_can_be_none(self):
        run = _make_run(rows_read=None, rows_written=None)
        payload = _build_payload(run)
        assert payload["rows_read"] is None
        assert payload["rows_written"] is None

    def test_duration_included(self):
        run = _make_run(duration_seconds=3.14)
        payload = _build_payload(run)
        assert payload["duration_seconds"] == 3.14


class TestInsertAuditRow:
    """Verifica retry e backoff de _insert_audit_row."""

    def test_success_on_first_try(self):
        client = MagicMock()
        client.insert_rows_json.return_value = []
        _insert_audit_row(client, "project.dataset.table", {"key": "val"})
        assert client.insert_rows_json.call_count == 1

    def test_success_after_failures(self):
        """Sobrecorre duas vezes, depois sucede."""
        client = MagicMock()
        client.insert_rows_json.side_effect = [
            ["error1"],
            ["error2"],
            [],  # sucesso
        ]
        with patch("observability.audit.time.sleep"):
            _insert_audit_row(client, "p.d.t", {"key": "val"})
        assert client.insert_rows_json.call_count == 3

    def test_raises_after_max_attempts(self):
        """Todas as tentativas falham — levanta RuntimeError."""
        client = MagicMock()
        client.insert_rows_json.return_value = ["fail"]
        with patch("observability.audit.time.sleep"):
            with pytest.raises(RuntimeError, match=f"after {MAX_ATTEMPTS} attempts"):
                _insert_audit_row(client, "p.d.t", {"key": "val"})
        assert client.insert_rows_json.call_count == MAX_ATTEMPTS

    def test_raises_on_exception(self):
        """Exceção do client — também respeita MAX_ATTEMPTS."""
        client = MagicMock()
        client.insert_rows_json.side_effect = ConnectionError("timeout")
        with patch("observability.audit.time.sleep"):
            with pytest.raises(ConnectionError):
                _insert_audit_row(client, "p.d.t", {"key": "val"})
        assert client.insert_rows_json.call_count == MAX_ATTEMPTS


class TestWriteAuditRow:
    """Verifica que write_audit_row é best-effort (nunca propaga)."""

    def test_never_propagates_exception(self):
        run = _make_run()
        with patch("observability.audit.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.insert_rows_json.return_value = ["fail"]
            with patch("observability.audit.time.sleep"):
                # Não deve levantar
                write_audit_row(run)

    def test_silent_on_success(self):
        run = _make_run()
        with patch("observability.audit.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.insert_rows_json.return_value = []
            write_audit_row(run)
            mock_client_cls.return_value.insert_rows_json.assert_called_once()
