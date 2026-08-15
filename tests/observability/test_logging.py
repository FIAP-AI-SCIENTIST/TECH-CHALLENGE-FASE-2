"""Testes do módulo observability.logging — context manager e logger JSON."""

import json
import logging
from unittest.mock import patch

import pytest

from observability.logging import log_execution, setup_logger, _logger_initialized


def _reset_logger_state():
    """Reseta o estado do logger para evitar interferência entre testes."""
    global _logger_initialized
    logger = logging.getLogger("pipeline")
    logger.handlers.clear()
    _logger_initialized = False


class TestSetupLogger:
    """Verifica configuração idempotente do logger JSON."""

    def setup_method(self):
        _reset_logger_state()

    def teardown_method(self):
        _reset_logger_state()

    def test_returns_logger(self):
        logger = setup_logger()
        assert isinstance(logger, logging.Logger)
        assert logger.name == "pipeline"

    def test_no_duplicate_handlers(self):
        logger = setup_logger()
        handler_count = len(logger.handlers)
        setup_logger()
        assert len(logger.handlers) == handler_count

    def test_emits_json(self, caplog):
        logger = setup_logger()
        with caplog.at_level(logging.INFO, logger="pipeline"):
            logger.info("teste json")
        record = caplog.records[-1]
        assert record.levelno == logging.INFO
        assert record.getMessage() == "teste json"


class TestLogExecution:
    """Verifica o context manager log_execution."""

    def setup_method(self):
        _reset_logger_state()

    def teardown_method(self):
        _reset_logger_state()

    def test_success_status_and_duration(self):
        with patch("observability.audit.write_audit_row") as mock_write:
            with log_execution("unit-A", "bronze") as run:
                run.rows_read = 100
                run.rows_written = 95

            assert run.status == "SUCCESS"
            assert run.duration_seconds >= 0
            assert run.rows_read == 100
            assert run.rows_written == 95
            mock_write.assert_called_once()
            written_run = mock_write.call_args[0][0]
            assert written_run.status == "SUCCESS"

    def test_error_status_and_reraise(self):
        with patch("observability.audit.write_audit_row") as mock_write:
            with pytest.raises(ValueError, match="boom"):
                with log_execution("unit-B", "silver") as run:
                    run.rows_read = 50
                    raise ValueError("boom")

            mock_write.assert_called_once()
            written_run = mock_write.call_args[0][0]
            assert written_run.status == "ERROR"
            assert written_run.duration_seconds >= 0

    def test_error_logs_error_level(self, caplog):
        _reset_logger_state()
        with patch("observability.audit.write_audit_row"):
            with pytest.raises(RuntimeError):
                with log_execution("unit-C", "gold"):
                    raise RuntimeError("falha")

        error_records = [
            r for r in caplog.records
            if r.levelno == logging.ERROR
        ]
        assert len(error_records) >= 1
        assert "RuntimeError" in error_records[0].getMessage()

    def test_unique_run_id_per_invocation(self):
        with patch("observability.audit.write_audit_row"):
            with log_execution("u", "l") as r1:
                id1 = r1.run_id
            with log_execution("u", "l") as r2:
                id2 = r2.run_id
            assert id1 != id2

    def test_preserves_caller_quality_status(self):
        with patch("observability.audit.write_audit_row") as mock_write:
            with log_execution("DataQuality", "Quality") as run:
                run.status = "SUCCESS_WITH_DQ_FAILURE"
            assert mock_write.call_args.args[0].status == "SUCCESS_WITH_DQ_FAILURE"
