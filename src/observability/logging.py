"""Módulo de logging estruturado em JSON para auditoria de execuções do pipeline."""

import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

_logger_initialized: bool = False


@dataclass
class RunContext:
    """Contexto de uma execução do pipeline, usado para auditoria."""
    run_id: str
    unit: str
    layer: str
    timestamp: datetime
    rows_read: int | None = None
    rows_written: int | None = None
    total_bytes_processed: int | None = None  # bytes scaneados pela query BigQuery
    duration_seconds: float = 0.0
    status: str = "SUCCESS"

class _JSONFormatter(logging.Formatter):
    """Serializa registros de log como linhas JSON para stdout.

    O agente do Cloud Logging no GCP faz parsing automático de JSON
    emitido para stdout, sem necessidade de chamada de rede síncrona.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "severity": record.levelname,
            "message": record.getMessage(),
        }
        # Adiciona campos extras do RunContext quando presentes
        for key in ("run_id", "unit", "layer", "status"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        for key in ("rows_read", "rows_written", "duration_seconds", "total_bytes_processed"):
            val = getattr(record, key, None)
            if val is not None:
                payload[key] = val
        return json.dumps(payload)


def setup_logger(name: str = "pipeline") -> logging.Logger:
    """Configura um logger com emitente JSON para stdout.

    Seguro para chamadas repetidas: handlers só são adicionados na
    primeira invocação.
    """
    logger = logging.getLogger(name)
    global _logger_initialized

    if _logger_initialized:
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(_JSONFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    _logger_initialized = True
    return logger


@contextmanager
def log_execution(unit: str, layer: str):
    """Context manager que registra início/fim de execução com auditoria.

    O caller pode mutar ``run.rows_read`` e ``run.rows_written``
    durante o bloco ``with``. Em sucesso, grava status SUCCESS;
    em exceção, grava ERROR e relança a exceção original.
    """
    logger = setup_logger()
    run_id = str(uuid.uuid4())
    run = RunContext(
        run_id=run_id,
        unit=unit,
        layer=layer,
        timestamp=datetime.now(timezone.utc),
    )

    logger.info(
        "Início da execução",
        extra={
            "run_id": run_id,
            "unit": unit,
            "layer": layer,
        },
    )

    start = time.monotonic()
    try:
        yield run
    except Exception as exc:
        run.duration_seconds = round(time.monotonic() - start, 3)
        run.status = "ERROR"
        logger.error(
            f"{type(exc).__name__}: {exc}",
            extra={
                "run_id": run_id,
                "unit": unit,
                "layer": layer,
                "status": run.status,
                "duration_seconds": run.duration_seconds,
            },
        )
        _write_audit_safely(run, logger)
        # O status definido pelo caller (ex.: SUCCESS_WITH_DQ_FAILURE) é preservado.
        raise
    else:
        run.duration_seconds = round(time.monotonic() - start, 3)
        _write_audit_safely(run, logger)
        logger.info(
            "Fim da execução — sucesso",
            extra={
                "run_id": run_id,
                "unit": unit,
                "layer": layer,
                "status": run.status,
                "rows_read": run.rows_read,
                "rows_written": run.rows_written,
                "total_bytes_processed": run.total_bytes_processed,
                "duration_seconds": run.duration_seconds,
            },
        )


def _write_audit_safely(run: RunContext, logger: logging.Logger) -> None:
    """Escreve linha de auditoria de forma best-effort.

    Uma falha ao gravar auditoria nunca derruba o fluxo principal
    nem impede o relançamento da exceção original.
    """
    # Importação local para quebrar ciclo: audit importa logging
    from observability.audit import write_audit_row  # noqa: PLC0414

    try:
        write_audit_row(run)
    except Exception as audit_exc:
        logger.error(
            f"Falha ao gravar auditoria: {type(audit_exc).__name__}: {audit_exc}",
            extra={
                "run_id": run.run_id,
                "unit": run.unit,
                "layer": run.layer,
            },
        )
