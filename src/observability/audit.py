"""Escrita de linhas de auditoria no BigQuery para rastreio de execuções."""

import time
from typing import Optional

from google.cloud import bigquery

from observability.logging import RunContext, setup_logger

PROJECT_ID = "useful-space-277919"
DATASET_ID = "alfabetizacao_analytics"
TABLE_ID = "pipeline_audit_log"
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = (0.5, 1.0, 2.0)
TIMEOUT_SECONDS = 10


def _build_payload(run: RunContext) -> dict:
    """Monta o dicionário para insert_rows_json do BigQuery.

    Todos os campos REQUIRED do schema nunca são None;
    timestamp é serializado como string ISO 8601.
    """
    return {
        "run_id": run.run_id,
        "unit": run.unit,
        "layer": run.layer,
        "rows_read": run.rows_read,
        "rows_written": run.rows_written,
        "duration_seconds": run.duration_seconds,
        "status": run.status,
        "timestamp": run.timestamp.isoformat(),
    }


def _insert_audit_row(client: bigquery.Client, table_id: str, payload: dict) -> None:
    """Insere uma linha de auditoria com retry exponencial.

    BigQuery.insert_rows_json retorna uma lista de erros (não levanta
    exceção em erro de linha). Lista não-vazia é tratada como falha.
    Após todas as tentativas esgotadas, levanta RuntimeError.
    """
    for tentativa in range(MAX_ATTEMPTS):
        try:
            errors = client.insert_rows_json(table_id, [payload], timeout=TIMEOUT_SECONDS)
        except Exception:
            if tentativa < MAX_ATTEMPTS - 1:
                time.sleep(BACKOFF_SECONDS[tentativa])
                continue
            raise

        if not errors:
            return

        if tentativa < MAX_ATTEMPTS - 1:
            time.sleep(BACKOFF_SECONDS[tentativa])
            continue

    raise RuntimeError(f"Failed to insert audit row after {MAX_ATTEMPTS} attempts")


def write_audit_row(run: RunContext) -> None:
    """Escreve uma linha de auditoria no BigQuery (best-effort).

    Nunca propaga exceção: falhas são logadas com severidade ERROR
    e ignoradas para não interferir no fluxo principal.
    """
    logger = setup_logger()
    try:
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        payload = _build_payload(run)
        _insert_audit_row(client, table_id, payload)
    except Exception as exc:
        logger.error(
            f"Erro ao escrever auditoria: {type(exc).__name__}: {exc}",
            extra={"run_id": run.run_id, "unit": run.unit, "layer": run.layer},
        )
