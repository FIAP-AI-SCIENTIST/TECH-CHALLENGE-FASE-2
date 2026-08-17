"""Best-effort evidence writer for BigQuery.

Retry com backoff via `common.retry.with_retry` (mesmo padrão de
`observability.audit` e da implementação original da Nic): uma falha de
gravação de evidência nunca derruba o pipeline — o caller decide.
"""
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Iterable

from common.retry import with_retry
from config import get_settings

from .translate import QualityResult

TIMEOUT_SECONDS = 10
TABLE_ID = "data_quality_log"


def rows_for_bigquery(results: Iterable[QualityResult]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    return [{**asdict(result), "timestamp": result.timestamp or now} for result in results]


@with_retry()
def _do_insert(client, table: str, rows: list[dict]) -> list:
    """Operação atômica: insere o lote e levanta se o BigQuery devolver erros."""
    errors = client.insert_rows_json(table, rows, timeout=TIMEOUT_SECONDS)
    if errors:
        raise RuntimeError(f"BigQuery insert returned errors: {errors}")
    return []


def write_results(results: Iterable[QualityResult], client=None, table: str | None = None) -> list:
    """Insert evidence; writer failures are raised to the caller, which treats
    them as best-effort (quality.pipeline already swallows and logs them).
    """
    rows = rows_for_bigquery(results)
    if not rows:
        return []
    if client is None:
        from google.cloud import bigquery
        client = bigquery.Client()
    settings = get_settings()
    return _do_insert(client, table or f"{settings.dataset_id}.{TABLE_ID}", rows)
