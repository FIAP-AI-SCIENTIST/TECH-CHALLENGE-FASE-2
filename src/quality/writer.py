"""Gravação de resultados de Data Quality no BigQuery — mesmo padrão de
`observability.audit`: best-effort, uma falha ao gravar evidência nunca
derruba o pipeline principal.
"""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

from common.retry import with_retry
from observability.logging import setup_logger
from quality.checks import QualityResult

PROJECT_ID = "useful-space-277919"
DATASET_ID = "alfabetizacao_analytics"
TABLE_ID = "data_quality_log"
TIMEOUT_SECONDS = 10


def _build_payload(resultado: QualityResult, timestamp: datetime) -> dict:
    return {
        "check_id": str(uuid.uuid4()),
        "check": resultado.check,
        "entidade": resultado.entidade,
        "passou": resultado.passou,
        "linhas_afetadas": resultado.linhas_afetadas,
        "detalhe": resultado.detalhe,
        "timestamp": timestamp.isoformat(),
    }


@with_retry()
def _do_insert(client: bigquery.Client, table_id: str, payloads: list[dict]) -> None:
    """Operação atômica: insere o lote de resultados e levanta se houver erro."""
    errors = client.insert_rows_json(table_id, payloads, timeout=TIMEOUT_SECONDS)
    if errors:
        raise RuntimeError("BigQuery insert returned errors")


def write_quality_results(resultados: list[QualityResult]) -> None:
    """Grava todos os resultados de uma rodada de checks em um único
    insert (passando ou não — o histórico de qualidade importa mesmo
    quando tudo está OK). Nunca propaga exceção.
    """
    if not resultados:
        return

    logger = setup_logger()
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        timestamp = datetime.now(timezone.utc)
        payloads = [_build_payload(r, timestamp) for r in resultados]
        _do_insert(client, table_id, payloads)
    except Exception as exc:
        logger.error(f"Falha ao gravar resultados de Data Quality: {type(exc).__name__}: {exc}")
