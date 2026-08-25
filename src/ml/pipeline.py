"""Orquestração do modelo de IA sobre a Gold: treina, avalia e materializa a
view de predição. Roda depois de `gold.pipeline.run_gold` — depende de
`fact_meta_resultado_municipio` e `dim_municipio` já materializadas.

Passo isolado do resto do pipeline batch (não é chamado por `gold.pipeline`)
de propósito: treinar um modelo é uma operação distinta de materializar uma
tabela determinística, e `make ml` roda sob demanda, sem custo implícito em
todo `make pipeline`.
"""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

from google.cloud import bigquery

from common.retry import with_retry
from config import get_settings
from ml import model as ml_model
from observability.logging import log_execution, setup_logger


@with_retry()
def _run_query(client: bigquery.Client, sql: str, timeout: int) -> bigquery.table.RowIterator:
    """Operação atômica: submete a query e aguarda a conclusão (com retry).

    `maximum_bytes_billed` cap: JOIN acidentalmente caro falha a query em vez
    de gerar fatura, mesmo padrão de `extraction.extraction`."""
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=ml_model.MAX_BYTES_BILLED)
    return client.query(sql, job_config=job_config, timeout=timeout).result(timeout=timeout)


def run_ml() -> None:
    """Treina o modelo de risco de não-alfabetização, registra as métricas de
    avaliação e materializa a view de predição pronta para consumo."""
    logger = setup_logger()
    client = bigquery.Client(project=get_settings().project_id)

    with log_execution(step="ML_Train", layer="Gold"):
        _run_query(client, ml_model.render_train_ddl(), ml_model.TRAIN_TIMEOUT_SECONDS)

    metricas = list(_run_query(client, ml_model.render_evaluate_query(), ml_model.QUERY_TIMEOUT_SECONDS))
    if metricas:
        logger.info(f"ML.EVALUATE {ml_model.MODEL_NAME}: {dict(metricas[0])}")

    with log_execution(step="ML_Predict", layer="Gold"):
        _run_query(client, ml_model.render_predictions_view_ddl(), ml_model.QUERY_TIMEOUT_SECONDS)
