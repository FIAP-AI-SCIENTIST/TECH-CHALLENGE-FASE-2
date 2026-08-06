"""Leitura de métricas do Cloud Monitoring, com foco em Consumer Lag do Pub/Sub."""

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from google.cloud import monitoring_v3

from observability.logging import setup_logger

PROJECT_ID = "useful-space-277919"
DEFAULT_SUBSCRIPTION = "alfabetizacao-streaming-consumer-sub"
METRIC_TYPE = "pubsub.googleapis.com/subscription/num_undelivered_messages"
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = (0.5, 1.0, 2.0)
TIMEOUT_SECONDS = 10


def _read_undelivered_count(client: monitoring_v3.MetricServiceClient, subscription_path: str) -> Optional[int]:
    """Lê o número de mensagens não entregues via Cloud Monitoring.

    Filtra por ``METRIC_TYPE`` e label da subscription, intervalo de
    5 minutos. Retorna o ponto mais recente ou ``None`` se não houver
    séries. Implementa retry com backoff.
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=5)

    filter_expr = (
        f'metric.type="{METRIC_TYPE}" '
        f'AND resource.label.subscription_id='
        f'"{subscription_path.split("/")[-1]}"'
    )

    request = monitoring_v3.ListTimeSeriesRequest(
        {
            "name": f"projects/{PROJECT_ID}",
            "filter": filter_expr,
            "interval": {
                "start_time": {"seconds": int(start_time.timestamp())},
                "end_time": {"seconds": int(end_time.timestamp())},
            },
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )

    for tentativa in range(MAX_ATTEMPTS):
        try:
            response = client.list_time_series(request=request, timeout=TIMEOUT_SECONDS)
        except Exception:
            if tentativa < MAX_ATTEMPTS - 1:
                time.sleep(BACKOFF_SECONDS[tentativa])
                continue
            raise

        for series in response:
            if series.points:
                point = series.points[-1]
                if point.value.int64_value is not None:
                    return int(point.value.int64_value)
                if point.value.double_value is not None:
                    return int(point.value.double_value)
        # Sem pontos — retorna None
        return None

    raise RuntimeError(f"Failed to read consumer lag after {MAX_ATTEMPTS} attempts")


def get_consumer_lag(subscription_name: str = DEFAULT_SUBSCRIPTION) -> Optional[int]:
    """Retorna o Consumer Lag (mensagens não entregues) de uma subscription.

    Retorna ``None`` em caso de falha, sem propagar exceção.
    """
    logger = setup_logger()
    try:
        client = monitoring_v3.MetricServiceClient()
        subscription_path = f"projects/{PROJECT_ID}/subscriptions/{subscription_name}"
        return _read_undelivered_count(client, subscription_path)
    except Exception as exc:
        logger.error(
            f"Erro ao ler Consumer Lag: {type(exc).__name__}: {exc}"
        )
        return None
