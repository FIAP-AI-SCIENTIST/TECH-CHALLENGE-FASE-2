"""Consumo de eventos do Pub/Sub e persistência em micro-batches na camada Bronze."""

import json
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
from collections import defaultdict
from datetime import date

from google.cloud import pubsub_v1

from bronze import writer as bronze_writer
from common.retry import with_retry
from contracts.models import (
    DadosAlunosRecord,
    MetaAlfabetizacaoMunicipioRecord,
    MetaAlfabetizacaoUFRecord,
    MunicipioRecord,
    UFRecord,
)
from contracts.schema_mapper import to_pyarrow_schema
from contracts.serialization import to_pyarrow_table
from observability.logging import log_execution, setup_logger
from observability.monitoring import get_consumer_lag

PROJECT_ID = "useful-space-277919"
SUBSCRIPTION_NAME = "alfabetizacao-streaming-consumer-sub"
TIMEOUT_SECONDS = 10
LAG_WARNING_THRESHOLD = 100

# Mapa entidade -> modelo Pydantic, para decodificar sem inspecionar o payload
ENTITY_MODEL_MAP = {
    "alunos": DadosAlunosRecord,
    "meta_alfabetizacao_uf": MetaAlfabetizacaoUFRecord,
    "meta_alfabetizacao_municipio": MetaAlfabetizacaoMunicipioRecord,
    "uf": UFRecord,
    "municipio": MunicipioRecord,
}


@with_retry()
def _do_pull(client: pubsub_v1.SubscriberClient, subscription: str, max_messages: int):
    """Operação atômica: pull de até max_messages mensagens (com timeout)."""
    response = client.pull(
        request={"subscription": subscription, "max_messages": max_messages},
        timeout=TIMEOUT_SECONDS,
    )
    return response.received_messages


@with_retry()
def _do_ack(client: pubsub_v1.SubscriberClient, subscription: str, ack_ids: list) -> None:
    """Operação atômica: confirma o processamento das mensagens (com timeout)."""
    client.acknowledge(
        request={"subscription": subscription, "ack_ids": ack_ids},
        timeout=TIMEOUT_SECONDS,
    )


def consume_batch(max_messages: int = 10, timeout: float = 5.0) -> None:
    """Consome um micro-batch de eventos e grava na Bronze — execução single-shot.

    Cada mensagem é processada e ackada independentemente: uma mensagem
    malformada não impede o ack das demais que deram certo (isolamento
    por mensagem). Ack só acontece depois da escrita confirmada na Bronze
    (garante at-least-once, NFR05).
    """
    logger = setup_logger()

    with log_execution(unit="Streaming_Consumer", layer="Bronze") as run:
        client = pubsub_v1.SubscriberClient()
        subscription_path = client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

        messages = _do_pull(client, subscription_path, max_messages)

        chave = f"data_ingestao={date.today().isoformat()}"
        grupos: dict = defaultdict(list)
        ack_ids_ok: list = []
        rows_read = 0

        for msg in messages:
            rows_read += 1
            entidade = msg.message.attributes.get("entidade")
            modelo = ENTITY_MODEL_MAP.get(entidade)
            if modelo is None:
                logger.error(
                    f"Entidade desconhecida em mensagem recebida: {entidade}",
                    extra={"message_id": msg.message.message_id},
                )
                continue

            try:
                payload = json.loads(msg.message.data.decode("utf-8"))
                instancia = modelo(**payload)
            except Exception as exc:
                logger.error(
                    f"Falha ao decodificar/validar mensagem: {type(exc).__name__}: {exc}",
                    extra={"message_id": msg.message.message_id, "entidade": entidade},
                )
                continue

            grupos[entidade].append((instancia, modelo, msg.ack_id))

        chaves_limpas: set = set()
        rows_written = 0
        for entidade, itens in grupos.items():
            if entidade not in chaves_limpas:
                bronze_writer.clear_partition(entidade, chave)
                chaves_limpas.add(entidade)

            instancias = [i for i, _m, _a in itens]
            modelo = itens[0][1]
            schema = to_pyarrow_schema(modelo)
            table = to_pyarrow_table(instancias, schema)

            try:
                written = bronze_writer.write_partition(entidade, chave, table, part_index=0)
                rows_written += written
                ack_ids_ok.extend(ack_id for _i, _m, ack_id in itens)
            except Exception as exc:
                logger.error(
                    f"Falha ao escrever partição da Bronze: {type(exc).__name__}: {exc}",
                    extra={"entidade": entidade},
                )
                # Não ackar — mensagens ficam pendentes, Pub/Sub reentrega

        if ack_ids_ok:
            _do_ack(client, subscription_path, ack_ids_ok)

        run.rows_read = rows_read
        run.rows_written = rows_written

        lag = get_consumer_lag()
        if lag is not None and lag > LAG_WARNING_THRESHOLD:
            logger.warning(f"Consumer Lag acima do limiar: {lag} mensagens não entregues")
