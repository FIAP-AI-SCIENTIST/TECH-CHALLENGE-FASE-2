"""Consumo de eventos do Pub/Sub e persistência em micro-batches na camada Bronze."""

import json
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
from collections import defaultdict
from datetime import date

import pyarrow as pa
from google.cloud import pubsub_v1

from bronze import writer as bronze_writer
from common.retry import with_retry
from config import get_settings
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


def consume_batch(max_messages: int = 10) -> None:
    """Consome um micro-batch de eventos e grava na Bronze — execução single-shot.

    Cada mensagem é processada e ackada independentemente: uma mensagem
    malformada não impede o ack das demais que deram certo (isolamento
    por mensagem). Ack só acontece depois da escrita confirmada na Bronze
    (garante at-least-once).

    A escrita é append-only: a partição do dia acumula um arquivo por
    execução e nunca é limpa, então rodar o consumer várias vezes no mesmo
    dia soma micro-batches em vez de substituí-los.
    """
    logger = setup_logger()

    with log_execution(step="Streaming_Consumer", layer="Bronze") as run:
        client = pubsub_v1.SubscriberClient()
        settings = get_settings()
        subscription_path = client.subscription_path(settings.project_id, settings.subscription_name)

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

            # publish_time é o event time: quando o evento entrou no Pub/Sub.
            # Gravado junto para permitir medir o lag ponta a ponta do streaming
            # (data_ingestao da partição - data_evento da linha).
            grupos[entidade].append((instancia, modelo, msg.ack_id, msg.message.publish_time))

        rows_written = 0
        for entidade, itens in grupos.items():
            instancias = [i for i, _m, _a, _p in itens]
            modelo = itens[0][1]
            schema = to_pyarrow_schema(modelo)
            table = to_pyarrow_table(instancias, schema)
            # data_evento fica fora dos contratos Pydantic de propósito: ela só
            # existe no streaming (a extração batch não tem event time), e as
            # partições data_ingestao= da Bronze não são lidas pela Silver —
            # colocar o campo no modelo compartilhado propagaria uma coluna
            # toda nula para as camadas de batch.
            table = table.append_column(
                pa.field("data_evento", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.array([publish_time for _i, _m, _a, publish_time in itens], type=pa.timestamp("us", tz="UTC")),
            )

            try:
                # Nunca limpa a partição: "data_ingestao=<hoje>" é compartilhada
                # por todos os micro-batches do dia. O part_id é o run_id desta
                # execução, então o arquivo nunca colide com o de um run anterior
                # e a Bronze mantém o histórico completo do dia.
                written = bronze_writer.write_partition(entidade, chave, table, part_id=run.run_id)
                rows_written += written
                ack_ids_ok.extend(ack_id for _i, _m, ack_id, _p in itens)
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
