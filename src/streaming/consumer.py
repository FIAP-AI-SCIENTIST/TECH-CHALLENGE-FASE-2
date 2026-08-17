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
from contracts.registry import is_registered, model_for
from contracts.schema_mapper import to_pyarrow_schema
from contracts.serialization import to_pyarrow_table
from observability.logging import log_execution, setup_logger
from observability.monitoring import get_consumer_lag

TIMEOUT_SECONDS = 10
LAG_WARNING_THRESHOLD = 100
# Teto de caracteres do payload no log de descarte: o suficiente para
# identificar o que veio errado, sem despejar o corpo inteiro de cada mensagem
# ruim no log estruturado.
PAYLOAD_LOG_MAX_CHARS = 256


def _payload_para_log(data: bytes) -> str:
    """Payload em texto, truncado, para servir de evidência do descarte.

    `errors="replace"` porque o payload de uma mensagem irrecuperável pode não
    ser UTF-8 válido — e o log dessa mensagem é justamente onde isso precisa
    aparecer, em vez de levantar outra exceção.
    """
    texto = data.decode("utf-8", errors="replace")
    if len(texto) <= PAYLOAD_LOG_MAX_CHARS:
        return texto
    return f"{texto[:PAYLOAD_LOG_MAX_CHARS]}... (truncado)"


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

    O tratamento de erro distingue duas naturezas de falha, e essa distinção é o
    que impede uma mensagem ruim de parar o consumo:

    - **Irrecuperável** — entidade não registrada ou payload que não satisfaz o
      contrato. Reentregar produz o mesmo resultado para sempre, então a mensagem
      é descartada: log de erro com `message_id`, entidade e payload truncado,
      e **ack** para que ela saia da subscription. Como cada execução puxa até
      `max_messages`, mensagens nessa situação acumuladas ocupariam o lote inteiro
      e nenhuma mensagem boa voltaria a ser consumida.
    - **Transitório** — a mensagem é válida, mas a escrita na Bronze falhou (erro
      de rede/GCS). Aqui a reentrega **é** o mecanismo de recuperação: a mensagem
      **não** é ackada e volta no próximo consumo.

    O descarte é perda de dado deliberada e localizada, aceitável porque a fonte
    pode republicar e a evidência fica registrada. A solução completa é uma
    dead-letter queue no Pub/Sub, que troca descarte por quarentena — isto aqui é
    mitigação do modo de falha, não substituto dela.

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
        # Ackadas por serem irrecuperáveis — contadas em separado das gravadas,
        # porque "saiu da subscription" aqui significa descartada, não processada.
        ack_ids_descartadas: list = []
        rows_read = 0

        for msg in messages:
            rows_read += 1
            entidade = msg.message.attributes.get("entidade")
            if not is_registered(entidade):
                logger.error(
                    f"Entidade desconhecida em mensagem recebida: {entidade}",
                    extra={
                        "message_id": msg.message.message_id,
                        "payload": _payload_para_log(msg.message.data),
                    },
                )
                ack_ids_descartadas.append(msg.ack_id)
                continue
            modelo = model_for(entidade)

            try:
                payload = json.loads(msg.message.data.decode("utf-8"))
                instancia = modelo(**payload)
            except Exception as exc:
                logger.error(
                    f"Falha ao decodificar/validar mensagem: {type(exc).__name__}: {exc}",
                    extra={
                        "message_id": msg.message.message_id,
                        "entidade": entidade,
                        "payload": _payload_para_log(msg.message.data),
                    },
                )
                ack_ids_descartadas.append(msg.ack_id)
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
            # `data_evento` fica fora dos contratos Pydantic de propósito: o
            # event time só existe no streaming (a extração batch não tem), e
            # colocá-lo no modelo compartilhado propagaria uma coluna sempre nula
            # às linhas de batch. A Silver **lê** estas partições (`read_partition`
            # sem chave devolve `ano=` e `data_ingestao=` juntas, com os schemas
            # promovidos) e descarta esta coluna na entrada, para que o schema da
            # camada não dependa de ter havido micro-batch antes da execução.
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
                # Não ackar — a mensagem é válida e a reentrega é o mecanismo de
                # recuperação para falha transitória de escrita.

        # Gravadas e descartadas saem juntas da subscription, por motivos opostos:
        # as primeiras porque foram processadas, as segundas porque reentregá-las
        # repetiria o mesmo erro indefinidamente.
        ack_ids = ack_ids_ok + ack_ids_descartadas
        if ack_ids:
            _do_ack(client, subscription_path, ack_ids)

        if ack_ids_descartadas:
            logger.error(
                f"{len(ack_ids_descartadas)} mensagem(ns) irrecuperável(is) descartada(s) e ackada(s) "
                "para não bloquear a subscription — ver os logs de erro anteriores para o payload"
            )

        run.rows_read = rows_read
        run.rows_written = rows_written
        # Nota de leitura da auditoria: `rows_read - rows_written` é "linhas não
        # gravadas nesta execução", o que soma dois casos distintos — descartadas
        # (não voltam) e válidas cuja escrita falhou (voltam na reentrega). Quem
        # precisa separar os dois usa o log de descarte acima, que traz a contagem.

        lag = get_consumer_lag()
        if lag is not None and lag > LAG_WARNING_THRESHOLD:
            logger.warning(f"Consumer Lag acima do limiar: {lag} mensagens não entregues")
