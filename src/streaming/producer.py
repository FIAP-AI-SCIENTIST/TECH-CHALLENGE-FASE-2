"""Geração e publicação de eventos sintéticos para simular ingestão em tempo quase real."""

import random
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
import uuid

from google.cloud import pubsub_v1

from common.retry import with_retry
from contracts.models import (
    DadosAlunosRecord,
    MetaAlfabetizacaoMunicipioRecord,
    MetaAlfabetizacaoUFRecord,
    MunicipioRecord,
    UFRecord,
)
from observability.logging import log_execution

PROJECT_ID = "useful-space-277919"
TOPIC_NAME = "alfabetizacao-streaming-events"
TIMEOUT_SECONDS = 10

# Mapa tipo_evento -> modelo(s) candidatos (contratos ja existentes, sem contrato novo)
EVENT_TYPE_MODELS = {
    "medicao": [(DadosAlunosRecord, "alunos")],
    "meta": [
        (MetaAlfabetizacaoUFRecord, "meta_alfabetizacao_uf"),
        (MetaAlfabetizacaoMunicipioRecord, "meta_alfabetizacao_municipio"),
    ],
    "indicador": [(UFRecord, "uf"), (MunicipioRecord, "municipio")],
}

_UFS = ["SP", "RJ", "MG", "BA", "CE", "PR", "PE", "RS"]


def gerar_evento_sintetico(tipo_evento: str):
    """Gera uma instância válida (contrato Pydantic já existente) para o tipo de evento pedido.

    Função pura: mesmos argumentos podem gerar valores diferentes (é
    aleatório por natureza), mas o resultado SEMPRE passa na validação
    Pydantic do modelo escolhido e os valores numéricos ficam dentro de
    faixas plausíveis.

    Retorna (instancia, entidade).
    """
    candidatos = EVENT_TYPE_MODELS.get(tipo_evento)
    if not candidatos:
        raise ValueError(f"tipo_evento desconhecido: {tipo_evento}")

    modelo, entidade = random.choice(candidatos)
    ano = random.randint(2024, 2026)

    if modelo is DadosAlunosRecord:
        instancia = DadosAlunosRecord(
            ano=ano,
            id_municipio=str(random.randint(1100000, 5399999)),
            id_escola=str(uuid.uuid4()),
            id_aluno=str(uuid.uuid4()),
            caderno=str(random.randint(1, 4)),
            serie="2",
            rede=str(random.randint(1, 4)),
            presenca="1",
            preenchimento_caderno="1",
            alfabetizado=random.choice(["0", "1"]),
            proficiencia=round(random.uniform(400.0, 900.0), 2),
            peso_aluno=round(random.uniform(0.5, 2.0), 4),
        )
    elif modelo is MetaAlfabetizacaoUFRecord:
        instancia = MetaAlfabetizacaoUFRecord(
            ano=ano,
            sigla_uf=random.choice(_UFS),
            rede=str(random.randint(0, 6)),
            taxa_alfabetizacao=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2024=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2025=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2026=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2027=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2028=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2029=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2030=round(random.uniform(0.0, 100.0), 2),
            percentual_participacao=round(random.uniform(0.0, 100.0), 2),
        )
    elif modelo is MetaAlfabetizacaoMunicipioRecord:
        instancia = MetaAlfabetizacaoMunicipioRecord(
            ano=ano,
            id_municipio=str(random.randint(1100000, 5399999)),
            nivel_alfabetizacao=random.choice([0, 1]),
            rede=str(random.randint(0, 6)),
            taxa_alfabetizacao=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2024=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2025=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2026=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2027=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2028=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2029=round(random.uniform(0.0, 100.0), 2),
            meta_alfabetizacao_2030=round(random.uniform(0.0, 100.0), 2),
            percentual_participacao=round(random.uniform(0.0, 100.0), 2),
        )
    elif modelo is UFRecord:
        instancia = UFRecord(
            ano=ano,
            sigla_uf=random.choice(_UFS),
            serie="2",
            rede=str(random.randint(0, 6)),
            taxa_alfabetizacao=round(random.uniform(0.0, 100.0), 2),
            media_portugues=round(random.uniform(0.0, 10.0), 2),
            proporcao_aluno_nivel_0=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_1=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_2=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_3=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_4=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_5=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_6=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_7=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_8=round(random.uniform(0.0, 100.0), 2),
        )
    else:  # MunicipioRecord
        instancia = MunicipioRecord(
            ano=ano,
            id_municipio=str(random.randint(1100000, 5399999)),
            serie="2",
            rede=str(random.randint(0, 6)),
            taxa_alfabetizacao=round(random.uniform(0.0, 100.0), 2),
            media_portugues=round(random.uniform(0.0, 10.0), 2),
            proporcao_aluno_nivel_0=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_1=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_2=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_3=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_4=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_5=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_6=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_7=round(random.uniform(0.0, 100.0), 2),
            proporcao_aluno_nivel_8=round(random.uniform(0.0, 100.0), 2),
        )

    return instancia, entidade


@with_retry()
def _do_publish(client: pubsub_v1.PublisherClient, topic: str, data: bytes, attributes: dict) -> str:
    """Operação atômica: publica a mensagem e aguarda confirmação (com timeout)."""
    future = client.publish(topic, data, timeout=TIMEOUT_SECONDS, **attributes)
    return future.result(timeout=TIMEOUT_SECONDS)


def produce_events(tipo_evento: str, n: int = 1) -> None:
    """Gera e publica `n` eventos sintéticos do tipo pedido — execução single-shot.

    Não é um processo de longa duração: gera o lote, publica, termina.
    Repetição/agendamento é responsabilidade de um trigger externo.
    """
    with log_execution(unit="Streaming_Producer", layer="Bronze") as run:
        client = pubsub_v1.PublisherClient()
        topic_path = client.topic_path(PROJECT_ID, TOPIC_NAME)

        published = 0
        for _ in range(n):
            instancia, entidade = gerar_evento_sintetico(tipo_evento)
            data = instancia.model_dump_json().encode("utf-8")
            attributes = {"tipo_evento": tipo_evento, "entidade": entidade}
            _do_publish(client, topic_path, data, attributes)
            published += 1

        run.rows_written = published
