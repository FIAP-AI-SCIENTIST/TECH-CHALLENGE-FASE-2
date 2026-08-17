"""Geração e publicação de eventos sintéticos para simular ingestão em tempo quase real."""

import random
import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
import uuid

from google.cloud import pubsub_v1

from common.retry import with_retry
from config import get_settings
from contracts.models import (
    DadosAlunosRecord,
    MetaAlfabetizacaoMunicipioRecord,
    MetaAlfabetizacaoUFRecord,
    MunicipioRecord,
    UFRecord,
)
from contracts.registry import model_for
from observability.logging import log_execution

TIMEOUT_SECONDS = 10

# tipo_evento -> entidades candidatas. O contrato de cada entidade vem do
# registro (`contracts.registry`), que é a fonte única; este mapa carrega só a
# composição da demo sintética, que é conhecimento do producer.
#
# Renomeado de EVENT_TYPE_MODELS: o mapa nunca mais guarda modelo, e um nome que
# diz "MODELS" mandaria o próximo leitor procurar contrato no lugar errado.
EVENT_TYPE_ENTITIES: dict[str, list[str]] = {
    "medicao": ["alunos"],
    "meta": ["meta_alfabetizacao_uf", "meta_alfabetizacao_municipio"],
    "indicador": ["uf", "municipio"],
}

_UFS = ["SP", "RJ", "MG", "BA", "CE", "PR", "PE", "RS"]

# Códigos IBGE reais e estáveis. O gerador sintético precisa produzir eventos
# que também sejam aceitos pelos FKs da Gold: sortear qualquer inteiro de 7
# dígitos cria uma linha válida no Pydantic, mas órfã em `dim_municipio`, fazendo
# o `quality-gate` bloquear a demo. Estes oito municípios foram verificados no
# diretório oficial carregado em `dim_municipio` (rodada GCP de 2026-08-17), um
# por UF da lista acima. Não consultamos BigQuery por evento: o Producer roda em
# Cloud Function e uma consulta por mensagem seria mais lenta e mais cara que a
# própria publicação.
_MUNICIPIOS_VALIDOS = [
    "3550308",  # São Paulo/SP
    "3304557",  # Rio de Janeiro/RJ
    "3106200",  # Belo Horizonte/MG
    "2927408",  # Salvador/BA
    "2304400",  # Fortaleza/CE
    "4106902",  # Curitiba/PR
    "2611606",  # Recife/PE
    "4314902",  # Porto Alegre/RS
]


def gerar_evento_sintetico(tipo_evento: str):
    """Gera uma instância válida (contrato Pydantic já existente) para o tipo de evento pedido.

    Função pura: mesmos argumentos podem gerar valores diferentes (é
    aleatório por natureza), mas o resultado SEMPRE passa na validação
    Pydantic do modelo escolhido e os valores numéricos ficam dentro de
    faixas plausíveis.

    Retorna (instancia, entidade).
    """
    candidatos = EVENT_TYPE_ENTITIES.get(tipo_evento)
    if not candidatos:
        raise ValueError(f"tipo_evento desconhecido: {tipo_evento}")

    entidade = random.choice(candidatos)
    modelo = model_for(entidade)
    ano = random.randint(2024, 2026)

    if modelo is DadosAlunosRecord:
        instancia = DadosAlunosRecord(
            ano=ano,
            id_municipio=random.choice(_MUNICIPIOS_VALIDOS),
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
            id_municipio=random.choice(_MUNICIPIOS_VALIDOS),
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
            id_municipio=random.choice(_MUNICIPIOS_VALIDOS),
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
    with log_execution(step="Streaming_Producer", layer="Bronze") as run:
        client = pubsub_v1.PublisherClient()
        settings = get_settings()
        topic_path = client.topic_path(settings.project_id, settings.topic_name)

        published = 0
        for _ in range(n):
            instancia, entidade = gerar_evento_sintetico(tipo_evento)
            data = instancia.model_dump_json().encode("utf-8")
            attributes = {"tipo_evento": tipo_evento, "entidade": entidade}
            _do_publish(client, topic_path, data, attributes)
            published += 1

        run.rows_written = published


def cloud_function_entrypoint(request):
    """Ponto de entrada HTTP para o Cloud Function (Gen2), disparado pelo Cloud Scheduler.

    Aceita `tipo_evento` (default "indicador", sorteado entre os 3 tipos se
    omitido) e `n` (default 1) via query string ou corpo JSON. Retorna uma
    tupla (corpo, status) no formato esperado pelo Functions Framework.
    """
    args = request.args or {}
    body = request.get_json(silent=True) or {}

    tipo_evento = args.get("tipo_evento") or body.get("tipo_evento") or random.choice(list(EVENT_TYPE_ENTITIES))
    n = int(args.get("n") or body.get("n") or 1)

    try:
        produce_events(tipo_evento, n=n)
    except Exception as exc:
        return f"Falha ao produzir eventos: {type(exc).__name__}: {exc}", 500

    return f"Publicados {n} eventos do tipo {tipo_evento}", 200
