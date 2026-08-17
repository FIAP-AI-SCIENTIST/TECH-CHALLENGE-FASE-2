"""Testes de `config.Settings`.

Todos constroem `Settings(_env_file=None)`: o `.env` real do repositório existe e
exporta `TF_VAR_project_id`, então lê-lo aqui tornaria o resultado dependente da
máquina de quem roda a suíte.

O ambiente é montado pelo context manager `ambiente()`, não pela fixture
`monkeypatch`: Hypothesis não reinicia fixture de escopo de função entre os casos
gerados por `@given`, e um teste de propriedade que herda o ambiente do caso
anterior não prova nada.
"""

import os
from contextlib import contextmanager
from typing import Generator
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from config import Settings, get_settings

# Alfabeto de project id do GCP: minúsculas, dígitos e hífen.
_PROJECT_IDS = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=30
).filter(lambda s: s.strip() != "")

_VARIAVEIS = (
    "GCP_PROJECT_ID",
    "TF_VAR_project_id",
    "GCP_DATASET_ID",
    "GCP_BUCKET_NAME",
    "GCP_SOURCE_DATASET",
    "PUBSUB_TOPIC",
    "PUBSUB_SUBSCRIPTION",
)


@contextmanager
def ambiente(**valores: str) -> Generator[None]:
    """Ambiente determinístico: descarta toda variável reconhecida por `Settings`
    (inclusive as que o `conftest.py` define para a suíte) e aplica só as passadas."""
    base = {chave: valor for chave, valor in os.environ.items() if chave not in _VARIAVEIS}
    base.update(valores)
    with patch.dict(os.environ, base, clear=True):
        yield


def _settings() -> Settings:
    return Settings(_env_file=None)  # pyright: ignore[reportCallIssue]


# --- Falha fechada -----------------------------------------------------------

def test_projeto_ausente_levanta() -> None:
    """Sem projeto configurado, aborta em vez de assumir um default.

    A mensagem nomeia a variável de ambiente (`GCP_PROJECT_ID`), não o campo
    interno — é o que o operador precisa ler para corrigir.
    """
    with ambiente():
        with pytest.raises(ValidationError) as erro:
            _settings()
    assert "GCP_PROJECT_ID" in str(erro.value)


@pytest.mark.parametrize("valor", ["", "   ", "\t"])
def test_projeto_vazio_levanta(valor: str) -> None:
    """String vazia falharia adiante em `bigquery.Client` sem apontar a causa."""
    with ambiente(GCP_PROJECT_ID=valor):
        with pytest.raises(ValidationError):
            _settings()


# --- Resolução do project_id -------------------------------------------------


def test_projeto_normalizado() -> None:
    """Espaço de copiar/colar produziria nome de bucket inválido."""
    with ambiente(GCP_PROJECT_ID="  meu-projeto  "):
        assert _settings().project_id == "meu-projeto"


def test_nome_canonico_vence_alias() -> None:
    """Com os dois definidos, o nome canônico tem precedência."""
    with ambiente(GCP_PROJECT_ID="canonico", TF_VAR_project_id="alias"):
        assert _settings().project_id == "canonico"


def test_alias_terraform_sozinho_resolve() -> None:
    """O `.env` atual do grupo só exporta `TF_VAR_project_id`."""
    with ambiente(TF_VAR_project_id="vindo-do-terraform"):
        assert _settings().project_id == "vindo-do-terraform"


# --- Derivação do bucket -----------------------------------------------------


def test_bucket_derivado_do_projeto() -> None:
    """Espelha `name = "${var.project_id}-datalake"` do Terraform."""
    with ambiente(GCP_PROJECT_ID="meu-projeto"):
        assert _settings().bucket_name == "meu-projeto-datalake"


def test_bucket_override_vence() -> None:
    """Saída de emergência para emulador local ou bucket legado."""
    with ambiente(GCP_PROJECT_ID="meu-projeto", GCP_BUCKET_NAME="bucket-legado"):
        assert _settings().bucket_name == "bucket-legado"


# --- Contrato do objeto ------------------------------------------------------


def test_settings_imutavel() -> None:
    """Configuração mutável reintroduziria o estado global que a centralização remove."""
    with ambiente(GCP_PROJECT_ID="meu-projeto"):
        settings = _settings()
        with pytest.raises(ValidationError):
            settings.project_id = "outro"  # pyright: ignore[reportAttributeAccessIssue]


def test_get_settings_cacheado() -> None:
    """O pipeline chama isto em centenas de pontos de I/O."""
    assert get_settings() is get_settings()


def test_variavel_desconhecida_ignorada() -> None:
    """O `.env` compartilhado carrega várias `TF_VAR_*` que não são da aplicação."""
    with ambiente(GCP_PROJECT_ID="meu-projeto", TF_VAR_billing_account="XXXXXX-XXXXXX"):
        assert _settings().project_id == "meu-projeto"


def test_defaults_de_infraestrutura() -> None:
    """Os defaults têm de bater com o que o Terraform provisiona."""
    with ambiente(GCP_PROJECT_ID="meu-projeto"):
        settings = _settings()
    assert settings.dataset_id == "alfabetizacao_analytics"
    assert settings.source_dataset == "basedosdados.br_inep_avaliacao_alfabetizacao"
    assert settings.topic_name == "alfabetizacao-streaming-events"
    assert settings.subscription_name == "alfabetizacao-streaming-consumer-sub"


# --- Property-Based Testing --------------------------------------------------


@given(project_id=_PROJECT_IDS)
def test_pbt_derivacao_do_bucket_sempre_sufixada(project_id: str) -> None:
    """Invariante: sem override, o bucket é sempre o projeto mais o sufixo do Terraform."""
    with ambiente(GCP_PROJECT_ID=project_id):
        settings = _settings()
    assert settings.bucket_name == f"{settings.project_id}-datalake"
    assert settings.bucket_name.endswith("-datalake")


@given(project_id=_PROJECT_IDS, override=_PROJECT_IDS)
def test_pbt_override_ignora_o_projeto(project_id: str, override: str) -> None:
    """Invariante: com override, o `project_id` não influencia o bucket."""
    with ambiente(GCP_PROJECT_ID=project_id, GCP_BUCKET_NAME=override):
        assert _settings().bucket_name == override


@given(project_id=_PROJECT_IDS)
def test_pbt_idempotencia(project_id: str) -> None:
    """Invariante: o mesmo ambiente sempre produz a mesma configuração."""
    with ambiente(GCP_PROJECT_ID=project_id):
        assert _settings() == _settings()


@given(project_id=_PROJECT_IDS, espacos=st.text(alphabet=" \t", max_size=4))
def test_pbt_metamorfico_espacos_nao_alteram_resultado(project_id: str, espacos: str) -> None:
    """Metamórfica: envolver o valor em espaço não muda nem o projeto nem o bucket."""
    with ambiente(GCP_PROJECT_ID=project_id):
        limpo = _settings()
    with ambiente(GCP_PROJECT_ID=f"{espacos}{project_id}{espacos}"):
        com_espacos = _settings()

    assert com_espacos.project_id == limpo.project_id
    assert com_espacos.bucket_name == limpo.bucket_name
