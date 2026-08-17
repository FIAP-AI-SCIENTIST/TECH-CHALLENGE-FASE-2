"""Configuração compartilhada da suíte.

`config.Settings` falha fechada quando `GCP_PROJECT_ID` não está definido — é o
comportamento desejado em produção (melhor abortar do que gravar num projeto
vazio), mas a suíte precisa de um ambiente determinístico que não dependa do
`.env` de quem está rodando os testes.

O valor abaixo é fictício de propósito: nenhum teste toca serviço real (todo
cliente GCS/BigQuery/Pub/Sub é mockado), então um identificador falso torna
explícito que uma chamada de rede que escape do mock vai falhar em vez de
acertar a infraestrutura de alguém.
"""

import os

import pytest

TEST_PROJECT_ID = "test-project"
TEST_DATASET_ID = "test_dataset"


@pytest.fixture(autouse=True, scope="session")
def _ambiente_de_teste() -> None:
    """Fixa a configuração da suíte antes de qualquer import de `config`."""
    os.environ.setdefault("GCP_PROJECT_ID", TEST_PROJECT_ID)
    os.environ.setdefault("GCP_DATASET_ID", TEST_DATASET_ID)

    from config import get_settings

    get_settings.cache_clear()
