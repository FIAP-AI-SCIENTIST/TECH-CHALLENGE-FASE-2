"""Configuração de ambiente do pipeline — única fonte de verdade dos identificadores
de infraestrutura (projeto GCP, bucket, dataset, tópico, subscription).

Antes desta mudança, o identificador do projeto GCP estava literal em 11 módulos
e nenhum arquivo de `src/` lia variável de ambiente: clonar o repositório e
apontar para outro projeto exigia editar código. Agora exige editar só o `.env`.

Acesso via `get_settings()`, cacheado por processo. Teste que precisa de outro
ambiente chama `get_settings.cache_clear()` depois de ajustar as variáveis.

Falha fechada de propósito: sem `GCP_PROJECT_ID` (ou `TF_VAR_project_id`) a
construção levanta `ValidationError` na primeira chamada, em vez de seguir com
um identificador vazio e gravar em lugar nenhum.
"""

from functools import lru_cache

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Identificadores de infraestrutura lidos do ambiente ou do `.env`.

    `TF_VAR_project_id` é aceito como alias porque o `.env` do projeto já o
    exporta para o Terraform — assim um único arquivo configura as duas
    ferramentas, sem duplicar o mesmo valor sob dois nomes.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    project_id: str = Field(
        validation_alias=AliasChoices("GCP_PROJECT_ID", "TF_VAR_project_id"),
        description="Projeto GCP que fatura as queries e hospeda os recursos.",
    )
    dataset_id: str = Field(
        default="alfabetizacao_analytics",
        validation_alias=AliasChoices("GCP_DATASET_ID"),
        description="Dataset BigQuery da camada Gold, auditoria e Data Quality.",
    )
    source_dataset: str = Field(
        default="basedosdados.br_inep_avaliacao_alfabetizacao",
        validation_alias=AliasChoices("GCP_SOURCE_DATASET"),
        description="Dataset público da Base dos Dados lido pela extração batch.",
    )
    topic_name: str = Field(
        default="alfabetizacao-streaming-events",
        validation_alias=AliasChoices("PUBSUB_TOPIC"),
    )
    subscription_name: str = Field(
        default="alfabetizacao-streaming-consumer-sub",
        validation_alias=AliasChoices("PUBSUB_SUBSCRIPTION"),
    )
    bucket_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("GCP_BUCKET_NAME"),
        description="Só é necessário se o bucket não seguir a convenção do Terraform.",
    )

    @field_validator("project_id")
    @classmethod
    def _project_id_nao_vazio(cls, valor: str) -> str:
        if not valor.strip():
            raise ValueError("project_id não pode ser vazio")
        return valor.strip()

    @property
    def bucket_name(self) -> str:
        """Bucket do data lake.

        O Terraform o nomeia como `${project_id}-datalake`
        (`infra/modules/storage/main.tf`). Derivar aqui evita duas variáveis que
        sempre andam juntas e que, se divergirem, fazem a pipeline escrever num
        bucket e a infra criar outro.
        """
        return self.bucket_override or f"{self.project_id}-datalake"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Configuração do processo. Cacheada — leia sempre por esta função."""
    return Settings()
