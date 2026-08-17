"""Registro de entidades do pipeline: entidade -> contrato.

Fonte única. Antes desta tabela, o contrato de uma entidade era declarado em
**três** lugares independentes — na extração batch (`extraction.ENTITY_TABLE_MAP`),
no producer de streaming e no consumer de streaming — e nada impedia que
divergissem. O modo de falha era silencioso e assimétrico: registrar a entidade
só no producer publicava mensagens que o consumer rejeitava; registrar só no
consumer deixava o dado inalcançável.

Fica em `contracts/` porque "qual contrato governa esta entidade" é conhecimento
de contrato, não de transporte (Pub/Sub) nem de origem (BigQuery). Quem consome
o registro é que decide o que fazer com ele: a extração acrescenta a tabela de
origem, o streaming acrescenta o tipo de evento.

Registrar uma fonte nova é acrescentar uma entrada em `ENTITY_MODELS`. Isso a
torna publicável e consumível pelo streaming, e elegível para a extração batch
(que também precisa da tabela de origem). **Não** a faz chegar à Silver: essa
camada exige chave de negócio declarada por entidade — ver `docs/fontes-externas.md`.
"""

from pydantic import BaseModel

from contracts.models import (
    DadosAlunosRecord,
    MetaAlfabetizacaoBrasilRecord,
    MetaAlfabetizacaoMunicipioRecord,
    MetaAlfabetizacaoUFRecord,
    MunicipioRecord,
    UFRecord,
)

ENTITY_MODELS: dict[str, type[BaseModel]] = {
    "uf": UFRecord,
    "municipio": MunicipioRecord,
    "meta_alfabetizacao_brasil": MetaAlfabetizacaoBrasilRecord,
    "meta_alfabetizacao_uf": MetaAlfabetizacaoUFRecord,
    "meta_alfabetizacao_municipio": MetaAlfabetizacaoMunicipioRecord,
    "alunos": DadosAlunosRecord,
}


class EntidadeNaoRegistrada(LookupError):
    """Entidade sem contrato registrado.

    Herda de `LookupError` e não de `KeyError` de propósito: `str(KeyError(msg))`
    envolve a mensagem em aspas (é o `repr` da chave), o que deixaria ilegível
    justamente a mensagem que diz onde registrar a entidade.
    """


def model_for(entidade: str) -> type[BaseModel]:
    """Resolve o contrato de uma entidade, ou falha dizendo o que fazer."""
    modelo = ENTITY_MODELS.get(entidade)
    if modelo is None:
        conhecidas = ", ".join(sorted(ENTITY_MODELS))
        raise EntidadeNaoRegistrada(
            f"Entidade '{entidade}' não está registrada. Declare o contrato em "
            f"contracts/registry.py:ENTITY_MODELS. Entidades conhecidas: {conhecidas}"
        )
    return modelo


def is_registered(entidade: str | None) -> bool:
    """Consulta sem exceção — para quem decide descartar em vez de abortar.

    Aceita `None` porque o chamador típico é o consumer lendo um atributo de
    mensagem que pode simplesmente não vir.
    """
    return entidade in ENTITY_MODELS
