"""Testes do registro de entidades — fonte única de entidade -> contrato.

O valor do registro não é a resolução em si (é um dicionário), e sim a garantia
de que os três consumidores — extração batch, producer e consumer de streaming —
concordam sobre quais entidades existem. Os testes de coerência abaixo são o que
impede a divergência silenciosa que motivou o registro.
"""

import pytest
from pydantic import BaseModel

from contracts.models import DadosAlunosRecord, UFRecord
from contracts.registry import (
    ENTITY_MODELS,
    EntidadeNaoRegistrada,
    is_registered,
    model_for,
)
from contracts.schema_mapper import to_pyarrow_schema


class TestModelFor:
    def test_resolves_registered_entity(self):
        assert model_for("uf") is UFRecord
        assert model_for("alunos") is DadosAlunosRecord

    def test_unknown_entity_raises_with_actionable_message(self):
        with pytest.raises(EntidadeNaoRegistrada) as exc:
            model_for("censo_escolar")

        mensagem = str(exc.value)
        # A mensagem é interface para quem vai registrar uma fonte nova: precisa
        # dizer o que faltou, onde declarar e o que já existe.
        assert "censo_escolar" in mensagem
        assert "contracts/registry.py" in mensagem
        assert "uf" in mensagem

    def test_message_is_not_repr_wrapped(self):
        """Regressão do motivo de herdar `LookupError` e não `KeyError`: `str()` de
        um `KeyError` devolve o `repr` do argumento, envolvendo a mensagem inteira
        em aspas e escondendo a instrução.

        A asserção olha o **fim** da mensagem, não o começo: o começo cita o nome
        da entidade entre aspas de propósito, então `startswith` não discrimina.
        """
        with pytest.raises(EntidadeNaoRegistrada) as exc:
            model_for("inexistente")

        mensagem = str(exc.value)
        assert not mensagem.endswith(("'", '"'))
        # E, de forma direta: a mensagem é a própria string, não o repr dela.
        assert mensagem == exc.value.args[0]


class TestIsRegistered:
    def test_true_for_registered(self):
        assert is_registered("municipio") is True

    def test_false_for_unknown(self):
        assert is_registered("censo_escolar") is False

    def test_false_for_none(self):
        """O chamador típico é o consumer lendo um atributo que pode não vir."""
        assert is_registered(None) is False


class TestRegistryCoherence:
    """Os consumidores do registro não podem conhecer entidade que ele não conhece."""

    def test_extraction_entities_are_registered(self):
        from extraction.extraction import ENTITY_TABLE_MAP

        assert set(ENTITY_TABLE_MAP) <= set(ENTITY_MODELS)

    def test_producer_entities_are_registered(self):
        from streaming.producer import EVENT_TYPE_ENTITIES

        publicaveis = {e for entidades in EVENT_TYPE_ENTITIES.values() for e in entidades}
        assert publicaveis <= set(ENTITY_MODELS)

    def test_silver_entities_are_registered(self):
        """A Silver processa entidades da fonte; todas têm de ter contrato.

        A tabela integrada é derivada (nasce de um JOIN na Silver, não de
        ingestão), então não passa por contrato de entrada e é excluída.
        """
        from silver.pipeline import ENTIDADES

        assert set(ENTIDADES) <= set(ENTITY_MODELS)


class TestRegisteredContractsAreUsable:
    """Toda entidade registrada tem de atravessar o caminho de serialização.

    É a propriedade que o registro promete a quem adiciona uma fonte: registrar
    basta para o dado ser publicável e gravável em Parquet.
    """

    @pytest.mark.parametrize("entidade", sorted(ENTITY_MODELS))
    def test_contract_is_a_model_with_arrow_schema(self, entidade):
        modelo = model_for(entidade)
        assert issubclass(modelo, BaseModel)

        schema = to_pyarrow_schema(modelo)
        assert schema.names == list(modelo.model_fields)

    @pytest.mark.parametrize("entidade", sorted(ENTITY_MODELS))
    def test_empty_instance_round_trips(self, entidade):
        """Todos os campos dos contratos são opcionais, então a instância vazia é
        válida e serve de round-trip mínimo sem depender de gerar valor plausível."""
        modelo = model_for(entidade)
        instancia = modelo()

        assert modelo(**instancia.model_dump()) == instancia
