"""Testes das faixas numéricas (ge/le) declaradas nos contratos.

Os limites vêm do min/max real do dataset público de origem (consulta
documentada no docstring de `contracts.models`) cruzado com as faixas do
Data Quality — estes testes fixam tanto os extremos válidos quanto a
rejeição do que está fora.
"""

import pytest
from pydantic import ValidationError

from contracts.models import (
    DadosAlunosRecord,
    MetaAlfabetizacaoMunicipioRecord,
    MetaAlfabetizacaoUFRecord,
    MunicipioRecord,
    UFRecord,
)

INDICADOR_MODELS = [UFRecord, MunicipioRecord]


class TestFaixasIndicador:
    """UF e Municipio: percentuais 0-100 e media_portugues na escala de proficiência."""

    @pytest.mark.parametrize("modelo", INDICADOR_MODELS)
    @pytest.mark.parametrize("valor", [0.0, 100.0])
    def test_percentuais_nos_extremos_passam(self, modelo, valor):
        inst = modelo(taxa_alfabetizacao=valor, proporcao_aluno_nivel_0=valor,
                      proporcao_aluno_nivel_8=valor)
        assert inst.taxa_alfabetizacao == valor

    @pytest.mark.parametrize("modelo", INDICADOR_MODELS)
    @pytest.mark.parametrize("valor", [-0.01, 100.01])
    def test_percentuais_fora_da_faixa_rejeitam(self, modelo, valor):
        with pytest.raises(ValidationError):
            modelo(taxa_alfabetizacao=valor)
        with pytest.raises(ValidationError):
            modelo(proporcao_aluno_nivel_3=valor)

    @pytest.mark.parametrize("modelo", INDICADOR_MODELS)
    def test_media_portugues_escala_proficiencia(self, modelo):
        """Dado real vai de ~673 a ~868 — a faixa NÃO é 0-10."""
        inst = modelo(media_portugues=797.34)
        assert inst.media_portugues == 797.34
        with pytest.raises(ValidationError):
            modelo(media_portugues=1000.01)


class TestFaixasMeta:
    """Tabelas de meta: trajetória 2024-2030 e participação em 0-100; nível em 0-5."""

    def test_metas_e_participacao_nos_extremos_passam(self):
        inst = MetaAlfabetizacaoUFRecord(
            meta_alfabetizacao_2024=0.0, meta_alfabetizacao_2030=100.0,
            percentual_participacao=100.0,
        )
        assert inst.meta_alfabetizacao_2030 == 100.0

    def test_metas_fora_da_faixa_rejeitam(self):
        with pytest.raises(ValidationError):
            MetaAlfabetizacaoUFRecord(meta_alfabetizacao_2027=100.01)
        with pytest.raises(ValidationError):
            MetaAlfabetizacaoUFRecord(percentual_participacao=-0.01)

    def test_nivel_alfabetizacao_dominio_real_0_a_5(self):
        """Dado real observado: 0-5 (não apenas 0/1)."""
        assert MetaAlfabetizacaoMunicipioRecord(nivel_alfabetizacao=0).nivel_alfabetizacao == 0
        assert MetaAlfabetizacaoMunicipioRecord(nivel_alfabetizacao=5).nivel_alfabetizacao == 5
        with pytest.raises(ValidationError):
            MetaAlfabetizacaoMunicipioRecord(nivel_alfabetizacao=6)
        with pytest.raises(ValidationError):
            MetaAlfabetizacaoMunicipioRecord(nivel_alfabetizacao=-1)


class TestFaixasAlunos:
    """Alunos: proficiência 0-1000 (faixa do Data Quality) e peso amostral não-negativo."""

    def test_proficiencia_nos_extremos_passa(self):
        assert DadosAlunosRecord(proficiencia=0.0).proficiencia == 0.0
        assert DadosAlunosRecord(proficiencia=1000.0).proficiencia == 1000.0

    def test_proficiencia_fora_da_faixa_rejeita(self):
        with pytest.raises(ValidationError):
            DadosAlunosRecord(proficiencia=1000.01)
        with pytest.raises(ValidationError):
            DadosAlunosRecord(proficiencia=-0.01)

    def test_peso_aluno_sem_teto_mas_nao_negativo(self):
        """Peso amostral real vai de ~0.095 a ~142.5 — só se exige ge=0."""
        assert DadosAlunosRecord(peso_aluno=0.0952381).peso_aluno == pytest.approx(0.0952381)
        assert DadosAlunosRecord(peso_aluno=142.5454).peso_aluno == pytest.approx(142.5454)
        with pytest.raises(ValidationError):
            DadosAlunosRecord(peso_aluno=-0.1)


class TestFaixaAno:
    """Ano: banda de sanidade 2000-2100 — cobre o dado real (2023-2025) e o
    sintético (2024-2026) sem prender edições futuras da pesquisa."""

    @pytest.mark.parametrize("ano", [2023, 2024, 2025, 2026])
    def test_anos_reais_e_sinteticos_passam(self, ano):
        assert UFRecord(ano=ano).ano == ano

    @pytest.mark.parametrize("ano", [0, 1999, 2101, 99999])
    def test_anos_lixo_rejeitam(self, ano):
        with pytest.raises(ValidationError):
            UFRecord(ano=ano)


class TestNonePassa:
    """Campo Optional com ge/le aceita None — ausência de valor não é violação
    de faixa (completude é verificada pelo Data Quality, não pelo contrato)."""

    @pytest.mark.parametrize("modelo", [
        UFRecord, MunicipioRecord, MetaAlfabetizacaoUFRecord,
        MetaAlfabetizacaoMunicipioRecord, DadosAlunosRecord,
    ])
    def test_instancia_toda_none_valida(self, modelo):
        assert modelo() is not None
