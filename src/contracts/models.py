"""Contratos lógicos das entidades do pipeline.

As faixas numéricas (ge/le) foram calibradas com o min/max real do dataset
público `basedosdados.br_inep_avaliacao_alfabetizacao` (consulta em ago/2026)
cruzado com as faixas declaradas no Data Quality (`quality.rules.VALUE_RANGES`):

- `ano`: dado real 2023-2025; eventos sintéticos 2024-2026. A faixa 2000-2100
  é banda de sanidade contra lixo (ex: 0, 99999), não restrição de edição —
  edições futuras da pesquisa não devem exigir alteração de contrato.
- `media_portugues`: escala de proficiência (real: 673-868), NÃO nota 0-10.
- `peso_aluno`: peso amostral sem teto natural (real: 0.095-142.5) — só se
  exige não-negatividade.
- `nivel_alfabetizacao`: domínio real 0-5.
- Percentuais (taxas, proporções, metas, participação): 0-100 por definição.
- `proficiencia`: 0-1000, mesma faixa do check `consistencia_faixa`.

Padrões de string (`sigla_uf`, `id_municipio`) ficam fora do contrato de
propósito: são verificados pelo Data Quality (`COLUMN_PATTERNS`), que gera
evidência sem descartar a linha na extração.
"""
from pydantic import BaseModel, Field

class UFRecord(BaseModel):
    """Contrato lógico para a entidade UF."""
    ano: int | None = Field(default=None, ge=2000, le=2100)
    sigla_uf: str | None = None
    serie: str | None = None
    rede: str | None = None
    taxa_alfabetizacao: float | None = Field(default=None, ge=0.0, le=100.0)
    media_portugues: float | None = Field(default=None, ge=0.0, le=1000.0)
    proporcao_aluno_nivel_0: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_1: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_2: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_3: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_4: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_5: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_6: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_7: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_8: float | None = Field(default=None, ge=0.0, le=100.0)

class MunicipioRecord(BaseModel):
    """Contrato lógico para a entidade Municipio."""
    ano: int | None = Field(default=None, ge=2000, le=2100)
    id_municipio: str | None = None
    serie: str | None = None
    rede: str | None = None
    taxa_alfabetizacao: float | None = Field(default=None, ge=0.0, le=100.0)
    media_portugues: float | None = Field(default=None, ge=0.0, le=1000.0)
    proporcao_aluno_nivel_0: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_1: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_2: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_3: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_4: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_5: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_6: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_7: float | None = Field(default=None, ge=0.0, le=100.0)
    proporcao_aluno_nivel_8: float | None = Field(default=None, ge=0.0, le=100.0)

class _MetaAlfabetizacaoBaseRecord(BaseModel):
    """Campos comuns às 3 tabelas de meta (Brasil/UF/Municipio). Não é uma tabela própria."""
    ano: int | None = Field(default=None, ge=2000, le=2100)
    rede: str | None = None
    taxa_alfabetizacao: float | None = Field(default=None, ge=0.0, le=100.0)
    meta_alfabetizacao_2024: float | None = Field(default=None, ge=0.0, le=100.0)
    meta_alfabetizacao_2025: float | None = Field(default=None, ge=0.0, le=100.0)
    meta_alfabetizacao_2026: float | None = Field(default=None, ge=0.0, le=100.0)
    meta_alfabetizacao_2027: float | None = Field(default=None, ge=0.0, le=100.0)
    meta_alfabetizacao_2028: float | None = Field(default=None, ge=0.0, le=100.0)
    meta_alfabetizacao_2029: float | None = Field(default=None, ge=0.0, le=100.0)
    meta_alfabetizacao_2030: float | None = Field(default=None, ge=0.0, le=100.0)
    percentual_participacao: float | None = Field(default=None, ge=0.0, le=100.0)

class MetaAlfabetizacaoBrasilRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao Brasil."""
    pass

class MetaAlfabetizacaoUFRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao por UF."""
    sigla_uf: str | None = None

class MetaAlfabetizacaoMunicipioRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao por Municipio."""
    id_municipio: str | None = None
    nivel_alfabetizacao: int | None = Field(default=None, ge=0, le=5)

class DadosAlunosRecord(BaseModel):
    """Contrato lógico para a entidade de Dados Agregados do Indicador."""
    ano: int | None = Field(default=None, ge=2000, le=2100)
    id_municipio: str | None = None
    id_escola: str | None = None
    id_aluno: str | None = None
    caderno: str | None = None
    serie: str | None = None
    rede: str | None = None
    presenca: str | None = None
    preenchimento_caderno: str | None = None
    alfabetizado: str | None = None #codigo 0/1 (não/sim)
    proficiencia: float | None = Field(default=None, ge=0.0, le=1000.0)
    peso_aluno: float | None = Field(default=None, ge=0.0)
