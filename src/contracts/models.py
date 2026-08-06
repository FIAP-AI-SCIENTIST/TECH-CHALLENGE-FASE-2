from pydantic import BaseModel

class UFRecord(BaseModel):
    """Contrato lógico para a entidade UF."""
    ano: int | None = None
    sigla_uf: str | None = None
    serie: str | None = None
    rede: str | None = None
    taxa_alfabetizacao: float | None = None
    media_portugues: float | None = None
    proporcao_aluno_nivel_0: float | None = None
    proporcao_aluno_nivel_1: float | None = None
    proporcao_aluno_nivel_2: float | None = None
    proporcao_aluno_nivel_3: float | None = None
    proporcao_aluno_nivel_4: float | None = None
    proporcao_aluno_nivel_5: float | None = None
    proporcao_aluno_nivel_6: float | None = None
    proporcao_aluno_nivel_7: float | None = None
    proporcao_aluno_nivel_8: float | None = None

class MunicipioRecord(BaseModel):
    """Contrato lógico para a entidade Municipio."""
    ano: int | None = None
    id_municipio: str | None = None
    serie: str | None = None
    rede: str | None = None
    taxa_alfabetizacao: float | None = None
    media_portugues: float | None = None
    proporcao_aluno_nivel_0: float | None = None
    proporcao_aluno_nivel_1: float | None = None
    proporcao_aluno_nivel_2: float | None = None
    proporcao_aluno_nivel_3: float | None = None
    proporcao_aluno_nivel_4: float | None = None
    proporcao_aluno_nivel_5: float | None = None
    proporcao_aluno_nivel_6: float | None = None
    proporcao_aluno_nivel_7: float | None = None
    proporcao_aluno_nivel_8: float | None = None

class _MetaAlfabetizacaoBaseRecord(BaseModel):
    """Campos comuns às 3 tabelas de meta (Brasil/UF/Municipio). Não é uma tabela própria."""
    ano: int | None = None
    rede: str | None = None
    taxa_alfabetizacao: float | None = None
    meta_alfabetizacao_2024: float | None = None
    meta_alfabetizacao_2025: float | None = None
    meta_alfabetizacao_2026: float | None = None
    meta_alfabetizacao_2027: float | None = None
    meta_alfabetizacao_2028: float | None = None
    meta_alfabetizacao_2029: float | None = None
    meta_alfabetizacao_2030: float | None = None
    percentual_participacao: float | None = None

class MetaAlfabetizacaoBrasilRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao Brasil."""
    pass

class MetaAlfabetizacaoUFRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao por UF."""
    sigla_uf: str | None = None

class MetaAlfabetizacaoMunicipioRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao por Municipio."""
    id_municipio: str | None = None
    nivel_alfabetizacao: int | None = None

class DadosAlunosRecord(BaseModel):
    """Contrato lógico para a entidade de Dados Agregados do Indicador."""
    ano: int | None = None
    id_municipio: str | None = None
    id_escola: str | None = None
    id_aluno: str | None = None
    caderno: str | None = None
    serie: str | None = None
    rede: str | None = None
    presenca: str | None = None
    preenchimento_caderno: str | None = None
    alfabetizado: str | None = None #codigo 0/1 (não/sim) 
    proficiencia: float | None = None
    peso_aluno: float | None = None
