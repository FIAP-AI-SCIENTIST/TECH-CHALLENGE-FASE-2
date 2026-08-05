from typing import Optional

from pydantic import BaseModel

class UFRecord(BaseModel):
    """Contrato lógico para a entidade UF."""
    ano: Optional[int] = None
    sigla_uf: Optional[str] = None
    serie: Optional[str] = None
    rede: Optional[str] = None
    taxa_alfabetizacao: Optional[float] = None
    media_portugues: Optional[float] = None
    proporcao_aluno_nivel_0: Optional[float] = None
    proporcao_aluno_nivel_1: Optional[float] = None
    proporcao_aluno_nivel_2: Optional[float] = None
    proporcao_aluno_nivel_3: Optional[float] = None
    proporcao_aluno_nivel_4: Optional[float] = None
    proporcao_aluno_nivel_5: Optional[float] = None
    proporcao_aluno_nivel_6: Optional[float] = None
    proporcao_aluno_nivel_7: Optional[float] = None
    proporcao_aluno_nivel_8: Optional[float] = None
    
class MunicipioRecord(BaseModel):
    """Contrato lógico para a entidade Municipio."""
    ano: Optional[int] = None
    id_municipio: Optional[str] = None
    serie: Optional[str] = None
    rede: Optional[str] = None
    taxa_alfabetizacao: Optional[float] = None
    media_portugues: Optional[float] = None
    proporcao_aluno_nivel_0: Optional[float] = None
    proporcao_aluno_nivel_1: Optional[float] = None
    proporcao_aluno_nivel_2: Optional[float] = None
    proporcao_aluno_nivel_3: Optional[float] = None
    proporcao_aluno_nivel_4: Optional[float] = None
    proporcao_aluno_nivel_5: Optional[float] = None
    proporcao_aluno_nivel_6: Optional[float] = None
    proporcao_aluno_nivel_7: Optional[float] = None
    proporcao_aluno_nivel_8: Optional[float] = None

class _MetaAlfabetizacaoBaseRecord(BaseModel):
    """Campos comuns às 3 tabelas de meta (Brasil/UF/Municipio). Não é uma tabela própria."""
    ano: Optional[int] = None
    rede: Optional[str] = None
    taxa_alfabetizacao: Optional[float] = None
    meta_alfabetizacao_2024: Optional[float] = None
    meta_alfabetizacao_2025: Optional[float] = None
    meta_alfabetizacao_2026: Optional[float] = None
    meta_alfabetizacao_2027: Optional[float] = None
    meta_alfabetizacao_2028: Optional[float] = None
    meta_alfabetizacao_2029: Optional[float] = None
    meta_alfabetizacao_2030: Optional[float] = None
    percentual_participacao: Optional[float] = None

class MetaAlfabetizacaoBrasilRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao Brasil."""
    pass

class MetaAlfabetizacaoUFRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao por UF."""
    sigla_uf: Optional[str] = None

class MetaAlfabetizacaoMunicipioRecord(_MetaAlfabetizacaoBaseRecord):
    """Contrato lógico para a entidade Meta Alfabetizacao por Municipio."""
    id_municipio: Optional[str] = None
    nivel_alfabetizacao: Optional[int] = None

class DadosAlunosRecord(BaseModel):
    """Contrato lógico para a entidade de Dados Agregados do Indicador."""
    ano: Optional[int] = None
    id_municipio: Optional[str] = None
    id_escola: Optional[str] = None
    id_aluno: Optional[str] = None
    caderno: Optional[str] = None
    serie: Optional[str] = None
    rede: Optional[str] = None
    presenca: Optional[str] = None
    preenchimento_caderno: Optional[str] = None
    alfabetizado: Optional[str] = None #codigo 0/1 (não/sim) 
    proficiencia: Optional[float] = None
    peso_aluno: Optional[float] = None
