from hypothesis import strategies as st

from contracts.models import (
    UFRecord,
    MunicipioRecord,
    MetaAlfabetizacaoBrasilRecord,
    MetaAlfabetizacaoUFRecord,
    MetaAlfabetizacaoMunicipioRecord,
    DadosAlunosRecord
)

_st_ano = st.one_of(st.none(), st.integers(min_value=2010, max_value=2030))

_st_sigla_uf = st.one_of(
    st.none(),
    st.sampled_from([
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
        "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
        "RS", "RO", "RR", "SC", "SP", "SE", "TO"
    ])
)

_st_serie = st.one_of(st.none(), st.just("2"))

_st_rede = st.one_of(st.none(), st.sampled_from(["0", "1", "2", "3", "4", "5", "6"]))

_st_float = st.one_of(st.none(), st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False))

_st_id = st.one_of(st.none(), st.from_regex(r"^\d{1,7}$"))

_st_caderno = st.one_of(st.none(), st.text(min_size=1, max_size=5))

_st_presenca = st.one_of(st.none(), st.sampled_from(["0", "1"]))

_st_nivel_alf = st.one_of(st.none(), st.sampled_from([0, 1]))

@st.composite
def st_uf_record(draw):
    return UFRecord(
        ano=draw(_st_ano),
        sigla_uf=draw(_st_sigla_uf),
        serie=draw(_st_serie),
        rede=draw(_st_rede),
        taxa_alfabetizacao=draw(_st_float),
        media_portugues=draw(_st_float),
        proporcao_aluno_nivel_0=draw(_st_float),
        proporcao_aluno_nivel_1=draw(_st_float),
        proporcao_aluno_nivel_2=draw(_st_float),
        proporcao_aluno_nivel_3=draw(_st_float),
        proporcao_aluno_nivel_4=draw(_st_float),
        proporcao_aluno_nivel_5=draw(_st_float),
        proporcao_aluno_nivel_6=draw(_st_float),
        proporcao_aluno_nivel_7=draw(_st_float),
        proporcao_aluno_nivel_8=draw(_st_float),
    )

@st.composite
def st_municipio_record(draw):
    return MunicipioRecord(
        ano=draw(_st_ano),
        id_municipio=draw(_st_id),
        serie=draw(_st_serie),
        rede=draw(_st_rede),
        taxa_alfabetizacao=draw(_st_float),
        media_portugues=draw(_st_float),
        proporcao_aluno_nivel_0=draw(_st_float),
        proporcao_aluno_nivel_1=draw(_st_float),
        proporcao_aluno_nivel_2=draw(_st_float),
        proporcao_aluno_nivel_3=draw(_st_float),
        proporcao_aluno_nivel_4=draw(_st_float),
        proporcao_aluno_nivel_5=draw(_st_float),
        proporcao_aluno_nivel_6=draw(_st_float),
        proporcao_aluno_nivel_7=draw(_st_float),
        proporcao_aluno_nivel_8=draw(_st_float),
    )

@st.composite
def st_meta_alfabetizacao_brasil_record(draw):
    return MetaAlfabetizacaoBrasilRecord(
        ano=draw(_st_ano),
        rede=draw(_st_rede),
        taxa_alfabetizacao=draw(_st_float),
        meta_alfabetizacao_2024=draw(_st_float),
        meta_alfabetizacao_2025=draw(_st_float),
        meta_alfabetizacao_2026=draw(_st_float),
        meta_alfabetizacao_2027=draw(_st_float),
        meta_alfabetizacao_2028=draw(_st_float),
        meta_alfabetizacao_2029=draw(_st_float),
        meta_alfabetizacao_2030=draw(_st_float),
        percentual_participacao=draw(_st_float),
    )

@st.composite
def st_meta_alfabetizacao_uf_record(draw):
    base = st_meta_alfabetizacao_brasil_record()
    return MetaAlfabetizacaoUFRecord(
        **base.model_dump(),
        sigla_uf=draw(_st_sigla_uf)
    )

@st.composite
def st_meta_alfabetizacao_municipio_record(draw):
    base = st_meta_alfabetizacao_brasil_record()
    return MetaAlfabetizacaoMunicipioRecord(
        **base.model_dump(),
        id_municipio=draw(_st_id),
        nivel_alfabetizacao=draw(_st_nivel_alf)
    )

@st.composite
def st_dados_alunos_record(draw):
    return DadosAlunosRecord(
        ano=draw(_st_ano),
        id_municipio=draw(_st_id),
        id_escola=draw(_st_id),
        id_aluno=draw(_st_id),
        caderno=draw(_st_caderno),
        serie=draw(_st_serie),
        rede=draw(_st_rede),
        presenca=draw(_st_presenca),
        preenchimento_caderno=draw(_st_presenca),
        alfabetizado=draw(_st_presenca),
        proficiencia=draw(_st_float),
        peso_aluno=draw(_st_float)
    )
