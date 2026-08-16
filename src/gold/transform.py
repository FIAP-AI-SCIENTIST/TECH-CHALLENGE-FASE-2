"""Transformações da camada Gold — modelo dimensional (Kimball) construído
a partir da Silver via DuckDB. Cada dimensão/fato é uma projeção,
deduplicação por chave mais recente ou derivação pura de tabelas Silver já
limpas — sem I/O, mesmo padrão de `silver.transform`.
"""

import duckdb
import pyarrow as pa

FACT_UF_MEASURES = [
    "taxa_alfabetizacao",
    "media_portugues",
    "proporcao_aluno_nivel_0",
    "proporcao_aluno_nivel_1",
    "proporcao_aluno_nivel_2",
    "proporcao_aluno_nivel_3",
    "proporcao_aluno_nivel_4",
    "proporcao_aluno_nivel_5",
    "proporcao_aluno_nivel_6",
    "proporcao_aluno_nivel_7",
    "proporcao_aluno_nivel_8",
]

FACT_ALUNOS_COLUMNS = [
    "ano",
    "id_municipio",
    "id_escola",
    "id_aluno",
    "caderno",
    "serie",
    "rede",
    "presenca",
    "preenchimento_caderno",
    "alfabetizado",
    "proficiencia",
    "peso_aluno",
]

# Anos com meta definida no contrato (meta_alfabetizacao_2024..2030) —
# usado para escolher, por linha, o alvo referente ao próprio `ano` da linha.
META_ANOS = list(range(2024, 2031))


def _select_existing(tabela: pa.Table, colunas: list[str]) -> pa.Table:
    """Seleciona só as colunas de `colunas` que existem em `tabela` — a
    fonte pode não ter todas (ex: entidade ainda vazia na Silver)."""
    presentes = [c for c in colunas if c in tabela.column_names]
    return tabela.select(presentes)


def build_dim_uf(diretorio_uf: pa.Table) -> pa.Table:
    """Dimensão UF: sigla + nome completo, uma linha por `sigla_uf`
    (fonte: diretório oficial `br_bd_diretorios_brasil.uf`, 27 UFs)."""
    if "sigla_uf" not in diretorio_uf.column_names or "nome" not in diretorio_uf.column_names:
        return pa.table({"sigla_uf": pa.array([], type=pa.string()), "nome": pa.array([], type=pa.string())})

    conn = duckdb.connect(":memory:")
    conn.register("t", diretorio_uf)
    sql = "SELECT DISTINCT sigla_uf, nome FROM t WHERE sigla_uf IS NOT NULL ORDER BY sigla_uf"
    return conn.sql(sql).to_arrow_table()


def build_dim_municipio(diretorio_municipio: pa.Table) -> pa.Table:
    """Dimensão Município: uma linha por `id_municipio`
    (fonte: diretório oficial `br_bd_diretorios_brasil.municipio`,
    ~5.570 municípios IBGE). Sem `ROW_NUMBER` por ano — o diretório
    não tem `ano`; é referência territorial atual."""
    colunas = ["id_municipio", "nome", "sigla_uf", "nome_regiao", "capital_uf"]
    if "id_municipio" not in diretorio_municipio.column_names:
        return pa.table({c: pa.array([], type=pa.string()) for c in colunas})

    conn = duckdb.connect(":memory:")
    conn.register("t", _select_existing(diretorio_municipio, colunas))
    sql = """
        SELECT DISTINCT id_municipio, nome, sigla_uf, nome_regiao, capital_uf
        FROM t WHERE id_municipio IS NOT NULL ORDER BY id_municipio
    """
    return conn.sql(sql).to_arrow_table()


def _build_dim_codigo(tabelas: list[pa.Table], codigo_col: str, desc_col: str) -> pa.Table:
    """Dimensão de código genérica (rede/série): união das traduções já
    feitas na Silver, distinct por código (o mesmo dicionário vale para
    todas as entidades fonte)."""
    partes = [
        _select_existing(t, [codigo_col, desc_col])
        for t in tabelas
        if codigo_col in t.column_names and desc_col in t.column_names
    ]
    if not partes:
        return pa.table({codigo_col: pa.array([], type=pa.string()), desc_col: pa.array([], type=pa.string())})

    unidas = pa.concat_tables(partes, promote_options="default")
    conn = duckdb.connect(":memory:")
    conn.register("t", unidas)
    sql = f"SELECT DISTINCT {codigo_col}, {desc_col} FROM t WHERE {codigo_col} IS NOT NULL"
    return conn.sql(sql).to_arrow_table()


def build_dim_rede(*tabelas: pa.Table) -> pa.Table:
    return _build_dim_codigo(list(tabelas), "rede", "rede_desc")


def build_dim_serie(*tabelas: pa.Table) -> pa.Table:
    return _build_dim_codigo(list(tabelas), "serie", "serie_desc")


def build_fact_indicador_uf(uf_table: pa.Table) -> pa.Table:
    """Fato no grão (ano, sigla_uf, serie, rede) — indicador de
    alfabetização e proficiência por UF."""
    return _select_existing(uf_table, ["ano", "sigla_uf", "serie", "rede"] + FACT_UF_MEASURES)


def build_fact_indicador_municipio(municipio_table: pa.Table) -> pa.Table:
    """Fato no grão (ano, id_municipio, serie, rede) — indicador de
    alfabetização e proficiência por município."""
    return _select_existing(municipio_table, ["ano", "id_municipio", "serie", "rede"] + FACT_UF_MEASURES)


def build_fact_alunos(alunos_table: pa.Table) -> pa.Table:
    """Fato no grão do aluno — base para treinamento de modelos (feature
    mais granular disponível: proficiência individual, presença, etc.)."""
    return _select_existing(alunos_table, FACT_ALUNOS_COLUMNS)


def build_fact_meta_resultado(scd2_table: pa.Table, chave_cols: list[str]) -> pa.Table:
    """Compara meta x resultado: para cada linha da dimensão SCD2 de meta,
    usa o `ano`/`taxa_alfabetizacao` da própria linha (resultado observado
    naquele ano) contra `meta_alfabetizacao_{ano}` da mesma linha (alvo
    definido especificamente para aquele ano na trajetória vigente).

    Assume que todo ano da fonte abre pelo menos uma versão SCD2 — verdade
    na prática, porque `percentual_participacao` varia ano a ano (ver
    trade-off documentado no README: se a trajetória E a participação
    ficarem idênticas de um ano para o outro, aquele ano específico não
    aparece isolado aqui, herda a versão anterior).
    """
    conn = duckdb.connect(":memory:")
    conn.register("t", scd2_table)
    case_expr = " ".join(f"WHEN {ano} THEN meta_alfabetizacao_{ano}" for ano in META_ANOS)
    chave_sql = ", ".join(chave_cols)
    sql = f"""
        SELECT
            {chave_sql},
            ano,
            taxa_alfabetizacao,
            CASE ano {case_expr} END AS meta_indicador,
            taxa_alfabetizacao - (CASE ano {case_expr} END) AS gap_pontos,
            taxa_alfabetizacao >= (CASE ano {case_expr} END) AS atingiu_meta,
            percentual_participacao,
            valid_from,
            valid_to,
            is_current
        FROM t
    """
    return conn.sql(sql).to_arrow_table()
