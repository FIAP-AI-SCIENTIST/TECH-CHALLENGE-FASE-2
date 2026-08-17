"""Transformações da camada Gold — modelo dimensional (Kimball) construído
a partir da Silver via DuckDB. Cada dimensão/fato é uma projeção,
deduplicação por chave mais recente ou derivação pura de tabelas Silver já
limpas — sem I/O, mesmo padrão de `silver.transform`.
"""

import hashlib

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


def surrogate_key(namespace: str, natural_key) -> int | None:
    """Surrogate key determinística: SHA-256 de "{namespace}|{chave natural}",
    primeiros 8 bytes big-endian com sinal → INT64.

    A Gold é reescrita por completo a cada execução (WRITE_TRUNCATE), então a
    SK não pode depender de ordem de leitura nem de sequencial — tem que ser
    função pura da chave natural, idêntica em qualquer máquina e versão. O
    namespace impede que a mesma string em dimensões diferentes ("2" como rede
    e como série) colida semanticamente. Chave NULL → SK NULL.
    """
    if natural_key is None:
        return None
    digest = hashlib.sha256(f"{namespace}|{natural_key}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


# Mapa coluna SK → (namespace, coluna da chave natural). Composições por fato
# ficam em `_SK_POR_FATO`, mais abaixo.
SK_UF = {"sk_uf": ("uf", "sigla_uf")}
SK_MUNICIPIO = {"sk_municipio": ("municipio", "id_municipio")}
SK_REDE = {"sk_rede": ("rede", "rede")}
SK_SERIE = {"sk_serie": ("serie", "serie")}
SK_TEMPO = {"sk_tempo": ("tempo", "ano")}

# Chave natural da dimensão SCD2 de meta → mapa SK correspondente, usado por
# `build_fact_meta_resultado` para derivar as FKs de cada fato de meta.
_SK_POR_CHAVE_NATURAL: dict[str, dict[str, tuple[str, str]]] = {
    "sigla_uf": SK_UF,
    "id_municipio": SK_MUNICIPIO,
    "rede": SK_REDE,
}


def with_surrogate_keys(tabela: pa.Table, key_map: dict[str, tuple[str, str]]) -> pa.Table:
    """Acrescenta colunas `sk_*` derivadas das chaves naturais — aditiva,
    nunca altera nem remove coluna existente.

    O hash é cacheado por valor distinto da chave natural: a cardinalidade
    real é baixa (27 UFs, ~5,6 mil municípios, ~15 anos), então o custo é o
    de chaves distintas, não o de linhas — mesmo em `fact_alunos` (~4M).
    Tabela sem colunas (fonte ainda não processada) passa direto.
    """
    if not tabela.column_names:
        return tabela
    for sk_col, (namespace, natural_col) in key_map.items():
        if natural_col not in tabela.column_names:
            continue
        cache: dict = {}
        sks = []
        for valor in tabela.column(natural_col).to_pylist():
            if valor not in cache:
                cache[valor] = surrogate_key(namespace, valor)
            sks.append(cache[valor])
        tabela = tabela.append_column(sk_col, pa.array(sks, type=pa.int64()))
    return tabela


def _select_existing(tabela: pa.Table, colunas: list[str]) -> pa.Table:
    """Seleciona só as colunas de `colunas` que existem em `tabela` — a
    fonte pode não ter todas (ex: entidade ainda vazia na Silver)."""
    presentes = [c for c in colunas if c in tabela.column_names]
    return tabela.select(presentes)


def build_dim_uf(diretorio_uf: pa.Table) -> pa.Table:
    """Dimensão UF: sigla + nome completo, uma linha por `sigla_uf`
    (fonte: diretório oficial `br_bd_diretorios_brasil.uf`, 27 UFs).
    `sigla_uf` permanece como atributo consultável; `sk_uf` é a PK física."""
    if "sigla_uf" not in diretorio_uf.column_names or "nome" not in diretorio_uf.column_names:
        return with_surrogate_keys(
            pa.table({"sigla_uf": pa.array([], type=pa.string()), "nome": pa.array([], type=pa.string())}),
            SK_UF,
        ).select(["sk_uf", "sigla_uf", "nome"])

    conn = duckdb.connect(":memory:")
    conn.register("t", diretorio_uf)
    sql = "SELECT DISTINCT sigla_uf, nome FROM t WHERE sigla_uf IS NOT NULL ORDER BY sigla_uf"
    dim = conn.sql(sql).to_arrow_table()
    return with_surrogate_keys(dim, SK_UF).select(["sk_uf", "sigla_uf", "nome"])


def build_dim_municipio(diretorio_municipio: pa.Table) -> pa.Table:
    """Dimensão Município: uma linha por `id_municipio`
    (fonte: diretório oficial `br_bd_diretorios_brasil.municipio`,
    ~5.570 municípios IBGE). Sem `ROW_NUMBER` por ano — o diretório
    não tem `ano`; é referência territorial atual."""
    colunas = ["id_municipio", "nome", "sigla_uf", "nome_regiao", "capital_uf"]
    if "id_municipio" not in diretorio_municipio.column_names:
        vazio = {c: pa.array([], type=pa.string()) for c in colunas}
        # `capital_uf` é INT64 na fonte — o fallback vazio precisa do mesmo tipo,
        # senão a tabela criada num run sem diretório conflita com o do run seguinte.
        vazio["capital_uf"] = pa.array([], type=pa.int64())
        return with_surrogate_keys(pa.table(vazio), SK_MUNICIPIO).select(["sk_municipio"] + colunas)

    conn = duckdb.connect(":memory:")
    conn.register("t", _select_existing(diretorio_municipio, colunas))
    sql = """
        SELECT DISTINCT id_municipio, nome, sigla_uf, nome_regiao, capital_uf
        FROM t WHERE id_municipio IS NOT NULL ORDER BY id_municipio
    """
    dim = conn.sql(sql).to_arrow_table()
    return with_surrogate_keys(dim, SK_MUNICIPIO).select(["sk_municipio"] + colunas)


def _build_dim_codigo(tabelas: list[pa.Table], codigo_col: str, desc_col: str) -> pa.Table:
    """Dimensão de código genérica (rede/série): união das traduções já
    feitas na Silver, distinct por código (o mesmo dicionário vale para
    todas as entidades fonte). A SK usa o próprio código como chave natural
    (namespace = nome da coluna)."""
    sk_col = f"sk_{codigo_col}"
    key_map = {sk_col: (codigo_col, codigo_col)}
    partes = [
        _select_existing(t, [codigo_col, desc_col])
        for t in tabelas
        if codigo_col in t.column_names and desc_col in t.column_names
    ]
    if not partes:
        vazio = pa.table({codigo_col: pa.array([], type=pa.string()), desc_col: pa.array([], type=pa.string())})
        return with_surrogate_keys(vazio, key_map).select([sk_col, codigo_col, desc_col])

    unidas = pa.concat_tables(partes, promote_options="default")
    conn = duckdb.connect(":memory:")
    conn.register("t", unidas)
    sql = f"SELECT DISTINCT {codigo_col}, {desc_col} FROM t WHERE {codigo_col} IS NOT NULL"
    dim = conn.sql(sql).to_arrow_table()
    return with_surrogate_keys(dim, key_map).select([sk_col, codigo_col, desc_col])


def build_dim_rede(*tabelas: pa.Table) -> pa.Table:
    return _build_dim_codigo(list(tabelas), "rede", "rede_desc")


def build_dim_serie(*tabelas: pa.Table) -> pa.Table:
    return _build_dim_codigo(list(tabelas), "serie", "serie_desc")


_DIM_TEMPO_SCHEMA = pa.schema([
    pa.field("sk_tempo", pa.int64()),
    pa.field("ano", pa.int64()),
    pa.field("decada", pa.int64()),
    pa.field("ano_tem_meta", pa.bool_()),
    pa.field("anos_para_meta_final", pa.int64()),
])


def build_dim_tempo(anos: list[int]) -> pa.Table:
    """Dimensão de tempo no grão ano — a fonte é uma avaliação anual, então
    grão mensal/diário seria cerimônia sem função analítica.

    Cobertura: anos observados na Silver ∪ 2024–2030 (horizonte da trajetória
    de meta). A união com o horizonte garante por construção que todo
    `sk_tempo` referenciado por qualquer fato existe na dimensão — inclusive o
    dos anos de meta futura que a trajetória SCD2 referencia. `ano_tem_meta`
    e `anos_para_meta_final` respondem "este ano tem meta definida" e "falta
    quanto para 2030" sem join.
    """
    anos_unicos = sorted({a for a in anos if a is not None} | set(META_ANOS))
    rows = [
        {
            "sk_tempo": surrogate_key("tempo", ano),
            "ano": ano,
            "decada": ano - ano % 10,
            "ano_tem_meta": ano in META_ANOS,
            "anos_para_meta_final": 2030 - ano,
        }
        for ano in anos_unicos
    ]
    return pa.Table.from_pylist(rows, schema=_DIM_TEMPO_SCHEMA)


def build_fact_indicador_uf(uf_table: pa.Table) -> pa.Table:
    """Fato no grão (ano, sigla_uf, serie, rede) — indicador de
    alfabetização e proficiência por UF. Chaves naturais preservadas como
    atributos consultáveis; as `sk_*` são as FKs físicas."""
    fato = _select_existing(uf_table, ["ano", "sigla_uf", "serie", "rede"] + FACT_UF_MEASURES)
    return with_surrogate_keys(fato, {**SK_UF, **SK_SERIE, **SK_REDE, **SK_TEMPO})


def build_fact_indicador_municipio(municipio_table: pa.Table) -> pa.Table:
    """Fato no grão (ano, id_municipio, serie, rede) — indicador de
    alfabetização e proficiência por município."""
    fato = _select_existing(municipio_table, ["ano", "id_municipio", "serie", "rede"] + FACT_UF_MEASURES)
    return with_surrogate_keys(fato, {**SK_MUNICIPIO, **SK_SERIE, **SK_REDE, **SK_TEMPO})


def build_fact_alunos(alunos_table: pa.Table) -> pa.Table:
    """Fato no grão do aluno — base para treinamento de modelos (feature
    mais granular disponível: proficiência individual, presença, etc.)."""
    fato = _select_existing(alunos_table, FACT_ALUNOS_COLUMNS)
    return with_surrogate_keys(fato, {**SK_MUNICIPIO, **SK_SERIE, **SK_REDE, **SK_TEMPO})


def build_fact_alfabetizacao_municipio(integrada: pa.Table) -> pa.Table:
    """Fato integrado no grão (ano, id_municipio, rede, serie): meta e
    resultado observado na mesma linha, vindos de **entidades distintas da
    fonte** — a meta veio de `meta_alfabetizacao_municipio` e o resultado de
    `municipio`, já cruzados pelo JOIN temporal da tabela integrada da Silver.

    `gap_pontos`/`atingiu_meta` derivam do `meta_indicador` da mesma linha
    (NULL quando o ano está fora do horizonte 2024–2030 ou o município não
    tem meta vigente — ausência de meta é achado analítico, não zero).
    """
    if not integrada.column_names:
        return integrada

    grao = [c for c in ("ano", "id_municipio", "rede", "serie") if c in integrada.column_names]
    medidas = [c for c in FACT_UF_MEASURES if c in integrada.column_names]
    extras = [c for c in ("meta_indicador", "percentual_participacao", "nivel_alfabetizacao") if c in integrada.column_names]
    derivadas = []
    if "taxa_alfabetizacao" in integrada.column_names and "meta_indicador" in integrada.column_names:
        # Em SQL, comparação/subtração com NULL propagam NULL — sem CASE especial.
        derivadas = [
            "taxa_alfabetizacao - meta_indicador AS gap_pontos",
            "taxa_alfabetizacao >= meta_indicador AS atingiu_meta",
        ]

    conn = duckdb.connect(":memory:")
    conn.register("t", integrada)
    fato = conn.sql(f"SELECT {', '.join(grao + medidas + extras + derivadas)} FROM t").to_arrow_table()
    return with_surrogate_keys(fato, {**SK_TEMPO, **SK_MUNICIPIO, **SK_REDE, **SK_SERIE})


def build_fact_meta_resultado(scd2_table: pa.Table, chave_cols: list[str]) -> pa.Table:
    """Compara meta x resultado: para cada linha da dimensão SCD2 de meta,
    usa o `ano`/`taxa_alfabetizacao` da própria linha (resultado observado
    naquele ano) contra `meta_alfabetizacao_{ano}` da mesma linha (alvo
    definido especificamente para aquele ano na trajetória vigente).

    Todo ano presente na fonte abre pelo menos uma versão na dimensão SCD2,
    porque o versionamento rastreia o resultado observado
    (`taxa_alfabetizacao`) e não apenas a trajetória de metas — dois anos
    consecutivos só colapsam numa única versão se o resultado *e* a
    trajetória *e* a participação forem idênticos, o que na prática não
    ocorre. A série anual deste fato é, portanto, completa.
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
    fato = conn.sql(sql).to_arrow_table()
    sk_map = {k: v for c in chave_cols for k, v in _SK_POR_CHAVE_NATURAL[c].items()}
    return with_surrogate_keys(fato, {**sk_map, **SK_TEMPO})
