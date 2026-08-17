"""Transformações da camada Silver — normalização de chave, tradução de código,
deduplicação e SCD Tipo 2.
"""

from collections import defaultdict

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

# Entidades regulares: Silver é dona de `ano=`, reescreve por ano.
ENTIDADES_REGULARES = {"uf", "municipio", "alunos"}
# Entidades de meta: versionadas via SCD2, tabela cumulativa sem partição por ano.
ENTIDADES_META = {
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
}

# Chave de deduplicação por entidade — resolve duplicata
# do at-least-once do streaming.
DEDUPE_KEYS: dict[str, list[str]] = {
    "uf": ["ano", "sigla_uf", "serie", "rede"],
    "municipio": ["ano", "id_municipio", "serie", "rede"],
    "alunos": ["ano", "id_aluno"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "rede"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "rede"],
    # A integrada não passa por `dedupe` (nasce de fontes já deduplicadas); a
    # chave fica registrada aqui porque `quality/rules.py` a consome como
    # restrição de unicidade do grão.
    "alfabetizacao_municipio_integrado": ["ano", "id_municipio", "rede", "serie"],
}

# Tabela derivada que cruza indicador municipal x meta municipal (entidades
# distintas da fonte) — a integracao de fato, nao lookup de diretorio.
ENTIDADE_INTEGRADA = "alfabetizacao_municipio_integrado"

# Colunas herdadas do indicador municipal, na ordem de saída da integrada.
_COLUNAS_INDICADOR_INTEGRADA = [
    "ano",
    "id_municipio",
    "rede",
    "serie",
    "rede_desc",
    "serie_desc",
    "nome",
    "sigla_uf",
    "nome_regiao",
    "capital_uf",
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

# Chave natural do SCD2 (sem `ano` — é o que muda de versão).
SCD2_NATURAL_KEYS: dict[str, list[str]] = {
    "meta_alfabetizacao_brasil": ["rede"],
    "meta_alfabetizacao_uf": ["sigla_uf", "rede"],
    "meta_alfabetizacao_municipio": ["id_municipio", "rede"],
}

# Colunas rastreadas pelo SCD2 — mudança em qualquer uma gera nova versão.
#
# Inclui o resultado observado (`taxa_alfabetizacao`, `nivel_alfabetizacao`) e não
# apenas a trajetória de metas: quando só a trajetória era rastreada, um ano cujo
# alvo repetia o do ano anterior não abria versão nova, e `apply_scd2` mantinha a
# linha antiga inteira — descartando a taxa de alfabetização daquele ano junto.
# Como o resultado observado é exatamente o que os fatos de meta x resultado
# comparam, o efeito era um fato com valor defasado e sem linha para o ano.
#
# `nivel_alfabetizacao` só existe na meta municipal; nas outras duas entidades
# `rastreados()` lê `None` dos dois lados da comparação, então a coluna extra é
# inócua ali e não exige uma lista por entidade.
SCD2_TRACKED_COLUMNS = [
    "meta_alfabetizacao_2024",
    "meta_alfabetizacao_2025",
    "meta_alfabetizacao_2026",
    "meta_alfabetizacao_2027",
    "meta_alfabetizacao_2028",
    "meta_alfabetizacao_2029",
    "meta_alfabetizacao_2030",
    "percentual_participacao",
    "taxa_alfabetizacao",
    "nivel_alfabetizacao",
]

_ALUNOS_BOOL_COLUMNS = ("alfabetizado", "presenca", "preenchimento_caderno")


def normalize_key(raw_id_municipio: str | None) -> str | None:
    """Normaliza `id_municipio` para 7 dígitos IBGE.

    Retorna `None` se a entrada for vazia ou se, após o padding, não resultar
    em exatamente 7 dígitos numéricos — quem chama decide como tratar a
    rejeição (nunca descarte silencioso, sempre
    contabilizado).
    """
    if raw_id_municipio is None:
        return None
    valor = str(raw_id_municipio).strip()
    if not valor:
        return None
    valor = valor.zfill(7)
    if len(valor) != 7 or not valor.isdigit():
        return None
    return valor


def _dict_to_table(mapping: dict, key_col: str, value_col: str) -> pa.Table:
    if not mapping:
        return pa.table({key_col: pa.array([], type=pa.string()), value_col: pa.array([], type=pa.string())})
    return pa.table({key_col: list(mapping.keys()), value_col: list(mapping.values())})


def _municipio_dict_to_table(mapping: dict) -> pa.Table:
    if not mapping:
        return pa.table({
            "id_municipio": pa.array([], type=pa.string()),
            "nome": pa.array([], type=pa.string()),
            "sigla_uf": pa.array([], type=pa.string()),
            "nome_regiao": pa.array([], type=pa.string()),
            "capital_uf": pa.array([], type=pa.int64()),
        })
    ids = list(mapping.keys())
    return pa.table({
        "id_municipio": ids,
        "nome": [mapping[i]["nome"] for i in ids],
        "sigla_uf": [mapping[i]["sigla_uf"] for i in ids],
        "nome_regiao": [mapping[i]["nome_regiao"] for i in ids],
        "capital_uf": [mapping[i]["capital_uf"] for i in ids],
    })


def _cast_alunos_booleans(tabela: pa.Table) -> pa.Table:
    for col in _ALUNOS_BOOL_COLUMNS:
        if col in tabela.column_names:
            idx = tabela.column_names.index(col)
            valores = pc.cast(tabela.column(col), pa.string())
            bool_valores = pc.equal(valores, "1")
            tabela = tabela.set_column(idx, col, bool_valores)
    return tabela


def clean(entidade: str, tabela: pa.Table, referencias: dict) -> tuple[pa.Table, int]:
    """Traduz código, normaliza chave e padroniza tipos via DuckDB.

    `referencias` é o dict de mapas de tradução (chaves possíveis: "rede",
    "serie", "diretorio_uf", "diretorio_municipio" — cada entidade usa só o
    que precisa).

    Retorna (tabela limpa, nº de linhas rejeitadas por `id_municipio`
    inválido — nunca descartadas silenciosamente, contabilizadas aqui pra o
    caller logar com `WARNING`).
    """
    if tabela.num_rows == 0:
        return tabela, 0

    conn = duckdb.connect(":memory:")
    conn.create_function(
        "normalize_key",
        normalize_key,
        null_handling="special",
    )
    conn.register("bronze", tabela)

    columns = tabela.column_names
    select_parts = []
    join_parts = []

    for col in columns:
        if col == "id_municipio":
            select_parts.append("normalize_key(bronze.id_municipio) AS id_municipio")
        else:
            select_parts.append(f"bronze.{col}")

    if "rede" in columns and "rede" in referencias:
        conn.register("rede_dict", _dict_to_table(referencias["rede"], "chave", "rede_desc"))
        select_parts.append("rede_dict.rede_desc AS rede_desc")
        join_parts.append("LEFT JOIN rede_dict ON CAST(bronze.rede AS VARCHAR) = rede_dict.chave")

    if "serie" in columns and "serie" in referencias:
        conn.register("serie_dict", _dict_to_table(referencias["serie"], "chave", "serie_desc"))
        select_parts.append("serie_dict.serie_desc AS serie_desc")
        join_parts.append("LEFT JOIN serie_dict ON CAST(bronze.serie AS VARCHAR) = serie_dict.chave")

    if entidade in ("uf", "meta_alfabetizacao_uf") and "diretorio_uf" in referencias:
        conn.register("uf_dict", _dict_to_table(referencias["diretorio_uf"], "sigla", "sigla_uf_nome"))
        select_parts.append("uf_dict.sigla_uf_nome AS sigla_uf_nome")
        join_parts.append("LEFT JOIN uf_dict ON bronze.sigla_uf = uf_dict.sigla")

    if entidade in ("municipio", "meta_alfabetizacao_municipio") and "diretorio_municipio" in referencias:
        conn.register("municipio_dict", _municipio_dict_to_table(referencias["diretorio_municipio"]))
        select_parts.extend([
            "municipio_dict.nome AS nome",
            "municipio_dict.sigla_uf AS sigla_uf",
            "municipio_dict.nome_regiao AS nome_regiao",
            "municipio_dict.capital_uf AS capital_uf",
        ])
        join_parts.append(
            "LEFT JOIN municipio_dict ON normalize_key(bronze.id_municipio) = municipio_dict.id_municipio"
        )

    sql = f"SELECT {', '.join(select_parts)} FROM bronze {' '.join(join_parts)}"
    limpa = conn.sql(sql).to_arrow_table()

    if entidade == "alunos":
        limpa = _cast_alunos_booleans(limpa)

    rejeitadas = 0
    if "id_municipio" in limpa.column_names:
        null_mask = pc.is_null(limpa.column("id_municipio"))
        rejeitadas = pc.sum(pc.cast(null_mask, pa.int64())).as_py() or 0
        if rejeitadas:
            limpa = limpa.filter(pc.invert(null_mask))

    return limpa, rejeitadas


def dedupe(entidade: str, tabela: pa.Table) -> pa.Table:
    """Remove duplicatas pela chave natural da entidade — resolve reentrega
    at-least-once do streaming. Em empate, mantém a última ocorrência lida.
    """
    if tabela.num_rows == 0:
        return tabela

    chave = DEDUPE_KEYS[entidade]
    idx_col = pa.array(range(tabela.num_rows), type=pa.int64())
    tabela_indexada = tabela.append_column("_idx", idx_col)

    conn = duckdb.connect(":memory:")
    conn.register("t", tabela_indexada)
    partition = ", ".join(chave)
    sql = f"""
        SELECT * EXCLUDE (_rn, _idx) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY _idx DESC) AS _rn
            FROM t
        )
        WHERE _rn = 1
    """
    return conn.sql(sql).to_arrow_table()


def group_by_ano(tabela: pa.Table) -> dict[int, pa.Table]:
    """Reagrupa uma tabela Silver limpa por `ano` — necessário porque
    `bronze.reader.read_partition` devolve todos os anos concatenados (batch
    `ano=` + streaming `data_ingestao=`, já unidos pelo reader da Bronze), e a
    Silver é dona de `ano=` na escrita. Só usada
    pelas 3 entidades regulares.
    """
    if tabela.num_rows == 0:
        return {}

    anos = tabela.column("ano").to_pylist()
    indices_por_ano: dict[int, list[int]] = defaultdict(list)
    for idx, ano in enumerate(anos):
        indices_por_ano[ano].append(idx)

    return {ano: tabela.take(indices) for ano, indices in indices_por_ano.items()}


def _with_scd2_columns(schema: pa.Schema) -> pa.Schema:
    base_fields = [f for f in schema if f.name not in ("valid_from", "valid_to", "is_current")]
    extra_fields = [
        pa.field("valid_from", pa.int64()),
        pa.field("valid_to", pa.int64()),
        pa.field("is_current", pa.bool_()),
    ]
    return pa.schema(base_fields + extra_fields)


def apply_scd2(entidade: str, dimension_atual: pa.Table, incoming: pa.Table, ano: int) -> pa.Table:
    """Aplica SCD Tipo 2 — abre nova versão só quando os valores rastreados
    (`SCD2_TRACKED_COLUMNS`) mudam em relação à última versão vigente da
    mesma chave natural. `ano` é o ano de
    referência desta chamada, usado como `valid_from`/`valid_to`.

    `dimension_atual` é a cadeia de versões acumulada pelos anos anteriores
    **desta mesma execução**, nunca o estado persistido no GCS:
    `silver.pipeline.run_silver` parte de tabela vazia e replaya os anos do
    Bronze em ordem cronológica, o que faz da tabela SCD2 uma função
    determinística do Bronze — duas execuções sobre o mesmo Bronze produzem
    a mesma saída, e nenhuma versão é fechada antes de abrir.
    """
    chave_cols = SCD2_NATURAL_KEYS[entidade]
    schema_final = _with_scd2_columns(incoming.schema)

    def chave(row: dict) -> tuple:
        return tuple(row.get(c) for c in chave_cols)

    def rastreados(row: dict) -> tuple:
        return tuple(row.get(c) for c in SCD2_TRACKED_COLUMNS)

    dim_rows = dimension_atual.to_pylist() if dimension_atual.num_rows else []
    inc_rows = incoming.to_pylist() if incoming.num_rows else []

    atuais_por_chave = {chave(row): row for row in dim_rows if row.get("is_current")}
    resultado = [row for row in dim_rows if not row.get("is_current")]  # históricas passam direto

    chaves_vistas = set()
    for row in inc_rows:
        k = chave(row)
        chaves_vistas.add(k)
        atual = atuais_por_chave.get(k)

        if atual is None:
            nova = dict(row)
            nova["valid_from"] = ano
            nova["valid_to"] = None
            nova["is_current"] = True
            resultado.append(nova)
        elif rastreados(atual) == rastreados(row):
            resultado.append(atual)  # sem mudança — mantém a versão vigente
        else:
            fechada = dict(atual)
            fechada["valid_to"] = ano
            fechada["is_current"] = False
            resultado.append(fechada)

            nova = dict(row)
            nova["valid_from"] = ano
            nova["valid_to"] = None
            nova["is_current"] = True
            resultado.append(nova)

    # Chaves vigentes que sumiram da fonte nesta execução — mantém como estava.
    for k, atual in atuais_por_chave.items():
        if k not in chaves_vistas:
            resultado.append(atual)

    if not resultado:
        return pa.Table.from_pylist([], schema=schema_final)
    return pa.Table.from_pylist(resultado, schema=schema_final)


def _meta_column_expr(meta_columns: list[str], col: str, tipo: str) -> str:
    """Projeta `m.<col>` quando existe na SCD2, senão um NULL tipado — mantém o
    schema da integrada estável mesmo se a fonte de meta vier sem a coluna."""
    if col in meta_columns:
        return f"m.{col}"
    return f"CAST(NULL AS {tipo})"


def integrate_alfabetizacao_municipio(municipio: pa.Table, meta_scd2: pa.Table | None) -> pa.Table:
    """Integra indicador municipal x meta municipal — JOIN real entre duas
    entidades distintas da fonte (nao lookup de diretorio).

    Grão de saída: `(ano, id_municipio, rede, serie)` (o do indicador). A meta,
    de grão mais grosso `(ano, id_municipio, rede)`, é localizada pelo JOIN
    temporal sobre a cadeia SCD2 — a versão vigente no ano do indicador
    (`valid_from <= ano < valid_to`) — e sofre broadcast para as séries do
    mesmo `(ano, id_municipio, rede)`.

    LEFT JOIN a partir do indicador: município com resultado mas sem meta
    vigente permanece, com as colunas de meta NULL. `meta_indicador` é a meta
    do próprio ano da linha (`meta_alfabetizacao_{ano}`), NULL fora do
    horizonte 2024-2030 (anos sem coluna de meta correspondente).
    """
    if not municipio.column_names or municipio.num_rows == 0:
        return pa.Table.from_pydict({})

    select_parts = [f"i.{c}" for c in _COLUNAS_INDICADOR_INTEGRADA if c in municipio.column_names]
    meta_columns = meta_scd2.column_names if meta_scd2 is not None else []

    anos_meta = sorted(
        int(c.rsplit("_", 1)[1]) for c in meta_columns if c.startswith("meta_alfabetizacao_")
    )
    if anos_meta and meta_columns:
        case_expr = " ".join(f"WHEN {ano} THEN m.meta_alfabetizacao_{ano}" for ano in anos_meta)
        select_parts.append(f"(CASE i.ano {case_expr} END) AS meta_indicador")
    else:
        select_parts.append("CAST(NULL AS DOUBLE) AS meta_indicador")
    select_parts.append(_meta_column_expr(meta_columns, "percentual_participacao", "DOUBLE") + " AS percentual_participacao")
    select_parts.append(_meta_column_expr(meta_columns, "nivel_alfabetizacao", "BIGINT") + " AS nivel_alfabetizacao")

    conn = duckdb.connect(":memory:")
    conn.register("indicador", municipio)

    if meta_scd2 is None or meta_scd2.num_rows == 0:
        # Sem cadeia SCD2 gravada, o JOIN é contra relação vazia — mesmo
        # resultado (tudo NULL nas colunas de meta), sem SQL especial.
        meta_scd2 = pa.table({
            "id_municipio": pa.array([], type=pa.string()),
            "rede": pa.array([], type=pa.string()),
            "valid_from": pa.array([], type=pa.int64()),
            "valid_to": pa.array([], type=pa.int64()),
        })
    conn.register("meta", meta_scd2)

    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM indicador i
        LEFT JOIN meta m
          ON i.id_municipio = m.id_municipio
         AND i.rede = m.rede
         AND i.ano >= m.valid_from
         AND (m.valid_to IS NULL OR i.ano < m.valid_to)
    """
    return conn.sql(sql).to_arrow_table()
