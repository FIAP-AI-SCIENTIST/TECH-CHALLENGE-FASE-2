"""Checks de qualidade de dados — funções puras sobre pyarrow.Table, sem
I/O, no mesmo padrão de `silver.transform` (fácil de testar em isolamento,
sem GCS/BigQuery). Cobrem as quatro regras pedidas para a pipeline:
duplicidade, valores ausentes, chave de relacionamento e consistência.
"""

from dataclasses import dataclass

import duckdb
import pyarrow as pa
import pyarrow.compute as pc


@dataclass
class QualityResult:
    check: str
    entidade: str
    passou: bool
    linhas_afetadas: int
    detalhe: str


def check_duplicates(entidade: str, tabela: pa.Table, chave: list[str]) -> QualityResult:
    """Verificação de duplicidade: nenhuma combinação de `chave` deve se
    repetir. A Silver já deduplica (`silver.transform.dedupe`) — este check
    é a evidência explícita, gravada, de que a garantia se sustenta.
    """
    if tabela.num_rows == 0 or not all(c in tabela.column_names for c in chave):
        return QualityResult("duplicidade", entidade, True, 0, "tabela vazia ou chave ausente")

    conn = duckdb.connect(":memory:")
    conn.register("t", tabela)
    partition = ", ".join(chave)
    sql = f"""
        SELECT COUNT(*) AS duplicadas FROM (
            SELECT {partition} FROM t GROUP BY {partition} HAVING COUNT(*) > 1
        )
    """
    duplicadas = conn.sql(sql).to_arrow_table().column("duplicadas")[0].as_py()
    passou = duplicadas == 0
    return QualityResult(
        "duplicidade",
        entidade,
        passou,
        duplicadas,
        f"{duplicadas} combinação(ões) de {chave} duplicada(s)" if duplicadas else "sem duplicatas",
    )


def check_missing_values(entidade: str, tabela: pa.Table, colunas_obrigatorias: list[str]) -> QualityResult:
    """Detecção de valores ausentes em colunas obrigatórias — chave de
    negócio ou medida crítica que não deveria existir nula.
    """
    if tabela.num_rows == 0:
        return QualityResult("valores_ausentes", entidade, True, 0, "tabela vazia")

    total_nulos = 0
    colunas_com_nulo = []
    for col in colunas_obrigatorias:
        if col not in tabela.column_names:
            continue
        nulos = pc.sum(pc.cast(pc.is_null(tabela.column(col)), pa.int64())).as_py() or 0
        if nulos:
            total_nulos += nulos
            colunas_com_nulo.append(f"{col}={nulos}")

    passou = total_nulos == 0
    return QualityResult(
        "valores_ausentes",
        entidade,
        passou,
        total_nulos,
        ", ".join(colunas_com_nulo) if colunas_com_nulo else "sem valores ausentes",
    )


def check_referential_integrity(
    entidade: str, tabela: pa.Table, coluna_fk: str, chaves_validas: set
) -> QualityResult:
    """Validação de chave de relacionamento: todo valor não nulo de
    `coluna_fk` deve existir em `chaves_validas` (ex: `sigla_uf` de
    `fact_indicador_uf` deve existir em `dim_uf`). Órfãos indicam falha de
    integração entre camadas/entidades.
    """
    if tabela.num_rows == 0 or coluna_fk not in tabela.column_names:
        return QualityResult("chave_relacionamento", entidade, True, 0, "tabela vazia ou coluna ausente")

    valores = tabela.column(coluna_fk).to_pylist()
    orfaos = sum(1 for v in valores if v is not None and v not in chaves_validas)
    passou = orfaos == 0
    return QualityResult(
        "chave_relacionamento",
        entidade,
        passou,
        orfaos,
        f"{orfaos} valor(es) de '{coluna_fk}' sem correspondência" if orfaos else "todas as chaves resolvidas",
    )


def check_value_range(
    entidade: str, tabela: pa.Table, coluna: str, minimo: float, maximo: float
) -> QualityResult:
    """Consistência de domínio: valores de `coluna` devem estar em
    [`minimo`, `maximo`] (ex: percentuais entre 0 e 100). Nulos não contam
    aqui — são responsabilidade de `check_missing_values`.
    """
    if tabela.num_rows == 0 or coluna not in tabela.column_names:
        return QualityResult("consistencia_faixa", entidade, True, 0, "tabela vazia ou coluna ausente")

    col = tabela.column(coluna)
    fora_da_faixa = (
        pc.sum(pc.cast(pc.or_(pc.less(col, minimo), pc.greater(col, maximo)), pa.int64())).as_py() or 0
    )
    passou = fora_da_faixa == 0
    return QualityResult(
        "consistencia_faixa",
        entidade,
        passou,
        fora_da_faixa,
        f"{fora_da_faixa} valor(es) de '{coluna}' fora de [{minimo}, {maximo}]"
        if fora_da_faixa
        else "todos os valores na faixa esperada",
    )
