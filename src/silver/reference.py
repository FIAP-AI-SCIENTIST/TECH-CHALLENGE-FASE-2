"""Extração das tabelas de referência do BigQuery público (`dicionario`,
diretórios de UF/município) usadas pela Silver para traduzir códigos e
enriquecer com dados territoriais.

Extraídas sob demanda no início de cada run da Silver — não passam pela
Bronze (são metadado de tradução, não dado bruto do domínio) nem são
versionadas no repositório (Q3 do NFR Requirements/Functional Design).
"""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

from google.cloud import bigquery

from common.retry import with_retry

PROJECT_ID = "useful-space-277919"
DICIONARIO_TABLE = "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario"
DIRETORIO_UF_TABLE = "basedosdados.br_bd_diretorios_brasil.uf"
DIRETORIO_MUNICIPIO_TABLE = "basedosdados.br_bd_diretorios_brasil.municipio"
TIMEOUT_SECONDS = 10


@with_retry()
def _do_query(client: bigquery.Client, sql: str):
    """Operação atômica: roda a query e materializa o RowIterator (com timeout)."""
    return client.query(sql).result(timeout=TIMEOUT_SECONDS)


def get_dicionario(id_tabela: str, nome_coluna: str) -> dict[str, str]:
    """Mapa chave -> valor de tradução de código para (id_tabela, nome_coluna).

    Ex: get_dicionario("uf", "rede") -> {"1": "Federal", "2": "Estadual", ...}
    """
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT chave, valor
        FROM `{DICIONARIO_TABLE}`
        WHERE id_tabela = '{id_tabela}' AND nome_coluna = '{nome_coluna}'
    """
    rows = _do_query(client, sql)
    return {row["chave"]: row["valor"] for row in rows}


def get_diretorio_uf() -> dict[str, str]:
    """Mapa sigla -> nome completo da UF."""
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"SELECT DISTINCT sigla, nome FROM `{DIRETORIO_UF_TABLE}`"
    rows = _do_query(client, sql)
    return {row["sigla"]: row["nome"] for row in rows}


def get_diretorio_municipio() -> dict[str, dict]:
    """Mapa id_municipio -> {nome, sigla_uf, nome_regiao, capital_uf}.

    Subconjunto de colunas já decidido em `source-schemas.md` (Functional
    Design da U6) — o resto do diretório (hierarquia de saúde, geocódigos
    alternativos, geometria) fica fora do MVP.
    """
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT id_municipio, nome, sigla_uf, nome_regiao, capital_uf
        FROM `{DIRETORIO_MUNICIPIO_TABLE}`
    """
    rows = _do_query(client, sql)
    return {
        row["id_municipio"]: {
            "nome": row["nome"],
            "sigla_uf": row["sigla_uf"],
            "nome_regiao": row["nome_regiao"],
            "capital_uf": row["capital_uf"],
        }
        for row in rows
    }
