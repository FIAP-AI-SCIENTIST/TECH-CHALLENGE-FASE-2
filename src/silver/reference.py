"""Extração das tabelas de referência do BigQuery público (`dicionario`,
diretórios de UF/município) usadas pela Silver para traduzir códigos e
enriquecer com dados territoriais.

Extraídas sob demanda no início de cada run da Silver — não passam pela
Bronze (são metadado de tradução, não dado bruto do domínio) nem são
versionadas no repositório.
"""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

from google.cloud import bigquery

from common.retry import with_retry

PROJECT_ID = "useful-space-277919"
DICIONARIO_TABLE = "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario"
DIRETORIO_UF_TABLE = "basedosdados.br_bd_diretorios_brasil.uf"
DIRETORIO_MUNICIPIO_TABLE = "basedosdados.br_bd_diretorios_brasil.municipio"
TIMEOUT_SECONDS = 10
# Cap de custo — mesmo padrão de extraction._do_query.
MAX_BYTES_BILLED = 10 * 2**30


@with_retry()
def _do_query(client: bigquery.Client, sql: str) -> tuple:
    """Operação atômica: roda a query (com cap de bytes) e materializa o RowIterator.

    Retorna ``(rows, total_bytes_processed)`` — o contador do job alimenta a
    auditoria de custo por execução.
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED))
    return job.result(timeout=TIMEOUT_SECONDS), job.total_bytes_processed


def get_dicionario(id_tabela: str, nome_coluna: str) -> tuple[dict[str, str], int | None]:
    """Mapa chave -> valor de tradução de código para (id_tabela, nome_coluna).

    Ex: get_dicionario("uf", "rede") -> ({"1": "Federal", "2": "Estadual", ...}, bytes)
    """
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT chave, valor
        FROM `{DICIONARIO_TABLE}`
        WHERE id_tabela = '{id_tabela}' AND nome_coluna = '{nome_coluna}'
    """
    rows, bytes_processed = _do_query(client, sql)
    return {row["chave"]: row["valor"] for row in rows}, bytes_processed


def get_diretorio_uf() -> tuple[dict[str, str], int | None]:
    """Mapa sigla -> nome completo da UF, + bytes processados pela consulta."""
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"SELECT DISTINCT sigla, nome FROM `{DIRETORIO_UF_TABLE}`"
    rows, bytes_processed = _do_query(client, sql)
    return {row["sigla"]: row["nome"] for row in rows}, bytes_processed


def get_diretorio_municipio() -> tuple[dict[str, dict], int | None]:
    """Mapa id_municipio -> {nome, sigla_uf, nome_regiao, capital_uf}.

    Subconjunto de colunas do domínio — o resto do diretório (hierarquia de
    saúde, geocódigos alternativos, geometria) fica fora do escopo.
    """
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT id_municipio, nome, sigla_uf, nome_regiao, capital_uf
        FROM `{DIRETORIO_MUNICIPIO_TABLE}`
    """
    rows, bytes_processed = _do_query(client, sql)
    return {
        row["id_municipio"]: {
            "nome": row["nome"],
            "sigla_uf": row["sigla_uf"],
            "nome_regiao": row["nome_regiao"],
            "capital_uf": row["capital_uf"],
        }
        for row in rows
    }, bytes_processed
