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
from config import get_settings
from silver.transform import normalize_key

DICIONARIO_TABLE = "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario"
DIRETORIO_UF_TABLE = "basedosdados.br_bd_diretorios_brasil.uf"
DIRETORIO_MUNICIPIO_TABLE = "basedosdados.br_bd_diretorios_brasil.municipio"
# Atlas do Desenvolvimento Humano (IPEA/PNUD/FJP) — fonte externa de
# enriquecimento (não é dado do domínio do indicador). Confirmado via
# documentação pública/uso da comunidade, não via query real contra o
# BigQuery (sem credenciais disponíveis no momento da escrita) — reconfirme
# com `bq show basedosdados:mundo_onu_adh.municipio` antes de rodar em
# produção pela primeira vez.
ATLAS_IDHM_TABLE = "basedosdados.mundo_onu_adh.municipio"
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
    settings = get_settings()
    client = bigquery.Client(project=settings.project_id)
    sql = f"""
        SELECT chave, valor
        FROM `{DICIONARIO_TABLE}`
        WHERE id_tabela = '{id_tabela}' AND nome_coluna = '{nome_coluna}'
    """
    rows, bytes_processed = _do_query(client, sql)
    return {row["chave"]: row["valor"] for row in rows}, bytes_processed


def get_diretorio_uf() -> tuple[dict[str, str], int | None]:
    """Mapa sigla -> nome completo da UF, + bytes processados pela consulta."""
    settings = get_settings()
    client = bigquery.Client(project=settings.project_id)
    sql = f"SELECT DISTINCT sigla, nome FROM `{DIRETORIO_UF_TABLE}`"
    rows, bytes_processed = _do_query(client, sql)
    return {row["sigla"]: row["nome"] for row in rows}, bytes_processed


def get_diretorio_municipio() -> tuple[dict[str, dict], int | None]:
    """Mapa id_municipio -> {nome, sigla_uf, nome_regiao, capital_uf}.

    Subconjunto de colunas do domínio — o resto do diretório (hierarquia de
    saúde, geocódigos alternativos, geometria) fica fora do escopo.
    """
    settings = get_settings()
    client = bigquery.Client(project=settings.project_id)
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


def get_atlas_idhm(ano: int = 2010) -> tuple[dict[str, dict], int | None]:
    """Mapa id_municipio -> {idhm, idhm_educacao, idhm_renda, idhm_longevidade}
    do Atlas do Desenvolvimento Humano (PNUD/IPEA/FJP), + bytes processados.

    O Atlas é censitário, não anual — os anos disponíveis são 1991, 2000 e
    2010; `ano` seleciona o Censo de referência, com default no mais recente.
    As colunas da fonte usam sufixo abreviado (`idhm_e`/`idhm_l`/`idhm_r`) —
    aqui saem com nome descritivo, mesma tradução que `get_diretorio_municipio`
    já faz para o subconjunto de colunas do diretório.
    """
    settings = get_settings()
    client = bigquery.Client(project=settings.project_id)
    sql = f"""
        SELECT id_municipio, idhm, idhm_e, idhm_l, idhm_r
        FROM `{ATLAS_IDHM_TABLE}`
        WHERE ano = {ano}
    """
    rows, bytes_processed = _do_query(client, sql)
    return {
        row["id_municipio"]: {
            "idhm": row["idhm"],
            "idhm_educacao": row["idhm_e"],
            "idhm_renda": row["idhm_r"],
            "idhm_longevidade": row["idhm_l"],
        }
        for row in rows
    }, bytes_processed


def merge_idhm_into_diretorio(diretorio_municipio: dict[str, dict], atlas_idhm: dict[str, dict]) -> dict[str, dict]:
    """Funde o Atlas IDHM no diretório de município por `id_municipio`
    normalizado — aditivo, sem tabela de lookup separada: é a mesma peça que
    já carrega dado territorial por município (`get_diretorio_municipio`),
    consumida por `silver.transform._municipio_dict_to_table` e por
    `gold.pipeline` para `dim_municipio`.

    Município sem cobertura no Atlas (ex.: criado depois do Censo 2010) sai
    com as 4 colunas IDHM em `None` — nunca é descartado do diretório por
    isso, mesma regra de "nunca descarte silencioso" já aplicada a
    `id_municipio` inválido em `silver.transform.clean`.
    """
    atlas_normalizado = {normalize_key(chave): dados for chave, dados in atlas_idhm.items()}
    fundido: dict[str, dict] = {}
    for id_municipio, dados in diretorio_municipio.items():
        info = atlas_normalizado.get(normalize_key(id_municipio))
        fundido[id_municipio] = {
            **dados,
            "idhm": info["idhm"] if info else None,
            "idhm_educacao": info["idhm_educacao"] if info else None,
            "idhm_renda": info["idhm_renda"] if info else None,
            "idhm_longevidade": info["idhm_longevidade"] if info else None,
        }
    return fundido
