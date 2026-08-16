"""Orquestração da camada Gold — dimensões e fatos materializados no
BigQuery (dataset `alfabetizacao_analytics`) a partir da Silver.

Cada tabela Gold é recomputada do zero a cada execução (WRITE_TRUNCATE no
load job) — sem merge incremental, mesma filosofia de posse total de
partição da Bronze/Silver, aplicada aqui à tabela inteira.
"""

import pyarrow as pa

from bronze.writer import BUCKET_NAME
from common.lock import gcs_lock
from gold import marts, transform
from gold.writer import write_table
from observability.logging import log_execution, setup_logger
from silver import reader as silver_reader
from silver.reference import get_diretorio_municipio, get_diretorio_uf

ENTIDADES_META = ("meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio")

_META_CHAVES: dict[str, list[str]] = {
    "meta_alfabetizacao_brasil": ["rede"],
    "meta_alfabetizacao_uf": ["sigla_uf", "rede"],
    "meta_alfabetizacao_municipio": ["id_municipio", "rede"],
}


def _materializar(nome_tabela: str, table) -> int:
    """Lock + escrita — mesmo padrão de guarda do Bronze/Silver, adaptado
    ao destino BigQuery.
    """
    if not table.column_names:
        setup_logger().warning(f"Gold '{nome_tabela}' sem colunas — fonte Silver ainda vazia, pulando.")
        return 0

    with gcs_lock(BUCKET_NAME, f"gold/.locks/{nome_tabela}.lock"):
        with log_execution(unit="Gold", layer="Gold") as run:
            rows = write_table(nome_tabela, table)
            run.rows_read = table.num_rows
            run.rows_written = rows
            return rows


def _diretorio_uf_to_arrow(diretorio: dict[str, str]) -> pa.Table:
    """dict[sigla, nome] → Arrow com colunas sigla_uf, nome."""
    if not diretorio:
        return pa.table({"sigla_uf": pa.array([], type=pa.string()), "nome": pa.array([], type=pa.string())})
    return pa.table({
        "sigla_uf": pa.array(list(diretorio.keys()), type=pa.string()),
        "nome": pa.array(list(diretorio.values()), type=pa.string()),
    })


def _diretorio_municipio_to_arrow(diretorio: dict[str, dict]) -> pa.Table:
    """dict[id, {nome, sigla_uf, nome_regiao, capital_uf}] → Arrow."""
    if not diretorio:
        return pa.table({
            "id_municipio": pa.array([], type=pa.string()),
            "nome": pa.array([], type=pa.string()),
            "sigla_uf": pa.array([], type=pa.string()),
            "nome_regiao": pa.array([], type=pa.string()),
            "capital_uf": pa.array([], type=pa.string()),
        })
    return pa.table({
        "id_municipio": pa.array(list(diretorio.keys()), type=pa.string()),
        "nome": pa.array([v["nome"] for v in diretorio.values()], type=pa.string()),
        "sigla_uf": pa.array([v["sigla_uf"] for v in diretorio.values()], type=pa.string()),
        "nome_regiao": pa.array([v["nome_regiao"] for v in diretorio.values()], type=pa.string()),
        "capital_uf": pa.array([v["capital_uf"] for v in diretorio.values()], type=pa.string()),
    })


def run_gold() -> None:
    """Materializa todas as dimensões e fatos da Gold a partir da Silver.

    Isolamento de falha por tabela (mesmo padrão de
    `silver.pipeline.run_all_silver`) — uma tabela falhando não impede a
    materialização das demais.
    """
    logger = setup_logger()
    falhou = False

    uf = silver_reader.read_entity("uf")
    municipio = silver_reader.read_entity("municipio")
    alunos = silver_reader.read_entity("alunos")

    # dim_uf e dim_municipio: fonte = diretório oficial (não entidade Silver)
    try:
        diretorio_uf = get_diretorio_uf()
        dim_uf = transform.build_dim_uf(_diretorio_uf_to_arrow(diretorio_uf))
    except Exception as exc:
        falhou = True
        logger.error(f"Falha lendo diretório UF para dim_uf: {type(exc).__name__}: {exc}")
        dim_uf = None

    try:
        diretorio_municipio = get_diretorio_municipio()
        dim_municipio = transform.build_dim_municipio(_diretorio_municipio_to_arrow(diretorio_municipio))
    except Exception as exc:
        falhou = True
        logger.error(f"Falha lendo diretório município para dim_municipio: {type(exc).__name__}: {exc}")
        dim_municipio = None

    builds = [
        ("dim_uf", dim_uf),
        ("dim_municipio", dim_municipio),
        ("dim_rede", transform.build_dim_rede(uf, municipio, alunos)),
        ("dim_serie", transform.build_dim_serie(uf, municipio, alunos)),
        ("fact_indicador_uf", transform.build_fact_indicador_uf(uf)),
        ("fact_indicador_municipio", transform.build_fact_indicador_municipio(municipio)),
        ("fact_alunos", transform.build_fact_alunos(alunos)),
    ]

    for nome_tabela, tabela in builds:
        if tabela is None:
            continue
        try:
            _materializar(nome_tabela, tabela)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha materializando '{nome_tabela}' na Gold: {type(exc).__name__}: {exc}")

    for entidade in ENTIDADES_META:
        try:
            scd2 = silver_reader.read_scd2_table_raw(entidade)
            if scd2 is None:
                logger.warning(f"Silver de '{entidade}' ainda não processada — pulando fato de meta na Gold.")
                continue

            chave_cols = _META_CHAVES[entidade]
            sufixo = entidade.removeprefix("meta_alfabetizacao_")
            nome_tabela = f"fact_meta_resultado_{sufixo}"
            tabela = transform.build_fact_meta_resultado(scd2, chave_cols)

            _materializar(nome_tabela, tabela)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha materializando fato de meta '{entidade}' na Gold: {type(exc).__name__}: {exc}")

    # Views analíticas — só depois dos fatos, que são sua fonte. DDL
    # idempotente (CREATE OR REPLACE VIEW), sem lock e com isolamento de falha
    # por view.
    for nome_view in marts.MART_QUERIES:
        try:
            with log_execution(unit="Gold", layer="Gold"):
                marts.create_view(nome_view)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha criando view '{nome_view}' na Gold: {type(exc).__name__}: {exc}")

    if falhou:
        raise RuntimeError("Uma ou mais tabelas falharam na materialização da Gold — ver logs.")
