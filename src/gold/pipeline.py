"""Orquestração da camada Gold — dimensões e fatos materializados no
BigQuery (dataset `alfabetizacao_analytics`) a partir da Silver.

Cada tabela Gold é recomputada do zero a cada execução (WRITE_TRUNCATE no
load job) — sem merge incremental, mesma filosofia de posse total de
partição da Bronze/Silver, aplicada aqui à tabela inteira.
"""

from bronze.writer import BUCKET_NAME
from common.lock import gcs_lock
from gold import transform
from gold.writer import write_table
from observability.logging import log_execution, setup_logger
from quality.checks import check_referential_integrity
from quality.writer import write_quality_results
from silver import reader as silver_reader

ENTIDADES_META = ("meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio")

_META_CHAVES: dict[str, list[str]] = {
    "meta_alfabetizacao_brasil": ["rede"],
    "meta_alfabetizacao_uf": ["sigla_uf", "rede"],
    "meta_alfabetizacao_municipio": ["id_municipio", "rede"],
}


def _check_referencial(nome_tabela: str, tabela, fks: dict) -> None:
    """Consistência entre tabelas: toda FK de um fato deve resolver contra
    a dimensão correspondente — a única verificação de qualidade que só faz
    sentido na Gold (as demais regras já correm na Silver, por entidade
    isolada, em `silver.pipeline`).
    """
    resultados = [check_referential_integrity(nome_tabela, tabela, col, chaves) for col, chaves in fks.items()]
    logger = setup_logger()
    for r in resultados:
        if not r.passou:
            logger.warning(f"Data Quality falhou: {r.check} em '{r.entidade}' — {r.detalhe}")
    write_quality_results(resultados)


def _materializar(nome_tabela: str, table, fks: dict | None = None) -> int:
    """Aplica Data Quality (se houver FK a validar) + lock + escrita — mesmo
    padrão de guarda do Bronze/Silver, adaptado ao destino BigQuery.
    """
    if not table.column_names:
        setup_logger().warning(f"Gold '{nome_tabela}' sem colunas — fonte Silver ainda vazia, pulando.")
        return 0

    if fks:
        _check_referencial(nome_tabela, table, fks)

    with gcs_lock(BUCKET_NAME, f"gold/.locks/{nome_tabela}.lock"):
        with log_execution(unit="Gold", layer="Gold") as run:
            rows = write_table(nome_tabela, table)
            run.rows_read = table.num_rows
            run.rows_written = rows
            return rows


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

    dim_uf = transform.build_dim_uf(uf)
    dim_municipio = transform.build_dim_municipio(municipio)
    chaves_uf = set(dim_uf.column("sigla_uf").to_pylist()) if "sigla_uf" in dim_uf.column_names else set()
    chaves_municipio = (
        set(dim_municipio.column("id_municipio").to_pylist())
        if "id_municipio" in dim_municipio.column_names
        else set()
    )

    builds = [
        ("dim_uf", dim_uf, None),
        ("dim_municipio", dim_municipio, None),
        ("dim_rede", transform.build_dim_rede(uf, municipio, alunos), None),
        ("dim_serie", transform.build_dim_serie(uf, municipio, alunos), None),
        ("fact_indicador_uf", transform.build_fact_indicador_uf(uf), {"sigla_uf": chaves_uf}),
        (
            "fact_indicador_municipio",
            transform.build_fact_indicador_municipio(municipio),
            {"id_municipio": chaves_municipio},
        ),
        ("fact_alunos", transform.build_fact_alunos(alunos), {"id_municipio": chaves_municipio}),
    ]

    for nome_tabela, tabela, fks in builds:
        try:
            _materializar(nome_tabela, tabela, fks)
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

            fks = None
            if "sigla_uf" in chave_cols:
                fks = {"sigla_uf": chaves_uf}
            elif "id_municipio" in chave_cols:
                fks = {"id_municipio": chaves_municipio}

            _materializar(nome_tabela, tabela, fks)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha materializando fato de meta '{entidade}' na Gold: {type(exc).__name__}: {exc}")

    if falhou:
        raise RuntimeError("Uma ou mais tabelas falharam na materialização da Gold — ver logs.")
