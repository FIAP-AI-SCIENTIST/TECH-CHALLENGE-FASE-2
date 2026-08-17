"""Orquestração da camada Gold — dimensões e fatos materializados no
BigQuery (dataset analítico) a partir da Silver.

Cada tabela Gold é recomputada do zero a cada execução (WRITE_TRUNCATE no
load job) — sem merge incremental, mesma filosofia de posse total de
partição da Bronze/Silver, aplicada aqui à tabela inteira.
"""

from collections.abc import Callable

import pyarrow as pa

from common.lock import gcs_lock
from config import get_settings
from gold import marts, transform
from gold.writer import write_table
from observability.logging import log_execution, setup_logger
from silver import reader as silver_reader
from silver.reference import get_atlas_idhm, get_diretorio_municipio, get_diretorio_uf, merge_idhm_into_diretorio
from silver.transform import ENTIDADE_INTEGRADA, normalize_key

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

    with gcs_lock(get_settings().bucket_name, f"gold/.locks/{nome_tabela}.lock"):
        with log_execution(step="Gold", layer="Gold") as run:
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
    """dict[id, {nome, sigla_uf, nome_regiao, capital_uf}] → Arrow.

    `id_municipio` sai normalizado em 7 dígitos (`silver.transform.normalize_key`),
    mesma normalização aplicada às FKs dos fatos na Silver — sem isso a dim e o
    fato podem divergir na formatação da chave.
    """
    itens = [(normalize_key(id_mun), dados) for id_mun, dados in diretorio.items()]
    itens = [(id_mun, dados) for id_mun, dados in itens if id_mun is not None]
    if not itens:
        return pa.table({
            "id_municipio": pa.array([], type=pa.string()),
            "nome": pa.array([], type=pa.string()),
            "sigla_uf": pa.array([], type=pa.string()),
            "nome_regiao": pa.array([], type=pa.string()),
            "capital_uf": pa.array([], type=pa.int64()),
            "idhm": pa.array([], type=pa.float64()),
            "idhm_educacao": pa.array([], type=pa.float64()),
            "idhm_renda": pa.array([], type=pa.float64()),
            "idhm_longevidade": pa.array([], type=pa.float64()),
        })
    # Só `id_municipio` tem tipo forçado (chave normalizada); o resto segue o
    # tipo do diretório — `capital_uf` é INT64 na fonte, igual ao que a Silver
    # já monta em `_municipio_dict_to_table`. IDHM via `.get()`: aditivo, o
    # diretório é válido mesmo sem o Atlas fundido.
    return pa.table({
        "id_municipio": pa.array([id_mun for id_mun, _ in itens], type=pa.string()),
        "nome": [d["nome"] for _, d in itens],
        "sigla_uf": [d["sigla_uf"] for _, d in itens],
        "nome_regiao": [d["nome_regiao"] for _, d in itens],
        "capital_uf": [d["capital_uf"] for _, d in itens],
        "idhm": pa.array([d.get("idhm") for _, d in itens], type=pa.float64()),
        "idhm_educacao": pa.array([d.get("idhm_educacao") for _, d in itens], type=pa.float64()),
        "idhm_renda": pa.array([d.get("idhm_renda") for _, d in itens], type=pa.float64()),
        "idhm_longevidade": pa.array([d.get("idhm_longevidade") for _, d in itens], type=pa.float64()),
    })


def _coletar_anos(*tabelas: pa.Table) -> list[int]:
    """União dos valores de `ano` das tabelas Silver lidas — cobertura da
    `dim_tempo`, que precisa conter todo ano referenciado por qualquer fato."""
    anos: list[int] = []
    for tabela in tabelas:
        if "ano" in tabela.column_names:
            anos.extend(tabela.column("ano").to_pylist())
    return anos


def run_gold() -> None:
    """Materializa todas as dimensões e fatos da Gold a partir da Silver.

    Isolamento de falha por tabela (mesmo padrão de
    `silver.pipeline.run_all_silver`) — uma tabela falhando não impede a
    materialização das demais. Dimensões são materializadas antes dos fatos:
    o DDL dos fatos declara FK, que exige a tabela referenciada existente.
    """
    logger = setup_logger()
    falhou = False

    uf = silver_reader.read_entity("uf")
    municipio = silver_reader.read_entity("municipio")
    alunos = silver_reader.read_entity("alunos")
    integrada = silver_reader.read_entity(ENTIDADE_INTEGRADA)

    # SCD2s lidas uma única vez: alimentam os fatos de meta e a cobertura de
    # anos da dim_tempo.
    scd2_por_entidade: dict[str, pa.Table] = {}
    for entidade in ENTIDADES_META:
        scd2 = silver_reader.read_scd2_table_raw(entidade)
        if scd2 is None:
            logger.warning(f"Silver de '{entidade}' ainda não processada — pulando fato de meta na Gold.")
            continue
        scd2_por_entidade[entidade] = scd2

    # dim_uf e dim_municipio: fonte = diretório oficial (não entidade Silver)
    try:
        diretorio_uf, _ = get_diretorio_uf()
        dim_uf = transform.build_dim_uf(_diretorio_uf_to_arrow(diretorio_uf))
    except Exception as exc:
        falhou = True
        logger.error(f"Falha lendo diretório UF para dim_uf: {type(exc).__name__}: {exc}")
        dim_uf = None

    try:
        diretorio_municipio, _ = get_diretorio_municipio()
        atlas_idhm, _ = get_atlas_idhm()
        diretorio_municipio = merge_idhm_into_diretorio(diretorio_municipio, atlas_idhm)
        dim_municipio = transform.build_dim_municipio(_diretorio_municipio_to_arrow(diretorio_municipio))
    except Exception as exc:
        falhou = True
        logger.error(f"Falha lendo diretório município para dim_municipio: {type(exc).__name__}: {exc}")
        dim_municipio = None

    # Lista de construtores, não de tabelas já construídas: um literal de lista
    # avalia as chamadas na hora em que é montado, o que colocaria toda a
    # construção fora do try/except abaixo e faria uma exceção em qualquer
    # tabela abortar a materialização de todas as outras — inclusive das que já
    # tinham sido computadas com sucesso. Adiando a chamada para dentro do laço,
    # cada tabela falha sozinha.
    builds: list[tuple[str, Callable[[], pa.Table | None]]] = [
        ("dim_uf", lambda: dim_uf),
        ("dim_municipio", lambda: dim_municipio),
        ("dim_rede", lambda: transform.build_dim_rede(uf, municipio, alunos)),
        ("dim_serie", lambda: transform.build_dim_serie(uf, municipio, alunos)),
        ("dim_tempo", lambda: transform.build_dim_tempo(
            _coletar_anos(uf, municipio, alunos, integrada, *scd2_por_entidade.values())
        )),
        ("fact_indicador_uf", lambda: transform.build_fact_indicador_uf(uf)),
        ("fact_indicador_municipio", lambda: transform.build_fact_indicador_municipio(municipio)),
        ("fact_alfabetizacao_municipio", lambda: transform.build_fact_alfabetizacao_municipio(integrada)),
        ("fact_alunos", lambda: transform.build_fact_alunos(alunos)),
    ]

    for nome_tabela, construir in builds:
        try:
            tabela = construir()
            if tabela is None:
                continue
            _materializar(nome_tabela, tabela)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha materializando '{nome_tabela}' na Gold: {type(exc).__name__}: {exc}")

    for entidade, scd2 in scd2_por_entidade.items():
        try:
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
            with log_execution(step="Gold", layer="Gold"):
                marts.create_view(nome_view)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha criando view '{nome_view}' na Gold: {type(exc).__name__}: {exc}")

    if falhou:
        raise RuntimeError("Uma ou mais tabelas falharam na materialização da Gold — ver logs.")
