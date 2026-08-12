"""Orquestração da camada Silver — uma execução por entidade, mesma
granularidade de auditoria já usada pelas units anteriores.
"""

from bronze import reader as bronze_reader
from bronze import writer as bronze_writer
from common.lock import gcs_lock
from observability.logging import log_execution, setup_logger
from quality.pipeline import run_quality_checks
from silver import reference
from silver import reader as silver_reader
from silver import writer as silver_writer
from silver.transform import (
    ENTIDADES_META,
    ENTIDADES_REGULARES,
    _with_scd2_columns,
    apply_scd2,
    clean,
    dedupe,
    group_by_ano,
)

ENTIDADES = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
]


def _load_referencias(entidade: str) -> dict:
    """Carrega os mapas de tradução necessários para a entidade — cada uma
    usa só um subconjunto (domain-entities.md)."""
    referencias: dict = {"rede": reference.get_dicionario(entidade, "rede")}

    if entidade in ("uf", "municipio"):
        referencias["serie"] = reference.get_dicionario(entidade, "serie")

    if entidade in ("uf", "meta_alfabetizacao_uf"):
        referencias["diretorio_uf"] = reference.get_diretorio_uf()

    if entidade in ("municipio", "meta_alfabetizacao_municipio"):
        referencias["diretorio_municipio"] = reference.get_diretorio_municipio()

    return referencias


def run_silver(entidade: str) -> None:
    """Processa uma entidade da Silver: lê Bronze -> traduz -> normaliza ->
    limpa -> deduplica -> (agrupa por ano | SCD2) -> escreve.

    Protegida por lock exclusivo (`common.lock.gcs_lock`) — a mesma corrida
    de concorrência já corrigida na U4 (Bronze) se aplica aqui, agravada no
    caso do SCD2 (leitura-decide-escreve).
    """
    logger = setup_logger()

    with gcs_lock(bronze_writer.BUCKET_NAME, f"silver/.locks/{entidade}.lock"):
        with log_execution(unit="Silver", layer="Silver") as run:
            bruta = bronze_reader.read_partition(entidade)
            rows_read = bruta.num_rows

            referencias = _load_referencias(entidade)
            limpa, rejeitadas = clean(entidade, bruta, referencias)
            if rejeitadas:
                logger.warning(
                    f"{rejeitadas} linha(s) rejeitada(s) em '{entidade}': id_municipio inválido"
                )

            dedupada = dedupe(entidade, limpa)
            run_quality_checks(entidade, dedupada)
            grupos_por_ano = group_by_ano(dedupada)

            rows_written = 0
            if entidade in ENTIDADES_REGULARES:
                for ano, tabela_ano in grupos_por_ano.items():
                    chave = f"ano={ano}"
                    silver_writer.clear_entity(entidade, chave)
                    rows_written += silver_writer.write_entity(entidade, chave, tabela_ano)
            elif entidade in ENTIDADES_META:
                schema_scd2 = _with_scd2_columns(dedupada.schema)
                dimensao = silver_reader.read_scd2_table(entidade, schema_scd2)
                # Aplica ano a ano, em ordem cronológica — cada ano é um
                # snapshot da fonte naquele momento, e o SCD2 precisa
                # comparar contra o estado vigente ANTES de avançar pro
                # próximo ano, não misturar todos os anos de uma vez.
                for ano in sorted(grupos_por_ano.keys()):
                    dimensao = apply_scd2(entidade, dimensao, grupos_por_ano[ano], ano)
                rows_written = silver_writer.write_scd2_table(entidade, dimensao)

            run.rows_read = rows_read
            run.rows_written = rows_written


def run_all_silver() -> None:
    """Processa as 6 entidades com isolamento de falha (mesmo padrão do
    `make bronze`): uma entidade falhando não impede as demais.
    """
    logger = setup_logger()
    falhou = False

    for entidade in ENTIDADES:
        try:
            run_silver(entidade)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha processando '{entidade}' na Silver: {type(exc).__name__}: {exc}")

    if falhou:
        raise RuntimeError("Uma ou mais entidades falharam no processamento da Silver — ver logs.")
