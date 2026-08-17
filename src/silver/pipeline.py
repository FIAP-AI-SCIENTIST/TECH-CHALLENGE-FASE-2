"""Orquestração da camada Silver — uma execução por entidade, mesma
granularidade de auditoria já usada pelas units anteriores.
"""

import pyarrow as pa

from bronze import reader as bronze_reader
from common.lock import gcs_lock
from config import get_settings
from observability.logging import log_execution, setup_logger
from silver import reader as silver_reader
from silver import reference
from silver import writer as silver_writer
from silver.transform import (
    ENTIDADE_INTEGRADA,
    ENTIDADES_META,
    ENTIDADES_REGULARES,
    _with_scd2_columns,
    apply_scd2,
    clean,
    dedupe,
    group_by_ano,
    integrate_alfabetizacao_municipio,
)

ENTIDADES = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
]


def _load_referencias(entidade: str) -> tuple[dict, int]:
    """Carrega os mapas de tradução necessários para a entidade — cada uma
    usa só um subconjunto de cada uma.

    Retorna ``(referencias, total_bytes_processed)``: a soma dos bytes das
    consultas de referência é auditada no run da entidade.
    """
    referencias: dict = {}
    total_bytes = 0

    referencias["rede"], b = reference.get_dicionario(entidade, "rede")
    total_bytes += b or 0

    if entidade in ("uf", "municipio"):
        referencias["serie"], b = reference.get_dicionario(entidade, "serie")
        total_bytes += b or 0

    if entidade in ("uf", "meta_alfabetizacao_uf"):
        referencias["diretorio_uf"], b = reference.get_diretorio_uf()
        total_bytes += b or 0

    if entidade in ("municipio", "meta_alfabetizacao_municipio"):
        referencias["diretorio_municipio"], b = reference.get_diretorio_municipio()
        total_bytes += b or 0

    return referencias, total_bytes


def run_silver(entidade: str) -> None:
    """Processa uma entidade da Silver: lê Bronze -> traduz -> normaliza ->
    limpa -> deduplica -> (agrupa por ano | SCD2) -> escreve.

    Protegida por lock exclusivo (`common.lock.gcs_lock`) — a mesma corrida
    de concorrência já corrigida na ingestão Bronze se aplica aqui: `clear` e
    `write` intercalados entre duas execuções deixariam a partição com
    arquivos de runs diferentes. As 3 entidades de meta são reconstruídas do
    Bronze a cada execução, então não há mais leitura-decide-escreve
    a proteger — só a escrita da tabela derivada.
    """
    logger = setup_logger()

    with gcs_lock(get_settings().bucket_name, f"silver/.locks/{entidade}.lock"):
        with log_execution(step="Silver", layer="Silver") as run:
            bruta = bronze_reader.read_partition(entidade)
            rows_read = bruta.num_rows

            referencias, total_bytes = _load_referencias(entidade)
            limpa, rejeitadas = clean(entidade, bruta, referencias)
            if rejeitadas:
                logger.warning(
                    f"{rejeitadas} linha(s) rejeitada(s) em '{entidade}': id_municipio inválido"
                )

            dedupada = dedupe(entidade, limpa)
            grupos_por_ano = group_by_ano(dedupada)

            rows_written = 0
            if entidade in ENTIDADES_REGULARES:
                for ano, tabela_ano in grupos_por_ano.items():
                    chave = f"ano={ano}"
                    silver_writer.clear_entity(entidade, chave)
                    rows_written += silver_writer.write_entity(entidade, chave, tabela_ano)
            elif entidade in ENTIDADES_META:
                schema_scd2 = _with_scd2_columns(dedupada.schema)
                # A cadeia de versões é reconstruída do zero a cada execução
                # a tabela SCD2 é sempre derivada do
                # Bronze, então partir do estado persistido tornava o replay
                # não idempotente — o ano mais antigo era comparado contra a
                # versão vigente deixada pelo ano mais recente do run
                # anterior, divergia sempre, e cada execução acrescentava uma
                # cadeia de versões duplicada.
                dimensao = pa.Table.from_pylist([], schema=schema_scd2)
                # Ano a ano, em ordem cronológica: cada ano é um snapshot da
                # fonte naquele momento e o SCD2 compara contra a versão
                # vigente ANTES de avançar, em vez de misturar todos os anos.
                for ano in sorted(grupos_por_ano.keys()):
                    dimensao = apply_scd2(entidade, dimensao, grupos_por_ano[ano], ano)
                rows_written = silver_writer.write_scd2_table(entidade, dimensao)

            run.rows_read = rows_read
            run.rows_written = rows_written
            run.total_bytes_processed = total_bytes

            # Valida o frame deduplicado ainda em memória, depois da escrita: a
            # regra é que uma falha CRITICA sinaliza a execução na auditoria e
            # deixa evidência na tabela de qualidade, mas nunca desfaz o que já
            # foi gravado nem interrompe as entidades seguintes — a Silver é
            # reconstruída por inteiro a cada execução, então reprocessar
            # corrige o dado sem precisar de rollback.
            from quality.pipeline import run_entity_quality_checks

            dq_results = run_entity_quality_checks(entidade, dedupada)
            if any(not r.passou and r.severidade == "CRITICA" for r in dq_results):
                run.status = "SUCCESS_WITH_DQ_FAILURE"
                logger.error(f"Data Quality com falha CRITICA em '{entidade}' — ver data_quality_log.")


def run_integracao() -> None:
    """Constrói a tabela integrada da Silver: indicador municipal x meta
    municipal (JOIN temporal sobre a cadeia SCD2), particionada por `ano=`.

    Roda sobre o **estado Silver** das duas entidades de insumo — não sobre a
    Bronze — então herda limpeza, normalização de chave e deduplicação já
    aplicadas. Clear+write por partição a cada execução: a integrada é função
    pura do estado Silver, sem estado próprio.
    """
    logger = setup_logger()

    with gcs_lock(get_settings().bucket_name, f"silver/.locks/{ENTIDADE_INTEGRADA}.lock"):
        with log_execution(step="Silver", layer="Silver") as run:
            municipio = silver_reader.read_entity("municipio")
            meta = silver_reader.read_scd2_table_raw("meta_alfabetizacao_municipio")

            integrada = integrate_alfabetizacao_municipio(municipio, meta)

            run.rows_read = municipio.num_rows
            run.rows_written = 0

            if integrada.num_rows == 0:
                # Sem indicador processado não há o que integrar — mantém o
                # estado já gravado em vez de apagar partições por um insumo
                # vazio (que pode ser leitura transitória, não fonte vazia).
                logger.warning(f"Integração '{ENTIDADE_INTEGRADA}' sem saída — 'municipio' vazio ou não processado.")
                return

            for ano, tabela_ano in group_by_ano(integrada).items():
                chave = f"ano={ano}"
                silver_writer.clear_entity(ENTIDADE_INTEGRADA, chave)
                run.rows_written += silver_writer.write_entity(ENTIDADE_INTEGRADA, chave, tabela_ano)

            from quality.pipeline import run_entity_quality_checks

            dq_results = run_entity_quality_checks(ENTIDADE_INTEGRADA, integrada)
            if any(not r.passou and r.severidade == "CRITICA" for r in dq_results):
                run.status = "SUCCESS_WITH_DQ_FAILURE"
                logger.error(f"Data Quality com falha CRITICA em '{ENTIDADE_INTEGRADA}' — ver data_quality_log.")


def run_all_silver() -> None:
    """Processa as 6 entidades com isolamento de falha (mesmo padrão do
    `make bronze`): uma entidade falhando não impede as demais. A tabela
    integrada só é reconstruída se todas as 6 tiverem sucesso — ela é
    derivada, e insumo parcial não deve gerar saída parcial.
    """
    logger = setup_logger()
    falhou = False

    for entidade in ENTIDADES:
        try:
            run_silver(entidade)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha processando '{entidade}' na Silver: {type(exc).__name__}: {exc}")

    if not falhou:
        try:
            run_integracao()
        except Exception as exc:
            falhou = True
            logger.error(f"Falha na integração da Silver: {type(exc).__name__}: {exc}")

    if falhou:
        raise RuntimeError("Uma ou mais entidades falharam no processamento da Silver — ver logs.")
