"""Orchestration for isolated entity validation and evidence persistence.

Two entry points: ``run_entity_quality_checks`` (hook per entity, called by
``silver.pipeline.run_silver`` right after dedup, over the in-memory frame) and
``run_all_quality_checks`` (``make quality``, reading the current Silver state
via ``silver.reader`` — same flow as the colleague's original implementation).
"""
from collections.abc import Mapping
from datetime import datetime
import logging

import pandas as pd
import pyarrow as pa

from bronze import reader as bronze_reader

from . import gold_reader, rules
from observability.logging import setup_logger
from .checks import check_data_freshness, check_reconciliation, check_referential_integrity, check_row_count
from .suites import validate_dataframe
from .translate import QualityResult, translate_validation
from .writer import write_results

logger = logging.getLogger(__name__)

ENTIDADES = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
]


def run_quality_checks(frames: Mapping[str, pd.DataFrame], *, writer=write_results) -> list[QualityResult]:
    """Run every entity independently; one bad entity does not suppress the others."""
    all_results: list[QualityResult] = []
    for entity, frame in frames.items():
        all_results.append(check_row_count(entity, len(frame)))
        try:
            validation, specs = validate_dataframe(frame, entity)
            entity_results = translate_validation(validation, specs, entity, len(frame))
            if not entity_results:
                entity_results = [QualityResult(
                    check_id=f"{entity}.schema", check="schema", entidade=entity,
                    dimensao="Consistência", passou=False, valor_medido=0.0, limiar=1.0,
                    severidade="CRITICA", linhas_afetadas=len(frame), detalhe="Entidade sem regras registradas",
                )]
            all_results.extend(entity_results)
        except Exception as exc:
            logger.exception("quality entity failed", extra={"entity": entity})
            all_results.append(QualityResult(
                check_id=f"{entity}.engine", check="engine", entidade=entity,
                dimensao="Consistência", passou=False, valor_medido=0.0, limiar=1.0,
                severidade="CRITICA", linhas_afetadas=len(frame), detalhe=str(exc),
            ))
    try:
        writer(all_results)
    except Exception:
        logger.exception("quality evidence write failed")
    return all_results


def run_entity_quality_checks(entity: str, table: pa.Table, *, writer=write_results) -> list[QualityResult]:
    """Hook por entidade para `silver.pipeline.run_silver` — valida o frame já
    deduplicado em memória, sem reler da Silver. Nunca levanta: falha de
    validação vira resultado; falha de engine vira QualityResult CRITICA.
    """
    return run_quality_checks({entity: table.to_pandas()}, writer=writer)


def _read_silver_state(entity: str) -> pd.DataFrame:
    """Lê o estado atual da Silver para `make quality` (mesma lógica da
    implementação original da Nic): entidades de meta vêm da tabela SCD2
    completa, as demais da concatenação das partições `ano=`.
    """
    from silver import reader as silver_reader
    from silver.transform import ENTIDADES_META

    if entity in ENTIDADES_META:
        scd2 = silver_reader.read_scd2_table_raw(entity)
        table = scd2 if scd2 is not None else pa.Table.from_pydict({})
    else:
        table = silver_reader.read_entity(entity)
    return table.to_pandas()


def _freshness_result(entity: str, frame: pd.DataFrame) -> QualityResult | None:
    """Frescor do dado (lag de ano) — pulado se o frame estiver vazio ou sem `ano`, mesma
    proteção que os demais checks já aplicam a entidade sem dado."""
    if frame.empty or "ano" not in frame.columns:
        return None
    latest_year = int(frame["ano"].max())
    current_year = datetime.now().year
    return check_data_freshness(entity, latest_year, current_year)


def _reconciliation_result(entity: str, target_rows: int) -> QualityResult:
    """Reconciliação Bronze→Silver — só para entidades regulares (chamador filtra)."""
    try:
        source_rows = bronze_reader.count_partition_rows(entity)
    except Exception as exc:
        logger.exception("quality bronze read failed", extra={"entity": entity})
        return QualityResult(
            check_id=f"{entity}.read", check="read", entidade=entity,
            dimensao="Consistência", passou=False, valor_medido=0.0, limiar=1.0,
            severidade="CRITICA", linhas_afetadas=0, detalhe=str(exc),
        )
    return check_reconciliation(entity, source_rows, target_rows)


def _fk_check_results() -> list[QualityResult]:
    """Integridade referencial fato×dimensão na Gold — um resultado por par de `rules.FK_PAIRS`,
    isolamento de falha por par (leitura da Gold é feita de fora, nunca acoplada a `gold.pipeline`)."""
    results: list[QualityResult] = []
    for fato, coluna, dimensao, coluna_dim in rules.FK_PAIRS:
        try:
            values, _ = gold_reader.read_column(fato, coluna)
            valid_values, _ = gold_reader.read_column(dimensao, coluna_dim)
            results.append(check_referential_integrity(fato, values, set(valid_values)))
        except Exception as exc:
            logger.exception("quality gold read failed", extra={"fato": fato, "dimensao": dimensao})
            results.append(QualityResult(
                check_id=f"{fato}.engine", check="engine", entidade=fato,
                dimensao="Consistência", passou=False, valor_medido=0.0, limiar=1.0,
                severidade="CRITICA", linhas_afetadas=0, detalhe=str(exc),
            ))
    return results


def _log_fim_execucao(all_results: list[QualityResult]) -> None:
    """Mensagem final de `make quality` no mesmo formato JSON das demais camadas
    (`observability.logging`) — o run termina com evidência, não com exceção,
    então o status reflete as falhas CRITICA encontradas, não a saúde do processo.
    """
    falhas = [r for r in all_results if not r.passou]
    criticas = [r for r in falhas if r.severidade == "CRITICA"]
    log = setup_logger()
    if criticas:
        log.error(
            "Fim da execução — SUCCESS_WITH_DQ_FAILURE: "
            f"{len(criticas)} falha(s) CRITICA em {', '.join(sorted({r.entidade for r in criticas}))} "
            "— ver data_quality_log.",
            extra={"unit": "Quality", "layer": "Quality", "status": "SUCCESS_WITH_DQ_FAILURE"},
        )
        return
    log.info(
        f"Fim da execução — sucesso: {len(all_results)} checks, {len(falhas)} falha(s) não-CRITICA "
        "— ver data_quality_log.",
        extra={"unit": "Quality", "layer": "Quality", "status": "SUCCESS"},
    )


def run_all_quality_checks(frames: Mapping[str, pd.DataFrame] | None = None, *, writer=write_results) -> list[QualityResult]:
    """Entry point (`make quality`). Com `frames` injetados, valida direto
    (testes); sem argumento, lê o estado atual da Silver para as 6 entidades
    com isolamento de falha — uma entidade ilegível não impede as demais.

    Sem `frames`, também roda freshness (6 entidades), reconciliação Bronze→Silver (só as 3
    entidades regulares) e integridade referencial fato×dimensão na Gold (5 pares) — U10.
    """
    if frames is not None:
        return run_quality_checks(frames, writer=writer)

    from silver.transform import ENTIDADES_REGULARES

    all_results: list[QualityResult] = []
    extra_results: list[QualityResult] = []
    for entity in ENTIDADES:
        try:
            frame = _read_silver_state(entity)
        except Exception as exc:
            logger.exception("quality silver read failed", extra={"entity": entity})
            all_results.append(QualityResult(
                check_id=f"{entity}.read", check="read", entidade=entity,
                dimensao="Consistência", passou=False, valor_medido=0.0, limiar=1.0,
                severidade="CRITICA", linhas_afetadas=0, detalhe=str(exc),
            ))
            continue
        all_results.extend(run_quality_checks({entity: frame}, writer=writer))

        freshness = _freshness_result(entity, frame)
        if freshness is not None:
            extra_results.append(freshness)

        if entity in ENTIDADES_REGULARES:
            extra_results.append(_reconciliation_result(entity, len(frame)))

    extra_results.extend(_fk_check_results())

    try:
        writer(extra_results)
    except Exception:
        logger.exception("quality evidence write failed")

    all_results.extend(extra_results)
    _log_fim_execucao(all_results)
    return all_results