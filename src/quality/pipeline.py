"""Orchestration for isolated entity validation and evidence persistence.

Two entry points: ``run_entity_quality_checks`` (hook per entity, called by
``silver.pipeline.run_silver`` right after dedup, over the in-memory frame) and
``run_all_quality_checks`` (``make quality``, reading the current Silver state
via ``silver.reader`` — same flow as the colleague's original implementation).
"""
from collections.abc import Mapping
import logging

import pandas as pd
import pyarrow as pa

from .checks import check_row_count
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


def run_all_quality_checks(frames: Mapping[str, pd.DataFrame] | None = None, *, writer=write_results) -> list[QualityResult]:
    """Entry point (`make quality`). Com `frames` injetados, valida direto
    (testes); sem argumento, lê o estado atual da Silver para as 6 entidades
    com isolamento de falha — uma entidade ilegível não impede as demais.
    """
    if frames is not None:
        return run_quality_checks(frames, writer=writer)

    all_results: list[QualityResult] = []
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
    return all_results