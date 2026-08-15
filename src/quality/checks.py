"""Pure checks that complement native GX expectations when no native metric exists."""
from datetime import datetime
from collections.abc import Sequence

from . import rules
from .translate import QualityResult


def check_row_count(entity: str, row_count: int) -> QualityResult:
    minimum = rules.ROW_COUNT_MIN.get(entity, 1)
    measured = min(1.0, row_count / minimum) if minimum else 1.0
    return QualityResult(f"{entity}.volumetria", "volumetria", entity, rules.DIMENSIONS["volumetria"], row_count >= minimum, measured, 1.0, "CRITICA", 0, f"rows={row_count}; minimum={minimum}")


def check_reconciliation(entity: str, source_rows: int, target_rows: int, minimum: float = rules.ROW_COUNT_MATCH_MIN) -> QualityResult:
    measured = target_rows / source_rows if source_rows else 0.0
    measured = min(1.0, measured)
    return QualityResult(f"{entity}.reconciliacao", "reconciliacao", entity, rules.DIMENSIONS["reconciliacao"], measured >= minimum, measured, minimum, "CRITICA", max(0, source_rows - target_rows))


def check_data_freshness(entity: str, latest_year: int, current_year: int, allowed_lag: int | None = None) -> QualityResult:
    allowed_lag = rules.FRESHNESS_ANOS.get(entity, 2) if allowed_lag is None else allowed_lag
    lag = max(0, current_year - latest_year)
    measured = 1.0 if lag <= allowed_lag else 0.0
    return QualityResult(f"{entity}.frescor_dado", "frescor_dado", entity, rules.DIMENSIONS["frescor_dado"], measured == 1.0, measured, 1.0, "AVISO", 0, f"lag_years={lag}; allowed={allowed_lag}")


def check_file_freshness(entity: str, processed_at: datetime, now: datetime, allowed_hours: int = rules.FRESHNESS_HORAS) -> QualityResult:
    age_hours = max(0.0, (now - processed_at).total_seconds() / 3600)
    measured = 1.0 if age_hours <= allowed_hours else 0.0
    return QualityResult(f"{entity}.frescor_arquivo", "frescor_arquivo", entity, rules.DIMENSIONS["frescor_arquivo"], measured == 1.0, measured, 1.0, "AVISO", 0, f"age_hours={age_hours:.2f}; allowed={allowed_hours}")


def check_referential_integrity(entity: str, values: Sequence, valid_values: set) -> QualityResult:
    invalid = sum(value not in valid_values for value in values)
    measured = 1.0 - (invalid / len(values)) if values else 0.0
    return QualityResult(f"{entity}.chave_relacionamento", "chave_relacionamento", entity, rules.DIMENSIONS["chave_relacionamento"], measured == 1.0, measured, 1.0, "CRITICA", invalid)
