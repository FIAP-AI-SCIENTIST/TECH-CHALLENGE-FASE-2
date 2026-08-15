"""Translate GX results to the project's stable QualityResult contract."""
from dataclasses import dataclass
from typing import Any

from . import rules
from .suites import ExpectationSpec


@dataclass(frozen=True)
class QualityResult:
    check_id: str
    check: str
    entidade: str
    dimensao: str
    passou: bool
    valor_medido: float
    limiar: float
    severidade: str
    linhas_afetadas: int
    detalhe: str = ""
    timestamp: str | None = None


def translate_result(raw: Any, spec: ExpectationSpec, entity: str, row_count: int) -> QualityResult:
    result = raw.result or {}
    unexpected_percent = float(result.get("unexpected_percent", 0.0) or 0.0)
    measured = max(0.0, min(1.0, 1.0 - unexpected_percent / 100.0))
    if spec.check in {"schema", "duplicidade", "consistencia_faixa", "formato_coluna"} and raw.success:
        measured = 1.0
    passed = bool(raw.success) and measured >= spec.threshold
    affected = int(result.get("unexpected_count", 0) or 0)
    return QualityResult(
        check_id=f"{entity}.{spec.check}.{spec.kwargs.get('column', 'key')}",
        check=spec.check, entidade=entity, dimensao=spec.dimension,
        passou=passed, valor_medido=measured, limiar=spec.threshold,
        severidade=rules.SEVERIDADE.get(spec.check, "AVISO"),
        linhas_afetadas=affected, detalhe=str(result),
    )


def translate_validation(validation: Any, specs: list[ExpectationSpec], entity: str, row_count: int) -> list[QualityResult]:
    """Translate by expectation type because GX may reorder graph results."""
    expectation_to_check = {
        "expect_column_to_exist": "schema",
        "expect_column_values_to_not_be_null": "valores_ausentes",
        "expect_column_values_to_be_between": "consistencia_faixa",
        "expect_column_values_to_match_regex": "formato_coluna",
        "expect_compound_columns_to_be_unique": "duplicidade",
    }
    remaining = list(specs)
    translated = []
    for raw in validation.results:
        check = expectation_to_check.get(raw.expectation_config.type)
        spec = next((candidate for candidate in remaining if candidate.check == check), None)
        if spec is None:
            continue
        remaining.remove(spec)
        translated.append(translate_result(raw, spec, entity, row_count))
    return translated
