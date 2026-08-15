"""Great Expectations suite construction and execution.

No cloud clients or file reads are allowed here. A fresh ephemeral context is used per call.
"""
from dataclasses import dataclass
from typing import Any

import great_expectations as gx
import pandas as pd
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator

from . import rules


@dataclass(frozen=True)
class ExpectationSpec:
    check: str
    kwargs: dict[str, Any]
    dimension: str
    threshold: float = 1.0


def expectation_specs(entity: str, frame: pd.DataFrame) -> list[ExpectationSpec]:
    """Return the declarative GX expectations applicable to an entity."""
    specs: list[ExpectationSpec] = []
    for column in rules.REQUIRED_COLUMNS.get(entity, []):
        specs.append(ExpectationSpec("schema", {"column": column}, rules.DIMENSIONS["schema"]))
        specs.append(ExpectationSpec("valores_ausentes", {"column": column}, rules.DIMENSIONS["valores_ausentes"], 0.95))
    for column, minimum, maximum in rules.VALUE_RANGES.get(entity, []):
        if column in frame.columns:
            specs.append(ExpectationSpec("consistencia_faixa", {"column": column, "min_value": minimum, "max_value": maximum}, rules.DIMENSIONS["consistencia_faixa"]))
    for column, regex in rules.COLUMN_PATTERNS.get(entity, {}).items():
        if column in frame.columns:
            specs.append(ExpectationSpec("formato_coluna", {"column": column, "regex": regex}, rules.DIMENSIONS["formato_coluna"]))
    keys = rules.DUPLICATE_KEYS.get(entity, [])
    if keys and all(column in frame.columns for column in keys):
        specs.append(ExpectationSpec("duplicidade", {"column_list": keys}, rules.DIMENSIONS["duplicidade"]))
    return specs


def build_suite(entity: str, frame: pd.DataFrame) -> tuple[ExpectationSuite, list[ExpectationSpec]]:
    """Build a named ephemeral suite and return its registry specifications."""
    gx.get_context(mode="ephemeral")
    specs = expectation_specs(entity, frame)
    return ExpectationSuite(name=f"quality_{entity}"), specs


def validate_dataframe(frame: pd.DataFrame, entity: str):
    """Validate a pandas frame through GX's PandasExecutionEngine."""
    gx.get_context(mode="ephemeral")
    suite, specs = build_suite(entity, frame)
    validator = Validator(execution_engine=PandasExecutionEngine(), batches=[Batch(data=frame)], expectation_suite=suite)
    executed: list[ExpectationSpec] = []
    for spec in specs:
        columns = spec.kwargs.get("column_list") or [spec.kwargs.get("column")]
        if spec.check != "schema" and not all(column in frame.columns for column in columns):
            continue
        if spec.check == "schema":
            validator.expect_column_to_exist(column=columns[0])
        elif spec.check == "valores_ausentes":
            validator.expect_column_values_to_not_be_null(column=columns[0], mostly=spec.threshold)
        elif spec.check == "consistencia_faixa":
            validator.expect_column_values_to_be_between(**spec.kwargs)
        elif spec.check == "formato_coluna":
            validator.expect_column_values_to_match_regex(**spec.kwargs)
        elif spec.check == "duplicidade":
            validator.expect_compound_columns_to_be_unique(**spec.kwargs)
        executed.append(spec)
    return validator.validate(only_return_failures=False), executed
