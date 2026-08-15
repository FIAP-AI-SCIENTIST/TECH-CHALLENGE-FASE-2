import pandas as pd

from quality.suites import build_suite, validate_dataframe
from quality.translate import translate_validation
from quality.writer import rows_for_bigquery


def frame():
    return pd.DataFrame({
        "ano": [2024, 2025], "sigla_uf": ["SP", "RJ"],
        "serie": ["A", "B"], "rede": ["publica", "publica"],
        "taxa_alfabetizacao": [90.0, 80.0],
    })


def test_suite_is_declarative_and_translates_gx_results():
    suite, specs = build_suite("uf", frame())
    assert suite.name == "quality_uf"
    assert any(spec.check == "valores_ausentes" for spec in specs)
    validation, specs = validate_dataframe(frame(), "uf")
    results = translate_validation(validation, specs, "uf", 2)
    assert all(result.passou for result in results if result.check != "volumetria")
    assert all(0 <= result.valor_medido <= 1 for result in results)


def test_bad_value_is_reported():
    data = frame()
    data.loc[0, "taxa_alfabetizacao"] = 101
    validation, specs = validate_dataframe(data, "uf")
    results = translate_validation(validation, specs, "uf", len(data))
    failed = [result for result in results if result.check == "consistencia_faixa"]
    assert failed and not failed[0].passou


def test_writer_payload_is_json_ready():
    validation, specs = validate_dataframe(frame(), "uf")
    rows = rows_for_bigquery(translate_validation(validation, specs, "uf", 2))
    assert rows and {"check_id", "entidade", "passou", "timestamp"} <= rows[0].keys()
