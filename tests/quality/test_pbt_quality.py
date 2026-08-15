from hypothesis import given, strategies as st

from quality.translate import QualityResult


@given(st.floats(min_value=0, max_value=1, allow_nan=False, allow_infinity=False), st.floats(min_value=0, max_value=1, allow_nan=False, allow_infinity=False))
def test_quality_result_metric_and_threshold_are_bounded(metric, threshold):
    result = QualityResult("id", "check", "uf", "Validade", metric >= threshold, metric, threshold, "AVISO", 0)
    assert 0 <= result.valor_medido <= 1
    assert result.passou == (result.valor_medido >= result.limiar)
