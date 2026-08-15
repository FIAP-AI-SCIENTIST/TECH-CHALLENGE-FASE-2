import pandas as pd
import pyarrow as pa
import pytest

from quality.pipeline import run_all_quality_checks, run_entity_quality_checks, run_quality_checks


def test_entities_are_isolated_and_writer_is_best_effort():
    frames = {"uf": pd.DataFrame({"ano": [2024]}), "unknown": pd.DataFrame({"x": [1]})}
    written = []
    results = run_quality_checks(frames, writer=lambda rows: written.extend(rows))
    assert {result.entidade for result in results} == {"uf", "unknown"}
    assert written == results


def test_writer_failure_does_not_abort_validation():
    results = run_quality_checks({"uf": pd.DataFrame({"ano": [2024]})}, writer=lambda _: (_ for _ in ()).throw(RuntimeError("offline")))
    assert results


def test_entity_hook_validates_deduped_frame():
    tabela = pa.table({"ano": [2024, 2024], "sigla_uf": ["SP", "SP"], "serie": ["2", "2"], "rede": ["0", "0"]})
    results = run_entity_quality_checks("uf", tabela, writer=lambda _: None)
    assert results
    assert all(r.entidade == "uf" for r in results)
    duplicidade = [r for r in results if r.check == "duplicidade"]
    assert duplicidade and not duplicidade[0].passou


def test_run_all_reads_current_silver_state(monkeypatch):
    from silver import reader as silver_reader

    monkeypatch.setattr(silver_reader, "read_entity", lambda entidade: pa.table({"ano": [2024]}))
    monkeypatch.setattr(silver_reader, "read_scd2_table_raw", lambda entidade: None)

    written = []
    results = run_all_quality_checks(writer=lambda rows: written.extend(rows))
    entidades = {r.entidade for r in results}
    assert {"uf", "municipio", "alunos"} <= entidades
    assert written == results


def test_run_all_isolates_silver_read_failure(monkeypatch):
    from silver import reader as silver_reader

    def explode(entidade):
        raise RuntimeError("gcs offline")

    monkeypatch.setattr(silver_reader, "read_entity", explode)
    monkeypatch.setattr(silver_reader, "read_scd2_table_raw", lambda entidade: pa.table({"ano": [2024]}))

    results = run_all_quality_checks(writer=lambda _: None)
    read_failures = [r for r in results if r.check == "read"]
    assert {r.entidade for r in read_failures} == {"uf", "municipio", "alunos"}
    assert all(r.severidade == "CRITICA" and not r.passou for r in read_failures)
    # Metas continuaram sendo validadas apesar da falha das regulares.
    assert any(r.entidade == "meta_alfabetizacao_uf" and r.check != "read" for r in results)


def test_writer_retries_with_backoff(monkeypatch):
    from quality import writer as quality_writer

    attempts = []

    class FlakyClient:
        def insert_rows_json(self, table, rows, timeout=None):
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("transient")
            return []

    monkeypatch.setattr(quality_writer.time, "sleep", lambda _: None)
    results = [type("R", (), {})()]
    from quality.translate import QualityResult
    result = QualityResult("id", "check", "uf", "Validade", True, 1.0, 1.0, "AVISO", 0)

    outcome = quality_writer.write_results([result], client=FlakyClient())
    assert outcome == []
    assert len(attempts) == 3
