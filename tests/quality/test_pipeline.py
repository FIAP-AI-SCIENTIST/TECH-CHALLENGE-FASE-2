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
    _patch_gold_and_bronze(monkeypatch, bronze_rows={"uf": 1, "municipio": 1, "alunos": 1})

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
    _patch_gold_and_bronze(monkeypatch)

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


def _patch_gold_and_bronze(monkeypatch, *, bronze_rows=None, gold_columns=None):
    """Helper: mocka bronze_reader.count_partition_rows e gold_reader.read_column dentro do
    módulo quality.pipeline, sem tocar rede real."""
    from quality import pipeline as quality_pipeline

    bronze_rows = bronze_rows or {}
    gold_columns = gold_columns or {}

    def fake_count(entidade):
        if entidade in bronze_rows:
            return bronze_rows[entidade]
        return 1

    def fake_read_column(tabela, coluna):
        return gold_columns.get((tabela, coluna), []), 0

    monkeypatch.setattr(quality_pipeline.bronze_reader, "count_partition_rows", fake_count)
    monkeypatch.setattr(quality_pipeline.gold_reader, "read_column", fake_read_column)


def test_freshness_runs_for_all_six_entities(monkeypatch):
    from silver import reader as silver_reader

    monkeypatch.setattr(silver_reader, "read_entity", lambda entidade: pa.table({"ano": [2024]}))
    monkeypatch.setattr(silver_reader, "read_scd2_table_raw", lambda entidade: pa.table({"ano": [2024]}))
    _patch_gold_and_bronze(monkeypatch)

    results = run_all_quality_checks(writer=lambda _: None)
    frescor = {r.entidade for r in results if r.check == "frescor_dado"}
    assert frescor == {
        "uf", "municipio", "alunos",
        "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio",
    }


def test_reconciliation_only_for_regular_entities(monkeypatch):
    from silver import reader as silver_reader

    monkeypatch.setattr(silver_reader, "read_entity", lambda entidade: pa.table({"ano": [2024]}))
    monkeypatch.setattr(silver_reader, "read_scd2_table_raw", lambda entidade: pa.table({"ano": [2024]}))
    _patch_gold_and_bronze(monkeypatch, bronze_rows={"uf": 1, "municipio": 1, "alunos": 1})

    results = run_all_quality_checks(writer=lambda _: None)
    reconciliadas = {r.entidade for r in results if r.check == "reconciliacao"}
    assert reconciliadas == {"uf", "municipio", "alunos"}


def test_reconciliation_isolates_bronze_read_failure(monkeypatch):
    from silver import reader as silver_reader
    from quality import pipeline as quality_pipeline

    monkeypatch.setattr(silver_reader, "read_entity", lambda entidade: pa.table({"ano": [2024]}))
    monkeypatch.setattr(silver_reader, "read_scd2_table_raw", lambda entidade: pa.table({"ano": [2024]}))

    def explode_for_alunos(entidade):
        if entidade == "alunos":
            raise RuntimeError("gcs offline")
        return 1

    monkeypatch.setattr(quality_pipeline.bronze_reader, "count_partition_rows", explode_for_alunos)
    monkeypatch.setattr(quality_pipeline.gold_reader, "read_column", lambda tabela, coluna: ([], 0))

    results = run_all_quality_checks(writer=lambda _: None)
    read_failures = [r for r in results if r.check == "read" and r.entidade == "alunos"]
    assert read_failures and read_failures[0].severidade == "CRITICA" and not read_failures[0].passou
    # uf/municipio continuam reconciliadas apesar da falha isolada em alunos.
    reconciliadas = {r.entidade for r in results if r.check == "reconciliacao"}
    assert reconciliadas == {"uf", "municipio"}


def test_fk_check_runs_for_five_pairs_and_isolates_failure(monkeypatch):
    from silver import reader as silver_reader
    from quality import pipeline as quality_pipeline
    from quality.rules import FK_PAIRS

    monkeypatch.setattr(silver_reader, "read_entity", lambda entidade: pa.table({"ano": [2024]}))
    monkeypatch.setattr(silver_reader, "read_scd2_table_raw", lambda entidade: pa.table({"ano": [2024]}))
    monkeypatch.setattr(quality_pipeline.bronze_reader, "count_partition_rows", lambda entidade: 1)

    def fake_read_column(tabela, coluna):
        if tabela == "fact_alunos":
            raise RuntimeError("bigquery offline")
        return ["SP"], 0

    monkeypatch.setattr(quality_pipeline.gold_reader, "read_column", fake_read_column)

    results = run_all_quality_checks(writer=lambda _: None)
    fk_results = [r for r in results if r.check == "chave_relacionamento"]
    fk_failures = [r for r in results if r.check == "engine" and r.entidade == "fact_alunos"]

    fatos_com_par = {fato for fato, _, _, _ in FK_PAIRS if fato != "fact_alunos"}
    assert {r.entidade for r in fk_results} == fatos_com_par
    assert fk_failures and fk_failures[0].severidade == "CRITICA" and not fk_failures[0].passou
    assert len(fk_results) + 1 == len(FK_PAIRS)
