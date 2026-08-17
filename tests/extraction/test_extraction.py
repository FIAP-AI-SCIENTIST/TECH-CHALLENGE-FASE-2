"""Testes do módulo extraction.extraction — extração do BigQuery para Bronze."""

from collections import defaultdict
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import PreconditionFailed

import pytest

from extraction.extraction import (
    BATCH_SIZE,
    BATCH_THRESHOLD,
    ENTITY_TABLE_MAP,
    MAX_BYTES_BILLED,
    _do_query,
    _instantiate_records,
    compute_incremental_years,
    extract_entity,
    extract_full,
    extract_incremental,
    batched,
)
from contracts.models import UFRecord
from common.lock import LockHeldError, gcs_lock


@pytest.fixture(autouse=True)
def _mock_lock():
    """Evita que os testes existentes toquem o GCS real via o novo gcs_lock
    que envolve _run_extraction — comportamento do lock em si é testado à
    parte em TestExtractionLock."""
    with patch("extraction.extraction.gcs_lock") as mock_lock:
        mock_lock.return_value.__enter__ = MagicMock(return_value=None)
        mock_lock.return_value.__exit__ = MagicMock(return_value=False)
        yield mock_lock



class TestDoQuery:
    """Verifica retry na chamada real ao BigQuery (gap encontrado na revisão)."""

    def test_retries_on_transient_failure_then_succeeds(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_client.query.return_value.result.side_effect = [
            ConnectionError("transient"),
            mock_result,
        ]
        with patch("extraction.extraction.time"):
            rows, _bytes = _do_query(mock_client, "SELECT 1")
        assert rows is mock_result
        assert mock_client.query.return_value.result.call_count == 2

    def test_passes_explicit_timeout(self):
        mock_client = MagicMock()
        _do_query(mock_client, "SELECT 1")
        mock_client.query.return_value.result.assert_called_once()
        _, kwargs = mock_client.query.return_value.result.call_args
        assert kwargs.get("timeout") == 10

    def test_applies_maximum_bytes_billed_and_returns_bytes_processed(self):
        """Cap de custo de 10 GB na query + contador de bytes propagado."""
        mock_client = MagicMock()
        mock_client.query.return_value.total_bytes_processed = 12345
        _rows, total_bytes = _do_query(mock_client, "SELECT 1")
        assert total_bytes == 12345
        _, kwargs = mock_client.query.call_args
        assert kwargs["job_config"].maximum_bytes_billed == MAX_BYTES_BILLED

class TestComputeIncrementalYears:
    """Verifica cálculo de anos incrementais."""

    def test_returns_new_years_only(self):
        existing = {2023, 2024}
        source = {2023, 2024, 2025, 2026}
        result = compute_incremental_years(existing, source)
        assert result == [2025, 2026]

    def test_never_returns_existing_year(self):
        existing = {2023}
        source = {2023}
        result = compute_incremental_years(existing, source)
        assert result == []

    def test_returns_sorted(self):
        existing = {2023}
        source = {2023, 2026, 2024, 2025}
        result = compute_incremental_years(existing, source)
        assert result == [2024, 2025, 2026]

    def test_empty_existing(self):
        existing = set()
        source = {2023, 2024}
        result = compute_incremental_years(existing, source)
        assert result == [2023, 2024]


class TestBatched:
    """Verifica loteamento de iteráveis."""

    def test_correct_batch_size(self):
        data = list(range(10))
        batches = list(batched(iter(data), size=3))
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 3
        assert len(batches[3]) == 1

    def test_last_batch_smaller(self):
        data = list(range(7))
        batches = list(batched(iter(data), size=3))
        assert len(batches[-1]) == 1

    def test_concatenation_reconstructs_original(self):
        original = list(range(42))
        batches = list(batched(iter(original), size=10))
        reconstructed = [item for batch in batches for item in batch]
        assert reconstructed == original

    def test_empty_iterable(self):
        batches = list(batched(iter([]), size=5))
        assert batches == []

    def test_exact_division(self):
        data = list(range(9))
        batches = list(batched(iter(data), size=3))
        assert len(batches) == 3
        for batch in batches:
            assert len(batch) == 3

    def test_works_with_generator(self):
        def gen():
            for i in range(5):
                yield i

        batches = list(batched(gen(), size=2))
        assert len(batches) == 3
        assert len(batches[-1]) == 1


class TestInstantiateRecords:
    """Verifica isolamento de falha por linha na validação do contrato:
    uma linha fora da faixa é descartada sem derrubar o lote."""

    def test_isolates_bad_row(self, caplog):
        rows = [
            {"ano": 2023, "sigla_uf": "SP", "taxa_alfabetizacao": 86.21},
            {"ano": 2023, "sigla_uf": "RJ", "taxa_alfabetizacao": 150.0},  # fora da faixa 0-100
            {"ano": 2024, "sigla_uf": "MG", "taxa_alfabetizacao": 70.0},
        ]
        with caplog.at_level("WARNING", logger="pipeline"):
            instancias = _instantiate_records(rows, UFRecord)
        assert [i.sigla_uf for i in instancias] == ["SP", "MG"]
        assert "1 linha(s) descartada(s)" in caplog.text
        assert "RJ" in caplog.text  # linha rejeitada identificada no log

    def test_all_rows_invalid_returns_empty_without_raising(self):
        rows = [{"ano": 1999}, {"ano": 3000}]  # fora da banda de sanidade 2000-2100
        assert _instantiate_records(rows, UFRecord) == []

    def test_none_values_pass_range_constraints(self):
        """Campo Optional com ge/le aceita None — ausência de valor não é violação."""
        instancias = _instantiate_records([{"ano": None, "taxa_alfabetizacao": None}], UFRecord)
        assert len(instancias) == 1
        assert instancias[0].taxa_alfabetizacao is None


class TestExtractFull:
    """Verifica extração completa de entidades."""

    def test_calls_write_partition_per_year_group(self):
        mock_rows = [
            {"ano": ano, "sigla_uf": "SP"}
            for ano in [2023, 2024]
            for _ in range(3)
        ]

        mock_rows_iter = MagicMock()
        mock_rows_iter.total_rows = 6
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("extraction.extraction.bigquery.Client") as mock_bq_client:
            mock_bq_client.return_value.query.return_value.result.return_value = mock_rows_iter
            with patch("extraction.extraction.bronze_writer.write_partition") as mock_write, \
                 patch("extraction.extraction.bronze_writer.clear_partition") as mock_clear:
                mock_write.return_value = 3
                with patch("extraction.extraction.log_execution") as mock_log:
                    mock_run = MagicMock()
                    mock_log.return_value.__enter__ = lambda self: mock_run
                    mock_log.return_value.__exit__ = lambda self, *a: None

                    extract_full("uf")

        # Verifica que write_partition foi chamado e que cada ano foi limpo 1x
        assert mock_write.called
        assert mock_clear.call_count == 2
        assert {c.args for c in mock_clear.call_args_list} == {("uf", "ano=2023"), ("uf", "ano=2024")}

    def test_multi_batch_same_year_clears_once_writes_each_batch(self):
        """Regressão do B1: clear_partition 1x por ano, write_partition 1x por lote
        com part_id crescente — nenhum lote sobrescreve o anterior."""
        mock_rows = [{"ano": 2023, "sigla_uf": "SP"} for _ in range(5)]

        mock_rows_iter = MagicMock()
        mock_rows_iter.total_rows = 600_000  # acima do BATCH_THRESHOLD, força batching
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("extraction.extraction.BATCH_SIZE", 2):
            with patch("extraction.extraction.bigquery.Client") as mock_bq_client:
                mock_bq_client.return_value.query.return_value.result.return_value = mock_rows_iter
                with patch("extraction.extraction.bronze_writer.write_partition") as mock_write, \
                     patch("extraction.extraction.bronze_writer.clear_partition") as mock_clear:
                    mock_write.return_value = 2
                    with patch("extraction.extraction.log_execution") as mock_log:
                        mock_run = MagicMock()
                        mock_log.return_value.__enter__ = lambda self: mock_run
                        mock_log.return_value.__exit__ = lambda self, *a: None

                        extract_full("uf")

        # 5 linhas em lotes de 2 -> 3 lotes (2, 2, 1), todos do ano 2023
        mock_clear.assert_called_once_with("uf", "ano=2023")
        assert mock_write.call_count == 3
        part_ids = [call.kwargs["part_id"] for call in mock_write.call_args_list]
        assert part_ids == ["0", "1", "2"]

    def test_entity_not_in_map_raises(self):
        with patch("extraction.extraction.bigquery.Client"):
            with patch("extraction.extraction.log_execution") as mock_log:
                mock_run = MagicMock()
                mock_log.return_value.__enter__ = lambda self: mock_run
                mock_log.return_value.__exit__ = lambda self, *a: None
                with pytest.raises(KeyError):
                    extract_full("inexistente")


class TestExtractIncremental:
    """Verifica extração incremental."""

    def test_delegates_to_full_when_no_existing_years(self):
        with patch("extraction.extraction.bronze_reader.list_bronze_years") as mock_list:
            mock_list.return_value = set()
            with patch("extraction.extraction.extract_full") as mock_full:
                extract_incremental("uf")
                mock_full.assert_called_once_with("uf")


class TestExtractEntity:
    """Verifica ponto de entrada único."""

    def test_calls_full_when_no_existing_years(self):
        with patch("extraction.extraction.bronze_reader.list_bronze_years") as mock_list:
            mock_list.return_value = set()
            with patch("extraction.extraction.extract_full") as mock_full:
                with patch("extraction.extraction.extract_incremental") as mock_inc:
                    extract_entity("uf")
                    mock_full.assert_called_once()
                    mock_inc.assert_not_called()

    def test_calls_incremental_when_existing_years(self):
        with patch("extraction.extraction.bronze_reader.list_bronze_years") as mock_list:
            mock_list.return_value = {2023}
            with patch("extraction.extraction.extract_full") as mock_full:
                with patch("extraction.extraction.extract_incremental") as mock_inc:
                    extract_entity("uf")
                    mock_inc.assert_called_once()
                    mock_full.assert_not_called()



class TestExtractionLock:
    """Regressão: _run_extraction é protegida por lock contra execução concorrente
    da mesma entidade (duas pessoas do grupo rodando `make bronze` ao mesmo tempo)."""

    def test_concurrent_run_same_entity_raises_lock_held_error(self):
        mock_rows_iter = MagicMock()
        mock_rows_iter.total_rows = 1
        mock_rows_iter.__iter__ = lambda self: iter([{"ano": 2023, "sigla_uf": "SP"}])

        # Desfaz o mock global do gcs_lock desta classe para exercitar o lock de
        # verdade, só mockando o storage.Client subjacente (simula o objeto de
        # lock já existindo, como se outra execução já tivesse adquirido).
        with patch("common.lock.storage.Client") as mock_storage_client:
            mock_blob = MagicMock()
            mock_blob.upload_from_string.side_effect = PreconditionFailed("already exists")
            mock_blob.time_created = datetime.now(timezone.utc)  # lock fresco, não obsoleto
            mock_storage_client.return_value.bucket.return_value.blob.return_value = mock_blob

            with patch("extraction.extraction.bigquery.Client") as mock_bq_client:
                mock_bq_client.return_value.query.return_value.result.return_value = mock_rows_iter
                with patch("extraction.extraction.gcs_lock", new=gcs_lock):
                    with pytest.raises(LockHeldError):
                        extract_full("uf")