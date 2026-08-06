"""Testes do módulo extraction.extraction — extração do BigQuery para Bronze."""

from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest

from extraction.extraction import (
    BATCH_SIZE,
    BATCH_THRESHOLD,
    ENTITY_TABLE_MAP,
    compute_incremental_years,
    extract_entity,
    extract_full,
    extract_incremental,
    batched,
)




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


class TestExtractFull:
    """Verifica extração completa de entidades."""

    def test_calls_write_partition_per_year_group(self):
        mock_rows = []
        for ano in [2023, 2024]:
            for _ in range(3):
                mock_row = MagicMock()
                mock_row.__iter__ = lambda self, a=[{"ano": ano, "sigla_uf": "SP"}]: iter(a.items())
                mock_row.__getitem__ = lambda self, key: {"ano": ano, "sigla_uf": "SP"}[key]
                mock_rows.append(mock_row)

        mock_rows_iter = MagicMock()
        mock_rows_iter.total_rows = 6
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("extraction.extraction.bigquery.Client") as mock_bq_client:
            mock_bq_client.return_value.query.return_value.result.return_value = mock_rows_iter
            with patch("extraction.extraction.bronze_writer.write_partition") as mock_write:
                mock_write.return_value = 3
                with patch("extraction.extraction.log_execution") as mock_log:
                    mock_run = MagicMock()
                    mock_log.return_value.__enter__ = lambda self: mock_run
                    mock_log.return_value.__exit__ = lambda self, *a: None

                    extract_full("uf")

        # Verifica que write_partition foi chamado
        assert mock_write.called

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
