"""Testes do módulo bronze.reader — leitura de partições Parquet do GCS."""

import io
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bronze.reader import (
    BUCKET_NAME,
    list_bronze_years,
    parse_partition_path,
    read_partition,
)


class TestParsePartitionPath:
    """Verifica parsing de caminhos de partição."""

    def test_round_trip_with_writer(self):
        from bronze.writer import build_partition_path

        for entidade in ["uf", "municipio", "alunos"]:
            for ano in [2023, 2024, 2025]:
                path = build_partition_path(entidade, ano)
                parsed_entidade, parsed_ano = parse_partition_path(path)
                assert (parsed_entidade, parsed_ano) == (entidade, ano)

    def test_with_trailing_filename(self):
        result = parse_partition_path("bronze/uf/ano=2023/part-0.parquet")
        assert result == ("uf", 2023)

    def test_with_trailing_slash(self):
        result = parse_partition_path("bronze/municipio/ano=2024/")
        assert result == ("municipio", 2024)

    def test_invalid_path_raises(self):
        with pytest.raises(ValueError):
            parse_partition_path("invalid/path")


class TestListBronzeYears:
    """Verifica listagem de anos gravados no Bronze."""

    def test_returns_correct_years(self):
        mock_blob_2023 = MagicMock()
        mock_blob_2023.name = "bronze/uf/ano=2023/part-0.parquet"
        mock_blob_2024 = MagicMock()
        mock_blob_2024.name = "bronze/uf/ano=2024/part-0.parquet"

        with patch("bronze.reader.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = [mock_blob_2023, mock_blob_2024]

            years = list_bronze_years("uf")

        assert years == {2023, 2024}

    def test_returns_empty_set_when_no_blobs(self):
        with patch("bronze.reader.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = []

            years = list_bronze_years("uf")

        assert years == set()

    def test_returns_empty_on_bucket_not_found(self):
        from google.api_core.exceptions import NotFound

        with patch("bronze.reader.storage.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.side_effect = NotFound("not found")

            years = list_bronze_years("uf")

        assert years == set()

    def test_deduplicates_years(self):
        """Múltiplos blobs do mesmo ano retornam só uma vez."""
        blobs = []
        for i in range(3):
            blob = MagicMock()
            blob.name = f"bronze/uf/ano=2023/part-{i}.parquet"
            blobs.append(blob)

        with patch("bronze.reader.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = blobs

            years = list_bronze_years("uf")

        assert years == {2023}


class TestReadPartition:
    """Verifica leitura de partições Parquet."""

    def _make_parquet_bytes(self) -> bytes:
        """Gera bytes Parquet válidos."""
        table = pa.Table.from_pydict({
            "ano": pa.array([2023]),
            "valor": pa.array([1.0]),
        })
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()

    def test_reads_specific_year(self):
        parquet_bytes = self._make_parquet_bytes()
        mock_blob = MagicMock()
        mock_blob.name = "bronze/uf/ano=2023/part-0.parquet"
        mock_blob.download_as_bytes.return_value = parquet_bytes

        with patch("bronze.reader.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = [mock_blob]

            result = read_partition("uf", ano=2023)

        assert result.num_rows == 1

    def test_reads_all_years_when_no_ano(self):
        parquet_bytes = self._make_parquet_bytes()
        blobs = []
        for ano in [2023, 2024]:
            blob = MagicMock()
            blob.name = f"bronze/uf/ano={ano}/part-0.parquet"
            blob.download_as_bytes.return_value = parquet_bytes
            blobs.append(blob)

        with patch("bronze.reader.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = blobs

            result = read_partition("uf")

        # Concatena tabelas de 2 anos
        assert result.num_rows == 2

    def test_skips_non_parquet_files(self):
        mock_blob = MagicMock()
        mock_blob.name = "bronze/uf/ano=2023/metadata.txt"

        with patch("bronze.reader.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = [mock_blob]

            result = read_partition("uf", ano=2023)

        # Retorna tabela vazia
        assert isinstance(result, pa.Table)
