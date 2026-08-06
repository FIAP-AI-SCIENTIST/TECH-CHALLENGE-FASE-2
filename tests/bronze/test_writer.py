"""Testes do módulo bronze.writer — escrita de partições Parquet no GCS."""

import io
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from bronze.writer import (
    BUCKET_NAME,
    build_partition_path,
    write_partition,
)


class TestBuildPartitionPath:
    """Verifica construção de caminhos de partição."""

    def test_basic_path(self):
        result = build_partition_path("uf", 2023)
        assert result == "bronze/uf/ano=2023/"

    def test_different_entity_and_year(self):
        result = build_partition_path("municipio", 2024)
        assert result == "bronze/municipio/ano=2024/"

    def test_alunos_entity(self):
        result = build_partition_path("alunos", 2025)
        assert result == "bronze/alunos/ano=2025/"


class TestWritePartition:
    """Verifica escrita de partições com mock de GCS."""

    def _make_table(self, num_rows: int = 10) -> pa.Table:
        """Cria uma tabela PyArrow de teste."""
        return pa.Table.from_pydict({
            "ano": pa.array([2023] * num_rows),
            "valor": pa.array([float(i) for i in range(num_rows)]),
        })

    def test_returns_num_rows(self):
        table = self._make_table(num_rows=42)
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = []
            mock_blob = MagicMock()
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob

            rows_written = write_partition("uf", 2023, table)

        assert rows_written == 42

    def test_deletes_existing_blobs_before_upload(self):
        table = self._make_table()
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            old_blob = MagicMock()
            mock_bucket.list_blobs.return_value = [old_blob]
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            write_partition("uf", 2023, table)

        old_blob.delete.assert_called_once()

    def test_uploads_with_correct_name(self):
        table = self._make_table()
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = []
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            write_partition("uf", 2023, table)

        mock_bucket.blob.assert_called_once_with("bronze/uf/ano=2023/part-0.parquet")
        mock_blob.upload_from_string.assert_called_once()

    def test_overwrite_reuses_prefix(self):
        """Segunda chamada ao mesmo ano limpa e reescreve."""
        table = self._make_table(num_rows=5)
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = []
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            write_partition("uf", 2023, table)
            write_partition("uf", 2023, table)

        assert mock_blob.upload_from_string.call_count == 2
