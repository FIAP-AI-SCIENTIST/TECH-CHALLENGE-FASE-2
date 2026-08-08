"""Testes do módulo bronze.writer — escrita de partições Parquet no GCS."""

import io
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from bronze.writer import (
    BUCKET_NAME,
    build_partition_path,
    clear_partition,
    write_partition,
)


class TestBuildPartitionPath:
    """Verifica construção de caminhos de partição (chave genérica, já formatada)."""

    def test_basic_path(self):
        result = build_partition_path("uf", "ano=2023")
        assert result == "bronze/uf/ano=2023/"

    def test_different_entity_and_year(self):
        result = build_partition_path("municipio", "ano=2024")
        assert result == "bronze/municipio/ano=2024/"

    def test_alunos_entity(self):
        result = build_partition_path("alunos", "ano=2025")
        assert result == "bronze/alunos/ano=2025/"

    def test_date_ingestion_key(self):
        """Chave de particionamento por data (streaming), não apenas ano."""
        result = build_partition_path("alunos", "data_ingestao=2026-08-06")
        assert result == "bronze/alunos/data_ingestao=2026-08-06/"


class TestWritePartition:
    """Verifica escrita de partições com mock de GCS.

    write_partition NUNCA apaga nada — só escreve o arquivo do lote.
    Limpeza da partição é responsabilidade exclusiva de clear_partition,
    chamada pelo caller uma única vez por (entidade, chave) por execução.
    """

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
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            rows_written = write_partition("uf", "ano=2023", table, part_id="0")

        assert rows_written == 42

    def test_never_deletes_blobs(self):
        """write_partition não deve chamar list_blobs/delete — isso é do clear_partition."""
        table = self._make_table()
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            write_partition("uf", "ano=2023", table, part_id="0")

        mock_bucket.list_blobs.assert_not_called()

    def test_uploads_with_correct_name(self):
        table = self._make_table()
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            write_partition("uf", "ano=2023", table, part_id="0")

        mock_bucket.blob.assert_called_once_with("bronze/uf/ano=2023/part-0.parquet")
        mock_blob.upload_from_string.assert_called_once()

    def test_uploads_with_date_ingestion_key(self):
        """Regressão: escrita usando chave de data (streaming), não só ano."""
        table = self._make_table()
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            write_partition("alunos", "data_ingestao=2026-08-06", table, part_id="0")

        mock_bucket.blob.assert_called_once_with(
            "bronze/alunos/data_ingestao=2026-08-06/part-0.parquet"
        )

    def test_multiple_batches_write_separate_files(self):
        """Regressão do B1: lotes diferentes da mesma chave nunca se sobrescrevem."""
        table = self._make_table(num_rows=5)
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob

            write_partition("uf", "ano=2023", table, part_id="0")
            write_partition("uf", "ano=2023", table, part_id="1")

        assert mock_bucket.blob.call_args_list == [
            (("bronze/uf/ano=2023/part-0.parquet",),),
            (("bronze/uf/ano=2023/part-1.parquet",),),
        ]
        assert mock_blob.upload_from_string.call_count == 2

    def test_part_id_can_be_a_run_id(self):
        """O consumer de streaming nomeia o lote pelo run_id, não por índice —
        dois runs no mesmo dia precisam gerar arquivos distintos na partição."""
        table = self._make_table(num_rows=3)
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.blob.return_value = MagicMock()

            write_partition("uf", "data_ingestao=2026-08-08", table, part_id="run-a")
            write_partition("uf", "data_ingestao=2026-08-08", table, part_id="run-b")

        assert mock_bucket.blob.call_args_list == [
            (("bronze/uf/data_ingestao=2026-08-08/part-run-a.parquet",),),
            (("bronze/uf/data_ingestao=2026-08-08/part-run-b.parquet",),),
        ]


class TestClearPartition:
    """Verifica limpeza de partição (delete dos blobs existentes)."""

    def test_deletes_existing_blobs(self):
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            old_blob = MagicMock()
            mock_bucket.list_blobs.return_value = [old_blob]

            clear_partition("uf", "ano=2023")

        old_blob.delete.assert_called_once()

    def test_no_blobs_is_a_noop(self):
        with patch("bronze.writer.storage.Client") as mock_client_cls:
            mock_bucket = MagicMock()
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            mock_bucket.list_blobs.return_value = []

            clear_partition("uf", "ano=2023")  # não deve levantar exceção
