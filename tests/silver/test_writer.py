"""Testes do módulo silver.writer — escrita de partições/tabelas Parquet no GCS."""

from unittest.mock import MagicMock, patch

import pyarrow as pa

from silver.writer import (
    clear_entity,
    scd2_path,
    write_entity,
    write_scd2_table,
)


def _make_table(num_rows: int = 5) -> pa.Table:
    return pa.table({"ano": [2023] * num_rows, "sigla_uf": ["SP"] * num_rows})


class TestScd2Path:
    def test_no_partition_key(self):
        assert scd2_path("meta_alfabetizacao_uf") == "silver/meta_alfabetizacao_uf/data.parquet"


class TestClearEntity:
    def test_deletes_blobs_under_prefix(self):
        with patch("silver.writer.storage.Client") as mock_client_cls, \
             patch("silver.writer._delete_blobs_under_prefix") as mock_delete:
            clear_entity("uf", "ano=2023")

        mock_delete.assert_called_once()
        args = mock_delete.call_args.args
        assert args[2] == "silver/uf/ano=2023/"


class TestWriteEntity:
    def test_returns_num_rows_and_uploads(self):
        table = _make_table(7)
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        with patch("silver.writer.storage.Client") as mock_client_cls, \
             patch("silver.writer._upload_blob") as mock_upload:
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            rows = write_entity("uf", "ano=2023", table)

        assert rows == 7
        mock_bucket.blob.assert_called_once_with("silver/uf/ano=2023/part-0.parquet")
        mock_upload.assert_called_once()


class TestWriteScd2Table:
    def test_writes_to_scd2_path(self):
        table = _make_table(3)
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        with patch("silver.writer.storage.Client") as mock_client_cls, \
             patch("silver.writer._upload_blob") as mock_upload:
            mock_client_cls.return_value.bucket.return_value = mock_bucket
            rows = write_scd2_table("meta_alfabetizacao_uf", table)

        assert rows == 3
        mock_bucket.blob.assert_called_once_with("silver/meta_alfabetizacao_uf/data.parquet")
        mock_upload.assert_called_once()
