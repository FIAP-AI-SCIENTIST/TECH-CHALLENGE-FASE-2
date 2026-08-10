"""Testes do módulo silver.reader — leitura do estado SCD2."""

import io
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound

from silver.reader import read_scd2_table

_SCHEMA = pa.schema([
    pa.field("sigla_uf", pa.string()),
    pa.field("valid_from", pa.int64()),
    pa.field("valid_to", pa.int64()),
    pa.field("is_current", pa.bool_()),
])


class TestReadScd2Table:
    def test_returns_empty_table_with_schema_when_not_found(self):
        with patch("silver.reader.storage.Client") as mock_client_cls, \
             patch("silver.reader._download_blob", side_effect=NotFound("not found")):
            result = read_scd2_table("meta_alfabetizacao_uf", _SCHEMA)

        assert result.num_rows == 0
        assert result.schema == _SCHEMA

    def test_reads_existing_table(self):
        existing = pa.Table.from_pylist(
            [{"sigla_uf": "SP", "valid_from": 2023, "valid_to": None, "is_current": True}],
            schema=_SCHEMA,
        )
        buffer = io.BytesIO()
        pq.write_table(existing, buffer)
        content = buffer.getvalue()

        with patch("silver.reader.storage.Client") as mock_client_cls, \
             patch("silver.reader._download_blob", return_value=content):
            result = read_scd2_table("meta_alfabetizacao_uf", _SCHEMA)

        assert result.num_rows == 1
        assert result.column("sigla_uf").to_pylist() == ["SP"]
