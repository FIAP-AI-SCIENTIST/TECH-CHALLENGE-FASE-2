"""Testes do módulo gold.writer — carga de tabelas Gold no BigQuery."""

from unittest.mock import MagicMock, patch

import pyarrow as pa

from config import get_settings
from gold.writer import write_table


class TestWriteTable:
    def test_returns_num_rows_and_loads(self):
        table = pa.table({"sigla_uf": ["SP", "RJ"], "nome": ["São Paulo", "Rio de Janeiro"]})

        with patch("gold.writer.bigquery.Client") as mock_client_cls, \
             patch("gold.writer._load") as mock_load:
            mock_client_cls.return_value = MagicMock()
            rows = write_table("dim_uf", table)

        assert rows == 2
        mock_load.assert_called_once()
        args = mock_load.call_args.args
        settings = get_settings()
        assert args[2] == f"{settings.project_id}.{settings.dataset_id}.dim_uf"

    def test_load_uses_write_truncate(self):
        table = pa.table({"sigla_uf": ["SP"]})

        with patch("gold.writer.bigquery.Client") as mock_client_cls, \
             patch("gold.writer._load") as mock_load:
            mock_client_cls.return_value = MagicMock()
            write_table("dim_uf", table)

        job_config = mock_load.call_args.args[3]
        assert job_config.write_disposition == "WRITE_TRUNCATE"

    def test_calls_ensure_table_before_load(self):
        """Toda escrita passa pelo ensure_table (DDL de partição/clustering)
        antes do load — para dim_* é no-op, para fact_* cria a tabela se ausente."""
        table = pa.table({"ano": [2023], "sigla_uf": ["SP"]})

        with patch("gold.writer.bigquery.Client"), \
             patch("gold.writer._load"), \
             patch("gold.writer.gold_schema.ensure_table") as mock_ensure:
            write_table("fact_indicador_uf", table)

        mock_ensure.assert_called_once()
        assert mock_ensure.call_args.args[1] == "fact_indicador_uf"
