"""Testes do módulo gold.writer — carga de tabelas Gold no BigQuery."""

from unittest.mock import MagicMock, patch

import pyarrow as pa

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
        assert args[2] == "useful-space-277919.alfabetizacao_analytics.dim_uf"

    def test_load_uses_write_truncate(self):
        table = pa.table({"sigla_uf": ["SP"]})

        with patch("gold.writer.bigquery.Client") as mock_client_cls, \
             patch("gold.writer._load") as mock_load:
            mock_client_cls.return_value = MagicMock()
            write_table("dim_uf", table)

        job_config = mock_load.call_args.args[3]
        assert job_config.write_disposition == "WRITE_TRUNCATE"
