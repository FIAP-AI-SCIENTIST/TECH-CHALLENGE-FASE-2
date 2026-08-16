"""Testes do módulo quality.gold_reader — leitura de coluna de fato/dimensão da Gold."""

from unittest.mock import MagicMock, patch

from quality.gold_reader import MAX_BYTES_BILLED, read_column


class TestReadColumn:
    """Verifica a leitura de uma coluna via BigQuery, sem DISTINCT."""

    def test_returns_values_and_bytes_processed(self):
        mock_rows = [{"sigla_uf": "SP"}, {"sigla_uf": "SP"}, {"sigla_uf": "RJ"}]
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("quality.gold_reader.bigquery.Client") as mock_client_cls:
            mock_job = mock_client_cls.return_value.query.return_value
            mock_job.result.return_value = mock_rows_iter
            mock_job.total_bytes_processed = 512

            values, bytes_processed = read_column("fact_indicador_uf", "sigla_uf")

        # Sem DISTINCT: linha repetida preservada — fração por linha, não por valor distinto.
        assert values == ["SP", "SP", "RJ"]
        assert bytes_processed == 512

    def test_sql_selects_only_requested_column_without_distinct(self):
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("quality.gold_reader.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            mock_client_cls.return_value.query.return_value.total_bytes_processed = 0

            read_column("dim_uf", "sigla_uf")

            sql = mock_client_cls.return_value.query.call_args[0][0]

        assert "SELECT sigla_uf FROM" in sql
        assert "DISTINCT" not in sql
        assert "dim_uf" in sql


class TestBytesCap:
    """_do_query aplica o cap de 10 GB, mesmo padrão de extraction/silver.reference."""

    def test_applies_maximum_bytes_billed(self):
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("quality.gold_reader.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            mock_client_cls.return_value.query.return_value.total_bytes_processed = 0

            read_column("fact_alunos", "id_municipio")

            _, kwargs = mock_client_cls.return_value.query.call_args
            assert kwargs["job_config"].maximum_bytes_billed == MAX_BYTES_BILLED == 10 * 2**30


class TestRetry:
    """Falha transitória é reexecutada via with_retry antes de propagar."""

    def test_retries_on_transient_failure(self, monkeypatch):
        from quality import gold_reader as gold_reader_module

        monkeypatch.setattr(gold_reader_module.time, "sleep", lambda _: None)
        attempts = []
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        class FlakyClient:
            def query(self, sql, job_config=None):
                attempts.append(1)
                if len(attempts) < 3:
                    raise RuntimeError("transient")
                job = MagicMock()
                job.result.return_value = mock_rows_iter
                job.total_bytes_processed = 0
                return job

        with patch("quality.gold_reader.bigquery.Client", return_value=FlakyClient()):
            values, _ = read_column("dim_municipio", "id_municipio")

        assert values == []
        assert len(attempts) == 3
