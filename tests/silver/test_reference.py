"""Testes do módulo silver.reference — extração de tabelas de referência do BigQuery."""

from unittest.mock import MagicMock, patch

from silver.reference import get_dicionario, get_diretorio_municipio, get_diretorio_uf


class TestGetDicionario:
    """Verifica montagem do mapa de tradução de código."""

    def test_builds_translation_map(self):
        mock_rows = [
            {"chave": "1", "valor": "Federal"},
            {"chave": "2", "valor": "Estadual"},
        ]
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            result, _bytes = get_dicionario("uf", "rede")

        assert result == {"1": "Federal", "2": "Estadual"}

    def test_query_filters_by_tabela_and_coluna(self):
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            get_dicionario("alunos", "presenca")  # retorno ignorado — teste inspeciona o SQL

        sql = mock_client_cls.return_value.query.call_args.args[0]
        assert "id_tabela = 'alunos'" in sql
        assert "nome_coluna = 'presenca'" in sql


class TestBytesProcessados:
    """_do_query devolve (rows, total_bytes_processed) e aplica o cap de 10 GB."""

    def test_returns_bytes_processed_and_applies_cap(self):
        from silver.reference import MAX_BYTES_BILLED

        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            job = mock_client_cls.return_value.query.return_value
            job.result.return_value = mock_rows_iter
            job.total_bytes_processed = 777
            _result, total_bytes = get_diretorio_uf()

        assert total_bytes == 777
        _, kwargs = mock_client_cls.return_value.query.call_args
        assert kwargs["job_config"].maximum_bytes_billed == MAX_BYTES_BILLED


class TestGetDiretorioUf:
    """Verifica montagem do mapa sigla -> nome da UF."""

    def test_builds_uf_name_map(self):
        mock_rows = [{"sigla": "SP", "nome": "São Paulo"}, {"sigla": "RJ", "nome": "Rio de Janeiro"}]
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            result, _bytes = get_diretorio_uf()

        assert result == {"SP": "São Paulo", "RJ": "Rio de Janeiro"}


class TestGetDiretorioMunicipio:
    """Verifica montagem do mapa de enriquecimento territorial do município."""

    def test_builds_municipio_enrichment_map(self):
        mock_rows = [
            {
                "id_municipio": "3550308",
                "nome": "São Paulo",
                "sigla_uf": "SP",
                "nome_regiao": "Sudeste",
                "capital_uf": 1,
            }
        ]
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            result, _bytes = get_diretorio_municipio()

        assert result == {
            "3550308": {
                "nome": "São Paulo",
                "sigla_uf": "SP",
                "nome_regiao": "Sudeste",
                "capital_uf": 1,
            }
        }

    def test_only_selects_decided_subset_of_columns(self):
        """Regressão: só o subconjunto de colunas do domínio, não o diretório inteiro."""
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            get_diretorio_municipio()  # retorno ignorado — teste inspeciona o SQL

        sql = mock_client_cls.return_value.query.call_args.args[0]
        for col in ("id_municipio", "nome", "sigla_uf", "nome_regiao", "capital_uf"):
            assert col in sql
        for fora_de_escopo in ("id_regiao_saude", "centroide", "ddd"):
            assert fora_de_escopo not in sql
