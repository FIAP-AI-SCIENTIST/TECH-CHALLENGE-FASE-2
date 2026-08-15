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
            result = get_dicionario("uf", "rede")

        assert result == {"1": "Federal", "2": "Estadual"}

    def test_query_filters_by_tabela_and_coluna(self):
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            get_dicionario("alunos", "presenca")

        sql = mock_client_cls.return_value.query.call_args.args[0]
        assert "id_tabela = 'alunos'" in sql
        assert "nome_coluna = 'presenca'" in sql


class TestGetDiretorioUf:
    """Verifica montagem do mapa sigla -> nome da UF."""

    def test_builds_uf_name_map(self):
        mock_rows = [{"sigla": "SP", "nome": "São Paulo"}, {"sigla": "RJ", "nome": "Rio de Janeiro"}]
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            result = get_diretorio_uf()

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
            result = get_diretorio_municipio()

        assert result == {
            "3550308": {
                "nome": "São Paulo",
                "sigla_uf": "SP",
                "nome_regiao": "Sudeste",
                "capital_uf": 1,
            }
        }

    def test_only_selects_decided_subset_of_columns(self):
        """Regressão: só o subconjunto decidido em source-schemas.md, não o diretório inteiro."""
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            get_diretorio_municipio()

        sql = mock_client_cls.return_value.query.call_args.args[0]
        for col in ("id_municipio", "nome", "sigla_uf", "nome_regiao", "capital_uf"):
            assert col in sql
        for fora_de_escopo in ("id_regiao_saude", "centroide", "ddd"):
            assert fora_de_escopo not in sql
