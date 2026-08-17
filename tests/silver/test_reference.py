"""Testes do módulo silver.reference — extração de tabelas de referência do BigQuery."""

from unittest.mock import MagicMock, patch

from silver.reference import (
    get_atlas_idhm,
    get_dicionario,
    get_diretorio_municipio,
    get_diretorio_uf,
    merge_idhm_into_diretorio,
)


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


class TestGetAtlasIdhm:
    """Verifica montagem do mapa de IDHM do Atlas do Desenvolvimento Humano."""

    def test_builds_idhm_map_with_descriptive_names(self):
        mock_rows = [
            {"id_municipio": "3550308", "idhm": 0.805, "idhm_e": 0.75, "idhm_l": 0.87, "idhm_r": 0.80},
        ]
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter(mock_rows)

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            result, _bytes = get_atlas_idhm()

        assert result == {
            "3550308": {
                "idhm": 0.805,
                "idhm_educacao": 0.75,
                "idhm_renda": 0.80,
                "idhm_longevidade": 0.87,
            }
        }

    def test_defaults_to_2010_census(self):
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            get_atlas_idhm()  # retorno ignorado — teste inspeciona o SQL

        sql = mock_client_cls.return_value.query.call_args.args[0]
        assert "ano = 2010" in sql

    def test_accepts_explicit_census_year(self):
        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            mock_client_cls.return_value.query.return_value.result.return_value = mock_rows_iter
            get_atlas_idhm(ano=1991)

        sql = mock_client_cls.return_value.query.call_args.args[0]
        assert "ano = 1991" in sql

    def test_returns_bytes_processed_and_applies_cap(self):
        from silver.reference import MAX_BYTES_BILLED

        mock_rows_iter = MagicMock()
        mock_rows_iter.__iter__ = lambda self: iter([])

        with patch("silver.reference.bigquery.Client") as mock_client_cls:
            job = mock_client_cls.return_value.query.return_value
            job.result.return_value = mock_rows_iter
            job.total_bytes_processed = 999
            _result, total_bytes = get_atlas_idhm()

        assert total_bytes == 999
        _, kwargs = mock_client_cls.return_value.query.call_args
        assert kwargs["job_config"].maximum_bytes_billed == MAX_BYTES_BILLED


class TestMergeIdhmIntoDiretorio:
    """Verifica a fusão (pura, sem I/O) do Atlas no diretório de município."""

    def test_merges_by_normalized_id(self):
        diretorio = {
            "3550308": {"nome": "São Paulo", "sigla_uf": "SP", "nome_regiao": "Sudeste", "capital_uf": 1},
        }
        atlas = {
            "3550308": {"idhm": 0.805, "idhm_educacao": 0.75, "idhm_renda": 0.80, "idhm_longevidade": 0.87},
        }

        fundido = merge_idhm_into_diretorio(diretorio, atlas)

        assert fundido["3550308"]["nome"] == "São Paulo"  # dado territorial preservado
        assert fundido["3550308"]["idhm"] == 0.805
        assert fundido["3550308"]["idhm_educacao"] == 0.75

    def test_id_format_mismatch_still_merges(self):
        """Diretório e Atlas podem divergir na formatação do id_municipio
        (padding de zero) — o merge normaliza os dois lados antes de casar."""
        diretorio = {
            "0550308": {"nome": "Cidade X", "sigla_uf": "SP", "nome_regiao": "Sudeste", "capital_uf": 0},
        }
        atlas = {
            "550308": {"idhm": 0.7, "idhm_educacao": 0.6, "idhm_renda": 0.7, "idhm_longevidade": 0.8},
        }

        fundido = merge_idhm_into_diretorio(diretorio, atlas)

        assert fundido["0550308"]["idhm"] == 0.7

    def test_municipio_without_atlas_coverage_gets_none_not_dropped(self):
        """Município criado depois do Censo 2010 não tem linha no Atlas — sai
        com IDHM None, não é descartado do diretório (nunca descarte silencioso)."""
        diretorio = {
            "9999999": {"nome": "Município Novo", "sigla_uf": "SP", "nome_regiao": "Sudeste", "capital_uf": 0},
        }

        fundido = merge_idhm_into_diretorio(diretorio, atlas_idhm={})

        assert "9999999" in fundido
        assert fundido["9999999"]["idhm"] is None
        assert fundido["9999999"]["idhm_educacao"] is None
