"""Testes do módulo gold.schema — DDL declarativo de partição/clustering."""

from unittest.mock import MagicMock, patch

from config import get_settings
from gold import schema as gold_schema
from gold.schema import TABLE_DDL, ensure_table, run_ddl

FATOS_COM_ANO = [
    "fact_indicador_uf",
    "fact_indicador_municipio",
    "fact_alunos",
    "fact_meta_resultado_brasil",
    "fact_meta_resultado_uf",
    "fact_meta_resultado_municipio",
]


class TestTableDdl:
    """Asserts estruturais sobre o registry — o DDL é dado, não lógica."""

    def test_registry_covers_exactly_the_six_facts(self):
        assert sorted(TABLE_DDL) == sorted(FATOS_COM_ANO)

    def test_every_fact_has_create_if_not_exists(self):
        for nome, ddl in TABLE_DDL.items():
            assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS `{get_settings().project_id}.{get_settings().dataset_id}.{nome}`")

    def test_every_fact_has_integer_range_partition_on_ano(self):
        for nome, ddl in TABLE_DDL.items():
            # Assinatura real do BigQuery: RANGE_BUCKET(valor, ARRAY) — a forma
            # de 4 argumentos é inválida (erro 400 confirmado em deploy real).
            assert "PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2016, 2031, 1))" in ddl, nome

    def test_cluster_por_chave_territorial(self):
        assert "CLUSTER BY sigla_uf" in TABLE_DDL["fact_indicador_uf"]
        assert "CLUSTER BY sigla_uf" in TABLE_DDL["fact_meta_resultado_uf"]
        assert "CLUSTER BY id_municipio" in TABLE_DDL["fact_indicador_municipio"]
        assert "CLUSTER BY id_municipio" in TABLE_DDL["fact_meta_resultado_municipio"]
        assert "CLUSTER BY id_municipio" in TABLE_DDL["fact_alunos"]

    def test_meta_resultado_brasil_sem_cluster(self):
        """Chave `rede` tem cardinalidade ~4 — clustering não ganha nada."""
        assert "CLUSTER BY" not in TABLE_DDL["fact_meta_resultado_brasil"]

    def test_dims_not_in_registry(self):
        """Dimensões são minúsculas (27 UFs, ~4 redes) — fora do DDL de partição."""
        for dim in ("dim_uf", "dim_municipio", "dim_rede", "dim_serie"):
            assert dim not in TABLE_DDL


class TestEnsureTable:
    def test_runs_ddl_for_registry_table(self):
        with patch("gold.schema.bigquery.Client"):
            client = MagicMock()
            ensure_table(client, "fact_indicador_uf")
        client.query.assert_called_once()
        assert client.query.call_args.args[0] == TABLE_DDL["fact_indicador_uf"]

    def test_noop_for_table_outside_registry(self):
        client = MagicMock()
        ensure_table(client, "dim_uf")
        client.query.assert_not_called()


class TestRunDdl:
    def test_retries_on_transient_failure_then_succeeds(self):
        client = MagicMock()
        client.query.return_value.result.side_effect = [ConnectionError("transient"), MagicMock()]
        with patch("gold.schema.time"):
            run_ddl(client, "CREATE TABLE IF NOT EXISTS x (a INT64)")
        assert client.query.return_value.result.call_count == 2

    def test_passes_timeout(self):
        client = MagicMock()
        run_ddl(client, "CREATE TABLE IF NOT EXISTS x (a INT64)")
        _, kwargs = client.query.call_args
        assert kwargs.get("timeout") == gold_schema.TIMEOUT_SECONDS