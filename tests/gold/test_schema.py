"""Testes do módulo gold.schema — DDL declarativo de partição/clustering/constraints."""

from unittest.mock import MagicMock, patch

from config import get_settings
from gold import schema as gold_schema
from gold.schema import TABLE_DDL, ensure_table, run_ddl

DIMS = [
    "dim_uf",
    "dim_municipio",
    "dim_rede",
    "dim_serie",
    "dim_tempo",
]

FATOS_COM_ANO = [
    "fact_indicador_uf",
    "fact_indicador_municipio",
    "fact_alunos",
    "fact_alfabetizacao_municipio",
    "fact_meta_resultado_brasil",
    "fact_meta_resultado_uf",
    "fact_meta_resultado_municipio",
]

# FKs declaradas por fato: (coluna no fato, dimensão referenciada).
FKS_POR_FATO = {
    "fact_indicador_uf": [("sk_uf", "dim_uf"), ("sk_serie", "dim_serie"), ("sk_rede", "dim_rede"), ("sk_tempo", "dim_tempo")],
    "fact_indicador_municipio": [("sk_municipio", "dim_municipio"), ("sk_serie", "dim_serie"), ("sk_rede", "dim_rede"), ("sk_tempo", "dim_tempo")],
    "fact_alunos": [("sk_municipio", "dim_municipio"), ("sk_serie", "dim_serie"), ("sk_rede", "dim_rede"), ("sk_tempo", "dim_tempo")],
    "fact_alfabetizacao_municipio": [("sk_municipio", "dim_municipio"), ("sk_serie", "dim_serie"), ("sk_rede", "dim_rede"), ("sk_tempo", "dim_tempo")],
    "fact_meta_resultado_brasil": [("sk_rede", "dim_rede"), ("sk_tempo", "dim_tempo")],
    "fact_meta_resultado_uf": [("sk_uf", "dim_uf"), ("sk_rede", "dim_rede"), ("sk_tempo", "dim_tempo")],
    "fact_meta_resultado_municipio": [("sk_municipio", "dim_municipio"), ("sk_rede", "dim_rede"), ("sk_tempo", "dim_tempo")],
}


class TestTableDdl:
    """Asserts estruturais sobre o registry — o DDL é dado, não lógica."""

    def test_registry_covers_exactly_dims_and_facts(self):
        assert sorted(TABLE_DDL) == sorted(DIMS + FATOS_COM_ANO)

    def test_every_table_has_create_if_not_exists(self):
        for nome, ddl in TABLE_DDL.items():
            assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS `{get_settings().project_id}.{get_settings().dataset_id}.{nome}`")

    def test_every_fact_has_integer_range_partition_on_ano(self):
        for nome in FATOS_COM_ANO:
            # Assinatura real do BigQuery: RANGE_BUCKET(valor, ARRAY) — a forma
            # de 4 argumentos é inválida (erro 400 confirmado em deploy real).
            assert "PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2016, 2031, 1))" in TABLE_DDL[nome], nome

    def test_dims_have_primary_key_not_enforced(self):
        for nome in DIMS:
            sk = f"sk_{nome.removeprefix('dim_')}"
            ddl = TABLE_DDL[nome]
            assert f"{sk} INT64 NOT NULL" in ddl, nome
            assert f"PRIMARY KEY ({sk}) NOT ENFORCED" in ddl, nome

    def test_dims_sem_particao_nem_cluster(self):
        """Dimensões são minúsculas (27 UFs, ~5,6 mil municípios) — partição e
        clustering seriam cerimônia sem efeito de custo."""
        for nome in DIMS:
            assert "PARTITION BY" not in TABLE_DDL[nome], nome
            assert "CLUSTER BY" not in TABLE_DDL[nome], nome

    def test_every_fact_declares_its_foreign_keys(self):
        for fato, fks in FKS_POR_FATO.items():
            ddl = TABLE_DDL[fato]
            for coluna, dim in fks:
                esperado = (
                    f"FOREIGN KEY ({coluna}) REFERENCES "
                    f"`{get_settings().project_id}.{get_settings().dataset_id}.{dim}`({coluna}) NOT ENFORCED"
                )
                assert esperado in ddl, f"{fato}: {coluna}"

    def test_facts_preserve_natural_keys_as_queryable_attributes(self):
        """Chaves naturais continuam colunas do fato — consulta direta sem join."""
        assert "sigla_uf STRING" in TABLE_DDL["fact_indicador_uf"]
        assert "id_municipio STRING" in TABLE_DDL["fact_indicador_municipio"]
        assert "id_municipio STRING" in TABLE_DDL["fact_alfabetizacao_municipio"]
        assert "ano INT64" in TABLE_DDL["fact_meta_resultado_brasil"]

    def test_cluster_por_chave_territorial(self):
        assert "CLUSTER BY sigla_uf" in TABLE_DDL["fact_indicador_uf"]
        assert "CLUSTER BY sigla_uf" in TABLE_DDL["fact_meta_resultado_uf"]
        assert "CLUSTER BY id_municipio" in TABLE_DDL["fact_indicador_municipio"]
        assert "CLUSTER BY id_municipio" in TABLE_DDL["fact_meta_resultado_municipio"]
        assert "CLUSTER BY id_municipio" in TABLE_DDL["fact_alfabetizacao_municipio"]
        assert "CLUSTER BY id_municipio" in TABLE_DDL["fact_alunos"]

    def test_meta_resultado_brasil_sem_cluster(self):
        """Chave `rede` tem cardinalidade ~4 — clustering não ganha nada."""
        assert "CLUSTER BY" not in TABLE_DDL["fact_meta_resultado_brasil"]

    def test_fact_alfabetizacao_municipio_tem_medidas_de_meta_e_resultado(self):
        """O fato integrado carrega meta e resultado na mesma linha."""
        ddl = TABLE_DDL["fact_alfabetizacao_municipio"]
        for coluna in ("taxa_alfabetizacao FLOAT64", "meta_indicador FLOAT64",
                       "gap_pontos FLOAT64", "atingiu_meta BOOL",
                       "percentual_participacao FLOAT64", "nivel_alfabetizacao INT64"):
            assert coluna in ddl

    def test_dim_tempo_tem_atributos_de_trajetoria(self):
        ddl = TABLE_DDL["dim_tempo"]
        for coluna in ("ano INT64", "decada INT64", "ano_tem_meta BOOL", "anos_para_meta_final INT64"):
            assert coluna in ddl


class TestEnsureTable:
    def test_runs_ddl_for_registry_table(self):
        with patch("gold.schema.bigquery.Client"):
            client = MagicMock()
            ensure_table(client, "fact_indicador_uf")
        client.query.assert_called_once()
        assert client.query.call_args.args[0] == TABLE_DDL["fact_indicador_uf"]

    def test_runs_ddl_for_dim_when_table_does_not_exist(self):
        """Dim nova (não existe ainda) é criada com o DDL completo (com PK)."""
        with patch("gold.schema.bigquery.Client"):
            client = MagicMock()
            client.get_table.side_effect = Exception("NotFound")
            ensure_table(client, "dim_uf")
        client.query.assert_called_once()
        assert client.query.call_args.args[0] == TABLE_DDL["dim_uf"]

    def test_evolves_existing_dim_without_pk(self):
        """Dim que já existe sem PK (criada antes das constraints) recebe a PK
        via ALTER TABLE — senão os fatos falham ao declarar FK."""
        with patch("gold.schema.bigquery.Client"):
            client = MagicMock()
            client.get_table.return_value = MagicMock(table_constraints=None)
            ensure_table(client, "dim_uf")
        client.query.assert_called_once()
        sql = client.query.call_args.args[0]
        assert sql.startswith("ALTER TABLE")
        assert "ADD PRIMARY KEY (sk_uf) NOT ENFORCED" in sql

    def test_noop_for_dim_that_already_has_pk(self):
        """Dim já com PK não é tocada — idempotente."""
        with patch("gold.schema.bigquery.Client"):
            client = MagicMock()
            client.get_table.return_value = MagicMock(table_constraints=[MagicMock()])
            ensure_table(client, "dim_uf")
        client.query.assert_not_called()

    def test_noop_for_table_outside_registry(self):
        client = MagicMock()
        ensure_table(client, "data_quality_log")
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
