"""DDL declarativo das tabelas Gold — particionamento e clustering para custo de consulta.

Registry estático (mesmo padrão de `quality/rules.py` e `extraction.ENTITY_TABLE_MAP`):
o DDL é dado, não lógica — testável por assert estrutural, sem executar nada.

As tabelas Gold continuam pipeline-managed (fora do Terraform): `gold.writer`
chama `ensure_table` antes do
load de cada tabela, e o `CREATE TABLE IF NOT EXISTS` garante idempotência sem lock.
Partição/clustering sobrevivem ao `WRITE_TRUNCATE` do load job (ele trunca dados,
não a definição da tabela). Dimensões (`dim_*`) ficam fora do registry — tabelas
minúsculas de referência, onde a otimização seria cerimônia sem efeito.
"""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo

from google.cloud import bigquery

from common.retry import with_retry

PROJECT_ID = "useful-space-277919"
DATASET_ID = "alfabetizacao_analytics"
TIMEOUT_SECONDS = 30

# Bounds 2016-2031: do primeiro ano da fonte (2016) ao ano final da meta nacional
# (2030), com folga nos dois lados. Ano fora do range cai na partição
# __UNPARTITIONED__ — sinal de bug de dados, ainda consultável.
_PARTITION_ANO = "PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2016, 2031, 1))"

_COLUNAS_MEDIDAS_INDICADOR = """  taxa_alfabetizacao FLOAT64,
  media_portugues FLOAT64,
  proporcao_aluno_nivel_0 FLOAT64,
  proporcao_aluno_nivel_1 FLOAT64,
  proporcao_aluno_nivel_2 FLOAT64,
  proporcao_aluno_nivel_3 FLOAT64,
  proporcao_aluno_nivel_4 FLOAT64,
  proporcao_aluno_nivel_5 FLOAT64,
  proporcao_aluno_nivel_6 FLOAT64,
  proporcao_aluno_nivel_7 FLOAT64,
  proporcao_aluno_nivel_8 FLOAT64"""

_COLUNAS_META_RESULTADO = """  ano INT64,
  taxa_alfabetizacao FLOAT64,
  meta_indicador FLOAT64,
  gap_pontos FLOAT64,
  atingiu_meta BOOL,
  percentual_participacao FLOAT64,
  valid_from INT64,
  valid_to INT64,
  is_current BOOL"""


def _ddl(nome: str, colunas: str, cluster: str | None = None) -> str:
    cluster_sql = f"\nCLUSTER BY {cluster}" if cluster else ""
    return (
        f"CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{nome}` (\n"
        f"{colunas}\n"
        f")\n{_PARTITION_ANO}{cluster_sql}"
    )


TABLE_DDL: dict[str, str] = {
    "fact_indicador_uf": _ddl(
        "fact_indicador_uf",
        "  ano INT64,\n  sigla_uf STRING,\n  serie STRING,\n  rede STRING,\n" + _COLUNAS_MEDIDAS_INDICADOR,
        cluster="sigla_uf",
    ),
    "fact_indicador_municipio": _ddl(
        "fact_indicador_municipio",
        "  ano INT64,\n  id_municipio STRING,\n  serie STRING,\n  rede STRING,\n" + _COLUNAS_MEDIDAS_INDICADOR,
        cluster="id_municipio",
    ),
    "fact_alunos": _ddl(
        "fact_alunos",
        """  ano INT64,
  id_municipio STRING,
  id_escola STRING,
  id_aluno STRING,
  caderno STRING,
  serie STRING,
  rede STRING,
  presenca STRING,
  preenchimento_caderno STRING,
  alfabetizado STRING,
  proficiencia FLOAT64,
  peso_aluno FLOAT64""",
        cluster="id_municipio",
    ),
    "fact_meta_resultado_brasil": _ddl(
        "fact_meta_resultado_brasil",
        "  rede STRING,\n" + _COLUNAS_META_RESULTADO,
        # Sem clustering: chave `rede` tem cardinalidade ~4.
    ),
    "fact_meta_resultado_uf": _ddl(
        "fact_meta_resultado_uf",
        "  sigla_uf STRING,\n  rede STRING,\n" + _COLUNAS_META_RESULTADO,
        cluster="sigla_uf",
    ),
    "fact_meta_resultado_municipio": _ddl(
        "fact_meta_resultado_municipio",
        "  id_municipio STRING,\n  rede STRING,\n" + _COLUNAS_META_RESULTADO,
        cluster="id_municipio",
    ),
}


@with_retry()
def run_ddl(client: bigquery.Client, sql: str) -> None:
    """Operação atômica: submete o DDL e aguarda a conclusão (com retry)."""
    client.query(sql, timeout=TIMEOUT_SECONDS).result(timeout=TIMEOUT_SECONDS)


def ensure_table(client: bigquery.Client, nome_tabela: str) -> None:
    """Cria `nome_tabela` com partição/clustering se estiver no registry.

    Idempotente (`IF NOT EXISTS`) — sem lock: DDL de criação não corrompe
    estado. Tabelas fora do registry (dim_*)
    seguem criadas pelo próprio load job, sem DDL prévio.
    """
    ddl = TABLE_DDL.get(nome_tabela)
    if ddl is None:
        return
    run_ddl(client, ddl)
