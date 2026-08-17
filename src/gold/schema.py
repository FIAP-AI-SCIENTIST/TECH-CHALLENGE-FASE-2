"""DDL declarativo das tabelas Gold — particionamento, clustering e chaves.

Registry estático (mesmo padrão de `quality/rules.py` e `extraction.ENTITY_TABLE_MAP`):
o DDL é dado, não lógica — testável por assert estrutural, sem executar nada.

As tabelas Gold continuam pipeline-managed (fora do Terraform): `gold.writer`
chama `ensure_table` antes do load de cada tabela, e o `CREATE TABLE IF NOT
EXISTS` garante idempotência sem lock. Partição/clustering sobrevivem ao
`WRITE_TRUNCATE` do load job (ele trunca dados, não a definição da tabela) —
mas constraints NÃO: o load job regrava o schema a partir do Parquet e remove
PK/FK (verificado empiricamente). Por isso `gold.writer` chama
`ensure_constraints` DEPOIS do load, re-aplicando via ALTER TABLE o que o
load derrubou.

Modelo Kimball: toda dimensão declara `PRIMARY KEY (sk_*) NOT ENFORCED` e todo
fato declara `FOREIGN KEY (sk_*) REFERENCES dim_* NOT ENFORCED`. NOT ENFORCED
porque a integridade é garantida pela construção determinística (SK = função
pura da chave natural) e verificada pela camada de qualidade — o constraint
documenta o relacionamento para o otimizador e para quem consulta, sem custo
de enforcement. FK exige a tabela referenciada existente com PK, então
`gold.pipeline.run_gold` materializa as dimensões antes dos fatos.
"""

import time  # pyright: ignore[reportUnusedImport] - necessario para with_retry (common.retry) interceptar time.sleep neste modulo
from collections.abc import Mapping

from google.cloud import bigquery

from common.retry import with_retry
from config import get_settings

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


def _pk(coluna: str) -> str:
    return f"  PRIMARY KEY ({coluna}) NOT ENFORCED"


def _fk(coluna: str, dim: str, coluna_dim: str) -> str:
    settings = get_settings()
    return (
        f"  FOREIGN KEY ({coluna}) "
        f"REFERENCES `{settings.project_id}.{settings.dataset_id}.{dim}`({coluna_dim}) NOT ENFORCED"
    )


def _ddl(nome: str, colunas: str, constraints: list[str] | None = None, cluster: str | None = None, partition: bool = True) -> str:
    settings = get_settings()
    constraints_sql = ""
    if constraints:
        constraints_sql = ",\n" + ",\n".join(constraints)
    partition_sql = f"\n{_PARTITION_ANO}" if partition else ""
    cluster_sql = f"\nCLUSTER BY {cluster}" if cluster else ""
    return (
        f"CREATE TABLE IF NOT EXISTS `{settings.project_id}.{settings.dataset_id}.{nome}` (\n"
        f"{colunas}{constraints_sql}\n"
        f"){partition_sql}{cluster_sql}"
    )


# FKs territoriais/tempo/código compartilhadas pelos fatos — cada fato declara
# só as dimensões cujas chaves naturais ele carrega.
_FK_TEMPO = [("sk_tempo", "dim_tempo", "sk_tempo")]
_FK_UF = [("sk_uf", "dim_uf", "sk_uf")]
_FK_MUNICIPIO = [("sk_municipio", "dim_municipio", "sk_municipio")]
_FK_REDE = [("sk_rede", "dim_rede", "sk_rede")]
_FK_SERIE = [("sk_serie", "dim_serie", "sk_serie")]


def _fks(*pares: tuple[str, str, str]) -> list[str]:
    return [_fk(coluna, dim, coluna_dim) for coluna, dim, coluna_dim in pares]


# Registry estruturado das FKs por fato — mesma informação que vai no DDL,
# reusada por `ensure_constraints` para re-aplicar as FKs que o load job
# WRITE_TRUNCATE remove a cada escrita.
FACT_FKS: dict[str, list[tuple[str, str, str]]] = {
    "fact_indicador_uf": [*_FK_UF, *_FK_SERIE, *_FK_REDE, *_FK_TEMPO],
    "fact_indicador_municipio": [*_FK_MUNICIPIO, *_FK_SERIE, *_FK_REDE, *_FK_TEMPO],
    "fact_alunos": [*_FK_MUNICIPIO, *_FK_SERIE, *_FK_REDE, *_FK_TEMPO],
    "fact_alfabetizacao_municipio": [*_FK_TEMPO, *_FK_MUNICIPIO, *_FK_REDE, *_FK_SERIE],
    "fact_meta_resultado_brasil": [*_FK_REDE, *_FK_TEMPO],
    "fact_meta_resultado_uf": [*_FK_UF, *_FK_REDE, *_FK_TEMPO],
    "fact_meta_resultado_municipio": [*_FK_MUNICIPIO, *_FK_REDE, *_FK_TEMPO],
}


def _table_ddl() -> dict[str, str]:
    return {
        # --- Dimensões: PK declarada, sem partição/clustering (minúsculas) ---
        "dim_uf": _ddl(
            "dim_uf",
            "  sk_uf INT64 NOT NULL,\n  sigla_uf STRING,\n  nome STRING",
            constraints=[_pk("sk_uf")],
            partition=False,
        ),
        "dim_municipio": _ddl(
            "dim_municipio",
            "  sk_municipio INT64 NOT NULL,\n  id_municipio STRING,\n  nome STRING,\n"
            "  sigla_uf STRING,\n  nome_regiao STRING,\n  capital_uf INT64,\n"
            "  idhm FLOAT64,\n  idhm_educacao FLOAT64,\n  idhm_renda FLOAT64,\n  idhm_longevidade FLOAT64",
            constraints=[_pk("sk_municipio")],
            partition=False,
        ),
        "dim_rede": _ddl(
            "dim_rede",
            "  sk_rede INT64 NOT NULL,\n  rede STRING,\n  rede_desc STRING",
            constraints=[_pk("sk_rede")],
            partition=False,
        ),
        "dim_serie": _ddl(
            "dim_serie",
            "  sk_serie INT64 NOT NULL,\n  serie STRING,\n  serie_desc STRING",
            constraints=[_pk("sk_serie")],
            partition=False,
        ),
        "dim_tempo": _ddl(
            "dim_tempo",
            "  sk_tempo INT64 NOT NULL,\n  ano INT64,\n  decada INT64,\n"
            "  ano_tem_meta BOOL,\n  anos_para_meta_final INT64",
            constraints=[_pk("sk_tempo")],
            partition=False,
        ),
        # --- Fatos: partição por ano + clustering + FKs declaradas ---
        "fact_indicador_uf": _ddl(
            "fact_indicador_uf",
            "  ano INT64,\n  sigla_uf STRING,\n  serie STRING,\n  rede STRING,\n"
            + _COLUNAS_MEDIDAS_INDICADOR
            + ",\n  sk_uf INT64,\n  sk_serie INT64,\n  sk_rede INT64,\n  sk_tempo INT64",
            constraints=_fks(*FACT_FKS["fact_indicador_uf"]),
            cluster="sigla_uf",
        ),
        "fact_indicador_municipio": _ddl(
            "fact_indicador_municipio",
            "  ano INT64,\n  id_municipio STRING,\n  serie STRING,\n  rede STRING,\n"
            + _COLUNAS_MEDIDAS_INDICADOR
            + ",\n  sk_municipio INT64,\n  sk_serie INT64,\n  sk_rede INT64,\n  sk_tempo INT64",
            constraints=_fks(*FACT_FKS["fact_indicador_municipio"]),
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
  presenca BOOL,
  preenchimento_caderno BOOL,
  alfabetizado BOOL,
  proficiencia FLOAT64,
  peso_aluno FLOAT64,
  sk_municipio INT64,
  sk_serie INT64,
  sk_rede INT64,
  sk_tempo INT64""",
            constraints=_fks(*FACT_FKS["fact_alunos"]),
            cluster="id_municipio",
        ),
        "fact_alfabetizacao_municipio": _ddl(
            "fact_alfabetizacao_municipio",
            "  ano INT64,\n  id_municipio STRING,\n  rede STRING,\n  serie STRING,\n"
            + _COLUNAS_MEDIDAS_INDICADOR
            + """,
  meta_indicador FLOAT64,
  percentual_participacao FLOAT64,
  nivel_alfabetizacao INT64,
  gap_pontos FLOAT64,
  atingiu_meta BOOL,
  sk_tempo INT64,
  sk_municipio INT64,
  sk_rede INT64,
  sk_serie INT64""",
            constraints=_fks(*FACT_FKS["fact_alfabetizacao_municipio"]),
            cluster="id_municipio",
        ),
        "fact_meta_resultado_brasil": _ddl(
            "fact_meta_resultado_brasil",
            "  rede STRING,\n" + _COLUNAS_META_RESULTADO + ",\n  sk_rede INT64,\n  sk_tempo INT64",
            constraints=_fks(*FACT_FKS["fact_meta_resultado_brasil"]),
            # Sem clustering: chave `rede` tem cardinalidade ~4.
        ),
        "fact_meta_resultado_uf": _ddl(
            "fact_meta_resultado_uf",
            "  sigla_uf STRING,\n  rede STRING,\n" + _COLUNAS_META_RESULTADO
            + ",\n  sk_uf INT64,\n  sk_rede INT64,\n  sk_tempo INT64",
            constraints=_fks(*FACT_FKS["fact_meta_resultado_uf"]),
            cluster="sigla_uf",
        ),
        "fact_meta_resultado_municipio": _ddl(
            "fact_meta_resultado_municipio",
            "  id_municipio STRING,\n  rede STRING,\n" + _COLUNAS_META_RESULTADO
            + ",\n  sk_municipio INT64,\n  sk_rede INT64,\n  sk_tempo INT64",
            constraints=_fks(*FACT_FKS["fact_meta_resultado_municipio"]),
            cluster="id_municipio",
        ),
    }


class _TableDdl(Mapping[str, str]):
    """Registry lido sob demanda — project/dataset vêm de `get_settings()`, sem congelar no import."""

    def __getitem__(self, nome: str) -> str:
        return _table_ddl()[nome]

    def __iter__(self):
        return iter(_table_ddl())

    def __len__(self) -> int:
        return len(_table_ddl())


TABLE_DDL: Mapping[str, str] = _TableDdl()


@with_retry()
def run_ddl(client: bigquery.Client, sql: str) -> None:
    """Operação atômica: submete o DDL e aguarda a conclusão (com retry)."""
    client.query(sql, timeout=TIMEOUT_SECONDS).result(timeout=TIMEOUT_SECONDS)


def _table_has_primary_key(client: bigquery.Client, nome_tabela: str) -> bool | None:
    """True se a tabela existe e tem PK; False se existe sem PK; None se não existe."""
    settings = get_settings()
    table_ref = f"{settings.project_id}.{settings.dataset_id}.{nome_tabela}"
    try:
        tabela = client.get_table(table_ref)
    except Exception:
        return None
    constraints = tabela.table_constraints
    return bool(constraints and constraints.primary_key)


def _existing_fk_pairs(tabela: bigquery.Table) -> set[tuple[str, str]]:
    """{(coluna_no_fato, dim_referenciada)} das FKs presentes na tabela."""
    constraints = tabela.table_constraints
    if not constraints or not constraints.foreign_keys:
        return set()
    pares: set[tuple[str, str]] = set()
    for fk in constraints.foreign_keys:
        dim = fk.referenced_table.table_id
        for ref in fk.column_references:
            pares.add((ref.referencing_column, dim))
    return pares


def _add_primary_key(client: bigquery.Client, nome_tabela: str) -> None:
    """(Re)adiciona a PK de uma dim via ALTER TABLE.

    Necessário porque o load job WRITE_TRUNCATE remove a constraint a cada
    escrita — inclusive a de dims recém-criadas, cujo DDL declara PK. Sem PK
    na dim, os fatos falham ao declarar FK. Idempotente: se a PK já existir,
    o ALTER falha com "already exists" e é ignorado.
    """
    settings = get_settings()
    sk_col = f"sk_{nome_tabela.removeprefix('dim_')}"
    sql = (
        f"ALTER TABLE `{settings.project_id}.{settings.dataset_id}.{nome_tabela}` "
        f"ADD PRIMARY KEY ({sk_col}) NOT ENFORCED"
    )
    try:
        run_ddl(client, sql)
    except Exception as exc:
        if "already exists" in str(exc).lower():
            return
        raise


def _add_foreign_key(client: bigquery.Client, nome_tabela: str, coluna: str, dim: str, coluna_dim: str) -> None:
    """(Re)adiciona uma FK de fato via ALTER TABLE — mesmo motivo de
    `_add_primary_key`: o load job WRITE_TRUNCATE remove as FKs a cada escrita.
    """
    settings = get_settings()
    sql = (
        f"ALTER TABLE `{settings.project_id}.{settings.dataset_id}.{nome_tabela}` "
        f"ADD FOREIGN KEY ({coluna}) "
        f"REFERENCES `{settings.project_id}.{settings.dataset_id}.{dim}`({coluna_dim}) NOT ENFORCED"
    )
    try:
        run_ddl(client, sql)
    except Exception as exc:
        if "already exists" in str(exc).lower():
            return
        raise


def ensure_table(client: bigquery.Client, nome_tabela: str) -> None:
    """Garante que `nome_tabela` existe com o DDL do registry (partição,
    clustering, constraints) — chamado ANTES do load.

    `CREATE TABLE IF NOT EXISTS` é idempotente e não altera tabela existente;
    constraints que o load anterior derrubou são re-aplicadas depois do load
    por `ensure_constraints` (adicioná-las aqui seria inútil: o load as
    removeria em seguida).
    """
    ddl = TABLE_DDL.get(nome_tabela)
    if ddl is None:
        return
    run_ddl(client, ddl)


def ensure_constraints(client: bigquery.Client, nome_tabela: str) -> None:
    """Re-aplica PK (dims) / FKs (fatos) que o load job WRITE_TRUNCATE removeu
    — chamado DEPOIS do load.

    Sem lock: ALTER TABLE ADD CONSTRAINT é idempotente e não corrompe estado.
    A ordem de `run_gold` (dims antes dos fatos) garante que a dim referenciada
    já está com PK quando o fato re-declara a FK.
    """
    if nome_tabela not in TABLE_DDL:
        return

    if nome_tabela.startswith("dim_"):
        if _table_has_primary_key(client, nome_tabela) is False:
            _add_primary_key(client, nome_tabela)
        return

    fks = FACT_FKS.get(nome_tabela)
    if not fks:
        return
    settings = get_settings()
    tabela = client.get_table(f"{settings.project_id}.{settings.dataset_id}.{nome_tabela}")
    existentes = _existing_fk_pairs(tabela)
    for coluna, dim, coluna_dim in fks:
        if (coluna, dim) not in existentes:
            _add_foreign_key(client, nome_tabela, coluna, dim, coluna_dim)
