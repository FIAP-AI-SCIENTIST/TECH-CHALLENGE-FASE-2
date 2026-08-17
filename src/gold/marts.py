"""Views analíticas (marts) sobre os fatos da Gold.

Cada mart é uma VIEW BigQuery (`CREATE OR REPLACE VIEW`) criada pelo pipeline ao
final de `run_gold`, depois dos fatos. O SQL é portável DuckDB/BigQuery (sem funções
só-BigQuery) para ser testado localmente: o placeholder `{prefix}`
vira `` `projeto.dataset`. `` em produção e string vazia nos testes DuckDB.
"""

from google.cloud import bigquery

from config import get_settings
from gold import schema as gold_schema

MART_QUERIES: dict[str, str] = {
    # Grão (ano, sigla_uf): evolução temporal do indicador, com variação ano a ano.
    # delta_pp_vs_ano_anterior é NULL no primeiro ano de cada UF (LAG sem predecessor).
    "mart_evolucao_indicador_uf": """WITH por_ano_uf AS (
    SELECT ano, sigla_uf,
           AVG(taxa_alfabetizacao) AS taxa_media_alfabetizacao,
           AVG(media_portugues)    AS media_portugues_media
    FROM {prefix}fact_indicador_uf
    GROUP BY ano, sigla_uf
)
SELECT ano, sigla_uf, taxa_media_alfabetizacao, media_portugues_media,
       taxa_media_alfabetizacao
         - LAG(taxa_media_alfabetizacao) OVER (PARTITION BY sigla_uf ORDER BY ano)
           AS delta_pp_vs_ano_anterior
FROM por_ano_uf
""",
    # Grão (ano, sigla_uf, rede) por versão SCD2: meta × resultado + percentual de
    # cumprimento agregado no nível (ano, sigla_uf).
    "mart_aderencia_metas_uf": """SELECT ano, sigla_uf, rede,
       taxa_alfabetizacao,
       meta_indicador AS meta_do_ano,
       gap_pontos, atingiu_meta,
       AVG(CASE WHEN atingiu_meta THEN 1.0 ELSE 0.0 END)
           OVER (PARTITION BY ano, sigla_uf) AS pct_cumprimento_ano_uf,
       valid_from, valid_to, is_current
FROM {prefix}fact_meta_resultado_uf
""",
    # Grão (ano, id_municipio): agrega série/rede antes de rankear, enriquece com
    # dim_municipio e rankeia dentro da UF (RANK — empates dividem posição).
    # O JOIN físico é pela surrogate key (FK declarada); a chave natural
    # `id_municipio` sai da dimensão como atributo consultável.
    "mart_ranking_indicador_municipio": """WITH por_municipio_ano AS (
    SELECT f.ano, f.sk_municipio,
           AVG(f.taxa_alfabetizacao) AS taxa_media_alfabetizacao,
           AVG(f.media_portugues)    AS media_portugues_media
    FROM {prefix}fact_indicador_municipio f
    GROUP BY f.ano, f.sk_municipio
)
SELECT p.ano, d.id_municipio,
       d.nome AS nome_municipio, d.sigla_uf, d.nome_regiao, d.capital_uf,
       p.taxa_media_alfabetizacao, p.media_portugues_media,
       RANK() OVER (PARTITION BY p.ano, d.sigla_uf
                    ORDER BY p.taxa_media_alfabetizacao DESC) AS rank_uf
FROM por_municipio_ano p
JOIN {prefix}dim_municipio d ON p.sk_municipio = d.sk_municipio
""",
}


def render_view_ddl(nome_view: str, project_id: str | None = None, dataset_id: str | None = None) -> str:
    """DDL completo da view: `CREATE OR REPLACE VIEW` sobre a query com prefixo
    qualificado (backticks obrigatórios — o project_id contém hífens)."""
    if project_id is None or dataset_id is None:
        settings = get_settings()
        if project_id is None:
            project_id = settings.project_id
        if dataset_id is None:
            dataset_id = settings.dataset_id
    prefixo = f"`{project_id}.{dataset_id}`."
    corpo = MART_QUERIES[nome_view].format(prefix=prefixo)
    return f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{nome_view}` AS\n{corpo}"


def create_view(nome_view: str) -> None:
    """Cria/substitui a view `nome_view` no dataset analítico.

    DDL idempotente (OR REPLACE) — sem lock GCS, view não guarda estado a proteger. O retry de
    falhas transitórias vem de `gold.schema.run_ddl`.
    """
    client = bigquery.Client(project=get_settings().project_id)
    gold_schema.run_ddl(client, render_view_ddl(nome_view))