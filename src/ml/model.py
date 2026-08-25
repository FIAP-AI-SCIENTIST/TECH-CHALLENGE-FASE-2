"""Modelo de predição de risco de não-alfabetização — BigQuery ML sobre a Gold.

Regressão logística (`CREATE MODEL ... OPTIONS(model_type='LOGISTIC_REG')`)
prevendo `atingiu_meta` (`fact_meta_resultado_municipio`) a partir do contexto
territorial de `dim_municipio` — IDHM geral e as três componentes (educação,
renda, longevidade) do Atlas do Desenvolvimento Humano — e do ano. É a
aplicação de IA descrita no README ("Modelos de predição de alfabetização")
materializada como modelo de verdade, não só como plano.

BigQuery ML (não um notebook externo) porque treino e predição rodam como SQL
no mesmo motor que já materializa a Gold — sem exportar dado, sem infra nova,
dentro do orçamento free-tier já desenhado para o projeto (`docs/estimativa-de-
custos.md`). O `id_municipio`/`nome`/`sigla_uf` entram só na query de predição,
nunca na de treino: incluí-los no treino faria o BQML tratá-los como feature de
altíssima cardinalidade (a chave de negócio virando "feature" é ruído puro para
uma regressão logística) — a query de predição os inclui como colunas de
passagem (BQML preserva colunas que não fazem parte do conjunto de features do
modelo), só para identificar a linha no resultado.
"""

from config import get_settings

MODEL_NAME = "modelo_risco_nao_alfabetizacao"
PREDICTIONS_VIEW = "ml_predicao_risco_municipio"

# Treino roda direto sobre a Gold; pode levar mais que os 30s usados no DDL
# estrutural (gold.schema) — regressão logística sobre a Gold atual (~10-25k
# linhas de fact_meta_resultado_municipio) treina em segundos, mas o teto fica
# generoso para não falhar por timeout em volume maior.
TRAIN_TIMEOUT_SECONDS = 300
QUERY_TIMEOUT_SECONDS = 30

# Cap de custo, mesmo espírito do MAX_BYTES_BILLED da extração
# (`extraction.extraction`): treino/avaliação/predição rodam sobre a Gold
# (~10-25k linhas em fact_meta_resultado_municipio, ordens de magnitude abaixo
# do teto) — a folga é para não falhar em volume maior, o teto é para que um
# JOIN acidentalmente caro falhe a query em vez de gerar fatura.
MAX_BYTES_BILLED = 1 * 2**30

_JOIN_MUNICIPIO_META = """FROM {prefix}fact_meta_resultado_municipio f
JOIN {prefix}dim_municipio d ON f.sk_municipio = d.sk_municipio
WHERE d.idhm IS NOT NULL"""


def _train_query(prefix: str) -> str:
    """Só features + label — nenhuma chave de negócio entra no treino."""
    return f"""SELECT
    f.ano,
    d.idhm,
    d.idhm_educacao,
    d.idhm_renda,
    d.idhm_longevidade,
    f.atingiu_meta AS label
{_JOIN_MUNICIPIO_META.format(prefix=prefix)}"""


def _scoring_query(prefix: str) -> str:
    """Mesmas features do treino + colunas de identificação (passthrough) e o
    rótulo real, para permitir comparar predição x resultado observado."""
    return f"""SELECT
    d.id_municipio,
    d.nome AS nome_municipio,
    d.sigla_uf,
    f.ano,
    d.idhm,
    d.idhm_educacao,
    d.idhm_renda,
    d.idhm_longevidade,
    f.atingiu_meta AS atingiu_meta_real
{_JOIN_MUNICIPIO_META.format(prefix=prefix)}"""


def _prefixo(project_id: str | None, dataset_id: str | None) -> tuple[str, str, str]:
    if project_id is None or dataset_id is None:
        settings = get_settings()
        project_id = project_id or settings.project_id
        dataset_id = dataset_id or settings.dataset_id
    return project_id, dataset_id, f"`{project_id}.{dataset_id}`."


def render_train_ddl(project_id: str | None = None, dataset_id: str | None = None) -> str:
    """`CREATE OR REPLACE MODEL` treinado direto sobre a Gold.

    `data_split_method='RANDOM'` com 20% de avaliação: a Gold não tem coluna de
    tempo reservada para split cronológico limpo por município (o mesmo ano
    aparece em várias versões SCD2), então split aleatório é a opção que não
    exige inventar uma partição temporal artificial nesta primeira versão.
    """
    project_id, dataset_id, prefix = _prefixo(project_id, dataset_id)
    return f"""CREATE OR REPLACE MODEL `{project_id}.{dataset_id}.{MODEL_NAME}`
OPTIONS (
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['label'],
  data_split_method = 'RANDOM',
  data_split_eval_fraction = 0.2
) AS
{_train_query(prefix)}"""


def render_evaluate_query(project_id: str | None = None, dataset_id: str | None = None) -> str:
    """`ML.EVALUATE` sobre o próprio split de avaliação reservado no treino
    (sem argumento de query — usa o que `data_split_method` já separou)."""
    project_id, dataset_id, _ = _prefixo(project_id, dataset_id)
    return f"SELECT * FROM ML.EVALUATE(MODEL `{project_id}.{dataset_id}.{MODEL_NAME}`)"


def render_predictions_view_ddl(project_id: str | None = None, dataset_id: str | None = None) -> str:
    """View servindo a predição pronta para consumo (dashboard/gestor) —
    `ML.PREDICT` sobre todo o histórico, não só o split de avaliação."""
    project_id, dataset_id, prefix = _prefixo(project_id, dataset_id)
    return f"""CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{PREDICTIONS_VIEW}` AS
SELECT
    id_municipio,
    nome_municipio,
    sigla_uf,
    ano,
    atingiu_meta_real,
    predicted_label AS risco_previsto_atingir_meta,
    predicted_label_probs
FROM ML.PREDICT(
    MODEL `{project_id}.{dataset_id}.{MODEL_NAME}`,
    ({_scoring_query(prefix)})
)"""
