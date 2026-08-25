"""Testes do módulo ml.model — SQL gerado para treino, avaliação e predição."""

from config import get_settings
from ml.model import (
    MODEL_NAME,
    PREDICTIONS_VIEW,
    render_evaluate_query,
    render_predictions_view_ddl,
    render_train_ddl,
)


class TestRenderTrainDdl:
    def test_qualifica_modelo_com_backticks(self):
        settings = get_settings()
        ddl = render_train_ddl()
        assert ddl.startswith(
            f"CREATE OR REPLACE MODEL `{settings.project_id}.{settings.dataset_id}.{MODEL_NAME}`"
        )

    def test_aceita_projeto_e_dataset_explicitos(self):
        ddl = render_train_ddl(project_id="p", dataset_id="d")
        assert ddl.startswith(f"CREATE OR REPLACE MODEL `p.d.{MODEL_NAME}`")
        assert "`p.d`.fact_meta_resultado_municipio" in ddl
        assert "`p.d`.dim_municipio" in ddl

    def test_regressao_logistica_prevendo_atingiu_meta(self):
        ddl = render_train_ddl()
        assert "model_type = 'LOGISTIC_REG'" in ddl
        assert "input_label_cols = ['label']" in ddl
        assert "f.atingiu_meta AS label" in ddl

    def test_features_sao_idhm_e_ano_sem_chave_de_negocio(self):
        """Nenhuma chave de negócio (id_municipio/nome/sigla_uf) entra na query
        de treino — só entraria como feature de altíssima cardinalidade, ruído
        puro para uma regressão logística."""
        ddl = render_train_ddl()
        for coluna in ("d.idhm", "d.idhm_educacao", "d.idhm_renda", "d.idhm_longevidade", "f.ano"):
            assert coluna in ddl
        for chave in ("id_municipio", "nome", "sigla_uf"):
            assert chave not in ddl

    def test_filtra_municipio_sem_idhm(self):
        assert "WHERE d.idhm IS NOT NULL" in render_train_ddl()

    def test_filtra_rotulo_nulo(self):
        """O BigQuery ML rejeita `CREATE MODEL` com rótulo NULL, e linha sem
        meta vigente tem `atingiu_meta` NULL por desenho — sem esse filtro o
        treino falha na criação do modelo."""
        assert "AND f.atingiu_meta IS NOT NULL" in render_train_ddl()


class TestRenderEvaluateQuery:
    def test_ml_evaluate_do_modelo_treinado(self):
        settings = get_settings()
        query = render_evaluate_query()
        assert query == f"SELECT * FROM ML.EVALUATE(MODEL `{settings.project_id}.{settings.dataset_id}.{MODEL_NAME}`)"


class TestRenderPredictionsViewDdl:
    def test_qualifica_view_com_backticks(self):
        settings = get_settings()
        ddl = render_predictions_view_ddl()
        assert ddl.startswith(
            f"CREATE OR REPLACE VIEW `{settings.project_id}.{settings.dataset_id}.{PREDICTIONS_VIEW}` AS"
        )

    def test_usa_ml_predict_sobre_o_modelo_treinado(self):
        settings = get_settings()
        ddl = render_predictions_view_ddl()
        assert f"MODEL `{settings.project_id}.{settings.dataset_id}.{MODEL_NAME}`" in ddl
        assert "ML.PREDICT(" in ddl

    def test_query_de_score_inclui_identificacao_como_passthrough(self):
        """Diferente do treino, a query de scoring inclui as chaves de negócio
        — não como feature, mas para identificar a linha no resultado."""
        ddl = render_predictions_view_ddl()
        for coluna in ("d.id_municipio", "d.nome AS nome_municipio", "d.sigla_uf"):
            assert coluna in ddl

    def test_aceita_projeto_e_dataset_explicitos(self):
        ddl = render_predictions_view_ddl(project_id="p", dataset_id="d")
        assert ddl.startswith(f"CREATE OR REPLACE VIEW `p.d.{PREDICTIONS_VIEW}` AS")
        assert f"MODEL `p.d.{MODEL_NAME}`" in ddl

    def test_nao_filtra_rotulo_nulo_ao_prever(self):
        """Assimetria proposital com o treino: prever município sem meta
        vigente é achado analítico válido, então a predição mantém a linha e
        devolve o rótulo real como passthrough."""
        ddl = render_predictions_view_ddl()
        assert "atingiu_meta IS NOT NULL" not in ddl
        assert "f.atingiu_meta AS atingiu_meta_real" in ddl
