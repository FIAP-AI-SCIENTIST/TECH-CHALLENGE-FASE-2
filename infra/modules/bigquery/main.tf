# O Dataset que abriga as tabelas analíticas (Camada Gold materializada por dbt)
# e as tabelas operacionais como a de Auditoria de pipeline.
resource "google_bigquery_dataset" "analytics" {
  dataset_id = "alfabetizacao_analytics"
  project    = var.project_id
  location   = var.location # CRÍTICO: DEVE ser "US" para co-localização com basedosdados

  # CRÍTICO: sem isso, `terraform destroy` falhará se houverem tabelas criadas no dataset
  # (ex: tabelas do dbt da camada Gold). Garante a efemeridade do projeto.
  delete_contents_on_destroy = true
}

# Tabela de auditoria para o componente Observability (U3) registrar métricas de cada job do pipeline
resource "google_bigquery_table" "audit_log" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "pipeline_audit_log"
  project    = var.project_id

  # Não é necessário delete_contents_on_destroy em nível de tabela, pois é gerenciado pelo dataset

  # Não é necessário delete_contents_on_destroy em nível de tabela, pois é gerenciado pelo dataset
  # Mas configuramos schema fixo
  schema = <<EOF
[
  {"name": "run_id", "type": "STRING", "mode": "REQUIRED", "description": "Identificador único da execução"},
  {"name": "unit", "type": "STRING", "mode": "REQUIRED", "description": "Qual unit rodou (ex: U4_Bronze)"},
  {"name": "layer", "type": "STRING", "mode": "REQUIRED", "description": "Camada alvo (ex: Bronze)"},
  {"name": "rows_read", "type": "INT64", "mode": "NULLABLE", "description": "Linhas lidas da origem"},
  {"name": "rows_written", "type": "INT64", "mode": "NULLABLE", "description": "Linhas escritas no destino"},
  {"name": "duration_seconds", "type": "FLOAT64", "mode": "REQUIRED", "description": "Duração do step"},
  {"name": "status", "type": "STRING", "mode": "REQUIRED", "description": "SUCCESS ou ERROR"},
  {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Momento da execução"}
]
EOF
}
