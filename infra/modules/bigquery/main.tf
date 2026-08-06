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

  # CRÍTICO: o provider protege tabelas BigQuery contra destroy por padrão
  # (deletion_protection = true implícito desde a v4.66 do provider google),
  # independente do delete_contents_on_destroy do dataset — são dois mecanismos
  # diferentes. Sem isso, `terraform destroy` falha nesta tabela (e por
  # dependência, no dataset que a contém), quebrando a efemeridade do projeto.
  deletion_protection = false
  schema              = <<EOF
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
