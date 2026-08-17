# O Dataset que abriga as tabelas do modelo dimensional (camada Gold, criadas
# pelo próprio pipeline via DDL) e as tabelas operacionais de auditoria de
# execução e de qualidade de dados.
resource "google_bigquery_dataset" "analytics" {
  dataset_id = "alfabetizacao_analytics"
  project    = var.project_id
  location   = var.location # CRÍTICO: DEVE ser "US" para co-localização com basedosdados

  labels = var.labels

  # CRÍTICO: sem isso, `terraform destroy` falhará se houverem tabelas criadas no
  # dataset (as dimensões, fatos e views da Gold são criadas em tempo de execução,
  # fora do Terraform). Garante a efemeridade do projeto.
  delete_contents_on_destroy = true
}

# Tabela de auditoria para o componente de Observabilidade registrar métricas de cada job do pipeline
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
  labels              = var.labels

  # Sem particionamento por data de propósito: o BigQuery não adiciona partição a
  # uma tabela já existente, então declarar `time_partitioning` aqui faria o
  # Terraform recriar a tabela e apagar todo o histórico de execuções acumulado —
  # que é justamente a evidência de que o pipeline rodou. A mudança só é segura
  # junto de um ciclo completo de recriação da infraestrutura, não de forma
  # incremental. Enquanto isso, o volume é de uma linha por etapa por execução:
  # dezenas de linhas por rodada, longe de qualquer limiar de custo.
  schema = <<EOF
[
  {"name": "run_id", "type": "STRING", "mode": "REQUIRED", "description": "Identificador único da execução"},
  {"name": "step", "type": "STRING", "mode": "REQUIRED", "description": "Qual etapa rodou (ex: Bronze_Ingestion)"},
  {"name": "layer", "type": "STRING", "mode": "REQUIRED", "description": "Camada alvo (ex: Bronze)"},
  {"name": "rows_read", "type": "INT64", "mode": "NULLABLE", "description": "Linhas lidas da origem"},
  {"name": "rows_written", "type": "INT64", "mode": "NULLABLE", "description": "Linhas escritas no destino"},
  {"name": "total_bytes_processed", "type": "INT64", "mode": "NULLABLE", "description": "Bytes processados pela query BigQuery (quando aplicavel)"},
  {"name": "duration_seconds", "type": "FLOAT64", "mode": "REQUIRED", "description": "Duração do step"},
  {"name": "status", "type": "STRING", "mode": "REQUIRED", "description": "SUCCESS ou ERROR"},
  {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Momento da execução"}
]
EOF
}

# Evidência histórica das expectativas Great Expectations por rodada.
resource "google_bigquery_table" "data_quality_log" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "data_quality_log"
  project             = var.project_id
  deletion_protection = false
  labels              = var.labels

  # Mesma razão da tabela de auditoria para não declarar partição aqui.
  schema = <<EOF
[
  {"name":"check_id","type":"STRING","mode":"REQUIRED"},
  {"name":"check","type":"STRING","mode":"REQUIRED"},
  {"name":"entidade","type":"STRING","mode":"REQUIRED"},
  {"name":"dimensao","type":"STRING","mode":"REQUIRED"},
  {"name":"passou","type":"BOOL","mode":"REQUIRED"},
  {"name":"valor_medido","type":"FLOAT64","mode":"REQUIRED"},
  {"name":"limiar","type":"FLOAT64","mode":"REQUIRED"},
  {"name":"severidade","type":"STRING","mode":"REQUIRED"},
  {"name":"linhas_afetadas","type":"INT64","mode":"REQUIRED"},
  {"name":"detalhe","type":"STRING","mode":"NULLABLE"},
  {"name":"timestamp","type":"TIMESTAMP","mode":"REQUIRED"}
]
EOF
}
