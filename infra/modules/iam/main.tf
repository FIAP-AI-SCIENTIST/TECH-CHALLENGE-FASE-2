# Service Account Central (Padrão de Autorização Least Privilege, Single Shared SA)
# Escolha por 1 SA agora para evitar over-engineering de 4 SAs antes das Units de código existirem.
resource "google_service_account" "pipeline_sa" {
  project      = var.project_id
  account_id   = "alfabetizacao-pipeline-sa"
  display_name = "Alfabetizacao Pipeline SA"
  description  = "Service Account usada por Cloud Run Job, dbt, e Publisher/Consumer."
}

# Concede papel Storage Object Admin APENAS para o bucket datalake (Princípio Least Privilege)
resource "google_storage_bucket_iam_member" "datalake_access" {
  bucket = var.bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Concede papel BigQuery Data Editor APENAS para o dataset analítico (Princípio Least Privilege)
resource "google_bigquery_dataset_iam_member" "bigquery_access" {
  project    = var.project_id
  dataset_id = var.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# É necessário permissão BigQuery Job User para o dbt e para gravar em tabelas
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Concede papel para Publicar no Tópico do Pub/Sub
resource "google_pubsub_topic_iam_member" "topic_access" {
  project = var.project_id
  topic   = var.topic_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Concede papel para Consumir as mensagens da Subscription
resource "google_pubsub_subscription_iam_member" "subscription_access" {
  project      = var.project_id
  subscription = var.subscription_name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Concede papel de leitura da Cloud Monitoring API — necessário para o componente de
# Observabilidade ler a métrica nativa de Consumer Lag (num_undelivered_messages) da
# subscription do Pub/Sub. Os papéis acima cobrem escrita de dados (Storage/BigQuery/
# Pub/Sub), mas nenhum cobria leitura de métricas de monitoramento — adicionado aqui.
resource "google_project_iam_member" "monitoring_viewer_access" {
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
