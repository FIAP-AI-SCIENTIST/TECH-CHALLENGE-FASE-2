# Habilita a API principal para gerenciar recursos do projeto (necessária para que o Terraform atribua papéis IAM)
resource "google_project_service" "cloudresourcemanager" {
  project            = var.project_id
  service            = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false # Desativar APIs no destroy pode quebrar o projeto permanentemente
}

# Habilita a API do Cloud Storage (para os buckets Bronze/Silver)
resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

# Habilita a API do BigQuery (para o dataset analítico e auditoria)
resource "google_project_service" "bigquery" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

# Habilita a API do Pub/Sub (para o streaming)
resource "google_project_service" "pubsub" {
  project            = var.project_id
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

# Habilita a API de Faturamento/Budget (para os alertas de custo R$ 1)
resource "google_project_service" "billingbudgets" {
  project            = var.project_id
  service            = "billingbudgets.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

# Habilita a API de Monitoramento (para o log-based alert policy)
resource "google_project_service" "monitoring" {
  project            = var.project_id
  service            = "monitoring.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

# Habilita APIs antecipadamente para Cloud Run e Cloud Scheduler
# Isso evita falhas de dependência na próxima fase, já que a ativação da API é idempotente e gratuita
resource "google_project_service" "run" {
  project            = var.project_id
  service            = "run.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

resource "google_project_service" "cloudscheduler" {
  project            = var.project_id
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

# Habilita a API do IAM (necessária para criar/ler Service Accounts e seus bindings).
# Faltou originalmente nesta lista — sem ela, chamadas de leitura/gestão de Service Account falham
# com "SERVICE_DISABLED" assim que o billing_project é fixado explicitamente no provider.
resource "google_project_service" "iam" {
  project            = var.project_id
  service            = "iam.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}

# Habilita a API do Cloud Billing (necessária para conceder papéis IAM na
# billing account, ex: roles/billing.costsManager pros membros "roles/editor")
resource "google_project_service" "cloudbilling" {
  project            = var.project_id
  service            = "cloudbilling.googleapis.com"
  disable_on_destroy = false
  depends_on         = [google_project_service.cloudresourcemanager]
}
