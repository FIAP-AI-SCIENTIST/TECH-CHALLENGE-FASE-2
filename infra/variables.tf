variable "project_id" {
  description = "ID do Projeto GCP"
  type        = string
}

variable "billing_account" {
  description = "ID da Conta de Faturamento"
  type        = string
}

variable "bq_location" {
  description = "Localização Multi-region do BigQuery (OBRIGATÓRIO: US)"
  type        = string
  default     = "US"
}

variable "gcs_location" {
  description = "Região para os buckets do GCS"
  type        = string
  default     = "us-central1"
}

variable "region" {
  description = "Região de deploy de recursos compute (Cloud Function, Cloud Scheduler)"
  type        = string
  default     = "us-central1"
}

variable "alert_email" {
  description = "Email de notificação para erros e faturamento"
  type        = string
}

variable "team_members" {
  description = "Mapa e-mail do membro do grupo -> papel IAM no projeto GCP (ex: { \"colega@gmail.com\" = \"roles/viewer\" })"
  type        = map(string)
  default     = {}
}
