variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "location" {
  description = "Região/Multi-region do dataset BigQuery. DEVE casar com a fonte (US)"
  type        = string
}
