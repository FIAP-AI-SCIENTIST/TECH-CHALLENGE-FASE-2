variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "location" {
  description = "Região/Multi-region do dataset BigQuery. DEVE casar com a fonte (US)"
  type        = string
}

variable "labels" {
  description = "Rótulos de atribuição de custo aplicados ao dataset e às tabelas operacionais"
  type        = map(string)
  default     = {}
}
