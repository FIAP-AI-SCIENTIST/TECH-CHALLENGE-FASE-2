variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "location" {
  description = "Região do bucket GCS (ex: us-central1)"
  type        = string
}

variable "labels" {
  description = "Rótulos de atribuição de custo aplicados ao bucket"
  type        = map(string)
  default     = {}
}
