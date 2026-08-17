variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "labels" {
  description = "Rótulos de atribuição de custo aplicados ao tópico e à subscription"
  type        = map(string)
  default     = {}
}
