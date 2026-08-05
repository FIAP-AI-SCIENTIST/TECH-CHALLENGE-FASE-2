variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "bucket_name" {
  description = "Nome do bucket gerado no módulo storage"
  type        = string
}

variable "dataset_id" {
  description = "ID do dataset gerado no módulo bigquery"
  type        = string
}

variable "topic_name" {
  description = "Nome do tópico gerado no módulo pubsub"
  type        = string
}

variable "subscription_name" {
  description = "Nome da subscription gerada no módulo pubsub"
  type        = string
}
