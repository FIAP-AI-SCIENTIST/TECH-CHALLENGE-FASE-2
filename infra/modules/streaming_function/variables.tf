variable "project_id" {
  description = "ID do Projeto GCP"
  type        = string
}

variable "region" {
  description = "Região de deploy do Cloud Function e Cloud Scheduler"
  type        = string
}

variable "service_account_email" {
  description = "E-mail da Service Account central da pipeline (execução da função e do scheduler)"
  type        = string
}

variable "source_zip_path" {
  description = "Caminho local do zip gerado por infra/scripts/package_producer.sh"
  type        = string
}

variable "bucket_name" {
  description = "Bucket onde o zip do código-fonte é armazenado (prefixo _deploy/) — reaproveita o bucket datalake"
  type        = string
}
