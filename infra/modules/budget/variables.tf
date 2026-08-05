variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "billing_account" {
  description = "ID da conta de faturamento GCP"
  type        = string
}

variable "notification_channel_id" {
  description = "Canal de notificação de e-mail (output do módulo de monitoring)"
  type        = string
}
