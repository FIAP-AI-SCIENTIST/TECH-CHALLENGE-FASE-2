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

variable "billing_account" {
  description = "ID da conta de faturamento (necessário para o papel de billing dos membros \"roles/editor\" — orçamento é escopo de billing account, não de projeto)."
  type        = string
}

variable "team_members" {
  description = "Mapa e-mail do membro do grupo -> papel concedido no projeto: \"roles/viewer\" (só visualizar) ou \"roles/editor\" (sobe e destrói a infra). \"roles/editor\" sozinho não basta para isso no GCP (não gerencia IAM nem orçamento), então o módulo concede automaticamente roles/resourcemanager.projectIamAdmin + roles/billing.costsManager junto pra quem tiver esse papel. Não confundir com a Service Account da pipeline acima — é acesso humano ao console/CLI."
  type        = map(string)
  default     = {}
}
