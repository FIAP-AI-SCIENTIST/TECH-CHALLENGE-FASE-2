variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "billing_account" {
  description = "ID da conta de faturamento (necessário para o papel de billing dos membros \"roles/editor\" — orçamento é escopo de billing account, não de projeto)."
  type        = string
}

variable "team_members" {
  description = "Mapa e-mail do membro do grupo -> papel concedido no projeto: \"roles/viewer\" (só visualizar) ou \"roles/editor\" (sobe e destrói a infra principal). \"roles/editor\" sozinho não basta para isso no GCP (não gerencia IAM nem orçamento), então este state concede automaticamente roles/resourcemanager.projectIamAdmin + roles/billing.costsManager junto pra quem tiver esse papel."
  type        = map(string)
  default     = {}
}
