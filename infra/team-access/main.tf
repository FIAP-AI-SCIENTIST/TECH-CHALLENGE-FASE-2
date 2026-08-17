terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Backend remoto próprio (mesmo bucket do state principal, prefix diferente).
  # Isolado de propósito: `terraform destroy` rodado em infra/ (ciclo efêmero de
  # demo) só enxerga o state em infra/terraform/state e NUNCA pode alcançar os
  # recursos daqui, porque eles simplesmente não existem nesse outro arquivo de
  # state. Ver README para o porquê (make infra-destroy não deve revogar o
  # acesso do time ao projeto).
  backend "gcs" {
    prefix = "terraform/team-access-state"
  }
}

provider "google" {
  project = var.project_id
  # Mesma lógica do root infra/main.tf: fixa o projeto que paga a cota das
  # chamadas de IAM/Billing feitas por este state.
  user_project_override = true
  billing_project       = var.project_id
}

# Acesso humano ao console (não confundir com a Service Account da pipeline,
# gerenciada em infra/modules/iam — essa é state separado de propósito, ver
# infra/main.tf). Cada membro usa a PRÓPRIA conta Google (nunca compartilhar
# credenciais).
#
# for_each sobre var.team_members: uma entrada por (email, papel) evita
# recriar todo mundo quando só uma pessoa/papel muda.
resource "google_project_iam_member" "team_access" {
  for_each = var.team_members

  project = var.project_id
  role    = each.value
  member  = "user:${each.key}"
}

# "roles/editor" puro NÃO sobe/destrói a stack principal (infra/): não inclui
# setIamPolicy (não gerencia os google_project_iam_member acima nem os do SA
# da pipeline) nem permissão de billing (o orçamento do módulo budget é
# escopo de billing account, não de projeto). Quem precisa rodar apply/destroy
# completo do state principal ganha os dois extras abaixo.
locals {
  editor_emails = toset([for email, role in var.team_members : email if role == "roles/editor"])
}

resource "google_project_iam_member" "team_access_iam_admin" {
  for_each = local.editor_emails

  project = var.project_id
  role    = "roles/resourcemanager.projectIamAdmin"
  member  = "user:${each.value}"
}

resource "google_billing_account_iam_member" "team_access_billing" {
  for_each = local.editor_emails

  billing_account_id = var.billing_account
  role               = "roles/billing.costsManager"
  member             = "user:${each.value}"
}
