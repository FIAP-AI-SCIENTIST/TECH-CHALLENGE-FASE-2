terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Configuração do backend remoto no GCS.
  # Resolve o problema de colisão do grupo (State locking ativado).
  # Nota: O nome do bucket não pode ser passado como variável aqui (limitação do Terraform),
  # por isso usamos a flag -backend-config="bucket=..." durante a inicialização (init).
  backend "gcs" {
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  # Autenticação acontece implicitamente via Application Default Credentials (ADC),
  # usando `gcloud auth application-default login`. Zero credenciais commitadas ou passadas aqui.

  # Corrige o erro "quota project not set" em APIs sensíveis a billing (ex: billingbudgets.googleapis.com)
  # sem depender de cada membro do grupo rodar `gcloud auth application-default set-quota-project`
  # na própria máquina. Aqui fixamos explicitamente qual projeto "paga a cota" de toda chamada de API.
  user_project_override = true
  billing_project       = var.project_id
}

# Rótulos aplicados a todo recurso faturável. São a base da atribuição de custo
# no relatório de faturamento do GCP: com eles é possível abrir o gasto por
# camada da pipeline e por componente, em vez de olhar só o total do projeto.
# Sem rótulo, o relatório de custo mostra "BigQuery: R$ x" e não responde qual
# parte da pipeline gerou o gasto.
locals {
  cost_labels = {
    projeto        = "alfabetizacao"
    ambiente       = "demo"
    gerenciado_por = "terraform"
  }
}

# 1. Habilitar APIs (Storage, BigQuery, PubSub, etc.)
module "apis" {
  source     = "./modules/apis"
  project_id = var.project_id
}

# 2. Configurar o Canal de Monitoramento (E-mail para erros)
module "monitoring" {
  source      = "./modules/monitoring"
  project_id  = var.project_id
  alert_email = var.alert_email

  # Só criar depois que as APIs estiverem prontas
  depends_on = [module.apis]
}

# 3. Configurar Orçamento da Conta (Budget R$ 1)
module "budget" {
  source                  = "./modules/budget"
  project_id              = var.project_id
  billing_account         = var.billing_account
  notification_channel_id = module.monitoring.notification_channel_id

  depends_on = [module.apis]
}

# 4. Storage: Criar Data Lake efêmero (Bronze e Silver)
module "storage" {
  source     = "./modules/storage"
  project_id = var.project_id
  location   = var.gcs_location
  labels     = local.cost_labels

  depends_on = [module.apis]
}

# 5. BigQuery: Criar Dataset Analytics na MESMA LOCATION da fonte Base dos Dados (US)
module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  location   = var.bq_location
  labels     = local.cost_labels

  depends_on = [module.apis]
}

# 6. Pub/Sub: Simulação de Streaming
module "pubsub" {
  source     = "./modules/pubsub"
  project_id = var.project_id
  labels     = local.cost_labels

  depends_on = [module.apis]
}

# 7. IAM: Service Account central da Pipeline com permissões MÍNIMAS aos módulos acima
module "iam" {
  source            = "./modules/iam"
  project_id        = var.project_id
  billing_account   = var.billing_account
  bucket_name       = module.storage.bucket_name
  dataset_id        = module.bigquery.dataset_id
  topic_name        = module.pubsub.topic_name
  subscription_name = module.pubsub.subscription_name
  team_members      = var.team_members

  depends_on = [module.apis]
}

# 8. Cloud Function (Gen2) + Cloud Scheduler: dispara o Streaming Producer periodicamente
module "streaming_function" {
  source                = "./modules/streaming_function"
  project_id            = var.project_id
  region                = var.region
  bucket_name           = module.storage.bucket_name
  service_account_email = module.iam.service_account_email
  source_zip_path       = "${path.root}/.build/producer.zip"

  depends_on = [module.apis, module.storage, module.iam]
}
