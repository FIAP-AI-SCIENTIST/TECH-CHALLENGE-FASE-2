# O Data Lake armazena a camada Bronze (arquivos brutos) e Silver (arquivos limpos em Parquet)
resource "google_storage_bucket" "datalake" {
  name     = "${var.project_id}-datalake"
  project  = var.project_id
  location = var.location

  # CRÍTICO: sem isso, o `terraform destroy` falha na hora da demo se o bucket tiver arquivos.
  # Essencial para garantir a infraestrutura efêmera.
  force_destroy = true

  # Garante que o IAM será aplicado apenas no nível do bucket (segurança)
  uniform_bucket_level_access = true

  # Previne que os arquivos fiquem expostos na internet (Sem acessos públicos acidentais)
  public_access_prevention = "enforced"

  labels = var.labels

  # Política de ciclo de vida do dado bruto. Nenhuma regra apaga objeto: a
  # camada Bronze precisa preservar o histórico completo da fonte, então o que
  # se otimiza é a classe de armazenamento, não a retenção. Partição antiga é
  # lida com frequência decrescente (só em reprocessamento), o que é exatamente
  # o perfil de Nearline e depois Coldline.
  #
  # Com a infraestrutura efêmera destruída ao fim de cada demonstração, estas
  # regras não chegam a disparar na prática — elas declaram a política que
  # valeria num projeto de vida longa, e é essa a diferença que importa para o
  # custo: sem elas, todo o histórico ficaria indefinidamente em Standard.
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  # Upload interrompido no meio deixa fragmentos que continuam sendo cobrados
  # como armazenamento e nunca aparecem na listagem de objetos.
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}