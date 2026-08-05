# O Data Lake armazena a camada Bronze (arquivos brutos) e Silver (arquivos limpos em Parquet)
resource "google_storage_bucket" "datalake" {
  name     = "${var.project_id}-datalake"
  project  = var.project_id
  location = var.location

  # CRÍTICO: sem isso, o `terraform destroy` falha na hora da demo se o bucket tiver arquivos.
  # Essencial para garantir a infraestrutura efêmera (NFR01).
  force_destroy = true

  # Garante que o IAM será aplicado apenas no nível do bucket (NFR04: Segurança)
  uniform_bucket_level_access = true

  # Previne que os arquivos fiquem expostos na internet (Sem acessos públicos acidentais)
  public_access_prevention = "enforced"
}
