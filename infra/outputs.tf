output "pipeline_service_account" {
  description = "A Service Account do Pipeline, usada para o Dataflow/Cloud Run rodar."
  value       = module.iam.service_account_email
}

output "datalake_bucket" {
  description = "Bucket GCS para as camadas Bronze e Silver."
  value       = module.storage.bucket_name
}
