output "service_account_email" {
  description = "O e-mail da SA criada"
  value       = google_service_account.pipeline_sa.email
}
