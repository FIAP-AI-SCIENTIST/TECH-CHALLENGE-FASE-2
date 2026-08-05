output "dataset_id" {
  description = "ID do Dataset (utilizado pelo IAM)"
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "audit_table_id" {
  description = "ID da tabela de auditoria"
  value       = google_bigquery_table.audit_log.table_id
}
