output "bucket_name" {
  description = "O nome do bucket Data Lake (utilizado pelo IAM)"
  value       = google_storage_bucket.datalake.name
}
