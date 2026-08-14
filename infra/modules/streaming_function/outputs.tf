output "function_url" {
  description = "URL HTTPS da Cloud Function do Producer (também usada pelo Cloud Scheduler)"
  value       = google_cloudfunctions2_function.producer.url
}
