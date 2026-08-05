output "ready" {
  description = "Um output dummy para sinalizar que as APIs foram habilitadas, servindo como dependência implícita (depends_on) para outros módulos"
  value       = true
  depends_on = [
    google_project_service.storage,
    google_project_service.bigquery,
    google_project_service.pubsub,
    google_project_service.billingbudgets,
    google_project_service.monitoring,
    google_project_service.run,
    google_project_service.cloudscheduler,
    google_project_service.iam
  ]
}
