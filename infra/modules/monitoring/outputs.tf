output "notification_channel_id" {
  description = "ID do Canal de Notificação para ser usado pelo Budget"
  value       = google_monitoring_notification_channel.email_alert.name
}
