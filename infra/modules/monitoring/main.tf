# Criação de um Notification Channel para enviar e-mails de alerta.
# Este é o Shared Notification Channel usado tanto para Log-based Alert Policy
# quanto para alertas de Faturamento.
resource "google_monitoring_notification_channel" "email_alert" {
  project      = var.project_id
  display_name = "Alerta Pipeline Educação"
  type         = "email"
  
  labels = {
    email_address = var.alert_email
  }
}

# Log-Based Alert Policy para capturar erros no pipeline.
# Quando um log com severidade ERROR for gerado (Cloud Run, Pub/Sub, etc), este alerta envia um e-mail.
resource "google_monitoring_alert_policy" "pipeline_error_alert" {
  project      = var.project_id
  display_name = "Erro no Pipeline de Alfabetizacao"
  combiner     = "OR"
  
  conditions {
    display_name = "Log matching ERROR"
    condition_matched_log {
      # Captura qualquer log de projeto com severidade ERROR (NFR03: Observabilidade básica).
      filter = "severity >= ERROR"
    }
  }

  notification_channels = [google_monitoring_notification_channel.email_alert.name]
}
