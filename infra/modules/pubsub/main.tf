# Tópico Pub/Sub para simular chegada contínua de eventos (Streaming Producer)
resource "google_pubsub_topic" "streaming_events" {
  name    = "alfabetizacao-streaming-events"
  project = var.project_id
  labels  = var.labels
}

# Assinatura (Subscription) para consumir os eventos e gravá-los no Bronze (Streaming Consumer)
# Por decisão arquitetural, a Dead-Letter Queue (DLQ) foi omitida nesta etapa,
# será responsabilidade do Streaming Consumer reconfigurar o recurso caso seja necessário.
resource "google_pubsub_subscription" "streaming_consumer" {
  name    = "alfabetizacao-streaming-consumer-sub"
  project = var.project_id
  topic   = google_pubsub_topic.streaming_events.name

  # Retenção padrão para não acumular lixo por muito tempo e consumir storage
  message_retention_duration = "86400s" # 1 dia

  # Acknowledgement deadline configurado para acomodar latência básica de IO e validação estrutural (Pydantic)
  ack_deadline_seconds = 20

  labels = var.labels
}
