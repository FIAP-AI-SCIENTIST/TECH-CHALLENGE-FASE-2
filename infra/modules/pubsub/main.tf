# Tópico Pub/Sub para simular chegada contínua de eventos (Streaming Producer - U5)
resource "google_pubsub_topic" "streaming_events" {
  name    = "alfabetizacao-streaming-events"
  project = var.project_id
}

# Assinatura (Subscription) para consumir os eventos e gravá-leno Bronze (Streaming Consumer - U5)
# Por decisão arquitetural (NFR Design Q4), a Dead-Letter Queue (DLQ) foi omitida nesta etapa,
# será responsabilidade exclusiva da Unit 5 (Streaming) reconfigurar o recurso caso seja necessário.
resource "google_pubsub_subscription" "streaming_consumer" {
  name    = "alfabetizacao-streaming-consumer-sub"
  project = var.project_id
  topic   = google_pubsub_topic.streaming_events.name

  # Retenção padrão para não acumular lixo por muito tempo e consumir storage
  message_retention_duration = "86400s" # 1 dia

  # Acknowledgement deadline configurado para acomodar latência básica de IO e validação estrutural (Pydantic)
  ack_deadline_seconds = 20
}
