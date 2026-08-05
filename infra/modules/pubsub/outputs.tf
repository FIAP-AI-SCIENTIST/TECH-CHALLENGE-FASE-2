output "topic_name" {
  description = "Nome do tópico Pub/Sub (usado pelo IAM)"
  value       = google_pubsub_topic.streaming_events.name
}

output "subscription_name" {
  description = "Nome da assinatura Pub/Sub (usado pelo IAM)"
  value       = google_pubsub_subscription.streaming_consumer.name
}
