# Reaproveita o bucket datalake (prefixo _deploy/) para guardar o zip do código-fonte —
# sem criar um bucket dedicado só para isso (decisão de projeto da infraestrutura).

# O nome do objeto inclui o hash do zip: sempre que infra/scripts/package_producer.sh
# gerar um zip novo, o nome muda, o que força o Cloud Function a fazer redeploy.
resource "google_storage_bucket_object" "producer_source" {
  name   = "_deploy/producer-${filemd5(var.source_zip_path)}.zip"
  bucket = var.bucket_name
  source = var.source_zip_path
}

resource "google_cloudfunctions2_function" "producer" {
  name     = "alfabetizacao-streaming-producer"
  location = var.region
  project  = var.project_id

  build_config {
    runtime     = "python311"
    entry_point = "handler" # main.py: from streaming.producer import cloud_function_entrypoint as handler
    source {
      storage_source {
        bucket = var.bucket_name
        object = google_storage_bucket_object.producer_source.name
      }
    }
  }

  service_config {
    # Recursos mínimos — a função só gera e publica alguns eventos sintéticos por invocação.
    available_memory      = "256M"
    timeout_seconds       = 60
    max_instance_count    = 1
    service_account_email = var.service_account_email
  }
}

# Só o Cloud Scheduler (autenticado como a mesma SA) pode invocar a função —
# sem acesso público (least privilege, mesmo princípio já aplicado em toda a infra).
resource "google_cloud_run_service_iam_member" "producer_invoker" {
  project  = var.project_id
  location = var.region
  service  = google_cloudfunctions2_function.producer.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.service_account_email}"
}

resource "google_cloud_scheduler_job" "producer_trigger" {
  name    = "alfabetizacao-streaming-producer-trigger"
  project = var.project_id
  region  = var.region
  # A cada 10 minutos — mesmo espírito do padrão de aula (EventBridge Scheduler
  # gerando eventos sintéticos periodicamente), ajustado para o free tier deste projeto.
  schedule  = "*/10 * * * *"
  time_zone = "America/Sao_Paulo"

  http_target {
    uri         = google_cloudfunctions2_function.producer.url
    http_method = "POST"
    body        = base64encode("{}")
    headers = {
      "Content-Type" = "application/json"
    }

    oidc_token {
      service_account_email = var.service_account_email
    }
  }
}