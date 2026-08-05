# Budget Alert: configurado com R$ 1,00 para garantir que sejamos notificados
# no mínimo gasto técnico que desvie do Free Tier, cumprindo NFR01.
# GCP não permite amounts vazios ou zero.
resource "google_billing_budget" "budget_alert" {
  billing_account = var.billing_account
  display_name    = "Alerta Free Tier Alfabetizacao"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "BRL"
      units         = "1" # 1 BRL
    }
  }

  # Notifica ao atingir 50%, 90% e 100% de R$ 1,00.
  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }

  all_updates_rule {
    # Reuse do notification channel do modulo Monitoring (e-mail da equipe).
    monitoring_notification_channels = [var.notification_channel_id]
    disable_default_iam_recipients   = true # Apenas quem está no email_alert receberá
  }
}
