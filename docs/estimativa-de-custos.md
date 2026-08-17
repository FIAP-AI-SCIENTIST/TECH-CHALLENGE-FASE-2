# Estimativa de custos

Preços vigentes em 2026-08, verificados na tabela de preços oficial do Google Cloud (região US) em 2026-08-17, sobre o volume que o pipeline efetivamente produz — não sobre projeções.

## Objetivo

O projeto é dimensionado para rodar dentro dos **free tiers** do GCP. Como salvaguarda contra qualquer desvio dessa premissa, a infraestrutura provisiona um **orçamento de R$ 1,00** com alertas em 50%, 90% e 100% — o primeiro custo técnico, se existir, aparece em dias, não em meses.

## Recursos provisionados (Terraform)

| Recurso | Propósito | Modelo de cobrança |
|---|---|---|
| GCS bucket `{project}-datalake` | Bronze e Silver efêmeros (Parquet) + código do producer em `_deploy/` | Storage Standard: ~US$ 0,020/GB·mês |
| BigQuery dataset `alfabetizacao_analytics` (localização `US`) | Gold (5 dims, 7 facts, 3 marts) + `pipeline_audit_log` + `data_quality_log` | Query on-demand: US$ 6,25/TiB escaneado + storage US$ 0,02/GB·mês (ativo) |
| Pub/Sub topic + subscription | Streaming sintético (producer → consumer) | US$ 40/TiB de throughput + retenção |
| Cloud Function 2nd gen `alfabetizacao-streaming-producer` (256 MB, até 1 instância) | Publica eventos sintéticos | Cloud Run: vCPU-s e GB-s |
| Cloud Scheduler | Dispara o producer a cada 10 min (`*/10 * * * *`) | US$ 0,004/job (acima do free tier) |
| Service account (IAM), canal de monitoramento (e-mail), alerta de orçamento | Acessos, alertas, salvaguarda | sem custo |

Todos os recursos cobráveis levam cost labels (`pipeline`, `componente`), permitindo atribuir o custo por camada no relatório de faturamento.

## Volumes esperados

Volumes com piso garantido pelo gate de qualidade (`src/quality/rules.py`):

| Entidade | Linhas/ano (mínimo) |
|---|---|
| `alunos` | 100.000 |
| `municipio` | 1.000 |
| `meta_alfabetizacao_municipio` | 1.000 |
| `uf`, `meta_alfabetizacao_uf` | 27 |
| `meta_alfabetizacao_brasil` | 1 |

Na ordem de ~10⁶ linhas e algumas centenas de MB em Parquet (Bronze + Silver + Gold) em todos os anos. Por execução full (extração do dataset público + leituras Silver/Gold/qualidade), o volume escaneado fica na ordem de algumas centenas de MB — o valor medido por execução está em `pipeline_audit_log` (`total_bytes_processed` por passo).

## Estimativa por serviço

| Serviço | Consumo mensal | Free tier | Custo esperado |
|---|---|---|---|
| BigQuery (queries) | Algumas centenas de MB escaneados por execução, algumas execuções/semana | 1 TiB/mês | **R$ 0** |
| BigQuery (storage) | Algumas GB (Gold + tabelas operacionais) | — | centavos de US$/mês |
| Cloud Storage | Algumas GB | 50 GB/mês (Standard, US) | **R$ 0** |
| Pub/Sub | Eventos sintéticos em KB | 10 GiB de throughput + 24h de retenção | **R$ 0** |
| Cloud Function 2nd gen | 144 invocações/dia × 256 MB × ~2s ≈ 290 vCPU-s/dia | Cloud Run (180k vCPU-s/dia, 360k GB-s/dia) | **R$ 0** |
| Cloud Scheduler | 144 jobs/dia | 50 jobs/dia | **~US$ 11/mês (~R$ 60)** |
| IAM, monitoramento, orçamento | — | — | R$ 0 |

### Cloud Scheduler — único custo previsível

O schedule do producer é a cada 10 minutos (144 jobs/dia). Acima dos 50 jobs/dia do free tier: 94 jobs × US$ 0,004 ≈ **US$ 0,38/dia ≈ US$ 11/mês**. A escolha foi consciente: granularidade de streaming mais alta. Se o custo quisesse ser exatamente zero, um schedule horário (24 jobs/dia) caberia no free tier; o alerta de orçamento de R$ 1 é a salvaguarda contra essa estimativa estar errada.

## Total mensal

- **Esperado: ~R$ 0** (tudo dentro dos free tiers).
- **Pior caso: ~R$ 60/mês** (apenas Cloud Scheduler; nenhum outro serviço deve gerar custo).

## Como verificar no ambiente real

- `pipeline_audit_log`: `rows_read`, `rows_written`, `total_bytes_processed` por passo e por execução.
- Relatório de faturamento filtrado pelos cost labels (por componente).
- Console GCP → Orçamentos e alertas (alertas em 50/90/100% dos R$ 1,00).
