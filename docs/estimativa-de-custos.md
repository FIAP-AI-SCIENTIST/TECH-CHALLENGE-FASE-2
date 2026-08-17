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

Na ordem de ~10⁶ linhas e algumas centenas de MB em Parquet (Bronze + Silver + Gold) em todos os anos. Por execução full (extração do dataset público + leituras Silver/Gold/qualidade), o volume escaneado fica na ordem de algumas centenas de MB — o valor medido por execução está em `pipeline_audit_log` (`total_bytes_processed` por passo), com a ressalva do cache de resultados explicada na seção de medições abaixo.

## Medições reais (rodada de 2026-08-17)

Rodada completa de `make pipeline-from-scratch` (destroy de 30 recursos → apply de 36 → batch + streaming + gate), medida em `pipeline_audit_log`, `data_quality_log` e `INFORMATION_SCHEMA.JOBS`:

| Etapa | Entidade | Linhas lidas | Linhas escritas | Duração |
|---|---|---:|---:|---:|
| Bronze | uf | 145 | 145 | 7,3 s |
| Bronze | municipio | 23.995 | 23.995 | 11,7 s |
| Bronze | meta_alfabetizacao_brasil | 3 | 3 | 8,7 s |
| Bronze | meta_alfabetizacao_uf | 81 | 81 | 8,0 s |
| Bronze | meta_alfabetizacao_municipio | 10.704 | 10.704 | 10,0 s |
| Bronze | alunos | 3.867.999 | 3.867.999 | 985,3 s |
| Silver | 6 entidades | 3.902.927 | 3.902.920 | ~278 s (total) |
| Gold | 5 dims + 7 fatos + 3 marts | — | — | ~161 s (total) |
| Qualidade | 2 passes completos × 76 checks + inline Silver | — | — | 212 vereditos, 0 falhas |

- **Total extraído**: 3.902.927 linhas; `rows_read == rows_written` nas 6 extrações — zero rejeições de contrato em dados reais. Na Silver, a deduplicação por chave de negócio aparece: `meta_alfabetizacao_uf` 81 → 80 e `meta_alfabetizacao_municipio` 10.704 → 10.698.
- **Bytes processados = 0 nas extrações — não é bug, é cache.** As queries contra a fonte pública foram servidas pelo cache de resultados do BigQuery (a fonte é imutável entre rodadas e o cache vale ~24 h, independente do ciclo destroy/apply das tabelas do projeto). Medição agregada da janela via `INFORMATION_SCHEMA.JOBS`: 94 query jobs, 37 servidos de cache, 31,6 MB processados / 125,8 MB cobrados → **~US$ 0,0008** na rodada.
- **Cenário frio (sem cache)**: o scan completo das 6 tabelas da fonte soma **≈ 259 MiB** (`alunos` 256,1 MiB + `municipio` 1,7 + `meta_alfabetizacao_municipio` 1,1; demais ≈ 0), medido em `__TABLES__` do dataset público. A US$ 6,25/TiB, uma rodada fria custa **~US$ 0,0016**; o free tier de 1 TiB/mês comporta **~4.000 rodadas completas**.

## Estimativa por serviço

| Serviço | Consumo mensal | Free tier | Custo esperado |
|---|---|---|---|
| BigQuery (queries) | Algumas centenas de MB escaneados por execução, algumas execuções/semana | 1 TiB/mês | **R$ 0** |
| BigQuery (storage) | Algumas GB (Gold + tabelas operacionais) | — | centavos de US$/mês |
| Cloud Storage | Algumas GB | 50 GB/mês (Standard, US) | **R$ 0** |
| Pub/Sub | Eventos sintéticos em KB | 10 GiB de throughput + 24h de retenção | **R$ 0** |
| Cloud Function 2nd gen | 144 invocações/dia × 256 MB × ~2s ≈ 290 vCPU-s/dia | Cloud Run (180k vCPU-s/dia, 360k GB-s/dia) | **R$ 0** |
| Cloud Scheduler | 1 job (`*/10 * * * *`) | 3 jobs/mês | **R$ 0** |
| IAM, monitoramento, orçamento | — | — | R$ 0 |

### Cloud Scheduler — cobrança por job, não por execução

O Cloud Scheduler cobra por **job agendado** (a definição), não por execução: US$ 0,10/job/mês, com free tier de 3 jobs/mês por conta de faturamento. O projeto tem exatamente **1 job** (o que dispara o producer a cada 10 min) — as ~4.300 execuções/mês que ele gera não são cobradas pelo Scheduler (o alvo, Cloud Function, tem seu próprio free tier, já contabilizado acima). Custo efetivo: **R$ 0**.

## Total mensal

- **Esperado: R$ 0** (tudo dentro dos free tiers, incluindo o Scheduler).
- **Pior caso: centavos de US$/mês** (storage BigQuery/GCS se os dados persistirem além da demonstração; o alerta de orçamento de R$ 1 é a salvaguarda contra qualquer desvio dessa estimativa).

## Como verificar no ambiente real

- `pipeline_audit_log`: `rows_read`, `rows_written`, `total_bytes_processed` por passo e por execução.
- Relatório de faturamento filtrado pelos cost labels (por componente).
- Console GCP → Orçamentos e alertas (alertas em 50/90/100% dos R$ 1,00).
