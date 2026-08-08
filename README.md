# Pipeline Híbrido de Análise da Alfabetização no Brasil

Tech Challenge Fase 2 (Pós FIAP) — pipeline de dados híbrido (batch + streaming) 100% nativo GCP para o **Indicador Criança Alfabetizada** (INEP, Pesquisa Alfabetiza Brasil 2023), fonte pública `basedosdados.br_inep_avaliacao_alfabetizacao` (BigQuery público).

## Contexto de negócio

O Compromisso Nacional Criança Alfabetizada é uma política pública (União + estados + DF + municípios) que busca garantir que toda criança brasileira esteja alfabetizada até o fim do 2º ano do ensino fundamental, com meta de 100% até 2030. O Indicador Criança Alfabetizada mede o percentual de estudantes que atingem o corte de 743 pontos na escala Saeb. Entender os fatores que influenciam esse resultado exige cruzar metas nacionais/estaduais/municipais, dados territoriais e desempenho — dados que a Base dos Dados expõe nativamente via BigQuery.

Este projeto simula o trabalho de um time de engenharia de dados de uma organização pública de análise educacional, entregando uma camada analítica confiável para subsidiar políticas públicas baseadas em evidência.

## Arquitetura

Arquitetura Lambda (camada batch + camada streaming convergindo na mesma camada Bronze), seguindo o padrão Medalhão (Bronze → Silver → Gold), 100% GCP.

```mermaid
flowchart LR
    subgraph Fonte
        BD[(BigQuery público\nbasedosdados)]
    end

    subgraph Batch
        BD -->|extract_full / extract_incremental| Extraction[extraction]
    end

    subgraph Streaming
        Scheduler[Cloud Scheduler\ncron] -->|HTTP + OIDC| CF[Cloud Function Gen2\nproducer]
        CF -->|publish| Topic[(Pub/Sub\nalfabetizacao-streaming-events)]
        Topic --> Consumer[consumer\npull + ack]
    end

    Extraction --> Bronze[(GCS — Bronze\nParquet particionado)]
    Consumer --> Bronze

    Bronze -.próxima unit.-> Silver[(Silver\nGCS/Parquet)]
    Silver -.próxima unit.-> Gold[(Gold\nBigQuery, modelo dimensional)]

    Extraction --> Audit[(BigQuery\naudit_log)]
    Consumer --> Audit
    Audit --> Alert[Alerta e-mail\nMonitoring]
```

- **Fonte**: BigQuery público (`basedosdados.br_inep_avaliacao_alfabetizacao`), sem exportação intermediária.
- **Batch**: `extraction.extract_full`/`extract_incremental` lêem do BigQuery público e gravam Parquet particionado por entidade/ano na Bronze (GCS). Full na primeira execução, incremental nas seguintes; lotes acima de `BATCH_THRESHOLD` são escritos em múltiplos arquivos por partição (`part-{n}.parquet`), sem apagar lotes anteriores.
- **Streaming**: `producer` gera eventos sintéticos (indicador/medição/meta) e publica no tópico Pub/Sub `alfabetizacao-streaming-events`, disparado por Cloud Scheduler via Cloud Function (Gen2). `consumer` faz pull da subscription, decodifica pelo contrato Pydantic correspondente e grava micro-batches na mesma Bronze, particionados por data de ingestão (`data_ingestao=YYYY-MM-DD`). A escrita é append-only: cada execução do consumer grava um arquivo próprio (`part-{run_id}.parquet`) e nunca limpa a partição do dia, então micro-batches sucessivos se somam em vez de se substituírem.
- **Contratos**: modelos Pydantic (`contracts/models.py`) validam e serializam para Arrow/Parquet, garantindo que Bronze batch e Bronze streaming escrevam sob o mesmo schema por entidade.
- **Observabilidade**: cada execução (batch ou streaming) registra uma linha na tabela de auditoria BigQuery (`alfabetizacao_analytics.pipeline_audit_log`) com `run_id`, linhas lidas/escritas, duração e status; logs estruturados em JSON; Consumer Lag do Pub/Sub monitorado via `num_undelivered_messages`; alerta por e-mail em erro. O `run_id` da auditoria é o mesmo que nomeia o arquivo Parquet do micro-batch de streaming, ligando cada arquivo da Bronze à sua linha de auditoria.
- **Silver e Gold** são as próximas units do roadmap (modelagem dimensional Kimball, SCD Tipo 2, integração das bases) — ainda não implementadas; a Bronze já está pronta para alimentá-las.

## Stack e por que essas escolhas

| Camada | Tecnologia | Justificativa |
|---|---|---|
| Extração/Streaming | Python + `google-cloud-bigquery`/`google-cloud-pubsub` | Sem Spark/Airflow self-hosted — volume da fonte (~4M linhas na maior entidade) não justifica cluster distribuído; Python simples cobre o caso |
| Formato de dados | Parquet particionado (hive-style) | Colunar, compressão eficiente, leitura seletiva por partição — custo de storage e de query menor |
| Streaming transporte | Pub/Sub (não Kafka) | Ver seção de trade-offs abaixo |
| Contratos de dados | Pydantic | Único schema por entidade compartilhado entre extração batch, producer e consumer streaming — evita drift de schema entre os dois caminhos que convergem na Bronze |
| IaC | Terraform | Infra 100% efêmera e reproduzível — sobe para demo, `terraform destroy` depois |
| Compute do Producer | Cloud Function Gen2 + Cloud Scheduler | Serverless, free tier, sem servidor para manter no ar entre execuções |
| Observabilidade | Cloud Logging + tabela de auditoria BigQuery + Monitoring | Suficiente para o escopo (sem dashboard dedicado); tudo dentro do free tier |

## Trade-offs arquiteturais

**Cloud única (GCP), não multi-cloud.** A fonte de dados (Base dos Dados/INEP) mora nativamente no BigQuery — não existe equivalente na AWS/Azure. Mesmo com uma camada de abstração multi-cloud, a extração continuaria presa ao GCP; portabilizar o resto seria engenharia sem retorno. Tirar os dados do BigQuery público para processar em outra nuvem geraria custo real de egress, o que colide direto com o orçamento free-tier do projeto. Terraform também não abstrai providers de forma nativa — "agnóstico" significaria manter 2-3 implementações paralelas por módulo, triplicando a superfície de bugs para um requisito que o desafio não pede (pede escolha justificada, não portabilidade).

**Pub/Sub em vez de Kafka.** O padrão publish/subscribe (tópico → subscription/consumer group, semântica at-least-once, monitoramento de lag) é o mesmo, mas Kafka self-hosted (ou mesmo um serviço gerenciado como Confluent Cloud/MSK) não tem free tier real, e o projeto já está comprometido com um único provedor gerenciado (GCP). Pub/Sub cobre publish/subscribe, consumer lag e entrega at-least-once dentro do orçamento zero.

**Sem camada de staging antes da Bronze.** Como a fonte já é uma tabela estruturada e confiável do BigQuery público (não um arquivo solto ou API instável), a extração aplica o contrato Pydantic direto na leitura e grava já na Bronze — uma camada de staging intermediária existiria só para reformatar algo que já chega formatado.

**Parquet puro (sem open table format — Delta/Iceberg/Hudi).** Sem ACID multi-writer nem time travel; no lugar disso, cada caminho de escrita tem uma regra explícita de posse da partição:

- **Batch** é dono da partição `ano=`: `clear_partition` limpa o prefixo **uma única vez por `(entidade, ano)` no início do run**, nunca dentro do loop de lotes, e cada lote grava seu próprio `part-{i}.parquet`. Reextrair um ano substitui aquele ano inteiro, de forma determinística.
- **Streaming** não é dono da partição `data_ingestao=`: ela é compartilhada por todos os micro-batches do dia, então o consumer **nunca** limpa nada e nomeia o arquivo pelo `run_id` da execução (`part-{run_id}.parquet`), o que torna a colisão com um run anterior impossível.

Isso preserva o histórico da Bronze sem depender de transações, ao custo de não ter merge/upsert. Se a Silver (SCD Tipo 2) precisar de merge incremental mais sofisticado, um open table format entra como candidato natural nessa unit futura.

**NoSQL fora do MVP.** Pelo CAP Theorem e pela decisão de persistência poliglota, o projeto não introduz um banco NoSQL de serving no MVP — o BigQuery (Gold, unit futura) já cobre consulta analítica dimensional. Um caso de uso de IA aplicada (ex.: buscar municípios com perfil educacional similar via embeddings, servidos por um banco vetorial) fica registrado como extensão natural pós-MVP, não como lacuna do design atual.

## FinOps

- **Orçamento**: `google_billing_budget` monitorando a conta de faturamento, alerta em 50%/90%/100% de R$ 1,00 — o GCP não aceita um valor de budget zero, então R$ 1,00 é o menor teto configurável para sinalizar qualquer gasto que fuja do free tier, não um limite de consumo esperado.
- **Free tier estrito**: nenhum serviço sem free tier generoso entra no design (sem Dataflow, Composer ou Dataproc); GCS, BigQuery (1TB de query/mês), Pub/Sub (10GB/mês) e Cloud Functions (2M invocações/mês) cobrem o volume do projeto.
- **Infraestrutura efêmera**: todo o Terraform é desenhado para subir e cair sem sujeira — bucket com `force_destroy`, dataset com `delete_contents_on_destroy = true`, tabela de auditoria sem `deletion_protection`. Suba só para testar/demonstrar, rode `make infra-destroy` depois. A única exceção é o bucket de state (`<project>-tfstate`), criado fora do Terraform pelo `bootstrap.sh` justamente por ser pré-requisito dele — some com ele à mão quando encerrar o projeto de vez.
- **Egress**: co-localizar o processamento na mesma nuvem da fonte (BigQuery público) evita custo de transferência entre nuvens — ver trade-off de cloud única acima.
- **Least privilege como controle de custo indireto**: a service account central (`alfabetizacao-pipeline-sa`) recebe papéis com escopo de recurso sempre que o GCP oferece um (Storage Object Admin no bucket específico, BigQuery Data Editor no dataset específico, Pub/Sub Publisher/Subscriber no tópico/subscription específicos). Dois papéis não têm equivalente com escopo de recurso no IAM do GCP e ficam necessariamente no nível do projeto: `roles/bigquery.jobUser` (rodar query/insert é uma operação de projeto, não de dataset) e `roles/monitoring.viewer` (ler a métrica de Consumer Lag). Nada além disso — reduz a superfície de uso indevido de cota.

## Estrutura do repositório

```
src/
├── contracts/       # Modelos Pydantic (schema por entidade) + mapeamento Arrow + serialização Parquet
├── extraction/       # Extração full/incremental do BigQuery público -> Bronze
├── bronze/           # Leitura/escrita de partições Parquet no GCS
├── streaming/         # Producer (eventos sintéticos -> Pub/Sub) e Consumer (Pub/Sub -> Bronze)
├── common/            # Retry compartilhado (backoff exponencial)
└── observability/     # Logging estruturado, auditoria BigQuery e monitoramento (Consumer Lag)
infra/                 # Terraform: toda a infra GCP (efêmera — sobe e destrói por demanda)
tests/                 # Testes espelhando src/, incluindo Property-Based Testing (Hypothesis)
docs/                  # Diagrama de arquitetura (fonte Excalidraw + PNG)
```

## Pré-requisitos

- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install) autenticado, com acesso IAM ao projeto GCP alvo
- [Terraform](https://developer.hashicorp.com/terraform/install) (provider `google ~> 5.0`)
- Python >= 3.11

## 1. Configurar credenciais

```bash
gcloud auth login
gcloud auth application-default login

cp .env.example .env   # preencher com os valores do projeto (nunca commitar o .env real)
```

Edite o `.env` e preencha `TF_VAR_billing_account` e `TF_VAR_alert_email` com
os valores reais que o Thyago passar. Ele também te passa o
`TF_VAR_team_members` já preenchido — **use o mesmo mapa que todo mundo do
grupo**, não só o seu e-mail.

> ⚠️ **Importante:** `team_members` é gerenciado como um mapa único pelo
> Terraform. Se o seu `.env` tiver só o seu e-mail nesse mapa e você rodar
> `apply`/`destroy`, o Terraform **revoga o acesso de todo mundo que não
> estiver no seu mapa**. Sempre use a cópia mais atual que o Thyago
> compartilhar, não invente a sua.

```bash
source .env
```

## 2. Subir a infraestrutura

O Terraform guarda o state num bucket GCS (state locking, porque mais de uma pessoa aplica no mesmo projeto). Esse bucket é pré-requisito do próprio `terraform init`, então não dá para o Terraform criá-lo — `bootstrap.sh` resolve o ovo-e-galinha criando o bucket e habilitando as duas APIs (Resource Manager e IAM) sem as quais o `refresh` trava antes de conseguir habilitar as demais.

```bash
bash infra/bootstrap.sh $TF_VAR_project_id $TF_VAR_gcs_location   # só uma vez por projeto GCP

make infra-init PROJECT_ID=$TF_VAR_project_id   # só na primeira vez (ou se infra/.terraform sumir)
make infra-plan                                 # empacota o Producer e mostra o que será criado
make infra-apply                                # cria dataset BigQuery, bucket GCS, Pub/Sub, SA, budget, monitoring, Cloud Function + Scheduler
```

`infra-apply` só roda a partir da branch `main`, com a working tree limpa e sincronizada com `origin/main` (`infra/apply-guard.sh` bloqueia isso de propósito, para evitar duas pessoas aplicando mudanças conflitantes em paralelo no mesmo projeto GCP compartilhado).

## 3. Rodar e testar

```bash
make install                                     # cria venv e instala o pacote em modo dev
make test                                        # roda toda a suíte (contratos, bronze, extração, observabilidade, streaming)

make bronze                                      # extrai as 6 entidades do BigQuery público -> Bronze (batch)
make streaming-producer TIPO=indicador N=5        # publica 5 eventos sintéticos no Pub/Sub
make streaming-consumer                          # consome o lote disponível e grava na Bronze
```

Em produção, o Producer roda sozinho via Cloud Scheduler → Cloud Function (sem intervenção manual); o Consumer, por ora, roda sob demanda (`make streaming-consumer`) — um pull single-shot não tem o mesmo encaixe natural de agendamento que o Producer tem.

**Por que só o caminho de streaming é agendado.** A extração batch (`make bronze`) roda sob demanda de propósito: a fonte é uma avaliação censitária anual do INEP e a extração incremental é particionada por ano (`extract_incremental` só busca `ano > max(ano já na Bronze)`). Agendar um job diário ou horário contra uma base que muda uma vez por ano gasta cota para não encontrar nada. O gatilho natural é a publicação de uma nova safra, que é um evento manual — quando isso deixar de valer (ou quando a Silver precisar de recomputação periódica), o caminho pronto é um Cloud Run Job com o mesmo Cloud Scheduler que já dispara o Producer.
## 4. Destruir a infraestrutura — sempre que terminar de testar

```bash
make infra-destroy
```

Todos os recursos gerenciados pelo Terraform foram desenhados para serem efêmeros de propósito — o `destroy` funciona limpo, sem sujeira. O bucket de state (`<project>-tfstate`) fica de fora, porque é ele que guarda o próprio state; remova à mão (`gcloud storage rm -r gs://$TF_VAR_project_id-tfstate`) só quando encerrar o projeto de vez. Não deixe a infra provisionada depois do seu teste; o orçamento de R$ 1,00 é só um alerta, não um limite automático que corta o projeto.

## Qualidade e testes

Suíte de testes unitários e de contrato por camada (`tests/`), incluindo Property-Based Testing (Hypothesis) nas funções puras de contratos (round-trip de serialização, invariantes de schema). Validação de qualidade de dados (duplicidade, nulos, chaves, consistência) fica para a unit de Data Quality, sobre Silver/Gold.

## Aplicação em IA

A camada Gold (modelo dimensional, unit futura) é o ponto natural para alimentar modelos preditivos e analíticos sobre o indicador de alfabetização:

- **Predição de risco de não-alfabetização**: modelo supervisionado (features territoriais + metas históricas + série temporal por município) para sinalizar municípios/escolas com maior probabilidade de ficar abaixo da meta, permitindo intervenção antes do resultado da avaliação.
- **Desigualdade educacional**: clusterização de municípios por perfil socioeducacional (combinando indicador de alfabetização com dados territoriais) para identificar grupos comparáveis e medir o efeito real de políticas públicas, isolando o contexto socioeconômico.
- **Apoio à decisão de política pública**: séries temporais por UF/município para simular cenários ("o que aconteceria com a meta nacional se a UF X replicasse a trajetória da UF Y") e priorizar investimento onde o retorno marginal em alfabetização é maior.
- **Busca por similaridade**: um banco vetorial sobre embeddings de perfil municipal (ver trade-off de NoSQL acima) permitiria consultas como "encontrar municípios com contexto parecido ao do município X" — útil para transferência de boas práticas entre gestões locais comparáveis.

Nenhum desses modelos está implementado neste MVP — dependem da Gold materializada (Silver/Gold são as próximas units); a arquitetura atual (contratos tipados, Bronze imutável, Gold dimensional planejada) já organiza os dados no formato que esse tipo de modelo consome.

## Roadmap

- [x] Contratos de dados (Pydantic + Arrow/Parquet)
- [x] Infraestrutura base (Terraform: storage, BigQuery, Pub/Sub, IAM, budget, monitoring)
- [x] Observabilidade (logging, auditoria, monitoramento)
- [x] Bronze — ingestão batch (extração full/incremental do BigQuery público)
- [x] Bronze — ingestão streaming (Producer sintético + Consumer, Cloud Function + Scheduler)
- [ ] Silver — limpeza, padronização, normalização de chaves, SCD Tipo 2
- [ ] Gold — modelo dimensional (Kimball) materializado no BigQuery
- [ ] Data Quality — testes de qualidade mapeados às seis dimensões (dbt/Great Expectations)

## Evidências de execução

_(a preencher com prints/link de vídeo de cada camada rodando de verdade — ver seção de entrega)._
