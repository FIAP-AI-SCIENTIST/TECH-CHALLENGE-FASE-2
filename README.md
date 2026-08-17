# Pipeline Híbrido de Análise da Alfabetização no Brasil

Tech Challenge Fase 2 (Pós FIAP) — pipeline de dados híbrido (batch + streaming) sobre GCP (Bronze, streaming e Gold), com a Silver processada por DuckDB embarcado e persistida no GCS, para o **Indicador Criança Alfabetizada** (INEP, Pesquisa Alfabetiza Brasil 2023), fonte pública `basedosdados.br_inep_avaliacao_alfabetizacao` (BigQuery público).

## Contexto de negócio

O Compromisso Nacional Criança Alfabetizada é uma política pública (União + estados + DF + municípios) que busca garantir que toda criança brasileira esteja alfabetizada até o fim do 2º ano do ensino fundamental, com meta de 100% até 2030. O Indicador Criança Alfabetizada mede o percentual de estudantes que atingem o corte de 743 pontos na escala Saeb. Entender os fatores que influenciam esse resultado exige cruzar metas nacionais/estaduais/municipais, dados territoriais e desempenho — dados que a Base dos Dados expõe nativamente via BigQuery.

Este projeto simula o trabalho de um time de engenharia de dados de uma organização pública de análise educacional, entregando uma camada analítica confiável para subsidiar políticas públicas baseadas em evidência.

## Arquitetura

Arquitetura Lambda (camada batch + camada streaming convergindo na mesma camada Bronze), seguindo o padrão Medalhão (Bronze → Silver → Gold): GCP para Bronze, streaming e Gold; a Silver é processada com DuckDB embarcado no mesmo processo Python e persistida em Parquet no GCS (justificativa na seção de trade-offs).

![Arquitetura do pipeline](docs/arquitetura.png)

- **Fonte**: BigQuery público (`basedosdados.br_inep_avaliacao_alfabetizacao`), sem exportação intermediária.
- **Batch**: `extraction.extract_full`/`extract_incremental` lêem do BigQuery público e gravam Parquet particionado por entidade/ano na Bronze (GCS). Full na primeira execução, incremental nas seguintes; lotes acima de `BATCH_THRESHOLD` são escritos em múltiplos arquivos por partição (`part-{n}.parquet`), sem apagar lotes anteriores. Toda query do pipeline carrega um teto de custo (`maximum_bytes_billed` de 10 GB): scan acidental caro falha em vez de cobrar.
- **Streaming**: `producer` gera eventos sintéticos (indicador/medição/meta) e publica no tópico Pub/Sub `alfabetizacao-streaming-events`, disparado por Cloud Scheduler via Cloud Function (Gen2). `consumer` faz pull da subscription, decodifica pelo contrato Pydantic correspondente e grava micro-batches na mesma Bronze, particionados por data de ingestão (`data_ingestao=YYYY-MM-DD`). A escrita é append-only: cada execução do consumer grava um arquivo próprio (`part-{run_id}.parquet`) e nunca limpa a partição do dia, então micro-batches sucessivos se somam em vez de se substituírem. O `publish_time` da mensagem vira a coluna `data_evento` na linha — o event time que permite medir o lag ponta a ponta (ingestão − evento), não apenas o lag da fila.
- **Contratos**: modelos Pydantic (`contracts/models.py`) validam e serializam para Arrow/Parquet, garantindo que Bronze batch e Bronze streaming escrevam sob o mesmo schema por entidade.
- **Observabilidade**: cada execução (batch ou streaming) registra uma linha na tabela de auditoria BigQuery (`alfabetizacao_analytics.pipeline_audit_log`) com `run_id`, linhas lidas/escritas, duração e status; logs estruturados em JSON; Consumer Lag do Pub/Sub monitorado via `num_undelivered_messages`; alerta por e-mail em erro. O `run_id` da auditoria é o mesmo que nomeia o arquivo Parquet do micro-batch de streaming, ligando cada arquivo da Bronze à sua linha de auditoria.
- **Silver** (DuckDB local): `silver.pipeline.run_silver` lê a Bronze inteira de uma entidade, traduz códigos (`rede`/`serie`) e enriquece com os diretórios de UF/município (extraídos sob demanda do BigQuery público — são metadado de tradução, não dado bruto, então não passam pela Bronze), normaliza `id_municipio` para 7 dígitos IBGE, deduplica por chave de negócio (resolve a reentrega at-least-once do streaming) e aplica SCD Tipo 2 nas três entidades de meta (nova versão só quando as colunas rastreadas mudam — incluindo o resultado observado). Também materializa a primeira tabela que cruza duas entidades de source: `alfabetizacao_municipio_integrado` (indicador municipal × meta municipal do mesmo ano). Ao fim de cada entidade, os checks de qualidade rodam inline sobre o frame em memória, e o run fecha com reconciliação Bronze→Silver (≥ 90% das linhas). Grava de volta no GCS: `ano=` para as entidades regulares (uf/município/alunos), tabela cumulativa sem partição para as de meta.
- **Gold**: `gold.pipeline.run_gold` lê a Silver e materializa um modelo dimensional (Kimball) no BigQuery (`alfabetizacao_analytics`): 5 dimensões (`dim_uf`, `dim_municipio`, `dim_rede`, `dim_serie`, `dim_tempo`), 7 fatos (`fact_indicador_uf`, `fact_indicador_municipio`, `fact_alunos`, `fact_alfabetizacao_municipio` — meta e resultado na mesma linha — e `fact_meta_resultado_{brasil,uf,municipio}`) e 3 marts (views prontas para consumo: `mart_evolucao_indicador_uf`, `mart_aderencia_metas_uf`, `mart_ranking_indicador_municipio`). Chaves substitutas determinísticas (SHA-256 da chave natural, 8 bytes com sinal → INT64) e PK/FK declaradas `NOT ENFORCED` — a integridade é garantida pela construção e verificada pela qualidade, não pelo banco. Cada tabela é recriada do zero a cada execução (`WRITE_TRUNCATE`) — sem merge incremental, sem estado próprio. Modelo completo em [docs/modelo-dimensional.md](docs/modelo-dimensional.md).
- **Qualidade**: checks declarativos em `src/quality/rules.py` executados por Great Expectations, mapeados às seis dimensões clássicas (unicidade, completude, validade, consistência, precisão, atualidade), com severidade `CRITICA`/`AVISO`; a evidência de cada check é persistida em `alfabetizacao_analytics.data_quality_log`. Detalhes em [docs/qualidade-dados.md](docs/qualidade-dados.md).
- **FinOps**: orçamento de R$ 1,00 + alertas como salvaguarda, free tiers dimensionando o design, cost labels por componente e ciclo de vida do dado bruto no bucket. Estimativa completa em [docs/estimativa-de-custos.md](docs/estimativa-de-custos.md).

## Stack e por que essas escolhas

| Camada | Tecnologia | Justificativa |
|---|---|---|
| Extração/Streaming | Python + `google-cloud-bigquery`/`google-cloud-pubsub` | Sem Spark/Airflow self-hosted — volume da fonte (~4M linhas na maior entidade) não justifica cluster distribuído (ver "Spark fora do desenho" nos trade-offs abaixo); Python single-node cobre o caso |
| Formato de dados | Parquet particionado (hive-style) | Colunar, compressão eficiente, leitura seletiva por partição — custo de storage e de query menor |
| Streaming transporte | Pub/Sub (não Kafka) | Ver seção de trade-offs abaixo |
| Contratos de dados | Pydantic | Único schema por entidade compartilhado entre extração batch, producer e consumer streaming — evita drift de schema entre os dois caminhos que convergem na Bronze |
| IaC | Terraform | Infra 100% efêmera e reproduzível — sobe para demo, `terraform destroy` depois |
| Compute do Producer | Cloud Function Gen2 + Cloud Scheduler | Serverless, free tier, sem servidor para manter no ar entre execuções |
| Observabilidade | Cloud Logging + tabela de auditoria BigQuery + Monitoring | Suficiente para o escopo (sem dashboard dedicado); tudo dentro do free tier |

## Trade-offs arquiteturais

**Cloud única (GCP), não multi-cloud.** A fonte de dados (Base dos Dados/INEP) mora nativamente no BigQuery — não existe equivalente na AWS/Azure. Mesmo com uma camada de abstração multi-cloud, a extração continuaria presa ao GCP; portabilizar o resto seria engenharia sem retorno. Tirar os dados do BigQuery público para processar em outra nuvem geraria custo real de egress, o que colide direto com o orçamento free-tier do projeto. Terraform também não abstrai providers de forma nativa — "agnóstico" significaria manter 2-3 implementações paralelas por módulo, triplicando a superfície de bugs para um requisito que o desafio não pede (pede escolha justificada, não portabilidade).

**Pub/Sub em vez de Kafka.** O padrão publish/subscribe (tópico → subscription/consumer group, semântica at-least-once, monitoramento de lag) é o mesmo, mas Kafka self-hosted (ou mesmo um serviço gerenciado como Confluent Cloud/MSK) não tem free tier real, e o projeto já está comprometido com um único provedor gerenciado (GCP). Pub/Sub cobre publish/subscribe, consumer lag e entrega at-least-once dentro do orçamento zero.

**Spark fora do desenho (processamento single-node de propósito).** O volume da fonte não paga um engine distribuído: a maior entidade tem ~4M de linhas e as demais ficam na casa das dezenas de milhares — carga que uma única máquina processa em segundos. Nessa escala o gargalo é I/O de rede com a fonte, não CPU paralela, e Spark não remove I/O. O processamento pesado já está delegado aos engines certos para o tamanho do problema: o scan e as agregações sobre a fonte rodam dentro do próprio BigQuery (que é um engine distribuído — só que gerenciado e dentro do free tier), e as transformações set-based da Silver/Gold rodam em DuckDB no mesmo processo Python, sem cluster para provisionar. O que se evita é concreto: mesmo o Dataproc Serverless — que tem free tier (500 DCU-hora e 2000 GB-hora de shuffle por mês) e em modo batch é efêmero (o cluster sobe, processa e morre, sem faturar 24/7) — cobraria por job algo que o pipeline já resolve de graça, e tunar executores, partições e shuffle é complexidade operacional sem retorno nesse volume. Se o volume crescer ordens de grandeza (ex.: censo escolar nacional no grão do aluno, centenas de milhões de linhas), o caminho natural é Dataproc Serverless ou empurrar as transformações para dentro do BigQuery — a decisão é revisitável, não um veto à ferramenta.

**Sem camada de staging antes da Bronze.** Como a fonte já é uma tabela estruturada e confiável do BigQuery público (não um arquivo solto ou API instável), a extração aplica o contrato Pydantic direto na leitura e grava já na Bronze — uma camada de staging intermediária existiria só para reformatar algo que já chega formatado.

**Parquet puro (sem open table format — Delta/Iceberg/Hudi).** Sem ACID multi-writer nem time travel; no lugar disso, cada caminho de escrita tem uma regra explícita de posse da partição:

- **Batch** é dono da partição `ano=`: `clear_partition` limpa o prefixo **uma única vez por `(entidade, ano)` no início do run**, nunca dentro do loop de lotes, e cada lote grava seu próprio `part-{i}.parquet`. Reextrair um ano substitui aquele ano inteiro, de forma determinística.
- **Streaming** não é dono da partição `data_ingestao=`: ela é compartilhada por todos os micro-batches do dia, então o consumer **nunca** limpa nada e nomeia o arquivo pelo `run_id` da execução (`part-{run_id}.parquet`), o que torna a colisão com um run anterior impossível.

Isso preserva o histórico da Bronze sem depender de transações, ao custo de não ter merge/upsert.

**Cadência do recompute: o streaming é near-real-time na ingestão, não no serving.** O Consumer leva o evento ao Bronze em segundos, mas as camadas derivadas recomputam **por ciclo**, não por mensagem — e isso é consequência direta da decisão acima. Sem merge/upsert, `run_silver` lê toda a Bronze da entidade e reescreve as partições `ano=` (o SCD Tipo 2 é replayado do zero, o que é o que o torna idempotente), e a Gold rematerializa as tabelas com `WRITE_TRUNCATE`. Medido na rodada de 2026-08-17: **Silver ~4m23s** (3,9M linhas) **+ Gold ~2m56s** por passada. Disparar isso a cada micro-batch consumido significaria recomputar o histórico inteiro para processar algumas dezenas de eventos — por isso `pipeline-from-scratch` reprocessa uma vez ao fim do ciclo, e nada no projeto encadeia Consumer → Silver → Gold como gatilho automático. É a camada batch da arquitetura Lambda fazendo o seu papel: a latência analítica é a cadência do recompute, não a do evento. Baixá-la sem recomputar tudo exige merge incremental por chave de negócio, que é precisamente onde um open table format (Iceberg/Delta) deixa de ser luxo e passa a ser o componente que falta — registrado como evolução, não como lacuna acidental.

**NoSQL fora do MVP.** Pelo CAP Theorem e pela decisão de persistência poliglota, o projeto não introduz um banco NoSQL de serving no MVP — o BigQuery (Gold) já cobre consulta analítica dimensional. Um caso de uso de IA aplicada (ex.: buscar municípios com perfil educacional similar via embeddings, servidos por um banco vetorial) fica registrado como extensão natural pós-MVP, não como lacuna do design atual.

**Gold materializada via load job direto no BigQuery, não dbt.** O comentário original do Terraform previa dbt para a Gold (`main.tf` do módulo BigQuery), mas o projeto não tem — nem precisa de — um projeto dbt para 17 tabelas: introduzir `profiles.yml`, modelos SQL e o runner do dbt só para recriar o que o Python já faz (DuckDB para o SQL set-based, PyArrow para o schema) duplicaria ferramenta para o mesmo resultado. Cada dimensão/fato é uma função pura testável em `gold.transform` (mesmo padrão de `silver.transform`); a escrita usa `load_table_from_file` com `WRITE_TRUNCATE` e schema inferido do próprio Parquet — sem exigir DDL prévio no Terraform, sem staging, sem camada extra de orquestração SQL.

**Comparação meta x resultado usa o próprio ano da versão SCD2, não um join com os fatos de indicador.** As tabelas de meta já carregam, na mesma linha, o resultado observado (`taxa_alfabetizacao`) e a trajetória de metas futuras (`meta_alfabetizacao_2024..2030`) — comparar contra o alvo do próprio ano da linha evita um join client-side com granularidade diferente (indicador é por `serie`, meta não). Para que essa comparação tenha uma linha por ano, o SCD2 rastreia o **resultado observado** junto com a trajetória de metas e a participação: dois anos consecutivos só colapsam numa versão única se as três coisas forem idênticas. Rastrear apenas a trajetória faria o ano cujo alvo repetisse o anterior herdar a linha antiga inteira, levando consigo a taxa de alfabetização defasada — ou seja, o fato perderia justamente o número que existe para comparar.

## FinOps

- **Orçamento**: `google_billing_budget` monitorando a conta de faturamento, alerta em 50%/90%/100% de R$ 1,00 — o GCP não aceita um valor de budget zero, então R$ 1,00 é o menor teto configurável para sinalizar qualquer gasto que fuja do free tier, não um limite de consumo esperado.
- **Free tier estrito**: nenhum serviço sem free tier generoso entra no design (sem Dataflow, Composer ou Dataproc); GCS, BigQuery (1TB de query/mês), Pub/Sub (10GB/mês) e Cloud Functions (2M invocações/mês) cobrem o volume do projeto.
- **Infraestrutura efêmera**: todo o Terraform é desenhado para subir e cair sem sujeira — bucket com `force_destroy`, dataset com `delete_contents_on_destroy = true`, tabela de auditoria sem `deletion_protection`. Suba só para testar/demonstrar, rode `make infra-destroy` depois. A única exceção é o bucket de state (`<project>-tfstate`), criado fora do Terraform pelo `bootstrap.sh` justamente por ser pré-requisito dele — some com ele à mão quando encerrar o projeto de vez.
- **Acesso humano fora do ciclo efêmero**: `google_project_iam_member`/`google_billing_account_iam_member` do time (`roles/viewer`/`roles/editor`) moram num state Terraform separado (`infra/team-access`), não no state principal (`infra/`). Só o segundo é ephemeral — `make infra-destroy` roda `terraform destroy` sem `-target`, então qualquer IAM binding que esteja no mesmo state seria destruído junto; separar o state é a única forma de garantir que derrubar a infra de demo nunca revoga o acesso do grupo ao projeto.
- **Egress**: co-localizar o processamento na mesma nuvem da fonte (BigQuery público) evita custo de transferência entre nuvens — ver trade-off de cloud única acima.
- **Ciclo de vida do dado bruto**: o bucket do data lake degrada o storage conforme o dado envelhece (30+ dias → Nearline, 180+ dias → Coldline) e aborta multipart uploads interrompidos após 1 dia. O acesso ao dado bruto antigo é raro — a Bronze é a camada de replay (a Silver é recomputada a partir dela, e a própria Bronze pode ser reextraída da fonte pública) — então o histórico bruto não vira custo morto em Standard.
- **Estimativa de custos**: a planilha completa (recurso × consumo × free tier × preço, com o pior caso) está em [docs/estimativa-de-custos.md](docs/estimativa-de-custos.md).
- **Gold recomputada, não incremental**: com o volume atual (dezenas de milhares de linhas nas maiores entidades), reler a Silver inteira e sobrescrever a Gold por completo (`WRITE_TRUNCATE`) é mais simples e mais barato em engenharia do que rastrear o que mudou — o custo de processamento fica dentro do free tier de BigQuery (1TB de query/mês cobre esse load job com folga). Se o volume crescer a ponto de o full-recompute pesar no orçamento, merge incremental por partição de `ano` é o próximo passo natural.
- **Least privilege como controle de custo indireto**: a service account central (`alfabetizacao-pipeline-sa`) recebe papéis com escopo de recurso sempre que o GCP oferece um (Storage Object Admin no bucket específico, BigQuery Data Editor no dataset específico, Pub/Sub Publisher/Subscriber no tópico/subscription específicos). Dois papéis não têm equivalente com escopo de recurso no IAM do GCP e ficam necessariamente no nível do projeto: `roles/bigquery.jobUser` (rodar query/insert é uma operação de projeto, não de dataset) e `roles/monitoring.viewer` (ler a métrica de Consumer Lag). Nada além disso — reduz a superfície de uso indevido de cota.

## Estrutura do repositório

```
src/
├── config.py          # Configuração centralizada (pydantic-settings, fail-closed; .env + alias TF_VAR)
├── contracts/         # Modelos Pydantic (schema por entidade) + mapeamento Arrow + serialização Parquet
├── extraction/        # Extração full/incremental do BigQuery público -> Bronze
├── bronze/            # Leitura/escrita de partições Parquet no GCS
├── streaming/         # Producer (eventos sintéticos -> Pub/Sub) e Consumer (Pub/Sub -> Bronze)
├── silver/            # Limpeza, tradução de código, normalização de chave, dedup, SCD Tipo 2, reconciliação
├── gold/              # Modelo dimensional (Kimball) materializado no BigQuery
├── quality/           # Data Quality: registry declarativo, checks Great Expectations, gate e evidência
├── common/            # Retry compartilhado (backoff exponencial) e lock exclusivo baseado em GCS
└── observability/     # Logging estruturado, auditoria BigQuery e monitoramento (Consumer Lag)
infra/                 # Terraform: infra GCP efêmera — módulos: storage, bigquery, pubsub, streaming_function, iam, budget, monitoring, apis
infra/team-access/     # Terraform: acesso IAM humano ao console — state separado, fora do ciclo efêmero (make infra-destroy não toca aqui)
tests/                 # Testes espelhando src/, incluindo Property-Based Testing (Hypothesis)
docs/                  # Diagrama de arquitetura (Excalidraw + PNG) e documentos: modelo dimensional, qualidade de dados, estimativa de custos, integração de fontes externas
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

Edite o `.env` e preencha `TF_VAR_billing_account`, `TF_VAR_alert_email` e `TF_VAR_team_members` com os valores do projeto.

> ⚠️ **Importante:** dentro de `infra/team-access`, `team_members` é
> gerenciado como um mapa único pelo Terraform. Se o seu `.env` tiver só o
> seu e-mail nesse mapa e você rodar `infra-team-apply`/`infra-team-destroy`,
> o Terraform **revoga o acesso de todo mundo que não estiver no seu mapa**.
> Use sempre a cópia mais recente combinada com o grupo, não invente a sua.
> Isso **não afeta** `infra-apply`/`infra-destroy` (state principal) — os
> dois states são isolados de propósito, ver passo 2 abaixo.

```bash
source .env
```

## 2. Acesso do time ao console (opcional, uma vez por projeto)

Acesso humano ao console (`roles/viewer`/`roles/editor`) vive num state
Terraform separado, `infra/team-access`, isolado do state principal
(`infra/`) de propósito: assim, destruir a infra efêmera de demo
(`make infra-destroy`) nunca revoga o acesso de ninguém ao projeto — os dois
states não compartilham recurso nenhum. Rode uma vez por projeto (ou sempre
que `TF_VAR_team_members` mudar):

```bash
make infra-team-init PROJECT_ID=$TF_VAR_project_id   # só na primeira vez
make infra-team-plan
make infra-team-apply
```

Para revogar o acesso de alguém que saiu do time de vez (raro e manual —
nunca disparado automaticamente): `make infra-team-destroy`.

## 3. Subir a infraestrutura

O Terraform guarda o state num bucket GCS (state locking, porque mais de uma pessoa aplica no mesmo projeto). Esse bucket é pré-requisito do próprio `terraform init`, então não dá para o Terraform criá-lo — `bootstrap.sh` resolve o ovo-e-galinha criando o bucket e habilitando as duas APIs (Resource Manager e IAM) sem as quais o `refresh` trava antes de conseguir habilitar as demais.

```bash
bash infra/bootstrap.sh $TF_VAR_project_id $TF_VAR_gcs_location   # só uma vez por projeto GCP

make infra-init PROJECT_ID=$TF_VAR_project_id   # só na primeira vez (ou se infra/.terraform sumir)
make infra-plan                                 # empacota o Producer e mostra o que será criado
make infra-apply                                # cria dataset BigQuery, bucket GCS, Pub/Sub, SA, budget, monitoring, Cloud Function + Scheduler
```

`infra-apply` só roda a partir da branch `main`, com a working tree limpa e sincronizada com `origin/main` (`infra/apply-guard.sh` bloqueia isso de propósito, para evitar duas pessoas aplicando mudanças conflitantes em paralelo no mesmo projeto GCP compartilhado).

## 4. Rodar e testar

```bash
make install                                     # cria venv e instala o pacote em modo dev
make test                                        # roda toda a suíte por camada

make bronze                                      # extrai as 6 entidades do BigQuery público -> Bronze (batch)
make streaming-producer TIPO=indicador N=5       # publica 5 eventos sintéticos no Pub/Sub
make streaming-consumer                          # consome o lote disponível e grava na Bronze (com event time)
make silver                                      # limpa, integra e deduplica a Bronze -> Silver (GCS), com checks inline + reconciliação
make gold                                        # materializa dimensões, fatos e marts da Silver -> Gold (BigQuery)
make quality                                     # checks de qualidade (Silver + Gold): registra a evidência e segue
make quality-gate                                # idem, mas bloqueante: sai com erro se houver falha CRITICA

make pipeline                                    # infra-apply + bronze -> silver -> gold -> quality num comando
make pipeline-from-scratch                       # derruba e recria a infra e roda o ciclo completo: batch -> streaming -> reprocessa Silver/Gold -> gate bloqueante
```

Em produção, o Producer roda sozinho via Cloud Scheduler → Cloud Function (sem intervenção manual); o Consumer, por ora, roda sob demanda (`make streaming-consumer`) — um pull single-shot não tem o mesmo encaixe natural de agendamento que o Producer tem.

**Por que só o caminho de streaming é agendado.** A extração batch (`make bronze`) roda sob demanda de propósito: a fonte é uma avaliação censitária anual do INEP e a extração incremental é particionada por ano (`extract_incremental` só busca `ano > max(ano já na Bronze)`). Agendar um job diário ou horário contra uma base que muda uma vez por ano gasta cota para não encontrar nada. O gatilho natural é a publicação de uma nova safra, que é um evento manual — quando isso deixar de valer (ou quando a Silver precisar de recomputação periódica), o caminho pronto é um Cloud Run Job com o mesmo Cloud Scheduler que já dispara o Producer.

## 5. Destruir a infraestrutura — sempre que terminar de testar

```bash
make infra-destroy
```

Todos os recursos gerenciados pelo Terraform foram desenhados para serem efêmeros de propósito — o `destroy` funciona limpo, sem sujeira. O bucket de state (`<project>-tfstate`) fica de fora, porque é ele que guarda o próprio state; remova à mão (`gcloud storage rm -r gs://$TF_VAR_project_id-tfstate`) só quando encerrar o projeto de vez. Não deixe a infra provisionada depois do seu teste; o orçamento de R$ 1,00 é só um alerta, não um limite automático que corta o projeto.

`infra-destroy` roda só no state principal (`infra/`) — o acesso humano do time (`infra/team-access`, passo 2) fica de fora de propósito: destruir a infra de demo não revoga o acesso de ninguém ao projeto.

## Qualidade e testes

**Testes de software**: suíte por camada (`tests/` espelhando `src/`), incluindo Property-Based Testing (Hypothesis) nas funções puras de contratos (round-trip de serialização, invariantes de schema) e na configuração de ambiente — `make test` roda tudo.

**Qualidade de dados**: checks declarativos sobre um registry (`src/quality/rules.py`) executados por Great Expectations, mapeados às seis dimensões clássicas (unicidade, completude, validade, consistência, precisão, atualidade), com severidade `CRITICA`/`AVISO`. Três pontos de entrada: inline na Silver (por entidade, a cada run), standalone (`make quality`, com isolamento de falhas entre entidades) e bloqueante (`make quality-gate`), que só falha quando há falha `CRITICA`. A evidência de cada check — inclusive a dos que passam — é persistida em `alfabetizacao_analytics.data_quality_log`. Design completo em [docs/qualidade-dados.md](docs/qualidade-dados.md).

## Aplicação em IA

A camada Gold (modelo dimensional no BigQuery, `alfabetizacao_analytics`) é o ponto de partida para modelos preditivos e analíticos sobre o indicador de alfabetização:

- **Predição de risco de não-alfabetização**: modelo supervisionado sobre `fact_indicador_municipio` + `fact_meta_resultado_municipio` (features territoriais via `dim_municipio` + metas históricas + série temporal por município) para sinalizar municípios/escolas com maior probabilidade de ficar abaixo da meta, permitindo intervenção antes do resultado da avaliação.
- **Desigualdade educacional**: clusterização de municípios por perfil socioeducacional (combinando `fact_indicador_municipio` com os atributos territoriais de `dim_municipio` — região, capital) para identificar grupos comparáveis e medir o efeito real de políticas públicas, isolando o contexto socioeconômico.
- **Apoio à decisão de política pública**: séries temporais em `fact_meta_resultado_{uf,municipio}` (`gap_pontos`, `atingiu_meta` por ano) para simular cenários ("o que aconteceria com a meta nacional se a UF X replicasse a trajetória da UF Y") e priorizar investimento onde o retorno marginal em alfabetização é maior.
- **Feature store para ML no grão do aluno**: `fact_alunos` já entrega a granularidade mais fina (proficiência individual, presença, preenchimento) para features de modelos supervisionados sem precisar voltar à Silver/Bronze.
- **Busca por similaridade**: um banco vetorial sobre embeddings de perfil municipal (ver trade-off de NoSQL acima) permitiria consultas como "encontrar municípios com contexto parecido ao do município X" — útil para transferência de boas práticas entre gestões locais comparáveis.

Os modelos em si não estão implementados neste MVP — a Gold dimensional (dimensões + fatos) já organiza os dados no formato que esse tipo de modelo consome; o próximo passo é treinar contra `alfabetizacao_analytics` diretamente do BigQuery (BigQuery ML ou export para notebook).

## Roadmap

- [x] Contratos de dados (Pydantic + Arrow/Parquet)
- [x] Infraestrutura base (Terraform: storage, BigQuery, Pub/Sub, IAM, budget, monitoring)
- [x] Observabilidade (logging, auditoria, monitoramento)
- [x] Bronze — ingestão batch (extração full/incremental do BigQuery público)
- [x] Bronze — ingestão streaming (Producer sintético + Consumer, Cloud Function + Scheduler)
- [x] Silver — limpeza, padronização, normalização de chaves, SCD Tipo 2
- [x] Gold — modelo dimensional (Kimball) materializado no BigQuery
- [x] Data Quality — testes de qualidade mapeados às seis dimensões, sobre Silver e Gold
- [ ] Enriquecimento com fontes externas (Censo Escolar/INEP, IBGE, FUNDEB) — opcional, fora do MVP
- [ ] Modelos de ML sobre a Gold (predição de risco, clusterização de desigualdade educacional)

## Evidências de execução

Cada execução deixa duas trilhas auditáveis no BigQuery: `pipeline_audit_log` (linhas lidas/escritas, bytes processados, duração e status por etapa) e `data_quality_log` (veredito de cada check, inclusive o dos que passam). Consultas de referência:

```sql
-- Resumo do último ciclo: uma linha por etapa (extração, streaming, silver, gold)
SELECT step, layer, rows_read, rows_written, duration_seconds, status, run_id
FROM `<project_id>.alfabetizacao_analytics.pipeline_audit_log`
ORDER BY timestamp DESC
LIMIT 30;

-- Falhas críticas do gate (resultado vazio = gate passou)
SELECT entidade, check, dimensao, valor_medido, limiar, detalhe
FROM `<project_id>.alfabetizacao_analytics.data_quality_log`
WHERE severidade = 'CRITICA' AND passou = FALSE
ORDER BY timestamp DESC;
```

Rodada completa executada em GCP em 2026-08-17 (`make pipeline-from-scratch`): destroy de 30 recursos → apply de 36 → Bronze (6 entidades, **3.902.927 linhas**, `rows_read == rows_written` em todas — zero rejeições de contrato em dados reais) → Silver (dedup por chave de negócio: `meta_alfabetizacao_uf` 81 → 80, `meta_alfabetizacao_municipio` 10.704 → 10.698) → Gold (5 dimensões + 7 fatos + 3 marts) → streaming (Producer ×3 + Consumer) → **quality-gate bloqueante: 76 checks, 0 falhas** (212 vereditos persistidos na janela, todos `passou = true`). O Cloud Scheduler disparou o Producer sozinho a cada 10 min durante a rodada (cron `*/10 * * * *` funcionando sem intervenção). Na janela da rodada, o BigQuery registrou 94 query jobs — 37 servidos pelo cache de resultados — totalizando 125,8 MB cobrados (~US$ 0,0008); os números por etapa e o cenário de scan frio (~259 MiB → ~US$ 0,0016/rodada) estão em [docs/estimativa-de-custos.md](docs/estimativa-de-custos.md#medições-reais-rodada-de-2026-08-17).
