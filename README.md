# Pipeline de Dados — Alfabetização (Tech Challenge Fase 2)

Pipeline de dados nativo GCP para os indicadores de alfabetização do INEP
(fonte pública `basedosdados.br_inep_avaliacao_alfabetizacao`). Extrai do
BigQuery público, valida contra contratos Pydantic e grava a camada Bronze
particionada por ano no Cloud Storage.

```
src/
├── contracts/     # Modelos Pydantic (schema de cada entidade) + serialização Parquet
├── extraction/     # Extração full/incremental do BigQuery público -> Bronze
├── bronze/         # Leitura/escrita de partições Parquet no GCS
├── common/         # Retry compartilhado
└── observability/  # Logging, auditoria e monitoramento de cada execução
infra/              # Terraform: toda a infra GCP (efêmera — sobe e destrói por demanda)
```

## Pré-requisitos

- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install) autenticado
- [Terraform](https://developer.hashicorp.com/terraform/install) (provider `google ~> 5.0`)
- Acesso IAM ao projeto GCP `useful-space-277919` — **peça pro Thyago te
  adicionar** antes de continuar (todo o grupo recebe `roles/editor`, então
  qualquer um consegue rodar `apply`/`destroy`, não só o Thyago)

## 1. Configurar credenciais

```bash
# Login com a conta que recebeu acesso IAM ao projeto
gcloud auth login
gcloud auth application-default login

# Variáveis de ambiente do Terraform (nunca commitar o resultado — já está no .gitignore)
cp .env.example .env
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

## 2. Subir a infra

```bash
make infra-init PROJECT_ID=$TF_VAR_project_id   # só na primeira vez (ou se o .terraform/ sumir)
make infra-plan                                 # confira o que vai ser criado antes de aplicar
make infra-apply                                # cria dataset BigQuery, bucket GCS, Pub/Sub, SA, budget, monitoring
```

`infra-apply` só roda a partir da branch `main` sincronizada com o `origin`
(`infra/apply-guard.sh` barra isso de propósito, pra evitar dois aplicando
mudanças conflitantes em paralelo). Se ele reclamar, faça `git checkout main
&& git pull` primeiro.

Ao final, os outputs mostram o bucket e a Service Account criados:
```
datalake_bucket           = "useful-space-277919-datalake"
pipeline_service_account  = "alfabetizacao-pipeline-sa@useful-space-277919.iam.gserviceaccount.com"
```

## 3. Testar

```bash
make install          # cria venv e instala o pacote em modo dev
make test             # roda toda a suíte
make bronze           # extrai as 6 entidades do BigQuery público pra Bronze (GCS)
```

## 4. Destruir a infra — **sempre que terminar de testar**

O projeto tem orçamento de **R$ 1,00** configurado (`infra/modules/budget`)
só pra alertar se sair do Free Tier — não é um limite automático que corta o
projeto. Deixar a infra no ar sem uso pode gerar cobrança de verdade.

```bash
make infra-destroy
```

Todos os recursos foram desenhados pra serem efêmeros de propósito
(`delete_contents_on_destroy = true` no dataset, `deletion_protection = false`
na tabela de auditoria) — o `destroy` funciona limpo, sem sujeira. **Não
deixe a infra provisionada depois do seu teste.**
