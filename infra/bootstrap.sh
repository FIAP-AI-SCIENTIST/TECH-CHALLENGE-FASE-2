#!/usr/bin/env bash
set -e

# Script de bootstrap para criar o bucket de estado do Terraform.
# Resolve o problema do "ovo e da galinha": o Terraform precisa de um bucket
# para armazenar seu estado remoto (para concorrência em grupo), mas não pode
# provisionar esse próprio bucket através da execução principal.

PROJECT_ID=$1
LOCATION=$2

if [ -z "$PROJECT_ID" ] || [ -z "$LOCATION" ]; then
    echo "Uso: ./bootstrap.sh <GCP_PROJECT_ID> <LOCATION>"
    echo "Exemplo: ./bootstrap.sh useful-space-277919 us-central1"
    exit 1
fi

BUCKET_NAME="${PROJECT_ID}-tfstate"

echo "Verificando se o bucket gs://${BUCKET_NAME} existe..."

if gcloud storage ls "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
    echo "Bucket gs://${BUCKET_NAME} já existe. Nada a fazer."
else
    echo "Criando bucket gs://${BUCKET_NAME} em ${LOCATION}..."
    gcloud storage buckets create "gs://${BUCKET_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${LOCATION}" \
        --uniform-bucket-level-access

    # Ativa versionamento de objetos para o state do terraform (boas práticas para reversão de estado corrompido)
    gcloud storage buckets update "gs://${BUCKET_NAME}" --versioning
    
    echo "Bucket criado com sucesso. Agora você pode rodar 'terraform init' com o backend configurado."
fi

# Habilita as APIs mínimas necessárias para o Terraform conseguir sequer LER o estado
# dos recursos que ele gerencia. Sem isso, o Terraform falha na fase de "refresh"
# antes de conseguir chegar na fase de criação (onde habilitaria as APIs sozinho),
# criando um deadlock impossível de resolver via `terraform apply`.
# As demais APIs (BigQuery, Pub/Sub, Run, etc.) são gerenciadas pelo módulo `apis`.
echo "Habilitando APIs base (Cloud Resource Manager e IAM)..."
gcloud services enable \
    cloudresourcemanager.googleapis.com \
    iam.googleapis.com \
    --project="${PROJECT_ID}"

echo "Bootstrap concluído. Rode 'make infra-init PROJECT_ID=${PROJECT_ID}' para continuar."
