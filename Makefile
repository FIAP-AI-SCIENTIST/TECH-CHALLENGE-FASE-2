.PHONY: install test test-contracts test-extraction test-streaming test-silver test-gold test-quality bronze silver gold quality streaming-producer streaming-consumer clean package-producer infra-init infra-plan infra-apply infra-destroy

# O projeto reside num CIFS/SMB share que não suporta symlinks.
# O venv fica em $HOME/.venvs para evitar o problema.
VENV := $(HOME)/.venvs/pipeline-alfabetizacao
PYTHON := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest

# --- Python ---

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -e ".[dev]"

test: install
	$(PYTEST) tests/

test-contracts: install
	$(PYTEST) tests/contracts/ -v

test-extraction: install
	$(PYTEST) tests/extraction/ tests/bronze/ -v

test-streaming: install
	$(PYTEST) tests/streaming/ -v

test-silver: install
	$(PYTEST) tests/silver/ tests/common/test_lock.py -v

test-gold: install
	$(PYTEST) tests/gold/ -v

test-quality: install
	$(PYTEST) tests/quality/ -v

# --- Bronze Ingestion ---

bronze: install
	@for entity in uf municipio meta_alfabetizacao_brasil meta_alfabetizacao_uf meta_alfabetizacao_municipio alunos; do \
		echo "Extraindo entidade: $$entity"; \
		$(PYTHON) -c "from extraction.extraction import extract_entity; extract_entity('$$entity')"; \
		echo "Entity $$entity done."; \
	done

# --- Silver (limpeza, normalização, dedup, SCD2) ---

silver: install
	$(PYTHON) -c "from silver.pipeline import run_all_silver; run_all_silver()"

# --- Gold (modelo dimensional materializado no BigQuery) ---

gold: install
	$(PYTHON) -c "from gold.pipeline import run_gold; run_gold()"


# --- Data Quality (Great Expectations sobre Silver/Gold) ---

quality: install
	$(PYTHON) -c "from quality.pipeline import run_all_quality_checks; run_all_quality_checks()"
# --- Streaming (Producer + Consumer) ---

# Uso: make streaming-producer TIPO=meta N=5
streaming-producer: install
	$(PYTHON) -c "from streaming.producer import produce_events; produce_events('$(or $(TIPO),indicador)', n=$(or $(N),1))"

streaming-consumer: install
	$(PYTHON) -c "from streaming.consumer import consume_batch; consume_batch()"

# Empacota o código do Producer como zip para deploy no Cloud Function (Gen2).
# Precisa rodar antes de infra-plan/infra-apply quando o código do Producer mudar.
package-producer:
	bash infra/scripts/package_producer.sh

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".hypothesis" -exec rm -rf {} +

# --- Terraform (Infrastructure) ---

# Inicializa o Terraform passando o nome do bucket do bootstrap.sh
# PROJECT_ID vem de: (1) argumento explícito make infra-init PROJECT_ID=x,
# ou (2) TF_VAR_project_id já exportado no ambiente (ex: após `source .env`)
PROJECT_ID ?= $(TF_VAR_project_id)

infra-init:
	@if [ -z "$(PROJECT_ID)" ]; then echo "Erro: Forneça o PROJECT_ID (ex: make infra-init PROJECT_ID=seu-projeto, ou rode 'source .env' antes)"; exit 1; fi
	cd infra && terraform init -backend-config="bucket=$(PROJECT_ID)-tfstate"

infra-plan: package-producer
	cd infra && terraform plan

# Protegido pelo apply-guard.sh para evitar drift em grupo
infra-apply: package-producer
	bash infra/apply-guard.sh
	cd infra && terraform apply

# Destrói tudo pós-demo
infra-destroy:
	cd infra && terraform destroy
