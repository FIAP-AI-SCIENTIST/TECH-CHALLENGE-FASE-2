.PHONY: install test test-u1 test-u4 bronze clean infra-plan infra-apply infra-destroy

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

test-u1: install
	$(PYTEST) tests/contracts/ -v

test-extraction: install
	$(PYTEST) tests/extraction/ tests/bronze/ -v

# --- Bronze Ingestion ---

bronze: install
	@for entity in uf municipio meta_alfabetizacao_brasil meta_alfabetizacao_uf meta_alfabetizacao_municipio alunos; do \
		echo "Extraindo entidade: $$entity"; \
		$(PYTHON) -c "from extraction.extraction import extract_entity; extract_entity('$$entity')"; \
		echo "Entity $$entity done."; \
	done

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

infra-plan:
	cd infra && terraform plan

# Protegido pelo apply-guard.sh para evitar drift em grupo
infra-apply:
	bash infra/apply-guard.sh
	cd infra && terraform apply

# Destrói tudo pós-demo
infra-destroy:
	cd infra && terraform destroy
