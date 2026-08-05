.PHONY: install test test-u1 clean infra-plan infra-apply infra-destroy

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

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".hypothesis" -exec rm -rf {} +

# --- Terraform (U2 Infrastructure) ---

# Inicializa o Terraform passando o nome do bucket do bootstrap.sh
# Exemplo de uso: make infra-init PROJECT_ID=useful-space-277919
infra-init:
	@if [ -z "$(PROJECT_ID)" ]; then echo "Erro: Forneça o PROJECT_ID (ex: make infra-init PROJECT_ID=seu-projeto)"; exit 1; fi
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
