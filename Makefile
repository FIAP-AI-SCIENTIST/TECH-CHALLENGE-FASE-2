.PHONY: install test test-u1 clean

# Cria venv local e instala dependências (equiv. a npm install)
VENV := .venv
PYTHON := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest
# Cria venv e instala dependências (equiv. a npm install)
install:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -e ".[dev]"

# Roda todos os testes
test: install
	$(PYTEST) tests/

# Roda especificamente os testes da U1 Contracts (failing tests)
test-u1: install
	$(PYTEST) tests/contracts/ -v

# Limpar cache do Python (venv fica em HOME, não apaga aqui)
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".hypothesis" -exec rm -rf {} +
