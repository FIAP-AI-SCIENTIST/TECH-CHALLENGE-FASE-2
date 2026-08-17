.PHONY: install test test-contracts test-extraction test-streaming test-silver test-gold test-quality bronze silver gold quality quality-gate streaming-producer streaming-consumer clean package-producer infra-init infra-plan infra-apply infra-destroy infra-team-init infra-team-plan infra-team-apply infra-team-destroy pipeline pipeline-from-scratch

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

# Registra a evidência e segue: sai com código zero mesmo com falha CRITICA.
quality: install
	$(PYTHON) -c "from quality.pipeline import run_all_quality_checks; run_all_quality_checks()"

# Mesmos checks, mas bloqueante: sai com código diferente de zero se houver falha
# CRITICA. Alvo separado de propósito — encadear o gate dentro de `pipeline` faria
# um problema de dado interromper a demonstração depois de todas as camadas já
# terem sido materializadas, quando o que se quer ali é ver o relatório inteiro.
quality-gate: install
	$(PYTHON) -c "from quality.pipeline import run_all_quality_checks; run_all_quality_checks(fail_on_critical=True)"

# --- Streaming (Producer + Consumer) ---

# Uso: make streaming-producer TIPO=meta N=5
streaming-producer: install
	$(PYTHON) -c "from streaming.producer import produce_events; produce_events('$(or $(TIPO),indicador)', n=$(or $(N),1))"

streaming-consumer: install
	$(PYTHON) -c "from streaming.consumer import consume_batch; consume_batch()"

# Empacota o código do Producer como zip para deploy no Cloud Function (Gen2).
# Precisa rodar antes de infra-plan/infra-apply quando o código do Producer mudar.
# O zip vai para /tmp (tmpfs local): o repo pode estar num share CIFS (NAS) que
# segura travas órfãs e quebra o `rm`/`zip` do build — já aconteceu 2x.
# O Terraform lê de /tmp/producer.zip (default de infra/variables.tf).
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

# Destrói tudo pós-demo. Roda só no state principal (infra/) — o acesso humano
# do time (infra/team-access) fica de fora de propósito, state separado, para
# não revogar o acesso de ninguém a cada ciclo efêmero. Ver infra-team-* abaixo.
infra-destroy:
	cd infra && terraform destroy

# --- Terraform (Acesso IAM do time — state separado, fora do ciclo efêmero) ---

# Mesmo bucket de state do bootstrap.sh, prefix diferente (infra/team-access/main.tf).
infra-team-init:
	@if [ -z "$(PROJECT_ID)" ]; then echo "Erro: Forneça o PROJECT_ID (ex: make infra-team-init PROJECT_ID=seu-projeto, ou rode 'source .env' antes)"; exit 1; fi
	cd infra/team-access && terraform init -backend-config="bucket=$(PROJECT_ID)-tfstate"

infra-team-plan:
	cd infra/team-access && terraform plan

# Concede acesso ao console GCP para quem está em TF_VAR_team_members. Reaproveita o
# mesmo guard do infra-apply: evita aplicar com working tree suja/branch errada, o que
# poderia revogar sem querer o acesso de quem não está na sua cópia local do mapa.
infra-team-apply:
	bash infra/apply-guard.sh
	cd infra/team-access && terraform apply

# Uso raro e manual: só quando alguém sai do time de verdade. NUNCA é chamado por
# infra-destroy/pipeline-from-scratch — offboarding é sempre uma decisão explícita.
infra-team-destroy:
	cd infra/team-access && terraform destroy

# --- Pipeline completo (um comando só) ---

# Sobe a infra efêmera e roda as camadas na ordem Bronze → Silver → Gold →
# Quality. Cada passo é um sub-make: se um falhar, o Make para ali e os
# passos seguintes não rodam (a camada seguinte sempre depende da anterior).
pipeline: infra-apply
	$(MAKE) bronze
	$(MAKE) silver
	$(MAKE) gold
	$(MAKE) quality

# Ciclo completo a partir do zero: derruba a infra atual, recria e roda tudo —
# as camadas batch (Bronze → Silver → Gold → Quality, via `pipeline`) e em
# seguida o streaming: o producer publica eventos sintéticos de cada tipo e o
# consumer grava o micro-batch na Bronze já com o event time (data_evento).
#
# Depois do consumer as camadas derivadas rodam DE NOVO (silver → gold), e é o
# que faz a ingestão híbrida significar algo: os eventos publicados agora estão
# na Bronze em partições `data_ingestao=`, e sem reprocessar eles ficariam
# parados ali, fora de Silver, Gold e da avaliação de qualidade. A ordem não
# pode ser invertida (streaming antes do batch) porque é a segunda passada da
# Silver que prova o caminho: ela aparece como uma segunda execução em
# pipeline_audit_log, com rows_read maior que a primeira.
#
# CADÊNCIA — este reprocesso pertence ao ciclo de demonstração, NÃO é gatilho por
# micro-batch. Reprocessar a cada lote consumido seria recomputar o histórico
# inteiro por algumas dezenas de eventos: `run_silver` lê toda a Bronze da
# entidade e reescreve as partições `ano=` (o SCD2 é replayado do zero, por
# idempotência), e a Gold rematerializa as 12 tabelas com WRITE_TRUNCATE. Medido
# na rodada real de 2026-08-17: Silver ~4m23s (3,9M linhas) + Gold ~2m56s por
# passada. O streaming alimenta a Bronze continuamente; as camadas derivadas
# recomputam por ciclo. Baixar essa latência sem recomputar tudo exige
# merge/upsert incremental na Silver, que é o argumento a favor de um open table
# format (Iceberg/Delta) — hoje fora do desenho, registrado no backlog.
#
# Fecha com o gate bloqueante sobre o estado final: num ciclo from-scratch o
# que se quer é o veredito — se houver falha CRITICA, o Make sai com erro e a
# evidência fica em data_quality_log. Para a versão que só registra e segue,
# use `make pipeline`.
# infra-destroy fica fora de `pipeline` de propósito — destruir não deve ser
# implícito no comando do dia a dia.
pipeline-from-scratch:
	$(MAKE) infra-destroy
	$(MAKE) pipeline
	$(MAKE) streaming-producer TIPO=indicador N=2
	$(MAKE) streaming-producer TIPO=meta N=2
	$(MAKE) streaming-producer TIPO=medicao N=2
	$(MAKE) streaming-consumer
	$(MAKE) silver
	$(MAKE) gold
	$(MAKE) quality-gate
