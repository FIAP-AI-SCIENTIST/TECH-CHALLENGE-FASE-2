# Qualidade de dados

## Design

- **O registry declarativo é a source of truth** (`src/quality/rules.py`). Great Expectations é apenas o engine de execução: as regras são dados (entidade, tipo de check, colunas, limiares), testáveis por assert estrutural sem executar nada.
- **Toda execução produz evidência**: cada resultado é gravado em `data_quality_log` (BigQuery) — passou ou falhou. A gravação é best-effort com retry: falha de evidência é logada, nunca aborta a execução.
- **A severidade decide o gate**: falha `CRITICA` bloqueia o pipeline só quando o gate está armado (`fail_on_critical`); `AVISO` registra sempre e nunca bloqueia.

## As seis dimensões

Todo check é classificado em uma das seis dimensões (`rules.DIMENSIONS`):

| Dimensão | Checks |
|---|---|
| Unicidade | `duplicidade` |
| Completude | `valores_ausentes` |
| Validade | `consistencia_faixa`, `formato_coluna`, `dominio_coluna` |
| Consistência | `schema`, `chave_relacionamento`, `volumetria` |
| Precisão | `reconciliacao` |
| Atualidade | `frescor_dado`, `frescor_arquivo` |

## Tipos de check

| Check | Garante | Origem no registry |
|---|---|---|
| `schema` | Colunas obrigatórias existem | `REQUIRED_COLUMNS` (7 entidades) |
| `valores_ausentes` | ≤ 5% de NULL nas colunas obrigatórias (`mostly=0.95`) | `REQUIRED_COLUMNS` |
| `consistencia_faixa` | Valores dentro do domínio (`taxa` 0–100, `proficiencia` 0–1000, `percentual_participacao` 0–100) | `VALUE_RANGES` |
| `formato_coluna` | Chaves no formato esperado (`id_municipio` `^\d{7}$` IBGE, `sigla_uf` `^[A-Z]{2}$`) | `COLUMN_PATTERNS` |
| `duplicidade` | Grão da entidade único (chave natural por entidade) | `DEDUPE_KEYS` (Silver) |
| `volumetria` | Mínimo de linhas por entidade/ano | `ROW_COUNT_MIN` |
| `reconciliacao` | Silver ≥ 90% das linhas do Bronze (transform não perde registro silenciosamente) | `ROW_COUNT_MATCH_MIN` — só para as entidades regulares (`uf`, `municipio`, `alunos`) |
| `frescor_dado` | Último `ano` dentro de 2 anos do ano corrente | `FRESHNESS_ANOS` |
| `frescor_arquivo` | Idade dos arquivos no GCS ≤ 168h (7 dias) | `FRESHNESS_HORAS` |
| `chave_relacionamento` | Zero órfãos entre fatos × dimensões (SK) | `FK_PAIRS` (6 pares) |

Mínimos de linhas por entidade/ano: `uf` 27 · `municipio` 1.000 · `alunos` 100.000 · `meta_alfabetizacao_brasil` 1 · `meta_alfabetizacao_uf` 27 · `meta_alfabetizacao_municipio` 1.000 · `alfabetizacao_municipio_integrado` 1.000.

## Severidade

| Severidade | Tipos de check | Racional |
|---|---|---|
| `CRITICA` | `duplicidade`, `chave_relacionamento`, `schema`, `formato_coluna`, `valores_ausentes` | Dado inutilizável a jusante: chave duplicada quebra o grão; chave órfã quebra o JOIN com a dimensão; schema/formato divergente quebra a leitura; coluna obrigatória nula torna a linha inútil para agregação |
| `AVISO` | todos os demais | Sinaliza degradação sem invalidar a carga |

## Execução

Três pontos de entrada:

1. **Inline na Silver** (por entidade): depois do transform de cada entidade, os checks rodam sobre o frame em memória — incluindo a tabela integrada, que só existe depois do cruzamento entre sources e não seria coberta se a gate lesse apenas as entidades de origem.
2. **Gate standalone** (`make quality`): lê o estado atual da Silver e roda os checks por entidade com **isolamento de falhas** (entidade ilegível vira falha `CRITICA` registrada, sem parar as demais), mais frescor, reconciliação Bronze→Silver e integridade fato × dimensão sobre a Gold.
3. **Modo bloqueante** (`fail_on_critical`): com todos os checks rodados e a evidência persistida, levanta `QualityGateFailed` se houve falha `CRITICA`. A ordem é proposital: o relatório sai sempre completo, em vez de parar no primeiro problema. O default é off — o comportamento histórico esperado é registrar e continuar.

A execução termina com log JSON no mesmo formato das demais camadas: `SUCCESS` ou `SUCCESS_WITH_DQ_FAILURE` (com contagem de falhas `CRITICA` e entidades afetadas) — o status reflete falha de dados, não saúde do processo.

## Evidência: `data_quality_log`

| Coluna | Tipo | Descrição |
|---|---|---|
| `check_id` | STRING | Identificador único do check na execução |
| `check` | STRING | Tipo de check |
| `entidade` | STRING | Entidade validada |
| `dimensao` | STRING | Dimensão (uma das seis) |
| `passou` | BOOL | Resultado |
| `valor_medido` | FLOAT64 | Medida do check (ex.: fração de linhas que passaram) |
| `limiar` | FLOAT64 | Limiar exigido |
| `severidade` | STRING | `CRITICA` ou `AVISO` |
| `linhas_afetadas` | INT64 | Linhas que falharam o check |
| `detalhe` | STRING | Contexto (coluna, faixa, erro) |
| `timestamp` | TIMESTAMP | Momento do check |

A tabela é cumulativa: cada execução appende a evidência, o que permite comparar execuções ao longo do tempo (regressão de um check, evolução de `linhas_afetadas`).

### Consultas úteis

```sql
-- Falhas da última execução, por entidade e severidade
SELECT entidade, dimensao, check, severidade, valor_medido, limiar, detalhe
FROM `<project_id>.alfabetizacao_analytics.data_quality_log`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND passou = FALSE
ORDER BY CASE severidade WHEN "CRITICA" THEN 0 ELSE 1 END, entidade;

-- Histórico de um check ao longo do tempo
SELECT DATE(timestamp) AS dia, check_id, valor_medido, linhas_afetadas
FROM `<project_id>.alfabetizacao_analytics.data_quality_log`
WHERE check = "volumetria" AND entidade = "municipio"
ORDER BY timestamp;
```
