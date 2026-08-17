# Integrando uma fonte externa de dados

Guia para acrescentar uma fonte de enriquecimento (Censo Escolar, IBGE/PNAD, Atlas do
Desenvolvimento Humano, Cadastro Único, FUNDEB) ao pipeline.

## 1. Escolha o caminho: batch ou streaming

As duas ingestões chegam na **mesma** camada Bronze e usam o **mesmo** contrato por entidade. O que
difere é o custo e a semântica.

| Sua fonte é… | Caminho | Por quê |
|---|---|---|
| Tabela no BigQuery público (Base dos Dados), com coluna `ano` | **Batch** | Uma query, particionamento por ano, extração incremental, lock e auditoria já resolvidos |
| Arquivo/API que você baixa e quer tratar como atualização pontual ("chegou uma medição nova") | **Streaming** | É o caso de uso que o Pub/Sub atende: evento avulso, near-real-time |
| Tabela grande de referência (dezenas de milhares de linhas ou mais) | **Batch, sempre** | O streaming confirma **uma mensagem por linha** — ver custo abaixo |
| Snapshot sem noção de ano (ex.: recorte territorial vigente) | **Batch, com ressalva** | A partição da Bronze batch é `ano=`; sem `ano` é preciso decidir outra chave de partição antes |

### O custo do streaming por linha

`streaming/producer.py` publica com confirmação **síncrona por mensagem**
(`future.result(timeout=10)`), e o consumer decodifica **um registro por mensagem**. Não existe
payload multi-linha. Portanto, publicar uma tabela de referência inteira por streaming custa uma
confirmação de rede por linha — ordem de minutos para milhares de linhas, e de horas para centenas de
milhares. Para carga de referência, batch não é só mais rápido: é o caminho certo.

## 2. Declare o contrato

Um modelo Pydantic com os campos da sua fonte, em `src/contracts/models.py`. Todos os campos
opcionais (é a convenção do projeto: dado ausente na fonte não deve derrubar a extração; a cobrança de
obrigatoriedade fica no Data Quality).

```python
class CensoEscolarRecord(BaseModel):
    """Contrato lógico para a entidade de estrutura escolar."""
    ano: int | None = Field(default=None, ge=2000, le=2100)
    id_municipio: str | None = None
    qtd_escolas: int | None = Field(default=None, ge=0)
    qtd_docentes: int | None = Field(default=None, ge=0)
```

Faixas (`ge`/`le`) valem a pena onde o domínio é conhecido: linha fora da faixa é descartada e logada
individualmente, sem derrubar o lote. O schema Parquet é derivado do contrato automaticamente
(`contracts/schema_mapper.py`), então você não escreve schema Arrow.

## 3. Registre a entidade

Uma entrada em `src/contracts/registry.py`:

```python
ENTITY_MODELS = {
    ...
    "censo_escolar": CensoEscolarRecord,
}
```

Esse é o **único** lugar onde entidade e contrato se encontram. Ele serve a extração batch, o
producer e o consumer — antes existiam três mapas independentes e registrar em um só deles falhava
silenciosamente.

## 4a. Se escolheu batch: declare a tabela de origem

Em `src/extraction/extraction.py`:

```python
ENTITY_TABLE_MAP = {
    ...
    # dataset explícito porque a fonte vive fora do dataset do indicador
    "censo_escolar": SourceTable("escola", dataset="basedosdados.br_inep_censo_escolar"),
}
```

`dataset` omitido significa "o dataset da fonte principal" (`GCP_SOURCE_DATASET`), que é o caso das 6
entidades do indicador. Para fonte externa, informe o dataset completo.

Depois:

```bash
make bronze                # extrai todas as entidades declaradas
# ou, para uma só:
python -c "from extraction.extraction import extract_entity; extract_entity('censo_escolar')"
```

A primeira execução é completa; as seguintes são incrementais (`WHERE ano > max(ano já na Bronze)`).
**Atenção**: o modo incremental pressupõe a coluna `ano`. Fonte sem `ano` precisa de decisão de
particionamento antes de entrar por aqui.

## 4b. Se escolheu streaming: publique os registros

O producer hoje só gera eventos **sintéticos** (`produce_events`), usados na demonstração. Publicar
dados reais é uma capacidade prevista e ainda não implementada — combine antes de começar, para não
duplicar caminho de publicação (retry, atributos de mensagem e auditoria já existem e devem ser
reusados).

O consumer grava micro-batches em `bronze/<entidade>/data_ingestao=YYYY-MM-DD/part-{run_id}.parquet`,
append-only.

**Cuidado conhecido**: mensagem publicada com um nome de entidade que não está no registro é logada
como erro e **não** é confirmada, então o Pub/Sub a reentrega indefinidamente. Como cada consumo puxa
até 10 mensagens, algumas mensagens nessa situação podem ocupar o lote e travar o progresso. Confira
o nome da entidade antes de publicar em volume. O tratamento definitivo está em discussão.

## 5. Aterrou na Bronze ≠ está no pipeline

Registrar a entidade faz o dado chegar à **Bronze**. Ele **não** sobe para Silver/Gold
automaticamente, e isso é deliberado: a Silver deduplica por chave de negócio, e a chave da sua fonte
é conhecimento seu.

Checklist para subir de camada:

| Passo | Onde | O que declarar |
|---|---|---|
| Chave de negócio | `silver/transform.py` → `DEDUPE_KEYS` | As colunas que identificam uma linha única |
| Entrar no processamento | `silver/pipeline.py` → `ENTIDADES` | O nome da entidade |
| Classe de escrita | `silver/transform.py` → `ENTIDADES_REGULARES` ou `ENTIDADES_META` | Partição por `ano=` ou tabela versionada (SCD Tipo 2) |
| Regras de qualidade | `quality/rules.py` | `REQUIRED_COLUMNS`, `VALUE_RANGES`, `COLUMN_PATTERNS`, `ROW_COUNT_MIN` |
| Validação no gate | `quality/pipeline.py` → `ENTIDADES` | O nome da entidade |

**Por que a chave de dedup é a parte delicada**: a deduplicação mantém a última ocorrência por chave.
Chave larga demais preserva duplicata; chave estreita demais **apaga linha legítima**. Não há default
seguro — por isso não é inferida.

## 6. Verifique

```bash
make test                  # suíte completa
make silver                # se a entidade já estiver declarada na Silver
make quality               # evidência de qualidade em data_quality_log
```

A auditoria de cada execução fica em `alfabetizacao_analytics.pipeline_audit_log` (`run_id`, linhas
lidas/escritas, duração, status). Diferença entre `rows_read` e `rows_written` indica linhas
descartadas por validação de contrato — os detalhes de cada rejeição estão no log da execução.
