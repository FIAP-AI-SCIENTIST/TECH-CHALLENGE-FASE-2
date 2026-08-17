# Modelo dimensional (Kimball)

DDL source of truth: `src/gold/schema.py` (registry declarativo). As tabelas são materializadas pelo próprio pipeline — `gold.writer` chama `ensure_table` antes de cada load e `ensure_constraints` depois — e continuam fora do Terraform, o que garante idempotência sem lock.

## Visão geral

| Camada | Tabelas |
|---|---|
| Dimensões (5) | `dim_uf`, `dim_municipio`, `dim_rede`, `dim_serie`, `dim_tempo` |
| Fatos (7) | `fact_indicador_uf`, `fact_indicador_municipio`, `fact_alunos`, `fact_alfabetizacao_municipio`, `fact_meta_resultado_brasil`, `fact_meta_resultado_uf`, `fact_meta_resultado_municipio` |
| Marts (3, views BigQuery) | `mart_evolucao_indicador_uf`, `mart_aderencia_metas_uf`, `mart_ranking_indicador_municipio` |
| Operacionais (2, fora do modelo) | `pipeline_audit_log`, `data_quality_log` |

## Chaves substitutas

`surrogate_key(namespace, chave_natural)`: SHA-256 de `"{namespace}|{chave}"`, primeiros 8 bytes como big-endian signed → `INT64`. `NULL → NULL`. Namespaces: `uf`, `municipio`, `rede`, `serie`, `tempo`.

Por que esse design:

- **Determinismo**: a mesma chave natural produz sempre a mesma SK entre execuções, máquinas e versões. A Gold é reescrita por inteiro a cada execução (`WRITE_TRUNCATE`), então as chaves precisam ser estáveis — SK instável quebraria todo JOIN downstream.
- **Constraints `NOT ENFORCED`**: toda dimensão declara `PRIMARY KEY (sk_*) NOT ENFORCED`; todo fato declara `FOREIGN KEY (sk_*) REFERENCES dim_*(sk_*) NOT ENFORCED`. A integridade é garantida pela construção determinística (SK = função pura da chave natural; dimensões = união dos códigos observados nos fatos) e verificada pela camada de qualidade. O constraint documenta o relacionamento para quem consulta e para o otimizador, sem custo de enforcement.
- **Re-aplicar constraints depois do load**: o job de load reescreve o schema a partir do Parquet e remove PK/FK (verificado empiricamente). Por isso `gold.writer` re-aplica via `ALTER TABLE`, só o que falta, após cada load.
- **Ordem de materialização**: dimensões antes dos fatos — o BigQuery exige a tabela referenciada existente com PK para declarar a FK.

## Dimensões

### `dim_uf` — grain: `sigla_uf`

| Coluna | Tipo |
|---|---|
| `sk_uf` | INT64 NOT NULL (PK) |
| `sigla_uf` | STRING |
| `nome` | STRING |

### `dim_municipio` — grain: `id_municipio` (IBGE, 7 dígitos)

| Coluna | Tipo |
|---|---|
| `sk_municipio` | INT64 NOT NULL (PK) |
| `id_municipio` | STRING |
| `nome` | STRING |
| `sigla_uf` | STRING |
| `nome_regiao` | STRING |
| `capital_uf` | INT64 |

### `dim_rede` — grain: código de rede

| Coluna | Tipo |
|---|---|
| `sk_rede` | INT64 NOT NULL (PK) |
| `rede` | STRING |
| `rede_desc` | STRING |

### `dim_serie` — grain: código de série

| Coluna | Tipo |
|---|---|
| `sk_serie` | INT64 NOT NULL (PK) |
| `serie` | STRING |
| `serie_desc` | STRING |

### `dim_tempo` — grain: ano

O source é anual; grain de mês/dia seria cerimônia sem ganho analítico.

| Coluna | Tipo | Derivação |
|---|---|---|
| `sk_tempo` | INT64 NOT NULL (PK) | hash de `"tempo\|<ano>"` |
| `ano` | INT64 | chave natural |
| `decada` | INT64 | `ano - ano % 10` |
| `ano_tem_meta` | BOOL | `2024 ≤ ano ≤ 2030` |
| `anos_para_meta_final` | INT64 | `2030 - ano` |

Cobertura: união dos `ano` observados nas entidades Silver ∪ 2024–2030 (horizonte da trajetória de metas) — todo `sk_tempo` referenciado por um fato existe na dimensão por construção.

## Fatos

Medidas comuns aos fatos de indicador: `taxa_alfabetizacao`, `media_portugues`, `proporcao_aluno_nivel_0..8`.

### `fact_indicador_uf` — grain `(ano, sigla_uf, serie, rede)`

Indicador agregado por UF. SKs: `sk_uf`, `sk_serie`, `sk_rede`, `sk_tempo`. Clustering: `sigla_uf`.

### `fact_indicador_municipio` — grain `(ano, id_municipio, serie, rede)`

Indicador por município. SKs: `sk_municipio`, `sk_serie`, `sk_rede`, `sk_tempo`. Clustering: `id_municipio`.

### `fact_alunos` — grain `(ano, id_municipio, id_escola, id_aluno, caderno, serie, rede)`

Fato de nível aluno (grain mais fino do modelo). Flags: `presenca`, `preenchimento_caderno`, `alfabetizado`; medidas: `proficiencia` (0–1000), `peso_aluno`. SKs: `sk_municipio`, `sk_serie`, `sk_rede`, `sk_tempo`. Clustering: `id_municipio`.

### `fact_alfabetizacao_municipio` — grain `(ano, id_municipio, rede, serie)`

**Fato integrado: meta e resultado observados na mesma linha, a partir de duas entidades de source diferentes** (meta da entidade SCD2 `meta_alfabetizacao_municipio`, resultado da entidade `municipio`), materializado a partir da tabela integrada da Silver.

- Medidas de meta: `meta_indicador`, `percentual_participacao`, `nivel_alfabetizacao` (atributo degenerado).
- Medidas derivadas: `gap_pontos` = `taxa_alfabetizacao − meta_indicador`; `atingiu_meta` = `taxa ≥ meta_indicador` (NULL quando não há meta — "não atingiu" e "sem meta" são coisas diferentes).
- SKs: `sk_tempo`, `sk_municipio`, `sk_rede`, `sk_serie`. Clustering: `id_municipio`.

### `fact_meta_resultado_brasil` — grain `(ano, rede)` por versão SCD2

Meta × resultado observado, nível nacional. Colunas: `rede`, `taxa_alfabetizacao`, `meta_indicador`, `gap_pontos`, `atingiu_meta`, `percentual_participacao`, `valid_from`, `valid_to`, `is_current`, `sk_rede`, `sk_tempo`. Sem clustering (cardinalidade de `rede` ~4).

### `fact_meta_resultado_uf` — grain `(ano, sigla_uf, rede)` por versão SCD2

Mesmas colunas, com `sigla_uf` e `sk_uf`. Clustering: `sigla_uf`.

### `fact_meta_resultado_municipio` — grain `(ano, id_municipio, rede)` por versão SCD2

Mesmas colunas, com `id_municipio` e `sk_municipio`. Clustering: `id_municipio`.

## Marts (views)

`CREATE OR REPLACE VIEW` no fim da Gold. O SQL é portável DuckDB/BigQuery (sem funções exclusivas do BigQuery) para que as consultas sejam testáveis localmente.

| Mart | Grain | Responde |
|---|---|---|
| `mart_evolucao_indicador_uf` | `(ano, sigla_uf)` | Evolução temporal do indicador, com `delta_pp_vs_ano_anterior` (janela LAG; NULL no primeiro ano da UF) |
| `mart_aderencia_metas_uf` | `(ano, sigla_uf, rede)` por versão SCD2 | Adesão às metas: `gap_pontos`, `atingiu_meta`, `pct_cumprimento_ano_uf` (fração de redes que atingiu a meta dentro da UF) |
| `mart_ranking_indicador_municipio` | `(ano, id_municipio)` | Ranking de municípios dentro da UF (`RANK` por média das taxas, empates compartilham posição), JOIN com `dim_municipio` via `sk_municipio` |

## Particionamento e clustering

- **Fatos**: `PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2016, 2031, 1))` — do primeiro ano da fonte (2016) ao ano final da meta nacional (2030), com folga nos dois lados. Ano fora do range cai em `__UNPARTITIONED__`: sinal de bug de dados, ainda consultável.
- **Dimensões**: sem partição (tabelas pequenas).
- **Clustering**: uma coluna por fato (`sigla_uf` ou `id_municipio`) — as consultas dominantes filtram por território.

## SCD Tipo 2 (Silver, entidades de meta)

As entidades de meta versionam a trajetória de metas ao longo dos anos:

| Entidade | Chave natural (sem `ano`) |
|---|---|
| `meta_alfabetizacao_brasil` | `rede` |
| `meta_alfabetizacao_uf` | `sigla_uf`, `rede` |
| `meta_alfabetizacao_municipio` | `id_municipio`, `rede` |

- **Colunas rastreadas**: `meta_alfabetizacao_2024..2030`, `percentual_participacao`, e o resultado observado (`taxa_alfabetizacao`, `nivel_alfabetizacao`). Mudança em qualquer uma abre nova versão. Rastrear o resultado é proposital: sem isso, um ano que repetisse a meta do ano anterior não abriria versão e a taxa observada daquele ano seria descartada junto da linha antiga.
- **Semântica de `apply_scd2`**: chave nova → abre versão (`valid_to = NULL`, `is_current = true`); sem mudança nas colunas rastreadas → mantém a versão corrente; mudança → fecha a corrente (`valid_to = ano`) e abre a nova; chave que some do source continua como está (ausência não é fechamento).
- **Determinismo por replay**: o pipeline Silver parte de tabela vazia e rejoga os anos em ordem cronológica sobre o Bronze. A cadeia SCD2 é função determinística do Bronze — nenhuma versão é fechada antes de ser aberta, e duas execuções sobre o mesmo Bronze produzem a mesma cadeia.
- **Persistência**: tabela cumulativa por entidade (sem partição por ano no GCS) — a cadeia é reescrita por inteiro a cada execução.

## JOIN temporal (tabela integrada)

`alfabetizacao_municipio_integrado` é o primeiro artefato do pipeline a juntar **duas entidades de source diferentes** (indicador municipal × meta municipal) — não é um lookup de diretório:

- LEFT JOIN a partir do indicador (grain `(ano, id_municipio, rede, serie)`): toda linha do indicador sobrevive.
- Localização da versão: `valid_from ≤ ano AND (valid_to IS NULL OR ano < valid_to)` — o ano herda a versão vigente quando não há nova versão para ele (é a semântica do SCD2; um JOIN por `ano = valid_from` perderia esses anos).
- A meta (grain `(ano, id_municipio, rede)`, sem `serie`) é broadcast para as séries do mesmo `(ano, id_municipio, rede)`.
- `meta_indicador` = `meta_alfabetizacao_{ano}` da versão vigente; NULL fora de 2024–2030 (trajetória só existe nesse horizonte — fabricar meta fora dele seria dado fabricado).
- Município com resultado e sem meta vigente continua com as colunas de meta NULL: achado analítico (cobertura de meta), não linha descartada.

## Integridade referencial

- Os 6 pares de `rules.FK_PAIRS` (fato × dimensão sobre SK) são validados pelo check `chave_relacionamento` da qualidade: espera-se zero órfãos.
- Só as FKs territoriais são validadas: `sk_rede`/`sk_serie`/`sk_tempo` são tautológicas — `dim_rede`/`dim_serie` são a união dos próprios códigos dos fatos e `dim_tempo` cobre os anos observados por construção.
