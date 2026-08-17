"""Declarative data-quality registry.

The registry is the source of truth; Great Expectations is only the execution engine.
"""
from silver.transform import DEDUPE_KEYS

DUPLICATE_KEYS = DEDUPE_KEYS
REQUIRED_COLUMNS = {
    "uf": ["ano", "sigla_uf", "serie", "rede"],
    "municipio": ["ano", "id_municipio", "serie", "rede"],
    "alunos": ["ano", "id_municipio", "id_aluno"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "rede"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "rede"],
    "alfabetizacao_municipio_integrado": ["ano", "id_municipio", "rede", "serie"],
}
VALUE_RANGES = {
    "uf": [("taxa_alfabetizacao", 0.0, 100.0)],
    "municipio": [("taxa_alfabetizacao", 0.0, 100.0)],
    "alunos": [("proficiencia", 0.0, 1000.0)],
    **{f"meta_alfabetizacao_{x}": [("taxa_alfabetizacao", 0.0, 100.0), ("percentual_participacao", 0.0, 100.0)] for x in ("brasil", "uf", "municipio")},
    "alfabetizacao_municipio_integrado": [("taxa_alfabetizacao", 0.0, 100.0)],
}
DIMENSIONS = {
    "duplicidade": "Unicidade", "valores_ausentes": "Completude", "consistencia_faixa": "Validade",
    "formato_coluna": "Validade", "dominio_coluna": "Validade", "chave_relacionamento": "Consistência",
    "schema": "Consistência", "volumetria": "Consistência", "reconciliacao": "Precisão",
    "frescor_dado": "Atualidade", "frescor_arquivo": "Atualidade",
}
ROW_COUNT_MIN = {"uf": 27, "municipio": 1000, "alunos": 100000,
                 "meta_alfabetizacao_brasil": 1, "meta_alfabetizacao_uf": 27,
                 "meta_alfabetizacao_municipio": 1000,
                 "alfabetizacao_municipio_integrado": 1000}
ROW_COUNT_MATCH_MIN = 0.9
FRESHNESS_ANOS = {entity: 2 for entity in REQUIRED_COLUMNS}
FRESHNESS_HORAS = 168
COLUMN_PATTERNS = {
    "municipio": {"id_municipio": r"^\d{7}$"}, "alunos": {"id_municipio": r"^\d{7}$"},
    "meta_alfabetizacao_municipio": {"id_municipio": r"^\d{7}$"},
    "alfabetizacao_municipio_integrado": {"id_municipio": r"^\d{7}$"},
    "uf": {"sigla_uf": r"^[A-Z]{2}$"}, "meta_alfabetizacao_uf": {"sigla_uf": r"^[A-Z]{2}$"},
}
# Severidade por tipo de check. CRITICA é o que caracteriza dado inutilizável a
# jusante: chave duplicada quebra o grão, chave órfã quebra o join com a dimensão,
# schema/formato divergente quebra a leitura, e coluna obrigatória nula abaixo do
# limiar torna a linha inútil para qualquer agregação por aquela chave. O que não
# está aqui vira AVISO: sinaliza degradação sem invalidar a carga.
SEVERIDADE = {
    "duplicidade": "CRITICA",
    "chave_relacionamento": "CRITICA",
    "schema": "CRITICA",
    "formato_coluna": "CRITICA",
    "valores_ausentes": "CRITICA",
}
# Pares fato x coluna x dimensao x coluna_dim verificados por check_referential_integrity,
# sobre as surrogate keys declaradas como FK no DDL da Gold. So as FKs territoriais:
# sk_rede/sk_serie/sk_tempo sao tautologicas (dim_rede/dim_serie sao a uniao dos proprios
# codigos dos fatos e dim_tempo cobre os anos observados por construcao) e nao entram aqui.
FK_PAIRS: list[tuple[str, str, str, str]] = [
    ("fact_indicador_uf", "sk_uf", "dim_uf", "sk_uf"),
    ("fact_meta_resultado_uf", "sk_uf", "dim_uf", "sk_uf"),
    ("fact_indicador_municipio", "sk_municipio", "dim_municipio", "sk_municipio"),
    ("fact_meta_resultado_municipio", "sk_municipio", "dim_municipio", "sk_municipio"),
    ("fact_alfabetizacao_municipio", "sk_municipio", "dim_municipio", "sk_municipio"),
    ("fact_alunos", "sk_municipio", "dim_municipio", "sk_municipio"),
]
