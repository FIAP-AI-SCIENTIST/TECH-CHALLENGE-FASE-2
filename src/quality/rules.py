"""Registro de regras de qualidade por entidade — qual chave usar para
duplicidade, quais colunas são obrigatórias, quais faixas de valor são
válidas (mesmo padrão de registro por entidade de `silver.transform`).
"""

from silver.transform import DEDUPE_KEYS

# A chave de negócio já deduplicada na Silver é a mesma usada para
# comprovar ausência de duplicidade — reaproveita em vez de redefinir.
DUPLICATE_KEYS: dict[str, list[str]] = DEDUPE_KEYS

REQUIRED_COLUMNS: dict[str, list[str]] = {
    "uf": ["ano", "sigla_uf", "serie", "rede"],
    "municipio": ["ano", "id_municipio", "serie", "rede"],
    "alunos": ["ano", "id_municipio", "id_aluno"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "rede"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "rede"],
}

# Faixas de valor válidas. 0-100 para percentuais; proficiência usa a
# escala Saeb (o corte de alfabetização do desafio é 743, então o teto
# generoso evita falso positivo em pontuações altas da escala).
VALUE_RANGES: dict[str, list[tuple[str, float, float]]] = {
    "uf": [("taxa_alfabetizacao", 0.0, 100.0)],
    "municipio": [("taxa_alfabetizacao", 0.0, 100.0)],
    "alunos": [("proficiencia", 0.0, 1000.0)],
    "meta_alfabetizacao_brasil": [
        ("taxa_alfabetizacao", 0.0, 100.0),
        ("percentual_participacao", 0.0, 100.0),
    ],
    "meta_alfabetizacao_uf": [
        ("taxa_alfabetizacao", 0.0, 100.0),
        ("percentual_participacao", 0.0, 100.0),
    ],
    "meta_alfabetizacao_municipio": [
        ("taxa_alfabetizacao", 0.0, 100.0),
        ("percentual_participacao", 0.0, 100.0),
    ],
}
