"""Orquestração dos checks de Data Quality — roda as regras registradas em
`quality.rules` contra uma tabela, loga cada falha e grava toda a rodada
(passando ou não) no BigQuery para auditoria histórica.

Chamada em dois pontos: dentro de `silver.pipeline.run_silver` (por
entidade, logo após a deduplicação) e, sob demanda, via `make quality`
(`run_all_quality_checks`) contra o estado atual da Silver, sem reprocessar
nada. A verificação de chave de relacionamento entre tabelas (Gold — fato
vs. dimensão) roda separadamente em `gold.pipeline`, porque só faz sentido
com as duas tabelas já materializadas.
"""

import pyarrow as pa

from observability.logging import setup_logger
from quality import rules
from quality.checks import (
    QualityResult,
    check_duplicates,
    check_missing_values,
    check_value_range,
)
from quality.writer import write_quality_results
from silver.transform import ENTIDADES_META

ENTIDADES = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
]


def run_quality_checks(entidade: str, tabela: pa.Table) -> list[QualityResult]:
    """Roda os checks registrados em `quality.rules` para `entidade`:
    duplicidade (se houver chave registrada), valores ausentes (se houver
    colunas obrigatórias) e consistência de faixa (se houver faixas
    registradas). Loga cada falha com WARNING — nunca aborta o pipeline por
    si só, quem decide o que fazer com uma falha é o caller.
    """
    logger = setup_logger()
    resultados: list[QualityResult] = []

    chave = rules.DUPLICATE_KEYS.get(entidade)
    if chave:
        resultados.append(check_duplicates(entidade, tabela, chave))

    obrigatorias = rules.REQUIRED_COLUMNS.get(entidade)
    if obrigatorias:
        resultados.append(check_missing_values(entidade, tabela, obrigatorias))

    for coluna, minimo, maximo in rules.VALUE_RANGES.get(entidade, []):
        resultados.append(check_value_range(entidade, tabela, coluna, minimo, maximo))

    for r in resultados:
        if not r.passou:
            logger.warning(f"Data Quality falhou: {r.check} em '{r.entidade}' — {r.detalhe}")

    write_quality_results(resultados)
    return resultados


def run_all_quality_checks() -> None:
    """Roda os checks contra o estado atual da Silver, para as 6 entidades
    — uso ad hoc (`make quality`), sem reprocessar nada. Isolamento de
    falha por entidade (mesmo padrão de `silver.pipeline.run_all_silver`).
    """
    from silver import reader as silver_reader

    logger = setup_logger()
    falhou = False

    for entidade in ENTIDADES:
        try:
            if entidade in ENTIDADES_META:
                scd2 = silver_reader.read_scd2_table_raw(entidade)
                tabela = scd2 if scd2 is not None else pa.Table.from_pydict({})
            else:
                tabela = silver_reader.read_entity(entidade)
            run_quality_checks(entidade, tabela)
        except Exception as exc:
            falhou = True
            logger.error(f"Falha ao rodar Data Quality em '{entidade}': {type(exc).__name__}: {exc}")

    if falhou:
        raise RuntimeError("Uma ou mais entidades falharam nos checks de Data Quality — ver logs.")
