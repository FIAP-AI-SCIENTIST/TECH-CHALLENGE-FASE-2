"""Lock exclusivo baseado em objeto GCS — protege sessões de escrita contra
execuções concorrentes (ex: dois membros do grupo rodando `make bronze`/
`make silver` ao mesmo tempo para a mesma entidade).

Não é um serviço de coordenação distribuída (Zookeeper/etcd) — é uma
precondição atômica de criação de objeto (`if_generation_match=0`, o GCS
recusa se o objeto já existir), proporcional ao risco real do projeto
(grupo pequeno, sem orquestrador central decidindo quem roda o quê).
"""

import time
from contextlib import contextmanager
from datetime import datetime, timezone

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

from common.retry import with_retry

TIMEOUT_SECONDS = 10
# Se o objeto de lock for mais velho que isso, foi abandonado por um processo
# que travou/crashou sem liberar — é seguro assumir e continuar em vez de
# bloquear o pipeline para sempre por causa de um lock morto.
STALE_AFTER_SECONDS = 600.0
WAIT_POLL_SECONDS = 0.5


class LockHeldError(Exception):
    """Outra execução já detém o lock para este caminho (e não está obsoleto)."""


@contextmanager
def gcs_lock(bucket_name: str, path: str, stale_after: float = STALE_AFTER_SECONDS):
    """Adquire um lock exclusivo em ``path`` (ex: "bronze/.locks/uf.lock").

    Levanta ``LockHeldError`` se outra execução já detém o lock e ele não
    está obsoleto. Libera o objeto ao sair do bloco `with`, inclusive em
    exceção — a sessão de escrita protegida (clear+write de uma entidade)
    nunca fica travada indefinidamente por uma falha no meio do processo.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)

    try:
        _acquire(blob)
    except PreconditionFailed:
        if not _is_stale(blob, stale_after):
            raise LockHeldError(
                f"Lock já detido em {bucket_name}/{path} — outra execução em andamento."
            )
        # Lock obsoleto (processo anterior não liberou) — assume o lock.
        _steal(blob)

    try:
        yield
    finally:
        _release(blob)


def wait_for_unlock(
    bucket_name: str,
    path: str,
    timeout: float = STALE_AFTER_SECONDS,
    poll: float = WAIT_POLL_SECONDS,
) -> None:
    """Bloqueia (melhor esforço) até o lock em ``path`` ser liberado.

    Usado por leitores (ex: Silver lendo Bronze) para nunca ler uma partição
    a meio de um clear+write. Não levanta erro se o timeout esgotar — só
    retorna, deixando o caller decidir como prosseguir (ex: logar aviso e
    ler mesmo assim). Retorna imediatamente se não houver lock nenhum.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)

    deadline = time.monotonic() + timeout
    while _exists(blob) and time.monotonic() < deadline:
        time.sleep(poll)


@with_retry()
def _acquire(blob: storage.Blob) -> None:
    blob.upload_from_string(b"", if_generation_match=0, timeout=TIMEOUT_SECONDS)


@with_retry()
def _release(blob: storage.Blob) -> None:
    blob.delete(timeout=TIMEOUT_SECONDS)


@with_retry()
def _steal(blob: storage.Blob) -> None:
    """Assume um lock obsoleto — sobrescreve incondicionalmente."""
    blob.upload_from_string(b"", timeout=TIMEOUT_SECONDS)


@with_retry()
def _exists(blob: storage.Blob) -> bool:
    return blob.exists(timeout=TIMEOUT_SECONDS)


def _is_stale(blob: storage.Blob, stale_after: float) -> bool:
    blob.reload(timeout=TIMEOUT_SECONDS)
    if blob.time_created is None:
        return False
    age = (datetime.now(timezone.utc) - blob.time_created).total_seconds()
    return age > stale_after
