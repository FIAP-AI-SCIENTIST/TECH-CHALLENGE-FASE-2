"""Decorator genérico para retry com backoff exponencial em chamadas de rede."""

import sys
import time
from functools import wraps
from typing import Callable, Tuple, Any


def with_retry(
    max_attempts: int = 3,
    backoff: Tuple[float, ...] = (0.5, 1.0, 2.0),
) -> Callable[..., Any]:
    """Decorator que envolve uma função de chamada de rede com retry + backoff.

    NOTA: este decorator não aplica timeout — timeout é responsabilidade da
    função decorada, que deve passar ``timeout=...`` explicitamente para a
    chamada real do client GCP (cada API tem sua própria forma de aceitar
    timeout; não há como impor isso genericamente aqui sem inspecionar a
    assinatura de cada client).

    CRÍTICO: recupera time.sleep do módulo da função decorada (não de
    common.retry) para que patches dos testes existentes em observability/
    continuem funcionando.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            func_module_name = sys.modules[func.__module__].__name__
            # Pega time.sleep do módulo da função decorada — assim
            # patch("observability.audit.time.sleep") intercepta corretamente
            sleep_fn = getattr(sys.modules[func.__module__], "time").sleep
            last_exc = None
            for tentativa in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if tentativa < max_attempts - 1:
                        sleep_fn(backoff[tentativa])
                        continue
                    raise
            raise last_exc

        return wrapper

    return decorator
