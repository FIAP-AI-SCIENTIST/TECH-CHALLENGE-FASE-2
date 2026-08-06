"""Testes do decorator with_retry de common.retry."""

from unittest.mock import MagicMock, patch
import time

import pytest

from common.retry import with_retry


class TestWithRetry:
    """Verifica comportamento do decorator with_retry."""

    def test_success_on_first_attempt(self):
        """Sucesso na primeira tentativa — chama a função 1x."""
        call_count = 0

        @with_retry(max_attempts=3)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = flaky_func()
        assert result == "ok"
        assert call_count == 1

    def test_success_after_failures(self):
        """Falha N vezes, depois sucede — call_count == N+1."""
        call_count = 0

        @with_retry(max_attempts=5, backoff=(0.01, 0.01, 0.01, 0.01))
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        with patch("tests.common.test_retry.time"):
            result = flaky_func()
        assert result == "ok"
        assert call_count == 3

    def test_raises_original_exception_after_max_attempts(self):
        """Todas as tentativas falham — relança a exceção original."""
        call_count = 0

        @with_retry(max_attempts=3, backoff=(0.01, 0.01))
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("network down")

        with patch("tests.common.test_retry.time"):
            with pytest.raises(ConnectionError, match="network down"):
                always_fails()
        assert call_count == 3
