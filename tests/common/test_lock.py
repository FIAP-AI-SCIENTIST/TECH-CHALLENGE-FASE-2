"""Testes do módulo common.lock — lock exclusivo baseado em objeto GCS."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import PreconditionFailed

from common.lock import LockHeldError, gcs_lock, wait_for_unlock


class TestGcsLock:
    """Verifica aquisição/liberação do lock com mock de GCS."""

    def _make_blob(self, exists_raises=None):
        blob = MagicMock()
        if exists_raises is not None:
            blob.upload_from_string.side_effect = exists_raises
        return blob

    def test_acquires_and_releases_normally(self):
        mock_blob = self._make_blob()
        with patch("common.lock.storage.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
            with gcs_lock("bucket", "silver/.locks/uf.lock"):
                pass

        mock_blob.upload_from_string.assert_called_once_with(
            b"", if_generation_match=0, timeout=10
        )
        mock_blob.delete.assert_called_once()

    def test_releases_even_on_exception(self):
        mock_blob = self._make_blob()
        with patch("common.lock.storage.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
            with pytest.raises(ValueError):
                with gcs_lock("bucket", "silver/.locks/uf.lock"):
                    raise ValueError("boom")

        mock_blob.delete.assert_called_once()

    def test_raises_lock_held_error_when_not_stale(self):
        mock_blob = self._make_blob(
            exists_raises=PreconditionFailed("already exists")
        )
        mock_blob.time_created = datetime.now(timezone.utc)  # lock fresco

        with patch("common.lock.storage.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
            with pytest.raises(LockHeldError):
                with gcs_lock("bucket", "silver/.locks/uf.lock"):
                    pass

    def test_steals_stale_lock(self):
        mock_blob = MagicMock()
        mock_blob.upload_from_string.side_effect = [
            PreconditionFailed("already exists"),  # tentativa inicial de aquisição
            None,  # roubo do lock obsoleto
        ]
        mock_blob.time_created = datetime.now(timezone.utc) - timedelta(seconds=9999)

        with patch("common.lock.storage.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
            with gcs_lock("bucket", "silver/.locks/uf.lock", stale_after=600.0):
                pass

        # upload_from_string chamado 2x: tentativa inicial (falha) + roubo (sucesso)
        assert mock_blob.upload_from_string.call_count == 2
        mock_blob.delete.assert_called_once()


class TestWaitForUnlock:
    """Verifica bloqueio best-effort de leitores."""

    def test_returns_immediately_when_no_lock(self):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False

        with patch("common.lock.storage.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
            wait_for_unlock("bucket", "bronze/.locks/uf.lock", timeout=5, poll=0.01)

        mock_blob.exists.assert_called()

    def test_returns_after_lock_released(self):
        mock_blob = MagicMock()
        mock_blob.exists.side_effect = [True, True, False]

        with patch("common.lock.storage.Client") as mock_client_cls, \
             patch("common.lock.time.sleep"):
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
            wait_for_unlock("bucket", "bronze/.locks/uf.lock", timeout=5, poll=0.01)

        assert mock_blob.exists.call_count == 3

    def test_times_out_without_raising(self):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True

        with patch("common.lock.storage.Client") as mock_client_cls, \
             patch("common.lock.time.sleep"), \
             patch("common.lock.time.monotonic", side_effect=[0, 0, 100]):
            mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
            wait_for_unlock("bucket", "bronze/.locks/uf.lock", timeout=5, poll=0.01)
        # Não levanta exceção — melhor esforço.
