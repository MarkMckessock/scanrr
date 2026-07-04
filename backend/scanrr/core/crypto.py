"""Symmetric encryption for secrets at rest (SPEC §14) — arr API keys, tokens.

The key comes from ``SCANRR_ENCRYPTION_KEY`` (a Fernet key). If unset, a fixed
**dev-only** key is used and a warning is logged — fine for local/CI, never for a
real deployment (set the env from a k8s Secret).
"""

from __future__ import annotations

import base64
import logging

from cryptography.fernet import Fernet

from scanrr.core.config import settings

_log = logging.getLogger("scanrr")

# A valid Fernet key (base64 of exactly 32 bytes) for local dev when none is set.
_DEV_KEY = base64.urlsafe_b64encode(b"scanrr-dev-insecure-key-0123456789"[:32]).decode()


def _fernet() -> Fernet:
    key = settings.encryption_key
    if not key:
        _log.warning("SCANRR_ENCRYPTION_KEY unset — using the insecure dev key")
        key = _DEV_KEY
    return Fernet(key.encode())


def encrypt(plaintext: str) -> str:
    return _fernet().encrypt(plaintext.encode()).decode()


def decrypt(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()
