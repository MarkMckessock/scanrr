"""Content hashing (SPEC §3). blake3 by default, sha256 configurable."""

from __future__ import annotations

import hashlib
from typing import Protocol

from scanrr.enums import HashAlgorithm

_CHUNK = 4 * 1024 * 1024


class _Hasher(Protocol):
    def update(self, data: bytes) -> object: ...
    def hexdigest(self) -> str: ...


def _new_hasher(algorithm: HashAlgorithm) -> _Hasher:
    if algorithm is HashAlgorithm.BLAKE3:
        import blake3

        return blake3.blake3()
    if algorithm is HashAlgorithm.SHA256:
        return hashlib.sha256()
    raise ValueError(f"unknown hash algorithm: {algorithm!r}")


def hash_file(path: str, algorithm: HashAlgorithm = HashAlgorithm.BLAKE3) -> str:
    hasher = _new_hasher(algorithm)
    with open(path, "rb") as fh:
        while chunk := fh.read(_CHUNK):
            hasher.update(chunk)
    return hasher.hexdigest()
