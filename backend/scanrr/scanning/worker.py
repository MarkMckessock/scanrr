"""The pure scan operations (no DB access) — SPEC §5, §6.

In M2 these run inside the ``pebble.ProcessPool``. The engine performs the
content-cache check between the hash and the decode (workers have no DB), so the
expensive decode is skipped on a cache hit.
"""

from __future__ import annotations

from scanrr.enums import DetectorBackend
from scanrr.scanning import integrity
from scanrr.scanning.hashing import hash_file
from scanrr.scanning.integrity import Outcome

__all__ = ["hash_file", "decode"]


def decode(
    path: str,
    backend: DetectorBackend = DetectorBackend.PYAV,
    timeout: float | None = None,  # noqa: ARG001 — enforced by the pool in M2
) -> Outcome:
    """Run the integrity check (the expensive decode). ``timeout`` is applied by
    the process pool in M2; here it's accepted for signature stability."""
    return integrity.check(path, backend=backend)
