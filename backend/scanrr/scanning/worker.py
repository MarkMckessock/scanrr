"""The pure scan operations (no DB access) — SPEC §5, §6.

In M2 these run inside the ``pebble.ProcessPool``. The engine performs the
content-cache check between the hash and the decode (workers have no DB), so the
expensive decode is skipped on a cache hit.
"""

from __future__ import annotations

from typing import Protocol

from scanrr.enums import DetectorBackend
from scanrr.scanning import integrity
from scanrr.scanning.hashing import hash_file
from scanrr.scanning.integrity import Outcome

__all__ = ["hash_file", "decode"]


class ProgressSink(Protocol):
    """Structural type for the manager Queue proxy the worker reports through."""

    def put(self, item: tuple[int, float, float, int]) -> None: ...


def decode(
    path: str,
    backend: DetectorBackend = DetectorBackend.PYAV,
    timeout: float | None = None,  # noqa: ARG001 — enforced by the pool
    *,
    task_id: int | None = None,
    progress_q: ProgressSink | None = None,
) -> Outcome:
    """Run the integrity check (the expensive decode). Runs in a worker process;
    if given a progress queue, streams live decode progress back to the parent."""
    on_progress = None
    if progress_q is not None and task_id is not None:

        def on_progress(position_s: float, duration_s: float, frames: int) -> None:
            progress_q.put((task_id, position_s, duration_s, frames))

    return integrity.check(path, backend=backend, on_progress=on_progress)
