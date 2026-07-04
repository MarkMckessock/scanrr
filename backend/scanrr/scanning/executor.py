"""Scan executors — where hashing and decoding actually run (SPEC §5, §6).

The orchestrator depends on the ``ScanExecutor`` protocol, not a concrete pool, so
orchestration is testable in-process (``InlineExecutor``) while production uses
``PebbleExecutor`` for true parallelism, per-file timeouts, and worker termination.

Workers have no DB access. Live per-file decode progress is streamed back to the
main process over an IPC queue; the orchestrator drains it via ``poll_progress``
and persists it (see ``engine.upsert_progress``).
"""

from __future__ import annotations

import asyncio
import queue as _queue
from dataclasses import dataclass
from typing import Protocol

from scanrr.enums import DetectorBackend, DetectorStatus, HashAlgorithm
from scanrr.scanning import integrity, worker
from scanrr.scanning.hashing import hash_file
from scanrr.scanning.integrity import Outcome


@dataclass
class ProgressUpdate:
    task_id: int
    position_s: float
    duration_s: float
    frames: int


class ScanExecutor(Protocol):
    async def hash(self, path: str, algorithm: HashAlgorithm) -> str: ...
    async def decode(
        self, path: str, backend: DetectorBackend, timeout: float, task_id: int | None = None
    ) -> Outcome: ...
    def poll_progress(self) -> list[ProgressUpdate]: ...
    async def close(self) -> None: ...


class InlineExecutor:
    """Runs in-process on the event loop thread. For the CLI and unit tests;
    ``timeout`` is not enforced (use the pool for real timeouts)."""

    def __init__(self, decode_threads: int = 1) -> None:
        self._progress: list[ProgressUpdate] = []
        self._decode_threads = decode_threads

    async def hash(self, path: str, algorithm: HashAlgorithm) -> str:
        return hash_file(path, algorithm)

    async def decode(
        self, path: str, backend: DetectorBackend, timeout: float, task_id: int | None = None
    ) -> Outcome:
        on_progress = None
        if task_id is not None:

            def on_progress(pos: float, dur: float, frames: int, _tid: int = task_id) -> None:
                self._progress.append(ProgressUpdate(_tid, pos, dur, frames))

        return integrity.check(
            path, backend=backend, on_progress=on_progress, threads=self._decode_threads
        )

    def poll_progress(self) -> list[ProgressUpdate]:
        out, self._progress = self._progress, []
        return out

    async def close(self) -> None:
        return None


class PebbleExecutor:
    """Runs hashing/decoding in a ``pebble.ProcessPool`` — true parallelism, a
    per-decode timeout that terminates the worker, and cancellation. A manager
    queue carries live decode progress from the workers back to the main process."""

    def __init__(self, max_workers: int, decode_threads: int = 1) -> None:
        from multiprocessing import Manager

        from pebble import ProcessPool

        self._pool = ProcessPool(max_workers=max_workers)
        self._manager = Manager()
        self._progress_q = self._manager.Queue()
        self._decode_threads = decode_threads

    async def hash(self, path: str, algorithm: HashAlgorithm) -> str:
        future = self._pool.schedule(hash_file, args=[path, algorithm])
        try:
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            future.cancel()  # terminate the worker
            raise

    async def decode(
        self, path: str, backend: DetectorBackend, timeout: float, task_id: int | None = None
    ) -> Outcome:
        future = self._pool.schedule(
            worker.decode,
            args=[path, backend],
            kwargs={
                "task_id": task_id,
                "progress_q": self._progress_q,
                "threads": self._decode_threads,
            },
            timeout=timeout,
        )
        try:
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            future.cancel()  # terminate the worker
            raise
        except TimeoutError:
            return Outcome(
                status=DetectorStatus.ERROR,
                log=f"timeout after {timeout}s",
                backend=backend,
            )
        except Exception as exc:  # worker crash → transient, let the engine retry
            return Outcome(
                status=DetectorStatus.ERROR,
                log=f"worker error: {type(exc).__name__}: {exc}",
                backend=backend,
            )

    def poll_progress(self) -> list[ProgressUpdate]:
        out: list[ProgressUpdate] = []
        while True:
            try:
                tid, pos, dur, frames = self._progress_q.get_nowait()
            except _queue.Empty:
                break
            out.append(ProgressUpdate(tid, pos, dur, frames))
        return out

    async def close(self) -> None:
        self._pool.stop()
        self._pool.join()
        self._manager.shutdown()
