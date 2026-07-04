"""Scan executors — where hashing and decoding actually run (SPEC §5, §6).

The orchestrator depends on the ``ScanExecutor`` protocol, not a concrete pool, so
orchestration is testable in-process (``InlineExecutor``) while production uses
``PebbleExecutor`` for true parallelism, per-file timeouts, and worker termination.
"""

from __future__ import annotations

import asyncio
from typing import Protocol

from scanrr.enums import DetectorBackend, DetectorStatus, HashAlgorithm
from scanrr.scanning import worker
from scanrr.scanning.hashing import hash_file
from scanrr.scanning.integrity import Outcome


class ScanExecutor(Protocol):
    async def hash(self, path: str, algorithm: HashAlgorithm) -> str: ...
    async def decode(self, path: str, backend: DetectorBackend, timeout: float) -> Outcome: ...
    async def close(self) -> None: ...


class InlineExecutor:
    """Runs in-process on the event loop thread. For the CLI and unit tests;
    ``timeout`` is not enforced (use the pool for real timeouts)."""

    async def hash(self, path: str, algorithm: HashAlgorithm) -> str:
        return hash_file(path, algorithm)

    async def decode(self, path: str, backend: DetectorBackend, timeout: float) -> Outcome:
        return worker.decode(path, backend)

    async def close(self) -> None:
        return None


class PebbleExecutor:
    """Runs hashing/decoding in a ``pebble.ProcessPool`` — true parallelism, a
    per-decode timeout that terminates the worker, and cancellation."""

    def __init__(self, max_workers: int) -> None:
        from pebble import ProcessPool

        self._pool = ProcessPool(max_workers=max_workers)

    async def hash(self, path: str, algorithm: HashAlgorithm) -> str:
        future = self._pool.schedule(hash_file, args=[path, algorithm])
        try:
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            future.cancel()  # terminate the worker
            raise

    async def decode(self, path: str, backend: DetectorBackend, timeout: float) -> Outcome:
        future = self._pool.schedule(worker.decode, args=[path, backend], timeout=timeout)
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

    async def close(self) -> None:
        self._pool.stop()
        self._pool.join()
