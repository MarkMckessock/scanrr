"""Replacement executor (SPEC §9) — the destructive delete → search → verify loop.

A periodic reconciler. Per tick it (1) executes **approved** replacements: delete
the corrupt arr file + trigger a search (bounded by ``max_deletions_per_run``), then
(2) polls **requested** ones — when the arr history shows a re-import, it re-scans the
file to verify; clean → resolve, still-corrupt → retry (up to ``max_replace_attempts``)
or exhaust; no import within ``replacement_search_timeout`` → failed. Arr instances
come from the YAML registry (§0); DB writes go through the single DB thread.
"""

from __future__ import annotations

import asyncio
import logging

from scanrr.core import clock
from scanrr.core.config import RuntimeConfig
from scanrr.core.fileconfig import ArrInstanceSpec
from scanrr.db.database import Database
from scanrr.enums import DetectorStatus, Verdict
from scanrr.integrations.arr import make_client
from scanrr.scanning import engine
from scanrr.scanning.executor import ScanExecutor

_log = logging.getLogger("scanrr")


class ReplacementExecutor:
    def __init__(
        self,
        db: Database,
        config: RuntimeConfig,
        arr_instances: dict[str, ArrInstanceSpec],
        scan_executor: ScanExecutor,
        *,
        interval: float | None = None,
    ) -> None:
        self._db = db
        self._config = config
        self._arr = arr_instances
        self._scan = scan_executor
        self._interval = interval if interval is not None else config.replacement_poll_interval
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._interval)
                await self.reconcile_once()
            except asyncio.CancelledError:
                break
            except Exception:
                _log.exception("replacement reconcile failed")

    async def reconcile_once(self) -> None:
        await self._execute_approved()
        await self._poll_in_flight()

    # --- step 1: execute approved (delete + search) ------------------------- #

    async def _execute_approved(self) -> None:
        cap = self._config.max_deletions_per_run

        def _claim(session) -> list[engine.ReplacementJob]:
            return engine.claim_approved(session, cap)

        for job in await self._db.run(_claim):
            inst = self._arr.get(job.arr_instance)
            if inst is None:
                await self._fail(job.id, f"arr instance {job.arr_instance!r} not configured")
                continue
            client = make_client(inst.type, inst.url, inst.api_key)
            try:
                await client.delete_file(job.media_type, job.arr_file_id)
                await client.search(job.media_type, job.media_id)
            except Exception as exc:
                await self._fail(job.id, f"execute failed: {exc}")
                continue
            finally:
                await client.close()
            await self._requested(job.id)

    # --- step 2: poll requested for import → verify ------------------------- #

    async def _poll_in_flight(self) -> None:
        for job in await self._db.run(engine.in_flight_replacements):
            inst = self._arr.get(job.arr_instance)
            if inst is None:
                continue
            client = make_client(inst.type, inst.url, inst.api_key)
            try:
                imported = await client.imported(job.media_type, job.media_id)
            except Exception:
                imported = False
            finally:
                await client.close()

            if imported:
                await self._verified(job.id, await self._verify(job.file_path))
            elif self._timed_out(job.requested_at):
                await self._fail(job.id, "no release imported within timeout")

    async def _verify(self, path: str) -> Verdict:
        try:
            outcome = await self._scan.decode(
                path, self._config.detector_backend, self._config.max_scan_seconds
            )
        except Exception:
            return Verdict.CORRUPT  # can't verify → treat as not-yet-clean, will retry
        return Verdict.OK if outcome.status is DetectorStatus.OK else Verdict.CORRUPT

    def _timed_out(self, requested_at: str | None) -> bool:
        return requested_at is not None and (
            clock.age_seconds(requested_at) > self._config.replacement_search_timeout
        )

    # --- DB writes (bound ids; each on the DB thread) ----------------------- #

    async def _fail(self, replacement_id: int, note: str) -> None:
        def _op(session) -> None:
            engine.replacement_failed(session, replacement_id, note)

        await self._db.run(_op)

    async def _requested(self, replacement_id: int) -> None:
        def _op(session) -> None:
            engine.mark_requested(session, replacement_id)

        await self._db.run(_op)

    async def _verified(self, replacement_id: int, verdict: Verdict) -> None:
        max_attempts = self._config.max_replace_attempts

        def _op(session) -> None:
            engine.replacement_verified(session, replacement_id, verdict, max_attempts)

        await self._db.run(_op)
