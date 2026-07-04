"""Async scan orchestrator (SPEC §6) — concurrent drain of the shared queue.

Owns the drain loop: claim pending tasks up to ``max_scan_workers``, run each
through hash → cache-check → decode → persist, fanning the result out to every
subscribed run. All DB work goes through the single DB thread; all hashing/decoding
goes through the ``ScanExecutor``. Handles crash recovery, cancellation, and clean
shutdown.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict

from sqlmodel import Session

from scanrr.core.config import RuntimeConfig
from scanrr.core.events import EventBus
from scanrr.core.fileconfig import JobSpec
from scanrr.db.database import Database
from scanrr.enums import DetectorStatus, JobType, RunTrigger, Verdict
from scanrr.integrations.arr import apply_path_mapping, make_client
from scanrr.scanning import engine
from scanrr.scanning.executor import ScanExecutor
from scanrr.scanning.integrity import Outcome

_log = logging.getLogger("scanrr")


class Orchestrator:
    def __init__(
        self,
        db: Database,
        executor: ScanExecutor,
        config: RuntimeConfig,
        *,
        bus: EventBus | None = None,
        yaml_jobs: dict[str, JobSpec] | None = None,
        poll_interval: float = 0.5,
    ) -> None:
        self.db = db
        self.executor = executor
        self.config = config
        self._bus = bus
        self._yaml_jobs: dict[str, JobSpec] = yaml_jobs or {}  # keyed by slug
        self._poll_interval = poll_interval
        self._max_workers = config.max_scan_workers
        self._inflight: dict[int, asyncio.Task[None]] = {}
        self._wake = asyncio.Event()
        self._running = False
        self._drain_task: asyncio.Task[None] | None = None

    # --- lifecycle ---------------------------------------------------------- #

    async def start(self) -> None:
        recovered = await self.db.run(engine.recover_interrupted)
        if recovered:
            _log.info("recovered %d interrupted task(s)", recovered)
        self._running = True
        self._drain_task = asyncio.create_task(self._drain_loop())

    async def stop(self) -> None:
        self._running = False
        self._wake.set()
        if self._drain_task is not None:
            await self._drain_task
        if self._inflight:
            await asyncio.gather(*list(self._inflight.values()), return_exceptions=True)
        await self.executor.close()

    # --- run control -------------------------------------------------------- #

    async def trigger_run(self, slug: str, trigger: RunTrigger = RunTrigger.SCHEDULED) -> int:
        spec = self._yaml_jobs.get(slug)
        if spec is None:
            raise ValueError(f"job {slug!r} not found")
        # arr enumeration is network I/O — do it here (async), off the DB thread.
        candidates = await self._arr_discover(spec) if spec.type is JobType.ARR else None

        def _start(session: Session) -> int:
            return engine.start_run(session, spec, trigger, self.config, candidates)

        run_id = await self.db.run(_start)
        await self._publish_run_started(run_id)
        self._wake.set()
        return run_id

    async def _arr_discover(self, spec: JobSpec) -> list[engine.ArrCandidate]:
        """Enumerate an arr instance and map its paths to local candidates (SPEC §9)."""
        instance_id = int(json.loads(spec.config)["arr_instance_id"])

        def _inst(session: Session) -> engine.ArrInstanceInfo | None:
            return engine.get_arr_instance_info(session, instance_id)

        def _maps(session: Session) -> list[tuple[str, str]]:
            return engine.get_path_mappings(session, instance_id)

        inst = await self.db.run(_inst)
        if inst is None:
            _log.warning("arr instance %d missing/disabled for job %r", instance_id, spec.slug)
            return []
        mappings = await self.db.run(_maps)
        client = make_client(inst.type, inst.base_url, inst.api_key)
        try:
            files = await client.list_media_files()
        except Exception as exc:
            _log.error("arr enumeration failed for instance %d: %r", instance_id, exc)
            return []
        finally:
            await client.close()

        candidates: list[engine.ArrCandidate] = []
        for arr_file in files:
            local = apply_path_mapping(mappings, arr_file.remote_path)
            if local is None:
                _log.warning("no path mapping for %s", arr_file.remote_path)
                continue
            candidates.append(
                engine.ArrCandidate(
                    local_path=local,
                    media_type=arr_file.media_type,
                    media_id=arr_file.media_id,
                    arr_file_id=arr_file.arr_file_id,
                    arr_instance_id=instance_id,
                )
            )
        return candidates

    def _publish_progress(self, progress: engine.RunProgress) -> None:
        if self._bus is None:
            return
        terminal = progress.status in engine.TERMINAL_RUN_STATES
        etype = "run.completed" if terminal else "run.progress"
        self._bus.publish({"type": etype, **asdict(progress)})

    async def _publish_run_started(self, run_id: int) -> None:
        if self._bus is None:
            return

        def _get(session: Session) -> engine.RunProgress | None:
            return engine.get_progress(session, run_id)

        progress = await self.db.run(_get)
        if progress is not None:
            self._bus.publish({"type": "run.started", "run_id": run_id})
            self._publish_progress(progress)

    def _publish_task(self, event: engine.TaskEvent) -> None:
        if self._bus is None:
            return
        self._bus.publish(
            {
                "type": "task.done",
                "task_id": event.task_id,
                "path": event.path,
                "verdict": event.verdict,
            }
        )
        for progress in event.runs:
            self._publish_progress(progress)

    async def cancel_run(self, run_id: int) -> None:
        orphaned = await self.db.run(lambda s: engine.cancel_run(s, run_id))
        to_await = [self._inflight[t] for t in orphaned if t in self._inflight]
        for task_id in orphaned:
            task = self._inflight.get(task_id)
            if task is not None:
                task.cancel()
        if to_await:
            await asyncio.gather(*to_await, return_exceptions=True)
        for task_id in orphaned:

            def _drop(session: Session, tid: int = task_id) -> None:
                engine.drop_orphan_task(session, tid)

            await self.db.run(_drop)
        await self.db.run(lambda s: engine.mark_cancelled(s, run_id))

    async def wait_for_run(self, run_id: int, timeout: float = 30.0) -> engine.RunStatus:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            status = await self.db.run(lambda s: engine.get_run_status(s, run_id))
            if status in engine.TERMINAL_RUN_STATES:
                return status
            if loop.time() > deadline:
                raise TimeoutError(f"run {run_id} did not finish within {timeout}s")
            await asyncio.sleep(0.02)

    # --- drain loop --------------------------------------------------------- #

    async def _drain_loop(self) -> None:
        while self._running:
            await self._dispatch_available()
            self._wake.clear()
            if await self._dispatch_available():  # re-check after clear (lost-wakeup guard)
                continue
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)
            except TimeoutError:
                pass

    async def _dispatch_available(self) -> bool:
        dispatched = False
        while self._running and len(self._inflight) < self._max_workers:
            claimed = await self.db.run(engine.claim)
            if claimed is None:
                break
            task_id, path = claimed
            task = asyncio.create_task(self._process(task_id, path))
            self._inflight[task_id] = task

            def _done(fut: asyncio.Task[None], tid: int = task_id) -> None:
                self._on_task_done(tid, fut)

            task.add_done_callback(_done)
            dispatched = True
        return dispatched

    def _on_task_done(self, task_id: int, fut: asyncio.Task[None]) -> None:
        self._inflight.pop(task_id, None)
        self._wake.set()
        if not fut.cancelled() and (exc := fut.exception()) is not None:
            _log.error("task %d failed: %r", task_id, exc)

    # --- per-task pipeline -------------------------------------------------- #

    async def _process(self, task_id: int, path: str) -> None:
        cfg = self.config
        content_hash: str | None = None
        verdict: Verdict | None = None
        cache_it = False
        out = Outcome(status=DetectorStatus.ERROR, log="not scanned")

        for _attempt in range(cfg.scan_max_attempts):
            try:
                content_hash = await self.executor.hash(path, cfg.hash_algorithm)

                def _check(session: Session, h: str = content_hash) -> Verdict | None:
                    return engine.check_cache(session, h, cfg.detector_backend)

                cached = await self.db.run(_check)
                if cached is not None:
                    verdict, cache_it = cached, False
                    break
                out = await self.executor.decode(path, cfg.detector_backend, cfg.max_scan_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # transient (IO/worker) — retry
                out = Outcome(status=DetectorStatus.ERROR, log=f"{type(exc).__name__}: {exc}")
                content_hash = None
                continue
            if out.status is DetectorStatus.OK:
                verdict, cache_it = Verdict.OK, True
                break
            if out.status is DetectorStatus.CORRUPT:
                verdict, cache_it = Verdict.CORRUPT, True
                break
            # DetectorStatus.ERROR / timeout → retry

        if verdict is None or content_hash is None:
            error = out.log or None

            def _unreadable(session: Session) -> engine.TaskEvent:
                return engine.record_unreadable(session, task_id, error)

            self._publish_task(await self.db.run(_unreadable))
        else:
            ch, verd, outcome, ci = content_hash, verdict, out, cache_it

            def _record(session: Session) -> engine.TaskEvent:
                return engine.record_verdict(
                    session, task_id, ch, verd, outcome, ci, cfg.detector_backend
                )

            self._publish_task(await self.db.run(_record))
