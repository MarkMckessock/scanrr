"""M2 orchestrator tests (SPEC §6): concurrency, timeout→unreadable, cancellation,
crash recovery — exercised with controllable in-process executors, plus one
real ProcessPool smoke test.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

import pytest
from sqlmodel import Session, select

from scanrr.core.config import RuntimeConfig
from scanrr.db import engine as db_engine
from scanrr.db.database import Database
from scanrr.db.models import Detection, Job, JobRun, RunFile, ScanTask, ScanTaskSubscriber
from scanrr.enums import DetectorStatus, Disposition, RunStatus, TaskStatus, Verdict
from scanrr.scanning.executor import InlineExecutor, PebbleExecutor
from scanrr.scanning.hashing import hash_file as real_hash
from scanrr.scanning.integrity import Outcome
from scanrr.scanning.orchestrator import Orchestrator

CFG = RuntimeConfig(min_file_size_bytes=0, min_file_age_seconds=0, max_scan_workers=2)


@pytest.fixture
def eng(tmp_path):
    e = db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    db_engine.init_db()
    return e


def make_job(eng, root: Path, *, ttl_seconds: int = 0) -> int:
    with Session(eng) as s:
        job = Job(name="t", ttl_seconds=ttl_seconds, config=json.dumps({"root_path": str(root)}))
        s.add(job)
        s.commit()
        s.refresh(job)
        assert job.id is not None
        return job.id


def lib_with(tmp_path: Path, media: dict, **names: str) -> Path:
    lib = tmp_path / "lib"
    lib.mkdir(exist_ok=True)
    for filename, sample in names.items():
        shutil.copy(media[sample], lib / f"{filename}.mkv")
    return lib


# --- controllable executors ------------------------------------------------- #


class GatedExecutor:
    """Real hashing; decode blocks on a gate so we can observe concurrency."""

    def __init__(self, verdict: DetectorStatus = DetectorStatus.OK) -> None:
        self.gate = asyncio.Event()
        self.started = 0
        self._start_signal = asyncio.Semaphore(0)
        self._verdict = verdict

    async def hash(self, path, algorithm):
        return real_hash(path, algorithm)

    async def decode(self, path, backend, timeout):
        self.started += 1
        self._start_signal.release()
        await self.gate.wait()
        return Outcome(status=self._verdict, backend=backend)

    async def await_started(self, n: int) -> None:
        for _ in range(n):
            await asyncio.wait_for(self._start_signal.acquire(), timeout=2)

    async def close(self):
        return None


class AlwaysTransientExecutor:
    """decode always returns a transient error (models repeated timeouts)."""

    async def hash(self, path, algorithm):
        return real_hash(path, algorithm)

    async def decode(self, path, backend, timeout):
        return Outcome(
            status=DetectorStatus.ERROR, log=f"timeout after {timeout}s", backend=backend
        )

    async def close(self):
        return None


# --- tests ------------------------------------------------------------------ #


async def test_full_run_inline(eng, media, tmp_path):
    lib = lib_with(tmp_path, media, good="clean", bad="bitflip")
    job_id = make_job(eng, lib)
    orch = Orchestrator(Database(eng), InlineExecutor(), CFG, poll_interval=0.02)
    await orch.start()
    try:
        run_id = await orch.trigger_run(job_id)
        assert await orch.wait_for_run(run_id, timeout=15) == RunStatus.COMPLETED
        with Session(eng) as s:
            run = s.get(JobRun, run_id)
            assert run.files_scanned == 2 and run.files_corrupt == 1
            assert len(s.exec(select(Detection)).all()) == 1
    finally:
        await orch.stop()


async def test_concurrency_is_bounded(eng, media, tmp_path):
    lib = lib_with(tmp_path, media, a="clean", b="bitflip", c="truncated", d="header")
    job_id = make_job(eng, lib)
    executor = GatedExecutor()
    orch = Orchestrator(Database(eng), executor, CFG, poll_interval=0.02)  # max_workers=2
    await orch.start()
    try:
        run_id = await orch.trigger_run(job_id)
        await executor.await_started(2)          # exactly two decodes get going
        await asyncio.sleep(0.1)                  # give any (wrongly) extra one a chance
        assert executor.started == 2
        assert len(orch._inflight) == 2
        executor.gate.set()                       # release everything
        assert await orch.wait_for_run(run_id, timeout=15) == RunStatus.COMPLETED
        assert executor.started == 4
    finally:
        executor.gate.set()
        await orch.stop()


async def test_transient_exhaustion_is_unreadable(eng, media, tmp_path):
    lib = lib_with(tmp_path, media, x="clean")
    job_id = make_job(eng, lib)
    orch = Orchestrator(Database(eng), AlwaysTransientExecutor(), CFG, poll_interval=0.02)
    await orch.start()
    try:
        run_id = await orch.trigger_run(job_id)
        assert await orch.wait_for_run(run_id, timeout=15) == RunStatus.COMPLETED
        with Session(eng) as s:
            run = s.get(JobRun, run_id)
            assert run.files_unreadable == 1 and run.files_corrupt == 0
            task = s.exec(select(ScanTask)).one()
            assert task.status == TaskStatus.UNREADABLE
    finally:
        await orch.stop()


async def test_cancel_run_terminates_inflight(eng, media, tmp_path):
    lib = lib_with(tmp_path, media, a="clean")
    job_id = make_job(eng, lib)
    executor = GatedExecutor()
    orch = Orchestrator(Database(eng), executor, CFG, poll_interval=0.02)
    await orch.start()
    try:
        run_id = await orch.trigger_run(job_id)
        await executor.await_started(1)           # decode is in-flight, gated
        await orch.cancel_run(run_id)
        with Session(eng) as s:
            assert s.get(JobRun, run_id).status == RunStatus.CANCELLED
            assert s.exec(select(ScanTask)).all() == []      # orphan task dropped
            assert s.exec(select(Detection)).all() == []     # nothing recorded
    finally:
        executor.gate.set()
        await orch.stop()


async def test_crash_recovery_resumes_scanning_task(eng, media, tmp_path):
    lib = lib_with(tmp_path, media, good="clean")
    path = str(lib / "good.mkv")
    job_id = make_job(eng, lib)
    # Simulate a crash mid-scan: a RUNNING run with a SCANNING task + subscription.
    with Session(eng) as s:
        run = JobRun(job_id=job_id, status=RunStatus.RUNNING, started_at="2020-01-01T00:00:00Z")
        s.add(run)
        s.flush()
        task = ScanTask(seq=1, path=path, status=TaskStatus.SCANNING)
        s.add(task)
        s.flush()
        s.add(ScanTaskSubscriber(scan_task_id=task.id, job_run_id=run.id))
        s.add(RunFile(job_run_id=run.id, path=path, disposition=Disposition.QUEUED))
        s.commit()
        run_id = run.id

    orch = Orchestrator(Database(eng), InlineExecutor(), CFG, poll_interval=0.02)
    await orch.start()  # recover_interrupted flips scanning → pending, drain resumes
    try:
        assert await orch.wait_for_run(run_id, timeout=15) == RunStatus.COMPLETED
        with Session(eng) as s:
            assert s.get(JobRun, run_id).files_scanned == 1
            assert s.exec(select(RunFile)).one().outcome == Verdict.OK
    finally:
        await orch.stop()


@pytest.mark.requires_ffmpeg
async def test_pebble_pool_end_to_end(eng, media, tmp_path):
    lib = lib_with(tmp_path, media, good="clean", bad="bitflip")
    job_id = make_job(eng, lib)
    orch = Orchestrator(Database(eng), PebbleExecutor(2), CFG, poll_interval=0.02)
    await orch.start()
    try:
        run_id = await orch.trigger_run(job_id)
        assert await orch.wait_for_run(run_id, timeout=30) == RunStatus.COMPLETED
        with Session(eng) as s:
            run = s.get(JobRun, run_id)
            assert run.files_scanned == 2 and run.files_corrupt == 1
    finally:
        await orch.stop()
