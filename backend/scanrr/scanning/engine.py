"""The scan engine (SPEC §3) — M1 synchronous drain.

Implements the full §3/§6 data model (shared dedup queue, subscribers, run_files,
fan-out, finalize) but drains the queue in-process, single-threaded. M2 swaps only
the executor (pebble pool + async writer + scheduler) behind this same logic.
"""

from __future__ import annotations

import json
import os
import time

from sqlmodel import Session, col, select

from scanrr.core import clock
from scanrr.core.config import RuntimeConfig
from scanrr.core.logging import decision
from scanrr.db.models import (
    Detection,
    File,
    Job,
    JobRun,
    RunFile,
    ScanResult,
    ScanTask,
    ScanTaskSubscriber,
)
from scanrr.enums import (
    DetectionStatus,
    DetectorBackend,
    DetectorStatus,
    Disposition,
    RunStatus,
    RunTrigger,
    TaskStatus,
    Verdict,
)
from scanrr.jobs import queue
from scanrr.jobs.discovery import walk_media
from scanrr.scanning import integrity, worker
from scanrr.scanning.hashing import hash_file

_OPEN_DETECTION_STATES = (
    DetectionStatus.OPEN,
    DetectionStatus.ACKNOWLEDGED,
    DetectionStatus.NEEDS_ATTENTION,
)


# --- helpers ---------------------------------------------------------------- #


def _get_file(session: Session, path: str) -> File | None:
    return session.exec(select(File).where(File.path == path)).first()


def _upsert_file(session: Session, path: str, st: os.stat_result, content_hash: str) -> File:
    f = _get_file(session, path)
    now = clock.iso_now()
    if f is None:
        f = File(path=path, first_seen_at=now)
    f.hash = content_hash
    f.size_bytes = st.st_size
    f.mtime = st.st_mtime
    f.last_seen_at = now
    f.last_scanned_at = now
    session.add(f)
    session.flush()
    return f


def _valid_cached(
    session: Session, content_hash: str, backend: DetectorBackend
) -> ScanResult | None:
    sr = session.get(ScanResult, content_hash)
    if (
        sr
        and sr.detector_version == integrity.DETECTOR_VERSION
        and sr.detector_backend == backend
    ):
        return sr
    return None


def _cache_result(
    session: Session,
    content_hash: str,
    verdict: Verdict,
    out: integrity.Outcome,
    backend: DetectorBackend,
) -> None:
    sr = session.get(ScanResult, content_hash)
    if sr is None:
        sr = ScanResult(hash=content_hash, status=verdict)
    sr.status = verdict
    sr.error_log = out.log or None
    sr.detector_version = integrity.DETECTOR_VERSION
    sr.detector_backend = backend
    sr.scan_duration_ms = out.duration_ms
    sr.scanned_at = clock.iso_now()
    session.add(sr)


def reconcile_detections(
    session: Session, file: File, content_hash: str, verdict: Verdict, run_id: int | None
) -> Detection | None:
    """Open a detection on corrupt; auto-resolve stale ones on a clean scan (#6)."""
    assert file.id is not None
    if verdict is Verdict.CORRUPT:
        det = session.exec(
            select(Detection).where(
                Detection.file_id == file.id, Detection.hash == content_hash
            )
        ).first()
        if det is None:
            det = Detection(file_id=file.id, hash=content_hash, job_run_id=run_id)
            session.add(det)
            session.flush()
        return det

    if verdict is Verdict.OK:
        stale = session.exec(
            select(Detection).where(
                Detection.file_id == file.id,
                Detection.hash != content_hash,
                col(Detection.status).in_(_OPEN_DETECTION_STATES),
            )
        )
        for det in stale:
            det.status = DetectionStatus.RESOLVED
            det.resolved_at = clock.iso_now()
            session.add(det)
    return None


def _fan_out(session: Session, task: ScanTask, outcome: Verdict) -> None:
    """Credit every subscribed run with the task's outcome (SPEC §3)."""
    assert task.id is not None
    for run_id in queue.subscribers(session, task.id):
        rf = session.get(RunFile, (run_id, task.path))
        if rf is not None:
            rf.outcome = outcome
            session.add(rf)


# --- phases ----------------------------------------------------------------- #


def _record(
    session: Session,
    run_id: int,
    path: str,
    disposition: Disposition,
    scan_task_id: int | None = None,
) -> None:
    rf = session.get(RunFile, (run_id, path))
    if rf is None:
        rf = RunFile(job_run_id=run_id, path=path)
    rf.disposition = disposition
    rf.scan_task_id = scan_task_id
    session.add(rf)


def discover(session: Session, job: Job, run: JobRun, config: RuntimeConfig) -> None:
    """Phase A — stat-only: skip or enqueue each media file (SPEC §3)."""
    assert run.id is not None
    root: str = json.loads(job.config)["root_path"]
    discovered = 0
    for path, st in walk_media(root, config.media_extensions, config.min_file_size_bytes):
        discovered += 1
        # 0. stability gate
        if time.time() - st.st_mtime < config.min_file_age_seconds:
            _record(session, run.id, path, Disposition.SKIPPED_TOO_FRESH)
            decision("skipped", path=path, reason=Disposition.SKIPPED_TOO_FRESH)
            continue
        # 1. TTL fast-path (global last_scanned_at)
        f = _get_file(session, path)
        if (
            f is not None
            and f.size_bytes == st.st_size
            and f.mtime == st.st_mtime
            and f.last_scanned_at is not None
            and clock.age_seconds(f.last_scanned_at) < job.ttl_seconds
        ):
            _record(session, run.id, path, Disposition.SKIPPED_TTL)
            decision("skipped", path=path, reason=Disposition.SKIPPED_TTL)
            continue
        # 2. enqueue on the shared queue (dedup by path) + subscribe
        task = queue.enqueue_or_subscribe(session, path, run.id)
        _record(session, run.id, path, Disposition.QUEUED, task.id)
    run.files_discovered = discovered
    session.add(run)
    session.flush()


def process_task(session: Session, task: ScanTask, config: RuntimeConfig) -> None:
    """Phase B — hash → cache-check → decode (with retry) → reconcile → fan-out."""
    st = os.stat(task.path)
    verdict: Verdict | None = None
    out = integrity.Outcome(status=DetectorStatus.ERROR)
    for attempt in range(1, config.scan_max_attempts + 1):
        task.attempts = attempt
        content_hash = hash_file(task.path, config.hash_algorithm)
        task.content_hash = content_hash
        cached = _valid_cached(session, content_hash, config.detector_backend)
        if cached is not None:
            verdict = Verdict(cached.status)
            break
        out = worker.decode(task.path, config.detector_backend)
        if out.status is DetectorStatus.OK:
            verdict = Verdict.OK
        elif out.status is DetectorStatus.CORRUPT:
            verdict = Verdict.CORRUPT
        if verdict is not None:
            _cache_result(session, content_hash, verdict, out, config.detector_backend)
            break
        # DetectorStatus.ERROR → transient; retry (M1 retries inline, no backoff)

    if verdict is None:  # retries exhausted → unreadable (never cached)
        task.status = TaskStatus.UNREADABLE
        task.result_status = Verdict.UNREADABLE
        task.error = out.log or None
        session.add(task)
        _fan_out(session, task, Verdict.UNREADABLE)
        decision("unreadable", path=task.path, attempts=task.attempts)
        return

    assert task.id is not None and task.content_hash is not None
    file = _upsert_file(session, task.path, st, task.content_hash)
    task.status = TaskStatus.DONE
    task.result_status = verdict
    session.add(task)
    first_run = next(iter(queue.subscribers(session, task.id)), None)
    reconcile_detections(session, file, task.content_hash, verdict, first_run)
    _fan_out(session, task, verdict)
    decision("scanned", path=task.path, verdict=verdict, ms=out.duration_ms)


def _finalize_run(session: Session, run: JobRun) -> None:
    rfs = list(session.exec(select(RunFile).where(RunFile.job_run_id == run.id)))
    run.files_skipped = sum(
        1 for r in rfs if r.disposition in (Disposition.SKIPPED_TTL, Disposition.SKIPPED_TOO_FRESH)
    )
    run.files_scanned = sum(1 for r in rfs if r.outcome in (Verdict.OK, Verdict.CORRUPT))
    run.files_corrupt = sum(1 for r in rfs if r.outcome is Verdict.CORRUPT)
    run.files_unreadable = sum(1 for r in rfs if r.outcome is Verdict.UNREADABLE)
    run.status = RunStatus.COMPLETED
    run.finished_at = clock.iso_now()
    session.add(run)


def run_job(
    session: Session,
    job: Job,
    *,
    config: RuntimeConfig | None = None,
    trigger: RunTrigger = RunTrigger.MANUAL,
) -> JobRun:
    """Execute a job end-to-end (M1 synchronous). Returns the finalized run."""
    config = config or RuntimeConfig()
    run = JobRun(
        job_id=job.id,
        status=RunStatus.RUNNING,
        trigger=trigger,
        started_at=clock.iso_now(),
    )
    session.add(run)
    session.flush()

    discover(session, job, run, config)
    while (task := queue.claim_next_pending(session)) is not None:
        process_task(session, task, config)
    _finalize_run(session, run)

    session.commit()
    session.refresh(run)
    return run


# --- M2 orchestrator operations (sync; each runs on the single DB thread) ---- #


def recover_interrupted(session: Session) -> int:
    """On startup, requeue tasks stuck 'scanning' (SPEC §6). Returns the count."""
    stuck = list(session.exec(select(ScanTask).where(ScanTask.status == TaskStatus.SCANNING)))
    for task in stuck:
        task.status = TaskStatus.PENDING
        task.updated_at = clock.iso_now()
        session.add(task)
    return len(stuck)


def start_run(session: Session, job_id: int, trigger: RunTrigger, config: RuntimeConfig) -> int:
    """Create a run and run Phase A discovery (enqueue). Returns the run id."""
    job = session.get(Job, job_id)
    assert job is not None and job.id is not None
    run = JobRun(
        job_id=job.id, status=RunStatus.RUNNING, trigger=trigger, started_at=clock.iso_now()
    )
    session.add(run)
    session.flush()
    assert run.id is not None
    discover(session, job, run, config)
    finalize_run_if_done(session, run.id)  # all-skipped runs finalize immediately
    return run.id


def claim(session: Session) -> tuple[int, str] | None:
    """Atomically claim the next pending task; returns (id, path) as plain data."""
    task = queue.claim_next_pending(session)
    if task is None:
        return None
    assert task.id is not None
    return task.id, task.path


def check_cache(session: Session, content_hash: str, backend: DetectorBackend) -> Verdict | None:
    sr = _valid_cached(session, content_hash, backend)
    return Verdict(sr.status) if sr is not None else None


def record_verdict(
    session: Session,
    task_id: int,
    content_hash: str,
    verdict: Verdict,
    out: integrity.Outcome,
    cache_it: bool,
    backend: DetectorBackend,
) -> None:
    """Persist an ok/corrupt verdict, reconcile detections, fan out, finalize."""
    task = session.get(ScanTask, task_id)
    assert task is not None
    task.content_hash = content_hash
    if cache_it:
        _cache_result(session, content_hash, verdict, out, backend)
    file = _upsert_file(session, task.path, os.stat(task.path), content_hash)
    task.status = TaskStatus.DONE
    task.result_status = verdict
    session.add(task)
    first_run = next(iter(queue.subscribers(session, task_id)), None)
    reconcile_detections(session, file, content_hash, verdict, first_run)
    _fan_out(session, task, verdict)
    _finalize_affected(session, task_id)


def record_unreadable(session: Session, task_id: int, error: str | None) -> None:
    """Mark a task unreadable (retries exhausted), fan out, finalize."""
    task = session.get(ScanTask, task_id)
    assert task is not None
    task.status = TaskStatus.UNREADABLE
    task.result_status = Verdict.UNREADABLE
    task.error = error
    session.add(task)
    _fan_out(session, task, Verdict.UNREADABLE)
    _finalize_affected(session, task_id)


def _run_incomplete(session: Session, run_id: int) -> bool:
    row = session.exec(
        select(RunFile)
        .where(
            RunFile.job_run_id == run_id,
            RunFile.disposition == Disposition.QUEUED,
            col(RunFile.outcome).is_(None),
        )
        .limit(1)
    ).first()
    return row is not None


def finalize_run_if_done(session: Session, run_id: int) -> bool:
    run = session.get(JobRun, run_id)
    if run is not None and run.status == RunStatus.RUNNING and not _run_incomplete(session, run_id):
        _finalize_run(session, run)
        return True
    return False


def _finalize_affected(session: Session, task_id: int) -> None:
    for run_id in queue.subscribers(session, task_id):
        finalize_run_if_done(session, run_id)


def cancel_run(session: Session, run_id: int) -> list[int]:
    """Unsubscribe a run; drop now-orphaned pending tasks. Returns orphaned
    *scanning* task ids for the orchestrator to terminate (SPEC §6)."""
    run = session.get(JobRun, run_id)
    if run is None or run.status not in (RunStatus.RUNNING, RunStatus.CANCELLING):
        return []
    run.status = RunStatus.CANCELLING
    session.add(run)
    subs = list(
        session.exec(
            select(ScanTaskSubscriber).where(ScanTaskSubscriber.job_run_id == run_id)
        )
    )
    task_ids = [s.scan_task_id for s in subs]
    for sub in subs:
        session.delete(sub)
    session.flush()

    orphaned_scanning: list[int] = []
    for tid in task_ids:
        if queue.subscribers(session, tid):
            continue  # another run still needs it
        task = session.get(ScanTask, tid)
        if task is None:
            continue
        if task.status == TaskStatus.PENDING:
            session.delete(task)  # never started — drop it
        elif task.status == TaskStatus.SCANNING:
            orphaned_scanning.append(tid)
    return orphaned_scanning


def mark_cancelled(session: Session, run_id: int) -> None:
    run = session.get(JobRun, run_id)
    if run is not None and run.status in (RunStatus.RUNNING, RunStatus.CANCELLING):
        run.status = RunStatus.CANCELLED
        run.finished_at = clock.iso_now()
        session.add(run)


def drop_orphan_task(session: Session, task_id: int) -> None:
    """Delete a task that has no remaining subscribers (post-cancellation)."""
    if queue.subscribers(session, task_id):
        return
    task = session.get(ScanTask, task_id)
    if task is not None:
        session.delete(task)


TERMINAL_RUN_STATES = (
    RunStatus.COMPLETED,
    RunStatus.FAILED,
    RunStatus.CANCELLED,
    RunStatus.INTERRUPTED,
)


def get_run_status(session: Session, run_id: int) -> RunStatus | None:
    run = session.get(JobRun, run_id)
    return run.status if run is not None else None


def job_has_active_run(session: Session, job_id: int) -> bool:
    row = session.exec(
        select(JobRun)
        .where(
            JobRun.job_id == job_id,
            col(JobRun.status).in_((RunStatus.RUNNING, RunStatus.CANCELLING)),
        )
        .limit(1)
    ).first()
    return row is not None


def scheduled_jobs(session: Session) -> list[tuple[int, str]]:
    """(job_id, cron) for enabled jobs that have a schedule."""
    rows = session.exec(
        select(Job).where(col(Job.enabled).is_(True), col(Job.schedule_cron).is_not(None))
    )
    return [(j.id, j.schedule_cron) for j in rows if j.id is not None and j.schedule_cron]
