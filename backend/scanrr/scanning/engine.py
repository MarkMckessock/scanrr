"""The scan engine (SPEC §3) — M1 synchronous drain.

Implements the full §3/§6 data model (shared dedup queue, subscribers, run_files,
fan-out, finalize) but drains the queue in-process, single-threaded. M2 swaps only
the executor (pebble pool + async writer + scheduler) behind this same logic.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

from sqlalchemy import ColumnElement, func
from sqlmodel import Session, col, select

from scanrr.core import clock
from scanrr.core.config import RuntimeConfig
from scanrr.core.fileconfig import JobSpec
from scanrr.core.logging import decision
from scanrr.db.models import (
    Detection,
    File,
    FileArrLink,
    JobRun,
    NotificationLog,
    NotificationQueue,
    Replacement,
    RunFile,
    ScanProgress,
    ScanResult,
    ScanTask,
    ScanTaskSubscriber,
)
from scanrr.enums import (
    DetectionStatus,
    DetectorBackend,
    DetectorStatus,
    Disposition,
    MediaType,
    NotificationEvent,
    NotificationStatus,
    ReplacementStatus,
    RunStatus,
    RunTrigger,
    TaskStatus,
    Verdict,
)
from scanrr.jobs import queue
from scanrr.jobs.discovery import walk_media
from scanrr.scanning import integrity, worker
from scanrr.scanning.executor import ProgressUpdate
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


@dataclass
class ArrCandidate:
    """A media file discovered via an arr instance (path already mapped to local)."""

    local_path: str
    media_type: MediaType
    media_id: int
    arr_file_id: int
    arr_instance: str  # YAML arr instance name


def _ensure_file(session: Session, path: str) -> File:
    """Get-or-create a File row by path (hash filled later by the worker)."""
    f = _get_file(session, path)
    if f is None:
        f = File(path=path)
        session.add(f)
        session.flush()
    return f


def _link_arr(session: Session, path: str, cand: ArrCandidate) -> None:
    file = _ensure_file(session, path)
    assert file.id is not None
    link = session.get(FileArrLink, (file.id, cand.arr_instance))
    if link is None:
        link = FileArrLink(file_id=file.id, arr_instance=cand.arr_instance)
    link.media_type = cand.media_type
    link.media_id = cand.media_id
    link.arr_file_id = cand.arr_file_id
    session.add(link)


def _consider(
    session: Session,
    run_id: int,
    ttl_seconds: int,
    path: str,
    st: os.stat_result,
    config: RuntimeConfig,
    cand: ArrCandidate | None,
) -> None:
    """Apply the §3 stat-only decision to one candidate file."""
    if time.time() - st.st_mtime < config.min_file_age_seconds:  # 0. stability gate
        _record(session, run_id, path, Disposition.SKIPPED_TOO_FRESH)
        decision("skipped", path=path, reason=Disposition.SKIPPED_TOO_FRESH)
        return
    f = _get_file(session, path)  # 1. TTL fast-path (global last_scanned_at)
    if (
        f is not None
        and f.size_bytes == st.st_size
        and f.mtime == st.st_mtime
        and f.last_scanned_at is not None
        and clock.age_seconds(f.last_scanned_at) < ttl_seconds
    ):
        _record(session, run_id, path, Disposition.SKIPPED_TTL)
        decision("skipped", path=path, reason=Disposition.SKIPPED_TTL)
        if cand is not None:
            _link_arr(session, path, cand)
        return
    task = queue.enqueue_or_subscribe(session, path, run_id)  # 2. enqueue + subscribe
    _record(session, run_id, path, Disposition.QUEUED, task.id)
    if cand is not None:
        _link_arr(session, path, cand)


def discover(
    session: Session,
    spec: JobSpec,
    run: JobRun,
    config: RuntimeConfig,
    arr_candidates: list[ArrCandidate] | None = None,
) -> None:
    """Phase A — stat-only: skip or enqueue each media file (SPEC §3, §9).

    Path jobs walk the configured directory; arr jobs receive already-mapped local
    candidates (enumeration + path mapping happens in the orchestrator, off the DB
    thread) and additionally record the arr linkage for remediation.
    """
    assert run.id is not None
    discovered = 0
    if arr_candidates is None:
        for path, st in walk_media(
            json.loads(spec.config)["root_path"],
            config.media_extensions,
            config.min_file_size_bytes,
        ):
            discovered += 1
            _consider(session, run.id, spec.ttl_seconds, path, st, config, None)
    else:
        for cand in arr_candidates:
            try:
                st = os.stat(cand.local_path)
            except OSError:
                continue  # vanished between enumeration and now
            discovered += 1
            _consider(session, run.id, spec.ttl_seconds, cand.local_path, st, config, cand)
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
    enqueue_notification(
        session,
        NotificationEvent.SCAN_COMPLETED,
        {
            "run_id": run.id,
            "job_name": run.job_name,
            "files_scanned": run.files_scanned,
            "files_corrupt": run.files_corrupt,
            "files_unreadable": run.files_unreadable,
        },
        dedup_key=f"run:{run.id}",
    )


def _new_run(spec: JobSpec, trigger: RunTrigger) -> JobRun:
    """A run row snapshotting its job's definition (SPEC §9 — self-contained runs)."""
    return JobRun(
        job_slug=spec.slug,
        job_name=spec.name,
        job_type=spec.type,
        job_config=spec.config,
        ttl_seconds=spec.ttl_seconds,
        auto_replace=spec.auto_replace,
        auto_approve=spec.auto_approve,
        status=RunStatus.RUNNING,
        trigger=trigger,
        started_at=clock.iso_now(),
    )


def run_job(
    session: Session,
    spec: JobSpec,
    *,
    config: RuntimeConfig | None = None,
    trigger: RunTrigger = RunTrigger.MANUAL,
) -> JobRun:
    """Execute a job end-to-end (M1 synchronous). Returns the finalized run."""
    config = config or RuntimeConfig()
    run = _new_run(spec, trigger)
    session.add(run)
    session.flush()

    discover(session, spec, run, config)
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


def start_run(
    session: Session,
    spec: JobSpec,
    trigger: RunTrigger,
    config: RuntimeConfig,
    arr_candidates: list[ArrCandidate] | None = None,
) -> int:
    """Create a run and run Phase A discovery (enqueue). Returns the run id."""
    run = _new_run(spec, trigger)
    session.add(run)
    session.flush()
    assert run.id is not None
    discover(session, spec, run, config, arr_candidates)
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
) -> TaskEvent:
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
    detection = reconcile_detections(session, file, content_hash, verdict, first_run)
    _fan_out(session, task, verdict)
    if verdict is Verdict.CORRUPT:
        enqueue_notification(
            session, NotificationEvent.CORRUPT_FOUND, {"path": task.path}, dedup_key=task.path
        )
        if detection is not None:
            _propose_replacements(session, task_id, detection)
    return _task_event(session, task_id, task.path, verdict)


def record_unreadable(session: Session, task_id: int, error: str | None) -> TaskEvent:
    """Mark a task unreadable (retries exhausted), fan out, finalize."""
    task = session.get(ScanTask, task_id)
    assert task is not None
    task.status = TaskStatus.UNREADABLE
    task.result_status = Verdict.UNREADABLE
    task.error = error
    session.add(task)
    _fan_out(session, task, Verdict.UNREADABLE)
    return _task_event(session, task_id, task.path, Verdict.UNREADABLE)


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


@dataclass
class RunProgress:
    run_id: int
    status: RunStatus
    files_discovered: int
    files_scanned: int
    files_skipped: int
    files_corrupt: int
    files_unreadable: int


@dataclass
class TaskEvent:
    task_id: int
    path: str
    verdict: Verdict
    runs: list[RunProgress]


def _progress(run: JobRun) -> RunProgress:
    assert run.id is not None
    return RunProgress(
        run_id=run.id,
        status=run.status,
        files_discovered=run.files_discovered,
        files_scanned=run.files_scanned,
        files_skipped=run.files_skipped,
        files_corrupt=run.files_corrupt,
        files_unreadable=run.files_unreadable,
    )


def get_progress(session: Session, run_id: int) -> RunProgress | None:
    run = session.get(JobRun, run_id)
    return _progress(run) if run is not None else None


def _task_event(session: Session, task_id: int, path: str, verdict: Verdict) -> TaskEvent:
    """Finalize every subscribed run and collect their progress for eventing."""
    runs: list[RunProgress] = []
    for run_id in queue.subscribers(session, task_id):
        finalize_run_if_done(session, run_id)
        run = session.get(JobRun, run_id)
        if run is not None:
            runs.append(_progress(run))
    return TaskEvent(task_id=task_id, path=path, verdict=verdict, runs=runs)


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


def job_has_active_run(session: Session, job_slug: str) -> bool:
    row = session.exec(
        select(JobRun)
        .where(
            JobRun.job_slug == job_slug,
            col(JobRun.status).in_((RunStatus.RUNNING, RunStatus.CANCELLING)),
        )
        .limit(1)
    ).first()
    return row is not None


# --- read helpers for the API ----------------------------------------------- #


def _job_dict(session: Session, spec: JobSpec) -> dict:
    """Render a (read-only, YAML-defined) job with its most recent run."""
    last = session.exec(
        select(JobRun)
        .where(JobRun.job_slug == spec.slug)
        .order_by(col(JobRun.id).desc())
        .limit(1)
    ).first()
    config = json.loads(spec.config)
    return {
        "slug": spec.slug,
        "name": spec.name,
        "type": spec.type,
        "enabled": spec.enabled,
        "ttl_seconds": spec.ttl_seconds,
        "schedule_cron": spec.schedule_cron,
        "root_path": config.get("root_path"),
        "arr_instance": config.get("arr_instance"),
        "auto_replace": spec.auto_replace,
        "last_run": None
        if last is None
        else {"id": last.id, "status": last.status, "finished_at": last.finished_at},
    }


def list_jobs(session: Session, yaml_specs: list[JobSpec]) -> list[dict]:
    return [_job_dict(session, spec) for spec in yaml_specs]


def _run_dict(run: JobRun) -> dict:
    return {
        "id": run.id,
        "job_slug": run.job_slug,
        "job_name": run.job_name,
        "status": run.status,
        "trigger": run.trigger,
        "files_discovered": run.files_discovered,
        "files_scanned": run.files_scanned,
        "files_skipped": run.files_skipped,
        "files_corrupt": run.files_corrupt,
        "files_unreadable": run.files_unreadable,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
    }


def get_run(session: Session, run_id: int) -> dict | None:
    run = session.get(JobRun, run_id)
    return _run_dict(run) if run is not None else None


def list_runs(session: Session, limit: int = 50) -> list[dict]:
    runs = session.exec(select(JobRun).order_by(col(JobRun.id).desc()).limit(limit)).all()
    return [_run_dict(r) for r in runs]


def run_files(session: Session, run_id: int) -> list[dict]:
    rows = session.exec(
        select(RunFile).where(RunFile.job_run_id == run_id).order_by(col(RunFile.path))
    ).all()
    return [
        {"path": r.path, "disposition": r.disposition, "outcome": r.outcome} for r in rows
    ]


def list_detections(session: Session, status: DetectionStatus | None = None) -> list[dict]:
    stmt = select(Detection, File).join(File, col(Detection.file_id) == col(File.id))
    if status is not None:
        stmt = stmt.where(Detection.status == status)
    rows = session.exec(stmt.order_by(col(Detection.id).desc())).all()
    out = []
    for det, file in rows:
        sr = session.get(ScanResult, det.hash)
        out.append(
            {
                "id": det.id,
                "path": file.path,
                "hash": det.hash,
                "status": det.status,
                "detected_at": det.detected_at,
                "resolved_at": det.resolved_at,
                "error_log": sr.error_log if sr is not None else None,
            }
        )
    return out


def set_detection_status(session: Session, det_id: int, status: DetectionStatus) -> bool:
    det = session.get(Detection, det_id)
    if det is None:
        return False
    det.status = status
    det.resolved_at = (
        clock.iso_now()
        if status in (DetectionStatus.RESOLVED, DetectionStatus.IGNORED)
        else None
    )
    session.add(det)
    return True


def stats(session: Session) -> dict:
    results = {r.hash: r.status for r in session.exec(select(ScanResult)).all()}
    hashes = [h for h in session.exec(select(File.hash)).all() if h is not None]
    ok = sum(1 for h in hashes if results.get(h) == Verdict.OK)
    corrupt = sum(1 for h in hashes if results.get(h) == Verdict.CORRUPT)
    open_detections = len(
        session.exec(
            select(Detection).where(Detection.status == DetectionStatus.OPEN)
        ).all()
    )
    active_runs = len(
        session.exec(
            select(JobRun).where(
                col(JobRun.status).in_((RunStatus.RUNNING, RunStatus.CANCELLING))
            )
        ).all()
    )
    # "jobs" count is added by the API from the YAML registry (no jobs table).
    return {
        "active_runs": active_runs,
        "open_detections": open_detections,
        "files_ok": ok,
        "files_corrupt": corrupt,
        "files_tracked": len(hashes),
    }


# --- Sonarr / Radarr remediation (arr instances are defined in YAML) -------- #


# A replacement is "open" (not terminal, not rejected) in these states:
_OPEN_REPLACEMENT_STATES = (
    ReplacementStatus.PENDING_APPROVAL,
    ReplacementStatus.APPROVED,
    ReplacementStatus.REQUESTED,
    ReplacementStatus.SEARCHING,
    ReplacementStatus.GRABBED,
    ReplacementStatus.IMPORTED,
    ReplacementStatus.VERIFYING,
)


@dataclass
class ReplacementJob:
    """Everything the async executor needs to act on a replacement (SPEC §9)."""

    id: int
    arr_instance: str
    media_type: MediaType
    media_id: int
    arr_file_id: int
    detection_id: int
    file_path: str
    attempt: int
    requested_at: str | None


def _replacement_dict(r: Replacement) -> dict:
    return {
        "id": r.id,
        "detection_id": r.detection_id,
        "attempt": r.attempt,
        "status": r.status,
        "arr_instance": r.arr_instance,
        "media_type": r.media_type,
        "approved_by": r.approved_by,
        "requested_at": r.requested_at,
        "notes": r.notes,
    }


def _open_replacement(session: Session, detection_id: int) -> Replacement | None:
    return session.exec(
        select(Replacement)
        .where(
            Replacement.detection_id == detection_id,
            col(Replacement.status).in_(_OPEN_REPLACEMENT_STATES),
        )
        .limit(1)
    ).first()


def _new_replacement(
    session: Session, detection: Detection, link: FileArrLink, *, approved_by: str | None = None
) -> Replacement:
    repl = Replacement(
        detection_id=detection.id,
        arr_instance=link.arr_instance,
        media_type=link.media_type,
        media_id=link.media_id,
        arr_file_id=link.arr_file_id,
    )
    if approved_by is not None:
        repl.status = ReplacementStatus.APPROVED
        repl.approved_by = approved_by
        repl.approved_at = clock.iso_now()
    session.add(repl)
    session.flush()
    return repl


def _propose_replacements(session: Session, task_id: int, detection: Detection) -> None:
    """On a corrupt scan, propose a replacement for any subscribing auto_replace job."""
    if detection.id is None:
        return
    link = session.exec(select(FileArrLink).where(FileArrLink.file_id == detection.file_id)).first()
    if link is None or _open_replacement(session, detection.id) is not None:
        return
    auto_replace = auto_approve = False
    for run_id in queue.subscribers(session, task_id):
        run = session.get(JobRun, run_id)
        if run is not None and run.auto_replace:
            auto_replace = True
            auto_approve = auto_approve or run.auto_approve
    if not auto_replace:
        return
    _new_replacement(session, detection, link, approved_by="auto" if auto_approve else None)
    event = (
        NotificationEvent.REPLACEMENT_REQUESTED
        if auto_approve
        else NotificationEvent.REPLACEMENT_PENDING_APPROVAL
    )
    enqueue_notification(
        session, event, {"detection_id": detection.id}, dedup_key=f"det:{detection.id}"
    )


def create_replacement(session: Session, detection_id: int) -> dict | None:
    """Manually propose a replacement (→ pending_approval). None if unknown / no arr link."""
    det = session.get(Detection, detection_id)
    if det is None:
        return None
    link = session.exec(select(FileArrLink).where(FileArrLink.file_id == det.file_id)).first()
    if link is None:
        return None
    existing = _open_replacement(session, detection_id)
    if existing is not None:
        return _replacement_dict(existing)
    repl = _new_replacement(session, det, link)
    enqueue_notification(
        session,
        NotificationEvent.REPLACEMENT_PENDING_APPROVAL,
        {"detection_id": detection_id},
        dedup_key=f"det:{detection_id}",
    )
    return _replacement_dict(repl)


def approve_replacement(session: Session, replacement_id: int, by: str = "user") -> dict | None:
    r = session.get(Replacement, replacement_id)
    if r is None:
        return None
    if r.status is ReplacementStatus.PENDING_APPROVAL:
        r.status = ReplacementStatus.APPROVED
        r.approved_by = by
        r.approved_at = clock.iso_now()
        r.updated_at = clock.iso_now()
        session.add(r)
    return _replacement_dict(r)


def reject_replacement(session: Session, replacement_id: int) -> dict | None:
    r = session.get(Replacement, replacement_id)
    if r is None:
        return None
    r.status = ReplacementStatus.REJECTED
    r.updated_at = clock.iso_now()
    session.add(r)
    return _replacement_dict(r)


def approve_all_pending(session: Session, by: str = "user") -> int:
    rows = session.exec(
        select(Replacement).where(Replacement.status == ReplacementStatus.PENDING_APPROVAL)
    ).all()
    now = clock.iso_now()
    for r in rows:
        r.status = ReplacementStatus.APPROVED
        r.approved_by, r.approved_at, r.updated_at = by, now, now
        session.add(r)
    return len(rows)


def list_replacements(session: Session) -> list[dict]:
    rows = session.exec(select(Replacement).order_by(col(Replacement.id).desc())).all()
    return [_replacement_dict(r) for r in rows]


# --- executor DB ops (run on the DB thread; the executor is async, §9) ------ #


def _replacement_job(session: Session, r: Replacement) -> ReplacementJob | None:
    if not (r.id and r.arr_instance and r.media_type and r.media_id and r.arr_file_id):
        return None
    det = session.get(Detection, r.detection_id)
    file = session.get(File, det.file_id) if det is not None else None
    if file is None:
        return None
    return ReplacementJob(
        id=r.id,
        arr_instance=r.arr_instance,
        media_type=r.media_type,
        media_id=r.media_id,
        arr_file_id=r.arr_file_id,
        detection_id=r.detection_id,
        file_path=file.path,
        attempt=r.attempt,
        requested_at=r.requested_at,
    )


def claim_approved(session: Session, limit: int) -> list[ReplacementJob]:
    rows = session.exec(
        select(Replacement).where(Replacement.status == ReplacementStatus.APPROVED).limit(limit)
    ).all()
    return [job for r in rows if (job := _replacement_job(session, r)) is not None]


def in_flight_replacements(session: Session) -> list[ReplacementJob]:
    rows = session.exec(
        select(Replacement).where(Replacement.status == ReplacementStatus.REQUESTED)
    ).all()
    return [job for r in rows if (job := _replacement_job(session, r)) is not None]


def mark_requested(session: Session, replacement_id: int) -> None:
    r = session.get(Replacement, replacement_id)
    if r is None:
        return
    r.status = ReplacementStatus.REQUESTED
    r.requested_at = r.requested_at or clock.iso_now()
    r.updated_at = clock.iso_now()
    session.add(r)
    enqueue_notification(
        session,
        NotificationEvent.REPLACEMENT_REQUESTED,
        {"replacement_id": replacement_id},
        dedup_key=f"repl:{replacement_id}:req:{r.attempt}",
    )


def replacement_failed(session: Session, replacement_id: int, note: str) -> None:
    r = session.get(Replacement, replacement_id)
    if r is None:
        return
    r.status = ReplacementStatus.FAILED
    r.notes, r.updated_at = note, clock.iso_now()
    session.add(r)
    det = session.get(Detection, r.detection_id)
    if det is not None:
        det.status = DetectionStatus.NEEDS_ATTENTION
        session.add(det)


def replacement_verified(
    session: Session, replacement_id: int, verdict: Verdict, max_attempts: int
) -> None:
    """Apply the verify re-scan outcome: succeed / retry / exhaust (SPEC §9 #6)."""
    r = session.get(Replacement, replacement_id)
    if r is None:
        return
    det = session.get(Detection, r.detection_id)
    if verdict is Verdict.OK:
        r.status = ReplacementStatus.SUCCEEDED
        if det is not None:
            det.status = DetectionStatus.RESOLVED
            det.resolved_at = clock.iso_now()
            session.add(det)
        enqueue_notification(
            session,
            NotificationEvent.REPLACEMENT_COMPLETED,
            {"detection_id": r.detection_id},
            dedup_key=f"repl:{replacement_id}:done",
        )
    elif r.attempt < max_attempts:
        r.attempt += 1
        r.status = ReplacementStatus.APPROVED  # re-execute delete+search
        r.requested_at = None
    else:
        r.status = ReplacementStatus.EXHAUSTED
        if det is not None:
            det.status = DetectionStatus.NEEDS_ATTENTION
            session.add(det)
    r.updated_at = clock.iso_now()
    session.add(r)




# --- Notifications (SPEC §10) ------------------------------------------------ #


@dataclass
class PendingNotification:
    id: int
    payload: str  # JSON


def enqueue_notification(
    session: Session,
    event: NotificationEvent,
    payload: dict,
    *,
    dedup_key: str | None = None,
) -> None:
    """Queue an event for the periodic flusher (§10). dedup_key collapses repeats."""
    if dedup_key is not None:
        existing = session.exec(
            select(NotificationQueue).where(
                NotificationQueue.event_type == event,
                NotificationQueue.dedup_key == dedup_key,
                NotificationQueue.status == NotificationStatus.PENDING,
            ).limit(1)
        ).first()
        if existing is not None:
            return
    session.add(
        NotificationQueue(event_type=event, dedup_key=dedup_key, payload=json.dumps(payload))
    )


def pending_notifications(session: Session) -> dict[NotificationEvent, list[PendingNotification]]:
    rows = session.exec(
        select(NotificationQueue)
        .where(NotificationQueue.status == NotificationStatus.PENDING)
        .order_by(col(NotificationQueue.created_at))
    ).all()
    groups: dict[NotificationEvent, list[PendingNotification]] = {}
    for r in rows:
        assert r.id is not None
        groups.setdefault(r.event_type, []).append(PendingNotification(id=r.id, payload=r.payload))
    return groups


def mark_notifications(session: Session, ids: list[int], status: NotificationStatus) -> None:
    now = clock.iso_now()
    for nid in ids:
        n = session.get(NotificationQueue, nid)
        if n is not None:
            n.status = status
            n.sent_at = now if status is NotificationStatus.SENT else None
            session.add(n)


def log_notification(
    session: Session,
    event: NotificationEvent,
    title: str,
    batched: int,
    status: NotificationStatus,
    error: str | None = None,
) -> None:
    session.add(
        NotificationLog(
            event_type=event, title=title, batched=batched, status=status, error=error
        )
    )


def list_notifications(session: Session, limit: int = 50) -> list[dict]:
    rows = session.exec(
        select(NotificationLog).order_by(col(NotificationLog.id).desc()).limit(limit)
    ).all()
    return [
        {
            "id": r.id,
            "event_type": r.event_type,
            "title": r.title,
            "batched": r.batched,
            "status": r.status,
            "created_at": r.created_at,
        }
        for r in rows
    ]


# --- live activity (dashboard) ---------------------------------------------- #

_SKIPPED = (Disposition.SKIPPED_TTL, Disposition.SKIPPED_TOO_FRESH)


def _count(session: Session, *where: ColumnElement[bool]) -> int:
    return int(session.exec(select(func.count()).select_from(RunFile).where(*where)).one())


def active_runs(session: Session) -> list[dict]:
    """Running runs with LIVE progress + ETA, computed from the run_files ledger
    (the JobRun counters are only materialised at finalize)."""
    runs = session.exec(select(JobRun).where(JobRun.status == RunStatus.RUNNING)).all()
    out: list[dict] = []
    for run in runs:
        total = run.files_discovered
        rid = col(RunFile.job_run_id) == run.id
        scanned = _count(session, rid, col(RunFile.outcome).is_not(None))
        skipped = _count(session, rid, col(RunFile.disposition).in_(_SKIPPED))
        corrupt = _count(session, rid, col(RunFile.outcome) == Verdict.CORRUPT)
        done = scanned + skipped
        elapsed = clock.age_seconds(run.started_at) if run.started_at else 0.0
        # ETA extrapolates from throughput so far; None until at least one file lands.
        eta = (elapsed * (total - done) / done) if done > 0 and total > done else None
        out.append(
            {
                "run_id": run.id,
                "job_name": run.job_name,
                "started_at": run.started_at,
                "files_total": total,
                "files_done": done,
                "files_corrupt": corrupt,
                "progress": round(done / total, 4) if total else 0.0,
                "elapsed_seconds": round(elapsed),
                "eta_seconds": round(eta) if eta is not None else None,
            }
        )
    return out


def active_tasks(session: Session, limit: int = 50) -> list[dict]:
    """Files being decoded right now (claimed → SCANNING). updated_at is the claim
    time, so it doubles as 'started scanning at' for elapsed display; the live
    intra-file decode progress comes from scan_progress."""
    rows = session.exec(
        select(ScanTask)
        .where(ScanTask.status == TaskStatus.SCANNING)
        .order_by(col(ScanTask.updated_at))
        .limit(limit)
    ).all()
    out: list[dict] = []
    for t in rows:
        try:
            size: int | None = os.stat(t.path).st_size
        except OSError:
            size = None
        prog = session.get(ScanProgress, t.id) if t.id is not None else None
        out.append(
            {
                "task_id": t.id,
                "path": t.path,
                "started_at": t.updated_at,
                "size_bytes": size,
                "pct": prog.pct if prog is not None else None,
                "position_s": prog.position_s if prog is not None else None,
                "duration_s": prog.duration_s if prog is not None else None,
                "frames": prog.frames if prog is not None else None,
            }
        )
    return out


def upsert_progress(session: Session, updates: list[ProgressUpdate]) -> None:
    """Persist the latest decode position for each in-flight task (main thread)."""
    for u in updates:
        task = session.get(ScanTask, u.task_id)
        if task is None or task.status is not TaskStatus.SCANNING:
            continue  # finished/vanished — don't resurrect a stale progress row
        row = session.get(ScanProgress, u.task_id)
        if row is None:
            row = ScanProgress(task_id=u.task_id)
        row.position_s = u.position_s
        row.duration_s = u.duration_s
        row.frames = u.frames
        row.pct = round(u.position_s / u.duration_s, 4) if u.duration_s > 0 else None
        row.updated_at = clock.iso_now()
        session.add(row)


def clear_progress(session: Session, task_id: int) -> None:
    row = session.get(ScanProgress, task_id)
    if row is not None:
        session.delete(row)


def clear_all_progress(session: Session) -> None:
    """Drop stale progress rows (e.g. at startup after a crash)."""
    for row in session.exec(select(ScanProgress)).all():
        session.delete(row)
