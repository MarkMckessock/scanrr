"""The shared, path-deduplicated scan queue (SPEC §6).

One active ``scan_task`` per path; many runs may subscribe. The
``ux_scan_tasks_active_path`` partial unique index is the real dedup guard —
``active_task`` is the optimistic check.
"""

from __future__ import annotations

from sqlmodel import Session, col, select

from scanrr.core import clock
from scanrr.db.models import ScanTask, ScanTaskSubscriber
from scanrr.enums import TaskStatus

_ACTIVE = (TaskStatus.PENDING, TaskStatus.SCANNING)


def _next_seq(session: Session) -> int:
    top = session.exec(
        select(ScanTask.seq).order_by(col(ScanTask.seq).desc()).limit(1)
    ).first()
    return (top or 0) + 1


def active_task(session: Session, path: str) -> ScanTask | None:
    return session.exec(
        select(ScanTask).where(ScanTask.path == path, col(ScanTask.status).in_(_ACTIVE))
    ).first()


def subscribe(session: Session, scan_task_id: int, job_run_id: int) -> None:
    existing = session.get(ScanTaskSubscriber, (scan_task_id, job_run_id))
    if existing is None:
        session.add(ScanTaskSubscriber(scan_task_id=scan_task_id, job_run_id=job_run_id))


def enqueue_or_subscribe(session: Session, path: str, job_run_id: int) -> ScanTask:
    """Subscribe to the active task for ``path`` if one exists, else create it."""
    task = active_task(session, path)
    if task is None:
        task = ScanTask(seq=_next_seq(session), path=path, status=TaskStatus.PENDING)
        session.add(task)
        session.flush()  # assign task.id
    assert task.id is not None
    subscribe(session, task.id, job_run_id)
    return task


def subscribers(session: Session, scan_task_id: int) -> list[int]:
    return list(
        session.exec(
            select(ScanTaskSubscriber.job_run_id).where(
                ScanTaskSubscriber.scan_task_id == scan_task_id
            )
        )
    )


def claim_next_pending(session: Session) -> ScanTask | None:
    """Claim the lowest-``seq`` pending task and mark it ``scanning``."""
    task = session.exec(
        select(ScanTask)
        .where(ScanTask.status == TaskStatus.PENDING)
        .order_by(col(ScanTask.seq))
        .limit(1)
    ).first()
    if task is None:
        return None
    task.status = TaskStatus.SCANNING
    task.updated_at = clock.iso_now()
    session.add(task)
    session.flush()
    return task
