"""FastAPI surface (SPEC §11). Full UI-facing API arrives in M3; this covers the
M2 run lifecycle: create jobs, trigger async runs, poll, cancel.

The orchestrator, scheduler, and single-thread DB live on ``app.state`` for the
process lifetime. All DB access goes through ``app.state.db`` (the one DB thread).
Mutating routes require the ``X-Scanrr-Token`` shared secret when configured.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel
from sqlmodel import Session, col, select

from scanrr.core.config import DEFAULTS, settings
from scanrr.core.logging import configure as configure_logging
from scanrr.db.database import Database
from scanrr.db.engine import get_engine, init_db
from scanrr.db.models import Detection, File, Job, JobRun
from scanrr.enums import JobType, RunTrigger
from scanrr.jobs.scheduler import Scheduler
from scanrr.scanning.executor import PebbleExecutor
from scanrr.scanning.orchestrator import Orchestrator


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    init_db()
    db = Database(get_engine())
    config = DEFAULTS
    orchestrator = Orchestrator(db, PebbleExecutor(config.max_scan_workers), config)
    await orchestrator.start()
    scheduler = Scheduler(orchestrator, db, config)
    await scheduler.start()
    app.state.db = db
    app.state.orchestrator = orchestrator
    app.state.scheduler = scheduler
    try:
        yield
    finally:
        scheduler.stop()
        await orchestrator.stop()
        db.close()


app = FastAPI(title="scanrr", version="0.1.0", lifespan=lifespan)


def require_token(x_scanrr_token: str | None = Header(default=None)) -> None:
    if settings.api_token and x_scanrr_token != settings.api_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-Scanrr-Token")


class JobCreate(BaseModel):
    name: str
    root_path: str
    ttl_days: int = 30


def _create_job(session: Session, body: JobCreate) -> dict:
    job = Job(
        name=body.name,
        type=JobType.PATH,
        ttl_seconds=body.ttl_days * 86_400,
        config=json.dumps({"root_path": body.root_path}),
    )
    session.add(job)
    session.flush()
    return {"id": job.id, "name": job.name, "type": job.type}


def _run_dict(session: Session, run_id: int) -> dict | None:
    run = session.get(JobRun, run_id)
    if run is None:
        return None
    return {
        "id": run.id,
        "job_id": run.job_id,
        "status": run.status,
        "files_discovered": run.files_discovered,
        "files_scanned": run.files_scanned,
        "files_skipped": run.files_skipped,
        "files_corrupt": run.files_corrupt,
        "files_unreadable": run.files_unreadable,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
    }


def _detections(session: Session) -> list[dict]:
    rows = session.exec(
        select(Detection, File).join(File, col(Detection.file_id) == col(File.id))
    ).all()
    return [
        {
            "id": d.id,
            "path": f.path,
            "hash": d.hash,
            "status": d.status,
            "detected_at": d.detected_at,
        }
        for d, f in rows
    ]


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/jobs", dependencies=[Depends(require_token)])
async def create_job(body: JobCreate, request: Request) -> dict:
    return await request.app.state.db.run(lambda s: _create_job(s, body))


@app.post("/api/jobs/{job_id}/run", dependencies=[Depends(require_token)])
async def trigger_run(job_id: int, request: Request) -> dict:
    db: Database = request.app.state.db
    if await db.run(lambda s: s.get(Job, job_id)) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    run_id = await request.app.state.orchestrator.trigger_run(job_id, RunTrigger.MANUAL)
    return {"run_id": run_id}


@app.post("/api/runs/{run_id}/cancel", dependencies=[Depends(require_token)])
async def cancel_run(run_id: int, request: Request) -> dict:
    await request.app.state.orchestrator.cancel_run(run_id)
    result = await request.app.state.db.run(lambda s: _run_dict(s, run_id))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return result


@app.get("/api/runs/{run_id}")
async def get_run(run_id: int, request: Request) -> dict:
    result = await request.app.state.db.run(lambda s: _run_dict(s, run_id))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return result


@app.get("/api/detections")
async def list_detections(request: Request) -> list[dict]:
    return await request.app.state.db.run(_detections)
