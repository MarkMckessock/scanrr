"""Minimal FastAPI surface for M1 (SPEC §11). Full API arrives with the UI (M3).

Mutating routes require the ``X-Scanrr-Token`` shared secret when one is
configured (SPEC §11/§14); when unset (dev), the check is skipped.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, col, select

from scanrr.core.config import settings
from scanrr.core.logging import configure as configure_logging
from scanrr.db.engine import get_engine, init_db
from scanrr.db.models import Detection, File, Job, JobRun
from scanrr.enums import JobType
from scanrr.scanning.engine import run_job


@asynccontextmanager
async def lifespan(_app: FastAPI):
    configure_logging()
    init_db()
    yield


app = FastAPI(title="scanrr", version="0.1.0", lifespan=lifespan)


def db() -> Iterator[Session]:
    with Session(get_engine()) as session:
        yield session


def require_token(x_scanrr_token: str | None = Header(default=None)) -> None:
    if settings.api_token and x_scanrr_token != settings.api_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-Scanrr-Token")


class JobCreate(BaseModel):
    name: str
    root_path: str
    ttl_days: int = 30


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/jobs", dependencies=[Depends(require_token)])
def create_job(body: JobCreate, session: Session = Depends(db)) -> Job:
    job = Job(
        name=body.name,
        type=JobType.PATH,
        ttl_seconds=body.ttl_days * 86_400,
        config=json.dumps({"root_path": body.root_path}),
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


@app.post("/api/jobs/{job_id}/run", dependencies=[Depends(require_token)])
def run(job_id: int, session: Session = Depends(db)) -> JobRun:
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return run_job(session, job)  # M1: synchronous


@app.get("/api/runs/{run_id}")
def get_run(run_id: int, session: Session = Depends(db)) -> JobRun:
    run_row = session.get(JobRun, run_id)
    if run_row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return run_row


@app.get("/api/detections")
def list_detections(session: Session = Depends(db)) -> list[dict]:
    rows = session.exec(
        select(Detection, File).join(File, col(Detection.file_id) == col(File.id))
    ).all()
    return [
        {
            "id": det.id,
            "path": f.path,
            "hash": det.hash,
            "status": det.status,
            "detected_at": det.detected_at,
        }
        for det, f in rows
    ]
