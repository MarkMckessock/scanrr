"""FastAPI surface (SPEC §11). Read/action endpoints for the UI, an SSE event
stream the orchestrator publishes to, and static serving of the built SPA.

The orchestrator, scheduler, event bus, and single-thread DB live on ``app.state``.
All DB access goes through ``app.state.db``. Mutating routes require the
``X-Scanrr-Token`` shared secret when configured.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlmodel import Session

from scanrr.core.config import DEFAULTS, settings
from scanrr.core.events import EventBus
from scanrr.core.logging import configure as configure_logging
from scanrr.db.database import Database
from scanrr.db.engine import get_engine, init_db
from scanrr.db.models import Job
from scanrr.enums import DetectionStatus, RunTrigger
from scanrr.jobs.scheduler import Scheduler
from scanrr.scanning import engine
from scanrr.scanning.executor import PebbleExecutor
from scanrr.scanning.orchestrator import Orchestrator

FRONTEND_DIST = Path(__file__).resolve().parents[3] / "frontend" / "dist"

_TRIAGE = {
    "acknowledge": DetectionStatus.ACKNOWLEDGED,
    "ignore": DetectionStatus.IGNORED,
    "resolve": DetectionStatus.RESOLVED,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    init_db()
    db = Database(get_engine())
    bus = EventBus()
    orchestrator = Orchestrator(db, PebbleExecutor(DEFAULTS.max_scan_workers), DEFAULTS, bus=bus)
    await orchestrator.start()
    scheduler = Scheduler(orchestrator, db, DEFAULTS)
    await scheduler.start()
    app.state.db, app.state.bus = db, bus
    app.state.orchestrator, app.state.scheduler = orchestrator, scheduler
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


def _db(request: Request) -> Database:
    return request.app.state.db


class JobCreate(BaseModel):
    name: str
    root_path: str
    ttl_days: int = 30
    schedule_cron: str | None = None


class JobUpdate(BaseModel):
    name: str | None = None
    enabled: bool | None = None
    ttl_seconds: int | None = None
    schedule_cron: str | None = None
    auto_replace: bool | None = None


# --- reads ------------------------------------------------------------------ #


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/stats")
async def get_stats(request: Request) -> dict:
    return await _db(request).run(engine.stats)


@app.get("/api/jobs")
async def list_jobs(request: Request) -> list[dict]:
    return await _db(request).run(engine.list_jobs)


@app.get("/api/runs")
async def list_runs(request: Request) -> list[dict]:
    return await _db(request).run(engine.list_runs)


@app.get("/api/runs/{run_id}")
async def get_run(run_id: int, request: Request) -> dict:
    result = await _db(request).run(lambda s: engine.get_run(s, run_id))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return result


@app.get("/api/runs/{run_id}/files")
async def get_run_files(run_id: int, request: Request) -> list[dict]:
    return await _db(request).run(lambda s: engine.run_files(s, run_id))


@app.get("/api/detections")
async def list_detections(request: Request, status: DetectionStatus | None = None) -> list[dict]:
    return await _db(request).run(lambda s: engine.list_detections(s, status))


@app.get("/api/settings")
def get_settings() -> dict:
    return DEFAULTS.model_dump(mode="json")


# --- actions ---------------------------------------------------------------- #


@app.post("/api/jobs", dependencies=[Depends(require_token)])
async def create_job(body: JobCreate, request: Request) -> dict:
    def _create(session: Session) -> dict:
        return engine.create_job(
            session,
            name=body.name,
            root_path=body.root_path,
            ttl_seconds=body.ttl_days * 86_400,
            schedule_cron=body.schedule_cron,
        )

    return await _db(request).run(_create)


@app.put("/api/jobs/{job_id}", dependencies=[Depends(require_token)])
async def update_job(job_id: int, body: JobUpdate, request: Request) -> dict:
    fields = body.model_dump(exclude_unset=True)
    result = await _db(request).run(lambda s: engine.update_job(s, job_id, fields))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return result


@app.delete("/api/jobs/{job_id}", dependencies=[Depends(require_token)])
async def delete_job(job_id: int, request: Request) -> dict:
    ok = await _db(request).run(lambda s: engine.delete_job(s, job_id))
    if not ok:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return {"deleted": job_id}


@app.post("/api/jobs/{job_id}/run", dependencies=[Depends(require_token)])
async def trigger_run(job_id: int, request: Request) -> dict:
    if await _db(request).run(lambda s: s.get(Job, job_id)) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    run_id = await request.app.state.orchestrator.trigger_run(job_id, RunTrigger.MANUAL)
    return {"run_id": run_id}


@app.post("/api/runs/{run_id}/cancel", dependencies=[Depends(require_token)])
async def cancel_run(run_id: int, request: Request) -> dict:
    await request.app.state.orchestrator.cancel_run(run_id)
    result = await _db(request).run(lambda s: engine.get_run(s, run_id))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return result


@app.post("/api/detections/{det_id}/{action}", dependencies=[Depends(require_token)])
async def triage_detection(det_id: int, action: str, request: Request) -> dict:
    new_status = _TRIAGE.get(action)
    if new_status is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"unknown action: {action}")
    ok = await _db(request).run(lambda s: engine.set_detection_status(s, det_id, new_status))
    if not ok:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "detection not found")
    return {"id": det_id, "status": new_status}


# --- realtime --------------------------------------------------------------- #


@app.get("/api/events")
async def events(request: Request) -> StreamingResponse:
    bus: EventBus = request.app.state.bus

    async def stream():
        source = bus.subscribe()
        try:
            while True:
                try:
                    data = await asyncio.wait_for(source.__anext__(), timeout=15)
                    yield f"data: {data}\n\n"
                except TimeoutError:
                    yield ": ping\n\n"  # heartbeat
                if await request.is_disconnected():
                    break
        finally:
            await source.aclose()

    return StreamingResponse(stream(), media_type="text/event-stream")


# --- static SPA (must be mounted last so /api/* wins) ----------------------- #

if FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="spa")
