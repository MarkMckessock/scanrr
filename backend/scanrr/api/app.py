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
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sqlmodel import Session

from scanrr.core.config import DEFAULTS, settings
from scanrr.core.events import EventBus
from scanrr.core.fileconfig import load_file_config
from scanrr.core.logging import configure as configure_logging
from scanrr.db.database import Database
from scanrr.db.engine import get_engine, init_db
from scanrr.enums import DetectionStatus, RunTrigger
from scanrr.integrations.arr import make_client
from scanrr.jobs.scheduler import Scheduler
from scanrr.scanning import engine
from scanrr.scanning.executor import PebbleExecutor
from scanrr.scanning.notifier import NotificationFlusher
from scanrr.scanning.orchestrator import Orchestrator
from scanrr.scanning.replacer import ReplacementExecutor

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
    # YAML config (settings override + in-memory jobs) is the IaC source of truth.
    fc = load_file_config(settings.config_file, DEFAULTS)
    yaml_registry = {spec.slug: spec for spec in fc.jobs}
    arr_registry = {inst.name: inst for inst in fc.arr_instances}
    db = Database(get_engine())
    bus = EventBus()
    orchestrator = Orchestrator(
        db,
        PebbleExecutor(fc.config.max_scan_workers),
        fc.config,
        bus=bus,
        yaml_jobs=yaml_registry,
        arr_instances=arr_registry,
    )
    await orchestrator.start()
    scheduler = Scheduler(orchestrator, db, fc.config, yaml_jobs=fc.jobs)
    await scheduler.start()
    flusher = NotificationFlusher(db, fc.config, fc.pushover)
    await flusher.start()
    replacer = ReplacementExecutor(db, fc.config, arr_registry, orchestrator.executor)
    await replacer.start()
    app.state.db, app.state.bus = db, bus
    app.state.orchestrator, app.state.scheduler = orchestrator, scheduler
    app.state.config, app.state.yaml_specs = fc.config, fc.jobs
    app.state.arr_instances = fc.arr_instances
    try:
        yield
    finally:
        scheduler.stop()
        await flusher.stop()
        await replacer.stop()
        await orchestrator.stop()
        db.close()


app = FastAPI(title="scanrr", version="0.1.0", lifespan=lifespan)


def require_token(x_scanrr_token: str | None = Header(default=None)) -> None:
    if settings.api_token and x_scanrr_token != settings.api_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-Scanrr-Token")


def _db(request: Request) -> Database:
    return request.app.state.db


# --- reads ------------------------------------------------------------------ #


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/stats")
async def get_stats(request: Request) -> dict:
    stats = await _db(request).run(engine.stats)
    stats["jobs"] = len(request.app.state.yaml_specs)  # jobs live in the YAML registry
    return stats


@app.get("/api/jobs")
async def list_jobs(request: Request) -> list[dict]:
    yaml_specs = request.app.state.yaml_specs

    def _list(session: Session) -> list[dict]:
        return engine.list_jobs(session, yaml_specs)

    return await _db(request).run(_list)


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
def get_settings(request: Request) -> dict:
    return request.app.state.config.model_dump(mode="json")


# --- actions ---------------------------------------------------------------- #


# Jobs are defined in the YAML config (read-only) — no create/update/delete here.


@app.post("/api/jobs/{slug}/run", dependencies=[Depends(require_token)])
async def trigger_run(slug: str, request: Request) -> dict:
    try:
        run_id = await request.app.state.orchestrator.trigger_run(slug, RunTrigger.MANUAL)
    except ValueError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found") from exc
    return {"run_id": run_id}


@app.post("/api/runs/{run_id}/cancel", dependencies=[Depends(require_token)])
async def cancel_run(run_id: int, request: Request) -> dict:
    await request.app.state.orchestrator.cancel_run(run_id)
    result = await _db(request).run(lambda s: engine.get_run(s, run_id))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return result


@app.post("/api/detections/{det_id}/replace", dependencies=[Depends(require_token)])
async def replace_detection(det_id: int, request: Request) -> dict:
    """Propose a replacement (→ pending_approval). Execution lands in M5."""
    result = await _db(request).run(lambda s: engine.create_replacement(s, det_id))
    if result is None:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "detection not found or file has no arr link"
        )
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


# --- Sonarr / Radarr (defined in YAML — read-only) -------------------------- #


def _arr_dict(inst) -> dict:  # never includes api_key
    return {
        "name": inst.name,
        "type": inst.type,
        "url": inst.url,
        "mappings": [{"from": remote, "to": local} for remote, local in inst.mappings],
    }


@app.get("/api/arr-instances")
def list_arr_instances(request: Request) -> list[dict]:
    return [_arr_dict(inst) for inst in request.app.state.arr_instances]


@app.post("/api/arr-instances/{name}/test", dependencies=[Depends(require_token)])
async def test_arr_instance(name: str, request: Request) -> dict:
    inst = next((a for a in request.app.state.arr_instances if a.name == name), None)
    if inst is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "instance not found")
    client = make_client(inst.type, inst.url, inst.api_key)
    try:
        info = await client.test()
        return {"ok": True, "version": info.get("version")}
    except Exception as exc:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"connection failed: {exc}") from exc
    finally:
        await client.close()


@app.get("/api/replacements")
async def list_replacements(request: Request) -> list[dict]:
    return await _db(request).run(engine.list_replacements)


@app.post("/api/replacements/approve", dependencies=[Depends(require_token)])
async def approve_all_replacements(request: Request) -> dict:
    n = await _db(request).run(lambda s: engine.approve_all_pending(s))
    return {"approved": n}


@app.post("/api/replacements/{replacement_id}/approve", dependencies=[Depends(require_token)])
async def approve_replacement(replacement_id: int, request: Request) -> dict:
    result = await _db(request).run(lambda s: engine.approve_replacement(s, replacement_id))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "replacement not found")
    return result


@app.post("/api/replacements/{replacement_id}/reject", dependencies=[Depends(require_token)])
async def reject_replacement(replacement_id: int, request: Request) -> dict:
    result = await _db(request).run(lambda s: engine.reject_replacement(s, replacement_id))
    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "replacement not found")
    return result


@app.get("/api/notifications")
async def list_notifications(request: Request) -> list[dict]:
    return await _db(request).run(engine.list_notifications)


# --- realtime --------------------------------------------------------------- #


@app.get("/api/events")
async def events(request: Request) -> StreamingResponse:
    bus: EventBus = request.app.state.bus

    async def stream():
        queue = bus.subscribe()
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {data}\n\n"
                except TimeoutError:
                    yield ": ping\n\n"  # heartbeat (cancelling queue.get() is safe)
                if await request.is_disconnected():
                    break
        finally:
            bus.unsubscribe(queue)

    return StreamingResponse(stream(), media_type="text/event-stream")


# --- static SPA (declared last so /api/* routes win) ------------------------ #

if FRONTEND_DIST.is_dir():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def spa(full_path: str) -> FileResponse:
        """Serve index.html for any non-API path so client-side routes deep-link."""
        if full_path.startswith("api/"):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "not found")
        return FileResponse(FRONTEND_DIST / "index.html")
