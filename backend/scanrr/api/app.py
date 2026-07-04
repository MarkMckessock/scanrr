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
from pydantic import BaseModel
from sqlmodel import Session

from scanrr.core.config import DEFAULTS, settings
from scanrr.core.events import EventBus
from scanrr.core.fileconfig import load_file_config
from scanrr.core.logging import configure as configure_logging
from scanrr.db.database import Database
from scanrr.db.engine import get_engine, init_db
from scanrr.enums import ArrType, DetectionStatus, RunTrigger
from scanrr.integrations.arr import make_client
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
    # YAML config (settings override + in-memory jobs) is the IaC source of truth.
    config, yaml_specs = load_file_config(settings.config_file, DEFAULTS)
    yaml_registry = {spec.slug: spec for spec in yaml_specs}
    db = Database(get_engine())
    bus = EventBus()
    orchestrator = Orchestrator(
        db, PebbleExecutor(config.max_scan_workers), config, bus=bus, yaml_jobs=yaml_registry
    )
    await orchestrator.start()
    scheduler = Scheduler(orchestrator, db, config, yaml_jobs=yaml_specs)
    await scheduler.start()
    app.state.db, app.state.bus = db, bus
    app.state.orchestrator, app.state.scheduler = orchestrator, scheduler
    app.state.config, app.state.yaml_specs = config, yaml_specs
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


class ArrInstanceCreate(BaseModel):
    type: ArrType
    name: str
    base_url: str
    api_key: str


class PathMappingCreate(BaseModel):
    arr_instance_id: int
    remote_path: str
    local_path: str


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


# --- Sonarr / Radarr integration (M4) --------------------------------------- #


@app.get("/api/arr-instances")
async def list_arr_instances(request: Request) -> list[dict]:
    return await _db(request).run(engine.list_arr_instances)


@app.post("/api/arr-instances", dependencies=[Depends(require_token)])
async def create_arr_instance(body: ArrInstanceCreate, request: Request) -> dict:
    def _create(session: Session) -> dict:
        return engine.create_arr_instance(
            session, type=body.type, name=body.name, base_url=body.base_url, api_key=body.api_key
        )

    return await _db(request).run(_create)


@app.delete("/api/arr-instances/{instance_id}", dependencies=[Depends(require_token)])
async def delete_arr_instance(instance_id: int, request: Request) -> dict:
    ok = await _db(request).run(lambda s: engine.delete_arr_instance(s, instance_id))
    if not ok:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "instance not found")
    return {"deleted": instance_id}


@app.post("/api/arr-instances/{instance_id}/test", dependencies=[Depends(require_token)])
async def test_arr_instance(instance_id: int, request: Request) -> dict:
    inst = await _db(request).run(lambda s: engine.get_arr_instance_info(s, instance_id))
    if inst is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "instance not found or disabled")
    client = make_client(inst.type, inst.base_url, inst.api_key)
    try:
        info = await client.test()
        return {"ok": True, "version": info.get("version")}
    except Exception as exc:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"connection failed: {exc}") from exc
    finally:
        await client.close()


@app.get("/api/path-mappings")
async def list_path_mappings(request: Request) -> list[dict]:
    return await _db(request).run(engine.list_path_mappings)


@app.post("/api/path-mappings", dependencies=[Depends(require_token)])
async def create_path_mapping(body: PathMappingCreate, request: Request) -> dict:
    def _create(session: Session) -> dict:
        return engine.create_path_mapping(
            session,
            arr_instance_id=body.arr_instance_id,
            remote_path=body.remote_path,
            local_path=body.local_path,
        )

    return await _db(request).run(_create)


@app.delete("/api/path-mappings/{mapping_id}", dependencies=[Depends(require_token)])
async def delete_path_mapping(mapping_id: int, request: Request) -> dict:
    ok = await _db(request).run(lambda s: engine.delete_path_mapping(s, mapping_id))
    if not ok:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "mapping not found")
    return {"deleted": mapping_id}


@app.get("/api/replacements")
async def list_replacements(request: Request) -> list[dict]:
    return await _db(request).run(engine.list_replacements)


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


# --- static SPA (declared last so /api/* routes win) ------------------------ #

if FRONTEND_DIST.is_dir():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def spa(full_path: str) -> FileResponse:
        """Serve index.html for any non-API path so client-side routes deep-link."""
        if full_path.startswith("api/"):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "not found")
        return FileResponse(FRONTEND_DIST / "index.html")
