"""Full-stack smoke: HTTP → orchestrator → real ProcessPool → DB (SPEC §11)."""

from __future__ import annotations

import shutil
import time

import pytest
from fastapi.testclient import TestClient

from scanrr.core import config as config_module
from scanrr.db import engine as db_engine


def test_api_jobs_crud_and_stats(tmp_path):
    db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    from scanrr.api.app import app

    with TestClient(app) as client:
        # the built SPA is served at the root (only when the frontend has been built)
        from scanrr.api.app import FRONTEND_DIST

        if FRONTEND_DIST.is_dir():
            root = client.get("/")
            assert root.status_code == 200 and "scanrr" in root.text

        assert client.get("/api/stats").json()["jobs"] == 0

        job = client.post("/api/jobs", json={"name": "j", "root_path": str(tmp_path)}).json()
        assert client.get("/api/jobs").json()[0]["name"] == "j"
        assert client.get("/api/stats").json()["jobs"] == 1

        updated = client.put(f"/api/jobs/{job['id']}", json={"enabled": False}).json()
        assert updated["enabled"] is False

        assert client.delete(f"/api/jobs/{job['id']}").json()["deleted"] == job["id"]
        assert client.get("/api/jobs").json() == []


@pytest.mark.requires_ffmpeg
def test_api_create_run_poll(tmp_path, media, monkeypatch):
    db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    # Fixtures are tiny and freshly written — relax the size/age gates for the test.
    monkeypatch.setattr(config_module.DEFAULTS, "min_file_size_bytes", 0)
    monkeypatch.setattr(config_module.DEFAULTS, "min_file_age_seconds", 0)

    lib = tmp_path / "lib"
    lib.mkdir()
    shutil.copy(media["clean"], lib / "good.mkv")
    shutil.copy(media["bitflip"], lib / "bad.mkv")

    from scanrr.api.app import app

    with TestClient(app) as client:
        assert client.get("/api/health").json() == {"status": "ok"}

        job_id = client.post(
            "/api/jobs", json={"name": "t", "root_path": str(lib)}
        ).json()["id"]
        run_id = client.post(f"/api/jobs/{job_id}/run").json()["run_id"]

        deadline = time.time() + 30
        while time.time() < deadline:
            run = client.get(f"/api/runs/{run_id}").json()
            if run["status"] == "completed":
                break
            time.sleep(0.1)
        assert run["status"] == "completed"
        assert run["files_scanned"] == 2
        assert run["files_corrupt"] == 1

        detections = client.get("/api/detections").json()
        assert len(detections) == 1
        assert detections[0]["path"].endswith("bad.mkv")

        # triage: acknowledge removes it from the open list
        det_id = detections[0]["id"]
        acked = client.post(f"/api/detections/{det_id}/acknowledge").json()
        assert acked["status"] == "acknowledged"
        assert client.get("/api/detections", params={"status": "open"}).json() == []
