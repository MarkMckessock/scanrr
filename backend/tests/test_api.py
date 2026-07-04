"""Full-stack smoke: HTTP → orchestrator → real ProcessPool → DB (SPEC §11)."""

from __future__ import annotations

import shutil
import time

import pytest
from fastapi.testclient import TestClient

from scanrr.core import config as config_module
from scanrr.db import engine as db_engine


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
