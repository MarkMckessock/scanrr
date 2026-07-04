"""Full-stack smoke: HTTP → orchestrator → real ProcessPool → DB (SPEC §11).

Jobs are defined only in the YAML config (mounted at ``SCANRR_CONFIG_FILE``).
"""

from __future__ import annotations

import shutil
import textwrap
import time

import pytest
from fastapi.testclient import TestClient

from scanrr.core import config as config_module
from scanrr.db import engine as db_engine


def _config(tmp_path, body: str, monkeypatch) -> None:
    path = tmp_path / "scanrr.yaml"
    path.write_text(textwrap.dedent(body))
    monkeypatch.setattr(config_module.settings, "config_file", str(path))


def test_api_serves_spa_and_lists_yaml_jobs(tmp_path, monkeypatch):
    db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    _config(
        tmp_path,
        f"""
        jobs:
          - {{ name: Movies, type: path, root_path: {tmp_path} }}
        """,
        monkeypatch,
    )
    from scanrr.api.app import FRONTEND_DIST, app

    with TestClient(app) as client:
        if FRONTEND_DIST.is_dir():  # SPA served only when the frontend is built
            root = client.get("/")
            assert root.status_code == 200 and "scanrr" in root.text

        assert client.get("/api/stats").json()["jobs"] == 1
        jobs = client.get("/api/jobs").json()
        assert jobs[0]["slug"] == "movies" and jobs[0]["name"] == "Movies"


@pytest.mark.requires_ffmpeg
def test_api_run_yaml_job_and_triage(tmp_path, media, monkeypatch):
    db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    lib = tmp_path / "lib"
    lib.mkdir()
    shutil.copy(media["clean"], lib / "good.mkv")
    shutil.copy(media["bitflip"], lib / "bad.mkv")
    _config(
        tmp_path,
        f"""
        settings:
          min_file_size_bytes: 0
          min_file_age_seconds: 0
        jobs:
          - {{ name: Test, type: path, root_path: {lib} }}
        """,
        monkeypatch,
    )
    from scanrr.api.app import app

    with TestClient(app) as client:
        assert client.get("/api/health").json() == {"status": "ok"}

        run_id = client.post("/api/jobs/test/run").json()["run_id"]
        deadline = time.time() + 30
        while time.time() < deadline:
            run = client.get(f"/api/runs/{run_id}").json()
            if run["status"] == "completed":
                break
            time.sleep(0.1)
        assert run["status"] == "completed"
        assert run["files_scanned"] == 2 and run["files_corrupt"] == 1
        assert run["job_name"] == "Test"  # snapshot on the run

        detections = client.get("/api/detections").json()
        assert len(detections) == 1 and detections[0]["path"].endswith("bad.mkv")

        det_id = detections[0]["id"]
        acked = client.post(f"/api/detections/{det_id}/acknowledge").json()
        assert acked["status"] == "acknowledged"
        assert client.get("/api/detections", params={"status": "open"}).json() == []
