"""YAML infrastructure-as-code config: settings override + jobs are the only source."""

from __future__ import annotations

import json
import shutil
import textwrap

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from scanrr.core import config as config_module
from scanrr.core.config import DEFAULTS, RuntimeConfig
from scanrr.core.fileconfig import load_file_config, slugify
from scanrr.db import engine as db_engine
from scanrr.db.database import Database
from scanrr.db.models import Detection, JobRun
from scanrr.enums import ArrType, JobType, RunStatus
from scanrr.scanning.executor import InlineExecutor
from scanrr.scanning.orchestrator import Orchestrator

CFG = RuntimeConfig(min_file_size_bytes=0, min_file_age_seconds=0)


@pytest.fixture
def eng(tmp_path):
    e = db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    db_engine.init_db()
    return e


def _write(tmp_path, body: str):
    path = tmp_path / "scanrr.yaml"
    path.write_text(textwrap.dedent(body))
    return str(path)


def test_slugify_is_deterministic():
    assert slugify("TV (Sonarr)") == "tv-sonarr"
    assert slugify("Movies") == "movies" == slugify("Movies")
    assert slugify("Movies") != slugify("TV")


def test_load_missing_file_is_noop():
    fc = load_file_config("/no/such/file.yaml", DEFAULTS)
    assert fc.config is DEFAULTS and fc.jobs == [] and fc.arr_instances == []


def test_load_parses_settings_and_jobs(tmp_path):
    path = _write(
        tmp_path,
        """
        settings:
          max_scan_workers: 7
          detector_backend: subprocess
        jobs:
          - name: Movies
            type: path
            root_path: /mnt/movies
            ttl_days: 14
            schedule_cron: "0 3 * * *"
          - name: TV
            type: arr
            arr_instance: sonarr-main
        """,
    )
    fc = load_file_config(path, DEFAULTS)
    assert fc.config.max_scan_workers == 7
    assert fc.config.detector_backend == "subprocess"
    assert {s.slug for s in fc.jobs} == {"movies", "tv"}
    movies = next(s for s in fc.jobs if s.slug == "movies")
    assert movies.type is JobType.PATH
    assert movies.ttl_seconds == 14 * 86_400
    assert movies.schedule_cron == "0 3 * * *"
    tv = next(s for s in fc.jobs if s.slug == "tv")
    assert json.loads(tv.config)["arr_instance"] == "sonarr-main"


def test_load_parses_arr_instances(tmp_path):
    path = _write(
        tmp_path,
        """
        radarr:
          - name: main
            url: http://radarr:7878
            api_key: rkey
            mappings:
              - { from: /data/movies, to: /mnt/movies }
        sonarr:
          - name: tv
            url: http://sonarr:8989
            api_key: skey
            mappings:
              - { from: /data/tv, to: /mnt/tv }
        """,
    )
    fc = load_file_config(path, DEFAULTS)
    assert {a.name for a in fc.arr_instances} == {"main", "tv"}
    main = next(a for a in fc.arr_instances if a.name == "main")
    assert main.type is ArrType.RADARR
    assert main.url == "http://radarr:7878" and main.api_key == "rkey"
    assert main.mappings == (("/data/movies", "/mnt/movies"),)


def test_duplicate_arr_name_rejected(tmp_path):
    path = _write(
        tmp_path,
        """
        radarr:
          - { name: dup, url: http://a, api_key: k }
        sonarr:
          - { name: dup, url: http://b, api_key: k }
        """,
    )
    with pytest.raises(ValueError):
        load_file_config(path, DEFAULTS)


def test_duplicate_slug_rejected(tmp_path):
    path = _write(
        tmp_path,
        """
        jobs:
          - { name: Dup, type: path, root_path: /a }
          - { name: dup, type: path, root_path: /b }
        """,
    )
    with pytest.raises(ValueError):
        load_file_config(path, DEFAULTS)


async def test_yaml_job_runs_and_snapshots_onto_the_run(eng, media, tmp_path):
    lib = tmp_path / "movies"
    lib.mkdir()
    shutil.copy(media["clean"], lib / "good.mkv")
    shutil.copy(media["bitflip"], lib / "bad.mkv")
    path = _write(
        tmp_path,
        f"""
        jobs:
          - name: Movies
            type: path
            root_path: {lib}
        """,
    )
    spec = load_file_config(path, DEFAULTS).jobs[0]

    orch = Orchestrator(
        Database(eng), InlineExecutor(), CFG, yaml_jobs={spec.slug: spec}, poll_interval=0.02
    )
    await orch.start()
    try:
        run_id = await orch.trigger_run(spec.slug)
        assert await orch.wait_for_run(run_id, timeout=15) == RunStatus.COMPLETED
        with Session(eng) as s:
            run = s.get(JobRun, run_id)
            # the run is self-contained: job identity snapshotted onto it
            assert run.job_slug == "movies" and run.job_name == "Movies"
            assert run.files_corrupt == 1
            assert len(s.exec(select(Detection)).all()) == 1
    finally:
        await orch.stop()


def test_jobs_are_read_only_via_api(tmp_path, monkeypatch):
    db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    path = _write(
        tmp_path,
        """
        settings:
          max_scan_workers: 9
        jobs:
          - { name: Movies, type: path, root_path: /mnt/movies }
        """,
    )
    monkeypatch.setattr(config_module.settings, "config_file", path)

    from scanrr.api.app import app

    with TestClient(app) as client:
        assert client.get("/api/settings").json()["max_scan_workers"] == 9

        jobs = client.get("/api/jobs").json()
        assert len(jobs) == 1 and jobs[0]["slug"] == "movies"

        # no create/update/delete — jobs are defined only in the YAML
        assert client.post("/api/jobs", json={"name": "x"}).status_code in (404, 405)
        assert client.put("/api/jobs/movies", json={}).status_code in (404, 405)
        assert client.delete("/api/jobs/movies").status_code in (404, 405)
