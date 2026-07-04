"""M4 tests: path mapping, arr clients (mocked HTTP), arr-job discovery + linking,
and the manual-replace proposal (SPEC §9)."""

from __future__ import annotations

import shutil

import httpx
import pytest
from sqlmodel import Session, select

from scanrr.core.config import RuntimeConfig
from scanrr.db import engine as db_engine
from scanrr.db.database import Database
from scanrr.db.models import Detection, FileArrLink, Replacement
from scanrr.enums import ArrType, MediaType, ReplacementStatus, RunStatus
from scanrr.integrations import arr
from scanrr.integrations.arr import RadarrClient, SonarrClient, apply_path_mapping
from scanrr.scanning import engine
from scanrr.scanning.executor import InlineExecutor
from scanrr.scanning.orchestrator import Orchestrator

CFG = RuntimeConfig(min_file_size_bytes=0, min_file_age_seconds=0)


@pytest.fixture
def eng(tmp_path):
    e = db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    db_engine.init_db()
    return e


# --- path mapping ----------------------------------------------------------- #


def test_apply_path_mapping_longest_prefix():
    maps = [("/data", "/mnt"), ("/data/movies", "/mnt/movies")]
    assert apply_path_mapping(maps, "/data/movies/x.mkv") == "/mnt/movies/x.mkv"
    assert apply_path_mapping(maps, "/data/tv/y.mkv") == "/mnt/tv/y.mkv"
    assert apply_path_mapping(maps, "/other/z.mkv") is None
    assert apply_path_mapping([("/data/", "/mnt/")], "/data/a.mkv") == "/mnt/a.mkv"


# --- arr clients over mocked HTTP ------------------------------------------- #


def _mock(routes: dict[str, object]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Api-Key"] == "secret"
        return httpx.Response(200, json=routes[request.url.path])

    return httpx.MockTransport(handler)


async def test_sonarr_enumerates_episode_files():
    transport = _mock(
        {
            "/api/v3/series": [{"id": 7}],
            "/api/v3/episodefile": [{"id": 100, "path": "/data/tv/S01E01.mkv"}],
        }
    )
    client = SonarrClient("http://sonarr", "secret", transport=transport)
    files = await client.list_media_files()
    await client.close()
    assert len(files) == 1
    assert files[0].remote_path == "/data/tv/S01E01.mkv"
    assert files[0].media_type is MediaType.EPISODE
    assert files[0].media_id == 7 and files[0].arr_file_id == 100


async def test_radarr_enumerates_movie_files():
    transport = _mock(
        {"/api/v3/movie": [{"id": 3, "movieFile": {"id": 55, "path": "/data/movies/m.mkv"}}]}
    )
    client = RadarrClient("http://radarr", "secret", transport=transport)
    files = await client.list_media_files()
    await client.close()
    assert len(files) == 1
    assert files[0].media_type is MediaType.MOVIE
    assert files[0].media_id == 3 and files[0].arr_file_id == 55


# --- encryption round-trip -------------------------------------------------- #


def test_arr_instance_api_key_encrypted(eng):
    from scanrr.core import crypto
    from scanrr.db.models import ArrInstance

    with Session(eng) as s:
        engine.create_arr_instance(
            s, type=ArrType.RADARR, name="R", base_url="http://r", api_key="topsecret"
        )
        s.commit()
        stored = s.exec(select(ArrInstance)).one()
        assert stored.api_key != "topsecret"  # encrypted at rest
        assert crypto.decrypt(stored.api_key) == "topsecret"


# --- arr-job discovery + linking (full run) --------------------------------- #


async def test_arr_job_discovers_maps_and_links(eng, media, tmp_path, monkeypatch):
    lib = tmp_path / "movies"
    lib.mkdir()
    shutil.copy(media["clean"], lib / "good.mkv")
    shutil.copy(media["bitflip"], lib / "bad.mkv")

    with Session(eng) as s:
        inst = engine.create_arr_instance(
            s, type=ArrType.RADARR, name="R", base_url="http://r", api_key="k"
        )
        engine.create_path_mapping(
            s, arr_instance_id=inst["id"], remote_path="/data/movies", local_path=str(lib)
        )
        job = engine.create_arr_job(
            s, name="Movies", arr_instance_id=inst["id"], ttl_seconds=0, schedule_cron=None
        )
        s.commit()
        job_id = job["id"]

    class FakeClient:
        async def list_media_files(self):
            return [
                arr.ArrFile("/data/movies/good.mkv", MediaType.MOVIE, 1, 10),
                arr.ArrFile("/data/movies/bad.mkv", MediaType.MOVIE, 2, 20),
            ]

        async def close(self):
            return None

    monkeypatch.setattr(
        "scanrr.scanning.orchestrator.make_client", lambda *a, **k: FakeClient()
    )

    orch = Orchestrator(Database(eng), InlineExecutor(), CFG, poll_interval=0.02)
    await orch.start()
    try:
        run_id = await orch.trigger_run(job_id)
        assert await orch.wait_for_run(run_id, timeout=15) == RunStatus.COMPLETED
        with Session(eng) as s:
            links = s.exec(select(FileArrLink)).all()
            assert {link.arr_file_id for link in links} == {10, 20}
            det = s.exec(select(Detection)).one()  # exactly one corrupt file
            # manual replace proposal for the corrupt file
            repl_dict = engine.create_replacement(s, det.id)
            s.commit()
            assert repl_dict is not None
            repl = s.exec(select(Replacement)).one()
            assert repl.status is ReplacementStatus.PENDING_APPROVAL
            assert repl.arr_file_id == 20  # the corrupt file's arr id
    finally:
        await orch.stop()
