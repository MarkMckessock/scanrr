"""M5 replacement execution (SPEC §9): propose → approve → delete+search →
verify re-scan → resolve / retry / exhaust."""

from __future__ import annotations

import shutil

import pytest
from sqlmodel import Session, select

from scanrr.core.config import RuntimeConfig
from scanrr.core.fileconfig import ArrInstanceSpec
from scanrr.db import engine as db_engine
from scanrr.db.database import Database
from scanrr.db.models import Detection, File, FileArrLink, Replacement
from scanrr.enums import ArrType, DetectionStatus, MediaType, ReplacementStatus
from scanrr.scanning import engine
from scanrr.scanning.executor import InlineExecutor
from scanrr.scanning.replacer import ReplacementExecutor

INSTANCE = ArrInstanceSpec(
    name="main", type=ArrType.RADARR, url="http://r", api_key="k", mappings=()
)
CFG = RuntimeConfig(max_replace_attempts=2, replacement_search_timeout=99999)


@pytest.fixture
def eng(tmp_path):
    e = db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    db_engine.init_db()
    return e


class FakeArr:
    """Stand-in arr client — records nothing, always reports 'imported'."""

    imported_value = True

    def __init__(self, *a, **k):
        pass

    async def delete_file(self, media_type, arr_file_id):
        return None

    async def search(self, media_type, media_id):
        return None

    async def imported(self, media_type, media_id):
        return FakeArr.imported_value

    async def close(self):
        return None


def _seed(eng, path: str, *, content_hash: str = "h") -> int:
    """A corrupt file at `path` linked to arr; returns the detection id."""
    with Session(eng) as s:
        f = File(path=path, hash=content_hash)
        s.add(f)
        s.flush()
        det = Detection(file_id=f.id, hash=content_hash)
        s.add(det)
        s.flush()
        s.add(
            FileArrLink(
                file_id=f.id, arr_instance="main", media_type=MediaType.MOVIE,
                media_id=5, arr_file_id=50,
            )
        )
        s.commit()
        assert det.id is not None
        return det.id


def _approved(eng, det_id: int) -> int:
    with Session(eng) as s:
        det = s.get(Detection, det_id)
        link = s.exec(select(FileArrLink)).one()
        r = engine._new_replacement(s, det, link, approved_by="auto")
        s.commit()
        assert r.id is not None
        return r.id


# --- proposal + approve/reject ---------------------------------------------- #


def test_manual_propose_is_idempotent_then_approve(eng, tmp_path):
    det_id = _seed(eng, str(tmp_path / "bad.mkv"))
    with Session(eng) as s:
        d = engine.create_replacement(s, det_id)
        s.commit()
        assert d is not None and d["status"] == ReplacementStatus.PENDING_APPROVAL
        assert engine.create_replacement(s, det_id)["id"] == d["id"]  # no duplicate
        r = engine.approve_replacement(s, d["id"])
        s.commit()
        assert r["status"] == ReplacementStatus.APPROVED and r["approved_by"] == "user"


def test_reject(eng, tmp_path):
    det_id = _seed(eng, str(tmp_path / "x.mkv"))
    with Session(eng) as s:
        d = engine.create_replacement(s, det_id)
        s.commit()
        assert engine.reject_replacement(s, d["id"])["status"] == ReplacementStatus.REJECTED


def test_no_arr_link_cannot_be_replaced(eng):
    with Session(eng) as s:
        f = File(path="/no-link.mkv", hash="h")
        s.add(f)
        s.flush()
        det = Detection(file_id=f.id, hash="h")
        s.add(det)
        s.commit()
        assert engine.create_replacement(s, det.id) is None


# --- executor loop ---------------------------------------------------------- #


async def test_executor_verify_clean_resolves(eng, media, tmp_path, monkeypatch):
    path = tmp_path / "movie.mkv"
    shutil.copy(media["clean"], path)  # arr re-imported a CLEAN file
    det_id = _seed(eng, str(path))
    repl_id = _approved(eng, det_id)
    monkeypatch.setattr("scanrr.scanning.replacer.make_client", lambda *a, **k: FakeArr())

    ex = ReplacementExecutor(Database(eng), CFG, {"main": INSTANCE}, InlineExecutor(), interval=999)
    await ex.reconcile_once()  # execute → requested → imported → verify(clean) → succeeded

    with Session(eng) as s:
        assert s.get(Replacement, repl_id).status is ReplacementStatus.SUCCEEDED
        assert s.get(Detection, det_id).status is DetectionStatus.RESOLVED


async def test_executor_still_corrupt_retries_then_exhausts(eng, media, tmp_path, monkeypatch):
    path = tmp_path / "movie.mkv"
    shutil.copy(media["bitflip"], path)  # re-import is still corrupt
    det_id = _seed(eng, str(path))
    repl_id = _approved(eng, det_id)
    monkeypatch.setattr("scanrr.scanning.replacer.make_client", lambda *a, **k: FakeArr())
    ex = ReplacementExecutor(Database(eng), CFG, {"main": INSTANCE}, InlineExecutor(), interval=999)

    await ex.reconcile_once()  # attempt 1 → still corrupt → retry (attempt 2, re-approved)
    with Session(eng) as s:
        r = s.get(Replacement, repl_id)
        assert r.attempt == 2 and r.status is ReplacementStatus.APPROVED

    await ex.reconcile_once()  # attempt 2 → still corrupt → exhausted
    with Session(eng) as s:
        assert s.get(Replacement, repl_id).status is ReplacementStatus.EXHAUSTED
        assert s.get(Detection, det_id).status is DetectionStatus.NEEDS_ATTENTION


async def test_auto_replace_scan_proposes_pending(eng, media, tmp_path, monkeypatch):
    """A corrupt file found by an auto_replace arr job proposes a replacement (§9)."""
    import json

    from scanrr.core.fileconfig import JobSpec
    from scanrr.enums import JobType
    from scanrr.integrations import arr as arrmod
    from scanrr.scanning.orchestrator import Orchestrator

    lib = tmp_path / "movies"
    lib.mkdir()
    shutil.copy(media["bitflip"], lib / "bad.mkv")
    spec = JobSpec(
        slug="movies", name="Movies", type=JobType.ARR,
        config=json.dumps({"arr_instance": "main"}), ttl_seconds=0,
        schedule_cron=None, enabled=True, auto_replace=True,
    )

    class DiscoverClient:
        async def list_media_files(self):
            return [arrmod.ArrFile("/data/movies/bad.mkv", MediaType.MOVIE, 5, 50)]

        async def close(self):
            return None

    monkeypatch.setattr(
        "scanrr.scanning.orchestrator.make_client", lambda *a, **k: DiscoverClient()
    )
    inst = ArrInstanceSpec(
        name="main", type=ArrType.RADARR, url="x", api_key="k",
        mappings=(("/data/movies", str(lib)),),
    )
    orch = Orchestrator(
        Database(eng),
        InlineExecutor(),
        RuntimeConfig(min_file_size_bytes=0, min_file_age_seconds=0),
        yaml_jobs={"movies": spec},
        arr_instances={"main": inst},
        poll_interval=0.02,
    )
    await orch.start()
    try:
        run_id = await orch.trigger_run("movies")
        await orch.wait_for_run(run_id, timeout=15)
        with Session(eng) as s:
            r = s.exec(select(Replacement)).one()
            assert r.status is ReplacementStatus.PENDING_APPROVAL and r.arr_file_id == 50
    finally:
        await orch.stop()
