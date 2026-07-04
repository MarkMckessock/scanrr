"""Live per-file decode progress (SPEC §6): frame-loop callback + DB persistence."""

from __future__ import annotations

from sqlmodel import Session

from scanrr.db import engine as dbe
from scanrr.db.models import ScanProgress, ScanTask
from scanrr.enums import TaskStatus
from scanrr.scanning import engine, integrity
from scanrr.scanning.executor import ProgressUpdate
from tests.conftest import requires_ffmpeg


def _eng(tmp_path):
    e = dbe.configure(f"sqlite:///{tmp_path / 'p.db'}")
    dbe.init_db()
    return e


def test_progress_roundtrip_for_scanning_task(tmp_path):
    e = _eng(tmp_path)
    with Session(e) as s:
        t = ScanTask(seq=1, path="/media/a.mkv", status=TaskStatus.SCANNING)
        s.add(t)
        s.commit()
        engine.upsert_progress(s, [ProgressUpdate(t.id, 60.0, 120.0, 1440)])
        s.commit()
        active = engine.active_tasks(s)
        assert len(active) == 1
        assert active[0]["pct"] == 0.5 and active[0]["frames"] == 1440
        engine.clear_progress(s, t.id)
        s.commit()
        assert s.get(ScanProgress, t.id) is None


def test_upsert_does_not_resurrect_finished_task(tmp_path):
    e = _eng(tmp_path)
    with Session(e) as s:
        t = ScanTask(seq=1, path="/media/b.mkv", status=TaskStatus.DONE)
        s.add(t)
        s.commit()
        engine.upsert_progress(s, [ProgressUpdate(t.id, 10.0, 100.0, 100)])
        s.commit()
        assert s.get(ScanProgress, t.id) is None  # DONE task → skipped


def test_unknown_duration_leaves_pct_none(tmp_path):
    e = _eng(tmp_path)
    with Session(e) as s:
        t = ScanTask(seq=1, path="/media/c.mkv", status=TaskStatus.SCANNING)
        s.add(t)
        s.commit()
        engine.upsert_progress(s, [ProgressUpdate(t.id, 5.0, 0.0, 50)])  # duration unknown
        s.commit()
        assert engine.active_tasks(s)[0]["pct"] is None


@requires_ffmpeg
def test_multithreaded_decode_matches_single_thread(media):
    """Threaded decode must produce the SAME verdict as single-threaded — libav
    routes decode-thread errors to the process-global logger we tap (fidelity)."""
    for key in ("clean", "bitflip"):
        single = integrity.check_pyav(str(media[key]), threads=1)
        threaded = integrity.check_pyav(str(media[key]), threads=4)
        assert single.status is threaded.status


@requires_ffmpeg
def test_check_pyav_invokes_progress_callback(media):
    calls: list[tuple[float, float, int]] = []

    def rec(p: float, d: float, f: int) -> None:
        calls.append((p, d, f))

    out = integrity.check_pyav(str(media["clean"]), on_progress=rec)
    assert out.frames_decoded > 0
    assert calls and calls[-1][2] == out.frames_decoded  # final callback = total frames
