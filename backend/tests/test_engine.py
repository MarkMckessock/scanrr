"""Core scan-engine tests (SPEC §3) — the idempotency + correctness matrix.

These are the M1 regression guard: they prove the content-addressed idempotency
rules and remediation behaviour, not just that ffmpeg detects corruption
(that's test_integrity.py).
"""

from __future__ import annotations

import shutil

import pytest
from specs import path_spec
from sqlmodel import Session, select

from scanrr.db import engine as db_engine
from scanrr.db.models import Detection, RunFile, ScanResult
from scanrr.enums import DetectionStatus, Verdict
from scanrr.scanning import engine as scan_engine
from scanrr.scanning import worker
from scanrr.scanning.engine import RuntimeConfig, run_job

# Include tiny fixtures + skip the stability gate for tests.
CFG = RuntimeConfig(min_file_size_bytes=0, min_file_age_seconds=0)


@pytest.fixture
def session(tmp_path) -> Session:
    eng = db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    db_engine.init_db()
    with Session(eng) as s:
        yield s


@pytest.fixture
def decode_counter(monkeypatch) -> list[int]:
    """Count real ffmpeg decodes so we can prove the cache skips them."""
    calls = [0]
    real = worker.decode

    def counting(*args, **kwargs):
        calls[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(worker, "decode", counting)
    return calls


def make_job(session: Session, root, *, ttl_seconds: int = 30 * 86_400):
    return path_spec(root, ttl_seconds=ttl_seconds)


def test_detects_corruption_and_is_idempotent(session, media, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    shutil.copy(media["clean"], lib / "good.mkv")
    shutil.copy(media["bitflip"], lib / "bad.mkv")
    job = make_job(session, lib)

    run1 = run_job(session, job, config=CFG)
    assert run1.files_scanned == 2
    assert run1.files_corrupt == 1
    opens = session.exec(select(Detection).where(Detection.status == DetectionStatus.OPEN)).all()
    assert len(opens) == 1

    # Second run within TTL: everything skips, nothing re-scanned.
    run2 = run_job(session, job, config=CFG)
    assert run2.files_scanned == 0
    assert run2.files_skipped == 2


def test_cross_path_hash_dedup_decodes_once(session, media, tmp_path, decode_counter):
    lib = tmp_path / "lib"
    lib.mkdir()
    shutil.copy(media["bitflip"], lib / "a.mkv")
    shutil.copy(media["bitflip"], lib / "b.mkv")   # identical content, different path
    job = make_job(session, lib)

    run = run_job(session, job, config=CFG)

    assert decode_counter[0] == 1                   # decoded once, cache served the twin
    assert run.files_corrupt == 2                   # both paths flagged
    hashes = session.exec(select(ScanResult.hash)).all()
    assert len(hashes) == 1                          # one content verdict cached


def test_mtime_change_rehashes_but_hits_content_cache(session, media, tmp_path, decode_counter):
    lib = tmp_path / "lib"
    lib.mkdir()
    f = lib / "good.mkv"
    shutil.copy(media["clean"], f)
    job = make_job(session, lib, ttl_seconds=0)     # ttl=0 → always re-enqueue

    run_job(session, job, config=CFG)
    assert decode_counter[0] == 1

    f.touch()  # change mtime; content identical
    run2 = run_job(session, job, config=CFG)
    assert decode_counter[0] == 1                   # cache hit — no second decode
    assert run2.files_scanned == 1
    assert run2.files_corrupt == 0


def test_replacement_autoresolves_detection(session, media, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    target = lib / "ep.mkv"
    shutil.copy(media["bitflip"], target)
    job = make_job(session, lib, ttl_seconds=0)

    run_job(session, job, config=CFG)
    det = session.exec(select(Detection)).one()
    assert det.status == DetectionStatus.OPEN

    shutil.copy(media["clean"], target)             # "replaced" with a clean copy
    run_job(session, job, config=CFG)
    session.refresh(det)
    assert det.status == DetectionStatus.RESOLVED
    assert det.resolved_at is not None


def test_unreadable_is_not_corrupt_and_not_cached(session, media, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    shutil.copy(media["header"], lib / "broken.mkv")   # can't open → transient error
    job = make_job(session, lib)

    run = run_job(session, job, config=CFG)
    assert run.files_unreadable == 1
    assert run.files_corrupt == 0
    assert run.files_scanned == 0
    rf = session.exec(select(RunFile)).one()
    assert rf.outcome == Verdict.UNREADABLE
    assert session.exec(select(ScanResult)).all() == []   # transient never cached


def test_detector_version_bump_invalidates_cache(
    session, media, tmp_path, decode_counter, monkeypatch
):
    lib = tmp_path / "lib"
    lib.mkdir()
    shutil.copy(media["clean"], lib / "good.mkv")
    job = make_job(session, lib, ttl_seconds=0)

    run_job(session, job, config=CFG)
    assert decode_counter[0] == 1

    monkeypatch.setattr(scan_engine.integrity, "DETECTOR_VERSION", 999)
    run_job(session, job, config=CFG)
    assert decode_counter[0] == 2                   # stale cache → re-decoded
