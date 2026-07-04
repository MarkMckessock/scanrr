"""M5 notifications (SPEC §10): enqueue → periodic flush → Pushover, with
threshold-based individual/batched sending."""

from __future__ import annotations

import pytest
from sqlmodel import Session, select

from scanrr.core.config import RuntimeConfig
from scanrr.core.fileconfig import PushoverConfig
from scanrr.db import engine as db_engine
from scanrr.db.database import Database
from scanrr.db.models import NotificationLog, NotificationQueue
from scanrr.enums import NotificationEvent, NotificationStatus
from scanrr.scanning import engine
from scanrr.scanning.notifier import NotificationFlusher


@pytest.fixture
def eng(tmp_path):
    e = db_engine.configure(f"sqlite:///{tmp_path / 'scanrr.db'}")
    db_engine.init_db()
    return e


@pytest.fixture
def sent(monkeypatch) -> list[tuple[str, str]]:
    """Capture Pushover sends instead of hitting the network."""
    captured: list[tuple[str, str]] = []

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def send(self, title, message, *, priority=0):
            captured.append((title, message))

        async def close(self):
            return None

    monkeypatch.setattr("scanrr.scanning.notifier.PushoverClient", FakeClient)
    return captured


def _enqueue(eng, event, *payloads):
    with Session(eng) as s:
        for i, p in enumerate(payloads):
            engine.enqueue_notification(s, event, p, dedup_key=f"{event}:{i}")
        s.commit()


PUSHOVER = PushoverConfig(user_key="u", api_token="t", events=frozenset())
CORRUPT = NotificationEvent.CORRUPT_FOUND
CFG5 = RuntimeConfig(notification_batch_threshold=5)


def test_dedup_key_collapses_duplicates(eng):
    with Session(eng) as s:
        engine.enqueue_notification(s, CORRUPT, {"path": "/a"}, dedup_key="/a")
        engine.enqueue_notification(s, CORRUPT, {"path": "/a"}, dedup_key="/a")
        s.commit()
        assert len(s.exec(select(NotificationQueue)).all()) == 1


async def test_flush_sends_individual_under_threshold(eng, sent):
    _enqueue(eng, CORRUPT, {"path": "/a.mkv"}, {"path": "/b.mkv"})
    flusher = NotificationFlusher(Database(eng), CFG5, PUSHOVER)
    await flusher.flush_once()
    assert len(sent) == 2  # one push per event
    with Session(eng) as s:
        assert all(n.status is NotificationStatus.SENT for n in s.exec(select(NotificationQueue)))
        assert s.exec(select(NotificationLog)).one().status is NotificationStatus.SENT


async def test_flush_batches_at_threshold(eng, sent):
    _enqueue(eng, CORRUPT, *({"path": f"/{i}.mkv"} for i in range(6)))
    flusher = NotificationFlusher(Database(eng), CFG5, PUSHOVER)
    await flusher.flush_once()
    assert len(sent) == 1  # one batched digest
    assert "6 corrupt files" in sent[0][0]


async def test_no_pushover_drains_queue(eng, sent):
    _enqueue(eng, NotificationEvent.CORRUPT_FOUND, {"path": "/a.mkv"})
    flusher = NotificationFlusher(Database(eng), RuntimeConfig(), pushover=None)
    await flusher.flush_once()
    assert sent == []  # nothing sent, but queue is drained (not stuck pending)
    with Session(eng) as s:
        assert all(n.status is NotificationStatus.SENT for n in s.exec(select(NotificationQueue)))


async def test_unsubscribed_event_is_skipped(eng, sent):
    _enqueue(eng, NotificationEvent.SCAN_COMPLETED, {"job_name": "X", "files_corrupt": 0})
    pushover = PushoverConfig("u", "t", frozenset({NotificationEvent.CORRUPT_FOUND}))
    flusher = NotificationFlusher(Database(eng), RuntimeConfig(), pushover)
    await flusher.flush_once()
    assert sent == []  # scan_completed not in the subscribed set
