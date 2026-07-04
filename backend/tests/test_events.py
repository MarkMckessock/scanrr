"""EventBus / SSE pub-sub (SPEC §11)."""

from __future__ import annotations

import asyncio

import pytest

from scanrr.core.events import EventBus


async def test_publish_delivers_then_unsubscribe_stops():
    bus = EventBus()
    q = bus.subscribe()
    bus.publish({"type": "x", "n": 1})
    data = await asyncio.wait_for(q.get(), timeout=1)
    assert '"type": "x"' in data

    bus.unsubscribe(q)
    bus.publish({"type": "y"})
    assert q.empty()  # no longer registered


async def test_heartbeat_timeout_keeps_subscription():
    """Regression: a cancelled queue.get() (the 15s SSE heartbeat) must NOT drop
    the subscription — previously it closed the generator and the next read raised
    StopAsyncIteration."""
    bus = EventBus()
    q = bus.subscribe()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.get(), timeout=0.02)  # times out → cancels get()
    bus.publish({"type": "after-heartbeat"})
    data = await asyncio.wait_for(q.get(), timeout=1)
    assert "after-heartbeat" in data
    bus.unsubscribe(q)


def test_slow_consumer_drops_not_blocks():
    bus = EventBus(max_queue=1)
    q = bus.subscribe()
    bus.publish({"n": 1})
    bus.publish({"n": 2})  # queue full → dropped, no raise
    assert q.qsize() == 1
