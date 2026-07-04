"""In-process pub/sub for Server-Sent Events (SPEC §11).

The orchestrator ``publish()``es small dicts from the event loop; SSE clients each
get a bounded queue. A slow client drops events rather than blocking producers —
the UI reconciles via a refetch on reconnect.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator


class EventBus:
    def __init__(self, max_queue: int = 1000) -> None:
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._max_queue = max_queue

    def publish(self, event: dict) -> None:
        data = json.dumps(event, default=str)
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:
                pass  # slow consumer — drop; it will refetch

    async def subscribe(self) -> AsyncIterator[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=self._max_queue)
        self._subscribers.add(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            self._subscribers.discard(queue)
