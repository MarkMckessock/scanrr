"""Periodic notification flusher (SPEC §10).

Drains ``notification_queue`` on an interval: per event type, sends individual
pushes when a group is under ``notification_batch_threshold`` else one batched
digest — so a big first scan can't storm Pushover. Sends never block scanning
(they run here, on their own timer). Pushover config comes from the YAML (§0).
"""

from __future__ import annotations

import asyncio
import json
import logging

from scanrr.core.config import RuntimeConfig
from scanrr.core.fileconfig import PushoverConfig
from scanrr.db.database import Database
from scanrr.enums import NotificationEvent, NotificationStatus
from scanrr.integrations.pushover import PushoverClient
from scanrr.scanning import engine

_log = logging.getLogger("scanrr")


def _individual(event: NotificationEvent, p: dict) -> tuple[str, str]:
    if event is NotificationEvent.CORRUPT_FOUND:
        return "Corrupt file found", p.get("path", "")
    if event is NotificationEvent.SCAN_COMPLETED:
        return (
            f"Scan complete: {p.get('job_name', '')}",
            f"{p.get('files_corrupt', 0)} corrupt · {p.get('files_scanned', 0)} scanned",
        )
    if event is NotificationEvent.REPLACEMENT_PENDING_APPROVAL:
        return "Replacement needs approval", f"detection {p.get('detection_id')}"
    if event is NotificationEvent.REPLACEMENT_COMPLETED:
        return "Replacement completed", f"detection {p.get('detection_id')} is clean"
    return event.replace("_", " ").title(), json.dumps(p)


def _batched(event: NotificationEvent, payloads: list[dict]) -> tuple[str, str]:
    if event is NotificationEvent.CORRUPT_FOUND:
        paths = "\n".join(p.get("path", "") for p in payloads[:20])
        return f"{len(payloads)} corrupt files found", paths
    return f"{len(payloads)} × {event.replace('_', ' ')}", ""


class NotificationFlusher:
    def __init__(
        self,
        db: Database,
        config: RuntimeConfig,
        pushover: PushoverConfig | None,
        *,
        interval: float | None = None,
    ) -> None:
        self._db = db
        self._config = config
        self._pushover = pushover
        self._interval = interval if interval is not None else config.notification_flush_interval
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._interval)
                await self.flush_once()
            except asyncio.CancelledError:
                break
            except Exception:  # never let the flusher die
                _log.exception("notification flush failed")

    async def flush_once(self) -> None:
        groups = await self._db.run(engine.pending_notifications)
        if not groups:
            return
        client = (
            PushoverClient(self._pushover.user_key, self._pushover.api_token)
            if self._pushover is not None
            else None
        )
        threshold = self._config.notification_batch_threshold
        try:
            for event, rows in groups.items():
                ids = [r.id for r in rows]
                subscribed = self._pushover is not None and (
                    not self._pushover.events or event in self._pushover.events
                )
                if client is None or not subscribed:
                    await self._mark(ids, NotificationStatus.SENT)  # drain, nothing to send
                    continue
                await self._send_group(client, event, rows, ids, threshold)
        finally:
            if client is not None:
                await client.close()

    async def _send_group(self, client, event, rows, ids, threshold) -> None:
        try:
            if len(rows) < threshold:
                for row in rows:
                    title, message = _individual(event, json.loads(row.payload))
                    await client.send(title, message)
            else:
                title, message = _batched(event, [json.loads(r.payload) for r in rows])
                await client.send(title, message)
            await self._log_and_mark(event, title, len(rows), NotificationStatus.SENT, ids)
        except Exception as exc:
            await self._log_and_mark(event, "", len(rows), NotificationStatus.FAILED, ids, str(exc))

    async def _mark(self, ids: list[int], status: NotificationStatus) -> None:
        def _op(session) -> None:
            engine.mark_notifications(session, ids, status)

        await self._db.run(_op)

    async def _log_and_mark(
        self,
        event: NotificationEvent,
        title: str,
        count: int,
        status: NotificationStatus,
        ids: list[int],
        error: str | None = None,
    ) -> None:
        def _op(session) -> None:
            engine.mark_notifications(session, ids, status)
            engine.log_notification(session, event, title, count, status, error)

        await self._db.run(_op)
