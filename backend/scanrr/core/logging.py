"""Structured JSON logging (SPEC §14a).

`decision(...)` emits one structured record per per-file scan decision so
"why did/didn't this file get scanned?" is answerable after the fact.
"""

from __future__ import annotations

import json
import logging
import sys

from scanrr.core.config import settings

_LOGGER = logging.getLogger("scanrr")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if extra := getattr(record, "extra_fields", None):
            payload.update(extra)
        return json.dumps(payload, default=str)


def configure() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger("scanrr")
    root.handlers[:] = [handler]
    root.setLevel(settings.log_level.upper())
    root.propagate = False


def decision(event: str, **fields: object) -> None:
    """Log a per-file scan decision, e.g. decision('skipped', path=p, reason='ttl')."""
    _LOGGER.info(event, extra={"extra_fields": {"event": event, **fields}})
