"""Async wrapper that serializes all DB access onto one dedicated thread (SPEC §5).

This single thread is the sole writer, keeps the event loop unblocked, and makes
`claim_next_pending` atomic for free. Operations receive a live ``Session`` and
must return **plain data** (ids/dataclasses/tuples) — never a live ORM object,
which would be detached once the session closes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

from sqlalchemy.engine import Engine
from sqlmodel import Session

T = TypeVar("T")


class Database:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scanrr-db")

    async def run(self, fn: Callable[[Session], T]) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, self._call, fn)

    def _call(self, fn: Callable[[Session], T]) -> T:
        with Session(self._engine) as session:
            result = fn(session)
            session.commit()
            return result

    def close(self) -> None:
        self._pool.shutdown(wait=True)
