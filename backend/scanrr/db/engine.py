"""SQLite engine + session (SPEC §5, §8).

WAL mode, busy_timeout, FK enforcement. M1 creates the schema from SQLModel
metadata plus a little raw DDL for constraints SQLModel can't express (the
partial unique dedup index, detections' composite unique). Alembic baseline
migrations come once the schema stabilises (M2).
"""

from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine

from scanrr.core.config import settings

_engine: Engine | None = None

# Raw DDL for what SQLModel field options can't express (SPEC §8).
_EXTRA_DDL = [
    # Path dedup: at most one ACTIVE task per path (done tasks are not dedup targets).
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_scan_tasks_active_path "
    "ON scan_tasks(path) WHERE status IN ('pending','scanning')",
    "CREATE INDEX IF NOT EXISTS ix_scan_tasks_drain ON scan_tasks(status, seq)",
    # A detection is unique per (file, bad-content-hash).
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_detections_file_hash "
    "ON detections(file_id, hash)",
    "CREATE INDEX IF NOT EXISTS ix_detections_status ON detections(status)",
    "CREATE INDEX IF NOT EXISTS ix_scan_task_subs_run "
    "ON scan_task_subscribers(job_run_id)",
    # arr reverse lookup: history/webhook references a file by its arr file id.
    "CREATE INDEX IF NOT EXISTS ix_file_arr_links_arrfile "
    "ON file_arr_links(arr_instance, arr_file_id)",
    "CREATE INDEX IF NOT EXISTS ix_replacements_status ON replacements(status)",
]


def _set_sqlite_pragmas(dbapi_conn, _rec) -> None:
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA foreign_keys=ON")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.close()


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        return configure(settings.database_url)
    return _engine


def configure(url: str) -> Engine:
    """(Re)bind the global engine to ``url``. Used at startup and in tests."""
    global _engine
    _engine = create_engine(url, echo=False)
    event.listen(_engine, "connect", _set_sqlite_pragmas)
    return _engine


def init_db() -> None:
    """Create tables + extra constraints. Idempotent."""
    import scanrr.db.models  # noqa: F401  (register tables on the metadata)

    engine = get_engine()
    SQLModel.metadata.create_all(engine)
    with engine.begin() as conn:
        for ddl in _EXTRA_DDL:
            conn.execute(text(ddl))


def get_session() -> Iterator[Session]:
    with Session(get_engine()) as session:
        yield session
