"""Constrained-value types. Always use these enums — never bare strings — for
fields with a fixed set of valid values (see CLAUDE.md).

`StrEnum` members ARE their string value, so they JSON-encode and compare against
strings transparently; for SQLModel columns pair them with ``db.columns.enum_col``
so the stored value is the enum *value* (e.g. ``"pending"``), not its name.
"""

from __future__ import annotations

from enum import StrEnum


class JobType(StrEnum):
    PATH = "path"
    ARR = "arr"


class RunTrigger(StrEnum):
    MANUAL = "manual"
    SCHEDULED = "scheduled"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class TaskStatus(StrEnum):
    PENDING = "pending"
    SCANNING = "scanning"
    DONE = "done"
    UNREADABLE = "unreadable"


class Disposition(StrEnum):
    QUEUED = "queued"
    SKIPPED_TTL = "skipped_ttl"
    SKIPPED_TOO_FRESH = "skipped_too_fresh"


class Verdict(StrEnum):
    """Terminal per-file outcome surfaced to runs. `scan_results` caches only
    OK / CORRUPT (UNREADABLE is transient-exhausted, never cached)."""

    OK = "ok"
    CORRUPT = "corrupt"
    UNREADABLE = "unreadable"


class DetectionStatus(StrEnum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    IGNORED = "ignored"
    NEEDS_ATTENTION = "needs_attention"


class HashAlgorithm(StrEnum):
    BLAKE3 = "blake3"
    SHA256 = "sha256"


class DetectorBackend(StrEnum):
    PYAV = "pyav"
    SUBPROCESS = "subprocess"


class DetectorStatus(StrEnum):
    """Raw detector outcome (SPEC §7). ERROR = couldn't open → treated as a
    transient failure by the engine, never a cached verdict."""

    OK = "ok"
    CORRUPT = "corrupt"
    ERROR = "error"


class ArrType(StrEnum):
    SONARR = "sonarr"
    RADARR = "radarr"


class MediaType(StrEnum):
    EPISODE = "episode"
    MOVIE = "movie"


class NotificationEvent(StrEnum):
    SCAN_STARTED = "scan_started"
    SCAN_COMPLETED = "scan_completed"
    CORRUPT_FOUND = "corrupt_found"
    REPLACEMENT_PENDING_APPROVAL = "replacement_pending_approval"
    REPLACEMENT_REQUESTED = "replacement_requested"
    REPLACEMENT_COMPLETED = "replacement_completed"
    JOB_FAILED = "job_failed"


class NotificationStatus(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class ReplacementStatus(StrEnum):
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    REQUESTED = "requested"
    SEARCHING = "searching"
    GRABBED = "grabbed"
    IMPORTED = "imported"
    VERIFYING = "verifying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    EXHAUSTED = "exhausted"
    ABORTED = "aborted"
