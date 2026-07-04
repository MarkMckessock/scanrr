"""Time helpers. All stored timestamps are ISO-8601 UTC per SPEC §8."""

from __future__ import annotations

from datetime import UTC, datetime


def now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return to_iso(now())


def to_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def age_seconds(iso_value: str) -> float:
    """Seconds elapsed since the given ISO timestamp."""
    return (now() - parse_iso(iso_value)).total_seconds()
