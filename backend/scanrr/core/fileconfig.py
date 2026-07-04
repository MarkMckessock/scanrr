"""YAML file config (infrastructure-as-code) — the sole source of truth for jobs.

Jobs are defined here and held **in memory** (never in the DB); the YAML is
authoritative and read-only in the app. Each job is referenced by a deterministic
`slug` derived from its name. A run snapshots its job's properties (see
``JobRun``), so runs stay valid even if a job is later removed from the YAML.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass

import yaml

from scanrr.core.config import RuntimeConfig
from scanrr.enums import JobType


@dataclass(frozen=True)
class JobSpec:
    """A job definition loaded from the YAML config (or a transient CLI job)."""

    slug: str
    name: str
    type: JobType
    config: str  # JSON: {"root_path": ...} | {"arr_instance_id": ...}
    ttl_seconds: int
    schedule_cron: str | None
    enabled: bool
    auto_replace: bool


def slugify(name: str) -> str:
    """Deterministic URL/id-safe slug from a job name."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "job"


def _job_spec_from_yaml(entry: dict) -> JobSpec:
    name = entry["name"]
    job_type = JobType(entry.get("type", "path"))
    if job_type is JobType.PATH:
        config = {"root_path": entry["root_path"]}
    else:
        config = {"arr_instance_id": int(entry["arr_instance_id"])}
    return JobSpec(
        slug=slugify(name),
        name=name,
        type=job_type,
        config=json.dumps(config),
        ttl_seconds=int(entry.get("ttl_days", 30)) * 86_400,
        schedule_cron=entry.get("schedule_cron"),
        enabled=bool(entry.get("enabled", True)),
        auto_replace=bool(entry.get("auto_replace", False)),
    )


def load_file_config(path: str, base: RuntimeConfig) -> tuple[RuntimeConfig, list[JobSpec]]:
    """Return (effective runtime config, YAML job specs). Missing file → (base, [])."""
    if not path or not os.path.exists(path):
        return base, []

    with open(path) as fh:
        data = yaml.safe_load(fh) or {}

    overrides = data.get("settings") or {}
    effective = RuntimeConfig(**{**base.model_dump(), **overrides})

    specs = [_job_spec_from_yaml(entry) for entry in (data.get("jobs") or [])]
    slugs = [s.slug for s in specs]
    if len(set(slugs)) != len(slugs):
        raise ValueError("duplicate job slug in YAML config (job names must be distinct)")
    return effective, specs
