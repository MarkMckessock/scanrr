"""Test helpers to build in-memory JobSpecs (jobs are YAML-defined, not in the DB)."""

from __future__ import annotations

import json

from scanrr.core.fileconfig import JobSpec, slugify
from scanrr.enums import JobType


def path_spec(root, *, name: str = "Test", ttl_seconds: int = 0) -> JobSpec:
    return JobSpec(
        slug=slugify(name),
        name=name,
        type=JobType.PATH,
        config=json.dumps({"root_path": str(root)}),
        ttl_seconds=ttl_seconds,
        schedule_cron=None,
        enabled=True,
        auto_replace=False,
    )


def arr_spec(arr_instance: str, *, name: str = "Arr", ttl_seconds: int = 0) -> JobSpec:
    return JobSpec(
        slug=slugify(name),
        name=name,
        type=JobType.ARR,
        config=json.dumps({"arr_instance": arr_instance}),
        ttl_seconds=ttl_seconds,
        schedule_cron=None,
        enabled=True,
        auto_replace=False,
    )
