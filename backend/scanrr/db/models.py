"""SQLModel table definitions — core M1 subset of SPEC §8.

Arr / replacement / notification tables are added in M4/M5. Timestamps are ISO-8601
UTC strings (SPEC §8). Constrained-value columns use enums via ``enum_col`` so the
stored value is the enum value, never a bare string (see CLAUDE.md).
"""

from __future__ import annotations

from sqlalchemy import Column, ForeignKey, Integer
from sqlmodel import Field, SQLModel

from scanrr.core import clock
from scanrr.db.columns import enum_col
from scanrr.enums import (
    ArrType,
    DetectionStatus,
    DetectorBackend,
    Disposition,
    JobType,
    MediaType,
    ReplacementStatus,
    RunStatus,
    RunTrigger,
    TaskStatus,
    Verdict,
)


class Setting(SQLModel, table=True):
    __tablename__ = "settings"
    key: str = Field(primary_key=True)
    value: str  # JSON-encoded
    updated_at: str = Field(default_factory=clock.iso_now)


class JobRun(SQLModel, table=True):
    """A run (job instance). Jobs live only in the YAML registry (no `jobs` table),
    so the run snapshots everything needed for execution/display — it stays valid
    even if the job is later removed from the config."""

    __tablename__ = "job_runs"
    id: int | None = Field(default=None, primary_key=True)
    job_slug: str = Field(index=True)  # deterministic job identifier (not an FK)
    job_name: str = ""
    job_type: JobType = Field(default=JobType.PATH, sa_column=enum_col(JobType))
    job_config: str = "{}"  # JSON snapshot: {"root_path": ...} | {"arr_instance_id": ...}
    ttl_seconds: int = 0
    auto_replace: bool = False
    status: RunStatus = Field(default=RunStatus.QUEUED, sa_column=enum_col(RunStatus))
    trigger: RunTrigger = Field(default=RunTrigger.MANUAL, sa_column=enum_col(RunTrigger))
    started_at: str | None = None
    finished_at: str | None = None
    files_discovered: int = 0
    files_scanned: int = 0
    files_skipped: int = 0
    files_corrupt: int = 0
    files_unreadable: int = 0
    error_message: str | None = None


class ScanTask(SQLModel, table=True):
    __tablename__ = "scan_tasks"
    id: int | None = Field(default=None, primary_key=True)
    seq: int = Field(index=True)
    path: str
    status: TaskStatus = Field(default=TaskStatus.PENDING, sa_column=enum_col(TaskStatus))
    content_hash: str | None = None   # blake3, computed by the worker (Phase B)
    result_status: Verdict | None = Field(
        default=None, sa_column=enum_col(Verdict, nullable=True)
    )
    attempts: int = 0
    next_attempt_at: str | None = None
    error: str | None = None
    created_at: str = Field(default_factory=clock.iso_now)
    updated_at: str = Field(default_factory=clock.iso_now)


class ScanTaskSubscriber(SQLModel, table=True):
    __tablename__ = "scan_task_subscribers"
    scan_task_id: int = Field(foreign_key="scan_tasks.id", primary_key=True)
    job_run_id: int = Field(foreign_key="job_runs.id", primary_key=True)


class RunFile(SQLModel, table=True):
    __tablename__ = "run_files"
    job_run_id: int = Field(foreign_key="job_runs.id", primary_key=True)
    path: str = Field(primary_key=True)
    disposition: Disposition = Field(sa_column=enum_col(Disposition))
    outcome: Verdict | None = Field(default=None, sa_column=enum_col(Verdict, nullable=True))
    # SET NULL so an orphaned task can be dropped on cancel without orphaning this row.
    scan_task_id: int | None = Field(
        default=None,
        sa_column=Column(Integer, ForeignKey("scan_tasks.id", ondelete="SET NULL"), nullable=True),
    )


class File(SQLModel, table=True):
    __tablename__ = "files"
    id: int | None = Field(default=None, primary_key=True)
    path: str = Field(unique=True, index=True)
    hash: str | None = Field(default=None, index=True)
    size_bytes: int | None = None
    mtime: float | None = None
    first_seen_at: str = Field(default_factory=clock.iso_now)
    last_seen_at: str = Field(default_factory=clock.iso_now)
    last_scanned_at: str | None = None


class ScanResult(SQLModel, table=True):
    __tablename__ = "scan_results"
    hash: str = Field(primary_key=True)
    status: Verdict = Field(sa_column=enum_col(Verdict))  # only OK|CORRUPT cached
    error_log: str | None = None
    detector_version: int = 0
    detector_backend: DetectorBackend = Field(
        default=DetectorBackend.PYAV, sa_column=enum_col(DetectorBackend)
    )
    scan_duration_ms: int | None = None
    scanned_at: str = Field(default_factory=clock.iso_now)


class Detection(SQLModel, table=True):
    __tablename__ = "detections"
    id: int | None = Field(default=None, primary_key=True)
    file_id: int = Field(foreign_key="files.id", index=True)
    hash: str
    job_run_id: int | None = Field(default=None, foreign_key="job_runs.id")
    status: DetectionStatus = Field(
        default=DetectionStatus.OPEN, sa_column=enum_col(DetectionStatus)
    )
    detected_at: str = Field(default_factory=clock.iso_now)
    resolved_at: str | None = None


# --- Sonarr / Radarr integration (SPEC §8, §9) ------------------------------ #


class ArrInstance(SQLModel, table=True):
    __tablename__ = "arr_instances"
    id: int | None = Field(default=None, primary_key=True)
    type: ArrType = Field(sa_column=enum_col(ArrType))
    name: str
    base_url: str
    api_key: str  # encrypted at rest (scanrr.core.crypto)
    enabled: bool = True
    created_at: str = Field(default_factory=clock.iso_now)


class PathMapping(SQLModel, table=True):
    __tablename__ = "path_mappings"
    id: int | None = Field(default=None, primary_key=True)
    arr_instance_id: int = Field(foreign_key="arr_instances.id", index=True)
    remote_path: str  # arr's namespace, e.g. /data/media/tv
    local_path: str  # scanrr's mount, e.g. /mnt/tv


class FileArrLink(SQLModel, table=True):
    __tablename__ = "file_arr_links"
    file_id: int = Field(foreign_key="files.id", primary_key=True)
    arr_instance_id: int = Field(foreign_key="arr_instances.id", primary_key=True)
    media_type: MediaType = Field(sa_column=enum_col(MediaType))
    media_id: int  # series/episode or movie id
    arr_file_id: int  # episodeFile / movieFile id


class Replacement(SQLModel, table=True):
    __tablename__ = "replacements"
    id: int | None = Field(default=None, primary_key=True)
    detection_id: int = Field(foreign_key="detections.id", index=True)
    attempt: int = 1
    arr_instance_id: int | None = Field(default=None, foreign_key="arr_instances.id")
    media_type: MediaType | None = Field(default=None, sa_column=enum_col(MediaType, nullable=True))
    media_id: int | None = None
    arr_file_id: int | None = None
    status: ReplacementStatus = Field(
        default=ReplacementStatus.PENDING_APPROVAL, sa_column=enum_col(ReplacementStatus)
    )
    approved_by: str | None = None
    approved_at: str | None = None
    requested_at: str | None = None
    updated_at: str = Field(default_factory=clock.iso_now)
    notes: str | None = None
