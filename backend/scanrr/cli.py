"""scanrr CLI (SPEC §18 M1 — CLI-testable core)."""

from __future__ import annotations

import json

import typer
from sqlmodel import Session, select

from scanrr.core.config import RuntimeConfig
from scanrr.core.fileconfig import JobSpec, slugify
from scanrr.core.logging import configure as configure_logging
from scanrr.db.engine import get_engine, init_db
from scanrr.db.models import Detection, File
from scanrr.enums import DetectionStatus, JobType
from scanrr.scanning.engine import run_job

app = typer.Typer(help="scanrr — scan a media library for corrupt video files")


@app.callback()
def _main() -> None:
    """scanrr CLI."""  # presence of a callback enables subcommands


@app.command()
def serve(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the API + orchestrator + scheduler service."""
    import uvicorn

    uvicorn.run("scanrr.api.app:app", host=host, port=port)


@app.command()
def scan(
    path: str,
    ttl_days: int = typer.Option(30, help="Skip files scanned within this window"),
    min_age: int = typer.Option(
        120, help="Skip files younger than this many seconds (stability gate)"
    ),
    min_size: int = typer.Option(1_000_000, help="Ignore files smaller than this many bytes"),
) -> None:
    """Scan a directory tree for corrupt media."""
    configure_logging()
    init_db()
    spec = JobSpec(
        slug=f"cli-{slugify(path)}",
        name=f"cli:{path}",
        type=JobType.PATH,
        config=json.dumps({"root_path": path}),
        ttl_seconds=ttl_days * 86_400,
        schedule_cron=None,
        enabled=True,
        auto_replace=False,
    )
    with Session(get_engine()) as session:
        run = run_job(
            session,
            spec,
            config=RuntimeConfig(min_file_age_seconds=min_age, min_file_size_bytes=min_size),
        )

        typer.echo(
            f"run {run.id}: discovered={run.files_discovered} scanned={run.files_scanned} "
            f"skipped={run.files_skipped} corrupt={run.files_corrupt} "
            f"unreadable={run.files_unreadable}"
        )
        open_dets = session.exec(
            select(Detection).where(Detection.status == DetectionStatus.OPEN)
        ).all()
        if open_dets:
            typer.echo("\nCorrupt files:")
            for det in open_dets:
                f = session.get(File, det.file_id)
                typer.echo(f"  [{det.hash[:12]}] {f.path if f else '?'}")


if __name__ == "__main__":
    app()
