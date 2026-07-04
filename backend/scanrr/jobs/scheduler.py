"""Cron scheduling of jobs (SPEC §6 #14). Coalesced, non-overlapping."""

from __future__ import annotations

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from scanrr.core.config import RuntimeConfig
from scanrr.db.database import Database
from scanrr.enums import RunTrigger
from scanrr.scanning import engine
from scanrr.scanning.orchestrator import Orchestrator

_log = logging.getLogger("scanrr")


class Scheduler:
    def __init__(self, orchestrator: Orchestrator, db: Database, config: RuntimeConfig) -> None:
        self._orch = orchestrator
        self._db = db
        self._config = config
        self._sched = AsyncIOScheduler()

    async def start(self) -> None:
        for job_id, cron in await self._db.run(engine.scheduled_jobs):
            self._sched.add_job(
                self._trigger,
                CronTrigger.from_crontab(cron),
                args=[job_id],
                id=f"job-{job_id}",
                coalesce=True,
                max_instances=1,
                misfire_grace_time=self._config.misfire_grace_time,
                replace_existing=True,
            )
        self._sched.start()

    async def _trigger(self, job_id: int) -> None:
        # Skip if the previous run is still active (don't stack runs of one job).
        if await self._db.run(lambda s: engine.job_has_active_run(s, job_id)):
            _log.info("skipping scheduled job %d — previous run still active", job_id)
            return
        await self._orch.trigger_run(job_id, RunTrigger.SCHEDULED)

    def stop(self) -> None:
        if self._sched.running:
            self._sched.shutdown(wait=False)
