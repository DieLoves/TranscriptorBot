"""Sequential transcription worker; pipeline is connected in later stages."""

import asyncio
import logging

from transcriptbot.config import Settings
from transcriptbot.storage.repository import Repository


async def run(settings: Settings, shutdown: asyncio.Event, repository: Repository) -> None:
    """Recover persisted state; media execution is connected in later stages."""
    if job := await repository.active_job():
        logging.getLogger(__name__).info(
            "job_id=%s event=recovered phase=%s status=%s",
            job.id,
            job.phase,
            job.status,
        )
    await shutdown.wait()
