"""FIFO source acquisition; preparation/transcription are connected in stage 5."""

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer

from transcriptbot.config import Settings
from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.service import SourceDownloader
from transcriptbot.storage.models import Job, JobStatus, Phase
from transcriptbot.storage.repository import Repository

logger = logging.getLogger(__name__)
RETRY_DELAYS = (5, 15, 45, 135, 300)


async def download_job(job: Job, downloader, repository: Repository, shutdown: asyncio.Event):
    operation = asyncio.create_task(downloader.acquire(job))
    try:
        while not operation.done():
            current = await repository.get_job(job.id, owner_id=job.owner_id)
            if (
                shutdown.is_set()
                or current is None
                or (current.status != JobStatus.ACTIVE or current.revision != job.revision)
            ):
                operation.cancel()
                return
            await asyncio.wait({operation}, timeout=0.2)
        if shutdown.is_set():
            return
        manifest = operation.result()
        await repository.finish_download(job, manifest)
    except DownloadError as exc:
        logger.info("job_id=%s event=download_failed error_code=%s", job.id, exc.code)
        if exc.retryable and job.attempts < downloader.settings.max_retries:
            delay = exc.retry_after or RETRY_DELAYS[min(job.attempts, len(RETRY_DELAYS) - 1)]
            await repository.defer(job, until=repository.clock() + delay, error_code=exc.code)
        else:
            await repository.fail(job, error_code=exc.code)
    except OSError:
        await repository.fail(job, error_code="download_io")
    finally:
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def run(settings: Settings, shutdown: asyncio.Event, repository: Repository) -> None:
    if job := await repository.active_job():
        logger.info("job_id=%s event=recovered phase=%s status=%s", job.id, job.phase, job.status)
    session = AiohttpSession(
        api=TelegramAPIServer.from_base(settings.telegram_bot_api_base_url, is_local=True),
        timeout=settings.http_read_timeout_seconds,
    )
    async with Bot(settings.telegram_bot_token.get_secret_value(), session=session) as bot:
        downloader = SourceDownloader(settings, repository, bot)
        while not shutdown.is_set():
            job = await repository.claim_next()
            if job is not None and job.phase == Phase.DOWNLOAD:
                await download_job(job, downloader, repository, shutdown)
                continue
            with suppress(TimeoutError):
                await asyncio.wait_for(shutdown.wait(), timeout=0.5)
