"""FIFO source acquisition and recoverable media/Groq pipeline."""

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer

from transcriptbot.config import Settings
from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.service import SourceDownloader
from transcriptbot.groq_adapter import GroqAdapter
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.service import MediaPipeline
from transcriptbot.operations.cleanup import Cleanup
from transcriptbot.storage.database import StorageError
from transcriptbot.storage.models import Job, JobStatus, Phase, SourceKind
from transcriptbot.storage.repository import Repository

logger = logging.getLogger(__name__)
RETRY_DELAYS = (5, 15, 45, 135, 300)


async def unexpected_failure(job: Job, exc: Exception, repository: Repository, settings) -> None:
    """Bound an unknown failure: a crash would replay the same job after every restart."""
    # The text of a library exception may contain URLs, tokens or response bodies.
    logger.warning(
        "job_id=%s event=unexpected_failure phase=%s error_type=%s",
        job.id,
        job.phase,
        type(exc).__name__,
    )
    current = await repository.get_job(job.id, owner_id=job.owner_id)
    if current is None or current.status != JobStatus.ACTIVE:
        return
    if current.attempts < settings.max_retries:
        delay = RETRY_DELAYS[min(current.attempts, len(RETRY_DELAYS) - 1)]
        await repository.defer(
            current, until=repository.clock() + delay, error_code="internal_error"
        )
    else:
        await repository.fail(current, error_code="internal_error")


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
    except StorageError:
        raise
    except Exception as exc:
        await unexpected_failure(job, exc, repository, downloader.settings)
    finally:
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def run(settings: Settings, shutdown: asyncio.Event, repository: Repository) -> None:
    cleanup = Cleanup(settings, repository)
    await cleanup.run(startup=True)
    if job := await repository.active_job():
        logger.info("job_id=%s event=recovered phase=%s status=%s", job.id, job.phase, job.status)
    session = AiohttpSession(
        api=TelegramAPIServer.from_base(settings.telegram_bot_api_base_url, is_local=True),
        timeout=settings.http_read_timeout_seconds,
    )
    async with Bot(settings.telegram_bot_token.get_secret_value(), session=session) as bot:
        downloader = SourceDownloader(settings, repository, bot)
        groq = GroqAdapter(settings, clock=repository.clock)
        pipeline = MediaPipeline(settings, repository, groq)

        async def work():
            while not shutdown.is_set():
                job = await repository.claim_next()
                if job is not None:
                    cleanup.busy.add(job.id)
                    if job.phase == Phase.DOWNLOAD and job.source_kind == SourceKind.TELEGRAM:
                        cleanup.cache_busy.add(job.id)
                    try:
                        if job.phase == Phase.DOWNLOAD:
                            await download_job(job, downloader, repository, shutdown)
                            continue
                        await process_job(job, pipeline, repository, shutdown)
                    finally:
                        cleanup.busy.discard(job.id)
                        cleanup.cache_busy.discard(job.id)
                    if job.phase != Phase.DELIVER:
                        continue
                with suppress(TimeoutError):
                    await asyncio.wait_for(shutdown.wait(), timeout=0.5)

        async def maintain():
            while not shutdown.is_set():
                await cleanup.run()
                with suppress(TimeoutError):
                    await asyncio.wait_for(shutdown.wait(), timeout=0.5)

        tasks = [asyncio.create_task(work()), asyncio.create_task(maintain())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await groq.close()


async def process_job(job: Job, pipeline, repository: Repository, shutdown: asyncio.Event):
    """Cancel active I/O on shutdown/revision change; only persist safe failure codes."""
    operation = asyncio.create_task(pipeline.step(job))
    try:
        while not operation.done():
            current = await repository.get_job(job.id, owner_id=job.owner_id)
            if (
                shutdown.is_set()
                or current is None
                or current.status != JobStatus.ACTIVE
                or current.revision != job.revision
            ):
                operation.cancel()
                return
            await asyncio.wait({operation}, timeout=0.1)
        operation.result()
    except (PipelineError, DownloadError) as exc:
        logger.info(
            "job_id=%s event=pipeline_failed phase=%s error_code=%s", job.id, job.phase, exc.code
        )
        settings = pipeline.settings
        if getattr(exc, "quota", False):
            delay = exc.retry_after or min(
                settings.quota_retry_max_seconds, 60 * 2 ** min(job.quota_attempts, 6)
            )
            await repository.defer(
                job, until=repository.clock() + delay, error_code=exc.code, quota=True
            )
        elif exc.retryable and job.attempts < settings.max_retries:
            delay = exc.retry_after or RETRY_DELAYS[min(job.attempts, len(RETRY_DELAYS) - 1)]
            await repository.defer(job, until=repository.clock() + delay, error_code=exc.code)
        else:
            await repository.fail(job, error_code=exc.code)
    except OSError:
        await repository.fail(job, error_code="pipeline_io")
    except StorageError:
        raise
    except Exception as exc:
        await unexpected_failure(job, exc, repository, pipeline.settings)
    finally:
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
