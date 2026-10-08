"""Process lifetime shared by the bot and worker."""

import asyncio
import logging
import signal

from transcriptbot.bootstrap import Role
from transcriptbot.config import Settings
from transcriptbot.operations.health import heartbeat
from transcriptbot.storage.database import Database
from transcriptbot.storage.repository import Repository
from transcriptbot.storage.worker_lock import WorkerLock

logger = logging.getLogger(__name__)


async def serve(settings: Settings, role: Role) -> None:
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGTERM, signal.SIGINT)
    for item in signals:
        loop.add_signal_handler(item, shutdown.set)
    database = None
    worker_lock = WorkerLock(settings.sqlite_path) if role == "worker" else None
    pulse = None
    try:
        if worker_lock is not None:
            worker_lock.acquire()
        database = await Database(settings.sqlite_path).open()
        repository = Repository(
            database,
            max_user_jobs=settings.max_user_jobs,
            max_queue_jobs=settings.max_queue_jobs,
            failed_retention_days=settings.failed_retention_days,
        )
        if role == "bot":
            from transcriptbot.telegram import run
        else:
            from transcriptbot.worker import run
        logger.info("role=%s event=ready phase=bootstrap", role)
        pulse = asyncio.create_task(heartbeat(repository, role))
        service = asyncio.create_task(run(settings, shutdown, repository))
        try:
            done, _ = await asyncio.wait((pulse, service), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            service.cancel()
            await asyncio.gather(service, return_exceptions=True)
    finally:
        if pulse is not None:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)
        if database is not None and pulse is not None:
            await database.transaction(
                lambda c: c.execute("DELETE FROM runtime_health WHERE role = ?", (role,))
            )
        if database is not None:
            await database.close()
        if worker_lock is not None:
            worker_lock.release()
        for item in signals:
            loop.remove_signal_handler(item)
        logger.info("role=%s event=stopped", role)
