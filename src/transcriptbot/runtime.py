"""Process lifetime shared by the bot and worker."""

import asyncio
import logging
import signal

from transcriptbot.bootstrap import Role
from transcriptbot.config import Settings

logger = logging.getLogger(__name__)


async def serve(settings: Settings, role: Role) -> None:
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGTERM, signal.SIGINT)
    for item in signals:
        loop.add_signal_handler(item, shutdown.set)
    try:
        if role == "bot":
            from transcriptbot.telegram import run
        else:
            from transcriptbot.worker import run
        logger.info("role=%s event=ready phase=bootstrap", role)
        await run(settings, shutdown)
    finally:
        for item in signals:
            loop.remove_signal_handler(item)
        logger.info("role=%s event=stopped", role)
