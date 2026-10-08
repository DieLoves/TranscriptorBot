"""Sequential transcription worker; pipeline is connected in later stages."""

import asyncio

from transcriptbot.config import Settings


async def run(settings: Settings, shutdown: asyncio.Event) -> None:
    """Keep the scaffold alive until shutdown, without starting jobs yet."""
    await shutdown.wait()
