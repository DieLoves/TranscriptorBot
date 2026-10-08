"""Telegram reception and delivery; implemented in stage 3."""

import asyncio

from transcriptbot.config import Settings


async def run(settings: Settings, shutdown: asyncio.Event) -> None:
    """Keep the scaffold alive until shutdown, without starting polling yet."""
    await shutdown.wait()
