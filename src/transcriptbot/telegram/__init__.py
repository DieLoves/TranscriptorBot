"""Telegram reception and delivery in the bot process."""

import asyncio

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.exceptions import (
    ClientDecodeError,
    TelegramAPIError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)

from transcriptbot.config import Settings
from transcriptbot.storage.repository import Repository
from transcriptbot.telegram.polling import TelegramRuntimeError, poll
from transcriptbot.telegram.requests import make_dispatcher
from transcriptbot.telegram.sender import Sender
from transcriptbot.telegram.store import TelegramStore


async def run(settings: Settings, shutdown: asyncio.Event, repository: Repository) -> None:
    session = AiohttpSession(
        api=TelegramAPIServer.from_base(
            settings.telegram_bot_api_base_url,
            is_local=True,
        )
    )
    bot = Bot(settings.telegram_bot_token.get_secret_value(), session=session)
    store = TelegramStore(repository, status_interval=settings.status_update_interval_seconds)

    async def receive() -> None:
        while True:
            try:
                identity = await bot.get_me(request_timeout=15)
                break
            except TelegramRetryAfter as exc:
                await asyncio.sleep(max(1, exc.retry_after))
            except (ClientDecodeError, TelegramNetworkError, TelegramServerError):
                await asyncio.sleep(5)
            except TelegramAPIError:
                raise TelegramRuntimeError(
                    "Telegram Bot API недоступен: проверьте настройки бота"
                ) from None
        await poll(bot, make_dispatcher(settings, identity.username or ""), store)

    tasks = [
        asyncio.create_task(receive()),
        asyncio.create_task(Sender(bot, store, settings).run()),
        asyncio.create_task(shutdown.wait()),
    ]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
