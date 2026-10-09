"""Sequential polling: only committed updates may advance the next getUpdates offset."""

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.dispatcher.event.bases import UNHANDLED
from aiogram.exceptions import (
    ClientDecodeError,
    TelegramAPIError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from aiogram.types import Update

from transcriptbot.retries import retry_after
from transcriptbot.telegram.requests import Request
from transcriptbot.telegram.store import TelegramStore

logger = logging.getLogger(__name__)


class TelegramRuntimeError(Exception):
    """Safe startup/polling diagnostic, without tokens or API response bodies."""


async def process_update(
    bot: Bot, dispatcher: Dispatcher, store: TelegramStore, update: Update
) -> None:
    request = await dispatcher.feed_update(bot, update)
    if request is UNHANDLED or not isinstance(request, Request):
        request = Request("ignored")
    await store.accept(update.update_id, request)


async def poll(bot: Bot, dispatcher: Dispatcher, store: TelegramStore) -> None:
    failures = 0
    while True:
        try:
            updates = await bot.get_updates(
                offset=await store.offset(),
                timeout=30,
                request_timeout=40,
                allowed_updates=["message", "channel_post", "callback_query"],
            )
        except TelegramRetryAfter as exc:
            await asyncio.sleep(retry_after(str(exc.retry_after), store.clock()) or 60)
            continue
        except (ClientDecodeError, TelegramNetworkError, TelegramServerError):
            failures += 1
            logger.warning("event=polling_retry code=telegram_network")
            await asyncio.sleep(min(30, 2 ** min(failures, 5)))
            continue
        except TelegramAPIError:
            raise TelegramRuntimeError(
                "Telegram polling недоступен: проверьте Bot API и настройки бота"
            ) from None
        failures = 0
        for update in updates:
            await process_update(bot, dispatcher, store, update)
