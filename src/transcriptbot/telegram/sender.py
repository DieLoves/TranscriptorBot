"""Send only persisted operations; lost responses can cause duplicates, never re-transcription."""

import asyncio
import json
import logging
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import (
    ClientDecodeError,
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from aiogram.types import BufferedInputFile, FSInputFile, InlineKeyboardMarkup, LinkPreviewOptions

from transcriptbot.config import Settings
from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.files import regular_path
from transcriptbot.storage.models import JobStatus
from transcriptbot.storage.worker_lock import DeliveryLock
from transcriptbot.telegram.presentation import preview_payload, safe_filename
from transcriptbot.telegram.store import TelegramStore

logger = logging.getLogger(__name__)
RETRY_DELAYS = (5, 15, 45, 135, 300)


class Sender:
    def __init__(self, bot: Bot, store: TelegramStore, settings: Settings) -> None:
        self.bot, self.store, self.settings = bot, store, settings

    def artifact(self, operation: dict) -> Path:
        path = Path(operation["artifact_path"])
        root = self.settings.work_dir / str(operation["job_id"])
        if not root.is_relative_to(self.settings.work_dir) or root.is_symlink():
            raise ValueError("Invalid artifact path")
        return regular_path(path, root)

    async def send_one(self) -> bool:
        operation = await self.store.next_operation()
        if operation is None:
            return False
        lease = None
        if operation["operation"] in {"preview", "txt", "json"}:
            lease = DeliveryLock(self.settings.sqlite_path, operation["job_id"])
            if not lease.try_acquire():
                return False
        try:
            return await self._send_operation(operation)
        finally:
            if lease is not None:
                lease.release()

    async def _send_operation(self, operation: dict) -> bool:
        kind = operation["operation"]
        job = await self.store.current_job(operation) if operation["job_id"] else None
        # Recheck immediately before external I/O. An already running call may finish after cancel.
        if job and (
            job.generation != operation["generation"]
            or (kind != "status" and job.status != JobStatus.ACTIVE)
        ):
            return True
        payload = json.loads(operation["payload"] or "{}")
        destination = {"chat_id": operation["chat_id"], "message_thread_id": operation["topic_id"]}
        try:
            if kind == "callback":
                await self.bot.answer_callback_query(
                    callback_query_id=operation["callback_id"], text=payload["text"][:200]
                )
                message_id = 0  # Callback acknowledgement has no message ID.
            elif kind == "status":
                markup = InlineKeyboardMarkup.model_validate(payload["reply_markup"])
                if job.status_message_id is not None:
                    result = await self.bot.edit_message_text(
                        chat_id=job.chat_id,
                        message_id=job.status_message_id,
                        text=payload["text"],
                        parse_mode=payload.get("parse_mode"),
                        link_preview_options=LinkPreviewOptions(is_disabled=True),
                        reply_markup=markup,
                    )
                    message_id = result.message_id if result is not True else job.status_message_id
                else:
                    result = await self.bot.send_message(
                        **destination,
                        text=payload["text"],
                        parse_mode=payload.get("parse_mode"),
                        link_preview_options=LinkPreviewOptions(is_disabled=True),
                        reply_markup=markup,
                    )
                    message_id = result.message_id
            elif kind in {"txt", "json"}:
                path = self.artifact(operation)
                filename = safe_filename(job, kind)
                # TDLib rejects zero-byte uploads. Keep the empty checkpoint intact,
                # but send a blank line for both new and recovered empty transcripts.
                document = (
                    BufferedInputFile(b"\n", filename=filename)
                    if kind == "txt" and path.stat().st_size == 0
                    else FSInputFile(path, filename=filename)
                )
                result = await self.bot.send_document(
                    **destination,
                    document=document,
                )
                message_id = result.message_id
            else:
                if kind == "preview":
                    payload = preview_payload(
                        self.artifact(operation), self.settings.preview_max_chars
                    )
                result = await self.bot.send_message(
                    **destination,
                    text=payload["text"],
                    parse_mode=payload.get("parse_mode"),
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    reply_markup=(
                        InlineKeyboardMarkup.model_validate(payload["reply_markup"])
                        if "reply_markup" in payload
                        else None
                    ),
                )
                message_id = result.message_id
        except TelegramRetryAfter as exc:
            await self.store.retry(operation, delay=max(1, exc.retry_after), count=False)
        except TelegramBadRequest as exc:
            description = exc.message.casefold()
            if (
                kind == "status"
                and job.status_message_id is not None
                and "message is not modified" in description
            ):
                await self.store.confirm(operation, job.status_message_id)
            elif (
                kind == "status"
                and job.status_message_id is not None
                and any(
                    reason in description
                    for reason in ("message to edit not found", "message can't be edited")
                )
            ):
                await self.store.replace_status(operation)
            else:
                await self.store.reject_delivery(operation, "telegram_delivery_unavailable")
        except TelegramForbiddenError:
            await self.store.reject_delivery(operation, "telegram_delivery_unavailable")
        except (ClientDecodeError, TelegramNetworkError, TelegramServerError):
            attempts = operation["attempts"]
            if attempts >= self.settings.max_retries:
                await self.store.reject_delivery(operation, "telegram_delivery_failed")
            else:
                await self.store.retry(operation, delay=RETRY_DELAYS[attempts], count=True)
        except TelegramAPIError:
            await self.store.reject_delivery(operation, "telegram_delivery_failed")
        except (DownloadError, OSError, ValueError, UnicodeError):
            await self.store.reject_delivery(operation, "result_artifact_unavailable")
        else:
            await self.store.confirm(operation, message_id)
        logger.info("event=outbox_attempt operation=%s job_id=%s", kind, operation["job_id"])
        return True

    async def run(self) -> None:
        while True:
            await self.store.refresh_statuses()
            if not await self.send_one():
                await asyncio.sleep(0.25)
