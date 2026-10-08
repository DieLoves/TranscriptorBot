"""Source acquisition at the head of the queue and cancellation-aware child lifetime."""

import asyncio
import json
import os
import signal
import sys
from contextlib import suppress
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import (
    ClientDecodeError,
    TelegramAPIError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)

from transcriptbot.config import Settings
from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.files import job_directory, regular_path, reserve_disk
from transcriptbot.storage.models import Job
from transcriptbot.storage.repository import Repository


async def child_process(payload: dict, timeout: float) -> None:
    environment = {"PATH": os.environ.get("PATH", ""), "LANG": "C.UTF-8", "DENO_DIR": "/tmp/deno"}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "transcriptbot.downloaders.child",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env=environment,
        start_new_session=True,
    )
    try:
        async with asyncio.timeout(timeout):
            process.stdin.write(json.dumps(payload).encode())
            await process.stdin.drain()
            process.stdin.close()
            output = b""
            while block := await process.stdout.read(4097 - len(output)):
                output += block
                if len(output) > 4096:
                    raise DownloadError("invalid_response")
            await process.wait()
            try:
                result = json.loads(output)
                if process.returncode or not isinstance(result["ok"], bool):
                    raise ValueError
                if not result["ok"]:
                    code = result["code"]
                    if not isinstance(code, str) or not code.replace("_", "").isalnum():
                        raise ValueError
                    raise DownloadError(
                        code,
                        retryable=bool(result.get("retryable")),
                        retry_after=result.get("retry_after"),
                    )
            except (ValueError, KeyError, TypeError):
                raise DownloadError("invalid_response") from None
    except TimeoutError:
        raise DownloadError("download_timeout", retryable=True) from None
    finally:
        # Kill the whole session, including FFprobe/Deno, before cleanup can start.
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()


class SourceDownloader:
    def __init__(self, settings: Settings, repository: Repository, bot: Bot):
        self.settings, self.repository, self.bot = settings, repository, bot

    async def acquire(self, job: Job) -> dict:
        settings = self.settings
        directory = job_directory(settings.work_dir, job.id)
        for name in (
            "source.media",
            "source.part",
            "source.media.part",
            "source.json",
            "source.json.part",
        ):
            await self.repository.track_asset(job.id, kind="workspace", path=str(directory / name))
        payload = {
            "job_id": job.id,
            "kind": str(job.source_kind),
            "reference": job.source_ref,
            "title": job.title,
            "work_dir": str(settings.work_dir),
            "telegram_files_dir": str(settings.telegram_files_dir),
            "maximum": settings.max_input_bytes,
            "duration": settings.max_duration_seconds,
            "reserve": settings.disk_reserve_bytes,
            "connect": settings.http_connect_timeout_seconds,
            "read": settings.http_read_timeout_seconds,
            "redirects": settings.http_max_redirects,
            "probe_timeout": settings.ffprobe_timeout_seconds,
        }
        if job.source_kind == "telegram":
            checkpoint = directory / "source.json"
            if checkpoint.exists() or checkpoint.is_symlink():
                regular_path(checkpoint, directory)
                if checkpoint.stat().st_size > 65536:
                    raise DownloadError("source_changed")
                try:
                    cached = json.loads(checkpoint.read_text(encoding="utf-8"))
                    path = regular_path(Path(cached["path"]), settings.telegram_files_dir)
                except (ValueError, KeyError, TypeError):
                    raise DownloadError("source_changed") from None
                await self.repository.track_asset(job.id, kind="telegram_cache", path=str(path))
                payload["path"] = str(path)
                return await self._verify(payload, directory)
            reserve_disk(
                settings.telegram_files_dir,
                job.size_bytes or settings.max_input_bytes,
                settings.disk_reserve_bytes,
            )
            try:
                file = await self.bot.get_file(job.source_ref)
            except TelegramRetryAfter as exc:
                raise DownloadError(
                    "download_rate_limit", retryable=True, retry_after=exc.retry_after
                ) from None
            except (ClientDecodeError, TelegramNetworkError, TelegramServerError):
                raise DownloadError("download_network", retryable=True) from None
            except TelegramAPIError:
                raise DownloadError("source_unavailable") from None
            if file.file_path is None:
                raise DownloadError("source_unavailable")
            path = regular_path(Path(file.file_path), settings.telegram_files_dir)
            await self.repository.track_asset(job.id, kind="telegram_cache", path=str(path))
            if (file.file_size is not None and file.file_size > settings.max_input_bytes) or (
                path.stat().st_size > settings.max_input_bytes
            ):
                raise DownloadError("size_limit")
            payload["path"] = str(path)
        return await self._verify(payload, directory)

    async def _verify(self, payload: dict, directory: Path) -> dict:
        settings = self.settings
        await child_process(payload, settings.download_timeout_seconds)
        manifest_path = regular_path(directory / "source.json", directory)
        if manifest_path.stat().st_size > 65536:
            raise DownloadError("invalid_response")
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            raise DownloadError("invalid_response") from None
