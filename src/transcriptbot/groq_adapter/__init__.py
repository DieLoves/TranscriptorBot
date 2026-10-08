"""Async SDK adapter: exact raw JSON, one request, bounded response and safe errors."""

import asyncio
import json
import math
import time
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
from groq import APIConnectionError, APIStatusError, AsyncGroq

from transcriptbot.config import Settings
from transcriptbot.media.artifacts import RESPONSE_LIMIT, validate_response
from transcriptbot.media.errors import PipelineError


def retry_after(value: str | None, now: float) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = parsedate_to_datetime(value).timestamp() - now
        except (ValueError, TypeError, OverflowError):
            return None
    return max(1, seconds) if math.isfinite(seconds) else None


class GroqAdapter:
    def __init__(self, settings: Settings, *, client: AsyncGroq | None = None, clock=time.time):
        self.settings, self.clock = settings, clock
        self.client = client or AsyncGroq(
            api_key=settings.groq_api_key.get_secret_value(),
            max_retries=0,
            timeout=settings.groq_timeout_seconds,
            http_client=httpx.AsyncClient(trust_env=False, follow_redirects=False),
        )

    async def close(self) -> None:
        await self.client.close()

    async def transcribe(self, path: Path) -> dict:
        if not 0 < path.stat().st_size <= self.settings.groq_max_upload_bytes:
            raise PipelineError("upload_size_limit")
        try:
            async with asyncio.timeout(self.settings.groq_timeout_seconds):
                with path.open("rb") as audio:
                    async with self.client.audio.transcriptions.with_streaming_response.create(
                        file=audio,
                        model=self.settings.groq_model,
                        response_format="verbose_json",
                        timestamp_granularities=["segment"],
                    ) as response:
                        data = bytearray()
                        async for block in response.iter_bytes():
                            data.extend(block)
                            if len(data) > RESPONSE_LIMIT:
                                raise PipelineError(
                                    "groq_invalid_response", retryable=True, uncertain=True
                                )
                try:
                    value = json.loads(data)
                except (ValueError, UnicodeError):
                    raise PipelineError(
                        "groq_invalid_response", retryable=True, uncertain=True
                    ) from None
                validate_response(value)
                return value
        except APIStatusError as exc:
            delay = retry_after(exc.response.headers.get("retry-after"), self.clock())
            if exc.status_code == 429:
                raise PipelineError("groq_quota", quota=True, retry_after=delay) from None
            if exc.status_code in (408, 409) or exc.status_code >= 500:
                raise PipelineError(
                    "groq_server", retryable=True, retry_after=delay, uncertain=True
                ) from None
            code = "groq_auth" if exc.status_code in (401, 403) else "groq_request"
            raise PipelineError(code) from None
        except (APIConnectionError, httpx.HTTPError, TimeoutError):
            raise PipelineError("groq_network", retryable=True, uncertain=True) from None
