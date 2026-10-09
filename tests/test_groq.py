import asyncio
import json
from email.utils import formatdate

import httpx
import pytest
from groq import AsyncGroq

from transcriptbot.groq_adapter import GroqAdapter, retry_after
from transcriptbot.media.errors import PipelineError


async def adapter(settings, handler):
    client = AsyncGroq(
        api_key="synthetic-test",
        max_retries=0,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    return GroqAdapter(settings, client=client, clock=lambda: 1000)


async def test_actual_sdk_multipart_and_exact_raw_object(settings, tmp_path):
    raw = {
        "text": "Текст.",
        "segments": [{"id": 4, "start": 0, "end": 1, "text": "Текст."}],
        "language": "ru",
        "custom": {"unknown": True},
    }
    calls = []

    async def handler(request):
        calls.append(request)
        body = await request.aread()
        assert request.url.path == "/openai/v1/audio/transcriptions"
        assert b"whisper-large-v3-turbo" in body
        assert b"verbose_json" in body
        assert b'name="timestamp_granularities[]"' in body
        assert b"segment" in body
        assert b'name="language"' not in body
        assert b"word" not in body and b'name="prompt"' not in body
        assert b"RIFF-test" in body
        return httpx.Response(200, json=raw)

    groq = await adapter(settings, handler)
    source = tmp_path / "current.wav"
    source.write_bytes(b"RIFF-test")
    try:
        assert await groq.transcribe(source) == raw
        assert len(calls) == 1
    finally:
        await groq.close()


@pytest.mark.parametrize(
    ("status", "code", "quota", "retryable", "uncertain"),
    [
        (429, "groq_quota", True, False, False),
        (500, "groq_server", False, True, True),
        (503, "groq_server", False, True, True),
        (408, "groq_server", False, True, True),
        (409, "groq_server", False, True, True),
        (401, "groq_auth", False, False, False),
        (403, "groq_auth", False, False, False),
        (400, "groq_request", False, False, False),
        (413, "groq_request", False, False, False),
    ],
)
async def test_api_failure_classification_no_sdk_retry(
    settings,
    tmp_path,
    status,
    code,
    quota,
    retryable,
    uncertain,
):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            status, headers={"retry-after": "123"}, json={"error": {"message": "secret-do-not-log"}}
        )

    groq = await adapter(settings, handler)
    path = tmp_path / "current.wav"
    path.write_bytes(b"audio")
    try:
        with pytest.raises(PipelineError) as error:
            await groq.transcribe(path)
        assert error.value.code == code
        assert error.value.quota == quota
        assert error.value.retryable == retryable
        assert error.value.uncertain == uncertain
        assert str(error.value) == code
        assert len(calls) == 1
        if quota or retryable:
            assert error.value.retry_after == 123
    finally:
        await groq.close()


@pytest.mark.parametrize("failure", ["connection", "read", "timeout"])
async def test_network_failure_is_uncertain(settings, tmp_path, failure):
    async def handler(request):
        if failure == "timeout":
            await asyncio.Future()
        error = httpx.ConnectError if failure == "connection" else httpx.ReadError
        raise error("secret-do-not-log", request=request)

    settings = settings.model_copy(update={"groq_timeout_seconds": 0.05})
    groq = await adapter(settings, handler)
    path = tmp_path / "current.wav"
    path.write_bytes(b"audio")
    try:
        with pytest.raises(PipelineError, match="groq_network") as error:
            await groq.transcribe(path)
        assert error.value.uncertain and error.value.retryable
    finally:
        await groq.close()


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[]",
        b'{"text": null}',
        b'{"text":"ok","segments":[{"start":-1}]}',
        b'{"text":"ok","segments":[{"text":"ok","start":0,"end":NaN}]}',
        b'{"text":"ok","segments":"invalid"}',
    ],
)
async def test_invalid_response(settings, tmp_path, body):
    groq = await adapter(settings, lambda r: httpx.Response(200, content=body))
    path = tmp_path / "current.wav"
    path.write_bytes(b"audio")
    try:
        with pytest.raises(PipelineError, match="groq_invalid_response"):
            await groq.transcribe(path)
    finally:
        await groq.close()


async def test_size_checked_before_request_and_bounded_response(settings, tmp_path, monkeypatch):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=json.dumps({"text": "x" * 100}).encode())

    settings = settings.model_copy(update={"groq_max_upload_bytes": 8})
    groq = await adapter(settings, handler)
    path = tmp_path / "current.wav"
    path.write_bytes(b"audio-too-large")
    try:
        with pytest.raises(PipelineError, match="upload_size_limit"):
            await groq.transcribe(path)
        assert not calls
        path.write_bytes(b"audio")
        monkeypatch.setattr("transcriptbot.groq_adapter.RESPONSE_LIMIT", 32)
        with pytest.raises(PipelineError, match="groq_invalid_response"):
            await groq.transcribe(path)
        assert len(calls) == 1
    finally:
        await groq.close()


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("123", 123),
        ("0", 1),
        ("-1", 1),
        ("nan", None),
        ("inf", None),
        ("bad", None),
        (None, None),
        (formatdate(1120, usegmt=True), 120),
    ],
)
def test_retry_after_seconds_and_http_date(header, expected):
    assert retry_after(header, 1000) == expected


async def test_sdk_environment_cannot_redirect_credentials(settings, monkeypatch):
    monkeypatch.setenv("GROQ_BASE_URL", "http://untrusted.invalid")
    adapter = GroqAdapter(settings)
    try:
        assert str(adapter.client.base_url).rstrip("/") == "https://api.groq.com"
    finally:
        await adapter.close()
