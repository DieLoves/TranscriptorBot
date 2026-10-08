"""Bounded, cancellable FFmpeg execution with local inputs and no secrets."""

import asyncio
import os
import signal
import wave
from contextlib import suppress
from pathlib import Path

from transcriptbot.config import Settings
from transcriptbot.downloaders.files import DEMUXERS, fsync_directory, regular_path
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.planning import RATE, ChunkPlan, plan_chunks, silence_ranges

FILTER = "aresample=16000,aformat=sample_fmts=s16:sample_rates=16000:channel_layouts=mono"


def input_command(source: Path, stream: int) -> list[str]:
    return [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "info",
        "-xerror",
        "-protocol_whitelist",
        "file",
        "-format_whitelist",
        DEMUXERS,
        # MOV external drefs/absolute aliases are disabled by the demuxer's defaults.
        # Passing MOV-only options here would reject WAV/MP3/other local formats.
        "-i",
        str(source),
        "-map",
        f"0:{stream}",
        "-vn",
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
    ]


async def run_tool(
    command: list[str],
    *,
    timeout: float,
    max_pcm_bytes: int = 0,
    output: Path | None = None,
    maximum: int = 0,
) -> tuple[int, str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"PATH": os.environ.get("PATH", ""), "LANG": "C.UTF-8"},
            start_new_session=True,
        )
    except OSError:
        raise PipelineError("media_tool_unavailable") from None

    async def pcm() -> int:
        size = 0
        while block := await process.stdout.read(65536):
            size += len(block)
            if size > max_pcm_bytes:
                raise PipelineError("duration_limit" if max_pcm_bytes else "invalid_media")
        return size

    async def errors() -> str:
        data = bytearray()
        # Four hours with >=0.3 s pauses fits this bounded diagnostic buffer.
        while block := await process.stderr.read(65536):
            data.extend(block)
            if len(data) > 16_000_000:
                raise PipelineError("invalid_media")
        return data.decode("utf-8", errors="replace")

    async def wait() -> None:
        while process.returncode is None:
            if output is not None and output.exists() and output.stat().st_size > maximum:
                raise PipelineError("upload_size_limit")
            await asyncio.sleep(0.02)
        await process.wait()

    tasks = [asyncio.create_task(pcm()), asyncio.create_task(errors()), asyncio.create_task(wait())]
    try:
        async with asyncio.timeout(timeout):
            size, log, _ = await asyncio.gather(*tasks)
        if process.returncode:
            raise PipelineError("invalid_media")
        return size, log
    except TimeoutError:
        raise PipelineError("media_timeout", retryable=True) from None
    finally:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # The group is stopped; drain its bounded remaining pipe buffers and close transports.
        # wait() alone can leave paused pipes alive past the event loop's lifetime.
        await process.communicate()


async def analyze(source: Path, stream: int, settings: Settings) -> list[ChunkPlan]:
    command = [
        *input_command(source, stream),
        "-af",
        FILTER + f",silencedetect=noise=-35dB:d={settings.silence_min_seconds}",
        "-c:a",
        "pcm_s16le",
        "-f",
        "s16le",
        "pipe:1",
    ]
    # pipe is only an output protocol; the input whitelist remains file-only.
    size, log = await run_tool(
        command,
        timeout=settings.ffmpeg_timeout_seconds,
        max_pcm_bytes=int(settings.max_duration_seconds * RATE) * 2,
    )
    if not size or size % 2:
        raise PipelineError("invalid_media")
    return plan_chunks(
        size // 2,
        silence_ranges(log, size // 2),
        target=round(settings.chunk_seconds * RATE),
        window=round(settings.silence_window_seconds * RATE),
        minimum=round(settings.silence_min_seconds * RATE),
    )


async def extract(
    source: Path,
    stream: int,
    chunk: ChunkPlan,
    destination: Path,
    settings: Settings,
) -> Path:
    temporary = destination.with_suffix(".wav.part")
    if destination.is_symlink() or temporary.is_symlink():
        raise PipelineError("unsafe_path")
    temporary.unlink(missing_ok=True)
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(descriptor)
    command = [
        *input_command(source, stream),
        "-af",
        FILTER + f",atrim=start_sample={chunk.start_sample}:end_sample={chunk.end_sample}",
        "-ar",
        "16000",
        "-ac",
        "1",
        "-c:a",
        "pcm_s16le",
        "-f",
        "wav",
        "-y",
        str(temporary),
    ]
    try:
        await run_tool(
            command,
            timeout=settings.ffmpeg_timeout_seconds,
            output=temporary,
            maximum=settings.wav_target_upload_bytes,
        )
        regular_path(temporary, destination.parent)
        if temporary.stat().st_size > settings.wav_target_upload_bytes:
            raise PipelineError("upload_size_limit")
        with wave.open(str(temporary), "rb") as audio:
            if (
                audio.getparams()[:3] != (1, 2, RATE)
                or audio.getnframes() != chunk.end_sample - chunk.start_sample
            ):
                raise PipelineError("invalid_media")
        with temporary.open("rb") as audio:
            os.fsync(audio.fileno())
        temporary.replace(destination)
        fsync_directory(destination.parent)
        return destination
    except (wave.Error, EOFError):
        raise PipelineError("invalid_media") from None
    finally:
        temporary.unlink(missing_ok=True)
