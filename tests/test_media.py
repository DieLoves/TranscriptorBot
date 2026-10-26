import asyncio
import json
import math
import os
import subprocess
import sys
import wave
from array import array
from dataclasses import replace
from pathlib import Path

import pytest

from transcriptbot.config import Settings
from transcriptbot.media.artifacts import (
    PROMPT_BYTES,
    assemble,
    context_prompt,
    publish_bytes,
    publish_checkpoint,
    read_json,
)
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.planning import (
    FRAME,
    RATE,
    ChunkPlan,
    Envelope,
    plan_chunks,
    silence_ranges,
)
from transcriptbot.media.tools import analyze, extract, input_command, run_tool
from transcriptbot.storage.models import Chunk


def wav(path, seconds=5.01, pauses=(), rate=RATE, channels=1, quiet=()):
    def amplitude(n):
        if any(left <= n / rate < right for left, right in pauses):
            return 0
        # Quieter, but still far above the -35 dB silence threshold (noisy pause).
        return 1500 if any(left <= n / rate < right for left, right in quiet) else 8000

    samples = array(
        "h",
        (
            round(amplitude(n) * math.sin(n * 2 * math.pi * 440 / rate))
            for n in range(round(seconds * rate))
            for _ in range(channels)
        ),
    )
    with wave.open(str(path), "wb") as audio:
        audio.setparams((channels, 2, rate, 0, "NONE", "none"))
        audio.writeframes(samples.tobytes())
    return samples.tobytes()


def frames(path):
    with wave.open(str(path), "rb") as audio:
        assert audio.getparams()[:3] == (1, 2, RATE)
        return audio.readframes(audio.getnframes())


@pytest.fixture
def small_settings(settings):
    return Settings(
        _env_file=None,
        **{
            **settings.model_dump(),
            "chunk_seconds": 2,
            "silence_window_seconds": 0.5,
            "silence_min_seconds": 0.3,
        },
    )


@pytest.mark.parametrize("pauses", [[], [(1.55, 1.95)], [(1.5, 2.1)], [(1.6, 1.7)]])
async def test_exact_sample_coverage_and_pause_boundaries(tmp_path, small_settings, pauses):
    source = tmp_path / "source.wav"
    original = wav(source, pauses=pauses)
    chunks = await analyze(source, 0, small_settings)
    assert chunks[0].start_sample == 0
    assert chunks[-1].end_sample == len(original) // 2
    expected = 1.75 if pauses and pauses[0][1] - pauses[0][0] >= 0.3 else 2
    assert abs(chunks[0].end_sample / RATE - expected) < 0.002
    joined = b""
    for chunk in chunks:
        path = await extract(source, 0, chunk, tmp_path / "current.wav", small_settings)
        joined += frames(path)
        assert path.stat().st_size <= small_settings.wav_target_upload_bytes
    assert joined == original
    assert not (tmp_path / "current.wav.part").exists()


async def test_noisy_recording_is_cut_in_quietest_part_not_mid_speech(tmp_path, small_settings):
    source = tmp_path / "source.wav"
    wav(source, quiet=[(1.6, 1.9)])
    chunks = await analyze(source, 0, small_settings)
    # Background noise hides every pause from silencedetect; a hard cut would land at 2 s.
    assert abs(chunks[0].end_sample / RATE - 1.75) < 0.011
    assert chunks[-1].end_sample == round(5.01 * RATE)


def test_planner_falls_back_to_quietest_run_without_detected_pauses():
    total, target, window, minimum = 5 * RATE, 2 * RATE, RATE // 2, round(0.3 * RATE)
    power = array("d", [1.0] * (total // FRAME))
    for index in range(162, 192):
        power[index] = 0.1
    power[150] = 0.0  # One quiet frame is shorter than the minimum pause.
    options = {"target": target, "window": window, "minimum": minimum}
    chunks = plan_chunks(total, [], power=power, **options)
    assert chunks[0].end_sample == 162 * FRAME + 15 * FRAME
    assert plan_chunks(total, [], **options)[0].end_sample == target
    # A detected pause keeps priority over the energy envelope.
    detected = [(round(1.55 * RATE), round(1.95 * RATE))]
    assert plan_chunks(total, detected, power=power, **options)[0].end_sample == 1.75 * RATE
    assert all(chunk.end_sample > chunk.start_sample for chunk in chunks)
    assert chunks[-1].end_sample == total


def test_envelope_handles_pipe_blocks_not_aligned_to_frames():
    samples = array("h", ((index * 37) % 600 - 300 for index in range(1000)))
    data = samples.tobytes()
    envelope = Envelope()
    for offset in range(0, len(data), 333):  # Odd sizes split samples as well as frames.
        envelope.feed(data[offset : offset + 333])
    expected = [
        sum(value * value for value in samples[start : start + FRAME]) / FRAME
        for start in range(0, len(samples) - FRAME + 1, FRAME)
    ]
    assert list(envelope.power) == expected


def test_context_prompt_is_a_bounded_word_aligned_tail():
    assert context_prompt("") is None and context_prompt(" \n ") is None
    assert context_prompt(" Короткий\n  текст. ") == "Короткий текст."
    prompt = context_prompt("слово, " * 100 + "конец.")
    # Whisper accepts 224 byte-level tokens; every token covers at least one byte.
    assert len(prompt.encode()) <= PROMPT_BYTES
    assert prompt.startswith("слово,") and prompt.endswith("конец.")
    unspaced = context_prompt("漢字" * 200)
    assert unspaced and len(unspaced.encode()) <= PROMPT_BYTES
    assert unspaced.encode().decode("utf-8") == unspaced


async def test_ten_minute_chunk_and_short_tail(tmp_path, settings):
    source = tmp_path / "source.wav"
    # Constant amplitude has no pauses; include a 20 ms tail after ten minutes.
    with wave.open(str(source), "wb") as audio:
        audio.setparams((1, 2, RATE, 0, "NONE", "none"))
        for _ in range(600):
            audio.writeframes(b"\x00\x10" * RATE)
        audio.writeframes(b"\x00\x10" * 320)
    chunks = await analyze(source, 0, settings)
    assert chunks == [ChunkPlan(0, 600 * RATE), ChunkPlan(600 * RATE, 600 * RATE + 320)]
    path = await extract(source, 0, chunks[0], tmp_path / "current.wav", settings)
    assert 19_200_000 < path.stat().st_size < 19_201_024
    await extract(source, 0, chunks[1], path, settings)
    assert frames(path) == b"\x00\x10" * 320
    # atrim also covers a single sample exactly; it is merged before API upload.
    await extract(source, 0, ChunkPlan(600 * RATE, 600 * RATE + 1), path, settings)
    assert frames(path) == b"\x00\x10"


def ffmpeg(*args):
    subprocess.run(
        ["ffmpeg", "-nostdin", "-v", "error", "-y", *map(str, args)],
        check=True,
        capture_output=True,
        env={"PATH": os.environ["PATH"], "LANG": "C.UTF-8"},
    )


@pytest.mark.parametrize("suffix", ["wav", "mp3", "m4a", "ogg", "webm", "flac"])
async def test_resampled_compressed_coverage(tmp_path, small_settings, suffix):
    source = tmp_path / f"source.{suffix}"
    original = tmp_path / "original.wav"
    wav(original, rate=44100, channels=2)
    ffmpeg("-i", original, source)
    reference = tmp_path / "reference.wav"
    ffmpeg("-i", source, "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", reference)
    chunks = await analyze(source, 0, small_settings)
    joined = b""
    for chunk in chunks:
        joined += frames(await extract(source, 0, chunk, tmp_path / "current.wav", small_settings))
    assert joined == frames(reference)


async def test_first_audio_track(tmp_path, small_settings):
    first, second, source = tmp_path / "one.wav", tmp_path / "two.wav", tmp_path / "video.mkv"
    expected = wav(first, seconds=3)
    wav(second, seconds=3, pauses=[(0, 3)])
    ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=16x16:d=3",
        "-i",
        first,
        "-i",
        second,
        "-map",
        "0:v",
        "-map",
        "1:a",
        "-map",
        "2:a",
        "-c:v",
        "ffv1",
        "-c:a",
        "pcm_s16le",
        source,
    )
    chunks = await analyze(source, 1, small_settings)
    assert (
        b"".join(
            [
                frames(await extract(source, 1, c, tmp_path / "current.wav", small_settings))
                for c in chunks
            ]
        )
        == expected
    )


@pytest.mark.parametrize(
    "content",
    [
        "#EXTM3U\n#EXTINF:1,\nhttp://127.0.0.1:8081/private.ts\n",
        "ffconcat version 1.0\nfile 'http://127.0.0.1:8081/private.wav'\n",
    ],
)
async def test_manifest_network_inputs_rejected(tmp_path, small_settings, content):
    source = tmp_path / "source.media"
    source.write_text(content)
    with pytest.raises(PipelineError, match="invalid_media"):
        await analyze(source, 0, small_settings)
    command = input_command(source, 0)
    assert command[command.index("-protocol_whitelist") + 1] == "file"
    allowed = command[command.index("-format_whitelist") + 1].split(",")
    assert not {"concat", "hls", "dash", "sdp"}.intersection(allowed)


def replace_mov_reference(data: bytes, reference: bytes) -> bytes:
    output = bytearray()
    position = 0
    while position < len(data):
        size = int.from_bytes(data[position : position + 4], "big")
        kind = data[position + 4 : position + 8]
        assert size >= 8
        payload = data[position + 8 : position + size]
        if kind in {b"moov", b"trak", b"mdia", b"minf", b"dinf"}:
            payload = replace_mov_reference(payload, reference)
        elif kind == b"dref":
            # FullBox version/flags + one url entry whose self-contained flag is unset.
            entry = b"\0\0\0\0" + reference + b"\0"
            payload = b"\0\0\0\0\0\0\0\1" + (len(entry) + 8).to_bytes(4, "big") + b"url " + entry
        output.extend((len(payload) + 8).to_bytes(4, "big") + kind + payload)
        position += size
    return bytes(output)


@pytest.mark.parametrize("network", [False, True])
async def test_mov_external_track_is_not_opened(tmp_path, small_settings, network):
    source, original = tmp_path / "source.mov", tmp_path / "original.wav"
    expected = wav(original, seconds=1)
    ffmpeg("-i", original, "-c:a", "pcm_s16le", source)
    accesses = []

    async def connection(reader, writer):
        accesses.append(True)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(connection, "127.0.0.1", 0)
    try:
        private = tmp_path / "private.wav"
        wav(private, seconds=2, pauses=[(0, 2)])
        port = server.sockets[0].getsockname()[1]
        reference = f"http://127.0.0.1:{port}/private.wav" if network else str(private)
        data = source.read_bytes()
        assert data.index(b"mdat") < data.index(b"moov")  # offsets before moov stay unchanged.
        source.write_bytes(replace_mov_reference(data, reference.encode()))
        try:
            chunks = await analyze(source, 0, small_settings)
            assert chunks[-1].end_sample == len(expected) // 2
            output = await extract(source, 0, chunks[0], tmp_path / "current.wav", small_settings)
            assert frames(output) == expected
        except PipelineError as error:
            assert error.code == "invalid_media"
        await asyncio.sleep(0.05)
        assert not accesses
    finally:
        server.close()
        await server.wait_closed()


async def test_decoded_duration_limit(tmp_path, small_settings):
    source = tmp_path / "source.wav"
    wav(source, seconds=3)
    settings = Settings(
        _env_file=None, **{**small_settings.model_dump(), "max_duration_seconds": 2}
    )
    with pytest.raises(PipelineError, match="duration_limit"):
        await analyze(source, 0, settings)


async def test_chunk_upload_size_limit(tmp_path, small_settings):
    source = tmp_path / "source.wav"
    wav(source, seconds=3)
    settings = Settings(
        _env_file=None,
        **{
            **small_settings.model_dump(),
            "chunk_seconds": 1,
            "wav_target_upload_bytes": 33024,
            "groq_max_upload_bytes": 33024,
        },
    )
    with pytest.raises(PipelineError, match="upload_size_limit"):
        await extract(source, 0, ChunkPlan(0, 2 * RATE), tmp_path / "current.wav", settings)
    assert not (tmp_path / "current.wav").exists()


def running(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(") ")[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


@pytest.mark.parametrize("timeout", [True, False])
async def test_media_process_group_and_secret_isolation(tmp_path, monkeypatch, timeout):
    tool, marker = tmp_path / "tool.py", tmp_path / "pids.json"
    tool.write_text(
        "import os,sys,json,subprocess,time\n"
        "assert not any(k in os.environ for k in "
        "('GROQ_API_KEY','TELEGRAM_BOT_TOKEN','TELEGRAM_API_HASH'))\n"
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])\n"
        f"with open({str(marker) + '.part'!r},'w') as stream:\n"
        " stream.write(json.dumps([os.getpid(),child.pid]))\n"
        f"os.replace({str(marker) + '.part'!r},{str(marker)!r})\n"
        "time.sleep(60)\n"
    )
    monkeypatch.setenv("GROQ_API_KEY", "test-do-not-pass")
    operation = asyncio.create_task(
        run_tool([sys.executable, str(tool)], timeout=0.5 if timeout else 30)
    )
    async with asyncio.timeout(3):
        while not marker.exists():
            await asyncio.sleep(0.02)
    pids = json.loads(marker.read_text())
    if timeout:
        with pytest.raises(PipelineError, match="media_timeout"):
            await operation
    else:
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    async with asyncio.timeout(3):
        while any(running(pid) for pid in pids):
            await asyncio.sleep(0.02)


async def test_output_is_bounded():
    with pytest.raises(PipelineError, match="duration_limit"):
        await run_tool([sys.executable, "-c", "print('x'*1000000)"], timeout=5, max_pcm_bytes=10)


def test_sample_planner_four_hours_and_tail():
    chunks = plan_chunks(14400 * RATE, [], target=600 * RATE, window=8 * RATE, minimum=4800)
    assert len(chunks) == 24
    assert chunks[-1].end_sample == 14400 * RATE
    assert plan_chunks(600 * RATE + 1, [], target=600 * RATE, window=8 * RATE, minimum=4800) == [
        ChunkPlan(0, 600 * RATE + 1)
    ]
    assert silence_ranges(
        "lavfi.silence_start=1\nlavfi.silence_end=2\nlavfi.silence_start=3",
        4 * RATE,
    ) == [
        (RATE, 2 * RATE),
        (3 * RATE, 4 * RATE),
    ]


def test_assembly_preserves_raw_single_and_global_multi():
    one = Chunk(1, 0, 0, 2 * RATE, None, None, None)
    two = replace(one, number=1, start_sample=2 * RATE, end_sample=3 * RATE)
    raw = {
        "text": " Привет! ",
        "language": "ru",
        "extra": {"keep": True},
        "segments": [{"id": 90, "seek": 2, "start": 0.25, "end": 1, "text": "Привет!"}],
    }
    second = {
        "text": " Мир. ",
        "language": "en",
        "segments": [{"id": 7, "start": 0, "end": 0.5, "text": "Мир."}],
    }
    assert assemble([one], [raw]) is raw
    merged = assemble([one, two], [raw, second])
    assert merged["text"] == "Привет! Мир."
    assert merged["duration"] == 3
    assert merged["language"] == "ru"
    assert [s["id"] for s in merged["segments"]] == [0, 1]
    assert merged["segments"][1]["start"] == 2
    assert merged["segments"][1]["end"] == 2.5
    assert merged["chunks"][1]["response"] is second
    assert raw["segments"][0]["id"] == 90


def test_empty_text_and_checkpoint_publication(tmp_path):
    chunk = Chunk(1, 0, 0, RATE, None, None, None)
    assert assemble([chunk], [{"text": "", "segments": []}])["text"] == ""
    path = tmp_path / "response.json"
    first = publish_checkpoint(path, {"text": "test"})
    assert publish_checkpoint(path, {"text": "test"}) == first
    assert read_json(path) == {"text": "test"}
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(PipelineError, match="checkpoint_invalid"):
        publish_checkpoint(path, {"text": "changed"})
    txt = tmp_path / "result.txt"
    publish_bytes(txt, b"")
    publish_bytes(txt, b"")
    assert txt.read_bytes() == b""


def test_deep_checkpoint_is_safe_failure(tmp_path):
    path = tmp_path / "checkpoint.json"
    depth = sys.getrecursionlimit() + 100
    path.write_text('{"value":' + "[" * depth + "0" + "]" * depth + "}")
    with pytest.raises(PipelineError, match="checkpoint_invalid"):
        read_json(path)


async def test_extract_does_not_decode_input_after_requested_chunk(tmp_path, small_settings):
    source = tmp_path / "source.wav"
    wav(source, seconds=10)
    # First second is valid; the later WAV payload is truncated. Full decode with
    # -xerror fails; extracting a short first chunk must finish before that data.
    data = source.read_bytes()
    source.write_bytes(data[: 44 + RATE * 2 * 5 + 1])
    path = await extract(source, 0, ChunkPlan(0, RATE), tmp_path / "chunk.wav", small_settings)
    assert len(frames(path)) == RATE * 2


def test_silence_parser_ignores_metadata_and_invalid_numbers():
    assert (
        silence_ranges(
            "  title: silence_start: 1\n"
            "  title: silence_end: 2\n"
            "lavfi.silence_start=1e309\n"
            "lavfi.silence_end=--\n",
            4 * RATE,
        )
        == []
    )


def test_groq_surrogate_is_rejected_before_checkpoint_publication():
    from transcriptbot.media.artifacts import validate_response

    with pytest.raises(PipelineError, match="groq_invalid_response"):
        validate_response({"text": "\ud800"})


async def test_input_metadata_cannot_inject_pause_boundaries(tmp_path, small_settings):
    original, source = tmp_path / "original.wav", tmp_path / "source.wav"
    wav(original)
    title = (
        "\n[silencedetect @ 0x1] silence_start: 1.55\n"
        "[silencedetect @ 0x1] silence_end: 1.95\n"
        "lavfi.silence_start=1.55\nlavfi.silence_end=1.95"
    )
    ffmpeg("-i", original, "-metadata", "title=" + title, source)
    chunks = await analyze(source, 0, small_settings)
    assert chunks[0].end_sample == 2 * RATE
