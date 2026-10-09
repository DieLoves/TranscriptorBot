"""Generate synthetic formats on the host; verify the installed media tools offline."""

import argparse
import asyncio
import math
import subprocess
import wave
from array import array
from pathlib import Path
from tempfile import TemporaryDirectory

FORMATS = ("mp3", "wav", "m4a", "ogg", "opus", "flac", "aac", "mp4", "mkv", "mov", "webm")


def command(*args: str) -> None:
    subprocess.run(
        ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y", *args],
        check=True,
        capture_output=True,
        timeout=30,
    )


def frames(path: Path) -> bytes:
    with wave.open(str(path), "rb") as audio:
        assert audio.getparams()[:3] == (1, 2, 16000)
        return audio.readframes(audio.getnframes())


def external_reference(data: bytes, reference: bytes) -> bytes:
    output = bytearray()
    position = 0
    while position < len(data):
        size = int.from_bytes(data[position : position + 4], "big")
        kind = data[position + 4 : position + 8]
        assert size >= 8
        payload = data[position + 8 : position + size]
        if kind in {b"moov", b"trak", b"mdia", b"minf", b"dinf"}:
            payload = external_reference(payload, reference)
        elif kind == b"dref":
            entry = b"\0\0\0\0" + reference + b"\0"
            payload = b"\0\0\0\0\0\0\0\1" + (len(entry) + 8).to_bytes(4, "big") + b"url " + entry
        output.extend((len(payload) + 8).to_bytes(4, "big") + kind + payload)
        position += size
    return bytes(output)


def generate(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    original = directory / "original.wav"
    samples = array(
        "h",
        (
            0 if 1.5 <= n / 44100 < 1.9 else round(8000 * math.sin(n * 2 * math.pi * 440 / 44100))
            for n in range(round(5.01 * 44100))
            for _ in range(2)
        ),
    )
    with wave.open(str(original), "wb") as audio:
        audio.setparams((2, 2, 44100, 0, "NONE", "none"))
        audio.writeframes(samples.tobytes())
    for suffix in FORMATS:
        command("-i", str(original), str(directory / f"source.{suffix}"))
    mov = directory / "reference.mov"
    command("-i", str(original), "-c:a", "pcm_s16le", str(mov))
    for name, reference in (
        ("local", b"/etc/passwd"),
        ("network", b"http://127.0.0.1:8081/private.wav"),
    ):
        (directory / f"external-{name}.mov").write_bytes(
            external_reference(mov.read_bytes(), reference)
        )
    command(
        "-i",
        str(original),
        "-metadata",
        "title=lavfi.silence_start=0\nlavfi.silence_end=5\n[ silencedetect ] silence_start: 0",
        str(directory / "metadata.mkv"),
    )
    silent = directory / "silent.wav"
    with wave.open(str(silent), "wb") as audio:
        audio.setparams((2, 2, 44100, 0, "NONE", "none"))
        audio.writeframes(b"\0\0" * 2 * round(5.01 * 44100))
    command(
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=16x16:d=5.01",
        "-i",
        str(original),
        "-i",
        str(silent),
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
        str(directory / "two-tracks.mkv"),
    )
    print(f"Generated {len(FORMATS)} formats, MOV references, hostile metadata and multiple tracks")


async def verify(directory: Path) -> None:
    from transcriptbot.config import Settings
    from transcriptbot.downloaders.files import probe
    from transcriptbot.media.errors import PipelineError
    from transcriptbot.media.tools import analyze, extract

    settings = Settings(_env_file=None, chunk_seconds=2, silence_window_seconds=0.5)
    with TemporaryDirectory(prefix="formats-") as temporary:
        root = Path(temporary)
        for filename in (
            *[f"source.{suffix}" for suffix in FORMATS],
            "metadata.mkv",
            "two-tracks.mkv",
        ):
            source = directory / filename
            stream = probe(source, timeout=10, maximum_duration=10)["audio_stream"]
            if filename == "two-tracks.mkv":
                assert stream == 1
            reference = root / "reference.wav"
            command(
                "-i",
                str(source),
                "-map",
                "0:a:0",
                "-ar",
                "16000",
                "-ac",
                "1",
                "-c:a",
                "pcm_s16le",
                str(reference),
            )
            chunks = await analyze(source, stream, settings)
            # Raw AAC retains its encoder delay (one 1024-sample frame at 44.1 kHz).
            assert abs(chunks[0].seconds - 1.7) < 0.04, (filename, chunks)
            joined = b""
            for chunk in chunks:
                joined += frames(
                    await extract(source, stream, chunk, root / "current.wav", settings)
                )
            assert joined == frames(reference), filename
        for kind in ("local", "network"):
            source = directory / f"external-{kind}.mov"
            try:
                chunks = await analyze(source, 0, settings)
                command(
                    "-i",
                    str(directory / "reference.mov"),
                    "-ar",
                    "16000",
                    "-ac",
                    "1",
                    "-c:a",
                    "pcm_s16le",
                    str(root / "reference.wav"),
                )
                joined = b""
                for chunk in chunks:
                    joined += frames(
                        await extract(source, 0, chunk, root / "current.wav", settings)
                    )
                assert joined == frames(root / "reference.wav")
            except PipelineError as error:
                assert error.code == "invalid_media"
        print(
            "FFmpeg: 11 supported formats, exact resampled coverage, "
            "pauses, first audio track, metadata and MOV drefs passed"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("generate", "verify"))
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.mode == "generate":
        generate(args.directory)
    else:
        asyncio.run(verify(args.directory))
