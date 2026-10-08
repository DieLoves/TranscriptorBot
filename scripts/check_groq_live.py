"""Explicit one-request Groq gate using locally synthesized speech and temporary data."""

import asyncio
import hashlib
import json
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from transcriptbot.config import Settings, load_settings
from transcriptbot.groq_adapter import GroqAdapter
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.planning import RATE
from transcriptbot.media.tools import analyze, extract


async def main() -> int:
    settings = load_settings()
    with TemporaryDirectory(prefix="transcriptbot-groq-live-") as temporary:
        root = Path(temporary)
        settings = Settings(
            _env_file=None,
            **{
                **settings.model_dump(),
                "sqlite_path": root / "data" / "db.sqlite3",
                "work_dir": root / "jobs",
                "telegram_files_dir": root / "telegram",
            },
        )
        source = root / "speech.wav"
        try:
            subprocess.run(
                [
                    "ffmpeg",
                    "-nostdin",
                    "-v",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "flite=text='This is a short synthetic recording "
                    "for the transcription bot test.':voice=slt",
                    "-ar",
                    "16000",
                    "-ac",
                    "1",
                    "-c:a",
                    "pcm_s16le",
                    str(source),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                env={"PATH": os.environ.get("PATH", ""), "LANG": "C.UTF-8"},
            )
            chunks = await analyze(source, 0, settings)
            assert len(chunks) == 1
            upload = await extract(source, 0, chunks[0], root / "current.wav", settings)
            groq = GroqAdapter(settings)
            try:
                raw = await groq.transcribe(upload)
            finally:
                await groq.close()
            assert raw["text"].strip() and raw.get("segments")
            print(
                json.dumps(
                    {
                        "ok": True,
                        "requests": 1,
                        "model": settings.groq_model,
                        "duration_seconds": chunks[0].end_sample / RATE,
                        "upload_bytes": upload.stat().st_size,
                        "text_chars": len(raw["text"]),
                        "segments": len(raw["segments"]),
                        "language": raw.get("language"),
                        "response_sha256": hashlib.sha256(
                            json.dumps(raw, sort_keys=True).encode()
                        ).hexdigest(),
                    }
                )
            )
            return 0
        except PipelineError as exc:
            print(json.dumps({"ok": False, "error_code": exc.code}))
        except (OSError, subprocess.SubprocessError, AssertionError):
            print(json.dumps({"ok": False, "error_code": "live_gate_failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
