"""Opt-in live source gate, synthetic settings, no Telegram/Groq requests or credentials."""

import argparse
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.service import child_process

MAILRU_URL = (
    "https://cloud.mail.ru/public/oedu/9Qx3xyVGv/3%20%D0%BC%D0%BE%D0%B4%D1%83%D0%BB%D1%8C/"
    "%D0%B2%D0%B2%D0%B5%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%B2%20%D0%BF%D1%80%D0%BE%D0%B4"
    "%D0%B0%D0%B6%D0%B8%20%5Bskladchik.org%5D.mp4"
)
SOURCES = {
    "direct": "https://download.samplelib.com/mp3/sample-3s.mp3",
    "mailru": MAILRU_URL,
    "youtube": "https://www.youtube.com/watch?v=jNQXAC9IVRw",
}


async def main(kinds):
    failed = False
    with TemporaryDirectory(prefix="transcriptbot-source-gate-") as temporary:
        root = Path(temporary)
        for job_id, kind in enumerate(kinds, 1):
            print(json.dumps({"source": kind, "event": "started"}), flush=True)
            payload = {
                "job_id": job_id,
                "kind": kind,
                "reference": SOURCES[kind],
                "title": kind,
                "work_dir": str(root / "jobs"),
                "telegram_files_dir": str(root / "telegram"),
                "maximum": 2_000_000_000,
                "duration": 14400,
                "reserve": 1_073_741_824,
                "connect": 10,
                "read": 60,
                "redirects": 5,
                "probe_timeout": 60,
            }
            try:
                await child_process(payload, 3600)
                result = json.loads((root / "jobs" / str(job_id) / "source.json").read_text())
                print(
                    json.dumps(
                        {
                            "source": kind,
                            "ok": True,
                            "size_bytes": result["size_bytes"],
                            "duration_seconds": result["duration_seconds"],
                            "sha256": result["sha256"],
                            "codec": result["codec"],
                            "audio_stream": result["audio_stream"],
                        }
                    ),
                    flush=True,
                )
            except DownloadError as exc:
                print(json.dumps({"source": kind, "ok": False, "error_code": exc.code}), flush=True)
                failed = True
    return int(failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", nargs="+", choices=tuple(SOURCES))
    raise SystemExit(asyncio.run(main(parser.parse_args().sources)))
