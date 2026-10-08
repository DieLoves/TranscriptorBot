"""Private process protocol; stdin contains metadata, stdout contains only a safe result."""

import json
import logging
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.files import job_directory, reserve_disk, source_manifest
from transcriptbot.downloaders.http_sources import download, mailru_download
from transcriptbot.downloaders.network import SafeHTTP


def execute(payload: dict) -> dict:
    directory = job_directory(Path(payload["work_dir"]), payload["job_id"])
    manifest = directory / "source.json"
    source = Path(payload["path"]) if payload["kind"] == "telegram" else directory / "source.media"
    if manifest.exists() or source.exists() or source.is_symlink():
        return source_manifest(directory, payload, source)
    reserve_disk(directory, payload["maximum"], payload["reserve"])
    http = SafeHTTP(
        connect=payload["connect"], read=payload["read"], redirects=payload["redirects"]
    )
    try:
        if payload["kind"] == "direct":
            source = download(http, payload["reference"], directory, payload["maximum"])
            payload["title"] = payload.get("title") or (
                unquote(urlsplit(payload["reference"]).path.rsplit("/", 1)[-1])[:256] or "source"
            )
        elif payload["kind"] == "mailru":
            source = mailru_download(http, payload["reference"], directory, payload["maximum"])
            payload["title"] = (
                payload.get("title")
                or unquote(urlsplit(payload["reference"]).path.rsplit("/", 1)[-1])[:256]
            )
        elif payload["kind"] == "youtube":
            from transcriptbot.downloaders.youtube import youtube_download

            source, payload["title"] = youtube_download(
                http, payload["reference"], directory, payload["maximum"], payload["duration"]
            )
        else:
            raise DownloadError("source_unavailable")
        return source_manifest(directory, payload, source)
    finally:
        http.close()


def main() -> None:
    logging.disable(logging.CRITICAL)
    try:
        payload = json.loads(sys.stdin.buffer.read(65537))
        execute(payload)
        result = {"ok": True}
    except DownloadError as exc:
        result = {
            "ok": False,
            "code": exc.code,
            "retryable": exc.retryable,
            "retry_after": exc.retry_after,
        }
    except OSError:
        result = {"ok": False, "code": "download_io", "retryable": True}
    except Exception:
        result = {"ok": False, "code": "invalid_response", "retryable": False}
    print(json.dumps(result))


if __name__ == "__main__":
    main()
