"""Direct streams and the public Mail.ru dispatcher, without third-party guest APIs."""

import json
import os
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import httpx

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.files import fsync_directory
from transcriptbot.downloaders.network import SafeHTTP, check_status, response_bytes, validate_url


def download(
    http: SafeHTTP, url: str, directory: Path, maximum: int, *, mailru=False, headers=None
):
    target, temporary = directory / "source.media", directory / "source.part"
    if target.is_symlink() or temporary.is_symlink():
        raise DownloadError("unsafe_path")
    temporary.unlink(missing_ok=True)
    response = http.open(url, headers=headers, mailru=mailru)
    try:
        check_status(response)
        length = response.headers.get("content-length")
        if length is not None:
            try:
                known = int(length)
                if known < 0:
                    raise ValueError
            except ValueError:
                raise DownloadError("invalid_response") from None
            if known > maximum:
                raise DownloadError("size_limit")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            size = 0
            for block in response.iter_raw(64 * 1024):
                size += len(block)
                if size > maximum:
                    raise DownloadError("size_limit")
                stream.write(block)
            if not size:
                raise DownloadError("invalid_media")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        fsync_directory(directory)
        return target
    except httpx.HTTPError:
        raise DownloadError("download_network", retryable=True) from None
    finally:
        response.close()
        temporary.unlink(missing_ok=True)


def mailru_path(url: str) -> str:
    validate_url(url, mailru=True)
    parsed = urlsplit(url)
    if parsed.hostname != "cloud.mail.ru" or not parsed.path.startswith("/public/"):
        raise DownloadError("unsafe_url")
    # Decode exactly once, preserve all nested segments, then encode exactly once.
    value = unquote(parsed.path[len("/public/") :], errors="strict")
    parts = value.split("/")
    if len(parts) < 2 or any(part in {"", ".", ".."} for part in parts):
        raise DownloadError("unsafe_url")
    if any(ord(c) < 32 or c == "\\" for c in value):
        raise DownloadError("unsafe_url")
    return value


def mailru_download(http: SafeHTTP, url: str, directory: Path, maximum: int):
    path = mailru_path(url)
    response = http.open("https://cloud.mail.ru/api/v2/dispatcher?api=2", mailru=True)
    try:
        check_status(response)
        try:
            body = json.loads(response_bytes(response, 1024 * 1024))
            server = body["body"]["weblink_get"][0]["url"]
            validate_url(server, mailru=True)
            parsed = urlsplit(server)
            if parsed.query or parsed.fragment:
                raise ValueError
        except (ValueError, KeyError, IndexError, TypeError):
            raise DownloadError("invalid_response") from None
    finally:
        response.close()
    return download(
        http,
        server.rstrip("/") + "/" + quote(path, safe="/"),
        directory,
        maximum,
        mailru=True,
        headers={"Referer": str(httpx.URL(url))},
    )
