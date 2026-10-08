"""yt-dlp with exactly one network handler and its built-in HTTP downloader."""

import io
import math
import os
from pathlib import Path

import httpx
from yt_dlp import YoutubeDL
from yt_dlp.downloader.http import HttpFD
from yt_dlp.networking.common import RequestDirector, RequestHandler, Response
from yt_dlp.networking.exceptions import HTTPError, RequestError
from yt_dlp.utils import DownloadError as YtdlpError

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.files import fsync_directory
from transcriptbot.downloaders.network import SafeHTTP, validate_url
from transcriptbot.telegram.requests import url_source


class QuietLogger:
    failure_code = None

    def debug(self, *args, **kwargs):
        pass

    info = warning = stdout = debug

    def error(self, message, *args, **kwargs):
        # Classify known diagnostics; never retain or output the message/URLs.
        if "sign in" in str(message).lower() or "login required" in str(message).lower():
            self.failure_code = "source_authorization"
        elif "requested format is not available" in str(message).lower():
            self.failure_code = "youtube_format_unavailable"


class BoundedBody(io.RawIOBase):
    def __init__(self, response, maximum, handler):
        self.response, self.maximum, self.handler = response, maximum, handler
        self.iterator = response.iter_raw(64 * 1024)
        self.buffer = bytearray()
        self.count = 0

    def readable(self):
        return True

    def read(self, size=-1):
        try:
            while size is None or size < 0 or len(self.buffer) < size:
                try:
                    block = next(self.iterator)
                except StopIteration:
                    break
                self.count += len(block)
                if self.handler.media:
                    self.handler.downloaded += len(block)
                if self.count > self.maximum or (
                    self.handler.media and self.handler.downloaded > self.maximum
                ):
                    raise DownloadError("size_limit" if self.handler.media else "response_limit")
                self.buffer.extend(block)
            take = len(self.buffer) if size is None or size < 0 else min(size, len(self.buffer))
            result = bytes(self.buffer[:take])
            del self.buffer[:take]
            return result
        except (DownloadError, httpx.HTTPError) as exc:
            self.handler.failure = (
                exc
                if isinstance(exc, DownloadError)
                else DownloadError("download_network", retryable=True)
            )
            self.close()
            raise RequestError(self.handler.failure.code) from None

    def close(self):
        self.response.close()
        super().close()


class ProtectedRH(RequestHandler):
    _SUPPORTED_URL_SCHEMES = ("http", "https")
    _SUPPORTED_PROXY_SCHEMES = ()

    def __init__(self, *, http, maximum, **kwargs):
        super().__init__(**kwargs)
        self.http, self.maximum = http, maximum
        self.media = False
        self.downloaded = 0
        self.failure = None

    def _check_extensions(self, extensions):
        super()._check_extensions(extensions)
        for key in ("cookiejar", "timeout", "keep_header_casing"):
            extensions.pop(key, None)

    def _send(self, request):
        try:
            validate_url(request.url)
            response = self.http.open(
                request.url,
                method=request.method,
                headers=self._get_headers(request),
                content=request.data,
            )
            limit = self.maximum if self.media else 8 * 1024 * 1024
            length = response.headers.get("content-length")
            try:
                if length is not None:
                    if int(length) < 0:
                        raise ValueError
                    if int(length) > limit:
                        response.close()
                        raise DownloadError("size_limit" if self.media else "response_limit")
            except ValueError:
                response.close()
                raise DownloadError("invalid_response") from None
            adapted = Response(
                BoundedBody(response, limit, self),
                str(response.url),
                dict(response.headers),
                response.status_code,
            )
            if response.status_code >= 400:
                if response.status_code == 429 or response.status_code >= 500:
                    self.failure = DownloadError("download_server", retryable=True)
                    if response.status_code == 429:
                        from transcriptbot.downloaders.network import check_status

                        try:
                            check_status(response)
                        except DownloadError as exc:
                            self.failure = exc
                raise HTTPError(adapted)
            return adapted
        except DownloadError as exc:
            self.failure = exc
            raise RequestError(exc.code) from None

    def close(self):
        self.http.close()


class ProtectedYoutubeDL(YoutubeDL):
    def __init__(self, http, maximum, params):
        self.protected_http, self.maximum = http, maximum
        super().__init__(params)

    def build_request_director(self, handlers, preferences=None):
        director = RequestDirector(logger=QuietLogger())
        director.add_handler(
            ProtectedRH(
                http=self.protected_http,
                maximum=self.maximum,
                logger=QuietLogger(),
                headers=self.params["http_headers"],
                cookiejar=self.cookiejar,
            )
        )
        return director


def youtube_download(http: SafeHTTP, url: str, directory: Path, maximum: int, duration: float):
    source = url_source(url)
    if source is None or source.kind != "youtube":
        raise DownloadError("unsupported_source")
    target = directory / "source.media"
    partial = Path(str(target) + ".part")
    if target.is_symlink() or partial.is_symlink():
        raise DownloadError("unsafe_path")
    partial.unlink(missing_ok=True)
    diagnostics = QuietLogger()
    params = {
        "quiet": True,
        "no_warnings": True,
        "logger": diagnostics,
        "noprogress": True,
        "noplaylist": True,
        "cachedir": False,
        "proxy": "",
        "socket_timeout": 60,
        "retries": 0,
        "extractor_retries": 0,
        "fragment_retries": 0,
        "file_access_retries": 0,
        "continuedl": False,
        "overwrites": True,
        "js_runtimes": {"deno": {}},
        "remote_components": [],
        "format": "bestaudio[protocol=https]/bestaudio[protocol=http]",
        "postprocessors": [],
        "external_downloader": None,
        "max_filesize": maximum,
        "buffersize": 65536,
        "noresizebuffer": True,
    }
    with ProtectedYoutubeDL(http, maximum, params) as ydl:
        handler = ydl._request_director.handlers["Protected"]
        try:
            info = ydl.extract_info(url, download=False)
            if (
                not info
                or info.get("_type", "video") != "video"
                or info.get("is_live")
                or info.get("live_status") in {"is_live", "is_upcoming", "post_live"}
                or info.get("availability")
                in {"private", "premium_only", "subscriber_only", "needs_auth"}
            ):
                raise DownloadError("unsupported_source")
            known_duration = info.get("duration")
            if known_duration is not None:
                if not math.isfinite(float(known_duration)) or float(known_duration) < 0:
                    raise DownloadError("invalid_response")
                if float(known_duration) > duration:
                    raise DownloadError("duration_limit")
            if info.get("filesize") is not None and info["filesize"] > maximum:
                raise DownloadError("size_limit")
            if info.get("protocol") not in {"http", "https"} or info.get("vcodec") != "none":
                raise DownloadError("unsupported_source")
            validate_url(info["url"])
            handler.media = True
            # Call HttpFD directly: no HLS/DASH/FFmpeg/external downloader dispatch exists.
            downloader = HttpFD(ydl, params)
            success, _ = downloader.download(str(target), info)
            if not success or not target.is_file():
                raise handler.failure or DownloadError("source_unavailable")
            with target.open("rb") as stream:
                os.fsync(stream.fileno())
            target.chmod(0o600)
            fsync_directory(directory)
            return target, str(info.get("title") or "youtube")[:256]
        except (YtdlpError, RequestError, KeyError, ValueError, TypeError):
            raise handler.failure or DownloadError(
                diagnostics.failure_code or "source_unavailable"
            ) from None
        finally:
            partial.unlink(missing_ok=True)
