from pathlib import Path

import httpx
import pytest
from yt_dlp.downloader.http import HttpFD
from yt_dlp.networking.common import Request
from yt_dlp.networking.exceptions import NoSupportingHandlers, RequestError

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.network import SafeHTTP
from transcriptbot.downloaders.youtube import (
    ProtectedRH,
    ProtectedYoutubeDL,
    QuietLogger,
    youtube_download,
)


class Blocks(httpx.SyncByteStream):
    def __init__(self, *blocks):
        self.blocks = blocks

    def __iter__(self):
        yield from self.blocks


def info(**changes):
    return {
        "id": "BaW_jenozKc",
        "title": "test video",
        "duration": 1,
        "protocol": "https",
        "vcodec": "none",
        "acodec": "opus",
        "url": "https://example.com/audio",
        "ext": "webm",
        **changes,
    }


@pytest.mark.parametrize(
    "url",
    [
        "https://www.youtube.com/playlist?list=123",
        "https://www.youtube.com/watch?v=a&list=b",
        "https://example.com/video",
    ],
)
def test_playlist_or_non_youtube_rejected(tmp_path, url):
    with pytest.raises(DownloadError, match="unsupported_source"):
        youtube_download(
            SafeHTTP(transport=httpx.MockTransport(lambda r: pytest.fail())), url, tmp_path, 100, 60
        )


def test_sole_handler_no_fallback_or_proxy(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    http = SafeHTTP(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=Blocks(b"ok")))
    )
    with ProtectedYoutubeDL(http, 100, {"quiet": True, "logger": QuietLogger()}) as ydl:
        director = ydl._request_director
        assert list(director.handlers) == ["Protected"]
        assert ydl.urlopen(Request("https://example.com")).read() == b"ok"
        with pytest.raises(NoSupportingHandlers):
            ydl.urlopen(Request("file:///etc/passwd"))
        with pytest.raises(NoSupportingHandlers):
            ydl.urlopen(Request("https://example.com", proxies={"https": "http://localhost:1"}))
        with pytest.raises(RequestError):
            ydl.urlopen(Request("https://example.com", extensions={"impersonate": "chrome"}))


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"is_live": True}, "unsupported_source"),
        ({"live_status": "is_upcoming"}, "unsupported_source"),
        ({"availability": "needs_auth"}, "unsupported_source"),
        ({"_type": "playlist"}, "unsupported_source"),
        ({"protocol": "m3u8_native"}, "unsupported_source"),
        ({"protocol": "http_dash_segments"}, "unsupported_source"),
        ({"vcodec": "h264"}, "unsupported_source"),
        ({"duration": 100}, "duration_limit"),
        ({"duration": float("inf")}, "invalid_response"),
        ({"filesize": 101}, "size_limit"),
    ],
)
def test_metadata_rejections_precede_download(tmp_path, monkeypatch, changes, code):
    monkeypatch.setattr(ProtectedYoutubeDL, "extract_info", lambda *a, **kw: info(**changes))
    monkeypatch.setattr(HttpFD, "download", lambda *a, **kw: pytest.fail("download must not run"))
    with pytest.raises(DownloadError, match=code):
        youtube_download(SafeHTTP(), "https://youtu.be/BaW_jenozKc", tmp_path, 100, 60)


def test_actual_builtin_http_downloader(tmp_path, monkeypatch):
    monkeypatch.setattr(ProtectedYoutubeDL, "extract_info", lambda *a, **kw: info())
    seen = []

    def respond(request):
        seen.append(request.url.host)
        return httpx.Response(200, headers={"Content-Length": "6"}, stream=Blocks(b"abc", b"def"))

    result, title = youtube_download(
        SafeHTTP(transport=httpx.MockTransport(respond)),
        "https://youtu.be/BaW_jenozKc",
        tmp_path,
        100,
        60,
    )
    assert result.read_bytes() == b"abcdef"
    assert title == "test video"
    assert seen == ["example.com"]
    assert not Path(str(result) + ".part").exists()
    assert result.stat().st_mode & 0o777 == 0o600


def test_youtube_actual_bytes_without_length(tmp_path, monkeypatch):
    monkeypatch.setattr(ProtectedYoutubeDL, "extract_info", lambda *a, **kw: info())
    http = SafeHTTP(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=Blocks(b"x" * 101)))
    )
    with pytest.raises(DownloadError, match="size_limit"):
        youtube_download(http, "https://youtu.be/BaW_jenozKc", tmp_path, 100, 60)
    assert not (tmp_path / "source.media").exists()


def test_youtube_cumulative_range_size_limit():
    http = SafeHTTP(
        transport=httpx.MockTransport(lambda r: httpx.Response(206, stream=Blocks(b"x" * 60)))
    )
    handler = ProtectedRH(http=http, maximum=100, logger=QuietLogger())
    handler.media = True
    response = handler.send(Request("https://example.com/audio"))
    assert len(response.read()) == 60
    response.close()
    response = handler.send(Request("https://example.com/audio"))
    with pytest.raises(RequestError, match="size_limit"):
        response.read()
    response.close()
    handler.close()


def test_youtube_error_does_not_leak_url(tmp_path, monkeypatch):
    monkeypatch.setattr(ProtectedYoutubeDL, "extract_info", lambda *a, **kw: info())
    http = SafeHTTP(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(429, headers={"Retry-After": "45"}, stream=Blocks(b"secret"))
        )
    )
    with pytest.raises(DownloadError) as error:
        youtube_download(http, "https://youtu.be/BaW_jenozKc", tmp_path, 100, 60)
    assert str(error.value) == "download_rate_limit"
    assert error.value.retry_after == 45


def test_metadata_response_limit():
    handler = ProtectedRH(
        http=SafeHTTP(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, stream=Blocks(b"x" * (8 * 1024 * 1024 + 1)))
            )
        ),
        maximum=100,
        logger=QuietLogger(),
    )
    response = handler.send(Request("https://example.com/player.js"))
    with pytest.raises(RequestError, match="response_limit"):
        response.read()
    response.close()
    handler.close()
