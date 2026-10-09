import json
import wave
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from aiogram.types import File

from transcriptbot.downloaders.child import execute
from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.files import (
    job_directory,
    probe,
    regular_path,
    reserve_disk,
    source_manifest,
)
from transcriptbot.downloaders.http_sources import download, mailru_download, mailru_path
from transcriptbot.downloaders.network import SafeHTTP
from transcriptbot.downloaders.service import SourceDownloader, child_process
from transcriptbot.downloaders.youtube import youtube_download
from transcriptbot.storage.database import Database
from transcriptbot.storage.models import SourceKind, SourceRef
from transcriptbot.storage.repository import Repository


class Blocks(httpx.SyncByteStream):
    def __init__(self, *blocks):
        self.blocks = blocks

    def __iter__(self):
        yield from self.blocks


def http_response(status=200, headers=None, blocks=(b"media",)):
    return httpx.Response(status, headers=headers, stream=Blocks(*blocks))


@pytest.mark.parametrize("host", ["youtube.com", "m.youtube.com"])
def test_youtube_playlist_context_normalized_and_playlist_result_rejected(
    tmp_path, monkeypatch, host
):
    seen = []

    def extract(url, download):
        seen.append(url)
        assert download is False
        return {"_type": "playlist"}

    def extractor(http, maximum, params):
        assert params["noplaylist"] is True
        return nullcontext(
            SimpleNamespace(
                _request_director=SimpleNamespace(handlers={"Protected": SimpleNamespace()}),
                extract_info=extract,
            )
        )

    monkeypatch.setattr("transcriptbot.downloaders.youtube.ProtectedYoutubeDL", extractor)
    with pytest.raises(DownloadError, match="unsupported_source"):
        youtube_download(
            None, f"https://{host}/watch?v=8B_GZk9lRY0&list=abc&index=16", tmp_path, 100, 60
        )
    assert seen == [f"https://{host}/watch?v=8B_GZk9lRY0"]


@pytest.mark.parametrize(
    "headers,blocks",
    [
        ({"Content-Length": "101"}, (b"x",)),
        ({}, (b"x" * 60, b"x" * 60)),
        ({"Content-Length": "50"}, (b"x" * 101,)),
    ],
)
def test_size_limit_known_or_streamed(tmp_path, headers, blocks):
    http = SafeHTTP(
        transport=httpx.MockTransport(lambda r: http_response(headers=headers, blocks=blocks))
    )
    with pytest.raises(DownloadError, match="size_limit"):
        download(http, "https://example.com/file", tmp_path, 100)
    assert not (tmp_path / "source.media").exists()
    assert not (tmp_path / "source.part").exists()
    http.close()


def test_atomic_publish_and_permissions(tmp_path):
    def blocks():
        assert (tmp_path / "source.part").exists()
        assert not (tmp_path / "source.media").exists()
        yield b"abc"
        assert not (tmp_path / "source.media").exists()
        yield b"def"

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            return blocks()

    http = SafeHTTP(transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=Stream())))
    path = download(http, "https://example.com/file", tmp_path, 100)
    assert path.read_bytes() == b"abcdef"
    assert path.stat().st_mode & 0o777 == 0o600
    assert not (tmp_path / "source.part").exists()
    http.close()


@pytest.mark.parametrize(
    "status,code,retryable",
    [
        (403, "source_unavailable", False),
        (404, "source_unavailable", False),
        (429, "download_rate_limit", True),
        (503, "download_server", True),
    ],
)
def test_typed_http_errors(tmp_path, status, code, retryable):
    http = SafeHTTP(transport=httpx.MockTransport(lambda r: http_response(status=status)))
    with pytest.raises(DownloadError) as error:
        download(http, "https://example.com/file", tmp_path, 100)
    assert error.value.code == code
    assert error.value.retryable == retryable
    http.close()


@pytest.mark.parametrize(
    "path",
    [
        "oedu/9Qx3xyVGv/3%20модуль/введение%20в%20продажи%20%5Bskladchik.org%5D.mp4",
        "oedu/9Qx3xyVGv/first/second/a%2520b.mp4",
    ],
)
def test_nested_mailru_dispatcher_and_encoding(tmp_path, path):
    seen = []

    def respond(request):
        seen.append(request)
        if request.url.path == "/api/v2/dispatcher":
            return http_response(
                blocks=(
                    json.dumps(
                        {
                            "body": {
                                "weblink_get": [{"url": "https://cloclo61.cloud.mail.ru/download/"}]
                            }
                        }
                    ).encode(),
                )
            )
        assert request.headers["referer"] == str(httpx.URL("https://cloud.mail.ru/public/" + path))
        return http_response()

    http = SafeHTTP(transport=httpx.MockTransport(respond))
    url = "https://cloud.mail.ru/public/" + path
    downloaded = mailru_download(http, url, tmp_path, 100)
    assert downloaded.read_bytes() == b"media"
    assert seen[1].url.path.startswith("/download/oedu/9Qx3xyVGv/")
    assert seen[1].url.path.endswith("a%20b.mp4" if "a%2520" in path else "[skladchik.org].mp4")
    http.close()


@pytest.mark.parametrize("path", ["a/b/../c", "a/b//c", "a/b/%2e%2e/c", "a/b/%00", "a"])
def test_bad_mailru_paths(path):
    with pytest.raises(DownloadError, match="unsafe_url"):
        mailru_path("https://cloud.mail.ru/public/" + path)


def test_mailru_rejects_untrusted_dispatcher(tmp_path):
    http = SafeHTTP(
        transport=httpx.MockTransport(
            lambda r: http_response(
                blocks=(b'{"body":{"weblink_get":[{"url":"https://evil.test/"}]}}',)
            )
        )
    )
    with pytest.raises(DownloadError, match="unsafe_url"):
        mailru_download(http, "https://cloud.mail.ru/public/a/b/file.mp4", tmp_path, 100)
    http.close()


def wav(path, seconds=1):
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as output:
        output.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
        output.writeframes(b"\x00\x00" * int(16000 * seconds))
    return path


@pytest.fixture
def payload(tmp_path):
    return {
        "job_id": 1,
        "kind": "direct",
        "reference": "https://example.com/a",
        "title": "audio",
        "work_dir": str(tmp_path / "jobs"),
        "telegram_files_dir": str(tmp_path / "telegram"),
        "maximum": 100000,
        "duration": 5,
        "reserve": 1,
        "connect": 1,
        "read": 1,
        "redirects": 2,
        "probe_timeout": 5,
    }


def test_source_checkpoint_restore_and_tampering(payload):
    directory = job_directory(Path(payload["work_dir"]), 1)
    path = wav(directory / "source.media")
    first = source_manifest(directory, payload, path)
    assert first["duration_seconds"] == 1
    assert first["audio_stream"] == 0
    assert len(first["sha256"]) == 64
    original = (directory / "source.json").read_bytes()
    assert execute(payload) == first
    assert (directory / "source.json").read_bytes() == original
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(DownloadError, match="source_changed"):
        execute(payload)


@pytest.mark.parametrize("kind", ["playlist", "corrupt", "too_long"])
def test_probe_rejects_invalid_or_oversized_media(tmp_path, kind):
    path = tmp_path / "media"
    if kind == "too_long":
        wav(path, 2)
    else:
        path.write_text("#EXTM3U\nhttp://127.0.0.1/private" if kind == "playlist" else "invalid")
    with pytest.raises(
        DownloadError, match="duration_limit" if kind == "too_long" else "invalid_media"
    ):
        probe(path, timeout=5, maximum_duration=1)


def test_symlink_and_job_directory_escape(tmp_path):
    directory = tmp_path / "jobs"
    directory.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    (directory / "1").symlink_to(other, target_is_directory=True)
    with pytest.raises(DownloadError, match="unsafe_path"):
        job_directory(directory, 1)
    file = other / "file"
    file.write_bytes(b"x")
    (directory / "file").symlink_to(file)
    with pytest.raises(DownloadError, match="unsafe_path"):
        regular_path(directory / "file", directory)
    with pytest.raises(DownloadError, match="unsafe_path"):
        regular_path(file, directory)


def test_disk_reservation(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.disk_usage", lambda p: SimpleNamespace(free=10))
    with pytest.raises(DownloadError, match="disk_space"):
        reserve_disk(tmp_path, 100, 1)


async def test_child_probes_local_source(payload):
    directory = job_directory(Path(payload["work_dir"]), 1)
    wav(directory / "source.media")
    await child_process(payload, 10)
    manifest = json.loads((directory / "source.json").read_text())
    assert manifest["codec"] == "pcm_s16le"
    assert manifest["size_bytes"] == 32044


@pytest.fixture
async def repository(settings):
    database = await Database(settings.sqlite_path).open(migrate=True)
    yield Repository(database)
    await database.close()


async def make_job(repository, kind=SourceKind.TELEGRAM):
    await repository.create_job(
        update_id=1,
        owner_id=123,
        chat_id=123,
        message_id=1,
        source=SourceRef(kind, "file-id", "test.wav"),
    )
    return await repository.claim_next()


async def test_telegram_uses_local_cache_and_tracks_asset(settings, repository):
    path = wav(settings.telegram_files_dir / "bot" / "voice" / "file.wav")

    class Bot:
        calls = 0

        async def get_file(self, reference, **kwargs):
            self.calls += 1
            assert reference == "file-id"
            return File(
                file_id="file-id", file_unique_id="unique", file_path=str(path), file_size=32044
            )

    bot = Bot()
    job = await make_job(repository)
    manifest = await SourceDownloader(settings, repository, bot).acquire(job)
    assert bot.calls == 1
    assert manifest["path"] == str(path)
    assert await SourceDownloader(settings, repository, bot).acquire(job) == manifest
    assert bot.calls == 1
    assert not (settings.work_dir / "1" / "source.media").exists()
    updated = await repository.finish_download(job, manifest)
    assert updated.phase == "prepare"
    assert updated.size_bytes == 32044
    assert updated.duration_seconds == 1
    rows = await repository.database.call(
        lambda c: [dict(r) for r in c.execute("SELECT * FROM assets WHERE kind='telegram_cache'")]
    )
    assert rows[0]["sha256"] == manifest["sha256"]
    assert rows[0]["path"] == str(path)


async def test_telegram_rejects_outside_cache(settings, repository, tmp_path):
    settings.telegram_files_dir.mkdir()
    path = wav(tmp_path / "outside.wav")

    class Bot:
        async def get_file(self, reference, **kwargs):
            return File(file_id="file-id", file_unique_id="unique", file_path=str(path))

    with pytest.raises(DownloadError, match="unsafe_path"):
        await SourceDownloader(settings, repository, Bot()).acquire(await make_job(repository))


async def test_cancelled_download_cannot_advance(repository):
    job = await make_job(repository)
    await repository.cancel(update_id=2, job_id=job.id, owner_id=123, generation=0)
    manifest = {
        "job_id": job.id,
        "size_bytes": 10,
        "duration_seconds": 1,
        "title": "x",
        "path": "/tmp/not-used",
        "sha256": "a" * 64,
    }
    assert await repository.finish_download(job, manifest) is None
    assert (await repository.get_job(job.id, owner_id=123)).status == "cancelled"


def test_probe_uses_first_audio_track(tmp_path):
    import subprocess

    path = tmp_path / "two-tracks.mkv"
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=8000:cl=mono",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=16000:cl=stereo",
            "-t",
            "1",
            "-map",
            "0:a",
            "-map",
            "1:a",
            "-c:a",
            "pcm_s16le",
            str(path),
        ],
        check=True,
    )
    result = probe(path, timeout=5, maximum_duration=10)
    assert result["sample_rate"] == 8000
    assert result["channels"] == 1
    assert result["audio_stream"] == 0
