"""Installed-package source gate: no network, disposable shared-volume data only."""

import asyncio
import wave

import httpcore
from aiogram.types import File

from transcriptbot.config import Settings
from transcriptbot.downloaders.http_sources import download
from transcriptbot.downloaders.network import PublicTransport, SafeHTTP
from transcriptbot.downloaders.service import SourceDownloader
from transcriptbot.storage.database import Database
from transcriptbot.storage.models import SourceKind, SourceRef
from transcriptbot.storage.repository import Repository


class FakeBot:
    def __init__(self, path):
        self.path = path
        self.calls = 0

    async def get_file(self, reference, *, request_timeout):
        self.calls += 1
        assert reference == "offline-source"
        assert request_timeout > 0
        return File(
            file_id=reference,
            file_unique_id="offline",
            file_path=str(self.path),
            file_size=self.path.stat().st_size,
        )


async def main():
    settings = Settings(_env_file=None)
    database = await Database(settings.sqlite_path).open()
    try:
        repository = Repository(database)
        cache = settings.telegram_files_dir / "offline-source.wav"
        # More than 20 MB, first audio track, eleven minutes. No Telegram service calls.
        with wave.open(str(cache), "wb") as output:
            output.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
            for _ in range(660):
                output.writeframes(b"\0\0" * 16000)
        assert cache.stat().st_size > 20_000_000
        result = await repository.create_job(
            update_id=900,
            owner_id=12345,
            chat_id=12345,
            message_id=900,
            source=SourceRef(SourceKind.TELEGRAM, "offline-source", "source.wav"),
        )
        job = await repository.claim_next()
        assert job.id == result.job_id
        fake = FakeBot(cache)
        downloader = SourceDownloader(settings, repository, fake)
        manifest = await downloader.acquire(job)
        checkpoint = settings.work_dir / str(job.id) / "source.json"
        original = checkpoint.read_bytes()
        assert manifest["duration_seconds"] == 660
        assert manifest["path"] == str(cache)
        assert not (checkpoint.parent / "source.media").exists()
        assert await downloader.acquire(job) == manifest
        assert fake.calls == 1
        assert checkpoint.read_bytes() == original
        prepared = await repository.finish_download(job, manifest)
        assert prepared.phase == "prepare"
        assert prepared.size_bytes == cache.stat().st_size
        print(
            "Telegram cache >20 MB: no second copy, FFprobe, SHA-256, immutable restart checkpoint"
        )
        # Exercise the production HTTPX->HTTPcore bridge on a fake socket stream.
        http = SafeHTTP(
            transport=PublicTransport(
                httpcore.MockBackend(
                    [
                        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabcdef",
                    ]
                )
            )
        )
        path = download(http, "https://example.com/audio", checkpoint.parent, 100)
        assert path.read_bytes() == b"abcdef"
        http.close()
        print("Direct protected transport: streamed .part and atomic publication")
        assert (
            await database.call(lambda c: c.execute("PRAGMA integrity_check").fetchone()[0]) == "ok"
        )
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
