import asyncio
import json
import sys
import wave
from pathlib import Path

import pytest

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.service import child_process
from transcriptbot.storage.database import Database, StorageError
from transcriptbot.storage.models import SourceKind, SourceRef
from transcriptbot.storage.repository import Repository
from transcriptbot.worker import download_job


@pytest.fixture
async def repository(settings):
    database = await Database(settings.sqlite_path).open(migrate=True)
    yield Repository(database)
    await database.close()


async def create(repository, update_id=1):
    await repository.create_job(
        update_id=update_id,
        owner_id=123,
        chat_id=123,
        message_id=update_id,
        source=SourceRef(SourceKind.DIRECT, "https://example.com/audio"),
    )
    return await repository.claim_next()


def manifest(job):
    return {
        "job_id": job.id,
        "size_bytes": 10,
        "duration_seconds": 1,
        "title": "test",
        "sha256": "a" * 64,
        "path": "/tmp/source.media",
    }


async def test_download_failure_retry_fifo_and_exhaustion(repository, settings):
    job = await create(repository)
    await create(repository, 2)

    class Downloader:
        async def acquire(self, expected):
            raise DownloadError("download_network", retryable=True)

    downloader = Downloader()
    downloader.settings = settings
    await download_job(job, downloader, repository, asyncio.Event())
    waiting = await repository.active_job()
    assert waiting.id == job.id
    assert waiting.status == "retry_wait"
    assert waiting.attempts == 1
    assert await repository.claim_next() is None
    repository.clock = lambda: waiting.next_attempt_at + 1
    for attempt in range(1, settings.max_retries + 1):
        job = await repository.claim_next()
        await download_job(job, downloader, repository, asyncio.Event())
        failed = await repository.get_job(job.id, owner_id=123)
        if attempt < settings.max_retries:
            assert failed.status == "retry_wait"
            repository.clock = lambda deadline=failed.next_attempt_at: deadline + 1
    assert failed.status == "failed"
    assert failed.error_code == "download_network"
    assert (await repository.claim_next()).id == 2


async def test_unexpected_download_error_is_bounded_instead_of_crash_loop(
    repository, settings, caplog
):
    job = await create(repository)

    class Downloader:
        async def acquire(self, expected):
            raise KeyError("https://example.com/?token=secret")

    downloader = Downloader()
    downloader.settings = settings
    with caplog.at_level("INFO"):
        for attempt in range(settings.max_retries + 1):
            await download_job(job, downloader, repository, asyncio.Event())
            job = await repository.get_job(job.id, owner_id=123)
            assert job.error_code == "internal_error"
            if attempt < settings.max_retries:
                assert job.status == "retry_wait"
                repository.clock = lambda deadline=job.next_attempt_at: deadline + 1
                job = await repository.claim_next()
    assert job.status == "failed" and job.resumable
    assert "KeyError" in caplog.text and "secret" not in caplog.text


async def test_storage_failure_still_stops_worker(repository, settings):
    job = await create(repository)

    class Downloader:
        async def acquire(self, expected):
            raise StorageError("Ошибка доступа к SQLite")

    downloader = Downloader()
    downloader.settings = settings
    with pytest.raises(StorageError):
        await download_job(job, downloader, repository, asyncio.Event())
    assert (await repository.get_job(job.id, owner_id=123)).status == "active"


async def test_retry_after_survives_repository_restart(repository, settings):
    job = await create(repository)

    class Downloader:
        async def acquire(self, expected):
            raise DownloadError("download_rate_limit", retryable=True, retry_after=120)

    downloader = Downloader()
    downloader.settings = settings
    repository.clock = lambda: 1000
    await download_job(job, downloader, repository, asyncio.Event())
    recovered = await Repository(repository.database, clock=lambda: 1010).active_job()
    assert recovered.status == "retry_wait"
    assert recovered.next_attempt_at == 1120


@pytest.mark.parametrize("cancel", [True, False])
async def test_cancel_or_shutdown_stops_active_operation(repository, settings, cancel):
    job = await create(repository)
    started, stopped, shutdown = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Downloader:
        async def acquire(self, expected):
            started.set()
            try:
                await asyncio.Future()
            finally:
                stopped.set()

    downloader = Downloader()
    downloader.settings = settings
    operation = asyncio.create_task(download_job(job, downloader, repository, shutdown))
    await started.wait()
    if cancel:
        await repository.cancel(update_id=2, job_id=job.id, owner_id=123, generation=0)
    else:
        shutdown.set()
    await asyncio.wait_for(operation, 2)
    assert stopped.is_set()
    current = await repository.get_job(job.id, owner_id=123)
    assert current.phase == "download"
    assert current.status == ("cancelled" if cancel else "active")


async def test_prepare_blocks_following_fifo_until_stage_five(repository, settings):
    first = await create(repository)
    await create(repository, 2)

    class Downloader:
        async def acquire(self, expected):
            return manifest(expected)

    downloader = Downloader()
    downloader.settings = settings
    await download_job(first, downloader, repository, asyncio.Event())
    next_job = await repository.claim_next()
    assert next_job.id == first.id
    assert next_job.phase == "prepare"


@pytest.fixture
def child_payload(tmp_path):
    directory = tmp_path / "jobs" / "1"
    directory.mkdir(parents=True)
    with wave.open(str(directory / "source.media"), "wb") as output:
        output.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
        output.writeframes(b"\0\0" * 16000)
    return {
        "job_id": 1,
        "kind": "direct",
        "title": "test",
        "work_dir": str(tmp_path / "jobs"),
        "telegram_files_dir": str(tmp_path / "telegram"),
        "maximum": 100000,
        "duration": 10,
        "probe_timeout": 30,
    }


def sleeping_probe(tmp_path, monkeypatch):
    executable = tmp_path / "ffprobe"
    marker = tmp_path / "pids.json"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import os,time,json,subprocess,sys\n"
        "assert not any(k in os.environ for k in "
        '("GROQ_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_API_HASH"))\n'
        'child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"])\n'
        f'open({str(marker)!r},"w").write(json.dumps([os.getpid(),child.pid]))\n'
        "time.sleep(60)\n"
    )
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv("GROQ_API_KEY", "secret-test-do-not-pass")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-test-do-not-pass")
    return marker


def running(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(") ")[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


@pytest.mark.parametrize("timeout", [False, True])
async def test_process_group_cancellation_and_timeout(
    tmp_path, monkeypatch, child_payload, timeout
):
    marker = sleeping_probe(tmp_path, monkeypatch)
    operation = asyncio.create_task(child_process(child_payload, 2 if timeout else 30))
    async with asyncio.timeout(5):
        while not marker.exists():
            await asyncio.sleep(0.02)
    pids = json.loads(marker.read_text())
    if timeout:
        with pytest.raises(DownloadError, match="download_timeout"):
            await operation
    else:
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    async with asyncio.timeout(2):
        while any(running(pid) for pid in pids):
            await asyncio.sleep(0.02)
    assert not (tmp_path / "jobs" / "1" / "source.json").exists()
