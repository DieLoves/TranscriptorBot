import asyncio
import json
import sqlite3
import sys
import wave
from pathlib import Path

import pytest
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiohttp import web

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.files import reserve_disk
from transcriptbot.downloaders.service import SourceDownloader
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.tools import run_tool
from transcriptbot.operations.backup import copy_database
from transcriptbot.operations.cleanup import Cleanup
from transcriptbot.operations.files import UnsafeCleanupPath, unlink_file
from transcriptbot.operations.health import diagnostics, heartbeat
from transcriptbot.storage.database import Database, StorageError
from transcriptbot.storage.models import Phase, SourceKind, SourceRef
from transcriptbot.storage.repository import Repository
from transcriptbot.telegram.store import TelegramStore
from transcriptbot.worker import process_job


@pytest.fixture
async def repository(settings):
    settings.work_dir.mkdir(parents=True)
    settings.telegram_files_dir.mkdir()
    database = await Database(settings.sqlite_path).open(migrate=True)
    yield Repository(database, clock=lambda: 1000)
    await database.close()


async def create(repository, settings, *, update=1, telegram=False):
    result = await repository.create_job(
        update_id=update,
        owner_id=123,
        chat_id=123,
        message_id=update,
        source=SourceRef(
            SourceKind.TELEGRAM if telegram else SourceKind.DIRECT,
            "same-file-id" if telegram else "https://example.com/a",
        ),
    )
    directory = settings.work_dir / str(result.job_id)
    directory.mkdir()
    for name in (
        "source.media",
        "source.json",
        "current.wav",
        "response-0000.json",
        "result.txt",
        "result.verbose.json",
        "result.txt.part",
    ):
        path = directory / name
        path.write_text("private content")
        await repository.track_asset(result.job_id, kind="workspace", path=str(path))
    return result.job_id, directory


async def cancel(repository, job_id, update=100):
    result = await repository.cancel(update_id=update, job_id=job_id, owner_id=123, generation=0)
    assert result.outcome == "cancelled"


async def test_completion_cleanup_keeps_history_and_delivery_markers(repository, settings):
    job_id, directory = await create(repository, settings)
    job = await repository.claim_next()
    for phase in (Phase.PREPARE, Phase.TRANSCRIBE, Phase.ASSEMBLE, Phase.DELIVER):
        job = await repository.advance(job, phase)
    store = TelegramStore(repository)
    await store.enqueue_results(
        job, txt=directory / "result.txt", verbose_json=directory / "result.verbose.json"
    )
    for index, kind in enumerate(("preview", "txt", "json")):
        operation = await repository.database.call(
            lambda c, kind=kind: dict(
                c.execute("SELECT * FROM outbox WHERE operation = ?", (kind,)).fetchone()
            )
        )
        await store.confirm(operation, 100 + index)
    await store.refresh_statuses()
    assert (await repository.get_job(job_id, owner_id=123)).status == "completed"
    await Cleanup(settings, repository).run(startup=True)
    assert not directory.exists()
    job = await repository.get_job(job_id, owner_id=123)
    assert job.status == "completed" and not job.cleanup_pending
    markers = await repository.database.call(
        lambda c: list(
            c.execute(
                "SELECT status, telegram_message_id, artifact_path FROM outbox "
                "WHERE operation IN ('preview', 'txt', 'json')"
            )
        )
    )
    assert all(row[0] == "sent" and row[1] and row[2] is None for row in markers)
    assert await repository.job_assets(job_id) == []


async def test_failed_retention_and_expiry_disable_resume_before_unlink(repository, settings):
    job_id, directory = await create(repository, settings)
    job = await repository.claim_next()
    failed = await repository.fail(job, error_code="groq_network")
    cleanup = Cleanup(settings, repository)
    await cleanup.run()
    assert directory.exists()
    repository.clock = lambda: failed.retention_until
    await cleanup.run()
    current = await repository.get_job(job_id, owner_id=123)
    assert current.status == "failed" and not current.resumable and not current.cleanup_pending
    assert not directory.exists()
    assert (
        await repository.resume(update_id=2, job_id=job_id, owner_id=123, generation=0)
    ).outcome == "unavailable"


async def test_cleanup_unlink_failure_retries_after_restart_without_delivery(
    repository, settings, monkeypatch
):
    job_id, directory = await create(repository, settings)
    await cancel(repository, job_id)
    import transcriptbot.operations.cleanup as module

    original = module.unlink_file
    monkeypatch.setattr(
        module, "unlink_file", lambda *a, **k: (_ for _ in ()).throw(PermissionError())
    )
    await Cleanup(settings, repository).run()
    assert directory.exists()
    assert (await repository.get_job(job_id, owner_id=123)).cleanup_pending
    assert (await diagnostics(settings, repository, "worker"))["cleanup_errors"] == 1
    monkeypatch.setattr(module, "unlink_file", original)
    await Cleanup(settings, Repository(repository.database, clock=repository.clock)).run(
        startup=True
    )
    assert not directory.exists()
    assert not (await repository.get_job(job_id, owner_id=123)).cleanup_pending
    assert (await diagnostics(settings, repository, "worker"))["cleanup_errors"] == 0


async def test_shared_cache_pins_queued_same_file_and_keeps_service_data(repository, settings):
    first, _ = await create(repository, settings, telegram=True)
    second, _ = await create(repository, settings, update=2, telegram=True)
    cache = settings.telegram_files_dir / "12345:test" / "music" / "file_1.wav"
    cache.parent.mkdir(parents=True)
    cache.write_bytes(b"audio")
    service = cache.parent.parent / "td.binlog"
    service.write_bytes(b"database")
    await repository.track_asset(first, kind="telegram_cache", path=str(cache))
    await cancel(repository, first)
    await Cleanup(settings, repository).run()
    assert cache.exists() and service.exists()
    assert any(a["path"] == str(cache) for a in await repository.job_assets(second))
    await cancel(repository, second, 101)
    await Cleanup(settings, repository).run()
    assert not cache.exists() and service.read_bytes() == b"database"


@pytest.mark.parametrize("kind", ["outside", "symlink", "parent_symlink", "directory", "service"])
async def test_cleanup_rejects_unsafe_assets(repository, settings, tmp_path, kind):
    job_id, directory = await create(repository, settings)
    outside = tmp_path / "keep.wav"
    outside.write_bytes(b"keep")
    asset_kind = "workspace"
    if kind == "outside":
        path = outside
    elif kind == "symlink":
        path = directory / "link"
        path.symlink_to(outside)
    elif kind == "parent_symlink":
        (directory / "linkdir").symlink_to(tmp_path, target_is_directory=True)
        path = directory / "linkdir" / outside.name
    elif kind == "directory":
        path = directory / "nested"
        path.mkdir()
    else:
        asset_kind = "telegram_cache"
        path = settings.telegram_files_dir / "td.binlog"
        path.write_bytes(b"db")
    await repository.track_asset(job_id, kind=asset_kind, path=str(path))
    await cancel(repository, job_id)
    await Cleanup(settings, repository).run()
    assert outside.read_bytes() == b"keep"
    assert (await repository.get_job(job_id, owner_id=123)).cleanup_pending
    if kind == "service":
        assert path.read_bytes() == b"db"


async def test_startup_preserves_published_response_and_removes_only_temporaries(
    repository, settings
):
    job_id, directory = await create(repository, settings)
    untracked = settings.work_dir / "unknown"
    untracked.mkdir()
    (untracked / "x.part").write_bytes(b"keep")
    await Cleanup(settings, repository).run(startup=True)
    assert (directory / "response-0000.json").exists()
    assert not (directory / "result.txt.part").exists()
    assert not (directory / "current.wav").exists()
    assert (untracked / "x.part").exists()
    assert (await repository.get_job(job_id, owner_id=123)).status == "queued"


async def test_startup_missing_delivery_files_fails_without_new_recognition(repository, settings):
    job_id, directory = await create(repository, settings)
    job = await repository.claim_next()
    for phase in (Phase.PREPARE, Phase.TRANSCRIBE, Phase.ASSEMBLE, Phase.DELIVER):
        job = await repository.advance(job, phase)
    (directory / "result.txt").unlink()
    await Cleanup(settings, repository).run(startup=True)
    current = await repository.get_job(job_id, owner_id=123)
    assert current.status == "failed" and current.phase == "deliver"
    assert current.error_code == "recovery_files"


async def test_cancel_during_operation_waits_for_stop_before_cleanup(repository, settings):
    job_id, directory = await create(repository, settings)
    job = await repository.claim_next()
    started, stopped = asyncio.Event(), asyncio.Event()

    class Pipeline:
        async def step(self, job):
            started.set()
            try:
                await asyncio.Future()
            finally:
                await asyncio.sleep(0.05)
                assert directory.exists()
                stopped.set()

    task = asyncio.create_task(process_job(job, Pipeline(), repository, asyncio.Event()))
    await started.wait()
    await cancel(repository, job_id)
    await task
    assert stopped.is_set()
    await Cleanup(settings, repository).run()
    assert not directory.exists()


async def test_health_diagnostics_stale_and_wait_metadata(repository, settings):
    await create(repository, settings)
    job = await repository.claim_next()
    await repository.defer(job, until=2000, error_code="groq_rate_limit", quota=True)
    task = asyncio.create_task(heartbeat(repository, "worker"))
    try:
        for _ in range(100):
            result = await diagnostics(settings, repository, "worker")
            if result["healthy"]:
                break
            await asyncio.sleep(0.01)
        assert result["healthy"] and result["queue_size"] == 1
        assert result["current_job"]["wait_remaining_seconds"] == 1000
        assert "private content" not in json.dumps(result)
        repository.clock = lambda: 1061
        assert not (await diagnostics(settings, repository, "worker"))["healthy"]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_backup_restores_wal_data_and_refuses_overwrite(repository, settings, tmp_path):
    job_id, _ = await create(repository, settings)
    backup = tmp_path / "backup" / "snapshot.sqlite3"
    copy_database(settings.sqlite_path, backup)
    assert backup.stat().st_mode & 0o777 == 0o600
    restored_path = tmp_path / "restored" / "db.sqlite3"
    copy_database(backup, restored_path)
    restored = await Database(restored_path).open()
    try:
        job = await Repository(restored).get_job(job_id, owner_id=123)
        assert job.source_ref == "https://example.com/a" and job.status == "queued"
    finally:
        await restored.close()
    original = backup.read_bytes()
    with pytest.raises(StorageError):
        copy_database(settings.sqlite_path, backup)
    assert backup.read_bytes() == original


@pytest.mark.parametrize("kind", ["invalid", "foreign_keys", "schema", "symlink", "sidecar"])
def test_restore_rejects_invalid_database(tmp_path, kind):
    source, target = tmp_path / "source.sqlite3", tmp_path / "new.sqlite3"
    if kind == "invalid":
        source.write_bytes(b"not a database")
    elif kind == "symlink":
        real = tmp_path / "real"
        real.write_bytes(b"preserved")
        source.symlink_to(real)
    elif kind == "sidecar":
        source.write_bytes(b"bad")
        Path(str(target) + "-wal").write_bytes(b"preserved")
    else:
        with sqlite3.connect(source) as c:
            c.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            c.execute("CREATE TABLE child (pid INTEGER REFERENCES parent(id))")
            if kind == "foreign_keys":
                c.execute("INSERT INTO child VALUES (1)")
    with pytest.raises(StorageError):
        copy_database(source, target)
    assert not target.exists()
    assert not list(tmp_path.glob(".sqlite-backup-*"))


def test_low_disk_refuses_download_without_deleting_retained_data(settings, monkeypatch, tmp_path):
    import shutil

    retained = tmp_path / "retained"
    retained.write_bytes(b"keep")
    monkeypatch.setattr(shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100, 99, 1))
    with pytest.raises(DownloadError, match="disk_space"):
        reserve_disk(tmp_path, settings.max_input_bytes, settings.disk_reserve_bytes)
    assert retained.read_bytes() == b"keep"


async def test_http_getfile_downloads_again_after_cache_cleanup(repository, settings):
    cache = settings.telegram_files_dir / "12345:test" / "music" / "file_1.wav"
    cache.parent.mkdir(parents=True)
    calls = []

    async def get_file(request):
        calls.append(dict(await request.post()))
        if not cache.exists():
            with wave.open(str(cache), "wb") as audio:
                audio.setparams((1, 2, 16000, 0, "NONE", "none"))
                audio.writeframes(b"\0\0" * 16000)
        return web.json_response(
            {
                "ok": True,
                "result": {
                    "file_id": "same-file-id",
                    "file_unique_id": "same",
                    "file_path": str(cache),
                    "file_size": cache.stat().st_size,
                },
            }
        )

    app = web.Application()
    app.router.add_post("/bot12345:test/getFile", get_file)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    session = AiohttpSession(
        api=TelegramAPIServer.from_base(f"http://127.0.0.1:{port}", is_local=True)
    )
    try:
        async with Bot("12345:test", session=session) as bot:
            downloader = SourceDownloader(settings, repository, bot)
            hashes = []
            for update in (1, 2):
                result = await repository.create_job(
                    update_id=update,
                    owner_id=123,
                    chat_id=123,
                    message_id=update,
                    source=SourceRef(SourceKind.TELEGRAM, "same-file-id"),
                )
                job = await repository.claim_next()
                assert job.id == result.job_id
                hashes.append((await downloader.acquire(job))["sha256"])
                await cancel(repository, job.id, 100 + update)
                await Cleanup(settings, repository).run()
                assert not cache.exists()
            assert len(calls) == 2 and hashes[0] == hashes[1]
    finally:
        await runner.cleanup()


def test_descriptor_unlink_rejects_dotdot_and_root(tmp_path):
    for path in (tmp_path, tmp_path / ".." / "keep"):
        with pytest.raises(UnsafeCleanupPath):
            unlink_file(path, tmp_path)


async def test_cleanup_restart_after_unlink_before_sqlite_confirmation(
    repository, settings, monkeypatch
):
    job_id, directory = await create(repository, settings)
    await cancel(repository, job_id)
    original = repository.finish_cleanup

    async def crash(job):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(repository, "finish_cleanup", crash)
    with pytest.raises(RuntimeError, match="simulated crash"):
        await Cleanup(settings, repository).run()
    assert not directory.exists()
    assert (await repository.get_job(job_id, owner_id=123)).cleanup_pending
    monkeypatch.setattr(repository, "finish_cleanup", original)
    await Cleanup(settings, repository).run(startup=True)
    assert not (await repository.get_job(job_id, owner_id=123)).cleanup_pending


async def test_ttl_scanning_continues_while_another_operation_is_busy(repository, settings):
    _failed_id, failed_directory = await create(repository, settings)
    failed = await repository.fail(await repository.claim_next(), error_code="groq_network")
    active_id, active_directory = await create(repository, settings, update=2)
    await repository.claim_next()
    cleanup = Cleanup(settings, repository)
    cleanup.busy.add(active_id)
    repository.clock = lambda: failed.retention_until + 1
    await cleanup.run()
    assert not failed_directory.exists()
    assert active_directory.exists()
    await cancel(repository, active_id)
    await cleanup.run()
    assert active_directory.exists()
    cleanup.busy.clear()
    await cleanup.run()
    assert not active_directory.exists()


async def test_cache_removal_defers_until_active_io_stops(repository, settings):
    first, directory = await create(repository, settings, telegram=True)
    second, _ = await create(repository, settings, update=2)
    cache = settings.telegram_files_dir / "music" / "source.wav"
    cache.parent.mkdir()
    cache.write_bytes(b"audio")
    await repository.track_asset(first, kind="telegram_cache", path=str(cache))
    await cancel(repository, first)
    cleanup = Cleanup(settings, repository)
    cleanup.busy.add(second)
    cleanup.cache_busy.add(second)
    await cleanup.run()
    assert cache.exists() and directory.exists()
    cleanup.busy.clear()
    cleanup.cache_busy.clear()
    await cleanup.run()
    assert not cache.exists() and not directory.exists()


async def test_unrelated_recognition_does_not_delay_completed_cache_cleanup(repository, settings):
    first, directory = await create(repository, settings, telegram=True)
    second, _ = await create(repository, settings, update=2)
    cache = settings.telegram_files_dir / "music" / "source.wav"
    cache.parent.mkdir()
    cache.write_bytes(b"audio")
    await repository.track_asset(first, kind="telegram_cache", path=str(cache))
    await cancel(repository, first)
    cleanup = Cleanup(settings, repository)
    cleanup.busy.add(second)
    await cleanup.run()
    assert not cache.exists() and not directory.exists()


async def test_failed_media_tool_drains_full_pipes_before_returning():
    # Overflow a pipe before exiting: waiting without draining can hang after the group is killed.
    code = "import os; data=b'x'*65536; [os.write(1, data) for _ in range(1024)]"
    with pytest.raises(PipelineError, match="invalid_media"):
        await asyncio.wait_for(run_tool([sys.executable, "-c", code], timeout=10), timeout=5)


async def test_worker_cleans_expired_job_during_long_download(repository, settings, monkeypatch):
    import transcriptbot.worker as worker

    _, failed_directory = await create(repository, settings)
    failed = await repository.fail(await repository.claim_next(), error_code="download_network")
    active_id, active_directory = await create(repository, settings, update=2)
    started, stopped, shutdown = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Downloader:
        def __init__(self, supplied_settings, *args):
            self.settings = supplied_settings

        async def acquire(self, job):
            started.set()
            try:
                await asyncio.Future()
            finally:
                assert active_directory.exists()
                stopped.set()

    monkeypatch.setattr(worker, "SourceDownloader", Downloader)
    task = asyncio.create_task(worker.run(settings, shutdown, repository))
    try:
        await asyncio.wait_for(started.wait(), 5)
        assert failed_directory.exists()
        repository.clock = lambda: failed.retention_until + 1
        async with asyncio.timeout(5):
            while failed_directory.exists():
                await asyncio.sleep(0.05)
        assert active_directory.exists()
        await cancel(repository, active_id)
        await asyncio.wait_for(stopped.wait(), 5)
        async with asyncio.timeout(5):
            while active_directory.exists():
                await asyncio.sleep(0.05)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, 5)
