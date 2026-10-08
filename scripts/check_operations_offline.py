"""Installed-package cleanup/re-download/backup gate on disposable files and fake HTTP API."""

import asyncio
import shutil
import wave
from pathlib import Path
from tempfile import TemporaryDirectory

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiohttp import web

from transcriptbot.config import Settings
from transcriptbot.downloaders.service import SourceDownloader
from transcriptbot.operations.backup import copy_database
from transcriptbot.operations.cleanup import Cleanup
from transcriptbot.operations.health import diagnostics, heartbeat
from transcriptbot.storage.database import Database
from transcriptbot.storage.models import SourceKind, SourceRef
from transcriptbot.storage.repository import Repository


async def main():
    with TemporaryDirectory(prefix="operations-check-", dir="/work") as temporary:
        root = Path(temporary)
        settings = Settings(
            _env_file=None,
            sqlite_path=root / "data" / "db.sqlite3",
            work_dir=root / "jobs",
            telegram_files_dir=root / "telegram",
        )
        settings.work_dir.mkdir()
        cache = settings.telegram_files_dir / "12345:synthetic" / "music" / "file_1.wav"
        cache.parent.mkdir(parents=True)
        service = cache.parent.parent / "td.binlog"
        service.write_bytes(b"synthetic service database, preserve")
        calls = []

        async def get_file(request):
            assert (await request.post())["file_id"] == "offline-file"
            calls.append(1)
            if not cache.exists():
                with wave.open(str(cache), "wb") as audio:
                    audio.setparams((1, 2, 16000, 0, "NONE", "none"))
                    for _ in range(660):
                        audio.writeframes(b"\0\0" * 16000)
            assert cache.stat().st_size > 20_000_000
            return web.json_response(
                {
                    "ok": True,
                    "result": {
                        "file_id": "offline-file",
                        "file_unique_id": "synthetic",
                        "file_path": str(cache),
                        "file_size": cache.stat().st_size,
                    },
                }
            )

        app = web.Application()
        app.router.add_post("/bot12345:synthetic/getFile", get_file)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        session = AiohttpSession(
            api=TelegramAPIServer.from_base(f"http://127.0.0.1:{port}", is_local=True)
        )
        database = await Database(settings.sqlite_path).open(migrate=True)
        repository = Repository(database)
        try:
            async with Bot("12345:synthetic", session=session) as bot:
                downloader = SourceDownloader(settings, repository, bot)
                hashes = []
                for update in (1, 2):
                    result = await repository.create_job(
                        update_id=update,
                        owner_id=12345,
                        chat_id=12345,
                        message_id=update,
                        source=SourceRef(SourceKind.TELEGRAM, "offline-file"),
                    )
                    job = await repository.claim_next()
                    assert job.id == result.job_id
                    manifest = await downloader.acquire(job)
                    hashes.append(manifest["sha256"])
                    job = await repository.finish_download(job, manifest)
                    if update == 2:
                        # Save database plus content while no operation changes the files.
                        backup = root / "backup.sqlite3"
                        copy_database(settings.sqlite_path, backup)
                        shutil.copytree(settings.work_dir, root / "saved-jobs")
                        shutil.copytree(settings.telegram_files_dir, root / "saved-telegram")
                    await repository.cancel(
                        update_id=100 + update, job_id=job.id, owner_id=12345, generation=0
                    )
                    await Cleanup(settings, repository).run()
                    assert not cache.exists() and service.exists()
                    assert not (settings.work_dir / str(job.id)).exists()
                assert len(calls) == 2 and hashes[0] == hashes[1]
                print(
                    "Cleanup: Telegram HTTP getFile >20 MB twice, same SHA-256, service DB retained"
                )

            restored_path = root / "restored-data" / "db.sqlite3"
            copy_database(backup, restored_path, restore=True)
            shutil.copytree(root / "saved-jobs", settings.work_dir, dirs_exist_ok=True)
            shutil.copytree(
                root / "saved-telegram", settings.telegram_files_dir, dirs_exist_ok=True
            )
            restored = await Database(restored_path).open()
            try:
                restored_repository = Repository(restored)
                await Cleanup(settings, restored_repository).run(startup=True)
                current = await restored_repository.active_job()
                assert current.id == 2 and current.phase == "prepare"
                assert current.status == "active" and cache.exists()
                pulse = asyncio.create_task(heartbeat(restored_repository, "worker"))
                try:
                    for _ in range(100):
                        state = await diagnostics(settings, restored_repository, "worker")
                        if state["healthy"]:
                            break
                        await asyncio.sleep(0.01)
                    assert state["healthy"] and state["queue_size"] == 1
                finally:
                    pulse.cancel()
                    await asyncio.gather(pulse, return_exceptions=True)
                await restored_repository.cancel(
                    update_id=999, job_id=current.id, owner_id=12345, generation=0
                )
                await Cleanup(settings, restored_repository).run()
                assert not cache.exists()
            finally:
                await restored.close()
            print(
                "Operations: WAL backup/restore with content, startup reconciliation and heartbeat"
            )
        finally:
            await database.close()
            await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
