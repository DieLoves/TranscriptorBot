"""Exercise the installed pipeline and SDK without network or real credentials."""

import asyncio
import wave
from tempfile import TemporaryDirectory

import httpx
from groq import AsyncGroq

from transcriptbot.config import Settings
from transcriptbot.downloaders.files import source_manifest
from transcriptbot.groq_adapter import GroqAdapter
from transcriptbot.media.artifacts import read_json
from transcriptbot.media.service import MediaPipeline
from transcriptbot.operations.cleanup import Cleanup
from transcriptbot.storage.database import Database
from transcriptbot.storage.models import SourceKind, SourceRef
from transcriptbot.storage.repository import Repository
from transcriptbot.telegram.store import TelegramStore
from transcriptbot.worker import process_job


async def main() -> None:
    with TemporaryDirectory(prefix="media-check-", dir="/work") as temporary:
        from pathlib import Path

        root = Path(temporary)
        settings = Settings(
            _env_file=None,
            sqlite_path=root / "data" / "db.sqlite3",
            work_dir=root / "jobs",
            telegram_files_dir=root / "telegram",
            chunk_seconds=2,
            silence_window_seconds=0.5,
        )
        calls = []

        async def handler(request):
            body = await request.aread()
            assert b"verbose_json" in body and b"whisper-large-v3-turbo" in body
            calls.append(request.url.path)
            return httpx.Response(
                200,
                json={
                    "text": f"Part {len(calls)}.",
                    "language": "en",
                    "segments": [{"id": 9, "start": 0, "end": 0.5, "text": "Part."}],
                    "custom": True,
                },
            )

        client = AsyncGroq(
            api_key="synthetic-check",
            max_retries=0,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        groq = GroqAdapter(settings, client=client)
        database = await Database(settings.sqlite_path).open(migrate=True)
        repository = Repository(database)
        try:
            result = await repository.create_job(
                update_id=1,
                owner_id=12345,
                chat_id=12345,
                message_id=1,
                source=SourceRef(SourceKind.DIRECT, "https://example.com/audio"),
            )
            job = await repository.claim_next()
            directory = settings.work_dir / str(result.job_id)
            directory.mkdir(parents=True)
            source = directory / "source.media"
            with wave.open(str(source), "wb") as audio:
                audio.setparams((1, 2, 16000, 0, "NONE", "none"))
                audio.writeframes(b"\x00\x10" * 80000)
            manifest = source_manifest(
                directory,
                {
                    "job_id": job.id,
                    "kind": "direct",
                    "maximum": 1_000_000,
                    "duration": 10,
                    "probe_timeout": 30,
                    "title": "synthetic",
                },
                source,
            )
            await repository.finish_download(job, manifest)
            pipeline = MediaPipeline(settings, repository, groq)
            await pipeline.step(await repository.claim_next())

            async def crash(*args, **kwargs):
                raise RuntimeError("simulated crash before DB acknowledgement")

            repository.save_chunk_response = crash
            try:
                await pipeline.step(await repository.claim_next())
            except RuntimeError:
                pass
            else:
                raise AssertionError("crash injection did not execute")
            assert len(calls) == 1
            await database.close()
            database = await Database(settings.sqlite_path).open()
            repository = Repository(database)
            await Cleanup(settings, repository).run(startup=True)
            pipeline = MediaPipeline(settings, repository, groq)
            await pipeline.step(await repository.claim_next())
            assert len(calls) == 1
            for _ in range(10):
                job = await repository.claim_next()
                await process_job(job, pipeline, repository, asyncio.Event())
                if job.phase == "deliver":
                    break
            else:
                raise AssertionError("pipeline did not reach delivery")
            assert len(calls) == 3
            value = read_json(directory / "result.verbose.json")
            assert value["text"] == "Part 1. Part 2. Part 3."
            assert [s["start"] for s in value["segments"]] == [0, 2, 4]
            assert all(c["response"]["custom"] for c in value["chunks"])
            assert len(await repository.chunks(job.id)) == 3
            assert (
                await database.call(lambda c: c.execute("PRAGMA integrity_check").fetchone()[0])
                == "ok"
            )
            assert (
                await database.call(
                    lambda c: c.execute(
                        "SELECT COUNT(*) FROM outbox WHERE job_id = ? "
                        "AND operation IN ('preview','txt','json')",
                        (job.id,),
                    ).fetchone()[0]
                )
                == 3
            )
            print(
                "Media/Groq: exact samples, SDK multipart, checkpoint crash recovery, "
                "global times and delivery handoff"
            )
            store = TelegramStore(repository)
            for number, kind in enumerate(("preview", "txt", "json")):
                operation = await database.call(
                    lambda c, kind=kind: dict(
                        c.execute("SELECT * FROM outbox WHERE operation = ?", (kind,)).fetchone()
                    )
                )
                await store.confirm(operation, number + 1)
            await store.refresh_statuses()
            await Cleanup(settings, repository).run(startup=True)
            assert not directory.exists() and len(calls) == 3
            history = await repository.get_job(job.id, owner_id=12345)
            assert history.status == "completed" and not history.cleanup_pending
            print("Delivery cleanup: all content removed, history retained, no repeated Groq")
        finally:
            await database.close()
            await groq.close()


if __name__ == "__main__":
    asyncio.run(main())
