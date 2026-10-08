import asyncio
import json
import sqlite3
import sys
import threading
from pathlib import Path

import pytest

from transcriptbot.storage.database import Database, StorageError
from transcriptbot.storage.models import JobStatus, Phase, SourceKind, SourceRef
from transcriptbot.storage.repository import Repository


class Clock:
    now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
async def database(tmp_path: Path):
    database = await Database(tmp_path / "data" / "db.sqlite3").open(migrate=True)
    try:
        yield database
    finally:
        await database.close()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def repository(database: Database, clock: Clock) -> Repository:
    return Repository(database, clock=clock)


async def create(
    repository: Repository, update_id: int = 1, owner_id: int = 123, chat_id: int = -100
):
    return await repository.create_job(
        update_id=update_id,
        owner_id=owner_id,
        chat_id=chat_id,
        message_id=10,
        topic_id=12,
        source=SourceRef(SourceKind.DIRECT, "https://example.com/audio?secret=test"),
    )


async def test_database_pragmas_permissions_and_thread(database: Database) -> None:
    main_thread = threading.get_ident()

    def inspect(connection):
        return (
            threading.get_ident(),
            *(
                connection.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "foreign_keys", "busy_timeout", "synchronous")
            ),
        )

    thread, *pragmas = await database.call(inspect)
    assert thread != main_thread
    assert await database.call(lambda _: threading.get_ident()) == thread
    assert pragmas == ["wal", 1, 10000, 2]
    assert database.path.stat().st_mode & 0o777 == 0o600
    assert database.path.parent.stat().st_mode & 0o777 == 0o700
    for suffix in ("-wal", "-shm"):
        assert Path(str(database.path) + suffix).stat().st_mode & 0o777 == 0o600
    assert await database.call(lambda c: c.execute("PRAGMA integrity_check").fetchone()[0]) == "ok"


async def test_migrations_are_required_and_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite3"
    with pytest.raises(StorageError, match="migrate"):
        await Database(path).open()
    assert not path.exists()
    first, second = await asyncio.gather(
        Database(path).open(migrate=True), Database(path).open(migrate=True)
    )
    try:
        assert (
            await first.call(
                lambda c: c.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
            )
            == 2
        )
        assert (
            await second.call(lambda c: c.execute("SELECT value FROM queue_sequence").fetchone()[0])
            == 0
        )
    finally:
        await first.close()
        await second.close()


async def test_incompatible_migrations_fail_safely(database: Database) -> None:
    await database.call(lambda c: c.execute("UPDATE schema_migrations SET checksum = 'changed'"))
    with pytest.raises(StorageError, match="контрольная сумма"):
        await Database(database.path).open()


async def test_failed_migration_rolls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "transcriptbot.storage.database.migration_scripts",
        lambda: [
            (
                1,
                "CREATE TABLE schema_migrations(version, checksum, applied_at);\n"
                "CREATE TABLE temporary_data(id);\nBROKEN SQL;\n",
                "test",
            )
        ],
    )
    path = tmp_path / "db.sqlite3"
    with pytest.raises(StorageError) as error:
        await Database(path).open(migrate=True)
    assert "BROKEN" not in str(error.value)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []


async def test_transaction_rollback_does_not_break_connection(database: Database) -> None:
    def broken(connection):
        connection.execute("UPDATE queue_sequence SET value = 42")
        raise ValueError("abort")

    with pytest.raises(ValueError, match="abort"):
        await database.transaction(broken)
    assert (
        await database.call(lambda c: c.execute("SELECT value FROM queue_sequence").fetchone()[0])
        == 0
    )


async def test_concurrent_user_limit_and_update_dedup(database: Database, clock: Clock) -> None:
    second = await Database(database.path).open()
    repositories = [Repository(database, clock=clock), Repository(second, clock=clock)]
    try:
        results = await asyncio.gather(*(create(repositories[i % 2], i) for i in range(15)))
        assert sum(result.outcome == "created" for result in results) == 3
        assert sum(result.outcome == "user_limit" for result in results) == 12
        assert sorted(
            result.queue_position for result in results if result.outcome == "created"
        ) == [1, 2, 3]
        for i, result in enumerate(results):
            assert await create(repositories[1], i) == result
        assert len(await repositories[0].list_jobs(owner_id=123)) == 3
        assert (
            await database.call(
                lambda c: c.execute("SELECT COUNT(*) FROM processed_updates").fetchone()[0]
            )
            == 15
        )
    finally:
        await second.close()


async def test_concurrent_global_limit(database: Database) -> None:
    second = await Database(database.path).open()
    repositories = [Repository(database, max_queue_jobs=4), Repository(second, max_queue_jobs=4)]
    try:
        results = await asyncio.gather(*(create(repositories[i % 2], i, i + 1) for i in range(20)))
        assert sum(result.outcome == "created" for result in results) == 4
        assert sum(result.outcome == "queue_limit" for result in results) == 16
    finally:
        await second.close()


async def test_same_update_concurrently_creates_only_one_job(database: Database) -> None:
    second = await Database(database.path).open()
    try:
        results = await asyncio.gather(create(Repository(database)), create(Repository(second)))
        assert results[0] == results[1]
        assert len(await Repository(database).list_jobs(owner_id=123)) == 1
    finally:
        await second.close()


async def test_rejected_update_stays_rejected(repository: Repository) -> None:
    rejected = await repository.record_rejection(1, "access_denied")
    assert await create(repository) == rejected
    assert await repository.list_jobs(owner_id=123) == []


async def test_job_and_update_are_rolled_back_together(database: Database) -> None:
    await database.call(
        lambda c: c.execute(
            """CREATE TRIGGER refuse_update BEFORE INSERT ON processed_updates
        BEGIN SELECT RAISE(ABORT, 'secret source must not appear'); END"""
        )
    )
    with pytest.raises(StorageError) as error:
        await create(Repository(database))
    assert "secret" not in str(error.value)
    assert await database.call(lambda c: c.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]) == 0
    assert (
        await database.call(lambda c: c.execute("SELECT value FROM queue_sequence").fetchone()[0])
        == 0
    )


async def test_fifo_failed_job_releases_queue_and_resume_goes_to_end(
    repository: Repository,
) -> None:
    first = await create(repository, 1)
    second = await create(repository, 2)
    active = await repository.claim_next()
    assert active.id == first.job_id
    assert await repository.queue_position(second.job_id) == 2
    failed = await repository.fail(active, error_code="download_failed")
    assert failed.status == JobStatus.FAILED
    assert await repository.queue_position(first.job_id) is None
    assert await repository.queue_position(second.job_id) == 1
    resumed = await repository.resume(update_id=3, job_id=first.job_id, owner_id=123, generation=0)
    assert resumed.queue_position == 2
    assert (await repository.claim_next()).id == second.job_id


@pytest.mark.parametrize("quota", [False, True])
async def test_wait_survives_restart_and_blocks_queue(
    database: Database, clock: Clock, quota: bool
) -> None:
    repository = Repository(database, clock=clock)
    first = await create(repository, 1)
    await create(repository, 2)
    active = await repository.claim_next()
    active = await repository.advance(active, Phase.PREPARE)
    waiting = await repository.defer(
        active, until=clock.now + 60, error_code="rate_limit", quota=quota
    )
    assert waiting.phase == Phase.PREPARE
    assert waiting.attempts == (0 if quota else 1)
    assert waiting.quota_attempts == (1 if quota else 0)
    reopened = await Database(database.path).open()
    try:
        restored_repository = Repository(reopened, clock=clock)
        assert await restored_repository.claim_next() is None
        clock.now += 60
        restored = await restored_repository.claim_next()
        assert restored.id == first.job_id
        assert restored.phase == Phase.PREPARE
        assert restored.status == JobStatus.ACTIVE
        assert restored.next_attempt_at is None
        assert restored.attempts == waiting.attempts
        assert restored.quota_attempts == waiting.quota_attempts
        assert await restored_repository.claim_next() == restored
    finally:
        await reopened.close()


async def test_resume_preserves_chunk_checkpoints_and_phase(
    repository: Repository, clock: Clock
) -> None:
    first = await create(repository)
    job = await repository.claim_next()
    job = await repository.advance(job, Phase.PREPARE)
    assert await repository.plan_chunks(job, [(0, 100), (100, 150)])
    job = await repository.advance(job, Phase.TRANSCRIBE)
    assert await repository.save_chunk_response(
        job, number=0, path="responses/0.json", sha256="a" * 64
    )
    failed = await repository.fail(job, error_code="groq_failed")
    assert failed.retention_until == clock.now + 7 * 86400
    result = await repository.resume(update_id=2, job_id=job.id, owner_id=123, generation=0)
    assert result.outcome == "resumed"
    restored = await repository.claim_next()
    assert restored.id == first.job_id
    assert restored.phase == Phase.TRANSCRIBE
    assert restored.generation == 1
    assert restored.queue_no > failed.queue_no
    chunks = await repository.chunks(job.id)
    assert chunks[0].response_sha256 == "a" * 64
    assert chunks[1].response_path is None
    assert await repository.advance(job, Phase.ASSEMBLE) is None
    assert not await repository.save_chunk_response(
        job, number=1, path="late.json", sha256="b" * 64
    )


@pytest.mark.parametrize(
    "status", [JobStatus.QUEUED, JobStatus.ACTIVE, JobStatus.RETRY_WAIT, JobStatus.QUOTA_WAIT]
)
async def test_cancel_is_final_and_idempotent(
    repository: Repository, clock: Clock, status: JobStatus
) -> None:
    created = await create(repository)
    old = None
    if status != JobStatus.QUEUED:
        old = await repository.claim_next()
        if status in (JobStatus.RETRY_WAIT, JobStatus.QUOTA_WAIT):
            old = await repository.defer(
                old,
                until=clock.now + 60,
                error_code="temporary",
                quota=status == JobStatus.QUOTA_WAIT,
            )
    cancelled = await repository.cancel(
        update_id=2, job_id=created.job_id, owner_id=123, generation=0
    )
    assert cancelled.outcome == "cancelled"
    assert (
        await repository.cancel(update_id=2, job_id=created.job_id, owner_id=123, generation=0)
        == cancelled
    )
    assert (
        await repository.cancel(update_id=3, job_id=created.job_id, owner_id=123, generation=0)
    ).outcome == "unavailable"
    if old:
        assert await repository.advance(old, Phase.PREPARE) is None
        assert await repository.fail(old, error_code="late_error") is None
    job = await repository.get_job(created.job_id, owner_id=123)
    assert job.status == JobStatus.CANCELLED
    assert job.cleanup_pending and not job.resumable
    assert await repository.claim_next() is None


async def test_old_callback_cannot_affect_new_generation(repository: Repository) -> None:
    created = await create(repository)
    active = await repository.claim_next()
    await repository.fail(active, error_code="failed")
    resumed = await repository.resume(
        update_id=2, job_id=created.job_id, owner_id=123, generation=0
    )
    active = await repository.claim_next()
    assert (
        await repository.cancel(update_id=3, job_id=active.id, owner_id=123, generation=0)
    ).outcome == "unavailable"
    await repository.fail(active, error_code="failed_again")
    assert (
        await repository.resume(update_id=2, job_id=active.id, owner_id=123, generation=0)
        == resumed
    )
    assert (
        await repository.resume(update_id=4, job_id=active.id, owner_id=123, generation=0)
    ).outcome == "unavailable"
    assert (await repository.get_job(active.id, owner_id=123)).status == JobStatus.FAILED


async def test_resume_rechecks_limits_and_expiry(repository: Repository, clock: Clock) -> None:
    first = await create(repository)
    active = await repository.claim_next()
    await repository.fail(active, error_code="failed")
    for update_id in (2, 3, 4):
        await create(repository, update_id)
    assert (
        await repository.resume(update_id=5, job_id=first.job_id, owner_id=123, generation=0)
    ).outcome == "user_limit"
    clock.now += 7 * 86400
    assert (
        await repository.resume(update_id=6, job_id=first.job_id, owner_id=123, generation=0)
    ).outcome == "unavailable"


async def test_history_and_callbacks_enforce_owner_and_group(repository: Repository) -> None:
    created = await create(repository)
    assert await repository.get_job(created.job_id, owner_id=456) is None
    assert await repository.get_job(created.job_id, owner_id=123, group_chat_id=-200) is None
    assert await repository.list_jobs(owner_id=123, group_chat_id=-200) == []
    job = await repository.get_job(created.job_id, owner_id=123, group_chat_id=-100)
    assert job.topic_id == 12
    assert "secret" not in repr(job)
    for update_id, owner_id, group_chat_id in [(2, 456, None), (3, 123, -200)]:
        assert (
            await repository.cancel(
                update_id=update_id,
                job_id=job.id,
                owner_id=owner_id,
                generation=0,
                group_chat_id=group_chat_id,
            )
        ).outcome == "unavailable"
    assert (await repository.get_job(job.id, owner_id=123)).status == JobStatus.QUEUED
    active = await repository.claim_next()
    await repository.fail(active, error_code="failed")
    for update_id, owner_id, group_chat_id in [(4, 456, None), (5, 123, -200)]:
        assert (
            await repository.resume(
                update_id=update_id,
                job_id=job.id,
                owner_id=owner_id,
                generation=0,
                group_chat_id=group_chat_id,
            )
        ).outcome == "unavailable"


async def test_history_returns_last_ten_and_keeps_terminal_metadata(database: Database) -> None:
    repository = Repository(database, max_user_jobs=15)
    for i in range(12):
        created = await create(repository, i)
        await repository.cancel(
            update_id=100 + i, job_id=created.job_id, owner_id=123, generation=0
        )
    jobs = await repository.list_jobs(owner_id=123)
    assert [job.id for job in jobs] == list(range(12, 2, -1))
    assert all(job.status == JobStatus.CANCELLED for job in jobs)
    assert await repository.get_job(1, owner_id=123) is not None


async def test_completion_requires_both_delivered_files(
    repository: Repository, database: Database
) -> None:
    created = await create(repository)
    job = await repository.claim_next()
    for phase in (Phase.PREPARE, Phase.TRANSCRIBE, Phase.ASSEMBLE, Phase.DELIVER):
        job = await repository.advance(job, phase)
    assert await repository.complete(job) is None
    for operation in ("txt", "json"):
        await database.call(
            lambda c, operation=operation: c.execute(
                """INSERT INTO outbox (job_id, generation, operation, artifact_path, status,
            telegram_message_id) VALUES (?, 0, ?, ?, 'sent', 42)""",
                (created.job_id, operation, "result." + operation),
            )
        )
        if operation == "txt":
            assert await repository.complete(job) is None
    done = await repository.complete(job)
    assert done.status == JobStatus.COMPLETED
    assert done.cleanup_pending and not done.resumable
    assert await repository.complete(job) is None
    assert await repository.claim_next() is None


async def test_delivery_failure_resumes_delivery_without_losing_sent_marks(
    repository: Repository, database: Database
) -> None:
    await create(repository)
    job = await repository.claim_next()
    for phase in (Phase.PREPARE, Phase.TRANSCRIBE, Phase.ASSEMBLE, Phase.DELIVER):
        job = await repository.advance(job, phase)
    await database.call(
        lambda c: c.execute(
            """INSERT INTO outbox (job_id, generation, operation, status, telegram_message_id)
        VALUES (?, 0, 'txt', 'sent', 42)""",
            (job.id,),
        )
    )
    await repository.fail(job, error_code="delivery_failed")
    assert (
        await repository.resume(update_id=2, job_id=job.id, owner_id=123, generation=0)
    ).outcome == "resumed"
    assert (await repository.claim_next()).phase == Phase.DELIVER
    assert (
        await database.call(
            lambda c: c.execute("SELECT telegram_message_id FROM outbox").fetchone()[0]
        )
        == 42
    )


async def test_foreign_keys_and_single_active_constraint(
    repository: Repository, database: Database
) -> None:
    await create(repository, 1)
    second = await create(repository, 2)
    await repository.claim_next()
    with pytest.raises(StorageError):
        await database.call(
            lambda c: c.execute("UPDATE jobs SET status = 'active' WHERE id = ?", (second.job_id,))
        )
    with pytest.raises(StorageError):
        await database.call(lambda c: c.execute("INSERT INTO job_assets VALUES (999, 999)"))


async def test_invalid_phase_and_late_chunk_response(repository: Repository) -> None:
    await create(repository)
    job = await repository.claim_next()
    with pytest.raises(ValueError, match="phase"):
        await repository.advance(job, Phase.DELIVER)
    job = await repository.advance(job, Phase.PREPARE)
    with pytest.raises(ValueError, match="contiguous"):
        await repository.plan_chunks(job, [(0, 100), (101, 200)])
    assert await repository.plan_chunks(job, [(0, 100)])
    assert await repository.plan_chunks(job, [(0, 100)])
    with pytest.raises(ValueError, match="immutable"):
        await repository.plan_chunks(job, [(0, 200)])
    job = await repository.advance(job, Phase.TRANSCRIBE)
    await repository.cancel(update_id=2, job_id=job.id, owner_id=123, generation=0)
    assert not await repository.save_chunk_response(
        job, number=0, path="late.json", sha256="a" * 64
    )
    assert (await repository.chunks(job.id))[0].response_path is None


async def test_database_wait_does_not_block_event_loop(database: Database, clock: Clock) -> None:
    second = await Database(database.path).open()
    await database.call(lambda c: c.execute("BEGIN IMMEDIATE"))
    pending = asyncio.create_task(create(Repository(second, clock=clock)))
    try:
        # A synchronous connection on the loop would prevent this deadline from running.
        await asyncio.wait_for(asyncio.sleep(0.02), timeout=1)
        assert not pending.done()
        await database.call(lambda c: c.rollback())
        assert (await asyncio.wait_for(pending, timeout=2)).outcome == "created"
    finally:
        await database.call(lambda c: c.rollback())
        await second.close()


async def test_concurrent_processes_share_limits_and_sequence(database: Database) -> None:
    code = """
import asyncio, json, sys
from pathlib import Path
from transcriptbot.storage.database import Database
from transcriptbot.storage.repository import Repository
from transcriptbot.storage.models import SourceRef, SourceKind
async def main():
    db = await Database(Path(sys.argv[1])).open()
    try:
        repo = Repository(db, max_queue_jobs=7)
        results = []
        base = int(sys.argv[2])
        for i in range(6):
            result = await repo.create_job(update_id=base + i, owner_id=base + i + 1,
                chat_id=1, message_id=1, source=SourceRef(SourceKind.TELEGRAM, 'test_file_id'))
            results.append([result.outcome, result.queue_position])
        print(json.dumps(results))
    finally:
        await db.close()
asyncio.run(main())
"""
    processes = await asyncio.gather(
        *(
            asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                code,
                str(database.path),
                str(base),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            for base in (0, 100, 200)
        )
    )
    results = []
    try:
        outputs = await asyncio.wait_for(
            asyncio.gather(*(process.communicate() for process in processes)),
            timeout=10,
        )
        for process, (stdout, stderr) in zip(processes, outputs, strict=True):
            assert process.returncode == 0, stderr.decode()
            results.extend(json.loads(stdout))
        assert sum(result[0] == "created" for result in results) == 7
        assert sorted(result[1] for result in results if result[0] == "created") == list(
            range(1, 8)
        )
        assert sum(result[0] == "queue_limit" for result in results) == 11
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()


@pytest.mark.parametrize("phase", list(Phase))
async def test_active_phase_is_restored_before_new_jobs(
    database: Database,
    repository: Repository,
    phase: Phase,
) -> None:
    first = await create(repository, 1)
    await create(repository, 2)
    job = await repository.claim_next()
    for next_phase in list(Phase)[1:]:
        if job.phase == phase:
            break
        job = await repository.advance(job, next_phase)
    reopened = await Database(database.path).open()
    try:
        recovered = Repository(reopened)
        assert await recovered.active_job() == job
        restored = await recovered.claim_next()
        assert restored.id == first.job_id and restored.phase == phase
    finally:
        await reopened.close()


async def test_resume_global_limit_and_removed_artifacts(database: Database) -> None:
    repository = Repository(database, max_queue_jobs=1)
    first = await create(repository)
    job = await repository.claim_next()
    await repository.fail(job, error_code="failed")
    second = await create(repository, 2, owner_id=456)
    assert (
        await repository.resume(
            update_id=3,
            job_id=job.id,
            owner_id=123,
            generation=0,
        )
    ).outcome == "queue_limit"
    await repository.cancel(update_id=4, job_id=second.job_id, owner_id=456, generation=0)
    await database.call(
        lambda c: c.execute("UPDATE jobs SET resumable = 0 WHERE id = ?", (first.job_id,))
    )
    assert (
        await repository.resume(
            update_id=5,
            job_id=job.id,
            owner_id=123,
            generation=0,
        )
    ).outcome == "unavailable"


async def test_cancel_and_late_completion_race(database: Database, repository: Repository) -> None:
    await create(repository)
    job = await repository.claim_next()
    for phase in list(Phase)[1:]:
        job = await repository.advance(job, phase)
    await database.call(
        lambda c: c.executemany(
            """INSERT INTO outbox (job_id, generation, operation, status, telegram_message_id)
        VALUES (?, 0, ?, 'sent', 42)""",
            [(job.id, "txt"), (job.id, "json")],
        )
    )
    second = await Database(database.path).open()
    try:
        cancellation, completion = await asyncio.gather(
            repository.cancel(update_id=2, job_id=job.id, owner_id=123, generation=0),
            Repository(second).complete(job),
        )
        final = await repository.get_job(job.id, owner_id=123)
        if cancellation.outcome == "cancelled":
            assert completion is None and final.status == JobStatus.CANCELLED
        else:
            assert completion.status == final.status == JobStatus.COMPLETED
    finally:
        await second.close()
