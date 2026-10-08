"""Atomic queue operations shared by reception, worker and delivery."""

import math
import re
import sqlite3
import time
from collections.abc import Callable
from itertools import pairwise

from transcriptbot.storage.database import Database
from transcriptbot.storage.models import (
    Chunk,
    Job,
    JobStatus,
    Phase,
    SourceKind,
    SourceRef,
    UpdateResult,
)

UNFINISHED = "('queued', 'active', 'retry_wait', 'quota_wait')"
RUNNING = "('active', 'retry_wait', 'quota_wait')"
NEXT_PHASE = {
    Phase.DOWNLOAD: Phase.PREPARE,
    Phase.PREPARE: Phase.TRANSCRIBE,
    Phase.TRANSCRIBE: Phase.ASSEMBLE,
    Phase.ASSEMBLE: Phase.DELIVER,
}


def job_from_row(row: sqlite3.Row | None) -> Job | None:
    if row is None:
        return None
    values = dict(row)
    values.update(
        status=JobStatus(row["status"]),
        phase=Phase(row["phase"]),
        source_kind=SourceKind(row["source_kind"]),
        resumable=bool(row["resumable"]),
        cleanup_pending=bool(row["cleanup_pending"]),
    )
    return Job(**values)


def read_job(connection: sqlite3.Connection, job_id: int) -> Job | None:
    return job_from_row(connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone())


class Repository:
    def __init__(
        self,
        database: Database,
        *,
        max_user_jobs: int = 3,
        max_queue_jobs: int = 30,
        failed_retention_days: int = 7,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if min(max_user_jobs, max_queue_jobs, failed_retention_days) <= 0:
            raise ValueError("Queue limits and retention must be positive")
        self.database = database
        self.max_user_jobs = max_user_jobs
        self.max_queue_jobs = max_queue_jobs
        self.retention_seconds = failed_retention_days * 86400
        self.clock = clock

    def _limit(self, connection: sqlite3.Connection, owner_id: int) -> str | None:
        total, own = connection.execute(
            f"SELECT COUNT(*), COALESCE(SUM(owner_id = ?), 0) FROM jobs "
            f"WHERE status IN {UNFINISHED}",
            (owner_id,),
        ).fetchone()
        if own >= self.max_user_jobs:
            return "user_limit"
        if total >= self.max_queue_jobs:
            return "queue_limit"
        return None

    @staticmethod
    def _sequence(connection: sqlite3.Connection) -> int:
        return connection.execute(
            "UPDATE queue_sequence SET value = value + 1 WHERE singleton = 1 RETURNING value"
        ).fetchone()[0]

    @staticmethod
    def _position(connection: sqlite3.Connection, queue_no: int) -> int:
        return connection.execute(
            f"SELECT COUNT(*) FROM jobs WHERE status IN {UNFINISHED} AND queue_no <= ?",
            (queue_no,),
        ).fetchone()[0]

    def _update(
        self,
        connection: sqlite3.Connection,
        update_id: int,
        operation: Callable[[], UpdateResult],
    ) -> UpdateResult:
        old = connection.execute(
            "SELECT outcome, job_id, queue_position FROM processed_updates WHERE update_id = ?",
            (update_id,),
        ).fetchone()
        if old is not None:
            return UpdateResult(**dict(old))
        result = operation()
        connection.execute(
            "INSERT INTO processed_updates VALUES (?, ?, ?, ?, ?)",
            (update_id, result.outcome, result.job_id, result.queue_position, self.clock()),
        )
        return result

    async def create_job(
        self,
        *,
        update_id: int,
        owner_id: int,
        chat_id: int,
        message_id: int,
        source: SourceRef,
        topic_id: int | None = None,
    ) -> UpdateResult:
        if update_id < 0:
            raise ValueError("Invalid update ID")
        return await self.database.transaction(
            lambda connection: self._update(
                connection,
                update_id,
                lambda: self.create_job_in_transaction(
                    connection,
                    owner_id=owner_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    source=source,
                    topic_id=topic_id,
                ),
            )
        )

    def create_job_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        owner_id: int,
        chat_id: int,
        message_id: int,
        source: SourceRef,
        topic_id: int | None = None,
    ) -> UpdateResult:
        if owner_id <= 0 or message_id <= 0 or not source.reference:
            raise ValueError("Invalid request metadata")
        if limit := self._limit(connection, owner_id):
            return UpdateResult(limit, None)
        now = self.clock()
        sequence = self._sequence(connection)
        job_id = connection.execute(
            """INSERT INTO jobs
            (owner_id, chat_id, message_id, topic_id, source_kind, source_ref, title,
             model, queue_no, status, phase, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'whisper-large-v3-turbo', ?, 'queued',
                    'download', ?, ?) RETURNING id""",
            (
                owner_id,
                chat_id,
                message_id,
                topic_id,
                source.kind,
                source.reference,
                source.title,
                sequence,
                now,
                now,
            ),
        ).fetchone()[0]
        return UpdateResult("created", job_id, self._position(connection, sequence))

    async def record_rejection(self, update_id: int, outcome: str) -> UpdateResult:
        """Remember a rejected/ignored update; Telegram offset is managed separately."""
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", outcome):
            raise ValueError("Invalid outcome code")
        return await self.database.transaction(
            lambda connection: self._update(
                connection, update_id, lambda: UpdateResult(outcome, None)
            )
        )

    async def get_job(
        self, job_id: int, *, owner_id: int, group_chat_id: int | None = None
    ) -> Job | None:
        """In private chat omit the group filter; groups may only read their own history."""
        return await self.database.call(
            lambda connection: job_from_row(
                connection.execute(
                    "SELECT * FROM jobs WHERE id = ? AND owner_id = ? "
                    "AND (? IS NULL OR chat_id = ?)",
                    (job_id, owner_id, group_chat_id, group_chat_id),
                ).fetchone()
            )
        )

    async def list_jobs(self, *, owner_id: int, group_chat_id: int | None = None) -> list[Job]:
        return await self.database.call(
            lambda connection: [
                job_from_row(row)
                for row in connection.execute(
                    """SELECT * FROM jobs WHERE owner_id = ? AND (? IS NULL OR chat_id = ?)
                ORDER BY id DESC LIMIT 10""",
                    (owner_id, group_chat_id, group_chat_id),
                )
            ]
        )

    async def queue_position(self, job_id: int) -> int | None:
        def position(connection: sqlite3.Connection) -> int | None:
            job = read_job(connection, job_id)
            if job is None or job.status not in (
                JobStatus.QUEUED,
                JobStatus.ACTIVE,
                JobStatus.RETRY_WAIT,
                JobStatus.QUOTA_WAIT,
            ):
                return None
            return self._position(connection, job.queue_no)

        return await self.database.call(position)

    async def active_job(self) -> Job | None:
        """Restore the persisted operation/wait without claiming a new queued job."""
        return await self.database.call(
            lambda connection: job_from_row(
                connection.execute(
                    f"SELECT * FROM jobs WHERE status IN {RUNNING} LIMIT 1"
                ).fetchone()
            )
        )

    async def claim_next(self) -> Job | None:
        """Only call while holding WorkerLock; an active wait blocks newer jobs."""

        def claim(connection: sqlite3.Connection) -> Job | None:
            job = job_from_row(
                connection.execute(
                    f"SELECT * FROM jobs WHERE status IN {RUNNING} ORDER BY queue_no LIMIT 1"
                ).fetchone()
            )
            now = self.clock()
            if job is not None:
                if job.next_attempt_at is not None and job.next_attempt_at > now:
                    return None
                if job.status == JobStatus.ACTIVE:
                    return job
                connection.execute(
                    """UPDATE jobs SET status = 'active', next_attempt_at = NULL,
                    revision = revision + 1, updated_at = ? WHERE id = ? AND revision = ?""",
                    (now, job.id, job.revision),
                )
                return read_job(connection, job.id)
            row = connection.execute(
                "SELECT id FROM jobs WHERE status = 'queued' ORDER BY queue_no LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """UPDATE jobs SET status = 'active', revision = revision + 1, updated_at = ?
                WHERE id = ? AND status = 'queued'""",
                (now, row[0]),
            )
            return read_job(connection, row[0])

        return await self.database.transaction(claim)

    async def advance(self, expected: Job, target: Phase) -> Job | None:
        if NEXT_PHASE.get(expected.phase) != target:
            raise ValueError("Invalid phase transition")
        return await self._change(
            expected, "phase = ?, attempts = 0, quota_attempts = 0, error_code = NULL", (target,)
        )

    async def track_asset(self, job_id: int, *, kind: str, path: str) -> None:
        """Track I/O resources, including sources discovered during cancellation."""
        if kind not in {"workspace", "telegram_cache"} or not path:
            raise ValueError("Invalid asset")

        def track(connection: sqlite3.Connection) -> None:
            asset = connection.execute(
                "INSERT INTO assets (kind, path) VALUES (?, ?) "
                "ON CONFLICT(path) DO UPDATE SET path = excluded.path RETURNING id, kind",
                (kind, path),
            ).fetchone()
            if asset[1] != kind:
                raise ValueError("Asset kind mismatch")
            connection.execute("INSERT OR IGNORE INTO job_assets VALUES (?, ?)", (job_id, asset[0]))
            connection.execute("UPDATE assets SET cleanup_pending = 0 WHERE id = ?", (asset[0],))

        await self.database.transaction(track)

    async def finish_download(self, expected: Job, manifest: dict) -> Job | None:
        """Atomically record verified source metadata and enter preparation."""
        if expected.phase != Phase.DOWNLOAD or manifest["job_id"] != expected.id:
            raise ValueError("Invalid download result")

        def finish(connection: sqlite3.Connection) -> Job | None:
            cursor = connection.execute(
                """UPDATE jobs SET phase = 'prepare', size_bytes = ?, duration_seconds = ?,
                title = COALESCE(?, title), revision = revision + 1, updated_at = ?,
                attempts = 0, error_code = NULL
                WHERE id = ? AND revision = ? AND status = 'active'""",
                (
                    manifest["size_bytes"],
                    manifest["duration_seconds"],
                    manifest["title"],
                    self.clock(),
                    expected.id,
                    expected.revision,
                ),
            )
            if not cursor.rowcount:
                return None
            connection.execute(
                "UPDATE assets SET sha256 = ?, size_bytes = ? WHERE path = ? AND id IN "
                "(SELECT asset_id FROM job_assets WHERE job_id = ?)",
                (manifest["sha256"], manifest["size_bytes"], manifest["path"], expected.id),
            )
            return read_job(connection, expected.id)

        return await self.database.transaction(finish)

    async def defer(
        self, expected: Job, *, until: float, error_code: str, quota: bool = False
    ) -> Job | None:
        self._validate_error(error_code)
        if until <= self.clock():
            raise ValueError("Retry must be scheduled in the future")
        counter = "quota_attempts" if quota else "attempts"
        return await self._change(
            expected,
            f"status = ?, next_attempt_at = ?, error_code = ?, {counter} = {counter} + 1",
            (JobStatus.QUOTA_WAIT if quota else JobStatus.RETRY_WAIT, until, error_code),
        )

    @staticmethod
    def _validate_error(code: str) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", code):
            raise ValueError("Error code must not contain API output or URLs")

    async def fail(self, expected: Job, *, error_code: str) -> Job | None:
        self._validate_error(error_code)
        now = self.clock()
        return await self._change(
            expected,
            """status = 'failed', next_attempt_at = NULL, error_code = ?, finished_at = ?,
            retention_until = ?""",
            (error_code, now, now + self.retention_seconds),
            allowed_statuses=RUNNING,
        )

    async def complete(self, expected: Job) -> Job | None:
        """Completion requires independently confirmed delivery of both attachments."""

        def complete(connection: sqlite3.Connection) -> Job | None:
            job = read_job(connection, expected.id)
            if (
                job is None
                or job.revision != expected.revision
                or job.status != JobStatus.ACTIVE
                or job.phase != Phase.DELIVER
            ):
                return None
            sent = {
                row[0]
                for row in connection.execute(
                    "SELECT operation FROM outbox WHERE job_id = ? AND status = 'sent'",
                    (job.id,),
                )
            }
            if not {"txt", "json"} <= sent:
                return None
            now = self.clock()
            connection.execute(
                """UPDATE jobs SET status = 'completed', revision = revision + 1,
                updated_at = ?, finished_at = ?, resumable = 0, cleanup_pending = 1
                WHERE id = ? AND revision = ? AND status = 'active'""",
                (now, now, job.id, job.revision),
            )
            return read_job(connection, job.id)

        return await self.database.transaction(complete)

    async def _change(
        self,
        expected: Job,
        assignments: str,
        values: tuple,
        *,
        allowed_statuses: str = "('active')",
    ) -> Job | None:
        def change(connection: sqlite3.Connection) -> Job | None:
            cursor = connection.execute(
                f"""UPDATE jobs SET {assignments}, revision = revision + 1, updated_at = ?
                WHERE id = ? AND revision = ? AND status = ? AND status IN {allowed_statuses}""",
                (*values, self.clock(), expected.id, expected.revision, expected.status),
            )
            return read_job(connection, expected.id) if cursor.rowcount else None

        return await self.database.transaction(change)

    async def cancel(
        self,
        *,
        update_id: int,
        job_id: int,
        owner_id: int,
        generation: int,
        group_chat_id: int | None = None,
    ) -> UpdateResult:
        return await self.database.transaction(
            lambda connection: self._update(
                connection,
                update_id,
                lambda: self.cancel_in_transaction(
                    connection,
                    job_id=job_id,
                    owner_id=owner_id,
                    generation=generation,
                    group_chat_id=group_chat_id,
                ),
            )
        )

    def cancel_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        job_id: int,
        owner_id: int,
        generation: int,
        group_chat_id: int | None = None,
    ) -> UpdateResult:
        now = self.clock()
        cursor = connection.execute(
            f"""UPDATE jobs SET status = 'cancelled', revision = revision + 1,
            updated_at = ?, finished_at = ?, next_attempt_at = NULL,
            resumable = 0, cleanup_pending = 1
            WHERE id = ? AND owner_id = ? AND generation = ?
            AND (? IS NULL OR chat_id = ?) AND status IN {UNFINISHED}""",
            (now, now, job_id, owner_id, generation, group_chat_id, group_chat_id),
        )
        if not cursor.rowcount:
            return UpdateResult("unavailable", None)
        connection.execute(
            "UPDATE outbox SET status = 'cancelled' WHERE job_id = ? "
            "AND status = 'pending' AND operation != 'status'",
            (job_id,),
        )
        return UpdateResult("cancelled", job_id)

    async def resume(
        self,
        *,
        update_id: int,
        job_id: int,
        owner_id: int,
        generation: int,
        group_chat_id: int | None = None,
    ) -> UpdateResult:
        return await self.database.transaction(
            lambda connection: self._update(
                connection,
                update_id,
                lambda: self.resume_in_transaction(
                    connection,
                    job_id=job_id,
                    owner_id=owner_id,
                    generation=generation,
                    group_chat_id=group_chat_id,
                ),
            )
        )

    def resume_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        job_id: int,
        owner_id: int,
        generation: int,
        group_chat_id: int | None = None,
    ) -> UpdateResult:
        job = read_job(connection, job_id)
        now = self.clock()
        if (
            job is None
            or job.owner_id != owner_id
            or job.generation != generation
            or (group_chat_id is not None and job.chat_id != group_chat_id)
            or job.status != JobStatus.FAILED
            or not job.resumable
            or job.retention_until <= now
        ):
            return UpdateResult("unavailable", None)
        if limit := self._limit(connection, owner_id):
            return UpdateResult(limit, job_id)
        sequence = self._sequence(connection)
        connection.execute(
            """UPDATE jobs SET status = 'queued', queue_no = ?, revision = revision + 1,
            generation = generation + 1, attempts = 0, quota_attempts = 0,
            next_attempt_at = NULL, error_code = NULL, updated_at = ?, finished_at = NULL,
            retention_until = NULL WHERE id = ? AND revision = ? AND status = 'failed'""",
            (sequence, now, job_id, job.revision),
        )
        connection.execute(
            "UPDATE outbox SET status = 'cancelled' WHERE job_id = ? AND status = 'pending'",
            (job_id,),
        )
        return UpdateResult("resumed", job_id, self._position(connection, sequence))

    async def plan_chunks(self, expected: Job, boundaries: list[tuple[int, int]]) -> bool:
        if (
            not boundaries
            or boundaries[0][0] != 0
            or any(start < 0 or end <= start for start, end in boundaries)
            or any(left[1] != right[0] for left, right in pairwise(boundaries))
        ):
            raise ValueError("Chunk plan must be contiguous and start at zero")

        def plan(connection: sqlite3.Connection) -> bool:
            job = read_job(connection, expected.id)
            if (
                job is None
                or job.revision != expected.revision
                or job.status != JobStatus.ACTIVE
                or job.phase != Phase.PREPARE
            ):
                return False
            existing = list(
                connection.execute(
                    "SELECT start_sample, end_sample FROM chunks WHERE job_id = ? ORDER BY number",
                    (job.id,),
                )
            )
            if existing:
                if [tuple(row) for row in existing] != boundaries:
                    raise ValueError("Persisted chunk plan is immutable")
                return True
            connection.executemany(
                "INSERT INTO chunks (job_id, number, start_sample, end_sample) VALUES (?, ?, ?, ?)",
                [(job.id, number, start, end) for number, (start, end) in enumerate(boundaries)],
            )
            return True

        return await self.database.transaction(plan)

    async def save_chunk_response(
        self, expected: Job, *, number: int, path: str, sha256: str
    ) -> bool:
        """Record a file already published atomically; content never enters SQLite."""
        if not path or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise ValueError("Invalid response reference")

        def save(connection: sqlite3.Connection) -> bool:
            job = read_job(connection, expected.id)
            if (
                job is None
                or job.revision != expected.revision
                or job.status != JobStatus.ACTIVE
                or job.phase != Phase.TRANSCRIBE
            ):
                return False
            saved = bool(
                connection.execute(
                    """UPDATE chunks SET response_path = ?, response_sha256 = ?, completed_at = ?
                WHERE job_id = ? AND number = ? AND response_path IS NULL""",
                    (path, sha256, self.clock(), job.id, number),
                ).rowcount
            )
            if saved:
                connection.execute(
                    "UPDATE jobs SET attempts = 0, quota_attempts = 0, error_code = NULL "
                    "WHERE id = ?",
                    (job.id,),
                )
            return saved

        return await self.database.transaction(save)

    async def reset_chunk_attempts(self, expected: Job) -> Job | None:
        return await self._change(
            expected, "attempts = 0, quota_attempts = 0, error_code = NULL", ()
        )

    async def reserve_groq(
        self,
        expected: Job,
        *,
        number: int,
        seconds: float,
        hourly: float,
        daily: float,
    ) -> tuple[int | None, float | None]:
        """Atomically reserve account audio usage or return its next available time."""
        seconds = max(10, seconds)
        if not math.isfinite(seconds) or seconds > min(hourly, daily):
            raise ValueError("Invalid quota reservation")

        def reserve(c: sqlite3.Connection) -> tuple[int | None, float | None]:
            job = read_job(c, expected.id)
            if (
                job is None
                or job.revision != expected.revision
                or job.status != JobStatus.ACTIVE
                or job.phase != Phase.TRANSCRIBE
            ):
                return None, None
            now = self.clock()
            rows = list(
                c.execute(
                    "SELECT started_at, seconds FROM groq_usage WHERE started_at > ? "
                    "AND outcome != 'rejected' ORDER BY started_at",
                    (now - 86400,),
                )
            )
            until = now
            for window, limit in ((3600, hourly), (86400, daily)):
                active = [(timestamp, used) for timestamp, used in rows if timestamp > now - window]
                used = sum(used for _, used in active)
                for timestamp, amount in active:
                    if used + seconds <= limit:
                        break
                    used -= amount
                    until = max(until, timestamp + window)
            if until > now:
                return None, until
            usage_id = c.execute(
                "INSERT INTO groq_usage (job_id, chunk_number, started_at, seconds) "
                "VALUES (?, ?, ?, ?) RETURNING id",
                (job.id, number, now, seconds),
            ).fetchone()[0]
            return usage_id, None

        return await self.database.transaction(reserve)

    async def finish_groq_usage(self, usage_id: int, *, outcome: str) -> None:
        if outcome not in {"succeeded", "rejected"}:
            raise ValueError("Invalid usage outcome")
        await self.database.transaction(
            lambda c: c.execute(
                "UPDATE groq_usage SET outcome = ? WHERE id = ? AND outcome = 'uncertain'",
                (outcome, usage_id),
            )
        )

    async def chunks(self, job_id: int) -> list[Chunk]:
        return await self.database.call(
            lambda connection: [
                Chunk(**dict(row))
                for row in connection.execute(
                    "SELECT * FROM chunks WHERE job_id = ? ORDER BY number", (job_id,)
                )
            ]
        )

    async def retained_jobs(self) -> list[Job]:
        return await self.database.call(
            lambda c: [
                job_from_row(row)
                for row in c.execute(
                    "SELECT * FROM jobs WHERE status NOT IN ('cancelled', 'completed') "
                    "AND cleanup_pending = 0 ORDER BY id"
                )
            ]
        )

    async def cleanup_jobs(self) -> list[Job]:
        return await self.database.call(
            lambda c: [
                job_from_row(row)
                for row in c.execute("SELECT * FROM jobs WHERE cleanup_pending = 1 ORDER BY id")
            ]
        )

    async def expire_failed(self) -> None:
        def expire(c):
            c.execute(
                "UPDATE jobs SET resumable = 0, cleanup_pending = 1, revision = revision + 1, "
                "updated_at = ? WHERE status = 'failed' AND retention_until <= ? "
                "AND resumable = 1",
                (self.clock(), self.clock()),
            )
            c.execute(
                "UPDATE outbox SET status = 'cancelled' WHERE status = 'pending' "
                "AND operation != 'status' AND job_id IN "
                "(SELECT id FROM jobs WHERE cleanup_pending = 1)"
            )

        await self.database.transaction(expire)

    async def job_assets(self, job_id: int) -> list[dict]:
        return await self.database.call(
            lambda c: [
                dict(row)
                for row in c.execute(
                    "SELECT a.* FROM assets a JOIN job_assets j ON a.id = j.asset_id "
                    "WHERE j.job_id = ?",
                    (job_id,),
                )
            ]
        )

    async def cache_used_elsewhere(self, job_id: int, asset_id: int) -> bool:
        def used(c):
            # A queued request for the same file ID already needs this cached resource.
            c.execute(
                "INSERT OR IGNORE INTO job_assets (job_id, asset_id) "
                "SELECT id, ? FROM jobs WHERE id != ? AND source_kind = 'telegram' "
                "AND source_ref = (SELECT source_ref FROM jobs WHERE id = ?) "
                "AND cleanup_pending = 0 AND (status IN "
                "('queued', 'active', 'retry_wait', 'quota_wait') OR "
                "(status = 'failed' AND resumable = 1))",
                (asset_id, job_id, job_id),
            )
            return bool(
                c.execute(
                    "SELECT 1 FROM job_assets ja JOIN jobs j ON ja.job_id = j.id "
                    "WHERE ja.asset_id = ? AND j.id != ? AND j.cleanup_pending = 0 LIMIT 1",
                    (asset_id, job_id),
                ).fetchone()
            )

        return await self.database.transaction(used)

    async def finish_cleanup(self, expected: Job) -> None:
        def finish(c):
            job = read_job(c, expected.id)
            if job is None or not job.cleanup_pending or job.revision != expected.revision:
                return
            c.execute("DELETE FROM chunks WHERE job_id = ?", (job.id,))
            c.execute("DELETE FROM job_assets WHERE job_id = ?", (job.id,))
            c.execute(
                "DELETE FROM assets WHERE NOT EXISTS "
                "(SELECT 1 FROM job_assets WHERE asset_id = assets.id)"
            )
            c.execute("UPDATE outbox SET artifact_path = NULL WHERE job_id = ?", (job.id,))
            c.execute(
                "UPDATE jobs SET cleanup_pending = 0, updated_at = ? WHERE id = ?",
                (self.clock(), job.id),
            )

        await self.database.transaction(finish)

    async def reconciliation_failed(self, expected: Job) -> None:
        now = self.clock()
        await self.database.transaction(
            lambda c: c.execute(
                "UPDATE jobs SET status = 'failed', next_attempt_at = NULL, "
                "error_code = 'recovery_files', revision = revision + 1, updated_at = ?, "
                "finished_at = ?, retention_until = ? WHERE id = ? AND revision = ? "
                "AND status IN ('queued', 'active', 'retry_wait', 'quota_wait')",
                (now, now, now + self.retention_seconds, expected.id, expected.revision),
            )
        )
