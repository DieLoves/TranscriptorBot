"""Durable Telegram inbox/outbox. No external request occurs inside a transaction."""

import json
import sqlite3
from pathlib import Path

from transcriptbot.retries import retry_after
from transcriptbot.storage.models import Job, JobStatus, Phase, UpdateResult
from transcriptbot.storage.repository import Repository, job_from_row, read_job
from transcriptbot.telegram.presentation import (
    OUTCOMES,
    jobs_payload,
    outcome_payload,
    status_payload,
)
from transcriptbot.telegram.requests import HELP, Request


class TelegramStore:
    def __init__(self, repository: Repository, *, status_interval: float = 5) -> None:
        self.repository = repository
        self.database = repository.database
        self.clock = repository.clock
        self.status_interval = status_interval

    async def offset(self) -> int:
        return await self.database.call(
            lambda c: c.execute("SELECT next_offset FROM polling_state").fetchone()[0]
        )

    def _status(self, c: sqlite3.Connection, job: Job) -> dict:
        done, total = c.execute(
            "SELECT COALESCE(SUM(response_path IS NOT NULL), 0), COUNT(*) "
            "FROM chunks WHERE job_id = ?",
            (job.id,),
        ).fetchone()
        position = self.repository._position(c, job.queue_no)
        return status_payload(job, position, done, total, self.clock())

    def _enqueue_status(self, c: sqlite3.Connection, job: Job) -> None:
        payload = json.dumps(self._status(c, job), ensure_ascii=False, sort_keys=True)
        c.execute(
            """INSERT INTO outbox
            (job_id, generation, operation, chat_id, topic_id, payload,
             status_revision, last_sent_at)
            VALUES (?, ?, 'status', ?, ?, ?, ?,
                (SELECT MAX(last_sent_at) FROM outbox WHERE job_id = ? AND operation = 'status'))
            ON CONFLICT(job_id, generation, operation) DO UPDATE SET
            status_revision = excluded.status_revision, payload = excluded.payload,
            status = CASE WHEN outbox.payload IS excluded.payload THEN outbox.status
                          ELSE 'pending' END,
            telegram_message_id = CASE WHEN outbox.payload IS excluded.payload
                                       THEN outbox.telegram_message_id ELSE NULL END,
            attempts = CASE WHEN outbox.status = 'sent' THEN 0 ELSE outbox.attempts END,
            next_attempt_at = CASE WHEN outbox.status = 'sent' THEN NULL
                                   ELSE outbox.next_attempt_at END
            WHERE outbox.status != 'cancelled' AND
                (outbox.payload IS NOT excluded.payload
                 OR outbox.status_revision IS NOT excluded.status_revision)""",
            (job.id, job.generation, job.chat_id, job.topic_id, payload, job.revision, job.id),
        )

    async def accept(self, update_id: int, request: Request) -> UpdateResult:
        repo = self.repository

        def accept(c: sqlite3.Connection) -> UpdateResult:
            old = c.execute(
                "SELECT outcome, job_id, queue_position FROM processed_updates WHERE update_id = ?",
                (update_id,),
            ).fetchone()
            if old is not None:
                c.execute(
                    "UPDATE polling_state SET next_offset = MAX(next_offset, ?) "
                    "WHERE singleton = 1",
                    (update_id + 1,),
                )
                return UpdateResult(**dict(old))
            # Processed inbox rows can expire; the committed offset is the durable
            # replay barrier. Polling delivers updates sequentially.
            if update_id < c.execute("SELECT next_offset FROM polling_state").fetchone()[0]:
                return UpdateResult("ignored", None)
            action = request.action
            result = UpdateResult(action, None)
            text = OUTCOMES.get(action)
            reply_payload = None
            if action == "create":
                result = repo.create_job_in_transaction(
                    c,
                    owner_id=request.owner_id,
                    chat_id=request.chat_id,
                    message_id=request.message_id,
                    topic_id=request.topic_id,
                    source=request.source,
                )
                if result.job_id:
                    c.execute(
                        "UPDATE jobs SET size_bytes = ?, duration_seconds = ?, "
                        "source_message_id = ? WHERE id = ?",
                        (
                            request.size_bytes,
                            request.duration_seconds,
                            request.source_message_id or request.message_id,
                            result.job_id,
                        ),
                    )
                text = OUTCOMES.get(result.outcome)
            elif action in {"cancel", "resume"}:
                operation = (
                    repo.cancel_in_transaction if action == "cancel" else repo.resume_in_transaction
                )
                result = operation(
                    c,
                    job_id=request.job_id,
                    owner_id=request.owner_id,
                    generation=request.generation,
                    group_chat_id=request.group_chat_id,
                )
                text = OUTCOMES[result.outcome]
            elif action == "help":
                text = HELP
                reply_payload = {"text": HELP, "parse_mode": "HTML"}
            elif action in {"status", "jobs"}:
                rows = list(
                    c.execute(
                        """SELECT * FROM jobs WHERE owner_id = ? AND (? IS NULL OR chat_id = ?)
                    AND (? IS NULL OR id = ?) ORDER BY id DESC LIMIT 10""",
                        (
                            request.owner_id,
                            request.group_chat_id,
                            request.group_chat_id,
                            request.job_id,
                            request.job_id,
                        ),
                    )
                )
                if action == "status":
                    job = job_from_row(rows[0]) if rows else None
                    reply_payload = self._status(c, job) if job else outcome_payload("unavailable")
                    text = reply_payload["text"]
                else:
                    reply_payload = jobs_payload([job_from_row(row) for row in rows])
                    text = reply_payload["text"]
            c.execute(
                "INSERT INTO processed_updates VALUES (?, ?, ?, ?, ?)",
                (update_id, result.outcome, result.job_id, result.queue_position, self.clock()),
            )
            if result.job_id is not None and result.outcome in {"created", "cancelled", "resumed"}:
                self._enqueue_status(c, read_job(c, result.job_id))
            reply_needed = request.callback_id or (text and request.chat_id is not None)
            pending = (
                c.execute(
                    "SELECT COUNT(*) FROM (SELECT 1 FROM outbox "
                    "WHERE job_id IS NULL AND status = 'pending' AND chat_id IS ? LIMIT 10)",
                    (request.chat_id,),
                ).fetchone()[0]
                if reply_needed
                else 0
            )
            total_pending = (
                c.execute(
                    "SELECT COUNT(*) FROM (SELECT 1 FROM outbox "
                    "WHERE job_id IS NULL AND status = 'pending' LIMIT 1000)"
                ).fetchone()[0]
                if reply_needed and pending < 10
                else 1000
            )
            if reply_needed and pending < 10 and total_pending < 1000:
                if request.callback_id:
                    # Callback notifications do not support HTML formatting.
                    reply_payload = {"text": text or "Готово."}
                elif reply_payload is None and result.outcome in OUTCOMES:
                    reply_payload = outcome_payload(result.outcome)
                # Only UI text enters SQLite; previews and transcripts remain in files.
                c.execute(
                    """INSERT INTO outbox
                    (operation, chat_id, topic_id, payload, callback_id, update_id)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        "callback" if request.callback_id else "reply",
                        request.chat_id,
                        request.topic_id,
                        json.dumps(
                            reply_payload or {"text": text or "Готово."}, ensure_ascii=False
                        ),
                        request.callback_id,
                        update_id,
                    ),
                )
            c.execute(
                "UPDATE polling_state SET next_offset = MAX(next_offset, ?) WHERE singleton = 1",
                (update_id + 1,),
            )
            return result

        return await self.database.transaction(accept)

    async def refresh_statuses(self) -> None:
        def refresh(c: sqlite3.Connection) -> None:
            # Include terminal rows until their latest state has been reflected in the status.
            candidates = """SELECT id FROM jobs WHERE status IN
                ('queued', 'active', 'retry_wait', 'quota_wait')
                UNION SELECT job_id FROM dirty_status
                UNION SELECT id FROM jobs INDEXED BY jobs_failed_retention
                    WHERE status = 'failed' AND resumable = 1
                    AND retention_until <= ?"""
            for row in list(
                c.execute(
                    f"""SELECT j.* FROM jobs j LEFT JOIN outbox o ON o.job_id = j.id
                AND o.generation = j.generation AND o.operation = 'status'
                WHERE j.id IN ({candidates}) AND (o.id IS NULL OR (o.status != 'cancelled' AND (
                    j.status IN ('queued', 'active', 'retry_wait', 'quota_wait')
                    OR o.status = 'pending' OR o.status_revision IS NOT j.revision
                    OR (j.status = 'failed' AND j.retention_until <= ?
                        AND COALESCE(o.last_sent_at, 0) < j.retention_until))))""",
                    (self.clock(), self.clock()),
                )
            ):
                self._enqueue_status(c, job_from_row(row))
            c.execute("DELETE FROM dirty_status")

        await self.database.transaction(refresh)
        # Reconcile a crash between confirmation of the final file and job completion.
        job = await self.repository.active_job()
        if job is not None and job.phase == Phase.DELIVER:
            preview = await self.database.call(
                lambda c: c.execute(
                    "SELECT 1 FROM outbox WHERE job_id = ? "
                    "AND operation = 'preview' AND status = 'sent'",
                    (job.id,),
                ).fetchone()
            )
            if preview:
                await self.repository.complete(job)

    async def enqueue_results(self, expected: Job, *, txt: Path, verbose_json: Path) -> bool:
        """Worker publishes artifacts first, then registers delivery of an active deliver phase."""

        def enqueue(c: sqlite3.Connection) -> bool:
            job = read_job(c, expected.id)
            if (
                job is None
                or job.revision != expected.revision
                or job.status != JobStatus.ACTIVE
                or job.phase != Phase.DELIVER
            ):
                return False
            for operation, path in (("preview", txt), ("txt", txt), ("json", verbose_json)):
                if c.execute(
                    "SELECT 1 FROM outbox WHERE job_id = ? AND operation = ? AND status = 'sent'",
                    (job.id, operation),
                ).fetchone():
                    continue
                c.execute(
                    """INSERT INTO outbox
                    (job_id, generation, operation, artifact_path, chat_id, topic_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(job_id, generation, operation) DO NOTHING""",
                    (job.id, job.generation, operation, str(path), job.chat_id, job.topic_id),
                )
            return True

        return await self.database.transaction(enqueue)

    async def next_operation(self) -> dict | None:
        now = self.clock()

        def next_operation(c: sqlite3.Connection) -> dict | None:
            row = c.execute(
                """SELECT o.* FROM outbox o LEFT JOIN jobs j ON j.id = o.job_id
                LEFT JOIN chat_send_state s ON s.chat_id = o.chat_id
                WHERE o.status = 'pending' AND COALESCE(o.next_attempt_at, 0) <= ?
                AND (o.operation = 'callback' OR COALESCE(s.next_send_at, 0) <= ?)
                AND (o.operation != 'status' OR o.last_sent_at IS NULL OR o.last_sent_at + ? <= ?)
                AND (o.job_id IS NULL OR (o.generation = j.generation AND
                    (o.operation = 'status' OR (j.status = 'active' AND j.phase = 'deliver'))))
                AND (o.operation NOT IN ('txt', 'json') OR EXISTS
                    (SELECT 1 FROM outbox p WHERE p.job_id = o.job_id AND p.operation = 'preview'
                     AND p.status = 'sent'))
                AND (o.operation != 'json' OR EXISTS
                    (SELECT 1 FROM outbox t WHERE t.job_id = o.job_id AND t.operation = 'txt'
                     AND t.status = 'sent'))
                ORDER BY (o.operation = 'callback') DESC,
                    (o.job_id IS NOT NULL) DESC, o.id LIMIT 1""",
                (now, now, self.status_interval, now),
            ).fetchone()
            return dict(row) if row else None

        return await self.database.call(next_operation)

    async def current_job(self, operation: dict) -> Job | None:
        return await self.database.call(lambda c: read_job(c, operation["job_id"]))

    async def confirm(self, operation: dict, message_id: int) -> None:
        def confirm(c: sqlite3.Connection) -> None:
            now = self.clock()
            # Remember an external send even if a newer status was coalesced while awaiting it.
            if operation["operation"] == "status":
                c.execute(
                    "UPDATE jobs SET status_message_id = ? WHERE id = ?",
                    (message_id, operation["job_id"]),
                )
            c.execute(
                """UPDATE outbox SET status = 'sent', telegram_message_id = ?, last_sent_at = ?,
                next_attempt_at = NULL WHERE id = ? AND status = 'pending' AND payload IS ?""",
                (message_id, now, operation["id"], operation["payload"]),
            )
            if operation["operation"] == "status":
                c.execute("UPDATE outbox SET last_sent_at = ? WHERE id = ?", (now, operation["id"]))
            if operation["chat_id"] is not None and operation["operation"] != "callback":
                delay = 3 if operation["chat_id"] < 0 else 1
                c.execute(
                    "INSERT INTO chat_send_state VALUES (?, ?) ON CONFLICT(chat_id) "
                    "DO UPDATE SET next_send_at = excluded.next_send_at",
                    (operation["chat_id"], now + delay),
                )

        await self.database.transaction(confirm)

    async def retry(self, operation: dict, *, delay: float, count: bool) -> None:
        def retry(c: sqlite3.Connection) -> None:
            until = self.clock() + (retry_after(str(delay), self.clock()) or 60)
            c.execute(
                "UPDATE outbox SET next_attempt_at = ?, attempts = attempts + ? "
                "WHERE id = ? AND status = 'pending'",
                (until, int(count), operation["id"]),
            )
            if not count and operation["chat_id"] is not None:
                c.execute(
                    "INSERT INTO chat_send_state VALUES (?, ?) ON CONFLICT(chat_id) "
                    "DO UPDATE SET next_send_at = MAX(next_send_at, excluded.next_send_at)",
                    (operation["chat_id"], until),
                )

        await self.database.transaction(retry)

    async def replace_status(self, operation: dict) -> None:
        await self.database.transaction(
            lambda c: c.execute(
                "UPDATE jobs SET status_message_id = NULL WHERE id = ?", (operation["job_id"],)
            )
        )

    async def reject_delivery(self, operation: dict, code: str) -> None:
        def reject(c: sqlite3.Connection) -> None:
            now = self.clock()
            rejected = c.execute(
                "UPDATE outbox SET status = 'cancelled' WHERE id = ? AND status = 'pending' "
                "AND payload IS ?",
                (operation["id"], operation["payload"]),
            )
            if not rejected.rowcount or operation["job_id"] is None:
                return
            job = read_job(c, operation["job_id"])
            if job.generation != operation["generation"]:
                return
            c.execute(
                """UPDATE jobs SET status = 'failed', revision = revision + 1, error_code = ?,
                next_attempt_at = NULL, finished_at = ?, retention_until = ?, updated_at = ?
                WHERE id = ? AND status IN ('queued', 'active', 'retry_wait', 'quota_wait')""",
                (code, now, now + self.repository.retention_seconds, now, job.id),
            )
            c.execute(
                "UPDATE outbox SET status = 'cancelled' WHERE job_id = ? "
                "AND status = 'pending' AND operation != 'status'",
                (job.id,),
            )

        await self.database.transaction(reject)
