"""Liveness and metadata-only diagnostics; no network or extra web server."""

import asyncio
import shutil

from transcriptbot.config import Settings
from transcriptbot.storage.repository import Repository


async def heartbeat(repository: Repository, role: str) -> None:
    while True:
        await repository.database.transaction(
            lambda c: c.execute(
                "INSERT INTO runtime_health VALUES (?, ?) ON CONFLICT(role) "
                "DO UPDATE SET heartbeat_at = excluded.heartbeat_at",
                (role, repository.clock()),
            )
        )
        await asyncio.sleep(5)


async def diagnostics(settings: Settings, repository: Repository, role: str) -> dict:
    now = repository.clock()

    def inspect(c):
        value = c.execute(
            "SELECT heartbeat_at FROM runtime_health WHERE role = ?", (role,)
        ).fetchone()
        age = max(0, now - value[0]) if value else None
        current = c.execute(
            "SELECT id, phase, status, created_at, next_attempt_at FROM jobs "
            "WHERE status IN ('active', 'retry_wait', 'quota_wait') LIMIT 1"
        ).fetchone()
        maintenance = c.execute("SELECT * FROM maintenance_state WHERE singleton = 1").fetchone()
        return {
            "role": role,
            "healthy": age is not None and age <= 60,
            "heartbeat_age_seconds": age,
            "queue_size": c.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN "
                "('queued', 'active', 'retry_wait', 'quota_wait')"
            ).fetchone()[0],
            "current_job": {
                "id": current["id"],
                "phase": current["phase"],
                "status": current["status"],
                "age_seconds": max(0, now - current["created_at"]),
                "wait_remaining_seconds": max(0, (current["next_attempt_at"] or now) - now),
            }
            if current
            else None,
            "cleanup_pending": c.execute(
                "SELECT COUNT(*) FROM jobs WHERE cleanup_pending = 1"
            ).fetchone()[0],
            "cleanup_errors": maintenance["error_count"],
            "cleanup_checked_at": maintenance["checked_at"],
        }

    result = await repository.database.call(inspect)
    result["disk_free_bytes"] = {
        "data": shutil.disk_usage(settings.sqlite_path.parent).free,
        "work": shutil.disk_usage(settings.work_dir).free,
        "telegram": shutil.disk_usage(settings.telegram_files_dir).free,
    }
    result["download_space_available"] = all(
        free >= settings.max_input_bytes + 41_000_000 + settings.disk_reserve_bytes
        for key, free in result["disk_free_bytes"].items()
        if key != "data"
    )
    return result
