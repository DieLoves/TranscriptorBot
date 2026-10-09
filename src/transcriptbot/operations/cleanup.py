"""Recoverable cleanup; never remove files held by active worker operations."""

import logging
from pathlib import Path

from transcriptbot.config import Settings
from transcriptbot.operations.files import (
    UnsafeCleanupPath,
    remove_directory,
    unlink_file,
    workspace_files,
)
from transcriptbot.storage.repository import Repository
from transcriptbot.storage.worker_lock import DeliveryLock

logger = logging.getLogger(__name__)


class Cleanup:
    def __init__(self, settings: Settings, repository: Repository):
        self.settings, self.repository = settings, repository
        self.next_scan = 0.0
        self.retry_at: dict[int, float] = {}
        self.busy: set[int] = set()
        self.cache_busy: set[int] = set()

    async def run(self, *, startup: bool = False) -> None:
        now = self.repository.clock()
        scan = startup or now >= self.next_scan
        if scan:
            self.next_scan = now + self.settings.cleanup_interval_seconds
            await self.repository.expire_failed()
            await self.repository.prune_inbox()
        errors = 0
        if startup:
            # Only known jobs: unknown directories and all Bot API internals are untouched.
            for job in await self.repository.retained_jobs():
                directory = self.settings.work_dir / str(job.id)
                try:
                    for path in workspace_files(directory, self.settings.work_dir):
                        if path.name.endswith(".part") or path.name == "current.wav":
                            unlink_file(path, directory)
                    await self.reconcile(job, directory)
                except OSError:
                    errors += 1
                    logger.warning("job_id=%s event=reconcile_failed error_code=cleanup_io", job.id)
        for job in await self.repository.cleanup_jobs():
            if job.id in self.busy or now < self.retry_at.get(job.id, 0):
                continue
            directory = self.settings.work_dir / str(job.id)
            lease = DeliveryLock(self.settings.sqlite_path, job.id)
            if not lease.try_acquire():
                continue
            try:
                assets = await self.repository.job_assets(job.id)
                if self.cache_busy and any(asset["kind"] == "telegram_cache" for asset in assets):
                    continue
                for path in workspace_files(directory, self.settings.work_dir):
                    unlink_file(path, directory)
                # Check tracked paths too, so unsafe metadata cannot silently disappear.
                blocked = False
                for asset in assets:
                    if asset["kind"] == "workspace":
                        unlink_file(Path(asset["path"]), directory)
                    elif not await self.repository.cache_used_elsewhere(job.id, asset["id"]):
                        if self.cache_busy:
                            blocked = True
                            break
                        unlink_file(
                            Path(asset["path"]), self.settings.telegram_files_dir, cache=True
                        )
                if blocked:
                    continue
                remove_directory(directory, self.settings.work_dir)
                await self.repository.finish_cleanup(job)
                self.retry_at.pop(job.id, None)
            except OSError as exc:
                self.retry_at[job.id] = now + self.settings.cleanup_interval_seconds
                code = "cleanup_unsafe_path" if isinstance(exc, UnsafeCleanupPath) else "cleanup_io"
                logger.warning("job_id=%s event=cleanup_failed error_code=%s", job.id, code)
            finally:
                lease.release()
        errors += len(self.retry_at)
        if startup or scan or errors or await self.repository.cleanup_jobs():
            await self.repository.database.transaction(
                lambda c: c.execute(
                    "UPDATE maintenance_state SET checked_at = ?, error_count = ?, "
                    "error_code = ? WHERE singleton = 1",
                    (now, errors, "cleanup_failed" if errors else None),
                )
            )

    async def reconcile(self, job, directory: Path) -> None:
        """Validate published checkpoints using the same invariants as normal recovery."""
        from transcriptbot.downloaders.errors import DownloadError
        from transcriptbot.media.errors import PipelineError
        from transcriptbot.media.service import MediaPipeline

        pipeline = MediaPipeline(self.settings, self.repository, None)
        try:
            if job.phase == "download":
                return  # SourceDownloader accepts any already published source.json.
            if job.phase == "deliver":
                from transcriptbot.downloaders.files import regular_path

                for path in pipeline.result_paths(directory):
                    regular_path(path, directory)
                return
            manifest = await pipeline.source(job, directory)
            for chunk in await self.repository.chunks(job.id):
                path = directory / f"response-{chunk.number:04d}.json"
                if chunk.response_path is not None or path.exists() or path.is_symlink():
                    pipeline.response(job, manifest, chunk, path)
        except (PipelineError, DownloadError, OSError):
            await self.repository.reconciliation_failed(job)
