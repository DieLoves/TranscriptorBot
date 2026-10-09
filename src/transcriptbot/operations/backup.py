"""SQLite backup API with validated, exclusive publication and safe diagnostics."""

import os
import sqlite3
import tempfile
import time
from pathlib import Path

from transcriptbot.downloaders.files import fsync_directory
from transcriptbot.storage.database import StorageError, check_schema


def copy_database(
    source: Path, target: Path, *, restore: bool = False, timeout: float = 60
) -> None:
    """Used for backup and restore; never overwrite an existing target database."""
    temporary = None
    try:
        if source.is_symlink() or not source.is_file() or target.exists() or target.is_symlink():
            raise StorageError("Источник недоступен или целевая база уже существует")
        if source.resolve() == target.resolve():
            raise StorageError("Источник и назначение должны различаться")
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Sidecars may belong to a running/damaged database; do not replace that state.
        if any(
            Path(str(target) + suffix).exists() or Path(str(target) + suffix).is_symlink()
            for suffix in ("-wal", "-shm")
        ):
            raise StorageError("Целевая база имеет служебные файлы; выберите пустое назначение")
        descriptor, name = tempfile.mkstemp(prefix=".sqlite-backup-", dir=target.parent)
        temporary = Path(name)
        os.close(descriptor)
        original = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=min(10, timeout))
        snapshot = sqlite3.connect(temporary)
        try:
            deadline = time.monotonic() + timeout

            def progress(status, remaining, total):
                if time.monotonic() >= deadline:
                    raise StorageError("Истёк срок копирования SQLite; повторите позже")

            original.backup(snapshot, pages=256, progress=progress, sleep=0.05)
            if snapshot.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise StorageError("Резервная копия не прошла проверку SQLite")
            if snapshot.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise StorageError("Резервная копия нарушает связи SQLite")
            snapshot.execute("PRAGMA journal_mode = DELETE")
            check_schema(snapshot, migrate=False)
            if restore:
                snapshot.execute("DELETE FROM runtime_health")
                snapshot.commit()
        finally:
            snapshot.close()
            original.close()
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.link(temporary, target, follow_symlinks=False)
        fsync_directory(target.parent)
    except (sqlite3.Error, OSError):
        raise StorageError("Не удалось скопировать SQLite; существующая база сохранена") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
