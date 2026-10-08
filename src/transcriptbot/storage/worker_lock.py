"""Linux advisory lock held for the entire worker process lifetime."""

import fcntl
import os
from pathlib import Path

from transcriptbot.storage.database import StorageError


class WorkerLock:
    def __init__(self, database_path: Path) -> None:
        self.path = database_path.with_name(database_path.name + ".worker.lock")
        self._descriptor: int | None = None

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise StorageError("Блокировка worker уже удерживается")
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        except OSError:
            raise StorageError("Не удалось открыть блокировку worker") from None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(descriptor)
            raise StorageError("Другой worker уже запущен или блокировка недоступна") from None
        self._descriptor = descriptor

    def release(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        # Never unlink: another worker could otherwise lock a different inode.
