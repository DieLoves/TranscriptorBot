"""Linux advisory lock held for the entire worker process lifetime."""

import fcntl
import os
import stat
from pathlib import Path

from transcriptbot.storage.database import StorageError


class LockBusy(StorageError):
    pass


class WorkerLock:
    def __init__(self, database_path: Path, *, role: str = "worker") -> None:
        if role not in {"bot", "worker"}:
            raise ValueError("Invalid process role")
        self.role = role
        self.path = database_path.with_name(database_path.name + f".{role}.lock")
        self._descriptor: int | None = None

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise StorageError("Блокировка worker уже удерживается")
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        except OSError:
            raise StorageError("Не удалось открыть блокировку worker") from None
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            raise LockBusy(f"Другой {self.role} уже запущен") from None
        except OSError:
            os.close(descriptor)
            raise StorageError(f"Блокировка {self.role} недоступна") from None
        self._descriptor = descriptor

    def release(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        # Never unlink: another worker could otherwise lock a different inode.


class DeliveryLock(WorkerLock):
    """Shared by bot I/O and worker cleanup; process death releases the lease."""

    def __init__(self, database_path: Path, job_id: int) -> None:
        super().__init__(database_path)
        if job_id <= 0:
            raise ValueError("Invalid job ID")
        # Outside the disposable workspace: this inode must never be unlinked.
        self.path = database_path.with_name(database_path.name + f".delivery-{job_id}.lock")

    def try_acquire(self) -> bool:
        try:
            self.acquire()
        except LockBusy:
            return False
        return True
