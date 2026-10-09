import os
import subprocess
import sys
from pathlib import Path

import pytest

from transcriptbot.storage.database import StorageError
from transcriptbot.storage.worker_lock import WorkerLock


def test_lock_excludes_second_worker_and_can_be_reopened(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite3"
    first, second = WorkerLock(path), WorkerLock(path)
    first.acquire()
    assert first.path.stat().st_mode & 0o777 == 0o600
    try:
        with pytest.raises(StorageError, match="Другой worker"):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()
    assert first.path.exists()


def test_process_death_releases_lock(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite3"
    code = (
        "import sys, time; from pathlib import Path; "
        "from transcriptbot.storage.worker_lock import WorkerLock; "
        "lock = WorkerLock(Path(sys.argv[1])); lock.acquire(); "
        "print('locked', flush=True); time.sleep(30)"
    )
    with subprocess.Popen(
        [sys.executable, "-c", code, str(path)],
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert process.stdout.readline().strip() == "locked"
            second = WorkerLock(path)
            with pytest.raises(StorageError, match="Другой worker"):
                second.acquire()
            process.kill()
            process.wait(timeout=5)
            second.acquire()
            second.release()
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_lock_does_not_follow_symlink(tmp_path: Path) -> None:
    lock = WorkerLock(tmp_path / "db.sqlite3")
    other = tmp_path / "important"
    other.write_text("preserved")
    lock.path.symlink_to(other)
    with pytest.raises(StorageError):
        lock.acquire()
    assert other.read_text() == "preserved"


def test_lock_descriptor_is_not_inherited(tmp_path: Path) -> None:
    lock = WorkerLock(tmp_path / "db.sqlite3")
    lock.acquire()
    try:
        assert not os.get_inheritable(lock._descriptor)
    finally:
        lock.release()


def test_bot_lock_prevents_parallel_delivery_and_does_not_block_worker(tmp_path):
    first = WorkerLock(tmp_path / "db", role="bot")
    second = WorkerLock(tmp_path / "db", role="bot")
    worker = WorkerLock(tmp_path / "db")
    first.acquire()
    try:
        with pytest.raises(StorageError, match="Другой bot"):
            second.acquire()
        worker.acquire()
        worker.release()
    finally:
        first.release()
    second.acquire()
    second.release()


def test_delivery_lock_is_exclusive_per_job_and_reusable(tmp_path):
    from transcriptbot.storage.worker_lock import DeliveryLock

    first = DeliveryLock(tmp_path / "db", 1)
    same = DeliveryLock(tmp_path / "db", 1)
    other = DeliveryLock(tmp_path / "db", 2)
    assert first.try_acquire()
    try:
        assert not same.try_acquire()
        assert other.try_acquire()
        other.release()
    finally:
        first.release()
    assert same.try_acquire()
    same.release()
