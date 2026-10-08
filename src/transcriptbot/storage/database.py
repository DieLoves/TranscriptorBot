"""One SQLite connection on a dedicated thread, with explicit transactions."""

import asyncio
import hashlib
import os
import sqlite3
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")


class StorageError(Exception):
    """Safe diagnostics: never expose database paths, SQL or source URLs."""


def migration_scripts() -> list[tuple[int, str, str]]:
    scripts = []
    for path in sorted(files("transcriptbot.storage").joinpath("migrations").iterdir()):
        if path.name.endswith(".sql"):
            sql = path.read_text(encoding="utf-8")
            scripts.append(
                (int(path.name.split("_", 1)[0]), sql, hashlib.sha256(sql.encode()).hexdigest())
            )
    return scripts


def check_schema(connection: sqlite3.Connection, *, migrate: bool) -> None:
    scripts = migration_scripts()
    connection.execute("BEGIN IMMEDIATE" if migrate else "BEGIN")
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'schema_migrations' AND type = 'table'"
        ).fetchone()
        applied = (
            dict(connection.execute("SELECT version, checksum FROM schema_migrations"))
            if exists
            else {}
        )
        expected = {version: checksum for version, _, checksum in scripts}
        if any(expected.get(version) != checksum for version, checksum in applied.items()):
            raise StorageError("Версия или контрольная сумма миграций не поддерживается")
        if list(sorted(applied)) != list(expected)[: len(applied)]:
            raise StorageError("Нарушен порядок миграций")
        if not migrate and applied != expected:
            raise StorageError("Нужны миграции: выполните transcriptbot migrate")
        for version, sql, checksum in scripts if migrate else []:
            if version in applied:
                continue
            # executescript commits an existing transaction, so execute statements individually.
            statement = ""
            for line in sql.splitlines(keepends=True):
                statement += line
                if sqlite3.complete_statement(statement):
                    connection.execute(statement)
                    statement = ""
            if statement.strip():
                raise StorageError("Незавершённая SQL-миграция")
            connection.execute(
                "INSERT INTO schema_migrations VALUES (?, ?, ?)", (version, checksum, time.time())
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite")
        self._connection: sqlite3.Connection | None = None

    async def open(self, *, migrate: bool = False) -> "Database":
        try:
            await self.call(lambda _: self._open(migrate))
        except BaseException:
            await self.close()
            raise
        return self

    def _open(self, migrate: bool) -> None:
        if self._connection is not None:
            raise StorageError("Соединение с базой уже открыто")
        if migrate:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            os.close(descriptor)
        elif not self.path.is_file():
            raise StorageError("Нужны миграции: выполните transcriptbot migrate")
        self.path.chmod(0o600)
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        self._connection = connection
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        if connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] != "wal":
            raise StorageError("Не удалось включить SQLite WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        check_schema(connection, migrate=migrate)
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(self.path) + suffix)
            if sidecar.exists():
                sidecar.chmod(0o600)

    async def call(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        def execute() -> T:
            try:
                return operation(self._connection)  # Connection only used on this thread.
            except (sqlite3.Error, OSError):
                raise StorageError("Ошибка доступа к SQLite") from None

        return await asyncio.get_running_loop().run_in_executor(self._executor, execute)

    async def transaction(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        def execute(connection: sqlite3.Connection) -> T:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = operation(connection)
                connection.commit()
                return result
            except BaseException:
                connection.rollback()
                raise

        return await self.call(execute)

    async def close(self) -> None:
        def close_connection() -> None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

        await asyncio.get_running_loop().run_in_executor(self._executor, close_connection)
        self._executor.shutdown(wait=True)
