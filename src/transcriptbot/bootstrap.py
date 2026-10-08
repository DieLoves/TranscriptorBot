"""Offline environment checks, before any external API activity."""

import importlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Literal

from transcriptbot.config import Settings

Role = Literal["bot", "worker"]


class EnvironmentError(Exception):
    """Safe startup failure, without environment values or library output."""


def check_directory(path: Path, *, create: bool, writable: bool) -> None:
    try:
        if create:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not path.is_dir() or not os.access(path, os.R_OK | os.X_OK):
            raise OSError
        if writable:
            with tempfile.TemporaryFile(dir=path) as probe:
                probe.write(b"startup check")
                probe.flush()
    except OSError:
        raise EnvironmentError("Каталог данных недоступен") from None


def check_executable(name: str) -> None:
    executable = shutil.which(name)
    if executable is None:
        raise EnvironmentError(f"Не найдена программа: {name}")
    try:
        result = subprocess.run(
            [executable, "--version" if name == "deno" else "-version"],
            env={"PATH": os.environ.get("PATH", ""), "LANG": "C.UTF-8"},
            capture_output=True,
            timeout=10,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        raise EnvironmentError(f"Программа не запускается: {name}") from None
    if name == "deno":
        match = re.search(rb"^deno (\d+)\.(\d+)\.(\d+)", result.stdout)
        if match is None or tuple(int(part) for part in match.groups()) < (2, 3, 0):
            raise EnvironmentError("Нужен Deno версии 2.3.0 или новее")


def check_environment(settings: Settings, role: Role) -> None:
    if sys.version_info[:2] != (3, 12):
        raise EnvironmentError("Требуется Python 3.12")
    for name in ("aiogram",) if role == "bot" else ("groq", "httpx", "httpcore", "yt_dlp"):
        try:
            importlib.import_module(name)
        except ImportError:
            raise EnvironmentError(f"Не установлена зависимость: {name}") from None
    check_database_environment(settings)
    check_directory(settings.work_dir, create=True, writable=True)
    check_directory(settings.telegram_files_dir, create=False, writable=role == "worker")
    if role == "worker":
        for name in ("ffmpeg", "ffprobe", "deno"):
            check_executable(name)


def check_database_environment(settings: Settings) -> None:
    check_directory(settings.sqlite_path.parent, create=True, writable=True)
    if settings.sqlite_path.exists() and (
        not settings.sqlite_path.is_file() or not os.access(settings.sqlite_path, os.R_OK | os.W_OK)
    ):
        raise EnvironmentError("База данных недоступна")
