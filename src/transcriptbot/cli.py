"""Command line entry points; configuration errors are safe to print."""

import argparse
import asyncio
import logging
import os
from pathlib import Path

from transcriptbot.bootstrap import (
    EnvironmentError,
    check_database_environment,
    check_environment,
)
from transcriptbot.config import ConfigurationError, Settings, load_settings
from transcriptbot.runtime import serve
from transcriptbot.storage.database import Database, StorageError


async def migrate(settings: Settings) -> None:
    database = await Database(settings.sqlite_path).open(migrate=True)
    await database.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TranscriptBot")
    parser.add_argument("role", choices=("bot", "worker", "migrate"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--check", action="store_true", help="Проверить настройки и окружение и выйти"
    )
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        settings = load_settings(args.env_file)
        if args.role == "migrate":
            check_database_environment(settings)
        else:
            check_environment(settings, args.role)
    except (ConfigurationError, EnvironmentError) as exc:
        parser.exit(2, f"{exc}\n")
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Libraries may log URLs, tokens and raw responses, especially in DEBUG mode.
    for name in ("aiogram", "aiohttp", "httpx", "httpcore", "groq", "yt_dlp"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    if args.check:
        print(f"Окружение {args.role} проверено")
        return 0
    try:
        if args.role == "migrate":
            asyncio.run(migrate(settings))
            print("Миграции SQLite применены")
        else:
            asyncio.run(serve(settings, args.role))
    except StorageError as exc:
        parser.exit(2, f"{exc}\n")
    return 0
