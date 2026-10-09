"""Command line entry points; configuration errors are safe to print."""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path

from transcriptbot.bootstrap import (
    EnvironmentError,
    check_database_environment,
    check_environment,
)
from transcriptbot.config import ConfigurationError, Settings, load_settings
from transcriptbot.operations.backup import copy_database
from transcriptbot.operations.health import diagnostics
from transcriptbot.runtime import serve
from transcriptbot.storage.database import Database, StorageError
from transcriptbot.storage.repository import Repository
from transcriptbot.telegram.polling import TelegramRuntimeError


async def migrate(settings: Settings) -> None:
    database = await Database(settings.sqlite_path).open(migrate=True)
    await database.close()


async def healthcheck(settings: Settings, role: str) -> int:
    database = await Database(settings.sqlite_path).open()
    try:
        result = await diagnostics(settings, Repository(database), role)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result["healthy"] else 1
    finally:
        await database.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TranscriptBot")
    parser.add_argument(
        "role", choices=("bot", "worker", "migrate", "healthcheck", "backup", "restore")
    )
    parser.add_argument("--service", choices=("bot", "worker"), default="worker")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--check", action="store_true", help="Проверить настройки и окружение и выйти"
    )
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        settings = load_settings(args.env_file)
        if args.role in {"migrate", "healthcheck", "backup", "restore"}:
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
        elif args.role == "healthcheck":
            return asyncio.run(healthcheck(settings, args.service))
        elif args.role == "backup":
            if args.output is None:
                parser.error("backup требует --output")
            copy_database(settings.sqlite_path, args.output.absolute())
            print("Резервная копия SQLite создана и проверена")
        elif args.role == "restore":
            if args.input is None:
                parser.error("restore требует --input")
            copy_database(args.input.absolute(), settings.sqlite_path, restore=True)
            print("SQLite восстановлена и проверена")
        else:
            asyncio.run(serve(settings, args.role))
    except (StorageError, TelegramRuntimeError) as exc:
        parser.exit(2, f"{exc}\n")
    except OSError:
        parser.exit(2, "Недоступны файлы эксплуатационной проверки\n")
    except Exception:
        # Third-party exceptions may embed tokens, response bodies or private URLs.
        parser.exit(2, "Непредвиденная ошибка приложения; требуется проверка оператором\n")
    return 0
