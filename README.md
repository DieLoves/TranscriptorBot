# TranscriptorBot

Telegram-бот для транскрипции аудио и видео через Groq с моделью
`whisper-large-v3-turbo`.

Планируемые источники: вложения Telegram, YouTube, публичные ссылки Облака
Mail.ru и прямые ссылки на медиафайлы. Результаты: превью текста, полный TXT
и verbose JSON.

## Документация

- [Спецификация v1](docs/specs/transcriptbot-v1.md)
- [Технический план реализации](docs/plans/transcriptbot-v1-implementation.md)

## Состояние проекта

Реализован каркас этапа 1: конфигурация, команды `bot` и `worker`, проверка
окружения и завершение по SIGTERM/SIGINT, зафиксированные зависимости и
контейнерные образы. Процессы пока ожидают сигнал остановки: Telegram polling
и обработка заданий будут подключены на следующих этапах.

Согласованный стек: Python 3.12, aiogram 3, Groq SDK, FFmpeg/FFprobe,
yt-dlp, HTTPX, SQLite и Docker Compose.

## Настройки

Шаблон настроек находится в [.env.example](.env.example). Рабочие значения
и ключи хранятся в локальном `.env`, исключённом из Git. Параметры Telegram
и список разрешённых Telegram ID нужно заполнить перед запуском.

`ALLOWED_USER_IDS` — положительные ID через запятую. Модель фиксирована
спецификацией. Приложение читает `.env` из текущего каталога; переменные
окружения имеют приоритет. Неверные настройки завершают процесс с кодом 2,
без вывода значений секретов.

## Локальная разработка и проверки

Нужны Python 3.12 и uv. Для worker также нужны FFmpeg, FFprobe и Deno
версии от 2.3.0. В контейнерном образе эти программы уже установлены.

```bash
uv sync --locked
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check .
```

Пути в шаблоне рассчитаны на контейнеры. Для локального запуска каркаса
можно переопределить пути, не изменяя секреты в `.env`:

```bash
mkdir -p var/data var/jobs var/telegram
SQLITE_PATH="$PWD/var/data/transcriptbot.sqlite3" \
WORK_DIR="$PWD/var/jobs" TELEGRAM_FILES_DIR="$PWD/var/telegram" \
uv run --locked transcriptbot bot --check
```

Без `--check` процесс остаётся запущенным до SIGTERM или Ctrl+C.
Для другого файла настроек доступен `--env-file /absolute/path/to/settings.env`.
Проверка окружения не обращается к Telegram или Groq и не создаёт базу:
миграции относятся к этапу 2.

## Контейнерное окружение

```bash
docker compose config --quiet
docker compose build
python3 scripts/check_containers.py
```

Последняя команда запускает временные контейнеры с вымышленными ключами,
без сети, проверяет обе команды, общие тома и SIGTERM; затем удаляет только
созданные ею контейнеры и тома. Рабочие сервисы при этом не запускаются.

Compose описывает три сервиса: бот, один worker и официальный Telegram Bot API
в режиме `--local`. Сервер собирается из
[официальных исходников](https://github.com/tdlib/telegram-bot-api)
на commit `e3e9dd8e5b3d7ab8537cd5a10dc31d5ffa8f82d1` с закреплённым TDLib.
Образы Python, uv и Deno закреплены digest; системные пакеты берутся из
снимка Debian от 2026-10-01. Python-пакеты и их хеши находятся в `uv.lock`.

Все сервисы используют UID/GID `10001:10001`. Bot API не публикует порты
на хост. Постоянные тома `data`, `work` и `telegram` хранят SQLite,
рабочие файлы и кеш/служебные данные сервера. Бот получает том `telegram`
только для чтения, worker — также для будущей очистки учтённых файлов.
Не удаляйте эти тома командой `docker compose down -v` на рабочем экземпляре.

В общем образе приложения доступны FFmpeg/FFprobe, yt-dlp и Deno 2.9.7.
Установлен `yt-dlp[default]` с локальным EJS, согласно
[инструкции yt-dlp](https://github.com/yt-dlp/yt-dlp/wiki/EJS).
Обновления зависимостей во время работы не выполняются.
Инструкция полноценного первого запуска появится после реализации
миграций и конвейера; развёртывание согласовывается отдельно.
