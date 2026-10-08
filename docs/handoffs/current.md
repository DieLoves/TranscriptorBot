# Handoff

Дата: 2026-10-08. Проект: `/home/dev/workspace/projects/transcriptbot`.

## Objective

Реализовать Telegram-бота транскрипции через Groq по спецификации v1.
После каждого этапа выполнить соответствующие и полные доступные проверки,
создать локальный коммит и остановиться до указаний пользователя.
Push и рабочее развёртывание требуют отдельного согласования.

## Current state

- **Этапы 1–4 завершены.** Пользователь подтвердил домен datacloudmail.ru;
  спецификация и allowlist обновлены для него и поддоменов. Предоставленный
  вложенный файл Mail.ru полностью скачан, SHA-256/FFprobe проверены.
  HTTPS/DNS/IP/TLS/redirect/size/timeout checks сохранены.
- Worker выполняет этап `download` в порядке FIFO: получение источника,
  размер/SHA-256/FFprobe, `source.json`, атомарный переход в `prepare`.
  Подготовка аудио, Groq и сборка — этап 5, очистка — этап 6; они не начаты.
  Активный `prepare` удерживает очередь до подключения следующего этапа.
- Четыре адаптера: direct/Mail.ru/YouTube в управляемом дочернем процессе;
  Telegram — getFile собственного Bot API при наступлении очереди и локальный
  кеш без обязательной второй копии. При готовом манифесте повторный getFile
  не выполняется. Все источники проверяются по SHA-256 при восстановлении.
- HTTPX/HTTPcore: все DNS-ответы должны быть публичными, смешанные ответы
  отклоняются; числовое соединение без повторного DNS, исходные Host/SNI,
  проверка TLS, отсутствие прокси из окружения. Проверяются каждый redirect,
  размеры ответов/скачивания, таймаут DNS/соединения/чтения и всего процесса.
  Mail.ru допускает только HTTPS, cloud.mail.ru, datacloudmail.ru и их поддомены.
- yt-dlp: единственный защищённый RequestHandler, встроенный HttpFD,
  отдельная HTTP(S)-аудиодорожка, локальный EJS/Deno. Нет диспетчера внешних
  загрузчиков, HLS/DASH/FFmpeg; remote_components отключены. Лимит байтов
  контролируется суммарно для range-запросов. Плейлисты/эфиры/авторизация запрещены.
- Прямые ссылки скачиваются потоком в `.part`, источник публикуется атомарно.
  Mail.ru сохраняет весь вложенный путь и корректное percent-encoding/Referer.
  Файлы находятся только в каталоге задания. FFprobe допускает только file
  и закрытый набор демультиплексоров, использует первую аудиодорожку.
- Манифест публикуется без перезаписи, с fsync. В SQLite сохраняются только
  метаданные/ссылки; assets/job_assets учитывают исходники, манифесты, `.part`
  и общий ресурс кеша Bot API. Новая миграция не потребовалась (по-прежнему две).
- Worker сохраняет повторы временных ошибок (5/15/45/135/300 с), учитывает
  Retry-After (секунды/HTTP-date), освобождает очередь после окончательной ошибки.
  Отмена/остановка/таймаут завершают всю группу дочерних процессов FFprobe/Deno.
  Дочерним процессам не передаются ключи; журналы содержат только ID и коды ошибок.
- Этап 3 сохранён: атомарный Telegram inbox/очередь/outbox/offset, durable polling,
  команды/routers, владелец и группа/тема, статусы не чаще 5 секунд, раздельные
  preview/TXT/JSON с восстановлением и ограничениями чата. Отмена/revision и
  generation защищают от поздних результатов и старых кнопок.
- SQLite/WAL, FULL, foreign keys, busy timeout 10 секунд, выделенный поток;
  миграции отдельно через `transcriptbot migrate`, один worker благодаря flock.
- Рабочие сервисы не запускались. `.env` не изменён, права 0600, исключён из Git.
  Не читать/не выводить значения ключей, не запрашивать их повторно.
  Запросов к рабочим Telegram/Groq не выполнялось. Новый push не выполнялся.

## Completed and validation

- Полный pytest: **268 passed** (155 прежних и 113 новых проверок этапа 4).
- Ruff check/format, uv lock --check --offline, compileall, sh -n и Git diff check
  прошли. Зависимости/lock-файл не изменены; доменное ограничение спецификации
  обновлено по подтверждению пользователя.
- Образ `transcriptbot-app:stage4` собран; Bot API остаётся `stage1`.
  Compose и scripts/check_containers.py используют stage4.
- Автономный контейнерный набор прошёл: Compose, официальный Bot API/--local/8081,
  SIGTERM, миграции и их повтор на томе, установленный Telegram-код, bot/worker,
  отказ второго worker, целостность SQLite и новые проверки загрузчиков.
- Проверена кешированная WAV-запись **21 120 044 байта / 660 секунд**:
  getFile на подставном API, FFprobe, SHA-256, immutable checkpoint при восстановлении,
  отсутствие второй копии и переход в prepare. Защищённый HTTPX/HTTPcore мост
  проверен на подставном сокете. Это не заменяет живой Telegram gate v1.
- Живая прямая ссылка прошла: MP3, 52 079 байт / 3,239184 с.
- Живой YouTube прошёл на текущем сервере из worker-образа с Deno:
  jNQXAC9IVRw, Opus, 252 182 байта / 19,021 с. Защита не обходилась.
- Живой Mail.ru gate прошёл после подтверждения домена: 944 416 750 байт,
  1818,958367 с, AAC, первая аудиодорожка 1. Хеш в отчёте проверок.
- Живые файлы удалены проверочным скриптом; контейнеры/тома проверок временные.
- Детали/хеши/команды: [stage-4-validation.md](stage-4-validation.md).
- Sandbox блокирует межпоточный asyncio self-pipe; SQLite/CLI/HTTP/process-тесты
  запускаются с разрешённой эскалацией. Обход в прикладной код не добавлен.
  Для uv в sandbox: UV_CACHE_DIR=/tmp/transcriptbot-uv-cache.

## Key decisions and relevant files

- Спецификация: docs/specs/transcriptbot-v1.md; план:
  docs/plans/transcriptbot-v1-implementation.md. Этап 4 отмечен завершённым;
  обязательная живая проверка вложенного Mail.ru прошла.
- downloaders/network.py — HTTPX/HTTPcore и DNS/IP/redirect/TLS policy.
  Адаптер использует закреплённые private HTTPX pool/HTTPcore SyncStream;
  их мост отдельно проверен. Не заменять обычным клиентом с проверкой DNS до запроса.
- downloaders/http_sources.py — direct/Mail.ru; youtube.py — защищённый
  RequestHandler и прямой вызов встроенного HttpFD.
- downloaders/files.py — каталог задания, ограничения FFprobe, хеш и манифест;
  child.py — приватный процесс; service.py — получение кеша Telegram и управление
  процессом; worker/__init__.py — выполнение этапа download.
- Repository.track_asset — учёт до I/O, в том числе временных путей;
  finish_download — атомарные метаданные, asset hash/size и prepare с revision.
- scripts/check_sources_live.py — явный живой gate без ключей/Telegram/Groq;
  scripts/check_downloaders_offline.py — установленный пакет в контейнере без сети.
  Тесты: tests/test_download_network.py, test_download_sources.py,
  test_download_youtube.py, test_download_worker.py.
- Будущие результаты: WORK_DIR/<job_id>/, active deliver и вызов
  TelegramStore.enqueue_results(expected, txt=..., verbose_json=...). Sender
  подтверждает preview/TXT/JSON и отмечает cleanup_pending; физическая очистка
  вместе с учтённым кешем Bot API — этап 6.
- Python 3.12, aiogram 3, один worker; Groq whisper-large-v3-turbo.
  Redis/HTTP API приложения нет. Host имеет FFmpeg/FFprobe, Docker/Compose/uv;
  Deno отсутствует в host PATH, установлен в worker-образе.
- Соседний groq_transcript/transcribe.py и тесты — источник алгоритмов этапа 5;
  соседний проект не менять.

## Repository and next step

- Ветка main; origin git@github.com:DieLoves/TranscriptorBot.git.
  Этапы 1–3 уже отправлены по прежнему запросу пользователя, origin/main aada9c8.
  Основной коммит этапа 4 — b0c2a2b; коммит завершения приёмки смотреть
  через git log -1 --oneline; новый push запрещён
  без отдельного указания.
- После локального коммита остановиться. Следующий этап — 5 (медиа/Groq),
  только по явному указанию пользователя. Самостоятельно не переходить.
- Для v1 ещё обязательны живые Telegram >20 МБ/Groq и очистка/повторная загрузка
  Bot API, медиа/Groq-конвейер, startup reconciliation, healthcheck, backup/restore.
- Обязательная Mail.ru-ссылка:
  [вложенный файл](https://cloud.mail.ru/public/oedu/9Qx3xyVGv/3%20%D0%BC%D0%BE%D0%B4%D1%83%D0%BB%D1%8C/%D0%B2%D0%B2%D0%B5%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%B2%20%D0%BF%D1%80%D0%BE%D0%B4%D0%B0%D0%B6%D0%B8%20%5Bskladchik.org%5D.mp4).
