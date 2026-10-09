# Первый рабочий запуск Docker — 2026-10-09

Разрешение: пользователь явно попросил «Запусти его в докере».
Запуск выполнен на этом хосте, проверка — 03:00 UTC / 06:00 МСК.

- До запуска контейнеров Compose и постоянной базы не было. Созданы новые
  тома `transcriptbot_data`, `transcriptbot_work`, `transcriptbot_telegram`.
  Backup существующей базы не требовался: база отсутствовала.
- Использованы уже проверенные образы `transcriptbot-app:audit` и
  `transcriptbot-bot-api:audit`, повторной сборки не было.
- `worker migrate`: все пять миграций применены; `worker --check` и `bot --check`
  прошли. Затем выполнен `docker compose up -d --no-build --wait --wait-timeout 60`.
- `bot`, `worker`, `telegram-bot-api`: running / healthy, RestartCount=0.
  CLI healthcheck обоих процессов: healthy, актуальный heartbeat, queue_size=0,
  cleanup_pending=0, cleanup_errors=0, download_space_available=true.
- Реальный getMe через локальный Bot API прошёл: `@transcriptusbot`.
  Проверочные сообщения и задания не отправлялись; платных Groq-вызовов не было.
  Безопасная проверка журналов: оба event=ready, ошибок startup/polling нет.
- Сервисы остаются в фоне с restart=unless-stopped. Порты на хост не опубликованы.
  `.env` и секреты не изменены, значения не выводились. Git push не выполнялся.

Управление: `docker compose ps`, `docker compose stop`, `docker compose start`.
Живая обработка Telegram >20 МБ, очистка настоящего кеша TDLib и повторный
getFile всё ещё требуют отдельной приёмки. Статус healthy и getMe её не заменяют.

Позднее после запроса стиля и подтверждения «Обновляй» bot/worker обновлены
с сохранением томов и Bot API. Все сервисы healthy; актуальный образ, backup
и проверки — в `message-style-2026-10-09.md`.
