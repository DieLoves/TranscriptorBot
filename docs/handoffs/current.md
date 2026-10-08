# Handoff

Дата: 2026-10-09. Проект: `/home/dev/workspace/projects/transcriptbot`.

## Objective

Реализовать Telegram-бота транскрипции через Groq по спецификации v1.
После каждого этапа выполнить соответствующие и полные доступные проверки,
создать локальные коммиты и остановиться до указаний пользователя.
Этап 6 разрешён текущим запросом. Новый push и рабочее развёртывание
требуют отдельного указания; разрешение предыдущего push относилось к этапу 5.

## Current state

- **Этапы 1–5 завершены**, этап 5 закоммичен и отправлен: `2a6801b`.
  **Реализация этапа 6 завершена**, все автономные проверки прошли.
  Полную эксплуатационную готовность v1 пока не объявлять: живая проверка
  Telegram >20 МБ и повторного getFile после очистки требует тестового файла.
  Пользователю задан вопрос о доступном file ID; на момент handoff ответа нет.
- Worker выполняет `download → prepare → transcribe → assemble → deliver`.
  Бот отдельно подтверждает preview/TXT/JSON; завершение ставит `cleanup_pending`.
- Очистка выполняется отдельной корутиной единственного worker: после успеха,
  остановки отменённого задания и истечения 7 дней окончательной ошибки.
  TTL проверяется при старте и каждые 600 с даже при долгом скачивании.
  Истечение атомарно отключает продолжение и ставит очистку в очередь.
  Ошибка удаления сохраняет флаг для повторной попытки через интервал или рестарт.
- Busy-защита исключает удаление файлов текущей операции до остановки I/O.
  Кеш Bot API удерживается ссылками других заданий, включая очередь с тем же
  file ID. Пока новый Telegram getFile не завершён и путь неизвестен, удаление
  кеша откладывается. Распознавание другого источника очистку не задерживает.
- Удаление использует dir_fd/O_NOFOLLOW для каждого каталога. Только учтённые
  медиа в разрешённых каталогах TDLib; база/служебные файлы Bot API сохраняются.
  Неизвестные каталоги, символьные ссылки, вложенные каталоги задания и выход
  за разрешённые корни не удаляются. Отказ виден как ошибка очистки.
- Startup reconciliation убирает .part/current.wav известных сохраняемых
  заданий, сверяет источники и ответы, завершает прерванное удаление.
  Опубликованный ответ до фиксации SQLite принимает прежний конвейер без API.
  Потерянные файлы позднего этапа дают recovery_files и освобождают FIFO.
  Deliver восстанавливается по TXT/JSON без источника и нового распознавания.
- После очистки остаются история/исходная ссылка или file ID/метаданные и
  подтверждения Telegram. Ссылки на артефакты, chunks и освобождённые assets
  удаляются; содержимого транскрипции в SQLite по-прежнему нет.
- Миграция 004 добавляет runtime_health и maintenance_state; всего 4 миграции.
  Heartbeat bot/worker каждые 5 с отдельной корутиной. CLI healthcheck проверяет
  возраст <=60 с и доступность SQLite; JSON показывает очередь, возраст,
  ожидание, свободный диск и ошибки очистки без URL/текста/ключей.
  Compose healthchecks добавлены для всех трёх сервисов.
- CLI backup/restore использует SQLite backup API, integrity/foreign-key/schema
  checks, права 0600 и атомарную публикацию без перезаписи. Restore принимает
  только отсутствующую целевую базу без WAL/SHM; heartbeat копии сбрасывается.
  Для восстановления незавершённых заданий нужно сохранить также рабочие файлы
  и кеш при остановленных трёх сервисах; одного SQLite backup недостаточно.
- Отмена/таймаут сначала останавливает дочернюю группу, затем дочитывает
  ограниченные оставшиеся данные каналов и закрывает их до очистки файлов.
- Сохранены правила медиа этапа 5: первая аудиодорожка, потоковые отсчёты
  16 кГц, 600 с/паузы в последних 8 с от 0,3 с, без полного WAV и перекрытий.
  Только current.wav PCM16/16 кГц/моно; извлечение декодирует источник с начала
  для точных отсчётов. Таймаут части 300 с. FFmpeg допускает только file и
  закрытый набор демультиплексоров, внешние дорожки MOV запрещены defaults.
- Immutable source/chunks/response checkpoints с SHA-256 и fsync; single JSON —
  исходный Groq, multi JSON — глобальные времена плюс локальные ответы chunks.
  Groq quota journal, сохранённые 429/Retry-After и обычные повторы, FIFO,
  revision/generation, durable inbox/outbox/offset и раздельная доставка сохранены.
- Сохранены защищённые загрузчики HTTPX/HTTPcore/yt-dlp: весь DNS, публичный
  числовой IP, Host/SNI/TLS, каждый redirect, лимиты, без proxies/обходов.
  Mail.ru: HTTPS cloud.mail.ru/datacloudmail.ru и поддомены; домен подтверждён
  пользователем. Источники сверяются по SHA-256, Telegram без обязательной копии.
- Рабочие сервисы не запущены. .env не изменён, права 0600, исключён из Git.
  Ключи не читать/не выводить/не запрашивать повторно. Реальные Telegram/Groq
  в этапе 6 не вызывались. Спецификация, зависимости и lock не изменены.

## Validation

- Этапы 4–5: живые direct/YouTube/вложенный Mail.ru и один живой Groq-запрос
  на синтетической речи прошли раньше; повторять без причины не требуется.
  Mail.ru: 944 416 750 байт / 1818,958367 с / AAC.
- Новые этапные tests/test_operations.py: **28 passed**. Отдельный набор медиа,
  отмены дочерних процессов и очистки: 57 passed; ещё ранее media/pipeline/
  operations: 69 passed. Проверки предупреждений — с PytestUnraisableExceptionWarning
  как ошибкой; закрытие subprocess pipe исправлено после промежуточного warning.
- Финальный полный pytest: **368 passed**, без предупреждений, с
  PytestUnraisableExceptionWarning как ошибкой.
- Ruff check/format, uv lock --check --offline, compileall, sh -n, Git diff check
  и сборка wheel/sdist прошли. Финальные статические проверки также прошли.
- Образ приложения transcriptbot-app:stage6 собран. Bot API остаётся stage1.
  Первый полный автономный контейнерный набор прошёл, включая healthchecks,
  миграции/повтор, worker lock/SIGTERM, доставку/очистку, два HTTP getFile >20 МБ,
  одинаковый SHA-256, сохранение service DB, согласованный snapshot/restore,
  startup recovery опубликованного ответа без повторного Groq.
  Финальный набор с CLI backup/restore и актуальным образом также прошёл.
- Подставной HTTP Telegram-файл: 21 120 044 байта / 660 с; это не живой gate TDLib.
- Sandbox блокирует asyncio self-pipe/SQLite из потоков; тесты запускать с
  разрешённой эскалацией и UV_CACHE_DIR=/tmp/transcriptbot-uv-cache.
  Полный pytest запускать отдельно от контейнерных/дополнительных медиа gates:
  параллельная нагрузка ранее давала CLI migrate timeout.
- Подробности: stage-4-validation.md, stage-5-validation.md, stage-6-validation.md.

## Relevant files

- docs/specs/transcriptbot-v1.md; docs/plans/transcriptbot-v1-implementation.md.
- docs/operations/transcriptbot-v1.md — первый запуск, health, очистка, диск,
  SQLite backup и согласованное восстановление трёх томов.
- operations/cleanup.py, files.py, health.py, backup.py; migration 004_operations.sql.
- Repository: expire_failed, cleanup_jobs, job_assets, cache_used_elsewhere,
  finish_cleanup, reconciliation_failed. Worker: work + maintain и busy-защита.
- scripts/check_operations_offline.py — настоящий HTTP aiogram/getFile >20 МБ,
  очистка/повтор/backup с содержимым/recovery/heartbeat на временных данных.
  scripts/check_media_offline.py — pipeline/checkpoint/restart/delivery/cleanup.
  scripts/check_containers.py — полный автономный установленный/container gate.
- tests/test_operations.py — TTL, совместный кеш/очередь, unlink failure,
  crash между unlink/SQLite, безопасность путей, active I/O, restore/диск/health.
- Python 3.12, aiogram 3, Groq whisper-large-v3-turbo; SQLite/WAL/FULL,
  отдельные bot/worker, один worker через flock. Redis/HTTP API приложения нет.

## Repository and next step

- Ветка main; origin git@github.com:DieLoves/TranscriptorBot.git.
  Этап 5/предыдущие этапы отправлены; этап 6 фиксируется локально.
  Актуальные хеши/синхронизацию смотреть git log -1 и git status --branch.
- Этап 6 проверен и фиксируется локальным коммитом; после коммита остановиться.
- Следующий запрос: живой Telegram >20 МБ, реальная очистка кеша TDLib и повторный
  getFile, затем эксплуатационная приёмка v1. Нужен доступный боту тестовый файл
  и изолированный тестовый кеш. Рабочее развёртывание — отдельное согласование.
