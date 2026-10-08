# Проверки этапа 6

Дата: 2026-10-09. Спецификация и зависимости не изменены.
Реализация очистки и эксплуатации закончена; полный статус v1 зависит от
отдельной живой приёмки Telegram, описанной ниже.

## Этапные проверки

`tests/test_operations.py`: **28 passed**. Проверены:

- удаление после подтверждённой доставки с сохранением истории и message ID;
- сохранение файлов ошибочного задания до TTL и атомарное отключение продолжения;
- отмена активной операции: файлы существуют до окончания её finally;
- TTL во время долгого скачивания в настоящем цикле worker;
- общий Telegram-кеш, ожидающий тот же file ID и сохранение служебной базы;
- защита кеша при getFile с ещё неизвестным путём; чужое распознавание не мешает очистке;
- ошибка unlink, повтор после рестарта и crash после удаления до подтверждения SQLite;
- пути за корнем, символьные ссылки файлов/каталогов, вложенный каталог и service DB;
- startup cleanup временных файлов при сохранении опубликованных ответов;
- потерянные результаты этапа deliver без нового распознавания;
- heartbeat, устаревший heartbeat, очередь, ожидание и свободный диск;
- WAL backup/restore, permissions, integrity/foreign-key/schema checks,
  отказ от перезаписи/неверной базы/символьной ссылки/существующего sidecar;
- нехватка места без удаления сохранённых файлов;
- реальный HTTP/multipart aiogram getFile на подставном сервере, очистка и повтор.

Отдельный набор медиа/дочерних процессов/очистки: **57 passed**;
предшествующий media/pipeline/operations: **69 passed**.
PytestUnraisableExceptionWarning в этих прогонах считается ошибкой.
Промежуточный полный pytest: 362 passed с предупреждением о subprocess pipe;
оно устранено закрытием каналов после остановки дочерней группы.
Финальный полный pytest: **368 passed за 211,65 с**, без предупреждений,
с PytestUnraisableExceptionWarning как ошибкой.

## Статические проверки и сборка

Прошли Ruff check/format, uv lock --check --offline, compileall,
sh -n deploy/bot-api-entrypoint.sh и git diff --check.
Wheel и sdist собраны без изменения uv.lock/pyproject.toml.

Образ приложения: `transcriptbot-app:stage6`.
Финальная сборка: `sha256:caf5efe6b819d15f3d8e06f93cb5063224b9290ea5cb83a84c39064cb12329fb`.
Образ официального Bot API остался `transcriptbot-bot-api:stage1`.
Compose содержит прежние три сервиса; добавлены healthchecks, публичных портов нет.

## Автономная контейнерная приёмка

`python3 -u scripts/check_containers.py` прошёл на финальном образе.
Только вымышленные ключи, `--network none`, временные тома и подставные API.
Рабочие сервисы не запускались; созданные проверкой контейнеры и тома удалены.

Проверены четыре миграции и повтор, локальный listener Bot API и SIGTERM,
bot/worker --check, SIGTERM, heartbeat обоих процессов, отказ второго worker,
SQLite/WAL и права общих томов, durable Telegram inbox/offset/частичная доставка.
CLI backup и restore выполнили проверенную копию; повторная запись в тот же
snapshot отклонена с кодом 2.

Новый operations gate использует настоящий HTTP aiogram getFile на подставном
сервере: WAV **21 120 044 байта / 660 с** скачивается дважды после очистки с
одинаковым SHA-256. Учтённый кеш и каталог задания удаляются, синтетический
`td.binlog` сохраняется. SQLite snapshot вместе с копией рабочих файлов и кеша
восстанавливает активный prepare; startup reconciliation проходит, heartbeat
работает, последующая отмена удаляет восстановленное содержимое.

Медиа-gate проверяет настоящий FFmpeg и SDK multipart, сбой между публикацией
ответа и SQLite, startup recovery без повторного запроса, глобальные времена,
outbox и подтверждённую выдачу. После выдачи источник/WAV/ответы/TXT/JSON удалены,
история сохранена, число Groq-вызовов остаётся прежним (подставной SDK).

## Воспроизведение

```bash
UV_CACHE_DIR=/tmp/transcriptbot-uv-cache uv run --offline pytest tests/test_operations.py -q \
  -W error::pytest.PytestUnraisableExceptionWarning
UV_CACHE_DIR=/tmp/transcriptbot-uv-cache uv run --offline pytest -q \
  -W error::pytest.PytestUnraisableExceptionWarning
UV_CACHE_DIR=/tmp/transcriptbot-uv-cache uv run --offline ruff check .
UV_CACHE_DIR=/tmp/transcriptbot-uv-cache uv run --offline ruff format --check .
UV_CACHE_DIR=/tmp/transcriptbot-uv-cache uv lock --check --offline
UV_CACHE_DIR=/tmp/transcriptbot-uv-cache uv build --offline
docker build -t transcriptbot-app:stage6 .
python3 -u scripts/check_containers.py
```

pytest и контейнерные/дополнительные медиа gates запускать последовательно.
Sandbox на этом хосте блокирует asyncio self-pipe в выделенных потоках;
SQLite/CLI/subprocess и Docker-проверки выполнены с разрешённой эскалацией.

## Открытая живая приёмка

Подставной getFile **не подтверждает реальное повторное скачивание TDLib**.
Для завершения эксплуатационной приёмки v1 нужен Telegram-медиафайл больше
20 МБ, доступный боту: живой getFile, обработка/выдача, очистка учтённого кеша и
повторный getFile с проверкой размера/SHA-256 в изолированном тестовом кеше.
Пользователю задан вопрос о доступном file ID; ответа пока нет.

Живые direct/YouTube/вложенный Mail.ru и один Groq-запрос прошли в этапах 4–5;
в этапе 6 они не повторялись. Реальные Telegram/Groq API здесь не вызывались.
Рабочее развёртывание и новый push не выполнялись.

Первый запуск, диагностика и согласованное резервное копирование трёх томов:
[инструкция эксплуатации](../operations/transcriptbot-v1.md).
