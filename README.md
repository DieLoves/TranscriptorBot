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

Спецификация и технический план подготовлены. Реализация ещё не начата.

Согласованный стек: Python 3.12, aiogram 3, Groq SDK, FFmpeg/FFprobe,
yt-dlp, HTTPX, SQLite и Docker Compose.
