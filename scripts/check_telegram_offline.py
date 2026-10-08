"""Exercise installed Telegram code against a fake API on disposable container volumes."""

import asyncio

from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.types import Message, Update

from transcriptbot.config import Settings
from transcriptbot.storage.database import Database
from transcriptbot.storage.models import JobStatus, Phase
from transcriptbot.storage.repository import Repository
from transcriptbot.telegram.polling import process_update
from transcriptbot.telegram.requests import make_dispatcher
from transcriptbot.telegram.sender import Sender
from transcriptbot.telegram.store import TelegramStore


class FakeSession(BaseSession):
    def __init__(self) -> None:
        super().__init__()
        self.operations: list[str] = []

    async def close(self) -> None:
        pass

    async def make_request(self, bot, method, timeout=None):
        self.operations.append(method.__api_method__)
        return Message.model_validate(
            {
                "message_id": 100 + len(self.operations),
                "date": 1_000_000,
                "chat": {"id": method.chat_id, "type": "private"},
            }
        )

    async def stream_content(self, *args, **kwargs):
        yield b""


async def main() -> None:
    settings = Settings(_env_file=None)
    session = FakeSession()
    bot = Bot(settings.telegram_bot_token.get_secret_value(), session=session)
    database = await Database(settings.sqlite_path).open()
    now = [1_000_000.0]
    repository = Repository(database, clock=lambda: now[0])
    store = TelegramStore(repository)
    try:
        update = Update.model_validate(
            {
                "update_id": 50,
                "message": {
                    "message_id": 10,
                    "date": 1_000_000,
                    "chat": {"id": 12345, "type": "private"},
                    "from": {"id": 12345, "is_bot": False, "first_name": "Test"},
                    "text": "https://example.com/audio.mp3",
                },
            }
        )
        dispatcher = make_dispatcher(settings, "TestBot")
        await process_update(bot, dispatcher, store, update)
        await process_update(bot, dispatcher, store, update)
        assert await store.offset() == 51
        assert len(await repository.list_jobs(owner_id=12345)) == 1
        assert session.operations == []
        await Sender(bot, store, settings).send_one()
        current = await repository.claim_next()
        for phase in (Phase.PREPARE, Phase.TRANSCRIBE, Phase.ASSEMBLE, Phase.DELIVER):
            current = await repository.advance(current, phase)
        directory = settings.work_dir / str(current.id)
        directory.mkdir(parents=True, exist_ok=True)
        text, verbose = directory / "result.txt", directory / "result.verbose.json"
        text.write_text("Synthetic transcription", encoding="utf-8")
        verbose.write_text('{"text":"Synthetic transcription"}', encoding="utf-8")
        await store.enqueue_results(current, txt=text, verbose_json=verbose)
        sender = Sender(bot, store, settings)
        for _ in range(3):
            now[0] += 5
            await store.refresh_statuses()
            assert await sender.send_one()
        assert session.operations.count("sendDocument") == 1
        await database.close()
        database = await Database(settings.sqlite_path).open()
        repository = Repository(database, clock=lambda: now[0])
        store = TelegramStore(repository)
        assert await store.offset() == 51
        now[0] += 5
        assert await Sender(bot, store, settings).send_one()
        await store.refresh_statuses()
        result = await repository.get_job(current.id, owner_id=12345)
        assert result.status == JobStatus.COMPLETED and result.cleanup_pending
        assert session.operations.count("sendDocument") == 2
        assert (
            await database.call(lambda c: c.execute("PRAGMA integrity_check").fetchone()[0]) == "ok"
        )
        print(
            "Telegram: atomic acceptance, replay, persisted offset/status "
            "and partial delivery restart"
        )
    finally:
        await database.close()
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
