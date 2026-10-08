import asyncio
import json
from dataclasses import replace

import pytest
from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.exceptions import (
    ClientDecodeError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.methods import GetUpdates, SendDocument
from aiogram.types import CallbackQuery, Message, Update

from transcriptbot.storage.database import Database, StorageError, migration_scripts
from transcriptbot.storage.models import JobStatus, Phase, SourceKind, SourceRef
from transcriptbot.storage.repository import Repository
from transcriptbot.telegram.polling import poll, process_update
from transcriptbot.telegram.presentation import preview_text
from transcriptbot.telegram.requests import (
    Request,
    make_dispatcher,
    message_request,
    url_source,
)
from transcriptbot.telegram.sender import Sender
from transcriptbot.telegram.store import TelegramStore


class Clock:
    now = 1_000_000.0

    def __call__(self):
        return self.now


class Session(BaseSession):
    def __init__(self):
        super().__init__()
        self.calls = []
        self.effects = []
        self.closed = False

    async def close(self):
        self.closed = True

    async def make_request(self, bot, method, timeout=None):
        self.calls.append(method)
        if self.effects:
            effect = self.effects.pop(0)
            if isinstance(effect, BaseException):
                raise effect
            if callable(effect):
                return await effect(method)
            return effect
        if method.__api_method__ == "answerCallbackQuery":
            return True
        return message(message_id=100 + len(self.calls), chat_id=method.chat_id)

    async def stream_content(self, *args, **kwargs):
        yield b""


@pytest.fixture
async def env(settings):
    database = await Database(settings.sqlite_path).open(migrate=True)
    clock = Clock()
    repository = Repository(database, clock=clock)
    store = TelegramStore(repository)
    session = Session()
    bot = Bot("12345:fake_test", session=session)
    try:
        yield repository, store, clock, bot, session, Sender(bot, store, settings)
    finally:
        await repository.database.close()
        await bot.session.close()


async def restart(repository, store):
    path = repository.database.path
    await repository.database.close()
    repository.database = await Database(path).open()
    store.database = repository.database


def message(text=None, *, chat_id=123, owner_id=123, message_id=10, **values):
    return Message.model_validate(
        {
            "message_id": message_id,
            "date": 1_000_000,
            "chat": {"id": chat_id, "type": "private" if chat_id > 0 else "supergroup"},
            "from": {"id": owner_id, "is_bot": False, "first_name": "Test"},
            "text": text,
            **values,
        }
    )


def incoming(**kwargs):
    return Request(
        "create",
        chat_id=-100,
        topic_id=12,
        message_id=10,
        owner_id=123,
        group_chat_id=-100,
        source=SourceRef(SourceKind.DIRECT, "https://example.com/a"),
        **kwargs,
    )


async def rows(store):
    return await store.database.call(
        lambda c: [dict(row) for row in c.execute("SELECT * FROM outbox")]
    )


async def job(repository, job_id=1):
    return await repository.get_job(job_id, owner_id=123)


async def delivery(env, settings, text="Секретный текст"):
    repository, store, _clock, _bot, _session, _sender = env
    await store.accept(1, incoming())
    current = await repository.claim_next()
    for phase in (Phase.PREPARE, Phase.TRANSCRIBE, Phase.ASSEMBLE, Phase.DELIVER):
        current = await repository.advance(current, phase)
    directory = settings.work_dir / str(current.id)
    directory.mkdir(parents=True)
    txt, verbose = directory / "result.txt", directory / "result.verbose.json"
    txt.write_text(text)
    verbose.write_text('{"text": "Секретный текст"}')
    assert await store.enqueue_results(current, txt=txt, verbose_json=verbose)
    await store.refresh_statuses()
    return txt, verbose


async def drain(env, count=8):
    _repository, store, clock, _bot, _session, sender = env
    for _ in range(count):
        await store.refresh_statuses()
        await sender.send_one()
        clock.now += 5


@pytest.mark.parametrize("kind", ["audio", "video", "document", "voice", "video_note"])
def test_private_attachments(settings, kind):
    media = {"file_id": "telegram-file", "file_unique_id": "unique", "file_size": 24_000_000}
    if kind in {"audio", "video", "voice", "video_note"}:
        media["duration"] = 60
    if kind in {"video", "video_note"}:
        media.update(width=10, height=10, length=10)
    request = message_request(message(**{kind: media}), settings, "BotName")
    assert request.action == "create"
    assert request.source.reference == "telegram-file"
    assert request.size_bytes == 24_000_000


@pytest.mark.parametrize(
    "url, kind",
    [
        ("https://youtu.be/abc", SourceKind.YOUTUBE),
        ("https://www.youtube.com/watch?v=abc", SourceKind.YOUTUBE),
        ("https://youtube.com/shorts/abc", SourceKind.YOUTUBE),
        ("https://cloud.mail.ru/public/token/key/folder/file%20name.mp4", SourceKind.MAILRU),
        ("https://example.com/a.mp3?token=sensitive", SourceKind.DIRECT),
    ],
)
def test_supported_urls(settings, url, kind):
    request = message_request(message(url), settings, "BotName")
    assert request.source.kind == kind
    assert request.source.reference == url


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/a https://example.com/b",
        "file:///tmp/a",
        "ftp://example.com/a",
        "https://user:password@example.com/a",
        "https://example.com:wrong/a",
        "https://youtube.com/playlist?list=abc",
        "https://youtube.com/watch?v=abc&list=abc",
        "http://cloud.mail.ru/public/token/key",
    ],
)
def test_invalid_urls(url):
    assert url_source(url) is None


def test_group_reply_checks_initiator_and_preserves_topic(settings):
    original = message("https://example.com/a", chat_id=-100, owner_id=999, message_id=8)
    command = message(
        "/transcribe@BotName",
        chat_id=-100,
        reply_to_message=original,
        message_thread_id=12,
        is_topic_message=True,
    )
    request = message_request(command, settings, "BotName")
    assert request.action == "create"
    assert (request.owner_id, request.message_id, request.topic_id) == (123, 10, 12)
    denied = command.model_copy(update={"from_user": original.from_user})
    assert message_request(denied, settings, "BotName").action == "forbidden"


@pytest.mark.parametrize(
    "text", ["https://example.com/a", "/transcribe@OtherBot https://example.com/a"]
)
def test_group_does_not_take_ordinary_or_other_bot_messages(settings, text):
    assert message_request(message(text, chat_id=-100), settings, "BotName").action == "ignored"


@pytest.mark.parametrize(
    "command", ["/jobs", "/status 1", "/help", "/start", "/transcribe https://example.com/a"]
)
def test_access_checked_for_all_commands(settings, command):
    assert (
        message_request(message(command, owner_id=999), settings, "BotName").action == "forbidden"
    )


def test_anonymous_group_command(settings):
    source = message(
        "/transcribe https://example.com/a",
        chat_id=-100,
        sender_chat={"id": -100, "type": "supergroup", "title": "group"},
    )
    assert message_request(source, settings, "BotName").action == "anonymous"


@pytest.mark.parametrize(
    "field, value, outcome",
    [
        ("file_size", 2_000_000_001, "size_limit"),
        ("duration", 14_401, "duration_limit"),
    ],
)
def test_known_limits_checked_before_creation(settings, field, value, outcome):
    media = {"file_id": "file", "file_unique_id": "unique", "duration": 10, field: value}
    assert message_request(message(audio=media), settings, "BotName").action == outcome


async def test_atomic_accept_restart_and_duplicate(env, settings):
    repository, store, _clock, bot, session, sender = env
    dispatcher = make_dispatcher(settings, "BotName")
    update = Update(update_id=50, message=message("https://example.com/a", chat_id=-100))
    # Ordinary group message is ignored but committed and acknowledged.
    await process_update(bot, dispatcher, store, update)
    assert await store.offset() == 51
    update = Update(
        update_id=51,
        message=message(
            "/transcribe https://example.com/a",
            chat_id=-100,
            is_topic_message=True,
            message_thread_id=12,
        ),
    )
    await process_update(bot, dispatcher, store, update)
    assert session.calls == []
    await restart(repository, store)
    await process_update(bot, dispatcher, store, update)
    assert await store.offset() == 52
    assert len(await repository.list_jobs(owner_id=123)) == 1
    assert len(await rows(store)) == 1
    await sender.send_one()
    assert session.calls[-1].message_thread_id == 12
    assert (await job(repository)).status_message_id == 101


async def test_outbox_failure_rolls_back_job_update_and_offset(env, monkeypatch):
    repository, store, *_ = env

    def fail(*args):
        raise RuntimeError("injected")

    monkeypatch.setattr(store, "_enqueue_status", fail)
    with pytest.raises(RuntimeError, match="injected"):
        await store.accept(50, incoming())
    assert await store.offset() == 0
    assert await repository.list_jobs(owner_id=123) == []
    assert (
        await store.database.call(
            lambda c: c.execute("SELECT COUNT(*) FROM processed_updates").fetchone()[0]
        )
        == 0
    )


async def test_rejections_limits_and_replay_committed(env):
    repository, store, *_ = env
    for update_id in range(1, 4):
        assert (await store.accept(update_id, incoming())).outcome == "created"
    assert (await store.accept(4, incoming())).outcome == "user_limit"
    await repository.cancel(update_id=5, job_id=1, owner_id=123, generation=0)
    assert (await store.accept(4, incoming())).outcome == "user_limit"
    assert await store.offset() == 5  # accept's offset, independent of direct repository calls.
    assert len([row for row in await rows(store) if row["operation"] == "reply"]) == 1


async def test_history_owner_and_chat_filters(env):
    _repository, store, *_ = env
    await store.accept(1, incoming())
    await store.accept(2, replace(incoming(), chat_id=-200, group_chat_id=-200))
    for update_id, request, expected in [
        (3, Request("jobs", chat_id=123, owner_id=123), ("#1", "#2")),
        (4, Request("jobs", chat_id=-100, owner_id=123, group_chat_id=-100), ("#1",)),
        (5, Request("status", chat_id=-200, owner_id=123, group_chat_id=-200, job_id=1), ()),
        (6, Request("status", chat_id=456, owner_id=456, job_id=1), ()),
    ]:
        await store.accept(update_id, request)
        text = json.loads((await rows(store))[-1]["payload"])["text"]
        assert tuple(item for item in ("#1", "#2") if item in text) == expected


async def test_callback_routes_owner_and_generation(env, settings):
    repository, store, _clock, bot, *_ = env
    await store.accept(1, incoming())
    dispatcher = make_dispatcher(settings, "BotName")

    def update(update_id, owner, data, chat=-100):
        return Update(
            update_id=update_id,
            callback_query=CallbackQuery(
                id=f"callback-{update_id}",
                from_user=message(owner_id=owner).from_user,
                chat_instance="instance",
                message=message(chat_id=chat),
                data=data,
            ),
        )

    for update_id, owner, data, chat in [
        (2, 456, "cancel:1:0", -100),
        (3, 123, "cancel:1:0", -200),
        (4, 999, "cancel:1:0", -100),
        (5, 123, "cancel:1:1", -100),
    ]:
        await process_update(bot, dispatcher, store, update(update_id, owner, data, chat))
        assert (await job(repository)).status == JobStatus.QUEUED
    await process_update(bot, dispatcher, store, update(6, 123, "cancel:1:0"))
    assert (await job(repository)).status == JobStatus.CANCELLED
    assert (await rows(store))[0][
        "status"
    ] == "pending"  # Cancellation updates the original status.
    await process_update(bot, dispatcher, store, update(6, 123, "cancel:1:0"))
    assert len([row for row in await rows(store) if row["callback_id"] == "callback-6"]) == 1


async def test_poll_offset_only_after_each_committed_update(env, settings, monkeypatch):
    repository, store, _clock, bot, session, _ = env
    batch = [
        Update(update_id=20, message=message("https://example.com/a")),
        Update(update_id=21, message=message("https://example.com/b")),
    ]
    session.effects = [batch]
    original = store.accept

    async def accept(update_id, request):
        if update_id == 21:
            raise RuntimeError("crash")
        return await original(update_id, request)

    monkeypatch.setattr(store, "accept", accept)
    with pytest.raises(RuntimeError, match="crash"):
        await poll(bot, make_dispatcher(settings, "BotName"), store)
    assert await store.offset() == 21
    assert len(await repository.list_jobs(owner_id=123)) == 1
    assert isinstance(session.calls[0], GetUpdates)
    assert session.calls[0].offset == 0
    monkeypatch.setattr(store, "accept", original)
    session.effects = [[batch[1]], RuntimeError("stop")]
    with pytest.raises(RuntimeError, match="stop"):
        await poll(bot, make_dispatcher(settings, "BotName"), store)
    assert session.calls[-2].offset == 21
    assert session.calls[-1].offset == 22


async def test_coalesced_progress_throttled_and_deleted_status_replaced(env):
    repository, store, clock, _bot, session, sender = env
    await store.accept(1, incoming())
    await sender.send_one()
    current = await repository.claim_next()
    await store.refresh_statuses()
    current = await repository.advance(current, Phase.PREPARE)
    await store.refresh_statuses()
    assert not await sender.send_one()
    clock.now += 5
    await sender.send_one()
    assert session.calls[-1].__api_method__ == "editMessageText"
    assert "подготовка" in session.calls[-1].text
    current = await repository.advance(current, Phase.TRANSCRIBE)
    await store.refresh_statuses()
    clock.now += 5
    session.effects = [
        TelegramBadRequest(method=GetUpdates(), message="Bad Request: message to edit not found")
    ]
    await sender.send_one()
    assert (await job(repository)).status_message_id is None
    await sender.send_one()
    assert session.calls[-1].__api_method__ == "sendMessage"
    assert (await job(repository)).status_message_id == 104


async def test_results_partial_confirmation_survives_restart(env, settings):
    repository, store, _clock, _bot, session, sender = env
    await delivery(env, settings, "Очень длинный текст. " * 1000)
    # Status, preview, TXT; JSON remains unconfirmed.
    await drain(env, count=3)
    operations = {row["operation"]: row for row in await rows(store)}
    assert operations["txt"]["status"] == "sent"
    assert operations["json"]["status"] == "pending"
    preview = [call.text for call in session.calls if call.__api_method__ == "sendMessage"][-1]
    assert len(preview) <= 3000 and "TXT" in preview
    await restart(repository, store)
    await sender.send_one()
    assert isinstance(session.calls[-1], SendDocument)
    assert session.calls[-1].document.filename.endswith("verbose.json")
    await store.refresh_statuses()
    assert (await job(repository)).status == JobStatus.COMPLETED
    assert (await job(repository)).cleanup_pending
    assert sum(call.__api_method__ == "sendDocument" for call in session.calls) == 2
    assert not await sender.send_one()  # Chat throttle also survives reopening.


async def test_429_schedules_retry_without_spending_attempts(env, settings):
    repository, store, clock, _bot, session, sender = env
    await store.accept(1, Request("help", chat_id=-100, owner_id=123))
    session.effects = [TelegramRetryAfter(method=GetUpdates(), message="limit", retry_after=60)]
    await sender.send_one()
    row = (await rows(store))[0]
    assert row["attempts"] == 0 and row["next_attempt_at"] == clock.now + 60
    await store.accept(2, Request("help", chat_id=-100, owner_id=123))
    assert not await sender.send_one()  # Entire chat respects Retry-After.
    await restart(repository, store)
    clock.now += 60
    await sender.send_one()
    assert (await rows(store))[0]["status"] == "sent"


async def test_lost_response_retries_only_unconfirmed_file(env, settings):
    repository, store, clock, _bot, session, sender = env
    await delivery(env, settings)
    await drain(env, count=2)
    session.effects = [TelegramNetworkError(method=GetUpdates(), message="sensitive raw error")]
    await sender.send_one()
    assert (await rows(store))[2]["status"] == "pending"
    assert (await rows(store))[2]["attempts"] == 1
    assert not await sender.send_one()
    clock.now += 5
    await drain(env, count=3)
    documents = [call.document.filename for call in session.calls if isinstance(call, SendDocument)]
    assert len(documents) == 3
    assert documents[0] == documents[1] and documents[2].endswith("verbose.json")
    assert (await job(repository)).status == JobStatus.COMPLETED


async def test_delivery_error_resume_keeps_sent_txt(env, settings):
    repository, store, _clock, _bot, session, sender = env
    txt, verbose = await delivery(env, settings)
    await drain(env, count=3)
    session.effects = [TelegramForbiddenError(method=GetUpdates(), message="chat unavailable")]
    await sender.send_one()
    failed = await job(repository)
    assert failed.status == JobStatus.FAILED and failed.phase == Phase.DELIVER
    assert txt.exists() and verbose.exists()
    await store.accept(
        2,
        Request("resume", chat_id=123, owner_id=123, job_id=1, generation=0, callback_id="resume"),
    )
    resumed = await repository.claim_next()
    assert resumed.phase == Phase.DELIVER and resumed.generation == 1
    assert await store.enqueue_results(resumed, txt=txt, verbose_json=verbose)
    await drain(env)
    documents = [call.document.filename for call in session.calls if isinstance(call, SendDocument)]
    assert len([name for name in documents if name.endswith(".txt")]) == 1
    assert (await job(repository)).status == JobStatus.COMPLETED


async def test_cancel_during_external_send_stops_subsequent_result(env, settings):
    repository, store, _clock, _bot, session, sender = env
    await delivery(env, settings)
    await drain(env, count=2)
    started, finish = asyncio.Event(), asyncio.Event()

    async def pending(method):
        started.set()
        await finish.wait()
        return message(message_id=200)

    session.effects = [pending]
    sending = asyncio.create_task(sender.send_one())
    await started.wait()
    await store.accept(
        2,
        Request("cancel", chat_id=123, owner_id=123, job_id=1, generation=0, callback_id="cancel"),
    )
    finish.set()
    await sending
    await drain(env)
    assert (await job(repository)).status == JobStatus.CANCELLED
    assert len([call for call in session.calls if isinstance(call, SendDocument)]) == 1
    assert all(
        row["status"] == "cancelled"
        for row in await rows(store)
        if row["operation"] in {"txt", "json"}
    )


@pytest.mark.parametrize("text", ["", "Короткая речь"])
async def test_empty_and_short_preview_always_send_both_files(env, settings, text):
    _repository, store, _clock, _bot, session, _sender = env
    await delivery(env, settings, text)
    await drain(env)
    assert sum(isinstance(call, SendDocument) for call in session.calls) == 2
    messages = [call.text for call in session.calls if call.__api_method__ == "sendMessage"]
    assert any((text or "Речь не распознана") in item for item in messages)
    assert not any("Секретный текст" in (row["payload"] or "") for row in await rows(store))


async def test_artifact_cannot_escape_job_directory(env, settings, tmp_path):
    repository, _store, _clock, _bot, session, _sender = env
    txt, _verbose = await delivery(env, settings)
    secret = tmp_path / "outside.txt"
    secret.write_text("outside")
    txt.unlink()
    txt.symlink_to(secret)
    await drain(env)
    assert (await job(repository)).status == JobStatus.FAILED
    assert not any(isinstance(call, SendDocument) for call in session.calls)
    assert not any("outside" in (getattr(call, "text", "") or "") for call in session.calls)


async def test_upgrade_preserves_existing_delivery_records(tmp_path, monkeypatch):
    path = tmp_path / "db.sqlite3"
    scripts = migration_scripts()
    with monkeypatch.context() as context:
        context.setattr("transcriptbot.storage.database.migration_scripts", lambda: scripts[:1])
        database = await Database(path).open(migrate=True)
        repository = Repository(database)
        await repository.create_job(
            update_id=1,
            owner_id=123,
            chat_id=-100,
            message_id=10,
            source=SourceRef(SourceKind.DIRECT, "https://example.com/a"),
        )
        await database.call(
            lambda c: c.execute(
                "INSERT INTO outbox (job_id, generation, operation, status, telegram_message_id) "
                "VALUES (1, 0, 'txt', 'sent', 42)"
            )
        )
        await database.close()
    with pytest.raises(StorageError, match="migrate"):
        await Database(path).open()
    database = await Database(path).open(migrate=True)
    try:
        row = await database.call(lambda c: dict(c.execute("SELECT * FROM outbox").fetchone()))
        assert (row["status"], row["telegram_message_id"], row["chat_id"]) == ("sent", 42, -100)
        assert (
            await database.call(lambda c: c.execute("PRAGMA integrity_check").fetchone()[0]) == "ok"
        )
    finally:
        await database.close()


def test_preview_read_bounded(tmp_path):
    path = tmp_path / "text.txt"
    path.write_text("A" * 100_000)
    assert len(preview_text(path, 3000)) == 3000


async def test_legacy_processed_update_advances_offset_without_duplicate(env):
    repository, store, *_ = env
    await repository.create_job(
        update_id=30,
        owner_id=123,
        chat_id=123,
        message_id=10,
        source=SourceRef(SourceKind.DIRECT, "https://example.com/a"),
    )
    assert (await store.accept(30, incoming())).outcome == "created"
    assert await store.offset() == 31
    assert len(await repository.list_jobs(owner_id=123)) == 1


async def test_source_and_command_message_ids_saved(env, settings):
    repository, store, *_ = env
    original = message("https://example.com/a", chat_id=-100, message_id=8)
    request = message_request(
        message("/transcribe", chat_id=-100, message_id=10, reply_to_message=original),
        settings,
        "BotName",
    )
    await store.accept(1, request)
    current = await job(repository)
    assert (current.message_id, current.source_message_id) == (10, 8)


async def test_chunk_progress_and_quota_status(env):
    repository, store, clock, _bot, session, sender = env
    await store.accept(1, incoming())
    current = await repository.claim_next()
    current = await repository.advance(current, Phase.PREPARE)
    assert await repository.plan_chunks(current, [(0, 16_000), (16_000, 32_000)])
    current = await repository.advance(current, Phase.TRANSCRIBE)
    assert await repository.save_chunk_response(
        current, number=0, path="chunk.json", sha256="a" * 64
    )
    await store.refresh_statuses()
    await sender.send_one()
    assert "части 2/2" in session.calls[-1].text
    await repository.defer(current, until=clock.now + 60, error_code="groq_quota", quota=True)
    clock.now += 5
    await store.refresh_statuses()
    await sender.send_one()
    assert "ожидание квоты" in session.calls[-1].text
    clock.now += 5
    await store.refresh_statuses()
    assert not await sender.send_one()  # A fixed deadline avoids meaningless countdown edits.


async def test_status_coalesced_while_send_in_flight_and_interval_kept(env):
    repository, store, clock, _bot, session, sender = env
    await store.accept(1, incoming())
    started, finish = asyncio.Event(), asyncio.Event()

    async def delayed(method):
        started.set()
        await finish.wait()
        return message(message_id=200)

    session.effects = [delayed]
    sending = asyncio.create_task(sender.send_one())
    await started.wait()
    await repository.claim_next()
    await store.refresh_statuses()
    finish.set()
    await sending
    assert (await job(repository)).status_message_id == 200
    assert (await rows(store))[0]["status"] == "pending"
    assert not await sender.send_one()
    clock.now += 5
    await sender.send_one()
    assert session.calls[-1].__api_method__ == "editMessageText"
    assert session.calls[-1].message_id == 200
    assert "скачивание" in session.calls[-1].text


async def test_retry_budget_survives_progress_coalescing(env, settings):
    repository, store, clock, _bot, session, sender = env
    await store.accept(1, incoming())
    session.effects = [TelegramNetworkError(method=GetUpdates(), message="lost")]
    await sender.send_one()
    await repository.claim_next()
    await store.refresh_statuses()
    row = (await rows(store))[0]
    assert row["attempts"] == 1 and row["next_attempt_at"] == clock.now + 5
    for attempt, delay in enumerate((5, 15, 45, 135, 300), start=1):
        clock.now += delay
        session.effects = [TelegramNetworkError(method=GetUpdates(), message="lost")]
        await sender.send_one()
        if attempt < 5:
            assert (await job(repository)).status == JobStatus.ACTIVE
    assert (await job(repository)).status == JobStatus.FAILED
    assert (await job(repository)).error_code == "telegram_delivery_failed"
    # Recovery remains accessible via a fresh /status reply with the resume button.
    await store.accept(2, Request("status", chat_id=123, owner_id=123, job_id=1))
    await sender.send_one()
    assert session.calls[-1].reply_markup.inline_keyboard[0][0].callback_data == "resume:1:0"


async def test_status_not_modified_confirms_and_topic_unavailable_fails(env):
    repository, store, clock, _bot, session, sender = env
    await store.accept(1, incoming())
    await sender.send_one()
    await repository.claim_next()
    await store.refresh_statuses()
    clock.now += 5
    session.effects = [TelegramBadRequest(method=GetUpdates(), message="message is not modified")]
    await sender.send_one()
    assert (await rows(store))[0]["status"] == "sent"
    current = await job(repository)
    await repository.advance(current, Phase.PREPARE)
    await store.refresh_statuses()
    clock.now += 5
    session.effects = [TelegramBadRequest(method=GetUpdates(), message="message thread not found")]
    await sender.send_one()
    assert (await job(repository)).status == JobStatus.FAILED
    assert (await job(repository)).error_code == "telegram_delivery_unavailable"
    clock.now += 60
    await store.refresh_statuses()
    assert not await sender.send_one()


async def test_resume_button_removed_when_retention_expires(env):
    repository, store, clock, _bot, session, sender = env
    await store.accept(1, incoming())
    current = await repository.claim_next()
    failed = await repository.fail(current, error_code="download_failed")
    await store.refresh_statuses()
    await sender.send_one()
    assert session.calls[-1].reply_markup.inline_keyboard[0][0].text == "Продолжить"
    clock.now = failed.retention_until + 1
    await store.refresh_statuses()
    await sender.send_one()
    assert session.calls[-1].reply_markup.inline_keyboard == []
    result = await store.accept(
        2, Request("resume", chat_id=123, owner_id=123, job_id=1, generation=0, callback_id="old")
    )
    assert result.outcome == "unavailable"


async def test_unavailable_callback_message_is_acknowledged(env, settings):
    _repository, store, _clock, bot, session, sender = env
    callback = CallbackQuery(
        id="old",
        from_user=message().from_user,
        chat_instance="chat",
        inline_message_id="inline",
        data="cancel:1:0",
    )
    await process_update(
        bot,
        make_dispatcher(settings, "BotName"),
        store,
        Update(update_id=1, callback_query=callback),
    )
    await sender.send_one()
    assert session.calls[-1].__api_method__ == "answerCallbackQuery"
    assert "недоступно" in session.calls[-1].text


async def test_runtime_uses_custom_http_api_and_stops_during_long_poll(env, settings):
    from aiohttp import web

    from transcriptbot.telegram import run

    repository, store, _clock, _bot, _session, _sender = env
    shutdown, received, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    offsets = []
    served = False

    async def endpoint(request):
        nonlocal served
        method = request.match_info["method"]
        fields = await request.post()
        if method == "getMe":
            result = {"id": 12345, "is_bot": True, "first_name": "Test", "username": "TestBot"}
        elif method == "getUpdates":
            offsets.append(int(fields["offset"]))
            if not served:
                served = True
                result = [
                    Update(update_id=50, message=message("https://example.com/a")).model_dump(
                        mode="json"
                    )
                ]
            else:
                await release.wait()
                result = []
        elif method == "sendMessage":
            assert "Задание #1" in fields["text"]
            result = message(message_id=101).model_dump(mode="json")
            received.set()
        else:
            raise AssertionError(method)
        return web.json_response({"ok": True, "result": result})

    app = web.Application()
    app.router.add_post("/bot12345:telegram_test_secret/{method}", endpoint)
    runner = web.AppRunner(app, shutdown_timeout=1)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    configured = settings.model_copy(
        update={"telegram_bot_api_base_url": f"http://127.0.0.1:{port}"}
    )
    task = asyncio.create_task(run(configured, shutdown, repository))
    try:
        await asyncio.wait_for(received.wait(), 5)
        assert await store.offset() == 51
        assert offsets[0] == 0

        async def confirmed():
            while (await job(repository)).status_message_id != 101 or len(offsets) < 2:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(confirmed(), 5)
        shutdown.set()
        await asyncio.wait_for(task, 2)
        assert offsets[-1] == 51
    finally:
        release.set()
        shutdown.set()
        await asyncio.gather(task, return_exceptions=True)
        await runner.cleanup()


async def test_real_http_transport_streams_both_documents(env, settings):
    from aiogram.client.session.aiohttp import AiohttpSession
    from aiogram.client.telegram import TelegramAPIServer
    from aiohttp import web

    repository, store, clock, _bot, _session, _sender = env
    uploaded = []
    destinations = []

    async def endpoint(request):
        fields = await request.post()
        destinations.append((fields["chat_id"], fields.get("message_thread_id")))
        if "document" in fields:
            upload = fields[fields["document"].removeprefix("attach://")]
            uploaded.append((upload.filename, upload.file.read()))
        return web.json_response(
            {
                "ok": True,
                "result": message(chat_id=-100, message_id=101 + len(destinations)).model_dump(
                    mode="json"
                ),
            }
        )

    app = web.Application()
    app.router.add_post("/bot12345:fake_test/{method}", endpoint)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    session = AiohttpSession(
        api=TelegramAPIServer.from_base(f"http://127.0.0.1:{port}", is_local=True)
    )
    bot = Bot("12345:fake_test", session=session)
    sender = Sender(bot, store, settings)
    try:
        txt, verbose = await delivery(env, settings)
        for _ in range(5):
            await store.refresh_statuses()
            await sender.send_one()
            clock.now += 5
        assert len(uploaded) == 2
        assert uploaded[0][0].endswith("-1.txt")
        assert uploaded[0][1] == txt.read_bytes()
        assert uploaded[1][1] == verbose.read_bytes()
        assert all(chat == "-100" and topic == "12" for chat, topic in destinations)
        assert (await job(repository)).status == JobStatus.COMPLETED
    finally:
        await session.close()
        await runner.cleanup()


async def test_malformed_api_response_retries_without_logging_body(env, caplog):
    _repository, store, clock, _bot, session, sender = env
    await store.accept(1, incoming())
    session.effects = [
        ClientDecodeError("invalid", ValueError("parse"), "sensitive_token_and_body")
    ]
    with caplog.at_level("INFO"):
        assert await sender.send_one()
    assert (await rows(store))[0]["next_attempt_at"] == clock.now + 5
    assert "sensitive_token_and_body" not in caplog.text


async def test_artifact_cannot_use_another_jobs_symlink_directory(env, settings):
    repository, _store, _clock, _bot, session, _sender = env
    txt, _verbose = await delivery(env, settings)
    real = txt.parent
    other = real.with_name("2")
    real.rename(other)
    real.symlink_to(other, target_is_directory=True)
    await drain(env)
    assert (await job(repository)).error_code == "result_artifact_unavailable"
    assert not any(isinstance(call, SendDocument) for call in session.calls)
