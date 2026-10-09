"""Turn Telegram messages into small, validated requests without network or storage I/O."""

import re
from dataclasses import dataclass, replace
from urllib.parse import parse_qs, urlsplit

from aiogram import Dispatcher, Router
from aiogram.types import CallbackQuery, Message

from transcriptbot.config import Settings
from transcriptbot.storage.models import SourceKind, SourceRef

HELP = (
    "🎙 <b>Из записи — в текст</b>\n\n"
    "Отправьте файл или ссылку — я распознаю речь и пришлю результат.\n\n"
    "<b>Что можно отправить</b>\n"
    "• Аудио, видео или документ с медиа\n"
    "• Голосовое сообщение или видеокружок\n"
    "• YouTube, публичный файл Облака Mail.ru или прямую HTTP/HTTPS-ссылку\n\n"
    "<b>Ограничения</b>\n"
    "До <b>2 ГБ</b> и <b>4 часов</b> на запись. Задания выполняются по очереди.\n"
    "Папки, плейлисты, эфиры и источники с авторизацией не поддерживаются.\n\n"
    "<b>Команды</b>\n"
    "/jobs — последние 10 ваших заданий\n"
    "<code>/status ID</code> — статус задания\n"
    "/help — эта справка\n\n"
    "<b>В группе</b>\n"
    "<code>/transcribe &lt;ссылка&gt;</code> или /transcribe ответом на файл или ссылку.\n"
    "В сообщении задания есть кнопки отмены и продолжения после ошибки.\n\n"
    "📎 <b>Результат</b>\n"
    "Превью в чате, полный текст в TXT и данные распознавания в verbose JSON.\n"
    "Файлы удаляются после доставки. После ошибки — хранятся 7 дней для продолжения."
)


@dataclass(frozen=True)
class Request:
    action: str
    chat_id: int | None = None
    topic_id: int | None = None
    message_id: int | None = None
    source_message_id: int | None = None
    owner_id: int | None = None
    group_chat_id: int | None = None
    source: SourceRef | None = None
    size_bytes: int | None = None
    duration_seconds: float | None = None
    job_id: int | None = None
    generation: int = 0
    callback_id: str | None = None


def url_source(text: str) -> SourceRef | None:
    if not text or any(character.isspace() for character in text):
        return None
    try:
        url = urlsplit(text)
        port = url.port
    except ValueError:
        return None
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or port == 0
        or url.username is not None
        or url.password is not None
    ):
        return None
    host = url.hostname.lower().rstrip(".")
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}:
        if "list" in parse_qs(url.query) or not (
            (
                host == "youtu.be"
                and len(url.path.strip("/").split("/")) == 1
                and bool(url.path.strip("/"))
            )
            or (url.path == "/watch" and len(parse_qs(url.query).get("v", [])) == 1)
            or re.fullmatch(r"/(shorts|embed)/[^/]+/?", url.path)
        ):
            return None
        return SourceRef(SourceKind.YOUTUBE, text)
    if host == "cloud.mail.ru":
        if url.scheme != "https" or not re.fullmatch(r"/public/[^/]+/[^/]+(?:/.*)?", url.path):
            return None
        return SourceRef(SourceKind.MAILRU, text)
    return SourceRef(SourceKind.DIRECT, text)


def extract_source(message: Message) -> tuple[SourceRef, int | None, float | None] | None:
    for kind in ("audio", "video", "document", "voice", "video_note"):
        media = getattr(message, kind)
        if media is not None:
            title = getattr(media, "file_name", None) or kind
            return (
                SourceRef(SourceKind.TELEGRAM, media.file_id, title),
                media.file_size,
                getattr(media, "duration", None),
            )
    text = (message.text or message.caption or "").strip()
    links = [
        entity.url
        for entity in (message.entities or message.caption_entities or [])
        if entity.type == "text_link"
    ]
    if links:
        source = (
            url_source(links[0]) if len(links) == 1 and not re.search(r"https?://", text) else None
        )
    else:
        source = url_source(text)
    return (source, None, None) if source is not None else None


def message_request(message: Message, settings: Settings, username: str) -> Request:
    base = Request(
        "invalid_request",
        chat_id=message.chat.id,
        topic_id=message.message_thread_id if message.is_topic_message else None,
        message_id=message.message_id,
        source_message_id=message.message_id,
        owner_id=message.from_user.id if message.from_user else None,
        group_chat_id=message.chat.id if message.chat.type != "private" else None,
    )
    text = message.text or ""
    match = re.match(r"^/([a-zA-Z]+)(?:@([a-zA-Z0-9_]+))?(?:\s+(.*))?$", text, re.DOTALL)
    command, argument = None, ""
    if match:
        command, target, argument = match.groups()
        argument = (argument or "").strip()
        if target and target.casefold() != username.casefold():
            return replace(base, action="ignored")
    elif text.startswith("/"):
        return replace(base, action="ignored" if base.group_chat_id else "invalid_request")
    if base.group_chat_id is not None and command is None:
        return replace(base, action="ignored")
    if message.sender_chat is not None or message.from_user is None or message.from_user.is_bot:
        return replace(base, action="anonymous")
    if base.owner_id not in settings.allowed_user_ids:
        return replace(base, action="forbidden")
    if command in {"start", "help", "jobs"}:
        return replace(base, action="help" if command != "jobs" else "jobs")
    if command == "status":
        if re.fullmatch(r"[1-9]\d{0,17}", argument):
            return replace(base, action="status", job_id=int(argument))
        return base
    if command not in {None, "transcribe"}:
        return base
    if command == "transcribe" and argument:
        source = url_source(argument)
        media = (source, None, None) if source else None
    else:
        original = message.reply_to_message if command == "transcribe" else message
        media = extract_source(original) if original else None
        if original:
            base = replace(base, source_message_id=original.message_id)
    if media is None:
        return base
    source, size, duration = media
    if size is not None and size > settings.max_input_bytes:
        return replace(base, action="size_limit")
    if duration is not None and duration > settings.max_duration_seconds:
        return replace(base, action="duration_limit")
    return replace(base, action="create", source=source, size_bytes=size, duration_seconds=duration)


def callback_request(callback: CallbackQuery, settings: Settings) -> Request:
    message = callback.message
    base = Request(
        "unavailable",
        chat_id=message.chat.id if message else None,
        topic_id=getattr(message, "message_thread_id", None),
        owner_id=callback.from_user.id,
        group_chat_id=message.chat.id if message and message.chat.type != "private" else None,
        callback_id=callback.id,
    )
    if callback.from_user.is_bot or callback.from_user.id not in settings.allowed_user_ids:
        return replace(base, action="forbidden")
    match = re.fullmatch(r"(cancel|resume):([1-9]\d{0,17}):(\d{1,9})", callback.data or "")
    if message is None or match is None:
        return base
    action, job_id, generation = match.groups()
    return replace(base, action=action, job_id=int(job_id), generation=int(generation))


def make_dispatcher(settings: Settings, username: str) -> Dispatcher:
    router = Router(name="transcriptbot")

    @router.message()
    async def on_message(message: Message) -> Request:
        return message_request(message, settings, username)

    @router.channel_post()
    async def on_channel(message: Message) -> Request:
        return message_request(message, settings, username)

    @router.callback_query()
    async def on_callback(callback_query: CallbackQuery) -> Request:
        return callback_request(callback_query, settings)

    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    return dispatcher
