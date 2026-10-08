"""Russian UI generated solely from metadata; transcript previews are read from files."""

import re
from datetime import UTC, datetime

from transcriptbot.storage.models import Job, JobStatus, Phase

PHASES = {
    Phase.DOWNLOAD: "скачивание",
    Phase.PREPARE: "подготовка аудио",
    Phase.TRANSCRIBE: "распознавание",
    Phase.ASSEMBLE: "сборка результата",
    Phase.DELIVER: "отправка результата",
}
OUTCOMES = {
    "forbidden": "Доступ к боту закрыт для вашего Telegram ID.",
    "anonymous": (
        "Нужна команда от пользователя с доступным Telegram ID. "
        "Анонимные команды не поддерживаются."
    ),
    "invalid_request": "Отправьте один медиафайл или одну ссылку. /help — правила использования.",
    "size_limit": "Файл превышает ограничение 2 ГБ.",
    "duration_limit": "Запись превышает ограничение 4 часа.",
    "user_limit": (
        "Достигнут лимит ваших незавершённых заданий. Дождитесь завершения или отмените задание."
    ),
    "queue_limit": "Общая очередь заполнена. Попробуйте позже.",
    "unavailable": (
        "Задание недоступно, кнопка устарела или срок продолжения истёк. "
        "При необходимости отправьте источник заново."
    ),
    "cancelled": "Задание отменено.",
    "resumed": "Задание поставлено в конец очереди.",
}


ERROR_MESSAGES = {
    "size_limit": "файл превышает ограничение размера",
    "duration_limit": "запись превышает ограничение длительности",
    "invalid_media": "не найдена пригодная аудиодорожка или формат не поддерживается",
    "unsafe_url": "ссылка или сервер скачивания не разрешены",
    "unsafe_address": "адрес скачивания относится к запрещённой сети",
    "unsafe_path": "источник находится вне разрешённого каталога",
    "source_unavailable": "источник недоступен для публичного скачивания",
    "source_authorization": "источник требует авторизации; такие ссылки не поддерживаются",
    "youtube_format_unavailable": "YouTube не предоставил доступную аудиодорожку",
    "download_tls": "сертификат сервера скачивания не прошёл проверку",
    "unsupported_source": "плейлисты, эфиры и этот способ скачивания не поддерживаются",
    "source_changed": "сохранённый источник изменился; отправьте его заново",
    "disk_space": "недостаточно свободного места для загрузки",
    "download_network": "не удалось подключиться к источнику",
    "download_timeout": "истёк срок скачивания",
    "telegram_delivery_unavailable": "чат или тема недоступны для отправки",
    "telegram_delivery_failed": "не удалось отправить сообщение или результат",
    "result_artifact_unavailable": "файл результата недоступен",
}


def status_payload(job: Job, position: int | None, done: int, total: int, now: float) -> dict:
    if job.status == JobStatus.QUEUED:
        stage = f"в очереди, место {position}"
    elif job.status == JobStatus.QUOTA_WAIT:
        stage = "ожидание квоты; проверка " + datetime.fromtimestamp(
            job.next_attempt_at, UTC
        ).strftime("%d.%m %H:%M UTC")
    elif job.status == JobStatus.RETRY_WAIT:
        stage = f"временная ошибка ({PHASES[job.phase]}); запланирован повтор"
    elif job.status == JobStatus.FAILED:
        reason = ERROR_MESSAGES.get(
            job.error_code, f"не удалось выполнить этап «{PHASES[job.phase]}»"
        )
        stage = f"ошибка: {reason}"
    elif job.status == JobStatus.CANCELLED:
        stage = "отменено"
    elif job.status == JobStatus.COMPLETED:
        stage = "результат отправлен"
    elif job.phase == Phase.TRANSCRIBE and total:
        stage = f"распознавание части {min(done + 1, total)}/{total}"
    else:
        stage = PHASES[job.phase]
    payload = {"text": f"Задание #{job.id}: {stage}.", "reply_markup": {"inline_keyboard": []}}
    button = None
    if job.status in {
        JobStatus.QUEUED,
        JobStatus.ACTIVE,
        JobStatus.RETRY_WAIT,
        JobStatus.QUOTA_WAIT,
    }:
        button = {"text": "Отменить", "callback_data": f"cancel:{job.id}:{job.generation}"}
    elif job.status == JobStatus.FAILED and job.resumable and job.retention_until > now:
        button = {"text": "Продолжить", "callback_data": f"resume:{job.id}:{job.generation}"}
    if button:
        payload["reply_markup"]["inline_keyboard"] = [[button]]
    return payload


def safe_filename(job: Job, operation: str) -> str:
    stem = re.sub(r"[^\w. -]", "_", job.title or "transcript", flags=re.UNICODE)
    stem = stem.strip(" .")[:80] or "transcript"
    return f"{stem}-{job.id}.{'txt' if operation == 'txt' else 'verbose.json'}"


def preview_text(path, limit: int) -> str:
    # Read a bounded prefix, including the character needed to detect truncation.
    with path.open(encoding="utf-8") as stream:
        text = stream.read(limit + 1)
    if not text.strip():
        return "Речь не распознана. Отправляю пустой TXT и JSON."
    if len(text) > limit:
        suffix = "\n… Полная версия — в TXT."
        return text[: limit - len(suffix)] + suffix if limit > len(suffix) else text[:limit]
    return text
