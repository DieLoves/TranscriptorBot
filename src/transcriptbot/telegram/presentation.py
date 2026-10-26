"""Russian UI generated solely from metadata; transcript previews are read from files."""

import re
from datetime import UTC, datetime
from html import escape

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
    "upload_size_limit": "подготовленная часть превышает лимит Groq",
    "checkpoint_invalid": "контрольная точка повреждена; отправьте источник заново",
    "groq_auth": "Groq отклонил доступ; требуется проверка настроек оператором",
    "groq_request": "Groq отклонил аудио или параметры запроса",
    "groq_quota_configuration": "настроенная квота Groq меньше сохранённой части аудио",
    "groq_network": "не удалось завершить запрос к Groq",
    "groq_server": "сервис Groq временно недоступен",
    "groq_invalid_response": "Groq вернул некорректный результат",
    "media_timeout": "истёк срок подготовки аудио",
    "pipeline_io": "не удалось сохранить рабочие файлы",
    "recovery_files": "рабочие файлы недоступны или повреждены; отправьте источник заново",
    "internal_error": "внутренняя ошибка обработки; повторы исчерпаны",
}

OUTCOME_TITLES = {
    "forbidden": "🔒 Доступ ограничен",
    "anonymous": "👤 Не удалось определить пользователя",
    "invalid_request": "💡 Как отправить запись",
    "size_limit": "⚠️ Файл слишком большой",
    "duration_limit": "⚠️ Запись слишком длинная",
    "user_limit": "⏳ Ваши задания ещё выполняются",
    "queue_limit": "⏳ Очередь заполнена",
    "unavailable": "⚠️ Задание недоступно",
    "cancelled": "✖️ Задание отменено",
    "resumed": "↻ Продолжаем обработку",
}


def outcome_payload(outcome: str) -> dict:
    return {
        "text": f"<b>{OUTCOME_TITLES[outcome]}</b>\n\n{escape(OUTCOMES[outcome])}",
        "parse_mode": "HTML",
    }


def _title(job: Job, limit: int = 100) -> str:
    title = " ".join((job.title or "").split())
    return escape(title[: limit - 1] + "…" if len(title) > limit else title)


def status_payload(job: Job, position: int | None, done: int, total: int, now: float) -> dict:
    icon = "⚙️"
    note = ""
    if job.status == JobStatus.QUEUED:
        icon = "🕓"
        stage = f"В очереди\nМесто: {position}" if position is not None else "В очереди"
        note = "Начну автоматически, когда подойдёт очередь."
    elif job.status == JobStatus.QUOTA_WAIT:
        icon = "⏳"
        try:
            deadline = datetime.fromtimestamp(job.next_attempt_at, UTC).strftime("%d.%m %H:%M UTC")
        except (TypeError, ValueError, OverflowError, OSError):
            # A legacy database may contain a deadline from an unchecked API header.
            deadline = "после ответа сервиса; требуется проверка срока оператором"
        stage = "Пауза: ожидание квоты\nПроверка: " + deadline
        note = "Продолжу автоматически, когда квота станет доступна."
    elif job.status == JobStatus.RETRY_WAIT:
        icon = "⏳"
        stage = f"Временная ошибка\nЭтап: {PHASES[job.phase]}"
        note = "Повтор запланирован автоматически."
    elif job.status == JobStatus.FAILED:
        icon = "⚠️"
        reason = ERROR_MESSAGES.get(
            job.error_code, f"не удалось выполнить этап «{PHASES[job.phase]}»"
        )
        stage = f"Не удалось завершить\nПричина: {reason}"
        note = (
            "Нажмите «Продолжить», чтобы вернуться к сохранённому месту."
            if job.resumable and job.retention_until is not None and job.retention_until > now
            else "Отправьте источник заново, чтобы создать задание."
        )
    elif job.status == JobStatus.CANCELLED:
        icon, stage = "✖️", "Отменено"
    elif job.status == JobStatus.COMPLETED:
        icon, stage = "✅", "Готово — результат отправлен"
        note = "В чате: превью, TXT и JSON."
    elif job.phase == Phase.TRANSCRIBE and total:
        done = min(max(done, 0), total)
        filled = done * 10 // total
        stage = (
            f"Этап: распознавание части {min(done + 1, total)}/{total}\n"
            f"{'▰' * filled}{'▱' * (10 - filled)}  Готово частей: {done}/{total}"
        )
    else:
        stage = "Этап: " + PHASES[job.phase]
    lines = [f"{icon} <b>Задание #{job.id}</b>"]
    if title := _title(job):
        lines.append(title)
    lines.extend(["", escape(stage)])
    if note:
        lines.extend(["", escape(note)])
    payload = {
        "text": "\n".join(lines),
        "parse_mode": "HTML",
        "reply_markup": {"inline_keyboard": []},
    }
    button = None
    if job.status in {
        JobStatus.QUEUED,
        JobStatus.ACTIVE,
        JobStatus.RETRY_WAIT,
        JobStatus.QUOTA_WAIT,
    }:
        button = {"text": "✖️ Отменить", "callback_data": f"cancel:{job.id}:{job.generation}"}
    elif (
        job.status == JobStatus.FAILED
        and job.resumable
        and job.retention_until is not None
        and job.retention_until > now
    ):
        button = {"text": "↻ Продолжить", "callback_data": f"resume:{job.id}:{job.generation}"}
    if button:
        payload["reply_markup"]["inline_keyboard"] = [[button]]
    return payload


def jobs_payload(jobs: list[Job]) -> dict:
    labels = {
        JobStatus.QUEUED: "🕓 в очереди",
        JobStatus.RETRY_WAIT: "⏳ ожидает повтора",
        JobStatus.QUOTA_WAIT: "⏳ ожидание квоты",
        JobStatus.FAILED: "⚠️ ошибка",
        JobStatus.CANCELLED: "✖️ отменено",
        JobStatus.COMPLETED: "✅ готово",
    }
    lines = ["🗂 <b>Ваши задания</b>"]
    for job in jobs:
        label = labels.get(job.status, "⚙️ " + PHASES[job.phase])
        lines.extend(["", f"<b>#{job.id}</b> · {label}"])
        if title := _title(job, 48):
            lines.append(title)
    lines.extend(
        ["", "Подробнее: <code>/status ID</code>"]
        if jobs
        else ["", "У вас пока нет заданий. Отправьте файл или ссылку, и я распознаю речь."]
    )
    return {"text": "\n".join(lines), "parse_mode": "HTML"}


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


def _fit_text(text: str, budget: int, *, html: bool = False) -> tuple[str, bool]:
    """Keep complete characters/entities within a conservative UTF-16 wire budget."""
    parts = []
    used = 0
    for char in text:
        part = escape(char, quote=False) if html else char
        size = len(part.encode("utf-16-le")) // 2
        if used + size > budget:
            return "".join(parts), True
        parts.append(part)
        used += size
    return "".join(parts), False


def preview_payload(path, limit: int) -> dict:
    header = "📝 <b>Расшифровка</b>\n\n"
    footer = "\n\n📎 Полный текст — в TXT. Таймкоды и данные — в JSON."
    overhead = len((header + footer).encode("utf-16-le")) // 2
    if limit <= overhead + 2:
        # Very small configured previews still respect their limit without broken markup.
        text, _ = _fit_text(preview_text(path, limit), limit)
        return {"text": text or "…"}
    with path.open(encoding="utf-8") as stream:
        source = stream.read(limit + 1)
    if not source.strip():
        source = "Речь не распознана."
        footer = "\n\n📎 Отправляю пустой TXT и JSON."
    text, truncated = _fit_text(source, limit - overhead - 2, html=True)
    return {
        "text": header + text + ("\n…" if truncated else "") + footer,
        "parse_mode": "HTML",
    }
