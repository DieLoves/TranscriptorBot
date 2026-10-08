"""Persisted metadata; transcript content belongs in the job workspace."""

from dataclasses import dataclass, field
from enum import StrEnum


class SourceKind(StrEnum):
    TELEGRAM = "telegram"
    YOUTUBE = "youtube"
    MAILRU = "mailru"
    DIRECT = "direct"


class JobStatus(StrEnum):
    QUEUED = "queued"
    ACTIVE = "active"
    RETRY_WAIT = "retry_wait"
    QUOTA_WAIT = "quota_wait"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


class Phase(StrEnum):
    DOWNLOAD = "download"
    PREPARE = "prepare"
    TRANSCRIBE = "transcribe"
    ASSEMBLE = "assemble"
    DELIVER = "deliver"


@dataclass(frozen=True)
class SourceRef:
    kind: SourceKind
    reference: str = field(repr=False)
    title: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class Job:
    id: int
    owner_id: int
    chat_id: int
    message_id: int
    topic_id: int | None
    source_kind: SourceKind
    source_ref: str = field(repr=False)
    title: str | None = field(repr=False)
    size_bytes: int | None
    duration_seconds: float | None
    model: str
    queue_no: int
    status: JobStatus
    phase: Phase
    revision: int
    generation: int
    attempts: int
    quota_attempts: int
    next_attempt_at: float | None
    error_code: str | None
    created_at: float
    updated_at: float
    finished_at: float | None
    retention_until: float | None
    resumable: bool
    cleanup_pending: bool
    status_message_id: int | None


@dataclass(frozen=True)
class UpdateResult:
    outcome: str
    job_id: int | None
    queue_position: int | None = None


@dataclass(frozen=True)
class Chunk:
    job_id: int
    number: int
    start_sample: int
    end_sample: int
    response_path: str | None
    response_sha256: str | None
    completed_at: float | None
