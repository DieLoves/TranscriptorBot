"""Validated configuration; secrets must never appear in diagnostics."""

import re
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

PositiveInt = Annotated[int, Field(gt=0)]
PositiveFloat = Annotated[float, Field(gt=0)]


class ConfigurationError(Exception):
    """Safe diagnostic containing field names, never supplied values."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
        allow_inf_nan=False,
    )

    groq_api_key: SecretStr = Field(min_length=1, repr=False)
    groq_model: Literal["whisper-large-v3-turbo"] = "whisper-large-v3-turbo"
    telegram_bot_token: SecretStr = Field(min_length=1, repr=False)
    telegram_api_id: PositiveInt
    telegram_api_hash: SecretStr = Field(min_length=1, repr=False)
    allowed_user_ids: Annotated[frozenset[int], NoDecode]

    telegram_bot_api_base_url: str = "http://telegram-bot-api:8081"
    sqlite_path: Path = Path("/data/transcriptbot.sqlite3")
    work_dir: Path = Path("/work/jobs")
    telegram_files_dir: Path = Path("/telegram")

    max_input_bytes: Annotated[int, Field(gt=0, le=2_000_000_000)] = 2_000_000_000
    max_duration_seconds: Annotated[float, Field(gt=0, le=14_400)] = 14_400
    groq_max_upload_bytes: Annotated[int, Field(gt=0, le=25_000_000)] = 25_000_000
    wav_target_upload_bytes: PositiveInt = 24_500_000
    chunk_seconds: PositiveFloat = 600
    silence_window_seconds: PositiveFloat = 8
    silence_min_seconds: PositiveFloat = 0.3
    preview_max_chars: Annotated[int, Field(gt=0, le=3000)] = 3000

    max_user_jobs: PositiveInt = 3
    max_queue_jobs: PositiveInt = 30
    failed_retention_days: PositiveInt = 7
    cleanup_interval_seconds: PositiveFloat = 600
    status_update_interval_seconds: Annotated[float, Field(ge=5)] = 5
    disk_reserve_bytes: PositiveInt = 1_073_741_824

    max_retries: Annotated[int, Field(ge=0, le=5)] = 5
    quota_retry_max_seconds: Annotated[float, Field(ge=60, le=3600)] = 3600
    http_connect_timeout_seconds: PositiveFloat = 10
    http_read_timeout_seconds: PositiveFloat = 60
    http_max_redirects: Annotated[int, Field(ge=0)] = 5
    download_timeout_seconds: PositiveFloat = 3600
    groq_timeout_seconds: PositiveFloat = 300
    ffprobe_timeout_seconds: PositiveFloat = 60
    ffmpeg_timeout_seconds: PositiveFloat = 300

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @field_validator("groq_api_key", "telegram_bot_token", "telegram_api_hash")
    @classmethod
    def secret_has_no_whitespace(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if not raw or any(character.isspace() for character in raw):
            raise ValueError("invalid secret")
        return value

    @field_validator("telegram_bot_token")
    @classmethod
    def valid_bot_token(cls, value: SecretStr) -> SecretStr:
        if not re.fullmatch(r"[1-9]\d*:[A-Za-z0-9_-]+", value.get_secret_value()):
            raise ValueError("invalid token")
        return value

    @field_validator("telegram_api_hash")
    @classmethod
    def valid_api_hash(cls, value: SecretStr) -> SecretStr:
        if not re.fullmatch(r"[a-fA-F0-9]{32}", value.get_secret_value()):
            raise ValueError("invalid API hash")
        return value

    @field_validator("allowed_user_ids", mode="before")
    @classmethod
    def parse_user_ids(cls, value: object) -> frozenset[int]:
        if isinstance(value, str):
            parts = value.split(",")
            if not all(re.fullmatch(r"[1-9]\d*", part.strip()) for part in parts):
                raise ValueError("expected positive Telegram IDs separated by commas")
            return frozenset(int(part.strip()) for part in parts)
        if (
            not isinstance(value, (set, frozenset, list, tuple))
            or not value
            or any(type(item) is not int or item <= 0 for item in value)
        ):
            raise ValueError("expected positive Telegram IDs")
        return frozenset(value)

    @field_validator("telegram_bot_api_base_url")
    @classmethod
    def valid_api_url(cls, value: str) -> str:
        try:
            url = urlsplit(value)
            port = url.port
        except ValueError:
            raise ValueError("invalid Bot API URL") from None
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
            or any(character.isspace() for character in value)
            or port == 0
            or url.hostname.lower().rstrip(".") == "api.telegram.org"
        ):
            raise ValueError("expected own Bot API base URL")
        return value.rstrip("/")

    @field_validator("sqlite_path", "work_dir", "telegram_files_dir")
    @classmethod
    def absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts or "\0" in str(value):
            raise ValueError("expected absolute path without parent traversal")
        try:
            return value.resolve()
        except (OSError, RuntimeError):
            raise ValueError("cannot resolve data path") from None

    @model_validator(mode="after")
    def consistent_limits(self) -> "Settings":
        if self.wav_target_upload_bytes > self.groq_max_upload_bytes:
            raise ValueError("WAV_TARGET_UPLOAD_BYTES exceeds GROQ_MAX_UPLOAD_BYTES")
        # PCM16, mono, 16 kHz with room for the WAV header emitted by FFmpeg.
        if self.chunk_seconds * 32_000 + 1024 > self.wav_target_upload_bytes:
            raise ValueError("CHUNK_SECONDS exceeds WAV_TARGET_UPLOAD_BYTES")
        if not self.silence_min_seconds <= self.silence_window_seconds < self.chunk_seconds:
            raise ValueError("inconsistent silence window")
        if self.max_user_jobs > self.max_queue_jobs:
            raise ValueError("MAX_USER_JOBS exceeds MAX_QUEUE_JOBS")
        roots = [self.sqlite_path.parent, self.work_dir, self.telegram_files_dir]
        for index, left in enumerate(roots):
            for right in roots[index + 1 :]:
                if left.is_relative_to(right) or right.is_relative_to(left):
                    raise ValueError("data directories must not overlap")
        return self


def load_settings(env_file: Path | None = Path(".env")) -> Settings:
    try:
        return Settings(_env_file=env_file)
    except ValidationError as exc:
        fields = sorted(
            {
                ".".join(str(part).upper() for part in error["loc"]) or "LIMITS/PATHS"
                for error in exc.errors(include_input=False, include_context=False)
            }
        )
        raise ConfigurationError("Некорректные настройки: " + ", ".join(fields)) from None
    except (OSError, ValueError):
        raise ConfigurationError("Не удалось прочитать настройки окружения") from None
