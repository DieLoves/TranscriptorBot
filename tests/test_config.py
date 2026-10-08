from pathlib import Path

import pytest
from pydantic import ValidationError

from transcriptbot.config import ConfigurationError, Settings, load_settings


def test_defaults_and_secret_repr(settings: Settings) -> None:
    assert settings.allowed_user_ids == {123, 456}
    assert settings.groq_model == "whisper-large-v3-turbo"
    assert settings.max_input_bytes == 2_000_000_000
    assert settings.chunk_seconds == 600
    for value in ("gsk_test_secret", "telegram_test_secret", "a" * 32):
        assert value not in repr(settings)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("groq_api_key", ""),
        ("groq_api_key", "space secret"),
        ("groq_model", "whisper-large-v3"),
        ("telegram_bot_token", "invalid-secret"),
        ("telegram_api_id", 0),
        ("telegram_api_hash", "not-a-hash"),
        ("allowed_user_ids", ""),
        ("allowed_user_ids", "123,"),
        ("allowed_user_ids", "123,-5"),
        ("allowed_user_ids", "[123,456]"),
        ("allowed_user_ids", [True]),
        ("telegram_bot_api_base_url", "https://api.telegram.org"),
        ("telegram_bot_api_base_url", "http://user:secret@localhost:8081"),
        ("telegram_bot_api_base_url", "http://localhost:8081?token=secret"),
        ("telegram_bot_api_base_url", "http://localhost:70000"),
        ("telegram_bot_api_base_url", "ftp://localhost"),
        ("sqlite_path", "relative.sqlite3"),
        ("work_dir", "/work/../telegram"),
        ("work_dir", "/work/\0jobs"),
        ("max_input_bytes", 2_000_000_001),
        ("max_duration_seconds", 14_401),
        ("groq_max_upload_bytes", 25_000_001),
        ("wav_target_upload_bytes", 25_000_001),
        ("chunk_seconds", 800),
        ("silence_min_seconds", 9),
        ("silence_window_seconds", 600),
        ("max_queue_jobs", 2),
        ("preview_max_chars", 3001),
        ("status_update_interval_seconds", 1),
        ("http_read_timeout_seconds", "nan"),
        ("groq_timeout_seconds", "inf"),
        ("max_retries", 6),
        ("quota_retry_max_seconds", 3601),
        ("log_level", "VERBOSE"),
    ],
)
def test_invalid_configuration(
    settings: Settings,
    name: str,
    value: object,
) -> None:
    values = settings.model_dump()
    values[name] = value
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)


@pytest.mark.parametrize("name", ["work_dir", "telegram_files_dir"])
def test_data_directories_cannot_overlap(settings: Settings, name: str) -> None:
    values = settings.model_dump()
    values[name] = settings.sqlite_path.parent / "child"
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)


def test_symlink_cannot_hide_directory_overlap(settings: Settings, tmp_path: Path) -> None:
    settings.sqlite_path.parent.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(settings.sqlite_path.parent, target_is_directory=True)
    values = settings.model_dump()
    values["work_dir"] = alias / "jobs"
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)


def test_env_file_and_environment_override(
    tmp_path: Path,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "settings.env"
    env_file.write_text(
        "\n".join(
            f"{key.upper()}="
            f"{value.get_secret_value() if hasattr(value, 'get_secret_value') else value}"
            for key, value in settings.model_dump().items()
            if key != "allowed_user_ids"
        )
        + "\nALLOWED_USER_IDS=789\n"
    )
    monkeypatch.setenv("ALLOWED_USER_IDS", "987")
    assert load_settings(env_file).allowed_user_ids == {987}


def test_configuration_error_never_contains_secret_values(
    tmp_path: Path,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "invalid-secret-do-not-log")
    with pytest.raises(ConfigurationError) as error:
        load_settings(tmp_path / "missing.env")
    assert "TELEGRAM_BOT_TOKEN" in str(error.value)
    assert "invalid-secret-do-not-log" not in str(error.value)
    assert error.value.__suppress_context__


def test_every_template_setting_is_declared() -> None:
    template = Path(__file__).parents[1] / ".env.example"
    names = {
        line.split("=", 1)[0].strip().lower()
        for line in template.read_text().splitlines()
        if line and not line.startswith("#")
    }
    assert names == set(Settings.model_fields)
