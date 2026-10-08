import sys
from pathlib import Path

import pytest

from transcriptbot.config import Settings


@pytest.fixture
def settings_values(tmp_path: Path) -> dict[str, object]:
    return {
        "groq_api_key": "gsk_test_secret",
        "telegram_bot_token": "12345:telegram_test_secret",
        "telegram_api_id": 12345,
        "telegram_api_hash": "a" * 32,
        "allowed_user_ids": "123, 456,123",
        "sqlite_path": tmp_path / "data" / "db.sqlite3",
        "work_dir": tmp_path / "work" / "jobs",
        "telegram_files_dir": tmp_path / "telegram",
    }


@pytest.fixture
def settings(settings_values: dict[str, object], monkeypatch: pytest.MonkeyPatch) -> Settings:
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)
    return Settings(_env_file=None, **settings_values)


@pytest.fixture
def fake_media_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "bin"
    directory.mkdir()
    script = (
        f"#!{sys.executable}\n"
        "import os, sys\n"
        "if any(key in os.environ for key in "
        "('GROQ_API_KEY', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_API_HASH')): sys.exit(99)\n"
        "print('deno 2.9.7' if sys.argv[0].endswith('deno') else 'media tool version')\n"
    )
    for name in ("ffmpeg", "ffprobe", "deno"):
        path = directory / name
        path.write_text(script)
        path.chmod(0o755)
    monkeypatch.setenv("PATH", str(directory))
    return directory
