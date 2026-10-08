from pathlib import Path

import pytest

from transcriptbot.bootstrap import EnvironmentError, check_environment, check_executable
from transcriptbot.config import Settings


@pytest.mark.parametrize("role", ["bot", "worker"])
def test_startup_checks_without_external_api(
    settings: Settings,
    fake_media_tools: Path,
    role: str,
) -> None:
    settings.telegram_files_dir.mkdir(mode=0o700)
    check_environment(settings, role)
    assert settings.work_dir.is_dir()
    assert settings.sqlite_path.parent.is_dir()
    assert not settings.sqlite_path.exists()  # Migrations belong to stage 2.
    assert settings.work_dir.stat().st_mode & 0o777 == 0o700


def test_missing_shared_cache_fails(settings: Settings) -> None:
    with pytest.raises(EnvironmentError, match="Каталог данных недоступен"):
        check_environment(settings, "bot")


def test_missing_executable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", "/nonexistent")
    with pytest.raises(EnvironmentError, match="deno"):
        check_executable("deno")


def test_old_deno_rejected(fake_media_tools: Path) -> None:
    path = fake_media_tools / "deno"
    path.write_text("#!/bin/sh\nprintf 'deno 2.2.0\\n'\n")
    with pytest.raises(EnvironmentError, match=r"2\.3\.0"):
        check_executable("deno")


def test_broken_media_tool_rejected(fake_media_tools: Path) -> None:
    (fake_media_tools / "ffmpeg").write_text("#!/bin/sh\nexit 1\n")
    with pytest.raises(EnvironmentError, match="ffmpeg"):
        check_executable("ffmpeg")
