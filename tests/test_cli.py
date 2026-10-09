import os
import selectors
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from transcriptbot.config import Settings


@pytest.fixture
def process_environment(settings: Settings, fake_media_tools: Path) -> dict[str, str]:
    settings.telegram_files_dir.mkdir(mode=0o700)
    environment = os.environ.copy()
    environment.update(
        {
            name.upper(): str(
                value.get_secret_value() if hasattr(value, "get_secret_value") else value
            )
            for name, value in settings.model_dump().items()
            if name != "allowed_user_ids"
        }
    )
    environment["ALLOWED_USER_IDS"] = "123,456"
    return environment


@pytest.mark.parametrize("role", ["bot", "worker"])
def test_cli_check(role: str, process_environment: dict[str, str], tmp_path: Path) -> None:
    process = subprocess.run(
        [sys.executable, "-m", "transcriptbot", role, "--check", "--env-file", "missing.env"],
        cwd=tmp_path,
        env=process_environment,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert process.returncode == 0, process.stderr
    assert f"Окружение {role} проверено" in process.stdout


@pytest.mark.parametrize("role", ["bot", "worker"])
def test_invalid_settings_exit_before_startup(
    role: str,
    process_environment: dict[str, str],
    tmp_path: Path,
) -> None:
    process_environment["TELEGRAM_BOT_TOKEN"] = "invalid-secret"
    process_environment["PATH"] = "/nonexistent"
    process = subprocess.run(
        [sys.executable, "-m", "transcriptbot", role, "--env-file", "missing.env"],
        cwd=tmp_path,
        env=process_environment,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert process.returncode == 2
    assert "TELEGRAM_BOT_TOKEN" in process.stderr
    assert "event=ready" not in process.stderr
    assert "invalid-secret" not in process.stdout + process.stderr


@pytest.mark.parametrize("role", ["bot", "worker"])
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_process_stops_cleanly(
    role: str,
    stop_signal: signal.Signals,
    process_environment: dict[str, str],
    tmp_path: Path,
) -> None:
    migration = subprocess.run(
        [sys.executable, "-m", "transcriptbot", "migrate", "--env-file", "missing.env"],
        cwd=tmp_path,
        env=process_environment,
        capture_output=True,
        timeout=15,
    )
    assert migration.returncode == 0, migration.stderr.decode()
    with subprocess.Popen(
        [sys.executable, "-m", "transcriptbot", role, "--env-file", "missing.env"],
        cwd=tmp_path,
        env=process_environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as process:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stderr, selectors.EVENT_READ)
                assert selector.select(timeout=15), "Process did not become ready"
                ready = process.stderr.readline().decode()
            assert "event=ready phase=bootstrap" in ready
            process.send_signal(stop_signal)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, stderr.decode()
            assert "event=stopped" in stderr.decode()
            output = ready + stdout.decode() + stderr.decode()
            assert "gsk_test_secret" not in output
            assert "telegram_test_secret" not in output
            assert "a" * 32 not in output
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_migrate_command_is_offline_and_repeatable(
    process_environment: dict[str, str],
    tmp_path: Path,
) -> None:
    process_environment["PATH"] = "/nonexistent"
    for _ in range(2):
        migration = subprocess.run(
            [sys.executable, "-m", "transcriptbot", "migrate", "--env-file", "missing.env"],
            cwd=tmp_path,
            env=process_environment,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert migration.returncode == 0, migration.stderr
        assert "Миграции SQLite применены" in migration.stdout
    assert Path(process_environment["SQLITE_PATH"]).is_file()


@pytest.mark.parametrize("role", ["bot", "worker"])
def test_startup_requires_explicit_migrations(
    role: str,
    process_environment: dict[str, str],
    tmp_path: Path,
) -> None:
    process = subprocess.run(
        [sys.executable, "-m", "transcriptbot", role, "--env-file", "missing.env"],
        cwd=tmp_path,
        env=process_environment,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert process.returncode == 2
    assert "transcriptbot migrate" in process.stderr
    assert "Traceback" not in process.stderr
    assert "event=ready" not in process.stderr


def test_unexpected_runtime_error_never_prints_sensitive_library_output(
    settings, fake_media_tools, monkeypatch, capsys
):
    from transcriptbot import cli

    settings.telegram_files_dir.mkdir(parents=True)
    monkeypatch.setattr(cli, "load_settings", lambda *_: settings)

    async def broken(*args):
        raise RuntimeError("private-library-output https://example.com/?token=secret")

    monkeypatch.setattr(cli, "serve", broken)
    with pytest.raises(SystemExit) as caught:
        cli.main(["worker", "--env-file", "missing.env"])
    assert caught.value.code == 2
    output = capsys.readouterr()
    assert "Непредвиденная ошибка" in output.err
    assert "private-library-output" not in output.err
    assert "token=secret" not in output.err
    assert "Traceback" not in output.err
