"""Offline container checks using synthetic credentials and disposable volumes."""

import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

# Override to check a candidate image before it replaces the running tag.
APP_IMAGE = os.environ.get("TRANSCRIPTBOT_APP_IMAGE", "transcriptbot-app:audit")
API_IMAGE = os.environ.get("TRANSCRIPTBOT_API_IMAGE", "transcriptbot-bot-api:audit")
TEST_ENV = """GROQ_API_KEY=gsk_container_test
TELEGRAM_BOT_TOKEN=12345:container_test
TELEGRAM_API_ID=12345
TELEGRAM_API_HASH=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
ALLOWED_USER_IDS=12345
"""


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=60,
    )


def check_compose(env_file: Path) -> None:
    # Avoid resolving service env_file entries, which contain real local credentials.
    config = json.loads(
        docker(
            "compose",
            "--env-file",
            str(env_file),
            "config",
            "--no-env-resolution",
            "--format",
            "json",
        ).stdout
    )
    assert set(config["services"]) == {"bot", "worker", "telegram-bot-api"}
    assert config["networks"]["backend"]["internal"]
    for name, service in config["services"].items():
        assert not service.get("ports"), f"Unexpected exposed port: {name}"
        assert service["user"] == "10001:10001"
        assert service["read_only"]
        assert "ALL" in service["cap_drop"]
        expected_memory = {"512m", "536870912"} if name == "bot" else {"2g", "2147483648"}
        # Compose versions emit either the original unit string or byte count.
        assert str(service["mem_limit"]).lower() in expected_memory
        assert service["pids_limit"] == 128
        assert float(service["cpus"]) == 2
    print("Compose: three services, private Bot API, persistent volumes, non-root")


def main() -> None:
    prefix = "transcriptbot-check-" + uuid.uuid4().hex[:12]
    volumes: list[str] = []
    containers: list[str] = []
    with TemporaryDirectory(prefix="transcriptbot-check-") as temporary:
        env_file = Path(temporary) / "test.env"
        env_file.write_text(TEST_ENV)
        try:
            check_compose(env_file)
            for suffix in ("data", "work", "telegram"):
                volume = f"{prefix}-{suffix}"
                docker("volume", "create", volume)
                volumes.append(volume)
            common = [
                "--network",
                "none",
                "--read-only",
                "--tmpfs",
                "/tmp:mode=1777,size=256m",
                "--memory",
                "2g",
                "--cpus",
                "2",
                "--pids-limit",
                "128",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges:true",
                "--env-file",
                str(env_file),
            ]
            # Populate the shared cache with the server image's UID and permissions.
            api_help = docker(
                "run",
                "--rm",
                *common,
                "--volume",
                f"{volumes[2]}:/telegram",
                API_IMAGE,
                "--help",
            )
            assert "--local" in api_help.stdout + api_help.stderr
            print("Official Bot API binary: executable and --local supported")
            api_name = f"{prefix}-api"
            containers.append(api_name)
            docker(
                "run",
                "--detach",
                "--name",
                api_name,
                *common,
                "--volume",
                f"{volumes[2]}:/telegram",
                API_IMAGE,
            )
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                probe = docker(
                    "exec",
                    api_name,
                    "python3",
                    "-c",
                    "import socket; socket.create_connection(('127.0.0.1', 8081), 1).close()",
                    check=False,
                )
                if probe.returncode == 0:
                    break
                time.sleep(0.25)
            else:
                raise RuntimeError("Bot API did not open its local listener")
            docker("stop", "--time", "5", api_name)
            api_state = json.loads(
                docker(
                    "inspect",
                    "--format",
                    "{{json .State}}",
                    api_name,
                ).stdout
            )
            assert api_state["ExitCode"] == 0 and not api_state["OOMKilled"]
            print("Bot API: local listener and SIGTERM passed without network access")
            migration_mounts = ["--volume", f"{volumes[0]}:/data"]
            for _ in range(2):
                result = docker("run", "--rm", *common, *migration_mounts, APP_IMAGE, "migrate")
                assert "Миграции SQLite применены" in result.stdout
            print("SQLite: offline migrations applied and repeated on the shared volume")
            copied = docker(
                "run",
                "--rm",
                *common,
                *migration_mounts,
                APP_IMAGE,
                "backup",
                "--output",
                "/data/snapshot.sqlite3",
            )
            assert "создана и проверена" in copied.stdout
            restored = docker(
                "run",
                "--rm",
                *common,
                *migration_mounts,
                "--env",
                "SQLITE_PATH=/data/restored.sqlite3",
                APP_IMAGE,
                "restore",
                "--input",
                "/data/snapshot.sqlite3",
            )
            assert "восстановлена и проверена" in restored.stdout
            duplicate_backup = docker(
                "run",
                "--rm",
                *common,
                *migration_mounts,
                APP_IMAGE,
                "backup",
                "--output",
                "/data/snapshot.sqlite3",
                check=False,
            )
            assert duplicate_backup.returncode == 2
            print("SQLite CLI: backup/restore and refusal to overwrite an existing snapshot")
            telegram_check = Path(__file__).with_name("check_telegram_offline.py").resolve()
            result = docker(
                "run",
                "--rm",
                *common,
                "--volume",
                f"{volumes[0]}:/data",
                "--volume",
                f"{volumes[1]}:/work",
                "--volume",
                f"{telegram_check}:/tmp/check_telegram.py:ro",
                "--entrypoint",
                "python",
                APP_IMAGE,
                "/tmp/check_telegram.py",
            )
            print(result.stdout.strip())
            for role in ("bot", "worker"):
                role_common = common.copy()
                if role == "bot":
                    role_common[role_common.index("--memory") + 1] = "512m"
                mounts = [
                    "--volume",
                    f"{volumes[0]}:/data",
                    "--volume",
                    f"{volumes[1]}:/work",
                    "--volume",
                    f"{volumes[2]}:/telegram" + (":ro" if role == "bot" else ""),
                ]
                result = docker("run", "--rm", *role_common, *mounts, APP_IMAGE, role, "--check")
                assert f"Окружение {role} проверено" in result.stdout
                name = f"{prefix}-{role}"
                containers.append(name)
                docker("run", "--detach", "--name", name, *role_common, *mounts, APP_IMAGE, role)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    log = docker("logs", name)
                    if "event=ready phase=bootstrap" in log.stdout + log.stderr:
                        break
                    time.sleep(0.25)
                else:
                    raise RuntimeError(f"Container did not become ready: {role}")
                duplicate = docker(
                    "run",
                    "--rm",
                    *role_common,
                    *mounts,
                    APP_IMAGE,
                    role,
                    check=False,
                )
                assert duplicate.returncode == 2
                assert f"Другой {role}" in duplicate.stdout + duplicate.stderr
                print(f"{role} lock: a second process cannot start on the shared volume")
                healthy = docker("exec", name, "transcriptbot", "healthcheck", "--service", role)
                assert json.loads(healthy.stdout)["healthy"]
                docker("stop", "--time", "5", name)
                state = json.loads(docker("inspect", "--format", "{{json .State}}", name).stdout)
                assert state["ExitCode"] == 0 and not state["OOMKilled"]
                log = docker("logs", name)
                assert "event=stopped" in log.stdout + log.stderr
                assert "gsk_container_test" not in log.stdout + log.stderr
                assert "12345:container_test" not in log.stdout + log.stderr
                print(f"{role}: environment checks and SIGTERM passed")
            # Verify that the shared volumes retained their ownership across processes.
            docker("run", "--rm", *common, *mounts, APP_IMAGE, "worker", "--check")
            database_check = (
                "import sqlite3; c = sqlite3.connect('/data/transcriptbot.sqlite3'); "
                "assert c.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'; "
                "assert c.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'; "
                "assert c.execute('SELECT COUNT(*) FROM schema_migrations').fetchone()[0] == 5"
            )
            docker(
                "run",
                "--rm",
                *common,
                *mounts,
                "--entrypoint",
                "python",
                APP_IMAGE,
                "-c",
                database_check,
            )
            print("Shared volumes: worker can reopen bot's data directories")
            download_check = Path(__file__).with_name("check_downloaders_offline.py").resolve()
            result = docker(
                "run",
                "--rm",
                *common,
                *mounts,
                "--volume",
                f"{download_check}:/tmp/check_downloaders.py:ro",
                "--entrypoint",
                "python",
                APP_IMAGE,
                "/tmp/check_downloaders.py",
            )
            print(result.stdout.strip())
            operations_check = Path(__file__).with_name("check_operations_offline.py").resolve()
            result = docker(
                "run",
                "--rm",
                *common,
                *mounts,
                "--volume",
                f"{operations_check}:/tmp/check_operations.py:ro",
                "--entrypoint",
                "python",
                APP_IMAGE,
                "/tmp/check_operations.py",
            )
            print(result.stdout.strip())
            media_check = Path(__file__).with_name("check_media_offline.py").resolve()
            result = docker(
                "run",
                "--rm",
                *common,
                *mounts,
                "--volume",
                f"{media_check}:/tmp/check_media.py:ro",
                "--entrypoint",
                "python",
                APP_IMAGE,
                "/tmp/check_media.py",
            )
            print(result.stdout.strip())
        finally:
            for name in containers:
                docker("rm", "--force", name, check=False)
            for volume in reversed(volumes):
                docker("volume", "rm", volume, check=False)


if __name__ == "__main__":
    main()
