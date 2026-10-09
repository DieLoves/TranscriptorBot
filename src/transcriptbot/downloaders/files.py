"""Job-local publication, source hashing and a bounded local-only FFprobe."""

import hashlib
import json
import math
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from transcriptbot.downloaders.errors import DownloadError

# A deliberately closed set. Manifest/playlists/concat demuxers are never enabled.
DEMUXERS = "aac,ac3,aiff,amr,ape,asf,au,avi,flac,matroska,webm,mov,mp3,mpeg,mpegts,ogg,wav"


def job_directory(root: Path, job_id: int) -> Path:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = root / str(job_id)
    if job_id <= 0 or directory.is_symlink():
        raise DownloadError("unsafe_path")
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.resolve().parent != root.resolve():
        raise DownloadError("unsafe_path")
    return directory


def regular_path(path: Path, root: Path) -> Path:
    if ".." in path.parts or not path.is_absolute():
        raise DownloadError("unsafe_path")
    try:
        if root.is_symlink():
            raise DownloadError("unsafe_path")
        relative = path.relative_to(root)
        current = root
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise DownloadError("unsafe_path")
        if not path.is_file() or path.resolve() == root.resolve():
            raise DownloadError("unsafe_path")
    except (ValueError, OSError, RuntimeError):
        raise DownloadError("unsafe_path") from None
    return path


def reserve_disk(directory: Path, maximum: int, reserve: int) -> None:
    if shutil.disk_usage(directory).free < maximum + 25_000_000 + 16_000_000 + reserve:
        raise DownloadError("disk_space")


def fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_json(path: Path, value: dict) -> None:
    # link() makes the immutable checkpoint visible without replacing an existing one.
    temporary = path.with_suffix(".json.part")
    temporary.unlink(missing_ok=True)
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def hash_file(path: Path, maximum: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            size += len(block)
            if size > maximum:
                raise DownloadError("size_limit")
            digest.update(block)
    if size == 0:
        raise DownloadError("invalid_media")
    return size, digest.hexdigest()


def probe(path: Path, *, timeout: float, maximum_duration: float) -> dict:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-max_alloc",
        "268435456",
        "-protocol_whitelist",
        "file",
        "-format_whitelist",
        DEMUXERS,
        "-select_streams",
        "a:0",
        "-show_entries",
        "format=duration:stream=index,codec_name,duration,sample_rate,channels",
        "-of",
        "json",
        str(path),
    ]
    # Spool output to files and monitor sizes; communicate() would buffer unbounded output.
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        try:
            process = subprocess.Popen(
                command,
                stdout=output,
                stderr=errors,
                env={"PATH": os.environ.get("PATH", ""), "LANG": "C.UTF-8"},
            )
        except OSError:
            raise DownloadError("media_tool_unavailable") from None
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None:
                if (
                    max(os.fstat(output.fileno()).st_size, os.fstat(errors.fileno()).st_size)
                    > 65536
                ):
                    raise DownloadError("invalid_media")
                if time.monotonic() >= deadline:
                    raise DownloadError("media_timeout", retryable=True)
                time.sleep(0.02)
            if process.returncode != 0 or os.fstat(output.fileno()).st_size > 65536:
                raise DownloadError("invalid_media")
            output.seek(0)
            try:
                info = json.loads(output.read(65537))
                audio = info["streams"][0]
                codec = audio["codec_name"]
                duration = float(info.get("format", {}).get("duration") or audio["duration"])
                sample_rate, channels = int(audio["sample_rate"]), int(audio["channels"])
                if (
                    codec in {"unknown", "none"}
                    or not math.isfinite(duration)
                    or duration <= 0
                    or sample_rate <= 0
                    or channels <= 0
                ):
                    raise ValueError
            except (ValueError, KeyError, IndexError, TypeError):
                raise DownloadError("invalid_media") from None
            if duration > maximum_duration:
                raise DownloadError("duration_limit")
            return {
                "duration_seconds": duration,
                "audio_stream": int(audio["index"]),
                "codec": codec,
                "sample_rate": sample_rate,
                "channels": channels,
            }
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()


def source_manifest(directory: Path, payload: dict, path: Path) -> dict:
    manifest_path = directory / "source.json"
    root = Path(payload["telegram_files_dir"]) if payload["kind"] == "telegram" else directory
    regular_path(path, root)
    size, digest = hash_file(path, payload["maximum"])
    if manifest_path.exists() or manifest_path.is_symlink():
        regular_path(manifest_path, directory)
        if manifest_path.stat().st_size > 65536:
            raise DownloadError("source_changed")
        try:
            old = json.loads(manifest_path.read_text())
            if (
                old["job_id"] != payload["job_id"]
                or old["kind"] != payload["kind"]
                or old["path"] != str(path)
                or old["size_bytes"] != size
                or old["sha256"] != digest
                or old["version"] != 1
                or not math.isfinite(old["duration_seconds"])
                or old["duration_seconds"] <= 0
                or old["audio_stream"] < 0
                or old["sample_rate"] <= 0
                or old["channels"] <= 0
                or not isinstance(old["codec"], str)
                or not old["codec"]
                or "title" not in old
            ):
                raise ValueError
            if old["duration_seconds"] > payload["duration"]:
                raise DownloadError("duration_limit")
            return old
        except (ValueError, KeyError, TypeError, RecursionError):
            raise DownloadError("source_changed") from None
    media = probe(path, timeout=payload["probe_timeout"], maximum_duration=payload["duration"])
    value = {
        "version": 1,
        "job_id": payload["job_id"],
        "kind": payload["kind"],
        "path": str(path),
        "size_bytes": size,
        "sha256": digest,
        "title": payload.get("title"),
        **media,
    }
    publish_json(manifest_path, value)
    return value
