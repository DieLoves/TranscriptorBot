"""Descriptor-relative removal; never traverse symlinks or Bot API service files."""

import os
import stat
from contextlib import contextmanager
from pathlib import Path


class UnsafeCleanupPath(OSError):
    pass


@contextmanager
def parent_descriptor(path: Path, root: Path):
    if not path.is_absolute() or not root.is_absolute() or ".." in path.parts:
        raise UnsafeCleanupPath
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        raise UnsafeCleanupPath from None
    if not parts:
        raise UnsafeCleanupPath
    descriptors = []
    try:
        # Open every ancestor, including the configured root, without following links.
        descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        descriptors.append(descriptor)
        for part in (*root.parts[1:], *parts[:-1]):
            descriptor = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor
            )
            descriptors.append(descriptor)
        yield descriptor, parts[-1]
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def unlink_file(path: Path, root: Path, *, cache: bool = False) -> None:
    if cache:
        # TDLib's persistent database/log/config files are outside these media directories.
        try:
            relative = path.relative_to(root)
        except ValueError:
            raise UnsafeCleanupPath from None
        if (
            not any(
                part
                in {"music", "videos", "documents", "voice", "video_notes", "audios", "animations"}
                for part in relative.parts[:-1]
            )
            or path.name in {"db.sqlite", "td.binlog", "config", "log"}
            or path.suffix in {".sqlite", ".sqlite3", ".db", ".binlog"}
        ):
            raise UnsafeCleanupPath
    try:
        with parent_descriptor(path, root) as (descriptor, name):
            value = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISREG(value.st_mode):
                raise UnsafeCleanupPath
            os.unlink(name, dir_fd=descriptor)
            os.fsync(descriptor)
    except FileNotFoundError:
        pass


def workspace_files(directory: Path, root: Path) -> list[Path]:
    try:
        with parent_descriptor(directory, root) as (descriptor, name):
            job_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            try:
                return [directory / name for name in os.listdir(job_fd)]
            finally:
                os.close(job_fd)
    except FileNotFoundError:
        return []


def remove_directory(directory: Path, root: Path) -> None:
    try:
        with parent_descriptor(directory, root) as (descriptor, name):
            os.rmdir(name, dir_fd=descriptor)
            os.fsync(descriptor)
    except FileNotFoundError:
        pass
