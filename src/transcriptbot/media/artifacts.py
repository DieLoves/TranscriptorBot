"""Immutable file checkpoints and transcript assembly."""

import hashlib
import json
import math
import os
from pathlib import Path

from transcriptbot.downloaders.files import fsync_directory, publish_json, regular_path
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.planning import RATE

RESPONSE_LIMIT = 8_000_000
RESULT_LIMIT = 16_000_000
# Whisper accepts 224 prompt tokens. Byte-level BPE tokens cover at least one byte
# each, so this bound holds for any script without a tokenizer.
PROMPT_BYTES = 200


def validate_depth(value: object) -> None:
    # The C JSON decoder can accept deep structures without Python recursion.
    # Bound nesting explicitly, using iterator stacks rather than recursive walks.
    stack = [iter((value,))]
    sentinel = object()
    while stack:
        item = next(stack[-1], sentinel)
        if item is sentinel:
            stack.pop()
            continue
        if isinstance(item, (dict, list)):
            if len(stack) > 100:
                raise ValueError("JSON nesting limit")
            stack.append(iter(item.values() if isinstance(item, dict) else item))


def read_json(path: Path, *, maximum: int = RESPONSE_LIMIT) -> dict:
    regular_path(path, path.parent)
    if path.stat().st_size > maximum:
        raise PipelineError("checkpoint_invalid")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError
        validate_depth(value)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise PipelineError("checkpoint_invalid") from None


def validate_response(value: dict) -> None:
    try:
        if not isinstance(value, dict) or not isinstance(value["text"], str):
            raise ValueError
        if value.get("segments") is not None:
            if not isinstance(value["segments"], list):
                raise ValueError
            for segment in value["segments"]:
                if not isinstance(segment, dict) or not isinstance(segment.get("text"), str):
                    raise ValueError
                start, end = segment["start"], segment["end"]
                if (
                    type(start) not in (float, int)
                    or type(end) not in (float, int)
                    or not math.isfinite(start)
                    or not math.isfinite(end)
                    or not 0 <= start <= end
                ):
                    raise ValueError
                if "seek" in segment and type(segment["seek"]) not in (float, int):
                    raise ValueError
        validate_depth(value)
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (KeyError, ValueError, TypeError, OverflowError, RecursionError):
        raise PipelineError("groq_invalid_response", retryable=True, uncertain=True) from None


def context_prompt(text: str) -> str | None:
    """End of the previous part's text; parts starting mid-sentence otherwise lose
    punctuation and misrecognise words. Only recognition output, never edited."""
    text = " ".join(text.split())
    encoded = text.encode("utf-8")
    if len(encoded) <= PROMPT_BYTES:
        return text or None
    # Drop a character split by the byte limit, then a partial leading word.
    tail = encoded[-PROMPT_BYTES:].decode("utf-8", errors="ignore")
    if " " in tail:
        tail = tail.split(" ", 1)[1]
    return tail or None


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def publish_checkpoint(path: Path, value: dict) -> str:
    if len(json.dumps(value, ensure_ascii=False).encode("utf-8")) > RESPONSE_LIMIT:
        raise PipelineError("checkpoint_invalid")
    if path.exists() or path.is_symlink():
        if read_json(path) != value:
            raise PipelineError("checkpoint_invalid")
    else:
        publish_json(path, value)
    return digest(path)


def assemble(chunks: list, responses: list[dict]) -> dict:
    if len(chunks) != len(responses) or not chunks:
        raise PipelineError("checkpoint_invalid")
    for response in responses:
        validate_response(response)
    if len(responses) == 1:
        return responses[0]
    result = {
        "text": " ".join(
            response["text"].strip() for response in responses if response["text"].strip()
        ),
        "duration": chunks[-1].end_sample / RATE,
        "language": responses[0].get("language"),
        "segments": [],
        "chunks": [],
    }
    for chunk, response in zip(chunks, responses, strict=True):
        offset = chunk.start_sample / RATE
        result["chunks"].append(
            {
                "number": chunk.number + 1,
                "start": offset,
                "end": chunk.end_sample / RATE,
                "response": response,
            }
        )
        for segment in response.get("segments") or []:
            shifted = dict(segment)
            shifted.update(
                id=len(result["segments"]),
                start=round(offset + segment["start"], 6),
                end=round(offset + segment["end"], 6),
            )
            if "seek" in shifted:
                shifted["seek"] += round(offset * 100)
            result["segments"].append(shifted)
    return result


def publish_bytes(path: Path, content: bytes) -> None:
    if len(content) > RESULT_LIMIT:
        raise PipelineError("result_size_limit")
    if path.exists() or path.is_symlink():
        regular_path(path, path.parent)
        if path.stat().st_size > RESULT_LIMIT or path.read_bytes() != content:
            raise PipelineError("checkpoint_invalid")
        return
    temporary = path.with_name(path.name + ".part")
    temporary.unlink(missing_ok=True)
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path, follow_symlinks=False)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
