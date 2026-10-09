"""Contiguous chunk boundaries on the decoded 16 kHz sample timeline."""

import math
import re
from dataclasses import dataclass

RATE = 16_000


@dataclass(frozen=True)
class ChunkPlan:
    start_sample: int
    end_sample: int

    @property
    def seconds(self) -> float:
        return (self.end_sample - self.start_sample) / RATE


def silence_ranges(log: str, total: int) -> list[tuple[int, int]]:
    ranges = []
    start = None
    for kind, value in re.findall(
        r"^lavfi\.silence_(start|end)=([0-9.eE+-]+)",
        log.replace("\r", "\n"),
        re.MULTILINE,
    ):
        try:
            seconds = float(value)
        except ValueError:
            continue
        if not math.isfinite(seconds):
            continue
        sample = round(max(0, min(total / RATE, seconds)) * RATE)
        if kind == "start":
            start = sample
        elif start is not None:
            ranges.append((start, sample))
            start = None
    if start is not None:
        ranges.append((start, total))
    return ranges


def plan_chunks(
    total: int,
    pauses: list[tuple[int, int]],
    *,
    target: int,
    window: int,
    minimum: int,
) -> list[ChunkPlan]:
    if total <= 0 or not 0 < minimum <= window < target:
        raise ValueError("Invalid chunk parameters")
    chunks = []
    start = 0
    while start < total:
        end = min(start + target, total)
        if end < total:
            candidates = [
                (max(left, end - window), min(right, end))
                for left, right in pauses
                if min(right, end) - max(left, end - window) >= minimum
            ]
            if candidates:
                left, right = candidates[-1]
                end = (left + right) // 2
        # Groq accepts >=10 ms. Keep a smaller tail in the preceding part;
        # at most 159 extra samples fit within the configured WAV header reserve.
        if 0 < total - end < RATE // 100:
            end = total
        chunks.append(ChunkPlan(start, end))
        start = end
    return chunks
