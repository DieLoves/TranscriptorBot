"""Contiguous chunk boundaries on the decoded 16 kHz sample timeline."""

import math
import re
import statistics
import sys
from array import array
from collections.abc import Sequence
from dataclasses import dataclass

RATE = 16_000
FRAME = RATE // 100  # 10 ms energy frames


@dataclass(frozen=True)
class ChunkPlan:
    start_sample: int
    end_sample: int

    @property
    def seconds(self) -> float:
        return (self.end_sample - self.start_sample) / RATE


class Envelope:
    """Mean power of consecutive 10 ms frames, fed from the PCM analysis already reads.

    The silence threshold is absolute: moderate background noise hides every pause.
    The envelope lets planning fall back to the quietest part instead of a hard cut.
    """

    def __init__(self) -> None:
        self.power = array("d")
        self._rest = b""

    def feed(self, block: bytes) -> None:
        data = self._rest + block
        usable = len(data) - len(data) % (FRAME * 2)
        samples = array("h")
        samples.frombytes(data[:usable])
        if sys.byteorder == "big":
            samples.byteswap()  # FFmpeg writes s16le.
        for start in range(0, len(samples), FRAME):
            frame = samples[start : start + FRAME]
            self.power.append(math.sumprod(frame, frame) / FRAME)
        self._rest = data[usable:]


def quietest(power: Sequence[float], left: int, right: int, minimum: int) -> int | None:
    """Centre of the quietest run of at least `minimum` samples within [left, right)."""
    first, last = -(-left // FRAME), min(right // FRAME, len(power))
    run = -(-minimum // FRAME)
    if last - first < run:
        return None
    sums = [math.fsum(power[first : first + run])]
    for start in range(first + 1, last - run + 1):
        sums.append(sums[-1] + power[start + run - 1] - power[start - 1])
    # Ties prefer the run nearer the target boundary.
    best = min(range(len(sums)), key=lambda index: (sums[index], -index))
    # A flat window (tone, uniform noise) has no quieter part: keep the exact boundary.
    if sums[best] > statistics.median(sums) / 2:
        return None
    return (first + best) * FRAME + run * FRAME // 2


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
    power: Sequence[float] | None = None,
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
            elif power:
                quiet = quietest(power, end - window, end, minimum)
                end = end if quiet is None else quiet
        # Groq accepts >=10 ms. Keep a smaller tail in the preceding part;
        # at most 159 extra samples fit within the configured WAV header reserve.
        if 0 < total - end < RATE // 100:
            end = total
        chunks.append(ChunkPlan(start, end))
        start = end
    return chunks
