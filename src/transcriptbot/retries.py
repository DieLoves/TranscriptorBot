"""Parse untrusted retry headers without overflowing durable deadlines."""

import math
from email.utils import parsedate_to_datetime

# Python/UI timestamps must remain representable, even for hostile HTTP headers.
MAX_DEADLINE = 253_402_300_799.0


def retry_after(value: str | None, now: float) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = parsedate_to_datetime(value).timestamp() - now
        except (ValueError, TypeError, OverflowError):
            return None
    if not math.isfinite(seconds) or not math.isfinite(now + seconds):
        return None
    return max(1, seconds) if now + max(1, seconds) <= MAX_DEADLINE else None
