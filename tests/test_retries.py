import math

import httpx
import pytest

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.network import check_status, public_address
from transcriptbot.retries import retry_after


@pytest.mark.parametrize("value", ["1e308", "nan", "inf", "1" * 10000, "invalid"])
def test_untrusted_retry_header_cannot_overflow_ui_or_queue(value):
    assert retry_after(value, 1_000_000) is None
    with pytest.raises(DownloadError) as caught:
        check_status(httpx.Response(429, headers={"retry-after": value}))
    assert caught.value.retry_after == 60
    assert math.isfinite(caught.value.retry_after)


@pytest.mark.parametrize("address", ["64:ff9b:1::7f00:1", "64:ff9b::a9fe:a9fe"])
def test_nat64_private_targets_are_rejected(address):
    assert not public_address(address)
