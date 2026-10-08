"""Safe, typed failures shared by source adapters and the worker."""


class DownloadError(Exception):
    def __init__(self, code: str, *, retryable: bool = False, retry_after: float | None = None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after = retry_after
