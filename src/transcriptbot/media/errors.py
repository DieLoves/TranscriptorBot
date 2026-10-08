"""Safe pipeline failures, without tool output or API response bodies."""


class PipelineError(Exception):
    def __init__(
        self,
        code: str,
        *,
        retryable: bool = False,
        quota: bool = False,
        retry_after: float | None = None,
        uncertain: bool = False,
    ):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.quota = quota
        self.retry_after = retry_after
        self.uncertain = uncertain
