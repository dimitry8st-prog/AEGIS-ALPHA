"""Limited retries for temporary IO failures (BPMN Process_IO subset)."""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")

TRANSIENT_MARKERS = (
    "timeout",
    "temporar",
    "connection refused",
    "connection reset",
    "connection aborted",
    "connection error",
    "could not connect",
    "server closed the connection",
    "too many connections",
    "deadlock",
    "rate limit",
    "too many requests",
    "429",
    "503",
    "502",
    "504",
    "temporarily unavailable",
)


class PermanentIOError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class TransientIOError(Exception):
    """Raised for retryable source/DB failures (timeouts, rate limits, etc.)."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, TransientIOError):
        return True
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError)):
        return True
    text = str(exc).lower()
    return any(m in text for m in TRANSIENT_MARKERS)


async def with_retries(
    operation: Callable[[], Awaitable[T]],
    *,
    max_attempts: int,
    wait_seconds: float,
    deadline_monotonic: float,
    operation_timeout: float,
) -> T:
    attempt = 0
    last_exc: BaseException | None = None
    while attempt < max_attempts:
        remaining = deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("DEADLINE_EXCEEDED")
        attempt += 1
        # Cap this attempt so it cannot overrun the overall run deadline.
        attempt_timeout = min(operation_timeout, remaining)
        try:
            return await asyncio.wait_for(operation(), timeout=attempt_timeout)
        except PermanentIOError:
            raise
        except Exception as exc:
            last_exc = exc
            if isinstance(exc, TimeoutError) and str(exc) == "DEADLINE_EXCEEDED":
                raise
            if not is_transient(exc):
                raise
            remaining_after = deadline_monotonic - time.monotonic()
            if attempt >= max_attempts or remaining_after <= wait_seconds:
                raise
            await asyncio.sleep(min(wait_seconds, max(0.0, remaining_after)))
    assert last_exc is not None
    raise last_exc
