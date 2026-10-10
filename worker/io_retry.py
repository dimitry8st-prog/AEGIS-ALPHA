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
    "too many connections",
    "deadlock",
    "could not connect",
    "server closed the connection",
    "rate limit",
    "429",
)


class PermanentIOError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def is_transient(exc: BaseException) -> bool:
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
        if time.monotonic() >= deadline_monotonic:
            raise TimeoutError("DEADLINE_EXCEEDED")
        attempt += 1
        try:
            return await asyncio.wait_for(operation(), timeout=operation_timeout)
        except PermanentIOError:
            raise
        except Exception as exc:
            last_exc = exc
            if isinstance(exc, TimeoutError) and str(exc) == "DEADLINE_EXCEEDED":
                raise
            if not is_transient(exc) and not isinstance(exc, asyncio.TimeoutError):
                raise
            remaining = deadline_monotonic - time.monotonic()
            if attempt >= max_attempts or remaining <= wait_seconds:
                raise
            await asyncio.sleep(min(wait_seconds, max(0.0, remaining)))
    assert last_exc is not None
    raise last_exc
