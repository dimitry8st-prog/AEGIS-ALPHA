"""Map Finam live failure codes to a safe stage label for logs and clients."""

from __future__ import annotations

AUTHORIZATION = "authorization"
CONNECTION = "connection"
SUBSCRIPTION = "subscription"
DATA = "data"

_STAGE_BY_CODE = {
    "FINAM_NOT_CONFIGURED": AUTHORIZATION,
    "FINAM_AUTHORIZATION_FAILED": AUTHORIZATION,
    "FINAM_INVALID_AUTH_RESPONSE": AUTHORIZATION,
    "FINAM_STREAM_UNAVAILABLE": CONNECTION,
    "FINAM_NETWORK_ERROR": CONNECTION,
    "FINAM_SOURCE_TRANSIENT": CONNECTION,
    "FINAM_RATE_LIMITED": CONNECTION,
    "FINAM_STREAM_REJECTED": SUBSCRIPTION,
    "FINAM_STREAM_INVALID_DATA": DATA,
    "FINAM_INVALID_M1_BAR": DATA,
    "FINAM_INVALID_RESPONSE": DATA,
}


def live_failure_stage(code: str | None) -> str:
    if not code:
        return CONNECTION
    return _STAGE_BY_CODE.get(code, CONNECTION)
