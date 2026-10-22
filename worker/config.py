"""Worker-only settings (does not require JWT like the API Settings)."""

from __future__ import annotations

from pydantic_settings import BaseSettings
from pydantic import SecretStr


class WorkerSettings(BaseSettings):
    DATABASE_URL: str
    LOG_LEVEL: str = "INFO"

    # IO policy (BPMN teaching defaults)
    IO_MAX_ATTEMPTS: int = 3
    IO_RETRY_WAIT_SECONDS: float = 0.5  # shorter in tests; override via env
    IO_OPERATION_TIMEOUT_SECONDS: float = 20.0
    RUN_DEADLINE_SECONDS: float = 900.0

    # Reserved. `python -m worker` does not open Redis and does not wrap
    # adapters in CachedAdapter. API readiness does not speak for this cache.
    REDIS_URL: str | None = None
    MARKET_CACHE_TTL_SECONDS: int = 3600
    FINAM_API_TOKEN: SecretStr | None = None

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


def sqlalchemy_async_url(database_url: str) -> str:
    if database_url.startswith("postgresql+asyncpg://"):
        return database_url
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return database_url
