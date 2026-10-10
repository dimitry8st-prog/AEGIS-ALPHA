from __future__ import annotations

import os

import pytest
import pytest_asyncio
from sqlalchemy import text

# Ensure worker settings can load even if .env has JWT for the API
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://aegis:aegis123@localhost:5432/aegis",
)
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")
os.environ.setdefault("IO_RETRY_WAIT_SECONDS", "0.05")


@pytest.fixture(scope="session")
def database_url() -> str:
    return os.environ["DATABASE_URL"]


@pytest_asyncio.fixture
async def engine(database_url: str):
    from worker.config import WorkerSettings
    from worker.repository import create_engine

    settings = WorkerSettings(DATABASE_URL=database_url)
    eng = create_engine(settings)
    # Verify connectivity; skip if Postgres not available
    try:
        async with eng.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        await eng.dispose()
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def migrated_engine(engine):
    """Assume migrations already applied (CI / local alembic upgrade)."""
    async with engine.connect() as conn:
        exists = await conn.execute(
            text(
                """
                SELECT to_regclass('public.market_quotes') IS NOT NULL
                """
            )
        )
        if not exists.scalar():
            pytest.skip("market_quotes missing — run alembic upgrade head")
    yield engine
