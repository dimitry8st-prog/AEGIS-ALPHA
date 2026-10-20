"""Collect offline bars and read them back through the protected API."""

from __future__ import annotations

from datetime import date
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text

from app.core.security import create_access_token
from app.main import app
from worker.adapters.offline import OfflineAdapter
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository
from worker.config import WorkerSettings
from worker.types import AdjustmentMode, RunStatus


@pytest.fixture
def settings(database_url):
    return WorkerSettings(
        DATABASE_URL=database_url,
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        RUN_DEADLINE_SECONDS=60,
    )


async def _cleanup(engine):
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM market_quote_revisions"))
        await conn.execute(text("DELETE FROM market_quote_quarantine"))
        await conn.execute(text("DELETE FROM market_quotes"))
        await conn.execute(text("DELETE FROM market_collection_runs"))


async def _count(engine, table: str) -> int:
    async with engine.connect() as conn:
        return int((await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))).scalar())


async def _reader(engine) -> tuple[str, str]:
    user_id = uuid4()
    email = f"reader-{user_id}@example.com"
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO users (id, email, password_hash, role, is_active)
                VALUES (:id, :email, 'not-a-login', 'user', true)
                """
            ),
            {"id": user_id, "email": email},
        )
    token = create_access_token({"sub": str(user_id), "email": email, "role": "user"})
    return email, token


@pytest.mark.asyncio
async def test_offline_window_is_readable_and_idempotent(migrated_engine, settings):
    await _cleanup(migrated_engine)
    email, token = await _reader(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(OfflineAdapter(), repo, settings)
    window = dict(
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
        adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
    )
    first = await pipeline.run(**window)
    second = await pipeline.run(**window)
    assert first.status is RunStatus.READY
    assert first.counters.inserted == 12
    assert second.status is RunStatus.READY
    assert second.counters.inserted == 0
    assert second.counters.duplicates == 12
    assert await _count(migrated_engine, "market_quotes") == 12

    raw = await pipeline.run(
        symbols=["AAPL"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 3),
        adjustment_mode=AdjustmentMode.RAW,
    )
    assert raw.status is RunStatus.READY
    assert raw.counters.inserted == 1

    transport = httpx.ASGITransport(app=app)
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        anonymous = await client.get(
            "/api/v1/market/quotes",
            params={
                "provider": "offline",
                "adjustment_mode": "auto_adjusted",
                "start": "2024-01-02",
                "end": "2024-01-06",
            },
        )
        quotes = await client.get(
            "/api/v1/market/quotes",
            params={
                "provider": "offline",
                "adjustment_mode": "auto_adjusted",
                "start": "2024-01-02",
                "end": "2024-01-06",
                "limit": 1,
                "offset": 0,
            },
            headers=headers,
        )
        next_page = await client.get(
            "/api/v1/market/quotes",
            params={
                "provider": "offline",
                "adjustment_mode": "auto_adjusted",
                "symbol": "AAPL",
                "start": "2024-01-02",
                "end": "2024-01-06",
                "limit": 2,
                "offset": 2,
            },
            headers=headers,
        )
        raw_quotes = await client.get(
            "/api/v1/market/quotes",
            params={
                "provider": "offline",
                "adjustment_mode": "raw",
                "symbol": "AAPL",
                "start": "2024-01-02",
                "end": "2024-01-06",
            },
            headers=headers,
        )
        yfinance_quotes = await client.get(
            "/api/v1/market/quotes",
            params={
                "provider": "yfinance",
                "adjustment_mode": "auto_adjusted",
                "start": "2024-01-02",
                "end": "2024-01-06",
            },
            headers=headers,
        )
        runs = await client.get(
            "/api/v1/market/runs",
            params={"provider": "offline", "status": "READY", "limit": 10},
            headers=headers,
        )
        missing = await client.get(
            f"/api/v1/market/runs/{uuid4()}",
            headers=headers,
        )
        detail = await client.get(
            f"/api/v1/market/runs/{first.run_id}",
            headers=headers,
        )

    assert anonymous.status_code in {401, 403}
    body = quotes.json()
    assert quotes.status_code == 200
    assert body["total"] == 12
    assert body["timezone"] == "America/New_York"
    assert len(body["items"]) == 1
    assert body["items"][0]["provider"] == "offline"
    assert body["items"][0]["adjustment_mode"] == "auto_adjusted"
    assert body["items"][0]["quote_time"].startswith("2024-01-02")

    page = next_page.json()
    assert page["total"] == 4
    assert len(page["items"]) == 2
    assert {row["symbol"] for row in page["items"]} == {"AAPL"}

    raw_body = raw_quotes.json()
    assert raw_body["total"] == 1
    assert raw_body["items"][0]["adjustment_mode"] == "raw"
    assert yfinance_quotes.json()["total"] == 0

    listed = runs.json()
    assert listed["total"] >= 2
    assert {row["status"] for row in listed["items"]} == {"READY"}
    assert missing.status_code == 404
    report = detail.json()
    assert detail.status_code == 200
    assert report["id"] == str(first.run_id)
    assert report["inserted_count"] == 12
    assert report["status"] == "READY"
    assert email
    await _cleanup(migrated_engine)
    async with migrated_engine.begin() as conn:
        await conn.execute(text("DELETE FROM users WHERE email = :email"), {"email": email})
