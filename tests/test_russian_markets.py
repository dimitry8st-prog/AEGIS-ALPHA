"""Russian sources join the existing pipeline/API without US time assumptions."""

import json
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
from sqlalchemy import text

from app.core.dependencies import get_current_active_user
from app.core.security import create_access_token
from app.main import app
from app.models.user import User
from worker.adapters.finam_adapter import FinamAdapter
from worker.adapters.moex_adapter import MoexAdapter
from worker.config import WorkerSettings
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository
from worker.types import AdjustmentMode, QuoteBar, RunStatus


async def _clean(engine):
    async with engine.begin() as connection:
        for table in ("market_quote_revisions", "market_quote_quarantine", "market_quotes", "market_collection_runs"):
            await connection.execute(text(f"DELETE FROM {table}"))


async def _token(engine):
    user_id = uuid4()
    email = f"russian-{user_id}@example.com"
    async with engine.begin() as connection:
        await connection.execute(
            text("INSERT INTO users (id,email,password_hash,role,is_active) VALUES (:id,:email,'not-a-login','user',true)"),
            {"id": user_id, "email": email},
        )
    return create_access_token({"sub": str(user_id), "email": email, "role": "user"})


@pytest.mark.asyncio
async def test_research_portfolio_requires_auth_and_has_no_invented_positions():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        denied = await client.get("/api/v1/market/portfolios/russian-research")
        assert denied.status_code in {401, 403}

        async def reader():
            return User(email="research@example.com")

        app.dependency_overrides[get_current_active_user] = reader
        try:
            response = await client.get("/api/v1/market/portfolios/russian-research")
        finally:
            app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert {row["symbol"] for row in body["instruments"]} == {"GAZP", "GMKN"}
    assert body["positions_configured"] is False
    assert body["broker"]["access"] == "read_only"
    assert body["base_currency"] == "RUB"
    assert body["broker_connection"]["status"] in {"NOT_CONFIGURED", "CONFIGURED_NOT_VERIFIED"}


@pytest.mark.asyncio
async def test_moex_collect_repeat_quarantine_and_protected_readback(migrated_engine, database_url):
    await _clean(migrated_engine)
    columns = ["open", "close", "high", "low", "volume", "begin", "end"]

    def source(request):
        rows = [] if request.url.params["start"] != "0" else [
            [100, 101, 102, 99, 10, "2024-01-09 00:00:00", "2024-01-09 23:59:59"],
            [100, 101, 102, 99, 10, "2024-01-01 00:00:00", "2024-01-01 23:59:59"],
        ]
        return httpx.Response(200, json={"candles": {"columns": columns, "data": rows}})

    settings = WorkerSettings(DATABASE_URL=database_url, IO_RETRY_WAIT_SECONDS=0.01)
    async with httpx.AsyncClient(transport=httpx.MockTransport(source)) as source_client:
        pipeline = MarketDataPipeline(MoexAdapter(client=source_client), QuoteRepository(migrated_engine), settings)
        options = dict(symbols=["GAZP", "GMKN"], start=date(2024, 1, 9), end=date(2024, 1, 10), adjustment_mode=AdjustmentMode.RAW)
        first = await pipeline.run(**options)
        second = await pipeline.run(**options)
    assert first.status is RunStatus.PARTIAL
    assert first.counters.received == 4
    assert first.counters.inserted == 2
    assert first.counters.quarantined == 2
    assert second.counters.inserted == 0 and second.counters.duplicates == 2

    token = await _token(migrated_engine)
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/v1/market/quotes", params={"provider": "moex", "adjustment_mode": "raw", "start": "2024-01-09", "end": "2024-01-10"}, headers=headers)
        report = await client.get(f"/api/v1/market/runs/{first.run_id}", headers=headers)
        filtered = await client.get("/api/v1/market/runs", params={"provider": "moex"}, headers=headers)
    assert response.status_code == 200
    quotes = response.json()
    assert quotes["timezone"] == "Europe/Moscow" and quotes["total"] == 2
    assert {item["currency"] for item in quotes["items"]} == {"RUB"}
    assert {item["exchange"] for item in quotes["items"]} == {"MISX"}
    assert report.json()["params"]["session_timezone"] == "Europe/Moscow"
    assert "not actual exchange close" in report.json()["params"]["quote_time_rule"]
    assert filtered.status_code == 200 and filtered.json()["total"] == 2


@pytest.mark.asyncio
async def test_finam_missing_credentials_is_a_failed_report_without_http(migrated_engine, database_url):
    await _clean(migrated_engine)
    def no_request(request):
        raise AssertionError("Missing credentials must not issue a request")
    async with httpx.AsyncClient(transport=httpx.MockTransport(no_request)) as client:
        pipeline = MarketDataPipeline(FinamAdapter(client=client), QuoteRepository(migrated_engine), WorkerSettings(DATABASE_URL=database_url))
        report = await pipeline.run(symbols=["GAZP", "GMKN"], start=date(2026, 10, 12), end=date(2026, 10, 17), adjustment_mode=AdjustmentMode.RAW)
    assert report.status is RunStatus.FAILED
    assert {error["code"] for error in report.errors} == {"FINAM_NOT_CONFIGURED"}
    assert report.counters.inserted == 0


@pytest.mark.asyncio
async def test_finam_readback_filters_by_moscow_date(migrated_engine):
    await _clean(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    run_id = await repo.create_run(provider="finam", symbols=["GAZP"], interval="1d", adjustment_mode="raw", window_start=date(2024, 1, 10), window_end=date(2024, 1, 11), params={})
    # Regression boundary: this instant belongs to the preceding New York date.
    bar = QuoteBar(provider="finam", symbol="GAZP", exchange="MISX", interval="1d", quote_time=datetime(2024, 1, 10, 0, 30, tzinfo=ZoneInfo("Europe/Moscow")), open=Decimal(100), high=Decimal(102), low=Decimal(99), close=Decimal(101), volume=10, adjustment_mode=AdjustmentMode.RAW, session_timezone="Europe/Moscow", currency="RUB")
    await repo.upsert_bar(bar, run_id)
    token = await _token(migrated_engine)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/v1/market/quotes", params={"provider": "finam", "adjustment_mode": "raw", "start": "2024-01-10", "end": "2024-01-11"}, headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert response.json()["timezone"] == "Europe/Moscow"
