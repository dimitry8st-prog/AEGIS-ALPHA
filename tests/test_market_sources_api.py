
import httpx
import pytest
from pydantic import SecretStr

from app.api.v1 import market
from app.core.dependencies import get_current_active_user
from app.main import app
from worker.adapters.finam_adapter import FinamAdapter

MINUTES = "/api/v1/market/finam/minutes"
IPOS = "/api/v1/market/ipos"
WINDOW = {"symbol": "GAZP", "start": "2026-11-20T07:10:00Z", "end": "2026-11-20T07:12:00Z"}


@pytest.mark.asyncio
async def test_market_sources_require_aegis_authentication():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get(IPOS)).status_code in (401, 403)
        assert (await client.get(MINUTES, params=WINDOW)).status_code in (401, 403)


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol, n, status", [("GAZP", 1, "PARTIAL"), ("GMKN", 2, "READY")])
async def test_api_reads_both_finam_symbols_and_keeps_key_private(monkeypatch, symbol, n, status):
    secret = "test-only-finam-secret"

    def broker(request):
        if request.url.path == "/v1/sessions":
            assert request.method == "POST"
            return httpx.Response(200, json={"token": "test-broker-session"})
        assert request.method == "GET"
        assert request.url.path == f"/v1/instruments/{symbol}@MISX/bars"
        assert request.url.params["timeframe"] == "TIME_FRAME_M1"
        assert request.headers["authorization"] == "Bearer test-broker-session"
        rows = [
            {
                "timestamp": f"2026-11-20T07:{minute}:00Z",
                "open": {"value": "123.56"}, "high": {"value": "124"},
                "low": {"value": "123.4"}, "close": {"value": "123.8"}, "volume": {"value": "201"},
            }
            for minute in range(10, 10 + n)
        ]
        return httpx.Response(200, json={"symbol": f"{symbol}@MISX", "bars": rows})

    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: object())
    monkeypatch.setattr(market.settings, "FINAM_API_TOKEN", SecretStr(secret))
    async with httpx.AsyncClient(transport=httpx.MockTransport(broker)) as finam_client:
        monkeypatch.setattr(market, "FinamAdapter", lambda key: FinamAdapter(key, client=finam_client))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(MINUTES, params={**WINDOW, "symbol": symbol})
    assert response.status_code == 200
    body = response.json()
    assert (body["symbol"], body["interval"], body["status"], body["expected_minutes"]) == (
        symbol, "1m", status, 2,
    )
    assert len(body["items"]) == n and body["items"][0]["open"] == "123.56"
    assert secret not in response.text


@pytest.mark.asyncio
async def test_ipo_filters_and_unconfigured_finam_are_reported(monkeypatch):
    async def search(market_name, month, query):
        assert (market_name, month, query) == ("us", "2026-11", "ANT")
        return {
            "observed_at": "2026-11-22T17:00:00Z", "status": "ok",
            "sources": [{
                "market": "us", "url": "https://www.nasdaq.com/market-activity/ipos",
                "scope": "2026-11", "status": "ok", "entries": [],
            }],
        }

    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: object())
    monkeypatch.setattr(market.settings, "FINAM_API_TOKEN", None)
    monkeypatch.setattr(market, "search_ipos", search)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        ipo = await client.get(IPOS, params={"market": "us", "month": "2026-11", "query": "ANT"})
        finam = await client.get(MINUTES, params=WINDOW)
    assert ipo.status_code == 200 and ipo.json()["sources"][0]["status"] == "ok"
    assert finam.status_code == 503
    assert finam.json()["error"]["code"] == "FINAM_NOT_CONFIGURED"
