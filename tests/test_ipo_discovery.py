import io
import json
from urllib.error import HTTPError

import pytest

from worker.ipo import MOEX_IPO_URL, NASDAQ_API_URL, search_ipos


def _moex_page():
    completed = {
        "contentItemId": "incb",
        "emitterFullName": "ПАО «Инкаб Холдинг»",
        "secId": "INCB",
        "tradingStartDate": "2026-06-24",
        "placementSharePrice": "100",
    }
    page = {"props": {"pageProps": {"recursiveComponentContentProps": {
        "component": {"initData": {"planned": [], "inProgress": [], "completed": [completed]}},
    }}}}
    return ('<script id="__NEXT_DATA__" type="application/json">' + json.dumps(page) + '</script>').encode()


def _nasdaq_json():
    upcoming = {
        "dealID": "test-1", "companyName": "Example Inc.", "proposedTickerSymbol": None,
        "expectedPriceDate": "11/30/2026", "proposedSharePrice": "10.00-12.00",
    }
    priced = {
        "dealID": "ant", "companyName": "Anthropic, PBC", "proposedTickerSymbol": "ANT",
        "pricedDate": "11/19/2026", "proposedSharePrice": "83.00",
    }
    return json.dumps({"status": {"rCode": 200}, "data": {
        "upcoming": {"upcomingTable": {"rows": [upcoming]}}, "priced": {"rows": [priced]},
        "filed": {"rows": None}, "withdrawn": {"rows": []},
    }}).encode()


@pytest.mark.asyncio
async def test_ipo_calendar_provenance_and_status(monkeypatch):
    def request(req, timeout):
        if req.full_url == MOEX_IPO_URL:
            return io.BytesIO(_moex_page())
        assert req.full_url == NASDAQ_API_URL + "?date=2026-11"
        return io.BytesIO(_nasdaq_json())

    monkeypatch.setattr("worker.ipo.urlopen", request)
    result = await search_ipos("all", "2026-11", None)
    assert result["status"] == "ok"
    moex, nasdaq = result["sources"]
    assert moex["entries"][0]["date_kind"] == "exchange_trading_start"
    upcoming, priced = nasdaq["entries"]
    assert (upcoming["status"], upcoming["date_kind"], upcoming["symbol"]) == ("expected", "expected_ipo_date", None)
    assert (priced["symbol"], priced["offer_price"], priced["status"]) == ("ANT", "83.00", "priced")


@pytest.mark.asyncio
async def test_unavailable_calendar_keeps_matching_results(monkeypatch):
    def request(req, timeout):
        if req.full_url == MOEX_IPO_URL:
            raise HTTPError(req.full_url, 503, "Unreachable", {}, None)
        return io.BytesIO(_nasdaq_json())

    monkeypatch.setattr("worker.ipo.urlopen", request)
    result = await search_ipos("all", "2026-11", "anthropic")
    assert result["status"] == "partial"
    moex, nasdaq = result["sources"]
    assert moex["error"] == "HTTP 503"
    assert [entry["symbol"] for entry in nasdaq["entries"]] == ["ANT"]
