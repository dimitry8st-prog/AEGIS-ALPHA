"""Five-ticker MOEX watchlist: registry, resilient updates, news and reaction timing."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from app.api.v1 import market
from app.core.dependencies import get_current_active_user
from app.main import app
from worker import finam_monitor
from worker.adapters import finam_stream
from worker.adapters.finam_adapter import FinamAdapter, MinuteHistory, RequestBudget, retry_after_seconds
from worker.event_reaction import measure_reaction
from worker.finam_live_state import ERROR, LIVE, RECOVERING, STALE, LiveMarketState
from worker.finam_news import NewsLedger, ingest_history, parse_news
from worker.io_retry import PermanentIOError, TransientIOError, with_retries
from worker.news_signals import (
    CONFIRMED, DIVIDEND_RECOMMENDATION, NO_SIGNAL, RECORD_DATE, SHAREHOLDER_DECISION, WATCH,
    event_status, event_types,
)
from worker.pipeline import MarketDataPipeline
from worker.russian_market import RU_CONTEXT_SYMBOLS, RU_WATCHLIST, finam_symbol
from worker.types import AdjustmentMode, FetchResult, FetchStatus, QuoteBar

FIVE = ["GAZP", "GMKN", "SBER", "YDEX", "X5"]
ROOT = Path(__file__).resolve().parents[1]
SECRET = "test-only-finam-secret"
JWT = "test-only-finam-session"
T0 = datetime(2026, 11, 27, 7, 0, tzinfo=timezone.utc)  # 10:00 MSK


def minute(offset: int) -> datetime:
    return T0 + timedelta(minutes=offset)


def quote_bar(symbol: str, offset: int, close: str = "100") -> QuoteBar:
    price = Decimal(close)
    return QuoteBar(
        provider="finam", symbol=symbol, exchange="MISX", interval="1m", quote_time=minute(offset),
        open=price, high=price + 1, low=price - 1, close=price, volume=10,
        adjustment_mode=AdjustmentMode.RAW, session_timezone="Europe/Moscow", currency="RUB",
    )


def bar_row(offset: int, close: str = "100") -> dict:
    price = Decimal(close)
    return {
        "quote_time": minute(offset).isoformat(), "open": str(price), "high": str(price + 1),
        "low": str(price - 1), "close": str(price), "volume": 10,
    }


# --- Registry -------------------------------------------------------------------------------


def test_registry_lists_five_tickers_and_imoex_everywhere():
    assert list(RU_WATCHLIST) == FIVE
    assert list(RU_CONTEXT_SYMBOLS) == ["IMOEX"]
    assert [finam_symbol(symbol) for symbol in FIVE] == [f"{symbol}@MISX" for symbol in FIVE]
    assert list(finam_stream.INSTRUMENTS.values()) == [*FIVE, "IMOEX"]
    script = (ROOT / "scripts/start_finam_dashboard.ps1").read_text(encoding="utf-8")
    assert "GAZP,GMKN" not in script
    page = (ROOT / "app/dashboard/index.html").read_text(encoding="utf-8")
    assert "data-symbol=" not in page and "GAZP" not in page


def test_worker_collect_defaults_to_the_registry(monkeypatch):
    from worker import __main__ as cli

    captured = {}

    class Pipeline:
        def __init__(self, adapter, repo, settings):
            pass

        async def run(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(status=SimpleNamespace(value="READY"), to_dict=lambda: {})

    class Engine:
        async def dispose(self):
            pass

    monkeypatch.setattr(cli, "MarketDataPipeline", Pipeline)
    monkeypatch.setattr(cli, "create_engine", lambda settings: Engine())
    assert cli.main(["collect", "--provider", "finam", "--start", "2026-11-20", "--end", "2026-11-27"]) == 0
    assert captured["symbols"] == FIVE


@pytest.mark.asyncio
async def test_api_exposes_registry_and_reads_minutes_for_each_ticker(monkeypatch):
    requested = []

    def broker(request):
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"token": JWT})
        instrument = request.url.path.split("/")[3]
        requested.append(instrument)
        return httpx.Response(200, json={"symbol": instrument, "bars": [{
            "timestamp": "2026-11-27T07:00:00Z", "open": {"value": "10"}, "high": {"value": "11"},
            "low": {"value": "9"}, "close": {"value": "10"}, "volume": {"value": "5"},
        }]})

    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: object())
    monkeypatch.setattr(market.settings, "FINAM_API_TOKEN", SecretStr(SECRET))
    window = {"start": "2026-11-27T07:00:00Z", "end": "2026-11-27T07:01:00Z"}
    async with httpx.AsyncClient(transport=httpx.MockTransport(broker)) as finam_client:
        monkeypatch.setattr(market, "FinamAdapter", lambda key: FinamAdapter(key, client=finam_client))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            registry = (await client.get("/api/v1/market/instruments")).json()
            minutes = [
                await client.get("/api/v1/market/finam/minutes", params={**window, "symbol": symbol})
                for symbol in [*FIVE, "IMOEX"]
            ]
            unknown = await client.get("/api/v1/market/finam/minutes", params={**window, "symbol": "AAPL"})
    assert [row["symbol"] for row in registry["watchlist"]] == FIVE
    assert registry["market_context"][0]["verification"] == "LIVE_CHECK_REQUIRED"
    assert [row["dividend_stages"] for row in registry["watchlist"]] == [False, False, False, False, True]
    assert [response.json()["status"] for response in minutes] == ["READY"] * 6
    assert requested == [f"{symbol}@MISX" for symbol in [*FIVE, "IMOEX"]]
    assert unknown.status_code == 422 and unknown.json()["error"]["code"] == "UNSUPPORTED_INSTRUMENT"


# --- One instrument failing -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_daily_collection_continues_when_one_ticker_fails():
    class Adapter:
        provider_name = "finam"

        async def fetch_daily(self, symbol, start, end, adjustment_mode):
            if symbol == "SBER":
                raise PermanentIOError("FINAM_HTTP_ERROR", "Finam request rejected (HTTP 404)")
            bar = QuoteBar(
                provider="finam", symbol=symbol, exchange="MISX", interval="1d",
                quote_time=datetime(2026, 11, 26, 23, 59, 59, tzinfo=timezone(timedelta(hours=3))),
                open=Decimal(1), high=Decimal(2), low=Decimal(1), close=Decimal(2), volume=1,
                adjustment_mode=AdjustmentMode.RAW, session_timezone="Europe/Moscow", currency="RUB",
            )
            return FetchResult(status=FetchStatus.OK, symbol=symbol, bars=[bar])

    class Repo:
        stored = []

        async def create_run(self, **kwargs):
            from uuid import uuid4
            return uuid4()

        async def upsert_bar(self, bar, run_id):
            self.stored.append(bar.symbol)
            return "inserted"

        async def finish_run(self, report):
            pass

    settings = SimpleNamespace(
        RUN_DEADLINE_SECONDS=30, IO_MAX_ATTEMPTS=1, IO_RETRY_WAIT_SECONDS=0, IO_OPERATION_TIMEOUT_SECONDS=5,
    )
    repo = Repo()
    report = await MarketDataPipeline(Adapter(), repo, settings).run(
        symbols=FIVE, start=date(2026, 11, 26), end=date(2026, 11, 27), adjustment_mode=AdjustmentMode.RAW,
    )
    assert report.status.value == "PARTIAL"
    assert repo.stored == ["GAZP", "GMKN", "YDEX", "X5"]
    assert report.per_symbol["SBER"]["status"] == "UNAVAILABLE"
    assert [error["code"] for error in report.errors] == ["FINAM_HTTP_ERROR"]


@pytest.mark.asyncio
async def test_backfill_failure_marks_only_that_ticker():
    state = LiveMarketState([*FIVE, "IMOEX"])

    class Adapter:
        async def fetch_minute_history(self, symbol, start, end):
            if symbol == "YDEX":
                raise TransientIOError("FINAM_NETWORK_ERROR", "Finam request failed or timed out")
            return MinuteHistory([quote_bar(symbol, 0)], {})

    state.mark_connected()
    results = await finam_monitor.backfill(Adapter(), state, minute(5))
    assert results["YDEX"] == {"status": "ERROR", "code": "FINAM_NETWORK_ERROR"}
    snapshot = state.snapshot(minute(5))["instruments"]
    assert snapshot["YDEX"]["status"] == ERROR
    assert {symbol: snapshot[symbol]["status"] for symbol in ("GAZP", "GMKN", "SBER", "X5", "IMOEX")} == dict.fromkeys(
        ("GAZP", "GMKN", "SBER", "X5", "IMOEX"), LIVE,
    )
    assert snapshot["X5"]["last_closed_m1"] == minute(0).isoformat()


def test_rejected_subscription_reports_only_its_ticker():
    events = finam_stream._market_events({
        "type": "DATA", "subscription_type": "BARS",
        "payload": {"symbol": "X5@MISX", "error": {"code": 5}},
    })
    assert events == [{"type": "symbol_error", "symbol": "X5", "feed": "BARS", "code": "FINAM_STREAM_REJECTED"}]
    state = LiveMarketState(FIVE)
    state.on_event(events[0], minute(0))
    state.on_event({"type": "quote", "symbol": "SBER", "last": "301.5", "quote_time": minute(0).isoformat()}, minute(0))
    snapshot = state.snapshot(minute(1))["instruments"]
    assert snapshot["X5"]["status"] == ERROR
    assert snapshot["SBER"]["last_price"] == "301.5"


# --- Stream break and recovery ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_break_keeps_last_data_and_backfills_the_gap():
    clock = {"now": minute(1)}
    sleeps = []
    windows = []

    class Adapter:
        async def open_market_session(self):
            return JWT

        async def fetch_minute_history(self, symbol, start, end):
            windows.append((symbol, start, end))
            if symbol != "GAZP":
                return MinuteHistory([], {})
            first, last = (int((stamp - T0).total_seconds() // 60) for stamp in (start, end))
            return MinuteHistory([quote_bar("GAZP", offset) for offset in range(max(first, 0), last)], {})

    connection = {"count": 0}

    async def stream(jwt):
        connection["count"] += 1
        if connection["count"] == 2:
            clock["now"] = minute(6)
        yield {"type": "connected"}
        if connection["count"] == 1:
            clock["now"] = minute(1) + timedelta(seconds=5)
            # The stream repeats the 10:00 bar that REST has already returned.
            yield {"type": "bar", "symbol": "GAZP", **bar_row(0)}
            yield {"type": "quote", "symbol": "GAZP", "last": "100.5", "quote_time": minute(1).isoformat()}
            raise TransientIOError("FINAM_STREAM_UNAVAILABLE", "lost", retry_after=7)
        yield {"type": "bar", "symbol": "GAZP", **bar_row(5)}

    state = LiveMarketState(FIVE)

    async def sleep(seconds):
        sleeps.append(seconds)
        snapshot = state.snapshot(clock["now"])["instruments"]["GAZP"]
        # During the outage the last data stays with its own timestamps.
        assert snapshot["status"] == STALE
        assert snapshot["last_price"] == "100.5"
        assert snapshot["last_quote_time"] == minute(1).isoformat()
        assert snapshot["last_closed_m1"] == minute(0).isoformat()

    await finam_monitor.run_stream(
        Adapter(), state, stream=stream, clock=lambda: clock["now"], sleep=sleep, max_connections=2,
    )
    assert sleeps == [7]  # Retry-After is longer than the 1 s backoff.
    gazp_windows = [(start, end) for symbol, start, end in windows if symbol == "GAZP"]
    # The reconnect asks only for minutes after the last closed bar.
    assert gazp_windows[-1] == (minute(1), minute(6))
    snapshot = state.snapshot(minute(7))["instruments"]["GAZP"]
    assert snapshot["status"] == STALE  # The second connection ended as well.
    assert snapshot["bars"] == 6 and snapshot["backfilled"] == 6
    assert snapshot["duplicates"] == 2  # The 10:00 and 10:05 bars arrived twice.
    assert state.disconnects == 2


def test_reconnect_moves_instruments_to_recovering_until_backfilled():
    state = LiveMarketState(FIVE)
    state.mark_connected()
    assert {row["status"] for row in state.snapshot(minute(0))["instruments"].values()} == {RECOVERING}
    state.mark_recovered("GAZP")
    assert state.snapshot(minute(0))["instruments"]["GAZP"]["status"] == LIVE


def test_duplicate_and_updated_candles_are_merged_once():
    state = LiveMarketState(["GAZP"])
    assert state.merge_bars("GAZP", [bar_row(0), bar_row(0)], minute(1)) == {
        "inserted": 1, "duplicates": 1, "replaced": 0,
    }
    # The stream updates the bar in progress; the final values replace it.
    assert state.merge_bars("GAZP", [bar_row(1, "100"), bar_row(1, "101")], minute(2)) == {
        "inserted": 1, "duplicates": 0, "replaced": 1,
    }
    assert state.merge_bars("GAZP", [quote_bar("GAZP", 0)], minute(3), source="rest")["duplicates"] == 1
    closed = state.closed_bars("GAZP", minute(1) + timedelta(seconds=30))
    assert [row["quote_time"] for row in closed] == [minute(0)]  # The 10:01 bar is still open.
    assert state.closed_bars("GAZP", minute(2))[-1]["close"] == Decimal("101")


# --- Limits and Retry-After -------------------------------------------------------------------


def test_retry_after_accepts_seconds_and_http_dates():
    now = datetime(2026, 11, 27, 7, 0, tzinfo=timezone.utc)
    assert retry_after_seconds("12") == 12
    assert retry_after_seconds("Fri, 27 Nov 2026 07:00:30 GMT", now) == 30
    assert retry_after_seconds("99999") == 120
    assert retry_after_seconds("soon") is None and retry_after_seconds(None) is None


@pytest.mark.asyncio
async def test_rate_limit_respects_retry_after_and_budget():
    clock = {"now": 0.0}
    waits = []

    async def sleep(seconds):
        waits.append(seconds)
        clock["now"] += seconds

    budget = RequestBudget(per_minute=2, clock=lambda: clock["now"], sleep=sleep)
    await budget.acquire()
    await budget.acquire()
    await budget.acquire()
    assert waits == [60]

    def broker(request):
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"token": JWT})
        return httpx.Response(429, headers={"Retry-After": "9"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(broker)) as client:
        adapter = FinamAdapter(SECRET, client=client, budget=budget)
        with pytest.raises(TransientIOError) as raised:
            await adapter.fetch_news_sources()
    assert (raised.value.code, raised.value.retry_after) == ("FINAM_RATE_LIMITED", 9)
    waits.clear()
    await budget.acquire()
    assert waits and waits[0] == pytest.approx(9)


@pytest.mark.asyncio
async def test_with_retries_waits_at_least_retry_after(monkeypatch):
    import asyncio
    import time

    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    calls = {"n": 0}

    async def operation():
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientIOError("FINAM_RATE_LIMITED", "limited", retry_after=3)
        return "ok"

    result = await with_retries(
        operation, max_attempts=2, wait_seconds=0.05,
        deadline_monotonic=time.monotonic() + 30, operation_timeout=5,
    )
    assert result == "ok" and slept == [3]


# --- News: pagination, duplicates, corrections -----------------------------------------------


def news_row(news_id="n1", version="1", title="Сбербанк отчитался", symbols=("SBER@MISX",), **extra):
    return {
        "id": news_id, "version": version, "source": "finam", "title": title,
        "published_at": "2026-11-27T07:00:00Z", "received_at": "2026-11-27T07:00:02Z",
        "symbols": list(symbols), "url": "https://www.finam.ru/news/1", **extra,
    }


@pytest.mark.asyncio
async def test_news_history_follows_cursor_with_five_ticker_filter():
    pages = {"": ("c2", [news_row("n1"), news_row("n2")]), "c2": ("", [news_row("n2"), news_row("n3")])}
    seen = []

    def broker(request):
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"token": JWT})
        assert request.url.path == "/v1/news/history"
        params = request.url.params
        seen.append(params["cursor"])
        assert params.get_list("filter.symbols") == [f"{symbol}@MISX" for symbol in FIVE]
        assert params["time_range.from"] == "2026-11-26T07:00:00Z"
        assert params["page_size"] == "100"
        cursor, items = pages[params["cursor"]]
        return httpx.Response(200, json={"items": items, "next_cursor": cursor})

    async with httpx.AsyncClient(transport=httpx.MockTransport(broker)) as client:
        page = await FinamAdapter(SECRET, client=client).fetch_news_history(FIVE, minute(-1440), T0)
    assert seen == ["", "c2"] and page.pages == 2 and not page.truncated
    ledger = NewsLedger()
    counts = ingest_history(ledger, page.items, minute(1))
    assert counts == {"new": 3, "duplicates": 1, "corrections": 0, "rejected": 0, "received": 4}


@pytest.mark.asyncio
async def test_repeated_news_cursor_is_rejected():
    def broker(request):
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"token": JWT})
        return httpx.Response(200, json={"items": [], "next_cursor": "same"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(broker)) as client:
        with pytest.raises(PermanentIOError):
            await FinamAdapter(SECRET, client=client).fetch_news_history(FIVE, minute(-60), T0)


def test_corrections_and_deletions_follow_what_was_known_at_the_time():
    ledger = NewsLedger()
    ledger.add([parse_news(news_row(version="1", title="Первая версия"), minute(1))])
    counts = ledger.add([
        parse_news(news_row(version="1", title="Первая версия"), minute(5)),
        parse_news(news_row(version="2", title="Исправлено", updated_at="2026-11-27T07:03:00Z"), minute(4)),
    ])
    assert counts == {"new": 0, "duplicates": 1, "corrections": 1}
    assert ledger.items[("finam", "n1", "1")].observed_at == minute(1)  # First observation kept.
    assert [item.title for item in ledger.known_at(minute(2))] == ["Первая версия"]
    assert [item.title for item in ledger.known_at(minute(4))] == ["Исправлено"]
    ledger.add([parse_news(news_row(version="3", deleted=True, updated_at="2026-11-27T07:06:00Z"), minute(6))])
    assert ledger.known_at(minute(6))[0].deleted is True
    assert event_status(ledger, "SBER", "COMPANY_NEWS", minute(6), frozenset())["status"] == NO_SIGNAL


@pytest.mark.asyncio
async def test_news_storage_is_idempotent_and_keeps_first_observation(migrated_engine):
    from sqlalchemy import text

    from worker.news_repository import NewsRepository

    async with migrated_engine.begin() as conn:
        await conn.execute(text("DELETE FROM market_news WHERE source = 'test-finam'"))
    repository = NewsRepository(migrated_engine)
    first = parse_news(news_row(source="test-finam"), minute(1))
    again = parse_news(news_row(source="test-finam"), minute(9))
    correction = parse_news(news_row(source="test-finam", version="2"), minute(9))
    assert await repository.store([first]) == {"inserted": 1, "already_stored": 0}
    assert await repository.store([again, correction]) == {"inserted": 1, "already_stored": 1}
    stored = await repository.first_observed([again, correction])
    assert stored[again.key] == minute(1) and stored[correction.key] == minute(9)
    async with migrated_engine.begin() as conn:
        await conn.execute(text("DELETE FROM market_news WHERE source = 'test-finam'"))


@pytest.mark.asyncio
async def test_news_feed_status_is_independent_of_quotes(monkeypatch):
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: object())
    monkeypatch.setattr(market.settings, "FINAM_API_TOKEN", None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/v1/market/finam/news")
    assert response.status_code == 200
    assert response.json()["feed"]["status"] == "NOT_CONFIGURED"

    def broker(request):
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"token": JWT})
        return httpx.Response(403)

    monkeypatch.setattr(market.settings, "FINAM_API_TOKEN", SecretStr(SECRET))
    async with httpx.AsyncClient(transport=httpx.MockTransport(broker)) as finam_client:
        monkeypatch.setattr(market, "FinamAdapter", lambda key: FinamAdapter(key, client=finam_client))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            denied = await client.get("/api/v1/market/finam/news")
    assert denied.json()["feed"] == {
        "status": "NO_ACCESS", "code": "FINAM_AUTHORIZATION_FAILED", "checked_at": denied.json()["feed"]["checked_at"],
    }
    assert SECRET not in denied.text


# --- WATCH / CONFIRMED and X5 dividend stages -----------------------------------------------


def test_public_message_watches_and_official_disclosure_confirms():
    ledger = NewsLedger()
    ledger.add([
        parse_news(news_row("p1", title="СМИ: совет директоров X5 рекомендовал дивиденды", symbols=("X5@MISX",)), minute(1)),
        parse_news({**news_row("o1", title="X5: совет директоров рекомендовал дивиденды", symbols=("X5@MISX",)),
                    "source": "disclosure", "published_at": "2026-11-27T07:10:00Z"}, minute(11)),
    ])
    official = frozenset({"disclosure"})
    assert event_status(ledger, "X5", DIVIDEND_RECOMMENDATION, minute(0), official)["status"] == NO_SIGNAL
    assert event_status(ledger, "X5", DIVIDEND_RECOMMENDATION, minute(5), official)["status"] == WATCH
    confirmed = event_status(ledger, "X5", DIVIDEND_RECOMMENDATION, minute(11), official)
    assert [row["status"] for row in confirmed["transitions"]] == [WATCH, CONFIRMED]
    assert confirmed["entry_candidate"] is None
    # The recommendation does not confirm the shareholder decision or the record date.
    assert event_status(ledger, "X5", SHAREHOLDER_DECISION, minute(11), official)["status"] == NO_SIGNAL
    assert event_status(ledger, "X5", RECORD_DATE, minute(11), official)["status"] == NO_SIGNAL


@pytest.mark.parametrize(("title", "expected"), [
    ("Совет директоров X5 рекомендовал дивиденды 648 руб.", [DIVIDEND_RECOMMENDATION]),
    ("Акционеры X5 утвердили дивиденды на ГОСА", [SHAREHOLDER_DECISION]),
    ("X5: дата закрытия реестра под дивиденды — 18 июля", [RECORD_DATE]),
    ("ГОСА X5 утвердило дивиденды, отсечка 18 июля", [SHAREHOLDER_DECISION, RECORD_DATE]),
    ("X5 открыла 300 магазинов", ["COMPANY_NEWS"]),
])
def test_x5_dividend_stages_are_distinct(title, expected):
    assert event_types(parse_news(news_row(title=title, symbols=("X5@MISX",)), minute(1))) == expected


def test_future_publication_produces_no_signal():
    ledger = NewsLedger()
    # The provider clock says 07:30; AEGIS cannot know it at 07:20 even if a reader saw it earlier.
    ledger.add([parse_news({**news_row(), "published_at": "2026-11-27T07:30:00Z"}, minute(20))])
    assert event_status(ledger, "SBER", "COMPANY_NEWS", minute(25), frozenset())["status"] == NO_SIGNAL
    assert event_status(ledger, "SBER", "COMPANY_NEWS", minute(30), frozenset())["status"] == WATCH


# --- Price reaction on closed M1 after availability -------------------------------------------


def event_at(available: datetime) -> dict:
    return {
        "published_at": minute(0).isoformat(), "provider_received_at": (minute(0) + timedelta(seconds=2)).isoformat(),
        "available_at": available.isoformat(),
    }


def test_reaction_uses_only_closed_bars_after_availability():
    available = minute(2) + timedelta(seconds=20)
    bars = [bar_row(offset, str(100 + offset)) for offset in range(0, 9)]
    report = measure_reaction(event_at(available), bars, now=minute(8) + timedelta(seconds=30), window_minutes=5)
    assert report["baseline"] == {"bar_open": minute(1).isoformat(), "close": "101"}
    assert report["reaction"]["first_bar_open"] == minute(3).isoformat()
    assert report["reaction"]["seconds_from_availability_to_first_close"] == 100
    assert report["reaction"]["last_bar_open"] == minute(7).isoformat()  # 10:08 is still open.
    assert report["status"] == "MEASURED"
    assert report["latency_seconds"] == {"publication_to_provider": 2, "publication_to_aegis": 140}

    # Rewriting the minute that was still open at availability changes nothing.
    tampered = [row if row["quote_time"] != minute(2).isoformat() else bar_row(2, "999") for row in bars]
    assert measure_reaction(event_at(available), tampered, now=minute(8) + timedelta(seconds=30)) == report
    early = measure_reaction(event_at(available), bars, now=minute(3) + timedelta(seconds=59))
    assert early["status"] == "WAITING_FOR_BARS" and early["reaction"] is None


def test_index_comparison_uses_only_common_minutes():
    available = minute(2)
    stock = [bar_row(offset, str(100 + offset)) for offset in range(0, 6)]
    index = [bar_row(offset, "3000") for offset in (1, 2, 4)]  # Minute 3 and 5 are missing.
    report = measure_reaction(event_at(available), stock, now=minute(7), window_minutes=4, index_bars=index)
    assert report["index"] == {
        "status": "MEASURED", "common_minutes": 2, "last_bar_open": minute(4).isoformat(),
        "stock_change_pct": round((104 - 101) / 101 * 100, 4), "index_change_pct": 0.0,
        "excess_change_pct": round((104 - 101) / 101 * 100, 4),
    }
    missing_base = measure_reaction(event_at(available), stock, now=minute(7), index_bars=[bar_row(4, "3000")])
    assert missing_base["index"] == {"status": "NO_COMMON_INTERVAL"}


def test_event_cannot_be_available_before_publication():
    with pytest.raises(ValueError):
        measure_reaction(event_at(minute(-1)), [], now=minute(5))


# --- Live check script and monitor CLI (no network) -----------------------------------------


def _load_watch_check():
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("finam_watch_check", ROOT / "scripts/finam_watch_check.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_live_check_without_token_reports_not_configured(monkeypatch, capsys):
    check = _load_watch_check()
    monkeypatch.delenv("FINAM_API_TOKEN", raising=False)
    assert check.main(["--stream-seconds", "0.1"]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "NOT_CONFIGURED"


def test_monitor_cli_requires_a_token(monkeypatch, capsys):
    from worker import __main__ as cli

    monkeypatch.setattr(cli, "WorkerSettings", lambda: SimpleNamespace(FINAM_API_TOKEN=None))
    assert cli.main(["finam-monitor", "--status-file", "unused.json"]) == 2
    assert "FINAM_NOT_CONFIGURED" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_live_check_reports_every_ticker_and_isolates_one_failure():
    check = _load_watch_check()
    now = minute(30) + timedelta(seconds=20)

    class Adapter:
        async def fetch_asset(self, symbol):
            if symbol == "SBER":
                raise TransientIOError("FINAM_SOURCE_TRANSIENT", "down")
            ticker, mic = finam_symbol(symbol).split("@")
            return {"ticker": ticker, "mic": mic, "board": "TQBR", "name": symbol, "token": SECRET}

        async def fetch_daily(self, symbol, start, end, adjustment_mode):
            stamp = datetime(2026, 11, 26, 20, 59, 59, tzinfo=timezone.utc)
            bar = quote_bar(symbol, 0)
            return FetchResult(status=FetchStatus.OK, symbol=symbol, bars=[QuoteBar(**{**bar.__dict__, "quote_time": stamp, "interval": "1d"})])

        async def fetch_minute_history(self, symbol, start, end):
            return MinuteHistory([quote_bar(symbol, offset) for offset in range(27, 31)], {})

        async def open_market_session(self):
            return JWT

        async def fetch_news_sources(self):
            raise PermanentIOError("FINAM_AUTHORIZATION_FAILED", "no news access")

    async def stream(jwt):
        yield {"type": "connected"}
        for symbol in (*FIVE, "IMOEX"):
            yield {"type": "quote", "symbol": symbol, "last": "101", "quote_time": now.isoformat()}
        await asyncio.sleep(10)  # A live subscription stays open until the check times out.

    report = await check.run_check(Adapter(), stream_seconds=0.2, stream=stream, clock=lambda: now)
    assert list(report["instruments"]) == [*FIVE, "IMOEX"]
    assert report["instruments"]["SBER"]["asset"]["status"] == "ERROR"
    assert report["instruments"]["GAZP"]["asset"]["status"] == "VERIFIED"
    assert "token" not in report["instruments"]["GAZP"]["asset"]
    for symbol in (*FIVE, "IMOEX"):
        row = report["instruments"][symbol]
        assert row["d1"]["status"] == "OK" and row["m1"]["last_closed_m1"] == minute(29).isoformat()
        assert row["stream"]["status"] == LIVE
    assert report["status"] == "LIVE_CHECK_FAILED"
    assert report["news"] == {"status": "NO_ACCESS", "code": "FINAM_AUTHORIZATION_FAILED"}
    assert SECRET not in check.dump({**report, "leak": SECRET}, (SECRET,))
