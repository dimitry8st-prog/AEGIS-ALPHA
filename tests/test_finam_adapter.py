"""Finam REST contract without network access, credentials or account calls."""

import json
from datetime import date, datetime
from decimal import Decimal

import httpx
import pytest

from worker.adapters.finam_adapter import FinamAdapter
from worker.io_retry import PermanentIOError, TransientIOError
from worker.types import AdjustmentMode, FetchStatus

SECRET = "mock-secret-never-log"
JWT = "mock-session-never-log"
START = date(2026, 10, 19)
END = date(2026, 10, 21)


def bar(stamp="2026-10-19T21:00:00Z", **changes):
    result = {
        "timestamp": stamp,
        "open": {"value": "124.12000001"},
        "high": {"value": "125.5"},
        "low": {"value": "123.1"},
        "close": {"value": "124.6"},
        "volume": {"value": "123456.0"},
    }
    result.update(changes)
    return result


@pytest.mark.asyncio
async def test_contract_decimal_and_moscow_date_with_exclusive_end():
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.host == "api.finam.ru"
        assert request.extensions["timeout"]["read"] == 15.0
        if request.url.path == "/v1/sessions":
            assert request.method == "POST"
            assert json.loads(request.content) == {"secret": SECRET}
            assert "authorization" not in request.headers
            return httpx.Response(200, json={"token": JWT})
        assert request.method == "GET"
        assert request.url.path == "/v1/instruments/GAZP@MISX/bars"
        assert request.headers["authorization"] == f"Bearer {JWT}"
        assert SECRET not in str(request.url)
        assert request.url.params["timeframe"] == "TIME_FRAME_D"
        assert request.url.params["interval.start_time"] == "2026-10-19T00:00:00+03:00"
        assert request.url.params["interval.end_time"] == "2026-10-21T00:00:00+03:00"
        return httpx.Response(200, json={"symbol": "GAZP@MISX", "bars": [
            bar(),  # 21:00 UTC on the 19th is Moscow session date 20th.
            bar("2026-10-20T21:00:00Z"),  # Outside the exclusive Moscow end.
        ]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = FinamAdapter(SECRET, client=client)
        result = await adapter.fetch_daily("gazp", START, END, AdjustmentMode.RAW)
        assert not client.is_closed  # Injected clients remain owned by their caller.
    assert len(requests) == 2
    assert result.status is FetchStatus.OK
    quote = result.bars[0]
    assert quote.symbol == "GAZP" and quote.exchange == "MISX"
    assert quote.currency == "RUB" and quote.session_timezone == "Europe/Moscow"
    assert quote.quote_time.isoformat() == "2026-10-20T23:59:59+03:00"
    assert quote.open == Decimal("124.12000001")
    assert quote.volume == 123456
    assert quote.adjustment_mode is AdjustmentMode.RAW
    assert len(result.parse_failures) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("include_window", [False, True])
async def test_previous_moscow_day_is_ignored_once_without_expanding_the_window(include_window):
    previous_day = "2026-11-15T05:00:00Z"
    rows = [bar(previous_day, is_data_snapshot=False)]
    rejected = []
    if include_window:
        rejected = [previous_day, "2026-11-14T05:00:00Z", "2026-11-20T21:00:00Z"]
        rows = [bar(f"2026-11-{day}T05:00:00Z") for day in range(16, 21)] + rows
        rows.extend(bar(stamp) for stamp in rejected)

    def handler(request):
        if request.method == "POST":
            return httpx.Response(200, json={"token": JWT})
        return httpx.Response(200, json={"symbol": "GAZP@MISX", "bars": rows})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await FinamAdapter(SECRET, client=client).fetch_daily(
            "GAZP", date(2026, 11, 16), date(2026, 11, 21), AdjustmentMode.RAW,
        )
    expected_dates = [date(2026, 11, day) for day in range(16, 21)] if include_window else []
    assert result.status is (FetchStatus.OK if include_window else FetchStatus.EMPTY)
    assert [quote.quote_time.date() for quote in result.bars] == expected_dates
    assert result.ignored_boundary_rows == 1
    assert [failure.payload["timestamp"] for failure in result.parse_failures] == rejected


@pytest.mark.asyncio
@pytest.mark.parametrize("status,exception,code", [
    (401, PermanentIOError, "FINAM_AUTHORIZATION_FAILED"),
    (403, PermanentIOError, "FINAM_AUTHORIZATION_FAILED"),
    (400, PermanentIOError, "FINAM_HTTP_ERROR"),
    (404, PermanentIOError, "FINAM_HTTP_ERROR"),
    (302, PermanentIOError, "FINAM_HTTP_ERROR"),
    (429, TransientIOError, "FINAM_SOURCE_TRANSIENT"),
    (500, TransientIOError, "FINAM_SOURCE_TRANSIENT"),
    (503, TransientIOError, "FINAM_SOURCE_TRANSIENT"),
])
@pytest.mark.parametrize("phase", ["auth", "bars"])
async def test_http_failures_are_classified_and_never_echo_secret(status, exception, code, phase):
    requests = []

    def handler(request):
        requests.append(request)
        if phase == "bars" and request.method == "POST":
            return httpx.Response(200, json={"token": JWT})
        return httpx.Response(status, json={"message": f"server echoes {SECRET} {JWT}"},
                              headers={"location": "https://untrusted.invalid/"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(exception) as caught:
            await FinamAdapter(SECRET, client=client).fetch_daily("GMKN", START, END, AdjustmentMode.RAW)
    assert caught.value.code == code
    assert SECRET not in str(caught.value) and JWT not in str(caught.value)
    assert len(requests) == (1 if phase == "auth" else 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("token,symbol,mode,start,end,code", [
    (None, "GAZP", AdjustmentMode.RAW, START, END, "FINAM_NOT_CONFIGURED"),
    (" ", "GAZP", AdjustmentMode.RAW, START, END, "FINAM_NOT_CONFIGURED"),
    (SECRET, "AAPL", AdjustmentMode.RAW, START, END, "UNSUPPORTED_INSTRUMENT"),
    (SECRET, "GAZP@MISX", AdjustmentMode.RAW, START, END, "UNSUPPORTED_INSTRUMENT"),
    (SECRET, "GAZP", AdjustmentMode.AUTO_ADJUSTED, START, END, "UNSUPPORTED_ADJUSTMENT"),
    (SECRET, "GAZP", AdjustmentMode.RAW, END, START, "INVALID_WINDOW"),
    (SECRET, "GAZP", AdjustmentMode.RAW, START, START, "INVALID_WINDOW"),
    (SECRET, "GAZP", AdjustmentMode.RAW, date(2024, 1, 1), date(2025, 1, 1), "INVALID_WINDOW"),
])
async def test_invalid_selection_never_calls_http(token, symbol, mode, start, end, code):
    def handler(request):
        raise AssertionError("HTTP must not be called")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PermanentIOError) as caught:
            await FinamAdapter(token, client=client).fetch_daily(symbol, start, end, mode)
    assert caught.value.code == code


@pytest.mark.asyncio
async def test_chunks_are_finite_non_overlapping_and_within_documented_depth():
    chunks = []

    def handler(request):
        if request.method == "POST":
            return httpx.Response(200, json={"token": JWT})
        chunks.append((request.url.params["interval.start_time"], request.url.params["interval.end_time"]))
        return httpx.Response(200, json={"symbol": "GMKN@MISX", "bars": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await FinamAdapter(SECRET, client=client).fetch_daily(
            "GMKN", date(2025, 10, 21), date(2026, 10, 21), AdjustmentMode.RAW)
    assert result.status is FetchStatus.EMPTY
    assert len(chunks) == 12
    assert chunks[0][0].startswith("2025-10-21")
    assert chunks[-1][1].startswith("2026-10-21")
    for start, end in chunks:
        assert 0 < (datetime.fromisoformat(end) - datetime.fromisoformat(start)).days <= 31
    assert all(chunks[i][1] == chunks[i + 1][0] for i in range(len(chunks) - 1))


@pytest.mark.asyncio
async def test_bad_decimal_fractional_volume_duplicate_and_naive_timestamp_are_quarantined():
    rows = [bar(), bar(), bar("2026-10-19T12:00:00"),
            bar("2026-10-19T12:00:00Z", volume={"value": "1.1"}),
            bar("2026-10-19T12:00:00Z", open={"value": f"NaN {SECRET} {JWT}"})]
    def handler(request):
        if request.method == "POST":
            return httpx.Response(200, json={"token": JWT})
        return httpx.Response(200, json={"symbol": "GAZP@MISX", "bars": rows})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await FinamAdapter(SECRET, client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert len(result.bars) == 1
    assert len(result.parse_failures) == 4
    payload = repr(result.parse_failures)
    assert SECRET not in payload and JWT not in payload
    assert "[redacted]" in payload


@pytest.mark.asyncio
async def test_source_symbol_mismatch_is_not_accepted():
    def handler(request):
        if request.method == "POST":
            return httpx.Response(200, json={"token": JWT})
        return httpx.Response(200, json={"symbol": "GMKN@MISX", "bars": []})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PermanentIOError) as caught:
            await FinamAdapter(SECRET, client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert caught.value.code == "FINAM_SYMBOL_MISMATCH"


@pytest.mark.asyncio
async def test_network_exception_is_sanitized():
    def handler(request):
        raise httpx.ReadTimeout(f"unsafe request {SECRET} {JWT}", request=request)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(TransientIOError) as caught:
            await FinamAdapter(SECRET, client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert caught.value.code == "FINAM_NETWORK_ERROR"
    assert SECRET not in str(caught.value) and JWT not in str(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_timestamp", [None, "2026-11-20T07:12:00Z"])
async def test_m1_uses_utc_and_excludes_the_window_end(bad_timestamp):
    def handler(request):
        if request.method == "POST":
            assert request.url.path == "/v1/sessions"
            return httpx.Response(200, json={"token": JWT})
        assert request.url.path == "/v1/instruments/GAZP@MISX/bars"
        assert dict(request.url.params) == {
            "timeframe": "TIME_FRAME_M1",
            "interval.start_time": "2026-11-20T07:10:00+00:00",
            "interval.end_time": "2026-11-20T07:12:00+00:00",
        }
        assert request.headers["authorization"] == f"Bearer {JWT}"
        rows = [bar("2026-11-20T07:11:00Z"), bar("2026-11-20T10:10:00+03:00")]
        if bad_timestamp:
            rows.append(bar(bad_timestamp))
        return httpx.Response(200, json={"symbol": "GAZP@MISX", "bars": rows})

    start = datetime.fromisoformat("2026-11-20T10:10:00+03:00")
    end = datetime.fromisoformat("2026-11-20T10:12:00+03:00")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = FinamAdapter(SECRET, client=client)
        if bad_timestamp:
            with pytest.raises(PermanentIOError, match="out-of-window minute"):
                await adapter.fetch_minutes("GAZP", start, end)
        else:
            bars = await adapter.fetch_minutes("GAZP", start, end)
            assert [row.quote_time.isoformat() for row in bars] == [
                "2026-11-20T07:10:00+00:00", "2026-11-20T07:11:00+00:00",
            ]
            assert all(row.interval == "1m" and row.currency == "RUB" for row in bars)
