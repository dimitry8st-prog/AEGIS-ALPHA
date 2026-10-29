"""Offline contract checks for the official, read-only MOEX ISS boundary."""

from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal

import httpx
import pytest

from worker.adapters.moex_adapter import MoexAdapter
from worker.io_retry import PermanentIOError, TransientIOError
from worker.types import AdjustmentMode, FetchStatus


COLUMNS = ["open", "close", "high", "low", "value", "volume", "begin", "end"]
START, END = date(2024, 1, 9), date(2024, 1, 13)


def row(day="2024-01-09", *, open_="150.10", close="151.20", high="152", low="149", volume=12300):
    return [open_, close, high, low, "1860000", volume, f"{day} 00:00:00", f"{day} 23:59:59"]


def reply(rows, *, columns=COLUMNS):
    return httpx.Response(200, json={"candles": {"columns": columns, "data": rows}})


@pytest.mark.asyncio
async def test_official_url_named_columns_and_pagination_until_empty():
    requests = []
    reordered = list(reversed(COLUMNS))

    def handler(request):
        requests.append(request)
        offset = int(request.url.params["start"])
        if offset == 0:
            return reply([list(reversed(row()))], columns=reordered)
        if offset == 1:
            return reply([row("2024-01-10")])
        return reply([])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await MoexAdapter(client=client, timeout_seconds=2).fetch_daily("gazp", START, END, AdjustmentMode.RAW)
        assert not client.is_closed
    assert result.status is FetchStatus.OK
    assert len(result.bars) == 2
    assert result.parse_failures == []
    first = result.bars[0]
    assert first.open == Decimal("150.10")
    assert first.close == Decimal("151.20")
    assert first.volume == 12300
    assert (first.provider, first.exchange, first.currency, first.symbol) == ("moex", "MISX", "RUB", "GAZP")
    assert first.session_timezone == "Europe/Moscow"
    assert first.quote_time.isoformat() == "2024-01-09T23:59:59+03:00"
    assert first.quote_time.utcoffset() == timedelta(hours=3)
    assert [request.url.params["start"] for request in requests] == ["0", "1", "2"]
    for request in requests:
        assert request.method == "GET"
        assert request.url.host == "iss.moex.com"
        assert request.url.path.endswith("/boards/TQBR/securities/GAZP/candles.json")
        assert request.url.params["interval"] == "24"
        assert request.url.params["from"] == "2024-01-09"
        assert request.url.params["till"] == "2024-01-12"
        assert request.extensions["timeout"]["read"] == 2


@pytest.mark.asyncio
async def test_json_numbers_use_decimal_without_binary_float_roundtrip():
    body = b'{"candles":{"columns":["open","high","low","close","volume","begin","end"],"data":[[150.123456789012345,152,149,151.123456789012345,12300,"2024-01-09 00:00:00","2024-01-09 23:59:59"]]}}'

    def handler(request):
        return httpx.Response(200, content=body) if request.url.params["start"] == "0" else reply([])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await MoexAdapter(client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert result.bars[0].open == Decimal("150.123456789012345")
    assert result.bars[0].close == Decimal("151.123456789012345")


@pytest.mark.asyncio
async def test_empty_table_is_empty_result():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: reply([]))) as client:
        result = await MoexAdapter(client=client).fetch_daily("GMKN", START, END, AdjustmentMode.RAW)
    assert result.status is FetchStatus.EMPTY
    assert not result.bars and not result.parse_failures


@pytest.mark.asyncio
async def test_raw_gmkn_does_not_adjust_split_discontinuity():
    source = [row("2024-04-01", open_="15000", close="15100", high="15200", low="14900"),
              row("2024-04-08", open_="150", close="151", high="152", low="149")]

    def handler(request):
        return reply(source) if request.url.params["start"] == "0" else reply([])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await MoexAdapter(client=client).fetch_daily("GMKN", date(2024, 4, 1), date(2024, 4, 9), AdjustmentMode.RAW)
    assert [bar.open for bar in result.bars] == [Decimal("15000"), Decimal("150")]
    assert all(bar.adjustment_mode is AdjustmentMode.RAW for bar in result.bars)


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol,start,end,mode,code", [
    ("AAPL", START, END, AdjustmentMode.RAW, "UNSUPPORTED_INSTRUMENT"),
    ("GAZP", START, END, AdjustmentMode.AUTO_ADJUSTED, "UNSUPPORTED_ADJUSTMENT"),
    ("GAZP", START, START, AdjustmentMode.RAW, "INVALID_WINDOW"),
])
async def test_invalid_selection_does_not_call_source(symbol, start, end, mode, code):
    def handler(_):
        raise AssertionError("source must not be called")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PermanentIOError) as caught:
            await MoexAdapter(client=client).fetch_daily(symbol, start, end, mode)
    assert caught.value.code == code


@pytest.mark.asyncio
@pytest.mark.parametrize("status,error", [(429, TransientIOError), (408, TransientIOError), (500, TransientIOError),
                                         (503, TransientIOError), (400, PermanentIOError), (403, PermanentIOError), (404, PermanentIOError)])
async def test_http_status_classification(status, error):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status))) as client:
        with pytest.raises(error) as caught:
            await MoexAdapter(client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert caught.value.code == "SOURCE_HTTP"


@pytest.mark.asyncio
async def test_http_timeout_is_retryable():
    def handler(request):
        raise httpx.ReadTimeout("fixture timeout", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(TransientIOError) as caught:
            await MoexAdapter(client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert caught.value.code == "SOURCE_TRANSPORT"


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [httpx.Response(200, text="<html>error</html>"),
                                      httpx.Response(200, json={}),
                                      reply([], columns=["open"]),
                                      reply([], columns=COLUMNS + ["open"])])
async def test_malformed_envelope_is_permanent_source_schema_failure(response):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as client:
        with pytest.raises(PermanentIOError) as caught:
            await MoexAdapter(client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert caught.value.code == "SOURCE_SCHEMA"


@pytest.mark.asyncio
async def test_bad_rows_and_outside_window_go_to_quarantine():
    outside = row("2024-01-13")
    missing = row(); missing[0] = None
    invalid_end = row(); invalid_end[-1] = "2024-01-10 23:59:59"
    source = [row(), row(volume="1.5"), row(open_="NaN"), outside, missing, invalid_end, [1]]

    def handler(request):
        return reply(source) if request.url.params["start"] == "0" else reply([])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await MoexAdapter(client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert result.status is FetchStatus.OK
    assert len(result.bars) == 1 and len(result.parse_failures) == 6
    assert [failure.reason_code for failure in result.parse_failures].count("OUTSIDE_WINDOW") == 1
    for failure in result.parse_failures:
        assert failure.provider == "moex" and failure.adjustment_mode == "raw"
        assert failure.payload["board"] == "TQBR"
        json.dumps(failure.payload)  # JSONB-safe payload, including malformed values.


@pytest.mark.asyncio
async def test_conflicting_duplicate_candles_are_both_quarantined():
    source = [row(), row(close="150.50"), row("2024-01-10"), row("2024-01-10")]

    def handler(request):
        return reply(source) if request.url.params["start"] == "0" else reply([])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await MoexAdapter(client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert [bar.quote_time.date() for bar in result.bars] == [date(2024, 1, 10)]
    assert sorted(failure.reason_code for failure in result.parse_failures) == ["CONFLICTING_CANDLE", "CONFLICTING_CANDLE", "DUPLICATE_CANDLE"]


@pytest.mark.asyncio
async def test_repeated_page_does_not_loop_or_return_false_complete():
    calls = []

    def handler(request):
        calls.append(request.url.params["start"])
        return reply([row()])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PermanentIOError) as caught:
            await MoexAdapter(client=client).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert caught.value.code == "SOURCE_PAGINATION"
    assert calls == ["0", "1"]


@pytest.mark.asyncio
async def test_page_limit_does_not_publish_truncated_result():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: reply([row()]))) as client:
        with pytest.raises(PermanentIOError) as caught:
            await MoexAdapter(client=client, max_pages=1).fetch_daily("GAZP", START, END, AdjustmentMode.RAW)
    assert caught.value.code == "SOURCE_PAGE_LIMIT"
