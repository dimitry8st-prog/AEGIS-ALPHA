import json
from contextlib import aclosing

import httpx
import pytest
from pydantic import SecretStr

from app.api.v1 import market
from app.main import app
from worker.adapters import finam_stream


@pytest.mark.asyncio
async def test_finam_feed_subscribes_after_handshake_to_only_read_sources(monkeypatch):
    class Socket:
        def __init__(self):
            self.sent = []
            self.incoming = iter([
                {"type": "EVENT", "event_info": {"event": "HANDSHAKE_SUCCESS"}},
                {"type": "DATA", "subscription_type": "QUOTES", "payload": {"quote": [
                    {"symbol": "GMKN@MISX", "timestamp": "2026-11-20T10:10:02+03:00", "last": "107.21"},
                    {"symbol": "GAZP@MISX", "bid": "105"},  # No last trade in this update.
                ]}},
                {"type": "DATA", "subscription_type": "BARS", "payload": {
                    "symbol": "GAZP@MISX", "bars": [{
                        "timestamp": "2026-11-20T07:10:02Z",
                    }, {
                        "timestamp": "2026-11-20T07:10:00Z", "open": "105.11", "high": "106",
                        "low": "104", "close": "105.22", "volume": "1000",
                    }],
                }},
            ])

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            pass

        async def recv(self):
            return json.dumps(next(self.incoming))

        async def send(self, data):
            self.sent.append(json.loads(data))

    socket = Socket()
    monkeypatch.setattr(finam_stream, "connect", lambda url, **kwargs: socket)
    async with aclosing(finam_stream.stream_finam_market("mock-broker-jwt")) as feed:
        assert await anext(feed) == {"type": "connected"}
        assert [row["type"] for row in socket.sent] == ["QUOTES", "BARS", "BARS"]
        assert socket.sent[0]["data"]["symbols"] == ["GAZP@MISX", "GMKN@MISX"]
        assert all(row["data"]["timeframe"] == "TIME_FRAME_M1" for row in socket.sent[1:])
        quote, bar, warning = await anext(feed), await anext(feed), await anext(feed)
    assert quote == {
        "type": "quote", "symbol": "GMKN", "last": "107.21", "quote_time": "2026-11-20T07:10:02+00:00",
    }
    assert (bar["symbol"], bar["close"], bar["volume"]) == ("GAZP", "105.22", 1000)
    assert warning == {"type": "data_warning", "feed": "BARS", "rejected": 1}


@pytest.mark.asyncio
async def test_api_stream_requires_login_and_does_not_forward_broker_jwt(monkeypatch):
    class Broker:
        def __init__(self, key):
            assert key == "test-secret"

        async def open_market_session(self):
            return "private-broker-jwt"

    async def events(token):
        assert token == "private-broker-jwt"
        yield {"type": "connected"}
        yield {"type": "quote", "symbol": "GAZP", "last": "105.27"}

    monkeypatch.setattr(market, "FinamAdapter", Broker)
    monkeypatch.setattr(market, "stream_finam_market", events)
    monkeypatch.setattr(market.settings, "FINAM_API_TOKEN", SecretStr("test-secret"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        route = "/api/v1/market/finam/stream"
        assert (await client.get(route)).status_code in {401, 403}
        monkeypatch.setitem(app.dependency_overrides, market._authenticate_stream, lambda: None)
        response = await client.get(route)
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-ndjson"
    assert [json.loads(row)["type"] for row in response.text.splitlines()] == ["connected", "quote"]
    assert "private-broker-jwt" not in response.text


@pytest.mark.asyncio
async def test_stream_maps_authorization_failure_to_stage(monkeypatch):
    from worker.io_retry import PermanentIOError

    class Socket:
        def __init__(self):
            self.incoming = iter([
                {"type": "ERROR", "error_info": {"type": "UNAUTHENTICATED"}},
            ])

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            pass

        async def recv(self):
            return json.dumps(next(self.incoming))

        async def send(self, data):
            pass

    monkeypatch.setattr(finam_stream, "connect", lambda url, **kwargs: Socket())
    with pytest.raises(PermanentIOError) as raised:
        async with aclosing(finam_stream.stream_finam_market("mock-broker-jwt")) as feed:
            await anext(feed)
    assert raised.value.code == "FINAM_AUTHORIZATION_FAILED"
    from worker.finam_live_status import live_failure_stage
    assert live_failure_stage(raised.value.code) == "authorization"
