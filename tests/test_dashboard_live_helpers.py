"""Acceptance helpers for Online · M1 dashboard behaviour."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from app.api.v1 import market
from app.main import app
from worker.finam_live_status import (
    AUTHORIZATION,
    CONNECTION,
    DATA,
    SUBSCRIPTION,
    live_failure_stage,
)

ROOT = Path(__file__).resolve().parents[1]
LIVE_JS = ROOT / "tests" / "test_dashboard_live.mjs"


def test_live_failure_stages_cover_auth_connection_subscription_and_data():
    assert live_failure_stage("FINAM_NOT_CONFIGURED") == AUTHORIZATION
    assert live_failure_stage("FINAM_AUTHORIZATION_FAILED") == AUTHORIZATION
    assert live_failure_stage("FINAM_STREAM_UNAVAILABLE") == CONNECTION
    assert live_failure_stage("FINAM_STREAM_REJECTED") == SUBSCRIPTION
    assert live_failure_stage("FINAM_STREAM_INVALID_DATA") == DATA


@pytest.mark.asyncio
async def test_stream_error_event_includes_stage_and_logs_only_safe_fields(monkeypatch):
    from worker.io_retry import TransientIOError

    class Broker:
        def __init__(self, key):
            assert key == "test-secret"

        async def open_market_session(self):
            return "private-broker-jwt"

    async def events(token):
        assert token == "private-broker-jwt"
        raise TransientIOError("FINAM_STREAM_UNAVAILABLE", "Finam live data connection unavailable")
        yield  # pragma: no cover

    records = []

    def capture(event, **kwargs):
        records.append((event, kwargs))

    monkeypatch.setattr(market, "FinamAdapter", Broker)
    monkeypatch.setattr(market, "stream_finam_market", events)
    monkeypatch.setattr(market.settings, "FINAM_API_TOKEN", SecretStr("test-secret"))
    monkeypatch.setattr(market.logger, "warning", capture)
    monkeypatch.setitem(app.dependency_overrides, market._authenticate_stream, lambda: None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/v1/market/finam/stream")
    assert response.status_code == 200
    payload = json.loads(response.text.strip())
    assert payload == {
        "type": "error",
        "code": "FINAM_STREAM_UNAVAILABLE",
        "stage": "connection",
    }
    assert records == [
        ("Finam live stream failed", {"code": "FINAM_STREAM_UNAVAILABLE", "stage": "connection"}),
    ]
    assert "private-broker-jwt" not in response.text
    assert "test-secret" not in response.text


def test_dashboard_live_js_acceptance():
    completed = subprocess.run(
        ["node", "--test", str(LIVE_JS)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
