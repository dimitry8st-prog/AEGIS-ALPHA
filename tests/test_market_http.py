"""Auth, HTML escaping, readiness, and market query validation."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

import httpx
import pytest

from app.core.dependencies import get_current_active_user, get_db
from app.main import app
from app.models.item import Item
from app.models.user import User


def _client():
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


@pytest.mark.asyncio
async def test_health_is_liveness_without_dependencies():
    async with _client() as client:
        response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["role"] == "liveness"
    assert "postgres" not in response.text
    assert "redis://" not in response.text


@pytest.mark.asyncio
async def test_results_requires_auth_and_escapes_html():
    async with _client() as client:
        anonymous = await client.get("/results")
        assert anonymous.status_code in {401, 403}

        item = Item()
        item.id = uuid.uuid4()
        item.title = "<script>alert(1)</script>"
        item.description = "safe & sound"
        item.created_at = datetime(2024, 1, 2, tzinfo=timezone.utc)

        class _Rows:
            def scalars(self):
                return self

            def all(self):
                return [item]

        class _Session:
            async def execute(self, *args, **kwargs):
                return _Rows()

        async def override_db():
            yield _Session()

        async def override_user():
            user = User()
            user.email = "researcher@example.com"
            return user

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[get_current_active_user] = override_user
        try:
            page = await client.get("/results")
        finally:
            app.dependency_overrides.clear()
    assert page.status_code == 200
    assert "AEGIS – Items" in page.text
    assert "не котировки" in page.text
    assert "<script>" not in page.text
    assert "&lt;script&gt;" in page.text
    assert "safe &amp; sound" in page.text


@pytest.mark.asyncio
async def test_market_routes_require_auth_and_validate_window():
    async with _client() as client:
        runs = await client.get("/api/v1/market/runs")
        quotes = await client.get("/api/v1/market/quotes")
        assert runs.status_code in {401, 403}
        assert quotes.status_code in {401, 403}

        async def override_user():
            user = User()
            user.email = "researcher@example.com"
            return user

        app.dependency_overrides[get_current_active_user] = override_user
        try:
            missing = await client.get("/api/v1/market/quotes")
            mixed = await client.get(
                "/api/v1/market/quotes",
                params={"provider": "offline", "start": "2024-01-02", "end": "2024-01-06"},
            )
            backwards = await client.get(
                "/api/v1/market/quotes",
                params={
                    "provider": "offline",
                    "adjustment_mode": "raw",
                    "start": "2024-01-06",
                    "end": "2024-01-02",
                },
            )
            too_long = await client.get(
                "/api/v1/market/quotes",
                params={
                    "provider": "offline",
                    "adjustment_mode": "raw",
                    "start": "2020-01-01",
                    "end": "2024-01-06",
                },
            )
        finally:
            app.dependency_overrides.clear()
    assert missing.status_code == 422
    assert mixed.status_code == 422
    assert backwards.status_code == 422
    assert too_long.status_code == 422


@pytest.mark.asyncio
async def test_readiness_reports_each_dependency_without_secrets():
    from app import main as app_main

    class Conn:
        async def execute(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class OkEngine:
        def connect(self):
            return Conn()

    class SlowConn(Conn):
        async def __aenter__(self):
            await asyncio.sleep(1)
            return self

    class SlowEngine:
        def connect(self):
            return SlowConn()

    class BrokenEngine:
        def connect(self):
            raise OSError("postgres password=secret host=db.internal")

    class OkRedis:
        async def ping(self):
            return True

    class BrokenRedis:
        async def ping(self):
            raise OSError("redis://:password@10.0.0.8:6379/0 down")

    original_engine = app_main.engine
    original_client = app_main.redis_client._client
    original_timeout = app_main.DEPENDENCY_TIMEOUT_SECONDS
    try:
        app_main.engine = OkEngine()
        app_main.redis_client._client = OkRedis()
        ready = await app_main.readiness_check()
        assert ready["status"] == "ready"

        app_main.engine = BrokenEngine()
        app_main.redis_client._client = OkRedis()
        postgres_down = await app_main.readiness_check()

        app_main.engine = OkEngine()
        app_main.redis_client._client = BrokenRedis()
        redis_down = await app_main.readiness_check()

        app_main.engine = BrokenEngine()
        app_main.redis_client._client = None
        both_down = await app_main.readiness_check()

        app_main.DEPENDENCY_TIMEOUT_SECONDS = 0.2
        app_main.engine = SlowEngine()
        app_main.redis_client._client = OkRedis()
        timed_out = await app_main.readiness_check()
    finally:
        app_main.engine = original_engine
        app_main.redis_client._client = original_client
        app_main.DEPENDENCY_TIMEOUT_SECONDS = original_timeout

    for response in (postgres_down, redis_down, both_down, timed_out):
        payload = json.loads(response.body)
        text = response.body.decode()
        assert response.status_code == 503
        assert payload["status"] == "not_ready"
        assert "password" not in text
        assert "10.0.0.8" not in text
        assert "db.internal" not in text
        assert "secret" not in text
    assert json.loads(postgres_down.body)["dependencies"] == ["postgres"]
    assert json.loads(redis_down.body)["dependencies"] == ["redis"]
    assert set(json.loads(both_down.body)["dependencies"]) == {"postgres", "redis"}
    assert "postgres" in json.loads(timed_out.body)["dependencies"]
