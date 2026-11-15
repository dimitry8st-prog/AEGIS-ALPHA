"""Strict Finam acceptance scenario without real broker calls."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text

from worker.adapters.finam_adapter import FinamAdapter
from worker.config import WorkerSettings
from worker.export import CSV_NAME, MANIFEST_NAME

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "finam_live_smoke.py"
SECRET = "mock-secret-never-log-acceptance"
JWT = "mock-session-never-log-acceptance"
START = date(2024, 1, 9)
END = date(2024, 1, 11)


def _load_smoke():
    spec = importlib.util.spec_from_file_location("finam_live_smoke", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses reads sys.modules[cls.__module__] while the class body runs.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


smoke = _load_smoke()


def _bar(day: str, close: str = "100.50") -> dict:
    return {
        "timestamp": f"{day}T12:00:00Z",
        "open": {"value": "100.00"},
        "high": {"value": "102.00"},
        "low": {"value": "99.00"},
        "close": {"value": close},
        "volume": {"value": "1000.0"},
    }


def _ok_payload(symbol: str, closes: dict[str, str] | None = None) -> dict:
    closes = closes or {"2024-01-09": "100.50", "2024-01-10": "101.25"}
    return {
        "symbol": f"{symbol}@MISX",
        "bars": [_bar(day, close) for day, close in closes.items()],
    }


def _handler(
    *,
    payloads: dict[str, dict] | None = None,
    auth_status: int = 200,
    bars_status: int = 200,
    timeout: bool = False,
    empty_symbols: set[str] | None = None,
    omit_symbols: set[str] | None = None,
    requests: list | None = None,
):
    payloads = payloads or {}
    empty_symbols = empty_symbols or set()
    omit_symbols = omit_symbols or set()
    seen = requests if requests is not None else []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if timeout:
            raise httpx.ConnectTimeout("connect timed out", request=request)
        if request.url.path == "/v1/sessions":
            if auth_status != 200:
                return httpx.Response(
                    auth_status,
                    json={"message": f"server echoes {SECRET} {JWT}"},
                )
            body = json.loads(request.content)
            assert body == {"secret": SECRET}
            return httpx.Response(200, json={"token": JWT})
        symbol = request.url.path.split("/")[3].split("@")[0]
        if symbol in omit_symbols:
            return httpx.Response(404, json={"message": "missing"})
        if bars_status != 200:
            return httpx.Response(
                bars_status,
                json={"message": f"server echoes {SECRET} {JWT}"},
            )
        if symbol in empty_symbols:
            return httpx.Response(200, json={"symbol": f"{symbol}@MISX", "bars": []})
        payload = payloads.get(symbol) or _ok_payload(symbol)
        return httpx.Response(200, json=payload)

    return handler


async def _clean(engine):
    async with engine.begin() as connection:
        for table in (
            "market_quote_revisions",
            "market_quote_quarantine",
            "market_quotes",
            "market_collection_runs",
        ):
            await connection.execute(text(f"DELETE FROM {table}"))


def _settings(database_url: str) -> WorkerSettings:
    return WorkerSettings(
        DATABASE_URL=database_url,
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        RUN_DEADLINE_SECONDS=60,
    )


async def _quote_total(engine) -> int:
    async with engine.connect() as conn:
        return int(await conn.scalar(text("SELECT COUNT(*)::int FROM market_quotes")) or 0)


@pytest.mark.asyncio
async def test_mock_full_acceptance_two_tickers(migrated_engine, database_url, tmp_path):
    await _clean(migrated_engine)
    requests: list = []
    async with smoke.mock_finam_client(_handler(requests=requests)) as client:
        adapter = FinamAdapter(SECRET, client=client)
        report = await smoke.run_acceptance_chain(
            adapter=adapter,
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=tmp_path / "finam-mock-a",
            mode="mock",
            settings=_settings(database_url),
            secrets=(SECRET, JWT, database_url),
        )
    assert report["status"] == smoke.STATUS_MOCK_PASSED, report.get("failure_reason")
    assert report["status"] != smoke.STATUS_LIVE_VERIFIED
    assert report["mode"] == "mock"
    assert report["failure_reason"] is None
    assert all(stage["ok"] for stage in report["stages"])
    assert report["ticker_counts"] == {"GAZP": 2, "GMKN": 2}
    assert report["repeat_collect"]["counters"]["inserted"] == 0
    assert report["repeat_collect"]["counters"]["updated"] == 0
    assert report["repeat_collect"]["counters"]["duplicates"] == 4
    assert report["csv_manifest_checks"]["first"]["ok"] is True
    assert report["csv_manifest_checks"]["first"]["coverage_status"] == "UNKNOWN"
    assert report["csv_manifest_checks"]["first"]["calendar_completeness_proven"] is False
    dumped = json.dumps(report)
    assert SECRET not in dumped and JWT not in dumped
    assert ":@" not in dumped
    assert requests  # mock HTTP happened inside the chain


@pytest.mark.asyncio
async def test_cli_without_live_touches_neither_http_nor_database(
    migrated_engine, monkeypatch, tmp_path
):
    await _clean(migrated_engine)
    before = await _quote_total(migrated_engine)
    monkeypatch.delenv("FINAM_API_TOKEN", raising=False)

    def boom(request):
        raise AssertionError("HTTP must not be called without --live")

    monkeypatch.setattr(
        FinamAdapter,
        "fetch_daily",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("adapter must not run")),
    )
    code, report = await smoke.async_main(
        [
            "--start",
            "2024-01-09",
            "--end",
            "2024-01-11",
            "--output-dir",
            str(tmp_path / "dry"),
        ]
    )
    assert code == 2
    assert report["status"] == smoke.STATUS_NOT_LIVE
    assert report["mode"] == "dry"
    assert await _quote_total(migrated_engine) == before
    assert not (tmp_path / "dry").exists()


@pytest.mark.asyncio
async def test_cli_live_without_token_is_not_configured(monkeypatch, tmp_path, migrated_engine):
    await _clean(migrated_engine)
    before = await _quote_total(migrated_engine)
    monkeypatch.delenv("FINAM_API_TOKEN", raising=False)
    code, report = await smoke.async_main(
        [
            "--live",
            "--database-url",
            "postgresql://acceptance:acceptance@127.0.0.1:55432/aegis_stage",
            "--start",
            "2024-01-09",
            "--end",
            "2024-01-11",
            "--output-dir",
            str(tmp_path / "live-missing-token"),
        ]
    )
    assert code == 2
    assert report["status"] == smoke.STATUS_NOT_CONFIGURED
    assert "FINAM_API_TOKEN" in report["failure_reason"]
    assert await _quote_total(migrated_engine) == before
    dumped = json.dumps(report)
    assert "acceptance:acceptance" not in dumped


@pytest.mark.asyncio
async def test_cli_live_without_explicit_database_url(monkeypatch, tmp_path):
    monkeypatch.setenv("FINAM_API_TOKEN", SECRET)
    monkeypatch.setenv("DATABASE_URL", "postgresql://should-not:use@127.0.0.1:5432/aegis")
    code, report = await smoke.async_main(
        [
            "--live",
            "--start",
            "2024-01-09",
            "--end",
            "2024-01-11",
            "--output-dir",
            str(tmp_path / "live-missing-db"),
        ]
    )
    assert code == 2
    assert report["status"] == smoke.STATUS_NOT_CONFIGURED
    assert "--database-url" in report["failure_reason"]
    assert "should-not:use" not in json.dumps(report)
    assert SECRET not in json.dumps(report)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,reason_part",
    [
        ({"empty_symbols": {"GAZP", "GMKN"}}, "PARTIAL"),
        ({"empty_symbols": {"GMKN"}}, "PARTIAL"),
        ({"omit_symbols": {"GMKN"}}, "PARTIAL"),
    ],
)
async def test_partial_empty_or_missing_ticker_fails_acceptance(
    migrated_engine, database_url, tmp_path, kwargs, reason_part
):
    await _clean(migrated_engine)
    async with smoke.mock_finam_client(_handler(**kwargs)) as client:
        report = await smoke.run_acceptance_chain(
            adapter=FinamAdapter(SECRET, client=client),
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=tmp_path / f"fail-{reason_part.lower()}",
            mode="mock",
            settings=_settings(database_url),
            secrets=(SECRET,),
        )
    assert report["status"] == smoke.STATUS_FAILED
    assert report["status"] != smoke.STATUS_LIVE_VERIFIED
    assert report["failure_reason"]
    first = next(stage for stage in report["stages"] if stage["name"] == "first_collect_ready")
    assert first["ok"] is False
    assert first["detail"]["status"] in {"PARTIAL", "FAILED"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth_status,bars_status,timeout,code_fragment",
    [
        (401, 200, False, "FINAM_AUTHORIZATION_FAILED"),
        (200, 429, False, "FINAM_SOURCE_TRANSIENT"),
        (200, 200, True, "FINAM_NETWORK_ERROR"),
    ],
)
async def test_auth_rate_limit_and_timeout_fail_without_leaking_secrets(
    migrated_engine, database_url, tmp_path, auth_status, bars_status, timeout, code_fragment
):
    await _clean(migrated_engine)
    async with smoke.mock_finam_client(
        _handler(auth_status=auth_status, bars_status=bars_status, timeout=timeout)
    ) as client:
        report = await smoke.run_acceptance_chain(
            adapter=FinamAdapter(SECRET, client=client),
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=tmp_path / f"err-{code_fragment}",
            mode="mock",
            settings=_settings(database_url),
            secrets=(SECRET, JWT, database_url),
        )
    assert report["status"] == smoke.STATUS_FAILED
    dumped = json.dumps(report)
    assert SECRET not in dumped and JWT not in dumped
    first = next(stage for stage in report["stages"] if stage["name"] == "first_collect_ready")
    assert first["ok"] is False
    # Pipeline marks unavailable symbols; acceptance must not treat this as success.
    assert first["detail"]["status"] in {"FAILED", "PARTIAL"}
    assert code_fragment in dumped or first["detail"]["status"] == "FAILED"


@pytest.mark.asyncio
async def test_repeat_collect_creates_neither_quotes_nor_revisions(
    migrated_engine, database_url, tmp_path
):
    await _clean(migrated_engine)
    async with smoke.mock_finam_client(_handler()) as client:
        report = await smoke.run_acceptance_chain(
            adapter=FinamAdapter(SECRET, client=client),
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=tmp_path / "idempotent",
            mode="mock",
            settings=_settings(database_url),
            secrets=(SECRET,),
        )
    stage = next(
        item for item in report["stages"] if item["name"] == "repeat_collect_duplicates_only"
    )
    assert stage["ok"] is True
    assert stage["detail"]["counters"]["inserted"] == 0
    assert stage["detail"]["counters"]["updated"] == 0
    assert stage["detail"]["revisions_after_repeat"] == stage["detail"]["revisions_before_repeat"]
    assert await _quote_total(migrated_engine) == 4


@pytest.mark.asyncio
async def test_csv_manifest_mismatch_is_detected(migrated_engine, database_url, tmp_path):
    await _clean(migrated_engine)
    async with smoke.mock_finam_client(_handler()) as client:
        await smoke.run_acceptance_chain(
            adapter=FinamAdapter(SECRET, client=client),
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=tmp_path / "hash-base",
            mode="mock",
            settings=_settings(database_url),
            secrets=(SECRET,),
        )
    target = tmp_path / "hash-base"
    csv_path = target / CSV_NAME
    csv_path.write_text(csv_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    check = smoke.verify_export_artifacts(target)
    assert check["ok"] is False
    assert check["checks"]["sha256_match"] is False


@pytest.mark.asyncio
async def test_export_to_existing_directory_preserves_contents(
    migrated_engine, database_url, tmp_path
):
    await _clean(migrated_engine)
    out = tmp_path / "preserve"
    async with smoke.mock_finam_client(_handler()) as client:
        report = await smoke.run_acceptance_chain(
            adapter=FinamAdapter(SECRET, client=client),
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=out,
            mode="mock",
            settings=_settings(database_url),
            secrets=(SECRET,),
        )
    assert report["status"] == smoke.STATUS_MOCK_PASSED
    stage = next(
        item for item in report["stages"] if item["name"] == "export_exists_preserves_target"
    )
    assert stage["ok"] is True
    assert (out / CSV_NAME).is_file()
    assert (out / MANIFEST_NAME).is_file()


@pytest.mark.asyncio
async def test_redact_strips_secrets_and_password_urls():
    payload = {
        "authorization": f"Bearer {JWT}",
        "database_url": "postgresql://user:pass@127.0.0.1:5432/db",
        "message": f"echo {SECRET} and {JWT}",
        "nested": {"api_token": SECRET},
    }
    cleaned = smoke.redact(payload, secrets=(SECRET, JWT))
    dumped = json.dumps(cleaned)
    assert SECRET not in dumped
    assert JWT not in dumped
    assert "user:pass" not in dumped
    assert cleaned["authorization"] == "[redacted]"
    assert cleaned["nested"]["api_token"] == "[redacted]"
    assert cleaned["database_url"] == "[redacted]"
    assert SECRET not in cleaned["message"] and JWT not in cleaned["message"]
    plain = smoke.redact(
        "postgresql://user:pass@127.0.0.1:5432/db leaked",
        secrets=(),
    )
    assert "user:pass" not in plain
    assert "://[redacted]@" in plain


@pytest.mark.asyncio
async def test_successful_mock_never_sets_live_verified(migrated_engine, database_url, tmp_path):
    await _clean(migrated_engine)
    async with smoke.mock_finam_client(_handler()) as client:
        report = await smoke.run_acceptance_chain(
            adapter=FinamAdapter(SECRET, client=client),
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=tmp_path / "no-live-label",
            mode="mock",
            settings=_settings(database_url),
            secrets=(SECRET,),
        )
    assert report["status"] == smoke.STATUS_MOCK_PASSED
    assert smoke.STATUS_LIVE_VERIFIED not in json.dumps(report)
