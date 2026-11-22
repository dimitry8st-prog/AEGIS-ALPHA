"""Strict Finam acceptance scenario without real broker calls."""

from __future__ import annotations

import importlib.util
import json
import logging
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
    gazp = _ok_payload("GAZP")
    gazp["bars"].insert(0, _bar("2024-01-08"))
    async with smoke.mock_finam_client(_handler(requests=requests, payloads={"GAZP": gazp})) as client:
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
    assert smoke.acceptance_exit_code(report) == 0
    assert report["status"] != smoke.STATUS_LIVE_VERIFIED
    assert report["mode"] == "mock"
    assert report["failure_reason"] is None
    assert all(stage["ok"] for stage in report["stages"])
    assert report["ticker_counts"] == {"GAZP": 2, "GMKN": 2}
    first = next(stage["detail"] for stage in report["stages"] if stage["name"] == "first_collect_ready")
    assert first["counters"]["received"] == 4
    assert first["per_symbol"]["GAZP"]["ignored_boundary_rows"] == 1
    assert report["repeat_collect"]["per_symbol"]["GAZP"]["ignored_boundary_rows"] == 1
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


def _must_not_build(*_args, **_kwargs):
    raise AssertionError("live resources must not be created")


@pytest.mark.asyncio
async def test_cli_without_live_touches_neither_http_nor_database(
    migrated_engine, monkeypatch, tmp_path
):
    await _clean(migrated_engine)
    before = await _quote_total(migrated_engine)
    monkeypatch.setenv("FINAM_API_TOKEN", SECRET)
    monkeypatch.setenv(
        "FINAM_ACCEPTANCE_DATABASE_URL",
        "postgresql://acceptance:super-secret-db-pass@127.0.0.1:1/aegis_finam_acceptance",
    )
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://worker:working-database-password@127.0.0.1:5432/aegis",
    )
    monkeypatch.setattr(smoke, "create_engine", _must_not_build)
    monkeypatch.setattr(smoke, "FinamAdapter", _must_not_build)
    argv = [
        "--start",
        "2024-01-09",
        "--end",
        "2024-01-11",
        "--output-dir",
        str(tmp_path / "dry"),
    ]
    assert "--live" not in argv
    assert SECRET not in argv
    code, report = await smoke.async_main(argv)
    assert code == 2
    assert report["status"] == smoke.STATUS_NOT_LIVE
    assert report["mode"] == "dry"
    assert await _quote_total(migrated_engine) == before
    assert not (tmp_path / "dry").exists()
    dumped = json.dumps(report)
    assert SECRET not in dumped
    assert "super-secret-db-pass" not in dumped
    assert "working-database-password" not in dumped


@pytest.mark.asyncio
async def test_cli_live_without_token_is_not_configured(monkeypatch, tmp_path, migrated_engine):
    await _clean(migrated_engine)
    before = await _quote_total(migrated_engine)
    monkeypatch.delenv("FINAM_API_TOKEN", raising=False)
    monkeypatch.setenv(
        "FINAM_ACCEPTANCE_DATABASE_URL",
        "postgresql://acceptance:super-secret-db-pass@127.0.0.1:55432/aegis_finam_acceptance",
    )
    monkeypatch.setattr(smoke, "create_engine", _must_not_build)
    monkeypatch.setattr(smoke, "FinamAdapter", _must_not_build)
    argv = [
        "--live",
        "--start",
        "2024-01-09",
        "--end",
        "2024-01-11",
        "--output-dir",
        str(tmp_path / "live-missing-token"),
    ]
    assert SECRET not in argv
    assert "super-secret-db-pass" not in argv
    code, report = await smoke.async_main(argv)
    assert code == 2
    assert report["status"] == smoke.STATUS_NOT_CONFIGURED
    assert "FINAM_API_TOKEN" in report["failure_reason"]
    assert await _quote_total(migrated_engine) == before
    dumped = json.dumps(report)
    assert "super-secret-db-pass" not in dumped


@pytest.mark.asyncio
async def test_cli_live_without_explicit_database_url(monkeypatch, tmp_path):
    monkeypatch.setenv("FINAM_API_TOKEN", SECRET)
    monkeypatch.delenv("FINAM_ACCEPTANCE_DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://should-not:working-database-password@127.0.0.1:5432/aegis",
    )
    monkeypatch.setattr(smoke, "create_engine", _must_not_build)
    monkeypatch.setattr(smoke, "FinamAdapter", _must_not_build)
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
    assert "FINAM_ACCEPTANCE_DATABASE_URL" in report["failure_reason"]
    assert "DATABASE_URL is not used" in report["failure_reason"]
    dumped = json.dumps(report)
    assert "working-database-password" not in dumped
    assert SECRET not in dumped


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
    assert smoke.acceptance_exit_code(report) != 0
    assert report["status"] != smoke.STATUS_LIVE_VERIFIED
    assert report["failure_reason"]
    first = next(stage for stage in report["stages"] if stage["name"] == "first_collect_ready")
    assert first["ok"] is False
    assert first["detail"]["status"] == "PARTIAL"
    assert reason_part == "PARTIAL"


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
    assert smoke.acceptance_exit_code(report) != 0
    dumped = json.dumps(report)
    assert SECRET not in dumped and JWT not in dumped
    first = next(stage for stage in report["stages"] if stage["name"] == "first_collect_ready")
    assert first["ok"] is False
    # Pipeline marks unavailable symbols; acceptance must not treat this as success.
    assert first["detail"]["status"] == "FAILED"
    assert code_fragment in dumped


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
    assert report["status"] == smoke.STATUS_MOCK_PASSED
    assert smoke.acceptance_exit_code(report) == 0
    assert stage["ok"] is True
    assert stage["detail"]["counters"]["inserted"] == 0
    assert stage["detail"]["counters"]["updated"] == 0
    assert stage["detail"]["counters"]["duplicates"] == stage["detail"]["counters"]["received"]
    assert stage["detail"]["revisions_after_repeat"] == stage["detail"]["revisions_before_repeat"]
    assert await _quote_total(migrated_engine) == 4


@pytest.mark.asyncio
async def test_repeat_rejects_missing_source_bar(migrated_engine, database_url, tmp_path):
    await _clean(migrated_engine)
    handler = _handler()
    gazp_reads = 0

    def omit_bar_on_repeat(request):
        nonlocal gazp_reads
        response = handler(request)
        if request.url.path.endswith("/GAZP@MISX/bars"):
            gazp_reads += 1
            if gazp_reads == 2:
                payload = response.json()
                payload["bars"].pop()
                return httpx.Response(200, json=payload)
        return response

    output_dir = tmp_path / "incomplete-repeat"
    async with smoke.mock_finam_client(omit_bar_on_repeat) as client:
        report = await smoke.run_acceptance_chain(
            adapter=FinamAdapter(SECRET, client=client),
            engine=migrated_engine,
            symbols=["GAZP", "GMKN"],
            start=START,
            end=END,
            output_dir=output_dir,
            mode="mock",
            settings=_settings(database_url),
        )

    assert report["repeat_collect"]["counters"]["duplicates"] == 3
    assert report["ticker_counts"] == {"GAZP": 2, "GMKN": 2}
    assert report["failure_reason"] == "REPEAT_SOURCE_BARS_CHANGED"
    assert smoke.acceptance_exit_code(report) != 0
    assert not output_dir.exists()


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
    assert smoke.acceptance_exit_code({"status": smoke.STATUS_FAILED, "stages": []}) != 0


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
    assert smoke.acceptance_exit_code(report) == 0
    assert smoke.STATUS_LIVE_VERIFIED not in json.dumps(report)


def test_parser_does_not_accept_secrets_as_arguments():
    parser = smoke.build_parser()
    dests = {action.dest for action in parser._actions}
    assert "database_url" not in dests
    assert "token" not in dests
    assert "password" not in dests
    help_text = " ".join(parser.format_help().split())
    assert "--database-url" not in help_text
    assert "--token" not in help_text
    assert "--password" not in help_text
    assert "FINAM_API_TOKEN" in help_text
    assert "FINAM_ACCEPTANCE_DATABASE_URL" in help_text
    assert "DATABASE_URL is not read" in help_text


@pytest.mark.asyncio
async def test_one_symbol_and_empty_window_exit_nonzero_without_broker(monkeypatch, tmp_path):
    monkeypatch.setenv("FINAM_API_TOKEN", SECRET)
    monkeypatch.setenv(
        "FINAM_ACCEPTANCE_DATABASE_URL",
        "postgresql://acceptance:super-secret-db-pass@127.0.0.1:1/aegis_finam_acceptance",
    )
    monkeypatch.setattr(smoke, "create_engine", _must_not_build)
    monkeypatch.setattr(smoke, "FinamAdapter", _must_not_build)
    one_symbol, one_report = await smoke.async_main(
        [
            "--live",
            "--symbols",
            "GAZP",
            "--start",
            "2024-01-09",
            "--end",
            "2024-01-11",
            "--output-dir",
            str(tmp_path / "one-symbol"),
        ]
    )
    assert one_symbol != 0
    assert one_report["status"] == smoke.STATUS_FAILED
    empty_window, empty_report = await smoke.async_main(
        [
            "--live",
            "--start",
            "2024-01-11",
            "--end",
            "2024-01-11",
            "--output-dir",
            str(tmp_path / "empty-window"),
        ]
    )
    assert empty_window != 0
    assert empty_report["status"] == smoke.STATUS_FAILED
    dumped = json.dumps({"one": one_report, "empty": empty_report})
    assert SECRET not in dumped
    assert "super-secret-db-pass" not in dumped


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "working_url",
    [
        "postgresql://test:mock-db-password@localhost/working",
        "postgresql+asyncpg://prod:other-password@127.0.0.1:5432/working",
    ],
)
async def test_live_rejects_working_database_before_opening_resources(
    working_url, monkeypatch, tmp_path
):
    monkeypatch.setenv("FINAM_API_TOKEN", SECRET)
    monkeypatch.setenv("DATABASE_URL", working_url)
    monkeypatch.setenv(
        "FINAM_ACCEPTANCE_DATABASE_URL",
        "postgresql://test:mock-db-password@localhost/working",
    )
    monkeypatch.setattr(smoke, "create_engine", _must_not_build)
    monkeypatch.setattr(smoke, "FinamAdapter", _must_not_build)
    code, report = await smoke.async_main(
        ["--live", "--start", "2024-01-09", "--end", "2024-01-11", "--output-dir", str(tmp_path / "unused")]
    )
    assert code != 0
    assert report["failure_reason"] == "ACCEPTANCE_DATABASE_MATCHES_WORKING_DATABASE"
    assert "mock-db-password" not in json.dumps(report)
    assert not (tmp_path / "unused").exists()


def test_live_uses_acceptance_url_and_hides_secrets_from_logs(monkeypatch, caplog, capsys):
    acceptance = "postgresql://acceptance:super-secret-db-pass@127.0.0.1:1/aegis_finam_acceptance"
    decoy = "postgresql://should-not:working-database-password@127.0.0.1:5432/aegis"
    monkeypatch.setenv("FINAM_API_TOKEN", SECRET)
    monkeypatch.setenv("FINAM_ACCEPTANCE_DATABASE_URL", acceptance)
    monkeypatch.setenv("DATABASE_URL", decoy)
    used: list[str] = []

    def fake_create(settings):
        used.append(settings.DATABASE_URL)
        logging.getLogger("finam.acceptance.test").error(
            "dsn %s token %s bearer %s",
            settings.DATABASE_URL,
            SECRET,
            JWT,
        )
        raise RuntimeError(f"connect failed {settings.DATABASE_URL} token {SECRET} bearer {JWT}")

    monkeypatch.setattr(smoke, "create_engine", fake_create)
    argv = [
        "--live",
        "--start",
        "2024-01-09",
        "--end",
        "2024-01-11",
        "--output-dir",
        "datasets/finam-live-smoke-should-not-run",
    ]
    assert SECRET not in argv
    assert "super-secret-db-pass" not in argv
    with caplog.at_level(logging.ERROR):
        code = smoke.main(argv)
    assert used == [acceptance]
    assert code == 2
    printed = capsys.readouterr().out
    assert SECRET not in printed
    assert JWT not in printed
    assert "super-secret-db-pass" not in printed
    assert "working-database-password" not in printed
    assert SECRET not in caplog.text
    assert JWT not in caplog.text
    assert "super-secret-db-pass" not in caplog.text
    assert "working-database-password" not in caplog.text
