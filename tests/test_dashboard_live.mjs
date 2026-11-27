import assert from "node:assert/strict";
import test from "node:test";

import {
  alignedChange,
  closedMinuteBars,
  newsFeedText,
  symbolStatus,
  classifyLiveFailure,
  failureStage,
  historyStatusText,
  lastCandleTime,
  latestLiveQuote,
  mergeMinuteBars,
  streamStatusText,
} from "../app/dashboard/assets/live.js";

test("REST history stays visible when the stream fails", () => {
  const history = mergeMinuteBars([], [
    { quote_time: "2026-11-20T07:10:00Z", close: "105.10" },
    { quote_time: "2026-11-20T07:11:00Z", close: "105.22" },
  ]);
  assert.equal(history.length, 2);
  assert.equal(historyStatusText({ loading: false, errorLabel: "", barCount: history.length }), "История M1: загружена");
  assert.match(
    streamStatusText({
      connecting: false,
      connected: false,
      waiting: false,
      errorLabel: classifyLiveFailure({ code: "FINAM_STREAM_UNAVAILABLE", stage: "connection" }, "stream").label,
    }),
    /подключение/,
  );
  assert.equal(lastCandleTime(history), "2026-11-20T07:11:00Z");
});

test("connected stream without trades keeps waiting status", () => {
  assert.equal(
    streamStatusText({ connecting: false, connected: true, waiting: true, errorLabel: "" }),
    "Поток: подключён · ждём рыночные данные",
  );
});

test("minute history distinguishes an incomplete or failed response from a loaded window", () => {
  assert.equal(historyStatusText({ status: "PARTIAL", barCount: 12 }), "История M1: загружена частично");
  assert.equal(historyStatusText({ status: "EMPTY", barCount: 0 }), "История M1: в окне нет свечей");
  assert.equal(historyStatusText({ errorLabel: "История M1 недоступна.", barCount: 12 }), "История M1 недоступна.");
  assert.match(historyStatusText({ status: "INVALID", rejected: { ohlcv: 3 } }), /все строки отклонены.*цены или объём.*3/);
  assert.match(streamStatusText({ connected: true, hasRejectedData: true }), /подключён.*некорректные данные/);
});

test("the last REST candle takes precedence over a stale stream quote", () => {
  const candles = [{ quote_time: "2026-11-20T07:12:00Z", close: "105.10" }];
  const staleQuote = { quote_time: "2026-11-20T07:11:55Z", last: "105.03" };
  const currentQuote = { quote_time: "2026-11-20T07:12:04Z", last: "105.20" };
  assert.equal(latestLiveQuote(staleQuote, candles), null);
  assert.equal(latestLiveQuote(currentQuote, candles), currentQuote);
  assert.equal(latestLiveQuote(staleQuote, []), staleQuote);
});

test("recovery merges a repeated candle without duplicates", () => {
  const first = mergeMinuteBars([], [
    { quote_time: "2026-11-20T07:10:00Z", close: "105.10", volume: 10 },
    { quote_time: "2026-11-20T07:11:00Z", close: "105.20", volume: 11 },
  ]);
  // Stream/event rows overwrite the same minute; the series stays unique by quote_time.
  const recovered = mergeMinuteBars(first, [
    { quote_time: "2026-11-20T07:11:00Z", close: "105.25", volume: 12 },
    { quote_time: "2026-11-20T07:12:00Z", close: "105.30", volume: 13 },
  ]);
  assert.deepEqual(recovered.map(row => row.quote_time), [
    "2026-11-20T07:10:00Z",
    "2026-11-20T07:11:00Z",
    "2026-11-20T07:12:00Z",
  ]);
  assert.equal(recovered[1].close, "105.25");
  const afterHistoryRefresh = mergeMinuteBars(recovered, [
    { quote_time: "2026-11-20T07:11:00Z", close: "105.00", volume: 1 },
  ], true);
  assert.equal(afterHistoryRefresh[1].close, "105.25");
  assert.equal(afterHistoryRefresh.length, 3);
});

test("failure stages stay within the safe set", () => {
  assert.equal(failureStage("FINAM_AUTHORIZATION_FAILED"), "authorization");
  assert.equal(failureStage("FINAM_STREAM_UNAVAILABLE"), "connection");
  assert.equal(failureStage("FINAM_STREAM_REJECTED"), "subscription");
  assert.equal(failureStage("FINAM_STREAM_INVALID_DATA"), "data");
  assert.equal(classifyLiveFailure({ status: 401 }, "history").stage, "authorization");
  assert.equal(classifyLiveFailure({ code: "FINAM_NOT_CONFIGURED" }, "stream").code, "FINAM_NOT_CONFIGURED");
});

test("return to 1D drops online status text contract", () => {
  // Period buttons leave live mode before load(); statuses must not keep stream errors.
  assert.equal(
    historyStatusText({ loading: false, errorLabel: "", barCount: 0 }),
    "История M1: в окне нет свечей",
  );
  assert.equal(
    streamStatusText({ connecting: false, connected: false, waiting: false, errorLabel: "" }),
    "Поток: ожидание",
  );
});

const minute = offset => new Date(Date.UTC(2026, 10, 27, 7, offset)).toISOString();
const bar = (offset, close) => ({ quote_time: minute(offset), close: String(close) });

test("each ticker keeps its own state and one rejected subscription does not affect others", () => {
  const shared = { streamConnected: true, streamError: "", history: { barCount: 5 } };
  assert.equal(symbolStatus({ ...shared, streamSeen: true, barCount: 5 }).state, "live");
  assert.equal(symbolStatus({ ...shared, symbolError: "FINAM_STREAM_REJECTED", barCount: 5 }).state, "error");
  assert.equal(symbolStatus({ ...shared, streamSeen: false, barCount: 5 }).label, "Ждём поток");
});

test("a stream break keeps the last data marked as stale", () => {
  const status = symbolStatus({ streamConnected: false, streamError: "Поток прерван", streamSeen: true, barCount: 3 });
  assert.deepEqual(status, { state: "stale", label: "Устарело" });
  assert.equal(symbolStatus({ history: { errorLabel: "x" }, barCount: 0 }).state, "error");
});

test("reconnect history merge removes duplicate minutes", () => {
  const stream = [bar(0, 100), bar(1, 101)];
  const merged = mergeMinuteBars(stream, [bar(1, 101), bar(2, 102), bar(3, 103)], true);
  assert.deepEqual(merged.map(row => row.close), ["100", "101", "102", "103"]);
});

test("index comparison uses only common closed minutes", () => {
  const now = Date.parse(minute(4)) + 30_000;
  const stock = [bar(0, 100), bar(1, 101), bar(2, 102), bar(3, 103), bar(4, 150)];
  const index = [bar(0, 3000), bar(2, 3030), bar(4, 1)];
  assert.equal(closedMinuteBars(stock, now).length, 4);
  const result = alignedChange(stock, index, now);
  assert.equal(result.points, 2);
  assert.equal(result.end, minute(2));
  assert.ok(Math.abs(result.stock - 0.02) < 1e-12);
  assert.ok(Math.abs(result.index - 0.01) < 1e-12);
  assert.equal(alignedChange(stock, [bar(1, 3000)], now), null);
});

test("news feed status is independent of quotes", () => {
  assert.equal(newsFeedText({ status: "NOT_CONFIGURED" }), "Новости: ключ Финама не настроен");
  assert.equal(newsFeedText({ status: "NO_ACCESS" }), "Новости: нет доступа к ленте");
  assert.match(newsFeedText({ status: "OK", truncated: true }), /часть истории/);
  assert.equal(newsFeedText(null), "Новости: загрузка…");
});
