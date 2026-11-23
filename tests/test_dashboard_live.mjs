import assert from "node:assert/strict";
import test from "node:test";

import {
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
