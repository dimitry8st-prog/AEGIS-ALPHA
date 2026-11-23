import { renderChart } from "./chart.js";
import {
  classifyLiveFailure,
  historyStatusText,
  lastCandleTime,
  mergeMinuteBars,
  streamStatusText,
} from "./live.js";
import { readMarketStream } from "./stream.js";

const byId = id => document.getElementById(id);
const api = "/api/v1";
const instruments = {
  GAZP: { name: "Газпром", letter: "Г" },
  GMKN: { name: "Норникель", letter: "Н" },
};
const statuses = {
  READY: ["Готово", "ready"],
  PARTIAL: ["Частично", "partial"],
  FAILED: ["Сбой", "failed"],
  RUNNING: ["В процессе", "running"],
};
const counts = ["inserted_count", "updated_count", "duplicate_count", "quarantined_count", "failed_count"];
const rub = new Intl.NumberFormat("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const signed = new Intl.NumberFormat("ru-RU", { style: "percent", maximumFractionDigits: 2, signDisplay: "exceptZero" });
const moscow = "Europe/Moscow";
const dayLabel = new Intl.DateTimeFormat("ru-RU", { day: "numeric", month: "short", year: "numeric", timeZone: moscow });
const runTime = new Intl.DateTimeFormat("ru-RU", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", timeZone: moscow });
const quoteTime = new Intl.DateTimeFormat("ru-RU", {
  day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit", timeZone: moscow,
});
const calendar = new Intl.DateTimeFormat("sv-SE", { day: "2-digit", month: "2-digit", year: "numeric", timeZone: moscow });
const oneDay = 86_400_000;

let token = null;
let selected = "GAZP";
let days = 30;
let request = null;
let quotes = {};
let runs = [];
let runsTotal = 0;
let loading = false;
let loadError = false;
let registering = false;
let liveMode = false;
let historyRequest = null;
let streamRequest = null;
let liveRetry = null;
let historyLoading = false;
let historyError = "";
let streamConnecting = false;
let streamConnected = false;
let streamWaiting = false;
let streamError = "";
let liveBars = {};
let liveQuotes = {};

function updateNote(message) {
  byId("notice").textContent = message;
  byId("notice").hidden = !message;
}

function toggleMarket(visible) {
  byId("sign-in").hidden = visible;
  byId("market-content").hidden = !visible;
  byId("refresh").hidden = !token;
  byId("logout").hidden = !token;
  byId("mode-label").textContent = visible ? "Данные из базы" : "Данные AEGIS";
}

async function json(url, options = {}) {
  const response = await fetch(url, { ...options, cache: "no-store", redirect: "error" });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw Object.assign(new Error("HTTP " + response.status), {
      status: response.status,
      code: body.error?.code,
    });
  }
  return response.json();
}

function windowDates() {
  const today = calendar.format(new Date());
  const end = Date.parse(today + "T00:00:00Z") + oneDay;
  const isoDate = timestamp => new Date(timestamp).toISOString().slice(0, 10);
  return { start: isoDate(end - days * oneDay), end: isoDate(end) };
}

async function load() {
  request?.abort();
  const current = new AbortController();
  request = current;
  const { start, end } = windowDates();
  if (!token) return;
  quotes = {};
  runs = [];
  runsTotal = 0;
  loading = true;
  loadError = false;
  updateNote("");
  render();
  try {
    const options = { signal: current.signal, headers: { Authorization: "Bearer " + token } };
    const symbols = Object.keys(instruments);
    const query = { provider: "finam", adjustment_mode: "raw" };
    const responses = await Promise.all([
      ...symbols.map(symbol => json(api + "/market/quotes?" + new URLSearchParams({
        ...query, symbol, start, end, limit: "200",
      }), options)),
      json(api + "/market/runs?" + new URLSearchParams({ ...query, limit: "8" }), options),
    ]);
    if (current.signal.aborted) return;
    const runPage = responses.pop();
    quotes = Object.fromEntries(symbols.map((symbol, index) => [symbol, responses[index].items]));
    runs = runPage.items;
    runsTotal = runPage.total;
  } catch (error) {
    if (current.signal.aborted) return;
    if ([401, 403].includes(error.status)) {
      signOut("Сессия закончилась или доступ закрыт. Войдите снова.");
    } else {
      loadError = true;
      updateNote(error.status === 429
        ? "Слишком много запросов. Повторите позже."
        : "Данные пока недоступны. Попробуйте обновить страницу данных.");
    }
  } finally {
    if (request === current) {
      loading = false;
      render();
    }
  }
}

function changeText(node, values, showWhenMissing) {
  const previous = Number(values.at(-2)?.close);
  const latest = Number(values.at(-1)?.close);
  const change = (latest - previous) / previous;
  const available = previous > 0 && Number.isFinite(change);
  node.hidden = !showWhenMissing && !available;
  node.textContent = available ? signed.format(change) : "—";
  node.classList.toggle("positive", available && change > 0);
  node.classList.toggle("negative", available && change < 0);
}

function liveBarCount() {
  return Object.values(liveBars).reduce((total, rows) => total + (rows?.length || 0), 0);
}

function renderLiveStatus() {
  const box = byId("live-status");
  box.hidden = !liveMode;
  if (!liveMode) return;
  byId("live-history-state").textContent = historyStatusText({
    loading: historyLoading,
    errorLabel: historyError,
    barCount: liveBarCount(),
  });
  byId("live-stream-state").textContent = streamStatusText({
    connecting: streamConnecting,
    connected: streamConnected,
    waiting: streamWaiting,
    errorLabel: streamError,
  });
}

function renderQuotes() {
  const quotesNow = (liveMode ? liveBars : quotes)[selected] || [];
  const first = quotesNow[0];
  const last = quotesNow.at(-1);
  const instrument = instruments[selected];
  byId("instrument-name").textContent = instrument.name;
  byId("instrument-symbol").textContent = selected + " · MISX · " + (liveMode ? "M1" : "1D");
  const icon = byId("instrument-icon");
  icon.textContent = instrument.letter;
  icon.className = "instrument-icon " + selected.toLowerCase();
  const tick = liveQuotes[selected];
  const latestPrice = liveMode ? (tick?.last ?? last?.close) : last?.close;
  byId("close-price").textContent = latestPrice ? rub.format(latestPrice) : "—";
  changeText(byId("close-change"), liveMode ? [] : quotesNow, false);
  renderLiveStatus();
  byId("live-mode").setAttribute("aria-pressed", String(liveMode));
  byId("chart-timezone").textContent = liveMode ? "Время свечей · МСК" : "Даты сессий · МСК";
  byId("chart-source").textContent = liveMode ? "Финам · История и поток M1" : "Финам · Дневные свечи";
  byId("chart-source-note").textContent = liveMode
    ? "История M1 загружается через REST независимо от потока. Цена и время котировки обновляются из потока."
    : "Цены без поправок на дивиденды и дробления. Полнота по торговому календарю не проверялась.";
  const candleStamp = lastCandleTime(quotesNow);
  byId("series-period").textContent = liveMode
    ? (tick
      ? "Котировка · " + quoteTime.format(new Date(tick.quote_time)) + " МСК"
      : candleStamp
        ? "Последняя свеча · " + quoteTime.format(new Date(candleStamp)) + " МСК"
        : "Минутные свечи · МСК")
    : (first ? dayLabel.formatRange(new Date(first.quote_time), new Date(last.quote_time))
      : "Сохранённые дневные свечи · МСК");
  for (const button of byId("instruments").querySelectorAll("button")) {
    const symbol = button.dataset.symbol;
    const series = quotes[symbol] || [];
    button.setAttribute("aria-pressed", String(selected === symbol));
    const value = liveMode ? (liveQuotes[symbol]?.last ?? liveBars[symbol]?.at(-1)?.close) : series.at(-1)?.close;
    byId("price-" + symbol).textContent = value ? rub.format(value) + " ₽" : "—";
    const change = byId("change-" + symbol);
    if (liveMode) {
      change.classList.remove("positive", "negative");
      const stamp = liveQuotes[symbol]?.quote_time || lastCandleTime(liveBars[symbol] || []);
      change.textContent = stamp ? quoteTime.format(new Date(stamp)) : "—";
    } else {
      changeText(change, series, true);
    }
  }
  for (const button of byId("period").querySelectorAll("[data-days]")) {
    button.setAttribute("aria-pressed", String(!liveMode && days === Number(button.dataset.days)));
  }
  let message = "В базе нет свечей за выбранный период.";
  if (loading) message = "Загрузка котировок…";
  if (loadError) message = "Котировки недоступны.";
  if (liveMode) {
    if (historyLoading && !quotesNow.length) message = "Загрузка минутных свечей…";
    else if (historyError && !quotesNow.length) message = "История M1 недоступна.";
    else message = "Пока нет минутных свечей в этом окне.";
  }
  renderChart(quotesNow, message, liveMode);
}

function renderRuns() {
  const body = byId("run-rows");
  body.replaceChildren();
  for (const run of runs) {
    const row = body.insertRow();
    row.insertCell().textContent = runTime.format(new Date(run.started_at));
    row.insertCell().textContent = Array.isArray(run.symbols) ? run.symbols.join(", ") : "—";
    const status = document.createElement("span");
    const [label, className] = statuses[run.status] || ["Неизвестно", ""];
    status.className = "run-status " + className;
    status.textContent = label;
    row.insertCell().append(status);
    for (const field of counts) row.insertCell().textContent = run[field];
  }
  const empty = byId("runs-empty");
  empty.textContent = loading ? "Загрузка истории сборов…" : "В базе ещё нет запусков Финама.";
  if (loadError) empty.textContent = "История запусков недоступна.";
  empty.hidden = runs.length > 0;
  byId("run-table").hidden = !runs.length;
  const count = byId("run-count");
  count.hidden = !runs.length;
  count.textContent = "Показано " + runs.length + " из " + runsTotal;
}

function render() {
  renderQuotes();
  renderRuns();
  byId("refresh").disabled = !liveMode && loading;
}

function resetLiveLabels() {
  historyLoading = false;
  historyError = "";
  streamConnecting = false;
  streamConnected = false;
  streamWaiting = false;
  streamError = "";
}

function signOut(message = "") {
  stopLive();
  liveMode = false;
  liveBars = {};
  liveQuotes = {};
  resetLiveLabels();
  request?.abort();
  request = null;
  token = null;
  quotes = {};
  runs = [];
  runsTotal = 0;
  loading = false;
  loadError = false;
  toggleMarket(false);
  render();
  updateNote(message);
  byId("login-form").elements.email.focus();
}

function stopStream() {
  streamRequest?.abort();
  streamRequest = null;
  clearTimeout(liveRetry);
  liveRetry = null;
}

function stopLive() {
  stopStream();
  historyRequest?.abort();
  historyRequest = null;
}

function mergeMinutes(symbol, rows, preferExisting = false) {
  liveBars[symbol] = mergeMinuteBars(liveBars[symbol] || [], rows, preferExisting);
}

async function loadMinuteHistory() {
  if (!token) return;
  historyRequest?.abort();
  const current = new AbortController();
  historyRequest = current;
  historyLoading = true;
  historyError = "";
  renderQuotes();
  const options = { signal: current.signal, headers: { Authorization: "Bearer " + token } };
  const end = Math.floor(Date.now() / 60000) * 60000;
  const start = new Date(end - 120 * 60000).toISOString();
  const results = await Promise.allSettled(Object.keys(instruments).map(async symbol => {
    const query = new URLSearchParams({ symbol, start, end: new Date(end).toISOString() });
    const snapshot = await json(api + "/market/finam/minutes?" + query, options);
    if (!current.signal.aborted) mergeMinutes(symbol, snapshot.items, true);
    return snapshot;
  }));
  if (historyRequest !== current || current.signal.aborted) return;
  historyLoading = false;
  const failure = results.find(result => result.status === "rejected")?.reason;
  if (failure) {
    if ([401, 403].includes(failure.status)) {
      signOut("Сессия закончилась. Войдите снова.");
      return;
    }
    if (!liveBarCount()) {
      historyError = classifyLiveFailure(failure, "history").label;
    }
  } else {
    historyError = "";
  }
  renderQuotes();
}

async function startLive(retryDelay = 3000, { refreshHistory = true } = {}) {
  if (!token) return;
  liveMode = true;
  stopStream();
  streamConnecting = true;
  streamConnected = false;
  streamWaiting = false;
  streamError = "";
  if (refreshHistory) void loadMinuteHistory();
  const current = new AbortController();
  streamRequest = current;
  const options = { signal: current.signal, headers: { Authorization: "Bearer " + token } };
  render();
  try {
    for await (const event of readMarketStream(api + "/market/finam/stream", options)) {
      if (current.signal.aborted) return;
      if (event.type === "connected") {
        streamConnecting = false;
        streamConnected = true;
        streamWaiting = true;
        streamError = "";
        // Intentional history refresh after reconnect; stream abort never cancels it.
        void loadMinuteHistory();
      } else if (event.type === "error") {
        throw Object.assign(new Error("Finam stream error"), {
          code: event.code,
          stage: event.stage,
        });
      } else if (event.type === "quote" || event.type === "bar") {
        retryDelay = 3000;
        streamConnecting = false;
        streamConnected = true;
        streamWaiting = false;
        streamError = "";
        if (event.type === "quote") {
          const previous = liveQuotes[event.symbol];
          if (!previous || Date.parse(event.quote_time) >= Date.parse(previous.quote_time)) {
            liveQuotes[event.symbol] = event;
          }
        } else {
          mergeMinutes(event.symbol, [event]);
        }
      } else {
        continue;
      }
      renderQuotes();
    }
  } catch (error) {
    if (current.signal.aborted) return;
    if ([401, 403].includes(error.status)) {
      signOut("Сессия закончилась. Войдите снова.");
      return;
    }
    const failure = classifyLiveFailure(error, "stream");
    streamConnecting = false;
    streamConnected = false;
    streamWaiting = false;
    streamError = failure.label;
    renderQuotes();
    if (failure.code === "FINAM_NOT_CONFIGURED" || failure.code === "FINAM_AUTHORIZATION_FAILED") {
      return;
    }
  }
  if (current.signal.aborted || streamRequest !== current) return;
  if (!streamError) {
    streamConnecting = false;
    streamConnected = false;
    streamWaiting = false;
    streamError = "Поток прерван · переподключение…";
  }
  renderQuotes();
  liveRetry = setTimeout(
    () => startLive(Math.min(retryDelay * 2, 30000), { refreshHistory: false }),
    retryDelay,
  );
}

const loginForm = byId("login-form");

function setRegistrationMode(enabled) {
  registering = enabled;
  byId("sign-in-title").textContent = enabled ? "Создать учётную запись" : "Вход в AEGIS";
  byId("sign-in-description").textContent = enabled
    ? "Учётная запись для этого экземпляра AEGIS. Используйте пароль от 12 символов."
    : "Войдите под своей учётной записью AEGIS, чтобы увидеть сохранённые котировки.";
  byId("login-mode").textContent = enabled ? "Вернуться ко входу" : "Создать учётную запись";
  loginForm.querySelector('[type="submit"]').textContent = enabled ? "Создать и войти" : "Войти";
  loginForm.elements.password.autocomplete = enabled ? "new-password" : "current-password";
  loginForm.elements.password.minLength = enabled ? 12 : 0;
  byId("login-error").hidden = true;
}

byId("login-mode").addEventListener("click", () => {
  setRegistrationMode(!registering);
  loginForm.elements.password.value = "";
  loginForm.elements.email.focus();
});

byId("login-form").addEventListener("submit", async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const submit = form.querySelector("button");
  const error = byId("login-error");
  const payload = { email: form.elements.email.value.trim(), password: form.elements.password.value };
  if (registering && new TextEncoder().encode(payload.password).length > 72) {
    error.textContent = "Сократите пароль до 72 байт.";
    error.hidden = false;
    return;
  }
  form.elements.password.value = "";
  submit.disabled = true;
  byId("login-mode").disabled = true;
  error.hidden = true;
  try {
    const options = {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    };
    if (registering) {
      await json(api + "/auth/register", options);
      setRegistrationMode(false);
    }
    const result = await json(api + "/auth/login", options);
    if (typeof result.access_token !== "string" || !result.access_token) throw new Error("Missing session");
    token = result.access_token;
    toggleMarket(true);
    byId("quotes").querySelector('[data-days="30"]').focus({ preventScroll: true });
    await load();
  } catch (failure) {
    error.textContent = {
      400: "Учётная запись с этой почтой уже существует. Вернитесь ко входу.",
      401: "Проверьте адрес почты и пароль.",
      403: "У этой учётной записи нет доступа.",
      422: "Проверьте формат почты и пароля.",
      429: "Слишком много запросов. Повторите позже.",
    }[failure.status] || "Вход сейчас недоступен. Повторите позже.";
    error.hidden = false;
  } finally {
    submit.disabled = false;
    byId("login-mode").disabled = false;
  }
});

byId("logout").addEventListener("click", () => signOut());
byId("refresh").addEventListener("click", () => liveMode ? startLive(3000, { refreshHistory: true }) : load());
byId("live-mode").addEventListener("click", () => startLive(3000, { refreshHistory: true }));
for (const button of byId("instruments").querySelectorAll("button")) {
  button.addEventListener("click", () => {
    selected = button.dataset.symbol;
    renderQuotes();
  });
}
for (const button of byId("period").querySelectorAll("[data-days]")) {
  button.addEventListener("click", () => {
    const next = Number(button.dataset.days);
    if (days === next && !liveMode) return;
    stopLive();
    liveMode = false;
    liveBars = {};
    liveQuotes = {};
    resetLiveLabels();
    days = next;
    load();
  });
}

byId("login-form").hidden = false;
