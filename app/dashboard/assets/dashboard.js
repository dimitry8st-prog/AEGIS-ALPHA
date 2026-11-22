import { renderChart } from "./chart.js";
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
let liveRequest = null;
let liveRetry = null;
let liveLabel = "";
let liveBars = {};
let liveQuotes = {};
const liveErrors = {
  FINAM_NOT_CONFIGURED: "Запустите AEGIS с ключом Финама «Просмотр».",
  FINAM_AUTHORIZATION_FAILED: "Финам отклонил доступ к рыночному потоку.",
};

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
  if (!response.ok) throw Object.assign(new Error("HTTP " + response.status), { status: response.status });
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
  const state = byId("live-state");
  state.hidden = !liveMode;
  if (state.textContent !== liveLabel) state.textContent = liveLabel;
  byId("live-mode").setAttribute("aria-pressed", String(liveMode));
  byId("chart-timezone").textContent = liveMode ? "Время свечей · МСК" : "Даты сессий · МСК";
  byId("chart-source").textContent = liveMode ? "Финам · Поток M1" : "Финам · Дневные свечи";
  byId("chart-source-note").textContent = liveMode
    ? "Цена и время последней полученной котировки. Минутная свеча обновляется до закрытия."
    : "Цены без поправок на дивиденды и дробления. Полнота по торговому календарю не проверялась.";
  byId("series-period").textContent = liveMode
    ? (tick ? "Котировка · " + quoteTime.format(new Date(tick.quote_time)) + " МСК" : "Минутные свечи · МСК")
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
      change.textContent = liveQuotes[symbol] ? quoteTime.format(new Date(liveQuotes[symbol].quote_time)) : "—";
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
  renderChart(quotesNow, liveMode ? "Пока нет минутных свечей в этом окне." : message, liveMode);
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

function signOut(message = "") {
  stopLive();
  liveMode = false;
  liveBars = {};
  liveQuotes = {};
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

function stopLive() {
  liveRequest?.abort();
  liveRequest = null;
  clearTimeout(liveRetry);
}

function mergeMinutes(symbol, rows, preferStream = false) {
  const prior = liveBars[symbol] || [];
  const combined = preferStream ? [...rows, ...prior] : [...prior, ...rows];
  const deduplicated = new Map(combined.map(row => [Date.parse(row.quote_time), row]));
  liveBars[symbol] = [...deduplicated.values()]
    .sort((a, b) => Date.parse(a.quote_time) - Date.parse(b.quote_time)).slice(-120);
}

async function refreshMinuteHistory(options) {
  const end = Math.floor(Date.now() / 60000) * 60000;
  const start = new Date(end - 120 * 60000).toISOString();
  await Promise.allSettled(Object.keys(instruments).map(async symbol => {
    const query = new URLSearchParams({ symbol, start, end: new Date(end).toISOString() });
    const snapshot = await json(api + "/market/finam/minutes?" + query, options);
    if (!options.signal.aborted) mergeMinutes(symbol, snapshot.items, true);
  }));
  if (!options.signal.aborted) renderQuotes();
}

async function startLive(retryDelay = 3000) {
  if (!token) return;
  stopLive();
  liveMode = true;
  const current = new AbortController();
  liveRequest = current;
  const options = { signal: current.signal, headers: { Authorization: "Bearer " + token } };
  liveLabel = "Подключение к Финаму…";
  render();
  try {
    for await (const event of readMarketStream(api + "/market/finam/stream", options)) {
      if (current.signal.aborted) return;
      if (event.type === "connected") {
        liveLabel = "Поток подключён · ждём рыночные данные";
        void refreshMinuteHistory(options);
      } else if (event.type === "error") {
        throw Object.assign(new Error("Finam stream error"), { code: event.code });
      } else if (event.type === "quote" || event.type === "bar") {
        retryDelay = 3000;
        liveLabel = "Поток Финама подключён";
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
    if (liveErrors[error.code]) {
      liveLabel = liveErrors[error.code];
      renderQuotes();
      return;
    }
  }
  if (current.signal.aborted) return;
  liveLabel = "Поток прерван · переподключение…";
  renderQuotes();
  liveRetry = setTimeout(() => startLive(Math.min(retryDelay * 2, 30000)), retryDelay);
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
byId("refresh").addEventListener("click", () => liveMode ? startLive() : load());
byId("live-mode").addEventListener("click", () => startLive());
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
    days = next;
    load();
  });
}

byId("login-form").hidden = false;
