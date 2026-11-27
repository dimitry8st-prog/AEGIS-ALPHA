import { renderChart } from "./chart.js";
import {
  alignedChange,
  classifyLiveFailure,
  EVENT_LABELS,
  historyStatusText,
  lastCandleTime,
  latestLiveQuote,
  mergeMinuteBars,
  newsFeedText,
  streamStatusText,
  symbolStatus,
} from "./live.js";
import { readMarketStream } from "./stream.js";

const byId = id => document.getElementById(id);
const api = "/api/v1";
// Filled from GET /market/instruments: the server registry is the only list.
let instruments = {};
let contextInstruments = {};
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
const minuteLabel = new Intl.DateTimeFormat("ru-RU", { hour: "2-digit", minute: "2-digit", timeZone: moscow });
const calendar = new Intl.DateTimeFormat("sv-SE", { day: "2-digit", month: "2-digit", year: "numeric", timeZone: moscow });
const oneDay = 86_400_000;
const minuteMs = 60_000;

let token = null;
let selected = null;
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
let historyRefresh = null;
let liveRetry = null;
let historyBySymbol = {};
let streamConnecting = false;
let streamConnected = false;
let streamWaiting = false;
let streamError = "";
let streamRejectedData = false;
let liveBars = {};
let liveQuotes = {};
let streamSeen = {};
let symbolErrors = {};
let newsTimer = null;
let newsRequest = null;
let news = null;

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

function instrumentRow(symbol, instrument) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "instrument-row";
  button.dataset.symbol = symbol;
  const icon = document.createElement("span");
  icon.className = "instrument-icon " + symbol.toLowerCase();
  icon.setAttribute("aria-hidden", "true");
  icon.textContent = instrument.letter;
  const id = document.createElement("span");
  id.className = "instrument-id";
  const code = document.createElement("strong");
  code.textContent = symbol;
  const name = document.createElement("small");
  name.textContent = instrument.name;
  id.append(code, name);
  const value = document.createElement("span");
  value.className = "instrument-value";
  const price = document.createElement("strong");
  price.id = "price-" + symbol;
  price.textContent = "—";
  const change = document.createElement("small");
  change.id = "change-" + symbol;
  change.textContent = "—";
  value.append(price, change);
  button.append(icon, id, value);
  return button;
}

async function loadRegistry() {
  const registry = await json(api + "/market/instruments", { headers: { Authorization: "Bearer " + token } });
  instruments = Object.fromEntries(registry.watchlist.map(item => [item.symbol, item]));
  contextInstruments = Object.fromEntries(registry.market_context.map(item => [item.symbol, item]));
  if (!instruments[selected]) selected = registry.watchlist[0]?.symbol ?? null;
  const list = byId("instruments");
  list.replaceChildren(...registry.watchlist.map(item => {
    const button = instrumentRow(item.symbol, item);
    button.addEventListener("click", () => {
      selected = item.symbol;
      renderQuotes();
    });
    return button;
  }));
  const rows = byId("context-rows");
  rows.replaceChildren(...registry.market_context.map(item => {
    const row = instrumentRow(item.symbol, item);
    row.disabled = true;
    row.classList.add("context-row");
    return row;
  }));
  byId("market-context").hidden = !registry.market_context.length;
  byId("news-scope").textContent = registry.watchlist.map(item => item.symbol).join(", ") + " · время МСК";
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

function renderLiveStatus() {
  const box = byId("live-status");
  box.hidden = !liveMode;
  if (!liveMode) return;
  byId("live-history-state").textContent = historyStatusText(historyBySymbol[selected]);
  byId("live-stream-state").textContent = symbolErrors[selected]
    ? `Поток ${selected}: Финам отклонил подписку (${symbolErrors[selected]}). Остальные бумаги обновляются.`
    : streamStatusText({
      connecting: streamConnecting,
      connected: streamConnected,
      waiting: streamWaiting,
      errorLabel: streamError,
      hasRejectedData: streamRejectedData,
    });
  const [indexSymbol] = Object.keys(contextInstruments);
  const compare = indexSymbol ? alignedChange(liveBars[selected], liveBars[indexSymbol], Date.now()) : null;
  byId("live-index-compare").textContent = !indexSymbol ? "" : compare
    ? `${selected} ${signed.format(compare.stock)} · ${indexSymbol} ${signed.format(compare.index)} · `
      + `закрытые минуты ${minuteLabel.format(new Date(compare.start))}–${minuteLabel.format(new Date(compare.end))} МСК`
    : `Сравнение с ${indexSymbol}: нет совпадающих закрытых минут.`;
}

function rowStatus(symbol, series) {
  return symbolStatus({
    symbolError: symbolErrors[symbol],
    history: historyBySymbol[symbol],
    streamConnected,
    streamError,
    streamSeen: Boolean(streamSeen[symbol]),
    barCount: series.length,
  });
}

function renderRow(symbol) {
  const series = (liveMode ? liveBars : quotes)[symbol] || [];
  const price = byId("price-" + symbol);
  const change = byId("change-" + symbol);
  if (!price || !change) return;
  const quote = liveMode ? latestLiveQuote(liveQuotes[symbol], series) : null;
  const value = quote?.last ?? series.at(-1)?.close;
  price.textContent = value ? rub.format(value) + (contextInstruments[symbol] ? "" : " ₽") : "—";
  if (liveMode) {
    change.classList.remove("positive", "negative");
    const status = rowStatus(symbol, series);
    const stamp = quote?.quote_time || lastCandleTime(series);
    change.dataset.state = status.state;
    change.textContent = status.label + (stamp ? " · " + quoteTime.format(new Date(stamp)) : "");
  } else if (contextInstruments[symbol]) {
    delete change.dataset.state;
    change.textContent = "в режиме «Онлайн · M1»";
  } else {
    delete change.dataset.state;
    changeText(change, series, true);
  }
}

function renderContext() {
  for (const symbol of Object.keys(contextInstruments)) renderRow(symbol);
  const unverified = Object.values(contextInstruments).filter(item => item.verification !== "CONFIGURED");
  byId("context-note").textContent = unverified.length
    ? "Идентификатор индекса подтверждается живой проверкой Финама. Сравнение — только по совпадающим закрытым минутам."
    : "Сравнение — только по совпадающим закрытым минутам.";
}

function renderQuotes() {
  const quotesNow = (liveMode ? liveBars : quotes)[selected] || [];
  const first = quotesNow[0];
  const last = quotesNow.at(-1);
  const instrument = instruments[selected];
  byId("instrument-name").textContent = instrument?.name ?? "—";
  byId("instrument-symbol").textContent = (selected ? selected + " · " : "") + "MISX · " + (liveMode ? "M1" : "1D");
  const icon = byId("instrument-icon");
  icon.textContent = instrument?.letter ?? "";
  icon.className = "instrument-icon " + (selected ?? "").toLowerCase();
  const tick = latestLiveQuote(liveQuotes[selected], quotesNow);
  const latestPrice = liveMode ? (tick?.last ?? last?.close) : last?.close;
  byId("close-price").textContent = latestPrice ? rub.format(latestPrice) : "—";
  changeText(byId("close-change"), liveMode ? [] : quotesNow, false);
  renderLiveStatus();
  byId("live-mode").setAttribute("aria-pressed", String(liveMode));
  byId("chart-timezone").textContent = liveMode ? "Время свечей · МСК" : "Даты сессий · МСК";
  byId("chart-source").textContent = liveMode ? "Финам · История и поток M1" : "Финам · Дневные свечи";
  byId("chart-source-note").textContent = liveMode
    ? "История M1 обновляется каждую минуту. Когда поток недоступен, панель показывает цену последней полученной свечи."
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
    button.setAttribute("aria-pressed", String(selected === button.dataset.symbol));
    renderRow(button.dataset.symbol);
  }
  renderContext();
  for (const button of byId("period").querySelectorAll("[data-days]")) {
    button.setAttribute("aria-pressed", String(!liveMode && days === Number(button.dataset.days)));
  }
  let message = "В базе нет свечей за выбранный период.";
  if (loading) message = "Загрузка котировок…";
  if (loadError) message = "Котировки недоступны.";
  if (liveMode) {
    const history = historyBySymbol[selected];
    if (history?.loading) message = "Загрузка минутных свечей…";
    else if (history?.errorLabel || history?.status === "INVALID") message = "История M1 недоступна.";
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

function renderNews() {
  byId("news-feed").textContent = newsFeedText(news?.feed);
  const events = byId("news-events");
  events.replaceChildren(...(news?.events || []).filter(event => event.status !== "NO_SIGNAL").map(event => {
    const item = document.createElement("li");
    item.className = "news-event " + event.status.toLowerCase();
    const latest = event.transitions.at(-1);
    item.textContent = `${event.symbol} · ${EVENT_LABELS[event.event_type] || event.event_type} · `
      + `${EVENT_LABELS[event.status]} · ${latest.source} · доступно ${quoteTime.format(new Date(latest.available_at))}`;
    return item;
  }));
  const items = byId("news-items");
  items.replaceChildren(...(news?.items || []).filter(row => !row.deleted).slice(0, 12).map(row => {
    const item = document.createElement("li");
    const meta = document.createElement("small");
    const version = row.version && row.version !== "0" ? ` · версия ${row.version}` : "";
    meta.textContent = `${quoteTime.format(new Date(row.published_at))} · ${row.symbols.join(", ")} · ${row.source}${version}`;
    const title = document.createElement("span");
    title.textContent = row.title;
    item.append(meta, title);
    return item;
  }));
  const empty = byId("news-empty");
  empty.hidden = items.children.length > 0;
  empty.textContent = !news ? "Загрузка новостей…"
    : news.feed.status === "OK" ? "За последние сутки новостей по бумагам нет."
      : "Лента новостей недоступна. Котировки работают независимо от неё.";
}

async function loadNews() {
  if (!token || newsRequest) return;
  const current = new AbortController();
  newsRequest = current;
  try {
    news = await json(api + "/market/finam/news?hours=24", {
      signal: current.signal, headers: { Authorization: "Bearer " + token },
    });
  } catch (error) {
    if (current.signal.aborted) return;
    if ([401, 403].includes(error.status)) {
      signOut("Сессия закончилась. Войдите снова.");
      return;
    }
    news = { feed: { status: error.status === 429 ? "RATE_LIMITED" : "UNAVAILABLE" }, items: [], events: [] };
  } finally {
    if (newsRequest === current) newsRequest = null;
  }
  renderNews();
}

function stopNews() {
  clearInterval(newsTimer);
  newsTimer = null;
  newsRequest?.abort();
  newsRequest = null;
  news = null;
}

function render() {
  renderQuotes();
  renderNews();
  renderRuns();
  byId("refresh").disabled = !liveMode && loading;
}

function resetLiveLabels() {
  historyBySymbol = {};
  streamConnecting = false;
  streamConnected = false;
  streamWaiting = false;
  streamError = "";
  streamRejectedData = false;
  streamSeen = {};
  symbolErrors = {};
}

function signOut(message = "") {
  stopLive();
  stopNews();
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
  clearInterval(historyRefresh);
  historyRefresh = null;
  historyRequest?.abort();
  historyRequest = null;
}

function mergeMinutes(symbol, rows, preferExisting = false) {
  liveBars[symbol] = mergeMinuteBars(liveBars[symbol] || [], rows, preferExisting);
}

async function loadMinuteHistory() {
  if (!token || !liveMode || historyRequest) return;
  const current = new AbortController();
  historyRequest = current;
  const symbols = [...Object.keys(instruments), ...Object.keys(contextInstruments)];
  for (const symbol of symbols) historyBySymbol[symbol] ??= { loading: true };
  renderQuotes();
  const options = { signal: current.signal, headers: { Authorization: "Bearer " + token } };
  const end = Math.floor(Date.now() / minuteMs) * minuteMs;
  const start = new Date(end - 120 * minuteMs).toISOString();
  try {
    await Promise.all(symbols.map(async symbol => {
      const query = new URLSearchParams({ symbol, start, end: new Date(end).toISOString() });
      try {
        const snapshot = await json(api + "/market/finam/minutes?" + query, options);
        if (current.signal.aborted) return;
        mergeMinutes(symbol, snapshot.items, true);
        historyBySymbol[symbol] = {
          barCount: snapshot.items.length, status: snapshot.status, rejected: snapshot.rejected,
        };
      } catch (error) {
        if (current.signal.aborted) return;
        if ([401, 403].includes(error.status)) {
          signOut("Сессия закончилась. Войдите снова.");
          return;
        }
        const failure = classifyLiveFailure(error, "history");
        const detail = error.code ? ` (${failure.code})` : "";
        historyBySymbol[symbol] = { errorLabel: failure.label + detail };
      }
      renderQuotes();
    }));
  } finally {
    if (historyRequest === current) historyRequest = null;
  }
}

async function startLive(retryDelay = 3000, { refreshHistory = true } = {}) {
  if (!token) return;
  liveMode = true;
  stopStream();
  streamConnecting = true;
  streamConnected = false;
  streamWaiting = false;
  streamError = "";
  streamRejectedData = false;
  if (refreshHistory) {
    void loadMinuteHistory();
    historyRefresh ??= setInterval(loadMinuteHistory, minuteMs);
  }
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
        symbolErrors = {};
        // After a break the REST window fills the missed closed minutes; merging drops repeats.
        if (!refreshHistory) void loadMinuteHistory();
      } else if (event.type === "symbol_error") {
        symbolErrors[event.symbol] = event.code;
      } else if (event.type === "data_warning") {
        streamRejectedData = true;
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
        streamSeen[event.symbol] = Date.now();
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
    try {
      await loadRegistry();
    } catch (registryError) {
      if ([401, 403].includes(registryError.status)) {
        signOut("Сессия закончилась или доступ закрыт. Войдите снова.");
      } else {
        updateNote("Список инструментов недоступен. Обновите страницу позже.");
      }
      return;
    }
    void loadNews();
    newsTimer ??= setInterval(loadNews, minuteMs);
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
