/** Pure helpers for Online · M1: history/stream status and minute merge. */

export const LIVE_STAGES = Object.freeze({
  authorization: "authorization",
  connection: "connection",
  subscription: "subscription",
  data: "data",
});

const STAGE_BY_CODE = Object.freeze({
  FINAM_NOT_CONFIGURED: LIVE_STAGES.authorization,
  FINAM_AUTHORIZATION_FAILED: LIVE_STAGES.authorization,
  FINAM_INVALID_AUTH_RESPONSE: LIVE_STAGES.authorization,
  FINAM_STREAM_UNAVAILABLE: LIVE_STAGES.connection,
  FINAM_NETWORK_ERROR: LIVE_STAGES.connection,
  FINAM_SOURCE_TRANSIENT: LIVE_STAGES.connection,
  FINAM_STREAM_REJECTED: LIVE_STAGES.subscription,
  FINAM_STREAM_INVALID_DATA: LIVE_STAGES.data,
  FINAM_INVALID_M1_BAR: LIVE_STAGES.data,
  FINAM_INVALID_RESPONSE: LIVE_STAGES.data,
});

const STAGE_LABEL = Object.freeze({
  authorization: "авторизация",
  connection: "подключение",
  subscription: "подписка",
  data: "получение данных",
});

const CODE_HINT = Object.freeze({
  FINAM_NOT_CONFIGURED: "Запустите AEGIS с ключом Финама «Просмотр».",
  FINAM_AUTHORIZATION_FAILED: "Финам отклонил доступ к рыночным данным.",
});

export function failureStage(code, status) {
  if (STAGE_BY_CODE[code]) return STAGE_BY_CODE[code];
  if (status === 401 || status === 403) return LIVE_STAGES.authorization;
  if (status === 429 || status === 502 || status === 503 || status === 504) {
    return LIVE_STAGES.connection;
  }
  return LIVE_STAGES.connection;
}

export function classifyLiveFailure(error = {}, source = "stream") {
  const code = typeof error.code === "string" && error.code ? error.code : "FINAM_LIVE_FAILED";
  const stage = error.stage && STAGE_LABEL[error.stage]
    ? error.stage
    : failureStage(code, error.status);
  const hint = CODE_HINT[code];
  const where = STAGE_LABEL[stage] || STAGE_LABEL.connection;
  const prefix = source === "history" ? "История M1" : "Поток";
  return {
    code,
    stage,
    label: hint || `${prefix}: сбой на этапе «${where}».`,
  };
}

export function mergeMinuteBars(prior = [], rows = [], preferExisting = false) {
  // Later entries win. History refresh keeps stream bars; stream events overwrite REST.
  const combined = preferExisting ? [...rows, ...prior] : [...prior, ...rows];
  const deduplicated = new Map(combined.map(row => [Date.parse(row.quote_time), row]));
  return [...deduplicated.values()]
    .sort((a, b) => Date.parse(a.quote_time) - Date.parse(b.quote_time))
    .slice(-120);
}

const rejectionLabels = {
  outside_window: "за пределами окна", timestamp: "метка времени", ohlcv: "цены или объём",
  duplicate: "повтор", conflict: "расхождение за одну минуту",
};

export function historyStatusText({ loading, errorLabel, barCount, status, rejected } = {}) {
  if (loading) return "История M1: загрузка…";
  if (errorLabel) return errorLabel;
  const reasons = Object.entries(rejectionLabels).filter(([key]) => rejected?.[key] > 0)
    .map(([key, label]) => `${label} — ${rejected[key]}`);
  const details = reasons.length ? ` (${reasons.join(", ")})` : "";
  if (status === "INVALID") return `История M1: все строки отклонены${details}`;
  if (status === "PARTIAL" && barCount > 0) return `История M1: загружена частично${details}`;
  if (barCount > 0) return "История M1: загружена";
  return "История M1: в окне нет свечей";
}

export function streamStatusText({ connecting, connected, waiting, errorLabel, hasRejectedData }) {
  if (errorLabel) return errorLabel;
  if (connecting) return "Поток: подключение…";
  if (connected && hasRejectedData) return "Поток: подключён · пропущены некорректные данные";
  if (connected && waiting) return "Поток: подключён · ждём рыночные данные";
  if (connected) return "Поток: подключён";
  return "Поток: ожидание";
}

export function lastCandleTime(bars = []) {
  const last = bars.at(-1);
  return last?.quote_time || null;
}

export function latestLiveQuote(quote, bars) {
  const candleTime = lastCandleTime(bars);
  if (quote && (!candleTime || Date.parse(quote.quote_time) >= Date.parse(candleTime))) return quote;
  return null;
}
