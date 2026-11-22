const chart = document.querySelector("#chart");
const context = chart.getContext("2d");
const dateShort = new Intl.DateTimeFormat("ru-RU", { day: "numeric", month: "short", timeZone: "Europe/Moscow" });
const dateLong = new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium", timeZone: "Europe/Moscow" });
const timeShort = new Intl.DateTimeFormat("ru-RU", { hour: "2-digit", minute: "2-digit", timeZone: "Europe/Moscow" });
const timeLong = new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium", timeStyle: "short", timeZone: "Europe/Moscow" });
const price = new Intl.NumberFormat("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const number = new Intl.NumberFormat("ru-RU");

let rows = [];
let intraday = false;
let lastMessage = null;
let active = -1;
let hitArea = { left: 0, step: 1 };

function priceScale(low, high) {
  const span = high - low || Math.max(high * 0.02, 0.02);
  const precision = 10 ** Math.floor(Math.log10(span / 4));
  const step = ([1, 2, 2.5, 5, 10].find(level => level * precision >= span / 4) || 10) * precision;
  return { step, min: Math.floor((low - span * 0.04) / step) * step, max: Math.ceil((high + span * 0.04) / step) * step };
}

function draw() {
  if (!rows.length || !context || chart.hidden) return;
  const { width, height } = chart.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  chart.width = Math.round(width * ratio);
  chart.height = Math.round(height * ratio);
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.font = '11px "Segoe UI", system-ui, sans-serif';

  const left = 4;
  const top = 13;
  const axisWidth = Math.max(56, context.measureText(price.format(Math.max(...rows.map(row => Number(row.high))))).width + 14);
  const right = width - axisWidth;
  const priceBottom = height - 67;
  const volumeTop = height - 56;
  const volumeBottom = height - 26;
  const scale = priceScale(Math.min(...rows.map(row => Number(row.low))), Math.max(...rows.map(row => Number(row.high))));
  const maxVolume = Math.max(1, ...rows.map(row => row.volume));
  const step = (right - left) / rows.length;
  const bodyWidth = Math.min(13, Math.max(1, step * 0.66));
  const y = value => top + (scale.max - value) / (scale.max - scale.min) * (priceBottom - top);
  const x = index => left + (index + 0.5) * step;
  hitArea = { left, step };

  const ticks = Math.round((scale.max - scale.min) / scale.step);
  for (let index = 0; index <= ticks; index++) {
    const value = scale.min + scale.step * index;
    const lineY = y(value);
    line(left, lineY, right, lineY, "#e5ece6");
    context.fillStyle = "#51675d";
    context.textAlign = "left";
    context.fillText(price.format(value), right + 8, lineY + 4);
  }
  line(right, top, right, volumeBottom, "#e5ece6");
  line(left, volumeBottom, right, volumeBottom, "#e5ece6");

  const labelCount = Math.min(rows.length, Math.max(2, Math.floor((right - left) / 93)));
  for (let label = 0; label < labelCount; label++) {
    const index = labelCount === 1 ? 0 : Math.round(label * (rows.length - 1) / (labelCount - 1));
    const labelX = x(index);
    line(labelX, top, labelX, volumeBottom, "#edf1ec");
    context.textAlign = "center";
    context.fillStyle = "#51675d";
    const text = (intraday ? timeShort : dateShort).format(new Date(rows[index].quote_time));
    const half = context.measureText(text).width * 0.5;
    context.fillText(text, Math.max(left + half, Math.min(right - half, labelX)), height - 10);
  }

  rows.forEach((row, index) => {
    const center = x(index);
    const rising = Number(row.close) >= Number(row.open);
    const color = rising ? "#188464" : "#c95459";
    const open = y(row.open);
    const close = y(row.close);
    line(center, y(row.high), center, y(row.low), color);
    context.fillStyle = color;
    context.fillRect(center - bodyWidth / 2, Math.min(open, close), bodyWidth, Math.max(1.3, Math.abs(open - close)));
    const volumeHeight = row.volume / maxVolume * (volumeBottom - volumeTop);
    context.fillStyle = rising ? "#b4d4c4" : "#e8bdbd";
    context.fillRect(center - bodyWidth / 2, volumeBottom - volumeHeight, bodyWidth, volumeHeight);
  });

  if (active !== -1) {
    const selectedX = x(active);
    context.setLineDash([4, 4]);
    line(selectedX, top, selectedX, volumeBottom, "#365b4b");
    context.setLineDash([]);
  }
}

function line(x1, y1, x2, y2, color) {
  context.beginPath();
  context.strokeStyle = color;
  context.lineWidth = 1;
  context.moveTo(x1, y1);
  context.lineTo(x2, y2);
  context.stroke();
}

function showBar() {
  const row = rows[active === -1 ? rows.length - 1 : active];
  if (!row) return;
  const timestamp = document.querySelector("#plot-date");
  timestamp.dateTime = row.quote_time;
  timestamp.textContent = (intraday ? timeLong : dateLong).format(new Date(row.quote_time));
  for (const field of ["open", "high", "low", "close", "volume"]) {
    const format = field === "volume" ? number : price;
    document.querySelector("#bar-" + field).textContent = format.format(row[field]);
  }
}

function selectBar(index) {
  if (active === index || !rows.length) return;
  active = index;
  showBar();
  draw();
}

chart.addEventListener("pointermove", event => {
  const index = Math.floor((event.clientX - chart.getBoundingClientRect().left - hitArea.left) / hitArea.step);
  selectBar(index >= 0 && index < rows.length ? index : -1);
});
chart.addEventListener("pointerleave", () => selectBar(-1));
chart.addEventListener("blur", () => selectBar(-1));
chart.addEventListener("keydown", event => {
  const direction = { ArrowLeft: -1, ArrowRight: 1 }[event.key];
  if (!direction || !rows.length) return;
  event.preventDefault();
  const index = active === -1 ? rows.length - 1 : active;
  selectBar(Math.max(0, Math.min(rows.length - 1, index + direction)));
});
new ResizeObserver(draw).observe(chart);

export function renderChart(quotes, message, minuteBars = false) {
  if (rows === quotes && intraday === minuteBars && lastMessage === message) return;
  intraday = minuteBars;
  lastMessage = message;
  rows = quotes;
  active = -1;
  const hasRows = rows.length > 0;
  chart.hidden = !hasRows;
  document.querySelector("#plot-heading").hidden = !hasRows;
  const empty = document.querySelector("#chart-empty");
  empty.hidden = hasRows;
  empty.textContent = message;
  showBar();
  draw();
}
