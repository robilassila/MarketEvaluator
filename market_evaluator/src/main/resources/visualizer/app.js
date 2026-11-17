const form = document.querySelector("#subscribe-form");
const symbolInput = document.querySelector("#symbol-input");
const symbolPicker = document.querySelector(".symbol-picker");
const symbolOptions = document.querySelector("#symbol-options");
const formHint = document.querySelector("#form-hint");
const feedState = document.querySelector(".feed-state");
const feedStateText = document.querySelector("#feed-state");
const chartTitle = document.querySelector("#chart-title");
const pointCount = document.querySelector("#point-count");
const chart = document.querySelector("#price-chart");
const chartEmpty = document.querySelector("#chart-empty");
const latestUpdate = document.querySelector("#latest-update");
const advisoryCount = document.querySelector("#advisory-count");
const advisorySummary = document.querySelector("#advisory-summary");
const advisoryRows = document.querySelector("#advisory-rows");
const advisoryEmpty = document.querySelector("#advisory-empty");
const exchangeTime = (timestamp, options) => new Intl.DateTimeFormat([], {
  timeZone: "Etc/GMT-2",
  ...options,
}).format(new Date(timestamp));

const indicators = new Map();
const advisories = new Map();
const MAX_SYMBOL_OPTIONS = 100;
let availableSymbols = [];
let matchingSymbols = [];
let activeOption = -1;
let symbolOptionsOpen = false;
let activeSymbol = "";
let stream;

const svgElement = (name, attributes = {}) => {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  return element;
};

function setFeedState(state, message) {
  feedState.dataset.state = state;
  feedStateText.textContent = message;
}

function setHint(message, error = false) {
  formHint.textContent = message;
  formHint.dataset.error = String(error);
}

async function loadSymbols() {
  try {
    const response = await fetch("/api/symbols");
    if (!response.ok) throw new Error(`Symbol request failed (${response.status})`);
    availableSymbols = await response.json();
    renderSymbolOptions();
    if (!symbolInput.value.trim()) {
      setHint(availableSymbols.length
        ? `${availableSymbols.length} symbols available. Type to filter, or enter a symbol.`
        : "Enter a full symbol ending in .FR, .NL or .ETR.");
    }
  } catch (error) {
    if (!symbolInput.value.trim()) setHint(error.message, true);
  }
}

function renderSymbolOptions() {
  const query = symbolInput.value.trim().toUpperCase();
  matchingSymbols = availableSymbols
    .filter((symbol) => symbol.includes(query))
    .slice(0, MAX_SYMBOL_OPTIONS);
  if (activeOption >= matchingSymbols.length) activeOption = matchingSymbols.length - 1;

  if (matchingSymbols.length) {
    symbolOptions.replaceChildren(...matchingSymbols.map((symbol, index) => {
      const option = document.createElement("button");
      option.type = "button";
      option.id = `symbol-option-${index}`;
      option.className = "symbol-option";
      option.setAttribute("role", "option");
      option.setAttribute("aria-selected", String(index === activeOption));
      option.dataset.active = String(index === activeOption);
      option.textContent = symbol;
      option.addEventListener("click", () => {
        symbolInput.value = symbol;
        closeSymbolOptions();
        symbolInput.focus();
      });
      return option;
    }));
  } else {
    const empty = document.createElement("div");
    empty.className = "symbol-options-empty";
    empty.textContent = "No matches. You can still enter a full symbol.";
    symbolOptions.replaceChildren(empty);
  }

  symbolOptions.hidden = !symbolOptionsOpen;
  symbolInput.setAttribute("aria-expanded", String(symbolOptionsOpen));
  if (symbolOptionsOpen && activeOption >= 0) {
    symbolInput.setAttribute("aria-activedescendant", `symbol-option-${activeOption}`);
    symbolOptions.querySelector(`#symbol-option-${activeOption}`)?.scrollIntoView({ block: "nearest" });
  } else {
    symbolInput.removeAttribute("aria-activedescendant");
  }
}

function openSymbolOptions() {
  symbolOptionsOpen = true;
  renderSymbolOptions();
}

function closeSymbolOptions() {
  symbolOptionsOpen = false;
  activeOption = -1;
  renderSymbolOptions();
}

function subscribe(symbol) {
  activeSymbol = symbol;
  closeSymbolOptions();
  indicators.clear();
  advisories.clear();
  chartTitle.textContent = symbol;
  pointCount.textContent = "0 points";
  latestUpdate.textContent = "Waiting for indicator data";
  advisorySummary.textContent = "Waiting for crossover signals";
  advisorySummary.dataset.active = "false";
  if (stream) stream.close();
  renderChart();
  renderAdvisories();
  setFeedState("connecting", "Connecting");

  stream = new EventSource(`/api/stream?symbol=${encodeURIComponent(symbol)}`);
  stream.addEventListener("snapshot", (event) => {
    const snapshot = JSON.parse(event.data);
    for (const item of snapshot.indicators) {
      indicators.set(item.windowEndMillis, item);
    }
    for (const item of snapshot.advisories) {
      advisories.set(`${item.windowEndMillis}:${item.direction}`, item);
    }
    trim(indicators, 240);
    trim(advisories, 100);
    renderChart();
    renderAdvisories();
  });
  stream.addEventListener("indicator", (event) => {
    const item = JSON.parse(event.data);
    indicators.set(item.windowEndMillis, item);
    trim(indicators, 240);
    renderChart();
  });
  stream.addEventListener("advisory", (event) => {
    const item = JSON.parse(event.data);
    advisories.set(`${item.windowEndMillis}:${item.direction}`, item);
    trim(advisories, 100);
    renderAdvisories();
  });
  stream.addEventListener("status", (event) => {
    const status = JSON.parse(event.data);
    if (status.state === "error") setFeedState("error", status.message);
    else if (status.state === "connected") setFeedState("connected", "Live feed");
    else setFeedState("connecting", "Connecting");
  });
  stream.onopen = () => setFeedState("connecting", "Connected");
  stream.onerror = () => {
    if (feedState.dataset.state !== "error") setFeedState("connecting", "Reconnecting");
  };
}

function trim(map, limit) {
  const oldestFirst = [...map.entries()].sort(([, left], [, right]) => left.windowEndMillis - right.windowEndMillis);
  while (oldestFirst.length > limit) map.delete(oldestFirst.shift()[0]);
}

function renderChart() {
  const rows = [...indicators.values()].sort((a, b) => a.windowEndMillis - b.windowEndMillis);
  chart.replaceChildren();
  chart.dataset.visible = String(rows.length > 0);
  chartEmpty.dataset.hidden = String(rows.length > 0);
  pointCount.textContent = `${rows.length} point${rows.length === 1 ? "" : "s"}`;
  if (rows.length === 0) return;

  const width = 960;
  const height = 400;
  const margin = { top: 16, right: 18, bottom: 38, left: 62 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const values = rows.flatMap((row) => [row.close, row.ema38, row.ema100]);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const padding = Math.max((rawMax - rawMin) * 0.12, Math.abs(rawMax) * 0.002, 0.01);
  const min = rawMin - padding;
  const max = rawMax + padding;
  const x = (index) => margin.left + (rows.length === 1 ? plotWidth / 2 : index * plotWidth / (rows.length - 1));
  const y = (value) => margin.top + (max - value) * plotHeight / (max - min);

  for (let tick = 0; tick <= 4; tick += 1) {
    const value = min + (max - min) * tick / 4;
    const tickY = y(value);
    chart.append(svgElement("line", {
      x1: margin.left, x2: width - margin.right, y1: tickY, y2: tickY, class: "chart-grid",
    }));
    const label = svgElement("text", {
      x: margin.left - 10, y: tickY + 3, "text-anchor": "end", class: "chart-axis-text",
    });
    label.textContent = value.toFixed(2);
    chart.append(label);
  }

  const labelIndices = [...new Set([0, Math.floor((rows.length - 1) / 2), rows.length - 1])];
  for (const index of labelIndices) {
    const label = svgElement("text", {
      x: x(index), y: height - 11, "text-anchor": index === 0 ? "start" : index === rows.length - 1 ? "end" : "middle",
      class: "chart-axis-text",
    });
    label.textContent = exchangeTime(rows[index].windowEndMillis, { hour: "2-digit", minute: "2-digit" });
    chart.append(label);
  }

  const series = [
    { key: "ema100", className: "ema100-line" },
    { key: "ema38", className: "ema38-line" },
    { key: "close", className: "close-line" },
  ];
  for (const item of series) {
    const path = rows.map((row, index) => `${index === 0 ? "M" : "L"}${x(index).toFixed(2)},${y(row[item.key]).toFixed(2)}`).join(" ");
    chart.append(svgElement("path", { d: path, class: `chart-line ${item.className}` }));
  }

  const last = rows[rows.length - 1];
  chart.append(svgElement("circle", {
    cx: x(rows.length - 1), cy: y(last.close), r: 4, class: "chart-last-dot chart-marker",
  }));
  latestUpdate.textContent = `Latest window · ${exchangeTime(last.windowEndMillis, {
    month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit",
  })}`;
}

function renderAdvisories() {
  const rows = [...advisories.values()].sort((a, b) => b.windowEndMillis - a.windowEndMillis);
  advisoryRows.replaceChildren(...rows.slice(0, 10).map((item) => {
    const row = document.createElement("tr");
    const signalCell = document.createElement("td");
    const signal = document.createElement("span");
    signal.className = `signal-pill ${item.direction.toLowerCase()}`;
    signal.textContent = item.direction;
    signalCell.append(signal);
    const timeCell = document.createElement("td");
    timeCell.textContent = exchangeTime(item.windowEndMillis, {
      month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit",
    });
    row.append(signalCell, timeCell);
    return row;
  }));
  advisoryCount.textContent = String(rows.length);
  advisoryEmpty.dataset.hidden = String(rows.length > 0);
  if (rows.length) {
    advisorySummary.textContent = `${rows[0].direction} · ${exchangeTime(rows[0].windowEndMillis, {
      month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit",
    })}`;
    advisorySummary.dataset.active = "true";
  } else {
    advisorySummary.textContent = "No crossover signals yet";
    advisorySummary.dataset.active = "false";
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const symbol = symbolInput.value.trim().toUpperCase();
  if (!/^[A-Z0-9._-]+\.(FR|NL|ETR)$/.test(symbol)) {
    setHint("Enter a full symbol ending in .FR, .NL or .ETR.", true);
    return;
  }
  symbolInput.value = symbol;
  setHint(`Subscribed to ${symbol}.`);
  subscribe(symbol);
});

symbolInput.addEventListener("focus", openSymbolOptions);
symbolInput.addEventListener("input", () => {
  activeOption = -1;
  openSymbolOptions();
});
symbolInput.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeSymbolOptions();
  } else if (event.key === "ArrowDown" && matchingSymbols.length) {
    event.preventDefault();
    if (!symbolOptionsOpen) openSymbolOptions();
    activeOption = Math.min(activeOption + 1, matchingSymbols.length - 1);
    renderSymbolOptions();
  } else if (event.key === "ArrowUp" && matchingSymbols.length) {
    event.preventDefault();
    if (!symbolOptionsOpen) openSymbolOptions();
    activeOption = activeOption < 0 ? matchingSymbols.length - 1 : Math.max(activeOption - 1, 0);
    renderSymbolOptions();
  } else if (event.key === "Enter" && symbolOptionsOpen && activeOption >= 0) {
    event.preventDefault();
    symbolInput.value = matchingSymbols[activeOption];
    closeSymbolOptions();
  }
});

document.addEventListener("pointerdown", (event) => {
  if (!symbolPicker.contains(event.target)) closeSymbolOptions();
});

loadSymbols();
setInterval(loadSymbols, 10_000);
