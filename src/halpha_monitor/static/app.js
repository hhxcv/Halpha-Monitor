"use strict";

const ui = {
  workspace: document.querySelector("#workspace"),
  serviceStatus: document.querySelector("#service-status"),
  lastRefresh: document.querySelector("#last-refresh"),
  monitorList: document.querySelector("#monitor-list"),
  healthyCount: document.querySelector("#healthy-count"),
  staleCount: document.querySelector("#stale-count"),
  failedCount: document.querySelector("#failed-count"),
  disabledCount: document.querySelector("#disabled-count"),
  summaryCutoff: document.querySelector("#summary-cutoff"),
  monitorTitle: document.querySelector("#monitor-title"),
  monitorDescription: document.querySelector("#monitor-description"),
  monitorState: document.querySelector("#monitor-state"),
  monitorStatusDetail: document.querySelector("#monitor-status-detail"),
  monitorControlButton: document.querySelector("#monitor-control-button"),
  monitorControlStatus: document.querySelector("#monitor-control-status"),
  configurationRegion: document.querySelector("#configuration-region"),
  configurationForm: document.querySelector("#configuration-form"),
  configurationFields: document.querySelector("#configuration-fields"),
  configurationSubmit: document.querySelector("#configuration-submit"),
  configurationStatus: document.querySelector("#configuration-status"),
  filters: document.querySelector("#dynamic-filters"),
  timeWindow: document.querySelector("#time-window"),
  dataCutoff: document.querySelector("#data-cutoff"),
  quoteTableTitle: document.querySelector("#quote-table-title"),
  quoteHead: document.querySelector("#quote-head"),
  quoteBody: document.querySelector("#quote-body"),
  quoteEmpty: document.querySelector("#quote-empty"),
  historyTitle: document.querySelector("#history-title"),
  historySeries: document.querySelector("#history-series"),
  historyChart: document.querySelector("#history-chart"),
  chartZoomOut: document.querySelector("#chart-zoom-out"),
  chartZoomIn: document.querySelector("#chart-zoom-in"),
  chartReset: document.querySelector("#chart-reset"),
  chartViewportStatus: document.querySelector("#chart-viewport-status"),
  collectionGapNote: document.querySelector("#collection-gap-note"),
  chartSummary: document.querySelector("#chart-summary"),
  diagnosticsRegion: document.querySelector("#diagnostics-region"),
  issueCount: document.querySelector("#issue-count"),
  issueBody: document.querySelector("#issue-body"),
  issueEmpty: document.querySelector("#issue-empty"),
  pageError: document.querySelector("#page-error"),
};

const state = {
  monitorId: null,
  seriesKey: null,
  hours: 6,
  filters: {},
  refreshTimer: null,
  request: null,
  configurationDirty: false,
  configurationSubmitting: false,
  controlSubmitting: false,
  pendingConfigurationRunAfter: null,
  latestRunId: null,
  chartModel: null,
  chartGeometry: null,
  chartView: { seriesKey: null, zoom: 1, start: 0 },
  chartDrag: null,
  chartResizeTimer: null,
};

const STATUS_LABELS = {
  HEALTHY: "运行正常",
  PARTIAL: "部分采集失败",
  DEGRADED: "部分异常",
  FAILED: "采集失败",
  STALE: "数据过期",
  RUNNING: "采集中",
  UNKNOWN: "尚无数据",
  DISABLED: "已关闭",
};

const ISSUE_REASON_LABELS = {
  NO_ELIGIBLE_C2C_AD: "无符合条件的广告",
  SMART_MONEY_BACKOFF_ACTIVE: "接口退避中",
  SMART_MONEY_BUSINESS_RESPONSE_FAILED: "接口业务响应异常",
  SMART_MONEY_DETAILS_EMPTY_WITH_FLOW: "流量明细为空",
  SMART_MONEY_FLOW_TIMESTAMP_STALE: "资金流时间戳陈旧",
  SMART_MONEY_HTTP_THROTTLED_418: "接口已封禁并退避",
  SMART_MONEY_HTTP_THROTTLED_429: "接口限流并退避",
  SMART_MONEY_MARK_PRICE_STALE: "标记价陈旧",
  SMART_MONEY_OI_STALE: "持仓量陈旧",
  SMART_MONEY_OVERVIEW_STALE: "仓位总览陈旧",
  SMART_MONEY_SCHEMA_CHANGED: "接口字段契约变化",
};

const ISSUE_REASON_DETAILS = {
  NO_ELIGIBLE_C2C_AD: "没有同时满足金额、广告限额、可交易库存和支付方式的广告",
  SMART_MONEY_BACKOFF_ACTIVE: "限流退避尚未结束，本轮没有请求网页内部接口",
  SMART_MONEY_BUSINESS_RESPONSE_FAILED: "Binance 网页内部接口没有返回预期业务成功码",
  SMART_MONEY_DETAILS_EMPTY_WITH_FLOW: "分项存在资金流，但用于核对新鲜度的最新明细为空",
  SMART_MONEY_FLOW_TIMESTAMP_STALE: "最新交易时间早于对应资金流窗口，未生成新特征",
  SMART_MONEY_HTTP_THROTTLED_418: "Binance 返回 HTTP 418；采集器已指数退避",
  SMART_MONEY_HTTP_THROTTLED_429: "Binance 返回 HTTP 429；采集器已指数退避",
  SMART_MONEY_MARK_PRICE_STALE: "官方 USDⓈ-M 标记价时间超过允许阈值，未生成新特征",
  SMART_MONEY_OI_STALE: "官方 USDⓈ-M 持仓量时间超过允许阈值，未生成新特征",
  SMART_MONEY_OVERVIEW_STALE: "仓位总览更新时间陈旧，资金流特征保留但不使用总览字段",
  SMART_MONEY_SCHEMA_CHANGED: "未文档化接口的字段集合与已核验契约不一致，未生成新特征",
};

const CHART = {
  defaultWidth: 800,
  desktopHeight: 270,
  compactHeight: 225,
  pad: { left: 84, right: 22, top: 20, bottom: 42 },
  maximumZoom: 16,
};

function formatTime(value) {
  if (!value) return "未知";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "未知";
  const rendered = new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(parsed);
  return `${rendered} UTC+8`;
}

function formatChartTime(value, includeDate = false) {
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: includeDate ? "2-digit" : undefined,
    day: includeDate ? "2-digit" : undefined,
    hour: "2-digit",
    minute: "2-digit",
    second: includeDate ? "2-digit" : undefined,
    hour12: false,
  }).format(value);
}

function formatChartValue(value) {
  return Number(value).toFixed(6);
}

function clamp(value, minimum, maximum) {
  return Math.min(Math.max(value, minimum), maximum);
}

function setStatus(element, status, label) {
  element.dataset.status = status;
  element.textContent = label || STATUS_LABELS[status] || status;
}

function createElement(tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== undefined) element.textContent = text;
  return element;
}

function queryUrl() {
  const params = new URLSearchParams();
  if (state.monitorId) params.set("monitor_id", state.monitorId);
  params.set("hours", String(state.hours));
  if (state.seriesKey) params.set("series_key", state.seriesKey);
  Object.entries(state.filters).forEach(([key, value]) => params.set(key, value));
  return `/api/view?${params.toString()}`;
}

async function loadView({ preserveSeries = true } = {}) {
  if (!preserveSeries) state.seriesKey = null;
  if (state.request) state.request.abort();
  state.request = new AbortController();
  ui.workspace.setAttribute("aria-busy", "true");
  try {
    const response = await fetch(queryUrl(), {
      signal: state.request.signal,
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    if (!response.ok) throw new Error(`HTTP_${response.status}`);
    const payload = await response.json();
    state.monitorId = payload.monitor.monitor_id;
    state.seriesKey = payload.selected_series_key;
    state.filters = payload.monitor.selected_filters;
    state.latestRunId = payload.monitor.latest_run?.run_id ?? null;
    render(payload);
    ui.pageError.hidden = true;
    return true;
  } catch (error) {
    if (error.name === "AbortError") return false;
    ui.pageError.hidden = false;
    ui.pageError.textContent = `页面数据不可用 · ${formatTime(new Date().toISOString())} · ${error.message}`;
    setStatus(ui.serviceStatus, "FAILED", "页面数据不可用");
    return false;
  } finally {
    ui.workspace.setAttribute("aria-busy", "false");
    clearTimeout(state.refreshTimer);
    state.refreshTimer = setTimeout(() => loadView(), 15000);
  }
}

function render(payload) {
  renderGlobal(payload);
  renderMonitorList(payload.monitors);
  renderContext(payload.monitor);
  renderControl(payload.monitor);
  renderConfiguration(payload.monitor.configuration, payload.monitor.latest_run);
  renderFilters(payload.monitor.filters, payload.time_windows);
  ui.quoteTableTitle.textContent = payload.monitor.table_title;
  renderTable(
    payload.monitor.columns,
    payload.rows,
    payload.selected_series_key,
    payload.issues,
    payload.monitor.selected_filters,
    payload.monitor.data_status,
    payload.monitor.latest_run?.run_id,
  );
  renderHistory(
    payload.monitor.chart_title,
    payload.rows,
    payload.selected_series_key,
    payload.history,
    payload.collection_gaps,
  );
  renderIssues(payload.issues);
}

function renderGlobal(payload) {
  setStatus(ui.serviceStatus, payload.service_status, payload.service_status_label);
  const completed = payload.monitor.latest_run?.completed_at || payload.monitor.data_run?.completed_at;
  ui.lastRefresh.textContent = formatTime(completed);
  ui.summaryCutoff.textContent = `状态统计截止：${formatTime(payload.server_time)}`;
  ui.healthyCount.textContent = String(payload.monitors.filter(
    (item) => item.enabled && ["CURRENT", "CURRENT_WITH_GAPS"].includes(item.data_status.kind)
  ).length);
  ui.staleCount.textContent = String(payload.monitors.filter(
    (item) => item.enabled && ["COLLECTING_PREVIOUS", "HISTORICAL", "STALE"].includes(item.data_status.kind)
  ).length);
  ui.failedCount.textContent = String(payload.monitors.filter(
    (item) => item.enabled && item.data_status.kind === "EMPTY"
  ).length);
  ui.disabledCount.textContent = String(payload.monitors.filter((item) => !item.enabled).length);
}

function renderMonitorList(monitors) {
  ui.monitorList.replaceChildren();
  monitors.forEach((monitor) => {
    const button = createElement("button", "monitor-link");
    button.type = "button";
    button.dataset.monitorId = monitor.monitor_id;
    if (monitor.monitor_id === state.monitorId) button.setAttribute("aria-current", "page");
    button.append(createElement("span", "monitor-link-name", monitor.display_name));
    const status = createElement("span", "monitor-link-status", monitor.data_status.label);
    status.dataset.status = monitor.data_status.tone;
    button.append(status);
    button.addEventListener("click", async () => {
      const previous = {
        monitorId: state.monitorId,
        filters: state.filters,
        seriesKey: state.seriesKey,
      };
      state.monitorId = monitor.monitor_id;
      state.filters = {};
      state.seriesKey = null;
      if (!await loadView({ preserveSeries: false })) {
        state.monitorId = previous.monitorId;
        state.filters = previous.filters;
        state.seriesKey = previous.seriesKey;
      }
    });
    ui.monitorList.append(button);
  });
}

function renderContext(monitor) {
  ui.monitorTitle.textContent = monitor.display_name;
  ui.monitorDescription.textContent = monitor.description;
  setStatus(ui.monitorState, monitor.data_status.tone, monitor.data_status.label);
  ui.monitorStatusDetail.textContent = monitor.data_status.detail;
  const cutoff = monitor.data_run?.completed_at;
  ui.dataCutoff.textContent = `${monitor.data_status.cutoff_label}：${cutoff ? formatTime(cutoff) : "—"}`;
}

function renderControl(monitor) {
  if (state.controlSubmitting) return;
  ui.monitorControlButton.disabled = false;
  ui.monitorControlButton.dataset.enabled = String(monitor.enabled);
  ui.monitorControlButton.textContent = monitor.enabled ? "关闭监控" : "开启监控";
  ui.monitorControlButton.setAttribute(
    "aria-label",
    `${monitor.enabled ? "关闭" : "开启"}${monitor.display_name}`,
  );
}

ui.monitorControlButton.addEventListener("click", async () => {
  if (state.controlSubmitting || !state.monitorId) return;
  const enabling = ui.monitorControlButton.dataset.enabled !== "true";
  state.controlSubmitting = true;
  ui.monitorControlButton.disabled = true;
  ui.monitorControlButton.textContent = enabling ? "正在开启…" : "正在关闭…";
  ui.monitorControlStatus.textContent = enabling ? "正在提交开启请求" : "正在提交关闭请求";
  try {
    const response = await fetch(`/api/monitors/${encodeURIComponent(state.monitorId)}/control`, {
      method: "PUT",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ enabled: enabling }),
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP_${response.status}`);
    ui.monitorControlStatus.textContent = payload.enabled
      ? payload.refresh_requested ? "已开启，正在采集" : "已开启"
      : "已关闭，不再安排新的采集";
    state.controlSubmitting = false;
    await loadView();
    ui.monitorControlButton.focus();
  } catch (error) {
    ui.monitorControlStatus.textContent = `操作失败 · ${error.message}`;
    ui.monitorControlButton.textContent = enabling ? "开启监控" : "关闭监控";
  } finally {
    state.controlSubmitting = false;
    ui.monitorControlButton.disabled = false;
  }
});

function setConfigurationStatus(message, kind = "") {
  ui.configurationStatus.textContent = message;
  ui.configurationStatus.dataset.kind = kind;
}

function renderConfiguration(configuration, latestRun) {
  ui.configurationRegion.hidden = !configuration;
  if (!configuration) return;
  if (
    state.pendingConfigurationRunAfter !== null &&
    latestRun?.run_id > state.pendingConfigurationRunAfter &&
    latestRun.status !== "RUNNING"
  ) {
    if (["SUCCESS", "PARTIAL"].includes(latestRun.status)) {
      setConfigurationStatus("已按新条件完成采集", "success");
    } else {
      setConfigurationStatus("新条件采集失败，仍显示上次可用报价", "error");
    }
    state.pendingConfigurationRunAfter = null;
  } else if (!state.configurationDirty && !state.configurationSubmitting && !ui.configurationStatus.textContent) {
    setConfigurationStatus(`保存于 ${formatTime(configuration.updated_at)}`);
  }
  if (state.configurationDirty || state.configurationSubmitting) return;

  ui.configurationFields.replaceChildren();
  configuration.fields.forEach((field) => {
    if (field.kind === "decimal") {
      const label = createElement("label", "configuration-field");
      label.append(createElement("span", "configuration-label", field.label));
      const inputRow = createElement("span", "configuration-input-row");
      const input = document.createElement("input");
      input.type = "number";
      input.name = field.key;
      input.value = configuration.values[field.key];
      input.min = field.minimum || "0";
      input.step = field.step || "any";
      input.required = true;
      input.setAttribute("aria-label", field.label);
      inputRow.append(input);
      if (field.unit) inputRow.append(createElement("span", "configuration-unit", field.unit));
      label.append(inputRow);
      ui.configurationFields.append(label);
      return;
    }
    if (field.kind === "multi_choice") {
      const group = document.createElement("fieldset");
      group.className = "configuration-choice-field";
      group.dataset.key = field.key;
      group.append(createElement("legend", "configuration-label", field.label));
      const choices = createElement("div", "configuration-choices");
      const selected = new Set(configuration.values[field.key]);
      field.choices.forEach((choice) => {
        const label = createElement("label", "configuration-choice");
        const input = document.createElement("input");
        input.type = "checkbox";
        input.name = field.key;
        input.value = choice.value;
        input.checked = selected.has(choice.value);
        label.append(input, createElement("span", "", choice.label));
        choices.append(label);
      });
      group.append(choices);
      ui.configurationFields.append(group);
    }
  });
}

ui.configurationForm.addEventListener("input", () => {
  state.configurationDirty = true;
  setConfigurationStatus("有未保存更改", "pending");
});

ui.configurationForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (state.configurationSubmitting || !ui.configurationForm.reportValidity()) return;
  const values = {};
  ui.configurationFields.querySelectorAll("input[type='number']").forEach((input) => {
    values[input.name] = input.value;
  });
  for (const group of ui.configurationFields.querySelectorAll("fieldset[data-key]")) {
    const selected = [...group.querySelectorAll("input[type='checkbox']:checked")].map((input) => input.value);
    if (!selected.length) {
      setConfigurationStatus("至少选择一种支付方式", "error");
      group.querySelector("input")?.focus();
      return;
    }
    values[group.dataset.key] = selected;
  }

  state.configurationSubmitting = true;
  ui.configurationSubmit.disabled = true;
  ui.configurationSubmit.textContent = "正在应用…";
  setConfigurationStatus("正在保存采集条件", "pending");
  const previousRunId = state.latestRunId ?? 0;
  try {
    const response = await fetch(`/api/monitors/${encodeURIComponent(state.monitorId)}/configuration`, {
      method: "PUT",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ values }),
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP_${response.status}`);
    state.configurationDirty = false;
    state.pendingConfigurationRunAfter = payload.refresh_requested ? previousRunId : null;
    setConfigurationStatus(payload.refresh_requested ? "条件已保存，正在刷新报价" : "条件已保存，下次采集生效", "pending");
    await loadView({ preserveSeries: false });
  } catch (error) {
    setConfigurationStatus(`保存失败 · ${error.message}`, "error");
  } finally {
    state.configurationSubmitting = false;
    ui.configurationSubmit.disabled = false;
    ui.configurationSubmit.textContent = "应用并刷新";
  }
});

function renderFilters(filters, timeWindows) {
  ui.filters.replaceChildren();
  filters.forEach((filter) => {
    const label = createElement("label", "filter-field");
    label.append(createElement("span", "", filter.label));
    const select = document.createElement("select");
    select.name = filter.key;
    select.setAttribute("aria-label", filter.label);
    filter.choices.forEach((choice) => {
      const option = document.createElement("option");
      option.value = choice.value;
      option.textContent = choice.label;
      option.selected = choice.value === filter.selected;
      select.append(option);
    });
    select.addEventListener("change", async () => {
      const previous = state.filters[filter.key];
      const previousSeries = state.seriesKey;
      state.filters[filter.key] = select.value;
      if (!await loadView({ preserveSeries: false })) {
        state.filters[filter.key] = previous;
        state.seriesKey = previousSeries;
        select.value = previous;
      }
    });
    label.append(select);
    ui.filters.append(label);
  });
  ui.timeWindow.replaceChildren();
  timeWindows.forEach((window) => {
    const option = document.createElement("option");
    option.value = String(window.hours);
    option.textContent = window.label;
    option.selected = window.hours === state.hours;
    ui.timeWindow.append(option);
  });
}

ui.timeWindow.addEventListener("change", async () => {
  const previous = state.hours;
  state.hours = Number(ui.timeWindow.value);
  if (!await loadView()) {
    state.hours = previous;
    ui.timeWindow.value = String(previous);
  }
});

function cellValue(row, column) {
  const value = row[column.key];
  const declaredReason = row.missing_reasons?.[column.key];
  const missing = (reason = declaredReason || "来源未返回可用值，未使用替代数据。") => ({
    text: "—",
    missing: true,
    reason,
  });
  if (value === null || value === undefined || value === "") return missing();
  if (column.kind === "time") {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return missing("时间值异常，已停止展示。");
    return { text: formatTime(value), missing: false };
  }
  if (column.kind === "percent") {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return missing("数值异常，已停止展示。");
    return {
      text: `${numeric >= 0 ? "+" : ""}${numeric.toFixed(4)}%`,
      missing: false,
    };
  }
  if (column.kind === "number") {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return missing("数值异常，已停止展示。");
    return {
      text: new Intl.NumberFormat("zh-CN", {
        useGrouping: Boolean(column.use_grouping),
        minimumFractionDigits: column.minimum_fraction_digits ?? 0,
        maximumFractionDigits: column.maximum_fraction_digits ?? 8,
      }).format(numeric),
      missing: false,
    };
  }
  return { text: String(value), missing: false };
}

function matchingIssue(issues, filters, latestRunId) {
  const exactScopes = new Set(["monitor"]);
  if (filters.symbol) {
    exactScopes.add(filters.symbol);
    if (filters.time_range) exactScopes.add(`${filters.symbol}:${filters.time_range}`);
  }
  return issues.find((issue) => (
    issue.run_id === latestRunId
    && (
      exactScopes.has(issue.scope)
      || (filters.trade_type && issue.scope.startsWith(`${filters.trade_type}:`))
    )
  ));
}

function emptyTableMessage(issues, filters, dataStatus, latestRunId) {
  const issue = matchingIssue(issues, filters, latestRunId);
  if (issue) {
    const reason = ISSUE_REASON_LABELS[issue.reason_code] || issue.reason_code;
    return `本轮该范围没有通过校验的数据；未使用任何替代值。原因：${reason}。系统会自动重试。`;
  }
  if ([
    "HISTORICAL",
    "STALE",
    "COLLECTING_PREVIOUS",
    "DISABLED_WITH_HISTORY",
  ].includes(dataStatus.kind)) {
    return "当前显示的历史记录中，该范围没有可用数据；未使用任何替代值。";
  }
  if (dataStatus.kind === "DISABLED_EMPTY") {
    return "监控尚未开启或没有已采集数据；未使用任何替代值。";
  }
  return "当前范围暂无可用数据；未使用任何替代值。";
}

function renderTable(
  columns,
  rows,
  selectedSeries,
  issues,
  filters,
  dataStatus,
  latestRunId,
) {
  ui.quoteHead.replaceChildren();
  columns.forEach((column) => {
    const th = createElement("th", column.priority === "secondary" ? "col-secondary" : "", column.label);
    th.scope = "col";
    th.dataset.kind = column.kind;
    ui.quoteHead.append(th);
  });
  ui.quoteBody.replaceChildren();
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.tabIndex = 0;
    tr.setAttribute("aria-selected", String(row.series_key === selectedSeries));
    columns.forEach((column) => {
      const rendered = cellValue(row, column);
      const td = createElement(
        "td",
        column.priority === "secondary" ? "col-secondary" : "",
        rendered.text,
      );
      td.dataset.kind = column.kind;
      if (rendered.missing) {
        td.dataset.missing = "true";
        td.title = rendered.reason;
        td.setAttribute("aria-label", `${column.label}：无可用数据。${rendered.reason}`);
      }
      tr.append(td);
    });
    const select = async () => {
      const previous = state.seriesKey;
      state.seriesKey = row.series_key;
      if (!await loadView()) state.seriesKey = previous;
    };
    tr.addEventListener("click", select);
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        select();
      }
    });
    ui.quoteBody.append(tr);
  });
  ui.quoteEmpty.hidden = rows.length > 0;
  ui.quoteEmpty.textContent = emptyTableMessage(
    issues,
    filters,
    dataStatus,
    latestRunId,
  );
}

function renderHistory(title, rows, selectedSeries, history, collectionGaps) {
  ui.historyTitle.textContent = title;
  const selected = rows.find((row) => row.series_key === selectedSeries);
  const fallbackLabel = selected
    ? [selected.asset, selected.direction_label, selected.unit].filter(Boolean).join(" · ")
    : "";
  const seriesLabel = selected?.series_label || fallbackLabel;
  ui.historySeries.textContent = seriesLabel || "尚未选择序列";
  const points = history
    .map((item) => ({
      time: new Date(item.observed_at),
      value:
        item.value === null || item.value === undefined || item.value === ""
          ? Number.NaN
          : Number(item.value),
      segment: Number(item.segment),
    }))
    .filter((item) => !Number.isNaN(item.time.getTime()) && Number.isFinite(item.value));
  const gaps = (collectionGaps || [])
    .map((item) => ({
      start: new Date(item.started_at),
      end: new Date(item.ended_at),
      open: Boolean(item.open),
      label: item.label,
    }))
    .filter((item) => (
      !Number.isNaN(item.start.getTime())
      && !Number.isNaN(item.end.getTime())
      && item.end > item.start
    ));
  if (state.chartView.seriesKey !== selectedSeries) {
    state.chartView = { seriesKey: selectedSeries, zoom: 1, start: 0 };
  }
  state.chartModel = { selected, points, gaps };
  drawHistoryChart();
}

function chartWindow(points) {
  if (points.length < 2) return { points, startIndex: 0, windowSize: points.length };
  const maximumZoom = Math.min(CHART.maximumZoom, points.length / 2);
  state.chartView.zoom = clamp(state.chartView.zoom, 1, maximumZoom);
  const windowSize = Math.max(2, Math.ceil(points.length / state.chartView.zoom));
  const maximumStart = Math.max(0, points.length - windowSize);
  const startIndex = Math.round(clamp(state.chartView.start, 0, 1) * maximumStart);
  return {
    points: points.slice(startIndex, startIndex + windowSize),
    startIndex,
    windowSize,
  };
}

function chartDimensions() {
  const measuredWidth = Math.round(ui.historyChart.clientWidth || CHART.defaultWidth);
  return {
    width: Math.max(340, measuredWidth),
    height: measuredWidth <= 620 ? CHART.compactHeight : CHART.desktopHeight,
    pad: CHART.pad,
  };
}

function drawHistoryChart() {
  ui.historyChart.replaceChildren();
  ui.chartSummary.replaceChildren();
  const model = state.chartModel;
  const allPoints = model?.points || [];
  const allGaps = model?.gaps || [];
  const maximumZoom = allPoints.length < 2 ? 1 : Math.min(CHART.maximumZoom, allPoints.length / 2);
  ui.chartZoomOut.disabled = state.chartView.zoom <= 1;
  ui.chartZoomIn.disabled = state.chartView.zoom >= maximumZoom;
  ui.chartReset.disabled = state.chartView.zoom <= 1 && state.chartView.start === 0;
  ui.historyChart.dataset.pannable = String(state.chartView.zoom > 1);
  if (!allPoints.length) {
    ui.chartViewportStatus.textContent = "";
    ui.collectionGapNote.textContent = "";
    ui.historyChart.append(createElement("p", "empty-state", "当前时间范围没有历史样本。"));
    return;
  }
  const visible = chartWindow(allPoints);
  const points = visible.points;
  const { width, height, pad } = chartDimensions();
  const values = points.map((item) => item.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, Math.abs(max) * 0.0005, 0.0001);
  const low = min - range * .12;
  const high = max + range * .12;
  const firstTime = points[0].time.getTime();
  const includesLatestPoint = points.at(-1) === allPoints.at(-1);
  const openGap = includesLatestPoint ? allGaps.find((gap) => gap.open) : null;
  const lastTime = Math.max(
    points.at(-1).time.getTime(),
    openGap?.end.getTime() || 0,
  );
  const timeRange = Math.max(lastTime - firstTime, 1);
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const x = (time) => points.length === 1
    ? pad.left + plotWidth / 2
    : pad.left + ((time.getTime() - firstTime) / timeRange) * plotWidth;
  const y = (value) => pad.top + ((high - value) / (high - low)) * (height - pad.top - pad.bottom);
  const segments = [];
  points.forEach((point) => {
    const latest = segments.at(-1);
    if (!latest || latest[0].segment !== point.segment) segments.push([point]);
    else latest.push(point);
  });
  const lineSvg = segments.map((segment) => {
    const line = segment
      .map((item) => `${x(item.time).toFixed(2)},${y(item.value).toFixed(2)}`)
      .join(" ");
    if (segment.length === 1) {
      return `<circle class="chart-point-static" cx="${x(segment[0].time).toFixed(2)}" cy="${y(segment[0].value).toFixed(2)}" r="3"></circle>`;
    }
    const area = `${x(segment[0].time).toFixed(2)},${height - pad.bottom} ${line} ${x(segment.at(-1).time).toFixed(2)},${height - pad.bottom}`;
    return `<polygon class="chart-area" points="${area}"></polygon><polyline class="chart-line" points="${line}"></polyline>`;
  }).join("");
  const visibleGaps = allGaps.filter((gap) => (
    gap.end.getTime() > firstTime && gap.start.getTime() < lastTime
  ));
  const gapSvg = visibleGaps.map((gap) => {
    const left = x(new Date(Math.max(gap.start.getTime(), firstTime)));
    const right = x(new Date(Math.min(gap.end.getTime(), lastTime)));
    return `<rect class="chart-gap" x="${left.toFixed(2)}" y="${pad.top}" width="${Math.max(1, right - left).toFixed(2)}" height="${plotHeight}"></rect>`;
  }).join("");
  const rowsSvg = [0, .5, 1].map((ratio) => {
    const yy = pad.top + ratio * plotHeight;
    const label = high - ratio * (high - low);
    return `<line class="chart-grid" x1="${pad.left}" y1="${yy}" x2="${width - pad.right}" y2="${yy}"></line><text class="chart-label" text-anchor="end" x="${pad.left - 10}" y="${yy + 5}">${formatChartValue(label)}</text>`;
  }).join("");
  const middleTime = new Date(firstTime + timeRange / 2);
  const svg = `<svg viewBox="0 0 ${width} ${height}" aria-hidden="true">
    ${rowsSvg}
    ${gapSvg}
    ${lineSvg}
    <text class="chart-label" x="${pad.left}" y="${height - 12}">${formatChartTime(points[0].time)}</text>
    <text class="chart-label" text-anchor="middle" x="${pad.left + plotWidth / 2}" y="${height - 12}">${formatChartTime(middleTime)}</text>
    <text class="chart-label" text-anchor="end" x="${width - pad.right}" y="${height - 12}">${formatChartTime(points.at(-1).time)}</text>
    <g class="chart-hover" hidden>
      <line class="chart-crosshair" data-axis="x" y1="${pad.top}" y2="${height - pad.bottom}"></line>
      <line class="chart-crosshair" data-axis="y" x1="${pad.left}" x2="${width - pad.right}"></line>
      <circle class="chart-point" r="4"></circle>
      <rect class="chart-hover-label" data-label="y" x="2" width="76" height="24" rx="2"></rect>
      <text class="chart-hover-text" data-value="y" text-anchor="middle" x="40"></text>
      <rect class="chart-hover-label" data-label="x" y="${height - pad.bottom + 6}" width="132" height="26" rx="2"></rect>
      <text class="chart-hover-text" data-value="x" text-anchor="middle" y="${height - pad.bottom + 24}"></text>
    </g>
  </svg>`;
  ui.historyChart.innerHTML = svg;
  const accessibleSeriesLabel = model.selected?.series_label || model.selected?.asset || "当前序列";
  ui.historyChart.setAttribute(
    "aria-label",
    `${accessibleSeriesLabel}非连续历史曲线，当前显示 ${points.length} 个已采集样本、${segments.length} 个采集段和 ${visibleGaps.length} 个未采集时段，最低 ${formatChartValue(min)}，最高 ${formatChartValue(max)}。可用滚轮缩放，放大后左右拖动。`,
  );
  ui.chartViewportStatus.textContent = state.chartView.zoom > 1
    ? `已放大 ${state.chartView.zoom.toFixed(1)} 倍 · 显示第 ${visible.startIndex + 1}–${visible.startIndex + points.length} 个，共 ${allPoints.length} 个已采集样本`
    : `显示全部 ${allPoints.length} 个已采集样本`;
  if (allGaps.length) {
    const recent = allGaps.slice(-2).map((gap) => (
      `${formatTime(gap.start.toISOString())}—${gap.open ? "现在" : formatTime(gap.end.toISOString())}`
    ));
    ui.collectionGapNote.textContent = `曲线不会跨越未采集时段。${allGaps.length} 段空档：${recent.join("；")}`;
  } else {
    ui.collectionGapNote.textContent = "已显示样本之间未发现采集空档。";
  }
  state.chartGeometry = { points, x, y, width, height, pad, plotWidth, plotHeight };
  const summary = [
    ["最新", points.at(-1).value],
    ["最低", min],
    ["最高", max],
    ["样本", points.length],
    ["采集段", segments.length],
    ["空档", visibleGaps.length],
  ];
  summary.forEach(([label, value]) => {
    const item = document.createElement("div");
    item.append(createElement("dt", "", label));
    const countLabels = new Set(["样本", "采集段", "空档"]);
    item.append(createElement(
      "dd",
      "",
      typeof value === "number" && !countLabels.has(label) ? value.toFixed(6) : String(value),
    ));
    ui.chartSummary.append(item);
  });
}

function setChartZoom(nextZoom, focusRatio = .5) {
  const allPoints = state.chartModel?.points || [];
  if (allPoints.length < 2) return;
  const previous = chartWindow(allPoints);
  const maximumZoom = Math.min(CHART.maximumZoom, allPoints.length / 2);
  const zoom = clamp(nextZoom, 1, maximumZoom);
  const nextWindowSize = Math.max(2, Math.ceil(allPoints.length / zoom));
  const focusIndex = previous.startIndex + focusRatio * Math.max(previous.windowSize - 1, 0);
  const nextMaximumStart = Math.max(0, allPoints.length - nextWindowSize);
  const nextStart = clamp(focusIndex - focusRatio * Math.max(nextWindowSize - 1, 0), 0, nextMaximumStart);
  state.chartView.zoom = zoom;
  state.chartView.start = nextMaximumStart ? nextStart / nextMaximumStart : 0;
  drawHistoryChart();
}

function resetChartView() {
  state.chartView.zoom = 1;
  state.chartView.start = 0;
  drawHistoryChart();
}

function panChart(direction) {
  const allPoints = state.chartModel?.points || [];
  const visible = chartWindow(allPoints);
  const maximumStart = Math.max(0, allPoints.length - visible.windowSize);
  if (!maximumStart) return;
  const currentStart = state.chartView.start * maximumStart;
  const step = Math.max(1, visible.windowSize * .12) * direction;
  state.chartView.start = clamp(currentStart + step, 0, maximumStart) / maximumStart;
  drawHistoryChart();
}

function chartPointerPosition(event) {
  const bounds = ui.historyChart.getBoundingClientRect();
  const geometry = state.chartGeometry || chartDimensions();
  return {
    x: ((event.clientX - bounds.left) / bounds.width) * geometry.width,
    clientPlotWidth: bounds.width * (geometry.width - geometry.pad.left - geometry.pad.right) / geometry.width,
  };
}

function updateChartHover(event) {
  if (state.chartDrag || !state.chartGeometry) return;
  const position = chartPointerPosition(event);
  const { points, x, y, width, height, pad } = state.chartGeometry;
  if (position.x < pad.left || position.x > width - pad.right) {
    ui.historyChart.querySelector(".chart-hover")?.setAttribute("hidden", "");
    return;
  }
  const point = points.reduce((nearest, candidate) => (
    Math.abs(x(candidate.time) - position.x) < Math.abs(x(nearest.time) - position.x) ? candidate : nearest
  ));
  const pointX = x(point.time);
  const pointY = y(point.value);
  const group = ui.historyChart.querySelector(".chart-hover");
  group.removeAttribute("hidden");
  group.querySelector('[data-axis="x"]').setAttribute("x1", pointX);
  group.querySelector('[data-axis="x"]').setAttribute("x2", pointX);
  group.querySelector('[data-axis="y"]').setAttribute("y1", pointY);
  group.querySelector('[data-axis="y"]').setAttribute("y2", pointY);
  const marker = group.querySelector(".chart-point");
  marker.setAttribute("cx", pointX);
  marker.setAttribute("cy", pointY);
  const yLabelTop = clamp(pointY - 12, pad.top, height - pad.bottom - 24);
  group.querySelector('[data-label="y"]').setAttribute("y", yLabelTop);
  const yText = group.querySelector('[data-value="y"]');
  yText.setAttribute("y", yLabelTop + 17);
  yText.textContent = formatChartValue(point.value);
  const xLabelLeft = clamp(pointX - 66, pad.left, width - pad.right - 132);
  group.querySelector('[data-label="x"]').setAttribute("x", xLabelLeft);
  const xText = group.querySelector('[data-value="x"]');
  xText.setAttribute("x", xLabelLeft + 66);
  xText.textContent = formatChartTime(point.time, true);
}

ui.chartZoomIn.addEventListener("click", () => setChartZoom(state.chartView.zoom * 1.5));
ui.chartZoomOut.addEventListener("click", () => setChartZoom(state.chartView.zoom / 1.5));
ui.chartReset.addEventListener("click", resetChartView);

ui.historyChart.addEventListener("wheel", (event) => {
  if (!state.chartModel?.points.length) return;
  event.preventDefault();
  const position = chartPointerPosition(event);
  const { width, pad } = state.chartGeometry;
  const focusRatio = clamp((position.x - pad.left) / (width - pad.left - pad.right), 0, 1);
  setChartZoom(state.chartView.zoom * (event.deltaY < 0 ? 1.35 : 1 / 1.35), focusRatio);
}, { passive: false });

ui.historyChart.addEventListener("pointerdown", (event) => {
  if (event.button !== 0 || state.chartView.zoom <= 1) return;
  const position = chartPointerPosition(event);
  const { width, pad } = state.chartGeometry;
  if (position.x < pad.left || position.x > width - pad.right) return;
  ui.historyChart.setPointerCapture(event.pointerId);
  state.chartDrag = { pointerId: event.pointerId, clientX: event.clientX, start: state.chartView.start };
  ui.historyChart.classList.add("is-dragging");
});

ui.historyChart.addEventListener("pointermove", (event) => {
  if (!state.chartDrag) {
    updateChartHover(event);
    return;
  }
  const allPoints = state.chartModel?.points || [];
  const visible = chartWindow(allPoints);
  const maximumStart = Math.max(0, allPoints.length - visible.windowSize);
  if (!maximumStart) return;
  const { clientPlotWidth } = chartPointerPosition(event);
  const deltaSamples = ((state.chartDrag.clientX - event.clientX) / clientPlotWidth) * visible.windowSize;
  const initialStart = state.chartDrag.start * maximumStart;
  state.chartView.start = clamp(initialStart + deltaSamples, 0, maximumStart) / maximumStart;
  drawHistoryChart();
});

function finishChartDrag(event) {
  if (!state.chartDrag || state.chartDrag.pointerId !== event.pointerId) return;
  state.chartDrag = null;
  ui.historyChart.classList.remove("is-dragging");
  if (ui.historyChart.hasPointerCapture(event.pointerId)) ui.historyChart.releasePointerCapture(event.pointerId);
}

ui.historyChart.addEventListener("pointerup", finishChartDrag);
ui.historyChart.addEventListener("pointercancel", finishChartDrag);
ui.historyChart.addEventListener("pointerleave", () => {
  if (!state.chartDrag) ui.historyChart.querySelector(".chart-hover")?.setAttribute("hidden", "");
});

ui.historyChart.addEventListener("keydown", (event) => {
  if (event.key === "+" || event.key === "=") setChartZoom(state.chartView.zoom * 1.5);
  else if (event.key === "-") setChartZoom(state.chartView.zoom / 1.5);
  else if (event.key === "ArrowLeft") panChart(-1);
  else if (event.key === "ArrowRight") panChart(1);
  else if (event.key === "0" || event.key === "Home") resetChartView();
  else return;
  event.preventDefault();
});

window.addEventListener("resize", () => {
  clearTimeout(state.chartResizeTimer);
  state.chartResizeTimer = setTimeout(() => {
    if (state.chartModel) drawHistoryChart();
  }, 100);
});

function renderIssues(issues) {
  ui.diagnosticsRegion.hidden = issues.length === 0;
  ui.issueBody.replaceChildren();
  issues.forEach((issue) => {
    const row = document.createElement("tr");
    const [direction, asset] = issue.scope.split(":", 2);
    const scope = asset ? `${direction === "BUY" ? "买入" : direction === "SELL" ? "卖出" : direction} ${asset}` : issue.scope;
    row.append(createElement("td", "", scope));
    row.append(createElement("td", "", formatTime(issue.occurred_at)));
    const reason = createElement("td", "", ISSUE_REASON_LABELS[issue.reason_code] || issue.reason_code);
    reason.title = `${ISSUE_REASON_DETAILS[issue.reason_code] || issue.reason_code}（${issue.reason_code}）`;
    row.append(reason);
    ui.issueBody.append(row);
  });
  ui.issueCount.textContent = `${issues.length} 条`;
  ui.issueEmpty.hidden = issues.length > 0;
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") loadView();
});

loadView();
