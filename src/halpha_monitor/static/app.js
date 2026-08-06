"use strict";

const MONITOR_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;

function monitorIdFromLocation() {
  const monitorId = new URL(window.location.href).searchParams.get("monitor_id");
  return monitorId && MONITOR_ID_PATTERN.test(monitorId) ? monitorId : null;
}

const ui = {
  workspace: document.querySelector("#workspace"),
  serviceStatus: document.querySelector("#service-status"),
  collectionLoad: document.querySelector("#collection-load"),
  networkRequests: document.querySelector("#network-requests"),
  collectionCadence: document.querySelector("#collection-cadence"),
  lastRefresh: document.querySelector("#last-refresh"),
  monitorList: document.querySelector("#monitor-list"),
  monitoringCount: document.querySelector("#monitoring-count"),
  healthyCount: document.querySelector("#healthy-count"),
  staleCount: document.querySelector("#stale-count"),
  failedCount: document.querySelector("#failed-count"),
  disabledCount: document.querySelector("#disabled-count"),
  summaryCutoff: document.querySelector("#summary-cutoff"),
  monitorTitle: document.querySelector("#monitor-title"),
  monitorDescription: document.querySelector("#monitor-description"),
  monitorMethodNote: document.querySelector("#monitor-method-note"),
  monitorState: document.querySelector("#monitor-state"),
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
  tableScrollControls: document.querySelector("#table-scroll-controls"),
  tableScrollLeft: document.querySelector("#table-scroll-left"),
  tableScrollRight: document.querySelector("#table-scroll-right"),
  quoteScroll: document.querySelector("#quote-scroll"),
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
  monitorId: monitorIdFromLocation(),
  seriesKey: null,
  hours: 6,
  filters: {},
  refreshTimer: null,
  request: null,
  configurationDirty: false,
  configurationSubmitting: false,
  controlSubmitting: false,
  pendingControl: null,
  pendingConfigurationRunAfter: null,
  latestRunId: null,
  chartModel: null,
  chartGeometry: null,
  chartView: { seriesKey: null, zoom: 1, start: 0 },
  chartDrag: null,
  chartResizeTimer: null,
  tableSort: null,
  tableResizeTimer: null,
};

const TABLE_TEXT_COLLATOR = new Intl.Collator("zh-CN", {
  numeric: true,
  sensitivity: "base",
});

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
  RADAR_BACKOFF_ACTIVE: "异动雷达退避中",
  RADAR_CANDIDATE_COLLECTION_FAILED: "候选详查失败",
  RADAR_FUTURES_ROWS_STALE: "合约数据陈旧",
  RADAR_HTTP_THROTTLED_418: "公开接口已封禁并退避",
  RADAR_HTTP_THROTTLED_429: "公开接口限流并退避",
  RADAR_KLINES_EMPTY: "闭合 K 线为空",
  RADAR_KLINES_INSUFFICIENT: "闭合 K 线不足",
  RADAR_KLINES_NON_CONTIGUOUS: "闭合 K 线不连续",
  RADAR_KLINES_STALE: "闭合 K 线陈旧",
  RADAR_OI_EMPTY: "OI 历史为空",
  RADAR_OI_INSUFFICIENT: "OI 历史不足",
  RADAR_OI_STALE: "OI 历史陈旧",
  RADAR_SOURCE_ROWS_MALFORMED: "来源存在畸形记录",
  RADAR_TICKER_ROWS_STALE: "行情记录陈旧",
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
  RADAR_BACKOFF_ACTIVE: "Binance 上游退避窗口尚未结束，本轮没有继续发送公开行情请求",
  RADAR_CANDIDATE_COLLECTION_FAILED: "单个候选详查发生隔离失败，其他候选仍可展示",
  RADAR_FUTURES_ROWS_STALE: "资金费率来源时间超过有效截止点，相关字段保持为空",
  RADAR_HTTP_THROTTLED_418: "Binance 返回 HTTP 418；采集器遵守 Retry-After 并停止本轮后续请求",
  RADAR_HTTP_THROTTLED_429: "Binance 返回 HTTP 429；采集器遵守 Retry-After 并停止本轮后续请求",
  RADAR_KLINES_EMPTY: "没有取得通过校验的已闭合 5 分钟 K 线，未生成评分",
  RADAR_KLINES_INSUFFICIENT: "连续闭合 5 分钟 K 线少于计算窗口，未生成评分",
  RADAR_KLINES_NON_CONTIGUOUS: "K 线窗口存在空档，未插值或跨空档计算",
  RADAR_KLINES_STALE: "最近闭合 K 线超过有效截止点，未沿用旧数据",
  RADAR_OI_EMPTY: "同名 USDⓈ-M 合约没有返回 OI 历史",
  RADAR_OI_INSUFFICIENT: "OI 历史不足以计算 15 分钟变化率",
  RADAR_OI_STALE: "最新 OI 时间超过有效截止点，变化率保持为空",
  RADAR_SOURCE_ROWS_MALFORMED: "公开来源中部分记录未通过字段或数值校验，已隔离",
  RADAR_TICKER_ROWS_STALE: "部分滚动行情超过有效截止点，已从本轮候选中排除",
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

function syncMonitorLocation(monitorId) {
  const url = new URL(window.location.href);
  if (monitorId) url.searchParams.set("monitor_id", monitorId);
  else url.searchParams.delete("monitor_id");
  const nextLocation = `${url.pathname}${url.search}${url.hash}`;
  if (`${window.location.pathname}${window.location.search}${window.location.hash}` !== nextLocation) {
    window.history.replaceState(window.history.state, "", nextLocation);
  }
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
    let response = await fetch(queryUrl(), {
      signal: state.request.signal,
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    if (response.status === 404 && state.monitorId) {
      state.monitorId = null;
      state.seriesKey = null;
      state.filters = {};
      state.tableSort = null;
      syncMonitorLocation(null);
      response = await fetch(queryUrl(), {
        signal: state.request.signal,
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
    }
    if (!response.ok) throw new Error(`HTTP_${response.status}`);
    const payload = await response.json();
    state.monitorId = payload.monitor.monitor_id;
    state.seriesKey = payload.selected_series_key;
    state.filters = payload.monitor.selected_filters;
    state.latestRunId = payload.monitor.latest_run?.run_id ?? null;
    syncMonitorLocation(state.monitorId);
    render(payload);
    ui.pageError.hidden = true;
    return true;
  } catch (error) {
    if (error.name === "AbortError") return false;
    ui.pageError.hidden = false;
    ui.pageError.textContent = `页面加载失败 · ${formatTime(new Date().toISOString())} · ${error.message}`;
    setStatus(ui.serviceStatus, "FAILED", "页面加载失败");
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
    payload.current_issues,
    payload.monitor.selected_filters,
    payload.monitor.data_status,
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
  const load = payload.collection_load;
  ui.collectionLoad.dataset.load = load.level;
  ui.collectionLoad.textContent = `负载${load.level_label} ${load.utilization_percent}%`;
  ui.collectionLoad.title = load.definition;
  ui.collectionLoad.setAttribute(
    "aria-label",
    `采集负载${load.level_label}，占用${load.utilization_percent}%`,
  );
  ui.networkRequests.textContent = load.network_requests === null
    ? "近60秒请求未计量"
    : `近60秒请求 ${load.network_requests} 次`;
  ui.networkRequests.title = `${load.measured_monitor_count}/${load.enabled_count} 项启用监控已接入请求计数`;
  ui.collectionCadence.textContent = `计划 ${Number(load.planned_runs_per_minute).toFixed(1)} 轮/分`;
  ui.collectionCadence.title = "按各启用监控的采集周期计算；一轮可能包含多个公开 HTTP 请求。";
  ui.lastRefresh.textContent = formatTime(load.latest_completed_at);
  ui.summaryCutoff.textContent = `状态统计截止：${formatTime(payload.server_time)}`;
  ui.monitoringCount.textContent = String(payload.monitors.filter(
    (item) => item.enabled
  ).length);
  ui.healthyCount.textContent = String(payload.monitors.filter(
    (item) => item.enabled && ["CURRENT", "CURRENT_WITH_NOTICES", "CURRENT_WITH_GAPS"].includes(item.data_status.kind)
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
    const status = createElement(
      "span",
      "monitor-link-status",
      monitor.operational_status.label,
    );
    status.dataset.status = monitor.operational_status.tone;
    button.append(status);
    button.addEventListener("click", async () => {
      const previous = {
        monitorId: state.monitorId,
        filters: state.filters,
        seriesKey: state.seriesKey,
        tableSort: state.tableSort,
        tableScrollLeft: ui.quoteScroll.scrollLeft,
      };
      state.monitorId = monitor.monitor_id;
      state.filters = {};
      state.seriesKey = null;
      state.tableSort = null;
      ui.quoteScroll.scrollLeft = 0;
      if (!await loadView({ preserveSeries: false })) {
        state.monitorId = previous.monitorId;
        state.filters = previous.filters;
        state.seriesKey = previous.seriesKey;
        state.tableSort = previous.tableSort;
        ui.quoteScroll.scrollLeft = previous.tableScrollLeft;
      }
    });
    ui.monitorList.append(button);
  });
}

function renderContext(monitor) {
  ui.monitorTitle.textContent = monitor.display_name;
  const description = String(monitor.description || "").trim();
  ui.monitorDescription.textContent = description;
  ui.monitorDescription.hidden = !description || monitor.show_description === false;
  const methodNote = String(monitor.method_note || "").trim();
  ui.monitorMethodNote.textContent = methodNote;
  ui.monitorMethodNote.hidden = !methodNote;
  setStatus(
    ui.monitorState,
    monitor.operational_status.tone,
    monitor.operational_status.label,
  );
  const cutoff = monitor.data_run?.completed_at;
  ui.dataCutoff.textContent = `${monitor.data_status.cutoff_label}：${cutoff ? formatTime(cutoff) : "—"}`;
  ui.dataCutoff.dataset.status = monitor.data_status.tone;
}

function renderControl(monitor) {
  if (state.controlSubmitting) return;
  if (
    state.pendingControl?.monitorId === monitor.monitor_id
    && monitor.latest_run?.run_id > state.pendingControl.runAfter
    && monitor.latest_run.status !== "RUNNING"
  ) {
    ui.monitorControlStatus.textContent = ["SUCCESS", "PARTIAL"].includes(monitor.latest_run.status)
      ? "已开启，首轮采集完成"
      : "已开启，首轮采集失败；系统将自动重试";
    state.pendingControl = null;
  }
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
  const controlMonitorId = state.monitorId;
  const runAfter = state.latestRunId ?? 0;
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
    state.pendingControl = payload.enabled && payload.refresh_requested
      ? { monitorId: controlMonitorId, runAfter }
      : null;
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
      setConfigurationStatus("新条件采集失败，仍显示上次已校验报价", "error");
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
  const missing = (reason = declaredReason || "来源未返回通过校验的值，未使用替代数据。") => ({
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
      text: `${column.show_sign !== false && numeric >= 0 ? "+" : ""}${numeric.toFixed(4)}%`,
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

function emptyTableMessage(dataStatus) {
  if ([
    "HISTORICAL",
    "STALE",
    "COLLECTING_PREVIOUS",
    "DISABLED_WITH_HISTORY",
  ].includes(dataStatus.kind)) {
    return "当前历史快照中没有该范围的已校验记录；未使用任何替代值。";
  }
  if (dataStatus.kind === "DISABLED_EMPTY") {
    return "监控尚未开启或没有已采集数据；未使用任何替代值。";
  }
  return "当前范围尚无采集结果；未使用任何替代值。";
}

const GLOBAL_ISSUE_SCOPES = new Set([
  "monitor",
  "universe",
  "exchange-info",
  "ticker-24h",
  "futures",
]);

function rowEntityKeys(row) {
  return new Set([
    row.entity_key,
    row.asset,
    row.symbol,
  ].filter(Boolean).map(String));
}

function scopeEntity(scope) {
  const parts = String(scope).split(":");
  return parts.length > 1 && ["BUY", "SELL"].includes(parts[0])
    ? parts[1]
    : parts[0];
}

function issueMatchesTable(issue, filters, rows) {
  const scope = String(issue.scope);
  if (GLOBAL_ISSUE_SCOPES.has(scope)) return true;
  const directionalScope = /^(BUY|SELL):(.+)$/.exec(scope);
  if (directionalScope) {
    return !filters.trade_type || directionalScope[1] === filters.trade_type;
  }
  if (filters.symbol) {
    if (scope === filters.symbol) return true;
    if (filters.time_range && scope === `${filters.symbol}:${filters.time_range}`) return true;
  }
  if (filters.trade_type) {
    if (/^[A-Z0-9]{2,20}$/.test(scope)) return true;
  }
  const entity = scopeEntity(scope);
  return rows.some((row) => rowEntityKeys(row).has(entity));
}

function rowAlreadyMarksIssue(issue, rows) {
  const entity = scopeEntity(issue.scope);
  return rows.some((row) => (
    rowEntityKeys(row).has(entity)
    && row.missing_reasons
    && Object.keys(row.missing_reasons).length > 0
  ));
}

function issueScopeLabel(scope) {
  if (scope === "monitor") return "全部范围";
  const parts = String(scope).split(":");
  if (parts.length > 1 && ["BUY", "SELL"].includes(parts[0])) return parts[1];
  return String(scope);
}

function tableIssueGroups(issues, filters, rows) {
  const groups = new Map();
  issues
    .filter((issue) => issueMatchesTable(issue, filters, rows))
    .filter((issue) => !rowAlreadyMarksIssue(issue, rows))
    .forEach((issue) => {
      const key = `${issue.classification}|${issue.reason_code}`;
      if (!groups.has(key)) groups.set(key, { ...issue, scopes: [] });
      groups.get(key).scopes.push(issueScopeLabel(issue.scope));
    });
  return [...groups.values()].map((group) => ({
    ...group,
    scopes: [...new Set(group.scopes)],
  }));
}

function appendTableIssueRow(columns, group) {
  const tr = document.createElement("tr");
  tr.className = "table-notice-row";
  tr.dataset.tone = group.tone;
  const td = document.createElement("td");
  td.colSpan = Math.max(columns.length, 1);
  const scopes = group.scopes.join("、");
  const expectedAbsence = group.classification === "EXPECTED_ABSENCE";
  const heading = expectedAbsence
    ? `本轮无报价：${scopes}`
    : `本轮受影响：${scopes}`;
  const detail = expectedAbsence
    ? ISSUE_REASON_DETAILS.NO_ELIGIBLE_C2C_AD
    : `${ISSUE_REASON_LABELS[group.reason_code] || group.reason_code}；对应范围未展示未通过校验的值。`;
  td.append(createElement("strong", "table-notice-title", heading));
  td.append(createElement("span", "table-notice-detail", detail));
  td.title = `${detail}（${group.reason_code}）`;
  tr.append(td);
  ui.quoteBody.append(tr);
}

function tableSortValue(row, column) {
  const value = row[column.key];
  if (value === null || value === undefined || value === "") return null;
  if (column.kind === "number" || column.kind === "percent") {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }
  if (column.kind === "time") {
    const timestamp = Date.parse(value);
    return Number.isFinite(timestamp) ? timestamp : null;
  }
  return String(value);
}

function sortTableRows(rows, columns) {
  const activeSort = state.tableSort;
  if (!activeSort || activeSort.monitorId !== state.monitorId) return [...rows];
  const column = columns.find((item) => item.key === activeSort.columnKey);
  if (!column) return [...rows];
  return rows
    .map((row, index) => ({ row, index, value: tableSortValue(row, column) }))
    .sort((left, right) => {
      const leftMissing = left.value === null;
      const rightMissing = right.value === null;
      if (leftMissing || rightMissing) {
        if (leftMissing && rightMissing) return left.index - right.index;
        return leftMissing ? 1 : -1;
      }
      const comparison = typeof left.value === "number"
        ? left.value - right.value
        : TABLE_TEXT_COLLATOR.compare(left.value, right.value);
      if (comparison === 0) return left.index - right.index;
      return activeSort.direction === "ascending" ? comparison : -comparison;
    })
    .map((item) => item.row);
}

function nextSortDirection(columnKey) {
  if (
    state.tableSort?.monitorId === state.monitorId
    && state.tableSort.columnKey === columnKey
  ) {
    return state.tableSort.direction === "ascending" ? "descending" : "ascending";
  }
  return "ascending";
}

function marketDestination(row) {
  if (state.monitorId !== "binance-altcoin-radar") return null;
  const symbol = String(row.symbol || "").toUpperCase();
  if (!/^[A-Z0-9]{1,24}USDT$/.test(symbol)) return null;
  if (row.data_scope_label === "现货 + 合约") {
    return {
      url: `https://www.binance.com/zh-CN/futures/${encodeURIComponent(symbol)}`,
      label: "Binance USDⓈ-M 合约行情",
    };
  }
  const baseAsset = String(row.base_asset || symbol.slice(0, -4)).toUpperCase();
  if (!/^[A-Z0-9]{1,24}$/.test(baseAsset)) return null;
  return {
    url: `https://www.binance.com/zh-CN/trade/${encodeURIComponent(baseAsset)}_USDT`,
    label: "本轮未确认同名合约，打开 Binance 现货行情",
  };
}

function updateTableScrollControls() {
  const maximum = Math.max(0, ui.quoteScroll.scrollWidth - ui.quoteScroll.clientWidth);
  const overflowed = maximum > 2;
  ui.tableScrollControls.hidden = !overflowed;
  ui.tableScrollLeft.disabled = !overflowed || ui.quoteScroll.scrollLeft <= 2;
  ui.tableScrollRight.disabled = !overflowed || ui.quoteScroll.scrollLeft >= maximum - 2;
}

function scrollTableHorizontally(direction) {
  const distance = Math.max(280, Math.round(ui.quoteScroll.clientWidth * 0.72));
  ui.quoteScroll.scrollBy({ left: direction * distance, behavior: "smooth" });
}

function renderTable(
  columns,
  rows,
  selectedSeries,
  issues,
  filters,
  dataStatus,
) {
  ui.quoteHead.replaceChildren();
  columns.forEach((column) => {
    const th = createElement("th", column.priority === "secondary" ? "col-secondary" : "");
    th.scope = "col";
    th.dataset.kind = column.kind;
    const active = state.tableSort?.monitorId === state.monitorId
      && state.tableSort.columnKey === column.key;
    const direction = active ? state.tableSort.direction : null;
    th.ariaSort = direction || "none";
    const button = createElement("button", "table-sort-button");
    button.type = "button";
    button.dataset.columnKey = column.key;
    const nextDirection = nextSortDirection(column.key);
    button.setAttribute(
      "aria-label",
      `${column.label}，${direction === "ascending" ? "当前正序" : direction === "descending" ? "当前倒序" : "当前未排序"}，点击切换为${nextDirection === "ascending" ? "正序" : "倒序"}`,
    );
    button.append(createElement("span", "table-sort-label", column.label));
    if (column.description) {
      button.title = column.description;
      button.setAttribute("aria-description", column.description);
      const help = createElement("span", "table-column-help", "ⓘ");
      help.setAttribute("aria-hidden", "true");
      button.append(help);
    }
    const indicator = createElement(
      "span",
      "table-sort-indicator",
      direction === "ascending" ? "↑" : direction === "descending" ? "↓" : "↕",
    );
    indicator.setAttribute("aria-hidden", "true");
    button.append(indicator);
    button.addEventListener("click", () => {
      state.tableSort = {
        monitorId: state.monitorId,
        columnKey: column.key,
        direction: nextSortDirection(column.key),
      };
      renderTable(columns, rows, selectedSeries, issues, filters, dataStatus);
    });
    th.append(button);
    ui.quoteHead.append(th);
  });
  ui.quoteBody.replaceChildren();
  sortTableRows(rows, columns).forEach((row) => {
    const tr = document.createElement("tr");
    tr.tabIndex = 0;
    if (row.row_tone) tr.dataset.tone = String(row.row_tone);
    tr.setAttribute("aria-selected", String(row.series_key === selectedSeries));
    const destination = marketDestination(row);
    let marketAnchor = null;
    const select = async () => {
      const previous = state.seriesKey;
      state.seriesKey = row.series_key;
      if (!await loadView()) state.seriesKey = previous;
    };
    columns.forEach((column) => {
      const rendered = cellValue(row, column);
      const td = createElement(
        "td",
        column.priority === "secondary" ? "col-secondary" : "",
      );
      td.dataset.kind = column.kind;
      if (rendered.missing) {
        td.textContent = rendered.text;
        td.dataset.missing = "true";
        td.title = rendered.reason;
        td.setAttribute("aria-label", `${column.label}：无已校验值。${rendered.reason}`);
      } else if (destination && column.key === "symbol") {
        marketAnchor = createElement("a", "market-symbol-link", rendered.text);
        marketAnchor.href = destination.url;
        marketAnchor.target = "_blank";
        marketAnchor.rel = "noopener noreferrer";
        marketAnchor.title = `${destination.label}（新标签页）`;
        marketAnchor.setAttribute("aria-label", `${row.symbol}：${destination.label}，新标签页`);
        marketAnchor.addEventListener("click", () => { void select(); });
        td.append(marketAnchor);
        const externalIcon = createElement("span", "market-link-icon", "↗");
        externalIcon.setAttribute("aria-hidden", "true");
        td.append(externalIcon);
      } else {
        td.textContent = rendered.text;
      }
      tr.append(td);
    });
    if (destination && marketAnchor) {
      tr.classList.add("market-row");
      tr.title = `${destination.label}（新标签页）`;
      tr.setAttribute("aria-label", `${row.symbol}：${destination.label}，新标签页`);
    }
    tr.addEventListener("click", (event) => {
      if (event.target.closest("a")) return;
      if (marketAnchor) marketAnchor.click();
      else void select();
    });
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || (!marketAnchor && event.key === " ")) {
        event.preventDefault();
        if (marketAnchor) marketAnchor.click();
        else void select();
      }
    });
    ui.quoteBody.append(tr);
  });
  const issueGroups = tableIssueGroups(issues || [], filters, rows);
  issueGroups.forEach((group) => appendTableIssueRow(columns, group));
  ui.quoteEmpty.hidden = rows.length > 0 || issueGroups.length > 0;
  ui.quoteEmpty.textContent = emptyTableMessage(dataStatus);
  requestAnimationFrame(updateTableScrollControls);
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
    state.chartGeometry = null;
    ui.historyChart.setAttribute(
      "aria-label",
      `${ui.historySeries.textContent}，当前时间范围没有历史样本。`,
    );
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
  clearTimeout(state.tableResizeTimer);
  state.chartResizeTimer = setTimeout(() => {
    if (state.chartModel) drawHistoryChart();
  }, 100);
  state.tableResizeTimer = setTimeout(updateTableScrollControls, 100);
});

ui.quoteScroll.addEventListener("scroll", updateTableScrollControls, { passive: true });
ui.tableScrollLeft.addEventListener("click", () => scrollTableHorizontally(-1));
ui.tableScrollRight.addEventListener("click", () => scrollTableHorizontally(1));

function renderIssues(issues) {
  const diagnosticIssues = issues.filter(
    (issue) => issue.classification !== "EXPECTED_ABSENCE"
  );
  ui.diagnosticsRegion.hidden = diagnosticIssues.length === 0;
  ui.issueBody.replaceChildren();
  diagnosticIssues.forEach((issue) => {
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
  ui.issueCount.textContent = `${diagnosticIssues.length} 条`;
  ui.issueEmpty.hidden = diagnosticIssues.length > 0;
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") loadView();
});

loadView();
