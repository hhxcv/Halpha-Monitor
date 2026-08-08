"use strict";

const MONITOR_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;
const RADAR_MONITOR_ID = "binance-altcoin-radar";
const RADAR_TAB_LOCATION_VALUES = {
  TABLE: "candidates",
  POSITION: "position",
  HISTORY: "history",
  EVALUATION: "evaluation",
};

function monitorIdFromLocation() {
  const monitorId = new URL(window.location.href).searchParams.get("monitor_id");
  return monitorId && MONITOR_ID_PATTERN.test(monitorId) ? monitorId : null;
}

function radarTabFromLocation() {
  const requested = new URL(window.location.href).searchParams.get("view");
  const entry = Object.entries(RADAR_TAB_LOCATION_VALUES)
    .find(([, value]) => value === requested);
  return entry?.[0] || "TABLE";
}

const MONITOR_RAIL_STORAGE_KEY = "halpha-monitor-rail-collapsed";

function storedMonitorRailCollapsed() {
  try {
    return window.localStorage.getItem(MONITOR_RAIL_STORAGE_KEY) === "true";
  } catch (_error) {
    return false;
  }
}

const ui = {
  appShell: document.querySelector(".app-shell"),
  workspace: document.querySelector("#workspace"),
  serviceStatus: document.querySelector("#service-status"),
  collectionLoad: document.querySelector("#collection-load"),
  networkRequests: document.querySelector("#network-requests"),
  collectionCadence: document.querySelector("#collection-cadence"),
  lastRefresh: document.querySelector("#last-refresh"),
  monitorRailToggle: document.querySelector("#monitor-rail-toggle"),
  monitorRailToggleIcon: document.querySelector("#monitor-rail-toggle-icon"),
  monitorList: document.querySelector("#monitor-list"),
  monitorTitle: document.querySelector("#monitor-title"),
  monitorDescription: document.querySelector("#monitor-description"),
  monitorMethodNote: document.querySelector("#monitor-method-note"),
  btcIntelligence: document.querySelector("#btc-intelligence"),
  btcCurrentPrice: document.querySelector("#btc-current-price"),
  btcPriceState: document.querySelector("#btc-price-state"),
  btcRegime: document.querySelector("#btc-regime"),
  btcRegimeDetail: document.querySelector("#btc-regime-detail"),
  btcClockStrip: document.querySelector("#btc-clock-strip"),
  btcMonthlyTarget: document.querySelector("#btc-monthly-target"),
  btcMonthlyFormed: document.querySelector("#btc-monthly-formed"),
  btcMonthlyMetrics: document.querySelector("#btc-monthly-metrics"),
  btcMonthlyNote: document.querySelector("#btc-monthly-note"),
  btcDailyAgreement: document.querySelector("#btc-daily-agreement"),
  btcDailyState: document.querySelector("#btc-daily-state"),
  btcDailyComponents: document.querySelector("#btc-daily-components"),
  btcDailyTransition: document.querySelector("#btc-daily-transition"),
  btcStructureEnvironment: document.querySelector("#btc-structure-environment"),
  btcStructureCutoff: document.querySelector("#btc-structure-cutoff"),
  btcZoneBody: document.querySelector("#btc-zone-body"),
  btcZoneEmpty: document.querySelector("#btc-zone-empty"),
  btcSmartBody: document.querySelector("#btc-smart-body"),
  btcSmartEmpty: document.querySelector("#btc-smart-empty"),
  btcLedgerMetrics: document.querySelector("#btc-ledger-metrics"),
  btcLedgerStart: document.querySelector("#btc-ledger-start"),
  btcEventBody: document.querySelector("#btc-event-body"),
  btcEventEmpty: document.querySelector("#btc-event-empty"),
  btcInterpretation: document.querySelector("#btc-interpretation"),
  monitorState: document.querySelector("#monitor-state"),
  diagnosticsOpen: document.querySelector("#diagnostics-open"),
  diagnosticsOpenCount: document.querySelector("#diagnostics-open-count"),
  monitorRefreshButton: document.querySelector("#monitor-refresh-button"),
  monitorControlButton: document.querySelector("#monitor-control-button"),
  monitorControlStatus: document.querySelector("#monitor-control-status"),
  filtersRegion: document.querySelector("#filters"),
  historyWindowField: document.querySelector(".history-window-field"),
  configurationRegion: document.querySelector("#configuration-region"),
  configurationForm: document.querySelector("#configuration-form"),
  configurationFields: document.querySelector("#configuration-fields"),
  configurationSubmit: document.querySelector("#configuration-submit"),
  configurationStatus: document.querySelector("#configuration-status"),
  filters: document.querySelector("#dynamic-filters"),
  buybackStockSearchField: document.querySelector("#buyback-stock-search-field"),
  buybackStockSearch: document.querySelector("#buyback-stock-search"),
  eventSearchField: document.querySelector("#event-search-field"),
  eventSearch: document.querySelector("#event-search"),
  radarPriceStateField: document.querySelector("#radar-price-state-field"),
  radarPriceState: document.querySelector("#radar-price-state"),
  timeWindow: document.querySelector("#time-window"),
  dataCutoff: document.querySelector("#data-cutoff"),
  quoteScroll: document.querySelector("#quote-scroll"),
  quoteHorizontalScrollbar: document.querySelector("#quote-horizontal-scrollbar"),
  quoteHorizontalScrollbarTrack: document.querySelector("#quote-horizontal-scrollbar-track"),
  quoteTableTitle: document.querySelector("#quote-table-title"),
  tablePagination: document.querySelector("#table-pagination"),
  tablePageSummary: document.querySelector("#table-page-summary"),
  tablePagePrevious: document.querySelector("#table-page-previous"),
  tablePageSelect: document.querySelector("#table-page-select"),
  tablePageTotal: document.querySelector("#table-page-total"),
  tablePageNext: document.querySelector("#table-page-next"),
  runSummary: document.querySelector("#run-summary"),
  quoteHead: document.querySelector("#quote-head"),
  quoteBody: document.querySelector("#quote-body"),
  quoteEmpty: document.querySelector("#quote-empty"),
  buybackOverviewRegion: document.querySelector("#buyback-overview-region"),
  buybackOverview: document.querySelector("#buyback-overview"),
  buybackSourceRegion: document.querySelector("#buyback-source-region"),
  buybackSourceSummary: document.querySelector("#buyback-source-summary"),
  buybackSourceGrid: document.querySelector("#buyback-source-grid"),
  eventSourceRegion: document.querySelector("#event-source-region"),
  eventSourceSummary: document.querySelector("#event-source-summary"),
  eventSourceDetails: document.querySelector("#event-source-details"),
  eventViewTabs: document.querySelector("#event-view-tabs"),
  eventUpcomingTab: document.querySelector("#event-upcoming-tab"),
  eventHistoryTab: document.querySelector("#event-history-tab"),
  eventHistoryCount: document.querySelector("#event-history-count"),
  radarViewTabs: document.querySelector("#radar-view-tabs"),
  radarTableTab: document.querySelector("#radar-table-tab"),
  radarPositionTab: document.querySelector("#radar-position-tab"),
  radarHistoryTab: document.querySelector("#radar-history-tab"),
  radarEvaluationTab: document.querySelector("#radar-evaluation-tab"),
  eventAttentionRegion: document.querySelector("#event-attention-region"),
  eventAttentionSummary: document.querySelector("#event-attention-summary"),
  eventSourceCutoff: document.querySelector("#event-source-cutoff"),
  eventAttentionCards: document.querySelector("#event-attention-cards"),
  eventAttentionEmpty: document.querySelector("#event-attention-empty"),
  macroIndicatorRegion: document.querySelector("#macro-indicator-region"),
  macroIndicatorCards: document.querySelector("#macro-indicator-cards"),
  eventCalendarRegion: document.querySelector("#event-calendar-region"),
  eventCalendarGrid: document.querySelector("#event-calendar-grid"),
  eventHistoryRegion: document.querySelector("#event-history-region"),
  eventHistorySummary: document.querySelector("#event-history-summary"),
  eventHistoryBody: document.querySelector("#event-history-body"),
  eventHistoryPagination: document.querySelector("#event-history-pagination"),
  eventHistoryPageSummary: document.querySelector("#event-history-page-summary"),
  eventHistoryPrevious: document.querySelector("#event-history-previous"),
  eventHistoryNext: document.querySelector("#event-history-next"),
  eventHistoryPageStatus: document.querySelector("#event-history-page-status"),
  eventHistoryEmpty: document.querySelector("#event-history-empty"),
  tableRegion: document.querySelector("#table-region"),
  historyRegion: document.querySelector("#history-region"),
  historyTitle: document.querySelector("#history-title"),
  historySeries: document.querySelector("#history-series"),
  historyChart: document.querySelector("#history-chart"),
  chartZoomOut: document.querySelector("#chart-zoom-out"),
  chartZoomIn: document.querySelector("#chart-zoom-in"),
  chartReset: document.querySelector("#chart-reset"),
  chartViewportStatus: document.querySelector("#chart-viewport-status"),
  collectionGapNote: document.querySelector("#collection-gap-note"),
  chartSummary: document.querySelector("#chart-summary"),
  evaluationRegion: document.querySelector("#evaluation-region"),
  evaluationTitle: document.querySelector("#evaluation-title"),
  evaluationMethodNote: document.querySelector("#evaluation-method-note"),
  evaluationOverview: document.querySelector("#evaluation-overview"),
  evaluationComparison: document.querySelector("#evaluation-comparison"),
  evaluationComparisonPeriod: document.querySelector("#evaluation-comparison-period"),
  evaluationComparisonOverview: document.querySelector("#evaluation-comparison-overview"),
  evaluationComparisonBody: document.querySelector("#evaluation-comparison-body"),
  evaluationComparisonEmpty: document.querySelector("#evaluation-comparison-empty"),
  evaluationGroupBody: document.querySelector("#evaluation-group-body"),
  evaluationGroupEmpty: document.querySelector("#evaluation-group-empty"),
  evaluationRecentBody: document.querySelector("#evaluation-recent-body"),
  evaluationRecentEmpty: document.querySelector("#evaluation-recent-empty"),
  diagnosticsDialog: document.querySelector("#diagnostics-dialog"),
  diagnosticsDialogClose: document.querySelector("#diagnostics-dialog-close"),
  diagnosticsDialogSubtitle: document.querySelector("#diagnostics-dialog-subtitle"),
  issueCount: document.querySelector("#issue-count"),
  issueScroll: document.querySelector("#issue-scroll"),
  issueBody: document.querySelector("#issue-body"),
  issueEmpty: document.querySelector("#issue-empty"),
  backToTop: document.querySelector("#back-to-top"),
  pageError: document.querySelector("#page-error"),
  buybackDetailDialog: document.querySelector("#buyback-detail-dialog"),
  buybackDetailClose: document.querySelector("#buyback-detail-close"),
  buybackDetailTitle: document.querySelector("#buyback-detail-title"),
  buybackDetailSubtitle: document.querySelector("#buyback-detail-subtitle"),
  buybackDetailStatus: document.querySelector("#buyback-detail-status"),
  buybackDetailContent: document.querySelector("#buyback-detail-content"),
  buybackFacts: document.querySelector("#buyback-facts"),
  buybackEvidenceLinks: document.querySelector("#buyback-evidence-links"),
  buybackEvidenceMeta: document.querySelector("#buyback-evidence-meta"),
  buybackEvidenceExcerpt: document.querySelector("#buyback-evidence-excerpt"),
  buybackReviewForm: document.querySelector("#buyback-review-form"),
  buybackReviewDecision: document.querySelector("#buyback-review-decision"),
  buybackReviewEventType: document.querySelector("#buyback-review-event-type"),
  buybackReviewProgramKey: document.querySelector("#buyback-review-program-key"),
  buybackReviewProgramStatus: document.querySelector("#buyback-review-program-status"),
  buybackReviewNote: document.querySelector("#buyback-review-note"),
  buybackReviewSubmit: document.querySelector("#buyback-review-submit"),
  buybackReviewHistory: document.querySelector("#buyback-review-history"),
  marketEventDetailDialog: document.querySelector("#market-event-detail-dialog"),
  marketEventDetailClose: document.querySelector("#market-event-detail-close"),
  marketEventDetailTitle: document.querySelector("#market-event-detail-title"),
  marketEventDetailSubtitle: document.querySelector("#market-event-detail-subtitle"),
  marketEventDetailFacts: document.querySelector("#market-event-detail-facts"),
  marketEventImpactReason: document.querySelector("#market-event-impact-reason"),
  marketEventDescription: document.querySelector("#market-event-description"),
  marketEventHowToRead: document.querySelector("#market-event-how-to-read"),
  marketEventDecisionRule: document.querySelector("#market-event-decision-rule"),
  marketEventDirectionSection: document.querySelector("#market-event-direction-section"),
  marketEventDirectionLabel: document.querySelector("#market-event-direction-label"),
  marketEventDirectionAction: document.querySelector("#market-event-direction-action"),
  marketEventDirectionFormula: document.querySelector("#market-event-direction-formula"),
  marketEventDirectionInputs: document.querySelector("#market-event-direction-inputs"),
  marketEventSourceLinks: document.querySelector("#market-event-source-links"),
  stockEvents: document.querySelector("#stock-events"),
  stockEventsScope: document.querySelector("#stock-events-scope"),
  stockEventsSelectButton: document.querySelector("#stock-events-select-button"),
  stockEventsCalendarTab: document.querySelector("#stock-events-calendar-tab"),
  stockEventsTimelineTab: document.querySelector("#stock-events-timeline-tab"),
  stockEventsWindow: document.querySelector("#stock-events-window"),
  stockEventsNotice: document.querySelector("#stock-events-notice"),
  stockEventsCalendarPanel: document.querySelector("#stock-events-calendar-panel"),
  stockEventsTimelinePanel: document.querySelector("#stock-events-timeline-panel"),
  stockCalendarPrevious: document.querySelector("#stock-calendar-previous"),
  stockCalendarNext: document.querySelector("#stock-calendar-next"),
  stockCalendarToday: document.querySelector("#stock-calendar-today"),
  stockCalendarTitle: document.querySelector("#stock-calendar-title"),
  stockCalendarGrid: document.querySelector("#stock-calendar-grid"),
  stockDayAgendaDate: document.querySelector("#stock-day-agenda-date"),
  stockDayAgendaCount: document.querySelector("#stock-day-agenda-count"),
  stockDayAgendaList: document.querySelector("#stock-day-agenda-list"),
  stockDayAgendaEmpty: document.querySelector("#stock-day-agenda-empty"),
  stockEventDetail: document.querySelector("#stock-event-detail"),
  stockEventDetailBadges: document.querySelector("#stock-event-detail-badges"),
  stockEventDetailTitle: document.querySelector("#stock-event-detail-title"),
  stockEventDetailStock: document.querySelector("#stock-event-detail-stock"),
  stockEventDetailSummary: document.querySelector("#stock-event-detail-summary"),
  stockEventDetailSource: document.querySelector("#stock-event-detail-source"),
  stockEventsTimeline: document.querySelector("#stock-events-timeline"),
  stockEventsTimelineEmpty: document.querySelector("#stock-events-timeline-empty"),
  stockEventsSourceSummary: document.querySelector("#stock-events-source-summary"),
  stockEventsSourceList: document.querySelector("#stock-events-source-list"),
  stockSelectorDialog: document.querySelector("#stock-selector-dialog"),
  stockSelectorClose: document.querySelector("#stock-selector-close"),
  stockSelectorCancel: document.querySelector("#stock-selector-cancel"),
  stockSelectorApply: document.querySelector("#stock-selector-apply"),
  stockSelectorSearch: document.querySelector("#stock-selector-search"),
  stockSelectorCount: document.querySelector("#stock-selector-count"),
  stockSelectorManualTab: document.querySelector("#stock-selector-manual-tab"),
  stockSelectorAutoTab: document.querySelector("#stock-selector-auto-tab"),
  stockSelectorAddForm: document.querySelector("#stock-selector-add-form"),
  stockSelectorAddQuery: document.querySelector("#stock-selector-add-query"),
  stockSelectorSuggestions: document.querySelector("#stock-selector-suggestions"),
  stockSelectorHelp: document.querySelector("#stock-selector-help"),
  stockSelectorList: document.querySelector("#stock-selector-list"),
  stockSelectorStatus: document.querySelector("#stock-selector-status"),
};

const state = {
  monitorId: monitorIdFromLocation(),
  seriesKey: null,
  hours: 6,
  filters: {},
  refreshTimer: null,
  observationTimer: null,
  observationMonitorId: null,
  request: null,
  configurationDirty: false,
  configurationSubmitting: false,
  controlSubmitting: false,
  manualRefreshSubmitting: false,
  pendingControl: null,
  pendingManualRefresh: null,
  pendingConfigurationRunAfter: null,
  latestRunId: null,
  chartModel: null,
  chartGeometry: null,
  chartView: { seriesKey: null, zoom: 1, start: 0 },
  chartDrag: null,
  chartResizeTimer: null,
  tableSort: null,
  tablePage: 1,
  monitorRailCollapsed: storedMonitorRailCollapsed(),
  projectionKind: "time_series",
  buybackStockQuery: "",
  buybackStockSearchTimer: null,
  eventQuery: "",
  eventSearchTimer: null,
  eventTab: "UPCOMING",
  radarTab: radarTabFromLocation(),
  radarPriceState: "*",
  viewPayload: null,
  eventHistoryPage: 1,
  marketEventPayload: null,
  stockEventPayload: null,
  stockEventView: "CALENDAR",
  stockCalendarMonth: null,
  stockSelectedDate: null,
  stockSelectedEventId: null,
  stockSelectedCodes: null,
  stockSelectorTab: "MANUAL",
  stockSelectorDraftCodes: new Set(),
  stockSelectorDraftManual: new Set(),
  stockSelectorOriginalManual: new Set(),
  stockSelectorSubmitting: false,
  stockDirectorySuggestions: [],
  stockDirectorySuggestionIndex: -1,
  stockDirectorySelected: null,
  stockDirectoryKnown: new Map(),
  stockDirectorySearchSerial: 0,
  stockDirectorySearchTimer: null,
  buybackDetailEntityKey: null,
  buybackDetailRevisionNo: null,
  buybackReviewSubmitting: false,
};

const TABLE_TEXT_COLLATOR = new Intl.Collator("zh-CN", {
  numeric: true,
  sensitivity: "base",
});

const BUYBACK_TABLE_PAGE_SIZE = 50;
const MARKET_EVENT_TABLE_PAGE_SIZE = 50;
const RADAR_POSITION_TABLE_PAGE_SIZE = 50;
const MARKET_EVENT_HISTORY_PAGE_SIZE = 50;

const MONITOR_COMPACT_LABELS = {
  "binance-c2c-normalized": "C2C",
  "btc-market-intelligence": "BTC",
  "binance-altcoin-radar": "异动",
  "binance-btc-relationship": "BTC",
  "a-hk-buyback": "回购",
  "market-event-calendar": "事件",
  "stock-event-calendar": "个股",
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
  SMART_MONEY_COLLECTION_FAILED: "聪明钱采集隔离失败",
  BTC_INTELLIGENCE_HTTP_BACKOFF_ACTIVE: "现货公开接口退避中",
  BTC_INTELLIGENCE_HTTP_THROTTLED: "现货公开接口限流并退避",
  BTC_INTELLIGENCE_UPSTREAM_HTTP_ERROR: "现货公开接口响应异常",
  BTC_INTELLIGENCE_UPSTREAM_UNAVAILABLE: "现货公开接口暂时不可用",
  BTC_INTELLIGENCE_RESPONSE_INVALID: "现货公开接口响应无法解析",
  BTC_INTELLIGENCE_SOURCE_STALE: "现货闭合 K 线未更新到当前截止",
  BTC_INTELLIGENCE_KLINES_NON_CONTIGUOUS: "现货闭合 K 线不连续",
  BTC_INTELLIGENCE_KLINES_INSUFFICIENT: "现货闭合 K 线历史不足",
  BTC_INTELLIGENCE_TICKER_FAILED: "现货参考价暂时不可用",
  BTC_INTELLIGENCE_TICKER_SCHEMA_CHANGED: "现货参考价字段变化",
  BTC_INTELLIGENCE_SERIES_COLLECTION_FAILED: "现货 K 线采集隔离失败",
  BTC_INTELLIGENCE_KLINE_SCHEMA_CHANGED: "现货 K 线字段变化",
  BTC_INTELLIGENCE_KLINES_DUPLICATE: "现货闭合 K 线重复",
  BTC_INTELLIGENCE_KLINES_INVALID: "现货闭合 K 线数值无效",
  BTC_INTELLIGENCE_RESPONSE_TOO_LARGE: "现货公开接口响应超过上限",
  BTC_INTELLIGENCE_PAGINATION_STALLED: "现货 K 线分页没有前进",
  BTC_STRUCTURE_EVENT_MISSED_DURING_DOWNTIME: "停机期间事件未作为前向预测入账",
  RADAR_BACKOFF_ACTIVE: "异动雷达退避中",
  RADAR_CANDIDATE_COLLECTION_FAILED: "候选详查失败",
  RADAR_FUTURES_ROWS_STALE: "合约数据陈旧",
  RADAR_HTTP_THROTTLED_418: "公开接口已封禁并退避",
  RADAR_HTTP_THROTTLED_429: "公开接口限流并退避",
  RADAR_KLINES_EMPTY: "闭合 K 线为空",
  RADAR_KLINES_INSUFFICIENT: "闭合 K 线不足",
  RADAR_KLINES_NON_CONTIGUOUS: "闭合 K 线不连续",
  RADAR_KLINES_STALE: "闭合 K 线陈旧",
  RADAR_DAILY_COLLECTION_FAILED: "日线价格位置采集失败",
  RADAR_DAILY_COMPUTATION_INVALID: "日线价格位置计算失败",
  RADAR_DAILY_HISTORY_INSUFFICIENT: "日线上市历史不足",
  RADAR_DAILY_KLINES_EMPTY: "闭合日线为空",
  RADAR_DAILY_KLINES_NON_CONTIGUOUS: "闭合日线不连续",
  RADAR_DAILY_KLINES_SCHEMA_INVALID: "日线响应结构变化",
  RADAR_DAILY_KLINES_STALE: "闭合日线未更新",
  RADAR_DAILY_KLINES_UNAVAILABLE: "日线价格位置暂缺",
  RADAR_DAILY_RANGE_INVALID: "日线请求范围无效",
  RADAR_DAILY_SOURCE_ROWS_MALFORMED: "部分日线记录未通过校验",
  RADAR_DAILY_SYMBOL_INVALID: "合约代码无效",
  RADAR_OI_EMPTY: "OI 历史为空",
  RADAR_OI_INSUFFICIENT: "OI 历史不足",
  RADAR_OI_STALE: "OI 历史陈旧",
  RADAR_SOURCE_ROWS_MALFORMED: "来源存在畸形记录",
  RADAR_TICKER_ROWS_STALE: "行情记录陈旧",
  BUYBACK_BACKOFF_ACTIVE: "公开来源暂时无法读取",
  BUYBACK_DOCUMENT_RUN_LIMIT_REACHED: "部分 A 股公告原文尚未获取",
  BUYBACK_DOCUMENTS_INCOMPLETE: "部分 A 股公告原文尚未获取",
  BUYBACK_PDF_TEXT_EMPTY: "公告原文无法读取文字",
  BUYBACK_HKEX_CURRENCY_INCONSISTENT: "港交所日报币种不一致",
  BUYBACK_HKEX_REPORTS_INCOMPLETE: "部分港交所日报未能读取",
  BUYBACK_A_REFERENCE_JSON_INVALID: "A股回购参考数据格式异常",
  BUYBACK_A_REFERENCE_SCHEMA_CHANGED: "A股回购参考数据字段变化",
  BUYBACK_A_REFERENCE_CONTENT_TYPE_INVALID: "A股回购参考数据响应异常",
  BUYBACK_HK_REFERENCE_JSON_INVALID: "港股行情参考格式异常",
  BUYBACK_HK_REFERENCE_SCHEMA_CHANGED: "港股行情参考字段变化",
  BUYBACK_HK_REFERENCE_EMPTY: "港股行情参考没有返回目标证券",
  BUYBACK_HK_REFERENCE_CONTENT_TYPE_INVALID: "港股行情参考响应异常",
  BUYBACK_MARKET_REFERENCE_JSON_INVALID: "行情与业绩参考格式异常",
  BUYBACK_MARKET_REFERENCE_TEXT_INVALID: "备用行情参考格式异常",
  BUYBACK_MARKET_REFERENCE_SCHEMA_CHANGED: "行情与业绩参考字段变化",
  BUYBACK_MARKET_REFERENCE_EMPTY: "行情与业绩参考没有返回目标证券",
  BUYBACK_MARKET_REFERENCE_CONTENT_TYPE_INVALID: "行情与业绩参考响应异常",
  BUYBACK_MARKET_REFERENCE_COUNT_INVALID: "行情与业绩参考请求范围超出上限",
  BUYBACK_MARKET_REFERENCE_INCOMPLETE: "部分证券暂时没有行情或业绩参考",
  BUYBACK_MARKET_PERFORMANCE_UNAVAILABLE: "业绩参考暂时不可用",
  BUYBACK_FINANCIAL_REFERENCE_JSON_INVALID: "公开业绩参考格式异常",
  BUYBACK_FINANCIAL_REFERENCE_SCHEMA_CHANGED: "公开业绩参考字段变化",
  BUYBACK_FINANCIAL_REFERENCE_TRUNCATED: "公开业绩参考返回不完整",
  BUYBACK_FINANCIAL_REFERENCE_EMPTY: "没有取得公开业绩参考",
  BUYBACK_FINANCIAL_REFERENCE_COUNT_INVALID: "公开业绩参考请求范围超出上限",
  BUYBACK_FINANCIAL_REFERENCE_CONTENT_TYPE_INVALID: "公开业绩参考响应异常",
  BUYBACK_FINANCIAL_REFERENCE_INCOMPLETE: "部分证券暂时没有公开业绩参考",
  MARKET_EVENTS_BEA_JSON_INVALID: "BEA发布日程格式异常",
  MARKET_EVENTS_BEA_SCHEMA_CHANGED: "BEA发布日程字段变化",
  MARKET_EVENTS_BEA_RELEASE_DATE_INVALID: "BEA发布时间异常",
  MARKET_EVENTS_NYFED_SCHEMA_CHANGED: "纽约联储事件日历格式变化",
  MARKET_EVENTS_NYFED_MONTH_MISMATCH: "纽约联储事件日历月份不符",
  MARKET_EVENTS_NYFED_EVENT_TIME_MISSING: "纽约联储事件缺少发布时间",
  MARKET_EVENTS_FOMC_SCHEMA_CHANGED: "美联储议息日历格式变化",
  MARKET_EVENTS_FOMC_DATE_INVALID: "美联储议息日期异常",
  MARKET_EVENTS_NBS_SCHEMA_CHANGED: "国家统计局发布日程格式变化",
  MARKET_EVENTS_NBS_SCHEDULE_NOT_FOUND: "国家统计局年度发布日程尚未找到",
  MARKET_EVENTS_NBS_DATE_INVALID: "国家统计局发布日期异常",
  MARKET_EVENTS_NBS_TIME_INVALID: "国家统计局发布时间异常",
  MARKET_EVENTS_HK_CSD_SCHEMA_CHANGED: "香港政府统计处发布日程格式变化",
  MARKET_EVENTS_HK_CSD_XLSX_INVALID: "香港政府统计处发布日程文件异常",
  MARKET_EVENTS_HK_CSD_DATE_INVALID: "香港政府统计处发布日期异常",
  MARKET_EVENTS_SFC_SCHEMA_CHANGED: "香港证监会监管日历格式变化",
  MARKET_EVENTS_SFC_JSON_INVALID: "香港证监会监管日历数据异常",
  MARKET_EVENTS_SFC_DATE_INVALID: "香港证监会监管日历日期异常",
  MARKET_EVENTS_BLS_JSON_INVALID: "BLS宏观数据格式异常",
  MARKET_EVENTS_BLS_RESPONSE_FAILED: "BLS宏观数据请求失败",
  MARKET_EVENTS_BLS_SCHEMA_CHANGED: "BLS宏观数据字段变化",
  MARKET_EVENTS_BLS_SERIES_MISSING: "BLS宏观数据序列缺失",
  MARKET_EVENTS_BLS_SERIES_INSUFFICIENT: "BLS宏观数据不足以计算最近变化",
  MARKET_EVENTS_BLS_PERIOD_MISMATCH: "BLS宏观数据参考期不一致",
  MARKET_EVENTS_CONSENSUS_JSON_INVALID: "市场一致预期格式异常",
  MARKET_EVENTS_CONSENSUS_SCHEMA_CHANGED: "市场一致预期字段变化",
  MARKET_EVENTS_CONSENSUS_DATE_INVALID: "市场一致预期发布时间异常",
  MARKET_EVENTS_CONSENSUS_VALUE_INVALID: "市场一致预期数值异常",
  MARKET_EVENTS_CONSENSUS_AMBIGUOUS: "市场一致预期存在冲突",
  STOCK_EVENTS_POOL_SCHEMA_CHANGED: "每日动态股池字段变化",
  STOCK_EVENTS_DIRECTORY_SCHEMA_CHANGED: "股票名称与代码目录字段变化",
  STOCK_EVENTS_DIRECTORY_BOUNDS_CHANGED: "股票目录规模超出安全边界",
  STOCK_EVENTS_DIRECTORY_PAGING_CHANGED: "股票目录分页发生变化",
  STOCK_EVENTS_DIRECTORY_INCOMPLETE: "股票目录读取不完整",
  STOCK_EVENTS_CALENDAR_SCHEMA_CHANGED: "公司事件日历字段变化",
  STOCK_EVENTS_CALENDAR_TRUNCATED: "公司事件日历超过读取上限",
  STOCK_EVENTS_ANNOUNCEMENT_SCHEMA_CHANGED: "公司公告索引字段变化",
  STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED: "公司公告索引超过读取上限",
  STOCK_EVENTS_PROJECTION_TRUNCATED: "个股事件展示超过保留上限",
};

const ISSUE_SCOPE_LABELS = {
  "sse-announcements": "上交所公告索引",
  "cninfo-sh-announcements": "巨潮沪市公告索引",
  "cninfo-sz-announcements": "巨潮深市公告索引",
  "a-share-documents": "A股公告原文",
  "hkex-main-reports": "港交所主板回购日报",
  "hkex-gem-reports": "港交所 GEM 回购日报",
  "connect-sh": "沪港通港股名单",
  "connect-sz": "深港通港股名单",
  "a-share-buyback-reference": "A股回购结构化参考",
  "hk-market-reference": "A股与港股行情参考",
  "buyback-financial-reference": "A股与港股公开业绩参考",
  "bea-schedule": "美国经济分析局发布日程",
  "fomc-calendar": "美联储议息日历",
  "sfc-regulatory-calendar": "香港证监会监管日历",
  "bls-macro-data": "美国通胀与就业数据",
  "market-consensus": "本周市场一致预期",
  "daily-universe": "每日动态股池",
  "stock-directory": "股票名称与代码目录",
  "stock-calendar": "公司事件日历",
  "stock-announcements": "公司公告索引",
  "stock-events-projection": "个股事件结果集",
  "BTCUSDT:1d": "BTC Spot 日线",
  "BTCUSDT:4h": "BTC Spot 4小时",
  "BTCUSDT:spot-price": "BTC Spot 参考价",
  "BTCUSDT:smart-money": "BTC 聪明钱",
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
  SMART_MONEY_SCHEMA_CHANGED: "未文档化接口的字段集合与预期契约不一致，未生成新特征",
  SMART_MONEY_COLLECTION_FAILED: "聪明钱采集器发生未预期异常；该来源被隔离，月线、日线与 4 小时结构仍可独立更新。",
  BTC_INTELLIGENCE_HTTP_BACKOFF_ACTIVE: "Binance Spot 公开接口仍在限流退避窗口，本轮没有继续请求。",
  BTC_INTELLIGENCE_HTTP_THROTTLED: "Binance Spot 返回限流状态；采集器已经打开有界指数退避。",
  BTC_INTELLIGENCE_UPSTREAM_HTTP_ERROR: "Binance Spot 公开接口没有返回成功响应；可用缓存仅在截止时间仍当前时使用。",
  BTC_INTELLIGENCE_UPSTREAM_UNAVAILABLE: "连接 Binance Spot 公开接口失败；各来源独立隔离，聪明钱层仍可更新。",
  BTC_INTELLIGENCE_RESPONSE_INVALID: "Binance Spot 响应无法按冻结字段契约解析，没有生成替代指标。",
  BTC_INTELLIGENCE_SOURCE_STALE: "缓存或公开来源没有覆盖最新闭合周期，相关结构层保持不可用。",
  BTC_INTELLIGENCE_KLINES_NON_CONTIGUOUS: "闭合 K 线时间轴存在缺口，本轮不产生正式状态或事件。",
  BTC_INTELLIGENCE_KLINES_INSUFFICIENT: "历史长度不足以覆盖指标暖机与 1080 根因果枢轴窗口。",
  BTC_INTELLIGENCE_TICKER_FAILED: "实时现货参考价读取失败；若 4h 数据当前，页面明确降级为最近闭合 4h 收盘。",
  BTC_INTELLIGENCE_TICKER_SCHEMA_CHANGED: "现货参考价字段与冻结契约不符，没有猜测或替代字段。",
  BTC_INTELLIGENCE_SERIES_COLLECTION_FAILED: "现货 K 线采集发生未预期异常；对应周期保持不可用，不沿用陈旧状态。",
  BTC_INTELLIGENCE_KLINE_SCHEMA_CHANGED: "K 线记录不符合冻结的 Binance Spot 字段契约，本轮没有计算指标。",
  BTC_INTELLIGENCE_KLINES_DUPLICATE: "闭合 K 线时间戳重复，本轮没有猜测重复记录的优先级。",
  BTC_INTELLIGENCE_KLINES_INVALID: "K 线价格、成交量或时间关系未通过数值校验，本轮没有计算指标。",
  BTC_INTELLIGENCE_RESPONSE_TOO_LARGE: "公开接口正文超过有界响应上限，采集已停止。",
  BTC_INTELLIGENCE_PAGINATION_STALLED: "分页返回没有推进到更早的 K 线，采集已停止以避免无界循环。",
  BTC_STRUCTURE_EVENT_MISSED_DURING_DOWNTIME: "事件在服务恢复时已经超过六根结果窗口，无法证明结果发生前已冻结，因此不计入前向账本。",
  RADAR_BACKOFF_ACTIVE: "Binance 上游退避窗口尚未结束，本轮没有继续发送公开行情请求",
  RADAR_CANDIDATE_COLLECTION_FAILED: "单个候选详查发生隔离失败，其他候选仍可展示",
  RADAR_FUTURES_ROWS_STALE: "资金费率来源时间超过有效截止点，相关字段保持为空",
  RADAR_HTTP_THROTTLED_418: "Binance 返回 HTTP 418；采集器遵守 Retry-After 并停止本轮后续请求",
  RADAR_HTTP_THROTTLED_429: "Binance 返回 HTTP 429；采集器遵守 Retry-After 并停止本轮后续请求",
  RADAR_KLINES_EMPTY: "没有取得通过校验的已闭合 5 分钟 K 线，未生成评分",
  RADAR_KLINES_INSUFFICIENT: "连续闭合 5 分钟 K 线少于计算窗口，未生成评分",
  RADAR_KLINES_NON_CONTIGUOUS: "K 线窗口存在空档，未插值或跨空档计算",
  RADAR_KLINES_STALE: "最近闭合 K 线超过有效截止点，未沿用旧数据",
  RADAR_DAILY_COLLECTION_FAILED: "该合约的日线价格位置采集发生未分类失败，本轮不进入价格位置表。",
  RADAR_DAILY_COMPUTATION_INVALID: "日线价格或区间无法形成有限值，本轮不生成价格位置。",
  RADAR_DAILY_HISTORY_INSUFFICIENT: "连续闭合 UTC 日线不足 90 根，本轮不生成价格位置。",
  RADAR_DAILY_KLINES_EMPTY: "公开接口没有返回已闭合 UTC 日线，本轮不生成价格位置。",
  RADAR_DAILY_KLINES_NON_CONTIGUOUS: "日线序列存在断档，本轮不生成价格位置，也不跨空档计算。",
  RADAR_DAILY_KLINES_SCHEMA_INVALID: "公开日线响应不符合预期结构，本轮不生成价格位置。",
  RADAR_DAILY_KLINES_STALE: "最新闭合 UTC 日线未到达本轮截止日，本轮不生成价格位置。",
  RADAR_DAILY_KLINES_UNAVAILABLE: "该合约本轮没有可用于价格位置的完整日线序列。",
  RADAR_DAILY_RANGE_INVALID: "公开日线请求范围无法安全构造，本轮不生成价格位置。",
  RADAR_DAILY_SOURCE_ROWS_MALFORMED: "少量来源行未通过数值与时间校验；表内只使用通过校验的闭合日线。",
  RADAR_DAILY_SYMBOL_INVALID: "合约代码未通过本地格式校验，未向公开接口发送请求。",
  RADAR_OI_EMPTY: "同名 USDⓈ-M 合约没有返回 OI 历史",
  RADAR_OI_INSUFFICIENT: "OI 历史不足以计算 15 分钟变化率",
  RADAR_OI_STALE: "最新 OI 时间超过有效截止点，变化率保持为空",
  RADAR_SOURCE_ROWS_MALFORMED: "公开来源中部分记录未通过字段或数值校验，已隔离",
  RADAR_TICKER_ROWS_STALE: "部分滚动行情超过有效截止点，已从本轮候选中排除",
  BUYBACK_BACKOFF_ACTIVE: "此前读取失败或受限，系统将在限定时间后重试；相关记录暂不展示。",
  BUYBACK_DOCUMENT_RUN_LIMIT_REACHED: "尚未取得原文的公告会在后续检查中继续获取；相关记录暂不展示。",
  BUYBACK_DOCUMENTS_INCOMPLETE: "部分公告尚未取得可读取的原文；相关记录暂不展示。",
  BUYBACK_PDF_TEXT_EMPTY: "公告文件已取得，但无法从中读取文字；相关记录暂不展示。",
  BUYBACK_HKEX_CURRENCY_INCONSISTENT: "该行金额或币种没有通过一致性校验，相关字段保持为空",
  BUYBACK_HKEX_REPORTS_INCOMPLETE: "部分日报没有通过文件或表头契约，其他日期仍照常更新",
  BUYBACK_A_REFERENCE_JSON_INVALID: "本轮不计算依赖A股结构化参考值的指标，公告事实仍照常展示。",
  BUYBACK_A_REFERENCE_SCHEMA_CHANGED: "参考字段与约定不一致，本轮不计算相关衍生指标。",
  BUYBACK_A_REFERENCE_CONTENT_TYPE_INVALID: "参考来源没有返回预期数据格式，本轮不计算相关衍生指标。",
  BUYBACK_HK_REFERENCE_JSON_INVALID: "本轮不使用港股当前参考价，港交所回购事实仍照常展示。",
  BUYBACK_HK_REFERENCE_SCHEMA_CHANGED: "行情参考字段与约定不一致，本轮不计算现价相关指标。",
  BUYBACK_HK_REFERENCE_EMPTY: "目标证券没有可用行情参考，本轮不计算现价相关指标。",
  BUYBACK_HK_REFERENCE_CONTENT_TYPE_INVALID: "行情参考没有返回预期数据格式，本轮不计算现价相关指标。",
  BUYBACK_MARKET_REFERENCE_JSON_INVALID: "行情与业绩参考无法解析，本轮不展示相关字段。",
  BUYBACK_MARKET_REFERENCE_TEXT_INVALID: "备用行情参考无法解析，本轮不展示相关字段。",
  BUYBACK_MARKET_REFERENCE_SCHEMA_CHANGED: "行情与业绩参考字段与约定不一致，本轮不展示相关字段。",
  BUYBACK_MARKET_REFERENCE_EMPTY: "目标证券没有可用行情与业绩参考，本轮不展示相关字段。",
  BUYBACK_MARKET_REFERENCE_CONTENT_TYPE_INVALID: "行情与业绩参考没有返回预期数据格式，本轮不展示相关字段。",
  BUYBACK_MARKET_REFERENCE_COUNT_INVALID: "目标证券数量超过单轮上限，本轮不展示行情与业绩字段。",
  BUYBACK_MARKET_REFERENCE_INCOMPLETE: "缺少参考值的证券在对应字段显示为空，其他证券不受影响。",
  BUYBACK_MARKET_PERFORMANCE_UNAVAILABLE: "现价、涨跌幅和市值参考可用；ROE、营收同比和净利同比暂时留空。",
  BUYBACK_FINANCIAL_REFERENCE_JSON_INVALID: "公开业绩参考无法解析，对应业绩字段留空。",
  BUYBACK_FINANCIAL_REFERENCE_SCHEMA_CHANGED: "公开业绩参考字段与约定不一致，对应业绩字段留空。",
  BUYBACK_FINANCIAL_REFERENCE_TRUNCATED: "公开业绩参考超过单次读取边界，对应批次的业绩字段留空。",
  BUYBACK_FINANCIAL_REFERENCE_EMPTY: "目标证券没有可用的公开业绩参考，对应字段留空。",
  BUYBACK_FINANCIAL_REFERENCE_COUNT_INVALID: "目标证券数量超过单轮上限，本轮不读取业绩参考。",
  BUYBACK_FINANCIAL_REFERENCE_CONTENT_TYPE_INVALID: "公开业绩参考没有返回预期数据格式，对应字段留空。",
  BUYBACK_FINANCIAL_REFERENCE_INCOMPLETE: "缺少业绩参考的证券在对应字段显示为空，其他证券不受影响。",
  MARKET_EVENTS_BEA_JSON_INVALID: "BEA机器可读日程无法解析，本轮不展示依赖该来源的GDP、PCE和贸易事件。",
  MARKET_EVENTS_BEA_SCHEMA_CHANGED: "BEA日程字段与既定契约不一致，本轮隔离该来源。",
  MARKET_EVENTS_BEA_RELEASE_DATE_INVALID: "BEA日程包含无效时间，本轮隔离该来源。",
  MARKET_EVENTS_NYFED_SCHEMA_CHANGED: "该月份日历结构与既定契约不一致，本轮不展示其中事件。",
  MARKET_EVENTS_NYFED_MONTH_MISMATCH: "页面返回的月份与请求月份不一致，本轮不展示其中事件。",
  MARKET_EVENTS_NYFED_EVENT_TIME_MISSING: "目标事件没有可校验的发布时间，本轮不展示该月份事件。",
  MARKET_EVENTS_FOMC_SCHEMA_CHANGED: "美联储会议日历结构与既定契约不一致，本轮不展示议息事件。",
  MARKET_EVENTS_FOMC_DATE_INVALID: "美联储会议日期无法解析，本轮不展示议息事件。",
  MARKET_EVENTS_NBS_SCHEMA_CHANGED: "国家统计局年度日程与既定表格契约不一致，本轮隔离该来源。",
  MARKET_EVENTS_NBS_SCHEDULE_NOT_FOUND: "尚未在有界的官方通知公告页中发现对应年度日程；系统会按计划重试。",
  MARKET_EVENTS_NBS_DATE_INVALID: "国家统计局日程包含无效日期，本轮隔离对应年度。",
  MARKET_EVENTS_NBS_TIME_INVALID: "国家统计局日程包含无效时间，本轮隔离对应年度。",
  MARKET_EVENTS_HK_CSD_SCHEMA_CHANGED: "香港政府统计处日程字段与既定契约不一致，本轮隔离对应年度。",
  MARKET_EVENTS_HK_CSD_XLSX_INVALID: "香港政府统计处日程文件无法安全解析，本轮隔离对应年度。",
  MARKET_EVENTS_HK_CSD_DATE_INVALID: "香港政府统计处日程包含无效日期，本轮隔离对应年度。",
  MARKET_EVENTS_SFC_SCHEMA_CHANGED: "香港证监会监管日历字段与既定契约不一致，本轮隔离该来源。",
  MARKET_EVENTS_SFC_JSON_INVALID: "香港证监会监管日历内嵌数据无法解析，本轮隔离该来源。",
  MARKET_EVENTS_SFC_DATE_INVALID: "香港证监会监管日历包含无效日期，本轮隔离该来源。",
  MARKET_EVENTS_BLS_JSON_INVALID: "BLS公共API响应无法解析，最近通胀与就业结果暂不展示。",
  MARKET_EVENTS_BLS_RESPONSE_FAILED: "BLS公共API没有返回成功状态，事件日历不受影响。",
  MARKET_EVENTS_BLS_SCHEMA_CHANGED: "BLS公共API字段与既定契约不一致，最近宏观结果暂不展示。",
  MARKET_EVENTS_BLS_SERIES_MISSING: "计算所需的官方时间序列缺失，最近宏观结果暂不展示。",
  MARKET_EVENTS_BLS_SERIES_INSUFFICIENT: "官方时间序列不足以计算同比或月度变化，相关结果暂不展示。",
  MARKET_EVENTS_BLS_PERIOD_MISMATCH: "就业或通胀序列的最近参考期不一致，未拼接不同月份的数据。",
  MARKET_EVENTS_CONSENSUS_JSON_INVALID: "本周市场一致预期响应无法解析；未使用替代预期。",
  MARKET_EVENTS_CONSENSUS_SCHEMA_CHANGED: "市场一致预期字段与既定契约不一致；未使用替代预期。",
  MARKET_EVENTS_CONSENSUS_DATE_INVALID: "一致预期的事件时间无效；没有与官方日历强行匹配。",
  MARKET_EVENTS_CONSENSUS_VALUE_INVALID: "一致预期数值无法可靠解析；相关指标不计算预期差。",
  MARKET_EVENTS_CONSENSUS_AMBIGUOUS: "同一事件同一指标出现冲突预期；相关指标不计算方向。",
  STOCK_EVENTS_POOL_SCHEMA_CHANGED: "公开热点股池字段与既定契约不一致；对应子来源已隔离。",
  STOCK_EVENTS_DIRECTORY_SCHEMA_CHANGED: "股票名称与代码目录字段与既定契约不一致；本轮保留安全的本地缓存。",
  STOCK_EVENTS_DIRECTORY_BOUNDS_CHANGED: "股票目录页数或记录数超出既定安全边界；没有继续无界读取。",
  STOCK_EVENTS_DIRECTORY_PAGING_CHANGED: "股票目录分页总数在同一轮读取中变化；本轮目录没有覆盖旧缓存。",
  STOCK_EVENTS_DIRECTORY_INCOMPLETE: "股票目录没有完整读取到全部分页；本轮目录没有覆盖旧缓存。",
  STOCK_EVENTS_CALENDAR_SCHEMA_CHANGED: "公司事件日历字段与既定契约不一致；受影响股票批次未生成日历事项。",
  STOCK_EVENTS_CALENDAR_TRUNCATED: "公司事件日历超过有界分页上限；日志列出受影响股票批次与读取边界。",
  STOCK_EVENTS_ANNOUNCEMENT_SCHEMA_CHANGED: "公司公告索引字段与既定契约不一致；受影响股票批次的公告没有继续解析。",
  STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED: "公司公告总量超过该股票批次的有界分页上限；已读取记录仍保留，超出部分未展示。",
  STOCK_EVENTS_PROJECTION_TRUNCATED: "个股事件总量超过本地结果上限；手动关注股票事件优先保留。",
};

function issueReasonLabel(reasonCode) {
  if (ISSUE_REASON_LABELS[reasonCode]) return ISSUE_REASON_LABELS[reasonCode];
  if (String(reasonCode).startsWith("BUYBACK_NETWORK_")) return "公开来源连接中断";
  if (String(reasonCode).startsWith("MARKET_EVENTS_BEA_")) return "BEA发布日程暂时无法读取";
  if (String(reasonCode).startsWith("MARKET_EVENTS_NYFED_")) return "纽约联储事件日历暂时无法读取";
  if (String(reasonCode).startsWith("MARKET_EVENTS_FOMC_")) return "美联储议息日历暂时无法读取";
  if (String(reasonCode).startsWith("MARKET_EVENTS_NBS_")) return "国家统计局发布日程暂时无法读取";
  if (String(reasonCode).startsWith("MARKET_EVENTS_HK_CSD_")) return "香港政府统计处发布日程暂时无法读取";
  if (String(reasonCode).startsWith("MARKET_EVENTS_SFC_")) return "香港证监会监管日历暂时无法读取";
  if (String(reasonCode).startsWith("MARKET_EVENTS_BLS_")) return "BLS宏观数据暂时无法读取";
  if (String(reasonCode).startsWith("MARKET_EVENTS_CONSENSUS_")) return "市场一致预期暂时无法读取";
  if (String(reasonCode).startsWith("STOCK_EVENTS_")) return "个股事件公开来源暂时无法读取";
  return "采集过程出现未识别问题";
}

function issueReasonDetail(reasonCode) {
  if (ISSUE_REASON_DETAILS[reasonCode]) return ISSUE_REASON_DETAILS[reasonCode];
  if (String(reasonCode).startsWith("BUYBACK_NETWORK_")) {
    return "读取公开来源时连接中断；系统会按计划重试，受影响字段保持为空。";
  }
  if (String(reasonCode).startsWith("MARKET_EVENTS_")) {
    return "读取对应官方来源时失败；系统会按计划重试，其他来源的事件仍照常展示。";
  }
  if (String(reasonCode).startsWith("STOCK_EVENTS_")) {
    return "读取对应个股事件公开来源时失败；受影响批次已隔离，其他来源仍照常更新。";
  }
  return "该问题尚无更具体的页面说明；系统会保留记录并继续按计划采集。";
}

const ISSUE_STATE_LABELS = {
  ACTIVE: "当前影响",
  RECOVERED: "已恢复",
  HISTORICAL: "历史记录",
};

const ISSUE_CONTEXT_LABELS = {
  selection_origin: "股票来源",
  stock_codes: "股票批次",
  stock_count: "股票数量",
  window_start: "查询起始",
  window_end: "查询截止",
  page_size: "每页上限",
  page_limit: "分页上限",
  pages_read: "已读页数",
  failed_page: "失败页码",
  records_read: "已读记录",
  upstream_total_hits: "上游总量",
  events_before_limit: "截断前事件",
  event_limit: "事件上限",
  manual_event_count: "手动关注事件",
  automatic_event_count: "每日入选事件",
  exception_type: "异常类型",
  origin_module: "代码模块",
  origin_function: "代码函数",
  origin_line: "代码行号",
  boundary_module: "项目边界模块",
  boundary_function: "项目边界函数",
  boundary_line: "项目边界行号",
};

const ISSUE_CONTEXT_ORDER = Object.keys(ISSUE_CONTEXT_LABELS);

function issueContextValue(key, value) {
  if (value === null || value === undefined || value === "") return "未知";
  if (key === "selection_origin") {
    if (value === "MANUAL") return "手动关注";
    if (value === "AUTO") return "每日入选";
  }
  if (key === "stock_codes") return String(value).replaceAll(",", "、");
  return String(value);
}

function issueContextEntries(context) {
  if (!context || typeof context !== "object" || Array.isArray(context)) return [];
  const keys = Object.keys(context).sort((left, right) => {
    const leftIndex = ISSUE_CONTEXT_ORDER.indexOf(left);
    const rightIndex = ISSUE_CONTEXT_ORDER.indexOf(right);
    return (leftIndex < 0 ? ISSUE_CONTEXT_ORDER.length : leftIndex)
      - (rightIndex < 0 ? ISSUE_CONTEXT_ORDER.length : rightIndex)
      || left.localeCompare(right);
  });
  return keys.map((key) => ({
    key,
    label: ISSUE_CONTEXT_LABELS[key] || key,
    value: issueContextValue(key, context[key]),
  }));
}

function issueReasonDetailForRecord(issue) {
  const detail = issueReasonDetail(issue.reason_code);
  if (
    issue.reason_code === "STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED"
    && issueContextEntries(issue.context).length === 0
  ) {
    return `${detail} 这条历史记录产生于批次定位字段上线前，原分页统计未被保存。`;
  }
  return detail;
}

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

function formatCadenceSeconds(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds <= 0) return "未知周期";
  if (seconds % 3600 === 0) return `${seconds / 3600} 小时`;
  if (seconds % 60 === 0) return `${seconds / 60} 分钟`;
  return `${seconds} 秒`;
}

function formatDate(value) {
  if (!value) return "未知";
  const text = String(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "未知";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(parsed);
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
  if (monitorId === RADAR_MONITOR_ID && state.projectionKind === "altcoin_radar") {
    url.searchParams.set("view", RADAR_TAB_LOCATION_VALUES[state.radarTab]);
  } else {
    url.searchParams.delete("view");
  }
  const nextLocation = `${url.pathname}${url.search}${url.hash}`;
  if (`${window.location.pathname}${window.location.search}${window.location.hash}` !== nextLocation) {
    window.history.replaceState(window.history.state, "", nextLocation);
  }
}

function queryUrl() {
  const params = new URLSearchParams();
  if (state.monitorId) params.set("monitor_id", state.monitorId);
  if (state.monitorId === RADAR_MONITOR_ID) {
    params.set(
      "view",
      RADAR_TAB_LOCATION_VALUES[state.radarTab] || RADAR_TAB_LOCATION_VALUES.TABLE,
    );
  }
  params.set("hours", String(state.hours));
  if (state.seriesKey) params.set("series_key", state.seriesKey);
  if (state.buybackStockQuery.trim()) {
    params.set("stock_query", state.buybackStockQuery.trim());
  }
  if (state.eventQuery.trim()) {
    params.set("event_query", state.eventQuery.trim());
  }
  if (state.projectionKind === "stock_events" && Array.isArray(state.stockSelectedCodes)) {
    state.stockSelectedCodes.forEach((code) => params.append("stock_code", code));
  }
  Object.entries(state.filters).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => params.append(key, item));
    } else {
      params.set(key, value);
    }
  });
  return `/api/view?${params.toString()}`;
}

async function maintainForegroundObservation(monitor) {
  const cadence = monitor?.collection_cadence;
  if (
    document.visibilityState !== "visible"
    || !monitor?.enabled
    || !cadence?.adaptive
  ) return false;
  try {
    const response = await fetch(
      `/api/monitors/${encodeURIComponent(monitor.monitor_id)}/observe`,
      { method: "POST", headers: { Accept: "application/json" } },
    );
    if (!response.ok) return false;
    const payload = await response.json();
    return Boolean(payload.refresh_requested);
  } catch (_error) {
    return false;
  }
}

function stopForegroundObservation() {
  clearInterval(state.observationTimer);
  state.observationTimer = null;
  state.observationMonitorId = null;
}

function requestNearTermViewRefresh() {
  if (document.visibilityState !== "visible") return;
  clearTimeout(state.refreshTimer);
  state.refreshTimer = setTimeout(() => loadView(), 3000);
}

function syncForegroundObservation(monitor) {
  if (
    document.visibilityState !== "visible"
    || !monitor?.enabled
    || !monitor?.collection_cadence?.adaptive
  ) {
    stopForegroundObservation();
    return;
  }
  const monitorId = monitor.monitor_id;
  if (
    state.observationTimer !== null
    && state.observationMonitorId === monitorId
  ) return;
  stopForegroundObservation();
  state.observationMonitorId = monitorId;
  state.observationTimer = setInterval(async () => {
    const current = state.viewPayload?.monitor;
    if (state.monitorId !== monitorId || current?.monitor_id !== monitorId) {
      stopForegroundObservation();
      return;
    }
    if (await maintainForegroundObservation(current)) requestNearTermViewRefresh();
  }, 15000);
}

async function loadView({ preserveSeries = true } = {}) {
  let nextRefreshMilliseconds = 15000;
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
      state.buybackStockQuery = "";
      state.eventQuery = "";
      state.radarTab = "TABLE";
      state.radarPriceState = "*";
      state.tableSort = null;
      state.tablePage = 1;
      syncMonitorLocation(null);
      response = await fetch(queryUrl(), {
        signal: state.request.signal,
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
    }
    if (
      response.status === 422
      && state.projectionKind === "stock_events"
      && Array.isArray(state.stockSelectedCodes)
    ) {
      state.stockSelectedCodes = null;
      response = await fetch(queryUrl(), {
        signal: state.request.signal,
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
    }
    if (!response.ok) throw new Error(`HTTP_${response.status}`);
    const payload = await response.json();
    const requestedRefreshSeconds = Number(payload.refresh_after_seconds);
    if (Number.isFinite(requestedRefreshSeconds)) {
      nextRefreshMilliseconds = clamp(requestedRefreshSeconds, 15, 7 * 86400) * 1000;
    }
    state.monitorId = payload.monitor.monitor_id;
    state.projectionKind = payload.monitor.projection_kind || "time_series";
    if (state.projectionKind === "buyback") {
      if (document.activeElement !== ui.buybackStockSearch) {
        state.buybackStockQuery = String(payload.buyback?.stock_query || "");
      }
    } else {
      state.buybackStockQuery = "";
    }
    if (["market_events", "stock_events"].includes(state.projectionKind)) {
      if (document.activeElement !== ui.eventSearch) {
        state.eventQuery = String(
          (state.projectionKind === "stock_events" ? payload.stock_events : payload.market_events)?.event_query || "",
        );
      }
    } else {
      state.eventQuery = "";
    }
    if (state.projectionKind === "stock_events") {
      const available = new Set((payload.stock_events?.securities || []).map((item) => String(item.code)));
      if (Array.isArray(state.stockSelectedCodes)) {
        state.stockSelectedCodes = state.stockSelectedCodes.filter((code) => available.has(code));
      }
    } else {
      state.stockSelectedCodes = null;
    }
    if (
      payload.monitor.projection_kind !== "altcoin_radar"
      || ["TABLE", "HISTORY"].includes(state.radarTab)
    ) {
      state.seriesKey = payload.selected_series_key;
    }
    state.filters = payload.monitor.selected_filters;
    state.latestRunId = payload.monitor.latest_run?.run_id ?? null;
    state.viewPayload = payload;
    syncMonitorLocation(state.monitorId);
    render(payload);
    const observationRequested = state.observationMonitorId === payload.monitor.monitor_id
      ? false
      : await maintainForegroundObservation(payload.monitor);
    syncForegroundObservation(payload.monitor);
    if (observationRequested) nextRefreshMilliseconds = 3000;
    if (state.pendingManualRefresh?.monitorId === state.monitorId) {
      const manualRunStarted = Number(payload.monitor.latest_run?.run_id ?? 0)
        > state.pendingManualRefresh.runAfter;
      nextRefreshMilliseconds = manualRunStarted ? 15000 : 3000;
    }
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
    state.refreshTimer = document.visibilityState === "visible"
      ? setTimeout(() => loadView(), nextRefreshMilliseconds)
      : null;
  }
}

function render(payload) {
  const isBuyback = payload.monitor.projection_kind === "buyback";
  const isMarketEvents = payload.monitor.projection_kind === "market_events";
  const isStockEvents = payload.monitor.projection_kind === "stock_events";
  const isAltcoinRadar = payload.monitor.projection_kind === "altcoin_radar";
  const isBtcIntelligence = payload.monitor.projection_kind === "btc_intelligence";
  ui.workspace.dataset.projectionKind = isBuyback
    ? "buyback"
    : isMarketEvents
      ? "market-events"
      : isStockEvents
        ? "stock-events"
        : isAltcoinRadar
          ? "altcoin-radar"
          : isBtcIntelligence
            ? "btc-intelligence"
            : "time-series";
  renderGlobal(payload);
  renderMonitorList(payload.monitors);
  renderContext(payload.monitor);
  renderControl(payload.monitor);
  renderConfiguration(payload.monitor.configuration, payload.monitor.latest_run);
  if (isStockEvents) ui.configurationRegion.hidden = true;
  renderFilters(
    payload.monitor.filters,
    payload.time_windows,
    payload.monitor.projection_kind,
  );
  renderBuybackOverview(payload.buyback);
  renderBuybackSources(payload.buyback);
  renderMarketEvents(payload.market_events);
  renderStockEvents(payload.stock_events);
  renderBtcIntelligence(payload.btc_intelligence);
  ui.historyRegion.hidden = isBuyback || isMarketEvents || isStockEvents || isAltcoinRadar || isBtcIntelligence;
  ui.historyWindowField.hidden = isBuyback || isMarketEvents || isStockEvents || isAltcoinRadar || isBtcIntelligence;
  ui.dataCutoff.hidden = isBuyback || isMarketEvents || isStockEvents || isBtcIntelligence;
  ui.quoteScroll.classList.toggle("buyback-table-scroll", isBuyback);
  ui.quoteScroll.classList.toggle("market-event-table-scroll", isMarketEvents);
  ui.quoteScroll.classList.toggle("radar-table-scroll", isAltcoinRadar);
  ui.quoteScroll.setAttribute(
    "aria-label",
    isBuyback
      ? "回购情报清单，可横向滚动"
      : isMarketEvents
        ? "关键事件日历"
        : isAltcoinRadar
          ? "USDⓈ-M 永续合约异动候选，可横向滚动"
          : "最新监控数据，可横向滚动",
  );
  renderRadarPriceFilter(payload.altcoin_price_position);
  if (isStockEvents) {
    ui.tableRegion.hidden = true;
    renderRunSummary([]);
  } else if (isAltcoinRadar) {
    renderRadarTableView(payload);
  } else {
    ui.quoteTableTitle.textContent = payload.buyback?.list_title
      || payload.market_events?.list_title
      || payload.monitor.table_title;
    renderRunSummary(payload.run_summary);
    renderTable(
      payload.monitor.columns,
      payload.rows,
      payload.selected_series_key,
      payload.current_issues,
      payload.monitor.selected_filters,
      payload.monitor.data_status,
    );
  }
  renderHistory(
    payload.monitor.chart_title,
    payload.rows,
    payload.selected_series_key,
    payload.history,
    payload.collection_gaps,
  );
  renderEvaluation(payload.evaluation);
  applyRadarTabState();
  renderIssues(payload.issues, payload.current_issues, payload.monitor);
  updateBackToTopVisibility();
  requestAnimationFrame(updateQuoteHorizontalScrollbar);
}

function btcNumber(value, maximumFractionDigits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "—";
  return new Intl.NumberFormat("zh-CN", {
    minimumFractionDigits: 0,
    maximumFractionDigits,
  }).format(numeric);
}

function btcSigned(value, suffix = "") {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "—";
  const sign = numeric > 0 ? "+" : "";
  return `${sign}${btcNumber(numeric, 2)}${suffix}`;
}

function btcMetric(label, value, detail = "") {
  const group = document.createElement("div");
  group.append(createElement("dt", "", label));
  group.append(createElement("dd", "", value));
  if (detail) group.append(createElement("small", "", detail));
  return group;
}

function btcTableCell(row, text, className = "") {
  row.append(createElement("td", className, text));
}

function renderBtcIntelligence(payload) {
  const active = state.projectionKind === "btc_intelligence";
  ui.btcIntelligence.hidden = !active;
  if (!active) return;

  const monthly = payload?.monthly || null;
  const daily = payload?.daily || null;
  const structure = payload?.structure || null;
  const smartMoney = payload?.smart_money || null;
  const monthlyLedger = payload?.monthly_ledger || null;
  const ledger = payload?.ledger || null;
  const regime = payload?.unified_regime || null;
  const clocks = payload?.source_clocks || {};

  ui.btcCurrentPrice.textContent = payload?.current_price
    ? `$${btcNumber(payload.current_price, 2)}`
    : "—";
  const priceStateLabels = {
    LIVE_SPOT_REFERENCE: "Binance Spot 即时参考 · 只用于距离显示",
    CLOSED_4H_REFERENCE: "即时价不可用 · 使用最近闭合 4h 收盘",
    UNAVAILABLE: "现货参考价不可用",
  };
  ui.btcPriceState.textContent = priceStateLabels[payload?.current_price_state]
    || "等待现货参考价";
  ui.btcRegime.textContent = regime?.label || "数据准备中";
  ui.btcRegime.dataset.regime = regime?.code || "UNKNOWN";
  ui.btcRegimeDetail.textContent = monthly && daily
    ? `${monthly.official_target_label} · ${daily.state_label}`
    : "月频与日频状态尚未同时可用";
  ui.btcInterpretation.textContent = payload?.interpretation_limit
    || "状态与研究目标不是买卖、仓位、杠杆、止损、止盈或跟单指令。";

  ui.btcClockStrip.replaceChildren();
  const sourceStateLabels = {
    LIVE_SPOT_REFERENCE: "现货即时参考",
    CLOSED_4H_REFERENCE: "最近 4 小时收盘",
    FETCHED: "本轮已从公开源更新",
    CACHE_CURRENT: "缓存已更新至截止点",
    CACHE_CURRENT_AFTER_ERROR: "缓存已到截止点，本轮上游读取失败",
    STALE: "来源落后于应有截止点",
    FAILED: "来源当前不可用",
    UNAVAILABLE: "当前不可用",
    OBSERVED: "本轮已生成",
  };
  [
    ["现货参考", payload?.current_price_at, payload?.current_price_state || "UNAVAILABLE"],
    ["4h 闭合", clocks.four_hour_cutoff_at, clocks.four_hour_state || "UNAVAILABLE"],
    ["日线闭合", clocks.daily_cutoff_at, clocks.daily_state || "UNAVAILABLE"],
    ["快照生成", payload?.observed_at, "OBSERVED"],
  ].forEach(([label, value, sourceState]) => {
    const item = createElement("div", "btc-clock-item");
    item.append(createElement("span", "", label));
    item.append(createElement("strong", "", value ? formatTime(value) : "不可用"));
    item.append(createElement("small", "", sourceStateLabels[sourceState] || "状态未识别"));
    ui.btcClockStrip.append(item);
  });

  ui.btcMonthlyMetrics.replaceChildren();
  if (monthly) {
    ui.btcMonthlyTarget.textContent = monthly.official_target_label;
    ui.btcMonthlyTarget.dataset.target = String(monthly.official_target);
    ui.btcMonthlyFormed.textContent = `${monthly.formed_month} 完整月确认 · 本月内正式目标不变`;
    ui.btcMonthlyMetrics.append(
      btcMetric("月末确认边界", `$${btcNumber(monthly.current_boundary, 2)}`, `${monthly.current_month} 整月固定`),
      btcMetric("现价距边界", btcSigned(monthly.distance_percent, "%"), `${btcSigned(monthly.distance_atr)} 日 ATR`),
      btcMetric("日 ATR20", `$${btcNumber(monthly.daily_atr20, 2)}`, "20 日真实波幅简单均值"),
      btcMetric("下次月末确认", formatTime(monthly.next_confirmation_at), "月首 00:01Z 仅为研究成交代理"),
      btcMetric(
        "前向月度记录",
        monthlyLedger ? new Intl.NumberFormat("zh-CN").format(Number(monthlyLedger.signal_count || 0)) : "尚未初始化",
        monthlyLedger ? `${monthlyLedger.execution_count || 0} 个执行代理已固定` : "只记录部署后及时冻结的月份",
      ),
      btcMetric(
        "完整持有周期",
        monthlyLedger ? new Intl.NumberFormat("zh-CN").format(Number(monthlyLedger.complete_long_cash_cycles || 0)) : "—",
        "至少新增两个完整周期后才提升证据等级",
      ),
    );
    ui.btcMonthlyNote.textContent = `${monthly.provisional_label}。这是月中预警，不是已经切换的正式目标。`;
    ui.btcMonthlyNote.dataset.side = monthly.provisional_side;
  } else {
    ui.btcMonthlyTarget.textContent = "闭合日线历史暂不可用";
    ui.btcMonthlyTarget.dataset.target = "UNKNOWN";
    ui.btcMonthlyFormed.textContent = "不会用陈旧值或替代市场填充";
    ui.btcMonthlyNote.textContent = "等待至少十个连续完整自然月。";
  }

  ui.btcDailyComponents.replaceChildren();
  if (daily) {
    ui.btcDailyAgreement.textContent = `${btcNumber(daily.agreement_percent, 0)}%`;
    ui.btcDailyState.textContent = daily.state_label;
    ui.btcDailyState.dataset.state = daily.state;
    (daily.components || []).forEach((component) => {
      const row = createElement("div", "btc-component-row");
      const identity = createElement("div", "btc-component-identity");
      identity.append(createElement("strong", "", `${component.window} 日`));
      const badge = createElement(
        "span",
        component.active ? "btc-active-badge is-active" : "btc-active-badge",
        component.active ? "激活" : "未激活",
      );
      identity.append(badge);
      row.append(identity);
      const boundary = createElement("div", "btc-component-boundary");
      boundary.append(createElement("span", "", component.active ? "棘轮支撑" : "突破阻力"));
      boundary.append(createElement("strong", "", `$${btcNumber(component.boundary, 2)}`));
      boundary.append(createElement("small", "", `${btcSigned(component.distance_percent, "%")} 至边界`));
      row.append(boundary);
      ui.btcDailyComponents.append(row);
    });
    const transition = daily.latest_transition;
    ui.btcDailyTransition.textContent = transition
      ? `最近变化：${transition.window} 日组件 ${transition.from === "ACTIVE" ? "激活" : "未激活"} → ${transition.to === "ACTIVE" ? "激活" : "未激活"} · ${formatTime(transition.at)} · 触发边界 $${btcNumber(transition.trigger_boundary, 2)}`
      : "当前可用历史内没有组件状态变化。";
  } else {
    ui.btcDailyAgreement.textContent = "—";
    ui.btcDailyState.textContent = "闭合日线暂不可用";
    ui.btcDailyTransition.textContent = "日频状态不会由盘中价格提前改变。";
  }

  ui.btcZoneBody.replaceChildren();
  const roleLabels = { SUPPORT: "支撑", RESISTANCE: "阻力", TESTING: "测试中" };
  const zones = structure?.zones || [];
  zones.forEach((zone) => {
    const row = document.createElement("tr");
    const role = roleLabels[zone.role] || zone.role;
    btcTableCell(row, `${role} · ${zone.lifecycle === "TESTING" ? "区间内" : "活跃"}`, `btc-zone-role is-${String(zone.role).toLowerCase()}`);
    btcTableCell(row, `$${btcNumber(zone.lower, 2)} – $${btcNumber(zone.upper, 2)}`, "numeric");
    btcTableCell(row, `${btcSigned(zone.distance_atr)} ATR`, "numeric");
    btcTableCell(row, btcNumber(zone.strength, 2), "numeric");
    btcTableCell(row, String(zone.anchor_count ?? "—"), "numeric");
    btcTableCell(row, `${zone.age_bars ?? "—"} 根`, "numeric");
    btcTableCell(row, formatTime(zone.formed_at));
    btcTableCell(row, formatTime(zone.version_effective_at || zone.effective_at));
    ui.btcZoneBody.append(row);
  });
  ui.btcZoneEmpty.hidden = zones.length > 0;
  ui.btcStructureEnvironment.textContent = structure
    ? `${structure.environment_label} · ADX ${btcNumber(structure.adx14, 1)}`
    : "闭合 4h 结构暂不可用";
  ui.btcStructureEnvironment.dataset.environment = structure?.environment || "UNKNOWN";
  ui.btcStructureCutoff.textContent = structure?.source_cutoff_at
    ? `截止 ${formatTime(structure.source_cutoff_at)} · ${structure.model_label}`
    : "仅闭合 K 线产生正式结构";

  ui.btcSmartBody.replaceChildren();
  const smartRows = smartMoney?.rows || [];
  smartRows.forEach((item) => {
    const row = document.createElement("tr");
    btcTableCell(row, item.time_range_label || item.time_range || "—");
    btcTableCell(row, item.dominant_flow || "—");
    btcTableCell(row, btcSigned(item.flow_imbalance_percent, "%"), "numeric");
    btcTableCell(row, btcSigned(item.normalized_flow_percent, "%"), "numeric");
    btcTableCell(row, btcSigned(item.whale_divergence_percent, "%"), "numeric");
    btcTableCell(row, btcSigned(item.last_funding_rate_percent, "%"), "numeric");
    btcTableCell(row, formatTime(item.latest_trade_at || item.observed_at));
    ui.btcSmartBody.append(row);
  });
  ui.btcSmartEmpty.hidden = smartRows.length > 0;

  ui.btcLedgerMetrics.replaceChildren();
  if (ledger) {
    ui.btcLedgerMetrics.append(
      btcMetric("前向事件", new Intl.NumberFormat("zh-CN").format(Number(ledger.total_events || 0))),
      btcMetric("待结算", new Intl.NumberFormat("zh-CN").format(Number(ledger.pending_events || 0))),
      btcMetric("完整结果", new Intl.NumberFormat("zh-CN").format(Number(ledger.completed_events || 0))),
      btcMetric("原始反应率", ledger.reaction_rate_percent == null ? "样本积累中" : `${btcNumber(ledger.reaction_rate_percent, 2)}%`, "未决保留在分母"),
    );
    const coverage = `${ledger.support_events || 0} 个支撑 / ${ledger.resistance_events || 0} 个阻力 · ${ledger.volatility_regimes?.length || 0} 种波动状态`;
    const costResult = ledger.average_net_return_30bps_percent == null
      ? "成本后结果等待完整样本"
      : `30bp 后平均 ${btcSigned(ledger.average_net_return_30bps_percent, "%")} · 50bp 后平均 ${btcSigned(ledger.average_net_return_50bps_percent, "%")}`;
    ui.btcLedgerStart.textContent = ledger.started_at
      ? `前向账本始于 ${formatTime(ledger.started_at)} · ${coverage} · ${costResult} · 保留 ${ledger.retention_days} 天 / 最多 ${new Intl.NumberFormat("zh-CN").format(ledger.maximum_events)} 个事件。`
      : "首次有效 4h 结构刷新后建立前向账本，不回填历史胜率。";
  } else {
    ui.btcLedgerMetrics.append(btcMetric("前向事件", "尚未初始化"));
    ui.btcLedgerStart.textContent = "首次有效 4h 结构刷新后建立前向账本，不回填历史胜率。";
  }

  ui.btcEventBody.replaceChildren();
  const eventStateLabels = {
    PENDING: "待满 6 根",
    REACTION: "反应",
    BREAK: "突破",
    UNRESOLVED: "六根内未决",
  };
  const events = ledger?.events || [];
  events.forEach((event) => {
    const signal = event.signal || {};
    const row = document.createElement("tr");
    btcTableCell(row, formatTime(event.event_at));
    btcTableCell(row, roleLabels[signal.kind] || signal.kind || "—");
    btcTableCell(row, `$${btcNumber(signal.zone_lower, 2)} – $${btcNumber(signal.zone_upper, 2)}`, "numeric");
    btcTableCell(row, `$${btcNumber(signal.touch_close, 2)}`, "numeric");
    btcTableCell(row, formatTime(signal.due_at));
    btcTableCell(row, eventStateLabels[event.state] || event.state, `btc-event-state is-${String(event.state).toLowerCase()}`);
    ui.btcEventBody.append(row);
  });
  ui.btcEventEmpty.hidden = events.length > 0;
}

const BUYBACK_SOURCE_SUMMARY_LABELS = {
  as_of: "名单日期",
  window_start: "窗口开始",
  window_end: "窗口结束",
  target_candidate_count: "相关公告",
  candidate_count: "去重后相关公告",
  report_count: "已读取日报",
  hkex_execution_row_count: "实际回购记录",
  cross_market_row_count: "已排除跨市场行",
  new_document_count: "本次获取原文",
  existing_document_count: "已有可用原文",
  fallback_document_count: "从备用来源获取",
  failed_document_count: "获取原文失败",
  empty_text_document_count: "原文无法读取文字",
  backlog_count: "尚未获取原文",
  run_document_limit: "每次最多获取原文",
  page_count: "查询页数",
  programme_count: "回购方案参考",
  quote_count: "行情记录",
  requested_count: "请求证券",
};

function renderBuybackOverview(buybackPayload) {
  ui.buybackOverview.replaceChildren();
  if (!buybackPayload) {
    ui.buybackOverviewRegion.hidden = true;
    return;
  }
  const metrics = [
    ["近24小时新增", buybackPayload.fresh_intelligence_count],
    ["回购情报", buybackPayload.intelligence_count],
    ["执行类事件", buybackPayload.execution_count],
    ["高吸引力", buybackPayload.high_attractiveness_count],
  ];
  metrics.forEach(([label, value]) => {
    const group = document.createElement("div");
    group.append(createElement("dt", "", label));
    group.append(createElement("dd", "", new Intl.NumberFormat("zh-CN").format(Number(value || 0))));
    ui.buybackOverview.append(group);
  });
  const timing = document.createElement("div");
  timing.className = "buyback-overview-time";
  timing.append(createElement("dt", "", "最近来源检查"));
  timing.append(createElement("dd", "", formatTime(buybackPayload.source_checked_at)));
  ui.buybackOverview.append(timing);
  ui.buybackOverviewRegion.hidden = false;
}

function renderBuybackSources(buybackPayload) {
  if (!buybackPayload) {
    ui.buybackSourceRegion.hidden = true;
    ui.buybackSourceGrid.replaceChildren();
    return;
  }
  const problemCount = Number(buybackPayload.source_problem_count || 0);
  const pendingCount = Number(buybackPayload.pending_count || 0);
  const problemSources = buybackPayload.source_states.filter(
    (source) => !["SUCCESS", "EMPTY"].includes(source.status),
  );
  const documentSource = buybackPayload.source_states.find(
    (source) => source.source_key === "a-share-documents",
  );
  const documentProblem = documentSource
    && !["SUCCESS", "EMPTY"].includes(documentSource.status);
  const backlogCount = Number(documentSource?.summary?.backlog_count || 0);
  const failedDocumentCount = Number(documentSource?.summary?.failed_document_count || 0);
  const unreadableDocumentCount = Number(documentSource?.summary?.empty_text_document_count || 0);
  const sourceMessages = [];
  if (backlogCount > 0) sourceMessages.push(`${backlogCount} 份 A 股公告原文尚未获取`);
  if (failedDocumentCount > 0) sourceMessages.push(`${failedDocumentCount} 份 A 股公告原文获取失败`);
  if (unreadableDocumentCount > 0) sourceMessages.push(`${unreadableDocumentCount} 份 A 股公告原文无法读取文字`);
  if (documentProblem && sourceMessages.length === 0) {
    sourceMessages.push("A 股公告原文未完整获取");
  }
  const documentedPendingCount = backlogCount + failedDocumentCount + unreadableDocumentCount;
  const otherPendingCount = Math.max(pendingCount - documentedPendingCount, 0);
  if (otherPendingCount > 0) {
    sourceMessages.push(`${otherPendingCount} 条记录存在其他信息缺失`);
  }
  const otherProblemSources = problemSources.filter(
    (source) => source.source_key !== "a-share-documents",
  );
  if (otherProblemSources.length === 1) {
    sourceMessages.push(`${otherProblemSources[0].source_label}读取异常`);
  } else if (otherProblemSources.length > 1) {
    sourceMessages.push(`${otherProblemSources.length} 个其他数据来源读取异常`);
  }
  const hasVisibleProblem = problemCount > 0 || pendingCount > 0 || sourceMessages.length > 0;
  ui.buybackSourceRegion.hidden = !hasVisibleProblem;
  if (!hasVisibleProblem) {
    ui.buybackSourceGrid.replaceChildren();
    return;
  }
  if (sourceMessages.length > 0) {
    const impact = pendingCount > 0
      ? `共 ${pendingCount} 条信息不完整的记录暂不展示。`
      : "页面不会展示受影响的数据。";
    ui.buybackSourceSummary.textContent = `${sourceMessages.join("；")}。${impact}`;
  } else if (pendingCount > 0) {
    ui.buybackSourceSummary.textContent = `${pendingCount} 条记录信息不完整，暂不展示。`;
  } else {
    ui.buybackSourceSummary.textContent = "查看数据来源";
  }
  ui.buybackSourceRegion.dataset.tone = problemCount > 0 || pendingCount > 0
    ? "WARNING"
    : "HEALTHY";
  ui.buybackSourceGrid.replaceChildren();
  if (!buybackPayload.source_states.length) {
    ui.buybackSourceGrid.append(createElement("p", "empty-state", "等待首轮来源检查。"));
    return;
  }
  buybackPayload.source_states.forEach((source) => {
    const card = createElement("article", "buyback-source-card");
    card.dataset.tone = source.tone;
    const head = createElement("div", "buyback-source-card-head");
    head.append(createElement("h3", "", source.source_label));
    const badge = createElement("span", "buyback-source-status", source.status_label);
    badge.dataset.tone = source.tone;
    head.append(badge);
    card.append(head);
    const timing = createElement("p", "buyback-source-timing");
    timing.textContent = `检查 ${formatTime(source.checked_at)} · 来源 ${formatTime(source.source_time)}`;
    card.append(timing);
    const facts = createElement("dl", "buyback-source-facts");
    if (source.record_count !== null && source.record_count !== undefined) {
      const group = document.createElement("div");
      group.append(createElement("dt", "", "记录"));
      group.append(createElement("dd", "", String(source.record_count)));
      facts.append(group);
    }
    Object.entries(source.summary || {}).slice(0, 5).forEach(([key, value]) => {
      const group = document.createElement("div");
      group.append(createElement("dt", "", BUYBACK_SOURCE_SUMMARY_LABELS[key] || key));
      group.append(createElement("dd", "", String(value)));
      facts.append(group);
    });
    card.append(facts);
    if (source.detail_code) {
      const detail = createElement(
        "p",
        "buyback-source-detail",
        issueReasonLabel(source.detail_code),
      );
      detail.title = issueReasonDetail(source.detail_code);
      card.append(detail);
    }
    ui.buybackSourceGrid.append(card);
  });
}

function marketEventScheduleText(event) {
  const label = String(event.schedule_label || "");
  if (!label) return "发布时间待公布";
  if (event.time_precision === "DATE") return label;
  return `${label}（北京时间）`;
}

function marketEventButton(event, className) {
  const button = createElement("button", className);
  button.type = "button";
  button.addEventListener("click", () => openMarketEventDetail(event));
  return button;
}

function renderEventCoverage(eventPayload) {
  const messages = eventPayload?.coverage_messages || [];
  ui.eventSourceRegion.hidden = messages.length === 0;
  ui.eventSourceDetails.replaceChildren();
  if (!messages.length) return;
  ui.eventSourceSummary.textContent = `${messages.join("；")}。`;
  messages.forEach((message) => {
    const item = createElement("p", "event-source-message", message);
    ui.eventSourceDetails.append(item);
  });
}

function renderEventAttention(eventPayload) {
  ui.eventAttentionCards.replaceChildren();
  if (!eventPayload) {
    ui.eventAttentionRegion.hidden = true;
    return;
  }
  ui.eventAttentionRegion.hidden = false;
  ui.eventAttentionSummary.textContent = [
    `未来24小时 ${Number(eventPayload.next_24h_count || 0)} 项`,
    `未来7天高影响 ${Number(eventPayload.next_7d_high_count || 0)} 项`,
    Number(eventPayload.recent_schedule_change_count || 0) > 0
      ? `近期时间调整 ${Number(eventPayload.recent_schedule_change_count)} 项`
      : null,
  ].filter(Boolean).join(" · ");
  ui.eventSourceCutoff.textContent = `最近检查 ${formatTime(eventPayload.source_checked_at)}`;
  const events = eventPayload.attention_events || [];
  events.forEach((event) => {
    const card = marketEventButton(event, "event-attention-card");
    card.dataset.priority = String(event.priority_rank || 4);
    const top = createElement("span", "event-attention-card-top");
    const priority = createElement("span", "event-priority-badge", event.priority_label || "日历关注");
    priority.dataset.priority = String(event.priority_rank || 4);
    const countdown = createElement("strong", "event-countdown", event.countdown_label || "");
    top.append(priority, countdown);
    card.append(
      top,
      createElement("strong", "event-attention-title", event.event_title || "关键事件"),
      createElement("span", "event-attention-time", marketEventScheduleText(event)),
      createElement("span", "event-attention-markets", event.markets_label || ""),
    );
    if (event.expectation_summary) {
      card.append(createElement(
        "span",
        "event-attention-result",
        `市场预期 ${event.expectation_summary}`,
      ));
    }
    card.title = `${event.priority_reason || ""} ${event.impact_reason || ""}`.trim();
    ui.eventAttentionCards.append(card);
  });
  ui.eventAttentionEmpty.hidden = events.length > 0;
  ui.eventAttentionEmpty.textContent = Number(eventPayload.event_count || 0) > 0
    ? "当前筛选下，未来7天没有进入优先准备窗口的事件。"
    : "当前筛选没有未来事件。";
}

function renderMacroIndicators(eventPayload) {
  ui.macroIndicatorCards.replaceChildren();
  const indicators = eventPayload?.indicators || [];
  ui.macroIndicatorRegion.hidden = indicators.length === 0;
  indicators.forEach((indicator) => {
    const sourceUrl = safeExternalUrl(indicator.source_url);
    const card = sourceUrl
      ? createElement("a", "macro-indicator-card")
      : createElement("article", "macro-indicator-card");
    if (sourceUrl) {
      card.href = sourceUrl;
      card.target = "_blank";
      card.rel = "noopener noreferrer";
      card.setAttribute("aria-label", `${indicator.indicator_label}，打开${indicator.source_label}，新标签页`);
    }
    const heading = createElement("span", "macro-indicator-heading");
    heading.append(
      createElement("strong", "", indicator.indicator_label || "宏观数据"),
      createElement("span", "", indicator.period_label || ""),
    );
    card.append(
      heading,
      createElement("strong", "macro-indicator-primary", indicator.primary_value || "—"),
      createElement("span", "macro-indicator-secondary", indicator.secondary_value || ""),
      createElement("span", "macro-indicator-method", indicator.method_label || ""),
    );
    ui.macroIndicatorCards.append(card);
  });
}

function calendarDayHeading(day) {
  if (day.day_offset === 0) return "今天";
  if (day.day_offset === 1) return "明天";
  const parsed = new Date(`${day.date}T12:00:00+08:00`);
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    weekday: "short",
  }).format(parsed);
}

function renderEventCalendar(eventPayload) {
  ui.eventCalendarGrid.replaceChildren();
  if (!eventPayload) {
    ui.eventCalendarRegion.hidden = true;
    return;
  }
  ui.eventCalendarRegion.hidden = false;
  (eventPayload.calendar_days || []).forEach((day) => {
    const cell = createElement("article", "event-calendar-day");
    if (day.day_offset === 0) cell.dataset.today = "true";
    const heading = createElement("div", "event-calendar-day-head");
    heading.append(
      createElement("strong", "", calendarDayHeading(day)),
      createElement("span", "", String(day.date || "").slice(5).replace("-", "/")),
    );
    cell.append(heading);
    (day.events || []).forEach((event) => {
      const item = marketEventButton(event, "event-calendar-item");
      item.dataset.importance = event.importance || "MEDIUM";
      item.append(
        createElement("strong", "", event.event_title || "关键事件"),
        createElement("span", "", event.time_precision === "EXACT"
          ? String(event.schedule_label || "").slice(11)
          : "时间待公布"),
      );
      item.title = `${event.countdown_label || ""} · ${event.markets_label || ""}`;
      cell.append(item);
    });
    if (Number(day.additional_count || 0) > 0) {
      cell.append(createElement("span", "event-calendar-more", `另有 ${day.additional_count} 项`));
    }
    if (!(day.events || []).length) {
      cell.append(createElement("span", "event-calendar-none", "—"));
    }
    ui.eventCalendarGrid.append(cell);
  });
}

function marketEventMetricSummary(items) {
  return (items || []).slice(0, 2).map((item) => (
    `${item.label || "数据"} ${item.display || "—"}`
  )).join(" · ");
}

function renderEventHistory(eventPayload) {
  ui.eventHistoryBody.replaceChildren();
  const events = eventPayload?.history_events || [];
  const pageCount = Math.max(1, Math.ceil(events.length / MARKET_EVENT_HISTORY_PAGE_SIZE));
  state.eventHistoryPage = Math.min(Math.max(1, state.eventHistoryPage), pageCount);
  const start = (state.eventHistoryPage - 1) * MARKET_EVENT_HISTORY_PAGE_SIZE;
  const pageRows = events.slice(start, start + MARKET_EVENT_HISTORY_PAGE_SIZE);
  pageRows.forEach((event) => {
    const tr = document.createElement("tr");
    tr.className = "event-history-row";
    tr.tabIndex = 0;
    const timeCell = document.createElement("td");
    timeCell.textContent = marketEventScheduleText(event);
    const titleCell = document.createElement("td");
    const title = createElement("div", "market-event-title-cell");
    title.append(
      createElement("strong", "", event.event_title || "关键事件"),
      createElement("span", "", `${event.category_label || ""} · ${event.release_state_label || ""}`),
    );
    titleCell.append(title);
    const expectationCell = document.createElement("td");
    expectationCell.textContent = event.expectation_summary || "—";
    if (!event.expectation_summary) expectationCell.dataset.missing = "true";
    const actualCell = document.createElement("td");
    actualCell.textContent = event.actual_summary || event.release_state_label || "—";
    if (!event.actual_summary) actualCell.className = "event-history-state";
    const surpriseCell = document.createElement("td");
    surpriseCell.textContent = event.surprise_summary || "—";
    if (!event.surprise_summary) surpriseCell.dataset.missing = "true";
    const directionCell = document.createElement("td");
    if (event.direction) {
      const badge = createElement("span", "event-direction-badge", event.direction.label || "中性");
      badge.dataset.tone = event.direction.tone || "NEUTRAL";
      badge.title = `方向分 ${event.direction.score} · ${event.direction.threshold || ""}`;
      directionCell.append(badge);
    } else {
      directionCell.textContent = "—";
      directionCell.dataset.missing = "true";
    }
    tr.append(timeCell, titleCell, expectationCell, actualCell, surpriseCell, directionCell);
    const open = () => openMarketEventDetail(event);
    tr.addEventListener("click", open);
    tr.addEventListener("keydown", (keyboardEvent) => {
      if (keyboardEvent.key === "Enter" || keyboardEvent.key === " ") {
        keyboardEvent.preventDefault();
        open();
      }
    });
    ui.eventHistoryBody.append(tr);
  });
  const started = eventPayload?.history_started_at
    ? formatTime(eventPayload.history_started_at)
    : "首次成功采集后";
  ui.eventHistorySummary.textContent = `仅保存 ${started} 起采集到的事件，不补录此前历史`;
  ui.eventHistoryEmpty.hidden = events.length > 0;
  ui.eventHistoryEmpty.textContent = `历史从 ${started} 开始记录，目前还没有已发生事件。`;
  ui.eventHistoryPagination.hidden = events.length <= MARKET_EVENT_HISTORY_PAGE_SIZE;
  ui.eventHistoryPageSummary.textContent = events.length
    ? `显示第 ${start + 1}–${Math.min(start + pageRows.length, events.length)} 项，共 ${events.length} 项`
    : "暂无历史事件";
  ui.eventHistoryPageStatus.textContent = `${state.eventHistoryPage} / ${pageCount} 页`;
  ui.eventHistoryPrevious.disabled = state.eventHistoryPage <= 1;
  ui.eventHistoryNext.disabled = state.eventHistoryPage >= pageCount;
}

function applyEventTabState() {
  const isMarketEvents = state.projectionKind === "market_events";
  ui.eventViewTabs.hidden = !isMarketEvents;
  if (!isMarketEvents) {
    ui.eventHistoryRegion.hidden = true;
    ui.tableRegion.hidden = false;
    ui.filtersRegion.hidden = false;
    return;
  }
  const history = state.eventTab === "HISTORY";
  ui.eventUpcomingTab.setAttribute("aria-selected", String(!history));
  ui.eventHistoryTab.setAttribute("aria-selected", String(history));
  ui.eventHistoryRegion.hidden = !history;
  ui.tableRegion.hidden = history;
  ui.filtersRegion.hidden = history;
  if (history) {
    ui.eventAttentionRegion.hidden = true;
    ui.macroIndicatorRegion.hidden = true;
    ui.eventCalendarRegion.hidden = true;
    renderEventHistory(state.marketEventPayload);
  } else {
    renderEventAttention(state.marketEventPayload);
    renderMacroIndicators(state.marketEventPayload);
    renderEventCalendar(state.marketEventPayload);
  }
  ui.eventHistoryCount.textContent = String(state.marketEventPayload?.history_event_count || 0);
  requestAnimationFrame(updateQuoteHorizontalScrollbar);
}

function renderMarketEvents(eventPayload) {
  state.marketEventPayload = eventPayload;
  renderEventCoverage(eventPayload);
  applyEventTabState();
}

function shanghaiDateKey(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(parsed);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function stockMonthShift(monthKey, offset) {
  const [year, month] = String(monthKey || "").split("-").map(Number);
  if (!year || !month) return null;
  const shifted = new Date(Date.UTC(year, month - 1 + offset, 1));
  return `${shifted.getUTCFullYear()}-${String(shifted.getUTCMonth() + 1).padStart(2, "0")}`;
}

function stockDateLabel(dateKey, options = {}) {
  const parsed = new Date(`${dateKey}T12:00:00+08:00`);
  if (Number.isNaN(parsed.getTime())) return "日期未知";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: options.month || "long",
    day: options.day || "numeric",
    weekday: options.weekday || "short",
    year: options.year,
  }).format(parsed);
}

function stockEventBadge(text, kind, value = "") {
  const badge = createElement("span", `stock-event-badge stock-event-badge-${kind}`, text);
  if (value) badge.dataset.value = value;
  return badge;
}

function stockEventTitle(event) {
  const name = event.stock_name || "名称待更新";
  return `${name} ${event.stock_code || ""}`.trim();
}

function stockEventDisplayTitle(event) {
  if (
    event.source_kind === "EVENT_CALENDAR"
    && event.summary
    && event.summary !== event.title
  ) {
    return `${event.title || "公司事件"} · ${event.summary}`;
  }
  return event.title || "公司事件";
}

function eventsOnStockDate(payload, dateKey) {
  return (payload?.events || []).filter((event) => event.event_date === dateKey);
}

function renderStockEventDetail(event) {
  ui.stockEventDetail.hidden = !event;
  if (!event) return;
  ui.stockEventDetailBadges.replaceChildren(
    stockEventBadge(event.category_label || "公司事件", "category", event.category || "DISCLOSURE"),
    stockEventBadge(event.importance === "HIGH" ? "重点" : "一般", "importance", event.importance || "MEDIUM"),
    stockEventBadge(event.state_label || "状态未知", "state", event.state || "UNKNOWN"),
  );
  ui.stockEventDetailTitle.textContent = event.title || "公司事件";
  ui.stockEventDetailStock.textContent = `${stockEventTitle(event)} · ${stockDateLabel(event.event_date)}`;
  ui.stockEventDetailSummary.textContent = event.summary || "来源未提供补充摘要。";
  const sourceUrl = safeExternalUrl(event.source_url);
  ui.stockEventDetailSource.hidden = !sourceUrl;
  if (sourceUrl) {
    ui.stockEventDetailSource.href = sourceUrl;
    ui.stockEventDetailSource.textContent = `查看${event.source_label || "公开来源"} ↗`;
    ui.stockEventDetailSource.setAttribute(
      "aria-label",
      `查看${event.source_label || "公开来源"}，新标签页`,
    );
  } else {
    ui.stockEventDetailSource.removeAttribute("href");
  }
}

function renderStockDayAgenda(payload) {
  const dateKey = state.stockSelectedDate;
  const events = eventsOnStockDate(payload, dateKey);
  ui.stockDayAgendaDate.textContent = dateKey ? stockDateLabel(dateKey, { year: "numeric" }) : "日期未知";
  ui.stockDayAgendaCount.textContent = `${events.length} 项`;
  ui.stockDayAgendaList.replaceChildren();
  if (!events.some((event) => event.event_id === state.stockSelectedEventId)) {
    state.stockSelectedEventId = events[0]?.event_id || null;
  }
  events.forEach((event) => {
    const button = createElement("button", "stock-day-event");
    button.type = "button";
    button.dataset.selected = String(event.event_id === state.stockSelectedEventId);
    const heading = createElement("span", "stock-day-event-heading");
    heading.append(
      createElement("strong", "", stockEventTitle(event)),
      stockEventBadge(event.category_label || "公司事件", "category", event.category || "DISCLOSURE"),
    );
    button.append(
      heading,
      createElement("span", "stock-day-event-title", stockEventDisplayTitle(event)),
      createElement("span", "stock-day-event-meta", `${event.state_label || ""} · ${event.source_label || "公开来源"}`),
    );
    button.addEventListener("click", () => {
      state.stockSelectedEventId = event.event_id;
      renderStockDayAgenda(payload);
    });
    ui.stockDayAgendaList.append(button);
  });
  ui.stockDayAgendaEmpty.hidden = events.length > 0;
  ui.stockEventDetail.hidden = events.length === 0;
  renderStockEventDetail(
    events.find((event) => event.event_id === state.stockSelectedEventId) || null,
  );
}

function renderStockCalendar(payload) {
  const monthKey = state.stockCalendarMonth;
  const [year, month] = String(monthKey || "").split("-").map(Number);
  if (!year || !month) return;
  const todayKey = shanghaiDateKey(state.viewPayload?.server_time || new Date().toISOString());
  ui.stockCalendarTitle.textContent = `${year}年${month}月`;
  ui.stockCalendarGrid.replaceChildren();
  const first = new Date(Date.UTC(year, month - 1, 1));
  const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
  const leading = (first.getUTCDay() + 6) % 7;
  const totalCells = Math.ceil((leading + daysInMonth) / 7) * 7;
  const eventsByDate = new Map();
  (payload?.events || []).forEach((event) => {
    if (!String(event.event_date || "").startsWith(`${monthKey}-`)) return;
    const items = eventsByDate.get(event.event_date) || [];
    items.push(event);
    eventsByDate.set(event.event_date, items);
  });
  for (let index = 0; index < totalCells; index += 1) {
    const day = index - leading + 1;
    if (day < 1 || day > daysInMonth) {
      const blank = createElement("span", "stock-calendar-day stock-calendar-day-blank");
      blank.setAttribute("aria-hidden", "true");
      ui.stockCalendarGrid.append(blank);
      continue;
    }
    const dateKey = `${monthKey}-${String(day).padStart(2, "0")}`;
    const dayEvents = eventsByDate.get(dateKey) || [];
    const cell = createElement("div", "stock-calendar-day");
    cell.setAttribute("role", "gridcell");
    cell.dataset.today = String(dateKey === todayKey);
    cell.dataset.selected = String(dateKey === state.stockSelectedDate);
    const dateButton = createElement("button", "stock-calendar-date", String(day));
    dateButton.type = "button";
    dateButton.setAttribute("aria-label", `${stockDateLabel(dateKey)}，${dayEvents.length} 项事件`);
    dateButton.addEventListener("click", () => {
      state.stockSelectedDate = dateKey;
      state.stockSelectedEventId = dayEvents[0]?.event_id || null;
      renderStockCalendar(payload);
    });
    cell.append(dateButton);
    const items = createElement("div", "stock-calendar-events");
    dayEvents.slice(0, 3).forEach((event) => {
      const item = createElement("button", "stock-calendar-event", event.stock_name || event.stock_code || "股票");
      item.type = "button";
      item.dataset.category = event.category || "DISCLOSURE";
      item.title = `${stockEventTitle(event)} · ${stockEventDisplayTitle(event)}`;
      item.addEventListener("click", () => {
        state.stockSelectedDate = dateKey;
        state.stockSelectedEventId = event.event_id;
        renderStockCalendar(payload);
      });
      items.append(item);
    });
    if (dayEvents.length > 3) {
      items.append(createElement("span", "stock-calendar-more", `+${dayEvents.length - 3}`));
    }
    cell.append(items);
    ui.stockCalendarGrid.append(cell);
  }
  const minimumMonth = String(payload?.window_start || monthKey).slice(0, 7);
  const maximumMonth = String(payload?.window_end || monthKey).slice(0, 7);
  ui.stockCalendarPrevious.disabled = monthKey <= minimumMonth;
  ui.stockCalendarNext.disabled = monthKey >= maximumMonth;
  renderStockDayAgenda(payload);
}

function renderStockTimeline(payload) {
  ui.stockEventsTimeline.replaceChildren();
  const todayKey = shanghaiDateKey(state.viewPayload?.server_time || new Date().toISOString());
  const allEvents = payload?.events || [];
  const upcoming = allEvents
    .filter((event) => event.event_date >= todayKey)
    .sort((left, right) => (
      String(left.event_date || "").localeCompare(String(right.event_date || ""))
      || String(left.sort_at || "").localeCompare(String(right.sort_at || ""))
    ));
  const history = allEvents
    .filter((event) => event.event_date < todayKey)
    .sort((left, right) => (
      String(right.event_date || "").localeCompare(String(left.event_date || ""))
      || String(right.sort_at || "").localeCompare(String(left.sort_at || ""))
    ));
  const events = [...upcoming, ...history].slice(0, 240);
  let previousDate = null;
  let group = null;
  events.forEach((event) => {
    if (event.event_date !== previousDate) {
      group = createElement("section", "stock-timeline-group");
      const heading = createElement("header", "stock-timeline-date");
      heading.append(
        createElement("strong", "", stockDateLabel(event.event_date)),
        createElement("span", "", event.event_date),
      );
      group.append(heading);
      ui.stockEventsTimeline.append(group);
      previousDate = event.event_date;
    }
    const button = createElement("button", "stock-timeline-event");
    button.type = "button";
    const body = createElement("span", "stock-timeline-event-body");
    body.append(
      createElement("strong", "", stockEventDisplayTitle(event)),
      createElement("span", "", `${stockEventTitle(event)} · ${event.source_label || "公开来源"}`),
    );
    button.append(
      stockEventBadge(event.category_label || "公司事件", "category", event.category || "DISCLOSURE"),
      body,
      createElement("span", "stock-timeline-state", event.state_label || ""),
    );
    button.addEventListener("click", () => {
      state.stockCalendarMonth = String(event.event_date).slice(0, 7);
      state.stockSelectedDate = event.event_date;
      state.stockSelectedEventId = event.event_id;
      state.stockEventView = "CALENDAR";
      renderStockEvents(payload);
    });
    group?.append(button);
  });
  if ((payload?.events || []).length > events.length) {
    ui.stockEventsTimeline.append(
      createElement("p", "stock-timeline-limit", `时间线仅渲染前 ${events.length} 项；可缩小股票或事件筛选范围。`),
    );
  }
  ui.stockEventsTimelineEmpty.hidden = events.length > 0;
}

function renderStockEventSources(payload) {
  const sources = payload?.source_states || [];
  ui.stockEventsSourceSummary.textContent = sources.length
    ? `最近检查 ${payload.source_checked_at ? formatTime(payload.source_checked_at) : "未知"} · ${sources.length} 组来源`
    : "等待首次来源检查";
  ui.stockEventsSourceList.replaceChildren();
  const labels = {
    SUCCESS: "已更新",
    EMPTY: "本轮为空",
    PARTIAL: "部分可用",
    ERROR: "暂不可用",
  };
  sources.forEach((source) => {
    const row = createElement("article", "stock-source-row");
    const heading = createElement("div", "stock-source-heading");
    heading.append(
      createElement("strong", "", source.label || "公开来源"),
      stockEventBadge(labels[source.status] || source.status || "未知", "source", source.status || "UNKNOWN"),
    );
    const linkUrl = safeExternalUrl(source.source_url);
    const detail = createElement("p", "", source.detail || "未提供来源说明");
    row.append(heading, detail);
    const meta = createElement("p", "stock-source-meta");
    meta.textContent = [
      source.record_count === null || source.record_count === undefined ? null : `${source.record_count} 条`,
      source.checked_at ? formatTime(source.checked_at) : null,
    ].filter(Boolean).join(" · ") || "尚无成功检查时间";
    row.append(meta);
    if (linkUrl) {
      const link = createElement("a", "", "打开来源 ↗");
      link.href = linkUrl;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      row.append(link);
    }
    ui.stockEventsSourceList.append(row);
  });
}

function renderStockEvents(payload) {
  const active = state.projectionKind === "stock_events";
  ui.stockEvents.hidden = !active;
  if (!active) {
    state.stockEventPayload = null;
    return;
  }
  state.stockEventPayload = payload;
  const todayKey = shanghaiDateKey(state.viewPayload?.server_time || new Date().toISOString());
  const minimumMonth = String(payload?.window_start || todayKey || "").slice(0, 7);
  const maximumMonth = String(payload?.window_end || todayKey || "").slice(0, 7);
  if (!state.stockCalendarMonth || state.stockCalendarMonth < minimumMonth || state.stockCalendarMonth > maximumMonth) {
    state.stockCalendarMonth = String(todayKey || payload?.window_start || "").slice(0, 7);
  }
  const monthEvents = (payload?.events || []).filter(
    (event) => String(event.event_date || "").startsWith(`${state.stockCalendarMonth}-`),
  );
  if (!state.stockSelectedDate || !state.stockSelectedDate.startsWith(`${state.stockCalendarMonth}-`)) {
    state.stockSelectedDate = todayKey?.startsWith(`${state.stockCalendarMonth}-`)
      ? todayKey
      : monthEvents[0]?.event_date || `${state.stockCalendarMonth}-01`;
    state.stockSelectedEventId = null;
  }
  ui.stockEventsScope.textContent = [
    `关注股 ${Number(payload?.manual_security_count || 0)}`,
    `每日入选 ${Number(payload?.auto_security_count || 0)}`,
    `当前显示 ${Number(payload?.selected_security_count || 0)}`,
    payload?.selection_trade_date ? `名单截至 ${payload.selection_trade_date}` : null,
  ].filter(Boolean).join(" · ");
  ui.stockEventsWindow.textContent = payload?.window_start && payload?.window_end
    ? `事件范围 ${payload.window_start} 至 ${payload.window_end} · ${Number(payload.event_count || 0)} 项`
    : "等待首次成功采集";
  const messages = payload?.coverage_messages || [];
  ui.stockEventsNotice.hidden = messages.length === 0;
  ui.stockEventsNotice.textContent = messages.length
    ? `本轮覆盖提示：${messages.join("；")}。`
    : "";
  const calendar = state.stockEventView === "CALENDAR";
  ui.stockEventsCalendarTab.setAttribute("aria-selected", String(calendar));
  ui.stockEventsTimelineTab.setAttribute("aria-selected", String(!calendar));
  ui.stockEventsCalendarPanel.hidden = !calendar;
  ui.stockEventsTimelinePanel.hidden = calendar;
  if (calendar) renderStockCalendar(payload);
  else renderStockTimeline(payload);
  renderStockEventSources(payload);
}

function stockSelectorSecurities() {
  const securities = (state.stockEventPayload?.securities || []).map((item) => ({ ...item }));
  const known = new Set(securities.map((item) => String(item.code)));
  state.stockSelectorDraftManual.forEach((code) => {
    if (!known.has(code)) {
      const directoryEntry = state.stockDirectoryKnown.get(code) || {};
      securities.push({
        code,
        name: directoryEntry.name || null,
        market_label: directoryEntry.market_label || null,
        industry: directoryEntry.industry || null,
        is_manual: true,
        is_auto: false,
        auto_reasons: [],
        verification_state: "PENDING",
      });
    }
  });
  return securities;
}

function closeStockDirectorySuggestions() {
  state.stockDirectorySuggestions = [];
  state.stockDirectorySuggestionIndex = -1;
  ui.stockSelectorSuggestions.replaceChildren();
  ui.stockSelectorSuggestions.hidden = true;
  ui.stockSelectorAddQuery.setAttribute("aria-expanded", "false");
  ui.stockSelectorAddQuery.removeAttribute("aria-activedescendant");
  ui.stockSelectorAddQuery.removeAttribute("aria-busy");
}

function resetStockDirectorySearch() {
  state.stockDirectorySearchSerial += 1;
  if (state.stockDirectorySearchTimer !== null) {
    clearTimeout(state.stockDirectorySearchTimer);
    state.stockDirectorySearchTimer = null;
  }
  state.stockDirectorySelected = null;
  closeStockDirectorySuggestions();
}

function chooseStockDirectorySuggestion(suggestion) {
  state.stockDirectoryKnown.set(String(suggestion.code), suggestion);
  state.stockDirectorySelected = suggestion;
  ui.stockSelectorAddQuery.value = `${suggestion.name} ${suggestion.code}`;
  ui.stockSelectorStatus.textContent = `已选择 ${suggestion.name} ${suggestion.code}，点击添加。`;
  closeStockDirectorySuggestions();
}

function renderStockDirectorySuggestions(status = "SUCCESS") {
  const suggestions = state.stockDirectorySuggestions;
  ui.stockSelectorSuggestions.replaceChildren();
  if (!suggestions.length) {
    const message = status === "UNAVAILABLE"
      ? "股票目录正在等待更新，可直接输入6位代码。"
      : "本地股票目录没有匹配结果。";
    const empty = createElement("div", "stock-selector-suggestion-empty", message);
    empty.setAttribute("role", "option");
    empty.setAttribute("aria-disabled", "true");
    ui.stockSelectorSuggestions.append(empty);
  } else {
    suggestions.forEach((suggestion, index) => {
      const option = createElement("button", "stock-selector-suggestion");
      option.type = "button";
      option.id = `stock-selector-suggestion-${index}`;
      option.setAttribute("role", "option");
      option.setAttribute("aria-selected", String(index === state.stockDirectorySuggestionIndex));
      const identity = createElement("span", "stock-selector-suggestion-identity");
      identity.append(
        createElement("strong", "", suggestion.name),
        createElement("span", "", suggestion.code),
      );
      const context = [suggestion.market_label, suggestion.industry].filter(Boolean).join(" · ");
      option.append(identity, createElement("span", "stock-selector-suggestion-context", context));
      option.addEventListener("click", () => chooseStockDirectorySuggestion(suggestion));
      ui.stockSelectorSuggestions.append(option);
    });
  }
  ui.stockSelectorSuggestions.hidden = false;
  ui.stockSelectorAddQuery.setAttribute("aria-expanded", "true");
  const active = state.stockDirectorySuggestionIndex;
  if (active >= 0) {
    ui.stockSelectorAddQuery.setAttribute("aria-activedescendant", `stock-selector-suggestion-${active}`);
  } else {
    ui.stockSelectorAddQuery.removeAttribute("aria-activedescendant");
  }
}

async function searchStockDirectory(query, serial) {
  try {
    const params = new URLSearchParams({ q: query, limit: "8" });
    const response = await fetch(
      `/api/monitors/${encodeURIComponent(state.monitorId)}/stocks/search?${params.toString()}`,
      { headers: { Accept: "application/json" } },
    );
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP_${response.status}`);
    if (serial !== state.stockDirectorySearchSerial) return;
    state.stockDirectorySuggestions = Array.isArray(payload.matches) ? payload.matches : [];
    state.stockDirectorySuggestions.forEach((item) => {
      state.stockDirectoryKnown.set(String(item.code), item);
    });
    state.stockDirectorySuggestionIndex = state.stockDirectorySuggestions.length ? 0 : -1;
    renderStockDirectorySuggestions(payload.status);
    if (payload.status === "STALE") {
      ui.stockSelectorStatus.textContent = "搜索推荐来自上次成功更新的本地股票目录。";
    } else if (payload.status === "UNAVAILABLE") {
      ui.stockSelectorStatus.textContent = "股票目录暂不可用，也可直接输入6位代码。";
    } else {
      ui.stockSelectorStatus.textContent = "";
    }
  } catch (error) {
    if (serial !== state.stockDirectorySearchSerial) return;
    closeStockDirectorySuggestions();
    ui.stockSelectorStatus.textContent = `股票搜索暂不可用，可直接输入6位代码 · ${error.message}`;
  } finally {
    if (serial === state.stockDirectorySearchSerial) {
      ui.stockSelectorAddQuery.removeAttribute("aria-busy");
    }
  }
}

function scheduleStockDirectorySearch() {
  state.stockDirectorySelected = null;
  state.stockDirectorySearchSerial += 1;
  const serial = state.stockDirectorySearchSerial;
  if (state.stockDirectorySearchTimer !== null) {
    clearTimeout(state.stockDirectorySearchTimer);
    state.stockDirectorySearchTimer = null;
  }
  closeStockDirectorySuggestions();
  const query = ui.stockSelectorAddQuery.value.trim();
  if (!query) {
    ui.stockSelectorStatus.textContent = "";
    return;
  }
  ui.stockSelectorAddQuery.setAttribute("aria-busy", "true");
  state.stockDirectorySearchTimer = setTimeout(() => {
    state.stockDirectorySearchTimer = null;
    void searchStockDirectory(query, serial);
  }, 180);
}

function renderStockSelector() {
  const query = String(ui.stockSelectorSearch.value || "").trim().toLocaleLowerCase("zh-CN");
  const manual = state.stockSelectorTab === "MANUAL";
  const securities = stockSelectorSecurities().filter((item) => {
    const code = String(item.code || "");
    const isInTab = manual
      ? state.stockSelectorDraftManual.has(code)
      : Boolean(item.is_auto);
    if (!isInTab) return false;
    return !query || `${code}${item.name || ""}`.toLocaleLowerCase("zh-CN").includes(query);
  });
  ui.stockSelectorManualTab.setAttribute("aria-selected", String(manual));
  ui.stockSelectorAutoTab.setAttribute("aria-selected", String(!manual));
  ui.stockSelectorAddForm.hidden = !manual;
  ui.stockSelectorHelp.textContent = manual
    ? "输入中文名称或代码搜索；添加后保存到本机并持续监控。勾选只控制当前页面显示。"
    : "每日入选按最近交易日强势、涨停/连板与近期新高股池冻结；不表示推荐。";
  ui.stockSelectorList.replaceChildren();
  securities.forEach((security) => {
    const code = String(security.code);
    const row = createElement("div", "stock-selector-row");
    const choice = createElement("label", "stock-selector-choice");
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = state.stockSelectorDraftCodes.has(code);
    checkbox.setAttribute("aria-label", `显示 ${security.name || code}`);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) state.stockSelectorDraftCodes.add(code);
      else state.stockSelectorDraftCodes.delete(code);
      renderStockSelector();
    });
    const identity = createElement("span", "stock-selector-identity");
    identity.append(
      createElement("strong", "", security.name || "名称待来源确认"),
      createElement("span", "", code),
    );
    choice.append(checkbox, identity);
    const tags = createElement("span", "stock-selector-tags");
    if (state.stockSelectorDraftManual.has(code)) {
      tags.append(stockEventBadge("长期关注", "manual"));
    }
    if (security.is_auto) {
      tags.append(stockEventBadge((security.auto_reasons || []).join(" / ") || "每日入选", "auto"));
    }
    row.append(choice, tags);
    if (manual) {
      const remove = createElement("button", "stock-selector-remove", "移除");
      remove.type = "button";
      remove.setAttribute("aria-label", `移除关注 ${security.name || code}`);
      remove.addEventListener("click", () => {
        state.stockSelectorDraftManual.delete(code);
        if (!security.is_auto) state.stockSelectorDraftCodes.delete(code);
        renderStockSelector();
      });
      row.append(remove);
    }
    ui.stockSelectorList.append(row);
  });
  if (!securities.length) {
    ui.stockSelectorList.append(
      createElement("p", "stock-selector-empty", query ? "没有匹配股票。" : manual ? "尚未添加手动关注股票。" : "等待每日动态股池。"),
    );
  }
  ui.stockSelectorCount.textContent = `已选 ${state.stockSelectorDraftCodes.size} 支`;
}

function openStockSelector() {
  const configurationCodes = state.viewPayload?.monitor?.configuration?.values?.manual_stock_codes || [];
  const selectedCodes = state.stockEventPayload?.selected_stock_codes || [];
  state.stockSelectorOriginalManual = new Set(configurationCodes.map(String));
  state.stockSelectorDraftManual = new Set(configurationCodes.map(String));
  state.stockSelectorDraftCodes = new Set(selectedCodes.map(String));
  state.stockDirectoryKnown = new Map(
    (state.stockEventPayload?.securities || []).map((item) => [String(item.code), item]),
  );
  state.stockSelectorTab = "MANUAL";
  ui.stockSelectorSearch.value = "";
  ui.stockSelectorAddQuery.value = "";
  ui.stockSelectorStatus.textContent = "";
  resetStockDirectorySearch();
  renderStockSelector();
  ui.stockSelectorDialog.showModal();
  ui.stockSelectorAddQuery.focus();
}

async function applyStockSelector() {
  if (state.stockSelectorSubmitting) return;
  const manual = [...state.stockSelectorDraftManual].sort();
  const original = [...state.stockSelectorOriginalManual].sort();
  const manualChanged = manual.join(",") !== original.join(",");
  if (!state.stockSelectorDraftCodes.size && !manualChanged) {
    ui.stockSelectorStatus.textContent = "请至少选择一支股票用于显示。";
    return;
  }
  state.stockSelectorSubmitting = true;
  ui.stockSelectorApply.disabled = true;
  ui.stockSelectorStatus.textContent = manualChanged ? "正在保存关注股票…" : "正在应用显示范围…";
  try {
    if (manualChanged) {
      const response = await fetch(
        `/api/monitors/${encodeURIComponent(state.monitorId)}/configuration`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ values: { manual_stock_codes: manual } }),
        },
      );
      const result = await response.json();
      if (!response.ok) throw new Error(result.detail || `HTTP_${response.status}`);
      state.pendingConfigurationRunAfter = result.refresh_requested ? (state.latestRunId ?? 0) : null;
      state.stockSelectedCodes = null;
    } else {
      state.stockSelectedCodes = [...state.stockSelectorDraftCodes];
    }
    ui.stockSelectorDialog.close();
    await loadView({ preserveSeries: false });
  } catch (error) {
    ui.stockSelectorStatus.textContent = `应用失败 · ${error.message}`;
  } finally {
    state.stockSelectorSubmitting = false;
    ui.stockSelectorApply.disabled = false;
  }
}

ui.stockEventsCalendarTab.addEventListener("click", () => {
  state.stockEventView = "CALENDAR";
  renderStockEvents(state.stockEventPayload);
});

ui.stockEventsTimelineTab.addEventListener("click", () => {
  state.stockEventView = "TIMELINE";
  renderStockEvents(state.stockEventPayload);
});

ui.stockCalendarPrevious.addEventListener("click", () => {
  state.stockCalendarMonth = stockMonthShift(state.stockCalendarMonth, -1);
  state.stockSelectedDate = null;
  renderStockEvents(state.stockEventPayload);
});

ui.stockCalendarNext.addEventListener("click", () => {
  state.stockCalendarMonth = stockMonthShift(state.stockCalendarMonth, 1);
  state.stockSelectedDate = null;
  renderStockEvents(state.stockEventPayload);
});

ui.stockCalendarToday.addEventListener("click", () => {
  const todayKey = shanghaiDateKey(state.viewPayload?.server_time || new Date().toISOString());
  state.stockCalendarMonth = String(todayKey || "").slice(0, 7);
  state.stockSelectedDate = todayKey;
  state.stockSelectedEventId = null;
  renderStockEvents(state.stockEventPayload);
});

ui.stockEventsSelectButton.addEventListener("click", openStockSelector);
ui.stockSelectorClose.addEventListener("click", () => ui.stockSelectorDialog.close());
ui.stockSelectorCancel.addEventListener("click", () => ui.stockSelectorDialog.close());
ui.stockSelectorApply.addEventListener("click", () => { void applyStockSelector(); });
ui.stockSelectorSearch.addEventListener("input", renderStockSelector);
ui.stockSelectorManualTab.addEventListener("click", () => {
  state.stockSelectorTab = "MANUAL";
  renderStockSelector();
});
ui.stockSelectorAutoTab.addEventListener("click", () => {
  state.stockSelectorTab = "AUTO";
  ui.stockSelectorAddQuery.value = "";
  resetStockDirectorySearch();
  renderStockSelector();
});
ui.stockSelectorAddQuery.addEventListener("input", scheduleStockDirectorySearch);
ui.stockSelectorAddQuery.addEventListener("keydown", (event) => {
  const suggestions = state.stockDirectorySuggestions;
  if (!suggestions.length) return;
  if (event.key === "ArrowDown" || event.key === "ArrowUp") {
    event.preventDefault();
    const direction = event.key === "ArrowDown" ? 1 : -1;
    state.stockDirectorySuggestionIndex = (
      state.stockDirectorySuggestionIndex + direction + suggestions.length
    ) % suggestions.length;
    renderStockDirectorySuggestions();
    document.querySelector(`#stock-selector-suggestion-${state.stockDirectorySuggestionIndex}`)
      ?.scrollIntoView({ block: "nearest" });
    return;
  }
  if (event.key === "Escape") {
    event.preventDefault();
    closeStockDirectorySuggestions();
    return;
  }
  if (event.key === "Enter" && state.stockDirectorySuggestionIndex >= 0) {
    event.preventDefault();
    chooseStockDirectorySuggestion(suggestions[state.stockDirectorySuggestionIndex]);
    ui.stockSelectorAddForm.requestSubmit();
  }
});
ui.stockSelectorAddForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const query = ui.stockSelectorAddQuery.value.trim();
  const selected = state.stockDirectorySelected;
  const code = selected?.code || (/^\d{6}$/.test(query) ? query : "");
  const limit = Number(state.stockEventPayload?.manual_limit || 50);
  if (!/^\d{6}$/.test(String(code))) {
    ui.stockSelectorStatus.textContent = "请从搜索推荐中选择股票，或直接输入6位代码。";
    return;
  }
  if (!state.stockSelectorDraftManual.has(code) && state.stockSelectorDraftManual.size >= limit) {
    ui.stockSelectorStatus.textContent = `手动关注最多 ${limit} 支。`;
    return;
  }
  state.stockSelectorDraftManual.add(code);
  state.stockSelectorDraftCodes.add(code);
  ui.stockSelectorAddQuery.value = "";
  const stockLabel = selected ? `${selected.name} ${code}` : code;
  resetStockDirectorySearch();
  ui.stockSelectorStatus.textContent = `${stockLabel} 已加入待保存关注。`;
  renderStockSelector();
});
ui.stockSelectorDialog.addEventListener("click", (event) => {
  if (event.target === ui.stockSelectorDialog) ui.stockSelectorDialog.close();
});
ui.stockSelectorDialog.addEventListener("close", resetStockDirectorySearch);

function renderRadarPriceFilter(pricePayload) {
  const choices = Array.isArray(pricePayload?.filter_choices)
    ? pricePayload.filter_choices
    : [];
  const allowed = new Set(choices.map((choice) => choice.value));
  if (!allowed.has(state.radarPriceState)) state.radarPriceState = "*";
  const groupCounts = (pricePayload?.rows || []).reduce((counts, row) => {
    const group = String(row.price_state_group || "NEUTRAL");
    counts[group] = (counts[group] || 0) + 1;
    return counts;
  }, {});
  ui.radarPriceState.replaceChildren();
  choices.forEach((choice) => {
    const option = document.createElement("option");
    option.value = choice.value;
    const count = choice.value === "*"
      ? (pricePayload?.rows || []).length
      : (groupCounts[choice.value] || 0);
    option.textContent = `${choice.label}（${count}）`;
    option.selected = choice.value === state.radarPriceState;
    if (choice.description) option.title = choice.description;
    ui.radarPriceState.append(option);
  });
  const selected = choices.find((choice) => choice.value === state.radarPriceState);
  if (selected?.description) {
    ui.radarPriceState.title = selected.description;
    ui.radarPriceState.setAttribute("aria-description", selected.description);
  } else {
    ui.radarPriceState.removeAttribute("title");
    ui.radarPriceState.removeAttribute("aria-description");
  }
  ui.radarPriceState.onchange = () => {
    state.radarPriceState = ui.radarPriceState.value;
    state.tablePage = 1;
    renderRadarPriceFilter(state.viewPayload?.altcoin_price_position);
    renderRadarTableView(state.viewPayload);
    requestAnimationFrame(updateQuoteHorizontalScrollbar);
  };
}

function renderRadarTableView(payload) {
  if (!payload || payload.monitor?.projection_kind !== "altcoin_radar") return;
  if (!["TABLE", "POSITION"].includes(state.radarTab)) return;
  const showingPosition = state.radarTab === "POSITION";
  if (!showingPosition) {
    ui.quoteTableTitle.textContent = payload.monitor.table_title;
    ui.quoteScroll.setAttribute(
      "aria-label",
      "USDⓈ-M 永续合约异动候选，可横向滚动",
    );
    ui.dataCutoff.hidden = false;
    const cutoff = payload.monitor.data_run?.completed_at;
    ui.dataCutoff.textContent = `${payload.monitor.data_status.cutoff_label}：${cutoff ? formatTime(cutoff) : "—"}`;
    ui.dataCutoff.dataset.status = payload.monitor.data_status.tone;
    renderRunSummary(payload.run_summary);
    renderTable(
      payload.monitor.columns,
      payload.rows,
      payload.selected_series_key,
      payload.current_issues,
      payload.monitor.selected_filters,
      payload.monitor.data_status,
    );
    return;
  }
  const position = payload.altcoin_price_position || {};
  const allRows = Array.isArray(position.rows) ? position.rows : [];
  const rows = state.radarPriceState === "*"
    ? allRows
    : allRows.filter((row) => row.price_state_group === state.radarPriceState);
  ui.quoteTableTitle.textContent = position.table_title || "日线价格位置";
  ui.quoteScroll.setAttribute(
    "aria-label",
    "USDⓈ-M 永续合约日线价格位置，可横向滚动",
  );
  ui.dataCutoff.hidden = false;
  ui.dataCutoff.textContent = position.price_cutoff_at
    ? `当前价截至：${formatTime(position.price_cutoff_at)}`
    : "当前价截至：—";
  ui.dataCutoff.dataset.status = position.status === "CURRENT" ? "HEALTHY" : "STALE";
  renderRunSummary(position.summary || []);
  renderTable(
    position.columns || [],
    rows,
    null,
    [],
    {},
    { kind: position.status || "EMPTY" },
  );
  ui.quoteEmpty.textContent = position.empty_message
    || "当前筛选没有匹配的价格状态。";
}

function applyRadarTabState() {
  const isAltcoinRadar = state.projectionKind === "altcoin_radar";
  ui.radarViewTabs.hidden = !isAltcoinRadar;
  ui.filters.hidden = false;
  if (!isAltcoinRadar) {
    ui.radarPriceStateField.hidden = true;
    [ui.tableRegion, ui.historyRegion, ui.evaluationRegion].forEach((region) => {
      region.removeAttribute("role");
      region.removeAttribute("aria-labelledby");
    });
    return;
  }

  const tabs = {
    TABLE: ui.radarTableTab,
    POSITION: ui.radarPositionTab,
    HISTORY: ui.radarHistoryTab,
    EVALUATION: ui.radarEvaluationTab,
  };
  Object.entries(tabs).forEach(([key, tab]) => {
    const selected = key === state.radarTab;
    tab.setAttribute("aria-selected", String(selected));
    tab.tabIndex = selected ? 0 : -1;
  });

  const showingTable = state.radarTab === "TABLE";
  const showingPosition = state.radarTab === "POSITION";
  const showingHistory = state.radarTab === "HISTORY";
  const showingEvaluation = state.radarTab === "EVALUATION";
  ui.tableRegion.hidden = !(showingTable || showingPosition);
  ui.historyRegion.hidden = !showingHistory;
  ui.evaluationRegion.hidden = !showingEvaluation;
  ui.filtersRegion.hidden = showingEvaluation;
  ui.filters.hidden = !(showingTable || showingPosition);
  ui.filters.querySelectorAll(".dynamic-filter-field").forEach((field) => {
    field.hidden = !showingTable;
  });
  ui.radarPriceStateField.hidden = !showingPosition;
  ui.historyWindowField.hidden = !showingHistory;
  const methodNote = showingPosition
    ? String(state.viewPayload?.altcoin_price_position?.method_note || "")
    : showingTable
      ? String(state.viewPayload?.monitor?.method_note || "")
      : "";
  ui.monitorMethodNote.textContent = methodNote;
  ui.monitorMethodNote.hidden = !methodNote;

  const panels = [
    [ui.historyRegion, "radar-history-tab"],
    [ui.evaluationRegion, "radar-evaluation-tab"],
  ];
  ui.tableRegion.setAttribute("role", "tabpanel");
  ui.tableRegion.setAttribute(
    "aria-labelledby",
    showingPosition ? "radar-position-tab" : "radar-table-tab",
  );
  panels.forEach(([region, labelledBy]) => {
    region.setAttribute("role", "tabpanel");
    region.setAttribute("aria-labelledby", labelledBy);
  });
  requestAnimationFrame(() => {
    if (showingHistory && state.chartModel) drawHistoryChart();
    updateQuoteHorizontalScrollbar();
  });
}

function activateRadarTab(tab) {
  if (!Object.hasOwn(RADAR_TAB_LOCATION_VALUES, tab)) return;
  state.radarTab = tab;
  state.tablePage = 1;
  syncMonitorLocation(state.monitorId);
  applyRadarTabState();
  loadView();
}

ui.eventUpcomingTab.addEventListener("click", () => {
  state.eventTab = "UPCOMING";
  applyEventTabState();
});

ui.eventHistoryTab.addEventListener("click", () => {
  state.eventTab = "HISTORY";
  state.eventHistoryPage = 1;
  applyEventTabState();
});

ui.radarTableTab.addEventListener("click", () => activateRadarTab("TABLE"));
ui.radarPositionTab.addEventListener("click", () => activateRadarTab("POSITION"));
ui.radarHistoryTab.addEventListener("click", () => activateRadarTab("HISTORY"));
ui.radarEvaluationTab.addEventListener("click", () => activateRadarTab("EVALUATION"));

ui.radarViewTabs.addEventListener("keydown", (event) => {
  if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
  const order = ["TABLE", "POSITION", "HISTORY", "EVALUATION"];
  const current = Math.max(0, order.indexOf(state.radarTab));
  const next = event.key === "Home"
    ? 0
    : event.key === "End"
      ? order.length - 1
      : (current + (event.key === "ArrowRight" ? 1 : -1) + order.length) % order.length;
  event.preventDefault();
  activateRadarTab(order[next]);
  ({
    TABLE: ui.radarTableTab,
    POSITION: ui.radarPositionTab,
    HISTORY: ui.radarHistoryTab,
    EVALUATION: ui.radarEvaluationTab,
  })[order[next]].focus();
});

ui.eventHistoryPrevious.addEventListener("click", () => {
  state.eventHistoryPage = Math.max(1, state.eventHistoryPage - 1);
  renderEventHistory(state.marketEventPayload);
});

ui.eventHistoryNext.addEventListener("click", () => {
  state.eventHistoryPage += 1;
  renderEventHistory(state.marketEventPayload);
});

function renderRunSummary(items) {
  ui.runSummary.replaceChildren();
  (items || []).forEach((item) => {
    const group = document.createElement("div");
    if (item.description) {
      group.title = item.description;
      group.setAttribute("aria-description", item.description);
    }
    const rendered = cellValue({ [item.key]: item.value }, item);
    group.append(createElement("dt", "", item.label));
    group.append(createElement("dd", rendered.missing ? "missing-value" : "", rendered.text));
    ui.runSummary.append(group);
  });
  ui.runSummary.hidden = ui.runSummary.childElementCount === 0;
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
  ui.collectionCadence.title = "按各启用监控当前生效的前台或后台周期计算；一轮可能包含多个公开 HTTP 请求。";
  ui.lastRefresh.textContent = formatTime(load.latest_completed_at);
}

function monitorCompactLabel(monitor) {
  const configured = MONITOR_COMPACT_LABELS[monitor.monitor_id];
  if (configured) return configured;
  const normalized = String(monitor.display_name || monitor.monitor_id)
    .replace(/[^\p{L}\p{N}]+/gu, "")
    .trim();
  return normalized.slice(0, 3) || "监控";
}

function applyMonitorRailState({ persist = true } = {}) {
  const collapsed = Boolean(state.monitorRailCollapsed);
  ui.appShell.dataset.railCollapsed = String(collapsed);
  ui.monitorRailToggle.setAttribute("aria-expanded", String(!collapsed));
  ui.monitorRailToggle.setAttribute(
    "aria-label",
    collapsed ? "展开监控列表" : "收起监控列表",
  );
  ui.monitorRailToggle.title = collapsed ? "展开监控列表" : "收起监控列表";
  ui.monitorRailToggleIcon.textContent = collapsed ? "›" : "‹";
  if (persist) {
    try {
      window.localStorage.setItem(MONITOR_RAIL_STORAGE_KEY, String(collapsed));
    } catch (_error) {
      // 页面仍可使用；仅不记住折叠状态。
    }
  }
  requestAnimationFrame(() => {
    updateQuoteHorizontalScrollbar();
    if (state.chartModel) drawHistoryChart();
  });
}

ui.monitorRailToggle.addEventListener("click", () => {
  state.monitorRailCollapsed = !state.monitorRailCollapsed;
  applyMonitorRailState();
});

function renderMonitorList(monitors) {
  ui.monitorList.replaceChildren();
  monitors.forEach((monitor) => {
    const button = createElement("button", "monitor-link");
    button.type = "button";
    button.dataset.monitorId = monitor.monitor_id;
    button.dataset.status = monitor.operational_status.tone;
    if (monitor.monitor_id === state.monitorId) button.setAttribute("aria-current", "page");
    button.setAttribute(
      "aria-label",
      `${monitor.display_name}，${monitor.operational_status.label}`,
    );
    button.title = `${monitor.display_name} · ${monitor.operational_status.label}`;
    button.append(
      createElement("span", "monitor-link-name", monitor.display_name),
      createElement("span", "monitor-link-compact", monitorCompactLabel(monitor)),
    );
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
        buybackStockQuery: state.buybackStockQuery,
        eventQuery: state.eventQuery,
        stockSelectedCodes: state.stockSelectedCodes,
        radarTab: state.radarTab,
        radarPriceState: state.radarPriceState,
        tableSort: state.tableSort,
        tablePage: state.tablePage,
        tableScrollLeft: ui.quoteScroll.scrollLeft,
      };
      clearTimeout(state.buybackStockSearchTimer);
      state.buybackStockSearchTimer = null;
      clearTimeout(state.eventSearchTimer);
      state.eventSearchTimer = null;
      state.monitorId = monitor.monitor_id;
      state.filters = {};
      state.seriesKey = null;
      state.buybackStockQuery = "";
      state.eventQuery = "";
      state.stockSelectedCodes = null;
      state.radarTab = "TABLE";
      state.radarPriceState = "*";
      state.tableSort = null;
      state.tablePage = 1;
      ui.quoteScroll.scrollLeft = 0;
      if (!await loadView({ preserveSeries: false })) {
        state.monitorId = previous.monitorId;
        state.filters = previous.filters;
        state.seriesKey = previous.seriesKey;
        state.buybackStockQuery = previous.buybackStockQuery;
        state.eventQuery = previous.eventQuery;
        state.stockSelectedCodes = previous.stockSelectedCodes;
        state.radarTab = previous.radarTab;
        state.radarPriceState = previous.radarPriceState;
        state.tableSort = previous.tableSort;
        state.tablePage = previous.tablePage;
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
  let showedManualResult = false;
  const isBuyback = monitor.projection_kind === "buyback";
  const canManualRefresh = isBuyback || monitor.projection_kind === "btc_intelligence";
  const collecting = monitor.latest_run?.status === "RUNNING";
  const schedule = monitor.automatic_collection;
  ui.monitorRefreshButton.hidden = !canManualRefresh;
  ui.monitorRefreshButton.disabled = !monitor.enabled
    || collecting
    || state.manualRefreshSubmitting;
  ui.monitorRefreshButton.textContent = state.manualRefreshSubmitting
    ? "正在请求…"
    : collecting && canManualRefresh
      ? "正在刷新"
      : "手动刷新";
  ui.monitorRefreshButton.title = !monitor.enabled
    ? "请先开启监控"
    : isBuyback
      ? "无论当前是否处于交易时段，显式采集一次公开来源"
      : "显式刷新 BTC 公开市场情报";
  if (
    state.pendingManualRefresh?.monitorId === monitor.monitor_id
    && monitor.latest_run?.run_id > state.pendingManualRefresh.runAfter
    && monitor.latest_run.status !== "RUNNING"
  ) {
    ui.monitorControlStatus.textContent = ["SUCCESS", "PARTIAL"].includes(monitor.latest_run.status)
      ? "手动刷新已完成"
      : "手动刷新失败；仍显示最近已提交数据";
    state.pendingManualRefresh = null;
    showedManualResult = true;
  }
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
  const cadence = monitor.collection_cadence;
  if (cadence?.adaptive) {
    ui.monitorControlStatus.title = [
      `后台 ${formatCadenceSeconds(cadence.background_interval_seconds)}`,
      `页面可见 ${formatCadenceSeconds(cadence.foreground_interval_seconds)}`,
    ].join("；");
  } else {
    ui.monitorControlStatus.title = "";
  }
  if (
    cadence?.adaptive
    && monitor.enabled
    && !showedManualResult
    && !state.pendingManualRefresh
    && !state.manualRefreshSubmitting
    && !state.pendingControl
  ) {
    ui.monitorControlStatus.textContent = cadence.foreground_active
      ? `观察模式 · 每 ${formatCadenceSeconds(cadence.foreground_interval_seconds)}采集`
      : `后台模式 · 每 ${formatCadenceSeconds(cadence.background_interval_seconds)}采集`;
  }
  if (
    isBuyback
    && monitor.enabled
    && !showedManualResult
    && !state.pendingManualRefresh
    && !state.manualRefreshSubmitting
    && schedule
  ) {
    const nextOpen = schedule.next_open_at
      ? `；下次自动刷新时段 ${formatTime(schedule.next_open_at)}`
      : "";
    ui.monitorControlStatus.textContent = `${schedule.detail}${nextOpen}`;
  }
}

ui.monitorRefreshButton.addEventListener("click", async () => {
  if (state.manualRefreshSubmitting || !state.monitorId) return;
  const monitorId = state.monitorId;
  const runAfter = state.latestRunId ?? 0;
  state.manualRefreshSubmitting = true;
  ui.monitorRefreshButton.disabled = true;
  ui.monitorRefreshButton.textContent = "正在请求…";
  ui.monitorControlStatus.textContent = "正在提交手动刷新请求";
  try {
    const response = await fetch(
      `/api/monitors/${encodeURIComponent(monitorId)}/refresh`,
      { method: "POST", headers: { Accept: "application/json" } },
    );
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP_${response.status}`);
    state.pendingManualRefresh = {
      monitorId,
      runAfter: Number(payload.run_after ?? runAfter),
    };
    ui.monitorControlStatus.textContent = "已请求手动刷新，等待本轮完成";
    state.manualRefreshSubmitting = false;
    await loadView();
    ui.monitorRefreshButton.focus();
  } catch (error) {
    ui.monitorControlStatus.textContent = `手动刷新失败 · ${error.message}`;
  } finally {
    state.manualRefreshSubmitting = false;
    ui.monitorRefreshButton.disabled = false;
  }
});

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

function updateFilterChoiceDescription(select, choices) {
  const selectedChoice = choices.find((choice) => choice.value === select.value);
  const description = String(selectedChoice?.description || "").trim();
  if (description) {
    select.title = description;
    select.setAttribute("aria-description", description);
  } else {
    select.removeAttribute("title");
    select.removeAttribute("aria-description");
  }
}

function createFilterLabel(filter) {
  const heading = createElement("span", "filter-label", filter.label);
  const describedChoices = filter.choices.filter((choice) => choice.description);
  if (!describedChoices.length) return heading;

  const help = createElement("span", "filter-help");
  help.tabIndex = 0;
  help.setAttribute("aria-label", `${filter.label}说明`);
  help.append(createElement("span", "field-help", "ⓘ"));
  const tooltip = createElement("span", "filter-help-tooltip");
  tooltip.setAttribute("role", "tooltip");
  describedChoices.forEach((choice) => {
    const row = createElement("span", "filter-help-row");
    row.append(
      createElement("strong", "", choice.label),
      createElement("span", "", choice.description),
    );
    tooltip.append(row);
  });
  help.append(tooltip);
  heading.append(help);
  return heading;
}

function renderFilters(filters, timeWindows, projectionKind) {
  ui.filters.dataset.projectionKind = projectionKind;
  ui.filters.querySelectorAll(".dynamic-filter-field").forEach((field) => field.remove());
  filters.forEach((filter) => {
    if (filter.multiple) {
      const field = createElement("div", "filter-field dynamic-filter-field");
      field.dataset.filterKey = filter.key;
      const heading = createFilterLabel(filter);
      heading.id = `filter-${filter.key}-label`;
      field.append(heading);
      const group = createElement("div", "filter-multi-choice");
      group.setAttribute("role", "group");
      group.setAttribute("aria-labelledby", heading.id);
      const selected = new Set(
        Array.isArray(filter.selected) ? filter.selected : [filter.selected],
      );
      const checkboxes = [];
      filter.choices.forEach((choice) => {
        const option = createElement("label", "filter-multi-option");
        if (choice.description) option.title = choice.description;
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.name = filter.key;
        checkbox.value = choice.value;
        checkbox.checked = selected.has(choice.value);
        checkbox.setAttribute("aria-label", choice.label);
        option.append(checkbox, createElement("span", "", choice.label));
        group.append(option);
        checkboxes.push(checkbox);
        checkbox.addEventListener("change", async () => {
          const previous = Array.isArray(state.filters[filter.key])
            ? [...state.filters[filter.key]]
            : [state.filters[filter.key]].filter(Boolean);
          const next = checkboxes
            .filter((item) => item.checked)
            .map((item) => item.value);
          if (!next.length) {
            checkbox.checked = true;
            return;
          }
          const previousSeries = state.seriesKey;
          const previousPage = state.tablePage;
          state.filters[filter.key] = next;
          state.tablePage = 1;
          if (!await loadView({ preserveSeries: false })) {
            state.filters[filter.key] = previous;
            state.seriesKey = previousSeries;
            state.tablePage = previousPage;
            checkboxes.forEach((item) => {
              item.checked = previous.includes(item.value);
            });
          }
        });
      });
      field.append(group);
      ui.filters.insertBefore(field, ui.buybackStockSearchField);
      return;
    }
    const label = createElement("label", "filter-field dynamic-filter-field");
    label.append(createFilterLabel(filter));
    const select = document.createElement("select");
    select.name = filter.key;
    select.setAttribute("aria-label", filter.label);
    filter.choices.forEach((choice) => {
      const option = document.createElement("option");
      option.value = choice.value;
      option.textContent = choice.label;
      option.selected = choice.value === filter.selected;
      if (choice.description) option.title = choice.description;
      select.append(option);
    });
    updateFilterChoiceDescription(select, filter.choices);
    select.addEventListener("change", async () => {
      const previous = state.filters[filter.key];
      const previousSeries = state.seriesKey;
      const previousPage = state.tablePage;
      state.filters[filter.key] = select.value;
      state.tablePage = 1;
      updateFilterChoiceDescription(select, filter.choices);
      if (!await loadView({ preserveSeries: false })) {
        state.filters[filter.key] = previous;
        state.seriesKey = previousSeries;
        state.tablePage = previousPage;
        select.value = previous;
        updateFilterChoiceDescription(select, filter.choices);
      }
    });
    label.append(select);
    ui.filters.insertBefore(label, ui.buybackStockSearchField);
  });
  const isBuyback = projectionKind === "buyback";
  const isMarketEvents = projectionKind === "market_events";
  const isStockEvents = projectionKind === "stock_events";
  ui.buybackStockSearchField.hidden = !isBuyback;
  if (document.activeElement !== ui.buybackStockSearch) {
    ui.buybackStockSearch.value = isBuyback ? state.buybackStockQuery : "";
  }
  ui.eventSearchField.hidden = !isMarketEvents && !isStockEvents;
  if (document.activeElement !== ui.eventSearch) {
    ui.eventSearch.value = isMarketEvents || isStockEvents ? state.eventQuery : "";
  }
  ui.timeWindow.replaceChildren();
  timeWindows.forEach((window) => {
    const option = document.createElement("option");
    option.value = String(window.hours);
    option.textContent = window.label;
    option.selected = window.hours === state.hours;
    ui.timeWindow.append(option);
  });
}

function runBuybackStockSearch() {
  clearTimeout(state.buybackStockSearchTimer);
  state.buybackStockSearchTimer = null;
  loadView({ preserveSeries: false });
}

ui.buybackStockSearch.addEventListener("input", () => {
  state.buybackStockQuery = ui.buybackStockSearch.value;
  state.tablePage = 1;
  clearTimeout(state.buybackStockSearchTimer);
  state.buybackStockSearchTimer = setTimeout(runBuybackStockSearch, 300);
});

ui.buybackStockSearch.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    runBuybackStockSearch();
  } else if (event.key === "Escape" && ui.buybackStockSearch.value) {
    event.preventDefault();
    ui.buybackStockSearch.value = "";
    state.buybackStockQuery = "";
    state.tablePage = 1;
    runBuybackStockSearch();
  }
});

function runEventSearch() {
  clearTimeout(state.eventSearchTimer);
  state.eventSearchTimer = null;
  loadView({ preserveSeries: false });
}

ui.eventSearch.addEventListener("input", () => {
  state.eventQuery = ui.eventSearch.value;
  state.tablePage = 1;
  clearTimeout(state.eventSearchTimer);
  state.eventSearchTimer = setTimeout(runEventSearch, 300);
});

ui.eventSearch.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    runEventSearch();
  } else if (event.key === "Escape" && ui.eventSearch.value) {
    event.preventDefault();
    ui.eventSearch.value = "";
    state.eventQuery = "";
    state.tablePage = 1;
    runEventSearch();
  }
});

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
    const formatted = new Intl.NumberFormat("zh-CN", {
      minimumFractionDigits: column.minimum_fraction_digits ?? 0,
      maximumFractionDigits: column.maximum_fraction_digits ?? 4,
    }).format(Math.abs(numeric));
    return {
      text: `${numeric < 0 ? "-" : column.show_sign !== false ? "+" : ""}${formatted}%`,
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

function emptyTableMessage(dataStatus, _filters = {}) {
  if (state.projectionKind === "buyback") {
    if (state.buybackStockQuery.trim()) {
      return "没有找到匹配的股票，请检查代码或名称。";
    }
    return "当前筛选没有匹配的回购情报；如有数据问题，可在下方“数据来源与问题”查看。";
  }
  if (state.projectionKind === "market_events") {
    if (state.eventQuery.trim()) return "没有找到匹配的未来事件。";
    return "当前筛选范围没有未来事件。";
  }
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
  const scopeValue = String(scope || "");
  if (ISSUE_SCOPE_LABELS[scopeValue]) return ISSUE_SCOPE_LABELS[scopeValue];
  if (scopeValue.startsWith("nyfed-calendar:")) {
    return `纽约联储事件日历 ${scopeValue.split(":", 2)[1]}`;
  }
  if (scopeValue.startsWith("nbs-schedule:")) {
    return `国家统计局发布日程 ${scopeValue.split(":", 2)[1]}`;
  }
  if (scopeValue.startsWith("hk-csd-calendar:")) {
    return `香港政府统计处发布日程 ${scopeValue.split(":", 2)[1]}`;
  }
  if (scopeValue === "monitor") return "全部范围";
  const [direction, asset] = scopeValue.split(":", 2);
  if (!asset) return scopeValue || "当前监控";
  const directionLabel = direction === "BUY" ? "买入" : direction === "SELL" ? "卖出" : direction;
  return `${directionLabel} ${asset}`;
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
    : `${issueReasonLabel(group.reason_code)}；对应范围未展示未通过校验的值。`;
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

function tableSortContext() {
  return state.projectionKind === "altcoin_radar" && state.radarTab === "POSITION"
    ? `${state.monitorId}:position`
    : `${state.monitorId}:main`;
}

function sortTableRows(rows, columns) {
  const activeSort = state.tableSort;
  if (
    !activeSort
    || activeSort.monitorId !== state.monitorId
    || activeSort.context !== tableSortContext()
  ) return [...rows];
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
    && state.tableSort.context === tableSortContext()
    && state.tableSort.columnKey === columnKey
  ) {
    return state.tableSort.direction === "ascending" ? "descending" : "ascending";
  }
  return "ascending";
}

function buybackTablePage(totalRows) {
  const isRadarPosition = state.projectionKind === "altcoin_radar"
    && state.radarTab === "POSITION";
  if (!["buyback", "market_events"].includes(state.projectionKind) && !isRadarPosition) {
    return {
      enabled: false,
      page: 1,
      pageCount: 1,
      start: 0,
      end: totalRows,
      totalRows,
    };
  }
  const pageSize = state.projectionKind === "market_events"
    ? MARKET_EVENT_TABLE_PAGE_SIZE
    : isRadarPosition
      ? RADAR_POSITION_TABLE_PAGE_SIZE
      : BUYBACK_TABLE_PAGE_SIZE;
  const pageCount = Math.max(1, Math.ceil(totalRows / pageSize));
  state.tablePage = Math.min(Math.max(Number(state.tablePage) || 1, 1), pageCount);
  const start = (state.tablePage - 1) * pageSize;
  return {
    enabled: totalRows > pageSize,
    page: state.tablePage,
    pageCount,
    start,
    end: Math.min(start + pageSize, totalRows),
    totalRows,
  };
}

function scrollToTableStart() {
  const tableRegion = ui.quoteTableTitle.closest(".table-region");
  if (!tableRegion) return;
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  requestAnimationFrame(() => {
    tableRegion.scrollIntoView({
      behavior: reducedMotion ? "auto" : "smooth",
      block: "start",
    });
  });
}

function renderTablePagination(model, rerender) {
  ui.tablePagination.hidden = !model.enabled;
  if (!model.enabled) {
    ui.tablePageSelect.replaceChildren();
    return;
  }
  ui.tablePageSummary.textContent = `第 ${model.start + 1}–${model.end} 条，共 ${model.totalRows} 条`;
  ui.tablePageTotal.textContent = `/ ${model.pageCount} 页`;
  ui.tablePageSelect.replaceChildren();
  for (let page = 1; page <= model.pageCount; page += 1) {
    const option = document.createElement("option");
    option.value = String(page);
    option.textContent = String(page);
    option.selected = page === model.page;
    ui.tablePageSelect.append(option);
  }
  ui.tablePagePrevious.disabled = model.page === 1;
  ui.tablePageNext.disabled = model.page === model.pageCount;
  const moveToPage = (page) => {
    state.tablePage = Math.min(Math.max(page, 1), model.pageCount);
    rerender();
    scrollToTableStart();
  };
  ui.tablePagePrevious.onclick = () => moveToPage(model.page - 1);
  ui.tablePageNext.onclick = () => moveToPage(model.page + 1);
  ui.tablePageSelect.onchange = () => moveToPage(Number(ui.tablePageSelect.value));
}

function updateQuoteHorizontalScrollbar() {
  const rect = ui.quoteScroll.getBoundingClientRect();
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;
  const left = Math.max(0, rect.left);
  const right = Math.min(viewportWidth, rect.right);
  const visibleWidth = Math.max(0, right - left);
  const hasHorizontalOverflow = ui.quoteScroll.scrollWidth > ui.quoteScroll.clientWidth + 1;
  const tableContinuesBelowViewport = rect.bottom > viewportHeight + 1;
  const tableHasEnteredViewport = rect.top < viewportHeight - 18;
  const usesPageLengthTable = state.projectionKind === "buyback"
    || (
      state.projectionKind === "altcoin_radar"
      && ["TABLE", "POSITION"].includes(state.radarTab)
    );
  const shouldShow = usesPageLengthTable
    && hasHorizontalOverflow
    && visibleWidth > 0
    && tableHasEnteredViewport
    && tableContinuesBelowViewport;
  ui.quoteHorizontalScrollbar.hidden = !shouldShow;
  if (!shouldShow) return;
  ui.quoteHorizontalScrollbar.style.left = `${left}px`;
  ui.quoteHorizontalScrollbar.style.width = `${visibleWidth}px`;
  const overflowRange = ui.quoteScroll.scrollWidth - ui.quoteScroll.clientWidth;
  ui.quoteHorizontalScrollbarTrack.style.width = `${ui.quoteHorizontalScrollbar.clientWidth + overflowRange}px`;
  if (Math.abs(ui.quoteHorizontalScrollbar.scrollLeft - ui.quoteScroll.scrollLeft) > 1) {
    ui.quoteHorizontalScrollbar.scrollLeft = ui.quoteScroll.scrollLeft;
  }
}

ui.quoteScroll.addEventListener("scroll", () => {
  if (Math.abs(ui.quoteHorizontalScrollbar.scrollLeft - ui.quoteScroll.scrollLeft) > 1) {
    ui.quoteHorizontalScrollbar.scrollLeft = ui.quoteScroll.scrollLeft;
  }
});

ui.quoteScroll.addEventListener("wheel", (event) => {
  const usesPageLengthTable = state.projectionKind === "buyback"
    || (
      state.projectionKind === "altcoin_radar"
      && ["TABLE", "POSITION"].includes(state.radarTab)
    );
  if (!usesPageLengthTable) return;
  if (Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;
  const documentHeight = document.documentElement.scrollHeight;
  const canScrollUp = event.deltaY < 0 && window.scrollY > 0;
  const canScrollDown = event.deltaY > 0
    && window.scrollY + window.innerHeight < documentHeight;
  if (!canScrollUp && !canScrollDown) return;
  const multiplier = event.deltaMode === WheelEvent.DOM_DELTA_LINE
    ? 16
    : event.deltaMode === WheelEvent.DOM_DELTA_PAGE
      ? window.innerHeight
      : 1;
  event.preventDefault();
  window.scrollBy({ top: event.deltaY * multiplier, behavior: "auto" });
}, { passive: false });

ui.quoteHorizontalScrollbar.addEventListener("scroll", () => {
  if (Math.abs(ui.quoteScroll.scrollLeft - ui.quoteHorizontalScrollbar.scrollLeft) > 1) {
    ui.quoteScroll.scrollLeft = ui.quoteHorizontalScrollbar.scrollLeft;
  }
});

function marketDestination(row) {
  if (state.monitorId !== RADAR_MONITOR_ID) return null;
  const symbol = String(row.symbol || "").toUpperCase();
  if (!/^[\p{L}\p{N}]{1,56}USDT$/u.test(symbol)) return null;
  return {
    url: `https://www.binance.com/zh-CN/futures/${encodeURIComponent(symbol)}`,
    label: "Binance USDⓈ-M 永续合约行情",
  };
}

function buybackMarketDestination(row) {
  const code = String(row.stock_code || "");
  if (row.market_scope === "HK" && /^\d{5}$/.test(code)) {
    const tradingViewCode = code.replace(/^0+(?=\d)/, "");
    return {
      url: `https://cn.tradingview.com/chart/?symbol=${encodeURIComponent(`HKEX:${tradingViewCode}`)}`,
      label: "TradingView 港股专业图表",
    };
  }
  if (row.market_scope === "A_SHARE" && /^\d{6}$/.test(code)) {
    const exchange = row.market === "SH" ? "SSE" : row.market === "SZ" ? "SZSE" : null;
    if (exchange) {
      return {
        url: `https://cn.tradingview.com/chart/?symbol=${encodeURIComponent(`${exchange}:${code}`)}`,
        label: "TradingView A股专业图表",
      };
    }
  }
  return null;
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
      && state.tableSort.context === tableSortContext()
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
      button.setAttribute("aria-description", column.description);
      const help = createElement("span", "table-column-help", "ⓘ");
      help.setAttribute("aria-hidden", "true");
      const tooltip = createElement(
        "span",
        "table-column-help-tooltip",
        column.description,
      );
      tooltip.setAttribute("role", "tooltip");
      help.append(tooltip);
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
        context: tableSortContext(),
        columnKey: column.key,
        direction: nextSortDirection(column.key),
      };
      state.tablePage = 1;
      renderTable(columns, rows, selectedSeries, issues, filters, dataStatus);
    });
    th.append(button);
    ui.quoteHead.append(th);
  });
  ui.quoteBody.replaceChildren();
  const sortedRows = sortTableRows(rows, columns);
  const page = buybackTablePage(sortedRows.length);
  sortedRows.slice(page.start, page.end).forEach((row) => {
    const tr = document.createElement("tr");
    tr.tabIndex = 0;
    if (row.row_tone) tr.dataset.tone = String(row.row_tone);
    tr.setAttribute("aria-selected", String(row.series_key === selectedSeries));
    const destination = marketDestination(row);
    let marketAnchor = null;
    const select = async () => {
      if (state.projectionKind === "buyback" && row.entity_key) {
        await openBuybackDetail(String(row.entity_key));
        return;
      }
      if (state.projectionKind === "market_events") {
        openMarketEventDetail(row);
        return;
      }
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
      if (state.projectionKind === "market_events" && column.key === "priority_rank") {
        td.classList.add("market-event-priority-cell");
        const badge = createElement("span", "event-priority-badge", row.priority_label || "日历关注");
        badge.dataset.priority = String(row.priority_rank || 4);
        badge.title = row.priority_reason || "";
        td.append(badge);
      } else if (state.projectionKind === "market_events" && column.key === "scheduled_sort_at") {
        td.classList.add("market-event-time-cell");
        const timing = createElement("div", "market-event-time");
        timing.append(
          createElement("strong", "", row.schedule_label || "时间待公布"),
          createElement("span", "", row.countdown_label || ""),
        );
        if (row.schedule_changed_recently) {
          timing.append(createElement("span", "market-event-change", "时间有调整"));
        }
        timing.title = row.time_precision === "DATE"
          ? `官方目前只公布了${row.source_timezone_label || "来源所在地日期"}，具体发布时间待公布。`
          : "已换算为北京时间。";
        td.append(timing);
      } else if (state.projectionKind === "market_events" && column.key === "event_title") {
        const event = createElement("div", "market-event-title-cell");
        event.append(
          createElement("strong", "", row.event_title || "关键事件"),
          createElement("span", "", `${row.category_label || ""} · ${row.source_label || ""}`),
        );
        td.append(event);
      } else if (state.projectionKind === "market_events" && column.key === "importance_rank") {
        td.classList.add("market-event-importance-cell");
        const badge = createElement("span", "market-event-importance", row.importance_label || "中");
        badge.dataset.importance = row.importance || "MEDIUM";
        badge.title = row.impact_reason || "";
        td.append(badge);
      } else if (state.projectionKind === "market_events" && column.key === "markets_label") {
        const markets = createElement("div", "market-event-markets");
        String(row.markets_label || "").split("、").filter(Boolean).forEach((label) => {
          markets.append(createElement("span", "", label));
        });
        td.append(markets);
      } else if (state.projectionKind === "market_events" && column.key === "expectation_summary") {
        if (row.expectation_summary) {
          const result = createElement("div", "market-event-result");
          result.append(
            createElement("strong", "", row.expectation_summary),
            createElement("span", "", row.consensus_observed_at
              ? `预期记录 ${formatTime(row.consensus_observed_at)}`
              : ""),
          );
          td.append(result);
        } else {
          td.textContent = "—";
          td.dataset.missing = "true";
          td.title = row.expected_period
            ? "尚未取得与官方发布时间精确匹配的市场一致预期。"
            : "该事件当前不使用量化一致预期。";
        }
      } else if (state.projectionKind === "market_events" && column.key === "release_summary") {
        if (row.release_summary) {
          const result = createElement("div", "market-event-result");
          result.append(createElement("strong", "", row.release_summary));
          if (row.direction) {
            const badge = createElement("span", "event-direction-badge", row.direction.label || "中性");
            badge.dataset.tone = row.direction.tone || "NEUTRAL";
            badge.title = `方向分 ${row.direction.score} · ${row.direction.threshold || ""}`;
            result.append(badge);
          }
          td.append(result);
        } else {
          td.textContent = row.release_state_label || "等待公布";
          td.classList.add("event-history-state");
        }
      } else if (state.projectionKind === "buyback" && column.key === "attention_label") {
        const badge = createElement("span", "buyback-attention-badge", row.attention_label || "状态更新");
        badge.dataset.level = row.attention_level || "UPDATE";
        td.append(badge);
      } else if (state.projectionKind === "buyback" && column.key === "security_label") {
        const security = createElement("div", "buyback-security");
        const quoteDestination = buybackMarketDestination(row);
        const content = quoteDestination
          ? createElement("a", "buyback-security-link")
          : createElement("span", "buyback-security-link");
        if (quoteDestination) {
          content.href = quoteDestination.url;
          content.target = "_blank";
          content.rel = "noopener noreferrer";
          content.title = `${quoteDestination.label}（新标签页）`;
          content.setAttribute(
            "aria-label",
            `${row.stock_code || ""} ${row.issuer_name || ""}，${row.connect_status_label || ""}，打开${quoteDestination.label}，新标签页`,
          );
        }
        const nameLine = createElement("span", "buyback-security-name");
        const eligibility = createElement("span", "buyback-eligibility-dot");
        eligibility.dataset.status = row.connect_status || "";
        eligibility.title = [row.connect_status_label, row.connect_route_label]
          .filter((value) => value && value !== "—")
          .join(" · ");
        eligibility.setAttribute("aria-hidden", "true");
        nameLine.append(eligibility, createElement("span", "", row.issuer_name || "未知公司"));
        content.append(
          createElement("strong", "", row.stock_code || "未知代码"),
          nameLine,
        );
        security.append(content);
        if (quoteDestination) {
          const externalIcon = createElement("span", "buyback-market-link-icon", "↗");
          externalIcon.setAttribute("aria-hidden", "true");
          security.append(externalIcon);
        }
        td.append(security);
      } else if (state.projectionKind === "buyback" && column.key === "attractiveness_score") {
        const metric = createElement("div", "buyback-score");
        metric.dataset.level = row.attractiveness_level || "INSUFFICIENT";
        const score = Number(row.attractiveness_score);
        const hasScore = row.attractiveness_score !== null
          && row.attractiveness_score !== undefined
          && Number.isFinite(score);
        const head = createElement("span", "buyback-score-head");
        if (hasScore) {
          head.append(
            createElement("strong", "", score.toFixed(1)),
            createElement("span", "buyback-score-label", row.attractiveness_label || ""),
          );
        } else {
          head.append(createElement("strong", "buyback-score-unavailable", row.attractiveness_label || "数据不足"));
          td.dataset.missing = "true";
        }
        metric.append(head);
        metric.title = [
          row.attractiveness_summary,
          row.attractiveness_explanation,
          row.missing_reasons?.attractiveness_score,
        ].filter(Boolean).join("。 ");
        td.append(metric);
      } else if (state.projectionKind === "buyback" && column.key === "execution_days_value") {
        const metric = createElement("div", "buyback-derived-metric");
        if (row.execution_days_label) {
          metric.append(createElement("strong", "", row.execution_days_label));
          if (row.execution_days_scope === "LOWER_BOUND") {
            metric.append(createElement("span", "buyback-partial-label", "历史未完整"));
            metric.title = row.missing_reasons?.cumulative_amount || "当前只显示已覆盖历史中的最低天数。";
          }
        } else {
          metric.append(createElement("strong", "buyback-score-unavailable", "—"));
          td.dataset.missing = "true";
          metric.title = row.missing_reasons?.execution_days_value || "无法计算实际执行天数。";
        }
        td.append(metric);
      } else if (state.projectionKind === "buyback" && column.key === "cumulative_shares") {
        const metric = createElement("div", "buyback-derived-metric");
        if (row.cumulative_shares_label) {
          metric.append(createElement("strong", "", row.cumulative_shares_label));
        } else {
          metric.append(createElement("strong", "buyback-score-unavailable", "—"));
          td.dataset.missing = "true";
          metric.title = row.missing_reasons?.cumulative_shares || "没有可计算的累计回购股数。";
        }
        td.append(metric);
      } else if (state.projectionKind === "buyback" && column.key === "cumulative_amount") {
        const metric = createElement("div", "buyback-derived-metric");
        if (row.cumulative_amount_label) {
          metric.append(createElement("strong", "", row.cumulative_amount_label));
        } else if (row.recent_amount_label) {
          metric.append(
            createElement("strong", "", row.recent_amount_label),
            createElement("span", "buyback-partial-label", "近7日"),
          );
          td.dataset.partial = "true";
          metric.title = row.missing_reasons?.cumulative_amount || "当前仅有近7日金额。";
        } else {
          metric.append(createElement("strong", "buyback-score-unavailable", "—"));
          td.dataset.missing = "true";
          metric.title = row.missing_reasons?.cumulative_amount || "没有可计算的累计回购金额。";
        }
        td.append(metric);
      } else if (state.projectionKind === "buyback" && column.key === "average_cost") {
        const metric = createElement("div", "buyback-derived-metric");
        if (row.average_cost_label) {
          metric.append(createElement("strong", "", row.average_cost_label));
          metric.title = row.average_cost_scope_label || "";
        } else if (row.recent_average_cost_label) {
          metric.append(
            createElement("strong", "", row.recent_average_cost_label),
            createElement("span", "buyback-partial-label", "近7日"),
          );
          td.dataset.partial = "true";
          metric.title = row.missing_reasons?.average_cost || "当前仅有近7日加权均价。";
        } else {
          metric.append(createElement("strong", "buyback-score-unavailable", "—"));
          td.dataset.missing = "true";
          metric.title = row.missing_reasons?.average_cost || "无法估算回购均价。";
        }
        td.append(metric);
      } else if (state.projectionKind === "buyback" && column.key === "current_price") {
        const metric = createElement("div", "buyback-derived-metric");
        if (row.current_price_label) {
          metric.append(createElement("strong", "", row.current_price_label));
        } else {
          metric.append(createElement("strong", "buyback-score-unavailable", "—"));
          td.dataset.missing = "true";
          metric.title = row.missing_reasons?.current_price || "当前没有可用行情价格。";
        }
        td.append(metric);
      } else if (state.projectionKind === "buyback" && column.key === "price_vs_average_percent") {
        const metric = createElement("div", "buyback-derived-metric");
        const fullDifference = Number(row.price_vs_average_percent);
        const recentDifference = Number(row.recent_price_vs_average_percent);
        if (row.price_vs_average_percent !== null && Number.isFinite(fullDifference)) {
          const value = `${fullDifference >= 0 ? "+" : ""}${fullDifference.toFixed(2)}%`;
          const result = createElement("strong", "buyback-directional-value", value);
          result.dataset.direction = fullDifference > 0 ? "UP" : fullDifference < 0 ? "DOWN" : "FLAT";
          metric.append(result);
        } else if (row.recent_price_vs_average_percent !== null && Number.isFinite(recentDifference)) {
          const value = `${recentDifference >= 0 ? "+" : ""}${recentDifference.toFixed(2)}%`;
          const result = createElement("strong", "buyback-directional-value", value);
          result.dataset.direction = recentDifference > 0 ? "UP" : recentDifference < 0 ? "DOWN" : "FLAT";
          metric.append(result, createElement("span", "buyback-partial-label", "近7日"));
          td.dataset.partial = "true";
          metric.title = "现价相对近7日回购均价；本轮完整均价尚未形成。";
        } else {
          metric.append(createElement("strong", "buyback-score-unavailable", "—"));
          td.dataset.missing = "true";
          metric.title = row.missing_reasons?.price_vs_average_percent || "无法比较现价与回购均价。";
        }
        td.append(metric);
      } else if (state.projectionKind === "buyback" && column.key === "intelligence_summary") {
        const summary = createElement("div", "buyback-intelligence-summary");
        summary.append(createElement("strong", "", row.intelligence_headline || row.event_type_label || "回购事件"));
        const detail = createElement("span", "", row.intelligence_summary || "官方回购事实");
        detail.title = row.intelligence_summary || "官方回购事实";
        summary.append(detail);
        td.append(summary);
      } else if (state.projectionKind === "buyback" && column.key === "scale_label") {
        const scale = createElement("span", "buyback-scale-label", row.scale_label || "规模未结构化");
        scale.dataset.status = row.scale_status || "MISSING";
        scale.title = row.scale_reason || "";
        td.append(scale);
      } else if (
        state.projectionKind === "altcoin_radar"
        && column.key === "price_state_label"
      ) {
        const badge = createElement(
          "span",
          "radar-price-state",
          row.price_state_label || "区间中部",
        );
        badge.dataset.state = row.price_state || "MID_RANGE";
        badge.title = row.state_reason || row.context_stage_reason || "";
        td.append(badge);
      } else if (
        state.projectionKind === "altcoin_radar"
        && column.key === "context_stage_label"
      ) {
        const badge = createElement(
          "span",
          "radar-context-stage",
          row.context_stage_label || "等待位置确认",
        );
        badge.dataset.group = row.context_stage_group || "WATCHING";
        badge.title = row.context_stage_reason || "";
        td.append(badge);
      } else if (rendered.missing) {
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
        if (
          state.projectionKind === "altcoin_radar"
          && state.radarTab === "POSITION"
          && column.kind === "percent"
          && column.key.startsWith("return_")
          && Number.isFinite(Number(row[column.key]))
        ) {
          const value = createElement("span", "radar-price-change", rendered.text);
          const numeric = Number(row[column.key]);
          value.dataset.direction = numeric > 0 ? "UP" : numeric < 0 ? "DOWN" : "FLAT";
          td.append(value);
        } else {
          td.textContent = rendered.text;
        }
        if (
          state.projectionKind === "buyback"
          && column.kind === "percent"
          && column.key !== "actual_amount_yield_percent"
          && Number.isFinite(Number(row[column.key]))
        ) {
          const numeric = Number(row[column.key]);
          td.classList.add("buyback-directional-cell");
          td.dataset.direction = numeric > 0 ? "UP" : numeric < 0 ? "DOWN" : "FLAT";
        }
      }
      tr.append(td);
    });
    if (destination && marketAnchor) {
      tr.classList.add("market-row");
      tr.title = `${destination.label}（新标签页）`;
      tr.setAttribute("aria-label", `${row.symbol}：${destination.label}，新标签页`);
    }
    if (state.projectionKind === "buyback") {
      tr.classList.add("buyback-row");
      tr.setAttribute(
        "aria-label",
        `${row.stock_code || ""} ${row.issuer_name || ""}，${row.intelligence_headline || "回购记录"}，按回车查看情报详情与官方证据`,
      );
    }
    if (state.projectionKind === "market_events") {
      tr.classList.add("market-event-row");
      tr.setAttribute(
        "aria-label",
        `${row.event_title || "关键事件"}，${row.schedule_label || "时间待公布"}，按回车查看事件详情与官方来源`,
      );
    }
    tr.addEventListener("click", (event) => {
      if (event.target.closest("a, button")) return;
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
  const issueGroups = state.projectionKind === "market_events"
    ? []
    : tableIssueGroups(issues || [], filters, rows);
  issueGroups.forEach((group) => appendTableIssueRow(columns, group));
  ui.quoteEmpty.hidden = rows.length > 0 || issueGroups.length > 0;
  ui.quoteEmpty.textContent = emptyTableMessage(dataStatus, filters);
  renderTablePagination(
    page,
    () => renderTable(columns, rows, selectedSeries, issues, filters, dataStatus),
  );
}

const BUYBACK_EVENT_OPTIONS = [
  ["PLAN_OR_APPROVAL", "方案 / 审议"],
  ["FIRST_EXECUTION", "首次实施"],
  ["PROGRESS", "实施进展"],
  ["MODIFICATION", "方案变更"],
  ["COMPLETION_OR_TERMINATION", "完成 / 终止"],
  ["POST_BUYBACK_CANCELLATION", "注销"],
  ["POST_BUYBACK_DISPOSAL", "出售已回购股份"],
  ["AMBIGUOUS_BUYBACK", "待确认回购事件"],
  ["HKEX_EXECUTION", "港股实际回购"],
];

function appendBuybackFact(label, value) {
  const group = document.createElement("div");
  group.append(createElement("dt", "", label));
  group.append(createElement("dd", value ? "" : "missing-value", value || "—"));
  ui.buybackFacts.append(group);
}

function safeExternalUrl(value) {
  try {
    const parsed = new URL(String(value));
    return parsed.protocol === "https:" ? parsed.href : null;
  } catch (_error) {
    return null;
  }
}

function evidenceLink(label, href, { external = false } = {}) {
  const anchor = createElement("a", "buyback-evidence-link", label);
  anchor.href = href;
  anchor.target = "_blank";
  anchor.rel = external ? "noopener noreferrer" : "noopener";
  return anchor;
}

function appendMarketEventFact(label, value, { warning = false } = {}) {
  const group = document.createElement("div");
  if (warning) group.dataset.warning = "true";
  group.append(
    createElement("dt", "", label),
    createElement("dd", value ? "" : "missing-value", value || "—"),
  );
  ui.marketEventDetailFacts.append(group);
}

function openMarketEventDetail(event) {
  ui.marketEventDetailFacts.replaceChildren();
  ui.marketEventSourceLinks.replaceChildren();
  ui.marketEventDetailTitle.textContent = event.event_title || "关键事件";
  ui.marketEventDetailSubtitle.textContent = [
    marketEventScheduleText(event),
    event.countdown_label,
  ].filter(Boolean).join(" · ");
  appendMarketEventFact("准备优先级", event.priority_label);
  appendMarketEventFact("影响级别", event.importance_label);
  appendMarketEventFact("事件类别", event.category_label);
  appendMarketEventFact("需考虑市场", event.markets_label);
  appendMarketEventFact("发布时间", marketEventScheduleText(event));
  if (event.expected_period) appendMarketEventFact("数据所属期", event.expected_period);
  if (event.expected_period || (event.expectations || []).length) {
    appendMarketEventFact(
      "市场预期",
      event.expectation_summary || "暂未取得匹配的一致预期",
      { warning: !event.expectation_summary },
    );
  }
  if (event.release_state !== "SCHEDULED") {
    appendMarketEventFact(
      "官方公布",
      event.actual_summary || event.release_state_label,
      { warning: event.release_state === "AWAITING_OFFICIAL" },
    );
  }
  if (event.surprise_summary) appendMarketEventFact("预期差（实际-预期）", event.surprise_summary);
  if (event.direction) {
    appendMarketEventFact(
      event.direction.scope || "风险资产短线",
      `${event.direction.label} · 方向分 ${Number(event.direction.score).toFixed(2)}`,
    );
  }
  if (event.schedule_changed_recently || Number(event.schedule_change_count || 0) > 0) {
    appendMarketEventFact(
      "时间变化",
      event.previous_schedule_label
        ? `此前 ${event.previous_schedule_label}`
        : `累计调整 ${event.schedule_change_count} 次`,
      { warning: true },
    );
  }
  ui.marketEventImpactReason.textContent = [
    event.priority_reason,
    event.impact_reason,
  ].filter(Boolean).join(" ");
  ui.marketEventDescription.textContent = event.event_description || "—";
  ui.marketEventHowToRead.textContent = event.interpretation?.how_to_read || "—";
  ui.marketEventDecisionRule.textContent = event.interpretation?.decision_rule || "";

  const directionMethod = event.direction || event.direction_method;
  ui.marketEventDirectionSection.hidden = !directionMethod;
  ui.marketEventDirectionInputs.replaceChildren();
  if (directionMethod) {
    const hasDirection = Boolean(event.direction);
    ui.marketEventDirectionLabel.textContent = hasDirection
      ? `${event.direction.scope || "风险资产短线"} ${event.direction.label} · ${Number(event.direction.score).toFixed(2)}`
      : "公布后自动计算";
    ui.marketEventDirectionLabel.dataset.tone = event.direction?.tone || "NEUTRAL";
    ui.marketEventDirectionAction.textContent = hasDirection
      ? event.direction.action || ""
      : "实际值与发布前一致预期均完整后，系统按下列固定公式自动给出偏多、偏空或中性判断。";
    ui.marketEventDirectionFormula.textContent = [
      directionMethod.formula,
      directionMethod.threshold,
    ].filter(Boolean).join("；");
    const inputs = event.direction?.inputs || directionMethod.required_inputs || [];
    inputs.forEach((input) => {
      ui.marketEventDirectionInputs.append(createElement(
        "span",
        "",
        input.display ? `${input.label} ${input.display}` : String(input),
      ));
    });
  }

  const links = [
    ["查看事件发布页", safeExternalUrl(event.official_release_url)],
    ["查看官方日历", safeExternalUrl(event.schedule_source_url)],
    ["查看市场一致预期来源", safeExternalUrl(event.consensus_source_url)],
    ["查看官方实值来源", safeExternalUrl(event.actual_source_url)],
  ];
  const seen = new Set();
  links.forEach(([label, href]) => {
    if (!href || seen.has(href)) return;
    seen.add(href);
    const anchor = createElement("a", "market-event-source-link", label);
    anchor.href = href;
    anchor.target = "_blank";
    anchor.rel = "noopener noreferrer";
    ui.marketEventSourceLinks.append(anchor);
  });
  if (!ui.marketEventDetailDialog.open) ui.marketEventDetailDialog.showModal();
}

ui.marketEventDetailClose.addEventListener("click", () => {
  ui.marketEventDetailDialog.close();
});

ui.marketEventDetailDialog.addEventListener("click", (event) => {
  if (event.target === ui.marketEventDetailDialog) {
    ui.marketEventDetailDialog.close();
  }
});

async function openBuybackDetail(entityKey) {
  state.buybackDetailEntityKey = entityKey;
  state.buybackDetailRevisionNo = null;
  ui.buybackDetailTitle.textContent = "正在读取证据";
  ui.buybackDetailSubtitle.textContent = "";
  ui.buybackDetailStatus.textContent = "正在加载回购事实、官方证据与校正记录…";
  ui.buybackDetailStatus.dataset.kind = "loading";
  ui.buybackDetailStatus.hidden = false;
  ui.buybackDetailContent.hidden = true;
  if (!ui.buybackDetailDialog.open) ui.buybackDetailDialog.showModal();
  try {
    const response = await fetch(
      `/api/buybacks/entities/${encodeURIComponent(entityKey)}`,
      { headers: { Accept: "application/json" }, cache: "no-store" },
    );
    if (!response.ok) throw new Error(`HTTP_${response.status}`);
    const payload = await response.json();
    renderBuybackDetail(payload);
  } catch (error) {
    ui.buybackDetailStatus.textContent = `详情读取失败 · ${error.message}`;
    ui.buybackDetailStatus.dataset.kind = "error";
    ui.buybackDetailStatus.hidden = false;
  }
}

function renderBuybackDetail(payload) {
  const entity = payload.entity;
  const documentValue = payload.document;
  const latestReview = payload.reviews.find(
    (review) => Number(review.base_revision_no) === Number(entity.revision_no),
  ) || null;
  state.buybackDetailRevisionNo = entity.revision_no;
  ui.buybackDetailTitle.textContent = `${entity.stock_code || "未知代码"} · ${entity.issuer_name || "未知公司"}`;
  ui.buybackDetailSubtitle.textContent = `${entity.event_type_label || entity.event_type} · ${formatDate(entity.effective_date || entity.effective_at)}`;
  ui.buybackDetailStatus.textContent = "";
  ui.buybackDetailStatus.dataset.kind = "";
  ui.buybackDetailStatus.hidden = true;
  ui.buybackDetailContent.hidden = false;

  ui.buybackFacts.replaceChildren();
  const attractivenessScore = Number(entity.attractiveness_score);
  const hasAttractivenessScore = entity.attractiveness_score !== null
    && entity.attractiveness_score !== undefined
    && Number.isFinite(attractivenessScore);
  appendBuybackFact(
    "回购吸引力",
    hasAttractivenessScore
      ? `${attractivenessScore.toFixed(1)} · ${entity.attractiveness_label} · 输入覆盖 ${Number(entity.attractiveness_coverage_percent || 0).toFixed(1)}%`
      : entity.attractiveness_label,
  );
  appendBuybackFact("实际执行天数", entity.execution_days_label);
  appendBuybackFact("累计股数", entity.cumulative_shares_label);
  appendBuybackFact(
    "累计金额",
    entity.cumulative_amount_label
      || (entity.recent_amount_label ? `${entity.recent_amount_label} · 仅近7日` : null),
  );
  appendBuybackFact(
    "回购均价",
    entity.average_cost_label
      || (entity.recent_average_cost_label ? `${entity.recent_average_cost_label} · 仅近7日` : null),
  );
  appendBuybackFact(
    "现价",
    entity.current_price_label,
  );
  const priceDifference = entity.price_vs_average_percent ?? entity.recent_price_vs_average_percent;
  const priceDifferenceNumber = Number(priceDifference);
  appendBuybackFact(
    "现价/回购均价",
    priceDifference != null && Number.isFinite(priceDifferenceNumber)
      ? `${priceDifferenceNumber >= 0 ? "+" : ""}${priceDifferenceNumber.toFixed(2)}%${entity.price_vs_average_percent == null ? " · 近7日口径" : ""}`
      : null,
  );
  const dailyChange = Number(entity.daily_change_percent);
  appendBuybackFact(
    "涨跌幅",
    entity.daily_change_percent != null && Number.isFinite(dailyChange)
      ? `${dailyChange >= 0 ? "+" : ""}${dailyChange.toFixed(2)}%`
      : null,
  );
  const marketCapYield = Number(entity.actual_amount_yield_percent);
  appendBuybackFact(
    "回购金额/市值",
    entity.actual_amount_yield_percent != null && Number.isFinite(marketCapYield)
      ? `${marketCapYield.toFixed(2)}%`
      : null,
  );
  [
    ["年度ROE", entity.roe_percent],
    ["营收同比", entity.revenue_yoy_percent],
    ["净利同比", entity.net_profit_yoy_percent],
  ].forEach(([label, value]) => {
    const numeric = Number(value);
    appendBuybackFact(
      label,
      value != null && Number.isFinite(numeric)
        ? `${numeric > 0 ? "+" : ""}${numeric.toFixed(1)}%`
        : null,
      );
    });
  const financialPeriods = [
    entity.roe_report_date ? `年度ROE ${formatDate(entity.roe_report_date)}` : null,
    entity.financial_report_date ? `增长数据 ${formatDate(entity.financial_report_date)}` : null,
  ].filter(Boolean);
  if (financialPeriods.length) appendBuybackFact("业绩报告期", financialPeriods.join(" · "));
  (entity.attractiveness_components || [])
    .filter((component) => component.available)
    .forEach((component) => {
      appendBuybackFact(
        `${component.name} · 权重${Number(component.weight).toFixed(0)}%`,
        component.detail,
      );
    });
  appendBuybackFact("情报", entity.intelligence_headline || entity.event_type_label || entity.event_type);
  appendBuybackFact("事实摘要", entity.intelligence_summary);
  appendBuybackFact("市场", entity.market_label);
  appendBuybackFact("日期", formatDate(entity.effective_date || entity.effective_at));
  appendBuybackFact("官方发布时间", formatTime(entity.official_release_at));
  const connectRoute = entity.connect_route_label && entity.connect_route_label !== "—"
    ? ` · ${entity.connect_route_label}`
    : "";
  appendBuybackFact("购买资格", `${entity.connect_status_label || "—"}${connectRoute}`);
  if (entity.program_key) appendBuybackFact("回购方案", entity.program_key);

  ui.buybackEvidenceLinks.replaceChildren();
  const marketQuote = buybackMarketDestination(entity);
  if (marketQuote) {
    ui.buybackEvidenceLinks.append(
      evidenceLink("查看 TradingView 专业图表 ↗", marketQuote.url, { external: true }),
    );
  }
  const official = safeExternalUrl(
    documentValue?.source_url || entity.document_url || entity.source_url,
  );
  if (official) {
    ui.buybackEvidenceLinks.append(evidenceLink("打开官方原文 ↗", official, { external: true }));
  }
  if (documentValue?.local_url && /^\/api\/buybacks\/documents\/[0-9a-f]{64}$/.test(documentValue.local_url)) {
    ui.buybackEvidenceLinks.append(evidenceLink("打开本机证据副本", documentValue.local_url));
  }
  ui.buybackEvidenceMeta.textContent = documentValue
    ? `${documentValue.source_label} · 取得 ${formatTime(documentValue.observed_at)} · 本机证据副本 ${new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 1 }).format(Number(documentValue.size_bytes) / 1024)} KB`
    : "尚未取得通过校验的原始文件；当前仅保留公告索引事实。";
  const excerpt = entity.evidence_excerpt || documentValue?.metadata?.evidence_excerpt || "";
  ui.buybackEvidenceExcerpt.textContent = excerpt;
  ui.buybackEvidenceExcerpt.hidden = !excerpt;

  ui.buybackReviewDecision.value = latestReview?.decision || "";
  ui.buybackReviewEventType.replaceChildren();
  BUYBACK_EVENT_OPTIONS.forEach(([value, label]) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    option.selected = value === entity.event_type;
    ui.buybackReviewEventType.append(option);
  });
  ui.buybackReviewProgramKey.value = entity.program_key || "";
  ui.buybackReviewProgramStatus.value = entity.program_status || "";
  ui.buybackReviewNote.value = "";
  ui.buybackReviewSubmit.disabled = false;
  ui.buybackReviewSubmit.textContent = "提交校正";

  ui.buybackReviewHistory.replaceChildren();
  if (!payload.reviews.length) {
    ui.buybackReviewHistory.append(createElement("p", "empty-state", "尚无人工校正记录。"));
  } else {
    payload.reviews.forEach((review) => {
      const item = createElement("article", "buyback-review-record");
      item.append(createElement("strong", "", `${review.decision_label} · ${formatTime(review.created_at)}`));
      const details = [
        review.corrected_event_type,
        review.program_key,
        review.program_status,
      ].filter(Boolean).join(" · ");
      if (details) item.append(createElement("p", "", details));
      if (review.note) item.append(createElement("p", "", review.note));
      ui.buybackReviewHistory.append(item);
    });
  }
}

ui.buybackDetailClose.addEventListener("click", () => {
  ui.buybackDetailDialog.close();
});

ui.buybackReviewForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (
    state.buybackReviewSubmitting
    || !state.buybackDetailEntityKey
    || !state.buybackDetailRevisionNo
    || !ui.buybackReviewForm.reportValidity()
  ) return;
  state.buybackReviewSubmitting = true;
  ui.buybackReviewSubmit.disabled = true;
  ui.buybackReviewSubmit.textContent = "正在保存…";
  ui.buybackDetailStatus.textContent = "正在追加人工校正记录…";
  ui.buybackDetailStatus.dataset.kind = "loading";
  ui.buybackDetailStatus.hidden = false;
  const entityKey = state.buybackDetailEntityKey;
  try {
    const response = await fetch(
      `/api/buybacks/entities/${encodeURIComponent(entityKey)}/reviews`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          base_revision_no: state.buybackDetailRevisionNo,
          decision: ui.buybackReviewDecision.value,
          corrected_event_type: ui.buybackReviewEventType.value || null,
          program_key: ui.buybackReviewProgramKey.value.trim() || null,
          program_status: ui.buybackReviewProgramStatus.value || null,
          note: ui.buybackReviewNote.value.trim(),
        }),
      },
    );
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || `HTTP_${response.status}`);
    await loadView();
    await openBuybackDetail(entityKey);
    ui.buybackDetailStatus.textContent = "人工校正已追加保存。";
    ui.buybackDetailStatus.dataset.kind = "success";
    ui.buybackDetailStatus.hidden = false;
  } catch (error) {
    ui.buybackDetailStatus.textContent = error.message === "BUYBACK_REVISION_CONFLICT"
      ? "保存前事实 revision 已变化，请关闭后重新查看最新情报。"
      : `人工校正保存失败 · ${error.message}`;
    ui.buybackDetailStatus.dataset.kind = "error";
    ui.buybackDetailStatus.hidden = false;
  } finally {
    state.buybackReviewSubmitting = false;
    ui.buybackReviewSubmit.disabled = false;
    ui.buybackReviewSubmit.textContent = "提交校正";
  }
});

function evaluationHorizonLabel(minutes) {
  if (Number(minutes) === 60) return "1小时";
  if (Number(minutes) === 240) return "4小时";
  return `${Number(minutes)}分钟`;
}

function evaluationPercent(value) {
  const numeric = Number(value);
  if (value === null || value === undefined || !Number.isFinite(numeric)) return "—";
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(4)}%`;
}

function evaluationMaturityText(maturity) {
  if (!maturity) return "样本继续积累";
  const coverage = `${maturity.sample_count}/${maturity.minimum_sample_count}例 · ${maturity.distinct_cutoff_count}/${maturity.minimum_distinct_cutoffs}截止点 · ${maturity.distinct_entity_count}/${maturity.minimum_distinct_entities}币种 · ${Number(maturity.observation_days).toFixed(1)}/${Number(maturity.minimum_observation_days).toFixed(0)}天`;
  return maturity.ready ? `已达门槛 · ${coverage}` : coverage;
}

function evaluationComparisonRate(value, maturity) {
  if (value === null || value === undefined) {
    return maturity?.ready ? "无结果" : "暂不显示";
  }
  return `${Number(value).toFixed(1)}%`;
}

function appendEvaluationCell(row, text, { numeric = false } = {}) {
  const cell = createElement("td", numeric ? "evaluation-number" : "", text);
  row.append(cell);
}

function renderEvaluation(evaluation) {
  if (!evaluation) {
    ui.evaluationRegion.hidden = true;
    ui.evaluationOverview.replaceChildren();
    ui.evaluationComparison.hidden = true;
    ui.evaluationComparisonOverview.replaceChildren();
    ui.evaluationComparisonBody.replaceChildren();
    ui.evaluationGroupBody.replaceChildren();
    ui.evaluationRecentBody.replaceChildren();
    return;
  }
  ui.evaluationRegion.hidden = false;
  ui.evaluationTitle.textContent = evaluation.title;
  ui.evaluationMethodNote.textContent = evaluation.method_note;

  const overview = evaluation.overview;
  const maturity = evaluation.maturity;
  const overviewItems = [
    ["已固定期限样本", overview.total_cases],
    ["已到期", overview.due_cases],
    ["已完成", overview.completed_cases],
    ["到期覆盖", overview.coverage_percent === null ? "等待首批到期" : `${Number(overview.coverage_percent).toFixed(1)}%`],
    ["等待到期", overview.pending_future_cases],
    ["待补采 / 无法检验", `${overview.pending_due_cases} / ${overview.unavailable_cases}`],
    ["独立信号截止", maturity.distinct_cutoff_count],
    ["覆盖币种", maturity.distinct_entity_count],
    ["观测跨度", `${Number(maturity.observation_days).toFixed(1)} 天`],
    ["率值状态", maturity.status_label],
  ];
  ui.evaluationOverview.replaceChildren();
  overviewItems.forEach(([label, value]) => {
    const group = document.createElement("div");
    group.append(createElement("dt", "", label));
    group.append(createElement("dd", "", String(value)));
    ui.evaluationOverview.append(group);
  });

  const comparison = evaluation.comparison;
  ui.evaluationComparison.hidden = !comparison;
  ui.evaluationComparisonOverview.replaceChildren();
  ui.evaluationComparisonBody.replaceChildren();
  if (comparison) {
    const flipRelation = comparison.relations.find((item) => item.direction_relation === "DIRECTION_FLIP");
    const sameRelation = comparison.relations.find((item) => item.direction_relation === "SAME_DIRECTION");
    const comparisonItems = [
      ["同批已建", comparison.paired_case_count],
      ["同批完成", comparison.sample_count],
      ["方向同向（已建 / 完成）", `${sameRelation?.paired_case_count || 0} / ${sameRelation?.sample_count || 0}`],
      ["方向翻转（已建 / 完成）", `${flipRelation?.paired_case_count || 0} / ${flipRelation?.sample_count || 0}`],
      ["等待完成", comparison.pending_pair_count],
      ["无法比较", comparison.unavailable_pair_count],
    ];
    comparisonItems.forEach(([label, value]) => {
      const group = document.createElement("div");
      group.append(createElement("dt", "", label));
      group.append(createElement("dd", "", String(value)));
      ui.evaluationComparisonOverview.append(group);
    });
    ui.evaluationComparisonPeriod.textContent = comparison.first_cutoff_at
      ? `增量只看“方向翻转”样本；同方向样本仅检查筛选表现。完成结果从 ${formatTime(comparison.first_cutoff_at)} 覆盖至 ${formatTime(comparison.last_outcome_at)}。`
      : "增量只看价格位置使原规则方向发生翻转的同批样本；同方向样本不重复计算为规则增益。";
    comparison.groups.forEach((group) => {
      const row = document.createElement("tr");
      row.className = "evaluation-row";
      row.dataset.relation = group.direction_relation;
      appendEvaluationCell(row, group.relation_label);
      appendEvaluationCell(row, group.stage_label);
      appendEvaluationCell(row, evaluationHorizonLabel(group.horizon_minutes));
      appendEvaluationCell(row, `${group.paired_case_count} / ${group.sample_count}`, { numeric: true });
      appendEvaluationCell(row, evaluationMaturityText(group.maturity));
      appendEvaluationCell(
        row,
        evaluationComparisonRate(group.primary_agreement_rate_percent, group.maturity),
        { numeric: group.primary_agreement_rate_percent !== null },
      );
      appendEvaluationCell(
        row,
        evaluationComparisonRate(group.baseline_agreement_rate_percent, group.maturity),
        { numeric: group.baseline_agreement_rate_percent !== null },
      );
      appendEvaluationCell(
        row,
        !group.incremental_comparison
          ? "不衡量增量"
          : group.agreement_change_percentage_points === null
            ? "等待样本成熟"
          : `${Number(group.agreement_change_percentage_points) >= 0 ? "+" : ""}${Number(group.agreement_change_percentage_points).toFixed(1)} 个百分点`,
        { numeric: group.agreement_change_percentage_points !== null },
      );
      ui.evaluationComparisonBody.append(row);
    });
    ui.evaluationComparisonEmpty.hidden = comparison.groups.length > 0;
  }

  ui.evaluationGroupBody.replaceChildren();
  evaluation.groups.forEach((group) => {
    const row = document.createElement("tr");
    row.className = "evaluation-row";
    appendEvaluationCell(row, group.stage_label);
    appendEvaluationCell(row, evaluationHorizonLabel(group.horizon_minutes));
    appendEvaluationCell(row, String(group.sample_count), { numeric: true });
    appendEvaluationCell(row, evaluationMaturityText(group.maturity));
    appendEvaluationCell(
      row,
      evaluationComparisonRate(group.agreement_rate_percent, group.maturity),
      { numeric: group.agreement_rate_percent !== null },
    );
    appendEvaluationCell(row, evaluationPercent(group.average_relative_return_percent), { numeric: true });
    appendEvaluationCell(row, evaluationPercent(group.average_favorable_excursion_percent), { numeric: true });
    appendEvaluationCell(row, evaluationPercent(group.average_adverse_excursion_percent), { numeric: true });
    ui.evaluationGroupBody.append(row);
  });
  ui.evaluationGroupEmpty.hidden = evaluation.groups.length > 0;

  ui.evaluationRecentBody.replaceChildren();
  evaluation.recent.forEach((item) => {
    const row = document.createElement("tr");
    row.className = "evaluation-row";
    if (item.verdict) row.dataset.verdict = item.verdict;
    appendEvaluationCell(row, item.entity_key);
    appendEvaluationCell(row, item.stage_label);
    appendEvaluationCell(row, formatTime(item.source_cutoff_at));
    appendEvaluationCell(row, evaluationHorizonLabel(item.horizon_minutes));
    appendEvaluationCell(row, evaluationPercent(item.forward_return_percent), { numeric: true });
    appendEvaluationCell(row, evaluationPercent(item.relative_return_percent), { numeric: true });
    appendEvaluationCell(row, evaluationPercent(item.maximum_favorable_excursion_percent), { numeric: true });
    appendEvaluationCell(row, evaluationPercent(item.maximum_adverse_excursion_percent), { numeric: true });
    appendEvaluationCell(row, item.verdict_label);
    appendEvaluationCell(
      row,
      item.status === "COMPLETE"
        ? `已完成 · ${formatTime(item.outcome_cutoff_at)}`
        : item.status === "PENDING"
          ? `${item.status_label} · 到期 ${formatTime(item.due_at)}`
          : item.reason_code
            ? `${item.status_label} · ${item.reason_code}`
            : item.status_label,
    );
    ui.evaluationRecentBody.append(row);
  });
  ui.evaluationRecentEmpty.hidden = evaluation.recent.length > 0;
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
  state.chartResizeTimer = setTimeout(() => {
    if (state.chartModel) drawHistoryChart();
    updateQuoteHorizontalScrollbar();
  }, 100);
});

function updateBackToTopVisibility() {
  ui.backToTop.hidden = window.scrollY < 320;
}

window.addEventListener("scroll", () => {
  updateBackToTopVisibility();
  updateQuoteHorizontalScrollbar();
}, { passive: true });

ui.backToTop.addEventListener("click", () => {
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  window.scrollTo({ top: 0, behavior: reducedMotion ? "auto" : "smooth" });
});

updateBackToTopVisibility();

function renderIssues(issues, currentIssues, monitor) {
  const diagnosticIssues = issues.filter(
    (issue) => issue.classification !== "EXPECTED_ABSENCE"
  );
  const currentIssueIds = new Set(
    (currentIssues || [])
      .filter((issue) => issue.classification !== "EXPECTED_ABSENCE")
      .map((issue) => issue.issue_id),
  );
  const isActive = (issue) => issue.state === "ACTIVE" || currentIssueIds.has(issue.issue_id);
  const activeCount = diagnosticIssues.filter(isActive).length;
  const historyCount = diagnosticIssues.length - activeCount;
  ui.issueBody.replaceChildren();
  diagnosticIssues.forEach((issue) => {
    const row = document.createElement("tr");
    const stateValue = isActive(issue) ? "ACTIVE" : issue.state || "HISTORICAL";
    row.className = "issue-row";
    row.dataset.state = stateValue;

    const stateCell = document.createElement("td");
    const stateBadge = createElement(
      "span",
      "issue-state-badge",
      ISSUE_STATE_LABELS[stateValue] || "历史记录",
    );
    stateBadge.dataset.state = stateValue;
    stateCell.append(stateBadge);
    if (stateValue === "RECOVERED" && issue.recovered_at) {
      stateCell.append(createElement("span", "issue-meta", `恢复于 ${formatTime(issue.recovered_at)}`));
    }
    row.append(stateCell);

    const scope = document.createElement("td");
    scope.className = "issue-scope";
    scope.append(createElement("strong", "issue-cell-title", issueScopeLabel(issue.scope)));
    if (issueScopeLabel(issue.scope) !== issue.scope) {
      scope.append(createElement("code", "issue-code", issue.scope));
    }
    row.append(scope);

    const time = document.createElement("td");
    time.append(createElement("span", "issue-cell-title", formatTime(issue.occurred_at)));
    time.append(createElement("span", "issue-meta", `运行 #${issue.run_id} · 记录 #${issue.issue_id}`));
    row.append(time);

    const reason = document.createElement("td");
    reason.className = "issue-reason";
    reason.append(createElement("strong", "issue-reason-label", issueReasonLabel(issue.reason_code)));
    reason.append(createElement("code", "issue-code", issue.reason_code));
    reason.append(createElement("span", "issue-reason-detail", issueReasonDetailForRecord(issue)));
    const contextEntries = issueContextEntries(issue.context);
    if (contextEntries.length > 0) {
      const context = document.createElement("dl");
      context.className = "issue-context";
      contextEntries.forEach((entry) => {
        context.append(createElement("dt", "", entry.label));
        context.append(createElement("dd", "", entry.value));
      });
      reason.append(context);
    }
    row.append(reason);
    ui.issueBody.append(row);
  });
  const countParts = [];
  if (activeCount > 0) countParts.push(`${activeCount} 条当前`);
  if (historyCount > 0) countParts.push(`${historyCount} 条历史`);
  ui.issueCount.textContent = countParts.join(" · ") || "0 条";
  ui.diagnosticsOpenCount.textContent = String(activeCount);
  ui.diagnosticsOpenCount.hidden = activeCount === 0;
  ui.diagnosticsOpen.dataset.hasIssues = String(activeCount > 0);
  ui.diagnosticsOpen.setAttribute(
    "aria-label",
    activeCount > 0
      ? `查看${monitor.display_name}的诊断日志，${activeCount} 条当前问题`
      : historyCount > 0
        ? `查看${monitor.display_name}的诊断日志，当前采集正常，保留${historyCount}条历史记录`
        : `查看${monitor.display_name}的诊断日志，当前没有采集失败`,
  );
  ui.diagnosticsDialogSubtitle.textContent = activeCount > 0
    ? `${monitor.display_name} · ${activeCount} 条当前问题${historyCount > 0 ? `，另有 ${historyCount} 条历史记录` : ""}`
    : historyCount > 0
      ? `${monitor.display_name} · 当前采集正常，保留 ${historyCount} 条近期历史记录`
      : `${monitor.display_name} · 当前没有采集失败`;
  ui.issueScroll.hidden = diagnosticIssues.length === 0;
  ui.issueEmpty.hidden = diagnosticIssues.length > 0;
}

ui.diagnosticsOpen.addEventListener("click", () => {
  if (!ui.diagnosticsDialog.open) ui.diagnosticsDialog.showModal();
});

ui.diagnosticsDialogClose.addEventListener("click", () => {
  ui.diagnosticsDialog.close();
});

ui.diagnosticsDialog.addEventListener("click", (event) => {
  if (event.target === ui.diagnosticsDialog) ui.diagnosticsDialog.close();
});

document.addEventListener("visibilitychange", () => {
  clearTimeout(state.refreshTimer);
  state.refreshTimer = null;
  if (document.visibilityState === "visible") loadView();
  else stopForegroundObservation();
});

applyMonitorRailState({ persist: false });
loadView();
