from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
from pathlib import Path
import time
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

from halpha_monitor.contracts import (
    AutomaticCollectionState,
    BtcMonthlyResearchHistoryObservation,
    BtcMonthlyResearchRevision,
    BtcStructureEventRevision,
    BtcStructureHistoryObservation,
    BuybackEntityRevision,
    BuybackEvidenceDocument,
    BuybackSourceObservation,
    CollectionBatch,
    CollectionIssue,
    ConfigurationField,
    EvaluationView,
    FilterChoice,
    ForwardEvaluationCase,
    ForwardEvaluationResult,
    MetricSample,
    MarketEventRevision,
    MonitorView,
    ProjectionSnapshot,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.monitors.a_hk_buyback import AHKBuybackMonitor
from halpha_monitor.monitors.market_events import MarketEventMonitor
from halpha_monitor.service import MonitorRegistry
from halpha_monitor.service import MonitorScheduler
from halpha_monitor.store import SQLiteMonitorStore, iso_utc
from halpha_monitor.web import create_app


@dataclass
class FakeMonitor:
    monitor_id: str = "fake-monitor"
    display_name: str = "Fixture Monitor"
    description: str = "Fixture description"
    interval_seconds: float = 60
    foreground_interval_seconds: float | None = None
    view: MonitorView = MonitorView(
        filters=(
            ViewFilter(
                key="trade_type",
                label="方向",
                default="BUY",
                choices=(FilterChoice("BUY", "买入币"), FilterChoice("SELL", "卖出币")),
            ),
        ),
        columns=(
            ViewColumn("asset", "币种"),
            ViewColumn(
                "value",
                "核算价",
                "number",
                description="经过校验的核算价格。",
            ),
            ViewColumn("observed_at", "采集时间", "time"),
        ),
        chart_title="核算价历史",
        method_note="方法说明",
    )
    configuration_fields: tuple[ConfigurationField, ...] = (
        ConfigurationField(
            key="target_fiat",
            label="核算金额",
            kind="decimal",
            unit="CNY",
            minimum="1",
            step="1",
        ),
        ConfigurationField(
            key="trade_methods",
            label="支付方式",
            kind="multi_choice",
            choices=(
                FilterChoice("BANK", "银行卡"),
                FilterChoice("ALIPAY", "支付宝"),
                FilterChoice("WECHAT", "微信"),
            ),
        ),
    )
    configuration_values: dict[str, object] = field(
        default_factory=lambda: {
            "target_fiat": "2000",
            "trade_methods": ["BANK", "ALIPAY", "WECHAT"],
        }
    )
    observed_network_requests: int = 7

    def collect(self) -> CollectionBatch:
        return CollectionBatch(samples=())

    def configuration(self) -> dict[str, object]:
        return dict(self.configuration_values)

    def normalize_configuration(self, values: dict[str, object]) -> dict[str, object]:
        if set(values) != {"target_fiat", "trade_methods"}:
            raise ValueError("CONFIGURATION_INVALID")
        methods = values["trade_methods"]
        if not isinstance(methods, list) or not methods:
            raise ValueError("CONFIGURATION_INVALID")
        return {
            "target_fiat": str(values["target_fiat"]),
            "trade_methods": [str(item) for item in methods],
        }

    def apply_configuration(self, values: dict[str, object]) -> None:
        self.configuration_values = self.normalize_configuration(values)

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        assert window_seconds == 60
        return self.observed_network_requests


def add_run(store: SQLiteMonitorStore, observed_at: datetime, value: str) -> None:
    run_id = store.start_run("fake-monitor", started_at=observed_at)
    store.finish_run(
        run_id,
        "fake-monitor",
        CollectionBatch(
            samples=(
                MetricSample(
                    series_key="BUY|BTC",
                    entity_key="BTC",
                    observed_at=observed_at,
                    value_text=value,
                    unit="CNY_PER_USDT",
                    payload={"asset": "BTC", "trade_type": "BUY", "value": value},
                ),
            )
        ),
        completed_at=observed_at,
    )


def make_client(tmp_path: Path) -> TestClient:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    add_run(store, now - timedelta(minutes=2), "6.70")
    add_run(store, now, "6.75")
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    app = create_app(store, registry, None, start_scheduler=False)
    return TestClient(app, base_url="http://127.0.0.1:8790")


def test_page_and_static_assets_are_local_and_hardened(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        page = client.get("/")
        script = client.get("/static/app.js")
        style = client.get("/static/styles.css")
        api = client.get("/api/view")

    assert page.status_code == 200
    assert "Halpha Monitor" in page.text
    assert script.status_code == 200
    assert style.status_code == 200
    assert api.status_code == 200
    assert "monitor.operational_status" in script.text
    assert 'id="monitor-status-detail"' not in page.text
    assert 'id="collection-load"' in page.text
    assert 'id="network-requests"' in page.text
    assert 'id="monitor-rail-toggle"' in page.text
    assert 'class="monitor-rail-sticky"' in page.text
    assert 'class="rail-summary"' not in page.text
    assert 'id="monitoring-count"' not in page.text
    assert 'id="diagnostics-open"' in page.text
    assert 'id="diagnostics-dialog"' in page.text
    assert "发生时间 / 运行" in page.text
    assert "原因与定位" in page.text
    assert 'id="diagnostics-region"' not in page.text
    assert page.text.index('id="diagnostics-open"') < page.text.index(
        'id="monitor-control-button"'
    )
    assert 'id="monitor-refresh-button"' in page.text
    assert "/api/monitors/${encodeURIComponent(monitorId)}/refresh" in script.text
    assert (
        "/api/monitors/${encodeURIComponent(monitor.monitor_id)}/observe" in script.text
    )
    assert 'document.visibilityState === "visible"' in script.text
    assert "maintainForegroundObservation" in script.text
    assert "syncForegroundObservation" in script.text
    assert "state.observationTimer = setInterval" in script.text
    assert "state.observationMonitorId === payload.monitor.monitor_id" in script.text
    assert "payload.refresh_after_seconds" in script.text
    assert "ui.btcRegime.textContent = regime?.label" in script.text
    assert "String(sourceState).replaceAll" not in script.text
    assert "当前版本确认" in page.text
    assert "nextRefreshMilliseconds = manualRunStarted ? 15000 : 3000" in script.text
    assert 'id="quote-scroll"' in page.text
    assert 'id="quote-horizontal-scrollbar"' in page.text
    assert 'id="quote-horizontal-scrollbar-track"' in page.text
    assert 'id="table-pagination"' in page.text
    assert 'id="table-page-previous"' in page.text
    assert 'id="table-page-next"' in page.text
    assert 'id="back-to-top"' in page.text
    assert 'id="table-scroll-controls"' not in page.text
    assert 'id="table-scroll-left"' not in page.text
    assert 'id="table-scroll-right"' not in page.text
    assert 'id="monitor-method-note"' in page.text
    assert 'id="buyback-overview-region"' in page.text
    assert 'id="buyback-stock-search"' in page.text
    assert 'placeholder="输入代码或名称"' in page.text
    assert "数据来源与问题" in page.text
    assert page.text.index('id="buyback-source-region"') < page.text.index(
        'class="table-region"'
    )
    assert "数据覆盖与异常（系统维护）" not in page.text
    assert "原文追赶" not in script.text
    assert "份 A 股公告原文尚未获取" in script.text
    assert "filter-help-tooltip" in script.text
    assert ".filter-help-tooltip" in style.text
    assert "filter-multi-choice" in script.text
    assert ".filter-multi-choice" in style.text
    assert "table-column-help-tooltip" in script.text
    assert ".table-column-help-tooltip" in style.text
    assert "ui.diagnosticsDialog.showModal()" in script.text
    assert "renderIssues(payload.issues, payload.current_issues, payload.monitor)" in script.text
    assert '"stock-announcements": "公司公告索引"' in script.text
    assert "STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED" in script.text
    assert "运行 #${issue.run_id} · 记录 #${issue.issue_id}" in script.text
    assert "issueContextEntries(issue.context)" in script.text
    assert '.issue-state-badge[data-state="RECOVERED"]' in style.text
    assert "ui.buybackSourceRegion.hidden = !hasVisibleProblem" in script.text
    assert '"a-share-documents": "A股公告原文"' in script.text
    assert 'return "公开来源连接中断"' in script.text
    assert (
        'const isMarketEvents = payload.monitor.projection_kind === "market_events"'
        in script.text
    )
    assert '.workspace[data-projection-kind="buyback"] #quote-scroll' in style.text
    assert "overflow-y: hidden" in style.text
    assert '.app-shell[data-rail-collapsed="true"]' in style.text
    assert ".monitor-link-compact" in style.text
    assert ".quote-horizontal-scrollbar" in style.text
    assert "MONITOR_RAIL_STORAGE_KEY" in script.text
    assert "function applyMonitorRailState" in script.text
    assert "function monitorCompactLabel" in script.text
    assert "function updateQuoteHorizontalScrollbar" in script.text
    assert 'ui.quoteHorizontalScrollbar.addEventListener("scroll"' in script.text
    assert 'ui.quoteScroll.addEventListener("wheel"' in script.text
    assert "usesPageLengthTable" in script.text
    assert "button.dataset.status = monitor.operational_status.tone" in script.text
    assert '.monitor-link[data-status="ACTIVE"]' in style.text
    assert '.monitor-link[data-status="IDLE"]' in style.text
    assert '.monitor-link[data-status="DISABLED"]' in style.text
    assert "box-shadow: inset -5px 0 0 var(--monitor-status-color)" in style.text
    assert ".monitor-link .monitor-link-status::before" not in style.text
    assert 'app-shell[data-rail-collapsed="true"] .monitor-link-status' in style.text
    assert "place-items: center" in style.text
    assert 'id="btc-intelligence"' in page.text
    assert "function renderBtcIntelligence" in script.text
    assert '.workspace[data-projection-kind="btc-intelligence"]' in style.text

    assert ".table-pagination" in style.text
    assert ".back-to-top" in style.text
    assert "BUYBACK_TABLE_PAGE_SIZE = 50" in script.text
    assert "function buybackTablePage" in script.text
    assert "function renderTablePagination" in script.text
    assert "sortedRows.slice(page.start, page.end)" in script.text
    assert "function scrollToTableStart" in script.text
    assert "function updateBackToTopVisibility" in script.text
    assert 'window.addEventListener("scroll", () =>' in script.text
    assert "window.scrollTo({ top: 0" in script.text
    assert "function buybackMarketDestination" in script.text
    assert "https://cn.tradingview.com/chart/?symbol=" in script.text
    assert "encodeURIComponent(`HKEX:${tradingViewCode}`)" in script.text
    assert 'row.market === "SH" ? "SSE" : row.market === "SZ" ? "SZSE"' in script.text
    assert "tradingViewCode = code.replace" in script.text
    assert "https://quote.eastmoney.com/" not in script.text
    assert "查看 TradingView 专业图表 ↗" in script.text
    assert 'id="event-attention-region"' in page.text
    assert 'id="event-calendar-region"' in page.text
    assert 'id="macro-indicator-region"' in page.text
    assert 'id="market-event-detail-dialog"' in page.text
    assert 'id="event-view-tabs"' in page.text
    assert 'id="radar-view-tabs"' in page.text
    assert 'id="radar-table-tab"' in page.text
    assert 'id="radar-position-tab"' in page.text
    assert 'id="radar-history-tab"' in page.text
    assert 'id="radar-evaluation-tab"' in page.text
    assert 'id="evaluation-comparison"' in page.text
    assert 'id="evaluation-comparison-body"' in page.text
    assert "综合状态 × 检验期限" in page.text
    assert "原阶段" not in page.text
    assert 'id="radar-price-state-field"' in page.text
    assert page.text.index('id="radar-view-tabs"') < page.text.index('id="filters"')
    assert 'id="event-history-region"' in page.text
    assert 'id="market-event-how-to-read"' in page.text
    assert 'id="market-event-direction-formula"' in page.text
    assert "function renderMarketEvents" in script.text
    assert 'id="stock-events"' in page.text
    assert 'id="stock-selector-dialog"' in page.text
    assert 'id="stock-selector-add-query"' in page.text
    assert 'aria-autocomplete="list"' in page.text
    assert 'id="stock-selector-suggestions"' in page.text
    assert 'id="stock-calendar-grid"' in page.text
    assert "function renderStockEvents" in script.text
    assert "function renderStockSelector" in script.text
    assert "function scheduleStockDirectorySearch" in script.text
    assert "/stocks/search?${params.toString()}" in script.text
    assert "manual_stock_codes: manual" in script.text
    assert '.workspace[data-projection-kind="stock-events"]' in style.text
    assert ".stock-calendar-layout" in style.text
    assert ".stock-selector-suggestions" in style.text
    assert ".stock-selector-dialog {\n  --stock-blue: #0b6cf0;" in style.text
    assert "function renderEventHistory" in script.text
    assert "function applyEventTabState" in script.text
    assert "function applyRadarTabState" in script.text
    assert "function radarTabFromLocation" in script.text
    assert 'POSITION: "position"' in script.text
    assert 'params.set(\n      "view"' in script.text
    assert "function renderRadarPriceFilter" in script.text
    assert "function renderRadarTableView" in script.text
    assert "RADAR_POSITION_TABLE_PAGE_SIZE = 50" in script.text
    assert "function openMarketEventDetail" in script.text
    assert (
        '.workspace[data-projection-kind="market-events"] #quote-scroll' in style.text
    )
    assert "buyback-eligibility-dot" in script.text
    assert "buyback-detail-button" not in script.text
    assert '["高吸引力", buybackPayload.high_attractiveness_count]' in script.text
    assert "当前港股通可买" not in script.text
    assert "发现事实错误？提交人工校正" in page.text
    for routine_validity_label in ("已核验", "系统已核验", "核验状态", "情报范围"):
        assert routine_validity_label not in page.text
        assert routine_validity_label not in script.text
    assert 'id="time-window-label"' in page.text
    assert "历史范围" in page.text
    assert "payload.current_issues" in script.text
    assert "function sortTableRows" in script.text
    assert "updateTableScrollControls" not in script.text
    assert "scrollTableHorizontally" not in script.text
    assert 'button.setAttribute("aria-description", column.description)' in script.text
    assert "function monitorIdFromLocation" in script.text
    assert "window.history.replaceState" in script.text
    assert "https://www.binance.com/zh-CN/futures/" in script.text
    assert "https://www.binance.com/zh-CN/trade/" not in script.text
    assert (
        '.workspace[data-projection-kind="altcoin-radar"] #quote-scroll' in style.text
    )
    assert '.radar-price-state[data-state="PUMPING"]' in style.text
    assert 'column.key === "context_stage_label"' in script.text
    assert '.radar-context-stage[data-group="HIGH_RISK"]' in style.text
    assert "#quote-scroll thead th" in style.text
    assert '.monitor-link-status[data-status="ACTIVE"]' in style.text
    assert '.monitor-link-status[data-status="IDLE"]' in style.text
    assert '[data-status="IDLE"]::before' in style.text
    assert "--idle: #356a8a" in style.text
    assert "box-shadow: 0 0 0 2px var(--idle-soft)" in style.text
    assert page.headers["x-frame-options"] == "DENY"
    page_policy = page.headers["content-security-policy"]
    assert "default-src 'self'" in page_policy
    assert "script-src 'self'" in page_policy
    assert "style-src 'self' 'unsafe-inline'" in page_policy
    for strict_response in (script, style, api):
        strict_policy = strict_response.headers["content-security-policy"]
        assert "style-src 'self';" in strict_policy
        assert "'unsafe-inline'" not in strict_policy
    assert page.headers["cache-control"] == "no-store"


class FakeBtcIntelligenceMonitor:
    monitor_id = "btc-market-intelligence"
    display_name = "BTC 专业情报"
    description = "分层证据测试"
    interval_seconds = 60
    projection_kind = "btc_intelligence"
    view = MonitorView(
        filters=(),
        columns=(ViewColumn("symbol", "市场"),),
        chart_title="BTC 专业情报",
        table_title="BTC 专业情报",
    )

    def collect(self) -> CollectionBatch:
        return CollectionBatch(samples=())

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return 0


def test_btc_intelligence_projection_includes_forward_ledger_without_generic_rows(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    observed = datetime.now(UTC)
    started = observed - timedelta(hours=3)
    event_at = observed - timedelta(hours=2)
    monthly_signal_at = observed - timedelta(hours=1)
    monthly_execution_at = observed + timedelta(days=1)
    signal = {
        "kind": "SUPPORT",
        "open_time_ms": int(event_at.timestamp() * 1000),
        "zone_lower": "100",
        "zone_upper": "102",
        "touch_close": "101",
        "atr_touch": "2",
        "due_at": (event_at + timedelta(hours=24)).isoformat(),
    }
    run_id = store.start_run("btc-market-intelligence", started_at=observed)
    store.finish_run(
        run_id,
        "btc-market-intelligence",
        CollectionBatch(
            samples=(
                MetricSample(
                    "BTCUSDT|intelligence",
                    "BTCUSDT",
                    observed,
                    "40000",
                    "USDT",
                    {
                        "row_type": "BTC_INTELLIGENCE",
                        "symbol": "BTCUSDT",
                        "observed_at": observed.isoformat(),
                        "current_price": "40000",
                        "monthly": {"official_target": 1},
                        "daily": {"agreement": "0.75"},
                        "structure": {"model_status": "NOT_DEPLOYED"},
                        "smart_money": {"authority": "CONTEXT_ONLY", "rows": []},
                    },
                ),
            ),
            btc_structure_history=BtcStructureHistoryObservation(
                started_at=started,
                processed_through_at=observed,
                algorithm_version="btc-structure-causal-v1",
            ),
            btc_structure_event_revisions=(
                BtcStructureEventRevision(
                    event_key="event-1",
                    event_at=event_at,
                    observed_at=observed,
                    state="PENDING",
                    payload={
                        "algorithm_version": "btc-structure-causal-v1",
                        "signal": signal,
                        "outcome": None,
                    },
                ),
            ),
            btc_monthly_research_history=BtcMonthlyResearchHistoryObservation(
                started_at=monthly_signal_at,
                processed_through_at=monthly_signal_at,
                algorithm_version="btc-faber-10m-forward-v1",
            ),
            btc_monthly_research_revisions=(
                BtcMonthlyResearchRevision(
                    signal_key="btc-faber-10m-forward-v1:2026-07",
                    signal_at=monthly_signal_at,
                    observed_at=observed,
                    state="SIGNAL_FROZEN",
                    payload={
                        "algorithm_version": "btc-faber-10m-forward-v1",
                        "signal": {
                            "official_target": 1,
                            "execution_eligible_at": monthly_execution_at.isoformat(),
                        },
                        "execution": None,
                    },
                ),
            ),
        ),
        completed_at=observed,
    )
    registry = MonitorRegistry()
    registry.register(FakeBtcIntelligenceMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get("/api/view").json()

    assert payload["monitor"]["projection_kind"] == "btc_intelligence"
    assert payload["rows"] == []
    assert payload["btc_intelligence"]["current_price"] == "40000"
    assert payload["btc_intelligence"]["ledger"]["total_events"] == 1
    assert payload["btc_intelligence"]["ledger"]["events"][0]["state"] == "PENDING"
    assert payload["btc_intelligence"]["monthly_ledger"]["signal_count"] == 1
    assert (
        payload["btc_intelligence"]["monthly_ledger"]["records"][0]["state"]
        == "SIGNAL_FROZEN"
    )


def test_page_rejects_untrusted_host_header(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        response = client.get("/", headers={"Host": "attacker.example"})

    assert response.status_code == 400


def test_view_returns_latest_rows_history_and_registration_metadata(
    tmp_path: Path,
) -> None:
    with make_client(tmp_path) as client:
        response = client.get("/api/view", params={"trade_type": "BUY", "hours": 6})

    assert response.status_code == 200
    payload = response.json()
    assert payload["service_status"] == "HEALTHY"
    assert payload["monitor"]["monitor_id"] == "fake-monitor"
    assert payload["monitor"]["enabled"] is True
    assert payload["monitor"]["table_title"] == "最新监控数据"
    assert payload["monitor"]["filters"][0]["label"] == "方向"
    assert payload["monitor"]["columns"][1]["maximum_fraction_digits"] == 8
    assert payload["monitor"]["columns"][1]["show_sign"] is True
    assert payload["monitor"]["columns"][1]["description"] == ("经过校验的核算价格。")
    assert payload["monitor"]["method_note"] == "方法说明"
    assert payload["monitor"]["show_description"] is True
    assert payload["time_windows"] == [
        {"hours": 1, "label": "1小时"},
        {"hours": 3, "label": "3小时"},
        {"hours": 6, "label": "6小时"},
        {"hours": 12, "label": "12小时"},
        {"hours": 24, "label": "1天"},
        {"hours": 72, "label": "3天"},
        {"hours": 168, "label": "7天"},
        {"hours": 336, "label": "14天"},
        {"hours": 720, "label": "30天"},
    ]
    assert payload["monitor"]["configuration"]["values"]["target_fiat"] == "2000"
    assert payload["monitor"]["operational_status"] == {
        "kind": "MONITORING",
        "tone": "ACTIVE",
        "label": "监控中",
    }
    assert payload["monitor"]["data_status"] == {
        "kind": "CURRENT",
        "tone": "HEALTHY",
        "label": "最新采集已完成",
        "detail": "展示字段均已通过校验，并对应所示截止时间。",
        "cutoff_label": "最近采集完成",
    }
    assert payload["rows"][0]["value"] == "6.75"
    assert [point["value"] for point in payload["history"]] == ["6.70", "6.75"]
    assert payload["collection_gaps"] == []
    assert payload["collection_load"] == {
        "level": "LOW",
        "level_label": "低",
        "utilization_percent": 0,
        "enabled_count": 1,
        "collecting_count": 0,
        "planned_runs_per_minute": 1.0,
        "network_requests": 7,
        "request_window_seconds": 60,
        "request_measurement": "FULL",
        "measured_monitor_count": 1,
        "latest_completed_at": payload["monitor"]["latest_run"]["completed_at"],
        "definition": (
            "负载占用为各启用监控最近或当前一轮耗时除以各自采集周期之和；"
            "低于 25% 为低，25% 至 74% 为中，75% 及以上为高；"
            "请求数为本进程近 60 秒实际发出的公开 HTTP 请求。"
        ),
    }


def test_altcoin_radar_projection_excludes_legacy_spot_samples(
    tmp_path: Path,
) -> None:
    @dataclass
    class RadarMonitor:
        monitor_id: str = "binance-altcoin-radar"
        display_name: str = "山寨币异动雷达"
        description: str = "Contract fixture"
        interval_seconds: float = 300
        projection_kind: str = "altcoin_radar"
        price_position_snapshot_key: str = "price-position-v1"
        price_position_table_title: str = "日线价格位置"
        price_position_method_note: str = "位置方法说明"
        price_position_filter_choices: tuple[FilterChoice, ...] = (
            FilterChoice("*", "全部状态"),
            FilterChoice("RISING", "拉升与暴涨"),
        )
        price_position_columns: tuple[ViewColumn, ...] = (
            ViewColumn("symbol", "币种"),
            ViewColumn("price_state_label", "价格状态"),
            ViewColumn("return_24h_percent", "24h涨跌", "percent"),
        )
        view: MonitorView = MonitorView(
            filters=(),
            columns=(ViewColumn("symbol", "合约"),),
            table_title="全市场初筛后的异动候选",
            chart_title="异动强度历史",
        )

        def collect(self) -> CollectionBatch:
            return CollectionBatch(samples=())

    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    run_id = store.start_run("binance-altcoin-radar", started_at=now)
    store.finish_run(
        run_id,
        "binance-altcoin-radar",
        CollectionBatch(
            samples=(
                MetricSample(
                    series_key="OLDUSDT|alert-score",
                    entity_key="OLDUSDT",
                    observed_at=now,
                    value_text="70",
                    unit="RADAR_SCORE",
                    payload={"symbol": "OLDUSDT"},
                ),
                MetricSample(
                    series_key="NEWUSDT|usdm-perpetual-alert-score",
                    entity_key="NEWUSDT",
                    observed_at=now,
                    value_text="80",
                    unit="RADAR_SCORE",
                    payload={
                        "symbol": "NEWUSDT",
                        "market_scope": "USDM_PERPETUAL",
                    },
                ),
            ),
            projection_snapshots=(
                ProjectionSnapshot(
                    snapshot_key="price-position-v1",
                    observed_at=now,
                    cutoff_at=now,
                    payload={
                        "price_cutoff_at": now.isoformat(),
                        "daily_cutoff_at": (
                            now.replace(hour=0, minute=0, second=0, microsecond=0)
                            - timedelta(milliseconds=1)
                        ).isoformat(),
                        "valid_until": (now + timedelta(minutes=15)).isoformat(),
                        "coverage_label": "1 个合约形成价格位置。",
                        "counts": {"eligible": 1, "included": 1},
                        "state_counts": {"SURGE": 1},
                        "rows": [
                            {
                                "symbol": "NEWUSDT",
                                "price_state": "SURGE",
                                "price_state_label": "暴涨",
                                "price_state_group": "RISING",
                                "return_24h_percent": "18.5",
                            }
                        ],
                    },
                ),
            ),
        ),
        completed_at=now,
    )
    registry = MonitorRegistry()
    registry.register(RadarMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get(
            "/api/view",
            params={"monitor_id": "binance-altcoin-radar"},
        ).json()
        candidates = client.get(
            "/api/view",
            params={
                "monitor_id": "binance-altcoin-radar",
                "view": "candidates",
            },
        ).json()
        position = client.get(
            "/api/view",
            params={
                "monitor_id": "binance-altcoin-radar",
                "view": "position",
            },
        ).json()
        invalid = client.get(
            "/api/view",
            params={
                "monitor_id": "binance-altcoin-radar",
                "view": "unsupported",
            },
        )

    assert payload["monitor"]["projection_kind"] == "altcoin_radar"
    assert [row["symbol"] for row in payload["rows"]] == ["NEWUSDT"]
    assert payload["selected_series_key"] == ("NEWUSDT|usdm-perpetual-alert-score")
    price_position = payload["altcoin_price_position"]
    assert price_position["status"] == "CURRENT"
    assert price_position["table_title"] == "日线价格位置"
    assert [row["symbol"] for row in price_position["rows"]] == ["NEWUSDT"]
    assert price_position["state_counts"] == {"SURGE": 1}
    assert [column["key"] for column in price_position["columns"]] == [
        "symbol",
        "price_state_label",
        "return_24h_percent",
    ]
    assert [row["symbol"] for row in candidates["rows"]] == ["NEWUSDT"]
    assert candidates["altcoin_price_position"] is None
    assert candidates["history"] == []
    assert position["rows"] == []
    assert [row["symbol"] for row in position["altcoin_price_position"]["rows"]] == [
        "NEWUSDT"
    ]
    assert position["evaluation"] is None
    assert invalid.status_code == 422
    assert invalid.json()["detail"] == "RADAR_VIEW_UNSUPPORTED"


@pytest.mark.parametrize(
    ("minimum_observation_days", "expected_ready", "expected_primary_rate"),
    ((0.0, True, 100.0), (1.0, False, None)),
)
def test_forward_evaluation_payload_compares_exact_primary_and_baseline_pairs(
    tmp_path: Path,
    minimum_observation_days: float,
    expected_ready: bool,
    expected_primary_rate: float | None,
) -> None:
    @dataclass
    class EvaluationMonitor:
        monitor_id: str = "evaluation-monitor"
        display_name: str = "Evaluation"
        description: str = "Evaluation fixture"
        interval_seconds: float = 300
        evaluation_source: str = "PRICE_CONTEXT_V1"
        baseline_evaluation_source: str = "SHORT_RULE_V1"
        view: MonitorView = MonitorView(
            filters=(),
            columns=(ViewColumn("symbol", "币种"),),
            table_title="候选",
            chart_title="历史",
            evaluation=EvaluationView(
                title="后续行情检验",
                method_note="同批比较。",
                minimum_group_samples=1,
                minimum_distinct_cutoffs=1,
                minimum_distinct_entities=1,
                minimum_observation_days=minimum_observation_days,
            ),
        )

        def collect(self) -> CollectionBatch:
            return CollectionBatch(samples=())

    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC).replace(microsecond=0)
    due_at = now + timedelta(minutes=15)
    cases = (
        ForwardEvaluationCase(
            case_key="price|AAAUSDT|BLOWOFF_RISK|15",
            entity_key="AAAUSDT",
            stage="BLOWOFF_RISK",
            stage_label="冲高回落风险",
            direction="DOWN",
            signal_observed_at=now,
            source_cutoff_at=now,
            horizon_minutes=15,
            due_at=due_at,
            entry_price_text="10",
            benchmark_entry_price_text="50000",
            source="PRICE_CONTEXT_V1",
        ),
        ForwardEvaluationCase(
            case_key="short|AAAUSDT|ACCELERATION|15",
            entity_key="AAAUSDT",
            stage="ACCELERATION",
            stage_label="加速",
            direction="UP",
            signal_observed_at=now,
            source_cutoff_at=now,
            horizon_minutes=15,
            due_at=due_at,
            entry_price_text="10",
            benchmark_entry_price_text="50000",
            source="SHORT_RULE_V1",
        ),
    )
    source_run = store.start_run("evaluation-monitor", started_at=now)
    store.finish_run(
        source_run,
        "evaluation-monitor",
        CollectionBatch(
            samples=(
                MetricSample(
                    series_key="AAAUSDT|score",
                    entity_key="AAAUSDT",
                    observed_at=now,
                    value_text="80",
                    unit="RADAR_SCORE",
                    payload={"symbol": "AAAUSDT"},
                ),
            ),
            evaluation_cases=cases,
        ),
        completed_at=now,
    )
    resolved_at = due_at + timedelta(minutes=1)
    results = tuple(
        ForwardEvaluationResult(
            case_key=case.case_key,
            status="COMPLETE",
            evaluated_at=resolved_at,
            outcome_cutoff_at=due_at,
            exit_price_text="9.5",
            benchmark_exit_price_text="50000",
            forward_return_percent=-5,
            benchmark_return_percent=0,
            relative_return_percent=-5,
            maximum_favorable_excursion_percent=1,
            maximum_adverse_excursion_percent=-6,
            verdict="ALIGNED" if case.direction == "DOWN" else "OPPOSED",
        )
        for case in cases
    )
    resolved_run = store.start_run("evaluation-monitor", started_at=resolved_at)
    store.finish_run(
        resolved_run,
        "evaluation-monitor",
        CollectionBatch(samples=(), evaluation_results=results),
        completed_at=resolved_at,
    )
    registry = MonitorRegistry()
    registry.register(EvaluationMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get(
            "/api/view",
            params={"monitor_id": "evaluation-monitor"},
        ).json()

    comparison = payload["evaluation"]["comparison"]
    assert comparison["paired_case_count"] == 1
    assert comparison["sample_count"] == 1
    flip_relation = comparison["relations"][0]
    assert flip_relation["direction_relation"] == "DIRECTION_FLIP"
    assert flip_relation["maturity"]["ready"] is expected_ready
    assert flip_relation["primary_agreement_rate_percent"] == expected_primary_rate
    assert flip_relation["baseline_agreement_rate_percent"] == (
        0.0 if expected_ready else None
    )
    assert flip_relation["agreement_change_percentage_points"] == (
        100.0 if expected_ready else None
    )
    assert comparison["relations"][1]["direction_relation"] == "SAME_DIRECTION"
    assert comparison["relations"][1]["sample_count"] == 0
    assert comparison["groups"][0]["stage"] == "BLOWOFF_RISK"
    assert comparison["groups"][0]["direction_relation"] == "DIRECTION_FLIP"
    assert payload["evaluation"]["maturity"]["ready"] is expected_ready


def test_view_promotes_uniform_opt_in_columns_and_restores_differing_values(
    tmp_path: Path,
) -> None:
    @dataclass
    class UniformMonitor:
        monitor_id: str = "uniform-monitor"
        display_name: str = "Uniform"
        description: str = "Uniform fixture"
        interval_seconds: float = 60
        view: MonitorView = MonitorView(
            filters=(),
            columns=(
                ViewColumn("symbol", "币种"),
                ViewColumn("stage", "阶段"),
                ViewColumn(
                    "valid_until",
                    "有效至",
                    "time",
                    promote_when_uniform=True,
                    uniform_summary_label="本轮结论有效至",
                ),
            ),
            chart_title="分值",
        )

        def collect(self) -> CollectionBatch:
            return CollectionBatch(samples=())

    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    registry = MonitorRegistry()
    registry.register(UniformMonitor())
    app = create_app(store, registry, None, start_scheduler=False)
    now = datetime.now(UTC)

    def add_uniform_run(*, differing: bool) -> None:
        run_id = store.start_run("uniform-monitor", started_at=now)
        values = (
            "2026-08-06T16:00:00Z",
            "2026-08-06T16:05:00Z" if differing else "2026-08-06T16:00:00Z",
        )
        store.finish_run(
            run_id,
            "uniform-monitor",
            CollectionBatch(
                samples=tuple(
                    MetricSample(
                        series_key=f"{symbol}|score",
                        entity_key=symbol,
                        observed_at=now,
                        value_text="1",
                        unit="SCORE",
                        payload={
                            "symbol": symbol,
                            "stage": "SETUP",
                            "valid_until": value,
                        },
                    )
                    for symbol, value in zip(("AAA", "BBB"), values, strict=True)
                )
            ),
            completed_at=now,
        )

    add_uniform_run(differing=False)
    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        uniform_payload = client.get("/api/view").json()
        assert [column["key"] for column in uniform_payload["monitor"]["columns"]] == [
            "symbol",
            "stage",
        ]
        assert uniform_payload["run_summary"] == [
            {
                "key": "valid_until",
                "label": "本轮结论有效至",
                "kind": "time",
                "priority": "primary",
                "minimum_fraction_digits": 0,
                "maximum_fraction_digits": 8,
                "use_grouping": False,
                "show_sign": True,
                "description": None,
                "value": "2026-08-06T16:00:00Z",
            }
        ]

        add_uniform_run(differing=True)
        differing_payload = client.get("/api/view").json()

    assert [column["key"] for column in differing_payload["monitor"]["columns"]] == [
        "symbol",
        "stage",
        "valid_until",
    ]
    assert differing_payload["run_summary"] == []


def test_view_wildcard_filter_returns_all_registered_values(tmp_path: Path) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    run_id = store.start_run("wildcard-monitor", started_at=now)
    store.finish_run(
        run_id,
        "wildcard-monitor",
        CollectionBatch(
            samples=tuple(
                MetricSample(
                    series_key=f"{stage}|score",
                    entity_key=stage,
                    observed_at=now,
                    value_text="50",
                    unit="SCORE",
                    payload={"stage": stage, "value": "50"},
                )
                for stage in ("SETUP", "BREAKOUT")
            )
        ),
        completed_at=now,
    )

    @dataclass
    class WildcardMonitor:
        monitor_id: str = "wildcard-monitor"
        display_name: str = "Wildcard"
        description: str = "Wildcard fixture"
        interval_seconds: float = 60
        view: MonitorView = MonitorView(
            filters=(
                ViewFilter(
                    "stage",
                    "阶段",
                    "*",
                    (
                        FilterChoice("*", "全部"),
                        FilterChoice("SETUP", "蓄势"),
                        FilterChoice("BREAKOUT", "启动"),
                    ),
                ),
            ),
            columns=(ViewColumn("value", "值", "number"),),
            chart_title="分值",
        )

        def collect(self) -> CollectionBatch:
            return CollectionBatch(samples=())

    registry = MonitorRegistry()
    registry.register(WildcardMonitor())
    app = create_app(store, registry, None, start_scheduler=False)
    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        all_rows = client.get("/api/view", params={"stage": "*"}).json()["rows"]
        setup_rows = client.get("/api/view", params={"stage": "SETUP"}).json()["rows"]

    assert {row["stage"] for row in all_rows} == {"SETUP", "BREAKOUT"}
    assert [row["stage"] for row in setup_rows] == ["SETUP"]


def test_view_separates_collecting_state_from_previous_result(tmp_path: Path) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    add_run(store, now - timedelta(seconds=10), "6.75")
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        store.start_run("fake-monitor", started_at=now)
        payload = client.get("/api/view").json()

    assert payload["monitor"]["operational_status"] == {
        "kind": "COLLECTING",
        "tone": "ACTIVE",
        "label": "采集中",
    }
    assert payload["monitor"]["data_status"]["kind"] == "COLLECTING_PREVIOUS"
    assert payload["monitor"]["data_status"]["label"] == ("正在刷新 · 显示上一轮结果")
    assert payload["service_status_label"] == "采集中"


def test_enabling_monitor_uses_single_scheduler_wakeup(tmp_path: Path) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = FakeMonitor()
    registry = MonitorRegistry()
    registry.register(monitor)
    scheduler = MonitorScheduler(registry, store)
    scheduler.start()
    calls = 0
    original_request_run = scheduler.request_run

    def counted_request_run(monitor_id: str) -> bool:
        nonlocal calls
        calls += 1
        return original_request_run(monitor_id)

    scheduler.request_run = counted_request_run  # type: ignore[method-assign]
    app = create_app(store, registry, scheduler, start_scheduler=False)
    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        response = client.put(
            "/api/monitors/fake-monitor/control",
            json={"enabled": True},
        )
    scheduler.stop()

    assert response.status_code == 200
    assert response.json()["refresh_requested"] is True
    assert calls == 0


def test_observation_endpoint_activates_adaptive_cadence(tmp_path: Path) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = FakeMonitor(
        interval_seconds=600,
        foreground_interval_seconds=60,
    )
    registry = MonitorRegistry()
    registry.register(monitor)
    scheduler = MonitorScheduler(registry, store)
    scheduler.start()
    app = create_app(store, registry, scheduler, start_scheduler=False)
    try:
        with TestClient(app, base_url="http://127.0.0.1:8790") as client:
            response = client.post("/api/monitors/fake-monitor/observe")
            payload = client.get("/api/view").json()
            rejected = client.post(
                "/api/monitors/fake-monitor/observe",
                headers={"Origin": "https://example.invalid"},
            )
    finally:
        scheduler.stop()

    assert response.status_code == 200
    assert response.json()["collection_cadence"] == {
        "adaptive": True,
        "background_interval_seconds": 600.0,
        "foreground_interval_seconds": 60.0,
        "effective_interval_seconds": 60.0,
        "foreground_active": True,
    }
    assert payload["monitor"]["collection_cadence"]["foreground_active"] is True
    assert payload["monitor"]["collection_cadence"]["effective_interval_seconds"] == 60
    assert rejected.status_code == 403


def test_health_reports_managed_worker_liveness(tmp_path: Path) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    scheduler = MonitorScheduler(registry, store)
    app = create_app(store, registry, scheduler)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["scheduler"] == "running"
    assert response.json()["workers"][0]["alive"] is True


def test_view_distinguishes_valid_history_after_latest_collection_failure(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    add_run(store, now - timedelta(seconds=20), "6.70")
    failed_run = store.start_run("fake-monitor", started_at=now - timedelta(seconds=2))
    store.fail_run(
        failed_run,
        "fake-monitor",
        "UPSTREAM_UNAVAILABLE",
        completed_at=now,
    )
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get("/api/view").json()

    assert payload["rows"][0]["value"] == "6.70"
    assert payload["monitor"]["data_status"]["kind"] == "HISTORICAL"
    assert payload["monitor"]["data_status"]["label"] == "历史快照 · 本轮无新结果"
    assert "已校验历史事实" in payload["monitor"]["data_status"]["detail"]
    assert payload["service_status_label"] == "监控运行中"


def test_view_marks_expected_no_match_in_place_without_degrading_service(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    run_id = store.start_run("fake-monitor", started_at=now - timedelta(seconds=1))
    store.finish_run(
        run_id,
        "fake-monitor",
        CollectionBatch(
            samples=(
                MetricSample(
                    series_key="BUY|BTC",
                    entity_key="BTC",
                    observed_at=now,
                    value_text="6.75",
                    unit="CNY_PER_USDT",
                    payload={"asset": "BTC", "trade_type": "BUY", "value": "6.75"},
                ),
            ),
            issues=(CollectionIssue("BUY:ETH", "NO_ELIGIBLE_C2C_AD"),),
        ),
        completed_at=now,
    )
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get("/api/view").json()

    data_status = payload["monitor"]["data_status"]
    assert data_status["kind"] == "CURRENT_WITH_NOTICES"
    assert data_status["tone"] == "HEALTHY"
    assert data_status["label"] == "最新采集已完成"
    assert "对应数据表内标记" in data_status["detail"]
    assert payload["service_status"] == "HEALTHY"
    assert payload["service_status_label"] == "监控运行中"
    assert payload["current_issues"] == [
        {
            "issue_id": payload["current_issues"][0]["issue_id"],
            "run_id": run_id,
            "occurred_at": payload["current_issues"][0]["occurred_at"],
            "scope": "BUY:ETH",
            "reason_code": "NO_ELIGIBLE_C2C_AD",
            "context": {},
            "state": "ACTIVE",
            "recovered_at": None,
            "classification": "EXPECTED_ABSENCE",
            "tone": "NOTICE",
        }
    ]


def test_view_keeps_real_partial_issue_local_and_explicit(tmp_path: Path) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    run_id = store.start_run("fake-monitor", started_at=now - timedelta(seconds=1))
    store.finish_run(
        run_id,
        "fake-monitor",
        CollectionBatch(
            samples=(
                MetricSample(
                    series_key="BUY|BTC",
                    entity_key="BTC",
                    observed_at=now,
                    value_text="6.75",
                    unit="CNY_PER_USDT",
                    payload={"asset": "BTC", "trade_type": "BUY", "value": "6.75"},
                ),
            ),
            issues=(CollectionIssue("BUY:ETH", "UPSTREAM_UNAVAILABLE"),),
        ),
        completed_at=now,
    )
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get("/api/view").json()

    assert payload["monitor"]["data_status"]["kind"] == "CURRENT_WITH_GAPS"
    assert payload["monitor"]["data_status"]["label"] == "最新采集已完成"
    assert payload["service_status_label"] == "监控运行中"
    assert payload["current_issues"][0]["classification"] == "DATA_ISSUE"
    assert payload["current_issues"][0]["tone"] == "WARNING"
    assert payload["current_issues"][0]["state"] == "ACTIVE"
    assert payload["current_issues"][0]["context"] == {}


def test_view_marks_an_old_issue_recovered_after_a_successful_run(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    issue_time = datetime.now(UTC) - timedelta(minutes=2)
    issue_run = store.start_run(
        "fake-monitor",
        started_at=issue_time - timedelta(seconds=1),
    )
    store.finish_run(
        issue_run,
        "fake-monitor",
        CollectionBatch(
            samples=(
                MetricSample(
                    series_key="BUY|BTC",
                    entity_key="BTC",
                    observed_at=issue_time,
                    value_text="6.75",
                    unit="CNY_PER_USDT",
                    payload={"asset": "BTC", "trade_type": "BUY", "value": "6.75"},
                ),
            ),
            issues=(
                CollectionIssue(
                    "stock-announcements",
                    "STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED",
                    context={"page_limit": 2, "upstream_total_hits": 238},
                ),
            ),
        ),
        completed_at=issue_time,
    )
    recovered_at = issue_time + timedelta(minutes=1)
    success_run = store.start_run("fake-monitor", started_at=recovered_at)
    store.finish_run(
        success_run,
        "fake-monitor",
        CollectionBatch(
            samples=(
                MetricSample(
                    series_key="BUY|BTC",
                    entity_key="BTC",
                    observed_at=recovered_at,
                    value_text="6.76",
                    unit="CNY_PER_USDT",
                    payload={"asset": "BTC", "trade_type": "BUY", "value": "6.76"},
                ),
            ),
        ),
        completed_at=recovered_at,
    )
    registry = MonitorRegistry()
    registry.register(FakeMonitor())

    with TestClient(
        create_app(store, registry, None, start_scheduler=False),
        base_url="http://127.0.0.1:8790",
    ) as client:
        payload = client.get("/api/view").json()

    assert payload["current_issues"] == []
    recovered = payload["issues"][0]
    assert recovered["run_id"] == issue_run
    assert recovered["state"] == "RECOVERED"
    assert recovered["recovered_at"] == iso_utc(recovered_at)
    assert recovered["context"] == {
        "page_limit": 2,
        "upstream_total_hits": 238,
    }


def test_view_keeps_last_finished_issue_active_while_next_run_is_running(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    issue_time = datetime.now(UTC) - timedelta(minutes=1)
    registry = MonitorRegistry()
    registry.register(FakeMonitor())

    with TestClient(
        create_app(store, registry, None, start_scheduler=False),
        base_url="http://127.0.0.1:8790",
    ) as client:
        issue_run = store.start_run(
            "fake-monitor",
            started_at=issue_time - timedelta(seconds=1),
        )
        store.finish_run(
            issue_run,
            "fake-monitor",
            CollectionBatch(
                samples=(
                    MetricSample(
                        series_key="BUY|BTC",
                        entity_key="BTC",
                        observed_at=issue_time,
                        value_text="6.75",
                        unit="CNY_PER_USDT",
                        payload={"asset": "BTC", "trade_type": "BUY", "value": "6.75"},
                    ),
                ),
                issues=(CollectionIssue("BUY:ETH", "UPSTREAM_UNAVAILABLE"),),
            ),
            completed_at=issue_time,
        )
        running_id = store.start_run(
            "fake-monitor",
            started_at=issue_time + timedelta(seconds=30),
        )
        payload = client.get("/api/view").json()

    assert payload["monitor"]["latest_run"]["run_id"] == running_id
    assert payload["monitor"]["latest_run"]["status"] == "RUNNING"
    assert payload["current_issues"][0]["run_id"] == issue_run
    assert payload["current_issues"][0]["state"] == "ACTIVE"


def test_view_keeps_empty_state_explicit_when_no_validated_data_exists(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get("/api/view").json()

    assert payload["rows"] == []
    assert payload["monitor"]["data_run"] is None
    assert payload["monitor"]["data_status"]["kind"] == "EMPTY"
    assert payload["monitor"]["data_status"]["label"] == "尚无采集结果"
    assert "未使用任何替代值" in payload["monitor"]["data_status"]["detail"]
    assert payload["service_status"] == "RUNNING"
    assert payload["service_status_label"] == "监控已启动"


def test_view_rejects_unknown_monitor_filter_and_window(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        unknown = client.get("/api/view", params={"monitor_id": "missing"})
        filter_error = client.get("/api/view", params={"trade_type": "UNKNOWN"})
        window_error = client.get("/api/view", params={"hours": 2})

    assert unknown.status_code == 404
    assert filter_error.status_code == 422
    assert window_error.status_code == 422


def test_view_accepts_expanded_history_windows(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        responses = [
            client.get("/api/view", params={"hours": hours})
            for hours in (3, 12, 72, 336, 720)
        ]

    assert all(response.status_code == 200 for response in responses)


def test_configuration_update_is_persisted_and_rejects_foreign_origin(
    tmp_path: Path,
) -> None:
    with make_client(tmp_path) as client:
        updated = client.put(
            "/api/monitors/fake-monitor/configuration",
            json={
                "values": {
                    "target_fiat": "2500",
                    "trade_methods": ["ALIPAY", "WECHAT"],
                }
            },
        )
        view = client.get("/api/view")
        rejected = client.put(
            "/api/monitors/fake-monitor/configuration",
            headers={"Origin": "https://example.com"},
            json={
                "values": {
                    "target_fiat": "3000",
                    "trade_methods": ["BANK"],
                }
            },
        )

    assert updated.status_code == 200
    assert updated.json()["refresh_requested"] is False
    assert view.json()["monitor"]["configuration"]["values"] == {
        "target_fiat": "2500",
        "trade_methods": ["ALIPAY", "WECHAT"],
    }
    assert rejected.status_code == 403


def test_monitor_can_be_disabled_and_reenabled_without_treating_history_as_fault(
    tmp_path: Path,
) -> None:
    with make_client(tmp_path) as client:
        disabled = client.put(
            "/api/monitors/fake-monitor/control",
            json={"enabled": False},
        )
        disabled_view = client.get("/api/view").json()
        enabled = client.put(
            "/api/monitors/fake-monitor/control",
            json={"enabled": True},
        )
        enabled_view = client.get("/api/view").json()

    assert disabled.status_code == 200
    assert disabled.json()["enabled"] is False
    assert disabled_view["service_status"] == "DISABLED"
    assert disabled_view["monitor"]["operational_status"] == {
        "kind": "DISABLED",
        "tone": "DISABLED",
        "label": "已关闭",
    }
    assert disabled_view["monitor"]["data_status"]["kind"] == "DISABLED_WITH_HISTORY"
    assert "不代表故障" in disabled_view["monitor"]["data_status"]["detail"]
    assert disabled_view["rows"][0]["value"] == "6.75"
    assert enabled.status_code == 200
    assert enabled.json()["enabled"] is True
    assert enabled_view["monitor"]["enabled"] is True


def test_view_splits_non_continuous_history_and_marks_the_uncollected_period(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    add_run(store, now - timedelta(minutes=20), "6.70")
    add_run(store, now - timedelta(minutes=10), "")
    add_run(store, now, "6.75")
    registry = MonitorRegistry()
    registry.register(FakeMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get("/api/view", params={"hours": 1}).json()

    assert [point["segment"] for point in payload["history"]] == [0, 1]
    assert [point["value"] for point in payload["history"]] == ["6.70", "6.75"]
    assert len(payload["collection_gaps"]) == 1
    assert payload["collection_gaps"][0]["label"] == "未采集时段"
    assert payload["collection_gaps"][0]["open"] is False


@dataclass
class FakeBuybackMonitor:
    monitor_id: str = "a-hk-buyback"
    display_name: str = "A股与港股通回购情报"
    description: str = "Buyback fixture"
    interval_seconds: float = 3600
    default_enabled: bool = True
    projection_kind: str = "buyback"
    view: MonitorView = AHKBuybackMonitor.view

    def collect(self) -> CollectionBatch:
        return CollectionBatch(samples=())


@dataclass
class ClosedBuybackMonitor(FakeBuybackMonitor):
    calls: int = 0

    def collect(self) -> CollectionBatch:
        self.calls += 1
        return CollectionBatch(samples=())

    def automatic_collection_state(
        self,
        *,
        now: datetime,
    ) -> AutomaticCollectionState:
        return AutomaticCollectionState(
            allowed=False,
            status="CLOSED",
            reason_code="FIXTURE_MARKETS_CLOSED",
            label="已收市 · 静态历史数据",
            detail="fixture closed",
            next_open_at=now + timedelta(hours=8),
        )


def seed_buyback(store: SQLiteMonitorStore, now: datetime) -> tuple[str, str]:
    body = b"%PDF-1.4\nfixture evidence\n%%EOF\n"
    digest = hashlib.sha256(body).hexdigest()
    entity_key = "A:SH:600000:web-fixture"
    source = BuybackSourceObservation(
        source_key="sse-announcements",
        source_label="上交所公告索引",
        status="SUCCESS",
        checked_at=now,
        source_time=now - timedelta(minutes=1),
        next_due_at=now + timedelta(hours=1),
        record_count=1,
        detail_code=None,
        payload={
            "window_start": "2026-08-01",
            "window_end": "2026-08-07",
            "target_candidate_count": 1,
            "codes": ["must-not-be-projected"],
        },
    )
    first_run = store.start_run("a-hk-buyback", started_at=now - timedelta(minutes=2))
    store.finish_run(
        first_run,
        "a-hk-buyback",
        CollectionBatch(
            samples=(),
            buyback_documents=(
                BuybackEvidenceDocument(
                    source_key="sse-announcements",
                    source_label="上交所公告索引",
                    source_document_id="web-document",
                    source_url="https://example.com/web-document.pdf",
                    published_at=now - timedelta(minutes=5),
                    observed_at=now - timedelta(minutes=2),
                    media_type="application/pdf",
                    file_suffix=".pdf",
                    body=body,
                    quality_state="VALID_PDF_TEXT",
                    metadata={"page_count": 1, "evidence_excerpt": "回购股份方案"},
                ),
            ),
            buyback_revisions=(
                BuybackEntityRevision(
                    entity_key=entity_key,
                    entity_type="DISCLOSURE_CANDIDATE",
                    effective_at=now - timedelta(minutes=5),
                    observed_at=now - timedelta(minutes=2),
                    source_key="sse-announcements",
                    document_sha256=digest,
                    payload={
                        "entity_key": entity_key,
                        "market_scope": "A_SHARE",
                        "market_label": "沪市 A 股",
                        "stock_code": "600000",
                        "issuer_name": "Fixture Issuer",
                        "title": "关于首次回购股份的公告",
                        "event_type": "FIRST_EXECUTION",
                        "event_type_label": "首次实施",
                        "effective_at": now.isoformat(),
                        "source_url": "https://example.com/web-document.pdf",
                        "document_quality": "VALID_PDF_TEXT",
                        "review_status": "UNREVIEWED",
                        "review_status_label": "待复核",
                        "connect_status": "NOT_APPLICABLE",
                        "connect_status_label": "不适用",
                        "connect_route_label": "—",
                        "data_quality_label": "原文可读 · 待人工确认",
                        "no_action_reason": "MANUAL_CONFIRMATION_REQUIRED",
                        "detail_action": "查看 / 复核",
                    },
                ),
                BuybackEntityRevision(
                    entity_key="A:SH:600001:web-pending-fixture",
                    entity_type="DISCLOSURE_CANDIDATE",
                    effective_at=now - timedelta(minutes=6),
                    observed_at=now - timedelta(minutes=2),
                    source_key="sse-announcements",
                    document_sha256=None,
                    payload={
                        "entity_key": "A:SH:600001:web-pending-fixture",
                        "market_scope": "A_SHARE",
                        "market_label": "沪市 A 股",
                        "stock_code": "600001",
                        "issuer_name": "Pending Fixture",
                        "title": "关于回购股份进展的公告",
                        "event_type": "PROGRESS",
                        "event_type_label": "实施进展",
                        "effective_at": now.isoformat(),
                        "source_url": "https://example.com/pending.pdf",
                        "document_quality": "INDEX_ONLY",
                        "review_status": "UNREVIEWED",
                        "connect_status": "NOT_APPLICABLE",
                        "connect_status_label": "不适用",
                        "connect_route_label": "—",
                    },
                ),
                BuybackEntityRevision(
                    entity_key="A:SZ:200512:web-b-share-fixture",
                    entity_type="DISCLOSURE_CANDIDATE",
                    effective_at=now - timedelta(minutes=7),
                    observed_at=now - timedelta(minutes=2),
                    source_key="cninfo-sz-announcements",
                    document_sha256=digest,
                    payload={
                        "entity_key": "A:SZ:200512:web-b-share-fixture",
                        "market_scope": "A_SHARE",
                        "market": "SZ",
                        "market_label": "深市",
                        "stock_code": "200512",
                        "issuer_name": "B Share Fixture",
                        "title": "关于回购公司股份方案的公告",
                        "event_type": "PLAN_OR_APPROVAL",
                        "event_type_label": "方案 / 审议",
                        "effective_at": now.isoformat(),
                        "source_url": "https://example.com/b-share.pdf",
                        "document_quality": "VALID_PDF_TEXT",
                        "review_status": "UNREVIEWED",
                        "connect_status": "NOT_APPLICABLE",
                        "connect_status_label": "不适用",
                        "connect_route_label": "—",
                    },
                ),
            ),
            buyback_source_observations=(source,),
        ),
        completed_at=now - timedelta(minutes=2),
    )
    second_run = store.start_run("a-hk-buyback", started_at=now)
    store.finish_run(
        second_run,
        "a-hk-buyback",
        CollectionBatch(
            samples=(),
            buyback_source_observations=(source,),
        ),
        completed_at=now,
    )
    return entity_key, digest


def make_buyback_client(tmp_path: Path) -> tuple[TestClient, str, str]:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    entity_key, digest = seed_buyback(store, now)
    registry = MonitorRegistry()
    registry.register(FakeBuybackMonitor())
    app = create_app(store, registry, None, start_scheduler=False)
    return TestClient(app, base_url="http://127.0.0.1:8790"), entity_key, digest


def test_closed_buyback_view_is_static_and_uses_a_long_refresh_timer(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    seed_buyback(store, datetime.now(UTC))
    registry = MonitorRegistry()
    registry.register(ClosedBuybackMonitor())
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback"},
        ).json()

    assert payload["monitor"]["automatic_collection"]["status"] == "CLOSED"
    assert payload["monitor"]["operational_status"]["kind"] == "SCHEDULED_IDLE"
    assert payload["monitor"]["operational_status"]["tone"] == "IDLE"
    assert payload["monitor"]["data_status"]["kind"] == "STATIC_CLOSED"
    assert payload["refresh_after_seconds"] > 15
    assert "source_url" not in payload["rows"][0]


def test_manual_refresh_endpoint_bypasses_closed_schedule_once(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    registry = MonitorRegistry()
    monitor = ClosedBuybackMonitor()
    registry.register(monitor)
    scheduler = MonitorScheduler(registry, store)
    scheduler.start()
    app = create_app(store, registry, scheduler, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        accepted = client.post("/api/monitors/a-hk-buyback/refresh")
        rejected_origin = client.post(
            "/api/monitors/a-hk-buyback/refresh",
            headers={"Origin": "https://example.com"},
        )
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            latest = store.latest_run("a-hk-buyback")
            if latest is not None and latest.status != "RUNNING":
                break
            time.sleep(0.02)
        scheduler.set_enabled("a-hk-buyback", False)
        disabled = client.post("/api/monitors/a-hk-buyback/refresh")
    scheduler.stop()

    assert accepted.status_code == 200
    assert accepted.json()["status"] == "ACCEPTED"
    assert accepted.json()["manual"] is True
    assert rejected_origin.status_code == 403
    assert disabled.status_code == 409
    assert monitor.calls == 1


def test_buyback_projection_is_cached_for_repeated_view_reads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client, _entity_key, _digest = make_buyback_client(tmp_path)
    from halpha_monitor import web as web_module

    calls = 0
    original = web_module.project_buyback_metrics

    def counted_projection(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(web_module, "project_buyback_metrics", counted_projection)
    clock = [datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)]
    monkeypatch.setattr(web_module, "utc_now", lambda: clock[0])
    with client:
        first = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback"},
        )
        second = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback"},
        )
        clock[0] += timedelta(minutes=5)
        third = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback"},
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 200
    assert calls == 1


def test_buyback_view_uses_durable_entities_after_a_valid_empty_event_scan(
    tmp_path: Path,
) -> None:
    client, entity_key, _digest = make_buyback_client(tmp_path)
    with client:
        payload = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback"},
        ).json()
        hk_payload = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "market_scope": "HK"},
        ).json()
        code_search = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "stock_query": "６０００００"},
        ).json()
        name_search = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "stock_query": "fixtureissuer"},
        ).json()
        pending_search = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "stock_query": "Pending Fixture"},
        ).json()
        unmatched_search = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "stock_query": "不存在的股票"},
        ).json()
        oversized_search = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "stock_query": "a" * 65},
        )
        priority_payload = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "attention_level": "PRIORITY"},
        ).json()
        tracking_payload = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "attention_level": "TRACKING"},
        ).json()
        unsupported_attention = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "attention_level": "URGENT"},
        )
        not_buyable_payload = client.get(
            "/api/view",
            params={
                "monitor_id": "a-hk-buyback",
                "connect_status": "NOT_BUY_ELIGIBLE",
            },
        ).json()
        both_purchase_states = client.get(
            "/api/view",
            params=[
                ("monitor_id", "a-hk-buyback"),
                ("connect_status", "BUY_ELIGIBLE"),
                ("connect_status", "NOT_BUY_ELIGIBLE"),
            ],
        ).json()
        unsupported_purchase_state = client.get(
            "/api/view",
            params={"monitor_id": "a-hk-buyback", "connect_status": "UNKNOWN"},
        )

    assert payload["monitor"]["projection_kind"] == "buyback"
    assert payload["monitor"]["data_status"]["kind"] == "CURRENT"
    assert payload["monitor"]["data_run"]["sample_count"] == 0
    assert [item["key"] for item in payload["monitor"]["filters"]] == [
        "market_scope",
        "event_type",
        "attention_level",
        "connect_status",
    ]
    event_filter = next(
        item for item in payload["monitor"]["filters"] if item["key"] == "event_type"
    )
    assert all(choice["description"] for choice in event_filter["choices"])
    assert "AMBIGUOUS_BUYBACK" not in {
        choice["value"] for choice in event_filter["choices"]
    }
    attention_filter = next(
        item
        for item in payload["monitor"]["filters"]
        if item["key"] == "attention_level"
    )
    assert [choice["value"] for choice in attention_filter["choices"]] == [
        "*",
        "PRIORITY",
        "TRACKING",
        "UPDATE",
    ]
    assert all(choice["description"] for choice in attention_filter["choices"])
    purchase_filter = next(
        item
        for item in payload["monitor"]["filters"]
        if item["key"] == "connect_status"
    )
    assert purchase_filter["label"] == "购买资格"
    assert purchase_filter["multiple"] is True
    assert purchase_filter["selected"] == ["BUY_ELIGIBLE"]
    assert [choice["value"] for choice in purchase_filter["choices"]] == [
        "BUY_ELIGIBLE",
        "NOT_BUY_ELIGIBLE",
    ]
    assert [choice["label"] for choice in purchase_filter["choices"]] == [
        "可购买",
        "不可购买",
    ]
    assert all(choice["description"] for choice in purchase_filter["choices"])
    assert payload["monitor"]["selected_filters"]["connect_status"] == ["BUY_ELIGIBLE"]
    assert payload["monitor"]["method_note"] is None
    assert payload["monitor"]["table_title"] == "回购情报清单"
    assert "首次实施" in payload["monitor"]["columns"][0]["description"]
    assert [item["label"] for item in payload["monitor"]["columns"]] == [
        "关注分类",
        "证券",
        "涨跌幅",
        "回购吸引力",
        "年度ROE",
        "营收同比",
        "净利同比",
        "实际执行天数",
        "累计股数",
        "累计金额",
        "回购均价",
        "现价",
        "现价/均价",
        "回购/市值",
        "回购情报",
        "日期",
    ]
    assert "证据" not in [item["label"] for item in payload["monitor"]["columns"]]
    assert "购买资格" not in [item["label"] for item in payload["monitor"]["columns"]]
    assert "核验" not in [item["label"] for item in payload["monitor"]["columns"]]
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["entity_key"] == entity_key
    assert payload["rows"][0]["review_status"] == "UNREVIEWED"
    assert payload["rows"][0]["connect_status"] == "BUY_ELIGIBLE"
    assert payload["rows"][0]["connect_status_label"] == "可购买"
    assert payload["rows"][0]["scale_status"] == "MISSING"
    assert payload["rows"][0]["scale_label"] == "规模未结构化"
    assert payload["buyback"]["intelligence_count"] == 1
    assert payload["buyback"]["priority_count"] == 1
    assert payload["buyback"]["high_attractiveness_count"] == 0
    assert "buy_eligible_count" not in payload["buyback"]
    assert payload["buyback"]["pending_count"] == 1
    assert payload["buyback"]["excluded_count"] == 1
    assert hk_payload["rows"] == []
    assert hk_payload["buyback"]["intelligence_count"] == 0
    assert code_search["buyback"]["stock_query"] == "６０００００"
    assert [row["entity_key"] for row in code_search["rows"]] == [entity_key]
    assert [row["entity_key"] for row in name_search["rows"]] == [entity_key]
    assert name_search["buyback"]["intelligence_count"] == 1
    assert pending_search["rows"] == []
    assert pending_search["buyback"]["intelligence_count"] == 0
    assert unmatched_search["rows"] == []
    assert unmatched_search["buyback"]["fresh_intelligence_count"] == 0
    assert oversized_search.status_code == 422
    assert [row["entity_key"] for row in priority_payload["rows"]] == [entity_key]
    assert priority_payload["buyback"]["priority_count"] == 1
    assert tracking_payload["rows"] == []
    assert tracking_payload["buyback"]["priority_count"] == 0
    assert unsupported_attention.status_code == 422
    assert not_buyable_payload["rows"] == []
    assert both_purchase_states["monitor"]["selected_filters"]["connect_status"] == [
        "BUY_ELIGIBLE",
        "NOT_BUY_ELIGIBLE",
    ]
    assert [row["entity_key"] for row in both_purchase_states["rows"]] == [entity_key]
    assert unsupported_purchase_state.status_code == 422
    source = payload["buyback"]["source_states"][0]
    assert source["status"] == "SUCCESS"
    assert "codes" not in source["summary"]


def test_buyback_detail_document_and_review_form_a_complete_local_loop(
    tmp_path: Path,
) -> None:
    client, entity_key, digest = make_buyback_client(tmp_path)
    with client:
        detail = client.get(f"/api/buybacks/entities/{entity_key}")
        evidence = client.get(f"/api/buybacks/documents/{digest}")
        reviewed = client.post(
            f"/api/buybacks/entities/{entity_key}/reviews",
            json={
                "base_revision_no": 1,
                "decision": "CONFIRMED_EVENT",
                "corrected_event_type": "PLAN_OR_APPROVAL",
                "program_key": "600000-2026-08",
                "program_status": "PROPOSED",
                "note": "已核对正式公告原文。",
            },
        )
        removed_review_filter = client.get(
            "/api/view",
            params={
                "monitor_id": "a-hk-buyback",
                "review_status": "CONFIRMED_EVENT",
            },
        )
        rejected_origin = client.post(
            f"/api/buybacks/entities/{entity_key}/reviews",
            headers={"Origin": "https://example.com"},
            json={
                "base_revision_no": 1,
                "decision": "REJECTED_EVENT",
                "note": "foreign",
            },
        )

    assert detail.status_code == 200
    assert detail.json()["document"]["quality_state"] == "VALID_PDF_TEXT"
    assert detail.json()["document"]["metadata"]["evidence_excerpt"] == "回购股份方案"
    assert evidence.status_code == 200
    assert evidence.content.startswith(b"%PDF-")
    assert "inline" in evidence.headers["content-disposition"]
    assert reviewed.status_code == 200
    reviewed_entity = reviewed.json()["entity"]
    assert reviewed_entity["review_status"] == "CONFIRMED_EVENT"
    assert reviewed_entity["program_key"] == "600000-2026-08"
    assert removed_review_filter.status_code == 200
    assert removed_review_filter.json()["rows"][0]["review_status"] == "CONFIRMED_EVENT"
    assert rejected_origin.status_code == 403


def _event_sample(
    entity_key: str,
    title: str,
    scheduled_at: datetime,
    *,
    importance: str,
    category: str,
    category_label: str,
    markets: list[str],
    latest_result: str | None = None,
) -> MetricSample:
    shanghai = scheduled_at.astimezone(ZoneInfo("Asia/Shanghai"))
    return MetricSample(
        series_key="market-event",
        entity_key=entity_key,
        observed_at=scheduled_at - timedelta(hours=12),
        value_text="1",
        unit="event",
        payload={
            "row_type": "EVENT",
            "event_title": title,
            "event_key": entity_key,
            "category": category,
            "category_label": category_label,
            "importance": importance,
            "importance_rank": 2 if importance == "HIGH" else 1,
            "impact_reason": "用于网页投影测试的影响说明。",
            "event_description": "用于网页投影测试的事件说明。",
            "market_scopes": markets,
            "scheduled_at": scheduled_at.isoformat(),
            "scheduled_date": scheduled_at.date().isoformat(),
            "scheduled_sort_at": scheduled_at.isoformat(),
            "schedule_label": shanghai.strftime("%Y-%m-%d %H:%M"),
            "time_precision": "EXACT",
            "source_key": "fixture-calendar",
            "source_label": "官方测试日历",
            "schedule_source_url": "https://example.com/calendar",
            "official_release_url": "https://example.com/release",
            "source_timezone_label": "北京时间",
            "source_timezone": "America/New_York",
            "source_checked_at": (scheduled_at - timedelta(hours=12)).isoformat(),
            "schedule_change_count": 0,
            "last_schedule_changed_at": None,
            "previous_schedule_label": None,
            "latest_result": latest_result,
            "latest_result_period": "2026年7月" if latest_result else None,
        },
    )


def make_market_event_client(tmp_path: Path) -> TestClient:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    historical = _event_sample(
        "nyfed:employment:history",
        "美国非农就业报告",
        now - timedelta(hours=1),
        importance="HIGH",
        category="LABOR",
        category_label="就业",
        markets=["CRYPTO", "US_STOCKS", "A_STOCKS", "HK_STOCKS"],
    )
    historical.payload.update(
        {
            "release_state": "RELEASED",
            "release_state_label": "已公布",
            "expectation_summary": "新增非农 8.5万人 · 失业率 4.2%",
            "actual_summary": "新增非农 +11.0万人 · 失业率 4.0%",
            "surprise_summary": "新增非农 +25千人 · 失业率 -0.2%",
            "direction": {
                "label": "偏空",
                "tone": "BEARISH",
                "scope": "风险资产短线",
                "score": -1.03,
                "threshold": "≥ +0.5偏多；≤ -0.5偏空；其余中性",
            },
        }
    )
    run_id = store.start_run("market-event-calendar", started_at=now)
    store.finish_run(
        run_id,
        "market-event-calendar",
        CollectionBatch(
            samples=(
                _event_sample(
                    "nyfed:cpi:current",
                    "美国CPI",
                    now + timedelta(hours=6),
                    importance="HIGH",
                    category="INFLATION",
                    category_label="通胀",
                    markets=["CRYPTO", "US_STOCKS", "A_STOCKS", "HK_STOCKS"],
                    latest_result="同比 +2.8% · 环比 +0.2%",
                ),
                _event_sample(
                    "nyfed:housing:current",
                    "美国新屋销售",
                    now + timedelta(days=2),
                    importance="MEDIUM",
                    category="HOUSING",
                    category_label="房地产",
                    markets=["US_STOCKS"],
                ),
                _event_sample(
                    "fomc:decision:current",
                    "美联储利率决议",
                    now + timedelta(days=20),
                    importance="HIGH",
                    category="MONETARY_POLICY",
                    category_label="货币政策",
                    markets=["CRYPTO", "US_STOCKS", "A_STOCKS", "HK_STOCKS"],
                ),
                MetricSample(
                    series_key="macro-indicator",
                    entity_key="indicator:cpi",
                    observed_at=now,
                    value_text="同比 +2.8%",
                    unit="official release",
                    payload={
                        "row_type": "INDICATOR",
                        "indicator_key": "CPI",
                        "indicator_label": "美国CPI",
                        "primary_value": "同比 +2.8%",
                        "secondary_value": "环比 +0.2%",
                        "period_label": "2026年7月",
                        "source_label": "美国劳工统计局",
                        "source_url": "https://www.bls.gov/cpi/",
                        "method_label": "CPI-U未季调同比；CPI-U季调环比",
                        "source_checked_at": now.isoformat(),
                    },
                ),
            ),
            issues=(
                CollectionIssue(
                    scope="bls-macro-data",
                    reason_code="MARKET_EVENTS_BLS_RESPONSE_FAILED",
                ),
            ),
            market_event_revisions=(
                MarketEventRevision(
                    event_key=historical.entity_key,
                    scheduled_at=now - timedelta(hours=1),
                    observed_at=now - timedelta(hours=2),
                    state="RELEASED",
                    payload=historical.payload,
                ),
            ),
        ),
        completed_at=now,
    )
    registry = MonitorRegistry()
    registry.register(MarketEventMonitor(store=store))
    app = create_app(store, registry, None, start_scheduler=False)
    return TestClient(app, base_url="http://127.0.0.1:8790")


def test_market_event_projection_prioritizes_upcoming_risk_and_filters(
    tmp_path: Path,
) -> None:
    with make_market_event_client(tmp_path) as client:
        payload = client.get(
            "/api/view",
            params={"monitor_id": "market-event-calendar"},
        ).json()
        next_day = client.get(
            "/api/view",
            params={
                "monitor_id": "market-event-calendar",
                "time_range": "NEXT_24H",
            },
        ).json()
        a_h_markets = client.get(
            "/api/view",
            params=[
                ("monitor_id", "market-event-calendar"),
                ("affected_market", "A_STOCKS"),
                ("affected_market", "HK_STOCKS"),
            ],
        ).json()
        searched = client.get(
            "/api/view",
            params={
                "monitor_id": "market-event-calendar",
                "event_query": "房地产",
            },
        ).json()
        unsupported = client.get(
            "/api/view",
            params={
                "monitor_id": "market-event-calendar",
                "importance": "CRITICAL",
            },
        )

    assert payload["monitor"]["projection_kind"] == "market_events"
    assert payload["market_events"]["event_count"] == 3
    assert payload["market_events"]["next_24h_count"] == 1
    assert payload["market_events"]["attention_count"] == 1
    assert payload["market_events"]["attention_events"][0]["event_title"] == "美国CPI"
    assert payload["market_events"]["indicators"][0]["primary_value"] == "同比 +2.8%"
    assert payload["market_events"]["coverage_messages"] == [
        "最近CPI与就业数据暂时无法更新，事件时间仍可使用"
    ]
    assert len(payload["market_events"]["calendar_days"]) == 14
    assert payload["market_events"]["history_event_count"] == 1
    assert payload["market_events"]["history_events"][0]["direction"]["label"] == "偏空"
    assert payload["market_events"]["history_started_at"] is not None
    market_filter = next(
        item
        for item in payload["monitor"]["filters"]
        if item["key"] == "affected_market"
    )
    assert market_filter["multiple"] is True
    assert market_filter["selected"] == [
        "US_STOCKS",
        "A_STOCKS",
        "HK_STOCKS",
        "CRYPTO",
    ]
    assert [row["event_title"] for row in next_day["rows"]] == ["美国CPI"]
    assert [row["event_title"] for row in a_h_markets["rows"]] == [
        "美国CPI",
        "美联储利率决议",
    ]
    assert "A股、港股" in a_h_markets["rows"][0]["markets_label"]
    assert a_h_markets["monitor"]["filters"][3]["selected"] == [
        "A_STOCKS",
        "HK_STOCKS",
    ]
    assert [row["event_title"] for row in searched["rows"]] == ["美国新屋销售"]
    assert unsupported.status_code == 422


def test_market_event_valid_empty_scan_does_not_reuse_an_older_calendar(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    now = datetime.now(UTC)
    first_run = store.start_run("market-event-calendar", started_at=now)
    store.finish_run(
        first_run,
        "market-event-calendar",
        CollectionBatch(
            samples=(
                _event_sample(
                    "fixture:old",
                    "旧事件",
                    now + timedelta(days=1),
                    importance="HIGH",
                    category="INFLATION",
                    category_label="通胀",
                    markets=["CRYPTO"],
                ),
            )
        ),
        completed_at=now,
    )
    empty_run = store.start_run(
        "market-event-calendar",
        started_at=now + timedelta(seconds=1),
    )
    store.finish_run(
        empty_run,
        "market-event-calendar",
        CollectionBatch(samples=()),
        completed_at=now + timedelta(seconds=1),
    )
    registry = MonitorRegistry()
    registry.register(MarketEventMonitor(store=store))
    app = create_app(store, registry, None, start_scheduler=False)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        payload = client.get(
            "/api/view",
            params={"monitor_id": "market-event-calendar"},
        ).json()

    assert payload["monitor"]["data_run"]["run_id"] == empty_run
    assert payload["rows"] == []
    assert payload["market_events"]["event_count"] == 0
