from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from halpha_monitor.contracts import (
    CollectionBatch,
    CollectionIssue,
    ConfigurationField,
    FilterChoice,
    MetricSample,
    MonitorView,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.service import MonitorRegistry
from halpha_monitor.service import MonitorScheduler
from halpha_monitor.store import SQLiteMonitorStore
from halpha_monitor.web import create_app


@dataclass
class FakeMonitor:
    monitor_id: str = "fake-monitor"
    display_name: str = "Fixture Monitor"
    description: str = "Fixture description"
    interval_seconds: float = 60
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
    assert 'id="quote-scroll"' in page.text
    assert 'id="table-scroll-controls"' in page.text
    assert 'id="monitor-method-note"' in page.text
    assert 'id="time-window-label"' in page.text
    assert "历史范围" in page.text
    assert "payload.current_issues" in script.text
    assert "function sortTableRows" in script.text
    assert 'button.setAttribute("aria-description", column.description)' in script.text
    assert "function monitorIdFromLocation" in script.text
    assert "window.history.replaceState" in script.text
    assert "https://www.binance.com/zh-CN/futures/" in script.text
    assert "https://www.binance.com/zh-CN/trade/" in script.text
    assert "#quote-scroll thead th" in style.text
    assert '.monitor-link-status[data-status="ACTIVE"]' in style.text
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
    assert payload["monitor"]["columns"][1]["description"] == (
        "经过校验的核算价格。"
    )
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
        setup_rows = client.get(
            "/api/view", params={"stage": "SETUP"}
        ).json()["rows"]

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
    assert payload["monitor"]["data_status"]["label"] == (
        "正在刷新 · 显示上一轮结果"
    )
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
