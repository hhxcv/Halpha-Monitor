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
            ViewColumn("value", "核算价", "number"),
            ViewColumn("observed_at", "采集时间", "time"),
        ),
        chart_title="核算价历史",
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

    assert page.status_code == 200
    assert "Halpha Monitor" in page.text
    assert script.status_code == 200
    assert page.headers["x-frame-options"] == "DENY"
    assert "default-src 'self'" in page.headers["content-security-policy"]
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
    assert payload["monitor"]["configuration"]["values"]["target_fiat"] == "2000"
    assert payload["monitor"]["data_status"] == {
        "kind": "CURRENT",
        "tone": "HEALTHY",
        "label": "当前数据可用",
        "detail": "展示字段均已通过校验；无需用户处理。",
        "cutoff_label": "数据截止",
    }
    assert payload["rows"][0]["value"] == "6.75"
    assert [point["value"] for point in payload["history"]] == ["6.70", "6.75"]
    assert payload["collection_gaps"] == []


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
    assert payload["monitor"]["data_status"]["label"] == "历史数据 · 本轮无新数据"
    assert "不代表当前" in payload["monitor"]["data_status"]["detail"]
    assert payload["service_status_label"] == "存在监控本轮无新数据"


def test_view_marks_partial_run_as_validated_current_data_with_gaps(
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
    assert data_status["kind"] == "CURRENT_WITH_GAPS"
    assert data_status["label"] == "当前数据可用"
    assert "缺失字段保持为空" in data_status["detail"]
    assert payload["service_status_label"] == "当前数据可用 · 部分来源异常"


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
    assert payload["monitor"]["data_status"]["label"] == "暂无可用数据"
    assert "未使用任何替代值" in payload["monitor"]["data_status"]["detail"]
    assert payload["service_status_label"] == "暂无可用数据"


def test_view_rejects_unknown_monitor_filter_and_window(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        unknown = client.get("/api/view", params={"monitor_id": "missing"})
        filter_error = client.get("/api/view", params={"trade_type": "UNKNOWN"})
        window_error = client.get("/api/view", params={"hours": 2})

    assert unknown.status_code == 404
    assert filter_error.status_code == 422
    assert window_error.status_code == 422


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
