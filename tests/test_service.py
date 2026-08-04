import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from halpha_monitor.contracts import (
    CollectionBatch,
    MetricSample,
    MonitorView,
)
from halpha_monitor.service import MonitorRegistry, MonitorScheduler
from halpha_monitor.store import SQLiteMonitorStore


@dataclass
class FakeMonitor:
    monitor_id: str
    fail: bool = False
    display_name: str = "Fake"
    description: str = "Fake monitor"
    interval_seconds: float = 15
    default_enabled: bool = True
    view: MonitorView = MonitorView(filters=(), columns=(), chart_title="History")
    calls: int = 0

    def collect(self) -> CollectionBatch:
        self.calls += 1
        if self.fail:
            raise RuntimeError("fixture failure")
        return CollectionBatch(
            samples=(
                MetricSample(
                    series_key=f"{self.monitor_id}|series",
                    entity_key="entity",
                    observed_at=datetime.now(UTC),
                    value_text="1.25",
                    unit="UNIT",
                    payload={"value": "1.25"},
                ),
            )
        )


def make_store(tmp_path: Path) -> SQLiteMonitorStore:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    return store


def test_registry_rejects_duplicate_ids() -> None:
    registry = MonitorRegistry()
    registry.register(FakeMonitor("fake-monitor"))

    with pytest.raises(ValueError, match="MONITOR_ID_DUPLICATE"):
        registry.register(FakeMonitor("fake-monitor"))


def test_one_monitor_failure_does_not_block_another(tmp_path: Path) -> None:
    registry = MonitorRegistry()
    registry.register(FakeMonitor("healthy-monitor"))
    registry.register(FakeMonitor("failed-monitor", fail=True))
    store = make_store(tmp_path)
    scheduler = MonitorScheduler(registry, store)

    scheduler.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        healthy = store.latest_run("healthy-monitor")
        failed = store.latest_run("failed-monitor")
        if healthy is not None and failed is not None:
            break
        time.sleep(0.02)
    scheduler.stop()

    healthy = store.latest_run("healthy-monitor")
    failed = store.latest_run("failed-monitor")
    assert healthy is not None and healthy.status == "SUCCESS"
    assert failed is not None and failed.status == "FAILED"
    assert failed.error_code == "COLLECTION_FAILED_RUNTIMEERROR"


def test_request_run_wakes_only_requested_monitor(tmp_path: Path) -> None:
    requested = FakeMonitor("requested-monitor", interval_seconds=3600)
    untouched = FakeMonitor("untouched-monitor", interval_seconds=3600)
    registry = MonitorRegistry()
    registry.register(requested)
    registry.register(untouched)
    scheduler = MonitorScheduler(registry, make_store(tmp_path))
    scheduler.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and (requested.calls < 1 or untouched.calls < 1):
        time.sleep(0.02)

    assert scheduler.request_run("requested-monitor") is True
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and requested.calls < 2:
        time.sleep(0.02)
    scheduler.stop()

    assert requested.calls == 2
    assert untouched.calls == 1


def test_disabled_monitor_waits_until_enabled_and_then_collects(
    tmp_path: Path,
) -> None:
    monitor = FakeMonitor(
        "disabled-monitor",
        interval_seconds=3600,
        default_enabled=False,
    )
    registry = MonitorRegistry()
    registry.register(monitor)
    scheduler = MonitorScheduler(registry, make_store(tmp_path))
    scheduler.start()
    time.sleep(0.05)

    assert monitor.calls == 0
    assert scheduler.request_run("disabled-monitor") is False
    control = scheduler.set_enabled("disabled-monitor", True)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and monitor.calls < 1:
        time.sleep(0.02)
    scheduler.set_enabled("disabled-monitor", False)
    scheduler.stop()

    assert control.enabled is True
    assert monitor.calls == 1


def test_stop_reports_a_collector_that_did_not_exit_before_timeout(
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    @dataclass
    class BlockingMonitor(FakeMonitor):
        monitor_id: str = "blocking-monitor"

        def collect(self) -> CollectionBatch:
            entered.set()
            release.wait(timeout=2)
            return super().collect()

    registry = MonitorRegistry()
    registry.register(BlockingMonitor())
    scheduler = MonitorScheduler(
        registry,
        make_store(tmp_path),
        stop_timeout_seconds=0.01,
    )
    scheduler.start()
    assert entered.wait(timeout=1)

    with pytest.raises(RuntimeError, match="MONITOR_STOP_TIMEOUT"):
        scheduler.stop()

    release.set()
    scheduler.stop_timeout_seconds = 1
    scheduler.stop()


def test_global_retention_maintenance_runs_once_per_interval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = make_store(tmp_path)
    calls: list[int] = []
    current_time = [100.0]
    monkeypatch.setattr(store, "prune", calls.append)
    monkeypatch.setattr(
        "halpha_monitor.service.time.monotonic",
        lambda: current_time[0],
    )
    registry = MonitorRegistry()
    first = FakeMonitor("first-monitor")
    second = FakeMonitor("second-monitor")
    registry.register(first)
    registry.register(second)
    scheduler = MonitorScheduler(
        registry,
        store,
        retention_days=30,
        maintenance_interval_seconds=3600,
    )

    scheduler.run_once(first)
    scheduler.run_once(second)
    current_time[0] += 3600
    scheduler.run_once(second)

    assert calls == [30, 30]


def test_maintenance_failure_does_not_change_a_completed_monitor_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = make_store(tmp_path)
    calls = 0

    def fail_prune(_retention_days: int) -> int:
        nonlocal calls
        calls += 1
        raise RuntimeError("fixture maintenance failure")

    monkeypatch.setattr(store, "prune", fail_prune)
    monitor = FakeMonitor("healthy-monitor")
    registry = MonitorRegistry()
    registry.register(monitor)
    scheduler = MonitorScheduler(registry, store)

    scheduler.run_once(monitor)
    scheduler.run_once(monitor)

    latest = store.latest_run(monitor.monitor_id)
    assert latest is not None
    assert latest.status == "SUCCESS"
    assert calls == 1
