import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from halpha_monitor.contracts import (
    AutomaticCollectionState,
    CollectionBatch,
    CollectionCancelled,
    ForwardEvaluationCase,
    ForwardEvaluationResult,
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
    foreground_interval_seconds: float | None = None
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


def test_registry_rejects_foreground_interval_longer_than_background() -> None:
    registry = MonitorRegistry()

    with pytest.raises(
        ValueError,
        match="MONITOR_FOREGROUND_INTERVAL_EXCEEDS_BACKGROUND",
    ):
        registry.register(
            FakeMonitor(
                "adaptive-monitor",
                interval_seconds=60,
                foreground_interval_seconds=300,
            )
        )


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
    issue = store.issues_for_run(failed.run_id)[0]
    assert issue.context["exception_type"] == "RuntimeError"
    assert str(issue.context["origin_module"]).endswith("test_service")
    assert issue.context["origin_function"] == "collect"
    assert int(issue.context["origin_line"]) > 0
    assert "fixture failure" not in str(issue.context)


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


def test_visible_observation_requests_due_run_and_expires_to_background(
    tmp_path: Path,
) -> None:
    monitor = FakeMonitor(
        "adaptive-monitor",
        interval_seconds=3600,
        foreground_interval_seconds=15,
    )
    registry = MonitorRegistry()
    registry.register(monitor)
    store = make_store(tmp_path)
    scheduler = MonitorScheduler(registry, store)
    scheduler.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and monitor.calls < 1:
        time.sleep(0.02)

    stale_completed_at = datetime.now(UTC) - timedelta(seconds=20)
    stale_run = store.start_run(
        monitor.monitor_id,
        started_at=stale_completed_at - timedelta(seconds=1),
    )
    store.finish_run(
        stale_run,
        monitor.monitor_id,
        CollectionBatch(samples=()),
        completed_at=stale_completed_at,
    )
    observed = scheduler.observe_monitor(
        monitor.monitor_id,
        lease_seconds=0.1,
    )
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and monitor.calls < 2:
        time.sleep(0.02)

    assert observed.refresh_requested is True
    assert observed.cadence.foreground_active is True
    assert observed.cadence.effective_interval_seconds == 15
    assert monitor.calls == 2
    time.sleep(0.12)
    expired = scheduler.collection_cadence(monitor.monitor_id)
    scheduler.stop()

    assert expired.foreground_active is False
    assert expired.effective_interval_seconds == 3600


def test_scheduled_monitor_is_static_when_closed_but_manual_run_bypasses_gate(
    tmp_path: Path,
) -> None:
    @dataclass
    class ScheduledMonitor(FakeMonitor):
        monitor_id: str = "scheduled-monitor"

        def automatic_collection_state(
            self,
            *,
            now: datetime,
        ) -> AutomaticCollectionState:
            return AutomaticCollectionState(
                allowed=False,
                status="CLOSED",
                reason_code="FIXTURE_CLOSED",
                label="closed",
                detail="fixture closed",
                next_open_at=now + timedelta(hours=8),
            )

    monitor = ScheduledMonitor(interval_seconds=3600)
    registry = MonitorRegistry()
    registry.register(monitor)
    scheduler = MonitorScheduler(registry, make_store(tmp_path))
    scheduler.start()
    time.sleep(0.05)

    assert monitor.calls == 0
    assert scheduler.request_run(monitor.monitor_id) is True
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and monitor.calls < 1:
        time.sleep(0.02)
    scheduler.stop()

    assert monitor.calls == 1


def test_transient_control_read_failure_does_not_kill_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monitor = FakeMonitor("resilient-monitor", interval_seconds=3600)
    registry = MonitorRegistry()
    registry.register(monitor)
    store = make_store(tmp_path)
    original_is_enabled = store.is_enabled
    failure_seen = threading.Event()

    def flaky_is_enabled(monitor_id: str) -> bool:
        if not failure_seen.is_set():
            failure_seen.set()
            raise OSError("fixture transient read failure")
        return original_is_enabled(monitor_id)

    monkeypatch.setattr(store, "is_enabled", flaky_is_enabled)
    scheduler = MonitorScheduler(registry, store)
    scheduler.start()
    assert failure_seen.wait(timeout=1)
    scheduler.set_enabled(monitor.monitor_id, True)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and monitor.calls < 1:
        time.sleep(0.02)

    states = scheduler.worker_states()
    scheduler.stop()

    assert monitor.calls == 1
    assert states[0].alive is True
    assert states[0].last_error == "OSError"


def test_cooperative_collection_exits_within_global_stop_deadline(
    tmp_path: Path,
) -> None:
    entered = threading.Event()

    @dataclass
    class CooperativeFixture(FakeMonitor):
        monitor_id: str = "cooperative-monitor"
        stop_event: threading.Event | None = None

        def bind_stop_event(self, stop_event: threading.Event) -> None:
            self.stop_event = stop_event

        def collect(self) -> CollectionBatch:
            entered.set()
            if self.stop_event is not None and self.stop_event.wait(timeout=2):
                raise CollectionCancelled("fixture stop")
            return super().collect()

    monitor = CooperativeFixture()
    registry = MonitorRegistry()
    registry.register(monitor)
    store = make_store(tmp_path)
    scheduler = MonitorScheduler(registry, store, stop_timeout_seconds=0.5)
    scheduler.start()
    assert entered.wait(timeout=1)

    scheduler.stop()

    latest = store.latest_run(monitor.monitor_id)
    assert latest is not None
    assert latest.status == "FAILED"
    assert latest.error_code == "COLLECTION_CANCELLED"


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


def test_scheduler_exposes_started_lifecycle_state(tmp_path: Path) -> None:
    registry = MonitorRegistry()
    registry.register(FakeMonitor("lifecycle-monitor", default_enabled=False))
    scheduler = MonitorScheduler(registry, make_store(tmp_path))

    assert scheduler.started is False
    scheduler.start()
    assert scheduler.started is True
    scheduler.stop()
    assert scheduler.started is False


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


def test_broken_stdout_does_not_break_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_print(*_args: object, **_kwargs: object) -> None:
        raise BrokenPipeError("fixture stdout pipe closed")

    monitor = FakeMonitor("healthy-monitor")
    registry = MonitorRegistry()
    registry.register(monitor)
    store = make_store(tmp_path)
    scheduler = MonitorScheduler(registry, store)
    monkeypatch.setattr("builtins.print", broken_print)

    scheduler.run_once(monitor)

    latest = store.latest_run(monitor.monitor_id)
    assert latest is not None
    assert latest.status == "SUCCESS"


def test_scheduler_resolves_due_evaluations_without_changing_current_samples(
    tmp_path: Path,
) -> None:
    @dataclass
    class EvaluatingMonitor(FakeMonitor):
        monitor_id: str = "evaluating-monitor"
        evaluation_batch_limit: int = 4
        evaluated_case_keys: tuple[str, ...] = ()

        def evaluate(
            self,
            cases: tuple[ForwardEvaluationCase, ...],
            *,
            now: datetime,
        ) -> tuple[ForwardEvaluationResult, ...]:
            self.evaluated_case_keys = tuple(case.case_key for case in cases)
            return tuple(
                ForwardEvaluationResult(
                    case_key=case.case_key,
                    status="COMPLETE",
                    evaluated_at=now,
                    outcome_cutoff_at=case.due_at,
                    exit_price_text="10.5",
                    benchmark_exit_price_text="101",
                    forward_return_percent=5.0,
                    benchmark_return_percent=1.0,
                    relative_return_percent=4.0,
                    maximum_favorable_excursion_percent=6.0,
                    maximum_adverse_excursion_percent=-1.0,
                    verdict="ALIGNED",
                )
                for case in cases
            )

    store = make_store(tmp_path)
    monitor = EvaluatingMonitor()
    now = datetime.now(UTC)
    case = ForwardEvaluationCase(
        case_key="AAAUSDT|BREAKOUT|fixture|15",
        entity_key="AAAUSDT",
        stage="BREAKOUT",
        stage_label="启动",
        direction="UP",
        signal_observed_at=now - timedelta(minutes=20),
        source_cutoff_at=now - timedelta(minutes=20),
        horizon_minutes=15,
        due_at=now - timedelta(minutes=5),
        entry_price_text="10",
        benchmark_entry_price_text="100",
        source="PUBLIC_FIXTURE",
    )
    source_run = store.start_run(monitor.monitor_id, started_at=now - timedelta(minutes=20))
    store.finish_run(
        source_run,
        monitor.monitor_id,
        CollectionBatch(samples=(), evaluation_cases=(case,)),
        completed_at=now - timedelta(minutes=20),
    )
    registry = MonitorRegistry()
    registry.register(monitor)
    scheduler = MonitorScheduler(registry, store)

    scheduler.run_once(monitor)

    latest = store.latest_run(monitor.monitor_id)
    evaluation = store.recent_forward_evaluations(monitor.monitor_id)[0]
    assert monitor.evaluated_case_keys == (case.case_key,)
    assert latest is not None and latest.sample_count == 1
    assert evaluation.status == "COMPLETE"
    assert evaluation.resolved_run_id == latest.run_id
