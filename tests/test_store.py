import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from halpha_monitor.contracts import (
    CollectionArtifact,
    CollectionBatch,
    CollectionIssue,
    MetricSample,
)
from halpha_monitor.store import SQLiteMonitorStore


NOW = datetime(2026, 7, 22, 5, 0, tzinfo=UTC)


def sample(value: str, observed_at: datetime = NOW) -> MetricSample:
    return MetricSample(
        series_key="CNY|50000|BANK|BUY|BTC",
        entity_key="BTC",
        observed_at=observed_at,
        value_text=value,
        unit="CNY_PER_USDT",
        payload={"asset": "BTC", "trade_type": "BUY", "value": value},
    )


def store_at(tmp_path: Path) -> SQLiteMonitorStore:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    return store


def artifact() -> CollectionArtifact:
    return CollectionArtifact(
        artifact_key="BTCUSDT:30m:stats",
        source="https://www.binance.com/example?symbol=BTCUSDT",
        request_started_at=NOW - timedelta(milliseconds=20),
        response_completed_at=NOW,
        http_status=200,
        business_code="000000",
        schema_hash="schema-hash",
        response_sha256="response-hash",
        record_count=1,
        response_body='{"code":"000000","data":{}}',
    )


def test_samples_survive_reopen_and_remain_queryable(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    run_id = store.start_run("test-monitor", started_at=NOW)
    status = store.finish_run(
        run_id,
        "test-monitor",
        CollectionBatch(samples=(sample("6.75"),)),
        completed_at=NOW,
    )

    reopened = SQLiteMonitorStore(store.path)
    reopened.initialize()
    latest = reopened.latest_run("test-monitor")
    history = reopened.history(
        "test-monitor",
        "CNY|50000|BANK|BUY|BTC",
        since=NOW - timedelta(hours=1),
    )

    assert status == "SUCCESS"
    assert latest is not None
    assert latest.sample_count == 1
    assert history[0].value_text == "6.75"
    assert history[0].payload["asset"] == "BTC"


def test_version_two_database_adds_raw_artifact_storage_in_place(
    tmp_path: Path,
) -> None:
    path = tmp_path / "monitor.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE monitor_run (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                monitor_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                error_code TEXT
            );
            PRAGMA user_version = 2;
            """
        )

    store = SQLiteMonitorStore(path)
    store.initialize()

    with sqlite3.connect(path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        artifact_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(monitor_artifact)")
        }
    assert version == 4
    assert {
        "request_started_at",
        "response_completed_at",
        "schema_hash",
        "response_sha256",
        "response_body",
    }.issubset(artifact_columns)


def test_configuration_survives_reopen(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    store.save_configuration(
        "test-monitor",
        {"target_fiat": "2000", "trade_methods": ["BANK", "ALIPAY"]},
        updated_at=NOW,
    )

    reopened = SQLiteMonitorStore(store.path)
    reopened.initialize()
    configuration = reopened.load_configuration("test-monitor")

    assert configuration is not None
    assert configuration.updated_at == NOW
    assert configuration.values == {
        "target_fiat": "2000",
        "trade_methods": ["BANK", "ALIPAY"],
    }


def test_monitor_control_defaults_once_and_survives_reopen(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    initial = store.ensure_control(
        "test-monitor",
        default_enabled=False,
        updated_at=NOW,
    )
    changed = store.set_enabled(
        "test-monitor",
        True,
        updated_at=NOW + timedelta(minutes=1),
    )

    reopened = SQLiteMonitorStore(store.path)
    reopened.initialize()
    preserved = reopened.ensure_control(
        "test-monitor",
        default_enabled=False,
        updated_at=NOW + timedelta(minutes=2),
    )

    assert initial.enabled is False
    assert changed.enabled is True
    assert preserved == changed
    assert reopened.is_enabled("test-monitor") is True


def test_partial_run_commits_samples_and_scoped_issue_together(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    run_id = store.start_run("test-monitor", started_at=NOW)

    status = store.finish_run(
        run_id,
        "test-monitor",
        CollectionBatch(
            samples=(sample("6.75"),),
            issues=(CollectionIssue("BUY:USDT", "NO_ELIGIBLE_C2C_AD"),),
        ),
        completed_at=NOW,
    )

    latest = store.latest_run("test-monitor")
    issues = store.recent_issues("test-monitor")
    assert status == "PARTIAL"
    assert latest is not None and latest.status == "PARTIAL"
    assert store.samples_for_run(run_id)[0].value_text == "6.75"
    assert issues[0].scope == "BUY:USDT"


def test_raw_artifact_survives_reopen_with_the_same_run_transaction(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    run_id = store.start_run("test-monitor", started_at=NOW)
    store.finish_run(
        run_id,
        "test-monitor",
        CollectionBatch(samples=(sample("6.75"),), artifacts=(artifact(),)),
        completed_at=NOW,
    )

    reopened = SQLiteMonitorStore(store.path)
    reopened.initialize()
    stored = reopened.artifacts_for_run(run_id)

    assert len(stored) == 1
    assert stored[0].artifact_key == "BTCUSDT:30m:stats"
    assert stored[0].response_body == '{"code":"000000","data":{}}'
    assert stored[0].business_code == "000000"


def test_issue_without_any_sample_is_a_failed_run(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    run_id = store.start_run("test-monitor", started_at=NOW)

    status = store.finish_run(
        run_id,
        "test-monitor",
        CollectionBatch(
            samples=(),
            issues=(CollectionIssue("BTCUSDT", "SMART_MONEY_SCHEMA_CHANGED"),),
            artifacts=(artifact(),),
        ),
        completed_at=NOW,
    )

    latest = store.latest_run("test-monitor")
    assert status == "FAILED"
    assert latest is not None and latest.status == "FAILED"
    assert latest.error_code == "SMART_MONEY_SCHEMA_CHANGED"


def test_interrupted_run_is_failed_on_restart(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    store.start_run("test-monitor", started_at=NOW)

    reopened = SQLiteMonitorStore(store.path)
    reopened.initialize()

    latest = reopened.latest_run("test-monitor")
    assert latest is not None
    assert latest.status == "FAILED"
    assert latest.error_code == "PROCESS_INTERRUPTED"
    assert (
        reopened.recent_issues("test-monitor")[0].reason_code == "PROCESS_INTERRUPTED"
    )


def test_retention_removes_whole_old_run_by_cascade(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    old = NOW - timedelta(days=100)
    run_id = store.start_run("test-monitor", started_at=old)
    store.finish_run(
        run_id,
        "test-monitor",
        CollectionBatch(samples=(sample("6.70", old),)),
        completed_at=old,
    )

    assert store.prune(90, now=NOW) == 1
    assert store.latest_run("test-monitor") is None
    assert (
        store.history(
            "test-monitor",
            "CNY|50000|BANK|BUY|BTC",
            since=old - timedelta(hours=1),
        )
        == ()
    )


def test_history_limit_keeps_the_newest_samples_in_time_order(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    for offset, value in enumerate(("6.70", "6.71", "6.72")):
        observed = NOW + timedelta(minutes=offset)
        run_id = store.start_run("test-monitor", started_at=observed)
        store.finish_run(
            run_id,
            "test-monitor",
            CollectionBatch(samples=(sample(value, observed),)),
            completed_at=observed,
        )

    history = store.history(
        "test-monitor",
        "CNY|50000|BANK|BUY|BTC",
        since=NOW - timedelta(minutes=1),
        limit=2,
    )

    assert [item.value_text for item in history] == ["6.71", "6.72"]
