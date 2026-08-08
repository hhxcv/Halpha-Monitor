import sqlite3
from contextlib import closing
import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from halpha_monitor.contracts import (
    BuybackEntityRevision,
    BuybackEvidenceDocument,
    BuybackSourceObservation,
    CollectionArtifact,
    CollectionBatch,
    CollectionIssue,
    ForwardEvaluationCase,
    ForwardEvaluationResult,
    MetricSample,
    MarketEventRevision,
    ProjectionSnapshot,
)
from halpha_monitor.store import SCHEMA_VERSION, SQLiteMonitorStore


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


def market_event_revision(
    event_key: str,
    *,
    scheduled_at: datetime,
    observed_at: datetime,
    state: str,
    value: str,
) -> MarketEventRevision:
    return MarketEventRevision(
        event_key=event_key,
        scheduled_at=scheduled_at,
        observed_at=observed_at,
        state=state,
        payload={
            "row_type": "EVENT",
            "event_key": event_key,
            "event_title": event_key,
            "scheduled_at": scheduled_at.isoformat(),
            "scheduled_date": scheduled_at.date().isoformat(),
            "scheduled_sort_at": scheduled_at.isoformat(),
            "release_state": state,
            "value": value,
        },
    )


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


def test_starting_a_new_run_closes_a_stale_running_row(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    stale_run = store.start_run("test-monitor", started_at=NOW)

    current_run = store.start_run(
        "test-monitor",
        started_at=NOW + timedelta(minutes=1),
    )

    with closing(sqlite3.connect(store.path)) as connection:
        row = connection.execute(
            "SELECT status, error_code FROM monitor_run WHERE run_id = ?",
            (stale_run,),
        ).fetchone()
    assert current_run > stale_run
    assert row == ("FAILED", "WORKER_PREVIOUS_RUN_INTERRUPTED")
    assert store.issues_for_run(stale_run)[0].reason_code == (
        "WORKER_PREVIOUS_RUN_INTERRUPTED"
    )


def test_version_two_database_adds_raw_artifact_storage_in_place(
    tmp_path: Path,
) -> None:
    path = tmp_path / "monitor.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        with connection:
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

    with closing(sqlite3.connect(path)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        artifact_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(monitor_artifact)")
        }
        snapshot_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(monitor_projection_snapshot)"
            )
        }
    assert version == SCHEMA_VERSION
    assert {
        "request_started_at",
        "response_completed_at",
        "schema_hash",
        "response_sha256",
        "response_body",
    }.issubset(artifact_columns)
    assert {
        "monitor_id",
        "snapshot_key",
        "run_id",
        "observed_at",
        "cutoff_at",
        "payload_json",
    } == snapshot_columns


def test_forward_evaluation_case_is_frozen_then_resolved_atomically(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    case = ForwardEvaluationCase(
        case_key="AAAUSDT|BREAKOUT|2026-07-22T05:00:00.000Z|15",
        entity_key="AAAUSDT",
        stage="BREAKOUT",
        stage_label="启动",
        direction="UP",
        signal_observed_at=NOW,
        source_cutoff_at=NOW,
        horizon_minutes=15,
        due_at=NOW + timedelta(minutes=15),
        entry_price_text="10.000000000000",
        benchmark_entry_price_text="50000.000000000000",
        source="BINANCE_SPOT_PUBLIC_CLOSED_5M_KLINES",
    )
    source_run = store.start_run("test-monitor", started_at=NOW)
    store.finish_run(
        source_run,
        "test-monitor",
        CollectionBatch(samples=(sample("6.75"),), evaluation_cases=(case,)),
        completed_at=NOW,
    )

    assert (
        store.pending_forward_evaluations(
            "test-monitor",
            due_before=NOW + timedelta(minutes=14),
            limit=10,
        )
        == ()
    )
    assert store.pending_forward_evaluations(
        "test-monitor",
        due_before=NOW + timedelta(minutes=15),
        limit=10,
    ) == (case,)

    resolved_at = NOW + timedelta(minutes=16)
    result = ForwardEvaluationResult(
        case_key=case.case_key,
        status="COMPLETE",
        evaluated_at=resolved_at,
        outcome_cutoff_at=case.due_at,
        exit_price_text="10.500000000000",
        benchmark_exit_price_text="50250.000000000000",
        forward_return_percent=5.0,
        benchmark_return_percent=0.5,
        relative_return_percent=4.5,
        maximum_favorable_excursion_percent=6.0,
        maximum_adverse_excursion_percent=-1.0,
        verdict="ALIGNED",
    )
    resolved_run = store.start_run("test-monitor", started_at=resolved_at)
    store.finish_run(
        resolved_run,
        "test-monitor",
        CollectionBatch(
            samples=(sample("6.76", resolved_at),), evaluation_results=(result,)
        ),
        completed_at=resolved_at,
    )

    stored = store.recent_forward_evaluations("test-monitor")
    summary = store.forward_evaluation_summary("test-monitor", now=resolved_at)
    assert len(stored) == 1
    assert stored[0].source_run_id == source_run
    assert stored[0].resolved_run_id == resolved_run
    assert stored[0].status == "COMPLETE"
    assert stored[0].relative_return_percent == 4.5
    assert summary["due_cases"] == 1
    assert summary["completed_cases"] == 1
    assert summary["distinct_cutoff_count"] == 1
    assert summary["distinct_entity_count"] == 1
    assert summary["first_cutoff_at"] == NOW
    assert summary["last_outcome_at"] == case.due_at
    assert summary["groups"][0]["aligned_count"] == 1
    assert (
        store.recent_forward_evaluations(
            "test-monitor",
            source=case.source,
        )
        == stored
    )
    assert (
        store.recent_forward_evaluations(
            "test-monitor",
            source="BINANCE_USDM_PUBLIC_CLOSED_5M_KLINES",
        )
        == ()
    )
    assert (
        store.latest_forward_evaluations_by_entity(
            "test-monitor",
            ("AAAUSDT",),
            source=case.source,
        )
        == stored
    )
    assert (
        store.forward_evaluation_summary(
            "test-monitor",
            now=resolved_at,
            source="BINANCE_USDM_PUBLIC_CLOSED_5M_KLINES",
        )["total_cases"]
        == 0
    )


def test_forward_evaluation_comparison_joins_only_exact_completed_pairs(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    primary_source = "PRICE_CONTEXT_V1"
    baseline_source = "SHORT_RULE_V1"
    primary = ForwardEvaluationCase(
        case_key="price-context-v1|AAAUSDT|BLOWOFF_RISK|cutoff|240",
        entity_key="AAAUSDT",
        stage="BLOWOFF_RISK",
        stage_label="冲高回落风险",
        direction="DOWN",
        signal_observed_at=NOW,
        source_cutoff_at=NOW,
        horizon_minutes=240,
        due_at=NOW + timedelta(minutes=240),
        entry_price_text="10",
        benchmark_entry_price_text="50000",
        source=primary_source,
    )
    baseline = ForwardEvaluationCase(
        case_key="AAAUSDT|SETUP|cutoff|240",
        entity_key="AAAUSDT",
        stage="ACCELERATION",
        stage_label="加速",
        direction="UP",
        signal_observed_at=NOW,
        source_cutoff_at=NOW,
        horizon_minutes=240,
        due_at=NOW + timedelta(minutes=240),
        entry_price_text="10",
        benchmark_entry_price_text="50000",
        source=baseline_source,
    )
    source_run = store.start_run("test-monitor", started_at=NOW)
    store.finish_run(
        source_run,
        "test-monitor",
        CollectionBatch(
            samples=(sample("6.75"),),
            evaluation_cases=(primary, baseline),
        ),
        completed_at=NOW,
    )
    resolved_at = primary.due_at + timedelta(minutes=1)
    resolved_run = store.start_run("test-monitor", started_at=resolved_at)
    store.finish_run(
        resolved_run,
        "test-monitor",
        CollectionBatch(
            samples=(sample("6.76", resolved_at),),
            evaluation_results=(
                ForwardEvaluationResult(
                    case_key=primary.case_key,
                    status="COMPLETE",
                    evaluated_at=resolved_at,
                    outcome_cutoff_at=primary.due_at,
                    exit_price_text="9.5",
                    benchmark_exit_price_text="50000",
                    forward_return_percent=-5,
                    benchmark_return_percent=0,
                    relative_return_percent=-5,
                    maximum_favorable_excursion_percent=1,
                    maximum_adverse_excursion_percent=-6,
                    verdict="ALIGNED",
                ),
                ForwardEvaluationResult(
                    case_key=baseline.case_key,
                    status="COMPLETE",
                    evaluated_at=resolved_at,
                    outcome_cutoff_at=baseline.due_at,
                    exit_price_text="9.5",
                    benchmark_exit_price_text="50000",
                    forward_return_percent=-5,
                    benchmark_return_percent=0,
                    relative_return_percent=-5,
                    maximum_favorable_excursion_percent=1,
                    maximum_adverse_excursion_percent=-6,
                    verdict="OPPOSED",
                ),
            ),
        ),
        completed_at=resolved_at,
    )

    comparison = store.forward_evaluation_comparison(
        "test-monitor",
        primary_source=primary_source,
        baseline_source=baseline_source,
    )

    assert comparison["paired_case_count"] == 1
    assert comparison["sample_count"] == 1
    assert comparison["pending_pair_count"] == 0
    assert comparison["unavailable_pair_count"] == 0
    assert comparison["primary_aligned_count"] == 1
    assert comparison["primary_opposed_count"] == 0
    assert comparison["baseline_aligned_count"] == 0
    assert comparison["baseline_opposed_count"] == 1
    assert comparison["first_cutoff_at"] == NOW
    assert comparison["last_outcome_at"] == primary.due_at
    flip_relation = comparison["relations"][0]
    same_relation = comparison["relations"][1]
    assert flip_relation["direction_relation"] == "DIRECTION_FLIP"
    assert flip_relation["paired_case_count"] == 1
    assert flip_relation["sample_count"] == 1
    assert flip_relation["distinct_cutoff_count"] == 1
    assert flip_relation["distinct_entity_count"] == 1
    assert same_relation["direction_relation"] == "SAME_DIRECTION"
    assert same_relation["paired_case_count"] == 0
    assert same_relation["sample_count"] == 0
    assert len(comparison["groups"]) == 1
    group = comparison["groups"][0]
    assert group["stage"] == "BLOWOFF_RISK"
    assert group["stage_label"] == "冲高回落风险"
    assert group["horizon_minutes"] == 240
    assert group["direction_relation"] == "DIRECTION_FLIP"
    assert group["paired_case_count"] == 1
    assert group["sample_count"] == 1
    assert group["primary_aligned_count"] == 1
    assert group["baseline_opposed_count"] == 1


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
    run_issues = store.issues_for_run(run_id)
    assert status == "PARTIAL"
    assert latest is not None and latest.status == "PARTIAL"
    assert store.samples_for_run(run_id)[0].value_text == "6.75"
    assert issues[0].scope == "BUY:USDT"
    assert run_issues == issues


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


def test_projection_snapshot_is_atomically_replaced_without_history_growth(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    first_run = store.start_run("test-monitor", started_at=NOW)
    store.finish_run(
        first_run,
        "test-monitor",
        CollectionBatch(
            samples=(sample("6.75"),),
            projection_snapshots=(
                ProjectionSnapshot(
                    snapshot_key="price-position-v1",
                    observed_at=NOW,
                    cutoff_at=NOW - timedelta(seconds=1),
                    payload={"rows": [{"symbol": "AAAUSDT"}]},
                ),
            ),
        ),
        completed_at=NOW,
    )
    second_time = NOW + timedelta(minutes=5)
    second_run = store.start_run("test-monitor", started_at=second_time)
    store.finish_run(
        second_run,
        "test-monitor",
        CollectionBatch(
            samples=(sample("6.80", second_time),),
            projection_snapshots=(
                ProjectionSnapshot(
                    snapshot_key="price-position-v1",
                    observed_at=second_time,
                    cutoff_at=second_time - timedelta(seconds=1),
                    payload={"rows": [{"symbol": "BBBUSDT"}]},
                ),
            ),
        ),
        completed_at=second_time,
    )

    reopened = SQLiteMonitorStore(store.path)
    reopened.initialize()
    snapshot = reopened.projection_snapshot(
        "test-monitor",
        "price-position-v1",
    )
    with closing(sqlite3.connect(store.path)) as connection:
        row_count = connection.execute(
            "SELECT COUNT(*) FROM monitor_projection_snapshot"
        ).fetchone()[0]

    assert snapshot is not None
    assert snapshot.run_id == second_run
    assert snapshot.payload["rows"] == [{"symbol": "BBBUSDT"}]
    assert row_count == 1


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


def buyback_batch(
    *,
    observed_at: datetime = NOW,
    payload: dict[str, object] | None = None,
) -> CollectionBatch:
    body = b"%PDF-1.4\nfixture\n%%EOF\n"
    digest = hashlib.sha256(body).hexdigest()
    entity_payload = payload or {
        "market": "SH",
        "stock_code": "600000",
        "issuer_name": "Fixture Issuer",
        "event_type": "PLAN_OR_APPROVAL",
        "review_status": "CANDIDATE_UNCONFIRMED",
    }
    return CollectionBatch(
        samples=(),
        buyback_documents=(
            BuybackEvidenceDocument(
                source_key="sse-announcements",
                source_label="上交所公告",
                source_document_id="fixture-document",
                source_url="https://example.com/fixture.pdf",
                published_at=observed_at,
                observed_at=observed_at,
                media_type="application/pdf",
                file_suffix=".pdf",
                body=body,
                quality_state="VALID_PDF",
                metadata={"page_count": 1},
            ),
        ),
        buyback_revisions=(
            BuybackEntityRevision(
                entity_key="A:SH:600000:fixture-document",
                entity_type="DISCLOSURE_CANDIDATE",
                effective_at=observed_at,
                observed_at=observed_at,
                source_key="sse-announcements",
                document_sha256=digest,
                payload=entity_payload,
            ),
        ),
        buyback_source_observations=(
            BuybackSourceObservation(
                source_key="sse-announcements",
                source_label="上交所公告",
                status="SUCCESS",
                checked_at=observed_at,
                source_time=observed_at,
                next_due_at=observed_at + timedelta(hours=1),
                record_count=1,
                detail_code=None,
                payload={"window_days": 7},
            ),
        ),
    )


def test_buyback_state_is_atomic_idempotent_and_outlives_run_retention(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(
        tmp_path / "monitor.sqlite3",
        buyback_retention_days=365,
    )
    store.initialize()
    run_id = store.start_run("a-hk-buyback", started_at=NOW)
    status = store.finish_run(
        run_id,
        "a-hk-buyback",
        buyback_batch(),
        completed_at=NOW,
    )
    second_run = store.start_run("a-hk-buyback", started_at=NOW + timedelta(hours=1))
    store.finish_run(
        second_run,
        "a-hk-buyback",
        buyback_batch(observed_at=NOW + timedelta(hours=1)),
        completed_at=NOW + timedelta(hours=1),
    )

    entities = store.latest_buyback_entities("a-hk-buyback")
    revisions = store.buyback_entity_revisions(
        "a-hk-buyback", "A:SH:600000:fixture-document"
    )
    source = store.buyback_source_states("a-hk-buyback")[0]
    document = store.buyback_document(entities[0].document_sha256 or "")

    assert status == "SUCCESS"
    assert len(entities) == 1
    assert len(revisions) == 1
    assert source.last_run_id == second_run
    assert document is not None
    assert store.buyback_document_path(document).read_bytes().startswith(b"%PDF-")

    review = store.save_buyback_review(
        "a-hk-buyback",
        entities[0].entity_key,
        base_revision_no=entities[0].revision_no,
        decision="CONFIRMED_EVENT",
        corrected_event_type="PLAN_OR_APPROVAL",
        program_key="600000-2026-07",
        program_status="PROPOSED",
        note="人工核对原文。",
        created_at=NOW + timedelta(hours=2),
    )
    assert review.review_id > 0

    store.prune(1, now=NOW + timedelta(days=2))
    assert store.latest_run("a-hk-buyback") is None
    preserved = store.buyback_entity("a-hk-buyback", "A:SH:600000:fixture-document")
    assert preserved is not None
    assert preserved.review is not None
    assert preserved.review.decision == "CONFIRMED_EVENT"
    assert store.buyback_document(document.sha256) is not None


def test_buyback_evidence_read_detects_same_size_content_tampering(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    run_id = store.start_run("a-hk-buyback", started_at=NOW)
    store.finish_run(
        run_id,
        "a-hk-buyback",
        buyback_batch(),
        completed_at=NOW,
    )
    entity = store.latest_buyback_entities("a-hk-buyback")[0]
    document = store.buyback_document(entity.document_sha256 or "")
    assert document is not None
    evidence_path = store.buyback_document_path(document)
    evidence_path.write_bytes(b"x" * document.size_bytes)

    with pytest.raises(RuntimeError, match="BUYBACK_EVIDENCE_CONTENT_MISMATCH"):
        store.buyback_document_path(document)


def test_buyback_review_rejects_a_stale_revision(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    first_run = store.start_run("a-hk-buyback", started_at=NOW)
    store.finish_run(
        first_run,
        "a-hk-buyback",
        buyback_batch(),
        completed_at=NOW,
    )
    changed = {
        "market": "SH",
        "stock_code": "600000",
        "issuer_name": "Fixture Issuer",
        "event_type": "MODIFICATION",
        "review_status": "CANDIDATE_UNCONFIRMED",
    }
    second_run = store.start_run("a-hk-buyback", started_at=NOW + timedelta(minutes=10))
    store.finish_run(
        second_run,
        "a-hk-buyback",
        buyback_batch(observed_at=NOW + timedelta(minutes=10), payload=changed),
        completed_at=NOW + timedelta(minutes=10),
    )

    with pytest.raises(RuntimeError, match="BUYBACK_REVISION_CONFLICT"):
        store.save_buyback_review(
            "a-hk-buyback",
            "A:SH:600000:fixture-document",
            base_revision_no=1,
            decision="CONFIRMED_EVENT",
            corrected_event_type=None,
            program_key=None,
            program_status=None,
            note="stale",
        )


def test_buyback_review_is_invalidated_by_a_new_entity_revision(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    first_run = store.start_run("a-hk-buyback", started_at=NOW)
    store.finish_run(
        first_run,
        "a-hk-buyback",
        buyback_batch(),
        completed_at=NOW,
    )
    store.save_buyback_review(
        "a-hk-buyback",
        "A:SH:600000:fixture-document",
        base_revision_no=1,
        decision="CONFIRMED_EVENT",
        corrected_event_type="PLAN_OR_APPROVAL",
        program_key="600000-2026-08",
        program_status="PROPOSED",
        note="revision one",
        created_at=NOW + timedelta(minutes=1),
    )
    second_run = store.start_run(
        "a-hk-buyback",
        started_at=NOW + timedelta(minutes=2),
    )
    store.finish_run(
        second_run,
        "a-hk-buyback",
        buyback_batch(
            observed_at=NOW + timedelta(minutes=2),
            payload={
                "market": "SH",
                "stock_code": "600000",
                "issuer_name": "Fixture Issuer",
                "event_type": "MODIFICATION",
                "review_status": "CANDIDATE_UNCONFIRMED",
            },
        ),
        completed_at=NOW + timedelta(minutes=2),
    )

    current = store.buyback_entity(
        "a-hk-buyback",
        "A:SH:600000:fixture-document",
    )

    assert current is not None
    assert current.revision_no == 2
    assert current.review is None
    assert (
        len(
            store.buyback_reviews(
                "a-hk-buyback",
                "A:SH:600000:fixture-document",
            )
        )
        == 1
    )


def test_buyback_evidence_quota_counts_unreferenced_files_on_disk(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(
        tmp_path / "monitor.sqlite3",
        buyback_evidence_max_bytes=30,
    )
    store.initialize()
    store.buyback_evidence_root.mkdir(parents=True)
    (store.buyback_evidence_root / "unreferenced.bin").write_bytes(b"orphaned")
    run_id = store.start_run("a-hk-buyback", started_at=NOW)

    with pytest.raises(RuntimeError, match="BUYBACK_EVIDENCE_QUOTA_EXCEEDED"):
        store.finish_run(
            run_id,
            "a-hk-buyback",
            buyback_batch(),
            completed_at=NOW,
        )

    assert store.latest_buyback_entities("a-hk-buyback") == ()


def test_buyback_retention_removes_old_entity_review_and_evidence(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(
        tmp_path / "monitor.sqlite3",
        buyback_retention_days=1,
    )
    store.initialize()
    run_id = store.start_run("a-hk-buyback", started_at=NOW)
    store.finish_run(
        run_id,
        "a-hk-buyback",
        buyback_batch(),
        completed_at=NOW,
    )
    entity = store.latest_buyback_entities("a-hk-buyback")[0]
    document = store.buyback_document(entity.document_sha256 or "")
    assert document is not None
    evidence_path = store.buyback_document_path(document)
    store.save_buyback_review(
        "a-hk-buyback",
        entity.entity_key,
        base_revision_no=1,
        decision="REJECTED_EVENT",
        corrected_event_type=None,
        program_key=None,
        program_status=None,
        note="fixture",
        created_at=NOW,
    )

    store.prune(90, now=NOW + timedelta(days=2))

    assert store.buyback_entity("a-hk-buyback", entity.entity_key) is None
    assert store.buyback_document(document.sha256) is None
    assert not evidence_path.exists()


def test_buyback_prune_retries_an_old_orphan_after_file_unlink_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SQLiteMonitorStore(
        tmp_path / "monitor.sqlite3",
        buyback_retention_days=1,
    )
    store.initialize()
    run_id = store.start_run("a-hk-buyback", started_at=NOW)
    store.finish_run(
        run_id,
        "a-hk-buyback",
        buyback_batch(),
        completed_at=NOW,
    )
    entity = store.latest_buyback_entities("a-hk-buyback")[0]
    document = store.buyback_document(entity.document_sha256 or "")
    assert document is not None
    evidence_path = store.buyback_document_path(document)
    old_timestamp = (NOW - timedelta(days=2)).timestamp()
    os.utime(evidence_path, (old_timestamp, old_timestamp))
    original_unlink = Path.unlink

    def fail_target_unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path.resolve() == evidence_path.resolve():
            raise PermissionError("fixture locked file")
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_target_unlink)
    store.prune(90, now=NOW + timedelta(days=2))

    assert store.buyback_document(document.sha256) is None
    assert evidence_path.exists()

    monkeypatch.setattr(Path, "unlink", original_unlink)
    store.prune(90, now=NOW + timedelta(days=2, minutes=1))

    assert not evidence_path.exists()
    metrics = store.storage_metrics()
    assert metrics["database_bytes"] > 0
    assert metrics["wal_bytes"] >= 0
    assert metrics["buyback_evidence_bytes"] == 0


def test_market_event_history_starts_without_backfilling_and_keeps_revisions(
    tmp_path: Path,
) -> None:
    store = store_at(tmp_path)
    first_run = store.start_run("market-event-calendar", started_at=NOW)
    store.finish_run(
        first_run,
        "market-event-calendar",
        CollectionBatch(
            samples=(),
            market_event_revisions=(
                market_event_revision(
                    "past-event",
                    scheduled_at=NOW - timedelta(hours=1),
                    observed_at=NOW,
                    state="OCCURRED",
                    value="past",
                ),
                market_event_revision(
                    "future-event",
                    scheduled_at=NOW + timedelta(hours=1),
                    observed_at=NOW,
                    state="SCHEDULED",
                    value="forecast",
                ),
            ),
        ),
        completed_at=NOW,
    )

    second_observation = NOW + timedelta(hours=2)
    second_run = store.start_run(
        "market-event-calendar",
        started_at=second_observation,
    )
    store.finish_run(
        second_run,
        "market-event-calendar",
        CollectionBatch(
            samples=(),
            market_event_revisions=(
                market_event_revision(
                    "future-event",
                    scheduled_at=NOW + timedelta(hours=1),
                    observed_at=second_observation,
                    state="RELEASED",
                    value="actual",
                ),
            ),
        ),
        completed_at=second_observation,
    )

    history = store.latest_market_event_revisions("market-event-calendar")
    assert store.market_event_history_started_at("market-event-calendar") == NOW
    assert [item.event_key for item in history] == ["future-event"]
    assert history[0].state == "RELEASED"
    assert history[0].revision_no == 2
