from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import halpha_monitor.monitors.binance_btc_intelligence as btc_intelligence

from halpha_monitor.contracts import (
    BtcMonthlyResearchHistoryObservation,
    BtcStructureEventRevision,
    BtcStructureHistoryObservation,
    CollectionBatch,
    CollectionIssue,
    MetricSample,
)
from halpha_monitor.monitors.binance_btc_intelligence import (
    ALGORITHM_VERSION,
    MONTHLY_ALGORITHM_VERSION,
    BinanceBtcIntelligenceMonitor,
    BinanceBtcIntelligenceSettings,
    Pivot,
    SpotSeriesResult,
    add_4h_indicators,
    add_event_quality_features,
    classify_event_outcome,
    current_structure_payload,
    daily_donchian_state,
    form_zones,
    monthly_state,
    normalize_spot_klines,
    unified_regime,
)
from halpha_monitor.store import SQLiteMonitorStore


def _bar_frame(start: datetime, rows: int, interval: timedelta) -> pd.DataFrame:
    interval_ms = int(interval.total_seconds() * 1000)
    open_times = np.array(
        [int((start + index * interval).timestamp() * 1000) for index in range(rows)],
        dtype=np.int64,
    )
    trend = np.arange(rows, dtype=float) * 2.5
    close = 30_000 + trend + 480 * np.sin(np.arange(rows) / 17)
    open_price = close - 12 * np.cos(np.arange(rows) / 9)
    return pd.DataFrame(
        {
            "open_time_ms": open_times,
            "open": open_price,
            "high": np.maximum(open_price, close) + 90,
            "low": np.minimum(open_price, close) - 90,
            "close": close,
            "volume": np.full(rows, 100.0),
            "close_time_ms": open_times + interval_ms - 1,
            "quote_volume": np.full(rows, 3_000_000.0),
            "trade_count": np.full(rows, 1000),
            "taker_buy_volume": np.full(rows, 51.0),
            "taker_buy_quote_volume": np.full(rows, 1_530_000.0),
        }
    )


def test_normalize_spot_klines_keeps_only_valid_closed_rows() -> None:
    cutoff = 99_999
    rows = [
        [0, "10", "12", "9", "11", "2", 9_999, "22", 3, "1", "11"],
        [10_000, "10", "12", "9", "11", "2", 199_999, "22", 3, "1", "11"],
    ]

    frame = normalize_spot_klines(rows, cutoff)

    assert len(frame) == 1
    assert frame.iloc[0]["close"] == 11
    with pytest.raises(ValueError, match="KLINE_SCHEMA_CHANGED"):
        normalize_spot_klines([["bad"]], cutoff)


def test_confirmed_zone_requires_two_separated_causally_available_anchors() -> None:
    pivots = [
        Pivot(10, 13, "LOW", 100.0, 4.0, 1.2),
        Pivot(22, 25, "LOW", 100.8, 4.0, 1.2),
    ]

    assert form_zones(pivots, known_through=24, current_atr=4.0) == []
    zones = form_zones(pivots, known_through=25, current_atr=4.0)

    assert len(zones) == 1
    assert zones[0].anchor_count == 2
    assert zones[0].formed_index == 25
    assert zones[0].score >= 1.25


@pytest.mark.parametrize(
    ("kind", "future", "expected", "offset"),
    [
        ("SUPPORT", [100.5, 102.2, 99.0], "REACTION", 2),
        ("SUPPORT", [99.5, 96.9, 103.0], "BREAK", 2),
        ("RESISTANCE", [99.0, 97.8, 104.0], "REACTION", 2),
        ("RESISTANCE", [101.0, 104.1, 96.0], "BREAK", 2),
        ("SUPPORT", [100.2] * 6, "UNRESOLVED", None),
    ],
)
def test_event_outcome_uses_first_closed_bar_threshold(
    kind: str,
    future: list[float],
    expected: str,
    offset: int | None,
) -> None:
    result = classify_event_outcome(
        future,
        kind=kind,
        touch_close=100,
        atr=2,
        zone_lower=98,
        zone_upper=102,
    )

    assert result == (expected, offset)


def test_daily_donchian_replays_four_fixed_components() -> None:
    frame = _bar_frame(datetime(2025, 1, 1, tzinfo=UTC), 150, timedelta(days=1))
    frame["close"] = np.arange(1, 151, dtype=float)
    frame["open"] = frame["close"]
    frame["high"] = frame["close"] + 1
    frame["low"] = frame["close"] - 0.5

    result = daily_donchian_state(frame)

    assert result["agreement"] == "1"
    assert result["state"] == "STRONG_UP"
    assert [item["window"] for item in result["components"]] == [20, 30, 60, 90]
    assert all(item["active"] for item in result["components"])
    assert result["authority"] == "STATE_ONLY"


def test_monthly_state_uses_complete_months_and_current_month_boundary() -> None:
    frame = _bar_frame(datetime(2024, 1, 1, tzinfo=UTC), 731, timedelta(days=1))
    now = datetime(2026, 1, 15, tzinfo=UTC)
    complete = frame[frame["close_time_ms"] < int(now.timestamp() * 1000)].copy()

    result = monthly_state(complete, current_price=40_000, now=now)

    periods = pd.to_datetime(complete["close_time_ms"], unit="ms", utc=True).dt.tz_localize(None).dt.to_period("M")
    closes = pd.Series(complete["close"].to_numpy(float), index=periods).groupby(level=0).last()
    assert float(result["current_boundary"]) == pytest.approx(float(closes.iloc[-9:].mean()))
    assert result["formed_month"] == "2025-12"
    expected_target = int(float(closes.iloc[-1]) > float(closes.iloc[-10:-1].mean()))
    assert result["official_target"] == expected_target
    assert result["authority"] == "RESEARCH_TARGET_ONLY"


@pytest.mark.parametrize(
    ("target", "agreement", "code"),
    [
        (1, 1.0, "ALIGNED_UP"),
        (1, 0.0, "SLOW_UP_TACTICAL_PULLBACK"),
        (0, 1.0, "COUNTERTREND_RALLY"),
        (0, 0.0, "DEFENSIVE"),
        (1, 0.5, "TRANSITION"),
    ],
)
def test_unified_regime_has_no_weighted_composite(
    target: int, agreement: float, code: str
) -> None:
    assert unified_regime(target, agreement)["code"] == code


def test_store_freezes_signal_before_appending_structure_outcome(tmp_path: Path) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor_id = "btc-market-intelligence"
    started = datetime(2026, 8, 7, 0, 0, tzinfo=UTC)
    event_at = started + timedelta(hours=4)
    signal = {
        "event_key": "event-1",
        "kind": "SUPPORT",
        "open_time_ms": int(event_at.timestamp() * 1000),
        "zone_lower": "100",
        "zone_upper": "102",
        "touch_close": "101",
        "atr_touch": "2",
    }

    first_run = store.start_run(monitor_id, started_at=started)
    store.finish_run(
        first_run,
        monitor_id,
        CollectionBatch(
            samples=(),
            btc_structure_history=BtcStructureHistoryObservation(
                started_at=started,
                processed_through_at=started - timedelta(milliseconds=1),
                algorithm_version=ALGORITHM_VERSION,
            ),
        ),
        completed_at=started,
    )
    signal_run = store.start_run(monitor_id, started_at=event_at)
    store.finish_run(
        signal_run,
        monitor_id,
        CollectionBatch(
            samples=(),
            btc_structure_history=BtcStructureHistoryObservation(
                started_at=started,
                processed_through_at=event_at,
                algorithm_version=ALGORITHM_VERSION,
            ),
            btc_structure_event_revisions=(
                BtcStructureEventRevision(
                    event_key="event-1",
                    event_at=event_at,
                    observed_at=event_at + timedelta(seconds=5),
                    state="PENDING",
                    payload={
                        "algorithm_version": ALGORITHM_VERSION,
                        "signal": signal,
                        "outcome": None,
                    },
                ),
            ),
        ),
        completed_at=event_at + timedelta(seconds=5),
    )
    outcome_run = store.start_run(monitor_id, started_at=event_at + timedelta(days=1))
    store.finish_run(
        outcome_run,
        monitor_id,
        CollectionBatch(
            samples=(),
            btc_structure_event_revisions=(
                BtcStructureEventRevision(
                    event_key="event-1",
                    event_at=event_at,
                    observed_at=event_at + timedelta(days=1),
                    state="UNRESOLVED",
                    payload={
                        "algorithm_version": ALGORITHM_VERSION,
                        "signal": signal,
                        "outcome": {
                            "state": "UNRESOLVED",
                            "unresolved_retained_in_denominator": True,
                        },
                    },
                ),
            ),
        ),
        completed_at=event_at + timedelta(days=1),
    )

    latest = store.latest_btc_structure_event_revisions(monitor_id)
    assert len(latest) == 1
    assert latest[0].revision_no == 2
    assert latest[0].state == "UNRESOLVED"
    assert latest[0].payload["signal"] == signal
    summary = store.btc_structure_event_summary(monitor_id)
    assert summary["total_events"] == 1
    assert summary["pending_events"] == 0
    assert summary["reaction_events"] == 0
    assert summary["break_events"] == 0
    assert summary["unresolved_events"] == 1
    assert summary["completed_events"] == 1
    assert summary["reaction_rate_percent"] == 0.0
    assert summary["probability_validation_status"] == "NOT_STARTED"
    assert summary["promotion_evaluable"] is False


def test_store_rejects_event_revision_from_another_algorithm_version(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor_id = "btc-market-intelligence"
    started = datetime(2026, 8, 7, 0, 0, tzinfo=UTC)
    event_at = started + timedelta(hours=4)
    history_run = store.start_run(monitor_id, started_at=started)
    store.finish_run(
        history_run,
        monitor_id,
        CollectionBatch(
            samples=(),
            btc_structure_history=BtcStructureHistoryObservation(
                started_at=started,
                processed_through_at=started,
                algorithm_version=ALGORITHM_VERSION,
            ),
        ),
        completed_at=started,
    )

    event_run = store.start_run(monitor_id, started_at=event_at)
    with pytest.raises(RuntimeError, match="BTC_STRUCTURE_EVENT_VERSION_CHANGED"):
        store.finish_run(
            event_run,
            monitor_id,
            CollectionBatch(
                samples=(),
                btc_structure_event_revisions=(
                    BtcStructureEventRevision(
                        event_key="event-other-version",
                        event_at=event_at,
                        observed_at=event_at,
                        state="PENDING",
                        payload={
                            "algorithm_version": "other-version",
                            "signal": {"event_key": "event-other-version"},
                            "outcome": None,
                        },
                    ),
                ),
            ),
            completed_at=event_at,
        )
    store.fail_run(
        event_run,
        monitor_id,
        "EXPECTED_TEST_REJECTION",
        completed_at=event_at,
    )
    assert store.latest_btc_structure_event_revisions(monitor_id) == ()


def test_store_rotates_forward_clock_without_mixing_old_algorithm_events(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor_id = "btc-market-intelligence"
    started = datetime(2026, 8, 7, 0, 0, tzinfo=UTC)
    event_at = started + timedelta(hours=4)
    old_version = "btc-structure-causal-v1"
    run_id = store.start_run(monitor_id, started_at=started)
    store.finish_run(
        run_id,
        monitor_id,
        CollectionBatch(
            samples=(),
            btc_structure_history=BtcStructureHistoryObservation(
                started_at=started,
                processed_through_at=event_at,
                algorithm_version=old_version,
            ),
            btc_structure_event_revisions=(
                BtcStructureEventRevision(
                    event_key=f"{old_version}:event-1",
                    event_at=event_at,
                    observed_at=event_at + timedelta(seconds=1),
                    state="PENDING",
                    payload={
                        "algorithm_version": old_version,
                        "signal": {"event_key": "event-1"},
                        "outcome": None,
                    },
                ),
            ),
        ),
        completed_at=event_at + timedelta(seconds=1),
    )
    rotated_at = event_at + timedelta(hours=4)
    run_id = store.start_run(monitor_id, started_at=rotated_at)
    store.finish_run(
        run_id,
        monitor_id,
        CollectionBatch(
            samples=(),
            btc_structure_history=BtcStructureHistoryObservation(
                started_at=rotated_at,
                processed_through_at=rotated_at,
                algorithm_version=ALGORITHM_VERSION,
            ),
        ),
        completed_at=rotated_at,
    )

    history = store.btc_structure_history(monitor_id)
    assert history is not None
    assert history.algorithm_version == ALGORITHM_VERSION
    assert history.started_at == rotated_at
    assert store.pending_btc_structure_event_revisions(
        monitor_id,
        algorithm_version=ALGORITHM_VERSION,
    ) == ()
    assert store.btc_structure_event_summary(
        monitor_id,
        algorithm_version=ALGORITHM_VERSION,
    )["total_events"] == 0
    assert len(store.latest_btc_structure_event_revisions(monitor_id)) == 1


class _FakeSpotClient:
    def __init__(self, daily: pd.DataFrame, four_hour: pd.DataFrame, now: datetime) -> None:
        self.daily = daily
        self.four_hour = four_hour
        self.now = now

    def fetch_bars(
        self, *, interval: str, cutoff: datetime, history_bars: int
    ) -> SpotSeriesResult:
        frame = self.daily if interval == "1d" else self.four_hour
        return SpotSeriesResult(interval, "FETCHED", frame, cutoff, self.now)

    def fetch_price(self) -> tuple[float, datetime]:
        return 41_000.0, self.now

    def fetch_execution_price(self, at: datetime) -> tuple[float, datetime]:
        return 42_000.0, at

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return 0

    def bind_stop_event(self, _stop_event) -> None:  # type: ignore[no-untyped-def]
        return None


class _FakeSmartMoneyMonitor:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def collect(self) -> CollectionBatch:
        return CollectionBatch(
            samples=(
                MetricSample(
                    "BTCUSDT|1h|normalized-flow",
                    "BTCUSDT",
                    self.now,
                    "0.1",
                    "% OI",
                    {
                        "time_range": "1h",
                        "time_range_label": "1 小时",
                        "dominant_flow": "多头净流入",
                        "normalized_flow_percent": "0.1",
                    },
                ),
            )
        )

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return 0

    def bind_stop_event(self, _stop_event) -> None:  # type: ignore[no-untyped-def]
        return None


class _StaleOverviewSmartMoneyMonitor(_FakeSmartMoneyMonitor):
    def collect(self) -> CollectionBatch:
        rows = tuple(
            MetricSample(
                f"BTCUSDT|{time_range}|normalized-flow",
                "BTCUSDT",
                self.now,
                "0.1",
                "% OI",
                {
                    "time_range": time_range,
                    "time_range_label": label,
                    "dominant_flow": "多头净流入",
                    "normalized_flow_percent": "0.1",
                },
            )
            for time_range, label in (("30m", "30 分钟"), ("1h", "1 小时"))
        )
        return CollectionBatch(
            samples=rows,
            issues=(CollectionIssue("BTCUSDT", "SMART_MONEY_OVERVIEW_STALE"),),
        )


def _initialize_structure_history(
    store: SQLiteMonitorStore,
    *,
    started_at: datetime,
    processed_through_at: datetime,
) -> None:
    run_id = store.start_run("btc-market-intelligence", started_at=started_at)
    store.finish_run(
        run_id,
        "btc-market-intelligence",
        CollectionBatch(
            samples=(),
            btc_structure_history=BtcStructureHistoryObservation(
                started_at=started_at,
                processed_through_at=processed_through_at,
                algorithm_version=ALGORITHM_VERSION,
            ),
        ),
        completed_at=started_at,
    )


def _synthetic_event(frame: pd.DataFrame, index: int) -> dict[str, object]:
    close = float(frame.loc[index, "close"])
    atr = float(frame.loc[index, "atr"])
    return {
        "index": index,
        "open_time_ms": int(frame.loc[index, "open_time_ms"]),
        "close_time_ms": int(frame.loc[index, "close_time_ms"]),
        "kind": "SUPPORT",
        "zone_lower": close - atr * 0.3,
        "zone_upper": close + atr * 0.3,
        "zone_center": close,
        "strength": 2.0,
        "anchor_count": 3,
        "formed_close_time_ms": int(frame.loc[index - 20, "close_time_ms"]),
        "level_effective_close_time_ms": int(
            frame.loc[index - 3, "close_time_ms"]
        ),
        "level_version": "a" * 64,
        "zone_age_bars": 20,
        "touch_close": close,
        "atr": atr,
        "result": "PENDING",
        "outcome_offset": None,
    }


def test_recovery_never_registers_a_touch_after_a_later_bar_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 2, 1, 1, 0, tzinfo=UTC)
    daily = _bar_frame(datetime(2023, 8, 1, tzinfo=UTC), 900, timedelta(days=1))
    raw = _bar_frame(datetime(2025, 5, 1, tzinfo=UTC), 1200, timedelta(hours=4))
    frame = add_event_quality_features(add_4h_indicators(raw))
    event = _synthetic_event(frame, len(frame) - 2)
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    processed = datetime.fromtimestamp(
        float(frame.loc[len(frame) - 3, "close_time_ms"]) / 1000,
        tz=UTC,
    )
    _initialize_structure_history(store, started_at=processed, processed_through_at=processed)
    monitor = BinanceBtcIntelligenceMonitor(
        BinanceBtcIntelligenceSettings(cache_root=tmp_path / "cache"),
        store=store,
        spot_client=_FakeSpotClient(daily, raw, now),
        smart_money_monitor=_FakeSmartMoneyMonitor(now),  # type: ignore[arg-type]
        now=lambda: now,
    )
    monkeypatch.setattr(
        btc_intelligence,
        "scan_structure_events",
        lambda _frame, _pivots: [event],
    )

    _, revisions, issues = monitor._event_updates(  # noqa: SLF001
        frame,
        (),
        observed_at=now,
        market_context={},
    )

    assert revisions == []
    assert [issue.reason_code for issue in issues] == [
        "BTC_STRUCTURE_EVENT_MISSED_DURING_DOWNTIME"
    ]


def test_latest_touch_freezes_version_features_context_and_costs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 2, 1, 1, 0, tzinfo=UTC)
    daily = _bar_frame(datetime(2023, 8, 1, tzinfo=UTC), 900, timedelta(days=1))
    raw = _bar_frame(datetime(2025, 5, 1, tzinfo=UTC), 1200, timedelta(hours=4))
    frame = add_event_quality_features(add_4h_indicators(raw))
    event = _synthetic_event(frame, len(frame) - 1)
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    processed = datetime.fromtimestamp(
        float(frame.loc[len(frame) - 2, "close_time_ms"]) / 1000,
        tz=UTC,
    )
    _initialize_structure_history(store, started_at=processed, processed_through_at=processed)
    monitor = BinanceBtcIntelligenceMonitor(
        BinanceBtcIntelligenceSettings(cache_root=tmp_path / "cache"),
        store=store,
        spot_client=_FakeSpotClient(daily, raw, now),
        smart_money_monitor=_FakeSmartMoneyMonitor(now),  # type: ignore[arg-type]
        now=lambda: now,
    )
    monkeypatch.setattr(
        btc_intelligence,
        "scan_structure_events",
        lambda _frame, _pivots: [event],
    )

    _, revisions, issues = monitor._event_updates(  # noqa: SLF001
        frame,
        (),
        observed_at=now,
        market_context={"monthly_target": 1, "daily_state": "STRONG_UP"},
    )

    assert issues == []
    assert len(revisions) == 1
    signal = revisions[0].payload["signal"]
    assert signal["level_version"] == "a" * 64
    assert signal["level_effective_at"] != signal["zone_formed_at"]
    assert signal["feature_schema_version"] == "btc-4h-event-features-v1"
    assert signal["features"]["volatility_regime"] in {"LOW", "NORMAL", "HIGH"}
    assert signal["market_context"]["monthly_target"] == 1
    assert signal["cost_assumptions_bps"] == {"base": 30, "stress": 50}


def test_settled_structure_event_keeps_cost_returns_and_excursions(
    tmp_path: Path,
) -> None:
    frame = add_event_quality_features(
        add_4h_indicators(
            _bar_frame(
                datetime(2026, 1, 1, tzinfo=UTC),
                400,
                timedelta(hours=4),
            )
        )
    )
    index = 390
    touch_close = float(frame.loc[index, "close"])
    atr = float(frame.loc[index, "atr"])
    frame.loc[index + 1, "open"] = touch_close
    frame.loc[index + 1, "close"] = touch_close + atr * 1.2
    frame.loc[index + 1, "high"] = touch_close + atr * 1.4
    frame.loc[index + 1, "low"] = touch_close - atr * 0.2
    event_at = datetime.fromtimestamp(
        float(frame.loc[index, "close_time_ms"]) / 1000,
        tz=UTC,
    )
    signal = {
        "event_key": "event-with-forward-path",
        "kind": "SUPPORT",
        "open_time_ms": int(frame.loc[index, "open_time_ms"]),
        "zone_lower": str(touch_close - atr * 0.3),
        "zone_upper": str(touch_close + atr * 0.3),
        "touch_close": str(touch_close),
        "atr_touch": str(atr),
    }
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    _initialize_structure_history(
        store,
        started_at=event_at - timedelta(seconds=1),
        processed_through_at=event_at,
    )
    run_id = store.start_run("btc-market-intelligence", started_at=event_at)
    store.finish_run(
        run_id,
        "btc-market-intelligence",
        CollectionBatch(
            samples=(),
            btc_structure_event_revisions=(
                BtcStructureEventRevision(
                    event_key="event-with-forward-path",
                    event_at=event_at,
                    observed_at=event_at + timedelta(seconds=1),
                    state="PENDING",
                    payload={
                        "algorithm_version": ALGORITHM_VERSION,
                        "signal": signal,
                        "outcome": None,
                    },
                ),
            ),
        ),
        completed_at=event_at + timedelta(seconds=1),
    )
    stored = store.pending_btc_structure_event_revisions(
        "btc-market-intelligence",
        algorithm_version=ALGORITHM_VERSION,
    )[0]

    revision = BinanceBtcIntelligenceMonitor._settle_pending_event(  # noqa: SLF001
        stored,
        frame=frame,
        closes=frame["close"].to_numpy(float),
        open_time_to_index={
            int(value): row_index
            for row_index, value in enumerate(frame["open_time_ms"].to_numpy(np.int64))
        },
        observed_at=event_at + timedelta(days=1),
    )

    assert revision is not None
    assert revision.state == "REACTION"
    outcome = revision.payload["outcome"]
    assert outcome["outcome_bars"] == 1
    assert outcome["gross_return_percent"] is not None
    assert outcome["net_return_30bps_percent"] is not None
    assert outcome["net_return_50bps_percent"] is not None
    assert outcome["maximum_favorable_excursion_percent"] is not None
    assert outcome["maximum_adverse_excursion_percent"] is not None


def test_zone_payload_separates_first_formation_from_current_version() -> None:
    raw = _bar_frame(datetime(2026, 1, 1, tzinfo=UTC), 90, timedelta(hours=4))
    frame = add_4h_indicators(raw)
    atr = float(frame.loc[40, "atr"])
    pivots = [
        Pivot(10, 13, "LOW", 29_000.0, atr, 100.0),
        Pivot(22, 25, "LOW", 29_010.0, atr, 100.0),
        Pivot(40, 43, "LOW", 29_020.0, atr, 100.0),
    ]

    payload = current_structure_payload(frame, pivots, current_price=30_500.0)
    support = next(zone for zone in payload["zones"] if zone["role"] == "SUPPORT")

    assert support["formed_at"] != support["version_effective_at"]
    assert support["effective_at"] == support["version_effective_at"]
    assert len(support["level_version"]) == 64


def test_monthly_ledger_freezes_before_and_captures_fixed_execution_proxy(
    tmp_path: Path,
) -> None:
    daily_all = _bar_frame(
        datetime(2024, 1, 1, tzinfo=UTC),
        800,
        timedelta(days=1),
    )
    four_hour = _bar_frame(
        datetime(2025, 5, 1, tzinfo=UTC),
        1200,
        timedelta(hours=4),
    )
    initial_at = datetime(2026, 1, 15, tzinfo=UTC)
    initial_daily = daily_all[
        daily_all["close_time_ms"] < int(initial_at.timestamp() * 1000)
    ].reset_index(drop=True)
    initial_monthly = monthly_state(
        initial_daily,
        current_price=41_000.0,
        now=initial_at,
    )
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    spot = _FakeSpotClient(initial_daily, four_hour, initial_at)
    monitor = BinanceBtcIntelligenceMonitor(
        BinanceBtcIntelligenceSettings(cache_root=tmp_path / "cache"),
        store=store,
        spot_client=spot,
        smart_money_monitor=_FakeSmartMoneyMonitor(initial_at),  # type: ignore[arg-type]
        now=lambda: initial_at,
    )

    history, revisions, issues = monitor._monthly_updates(  # noqa: SLF001
        initial_monthly,
        observed_at=initial_at,
    )
    assert isinstance(history, BtcMonthlyResearchHistoryObservation)
    assert revisions == []
    assert issues == []
    run_id = store.start_run("btc-market-intelligence", started_at=initial_at)
    store.finish_run(
        run_id,
        "btc-market-intelligence",
        CollectionBatch(samples=(), btc_monthly_research_history=history),
        completed_at=initial_at,
    )

    freeze_at = datetime(2026, 2, 1, 0, 0, 30, tzinfo=UTC)
    january_daily = daily_all[
        daily_all["close_time_ms"] < int(freeze_at.timestamp() * 1000)
    ].reset_index(drop=True)
    january_monthly = monthly_state(
        january_daily,
        current_price=41_000.0,
        now=freeze_at,
    )
    history, revisions, issues = monitor._monthly_updates(  # noqa: SLF001
        january_monthly,
        observed_at=freeze_at,
    )
    assert issues == []
    assert len(revisions) == 1
    frozen = revisions[0]
    assert frozen.state == "SIGNAL_FROZEN"
    assert frozen.observed_at < datetime(2026, 2, 1, 0, 1, tzinfo=UTC)
    assert frozen.payload["signal"]["base_cost_bps"] == 15
    run_id = store.start_run("btc-market-intelligence", started_at=freeze_at)
    store.finish_run(
        run_id,
        "btc-market-intelligence",
        CollectionBatch(
            samples=(),
            btc_monthly_research_history=history,
            btc_monthly_research_revisions=tuple(revisions),
        ),
        completed_at=freeze_at,
    )

    settle_at = datetime(2026, 2, 1, 0, 2, 30, tzinfo=UTC)
    history, revisions, issues = monitor._monthly_updates(  # noqa: SLF001
        january_monthly,
        observed_at=settle_at,
    )
    assert issues == []
    assert len(revisions) == 1
    assert revisions[0].state == "EXECUTION_CAPTURED"
    assert revisions[0].payload["execution"]["price"] == "42000"
    run_id = store.start_run("btc-market-intelligence", started_at=settle_at)
    store.finish_run(
        run_id,
        "btc-market-intelligence",
        CollectionBatch(
            samples=(),
            btc_monthly_research_history=history,
            btc_monthly_research_revisions=tuple(revisions),
        ),
        completed_at=settle_at,
    )

    latest = store.latest_btc_monthly_research_revisions(
        "btc-market-intelligence",
        algorithm_version=MONTHLY_ALGORITHM_VERSION,
    )
    assert len(latest) == 1
    assert latest[0].revision_no == 2
    assert latest[0].state == "EXECUTION_CAPTURED"
    summary = store.btc_monthly_research_summary(
        "btc-market-intelligence",
        algorithm_version=MONTHLY_ALGORITHM_VERSION,
    )
    assert summary["signal_count"] == 1
    assert summary["execution_count"] == 1
    assert summary["pending_execution_count"] == 0


def test_monthly_ledger_marks_signal_missed_when_first_seen_after_execution(
    tmp_path: Path,
) -> None:
    daily_all = _bar_frame(
        datetime(2024, 1, 1, tzinfo=UTC),
        800,
        timedelta(days=1),
    )
    four_hour = _bar_frame(
        datetime(2025, 5, 1, tzinfo=UTC),
        1200,
        timedelta(hours=4),
    )
    initial_at = datetime(2026, 1, 15, tzinfo=UTC)
    initial_daily = daily_all[
        daily_all["close_time_ms"] < int(initial_at.timestamp() * 1000)
    ].reset_index(drop=True)
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = BinanceBtcIntelligenceMonitor(
        BinanceBtcIntelligenceSettings(cache_root=tmp_path / "cache"),
        store=store,
        spot_client=_FakeSpotClient(initial_daily, four_hour, initial_at),
        smart_money_monitor=_FakeSmartMoneyMonitor(initial_at),  # type: ignore[arg-type]
        now=lambda: initial_at,
    )
    initial_monthly = monthly_state(
        initial_daily,
        current_price=41_000.0,
        now=initial_at,
    )
    history, _, _ = monitor._monthly_updates(  # noqa: SLF001
        initial_monthly,
        observed_at=initial_at,
    )
    run_id = store.start_run("btc-market-intelligence", started_at=initial_at)
    store.finish_run(
        run_id,
        "btc-market-intelligence",
        CollectionBatch(samples=(), btc_monthly_research_history=history),
        completed_at=initial_at,
    )

    late_at = datetime(2026, 2, 1, 0, 1, tzinfo=UTC)
    january_daily = daily_all[
        daily_all["close_time_ms"] < int(late_at.timestamp() * 1000)
    ].reset_index(drop=True)
    january_monthly = monthly_state(
        january_daily,
        current_price=41_000.0,
        now=late_at,
    )
    _, revisions, issues = monitor._monthly_updates(  # noqa: SLF001
        january_monthly,
        observed_at=late_at,
    )

    assert revisions == []
    assert [issue.reason_code for issue in issues] == [
        "BTC_MONTHLY_SIGNAL_MISSED_BEFORE_EXECUTION"
    ]


def test_composite_monitor_emits_one_evidence_tiered_snapshot(tmp_path: Path) -> None:
    now = datetime(2026, 2, 1, 1, 0, tzinfo=UTC)
    daily = _bar_frame(datetime(2023, 8, 1, tzinfo=UTC), 900, timedelta(days=1))
    four_hour = _bar_frame(datetime(2025, 5, 1, tzinfo=UTC), 1200, timedelta(hours=4))
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = BinanceBtcIntelligenceMonitor(
        BinanceBtcIntelligenceSettings(cache_root=tmp_path / "cache"),
        store=store,
        spot_client=_FakeSpotClient(daily, four_hour, now),
        smart_money_monitor=_FakeSmartMoneyMonitor(now),  # type: ignore[arg-type]
        now=lambda: now,
    )

    batch = monitor.collect()

    assert len(batch.samples) == 1
    snapshot = batch.samples[0].payload
    assert snapshot["row_type"] == "BTC_INTELLIGENCE"
    assert snapshot["monthly"]["authority"] == "RESEARCH_TARGET_ONLY"
    assert snapshot["daily"]["authority"] == "STATE_ONLY"
    assert snapshot["structure"]["model_status"] == "NOT_DEPLOYED"
    assert snapshot["smart_money"]["authority"] == "CONTEXT_ONLY"
    assert batch.btc_structure_history is not None
    assert batch.btc_structure_event_revisions == ()


def test_unused_stale_smart_money_overview_does_not_degrade_complete_rows(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 1, 1, 0, tzinfo=UTC)
    daily = _bar_frame(datetime(2023, 8, 1, tzinfo=UTC), 900, timedelta(days=1))
    four_hour = _bar_frame(datetime(2025, 5, 1, tzinfo=UTC), 1200, timedelta(hours=4))
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = BinanceBtcIntelligenceMonitor(
        BinanceBtcIntelligenceSettings(cache_root=tmp_path / "cache"),
        store=store,
        spot_client=_FakeSpotClient(daily, four_hour, now),
        smart_money_monitor=_StaleOverviewSmartMoneyMonitor(now),  # type: ignore[arg-type]
        now=lambda: now,
    )

    batch = monitor.collect()

    assert "SMART_MONEY_OVERVIEW_STALE" not in {
        issue.reason_code for issue in batch.issues
    }
    assert batch.samples[0].payload["smart_money"]["status"] == "AVAILABLE"
    assert batch.samples[0].payload["smart_money"]["unused_context_issues"] == [
        "SMART_MONEY_OVERVIEW_STALE"
    ]
