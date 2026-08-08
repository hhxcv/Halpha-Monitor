import io
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError

import numpy as np
import pandas as pd

from halpha_monitor.monitors.binance_btc_relationship import (
    BinanceBtcRelationshipMonitor,
    BinanceBtcRelationshipSettings,
    BinanceSpotDailyClient,
    DailySeriesResult,
    MAX_KLINE_RESPONSE_BYTES,
    _price_series,
    analyze_pair,
    latest_closed_cutoff,
    normalize_klines,
)


def price_frame(days: int, *, beta: float = 1.0) -> pd.DataFrame:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    index = np.arange(days, dtype=float)
    returns = 0.001 + 0.008 * np.sin(index / 11)
    prices = 100 * np.exp(np.cumsum(returns * beta))
    open_times = [
        int((start + timedelta(days=offset)).timestamp() * 1000)
        for offset in range(days)
    ]
    return pd.DataFrame(
        {
            "open_time_ms": open_times,
            "close_time_ms": [value + 86_399_999 for value in open_times],
            "close": prices,
        }
    )


def result(symbol: str, frame: pd.DataFrame) -> DailySeriesResult:
    latest = datetime.fromtimestamp(
        int(frame["close_time_ms"].max()) / 1000,
        tz=UTC,
    )
    return DailySeriesResult(
        symbol=symbol,
        status="FETCHED",
        frame=frame,
        latest_close_at=latest,
        acquired_at=latest,
    )


class FakeProvider:
    def __init__(self, values: dict[str, DailySeriesResult]) -> None:
        self.values = values

    def fetch(self, symbol: str, cutoff: datetime) -> DailySeriesResult:
        return self.values[symbol]


class ThrottledOpenUrl:
    def __init__(self, retry_after: str = "120") -> None:
        self.calls = 0
        self.retry_after = retry_after

    def __call__(self, request, timeout):  # type: ignore[no-untyped-def]
        self.calls += 1
        raise HTTPError(
            request.full_url,
            429,
            "Too Many Requests",
            hdrs={"Retry-After": self.retry_after},
            fp=io.BytesIO(b'{"code":-1003,"msg":"rate limited"}'),
        )


class ConcurrentThrottledOpenUrl(ThrottledOpenUrl):
    def __init__(self) -> None:
        super().__init__(retry_after="0")
        self._barrier = threading.Barrier(2)
        self._lock = threading.Lock()

    def __call__(self, request, timeout):  # type: ignore[no-untyped-def]
        with self._lock:
            call_number = self.calls + 1
            self.calls = call_number
        if call_number <= 2:
            self._barrier.wait(timeout=2)
        raise HTTPError(
            request.full_url,
            429,
            "Too Many Requests",
            hdrs={"Retry-After": self.retry_after},
            fp=io.BytesIO(b'{"code":-1003,"msg":"rate limited"}'),
        )


class OversizedResponse:
    def __init__(self) -> None:
        self.read_sizes: list[int] = []

    def __enter__(self) -> "OversizedResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        return b"x" * size


def test_spot_client_bounds_each_kline_response_read(tmp_path: Path) -> None:
    response = OversizedResponse()
    client = BinanceSpotDailyClient(
        tmp_path,
        attempts=1,
        open_url=lambda *_args, **_kwargs: response,
    )

    result = client.fetch(
        "BTCUSDT",
        datetime(2026, 8, 4, tzinfo=UTC) - timedelta(milliseconds=1),
    )

    assert response.read_sizes == [MAX_KLINE_RESPONSE_BYTES + 1]
    assert result.status == "FAILED"
    assert result.reason_code == "BTC_RELATIONSHIP_RESPONSE_TOO_LARGE"


def test_normalize_klines_keeps_only_valid_closed_rows() -> None:
    cutoff = 200_000
    rows = [
        [0, "1", "2", "0.5", "1.5", "10", 99_999, "0", 1, "0", "0", "0"],
        [100_000, "1", "2", "0.5", "1.6", "10", 199_999, "0", 1, "0", "0", "0"],
        [200_000, "1", "2", "0.5", "1.7", "10", 299_999, "0", 1, "0", "0", "0"],
    ]

    frame = normalize_klines(rows, cutoff)

    assert frame["close"].tolist() == [1.5, 1.6]


def test_spot_client_honors_retry_after_and_shares_backoff_across_symbols(
    tmp_path: Path,
) -> None:
    opener = ThrottledOpenUrl()
    monotonic_now = 100.0
    client = BinanceSpotDailyClient(
        tmp_path,
        attempts=3,
        open_url=opener,
        monotonic=lambda: monotonic_now,
        wall_now=lambda: datetime(2026, 8, 4, tzinfo=UTC),
        sleep=lambda _seconds: None,
    )
    cutoff = datetime(2026, 8, 4, tzinfo=UTC) - timedelta(milliseconds=1)

    first = client.fetch("BTCUSDT", cutoff)
    second = client.fetch("ETHUSDT", cutoff)

    assert first.status == "FAILED"
    assert second.status == "FAILED"
    assert first.reason_code == "BTC_RELATIONSHIP_HTTP_THROTTLED"
    assert second.reason_code == "BTC_RELATIONSHIP_HTTP_THROTTLED"
    assert opener.calls == 1


def test_concurrent_rate_limits_open_only_one_backoff_window(tmp_path: Path) -> None:
    opener = ConcurrentThrottledOpenUrl()
    clock = {"value": 100.0}
    client = BinanceSpotDailyClient(
        tmp_path,
        attempts=1,
        open_url=opener,
        monotonic=lambda: clock["value"],
        wall_now=lambda: datetime(2026, 8, 4, tzinfo=UTC),
        sleep=lambda _seconds: None,
    )
    cutoff = datetime(2026, 8, 4, tzinfo=UTC) - timedelta(milliseconds=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(client.fetch, "BTCUSDT", cutoff)
        second = executor.submit(client.fetch, "ETHUSDT", cutoff)
        assert first.result().reason_code == "BTC_RELATIONSHIP_HTTP_THROTTLED"
        assert second.result().reason_code == "BTC_RELATIONSHIP_HTTP_THROTTLED"

    clock["value"] = 161.0
    assert client.fetch("SOLUSDT", cutoff).reason_code == (
        "BTC_RELATIONSHIP_HTTP_THROTTLED"
    )
    assert opener.calls == 3


def test_analysis_reuses_aligned_closed_returns_for_relationship_metrics() -> None:
    btc = _price_series(price_frame(500, beta=1.0))
    asset = _price_series(price_frame(500, beta=1.5))

    analysis = analyze_pair(asset, btc)

    assert analysis["status"] == "ANALYZED"
    assert analysis["n_obs"] == 365
    assert analysis["pearson"] > 0.999
    assert 1.49 < analysis["beta"] < 1.51
    assert analysis["r_squared"] > 0.999


def test_monitor_keeps_insufficient_asset_metrics_empty_without_substitution(
    tmp_path: Path,
) -> None:
    btc = price_frame(500, beta=1.0)
    eth = price_frame(500, beta=1.3)
    new = price_frame(30, beta=0.7)
    provider = FakeProvider(
        {
            "BTCUSDT": result("BTCUSDT", btc),
            "ETHUSDT": result("ETHUSDT", eth),
            "NEWUSDT": result("NEWUSDT", new),
        }
    )
    monitor = BinanceBtcRelationshipMonitor(
        BinanceBtcRelationshipSettings(
            cache_root=tmp_path,
            symbols=("BTCUSDT", "ETHUSDT", "NEWUSDT"),
            workers=2,
        ),
        provider,
    )

    batch = monitor.collect()

    by_symbol = {sample.entity_key: sample for sample in batch.samples}
    assert batch.issues == ()
    assert by_symbol["ETHUSDT"].payload["pearson"] is not None
    assert by_symbol["NEWUSDT"].value_text == ""
    assert by_symbol["NEWUSDT"].payload["pearson"] is None
    assert by_symbol["NEWUSDT"].payload["data_state"].startswith("样本不足")
    assert "指标保持为空" in by_symbol["NEWUSDT"].payload["missing_reasons"]["pearson"]


def test_monitor_marks_failed_asset_missing_but_preserves_other_valid_rows(
    tmp_path: Path,
) -> None:
    btc = price_frame(500, beta=1.0)
    eth = price_frame(500, beta=1.3)
    missing = DailySeriesResult(
        symbol="MISSUSDT",
        status="FAILED",
        frame=pd.DataFrame(),
        latest_close_at=None,
        acquired_at=None,
        reason_code="BTC_RELATIONSHIP_UPSTREAM_UNAVAILABLE",
    )
    monitor = BinanceBtcRelationshipMonitor(
        BinanceBtcRelationshipSettings(
            cache_root=tmp_path,
            symbols=("BTCUSDT", "ETHUSDT", "MISSUSDT"),
            workers=2,
        ),
        FakeProvider(
            {
                "BTCUSDT": result("BTCUSDT", btc),
                "ETHUSDT": result("ETHUSDT", eth),
                "MISSUSDT": missing,
            }
        ),
    )

    batch = monitor.collect()

    by_symbol = {sample.entity_key: sample for sample in batch.samples}
    assert by_symbol["ETHUSDT"].payload["data_state"].startswith("可用")
    assert by_symbol["MISSUSDT"].payload["pearson"] is None
    assert batch.issues[0].scope == "MISSUSDT"


def test_daily_relationship_computation_waits_until_next_closed_utc_day(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
    cutoff = latest_closed_cutoff(now)

    class StateStore:
        def __init__(self, stored_cutoff: datetime, *, status: str = "SUCCESS") -> None:
            self.stored_cutoff = stored_cutoff
            self.status = status

        def latest_finished_run(self, monitor_id):  # type: ignore[no-untyped-def]
            del monitor_id
            return SimpleNamespace(status=self.status)

        def latest_samples_by_entity(self, monitor_id, entity_keys):  # type: ignore[no-untyped-def]
            del monitor_id, entity_keys
            return (
                SimpleNamespace(
                    payload={"data_cutoff_at": self.stored_cutoff.isoformat()}
                ),
            )

    settings = BinanceBtcRelationshipSettings(
        cache_root=tmp_path,
        symbols=("BTCUSDT", "ETHUSDT"),
    )
    current = BinanceBtcRelationshipMonitor(
        settings,
        FakeProvider({}),
        store=StateStore(cutoff),  # type: ignore[arg-type]
    ).automatic_collection_state(now=now)
    pending = BinanceBtcRelationshipMonitor(
        settings,
        FakeProvider({}),
        store=StateStore(cutoff - timedelta(days=1)),  # type: ignore[arg-type]
    ).automatic_collection_state(now=now)
    partial = BinanceBtcRelationshipMonitor(
        settings,
        FakeProvider({}),
        store=StateStore(cutoff, status="PARTIAL"),  # type: ignore[arg-type]
    ).automatic_collection_state(now=now)

    assert current.allowed is False
    assert current.status == "CLOSED"
    assert current.next_open_at == datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
    assert pending.allowed is True
    assert pending.status == "OPEN"
    assert partial.allowed is True
    assert partial.status == "OPEN"
