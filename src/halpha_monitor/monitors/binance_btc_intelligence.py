"""BTC professional intelligence from closed Spot bars and Smart Money context.

The monitor deliberately keeps four evidence tiers separate: the medium-evidence
monthly Faber state, state-only daily Donchian components, research-only causal
4h zones, and contextual Binance USDⓈ-M Smart Money observations.  It has no
account, order, position-sizing, or trading-control capability.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener

import numpy as np
import pandas as pd

from halpha_monitor.contracts import (
    BtcMonthlyResearchHistoryObservation,
    BtcMonthlyResearchRevision,
    BtcStructureEventRevision,
    BtcStructureHistoryObservation,
    CollectionArtifact,
    CollectionBatch,
    CollectionCancelled,
    CollectionIssue,
    MetricSample,
    MonitorView,
    ViewColumn,
)
from halpha_monitor.monitors.binance_smart_money import (
    BinanceSmartMoneyMonitor,
    BinanceSmartMoneySettings,
)
from halpha_monitor.store import (
    SQLiteMonitorStore,
    StoredBtcMonthlyResearchRevision,
    StoredBtcStructureEventRevision,
    iso_utc,
    parse_utc,
)
from halpha_monitor.telemetry import NetworkRequestWindow


SYMBOL = "BTCUSDT"
ALGORITHM_VERSION = "btc-structure-causal-v2"
EVENT_FEATURE_SCHEMA_VERSION = "btc-4h-event-features-v1"
MONTHLY_ALGORITHM_VERSION = "btc-faber-10m-forward-v1"
BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
BINANCE_TICKER_URL = "https://data-api.binance.vision/api/v3/ticker/price"
USER_AGENT = "Halpha-Monitor/1.0"
MAX_RESPONSE_BYTES = 4 * 1024 * 1024

DAY_MS = 86_400_000
FOUR_HOUR_MS = 4 * 60 * 60 * 1000
MINUTE_MS = 60_000
DAILY_HISTORY_BARS = 900
FOUR_HOUR_HISTORY_BARS = 1_500
DONCHIAN_WINDOWS = (20, 30, 60, 90)

ATR_PERIOD = 14
ADX_PERIOD = 14
EMA_FAST = 20
EMA_SLOW = 50
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
LOOKBACK_BARS = 1_080
HALF_LIFE_BARS = 540
MIN_ANCHOR_SEPARATION = 12
MIN_ZONE_SCORE = 1.25
MERGE_ATR_MULTIPLE = 0.60
ZONE_ATR_HALF_WIDTH = 0.30
ZONE_PRICE_HALF_WIDTH = 0.0025
EVENT_COOLDOWN_BARS = 12
OUTCOME_HORIZON_BARS = 6
REACTION_ATR_MULTIPLE = 1.0
BREAK_ATR_MULTIPLE = 0.5
EVENT_BASE_COST_BPS = 30
EVENT_STRESS_COST_BPS = 50
MONTHLY_BASE_COST_BPS = 15
MONTHLY_STRESS_COST_BPS = 30

TREND_GAP_THRESHOLD = 0.35
TREND_SLOPE_THRESHOLD = 0.15
TREND_ADX_THRESHOLD = 20.0


def utc_now() -> datetime:
    return datetime.now(UTC)


def _float_text(value: float | None) -> str | None:
    if value is None or not math.isfinite(value):
        return None
    return format(value, ".12g")


def _latest_closed_cutoff(now: datetime, interval_ms: int) -> datetime:
    current_ms = int(now.astimezone(UTC).timestamp() * 1000)
    cutoff_ms = (current_ms // interval_ms) * interval_ms - 1
    return datetime.fromtimestamp(cutoff_ms / 1000, tz=UTC)


def _read_frame(path: Path) -> pd.DataFrame:
    columns = [
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time_ms",
        "quote_volume",
        "trade_count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    ]
    if not path.is_file():
        return pd.DataFrame(columns=columns)
    try:
        frame = pd.read_csv(path)
    except (OSError, ValueError):
        return pd.DataFrame(columns=columns)
    if not set(columns).issubset(frame.columns):
        return pd.DataFrame(columns=columns)
    for column in ("open_time_ms", "close_time_ms", "trade_count"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in set(columns).difference(
        {"open_time_ms", "close_time_ms", "trade_count"}
    ):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.dropna(subset=columns)
        .drop_duplicates(subset=["open_time_ms"], keep="last")
        .sort_values("open_time_ms")
        .reset_index(drop=True)
    )


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.to_csv(
            temporary,
            index=False,
            compression="gzip",
            float_format="%.12g",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def normalize_spot_klines(rows: Any, cutoff_ms: int) -> pd.DataFrame:
    if not isinstance(rows, list):
        raise ValueError("BTC_INTELLIGENCE_KLINE_SCHEMA_CHANGED")
    normalized: list[dict[str, int | float]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 11:
            raise ValueError("BTC_INTELLIGENCE_KLINE_SCHEMA_CHANGED")
        try:
            item = {
                "open_time_ms": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time_ms": int(row[6]),
                "quote_volume": float(row[7]),
                "trade_count": int(row[8]),
                "taker_buy_volume": float(row[9]),
                "taker_buy_quote_volume": float(row[10]),
            }
        except (TypeError, ValueError, OverflowError):
            continue
        prices = [item[key] for key in ("open", "high", "low", "close")]
        if (
            item["open_time_ms"] < 0
            or item["close_time_ms"] < item["open_time_ms"]
            or item["close_time_ms"] > cutoff_ms
            or any(not math.isfinite(float(value)) or float(value) <= 0 for value in prices)
            or not math.isfinite(float(item["volume"]))
            or float(item["volume"]) < 0
            or float(item["high"]) < max(float(item["open"]), float(item["close"]))
            or float(item["low"]) > min(float(item["open"]), float(item["close"]))
        ):
            continue
        normalized.append(item)
    if not normalized:
        return pd.DataFrame()
    return (
        pd.DataFrame(normalized)
        .drop_duplicates(subset=["open_time_ms"], keep="last")
        .sort_values("open_time_ms")
        .reset_index(drop=True)
    )


def validate_closed_bars(
    frame: pd.DataFrame,
    *,
    interval_ms: int,
    minimum_rows: int,
) -> None:
    if len(frame) < minimum_rows:
        raise ValueError("BTC_INTELLIGENCE_KLINES_INSUFFICIENT")
    if frame["open_time_ms"].duplicated().any():
        raise ValueError("BTC_INTELLIGENCE_KLINES_DUPLICATE")
    gaps = frame["open_time_ms"].diff().dropna()
    if (gaps != interval_ms).any():
        raise ValueError("BTC_INTELLIGENCE_KLINES_NON_CONTIGUOUS")
    values = frame[["open", "high", "low", "close", "volume"]].to_numpy(float)
    if not np.isfinite(values).all():
        raise ValueError("BTC_INTELLIGENCE_KLINES_INVALID")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("BTC_INTELLIGENCE_KLINES_INVALID")
    if (frame["volume"] < 0).any():
        raise ValueError("BTC_INTELLIGENCE_KLINES_INVALID")
    if (frame["high"] < frame[["open", "close"]].max(axis=1)).any():
        raise ValueError("BTC_INTELLIGENCE_KLINES_INVALID")
    if (frame["low"] > frame[["open", "close"]].min(axis=1)).any():
        raise ValueError("BTC_INTELLIGENCE_KLINES_INVALID")


@dataclass(frozen=True)
class SpotSeriesResult:
    interval: str
    status: str
    frame: pd.DataFrame
    cutoff_at: datetime
    acquired_at: datetime | None
    reason_code: str | None = None

    @property
    def current(self) -> bool:
        return self.status in {"FETCHED", "CACHE_CURRENT", "CACHE_CURRENT_AFTER_ERROR"}


class SpotMarketProvider(Protocol):
    def fetch_bars(
        self,
        *,
        interval: str,
        cutoff: datetime,
        history_bars: int,
    ) -> SpotSeriesResult: ...

    def fetch_price(self) -> tuple[float, datetime]: ...

    def fetch_execution_price(self, at: datetime) -> tuple[float, datetime]: ...


class SpotMarketError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class BinanceSpotMarketClient:
    """Bounded official Spot client with durable normalized 1d/4h caches."""

    def __init__(
        self,
        cache_root: Path,
        *,
        timeout_seconds: float = 10,
        attempts: int = 2,
        proxy_url: str | None = None,
        opener: OpenerDirector | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wall_now: Callable[[], datetime] = utc_now,
    ) -> None:
        if timeout_seconds <= 0 or attempts < 1:
            raise ValueError("BTC_INTELLIGENCE_CLIENT_CONFIGURATION_INVALID")
        if opener is not None and proxy_url is not None:
            raise ValueError("opener and proxy_url are mutually exclusive")
        self.cache_root = cache_root.resolve()
        self.timeout_seconds = timeout_seconds
        self.attempts = attempts
        self.opener = opener or (
            build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
            if proxy_url
            else build_opener()
        )
        self._monotonic = monotonic
        self._wall_now = wall_now
        self._throttle_until = 0.0
        self._throttle_failures = 0
        self._throttle_lock = threading.Lock()
        self._network_requests = NetworkRequestWindow(monotonic=monotonic)
        self._stop_event: threading.Event | None = None

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event

    def _raise_if_cancelled(self) -> None:
        if self._stop_event is not None and self._stop_event.is_set():
            raise CollectionCancelled("BTC_INTELLIGENCE_COLLECTION_CANCELLED")

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return self._network_requests.count(window_seconds=window_seconds)

    def _throttle_active(self) -> bool:
        with self._throttle_lock:
            return self._monotonic() < self._throttle_until

    def _open_backoff(self, error: HTTPError) -> None:
        retry_after: float | None = None
        raw = error.headers.get("Retry-After") if error.headers else None
        if raw:
            try:
                retry_after = max(0.0, float(raw))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(raw)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=UTC)
                    retry_after = max(
                        0.0,
                        (retry_at.astimezone(UTC) - self._wall_now()).total_seconds(),
                    )
                except (TypeError, ValueError, OverflowError):
                    retry_after = None
        with self._throttle_lock:
            now = self._monotonic()
            if now >= self._throttle_until:
                self._throttle_failures += 1
            fallback = min(3600.0, 60.0 * (2 ** min(self._throttle_failures - 1, 6)))
            self._throttle_until = max(self._throttle_until, now + max(fallback, retry_after or 0))

    def _record_success(self) -> None:
        with self._throttle_lock:
            if self._monotonic() >= self._throttle_until:
                self._throttle_until = 0.0
                self._throttle_failures = 0

    def _get_json(self, url: str) -> Any:
        self._raise_if_cancelled()
        if self._throttle_active():
            raise SpotMarketError("BTC_INTELLIGENCE_HTTP_BACKOFF_ACTIVE")
        request = Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        self._network_requests.record()
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise SpotMarketError("BTC_INTELLIGENCE_RESPONSE_TOO_LARGE")
            return json.loads(body.decode("utf-8"))
        except HTTPError as exc:
            if exc.code in {418, 429}:
                self._open_backoff(exc)
                raise SpotMarketError("BTC_INTELLIGENCE_HTTP_THROTTLED") from None
            raise SpotMarketError("BTC_INTELLIGENCE_UPSTREAM_HTTP_ERROR") from None
        except (URLError, TimeoutError, OSError):
            raise SpotMarketError("BTC_INTELLIGENCE_UPSTREAM_UNAVAILABLE") from None
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise SpotMarketError("BTC_INTELLIGENCE_RESPONSE_INVALID") from None

    def fetch_bars(
        self,
        *,
        interval: str,
        cutoff: datetime,
        history_bars: int,
    ) -> SpotSeriesResult:
        if interval not in {"1d", "4h"} or history_bars < 100:
            raise ValueError("BTC_INTELLIGENCE_SERIES_REQUEST_INVALID")
        self._raise_if_cancelled()
        interval_ms = DAY_MS if interval == "1d" else FOUR_HOUR_MS
        cache_path = self.cache_root / f"{SYMBOL}-{interval}.csv.gz"
        current = _read_frame(cache_path)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        latest_ms = int(current["close_time_ms"].max()) if not current.empty else None
        if latest_ms is not None and latest_ms >= cutoff_ms:
            return SpotSeriesResult(
                interval,
                "CACHE_CURRENT",
                current.tail(history_bars).reset_index(drop=True),
                cutoff,
                datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC),
            )

        earliest_ms = cutoff_ms - (history_bars + 20) * interval_ms
        cursor = (
            earliest_ms
            if current.empty or int(current["open_time_ms"].min()) > earliest_ms
            else max(earliest_ms, int(current["open_time_ms"].max()) - 3 * interval_ms)
        )
        last_reason = "BTC_INTELLIGENCE_UPSTREAM_UNAVAILABLE"
        max_pages = math.ceil((history_bars + 24) / 1000) + 1
        for attempt in range(self.attempts):
            fetched: list[pd.DataFrame] = []
            page_cursor = cursor
            try:
                for _page in range(max_pages):
                    params = {
                        "symbol": SYMBOL,
                        "interval": interval,
                        "startTime": page_cursor,
                        "endTime": cutoff_ms,
                        "timeZone": "0",
                        "limit": 1000,
                    }
                    payload = self._get_json(f"{BINANCE_KLINES_URL}?{urlencode(params)}")
                    normalized = normalize_spot_klines(payload, cutoff_ms)
                    if normalized.empty:
                        break
                    fetched.append(normalized)
                    next_cursor = int(normalized["open_time_ms"].max()) + interval_ms
                    if next_cursor <= page_cursor:
                        raise SpotMarketError("BTC_INTELLIGENCE_PAGINATION_STALLED")
                    page_cursor = next_cursor
                    if len(payload) < 1000 or page_cursor > cutoff_ms:
                        break
                frames = ([current] if not current.empty else []) + fetched
                combined = (
                    pd.concat(frames, ignore_index=True)
                    if frames
                    else pd.DataFrame()
                )
                combined = (
                    combined.drop_duplicates(subset=["open_time_ms"], keep="last")
                    .loc[lambda value: value["close_time_ms"] <= cutoff_ms]
                    .sort_values("open_time_ms")
                    .tail(history_bars + 20)
                    .reset_index(drop=True)
                )
                minimum_rows = 120 if interval == "1d" else LOOKBACK_BARS + 60
                validate_closed_bars(
                    combined,
                    interval_ms=interval_ms,
                    minimum_rows=minimum_rows,
                )
                combined_latest = int(combined["close_time_ms"].max())
                if combined_latest < cutoff_ms:
                    return SpotSeriesResult(
                        interval,
                        "STALE",
                        combined.tail(history_bars).reset_index(drop=True),
                        cutoff,
                        utc_now(),
                        "BTC_INTELLIGENCE_SOURCE_STALE",
                    )
                _write_frame(combined, cache_path)
                self._record_success()
                return SpotSeriesResult(
                    interval,
                    "FETCHED",
                    combined.tail(history_bars).reset_index(drop=True),
                    cutoff,
                    utc_now(),
                )
            except SpotMarketError as exc:
                last_reason = exc.reason_code
                retryable = exc.reason_code in {
                    "BTC_INTELLIGENCE_UPSTREAM_HTTP_ERROR",
                    "BTC_INTELLIGENCE_UPSTREAM_UNAVAILABLE",
                    "BTC_INTELLIGENCE_RESPONSE_INVALID",
                }
            except ValueError as exc:
                last_reason = str(exc)
                retryable = False
            if not retryable or attempt + 1 >= self.attempts:
                break
            delay = 0.5 * (2**attempt)
            if self._stop_event is not None and self._stop_event.wait(delay):
                raise CollectionCancelled("BTC_INTELLIGENCE_COLLECTION_CANCELLED")
            if self._stop_event is None:
                time.sleep(delay)

        if latest_ms is not None and latest_ms >= cutoff_ms:
            return SpotSeriesResult(
                interval,
                "CACHE_CURRENT_AFTER_ERROR",
                current.tail(history_bars).reset_index(drop=True),
                cutoff,
                datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC),
                last_reason,
            )
        return SpotSeriesResult(
            interval,
            "FAILED" if current.empty else "STALE",
            current.tail(history_bars).reset_index(drop=True),
            cutoff,
            (
                datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC)
                if cache_path.is_file()
                else None
            ),
            last_reason,
        )

    def fetch_price(self) -> tuple[float, datetime]:
        payload = self._get_json(f"{BINANCE_TICKER_URL}?{urlencode({'symbol': SYMBOL})}")
        if not isinstance(payload, dict) or payload.get("symbol") != SYMBOL:
            raise SpotMarketError("BTC_INTELLIGENCE_TICKER_SCHEMA_CHANGED")
        try:
            price = float(payload["price"])
        except (KeyError, TypeError, ValueError, OverflowError):
            raise SpotMarketError("BTC_INTELLIGENCE_TICKER_SCHEMA_CHANGED") from None
        if not math.isfinite(price) or price <= 0:
            raise SpotMarketError("BTC_INTELLIGENCE_TICKER_INVALID")
        self._record_success()
        return price, utc_now()

    def fetch_execution_price(self, at: datetime) -> tuple[float, datetime]:
        execution_at = at.astimezone(UTC).replace(second=0, microsecond=0)
        start_ms = int(execution_at.timestamp() * 1000)
        params = {
            "symbol": SYMBOL,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": start_ms + MINUTE_MS - 1,
            "timeZone": "0",
            "limit": 1,
        }
        payload = self._get_json(f"{BINANCE_KLINES_URL}?{urlencode(params)}")
        frame = normalize_spot_klines(payload, start_ms + MINUTE_MS - 1)
        if frame.empty or int(frame.loc[0, "open_time_ms"]) != start_ms:
            raise SpotMarketError("BTC_MONTHLY_EXECUTION_PROXY_UNAVAILABLE")
        price = float(frame.loc[0, "open"])
        if not math.isfinite(price) or price <= 0:
            raise SpotMarketError("BTC_MONTHLY_EXECUTION_PROXY_INVALID")
        self._record_success()
        return price, execution_at


def _wilder_average(values: pd.Series, period: int) -> pd.Series:
    return values.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def add_4h_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    previous_close = output["close"].shift(1)
    true_range = pd.concat(
        [
            output["high"] - output["low"],
            (output["high"] - previous_close).abs(),
            (output["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    output["atr"] = _wilder_average(true_range, ATR_PERIOD)
    up_move = output["high"].diff()
    down_move = -output["low"].diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=output.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=output.index,
    )
    smoothed_tr = _wilder_average(true_range, ADX_PERIOD)
    plus_di = 100 * _wilder_average(plus_dm, ADX_PERIOD) / smoothed_tr
    minus_di = 100 * _wilder_average(minus_dm, ADX_PERIOD) / smoothed_tr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    output["adx"] = _wilder_average(dx, ADX_PERIOD)
    output["ema_fast"] = output["close"].ewm(
        span=EMA_FAST, adjust=False, min_periods=EMA_FAST
    ).mean()
    output["ema_slow"] = output["close"].ewm(
        span=EMA_SLOW, adjust=False, min_periods=EMA_SLOW
    ).mean()
    output["trend_gap"] = (output["ema_fast"] - output["ema_slow"]) / output["atr"]
    output["trend_slope"] = (output["ema_slow"] - output["ema_slow"].shift(6)) / output["atr"]
    gap = output["trend_gap"]
    slope = output["trend_slope"]
    adx = output["adx"]
    trend = np.full(len(output), "RANGE", dtype=object)
    trend[(gap >= TREND_GAP_THRESHOLD) & (slope >= TREND_SLOPE_THRESHOLD) & (adx >= TREND_ADX_THRESHOLD)] = "UP"
    trend[(gap <= -TREND_GAP_THRESHOLD) & (slope <= -TREND_SLOPE_THRESHOLD) & (adx >= TREND_ADX_THRESHOLD)] = "DOWN"
    output["trend_state"] = trend
    return output


def add_event_quality_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add the frozen causal inputs required by the forward research ledger."""
    output = frame.copy()
    close = output["close"].astype(float)
    log_return = np.log(close).diff()
    output["ema120"] = close.ewm(
        span=120,
        adjust=False,
        min_periods=120,
    ).mean()
    output["ema300"] = close.ewm(
        span=300,
        adjust=False,
        min_periods=300,
    ).mean()
    output["htf_gap"] = (output["ema120"] - output["ema300"]) / output["atr"]
    output["htf_slope"] = (
        output["ema300"] - output["ema300"].shift(30)
    ) / output["atr"]
    output["rv6"] = log_return.rolling(6, min_periods=6).std(ddof=0)
    output["rv30"] = log_return.rolling(30, min_periods=30).std(ddof=0)

    def rolling_zscore(values: pd.Series) -> pd.Series:
        mean = values.rolling(30, min_periods=20).mean()
        std = values.rolling(30, min_periods=20).std(ddof=0).replace(0, np.nan)
        return (values - mean) / std

    output["volume_z30"] = rolling_zscore(np.log1p(output["volume"]))
    output["quote_volume_z30"] = rolling_zscore(
        np.log1p(output["quote_volume"])
    )
    output["trade_count_z30"] = rolling_zscore(
        np.log1p(output["trade_count"].astype(float))
    )
    output["taker_buy_share"] = (
        output["taker_buy_quote_volume"]
        / output["quote_volume"].replace(0, np.nan)
    )
    return output


@dataclass(frozen=True)
class Pivot:
    pivot_index: int
    available_index: int
    kind: str
    price: float
    atr: float
    half_width: float


@dataclass(frozen=True)
class Zone:
    center: float
    half_width: float
    score: float
    anchor_count: int
    first_pivot_index: int
    last_pivot_index: int
    formed_index: int
    last_available_index: int
    anchors: tuple[Pivot, ...]

    @property
    def lower(self) -> float:
        return self.center - self.half_width

    @property
    def upper(self) -> float:
        return self.center + self.half_width


def find_pivots(frame: pd.DataFrame) -> list[Pivot]:
    highs = frame["high"].to_numpy(float)
    lows = frame["low"].to_numpy(float)
    atr = frame["atr"].to_numpy(float)
    pivots: list[Pivot] = []
    for index in range(PIVOT_LEFT, len(frame) - PIVOT_RIGHT):
        if not math.isfinite(atr[index]) or atr[index] <= 0:
            continue
        high_window = highs[index - PIVOT_LEFT : index + PIVOT_RIGHT + 1]
        low_window = lows[index - PIVOT_LEFT : index + PIVOT_RIGHT + 1]
        if highs[index] == high_window.max() and np.count_nonzero(high_window == highs[index]) == 1:
            price = highs[index]
            pivots.append(
                Pivot(
                    index,
                    index + PIVOT_RIGHT,
                    "HIGH",
                    price,
                    atr[index],
                    max(ZONE_ATR_HALF_WIDTH * atr[index], ZONE_PRICE_HALF_WIDTH * price),
                )
            )
        if lows[index] == low_window.min() and np.count_nonzero(low_window == lows[index]) == 1:
            price = lows[index]
            pivots.append(
                Pivot(
                    index,
                    index + PIVOT_RIGHT,
                    "LOW",
                    price,
                    atr[index],
                    max(ZONE_ATR_HALF_WIDTH * atr[index], ZONE_PRICE_HALF_WIDTH * price),
                )
            )
    return sorted(pivots, key=lambda item: (item.available_index, item.pivot_index, item.kind))


def _pivot_weight(known_through: int, pivot: Pivot) -> float:
    age = max(0, known_through - pivot.pivot_index)
    return math.exp(-math.log(2) * age / HALF_LIFE_BARS)


def form_zones(
    pivots: Sequence[Pivot],
    *,
    known_through: int,
    current_atr: float,
) -> list[Zone]:
    eligible = [
        pivot
        for pivot in pivots
        if pivot.available_index <= known_through
        and pivot.pivot_index >= known_through - LOOKBACK_BARS
    ]
    clusters: list[dict[str, Any]] = []
    for pivot in sorted(eligible, key=lambda item: (item.price, item.pivot_index)):
        weight = _pivot_weight(known_through, pivot)
        best_index: int | None = None
        best_distance = math.inf
        for cluster_index, cluster in enumerate(clusters):
            distance = abs(pivot.price - float(cluster["center"]))
            merge_distance = max(
                float(cluster["half_width"]),
                pivot.half_width,
                MERGE_ATR_MULTIPLE * current_atr,
            )
            if distance <= merge_distance and distance < best_distance:
                best_index = cluster_index
                best_distance = distance
        if best_index is None:
            clusters.append(
                {
                    "weighted_price": weight * pivot.price,
                    "weighted_width": weight * pivot.half_width,
                    "weight": weight,
                    "center": pivot.price,
                    "half_width": pivot.half_width,
                    "pivots": [pivot],
                }
            )
            continue
        cluster = clusters[best_index]
        cluster["weighted_price"] += weight * pivot.price
        cluster["weighted_width"] += weight * pivot.half_width
        cluster["weight"] += weight
        cluster["center"] = cluster["weighted_price"] / cluster["weight"]
        cluster["half_width"] = cluster["weighted_width"] / cluster["weight"]
        cluster["pivots"].append(pivot)

    zones: list[Zone] = []
    for cluster in clusters:
        anchors: list[Pivot] = cluster["pivots"]
        indices = sorted(anchor.pivot_index for anchor in anchors)
        if len(indices) < 2 or indices[-1] - indices[0] < MIN_ANCHOR_SEPARATION:
            continue
        score = float(cluster["weight"])
        if score < MIN_ZONE_SCORE:
            continue
        formed_index = min(
            max(left.available_index, right.available_index)
            for left_index, left in enumerate(anchors)
            for right in anchors[left_index + 1 :]
            if abs(left.pivot_index - right.pivot_index) >= MIN_ANCHOR_SEPARATION
        )
        zones.append(
            Zone(
                center=float(cluster["center"]),
                half_width=float(cluster["half_width"]),
                score=score,
                anchor_count=len(anchors),
                first_pivot_index=indices[0],
                last_pivot_index=indices[-1],
                formed_index=formed_index,
                last_available_index=max(anchor.available_index for anchor in anchors),
                anchors=tuple(
                    sorted(
                        anchors,
                        key=lambda item: (
                            item.available_index,
                            item.pivot_index,
                            item.kind,
                            item.price,
                        ),
                    )
                ),
            )
        )
    return sorted(zones, key=lambda item: item.center)


def _zone_level_version(frame: pd.DataFrame, zone: Zone) -> str:
    anchors = [
        {
            "pivot_open_time_ms": int(frame.loc[anchor.pivot_index, "open_time_ms"]),
            "available_close_time_ms": int(
                frame.loc[anchor.available_index, "close_time_ms"]
            ),
            "kind": anchor.kind,
            "price": _float_text(anchor.price),
        }
        for anchor in zone.anchors
    ]
    canonical = json.dumps(
        anchors,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def nearest_role_zones(
    zones: Sequence[Zone], reference_price: float
) -> tuple[Zone | None, Zone | None]:
    supports = [zone for zone in zones if zone.upper < reference_price]
    resistances = [zone for zone in zones if zone.lower > reference_price]
    return (
        max(supports, key=lambda zone: zone.center, default=None),
        min(resistances, key=lambda zone: zone.center, default=None),
    )


def _same_recent_zone(
    recent: Sequence[tuple[int, float, float]], index: int, zone: Zone
) -> bool:
    for prior_index, prior_center, prior_width in reversed(recent):
        if index - prior_index >= EVENT_COOLDOWN_BARS:
            break
        if abs(zone.center - prior_center) <= max(zone.half_width, prior_width):
            return True
    return False


def classify_event_outcome(
    future_closes: Sequence[float],
    *,
    kind: str,
    touch_close: float,
    atr: float,
    zone_lower: float,
    zone_upper: float,
) -> tuple[str, int | None]:
    if kind == "SUPPORT":
        reaction_threshold = touch_close + REACTION_ATR_MULTIPLE * atr
        break_threshold = zone_lower - BREAK_ATR_MULTIPLE * atr
        for offset, close in enumerate(future_closes, start=1):
            if close >= reaction_threshold:
                return "REACTION", offset
            if close < break_threshold:
                return "BREAK", offset
    elif kind == "RESISTANCE":
        reaction_threshold = touch_close - REACTION_ATR_MULTIPLE * atr
        break_threshold = zone_upper + BREAK_ATR_MULTIPLE * atr
        for offset, close in enumerate(future_closes, start=1):
            if close <= reaction_threshold:
                return "REACTION", offset
            if close > break_threshold:
                return "BREAK", offset
    else:
        raise ValueError("BTC_INTELLIGENCE_EVENT_KIND_INVALID")
    return "UNRESOLVED", None


def scan_structure_events(
    frame: pd.DataFrame,
    pivots: Sequence[Pivot],
) -> list[dict[str, Any]]:
    highs = frame["high"].to_numpy(float)
    lows = frame["low"].to_numpy(float)
    closes = frame["close"].to_numpy(float)
    atr_values = frame["atr"].to_numpy(float)
    recent: dict[str, list[tuple[int, float, float]]] = {
        "SUPPORT": [],
        "RESISTANCE": [],
    }
    events: list[dict[str, Any]] = []
    for index in range(1, len(frame)):
        known_through = index - 1
        current_atr = atr_values[known_through]
        if not math.isfinite(current_atr) or current_atr <= 0:
            continue
        zones = form_zones(
            pivots,
            known_through=known_through,
            current_atr=current_atr,
        )
        support, resistance = nearest_role_zones(zones, closes[known_through])
        for kind, zone in (("SUPPORT", support), ("RESISTANCE", resistance)):
            if zone is None:
                continue
            if highs[index] < zone.lower or lows[index] > zone.upper:
                continue
            if _same_recent_zone(recent[kind], index, zone):
                continue
            event_atr = atr_values[index]
            if not math.isfinite(event_atr) or event_atr <= 0:
                continue
            result = "PENDING"
            outcome_offset: int | None = None
            if index + OUTCOME_HORIZON_BARS < len(frame):
                result, outcome_offset = classify_event_outcome(
                    closes[index + 1 : index + OUTCOME_HORIZON_BARS + 1],
                    kind=kind,
                    touch_close=closes[index],
                    atr=event_atr,
                    zone_lower=zone.lower,
                    zone_upper=zone.upper,
                )
            events.append(
                {
                    "index": index,
                    "open_time_ms": int(frame.loc[index, "open_time_ms"]),
                    "close_time_ms": int(frame.loc[index, "close_time_ms"]),
                    "kind": kind,
                    "zone_lower": zone.lower,
                    "zone_upper": zone.upper,
                    "zone_center": zone.center,
                    "strength": zone.score,
                    "anchor_count": zone.anchor_count,
                    "formed_close_time_ms": int(frame.loc[zone.formed_index, "close_time_ms"]),
                    "level_effective_close_time_ms": int(
                        frame.loc[zone.last_available_index, "close_time_ms"]
                    ),
                    "level_version": _zone_level_version(frame, zone),
                    "zone_age_bars": index - zone.formed_index,
                    "touch_close": closes[index],
                    "atr": event_atr,
                    "result": result,
                    "outcome_offset": outcome_offset,
                }
            )
            recent[kind].append((index, zone.center, zone.half_width))
            recent[kind] = [
                item for item in recent[kind] if index - item[0] < EVENT_COOLDOWN_BARS
            ]
    return events


def _finite_number(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if math.isfinite(numeric) else None


def _event_feature_snapshot(
    frame: pd.DataFrame,
    event: dict[str, Any],
) -> dict[str, Any]:
    index = int(event["index"])
    bar = frame.iloc[index]
    prior = frame.iloc[index - 1]
    kind = str(event["kind"])
    role_sign = 1.0 if kind == "SUPPORT" else -1.0
    atr = float(event["atr"])
    close = float(bar["close"])
    atr_pct = atr / close
    high = float(bar["high"])
    low = float(bar["low"])
    open_price = float(bar["open"])
    candle_range = max(high - low, 1e-12)
    close_location = (close - low) / candle_range
    role_close_location = (
        close_location if kind == "SUPPORT" else 1.0 - close_location
    )
    rejection_wick = (
        min(open_price, close) - low
        if kind == "SUPPORT"
        else high - max(open_price, close)
    )
    zone_lower = float(event["zone_lower"])
    zone_upper = float(event["zone_upper"])
    zone_center = float(event["zone_center"])
    prior_distance = (
        float(prior["close"]) - zone_upper
        if kind == "SUPPORT"
        else zone_lower - float(prior["close"])
    )
    penetration = (
        zone_upper - low if kind == "SUPPORT" else high - zone_lower
    )
    trend_state = str(bar["trend_state"])
    trend_alignment = 0.0
    if trend_state == "UP":
        trend_alignment = role_sign
    elif trend_state == "DOWN":
        trend_alignment = -role_sign

    closes = frame["close"].to_numpy(float)

    def momentum(lag: int) -> float | None:
        if index < lag or atr_pct <= 0:
            return None
        return _finite_number(
            role_sign * math.log(closes[index] / closes[index - lag]) / atr_pct
        )

    rv30 = _finite_number(bar["rv30"])
    rv6 = _finite_number(bar["rv6"])
    volatility_regime = (
        "LOW"
        if atr_pct < 0.01
        else "NORMAL"
        if atr_pct < 0.02
        else "HIGH"
    )
    open_time = datetime.fromtimestamp(
        float(bar["open_time_ms"]) / 1000,
        tz=UTC,
    )
    return {
        "schema_version": EVENT_FEATURE_SCHEMA_VERSION,
        "kind": kind,
        "trend_state": trend_state,
        "utc_slot": open_time.hour,
        "volatility_regime": volatility_regime,
        "log_strength": _finite_number(math.log1p(float(event["strength"]))),
        "anchor_count": int(event["anchor_count"]),
        "zone_width_atr": _finite_number((zone_upper - zone_lower) / atr),
        "zone_age_bars": int(event["zone_age_bars"]),
        "prior_distance_atr": _finite_number(prior_distance / atr),
        "penetration_atr": _finite_number(max(0.0, penetration / atr)),
        "range_atr": _finite_number(candle_range / atr),
        "body_atr": _finite_number(abs(close - open_price) / atr),
        "signed_body_atr": _finite_number(
            role_sign * (close - open_price) / atr
        ),
        "role_close_location": _finite_number(role_close_location),
        "rejection_wick_atr": _finite_number(max(0.0, rejection_wick / atr)),
        "close_center_signed_atr": _finite_number(
            role_sign * (close - zone_center) / atr
        ),
        "momentum_1": momentum(1),
        "momentum_3": momentum(3),
        "momentum_6": momentum(6),
        "momentum_12": momentum(12),
        "trend_gap": _finite_number(bar["trend_gap"]),
        "trend_slope": _finite_number(bar["trend_slope"]),
        "adx": _finite_number(bar["adx"]),
        "close_ema20_signed_atr": _finite_number(
            role_sign * (close - float(bar["ema_fast"])) / atr
        ),
        "close_ema50_signed_atr": _finite_number(
            role_sign * (close - float(bar["ema_slow"])) / atr
        ),
        "htf_gap": _finite_number(bar["htf_gap"]),
        "htf_slope": _finite_number(bar["htf_slope"]),
        "htf_alignment": _finite_number(role_sign * float(bar["htf_gap"])),
        "rv6": rv6,
        "rv30": rv30,
        "rv_ratio": _finite_number(rv6 / rv30)
        if rv6 is not None and rv30 is not None and rv30 > 0
        else None,
        "atr_pct": _finite_number(atr_pct),
        "volume_z30": _finite_number(bar["volume_z30"]),
        "quote_volume_z30": _finite_number(bar["quote_volume_z30"]),
        "trade_count_z30": _finite_number(bar["trade_count_z30"]),
        "taker_buy_share_delta": _finite_number(
            float(bar["taker_buy_share"]) - 0.5
        ),
        "trend_alignment": trend_alignment,
    }


def _daily_atr20(frame: pd.DataFrame) -> float | None:
    previous = frame["close"].shift(1).fillna(frame["close"])
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous).abs(),
            (frame["low"] - previous).abs(),
        ],
        axis=1,
    ).max(axis=1)
    value = true_range.rolling(20, min_periods=20).mean().iloc[-1]
    return float(value) if math.isfinite(float(value)) and float(value) > 0 else None


def monthly_state(
    daily: pd.DataFrame,
    *,
    current_price: float,
    now: datetime,
) -> dict[str, Any]:
    close_times = pd.to_datetime(daily["close_time_ms"], unit="ms", utc=True)
    periods = close_times.dt.tz_localize(None).dt.to_period("M")
    monthly = pd.Series(daily["close"].to_numpy(float), index=periods).groupby(level=0).last()
    monthly_close_times = pd.Series(
        daily["close_time_ms"].to_numpy(np.int64),
        index=periods,
    ).groupby(level=0).last()
    current_period = pd.Period(now.astimezone(UTC).strftime("%Y-%m"), freq="M")
    monthly = monthly[monthly.index < current_period]
    monthly_close_times = monthly_close_times[
        monthly_close_times.index < current_period
    ]
    if len(monthly) < 10:
        raise ValueError("BTC_INTELLIGENCE_MONTHLY_HISTORY_INSUFFICIENT")
    recent_periods = monthly.index[-10:]
    if any(
        recent_periods[index].ordinal - recent_periods[index - 1].ordinal != 1
        for index in range(1, len(recent_periods))
    ):
        raise ValueError("BTC_INTELLIGENCE_MONTHLY_HISTORY_NON_CONTIGUOUS")
    latest_close = float(monthly.iloc[-1])
    prior_nine_mean = float(monthly.iloc[-10:-1].mean())
    official_target = 1 if latest_close > prior_nine_mean else 0
    official_sma10 = float(monthly.iloc[-10:].mean())
    current_boundary = float(monthly.iloc[-9:].mean())
    previous_target: int | None = None
    if len(monthly) >= 11:
        previous_close = float(monthly.iloc[-2])
        previous_boundary = float(monthly.iloc[-11:-2].mean())
        previous_target = 1 if previous_close > previous_boundary else 0
    atr20 = _daily_atr20(daily)
    next_month = (current_period + 1).start_time.tz_localize(UTC).to_pydatetime()
    current_month = current_period.start_time.tz_localize(UTC).to_pydatetime()
    formed_at = datetime.fromtimestamp(
        float(monthly_close_times.iloc[-1]) / 1000,
        tz=UTC,
    )
    distance = current_price / current_boundary - 1
    return {
        "official_target": official_target,
        "official_target_label": "BTC Spot 多头研究目标" if official_target else "现金研究目标",
        "previous_target": previous_target,
        "target_changed": previous_target is not None and previous_target != official_target,
        "formed_month": str(monthly.index[-1]),
        "formed_at": iso_utc(formed_at),
        "formed_month_close": _float_text(latest_close),
        "formed_target_boundary": _float_text(prior_nine_mean),
        "formed_month_sma10": _float_text(official_sma10),
        "current_month": str(current_period),
        "current_boundary": _float_text(current_boundary),
        "distance_percent": _float_text(distance * 100),
        "distance_atr": _float_text(
            (current_price - current_boundary) / atr20 if atr20 is not None else None
        ),
        "daily_atr20": _float_text(atr20),
        "provisional_side": "ABOVE" if current_price > current_boundary else "BELOW_OR_EQUAL",
        "provisional_label": "月末若保持在边界上方，下月目标为 BTC" if current_price > current_boundary else "月末若未回到边界上方，下月目标为现金",
        "next_confirmation_at": iso_utc(next_month),
        "research_execution_proxy_at": iso_utc(
            current_month + timedelta(minutes=1)
        ),
        "next_research_execution_proxy_at": iso_utc(
            next_month + timedelta(minutes=1)
        ),
        "evidence_level": "MEDIUM",
        "authority": "RESEARCH_TARGET_ONLY",
        "source": "BINANCE_SPOT_PUBLIC_1D_CLOSED",
    }


def daily_donchian_state(daily: pd.DataFrame) -> dict[str, Any]:
    closes = daily["close"].to_numpy(float)
    close_times = daily["close_time_ms"].to_numpy(np.int64)
    components: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    for lookback in DONCHIAN_WINDOWS:
        series = pd.Series(closes)
        upper = series.rolling(lookback, min_periods=lookback).max().to_numpy(float)
        lower = series.rolling(lookback, min_periods=lookback).min().to_numpy(float)
        midpoint = (upper + lower) / 2.0
        active = False
        trailing = math.nan
        last_transition: dict[str, Any] | None = None
        for index, price in enumerate(closes):
            if not math.isfinite(upper[index]):
                continue
            old_active = active
            trigger = None
            if active:
                trailing = max(trailing, float(midpoint[index]))
                if price <= trailing:
                    active = False
                    trigger = trailing
                    trailing = math.nan
            elif price >= float(upper[index]):
                active = True
                trailing = float(midpoint[index])
                trigger = float(upper[index])
            if active != old_active:
                last_transition = {
                    "window": lookback,
                    "from": "ACTIVE" if old_active else "INACTIVE",
                    "to": "ACTIVE" if active else "INACTIVE",
                    "trigger_boundary": _float_text(trigger),
                    "at": iso_utc(datetime.fromtimestamp(close_times[index] / 1000, tz=UTC)),
                }
        boundary = trailing if active else float(upper[-1])
        component = {
            "window": lookback,
            "active": active,
            "state": "ACTIVE" if active else "INACTIVE",
            "boundary_role": "DYNAMIC_SUPPORT" if active else "BREAKOUT_RESISTANCE",
            "boundary": _float_text(boundary),
            "upper": _float_text(float(upper[-1])),
            "lower": _float_text(float(lower[-1])),
            "midpoint": _float_text(float(midpoint[-1])),
            "distance_percent": _float_text((closes[-1] / boundary - 1) * 100),
        }
        components.append(component)
        if last_transition is not None:
            transitions.append(last_transition)
    agreement = sum(bool(component["active"]) for component in components) / len(components)
    state = "STRONG_UP" if agreement >= 0.75 else "WEAK_OR_DEFENSIVE" if agreement <= 0.25 else "TRANSITION"
    latest_transition = max(transitions, key=lambda item: str(item["at"]), default=None)
    return {
        "agreement": _float_text(agreement),
        "agreement_percent": _float_text(agreement * 100),
        "state": state,
        "state_label": {
            "STRONG_UP": "日频强上行结构",
            "WEAK_OR_DEFENSIVE": "日频弱势或防御结构",
            "TRANSITION": "日频过渡结构",
        }[state],
        "components": components,
        "latest_transition": latest_transition,
        "authority": "STATE_ONLY",
        "source": "BINANCE_SPOT_PUBLIC_1D_CLOSED",
    }


def unified_regime(official_target: int, agreement: float) -> dict[str, str]:
    if official_target == 1 and agreement >= 0.75:
        code = "ALIGNED_UP"
        label = "慢趋势与日频通道一致"
    elif official_target == 1 and agreement <= 0.25:
        code = "SLOW_UP_TACTICAL_PULLBACK"
        label = "月频多头中的日频回撤风险"
    elif official_target == 0 and agreement >= 0.75:
        code = "COUNTERTREND_RALLY"
        label = "现金状态下的日频反弹"
    elif official_target == 0 and agreement <= 0.25:
        code = "DEFENSIVE"
        label = "慢趋势与日频弱势一致"
    else:
        code = "TRANSITION"
        label = "月频与日频组件分歧"
    return {"code": code, "label": label}


def _zone_payload(
    frame: pd.DataFrame,
    zone: Zone,
    *,
    role: str,
    current_price: float,
    current_atr: float,
    current_index: int,
) -> dict[str, Any]:
    formed_at = datetime.fromtimestamp(
        float(frame.loc[zone.formed_index, "close_time_ms"]) / 1000,
        tz=UTC,
    )
    version_effective_at = datetime.fromtimestamp(
        float(frame.loc[zone.last_available_index, "close_time_ms"]) / 1000,
        tz=UTC,
    )
    return {
        "role": role,
        "lifecycle": "TESTING" if zone.lower <= current_price <= zone.upper else "ACTIVE",
        "lower": _float_text(zone.lower),
        "upper": _float_text(zone.upper),
        "center": _float_text(zone.center),
        "distance_atr": _float_text((zone.center - current_price) / current_atr),
        "distance_percent": _float_text((zone.center / current_price - 1) * 100),
        "strength": _float_text(zone.score),
        "anchor_count": zone.anchor_count,
        "age_bars": current_index - zone.formed_index,
        "version_age_bars": current_index - zone.last_available_index,
        "level_version": _zone_level_version(frame, zone),
        "formed_at": iso_utc(formed_at),
        "version_effective_at": iso_utc(version_effective_at),
        "effective_at": iso_utc(version_effective_at),
        "authority": "RESEARCH_ONLY",
    }


def current_structure_payload(
    frame: pd.DataFrame,
    pivots: Sequence[Pivot],
    *,
    current_price: float,
) -> dict[str, Any]:
    index = len(frame) - 1
    bar = frame.iloc[index]
    current_atr = float(bar["atr"])
    zones = form_zones(pivots, known_through=index, current_atr=current_atr)
    testing = [zone for zone in zones if zone.lower <= current_price <= zone.upper]
    support, resistance = nearest_role_zones(zones, current_price)
    selected: list[dict[str, Any]] = []
    if testing:
        zone = max(testing, key=lambda item: item.score)
        selected.append(
            _zone_payload(
                frame,
                zone,
                role="TESTING",
                current_price=current_price,
                current_atr=current_atr,
                current_index=index,
            )
        )
    if support is not None:
        selected.append(
            _zone_payload(
                frame,
                support,
                role="SUPPORT",
                current_price=current_price,
                current_atr=current_atr,
                current_index=index,
            )
        )
    if resistance is not None:
        selected.append(
            _zone_payload(
                frame,
                resistance,
                role="RESISTANCE",
                current_price=current_price,
                current_atr=current_atr,
                current_index=index,
            )
        )
    return {
        "source_cutoff_at": iso_utc(
            datetime.fromtimestamp(float(bar["close_time_ms"]) / 1000, tz=UTC)
        ),
        "atr14": _float_text(current_atr),
        "environment": str(bar["trend_state"]),
        "environment_label": {
            "UP": "4h 上行环境",
            "DOWN": "4h 下行环境",
            "RANGE": "4h 区间环境",
        }[str(bar["trend_state"])],
        "trend_gap": _float_text(float(bar["trend_gap"])),
        "trend_slope": _float_text(float(bar["trend_slope"])),
        "adx14": _float_text(float(bar["adx"])),
        "zones": selected,
        "confirmed_zone_count": len(zones),
        "model_status": "NOT_DEPLOYED",
        "model_label": "概率模型尚未部署",
        "p_reaction": None,
        "p_break": None,
        "p_unresolved": None,
        "research_priority": None,
        "authority": "RESEARCH_ONLY",
        "source": "BINANCE_SPOT_PUBLIC_4H_CLOSED",
    }


@dataclass(frozen=True)
class BinanceBtcIntelligenceSettings:
    cache_root: Path
    interval_seconds: float = 600
    jitter_seconds: float = 30
    timeout_seconds: float = 10
    proxy_url: str | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.interval_seconds) or self.interval_seconds < 60:
            raise ValueError("BTC_INTELLIGENCE_INTERVAL_TOO_SHORT")
        if not 0 <= self.jitter_seconds <= 30:
            raise ValueError("BTC_INTELLIGENCE_JITTER_INVALID")
        if not math.isfinite(self.timeout_seconds) or not 0 < self.timeout_seconds <= 60:
            raise ValueError("BTC_INTELLIGENCE_TIMEOUT_INVALID")


class BinanceBtcIntelligenceMonitor:
    monitor_id = "btc-market-intelligence"
    display_name = "BTC 专业情报"
    description = (
        "月度收益核心、日频趋势状态、4 小时因果支撑阻力与 BTC 聪明钱背景；"
        "各层证据权限独立，不输出买卖、仓位、杠杆或跟单指令。"
    )
    projection_kind = "btc_intelligence"
    foreground_interval_seconds = 60.0

    def __init__(
        self,
        settings: BinanceBtcIntelligenceSettings,
        *,
        store: SQLiteMonitorStore,
        spot_client: SpotMarketProvider | None = None,
        smart_money_monitor: BinanceSmartMoneyMonitor | None = None,
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.jitter_seconds = settings.jitter_seconds
        self.store = store
        self.spot_client = spot_client or BinanceSpotMarketClient(
            settings.cache_root,
            timeout_seconds=settings.timeout_seconds,
            proxy_url=settings.proxy_url,
        )
        self.smart_money_monitor = smart_money_monitor or BinanceSmartMoneyMonitor(
            BinanceSmartMoneySettings(
                interval_seconds=settings.interval_seconds,
                jitter_seconds=settings.jitter_seconds,
                symbols=(SYMBOL,),
                timeout_seconds=settings.timeout_seconds,
                proxy_url=settings.proxy_url,
            )
        )
        self._now = now
        self.view = MonitorView(
            filters=(),
            columns=(ViewColumn("symbol", "市场"),),
            chart_title="BTC 专业情报",
            table_title="BTC 专业情报",
            method_note=(
                "月频状态只有中等证据；日频和 4h 只描述状态；聪明钱结论为证据不足。"
            ),
        )

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        for target in (self.spot_client, self.smart_money_monitor):
            binder = getattr(target, "bind_stop_event", None)
            if callable(binder):
                binder(stop_event)

    def network_request_count(self, *, window_seconds: float = 60) -> int | None:
        total = 0
        observed = False
        for target in (self.spot_client, self.smart_money_monitor):
            counter = getattr(target, "network_request_count", None)
            if not callable(counter):
                continue
            value = counter(window_seconds=window_seconds)
            if value is not None:
                total += int(value)
                observed = True
        return total if observed else None

    def collect(self) -> CollectionBatch:
        observed_at = self._now().astimezone(UTC)
        issues: list[CollectionIssue] = []
        artifacts: list[CollectionArtifact] = []

        try:
            smart_batch = self.smart_money_monitor.collect()
        except CollectionCancelled:
            raise
        except Exception:
            smart_batch = CollectionBatch(
                samples=(),
                issues=(
                    CollectionIssue(
                        "BTCUSDT:smart-money",
                        "SMART_MONEY_COLLECTION_FAILED",
                    ),
                ),
            )
        artifacts.extend(smart_batch.artifacts)
        smart_rows = [
            sample.payload
            for sample in smart_batch.samples
            if sample.entity_key == SYMBOL
        ]
        ignored_context_issues: list[str] = []
        for issue in smart_batch.issues:
            if (
                issue.reason_code == "SMART_MONEY_OVERVIEW_STALE"
                and len(smart_rows) >= 2
            ):
                ignored_context_issues.append(issue.reason_code)
                continue
            issues.append(issue)
        smart_payload = {
            "status": "AVAILABLE" if len(smart_rows) >= 2 else "PARTIAL" if smart_rows else "UNAVAILABLE",
            "rows": smart_rows,
            "research_conclusion": "INSUFFICIENT_EVIDENCE",
            "authority": "CONTEXT_ONLY",
            "source_kind": "BINANCE_USDM_WEB_INTERNAL",
            "unused_context_issues": sorted(set(ignored_context_issues)),
        }

        daily_cutoff = _latest_closed_cutoff(observed_at, DAY_MS)
        four_hour_cutoff = _latest_closed_cutoff(observed_at, FOUR_HOUR_MS)
        def fetch_or_failed(
            interval: str,
            cutoff: datetime,
            history_bars: int,
        ) -> SpotSeriesResult:
            try:
                return self.spot_client.fetch_bars(
                    interval=interval,
                    cutoff=cutoff,
                    history_bars=history_bars,
                )
            except CollectionCancelled:
                raise
            except SpotMarketError as exc:
                reason_code = exc.reason_code
            except Exception:
                reason_code = "BTC_INTELLIGENCE_SERIES_COLLECTION_FAILED"
            return SpotSeriesResult(
                interval=interval,
                status="FAILED",
                frame=pd.DataFrame(),
                cutoff_at=cutoff,
                acquired_at=None,
                reason_code=reason_code,
            )

        daily = fetch_or_failed("1d", daily_cutoff, DAILY_HISTORY_BARS)
        four_hour = fetch_or_failed("4h", four_hour_cutoff, FOUR_HOUR_HISTORY_BARS)
        for scope, result in (("BTCUSDT:1d", daily), ("BTCUSDT:4h", four_hour)):
            if result.reason_code is not None:
                issues.append(CollectionIssue(scope, result.reason_code))

        price: float | None = None
        price_at: datetime | None = None
        price_state = "UNAVAILABLE"
        try:
            price, price_at = self.spot_client.fetch_price()
            price_state = "LIVE_SPOT_REFERENCE"
        except SpotMarketError as exc:
            issues.append(CollectionIssue("BTCUSDT:spot-price", exc.reason_code))
        except Exception:
            issues.append(CollectionIssue("BTCUSDT:spot-price", "BTC_INTELLIGENCE_TICKER_FAILED"))
        if price is None and four_hour.current and not four_hour.frame.empty:
            price = float(four_hour.frame["close"].iloc[-1])
            price_at = datetime.fromtimestamp(
                float(four_hour.frame["close_time_ms"].iloc[-1]) / 1000,
                tz=UTC,
            )
            price_state = "CLOSED_4H_REFERENCE"

        monthly: dict[str, Any] | None = None
        daily_payload: dict[str, Any] | None = None
        structure: dict[str, Any] | None = None
        history_observation: BtcStructureHistoryObservation | None = None
        event_revisions: list[BtcStructureEventRevision] = []
        monthly_history_observation: BtcMonthlyResearchHistoryObservation | None = None
        monthly_revisions: list[BtcMonthlyResearchRevision] = []

        if daily.current and price is not None:
            try:
                validate_closed_bars(daily.frame, interval_ms=DAY_MS, minimum_rows=120)
            except ValueError as exc:
                issues.append(CollectionIssue("BTCUSDT:1d", str(exc)))
            else:
                try:
                    monthly = monthly_state(
                        daily.frame,
                        current_price=price,
                        now=observed_at,
                    )
                except ValueError as exc:
                    issues.append(CollectionIssue("BTCUSDT:monthly", str(exc)))
                try:
                    daily_payload = daily_donchian_state(daily.frame)
                    daily_payload["source_cutoff_at"] = iso_utc(daily.cutoff_at)
                except ValueError as exc:
                    issues.append(CollectionIssue("BTCUSDT:daily-trend", str(exc)))

        regime = None
        if monthly is not None and daily_payload is not None:
            regime = unified_regime(
                int(monthly["official_target"]),
                float(daily_payload["agreement"]),
            )
        if monthly is not None:
            try:
                (
                    monthly_history_observation,
                    monthly_revisions,
                    monthly_issues,
                ) = self._monthly_updates(monthly, observed_at=observed_at)
                issues.extend(monthly_issues)
            except ValueError as exc:
                issues.append(CollectionIssue("BTCUSDT:monthly-ledger", str(exc)))

        if four_hour.current and price is not None:
            try:
                validate_closed_bars(
                    four_hour.frame,
                    interval_ms=FOUR_HOUR_MS,
                    minimum_rows=LOOKBACK_BARS + 60,
                )
                enriched = add_event_quality_features(
                    add_4h_indicators(four_hour.frame)
                )
                pivots = find_pivots(enriched)
                structure = current_structure_payload(enriched, pivots, current_price=price)
                history_observation, event_revisions, event_issues = self._event_updates(
                    enriched,
                    pivots,
                    observed_at=observed_at,
                    market_context={
                        "monthly_target": (
                            int(monthly["official_target"])
                            if monthly is not None
                            else None
                        ),
                        "monthly_formed_month": (
                            monthly.get("formed_month")
                            if monthly is not None
                            else None
                        ),
                        "daily_agreement": (
                            _finite_number(daily_payload.get("agreement"))
                            if daily_payload is not None
                            else None
                        ),
                        "daily_state": (
                            daily_payload.get("state")
                            if daily_payload is not None
                            else None
                        ),
                        "combined_regime": (
                            regime.get("code") if regime is not None else None
                        ),
                    },
                )
                issues.extend(event_issues)
            except ValueError as exc:
                issues.append(CollectionIssue("BTCUSDT:4h", str(exc)))

        snapshot = MetricSample(
            series_key=f"{SYMBOL}|intelligence",
            entity_key=SYMBOL,
            observed_at=observed_at,
            value_text=_float_text(price) or "",
            unit="USDT",
            payload={
                "row_type": "BTC_INTELLIGENCE",
                "symbol": SYMBOL,
                "observed_at": iso_utc(observed_at),
                "current_price": _float_text(price),
                "current_price_at": iso_utc(price_at) if price_at is not None else None,
                "current_price_state": price_state,
                "unified_regime": regime,
                "monthly": monthly,
                "daily": daily_payload,
                "structure": structure,
                "smart_money": smart_payload,
                "source_clocks": {
                    "daily_cutoff_at": iso_utc(daily.cutoff_at) if daily.current else None,
                    "four_hour_cutoff_at": iso_utc(four_hour.cutoff_at) if four_hour.current else None,
                    "daily_state": daily.status,
                    "four_hour_state": four_hour.status,
                },
                "evidence_registry": [
                    {
                        "layer": "MONTHLY_FABER_10M",
                        "evidence": "MEDIUM",
                        "authority": "RESEARCH_TARGET_ONLY",
                    },
                    {
                        "layer": "DAILY_DONCHIAN_20_30_60_90",
                        "evidence": "STATE_ONLY",
                        "authority": "NO_TRADE_AUTHORITY",
                    },
                    {
                        "layer": "4H_CAUSAL_ZONES",
                        "evidence": "RESEARCH_ONLY",
                        "authority": "NO_TRADE_AUTHORITY",
                    },
                    {
                        "layer": "BINANCE_SMART_MONEY",
                        "evidence": "INSUFFICIENT",
                        "authority": "CONTEXT_ONLY",
                    },
                ],
                "interpretation_limit": (
                    "状态与研究目标不是买卖、仓位、杠杆、止损、止盈或跟单指令。"
                ),
            },
        )
        return CollectionBatch(
            samples=(snapshot,),
            issues=tuple(issues),
            artifacts=tuple(artifacts),
            btc_structure_history=history_observation,
            btc_structure_event_revisions=tuple(event_revisions),
            btc_monthly_research_history=monthly_history_observation,
            btc_monthly_research_revisions=tuple(monthly_revisions),
        )

    def _monthly_updates(
        self,
        monthly: dict[str, Any],
        *,
        observed_at: datetime,
    ) -> tuple[
        BtcMonthlyResearchHistoryObservation,
        list[BtcMonthlyResearchRevision],
        list[CollectionIssue],
    ]:
        try:
            formed_at = parse_utc(str(monthly["formed_at"]))
            execution_eligible_at = parse_utc(
                str(monthly["research_execution_proxy_at"])
            )
        except (KeyError, TypeError, ValueError):
            raise ValueError("BTC_MONTHLY_LEDGER_STATE_INVALID") from None
        stored_history = self.store.btc_monthly_research_history(self.monitor_id)
        if (
            stored_history is None
            or stored_history.algorithm_version != MONTHLY_ALGORITHM_VERSION
        ):
            return (
                BtcMonthlyResearchHistoryObservation(
                    started_at=observed_at,
                    processed_through_at=formed_at,
                    algorithm_version=MONTHLY_ALGORITHM_VERSION,
                ),
                [],
                [],
            )

        revisions: list[BtcMonthlyResearchRevision] = []
        issues: list[CollectionIssue] = []
        pending = self.store.pending_btc_monthly_research_revisions(
            self.monitor_id,
            algorithm_version=MONTHLY_ALGORITHM_VERSION,
        )
        for stored in pending:
            settled, issue = self._settle_monthly_execution(
                stored,
                observed_at=observed_at,
            )
            if settled is not None:
                revisions.append(settled)
            if issue is not None:
                issues.append(issue)

        if formed_at > stored_history.processed_through_at:
            signal_key = (
                f"{MONTHLY_ALGORITHM_VERSION}:{str(monthly['formed_month'])}"
            )
            if observed_at >= execution_eligible_at:
                issues.append(
                    CollectionIssue(
                        signal_key,
                        "BTC_MONTHLY_SIGNAL_MISSED_BEFORE_EXECUTION",
                    )
                )
            else:
                signal = {
                    "signal_key": signal_key,
                    "formed_month": monthly["formed_month"],
                    "signal_at": iso_utc(formed_at),
                    "observed_at": iso_utc(observed_at),
                    "official_target": int(monthly["official_target"]),
                    "previous_target": monthly.get("previous_target"),
                    "target_changed": bool(monthly.get("target_changed")),
                    "formed_month_close": monthly["formed_month_close"],
                    "formed_target_boundary": monthly["formed_target_boundary"],
                    "execution_eligible_at": iso_utc(execution_eligible_at),
                    "base_cost_bps": MONTHLY_BASE_COST_BPS,
                    "stress_cost_bps": MONTHLY_STRESS_COST_BPS,
                    "rule": "FABER_10M_COMPLETE_UTC_MONTH_NEXT_MONTH_SPOT_OR_CASH",
                    "source": "BINANCE_SPOT_PUBLIC_1D_CLOSED",
                    "source_cutoff_at": iso_utc(formed_at),
                }
                revisions.append(
                    BtcMonthlyResearchRevision(
                        signal_key=signal_key,
                        signal_at=formed_at,
                        observed_at=observed_at,
                        state="SIGNAL_FROZEN",
                        payload={
                            "algorithm_version": MONTHLY_ALGORITHM_VERSION,
                            "signal": signal,
                            "execution": None,
                        },
                    )
                )
        return (
            BtcMonthlyResearchHistoryObservation(
                started_at=stored_history.started_at,
                processed_through_at=max(
                    formed_at,
                    stored_history.processed_through_at,
                ),
                algorithm_version=MONTHLY_ALGORITHM_VERSION,
            ),
            revisions,
            issues,
        )

    def _settle_monthly_execution(
        self,
        stored: StoredBtcMonthlyResearchRevision,
        *,
        observed_at: datetime,
    ) -> tuple[BtcMonthlyResearchRevision | None, CollectionIssue | None]:
        signal = stored.payload.get("signal")
        if not isinstance(signal, dict):
            raise ValueError("BTC_MONTHLY_PENDING_SIGNAL_INVALID")
        try:
            execution_eligible_at = parse_utc(str(signal["execution_eligible_at"]))
        except (KeyError, TypeError, ValueError):
            raise ValueError("BTC_MONTHLY_PENDING_SIGNAL_INVALID") from None
        if observed_at < execution_eligible_at + timedelta(minutes=1):
            return None, None
        try:
            price, execution_at = self.spot_client.fetch_execution_price(
                execution_eligible_at
            )
        except CollectionCancelled:
            raise
        except SpotMarketError as exc:
            return None, CollectionIssue(stored.signal_key, exc.reason_code)
        except Exception:
            return None, CollectionIssue(
                stored.signal_key,
                "BTC_MONTHLY_EXECUTION_PROXY_FAILED",
            )
        return (
            BtcMonthlyResearchRevision(
                signal_key=stored.signal_key,
                signal_at=stored.signal_at,
                observed_at=observed_at,
                state="EXECUTION_CAPTURED",
                payload={
                    "algorithm_version": MONTHLY_ALGORITHM_VERSION,
                    "signal": signal,
                    "execution": {
                        "execution_at": iso_utc(execution_at),
                        "observed_at": iso_utc(observed_at),
                        "price": _float_text(price),
                        "price_field": "OPEN",
                        "interval": "1m",
                        "source": "BINANCE_SPOT_PUBLIC_1M_CLOSED",
                        "base_cost_bps": MONTHLY_BASE_COST_BPS,
                        "stress_cost_bps": MONTHLY_STRESS_COST_BPS,
                    },
                },
            ),
            None,
        )

    def _event_updates(
        self,
        frame: pd.DataFrame,
        pivots: Sequence[Pivot],
        *,
        observed_at: datetime,
        market_context: dict[str, Any],
    ) -> tuple[
        BtcStructureHistoryObservation,
        list[BtcStructureEventRevision],
        list[CollectionIssue],
    ]:
        processed_through = datetime.fromtimestamp(
            float(frame["close_time_ms"].iloc[-1]) / 1000,
            tz=UTC,
        )
        stored_history = self.store.btc_structure_history(self.monitor_id)
        if stored_history is None:
            return (
                BtcStructureHistoryObservation(
                    started_at=observed_at,
                    processed_through_at=processed_through,
                    algorithm_version=ALGORITHM_VERSION,
                ),
                [],
                [],
            )
        if stored_history.algorithm_version != ALGORITHM_VERSION:
            return (
                BtcStructureHistoryObservation(
                    started_at=observed_at,
                    processed_through_at=processed_through,
                    algorithm_version=ALGORITHM_VERSION,
                ),
                [],
                [],
            )

        revisions: list[BtcStructureEventRevision] = []
        issues: list[CollectionIssue] = []
        pending = self.store.pending_btc_structure_event_revisions(
            self.monitor_id,
            algorithm_version=ALGORITHM_VERSION,
        )
        open_time_to_index = {
            int(value): index for index, value in enumerate(frame["open_time_ms"].to_numpy(np.int64))
        }
        closes = frame["close"].to_numpy(float)
        for stored in pending:
            settled = self._settle_pending_event(
                stored,
                frame=frame,
                closes=closes,
                open_time_to_index=open_time_to_index,
                observed_at=observed_at,
            )
            if settled is not None:
                revisions.append(settled)

        known_pending_keys = {item.event_key for item in pending}
        events = scan_structure_events(frame, pivots)
        latest_close_time_ms = int(frame["close_time_ms"].iloc[-1])
        for event in events:
            event_at = datetime.fromtimestamp(event["close_time_ms"] / 1000, tz=UTC)
            if event_at <= stored_history.processed_through_at:
                continue
            event_key = (
                f"{ALGORITHM_VERSION}:{event['open_time_ms']}:{event['kind']}:"
                f"{str(event['level_version'])[:16]}"
            )
            if event_key in known_pending_keys:
                continue
            if int(event["close_time_ms"]) != latest_close_time_ms:
                issues.append(
                    CollectionIssue(event_key, "BTC_STRUCTURE_EVENT_MISSED_DURING_DOWNTIME")
                )
                continue
            features = _event_feature_snapshot(frame, event)
            signal = {
                "event_key": event_key,
                "kind": event["kind"],
                "event_at": iso_utc(event_at),
                "open_time_ms": event["open_time_ms"],
                "zone_lower": _float_text(float(event["zone_lower"])),
                "zone_upper": _float_text(float(event["zone_upper"])),
                "zone_center": _float_text(float(event["zone_center"])),
                "zone_width": _float_text(
                    float(event["zone_upper"]) - float(event["zone_lower"])
                ),
                "strength": _float_text(float(event["strength"])),
                "anchor_count": int(event["anchor_count"]),
                "level_version": event["level_version"],
                "zone_formed_at": iso_utc(
                    datetime.fromtimestamp(event["formed_close_time_ms"] / 1000, tz=UTC)
                ),
                "level_effective_at": iso_utc(
                    datetime.fromtimestamp(
                        event["level_effective_close_time_ms"] / 1000,
                        tz=UTC,
                    )
                ),
                "touch_close": _float_text(float(event["touch_close"])),
                "atr_touch": _float_text(float(event["atr"])),
                "due_at": iso_utc(event_at + timedelta(hours=4 * OUTCOME_HORIZON_BARS)),
                "source": "BINANCE_SPOT_PUBLIC_4H_CLOSED",
                "source_cutoff_at": iso_utc(event_at),
                "feature_schema_version": EVENT_FEATURE_SCHEMA_VERSION,
                "features": features,
                "market_context": dict(market_context),
                "cost_assumptions_bps": {
                    "base": EVENT_BASE_COST_BPS,
                    "stress": EVENT_STRESS_COST_BPS,
                },
                "model_status": "NOT_DEPLOYED",
                "model_version": None,
                "p_reaction": None,
                "p_break": None,
                "p_unresolved": None,
                "research_priority": None,
            }
            revisions.append(
                BtcStructureEventRevision(
                    event_key=event_key,
                    event_at=event_at,
                    observed_at=observed_at,
                    state="PENDING",
                    payload={
                        "algorithm_version": ALGORITHM_VERSION,
                        "signal": signal,
                        "outcome": None,
                    },
                )
            )
        return (
            BtcStructureHistoryObservation(
                started_at=stored_history.started_at,
                processed_through_at=max(processed_through, stored_history.processed_through_at),
                algorithm_version=ALGORITHM_VERSION,
            ),
            revisions,
            issues,
        )

    @staticmethod
    def _settle_pending_event(
        stored: StoredBtcStructureEventRevision,
        *,
        frame: pd.DataFrame,
        closes: np.ndarray,
        open_time_to_index: dict[int, int],
        observed_at: datetime,
    ) -> BtcStructureEventRevision | None:
        signal = stored.payload.get("signal")
        if not isinstance(signal, dict):
            raise ValueError("BTC_STRUCTURE_PENDING_SIGNAL_INVALID")
        try:
            index = open_time_to_index[int(signal["open_time_ms"])]
            kind = str(signal["kind"])
            touch_close = float(signal["touch_close"])
            atr = float(signal["atr_touch"])
            zone_lower = float(signal["zone_lower"])
            zone_upper = float(signal["zone_upper"])
        except (KeyError, TypeError, ValueError, OverflowError):
            raise ValueError("BTC_STRUCTURE_PENDING_SIGNAL_INVALID") from None
        if index + OUTCOME_HORIZON_BARS >= len(frame):
            return None
        result, offset = classify_event_outcome(
            closes[index + 1 : index + OUTCOME_HORIZON_BARS + 1],
            kind=kind,
            touch_close=touch_close,
            atr=atr,
            zone_lower=zone_lower,
            zone_upper=zone_upper,
        )
        outcome_index = index + (offset if offset is not None else OUTCOME_HORIZON_BARS)
        entry_index = index + 1
        entry_price = float(frame.loc[entry_index, "open"])
        exit_price = float(frame.loc[outcome_index, "close"])
        role_sign = 1.0 if kind == "SUPPORT" else -1.0
        gross_return = role_sign * (exit_price / entry_price - 1.0)
        gross_r = role_sign * (exit_price - entry_price) / atr
        path = frame.loc[entry_index:outcome_index]
        if kind == "SUPPORT":
            favorable = path["high"].to_numpy(float) / entry_price - 1.0
            adverse = path["low"].to_numpy(float) / entry_price - 1.0
        else:
            favorable = 1.0 - path["low"].to_numpy(float) / entry_price
            adverse = 1.0 - path["high"].to_numpy(float) / entry_price
        entry_at = datetime.fromtimestamp(
            float(frame.loc[entry_index, "open_time_ms"]) / 1000,
            tz=UTC,
        )
        outcome_at = datetime.fromtimestamp(
            float(frame.loc[outcome_index, "close_time_ms"]) / 1000,
            tz=UTC,
        )
        return BtcStructureEventRevision(
            event_key=stored.event_key,
            event_at=stored.event_at,
            observed_at=observed_at,
            state=result,  # type: ignore[arg-type]
            payload={
                "algorithm_version": ALGORITHM_VERSION,
                "signal": signal,
                "outcome": {
                    "state": result,
                    "outcome_at": iso_utc(outcome_at),
                    "outcome_bars": offset if offset is not None else OUTCOME_HORIZON_BARS,
                    "evaluated_at": iso_utc(observed_at),
                    "unresolved_retained_in_denominator": True,
                    "entry_at": iso_utc(entry_at),
                    "entry_price": _float_text(entry_price),
                    "exit_price": _float_text(exit_price),
                    "gross_return_percent": _float_text(gross_return * 100.0),
                    "gross_r": _float_text(gross_r),
                    "net_return_30bps_percent": _float_text(
                        (gross_return - EVENT_BASE_COST_BPS / 10_000.0) * 100.0
                    ),
                    "net_return_50bps_percent": _float_text(
                        (gross_return - EVENT_STRESS_COST_BPS / 10_000.0) * 100.0
                    ),
                    "maximum_favorable_excursion_percent": _float_text(
                        float(np.max(favorable)) * 100.0
                    ),
                    "maximum_adverse_excursion_percent": _float_text(
                        float(np.min(adverse)) * 100.0
                    ),
                    "price_path_source": "BINANCE_SPOT_PUBLIC_4H_CLOSED",
                    "price_path_cutoff_at": iso_utc(outcome_at),
                },
            },
        )


__all__ = [
    "ALGORITHM_VERSION",
    "BinanceBtcIntelligenceMonitor",
    "BinanceBtcIntelligenceSettings",
    "BinanceSpotMarketClient",
    "Pivot",
    "SpotMarketError",
    "SpotSeriesResult",
    "Zone",
    "add_4h_indicators",
    "classify_event_outcome",
    "daily_donchian_state",
    "find_pivots",
    "form_zones",
    "monthly_state",
    "normalize_spot_klines",
    "scan_structure_events",
    "unified_regime",
]
