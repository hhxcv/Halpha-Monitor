"""Bounded USDⓈ-M perpetual radar for explainable altcoin anomalies.

The monitor intentionally reports observable stages and evidence scores.  It does
not claim a calibrated probability, causal prediction, or trading instruction.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import random
import threading
from typing import Any, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener

from halpha_monitor.contracts import (
    CollectionBatch,
    CollectionCancelled,
    CollectionIssue,
    EvaluationView,
    FilterChoice,
    ForwardEvaluationCase,
    ForwardEvaluationResult,
    MetricSample,
    MonitorView,
    ProjectionSnapshot,
    ViewColumn,
    ViewFilter,
    ViewSummaryField,
)
from halpha_monitor.store import SQLiteMonitorStore, iso_utc
from halpha_monitor.telemetry import NetworkRequestWindow


BINANCE_USDM_BASE = "https://fapi.binance.com"
USER_AGENT = "Halpha-Monitor/0.1 public-market-read-only"
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
KLINE_INTERVAL_MINUTES = 5
DAILY_INTERVAL_SECONDS = 86_400
DAILY_HISTORY_DAYS = 220
DAILY_MINIMUM_DAYS = 90
DAILY_CACHE_OVERLAP_DAYS = 3
PUMP_BASELINE_DAYS = 7
PUMP_DAILY_TRIGGER_PERCENT = 15.0
PUMP_THREE_DAY_TRIGGER_PERCENT = 25.0
PUMP_SEVEN_DAY_TRIGGER_PERCENT = 25.0
PUMP_EPISODE_RESET_DRAWDOWN_PERCENT = -18.0
PRICE_POSITION_SNAPSHOT_KEY = "price-position-v1"
MARKET_SCOPE = "USDM_PERPETUAL"
BASELINE_EVALUATION_SOURCE = "BINANCE_USDM_PUBLIC_CLOSED_5M_KLINES"
EVALUATION_SOURCE = "BINANCE_USDM_PRICE_CONTEXT_V1_CLOSED_5M_KLINES"
PRICE_CONTEXT_MODEL_VERSION = "price-context-v1"
EVALUATION_HORIZONS_MINUTES = (15, 60, 240)
EVALUATION_REPEAT_AFTER = timedelta(hours=4)
EVALUATION_GRACE = timedelta(hours=1)
EVALUATION_RELATIVE_BAND_PERCENT = 0.5
EVALUATION_DIRECTIONS = {
    "SETUP": "UP",
    "BREAKOUT": "UP",
    "ACCELERATION": "UP",
    "EXHAUSTION": "DOWN",
    "COOLDOWN": "DOWN",
}

STABLE_BASE_ASSETS = frozenset(
    {
        "BFUSD",
        "U",
        "USDT",
        "USDC",
        "FDUSD",
        "TUSD",
        "USDP",
        "USDE",
        "USDS",
        "USD1",
        "XUSD",
        "RLUSD",
        "DAI",
        "BUSD",
        "EUR",
        "EURI",
        "TRY",
        "BRL",
    }
)
LEVERAGED_BASE_ASSETS = frozenset(
    f"{underlying}{direction}"
    for underlying in (
        "1INCH",
        "AAVE",
        "ADA",
        "BNB",
        "BTC",
        "DOT",
        "EOS",
        "ETH",
        "FIL",
        "LINK",
        "LTC",
        "SUSHI",
        "TRX",
        "UNI",
        "XLM",
        "XRP",
        "YFI",
    )
    for direction in ("UP", "DOWN", "BULL", "BEAR")
)
THROTTLE_REASON_CODES = frozenset(
    {
        "RADAR_BACKOFF_ACTIVE",
        "RADAR_HTTP_THROTTLED_418",
        "RADAR_HTTP_THROTTLED_429",
    }
)

STAGE_LABELS = {
    "SETUP": "蓄势观察",
    "BREAKOUT": "启动",
    "ACCELERATION": "加速",
    "EXHAUSTION": "尾声风险",
    "COOLDOWN": "回落确认",
    "NEUTRAL": "尚未形成",
}
STAGE_TONES = {
    "SETUP": "INFO",
    "BREAKOUT": "WARNING",
    "ACCELERATION": "DANGER",
    "EXHAUSTION": "DANGER",
    "COOLDOWN": "MUTED",
    "NEUTRAL": "NEUTRAL",
}
STAGE_REVIEW_LABELS = {
    "SETUP": "下一根 5m K 复核",
    "BREAKOUT": "下一根 5m K 复核",
    "ACCELERATION": "每根 5m K 复核",
    "EXHAUSTION": "当前即复核",
    "COOLDOWN": "每根 5m K 复核",
    "NEUTRAL": "下次采集重算",
}

PRICE_POSITION_LABELS = {
    "CRASH": "暴跌",
    "SURGE": "暴涨",
    "PUMPING": "拉升中",
    "RETEST_PREVIOUS_PEAK": "前轮高点附近",
    "HIGH_CONSOLIDATION": "高位整理",
    "BOTTOM_CONSOLIDATION": "底部横盘",
    "REBOUND": "底部反弹",
    "PERSISTENT_DECLINE": "持续下跌",
    "HIGH_ZONE": "高位",
    "BOTTOM_ZONE": "底部",
    "MID_RANGE": "区间中部",
}
PRICE_POSITION_GROUPS = {
    "CRASH": "FALLING",
    "SURGE": "RISING",
    "PUMPING": "RISING",
    "RETEST_PREVIOUS_PEAK": "HIGH",
    "HIGH_CONSOLIDATION": "HIGH",
    "BOTTOM_CONSOLIDATION": "BOTTOM",
    "REBOUND": "BOTTOM",
    "PERSISTENT_DECLINE": "FALLING",
    "HIGH_ZONE": "HIGH",
    "BOTTOM_ZONE": "BOTTOM",
    "MID_RANGE": "NEUTRAL",
}
PRICE_POSITION_TONES = {
    "CRASH": "DANGER",
    "SURGE": "WARNING",
    "PUMPING": "WARNING",
    "RETEST_PREVIOUS_PEAK": "INFO",
    "HIGH_CONSOLIDATION": "INFO",
    "BOTTOM_CONSOLIDATION": "HEALTHY",
    "REBOUND": "HEALTHY",
    "PERSISTENT_DECLINE": "MUTED",
    "HIGH_ZONE": "INFO",
    "BOTTOM_ZONE": "HEALTHY",
    "MID_RANGE": "NEUTRAL",
}
PRICE_POSITION_RANKS = {
    "CRASH": 1,
    "SURGE": 2,
    "PUMPING": 3,
    "RETEST_PREVIOUS_PEAK": 4,
    "PERSISTENT_DECLINE": 5,
    "REBOUND": 6,
    "HIGH_CONSOLIDATION": 7,
    "BOTTOM_CONSOLIDATION": 8,
    "HIGH_ZONE": 9,
    "BOTTOM_ZONE": 10,
    "MID_RANGE": 11,
}

CONTEXT_STAGE_GROUP_LABELS = {
    "EARLY_RISE": "低位与突破",
    "HIGH_RISK": "高位回落风险",
    "DECLINING": "下跌延续",
    "WATCHING": "等待位置确认",
    "NEUTRAL": "尚未形成",
}

CONTEXT_STAGE_RANKS = {
    "BLOWOFF_RISK": 1,
    "HIGH_REVERSAL_RISK": 2,
    "HIGH_REJECTION_RISK": 3,
    "DECLINE_CONTINUATION": 4,
    "LOW_REVERSAL_ACCELERATION": 5,
    "LOW_BREAKOUT": 6,
    "PEAK_BREAKOUT_TEST": 7,
    "BOTTOM_SETUP": 8,
    "SETUP_UNCONFIRMED": 9,
    "BREAKOUT_UNCONFIRMED": 9,
    "ACCELERATION_UNCONFIRMED": 9,
    "EXHAUSTION_UNCONFIRMED": 9,
    "COOLDOWN_UNCONFIRMED": 9,
    "PRICE_CONTEXT_UNAVAILABLE": 10,
    "NO_SIGNAL": 11,
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def _decimal(value: Any, *, field: str, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"RADAR_DECIMAL_INVALID field={field}") from None
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError(f"RADAR_DECIMAL_INVALID field={field}")
    return parsed


def _float_text(value: float | None, digits: int = 6) -> str | None:
    if value is None or not math.isfinite(value):
        return None
    return f"{value:.{digits}f}"


def _timestamp_ms(value: Any, *, field: str) -> datetime:
    try:
        parsed = int(value)
        result = datetime.fromtimestamp(parsed / 1000, tz=UTC)
    except (OverflowError, OSError, TypeError, ValueError):
        raise ValueError(f"RADAR_TIMESTAMP_INVALID field={field}") from None
    return result


def _bounded(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return min(max(value, minimum), maximum)


def _ramp(value: float, start: float, end: float, points: float) -> float:
    if value <= start:
        return 0.0
    if value >= end:
        return points
    return points * (value - start) / (end - start)


def _percent_change(current: float, previous: float) -> float | None:
    if current <= 0 or previous <= 0:
        return None
    return (current / previous - 1.0) * 100.0


def _ratio(current: float, baseline: float) -> float | None:
    if current < 0 or baseline <= 0:
        return None
    return current / baseline


def _eligible_altcoin(base_asset: str) -> bool:
    normalized = base_asset.upper()
    return (
        normalized != "BTC"
        and normalized not in STABLE_BASE_ASSETS
        and normalized not in LEVERAGED_BASE_ASSETS
    )


@dataclass(frozen=True)
class RollingTicker:
    symbol: str
    price_change_percent: float
    open_price: float
    high_price: float
    low_price: float
    last_price: float
    quote_volume: float
    trade_count: int
    close_time: datetime

    @property
    def range_position_percent(self) -> float | None:
        span = self.high_price - self.low_price
        if span <= 0:
            return None
        return _bounded((self.last_price - self.low_price) / span * 100.0)


@dataclass(frozen=True)
class ContractCandle:
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    quote_volume: float
    trade_count: int
    taker_buy_quote_volume: float


@dataclass(frozen=True)
class DailyContractCandle:
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    quote_volume: float


@dataclass(frozen=True)
class DailySeriesResult:
    symbol: str
    status: str
    candles: tuple[DailyContractCandle, ...]
    latest_close_at: datetime | None
    acquired_at: datetime | None
    reason_code: str | None = None

    @property
    def current(self) -> bool:
        return self.status in {"FETCHED", "CACHE_CURRENT"}


@dataclass(frozen=True)
class FundingContext:
    symbol: str
    funding_rate_percent: float
    source_time: datetime


@dataclass(frozen=True)
class OpenInterestPoint:
    value: float
    source_time: datetime


@dataclass(frozen=True)
class TimedValue:
    value: Any
    completed_at: datetime
    malformed_count: int = 0


@dataclass(frozen=True)
class CandleFeatures:
    cutoff_at: datetime
    return_15m_percent: float
    return_1h_percent: float
    quote_volume_ratio_15m: float | None
    trade_count_ratio_15m: float | None
    taker_buy_percent: float | None
    breakout_percent: float | None
    range_position_1h_percent: float | None
    compression_ratio: float | None
    peak_drawdown_15m_percent: float
    latest_candle_return_percent: float
    close_price: float


@dataclass(frozen=True)
class PricePositionFeatures:
    daily_cutoff_at: datetime
    history_days: int
    range_days: int
    current_price: float
    return_24h_percent: float
    return_3d_percent: float
    return_7d_percent: float
    return_14d_percent: float
    return_30d_percent: float
    return_90d_percent: float
    position_90d_percent: float
    distance_from_range_high_percent: float
    distance_from_range_low_percent: float
    current_pump_multiple: float | None
    pump_baseline_price: float | None
    pump_start_at: datetime | None
    pump_start_trigger: str | None
    distance_from_previous_pump_peak_percent: float | None
    previous_pump_peak_at: datetime | None
    previous_pump_peak_price: float | None
    ma20_gap_percent: float
    ma60_gap_percent: float
    trend_structure: str
    range_30d_percent: float
    volatility_compression_ratio: float | None
    state: str
    state_label: str
    state_group: str
    state_reason: str
    row_tone: str
    state_rank: int


@dataclass(frozen=True)
class ContextualStage:
    """Price-aware interpretation of one already-observed short-term stage."""

    stage: str
    label: str
    group: str
    direction: str | None
    evaluation_horizons_minutes: tuple[int, ...]
    reason: str
    row_tone: str
    rank: int

    @property
    def evaluation_target_label(self) -> str:
        if self.direction is None or not self.evaluation_horizons_minutes:
            return "不输出方向"
        horizon_labels = {
            15: "15分钟",
            60: "1小时",
            240: "4小时",
        }
        direction_label = "向上" if self.direction == "UP" else "向下"
        horizons = "/".join(
            horizon_labels[value] for value in self.evaluation_horizons_minutes
        )
        return f"{direction_label} · {horizons}"


@dataclass(frozen=True)
class CandidateSeed:
    symbol: str
    base_asset: str
    ticker_24h: RollingTicker


@dataclass(frozen=True)
class CandidateEnrichment:
    seed: CandidateSeed
    features: CandleFeatures | None
    funding: FundingContext | None
    oi_change_15m_percent: float | None
    issues: tuple[CollectionIssue, ...]
    missing_reason: str | None = None


class RadarSourceError(RuntimeError):
    def __init__(self, reason_code: str, *, throttled: bool = False) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.throttled = throttled


class AltcoinRadarProvider(Protocol):
    def ensure_available(self) -> None: ...

    def reset_throttle_backoff(self) -> None: ...

    def exchange_symbols(self) -> TimedValue: ...

    def ticker_24h(self) -> TimedValue: ...

    def klines(self, symbol: str, *, limit: int) -> TimedValue: ...

    def daily_klines(
        self,
        symbol: str,
        *,
        start_at: datetime,
        end_at: datetime,
        limit: int,
    ) -> TimedValue: ...

    def klines_between(
        self,
        symbol: str,
        *,
        start_at: datetime,
        end_at: datetime,
        limit: int,
    ) -> TimedValue: ...

    def futures_premium_index(self) -> TimedValue: ...

    def open_interest_history(self, symbol: str, *, limit: int) -> TimedValue: ...


class DailySeriesProvider(Protocol):
    def fetch(self, symbol: str, cutoff: datetime) -> DailySeriesResult: ...


def parse_exchange_symbols(payload: Any) -> tuple[dict[str, str], int]:
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise RadarSourceError("RADAR_EXCHANGE_INFO_SCHEMA_INVALID")
    parsed: dict[str, str] = {}
    malformed = 0
    for item in payload["symbols"]:
        if not isinstance(item, dict):
            malformed += 1
            continue
        try:
            symbol = str(item["symbol"]).upper()
            base_asset = str(item["baseAsset"]).upper()
            quote_asset = str(item["quoteAsset"]).upper()
            status = str(item["status"]).upper()
            contract_type = str(item["contractType"]).upper()
            underlying_type = str(item["underlyingType"]).upper()
        except (KeyError, TypeError, ValueError):
            malformed += 1
            continue
        if (
            not symbol
            or not base_asset
            or quote_asset != "USDT"
            or status != "TRADING"
            or contract_type != "PERPETUAL"
            or underlying_type != "COIN"
        ):
            continue
        parsed[symbol] = base_asset
    if not parsed:
        raise RadarSourceError("RADAR_EXCHANGE_INFO_EMPTY")
    return parsed, malformed


def parse_tickers(payload: Any) -> tuple[dict[str, RollingTicker], int]:
    rows = payload if isinstance(payload, list) else [payload]
    if not rows or not isinstance(rows, list):
        raise RadarSourceError("RADAR_TICKER_SCHEMA_INVALID")
    parsed: dict[str, RollingTicker] = {}
    malformed = 0
    for item in rows:
        if not isinstance(item, dict):
            malformed += 1
            continue
        try:
            symbol = str(item["symbol"]).upper()
            raw_open = _decimal(item["openPrice"], field="openPrice")
            raw_high = _decimal(item["highPrice"], field="highPrice")
            raw_low = _decimal(item["lowPrice"], field="lowPrice")
            raw_last = _decimal(item["lastPrice"], field="lastPrice")
            quote_volume = float(_decimal(item["quoteVolume"], field="quoteVolume"))
            price_change_percent = float(
                _decimal(item["priceChangePercent"], field="priceChangePercent")
            )
            trade_count = int(item["count"])
            close_time = _timestamp_ms(item["closeTime"], field="closeTime")
            if (
                trade_count == 0
                and quote_volume == 0
                and raw_open == raw_high == raw_low == raw_last == 0
            ):
                continue
            open_price = float(raw_open)
            high_price = float(raw_high)
            low_price = float(raw_low)
            last_price = float(raw_last)
            if (
                not symbol
                or min(open_price, high_price, low_price, last_price) <= 0
                or quote_volume < 0
                or trade_count < 0
                or low_price > min(open_price, last_price)
                or high_price < max(open_price, last_price)
            ):
                raise ValueError("ticker values invalid")
        except (KeyError, TypeError, ValueError):
            malformed += 1
            continue
        parsed[symbol] = RollingTicker(
            symbol=symbol,
            price_change_percent=price_change_percent,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            last_price=last_price,
            quote_volume=quote_volume,
            trade_count=trade_count,
            close_time=close_time,
        )
    if not parsed:
        raise RadarSourceError("RADAR_TICKER_EMPTY")
    return parsed, malformed


def parse_contract_klines(
    payload: Any,
    *,
    completed_at: datetime,
) -> tuple[tuple[ContractCandle, ...], int]:
    if not isinstance(payload, list):
        raise RadarSourceError("RADAR_KLINES_SCHEMA_INVALID")
    parsed: dict[int, ContractCandle] = {}
    malformed = 0
    completed_ms = int(completed_at.timestamp() * 1000)
    for row in payload:
        if not isinstance(row, list) or len(row) < 11:
            malformed += 1
            continue
        try:
            open_time = _timestamp_ms(row[0], field="kline.openTime")
            close_time = _timestamp_ms(row[6], field="kline.closeTime")
            if int(row[6]) > completed_ms:
                continue
            open_price = float(_decimal(row[1], field="kline.open", positive=True))
            high_price = float(_decimal(row[2], field="kline.high", positive=True))
            low_price = float(_decimal(row[3], field="kline.low", positive=True))
            close_price = float(_decimal(row[4], field="kline.close", positive=True))
            quote_volume = float(_decimal(row[7], field="kline.quoteVolume"))
            trade_count = int(row[8])
            taker_buy_quote_volume = float(
                _decimal(row[10], field="kline.takerBuyQuoteVolume")
            )
            if (
                close_time <= open_time
                or abs((close_time - open_time).total_seconds() - 300.0) > 5.0
                or low_price > min(open_price, close_price)
                or high_price < max(open_price, close_price)
                or quote_volume < 0
                or taker_buy_quote_volume < 0
                or taker_buy_quote_volume > quote_volume + 1e-9
                or trade_count < 0
            ):
                raise ValueError("kline values invalid")
        except (OverflowError, TypeError, ValueError):
            malformed += 1
            continue
        parsed[int(row[0])] = ContractCandle(
            open_time=open_time,
            close_time=close_time,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            quote_volume=quote_volume,
            trade_count=trade_count,
            taker_buy_quote_volume=taker_buy_quote_volume,
        )
    candles = tuple(parsed[key] for key in sorted(parsed))
    if not candles:
        raise RadarSourceError("RADAR_KLINES_EMPTY")
    return candles, malformed


def parse_daily_contract_klines(
    payload: Any,
    *,
    completed_at: datetime,
) -> tuple[tuple[DailyContractCandle, ...], int]:
    """Normalize only fully closed UTC daily USDⓈ-M candles."""

    if not isinstance(payload, list):
        raise RadarSourceError("RADAR_DAILY_KLINES_SCHEMA_INVALID")
    parsed: dict[int, DailyContractCandle] = {}
    malformed = 0
    completed_ms = int(completed_at.timestamp() * 1000)
    for row in payload:
        if not isinstance(row, list) or len(row) < 8:
            malformed += 1
            continue
        try:
            open_time = _timestamp_ms(row[0], field="dailyKline.openTime")
            close_time = _timestamp_ms(row[6], field="dailyKline.closeTime")
            if int(row[6]) > completed_ms:
                continue
            open_price = float(_decimal(row[1], field="dailyKline.open", positive=True))
            high_price = float(_decimal(row[2], field="dailyKline.high", positive=True))
            low_price = float(_decimal(row[3], field="dailyKline.low", positive=True))
            close_price = float(
                _decimal(row[4], field="dailyKline.close", positive=True)
            )
            quote_volume = float(_decimal(row[7], field="dailyKline.quoteVolume"))
            duration = (close_time - open_time).total_seconds()
            if (
                duration <= 0
                or abs(duration - (DAILY_INTERVAL_SECONDS - 0.001)) > 5
                or open_time.hour != 0
                or open_time.minute != 0
                or open_time.second != 0
                or open_time.microsecond != 0
                or low_price > min(open_price, close_price)
                or high_price < max(open_price, close_price)
                or quote_volume < 0
            ):
                raise ValueError("daily kline values invalid")
        except (OverflowError, TypeError, ValueError):
            malformed += 1
            continue
        parsed[int(row[0])] = DailyContractCandle(
            open_time=open_time,
            close_time=close_time,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            quote_volume=quote_volume,
        )
    candles = tuple(parsed[key] for key in sorted(parsed))
    if not candles:
        raise RadarSourceError("RADAR_DAILY_KLINES_EMPTY")
    return candles, malformed


def parse_funding_contexts(payload: Any) -> tuple[dict[str, FundingContext], int]:
    rows = payload if isinstance(payload, list) else [payload]
    if not rows or not isinstance(rows, list):
        raise RadarSourceError("RADAR_FUTURES_SCHEMA_INVALID")
    parsed: dict[str, FundingContext] = {}
    malformed = 0
    for item in rows:
        if not isinstance(item, dict):
            malformed += 1
            continue
        try:
            symbol = str(item["symbol"]).upper()
            rate = float(_decimal(item["lastFundingRate"], field="lastFundingRate"))
            source_time = _timestamp_ms(item["time"], field="premiumIndex.time")
            if not symbol:
                raise ValueError("symbol empty")
        except (KeyError, TypeError, ValueError):
            malformed += 1
            continue
        parsed[symbol] = FundingContext(
            symbol=symbol,
            funding_rate_percent=rate * 100.0,
            source_time=source_time,
        )
    if not parsed:
        raise RadarSourceError("RADAR_FUTURES_EMPTY")
    return parsed, malformed


def parse_open_interest_history(
    payload: Any,
) -> tuple[tuple[OpenInterestPoint, ...], int]:
    if not isinstance(payload, list):
        raise RadarSourceError("RADAR_OI_SCHEMA_INVALID")
    parsed: dict[int, OpenInterestPoint] = {}
    malformed = 0
    for item in payload:
        if not isinstance(item, dict):
            malformed += 1
            continue
        try:
            value = float(
                _decimal(
                    item["sumOpenInterestValue"],
                    field="sumOpenInterestValue",
                    positive=True,
                )
            )
            source_time = _timestamp_ms(item["timestamp"], field="oi.timestamp")
        except (KeyError, TypeError, ValueError):
            malformed += 1
            continue
        parsed[int(item["timestamp"])] = OpenInterestPoint(value, source_time)
    points = tuple(parsed[key] for key in sorted(parsed))
    if not points:
        raise RadarSourceError("RADAR_OI_EMPTY")
    return points, malformed


class BinanceAltcoinRadarClient:
    """Unauthenticated JSON client with shared, bounded throttle backoff."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        proxy_url: str | None = None,
        opener: OpenerDirector | None = None,
        now: Any = utc_now,
        random_uniform: Any = random.uniform,
    ) -> None:
        if opener is not None and proxy_url is not None:
            raise ValueError("opener and proxy_url are mutually exclusive")
        self.timeout_seconds = timeout_seconds
        self.opener = opener or (
            build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
            if proxy_url
            else build_opener()
        )
        self._now = now
        self._random_uniform = random_uniform
        self._backoff_until: datetime | None = None
        self._throttle_failures = 0
        self._backoff_lock = threading.Lock()
        self._network_requests = NetworkRequestWindow()
        self._stop_event: threading.Event | None = None

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event

    def _raise_if_cancelled(self) -> None:
        if self._stop_event is not None and self._stop_event.is_set():
            raise CollectionCancelled("RADAR_COLLECTION_CANCELLED")

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return self._network_requests.count(window_seconds=window_seconds)

    def ensure_available(self) -> None:
        self._raise_if_cancelled()
        with self._backoff_lock:
            if self._backoff_until is not None and self._now() < self._backoff_until:
                raise RadarSourceError("RADAR_BACKOFF_ACTIVE", throttled=True)

    def reset_throttle_backoff(self) -> None:
        with self._backoff_lock:
            self._backoff_until = None
            self._throttle_failures = 0

    def exchange_symbols(self) -> TimedValue:
        response = self._get_json(
            BINANCE_USDM_BASE,
            "/fapi/v1/exchangeInfo",
            (),
        )
        value, malformed = parse_exchange_symbols(response.value)
        return TimedValue(value, response.completed_at, malformed)

    def ticker_24h(self) -> TimedValue:
        response = self._get_json(
            BINANCE_USDM_BASE,
            "/fapi/v1/ticker/24hr",
            (),
        )
        value, malformed = parse_tickers(response.value)
        return TimedValue(value, response.completed_at, malformed)

    def klines(self, symbol: str, *, limit: int) -> TimedValue:
        response = self._get_json(
            BINANCE_USDM_BASE,
            "/fapi/v1/klines",
            (("symbol", symbol), ("interval", "5m"), ("limit", str(limit))),
        )
        value, malformed = parse_contract_klines(
            response.value,
            completed_at=response.completed_at,
        )
        return TimedValue(value, response.completed_at, malformed)

    def daily_klines(
        self,
        symbol: str,
        *,
        start_at: datetime,
        end_at: datetime,
        limit: int,
    ) -> TimedValue:
        if limit < 1 or limit > 500 or end_at <= start_at:
            raise ValueError("RADAR_DAILY_RANGE_INVALID")
        response = self._get_json(
            BINANCE_USDM_BASE,
            "/fapi/v1/klines",
            (
                ("symbol", symbol),
                ("interval", "1d"),
                ("startTime", str(int(start_at.timestamp() * 1000))),
                ("endTime", str(int(end_at.timestamp() * 1000))),
                ("limit", str(limit)),
            ),
        )
        value, malformed = parse_daily_contract_klines(
            response.value,
            completed_at=response.completed_at,
        )
        return TimedValue(value, response.completed_at, malformed)

    def klines_between(
        self,
        symbol: str,
        *,
        start_at: datetime,
        end_at: datetime,
        limit: int,
    ) -> TimedValue:
        if limit < 1 or limit > 1000 or end_at <= start_at:
            raise ValueError("RADAR_EVALUATION_RANGE_INVALID")
        response = self._get_json(
            BINANCE_USDM_BASE,
            "/fapi/v1/klines",
            (
                ("symbol", symbol),
                ("interval", "5m"),
                ("startTime", str(int(start_at.timestamp() * 1000) + 1)),
                ("endTime", str(int(end_at.timestamp() * 1000) + 1)),
                ("limit", str(limit)),
            ),
        )
        value, malformed = parse_contract_klines(
            response.value,
            completed_at=response.completed_at,
        )
        return TimedValue(value, response.completed_at, malformed)

    def futures_premium_index(self) -> TimedValue:
        response = self._get_json(BINANCE_USDM_BASE, "/fapi/v1/premiumIndex", ())
        value, malformed = parse_funding_contexts(response.value)
        return TimedValue(value, response.completed_at, malformed)

    def open_interest_history(self, symbol: str, *, limit: int) -> TimedValue:
        response = self._get_json(
            BINANCE_USDM_BASE,
            "/futures/data/openInterestHist",
            (("symbol", symbol), ("period", "5m"), ("limit", str(limit))),
        )
        value, malformed = parse_open_interest_history(response.value)
        return TimedValue(value, response.completed_at, malformed)

    def _get_json(
        self,
        base: str,
        path: str,
        params: tuple[tuple[str, str], ...],
    ) -> TimedValue:
        self.ensure_available()
        source = f"{base}{path}"
        if params:
            source = f"{source}?{urlencode(params)}"
        request = Request(
            source,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        self._network_requests.record()
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                raw_status = getattr(response, "status", None)
                status = int(
                    raw_status if raw_status is not None else response.getcode()
                )
                body = response.read(MAX_RESPONSE_BYTES + 1)
                headers = response.headers
        except HTTPError as exc:
            if exc.code in {418, 429}:
                self._open_throttle_backoff(exc.headers.get("Retry-After"))
                raise RadarSourceError(
                    f"RADAR_HTTP_THROTTLED_{exc.code}", throttled=True
                ) from None
            raise RadarSourceError(f"RADAR_HTTP_{exc.code}") from None
        except (TimeoutError, URLError, OSError) as exc:
            raise RadarSourceError(
                f"RADAR_NETWORK_{type(exc).__name__.upper()}"
            ) from None
        completed_at = self._now()
        if status in {418, 429}:
            retry_after = headers.get("Retry-After") if headers is not None else None
            self._open_throttle_backoff(retry_after)
            raise RadarSourceError(f"RADAR_HTTP_THROTTLED_{status}", throttled=True)
        if status < 200 or status >= 300:
            raise RadarSourceError(f"RADAR_HTTP_{status}")
        if len(body) > MAX_RESPONSE_BYTES:
            raise RadarSourceError("RADAR_RESPONSE_TOO_LARGE")
        if not body:
            raise RadarSourceError("RADAR_RESPONSE_EMPTY")
        try:
            payload = json.loads(body.decode("utf-8"), parse_float=Decimal)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RadarSourceError("RADAR_RESPONSE_JSON_INVALID") from None
        return TimedValue(payload, completed_at)

    def _open_throttle_backoff(self, retry_after: str | None) -> None:
        with self._backoff_lock:
            self._throttle_failures += 1
            exponential = min(3600.0, 30.0 * (2 ** (self._throttle_failures - 1)))
            try:
                upstream = max(0.0, float(retry_after or 0.0))
            except ValueError:
                upstream = 0.0
            delay = min(3600.0, max(exponential, upstream))
            delay = min(
                3600.0,
                delay + self._random_uniform(0.0, min(15.0, delay * 0.1)),
            )
            candidate = self._now() + timedelta(seconds=delay)
            if self._backoff_until is None or candidate > self._backoff_until:
                self._backoff_until = candidate


def latest_closed_daily_cutoff(now: datetime | None = None) -> datetime:
    current = (now or utc_now()).astimezone(UTC)
    return current.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        milliseconds=1
    )


def _read_daily_cache(path: Path) -> tuple[DailyContractCandle, ...]:
    if not path.is_file() or path.is_symlink():
        return ()
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            expected = {
                "open_time_ms",
                "close_time_ms",
                "open",
                "high",
                "low",
                "close",
                "quote_volume",
            }
            if set(reader.fieldnames or ()) != expected:
                return ()
            parsed: dict[int, DailyContractCandle] = {}
            for row in reader:
                open_time_ms = int(row["open_time_ms"])
                close_time_ms = int(row["close_time_ms"])
                open_price = float(row["open"])
                high_price = float(row["high"])
                low_price = float(row["low"])
                close_price = float(row["close"])
                quote_volume = float(row["quote_volume"])
                numbers = (
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    quote_volume,
                )
                if (
                    not all(math.isfinite(value) for value in numbers)
                    or min(open_price, high_price, low_price, close_price) <= 0
                    or quote_volume < 0
                    or low_price > min(open_price, close_price)
                    or high_price < max(open_price, close_price)
                    or abs(
                        (close_time_ms - open_time_ms)
                        - (DAILY_INTERVAL_SECONDS * 1000 - 1)
                    )
                    > 5_000
                    or open_time_ms % int(DAILY_INTERVAL_SECONDS * 1000) != 0
                ):
                    return ()
                parsed[open_time_ms] = DailyContractCandle(
                    open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=UTC),
                    close_time=datetime.fromtimestamp(close_time_ms / 1000, tz=UTC),
                    open_price=open_price,
                    high_price=high_price,
                    low_price=low_price,
                    close_price=close_price,
                    quote_volume=quote_volume,
                )
    except (KeyError, OSError, TypeError, ValueError, csv.Error):
        return ()
    return tuple(parsed[key] for key in sorted(parsed))


def _write_daily_cache(
    candles: Sequence[DailyContractCandle],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=(
                    "open_time_ms",
                    "close_time_ms",
                    "open",
                    "high",
                    "low",
                    "close",
                    "quote_volume",
                ),
                lineterminator="\n",
            )
            writer.writeheader()
            for candle in candles:
                writer.writerow(
                    {
                        "open_time_ms": int(candle.open_time.timestamp() * 1000),
                        "close_time_ms": int(candle.close_time.timestamp() * 1000),
                        "open": format(candle.open_price, ".12g"),
                        "high": format(candle.high_price, ".12g"),
                        "low": format(candle.low_price, ".12g"),
                        "close": format(candle.close_price, ".12g"),
                        "quote_volume": format(candle.quote_volume, ".12g"),
                    }
                )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class BinanceUsdmDailyCache:
    """Bounded normalized daily cache backed by the radar's public client."""

    def __init__(self, cache_root: Path, client: AltcoinRadarProvider) -> None:
        self.cache_root = cache_root.resolve()
        self.client = client

    def fetch(self, symbol: str, cutoff: datetime) -> DailySeriesResult:
        if (
            not symbol
            or len(symbol) > 64
            or not symbol.isalnum()
            or not symbol.upper().endswith("USDT")
        ):
            return DailySeriesResult(
                symbol,
                "FAILED",
                (),
                None,
                None,
                "RADAR_DAILY_SYMBOL_INVALID",
            )
        cache_key = (
            symbol
            if symbol.isascii()
            else f"unicode-{hashlib.sha256(symbol.encode('utf-8')).hexdigest()}"
        )
        cache_path = self.cache_root / f"{cache_key}.csv.gz"
        current = _read_daily_cache(cache_path)
        cutoff = cutoff.astimezone(UTC)
        latest = current[-1].close_time if current else None
        acquired_at = (
            datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC)
            if cache_path.is_file() and not cache_path.is_symlink()
            else None
        )
        if latest is not None and latest >= cutoff:
            return DailySeriesResult(
                symbol,
                "CACHE_CURRENT",
                current,
                latest,
                acquired_at,
            )

        next_midnight = cutoff + timedelta(milliseconds=1)
        earliest_open = next_midnight - timedelta(days=DAILY_HISTORY_DAYS)
        start_at = earliest_open
        if current:
            overlap_index = max(0, len(current) - DAILY_CACHE_OVERLAP_DAYS)
            start_at = max(earliest_open, current[overlap_index].open_time)
        requested_days = (
            math.ceil(
                max(1.0, (cutoff - start_at).total_seconds()) / DAILY_INTERVAL_SECONDS
            )
            + 2
        )
        try:
            result = self.client.daily_klines(
                symbol,
                start_at=start_at,
                end_at=cutoff,
                limit=min(500, max(1, requested_days)),
            )
        except RadarSourceError as exc:
            return DailySeriesResult(
                symbol,
                "FAILED" if not current else "STALE",
                current,
                latest,
                acquired_at,
                exc.reason_code,
            )
        except ValueError:
            return DailySeriesResult(
                symbol,
                "FAILED" if not current else "STALE",
                current,
                latest,
                acquired_at,
                "RADAR_DAILY_RANGE_INVALID",
            )

        merged = {
            int(candle.open_time.timestamp() * 1000): candle
            for candle in (*current, *tuple(result.value))
            if candle.open_time >= earliest_open and candle.close_time <= cutoff
        }
        candles = tuple(merged[key] for key in sorted(merged))[
            -(DAILY_HISTORY_DAYS + DAILY_CACHE_OVERLAP_DAYS) :
        ]
        latest = candles[-1].close_time if candles else None
        if latest is None or latest < cutoff:
            return DailySeriesResult(
                symbol,
                "FAILED" if not candles else "STALE",
                candles,
                latest,
                result.completed_at,
                "RADAR_DAILY_KLINES_STALE",
            )
        _write_daily_cache(candles, cache_path)
        return DailySeriesResult(
            symbol,
            "FETCHED",
            candles,
            latest,
            result.completed_at,
            ("RADAR_DAILY_SOURCE_ROWS_MALFORMED" if result.malformed_count else None),
        )


@dataclass(frozen=True)
class BinanceAltcoinRadarSettings:
    interval_seconds: float = 3600
    jitter_seconds: float = 30
    min_quote_volume_24h: Decimal = Decimal("5000000")
    max_candidates: int = 30
    max_screened_contracts: int = 240
    kline_limit: int = 48
    workers: int = 6
    timeout_seconds: float = 10
    proxy_url: str | None = None
    ticker_stale_seconds: int = 180
    kline_stale_seconds: int = 15 * 60
    futures_stale_seconds: int = 15 * 60
    cache_root: Path | None = None

    def __post_init__(self) -> None:
        if self.interval_seconds < 300:
            raise ValueError("RADAR_INTERVAL_TOO_SHORT")
        if not 0 <= self.jitter_seconds <= 120:
            raise ValueError("RADAR_JITTER_INVALID")
        if not self.min_quote_volume_24h.is_finite() or self.min_quote_volume_24h <= 0:
            raise ValueError("RADAR_MIN_QUOTE_VOLUME_INVALID")
        if not 5 <= self.max_candidates <= 50:
            raise ValueError("RADAR_MAX_CANDIDATES_INVALID")
        if not self.max_candidates <= self.max_screened_contracts <= 500:
            raise ValueError("RADAR_MAX_SCREENED_CONTRACTS_INVALID")
        if not 30 <= self.kline_limit <= 100:
            raise ValueError("RADAR_KLINE_LIMIT_INVALID")
        if not 1 <= self.workers <= 8:
            raise ValueError("RADAR_WORKERS_INVALID")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 60:
            raise ValueError("RADAR_TIMEOUT_INVALID")
        if (
            self.ticker_stale_seconds < 60
            or self.kline_stale_seconds < 300
            or self.futures_stale_seconds < 300
        ):
            raise ValueError("RADAR_FRESHNESS_INVALID")


def analyze_candles(candles: Sequence[ContractCandle]) -> CandleFeatures:
    """Derive closed-candle features without interpolation or synthetic bars."""

    ordered = tuple(sorted(candles, key=lambda candle: candle.open_time))
    if len(ordered) < 27:
        raise RadarSourceError("RADAR_KLINES_INSUFFICIENT")
    if any(
        current.open_time <= previous.open_time
        or abs(
            (current.open_time - previous.open_time).total_seconds()
            - KLINE_INTERVAL_MINUTES * 60
        )
        > 5
        for previous, current in zip(ordered, ordered[1:])
    ):
        raise RadarSourceError("RADAR_KLINES_NON_CONTIGUOUS")

    latest = ordered[-1]
    recent_15m = ordered[-3:]
    previous_45m = ordered[-12:-3]
    recent_1h = ordered[-12:]
    setup_window = ordered[-15:-3]
    comparison_window = ordered[-27:-15]

    return_15m = _percent_change(latest.close_price, ordered[-4].close_price)
    return_1h = _percent_change(latest.close_price, ordered[-13].close_price)
    if return_15m is None or return_1h is None:
        raise RadarSourceError("RADAR_KLINES_COMPUTATION_INVALID")

    recent_quote = sum(candle.quote_volume for candle in recent_15m)
    baseline_quote = sum(candle.quote_volume for candle in previous_45m) / 3.0
    recent_trades = float(sum(candle.trade_count for candle in recent_15m))
    baseline_trades = sum(candle.trade_count for candle in previous_45m) / 3.0
    quote_volume_ratio = _ratio(recent_quote, baseline_quote)
    trade_count_ratio = _ratio(recent_trades, baseline_trades)
    taker_buy_percent = (
        sum(candle.taker_buy_quote_volume for candle in recent_15m)
        / recent_quote
        * 100.0
        if recent_quote > 0
        else None
    )

    prior_high = max(candle.high_price for candle in setup_window)
    breakout_percent = _percent_change(latest.close_price, prior_high)
    one_hour_low = min(candle.low_price for candle in recent_1h)
    one_hour_high = max(candle.high_price for candle in recent_1h)
    one_hour_span = one_hour_high - one_hour_low
    range_position = (
        _bounded((latest.close_price - one_hour_low) / one_hour_span * 100.0)
        if one_hour_span > 0
        else None
    )

    def normalized_range(values: Sequence[ContractCandle]) -> float:
        return sum(
            (candle.high_price - candle.low_price) / candle.close_price
            for candle in values
        ) / len(values)

    comparison_range = normalized_range(comparison_window)
    compression_ratio = (
        normalized_range(setup_window) / comparison_range
        if comparison_range > 0
        else None
    )
    recent_peak = max(candle.high_price for candle in recent_15m)
    peak_drawdown = _percent_change(latest.close_price, recent_peak)
    latest_return = _percent_change(latest.close_price, latest.open_price)
    if peak_drawdown is None or latest_return is None:
        raise RadarSourceError("RADAR_KLINES_COMPUTATION_INVALID")

    return CandleFeatures(
        cutoff_at=latest.close_time,
        return_15m_percent=return_15m,
        return_1h_percent=return_1h,
        quote_volume_ratio_15m=quote_volume_ratio,
        trade_count_ratio_15m=trade_count_ratio,
        taker_buy_percent=taker_buy_percent,
        breakout_percent=breakout_percent,
        range_position_1h_percent=range_position,
        compression_ratio=compression_ratio,
        peak_drawdown_15m_percent=peak_drawdown,
        latest_candle_return_percent=latest_return,
        close_price=latest.close_price,
    )


def _daily_return(
    current_price: float,
    candles: Sequence[DailyContractCandle],
    days: int,
) -> float:
    if len(candles) < days:
        raise RadarSourceError("RADAR_DAILY_HISTORY_INSUFFICIENT")
    result = _percent_change(current_price, candles[-days].close_price)
    if result is None:
        raise RadarSourceError("RADAR_DAILY_COMPUTATION_INVALID")
    return result


def _previous_pump_peak(
    candles: Sequence[DailyContractCandle],
) -> tuple[datetime, float] | None:
    """Return the most recent completed price-spike peak in the lookback.

    A peak must be a local 11-day high, rise at least 30% from the prior
    14-day low, and then draw down at least 18% in the following 14 days.
    Requiring the subsequent drawdown prevents the current move from being
    mislabeled as a completed previous episode.
    """

    if len(candles) < 43:
        return None
    matches: list[tuple[datetime, float]] = []
    for index in range(14, len(candles) - 14):
        peak = candles[index].high_price
        local = candles[max(0, index - 5) : index + 6]
        if peak < max(candle.high_price for candle in local) - 1e-12:
            continue
        prior_low = min(candle.low_price for candle in candles[index - 14 : index])
        following_low = min(
            candle.low_price for candle in candles[index + 1 : index + 15]
        )
        advance = _percent_change(peak, prior_low)
        drawdown = _percent_change(following_low, peak)
        if (
            advance is not None
            and drawdown is not None
            and advance >= 30.0
            and drawdown <= -18.0
        ):
            matches.append((candles[index].close_time, peak))
    return matches[-1] if matches else None


def _current_pump_leg(
    candles: Sequence[DailyContractCandle],
    *,
    current_price: float,
    return_24h_percent: float,
    state: str,
) -> tuple[datetime, float, float, str] | None:
    """Return the active rise start, pre-start baseline, multiple and trigger.

    The start belongs to the earliest threshold window in the current episode.
    For multi-day windows it is the first UTC day after that window's last low.
    Only closed daily closes enter the seven-day pre-start baseline.  The live
    UTC day may be the start when the rolling 24-hour trigger is already met.
    """

    if state not in {"SURGE", "PUMPING"}:
        return None
    if len(candles) < PUMP_BASELINE_DAYS + 1:
        return None

    observations = [
        (candle.open_time, candle.close_price) for candle in candles
    ]
    current_day = candles[-1].open_time + timedelta(days=1)
    observations.append((current_day, current_price))
    prices = [price for _, price in observations]

    # A completed close at least 18% below the running peak ends the prior
    # episode.  The live, still-open UTC day never rewrites that boundary.
    segment_start = 0
    running_peak = prices[0]
    for index in range(1, len(candles)):
        drawdown = _percent_change(prices[index], running_peak)
        if (
            drawdown is not None
            and drawdown <= PUMP_EPISODE_RESET_DRAWDOWN_PERCENT
        ):
            segment_start = index
            running_peak = prices[index]
        elif prices[index] > running_peak:
            running_peak = prices[index]

    triggers: list[tuple[int, int, str]] = []
    current_index = len(observations) - 1
    if return_24h_percent >= PUMP_DAILY_TRIGGER_PERCENT:
        triggers.append((current_index, current_index, "ROLLING_24H_15"))

    trigger_windows = (
        (1, PUMP_DAILY_TRIGGER_PERCENT, "DAILY_WINDOW_1D_15"),
        (3, PUMP_THREE_DAY_TRIGGER_PERCENT, "DAILY_WINDOW_3D_25"),
        (7, PUMP_SEVEN_DAY_TRIGGER_PERCENT, "DAILY_WINDOW_7D_25"),
    )
    for end_index in range(segment_start + 1, len(observations)):
        for window_days, threshold, trigger in trigger_windows:
            base_index = end_index - window_days
            if base_index < segment_start:
                continue
            window_return = _percent_change(
                prices[end_index],
                prices[base_index],
            )
            if window_return is None or window_return < threshold:
                continue
            pre_end_prices = prices[base_index:end_index]
            window_low = min(pre_end_prices)
            low_index = max(
                index
                for index in range(base_index, end_index)
                if abs(prices[index] - window_low) <= 1e-12
            )
            start_index = min(low_index + 1, end_index)
            triggers.append((start_index, end_index, trigger))

    if not triggers:
        return None
    start_index, _, trigger = min(
        triggers,
        key=lambda item: (item[0], item[1], item[2]),
    )
    if start_index < PUMP_BASELINE_DAYS:
        return None
    baseline_prices = prices[
        start_index - PUMP_BASELINE_DAYS : start_index
    ]
    baseline_price = sum(baseline_prices) / PUMP_BASELINE_DAYS
    if baseline_price <= 0 or not math.isfinite(baseline_price):
        return None
    return (
        observations[start_index][0],
        baseline_price,
        current_price / baseline_price,
        trigger,
    )


def analyze_price_position(
    candles: Sequence[DailyContractCandle],
    *,
    current_price: float,
    return_24h_percent: float,
) -> PricePositionFeatures:
    """Classify an explainable current price location from closed UTC days."""

    if not math.isfinite(current_price) or current_price <= 0:
        raise RadarSourceError("RADAR_CURRENT_PRICE_INVALID")
    ordered = tuple(sorted(candles, key=lambda candle: candle.open_time))
    if len(ordered) < DAILY_MINIMUM_DAYS:
        raise RadarSourceError("RADAR_DAILY_HISTORY_INSUFFICIENT")
    recent = ordered[-min(DAILY_HISTORY_DAYS, len(ordered)) :]
    if any(
        current.open_time <= previous.open_time
        or abs(
            (current.open_time - previous.open_time).total_seconds()
            - DAILY_INTERVAL_SECONDS
        )
        > 5
        for previous, current in zip(recent, recent[1:])
    ):
        raise RadarSourceError("RADAR_DAILY_KLINES_NON_CONTIGUOUS")

    return_3d = _daily_return(current_price, recent, 3)
    return_7d = _daily_return(current_price, recent, 7)
    return_14d = _daily_return(current_price, recent, 14)
    return_30d = _daily_return(current_price, recent, 30)
    return_90d = _daily_return(current_price, recent, 90)
    window_90 = recent[-90:]
    range_window = recent[-min(180, len(recent)) :]
    high_90 = max(current_price, *(candle.high_price for candle in window_90))
    low_90 = min(current_price, *(candle.low_price for candle in window_90))
    span_90 = high_90 - low_90
    if span_90 <= 0:
        raise RadarSourceError("RADAR_DAILY_COMPUTATION_INVALID")
    position_90 = _bounded((current_price - low_90) / span_90 * 100.0)
    range_high = max(
        current_price,
        *(candle.high_price for candle in range_window),
    )
    range_low = min(
        current_price,
        *(candle.low_price for candle in range_window),
    )
    distance_high = _percent_change(current_price, range_high)
    distance_low = _percent_change(current_price, range_low)
    if distance_high is None or distance_low is None:
        raise RadarSourceError("RADAR_DAILY_COMPUTATION_INVALID")

    closes = [candle.close_price for candle in recent]
    ma20 = sum(closes[-20:]) / 20.0
    ma60 = sum(closes[-60:]) / 60.0
    ma20_gap = _percent_change(current_price, ma20)
    ma60_gap = _percent_change(current_price, ma60)
    if ma20_gap is None or ma60_gap is None:
        raise RadarSourceError("RADAR_DAILY_COMPUTATION_INVALID")
    if current_price > ma20 > ma60:
        trend_structure = "多头排列"
    elif current_price < ma20 < ma60:
        trend_structure = "空头排列"
    elif current_price > ma20 and ma20 <= ma60:
        trend_structure = "短线转强"
    elif current_price < ma20 and ma20 >= ma60:
        trend_structure = "短线转弱"
    else:
        trend_structure = "均线交错"

    window_30 = recent[-30:]
    range_30_high = max(current_price, *(candle.high_price for candle in window_30))
    range_30_low = min(current_price, *(candle.low_price for candle in window_30))
    range_30 = _percent_change(range_30_high, range_30_low)
    if range_30 is None:
        raise RadarSourceError("RADAR_DAILY_COMPUTATION_INVALID")
    true_range_percent: list[float] = []
    for previous, candle in zip(recent, recent[1:]):
        true_range = max(
            candle.high_price - candle.low_price,
            abs(candle.high_price - previous.close_price),
            abs(candle.low_price - previous.close_price),
        )
        true_range_percent.append(true_range / previous.close_price * 100.0)
    atr20 = sum(true_range_percent[-20:]) / 20.0
    atr60 = sum(true_range_percent[-60:]) / 60.0
    compression = atr20 / atr60 if atr60 > 0 else None

    previous_peak = _previous_pump_peak(range_window)
    previous_peak_at = previous_peak[0] if previous_peak else None
    previous_peak_price = previous_peak[1] if previous_peak else None
    previous_peak_distance = (
        _percent_change(current_price, previous_peak_price)
        if previous_peak_price is not None
        else None
    )

    facts = {
        "24h": return_24h_percent,
        "3日": return_3d,
        "7日": return_7d,
        "14日": return_14d,
        "30日": return_30d,
        "90日": return_90d,
    }

    def signed(key: str) -> str:
        return f"{key} {facts[key]:+.1f}%"

    if return_24h_percent <= -12.0 or return_3d <= -20.0:
        state = "CRASH"
        reason = f"{signed('24h')}，{signed('3日')}"
    elif (
        return_24h_percent >= PUMP_DAILY_TRIGGER_PERCENT
        or return_3d >= PUMP_THREE_DAY_TRIGGER_PERCENT
    ):
        state = "SURGE"
        reason = f"{signed('24h')}，{signed('3日')}"
    elif (
        return_7d >= PUMP_SEVEN_DAY_TRIGGER_PERCENT
        and distance_high >= -12.0
        and current_price > ma20
    ):
        state = "PUMPING"
        reason = f"{signed('7日')}，距{len(range_window)}日高点 {distance_high:+.1f}%"
    elif previous_peak_distance is not None and -12.0 <= previous_peak_distance <= 8.0:
        state = "RETEST_PREVIOUS_PEAK"
        reason = f"距上一轮完整拉升峰值 {previous_peak_distance:+.1f}%"
    elif position_90 >= 80.0 and abs(return_7d) <= 10.0 and distance_high >= -15.0:
        state = "HIGH_CONSOLIDATION"
        reason = f"90日位置 {position_90:.0f}%，{signed('7日')}"
    elif (
        position_90 <= 20.0
        and abs(return_14d) <= 8.0
        and range_30 <= 18.0
        and compression is not None
        and compression <= 0.90
    ):
        state = "BOTTOM_CONSOLIDATION"
        reason = f"90日位置 {position_90:.0f}%，30日振幅 {range_30:.1f}%"
    elif position_90 <= 45.0 and return_7d >= 12.0 and current_price > ma20:
        state = "REBOUND"
        reason = f"90日位置 {position_90:.0f}%，{signed('7日')}"
    elif return_30d <= -20.0 and current_price < ma20 < ma60:
        state = "PERSISTENT_DECLINE"
        reason = f"{signed('30日')}，现价低于20日与60日均线"
    elif position_90 >= 75.0:
        state = "HIGH_ZONE"
        reason = f"90日位置 {position_90:.0f}%，距区间高点 {distance_high:+.1f}%"
    elif position_90 <= 25.0:
        state = "BOTTOM_ZONE"
        reason = f"90日位置 {position_90:.0f}%，距区间低点 {distance_low:+.1f}%"
    else:
        state = "MID_RANGE"
        reason = f"90日位置 {position_90:.0f}%，{signed('30日')}"

    current_pump = _current_pump_leg(
        recent,
        current_price=current_price,
        return_24h_percent=return_24h_percent,
        state=state,
    )

    return PricePositionFeatures(
        daily_cutoff_at=recent[-1].close_time,
        history_days=len(recent),
        range_days=len(range_window),
        current_price=current_price,
        return_24h_percent=return_24h_percent,
        return_3d_percent=return_3d,
        return_7d_percent=return_7d,
        return_14d_percent=return_14d,
        return_30d_percent=return_30d,
        return_90d_percent=return_90d,
        position_90d_percent=position_90,
        distance_from_range_high_percent=distance_high,
        distance_from_range_low_percent=distance_low,
        current_pump_multiple=(
            current_pump[2] if current_pump is not None else None
        ),
        pump_baseline_price=(current_pump[1] if current_pump is not None else None),
        pump_start_at=(current_pump[0] if current_pump is not None else None),
        pump_start_trigger=(current_pump[3] if current_pump is not None else None),
        distance_from_previous_pump_peak_percent=previous_peak_distance,
        previous_pump_peak_at=previous_peak_at,
        previous_pump_peak_price=previous_peak_price,
        ma20_gap_percent=ma20_gap,
        ma60_gap_percent=ma60_gap,
        trend_structure=trend_structure,
        range_30d_percent=range_30,
        volatility_compression_ratio=compression,
        state=state,
        state_label=PRICE_POSITION_LABELS[state],
        state_group=PRICE_POSITION_GROUPS[state],
        state_reason=reason,
        row_tone=PRICE_POSITION_TONES[state],
        state_rank=PRICE_POSITION_RANKS[state],
    )


def classify_contextual_stage(
    short_stage: str,
    price_position: PricePositionFeatures | None,
) -> ContextualStage:
    """Combine short-horizon flow with signal-time daily price location.

    The rules deliberately emit a direction for only a small set of combinations.
    A visible state without a direction remains useful for screening but does not
    create a forward-evaluation claim.
    """

    if short_stage not in STAGE_LABELS:
        raise ValueError("RADAR_STAGE_INVALID")

    def result(
        stage: str,
        label: str,
        group: str,
        direction: str | None,
        horizons: tuple[int, ...],
        reason: str,
        row_tone: str,
    ) -> ContextualStage:
        return ContextualStage(
            stage=stage,
            label=label,
            group=group,
            direction=direction,
            evaluation_horizons_minutes=horizons,
            reason=reason,
            row_tone=row_tone,
            rank=CONTEXT_STAGE_RANKS[stage],
        )

    short_label = STAGE_LABELS[short_stage]
    if price_position is None:
        return result(
            "PRICE_CONTEXT_UNAVAILABLE",
            f"{short_label} · 仅短线",
            "WATCHING",
            None,
            (),
            "本轮没有连续90根已闭合日线；保留短线触发，但不输出融合方向。",
            "NEUTRAL",
        )
    if short_stage == "NEUTRAL":
        return result(
            "NO_SIGNAL",
            "尚未形成",
            "NEUTRAL",
            None,
            (),
            f"短线尚未形成异动；日线处于{price_position.state_label}。",
            "NEUTRAL",
        )

    state = price_position.state
    position = price_position.position_90d_percent
    return_7d = price_position.return_7d_percent
    return_30d = price_position.return_30d_percent
    high_context = state in {
        "SURGE",
        "PUMPING",
        "RETEST_PREVIOUS_PEAK",
        "HIGH_CONSOLIDATION",
        "HIGH_ZONE",
    }
    low_context = state in {
        "CRASH",
        "PERSISTENT_DECLINE",
        "BOTTOM_CONSOLIDATION",
        "BOTTOM_ZONE",
        "REBOUND",
    }
    bearish_context = state in {"CRASH", "PERSISTENT_DECLINE"} or (
        return_30d <= -20.0 and price_position.trend_structure == "空头排列"
    )

    if short_stage in {"EXHAUSTION", "COOLDOWN"}:
        if high_context or position >= 70.0 or return_7d >= 15.0:
            return result(
                "HIGH_REVERSAL_RISK",
                "高位回落风险",
                "HIGH_RISK",
                "DOWN",
                (60,),
                f"短线{short_label}；日线{price_position.state_label}，"
                f"90日位置{position:.0f}%。",
                "DANGER",
            )
        if bearish_context:
            return result(
                "DECLINE_CONTINUATION",
                "下跌延续",
                "DECLINING",
                "DOWN",
                (240,),
                f"短线{short_label}；日线{price_position.state_label}，"
                f"30日{return_30d:+.1f}%。",
                "MUTED",
            )

    if short_stage == "ACCELERATION":
        if state in {"SURGE", "PUMPING"} or return_7d >= 25.0 or position >= 85.0:
            return result(
                "BLOWOFF_RISK",
                "冲高回落风险",
                "HIGH_RISK",
                "DOWN",
                (240,),
                f"短线加速；日线{price_position.state_label}，"
                f"7日{return_7d:+.1f}%，90日位置{position:.0f}%。",
                "DANGER",
            )
        if low_context and position <= 45.0:
            return result(
                "LOW_REVERSAL_ACCELERATION",
                "超跌反弹加速",
                "EARLY_RISE",
                "UP",
                (15, 60),
                f"短线加速；日线{price_position.state_label}，"
                f"90日位置{position:.0f}%。",
                "WARNING",
            )

    if short_stage == "BREAKOUT":
        if state in {"SURGE", "PUMPING"} or return_7d >= 25.0 or position >= 90.0:
            return result(
                "BLOWOFF_RISK",
                "冲高回落风险",
                "HIGH_RISK",
                "DOWN",
                (240,),
                f"短线启动但日线已{price_position.state_label}；"
                f"7日{return_7d:+.1f}%，90日位置{position:.0f}%。",
                "DANGER",
            )
        if state == "RETEST_PREVIOUS_PEAK" or position >= 75.0:
            return result(
                "PEAK_BREAKOUT_TEST",
                "前高突破测试",
                "EARLY_RISE",
                "UP",
                (15,),
                f"短线启动；日线{price_position.state_label}，"
                f"90日位置{position:.0f}%。",
                "WARNING",
            )
        if low_context or position <= 35.0:
            return result(
                "LOW_BREAKOUT",
                "低位启动",
                "EARLY_RISE",
                "UP",
                (15,),
                f"短线启动；日线{price_position.state_label}，"
                f"90日位置{position:.0f}%。",
                "WARNING",
            )

    if short_stage == "SETUP":
        if state == "BOTTOM_CONSOLIDATION" or (
            state == "BOTTOM_ZONE" and price_position.range_30d_percent <= 30.0
        ):
            return result(
                "BOTTOM_SETUP",
                "底部蓄势",
                "EARLY_RISE",
                "UP",
                (240,),
                f"短线蓄势；日线{price_position.state_label}，"
                f"90日位置{position:.0f}%，30日振幅"
                f"{price_position.range_30d_percent:.1f}%。",
                "INFO",
            )
        if bearish_context:
            return result(
                "DECLINE_CONTINUATION",
                "下跌延续",
                "DECLINING",
                "DOWN",
                (240,),
                f"短线蓄势未改变日线{price_position.state_label}；"
                f"30日{return_30d:+.1f}%。",
                "MUTED",
            )
        if state in {
            "RETEST_PREVIOUS_PEAK",
            "HIGH_ZONE",
            "HIGH_CONSOLIDATION",
        } and position >= 70.0:
            return result(
                "HIGH_REJECTION_RISK",
                "高位受阻风险",
                "HIGH_RISK",
                "DOWN",
                (240,),
                f"短线仅蓄势；日线{price_position.state_label}，"
                f"90日位置{position:.0f}%。",
                "WARNING",
            )

    return result(
        f"{short_stage}_UNCONFIRMED",
        f"{short_label}待位置确认",
        "WATCHING",
        None,
        (),
        f"短线{short_label}与日线{price_position.state_label}未形成已定义组合；"
        "本轮不输出方向。",
        "NEUTRAL",
    )


def open_interest_change_15m(
    points: Sequence[OpenInterestPoint],
) -> float | None:
    ordered = tuple(sorted(points, key=lambda point: point.source_time))
    if len(ordered) < 4:
        return None
    recent = ordered[-4:]
    if any(
        current.source_time <= previous.source_time
        or abs(
            (current.source_time - previous.source_time).total_seconds()
            - KLINE_INTERVAL_MINUTES * 60
        )
        > 5
        for previous, current in zip(recent, recent[1:])
    ):
        return None
    return _percent_change(recent[-1].value, recent[0].value)


def _screening_priority(ticker_24h: RollingTicker) -> float:
    """Prioritize broad daily anomalies only when the bounded screen must truncate."""

    daily_range_percent = (
        (ticker_24h.high_price - ticker_24h.low_price) / ticker_24h.open_price * 100.0
    )
    range_position = ticker_24h.range_position_percent
    range_extremity = abs(
        (range_position if range_position is not None else 50.0) - 50.0
    )
    liquidity_scale = math.log10(max(ticker_24h.quote_volume, 1.0))
    return _bounded(
        _ramp(abs(ticker_24h.price_change_percent), 0.5, 20.0, 40.0)
        + _ramp(daily_range_percent, 1.0, 30.0, 30.0)
        + _ramp(range_extremity, 8.0, 45.0, 20.0)
        + _ramp(liquidity_scale, 6.5, 9.5, 10.0)
    )


def select_candidate_seeds(
    symbols: dict[str, str],
    tickers_24h: dict[str, RollingTicker],
    *,
    min_quote_volume_24h: float,
    maximum: int,
) -> tuple[CandidateSeed, ...]:
    seeds: list[CandidateSeed] = []
    for symbol, base_asset in symbols.items():
        if not _eligible_altcoin(base_asset):
            continue
        ticker_24h = tickers_24h.get(symbol)
        if ticker_24h is None or ticker_24h.quote_volume < min_quote_volume_24h:
            continue
        seeds.append(
            CandidateSeed(
                symbol=symbol,
                base_asset=base_asset,
                ticker_24h=ticker_24h,
            )
        )
    seeds.sort(
        key=lambda seed: (
            -_screening_priority(seed.ticker_24h),
            -seed.ticker_24h.quote_volume,
            seed.symbol,
        )
    )
    return tuple(seeds[:maximum])


def score_candidate(
    features: CandleFeatures,
    *,
    return_24h_percent: float,
    relative_return_15m_percent: float | None,
    funding_rate_percent: float | None,
    oi_change_15m_percent: float | None,
) -> dict[str, Any]:
    volume_ratio = features.quote_volume_ratio_15m or 0.0
    trade_ratio = features.trade_count_ratio_15m or 0.0
    buy_share = features.taker_buy_percent
    breakout = features.breakout_percent
    range_position = features.range_position_1h_percent
    compression = features.compression_ratio

    setup_score = (
        _ramp(volume_ratio, 1.0, 3.0, 25.0)
        + _ramp(trade_ratio, 1.0, 2.5, 15.0)
        + _ramp(buy_share or 0.0, 50.0, 65.0, 15.0)
        + _ramp(range_position or 0.0, 55.0, 90.0, 15.0)
    )
    if compression is not None:
        setup_score += _ramp(1.0 - compression, 0.0, 0.45, 20.0)
    if -0.75 <= features.return_15m_percent <= 2.5:
        setup_score += 10.0
    elif -1.5 <= features.return_15m_percent <= 4.0:
        setup_score += 4.0
    setup_score = _bounded(setup_score)

    pump_score = (
        _ramp(features.return_15m_percent, 0.5, 6.0, 30.0)
        + _ramp(volume_ratio, 1.2, 4.0, 25.0)
        + _ramp(trade_ratio, 1.2, 3.0, 10.0)
        + _ramp(buy_share or 0.0, 50.0, 70.0, 15.0)
        + _ramp(breakout if breakout is not None else -10.0, -0.5, 2.0, 10.0)
    )
    if relative_return_15m_percent is not None:
        pump_score += _ramp(relative_return_15m_percent, 0.0, 5.0, 10.0)
    pump_score = _bounded(pump_score)

    tail_risk_score = (
        _ramp(features.return_1h_percent, 5.0, 20.0, 20.0)
        + _ramp(return_24h_percent, 10.0, 50.0, 10.0)
        + _ramp(-features.peak_drawdown_15m_percent, 1.0, 5.0, 25.0)
    )
    if volume_ratio >= 2.0 and buy_share is not None:
        tail_risk_score += _ramp(52.0 - buy_share, 0.0, 12.0, 20.0)
    if features.latest_candle_return_percent < 0 and features.return_1h_percent > 5:
        tail_risk_score += 15.0
    if funding_rate_percent is not None:
        tail_risk_score += _ramp(funding_rate_percent, 0.03, 0.15, 10.0)
    if (
        oi_change_15m_percent is not None
        and oi_change_15m_percent < 0
        and features.return_15m_percent <= 0
        and features.return_1h_percent > 3
    ):
        tail_risk_score += 10.0
    tail_risk_score = _bounded(tail_risk_score)

    if (
        features.return_1h_percent <= -3.0
        and return_24h_percent >= 5.0
        and (
            (range_position is not None and range_position < 45.0)
            or (buy_share is not None and buy_share < 45.0)
        )
    ):
        stage = "COOLDOWN"
    elif tail_risk_score >= 55.0 and (pump_score >= 45.0 or return_24h_percent >= 10.0):
        stage = "EXHAUSTION"
    elif pump_score >= 70.0 and features.return_15m_percent >= 3.0:
        stage = "ACCELERATION"
    elif pump_score >= 55.0 and (
        (breakout is not None and breakout >= 0.0)
        or (range_position is not None and range_position >= 80.0)
    ):
        stage = "BREAKOUT"
    elif setup_score >= 60.0 and -0.75 <= features.return_15m_percent <= 2.5:
        stage = "SETUP"
    else:
        stage = "NEUTRAL"

    alert_score = (
        setup_score
        if stage == "SETUP"
        else pump_score
        if stage in {"BREAKOUT", "ACCELERATION"}
        else tail_risk_score
        if stage in {"EXHAUSTION", "COOLDOWN"}
        else max(setup_score, pump_score, tail_risk_score)
    )
    return {
        "stage": stage,
        "alert_score": round(alert_score, 3),
        "setup_score": round(setup_score, 3),
        "pump_score": round(pump_score, 3),
        "tail_risk_score": round(tail_risk_score, 3),
    }


def _evidence_label(
    stage: str,
    features: CandleFeatures,
    *,
    relative_return_15m_percent: float | None,
    funding_rate_percent: float | None,
    oi_change_15m_percent: float | None,
) -> str:
    evidence: list[tuple[float, str]] = []
    if features.quote_volume_ratio_15m is not None:
        evidence.append(
            (
                abs(features.quote_volume_ratio_15m - 1.0) * 10,
                f"15m 成交额 {features.quote_volume_ratio_15m:.1f}×",
            )
        )
    evidence.append(
        (
            abs(features.return_15m_percent) * 4,
            f"15m 涨跌 {features.return_15m_percent:+.2f}%",
        )
    )
    if features.taker_buy_percent is not None:
        evidence.append(
            (
                abs(features.taker_buy_percent - 50.0),
                f"主动买入 {features.taker_buy_percent:.1f}%",
            )
        )
    if stage == "SETUP" and features.compression_ratio is not None:
        evidence.append(
            (
                max(0.0, 1.0 - features.compression_ratio) * 30,
                f"波动压缩 {features.compression_ratio:.2f}×",
            )
        )
    if stage in {"EXHAUSTION", "COOLDOWN"}:
        evidence.append(
            (
                abs(features.peak_drawdown_15m_percent) * 5,
                f"15m 峰值回撤 {features.peak_drawdown_15m_percent:+.2f}%",
            )
        )
    if relative_return_15m_percent is not None:
        evidence.append(
            (
                abs(relative_return_15m_percent) * 2,
                f"相对 BTC {relative_return_15m_percent:+.2f}%",
            )
        )
    if funding_rate_percent is not None and abs(funding_rate_percent) >= 0.02:
        evidence.append(
            (
                abs(funding_rate_percent) * 50,
                f"资金费率 {funding_rate_percent:+.4f}%",
            )
        )
    if oi_change_15m_percent is not None and abs(oi_change_15m_percent) >= 1.0:
        evidence.append(
            (
                abs(oi_change_15m_percent) * 2,
                f"OI 15m {oi_change_15m_percent:+.2f}%",
            )
        )
    evidence.sort(key=lambda item: -item[0])
    return " · ".join(label for _, label in evidence[:3])


class BinanceAltcoinRadarMonitor:
    monitor_id = "binance-altcoin-radar"
    display_name = "山寨币异动雷达"
    projection_kind = "altcoin_radar"
    price_position_snapshot_key = PRICE_POSITION_SNAPSHOT_KEY
    price_position_table_title = "日线价格位置"
    price_position_method_note = (
        "状态使用当前合约价与最多180根已闭合UTC日线计算；"
        "90日位置、距区间高低点、均线结构和独立收益窗口均保留为可排序事实。"
        "暴涨或拉升中会以起点前7根闭合日线收盘均价计算当前拉升倍数，"
        "起点日不进入均价。"
        "“上一轮拉升峰值”只认定先在14日内上涨至少30%、随后14日回撤至少18%的"
        "已完成价格段，不把当前上涨直接当作上一轮。"
    )
    price_position_filter_choices = (
        FilterChoice("*", "全部状态"),
        FilterChoice("RISING", "拉升与暴涨", "暴涨或仍处于快速拉升规则内。"),
        FilterChoice("HIGH", "高位与前高", "处于高位、整理或接近上一轮拉升峰值。"),
        FilterChoice("BOTTOM", "底部与反弹", "处于区间底部、底部横盘或低位反弹。"),
        FilterChoice("FALLING", "下跌与暴跌", "持续下跌或短期暴跌。"),
        FilterChoice("NEUTRAL", "区间中部", "未落入上述状态的区间中部。"),
    )
    price_position_columns = (
        ViewColumn("symbol", "币种"),
        ViewColumn(
            "price_state_label",
            "价格状态",
            description=(
                "按顺序判定：24h≤-12%或3日≤-20%为暴跌；24h≥15%或3日≥25%"
                "为暴涨；7日≥25%、距区间高点不超过12%且高于20日均线为拉升中；"
                "距上一轮完整拉升峰值-12%至+8%为前高附近；90日位置≥80%、"
                "7日波动≤10%且距区间高点≤15%为高位整理；90日位置≤20%、"
                "14日波动≤8%、30日振幅≤18%且20日波动不高于60日的90%为底部横盘；"
                "随后依次检查低位反弹、持续下跌、高位、底部和区间中部。"
                "它描述价格形态，不证明操纵主体。"
            ),
        ),
        ViewColumn(
            "state_reason",
            "判定依据",
            description="触发当前价格状态的最少一组可核验价格事实。",
        ),
        ViewColumn(
            "current_price",
            "现价",
            "number",
            maximum_fraction_digits=8,
            use_grouping=True,
        ),
        ViewColumn(
            "current_pump_multiple",
            "拉升倍数",
            "number",
            minimum_fraction_digits=2,
            maximum_fraction_digits=2,
            description=(
                "仅在价格状态为暴涨或拉升中时计算：当前合约价 ÷ 拉升起点前"
                "7根已闭合UTC日线收盘价的算术平均。1.50表示现价为基准价的1.5倍；"
                "小于1表示短期虽触发暴涨，但当前价仍低于起点前均价；起点日不进入均价。"
            ),
        ),
        ViewColumn(
            "pump_start_date",
            "拉升起点",
            description=(
                "当前活动价格段首次命中涨幅门槛的UTC日期：滚动24h涨幅≥15%，"
                "或以当前价/历史收盘计算的1日涨幅≥15%、3日或7日涨幅≥25%。"
                "多日窗口取窗口低点后的第一个UTC日；当前UTC日尚未收盘时可作为起点，"
                "但不会进入基准均价。已闭合日线从峰值回撤18%后重新识别一轮。"
            ),
        ),
        ViewColumn(
            "pump_baseline_price",
            "拉升基准价",
            "number",
            maximum_fraction_digits=8,
            use_grouping=True,
            description=(
                "拉升起点之前连续7根已闭合UTC日线收盘价的算术平均；"
                "严格不包含拉升起点日，也不使用当天尚未收盘的价格。"
            ),
        ),
        ViewColumn(
            "return_24h_percent",
            "24h涨跌",
            "percent",
            description="Binance USDⓈ-M合约滚动24小时价格涨跌幅。",
        ),
        ViewColumn(
            "return_7d_percent",
            "7日涨跌",
            "percent",
            description="当前价相对7根前闭合UTC日线收盘价的涨跌幅。",
        ),
        ViewColumn(
            "return_30d_percent",
            "30日涨跌",
            "percent",
            description="当前价相对30根前闭合UTC日线收盘价的涨跌幅。",
        ),
        ViewColumn(
            "position_90d_percent",
            "90日位置",
            "percent",
            show_sign=False,
            description=(
                "当前价在最近90根闭合日线最高价与最低价之间的位置；"
                "0%为区间最低，100%为区间最高。"
            ),
        ),
        ViewColumn(
            "distance_from_range_high_percent",
            "距区间高点",
            "percent",
            description=(
                "当前价相对最多180根闭合日线最高价的距离；负值表示仍低于高点。"
                "实际覆盖日数见“日线覆盖”。"
            ),
        ),
        ViewColumn(
            "distance_from_previous_pump_peak_percent",
            "距前轮高点",
            "percent",
            description=(
                "当前价相对最近一个完整拉升回落段峰值的距离。该峰值必须是局部高点，"
                "此前14日内从低点上涨至少30%，此后14日内回撤至少18%；"
                "未识别到完整价格段时保持为空。"
            ),
        ),
        ViewColumn(
            "previous_pump_peak_date",
            "前轮高点日期",
            priority="secondary",
            description="最近一个满足完整拉升回落规则的UTC日线峰值日期。",
        ),
        ViewColumn(
            "trend_structure",
            "均线结构",
            priority="secondary",
            description=(
                "比较当前价、最近20根与60根闭合日线收盘均值，"
                "显示多头排列、空头排列、短线转强、短线转弱或均线交错。"
            ),
        ),
        ViewColumn(
            "return_90d_percent",
            "90日涨跌",
            "percent",
            priority="secondary",
            description="当前价相对90根前闭合UTC日线收盘价的涨跌幅。",
        ),
        ViewColumn(
            "distance_from_range_low_percent",
            "距区间低点",
            "percent",
            priority="secondary",
            description=(
                "当前价相对最多180根闭合日线最低价的距离；0%表示正在该区间最低点。"
            ),
        ),
        ViewColumn(
            "range_30d_percent",
            "30日振幅",
            "percent",
            priority="secondary",
            show_sign=False,
            description="最近30根闭合日线最高价相对最低价的区间宽度。",
        ),
        ViewColumn(
            "history_days",
            "日线覆盖",
            "number",
            priority="secondary",
            maximum_fraction_digits=0,
            description=(
                "本轮连续闭合UTC日线的实际覆盖根数，最多220根；"
                "价格区间指标最多使用其中最近180根。"
            ),
        ),
    )
    evaluation_source = EVALUATION_SOURCE
    baseline_evaluation_source = BASELINE_EVALUATION_SOURCE
    description = (
        "Binance USDⓈ-M 永续合约全市场初筛与候选详查；把闭合5m异动与信号当时"
        "已闭合日线价格位置结合，区分低位启动、冲高回落风险和下跌延续等状态。"
    )
    default_enabled = False
    evaluation_batch_limit = 36
    foreground_interval_seconds = 300.0

    def __init__(
        self,
        settings: BinanceAltcoinRadarSettings,
        *,
        client: AltcoinRadarProvider | None = None,
        daily_provider: DailySeriesProvider | None = None,
        evaluation_store: SQLiteMonitorStore | None = None,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.jitter_seconds = settings.jitter_seconds
        self.client = client or BinanceAltcoinRadarClient(
            timeout_seconds=settings.timeout_seconds,
            proxy_url=settings.proxy_url,
        )
        self.daily_provider = daily_provider or (
            BinanceUsdmDailyCache(settings.cache_root, self.client)
            if settings.cache_root is not None
            else None
        )
        self.evaluation_store = evaluation_store
        self.view = MonitorView(
            filters=(
                ViewFilter(
                    key="context_stage_group",
                    label="综合状态",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部候选"),
                        FilterChoice(
                            "EARLY_RISE",
                            "低位与突破",
                            "底部蓄势、低位启动、超跌反弹加速或前高突破测试。",
                        ),
                        FilterChoice(
                            "HIGH_RISK",
                            "高位回落风险",
                            "短线触发位于日线高位、快速拉升或上一轮峰值附近。",
                        ),
                        FilterChoice(
                            "DECLINING",
                            "下跌延续",
                            "短线触发没有改变日线空头排列或持续下跌。",
                        ),
                        FilterChoice(
                            "WATCHING",
                            "等待位置确认",
                            "短线与日线位置尚未形成已定义组合，本轮不输出方向。",
                        ),
                        FilterChoice("NEUTRAL", "尚未形成"),
                    ),
                ),
            ),
            columns=(
                ViewColumn("symbol", "币种"),
                ViewColumn(
                    "context_stage_label",
                    "综合状态",
                    description=(
                        "先按闭合5m成交、主动买入、突破、相对BTC、资金费率和OI"
                        "形成短线触发，再用信号当时最多180根已闭合UTC日线判断"
                        "其位于底部、高位、前高附近或持续下跌。只有规则明确的组合"
                        "才输出限定期限方向；其他组合显示等待位置确认。"
                    ),
                ),
                ViewColumn(
                    "price_state_label",
                    "价格位置",
                    description=(
                        "与“价格位置”标签同源、同轮且不晚于短线信号的日线状态；"
                        "连续日线不足90根时保持为空。"
                    ),
                ),
                ViewColumn(
                    "current_pump_multiple",
                    "拉升倍数",
                    "number",
                    minimum_fraction_digits=2,
                    maximum_fraction_digits=2,
                    description=(
                        "仅在信号时价格状态为暴涨或拉升中时计算：信号价 ÷ 拉升起点前"
                        "7根已闭合UTC日线收盘价均值。1.50表示信号价为基准价的1.5倍；"
                        "小于1表示短期虽触发暴涨，但信号价仍低于起点前均价；"
                        "起点日不进入均价。"
                    ),
                ),
                ViewColumn(
                    "pump_start_date",
                    "拉升起点",
                    description=(
                        "信号当时当前活动价格段的首个异动拉升UTC日。触发规则与价格位置"
                        "标签一致；已闭合日线从峰值回撤18%后重新识别一轮。"
                    ),
                ),
                ViewColumn(
                    "pump_baseline_price",
                    "拉升基准价",
                    "number",
                    maximum_fraction_digits=8,
                    use_grouping=True,
                    description=(
                        "拉升起点之前连续7根已闭合UTC日线收盘价的算术平均；"
                        "严格不包含拉升起点日。"
                    ),
                ),
                ViewColumn(
                    "context_stage_reason",
                    "融合依据",
                    description=(
                        "列出触发综合状态的短线阶段、日线状态及关键位置事实；"
                        "未满足组合规则时明确不输出方向。"
                    ),
                ),
                ViewColumn(
                    "evaluation_target_label",
                    "待检验方向",
                    description=(
                        "显示本状态将由后续闭合5m行情检验的方向与期限。"
                        "“不输出方向”表示当前只有筛选价值，不计入准确性统计。"
                    ),
                ),
                ViewColumn(
                    "alert_score",
                    "异动强度",
                    "number",
                    minimum_fraction_digits=1,
                    maximum_fraction_digits=1,
                    description=(
                        "0–100规则分：蓄势观察使用蓄势分，启动/加速使用启动分，"
                        "尾声风险/回落确认使用尾声风险分，尚未形成取三者最大值。"
                        "它是规则化异动强度，不是证据充分性或上涨概率。"
                    ),
                ),
                ViewColumn(
                    "return_15m_percent",
                    "15m涨跌",
                    "percent",
                    description="最新连续3根闭合5m K线的收盘价涨跌幅。",
                ),
                ViewColumn(
                    "quote_volume_ratio_15m",
                    "成交额放大",
                    "number",
                    minimum_fraction_digits=2,
                    maximum_fraction_digits=2,
                    description=(
                        "最近15m成交额 ÷ 前45m按15m折算的平均成交额。"
                        "1.00表示与前45m基准相同，2.00表示放大到2倍。"
                    ),
                ),
                ViewColumn(
                    "taker_buy_percent",
                    "主动买入",
                    "percent",
                    show_sign=False,
                    description="最近15m主动买入成交额占该15m总成交额的比例。",
                ),
                ViewColumn(
                    "tail_risk_score",
                    "尾声风险",
                    "number",
                    minimum_fraction_digits=1,
                    maximum_fraction_digits=1,
                    description=(
                        "0–100规则分，综合1h/24h涨幅、最近15m峰值回撤、"
                        "放量后主动买入转弱、最新5m下跌、资金费率和价格/OI同步转弱。"
                        "分值越高表示冲高后衰竭或回落证据越集中。"
                    ),
                ),
                ViewColumn(
                    "relative_return_15m_percent",
                    "相对 BTC",
                    "percent",
                    description="该币15m涨跌幅减去BTC同期15m涨跌幅。",
                ),
                ViewColumn(
                    "evidence_label",
                    "关键事实",
                    description=(
                        "按本轮各特征偏离程度排序后，显示贡献最显著的3项已观测市场事实。"
                        "它解释规则归因，不代表独立来源佐证、新闻事实或证据充分性分级。"
                    ),
                ),
                ViewColumn(
                    "oi_change_15m_percent",
                    "OI 15m",
                    "percent",
                    priority="secondary",
                    description=(
                        "该USDⓈ-M永续合约未平仓量最近15m变化率；"
                        "需要连续4个5m OI点，否则保持为空。"
                    ),
                ),
                ViewColumn(
                    "funding_rate_percent",
                    "资金费率",
                    "percent",
                    priority="secondary",
                    description=(
                        "该USDⓈ-M永续合约premiumIndex返回的最近资金费率；"
                        "来源未通过校验时保持为空。"
                    ),
                ),
                ViewColumn(
                    "quote_volume_24h",
                    "24h 成交额 (USDT)",
                    "number",
                    priority="secondary",
                    maximum_fraction_digits=0,
                    use_grouping=True,
                    description=(
                        "Binance USDⓈ-M永续合约滚动24h的USDT计价成交额，"
                        "用于流动性初筛。"
                    ),
                ),
                ViewColumn(
                    "stage_label",
                    "短线触发",
                    priority="secondary",
                    description=(
                        "只使用闭合5m行情、相对BTC、资金费率和OI得到的原始阶段；"
                        "综合状态会再结合日线价格位置。"
                    ),
                ),
                ViewColumn(
                    "review_window_label",
                    "复核节奏",
                    priority="secondary",
                    description=(
                        "按5m闭合K线更新频率给出的风险复核节奏；尾声风险需当前即复核。"
                        "这不是经过手续费、滑点和样本外回测验证的期望盈利持仓期。"
                    ),
                ),
                ViewColumn(
                    "valid_until",
                    "有效至",
                    "time",
                    priority="secondary",
                    description=(
                        "规则结论的时效截止：最新闭合5m K线结束时间加15分钟。"
                        "本轮全部候选一致时在表格上方统一显示；存在差异或缺失时保留逐行值。"
                    ),
                    promote_when_uniform=True,
                    uniform_summary_label="本轮结论有效至",
                ),
                ViewColumn(
                    "data_cutoff_at",
                    "市场截止",
                    "time",
                    priority="secondary",
                    description=(
                        "本行计算所使用的最新闭合5m K线结束时间，"
                        "不是页面刷新时间或网络响应时间。全部候选一致时在表格上方统一显示。"
                    ),
                    promote_when_uniform=True,
                    uniform_summary_label="本轮市场截止",
                ),
            ),
            table_title="全市场初筛后的异动候选",
            chart_title="异动强度历史（0–100）",
            method_note=(
                "分析周期：每根闭合5m K重算，短线主判看15m，趋势背景看1h，"
                "再用信号当时已闭合日线判断价格位于底部、高位、前高或下跌趋势。"
                "综合状态只在规则明确时给出15分钟、1小时或4小时待检验方向；"
                "其余状态明确不输出方向。"
                "“本轮结论有效至”是规则结论的硬截止；“复核节奏”是风险检查频率，"
                "不是期望盈利持仓期。当前尚无含手续费、滑点的样本外回测，"
                "因此不推断持仓时长。"
            ),
            show_description=False,
            summary_fields=(
                ViewSummaryField(
                    "coverage_label",
                    "本轮扫描",
                    (
                        "本轮参与初筛的USDT永续合约数、达到24h流动性门槛的数量、"
                        "实际完成5m K线分析的数量以及最终展示数量。该信息属于整轮采集，"
                        "不在每个合约行内重复。"
                    ),
                ),
            ),
            evaluation=EvaluationView(
                title="后续行情检验",
                method_note=(
                    "综合状态首次出现、发生变化或连续4小时未复建样本时，冻结当时"
                    "可见的短线触发、日线价格位置和信号截止价，并只在该状态指定的"
                    "15分钟、1小时或4小时时限后用闭合5m K线检验。"
                    "方向一致要求绝对方向正确且相对BTC超出±0.5个百分点；"
                    "该带宽是暂定噪声区间，不含手续费、滑点或可成交性。"
                    "页面用同一币种、同一信号截止和同一期限同步比较原短线规则；"
                    "只有价格位置改变原规则方向的样本才用于衡量增量，同方向样本只"
                    "用于检查筛选表现。每组需同时达到30个完成样本、20个独立信号"
                    "截止、15个币种和14天观测跨度，才报告一致率与均值。"
                ),
                minimum_group_samples=30,
                minimum_distinct_cutoffs=20,
                minimum_distinct_entities=15,
                minimum_observation_days=14.0,
            ),
        )

    def network_request_count(self, *, window_seconds: float = 60) -> int | None:
        counter = getattr(self.client, "network_request_count", None)
        if not callable(counter):
            return None
        return int(counter(window_seconds=window_seconds))

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        binder = getattr(self.client, "bind_stop_event", None)
        if callable(binder):
            binder(stop_event)

    def collect(self) -> CollectionBatch:
        try:
            self.client.ensure_available()
        except RadarSourceError as exc:
            return CollectionBatch(
                samples=(),
                issues=(CollectionIssue("monitor", exc.reason_code),),
            )

        issues: list[CollectionIssue] = []
        try:
            exchange = self.client.exchange_symbols()
            day = self.client.ticker_24h()
        except RadarSourceError as exc:
            return CollectionBatch(
                samples=(),
                issues=(CollectionIssue("universe", exc.reason_code),),
            )
        self._append_malformed_issue(issues, "exchange-info", exchange)
        self._append_malformed_issue(issues, "ticker-24h", day)

        symbols = {
            symbol: base
            for symbol, base in dict(exchange.value).items()
            if _eligible_altcoin(base)
        }
        liquid_day_values = {
            symbol: ticker
            for symbol, ticker in dict(day.value).items()
            if symbol in symbols
            and ticker.quote_volume >= float(self.settings.min_quote_volume_24h)
        }
        tickers_24h = self._fresh_tickers(
            liquid_day_values,
            observed_at=day.completed_at,
            scope="ticker-24h",
            issues=issues,
        )
        liquid_symbols = sorted(
            symbol
            for symbol in symbols
            if symbol in tickers_24h
            and tickers_24h[symbol].quote_volume
            >= float(self.settings.min_quote_volume_24h)
        )
        if not liquid_symbols:
            return CollectionBatch(
                samples=(),
                issues=tuple(
                    issues or [CollectionIssue("universe", "RADAR_NO_LIQUID_SYMBOLS")]
                ),
            )

        screening_seeds = select_candidate_seeds(
            symbols,
            tickers_24h,
            min_quote_volume_24h=float(self.settings.min_quote_volume_24h),
            maximum=self.settings.max_screened_contracts,
        )
        if not screening_seeds:
            return CollectionBatch(
                samples=(),
                issues=tuple(
                    issues or [CollectionIssue("universe", "RADAR_NO_CANDIDATES")]
                ),
            )

        benchmark_features: CandleFeatures | None = None
        try:
            benchmark = self.client.klines("BTCUSDT", limit=self.settings.kline_limit)
            benchmark_features = self._validated_features("BTCUSDT", benchmark)
            self._append_malformed_issue(issues, "BTCUSDT", benchmark)
        except RadarSourceError as exc:
            issues.append(CollectionIssue("BTCUSDT", exc.reason_code))

        funding_contexts: dict[str, FundingContext] = {}
        try:
            funding = self.client.futures_premium_index()
            funding_contexts = self._fresh_funding(
                dict(funding.value),
                observed_at=funding.completed_at,
                issues=issues,
            )
            self._append_malformed_issue(issues, "futures", funding)
        except RadarSourceError as exc:
            issues.append(CollectionIssue("futures", exc.reason_code))

        screened: list[CandidateEnrichment] = []
        with ThreadPoolExecutor(max_workers=self.settings.workers) as pool:
            futures = {
                pool.submit(
                    self._screen_candidate,
                    seed,
                    funding_contexts.get(seed.symbol),
                ): seed
                for seed in screening_seeds
            }
            for future in as_completed(futures):
                seed = futures[future]
                try:
                    screened.append(future.result())
                except CollectionCancelled:
                    raise
                except Exception:
                    screened.append(
                        CandidateEnrichment(
                            seed=seed,
                            features=None,
                            funding=funding_contexts.get(seed.symbol),
                            oi_change_15m_percent=None,
                            issues=(
                                CollectionIssue(
                                    seed.symbol,
                                    "RADAR_CANDIDATE_COLLECTION_FAILED",
                                ),
                            ),
                            missing_reason=(
                                "合约K线分析发生未分类失败；未生成阶段或评分，"
                                "也未使用替代值。"
                            ),
                        )
                    )

        for enrichment in screened:
            issues.extend(enrichment.issues)
        analyzed = [
            enrichment for enrichment in screened if enrichment.features is not None
        ]
        if not analyzed:
            return CollectionBatch(
                samples=(),
                issues=tuple(
                    issues or [CollectionIssue("universe", "RADAR_NO_CANDIDATES")]
                ),
            )
        analyzed.sort(
            key=lambda enrichment: self._candidate_sort_key(
                enrichment,
                benchmark_features=benchmark_features,
            )
        )
        price_position_rows: tuple[dict[str, Any], ...] = ()
        price_position_counts = {
            "eligible": len(analyzed),
            "included": 0,
            "history_insufficient": 0,
            "unavailable": len(analyzed),
        }
        price_positions_by_symbol: dict[str, PricePositionFeatures] = {}
        if self.daily_provider is not None:
            (
                price_position_rows,
                price_position_issues,
                price_position_counts,
                price_positions_by_symbol,
            ) = self._collect_price_positions(
                analyzed,
                daily_cutoff=latest_closed_daily_cutoff(day.completed_at),
            )
            issues.extend(price_position_issues)
        shortlist = analyzed[: self.settings.max_candidates]

        enrichments: list[CandidateEnrichment] = []
        with ThreadPoolExecutor(max_workers=self.settings.workers) as pool:
            futures = {
                pool.submit(self._enrich_open_interest, enrichment): enrichment
                for enrichment in shortlist
            }
            for future in as_completed(futures):
                enrichment = futures[future]
                try:
                    enrichments.append(future.result())
                except CollectionCancelled:
                    raise
                except Exception:
                    enrichments.append(
                        CandidateEnrichment(
                            seed=enrichment.seed,
                            features=enrichment.features,
                            funding=enrichment.funding,
                            oi_change_15m_percent=None,
                            issues=(
                                CollectionIssue(
                                    enrichment.seed.symbol,
                                    "RADAR_OI_COLLECTION_FAILED",
                                ),
                            ),
                        )
                    )

        observed_at = utc_now()
        samples: list[MetricSample] = []
        for enrichment in enrichments:
            issues.extend(enrichment.issues)
            samples.append(
                self._sample(
                    enrichment,
                    benchmark_features=benchmark_features,
                    observed_at=observed_at,
                    universe_size=len(symbols),
                    liquid_size=len(liquid_symbols),
                    screened_size=len(screening_seeds),
                    analyzed_size=len(analyzed),
                    shortlist_size=len(shortlist),
                    price_position=price_positions_by_symbol.get(
                        enrichment.seed.symbol
                    ),
                )
            )

        samples.sort(
            key=lambda sample: (
                int(sample.payload.get("context_stage_rank") or 999),
                sample.payload.get("alert_score") is None,
                -float(sample.payload.get("alert_score") or 0),
                sample.entity_key,
            )
        )
        if any(sample.value_text for sample in samples) and not any(
            issue.reason_code in THROTTLE_REASON_CODES for issue in issues
        ):
            self.client.reset_throttle_backoff()
        evaluation_cases = self._new_evaluation_cases(tuple(samples))
        projection_snapshots: tuple[ProjectionSnapshot, ...] = ()
        if price_position_rows:
            projection_snapshots = (
                self._price_position_snapshot(
                    price_position_rows,
                    counts=price_position_counts,
                    observed_at=observed_at,
                ),
            )
        return CollectionBatch(
            samples=tuple(samples),
            issues=tuple(issues),
            evaluation_cases=evaluation_cases,
            projection_snapshots=projection_snapshots,
        )

    def _collect_price_positions(
        self,
        analyzed: Sequence[CandidateEnrichment],
        *,
        daily_cutoff: datetime,
    ) -> tuple[
        tuple[dict[str, Any], ...],
        tuple[CollectionIssue, ...],
        dict[str, int],
        dict[str, PricePositionFeatures],
    ]:
        provider = self.daily_provider
        if provider is None:
            return (
                (),
                (),
                {
                    "eligible": len(analyzed),
                    "included": 0,
                    "history_insufficient": 0,
                    "unavailable": len(analyzed),
                },
                {},
            )
        issues: list[CollectionIssue] = []
        results: list[tuple[CandidateEnrichment, DailySeriesResult]] = []
        unavailable = 0
        with ThreadPoolExecutor(max_workers=self.settings.workers) as pool:
            futures = {
                pool.submit(provider.fetch, item.seed.symbol, daily_cutoff): item
                for item in analyzed
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                except CollectionCancelled:
                    raise
                except Exception:
                    unavailable += 1
                    issues.append(
                        CollectionIssue(
                            item.seed.symbol,
                            "RADAR_DAILY_COLLECTION_FAILED",
                        )
                    )
                    continue
                if not result.current or result.latest_close_at is None:
                    unavailable += 1
                    issues.append(
                        CollectionIssue(
                            item.seed.symbol,
                            result.reason_code or "RADAR_DAILY_KLINES_UNAVAILABLE",
                        )
                    )
                    continue
                if abs((result.latest_close_at - daily_cutoff).total_seconds()) > 5:
                    unavailable += 1
                    issues.append(
                        CollectionIssue(
                            item.seed.symbol,
                            "RADAR_DAILY_KLINES_STALE",
                        )
                    )
                    continue
                if result.reason_code is not None:
                    issues.append(CollectionIssue(item.seed.symbol, result.reason_code))
                results.append((item, result))

        rows: list[dict[str, Any]] = []
        features_by_symbol: dict[str, PricePositionFeatures] = {}
        history_insufficient = 0
        for item, result in results:
            try:
                current_features = analyze_price_position(
                    result.candles,
                    current_price=item.seed.ticker_24h.last_price,
                    return_24h_percent=item.seed.ticker_24h.price_change_percent,
                )
                if item.features is None:
                    raise RadarSourceError("RADAR_FEATURES_REQUIRED")
                signal_return_24h = _percent_change(
                    item.features.close_price,
                    item.seed.ticker_24h.open_price,
                )
                if signal_return_24h is None:
                    raise RadarSourceError("RADAR_DAILY_COMPUTATION_INVALID")
                signal_features = analyze_price_position(
                    result.candles,
                    current_price=item.features.close_price,
                    return_24h_percent=signal_return_24h,
                )
            except RadarSourceError as exc:
                if exc.reason_code == "RADAR_DAILY_HISTORY_INSUFFICIENT":
                    history_insufficient += 1
                else:
                    unavailable += 1
                    issues.append(CollectionIssue(item.seed.symbol, exc.reason_code))
                continue
            features_by_symbol[item.seed.symbol] = signal_features
            rows.append(self._price_position_row(item, current_features))
        rows.sort(
            key=lambda row: (
                int(row["price_state_rank"]),
                -abs(float(row["return_24h_percent"])),
                str(row["symbol"]),
            )
        )
        counts = {
            "eligible": len(analyzed),
            "included": len(rows),
            "history_insufficient": history_insufficient,
            "unavailable": unavailable,
        }
        return tuple(rows), tuple(issues), counts, features_by_symbol

    @staticmethod
    def _price_position_row(
        item: CandidateEnrichment,
        features: PricePositionFeatures,
    ) -> dict[str, Any]:
        peak_missing = (
            "最近最多180根闭合日线中未识别到先涨至少30%、"
            "随后回撤至少18%的完整拉升回落段。"
        )
        missing_reasons: dict[str, str] = {}
        if features.previous_pump_peak_at is None:
            missing_reasons.update(
                {
                    "distance_from_previous_pump_peak_percent": peak_missing,
                    "previous_pump_peak_date": peak_missing,
                    "previous_pump_peak_price": peak_missing,
                }
            )
        if features.current_pump_multiple is None:
            if features.state in {"SURGE", "PUMPING"}:
                pump_missing = (
                    "当前价格属于暴涨或拉升中，但未能取得起点前连续7根闭合UTC日线；"
                    "拉升倍数、起点和基准价保持为空。"
                )
            else:
                pump_missing = (
                    "当前价格状态不是暴涨或拉升中；拉升倍数、起点和基准价不适用。"
                )
            missing_reasons.update(
                {
                    "current_pump_multiple": pump_missing,
                    "pump_start_date": pump_missing,
                    "pump_baseline_price": pump_missing,
                }
            )
        return {
            "symbol": item.seed.symbol,
            "entity_key": item.seed.symbol,
            "series_key": (f"{item.seed.symbol}|usdm-perpetual-price-position"),
            "market_scope": MARKET_SCOPE,
            "price_state": features.state,
            "price_state_label": features.state_label,
            "price_state_group": features.state_group,
            "price_state_rank": features.state_rank,
            "state_reason": features.state_reason,
            "row_tone": features.row_tone,
            "current_price": _float_text(features.current_price, 12),
            "current_price_at": iso_utc(item.seed.ticker_24h.close_time),
            "current_pump_multiple": _float_text(
                features.current_pump_multiple,
                6,
            ),
            "pump_start_date": (
                features.pump_start_at.date().isoformat()
                if features.pump_start_at is not None
                else None
            ),
            "pump_baseline_price": _float_text(
                features.pump_baseline_price,
                12,
            ),
            "pump_start_trigger": features.pump_start_trigger,
            "return_24h_percent": _float_text(features.return_24h_percent),
            "return_3d_percent": _float_text(features.return_3d_percent),
            "return_7d_percent": _float_text(features.return_7d_percent),
            "return_14d_percent": _float_text(features.return_14d_percent),
            "return_30d_percent": _float_text(features.return_30d_percent),
            "return_90d_percent": _float_text(features.return_90d_percent),
            "position_90d_percent": _float_text(features.position_90d_percent),
            "distance_from_range_high_percent": _float_text(
                features.distance_from_range_high_percent
            ),
            "distance_from_range_low_percent": _float_text(
                features.distance_from_range_low_percent
            ),
            "distance_from_previous_pump_peak_percent": _float_text(
                features.distance_from_previous_pump_peak_percent
            ),
            "previous_pump_peak_date": (
                features.previous_pump_peak_at.date().isoformat()
                if features.previous_pump_peak_at is not None
                else None
            ),
            "previous_pump_peak_price": _float_text(
                features.previous_pump_peak_price,
                12,
            ),
            "trend_structure": features.trend_structure,
            "ma20_gap_percent": _float_text(features.ma20_gap_percent),
            "ma60_gap_percent": _float_text(features.ma60_gap_percent),
            "range_30d_percent": _float_text(features.range_30d_percent),
            "volatility_compression_ratio": _float_text(
                features.volatility_compression_ratio
            ),
            "history_days": features.history_days,
            "range_days": features.range_days,
            "daily_cutoff_at": iso_utc(features.daily_cutoff_at),
            "missing_reasons": missing_reasons,
        }

    @staticmethod
    def _price_position_snapshot(
        rows: tuple[dict[str, Any], ...],
        *,
        counts: dict[str, int],
        observed_at: datetime,
    ) -> ProjectionSnapshot:
        price_cutoff_at = min(
            datetime.fromisoformat(
                str(row["current_price_at"]).replace("Z", "+00:00")
            ).astimezone(UTC)
            for row in rows
        )
        daily_cutoff_at = min(
            datetime.fromisoformat(
                str(row["daily_cutoff_at"]).replace("Z", "+00:00")
            ).astimezone(UTC)
            for row in rows
        )
        coverage_parts = [
            f"{counts['eligible']} 个合约完成短周期分析",
            f"{counts['included']} 个具备连续90日日线并形成价格位置",
        ]
        if counts["history_insufficient"]:
            coverage_parts.append(
                f"{counts['history_insufficient']} 个上市历史不足90日"
            )
        if counts["unavailable"]:
            coverage_parts.append(f"{counts['unavailable']} 个本轮日线缺失或不连续")
        state_counts: dict[str, int] = {}
        for row in rows:
            state = str(row["price_state"])
            state_counts[state] = state_counts.get(state, 0) + 1
        return ProjectionSnapshot(
            snapshot_key=PRICE_POSITION_SNAPSHOT_KEY,
            observed_at=observed_at,
            cutoff_at=price_cutoff_at,
            payload={
                "schema_version": 1,
                "market_scope": MARKET_SCOPE,
                "source": "BINANCE_USDM_PUBLIC_CURRENT_AND_CLOSED_1D_KLINES",
                "price_cutoff_at": iso_utc(price_cutoff_at),
                "daily_cutoff_at": iso_utc(daily_cutoff_at),
                "valid_until": iso_utc(price_cutoff_at + timedelta(minutes=15)),
                "coverage_label": "；".join(coverage_parts) + "。",
                "counts": dict(counts),
                "state_counts": state_counts,
                "rows": list(rows),
            },
        )

    def _new_evaluation_cases(
        self,
        samples: tuple[MetricSample, ...],
    ) -> tuple[ForwardEvaluationCase, ...]:
        """Freeze baseline and price-aware claims before outcomes are observable."""

        if self.evaluation_store is None:
            return ()
        candidates = tuple(
            sample
            for sample in samples
            if str(sample.payload.get("stage")) in EVALUATION_DIRECTIONS
            and sample.payload.get("close_price")
            and sample.payload.get("benchmark_close_price")
            and sample.payload.get("data_cutoff_at")
        )
        if not candidates:
            return ()
        entity_keys = tuple(dict.fromkeys(sample.entity_key for sample in candidates))
        previous_samples = {
            sample.entity_key: sample
            for sample in self.evaluation_store.latest_samples_by_entity(
                self.monitor_id,
                entity_keys,
            )
        }
        previous_baseline_evaluations = {
            evaluation.entity_key: evaluation
            for evaluation in self.evaluation_store.latest_forward_evaluations_by_entity(
                self.monitor_id,
                entity_keys,
                source=self.baseline_evaluation_source,
            )
        }
        previous_context_evaluations = {
            evaluation.entity_key: evaluation
            for evaluation in self.evaluation_store.latest_forward_evaluations_by_entity(
                self.monitor_id,
                entity_keys,
                source=self.evaluation_source,
            )
        }
        cases: list[ForwardEvaluationCase] = []
        for sample in candidates:
            cutoff = datetime.fromisoformat(
                str(sample.payload["data_cutoff_at"]).replace("Z", "+00:00")
            ).astimezone(UTC)
            previous_sample = previous_samples.get(sample.entity_key)
            previous_short_stage = (
                str(previous_sample.payload.get("stage"))
                if previous_sample is not None
                and previous_sample.payload.get("market_scope") == MARKET_SCOPE
                else None
            )
            previous_context_stage = (
                str(previous_sample.payload.get("context_stage"))
                if previous_sample is not None
                and previous_sample.payload.get("market_scope") == MARKET_SCOPE
                and previous_sample.payload.get("context_stage")
                else None
            )

            short_stage = str(sample.payload["stage"])
            context_stage = str(sample.payload.get("context_stage") or "")
            context_direction = str(sample.payload.get("context_direction") or "")
            context_horizons = tuple(
                int(value)
                for value in sample.payload.get(
                    "context_evaluation_horizons_minutes",
                    (),
                )
                if int(value) in EVALUATION_HORIZONS_MINUTES
            )
            previous_context = previous_context_evaluations.get(sample.entity_key)
            context_repeated_after_gap = (
                previous_context is None
                or cutoff - previous_context.source_cutoff_at >= EVALUATION_REPEAT_AFTER
            )
            create_context_cases = (
                context_direction in {"UP", "DOWN"}
                and bool(context_horizons)
                and (
                    previous_context_stage != context_stage
                    or context_repeated_after_gap
                )
            )

            previous_baseline = previous_baseline_evaluations.get(sample.entity_key)
            baseline_repeated_after_gap = (
                previous_baseline is None
                or cutoff - previous_baseline.source_cutoff_at
                >= EVALUATION_REPEAT_AFTER
            )
            create_baseline_episode = (
                previous_short_stage != short_stage or baseline_repeated_after_gap
            )
            baseline_horizons = (
                EVALUATION_HORIZONS_MINUTES
                if create_baseline_episode
                else context_horizons
                if create_context_cases
                else ()
            )
            if baseline_horizons:
                baseline_direction = EVALUATION_DIRECTIONS[short_stage]
                for horizon_minutes in baseline_horizons:
                    cases.append(
                        ForwardEvaluationCase(
                            case_key=(
                                f"{sample.entity_key}|{short_stage}|"
                                f"{iso_utc(cutoff)}|{horizon_minutes}"
                            ),
                            entity_key=sample.entity_key,
                            stage=short_stage,
                            stage_label=str(sample.payload["stage_label"]),
                            direction=baseline_direction,  # type: ignore[arg-type]
                            signal_observed_at=sample.observed_at,
                            source_cutoff_at=cutoff,
                            horizon_minutes=horizon_minutes,
                            due_at=cutoff + timedelta(minutes=horizon_minutes),
                            entry_price_text=str(sample.payload["close_price"]),
                            benchmark_entry_price_text=str(
                                sample.payload["benchmark_close_price"]
                            ),
                            source=self.baseline_evaluation_source,
                        )
                    )

            if not create_context_cases:
                continue
            for horizon_minutes in context_horizons:
                cases.append(
                    ForwardEvaluationCase(
                        case_key=(
                            f"{PRICE_CONTEXT_MODEL_VERSION}|{sample.entity_key}|"
                            f"{context_stage}|{iso_utc(cutoff)}|{horizon_minutes}"
                        ),
                        entity_key=sample.entity_key,
                        stage=context_stage,
                        stage_label=str(sample.payload["context_stage_label"]),
                        direction=context_direction,  # type: ignore[arg-type]
                        signal_observed_at=sample.observed_at,
                        source_cutoff_at=cutoff,
                        horizon_minutes=horizon_minutes,
                        due_at=cutoff + timedelta(minutes=horizon_minutes),
                        entry_price_text=str(sample.payload["close_price"]),
                        benchmark_entry_price_text=str(
                            sample.payload["benchmark_close_price"]
                        ),
                        source=self.evaluation_source,
                    )
                )
        return tuple(cases)

    def evaluate(
        self,
        cases: Sequence[ForwardEvaluationCase],
        *,
        now: datetime,
    ) -> tuple[ForwardEvaluationResult, ...]:
        """Resolve due cases from exact closed 5m windows, with no interpolation."""

        pending = tuple(cases)
        if not pending:
            return ()
        supported_sources = {
            self.evaluation_source,
            self.baseline_evaluation_source,
        }
        incompatible = tuple(case for case in pending if case.source not in supported_sources)
        pending = tuple(
            case for case in pending if case.source in supported_sources
        )
        resolved: list[ForwardEvaluationResult] = [
            self._unavailable_evaluation(
                case,
                now=now,
                reason_code="RADAR_EVALUATION_MARKET_SCOPE_CHANGED",
            )
            for case in incompatible
        ]
        if not pending:
            return tuple(resolved)
        earliest = min(case.source_cutoff_at for case in pending)
        latest = max(case.due_at for case in pending)
        try:
            benchmark_result = self.client.klines_between(
                "BTCUSDT",
                start_at=earliest,
                end_at=latest,
                limit=self._evaluation_range_limit(earliest, latest),
            )
            benchmark_candles = tuple(benchmark_result.value)
        except (RadarSourceError, ValueError) as exc:
            reason = (
                exc.reason_code
                if isinstance(exc, RadarSourceError)
                else "RADAR_EVALUATION_RANGE_INVALID"
            )
            resolved.extend(
                self._unavailable_evaluation(case, now=now, reason_code=reason)
                for case in pending
                if now >= case.due_at + EVALUATION_GRACE
            )
            return tuple(resolved)

        by_entity: dict[str, list[ForwardEvaluationCase]] = {}
        for case in pending:
            by_entity.setdefault(case.entity_key, []).append(case)
        for entity_key, entity_cases in by_entity.items():
            entity_start = min(case.source_cutoff_at for case in entity_cases)
            entity_end = max(case.due_at for case in entity_cases)
            try:
                asset_result = self.client.klines_between(
                    entity_key,
                    start_at=entity_start,
                    end_at=entity_end,
                    limit=self._evaluation_range_limit(entity_start, entity_end),
                )
                asset_candles = tuple(asset_result.value)
            except (RadarSourceError, ValueError) as exc:
                reason = (
                    exc.reason_code
                    if isinstance(exc, RadarSourceError)
                    else "RADAR_EVALUATION_RANGE_INVALID"
                )
                resolved.extend(
                    self._unavailable_evaluation(
                        case,
                        now=now,
                        reason_code=reason,
                    )
                    for case in entity_cases
                    if now >= case.due_at + EVALUATION_GRACE
                )
                continue
            for case in entity_cases:
                result = self._resolve_evaluation(
                    case,
                    asset_candles=asset_candles,
                    benchmark_candles=benchmark_candles,
                    now=now,
                )
                if result is not None:
                    resolved.append(result)
        return tuple(resolved)

    @staticmethod
    def _evaluation_range_limit(start_at: datetime, end_at: datetime) -> int:
        candle_count = math.ceil(
            (end_at - start_at).total_seconds() / (KLINE_INTERVAL_MINUTES * 60)
        )
        return min(1000, max(1, candle_count + 2))

    @staticmethod
    def _evaluation_window(
        case: ForwardEvaluationCase,
        candles: Sequence[ContractCandle],
    ) -> tuple[ContractCandle, ...] | None:
        expected = case.horizon_minutes // KLINE_INTERVAL_MINUTES
        selected = tuple(
            candle
            for candle in candles
            if candle.close_time > case.source_cutoff_at
            and candle.close_time <= case.due_at + timedelta(seconds=5)
        )
        if len(selected) != expected:
            return None
        if abs((selected[0].open_time - case.source_cutoff_at).total_seconds()) > 5:
            return None
        if abs((selected[-1].close_time - case.due_at).total_seconds()) > 5:
            return None
        if any(
            abs(
                (current.open_time - previous.open_time).total_seconds()
                - KLINE_INTERVAL_MINUTES * 60
            )
            > 5
            for previous, current in zip(selected, selected[1:])
        ):
            return None
        return selected

    def _resolve_evaluation(
        self,
        case: ForwardEvaluationCase,
        *,
        asset_candles: Sequence[ContractCandle],
        benchmark_candles: Sequence[ContractCandle],
        now: datetime,
    ) -> ForwardEvaluationResult | None:
        asset_window = self._evaluation_window(case, asset_candles)
        benchmark_window = self._evaluation_window(case, benchmark_candles)
        if asset_window is None or benchmark_window is None:
            if now < case.due_at + EVALUATION_GRACE:
                return None
            return self._unavailable_evaluation(
                case,
                now=now,
                reason_code="RADAR_EVALUATION_KLINES_UNAVAILABLE",
            )
        try:
            entry_price = float(
                _decimal(
                    case.entry_price_text,
                    field="evaluation.entryPrice",
                    positive=True,
                )
            )
            benchmark_entry = float(
                _decimal(
                    case.benchmark_entry_price_text,
                    field="evaluation.benchmarkEntryPrice",
                    positive=True,
                )
            )
        except ValueError:
            return self._unavailable_evaluation(
                case,
                now=now,
                reason_code="RADAR_EVALUATION_ENTRY_INVALID",
            )
        exit_price = asset_window[-1].close_price
        benchmark_exit = benchmark_window[-1].close_price
        forward_return = (exit_price / entry_price - 1.0) * 100.0
        benchmark_return = (benchmark_exit / benchmark_entry - 1.0) * 100.0
        relative_return = forward_return - benchmark_return
        highest_return = (
            max(candle.high_price for candle in asset_window) / entry_price - 1.0
        ) * 100.0
        lowest_return = (
            min(candle.low_price for candle in asset_window) / entry_price - 1.0
        ) * 100.0
        if case.direction == "UP":
            favorable = highest_return
            adverse = lowest_return
            aligned = (
                forward_return > 0
                and relative_return >= EVALUATION_RELATIVE_BAND_PERCENT
            )
            opposed = (
                forward_return < 0
                and relative_return <= -EVALUATION_RELATIVE_BAND_PERCENT
            )
        else:
            favorable = -lowest_return
            adverse = -highest_return
            aligned = (
                forward_return < 0
                and relative_return <= -EVALUATION_RELATIVE_BAND_PERCENT
            )
            opposed = (
                forward_return > 0
                and relative_return >= EVALUATION_RELATIVE_BAND_PERCENT
            )
        verdict = "ALIGNED" if aligned else "OPPOSED" if opposed else "INCONCLUSIVE"
        return ForwardEvaluationResult(
            case_key=case.case_key,
            status="COMPLETE",
            evaluated_at=now,
            outcome_cutoff_at=min(
                asset_window[-1].close_time,
                benchmark_window[-1].close_time,
            ),
            exit_price_text=f"{exit_price:.12f}",
            benchmark_exit_price_text=f"{benchmark_exit:.12f}",
            forward_return_percent=forward_return,
            benchmark_return_percent=benchmark_return,
            relative_return_percent=relative_return,
            maximum_favorable_excursion_percent=favorable,
            maximum_adverse_excursion_percent=adverse,
            verdict=verdict,  # type: ignore[arg-type]
        )

    @staticmethod
    def _unavailable_evaluation(
        case: ForwardEvaluationCase,
        *,
        now: datetime,
        reason_code: str,
    ) -> ForwardEvaluationResult:
        return ForwardEvaluationResult(
            case_key=case.case_key,
            status="UNAVAILABLE",
            evaluated_at=now,
            outcome_cutoff_at=None,
            exit_price_text=None,
            benchmark_exit_price_text=None,
            forward_return_percent=None,
            benchmark_return_percent=None,
            relative_return_percent=None,
            maximum_favorable_excursion_percent=None,
            maximum_adverse_excursion_percent=None,
            verdict="UNAVAILABLE",
            reason_code=reason_code,
        )

    def _fresh_tickers(
        self,
        values: dict[str, RollingTicker],
        *,
        observed_at: datetime,
        scope: str,
        issues: list[CollectionIssue],
    ) -> dict[str, RollingTicker]:
        fresh: dict[str, RollingTicker] = {}
        stale = 0
        for symbol, ticker in values.items():
            age = (observed_at - ticker.close_time).total_seconds()
            if -120 <= age <= self.settings.ticker_stale_seconds:
                fresh[symbol] = ticker
            else:
                stale += 1
        if stale:
            issues.append(CollectionIssue(scope, "RADAR_TICKER_ROWS_STALE"))
        return fresh

    def _fresh_funding(
        self,
        values: dict[str, FundingContext],
        *,
        observed_at: datetime,
        issues: list[CollectionIssue],
    ) -> dict[str, FundingContext]:
        fresh: dict[str, FundingContext] = {}
        stale = 0
        for symbol, context in values.items():
            age = (observed_at - context.source_time).total_seconds()
            if -120 <= age <= self.settings.futures_stale_seconds:
                fresh[symbol] = context
            else:
                stale += 1
        if stale:
            issues.append(CollectionIssue("futures", "RADAR_FUTURES_ROWS_STALE"))
        return fresh

    @staticmethod
    def _append_malformed_issue(
        issues: list[CollectionIssue], scope: str, result: TimedValue
    ) -> None:
        if result.malformed_count:
            issues.append(CollectionIssue(scope, "RADAR_SOURCE_ROWS_MALFORMED"))

    def _validated_features(self, symbol: str, result: TimedValue) -> CandleFeatures:
        features = analyze_candles(tuple(result.value))
        age = (result.completed_at - features.cutoff_at).total_seconds()
        if age < -120 or age > self.settings.kline_stale_seconds:
            raise RadarSourceError("RADAR_KLINES_STALE")
        return features

    def _screen_candidate(
        self,
        seed: CandidateSeed,
        funding: FundingContext | None,
    ) -> CandidateEnrichment:
        issues: list[CollectionIssue] = []
        try:
            candles = self.client.klines(seed.symbol, limit=self.settings.kline_limit)
            features = self._validated_features(seed.symbol, candles)
            self._append_malformed_issue(issues, seed.symbol, candles)
        except RadarSourceError as exc:
            return CandidateEnrichment(
                seed=seed,
                features=None,
                funding=funding,
                oi_change_15m_percent=None,
                issues=(CollectionIssue(seed.symbol, exc.reason_code),),
                missing_reason=(
                    "该合约没有连续、新鲜且通过校验的5m闭合K线；"
                    "未生成阶段或评分，也未使用旧值或替代值。"
                ),
            )
        return CandidateEnrichment(
            seed=seed,
            features=features,
            funding=funding,
            oi_change_15m_percent=None,
            issues=tuple(issues),
        )

    def _candidate_sort_key(
        self,
        enrichment: CandidateEnrichment,
        *,
        benchmark_features: CandleFeatures | None,
    ) -> tuple[float, float, float, str]:
        features = enrichment.features
        if features is None:
            return (math.inf, math.inf, math.inf, enrichment.seed.symbol)
        benchmark_return = (
            benchmark_features.return_15m_percent
            if benchmark_features is not None
            else None
        )
        relative_return = (
            features.return_15m_percent - benchmark_return
            if benchmark_return is not None
            else None
        )
        funding_rate = (
            enrichment.funding.funding_rate_percent
            if enrichment.funding is not None
            else None
        )
        scoring = score_candidate(
            features,
            return_24h_percent=enrichment.seed.ticker_24h.price_change_percent,
            relative_return_15m_percent=relative_return,
            funding_rate_percent=funding_rate,
            oi_change_15m_percent=None,
        )
        return (
            -float(scoring["alert_score"]),
            -abs(features.return_15m_percent),
            -enrichment.seed.ticker_24h.quote_volume,
            enrichment.seed.symbol,
        )

    def _enrich_open_interest(
        self,
        enrichment: CandidateEnrichment,
    ) -> CandidateEnrichment:
        issues: list[CollectionIssue] = []
        oi_change: float | None = None
        try:
            oi = self.client.open_interest_history(
                enrichment.seed.symbol,
                limit=6,
            )
            points = tuple(oi.value)
            if not points:
                raise RadarSourceError("RADAR_OI_EMPTY")
            age = (oi.completed_at - points[-1].source_time).total_seconds()
            if age < -120 or age > self.settings.futures_stale_seconds:
                raise RadarSourceError("RADAR_OI_STALE")
            oi_change = open_interest_change_15m(points)
            if oi_change is None:
                raise RadarSourceError("RADAR_OI_INSUFFICIENT")
            self._append_malformed_issue(issues, enrichment.seed.symbol, oi)
        except RadarSourceError as exc:
            issues.append(CollectionIssue(enrichment.seed.symbol, exc.reason_code))
        return CandidateEnrichment(
            seed=enrichment.seed,
            features=enrichment.features,
            funding=enrichment.funding,
            oi_change_15m_percent=oi_change,
            issues=tuple(issues),
        )

    @staticmethod
    def _base_payload(
        enrichment: CandidateEnrichment,
        *,
        observed_at: datetime,
        universe_size: int,
        liquid_size: int,
        screened_size: int,
        analyzed_size: int,
        shortlist_size: int,
    ) -> dict[str, Any]:
        seed = enrichment.seed
        return {
            "symbol": seed.symbol,
            "base_asset": seed.base_asset,
            "series_label": f"{seed.symbol} · 异动强度",
            "observed_at": iso_utc(observed_at),
            "ticker_24h_close_at": iso_utc(seed.ticker_24h.close_time),
            "ticker_1h_close_at": (
                iso_utc(enrichment.features.cutoff_at)
                if enrichment.features is not None
                else None
            ),
            "return_1h_percent": (
                _float_text(enrichment.features.return_1h_percent)
                if enrichment.features is not None
                else None
            ),
            "return_24h_percent": _float_text(seed.ticker_24h.price_change_percent),
            "quote_volume_24h": _float_text(seed.ticker_24h.quote_volume, 2),
            "universe_size": universe_size,
            "liquid_universe_size": liquid_size,
            "screened_contract_size": screened_size,
            "analyzed_contract_size": analyzed_size,
            "shortlist_size": shortlist_size,
            "market_scope": MARKET_SCOPE,
            "data_scope_label": "USDⓈ-M 永续合约",
            "coverage_label": (
                f"全市场 {universe_size} 个 USDT 永续合约初筛"
                "（已排除 BTC、稳定币、指数及内置名单）；"
                f"{liquid_size} 个达到流动性阈值；"
                f"{screened_size} 个读取5m K线，{analyzed_size} 个完成分析；"
                f"展示 {shortlist_size} 个。"
                + (
                    " K线分析按24h异动与流动性排序达到本轮上限。"
                    if screened_size < liquid_size
                    else ""
                )
            ),
            "source": "BINANCE_USDM_PUBLIC_MARKET_DATA",
            "interpretation_limit": (
                "阶段与 0–100 分值是规则化异常证据，不是上涨概率、"
                "拉盘主体定性、买卖建议或自动交易输入。"
            ),
            "onchain_state": "NOT_INCLUDED_IN_MARKET_SCORE",
            "onchain_note": (
                "首版未把跨链、跨代币口径不一致的链上数据混入评分；"
                "需按指定链和合约地址另行建立来源与时间语义。"
            ),
        }

    def _sample(
        self,
        enrichment: CandidateEnrichment,
        *,
        benchmark_features: CandleFeatures | None,
        observed_at: datetime,
        universe_size: int,
        liquid_size: int,
        screened_size: int,
        analyzed_size: int,
        shortlist_size: int,
        price_position: PricePositionFeatures | None,
    ) -> MetricSample:
        features = enrichment.features
        if features is None:
            raise ValueError("RADAR_FEATURES_REQUIRED")
        benchmark_return = (
            benchmark_features.return_15m_percent
            if benchmark_features is not None
            else None
        )
        relative_return = (
            features.return_15m_percent - benchmark_return
            if benchmark_return is not None
            else None
        )
        funding_rate = (
            enrichment.funding.funding_rate_percent
            if enrichment.funding is not None
            else None
        )
        scoring = score_candidate(
            features,
            return_24h_percent=enrichment.seed.ticker_24h.price_change_percent,
            relative_return_15m_percent=relative_return,
            funding_rate_percent=funding_rate,
            oi_change_15m_percent=enrichment.oi_change_15m_percent,
        )
        stage = str(scoring["stage"])
        contextual_stage = classify_contextual_stage(stage, price_position)
        missing_reasons: dict[str, str] = {}
        if relative_return is None:
            missing_reasons["relative_return_15m_percent"] = (
                "未取得 BTC 同期通过校验的闭合 K 线；相对表现保持为空，未使用替代值。"
            )
            missing_reasons["benchmark_close_price"] = (
                "未取得BTC同期通过校验的闭合K线；本轮不建立后续行情检验样本。"
            )
        if funding_rate is None:
            missing_reasons["funding_rate_percent"] = (
                "本轮该USDⓈ-M永续合约的资金费率来源未返回通过校验的结果。"
            )
        if enrichment.oi_change_15m_percent is None:
            missing_reasons["oi_change_15m_percent"] = (
                "该币种没有连续、新鲜的 5m OI 历史；未使用当前 OI 代替变化率。"
            )
        if price_position is None:
            price_context_missing = (
                "本轮未取得连续90根已闭合日线；价格位置保持为空，"
                "综合状态只保留短线触发且不输出方向。"
            )
            missing_reasons["price_state_label"] = price_context_missing
            missing_reasons["price_position_90d_percent"] = price_context_missing
        if price_position is None or price_position.current_pump_multiple is None:
            if price_position is not None and price_position.state not in {
                "SURGE",
                "PUMPING",
            }:
                pump_context_missing = (
                    "信号时价格状态不是暴涨或拉升中；拉升倍数、起点和基准价不适用。"
                )
            else:
                pump_context_missing = (
                    "信号时未取得可复核的拉升起点及起点前7根闭合UTC日线；"
                    "相关字段保持为空。"
                )
            missing_reasons.update(
                {
                    "current_pump_multiple": pump_context_missing,
                    "pump_start_date": pump_context_missing,
                    "pump_baseline_price": pump_context_missing,
                }
            )

        alert_score = float(scoring["alert_score"])
        payload = {
            **self._base_payload(
                enrichment,
                observed_at=observed_at,
                universe_size=universe_size,
                liquid_size=liquid_size,
                screened_size=screened_size,
                analyzed_size=analyzed_size,
                shortlist_size=shortlist_size,
            ),
            "stage": stage,
            "stage_label": STAGE_LABELS[stage],
            "context_stage": contextual_stage.stage,
            "context_stage_label": contextual_stage.label,
            "context_stage_group": contextual_stage.group,
            "context_stage_group_label": CONTEXT_STAGE_GROUP_LABELS[
                contextual_stage.group
            ],
            "context_stage_rank": contextual_stage.rank,
            "context_stage_reason": contextual_stage.reason,
            "context_direction": contextual_stage.direction,
            "context_evaluation_horizons_minutes": list(
                contextual_stage.evaluation_horizons_minutes
            ),
            "evaluation_target_label": contextual_stage.evaluation_target_label,
            "price_context_model_version": PRICE_CONTEXT_MODEL_VERSION,
            "price_context_price": (
                _float_text(price_position.current_price, 12)
                if price_position is not None
                else None
            ),
            "price_context_price_at": (
                iso_utc(features.cutoff_at) if price_position is not None else None
            ),
            "price_state": price_position.state if price_position is not None else None,
            "price_state_label": (
                price_position.state_label if price_position is not None else None
            ),
            "current_pump_multiple": (
                _float_text(price_position.current_pump_multiple, 6)
                if price_position is not None
                else None
            ),
            "pump_start_date": (
                price_position.pump_start_at.date().isoformat()
                if price_position is not None
                and price_position.pump_start_at is not None
                else None
            ),
            "pump_baseline_price": (
                _float_text(price_position.pump_baseline_price, 12)
                if price_position is not None
                else None
            ),
            "pump_start_trigger": (
                price_position.pump_start_trigger
                if price_position is not None
                else None
            ),
            "price_position_90d_percent": (
                _float_text(price_position.position_90d_percent)
                if price_position is not None
                else None
            ),
            "price_return_7d_percent": (
                _float_text(price_position.return_7d_percent)
                if price_position is not None
                else None
            ),
            "price_return_30d_percent": (
                _float_text(price_position.return_30d_percent)
                if price_position is not None
                else None
            ),
            "price_daily_cutoff_at": (
                iso_utc(price_position.daily_cutoff_at)
                if price_position is not None
                else None
            ),
            "row_tone": contextual_stage.row_tone,
            "review_window_label": STAGE_REVIEW_LABELS[stage],
            "alert_score": _float_text(alert_score, 3),
            "setup_score": _float_text(float(scoring["setup_score"]), 3),
            "pump_score": _float_text(float(scoring["pump_score"]), 3),
            "tail_risk_score": _float_text(float(scoring["tail_risk_score"]), 3),
            "return_15m_percent": _float_text(features.return_15m_percent),
            "quote_volume_ratio_15m": _float_text(features.quote_volume_ratio_15m),
            "trade_count_ratio_15m": _float_text(features.trade_count_ratio_15m),
            "taker_buy_percent": _float_text(features.taker_buy_percent),
            "breakout_percent": _float_text(features.breakout_percent),
            "range_position_1h_percent": _float_text(
                features.range_position_1h_percent
            ),
            "compression_ratio": _float_text(features.compression_ratio),
            "peak_drawdown_15m_percent": _float_text(
                features.peak_drawdown_15m_percent
            ),
            "relative_return_15m_percent": _float_text(relative_return),
            "funding_rate_percent": _float_text(funding_rate),
            "oi_change_15m_percent": _float_text(enrichment.oi_change_15m_percent),
            "evidence_label": _evidence_label(
                stage,
                features,
                relative_return_15m_percent=relative_return,
                funding_rate_percent=funding_rate,
                oi_change_15m_percent=enrichment.oi_change_15m_percent,
            ),
            "data_cutoff_at": iso_utc(features.cutoff_at),
            "close_price": _float_text(features.close_price, 12),
            "benchmark_close_price": (
                _float_text(benchmark_features.close_price, 12)
                if benchmark_features is not None
                else None
            ),
            "valid_until": iso_utc(
                features.cutoff_at
                + timedelta(seconds=self.settings.kline_stale_seconds)
            ),
            "missing_reasons": missing_reasons,
        }
        return MetricSample(
            series_key=f"{enrichment.seed.symbol}|usdm-perpetual-alert-score",
            entity_key=enrichment.seed.symbol,
            observed_at=observed_at,
            value_text=f"{alert_score:.3f}",
            unit="RADAR_SCORE",
            payload=payload,
        )
