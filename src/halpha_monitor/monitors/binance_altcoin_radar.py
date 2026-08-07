"""Bounded USDⓈ-M perpetual radar for explainable altcoin anomalies.

The monitor intentionally reports observable stages and evidence scores.  It does
not claim a calibrated probability, causal prediction, or trading instruction.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
import math
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
MARKET_SCOPE = "USDM_PERPETUAL"
EVALUATION_SOURCE = "BINANCE_USDM_PUBLIC_CLOSED_5M_KLINES"
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
                status = int(raw_status if raw_status is not None else response.getcode())
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
            raise RadarSourceError(
                f"RADAR_HTTP_THROTTLED_{status}", throttled=True
            )
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


@dataclass(frozen=True)
class BinanceAltcoinRadarSettings:
    interval_seconds: float = 300
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

    def __post_init__(self) -> None:
        if self.interval_seconds < 300:
            raise ValueError("RADAR_INTERVAL_TOO_SHORT")
        if not 0 <= self.jitter_seconds <= 120:
            raise ValueError("RADAR_JITTER_INVALID")
        if (
            not self.min_quote_volume_24h.is_finite()
            or self.min_quote_volume_24h <= 0
        ):
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
        (ticker_24h.high_price - ticker_24h.low_price)
        / ticker_24h.open_price
        * 100.0
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
        if (
            ticker_24h is None
            or ticker_24h.quote_volume < min_quote_volume_24h
        ):
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
    elif tail_risk_score >= 55.0 and (
        pump_score >= 45.0 or return_24h_percent >= 10.0
    ):
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
    evaluation_source = EVALUATION_SOURCE
    description = (
        "Binance USDⓈ-M 永续合约全市场初筛与候选详查；识别蓄势、启动、加速、"
        "尾声风险和回落确认。评分是可解释异常证据，不是上涨概率、买卖建议或拉盘定性。"
    )
    default_enabled = False
    evaluation_batch_limit = 12

    def __init__(
        self,
        settings: BinanceAltcoinRadarSettings,
        *,
        client: AltcoinRadarProvider | None = None,
        evaluation_store: SQLiteMonitorStore | None = None,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.jitter_seconds = settings.jitter_seconds
        self.client = client or BinanceAltcoinRadarClient(
            timeout_seconds=settings.timeout_seconds,
            proxy_url=settings.proxy_url,
        )
        self.evaluation_store = evaluation_store
        self.view = MonitorView(
            filters=(
                ViewFilter(
                    key="stage",
                    label="异动阶段",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部候选"),
                        FilterChoice("SETUP", "蓄势观察"),
                        FilterChoice("BREAKOUT", "启动"),
                        FilterChoice("ACCELERATION", "加速"),
                        FilterChoice("EXHAUSTION", "尾声风险"),
                        FilterChoice("COOLDOWN", "回落确认"),
                        FilterChoice("NEUTRAL", "尚未形成"),
                    ),
                ),
            ),
            columns=(
                ViewColumn("symbol", "币种"),
                ViewColumn(
                    "stage_label",
                    "阶段",
                    description=(
                        "按优先级判定：1h≤-3%且24h≥5%，并伴随1h区间位置<45%"
                        "或主动买入<45%时为回落确认；尾声风险分≥55且启动分≥45"
                        "或24h≥10%时为尾声风险；启动分≥70且15m≥3%时为加速；"
                        "启动分≥55且突破前高或1h区间位置≥80%时为启动；蓄势分≥60"
                        "且15m处于-0.75%至+2.5%时为蓄势观察；否则尚未形成。"
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
                "波动压缩比较覆盖约2小时15分，流动性与部分风险条件看24h。"
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
                    "阶段首次出现、发生变化或连续4小时未复建样本时，固定记录信号"
                    "截止价，并在15分钟、1小时、4小时后用同一USDⓈ-M合约的闭合"
                    "5m K线检验。"
                    "方向一致要求绝对方向正确且相对BTC超出±0.5个百分点；"
                    "该带宽是暂定噪声区间，不含手续费、滑点或可成交性。"
                    "每个阶段×期限完成少于30例时只显示样本积累，不报告一致率。"
                ),
                minimum_group_samples=30,
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
                    issues
                    or [CollectionIssue("universe", "RADAR_NO_LIQUID_SYMBOLS")]
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
                    issues
                    or [CollectionIssue("universe", "RADAR_NO_CANDIDATES")]
                ),
            )

        benchmark_features: CandleFeatures | None = None
        try:
            benchmark = self.client.klines(
                "BTCUSDT", limit=self.settings.kline_limit
            )
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
            enrichment
            for enrichment in screened
            if enrichment.features is not None
        ]
        if not analyzed:
            return CollectionBatch(
                samples=(),
                issues=tuple(
                    issues
                    or [CollectionIssue("universe", "RADAR_NO_CANDIDATES")]
                ),
            )
        analyzed.sort(
            key=lambda enrichment: self._candidate_sort_key(
                enrichment,
                benchmark_features=benchmark_features,
            )
        )
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
                )
            )

        samples.sort(
            key=lambda sample: (
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
        return CollectionBatch(
            samples=tuple(samples),
            issues=tuple(issues),
            evaluation_cases=evaluation_cases,
        )

    def _new_evaluation_cases(
        self,
        samples: tuple[MetricSample, ...],
    ) -> tuple[ForwardEvaluationCase, ...]:
        """Freeze bounded stage episodes before any future return is observable."""

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
        entity_keys = tuple(sample.entity_key for sample in candidates)
        previous_samples = {
            sample.entity_key: sample
            for sample in self.evaluation_store.latest_samples_by_entity(
                self.monitor_id,
                entity_keys,
            )
        }
        previous_evaluations = {
            evaluation.entity_key: evaluation
            for evaluation in self.evaluation_store.latest_forward_evaluations_by_entity(
                self.monitor_id,
                entity_keys,
                source=self.evaluation_source,
            )
        }
        cases: list[ForwardEvaluationCase] = []
        for sample in candidates:
            stage = str(sample.payload["stage"])
            cutoff = datetime.fromisoformat(
                str(sample.payload["data_cutoff_at"]).replace("Z", "+00:00")
            ).astimezone(UTC)
            previous_sample = previous_samples.get(sample.entity_key)
            previous_stage = (
                str(previous_sample.payload.get("stage"))
                if previous_sample is not None
                and previous_sample.payload.get("market_scope") == MARKET_SCOPE
                else None
            )
            previous_evaluation = previous_evaluations.get(sample.entity_key)
            repeated_after_gap = (
                previous_evaluation is None
                or cutoff - previous_evaluation.source_cutoff_at
                >= EVALUATION_REPEAT_AFTER
            )
            if previous_stage == stage and not repeated_after_gap:
                continue
            direction = EVALUATION_DIRECTIONS[stage]
            for horizon_minutes in EVALUATION_HORIZONS_MINUTES:
                cases.append(
                    ForwardEvaluationCase(
                        case_key=(
                            f"{sample.entity_key}|{stage}|"
                            f"{iso_utc(cutoff)}|{horizon_minutes}"
                        ),
                        entity_key=sample.entity_key,
                        stage=stage,
                        stage_label=str(sample.payload["stage_label"]),
                        direction=direction,  # type: ignore[arg-type]
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
        incompatible = tuple(
            case for case in pending if case.source != self.evaluation_source
        )
        pending = tuple(
            case for case in pending if case.source == self.evaluation_source
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
            (end_at - start_at).total_seconds()
            / (KLINE_INTERVAL_MINUTES * 60)
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
            entry_price = float(_decimal(
                case.entry_price_text,
                field="evaluation.entryPrice",
                positive=True,
            ))
            benchmark_entry = float(_decimal(
                case.benchmark_entry_price_text,
                field="evaluation.benchmarkEntryPrice",
                positive=True,
            ))
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
            candles = self.client.klines(
                seed.symbol, limit=self.settings.kline_limit
            )
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
        missing_reasons: dict[str, str] = {}
        if relative_return is None:
            missing_reasons["relative_return_15m_percent"] = (
                "未取得 BTC 同期通过校验的闭合 K 线；"
                "相对表现保持为空，未使用替代值。"
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
            "row_tone": STAGE_TONES[stage],
            "review_window_label": STAGE_REVIEW_LABELS[stage],
            "alert_score": _float_text(alert_score, 3),
            "setup_score": _float_text(float(scoring["setup_score"]), 3),
            "pump_score": _float_text(float(scoring["pump_score"]), 3),
            "tail_risk_score": _float_text(float(scoring["tail_risk_score"]), 3),
            "return_15m_percent": _float_text(features.return_15m_percent),
            "quote_volume_ratio_15m": _float_text(
                features.quote_volume_ratio_15m
            ),
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
            "oi_change_15m_percent": _float_text(
                enrichment.oi_change_15m_percent
            ),
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
                features.cutoff_at + timedelta(seconds=self.settings.kline_stale_seconds)
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
