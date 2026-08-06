"""Bounded public-market radar for explainable altcoin pump-like anomalies.

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
    CollectionIssue,
    FilterChoice,
    MetricSample,
    MonitorView,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.store import iso_utc
from halpha_monitor.telemetry import NetworkRequestWindow


BINANCE_SPOT_MARKET_BASE = "https://data-api.binance.vision"
BINANCE_USDM_BASE = "https://fapi.binance.com"
USER_AGENT = "Halpha-Monitor/0.1 public-market-read-only"
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_ROLLING_SYMBOLS = 100
KLINE_INTERVAL_MINUTES = 5

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
    "DATA_GAP": "数据不足",
}
STAGE_TONES = {
    "SETUP": "INFO",
    "BREAKOUT": "WARNING",
    "ACCELERATION": "DANGER",
    "EXHAUSTION": "DANGER",
    "COOLDOWN": "MUTED",
    "NEUTRAL": "NEUTRAL",
    "DATA_GAP": "MUTED",
}
STAGE_REVIEW_LABELS = {
    "SETUP": "下一根 5m K 复核",
    "BREAKOUT": "下一根 5m K 复核",
    "ACCELERATION": "每根 5m K 复核",
    "EXHAUSTION": "当前即复核",
    "COOLDOWN": "每根 5m K 复核",
    "NEUTRAL": "下次采集重算",
    "DATA_GAP": "不可判断",
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


def rolling_symbol_batches(
    symbols: Sequence[str], batch_size: int
) -> tuple[tuple[str, ...], ...]:
    """Keep non-ASCII symbols isolated for Binance's multi-symbol validator."""

    ascii_symbols = [symbol for symbol in symbols if symbol.isascii()]
    unicode_symbols = [symbol for symbol in symbols if not symbol.isascii()]
    batches = [
        tuple(ascii_symbols[offset : offset + batch_size])
        for offset in range(0, len(ascii_symbols), batch_size)
    ]
    batches.extend((symbol,) for symbol in unicode_symbols)
    return tuple(batch for batch in batches if batch)


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
class SpotCandle:
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


@dataclass(frozen=True)
class CandidateSeed:
    symbol: str
    base_asset: str
    ticker_24h: RollingTicker
    ticker_1h: RollingTicker
    volume_acceleration_1h: float | None
    priority_score: float


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

    def rolling_tickers(
        self, symbols: Sequence[str], *, window_size: str
    ) -> TimedValue: ...

    def spot_klines(self, symbol: str, *, limit: int) -> TimedValue: ...

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
        except (KeyError, TypeError, ValueError):
            malformed += 1
            continue
        if (
            not symbol
            or not base_asset
            or quote_asset != "USDT"
            or status != "TRADING"
            or item.get("isSpotTradingAllowed") is False
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


def parse_spot_klines(
    payload: Any,
    *,
    completed_at: datetime,
) -> tuple[tuple[SpotCandle, ...], int]:
    if not isinstance(payload, list):
        raise RadarSourceError("RADAR_KLINES_SCHEMA_INVALID")
    parsed: dict[int, SpotCandle] = {}
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
        parsed[int(row[0])] = SpotCandle(
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

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return self._network_requests.count(window_seconds=window_seconds)

    def ensure_available(self) -> None:
        with self._backoff_lock:
            if self._backoff_until is not None and self._now() < self._backoff_until:
                raise RadarSourceError("RADAR_BACKOFF_ACTIVE", throttled=True)

    def reset_throttle_backoff(self) -> None:
        with self._backoff_lock:
            self._backoff_until = None
            self._throttle_failures = 0

    def exchange_symbols(self) -> TimedValue:
        response = self._get_json(
            BINANCE_SPOT_MARKET_BASE,
            "/api/v3/exchangeInfo",
            (("permissions", "SPOT"), ("symbolStatus", "TRADING")),
        )
        value, malformed = parse_exchange_symbols(response.value)
        return TimedValue(value, response.completed_at, malformed)

    def ticker_24h(self) -> TimedValue:
        response = self._get_json(
            BINANCE_SPOT_MARKET_BASE,
            "/api/v3/ticker/24hr",
            (("type", "FULL"), ("symbolStatus", "TRADING")),
        )
        value, malformed = parse_tickers(response.value)
        return TimedValue(value, response.completed_at, malformed)

    def rolling_tickers(
        self, symbols: Sequence[str], *, window_size: str
    ) -> TimedValue:
        if not symbols or len(symbols) > MAX_ROLLING_SYMBOLS:
            raise ValueError("RADAR_ROLLING_BATCH_INVALID")
        response = self._get_json(
            BINANCE_SPOT_MARKET_BASE,
            "/api/v3/ticker",
            (
                (
                    "symbols",
                    json.dumps(
                        list(symbols), separators=(",", ":"), ensure_ascii=False
                    ),
                ),
                ("windowSize", window_size),
                ("type", "FULL"),
                ("symbolStatus", "TRADING"),
            ),
        )
        value, malformed = parse_tickers(response.value)
        return TimedValue(value, response.completed_at, malformed)

    def spot_klines(self, symbol: str, *, limit: int) -> TimedValue:
        response = self._get_json(
            BINANCE_SPOT_MARKET_BASE,
            "/api/v3/klines",
            (("symbol", symbol), ("interval", "5m"), ("limit", str(limit))),
        )
        value, malformed = parse_spot_klines(
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
    rolling_batch_size: int = 100
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
        if not 1 <= self.rolling_batch_size <= MAX_ROLLING_SYMBOLS:
            raise ValueError("RADAR_ROLLING_BATCH_INVALID")
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


def analyze_candles(candles: Sequence[SpotCandle]) -> CandleFeatures:
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

    def normalized_range(values: Sequence[SpotCandle]) -> float:
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


def _candidate_priority(ticker_1h: RollingTicker, ticker_24h: RollingTicker) -> float:
    volume_acceleration = _ratio(
        ticker_1h.quote_volume,
        ticker_24h.quote_volume / 24.0,
    )
    volume_points = _ramp(volume_acceleration or 0.0, 0.8, 4.0, 38.0)
    momentum_points = _ramp(ticker_1h.price_change_percent, -0.5, 8.0, 30.0)
    day_points = _ramp(ticker_24h.price_change_percent, 0.0, 25.0, 12.0)
    range_points = _ramp(
        ticker_1h.range_position_percent or 0.0,
        45.0,
        90.0,
        20.0,
    )
    setup_bonus = (
        15.0
        if volume_acceleration is not None
        and volume_acceleration >= 1.4
        and -1.0 <= ticker_1h.price_change_percent <= 3.0
        else 0.0
    )
    return _bounded(volume_points + momentum_points + day_points + range_points + setup_bonus)


def select_candidate_seeds(
    symbols: dict[str, str],
    tickers_24h: dict[str, RollingTicker],
    tickers_1h: dict[str, RollingTicker],
    *,
    min_quote_volume_24h: float,
    maximum: int,
) -> tuple[CandidateSeed, ...]:
    seeds: list[CandidateSeed] = []
    for symbol, base_asset in symbols.items():
        if not _eligible_altcoin(base_asset):
            continue
        ticker_24h = tickers_24h.get(symbol)
        ticker_1h = tickers_1h.get(symbol)
        if (
            ticker_24h is None
            or ticker_1h is None
            or ticker_24h.quote_volume < min_quote_volume_24h
        ):
            continue
        volume_acceleration = _ratio(
            ticker_1h.quote_volume,
            ticker_24h.quote_volume / 24.0,
        )
        seeds.append(
            CandidateSeed(
                symbol=symbol,
                base_asset=base_asset,
                ticker_24h=ticker_24h,
                ticker_1h=ticker_1h,
                volume_acceleration_1h=volume_acceleration,
                priority_score=_candidate_priority(ticker_1h, ticker_24h),
            )
        )
    seeds.sort(
        key=lambda seed: (
            -seed.priority_score,
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


def _evidence_strength(
    *,
    stage: str,
    alert_score: float,
    benchmark_available: bool,
    futures_available: bool,
) -> tuple[str, str]:
    if (
        stage != "NEUTRAL"
        and alert_score >= 75.0
        and benchmark_available
        and futures_available
    ):
        return "HIGH", "高"
    if stage != "NEUTRAL" and alert_score >= 55.0 and benchmark_available:
        return "MEDIUM", "中"
    return "LOW", "低"


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
    description = (
        "Binance 公开现货全市场初筛与候选详查；识别蓄势、启动、加速、"
        "尾声风险和回落确认。评分是可解释异常证据，不是上涨概率、买卖建议或拉盘定性。"
    )
    default_enabled = False

    def __init__(
        self,
        settings: BinanceAltcoinRadarSettings,
        *,
        client: AltcoinRadarProvider | None = None,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.jitter_seconds = settings.jitter_seconds
        self.client = client or BinanceAltcoinRadarClient(
            timeout_seconds=settings.timeout_seconds,
            proxy_url=settings.proxy_url,
        )
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
                        FilterChoice("DATA_GAP", "数据不足"),
                    ),
                ),
                ViewFilter(
                    key="evidence_strength",
                    label="证据强度",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部"),
                        FilterChoice("HIGH", "高"),
                        FilterChoice("MEDIUM", "中"),
                        FilterChoice("LOW", "低"),
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
                        "它是异常证据强度，不是上涨概率。"
                    ),
                ),
                ViewColumn(
                    "evidence_strength_label",
                    "证据",
                    description=(
                        "高：已形成阶段、异动强度≥75，且BTC基准与合约补充均完整；"
                        "中：已形成阶段、异动强度≥55，且BTC基准完整；其他为低。"
                    ),
                ),
                ViewColumn(
                    "valid_until",
                    "有效至",
                    "time",
                    description=(
                        "本行规则结论的时效截止：最新闭合5m K线结束时间加15分钟。"
                        "到期后必须等待包含更新K线的新一轮采集，不能继续把本行视为当前信号。"
                    ),
                ),
                ViewColumn(
                    "review_window_label",
                    "复核节奏",
                    description=(
                        "按5m闭合K线更新频率给出的风险复核节奏；尾声风险需当前即复核。"
                        "这不是经过手续费、滑点和样本外回测验证的期望盈利持仓期。"
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
                    priority="secondary",
                    description="该币15m涨跌幅减去BTC同期15m涨跌幅。",
                ),
                ViewColumn(
                    "oi_change_15m_percent",
                    "OI 15m",
                    "percent",
                    priority="secondary",
                    description=(
                        "同名USDⓈ-M合约未平仓量最近15m变化率；"
                        "需要连续4个5m OI点，否则保持为空。"
                    ),
                ),
                ViewColumn(
                    "funding_rate_percent",
                    "资金费率",
                    "percent",
                    priority="secondary",
                    description=(
                        "同名USDⓈ-M永续合约premiumIndex返回的最近资金费率；"
                        "没有同名合约或来源未通过校验时保持为空。"
                    ),
                ),
                ViewColumn(
                    "quote_volume_24h",
                    "24h 成交额 (USDT)",
                    "number",
                    priority="secondary",
                    maximum_fraction_digits=0,
                    use_grouping=True,
                    description="Binance现货滚动24h的USDT计价成交额，用于流动性初筛。",
                ),
                ViewColumn(
                    "evidence_label",
                    "主要证据",
                    priority="secondary",
                    description=(
                        "按本轮各特征偏离程度排序后显示贡献最显著的3项事实；"
                        "它用于解释阶段归因，不是额外评分。"
                    ),
                ),
                ViewColumn(
                    "coverage_label",
                    "扫描覆盖",
                    priority="secondary",
                    description=(
                        "依次列出本轮参与初筛的USDT现货数、达到24h流动性门槛的数量，"
                        "以及实际读取5m K线详查的候选数量。"
                    ),
                ),
                ViewColumn(
                    "data_cutoff_at",
                    "市场截止",
                    "time",
                    description=(
                        "本行计算所使用的最新闭合5m K线结束时间，"
                        "不是页面刷新时间或网络响应时间。"
                    ),
                ),
            ),
            table_title="全市场初筛后的异动候选",
            chart_title="异动强度历史（0–100）",
            method_note=(
                "分析周期：每根闭合5m K重算，短线主判看15m，趋势背景看1h，"
                "波动压缩比较覆盖约2小时15分，流动性与部分风险条件看24h。"
                "表内“有效至”是每行结论的硬截止；“复核节奏”是风险检查频率，"
                "不是期望盈利持仓期。当前尚无含手续费、滑点的样本外回测，"
                "因此不推断持仓时长。"
            ),
            show_description=False,
        )

    def network_request_count(self, *, window_seconds: float = 60) -> int | None:
        counter = getattr(self.client, "network_request_count", None)
        if not callable(counter):
            return None
        return int(counter(window_seconds=window_seconds))

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

        tickers_1h: dict[str, RollingTicker] = {}
        rolling_batches = rolling_symbol_batches(
            liquid_symbols, self.settings.rolling_batch_size
        )
        for batch_number, batch_symbols in enumerate(rolling_batches, start=1):
            scope = f"ticker-1h:{batch_number}"
            try:
                result = self.client.rolling_tickers(
                    batch_symbols,
                    window_size="1h",
                )
                self._append_malformed_issue(issues, scope, result)
                tickers_1h.update(
                    self._fresh_tickers(
                        dict(result.value),
                        observed_at=result.completed_at,
                        scope=scope,
                        issues=issues,
                    )
                )
            except RadarSourceError as exc:
                issues.append(CollectionIssue(scope, exc.reason_code))
                if exc.throttled:
                    break

        seeds = select_candidate_seeds(
            symbols,
            tickers_24h,
            tickers_1h,
            min_quote_volume_24h=float(self.settings.min_quote_volume_24h),
            maximum=self.settings.max_candidates,
        )
        if not seeds:
            return CollectionBatch(
                samples=(),
                issues=tuple(
                    issues
                    or [CollectionIssue("universe", "RADAR_NO_CANDIDATES")]
                ),
            )

        benchmark_features: CandleFeatures | None = None
        try:
            benchmark = self.client.spot_klines(
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

        enrichments: list[CandidateEnrichment] = []
        with ThreadPoolExecutor(max_workers=self.settings.workers) as pool:
            futures = {
                pool.submit(self._enrich, seed, funding_contexts.get(seed.symbol)): seed
                for seed in seeds
            }
            for future in as_completed(futures):
                seed = futures[future]
                try:
                    enrichments.append(future.result())
                except Exception:
                    enrichments.append(
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
                                "候选详查发生未分类失败；未生成阶段或评分，"
                                "也未使用替代值。"
                            ),
                        )
                    )

        observed_at = utc_now()
        samples: list[MetricSample] = []
        for enrichment in enrichments:
            issues.extend(enrichment.issues)
            if enrichment.features is None:
                samples.append(
                    self._missing_sample(
                        enrichment,
                        observed_at=observed_at,
                        universe_size=len(symbols),
                        liquid_size=len(liquid_symbols),
                        shortlist_size=len(seeds),
                    )
                )
                continue
            samples.append(
                self._sample(
                    enrichment,
                    benchmark_features=benchmark_features,
                    observed_at=observed_at,
                    universe_size=len(symbols),
                    liquid_size=len(liquid_symbols),
                    shortlist_size=len(seeds),
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
        return CollectionBatch(samples=tuple(samples), issues=tuple(issues))

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

    def _enrich(
        self,
        seed: CandidateSeed,
        funding: FundingContext | None,
    ) -> CandidateEnrichment:
        issues: list[CollectionIssue] = []
        try:
            candles = self.client.spot_klines(
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
                    "该候选没有连续、新鲜且通过校验的 5m 闭合 K 线；"
                    "未生成阶段或评分，也未使用旧值或替代值。"
                ),
            )

        oi_change: float | None = None
        if funding is not None:
            try:
                oi = self.client.open_interest_history(seed.symbol, limit=6)
                points = tuple(oi.value)
                if not points:
                    raise RadarSourceError("RADAR_OI_EMPTY")
                age = (oi.completed_at - points[-1].source_time).total_seconds()
                if age < -120 or age > self.settings.futures_stale_seconds:
                    raise RadarSourceError("RADAR_OI_STALE")
                oi_change = open_interest_change_15m(points)
                if oi_change is None:
                    raise RadarSourceError("RADAR_OI_INSUFFICIENT")
                self._append_malformed_issue(issues, seed.symbol, oi)
            except RadarSourceError as exc:
                issues.append(CollectionIssue(seed.symbol, exc.reason_code))
        return CandidateEnrichment(
            seed=seed,
            features=features,
            funding=funding,
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
        shortlist_size: int,
    ) -> dict[str, Any]:
        seed = enrichment.seed
        return {
            "symbol": seed.symbol,
            "base_asset": seed.base_asset,
            "series_label": f"{seed.symbol} · 异动强度",
            "observed_at": iso_utc(observed_at),
            "ticker_24h_close_at": iso_utc(seed.ticker_24h.close_time),
            "ticker_1h_close_at": iso_utc(seed.ticker_1h.close_time),
            "return_1h_percent": _float_text(seed.ticker_1h.price_change_percent),
            "return_24h_percent": _float_text(seed.ticker_24h.price_change_percent),
            "volume_acceleration_1h": _float_text(seed.volume_acceleration_1h),
            "quote_volume_24h": _float_text(seed.ticker_24h.quote_volume, 2),
            "universe_size": universe_size,
            "liquid_universe_size": liquid_size,
            "shortlist_size": shortlist_size,
            "data_scope_label": (
                "现货 + 合约" if enrichment.funding is not None else "仅现货"
            ),
            "coverage_label": (
                f"全市场 {universe_size} 个 USDT 现货初筛（已排除内置名单）；"
                f"{liquid_size} 个达到流动性阈值；详查 {shortlist_size} 个。"
            ),
            "source": "BINANCE_SPOT_AND_USDM_PUBLIC_MARKET_DATA",
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

    def _missing_sample(
        self,
        enrichment: CandidateEnrichment,
        *,
        observed_at: datetime,
        universe_size: int,
        liquid_size: int,
        shortlist_size: int,
    ) -> MetricSample:
        reason = enrichment.missing_reason or (
            "候选详查未取得通过校验的结果；未生成阶段或评分，"
            "也未使用替代值。"
        )
        missing_fields = (
            "alert_score",
            "valid_until",
            "return_15m_percent",
            "quote_volume_ratio_15m",
            "taker_buy_percent",
            "tail_risk_score",
            "relative_return_15m_percent",
            "oi_change_15m_percent",
            "funding_rate_percent",
            "data_cutoff_at",
        )
        return MetricSample(
            series_key=f"{enrichment.seed.symbol}|alert-score",
            entity_key=enrichment.seed.symbol,
            observed_at=observed_at,
            value_text="",
            unit="RADAR_SCORE",
            payload={
                **self._base_payload(
                    enrichment,
                    observed_at=observed_at,
                    universe_size=universe_size,
                    liquid_size=liquid_size,
                    shortlist_size=shortlist_size,
                ),
                "stage": "DATA_GAP",
                "stage_label": STAGE_LABELS["DATA_GAP"],
                "row_tone": STAGE_TONES["DATA_GAP"],
                "evidence_strength": "LOW",
                "evidence_strength_label": "低",
                "evidence_label": "候选详查数据不足",
                "review_window_label": STAGE_REVIEW_LABELS["DATA_GAP"],
                **{field: None for field in missing_fields},
                "missing_reasons": {field: reason for field in missing_fields},
            },
        )

    def _sample(
        self,
        enrichment: CandidateEnrichment,
        *,
        benchmark_features: CandleFeatures | None,
        observed_at: datetime,
        universe_size: int,
        liquid_size: int,
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
        strength, strength_label = _evidence_strength(
            stage=stage,
            alert_score=float(scoring["alert_score"]),
            benchmark_available=benchmark_features is not None,
            futures_available=(
                enrichment.funding is not None
                and enrichment.oi_change_15m_percent is not None
            ),
        )
        missing_reasons: dict[str, str] = {}
        if relative_return is None:
            missing_reasons["relative_return_15m_percent"] = (
                "未取得 BTC 同期通过校验的闭合 K 线；"
                "相对表现保持为空，未使用替代值。"
            )
        if funding_rate is None:
            missing_reasons["funding_rate_percent"] = (
                "未取得该现货的同名 USDⓈ-M 永续合约，"
                "或本轮合约来源未返回通过校验的结果。"
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
                shortlist_size=shortlist_size,
            ),
            "stage": stage,
            "stage_label": STAGE_LABELS[stage],
            "row_tone": STAGE_TONES[stage],
            "review_window_label": STAGE_REVIEW_LABELS[stage],
            "evidence_strength": strength,
            "evidence_strength_label": strength_label,
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
            "valid_until": iso_utc(
                features.cutoff_at + timedelta(seconds=self.settings.kline_stale_seconds)
            ),
            "missing_reasons": missing_reasons,
        }
        return MetricSample(
            series_key=f"{enrichment.seed.symbol}|alert-score",
            entity_key=enrichment.seed.symbol,
            observed_at=observed_at,
            value_text=f"{alert_score:.3f}",
            unit="RADAR_SCORE",
            payload=payload,
        )
