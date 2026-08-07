"""Forward-only Binance USDⓈ-M Smart Money research monitor.

The Smart Money endpoints are Binance website-internal `/bapi` interfaces, not a
documented Developer API. This monitor therefore persists every bounded raw
response, validates the observed contract strictly, and fails closed before
producing a derived feature when the required source becomes ambiguous.
"""

from __future__ import annotations

import json
import math
import random
import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener

from halpha_monitor.contracts import (
    CollectionArtifact,
    CollectionBatch,
    CollectionCancelled,
    CollectionIssue,
    FilterChoice,
    MetricSample,
    MonitorView,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.store import iso_utc
from halpha_monitor.telemetry import NetworkRequestWindow


TimeRange = Literal["30m", "1h"]

BINANCE_WEB_BASE = "https://www.binance.com"
BINANCE_USDM_BASE = "https://fapi.binance.com"
USER_AGENT = "Halpha-Monitor/0.3"
MAX_RESPONSE_BYTES = 1_000_000
SUPPORTED_TIME_RANGES: tuple[TimeRange, ...] = ("30m", "1h")
TIME_RANGE_SECONDS: dict[TimeRange, int] = {"30m": 30 * 60, "1h": 60 * 60}
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{5,20}$")

BAPI_ROOT_KEYS = {"code", "message", "messageDetail", "data", "success"}
BAPI_DETAILS_ROOT_KEYS = {*BAPI_ROOT_KEYS, "total"}
OVERVIEW_KEYS = {
    "longProfitTraders",
    "longProfitWhales",
    "longShortRatio",
    "longTraders",
    "longTradersAvgEntryPrice",
    "longTradersQty",
    "longWhales",
    "longWhalesAvgEntryPrice",
    "longWhalesQty",
    "shortProfitTraders",
    "shortProfitWhales",
    "shortTraders",
    "shortTradersAvgEntryPrice",
    "shortTradersQty",
    "shortWhales",
    "shortWhalesAvgEntryPrice",
    "shortWhalesQty",
    "symbol",
    "totalPositions",
    "totalTraders",
    "updateTime",
}
STATS_KEYS = {
    "longPositions",
    "longQty",
    "longTraders",
    "longWhalePositions",
    "longWhaleQty",
    "longWhales",
    "shortPositions",
    "shortQty",
    "shortTraders",
    "shortWhalePositions",
    "shortWhaleQty",
    "shortWhales",
}
DETAIL_KEYS = {
    "avgEntryPrice",
    "currentPositions",
    "currentQty",
    "lastTradeTime",
    "netPositions",
    "side",
    "timeRange",
    "topTraderId",
    "traderName",
    "traders",
    "userType",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = format(value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _schema_paths(value: Any, prefix: str = "$") -> set[str]:
    if isinstance(value, dict):
        paths = {f"{prefix}:object"}
        for key, child in value.items():
            paths.update(_schema_paths(child, f"{prefix}.{key}"))
        return paths
    if isinstance(value, list):
        paths = {f"{prefix}:array"}
        for child in value[:20]:
            paths.update(_schema_paths(child, f"{prefix}[]"))
        return paths
    return {f"{prefix}:value"}


def schema_hash(value: Any) -> str:
    shape = "\n".join(sorted(_schema_paths(value))).encode("utf-8")
    return sha256(shape).hexdigest()


def _record_count(payload: Any) -> int | None:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return len(data)
        if isinstance(data, dict):
            return 1
        return 1
    if isinstance(payload, list):
        return len(payload)
    return None


class SmartMoneyMonitorError(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        *,
        artifact: CollectionArtifact | None = None,
        throttled: bool = False,
    ) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.artifact = artifact
        self.throttled = throttled


@dataclass(frozen=True)
class RecordedJsonResponse:
    artifact: CollectionArtifact
    payload: Any


@dataclass(frozen=True)
class BinanceSmartMoneySettings:
    interval_seconds: float = 60
    jitter_seconds: float = 5
    symbols: tuple[str, ...] = ("BTCUSDT",)
    time_ranges: tuple[TimeRange, ...] = SUPPORTED_TIME_RANGES
    timeout_seconds: float = 10
    proxy_url: str | None = None
    overview_stale_seconds: int = 2 * 60 * 60
    market_stale_seconds: int = 5 * 60

    def __post_init__(self) -> None:
        symbols = tuple(
            dict.fromkeys(symbol.strip().upper() for symbol in self.symbols if symbol.strip())
        )
        if (
            not symbols
            or len(symbols) > 20
            or any(SYMBOL_PATTERN.fullmatch(symbol) is None for symbol in symbols)
        ):
            raise ValueError("SMART_MONEY_SYMBOLS_INVALID")
        if not math.isfinite(self.interval_seconds) or self.interval_seconds < 60:
            raise ValueError("SMART_MONEY_INTERVAL_TOO_SHORT")
        if not 0 <= self.jitter_seconds <= 30:
            raise ValueError("SMART_MONEY_JITTER_INVALID")
        if not self.time_ranges or any(
            value not in SUPPORTED_TIME_RANGES for value in self.time_ranges
        ):
            raise ValueError("SMART_MONEY_TIME_RANGES_INVALID")
        if not math.isfinite(self.timeout_seconds) or not 0 < self.timeout_seconds <= 60:
            raise ValueError("SMART_MONEY_TIMEOUT_INVALID")
        if self.overview_stale_seconds < 60 or self.market_stale_seconds < 30:
            raise ValueError("SMART_MONEY_FRESHNESS_THRESHOLD_INVALID")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "time_ranges", tuple(dict.fromkeys(self.time_ranges)))


@dataclass(frozen=True)
class SmartMoneyOverview:
    updated_at: datetime
    long_traders: int
    short_traders: int
    long_whales: int
    short_whales: int


@dataclass(frozen=True)
class SmartMoneyStats:
    long_positions: Decimal
    short_positions: Decimal
    long_whale_positions: Decimal
    short_whale_positions: Decimal
    long_traders: int
    short_traders: int
    long_whales: int
    short_whales: int


@dataclass(frozen=True)
class UsdmMarketContext:
    open_interest: Decimal
    open_interest_time: datetime
    mark_price: Decimal
    funding_rate: Decimal
    mark_time: datetime


class BinanceSmartMoneyClient:
    """Bounded unauthenticated JSON client with in-memory throttle backoff."""

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
        self._network_requests = NetworkRequestWindow()
        self._stop_event: threading.Event | None = None

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event

    def _raise_if_cancelled(self) -> None:
        if self._stop_event is not None and self._stop_event.is_set():
            raise CollectionCancelled("SMART_MONEY_COLLECTION_CANCELLED")

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return self._network_requests.count(window_seconds=window_seconds)

    def ensure_available(self) -> None:
        self._raise_if_cancelled()
        if self._backoff_until is not None and self._now() < self._backoff_until:
            raise SmartMoneyMonitorError("SMART_MONEY_BACKOFF_ACTIVE", throttled=True)

    def reset_throttle_backoff(self) -> None:
        self._backoff_until = None
        self._throttle_failures = 0

    def get_json(
        self,
        *,
        artifact_key: str,
        base: str,
        path: str,
        params: tuple[tuple[str, str], ...],
    ) -> RecordedJsonResponse:
        self.ensure_available()
        source = f"{base}{path}"
        if params:
            source = f"{source}?{urlencode(params)}"
        request = Request(
            source,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        requested_at = self._now()
        self._network_requests.record()
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                raw_status = getattr(response, "status", None)
                status = int(raw_status if raw_status is not None else response.getcode())
                body = response.read(MAX_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            body = exc.read(MAX_RESPONSE_BYTES + 1)
            return self._decode_or_raise(
                artifact_key=artifact_key,
                source=source,
                requested_at=requested_at,
                completed_at=self._now(),
                status=int(exc.code),
                body=body,
            )
        except (TimeoutError, URLError, OSError) as exc:
            completed_at = self._now()
            artifact = self._artifact(
                artifact_key=artifact_key,
                source=source,
                requested_at=requested_at,
                completed_at=completed_at,
                status=0,
                body=b"",
                payload=None,
            )
            raise SmartMoneyMonitorError(
                f"SMART_MONEY_NETWORK_{type(exc).__name__.upper()}",
                artifact=artifact,
            ) from None
        return self._decode_or_raise(
            artifact_key=artifact_key,
            source=source,
            requested_at=requested_at,
            completed_at=self._now(),
            status=status,
            body=body,
        )

    def _decode_or_raise(
        self,
        *,
        artifact_key: str,
        source: str,
        requested_at: datetime,
        completed_at: datetime,
        status: int,
        body: bytes,
    ) -> RecordedJsonResponse:
        if len(body) > MAX_RESPONSE_BYTES:
            artifact = self._artifact(
                artifact_key=artifact_key,
                source=source,
                requested_at=requested_at,
                completed_at=completed_at,
                status=status,
                body=body[:MAX_RESPONSE_BYTES],
                payload=None,
            )
            raise SmartMoneyMonitorError(
                "SMART_MONEY_RESPONSE_TOO_LARGE", artifact=artifact
            )
        if not body:
            artifact = self._artifact(
                artifact_key=artifact_key,
                source=source,
                requested_at=requested_at,
                completed_at=completed_at,
                status=status,
                body=body,
                payload=None,
            )
            raise SmartMoneyMonitorError("SMART_MONEY_RESPONSE_EMPTY", artifact=artifact)
        try:
            text = body.decode("utf-8")
            payload = json.loads(text, parse_float=Decimal)
        except (UnicodeDecodeError, json.JSONDecodeError):
            artifact = self._artifact(
                artifact_key=artifact_key,
                source=source,
                requested_at=requested_at,
                completed_at=completed_at,
                status=status,
                body=body,
                payload=None,
            )
            raise SmartMoneyMonitorError(
                "SMART_MONEY_RESPONSE_JSON_INVALID", artifact=artifact
            ) from None
        artifact = self._artifact(
            artifact_key=artifact_key,
            source=source,
            requested_at=requested_at,
            completed_at=completed_at,
            status=status,
            body=body,
            payload=payload,
        )
        if status in {418, 429}:
            self._throttle_failures += 1
            delay = min(3600, 60 * (2 ** (self._throttle_failures - 1)))
            delay += self._random_uniform(0, 15)
            self._backoff_until = completed_at + timedelta(seconds=delay)
            raise SmartMoneyMonitorError(
                f"SMART_MONEY_HTTP_THROTTLED_{status}",
                artifact=artifact,
                throttled=True,
            )
        if status < 200 or status >= 300:
            raise SmartMoneyMonitorError(
                f"SMART_MONEY_HTTP_{status}", artifact=artifact
            )
        return RecordedJsonResponse(artifact=artifact, payload=payload)

    @staticmethod
    def _artifact(
        *,
        artifact_key: str,
        source: str,
        requested_at: datetime,
        completed_at: datetime,
        status: int,
        body: bytes,
        payload: Any,
    ) -> CollectionArtifact:
        try:
            response_body = body.decode("utf-8")
        except UnicodeDecodeError:
            response_body = body.decode("utf-8", errors="replace")
        business_code = (
            str(payload.get("code"))
            if isinstance(payload, dict) and payload.get("code") is not None
            else None
        )
        return CollectionArtifact(
            artifact_key=artifact_key,
            source=source,
            request_started_at=requested_at,
            response_completed_at=completed_at,
            http_status=status,
            business_code=business_code,
            schema_hash=schema_hash(payload) if payload is not None else sha256(b"").hexdigest(),
            response_sha256=sha256(body).hexdigest(),
            record_count=_record_count(payload),
            response_body=response_body,
        )


def _bapi_data(payload: Any, *, details: bool = False) -> Any:
    if not isinstance(payload, dict):
        raise SmartMoneyMonitorError("SMART_MONEY_SCHEMA_CHANGED")
    if payload.get("code") != "000000" or payload.get("success") is not True:
        raise SmartMoneyMonitorError("SMART_MONEY_BUSINESS_RESPONSE_FAILED")
    expected = BAPI_DETAILS_ROOT_KEYS if details else BAPI_ROOT_KEYS
    if set(payload) != expected:
        raise SmartMoneyMonitorError("SMART_MONEY_SCHEMA_CHANGED")
    return payload["data"]


def _decimal(
    value: Any,
    *,
    field: str,
    allow_string: bool = False,
    positive: bool = False,
    allow_negative: bool = False,
) -> Decimal:
    if isinstance(value, bool) or (
        not allow_string and not isinstance(value, (int, Decimal))
    ):
        raise SmartMoneyMonitorError(f"SMART_MONEY_FIELD_INVALID_{field.upper()}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise SmartMoneyMonitorError(
            f"SMART_MONEY_FIELD_INVALID_{field.upper()}"
        ) from None
    if (
        not parsed.is_finite()
        or (not allow_negative and parsed < 0)
        or (positive and parsed == 0)
    ):
        raise SmartMoneyMonitorError(f"SMART_MONEY_FIELD_INVALID_{field.upper()}")
    return parsed


def _integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SmartMoneyMonitorError(f"SMART_MONEY_FIELD_INVALID_{field.upper()}")
    return value


def _timestamp_ms(value: Any, *, field: str) -> datetime:
    milliseconds = _decimal(value, field=field)
    if milliseconds != milliseconds.to_integral_value() or milliseconds <= 0:
        raise SmartMoneyMonitorError(f"SMART_MONEY_FIELD_INVALID_{field.upper()}")
    try:
        return datetime.fromtimestamp(float(milliseconds / 1000), tz=UTC)
    except (OverflowError, OSError, ValueError):
        raise SmartMoneyMonitorError(
            f"SMART_MONEY_FIELD_INVALID_{field.upper()}"
        ) from None


def parse_overview(payload: Any, *, symbol: str) -> SmartMoneyOverview:
    data = _bapi_data(payload)
    if not isinstance(data, dict) or set(data) != OVERVIEW_KEYS:
        raise SmartMoneyMonitorError("SMART_MONEY_SCHEMA_CHANGED")
    if data.get("symbol") != symbol:
        raise SmartMoneyMonitorError("SMART_MONEY_SYMBOL_MISMATCH")
    long_traders = _integer(data["longTraders"], field="long_traders")
    short_traders = _integer(data["shortTraders"], field="short_traders")
    long_whales = _integer(data["longWhales"], field="long_whales")
    short_whales = _integer(data["shortWhales"], field="short_whales")
    if long_whales > long_traders or short_whales > short_traders:
        raise SmartMoneyMonitorError("SMART_MONEY_WHALE_SUBSET_INVALID")
    average_price_keys = {
        "longTradersAvgEntryPrice",
        "longWhalesAvgEntryPrice",
        "shortTradersAvgEntryPrice",
        "shortWhalesAvgEntryPrice",
    }
    for key in OVERVIEW_KEYS - {
        "symbol",
        "updateTime",
        "longTraders",
        "shortTraders",
        "longWhales",
        "shortWhales",
        "longProfitTraders",
        "shortProfitTraders",
        "longProfitWhales",
        "shortProfitWhales",
        "totalTraders",
        *average_price_keys,
    }:
        _decimal(data[key], field=key)
    for key in average_price_keys:
        if data[key] is not None:
            _decimal(data[key], field=key)
    for key in (
        "longProfitTraders",
        "shortProfitTraders",
        "longProfitWhales",
        "shortProfitWhales",
        "totalTraders",
    ):
        _integer(data[key], field=key)
    return SmartMoneyOverview(
        updated_at=_timestamp_ms(data["updateTime"], field="overview_update_time"),
        long_traders=long_traders,
        short_traders=short_traders,
        long_whales=long_whales,
        short_whales=short_whales,
    )


def parse_stats(payload: Any) -> SmartMoneyStats:
    data = _bapi_data(payload)
    if not isinstance(data, dict) or set(data) != STATS_KEYS:
        raise SmartMoneyMonitorError("SMART_MONEY_SCHEMA_CHANGED")
    stats = SmartMoneyStats(
        long_positions=_decimal(data["longPositions"], field="long_positions"),
        short_positions=_decimal(data["shortPositions"], field="short_positions"),
        long_whale_positions=_decimal(
            data["longWhalePositions"], field="long_whale_positions"
        ),
        short_whale_positions=_decimal(
            data["shortWhalePositions"], field="short_whale_positions"
        ),
        long_traders=_integer(data["longTraders"], field="long_traders"),
        short_traders=_integer(data["shortTraders"], field="short_traders"),
        long_whales=_integer(data["longWhales"], field="long_whales"),
        short_whales=_integer(data["shortWhales"], field="short_whales"),
    )
    _decimal(data["longQty"], field="long_qty")
    _decimal(data["shortQty"], field="short_qty")
    _decimal(data["longWhaleQty"], field="long_whale_qty")
    _decimal(data["shortWhaleQty"], field="short_whale_qty")
    tolerance = Decimal("0.01")
    if (
        stats.long_whale_positions > stats.long_positions + tolerance
        or stats.short_whale_positions > stats.short_positions + tolerance
        or stats.long_whales > stats.long_traders
        or stats.short_whales > stats.short_traders
    ):
        raise SmartMoneyMonitorError("SMART_MONEY_WHALE_SUBSET_INVALID")
    return stats


def parse_latest_trade_time(payload: Any, *, time_range: TimeRange) -> datetime | None:
    data = _bapi_data(payload, details=True)
    if not isinstance(data, list):
        raise SmartMoneyMonitorError("SMART_MONEY_SCHEMA_CHANGED")
    observed: list[datetime] = []
    for row in data:
        if not isinstance(row, dict) or set(row) != DETAIL_KEYS:
            raise SmartMoneyMonitorError("SMART_MONEY_SCHEMA_CHANGED")
        if row.get("timeRange") != time_range:
            raise SmartMoneyMonitorError("SMART_MONEY_TIME_RANGE_MISMATCH")
        if row.get("side") not in {"BUY", "SELL"}:
            raise SmartMoneyMonitorError("SMART_MONEY_DETAIL_SIDE_INVALID")
        observed.append(
            _timestamp_ms(row["lastTradeTime"], field="last_trade_time")
        )
    return max(observed) if observed else None


def parse_market_context(
    oi_payload: Any,
    premium_payload: Any,
    *,
    symbol: str,
) -> UsdmMarketContext:
    if (
        not isinstance(oi_payload, dict)
        or not {"openInterest", "symbol", "time"}.issubset(oi_payload)
        or oi_payload.get("symbol") != symbol
    ):
        raise SmartMoneyMonitorError("SMART_MONEY_OI_SCHEMA_INVALID")
    if (
        not isinstance(premium_payload, dict)
        or not {"lastFundingRate", "markPrice", "symbol", "time"}.issubset(
            premium_payload
        )
        or premium_payload.get("symbol") != symbol
    ):
        raise SmartMoneyMonitorError("SMART_MONEY_PREMIUM_SCHEMA_INVALID")
    return UsdmMarketContext(
        open_interest=_decimal(
            oi_payload["openInterest"],
            field="open_interest",
            allow_string=True,
            positive=True,
        ),
        open_interest_time=_timestamp_ms(oi_payload["time"], field="open_interest_time"),
        mark_price=_decimal(
            premium_payload["markPrice"],
            field="mark_price",
            allow_string=True,
            positive=True,
        ),
        funding_rate=_decimal(
            premium_payload["lastFundingRate"],
            field="last_funding_rate",
            allow_string=True,
            allow_negative=True,
        ),
        mark_time=_timestamp_ms(premium_payload["time"], field="mark_time"),
    )


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    return numerator / denominator if denominator > 0 else None


class BinanceSmartMoneyMonitor:
    monitor_id = "binance-usdm-smart-money"
    display_name = "Binance USDⓈ-M 聪明钱"
    description = (
        "网页内部接口的前向研究记录；当前证据不足，不是正式 API、方向建议或跟单信号。"
    )

    def __init__(
        self,
        settings: BinanceSmartMoneySettings,
        *,
        client: BinanceSmartMoneyClient | None = None,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.jitter_seconds = settings.jitter_seconds
        self.client = client or BinanceSmartMoneyClient(
            timeout_seconds=settings.timeout_seconds,
            proxy_url=settings.proxy_url,
        )
        self.view = MonitorView(
            filters=(
                ViewFilter(
                    key="symbol",
                    label="合约",
                    default=settings.symbols[0],
                    choices=tuple(FilterChoice(symbol, symbol) for symbol in settings.symbols),
                ),
                ViewFilter(
                    key="time_range",
                    label="资金流窗口",
                    default=settings.time_ranges[0],
                    choices=tuple(
                        FilterChoice(value, "30 分钟" if value == "30m" else "1 小时")
                        for value in settings.time_ranges
                    ),
                ),
            ),
            columns=(
                ViewColumn("symbol", "合约"),
                ViewColumn("time_range_label", "窗口"),
                ViewColumn("dominant_flow", "净流方向"),
                ViewColumn("flow_imbalance_percent", "流量失衡", "percent"),
                ViewColumn("normalized_flow_percent", "净流 / OI", "percent"),
                ViewColumn("whale_divergence_percent", "巨鲸分歧", "percent"),
                ViewColumn(
                    "long_positions",
                    "多头流量 (USD)",
                    "number",
                    priority="secondary",
                    maximum_fraction_digits=0,
                    use_grouping=True,
                ),
                ViewColumn(
                    "short_positions",
                    "空头流量 (USD)",
                    "number",
                    priority="secondary",
                    maximum_fraction_digits=0,
                    use_grouping=True,
                ),
                ViewColumn(
                    "mark_price",
                    "标记价",
                    "number",
                    priority="secondary",
                    minimum_fraction_digits=2,
                    maximum_fraction_digits=2,
                    use_grouping=True,
                ),
                ViewColumn(
                    "open_interest",
                    "OI (币)",
                    "number",
                    priority="secondary",
                    maximum_fraction_digits=3,
                    use_grouping=True,
                ),
                ViewColumn(
                    "last_funding_rate_percent",
                    "资金费率",
                    "percent",
                    priority="secondary",
                ),
                ViewColumn("latest_trade_at", "最新流量时间", "time"),
                ViewColumn(
                    "observed_at",
                    "采集完成",
                    "time",
                    priority="secondary",
                ),
            ),
            table_title="最新聪明钱特征",
            chart_title="OI 标准化净流历史 (%)",
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
        artifacts: list[CollectionArtifact] = []
        issues: list[CollectionIssue] = []
        samples: list[MetricSample] = []
        try:
            self.client.ensure_available()
        except SmartMoneyMonitorError as exc:
            return CollectionBatch(
                samples=(),
                issues=(CollectionIssue("monitor", exc.reason_code),),
            )

        throttled = False
        for symbol in self.settings.symbols:
            try:
                overview_response = self._request(
                    artifacts,
                    artifact_key=f"{symbol}:overview",
                    base=BINANCE_WEB_BASE,
                    path="/bapi/futures/v1/public/future/smart-money/signal/overview",
                    params=(("symbol", symbol),),
                )
                overview = parse_overview(overview_response.payload, symbol=symbol)
                oi_response = self._request(
                    artifacts,
                    artifact_key=f"{symbol}:open-interest",
                    base=BINANCE_USDM_BASE,
                    path="/fapi/v1/openInterest",
                    params=(("symbol", symbol),),
                )
                premium_response = self._request(
                    artifacts,
                    artifact_key=f"{symbol}:premium-index",
                    base=BINANCE_USDM_BASE,
                    path="/fapi/v1/premiumIndex",
                    params=(("symbol", symbol),),
                )
                market = parse_market_context(
                    oi_response.payload,
                    premium_response.payload,
                    symbol=symbol,
                )
                common_completed_at = max(
                    overview_response.artifact.response_completed_at,
                    oi_response.artifact.response_completed_at,
                    premium_response.artifact.response_completed_at,
                )
                self._validate_market_freshness(market, common_completed_at)
                overview_age = (
                    common_completed_at - overview.updated_at
                ).total_seconds()
                overview_fresh = overview_age <= self.settings.overview_stale_seconds
                if not overview_fresh:
                    issues.append(
                        CollectionIssue(symbol, "SMART_MONEY_OVERVIEW_STALE")
                    )
            except SmartMoneyMonitorError as exc:
                if exc.artifact is not None:
                    artifacts.append(exc.artifact)
                issues.append(CollectionIssue(symbol, exc.reason_code))
                if exc.throttled:
                    throttled = True
                    break
                continue

            for time_range in self.settings.time_ranges:
                scope = f"{symbol}:{time_range}"
                try:
                    stats_response = self._request(
                        artifacts,
                        artifact_key=f"{scope}:stats",
                        base=BINANCE_WEB_BASE,
                        path=(
                            "/bapi/futures/v1/public/future/smart-money/"
                            "signal/details/stats"
                        ),
                        params=(("symbol", symbol), ("timeRange", time_range)),
                    )
                    stats = parse_stats(stats_response.payload)
                    details_response = self._request(
                        artifacts,
                        artifact_key=f"{scope}:details",
                        base=BINANCE_WEB_BASE,
                        path=(
                            "/bapi/futures/v1/public/future/smart-money/"
                            "signal/details/list"
                        ),
                        params=(
                            ("symbol", symbol),
                            ("page", "1"),
                            ("rows", "1"),
                            ("timeRange", time_range),
                            ("sortingField", "TIME"),
                            ("sortingOrder", "DESC"),
                        ),
                    )
                    latest_trade_at = parse_latest_trade_time(
                        details_response.payload, time_range=time_range
                    )
                    observed_at = max(
                        common_completed_at,
                        stats_response.artifact.response_completed_at,
                        details_response.artifact.response_completed_at,
                    )
                    self._validate_flow_freshness(
                        stats,
                        latest_trade_at,
                        observed_at=observed_at,
                        time_range=time_range,
                    )
                    samples.append(
                        self._sample(
                            symbol=symbol,
                            time_range=time_range,
                            stats=stats,
                            overview=overview,
                            overview_fresh=overview_fresh,
                            overview_age_seconds=overview_age,
                            market=market,
                            latest_trade_at=latest_trade_at,
                            observed_at=observed_at,
                        )
                    )
                except SmartMoneyMonitorError as exc:
                    if exc.artifact is not None:
                        artifacts.append(exc.artifact)
                    issues.append(CollectionIssue(scope, exc.reason_code))
                    if exc.throttled:
                        throttled = True
                        break
                    if exc.reason_code in {
                        "SMART_MONEY_SCHEMA_CHANGED",
                        "SMART_MONEY_BUSINESS_RESPONSE_FAILED",
                    }:
                        break
            if throttled:
                break

        if not throttled and samples:
            self.client.reset_throttle_backoff()
        if not samples and not issues:
            issues.append(CollectionIssue("monitor", "NO_SAMPLES_RETURNED"))
        return CollectionBatch(
            samples=tuple(samples),
            issues=tuple(issues),
            artifacts=tuple(artifacts),
        )

    def _request(
        self,
        artifacts: list[CollectionArtifact],
        *,
        artifact_key: str,
        base: str,
        path: str,
        params: tuple[tuple[str, str], ...],
    ) -> RecordedJsonResponse:
        response = self.client.get_json(
            artifact_key=artifact_key,
            base=base,
            path=path,
            params=params,
        )
        artifacts.append(response.artifact)
        return response

    def _validate_market_freshness(
        self,
        market: UsdmMarketContext,
        observed_at: datetime,
    ) -> None:
        for timestamp, reason_code in (
            (market.open_interest_time, "SMART_MONEY_OI_STALE"),
            (market.mark_time, "SMART_MONEY_MARK_PRICE_STALE"),
        ):
            age = (observed_at - timestamp).total_seconds()
            if age > self.settings.market_stale_seconds or age < -120:
                raise SmartMoneyMonitorError(reason_code)

    @staticmethod
    def _validate_flow_freshness(
        stats: SmartMoneyStats,
        latest_trade_at: datetime | None,
        *,
        observed_at: datetime,
        time_range: TimeRange,
    ) -> None:
        total_flow = stats.long_positions + stats.short_positions
        if latest_trade_at is None:
            if total_flow > 0:
                raise SmartMoneyMonitorError("SMART_MONEY_DETAILS_EMPTY_WITH_FLOW")
            return
        age = (observed_at - latest_trade_at).total_seconds()
        if age < -120:
            raise SmartMoneyMonitorError("SMART_MONEY_FLOW_TIME_IN_FUTURE")
        if age > TIME_RANGE_SECONDS[time_range] + 120 and total_flow > 0:
            raise SmartMoneyMonitorError("SMART_MONEY_FLOW_TIMESTAMP_STALE")

    @staticmethod
    def _sample(
        *,
        symbol: str,
        time_range: TimeRange,
        stats: SmartMoneyStats,
        overview: SmartMoneyOverview,
        overview_fresh: bool,
        overview_age_seconds: float,
        market: UsdmMarketContext,
        latest_trade_at: datetime | None,
        observed_at: datetime,
    ) -> MetricSample:
        signed_flow = stats.long_positions - stats.short_positions
        total_flow = stats.long_positions + stats.short_positions
        flow_imbalance = _ratio(signed_flow, total_flow)
        oi_notional = market.open_interest * market.mark_price
        normalized_flow = _ratio(signed_flow, oi_notional)

        non_whale_long = max(
            Decimal(0), stats.long_positions - stats.long_whale_positions
        )
        non_whale_short = max(
            Decimal(0), stats.short_positions - stats.short_whale_positions
        )
        whale_imbalance = _ratio(
            stats.long_whale_positions - stats.short_whale_positions,
            stats.long_whale_positions + stats.short_whale_positions,
        )
        non_whale_imbalance = _ratio(
            non_whale_long - non_whale_short,
            non_whale_long + non_whale_short,
        )
        whale_divergence = (
            whale_imbalance - non_whale_imbalance
            if whale_imbalance is not None and non_whale_imbalance is not None
            else None
        )
        normalized_flow_percent = (
            normalized_flow * Decimal(100) if normalized_flow is not None else Decimal(0)
        )
        range_label = "30 分钟" if time_range == "30m" else "1 小时"
        dominant_flow = (
            "多头净流入"
            if signed_flow > 0
            else "空头净流入"
            if signed_flow < 0
            else "多空平衡"
        )
        missing_reasons: dict[str, str] = {}
        if whale_divergence is None:
            missing_reasons["whale_divergence_percent"] = (
                "巨鲸或非巨鲸资金流为空，无法计算分歧；未使用替代值。"
            )
        if latest_trade_at is None:
            missing_reasons["latest_trade_at"] = (
                "窗口内没有可核对的交易明细；未使用替代时间。"
            )
        return MetricSample(
            series_key=f"{symbol}|{time_range}|normalized-flow",
            entity_key=symbol,
            observed_at=observed_at,
            value_text=decimal_text(normalized_flow_percent) or "0",
            unit="% OI",
            payload={
                "symbol": symbol,
                "time_range": time_range,
                "time_range_label": range_label,
                "series_label": f"{symbol} · {range_label} · 净流/OI (%)",
                "dominant_flow": dominant_flow,
                "long_positions": decimal_text(stats.long_positions),
                "short_positions": decimal_text(stats.short_positions),
                "signed_flow": decimal_text(signed_flow),
                "flow_imbalance": decimal_text(flow_imbalance),
                "flow_imbalance_percent": decimal_text(
                    flow_imbalance * Decimal(100)
                    if flow_imbalance is not None
                    else None
                ),
                "normalized_flow": decimal_text(normalized_flow),
                "normalized_flow_percent": decimal_text(normalized_flow_percent),
                "open_interest": decimal_text(market.open_interest),
                "open_interest_notional": decimal_text(oi_notional),
                "mark_price": decimal_text(market.mark_price),
                "last_funding_rate": decimal_text(market.funding_rate),
                "last_funding_rate_percent": decimal_text(
                    market.funding_rate * Decimal(100)
                ),
                "whale_imbalance": decimal_text(whale_imbalance),
                "non_whale_imbalance": decimal_text(non_whale_imbalance),
                "whale_divergence": decimal_text(whale_divergence),
                "whale_divergence_percent": decimal_text(
                    whale_divergence * Decimal(100)
                    if whale_divergence is not None
                    else None
                ),
                "long_traders": stats.long_traders,
                "short_traders": stats.short_traders,
                "long_whales": stats.long_whales,
                "short_whales": stats.short_whales,
                "overview_long_traders": overview.long_traders,
                "overview_short_traders": overview.short_traders,
                "overview_long_whales": overview.long_whales,
                "overview_short_whales": overview.short_whales,
                "overview_updated_at": iso_utc(overview.updated_at),
                "overview_age_seconds": round(overview_age_seconds, 3),
                "overview_freshness": "新鲜" if overview_fresh else "陈旧，不用于特征",
                "latest_trade_at": (
                    iso_utc(latest_trade_at) if latest_trade_at is not None else None
                ),
                "open_interest_time": iso_utc(market.open_interest_time),
                "mark_time": iso_utc(market.mark_time),
                "observed_at": iso_utc(observed_at),
                "missing_reasons": missing_reasons,
                "source_kind": "BINANCE_USDM_WEB_INTERNAL",
                "research_conclusion": "INSUFFICIENT_EVIDENCE",
            },
        )


__all__ = [
    "BinanceSmartMoneyClient",
    "BinanceSmartMoneyMonitor",
    "BinanceSmartMoneySettings",
    "RecordedJsonResponse",
    "SmartMoneyMonitorError",
    "parse_latest_trade_time",
    "parse_market_context",
    "parse_overview",
    "parse_stats",
    "schema_hash",
]
