"""BTC relationship and relative-strength monitor using closed Binance Spot bars."""

from __future__ import annotations

import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd

from halpha_monitor.contracts import (
    CollectionBatch,
    CollectionIssue,
    MetricSample,
    MonitorView,
    ViewColumn,
)
from halpha_monitor.monitors.btc_relationship_symbols import (
    DEFAULT_SYMBOLS,
    UNIVERSE_SNAPSHOT_AT,
    UNIVERSE_SOURCE_SHA256,
)
from halpha_monitor.store import iso_utc


BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
REFERENCE_SYMBOL = "BTCUSDT"
DAY_MS = 86_400_000
FETCH_DAYS = 800
MAIN_WINDOW = 365
MINIMUM_OBSERVATIONS = 120
SUB_WINDOW = 180
MINIMUM_SUB_OBSERVATIONS = 90


def utc_now() -> datetime:
    return datetime.now(UTC)


def latest_closed_cutoff(now: datetime | None = None) -> datetime:
    current = (now or utc_now()).astimezone(UTC)
    return current.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        milliseconds=1
    )


def float_text(value: float | None) -> str | None:
    if value is None or not math.isfinite(value):
        return None
    return format(value, ".12g")


def normalize_klines(rows: Any, cutoff_ms: int) -> pd.DataFrame:
    if not isinstance(rows, list):
        raise ValueError("BTC_RELATIONSHIP_SCHEMA_CHANGED")
    normalized: list[tuple[int, int, float]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 7:
            raise ValueError("BTC_RELATIONSHIP_SCHEMA_CHANGED")
        try:
            open_time = int(row[0])
            close = float(row[4])
            close_time = int(row[6])
        except (TypeError, ValueError, OverflowError):
            continue
        if (
            open_time < 0
            or close_time < open_time
            or close_time > cutoff_ms
            or not math.isfinite(close)
            or close <= 0
        ):
            continue
        normalized.append((open_time, close_time, close))
    frame = pd.DataFrame(
        normalized,
        columns=["open_time_ms", "close_time_ms", "close"],
    )
    if frame.empty:
        return frame
    return (
        frame.drop_duplicates(subset=["open_time_ms"], keep="last")
        .sort_values("open_time_ms")
        .reset_index(drop=True)
    )


def _read_cache(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame(columns=["open_time_ms", "close_time_ms", "close"])
    try:
        frame = pd.read_csv(path)
    except (OSError, ValueError):
        return pd.DataFrame(columns=["open_time_ms", "close_time_ms", "close"])
    required = {"open_time_ms", "close_time_ms", "close"}
    if not required.issubset(frame.columns):
        return pd.DataFrame(columns=["open_time_ms", "close_time_ms", "close"])
    for column in required:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.dropna(subset=list(required))
        .loc[lambda value: value["close"] > 0]
        .drop_duplicates(subset=["open_time_ms"], keep="last")
        .sort_values("open_time_ms")
        .reset_index(drop=True)
    )


def _write_cache(frame: pd.DataFrame, path: Path) -> None:
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


@dataclass(frozen=True)
class DailySeriesResult:
    symbol: str
    status: str
    frame: pd.DataFrame
    latest_close_at: datetime | None
    acquired_at: datetime | None
    reason_code: str | None = None

    @property
    def current(self) -> bool:
        return self.status in {"FETCHED", "CACHE_CURRENT", "CACHE_CURRENT_AFTER_ERROR"}


class DailySeriesProvider(Protocol):
    def fetch(self, symbol: str, cutoff: datetime) -> DailySeriesResult: ...


class BinanceSpotDailyClient:
    """Bounded official public-data client with one durable normalized cache."""

    def __init__(
        self,
        cache_root: Path,
        *,
        timeout_seconds: float = 10,
        attempts: int = 2,
        open_url: Callable[..., Any] = urlopen,
        monotonic: Callable[[], float] = time.monotonic,
        wall_now: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0 or attempts < 1:
            raise ValueError("BTC_RELATIONSHIP_CLIENT_CONFIGURATION_INVALID")
        self.cache_root = cache_root.resolve()
        self.timeout_seconds = timeout_seconds
        self.attempts = attempts
        self._open_url = open_url
        self._monotonic = monotonic
        self._wall_now = wall_now
        self._sleep = sleep
        self._throttle_lock = threading.Lock()
        self._throttle_until = 0.0
        self._throttle_failures = 0

    def _throttle_active(self) -> bool:
        with self._throttle_lock:
            return self._monotonic() < self._throttle_until

    def _open_throttle_backoff(self, error: HTTPError) -> None:
        retry_after: float | None = None
        raw_retry_after = error.headers.get("Retry-After") if error.headers else None
        if raw_retry_after:
            try:
                retry_after = max(0.0, float(raw_retry_after))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(raw_retry_after)
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
            # Several symbol workers can already be in flight when the first
            # 429 arrives. Treat their responses as one rate-limit incident;
            # otherwise a six-worker refresh can inflate a 60-second fallback
            # into a multi-minute cooldown before any retry is attempted.
            if now >= self._throttle_until:
                self._throttle_failures += 1
            exponential = min(
                3600.0,
                60.0 * (2 ** min(self._throttle_failures - 1, 6)),
            )
            delay = max(exponential, retry_after or 0.0)
            self._throttle_until = max(
                self._throttle_until,
                now + delay,
            )

    def _record_success(self) -> None:
        with self._throttle_lock:
            if self._monotonic() >= self._throttle_until:
                self._throttle_until = 0.0
                self._throttle_failures = 0

    def fetch(self, symbol: str, cutoff: datetime) -> DailySeriesResult:
        cache_path = self.cache_root / f"{symbol}.csv.gz"
        current = _read_cache(cache_path)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        current_latest_ms = (
            int(current["close_time_ms"].max()) if not current.empty else None
        )
        if current_latest_ms is not None and current_latest_ms >= cutoff_ms:
            return DailySeriesResult(
                symbol=symbol,
                status="CACHE_CURRENT",
                frame=current,
                latest_close_at=datetime.fromtimestamp(
                    current_latest_ms / 1000, tz=UTC
                ),
                acquired_at=datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC),
            )

        earliest_ms = cutoff_ms - FETCH_DAYS * DAY_MS
        start_ms = (
            earliest_ms
            if current.empty
            else max(earliest_ms, int(current["open_time_ms"].max()) - 3 * DAY_MS)
        )
        params = {
            "symbol": symbol,
            "interval": "1d",
            "startTime": start_ms,
            "endTime": cutoff_ms,
            "timeZone": "0",
            "limit": 1000,
        }
        last_reason = "BTC_RELATIONSHIP_UPSTREAM_UNAVAILABLE"
        for attempt in range(self.attempts):
            if self._throttle_active():
                last_reason = "BTC_RELATIONSHIP_HTTP_THROTTLED"
                break
            retryable = False
            try:
                request = Request(
                    f"{BINANCE_KLINES_URL}?{urlencode(params)}",
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "Halpha-Monitor/1.0",
                    },
                )
                with self._open_url(request, timeout=self.timeout_seconds) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                normalized = normalize_klines(payload, cutoff_ms)
                combined = (
                    normalized
                    if current.empty
                    else pd.concat([current, normalized], ignore_index=True)
                )
                combined = (
                    combined.drop_duplicates(subset=["open_time_ms"], keep="last")
                    .loc[lambda value: value["close_time_ms"] <= cutoff_ms]
                    .sort_values("open_time_ms")
                    .tail(FETCH_DAYS + 5)
                    .reset_index(drop=True)
                )
                if combined.empty:
                    raise ValueError("BTC_RELATIONSHIP_NO_CLOSED_BARS")
                latest_ms = int(combined["close_time_ms"].max())
                if latest_ms < cutoff_ms:
                    return DailySeriesResult(
                        symbol=symbol,
                        status="STALE",
                        frame=combined,
                        latest_close_at=datetime.fromtimestamp(latest_ms / 1000, tz=UTC),
                        acquired_at=utc_now(),
                        reason_code="BTC_RELATIONSHIP_SOURCE_STALE",
                    )
                _write_cache(combined, cache_path)
                self._record_success()
                return DailySeriesResult(
                    symbol=symbol,
                    status="FETCHED",
                    frame=combined,
                    latest_close_at=datetime.fromtimestamp(latest_ms / 1000, tz=UTC),
                    acquired_at=utc_now(),
                )
            except HTTPError as exc:
                if exc.code in {418, 429}:
                    self._open_throttle_backoff(exc)
                    last_reason = "BTC_RELATIONSHIP_HTTP_THROTTLED"
                    break
                last_reason = "BTC_RELATIONSHIP_UPSTREAM_HTTP_ERROR"
                retryable = 500 <= exc.code < 600
            except (URLError, TimeoutError):
                last_reason = "BTC_RELATIONSHIP_UPSTREAM_UNAVAILABLE"
                retryable = True
            except json.JSONDecodeError:
                last_reason = "BTC_RELATIONSHIP_RESPONSE_INVALID"
                retryable = True
            except ValueError as exc:
                last_reason = str(exc)
            if not retryable or attempt + 1 >= self.attempts:
                break
            self._sleep(0.5 * (2**attempt))

        if current_latest_ms is not None and current_latest_ms >= cutoff_ms:
            return DailySeriesResult(
                symbol=symbol,
                status="CACHE_CURRENT_AFTER_ERROR",
                frame=current,
                latest_close_at=datetime.fromtimestamp(
                    current_latest_ms / 1000, tz=UTC
                ),
                acquired_at=datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC),
                reason_code=last_reason,
            )
        return DailySeriesResult(
            symbol=symbol,
            status="FAILED" if current.empty else "STALE",
            frame=current,
            latest_close_at=(
                datetime.fromtimestamp(current_latest_ms / 1000, tz=UTC)
                if current_latest_ms is not None
                else None
            ),
            acquired_at=(
                datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC)
                if cache_path.is_file()
                else None
            ),
            reason_code=last_reason,
        )


def _price_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    index = pd.to_datetime(frame["open_time_ms"], unit="ms", utc=True)
    values = pd.to_numeric(frame["close"], errors="coerce").to_numpy(dtype=float)
    series = pd.Series(values, index=index).replace([np.inf, -np.inf], np.nan)
    return series.dropna().loc[lambda value: value > 0].sort_index()


def aligned_daily_returns(
    asset_price: pd.Series,
    btc_price: pd.Series,
) -> pd.DataFrame:
    prices = pd.concat(
        {"asset": asset_price, "btc": btc_price},
        axis=1,
        join="inner",
    ).dropna()
    log_returns = np.log(prices).diff()
    consecutive = prices.index.to_series().diff().dt.total_seconds().eq(86_400)
    return log_returns[consecutive].replace([np.inf, -np.inf], np.nan).dropna()


def _correlation(values: pd.DataFrame) -> float:
    if (
        len(values) < 3
        or values["asset"].std(ddof=1) == 0
        or values["btc"].std(ddof=1) == 0
    ):
        return math.nan
    return float(values["asset"].corr(values["btc"], method="pearson"))


def analyze_pair(asset_price: pd.Series, btc_price: pd.Series) -> dict[str, Any]:
    returns = aligned_daily_returns(asset_price, btc_price)
    main = returns.tail(MAIN_WINDOW)
    if len(main) < MINIMUM_OBSERVATIONS:
        return {
            "status": "INSUFFICIENT_SAMPLE",
            "n_obs": int(len(main)),
            "last_return_at": main.index.max().to_pydatetime() if len(main) else None,
        }
    pearson = _correlation(main)
    ranked = main.rank(method="average")
    spearman = _correlation(ranked)
    btc_variance = float(main["btc"].var(ddof=1))
    beta = (
        float(main[["asset", "btc"]].cov(ddof=1).loc["asset", "btc"])
        / btc_variance
        if btc_variance > 0
        else math.nan
    )
    residual = main["asset"] - (
        float(main["asset"].mean()) - beta * float(main["btc"].mean())
    ) - beta * main["btc"]
    recent = returns.tail(SUB_WINDOW)
    prior = returns.iloc[-2 * SUB_WINDOW : -SUB_WINDOW]
    recent_corr = (
        _correlation(recent)
        if len(recent) >= MINIMUM_SUB_OBSERVATIONS
        else math.nan
    )
    prior_corr = (
        _correlation(prior)
        if len(prior) >= MINIMUM_SUB_OBSERVATIONS
        else math.nan
    )
    signs = [np.sign(value) for value in (pearson, spearman, recent_corr, prior_corr)]
    stable_sign = bool(
        all(math.isfinite(float(value)) and value != 0 and value == signs[0] for value in signs)
    )
    result: dict[str, Any] = {
        "status": "ANALYZED",
        "n_obs": int(len(main)),
        "last_return_at": main.index.max().to_pydatetime(),
        "pearson": pearson,
        "spearman": spearman,
        "beta": beta,
        "r_squared": pearson**2 if math.isfinite(pearson) else math.nan,
        "volatility_ratio": float(
            main["asset"].std(ddof=1) / main["btc"].std(ddof=1)
        ),
        "residual_volatility_annualized": float(
            residual.std(ddof=1) * math.sqrt(365)
        ),
        "recent_180_pearson": recent_corr,
        "prior_180_pearson": prior_corr,
        "stable_sign": stable_sign,
    }
    for horizon in (7, 30, 90):
        result[f"relative_strength_{horizon}d"] = (
            float(math.expm1((returns["asset"] - returns["btc"]).tail(horizon).sum()))
            if len(returns) >= horizon
            else math.nan
        )
    return result


def association_band(pearson: float) -> str:
    magnitude = abs(pearson)
    if magnitude >= 0.70:
        return "很强"
    if magnitude >= 0.50:
        return "强"
    if magnitude >= 0.30:
        return "中等"
    return "弱"


@dataclass(frozen=True)
class BinanceBtcRelationshipSettings:
    cache_root: Path
    interval_seconds: float = 3600
    jitter_seconds: float = 120
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    timeout_seconds: float = 10
    workers: int = 8

    def __post_init__(self) -> None:
        if self.interval_seconds < 60 or self.jitter_seconds < 0:
            raise ValueError("BTC_RELATIONSHIP_INTERVAL_INVALID")
        if not 1 <= self.workers <= 12:
            raise ValueError("BTC_RELATIONSHIP_WORKERS_INVALID")
        if REFERENCE_SYMBOL not in self.symbols:
            raise ValueError("BTC_RELATIONSHIP_REFERENCE_REQUIRED")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("BTC_RELATIONSHIP_SYMBOL_DUPLICATE")
        if any(not symbol.endswith("USDT") for symbol in self.symbols):
            raise ValueError("BTC_RELATIONSHIP_SYMBOL_INVALID")


class BinanceBtcRelationshipMonitor:
    monitor_id = "binance-btc-relationship"
    display_name = "BTC 市场关联与相对强弱"
    description = (
        "固定 2026-07-21 Binance Spot USDT 加密资产范围；"
        "基于闭合 UTC 日线计算与 BTC 的关联和 7/30/90 日相对强弱。"
        "这是历史关联，不是预测、交易信号或 Alpha。"
    )
    default_enabled = False

    def __init__(
        self,
        settings: BinanceBtcRelationshipSettings,
        client: DailySeriesProvider | None = None,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.jitter_seconds = settings.jitter_seconds
        self.client = client or BinanceSpotDailyClient(
            settings.cache_root,
            timeout_seconds=settings.timeout_seconds,
        )
        self.view = MonitorView(
            filters=(),
            columns=(
                ViewColumn("symbol", "币种"),
                ViewColumn("data_state", "数据状态"),
                ViewColumn(
                    "pearson",
                    "Pearson",
                    "number",
                    minimum_fraction_digits=4,
                    maximum_fraction_digits=4,
                ),
                ViewColumn(
                    "spearman",
                    "Spearman",
                    "number",
                    priority="secondary",
                    minimum_fraction_digits=4,
                    maximum_fraction_digits=4,
                ),
                ViewColumn(
                    "beta",
                    "BTC Beta",
                    "number",
                    minimum_fraction_digits=4,
                    maximum_fraction_digits=4,
                ),
                ViewColumn(
                    "r_squared",
                    "R²",
                    "number",
                    priority="secondary",
                    minimum_fraction_digits=4,
                    maximum_fraction_digits=4,
                ),
                ViewColumn(
                    "volatility_ratio",
                    "波动倍数",
                    "number",
                    priority="secondary",
                    minimum_fraction_digits=3,
                    maximum_fraction_digits=3,
                ),
                ViewColumn(
                    "relative_strength_7d_percent",
                    "相对 BTC 7日",
                    "percent",
                ),
                ViewColumn(
                    "relative_strength_30d_percent",
                    "相对 BTC 30日",
                    "percent",
                ),
                ViewColumn(
                    "relative_strength_90d_percent",
                    "相对 BTC 90日",
                    "percent",
                    priority="secondary",
                ),
                ViewColumn(
                    "data_cutoff_at",
                    "日线截止",
                    "time",
                ),
                ViewColumn(
                    "observed_at",
                    "采集完成",
                    "time",
                    priority="secondary",
                ),
            ),
            table_title="BTC 关联与相对强弱",
            chart_title="BTC Pearson 相关性历史",
        )

    def collect(self) -> CollectionBatch:
        cutoff = latest_closed_cutoff()
        reference = self.client.fetch(REFERENCE_SYMBOL, cutoff)
        if not reference.current:
            return CollectionBatch(
                samples=(),
                issues=(
                    CollectionIssue(
                        REFERENCE_SYMBOL,
                        reference.reason_code
                        or "BTC_RELATIONSHIP_REFERENCE_UNAVAILABLE",
                    ),
                ),
            )
        issues: list[CollectionIssue] = []
        if reference.reason_code is not None:
            issues.append(CollectionIssue(REFERENCE_SYMBOL, reference.reason_code))
        fetched: dict[str, DailySeriesResult] = {REFERENCE_SYMBOL: reference}
        objects = [
            symbol for symbol in self.settings.symbols if symbol != REFERENCE_SYMBOL
        ]
        with ThreadPoolExecutor(max_workers=self.settings.workers) as pool:
            futures = {
                pool.submit(self.client.fetch, symbol, cutoff): symbol
                for symbol in objects
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    fetched[symbol] = future.result()
                except Exception:
                    fetched[symbol] = DailySeriesResult(
                        symbol=symbol,
                        status="FAILED",
                        frame=pd.DataFrame(),
                        latest_close_at=None,
                        acquired_at=None,
                        reason_code="BTC_RELATIONSHIP_COLLECTION_FAILED",
                    )

        observed_at = utc_now()
        btc_price = _price_series(reference.frame)
        samples: list[MetricSample] = []
        valid_count = 0
        for symbol in objects:
            result = fetched[symbol]
            if result.reason_code is not None:
                issues.append(CollectionIssue(symbol, result.reason_code))
            if not result.current:
                samples.append(self._missing_sample(symbol, result, observed_at, cutoff))
                continue
            try:
                analysis = analyze_pair(_price_series(result.frame), btc_price)
            except Exception:
                issues.append(
                    CollectionIssue(symbol, "BTC_RELATIONSHIP_COMPUTATION_FAILED")
                )
                samples.append(
                    self._missing_sample(
                        symbol,
                        result,
                        observed_at,
                        cutoff,
                        reason="计算未通过校验；没有展示或替代指标。",
                    )
                )
                continue
            if analysis["status"] != "ANALYZED":
                samples.append(
                    self._analysis_sample(symbol, analysis, result, observed_at, cutoff)
                )
                continue
            valid_count += 1
            samples.append(
                self._analysis_sample(symbol, analysis, result, observed_at, cutoff)
            )

        if valid_count == 0:
            return CollectionBatch(
                samples=(),
                issues=tuple(
                    issues
                    or [
                        CollectionIssue(
                            "monitor",
                            "BTC_RELATIONSHIP_NO_ANALYZED_SYMBOLS",
                        )
                    ]
                ),
            )
        minimum_coverage = max(1, int(len(objects) * 0.8))
        if valid_count < minimum_coverage:
            issues.append(
                CollectionIssue(
                    "monitor",
                    "BTC_RELATIONSHIP_COVERAGE_INSUFFICIENT",
                )
            )
        samples.sort(
            key=lambda sample: (
                sample.payload.get("pearson") is None,
                -abs(float(sample.payload.get("pearson") or 0)),
                sample.entity_key,
            )
        )
        return CollectionBatch(samples=tuple(samples), issues=tuple(issues))

    @staticmethod
    def _base_payload(
        symbol: str,
        result: DailySeriesResult,
        observed_at: datetime,
        cutoff: datetime,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "base_asset": symbol.removesuffix("USDT"),
            "series_label": f"{symbol} · BTC Pearson",
            "observed_at": iso_utc(observed_at),
            "data_cutoff_at": iso_utc(cutoff),
            "source_latest_close_at": (
                iso_utc(result.latest_close_at)
                if result.latest_close_at is not None
                else None
            ),
            "source_acquired_at": (
                iso_utc(result.acquired_at)
                if result.acquired_at is not None
                else None
            ),
            "source_state": result.status,
            "source": "BINANCE_SPOT_PUBLIC_1D",
            "universe_snapshot_at": UNIVERSE_SNAPSHOT_AT,
            "universe_source_sha256": UNIVERSE_SOURCE_SHA256,
            "research_conclusion": "SUPPORTS_WITHIN_SCOPE",
            "interpretation_limit": (
                "历史关联不等于因果、领先关系、预测、策略证据或 Alpha。"
            ),
        }

    def _missing_sample(
        self,
        symbol: str,
        result: DailySeriesResult,
        observed_at: datetime,
        cutoff: datetime,
        *,
        reason: str | None = None,
    ) -> MetricSample:
        explanation = reason or (
            "该币种没有截至当前闭合日线的可用来源数据；"
            "指标保持为空，未使用缓存旧值或任何替代值。"
        )
        fields = (
            "pearson",
            "spearman",
            "beta",
            "r_squared",
            "volatility_ratio",
            "relative_strength_7d_percent",
            "relative_strength_30d_percent",
            "relative_strength_90d_percent",
        )
        return MetricSample(
            series_key=f"{symbol}|pearson",
            entity_key=symbol,
            observed_at=observed_at,
            value_text="",
            unit="CORRELATION",
            payload={
                **self._base_payload(symbol, result, observed_at, cutoff),
                "data_state": "未采集",
                **{field: None for field in fields},
                "missing_reasons": {field: explanation for field in fields},
            },
        )

    def _analysis_sample(
        self,
        symbol: str,
        analysis: dict[str, Any],
        result: DailySeriesResult,
        observed_at: datetime,
        cutoff: datetime,
    ) -> MetricSample:
        if analysis["status"] != "ANALYZED":
            n_obs = int(analysis["n_obs"])
            explanation = (
                f"只有 {n_obs} 个连续日收益观测，少于最低 "
                f"{MINIMUM_OBSERVATIONS} 个；指标保持为空。"
            )
            sample = self._missing_sample(
                symbol,
                result,
                observed_at,
                cutoff,
                reason=explanation,
            )
            sample.payload["data_state"] = f"样本不足（{n_obs}）"
            sample.payload["n_obs"] = n_obs
            return sample

        pearson = float(analysis["pearson"])
        relative_values = {
            key: (
                float(analysis[key]) * 100
                if math.isfinite(float(analysis[key]))
                else None
            )
            for key in (
                "relative_strength_7d",
                "relative_strength_30d",
                "relative_strength_90d",
            )
        }
        missing_reasons: dict[str, str] = {}
        payload = {
            **self._base_payload(symbol, result, observed_at, cutoff),
            "data_state": f"可用 · {association_band(pearson)}关联",
            "n_obs": int(analysis["n_obs"]),
            "pearson": float_text(pearson),
            "spearman": float_text(float(analysis["spearman"])),
            "beta": float_text(float(analysis["beta"])),
            "r_squared": float_text(float(analysis["r_squared"])),
            "volatility_ratio": float_text(float(analysis["volatility_ratio"])),
            "residual_volatility_annualized": float_text(
                float(analysis["residual_volatility_annualized"])
            ),
            "recent_180_pearson": float_text(
                float(analysis["recent_180_pearson"])
            ),
            "prior_180_pearson": float_text(
                float(analysis["prior_180_pearson"])
            ),
            "stable_sign": bool(analysis["stable_sign"]),
            "relative_strength_7d_percent": float_text(
                relative_values["relative_strength_7d"]
            ),
            "relative_strength_30d_percent": float_text(
                relative_values["relative_strength_30d"]
            ),
            "relative_strength_90d_percent": float_text(
                relative_values["relative_strength_90d"]
            ),
        }
        for key in (
            "relative_strength_7d_percent",
            "relative_strength_30d_percent",
            "relative_strength_90d_percent",
        ):
            if payload[key] is None:
                missing_reasons[key] = (
                    "连续收益观测不足以计算该窗口；指标保持为空。"
                )
        payload["missing_reasons"] = missing_reasons
        return MetricSample(
            series_key=f"{symbol}|pearson",
            entity_key=symbol,
            observed_at=observed_at,
            value_text=float_text(pearson) or "",
            unit="CORRELATION",
            payload=payload,
        )


__all__ = [
    "BinanceBtcRelationshipMonitor",
    "BinanceBtcRelationshipSettings",
    "BinanceSpotDailyClient",
    "DailySeriesResult",
    "aligned_daily_returns",
    "analyze_pair",
    "latest_closed_cutoff",
    "normalize_klines",
]
