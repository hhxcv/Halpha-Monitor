"""Binance C2C advertisements normalized to an executable USDT spot basis."""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener

from halpha_monitor.contracts import (
    CollectionBatch,
    CollectionIssue,
    ConfigurationField,
    FilterChoice,
    MetricSample,
    MonitorView,
    ViewColumn,
    ViewFilter,
)


C2C_AGENT_BASE = "https://www.binance.com"
SPOT_MARKET_BASE = "https://data-api.binance.vision"
USER_AGENT = "Halpha-Monitor/0.2"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
ASSET_PATTERN = re.compile(r"^[A-Z0-9]{2,20}$")
TradeType = Literal["BUY", "SELL"]
TRADE_METHOD_CHOICES = (
    FilterChoice("BANK", "银行卡"),
    FilterChoice("ALIPAY", "支付宝"),
    FilterChoice("WECHAT", "微信"),
)
TRADE_METHOD_LABELS = {choice.value: choice.label for choice in TRADE_METHOD_CHOICES}


class C2CMonitorError(RuntimeError):
    """Sanitized public-data collection failure."""


class SpotSymbolUnavailable(C2CMonitorError):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime) -> str:
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    token = format(value, "f")
    if "." in token:
        token = token.rstrip("0").rstrip(".")
    return token or "0"


def decimal_value(value: Any, *, field: str, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise C2CMonitorError(f"INVALID_DECIMAL_{field.upper()}") from None
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise C2CMonitorError(f"INVALID_DECIMAL_{field.upper()}")
    return parsed


@dataclass(frozen=True)
class BinanceC2CSettings:
    interval_seconds: float = 60
    fiat: str = "CNY"
    assets: tuple[str, ...] = ("USDT", "USDC", "BTC", "ETH", "BNB", "SOL")
    target_fiat: Decimal = Decimal("2000")
    trade_methods: tuple[str, ...] = ("BANK", "ALIPAY", "WECHAT")
    ad_limit: int = 20
    timeout_seconds: float = 10
    proxy_url: str | None = None

    def __post_init__(self) -> None:
        normalized_fiat = self.fiat.strip().upper()
        normalized_assets = tuple(
            dict.fromkeys(("USDT", *(asset.strip().upper() for asset in self.assets)))
        )
        normalized_methods = tuple(
            dict.fromkeys(
                method.strip() for method in self.trade_methods if method.strip()
            )
        )
        if not ASSET_PATTERN.fullmatch(normalized_fiat):
            raise ValueError("C2C_FIAT_INVALID")
        if not normalized_assets or any(
            not ASSET_PATTERN.fullmatch(asset) for asset in normalized_assets
        ):
            raise ValueError("C2C_ASSETS_INVALID")
        if self.interval_seconds < 15:
            raise ValueError("C2C_INTERVAL_TOO_SHORT")
        if self.target_fiat <= 0:
            raise ValueError("C2C_TARGET_FIAT_INVALID")
        if not normalized_methods:
            raise ValueError("C2C_TRADE_METHODS_INVALID")
        if not 1 <= self.ad_limit <= 20:
            raise ValueError("C2C_AD_LIMIT_INVALID")
        if self.timeout_seconds <= 0:
            raise ValueError("C2C_TIMEOUT_INVALID")
        object.__setattr__(self, "fiat", normalized_fiat)
        object.__setattr__(self, "assets", normalized_assets)
        object.__setattr__(self, "trade_methods", normalized_methods)


@dataclass(frozen=True)
class C2CAd:
    ad_no: str
    asset: str
    fiat: str
    price: Decimal
    min_fiat: Decimal
    max_fiat: Decimal
    tradable_asset: Decimal
    trade_methods: tuple[str, ...]
    retrieved_at: datetime


@dataclass(frozen=True)
class SpotConversion:
    asset: str
    symbol: str
    route: Literal["IDENTITY", "DIRECT", "INVERSE"]
    bid_usdt_per_asset: Decimal
    ask_usdt_per_asset: Decimal
    retrieved_at: datetime


@dataclass(frozen=True)
class NormalizedQuote:
    asset: str
    fiat: str
    trade_type: TradeType
    ad_no: str
    c2c_fiat_per_asset: Decimal
    spot_symbol: str
    spot_route: str
    spot_basis: Literal["IDENTITY", "BID", "ASK"]
    spot_usdt_per_asset: Decimal
    normalized_fiat_per_usdt: Decimal
    premium_vs_usdt_pct: Decimal | None
    trade_methods: tuple[str, ...]
    c2c_retrieved_at: datetime
    spot_retrieved_at: datetime


def parse_ads(payload: Any, *, retrieved_at: datetime) -> tuple[C2CAd, ...]:
    if not isinstance(payload, list):
        raise C2CMonitorError("C2C_AD_LIST_SCHEMA_INVALID")
    ads: list[C2CAd] = []
    for raw in payload:
        if not isinstance(raw, dict):
            raise C2CMonitorError("C2C_AD_SCHEMA_INVALID")
        methods = raw.get("tradeMethods")
        if (
            not isinstance(methods, list)
            or any(not isinstance(method, str) or not method for method in methods)
        ):
            raise C2CMonitorError("C2C_TRADE_METHODS_SCHEMA_INVALID")
        method_identifiers = tuple(dict.fromkeys(methods))
        ads.append(
            C2CAd(
                ad_no=str(raw.get("adNo", "")),
                asset=str(raw.get("asset", "")).upper(),
                fiat=str(raw.get("fiat", "")).upper(),
                price=decimal_value(raw.get("price"), field="price", positive=True),
                min_fiat=decimal_value(
                    raw.get("minTransAmount"), field="min_fiat_amount"
                ),
                max_fiat=decimal_value(
                    raw.get("maxTransAmount"),
                    field="max_fiat_amount",
                ),
                tradable_asset=decimal_value(
                    raw.get("tradableAmount"), field="tradable_amount"
                ),
                trade_methods=method_identifiers,
                retrieved_at=retrieved_at,
            )
        )
    if any(
        not ad.ad_no
        or not ad.asset
        or not ad.fiat
        or ad.min_fiat < 0
        or ad.max_fiat <= 0
        or ad.tradable_asset < 0
        or ad.max_fiat < ad.min_fiat
        for ad in ads
    ):
        raise C2CMonitorError("C2C_AD_VALUES_INVALID")
    return tuple(ads)


def ad_supports_target(ad: C2CAd, target_fiat: Decimal) -> bool:
    target_asset = target_fiat / ad.price
    return (
        target_fiat >= ad.min_fiat
        and target_fiat <= ad.max_fiat
        and target_asset <= ad.tradable_asset
    )


def choose_best_ad(
    ads: tuple[C2CAd, ...],
    *,
    trade_type: TradeType,
    target_fiat: Decimal,
) -> C2CAd:
    eligible = tuple(ad for ad in ads if ad_supports_target(ad, target_fiat))
    if not eligible:
        raise C2CMonitorError("NO_ELIGIBLE_C2C_AD")
    return (min if trade_type == "BUY" else max)(eligible, key=lambda item: item.price)


def normalize_ad(
    ad: C2CAd,
    conversion: SpotConversion,
    *,
    trade_type: TradeType,
) -> NormalizedQuote:
    if ad.asset != conversion.asset:
        raise C2CMonitorError("C2C_SPOT_ASSET_MISMATCH")
    if trade_type == "BUY":
        basis: Literal["IDENTITY", "BID", "ASK"] = (
            "IDENTITY" if conversion.route == "IDENTITY" else "BID"
        )
        spot_price = conversion.bid_usdt_per_asset
    else:
        basis = "IDENTITY" if conversion.route == "IDENTITY" else "ASK"
        spot_price = conversion.ask_usdt_per_asset
    return NormalizedQuote(
        asset=ad.asset,
        fiat=ad.fiat,
        trade_type=trade_type,
        ad_no=ad.ad_no,
        c2c_fiat_per_asset=ad.price,
        spot_symbol=conversion.symbol,
        spot_route=conversion.route,
        spot_basis=basis,
        spot_usdt_per_asset=spot_price,
        normalized_fiat_per_usdt=ad.price / spot_price,
        premium_vs_usdt_pct=None,
        trade_methods=ad.trade_methods,
        c2c_retrieved_at=ad.retrieved_at,
        spot_retrieved_at=conversion.retrieved_at,
    )


class BinancePublicClient:
    def __init__(
        self,
        *,
        timeout_seconds: float,
        proxy_url: str | None = None,
        opener: OpenerDirector | None = None,
    ) -> None:
        if opener is not None and proxy_url is not None:
            raise ValueError("opener and proxy_url are mutually exclusive")
        self.timeout_seconds = timeout_seconds
        self.opener = opener or (
            build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
            if proxy_url
            else build_opener()
        )

    def _get(self, base: str, path: str, params: list[tuple[str, str]]) -> Any:
        url = f"{base}{path}?{urlencode(params)}"
        request = Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise C2CMonitorError("RESPONSE_TOO_LARGE")
                raw = body.decode("utf-8")
        except HTTPError as exc:
            try:
                payload = json.loads(
                    exc.read(MAX_RESPONSE_BYTES + 1).decode("utf-8")
                )
            except Exception:
                payload = None
            if isinstance(payload, dict) and payload.get("code") == -1121:
                raise SpotSymbolUnavailable("SPOT_SYMBOL_UNAVAILABLE") from None
            raise C2CMonitorError(f"HTTP_ERROR_{exc.code}") from None
        except (TimeoutError, URLError) as exc:
            raise C2CMonitorError(
                f"NETWORK_ERROR_{type(exc).__name__.upper()}"
            ) from None
        try:
            return json.loads(raw, parse_float=Decimal)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise C2CMonitorError("RESPONSE_JSON_INVALID") from None

    @staticmethod
    def _unwrap_c2c(payload: Any) -> Any:
        if (
            not isinstance(payload, dict)
            or payload.get("success") is not True
            or payload.get("code") != "000000"
            or "data" not in payload
        ):
            raise C2CMonitorError("C2C_RESPONSE_FAILED")
        return payload["data"]

    def canonical_trade_methods(
        self, fiat: str, requested: tuple[str, ...]
    ) -> tuple[str, ...]:
        if not requested:
            return ()
        data = self._unwrap_c2c(
            self._get(
                C2C_AGENT_BASE,
                "/bapi/c2c/v1/public/c2c/agent/trade-methods",
                [("fiat", fiat)],
            )
        )
        if not isinstance(data, list):
            raise C2CMonitorError("C2C_TRADE_METHOD_LIST_SCHEMA_INVALID")
        canonical = {
            str(item.get("identifier", "")).upper(): str(item.get("identifier", ""))
            for item in data
            if isinstance(item, dict) and item.get("identifier")
        }
        unknown = tuple(item for item in requested if item.upper() not in canonical)
        if unknown:
            raise C2CMonitorError("UNKNOWN_TRADE_METHOD")
        return tuple(canonical[item.upper()] for item in requested)

    def fetch_ads(
        self,
        *,
        fiat: str,
        asset: str,
        trade_type: TradeType,
        limit: int,
        trade_methods: tuple[str, ...],
    ) -> tuple[C2CAd, ...]:
        data = self._unwrap_c2c(
            self._get(
                C2C_AGENT_BASE,
                "/bapi/c2c/v1/public/c2c/agent/ad-list",
                [
                    ("fiat", fiat),
                    ("asset", asset),
                    ("tradeType", trade_type),
                    ("limit", str(limit)),
                    *(
                        ("tradeMethodIdentifiers", method)
                        for method in trade_methods
                    ),
                ],
            )
        )
        if not isinstance(data, dict):
            raise C2CMonitorError("C2C_AD_LIST_SCHEMA_INVALID")
        ads = parse_ads(data.get("items"), retrieved_at=utc_now())
        if any(ad.asset != asset or ad.fiat != fiat for ad in ads):
            raise C2CMonitorError("C2C_QUERY_RESULT_MISMATCH")
        requested = set(trade_methods)
        return tuple(
            ad for ad in ads if requested.intersection(ad.trade_methods)
        )

    def _fetch_spot_book(self, symbol: str) -> tuple[Decimal, Decimal, datetime]:
        payload = self._get(
            SPOT_MARKET_BASE,
            "/api/v3/ticker/bookTicker",
            [("symbol", symbol)],
        )
        if not isinstance(payload, dict) or payload.get("symbol") != symbol:
            raise C2CMonitorError("SPOT_BOOK_SCHEMA_INVALID")
        bid = decimal_value(payload.get("bidPrice"), field="bid_price", positive=True)
        ask = decimal_value(payload.get("askPrice"), field="ask_price", positive=True)
        if ask < bid:
            raise C2CMonitorError("SPOT_BOOK_CROSSED")
        return bid, ask, utc_now()

    def fetch_spot_conversion(self, asset: str) -> SpotConversion:
        if asset == "USDT":
            observed = utc_now()
            return SpotConversion(
                asset="USDT",
                symbol="USDT",
                route="IDENTITY",
                bid_usdt_per_asset=Decimal(1),
                ask_usdt_per_asset=Decimal(1),
                retrieved_at=observed,
            )
        direct_symbol = f"{asset}USDT"
        try:
            bid, ask, observed = self._fetch_spot_book(direct_symbol)
            return SpotConversion(
                asset=asset,
                symbol=direct_symbol,
                route="DIRECT",
                bid_usdt_per_asset=bid,
                ask_usdt_per_asset=ask,
                retrieved_at=observed,
            )
        except SpotSymbolUnavailable:
            pass
        inverse_symbol = f"USDT{asset}"
        try:
            inverse_bid, inverse_ask, observed = self._fetch_spot_book(inverse_symbol)
        except SpotSymbolUnavailable:
            raise C2CMonitorError(f"SPOT_USDT_PAIR_UNAVAILABLE_{asset}") from None
        return SpotConversion(
            asset=asset,
            symbol=inverse_symbol,
            route="INVERSE",
            bid_usdt_per_asset=Decimal(1) / inverse_ask,
            ask_usdt_per_asset=Decimal(1) / inverse_bid,
            retrieved_at=observed,
        )


class BinanceC2CMonitor:
    monitor_id = "binance-c2c-normalized"
    display_name = "Binance C2C 核算"
    description = "公开 C2C 广告样本按现货一档折算为法币/USDT 观察值。"

    def __init__(
        self,
        settings: BinanceC2CSettings,
        *,
        client: BinancePublicClient | None = None,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.client = client or BinancePublicClient(
            timeout_seconds=settings.timeout_seconds,
            proxy_url=settings.proxy_url,
        )
        self._settings_lock = threading.Lock()
        self._canonical_methods: dict[tuple[str, tuple[str, ...]], tuple[str, ...]] = {}
        self.configuration_fields = (
            ConfigurationField(
                key="target_fiat",
                label="核算金额",
                kind="decimal",
                unit=settings.fiat,
                minimum="1",
                step="1",
            ),
            ConfigurationField(
                key="trade_methods",
                label="支付方式",
                kind="multi_choice",
                choices=TRADE_METHOD_CHOICES,
            ),
        )
        self.view = MonitorView(
            filters=(
                ViewFilter(
                    key="trade_type",
                    label="方向",
                    default="BUY",
                    choices=(
                        FilterChoice("BUY", "买入币"),
                        FilterChoice("SELL", "卖出币"),
                    ),
                ),
                ViewFilter(
                    key="fiat",
                    label="法币",
                    default=settings.fiat,
                    choices=(FilterChoice(settings.fiat, settings.fiat),),
                ),
            ),
            columns=(
                ViewColumn("asset", "币种"),
                ViewColumn("direction_label", "方向"),
                ViewColumn("c2c_price", "C2C 价", "number"),
                ViewColumn("spot_basis_label", "现货依据", priority="secondary"),
                ViewColumn(
                    "normalized_price",
                    "USDT 核算价",
                    "number",
                    minimum_fraction_digits=6,
                    maximum_fraction_digits=6,
                ),
                ViewColumn("premium_pct", "相对 USDT", "percent"),
                ViewColumn("trade_methods_label", "支付方式", priority="secondary"),
                ViewColumn("observed_at", "采集时间", "time"),
            ),
            chart_title="USDT 核算价历史",
            table_title="最新核算价格",
        )

    def configuration(self) -> dict[str, Any]:
        with self._settings_lock:
            settings = self.settings
        return {
            "target_fiat": decimal_text(settings.target_fiat) or "0",
            "trade_methods": list(settings.trade_methods),
        }

    def normalize_configuration(self, values: dict[str, Any]) -> dict[str, Any]:
        if set(values) != {"target_fiat", "trade_methods"}:
            raise ValueError("C2C_CONFIGURATION_FIELDS_INVALID")
        try:
            target = Decimal(str(values["target_fiat"]))
        except (InvalidOperation, TypeError, ValueError):
            raise ValueError("C2C_TARGET_FIAT_INVALID") from None
        if not target.is_finite() or target <= 0:
            raise ValueError("C2C_TARGET_FIAT_INVALID")

        raw_methods = values["trade_methods"]
        if not isinstance(raw_methods, list):
            raise ValueError("C2C_TRADE_METHODS_INVALID")
        methods = tuple(
            dict.fromkeys(
                str(method).strip().upper()
                for method in raw_methods
                if str(method).strip()
            )
        )
        allowed = {choice.value for choice in TRADE_METHOD_CHOICES}
        if not methods or any(method not in allowed for method in methods):
            raise ValueError("C2C_TRADE_METHODS_INVALID")
        return {
            "target_fiat": decimal_text(target) or "0",
            "trade_methods": list(methods),
        }

    def apply_configuration(self, values: dict[str, Any]) -> None:
        normalized = self.normalize_configuration(values)
        with self._settings_lock:
            self.settings = replace(
                self.settings,
                target_fiat=Decimal(str(normalized["target_fiat"])),
                trade_methods=tuple(str(item) for item in normalized["trade_methods"]),
            )

    def collect(self) -> CollectionBatch:
        with self._settings_lock:
            settings = self.settings
        methods = self._methods(settings)
        conversions: dict[str, SpotConversion] = {}
        issues: list[CollectionIssue] = []
        for asset in settings.assets:
            try:
                conversions[asset] = self.client.fetch_spot_conversion(asset)
            except C2CMonitorError as exc:
                issues.append(CollectionIssue(asset, str(exc)))

        quotes_by_side: dict[TradeType, list[NormalizedQuote]] = {
            "BUY": [],
            "SELL": [],
        }
        for trade_type in ("BUY", "SELL"):
            for asset in settings.assets:
                conversion = conversions.get(asset)
                if conversion is None:
                    continue
                try:
                    ads = self.client.fetch_ads(
                        fiat=settings.fiat,
                        asset=asset,
                        trade_type=trade_type,
                        limit=settings.ad_limit,
                        trade_methods=methods,
                    )
                    ad = choose_best_ad(
                        ads,
                        trade_type=trade_type,
                        target_fiat=settings.target_fiat,
                    )
                    quotes_by_side[trade_type].append(
                        normalize_ad(ad, conversion, trade_type=trade_type)
                    )
                except C2CMonitorError as exc:
                    issues.append(CollectionIssue(f"{trade_type}:{asset}", str(exc)))

        samples: list[MetricSample] = []
        for trade_type, quotes in quotes_by_side.items():
            benchmark = next((quote for quote in quotes if quote.asset == "USDT"), None)
            if benchmark is not None:
                quotes = [
                    replace(
                        quote,
                        premium_vs_usdt_pct=(
                            quote.normalized_fiat_per_usdt
                            / benchmark.normalized_fiat_per_usdt
                            - Decimal(1)
                        )
                        * Decimal(100),
                    )
                    for quote in quotes
                ]
            quotes.sort(
                key=lambda item: item.normalized_fiat_per_usdt,
                reverse=trade_type == "SELL",
            )
            quotes.sort(key=lambda item: item.asset != "USDT")
            samples.extend(self._sample(quote, methods, settings) for quote in quotes)
        if not samples and not issues:
            issues.append(CollectionIssue("monitor", "NO_SAMPLES_RETURNED"))
        return CollectionBatch(samples=tuple(samples), issues=tuple(issues))

    def _methods(self, settings: BinanceC2CSettings) -> tuple[str, ...]:
        key = (settings.fiat, settings.trade_methods)
        methods = self._canonical_methods.get(key)
        if methods is None:
            methods = self.client.canonical_trade_methods(*key)
            self._canonical_methods[key] = methods
        return methods

    def _sample(
        self,
        quote: NormalizedQuote,
        methods: tuple[str, ...],
        settings: BinanceC2CSettings,
    ) -> MetricSample:
        method_key = ",".join(methods) or "ANY"
        observed_at = max(quote.c2c_retrieved_at, quote.spot_retrieved_at)
        target = decimal_text(settings.target_fiat) or "0"
        missing_reasons = (
            {
                "premium_pct": (
                    "缺少同方向 USDT 基准，无法计算相对值；未使用替代数据。"
                )
            }
            if quote.premium_vs_usdt_pct is None
            else {}
        )
        series_key = "|".join(
            (settings.fiat, target, method_key, quote.trade_type, quote.asset)
        )
        return MetricSample(
            series_key=series_key,
            entity_key=quote.asset,
            observed_at=observed_at,
            value_text=decimal_text(quote.normalized_fiat_per_usdt) or "0",
            unit=f"{quote.fiat}_PER_USDT",
            payload={
                "asset": quote.asset,
                "trade_type": quote.trade_type,
                "direction_label": "买入币" if quote.trade_type == "BUY" else "卖出币",
                "series_label": (
                    f"{quote.asset} · "
                    f"{'买入币' if quote.trade_type == 'BUY' else '卖出币'} · "
                    f"{quote.fiat}/USDT"
                ),
                "fiat": quote.fiat,
                "target_fiat": target,
                "trade_methods_key": method_key,
                "trade_methods_label": "、".join(
                    TRADE_METHOD_LABELS.get(method, method)
                    for method in quote.trade_methods
                )
                or "全部",
                "c2c_price": decimal_text(quote.c2c_fiat_per_asset),
                "spot_basis_label": f"{quote.spot_symbol} {quote.spot_basis}",
                "spot_price": decimal_text(quote.spot_usdt_per_asset),
                "normalized_price": decimal_text(quote.normalized_fiat_per_usdt),
                "premium_pct": decimal_text(quote.premium_vs_usdt_pct),
                "ad_no": quote.ad_no,
                "c2c_retrieved_at": iso_utc(quote.c2c_retrieved_at),
                "spot_retrieved_at": iso_utc(quote.spot_retrieved_at),
                "missing_reasons": missing_reasons,
            },
        )
