"""One explicit integration point for built-in monitor CLI and construction.

Each monitor owns its collection module. Adding one monitor changes that module, its
tests, and this file only; there is no dynamic discovery or import-time registration.
"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation

from halpha_monitor.monitors.binance_c2c import (
    BinanceC2CMonitor,
    BinanceC2CSettings,
)
from halpha_monitor.monitors.binance_btc_intelligence import (
    BinanceBtcIntelligenceMonitor,
    BinanceBtcIntelligenceSettings,
)
from halpha_monitor.monitors.binance_altcoin_radar import (
    BinanceAltcoinRadarMonitor,
    BinanceAltcoinRadarSettings,
)
from halpha_monitor.monitors.binance_btc_relationship import (
    BinanceBtcRelationshipMonitor,
    BinanceBtcRelationshipSettings,
)
from halpha_monitor.monitors.a_hk_buyback import (
    AHKBuybackMonitor,
    BuybackSettings,
)
from halpha_monitor.monitors.market_events import (
    MarketEventMonitor,
    MarketEventSettings,
)
from halpha_monitor.service import MonitorRegistry
from halpha_monitor.store import SQLiteMonitorStore


def _csv_tokens(value: str) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(token.strip() for token in value.split(",") if token.strip())
    )


def _positive_decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise argparse.ArgumentTypeError("positive decimal required") from None
    if not parsed.is_finite() or parsed <= 0:
        raise argparse.ArgumentTypeError("positive decimal required")
    return parsed


def _btc_smart_money_symbol(value: str) -> str:
    if value.strip().upper() != "BTCUSDT":
        raise argparse.ArgumentTypeError(
            "the integrated BTC intelligence monitor only supports BTCUSDT"
        )
    return "BTCUSDT"


def add_builtin_monitor_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare built-in monitor settings without coupling the service entry point."""

    parser.add_argument("--interval-seconds", type=float, default=300)
    parser.add_argument("--smart-money-interval-seconds", type=float, default=600)
    parser.add_argument("--smart-money-jitter-seconds", type=float, default=30)
    parser.add_argument(
        "--smart-money-symbols",
        type=_btc_smart_money_symbol,
        default="BTCUSDT",
        help="compatibility option; the integrated monitor requires BTCUSDT",
    )
    parser.add_argument("--altcoin-radar-interval-seconds", type=float, default=3600)
    parser.add_argument("--altcoin-radar-jitter-seconds", type=float, default=30)
    parser.add_argument(
        "--altcoin-radar-min-quote-volume",
        type=_positive_decimal,
        default=Decimal("5000000"),
    )
    parser.add_argument("--altcoin-radar-max-candidates", type=int, default=30)
    parser.add_argument(
        "--altcoin-radar-max-screened-contracts",
        type=int,
        default=240,
    )
    parser.add_argument("--altcoin-radar-workers", type=int, default=6)
    parser.add_argument("--btc-relationship-interval-seconds", type=float, default=3600)
    parser.add_argument("--btc-relationship-jitter-seconds", type=float, default=120)
    parser.add_argument("--btc-relationship-workers", type=int, default=8)
    parser.add_argument("--buyback-interval-seconds", type=float, default=3600)
    parser.add_argument("--buyback-jitter-seconds", type=float, default=300)
    parser.add_argument("--buyback-lookback-days", type=int, default=7)
    parser.add_argument("--buyback-max-documents-per-run", type=int, default=20)
    parser.add_argument("--market-events-interval-seconds", type=float, default=21600)
    parser.add_argument("--market-events-jitter-seconds", type=float, default=900)
    parser.add_argument("--market-events-lookahead-days", type=int, default=60)
    parser.add_argument("--fiat", default="CNY")
    parser.add_argument("--assets", default="USDT,USDC,BTC,ETH,BNB,SOL")
    parser.add_argument(
        "--target-fiat",
        type=_positive_decimal,
        default=Decimal("2000"),
    )
    parser.add_argument("--pay-types", default="BANK,ALIPAY,WECHAT")
    parser.add_argument("--ad-limit", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=float, default=10)
    parser.add_argument("--proxy-url")


def register_builtin_monitors(
    registry: MonitorRegistry,
    *,
    args: argparse.Namespace,
    store: SQLiteMonitorStore,
) -> None:
    registry.register(
        BinanceC2CMonitor(
            BinanceC2CSettings(
                interval_seconds=args.interval_seconds,
                fiat=args.fiat,
                assets=_csv_tokens(args.assets),
                target_fiat=args.target_fiat,
                trade_methods=_csv_tokens(args.pay_types),
                ad_limit=args.ad_limit,
                timeout_seconds=args.timeout_seconds,
                proxy_url=args.proxy_url,
            )
        )
    )
    registry.register(
        BinanceBtcIntelligenceMonitor(
            BinanceBtcIntelligenceSettings(
                cache_root=store.path.parent / "cache" / "btc-intelligence",
                interval_seconds=args.smart_money_interval_seconds,
                jitter_seconds=args.smart_money_jitter_seconds,
                timeout_seconds=args.timeout_seconds,
                proxy_url=args.proxy_url,
            ),
            store=store,
        )
    )
    registry.register(
        BinanceAltcoinRadarMonitor(
            BinanceAltcoinRadarSettings(
                cache_root=store.path.parent / "cache" / "altcoin-radar",
                interval_seconds=args.altcoin_radar_interval_seconds,
                jitter_seconds=args.altcoin_radar_jitter_seconds,
                min_quote_volume_24h=args.altcoin_radar_min_quote_volume,
                max_candidates=args.altcoin_radar_max_candidates,
                max_screened_contracts=args.altcoin_radar_max_screened_contracts,
                workers=args.altcoin_radar_workers,
                timeout_seconds=args.timeout_seconds,
                proxy_url=args.proxy_url,
            ),
            evaluation_store=store,
        )
    )
    registry.register(
        BinanceBtcRelationshipMonitor(
            BinanceBtcRelationshipSettings(
                cache_root=store.path.parent / "cache" / "btc-relationship",
                interval_seconds=args.btc_relationship_interval_seconds,
                jitter_seconds=args.btc_relationship_jitter_seconds,
                timeout_seconds=args.timeout_seconds,
                workers=args.btc_relationship_workers,
            )
        )
    )
    registry.register(
        AHKBuybackMonitor(
            BuybackSettings(
                interval_seconds=args.buyback_interval_seconds,
                jitter_seconds=args.buyback_jitter_seconds,
                lookback_days=args.buyback_lookback_days,
                timeout_seconds=args.timeout_seconds,
                max_documents_per_run=args.buyback_max_documents_per_run,
                hkex_refresh_seconds=max(
                    args.buyback_interval_seconds,
                    6 * 3600,
                ),
                connect_refresh_seconds=max(
                    args.buyback_interval_seconds,
                    24 * 3600,
                ),
                proxy_url=args.proxy_url,
            ),
            store=store,
        )
    )
    registry.register(
        MarketEventMonitor(
            MarketEventSettings(
                interval_seconds=args.market_events_interval_seconds,
                jitter_seconds=args.market_events_jitter_seconds,
                lookahead_days=args.market_events_lookahead_days,
                timeout_seconds=args.timeout_seconds,
                proxy_url=args.proxy_url,
            ),
            store=store,
        )
    )


__all__ = [
    "AHKBuybackMonitor",
    "BuybackSettings",
    "BinanceAltcoinRadarMonitor",
    "BinanceAltcoinRadarSettings",
    "BinanceC2CMonitor",
    "BinanceC2CSettings",
    "BinanceBtcRelationshipMonitor",
    "BinanceBtcRelationshipSettings",
    "BinanceBtcIntelligenceMonitor",
    "BinanceBtcIntelligenceSettings",
    "MarketEventMonitor",
    "MarketEventSettings",
    "add_builtin_monitor_arguments",
    "register_builtin_monitors",
]
