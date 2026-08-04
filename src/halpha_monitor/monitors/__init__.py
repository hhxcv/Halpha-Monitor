"""One explicit integration point for built-in monitor CLI and construction.

Each monitor owns its collection module. Adding one monitor changes that module, its
tests, and this file only; there is no dynamic discovery or import-time registration.
"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
from pathlib import Path

from halpha_monitor.monitors.binance_c2c import (
    BinanceC2CMonitor,
    BinanceC2CSettings,
)
from halpha_monitor.monitors.binance_smart_money import (
    BinanceSmartMoneyMonitor,
    BinanceSmartMoneySettings,
)
from halpha_monitor.monitors.binance_btc_relationship import (
    BinanceBtcRelationshipMonitor,
    BinanceBtcRelationshipSettings,
)
from halpha_monitor.service import MonitorRegistry


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


def add_builtin_monitor_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare built-in monitor settings without coupling the service entry point."""

    parser.add_argument("--interval-seconds", type=float, default=60)
    parser.add_argument("--smart-money-interval-seconds", type=float, default=60)
    parser.add_argument("--smart-money-jitter-seconds", type=float, default=5)
    parser.add_argument("--smart-money-symbols", default="BTCUSDT")
    parser.add_argument("--btc-relationship-interval-seconds", type=float, default=3600)
    parser.add_argument("--btc-relationship-jitter-seconds", type=float, default=120)
    parser.add_argument("--btc-relationship-workers", type=int, default=8)
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
    database_path: Path,
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
        BinanceSmartMoneyMonitor(
            BinanceSmartMoneySettings(
                interval_seconds=args.smart_money_interval_seconds,
                jitter_seconds=args.smart_money_jitter_seconds,
                symbols=_csv_tokens(args.smart_money_symbols),
                timeout_seconds=args.timeout_seconds,
                proxy_url=args.proxy_url,
            )
        )
    )
    registry.register(
        BinanceBtcRelationshipMonitor(
            BinanceBtcRelationshipSettings(
                cache_root=database_path.resolve().parent
                / "cache"
                / "btc-relationship",
                interval_seconds=args.btc_relationship_interval_seconds,
                jitter_seconds=args.btc_relationship_jitter_seconds,
                timeout_seconds=args.timeout_seconds,
                workers=args.btc_relationship_workers,
            )
        )
    )


__all__ = [
    "BinanceC2CMonitor",
    "BinanceC2CSettings",
    "BinanceBtcRelationshipMonitor",
    "BinanceBtcRelationshipSettings",
    "BinanceSmartMoneyMonitor",
    "BinanceSmartMoneySettings",
    "add_builtin_monitor_arguments",
    "register_builtin_monitors",
]
