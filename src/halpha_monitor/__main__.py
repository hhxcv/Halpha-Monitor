"""Run the independent Halpha Monitor service on localhost."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import uvicorn

from halpha_monitor.monitors import (
    add_builtin_monitor_arguments,
    register_builtin_monitors,
)
from halpha_monitor.service import MonitorRegistry, MonitorScheduler
from halpha_monitor.store import (
    DEFAULT_BTC_STRUCTURE_MAX_EVENTS,
    DEFAULT_BTC_STRUCTURE_RETENTION_DAYS,
    DEFAULT_BUYBACK_EVIDENCE_MAX_BYTES,
    DEFAULT_BUYBACK_RETENTION_DAYS,
    DEFAULT_MARKET_EVENT_RETENTION_DAYS,
    SQLiteMonitorStore,
)
from halpha_monitor.web import create_app


def default_database_path() -> Path:
    configured_path = os.environ.get("HALPHA_MONITOR_DB_PATH", "").strip()
    if configured_path:
        return Path(configured_path).expanduser()
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "Halpha" / "monitor" / "monitor.sqlite3"
    return Path.cwd() / "output" / "monitoring" / "monitor.sqlite3"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the read-only Halpha monitoring page. The service binds to "
            "127.0.0.1 and has no trading or account capability."
        )
    )
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--db-path", type=Path, default=default_database_path())
    parser.add_argument("--retention-days", type=int, default=90)
    parser.add_argument(
        "--buyback-retention-days",
        type=int,
        default=DEFAULT_BUYBACK_RETENTION_DAYS,
    )
    parser.add_argument(
        "--buyback-evidence-max-mib",
        type=int,
        default=DEFAULT_BUYBACK_EVIDENCE_MAX_BYTES // (1024 * 1024),
    )
    parser.add_argument(
        "--market-event-retention-days",
        type=int,
        default=DEFAULT_MARKET_EVENT_RETENTION_DAYS,
    )
    parser.add_argument(
        "--btc-structure-retention-days",
        type=int,
        default=DEFAULT_BTC_STRUCTURE_RETENTION_DAYS,
    )
    parser.add_argument(
        "--btc-structure-max-events",
        type=int,
        default=DEFAULT_BTC_STRUCTURE_MAX_EVENTS,
    )
    add_builtin_monitor_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1024 <= args.port <= 65535:
        raise SystemExit("port must be between 1024 and 65535")
    if args.retention_days < 1:
        raise SystemExit("retention-days must be positive")
    if args.buyback_retention_days < 1:
        raise SystemExit("buyback-retention-days must be positive")
    if args.buyback_evidence_max_mib < 1:
        raise SystemExit("buyback-evidence-max-mib must be positive")
    if args.market_event_retention_days < 1:
        raise SystemExit("market-event-retention-days must be positive")
    if args.btc_structure_retention_days < 1:
        raise SystemExit("btc-structure-retention-days must be positive")
    if args.btc_structure_max_events < 1:
        raise SystemExit("btc-structure-max-events must be positive")
    store = SQLiteMonitorStore(
        args.db_path,
        buyback_retention_days=args.buyback_retention_days,
        buyback_evidence_max_bytes=args.buyback_evidence_max_mib * 1024 * 1024,
        market_event_retention_days=args.market_event_retention_days,
        btc_structure_retention_days=args.btc_structure_retention_days,
        btc_structure_max_events=args.btc_structure_max_events,
    )
    registry = MonitorRegistry()
    register_builtin_monitors(
        registry,
        args=args,
        store=store,
    )
    scheduler = MonitorScheduler(
        registry,
        store,
        retention_days=args.retention_days,
    )
    app = create_app(store, registry, scheduler)
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=args.port,
        access_log=False,
        log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
