from decimal import Decimal
import os
from pathlib import Path
import subprocess
import sys

import pytest

from halpha_monitor.__main__ import build_parser, default_database_path
from halpha_monitor.monitors import register_builtin_monitors
from halpha_monitor.service import MONITOR_ID_PATTERN, MonitorRegistry
from halpha_monitor.store import SQLiteMonitorStore


def test_package_defaults_openblas_to_one_thread_but_respects_override() -> None:
    command = [
        sys.executable,
        "-c",
        "import os; import halpha_monitor; print(os.environ['OPENBLAS_NUM_THREADS'])",
    ]
    environment = os.environ.copy()
    environment.pop("OPENBLAS_NUM_THREADS", None)
    environment["PYTHONPATH"] = str(Path.cwd() / "src")

    defaulted = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    environment["OPENBLAS_NUM_THREADS"] = "3"
    overridden = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert defaulted.stdout.strip() == "1"
    assert overridden.stdout.strip() == "3"


def test_database_path_prefers_explicit_environment_override(
    monkeypatch, tmp_path
) -> None:
    database_path = tmp_path / "monitor.sqlite3"
    monkeypatch.setenv("HALPHA_MONITOR_DB_PATH", str(database_path))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local-app-data"))

    assert default_database_path() == database_path
    assert build_parser().parse_args([]).db_path == database_path


def test_database_path_keeps_local_app_data_fallback(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("HALPHA_MONITOR_DB_PATH", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    assert default_database_path() == (
        tmp_path / "Halpha" / "monitor" / "monitor.sqlite3"
    )


def test_builtin_monitors_share_one_explicit_cli_integration_point(tmp_path) -> None:
    database_path = tmp_path / "monitor.sqlite3"
    args = build_parser().parse_args(
        [
            "--db-path",
            str(database_path),
            "--assets",
            "USDT,BTC,BTC",
            "--smart-money-symbols",
            "BTCUSDT",
            "--target-fiat",
            "2500",
            "--altcoin-radar-min-quote-volume",
            "8000000",
            "--altcoin-radar-max-candidates",
            "12",
        ]
    )
    registry = MonitorRegistry()

    register_builtin_monitors(
        registry,
        args=args,
        store=SQLiteMonitorStore(database_path),
    )

    assert tuple(monitor.monitor_id for monitor in registry) == (
        "binance-c2c-normalized",
        "btc-market-intelligence",
        "binance-altcoin-radar",
        "binance-btc-relationship",
        "a-hk-buyback",
        "market-event-calendar",
        "stock-event-calendar",
    )
    (
        c2c,
        btc_intelligence,
        altcoin_radar,
        btc_relationship,
        buyback,
        market_events,
        stock_events,
    ) = registry.all()
    assert c2c.settings.assets == ("USDT", "BTC")
    assert c2c.settings.target_fiat == Decimal("2500")
    assert (
        btc_intelligence.settings.cache_root
        == tmp_path / "cache" / "btc-intelligence"
    )
    assert altcoin_radar.settings.min_quote_volume_24h == Decimal("8000000")
    assert altcoin_radar.settings.max_candidates == 12
    assert (
        btc_relationship.settings.cache_root
        == tmp_path / "cache" / "btc-relationship"
    )
    assert c2c.interval_seconds == 300
    assert btc_intelligence.interval_seconds == 600
    assert btc_intelligence.jitter_seconds == 30
    assert btc_intelligence.foreground_interval_seconds == 60
    assert altcoin_radar.interval_seconds == 3600
    assert altcoin_radar.jitter_seconds == 30
    assert altcoin_radar.foreground_interval_seconds == 300
    assert btc_relationship.interval_seconds == 3600
    assert btc_relationship.jitter_seconds == 120
    assert buyback.default_enabled is False
    assert buyback.interval_seconds == 3600
    assert buyback.jitter_seconds == 300
    assert buyback.settings.lookback_days == 7
    assert buyback.settings.max_documents_per_run == 20
    assert market_events.default_enabled is True
    assert market_events.settings.lookahead_days == 60
    assert market_events.interval_seconds == 21600
    assert market_events.jitter_seconds == 900
    assert stock_events.default_enabled is True
    assert stock_events.settings.history_days == 30
    assert stock_events.settings.lookahead_days == 60
    assert stock_events.settings.auto_limit == 80
    assert stock_events.interval_seconds == 3600
    assert stock_events.jitter_seconds == 300
    assert args.buyback_retention_days == 1095
    assert args.buyback_evidence_max_mib == 2048
    assert args.btc_structure_retention_days == 1825
    assert args.btc_structure_max_events == 20000


def test_integrated_btc_intelligence_rejects_non_btc_smart_money_scope() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--smart-money-symbols", "ETHUSDT"])


def test_builtin_monitors_conform_to_shared_view_contract(tmp_path) -> None:
    args = build_parser().parse_args(
        ["--db-path", str(tmp_path / "monitor.sqlite3")]
    )
    registry = MonitorRegistry()
    register_builtin_monitors(
        registry,
        args=args,
        store=SQLiteMonitorStore(args.db_path),
    )

    for monitor in registry:
        assert MONITOR_ID_PATTERN.fullmatch(monitor.monitor_id)
        assert monitor.display_name.strip()
        assert monitor.description.strip()
        assert monitor.interval_seconds >= 15
        assert monitor.network_request_count(window_seconds=60) == 0
        assert monitor.view.chart_title.strip()
        keys = [column.key for column in monitor.view.columns]
        assert keys
        assert len(keys) == len(set(keys))
