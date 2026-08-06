from decimal import Decimal

from halpha_monitor.__main__ import build_parser, default_database_path
from halpha_monitor.monitors import register_builtin_monitors
from halpha_monitor.service import MONITOR_ID_PATTERN, MonitorRegistry
from halpha_monitor.store import SQLiteMonitorStore


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
            "BTCUSDT,ETHUSDT,BTCUSDT",
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
        "binance-usdm-smart-money",
        "binance-altcoin-radar",
        "binance-btc-relationship",
    )
    c2c, smart_money, altcoin_radar, btc_relationship = registry.all()
    assert c2c.settings.assets == ("USDT", "BTC")
    assert c2c.settings.target_fiat == Decimal("2500")
    assert smart_money.settings.symbols == ("BTCUSDT", "ETHUSDT")
    assert altcoin_radar.settings.min_quote_volume_24h == Decimal("8000000")
    assert altcoin_radar.settings.max_candidates == 12
    assert (
        btc_relationship.settings.cache_root
        == tmp_path / "cache" / "btc-relationship"
    )


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
