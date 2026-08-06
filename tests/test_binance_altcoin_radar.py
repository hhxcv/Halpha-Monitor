from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.error import HTTPError

import pytest

from halpha_monitor.monitors.binance_altcoin_radar import (
    BinanceAltcoinRadarClient,
    BinanceAltcoinRadarMonitor,
    BinanceAltcoinRadarSettings,
    CandleFeatures,
    FundingContext,
    OpenInterestPoint,
    RadarSourceError,
    RollingTicker,
    SpotCandle,
    TimedValue,
    open_interest_change_15m,
    parse_spot_klines,
    parse_tickers,
    rolling_symbol_batches,
    score_candidate,
    select_candidate_seeds,
)


NOW = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)


def ticker(
    symbol: str,
    *,
    change: float,
    quote_volume: float,
    high: float = 110,
    low: float = 90,
    last: float = 105,
) -> RollingTicker:
    return RollingTicker(
        symbol=symbol,
        price_change_percent=change,
        open_price=100,
        high_price=high,
        low_price=low,
        last_price=last,
        quote_volume=quote_volume,
        trade_count=10_000,
        close_time=NOW,
    )


def candles(*, accelerating: bool = False) -> tuple[SpotCandle, ...]:
    values: list[SpotCandle] = []
    start = NOW - timedelta(minutes=48 * 5)
    previous_close = 100.0
    for index in range(48):
        open_time = start + timedelta(minutes=index * 5)
        multiplier = 1.0
        quote_volume = 1_000.0
        taker_share = 0.50
        if accelerating and index >= 45:
            multiplier = 1.02
            quote_volume = 4_000.0
            taker_share = 0.72
        close = previous_close * multiplier
        values.append(
            SpotCandle(
                open_time=open_time,
                close_time=open_time + timedelta(minutes=5) - timedelta(milliseconds=1),
                open_price=previous_close,
                high_price=max(previous_close, close) * 1.001,
                low_price=min(previous_close, close) * 0.999,
                close_price=close,
                quote_volume=quote_volume,
                trade_count=int(quote_volume / 10),
                taker_buy_quote_volume=quote_volume * taker_share,
            )
        )
        previous_close = close
    return tuple(values)


def test_parse_spot_klines_uses_only_closed_valid_rows() -> None:
    completed = datetime(2026, 8, 6, 12, 1, tzinfo=UTC)
    closed_at = int(datetime(2026, 8, 6, 11, 59, 59, tzinfo=UTC).timestamp() * 1000)
    future_at = int(datetime(2026, 8, 6, 12, 4, 59, tzinfo=UTC).timestamp() * 1000)
    rows = [
        [
            closed_at - 299_999,
            "1",
            "2",
            "0.5",
            "1.5",
            "10",
            closed_at,
            "15",
            20,
            "5",
            "8",
            "0",
        ],
        [
            future_at - 299_999,
            "1.5",
            "2",
            "1",
            "1.8",
            "10",
            future_at,
            "18",
            20,
            "5",
            "9",
            "0",
        ],
        ["malformed"],
    ]

    parsed, malformed = parse_spot_klines(rows, completed_at=completed)

    assert len(parsed) == 1
    assert parsed[0].close_price == 1.5
    assert malformed == 1


def test_parse_tickers_ignores_a_trading_market_with_no_trades() -> None:
    empty = {
        "symbol": "EMPTYUSDT",
        "priceChangePercent": "0",
        "openPrice": "0",
        "highPrice": "0",
        "lowPrice": "0",
        "lastPrice": "0",
        "quoteVolume": "0",
        "count": 0,
        "closeTime": int(NOW.timestamp() * 1000),
    }

    valid = {
        "symbol": "AAAUSDT",
        "priceChangePercent": "1",
        "openPrice": "100",
        "highPrice": "110",
        "lowPrice": "90",
        "lastPrice": "105",
        "quoteVolume": "1000000",
        "count": 100,
        "closeTime": int(NOW.timestamp() * 1000),
    }
    parsed, malformed = parse_tickers([empty, valid])

    assert set(parsed) == {"AAAUSDT"}
    assert malformed == 0


def test_rolling_batches_isolate_non_ascii_symbols() -> None:
    assert rolling_symbol_batches(
        ("AAAUSDT", "币安人生USDT", "BBBUSDT"), 100
    ) == (("AAAUSDT", "BBBUSDT"), ("币安人生USDT",))


def test_http_throttle_backoff_is_bounded_and_recovers() -> None:
    current = [NOW]

    class ThrottledOpener:
        def open(self, request, *, timeout):  # type: ignore[no-untyped-def]
            raise HTTPError(
                request.full_url,
                429,
                "throttled",
                {"Retry-After": "7200"},
                None,
            )

    client = BinanceAltcoinRadarClient(
        timeout_seconds=1,
        opener=ThrottledOpener(),  # type: ignore[arg-type]
        now=lambda: current[0],
        random_uniform=lambda _start, _end: 15,
    )

    with pytest.raises(RadarSourceError, match="RADAR_HTTP_THROTTLED_429"):
        client.ticker_24h()
    current[0] = NOW + timedelta(seconds=3599)
    with pytest.raises(RadarSourceError, match="RADAR_BACKOFF_ACTIVE"):
        client.ensure_available()
    current[0] = NOW + timedelta(seconds=3600)
    client.ensure_available()


def test_scoring_distinguishes_setup_acceleration_and_exhaustion() -> None:
    setup = CandleFeatures(
        cutoff_at=NOW,
        return_15m_percent=1.0,
        return_1h_percent=2.0,
        quote_volume_ratio_15m=2.5,
        trade_count_ratio_15m=2.0,
        taker_buy_percent=62.0,
        breakout_percent=-0.2,
        range_position_1h_percent=85.0,
        compression_ratio=0.55,
        peak_drawdown_15m_percent=-0.2,
        latest_candle_return_percent=0.3,
    )
    acceleration = CandleFeatures(
        cutoff_at=NOW,
        return_15m_percent=6.0,
        return_1h_percent=10.0,
        quote_volume_ratio_15m=4.0,
        trade_count_ratio_15m=3.0,
        taker_buy_percent=70.0,
        breakout_percent=2.0,
        range_position_1h_percent=95.0,
        compression_ratio=1.0,
        peak_drawdown_15m_percent=-0.2,
        latest_candle_return_percent=2.0,
    )
    exhaustion = CandleFeatures(
        cutoff_at=NOW,
        return_15m_percent=-1.0,
        return_1h_percent=15.0,
        quote_volume_ratio_15m=4.0,
        trade_count_ratio_15m=3.0,
        taker_buy_percent=40.0,
        breakout_percent=2.0,
        range_position_1h_percent=70.0,
        compression_ratio=1.2,
        peak_drawdown_15m_percent=-4.0,
        latest_candle_return_percent=-3.0,
    )

    assert score_candidate(
        setup,
        return_24h_percent=4,
        relative_return_15m_percent=1,
        funding_rate_percent=0.01,
        oi_change_15m_percent=1,
    )["stage"] == "SETUP"
    assert score_candidate(
        acceleration,
        return_24h_percent=25,
        relative_return_15m_percent=5,
        funding_rate_percent=0.01,
        oi_change_15m_percent=4,
    )["stage"] == "ACCELERATION"
    exhausted = score_candidate(
        exhaustion,
        return_24h_percent=40,
        relative_return_15m_percent=-2,
        funding_rate_percent=0.12,
        oi_change_15m_percent=-5,
    )
    assert exhausted["stage"] == "EXHAUSTION"
    assert exhausted["tail_risk_score"] > 75

    cooled_from_range_low = score_candidate(
        CandleFeatures(
            cutoff_at=NOW,
            return_15m_percent=-1.0,
            return_1h_percent=-4.0,
            quote_volume_ratio_15m=0.8,
            trade_count_ratio_15m=0.8,
            taker_buy_percent=55.0,
            breakout_percent=-2.0,
            range_position_1h_percent=0.0,
            compression_ratio=1.0,
            peak_drawdown_15m_percent=-3.0,
            latest_candle_return_percent=-1.0,
        ),
        return_24h_percent=12,
        relative_return_15m_percent=-1,
        funding_rate_percent=None,
        oi_change_15m_percent=None,
    )
    assert cooled_from_range_low["stage"] == "COOLDOWN"


def test_open_interest_change_requires_contiguous_five_minute_points() -> None:
    contiguous = tuple(
        OpenInterestPoint(100 + index * 10, NOW + timedelta(minutes=index * 5))
        for index in range(4)
    )
    with_gap = contiguous[:2] + (
        OpenInterestPoint(130, NOW + timedelta(minutes=20)),
        OpenInterestPoint(140, NOW + timedelta(minutes=25)),
    )

    assert open_interest_change_15m(contiguous) == pytest.approx(30)
    assert open_interest_change_15m(with_gap) is None


def test_candidate_selection_excludes_btc_stables_and_leveraged_tokens() -> None:
    symbols = {
        "BTCUSDT": "BTC",
        "UUSDT": "U",
        "USDCUSDT": "USDC",
        "USDEUSDT": "USDE",
        "ETHUPUSDT": "ETHUP",
        "JUPUSDT": "JUP",
        "SYRUPUSDT": "SYRUP",
        "AAAUSDT": "AAA",
        "BBBUSDT": "BBB",
    }
    day = {
        symbol: ticker(symbol, change=10, quote_volume=20_000_000)
        for symbol in symbols
    }
    hour = {
        symbol: ticker(symbol, change=3, quote_volume=3_000_000)
        for symbol in symbols
    }

    selected = select_candidate_seeds(
        symbols,
        day,
        hour,
        min_quote_volume_24h=5_000_000,
        maximum=5,
    )

    assert {seed.symbol for seed in selected} == {
        "AAAUSDT",
        "BBBUSDT",
        "JUPUSDT",
        "SYRUPUSDT",
    }


class FakeProvider:
    def __init__(self) -> None:
        self.reset_calls = 0

    def ensure_available(self) -> None:
        return None

    def reset_throttle_backoff(self) -> None:
        self.reset_calls += 1

    def exchange_symbols(self) -> TimedValue:
        return TimedValue(
            {
                "BTCUSDT": "BTC",
                "USDCUSDT": "USDC",
                "AAAUSDT": "AAA",
                "BBBUSDT": "BBB",
            },
            NOW,
        )

    def ticker_24h(self) -> TimedValue:
        return TimedValue(
            {
                "AAAUSDT": ticker(
                    "AAAUSDT", change=24, quote_volume=30_000_000
                ),
                "BBBUSDT": ticker(
                    "BBBUSDT", change=8, quote_volume=20_000_000
                ),
            },
            NOW,
        )

    def rolling_tickers(self, symbols, *, window_size):  # type: ignore[no-untyped-def]
        assert window_size == "1h"
        return TimedValue(
            {
                symbol: ticker(symbol, change=5, quote_volume=4_000_000)
                for symbol in symbols
            },
            NOW,
        )

    def spot_klines(self, symbol: str, *, limit: int) -> TimedValue:
        assert limit == 48
        if symbol == "BBBUSDT":
            raise RadarSourceError("RADAR_KLINES_STALE")
        return TimedValue(candles(accelerating=symbol == "AAAUSDT"), NOW)

    def futures_premium_index(self) -> TimedValue:
        return TimedValue(
            {
                "AAAUSDT": FundingContext(
                    "AAAUSDT",
                    funding_rate_percent=0.02,
                    source_time=NOW - timedelta(minutes=1),
                )
            },
            NOW,
        )

    def open_interest_history(self, symbol: str, *, limit: int) -> TimedValue:
        assert symbol == "AAAUSDT"
        assert limit == 6
        return TimedValue(
            tuple(
                OpenInterestPoint(
                    value=1_000_000 + index * 50_000,
                    source_time=NOW - timedelta(minutes=(5 - index) * 5),
                )
                for index in range(6)
            ),
            NOW,
        )


def test_monitor_keeps_failed_candidate_explicit_and_preserves_valid_rows() -> None:
    provider = FakeProvider()
    monitor = BinanceAltcoinRadarMonitor(
        BinanceAltcoinRadarSettings(
            min_quote_volume_24h=Decimal("5000000"),
            max_candidates=5,
            workers=2,
        ),
        client=provider,
    )

    batch = monitor.collect()

    by_symbol = {sample.entity_key: sample for sample in batch.samples}
    assert set(by_symbol) == {"AAAUSDT", "BBBUSDT"}
    assert by_symbol["AAAUSDT"].value_text
    assert by_symbol["AAAUSDT"].payload["universe_size"] == 2
    assert by_symbol["AAAUSDT"].payload["onchain_state"] == (
        "NOT_INCLUDED_IN_MARKET_SCORE"
    )
    assert by_symbol["AAAUSDT"].payload["data_scope_label"] == "现货 + 合约"
    cutoff_at = datetime.fromisoformat(
        by_symbol["AAAUSDT"].payload["data_cutoff_at"].replace("Z", "+00:00")
    )
    valid_until = datetime.fromisoformat(
        by_symbol["AAAUSDT"].payload["valid_until"].replace("Z", "+00:00")
    )
    assert valid_until - cutoff_at == timedelta(minutes=15)
    assert by_symbol["AAAUSDT"].payload["review_window_label"] == (
        "每根 5m K 复核"
    )
    assert by_symbol["BBBUSDT"].value_text == ""
    assert by_symbol["BBBUSDT"].payload["stage"] == "DATA_GAP"
    assert by_symbol["BBBUSDT"].payload["data_scope_label"] == "仅现货"
    assert by_symbol["BBBUSDT"].payload["valid_until"] is None
    assert by_symbol["BBBUSDT"].payload["review_window_label"] == "不可判断"
    assert any(
        issue.scope == "BBBUSDT" and issue.reason_code == "RADAR_KLINES_STALE"
        for issue in batch.issues
    )
    assert provider.reset_calls == 1
    taker_column = next(
        column for column in monitor.view.columns if column.key == "taker_buy_percent"
    )
    assert taker_column.show_sign is False
    assert any(column.key == "coverage_label" for column in monitor.view.columns)
    assert all(
        column.description
        for column in monitor.view.columns
        if column.key != "symbol"
    )
    assert monitor.description.strip()
    assert monitor.view.show_description is False
    assert monitor.view.method_note is not None
    assert "不是期望盈利持仓期" in monitor.view.method_note
    assert any(
        choice.value == "DATA_GAP"
        for choice in monitor.view.filters[0].choices
    )


def test_partial_throttle_does_not_clear_shared_backoff() -> None:
    class PartiallyThrottledProvider(FakeProvider):
        def spot_klines(self, symbol: str, *, limit: int) -> TimedValue:
            if symbol == "BBBUSDT":
                raise RadarSourceError("RADAR_HTTP_THROTTLED_429", throttled=True)
            return super().spot_klines(symbol, limit=limit)

    provider = PartiallyThrottledProvider()
    monitor = BinanceAltcoinRadarMonitor(
        BinanceAltcoinRadarSettings(max_candidates=5, workers=2),
        client=provider,
    )

    batch = monitor.collect()

    assert any(sample.value_text for sample in batch.samples)
    assert any(
        issue.reason_code == "RADAR_HTTP_THROTTLED_429"
        for issue in batch.issues
    )
    assert provider.reset_calls == 0
