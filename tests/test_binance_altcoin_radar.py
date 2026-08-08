from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.error import HTTPError

import pytest

from halpha_monitor.contracts import ForwardEvaluationCase
from halpha_monitor.monitors.binance_altcoin_radar import (
    BinanceAltcoinRadarClient,
    BinanceAltcoinRadarMonitor,
    BinanceAltcoinRadarSettings,
    BinanceUsdmDailyCache,
    BASELINE_EVALUATION_SOURCE,
    CandleFeatures,
    ContractCandle,
    DailyContractCandle,
    DailySeriesResult,
    EVALUATION_SOURCE,
    FundingContext,
    OpenInterestPoint,
    RadarSourceError,
    RollingTicker,
    TimedValue,
    analyze_price_position,
    classify_contextual_stage,
    latest_closed_daily_cutoff,
    open_interest_change_15m,
    parse_contract_klines,
    parse_daily_contract_klines,
    parse_exchange_symbols,
    parse_tickers,
    score_candidate,
    select_candidate_seeds,
)
from halpha_monitor.store import SQLiteMonitorStore


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


def candles(*, accelerating: bool = False) -> tuple[ContractCandle, ...]:
    values: list[ContractCandle] = []
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
            ContractCandle(
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


def future_candles(
    cutoff_at: datetime,
    *,
    entry_price: float,
    closes: tuple[float, ...],
) -> tuple[ContractCandle, ...]:
    values: list[ContractCandle] = []
    previous = entry_price
    for index, close in enumerate(closes):
        open_time = cutoff_at + timedelta(milliseconds=1, minutes=index * 5)
        values.append(
            ContractCandle(
                open_time=open_time,
                close_time=open_time + timedelta(minutes=5) - timedelta(milliseconds=1),
                open_price=previous,
                high_price=max(previous, close) * 1.001,
                low_price=min(previous, close) * 0.999,
                close_price=close,
                quote_volume=1_000,
                trade_count=100,
                taker_buy_quote_volume=500,
            )
        )
        previous = close
    return tuple(values)


def daily_candles_from_closes(
    closes: tuple[float, ...],
    *,
    cutoff: datetime | None = None,
) -> tuple[DailyContractCandle, ...]:
    daily_cutoff = cutoff or latest_closed_daily_cutoff(NOW)
    next_midnight = daily_cutoff + timedelta(milliseconds=1)
    start = next_midnight - timedelta(days=len(closes))
    values: list[DailyContractCandle] = []
    previous = closes[0]
    for index, close in enumerate(closes):
        open_time = start + timedelta(days=index)
        values.append(
            DailyContractCandle(
                open_time=open_time,
                close_time=open_time + timedelta(days=1) - timedelta(milliseconds=1),
                open_price=previous,
                high_price=max(previous, close) * 1.01,
                low_price=min(previous, close) * 0.99,
                close_price=close,
                quote_volume=10_000_000,
            )
        )
        previous = close
    return tuple(values)


def test_parse_contract_klines_uses_only_closed_valid_rows() -> None:
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

    parsed, malformed = parse_contract_klines(rows, completed_at=completed)

    assert len(parsed) == 1
    assert parsed[0].close_price == 1.5
    assert malformed == 1


def test_parse_daily_contract_klines_keeps_only_closed_utc_days() -> None:
    completed = datetime(2026, 8, 6, 12, 1, tzinfo=UTC)
    open_at = datetime(2026, 8, 5, tzinfo=UTC)
    closed_at = open_at + timedelta(days=1) - timedelta(milliseconds=1)
    open_ms = int(open_at.timestamp() * 1000)
    close_ms = int(closed_at.timestamp() * 1000)
    future_open_ms = int(datetime(2026, 8, 6, tzinfo=UTC).timestamp() * 1000)
    rows = [
        [
            open_ms,
            "100",
            "110",
            "90",
            "105",
            "10",
            close_ms,
            "1000000",
        ],
        [
            future_open_ms,
            "105",
            "115",
            "100",
            "110",
            "10",
            future_open_ms + 86_399_999,
            "1000000",
        ],
        [
            open_ms + 1_000,
            "100",
            "110",
            "90",
            "105",
            "10",
            close_ms + 1_000,
            "1000000",
        ],
        ["malformed"],
    ]

    parsed, malformed = parse_daily_contract_klines(rows, completed_at=completed)

    assert len(parsed) == 1
    assert parsed[0].close_time == closed_at
    assert parsed[0].close_price == 105
    assert malformed == 2


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


def test_exchange_symbols_keeps_only_coin_backed_usdt_perpetuals() -> None:
    parsed, malformed = parse_exchange_symbols(
        {
            "symbols": [
                {
                    "symbol": "FUTURESONLYUSDT",
                    "baseAsset": "FUTURESONLY",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "underlyingType": "COIN",
                },
                {
                    "symbol": "QUARTERUSDT",
                    "baseAsset": "QUARTER",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "CURRENT_QUARTER",
                    "underlyingType": "COIN",
                },
                {
                    "symbol": "INDEXUSDT",
                    "baseAsset": "INDEX",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "underlyingType": "INDEX",
                },
            ]
        }
    )

    assert parsed == {"FUTURESONLYUSDT": "FUTURESONLY"}
    assert malformed == 0


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
        close_price=100.0,
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
        close_price=100.0,
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
        close_price=100.0,
    )

    assert (
        score_candidate(
            setup,
            return_24h_percent=4,
            relative_return_15m_percent=1,
            funding_rate_percent=0.01,
            oi_change_15m_percent=1,
        )["stage"]
        == "SETUP"
    )
    assert (
        score_candidate(
            acceleration,
            return_24h_percent=25,
            relative_return_15m_percent=5,
            funding_rate_percent=0.01,
            oi_change_15m_percent=4,
        )["stage"]
        == "ACCELERATION"
    )
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
            close_price=100.0,
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


def test_price_position_classifies_fast_move_bottom_and_persistent_decline() -> None:
    flat_then_rising = tuple([100.0] * 113 + [100, 104, 108, 113, 118, 124, 130])
    pumping_candles = daily_candles_from_closes(flat_then_rising)
    pumping = analyze_price_position(
        pumping_candles,
        current_price=132.0,
        return_24h_percent=4.0,
    )
    assert pumping.state == "PUMPING"
    assert pumping.return_7d_percent >= 25
    assert pumping.current_pump_multiple == pytest.approx(1.32)
    assert pumping.pump_baseline_price == pytest.approx(100.0)
    assert pumping.pump_start_at == pumping_candles[-6].open_time
    assert pumping.pump_start_trigger == "DAILY_WINDOW_7D_25"

    falling_then_flat = tuple(
        [120.0 - index * 0.8 for index in range(90)]
        + [48.0 + (index % 3) * 0.2 for index in range(30)]
    )
    bottom = analyze_price_position(
        daily_candles_from_closes(falling_then_flat),
        current_price=48.2,
        return_24h_percent=0.1,
    )
    assert bottom.state == "BOTTOM_CONSOLIDATION"
    assert bottom.position_90d_percent <= 20
    assert bottom.current_pump_multiple is None
    assert bottom.pump_baseline_price is None
    assert bottom.pump_start_at is None

    persistent_fall = tuple(220.0 - index for index in range(120))
    decline = analyze_price_position(
        daily_candles_from_closes(persistent_fall),
        current_price=98.0,
        return_24h_percent=-2.0,
    )
    assert decline.state == "PERSISTENT_DECLINE"
    assert decline.trend_structure == "空头排列"

    crash = analyze_price_position(
        daily_candles_from_closes(tuple([100.0] * 120)),
        current_price=84.0,
        return_24h_percent=-16.0,
    )
    assert crash.state == "CRASH"


def test_current_pump_uses_pre_start_closes_and_resets_after_closed_drawdown() -> None:
    closes = tuple(
        [100.0] * 90
        + [120.0, 150.0, 120.0]
        + [120.0] * 7
        + [126.0, 132.0, 140.0, 148.0, 156.0, 164.0]
    )
    candles = daily_candles_from_closes(closes)

    result = analyze_price_position(
        candles,
        current_price=168.0,
        return_24h_percent=2.0,
    )

    assert result.state == "PUMPING"
    assert result.pump_start_at == candles[-6].open_time
    assert result.pump_baseline_price == pytest.approx(120.0)
    assert result.current_pump_multiple == pytest.approx(1.4)
    assert result.pump_start_trigger == "DAILY_WINDOW_7D_25"


def test_current_utc_day_can_be_the_pump_start_without_entering_baseline() -> None:
    candles = daily_candles_from_closes(tuple([100.0] * 120))

    result = analyze_price_position(
        candles,
        current_price=120.0,
        return_24h_percent=20.0,
    )

    assert result.state == "SURGE"
    assert result.pump_start_at == candles[-1].open_time + timedelta(days=1)
    assert result.pump_baseline_price == pytest.approx(100.0)
    assert result.current_pump_multiple == pytest.approx(1.2)
    assert result.pump_start_trigger == "DAILY_WINDOW_1D_15"


def test_contextual_stage_only_emits_direction_for_defined_price_combinations() -> None:
    surge = analyze_price_position(
        daily_candles_from_closes(tuple([100.0] * 120)),
        current_price=125.0,
        return_24h_percent=20.0,
    )
    blowoff = classify_contextual_stage("ACCELERATION", surge)
    assert blowoff.stage == "BLOWOFF_RISK"
    assert blowoff.direction == "DOWN"
    assert blowoff.evaluation_horizons_minutes == (240,)

    falling_then_flat = tuple(
        [120.0 - index * 0.8 for index in range(90)]
        + [48.0 + (index % 3) * 0.2 for index in range(30)]
    )
    bottom = analyze_price_position(
        daily_candles_from_closes(falling_then_flat),
        current_price=48.2,
        return_24h_percent=0.1,
    )
    bottom_setup = classify_contextual_stage("SETUP", bottom)
    assert bottom_setup.stage == "BOTTOM_SETUP"
    assert bottom_setup.direction == "UP"
    assert bottom_setup.evaluation_horizons_minutes == (240,)

    decline = analyze_price_position(
        daily_candles_from_closes(tuple(220.0 - index for index in range(120))),
        current_price=98.0,
        return_24h_percent=-2.0,
    )
    continuation = classify_contextual_stage("SETUP", decline)
    assert continuation.stage == "DECLINE_CONTINUATION"
    assert continuation.direction == "DOWN"
    assert continuation.evaluation_horizons_minutes == (240,)

    unconfirmed = classify_contextual_stage("SETUP", surge)
    assert unconfirmed.stage == "SETUP_UNCONFIRMED"
    assert unconfirmed.direction is None
    assert unconfirmed.evaluation_horizons_minutes == ()

    unavailable = classify_contextual_stage("BREAKOUT", None)
    assert unavailable.stage == "PRICE_CONTEXT_UNAVAILABLE"
    assert unavailable.direction is None


def test_price_position_finds_only_a_completed_previous_pump_peak() -> None:
    closes = (
        [100.0] * 30
        + [100.0 + index * 4.0 for index in range(14)]
        + [160.0]
        + [155.0 - index * 4.0 for index in range(14)]
        + [99.0 + index * 0.55 for index in range(61)]
    )
    result = analyze_price_position(
        daily_candles_from_closes(tuple(closes)),
        current_price=150.0,
        return_24h_percent=0.5,
    )

    assert result.state == "RETEST_PREVIOUS_PEAK"
    assert result.previous_pump_peak_at is not None
    assert result.previous_pump_peak_price is not None
    assert result.distance_from_previous_pump_peak_percent == pytest.approx(
        150.0 / result.previous_pump_peak_price * 100 - 100
    )


def test_daily_cache_fetches_once_per_closed_day_and_survives_reopen(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    cutoff = latest_closed_daily_cutoff(NOW)
    source_candles = daily_candles_from_closes(tuple([100.0] * 120), cutoff=cutoff)

    class DailyClient:
        def __init__(self) -> None:
            self.calls = 0

        def daily_klines(
            self,
            symbol: str,
            *,
            start_at: datetime,
            end_at: datetime,
            limit: int,
        ) -> TimedValue:
            del symbol, start_at, end_at, limit
            self.calls += 1
            return TimedValue(source_candles, NOW)

    client = DailyClient()
    cache_root = tmp_path / "cache"
    first = BinanceUsdmDailyCache(cache_root, client).fetch("AAAUSDT", cutoff)  # type: ignore[arg-type]
    second = BinanceUsdmDailyCache(cache_root, client).fetch("AAAUSDT", cutoff)  # type: ignore[arg-type]

    assert first.status == "FETCHED"
    assert second.status == "CACHE_CURRENT"
    assert len(second.candles) == 120
    assert client.calls == 1

    unicode_symbol = BinanceUsdmDailyCache(cache_root, client).fetch(
        "币安人生USDT",
        cutoff,
    )

    assert unicode_symbol.status == "FETCHED"
    assert client.calls == 2
    assert any(path.name.startswith("unicode-") for path in cache_root.iterdir())


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
        symbol: ticker(symbol, change=10, quote_volume=20_000_000) for symbol in symbols
    }
    selected = select_candidate_seeds(
        symbols,
        day,
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
                "AAAUSDT": ticker("AAAUSDT", change=24, quote_volume=30_000_000),
                "BBBUSDT": ticker("BBBUSDT", change=8, quote_volume=20_000_000),
            },
            NOW,
        )

    def klines(self, symbol: str, *, limit: int) -> TimedValue:
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


class FakeDailyProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, datetime]] = []

    def fetch(self, symbol: str, cutoff_at: datetime) -> DailySeriesResult:
        self.calls.append((symbol, cutoff_at))
        return DailySeriesResult(
            symbol=symbol,
            status="CACHE_CURRENT",
            candles=daily_candles_from_closes(
                tuple([100.0] * 120),
                cutoff=cutoff_at,
            ),
            latest_close_at=cutoff_at,
            acquired_at=NOW,
        )


def test_monitor_omits_incomplete_contract_and_preserves_valid_rows() -> None:
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
    assert set(by_symbol) == {"AAAUSDT"}
    assert by_symbol["AAAUSDT"].value_text
    assert by_symbol["AAAUSDT"].payload["universe_size"] == 2
    assert by_symbol["AAAUSDT"].payload["onchain_state"] == (
        "NOT_INCLUDED_IN_MARKET_SCORE"
    )
    assert by_symbol["AAAUSDT"].payload["data_scope_label"] == ("USDⓈ-M 永续合约")
    assert by_symbol["AAAUSDT"].payload["market_scope"] == "USDM_PERPETUAL"
    assert by_symbol["AAAUSDT"].payload["screened_contract_size"] == 2
    assert by_symbol["AAAUSDT"].payload["analyzed_contract_size"] == 1
    assert by_symbol["AAAUSDT"].series_key.endswith("|usdm-perpetual-alert-score")
    cutoff_at = datetime.fromisoformat(
        by_symbol["AAAUSDT"].payload["data_cutoff_at"].replace("Z", "+00:00")
    )
    valid_until = datetime.fromisoformat(
        by_symbol["AAAUSDT"].payload["valid_until"].replace("Z", "+00:00")
    )
    assert valid_until - cutoff_at == timedelta(minutes=15)
    assert by_symbol["AAAUSDT"].payload["review_window_label"] == ("每根 5m K 复核")
    assert "evidence_strength" not in by_symbol["AAAUSDT"].payload
    assert "evidence_strength_label" not in by_symbol["AAAUSDT"].payload
    assert any(
        issue.scope == "BBBUSDT" and issue.reason_code == "RADAR_KLINES_STALE"
        for issue in batch.issues
    )
    assert provider.reset_calls == 1
    taker_column = next(
        column for column in monitor.view.columns if column.key == "taker_buy_percent"
    )
    assert taker_column.show_sign is False
    assert not any(column.key == "coverage_label" for column in monitor.view.columns)
    assert any(field.key == "coverage_label" for field in monitor.view.summary_fields)
    assert [definition.key for definition in monitor.view.filters] == [
        "context_stage_group"
    ]
    assert [column.key for column in monitor.view.columns] == [
        "symbol",
        "context_stage_label",
        "price_state_label",
        "current_pump_multiple",
        "pump_start_date",
        "pump_baseline_price",
        "context_stage_reason",
        "evaluation_target_label",
        "alert_score",
        "return_15m_percent",
        "quote_volume_ratio_15m",
        "taker_buy_percent",
        "tail_risk_score",
        "relative_return_15m_percent",
        "evidence_label",
        "oi_change_15m_percent",
        "funding_rate_percent",
        "quote_volume_24h",
        "stage_label",
        "review_window_label",
        "valid_until",
        "data_cutoff_at",
    ]
    valid_until_column = next(
        column for column in monitor.view.columns if column.key == "valid_until"
    )
    assert valid_until_column.promote_when_uniform is True
    assert valid_until_column.uniform_summary_label == "本轮结论有效至"
    evidence_column = next(
        column for column in monitor.view.columns if column.key == "evidence_label"
    )
    assert evidence_column.label == "关键事实"
    assert evidence_column.priority == "primary"
    assert "独立来源佐证" in (evidence_column.description or "")
    assert all(
        column.description for column in monitor.view.columns if column.key != "symbol"
    )
    assert monitor.description.strip()
    assert monitor.view.show_description is False
    assert monitor.view.method_note is not None


def test_monitor_builds_one_current_price_position_snapshot() -> None:
    daily_provider = FakeDailyProvider()
    monitor = BinanceAltcoinRadarMonitor(
        BinanceAltcoinRadarSettings(
            min_quote_volume_24h=Decimal("5000000"),
            max_candidates=5,
            workers=2,
        ),
        client=FakeProvider(),
        daily_provider=daily_provider,
    )

    batch = monitor.collect()

    assert [symbol for symbol, _ in daily_provider.calls] == ["AAAUSDT"]
    assert len(batch.projection_snapshots) == 1
    snapshot = batch.projection_snapshots[0]
    assert snapshot.snapshot_key == monitor.price_position_snapshot_key
    assert snapshot.payload["counts"] == {
        "eligible": 1,
        "included": 1,
        "history_insufficient": 0,
        "unavailable": 0,
    }
    assert snapshot.payload["rows"][0]["symbol"] == "AAAUSDT"
    assert snapshot.payload["rows"][0]["price_state"] == "SURGE"
    assert snapshot.payload["rows"][0]["history_days"] == 120
    assert snapshot.payload["rows"][0]["current_pump_multiple"] == "1.050000"
    assert snapshot.payload["rows"][0]["pump_baseline_price"] == "100.000000000000"
    assert snapshot.payload["rows"][0]["pump_start_date"] == "2026-08-06"
    assert snapshot.payload["rows"][0]["pump_start_trigger"] == "ROLLING_24H_15"
    assert [column.key for column in monitor.price_position_columns[:7]] == [
        "symbol",
        "price_state_label",
        "state_reason",
        "current_price",
        "current_pump_multiple",
        "pump_start_date",
        "pump_baseline_price",
    ]
    sample = batch.samples[0]
    assert sample.payload["context_stage"] == "BLOWOFF_RISK"
    assert sample.payload["context_direction"] == "DOWN"
    assert sample.payload["context_evaluation_horizons_minutes"] == [240]
    assert sample.payload["price_context_price"] == sample.payload["close_price"]
    assert sample.payload["price_context_price_at"] == sample.payload["data_cutoff_at"]
    assert "不是期望盈利持仓期" in monitor.view.method_note
    assert not any(
        choice.value == "DATA_GAP" for choice in monitor.view.filters[0].choices
    )


def test_monitor_freezes_one_three_horizon_case_set_per_stage_episode(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = BinanceAltcoinRadarMonitor(
        BinanceAltcoinRadarSettings(max_candidates=5, workers=2),
        client=FakeProvider(),
        evaluation_store=store,
    )

    first = monitor.collect()
    assert [case.horizon_minutes for case in first.evaluation_cases] == [15, 60, 240]
    assert {case.entity_key for case in first.evaluation_cases} == {"AAAUSDT"}
    assert {case.direction for case in first.evaluation_cases} == {"UP"}
    assert {case.source for case in first.evaluation_cases} == {
        BASELINE_EVALUATION_SOURCE
    }

    run_id = store.start_run(monitor.monitor_id, started_at=NOW)
    store.finish_run(
        run_id,
        monitor.monitor_id,
        first,
        completed_at=NOW,
    )
    second = monitor.collect()
    assert second.evaluation_cases == ()


def test_monitor_freezes_price_context_and_exact_baseline_pair(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = BinanceAltcoinRadarMonitor(
        BinanceAltcoinRadarSettings(max_candidates=5, workers=2),
        client=FakeProvider(),
        daily_provider=FakeDailyProvider(),
        evaluation_store=store,
    )

    batch = monitor.collect()

    baseline = [
        case
        for case in batch.evaluation_cases
        if case.source == BASELINE_EVALUATION_SOURCE
    ]
    contextual = [
        case for case in batch.evaluation_cases if case.source == EVALUATION_SOURCE
    ]
    assert [case.horizon_minutes for case in baseline] == [15, 60, 240]
    assert len(contextual) == 1
    assert contextual[0].stage == "BLOWOFF_RISK"
    assert contextual[0].direction == "DOWN"
    assert contextual[0].horizon_minutes == 240
    paired = next(case for case in baseline if case.horizon_minutes == 240)
    assert contextual[0].source_cutoff_at == paired.source_cutoff_at
    assert contextual[0].entry_price_text == paired.entry_price_text
    assert contextual[0].benchmark_entry_price_text == paired.benchmark_entry_price_text


def test_due_case_uses_closed_asset_and_btc_candles_for_forward_result() -> None:
    class EvaluatingProvider(FakeProvider):
        def klines_between(
            self,
            symbol: str,
            *,
            start_at: datetime,
            end_at: datetime,
            limit: int,
        ) -> TimedValue:
            del end_at, limit
            if symbol == "BTCUSDT":
                values = future_candles(
                    start_at,
                    entry_price=100.0,
                    closes=(100.1, 100.2, 100.3),
                )
            else:
                values = future_candles(
                    start_at,
                    entry_price=10.0,
                    closes=(10.2, 10.4, 10.6),
                )
            return TimedValue(values, start_at + timedelta(minutes=16))

    cutoff = NOW - timedelta(milliseconds=1)
    case = ForwardEvaluationCase(
        case_key="AAAUSDT|BREAKOUT|fixture|15",
        entity_key="AAAUSDT",
        stage="BREAKOUT",
        stage_label="启动",
        direction="UP",
        signal_observed_at=NOW,
        source_cutoff_at=cutoff,
        horizon_minutes=15,
        due_at=cutoff + timedelta(minutes=15),
        entry_price_text="10",
        benchmark_entry_price_text="100",
        source=EVALUATION_SOURCE,
    )
    monitor = BinanceAltcoinRadarMonitor(
        BinanceAltcoinRadarSettings(max_candidates=5),
        client=EvaluatingProvider(),
    )

    results = monitor.evaluate((case,), now=case.due_at + timedelta(minutes=1))

    assert len(results) == 1
    assert results[0].status == "COMPLETE"
    assert results[0].verdict == "ALIGNED"
    assert results[0].forward_return_percent == pytest.approx(6.0)
    assert results[0].benchmark_return_percent == pytest.approx(0.3)
    assert results[0].relative_return_percent == pytest.approx(5.7)
    assert results[0].maximum_favorable_excursion_percent > 6.0
    assert results[0].maximum_adverse_excursion_percent < 0.0


def test_partial_throttle_does_not_clear_shared_backoff() -> None:
    class PartiallyThrottledProvider(FakeProvider):
        def klines(self, symbol: str, *, limit: int) -> TimedValue:
            if symbol == "BBBUSDT":
                raise RadarSourceError("RADAR_HTTP_THROTTLED_429", throttled=True)
            return super().klines(symbol, limit=limit)

    provider = PartiallyThrottledProvider()
    monitor = BinanceAltcoinRadarMonitor(
        BinanceAltcoinRadarSettings(max_candidates=5, workers=2),
        client=provider,
    )

    batch = monitor.collect()

    assert any(sample.value_text for sample in batch.samples)
    assert any(
        issue.reason_code == "RADAR_HTTP_THROTTLED_429" for issue in batch.issues
    )
    assert provider.reset_calls == 0
