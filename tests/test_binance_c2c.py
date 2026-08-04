import io
import json
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from halpha_monitor.monitors.binance_c2c import (
    BinanceC2CMonitor,
    BinanceC2CSettings,
    BinancePublicClient,
    C2CAd,
    C2CMonitorError,
    SpotConversion,
    ad_supports_target,
    choose_best_ad,
    normalize_ad,
    parse_ads,
)


NOW = datetime(2026, 7, 22, 5, 0, tzinfo=UTC)


def ad(
    asset: str,
    price: str,
    *,
    minimum: str = "0",
    maximum: str = "100000",
    tradable: str = "100000",
) -> C2CAd:
    return C2CAd(
        ad_no=f"{asset}-{price}",
        asset=asset,
        fiat="CNY",
        price=Decimal(price),
        min_fiat=Decimal(minimum),
        max_fiat=Decimal(maximum),
        tradable_asset=Decimal(tradable),
        trade_methods=("BANK",),
        retrieved_at=NOW,
    )


def test_target_filter_and_directional_best_price() -> None:
    constrained = ad("BTC", "100", minimum="200", maximum="500", tradable="4")
    assert ad_supports_target(constrained, Decimal("300")) is True
    assert ad_supports_target(constrained, Decimal("450")) is False

    ads = (ad("BTC", "105"), ad("BTC", "100"), ad("BTC", "110"))
    assert choose_best_ad(
        ads, trade_type="BUY", target_fiat=Decimal("1000")
    ).price == Decimal("100")
    assert choose_best_ad(
        ads, trade_type="SELL", target_fiat=Decimal("1000")
    ).price == Decimal("110")


def test_buy_uses_bid_and_sell_uses_ask() -> None:
    conversion = SpotConversion(
        asset="BTC",
        symbol="BTCUSDT",
        route="DIRECT",
        bid_usdt_per_asset=Decimal("50000"),
        ask_usdt_per_asset=Decimal("50010"),
        retrieved_at=NOW,
    )

    buy = normalize_ad(ad("BTC", "350000"), conversion, trade_type="BUY")
    sell = normalize_ad(ad("BTC", "350070"), conversion, trade_type="SELL")

    assert buy.spot_basis == "BID"
    assert buy.normalized_fiat_per_usdt == Decimal("7")
    assert sell.spot_basis == "ASK"
    assert sell.normalized_fiat_per_usdt == Decimal("7")


def test_parse_ads_rejects_invalid_limits() -> None:
    with pytest.raises(C2CMonitorError, match="C2C_AD_VALUES_INVALID"):
        parse_ads(
            [
                {
                    "adNo": "1",
                    "asset": "BTC",
                    "fiat": "CNY",
                    "price": "100",
                    "minTransAmount": "5",
                    "maxTransAmount": "4",
                    "tradableAmount": "10",
                    "tradeMethods": ["BANK"],
                }
            ],
            retrieved_at=NOW,
        )


class FakeResponse(io.BytesIO):
    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class RecordingOpener:
    def __init__(self) -> None:
        self.request = None

    def open(self, request: object, *, timeout: float) -> FakeResponse:
        self.request = request
        assert timeout == 10
        return FakeResponse(
            json.dumps(
                {
                    "code": "000000",
                    "success": True,
                    "data": {
                        "items": [
                            {
                                "adNo": "usdt-1",
                                "asset": "USDT",
                                "fiat": "CNY",
                                "price": "6.75",
                                "minTransAmount": "1000",
                                "maxTransAmount": "30000",
                                "tradableAmount": "5000",
                                "tradeMethods": ["ALIPAY"],
                            }
                        ]
                    },
                }
            ).encode()
        )


def test_fetch_ads_uses_official_agent_query_and_repeated_methods() -> None:
    opener = RecordingOpener()
    client = BinancePublicClient(timeout_seconds=10, opener=opener)  # type: ignore[arg-type]

    ads = client.fetch_ads(
        fiat="CNY",
        asset="USDT",
        trade_type="BUY",
        limit=20,
        trade_methods=("ALIPAY", "WECHAT"),
    )

    assert len(ads) == 1
    assert ads[0].min_fiat == Decimal("1000")
    assert ads[0].max_fiat == Decimal("30000")
    request = opener.request
    assert request is not None
    parsed = urlparse(request.full_url)
    assert parsed.path == "/bapi/c2c/v1/public/c2c/agent/ad-list"
    assert parse_qs(parsed.query) == {
        "fiat": ["CNY"],
        "asset": ["USDT"],
        "tradeType": ["BUY"],
        "limit": ["20"],
        "tradeMethodIdentifiers": ["ALIPAY", "WECHAT"],
    }
    assert request.data is None


class FakeClient:
    def canonical_trade_methods(
        self, fiat: str, requested: tuple[str, ...]
    ) -> tuple[str, ...]:
        assert fiat == "CNY"
        assert requested == ("BANK",)
        return requested

    def fetch_spot_conversion(self, asset: str) -> SpotConversion:
        price = Decimal(1) if asset == "USDT" else Decimal("50000")
        return SpotConversion(
            asset=asset,
            symbol=asset if asset == "USDT" else f"{asset}USDT",
            route="IDENTITY" if asset == "USDT" else "DIRECT",
            bid_usdt_per_asset=price,
            ask_usdt_per_asset=price,
            retrieved_at=NOW,
        )

    def fetch_ads(
        self,
        *,
        fiat: str,
        asset: str,
        trade_type: str,
        limit: int,
        trade_methods: tuple[str, ...],
    ) -> tuple[C2CAd, ...]:
        assert fiat == "CNY"
        assert limit == 20
        assert trade_methods == ("BANK",)
        prices = {
            ("BUY", "USDT"): "7",
            ("BUY", "BTC"): "350000",
            ("SELL", "USDT"): "6.9",
            ("SELL", "BTC"): "345000",
        }
        return (ad(asset, prices[(trade_type, asset)]),)


def test_registered_monitor_collects_both_sides_and_builds_stable_series() -> None:
    monitor = BinanceC2CMonitor(
        BinanceC2CSettings(
            assets=("USDT", "BTC"),
            trade_methods=("BANK",),
        ),
        client=FakeClient(),  # type: ignore[arg-type]
    )

    batch = monitor.collect()

    assert batch.issues == ()
    assert len(batch.samples) == 4
    assert [sample.entity_key for sample in batch.samples] == [
        "USDT",
        "BTC",
        "USDT",
        "BTC",
    ]
    assert {sample.payload["trade_type"] for sample in batch.samples} == {"BUY", "SELL"}
    assert {sample.value_text for sample in batch.samples} == {"7", "6.9"}
    assert batch.samples[0].payload["premium_pct"] == "0"
    assert batch.samples[2].payload["premium_pct"] == "0"
    assert all("BANK" in sample.series_key for sample in batch.samples)


def test_missing_usdt_benchmark_keeps_premium_empty_with_reason() -> None:
    settings = BinanceC2CSettings(assets=("BTC",), trade_methods=("BANK",))
    monitor = BinanceC2CMonitor(settings, client=FakeClient())  # type: ignore[arg-type]
    quote = normalize_ad(
        ad("BTC", "350000"),
        SpotConversion(
            asset="BTC",
            symbol="BTCUSDT",
            route="DIRECT",
            bid_usdt_per_asset=Decimal("50000"),
            ask_usdt_per_asset=Decimal("50010"),
            retrieved_at=NOW,
        ),
        trade_type="BUY",
    )

    sample = monitor._sample(quote, ("BANK",), settings)

    assert sample.payload["premium_pct"] is None
    assert sample.payload["missing_reasons"] == {
        "premium_pct": "缺少同方向 USDT 基准，无法计算相对值；未使用替代数据。"
    }


def test_default_and_editable_collection_configuration() -> None:
    monitor = BinanceC2CMonitor(BinanceC2CSettings(), client=FakeClient())  # type: ignore[arg-type]

    assert monitor.configuration() == {
        "target_fiat": "2000",
        "trade_methods": ["BANK", "ALIPAY", "WECHAT"],
    }

    normalized = monitor.normalize_configuration(
        {"target_fiat": "2500.00", "trade_methods": ["wechat", "ALIPAY"]}
    )
    monitor.apply_configuration(normalized)

    assert monitor.configuration() == {
        "target_fiat": "2500",
        "trade_methods": ["WECHAT", "ALIPAY"],
    }
    with pytest.raises(ValueError, match="C2C_TRADE_METHODS_INVALID"):
        monitor.normalize_configuration({"target_fiat": "2500", "trade_methods": []})
