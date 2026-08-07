from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from urllib.error import HTTPError

import pytest

from halpha_monitor.contracts import CollectionArtifact
from halpha_monitor.monitors.binance_smart_money import (
    BINANCE_USDM_BASE,
    BINANCE_WEB_BASE,
    BinanceSmartMoneyClient,
    BinanceSmartMoneyMonitor,
    BinanceSmartMoneySettings,
    RecordedJsonResponse,
    SmartMoneyMonitorError,
    parse_stats,
    schema_hash,
)


NOW = datetime(2026, 7, 27, 8, 0, tzinfo=UTC)


def test_settings_bound_cli_symbol_fanout() -> None:
    with pytest.raises(ValueError, match="SMART_MONEY_SYMBOLS_INVALID"):
        BinanceSmartMoneySettings(
            symbols=tuple(f"ASSET{index}USDT" for index in range(21))
        )


def timestamp(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def bapi(data: object, *, total: int | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "code": "000000",
        "message": None,
        "messageDetail": None,
        "data": data,
        "success": True,
    }
    if total is not None:
        payload["total"] = total
    return payload


def overview(*, updated_at: datetime = NOW - timedelta(minutes=1)) -> dict[str, object]:
    return bapi(
        {
            "longProfitTraders": 70,
            "longProfitWhales": 7,
            "longShortRatio": 1.2,
            "longTraders": 100,
            "longTradersAvgEntryPrice": 9.5,
            "longTradersQty": 200,
            "longWhales": 10,
            "longWhalesAvgEntryPrice": 9.4,
            "longWhalesQty": 180,
            "shortProfitTraders": 60,
            "shortProfitWhales": 6,
            "shortTraders": 90,
            "shortTradersAvgEntryPrice": 10.5,
            "shortTradersQty": 150,
            "shortWhales": 9,
            "shortWhalesAvgEntryPrice": 10.6,
            "shortWhalesQty": 140,
            "symbol": "BTCUSDT",
            "totalPositions": 3500,
            "totalTraders": 190,
            "updateTime": timestamp(updated_at),
        }
    )


def stats() -> dict[str, object]:
    return bapi(
        {
            "longPositions": 100,
            "longQty": 10,
            "longTraders": 20,
            "longWhalePositions": 80,
            "longWhaleQty": 8,
            "longWhales": 4,
            "shortPositions": 300,
            "shortQty": 30,
            "shortTraders": 30,
            "shortWhalePositions": 180,
            "shortWhaleQty": 18,
            "shortWhales": 6,
        }
    )


def details(time_range: str) -> dict[str, object]:
    return bapi(
        [
            {
                "avgEntryPrice": None,
                "currentPositions": None,
                "currentQty": None,
                "lastTradeTime": timestamp(NOW - timedelta(seconds=20)),
                "netPositions": 100,
                "side": "BUY",
                "timeRange": time_range,
                "topTraderId": None,
                "traderName": None,
                "traders": 20,
                "userType": 2,
            }
        ],
        total=1,
    )


def recorded(
    artifact_key: str,
    payload: object,
    *,
    completed_at: datetime = NOW,
) -> RecordedJsonResponse:
    body = json.dumps(payload, separators=(",", ":"))
    normalized_payload = json.loads(body, parse_float=Decimal)
    return RecordedJsonResponse(
        artifact=CollectionArtifact(
            artifact_key=artifact_key,
            source=f"https://example.test/{artifact_key}",
            request_started_at=completed_at - timedelta(milliseconds=50),
            response_completed_at=completed_at,
            http_status=200,
            business_code=(
                str(normalized_payload.get("code"))
                if isinstance(normalized_payload, dict)
                and normalized_payload.get("code") is not None
                else None
            ),
            schema_hash=schema_hash(normalized_payload),
            response_sha256=sha256(body.encode()).hexdigest(),
            record_count=1,
            response_body=body,
        ),
        payload=normalized_payload,
    )


class FakeClient:
    def __init__(self, responses: dict[str, RecordedJsonResponse]) -> None:
        self.responses = responses
        self.reset_calls = 0

    def ensure_available(self) -> None:
        return None

    def reset_throttle_backoff(self) -> None:
        self.reset_calls += 1

    def get_json(
        self,
        *,
        artifact_key: str,
        base: str,
        path: str,
        params: tuple[tuple[str, str], ...],
    ) -> RecordedJsonResponse:
        assert base in {BINANCE_WEB_BASE, BINANCE_USDM_BASE}
        assert path.startswith("/")
        assert params
        return self.responses[artifact_key]


def fixture_responses(
    *,
    overview_payload: dict[str, object] | None = None,
) -> dict[str, RecordedJsonResponse]:
    payloads = {
        "BTCUSDT:overview": overview_payload or overview(),
        "BTCUSDT:open-interest": {
            "openInterest": "100",
            "symbol": "BTCUSDT",
            "time": timestamp(NOW - timedelta(seconds=5)),
        },
        "BTCUSDT:premium-index": {
            "lastFundingRate": "-0.0001",
            "markPrice": "10",
            "symbol": "BTCUSDT",
            "time": timestamp(NOW - timedelta(seconds=4)),
        },
        "BTCUSDT:30m:stats": stats(),
        "BTCUSDT:30m:details": details("30m"),
        "BTCUSDT:1h:stats": stats(),
        "BTCUSDT:1h:details": details("1h"),
    }
    return {key: recorded(key, value) for key, value in payloads.items()}


def test_collects_raw_evidence_and_forward_features_for_each_window() -> None:
    client = FakeClient(fixture_responses())
    monitor = BinanceSmartMoneyMonitor(
        BinanceSmartMoneySettings(),
        client=client,  # type: ignore[arg-type]
    )

    batch = monitor.collect()

    assert batch.issues == ()
    assert len(batch.artifacts) == 7
    assert [sample.series_key for sample in batch.samples] == [
        "BTCUSDT|30m|normalized-flow",
        "BTCUSDT|1h|normalized-flow",
    ]
    sample = batch.samples[0]
    assert sample.value_text == "-20"
    assert sample.payload["flow_imbalance"] == "-0.5"
    assert sample.payload["normalized_flow"] == "-0.2"
    assert float(sample.payload["whale_divergence"]) == pytest.approx(
        0.32967032967032966
    )
    assert sample.payload["last_funding_rate_percent"] == "-0.01"
    assert sample.payload["overview_freshness"] == "新鲜"
    assert client.reset_calls == 1


def test_stale_overview_is_visible_but_not_used_to_suppress_flow_features() -> None:
    client = FakeClient(
        fixture_responses(
            overview_payload=overview(updated_at=NOW - timedelta(hours=8))
        )
    )
    monitor = BinanceSmartMoneyMonitor(
        BinanceSmartMoneySettings(overview_stale_seconds=3600),
        client=client,  # type: ignore[arg-type]
    )

    batch = monitor.collect()

    assert len(batch.samples) == 2
    assert batch.samples[0].payload["overview_freshness"] == "陈旧，不用于特征"
    assert len(batch.issues) == 1
    assert batch.issues[0].reason_code == "SMART_MONEY_OVERVIEW_STALE"


def test_uncomputable_divergence_stays_empty_with_an_in_place_reason() -> None:
    responses = fixture_responses()
    no_non_whale_flow = stats()
    assert isinstance(no_non_whale_flow["data"], dict)
    no_non_whale_flow["data"]["longWhalePositions"] = 100
    no_non_whale_flow["data"]["shortWhalePositions"] = 300
    responses["BTCUSDT:30m:stats"] = recorded(
        "BTCUSDT:30m:stats",
        no_non_whale_flow,
    )
    monitor = BinanceSmartMoneyMonitor(
        BinanceSmartMoneySettings(),
        client=FakeClient(responses),  # type: ignore[arg-type]
    )

    batch = monitor.collect()

    sample = batch.samples[0]
    assert sample.payload["whale_divergence_percent"] is None
    assert sample.payload["missing_reasons"] == {
        "whale_divergence_percent": (
            "巨鲸或非巨鲸资金流为空，无法计算分歧；未使用替代值。"
        )
    }
    assert "overview_freshness" not in {
        column.key for column in monitor.view.columns
    }


def test_schema_change_fails_closed_before_any_window_sample() -> None:
    responses = fixture_responses()
    changed = stats()
    assert isinstance(changed["data"], dict)
    changed["data"]["unexpected"] = 1
    responses["BTCUSDT:30m:stats"] = recorded("BTCUSDT:30m:stats", changed)
    client = FakeClient(responses)
    monitor = BinanceSmartMoneyMonitor(
        BinanceSmartMoneySettings(),
        client=client,  # type: ignore[arg-type]
    )

    batch = monitor.collect()

    assert batch.samples == ()
    assert [issue.reason_code for issue in batch.issues] == [
        "SMART_MONEY_SCHEMA_CHANGED"
    ]
    assert {artifact.artifact_key for artifact in batch.artifacts} == {
        "BTCUSDT:overview",
        "BTCUSDT:open-interest",
        "BTCUSDT:premium-index",
        "BTCUSDT:30m:stats",
    }


def test_whales_must_remain_a_subset_of_all_traders_and_positions() -> None:
    payload = stats()
    assert isinstance(payload["data"], dict)
    payload["data"]["longWhalePositions"] = 101

    with pytest.raises(
        SmartMoneyMonitorError, match="SMART_MONEY_WHALE_SUBSET_INVALID"
    ):
        parse_stats(payload)


class ThrottledOpener:
    def __init__(self) -> None:
        self.calls = 0

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        self.calls += 1
        raise HTTPError(
            request.full_url,
            429,
            "Too Many Requests",
            hdrs=None,
            fp=io.BytesIO(b'{"code":-1003,"msg":"rate limited"}'),
        )


def test_http_429_opens_exponential_backoff_without_a_second_request() -> None:
    opener = ThrottledOpener()
    client = BinanceSmartMoneyClient(
        timeout_seconds=1,
        opener=opener,  # type: ignore[arg-type]
        now=lambda: NOW,
        random_uniform=lambda _minimum, _maximum: 0,
    )

    with pytest.raises(SmartMoneyMonitorError) as first:
        client.get_json(
            artifact_key="BTCUSDT:overview",
            base=BINANCE_WEB_BASE,
            path="/example",
            params=(("symbol", "BTCUSDT"),),
        )
    with pytest.raises(
        SmartMoneyMonitorError, match="SMART_MONEY_BACKOFF_ACTIVE"
    ):
        client.get_json(
            artifact_key="BTCUSDT:overview",
            base=BINANCE_WEB_BASE,
            path="/example",
            params=(("symbol", "BTCUSDT"),),
        )

    assert first.value.reason_code == "SMART_MONEY_HTTP_THROTTLED_429"
    assert first.value.artifact is not None
    assert first.value.artifact.http_status == 429
    assert opener.calls == 1
