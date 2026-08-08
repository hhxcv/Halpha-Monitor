from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from halpha_monitor.monitors.a_hk_buyback import PublicResponse
from halpha_monitor.monitors.stock_events import (
    StockEventMonitor,
    StockEventSettings,
)
from halpha_monitor.service import MonitorRegistry
from halpha_monitor.store import SQLiteMonitorStore
from halpha_monitor.web import create_app


NOW = datetime(2026, 8, 8, 2, 0, tzinfo=UTC)


class FakeStockEventClient:
    def __init__(
        self,
        *,
        fail_directory: bool = False,
        announcement_total_hits: int | None = None,
        fail_announcement_page: int | None = None,
    ) -> None:
        self.urls: list[str] = []
        self.fail_directory = fail_directory
        self.announcement_total_hits = announcement_total_hits
        self.fail_announcement_page = fail_announcement_page

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
        content_type: str | None = None,
        referer: str | None = None,
        max_bytes: int,
        attempts: int = 3,
    ) -> PublicResponse:
        del method, body, content_type, referer, max_bytes, attempts
        self.urls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        if (
            self.fail_directory
            and query.get("reportName") == ["RPT_STOCK_HEADERCHANGE"]
        ):
            raise OSError("synthetic directory outage")
        if parsed.path.endswith("getTopicZTPool"):
            payload = self._pool(
                [
                    {"c": "600519", "n": "贵州茅台", "hybk": "白酒"},
                    {"c": "300059", "n": "东方财富", "hybk": "证券"},
                ]
            )
        elif parsed.path.endswith("getTopicQSPool"):
            payload = self._pool(
                [
                    {"c": "300059", "n": "东方财富", "hybk": "证券"},
                    {"c": "000001", "n": "平安银行", "hybk": "银行"},
                ]
            )
        elif parsed.path.endswith("getTopicCXPool"):
            payload = self._pool(
                [{"c": "000001", "n": "平安银行", "hybk": "银行"}]
            )
        elif query.get("reportName") == ["RPT_STOCK_HEADERCHANGE"]:
            payload = {
                "success": True,
                "result": {
                    "pages": 1,
                    "count": 4,
                    "data": [
                        {
                            "SECURITY_CODE": "000001",
                            "SECURITY_NAME_ABBR": "平安银行",
                            "SECUCODE": "000001.SZ",
                            "TRADE_MARKET_CODE": "069001002001",
                        },
                        {
                            "SECURITY_CODE": "300059",
                            "SECURITY_NAME_ABBR": "东方财富",
                            "SECUCODE": "300059.SZ",
                            "TRADE_MARKET_CODE": "069001002002",
                        },
                        {
                            "SECURITY_CODE": "600519",
                            "SECURITY_NAME_ABBR": "贵州茅台",
                            "SECUCODE": "600519.SH",
                            "TRADE_MARKET_CODE": "069001001001",
                        },
                    ],
                },
            }
        elif query.get("reportName") == ["RPT_STOCKCALENDAR"]:
            payload = {
                "success": True,
                "result": {
                    "pages": 1,
                    "count": 3,
                    "data": [
                        {
                            "SECURITY_CODE": "600519",
                            "NOTICE_DATE": "2026-08-22 00:00:00",
                            "EVENT_TYPE": "预约披露日",
                            "EVENT_TYPE_CODE": "006",
                            "LEVEL1_CONTENT": "2026年半年报预约披露",
                        },
                        {
                            "SECURITY_CODE": "300059",
                            "NOTICE_DATE": "2026-08-07 00:00:00",
                            "EVENT_TYPE": "分红",
                            "EVENT_TYPE_CODE": "004",
                            "LEVEL1_CONTENT": "权益分派实施",
                        },
                        {
                            "SECURITY_CODE": "300059",
                            "NOTICE_DATE": "2026-08-07 00:00:00",
                            "EVENT_TYPE": "龙虎榜",
                            "EVENT_TYPE_CODE": "012",
                            "LEVEL1_CONTENT": "不属于公司事件",
                        },
                    ],
                },
            }
        elif parsed.path.endswith("/api/security/ann"):
            page = int(query.get("page_index", ["1"])[0])
            if self.fail_announcement_page == page:
                raise OSError("synthetic announcement outage")
            requested = set(query.get("stock_list", [""])[0].split(","))
            rows = []
            if "600519" in requested:
                rows.append(
                    self._announcement(
                        "AN202608070001",
                        "600519",
                        "贵州茅台",
                        "贵州茅台:关于董事会会议决议的公告",
                    )
                )
            if "000001" in requested:
                rows.append(
                    self._announcement(
                        "AN202608070002",
                        "000001",
                        "平安银行",
                        "平安银行:2026年半年度业绩预告",
                    )
                )
            if "300059" in requested:
                rows.append(
                    self._announcement(
                        "AN202608070003",
                        "300059",
                        "东方财富",
                        "东方财富:日常经营说明公告",
                    )
                )
            payload = {
                "success": 1,
                "error": "",
                "data": {
                    "list": rows,
                    "page_index": page,
                    "page_size": 100,
                    "total_hits": (
                        self.announcement_total_hits
                        if self.announcement_total_hits is not None
                        else len(rows)
                    ),
                },
            }
        else:
            raise AssertionError(f"unexpected URL: {url}")
        encoded = json.dumps(payload, ensure_ascii=False).encode()
        return PublicResponse(
            body=encoded,
            status=200,
            content_type="application/json",
            started_at=NOW,
            completed_at=NOW,
            headers={},
        )

    @staticmethod
    def _pool(rows: list[dict[str, object]]) -> dict[str, object]:
        return {"rc": 0, "data": {"tc": len(rows), "qdate": 20260807, "pool": rows}}

    @staticmethod
    def _announcement(
        art_code: str,
        code: str,
        name: str,
        title: str,
    ) -> dict[str, object]:
        return {
            "art_code": art_code,
            "codes": [{"stock_code": code, "short_name": name}],
            "columns": [{"column_name": "公司公告"}],
            "display_time": "2026-08-07 18:30:00:000",
            "notice_date": "2026-08-07 00:00:00",
            "title": title,
            "title_ch": title,
        }

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        del window_seconds
        return len(self.urls)


def _store(tmp_path: Path) -> SQLiteMonitorStore:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    return store


def test_stock_event_monitor_builds_manual_and_bounded_daily_projection(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    client = FakeStockEventClient()
    monitor = StockEventMonitor(
        StockEventSettings(
            auto_limit=3,
            manual_stock_codes=("600519",),
        ),
        store=store,
        client=client,
        now=lambda: NOW,
    )

    batch = monitor.collect()
    run_id = store.start_run(monitor.monitor_id, started_at=NOW)
    store.finish_run(run_id, monitor.monitor_id, batch, completed_at=NOW)
    snapshot = store.projection_snapshot(monitor.monitor_id, "current")

    assert snapshot is not None
    payload = snapshot.payload
    assert payload["selection_trade_date"] == "2026-08-07"
    assert len([item for item in payload["securities"] if item["is_auto"]]) == 3
    assert payload["securities"][0]["code"] == "600519"
    assert payload["securities"][0]["origin"] == "BOTH"
    assert {item["code"] for item in payload["securities"]} == {
        "000001",
        "300059",
        "600519",
    }
    events = payload["events"]
    assert any(
        item["stock_code"] == "600519" and item["state"] == "UPCOMING"
        for item in events
    )
    assert any(item["event_id"] == "announcement:AN202608070001" for item in events)
    assert any(item["event_id"] == "announcement:AN202608070002" for item in events)
    assert not any(item["event_id"] == "announcement:AN202608070003" for item in events)
    assert not any(item["title"] == "龙虎榜" for item in events)
    assert batch.issues == ()


def test_stock_announcement_truncation_keeps_bounded_batch_context(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    client = FakeStockEventClient(announcement_total_hits=250)
    monitor = StockEventMonitor(
        StockEventSettings(auto_limit=3),
        store=store,
        client=client,
        now=lambda: NOW,
    )

    batch = monitor.collect()
    issue = next(
        item
        for item in batch.issues
        if item.reason_code == "STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED"
    )
    assert issue.context == {
        "selection_origin": "AUTO",
        "stock_codes": "600519,300059,000001",
        "stock_count": 3,
        "window_start": "2026-08-01",
        "window_end": "2026-08-08",
        "page_size": 100,
        "page_limit": 2,
        "pages_read": 2,
        "records_read": 6,
        "upstream_total_hits": 250,
    }
    announcement_urls = [
        url for url in client.urls if urlparse(url).path.endswith("/api/security/ann")
    ]
    assert len(announcement_urls) == 2

    run_id = store.start_run(monitor.monitor_id, started_at=NOW)
    store.finish_run(run_id, monitor.monitor_id, batch, completed_at=NOW)
    reopened = SQLiteMonitorStore(store.path)
    reopened.initialize()
    stored_issue = next(
        item
        for item in reopened.recent_issues(monitor.monitor_id)
        if item.reason_code == "STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED"
    )
    assert stored_issue.context == issue.context


def test_stock_announcement_failure_keeps_page_and_partial_read_context(
    tmp_path: Path,
) -> None:
    monitor = StockEventMonitor(
        StockEventSettings(auto_limit=3),
        store=_store(tmp_path),
        client=FakeStockEventClient(
            announcement_total_hits=250,
            fail_announcement_page=2,
        ),
        now=lambda: NOW,
    )

    batch = monitor.collect()
    issue = next(
        item
        for item in batch.issues
        if item.reason_code == "STOCK_EVENTS_ANNOUNCEMENT_UNAVAILABLE"
    )

    assert issue.context is not None
    assert issue.context["selection_origin"] == "AUTO"
    assert issue.context["failed_page"] == 2
    assert issue.context["pages_read"] == 1
    assert issue.context["records_read"] == 3
    assert issue.context["upstream_total_hits"] == 250


def test_stock_event_monitor_reuses_same_trade_day_universe(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first_client = FakeStockEventClient()
    monitor = StockEventMonitor(
        StockEventSettings(auto_limit=2, manual_stock_codes=("600519",)),
        store=store,
        client=first_client,
        now=lambda: NOW,
    )
    first = monitor.collect()
    run_id = store.start_run(monitor.monitor_id, started_at=NOW)
    store.finish_run(run_id, monitor.monitor_id, first, completed_at=NOW)

    second_client = FakeStockEventClient()
    monitor.client = second_client
    second = monitor.collect()

    assert second.projection_snapshots
    assert not any("getTopic" in url for url in second_client.urls)
    assert not any("RPT_STOCK_HEADERCHANGE" in url for url in second_client.urls)
    source = second.projection_snapshots[0].payload["source_states"][0]
    assert source["detail"] == "沿用本交易日已冻结名单"


def test_stock_directory_refresh_is_persistent_and_attempted_at_most_daily(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    first_client = FakeStockEventClient()
    first_monitor = StockEventMonitor(
        StockEventSettings(auto_limit=2, manual_stock_codes=("600519",)),
        store=store,
        client=first_client,
        now=lambda: NOW,
    )
    first = first_monitor.collect()
    first_run = store.start_run(first_monitor.monitor_id, started_at=NOW)
    store.finish_run(first_run, first_monitor.monitor_id, first, completed_at=NOW)

    directory = store.projection_snapshot(first_monitor.monitor_id, "stock-directory")
    assert directory is not None
    assert directory.payload["status"] == "SUCCESS"
    assert directory.payload["record_count"] == 3
    assert sum("RPT_STOCK_HEADERCHANGE" in url for url in first_client.urls) == 1

    failed_client = FakeStockEventClient(fail_directory=True)
    failed_at = NOW + timedelta(hours=25)
    failed_monitor = StockEventMonitor(
        StockEventSettings(auto_limit=2, manual_stock_codes=("600519",)),
        store=store,
        client=failed_client,
        now=lambda: failed_at,
    )
    failed = failed_monitor.collect()
    failed_run = store.start_run(failed_monitor.monitor_id, started_at=failed_at)
    store.finish_run(
        failed_run,
        failed_monitor.monitor_id,
        failed,
        completed_at=failed_at,
    )
    stale = store.projection_snapshot(failed_monitor.monitor_id, "stock-directory")
    assert stale is not None
    assert stale.payload["status"] == "STALE"
    assert stale.payload["record_count"] == 3
    assert sum("RPT_STOCK_HEADERCHANGE" in url for url in failed_client.urls) == 1

    gated_client = FakeStockEventClient(fail_directory=True)
    gated_monitor = StockEventMonitor(
        StockEventSettings(auto_limit=2, manual_stock_codes=("600519",)),
        store=store,
        client=gated_client,
        now=lambda: failed_at + timedelta(hours=1),
    )
    gated_monitor.collect()
    assert not any("RPT_STOCK_HEADERCHANGE" in url for url in gated_client.urls)


def test_stock_event_watchlist_configuration_is_atomic_and_bounded(
    tmp_path: Path,
) -> None:
    monitor = StockEventMonitor(store=_store(tmp_path), client=FakeStockEventClient())

    normalized = monitor.normalize_configuration(
        {"manual_stock_codes": "600519，300059 600519"}
    )
    assert normalized == {"manual_stock_codes": ["600519", "300059"]}
    monitor.apply_configuration(normalized)
    assert monitor.configuration() == normalized

    with pytest.raises(ValueError, match="STOCK_EVENTS_STOCK_CODE_INVALID"):
        monitor.normalize_configuration({"manual_stock_codes": ["AAPL"]})
    with pytest.raises(ValueError, match="STOCK_EVENTS_WATCHLIST_TOO_LARGE"):
        monitor.normalize_configuration(
            {"manual_stock_codes": [f"{index:06d}" for index in range(51)]}
        )


def test_stock_event_web_projection_filters_dynamic_stock_selection(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    monitor = StockEventMonitor(
        StockEventSettings(auto_limit=3, manual_stock_codes=("600519",)),
        store=store,
        client=FakeStockEventClient(),
        now=lambda: NOW,
    )
    batch = monitor.collect()
    run_id = store.start_run(monitor.monitor_id, started_at=NOW)
    store.finish_run(run_id, monitor.monitor_id, batch, completed_at=NOW)
    registry = MonitorRegistry()
    registry.register(monitor)

    with TestClient(
        create_app(store, registry, None, start_scheduler=False),
        base_url="http://127.0.0.1:8790",
    ) as client:
        response = client.get(
            "/api/view",
            params=[
                ("monitor_id", monitor.monitor_id),
                ("stock_code", "600519"),
                ("stock_origin", "MANUAL"),
                ("category", "EARNINGS"),
                ("importance", "*"),
            ],
        )
        invalid = client.get(
            "/api/view",
            params={"monitor_id": monitor.monitor_id, "stock_code": "999999"},
        )
        by_name = client.get(
            f"/api/monitors/{monitor.monitor_id}/stocks/search",
            params={"q": "茅台", "limit": 8},
        )
        by_code = client.get(
            f"/api/monitors/{monitor.monitor_id}/stocks/search",
            params={"q": "300", "limit": 8},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["monitor"]["projection_kind"] == "stock_events"
    assert payload["monitor"]["configuration"]["fields"][0] == {
        "key": "manual_stock_codes",
        "label": "手动关注股票",
        "kind": "stock_list",
        "unit": None,
        "minimum": None,
        "step": None,
        "description": "通过中文名称或代码搜索沪深京 A 股；手动关注长期保留，可随时移除。",
        "placeholder": "例如 贵州茅台 / 600519",
        "maximum_items": 50,
        "choices": [],
    }
    assert payload["stock_events"]["selected_stock_codes"] == ["600519"]
    assert payload["stock_events"]["event_count"] == 1
    assert payload["rows"][0]["state"] == "UPCOMING"
    assert invalid.status_code == 422
    assert invalid.json()["detail"] == "STOCK_EVENT_SELECTION_INVALID"
    assert by_name.status_code == 200
    assert by_name.json()["status"] == "SUCCESS"
    assert by_name.json()["matches"][0]["code"] == "600519"
    assert by_name.json()["matches"][0]["name"] == "贵州茅台"
    assert by_name.json()["next_update_at"] is not None
    assert by_code.status_code == 200
    assert by_code.json()["matches"][0]["code"] == "300059"


def test_stock_directory_search_is_explicitly_unavailable_before_first_refresh(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    monitor = StockEventMonitor(store=store, client=FakeStockEventClient())
    registry = MonitorRegistry()
    registry.register(monitor)

    with TestClient(
        create_app(store, registry, None, start_scheduler=False),
        base_url="http://127.0.0.1:8790",
    ) as client:
        response = client.get(
            f"/api/monitors/{monitor.monitor_id}/stocks/search",
            params={"q": "贵州"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "UNAVAILABLE"
    assert response.json()["matches"] == []
