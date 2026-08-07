from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from halpha_monitor.monitors.a_hk_buyback import PublicResponse
from halpha_monitor.monitors.market_events import (
    BEA_RELEASE_DATES_URL,
    BLS_API_URL,
    CONSENSUS_CALENDAR_URL,
    FOMC_CALENDAR_URL,
    NYFED_CALENDAR_URL,
    MarketEventMonitor,
    MarketEventSettings,
    MarketEventSourceError,
    nyfed_month_urls,
    parse_bea_schedule,
    parse_bls_indicators,
    parse_consensus_calendar,
    parse_fomc_calendar,
    parse_nyfed_calendar,
)
from halpha_monitor.store import SQLiteMonitorStore


NOW = datetime(2026, 8, 7, 6, 0, tzinfo=UTC)


def bea_payload(release_at: str = "2026-08-08T12:30:00+00:00") -> dict[str, object]:
    return {
        "Gross Domestic Product": {"release_dates": [release_at]},
        "Personal Income and Outlays": {"release_dates": [release_at]},
        "U.S. International Trade in Goods and Services": {
            "release_dates": [release_at]
        },
        "Corporate Profits": {"release_dates": [release_at]},
        "file_last_updated": "2026-08-07T04:00:00+00:00",
    }


def nyfed_html(*, employment_time: str = "08:30") -> str:
    cells = []
    for day in range(1, 21):
        content = ""
        if day == 8:
            content = (
                '<br><br><span class="ts-accordion-content">'
                '<a href="https://www.bls.gov/news.release/empsit.toc.htm">'
                f"Employment Situation</a><br>({employment_time})<br><br></span>"
            )
        elif day == 9:
            content = (
                '<br><br><span class="ts-accordion-content">'
                '<a href="https://www.bls.gov/news.release/cpi.toc.htm">'
                "Consumer Price Index</a><br>(08:30)<br><br></span>"
            )
        cells.append(
            f'<td class="somatdR dirColL"><div>{day}{content}</div></td>'
        )
    return (
        "<html><body><div align=\"center\">August 2026</div><table><tr>"
        + "".join(cells)
        + "</tr></table></body></html>"
    )


def fomc_html() -> str:
    def panel(year: int, months: tuple[tuple[str, str], ...]) -> str:
        meetings = "".join(
            (
                '<div class="row fomc-meeting">'
                '<div class="fomc-meeting__month"><strong>'
                f"{month}</strong></div>"
                f'<div class="fomc-meeting__date">{dates}</div></div>'
            )
            for month, dates in months
        )
        return (
            '<div class="panel panel-default"><div class="panel-heading">'
            f"<h4><a>{year} FOMC Meetings</a></h4></div>{meetings}</div>"
        )

    return panel(
        2026,
        (
            ("January", "27-28"),
            ("March", "17-18*"),
            ("August", "7-8*"),
            ("December", "8-9*"),
        ),
    ) + panel(
        2027,
        (
            ("January", "26-27"),
            ("March", "16-17*"),
            ("June", "15-16*"),
            ("December", "7-8*"),
        ),
    )


def bls_payload() -> dict[str, object]:
    def series(series_id: str, values: list[tuple[int, int, float]]) -> dict[str, object]:
        return {
            "seriesID": series_id,
            "data": [
                {
                    "year": str(year),
                    "period": f"M{month:02d}",
                    "periodName": "fixture",
                    "value": str(value),
                    "latest": "true" if index == 0 else "false",
                    "footnotes": ([{"code": "P", "text": "preliminary"}]
                                  if series_id == "CES0000000001" and index == 0
                                  else [{}]),
                }
                for index, (year, month, value) in enumerate(values)
            ],
        }

    return {
        "status": "REQUEST_SUCCEEDED",
        "message": [],
        "Results": {
            "series": [
                series("CUSR0000SA0", [(2026, 6, 310), (2026, 5, 309)]),
                series(
                    "CUUR0000SA0",
                    [(2026, 6, 312), (2026, 5, 311), (2025, 6, 300)],
                ),
                series(
                    "CUSR0000SA0L1E",
                    [(2026, 6, 320), (2026, 5, 319)],
                ),
                series(
                    "CUUR0000SA0L1E",
                    [(2026, 6, 322), (2026, 5, 321), (2025, 6, 310)],
                ),
                series(
                    "CES0000000001",
                    [(2026, 6, 160_100), (2026, 5, 160_000)],
                ),
                series("LNS14000000", [(2026, 6, 4.1), (2026, 5, 4.0)]),
            ]
        },
    }


def consensus_payload() -> list[dict[str, str]]:
    rows = [
        {
            "title": "Non-Farm Employment Change",
            "country": "USD",
            "date": "2026-08-08T08:30:00-04:00",
            "impact": "High",
            "forecast": "85K",
            "previous": "57K",
        },
        {
            "title": "Unemployment Rate",
            "country": "USD",
            "date": "2026-08-08T08:30:00-04:00",
            "impact": "High",
            "forecast": "4.2%",
            "previous": "4.1%",
        },
    ]
    rows.extend(
        {
            "title": f"Fixture {index}",
            "country": "USD",
            "date": f"2026-08-0{index}T10:00:00-04:00",
            "impact": "Low",
            "forecast": "",
            "previous": "",
        }
        for index in range(1, 9)
    )
    return rows


def bls_payload_july() -> dict[str, object]:
    payload = bls_payload()
    series_rows = payload["Results"]["series"]  # type: ignore[index]
    replacements = {
        "CUSR0000SA0": [(2026, 7, 311), (2026, 6, 310)],
        "CUUR0000SA0": [(2026, 7, 313), (2026, 6, 312), (2025, 7, 301)],
        "CUSR0000SA0L1E": [(2026, 7, 321), (2026, 6, 320)],
        "CUUR0000SA0L1E": [(2026, 7, 323), (2026, 6, 322), (2025, 7, 311)],
        "CES0000000001": [(2026, 7, 160_110), (2026, 6, 160_000)],
        "LNS14000000": [(2026, 7, 4.0), (2026, 6, 4.1)],
    }
    for series in series_rows:  # type: ignore[union-attr]
        values = replacements[str(series["seriesID"])]
        series["data"] = [
            {
                "year": str(year),
                "period": f"M{month:02d}",
                "periodName": "fixture",
                "value": str(value),
                "latest": "true" if index == 0 else "false",
                "footnotes": ([{"code": "P", "text": "preliminary"}]
                              if series["seriesID"] == "CES0000000001" and index == 0
                              else [{}]),
            }
            for index, (year, month, value) in enumerate(values)
        ]
    return payload


class FakeClient:
    def __init__(self, bodies: dict[str, bytes]) -> None:
        self.bodies = bodies
        self.calls: list[tuple[str, str]] = []

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
        del body, content_type, referer, max_bytes, attempts
        self.calls.append((method, url))
        payload = self.bodies[url]
        return PublicResponse(
            body=payload,
            status=200,
            content_type="application/json" if url.endswith("json") else "text/html",
            started_at=NOW,
            completed_at=NOW,
            headers={},
        )

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        assert window_seconds == 60
        return len(self.calls)


def test_official_schedule_parsers_preserve_precision_and_source_contracts() -> None:
    bea = parse_bea_schedule(
        bea_payload(),
        now=NOW,
        history_days=0,
        lookahead_days=30,
    )
    nyfed = parse_nyfed_calendar(
        nyfed_html(),
        expected_year=2026,
        expected_month=8,
        page_url=NYFED_CALENDAR_URL,
    )
    fomc = parse_fomc_calendar(
        fomc_html(),
        now=NOW,
        history_days=0,
        lookahead_days=30,
    )

    assert len(bea) == 4
    assert all(event.time_precision == "EXACT" for event in bea)
    assert [event.definition.title for event in nyfed] == [
        "美国非农就业报告",
        "美国CPI",
    ]
    assert nyfed[0].scheduled_at == datetime(2026, 8, 8, 12, 30, tzinfo=UTC)
    assert nyfed[0].official_release_url.startswith("https://www.bls.gov/")
    assert len(fomc) == 1
    assert fomc[0].time_precision == "DATE"
    assert fomc[0].scheduled_at is None
    assert fomc[0].definition.title == "美联储利率决议与经济预测"


def test_parsers_fail_closed_when_an_official_contract_changes() -> None:
    with pytest.raises(MarketEventSourceError, match="BEA_SCHEMA_CHANGED"):
        parse_bea_schedule(
            {"file_last_updated": "2026-08-07"},
            now=NOW,
            history_days=0,
            lookahead_days=30,
        )
    with pytest.raises(MarketEventSourceError, match="NYFED_SCHEMA_CHANGED"):
        parse_nyfed_calendar(
            "<html>August 2026</html>",
            expected_year=2026,
            expected_month=8,
            page_url=NYFED_CALENDAR_URL,
        )
    with pytest.raises(MarketEventSourceError, match="FOMC_SCHEMA_CHANGED"):
        parse_fomc_calendar(
            "<html></html>",
            now=NOW,
            history_days=0,
            lookahead_days=30,
        )


def test_bls_metrics_use_same_periods_and_explicit_calculations() -> None:
    cpi, employment = parse_bls_indicators(bls_payload())

    assert cpi["primary_value"] == "同比 +4.0%"
    assert cpi["secondary_value"] == "环比 +0.3%"
    assert cpi["period_label"] == "2026年6月"
    assert employment["primary_value"] == "新增非农 +10.0万人（初值）"
    assert employment["secondary_value"] == "失业率 4.1%"


def test_consensus_parser_normalizes_units_and_keeps_observation_time() -> None:
    parsed = parse_consensus_calendar(consensus_payload(), observed_at=NOW)

    assert [(item.metric_key, item.forecast_value) for item in parsed] == [
        ("payroll_change_thousand", 85.0),
        ("unemployment_rate", 4.2),
    ]
    assert all(item.observed_at == NOW for item in parsed)


def test_release_uses_pre_release_consensus_and_official_actual_for_direction(
    tmp_path: Path,
) -> None:
    pre_release = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    released = datetime(2026, 8, 8, 12, 31, tzinfo=UTC)
    current_time = [pre_release]
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    bodies = {
        BEA_RELEASE_DATES_URL: json.dumps(bea_payload()).encode(),
        NYFED_CALENDAR_URL: nyfed_html().encode(),
        FOMC_CALENDAR_URL: fomc_html().encode(),
        BLS_API_URL: json.dumps(bls_payload()).encode(),
        CONSENSUS_CALENDAR_URL: json.dumps(consensus_payload()).encode(),
    }
    monitor = MarketEventMonitor(
        MarketEventSettings(
            interval_seconds=21600,
            jitter_seconds=0,
            lookahead_days=2,
            history_days=1,
        ),
        store=store,
        client=FakeClient(bodies),
        now=lambda: current_time[0],
    )

    before = monitor.collect()
    first_run = store.start_run(monitor.monitor_id, started_at=pre_release)
    store.finish_run(first_run, monitor.monitor_id, before, completed_at=pre_release)
    current_time[0] = released
    awaiting = monitor.collect()
    second_run = store.start_run(monitor.monitor_id, started_at=released)
    store.finish_run(second_run, monitor.monitor_id, awaiting, completed_at=released)
    waiting_employment = next(
        sample.payload
        for sample in awaiting.samples
        if sample.entity_key.startswith("nyfed:employment-situation")
    )
    assert waiting_employment["release_state"] == "AWAITING_OFFICIAL"
    assert monitor.next_collection_delay_seconds() == 60

    bodies[BLS_API_URL] = json.dumps(bls_payload_july()).encode()
    completed = released + timedelta(minutes=1)
    current_time[0] = completed
    after = monitor.collect()
    third_run = store.start_run(monitor.monitor_id, started_at=completed)
    store.finish_run(third_run, monitor.monitor_id, after, completed_at=completed)

    employment = next(
        sample.payload
        for sample in after.samples
        if sample.entity_key.startswith("nyfed:employment-situation")
    )
    assert employment["release_state"] == "RELEASED"
    assert employment["expectation_summary"] == "新增非农 8.5万人 · 失业率 4.2%"
    assert employment["actual_summary"] == "新增非农 +11.0万人 · 失业率 4.0%"
    assert employment["surprise_summary"] == "新增非农 +25千人 · 失业率 -0.2%"
    assert employment["direction"]["label"] == "偏空"
    assert employment["direction"]["score"] == pytest.approx(-1.03)
    latest = {
        item.event_key: item
        for item in store.latest_market_event_revisions(monitor.monitor_id)
    }
    revision = next(
        item
        for key, item in latest.items()
        if key.startswith("nyfed:employment-situation")
    )
    assert revision.state == "RELEASED"
    assert revision.revision_no == 3
    assert monitor.next_collection_delay_seconds() == 3600


def test_monitor_isolates_macro_result_failure_and_tracks_schedule_changes(
    tmp_path: Path,
) -> None:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    bodies = {
        BEA_RELEASE_DATES_URL: json.dumps(bea_payload()).encode(),
        NYFED_CALENDAR_URL: nyfed_html().encode(),
        FOMC_CALENDAR_URL: fomc_html().encode(),
        BLS_API_URL: b"{not-json",
        CONSENSUS_CALENDAR_URL: json.dumps(consensus_payload()).encode(),
    }
    client = FakeClient(bodies)
    monitor = MarketEventMonitor(
        MarketEventSettings(
            interval_seconds=21600,
            jitter_seconds=0,
            lookahead_days=2,
            history_days=0,
        ),
        store=store,
        client=client,
        now=lambda: NOW,
    )

    first = monitor.collect()
    first_run = store.start_run(monitor.monitor_id, started_at=NOW)
    store.finish_run(first_run, monitor.monitor_id, first, completed_at=NOW)
    client.bodies[NYFED_CALENDAR_URL] = nyfed_html(employment_time="09:00").encode()
    monitor._schedule_refreshed_at = NOW - timedelta(seconds=21601)
    second = monitor.collect()

    assert first.samples
    assert {issue.scope for issue in first.issues} == {"bls-macro-data"}
    assert all(sample.payload["row_type"] == "EVENT" for sample in first.samples)
    changed = next(
        sample
        for sample in second.samples
        if sample.entity_key.startswith("nyfed:employment-situation")
    )
    assert changed.payload["schedule_change_count"] == 1
    assert changed.payload["previous_schedule_label"] == "2026-08-08 20:30"
    assert changed.payload["schedule_label"] == "2026-08-08 21:00"
    assert monitor.network_request_count(window_seconds=60) == 9


def test_month_requests_are_bounded_by_the_configured_window() -> None:
    urls = nyfed_month_urls(NOW, history_days=7, lookahead_days=60)

    assert [(year, month) for year, month, _url in urls] == [
        (2026, 7),
        (2026, 8),
        (2026, 9),
        (2026, 10),
    ]
    assert urls[1][2] == NYFED_CALENDAR_URL
