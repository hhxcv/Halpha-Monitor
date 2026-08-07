"""Scheduled market-moving event calendar backed by public official sources.

The monitor keeps schedule facts separate from directional market opinions.  It
collects only events with an explicit publication date from the source and does
not manufacture consensus forecasts when no maintainable public contract exists.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time as datetime_time, timedelta
import hashlib
import html
import json
import re
import threading
from typing import Any, Protocol
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from halpha_monitor.contracts import (
    CollectionArtifact,
    CollectionBatch,
    CollectionIssue,
    FilterChoice,
    MetricSample,
    MarketEventRevision,
    MonitorView,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.monitors.a_hk_buyback import (
    BuybackPublicClient,
    BuybackSourceError,
    PublicResponse,
)
from halpha_monitor.store import SQLiteMonitorStore, iso_utc, utc_now


MONITOR_ID = "market-event-calendar"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
NEW_YORK_TZ = ZoneInfo("America/New_York")

BEA_RELEASE_DATES_URL = "https://apps.bea.gov/API/signup/release_dates.json"
BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"
FOMC_CALENDAR_URL = (
    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
)
NYFED_CALENDAR_URL = (
    "https://www.newyorkfed.org/research/calendars/nationalecon_cal"
)
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
CONSENSUS_CALENDAR_URL = (
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
)
CONSENSUS_SOURCE_PAGE_URL = "https://www.forexfactory.com/calendar"

MONTH_NAMES = {
    name: month
    for month, name in enumerate(
        (
            "",
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        )
    )
    if month
}
MONTH_ABBREVIATIONS = (
    "",
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
)


class MarketEventSourceError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class PublicMarketClient(Protocol):
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
    ) -> PublicResponse: ...

    def network_request_count(self, *, window_seconds: float = 60) -> int: ...


@dataclass(frozen=True)
class MarketEventSettings:
    interval_seconds: float = 6 * 3600
    jitter_seconds: float = 15 * 60
    lookahead_days: int = 60
    history_days: int = 7
    timeout_seconds: float = 10
    proxy_url: str | None = None
    consensus_refresh_seconds: float = 3600

    def __post_init__(self) -> None:
        if self.interval_seconds < 900:
            raise ValueError("MARKET_EVENTS_INTERVAL_TOO_SHORT")
        if self.jitter_seconds < 0 or self.jitter_seconds > self.interval_seconds:
            raise ValueError("MARKET_EVENTS_JITTER_INVALID")
        if not 1 <= self.lookahead_days <= 120:
            raise ValueError("MARKET_EVENTS_LOOKAHEAD_INVALID")
        if not 0 <= self.history_days <= 14:
            raise ValueError("MARKET_EVENTS_HISTORY_INVALID")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 60:
            raise ValueError("MARKET_EVENTS_TIMEOUT_INVALID")
        if self.consensus_refresh_seconds < 1800:
            raise ValueError("MARKET_EVENTS_CONSENSUS_INTERVAL_TOO_SHORT")


@dataclass(frozen=True)
class EventDefinition:
    key: str
    title: str
    category: str
    category_label: str
    importance: str
    impact_reason: str
    market_scopes: tuple[str, ...]
    description: str
    indicator_key: str | None = None


@dataclass(frozen=True)
class ScheduledEvent:
    entity_key: str
    definition: EventDefinition
    scheduled_date: date
    scheduled_at: datetime | None
    time_precision: str
    source_key: str
    source_label: str
    schedule_source_url: str
    official_release_url: str | None
    source_timezone_label: str
    source_updated_at: datetime | None = None
    source_checked_at: datetime | None = None


@dataclass(frozen=True)
class ConsensusObservation:
    event_definition_key: str
    metric_key: str
    metric_label: str
    scheduled_at: datetime
    forecast_value: float
    forecast_text: str
    previous_text: str | None
    unit: str
    observed_at: datetime


ALL_MARKETS = ("CRYPTO", "US_STOCKS", "A_HK_STOCKS")
US_AND_CRYPTO = ("CRYPTO", "US_STOCKS")

BEA_DEFINITIONS = {
    "Gross Domestic Product": EventDefinition(
        "gdp",
        "美国GDP",
        "GROWTH",
        "经济增长",
        "HIGH",
        "GDP会重估增长、盈利与利率路径，是股票和加密资产的重要宏观定价输入。",
        ALL_MARKETS,
        "美国经济分析局发布国内生产总值及其修订值。",
    ),
    "Personal Income and Outlays": EventDefinition(
        "pce-income",
        "美国PCE物价与个人收支",
        "INFLATION",
        "通胀",
        "HIGH",
        "核心PCE是美联储重点参考的通胀指标，同时包含消费与收入信息。",
        ALL_MARKETS,
        "美国经济分析局发布个人收入、消费支出与PCE物价数据。",
    ),
    "U.S. International Trade in Goods and Services": EventDefinition(
        "trade-balance",
        "美国国际贸易",
        "TRADE",
        "贸易",
        "MEDIUM",
        "贸易流量会影响增长估计、美元预期及外向型行业判断。",
        ALL_MARKETS,
        "美国经济分析局与人口普查局发布商品和服务国际贸易数据。",
    ),
    "Corporate Profits": EventDefinition(
        "corporate-profits",
        "美国企业利润",
        "EARNINGS",
        "企业盈利",
        "MEDIUM",
        "企业利润是总量盈利周期与股票估值判断的宏观参考。",
        ("US_STOCKS", "A_HK_STOCKS"),
        "美国经济分析局随国民经济账户更新企业利润。",
    ),
}

NYFED_DEFINITIONS: tuple[tuple[re.Pattern[str], EventDefinition], ...] = (
    (
        re.compile(r"^Employment Situation$", re.I),
        EventDefinition(
            "employment-situation",
            "美国非农就业报告",
            "LABOR",
            "就业",
            "HIGH",
            "非农就业与失业率会直接改变增长和美联储政策预期。",
            ALL_MARKETS,
            "美国劳工统计局发布非农就业、失业率与工资等就业数据。",
            "EMPLOYMENT",
        ),
    ),
    (
        re.compile(r"^Consumer Price Index$", re.I),
        EventDefinition(
            "cpi",
            "美国CPI",
            "INFLATION",
            "通胀",
            "HIGH",
            "CPI会快速影响利率、美元与风险资产的定价。",
            ALL_MARKETS,
            "美国劳工统计局发布居民消费价格指数。",
            "CPI",
        ),
    ),
    (
        re.compile(r"^Producer Price Index", re.I),
        EventDefinition(
            "ppi",
            "美国PPI",
            "INFLATION",
            "通胀",
            "MEDIUM",
            "PPI提供上游价格压力线索，可能改变后续通胀判断。",
            ALL_MARKETS,
            "美国劳工统计局发布生产者价格指数。",
        ),
    ),
    (
        re.compile(r"^Advance Retail Sales$", re.I),
        EventDefinition(
            "retail-sales",
            "美国零售销售",
            "CONSUMPTION",
            "消费",
            "HIGH",
            "零售销售是美国消费动能的高频参考，可能影响增长与利率预期。",
            ALL_MARKETS,
            "美国人口普查局发布月度零售与餐饮销售初值。",
        ),
    ),
    (
        re.compile(r"^JOLTS$", re.I),
        EventDefinition(
            "jolts",
            "美国JOLTS职位空缺",
            "LABOR",
            "就业",
            "MEDIUM",
            "职位空缺反映劳动力需求与工资压力。",
            ALL_MARKETS,
            "美国劳工统计局发布职位空缺与劳动力流动调查。",
        ),
    ),
    (
        re.compile(r"^ISM Manufacturing$", re.I),
        EventDefinition(
            "ism-manufacturing",
            "美国ISM制造业PMI",
            "ACTIVITY",
            "经济活动",
            "HIGH",
            "制造业PMI是增长、订单与价格压力的高频领先线索。",
            ALL_MARKETS,
            "供应管理协会发布美国制造业采购经理指数。",
        ),
    ),
    (
        re.compile(r"^ISM Non-Manufacturing$", re.I),
        EventDefinition(
            "ism-services",
            "美国ISM服务业PMI",
            "ACTIVITY",
            "经济活动",
            "HIGH",
            "服务业覆盖美国经济主体，对增长与通胀预期均有影响。",
            ALL_MARKETS,
            "供应管理协会发布美国服务业采购经理指数。",
        ),
    ),
    (
        re.compile(r"^Industrial Production and Capacity Utilization$", re.I),
        EventDefinition(
            "industrial-production",
            "美国工业产出",
            "ACTIVITY",
            "经济活动",
            "MEDIUM",
            "工业产出与产能利用率反映实体生产强弱。",
            US_AND_CRYPTO,
            "美联储发布工业产出与产能利用率。",
        ),
    ),
    (
        re.compile(r"^Consumer Confidence$", re.I),
        EventDefinition(
            "consumer-confidence",
            "美国消费者信心",
            "SENTIMENT",
            "信心",
            "MEDIUM",
            "消费者信心为消费意愿和经济预期提供补充线索。",
            US_AND_CRYPTO,
            "消费者信心调查的计划发布时间。",
        ),
    ),
    (
        re.compile(r"^Michigan Consumer Survey \(Preliminary\)$", re.I),
        EventDefinition(
            "michigan-preliminary",
            "密歇根消费者信心初值",
            "SENTIMENT",
            "信心",
            "MEDIUM",
            "调查包含消费者信心与通胀预期，初值通常更受市场关注。",
            US_AND_CRYPTO,
            "密歇根大学消费者调查初值的计划发布时间。",
        ),
    ),
    (
        re.compile(r"^Michigan Consumer Survey \(Final\)$", re.I),
        EventDefinition(
            "michigan-final",
            "密歇根消费者信心终值",
            "SENTIMENT",
            "信心",
            "MEDIUM",
            "终值用于确认当月消费者信心与通胀预期。",
            US_AND_CRYPTO,
            "密歇根大学消费者调查终值的计划发布时间。",
        ),
    ),
    (
        re.compile(r"^Advance Durable Goods$", re.I),
        EventDefinition(
            "durable-goods",
            "美国耐用品订单初值",
            "ACTIVITY",
            "经济活动",
            "MEDIUM",
            "耐用品订单为企业投资和制造业需求提供高频线索。",
            US_AND_CRYPTO,
            "美国人口普查局发布耐用品订单初值。",
        ),
    ),
    (
        re.compile(r"^New Residential Construction$", re.I),
        EventDefinition(
            "housing-starts",
            "美国新屋开工",
            "HOUSING",
            "房地产",
            "MEDIUM",
            "新屋开工与许可反映利率敏感行业的活动强弱。",
            ("US_STOCKS",),
            "美国人口普查局发布新屋开工与建筑许可。",
        ),
    ),
    (
        re.compile(r"^New Residential Sales$", re.I),
        EventDefinition(
            "new-home-sales",
            "美国新屋销售",
            "HOUSING",
            "房地产",
            "MEDIUM",
            "新屋销售反映住房需求与按揭利率传导。",
            ("US_STOCKS",),
            "美国人口普查局发布新建住宅销售。",
        ),
    ),
)

FOMC_DEFINITION = EventDefinition(
    "fomc-decision",
    "美联储利率决议",
    "MONETARY_POLICY",
    "货币政策",
    "HIGH",
    "政策利率、声明与经济预测会重定价美元、利率和全球风险资产。",
    ALL_MARKETS,
    "联邦公开市场委员会公布政策决定；带经济预测的会议同时更新SEP。",
)


def _strict_json(body: bytes, reason_code: str) -> Any:
    try:
        return json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise MarketEventSourceError(reason_code) from None


def _strip_tags(value: str) -> str:
    return " ".join(
        html.unescape(re.sub(r"<[^>]+>", " ", value)).split()
    )


def _artifact(
    key: str,
    source: str,
    response: PublicResponse,
    *,
    body_text: str,
    record_count: int | None,
    schema_contract: str,
) -> CollectionArtifact:
    return CollectionArtifact(
        artifact_key=key,
        source=source,
        request_started_at=response.started_at,
        response_completed_at=response.completed_at,
        http_status=response.status,
        business_code=None,
        schema_hash=hashlib.sha256(schema_contract.encode("utf-8")).hexdigest(),
        response_sha256=hashlib.sha256(response.body).hexdigest(),
        record_count=record_count,
        response_body=body_text,
    )


def _parse_iso_datetime(value: Any, reason_code: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise MarketEventSourceError(reason_code) from None
    if parsed.tzinfo is None:
        raise MarketEventSourceError(reason_code)
    return parsed.astimezone(UTC)


def _event_in_window(
    event: ScheduledEvent,
    *,
    now: datetime,
    history_days: int,
    lookahead_days: int,
) -> bool:
    if event.scheduled_at is not None:
        return (
            now - timedelta(days=history_days)
            <= event.scheduled_at
            <= now + timedelta(days=lookahead_days)
        )
    today = now.astimezone(NEW_YORK_TZ).date()
    return (
        today - timedelta(days=history_days)
        <= event.scheduled_date
        <= today + timedelta(days=lookahead_days)
    )


def parse_bea_schedule(
    payload: Any,
    *,
    now: datetime,
    history_days: int,
    lookahead_days: int,
) -> tuple[ScheduledEvent, ...]:
    if not isinstance(payload, dict) or not isinstance(
        payload.get("file_last_updated"), str
    ):
        raise MarketEventSourceError("MARKET_EVENTS_BEA_SCHEMA_CHANGED")
    try:
        parsed_updated_at = datetime.fromisoformat(payload["file_last_updated"])
    except ValueError:
        raise MarketEventSourceError("MARKET_EVENTS_BEA_SCHEMA_CHANGED") from None
    source_updated_at = (
        parsed_updated_at.astimezone(UTC)
        if parsed_updated_at.tzinfo is not None
        else None
    )
    events: list[ScheduledEvent] = []
    recognized_series = 0
    for source_name, definition in BEA_DEFINITIONS.items():
        raw = payload.get(source_name)
        if raw is None:
            continue
        if not isinstance(raw, dict) or not isinstance(raw.get("release_dates"), list):
            raise MarketEventSourceError("MARKET_EVENTS_BEA_SCHEMA_CHANGED")
        recognized_series += 1
        parsed_dates = sorted(
            {
                _parse_iso_datetime(
                    value,
                    "MARKET_EVENTS_BEA_RELEASE_DATE_INVALID",
                )
                for value in raw["release_dates"]
            }
        )
        month_occurrences: defaultdict[str, int] = defaultdict(int)
        for scheduled_at in parsed_dates:
            local = scheduled_at.astimezone(NEW_YORK_TZ)
            month_key = f"{local.year:04d}-{local.month:02d}"
            month_occurrences[month_key] += 1
            event = ScheduledEvent(
                entity_key=(
                    f"bea:{definition.key}:{month_key}:"
                    f"{month_occurrences[month_key]}"
                ),
                definition=definition,
                scheduled_date=local.date(),
                scheduled_at=scheduled_at,
                time_precision="EXACT",
                source_key="bea-schedule",
                source_label="美国经济分析局发布日程",
                schedule_source_url=BEA_SCHEDULE_URL,
                official_release_url=BEA_SCHEDULE_URL,
                source_timezone_label="北京时间",
                source_updated_at=source_updated_at,
            )
            if _event_in_window(
                event,
                now=now,
                history_days=history_days,
                lookahead_days=lookahead_days,
            ):
                events.append(event)
    if recognized_series != len(BEA_DEFINITIONS):
        raise MarketEventSourceError("MARKET_EVENTS_BEA_SCHEMA_CHANGED")
    return tuple(events)


def _nyfed_definition(title: str) -> EventDefinition | None:
    for pattern, definition in NYFED_DEFINITIONS:
        if pattern.fullmatch(title):
            return definition
    return None


def parse_nyfed_calendar(
    body: str,
    *,
    expected_year: int,
    expected_month: int,
    page_url: str,
) -> tuple[ScheduledEvent, ...]:
    heading = re.search(
        r">\s*(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(20\d{2})\s*<",
        body,
        re.I,
    )
    if heading is None:
        raise MarketEventSourceError("MARKET_EVENTS_NYFED_SCHEMA_CHANGED")
    parsed_month = MONTH_NAMES.get(heading.group(1).title())
    parsed_year = int(heading.group(2))
    if parsed_month != expected_month or parsed_year != expected_year:
        raise MarketEventSourceError("MARKET_EVENTS_NYFED_MONTH_MISMATCH")

    cell_pattern = re.compile(
        r'<td[^>]*class="[^"]*somatdR[^"]*"[^>]*>\s*'
        r"<div>\s*(\d{1,2})(.*?)(?:</div>)\s*</td>",
        re.I | re.S,
    )
    anchor_pattern = re.compile(
        r'<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        re.I | re.S,
    )
    parsed: list[tuple[datetime, EventDefinition, str]] = []
    matched_cells = 0
    known_titles_seen = 0
    for cell in cell_pattern.finditer(body):
        matched_cells += 1
        day = int(cell.group(1))
        content = cell.group(2)
        anchors = list(anchor_pattern.finditer(content))
        for index, anchor in enumerate(anchors):
            title = _strip_tags(anchor.group(2))
            definition = _nyfed_definition(title)
            if definition is None:
                continue
            known_titles_seen += 1
            following_end = (
                anchors[index + 1].start()
                if index + 1 < len(anchors)
                else len(content)
            )
            following = content[anchor.end() : following_end]
            time_match = re.search(
                r"<br\s*/?>\s*\((\d{2}):(\d{2})\)",
                following,
                re.I,
            )
            if time_match is None:
                raise MarketEventSourceError(
                    "MARKET_EVENTS_NYFED_EVENT_TIME_MISSING"
                )
            try:
                scheduled_local = datetime(
                    expected_year,
                    expected_month,
                    day,
                    int(time_match.group(1)),
                    int(time_match.group(2)),
                    tzinfo=NEW_YORK_TZ,
                )
            except ValueError:
                raise MarketEventSourceError(
                    "MARKET_EVENTS_NYFED_EVENT_DATE_INVALID"
                ) from None
            official_url = urljoin(page_url, html.unescape(anchor.group(1)))
            if not official_url.startswith("https://"):
                raise MarketEventSourceError(
                    "MARKET_EVENTS_NYFED_EVENT_URL_INVALID"
                )
            parsed.append(
                (scheduled_local.astimezone(UTC), definition, official_url)
            )
    if matched_cells < 20 or known_titles_seen < 2:
        raise MarketEventSourceError("MARKET_EVENTS_NYFED_SCHEMA_CHANGED")

    occurrences: defaultdict[str, int] = defaultdict(int)
    events: list[ScheduledEvent] = []
    for scheduled_at, definition, official_url in sorted(
        parsed,
        key=lambda item: (item[0], item[1].key),
    ):
        occurrence_key = f"{definition.key}:{expected_year:04d}-{expected_month:02d}"
        occurrences[occurrence_key] += 1
        local = scheduled_at.astimezone(NEW_YORK_TZ)
        events.append(
            ScheduledEvent(
                entity_key=(
                    f"nyfed:{occurrence_key}:{occurrences[occurrence_key]}"
                ),
                definition=definition,
                scheduled_date=local.date(),
                scheduled_at=scheduled_at,
                time_precision="EXACT",
                source_key="nyfed-calendar",
                source_label="纽约联储经济指标日历",
                schedule_source_url=page_url,
                official_release_url=official_url,
                source_timezone_label="北京时间",
            )
        )
    return tuple(events)


def parse_fomc_calendar(
    body: str,
    *,
    now: datetime,
    history_days: int,
    lookahead_days: int,
) -> tuple[ScheduledEvent, ...]:
    panel_pattern = re.compile(
        r'<div class="panel panel-default"><div class="panel-heading">'
        r"<h4><a[^>]*>(20\d{2}) FOMC Meetings</a></h4></div>"
        r"(.*?)(?=<div class=\"panel panel-default\"><div class=\"panel-heading\">|\Z)",
        re.I | re.S,
    )
    meeting_pattern = re.compile(
        r"fomc-meeting__month[^>]*>\s*<strong>"
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)</strong>.*?"
        r"fomc-meeting__date[^>]*>\s*([0-9]{1,2}(?:-[0-9]{1,2})?)(\*)?\s*</div>",
        re.I | re.S,
    )
    events: list[ScheduledEvent] = []
    panel_count = 0
    meeting_count = 0
    for panel in panel_pattern.finditer(body):
        panel_count += 1
        year = int(panel.group(1))
        for meeting in meeting_pattern.finditer(panel.group(2)):
            meeting_count += 1
            month = MONTH_NAMES[meeting.group(1).title()]
            last_day = int(meeting.group(2).split("-")[-1])
            try:
                scheduled_date = date(year, month, last_day)
            except ValueError:
                raise MarketEventSourceError(
                    "MARKET_EVENTS_FOMC_DATE_INVALID"
                ) from None
            definition = FOMC_DEFINITION
            if meeting.group(3):
                definition = replace(
                    definition,
                    title="美联储利率决议与经济预测",
                )
            event = ScheduledEvent(
                entity_key=f"fomc:decision:{year:04d}-{month:02d}",
                definition=definition,
                scheduled_date=scheduled_date,
                scheduled_at=None,
                time_precision="DATE",
                source_key="fomc-calendar",
                source_label="美联储FOMC日历",
                schedule_source_url=FOMC_CALENDAR_URL,
                official_release_url=FOMC_CALENDAR_URL,
                source_timezone_label="美国东部日期；具体时间待公布",
            )
            if _event_in_window(
                event,
                now=now,
                history_days=history_days,
                lookahead_days=lookahead_days,
            ):
                events.append(event)
    if panel_count < 2 or meeting_count < 8:
        raise MarketEventSourceError("MARKET_EVENTS_FOMC_SCHEMA_CHANGED")
    return tuple(events)


def _monthly_series(payload: Any, series_id: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or payload.get("status") != "REQUEST_SUCCEEDED":
        raise MarketEventSourceError("MARKET_EVENTS_BLS_RESPONSE_FAILED")
    results = payload.get("Results")
    if not isinstance(results, dict) or not isinstance(results.get("series"), list):
        raise MarketEventSourceError("MARKET_EVENTS_BLS_SCHEMA_CHANGED")
    target = next(
        (
            item
            for item in results["series"]
            if isinstance(item, dict) and item.get("seriesID") == series_id
        ),
        None,
    )
    if target is None or not isinstance(target.get("data"), list):
        raise MarketEventSourceError("MARKET_EVENTS_BLS_SERIES_MISSING")
    rows: list[dict[str, Any]] = []
    for item in target["data"]:
        if not isinstance(item, dict):
            continue
        period = str(item.get("period") or "")
        if not re.fullmatch(r"M(0[1-9]|1[0-2])", period):
            continue
        try:
            year = int(item["year"])
            month = int(period[1:])
            value = float(item["value"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if not (-1e12 < value < 1e12):
            continue
        rows.append({**item, "year_value": year, "month_value": month, "number": value})
    rows.sort(key=lambda item: (item["year_value"], item["month_value"]), reverse=True)
    if len(rows) < 2:
        raise MarketEventSourceError("MARKET_EVENTS_BLS_SERIES_INSUFFICIENT")
    return rows


def _period_label(row: dict[str, Any]) -> str:
    return f"{row['year_value']}年{row['month_value']}月"


def _signed(value: float, digits: int = 1) -> str:
    return f"{value:+.{digits}f}"


def parse_bls_indicators(payload: Any) -> tuple[dict[str, Any], ...]:
    cpi_sa = _monthly_series(payload, "CUSR0000SA0")
    cpi_nsa = _monthly_series(payload, "CUUR0000SA0")
    core_cpi_sa = _monthly_series(payload, "CUSR0000SA0L1E")
    core_cpi_nsa = _monthly_series(payload, "CUUR0000SA0L1E")
    payroll = _monthly_series(payload, "CES0000000001")
    unemployment = _monthly_series(payload, "LNS14000000")

    cpi_period = (cpi_sa[0]["year_value"], cpi_sa[0]["month_value"])
    if (cpi_nsa[0]["year_value"], cpi_nsa[0]["month_value"]) != cpi_period:
        raise MarketEventSourceError("MARKET_EVENTS_BLS_PERIOD_MISMATCH")
    if (
        core_cpi_sa[0]["year_value"],
        core_cpi_sa[0]["month_value"],
    ) != cpi_period or (
        core_cpi_nsa[0]["year_value"],
        core_cpi_nsa[0]["month_value"],
    ) != cpi_period:
        raise MarketEventSourceError("MARKET_EVENTS_BLS_PERIOD_MISMATCH")
    prior_year = next(
        (
            row
            for row in cpi_nsa[1:]
            if row["year_value"] == cpi_nsa[0]["year_value"] - 1
            and row["month_value"] == cpi_nsa[0]["month_value"]
        ),
        None,
    )
    if prior_year is None or prior_year["number"] == 0 or cpi_sa[1]["number"] == 0:
        raise MarketEventSourceError("MARKET_EVENTS_BLS_SERIES_INSUFFICIENT")
    core_prior_year = next(
        (
            row
            for row in core_cpi_nsa[1:]
            if row["year_value"] == core_cpi_nsa[0]["year_value"] - 1
            and row["month_value"] == core_cpi_nsa[0]["month_value"]
        ),
        None,
    )
    if (
        core_prior_year is None
        or core_prior_year["number"] == 0
        or core_cpi_sa[1]["number"] == 0
    ):
        raise MarketEventSourceError("MARKET_EVENTS_BLS_SERIES_INSUFFICIENT")
    cpi_yoy = (cpi_nsa[0]["number"] / prior_year["number"] - 1) * 100
    cpi_mom = (cpi_sa[0]["number"] / cpi_sa[1]["number"] - 1) * 100
    core_cpi_yoy = (
        core_cpi_nsa[0]["number"] / core_prior_year["number"] - 1
    ) * 100
    core_cpi_mom = (
        core_cpi_sa[0]["number"] / core_cpi_sa[1]["number"] - 1
    ) * 100

    employment_period = (
        payroll[0]["year_value"],
        payroll[0]["month_value"],
    )
    if (
        unemployment[0]["year_value"],
        unemployment[0]["month_value"],
    ) != employment_period:
        raise MarketEventSourceError("MARKET_EVENTS_BLS_PERIOD_MISMATCH")
    payroll_change_ten_thousand = (payroll[0]["number"] - payroll[1]["number"]) / 10
    payroll_change_thousand = payroll[0]["number"] - payroll[1]["number"]
    preliminary = any(
        isinstance(footnote, dict) and footnote.get("code") == "P"
        for footnote in payroll[0].get("footnotes", [])
    )
    employment_primary = (
        f"新增非农 {_signed(payroll_change_ten_thousand)}万人"
        f"{'（初值）' if preliminary else ''}"
    )
    return (
        {
            "indicator_key": "CPI",
            "indicator_label": "美国CPI",
            "primary_value": f"同比 {_signed(cpi_yoy)}%",
            "secondary_value": f"环比 {_signed(cpi_mom)}%",
            "latest_result": f"同比 {_signed(cpi_yoy)}% · 环比 {_signed(cpi_mom)}%",
            "period_label": _period_label(cpi_sa[0]),
            "period_year": cpi_sa[0]["year_value"],
            "period_month": cpi_sa[0]["month_value"],
            "values": {
                "cpi_yoy": cpi_yoy,
                "cpi_mom": cpi_mom,
                "core_cpi_yoy": core_cpi_yoy,
                "core_cpi_mom": core_cpi_mom,
            },
            "source_label": "美国劳工统计局",
            "source_url": "https://www.bls.gov/cpi/",
            "method_label": "CPI-U未季调同比；CPI-U季调环比",
        },
        {
            "indicator_key": "EMPLOYMENT",
            "indicator_label": "美国就业",
            "primary_value": employment_primary,
            "secondary_value": f"失业率 {unemployment[0]['number']:.1f}%",
            "latest_result": (
                f"{employment_primary} · 失业率 {unemployment[0]['number']:.1f}%"
            ),
            "period_label": _period_label(payroll[0]),
            "period_year": payroll[0]["year_value"],
            "period_month": payroll[0]["month_value"],
            "values": {
                "payroll_change_thousand": payroll_change_thousand,
                "unemployment_rate": unemployment[0]["number"],
            },
            "source_label": "美国劳工统计局",
            "source_url": "https://www.bls.gov/ces/",
            "method_label": "总非农就业月增量；U-3失业率",
        },
    )


CONSENSUS_METRIC_DEFINITIONS: dict[
    str,
    tuple[str, str, str, str],
] = {
    "Non-Farm Employment Change": (
        "employment-situation",
        "payroll_change_thousand",
        "新增非农",
        "千人",
    ),
    "Unemployment Rate": (
        "employment-situation",
        "unemployment_rate",
        "失业率",
        "%",
    ),
    "CPI m/m": ("cpi", "cpi_mom", "CPI环比", "%"),
    "Core CPI m/m": ("cpi", "core_cpi_mom", "核心CPI环比", "%"),
    "CPI y/y": ("cpi", "cpi_yoy", "CPI同比", "%"),
    "Core CPI y/y": ("cpi", "core_cpi_yoy", "核心CPI同比", "%"),
}


def _consensus_number(value: str, unit: str) -> float:
    normalized = value.strip().replace(",", "").replace("−", "-")
    multiplier = 1.0
    if unit == "%":
        normalized = normalized.removesuffix("%").strip()
    elif unit == "千人":
        suffix = normalized[-1:].upper()
        multiplier = {"K": 1.0, "M": 1000.0}.get(suffix)
        if multiplier is not None:
            normalized = normalized[:-1].strip()
        else:
            multiplier = 1.0
    try:
        result = float(normalized) * multiplier
    except ValueError:
        raise MarketEventSourceError(
            "MARKET_EVENTS_CONSENSUS_VALUE_INVALID"
        ) from None
    if not (-1e9 < result < 1e9):
        raise MarketEventSourceError("MARKET_EVENTS_CONSENSUS_VALUE_INVALID")
    return result


def parse_consensus_calendar(
    payload: Any,
    *,
    observed_at: datetime,
) -> tuple[ConsensusObservation, ...]:
    """Parse the weekly public consensus export without using it as schedule truth."""

    if not isinstance(payload, list) or len(payload) < 10:
        raise MarketEventSourceError("MARKET_EVENTS_CONSENSUS_SCHEMA_CHANGED")
    valid_rows = 0
    parsed: list[ConsensusObservation] = []
    for item in payload:
        if not isinstance(item, dict):
            raise MarketEventSourceError("MARKET_EVENTS_CONSENSUS_SCHEMA_CHANGED")
        if not all(key in item for key in ("title", "country", "date", "impact")):
            raise MarketEventSourceError("MARKET_EVENTS_CONSENSUS_SCHEMA_CHANGED")
        try:
            scheduled_at = datetime.fromisoformat(str(item["date"]))
        except ValueError:
            raise MarketEventSourceError(
                "MARKET_EVENTS_CONSENSUS_DATE_INVALID"
            ) from None
        if scheduled_at.tzinfo is None:
            raise MarketEventSourceError("MARKET_EVENTS_CONSENSUS_DATE_INVALID")
        valid_rows += 1
        title = str(item["title"]).strip()
        definition = CONSENSUS_METRIC_DEFINITIONS.get(title)
        forecast_text = str(item.get("forecast") or "").strip()
        if str(item["country"]).strip().upper() != "USD" or definition is None:
            continue
        if not forecast_text:
            continue
        event_key, metric_key, metric_label, unit = definition
        parsed.append(
            ConsensusObservation(
                event_definition_key=event_key,
                metric_key=metric_key,
                metric_label=metric_label,
                scheduled_at=scheduled_at.astimezone(UTC),
                forecast_value=_consensus_number(forecast_text, unit),
                forecast_text=forecast_text,
                previous_text=(
                    str(item.get("previous") or "").strip() or None
                ),
                unit=unit,
                observed_at=observed_at,
            )
        )
    if valid_rows != len(payload):
        raise MarketEventSourceError("MARKET_EVENTS_CONSENSUS_SCHEMA_CHANGED")
    return tuple(parsed)


def _expected_release_period(event: ScheduledEvent) -> tuple[int, int] | None:
    if event.definition.indicator_key not in {"CPI", "EMPLOYMENT"}:
        return None
    release_date = (
        event.scheduled_at.astimezone(NEW_YORK_TZ).date()
        if event.scheduled_at is not None
        else event.scheduled_date
    )
    if release_date.month == 1:
        return release_date.year - 1, 12
    return release_date.year, release_date.month - 1


def _event_consensus(
    event: ScheduledEvent,
    observations: tuple[ConsensusObservation, ...],
) -> tuple[ConsensusObservation, ...]:
    if event.scheduled_at is None:
        return ()
    matches = [
        item
        for item in observations
        if item.event_definition_key == event.definition.key
        and abs((item.scheduled_at - event.scheduled_at).total_seconds()) <= 30 * 60
    ]
    unique: dict[str, ConsensusObservation] = {}
    for item in matches:
        existing = unique.get(item.metric_key)
        if existing is not None and existing.forecast_value != item.forecast_value:
            raise MarketEventSourceError("MARKET_EVENTS_CONSENSUS_AMBIGUOUS")
        unique[item.metric_key] = item
    return tuple(unique[key] for key in sorted(unique))


def _format_metric_number(value: float, unit: str) -> str:
    if unit == "千人":
        return f"{value:+,.0f}千人"
    if unit == "%":
        return f"{value:+.1f}%"
    return f"{value:+.2f}{unit}"


def _direction_payload(
    event: ScheduledEvent,
    *,
    expectations: dict[str, ConsensusObservation],
    actual_values: dict[str, float],
) -> dict[str, Any] | None:
    score: float
    inputs: list[dict[str, Any]]
    if event.definition.key == "cpi":
        required = ("cpi_mom", "core_cpi_mom")
        if not all(key in expectations and key in actual_values for key in required):
            return None
        headline_surprise = (
            actual_values["cpi_mom"] - expectations["cpi_mom"].forecast_value
        )
        core_surprise = (
            actual_values["core_cpi_mom"]
            - expectations["core_cpi_mom"].forecast_value
        )
        score = -(
            0.4 * headline_surprise / 0.1
            + 0.6 * core_surprise / 0.1
        )
        formula = (
            "方向分 = -[40% × (CPI环比预期差 ÷ 0.1个百分点) + "
            "60% × (核心CPI环比预期差 ÷ 0.1个百分点)]"
        )
        inputs = [
            {"label": "CPI环比预期差", "value": headline_surprise, "unit": "%"},
            {"label": "核心CPI环比预期差", "value": core_surprise, "unit": "%"},
        ]
    elif event.definition.key == "employment-situation":
        required = ("payroll_change_thousand", "unemployment_rate")
        if not all(key in expectations and key in actual_values for key in required):
            return None
        payroll_surprise = (
            actual_values["payroll_change_thousand"]
            - expectations["payroll_change_thousand"].forecast_value
        )
        unemployment_surprise = (
            actual_values["unemployment_rate"]
            - expectations["unemployment_rate"].forecast_value
        )
        labor_heat = (
            0.65 * payroll_surprise / 50
            + 0.35 * (-unemployment_surprise) / 0.1
        )
        score = -labor_heat
        formula = (
            "方向分 = -[65% × (新增非农预期差 ÷ 5万人) + "
            "35% × ((预期失业率 - 实际失业率) ÷ 0.1个百分点)]"
        )
        inputs = [
            {"label": "新增非农预期差", "value": payroll_surprise, "unit": "千人"},
            {"label": "失业率预期差", "value": unemployment_surprise, "unit": "%"},
        ]
    else:
        return None
    score = max(-3.0, min(3.0, score))
    if score >= 0.5:
        label = "偏多"
        tone = "BULLISH"
        action = "利率压力较预期缓和；先确认美元与美债收益率是否同步走弱，再评估风险资产多头机会。"
    elif score <= -0.5:
        label = "偏空"
        tone = "BEARISH"
        action = "利率压力较预期上升；优先检查多头敞口，并等待美元、利率与价格走势确认。"
    else:
        label = "中性"
        tone = "NEUTRAL"
        action = "两项数据合成后未形成明确方向；避免只依据单项超预期追价。"
    return {
        "label": label,
        "tone": tone,
        "scope": "风险资产短线",
        "score": round(score, 2),
        "threshold": "≥ +0.5偏多；≤ -0.5偏空；其余中性",
        "formula": formula,
        "formula_version": "macro-surprise-v1",
        "inputs": [
            {
                **item,
                "display": _format_metric_number(item["value"], item["unit"]),
            }
            for item in inputs
        ],
        "coverage_percent": 100,
        "action": action,
    }


def _direction_method_payload(event: ScheduledEvent) -> dict[str, Any] | None:
    if event.definition.key == "cpi":
        return {
            "formula": (
                "方向分 = -[40% × (CPI环比预期差 ÷ 0.1个百分点) + "
                "60% × (核心CPI环比预期差 ÷ 0.1个百分点)]"
            ),
            "threshold": "≥ +0.5偏多；≤ -0.5偏空；其余中性",
            "formula_version": "macro-surprise-v1",
            "required_inputs": ["CPI环比预期差", "核心CPI环比预期差"],
        }
    if event.definition.key == "employment-situation":
        return {
            "formula": (
                "方向分 = -[65% × (新增非农预期差 ÷ 5万人) + "
                "35% × ((预期失业率 - 实际失业率) ÷ 0.1个百分点)]"
            ),
            "threshold": "≥ +0.5偏多；≤ -0.5偏空；其余中性",
            "formula_version": "macro-surprise-v1",
            "required_inputs": ["新增非农预期差", "失业率预期差"],
        }
    return None


def _interpretation_payload(event: ScheduledEvent) -> dict[str, Any]:
    if event.definition.key == "cpi":
        return {
            "how_to_read": (
                "CPI与核心CPI低于市场预期，通常意味着通胀和利率压力缓和，"
                "短线更有利于股票与加密资产；高于预期通常相反。"
            ),
            "decision_rule": (
                "重点同时比较CPI环比和核心CPI环比；两者同向时信号更清晰，"
                "相互冲突时以合成方向分为准。"
            ),
        }
    if event.definition.key == "employment-situation":
        return {
            "how_to_read": (
                "新增非农高于预期且失业率低于预期，表示就业偏热，短线可能抬高利率预期；"
                "就业弱于预期则通常降低利率压力，但极弱数据也可能触发增长担忧。"
            ),
            "decision_rule": (
                "同时使用新增非农与失业率预期差；结果先解释利率压力方向，"
                "再结合美元、美债收益率和市场价格确认。"
            ),
        }
    category_rules = {
        "INFLATION": "高于预期通常抬高利率压力，低于预期通常缓和利率压力。",
        "GROWTH": "高于预期通常支持增长判断，但若同时推高利率预期，风险资产反应可能分化。",
        "CONSUMPTION": "高于预期表示需求更强；需同时观察利率预期是否上升。",
        "ACTIVITY": "高于预期表示经济活动更强，低于预期表示动能放缓。",
        "LABOR": "就业偏强支持增长但可能推高利率压力，需结合工资与失业率。",
    }
    return {
        "how_to_read": category_rules.get(
            event.definition.category,
            "先比较实际值、市场预期和前值，再判断它改变的是增长、通胀还是政策路径。",
        ),
        "decision_rule": "当前没有满足官方实值与一致预期双重质量门槛的固定方向公式。",
    }


def _month_range(start: date, end: date) -> Iterable[tuple[int, int]]:
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1


def nyfed_month_urls(
    now: datetime,
    *,
    history_days: int,
    lookahead_days: int,
) -> tuple[tuple[int, int, str], ...]:
    local_date = now.astimezone(NEW_YORK_TZ).date()
    start = local_date - timedelta(days=history_days)
    end = local_date + timedelta(days=lookahead_days)
    current = (local_date.year, local_date.month)
    result: list[tuple[int, int, str]] = []
    for year, month in _month_range(start, end):
        url = (
            NYFED_CALENDAR_URL
            if (year, month) == current
            else (
                "https://www.newyorkfed.org/research/calendars/"
                f"i-{MONTH_ABBREVIATIONS[month]}{year % 100:02d}.html"
            )
        )
        result.append((year, month, url))
    return tuple(result)


def _sort_at(event: ScheduledEvent) -> datetime:
    if event.scheduled_at is not None:
        return event.scheduled_at
    return datetime.combine(
        event.scheduled_date,
        datetime_time(hour=12),
        tzinfo=NEW_YORK_TZ,
    ).astimezone(UTC)


def _schedule_label(event: ScheduledEvent) -> str:
    if event.scheduled_at is not None:
        return event.scheduled_at.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M")
    return f"{event.scheduled_date.isoformat()} · 时间待公布"


def _source_error_code(prefix: str, error: Exception) -> str:
    if isinstance(error, MarketEventSourceError):
        return error.reason_code
    if isinstance(error, BuybackSourceError):
        suffix = error.reason_code.removeprefix("BUYBACK_")
        return f"MARKET_EVENTS_{prefix}_{suffix}"
    return f"MARKET_EVENTS_{prefix}_UNAVAILABLE"


class MarketEventMonitor:
    monitor_id = MONITOR_ID
    display_name = "市场关键事件日历"
    description = (
        "跟踪会影响加密资产与股票定价的宏观发布和央行决议，"
        "突出当前交易需要提前准备的事件。"
    )
    projection_kind = "market_events"
    default_enabled = True

    def __init__(
        self,
        settings: MarketEventSettings | None = None,
        *,
        store: SQLiteMonitorStore,
        client: PublicMarketClient | None = None,
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings or MarketEventSettings()
        self.interval_seconds = self.settings.interval_seconds
        self.jitter_seconds = self.settings.jitter_seconds
        self.store = store
        self.client = client or BuybackPublicClient(
            timeout_seconds=self.settings.timeout_seconds,
            proxy_url=self.settings.proxy_url,
        )
        self._now = now
        self._schedule_cache: dict[str, tuple[ScheduledEvent, ...]] = {}
        self._schedule_refreshed_at: datetime | None = None
        self._consensus_cache: dict[
            tuple[str, str, datetime],
            ConsensusObservation,
        ] = {}
        self._consensus_checked_at: datetime | None = None
        self._indicator_cache: tuple[dict[str, Any], ...] = ()
        self._indicator_checked_at: datetime | None = None
        self._next_delay_seconds = self.interval_seconds
        self.view = MonitorView(
            filters=(
                ViewFilter(
                    key="time_range",
                    label="时间范围",
                    default="NEXT_30D",
                    choices=(
                        FilterChoice("NEXT_24H", "未来24小时"),
                        FilterChoice("NEXT_7D", "未来7天"),
                        FilterChoice("NEXT_30D", "未来30天"),
                        FilterChoice("ALL_UPCOMING", "全部未来事件"),
                    ),
                ),
                ViewFilter(
                    key="importance",
                    label="影响级别",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部"),
                        FilterChoice(
                            "HIGH",
                            "高",
                            "利率决议、通胀、就业、GDP、PCE及关键消费或景气数据。",
                        ),
                        FilterChoice(
                            "MEDIUM",
                            "中",
                            "会补充增长、通胀或行业判断，但通常不是首要定价锚。",
                        ),
                    ),
                ),
                ViewFilter(
                    key="category",
                    label="事件类别",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部"),
                        FilterChoice("MONETARY_POLICY", "货币政策"),
                        FilterChoice("INFLATION", "通胀"),
                        FilterChoice("LABOR", "就业"),
                        FilterChoice("GROWTH", "经济增长"),
                        FilterChoice("CONSUMPTION", "消费"),
                        FilterChoice("ACTIVITY", "经济活动"),
                        FilterChoice("TRADE", "贸易"),
                        FilterChoice("EARNINGS", "企业盈利"),
                        FilterChoice("SENTIMENT", "信心"),
                        FilterChoice("HOUSING", "房地产"),
                    ),
                ),
                ViewFilter(
                    key="affected_market",
                    label="需考虑市场",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部"),
                        FilterChoice("CRYPTO", "加密资产"),
                        FilterChoice("US_STOCKS", "美股"),
                        FilterChoice("A_HK_STOCKS", "A股 / 港股"),
                    ),
                ),
            ),
            columns=(
                ViewColumn(
                    "priority_rank",
                    "准备优先级",
                    kind="number",
                    show_sign=False,
                    description=(
                        "按影响级别、距离发布时间和时间是否刚发生调整计算；"
                        "只表示需要提前纳入交易计划的先后，不表示涨跌方向。"
                    ),
                ),
                ViewColumn(
                    "scheduled_sort_at",
                    "发布时间",
                    kind="time",
                    description=(
                        "精确时间统一换算为北京时间；官方只公布日期时明确显示时间待公布。"
                    ),
                ),
                ViewColumn("event_title", "事件"),
                ViewColumn(
                    "importance_rank",
                    "影响级别",
                    kind="number",
                    show_sign=False,
                    description=(
                        "依据事件类型的市场定价相关性分级，不使用当次结果方向或主观买卖判断。"
                    ),
                ),
                ViewColumn("markets_label", "需考虑市场"),
                ViewColumn(
                    "expectation_summary",
                    "市场预期",
                    priority="secondary",
                    description="事件发布前最后取得的市场一致预期；来源和抓取时点可在详情查看。",
                ),
                ViewColumn(
                    "release_summary",
                    "公布 / 方向",
                    priority="secondary",
                    description="官方实值、相对预期差和固定公式计算的风险资产短线方向。",
                ),
            ),
            chart_title="事件时间线",
            table_title="事件日历",
            show_description=True,
        )

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return self.client.network_request_count(window_seconds=window_seconds)

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        binder = getattr(self.client, "bind_stop_event", None)
        if callable(binder):
            binder(stop_event)

    def next_collection_delay_seconds(self) -> float:
        """Return a bounded adaptive delay calculated by the latest collection."""

        return max(15.0, min(self.interval_seconds, self._next_delay_seconds))

    def _get_text(
        self,
        url: str,
        *,
        max_bytes: int,
    ) -> tuple[PublicResponse, str]:
        response = self.client.request(url, max_bytes=max_bytes, attempts=2)
        try:
            text = response.body.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise MarketEventSourceError(
                "MARKET_EVENTS_SOURCE_ENCODING_INVALID"
            ) from None
        return response, text

    def _schedule_events(
        self,
        now: datetime,
        issues: list[CollectionIssue],
        artifacts: list[CollectionArtifact],
    ) -> tuple[ScheduledEvent, ...]:
        due = (
            self._schedule_refreshed_at is None
            or now - self._schedule_refreshed_at
            >= timedelta(seconds=self.interval_seconds)
        )
        if due:
            self._schedule_refreshed_at = now
            try:
                response = self.client.request(
                    BEA_RELEASE_DATES_URL,
                    max_bytes=512 * 1024,
                    attempts=2,
                )
                parsed = parse_bea_schedule(
                    _strict_json(response.body, "MARKET_EVENTS_BEA_JSON_INVALID"),
                    now=now,
                    history_days=self.settings.history_days,
                    lookahead_days=self.settings.lookahead_days,
                )
                self._schedule_cache["bea-schedule"] = tuple(
                    replace(item, source_checked_at=now) for item in parsed
                )
                artifacts.append(
                    _artifact(
                        "bea-release-dates",
                        "美国经济分析局机器可读发布日程",
                        response,
                        body_text=response.body.decode("utf-8-sig"),
                        record_count=len(parsed),
                        schema_contract="bea-release-dates:v1:name-release_dates-file_last_updated",
                    )
                )
            except (BuybackSourceError, MarketEventSourceError) as error:
                issues.append(CollectionIssue("bea-schedule", _source_error_code("BEA", error)))

            active_nyfed_scopes: set[str] = set()
            for year, month, url in nyfed_month_urls(
                now,
                history_days=self.settings.history_days,
                lookahead_days=self.settings.lookahead_days,
            ):
                scope = f"nyfed-calendar:{year:04d}-{month:02d}"
                active_nyfed_scopes.add(scope)
                try:
                    response, body = self._get_text(url, max_bytes=512 * 1024)
                    parsed = parse_nyfed_calendar(
                        body,
                        expected_year=year,
                        expected_month=month,
                        page_url=url,
                    )
                    self._schedule_cache[scope] = tuple(
                        replace(item, source_checked_at=now) for item in parsed
                    )
                    artifacts.append(
                        _artifact(
                            f"nyfed-calendar-{year:04d}-{month:02d}",
                            "纽约联储经济指标日历",
                            response,
                            body_text=body,
                            record_count=len(parsed),
                            schema_contract="nyfed-calendar:v1:month-day-anchor-time",
                        )
                    )
                except (BuybackSourceError, MarketEventSourceError) as error:
                    issues.append(CollectionIssue(scope, _source_error_code("NYFED", error)))
            for scope in tuple(self._schedule_cache):
                if scope.startswith("nyfed-calendar:") and scope not in active_nyfed_scopes:
                    del self._schedule_cache[scope]

            try:
                response, body = self._get_text(FOMC_CALENDAR_URL, max_bytes=512 * 1024)
                parsed = parse_fomc_calendar(
                    body,
                    now=now,
                    history_days=self.settings.history_days,
                    lookahead_days=self.settings.lookahead_days,
                )
                self._schedule_cache["fomc-calendar"] = tuple(
                    replace(item, source_checked_at=now) for item in parsed
                )
                artifacts.append(
                    _artifact(
                        "fomc-calendar",
                        "美联储FOMC会议日历",
                        response,
                        body_text=body,
                        record_count=len(parsed),
                        schema_contract="fomc-calendar:v1:year-month-meeting-date-sep",
                    )
                )
            except (BuybackSourceError, MarketEventSourceError) as error:
                issues.append(CollectionIssue("fomc-calendar", _source_error_code("FOMC", error)))

        events = {
            event.entity_key: event
            for cached in self._schedule_cache.values()
            for event in cached
            if _event_in_window(
                event,
                now=now,
                history_days=self.settings.history_days,
                lookahead_days=self.settings.lookahead_days,
            )
        }
        return tuple(sorted(events.values(), key=lambda item: (_sort_at(item), item.entity_key)))

    def _consensus_observations(
        self,
        now: datetime,
        issues: list[CollectionIssue],
        artifacts: list[CollectionArtifact],
    ) -> tuple[ConsensusObservation, ...]:
        due = (
            self._consensus_checked_at is None
            or now - self._consensus_checked_at
            >= timedelta(seconds=self.settings.consensus_refresh_seconds)
        )
        if due:
            self._consensus_checked_at = now
            try:
                response = self.client.request(
                    CONSENSUS_CALENDAR_URL,
                    max_bytes=256 * 1024,
                    attempts=1,
                )
                parsed = parse_consensus_calendar(
                    _strict_json(
                        response.body,
                        "MARKET_EVENTS_CONSENSUS_JSON_INVALID",
                    ),
                    observed_at=now,
                )
                for item in parsed:
                    key = (
                        item.event_definition_key,
                        item.metric_key,
                        item.scheduled_at,
                    )
                    if item.observed_at <= item.scheduled_at:
                        self._consensus_cache[key] = item
                cutoff = now - timedelta(days=14)
                self._consensus_cache = {
                    key: item
                    for key, item in self._consensus_cache.items()
                    if item.scheduled_at >= cutoff
                }
                artifacts.append(
                    _artifact(
                        "weekly-market-consensus",
                        "Forex Factory市场一致预期周历",
                        response,
                        body_text=response.body.decode("utf-8-sig"),
                        record_count=len(parsed),
                        schema_contract="fair-economy-calendar:v1:title-country-date-impact-forecast-previous",
                    )
                )
            except (BuybackSourceError, MarketEventSourceError) as error:
                issues.append(
                    CollectionIssue(
                        "market-consensus",
                        _source_error_code("CONSENSUS", error),
                    )
                )
        return tuple(self._consensus_cache.values())

    @staticmethod
    def _indicator_matches_event(
        event: ScheduledEvent,
        indicator: dict[str, Any] | None,
    ) -> bool:
        expected = _expected_release_period(event)
        if expected is None or indicator is None:
            return False
        return expected == (
            int(indicator.get("period_year") or 0),
            int(indicator.get("period_month") or 0),
        )

    def _official_indicators(
        self,
        now: datetime,
        events: tuple[ScheduledEvent, ...],
        issues: list[CollectionIssue],
        artifacts: list[CollectionArtifact],
    ) -> tuple[dict[str, Any], ...]:
        cached_by_key = {
            str(item["indicator_key"]): item for item in self._indicator_cache
        }
        rapid_waiting = any(
            event.scheduled_at is not None
            and event.definition.indicator_key in {"CPI", "EMPLOYMENT"}
            and timedelta(0) <= now - event.scheduled_at <= timedelta(minutes=45)
            and not self._indicator_matches_event(
                event,
                cached_by_key.get(event.definition.indicator_key or ""),
            )
            for event in events
        )
        due = (
            not self._indicator_cache
            or self._indicator_checked_at is None
            or now - self._indicator_checked_at
            >= timedelta(seconds=self.interval_seconds)
            or rapid_waiting
        )
        if not due:
            return self._indicator_cache
        bls_request = json.dumps(
            {
                "seriesid": [
                    "CUSR0000SA0",
                    "CUUR0000SA0",
                    "CUSR0000SA0L1E",
                    "CUUR0000SA0L1E",
                    "CES0000000001",
                    "LNS14000000",
                ],
                "startyear": str(now.year - 1),
                "endyear": str(now.year),
            },
            separators=(",", ":"),
        ).encode("utf-8")
        try:
            response = self.client.request(
                BLS_API_URL,
                method="POST",
                body=bls_request,
                content_type="application/json",
                max_bytes=512 * 1024,
                attempts=2,
            )
            parsed = parse_bls_indicators(
                _strict_json(response.body, "MARKET_EVENTS_BLS_JSON_INVALID")
            )
            self._indicator_checked_at = now
            self._indicator_cache = tuple(
                {**item, "source_checked_at": iso_utc(now)} for item in parsed
            )
            artifacts.append(
                _artifact(
                    "bls-latest-indicators",
                    "美国劳工统计局公共数据API",
                    response,
                    body_text=response.body.decode("utf-8-sig"),
                    record_count=len(parsed),
                    schema_contract="bls-api:v2:headline-core-payroll-unemployment",
                )
            )
        except (BuybackSourceError, MarketEventSourceError) as error:
            issues.append(CollectionIssue("bls-macro-data", _source_error_code("BLS", error)))
        return self._indicator_cache

    @staticmethod
    def _forecast_display(item: ConsensusObservation) -> str:
        if item.unit == "千人":
            return f"{item.forecast_value / 10:.1f}万人"
        if item.unit == "%":
            return f"{item.forecast_value:.1f}%"
        return f"{item.forecast_value:g}{item.unit}"

    def _set_next_delay(
        self,
        now: datetime,
        events: tuple[ScheduledEvent, ...],
        released_event_keys: set[str],
    ) -> None:
        delay = float(self.interval_seconds)
        for event in events:
            if event.scheduled_at is None or event.definition.indicator_key not in {
                "CPI",
                "EMPLOYMENT",
            }:
                continue
            seconds_to_release = (event.scheduled_at - now).total_seconds()
            if 0 < seconds_to_release <= 24 * 3600:
                delay = min(delay, 3600.0, max(15.0, seconds_to_release))
                if seconds_to_release <= 3600:
                    delay = min(delay, 900.0, max(15.0, seconds_to_release))
                continue
            if event.entity_key in released_event_keys or seconds_to_release > 0:
                continue
            age = -seconds_to_release
            if age <= 5 * 60:
                delay = min(delay, 60.0)
            elif age <= 15 * 60:
                delay = min(delay, 120.0)
            elif age <= 45 * 60:
                delay = min(delay, 300.0)
        self._next_delay_seconds = delay

    def collect(self) -> CollectionBatch:
        now = self._now().astimezone(UTC)
        issues: list[CollectionIssue] = []
        artifacts: list[CollectionArtifact] = []
        events = self._schedule_events(now, issues, artifacts)
        consensus = self._consensus_observations(now, issues, artifacts)
        indicator_payloads = self._official_indicators(now, events, issues, artifacts)
        indicators = {
            str(item["indicator_key"]): item for item in indicator_payloads
        }
        previous_samples = {
            sample.entity_key: sample
            for sample in self.store.latest_samples_by_entity(
                self.monitor_id,
                tuple(event.entity_key for event in events),
            )
        }

        samples: list[MetricSample] = []
        revisions: list[MarketEventRevision] = []
        released_event_keys: set[str] = set()
        for event in events:
            previous = previous_samples.get(event.entity_key)
            previous_schedule = None
            change_count = 0
            last_changed_at = None
            if previous is not None:
                previous_schedule = str(previous.payload.get("schedule_label") or "") or None
                change_count = int(previous.payload.get("schedule_change_count") or 0)
                last_changed_at = previous.payload.get("last_schedule_changed_at")
            schedule_label = _schedule_label(event)
            if previous_schedule is not None and previous_schedule != schedule_label:
                change_count += 1
                last_changed_at = iso_utc(now)

            matched_consensus = _event_consensus(event, consensus)
            if not matched_consensus and previous is not None:
                restored: list[ConsensusObservation] = []
                for item in previous.payload.get("expectations", []):
                    if not isinstance(item, dict):
                        continue
                    observed_text = str(item.get("observed_at") or "")
                    try:
                        observed_at = datetime.fromisoformat(observed_text.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if event.scheduled_at is None or observed_at > event.scheduled_at:
                        continue
                    try:
                        restored.append(
                            ConsensusObservation(
                                event_definition_key=event.definition.key,
                                metric_key=str(item["metric_key"]),
                                metric_label=str(item["label"]),
                                scheduled_at=event.scheduled_at,
                                forecast_value=float(item["forecast_value"]),
                                forecast_text=str(item["forecast_text"]),
                                previous_text=(str(item["previous_text"]) if item.get("previous_text") else None),
                                unit=str(item["unit"]),
                                observed_at=observed_at.astimezone(UTC),
                            )
                        )
                    except (KeyError, TypeError, ValueError):
                        continue
                matched_consensus = tuple(restored)
            expectation_map = {item.metric_key: item for item in matched_consensus}
            expectations = [
                {
                    "metric_key": item.metric_key,
                    "label": item.metric_label,
                    "forecast_value": item.forecast_value,
                    "forecast_text": item.forecast_text,
                    "display": self._forecast_display(item),
                    "previous_text": item.previous_text,
                    "unit": item.unit,
                    "observed_at": iso_utc(item.observed_at),
                }
                for item in matched_consensus
            ]
            expected_period = _expected_release_period(event)
            indicator = indicators.get(event.definition.indicator_key or "")
            official_actual = indicator if self._indicator_matches_event(event, indicator) else None
            actual_values = dict(official_actual.get("values") or {}) if official_actual else {}
            actual_definitions = (
                (
                    ("cpi_mom", "CPI环比", "%"),
                    ("core_cpi_mom", "核心CPI环比", "%"),
                    ("cpi_yoy", "CPI同比", "%"),
                    ("core_cpi_yoy", "核心CPI同比", "%"),
                )
                if event.definition.key == "cpi"
                else (
                    ("payroll_change_thousand", "新增非农", "千人"),
                    ("unemployment_rate", "失业率", "%"),
                )
                if event.definition.key == "employment-situation"
                else ()
            )
            actuals = [
                {
                    "metric_key": key,
                    "label": label,
                    "actual_value": float(actual_values[key]),
                    "display": (
                        f"{float(actual_values[key]) / 10:+.1f}万人"
                        if unit == "千人"
                        else f"{float(actual_values[key]):.1f}%"
                    ),
                    "unit": unit,
                }
                for key, label, unit in actual_definitions
                if key in actual_values
            ]
            surprises = [
                {
                    "metric_key": item["metric_key"],
                    "label": item["label"],
                    "value": float(actual_values[item["metric_key"]])
                    - float(item["forecast_value"]),
                    "display": _format_metric_number(
                        float(actual_values[item["metric_key"]])
                        - float(item["forecast_value"]),
                        str(item["unit"]),
                    ),
                    "unit": item["unit"],
                }
                for item in expectations
                if item["metric_key"] in actual_values
            ]
            direction = (
                _direction_payload(
                    event,
                    expectations=expectation_map,
                    actual_values={key: float(value) for key, value in actual_values.items()},
                )
                if official_actual is not None
                else None
            )
            sort_at = _sort_at(event)
            if official_actual is not None:
                release_state = "RELEASED"
                released_event_keys.add(event.entity_key)
            elif now < sort_at:
                release_state = "SCHEDULED"
            elif event.definition.indicator_key in {"CPI", "EMPLOYMENT"}:
                release_state = "AWAITING_OFFICIAL"
            else:
                release_state = "OCCURRED"
            expectation_summary = " · ".join(
                f"{item['label']} {item['display']}" for item in expectations[:2]
            ) or None
            actual_summary = " · ".join(
                f"{item['label']} {item['display']}" for item in actuals[:2]
            ) or None
            surprise_summary = " · ".join(
                f"{item['label']} {item['display']}" for item in surprises[:2]
            ) or None
            release_summary = (
                " · ".join(
                    value
                    for value in (
                        actual_summary,
                        f"预期差 {surprise_summary}" if surprise_summary else None,
                        f"{direction['scope']} {direction['label']}" if direction else None,
                    )
                    if value
                )
                if official_actual is not None
                else "官方数据更新中"
                if release_state == "AWAITING_OFFICIAL"
                else None
            )
            checked_times = [
                value
                for value in (
                    event.source_checked_at,
                    max((item.observed_at for item in matched_consensus), default=None),
                    self._indicator_checked_at,
                )
                if value is not None
            ]
            payload: dict[str, Any] = {
                "row_type": "EVENT",
                "event_title": event.definition.title,
                "event_key": event.entity_key,
                "category": event.definition.category,
                "category_label": event.definition.category_label,
                "importance": event.definition.importance,
                "importance_rank": 2 if event.definition.importance == "HIGH" else 1,
                "impact_reason": event.definition.impact_reason,
                "event_description": event.definition.description,
                "interpretation": _interpretation_payload(event),
                "market_scopes": list(event.definition.market_scopes),
                "scheduled_at": iso_utc(event.scheduled_at) if event.scheduled_at is not None else None,
                "scheduled_date": event.scheduled_date.isoformat(),
                "scheduled_sort_at": iso_utc(sort_at),
                "schedule_label": schedule_label,
                "time_precision": event.time_precision,
                "source_key": event.source_key,
                "source_label": event.source_label,
                "schedule_source_url": event.schedule_source_url,
                "official_release_url": event.official_release_url,
                "source_timezone_label": event.source_timezone_label,
                "source_updated_at": iso_utc(event.source_updated_at) if event.source_updated_at is not None else None,
                "source_checked_at": iso_utc(max(checked_times)) if checked_times else None,
                "schedule_change_count": change_count,
                "last_schedule_changed_at": last_changed_at,
                "previous_schedule_label": (
                    previous_schedule
                    if previous_schedule != schedule_label
                    else previous.payload.get("previous_schedule_label")
                    if previous is not None
                    else None
                ),
                "expected_period": (
                    f"{expected_period[0]}年{expected_period[1]}月"
                    if expected_period is not None
                    else None
                ),
                "release_state": release_state,
                "release_state_label": {
                    "SCHEDULED": "等待公布",
                    "AWAITING_OFFICIAL": "官方数据更新中",
                    "RELEASED": "已公布",
                    "OCCURRED": "事件已发生",
                }[release_state],
                "expectations": expectations,
                "expectation_summary": expectation_summary,
                "consensus_source_label": "Forex Factory市场一致预期" if expectations else None,
                "consensus_source_url": CONSENSUS_SOURCE_PAGE_URL if expectations else None,
                "consensus_observed_at": max(
                    (iso_utc(item.observed_at) for item in matched_consensus),
                    default=None,
                ),
                "actuals": actuals,
                "actual_summary": actual_summary,
                "actual_source_label": official_actual.get("source_label") if official_actual else None,
                "actual_source_url": official_actual.get("source_url") if official_actual else None,
                "actual_checked_at": official_actual.get("source_checked_at") if official_actual else None,
                "surprises": surprises,
                "surprise_summary": surprise_summary,
                "direction": direction,
                "direction_method": _direction_method_payload(event),
                "direction_label": direction.get("label") if direction else None,
                "direction_score": direction.get("score") if direction else None,
                "release_summary": release_summary,
                "latest_result": indicator.get("latest_result") if indicator else None,
                "latest_result_period": indicator.get("period_label") if indicator else None,
            }
            samples.append(
                MetricSample(
                    series_key="market-event",
                    entity_key=event.entity_key,
                    observed_at=now,
                    value_text="1",
                    unit="event",
                    payload=payload,
                )
            )
            history_payload = dict(payload)
            for transient_key in (
                "source_checked_at",
                "actual_checked_at",
                "latest_result",
                "latest_result_period",
            ):
                history_payload.pop(transient_key, None)
            revisions.append(
                MarketEventRevision(
                    event_key=event.entity_key,
                    scheduled_at=sort_at,
                    observed_at=now,
                    state=release_state,
                    payload=history_payload,
                )
            )

        for indicator in indicator_payloads:
            samples.append(
                MetricSample(
                    series_key="macro-indicator",
                    entity_key=f"indicator:{str(indicator['indicator_key']).casefold()}",
                    observed_at=now,
                    value_text=str(indicator["primary_value"]),
                    unit="official release",
                    payload={"row_type": "INDICATOR", **indicator},
                )
            )
        self._set_next_delay(now, events, released_event_keys)
        return CollectionBatch(
            samples=tuple(samples),
            issues=tuple(issues),
            artifacts=tuple(artifacts),
            market_event_revisions=tuple(revisions),
        )


__all__ = [
    "MarketEventMonitor",
    "MarketEventSettings",
    "MarketEventSourceError",
    "ScheduledEvent",
    "nyfed_month_urls",
    "parse_bea_schedule",
    "parse_bls_indicators",
    "parse_fomc_calendar",
    "parse_nyfed_calendar",
]
