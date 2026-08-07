"""Auditable A-share and Stock-Connect HK-share repurchase fact monitor.

The monitor deliberately emits document candidates and HKEX execution facts. It
does not infer a directional trading signal or treat unreviewed structured fields
as a completed repurchase programme.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
import hashlib
import html
import io
import json
import math
import random
import re
import threading
import time
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

import exchange_calendars
from pypdf import PdfReader
import xlrd

from halpha_monitor.contracts import (
    AutomaticCollectionState,
    BuybackEntityRevision,
    BuybackEvidenceDocument,
    BuybackSourceObservation,
    CollectionBatch,
    CollectionCancelled,
    CollectionIssue,
    FilterChoice,
    MonitorView,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.buyback_metrics import (
    BuybackMetricError,
    match_a_share_program,
    parse_a_share_reference,
    parse_financial_reference,
    parse_market_reference,
    parse_tencent_market_reference,
)
from halpha_monitor.store import SQLiteMonitorStore, StoredBuybackSourceState, iso_utc, utc_now
from halpha_monitor.telemetry import NetworkRequestWindow


MONITOR_ID = "a-hk-buyback"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/136 Safari/537.36"
)

SSE_ANNOUNCEMENT_URL = (
    "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do"
)
SSE_ANNOUNCEMENT_REFERER = (
    "https://www.sse.com.cn/disclosure/listedinfo/announcement/"
)
CNINFO_SEARCH_URL = "https://www.cninfo.com.cn/new/fulltextSearch/full"
CNINFO_REFERER = "https://www.cninfo.com.cn/"
SSE_CONNECT_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_CONNECT_REFERER = "https://www.sse.com.cn/services/hkexsc/disclo/eligiblead/"
SZSE_REPORT_URL = "https://www.szse.cn/api/report/ShowReport"
SZSE_CONNECT_REFERER = "https://www.szse.cn/szhk/hkbussiness/underlylist/"
A_SHARE_REFERENCE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
HK_MARKET_REFERENCE_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
EASTMONEY_REFERER = "https://data.eastmoney.com/"
TENCENT_MARKET_REFERENCE_URL = "https://qt.gtimg.cn/q="
TENCENT_MARKET_REFERENCE_REFERER = "https://stock.qq.com/"
FINANCIAL_REFERENCE_URL = (
    "https://datacenter.eastmoney.com/securities/api/data/v1/get"
)

HKEX_BOARDS = {
    "main": {
        "label": "港交所主板回购日报",
        "calendar_url": "https://www3.hkexnews.hk/reports/sharerepur/sbn.asp",
        "report_prefix": "SRRPT",
    },
    "gem": {
        "label": "港交所 GEM 回购日报",
        "calendar_url": "https://www3.hkexnews.hk/reports/sharerepur/gem/sbmain.asp",
        "report_prefix": "SRGemRpt",
    },
}


@dataclass(frozen=True)
class _TradingWindow:
    market_label: str
    starts_at: datetime
    ends_at: datetime


class BuybackTradingSchedule:
    """Official-calendar trading windows for the combined A/H buyback page."""

    _MARKETS = (("XSHG", "A股"), ("XHKG", "港股"))

    def __init__(
        self,
        *,
        calendar_factory: Callable[..., Any] = exchange_calendars.get_calendar,
    ) -> None:
        self._calendar_factory = calendar_factory
        self._calendars: dict[str, tuple[int, Any]] = {}
        self._lock = threading.Lock()

    def state(self, *, now: datetime) -> AutomaticCollectionState:
        if now.utcoffset() is None:
            raise ValueError("BUYBACK_SCHEDULE_NOW_MUST_BE_TIMEZONE_AWARE")
        local_now = now.astimezone(SHANGHAI_TZ)
        active: list[_TradingWindow] = []
        next_windows: list[_TradingWindow] = []
        unavailable: list[str] = []
        for calendar_name, market_label in self._MARKETS:
            try:
                calendar = self._calendar(calendar_name, local_now.year)
                windows = self._windows_for_day(
                    calendar_name,
                    market_label,
                    calendar,
                    local_now.date(),
                )
                active.extend(
                    window
                    for window in windows
                    if window.starts_at <= local_now < window.ends_at
                )
                future = [window for window in windows if window.starts_at > local_now]
                if future:
                    next_windows.append(min(future, key=lambda item: item.starts_at))
                else:
                    next_window = self._next_session_window(
                        calendar_name,
                        market_label,
                        calendar,
                        local_now.date(),
                    )
                    if next_window is not None:
                        next_windows.append(next_window)
            except Exception:
                unavailable.append(market_label)

        next_open = min(
            (window.starts_at for window in next_windows),
            default=None,
        )
        if active:
            active_labels = "、".join(
                dict.fromkeys(window.market_label for window in active)
            )
            partial = (
                f"；{'、'.join(unavailable)}交易日历暂不可判定"
                if unavailable
                else ""
            )
            return AutomaticCollectionState(
                allowed=True,
                status="OPEN",
                reason_code="BUYBACK_TRADING_WINDOW_OPEN",
                label="交易时段 · 自动刷新",
                detail=f"{active_labels}处于交易时段，按设定周期自动采集{partial}。",
                next_open_at=(
                    next_open.astimezone(UTC) if next_open is not None else None
                ),
                active_until=max(window.ends_at for window in active).astimezone(UTC),
            )
        if unavailable:
            return AutomaticCollectionState(
                allowed=False,
                status="UNAVAILABLE",
                reason_code="BUYBACK_TRADING_CALENDAR_UNAVAILABLE",
                label="交易日历不可判定 · 自动刷新暂停",
                detail=(
                    f"{'、'.join(unavailable)}交易日历无法确认；为避免闭市误采集，"
                    "当前保持静态数据，只允许手动刷新。"
                ),
                next_open_at=(
                    next_open.astimezone(UTC) if next_open is not None else None
                ),
            )
        return AutomaticCollectionState(
            allowed=False,
            status="CLOSED",
            reason_code="BUYBACK_TRADING_WINDOW_CLOSED",
            label="已收市 · 静态历史数据",
            detail="当前不在 A股或港股交易时段，不自动采集；页面保留最近已提交数据。",
            next_open_at=(
                next_open.astimezone(UTC) if next_open is not None else None
            ),
        )

    def _calendar(self, calendar_name: str, year: int) -> Any:
        with self._lock:
            cached = self._calendars.get(calendar_name)
            if cached is not None and cached[0] == year:
                return cached[1]
            calendar = self._calendar_factory(
                calendar_name,
                start=f"{year}-01-01",
                end=f"{year}-12-31",
            )
            # Keep exactly one bounded calendar per market. A long-lived process
            # refreshes it when the local year changes.
            self._calendars[calendar_name] = (year, calendar)
            return calendar

    def _windows_for_day(
        self,
        calendar_name: str,
        market_label: str,
        calendar: Any,
        local_day: date,
    ) -> tuple[_TradingWindow, ...]:
        session = local_day.isoformat()
        if not bool(calendar.is_session(session)):
            return ()

        def local_time(hour: int, minute: int) -> datetime:
            return datetime.combine(
                local_day,
                datetime_time(hour, minute),
                tzinfo=SHANGHAI_TZ,
            )

        if calendar_name == "XSHG":
            return (
                _TradingWindow(market_label, local_time(9, 15), local_time(11, 30)),
                _TradingWindow(market_label, local_time(13, 0), local_time(15, 0)),
            )

        raw_close = calendar.session_close(session)
        close_value = (
            raw_close.to_pydatetime()
            if hasattr(raw_close, "to_pydatetime")
            else raw_close
        )
        close_at = close_value.astimezone(SHANGHAI_TZ) + timedelta(minutes=10)
        if close_at.hour <= 12:
            return (
                _TradingWindow(market_label, local_time(9, 0), close_at),
            )
        return (
            _TradingWindow(market_label, local_time(9, 0), local_time(12, 0)),
            _TradingWindow(market_label, local_time(13, 0), close_at),
        )

    def _next_session_window(
        self,
        calendar_name: str,
        market_label: str,
        calendar: Any,
        local_day: date,
    ) -> _TradingWindow | None:
        try:
            if bool(calendar.is_session(local_day.isoformat())):
                session = calendar.next_session(local_day.isoformat())
            else:
                session = calendar.date_to_session(
                    local_day.isoformat(),
                    direction="next",
                )
            session_day = session.date()
            windows = self._windows_for_day(
                calendar_name,
                market_label,
                calendar,
                session_day,
            )
            return windows[0] if windows else None
        except Exception:
            next_year = local_day.year + 1
            try:
                next_calendar = self._calendar(calendar_name, next_year)
                session = next_calendar.date_to_session(
                    f"{next_year}-01-01",
                    direction="next",
                )
                windows = self._windows_for_day(
                    calendar_name,
                    market_label,
                    next_calendar,
                    session.date(),
                )
                return windows[0] if windows else None
            except Exception:
                return None

ANNOUNCEMENT_KEYWORDS = ("回购股份", "股份回购")
TARGET_EVENT_TYPES = frozenset(
    {
        "PLAN_OR_APPROVAL",
        "FIRST_EXECUTION",
        "PROGRESS",
        "MODIFICATION",
        "COMPLETION_OR_TERMINATION",
        "POST_BUYBACK_CANCELLATION",
        "POST_BUYBACK_DISPOSAL",
        "AMBIGUOUS_BUYBACK",
    }
)
EVENT_TYPE_LABELS = {
    "PLAN_OR_APPROVAL": "方案 / 审议",
    "FIRST_EXECUTION": "首次实施",
    "PROGRESS": "实施进展",
    "MODIFICATION": "方案变更",
    "COMPLETION_OR_TERMINATION": "完成 / 终止",
    "POST_BUYBACK_CANCELLATION": "注销",
    "POST_BUYBACK_DISPOSAL": "出售已回购股份",
    "AMBIGUOUS_BUYBACK": "待确认回购事件",
    "HKEX_EXECUTION": "港股实际回购",
}
EVENT_STAGE_DESCRIPTIONS = {
    "PLAN_OR_APPROVAL": "公司提出或审议回购方案，尚不代表已经买入股份。",
    "FIRST_EXECUTION": "方案通过后首次实际买入股份，回购由计划进入执行。",
    "PROGRESS": "已经开始回购，披露累计买入数量、金额或执行进度。",
    "MODIFICATION": "回购价格、资金、用途、期限等方案条件发生调整。",
    "COMPLETION_OR_TERMINATION": "回购计划已经完成、届满或提前终止。",
    "POST_BUYBACK_CANCELLATION": "公司注销已回购股份，可能减少总股本。",
    "POST_BUYBACK_DISPOSAL": "公司出售或减持此前已回购的股份。",
    "HKEX_EXECUTION": "港交所日报披露当日实际买入股份，不等同于董事会方案公告。",
}
DISPLAY_EVENT_TYPES = tuple(EVENT_STAGE_DESCRIPTIONS)
ATTENTION_LEVEL_LABELS = {
    "PRIORITY": "优先研判",
    "TRACKING": "持续跟踪",
    "UPDATE": "状态更新",
}
ATTENTION_LEVEL_DESCRIPTIONS = {
    "PRIORITY": (
        "首次实施、方案变更、出售已回购股份或港股实际回购："
        "包含实际买入、关键条件变化或已回购股份再出售，应先阅读。"
    ),
    "TRACKING": (
        "方案 / 审议或实施进展：继续观察后续执行、金额和期限变化。"
    ),
    "UPDATE": "完成 / 终止或注销：主要用于更新回购生命周期状态。",
}
BUYBACK_PRIORITY_EVENT_TYPES = frozenset(
    {
        "FIRST_EXECUTION",
        "MODIFICATION",
        "POST_BUYBACK_DISPOSAL",
        "HKEX_EXECUTION",
    }
)
BUYBACK_TRACKING_EVENT_TYPES = frozenset({"PLAN_OR_APPROVAL", "PROGRESS"})


def classify_buyback_attention(event_type: str, scope: str) -> tuple[str, str]:
    if scope == "EXCLUDED":
        return "EXCLUDED", "已排除"
    if scope == "PENDING":
        return "PENDING", "待补全"
    if event_type in BUYBACK_PRIORITY_EVENT_TYPES:
        return "PRIORITY", ATTENTION_LEVEL_LABELS["PRIORITY"]
    if event_type in BUYBACK_TRACKING_EVENT_TYPES:
        return "TRACKING", ATTENTION_LEVEL_LABELS["TRACKING"]
    return "UPDATE", ATTENTION_LEVEL_LABELS["UPDATE"]


OUT_OF_SCOPE_PATTERN = re.compile(
    r"限制性股票|股权激励.*回购价格|业绩承诺.*回购|回购.*业绩承诺|"
    r"业绩补偿.*回购|回购.*业绩补偿|(?:控股|全资)?子公司.*回购|"
    r"质押式回购|债券回购|回购式融资|约定购回|回购担保|"
    r"股票期权.*注销|注销.*股票期权"
)

HKEX_HEADERS_11 = (
    "Company",
    "Stock code",
    "Sec. type",
    "Trading date (yyyy/mm/dd)",
    "Number of securities purchased",
    "Price per share or highest price paid($)",
    "Lowest price paid ($)",
    "Total paid ($)",
    "Method of purchase",
    "Number of such securities purchased on the Exchange in the year to date (since ordinary resolution)",
    "% of number of shares in issue at time ordinary resolution passed acquired on the Exchange since date of resolution",
)
HKEX_HEADERS_14 = (
    "Company",
    "Stock code",
    "Sec. type",
    "Trading date (yyyy/mm/dd)",
    "Number of shares/units repurchased",
    "Repurchase price or highest repurchase price per share/unit ($)",
    "Lowest repurchase price per share/unit ($)",
    "Aggregate price paid ($)",
    "Method of repurchase",
    "Total number of shares/units repurchased*",
    "Number of shares/units repurchased for cancellation*",
    "Number of shares/units repurchased for holding as treasury shares/units*",
    "Number of shares/units repurchased on the Exchange or another stock exchange under the repurchase mandate*",
    "As a % of number of issued shares/units (excluding treasury shares/units) as at the date of the resolution granting the repurchase mandate*",
)
KNOWN_HKEX_HEADERS = {HKEX_HEADERS_11, HKEX_HEADERS_14}


class BuybackSourceError(RuntimeError):
    def __init__(self, reason_code: str, *, temporary: bool = False) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.temporary = temporary


@dataclass(frozen=True)
class PublicResponse:
    body: bytes
    status: int
    content_type: str
    started_at: datetime
    completed_at: datetime
    headers: dict[str, str]


class BuybackPublicClient:
    """Small unauthenticated byte client with per-host bounded backoff."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        proxy_url: str | None = None,
        opener: OpenerDirector | None = None,
        now: Callable[[], datetime] = utc_now,
        sleeper: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if opener is not None and proxy_url is not None:
            raise ValueError("opener and proxy_url are mutually exclusive")
        if timeout_seconds <= 0 or timeout_seconds > 60:
            raise ValueError("BUYBACK_TIMEOUT_INVALID")
        self.timeout_seconds = timeout_seconds
        self.opener = opener or (
            build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
            if proxy_url
            else build_opener()
        )
        self._now = now
        self._sleeper = sleeper
        self._random_uniform = random_uniform
        self._network_requests = NetworkRequestWindow()
        self._backoff_until: dict[str, datetime] = {}
        self._backoff_failures: dict[str, int] = defaultdict(int)
        self._backoff_lock = threading.Lock()
        self._stop_event: threading.Event | None = None

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event

    def _raise_if_cancelled(self) -> None:
        if self._stop_event is not None and self._stop_event.is_set():
            raise CollectionCancelled("BUYBACK_COLLECTION_CANCELLED")

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return self._network_requests.count(window_seconds=window_seconds)

    def request(
        self,
        url: str,
        *,
        method: Literal["GET", "POST"] = "GET",
        body: bytes | None = None,
        content_type: str | None = None,
        referer: str | None = None,
        max_bytes: int,
        attempts: int = 3,
    ) -> PublicResponse:
        self._raise_if_cancelled()
        if not url.startswith("https://"):
            raise BuybackSourceError("BUYBACK_SOURCE_URL_INVALID")
        if max_bytes < 1 or max_bytes > 20 * 1024 * 1024 or attempts not in {1, 2, 3}:
            raise ValueError("BUYBACK_REQUEST_BOUND_INVALID")
        host = urlparse(url).hostname or ""
        self._ensure_host_available(host)
        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
            "User-Agent": USER_AGENT,
        }
        if referer:
            headers["Referer"] = referer
        if body is not None:
            headers["Content-Type"] = (
                content_type or "application/x-www-form-urlencoded"
            )

        last_reason = "BUYBACK_NETWORK_UNAVAILABLE"
        for attempt in range(1, attempts + 1):
            self._raise_if_cancelled()
            started_at = self._now()
            request = Request(url, data=body, headers=headers, method=method)
            self._network_requests.record()
            try:
                with self.opener.open(request, timeout=self.timeout_seconds) as response:
                    status = int(
                        getattr(response, "status", None) or response.getcode()
                    )
                    response_body = response.read(max_bytes + 1)
                    response_headers = response.headers
            except HTTPError as exc:
                if exc.code in {418, 429}:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    self._open_host_backoff(host, retry_after)
                    raise BuybackSourceError(
                        f"BUYBACK_HTTP_THROTTLED_{exc.code}", temporary=True
                    ) from None
                last_reason = f"BUYBACK_HTTP_{exc.code}"
                if exc.code < 500 or attempt == attempts:
                    raise BuybackSourceError(
                        last_reason,
                        temporary=exc.code >= 500,
                    ) from None
            except (TimeoutError, URLError, OSError) as exc:
                last_reason = f"BUYBACK_NETWORK_{type(exc).__name__.upper()}"
                if attempt == attempts:
                    raise BuybackSourceError(last_reason, temporary=True) from None
            else:
                completed_at = self._now()
                if status in {418, 429}:
                    self._open_host_backoff(
                        host,
                        response_headers.get("Retry-After"),
                    )
                    raise BuybackSourceError(
                        f"BUYBACK_HTTP_THROTTLED_{status}", temporary=True
                    )
                if not 200 <= status < 300:
                    last_reason = f"BUYBACK_HTTP_{status}"
                    if status < 500 or attempt == attempts:
                        raise BuybackSourceError(
                            last_reason,
                            temporary=status >= 500,
                        )
                else:
                    if len(response_body) > max_bytes:
                        raise BuybackSourceError("BUYBACK_RESPONSE_TOO_LARGE")
                    if not response_body:
                        raise BuybackSourceError("BUYBACK_RESPONSE_EMPTY")
                    self._clear_host_backoff(host)
                    return PublicResponse(
                        body=response_body,
                        status=status,
                        content_type=str(response_headers.get("Content-Type") or ""),
                        started_at=started_at,
                        completed_at=completed_at,
                        headers={
                            key: str(response_headers.get(key) or "")
                            for key in ("Content-Length", "Last-Modified", "ETag")
                        },
                    )
            if attempt < attempts:
                delay = min(
                    1.0,
                    0.2 * (2 ** (attempt - 1))
                    + self._random_uniform(0.0, 0.1),
                )
                if self._stop_event is not None:
                    if self._stop_event.wait(delay):
                        raise CollectionCancelled("BUYBACK_COLLECTION_CANCELLED")
                else:
                    self._sleeper(delay)
        raise BuybackSourceError(last_reason, temporary=True)

    def _ensure_host_available(self, host: str) -> None:
        with self._backoff_lock:
            until = self._backoff_until.get(host)
            if until is not None and self._now() < until:
                raise BuybackSourceError("BUYBACK_BACKOFF_ACTIVE", temporary=True)

    def _clear_host_backoff(self, host: str) -> None:
        with self._backoff_lock:
            self._backoff_until.pop(host, None)
            self._backoff_failures.pop(host, None)

    def _open_host_backoff(self, host: str, retry_after: str | None) -> None:
        with self._backoff_lock:
            self._backoff_failures[host] += 1
            failures = self._backoff_failures[host]
            exponential = min(3600.0, 30.0 * (2 ** (failures - 1)))
            upstream = _retry_after_seconds(retry_after, now=self._now())
            delay = min(
                3600.0,
                max(exponential, upstream)
                + self._random_uniform(0.0, min(15.0, exponential * 0.1)),
            )
            candidate = self._now() + timedelta(seconds=delay)
            current = self._backoff_until.get(host)
            if current is None or candidate > current:
                self._backoff_until[host] = candidate


@dataclass(frozen=True)
class AnnouncementRecord:
    source_key: str
    source_label: str
    market: Literal["SH", "SZ"]
    stock_code: str
    issuer_name: str
    source_document_id: str
    document_url: str
    title: str
    released_at: datetime
    display_date: str
    time_precision: Literal["SECOND", "DATE"]
    event_type: str

    @property
    def identity(self) -> str:
        normalized = disclosure_title_identity(self.title, self.issuer_name)
        raw = f"{self.market}|{self.stock_code}|{self.display_date}|{normalized}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class ParsedPdf:
    page_count: int
    text_sha256: str | None
    evidence_excerpt: str | None
    quality_state: str
    text: str | None = None


@dataclass(frozen=True)
class HkexExecution:
    company: str
    stock_code: str
    share_class: str
    trading_date: str
    shares: float
    high_price: float | None
    low_price: float | None
    amount: float | None
    currency: str | None
    method: str
    execution_venue: str
    total_repurchased_shares: float | None
    cancellation_shares: float | None
    treasury_shares: float | None
    mandate_exchange_shares: float | None
    mandate_percentage: float | None
    currency_consistent: bool


def _retry_after_seconds(value: str | None, *, now: datetime) -> float:
    if not value:
        return 0.0
    try:
        return min(3600.0, max(0.0, float(value)))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=UTC)
        return min(3600.0, max(0.0, (parsed.astimezone(UTC) - now).total_seconds()))


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_jsonp(value: bytes) -> dict[str, Any]:
    try:
        text = value.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise BuybackSourceError("BUYBACK_JSON_ENCODING_INVALID") from None
    first = text.find("(")
    last = text.rfind(")")
    candidate = text[first + 1 : last] if first >= 0 and last > first else text
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        raise BuybackSourceError("BUYBACK_JSON_INVALID") from None
    if not isinstance(payload, dict):
        raise BuybackSourceError("BUYBACK_JSON_ROOT_INVALID")
    return payload


def strip_markup(value: str | None) -> str:
    if not value:
        return ""
    without_tags = re.sub(r"<[^>]+>", "", value)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def disclosure_title_identity(title: str, issuer_name: str) -> str:
    value = strip_markup(title)
    value = re.sub(r"^\[临时公告\]", "", value)
    if issuer_name:
        value = value.replace(issuer_name, "")
    return re.sub(r"[\W_]+", "", value, flags=re.UNICODE).casefold()


def classify_buyback_title(title: str) -> str:
    compact = re.sub(r"\s+", "", title)
    if OUT_OF_SCOPE_PATTERN.search(compact):
        return "OUT_OF_SCOPE_OTHER_REPURCHASE"
    if re.search(r"境内上市外资股|（B股）|\(B股\)|B股股份", compact, re.I):
        return "OUT_OF_SCOPE_SHARE_CLASS"
    if re.search(r"尚未实施回购|未实施.*回购", compact):
        return "PROGRESS"
    if re.search(r"出售已回购股份|出售回购股份|减持.*已回购股份|回购股份.*减持", compact):
        return "POST_BUYBACK_DISPOSAL"
    if re.search(r"回购股份注销不调整.*转股价格|不调整.*可转债转股价格", compact):
        return "ANCILLARY"
    if re.search(r"(变更|调整).*回购股份用途", compact):
        return "MODIFICATION"
    if re.search(r"回购股份注销完成|实施回购股份注销|注销部分回购股份", compact):
        return "POST_BUYBACK_CANCELLATION"
    if re.search(r"回购.*(结果|完成|实施结果|期限届满|期限到期|终止|注销完成)", compact):
        return "COMPLETION_OR_TERMINATION"
    if re.search(r"(首次回购|首次实施回购|首次以集中竞价.*回购)", compact):
        return "FIRST_EXECUTION"
    if re.search(r"(回购.*进展|截至.*回购|实施回购.*进展)", compact):
        return "PROGRESS"
    if re.search(r"((变更|调整|延期|延长|修订|增加).*回购|回购.*(变更|调整|延期|延长|修订))", compact):
        return "MODIFICATION"
    if re.search(r"回购.*(方案|报告书|预案|议案|提议|批准|获批)", compact):
        return "PLAN_OR_APPROVAL"
    if re.search(r"前十(?:大|名)股东|法律意见|独立财务顾问|提示性公告|自愿性公告", compact):
        return "ANCILLARY"
    if "回购" in compact and "股份" in compact:
        return "AMBIGUOUS_BUYBACK"
    return "OUT_OF_SCOPE_OR_UNCLASSIFIED"


def is_target_a_share_security(market: str, stock_code: str) -> bool:
    normalized_market = market.strip().upper()
    normalized_code = stock_code.strip()
    if normalized_market == "SH" and normalized_code.startswith("900"):
        return False
    if normalized_market == "SZ" and normalized_code.startswith("200"):
        return False
    return bool(re.fullmatch(r"\d{6}", normalized_code))


def _parse_local_datetime(value: str) -> tuple[datetime, Literal["SECOND", "DATE"]]:
    text = value.strip()
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text[:19], pattern)
        except ValueError:
            continue
        precision: Literal["SECOND", "DATE"] = "DATE" if pattern == "%Y-%m-%d" else "SECOND"
        return parsed.replace(tzinfo=SHANGHAI_TZ).astimezone(UTC), precision
    raise BuybackSourceError("BUYBACK_RELEASE_TIME_INVALID")


def _cninfo_release(value: Any) -> tuple[datetime, Literal["SECOND", "DATE"]]:
    try:
        instant = datetime.fromtimestamp(int(value) / 1000, tz=UTC).astimezone(
            SHANGHAI_TZ
        )
    except (TypeError, ValueError, OverflowError):
        raise BuybackSourceError("BUYBACK_RELEASE_TIME_INVALID") from None
    precision: Literal["SECOND", "DATE"] = (
        "DATE"
        if instant.hour == 0 and instant.minute == 0 and instant.second == 0
        else "SECOND"
    )
    return instant.astimezone(UTC), precision


def parse_sse_announcement_payload(
    raw: bytes,
    *,
    begin: date,
    end: date,
) -> tuple[tuple[AnnouncementRecord, ...], int, str]:
    payload = parse_jsonp(raw)
    page_help = payload.get("pageHelp")
    if not isinstance(page_help, dict):
        raise BuybackSourceError("BUYBACK_SSE_SCHEMA_CHANGED")
    values = page_help.get("data") or []
    if not isinstance(values, list):
        raise BuybackSourceError("BUYBACK_SSE_SCHEMA_CHANGED")
    try:
        page_count = int(page_help.get("pageCount") or (1 if values else 0))
    except (TypeError, ValueError):
        raise BuybackSourceError("BUYBACK_SSE_SCHEMA_CHANGED") from None
    rows: list[AnnouncementRecord] = []
    schema_keys: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            raise BuybackSourceError("BUYBACK_SSE_SCHEMA_CHANGED")
        schema_keys.update(str(key) for key in value)
        stock_code = str(value.get("SECURITY_CODE") or "").strip()
        title = strip_markup(str(value.get("TITLE") or ""))
        relative_url = str(value.get("URL") or "").strip()
        release_text = str(value.get("ADDDATE") or value.get("SSEDATE") or "")
        if not re.fullmatch(r"\d{6}", stock_code) or not title or not relative_url:
            continue
        released_at, precision = _parse_local_datetime(release_text)
        display_date = str(value.get("SSEDATE") or release_text)[:10]
        if not begin <= released_at.astimezone(SHANGHAI_TZ).date() <= end:
            continue
        document_url = urljoin("https://www.sse.com.cn", relative_url)
        rows.append(
            AnnouncementRecord(
                source_key="sse-announcements",
                source_label="上交所公告索引",
                market="SH",
                stock_code=stock_code,
                issuer_name=str(value.get("SECURITY_NAME") or "").strip(),
                source_document_id=(
                    re.sub(r"\W+", "", relative_url.rsplit("/", 1)[-1].split(".")[0])
                    or hashlib.sha256(document_url.encode("utf-8")).hexdigest()[:20]
                ),
                document_url=document_url,
                title=title,
                released_at=released_at,
                display_date=display_date,
                time_precision=precision,
                event_type=classify_buyback_title(title),
            )
        )
    return tuple(rows), page_count, _canonical_sha256(sorted(schema_keys))


def parse_cninfo_announcement_payload(
    raw: bytes,
    *,
    market: Literal["SH", "SZ"],
    source_key: str,
    source_label: str,
) -> tuple[tuple[AnnouncementRecord, ...], bool, str]:
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BuybackSourceError("BUYBACK_CNINFO_JSON_INVALID") from None
    if not isinstance(payload, dict):
        raise BuybackSourceError("BUYBACK_CNINFO_SCHEMA_CHANGED")
    values = payload.get("announcements") or []
    if not isinstance(values, list):
        raise BuybackSourceError("BUYBACK_CNINFO_SCHEMA_CHANGED")
    rows: list[AnnouncementRecord] = []
    schema_keys: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            raise BuybackSourceError("BUYBACK_CNINFO_SCHEMA_CHANGED")
        schema_keys.update(str(key) for key in value)
        stock_code = str(value.get("secCode") or "").strip()
        title = strip_markup(str(value.get("announcementTitle") or ""))
        adjunct = str(value.get("adjunctUrl") or "").strip()
        if not re.fullmatch(r"\d{6}", stock_code) or not title or not adjunct:
            continue
        released_at, precision = _cninfo_release(value.get("announcementTime"))
        rows.append(
            AnnouncementRecord(
                source_key=source_key,
                source_label=source_label,
                market=market,
                stock_code=stock_code,
                issuer_name=str(value.get("secName") or "").strip(),
                source_document_id=str(
                    value.get("announcementId")
                    or adjunct.rsplit("/", 1)[-1].split(".")[0]
                ),
                document_url=urljoin("https://static.cninfo.com.cn/", adjunct),
                title=title,
                released_at=released_at,
                display_date=released_at.astimezone(SHANGHAI_TZ).date().isoformat(),
                time_precision=precision,
                event_type=classify_buyback_title(title),
            )
        )
    return tuple(rows), bool(payload.get("hasMore")), _canonical_sha256(sorted(schema_keys))


def validate_pdf(raw: bytes, *, maximum_pages: int = 200, maximum_text_chars: int = 200_000) -> ParsedPdf:
    if not raw.startswith(b"%PDF-"):
        raise BuybackSourceError("BUYBACK_DOCUMENT_NOT_PDF")
    try:
        reader = PdfReader(io.BytesIO(raw), strict=False)
        if reader.is_encrypted and reader.decrypt("") == 0:
            raise BuybackSourceError("BUYBACK_PDF_ENCRYPTED")
        page_count = len(reader.pages)
        if not 1 <= page_count <= maximum_pages:
            raise BuybackSourceError("BUYBACK_PDF_PAGE_COUNT_INVALID")
        parts: list[str] = []
        remaining = maximum_text_chars
        for page in reader.pages:
            if remaining <= 0:
                break
            text_value = page.extract_text() or ""
            if text_value:
                normalized = re.sub(r"\s+", " ", text_value).strip()
                parts.append(normalized[:remaining])
                remaining -= len(parts[-1])
    except BuybackSourceError:
        raise
    except Exception:
        raise BuybackSourceError("BUYBACK_PDF_PARSE_FAILED") from None
    text = " ".join(part for part in parts if part).strip()
    if not text:
        return ParsedPdf(page_count, None, None, "VALID_PDF_NO_TEXT", None)
    terms = ("回购", "购回", "repurchase", "buy-back", "buyback")
    folded = text.casefold()
    position = min(
        (folded.find(term.casefold()) for term in terms if folded.find(term.casefold()) >= 0),
        default=0,
    )
    start = max(0, position - 160)
    excerpt = text[start : start + 900]
    return ParsedPdf(
        page_count=page_count,
        text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        evidence_excerpt=excerpt,
        quality_state="VALID_PDF_TEXT",
        text=text,
    )


def parse_sse_connect(raw: bytes) -> tuple[frozenset[str], datetime, str]:
    payload = parse_jsonp(raw)
    values = payload.get("result")
    if not isinstance(values, list) or not values:
        raise BuybackSourceError("BUYBACK_CONNECT_SSE_SCHEMA_CHANGED")
    codes: set[str] = set()
    update_dates: list[date] = []
    schema_keys: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            raise BuybackSourceError("BUYBACK_CONNECT_SSE_SCHEMA_CHANGED")
        schema_keys.update(str(key) for key in value)
        code = str(value.get("SECURITY_CODE") or "").strip().zfill(5)
        if (
            re.fullmatch(r"\d{5}", code)
            and str(value.get("SECURITY_TYPE") or "") == "股票"
            and str(value.get("TRADE_FLAG") or "1") == "1"
        ):
            codes.add(code)
        raw_date = str(value.get("UPDATE_DATE") or "")[:10]
        try:
            update_dates.append(date.fromisoformat(raw_date))
        except ValueError:
            pass
    if not codes or len(codes) > 2000:
        raise BuybackSourceError("BUYBACK_CONNECT_SSE_COUNT_INVALID")
    if not update_dates:
        raise BuybackSourceError("BUYBACK_CONNECT_SSE_SOURCE_DATE_MISSING")
    source_date = max(update_dates)
    source_time = datetime.combine(source_date, datetime.min.time(), SHANGHAI_TZ).astimezone(UTC)
    return frozenset(codes), source_time, _canonical_sha256(sorted(schema_keys))


def parse_szse_connect_page(
    raw: bytes,
) -> tuple[tuple[str, ...], int, int, datetime, str]:
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BuybackSourceError("BUYBACK_CONNECT_SZ_JSON_INVALID") from None
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise BuybackSourceError("BUYBACK_CONNECT_SZ_SCHEMA_CHANGED")
    wrapper = payload[0]
    metadata = wrapper.get("metadata")
    values = wrapper.get("data")
    if not isinstance(metadata, dict) or not isinstance(values, list):
        raise BuybackSourceError("BUYBACK_CONNECT_SZ_SCHEMA_CHANGED")
    try:
        page_count = int(metadata["pagecount"])
        int(metadata["recordcount"])
        page_no = int(metadata["pageno"])
        source_date = date.fromisoformat(str(metadata["subname"])[:10])
    except (KeyError, TypeError, ValueError):
        raise BuybackSourceError("BUYBACK_CONNECT_SZ_SCHEMA_CHANGED") from None
    schema_keys: set[str] = set()
    codes: list[str] = []
    for value in values:
        if not isinstance(value, dict):
            raise BuybackSourceError("BUYBACK_CONNECT_SZ_SCHEMA_CHANGED")
        schema_keys.update(str(key) for key in value)
        code = str(value.get("zqdm") or "").strip().zfill(5)
        if re.fullmatch(r"\d{5}", code):
            codes.append(code)
    source_time = datetime.combine(source_date, datetime.min.time(), SHANGHAI_TZ).astimezone(UTC)
    return tuple(codes), page_no, page_count, source_time, _canonical_sha256(sorted(schema_keys))


def parse_hkex_calendar(raw: bytes, *, base_url: str) -> dict[str, str]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise BuybackSourceError("BUYBACK_HKEX_CALENDAR_ENCODING_INVALID") from None
    links = re.findall(r"href=['\"]([^'\"]+\.xls)['\"]", text, re.I)
    return {
        link.rsplit("/", 1)[-1].casefold(): urljoin(base_url, link)
        for link in links
    }


def _parse_number(value: Any, *, required: bool = False) -> float | None:
    if value is None:
        if required:
            raise BuybackSourceError("BUYBACK_HKEX_REQUIRED_NUMBER_MISSING")
        return None
    text = str(value).strip()
    if not text or text in {"-", "—", "--", "N/A"}:
        if required:
            raise BuybackSourceError("BUYBACK_HKEX_REQUIRED_NUMBER_MISSING")
        return None
    try:
        parsed = float(Decimal(text.replace(",", "").replace("%", "")))
    except (InvalidOperation, ValueError):
        if required:
            raise BuybackSourceError("BUYBACK_HKEX_NUMBER_INVALID") from None
        return None
    if not math.isfinite(parsed) or parsed < 0:
        raise BuybackSourceError("BUYBACK_HKEX_NUMBER_INVALID")
    return parsed


def _parse_currency_number(value: Any) -> tuple[str | None, float | None, str]:
    text = str(value or "").strip()
    if not text:
        return None, None, "EMPTY"
    if text in {"-", "—", "--", "N/A"}:
        return None, None, "EXPLICIT_NOT_REPORTED"
    match = re.fullmatch(r"([A-Z]{3})\s+([\d,.]+)", text)
    if not match:
        return None, None, "UNPARSED"
    return match.group(1), _parse_number(match.group(2)), "PARSED"


def _stock_code(value: Any) -> str | None:
    if isinstance(value, float) and value.is_integer():
        text = str(int(value))
    else:
        text = str(value).strip()
    return text.zfill(5) if re.fullmatch(r"\d{1,5}", text) else None


def parse_hkex_report(raw: bytes) -> tuple[tuple[HkexExecution, ...], str, str | None]:
    if not raw.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        raise BuybackSourceError("BUYBACK_HKEX_REPORT_NOT_XLS")
    try:
        book = xlrd.open_workbook(file_contents=raw)
        sheet = book.sheet_by_index(0)
    except Exception:
        raise BuybackSourceError("BUYBACK_HKEX_REPORT_PARSE_FAILED") from None
    header_row = next(
        (
            row_index
            for row_index in range(sheet.nrows)
            if str(sheet.cell_value(row_index, 0)).strip() == "Company"
        ),
        None,
    )
    if header_row is None:
        raise BuybackSourceError("BUYBACK_HKEX_HEADER_MISSING")
    headers = tuple(
        re.sub(r"\s+", " ", str(sheet.cell_value(header_row, column))).strip()
        for column in range(sheet.ncols)
    )
    if headers not in KNOWN_HKEX_HEADERS:
        raise BuybackSourceError("BUYBACK_HKEX_HEADER_CHANGED")
    printed_text = " ".join(
        str(sheet.cell_value(row_index, 0))
        for row_index in range(min(header_row + 1, sheet.nrows))
    )
    printed_match = re.search(r"Date Printed\s*:\s*(\d{2}/\d{2}/\d{4})", printed_text)
    printed_date = None
    if printed_match:
        printed_date = datetime.strptime(printed_match.group(1), "%d/%m/%Y").date().isoformat()

    executions: list[HkexExecution] = []
    for row_index in range(header_row + 1, sheet.nrows):
        cells = [sheet.cell_value(row_index, column) for column in range(sheet.ncols)]
        code = _stock_code(cells[1]) if len(cells) > 1 else None
        trading_date = str(cells[3]).strip() if len(cells) > 3 else ""
        if code is None or not re.fullmatch(r"\d{4}/\d{2}/\d{2}", trading_date):
            continue
        shares = _parse_number(cells[4], required=True)
        if shares is None:
            continue
        high_currency, high_price, high_status = _parse_currency_number(cells[5])
        low_currency, low_price, low_status = _parse_currency_number(cells[6])
        amount_currency, amount, amount_status = _parse_currency_number(cells[7])
        if high_status == "UNPARSED" or low_status == "UNPARSED" or amount_status == "UNPARSED":
            raise BuybackSourceError("BUYBACK_HKEX_CURRENCY_NUMBER_INVALID")
        method = str(cells[8]).strip()
        folded = method.casefold()
        if "shanghai stock exchange" in folded:
            venue = "SSE"
        elif "shenzhen stock exchange" in folded:
            venue = "SZSE"
        elif "new york stock exchange" in folded or "nyse" in folded:
            venue = "NYSE"
        elif folded.startswith("exchange") or "stock exchange of hong kong" in folded:
            venue = "HKEX"
        else:
            venue = "OTHER_OR_UNKNOWN"
        currencies = {
            value
            for value in (high_currency, low_currency, amount_currency)
            if value is not None
        }
        executions.append(
            HkexExecution(
                company=str(cells[0]).strip(),
                stock_code=code,
                share_class=str(cells[2]).strip(),
                trading_date=trading_date.replace("/", "-"),
                shares=shares,
                high_price=high_price,
                low_price=low_price,
                amount=amount,
                currency=(next(iter(currencies)) if len(currencies) == 1 else None),
                method=method,
                execution_venue=venue,
                total_repurchased_shares=_parse_number(cells[9]) if len(cells) > 9 else None,
                cancellation_shares=_parse_number(cells[10]) if len(cells) == 14 else None,
                treasury_shares=_parse_number(cells[11]) if len(cells) == 14 else None,
                mandate_exchange_shares=_parse_number(cells[12]) if len(cells) == 14 else None,
                mandate_percentage=(
                    _parse_number(cells[13])
                    if len(cells) == 14
                    else _parse_number(cells[10])
                ),
                currency_consistent=len(currencies) <= 1,
            )
        )
    return tuple(executions), _canonical_sha256(headers), printed_date


@dataclass(frozen=True)
class BuybackSettings:
    interval_seconds: float = 3600
    jitter_seconds: float = 300
    lookback_days: int = 7
    hkex_history_days: int = 120
    max_hkex_backfill_reports_per_run: int = 120
    timeout_seconds: float = 10
    max_documents_per_run: int = 20
    max_index_pages: int = 50
    hkex_refresh_seconds: float = 6 * 3600
    connect_refresh_seconds: float = 24 * 3600
    max_issues_per_run: int = 120
    proxy_url: str | None = None

    def __post_init__(self) -> None:
        if not 60 <= self.interval_seconds <= 24 * 3600:
            raise ValueError("BUYBACK_INTERVAL_INVALID")
        if not 0 <= self.jitter_seconds <= self.interval_seconds:
            raise ValueError("BUYBACK_JITTER_INVALID")
        if not 1 <= self.lookback_days <= 14:
            raise ValueError("BUYBACK_LOOKBACK_INVALID")
        if not self.lookback_days <= self.hkex_history_days <= 550:
            raise ValueError("BUYBACK_HKEX_HISTORY_INVALID")
        if not 1 <= self.max_hkex_backfill_reports_per_run <= 250:
            raise ValueError("BUYBACK_HKEX_BACKFILL_LIMIT_INVALID")
        if not 0 < self.timeout_seconds <= 60:
            raise ValueError("BUYBACK_TIMEOUT_INVALID")
        if not 1 <= self.max_documents_per_run <= 200:
            raise ValueError("BUYBACK_DOCUMENT_LIMIT_INVALID")
        if not 1 <= self.max_index_pages <= 50:
            raise ValueError("BUYBACK_PAGE_LIMIT_INVALID")
        if self.hkex_refresh_seconds < self.interval_seconds:
            raise ValueError("BUYBACK_HKEX_REFRESH_INVALID")
        if self.connect_refresh_seconds < self.interval_seconds:
            raise ValueError("BUYBACK_CONNECT_REFRESH_INVALID")
        if not 1 <= self.max_issues_per_run <= 500:
            raise ValueError("BUYBACK_ISSUE_LIMIT_INVALID")


@dataclass(frozen=True)
class SourceFetch:
    records: tuple[Any, ...]
    record_count: int
    source_time: datetime | None
    status: Literal["SUCCESS", "EMPTY", "PARTIAL"]
    detail_code: str | None
    payload: dict[str, Any]


class AHKBuybackMonitor:
    monitor_id = MONITOR_ID
    display_name = "A股与港股通回购情报"
    description = (
        "汇总 A 股官方回购披露、港交所实际回购日报及沪深港股通资格，"
        "计算实际执行天数、累计金额、回购均价与回购吸引力，帮助快速安排研究顺序。"
    )
    default_enabled = False
    projection_kind = "buyback"
    view = MonitorView(
        filters=(
            ViewFilter(
                "market_scope",
                "市场",
                "*",
                (
                    FilterChoice("*", "全部"),
                    FilterChoice("A_SHARE", "A股"),
                    FilterChoice("HK", "港股"),
                ),
            ),
            ViewFilter(
                "event_type",
                "事件阶段",
                "*",
                (
                    FilterChoice(
                        "*",
                        "全部",
                        "显示所有已经进入回购情报清单的事件阶段。",
                    ),
                    *tuple(
                        FilterChoice(
                            value,
                            EVENT_TYPE_LABELS[value],
                            EVENT_STAGE_DESCRIPTIONS[value],
                        )
                        for value in DISPLAY_EVENT_TYPES
                    ),
                ),
            ),
            ViewFilter(
                "attention_level",
                "关注分类",
                "*",
                (
                    FilterChoice("*", "全部", "显示全部关注分类。"),
                    *tuple(
                        FilterChoice(
                            value,
                            ATTENTION_LEVEL_LABELS[value],
                            ATTENTION_LEVEL_DESCRIPTIONS[value],
                        )
                        for value in ("PRIORITY", "TRACKING", "UPDATE")
                    ),
                ),
            ),
            ViewFilter(
                "connect_status",
                "购买资格",
                "BUY_ELIGIBLE",
                (
                    FilterChoice(
                        "BUY_ELIGIBLE",
                        "可购买",
                        "包括全部 A 股，以及已纳入沪港通或深港通当前名单的港股。",
                    ),
                    FilterChoice(
                        "NOT_BUY_ELIGIBLE",
                        "不可购买",
                        "仅包括未纳入沪港通和深港通当前名单的港股。",
                    ),
                ),
                multiple=True,
            ),
        ),
        columns=(
            ViewColumn(
                "attention_label",
                "关注分类",
                description=(
                    "按事件阶段安排研判顺序：优先研判包括首次实施、方案变更、"
                    "出售已回购股份和港股实际回购；持续跟踪包括方案 / 审议和"
                    "实施进展；状态更新包括完成 / 终止和注销。"
                ),
            ),
            ViewColumn("security_label", "证券"),
            ViewColumn(
                "daily_change_percent",
                "涨跌幅",
                "percent",
                maximum_fraction_digits=2,
                description=(
                    "交易时段显示最新涨跌幅；休市后显示最近一个交易日的收盘涨跌幅。"
                ),
            ),
            ViewColumn(
                "attractiveness_score",
                "回购吸引力",
                "number",
                description=(
                    "总分100：实际回购占市值25%、现价相对回购均价20%、"
                    "执行力度15%、净资产收益率与营收/净利润同比30%、披露时效10%。"
                    "缺失输入降低覆盖度；该分数只用于安排研究顺序。"
                ),
            ),
            ViewColumn(
                "roe_percent",
                "年度ROE",
                "percent",
                maximum_fraction_digits=1,
                show_sign=False,
                description=(
                    "最近一个完整财年的净资产收益率，用于跨公司比较盈利质量；"
                    "报告期可在详情中查看。"
                ),
            ),
            ViewColumn(
                "revenue_yoy_percent",
                "营收同比",
                "percent",
                maximum_fraction_digits=1,
                description="最新已披露报告期的营业收入同比变化。",
            ),
            ViewColumn(
                "net_profit_yoy_percent",
                "净利同比",
                "percent",
                maximum_fraction_digits=1,
                description="最新已披露报告期的归母净利润同比变化。",
            ),
            ViewColumn(
                "execution_days_value",
                "实际执行天数",
                "number",
                description=(
                    "只统计真正发生回购的交易日。港股日报历史覆盖本轮起点时"
                    "显示完整天数，否则显示可确认的最低天数；A股未披露逐日"
                    "明细时保持为空。"
                ),
            ),
            ViewColumn(
                "cumulative_shares",
                "累计股数",
                "number",
                description=(
                    "本轮已累计回购的股份数。历史未覆盖本轮起点时，港股仍采用"
                    "港交所日报直接披露的本轮累计股数。"
                ),
            ),
            ViewColumn(
                "cumulative_amount",
                "累计金额",
                "number",
                description=(
                    "本轮累计回购金额。港股日报历史尚未覆盖本轮起点时，仅标注"
                    "近7日金额并排在完整累计值之后。"
                ),
            ),
            ViewColumn(
                "average_cost",
                "回购均价",
                "number",
                description=(
                    "本轮累计回购金额除以累计回购股数。历史未完整时只标注近7日"
                    "均价，不与完整本轮均价混合排序。"
                ),
            ),
            ViewColumn("current_price", "现价", "number"),
            ViewColumn(
                "price_vs_average_percent",
                "现价/均价",
                "percent",
                maximum_fraction_digits=2,
                description="现价相对本轮回购均价的涨跌幅；负值表示现价低于回购均价。",
            ),
            ViewColumn(
                "actual_amount_yield_percent",
                "回购/市值",
                "percent",
                maximum_fraction_digits=2,
                show_sign=False,
                description="本轮累计回购金额占当前总市值的比例。",
            ),
            ViewColumn("intelligence_summary", "回购情报", priority="secondary"),
            ViewColumn("effective_date", "日期", priority="secondary"),
        ),
        chart_title="回购事件不生成价格历史曲线",
        table_title="回购情报清单",
    )

    def __init__(
        self,
        settings: BuybackSettings,
        *,
        store: SQLiteMonitorStore,
        client: BuybackPublicClient | None = None,
        now: Callable[[], datetime] = utc_now,
        schedule: BuybackTradingSchedule | None = None,
    ) -> None:
        self.settings = settings
        self.interval_seconds = settings.interval_seconds
        self.jitter_seconds = settings.jitter_seconds
        self.store = store
        self._now = now
        self._schedule = schedule or BuybackTradingSchedule()
        self._stop_event: threading.Event | None = None
        self.client = client or BuybackPublicClient(
            timeout_seconds=settings.timeout_seconds,
            proxy_url=settings.proxy_url,
            now=now,
        )
        self._document_text_cache: dict[str, str | None] = {}

    def automatic_collection_state(
        self,
        *,
        now: datetime,
    ) -> AutomaticCollectionState:
        return self._schedule.state(now=now)

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event
        binder = getattr(self.client, "bind_stop_event", None)
        if callable(binder):
            binder(stop_event)

    def _raise_if_cancelled(self) -> None:
        if self._stop_event is not None and self._stop_event.is_set():
            raise CollectionCancelled("BUYBACK_COLLECTION_CANCELLED")

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return self.client.network_request_count(window_seconds=window_seconds)

    def collect(self) -> CollectionBatch:
        self._raise_if_cancelled()
        now = self._now()
        if now.utcoffset() is None:
            raise RuntimeError("BUYBACK_NOW_MUST_BE_TIMEZONE_AWARE")
        cached: dict[str, StoredBuybackSourceState | BuybackSourceObservation] = {
            value.source_key: value
            for value in self.store.buyback_source_states(self.monitor_id)
        }
        observations: list[BuybackSourceObservation] = []
        issues: list[CollectionIssue] = []
        documents: list[BuybackEvidenceDocument] = []
        revisions: list[BuybackEntityRevision] = []

        self._collect_connect_sources(
            now,
            cached=cached,
            observations=observations,
            issues=issues,
        )
        self._raise_if_cancelled()

        announcement_records: list[AnnouncementRecord] = []
        announcement_sources = (
            ("sse-announcements", "上交所公告索引", self._fetch_sse_announcements),
            (
                "cninfo-sz-announcements",
                "巨潮深市公告索引",
                lambda observed: self._fetch_cninfo_announcements(observed, market="SZ"),
            ),
            (
                "cninfo-sh-announcements",
                "巨潮沪市公告索引",
                lambda observed: self._fetch_cninfo_announcements(observed, market="SH"),
            ),
        )
        for source_key, source_label, fetcher in announcement_sources:
            self._raise_if_cancelled()
            try:
                result = fetcher(now)
            except BuybackSourceError as exc:
                observation = self._failure_observation(
                    source_key,
                    source_label,
                    now=now,
                    cadence_seconds=self.interval_seconds,
                    cached=cached.get(source_key),
                    reason_code=exc.reason_code,
                )
                self._append_issue(issues, source_key, exc.reason_code)
            else:
                announcement_records.extend(result.records)
                observation = self._success_observation(
                    source_key,
                    source_label,
                    now=now,
                    cadence_seconds=self.interval_seconds,
                    result=result,
                )
            observations.append(observation)
            cached[source_key] = observation

        self._collect_a_reference_source(
            now,
            cached=cached,
            observations=observations,
            issues=issues,
        )
        self._raise_if_cancelled()
        reference_programmes = self._reference_programmes(
            cached.get("a-share-buyback-reference")
        )

        document_result = self._collect_a_share_documents(
            announcement_records,
            now=now,
            documents=documents,
            revisions=revisions,
            issues=issues,
            reference_programmes=reference_programmes,
        )
        document_observation = self._success_observation(
            "a-share-documents",
            "A股公告原文",
            now=now,
            cadence_seconds=self.interval_seconds,
            result=document_result,
        )
        observations.append(document_observation)
        cached[document_observation.source_key] = document_observation

        for board, config in HKEX_BOARDS.items():
            self._raise_if_cancelled()
            source_key = f"hkex-{board}-reports"
            cached_source = cached.get(source_key)
            history_window_current = bool(
                cached_source is not None
                and cached_source.payload.get("history_window_days")
                == self.settings.hkex_history_days
            )
            if history_window_current and not self._source_due(cached_source, now):
                continue
            try:
                result, board_documents, board_revisions, board_issues = (
                    self._fetch_hkex_board(board, now=now, source_states=cached)
                )
            except BuybackSourceError as exc:
                observation = self._failure_observation(
                    source_key,
                    str(config["label"]),
                    now=now,
                    cadence_seconds=self.settings.hkex_refresh_seconds,
                    cached=cached.get(source_key),
                    reason_code=exc.reason_code,
                )
                self._append_issue(issues, source_key, exc.reason_code)
            else:
                documents.extend(board_documents)
                revisions.extend(board_revisions)
                for issue in board_issues:
                    self._append_issue(issues, issue.scope, issue.reason_code)
                observation = self._success_observation(
                    source_key,
                    str(config["label"]),
                    now=now,
                    cadence_seconds=self.settings.hkex_refresh_seconds,
                    result=result,
                )
            observations.append(observation)
            cached[source_key] = observation

        market_securities: set[tuple[str, str, str]] = set()
        reference_payloads = [revision.payload for revision in revisions]
        reference_payloads.extend(
            entity.payload
            for entity in self.store.latest_buyback_entities(
                self.monitor_id,
                limit=20_000,
            )
        )
        for payload in reference_payloads:
            market_scope = str(payload.get("market_scope") or "")
            market = str(payload.get("market") or "")
            stock_code = str(payload.get("stock_code") or "")
            if market_scope == "HK" and re.fullmatch(r"\d{1,5}", stock_code):
                market_securities.add(("HK", "HK", stock_code.zfill(5)))
            elif (
                market_scope == "A_SHARE"
                and market in {"SH", "SZ"}
                and re.fullmatch(r"\d{6}", stock_code)
            ):
                market_securities.add(("A_SHARE", market, stock_code))
        self._collect_hk_market_reference(
            now,
            securities=market_securities,
            cached=cached,
            observations=observations,
            issues=issues,
        )
        self._raise_if_cancelled()
        self._collect_financial_reference(
            now,
            securities=market_securities,
            cached=cached,
            observations=observations,
            issues=issues,
        )

        return CollectionBatch(
            samples=(),
            issues=tuple(issues),
            buyback_documents=tuple(documents),
            buyback_revisions=tuple(revisions),
            buyback_source_observations=tuple(observations),
        )

    def _append_issue(
        self,
        issues: list[CollectionIssue],
        scope: str,
        reason_code: str,
    ) -> None:
        if len(issues) >= self.settings.max_issues_per_run:
            return
        issue = CollectionIssue(scope, reason_code)
        if issue not in issues:
            issues.append(issue)

    @staticmethod
    def _source_due(
        cached: StoredBuybackSourceState | BuybackSourceObservation | None,
        now: datetime,
    ) -> bool:
        return cached is None or cached.next_due_at <= now

    def _success_observation(
        self,
        source_key: str,
        source_label: str,
        *,
        now: datetime,
        cadence_seconds: float,
        result: SourceFetch,
    ) -> BuybackSourceObservation:
        return BuybackSourceObservation(
            source_key=source_key,
            source_label=source_label,
            status=result.status,
            checked_at=now,
            source_time=result.source_time,
            next_due_at=now + timedelta(seconds=cadence_seconds),
            record_count=result.record_count,
            detail_code=result.detail_code,
            payload=result.payload,
        )

    def _failure_observation(
        self,
        source_key: str,
        source_label: str,
        *,
        now: datetime,
        cadence_seconds: float,
        cached: StoredBuybackSourceState | BuybackSourceObservation | None,
        reason_code: str,
    ) -> BuybackSourceObservation:
        has_cached_value = bool(cached is not None and cached.payload)
        return BuybackSourceObservation(
            source_key=source_key,
            source_label=source_label,
            status="STALE" if has_cached_value else "ERROR",
            checked_at=now,
            source_time=cached.source_time if cached is not None else None,
            next_due_at=now
            + timedelta(seconds=min(cadence_seconds, self.interval_seconds)),
            record_count=cached.record_count if cached is not None else None,
            detail_code=reason_code,
            payload=dict(cached.payload) if cached is not None else {},
        )

    @staticmethod
    def _reference_programmes(
        state: StoredBuybackSourceState | BuybackSourceObservation | None,
    ) -> tuple[dict[str, Any], ...]:
        if state is None:
            return ()
        values = state.payload.get("programmes")
        if not isinstance(values, list):
            return ()
        return tuple(dict(value) for value in values if isinstance(value, dict))

    def _collect_a_reference_source(
        self,
        now: datetime,
        *,
        cached: dict[str, StoredBuybackSourceState | BuybackSourceObservation],
        observations: list[BuybackSourceObservation],
        issues: list[CollectionIssue],
    ) -> None:
        source_key = "a-share-buyback-reference"
        source_label = "A股回购结构化参考"
        if not self._source_due(cached.get(source_key), now):
            return
        try:
            result = self._fetch_a_share_reference(now)
        except (BuybackSourceError, BuybackMetricError) as exc:
            reason_code = (
                exc.reason_code
                if isinstance(exc, (BuybackSourceError, BuybackMetricError))
                else "BUYBACK_A_REFERENCE_FAILED"
            )
            observation = self._failure_observation(
                source_key,
                source_label,
                now=now,
                cadence_seconds=self.settings.hkex_refresh_seconds,
                cached=cached.get(source_key),
                reason_code=reason_code,
            )
            self._append_issue(issues, source_key, reason_code)
        else:
            observation = self._success_observation(
                source_key,
                source_label,
                now=now,
                cadence_seconds=self.settings.hkex_refresh_seconds,
                result=result,
            )
        observations.append(observation)
        cached[source_key] = observation

    def _fetch_a_share_reference(self, now: datetime) -> SourceFetch:
        params = {
            "sortColumns": "UPD,DIM_DATE,DIM_SCODE",
            "sortTypes": "-1,-1,-1",
            "pageSize": "500",
            "pageNumber": "1",
            "reportName": "RPTA_WEB_GETHGLIST_NEW",
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
        }
        response = self.client.request(
            f"{A_SHARE_REFERENCE_URL}?{urlencode(params)}",
            referer=EASTMONEY_REFERER,
            max_bytes=2 * 1024 * 1024,
            attempts=2,
        )
        content_type = response.content_type.casefold()
        if (
            "json" not in content_type
            and "text/plain" not in content_type
        ):
            raise BuybackSourceError("BUYBACK_A_REFERENCE_CONTENT_TYPE_INVALID")
        programmes, source_time, schema_hash = parse_a_share_reference(response.body)
        return SourceFetch(
            records=programmes,
            record_count=len(programmes),
            source_time=source_time,
            status="SUCCESS" if programmes else "EMPTY",
            detail_code=None,
            payload={
                "programmes": list(programmes),
                "programme_count": len(programmes),
                "schema_sha256": schema_hash,
                "checked_at": iso_utc(now),
            },
        )

    def _collect_hk_market_reference(
        self,
        now: datetime,
        *,
        securities: set[tuple[str, str, str]],
        cached: dict[str, StoredBuybackSourceState | BuybackSourceObservation],
        observations: list[BuybackSourceObservation],
        issues: list[CollectionIssue],
    ) -> None:
        source_key = "hk-market-reference"
        source_label = "A股与港股行情及业绩参考"
        cached_source = cached.get(source_key)
        schema_current = bool(
            cached_source is not None
            and cached_source.payload.get("schema_version") == 2
            and cached_source.payload.get("provider") in {"EASTMONEY", "TENCENT"}
            and cached_source.payload.get("market_contract_version") == 2
        )
        if (
            not securities
            or (schema_current and not self._source_due(cached_source, now))
        ):
            return
        primary_failure_code: str | None = None
        try:
            result = self._fetch_hk_market_reference(securities)
        except (BuybackSourceError, BuybackMetricError) as primary_exc:
            primary_failure_code = primary_exc.reason_code
            try:
                result = self._fetch_tencent_market_reference(securities)
            except (BuybackSourceError, BuybackMetricError) as fallback_exc:
                reason_code = fallback_exc.reason_code
                observation = self._failure_observation(
                    source_key,
                    source_label,
                    now=now,
                    cadence_seconds=self.settings.hkex_refresh_seconds,
                    cached=cached.get(source_key),
                    reason_code=reason_code,
                )
                self._append_issue(issues, source_key, reason_code)
            else:
                result.payload["primary_failure_code"] = primary_failure_code
                observation = self._success_observation(
                    source_key,
                    source_label,
                    now=now,
                    cadence_seconds=self.settings.hkex_refresh_seconds,
                    result=result,
                )
        else:
            observation = self._success_observation(
                source_key,
                source_label,
                now=now,
                cadence_seconds=self.settings.hkex_refresh_seconds,
                result=result,
            )
        observations.append(observation)
        cached[source_key] = observation

    def _fetch_hk_market_reference(
        self,
        securities: set[tuple[str, str, str]],
    ) -> SourceFetch:
        ordered = sorted(securities)
        if len(ordered) > 1000:
            raise BuybackSourceError("BUYBACK_MARKET_REFERENCE_COUNT_INVALID")
        quotes: list[dict[str, Any]] = []
        source_times: list[datetime] = []
        schema_hashes: list[str] = []
        market_ids = {"SH": "1", "SZ": "0", "HK": "116"}
        batch_size = 30
        for offset in range(0, len(ordered), batch_size):
            batch = ordered[offset : offset + batch_size]
            params = {
                "fltt": "2",
                "invt": "2",
                "fields": (
                    "f2,f3,f9,f12,f13,f14,f18,f20,f23,f37,f40,f41,"
                    "f45,f46,f49,f124"
                ),
                "secids": ",".join(
                    f"{market_ids[market]}.{code}"
                    for _market_scope, market, code in batch
                ),
            }
            response = self.client.request(
                f"{HK_MARKET_REFERENCE_URL}?{urlencode(params)}",
                referer=EASTMONEY_REFERER,
                max_bytes=512 * 1024,
                attempts=2,
            )
            if "json" not in response.content_type.casefold():
                raise BuybackSourceError(
                    "BUYBACK_MARKET_REFERENCE_CONTENT_TYPE_INVALID"
                )
            try:
                batch_quotes, batch_time, schema_hash = parse_market_reference(
                    response.body,
                    expected_securities=batch,
                )
            except BuybackMetricError as exc:
                if exc.reason_code != "BUYBACK_MARKET_REFERENCE_EMPTY":
                    raise
                batch_quotes, batch_time, schema_hash = (), None, "EMPTY"
            quotes.extend(batch_quotes)
            if batch_time is not None:
                source_times.append(batch_time)
            schema_hashes.append(schema_hash)
        source_time = max(source_times, default=None)
        schema_hash = _canonical_sha256(schema_hashes)
        missing_count = len(ordered) - len(quotes)
        return SourceFetch(
            records=tuple(quotes),
            record_count=len(quotes),
            source_time=source_time,
            status="PARTIAL" if missing_count else "SUCCESS" if quotes else "EMPTY",
            detail_code=(
                "BUYBACK_MARKET_REFERENCE_INCOMPLETE" if missing_count else None
            ),
            payload={
                "schema_version": 2,
                "market_contract_version": 2,
                "provider": "EASTMONEY",
                "quotes": quotes,
                "quote_count": len(quotes),
                "requested_count": len(ordered),
                "missing_count": missing_count,
                "batch_size": batch_size,
                "schema_sha256": schema_hash,
            },
        )

    def _fetch_tencent_market_reference(
        self,
        securities: set[tuple[str, str, str]],
    ) -> SourceFetch:
        ordered = sorted(securities)
        if len(ordered) > 1000:
            raise BuybackSourceError("BUYBACK_MARKET_REFERENCE_COUNT_INVALID")
        quotes: list[dict[str, Any]] = []
        source_times: list[datetime] = []
        schema_hashes: list[str] = []
        prefixes = {"SH": "sh", "SZ": "sz", "HK": "hk"}
        batch_size = 30
        for offset in range(0, len(ordered), batch_size):
            batch = ordered[offset : offset + batch_size]
            symbols = ",".join(
                f"{prefixes[market]}{code}"
                for _market_scope, market, code in batch
            )
            response = self.client.request(
                f"{TENCENT_MARKET_REFERENCE_URL}{symbols}",
                referer=TENCENT_MARKET_REFERENCE_REFERER,
                max_bytes=512 * 1024,
                attempts=2,
            )
            if "text" not in response.content_type.casefold():
                raise BuybackSourceError(
                    "BUYBACK_MARKET_REFERENCE_CONTENT_TYPE_INVALID"
                )
            try:
                batch_quotes, batch_time, schema_hash = (
                    parse_tencent_market_reference(
                        response.body,
                        expected_securities=batch,
                    )
                )
            except BuybackMetricError as exc:
                if exc.reason_code != "BUYBACK_MARKET_REFERENCE_EMPTY":
                    raise
                batch_quotes, batch_time, schema_hash = (), None, "EMPTY"
            quotes.extend(batch_quotes)
            if batch_time is not None:
                source_times.append(batch_time)
            schema_hashes.append(schema_hash)
        missing_count = len(ordered) - len(quotes)
        detail_code = (
            "BUYBACK_MARKET_REFERENCE_INCOMPLETE" if missing_count else None
        )
        return SourceFetch(
            records=tuple(quotes),
            record_count=len(quotes),
            source_time=max(source_times, default=None),
            status="PARTIAL" if missing_count else "SUCCESS" if quotes else "EMPTY",
            detail_code=detail_code,
            payload={
                "schema_version": 2,
                "market_contract_version": 2,
                "provider": "TENCENT",
                "quotes": quotes,
                "quote_count": len(quotes),
                "requested_count": len(ordered),
                "missing_count": missing_count,
                "batch_size": batch_size,
                "schema_sha256": _canonical_sha256(schema_hashes),
            },
        )

    def _collect_financial_reference(
        self,
        now: datetime,
        *,
        securities: set[tuple[str, str, str]],
        cached: dict[str, StoredBuybackSourceState | BuybackSourceObservation],
        observations: list[BuybackSourceObservation],
        issues: list[CollectionIssue],
    ) -> None:
        source_key = "buyback-financial-reference"
        source_label = "A股与港股公开业绩参考"
        cached_source = cached.get(source_key)
        schema_current = bool(
            cached_source is not None
            and cached_source.payload.get("schema_version") == 1
        )
        if (
            not securities
            or (schema_current and not self._source_due(cached_source, now))
        ):
            return
        try:
            result = self._fetch_financial_reference(securities, now=now)
        except (BuybackSourceError, BuybackMetricError) as exc:
            observation = self._failure_observation(
                source_key,
                source_label,
                now=now,
                cadence_seconds=self.settings.hkex_refresh_seconds,
                cached=cached_source,
                reason_code=exc.reason_code,
            )
            self._append_issue(issues, source_key, exc.reason_code)
        else:
            observation = self._success_observation(
                source_key,
                source_label,
                now=now,
                cadence_seconds=self.settings.hkex_refresh_seconds,
                result=result,
            )
        observations.append(observation)
        cached[source_key] = observation

    def _fetch_financial_reference(
        self,
        securities: set[tuple[str, str, str]],
        *,
        now: datetime,
    ) -> SourceFetch:
        ordered = sorted(securities)
        if len(ordered) > 1000:
            raise BuybackSourceError("BUYBACK_FINANCIAL_REFERENCE_COUNT_INVALID")
        financials: list[dict[str, Any]] = []
        source_times: list[datetime] = []
        schema_hashes: list[str] = []
        since = (now.astimezone(SHANGHAI_TZ).date() - timedelta(days=800)).isoformat()
        batch_size = 30
        source_contracts = (
            (
                "A_SHARE",
                "RPT_F10_FINANCE_MAINFINADATA",
                (
                    "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,REPORT_DATE,"
                    "REPORT_DATE_NAME,REPORT_TYPE,NOTICE_DATE,UPDATE_DATE,"
                    "CURRENCY,ROEJQ,TOTALOPERATEREVETZ,PARENTNETPROFITTZ"
                ),
                "HSF10",
                "REPORT_DATE",
            ),
            (
                "HK",
                "RPT_HKF10_FN_MAININDICATOR",
                "HKF10_FN_MAININDICATOR",
                "F10",
                "STD_REPORT_DATE",
            ),
        )
        for market_scope, report_name, columns, source, sort_column in source_contracts:
            scope_values = [
                value for value in ordered if value[0] == market_scope
            ]
            for offset in range(0, len(scope_values), batch_size):
                batch = scope_values[offset : offset + batch_size]
                secucodes = ",".join(
                    f'"{code}.{market}"' for _scope, market, code in batch
                )
                params = {
                    "reportName": report_name,
                    "columns": columns,
                    "quoteColumns": "",
                    "pageNumber": "1",
                    "pageSize": "500",
                    "sortTypes": "-1",
                    "sortColumns": sort_column,
                    "source": source,
                    "client": "PC",
                    "filter": (
                        f"(SECUCODE in ({secucodes}))"
                        f"(REPORT_DATE>='{since}')"
                    ),
                }
                response = self.client.request(
                    f"{FINANCIAL_REFERENCE_URL}?{urlencode(params)}",
                    referer="https://emweb.securities.eastmoney.com/",
                    max_bytes=2 * 1024 * 1024,
                    attempts=2,
                )
                content_type = response.content_type.casefold()
                if "json" not in content_type and "text/plain" not in content_type:
                    raise BuybackSourceError(
                        "BUYBACK_FINANCIAL_REFERENCE_CONTENT_TYPE_INVALID"
                    )
                try:
                    batch_records, batch_time, schema_hash = (
                        parse_financial_reference(
                            response.body,
                            expected_securities=batch,
                        )
                    )
                except BuybackMetricError as exc:
                    if exc.reason_code != "BUYBACK_FINANCIAL_REFERENCE_EMPTY":
                        raise
                    batch_records, batch_time, schema_hash = (), None, "EMPTY"
                financials.extend(batch_records)
                if batch_time is not None:
                    source_times.append(batch_time)
                schema_hashes.append(schema_hash)
        missing_count = len(ordered) - len(financials)
        return SourceFetch(
            records=tuple(financials),
            record_count=len(financials),
            source_time=max(source_times, default=None),
            status=(
                "PARTIAL" if missing_count else "SUCCESS" if financials else "EMPTY"
            ),
            detail_code=(
                "BUYBACK_FINANCIAL_REFERENCE_INCOMPLETE"
                if missing_count
                else None
            ),
            payload={
                "schema_version": 1,
                "provider": "EASTMONEY_F10",
                "financials": financials,
                "financial_count": len(financials),
                "requested_count": len(ordered),
                "missing_count": missing_count,
                "lookback_start": since,
                "batch_size": batch_size,
                "schema_sha256": _canonical_sha256(schema_hashes),
            },
        )

    def _fetch_sse_announcements(self, now: datetime) -> SourceFetch:
        end = now.astimezone(SHANGHAI_TZ).date()
        begin = end - timedelta(days=self.settings.lookback_days - 1)
        unique: dict[str, AnnouncementRecord] = {}
        schema_hashes: set[str] = set()
        response_hashes: list[str] = []
        total_pages = 0
        for keyword in ANNOUNCEMENT_KEYWORDS:
            page = 1
            page_count = 1
            while page <= page_count:
                params = {
                    "isPagination": "true",
                    "productId": "",
                    "keyWord": keyword,
                    "securityType": "0101,120100,020100,020200,120200",
                    "reportType": "ALL",
                    "beginDate": begin.isoformat(),
                    "endDate": (end + timedelta(days=1)).isoformat(),
                    "pageHelp.pageSize": "1000",
                    "pageHelp.pageNo": str(page),
                    "pageHelp.beginPage": str(page),
                    "pageHelp.endPage": str(page + 9),
                    "pageHelp.cacheSize": "1",
                    "jsonCallBack": "jsonpCallback",
                }
                response = self.client.request(
                    f"{SSE_ANNOUNCEMENT_URL}?{urlencode(params)}",
                    referer=SSE_ANNOUNCEMENT_REFERER,
                    max_bytes=2 * 1024 * 1024,
                )
                if "json" not in response.content_type.casefold():
                    raise BuybackSourceError("BUYBACK_SSE_CONTENT_TYPE_INVALID")
                rows, discovered_pages, schema_hash = parse_sse_announcement_payload(
                    response.body,
                    begin=begin,
                    end=end,
                )
                if discovered_pages < 0 or discovered_pages > self.settings.max_index_pages:
                    raise BuybackSourceError("BUYBACK_SSE_PAGE_COUNT_INVALID")
                page_count = max(1, discovered_pages)
                total_pages += 1
                schema_hashes.add(schema_hash)
                response_hashes.append(hashlib.sha256(response.body).hexdigest())
                for row in rows:
                    unique[row.source_document_id] = row
                page += 1
        records = tuple(
            sorted(unique.values(), key=lambda row: (row.released_at, row.stock_code))
        )
        source_time = max((row.released_at for row in records), default=None)
        target_count = sum(row.event_type in TARGET_EVENT_TYPES for row in records)
        return SourceFetch(
            records=records,
            record_count=len(records),
            source_time=source_time,
            status="SUCCESS" if records else "EMPTY",
            detail_code=None,
            payload={
                "window_start": begin.isoformat(),
                "window_end": end.isoformat(),
                "page_count": total_pages,
                "target_candidate_count": target_count,
                "excluded_title_count": len(records) - target_count,
                "schema_sha256": _canonical_sha256(sorted(schema_hashes)),
                "responses_sha256": _canonical_sha256(response_hashes),
            },
        )

    def _fetch_cninfo_announcements(
        self,
        now: datetime,
        *,
        market: Literal["SH", "SZ"],
    ) -> SourceFetch:
        end = now.astimezone(SHANGHAI_TZ).date()
        begin = end - timedelta(days=self.settings.lookback_days - 1)
        source_key = (
            "cninfo-sh-announcements"
            if market == "SH"
            else "cninfo-sz-announcements"
        )
        source_label = "巨潮沪市公告索引" if market == "SH" else "巨潮深市公告索引"
        market_types = "hzb,kcb" if market == "SH" else "szb,cyb"
        unique: dict[str, AnnouncementRecord] = {}
        schema_hashes: set[str] = set()
        response_hashes: list[str] = []
        total_pages = 0
        for keyword in ANNOUNCEMENT_KEYWORDS:
            page = 1
            while True:
                params = {
                    "searchkey": keyword,
                    "sdate": begin.isoformat(),
                    "edate": end.isoformat(),
                    "isfulltext": "false",
                    "sortName": "pubdate",
                    "sortType": "asc",
                    "pageNum": str(page),
                    "pageSize": "100",
                    "type": market_types,
                }
                response = self.client.request(
                    f"{CNINFO_SEARCH_URL}?{urlencode(params)}",
                    referer=CNINFO_REFERER,
                    max_bytes=2 * 1024 * 1024,
                )
                if "json" not in response.content_type.casefold():
                    raise BuybackSourceError("BUYBACK_CNINFO_CONTENT_TYPE_INVALID")
                rows, has_more, schema_hash = parse_cninfo_announcement_payload(
                    response.body,
                    market=market,
                    source_key=source_key,
                    source_label=source_label,
                )
                total_pages += 1
                schema_hashes.add(schema_hash)
                response_hashes.append(hashlib.sha256(response.body).hexdigest())
                for row in rows:
                    unique[row.source_document_id] = row
                if not has_more:
                    break
                page += 1
                if page > self.settings.max_index_pages:
                    raise BuybackSourceError("BUYBACK_CNINFO_PAGE_COUNT_INVALID")
        records = tuple(
            sorted(unique.values(), key=lambda row: (row.released_at, row.stock_code))
        )
        source_time = max((row.released_at for row in records), default=None)
        target_count = sum(row.event_type in TARGET_EVENT_TYPES for row in records)
        return SourceFetch(
            records=records,
            record_count=len(records),
            source_time=source_time,
            status="SUCCESS" if records else "EMPTY",
            detail_code=None,
            payload={
                "market": market,
                "window_start": begin.isoformat(),
                "window_end": end.isoformat(),
                "page_count": total_pages,
                "target_candidate_count": target_count,
                "excluded_title_count": len(records) - target_count,
                "schema_sha256": _canonical_sha256(sorted(schema_hashes)),
                "responses_sha256": _canonical_sha256(response_hashes),
            },
        )

    def _collect_connect_sources(
        self,
        now: datetime,
        *,
        cached: dict[str, StoredBuybackSourceState | BuybackSourceObservation],
        observations: list[BuybackSourceObservation],
        issues: list[CollectionIssue],
    ) -> None:
        sources = (
            ("connect-sh", "沪港通港股名单", self._fetch_sse_connect),
            ("connect-sz", "深港通港股名单", self._fetch_szse_connect),
        )
        for source_key, source_label, fetcher in sources:
            if not self._source_due(cached.get(source_key), now):
                continue
            try:
                result = fetcher(now)
            except BuybackSourceError as exc:
                observation = self._failure_observation(
                    source_key,
                    source_label,
                    now=now,
                    cadence_seconds=self.settings.connect_refresh_seconds,
                    cached=cached.get(source_key),
                    reason_code=exc.reason_code,
                )
                self._append_issue(issues, source_key, exc.reason_code)
            else:
                observation = self._success_observation(
                    source_key,
                    source_label,
                    now=now,
                    cadence_seconds=self.settings.connect_refresh_seconds,
                    result=result,
                )
            observations.append(observation)
            cached[source_key] = observation

    def _fetch_sse_connect(self, _now: datetime) -> SourceFetch:
        params = {
            "sqlId": "COMMON_SSE_JYFW_HGT_XXPL_BDZQQD_L",
            "jsonCallBack": "jsonpCallback",
        }
        response = self.client.request(
            f"{SSE_CONNECT_URL}?{urlencode(params)}",
            referer=SSE_CONNECT_REFERER,
            max_bytes=2 * 1024 * 1024,
        )
        if "json" not in response.content_type.casefold():
            raise BuybackSourceError("BUYBACK_CONNECT_SSE_CONTENT_TYPE_INVALID")
        codes, source_time, schema_hash = parse_sse_connect(response.body)
        return SourceFetch(
            records=tuple(sorted(codes)),
            record_count=len(codes),
            source_time=source_time,
            status="SUCCESS",
            detail_code=None,
            payload={
                "route": "SH",
                "codes": sorted(codes),
                "as_of": source_time.astimezone(SHANGHAI_TZ).date().isoformat(),
                "schema_sha256": schema_hash,
                "response_sha256": hashlib.sha256(response.body).hexdigest(),
            },
        )

    def _fetch_szse_connect(self, _now: datetime) -> SourceFetch:
        codes: set[str] = set()
        page = 1
        expected_pages: int | None = None
        expected_count: int | None = None
        source_time: datetime | None = None
        schema_hashes: set[str] = set()
        response_hashes: list[str] = []
        while expected_pages is None or page <= expected_pages:
            params = {
                "SHOWTYPE": "JSON",
                "CATALOGID": "SGT_GGTBDQD",
                "TABKEY": "tab1",
                "PAGENO": str(page),
            }
            response = self.client.request(
                f"{SZSE_REPORT_URL}/data?{urlencode(params)}",
                referer=SZSE_CONNECT_REFERER,
                max_bytes=512 * 1024,
            )
            if "json" not in response.content_type.casefold():
                raise BuybackSourceError("BUYBACK_CONNECT_SZ_CONTENT_TYPE_INVALID")
            page_codes, page_no, page_count, page_time, schema_hash = (
                parse_szse_connect_page(response.body)
            )
            if page_no != page or not 1 <= page_count <= self.settings.max_index_pages:
                raise BuybackSourceError("BUYBACK_CONNECT_SZ_PAGE_COUNT_INVALID")
            if expected_pages is None:
                try:
                    wrapper = json.loads(response.body.decode("utf-8-sig"))[0]
                    expected_count = int(wrapper["metadata"]["recordcount"])
                except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError):
                    raise BuybackSourceError("BUYBACK_CONNECT_SZ_SCHEMA_CHANGED") from None
                expected_pages = page_count
            elif expected_pages != page_count:
                raise BuybackSourceError("BUYBACK_CONNECT_SZ_PAGINATION_CHANGED")
            codes.update(page_codes)
            source_time = page_time if source_time is None else max(source_time, page_time)
            schema_hashes.add(schema_hash)
            response_hashes.append(hashlib.sha256(response.body).hexdigest())
            page += 1
        if expected_count is None or not codes or len(codes) != expected_count or len(codes) > 2000:
            raise BuybackSourceError("BUYBACK_CONNECT_SZ_COUNT_INVALID")
        assert source_time is not None
        return SourceFetch(
            records=tuple(sorted(codes)),
            record_count=len(codes),
            source_time=source_time,
            status="SUCCESS",
            detail_code=None,
            payload={
                "route": "SZ",
                "codes": sorted(codes),
                "as_of": source_time.astimezone(SHANGHAI_TZ).date().isoformat(),
                "page_count": expected_pages,
                "schema_sha256": _canonical_sha256(sorted(schema_hashes)),
                "responses_sha256": _canonical_sha256(response_hashes),
            },
        )

    def _cached_document_text(self, digest: str, path: Any) -> str | None:
        if digest in self._document_text_cache:
            return self._document_text_cache[digest]
        parsed = validate_pdf(path.read_bytes())
        if len(self._document_text_cache) >= 512:
            self._document_text_cache.pop(next(iter(self._document_text_cache)))
        self._document_text_cache[digest] = parsed.text
        return parsed.text

    def _collect_a_share_documents(
        self,
        records: Iterable[AnnouncementRecord],
        *,
        now: datetime,
        documents: list[BuybackEvidenceDocument],
        revisions: list[BuybackEntityRevision],
        issues: list[CollectionIssue],
        reference_programmes: tuple[dict[str, Any], ...],
    ) -> SourceFetch:
        grouped: dict[str, list[AnnouncementRecord]] = defaultdict(list)
        for record in records:
            if (
                record.event_type in TARGET_EVENT_TYPES
                and is_target_a_share_security(record.market, record.stock_code)
            ):
                grouped[record.identity].append(record)
        groups = sorted(
            grouped.values(),
            key=lambda values: max(value.released_at for value in values),
            reverse=True,
        )
        fetched_count = 0
        existing_count = 0
        fallback_count = 0
        failed_count = 0
        empty_text_count = 0
        backlog_count = 0

        for alternatives in groups:
            alternatives.sort(
                key=lambda value: (
                    0 if value.source_key == "sse-announcements" else 1,
                    value.source_document_id,
                )
            )
            primary = alternatives[0]
            document_sha256 = None
            document_quality = "INDEX_ONLY"
            document_source_label = None
            document_url = primary.document_url
            document_metadata: dict[str, Any] = {}
            document_text: str | None = None

            for candidate in alternatives:
                stored = self.store.buyback_document_for_source(
                    self.monitor_id,
                    candidate.source_key,
                    candidate.source_document_id,
                )
                if stored is None:
                    continue
                try:
                    path = self.store.buyback_document_path(stored, verify_content=False)
                    document_text = self._cached_document_text(stored.sha256, path)
                except (OSError, RuntimeError):
                    continue
                document_sha256 = stored.sha256
                document_quality = stored.quality_state
                document_source_label = stored.source_label
                document_url = stored.source_url
                document_metadata = stored.metadata
                existing_count += 1
                break

            last_reason = "BUYBACK_DOCUMENT_UNAVAILABLE"
            if document_sha256 is None and fetched_count < self.settings.max_documents_per_run:
                for candidate in alternatives:
                    try:
                        response = self.client.request(
                            candidate.document_url,
                            referer=(
                                SSE_ANNOUNCEMENT_REFERER
                                if candidate.source_key == "sse-announcements"
                                else CNINFO_REFERER
                            ),
                            max_bytes=20 * 1024 * 1024,
                            attempts=2,
                        )
                        parsed = validate_pdf(response.body)
                    except BuybackSourceError as exc:
                        last_reason = exc.reason_code
                        continue
                    digest = hashlib.sha256(response.body).hexdigest()
                    metadata = {
                        "page_count": parsed.page_count,
                        "text_sha256": parsed.text_sha256,
                        "evidence_excerpt": parsed.evidence_excerpt,
                        "response_content_type": response.content_type,
                        "response_bytes": len(response.body),
                        "primary_index_source": primary.source_key,
                        "primary_document_url": primary.document_url,
                    }
                    documents.append(
                        BuybackEvidenceDocument(
                            source_key=candidate.source_key,
                            source_label=candidate.source_label,
                            source_document_id=candidate.source_document_id,
                            source_url=candidate.document_url,
                            published_at=candidate.released_at,
                            observed_at=response.completed_at,
                            media_type="application/pdf",
                            file_suffix=".pdf",
                            body=response.body,
                            quality_state=parsed.quality_state,
                            metadata=metadata,
                        )
                    )
                    fetched_count += 1
                    document_sha256 = digest
                    document_quality = parsed.quality_state
                    document_source_label = candidate.source_label
                    document_url = candidate.document_url
                    document_metadata = metadata
                    document_text = parsed.text
                    if len(self._document_text_cache) >= 512:
                        self._document_text_cache.pop(next(iter(self._document_text_cache)))
                    self._document_text_cache[digest] = document_text
                    if candidate is not alternatives[0]:
                        fallback_count += 1
                    if parsed.quality_state == "VALID_PDF_NO_TEXT":
                        empty_text_count += 1
                        self._append_issue(
                            issues,
                            f"A:{primary.market}:{primary.stock_code}:{primary.identity}",
                            "BUYBACK_PDF_TEXT_EMPTY",
                        )
                    break
            elif document_sha256 is None:
                backlog_count += 1

            if document_sha256 is None and fetched_count < self.settings.max_documents_per_run:
                failed_count += 1
                self._append_issue(
                    issues,
                    f"A:{primary.market}:{primary.stock_code}:{primary.identity}",
                    last_reason,
                )

            effective_event = next(
                (
                    candidate.event_type
                    for candidate in alternatives
                    if candidate.event_type in TARGET_EVENT_TYPES
                ),
                "AMBIGUOUS_BUYBACK",
            )
            entity_key = f"A:{primary.market}:{primary.stock_code}:{primary.identity}"
            numeric_facts = (
                match_a_share_program(
                    document_text,
                    stock_code=primary.stock_code,
                    released_at=primary.released_at,
                    event_type=effective_event,
                    programmes=reference_programmes,
                )
                if document_text
                else None
            )
            data_quality_label = (
                "原文可读 · 待人工确认"
                if document_quality == "VALID_PDF_TEXT"
                else "原文需人工读取"
                if document_quality == "VALID_PDF_NO_TEXT"
                else "仅公告索引 · 原文待补"
            )
            revisions.append(
                BuybackEntityRevision(
                    entity_key=entity_key,
                    entity_type="DISCLOSURE_CANDIDATE",
                    effective_at=primary.released_at,
                    observed_at=now,
                    source_key=primary.source_key,
                    document_sha256=document_sha256,
                    payload={
                        "entity_key": entity_key,
                        "market_scope": "A_SHARE",
                        "market": primary.market,
                        "market_label": "沪市 A 股" if primary.market == "SH" else "深市 A 股",
                        "stock_code": primary.stock_code,
                        "issuer_name": primary.issuer_name,
                        "title": primary.title,
                        "event_type": effective_event,
                        "event_type_label": EVENT_TYPE_LABELS.get(
                            effective_event, effective_event
                        ),
                        "effective_at": iso_utc(primary.released_at),
                        "official_release_at": iso_utc(primary.released_at),
                        "source_time_precision": primary.time_precision,
                        "display_date": primary.display_date,
                        "earliest_actionable_rule": (
                            "FIRST_MARKET_SESSION_AFTER_OFFICIAL_RELEASE"
                            if primary.time_precision == "SECOND"
                            else "NEXT_TRADING_SESSION_AFTER_DISCLOSURE_DATE"
                        ),
                        "source_key": primary.source_key,
                        "source_label": primary.source_label,
                        "source_document_id": primary.source_document_id,
                        "source_url": primary.document_url,
                        "document_url": document_url,
                        "document_source_label": document_source_label,
                        "document_sha256": document_sha256,
                        "document_quality": document_quality,
                        "evidence_excerpt": document_metadata.get("evidence_excerpt"),
                        "candidate_status": "CANDIDATE_UNCONFIRMED",
                        "program_link_status": "UNLINKED_CANDIDATE",
                        "program_status": "UNKNOWN",
                        "review_status": "UNREVIEWED",
                        "review_status_label": "待复核",
                        "connect_status": "BUY_ELIGIBLE",
                        "connect_status_label": "可购买",
                        "connect_route_label": "A股",
                        "data_quality_label": data_quality_label,
                        "no_action_reason": "MANUAL_CONFIRMATION_REQUIRED",
                        "detail_action": "查看 / 复核",
                        "row_tone": "NOTICE",
                        "missing_reasons": (
                            {}
                            if numeric_facts is not None
                            else {
                                "shares": "公告正文中的累计回购数量尚未形成可计算字段。",
                                "amount": "公告正文中的累计回购金额尚未形成可计算字段。",
                                "currency": "累计金额缺失，币种不作推断。",
                            }
                        ),
                        **(numeric_facts or {}),
                        "alternative_sources": [
                            {
                                "source_key": candidate.source_key,
                                "source_document_id": candidate.source_document_id,
                                "source_url": candidate.document_url,
                            }
                            for candidate in alternatives[:3]
                        ],
                    },
                )
            )

        if backlog_count:
            self._append_issue(
                issues,
                "a-share-documents",
                "BUYBACK_DOCUMENT_RUN_LIMIT_REACHED",
            )
        partial = bool(failed_count or empty_text_count or backlog_count)
        source_time = max(
            (value.released_at for group in groups for value in group),
            default=None,
        )
        return SourceFetch(
            records=tuple(groups),
            record_count=len(groups),
            source_time=source_time,
            status="PARTIAL" if partial else "SUCCESS" if groups else "EMPTY",
            detail_code=("BUYBACK_DOCUMENTS_INCOMPLETE" if partial else None),
            payload={
                "candidate_count": len(groups),
                "new_document_count": fetched_count,
                "existing_document_count": existing_count,
                "fallback_document_count": fallback_count,
                "failed_document_count": failed_count,
                "empty_text_document_count": empty_text_count,
                "backlog_count": backlog_count,
                "run_document_limit": self.settings.max_documents_per_run,
            },
        )

    def _fetch_hkex_board(
        self,
        board: str,
        *,
        now: datetime,
        source_states: dict[str, StoredBuybackSourceState | BuybackSourceObservation],
    ) -> tuple[
        SourceFetch,
        tuple[BuybackEvidenceDocument, ...],
        tuple[BuybackEntityRevision, ...],
        tuple[CollectionIssue, ...],
    ]:
        config = HKEX_BOARDS[board]
        calendar_url = str(config["calendar_url"])
        end = now.astimezone(SHANGHAI_TZ).date()
        current_begin = end - timedelta(days=self.settings.lookback_days - 1)
        begin = end - timedelta(days=self.settings.hkex_history_days - 1)
        target_dates = tuple(begin + timedelta(days=offset) for offset in range((end - begin).days + 1))
        months = sorted({(value.year, value.month) for value in target_dates})
        links: dict[str, str] = {}
        calendar_hashes: list[str] = []
        for year, month in months:
            body = urlencode({"y": year, "m": month}).encode("ascii")
            response = self.client.request(
                calendar_url,
                method="POST",
                body=body,
                referer=calendar_url,
                max_bytes=512 * 1024,
            )
            if "html" not in response.content_type.casefold():
                raise BuybackSourceError("BUYBACK_HKEX_CALENDAR_CONTENT_TYPE_INVALID")
            links.update(parse_hkex_calendar(response.body, base_url=calendar_url))
            calendar_hashes.append(hashlib.sha256(response.body).hexdigest())

        report_targets: list[tuple[date, str, str]] = []
        prefix = str(config["report_prefix"])
        for release_date in target_dates:
            filename = f"{prefix}{release_date:%Y%m%d}.xls"
            link = links.get(filename.casefold())
            if link is not None:
                report_targets.append((release_date, filename, link))

        current_targets = [
            target for target in report_targets if target[0] >= current_begin
        ]
        history_targets: list[tuple[date, str, str]] = []
        existing_history_count = 0
        source_key = f"hkex-{board}-reports"
        for target in reversed(report_targets):
            release_date, filename, _link = target
            if release_date >= current_begin:
                continue
            source_document_id = filename.rsplit(".", 1)[0]
            if self.store.buyback_document_for_source(
                self.monitor_id,
                source_key,
                source_document_id,
            ) is not None:
                existing_history_count += 1
                continue
            history_targets.append(target)
        selected_history_targets = history_targets[
            : self.settings.max_hkex_backfill_reports_per_run
        ]
        history_backlog_count = len(history_targets) - len(selected_history_targets)
        selected_targets = current_targets + list(reversed(selected_history_targets))

        documents: list[BuybackEvidenceDocument] = []
        revisions: list[BuybackEntityRevision] = []
        issues: list[CollectionIssue] = []
        failed_reports = 0
        failed_current_reports = 0
        cross_market_rows = 0
        unknown_venue_rows = 0
        inconsistent_currency_rows = 0
        execution_rows = 0
        report_hashes: list[str] = []

        for release_date, filename, link in selected_targets:
            scope = f"hkex-{board}:{release_date.isoformat()}"
            try:
                response = self.client.request(
                    link,
                    referer=calendar_url,
                    max_bytes=2 * 1024 * 1024,
                    attempts=2,
                )
                if (
                    "excel" not in response.content_type.casefold()
                    and not response.body.startswith(b"\xd0\xcf\x11\xe0")
                ):
                    raise BuybackSourceError("BUYBACK_HKEX_REPORT_CONTENT_TYPE_INVALID")
                rows, header_hash, printed_date = parse_hkex_report(response.body)
            except BuybackSourceError as exc:
                failed_reports += 1
                if release_date >= current_begin:
                    failed_current_reports += 1
                issues.append(CollectionIssue(scope, exc.reason_code))
                continue
            digest = hashlib.sha256(response.body).hexdigest()
            report_hashes.append(digest)
            release_at = datetime.combine(
                release_date,
                datetime.min.time(),
                SHANGHAI_TZ,
            ).astimezone(UTC)
            documents.append(
                BuybackEvidenceDocument(
                    source_key=f"hkex-{board}-reports",
                    source_label=str(config["label"]),
                    source_document_id=filename.rsplit(".", 1)[0],
                    source_url=link,
                    published_at=release_at,
                    observed_at=response.completed_at,
                    media_type="application/vnd.ms-excel",
                    file_suffix=".xls",
                    body=response.body,
                    quality_state="VALID_HKEX_XLS",
                    metadata={
                        "board": board,
                        "release_date": release_date.isoformat(),
                        "printed_date": printed_date,
                        "header_sha256": header_hash,
                        "row_count": len(rows),
                    },
                )
            )
            for row in rows:
                if row.execution_venue != "HKEX":
                    if row.execution_venue == "OTHER_OR_UNKNOWN":
                        unknown_venue_rows += 1
                    else:
                        cross_market_rows += 1
                    continue
                execution_rows += 1
                if not row.currency_consistent:
                    inconsistent_currency_rows += 1
                eligibility = self._connect_eligibility(
                    row.stock_code,
                    source_states=source_states,
                )
                trading_date = date.fromisoformat(row.trading_date)
                effective_at = datetime.combine(
                    trading_date,
                    datetime.min.time(),
                    SHANGHAI_TZ,
                ).astimezone(UTC)
                identity_payload = {
                    "board": board,
                    "history_window_days": self.settings.hkex_history_days,
                    "report": release_date.isoformat(),
                    "code": row.stock_code,
                    "share_class": row.share_class,
                    "trading_date": row.trading_date,
                    "shares": row.shares,
                    "amount": row.amount,
                    "currency": row.currency,
                }
                row_identity = _canonical_sha256(identity_payload)[:24]
                entity_key = f"HK:{row.stock_code}:{row.trading_date}:{row_identity}"
                amount = row.amount if row.currency_consistent else None
                currency = row.currency if row.currency_consistent else None
                missing_reasons: dict[str, str] = {}
                if amount is None:
                    missing_reasons["amount"] = (
                        "港交所日报金额缺失、格式异常或币种不一致，未使用替代值。"
                    )
                if currency is None:
                    missing_reasons["currency"] = (
                        "港交所日报币种缺失或不一致，未使用替代值。"
                    )
                no_action_reason = (
                    "PROGRAM_LINK_UNCONFIRMED"
                    if eligibility["connect_status"] == "BUY_ELIGIBLE"
                    else "CONNECT_ELIGIBILITY_NOT_CURRENTLY_BUYABLE"
                )
                revisions.append(
                    BuybackEntityRevision(
                        entity_key=entity_key,
                        entity_type="HKEX_EXECUTION",
                        effective_at=effective_at,
                        observed_at=now,
                        source_key=f"hkex-{board}-reports",
                        document_sha256=digest,
                        payload={
                            "entity_key": entity_key,
                            "market_scope": "HK",
                            "market": "HK",
                            "market_label": "港股",
                            "stock_code": row.stock_code,
                            "issuer_name": row.company,
                            "share_class": row.share_class,
                            "event_type": "HKEX_EXECUTION",
                            "event_type_label": EVENT_TYPE_LABELS["HKEX_EXECUTION"],
                            "effective_at": iso_utc(effective_at),
                            "trading_date": row.trading_date,
                            "official_release_at": iso_utc(release_at),
                            "source_time_precision": "DATE",
                            "earliest_actionable_rule": (
                                "NEXT_TRADING_SESSION_AFTER_REPORT_DATE"
                            ),
                            "source_key": f"hkex-{board}-reports",
                            "source_label": str(config["label"]),
                            "source_document_id": filename.rsplit(".", 1)[0],
                            "source_url": link,
                            "document_url": link,
                            "document_source_label": str(config["label"]),
                            "document_sha256": digest,
                            "document_quality": "VALID_HKEX_XLS",
                            "shares": row.shares,
                            "high_price": row.high_price,
                            "low_price": row.low_price,
                            "amount": amount,
                            "currency": currency,
                            "total_repurchased_shares": row.total_repurchased_shares,
                            "cancellation_shares": row.cancellation_shares,
                            "treasury_shares": row.treasury_shares,
                            "mandate_exchange_shares": row.mandate_exchange_shares,
                            "mandate_percentage": row.mandate_percentage,
                            "execution_venue": row.execution_venue,
                            "candidate_status": "EXECUTION_UNLINKED",
                            "program_link_status": "UNLINKED_EXECUTION",
                            "program_status": "UNKNOWN",
                            "review_status": "UNREVIEWED",
                            "review_status_label": "待复核",
                            **eligibility,
                            "data_quality_label": "官方日报 · 待程序关联",
                            "no_action_reason": no_action_reason,
                            "detail_action": "查看 / 复核",
                            "row_tone": (
                                "NOTICE"
                                if eligibility["connect_status"] == "BUY_ELIGIBLE"
                                else "WARNING"
                            ),
                            "missing_reasons": missing_reasons,
                        },
                    )
                )

        if inconsistent_currency_rows:
            issues.append(
                CollectionIssue(
                    f"hkex-{board}-reports",
                    "BUYBACK_HKEX_CURRENCY_INCONSISTENT",
                )
            )
        partial = bool(failed_current_reports or inconsistent_currency_rows)
        source_time = (
            datetime.combine(
                max((value[0] for value in report_targets)),
                datetime.min.time(),
                SHANGHAI_TZ,
            ).astimezone(UTC)
            if report_targets
            else None
        )
        return (
            SourceFetch(
                records=tuple(revisions),
                record_count=execution_rows,
                source_time=source_time,
                status=(
                    "PARTIAL"
                    if partial
                    else "SUCCESS"
                    if report_targets
                    else "EMPTY"
                ),
                detail_code=("BUYBACK_HKEX_REPORTS_INCOMPLETE" if partial else None),
                payload={
                    "board": board,
                    "window_start": begin.isoformat(),
                    "window_end": end.isoformat(),
                    "current_window_start": current_begin.isoformat(),
                    "report_count": len(selected_targets),
                    "available_report_count": len(report_targets),
                    "current_report_count": len(current_targets),
                    "history_download_count": len(selected_history_targets),
                    "existing_history_count": existing_history_count,
                    "history_backlog_count": history_backlog_count,
                    "failed_report_count": failed_reports,
                    "failed_current_report_count": failed_current_reports,
                    "hkex_execution_row_count": execution_rows,
                    "cross_market_row_count": cross_market_rows,
                    "unknown_venue_row_count": unknown_venue_rows,
                    "currency_inconsistent_row_count": inconsistent_currency_rows,
                    "calendar_responses_sha256": _canonical_sha256(calendar_hashes),
                    "report_documents_sha256": _canonical_sha256(report_hashes),
                },
            ),
            tuple(documents),
            tuple(revisions),
            tuple(issues),
        )

    @staticmethod
    def _connect_eligibility(
        stock_code: str,
        *,
        source_states: dict[str, StoredBuybackSourceState | BuybackSourceObservation],
    ) -> dict[str, str]:
        current_routes: list[str] = []
        stale_routes: list[str] = []
        current_source_count = 0
        for source_key, route in (("connect-sh", "SH"), ("connect-sz", "SZ")):
            state = source_states.get(source_key)
            if state is None:
                continue
            codes_value = state.payload.get("codes")
            codes = {
                str(value).zfill(5)
                for value in codes_value
                if isinstance(value, (str, int))
            } if isinstance(codes_value, list) else set()
            if state.status == "SUCCESS":
                current_source_count += 1
                if stock_code in codes:
                    current_routes.append(route)
            elif codes and stock_code in codes:
                stale_routes.append(route)

        if current_routes:
            route_label = "+".join(current_routes)
            return {
                "connect_status": "BUY_ELIGIBLE",
                "connect_status_label": "可购买",
                "connect_route_label": route_label,
                "connect_quality": "CURRENT",
            }
        if current_source_count == 2:
            return {
                "connect_status": "NOT_BUY_ELIGIBLE",
                "connect_status_label": "不可购买",
                "connect_route_label": "—",
                "connect_quality": "CURRENT",
            }
        return {
            "connect_status": "UNKNOWN",
            "connect_status_label": "名单未完整取得",
            "connect_route_label": (
                f"历史：{'+'.join(stale_routes)}" if stale_routes else "—"
            ),
            "connect_quality": "STALE_OR_PARTIAL",
        }


__all__ = [
    "AHKBuybackMonitor",
    "AnnouncementRecord",
    "BuybackPublicClient",
    "BuybackSettings",
    "BuybackSourceError",
    "HkexExecution",
    "classify_buyback_title",
    "is_target_a_share_security",
    "parse_cninfo_announcement_payload",
    "parse_hkex_calendar",
    "parse_hkex_report",
    "parse_jsonp",
    "parse_sse_announcement_payload",
    "parse_sse_connect",
    "parse_szse_connect_page",
    "validate_pdf",
]
