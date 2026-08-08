"""A-share company event calendar from bounded public market endpoints.

The monitor deliberately separates a persistent user watchlist from a frozen
daily discovery universe.  Discovery controls coverage only; it is never
presented as a recommendation or directional signal.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time as datetime_time, timedelta
import hashlib
import json
import re
import threading
from typing import Any, Protocol
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import exchange_calendars as xcals

from halpha_monitor.contracts import (
    CollectionBatch,
    CollectionIssue,
    ConfigurationField,
    FilterChoice,
    MonitorView,
    ProjectionSnapshot,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.monitors.a_hk_buyback import (
    BuybackPublicClient,
    BuybackSourceError,
    PublicResponse,
)
from halpha_monitor.store import SQLiteMonitorStore, iso_utc, utc_now


MONITOR_ID = "stock-event-calendar"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DATA_CENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
ANNOUNCEMENT_API_URL = "https://np-anotice-stock.eastmoney.com/api/security/ann"
STOCK_CALENDAR_PAGE_URL = "https://data.eastmoney.com/Stockcalendar/"
ANNOUNCEMENT_PAGE_URL = "https://data.eastmoney.com/notices/"
POOL_PAGE_URL = "https://quote.eastmoney.com/ztb/"
POOL_API_ROOT = "https://push2ex.eastmoney.com"
POOL_TOKEN = "7eea3edcaed734bea9cbfc24409ed989"
STOCK_CODE_PATTERN = re.compile(r"^\d{6}$")
MAX_MANUAL_STOCKS = 50
MAX_AUTO_STOCKS = 100
STOCK_DIRECTORY_SNAPSHOT_KEY = "stock-directory"
STOCK_DIRECTORY_REFRESH_INTERVAL = timedelta(hours=24)
STOCK_DIRECTORY_PAGE_SIZE = 500
MAX_STOCK_DIRECTORY_PAGES = 20
MAX_STOCK_DIRECTORY_ROWS = 7000
ANNOUNCEMENT_PAGE_SIZE = 100
AUTO_ANNOUNCEMENT_CHUNK_SIZE = 20
AUTO_ANNOUNCEMENT_PAGE_LIMIT = 2
ACTIVE_A_SHARE_MARKET_CODES = frozenset(
    {
        "069001001001",  # Shanghai main board
        "069001001006",  # STAR Market
        "069001002001",  # Shenzhen main board
        "069001002002",  # ChiNext
        "069001017",  # Beijing Stock Exchange
    }
)

POOL_DEFINITIONS = (
    ("limit-up", "涨停或连板", "getTopicZTPool", "fbt:asc"),
    ("strong", "近期强势", "getTopicQSPool", "zdp:desc"),
    ("new-high", "近期新高", "getTopicCXPool", "zdp:desc"),
)

CALENDAR_EVENT_TYPES = {
    "001": ("SHAREHOLDER", "股东事项"),
    "003": ("CAPITAL", "股本与解禁"),
    "004": ("DIVIDEND", "分红"),
    "005": ("RESEARCH", "机构调研"),
    "006": ("EARNINGS", "业绩"),
    "007": ("EARNINGS", "业绩"),
    "008": ("CORPORATE", "经营与重大事项"),
    "009": ("SHAREHOLDER", "股东事项"),
    "010": ("EARNINGS", "业绩"),
    "011": ("SHAREHOLDER", "股东事项"),
    "013": ("CORPORATE", "经营与重大事项"),
    "014": ("CORPORATE", "经营与重大事项"),
    "015": ("CORPORATE", "经营与重大事项"),
    "016": ("SHAREHOLDER", "股东事项"),
    "017": ("CAPITAL", "股本与解禁"),
    "018": ("CORPORATE", "经营与重大事项"),
    "021": ("SHAREHOLDER", "股东事项"),
    "023": ("RISK", "风险与停复牌"),
    "024": ("CAPITAL", "股本与解禁"),
    "025": ("EARNINGS", "业绩"),
    "026": ("SHAREHOLDER", "股东事项"),
}

MATERIAL_ANNOUNCEMENT_KEYWORDS = (
    "业绩",
    "利润",
    "分红",
    "股东大会",
    "增持",
    "减持",
    "回购",
    "重大",
    "重组",
    "收购",
    "合同",
    "中标",
    "诉讼",
    "仲裁",
    "停牌",
    "复牌",
    "风险",
    "立案",
    "处罚",
    "退市",
    "解禁",
    "实际控制人",
    "控股股东",
    "股权激励",
    "发行",
    "募集资金",
    "变更",
)


class PublicStockEventClient(Protocol):
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


class StockEventSourceError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True)
class StockEventSettings:
    interval_seconds: float = 3600
    jitter_seconds: float = 300
    history_days: int = 30
    lookahead_days: int = 60
    auto_limit: int = 80
    manual_stock_codes: tuple[str, ...] = ()
    timeout_seconds: float = 10
    proxy_url: str | None = None
    max_calendar_pages_per_chunk: int = 4
    max_announcement_pages_per_chunk: int = 3
    max_events: int = 1800

    def __post_init__(self) -> None:
        if self.interval_seconds < 900:
            raise ValueError("STOCK_EVENTS_INTERVAL_TOO_SHORT")
        if self.jitter_seconds < 0 or self.jitter_seconds > self.interval_seconds:
            raise ValueError("STOCK_EVENTS_JITTER_INVALID")
        if not 1 <= self.history_days <= 31:
            raise ValueError("STOCK_EVENTS_HISTORY_INVALID")
        if not 1 <= self.lookahead_days <= 120:
            raise ValueError("STOCK_EVENTS_LOOKAHEAD_INVALID")
        if not 1 <= self.auto_limit <= MAX_AUTO_STOCKS:
            raise ValueError("STOCK_EVENTS_AUTO_LIMIT_INVALID")
        if len(self.manual_stock_codes) > MAX_MANUAL_STOCKS:
            raise ValueError("STOCK_EVENTS_WATCHLIST_TOO_LARGE")
        if any(not STOCK_CODE_PATTERN.fullmatch(code) for code in self.manual_stock_codes):
            raise ValueError("STOCK_EVENTS_STOCK_CODE_INVALID")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 60:
            raise ValueError("STOCK_EVENTS_TIMEOUT_INVALID")
        if not 1 <= self.max_calendar_pages_per_chunk <= 10:
            raise ValueError("STOCK_EVENTS_CALENDAR_PAGE_LIMIT_INVALID")
        if not 1 <= self.max_announcement_pages_per_chunk <= 10:
            raise ValueError("STOCK_EVENTS_ANNOUNCEMENT_PAGE_LIMIT_INVALID")
        if not 100 <= self.max_events <= 3000:
            raise ValueError("STOCK_EVENTS_EVENT_LIMIT_INVALID")


@dataclass(frozen=True)
class _AnnouncementChunkResult:
    records: tuple[dict[str, Any], ...]
    checked: tuple[datetime, ...]
    truncated: bool
    total_hits: int
    pages_read: int
    failure_reason: str | None = None
    failed_page: int | None = None


def _chunks(values: Iterable[str], size: int) -> tuple[tuple[str, ...], ...]:
    items = tuple(values)
    return tuple(items[index : index + size] for index in range(0, len(items), size))


def _source_reason(prefix: str, error: Exception) -> str:
    if isinstance(error, StockEventSourceError):
        return error.reason_code
    if isinstance(error, BuybackSourceError):
        return f"STOCK_EVENTS_{prefix}_{error.reason_code.removeprefix('BUYBACK_')}"
    return f"STOCK_EVENTS_{prefix}_UNAVAILABLE"


def _source_date(value: Any, reason_code: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        raise StockEventSourceError(reason_code) from None


def _source_datetime(value: Any, fallback_date: date) -> tuple[datetime, str]:
    text = str(value or "")
    try:
        parsed = datetime.fromisoformat(text[:19])
    except ValueError:
        parsed = datetime.combine(fallback_date, datetime_time(hour=12))
        precision = "DATE"
    else:
        precision = "EXACT"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(UTC), precision


def _event_datetime(event_date: date) -> datetime:
    return datetime.combine(event_date, datetime_time.min, tzinfo=SHANGHAI_TZ).astimezone(
        UTC
    )


def _optional_utc(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _market_label(secu_code: str) -> str | None:
    if secu_code.endswith(".SH"):
        return "上海证券交易所"
    if secu_code.endswith(".SZ"):
        return "深圳证券交易所"
    if secu_code.endswith(".BJ"):
        return "北京证券交易所"
    return None


def _latest_xshg_session(day: date) -> date:
    calendar = xcals.get_calendar("XSHG")
    return calendar.date_to_session(day, direction="previous").date()


def _clean_text(value: Any, *, limit: int) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _valid_pool_stock(code: str, name: str) -> bool:
    upper_name = name.upper()
    return bool(STOCK_CODE_PATTERN.fullmatch(code)) and "ST" not in upper_name and "退" not in name


def _announcement_category(title: str) -> tuple[str, str]:
    if any(token in title for token in ("业绩", "利润", "年报", "季报", "半年报")):
        return "EARNINGS", "业绩"
    if any(token in title for token in ("分红", "派息", "权益分派")):
        return "DIVIDEND", "分红"
    if any(token in title for token in ("股东", "增持", "减持", "实际控制人")):
        return "SHAREHOLDER", "股东事项"
    if any(token in title for token in ("股本", "解禁", "发行", "股权激励")):
        return "CAPITAL", "股本与解禁"
    if any(token in title for token in ("风险", "停牌", "复牌", "诉讼", "立案", "处罚", "退市")):
        return "RISK", "风险与停复牌"
    return "DISCLOSURE", "公司公告"


def _importance(event_type_code: str, text: str) -> str:
    if event_type_code in {"006", "007", "010", "013", "015", "017", "021", "023", "025"}:
        return "HIGH"
    if any(
        token in text
        for token in (
            "重大",
            "业绩",
            "利润",
            "停牌",
            "退市",
            "立案",
            "处罚",
            "重组",
            "股东大会",
        )
    ):
        return "HIGH"
    return "MEDIUM"


def _event_state(event_date: date, today: date) -> tuple[str, str]:
    if event_date > today:
        return "UPCOMING", "即将发生"
    if event_date == today:
        return "TODAY", "今日"
    return "OCCURRED", "已发生"


class StockEventMonitor:
    monitor_id = MONITOR_ID
    display_name = "个股事件日历"
    description = (
        "按手动关注与每日动态强势股池，集中查看 A 股近一个月公告、"
        "公司事件及已明确日期的未来事项。"
    )
    projection_kind = "stock_events"
    default_enabled = True

    def __init__(
        self,
        settings: StockEventSettings | None = None,
        *,
        store: SQLiteMonitorStore,
        client: PublicStockEventClient | None = None,
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings or StockEventSettings()
        self.interval_seconds = self.settings.interval_seconds
        self.jitter_seconds = self.settings.jitter_seconds
        self.store = store
        self.client = client or BuybackPublicClient(
            timeout_seconds=self.settings.timeout_seconds,
            proxy_url=self.settings.proxy_url,
        )
        self._now = now
        self._settings_lock = threading.Lock()
        self.configuration_fields = (
            ConfigurationField(
                key="manual_stock_codes",
                label="手动关注股票",
                kind="stock_list",
                description="通过中文名称或代码搜索沪深京 A 股；手动关注长期保留，可随时移除。",
                placeholder="例如 贵州茅台 / 600519",
                maximum_items=MAX_MANUAL_STOCKS,
            ),
        )
        self.view = MonitorView(
            filters=(
                ViewFilter(
                    key="stock_origin",
                    label="股票范围",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部"),
                        FilterChoice("MANUAL", "手动关注"),
                        FilterChoice("AUTO", "每日入选"),
                    ),
                ),
                ViewFilter(
                    key="category",
                    label="事件类型",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部"),
                        FilterChoice("EARNINGS", "业绩"),
                        FilterChoice("DIVIDEND", "分红"),
                        FilterChoice("SHAREHOLDER", "股东事项"),
                        FilterChoice("CAPITAL", "股本与解禁"),
                        FilterChoice("CORPORATE", "经营与重大事项"),
                        FilterChoice("DISCLOSURE", "公司公告"),
                        FilterChoice("RISK", "风险与停复牌"),
                        FilterChoice("RESEARCH", "机构调研"),
                    ),
                ),
                ViewFilter(
                    key="importance",
                    label="关注级别",
                    default="*",
                    choices=(
                        FilterChoice("*", "全部"),
                        FilterChoice("HIGH", "重点"),
                        FilterChoice("MEDIUM", "一般"),
                    ),
                ),
            ),
            columns=(
                ViewColumn("event_date", "日期", kind="time"),
                ViewColumn("stock_name", "股票"),
                ViewColumn("title", "事件"),
                ViewColumn("category_label", "类型"),
                ViewColumn("importance", "关注级别"),
                ViewColumn("state_label", "状态"),
                ViewColumn("source_label", "来源", priority="secondary"),
            ),
            chart_title="公司事件月历",
            table_title="个股事件",
            method_note=(
                "每日入选基于公开强势、涨停/连板与近期新高股池，仅决定监控范围，"
                "不构成股票推荐或涨跌判断。"
            ),
            show_description=True,
        )

    def configuration(self) -> dict[str, Any]:
        with self._settings_lock:
            codes = self.settings.manual_stock_codes
        return {"manual_stock_codes": list(codes)}

    def normalize_configuration(self, values: dict[str, Any]) -> dict[str, Any]:
        if set(values) != {"manual_stock_codes"}:
            raise ValueError("STOCK_EVENTS_CONFIGURATION_FIELDS_INVALID")
        raw = values["manual_stock_codes"]
        if isinstance(raw, str):
            tokens = re.split(r"[,，;；\s]+", raw.strip()) if raw.strip() else []
        elif isinstance(raw, (list, tuple)):
            tokens = [str(item).strip() for item in raw]
        else:
            raise ValueError("STOCK_EVENTS_WATCHLIST_INVALID")
        codes = tuple(dict.fromkeys(token for token in tokens if token))
        if len(codes) > MAX_MANUAL_STOCKS:
            raise ValueError("STOCK_EVENTS_WATCHLIST_TOO_LARGE")
        if any(not STOCK_CODE_PATTERN.fullmatch(code) for code in codes):
            raise ValueError("STOCK_EVENTS_STOCK_CODE_INVALID")
        return {"manual_stock_codes": list(codes)}

    def apply_configuration(self, values: dict[str, Any]) -> None:
        normalized = self.normalize_configuration(values)
        with self._settings_lock:
            self.settings = replace(
                self.settings,
                manual_stock_codes=tuple(normalized["manual_stock_codes"]),
            )

    def network_request_count(self, *, window_seconds: float = 60) -> int | None:
        counter = getattr(self.client, "network_request_count", None)
        if not callable(counter):
            return None
        return int(counter(window_seconds=window_seconds))

    def bind_stop_event(self, stop_event: threading.Event) -> None:
        binder = getattr(self.client, "bind_stop_event", None)
        if callable(binder):
            binder(stop_event)

    def _json_request(
        self,
        url: str,
        *,
        referer: str,
        max_bytes: int = 4 * 1024 * 1024,
    ) -> tuple[dict[str, Any], PublicResponse]:
        response = self.client.request(
            url,
            referer=referer,
            max_bytes=max_bytes,
            attempts=2,
        )
        try:
            payload = json.loads(response.body.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise StockEventSourceError("STOCK_EVENTS_SOURCE_JSON_INVALID") from None
        if not isinstance(payload, dict):
            raise StockEventSourceError("STOCK_EVENTS_SOURCE_SCHEMA_CHANGED")
        return payload, response

    def _fetch_pool(
        self,
        endpoint: str,
        sort: str,
        trade_date: date,
    ) -> tuple[list[dict[str, Any]], PublicResponse]:
        query = urlencode(
            {
                "ut": POOL_TOKEN,
                "dpt": "wz.ztzt",
                "Pageindex": 0,
                "pagesize": MAX_AUTO_STOCKS,
                "sort": sort,
                "date": trade_date.strftime("%Y%m%d"),
            }
        )
        payload, response = self._json_request(
            f"{POOL_API_ROOT}/{endpoint}?{query}",
            referer=POOL_PAGE_URL,
            max_bytes=2 * 1024 * 1024,
        )
        data = payload.get("data")
        if payload.get("rc") != 0 or not isinstance(data, dict):
            raise StockEventSourceError("STOCK_EVENTS_POOL_SCHEMA_CHANGED")
        pool = data.get("pool")
        if not isinstance(pool, list):
            raise StockEventSourceError("STOCK_EVENTS_POOL_SCHEMA_CHANGED")
        return [item for item in pool if isinstance(item, dict)], response

    def _daily_universe(
        self,
        trade_date: date,
        previous: dict[str, Any] | None,
        settings: StockEventSettings,
        issues: list[CollectionIssue],
        source_states: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], bool]:
        if previous and previous.get("selection_trade_date") == trade_date.isoformat():
            reused: dict[str, dict[str, Any]] = {}
            for security in previous.get("securities", []):
                if not isinstance(security, dict) or not security.get("is_auto"):
                    continue
                code = str(security.get("code") or "")
                if not STOCK_CODE_PATTERN.fullmatch(code):
                    continue
                reused[code] = {
                    "code": code,
                    "name": security.get("name"),
                    "industry": security.get("industry"),
                    "reasons": list(security.get("auto_reasons") or []),
                    "auto_rank": int(security.get("auto_rank") or len(reused) + 1),
                }
            source_states.append(
                {
                    "key": "daily-universe",
                    "label": "每日动态股池",
                    "status": "SUCCESS" if reused else "EMPTY",
                    "checked_at": previous.get("selection_checked_at"),
                    "record_count": len(reused),
                    "detail": "沿用本交易日已冻结名单",
                    "source_url": POOL_PAGE_URL,
                }
            )
            return reused, True

        pools: list[tuple[str, str, list[dict[str, Any]]]] = []
        checked_times: list[datetime] = []
        failed_reasons: list[str] = []
        total_records = 0
        for key, label, endpoint, sort in POOL_DEFINITIONS:
            try:
                records, response = self._fetch_pool(endpoint, sort, trade_date)
            except Exception as exc:
                reason = _source_reason("POOL", exc)
                failed_reasons.append(reason)
                issues.append(CollectionIssue(scope=f"auto-{key}", reason_code=reason))
                continue
            pools.append((key, label, records))
            checked_times.append(response.completed_at.astimezone(UTC))
            total_records += len(records)

        metadata: dict[str, dict[str, Any]] = {}
        ordered_codes: list[list[str]] = []
        for _, label, records in pools:
            pool_codes: list[str] = []
            for item in records:
                code = _clean_text(item.get("c"), limit=6)
                name = _clean_text(item.get("n"), limit=32)
                if not _valid_pool_stock(code, name):
                    continue
                candidate = metadata.setdefault(
                    code,
                    {
                        "code": code,
                        "name": name or None,
                        "industry": _clean_text(item.get("hybk"), limit=48) or None,
                        "reasons": [],
                    },
                )
                if label not in candidate["reasons"]:
                    candidate["reasons"].append(label)
                pool_codes.append(code)
            ordered_codes.append(list(dict.fromkeys(pool_codes)))

        selected: dict[str, dict[str, Any]] = {}
        rank = 1
        for row_index in range(MAX_AUTO_STOCKS):
            for pool_codes in ordered_codes:
                if row_index >= len(pool_codes):
                    continue
                code = pool_codes[row_index]
                if code in selected:
                    continue
                selected[code] = {**metadata[code], "auto_rank": rank}
                rank += 1
                if len(selected) >= settings.auto_limit:
                    break
            if len(selected) >= settings.auto_limit:
                break

        status = "ERROR" if not pools else "PARTIAL" if failed_reasons else "SUCCESS"
        if status == "SUCCESS" and not selected:
            status = "EMPTY"
        source_states.append(
            {
                "key": "daily-universe",
                "label": "每日动态股池",
                "status": status,
                "checked_at": iso_utc(max(checked_times)) if checked_times else None,
                "record_count": len(selected),
                "detail": (
                    f"{trade_date.isoformat()} 强势、涨停/连板与近期新高公开股池"
                    + (f"；{len(failed_reasons)} 个子来源失败" if failed_reasons else "")
                ),
                "source_url": POOL_PAGE_URL,
                "candidate_count": total_records,
            }
        )
        return selected, False

    def _fetch_stock_directory(self) -> tuple[list[dict[str, Any]], datetime]:
        resolved: dict[str, dict[str, Any]] = {}
        checked: list[datetime] = []
        active_markets = ",".join(
            f'"{market_code}"' for market_code in sorted(ACTIVE_A_SHARE_MARKET_CODES)
        )
        expected_pages: int | None = None
        for page in range(1, MAX_STOCK_DIRECTORY_PAGES + 1):
            query = urlencode(
                {
                    "reportName": "RPT_STOCK_HEADERCHANGE",
                    "columns": (
                        "SECURITY_CODE,SECURITY_NAME_ABBR,SECUCODE,"
                        "TRADE_MARKET_CODE,INDUSTRY_NAME"
                    ),
                    "filter": f"(TRADE_MARKET_CODE in ({active_markets}))",
                    "pageNumber": page,
                    "pageSize": STOCK_DIRECTORY_PAGE_SIZE,
                    "sortTypes": 1,
                    "sortColumns": "SECURITY_CODE",
                    "source": "WEB",
                    "client": "WEB",
                }
            )
            payload, response = self._json_request(
                f"{DATA_CENTER_URL}?{query}", referer=STOCK_CALENDAR_PAGE_URL
            )
            result = payload.get("result")
            rows = result.get("data") if isinstance(result, dict) else None
            if payload.get("success") is not True or not isinstance(rows, list):
                raise StockEventSourceError("STOCK_EVENTS_DIRECTORY_SCHEMA_CHANGED")
            try:
                pages = int(result.get("pages") or 0)
                count = int(result.get("count") or 0)
            except (TypeError, ValueError):
                raise StockEventSourceError(
                    "STOCK_EVENTS_DIRECTORY_SCHEMA_CHANGED"
                ) from None
            if (
                pages < 1
                or pages > MAX_STOCK_DIRECTORY_PAGES
                or count < 1
                or count > MAX_STOCK_DIRECTORY_ROWS
            ):
                raise StockEventSourceError("STOCK_EVENTS_DIRECTORY_BOUNDS_CHANGED")
            if expected_pages is None:
                expected_pages = pages
            elif pages != expected_pages:
                raise StockEventSourceError("STOCK_EVENTS_DIRECTORY_PAGING_CHANGED")
            checked.append(response.completed_at.astimezone(UTC))
            for row in rows:
                if not isinstance(row, dict):
                    continue
                market_code = _clean_text(row.get("TRADE_MARKET_CODE"), limit=24)
                code = _clean_text(row.get("SECURITY_CODE"), limit=6)
                name = _clean_text(row.get("SECURITY_NAME_ABBR"), limit=32)
                secu_code = _clean_text(row.get("SECUCODE"), limit=24)
                market_label = _market_label(secu_code)
                if (
                    market_code not in ACTIVE_A_SHARE_MARKET_CODES
                    or not STOCK_CODE_PATTERN.fullmatch(code)
                    or not name
                    or market_label is None
                ):
                    continue
                resolved[code] = {
                    "code": code,
                    "name": name,
                    "secu_code": secu_code,
                    "market_label": market_label,
                    "industry": _clean_text(row.get("INDUSTRY_NAME"), limit=48) or None,
                }
            if page >= pages:
                break
        if not resolved or expected_pages is None or len(checked) != expected_pages:
            raise StockEventSourceError("STOCK_EVENTS_DIRECTORY_INCOMPLETE")
        return [resolved[code] for code in sorted(resolved)], max(checked)

    @staticmethod
    def _cached_directory_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
        raw = payload.get("securities")
        if not isinstance(raw, list) or len(raw) > MAX_STOCK_DIRECTORY_ROWS:
            return []
        entries: dict[str, dict[str, Any]] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            code = _clean_text(item.get("code"), limit=6)
            name = _clean_text(item.get("name"), limit=32)
            secu_code = _clean_text(item.get("secu_code"), limit=24)
            market_label = _market_label(secu_code)
            if not STOCK_CODE_PATTERN.fullmatch(code) or not name or market_label is None:
                continue
            entries[code] = {
                "code": code,
                "name": name,
                "secu_code": secu_code,
                "market_label": market_label,
                "industry": _clean_text(item.get("industry"), limit=48) or None,
            }
        return [entries[code] for code in sorted(entries)]

    def _stock_directory(
        self,
        now: datetime,
        issues: list[CollectionIssue],
        source_states: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], ProjectionSnapshot | None]:
        stored = self.store.projection_snapshot(
            self.monitor_id, STOCK_DIRECTORY_SNAPSHOT_KEY
        )
        previous = stored.payload if stored is not None else {}
        entries = self._cached_directory_entries(previous)
        last_attempt = _optional_utc(previous.get("last_attempt_at"))
        attempt_is_current = bool(
            last_attempt is not None
            and last_attempt <= now + timedelta(minutes=2)
            and now < last_attempt + STOCK_DIRECTORY_REFRESH_INTERVAL
        )
        snapshot: ProjectionSnapshot | None = None

        if not attempt_is_current:
            try:
                entries, checked_at = self._fetch_stock_directory()
            except Exception as exc:
                reason = _source_reason("DIRECTORY", exc)
                issues.append(
                    CollectionIssue(scope="stock-directory", reason_code=reason)
                )
                status = "STALE" if entries else "ERROR"
                detail = (
                    "股票目录更新失败，继续使用上次本地缓存"
                    if entries
                    else "股票目录首次更新失败；仍可直接输入6位代码"
                )
                source_checked_at = previous.get("source_checked_at")
                cutoff_at = _optional_utc(source_checked_at) or now
            else:
                status = "SUCCESS"
                detail = "股票名称与代码目录已更新；24小时内不重复读取"
                source_checked_at = iso_utc(checked_at)
                cutoff_at = checked_at
            cache_payload = {
                "schema_version": 1,
                "status": status,
                "last_attempt_at": iso_utc(now),
                "source_checked_at": source_checked_at,
                "record_count": len(entries),
                "securities": entries,
                "detail": detail,
                "source_url": STOCK_CALENDAR_PAGE_URL,
            }
            snapshot = ProjectionSnapshot(
                snapshot_key=STOCK_DIRECTORY_SNAPSHOT_KEY,
                observed_at=now,
                cutoff_at=cutoff_at,
                payload=cache_payload,
            )
            previous = cache_payload
            last_attempt = now

        status = str(previous.get("status") or ("SUCCESS" if entries else "ERROR"))
        if status == "SUCCESS" and not entries:
            status = "ERROR"
        source_states.append(
            {
                "key": "stock-directory",
                "label": "A 股股票名称与代码目录",
                "status": status,
                "checked_at": previous.get("source_checked_at"),
                "record_count": len(entries),
                "detail": previous.get("detail") or "本地股票目录在24小时更新周期内",
                "source_url": STOCK_CALENDAR_PAGE_URL,
                "next_update_at": (
                    iso_utc(last_attempt + STOCK_DIRECTORY_REFRESH_INTERVAL)
                    if last_attempt is not None
                    else None
                ),
            }
        )
        return {str(item["code"]): item for item in entries}, snapshot

    def _calendar_chunk(
        self,
        codes: tuple[str, ...],
        start_date: date,
        end_date: date,
        settings: StockEventSettings,
    ) -> tuple[list[dict[str, Any]], list[datetime], bool]:
        records: list[dict[str, Any]] = []
        checked: list[datetime] = []
        truncated = False
        quoted = ",".join(f'"{code}"' for code in codes)
        for page in range(1, settings.max_calendar_pages_per_chunk + 1):
            query = urlencode(
                {
                    "reportName": "RPT_STOCKCALENDAR",
                    "columns": "ALL",
                    "filter": (
                        f"(SECURITY_CODE in ({quoted}))"
                        f"(NOTICE_DATE>='{start_date.isoformat()}')"
                        f"(NOTICE_DATE<='{end_date.isoformat()}')"
                    ),
                    "pageNumber": page,
                    "pageSize": 500,
                    "sortTypes": -1,
                    "sortColumns": "NOTICE_DATE",
                    "source": "WEB",
                    "client": "WEB",
                }
            )
            payload, response = self._json_request(
                f"{DATA_CENTER_URL}?{query}", referer=STOCK_CALENDAR_PAGE_URL
            )
            result = payload.get("result")
            rows = result.get("data") if isinstance(result, dict) else None
            if payload.get("success") is not True or not isinstance(rows, list):
                raise StockEventSourceError("STOCK_EVENTS_CALENDAR_SCHEMA_CHANGED")
            checked.append(response.completed_at.astimezone(UTC))
            records.extend(row for row in rows if isinstance(row, dict))
            pages = int(result.get("pages") or 1)
            if page >= pages:
                break
            if page == settings.max_calendar_pages_per_chunk:
                truncated = True
        return records, checked, truncated

    def _announcement_chunk(
        self,
        codes: tuple[str, ...],
        start_date: date,
        end_date: date,
        page_limit: int,
    ) -> _AnnouncementChunkResult:
        records: list[dict[str, Any]] = []
        checked: list[datetime] = []
        total_hits = 0
        for page in range(1, page_limit + 1):
            try:
                query = urlencode(
                    {
                        "sr": -1,
                        "page_size": ANNOUNCEMENT_PAGE_SIZE,
                        "page_index": page,
                        "ann_type": "A",
                        "client_source": "web",
                        "stock_list": ",".join(codes),
                        "begin_time": start_date.isoformat(),
                        "end_time": end_date.isoformat(),
                    }
                )
                payload, response = self._json_request(
                    f"{ANNOUNCEMENT_API_URL}?{query}", referer=ANNOUNCEMENT_PAGE_URL
                )
                data = payload.get("data")
                rows = data.get("list") if isinstance(data, dict) else None
                if payload.get("success") not in {1, True} or not isinstance(rows, list):
                    raise StockEventSourceError(
                        "STOCK_EVENTS_ANNOUNCEMENT_SCHEMA_CHANGED"
                    )
                try:
                    page_total = int(data.get("total_hits") or 0)
                except (TypeError, ValueError):
                    raise StockEventSourceError(
                        "STOCK_EVENTS_ANNOUNCEMENT_SCHEMA_CHANGED"
                    ) from None
                if page_total < 0:
                    raise StockEventSourceError(
                        "STOCK_EVENTS_ANNOUNCEMENT_SCHEMA_CHANGED"
                    )
            except Exception as exc:
                return _AnnouncementChunkResult(
                    records=tuple(records),
                    checked=tuple(checked),
                    truncated=False,
                    total_hits=total_hits,
                    pages_read=len(checked),
                    failure_reason=_source_reason("ANNOUNCEMENT", exc),
                    failed_page=page,
                )
            checked.append(response.completed_at.astimezone(UTC))
            records.extend(row for row in rows if isinstance(row, dict))
            total_hits = max(total_hits, page_total)
            if page * ANNOUNCEMENT_PAGE_SIZE >= total_hits:
                break
        return _AnnouncementChunkResult(
            records=tuple(records),
            checked=tuple(checked),
            truncated=(
                len(checked) == page_limit
                and page_limit * ANNOUNCEMENT_PAGE_SIZE < total_hits
            ),
            total_hits=total_hits,
            pages_read=len(checked),
        )

    def _security_rows(
        self,
        auto: dict[str, dict[str, Any]],
        manual_codes: tuple[str, ...],
        directory: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        codes = tuple(dict.fromkeys((*manual_codes, *auto.keys())))
        manual_set = set(manual_codes)
        rows: list[dict[str, Any]] = []
        for code in codes:
            automatic = auto.get(code)
            resolved = directory.get(code, {})
            is_manual = code in manual_set
            is_auto = automatic is not None
            rows.append(
                {
                    "code": code,
                    "name": resolved.get("name") or (automatic or {}).get("name"),
                    "secu_code": resolved.get("secu_code"),
                    "market_label": resolved.get("market_label"),
                    "industry": resolved.get("industry") or (automatic or {}).get("industry"),
                    "origin": "BOTH" if is_manual and is_auto else "MANUAL" if is_manual else "AUTO",
                    "is_manual": is_manual,
                    "is_auto": is_auto,
                    "auto_reasons": list((automatic or {}).get("reasons") or []),
                    "auto_rank": (automatic or {}).get("auto_rank"),
                    "verification_state": "VERIFIED" if code in directory else "UNAVAILABLE",
                }
            )
        return rows

    def _calendar_events(
        self,
        securities: list[dict[str, Any]],
        start_date: date,
        end_date: date,
        today: date,
        settings: StockEventSettings,
        issues: list[CollectionIssue],
        source_states: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool]:
        codes = tuple(str(item["code"]) for item in securities)
        names = {str(item["code"]): item.get("name") for item in securities}
        records: list[dict[str, Any]] = []
        checked: list[datetime] = []
        failures: list[str] = []
        truncated = False
        for chunk in _chunks(codes, 30):
            context = {
                "stock_codes": ",".join(chunk),
                "stock_count": len(chunk),
                "window_start": start_date.isoformat(),
                "window_end": end_date.isoformat(),
                "page_size": 500,
                "page_limit": settings.max_calendar_pages_per_chunk,
            }
            try:
                chunk_records, chunk_checked, chunk_truncated = self._calendar_chunk(
                    chunk, start_date, end_date, settings
                )
            except Exception as exc:
                reason = _source_reason("CALENDAR", exc)
                failures.append(reason)
                issues.append(
                    CollectionIssue(
                        scope="stock-calendar",
                        reason_code=reason,
                        context=context,
                    )
                )
                continue
            records.extend(chunk_records)
            checked.extend(chunk_checked)
            truncated = truncated or chunk_truncated
            if chunk_truncated:
                issues.append(
                    CollectionIssue(
                        scope="stock-calendar",
                        reason_code="STOCK_EVENTS_CALENDAR_TRUNCATED",
                        context={
                            **context,
                            "pages_read": len(chunk_checked),
                            "records_read": len(chunk_records),
                        },
                    )
                )
        status = "ERROR" if codes and not checked else "PARTIAL" if failures or truncated else "SUCCESS"
        if status == "SUCCESS" and not records:
            status = "EMPTY"
        source_states.append(
            {
                "key": "stock-calendar",
                "label": "东方财富公司事件日历（Choice 数据）",
                "status": status,
                "checked_at": iso_utc(max(checked)) if checked else None,
                "record_count": len(records) if checked or not codes else None,
                "detail": "近一个月历史与已明确日期的未来公司事项",
                "source_url": STOCK_CALENDAR_PAGE_URL,
            }
        )

        events: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in records:
            code = _clean_text(row.get("SECURITY_CODE"), limit=6)
            event_type_code = _clean_text(row.get("EVENT_TYPE_CODE"), limit=3)
            category = CALENDAR_EVENT_TYPES.get(event_type_code)
            if code not in names or category is None:
                continue
            event_date = _source_date(
                row.get("NOTICE_DATE"), "STOCK_EVENTS_CALENDAR_DATE_INVALID"
            )
            if not start_date <= event_date <= end_date:
                continue
            event_type = _clean_text(row.get("EVENT_TYPE"), limit=40) or category[1]
            summary = _clean_text(row.get("LEVEL1_CONTENT"), limit=600)
            digest = hashlib.sha256(summary.encode("utf-8")).hexdigest()[:12]
            event_id = f"calendar:{code}:{event_type_code}:{event_date.isoformat()}:{digest}"
            if event_id in seen:
                continue
            seen.add(event_id)
            state, state_label = _event_state(event_date, today)
            events.append(
                {
                    "event_id": event_id,
                    "stock_code": code,
                    "stock_name": names[code],
                    "event_date": event_date.isoformat(),
                    "sort_at": iso_utc(_event_datetime(event_date)),
                    "time_precision": "DATE",
                    "title": event_type,
                    "summary": summary,
                    "category": category[0],
                    "category_label": category[1],
                    "importance": _importance(event_type_code, summary),
                    "state": state,
                    "state_label": state_label,
                    "source_kind": "EVENT_CALENDAR",
                    "source_label": "东方财富公司事件日历（Choice 数据）",
                    "source_url": f"https://data.eastmoney.com/stockcalendar/{code}.html",
                    "source_checked_at": iso_utc(max(checked)) if checked else None,
                }
            )
        return events, bool(checked) or not codes

    def _announcement_events(
        self,
        securities: list[dict[str, Any]],
        start_date: date,
        today: date,
        settings: StockEventSettings,
        issues: list[CollectionIssue],
        source_states: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool]:
        manual_codes = tuple(str(item["code"]) for item in securities if item["is_manual"])
        auto_only_codes = tuple(
            str(item["code"])
            for item in securities
            if item["is_auto"] and not item["is_manual"]
        )
        names = {str(item["code"]): item.get("name") for item in securities}
        manual_set = set(manual_codes)
        records: list[dict[str, Any]] = []
        checked: list[datetime] = []
        failures: list[str] = []
        truncated = False
        requests: list[tuple[str, tuple[str, ...], date, int]] = [
            ("MANUAL", chunk, start_date, settings.max_announcement_pages_per_chunk)
            for chunk in _chunks(manual_codes, 30)
        ]
        auto_start_date = max(start_date, today - timedelta(days=7))
        requests.extend(
            ("AUTO", chunk, auto_start_date, AUTO_ANNOUNCEMENT_PAGE_LIMIT)
            for chunk in _chunks(auto_only_codes, AUTO_ANNOUNCEMENT_CHUNK_SIZE)
        )
        for selection_origin, chunk, request_start_date, page_limit in requests:
            result = self._announcement_chunk(
                chunk, request_start_date, today, page_limit
            )
            records.extend(result.records)
            checked.extend(result.checked)
            base_context: dict[str, str | int | None] = {
                "selection_origin": selection_origin,
                "stock_codes": ",".join(chunk),
                "stock_count": len(chunk),
                "window_start": request_start_date.isoformat(),
                "window_end": today.isoformat(),
                "page_size": ANNOUNCEMENT_PAGE_SIZE,
                "page_limit": page_limit,
                "pages_read": result.pages_read,
                "records_read": len(result.records),
                "upstream_total_hits": (
                    result.total_hits if result.pages_read else None
                ),
            }
            if result.failure_reason is not None:
                failures.append(result.failure_reason)
                issues.append(
                    CollectionIssue(
                        scope="stock-announcements",
                        reason_code=result.failure_reason,
                        context={
                            **base_context,
                            "failed_page": result.failed_page,
                        },
                    )
                )
            if result.truncated:
                truncated = True
                issues.append(
                    CollectionIssue(
                        scope="stock-announcements",
                        reason_code="STOCK_EVENTS_ANNOUNCEMENTS_TRUNCATED",
                        context=base_context,
                    )
                )
        codes = manual_codes + auto_only_codes
        status = "ERROR" if codes and not checked else "PARTIAL" if failures or truncated else "SUCCESS"
        if status == "SUCCESS" and not records:
            status = "EMPTY"
        source_states.append(
            {
                "key": "stock-announcements",
                "label": "东方财富 A 股公告索引",
                "status": status,
                "checked_at": iso_utc(max(checked)) if checked else None,
                "record_count": len(records) if checked or not codes else None,
                "detail": "手动关注回看一个月；每日入选回看七天且仅保留重大事项关键词公告",
                "source_url": ANNOUNCEMENT_PAGE_URL,
            }
        )

        events: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in records:
            code_rows = row.get("codes")
            if not isinstance(code_rows, list):
                continue
            code = next(
                (
                    _clean_text(item.get("stock_code"), limit=6)
                    for item in code_rows
                    if isinstance(item, dict)
                    and _clean_text(item.get("stock_code"), limit=6) in names
                ),
                "",
            )
            title = _clean_text(row.get("title_ch") or row.get("title"), limit=500)
            if not code or not title:
                continue
            material = any(token in title for token in MATERIAL_ANNOUNCEMENT_KEYWORDS)
            if code not in manual_set and not material:
                continue
            art_code = _clean_text(row.get("art_code"), limit=48)
            if not re.fullmatch(r"AN\d+", art_code) or art_code in seen:
                continue
            notice_date = _source_date(
                row.get("notice_date"), "STOCK_EVENTS_ANNOUNCEMENT_DATE_INVALID"
            )
            if not start_date <= notice_date <= today:
                continue
            seen.add(art_code)
            published_at, precision = _source_datetime(
                row.get("display_time"), notice_date
            )
            category, category_label = _announcement_category(title)
            columns = row.get("columns")
            column_label = next(
                (
                    _clean_text(item.get("column_name"), limit=40)
                    for item in columns
                    if isinstance(item, dict) and item.get("column_name")
                ),
                "公司公告",
            ) if isinstance(columns, list) else "公司公告"
            events.append(
                {
                    "event_id": f"announcement:{art_code}",
                    "stock_code": code,
                    "stock_name": names[code],
                    "event_date": notice_date.isoformat(),
                    "sort_at": iso_utc(published_at),
                    "time_precision": precision,
                    "title": title,
                    "summary": column_label,
                    "category": category,
                    "category_label": category_label,
                    "importance": _importance("", title),
                    "state": "OCCURRED",
                    "state_label": "刚发生的公告" if notice_date >= today - timedelta(days=2) else "已公告",
                    "source_kind": "ANNOUNCEMENT",
                    "source_label": "东方财富 A 股公告索引",
                    "source_url": f"https://data.eastmoney.com/notices/detail/{code}/{art_code}.html",
                    "source_checked_at": iso_utc(max(checked)) if checked else None,
                    "published_at": iso_utc(published_at),
                    "material_keyword_match": material,
                }
            )
        return events, bool(checked) or not codes

    def collect(self) -> CollectionBatch:
        now = self._now().astimezone(UTC)
        today = now.astimezone(SHANGHAI_TZ).date()
        trade_date = _latest_xshg_session(today)
        with self._settings_lock:
            settings = self.settings
        manual_codes = settings.manual_stock_codes
        previous_snapshot = self.store.projection_snapshot(self.monitor_id, "current")
        previous = previous_snapshot.payload if previous_snapshot is not None else None
        issues: list[CollectionIssue] = []
        source_states: list[dict[str, Any]] = []

        auto, reused = self._daily_universe(
            trade_date, previous, settings, issues, source_states
        )
        directory, directory_snapshot = self._stock_directory(
            now, issues, source_states
        )
        directory_snapshots = (
            (directory_snapshot,) if directory_snapshot is not None else ()
        )
        if not auto and not manual_codes and source_states[0]["status"] == "ERROR":
            return CollectionBatch(
                samples=(),
                issues=tuple(issues),
                projection_snapshots=directory_snapshots,
            )

        securities = self._security_rows(auto, manual_codes, directory)
        start_date = today - timedelta(days=settings.history_days)
        end_date = today + timedelta(days=settings.lookahead_days)
        calendar_events, calendar_valid = self._calendar_events(
            securities,
            start_date,
            end_date,
            today,
            settings,
            issues,
            source_states,
        )
        announcement_events, announcements_valid = self._announcement_events(
            securities,
            start_date,
            today,
            settings,
            issues,
            source_states,
        )
        if securities and not calendar_valid and not announcements_valid:
            return CollectionBatch(
                samples=(),
                issues=tuple(issues),
                projection_snapshots=directory_snapshots,
            )

        security_by_code = {str(item["code"]): item for item in securities}
        events = calendar_events + announcement_events
        for event in events:
            security = security_by_code[str(event["stock_code"])]
            event["stock_origin"] = security["origin"]
            event["is_manual_stock"] = security["is_manual"]
            event["is_auto_stock"] = security["is_auto"]
        events.sort(key=lambda item: (str(item["sort_at"]), str(item["event_id"])))
        truncated = len(events) > settings.max_events
        if truncated:
            original_event_count = len(events)
            manual_events = [item for item in events if item["is_manual_stock"]]
            auto_events = [item for item in events if not item["is_manual_stock"]]
            events = (manual_events + auto_events)[: settings.max_events]
            issues.append(
                CollectionIssue(
                    scope="stock-events-projection",
                    reason_code="STOCK_EVENTS_PROJECTION_TRUNCATED",
                    context={
                        "events_before_limit": original_event_count,
                        "event_limit": settings.max_events,
                        "manual_event_count": len(manual_events),
                        "automatic_event_count": len(auto_events),
                    },
                )
            )

        securities.sort(
            key=lambda item: (
                0 if item["is_manual"] else 1,
                int(item.get("auto_rank") or MAX_AUTO_STOCKS + 1),
                str(item["code"]),
            )
        )
        source_checked = [
            str(item["checked_at"])
            for item in source_states
            if item.get("checked_at")
        ]
        payload = {
            "projection_kind": "stock_events",
            "observed_at": iso_utc(now),
            "cutoff_at": iso_utc(now),
            "window_start": start_date.isoformat(),
            "window_end": end_date.isoformat(),
            "history_days": settings.history_days,
            "lookahead_days": settings.lookahead_days,
            "selection_trade_date": trade_date.isoformat(),
            "selection_checked_at": (
                previous.get("selection_checked_at")
                if reused and previous
                else source_states[0].get("checked_at")
            ),
            "auto_limit": settings.auto_limit,
            "manual_limit": MAX_MANUAL_STOCKS,
            "manual_stock_codes": list(manual_codes),
            "securities": securities,
            "events": events,
            "source_states": source_states,
            "source_checked_at": max(source_checked) if source_checked else None,
            "event_count": len(events),
            "event_truncated": truncated,
            "method_note": self.view.method_note,
        }
        return CollectionBatch(
            samples=(),
            issues=tuple(issues),
            projection_snapshots=(
                ProjectionSnapshot(
                    snapshot_key="current",
                    observed_at=now,
                    cutoff_at=now,
                    payload=payload,
                ),
                *directory_snapshots,
            ),
        )


__all__ = [
    "StockEventMonitor",
    "StockEventSettings",
    "StockEventSourceError",
]
