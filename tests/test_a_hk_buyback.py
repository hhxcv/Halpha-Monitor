import io
import json
from datetime import UTC, datetime, timedelta
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

import pytest
from pypdf import PdfWriter

from halpha_monitor.monitors import a_hk_buyback as buyback
from halpha_monitor.buyback_metrics import (
    buyback_projection_valid_until,
    match_a_share_program,
    parse_a_share_reference,
    parse_financial_reference,
    parse_hk_market_reference,
    parse_tencent_market_reference,
    project_buyback_metrics,
)
from halpha_monitor.monitors.a_hk_buyback import (
    AHKBuybackMonitor,
    BuybackPublicClient,
    BuybackSettings,
    BuybackSourceError,
    BuybackTradingSchedule,
    HkexExecution,
    PublicResponse,
    classify_buyback_attention,
    classify_buyback_title,
    is_target_a_share_security,
    parse_cninfo_announcement_payload,
    parse_hkex_report,
    parse_sse_announcement_payload,
    parse_sse_connect,
    parse_szse_connect_page,
    validate_pdf,
)
from halpha_monitor.store import SQLiteMonitorStore


NOW = datetime(2026, 8, 7, 2, 0, tzinfo=UTC)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def test_buyback_projection_cache_expiry_matches_next_time_derived_change() -> None:
    now = datetime(2026, 8, 7, 23, 30, tzinfo=UTC)
    reference_at = now - timedelta(days=7) + timedelta(minutes=15)
    source_payloads = {
        "a-share-buyback-reference": {
            "programmes": [{"updated_at": reference_at.isoformat()}]
        }
    }

    valid_until = buyback_projection_valid_until(source_payloads, now=now)
    midnight_only = buyback_projection_valid_until({}, now=now)

    assert valid_until == reference_at + timedelta(days=7, microseconds=1)
    assert midnight_only == datetime(2026, 8, 8, 0, 0, tzinfo=UTC)


def test_buyback_schedule_uses_a_h_union_and_stays_closed_off_session() -> None:
    schedule = BuybackTradingSchedule()

    before = schedule.state(
        now=datetime(2026, 8, 7, 8, 59, tzinfo=SHANGHAI_TZ)
    )
    hk_preopen = schedule.state(
        now=datetime(2026, 8, 7, 9, 0, tzinfo=SHANGHAI_TZ)
    )
    lunch = schedule.state(
        now=datetime(2026, 8, 7, 12, 30, tzinfo=SHANGHAI_TZ)
    )
    afternoon = schedule.state(
        now=datetime(2026, 8, 7, 13, 0, tzinfo=SHANGHAI_TZ)
    )
    hk_closing_auction = schedule.state(
        now=datetime(2026, 8, 7, 16, 9, tzinfo=SHANGHAI_TZ)
    )
    closed = schedule.state(
        now=datetime(2026, 8, 7, 16, 10, tzinfo=SHANGHAI_TZ)
    )
    weekend = schedule.state(
        now=datetime(2026, 8, 8, 10, 0, tzinfo=SHANGHAI_TZ)
    )

    assert before.status == "CLOSED" and before.allowed is False
    assert hk_preopen.status == "OPEN" and hk_preopen.allowed is True
    assert lunch.status == "CLOSED" and lunch.allowed is False
    assert afternoon.status == "OPEN" and afternoon.allowed is True
    assert hk_closing_auction.status == "OPEN"
    assert closed.status == "CLOSED" and closed.allowed is False
    assert weekend.status == "CLOSED"
    assert weekend.next_open_at == datetime(2026, 8, 10, 1, 0, tzinfo=UTC)


def test_buyback_schedule_fails_closed_when_exchange_calendars_are_unknown() -> None:
    def unavailable_calendar(*_args: object, **_kwargs: object) -> object:
        raise ValueError("fixture calendar unavailable")

    state = BuybackTradingSchedule(
        calendar_factory=unavailable_calendar
    ).state(now=NOW)

    assert state.status == "UNAVAILABLE"
    assert state.allowed is False
    assert state.reason_code == "BUYBACK_TRADING_CALENDAR_UNAVAILABLE"


def blank_pdf() -> bytes:
    stream = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=300, height=300)
    writer.write(stream)
    return stream.getvalue()


def response(body: bytes, content_type: str) -> PublicResponse:
    return PublicResponse(
        body=body,
        status=200,
        content_type=content_type,
        started_at=NOW,
        completed_at=NOW + timedelta(milliseconds=20),
        headers={},
    )


def sse_announcement_body() -> bytes:
    payload = {
        "pageHelp": {
            "pageCount": 1,
            "data": [
                {
                    "SECURITY_CODE": "600000",
                    "SECURITY_NAME": "公司A",
                    "TITLE": "公司A关于回购股份方案的公告",
                    "URL": "/disclosure/listedinfo/announcement/c/new.pdf",
                    "ADDDATE": "2026-08-06 18:00:00",
                    "SSEDATE": "2026-08-06",
                }
            ],
        }
    }
    return f"jsonpCallback({json.dumps(payload, ensure_ascii=False)})".encode()


def cninfo_body(*, market: str, include: bool) -> bytes:
    release = int(
        datetime(2026, 8, 6, 10, 0, tzinfo=UTC).timestamp() * 1000
    )
    announcements = (
        [
            {
                "secCode": "600000" if market == "SH" else "000001",
                "secName": "公司A" if market == "SH" else "公司B",
                "announcementTitle": (
                    "公司A关于回购股份方案的公告"
                    if market == "SH"
                    else "公司B关于首次回购股份的公告"
                ),
                "announcementId": f"{market}-document",
                "announcementTime": release,
                "adjunctUrl": f"finalpage/2026-08-06/{market}.PDF",
            }
        ]
        if include
        else []
    )
    return json.dumps(
        {"announcements": announcements, "hasMore": False},
        ensure_ascii=False,
    ).encode()


def sse_connect_body() -> bytes:
    payload = {
        "result": [
            {
                "SECURITY_CODE": "00005",
                "SECURITY_TYPE": "股票",
                "TRADE_FLAG": "1",
                "UPDATE_DATE": "2026-08-06",
            },
            {
                "SECURITY_CODE": "00006",
                "SECURITY_TYPE": "股票",
                "TRADE_FLAG": "0",
                "UPDATE_DATE": "2026-08-06",
            },
        ]
    }
    return f"jsonpCallback({json.dumps(payload, ensure_ascii=False)})".encode()


def sz_connect_body() -> bytes:
    return json.dumps(
        [
            {
                "metadata": {
                    "pagecount": 1,
                    "recordcount": 1,
                    "pageno": 1,
                    "subname": "2026-08-06",
                },
                "data": [{"zqdm": "00005", "zqjc": "Fixture"}],
                "error": None,
            }
        ],
        ensure_ascii=False,
    ).encode()


def a_share_reference_body() -> bytes:
    return json.dumps(
        {
            "success": True,
            "result": {
                "data": [
                    {
                        "REPURCODE": "123",
                        "DIM_SCODE": "600000",
                        "SECURITYSHORTNAME": "公司A",
                        "UPD": "2026-08-07 00:00:00",
                        "NOTICEDATE": "2026-08-06 00:00:00",
                        "DIM_TRADEDATE": "2026-08-05 00:00:00",
                        "REPURNUM": 1000,
                        "REPURAMOUNT": 9900,
                        "REPURPRICECAP1": 10,
                        "REPURPRICELOWER1": 9.8,
                        "REPURAMOUNTLOWER": 8000,
                        "REPURAMOUNTLIMIT": 12000,
                        "NEWPRICE": 9.5,
                        "ZSZ": 1000000,
                    }
                ]
            },
        },
        ensure_ascii=False,
    ).encode()


def hk_market_reference_body() -> bytes:
    return json.dumps(
        {
            "rc": 0,
            "data": {
                "diff": [
                    {
                        "f2": 9.5,
                        "f3": 1.25,
                        "f12": "600000",
                        "f13": 1,
                        "f14": "Fixture A",
                        "f18": 9.4,
                        "f20": 1000000,
                        "f9": 10,
                        "f23": 1,
                        "f37": 12.5,
                        "f41": 8.2,
                        "f46": -3.5,
                        "f124": int(NOW.timestamp()),
                    },
                    {
                        "f2": 9.5,
                        "f3": -1.0,
                        "f12": "00005",
                        "f13": 116,
                        "f14": "Fixture HK",
                        "f18": 9.6,
                        "f20": 1000000,
                        "f9": 10,
                        "f23": 1,
                        "f37": 18.2,
                        "f41": 15.0,
                        "f46": 22.0,
                        "f124": int(NOW.timestamp()),
                    }
                ]
            },
        }
    ).encode()


def tencent_market_reference_body() -> bytes:
    def line(symbol: str, *, name: str, code: str, price: str, previous: str,
             timestamp: str, change: str, change_percent: str,
             market_cap_100m: str) -> str:
        fields = [""] * 50
        fields[1] = name
        fields[2] = code
        fields[3] = price
        fields[4] = previous
        fields[30] = timestamp
        fields[31] = change
        fields[32] = change_percent
        fields[45] = market_cap_100m
        return f'v_{symbol}="{"~".join(fields)}";'

    return "\n".join(
        (
            line(
                "sh600000",
                name="公司A",
                code="600000",
                price="9.50",
                previous="9.60",
                timestamp="20260807100000",
                change="-0.10",
                change_percent="-1.04",
                market_cap_100m="123.45",
            ),
            line(
                "hk00005",
                name="Fixture HK",
                code="00005",
                price="9.50",
                previous="9.60",
                timestamp="2026/08/07 10:00:00",
                change="-0.10",
                change_percent="-1.04",
                market_cap_100m="10.00",
            ),
        )
    ).encode("gb18030")


def financial_reference_body(*, market_scope: str) -> bytes:
    if market_scope == "HK":
        data = [
            {
                "SECUCODE": "00005.HK",
                "SECURITY_CODE": "00005",
                "SECURITY_NAME_ABBR": "Fixture HK",
                "REPORT_DATE": "2026-03-31 00:00:00",
                "DATE_TYPE_CODE": "003",
                "OPERATE_INCOME_YOY": 15.0,
                "HOLDER_PROFIT_YOY": 22.0,
                "ROE_AVG": 4.1,
            },
            {
                "SECUCODE": "00005.HK",
                "SECURITY_CODE": "00005",
                "SECURITY_NAME_ABBR": "Fixture HK",
                "REPORT_DATE": "2025-12-31 00:00:00",
                "DATE_TYPE_CODE": "001",
                "OPERATE_INCOME_YOY": 12.0,
                "HOLDER_PROFIT_YOY": 18.0,
                "ROE_AVG": 18.2,
            },
        ]
    else:
        data = [
            {
                "SECUCODE": "600000.SH",
                "SECURITY_CODE": "600000",
                "SECURITY_NAME_ABBR": "公司A",
                "REPORT_DATE": "2026-03-31 00:00:00",
                "REPORT_TYPE": "一季报",
                "NOTICE_DATE": "2026-04-30 00:00:00",
                "ROEJQ": 3.0,
                "TOTALOPERATEREVETZ": 8.2,
                "PARENTNETPROFITTZ": -3.5,
            },
            {
                "SECUCODE": "600000.SH",
                "SECURITY_CODE": "600000",
                "SECURITY_NAME_ABBR": "公司A",
                "REPORT_DATE": "2025-12-31 00:00:00",
                "REPORT_TYPE": "年报",
                "NOTICE_DATE": "2026-03-30 00:00:00",
                "ROEJQ": 12.5,
                "TOTALOPERATEREVETZ": 7.0,
                "PARENTNETPROFITTZ": 5.0,
            },
        ]
    return json.dumps(
        {
            "code": 0,
            "success": True,
            "result": {"pages": 1, "data": data},
        },
        ensure_ascii=False,
    ).encode()


class FixtureClient:
    def __init__(
        self,
        *,
        fail_sse_connect: bool = False,
        fail_market_primary: bool = False,
        hk_report: bool = False,
    ):
        self.fail_sse_connect = fail_sse_connect
        self.fail_market_primary = fail_market_primary
        self.hk_report = hk_report
        self.calls: list[str] = []

    def network_request_count(self, *, window_seconds: float = 60) -> int:
        return len(self.calls)

    def request(self, url: str, **kwargs) -> PublicResponse:  # noqa: ANN003
        self.calls.append(url)
        if "datacenter-web.eastmoney.com" in url:
            return response(a_share_reference_body(), "text/plain;charset=UTF-8")
        if "datacenter.eastmoney.com/securities/api/data/v1/get" in url:
            return response(
                financial_reference_body(
                    market_scope="HK" if "RPT_HKF10" in url else "A_SHARE"
                ),
                "text/plain;charset=UTF-8",
            )
        if "push2.eastmoney.com" in url:
            if self.fail_market_primary:
                raise BuybackSourceError(
                    "BUYBACK_NETWORK_REMOTEDISCONNECTED",
                    temporary=True,
                )
            return response(hk_market_reference_body(), "application/json")
        if "qt.gtimg.cn" in url:
            return response(
                tencent_market_reference_body(),
                "text/html; charset=GBK",
            )
        if "commonQuery.do" in url:
            if self.fail_sse_connect:
                raise BuybackSourceError("BUYBACK_NETWORK_TIMEOUTERROR", temporary=True)
            return response(sse_connect_body(), "application/json")
        if "ShowReport/data" in url:
            return response(sz_connect_body(), "application/json")
        if "queryCompanyBulletin" in url:
            return response(sse_announcement_body(), "application/json")
        if "fulltextSearch" in url:
            include = "hzb%2Ckcb" in url
            return response(
                cninfo_body(market="SH" if include else "SZ", include=include),
                "application/json",
            )
        if url.endswith("new.pdf"):
            return response(b"<html>script challenge</html>", "text/html")
        if url.endswith("SH.PDF"):
            return response(blank_pdf(), "application/pdf")
        if url.endswith("SZ.PDF"):
            return response(blank_pdf(), "application/pdf")
        if url.endswith("sbn.asp"):
            link = (
                '<a href="./documents/SRRPT20260806.xls">report</a>'
                if self.hk_report
                else "<html>no report</html>"
            )
            return response(link.encode(), "text/html")
        if url.endswith("sbmain.asp"):
            return response(b"<html>no report</html>", "text/html")
        if url.endswith("SRRPT20260806.xls"):
            return response(
                b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1fixture",
                "application/vnd.ms-excel",
            )
        raise AssertionError(f"unexpected fixture URL: {url}")


def make_monitor(tmp_path: Path, client: FixtureClient) -> tuple[AHKBuybackMonitor, SQLiteMonitorStore]:
    store = SQLiteMonitorStore(tmp_path / "monitor.sqlite3")
    store.initialize()
    monitor = AHKBuybackMonitor(
        BuybackSettings(
            interval_seconds=3600,
            jitter_seconds=0,
            max_documents_per_run=10,
        ),
        store=store,
        client=client,  # type: ignore[arg-type]
        now=lambda: NOW,
    )
    return monitor, store


def test_title_classifier_keeps_lifecycle_events_separate_from_false_positives() -> None:
    assert classify_buyback_title("关于首次回购股份的公告") == "FIRST_EXECUTION"
    assert classify_buyback_title("关于调整回购股份用途的公告") == "MODIFICATION"
    assert (
        classify_buyback_title("业绩承诺方回购股份的提示性公告")
        == "OUT_OF_SCOPE_OTHER_REPURCHASE"
    )
    assert (
        classify_buyback_title("关于注销已回购业绩承诺补偿股份的公告")
        == "OUT_OF_SCOPE_OTHER_REPURCHASE"
    )
    assert (
        classify_buyback_title("关于控股子公司回购员工持股计划股份的公告")
        == "OUT_OF_SCOPE_OTHER_REPURCHASE"
    )
    assert classify_buyback_title("回购股份事项前十名股东持股情况公告") == "ANCILLARY"
    assert classify_buyback_title("出售已回购股份进展公告") == "POST_BUYBACK_DISPOSAL"
    assert classify_buyback_title("首次集中竞价减持已回购股份公告") == "POST_BUYBACK_DISPOSAL"
    assert is_target_a_share_security("SZ", "002249") is True
    assert is_target_a_share_security("SZ", "200512") is False
    assert is_target_a_share_security("SH", "900901") is False


def test_attention_classification_has_one_explicit_event_stage_owner() -> None:
    assert classify_buyback_attention("FIRST_EXECUTION", "VERIFIED") == (
        "PRIORITY",
        "优先研判",
    )
    assert classify_buyback_attention("PLAN_OR_APPROVAL", "VERIFIED") == (
        "TRACKING",
        "持续跟踪",
    )
    assert classify_buyback_attention(
        "COMPLETION_OR_TERMINATION", "VERIFIED"
    ) == ("UPDATE", "状态更新")
    assert classify_buyback_attention("FIRST_EXECUTION", "PENDING") == (
        "PENDING",
        "待补全",
    )
    assert AHKBuybackMonitor.view.method_note is None


def test_announcement_parsers_preserve_source_time_and_document_identity() -> None:
    sse_rows, page_count, _ = parse_sse_announcement_payload(
        sse_announcement_body(),
        begin=datetime(2026, 8, 1, tzinfo=UTC).date(),
        end=datetime(2026, 8, 7, tzinfo=UTC).date(),
    )
    cninfo_rows, has_more, _ = parse_cninfo_announcement_payload(
        cninfo_body(market="SH", include=True),
        market="SH",
        source_key="cninfo-sh-announcements",
        source_label="巨潮沪市公告索引",
    )

    assert page_count == 1
    assert sse_rows[0].stock_code == "600000"
    assert sse_rows[0].time_precision == "SECOND"
    assert sse_rows[0].document_url.startswith("https://www.sse.com.cn/")
    assert has_more is False
    assert cninfo_rows[0].source_document_id == "SH-document"
    assert cninfo_rows[0].identity == sse_rows[0].identity


def test_connect_parsers_keep_routes_and_five_digit_codes() -> None:
    sse_codes, sse_time, _ = parse_sse_connect(sse_connect_body())
    sz_codes, page, pages, sz_time, _ = parse_szse_connect_page(sz_connect_body())

    assert sse_codes == frozenset({"00005"})
    assert sz_codes == ("00005",)
    assert (page, pages) == (1, 1)
    assert sse_time == sz_time


def test_sse_connect_rejects_a_list_without_an_official_source_date() -> None:
    payload = {
        "result": [
            {
                "SECURITY_CODE": "00005",
                "SECURITY_TYPE": "股票",
                "TRADE_FLAG": "1",
            }
        ]
    }

    with pytest.raises(
        BuybackSourceError,
        match="BUYBACK_CONNECT_SSE_SOURCE_DATE_MISSING",
    ):
        parse_sse_connect(
            f"jsonpCallback({json.dumps(payload, ensure_ascii=False)})".encode()
        )


def test_pdf_validator_rejects_html_and_marks_textless_pdf_for_review() -> None:
    parsed = validate_pdf(blank_pdf())

    assert parsed.quality_state == "VALID_PDF_NO_TEXT"
    with pytest.raises(BuybackSourceError, match="BUYBACK_DOCUMENT_NOT_PDF"):
        validate_pdf(b"<html>not a pdf</html>")


def test_a_share_numeric_facts_require_one_four_field_pdf_match() -> None:
    programmes, source_time, schema_hash = parse_a_share_reference(
        a_share_reference_body()
    )
    matched = match_a_share_program(
        (
            "截至公告日，公司累计回购1,000股，支付总金额9,900元，"
            "最高成交价10.00元/股，最低成交价9.80元/股；"
            "计划金额下限8,000元、上限12,000元。"
        ),
        stock_code="600000",
        released_at=NOW,
        event_type="PROGRESS",
        programmes=programmes,
    )

    assert source_time == datetime(2026, 8, 7, tzinfo=UTC)
    assert len(schema_hash) == 64
    assert matched == {
        "program_reference_id": "123",
        "shares": 1000,
        "amount": 9900,
        "currency": "CNY",
        "high_price": 10,
        "low_price": 9.8,
        "average_cost": 9.9,
        "numeric_fact_scope": "PROGRAMME_CUMULATIVE",
        "plan_amount_lower": 8000,
        "plan_amount_upper": 12000,
    }
    assert match_a_share_program(
        "累计回购1,000股，支付总金额9,900元，最高成交价10元/股。",
        stock_code="600000",
        released_at=NOW,
        event_type="PROGRESS",
        programmes=programmes,
    ) is None


def test_market_reference_preserves_negative_change_and_financial_inputs() -> None:
    quotes, source_time, schema_hash = parse_hk_market_reference(
        hk_market_reference_body(),
        expected_codes=("00005",),
    )

    assert source_time == NOW
    assert len(schema_hash) == 64
    assert len(quotes) == 1
    assert quotes[0]["change_percent"] == -1.0
    assert quotes[0]["roe_percent"] == 18.2
    assert quotes[0]["revenue_yoy_percent"] == 15.0
    assert quotes[0]["net_profit_yoy_percent"] == 22.0


def test_tencent_fallback_keeps_only_documented_quote_fields() -> None:
    quotes, source_time, schema_hash = parse_tencent_market_reference(
        tencent_market_reference_body(),
        expected_securities=(
            ("A_SHARE", "SH", "600000"),
            ("HK", "HK", "00005"),
        ),
    )

    assert source_time == datetime(2026, 8, 7, 2, 0, tzinfo=UTC)
    assert len(schema_hash) == 64
    assert len(quotes) == 2
    by_code = {quote["stock_code"]: quote for quote in quotes}
    assert by_code["600000"]["change_percent"] == -1.04
    assert by_code["600000"]["market_cap"] == 12_345_000_000
    assert by_code["00005"]["provider"] == "TENCENT"
    assert by_code["00005"]["roe_percent"] is None


def test_market_reference_uses_quote_fallback_when_primary_fails(
    tmp_path: Path,
) -> None:
    monitor, _store = make_monitor(
        tmp_path,
        FixtureClient(fail_market_primary=True),
    )

    result = monitor._fetch_tencent_market_reference(  # noqa: SLF001
        {("A_SHARE", "SH", "600000"), ("HK", "HK", "00005")}
    )

    assert result.status == "SUCCESS"
    assert result.detail_code is None
    assert result.payload["provider"] == "TENCENT"


def test_financial_reference_uses_annual_roe_and_latest_period_growth() -> None:
    a_records, a_time, a_schema = parse_financial_reference(
        financial_reference_body(market_scope="A_SHARE"),
        expected_securities=(("A_SHARE", "SH", "600000"),),
    )
    hk_records, hk_time, hk_schema = parse_financial_reference(
        financial_reference_body(market_scope="HK"),
        expected_securities=(("HK", "HK", "00005"),),
    )

    assert len(a_schema) == len(hk_schema) == 64
    assert a_time == datetime(2026, 4, 30, tzinfo=UTC)
    assert hk_time == datetime(2026, 3, 31, tzinfo=UTC)
    assert a_records[0]["roe_percent"] == 12.5
    assert a_records[0]["revenue_yoy_percent"] == 8.2
    assert a_records[0]["net_profit_yoy_percent"] == -3.5
    assert a_records[0]["financial_report_date"] == "2026-03-31"
    assert a_records[0]["roe_report_date"] == "2025-12-31"
    assert hk_records[0]["roe_percent"] == 18.2
    assert hk_records[0]["revenue_yoy_percent"] == 15.0
    assert hk_records[0]["net_profit_yoy_percent"] == 22.0


def test_hk_daily_rows_roll_up_only_when_the_mandate_chain_is_complete() -> None:
    base = {
        "market_scope": "HK",
        "market": "HK",
        "stock_code": "00005",
        "issuer_name": "Fixture HK",
        "share_class": "ORD",
        "entity_type": "HKEX_EXECUTION",
        "event_type": "HKEX_EXECUTION",
        "source_key": "hkex-main-reports",
        "currency": "HKD",
        "connect_status": "BUY_ELIGIBLE",
        "missing_reasons": {},
    }
    rows = [
        {
            **base,
            "entity_key": "HK:00005:first",
            "effective_at": "2026-08-05T00:00:00+00:00",
            "trading_date": "2026-08-05",
            "shares": 1000,
            "amount": 9900,
            "mandate_exchange_shares": 1000,
            "mandate_percentage": 0.1,
        },
        {
            **base,
            "entity_key": "HK:00005:latest",
            "effective_at": "2026-08-06T00:00:00+00:00",
            "trading_date": "2026-08-06",
            "shares": 1500,
            "amount": 14700,
            "mandate_exchange_shares": 2500,
            "mandate_percentage": 0.25,
        },
    ]
    quotes = [{
        "security_key": "HK:HK:00005",
        "stock_code": "00005",
        "current_price": 9.5,
        "change_percent": -1.25,
        "updated_at": NOW.isoformat(),
    }]
    financials = [{
        "security_key": "HK:HK:00005",
        "stock_code": "00005",
        "roe_percent": 18.2,
        "revenue_yoy_percent": 15.0,
        "net_profit_yoy_percent": 22.0,
        "financial_report_date": "2026-03-31",
        "roe_report_date": "2025-12-31",
        "updated_at": "2026-03-31T00:00:00+00:00",
    }]
    source_payloads = {
        "hk-market-reference": {"schema_version": 2, "quotes": quotes},
        "buyback-financial-reference": {
            "schema_version": 1,
            "financials": financials,
        },
        "connect-sh": {"codes": ["00005"]},
        "connect-sz": {"codes": []},
    }
    source_statuses = {
        "hkex-main-reports": "SUCCESS",
        "hk-market-reference": "SUCCESS",
        "buyback-financial-reference": "SUCCESS",
        "connect-sh": "SUCCESS",
        "connect-sz": "SUCCESS",
    }
    projected = project_buyback_metrics(
        rows,
        source_payloads=source_payloads,
        source_statuses=source_statuses,
        now=NOW,
    )

    assert len(projected) == 1
    row = projected[0]
    assert row["entity_key"] == "HK:00005:latest"
    assert row["programme_history_complete"] is True
    assert row["execution_days_label"] == "2天"
    assert row["cumulative_amount"] == 24600
    assert row["average_cost"] == pytest.approx(9.84)
    assert row["attractiveness_score"] is not None
    assert row["connect_status"] == "BUY_ELIGIBLE"
    assert row["connect_status_label"] == "可购买"
    assert row["daily_change_percent"] == -1.25
    assert row["roe_percent"] == 18.2

    stale_reference = project_buyback_metrics(
        rows,
        source_payloads=source_payloads,
        source_statuses={**source_statuses, "hk-market-reference": "STALE"},
        now=NOW,
    )[0]
    assert stale_reference["current_price"] is None
    assert stale_reference["daily_change_percent"] is None
    assert stale_reference["roe_percent"] == 18.2

    stale_financial = project_buyback_metrics(
        rows,
        source_payloads=source_payloads,
        source_statuses={
            **source_statuses,
            "buyback-financial-reference": "STALE",
        },
        now=NOW,
    )[0]
    assert stale_financial["current_price"] == 9.5
    assert stale_financial["roe_percent"] is None

    duplicate = {**rows[-1], "entity_key": "HK:00005:duplicate"}
    deduplicated = project_buyback_metrics(
        [*rows, duplicate],
        source_payloads=source_payloads,
        source_statuses=source_statuses,
        now=NOW,
    )[0]
    assert deduplicated["execution_days_value"] == 2
    assert deduplicated["cumulative_amount"] == 24600

    rows[0]["mandate_exchange_shares"] = 5000
    rows[1]["mandate_exchange_shares"] = 6500
    partial = project_buyback_metrics(
        rows,
        source_payloads=source_payloads,
        source_statuses=source_statuses,
        now=NOW,
    )[0]
    assert partial["programme_history_complete"] is False
    assert partial["execution_days_label"] == "至少2天"
    assert partial["cumulative_amount"] is None
    assert partial["cumulative_label"] == "6,500股"
    assert partial["recent_amount_label"] == "HKD 2.46万"


def test_hk_programme_start_is_detected_even_when_new_cumulative_exceeds_old() -> None:
    base = {
        "market_scope": "HK",
        "market": "HK",
        "stock_code": "00005",
        "issuer_name": "Fixture HK",
        "share_class": "ORD",
        "entity_type": "HKEX_EXECUTION",
        "event_type": "HKEX_EXECUTION",
        "source_key": "hkex-main-reports",
        "currency": "HKD",
        "missing_reasons": {},
    }
    rows = [
        {
            **base,
            "entity_key": "old-programme",
            "trading_date": "2026-08-04",
            "shares": 500,
            "amount": 4500,
            "mandate_exchange_shares": 500,
        },
        {
            **base,
            "entity_key": "new-programme-first",
            "trading_date": "2026-08-05",
            "shares": 1000,
            "amount": 9900,
            "mandate_exchange_shares": 1000,
        },
        {
            **base,
            "entity_key": "new-programme-latest",
            "trading_date": "2026-08-06",
            "shares": 1500,
            "amount": 14700,
            "mandate_exchange_shares": 2500,
        },
    ]
    projected = project_buyback_metrics(
        rows,
        source_payloads={
            "connect-sh": {"codes": ["00005"]},
            "connect-sz": {"codes": []},
        },
        source_statuses={
            "hkex-main-reports": "SUCCESS",
            "connect-sh": "SUCCESS",
            "connect-sz": "SUCCESS",
        },
        now=NOW,
    )[0]

    assert projected["entity_key"] == "new-programme-latest"
    assert projected["programme_history_complete"] is True
    assert projected["execution_days_value"] == 2
    assert projected["cumulative_amount"] == 24600


def test_purchase_eligibility_is_binary_and_requires_complete_negative_evidence() -> None:
    base_hk = {
        "market_scope": "HK",
        "market": "HK",
        "issuer_name": "Fixture HK",
        "share_class": "ORD",
        "entity_type": "HKEX_EXECUTION",
        "event_type": "HKEX_EXECUTION",
        "source_key": "hkex-main-reports",
        "currency": "HKD",
        "effective_at": "2026-08-06T00:00:00+00:00",
        "trading_date": "2026-08-06",
        "shares": 1000,
        "amount": 9900,
        "mandate_exchange_shares": 1000,
        "mandate_percentage": 0.1,
        "missing_reasons": {},
    }
    rows = [
        {
            "market_scope": "A_SHARE",
            "market": "SH",
            "stock_code": "600000",
            "issuer_name": "Fixture A",
            "event_type": "PLAN_OR_APPROVAL",
            "effective_at": "2026-08-06T00:00:00+00:00",
            "connect_status": "NOT_APPLICABLE",
            "connect_status_label": "不适用",
        },
        {**base_hk, "entity_key": "HK:00005:latest", "stock_code": "00005"},
        {**base_hk, "entity_key": "HK:00006:latest", "stock_code": "00006"},
    ]
    payloads = {
        "connect-sh": {"codes": ["00005"]},
        "connect-sz": {"codes": []},
    }
    complete_statuses = {
        "connect-sh": "SUCCESS",
        "connect-sz": "SUCCESS",
        "hkex-main-reports": "SUCCESS",
    }
    projected = project_buyback_metrics(
        rows,
        source_payloads=payloads,
        source_statuses=complete_statuses,
        now=NOW,
    )
    by_code = {row["stock_code"]: row for row in projected}

    assert by_code["600000"]["connect_status"] == "BUY_ELIGIBLE"
    assert by_code["600000"]["connect_status_label"] == "可购买"
    assert by_code["00005"]["connect_status"] == "BUY_ELIGIBLE"
    assert by_code["00005"]["connect_status_label"] == "可购买"
    assert by_code["00006"]["connect_status"] == "NOT_BUY_ELIGIBLE"
    assert by_code["00006"]["connect_status_label"] == "不可购买"

    partial_statuses = {**complete_statuses, "connect-sz": "ERROR"}
    partial = project_buyback_metrics(
        rows,
        source_payloads=payloads,
        source_statuses=partial_statuses,
        now=NOW,
    )
    partial_by_code = {row["stock_code"]: row for row in partial}
    assert partial_by_code["00005"]["connect_status"] == "BUY_ELIGIBLE"
    assert partial_by_code["00006"]["connect_status"] is None
    assert partial_by_code["00006"]["intelligence_scope"] == "EXCLUDED"


def test_hkex_parser_is_header_versioned_and_excludes_no_rows_implicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Sheet:
        nrows = 6
        ncols = 14
        rows = [
            ["Daily Share Repurchase Report"] + [""] * 13,
            ["Date Printed : 06/08/2026"] + [""] * 13,
            [""] * 14,
            [""] * 14,
            list(buyback.HKEX_HEADERS_14),
            [
                "Fixture Co",
                "5",
                "ORD",
                "2026/08/05",
                "1,000",
                "HKD 10.00",
                "HKD 9.80",
                "HKD 9,900",
                "Exchange",
                "10,000",
                "500",
                "500",
                "10,000",
                "0.1",
            ],
        ]

        def cell_value(self, row: int, column: int):  # noqa: ANN201
            return self.rows[row][column]

    class Book:
        def sheet_by_index(self, _index: int) -> Sheet:
            return Sheet()

    monkeypatch.setattr(buyback.xlrd, "open_workbook", lambda **_kwargs: Book())
    rows, _, printed = parse_hkex_report(
        b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1fixture"
    )

    assert printed == "2026-08-06"
    assert rows[0].stock_code == "00005"
    assert rows[0].shares == 1000
    assert rows[0].execution_venue == "HKEX"
    assert rows[0].treasury_shares == 500

    Sheet.rows[4][1] = "Unexpected code header"
    with pytest.raises(BuybackSourceError, match="BUYBACK_HKEX_HEADER_CHANGED"):
        parse_hkex_report(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1fixture")


def test_monitor_uses_cninfo_pdf_fallback_and_persists_unreviewed_candidate(
    tmp_path: Path,
) -> None:
    monitor, store = make_monitor(tmp_path, FixtureClient())
    batch = monitor.collect()
    run_id = store.start_run(monitor.monitor_id, started_at=NOW)
    status = store.finish_run(run_id, monitor.monitor_id, batch, completed_at=NOW)

    entities = store.latest_buyback_entities(monitor.monitor_id)
    sources = {item.source_key: item for item in store.buyback_source_states(monitor.monitor_id)}
    assert status == "PARTIAL"
    assert len(entities) == 1
    assert entities[0].payload["stock_code"] == "600000"
    assert entities[0].payload["review_status"] == "UNREVIEWED"
    assert entities[0].payload["document_source_label"] == "巨潮沪市公告索引"
    assert entities[0].payload["no_action_reason"] == "MANUAL_CONFIRMATION_REQUIRED"
    assert sources["connect-sh"].payload["codes"] == ["00005"]
    assert sources["connect-sz"].payload["codes"] == ["00005"]
    assert sources["a-share-documents"].payload["fallback_document_count"] == 1


def test_one_connect_source_failure_remains_local_and_does_not_drop_candidates(
    tmp_path: Path,
) -> None:
    monitor, _store = make_monitor(
        tmp_path,
        FixtureClient(fail_sse_connect=True),
    )

    batch = monitor.collect()
    states = {item.source_key: item for item in batch.buyback_source_observations}

    assert batch.buyback_revisions
    assert states["connect-sh"].status == "ERROR"
    assert states["connect-sz"].status == "SUCCESS"
    assert any(issue.scope == "connect-sh" for issue in batch.issues)


def test_hkex_execution_keeps_connect_eligibility_and_program_link_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monitor, _store = make_monitor(tmp_path, FixtureClient(hk_report=True))
    execution = HkexExecution(
        company="Fixture HK",
        stock_code="00005",
        share_class="ORD",
        trading_date="2026-08-05",
        shares=1000,
        high_price=10,
        low_price=9.8,
        amount=9900,
        currency="HKD",
        method="Exchange",
        execution_venue="HKEX",
        total_repurchased_shares=10000,
        cancellation_shares=500,
        treasury_shares=500,
        mandate_exchange_shares=10000,
        mandate_percentage=0.1,
        currency_consistent=True,
    )
    cross_market = HkexExecution(
        **{**execution.__dict__, "stock_code": "00006", "execution_venue": "SSE"}
    )
    monkeypatch.setattr(
        buyback,
        "parse_hkex_report",
        lambda _raw: ((execution, cross_market), "header-hash", "2026-08-06"),
    )

    batch = monitor.collect()
    hk_rows = [
        revision
        for revision in batch.buyback_revisions
        if revision.entity_type == "HKEX_EXECUTION"
    ]

    assert len(hk_rows) == 1
    assert hk_rows[0].payload["connect_status"] == "BUY_ELIGIBLE"
    assert hk_rows[0].payload["connect_route_label"] == "SH+SZ"
    assert hk_rows[0].payload["program_link_status"] == "UNLINKED_EXECUTION"
    assert hk_rows[0].payload["no_action_reason"] == "PROGRAM_LINK_UNCONFIRMED"


def test_public_client_opens_retry_after_backoff_without_sleeping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = Message()
    headers["Retry-After"] = "120"

    class ThrottledOpener:
        def open(self, request, timeout):  # noqa: ANN001, ANN201
            raise HTTPError(request.full_url, 429, "throttled", headers, None)

    client = BuybackPublicClient(
        timeout_seconds=1,
        opener=ThrottledOpener(),  # type: ignore[arg-type]
        now=lambda: NOW,
        sleeper=lambda _seconds: pytest.fail("429 must not block the collector"),
        random_uniform=lambda _left, _right: 0,
    )

    with pytest.raises(BuybackSourceError, match="BUYBACK_HTTP_THROTTLED_429"):
        client.request("https://example.com/data", max_bytes=100)
    with pytest.raises(BuybackSourceError, match="BUYBACK_BACKOFF_ACTIVE"):
        client.request("https://example.com/data", max_bytes=100)
