"""Derived repurchase metrics with explicit evidence and coverage boundaries.

The functions in this module are deliberately pure.  Collection code supplies
official disclosure facts and bounded public reference snapshots; the web
projection derives programme aggregates and a coverage-penalised research
indicator without writing a second source of truth.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
from typing import Any, Iterable
import unicodedata
from zoneinfo import ZoneInfo


class BuybackMetricError(ValueError):
    """A bounded reference payload failed its schema or value contract."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _finite_number(
    value: Any,
    *,
    positive: bool = False,
    allow_negative: bool = False,
) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(Decimal(str(value).replace(",", "")))
    except (InvalidOperation, ValueError):
        return None
    if (
        not math.isfinite(parsed)
        or (not allow_negative and parsed < 0)
        or (positive and parsed <= 0)
    ):
        return None
    return parsed


def _source_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed_date = date.fromisoformat(text[:10])
        except ValueError:
            return None
        parsed = datetime.combine(parsed_date, datetime.min.time())
    if parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_a_share_reference(
    raw: bytes,
    *,
    maximum_records: int = 500,
) -> tuple[tuple[dict[str, Any], ...], datetime | None, str]:
    """Normalize the bounded Eastmoney repurchase programme reference page."""

    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BuybackMetricError("BUYBACK_A_REFERENCE_JSON_INVALID") from None
    if not isinstance(payload, dict) or payload.get("success") is False:
        raise BuybackMetricError("BUYBACK_A_REFERENCE_SCHEMA_CHANGED")
    result = payload.get("result")
    values = result.get("data") if isinstance(result, dict) else None
    if not isinstance(values, list) or len(values) > maximum_records:
        raise BuybackMetricError("BUYBACK_A_REFERENCE_SCHEMA_CHANGED")

    records: list[dict[str, Any]] = []
    source_times: list[datetime] = []
    schema_keys: set[str] = set()
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            raise BuybackMetricError("BUYBACK_A_REFERENCE_SCHEMA_CHANGED")
        schema_keys.update(str(key) for key in value)
        code = str(value.get("DIM_SCODE") or "").strip()
        program_id = str(value.get("REPURCODE") or "").strip()
        if not re.fullmatch(r"\d{6}", code) or not re.fullmatch(r"\d{1,24}", program_id):
            continue
        identity = f"{code}:{program_id}"
        if identity in seen:
            raise BuybackMetricError("BUYBACK_A_REFERENCE_ID_DUPLICATED")
        seen.add(identity)
        updated_at = _source_datetime(value.get("UPD") or value.get("UPDATEDATE"))
        notice_at = _source_datetime(value.get("NOTICEDATE"))
        trade_at = _source_datetime(value.get("DIM_TRADEDATE"))
        if updated_at is not None:
            source_times.append(updated_at)
        record = {
            "program_id": program_id,
            "stock_code": code,
            "stock_name": str(value.get("SECURITYSHORTNAME") or "").strip(),
            "updated_at": updated_at.isoformat() if updated_at else None,
            "notice_at": notice_at.isoformat() if notice_at else None,
            "trade_date": trade_at.date().isoformat() if trade_at else None,
            "progress_code": str(value.get("REPURPROGRESS") or "").strip(),
            "plan_start_date": str(value.get("REPURSTARTDATE") or "")[:10] or None,
            "plan_end_date": str(value.get("REPURENDDATE") or "")[:10] or None,
            "plan_amount_lower": _finite_number(value.get("REPURAMOUNTLOWER")),
            "plan_amount_upper": _finite_number(value.get("REPURAMOUNTLIMIT")),
            "plan_price_cap": _finite_number(value.get("REPURPRICECAP")),
            "actual_shares": _finite_number(value.get("REPURNUM"), positive=True),
            "actual_amount": _finite_number(value.get("REPURAMOUNT"), positive=True),
            "actual_high": _finite_number(value.get("REPURPRICECAP1"), positive=True),
            "actual_low": _finite_number(value.get("REPURPRICELOWER1"), positive=True),
            "current_price": _finite_number(value.get("NEWPRICE"), positive=True),
            "market_cap": _finite_number(value.get("ZSZ"), positive=True),
        }
        records.append(record)
    records.sort(
        key=lambda item: (
            str(item.get("updated_at") or ""),
            str(item["stock_code"]),
            str(item["program_id"]),
        ),
        reverse=True,
    )
    return (
        tuple(records),
        max(source_times, default=None),
        _canonical_sha256(sorted(schema_keys)),
    )


def parse_market_reference(
    raw: bytes,
    *,
    expected_securities: Iterable[tuple[str, str, str]],
) -> tuple[tuple[dict[str, Any], ...], datetime | None, str]:
    """Normalize one bounded A/HK quote and financial snapshot."""

    market_ids = {"SH": "1", "SZ": "0", "HK": "116"}
    expected: dict[tuple[str, str], tuple[str, str]] = {}
    for market_scope, market, raw_code in expected_securities:
        market_id = market_ids.get(str(market))
        if market_id is None:
            continue
        width = 5 if market_scope == "HK" else 6
        code = str(raw_code).zfill(width)
        expected[(market_id, code)] = (str(market_scope), str(market))
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_JSON_INVALID") from None
    data = payload.get("data") if isinstance(payload, dict) else None
    values = data.get("diff") if isinstance(data, dict) else None
    if payload.get("rc") != 0 or not isinstance(values, list) or len(values) > 200:
        raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_SCHEMA_CHANGED")

    records: list[dict[str, Any]] = []
    source_times: list[datetime] = []
    schema_keys: set[str] = set()
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_SCHEMA_CHANGED")
        schema_keys.update(str(key) for key in value)
        market_id = str(value.get("f13") or "").strip()
        raw_code = str(value.get("f12") or "").strip()
        expected_value = expected.get((market_id, raw_code))
        if expected_value is None:
            expected_value = expected.get((market_id, raw_code.zfill(5)))
        if expected_value is None:
            expected_value = expected.get((market_id, raw_code.zfill(6)))
        if expected_value is None:
            continue
        market_scope, market = expected_value
        code = raw_code.zfill(5 if market_scope == "HK" else 6)
        identity = f"{market_scope}:{market}:{code}"
        if identity in seen:
            continue
        price = _finite_number(value.get("f2"), positive=True)
        timestamp = _finite_number(value.get("f124"), positive=True)
        if price is None or timestamp is None:
            continue
        try:
            updated_at = datetime.fromtimestamp(timestamp, tz=UTC)
        except (OverflowError, OSError, ValueError):
            continue
        seen.add(identity)
        source_times.append(updated_at)
        records.append(
            {
                "security_key": identity,
                "market_scope": market_scope,
                "market": market,
                "stock_code": code,
                "stock_name": str(value.get("f14") or "").strip(),
                "provider": "EASTMONEY",
                "current_price": price,
                "previous_close": _finite_number(value.get("f18"), positive=True),
                "change_percent": _finite_number(
                    value.get("f3"), allow_negative=True
                ),
                "market_cap": _finite_number(value.get("f20"), positive=True),
                "pe_ratio": _finite_number(value.get("f9"), allow_negative=True),
                "pb_ratio": _finite_number(value.get("f23"), allow_negative=True),
                "roe_percent": _finite_number(
                    value.get("f37"), allow_negative=True
                ),
                "revenue": _finite_number(value.get("f40")),
                "revenue_yoy_percent": _finite_number(
                    value.get("f41"), allow_negative=True
                ),
                "net_profit": _finite_number(
                    value.get("f45"), allow_negative=True
                ),
                "net_profit_yoy_percent": _finite_number(
                    value.get("f46"), allow_negative=True
                ),
                "gross_margin_percent": _finite_number(
                    value.get("f49"), allow_negative=True
                ),
                "updated_at": updated_at.isoformat(),
            }
        )
    if expected and not records:
        raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_EMPTY")
    records.sort(key=lambda item: str(item["security_key"]))
    return (
        tuple(records),
        max(source_times, default=None),
        _canonical_sha256(sorted(schema_keys)),
    )


def parse_tencent_market_reference(
    raw: bytes,
    *,
    expected_securities: Iterable[tuple[str, str, str]],
) -> tuple[tuple[dict[str, Any], ...], datetime | None, str]:
    """Normalize Tencent's bounded quote fallback without guessing finance fields.

    The public response is positional rather than named.  Only the stable quote
    fields used here are admitted: current/previous price, daily change, quote
    time and total market value (reported in CNY/HKD 100-million units).
    """

    prefixes = {"SH": "sh", "SZ": "sz", "HK": "hk"}
    expected: dict[tuple[str, str], tuple[str, str]] = {}
    for market_scope, market, raw_code in expected_securities:
        prefix = prefixes.get(str(market))
        if prefix is None:
            continue
        code = str(raw_code).zfill(5 if market_scope == "HK" else 6)
        expected[(prefix, code)] = (str(market_scope), str(market))
    try:
        text = raw.decode("gb18030")
    except UnicodeDecodeError:
        raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_TEXT_INVALID") from None

    records: list[dict[str, Any]] = []
    source_times: list[datetime] = []
    field_counts: set[int] = set()
    seen: set[str] = set()
    shanghai = ZoneInfo("Asia/Shanghai")
    for match in re.finditer(r'v_(sh|sz|hk)(\d+)="([^"]*)";', text):
        prefix, raw_code, raw_values = match.groups()
        width = 5 if prefix == "hk" else 6
        code = raw_code.zfill(width)
        expected_value = expected.get((prefix, code))
        if expected_value is None:
            continue
        fields = raw_values.split("~")
        field_counts.add(len(fields))
        if len(fields) <= 45:
            raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_SCHEMA_CHANGED")
        market_scope, market = expected_value
        identity = f"{market_scope}:{market}:{code}"
        if identity in seen:
            continue
        payload_code = str(fields[2] or "").strip().zfill(width)
        if payload_code != code:
            raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_SCHEMA_CHANGED")
        price = _finite_number(fields[3], positive=True)
        previous_close = _finite_number(fields[4], positive=True)
        timestamp_text = str(fields[30] or "").strip()
        if price is None or previous_close is None or not timestamp_text:
            continue
        timestamp_formats = (
            ("%Y/%m/%d %H:%M:%S", "%Y%m%d%H%M%S")
            if market_scope == "HK"
            else ("%Y%m%d%H%M%S", "%Y/%m/%d %H:%M:%S")
        )
        updated_at: datetime | None = None
        for timestamp_format in timestamp_formats:
            try:
                updated_at = datetime.strptime(timestamp_text, timestamp_format).replace(
                    tzinfo=shanghai
                )
            except ValueError:
                continue
            break
        if updated_at is None:
            continue
        updated_at = updated_at.astimezone(UTC)
        market_cap_units = _finite_number(fields[45], positive=True)
        market_cap = (
            market_cap_units * 100_000_000
            if market_cap_units is not None
            else None
        )
        seen.add(identity)
        source_times.append(updated_at)
        records.append(
            {
                "security_key": identity,
                "market_scope": market_scope,
                "market": market,
                "stock_code": code,
                "stock_name": str(fields[1] or "").strip(),
                "provider": "TENCENT",
                "current_price": price,
                "previous_close": previous_close,
                "change_percent": _finite_number(
                    fields[32], allow_negative=True
                ),
                "market_cap": market_cap,
                "pe_ratio": None,
                "pb_ratio": None,
                "roe_percent": None,
                "revenue": None,
                "revenue_yoy_percent": None,
                "net_profit": None,
                "net_profit_yoy_percent": None,
                "gross_margin_percent": None,
                "updated_at": updated_at.isoformat(),
            }
        )
    if expected and not records:
        raise BuybackMetricError("BUYBACK_MARKET_REFERENCE_EMPTY")
    records.sort(key=lambda item: str(item["security_key"]))
    return (
        tuple(records),
        max(source_times, default=None),
        _canonical_sha256(sorted(field_counts)),
    )


def parse_financial_reference(
    raw: bytes,
    *,
    expected_securities: Iterable[tuple[str, str, str]],
) -> tuple[tuple[dict[str, Any], ...], datetime | None, str]:
    """Normalize named A/HK financial fields from Eastmoney's F10 dataset.

    Profitability uses the latest full-year ROE so records are comparable across
    issuers.  Revenue and attributable-profit growth use the latest disclosed
    reporting period so the score still reacts to current operating momentum.
    """

    expected: dict[str, tuple[str, str, str]] = {}
    for market_scope, market, raw_code in expected_securities:
        code = str(raw_code).zfill(5 if market_scope == "HK" else 6)
        expected[f"{code}.{market}".upper()] = (
            str(market_scope),
            str(market),
            code,
        )
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BuybackMetricError("BUYBACK_FINANCIAL_REFERENCE_JSON_INVALID") from None
    result = payload.get("result") if isinstance(payload, dict) else None
    values = result.get("data") if isinstance(result, dict) else None
    pages = result.get("pages") if isinstance(result, dict) else None
    if (
        payload.get("code") != 0
        or payload.get("success") is False
        or not isinstance(values, list)
        or len(values) > 500
    ):
        raise BuybackMetricError("BUYBACK_FINANCIAL_REFERENCE_SCHEMA_CHANGED")
    if isinstance(pages, int) and pages > 1:
        raise BuybackMetricError("BUYBACK_FINANCIAL_REFERENCE_TRUNCATED")

    grouped: dict[str, list[dict[str, Any]]] = {}
    schema_keys: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            raise BuybackMetricError("BUYBACK_FINANCIAL_REFERENCE_SCHEMA_CHANGED")
        schema_keys.update(str(key) for key in value)
        secucode = str(value.get("SECUCODE") or "").strip().upper()
        if secucode not in expected:
            continue
        report_at = _source_datetime(value.get("REPORT_DATE"))
        if report_at is None:
            continue
        grouped.setdefault(secucode, []).append(value)

    records: list[dict[str, Any]] = []
    source_times: list[datetime] = []
    for secucode, candidates in grouped.items():
        market_scope, market, code = expected[secucode]
        candidates.sort(
            key=lambda item: _source_datetime(item.get("REPORT_DATE")) or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        latest = candidates[0]
        if market_scope == "HK":
            annual = next(
                (
                    item
                    for item in candidates
                    if str(item.get("DATE_TYPE_CODE") or "") == "001"
                ),
                None,
            )
            revenue_yoy = _finite_number(
                latest.get("OPERATE_INCOME_YOY"), allow_negative=True
            )
            net_profit_yoy = _finite_number(
                latest.get("HOLDER_PROFIT_YOY"), allow_negative=True
            )
            roe = (
                _finite_number(annual.get("ROE_AVG"), allow_negative=True)
                if annual is not None
                else None
            )
            if roe is None and annual is not None:
                roe = _finite_number(
                    annual.get("ROE_YEARLY"), allow_negative=True
                )
        else:
            annual = next(
                (
                    item
                    for item in candidates
                    if str(item.get("REPORT_TYPE") or "") == "年报"
                    or str(item.get("REPORT_DATE") or "")[5:10] == "12-31"
                ),
                None,
            )
            revenue_yoy = _finite_number(
                latest.get("TOTALOPERATEREVETZ"), allow_negative=True
            )
            net_profit_yoy = _finite_number(
                latest.get("PARENTNETPROFITTZ"), allow_negative=True
            )
            roe = (
                _finite_number(annual.get("ROEJQ"), allow_negative=True)
                if annual is not None
                else None
            )
        if roe is None and revenue_yoy is None and net_profit_yoy is None:
            continue
        report_at = _source_datetime(latest.get("REPORT_DATE"))
        roe_report_at = (
            _source_datetime(annual.get("REPORT_DATE"))
            if annual is not None
            else None
        )
        updated_at = (
            _source_datetime(latest.get("NOTICE_DATE"))
            or _source_datetime(latest.get("UPDATE_DATE"))
            or report_at
        )
        if updated_at is not None:
            source_times.append(updated_at)
        records.append(
            {
                "security_key": f"{market_scope}:{market}:{code}",
                "market_scope": market_scope,
                "market": market,
                "stock_code": code,
                "stock_name": str(
                    latest.get("SECURITY_NAME_ABBR") or ""
                ).strip(),
                "provider": "EASTMONEY_F10",
                "roe_percent": roe,
                "revenue_yoy_percent": revenue_yoy,
                "net_profit_yoy_percent": net_profit_yoy,
                "financial_report_date": (
                    report_at.date().isoformat() if report_at is not None else None
                ),
                "roe_report_date": (
                    roe_report_at.date().isoformat()
                    if roe_report_at is not None
                    else None
                ),
                "updated_at": updated_at.isoformat() if updated_at else None,
            }
        )
    if expected and not records:
        raise BuybackMetricError("BUYBACK_FINANCIAL_REFERENCE_EMPTY")
    records.sort(key=lambda item: str(item["security_key"]))
    return (
        tuple(records),
        max(source_times, default=None),
        _canonical_sha256(sorted(schema_keys)),
    )


def parse_hk_market_reference(
    raw: bytes,
    *,
    expected_codes: Iterable[str],
) -> tuple[tuple[dict[str, Any], ...], datetime | None, str]:
    """Backward-compatible HK-only wrapper used by source contract tests."""

    return parse_market_reference(
        raw,
        expected_securities=(
            ("HK", "HK", str(value).zfill(5)) for value in expected_codes
        ),
    )


def _normalized_document_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return re.sub(r"[\s,，]", "", normalized)


def _rounded_texts(value: float, scale: float) -> set[str]:
    scaled = value / scale
    results: set[str] = set()
    for places in (0, 1, 2, 3, 4, 6):
        fixed = f"{scaled:.{places}f}"
        rendered = fixed.rstrip("0").rstrip(".")
        if not rendered:
            rendered = "0"
        reconstructed = float(fixed) * scale
        tolerance = max(0.011, abs(value) * 0.000_01, scale * 0.000_001)
        if abs(reconstructed - value) <= tolerance:
            results.add(rendered)
            results.add(fixed)
    return results


def _contains_scaled_value(
    normalized_text: str,
    value: float,
    *,
    kind: str,
) -> bool:
    if kind == "shares":
        scales = ((1.0, "股"), (10_000.0, "万股"), (100_000_000.0, "亿股"))
    elif kind == "amount":
        scales = ((1.0, "元"), (10_000.0, "万元"), (100_000_000.0, "亿元"))
    elif kind == "price":
        scales = ((1.0, "元"),)
    else:
        raise ValueError("unsupported numeric evidence kind")
    for scale, suffix in scales:
        for rendered in _rounded_texts(value, scale):
            pattern = rf"(?<![\d.]){re.escape(rendered)}{suffix}"
            if re.search(pattern, normalized_text):
                return True
    return False


def match_a_share_program(
    document_text: str,
    *,
    stock_code: str,
    released_at: datetime,
    event_type: str,
    programmes: Iterable[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return one programme only when four execution values match the PDF."""

    if event_type not in {
        "FIRST_EXECUTION",
        "PROGRESS",
        "COMPLETION_OR_TERMINATION",
    }:
        return None
    normalized_text = _normalized_document_text(document_text)
    if not normalized_text:
        return None
    released_date = released_at.date()
    matches: list[dict[str, Any]] = []
    for programme in programmes:
        if str(programme.get("stock_code") or "") != stock_code:
            continue
        reference_time = _source_datetime(
            programme.get("notice_at") or programme.get("updated_at")
        )
        if reference_time is None or abs((reference_time.date() - released_date).days) > 3:
            continue
        shares = _finite_number(programme.get("actual_shares"), positive=True)
        amount = _finite_number(programme.get("actual_amount"), positive=True)
        high = _finite_number(programme.get("actual_high"), positive=True)
        low = _finite_number(programme.get("actual_low"), positive=True)
        if None in {shares, amount, high, low}:
            continue
        assert shares is not None and amount is not None
        assert high is not None and low is not None
        average = amount / shares
        if not low * 0.995 <= average <= high * 1.005:
            continue
        if not all(
            (
                _contains_scaled_value(normalized_text, shares, kind="shares"),
                _contains_scaled_value(normalized_text, amount, kind="amount"),
                _contains_scaled_value(normalized_text, high, kind="price"),
                _contains_scaled_value(normalized_text, low, kind="price"),
            )
        ):
            continue
        enriched = {
            "program_reference_id": str(programme["program_id"]),
            "shares": shares,
            "amount": amount,
            "currency": "CNY",
            "high_price": high,
            "low_price": low,
            "average_cost": average,
            "numeric_fact_scope": "PROGRAMME_CUMULATIVE",
        }
        for key in ("plan_amount_lower", "plan_amount_upper"):
            plan_amount = _finite_number(programme.get(key), positive=True)
            if plan_amount is not None and _contains_scaled_value(
                normalized_text,
                plan_amount,
                kind="amount",
            ):
                enriched[key] = plan_amount
        matches.append(enriched)
    return matches[0] if len(matches) == 1 else None


def _compact_number(value: float | None) -> str | None:
    if value is None or not math.isfinite(value):
        return None
    absolute = abs(value)
    if absolute >= 100_000_000:
        rendered, suffix = value / 100_000_000, "亿"
    elif absolute >= 10_000:
        rendered, suffix = value / 10_000, "万"
    else:
        rendered, suffix = value, ""
    return f"{rendered:,.2f}".rstrip("0").rstrip(".") + suffix


def _money_label(currency: str | None, value: float | None) -> str | None:
    rendered = _compact_number(value)
    if rendered is None:
        return None
    return f"{currency or ''} {rendered}".strip()


def _price_label(currency: str | None, value: float | None) -> str | None:
    if value is None or not math.isfinite(value):
        return None
    return f"{currency or ''} {value:,.3f}".rstrip("0").rstrip(".")


def _parse_effective_date(row: dict[str, Any]) -> date | None:
    raw = row.get("trading_date") or row.get("effective_date") or row.get("effective_at")
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _interpolate(value: float, anchors: tuple[tuple[float, float], ...]) -> float:
    if value <= anchors[0][0]:
        return anchors[0][1]
    for (left_x, left_y), (right_x, right_y) in zip(anchors, anchors[1:]):
        if value <= right_x:
            ratio = (value - left_x) / (right_x - left_x)
            return left_y + ratio * (right_y - left_y)
    return anchors[-1][1]


def _score_payload(
    features: list[dict[str, Any]],
    *,
    block_label: str | None = None,
) -> dict[str, Any]:
    components = [
        {
            "name": str(feature["name"]),
            "weight": float(feature["weight"]),
            "reliability": float(feature.get("reliability", 0.0)),
            "score": feature.get("score"),
            "detail": str(feature.get("detail") or ""),
            "available": (
                feature.get("score") is not None
                and float(feature.get("reliability", 0.0)) > 0
            ),
        }
        for feature in features
    ]
    if block_label is not None:
        return {
            "attractiveness_score": None,
            "attractiveness_level": "BLOCKED",
            "attractiveness_label": block_label,
            "attractiveness_coverage_percent": 0.0,
            "attractiveness_summary": block_label,
            "attractiveness_explanation": block_label,
            "attractiveness_components": components,
            "attractiveness_method_version": "BUYBACK_PERFORMANCE_V2",
        }
    available = [
        feature
        for feature in features
        if feature.get("score") is not None and feature.get("reliability", 0) > 0
    ]
    total_weight = sum(float(feature["weight"]) for feature in features)
    reliable_weight = sum(
        float(feature["weight"]) * float(feature["reliability"])
        for feature in available
    )
    coverage = reliable_weight / total_weight if total_weight else 0.0
    if coverage < 0.35 or len(available) < 2:
        return {
            "attractiveness_score": None,
            "attractiveness_level": "INSUFFICIENT",
            "attractiveness_label": "数据不足",
            "attractiveness_coverage_percent": round(coverage * 100, 1),
            "attractiveness_summary": "当前可计算信息不足",
            "attractiveness_explanation": (
                f"可用输入覆盖 {coverage * 100:.1f}%，未达到形成指标所需条件。"
            ),
            "attractiveness_components": components,
            "attractiveness_method_version": "BUYBACK_PERFORMANCE_V2",
        }
    weighted_edge = sum(
        float(feature["weight"])
        * float(feature["reliability"])
        * float(feature["score"])
        for feature in available
    ) / reliable_weight
    score = max(0.0, min(100.0, 50.0 + 50.0 * weighted_edge * coverage))
    if score >= 70:
        level, label = "HIGH", "较高"
    elif score >= 55:
        level, label = "MEDIUM", "中等"
    else:
        level, label = "LOW", "较低"
    ranked = sorted(
        available,
        key=lambda feature: abs(
            float(feature["weight"])
            * float(feature["reliability"])
            * float(feature["score"])
        ),
        reverse=True,
    )
    drivers = [str(feature["detail"]) for feature in ranked[:2]]
    names = "、".join(str(feature["name"]) for feature in available)
    return {
        "attractiveness_score": round(score, 1),
        "attractiveness_level": level,
        "attractiveness_label": label,
        "attractiveness_coverage_percent": round(coverage * 100, 1),
        "attractiveness_summary": " · ".join(drivers),
        "attractiveness_explanation": (
            f"由{names}计算；缺失输入按覆盖度降权，当前覆盖 {coverage * 100:.1f}%。"
        ),
        "attractiveness_components": components,
        "attractiveness_method_version": "BUYBACK_PERFORMANCE_V2",
    }


def _freshness_feature(effective_date: date | None, *, now: datetime) -> dict[str, Any]:
    if effective_date is None:
        return {
            "name": "披露时效",
            "weight": 10.0,
            "reliability": 0.0,
            "score": None,
            "detail": "披露日期缺失",
        }
    age_days = max(0, (now.date() - effective_date).days)
    return {
        "name": "披露时效",
        "weight": 10.0,
        "reliability": 1.0,
        "score": max(-1.0, min(1.0, 2.0 * math.exp(-age_days / 10.0) - 1.0)),
        "detail": "今日披露" if age_days == 0 else f"披露距今 {age_days} 天",
    }


def _price_feature(
    average_cost: float | None,
    current_price: float | None,
    *,
    reliability: float,
) -> tuple[dict[str, Any], float | None, str | None]:
    if average_cost is None or current_price is None or average_cost <= 0 or current_price <= 0:
        return (
            {
                "name": "现价与回购均价",
                "weight": 20.0,
                "reliability": 0.0,
                "score": None,
                "detail": "现价或回购均价缺失",
            },
            None,
            None,
        )
    difference = (current_price / average_cost - 1.0) * 100.0
    if difference < 0:
        detail = f"现价低于回购均价 {abs(difference):.1f}%"
    elif difference > 0:
        detail = f"现价高于回购均价 {difference:.1f}%"
    else:
        detail = "现价与回购均价持平"
    return (
        {
            "name": "现价与回购均价",
            "weight": 20.0,
            "reliability": reliability,
            "score": max(-1.0, min(1.0, math.log(average_cost / current_price) / 0.25)),
            "detail": detail,
        },
        difference,
        detail,
    )


def _fundamental_features(
    quote: dict[str, Any],
    *,
    reliability: float,
) -> list[dict[str, Any]]:
    roe = _finite_number(quote.get("roe_percent"), allow_negative=True)
    revenue_growth = _finite_number(
        quote.get("revenue_yoy_percent"), allow_negative=True
    )
    profit_growth = _finite_number(
        quote.get("net_profit_yoy_percent"), allow_negative=True
    )
    return [
        {
            "name": "净资产收益率",
            "weight": 12.0,
            "reliability": reliability if roe is not None else 0.0,
            "score": (
                _interpolate(
                    roe,
                    (
                        (-20.0, -1.0),
                        (0.0, -0.4),
                        (5.0, 0.0),
                        (10.0, 0.3),
                        (15.0, 0.6),
                        (25.0, 1.0),
                    ),
                )
                if roe is not None
                else None
            ),
            "detail": f"净资产收益率 {roe:.1f}%" if roe is not None else "净资产收益率缺失",
        },
        {
            "name": "营业收入同比",
            "weight": 8.0,
            "reliability": reliability if revenue_growth is not None else 0.0,
            "score": (
                _interpolate(
                    revenue_growth,
                    (
                        (-30.0, -1.0),
                        (-10.0, -0.5),
                        (0.0, 0.0),
                        (10.0, 0.3),
                        (25.0, 0.7),
                        (50.0, 1.0),
                    ),
                )
                if revenue_growth is not None
                else None
            ),
            "detail": (
                f"营业收入同比 {revenue_growth:+.1f}%"
                if revenue_growth is not None
                else "营业收入同比缺失"
            ),
        },
        {
            "name": "净利润同比",
            "weight": 10.0,
            "reliability": reliability if profit_growth is not None else 0.0,
            "score": (
                _interpolate(
                    profit_growth,
                    (
                        (-50.0, -1.0),
                        (-20.0, -0.6),
                        (0.0, 0.0),
                        (20.0, 0.4),
                        (50.0, 0.8),
                        (100.0, 1.0),
                    ),
                )
                if profit_growth is not None
                else None
            ),
            "detail": (
                f"净利润同比 {profit_growth:+.1f}%"
                if profit_growth is not None
                else "净利润同比缺失"
            ),
        },
    ]


def _is_recent_reference(record: dict[str, Any], *, now: datetime) -> bool:
    raw = record.get("updated_at") or record.get("trade_date")
    parsed = _source_datetime(raw)
    if parsed is None and record.get("trade_date"):
        try:
            parsed_date = date.fromisoformat(str(record["trade_date"])[:10])
        except ValueError:
            return False
        parsed = datetime.combine(parsed_date, datetime.min.time(), tzinfo=UTC)
    return parsed is not None and now - timedelta(days=7) <= parsed <= now + timedelta(minutes=5)


def _enrich_a_share_row(
    row: dict[str, Any],
    *,
    programme_map: dict[str, dict[str, Any]],
    quote: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    enriched = dict(row)
    missing = dict(enriched.get("missing_reasons") or {})
    shares = _finite_number(enriched.get("shares"), positive=True)
    amount = _finite_number(enriched.get("amount"), positive=True)
    currency = str(enriched.get("currency") or "CNY") if amount is not None else None
    average_cost = amount / shares if shares and amount else None
    if average_cost is not None:
        low = _finite_number(enriched.get("low_price"), positive=True)
        high = _finite_number(enriched.get("high_price"), positive=True)
        if low is not None and high is not None and not low * 0.995 <= average_cost <= high * 1.005:
            average_cost = None

    programme = programme_map.get(str(enriched.get("program_reference_id") or ""), {})
    reference_recent = bool(programme) and _is_recent_reference(programme, now=now)
    quote_recent = bool(quote) and _is_recent_reference(quote, now=now)
    financial_current = quote.get("financial_reference_current") is True
    current_price = (
        _finite_number(quote.get("current_price"), positive=True)
        if quote_recent
        else _finite_number(programme.get("current_price"), positive=True)
        if reference_recent
        else None
    )
    market_cap = (
        _finite_number(quote.get("market_cap"), positive=True)
        if quote_recent
        else _finite_number(programme.get("market_cap"), positive=True)
        if reference_recent
        else None
    )
    daily_change = (
        _finite_number(quote.get("change_percent"), allow_negative=True)
        if quote_recent
        else None
    )
    execution_days = 1 if enriched.get("event_type") == "FIRST_EXECUTION" and amount else None
    execution_label = "1天" if execution_days else None
    if execution_days is None:
        missing["execution_days_value"] = "A股公告不披露逐日成交明细，不能推算实际执行天数。"

    cumulative_parts: list[str] = []
    share_label = _compact_number(shares)
    amount_label = _money_label(currency, amount)
    if share_label is not None:
        cumulative_parts.append(f"{share_label}股")
    if amount_label is not None:
        cumulative_parts.append(amount_label)
    cumulative_label = " · ".join(cumulative_parts) or None
    if amount is None:
        missing["cumulative_amount"] = "公告中的累计回购金额尚未形成可计算字段。"
    if average_cost is None:
        missing["average_cost"] = "累计股数或金额缺失，无法估算回购均价。"

    amount_yield = amount / market_cap * 100.0 if amount and market_cap else None
    plan_floor = _finite_number(enriched.get("plan_amount_lower"), positive=True)
    completion = min(100.0, amount / plan_floor * 100.0) if amount and plan_floor else None
    price_feature, price_difference, price_detail = _price_feature(
        average_cost,
        current_price,
        reliability=0.75,
    )
    yield_feature = {
        "name": "实际回购占市值",
        "weight": 25.0,
        "reliability": 0.8 if amount_yield is not None else 0.0,
        "score": (
            _interpolate(
                amount_yield,
                ((0.0, 0.0), (0.1, 0.1), (0.5, 0.5), (1.0, 0.8), (2.0, 1.0)),
            )
            if amount_yield is not None
            else None
        ),
        "detail": (
            f"累计回购约占市值 {amount_yield:.2f}%"
            if amount_yield is not None
            else "市值或累计金额缺失"
        ),
    }
    completion_feature = {
        "name": "计划执行进度",
        "weight": 15.0,
        "reliability": 0.9 if completion is not None else 0.0,
        "score": completion / 100.0 if completion is not None else None,
        "detail": (
            f"累计金额达到计划下限 {completion:.1f}%"
            if completion is not None
            else "计划金额下限缺失"
        ),
    }
    fundamentals = _fundamental_features(
        quote if financial_current else {},
        reliability=0.75,
    )
    score = _score_payload(
        [
            yield_feature,
            price_feature,
            completion_feature,
            *fundamentals,
            _freshness_feature(_parse_effective_date(enriched), now=now),
        ]
    )
    if score["attractiveness_score"] is None:
        missing["attractiveness_score"] = str(score["attractiveness_explanation"])
    enriched.update(
        {
            "connect_status": "BUY_ELIGIBLE",
            "connect_status_label": "可购买",
            "connect_route_label": "A股",
            "connect_quality": "CURRENT",
            "execution_days_value": execution_days,
            "execution_days_label": execution_label,
            "execution_days_scope": "FIRST_EXECUTION" if execution_days else "UNAVAILABLE",
            "cumulative_shares": shares,
            "cumulative_shares_label": (
                f"{_compact_number(shares)}股" if shares is not None else None
            ),
            "cumulative_amount": amount,
            "cumulative_amount_label": _money_label(currency, amount),
            "cumulative_label": cumulative_label,
            "cumulative_context": "本轮累计" if cumulative_label else None,
            "average_cost": average_cost,
            "average_cost_label": _price_label(currency, average_cost),
            "average_cost_scope_label": "累计金额 ÷ 累计股数" if average_cost else None,
            "current_price": current_price,
            "current_price_label": _price_label(currency, current_price),
            "daily_change_percent": daily_change,
            "price_vs_average_percent": price_difference,
            "price_position_label": price_detail,
            "actual_amount_yield_percent": amount_yield,
            "completion_to_plan_floor_percent": completion,
            "roe_percent": (
                _finite_number(quote.get("roe_percent"), allow_negative=True)
                if financial_current
                else None
            ),
            "revenue_yoy_percent": (
                _finite_number(
                    quote.get("revenue_yoy_percent"), allow_negative=True
                )
                if financial_current
                else None
            ),
            "net_profit_yoy_percent": (
                _finite_number(
                    quote.get("net_profit_yoy_percent"), allow_negative=True
                )
                if financial_current
                else None
            ),
            "financial_metric_scope": "ANNUAL_ROE_AND_LATEST_PERIOD_GROWTH",
            "financial_report_date": quote.get("financial_report_date"),
            "roe_report_date": quote.get("roe_report_date"),
            "market_reference_at": quote.get("updated_at") if quote_recent else None,
            "financial_reference_at": quote.get("financial_updated_at"),
            "missing_reasons": missing,
            **score,
        }
    )
    for key, label in (
        ("daily_change_percent", "最新涨跌幅"),
        ("roe_percent", "净资产收益率"),
        ("revenue_yoy_percent", "营业收入同比"),
        ("net_profit_yoy_percent", "净利润同比"),
    ):
        if enriched.get(key) is None:
            missing[key] = f"{label}当前没有可用的公开行情或财务值。"
    return enriched


def _approximately_equal(left: float, right: float) -> bool:
    return abs(left - right) <= max(1.0, abs(left), abs(right)) * 0.000_001


def _current_connect_eligibility(
    stock_code: str,
    *,
    source_payloads: dict[str, dict[str, Any]],
    source_statuses: dict[str, str],
) -> dict[str, str] | None:
    """Resolve a binary current result, or withhold it when absence is unproven."""

    current_routes: list[str] = []
    complete_source_count = 0
    for source_key, route in (("connect-sh", "SH"), ("connect-sz", "SZ")):
        if source_statuses.get(source_key) != "SUCCESS":
            continue
        codes_value = source_payloads.get(source_key, {}).get("codes")
        if not isinstance(codes_value, list):
            continue
        complete_source_count += 1
        codes = {
            str(value).zfill(5)
            for value in codes_value
            if isinstance(value, (str, int))
        }
        if stock_code in codes:
            current_routes.append(route)

    if current_routes:
        return {
            "connect_status": "BUY_ELIGIBLE",
            "connect_status_label": "可购买",
            "connect_route_label": "+".join(current_routes),
            "connect_quality": "CURRENT",
        }
    if complete_source_count == 2:
        return {
            "connect_status": "NOT_BUY_ELIGIBLE",
            "connect_status_label": "不可购买",
            "connect_route_label": "—",
            "connect_quality": "CURRENT",
        }
    return None


def _aggregate_hk_group(
    rows: list[dict[str, Any]],
    *,
    quote: dict[str, Any],
    connect_eligibility: dict[str, str] | None,
    source_statuses: dict[str, str],
    now: datetime,
) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda item: (
            _parse_effective_date(item) or date.min,
            str(item.get("entity_key") or ""),
        ),
    )
    daily_groups: dict[date, list[dict[str, Any]]] = {}
    seen_daily_facts: set[tuple[Any, ...]] = set()
    for item in ordered:
        effective_date = _parse_effective_date(item)
        if effective_date is not None:
            fact_identity = (
                effective_date,
                item.get("document_sha256") or item.get("source_document_id"),
                item.get("shares"),
                item.get("amount"),
                item.get("currency"),
                item.get("mandate_exchange_shares"),
            )
            if fact_identity in seen_daily_facts:
                continue
            seen_daily_facts.add(fact_identity)
            daily_groups.setdefault(effective_date, []).append(item)
    daily: list[dict[str, Any]] = []
    for effective_date, values in sorted(daily_groups.items()):
        share_values = [
            _finite_number(item.get("shares"), positive=True) for item in values
        ]
        amount_values = [
            _finite_number(item.get("amount"), positive=True) for item in values
        ]
        currencies = {
            str(item.get("currency")) for item in values if item.get("currency")
        }
        cumulative_values = [
            value
            for value in (
                _finite_number(item.get("mandate_exchange_shares"), positive=True)
                for item in values
            )
            if value is not None
        ]
        daily.append(
            {
                "date": effective_date,
                "rows": values,
                "latest": values[-1],
                "shares": (
                    sum(float(value) for value in share_values if value is not None)
                    if all(value is not None for value in share_values)
                    else None
                ),
                "amount": (
                    sum(float(value) for value in amount_values if value is not None)
                    if all(value is not None for value in amount_values)
                    and len(currencies) == 1
                    else None
                ),
                "currency": next(iter(currencies)) if len(currencies) == 1 else None,
                "cumulative": max(cumulative_values, default=None),
            }
        )
    if not daily:
        latest = dict(ordered[-1])
        daily = [{
            "date": _parse_effective_date(latest),
            "rows": [latest],
            "latest": latest,
            "shares": None,
            "amount": None,
            "currency": None,
            "cumulative": None,
        }]

    start = 0
    previous_cumulative: float | None = None
    for index, item in enumerate(daily):
        cumulative = item["cumulative"]
        daily_shares = item["shares"]
        begins_new_programme = bool(
            index > 0
            and cumulative is not None
            and daily_shares is not None
            and _approximately_equal(float(cumulative), float(daily_shares))
        )
        cumulative_reset = bool(
            previous_cumulative is not None
            and cumulative is not None
            and cumulative < previous_cumulative
            and not _approximately_equal(float(cumulative), previous_cumulative)
        )
        if begins_new_programme or cumulative_reset:
            start = index
        if cumulative is not None:
            previous_cumulative = float(cumulative)

    segment = daily[start:]
    latest = dict(segment[-1]["latest"])
    latest_cumulative = segment[-1]["cumulative"]
    chain_complete = bool(
        latest_cumulative is not None
        and segment[0]["cumulative"] is not None
        and segment[0]["shares"] is not None
        and _approximately_equal(
            float(segment[0]["cumulative"]), float(segment[0]["shares"])
        )
        and all(item["shares"] is not None for item in segment)
        and all(item["amount"] is not None for item in segment)
        and all(item["cumulative"] is not None for item in segment)
    )
    if chain_complete:
        for previous, current in zip(segment, segment[1:]):
            delta = float(current["cumulative"]) - float(previous["cumulative"])
            if not _approximately_equal(delta, float(current["shares"])):
                chain_complete = False
                break
    source_key = str(latest.get("source_key") or "")
    if source_statuses.get(source_key) != "SUCCESS":
        chain_complete = False
    currency_values = {
        str(item["currency"]) for item in segment if item["currency"]
    }
    if len(currency_values) != 1:
        chain_complete = False
    currency = next(iter(currency_values), str(latest.get("currency") or "HKD"))
    execution_days = len(segment)
    latest_date = segment[-1]["date"]
    recent_start = latest_date - timedelta(days=6) if latest_date else date.min
    recent = [item for item in segment if (item["date"] or date.min) >= recent_start]
    recent_shares = sum(float(item["shares"]) for item in recent if item["shares"] is not None)
    recent_amount = sum(float(item["amount"]) for item in recent if item["amount"] is not None)
    recent_days = len(recent)
    full_amount = (
        sum(float(item["amount"]) for item in segment)
        if chain_complete
        else None
    )
    average_cost = (
        full_amount / float(latest_cumulative)
        if full_amount and latest_cumulative
        else None
    )
    recent_average_cost = (
        recent_amount / recent_shares if recent_amount and recent_shares else None
    )
    shares_label = _compact_number(
        float(latest_cumulative) if latest_cumulative is not None else None
    )

    quote_recent = bool(quote) and _is_recent_reference(quote, now=now)
    financial_current = quote.get("financial_reference_current") is True
    current_price = (
        _finite_number(quote.get("current_price"), positive=True)
        if quote_recent
        else None
    )
    daily_change = (
        _finite_number(quote.get("change_percent"), allow_negative=True)
        if quote_recent
        else None
    )
    market_cap = (
        _finite_number(quote.get("market_cap"), positive=True)
        if quote_recent
        else None
    )
    price_feature, price_difference, price_detail = _price_feature(
        average_cost,
        current_price,
        reliability=0.75,
    )
    recent_price_difference = (
        (current_price / recent_average_cost - 1.0) * 100.0
        if current_price and recent_average_cost
        else None
    )
    amount_yield = (
        full_amount / market_cap * 100.0 if full_amount and market_cap else None
    )
    yield_feature = {
        "name": "实际回购占市值",
        "weight": 25.0,
        "reliability": 0.8 if amount_yield is not None else 0.0,
        "score": (
            _interpolate(
                amount_yield,
                ((0.0, 0.0), (0.1, 0.1), (0.5, 0.5), (1.0, 0.8), (2.0, 1.0)),
            )
            if amount_yield is not None
            else None
        ),
        "detail": (
            f"累计回购约占市值 {amount_yield:.2f}%"
            if amount_yield is not None
            else "本轮累计金额或市值缺失"
        ),
    }
    cadence_feature = {
        "name": "近7日执行频度",
        "weight": 15.0,
        "reliability": 1.0 if recent_days else 0.0,
        "score": min(1.0, recent_days / 5.0) if recent_days else None,
        "detail": f"近7日实际回购 {recent_days} 天" if recent_days else "近7日无执行记录",
    }
    fundamentals = _fundamental_features(
        quote if financial_current else {},
        reliability=0.75,
    )
    connect_status = (
        str(connect_eligibility["connect_status"])
        if connect_eligibility is not None
        else None
    )
    block_label = None
    if connect_status == "NOT_BUY_ELIGIBLE":
        block_label = "不可购买"
    elif connect_status is None:
        block_label = "沪深港股通名单未完整取得"
    score = _score_payload(
        [
            yield_feature,
            price_feature,
            cadence_feature,
            *fundamentals,
            _freshness_feature(latest_date, now=now),
        ],
        block_label=block_label,
    )
    missing = dict(latest.get("missing_reasons") or {})
    if not chain_complete:
        missing["cumulative_amount"] = (
            "已取得的日报历史尚未覆盖本轮起点；近7日金额已单独标注，不作为本轮累计金额。"
        )
    if average_cost is None:
        missing["average_cost"] = (
            "本轮日报链条尚未完整，近7日均价已单独标注，不作为本轮回购均价。"
        )
    if score["attractiveness_score"] is None:
        missing["attractiveness_score"] = str(score["attractiveness_explanation"])
    if execution_days:
        execution_label = f"{execution_days}天" if chain_complete else f"至少{execution_days}天"
    else:
        execution_label = None
        missing["execution_days_value"] = "没有可计算的实际回购日期。"
    latest.update(
        {
            **(
                connect_eligibility
                if connect_eligibility is not None
                else {
                    "connect_status": None,
                    "connect_status_label": None,
                    "connect_route_label": None,
                    "connect_quality": "UNAVAILABLE",
                    "intelligence_scope": "EXCLUDED",
                }
            ),
            "entity_rollup_count": sum(len(item["rows"]) for item in segment),
            "execution_days_value": execution_days or None,
            "execution_days_label": execution_label,
            "execution_days_scope": "FULL_PROGRAMME" if chain_complete else "LOWER_BOUND",
            "cumulative_shares": latest_cumulative,
            "cumulative_shares_label": f"{shares_label}股" if shares_label else None,
            "cumulative_amount": full_amount,
            "cumulative_amount_label": _money_label(currency, full_amount),
            "recent_amount": recent_amount or None,
            "recent_amount_label": _money_label(
                currency, recent_amount if recent_amount else None
            ),
            "recent_execution_days": recent_days,
            "cumulative_label": " · ".join(
                value
                for value in (
                    f"{shares_label}股" if shares_label else None,
                    _money_label(currency, full_amount),
                )
                if value
            ) or None,
            "cumulative_context": None if chain_complete else "金额历史尚未完整",
            "average_cost": average_cost,
            "average_cost_label": _price_label(currency, average_cost),
            "average_cost_scope_label": (
                "累计金额 ÷ 累计股数" if average_cost is not None else None
            ),
            "recent_average_cost": recent_average_cost,
            "recent_average_cost_label": _price_label(currency, recent_average_cost),
            "current_price": current_price,
            "current_price_label": _price_label(currency, current_price),
            "daily_change_percent": daily_change,
            "price_vs_average_percent": price_difference,
            "recent_price_vs_average_percent": recent_price_difference,
            "price_position_label": price_detail,
            "actual_amount_yield_percent": amount_yield,
            "roe_percent": (
                _finite_number(quote.get("roe_percent"), allow_negative=True)
                if financial_current
                else None
            ),
            "revenue_yoy_percent": (
                _finite_number(
                    quote.get("revenue_yoy_percent"), allow_negative=True
                )
                if financial_current
                else None
            ),
            "net_profit_yoy_percent": (
                _finite_number(
                    quote.get("net_profit_yoy_percent"), allow_negative=True
                )
                if financial_current
                else None
            ),
            "financial_metric_scope": "ANNUAL_ROE_AND_LATEST_PERIOD_GROWTH",
            "financial_report_date": quote.get("financial_report_date"),
            "roe_report_date": quote.get("roe_report_date"),
            "market_reference_at": quote.get("updated_at") if quote_recent else None,
            "financial_reference_at": quote.get("financial_updated_at"),
            "programme_history_complete": chain_complete,
            "missing_reasons": missing,
            "scale_label": " · ".join(
                value
                for value in (
                    f"{shares_label}股" if shares_label else None,
                    _money_label(currency, full_amount),
                )
                if value
            ) or latest.get("scale_label"),
            "scale_status": "COMPLETE" if chain_complete else "PARTIAL",
            "intelligence_summary": (
                f"本轮累计回购 {shares_label}股，近7日执行 {recent_days} 天"
                if shares_label
                else f"近7日实际回购 {recent_days} 天"
            ),
            **score,
        }
    )
    for key, label in (
        ("daily_change_percent", "最新涨跌幅"),
        ("actual_amount_yield_percent", "回购金额占市值"),
        ("roe_percent", "净资产收益率"),
        ("revenue_yoy_percent", "营业收入同比"),
        ("net_profit_yoy_percent", "净利润同比"),
    ):
        if latest.get(key) is None:
            missing[key] = f"{label}当前没有可用的完整口径值。"
    return latest


def project_buyback_metrics(
    rows: Iterable[dict[str, Any]],
    *,
    source_payloads: dict[str, dict[str, Any]],
    source_statuses: dict[str, str],
    now: datetime,
) -> list[dict[str, Any]]:
    """Add row-level metrics and collapse HK daily rows to current programmes."""

    programmes = source_payloads.get("a-share-buyback-reference", {}).get("programmes")
    programme_map = {
        str(value.get("program_id")): value
        for value in programmes
        if isinstance(value, dict) and value.get("program_id")
    } if isinstance(programmes, list) else {}
    market_payload = source_payloads.get("hk-market-reference", {})
    market_reference_current = bool(
        source_statuses.get("hk-market-reference") in {"SUCCESS", "PARTIAL"}
        and market_payload.get("schema_version") in {2, 3}
    )
    quotes = market_payload.get("quotes") if market_reference_current else None
    quote_map: dict[str, dict[str, Any]] = {}
    if isinstance(quotes, list):
        for value in quotes:
            if not isinstance(value, dict) or not value.get("stock_code"):
                continue
            security_key = value.get("security_key")
            if security_key:
                quote_map[str(security_key)] = value
            else:
                quote_map[str(value["stock_code"])] = value

    financial_payload = source_payloads.get("buyback-financial-reference", {})
    financial_reference_current = bool(
        source_statuses.get("buyback-financial-reference")
        in {"SUCCESS", "PARTIAL"}
        and financial_payload.get("schema_version") == 1
    )
    financials = (
        financial_payload.get("financials")
        if financial_reference_current
        else None
    )
    if isinstance(financials, list):
        for value in financials:
            if not isinstance(value, dict) or not value.get("security_key"):
                continue
            security_key = str(value["security_key"])
            combined = dict(quote_map.get(security_key, {}))
            for key in (
                "roe_percent",
                "revenue_yoy_percent",
                "net_profit_yoy_percent",
                "financial_report_date",
                "roe_report_date",
            ):
                combined[key] = value.get(key)
            combined["financial_updated_at"] = value.get("updated_at")
            combined["financial_reference_current"] = True
            combined.setdefault("security_key", security_key)
            combined.setdefault("stock_code", value.get("stock_code"))
            quote_map[security_key] = combined

    a_rows: list[dict[str, Any]] = []
    hk_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        if row.get("market_scope") == "HK":
            key = (
                str(row.get("stock_code") or ""),
                str(row.get("share_class") or ""),
            )
            hk_groups.setdefault(key, []).append(row)
        else:
            security_key = (
                f"{row.get('market_scope')}:{row.get('market')}:{row.get('stock_code')}"
            )
            a_rows.append(
                _enrich_a_share_row(
                    row,
                    programme_map=programme_map,
                    quote=quote_map.get(security_key, quote_map.get(str(row.get("stock_code")), {})),
                    now=now,
                )
            )
    result = a_rows + [
        _aggregate_hk_group(
            values,
            quote=quote_map.get(f"HK:HK:{key[0]}", quote_map.get(key[0], {})),
            connect_eligibility=_current_connect_eligibility(
                key[0],
                source_payloads=source_payloads,
                source_statuses=source_statuses,
            ),
            source_statuses=source_statuses,
            now=now,
        )
        for key, values in hk_groups.items()
    ]

    def ordering(row: dict[str, Any]) -> tuple[int, float, float]:
        score = _finite_number(row.get("attractiveness_score"))
        effective = _source_datetime(row.get("effective_at"))
        return (
            0 if score is not None else 1,
            -(score or 0.0),
            -(effective.timestamp() if effective is not None else 0.0),
        )

    return sorted(result, key=ordering)


__all__ = [
    "BuybackMetricError",
    "match_a_share_program",
    "parse_a_share_reference",
    "parse_hk_market_reference",
    "parse_market_reference",
    "parse_tencent_market_reference",
    "parse_financial_reference",
    "project_buyback_metrics",
]
