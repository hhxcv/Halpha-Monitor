"""Read-only FastAPI surface for registered monitor data."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, time as datetime_time, timedelta
import math
from pathlib import Path
import threading
from typing import Any, Literal
import unicodedata
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from halpha_monitor.contracts import (
    AutomaticCollectionMonitor,
    AutomaticCollectionState,
    ConfigurableMonitor,
    NetworkObservableMonitor,
    RegisteredMonitor,
    ViewColumn,
)
from halpha_monitor.buyback_metrics import project_buyback_metrics
from halpha_monitor.monitors.a_hk_buyback import (
    classify_buyback_attention,
    classify_buyback_title,
    is_target_a_share_security,
)
from halpha_monitor.service import (
    DEFAULT_OBSERVATION_LEASE_SECONDS,
    MonitorRegistry,
    MonitorScheduler,
)
from halpha_monitor.store import (
    SQLiteMonitorStore,
    StoredForwardEvaluation,
    StoredBuybackEntity,
    StoredBuybackReview,
    StoredBuybackSourceState,
    StoredIssue,
    StoredRun,
    StoredSample,
    iso_utc,
    utc_now,
)


MonitorStatus = Literal[
    "HEALTHY",
    "PARTIAL",
    "FAILED",
    "STALE",
    "RUNNING",
    "UNKNOWN",
    "DISABLED",
]

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
NEW_YORK_TZ = ZoneInfo("America/New_York")
ALLOWED_WINDOWS = (1, 3, 6, 12, 24, 72, 168, 336, 720)
EXPECTED_ABSENCE_REASON_CODES = frozenset({"NO_ELIGIBLE_C2C_AD"})
REQUEST_WINDOW_SECONDS = 60.0
BUYBACK_MONITOR_ID = "a-hk-buyback"
MARKET_EVENT_MONITOR_ID = "market-event-calendar"


class ConfigurationRequest(BaseModel):
    values: dict[str, Any]


class ControlRequest(BaseModel):
    enabled: bool


class BuybackReviewRequest(BaseModel):
    base_revision_no: int = Field(ge=1)
    decision: Literal[
        "CONFIRMED_EVENT",
        "REJECTED_EVENT",
        "NEEDS_FOLLOW_UP",
    ]
    corrected_event_type: (
        Literal[
            "PLAN_OR_APPROVAL",
            "FIRST_EXECUTION",
            "PROGRESS",
            "MODIFICATION",
            "COMPLETION_OR_TERMINATION",
            "POST_BUYBACK_CANCELLATION",
            "POST_BUYBACK_DISPOSAL",
            "AMBIGUOUS_BUYBACK",
            "HKEX_EXECUTION",
        ]
        | None
    ) = None
    program_key: str | None = Field(default=None, max_length=120)
    program_status: (
        Literal[
            "PROPOSED",
            "APPROVED",
            "ACTIVE",
            "COMPLETED",
            "TERMINATED",
            "UNKNOWN",
        ]
        | None
    ) = None
    note: str = Field(default="", max_length=1000)


def monitor_status(
    run: StoredRun | None,
    *,
    interval_seconds: float,
    now: datetime,
    enabled: bool = True,
) -> MonitorStatus:
    if not enabled:
        return "DISABLED"
    if run is None:
        return "UNKNOWN"
    if run.status == "RUNNING":
        return "RUNNING"
    if run.completed_at is None:
        return "UNKNOWN"
    stale_after = max(interval_seconds * 2.5, 90)
    if (now - run.completed_at).total_seconds() > stale_after:
        return "STALE"
    if run.status == "FAILED":
        return "FAILED"
    if run.status == "PARTIAL":
        return "PARTIAL"
    return "HEALTHY"


def _run_payload(run: StoredRun | None) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "run_id": run.run_id,
        "started_at": iso_utc(run.started_at),
        "completed_at": iso_utc(run.completed_at) if run.completed_at else None,
        "status": run.status,
        "sample_count": run.sample_count,
        "error_code": run.error_code,
        "duration_seconds": (
            round((run.completed_at - run.started_at).total_seconds(), 3)
            if run.completed_at is not None
            else None
        ),
    }


def _issue_payload(issue: StoredIssue) -> dict[str, Any]:
    expected_absence = issue.reason_code in EXPECTED_ABSENCE_REASON_CODES
    return {
        "issue_id": issue.issue_id,
        "run_id": issue.run_id,
        "occurred_at": iso_utc(issue.occurred_at),
        "scope": issue.scope,
        "reason_code": issue.reason_code,
        "classification": "EXPECTED_ABSENCE" if expected_absence else "DATA_ISSUE",
        "tone": "NOTICE" if expected_absence else "WARNING",
    }


def _operational_status(
    latest: StoredRun | None,
    *,
    enabled: bool,
) -> dict[str, str]:
    """Describe scheduler intent separately from data age or collection quality."""

    if not enabled:
        return {
            "kind": "DISABLED",
            "tone": "DISABLED",
            "label": "已关闭",
        }
    if latest is not None and latest.status == "RUNNING":
        return {
            "kind": "COLLECTING",
            "tone": "ACTIVE",
            "label": "采集中",
        }
    return {
        "kind": "MONITORING",
        "tone": "ACTIVE",
        "label": "监控中",
    }


def _data_status(
    latest: StoredRun | None,
    data_run: StoredRun | None,
    *,
    interval_seconds: float,
    now: datetime,
    user_can_configure: bool,
    enabled: bool,
    latest_issues: tuple[StoredIssue, ...],
) -> dict[str, str]:
    if not enabled:
        if data_run is None or data_run.completed_at is None:
            return {
                "kind": "DISABLED_EMPTY",
                "tone": "DISABLED",
                "label": "尚无采集记录",
                "detail": ("监控当前未开启；没有历史结果，也没有使用任何替代值。"),
                "cutoff_label": "采集完成",
            }
        return {
            "kind": "DISABLED_WITH_HISTORY",
            "tone": "DISABLED",
            "label": "历史快照",
            "detail": (
                "最后一次已校验结果按其截止时间如实保留；关闭后的空档"
                "属于未采集时段，不代表故障或当前状态。"
            ),
            "cutoff_label": "历史采集完成",
        }
    recovery = (
        "系统会自动重试；如缺失持续，可调整页面采集条件。"
        if user_can_configure
        else "无需用户处理，系统会自动重试。"
    )
    if data_run is None or data_run.completed_at is None:
        collecting = latest is not None and latest.status == "RUNNING"
        return {
            "kind": "EMPTY",
            "tone": "RUNNING" if collecting else "UNKNOWN",
            "label": "等待首轮采集结果" if collecting else "尚无采集结果",
            "detail": (
                "尚未产生通过校验的结果；未使用任何替代值，无需用户处理。"
                if collecting
                else f"没有通过校验的采集结果；未使用任何替代值。{recovery}"
            ),
            "cutoff_label": "采集完成",
        }

    stale_after = max(interval_seconds * 2.5, 90)
    if (now - data_run.completed_at).total_seconds() > stale_after:
        return {
            "kind": "STALE",
            "tone": "STALE",
            "label": "历史快照 · 尚未刷新",
            "detail": (
                "表格中的值仍是其截止时间对应的已校验历史事实，"
                f"但不代表当前状态。{recovery}"
            ),
            "cutoff_label": "历史采集完成",
        }

    if latest is not None and latest.run_id == data_run.run_id:
        if latest.status == "PARTIAL":
            expected_absence_only = bool(latest_issues) and all(
                issue.reason_code in EXPECTED_ABSENCE_REASON_CODES
                for issue in latest_issues
            )
            return {
                "kind": (
                    "CURRENT_WITH_NOTICES"
                    if expected_absence_only
                    else "CURRENT_WITH_GAPS"
                ),
                "tone": "HEALTHY" if expected_absence_only else "PARTIAL",
                "label": "最新采集已完成",
                "detail": ("本轮未取得结果的具体范围已在对应数据表内标记。"),
                "cutoff_label": "最近采集完成",
            }
        if latest.status == "SUCCESS":
            return {
                "kind": "CURRENT",
                "tone": "HEALTHY",
                "label": "最新采集已完成",
                "detail": "展示字段均已通过校验，并对应所示截止时间。",
                "cutoff_label": "最近采集完成",
            }

    if latest is not None and latest.status == "RUNNING":
        return {
            "kind": "COLLECTING_PREVIOUS",
            "tone": "RUNNING",
            "label": "正在刷新 · 显示上一轮结果",
            "detail": (
                "新一轮尚未完成；表格显示上一轮已通过校验的结果，无需用户处理。"
            ),
            "cutoff_label": "上一轮采集完成",
        }

    return {
        "kind": "HISTORICAL",
        "tone": "STALE",
        "label": "历史快照 · 本轮无新结果",
        "detail": (
            "表格中的值仍是其截止时间对应的已校验历史事实，"
            f"但本轮没有新结果，不代表当前状态。{recovery}"
        ),
        "cutoff_label": "历史采集完成",
    }


def _monitor_summary(
    monitor: RegisteredMonitor,
    store: SQLiteMonitorStore,
    *,
    now: datetime,
    scheduler: MonitorScheduler | None = None,
) -> dict[str, Any]:
    cadence = _collection_cadence_payload(monitor, scheduler=scheduler)
    effective_interval_seconds = float(cadence["effective_interval_seconds"])
    latest = store.latest_run(monitor.monitor_id)
    projection_kind = getattr(monitor, "projection_kind", None)
    is_buyback = projection_kind == "buyback"
    is_snapshot = projection_kind in {"buyback", "market_events", "btc_intelligence"}
    data_run = (
        store.latest_completed_run(monitor.monitor_id)
        if is_snapshot
        else store.latest_sample_run(monitor.monitor_id)
    )
    latest_issues = store.issues_for_run(latest.run_id) if latest is not None else ()
    control = store.load_control(monitor.monitor_id)
    if control is None:
        control = store.ensure_control(
            monitor.monitor_id,
            default_enabled=bool(getattr(monitor, "default_enabled", True)),
        )
    automatic_state = _automatic_collection_state(
        monitor,
        scheduler=scheduler,
        now=now,
    )
    data_status = _data_status(
        latest,
        data_run,
        interval_seconds=effective_interval_seconds,
        now=now,
        user_can_configure=isinstance(monitor, ConfigurableMonitor),
        enabled=control.enabled,
        latest_issues=latest_issues,
    )
    if is_buyback and data_status["kind"] in {
        "CURRENT",
        "CURRENT_WITH_NOTICES",
        "CURRENT_WITH_GAPS",
    }:
        data_status = {
            **data_status,
            "label": "最近来源检查已完成",
            "detail": "公开来源最近一轮检查已结束。",
            "cutoff_label": "最近来源检查",
        }
    if projection_kind == "market_events" and data_status["kind"] in {
        "CURRENT",
        "CURRENT_WITH_NOTICES",
        "CURRENT_WITH_GAPS",
    }:
        data_status = {
            **data_status,
            "label": "最近事件检查已完成",
            "detail": "宏观发布与央行日历最近一轮检查已结束。",
            "cutoff_label": "最近事件检查",
        }
    if projection_kind == "btc_intelligence" and data_status["kind"] in {
        "CURRENT",
        "CURRENT_WITH_NOTICES",
        "CURRENT_WITH_GAPS",
    }:
        data_status = {
            **data_status,
            "label": "BTC 情报快照已更新",
            "detail": "月频、日频、4h 与聪明钱来源已按各自截止时间投影。",
            "cutoff_label": "情报快照",
        }
    operational_status = _operational_status(latest, enabled=control.enabled)
    if (
        control.enabled
        and automatic_state is not None
        and not (latest is not None and latest.status == "RUNNING")
    ):
        if automatic_state.status == "CLOSED":
            operational_status = {
                "kind": "SCHEDULED_IDLE",
                "tone": "IDLE",
                "label": "已收市 · 静态",
            }
            if data_run is not None:
                data_status = {
                    "kind": "STATIC_CLOSED",
                    "tone": "HEALTHY",
                    "label": "收市后静态历史数据",
                    "detail": (
                        "闭市期间不会自动采集；展示最近已提交的历史事实，"
                        "可使用手动刷新显式检查一次。"
                    ),
                    "cutoff_label": "最近来源检查",
                }
        elif automatic_state.status == "UNAVAILABLE":
            operational_status = {
                "kind": "SCHEDULE_BLOCKED",
                "tone": "WARNING",
                "label": "自动刷新已暂停",
            }
    return {
        "monitor_id": monitor.monitor_id,
        "display_name": monitor.display_name,
        "description": monitor.description,
        "interval_seconds": monitor.interval_seconds,
        "collection_cadence": cadence,
        "enabled": control.enabled,
        "control_updated_at": iso_utc(control.updated_at),
        "status": monitor_status(
            latest,
            interval_seconds=effective_interval_seconds,
            now=now,
            enabled=control.enabled,
        ),
        "latest_run": _run_payload(latest),
        "data_run": _run_payload(data_run),
        "operational_status": operational_status,
        "data_status": data_status,
        "automatic_collection": _automatic_collection_payload(automatic_state),
    }


def _collection_cadence_payload(
    monitor: RegisteredMonitor,
    *,
    scheduler: MonitorScheduler | None,
) -> dict[str, Any]:
    if scheduler is not None:
        cadence = scheduler.collection_cadence(monitor.monitor_id)
        return {
            "adaptive": cadence.foreground_interval_seconds is not None,
            "background_interval_seconds": cadence.background_interval_seconds,
            "foreground_interval_seconds": cadence.foreground_interval_seconds,
            "effective_interval_seconds": cadence.effective_interval_seconds,
            "foreground_active": cadence.foreground_active,
        }
    background_interval = float(monitor.interval_seconds)
    raw_foreground_interval = getattr(
        monitor,
        "foreground_interval_seconds",
        None,
    )
    foreground_interval = (
        float(raw_foreground_interval) if raw_foreground_interval is not None else None
    )
    return {
        "adaptive": foreground_interval is not None,
        "background_interval_seconds": background_interval,
        "foreground_interval_seconds": foreground_interval,
        "effective_interval_seconds": background_interval,
        "foreground_active": False,
    }


def _automatic_collection_state(
    monitor: RegisteredMonitor,
    *,
    scheduler: MonitorScheduler | None,
    now: datetime,
) -> AutomaticCollectionState | None:
    if scheduler is not None:
        return scheduler.automatic_collection_state(monitor.monitor_id, now=now)
    if not isinstance(monitor, AutomaticCollectionMonitor):
        return None
    try:
        return monitor.automatic_collection_state(now=now)
    except Exception as exc:
        return AutomaticCollectionState(
            allowed=False,
            status="UNAVAILABLE",
            reason_code=f"AUTOMATIC_SCHEDULE_FAILED_{type(exc).__name__.upper()}",
            label="交易日历不可判定 · 自动刷新暂停",
            detail="无法确认交易时段；当前只允许手动刷新。",
        )


def _automatic_collection_payload(
    state: AutomaticCollectionState | None,
) -> dict[str, Any] | None:
    if state is None:
        return None
    return {
        "allowed": state.allowed,
        "status": state.status,
        "reason_code": state.reason_code,
        "label": state.label,
        "detail": state.detail,
        "next_open_at": (
            iso_utc(state.next_open_at) if state.next_open_at is not None else None
        ),
        "active_until": (
            iso_utc(state.active_until) if state.active_until is not None else None
        ),
    }


def _overall_data_status(summaries: list[dict[str, Any]]) -> tuple[str, str]:
    active = [item for item in summaries if bool(item["enabled"])]
    if not active:
        return "DISABLED", "监控均已关闭"
    if any(str(item["operational_status"]["kind"]) == "COLLECTING" for item in active):
        return "RUNNING", "采集中"
    if (
        active
        and all(
            item.get("automatic_collection", {}).get("status") == "CLOSED"
            for item in active
            if item.get("automatic_collection") is not None
        )
        and all(item.get("automatic_collection") is not None for item in active)
    ):
        return "IDLE", "闭市静态"
    kinds = [str(item["data_status"]["kind"]) for item in active]
    if kinds and all(kind == "EMPTY" for kind in kinds):
        return "RUNNING", "监控已启动"
    return "HEALTHY", "监控运行中"


def _payload_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _run_work_seconds(
    run: dict[str, Any] | None,
    *,
    now: datetime,
) -> float:
    if run is None:
        return 0.0
    duration = run.get("duration_seconds")
    if isinstance(duration, (int, float)) and math.isfinite(float(duration)):
        return max(0.0, float(duration))
    if run.get("status") != "RUNNING":
        return 0.0
    started_at = _payload_time(str(run.get("started_at") or ""))
    if started_at is None:
        return 0.0
    return max(0.0, (now - started_at).total_seconds())


def _collection_load(
    monitors: tuple[RegisteredMonitor, ...],
    summaries: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    by_id = {str(item["monitor_id"]): item for item in summaries}
    active = [
        (monitor, by_id[monitor.monitor_id])
        for monitor in monitors
        if bool(by_id[monitor.monitor_id]["enabled"])
    ]
    automatically_running = [
        (monitor, summary)
        for monitor, summary in active
        if summary.get("automatic_collection") is None
        or bool(summary["automatic_collection"].get("allowed"))
    ]
    automatically_running_ids = {
        monitor.monitor_id for monitor, _summary in automatically_running
    }
    utilization = 0.0
    latest_completed_at: datetime | None = None
    collecting_count = 0
    request_count = 0
    measured_monitors = 0
    for monitor, summary in active:
        cadence = summary.get("collection_cadence") or {}
        effective_interval_seconds = float(
            cadence.get("effective_interval_seconds", monitor.interval_seconds)
        )
        latest = summary.get("latest_run")
        data_run = summary.get("data_run")
        if latest and latest.get("status") == "RUNNING":
            collecting_count += 1
            work_seconds = max(
                _run_work_seconds(latest, now=now),
                _run_work_seconds(data_run, now=now),
            )
        else:
            work_seconds = _run_work_seconds(latest, now=now)
        if monitor.monitor_id in automatically_running_ids or (
            latest and latest.get("status") == "RUNNING"
        ):
            utilization += work_seconds / max(effective_interval_seconds, 1.0)

        for run in (latest, data_run):
            if not run:
                continue
            completed_at = _payload_time(run.get("completed_at"))
            if completed_at is not None and (
                latest_completed_at is None or completed_at > latest_completed_at
            ):
                latest_completed_at = completed_at

        if isinstance(monitor, NetworkObservableMonitor):
            try:
                measured = monitor.network_request_count(
                    window_seconds=REQUEST_WINDOW_SECONDS
                )
            except (RuntimeError, TypeError, ValueError):
                measured = None
            if measured is not None and measured >= 0:
                measured_monitors += 1
                request_count += int(measured)

    if not active:
        level, level_label = "IDLE", "空闲"
    elif utilization < 0.25:
        level, level_label = "LOW", "低"
    elif utilization < 0.75:
        level, level_label = "MEDIUM", "中"
    else:
        level, level_label = "HIGH", "高"
    measurement = (
        "FULL"
        if active and measured_monitors == len(active)
        else "PARTIAL"
        if measured_monitors
        else "NONE"
    )
    return {
        "level": level,
        "level_label": level_label,
        "utilization_percent": round(utilization * 100),
        "enabled_count": len(active),
        "collecting_count": collecting_count,
        "planned_runs_per_minute": round(
            sum(
                60.0
                / float(
                    (summary.get("collection_cadence") or {}).get(
                        "effective_interval_seconds",
                        monitor.interval_seconds,
                    )
                )
                for monitor, summary in automatically_running
            ),
            2,
        ),
        "network_requests": (request_count if measured_monitors else None),
        "request_window_seconds": int(REQUEST_WINDOW_SECONDS),
        "request_measurement": measurement,
        "measured_monitor_count": measured_monitors,
        "latest_completed_at": (
            iso_utc(latest_completed_at) if latest_completed_at is not None else None
        ),
        "definition": (
            "负载占用为各启用监控最近或当前一轮耗时除以各自采集周期之和；"
            "低于 25% 为低，25% 至 74% 为中，75% 及以上为高；"
            "请求数为本进程近 60 秒实际发出的公开 HTTP 请求。"
        ),
    }


def _history_payload(
    history: tuple[StoredSample, ...],
    *,
    interval_seconds: float,
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    threshold_seconds = max(interval_seconds * 2.5, 90)
    valid_history: list[StoredSample] = []
    for sample in history:
        try:
            numeric_value = float(sample.value_text)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric_value):
            valid_history.append(sample)

    points: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    segment = 0
    previous: StoredSample | None = None
    for sample in valid_history:
        if previous is not None:
            elapsed = (sample.observed_at - previous.observed_at).total_seconds()
            if elapsed > threshold_seconds:
                gaps.append(
                    {
                        "started_at": iso_utc(previous.observed_at),
                        "ended_at": iso_utc(sample.observed_at),
                        "duration_seconds": round(elapsed, 3),
                        "open": False,
                        "label": "未采集时段",
                    }
                )
                segment += 1
        points.append(
            {
                "observed_at": iso_utc(sample.observed_at),
                "value": sample.value_text,
                "unit": sample.unit,
                "segment": segment,
            }
        )
        previous = sample
    if previous is not None:
        elapsed = (now - previous.observed_at).total_seconds()
        if elapsed > threshold_seconds:
            gaps.append(
                {
                    "started_at": iso_utc(previous.observed_at),
                    "ended_at": iso_utc(now),
                    "duration_seconds": round(elapsed, 3),
                    "open": True,
                    "label": "未采集至今",
                }
            )
    return points, gaps


def _sample_payload(sample: StoredSample) -> dict[str, Any]:
    return {
        **sample.payload,
        "series_key": sample.series_key,
        "entity_key": sample.entity_key,
        "observed_at": iso_utc(sample.observed_at),
        "value": sample.value_text,
        "unit": sample.unit,
    }


def _btc_intelligence_projection(
    samples: tuple[StoredSample, ...],
    store: SQLiteMonitorStore,
    monitor_id: str,
) -> dict[str, Any] | None:
    snapshot = next(
        (
            sample
            for sample in samples
            if sample.payload.get("row_type") == "BTC_INTELLIGENCE"
        ),
        None,
    )
    if snapshot is None:
        return None
    history = store.btc_structure_history(monitor_id)
    structure_algorithm_version = (
        history.algorithm_version if history is not None else None
    )
    revisions = store.latest_btc_structure_event_revisions(
        monitor_id,
        limit=20,
        algorithm_version=structure_algorithm_version,
    )
    events = []
    for revision in revisions:
        signal = revision.payload.get("signal")
        outcome = revision.payload.get("outcome")
        events.append(
            {
                "event_key": revision.event_key,
                "event_at": iso_utc(revision.event_at),
                "observed_at": iso_utc(revision.observed_at),
                "state": revision.state,
                "revision_no": revision.revision_no,
                "signal": signal if isinstance(signal, dict) else None,
                "outcome": outcome if isinstance(outcome, dict) else None,
            }
        )
    monthly_history = store.btc_monthly_research_history(monitor_id)
    monthly_ledger: dict[str, Any] | None = None
    if monthly_history is not None:
        monthly_revisions = store.latest_btc_monthly_research_revisions(
            monitor_id,
            limit=12,
            algorithm_version=monthly_history.algorithm_version,
        )
        monthly_ledger = {
            **store.btc_monthly_research_summary(
                monitor_id,
                algorithm_version=monthly_history.algorithm_version,
            ),
            "started_at": iso_utc(monthly_history.started_at),
            "processed_through_at": iso_utc(monthly_history.processed_through_at),
            "algorithm_version": monthly_history.algorithm_version,
            "history_policy": "FORWARD_ONLY",
            "records": [
                {
                    "signal_key": revision.signal_key,
                    "signal_at": iso_utc(revision.signal_at),
                    "observed_at": iso_utc(revision.observed_at),
                    "state": revision.state,
                    "revision_no": revision.revision_no,
                    "signal": revision.payload.get("signal"),
                    "execution": revision.payload.get("execution"),
                }
                for revision in monthly_revisions
            ],
        }
    return {
        **snapshot.payload,
        "sample_observed_at": iso_utc(snapshot.observed_at),
        "monthly_ledger": monthly_ledger,
        "ledger": {
            **store.btc_structure_event_summary(
                monitor_id,
                algorithm_version=structure_algorithm_version,
            ),
            "started_at": iso_utc(history.started_at) if history is not None else None,
            "processed_through_at": (
                iso_utc(history.processed_through_at) if history is not None else None
            ),
            "algorithm_version": (
                history.algorithm_version if history is not None else None
            ),
            "events": events,
            "history_policy": "FORWARD_ONLY",
            "retention_days": store.btc_structure_retention_days,
            "maximum_events": store.btc_structure_max_events,
        },
    }


BUYBACK_EVENT_LABELS = {
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
BUYBACK_REVIEW_LABELS = {
    "UNREVIEWED": "尚无人工校正",
    "CONFIRMED_EVENT": "人工校正确认",
    "REJECTED_EVENT": "人工校正排除",
    "NEEDS_FOLLOW_UP": "人工标记待补全",
}
BUYBACK_SYSTEM_VERIFIED_A_EVENTS = frozenset(
    {
        "PLAN_OR_APPROVAL",
        "FIRST_EXECUTION",
        "PROGRESS",
        "MODIFICATION",
        "COMPLETION_OR_TERMINATION",
        "POST_BUYBACK_CANCELLATION",
        "POST_BUYBACK_DISPOSAL",
    }
)
BUYBACK_EXECUTION_EVENTS = frozenset({"FIRST_EXECUTION", "PROGRESS", "HKEX_EXECUTION"})


def _normalize_buyback_stock_query(value: str) -> str:
    return "".join(unicodedata.normalize("NFKC", value).casefold().split())


def _buyback_stock_matches(payload: dict[str, Any], query: str) -> bool:
    normalized_query = _normalize_buyback_stock_query(query)
    if not normalized_query:
        return True
    return any(
        normalized_query in _normalize_buyback_stock_query(str(value or ""))
        for value in (payload.get("stock_code"), payload.get("issuer_name"))
    )


def _row_matches_filters(
    payload: dict[str, Any],
    selected_filters: dict[str, str | list[str]],
) -> bool:
    for key, selected in selected_filters.items():
        current = str(payload.get(key, ""))
        if isinstance(selected, list):
            if current not in selected:
                return False
        elif selected != "*" and current != selected:
            return False
    return True


MARKET_EVENT_MARKET_LABELS = {
    "CRYPTO": "加密资产",
    "US_STOCKS": "美股",
    "A_HK_STOCKS": "A股 / 港股",
}


def _normalize_event_query(value: str) -> str:
    return "".join(unicodedata.normalize("NFKC", value).casefold().split())


def _market_event_exact_time(payload: dict[str, Any]) -> datetime | None:
    value = payload.get("scheduled_at")
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _market_event_date(payload: dict[str, Any]) -> date | None:
    try:
        return date.fromisoformat(str(payload.get("scheduled_date") or ""))
    except ValueError:
        return None


def _market_event_sort_time(payload: dict[str, Any]) -> datetime | None:
    exact = _market_event_exact_time(payload)
    if exact is not None:
        return exact
    scheduled_date = _market_event_date(payload)
    if scheduled_date is None:
        return None
    return datetime.combine(
        scheduled_date,
        datetime_time(hour=12),
        tzinfo=NEW_YORK_TZ,
    ).astimezone(UTC)


def _market_event_is_upcoming(payload: dict[str, Any], *, now: datetime) -> bool:
    exact = _market_event_exact_time(payload)
    if exact is not None:
        return exact >= now
    scheduled_date = _market_event_date(payload)
    return (
        scheduled_date is not None
        and scheduled_date >= now.astimezone(NEW_YORK_TZ).date()
    )


def _market_event_within_days(
    payload: dict[str, Any],
    *,
    now: datetime,
    days: int,
) -> bool:
    exact = _market_event_exact_time(payload)
    if exact is not None:
        return now <= exact <= now + timedelta(days=days)
    scheduled_date = _market_event_date(payload)
    if scheduled_date is None:
        return False
    today = now.astimezone(NEW_YORK_TZ).date()
    return today <= scheduled_date <= today + timedelta(days=days)


def _market_event_time_matches(
    payload: dict[str, Any],
    selected: str,
    *,
    now: datetime,
) -> bool:
    if not _market_event_is_upcoming(payload, now=now):
        return False
    if selected == "NEXT_24H":
        exact = _market_event_exact_time(payload)
        if exact is not None:
            return exact <= now + timedelta(hours=24)
        scheduled_date = _market_event_date(payload)
        today = now.astimezone(NEW_YORK_TZ).date()
        return scheduled_date is not None and scheduled_date <= today + timedelta(
            days=1
        )
    if selected == "NEXT_7D":
        return _market_event_within_days(payload, now=now, days=7)
    if selected == "NEXT_30D":
        return _market_event_within_days(payload, now=now, days=30)
    return selected == "ALL_UPCOMING"


def _market_event_matches(
    payload: dict[str, Any],
    selected_filters: dict[str, str | list[str]],
    query: str,
    *,
    now: datetime,
) -> bool:
    normalized_query = _normalize_event_query(query)
    if normalized_query and not any(
        normalized_query in _normalize_event_query(str(value or ""))
        for value in (
            payload.get("event_title"),
            payload.get("category_label"),
            payload.get("source_label"),
        )
    ):
        return False
    for key, selected_value in selected_filters.items():
        selected = str(selected_value)
        if key == "time_range":
            if not _market_event_time_matches(payload, selected, now=now):
                return False
        elif key == "affected_market":
            if selected != "*" and selected not in payload.get("market_scopes", []):
                return False
        elif selected != "*" and str(payload.get(key) or "") != selected:
            return False
    return True


def _market_event_countdown(payload: dict[str, Any], *, now: datetime) -> str:
    exact = _market_event_exact_time(payload)
    if exact is None:
        scheduled_date = _market_event_date(payload)
        if scheduled_date is None:
            return "时间待公布"
        days = (scheduled_date - now.astimezone(NEW_YORK_TZ).date()).days
        if days <= 0:
            return "今天 · 时间待公布"
        if days == 1:
            return "明天 · 时间待公布"
        return f"{days}天后 · 时间待公布"
    seconds = (exact - now).total_seconds()
    if seconds < 0:
        return str(payload.get("release_state_label") or "已发生")
    if seconds < 3600:
        return f"{max(1, math.ceil(seconds / 60))}分钟后"
    if seconds < 24 * 3600:
        return f"{math.ceil(seconds / 3600)}小时后"
    if seconds < 48 * 3600:
        return "明天"
    return f"{math.ceil(seconds / 86400)}天后"


def _market_event_recent_change(payload: dict[str, Any], *, now: datetime) -> bool:
    changed_at = _payload_time(str(payload.get("last_schedule_changed_at") or ""))
    return changed_at is not None and changed_at >= now - timedelta(days=7)


def _market_event_priority(
    payload: dict[str, Any],
    *,
    now: datetime,
) -> tuple[int, str, str]:
    high = payload.get("importance") == "HIGH"
    changed = _market_event_recent_change(payload, now=now)
    exact = _market_event_exact_time(payload)
    if exact is not None:
        hours = max(0.0, (exact - now).total_seconds() / 3600)
        if high and hours <= 24:
            return 1, "立即准备", "高影响事件将在24小时内发布。"
        if (high and hours <= 72) or (not high and hours <= 24):
            return 2, "提前准备", "发布时间已进入需要预留风险空间的窗口。"
        if changed and hours <= 7 * 24:
            return 2, "时间有调整", "发布时间最近发生调整，需更新交易计划。"
        if high and hours <= 7 * 24:
            return 3, "本周关注", "高影响事件将在未来7天内发布。"
    else:
        scheduled_date = _market_event_date(payload)
        if scheduled_date is not None:
            days = (scheduled_date - now.astimezone(NEW_YORK_TZ).date()).days
            if changed and days <= 7:
                return 2, "时间有调整", "官方日期最近发生调整，具体时间仍待公布。"
            if high and days <= 2:
                return 2, "提前准备", "高影响事件日期临近，具体发布时间尚未公布。"
            if high and days <= 7:
                return 3, "本周关注", "高影响事件将在未来7天内发生。"
    return 4, "日历关注", "尚未进入优先准备窗口。"


def _market_event_enriched(
    payload: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    rank, label, reason = _market_event_priority(payload, now=now)
    exact = _market_event_exact_time(payload)
    scheduled_date = _market_event_date(payload)
    local_date = (
        exact.astimezone(SHANGHAI_TZ).date() if exact is not None else scheduled_date
    )
    scopes = [
        str(value)
        for value in payload.get("market_scopes", [])
        if str(value) in MARKET_EVENT_MARKET_LABELS
    ]
    return {
        **payload,
        "priority_rank": rank,
        "priority_label": label,
        "priority_reason": reason,
        "countdown_label": _market_event_countdown(payload, now=now),
        "importance_label": ("高" if payload.get("importance") == "HIGH" else "中"),
        "markets_label": "、".join(
            MARKET_EVENT_MARKET_LABELS[scope] for scope in scopes
        ),
        "calendar_date": local_date.isoformat() if local_date is not None else None,
        "calendar_timezone_label": (
            "北京时间" if exact is not None else "美国东部日期"
        ),
        "schedule_changed_recently": _market_event_recent_change(payload, now=now),
    }


def _market_event_coverage_messages(
    issues: tuple[StoredIssue, ...],
) -> list[str]:
    scopes = {issue.scope for issue in issues}
    messages: list[str] = []
    if "bea-schedule" in scopes:
        messages.append("美国GDP、PCE与贸易发布日程暂时无法更新")
    if any(scope.startswith("nyfed-calendar:") for scope in scopes):
        messages.append("部分美国经济指标月份暂时无法更新")
    if "fomc-calendar" in scopes:
        messages.append("美联储议息日期暂时无法更新")
    if "bls-macro-data" in scopes:
        messages.append("最近CPI与就业数据暂时无法更新，事件时间仍可使用")
    if "market-consensus" in scopes:
        messages.append(
            "本周市场一致预期暂时无法更新；没有匹配预期时不计算预期差与方向"
        )
    return messages


def _market_event_projection(
    samples: tuple[StoredSample, ...],
    selected_filters: dict[str, str | list[str]],
    query: str,
    *,
    now: datetime,
    current_issues: tuple[StoredIssue, ...],
    history_payloads: tuple[dict[str, Any], ...] = (),
    history_started_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    event_rows = [
        _market_event_enriched(_sample_payload(sample), now=now)
        for sample in samples
        if sample.payload.get("row_type") == "EVENT"
    ]
    indicator_rows = [
        _sample_payload(sample)
        for sample in samples
        if sample.payload.get("row_type") == "INDICATOR"
    ]
    upcoming_rows = [
        row for row in event_rows if _market_event_is_upcoming(row, now=now)
    ]
    rows = [
        row
        for row in upcoming_rows
        if _market_event_matches(
            row,
            selected_filters,
            query,
            now=now,
        )
    ]
    rows.sort(
        key=lambda row: (
            _market_event_sort_time(row) or datetime.max.replace(tzinfo=UTC),
            int(row.get("priority_rank") or 99),
            str(row.get("event_title") or ""),
        )
    )
    attention_rows = [
        row
        for row in rows
        if int(row.get("priority_rank") or 99) <= 3
        and _market_event_within_days(row, now=now, days=7)
    ][:6]

    today = now.astimezone(SHANGHAI_TZ).date()
    calendar_days: list[dict[str, Any]] = []
    for offset in range(14):
        day = today + timedelta(days=offset)
        day_events = [
            row for row in rows if row.get("calendar_date") == day.isoformat()
        ]
        calendar_days.append(
            {
                "date": day.isoformat(),
                "day_offset": offset,
                "events": day_events[:3],
                "additional_count": max(0, len(day_events) - 3),
            }
        )

    checked_times = [
        parsed
        for row in (*event_rows, *indicator_rows)
        if (parsed := _payload_time(str(row.get("source_checked_at") or "")))
        is not None
    ]
    history_rows = [
        _market_event_enriched(dict(row), now=now)
        for row in history_payloads
        if ((sort_at := _market_event_sort_time(row)) is not None and sort_at < now)
    ]
    history_rows.sort(
        key=lambda row: (
            _market_event_sort_time(row) or datetime.min.replace(tzinfo=UTC),
            str(row.get("event_title") or ""),
        ),
        reverse=True,
    )
    payload = {
        "projection_kind": "market_events",
        "event_query": query.strip(),
        "event_count": len(rows),
        "next_24h_count": sum(
            _market_event_within_days(row, now=now, days=1) for row in rows
        ),
        "next_7d_high_count": sum(
            row.get("importance") == "HIGH"
            and _market_event_within_days(row, now=now, days=7)
            for row in rows
        ),
        "attention_count": len(attention_rows),
        "recent_schedule_change_count": sum(
            bool(row.get("schedule_changed_recently")) for row in rows
        ),
        "attention_events": attention_rows,
        "calendar_days": calendar_days,
        "indicators": sorted(
            indicator_rows,
            key=lambda row: str(row.get("indicator_key") or ""),
        ),
        "coverage_messages": _market_event_coverage_messages(current_issues),
        "coverage_problem_count": len(current_issues),
        "source_checked_at": (iso_utc(max(checked_times)) if checked_times else None),
        "list_title": "事件日历",
        "history_started_at": (
            iso_utc(history_started_at) if history_started_at is not None else None
        ),
        "history_event_count": len(history_rows),
        "history_events": history_rows,
    }
    return rows, payload


def _buyback_effective_at(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _buyback_review_payload(review: StoredBuybackReview) -> dict[str, Any]:
    return {
        "review_id": review.review_id,
        "entity_key": review.entity_key,
        "base_revision_no": review.base_revision_no,
        "decision": review.decision,
        "decision_label": BUYBACK_REVIEW_LABELS.get(review.decision, review.decision),
        "corrected_event_type": review.corrected_event_type,
        "program_key": review.program_key,
        "program_status": review.program_status,
        "note": review.note,
        "created_at": iso_utc(review.created_at),
    }


def _compact_buyback_number(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    absolute = abs(number)
    if absolute >= 100_000_000:
        scaled, suffix = number / 100_000_000, "亿"
    elif absolute >= 10_000:
        scaled, suffix = number / 10_000, "万"
    else:
        return f"{number:,.2f}".rstrip("0").rstrip(".")
    return f"{scaled:,.2f}".rstrip("0").rstrip(".") + suffix


def _buyback_scale_label(payload: dict[str, Any]) -> str:
    shares = _compact_buyback_number(payload.get("shares"))
    amount = _compact_buyback_number(payload.get("amount"))
    parts: list[str] = []
    if shares is not None:
        parts.append(f"{shares}股")
    if amount is not None:
        currency = str(payload.get("currency") or "").strip()
        parts.append(f"{currency} {amount}".strip())
    if shares is not None and amount is None:
        parts.append("金额缺失")
    elif amount is not None and shares is None:
        parts.append("股数缺失")
    return " · ".join(parts) if parts else "规模未结构化"


def _buyback_scale_status(payload: dict[str, Any]) -> tuple[str, str | None]:
    has_shares = _compact_buyback_number(payload.get("shares")) is not None
    has_amount = _compact_buyback_number(payload.get("amount")) is not None
    if has_shares and has_amount:
        return "COMPLETE", None
    if has_shares or has_amount:
        return (
            "PARTIAL",
            "回购股数或金额字段缺失；页面未使用替代值。",
        )
    return (
        "MISSING",
        "该记录没有可展示的结构化回购股数或金额；页面未使用替代值。",
    )


def _buyback_intelligence_summary(payload: dict[str, Any], headline: str) -> str:
    title = str(payload.get("title") or headline).strip()
    issuer_name = str(payload.get("issuer_name") or "").strip()
    if issuer_name:
        for prefix in (f"{issuer_name}：", f"{issuer_name}:"):
            while title.startswith(prefix):
                title = title[len(prefix) :].lstrip()
    return title or headline


def _buyback_system_verification(
    entity_type: str,
    payload: dict[str, Any],
) -> tuple[str, str, str]:
    event_type = str(payload.get("event_type") or "")
    document_quality = str(payload.get("document_quality") or "")
    if entity_type == "HKEX_EXECUTION" and document_quality == "VALID_HKEX_XLS":
        return (
            "VERIFIED",
            "SYSTEM_VERIFIED",
            "港交所官方回购日报表头、执行场所与结构化字段已通过契约校验。",
        )
    if entity_type == "DISCLOSURE_CANDIDATE":
        current_classification = str(
            payload.get("current_title_classification") or event_type
        )
        if not is_target_a_share_security(
            str(payload.get("market") or ""),
            str(payload.get("stock_code") or ""),
        ):
            return (
                "EXCLUDED",
                "SYSTEM_EXCLUDED",
                "证券代码属于非目标股份类别，未进入 A 股回购情报范围。",
            )
        if current_classification in {
            "ANCILLARY",
            "OUT_OF_SCOPE_OTHER_REPURCHASE",
            "OUT_OF_SCOPE_SHARE_CLASS",
            "OUT_OF_SCOPE_OR_UNCLASSIFIED",
        }:
            return (
                "EXCLUDED",
                "SYSTEM_EXCLUDED",
                "公告属于辅助材料或非目标回购类型，未进入主清单。",
            )
        if (
            document_quality == "VALID_PDF_TEXT"
            and event_type in BUYBACK_SYSTEM_VERIFIED_A_EVENTS
        ):
            return (
                "VERIFIED",
                "SYSTEM_VERIFIED",
                "交易所官方披露索引、明确事件标题与可读 PDF 原文已通过质量门。",
            )
    if event_type == "AMBIGUOUS_BUYBACK":
        reason = "官方标题尚不能确定回购事件阶段，系统暂不交付为情报。"
    elif document_quality == "VALID_PDF_NO_TEXT":
        reason = "官方 PDF 已取得但无可校验文本，等待补充读取能力。"
    elif document_quality == "INDEX_ONLY":
        reason = "仅取得官方公告索引，等待原文进入证据质量门。"
    else:
        reason = "官方证据或结构化字段尚未完整通过系统质量门。"
    return "PENDING", "SYSTEM_PENDING", reason


def _buyback_entity_payload(entity: StoredBuybackEntity) -> dict[str, Any]:
    payload = dict(entity.payload)
    review = entity.review
    review_status = review.decision if review is not None else "UNREVIEWED"
    if entity.entity_type == "DISCLOSURE_CANDIDATE":
        current_classification = classify_buyback_title(str(payload.get("title") or ""))
        payload["current_title_classification"] = current_classification
        if (
            not (review is not None and review.corrected_event_type)
            and current_classification in BUYBACK_EVENT_LABELS
        ):
            payload["event_type"] = current_classification
            payload["event_type_label"] = BUYBACK_EVENT_LABELS[current_classification]
    if review is not None and review.corrected_event_type:
        payload["event_type"] = review.corrected_event_type
        payload["event_type_label"] = BUYBACK_EVENT_LABELS.get(
            review.corrected_event_type,
            review.corrected_event_type,
        )
    if review is not None and review.program_status:
        payload["program_status"] = review.program_status
    if review is not None and review.program_key:
        payload["program_key"] = review.program_key
    intelligence_scope, verification_status, verification_basis = (
        _buyback_system_verification(entity.entity_type, payload)
    )
    if review_status == "REJECTED_EVENT":
        intelligence_scope = "EXCLUDED"
        verification_status = "HUMAN_REJECTED"
        verification_basis = "人工校正记录已将当前 revision 排除为非目标回购事件。"
    elif review_status == "CONFIRMED_EVENT":
        intelligence_scope = "VERIFIED"
        verification_status = "HUMAN_CONFIRMED"
        verification_basis = "人工校正记录已确认当前 revision，并覆盖相应事件字段。"
    elif review_status == "NEEDS_FOLLOW_UP":
        intelligence_scope = "PENDING"
        verification_status = "NEEDS_FOLLOW_UP"
        verification_basis = "人工校正记录要求补充核对，暂不进入默认情报清单。"
    verification_labels = {
        "SYSTEM_VERIFIED": "系统已核验",
        "HUMAN_CONFIRMED": "人工校正确认",
        "SYSTEM_PENDING": "系统待补全",
        "SYSTEM_EXCLUDED": "系统已排除",
        "HUMAN_REJECTED": "人工已排除",
        "NEEDS_FOLLOW_UP": "待补充核对",
    }
    event_type = str(payload.get("event_type") or "")
    attention_level, attention_label = classify_buyback_attention(
        event_type, intelligence_scope
    )
    headline = str(
        payload.get("event_type_label")
        or BUYBACK_EVENT_LABELS.get(event_type, event_type)
    )
    if entity.entity_type == "HKEX_EXECUTION":
        intelligence_summary = "港交所官方日报记录本次实际回购"
    else:
        intelligence_summary = _buyback_intelligence_summary(payload, headline)
    scale_status, scale_reason = _buyback_scale_status(payload)
    payload.update(
        {
            "entity_key": entity.entity_key,
            "entity_type": entity.entity_type,
            "revision_no": entity.revision_no,
            "revision_id": entity.revision_id,
            "observed_at": iso_utc(entity.observed_at),
            "effective_at": iso_utc(entity.effective_at),
            "effective_date": entity.effective_at.astimezone(SHANGHAI_TZ)
            .date()
            .isoformat(),
            "document_sha256": entity.document_sha256,
            "review_status": review_status,
            "review_status_label": BUYBACK_REVIEW_LABELS[review_status],
            "review_created_at": (
                iso_utc(review.created_at) if review is not None else None
            ),
            "intelligence_scope": intelligence_scope,
            "verification_status": verification_status,
            "verification_status_label": verification_labels[verification_status],
            "verification_basis": verification_basis,
            "verification_boundary": (
                "核验确认所示官方回购事实及字段来源；不包含收益方向、买卖建议或仓位。"
            ),
            "is_verified_intelligence": intelligence_scope == "VERIFIED",
            "attention_level": attention_level,
            "attention_label": attention_label,
            "security_label": " · ".join(
                value
                for value in (
                    str(payload.get("stock_code") or "").strip(),
                    str(payload.get("issuer_name") or "").strip(),
                )
                if value
            ),
            "intelligence_headline": headline,
            "intelligence_summary": intelligence_summary,
            "scale_label": _buyback_scale_label(payload),
            "scale_status": scale_status,
            "scale_reason": scale_reason,
        }
    )
    if intelligence_scope == "EXCLUDED":
        payload["row_tone"] = "DISABLED"
        payload["candidate_status"] = "REJECTED_EVENT"
        payload["data_quality_label"] = "人工校正排除"
        payload["no_action_reason"] = "REVIEW_REJECTED"
    elif intelligence_scope == "VERIFIED":
        payload.pop("row_tone", None)
        payload["candidate_status"] = "VERIFIED_INTELLIGENCE"
        payload["data_quality_label"] = verification_labels[verification_status]
    else:
        payload["row_tone"] = "WARNING"
        payload["candidate_status"] = "SYSTEM_PENDING"
    return payload


BUYBACK_LIST_FIELDS = frozenset(
    {
        "entity_key",
        "entity_type",
        "revision_no",
        "observed_at",
        "effective_at",
        "effective_date",
        "market_scope",
        "market",
        "market_label",
        "stock_code",
        "issuer_name",
        "event_type",
        "event_type_label",
        "attention_level",
        "attention_label",
        "security_label",
        "connect_status",
        "connect_status_label",
        "connect_route_label",
        "daily_change_percent",
        "attractiveness_score",
        "attractiveness_level",
        "attractiveness_label",
        "attractiveness_summary",
        "attractiveness_explanation",
        "roe_percent",
        "revenue_yoy_percent",
        "net_profit_yoy_percent",
        "execution_days_value",
        "execution_days_label",
        "execution_days_scope",
        "cumulative_shares",
        "cumulative_shares_label",
        "cumulative_amount",
        "cumulative_amount_label",
        "recent_amount_label",
        "average_cost",
        "average_cost_label",
        "average_cost_scope_label",
        "recent_average_cost_label",
        "current_price",
        "current_price_label",
        "price_vs_average_percent",
        "recent_price_vs_average_percent",
        "actual_amount_yield_percent",
        "intelligence_scope",
        "intelligence_headline",
        "intelligence_summary",
        "row_tone",
        "review_status",
        "scale_status",
        "scale_label",
        "scale_reason",
        "missing_reasons",
        "currency",
    }
)


def _buyback_list_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Bound the high-cardinality list response; details remain on their endpoint."""

    return {key: value for key, value in payload.items() if key in BUYBACK_LIST_FIELDS}


def _buyback_source_payload(source: StoredBuybackSourceState) -> dict[str, Any]:
    tone = {
        "SUCCESS": "HEALTHY",
        "EMPTY": "HEALTHY",
        "PARTIAL": "PARTIAL",
        "STALE": "STALE",
        "ERROR": "FAILED",
    }.get(source.status, "UNKNOWN")
    labels = {
        "SUCCESS": "读取成功",
        "EMPTY": "已检查 · 无记录",
        "PARTIAL": "部分可用",
        "STALE": "读取失败 · 保留旧值",
        "ERROR": "读取失败 · 无可用值",
    }
    summary_keys = {
        "as_of",
        "window_start",
        "window_end",
        "candidate_count",
        "target_candidate_count",
        "report_count",
        "hkex_execution_row_count",
        "cross_market_row_count",
        "new_document_count",
        "existing_document_count",
        "fallback_document_count",
        "failed_document_count",
        "empty_text_document_count",
        "backlog_count",
        "run_document_limit",
        "page_count",
        "programme_count",
        "quote_count",
        "requested_count",
    }
    return {
        "source_key": source.source_key,
        "source_label": source.source_label,
        "status": source.status,
        "status_label": labels.get(source.status, source.status),
        "tone": tone,
        "checked_at": iso_utc(source.checked_at),
        "source_time": (
            iso_utc(source.source_time) if source.source_time is not None else None
        ),
        "next_due_at": iso_utc(source.next_due_at),
        "record_count": source.record_count,
        "detail_code": source.detail_code,
        "summary": {
            key: value for key, value in source.payload.items() if key in summary_keys
        },
    }


def _column_payload(column: ViewColumn) -> dict[str, Any]:
    return {
        "key": column.key,
        "label": column.label,
        "kind": column.kind,
        "priority": column.priority,
        "minimum_fraction_digits": column.minimum_fraction_digits,
        "maximum_fraction_digits": column.maximum_fraction_digits,
        "use_grouping": column.use_grouping,
        "show_sign": column.show_sign,
        "description": column.description,
    }


def _altcoin_price_position_projection(
    monitor: RegisteredMonitor,
    store: SQLiteMonitorStore,
    *,
    data_run: StoredRun | None,
) -> dict[str, Any]:
    snapshot_key = str(
        getattr(monitor, "price_position_snapshot_key", "price-position-v1")
    )
    snapshot = store.projection_snapshot(monitor.monitor_id, snapshot_key)
    columns = tuple(getattr(monitor, "price_position_columns", ()))
    choices = tuple(getattr(monitor, "price_position_filter_choices", ()))
    common = {
        "table_title": str(
            getattr(monitor, "price_position_table_title", "日线价格位置")
        ),
        "method_note": str(getattr(monitor, "price_position_method_note", "")),
        "columns": [_column_payload(column) for column in columns],
        "filter_choices": [
            {
                "value": choice.value,
                "label": choice.label,
                "description": choice.description,
            }
            for choice in choices
        ],
    }
    if snapshot is None:
        return {
            **common,
            "status": "EMPTY",
            "snapshot_run_id": None,
            "observed_at": None,
            "price_cutoff_at": None,
            "daily_cutoff_at": None,
            "valid_until": None,
            "rows": [],
            "summary": [],
            "counts": {},
            "state_counts": {},
            "empty_message": (
                "尚无已完成的日线价格位置快照；开启监控并完成一轮采集后显示。"
            ),
        }
    raw_rows = snapshot.payload.get("rows")
    rows = (
        [dict(row) for row in raw_rows if isinstance(row, dict)]
        if isinstance(raw_rows, list)
        else []
    )
    is_current = data_run is not None and snapshot.run_id == data_run.run_id
    summary: list[dict[str, Any]] = [
        {
            "key": "price_position_coverage",
            "label": "本轮覆盖",
            "value": str(snapshot.payload.get("coverage_label") or ""),
            "kind": "text",
            "description": (
                "只统计本轮完成短周期分析的合约；日线少于90根或不连续时不进入价格位置主表。"
            ),
        },
        {
            "key": "price_position_price_cutoff",
            "label": "当前价截至",
            "value": snapshot.payload.get("price_cutoff_at")
            or iso_utc(snapshot.cutoff_at),
            "kind": "time",
        },
        {
            "key": "price_position_daily_cutoff",
            "label": "日线基准截至",
            "value": snapshot.payload.get("daily_cutoff_at"),
            "kind": "time",
        },
        {
            "key": "price_position_valid_until",
            "label": "本轮位置有效至",
            "value": snapshot.payload.get("valid_until"),
            "kind": "time",
        },
    ]
    if not is_current:
        summary.insert(
            0,
            {
                "key": "price_position_snapshot_state",
                "label": "快照状态",
                "value": "上一轮价格位置；最近候选采集未形成新的完整日线快照。",
                "kind": "text",
            },
        )
    return {
        **common,
        "status": "CURRENT" if is_current else "PREVIOUS",
        "snapshot_run_id": snapshot.run_id,
        "observed_at": iso_utc(snapshot.observed_at),
        "price_cutoff_at": snapshot.payload.get("price_cutoff_at")
        or iso_utc(snapshot.cutoff_at),
        "daily_cutoff_at": snapshot.payload.get("daily_cutoff_at"),
        "valid_until": snapshot.payload.get("valid_until"),
        "rows": rows,
        "summary": summary,
        "counts": snapshot.payload.get("counts") or {},
        "state_counts": snapshot.payload.get("state_counts") or {},
        "empty_message": "当前筛选没有匹配的价格状态。",
    }


def _promoted_uniform_columns(
    monitor: RegisteredMonitor,
    samples: tuple[StoredSample, ...],
) -> dict[str, Any]:
    promoted: dict[str, Any] = {}
    for column in monitor.view.columns:
        if not column.promote_when_uniform or not samples:
            continue
        values = [sample.payload.get(column.key) for sample in samples]
        if any(value in (None, "") for value in values):
            continue
        first = values[0]
        if all(value == first for value in values[1:]):
            promoted[column.key] = first
    return promoted


def _run_summary_payload(
    monitor: RegisteredMonitor,
    samples: tuple[StoredSample, ...],
    promoted_columns: dict[str, Any],
) -> list[dict[str, Any]]:
    if not samples:
        return []
    payload = samples[0].payload
    summary = [
        {
            "key": field.key,
            "label": field.label,
            "value": payload.get(field.key),
            "kind": "text",
            "description": field.description,
        }
        for field in monitor.view.summary_fields
        if payload.get(field.key) not in (None, "")
    ]
    summary.extend(
        {
            **_column_payload(column),
            "label": column.uniform_summary_label or column.label,
            "value": promoted_columns[column.key],
        }
        for column in monitor.view.columns
        if column.key in promoted_columns
    )
    return summary


def _forward_evaluation_row(
    evaluation: StoredForwardEvaluation,
    *,
    now: datetime,
) -> dict[str, Any]:
    verdict_labels = {
        "ALIGNED": "方向一致",
        "INCONCLUSIVE": "未形成显著方向",
        "OPPOSED": "方向相反",
        "UNAVAILABLE": "无法检验",
    }
    if evaluation.status == "PENDING":
        status_label = "等待到期" if evaluation.due_at > now else "待补采"
    elif evaluation.status == "COMPLETE":
        status_label = "已完成"
    else:
        status_label = "无法检验"
    return {
        "case_key": evaluation.case_key,
        "entity_key": evaluation.entity_key,
        "stage": evaluation.stage,
        "stage_label": evaluation.stage_label,
        "direction": evaluation.direction,
        "signal_observed_at": iso_utc(evaluation.signal_observed_at),
        "source_cutoff_at": iso_utc(evaluation.source_cutoff_at),
        "horizon_minutes": evaluation.horizon_minutes,
        "due_at": iso_utc(evaluation.due_at),
        "status": evaluation.status,
        "status_label": status_label,
        "evaluated_at": (
            iso_utc(evaluation.evaluated_at)
            if evaluation.evaluated_at is not None
            else None
        ),
        "outcome_cutoff_at": (
            iso_utc(evaluation.outcome_cutoff_at)
            if evaluation.outcome_cutoff_at is not None
            else None
        ),
        "forward_return_percent": evaluation.forward_return_percent,
        "benchmark_return_percent": evaluation.benchmark_return_percent,
        "relative_return_percent": evaluation.relative_return_percent,
        "maximum_favorable_excursion_percent": (
            evaluation.maximum_favorable_excursion_percent
        ),
        "maximum_adverse_excursion_percent": (
            evaluation.maximum_adverse_excursion_percent
        ),
        "verdict": evaluation.verdict,
        "verdict_label": verdict_labels.get(evaluation.verdict or "", status_label),
        "reason_code": evaluation.reason_code,
    }


def _forward_evaluation_payload(
    monitor: RegisteredMonitor,
    store: SQLiteMonitorStore,
    *,
    now: datetime,
) -> dict[str, Any] | None:
    definition = monitor.view.evaluation
    if definition is None:
        return None

    def maturity(values: dict[str, Any]) -> dict[str, Any]:
        sample_count = int(values.get("sample_count", 0))
        distinct_cutoff_count = int(values.get("distinct_cutoff_count", 0))
        distinct_entity_count = int(values.get("distinct_entity_count", 0))
        first_cutoff_at = values.get("first_cutoff_at")
        last_outcome_at = values.get("last_outcome_at")
        observation_days = (
            max(
                0.0,
                (last_outcome_at - first_cutoff_at).total_seconds() / 86_400.0,
            )
            if first_cutoff_at is not None and last_outcome_at is not None
            else 0.0
        )
        checks = (
            (
                "sample_count",
                sample_count,
                definition.minimum_group_samples,
                "完成样本",
            ),
            (
                "distinct_cutoff_count",
                distinct_cutoff_count,
                definition.minimum_distinct_cutoffs,
                "独立信号截止",
            ),
            (
                "distinct_entity_count",
                distinct_entity_count,
                definition.minimum_distinct_entities,
                "覆盖币种",
            ),
            (
                "observation_days",
                observation_days,
                definition.minimum_observation_days,
                "观测跨度",
            ),
        )
        blockers = [
            {
                "key": key,
                "label": (
                    f"{label} {current:.1f}/{required:.1f}"
                    if key == "observation_days"
                    else f"{label} {int(current)}/{int(required)}"
                ),
            }
            for key, current, required, label in checks
            if current < required
        ]
        return {
            "ready": not blockers,
            "status_label": "达到展示门槛" if not blockers else "样本继续积累",
            "sample_count": sample_count,
            "distinct_cutoff_count": distinct_cutoff_count,
            "distinct_entity_count": distinct_entity_count,
            "observation_days": round(observation_days, 2),
            "minimum_sample_count": definition.minimum_group_samples,
            "minimum_distinct_cutoffs": definition.minimum_distinct_cutoffs,
            "minimum_distinct_entities": definition.minimum_distinct_entities,
            "minimum_observation_days": definition.minimum_observation_days,
            "blockers": blockers,
        }

    evaluation_source = getattr(monitor, "evaluation_source", None)
    summary = store.forward_evaluation_summary(
        monitor.monitor_id,
        now=now,
        source=evaluation_source,
    )
    due_cases = int(summary["due_cases"])
    completed_cases = int(summary["completed_cases"])
    coverage_percent = (
        round(completed_cases / due_cases * 100.0, 1) if due_cases else None
    )
    groups = []
    for group in summary["groups"]:
        sample_count = int(group["sample_count"])
        group_maturity = maturity(group)
        groups.append(
            {
                **{
                    key: value
                    for key, value in group.items()
                    if key not in {"first_cutoff_at", "last_outcome_at"}
                },
                "maturity": group_maturity,
                "agreement_rate_percent": (
                    round(int(group["aligned_count"]) / sample_count * 100.0, 1)
                    if group_maturity["ready"] and sample_count > 0
                    else None
                ),
                "average_relative_return_percent": (
                    round(float(group["average_relative_return_percent"]), 4)
                    if group_maturity["ready"]
                    and group["average_relative_return_percent"] is not None
                    else None
                ),
                "average_favorable_excursion_percent": (
                    round(float(group["average_favorable_excursion_percent"]), 4)
                    if group_maturity["ready"]
                    and group["average_favorable_excursion_percent"] is not None
                    else None
                ),
                "average_adverse_excursion_percent": (
                    round(float(group["average_adverse_excursion_percent"]), 4)
                    if group_maturity["ready"]
                    and group["average_adverse_excursion_percent"] is not None
                    else None
                ),
            }
        )
    comparison_payload: dict[str, Any] | None = None
    baseline_evaluation_source = getattr(
        monitor,
        "baseline_evaluation_source",
        None,
    )
    if evaluation_source and baseline_evaluation_source:
        comparison = store.forward_evaluation_comparison(
            monitor.monitor_id,
            primary_source=str(evaluation_source),
            baseline_source=str(baseline_evaluation_source),
        )

        relation_labels = {
            "DIRECTION_FLIP": "方向翻转（增量检验）",
            "SAME_DIRECTION": "方向同向（筛选检验）",
        }

        def comparison_rates(
            values: dict[str, Any],
            *,
            direction_relation: str,
        ) -> dict[str, Any]:
            sample_count = int(values["sample_count"])
            relation_maturity = maturity(values)
            primary_aligned = int(values["primary_aligned_count"])
            primary_opposed = int(values["primary_opposed_count"])
            baseline_aligned = int(values["baseline_aligned_count"])
            baseline_opposed = int(values["baseline_opposed_count"])
            primary_rate = (
                round(primary_aligned / sample_count * 100.0, 1)
                if relation_maturity["ready"] and sample_count > 0
                else None
            )
            baseline_rate = (
                round(baseline_aligned / sample_count * 100.0, 1)
                if relation_maturity["ready"] and sample_count > 0
                else None
            )
            incremental_comparison = direction_relation == "DIRECTION_FLIP"
            return {
                "direction_relation": direction_relation,
                "relation_label": relation_labels[direction_relation],
                "incremental_comparison": incremental_comparison,
                "paired_case_count": int(values["paired_case_count"]),
                "sample_count": sample_count,
                "pending_pair_count": int(values["pending_pair_count"]),
                "unavailable_pair_count": int(values["unavailable_pair_count"]),
                "primary_aligned_count": primary_aligned,
                "primary_opposed_count": primary_opposed,
                "baseline_aligned_count": baseline_aligned,
                "baseline_opposed_count": baseline_opposed,
                "maturity": relation_maturity,
                "primary_agreement_rate_percent": primary_rate,
                "primary_opposed_rate_percent": (
                    round(primary_opposed / sample_count * 100.0, 1)
                    if relation_maturity["ready"] and sample_count > 0
                    else None
                ),
                "baseline_agreement_rate_percent": baseline_rate,
                "baseline_opposed_rate_percent": (
                    round(baseline_opposed / sample_count * 100.0, 1)
                    if relation_maturity["ready"] and sample_count > 0
                    else None
                ),
                "agreement_change_percentage_points": (
                    round(primary_rate - baseline_rate, 1)
                    if incremental_comparison
                    and primary_rate is not None
                    and baseline_rate is not None
                    else None
                ),
            }

        comparison_payload = {
            "primary_label": "价格位置融合规则",
            "baseline_label": "原短线规则",
            **{
                key: int(comparison[key])
                for key in (
                    "paired_case_count",
                    "sample_count",
                    "pending_pair_count",
                    "unavailable_pair_count",
                )
            },
            "first_cutoff_at": (
                iso_utc(comparison["first_cutoff_at"])
                if comparison["first_cutoff_at"] is not None
                else None
            ),
            "last_outcome_at": (
                iso_utc(comparison["last_outcome_at"])
                if comparison["last_outcome_at"] is not None
                else None
            ),
            "relations": [
                comparison_rates(
                    item,
                    direction_relation=str(item["direction_relation"]),
                )
                for item in comparison["relations"]
            ],
            "groups": [
                {
                    "stage": item["stage"],
                    "stage_label": item["stage_label"],
                    "horizon_minutes": item["horizon_minutes"],
                    **comparison_rates(
                        item,
                        direction_relation=str(item["direction_relation"]),
                    ),
                }
                for item in comparison["groups"]
            ],
        }
    return {
        "title": definition.title,
        "method_note": definition.method_note,
        "minimum_group_samples": definition.minimum_group_samples,
        "maturity": maturity(
            {
                "sample_count": summary["completed_cases"],
                "distinct_cutoff_count": summary["distinct_cutoff_count"],
                "distinct_entity_count": summary["distinct_entity_count"],
                "first_cutoff_at": summary["first_cutoff_at"],
                "last_outcome_at": summary["last_outcome_at"],
            }
        ),
        "overview": {
            **{
                key: summary[key]
                for key in (
                    "total_cases",
                    "due_cases",
                    "completed_cases",
                    "unavailable_cases",
                    "pending_due_cases",
                    "pending_future_cases",
                )
            },
            "coverage_percent": coverage_percent,
        },
        "groups": groups,
        "comparison": comparison_payload,
        "recent": [
            _forward_evaluation_row(item, now=now)
            for item in store.recent_forward_evaluations(
                monitor.monitor_id,
                limit=120,
                source=evaluation_source,
            )
        ],
    }


def _configuration_payload(
    monitor: RegisteredMonitor,
    store: SQLiteMonitorStore,
) -> dict[str, Any] | None:
    if not isinstance(monitor, ConfigurableMonitor):
        return None
    stored = store.load_configuration(monitor.monitor_id)
    return {
        "fields": [
            {
                "key": field.key,
                "label": field.label,
                "kind": field.kind,
                "unit": field.unit,
                "minimum": field.minimum,
                "step": field.step,
                "choices": [
                    {"value": choice.value, "label": choice.label}
                    for choice in field.choices
                ],
            }
            for field in monitor.configuration_fields
        ],
        "values": monitor.configuration(),
        "updated_at": iso_utc(stored.updated_at) if stored else None,
    }


def create_app(
    store: SQLiteMonitorStore,
    registry: MonitorRegistry,
    scheduler: MonitorScheduler | None,
    *,
    start_scheduler: bool = True,
) -> FastAPI:
    static_root = Path(__file__).with_name("static")
    buyback_cache_lock = threading.Lock()
    buyback_cache: dict[str, Any] = {"key": None}

    def buyback_projection(monitor_id: str, *, now: datetime) -> dict[str, Any]:
        display_limit = 19_999
        cache_key = (
            monitor_id,
            store.buyback_projection_version(monitor_id),
            int(now.timestamp() // 60),
        )
        with buyback_cache_lock:
            if buyback_cache.get("key") == cache_key:
                return dict(buyback_cache)
            entity_window = store.latest_buyback_entities(
                monitor_id,
                limit=display_limit + 1,
            )
            entities = entity_window[:display_limit]
            entity_payloads = tuple(
                _buyback_entity_payload(entity) for entity in entities
            )
            source_states = store.buyback_source_states(monitor_id)
            projected = project_buyback_metrics(
                entity_payloads,
                source_payloads={
                    source.source_key: dict(source.payload) for source in source_states
                },
                source_statuses={
                    source.source_key: source.status for source in source_states
                },
                now=now,
            )
            rows = tuple(_buyback_list_payload(row) for row in projected)
            buyback_cache.clear()
            buyback_cache.update(
                {
                    "key": cache_key,
                    "rows": rows,
                    "rows_by_key": {
                        str(row.get("entity_key") or ""): row
                        for row in rows
                        if row.get("entity_key")
                    },
                    "source_states": source_states,
                    "entity_count": len(entity_payloads),
                    "entity_truncated": len(entity_window) > display_limit,
                    "display_limit": display_limit,
                }
            )
            return dict(buyback_cache)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        store.initialize()
        for monitor in registry:
            store.ensure_control(
                monitor.monitor_id,
                default_enabled=bool(getattr(monitor, "default_enabled", True)),
            )
            if not isinstance(monitor, ConfigurableMonitor):
                continue
            stored = store.load_configuration(monitor.monitor_id)
            if stored is None:
                normalized = monitor.normalize_configuration(monitor.configuration())
                monitor.apply_configuration(normalized)
                store.save_configuration(monitor.monitor_id, normalized)
            else:
                normalized = monitor.normalize_configuration(stored.values)
                monitor.apply_configuration(normalized)
        if start_scheduler and scheduler is not None:
            scheduler.start()
        try:
            yield
        finally:
            if start_scheduler and scheduler is not None:
                scheduler.stop()

    app = FastAPI(
        title="Halpha Monitor",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["127.0.0.1", "localhost"],
    )

    @app.middleware("http")
    async def security_headers(request: Request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        # Browser annotation overlays need page-local inline CSS. Keep the
        # stricter policy on API and static-asset responses.
        style_source = (
            "style-src 'self' 'unsafe-inline'; "
            if request.url.path == "/"
            else "style-src 'self'; "
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; "
            f"{style_source}"
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'"
        )
        if request.url.path == "/" or request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    app.mount("/static", StaticFiles(directory=static_root), name="monitor-static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(static_root / "index.html")

    @app.get("/healthz", include_in_schema=False)
    def health() -> Response:
        if scheduler is None or not start_scheduler:
            return JSONResponse({"status": "ok", "scheduler": "not_managed_by_app"})
        workers = scheduler.worker_states()
        healthy = scheduler.healthy
        return JSONResponse(
            {
                "status": "ok" if healthy else "degraded",
                "scheduler": "running" if scheduler.started else "stopped",
                "workers": [
                    {
                        "monitor_id": worker.monitor_id,
                        "alive": worker.alive,
                        "collecting": worker.collecting,
                        "manual_run_pending": worker.manual_run_pending,
                        "last_seen_at": (
                            iso_utc(worker.last_seen_at)
                            if worker.last_seen_at is not None
                            else None
                        ),
                        "last_error": worker.last_error,
                    }
                    for worker in workers
                ],
            },
            status_code=200 if healthy else 503,
        )

    @app.get("/api/view")
    def view(
        request: Request,
        monitor_id: str | None = None,
        hours: int = Query(default=6),
        series_key: str | None = None,
        stock_query: str = Query(default="", max_length=64),
        event_query: str = Query(default="", max_length=64),
    ) -> dict[str, Any]:
        if hours not in ALLOWED_WINDOWS:
            raise HTTPException(status_code=422, detail="TIME_WINDOW_UNSUPPORTED")
        monitors = registry.all()
        if not monitors:
            raise HTTPException(status_code=503, detail="NO_MONITORS_REGISTERED")
        selected = monitors[0] if monitor_id is None else None
        if selected is None:
            try:
                selected = registry.get(monitor_id or "")
            except KeyError:
                raise HTTPException(
                    status_code=404, detail="MONITOR_NOT_FOUND"
                ) from None

        now = utc_now()
        summaries = [
            _monitor_summary(monitor, store, now=now, scheduler=scheduler)
            for monitor in monitors
        ]
        selected_summary = next(
            item for item in summaries if item["monitor_id"] == selected.monitor_id
        )
        selected_filters: dict[str, str | list[str]] = {}
        filter_payload: list[dict[str, Any]] = []
        for definition in selected.view.filters:
            allowed = {choice.value for choice in definition.choices}
            if definition.multiple:
                requested_values = request.query_params.getlist(definition.key)
                if not requested_values:
                    requested_values = [definition.default]
                requested = list(dict.fromkeys(requested_values))
                if not requested or any(value not in allowed for value in requested):
                    raise HTTPException(
                        status_code=422,
                        detail=f"FILTER_VALUE_UNSUPPORTED_{definition.key.upper()}",
                    )
            else:
                requested = request.query_params.get(
                    definition.key,
                    definition.default,
                )
                if requested not in allowed:
                    raise HTTPException(
                        status_code=422,
                        detail=f"FILTER_VALUE_UNSUPPORTED_{definition.key.upper()}",
                    )
            selected_filters[definition.key] = requested
            filter_payload.append(
                {
                    "key": definition.key,
                    "label": definition.label,
                    "selected": requested,
                    "multiple": definition.multiple,
                    "choices": [
                        {
                            "value": choice.value,
                            "label": choice.label,
                            "description": choice.description,
                        }
                        for choice in definition.choices
                    ],
                }
            )

        issues = store.recent_issues(selected.monitor_id, limit=20)
        latest_run = store.latest_run(selected.monitor_id)
        current_issues = (
            store.issues_for_run(latest_run.run_id) if latest_run is not None else ()
        )
        projection_kind = getattr(selected, "projection_kind", "time_series")
        is_buyback = projection_kind == "buyback"
        is_market_events = projection_kind == "market_events"
        is_btc_intelligence = projection_kind == "btc_intelligence"
        buyback_payload: dict[str, Any] | None = None
        market_events_payload: dict[str, Any] | None = None
        btc_intelligence_payload: dict[str, Any] | None = None
        altcoin_price_position_payload: dict[str, Any] | None = None
        if is_buyback:
            data_run = store.latest_completed_run(selected.monitor_id)
            samples: tuple[StoredSample, ...] = ()
            projection = buyback_projection(selected.monitor_id, now=now)
            all_row_payloads = projection["rows"]
            source_states = projection["source_states"]
            row_payloads = [
                row
                for row in all_row_payloads
                if row["intelligence_scope"] == "VERIFIED"
                and _buyback_stock_matches(row, stock_query)
                and _row_matches_filters(row, selected_filters)
            ]
            selected_series = None
            history_points: list[dict[str, Any]] = []
            collection_gaps: list[dict[str, Any]] = []
            promoted_columns: dict[str, Any] = {}
            intelligence_count = len(row_payloads)
            fresh_intelligence_count = sum(
                (parsed := _buyback_effective_at(row.get("effective_at"))) is not None
                and parsed >= now - timedelta(hours=24)
                for row in row_payloads
            )
            execution_count = sum(
                row.get("event_type") in BUYBACK_EXECUTION_EVENTS
                for row in row_payloads
            )
            priority_count = sum(
                row.get("attention_level") == "PRIORITY" for row in row_payloads
            )
            high_attractiveness_count = sum(
                row.get("attractiveness_level") == "HIGH" for row in row_payloads
            )
            pending_count = sum(
                row["intelligence_scope"] == "PENDING" for row in all_row_payloads
            )
            excluded_count = sum(
                row["intelligence_scope"] == "EXCLUDED" for row in all_row_payloads
            )
            source_problem_count = sum(
                source.status not in {"SUCCESS", "EMPTY"} for source in source_states
            )
            source_checked_at = max(
                (source.checked_at for source in source_states),
                default=None,
            )
            buyback_payload = {
                "projection_kind": "buyback",
                "entity_count": projection["entity_count"],
                "entity_truncated": projection["entity_truncated"],
                "display_limit": projection["display_limit"],
                "filtered_entity_count": len(row_payloads),
                "stock_query": stock_query.strip(),
                "intelligence_count": intelligence_count,
                "fresh_intelligence_count": fresh_intelligence_count,
                "execution_count": execution_count,
                "priority_count": priority_count,
                "high_attractiveness_count": high_attractiveness_count,
                "pending_count": pending_count,
                "excluded_count": excluded_count,
                "source_problem_count": source_problem_count,
                "source_checked_at": (
                    iso_utc(source_checked_at)
                    if source_checked_at is not None
                    else None
                ),
                "list_title": selected.view.table_title,
                "source_states": [
                    _buyback_source_payload(source) for source in source_states
                ],
            }
            run_summary_payload = []
        elif is_market_events:
            data_run = store.latest_completed_run(selected.monitor_id)
            samples = store.samples_for_run(data_run.run_id) if data_run else ()
            history_revisions = store.latest_market_event_revisions(
                selected.monitor_id,
                limit=5000,
            )
            history_payloads = tuple(
                {
                    **revision.payload,
                    "history_revision_no": revision.revision_no,
                    "history_observed_at": iso_utc(revision.observed_at),
                    "history_state": revision.state,
                }
                for revision in history_revisions
            )
            row_payloads, market_events_payload = _market_event_projection(
                samples,
                selected_filters,
                event_query,
                now=now,
                current_issues=current_issues,
                history_payloads=history_payloads,
                history_started_at=store.market_event_history_started_at(
                    selected.monitor_id
                ),
            )
            selected_series = None
            history_points = []
            collection_gaps = []
            promoted_columns = {}
            run_summary_payload = []
        elif is_btc_intelligence:
            data_run = store.latest_completed_run(selected.monitor_id)
            samples = store.samples_for_run(data_run.run_id) if data_run else ()
            btc_intelligence_payload = _btc_intelligence_projection(
                samples,
                store,
                selected.monitor_id,
            )
            row_payloads = []
            selected_series = None
            history_points = []
            collection_gaps = []
            promoted_columns = {}
            run_summary_payload = []
        else:
            data_run = store.latest_sample_run(selected.monitor_id)
            samples = store.samples_for_run(data_run.run_id) if data_run else ()
            if projection_kind == "altcoin_radar":
                samples = tuple(
                    sample
                    for sample in samples
                    if sample.payload.get("market_scope") == "USDM_PERPETUAL"
                )
                altcoin_price_position_payload = _altcoin_price_position_projection(
                    selected,
                    store,
                    data_run=data_run,
                )
            rows = [
                sample
                for sample in samples
                if _row_matches_filters(sample.payload, selected_filters)
            ]
            row_payloads = [_sample_payload(sample) for sample in rows]
            available_series = {sample.series_key for sample in rows}
            selected_series = (
                series_key
                if series_key in available_series
                else rows[0].series_key
                if rows
                else None
            )
            history = (
                store.history(
                    selected.monitor_id,
                    selected_series,
                    since=now - timedelta(hours=hours),
                )
                if selected_series is not None
                else ()
            )
            history_points, collection_gaps = _history_payload(
                history,
                interval_seconds=selected.interval_seconds,
                now=now,
            )
            promoted_columns = _promoted_uniform_columns(selected, samples)
            run_summary_payload = _run_summary_payload(
                selected,
                samples,
                promoted_columns,
            )
        refresh_after_seconds = 15
        automatic_collection = selected_summary.get("automatic_collection")
        if is_buyback and automatic_collection is not None:
            automatic_status = str(automatic_collection.get("status") or "")
            latest_selected_run = selected_summary.get("latest_run")
            if latest_selected_run and latest_selected_run.get("status") == "RUNNING":
                refresh_after_seconds = 15
            elif automatic_status == "CLOSED":
                next_open = _payload_time(automatic_collection.get("next_open_at"))
                refresh_after_seconds = (
                    max(
                        15, min(round((next_open - now).total_seconds()) + 2, 7 * 86400)
                    )
                    if next_open is not None
                    else 300
                )
            elif automatic_status == "UNAVAILABLE":
                refresh_after_seconds = 300
        service_status, service_status_label = _overall_data_status(summaries)
        return {
            "server_time": iso_utc(now),
            "refresh_after_seconds": refresh_after_seconds,
            "service_status": service_status,
            "service_status_label": service_status_label,
            "collection_load": _collection_load(monitors, summaries, now=now),
            "monitors": summaries,
            "monitor": {
                **selected_summary,
                "filters": filter_payload,
                "selected_filters": selected_filters,
                "columns": [
                    _column_payload(column)
                    for column in selected.view.columns
                    if column.key not in promoted_columns
                ],
                "table_title": selected.view.table_title,
                "chart_title": selected.view.chart_title,
                "method_note": selected.view.method_note,
                "show_description": selected.view.show_description,
                "data_run": _run_payload(data_run),
                "configuration": _configuration_payload(selected, store),
                "projection_kind": projection_kind,
            },
            "rows": row_payloads,
            "run_summary": run_summary_payload,
            "buyback": buyback_payload,
            "market_events": market_events_payload,
            "btc_intelligence": btc_intelligence_payload,
            "altcoin_price_position": altcoin_price_position_payload,
            "evaluation": _forward_evaluation_payload(selected, store, now=now),
            "selected_series_key": selected_series,
            "history": history_points,
            "collection_gaps": collection_gaps,
            "current_issues": [_issue_payload(issue) for issue in current_issues],
            "issues": [_issue_payload(issue) for issue in issues],
            "time_windows": [
                {
                    "hours": value,
                    "label": (f"{value // 24}天" if value >= 24 else f"{value}小时"),
                }
                for value in ALLOWED_WINDOWS
            ],
        }

    @app.put("/api/monitors/{monitor_id}/control")
    def update_control(
        monitor_id: str,
        body: ControlRequest,
        request: Request,
    ) -> dict[str, Any]:
        origin = request.headers.get("origin")
        expected_origin = f"{request.url.scheme}://{request.url.netloc}"
        if origin is not None and origin.rstrip("/") != expected_origin:
            raise HTTPException(status_code=403, detail="ORIGIN_NOT_ALLOWED")
        try:
            monitor = registry.get(monitor_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="MONITOR_NOT_FOUND") from None
        store.ensure_control(
            monitor_id,
            default_enabled=bool(getattr(monitor, "default_enabled", True)),
        )
        if scheduler is not None:
            stored = scheduler.set_enabled(monitor_id, body.enabled)
            automatic_state = scheduler.automatic_collection_state(
                monitor_id,
                now=utc_now(),
            )
            # set_enabled() already wakes the worker. Scheduled monitors only
            # start automatically when their current calendar gate is open.
            refresh_requested = bool(
                body.enabled
                and scheduler.started
                and (automatic_state is None or automatic_state.allowed)
            )
        else:
            stored = store.set_enabled(monitor_id, body.enabled)
            refresh_requested = False
            automatic_state = None
        return {
            "status": "APPLIED",
            "enabled": stored.enabled,
            "updated_at": iso_utc(stored.updated_at),
            "refresh_requested": refresh_requested,
            "automatic_collection": _automatic_collection_payload(automatic_state),
        }

    @app.post("/api/monitors/{monitor_id}/refresh")
    def refresh_monitor(monitor_id: str, request: Request) -> dict[str, Any]:
        origin = request.headers.get("origin")
        expected_origin = f"{request.url.scheme}://{request.url.netloc}"
        if origin is not None and origin.rstrip("/") != expected_origin:
            raise HTTPException(status_code=403, detail="ORIGIN_NOT_ALLOWED")
        try:
            registry.get(monitor_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="MONITOR_NOT_FOUND") from None
        if scheduler is None or not scheduler.started:
            raise HTTPException(status_code=503, detail="SCHEDULER_NOT_RUNNING")
        if not store.is_enabled(monitor_id):
            raise HTTPException(status_code=409, detail="MONITOR_DISABLED")
        latest = store.latest_run(monitor_id)
        if not scheduler.request_run(monitor_id):
            raise HTTPException(status_code=409, detail="REFRESH_NOT_ACCEPTED")
        return {
            "status": "ACCEPTED",
            "manual": True,
            "run_after": latest.run_id if latest is not None else 0,
            "automatic_collection": _automatic_collection_payload(
                scheduler.automatic_collection_state(monitor_id, now=utc_now())
            ),
        }

    @app.post("/api/monitors/{monitor_id}/observe")
    def observe_monitor(monitor_id: str, request: Request) -> dict[str, Any]:
        origin = request.headers.get("origin")
        expected_origin = f"{request.url.scheme}://{request.url.netloc}"
        if origin is not None and origin.rstrip("/") != expected_origin:
            raise HTTPException(status_code=403, detail="ORIGIN_NOT_ALLOWED")
        try:
            monitor = registry.get(monitor_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="MONITOR_NOT_FOUND") from None
        if getattr(monitor, "foreground_interval_seconds", None) is None:
            raise HTTPException(
                status_code=409,
                detail="MONITOR_FOREGROUND_CADENCE_UNSUPPORTED",
            )
        if scheduler is None or not scheduler.started:
            raise HTTPException(status_code=503, detail="SCHEDULER_NOT_RUNNING")
        result = scheduler.observe_monitor(
            monitor_id,
            lease_seconds=DEFAULT_OBSERVATION_LEASE_SECONDS,
        )
        cadence = result.cadence
        return {
            "status": "ACTIVE",
            "refresh_requested": result.refresh_requested,
            "lease_seconds": DEFAULT_OBSERVATION_LEASE_SECONDS,
            "collection_cadence": {
                "adaptive": True,
                "background_interval_seconds": (cadence.background_interval_seconds),
                "foreground_interval_seconds": (cadence.foreground_interval_seconds),
                "effective_interval_seconds": cadence.effective_interval_seconds,
                "foreground_active": cadence.foreground_active,
            },
        }

    @app.put("/api/monitors/{monitor_id}/configuration")
    def update_configuration(
        monitor_id: str,
        body: ConfigurationRequest,
        request: Request,
    ) -> dict[str, Any]:
        origin = request.headers.get("origin")
        expected_origin = f"{request.url.scheme}://{request.url.netloc}"
        if origin is not None and origin.rstrip("/") != expected_origin:
            raise HTTPException(status_code=403, detail="ORIGIN_NOT_ALLOWED")
        try:
            monitor = registry.get(monitor_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="MONITOR_NOT_FOUND") from None
        if not isinstance(monitor, ConfigurableMonitor):
            raise HTTPException(
                status_code=409,
                detail="MONITOR_CONFIGURATION_UNSUPPORTED",
            )
        try:
            normalized = monitor.normalize_configuration(body.values)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

        previous = monitor.configuration()
        monitor.apply_configuration(normalized)
        try:
            stored = store.save_configuration(monitor_id, normalized)
        except Exception:
            monitor.apply_configuration(previous)
            raise
        refresh_requested = (
            scheduler.request_run(monitor_id) if scheduler is not None else False
        )
        return {
            "status": "APPLIED",
            "refresh_requested": refresh_requested,
            "configuration": {
                "values": normalized,
                "updated_at": iso_utc(stored.updated_at),
            },
        }

    @app.get("/api/buybacks/entities/{entity_key}")
    def buyback_entity_detail(entity_key: str) -> dict[str, Any]:
        if not entity_key or len(entity_key) > 256:
            raise HTTPException(status_code=404, detail="BUYBACK_ENTITY_NOT_FOUND")
        entity = store.buyback_entity(BUYBACK_MONITOR_ID, entity_key)
        if entity is None:
            raise HTTPException(status_code=404, detail="BUYBACK_ENTITY_NOT_FOUND")
        revisions = store.buyback_entity_revisions(
            BUYBACK_MONITOR_ID,
            entity_key,
        )
        reviews = store.buyback_reviews(BUYBACK_MONITOR_ID, entity_key)
        document_payload = None
        if entity.document_sha256 is not None:
            document = store.buyback_document(entity.document_sha256)
            if document is not None:
                document_payload = {
                    "sha256": document.sha256,
                    "source_key": document.source_key,
                    "source_label": document.source_label,
                    "source_document_id": document.source_document_id,
                    "source_url": document.source_url,
                    "published_at": (
                        iso_utc(document.published_at)
                        if document.published_at is not None
                        else None
                    ),
                    "observed_at": iso_utc(document.observed_at),
                    "media_type": document.media_type,
                    "size_bytes": document.size_bytes,
                    "quality_state": document.quality_state,
                    "metadata": document.metadata,
                    "local_url": f"/api/buybacks/documents/{document.sha256}",
                }
        base_payload = _buyback_entity_payload(entity)
        projection = buyback_projection(BUYBACK_MONITOR_ID, now=utc_now())
        entity_payload = {
            **base_payload,
            **projection["rows_by_key"].get(entity_key, {}),
        }
        return {
            "entity": entity_payload,
            "document": document_payload,
            "reviews": [_buyback_review_payload(review) for review in reviews],
            "revisions": [
                {
                    "revision_no": revision.revision_no,
                    "revision_id": revision.revision_id,
                    "effective_at": iso_utc(revision.effective_at),
                    "observed_at": iso_utc(revision.observed_at),
                    "source_key": revision.source_key,
                    "document_sha256": revision.document_sha256,
                    "payload_sha256": revision.payload_sha256,
                    "payload": revision.payload,
                }
                for revision in revisions
            ],
        }

    @app.post("/api/buybacks/entities/{entity_key}/reviews")
    def create_buyback_review(
        entity_key: str,
        body: BuybackReviewRequest,
        request: Request,
    ) -> dict[str, Any]:
        origin = request.headers.get("origin")
        expected_origin = f"{request.url.scheme}://{request.url.netloc}"
        if origin is not None and origin.rstrip("/") != expected_origin:
            raise HTTPException(status_code=403, detail="ORIGIN_NOT_ALLOWED")
        if not entity_key or len(entity_key) > 256:
            raise HTTPException(status_code=404, detail="BUYBACK_ENTITY_NOT_FOUND")
        try:
            review = store.save_buyback_review(
                BUYBACK_MONITOR_ID,
                entity_key,
                base_revision_no=body.base_revision_no,
                decision=body.decision,
                corrected_event_type=body.corrected_event_type,
                program_key=body.program_key,
                program_status=body.program_status,
                note=body.note,
            )
        except KeyError:
            raise HTTPException(
                status_code=404,
                detail="BUYBACK_ENTITY_NOT_FOUND",
            ) from None
        except RuntimeError as exc:
            if str(exc) == "BUYBACK_REVISION_CONFLICT":
                raise HTTPException(status_code=409, detail=str(exc)) from None
            raise
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        refreshed = store.buyback_entity(BUYBACK_MONITOR_ID, entity_key)
        if refreshed is None:
            raise RuntimeError("BUYBACK_ENTITY_MISSING_AFTER_REVIEW")
        return {
            "status": "APPLIED",
            "review": _buyback_review_payload(review),
            "entity": _buyback_entity_payload(refreshed),
        }

    @app.get("/api/buybacks/documents/{sha256}")
    def buyback_document(sha256: str) -> FileResponse:
        document = store.buyback_document(sha256.casefold())
        if document is None:
            raise HTTPException(status_code=404, detail="BUYBACK_DOCUMENT_NOT_FOUND")
        try:
            path = store.buyback_document_path(document)
        except RuntimeError as exc:
            if str(exc) == "BUYBACK_EVIDENCE_FILE_MISSING":
                raise HTTPException(
                    status_code=410,
                    detail="BUYBACK_DOCUMENT_FILE_MISSING",
                ) from None
            if str(exc) in {
                "BUYBACK_EVIDENCE_LINK_UNSUPPORTED",
                "BUYBACK_EVIDENCE_PATH_INVALID",
                "BUYBACK_EVIDENCE_SIZE_MISMATCH",
                "BUYBACK_EVIDENCE_CONTENT_MISMATCH",
            }:
                raise HTTPException(
                    status_code=409,
                    detail="BUYBACK_DOCUMENT_INTEGRITY_FAILED",
                ) from None
            raise
        return FileResponse(
            path,
            media_type=document.media_type,
            filename=f"{document.source_document_id}{path.suffix}",
            content_disposition_type="inline",
        )

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        return Response(status_code=204)

    return app
