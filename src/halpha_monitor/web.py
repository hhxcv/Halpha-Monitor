"""Read-only FastAPI surface for registered monitor data."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import math
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.trustedhost import TrustedHostMiddleware

from halpha_monitor.contracts import (
    ConfigurableMonitor,
    NetworkObservableMonitor,
    RegisteredMonitor,
)
from halpha_monitor.service import MonitorRegistry, MonitorScheduler
from halpha_monitor.store import (
    SQLiteMonitorStore,
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
ALLOWED_WINDOWS = (1, 3, 6, 12, 24, 72, 168, 336, 720)
EXPECTED_ABSENCE_REASON_CODES = frozenset({"NO_ELIGIBLE_C2C_AD"})
REQUEST_WINDOW_SECONDS = 60.0


class ConfigurationRequest(BaseModel):
    values: dict[str, Any]


class ControlRequest(BaseModel):
    enabled: bool


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
                "detail": (
                    "监控当前未开启；没有历史结果，也没有使用任何替代值。"
                ),
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
                "detail": (
                    "本轮未取得结果的具体范围已在对应数据表内标记。"
                ),
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
                "新一轮尚未完成；表格显示上一轮已通过校验的结果，"
                "无需用户处理。"
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
) -> dict[str, Any]:
    latest = store.latest_run(monitor.monitor_id)
    data_run = store.latest_sample_run(monitor.monitor_id)
    latest_issues = store.issues_for_run(latest.run_id) if latest is not None else ()
    control = store.ensure_control(
        monitor.monitor_id,
        default_enabled=bool(getattr(monitor, "default_enabled", True)),
    )
    return {
        "monitor_id": monitor.monitor_id,
        "display_name": monitor.display_name,
        "description": monitor.description,
        "interval_seconds": monitor.interval_seconds,
        "enabled": control.enabled,
        "control_updated_at": iso_utc(control.updated_at),
        "status": monitor_status(
            latest,
            interval_seconds=monitor.interval_seconds,
            now=now,
            enabled=control.enabled,
        ),
        "latest_run": _run_payload(latest),
        "data_run": _run_payload(data_run),
        "operational_status": _operational_status(latest, enabled=control.enabled),
        "data_status": _data_status(
            latest,
            data_run,
            interval_seconds=monitor.interval_seconds,
            now=now,
            user_can_configure=isinstance(monitor, ConfigurableMonitor),
            enabled=control.enabled,
            latest_issues=latest_issues,
        ),
    }


def _overall_data_status(summaries: list[dict[str, Any]]) -> tuple[str, str]:
    active = [item for item in summaries if bool(item["enabled"])]
    if not active:
        return "DISABLED", "监控均已关闭"
    if any(
        str(item["operational_status"]["kind"]) == "COLLECTING"
        for item in active
    ):
        return "RUNNING", "采集中"
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
    utilization = 0.0
    latest_completed_at: datetime | None = None
    collecting_count = 0
    request_count = 0
    measured_monitors = 0
    for monitor, summary in active:
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
        utilization += work_seconds / max(float(monitor.interval_seconds), 1.0)

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
            sum(60.0 / float(monitor.interval_seconds) for monitor, _ in active),
            2,
        ),
        "network_requests": (
            request_count if measured_monitors else None
        ),
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
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/view")
    def view(
        request: Request,
        monitor_id: str | None = None,
        hours: int = Query(default=6),
        series_key: str | None = None,
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
        summaries = [_monitor_summary(monitor, store, now=now) for monitor in monitors]
        selected_summary = next(
            item for item in summaries if item["monitor_id"] == selected.monitor_id
        )
        selected_filters: dict[str, str] = {}
        filter_payload: list[dict[str, Any]] = []
        for definition in selected.view.filters:
            requested = request.query_params.get(definition.key, definition.default)
            allowed = {choice.value for choice in definition.choices}
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
                    "choices": [
                        {"value": choice.value, "label": choice.label}
                        for choice in definition.choices
                    ],
                }
            )

        data_run = store.latest_sample_run(selected.monitor_id)
        samples = store.samples_for_run(data_run.run_id) if data_run else ()
        rows = [
            sample
            for sample in samples
            if all(
                value == "*" or str(sample.payload.get(key, "")) == value
                for key, value in selected_filters.items()
            )
        ]
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
        issues = store.recent_issues(selected.monitor_id, limit=20)
        latest_run = store.latest_run(selected.monitor_id)
        current_issues = (
            store.issues_for_run(latest_run.run_id) if latest_run is not None else ()
        )

        service_status, service_status_label = _overall_data_status(summaries)
        return {
            "server_time": iso_utc(now),
            "service_status": service_status,
            "service_status_label": service_status_label,
            "collection_load": _collection_load(monitors, summaries, now=now),
            "monitors": summaries,
            "monitor": {
                **selected_summary,
                "filters": filter_payload,
                "selected_filters": selected_filters,
                "columns": [
                    {
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
                    for column in selected.view.columns
                ],
                "table_title": selected.view.table_title,
                "chart_title": selected.view.chart_title,
                "method_note": selected.view.method_note,
                "show_description": selected.view.show_description,
                "data_run": _run_payload(data_run),
                "configuration": _configuration_payload(selected, store),
            },
            "rows": [_sample_payload(sample) for sample in rows],
            "selected_series_key": selected_series,
            "history": history_points,
            "collection_gaps": collection_gaps,
            "current_issues": [_issue_payload(issue) for issue in current_issues],
            "issues": [_issue_payload(issue) for issue in issues],
            "time_windows": [
                {
                    "hours": value,
                    "label": (
                        f"{value // 24}天" if value >= 24 else f"{value}小时"
                    ),
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
            # set_enabled() already wakes the monitor thread. A second
            # request_run() can race with the thread clearing its event and
            # accidentally start an immediate duplicate collection.
            refresh_requested = bool(body.enabled and scheduler.started)
        else:
            stored = store.set_enabled(monitor_id, body.enabled)
            refresh_requested = False
        return {
            "status": "APPLIED",
            "enabled": stored.enabled,
            "updated_at": iso_utc(stored.updated_at),
            "refresh_requested": refresh_requested,
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

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        return Response(status_code=204)

    return app
