"""Explicit monitor registry and one-worker-per-monitor scheduler."""

from __future__ import annotations

import random
import re
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import datetime
import math

from halpha_monitor.contracts import (
    AutomaticCollectionMonitor,
    AutomaticCollectionState,
    CollectionCancelled,
    CooperativeMonitor,
    ForwardEvaluatingMonitor,
    RegisteredMonitor,
)
from halpha_monitor.store import SQLiteMonitorStore, StoredControl, utc_now


MONITOR_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")
DEFAULT_OBSERVATION_LEASE_SECONDS = 45.0


def _safe_runtime_log(message: str) -> None:
    """Best-effort stdout diagnostics must never stop collection workers."""
    try:
        print(message, flush=True)
    except (OSError, ValueError):
        return


def _runtime_exception_context(exc: Exception) -> dict[str, str | int]:
    """Keep path-free code location without persisting exception messages."""

    context: dict[str, str | int] = {
        "exception_type": type(exc).__name__[:80],
    }
    project_location: tuple[str, str, int] | None = None
    fallback_location: tuple[str, str, int] | None = None
    traceback = exc.__traceback__
    while traceback is not None:
        module = str(traceback.tb_frame.f_globals.get("__name__") or "")[:160]
        function = str(traceback.tb_frame.f_code.co_name or "")[:120]
        if (
            re.fullmatch(r"[A-Za-z0-9_.]+", module)
            and re.fullmatch(r"[A-Za-z0-9_<>]+", function)
        ):
            location = (module, function, int(traceback.tb_lineno))
            fallback_location = location
            if module == "halpha_monitor" or module.startswith("halpha_monitor."):
                project_location = location
        traceback = traceback.tb_next
    selected = fallback_location or project_location
    if selected is not None:
        context.update(
            {
                "origin_module": selected[0],
                "origin_function": selected[1],
                "origin_line": selected[2],
            }
        )
    if project_location is not None and project_location != selected:
        context.update(
            {
                "boundary_module": project_location[0],
                "boundary_function": project_location[1],
                "boundary_line": project_location[2],
            }
        )
    return context


def _runtime_location(context: dict[str, str | int]) -> str:
    module = context.get("origin_module", "unknown")
    function = context.get("origin_function", "unknown")
    line = context.get("origin_line", 0)
    return f"{module}:{function}:{line}"


@dataclass(frozen=True)
class WorkerState:
    monitor_id: str
    alive: bool
    collecting: bool
    manual_run_pending: bool
    last_seen_at: datetime | None
    last_error: str | None


@dataclass(frozen=True)
class CollectionCadence:
    monitor_id: str
    background_interval_seconds: float
    foreground_interval_seconds: float | None
    effective_interval_seconds: float
    foreground_active: bool


@dataclass(frozen=True)
class ObservationResult:
    cadence: CollectionCadence
    refresh_requested: bool


class MonitorRegistry:
    """Explicit registration avoids dynamic plugin discovery and hidden side effects."""

    def __init__(self) -> None:
        self._monitors: dict[str, RegisteredMonitor] = {}

    def register(self, monitor: RegisteredMonitor) -> None:
        if not MONITOR_ID_PATTERN.fullmatch(monitor.monitor_id):
            raise ValueError(f"MONITOR_ID_INVALID id={monitor.monitor_id}")
        if monitor.monitor_id in self._monitors:
            raise ValueError(f"MONITOR_ID_DUPLICATE id={monitor.monitor_id}")
        if not math.isfinite(float(monitor.interval_seconds)):
            raise ValueError("MONITOR_INTERVAL_INVALID")
        if monitor.interval_seconds < 15:
            raise ValueError("MONITOR_INTERVAL_TOO_SHORT")
        foreground_interval = getattr(
            monitor,
            "foreground_interval_seconds",
            None,
        )
        if foreground_interval is not None:
            if not math.isfinite(float(foreground_interval)):
                raise ValueError("MONITOR_FOREGROUND_INTERVAL_INVALID")
            if float(foreground_interval) < 15:
                raise ValueError("MONITOR_FOREGROUND_INTERVAL_TOO_SHORT")
            if float(foreground_interval) > float(monitor.interval_seconds):
                raise ValueError("MONITOR_FOREGROUND_INTERVAL_EXCEEDS_BACKGROUND")
        self._monitors[monitor.monitor_id] = monitor

    def get(self, monitor_id: str) -> RegisteredMonitor:
        try:
            return self._monitors[monitor_id]
        except KeyError:
            raise KeyError(f"MONITOR_NOT_REGISTERED id={monitor_id}") from None

    def all(self) -> tuple[RegisteredMonitor, ...]:
        return tuple(self._monitors.values())

    def __iter__(self) -> Iterator[RegisteredMonitor]:
        return iter(self._monitors.values())

    def __len__(self) -> int:
        return len(self._monitors)


class MonitorScheduler:
    """Run registered monitors independently inside one shared process."""

    def __init__(
        self,
        registry: MonitorRegistry,
        store: SQLiteMonitorStore,
        *,
        retention_days: int = 90,
        stop_timeout_seconds: float = 65,
        maintenance_interval_seconds: float = 3600,
    ) -> None:
        if retention_days < 1:
            raise ValueError("retention_days must be positive")
        if stop_timeout_seconds <= 0:
            raise ValueError("stop_timeout_seconds must be positive")
        if maintenance_interval_seconds <= 0:
            raise ValueError("maintenance_interval_seconds must be positive")
        self.registry = registry
        self.store = store
        self.retention_days = retention_days
        self.stop_timeout_seconds = stop_timeout_seconds
        self.maintenance_interval_seconds = maintenance_interval_seconds
        self._stop_event = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._wake_events: dict[str, threading.Event] = {}
        self._manual_run_events: dict[str, threading.Event] = {}
        self._maintenance_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._worker_state_lock = threading.Lock()
        self._observation_lock = threading.Lock()
        self._worker_last_seen_at: dict[str, datetime] = {}
        self._worker_last_error: dict[str, str] = {}
        self._collecting: set[str] = set()
        self._observation_expires_at: dict[str, float] = {}
        self._next_maintenance_at = 0.0
        self._started_at = 0.0
        self._started = False

    def start(self) -> None:
        try:
            with self._lifecycle_lock:
                if self._started:
                    return
                monitors = self.registry.all()
                for monitor in monitors:
                    self.store.ensure_control(
                        monitor.monitor_id,
                        default_enabled=bool(
                            getattr(monitor, "default_enabled", True)
                        ),
                    )
                self._stop_event.clear()
                self._next_maintenance_at = 0.0
                self._started_at = time.monotonic()
                self._wake_events = {
                    monitor.monitor_id: threading.Event() for monitor in monitors
                }
                self._manual_run_events = {
                    monitor.monitor_id: threading.Event() for monitor in monitors
                }
                self._threads = {}
                with self._worker_state_lock:
                    self._worker_last_seen_at.clear()
                    self._worker_last_error.clear()
                    self._collecting.clear()
                with self._observation_lock:
                    self._observation_expires_at.clear()
                self._started = True
                for monitor in monitors:
                    if isinstance(monitor, CooperativeMonitor):
                        monitor.bind_stop_event(self._stop_event)
                    thread = threading.Thread(
                        target=self._loop,
                        args=(
                            monitor,
                            self._wake_events[monitor.monitor_id],
                            self._manual_run_events[monitor.monitor_id],
                        ),
                        name=f"halpha-monitor:{monitor.monitor_id}",
                        daemon=True,
                    )
                    self._threads[monitor.monitor_id] = thread
                    thread.start()
        except Exception:
            self._stop_event.set()
            with self._lifecycle_lock:
                failed_threads = tuple(self._threads.values())
                for wake_event in self._wake_events.values():
                    wake_event.set()
            deadline = time.monotonic() + self.stop_timeout_seconds
            for thread in failed_threads:
                if thread.ident is not None:
                    thread.join(timeout=max(0.0, deadline - time.monotonic()))
            with self._lifecycle_lock:
                self._threads.clear()
                self._wake_events.clear()
                self._manual_run_events.clear()
                self._started = False
            with self._observation_lock:
                self._observation_expires_at.clear()
            raise

    def stop(self) -> None:
        with self._lifecycle_lock:
            if not self._started and not self._threads:
                return
            self._stop_event.set()
            threads = tuple(self._threads.values())
            for wake_event in self._wake_events.values():
                wake_event.set()
        deadline = time.monotonic() + self.stop_timeout_seconds
        for thread in threads:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("MONITOR_STOP_TIMEOUT")
        with self._lifecycle_lock:
            self._threads.clear()
            self._wake_events.clear()
            self._manual_run_events.clear()
            self._started = False
        with self._observation_lock:
            self._observation_expires_at.clear()

    def request_run(self, monitor_id: str) -> bool:
        self.registry.get(monitor_id)
        if not self.store.is_enabled(monitor_id):
            return False
        with self._lifecycle_lock:
            wake_event = self._wake_events.get(monitor_id)
            manual_event = self._manual_run_events.get(monitor_id)
            if not self._started or wake_event is None or manual_event is None:
                return False
            # Event semantics deliberately coalesce repeated clicks into one
            # bounded pending run instead of building an in-memory queue.
            manual_event.set()
            wake_event.set()
        return True

    def collection_cadence(self, monitor_id: str) -> CollectionCadence:
        monitor = self.registry.get(monitor_id)
        background_interval = float(monitor.interval_seconds)
        raw_foreground_interval = getattr(
            monitor,
            "foreground_interval_seconds",
            None,
        )
        foreground_interval = (
            float(raw_foreground_interval)
            if raw_foreground_interval is not None
            else None
        )
        foreground_active = False
        if foreground_interval is not None:
            now = time.monotonic()
            with self._observation_lock:
                expires_at = self._observation_expires_at.get(monitor_id, 0.0)
                foreground_active = expires_at > now
                if expires_at and not foreground_active:
                    self._observation_expires_at.pop(monitor_id, None)
        return CollectionCadence(
            monitor_id=monitor_id,
            background_interval_seconds=background_interval,
            foreground_interval_seconds=foreground_interval,
            effective_interval_seconds=(
                foreground_interval
                if foreground_active and foreground_interval is not None
                else background_interval
            ),
            foreground_active=foreground_active,
        )

    def observe_monitor(
        self,
        monitor_id: str,
        *,
        lease_seconds: float = DEFAULT_OBSERVATION_LEASE_SECONDS,
    ) -> ObservationResult:
        monitor = self.registry.get(monitor_id)
        foreground_interval = getattr(
            monitor,
            "foreground_interval_seconds",
            None,
        )
        if foreground_interval is None:
            raise ValueError("MONITOR_FOREGROUND_CADENCE_UNSUPPORTED")
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("MONITOR_OBSERVATION_LEASE_INVALID")

        with self._observation_lock:
            self._observation_expires_at[monitor_id] = (
                time.monotonic() + lease_seconds
            )

        refresh_requested = False
        if self.started and self.store.is_enabled(monitor_id):
            latest = self.store.latest_run(monitor_id)
            due = latest is None
            if latest is not None and latest.status != "RUNNING":
                due = latest.completed_at is None or (
                    utc_now() - latest.completed_at
                ).total_seconds() >= float(foreground_interval)
            schedule = self.automatic_collection_state(
                monitor_id,
                now=utc_now(),
            )
            if due and (schedule is None or schedule.allowed):
                # The existing event-based request path coalesces heartbeats
                # from one or many visible tabs into at most one pending run.
                refresh_requested = self.request_run(monitor_id)

        return ObservationResult(
            cadence=self.collection_cadence(monitor_id),
            refresh_requested=refresh_requested,
        )

    @property
    def started(self) -> bool:
        with self._lifecycle_lock:
            return self._started

    @property
    def healthy(self) -> bool:
        with self._lifecycle_lock:
            if not self._started or len(self._threads) != len(self.registry):
                return False
            return all(thread.is_alive() for thread in self._threads.values())

    def worker_states(self) -> tuple[WorkerState, ...]:
        with self._lifecycle_lock:
            threads = dict(self._threads)
            manual_events = dict(self._manual_run_events)
        with self._worker_state_lock:
            last_seen = dict(self._worker_last_seen_at)
            last_errors = dict(self._worker_last_error)
            collecting = set(self._collecting)
        return tuple(
            WorkerState(
                monitor_id=monitor.monitor_id,
                alive=bool(
                    (thread := threads.get(monitor.monitor_id))
                    and thread.is_alive()
                ),
                collecting=monitor.monitor_id in collecting,
                manual_run_pending=bool(
                    (event := manual_events.get(monitor.monitor_id))
                    and event.is_set()
                ),
                last_seen_at=last_seen.get(monitor.monitor_id),
                last_error=last_errors.get(monitor.monitor_id),
            )
            for monitor in self.registry
        )

    def automatic_collection_state(
        self,
        monitor_id: str,
        *,
        now: datetime | None = None,
    ) -> AutomaticCollectionState | None:
        monitor = self.registry.get(monitor_id)
        if not isinstance(monitor, AutomaticCollectionMonitor):
            return None
        observed_at = now or utc_now()
        try:
            state = monitor.automatic_collection_state(now=observed_at)
        except Exception as exc:
            return AutomaticCollectionState(
                allowed=False,
                status="UNAVAILABLE",
                reason_code=(
                    f"AUTOMATIC_SCHEDULE_FAILED_{type(exc).__name__.upper()}"
                ),
                label="交易日历不可判定 · 自动刷新暂停",
                detail="无法确认交易时段；为避免闭市误采集，当前只允许手动刷新。",
            )
        if not isinstance(state, AutomaticCollectionState):
            return AutomaticCollectionState(
                allowed=False,
                status="UNAVAILABLE",
                reason_code="AUTOMATIC_SCHEDULE_STATE_INVALID",
                label="交易日历不可判定 · 自动刷新暂停",
                detail="交易时段状态无效；当前只允许手动刷新。",
            )
        return state

    def set_enabled(self, monitor_id: str, enabled: bool) -> StoredControl:
        monitor = self.registry.get(monitor_id)
        self.store.ensure_control(
            monitor_id,
            default_enabled=bool(getattr(monitor, "default_enabled", True)),
        )
        stored = self.store.set_enabled(monitor_id, enabled)
        with self._lifecycle_lock:
            wake_event = self._wake_events.get(monitor_id)
            manual_event = self._manual_run_events.get(monitor_id)
            if not enabled and manual_event is not None:
                manual_event.clear()
            if wake_event is not None:
                wake_event.set()
        return stored

    def run_once(self, monitor: RegisteredMonitor) -> None:
        self._set_collecting(monitor.monitor_id, True)
        run_id: int | None = None
        try:
            run_id = self.store.start_run(monitor.monitor_id)
            try:
                batch = monitor.collect()
                if isinstance(monitor, ForwardEvaluatingMonitor):
                    evaluation_now = utc_now()
                    pending = self.store.pending_forward_evaluations(
                        monitor.monitor_id,
                        due_before=evaluation_now,
                        limit=monitor.evaluation_batch_limit,
                    )
                    if pending:
                        try:
                            resolved = monitor.evaluate(pending, now=evaluation_now)
                        except CollectionCancelled:
                            raise
                        except Exception as exc:
                            # Follow-up validation must not invalidate the current
                            # market snapshot. Pending cases remain durable and are
                            # retried by the next bounded collection cycle.
                            context = _runtime_exception_context(exc)
                            _safe_runtime_log(
                                "MONITOR_EVALUATION_FAILED "
                                f"id={monitor.monitor_id} type={type(exc).__name__} "
                                f"origin={_runtime_location(context)}"
                            )
                        else:
                            batch = replace(
                                batch,
                                evaluation_results=(
                                    *batch.evaluation_results,
                                    *resolved,
                                ),
                            )
                self.store.finish_run(run_id, monitor.monitor_id, batch)
            except CollectionCancelled:
                self.store.fail_run(
                    run_id,
                    monitor.monitor_id,
                    "COLLECTION_CANCELLED",
                )
            except Exception as exc:
                reason_code = f"COLLECTION_FAILED_{type(exc).__name__.upper()}"
                context = _runtime_exception_context(exc)
                self.store.fail_run(
                    run_id,
                    monitor.monitor_id,
                    reason_code,
                    context=context,
                )
                _safe_runtime_log(
                    "MONITOR_COLLECTION_FAILED "
                    f"id={monitor.monitor_id} run_id={run_id} "
                    f"type={type(exc).__name__} "
                    f"origin={_runtime_location(context)}"
                )
            self._run_maintenance_if_due()
        finally:
            self._set_collecting(monitor.monitor_id, False)
            self._touch_worker(monitor.monitor_id)

    def _run_maintenance_if_due(self) -> None:
        now = time.monotonic()
        if now < self._next_maintenance_at:
            return
        with self._maintenance_lock:
            now = time.monotonic()
            if now < self._next_maintenance_at:
                return
            self._next_maintenance_at = now + self.maintenance_interval_seconds
            try:
                removed_runs = self.store.prune(self.retention_days)
            except Exception as exc:
                context = _runtime_exception_context(exc)
                _safe_runtime_log(
                    f"MONITOR_MAINTENANCE_FAILED type={type(exc).__name__} "
                    f"origin={_runtime_location(context)}"
                )
                return
            try:
                storage = self.store.storage_metrics()
            except Exception as exc:
                context = _runtime_exception_context(exc)
                _safe_runtime_log(
                    f"MONITOR_RUNTIME_METRICS_FAILED type={type(exc).__name__} "
                    f"origin={_runtime_location(context)}"
                )
                return
            workers = self.worker_states()
            _safe_runtime_log(
                "MONITOR_RUNTIME "
                f"uptime_seconds={max(0, round(now - self._started_at))} "
                f"workers_alive={sum(worker.alive for worker in workers)}/"
                f"{len(workers)} collecting={sum(worker.collecting for worker in workers)} "
                f"python_threads={threading.active_count()} removed_runs={removed_runs} "
                f"db_bytes={storage['database_bytes']} "
                f"wal_bytes={storage['wal_bytes']} "
                f"evidence_bytes={storage['buyback_evidence_bytes']}"
            )

    def _touch_worker(self, monitor_id: str) -> None:
        with self._worker_state_lock:
            self._worker_last_seen_at[monitor_id] = utc_now()

    def _set_collecting(self, monitor_id: str, collecting: bool) -> None:
        with self._worker_state_lock:
            if collecting:
                self._collecting.add(monitor_id)
            else:
                self._collecting.discard(monitor_id)

    def _record_loop_error(self, monitor_id: str, exc: Exception) -> None:
        error_type = type(exc).__name__
        context = _runtime_exception_context(exc)
        with self._worker_state_lock:
            self._worker_last_error[monitor_id] = error_type
            self._worker_last_seen_at[monitor_id] = utc_now()
        _safe_runtime_log(
            f"MONITOR_LOOP_FAILED id={monitor_id} type={error_type} "
            f"origin={_runtime_location(context)}"
        )

    def _regular_wait_seconds(self, monitor: RegisteredMonitor) -> float:
        requested_delay = float(monitor.interval_seconds)
        delay_provider = getattr(monitor, "next_collection_delay_seconds", None)
        if callable(delay_provider):
            try:
                candidate = float(delay_provider())
                if math.isfinite(candidate):
                    requested_delay = candidate
            except (TypeError, ValueError):
                requested_delay = float(monitor.interval_seconds)
        wait_seconds = max(
            15.0,
            min(float(monitor.interval_seconds), requested_delay),
        )
        if wait_seconds >= float(monitor.interval_seconds):
            jitter_seconds = max(
                0.0, float(getattr(monitor, "jitter_seconds", 0.0))
            )
            if math.isfinite(jitter_seconds) and jitter_seconds:
                wait_seconds += random.uniform(0, jitter_seconds)
        return wait_seconds

    @staticmethod
    def _scheduled_wait_seconds(
        state: AutomaticCollectionState,
        *,
        now: datetime,
    ) -> float:
        if state.status == "UNAVAILABLE":
            return 300.0
        if state.next_open_at is None:
            return 300.0
        seconds = (state.next_open_at - now).total_seconds() + 1.0
        # Recheck at least every six hours so a long-lived process observes
        # calendar/timezone updates without creating closed-market load.
        return max(15.0, min(seconds, 6 * 3600.0))

    def _loop(
        self,
        monitor: RegisteredMonitor,
        wake_event: threading.Event,
        manual_event: threading.Event,
    ) -> None:
        while not self._stop_event.is_set():
            self._touch_worker(monitor.monitor_id)
            wait_seconds: float | None = None
            try:
                enabled = self.store.is_enabled(monitor.monitor_id)
                if not enabled:
                    manual_event.clear()
                else:
                    manual_requested = manual_event.is_set()
                    schedule = self.automatic_collection_state(
                        monitor.monitor_id,
                        now=utc_now(),
                    )
                    if manual_requested or schedule is None or schedule.allowed:
                        if manual_requested:
                            manual_event.clear()
                        self.run_once(monitor)
                    if self._stop_event.is_set():
                        break
                    observed_at = utc_now()
                    schedule = self.automatic_collection_state(
                        monitor.monitor_id,
                        now=observed_at,
                    )
                    if schedule is not None and not schedule.allowed:
                        wait_seconds = self._scheduled_wait_seconds(
                            schedule,
                            now=observed_at,
                        )
                    else:
                        wait_seconds = self._regular_wait_seconds(monitor)
            except CollectionCancelled:
                if self._stop_event.is_set():
                    break
                wait_seconds = 15.0
            except Exception as exc:
                self._record_loop_error(monitor.monitor_id, exc)
                wait_seconds = 30.0
            wake_event.wait(wait_seconds)
            wake_event.clear()
