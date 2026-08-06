"""Explicit monitor registry and one-thread-per-monitor scheduler."""

from __future__ import annotations

import random
import re
import threading
import time
from collections.abc import Iterator
from dataclasses import replace

from halpha_monitor.contracts import ForwardEvaluatingMonitor, RegisteredMonitor
from halpha_monitor.store import SQLiteMonitorStore, StoredControl, utc_now


MONITOR_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")


class MonitorRegistry:
    """Explicit registration avoids dynamic plugin discovery and hidden side effects."""

    def __init__(self) -> None:
        self._monitors: dict[str, RegisteredMonitor] = {}

    def register(self, monitor: RegisteredMonitor) -> None:
        if not MONITOR_ID_PATTERN.fullmatch(monitor.monitor_id):
            raise ValueError(f"MONITOR_ID_INVALID id={monitor.monitor_id}")
        if monitor.monitor_id in self._monitors:
            raise ValueError(f"MONITOR_ID_DUPLICATE id={monitor.monitor_id}")
        if monitor.interval_seconds < 15:
            raise ValueError("MONITOR_INTERVAL_TOO_SHORT")
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
        stop_timeout_seconds: float = 5,
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
        self._threads: list[threading.Thread] = []
        self._wake_events: dict[str, threading.Event] = {}
        self._maintenance_lock = threading.Lock()
        self._next_maintenance_at = 0.0
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        self._next_maintenance_at = 0.0
        for monitor in self.registry:
            self.store.ensure_control(
                monitor.monitor_id,
                default_enabled=bool(getattr(monitor, "default_enabled", True)),
            )
            wake_event = threading.Event()
            self._wake_events[monitor.monitor_id] = wake_event
            thread = threading.Thread(
                target=self._loop,
                args=(monitor, wake_event),
                name=f"halpha-monitor:{monitor.monitor_id}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop_event.set()
        for wake_event in self._wake_events.values():
            wake_event.set()
        for thread in self._threads:
            thread.join(timeout=self.stop_timeout_seconds)
        if any(thread.is_alive() for thread in self._threads):
            raise RuntimeError("MONITOR_STOP_TIMEOUT")
        self._threads.clear()
        self._wake_events.clear()
        self._started = False

    def request_run(self, monitor_id: str) -> bool:
        self.registry.get(monitor_id)
        if not self.store.is_enabled(monitor_id):
            return False
        wake_event = self._wake_events.get(monitor_id)
        if not self._started or wake_event is None:
            return False
        wake_event.set()
        return True

    @property
    def started(self) -> bool:
        return self._started

    def set_enabled(self, monitor_id: str, enabled: bool) -> StoredControl:
        monitor = self.registry.get(monitor_id)
        self.store.ensure_control(
            monitor_id,
            default_enabled=bool(getattr(monitor, "default_enabled", True)),
        )
        stored = self.store.set_enabled(monitor_id, enabled)
        wake_event = self._wake_events.get(monitor_id)
        if wake_event is not None:
            wake_event.set()
        return stored

    def run_once(self, monitor: RegisteredMonitor) -> None:
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
                    except Exception as exc:
                        # Follow-up validation must not invalidate the current
                        # market snapshot. Pending cases remain durable and are
                        # retried by the next bounded collection cycle.
                        print(
                            "MONITOR_EVALUATION_FAILED "
                            f"id={monitor.monitor_id} type={type(exc).__name__}",
                            flush=True,
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
        except Exception as exc:
            reason_code = f"COLLECTION_FAILED_{type(exc).__name__.upper()}"
            self.store.fail_run(run_id, monitor.monitor_id, reason_code)
        self._run_maintenance_if_due()

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
                self.store.prune(self.retention_days)
            except Exception as exc:
                print(
                    f"MONITOR_MAINTENANCE_FAILED type={type(exc).__name__}",
                    flush=True,
                )

    def _loop(
        self,
        monitor: RegisteredMonitor,
        wake_event: threading.Event,
    ) -> None:
        while not self._stop_event.is_set():
            if not self.store.is_enabled(monitor.monitor_id):
                wake_event.wait()
                wake_event.clear()
                continue
            try:
                self.run_once(monitor)
            except Exception as exc:
                print(
                    f"MONITOR_LOOP_FAILED id={monitor.monitor_id} type={type(exc).__name__}",
                    flush=True,
                )
            jitter_seconds = max(
                0.0, float(getattr(monitor, "jitter_seconds", 0.0))
            )
            wait_seconds = monitor.interval_seconds + (
                random.uniform(0, jitter_seconds) if jitter_seconds else 0
            )
            wake_event.wait(wait_seconds)
            wake_event.clear()
