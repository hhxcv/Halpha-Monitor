"""Bounded in-process telemetry for public HTTP request activity."""

from __future__ import annotations

from collections import deque
import threading
import time
from collections.abc import Callable


class NetworkRequestWindow:
    """Count request attempts in a recent monotonic-time window.

    The counter deliberately stores no URLs, parameters, response bodies, or
    host details. It is process-local, bounded, and intended only for the live
    workload indicator in the local UI.
    """

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        retention_seconds: float = 300,
        max_events: int = 10_000,
    ) -> None:
        if retention_seconds <= 0 or max_events < 1:
            raise ValueError("NETWORK_REQUEST_WINDOW_CONFIGURATION_INVALID")
        self._monotonic = monotonic
        self._retention_seconds = retention_seconds
        self._events: deque[float] = deque(maxlen=max_events)
        self._lock = threading.Lock()

    def record(self) -> None:
        now = self._monotonic()
        with self._lock:
            self._events.append(now)
            self._prune(now, self._retention_seconds)

    def count(self, *, window_seconds: float = 60) -> int:
        if window_seconds <= 0 or window_seconds > self._retention_seconds:
            raise ValueError("NETWORK_REQUEST_WINDOW_RANGE_INVALID")
        now = self._monotonic()
        with self._lock:
            self._prune(now, self._retention_seconds)
            cutoff = now - window_seconds
            return sum(observed_at >= cutoff for observed_at in self._events)

    def _prune(self, now: float, window_seconds: float) -> None:
        cutoff = now - window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
