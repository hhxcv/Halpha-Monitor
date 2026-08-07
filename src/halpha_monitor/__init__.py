"""Low-complexity, read-only monitoring service."""

import os


# The numerical relationship monitor gains no measurable throughput from a
# process-sized OpenBLAS pool, while every idle native worker remains resident.
# Respect an explicit operator choice and otherwise keep the shared service small.
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

from halpha_monitor.contracts import (
    AutomaticCollectionState,
    CollectionBatch,
    CollectionArtifact,
    CollectionIssue,
    ConfigurableMonitor,
    ConfigurationField,
    FilterChoice,
    MetricSample,
    MonitorView,
    RegisteredMonitor,
    ViewColumn,
    ViewFilter,
)
from halpha_monitor.service import MonitorRegistry, MonitorScheduler
from halpha_monitor.store import SQLiteMonitorStore

__all__ = [
    "AutomaticCollectionState",
    "CollectionBatch",
    "CollectionArtifact",
    "CollectionIssue",
    "ConfigurableMonitor",
    "ConfigurationField",
    "FilterChoice",
    "MetricSample",
    "MonitorRegistry",
    "MonitorScheduler",
    "MonitorView",
    "RegisteredMonitor",
    "SQLiteMonitorStore",
    "ViewColumn",
    "ViewFilter",
]
