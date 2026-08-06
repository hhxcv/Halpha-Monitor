"""Small registration contract shared by independently developed monitors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol, runtime_checkable


ColumnKind = Literal["text", "number", "percent", "time"]
ColumnPriority = Literal["primary", "secondary"]
ConfigurationKind = Literal["decimal", "multi_choice"]


@dataclass(frozen=True)
class FilterChoice:
    value: str
    label: str


@dataclass(frozen=True)
class ViewFilter:
    key: str
    label: str
    default: str
    choices: tuple[FilterChoice, ...]


@dataclass(frozen=True)
class ViewColumn:
    key: str
    label: str
    kind: ColumnKind = "text"
    priority: ColumnPriority = "primary"
    minimum_fraction_digits: int = 0
    maximum_fraction_digits: int = 8
    use_grouping: bool = False
    show_sign: bool = True
    description: str | None = None


@dataclass(frozen=True)
class MonitorView:
    filters: tuple[ViewFilter, ...]
    columns: tuple[ViewColumn, ...]
    chart_title: str
    table_title: str = "最新监控数据"
    method_note: str | None = None
    show_description: bool = True


@dataclass(frozen=True)
class ConfigurationField:
    key: str
    label: str
    kind: ConfigurationKind
    unit: str | None = None
    minimum: str | None = None
    step: str | None = None
    choices: tuple[FilterChoice, ...] = ()


@dataclass(frozen=True)
class MetricSample:
    series_key: str
    entity_key: str
    observed_at: datetime
    value_text: str
    unit: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class CollectionIssue:
    scope: str
    reason_code: str


@dataclass(frozen=True)
class CollectionArtifact:
    artifact_key: str
    source: str
    request_started_at: datetime
    response_completed_at: datetime
    http_status: int
    business_code: str | None
    schema_hash: str
    response_sha256: str
    record_count: int | None
    response_body: str


@dataclass(frozen=True)
class CollectionBatch:
    samples: tuple[MetricSample, ...]
    issues: tuple[CollectionIssue, ...] = ()
    artifacts: tuple[CollectionArtifact, ...] = ()


class RegisteredMonitor(Protocol):
    monitor_id: str
    display_name: str
    description: str
    interval_seconds: float
    view: MonitorView

    def collect(self) -> CollectionBatch: ...


@runtime_checkable
class NetworkObservableMonitor(Protocol):
    """Optional process-local count of actual public HTTP request attempts."""

    def network_request_count(self, *, window_seconds: float = 60) -> int | None: ...


@runtime_checkable
class ConfigurableMonitor(Protocol):
    """Optional UI configuration applied while collection threads may be active.

    Implementations must make configuration reads and updates atomic, and collect()
    must use one immutable snapshot for a complete batch.
    """

    monitor_id: str
    configuration_fields: tuple[ConfigurationField, ...]

    def configuration(self) -> dict[str, Any]: ...

    def normalize_configuration(self, values: dict[str, Any]) -> dict[str, Any]: ...

    def apply_configuration(self, values: dict[str, Any]) -> None: ...
