"""Small registration contract shared by independently developed monitors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import threading
from typing import Any, Literal, Protocol, Sequence, runtime_checkable


ColumnKind = Literal["text", "number", "percent", "time"]
ColumnPriority = Literal["primary", "secondary"]
ConfigurationKind = Literal["decimal", "multi_choice", "stock_list"]
EvaluationDirection = Literal["UP", "DOWN"]
EvaluationStatus = Literal["COMPLETE", "UNAVAILABLE"]
EvaluationVerdict = Literal["ALIGNED", "INCONCLUSIVE", "OPPOSED", "UNAVAILABLE"]
BuybackEntityType = Literal["DISCLOSURE_CANDIDATE", "HKEX_EXECUTION"]
BuybackSourceStatus = Literal["SUCCESS", "EMPTY", "PARTIAL", "STALE", "ERROR"]
AutomaticCollectionStatus = Literal["OPEN", "CLOSED", "UNAVAILABLE"]
BtcStructureEventState = Literal["PENDING", "REACTION", "BREAK", "UNRESOLVED"]
BtcMonthlyResearchState = Literal["SIGNAL_FROZEN", "EXECUTION_CAPTURED"]


class CollectionCancelled(RuntimeError):
    """Cooperative shutdown signal that must not be converted into source data."""


@dataclass(frozen=True)
class AutomaticCollectionState:
    """Current state of an optional wall-clock gate for automatic collection."""

    allowed: bool
    status: AutomaticCollectionStatus
    reason_code: str
    label: str
    detail: str
    next_open_at: datetime | None = None
    active_until: datetime | None = None


@dataclass(frozen=True)
class FilterChoice:
    value: str
    label: str
    description: str | None = None


@dataclass(frozen=True)
class ViewFilter:
    key: str
    label: str
    default: str | tuple[str, ...]
    choices: tuple[FilterChoice, ...]
    multiple: bool = False


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
    promote_when_uniform: bool = False
    uniform_summary_label: str | None = None


@dataclass(frozen=True)
class ViewSummaryField:
    key: str
    label: str
    description: str | None = None


@dataclass(frozen=True)
class EvaluationView:
    title: str
    method_note: str
    minimum_group_samples: int = 30
    minimum_distinct_cutoffs: int = 20
    minimum_distinct_entities: int = 15
    minimum_observation_days: float = 14.0


@dataclass(frozen=True)
class MonitorView:
    filters: tuple[ViewFilter, ...]
    columns: tuple[ViewColumn, ...]
    chart_title: str
    table_title: str = "最新监控数据"
    method_note: str | None = None
    show_description: bool = True
    summary_fields: tuple[ViewSummaryField, ...] = ()
    evaluation: EvaluationView | None = None


@dataclass(frozen=True)
class ConfigurationField:
    key: str
    label: str
    kind: ConfigurationKind
    unit: str | None = None
    minimum: str | None = None
    step: str | None = None
    choices: tuple[FilterChoice, ...] = ()
    description: str | None = None
    placeholder: str | None = None
    maximum_items: int | None = None


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
    context: dict[str, str | int | float | bool | None] | None = None


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
class BuybackEvidenceDocument:
    """Immutable official source document prepared for content-addressed storage."""

    source_key: str
    source_label: str
    source_document_id: str
    source_url: str
    published_at: datetime | None
    observed_at: datetime
    media_type: str
    file_suffix: str
    body: bytes
    quality_state: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class BuybackEntityRevision:
    """Self-contained candidate or execution fact revision."""

    entity_key: str
    entity_type: BuybackEntityType
    effective_at: datetime
    observed_at: datetime
    source_key: str
    document_sha256: str | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class BuybackSourceObservation:
    """Latest checked state for one independently fail-able public source."""

    source_key: str
    source_label: str
    status: BuybackSourceStatus
    checked_at: datetime
    source_time: datetime | None
    next_due_at: datetime
    record_count: int | None
    detail_code: str | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class MarketEventRevision:
    """One durable revision of a market event observed after history starts."""

    event_key: str
    scheduled_at: datetime
    observed_at: datetime
    state: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class BtcStructureHistoryObservation:
    """Forward-only ledger clock for causal BTC 4h structure events."""

    started_at: datetime
    processed_through_at: datetime
    algorithm_version: str


@dataclass(frozen=True)
class BtcStructureEventRevision:
    """Immutable signal or outcome revision for one BTC 4h touch event."""

    event_key: str
    event_at: datetime
    observed_at: datetime
    state: BtcStructureEventState
    payload: dict[str, Any]


@dataclass(frozen=True)
class BtcMonthlyResearchHistoryObservation:
    """Forward-only clock for monthly Faber signals and execution proxies."""

    started_at: datetime
    processed_through_at: datetime
    algorithm_version: str


@dataclass(frozen=True)
class BtcMonthlyResearchRevision:
    """Immutable signal or execution revision for one completed UTC month."""

    signal_key: str
    signal_at: datetime
    observed_at: datetime
    state: BtcMonthlyResearchState
    payload: dict[str, Any]


@dataclass(frozen=True)
class ForwardEvaluationCase:
    """A signal-time observation frozen before its future outcome is known."""

    case_key: str
    entity_key: str
    stage: str
    stage_label: str
    direction: EvaluationDirection
    signal_observed_at: datetime
    source_cutoff_at: datetime
    horizon_minutes: int
    due_at: datetime
    entry_price_text: str
    benchmark_entry_price_text: str
    source: str


@dataclass(frozen=True)
class ForwardEvaluationResult:
    """A forward outcome resolved from closed public-market candles."""

    case_key: str
    status: EvaluationStatus
    evaluated_at: datetime
    outcome_cutoff_at: datetime | None
    exit_price_text: str | None
    benchmark_exit_price_text: str | None
    forward_return_percent: float | None
    benchmark_return_percent: float | None
    relative_return_percent: float | None
    maximum_favorable_excursion_percent: float | None
    maximum_adverse_excursion_percent: float | None
    verdict: EvaluationVerdict
    reason_code: str | None = None


@dataclass(frozen=True)
class ProjectionSnapshot:
    """Bounded current-only projection committed atomically with one run.

    This is for a large decision view whose latest rows must survive a process
    restart without appending a duplicate copy on every collection cycle.
    """

    snapshot_key: str
    observed_at: datetime
    cutoff_at: datetime
    payload: dict[str, Any]


@dataclass(frozen=True)
class CollectionBatch:
    samples: tuple[MetricSample, ...]
    issues: tuple[CollectionIssue, ...] = ()
    artifacts: tuple[CollectionArtifact, ...] = ()
    evaluation_cases: tuple[ForwardEvaluationCase, ...] = ()
    evaluation_results: tuple[ForwardEvaluationResult, ...] = ()
    buyback_documents: tuple[BuybackEvidenceDocument, ...] = ()
    buyback_revisions: tuple[BuybackEntityRevision, ...] = ()
    buyback_source_observations: tuple[BuybackSourceObservation, ...] = ()
    market_event_revisions: tuple[MarketEventRevision, ...] = ()
    btc_structure_history: BtcStructureHistoryObservation | None = None
    btc_structure_event_revisions: tuple[BtcStructureEventRevision, ...] = ()
    btc_monthly_research_history: BtcMonthlyResearchHistoryObservation | None = None
    btc_monthly_research_revisions: tuple[BtcMonthlyResearchRevision, ...] = ()
    projection_snapshots: tuple[ProjectionSnapshot, ...] = ()


class RegisteredMonitor(Protocol):
    monitor_id: str
    display_name: str
    description: str
    interval_seconds: float
    view: MonitorView

    def collect(self) -> CollectionBatch: ...


@runtime_checkable
class AutomaticCollectionMonitor(Protocol):
    """Optional schedule gate; explicit manual runs may bypass this state."""

    monitor_id: str

    def automatic_collection_state(
        self,
        *,
        now: datetime,
    ) -> AutomaticCollectionState: ...


@runtime_checkable
class CooperativeMonitor(Protocol):
    """Optional binding for promptly stopping new network work on shutdown."""

    def bind_stop_event(self, stop_event: threading.Event) -> None: ...


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


@runtime_checkable
class ForwardEvaluatingMonitor(Protocol):
    """Optional bounded follow-up evaluation performed by the same worker."""

    monitor_id: str
    evaluation_batch_limit: int

    def evaluate(
        self,
        cases: Sequence[ForwardEvaluationCase],
        *,
        now: datetime,
    ) -> tuple[ForwardEvaluationResult, ...]: ...
