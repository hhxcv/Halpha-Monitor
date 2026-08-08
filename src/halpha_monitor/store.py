"""Durable SQLite storage for monitor runs, samples, and collection issues."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from collections.abc import Iterator
from typing import Any, Literal
from uuid import uuid4

from halpha_monitor.contracts import (
    BtcMonthlyResearchRevision,
    BtcStructureEventRevision,
    BuybackEvidenceDocument,
    CollectionBatch,
    ForwardEvaluationCase,
    ProjectionSnapshot,
)


RunStatus = Literal["RUNNING", "SUCCESS", "PARTIAL", "FAILED"]
SCHEMA_VERSION = 11
MAX_PROJECTION_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_ISSUE_CONTEXT_BYTES = 2 * 1024
MAX_ISSUE_CONTEXT_FIELDS = 16
MAX_ISSUE_CONTEXT_STRING_LENGTH = 512
MAX_BUYBACK_DOCUMENT_BYTES = 20 * 1024 * 1024
DEFAULT_BUYBACK_RETENTION_DAYS = 1095
DEFAULT_BUYBACK_EVIDENCE_MAX_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MARKET_EVENT_RETENTION_DAYS = 1095
DEFAULT_BTC_STRUCTURE_RETENTION_DAYS = 1825
DEFAULT_BTC_STRUCTURE_MAX_EVENTS = 20_000
DEFAULT_BTC_MONTHLY_MAX_SIGNALS = 240
BUYBACK_DOCUMENT_SUFFIXES = frozenset({".pdf", ".xls", ".xlsx", ".json", ".html"})
BUYBACK_REVIEW_EVENT_TYPES = frozenset(
    {
        "PLAN_OR_APPROVAL",
        "FIRST_EXECUTION",
        "PROGRESS",
        "MODIFICATION",
        "COMPLETION_OR_TERMINATION",
        "POST_BUYBACK_CANCELLATION",
        "POST_BUYBACK_DISPOSAL",
        "AMBIGUOUS_BUYBACK",
        "HKEX_EXECUTION",
    }
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime) -> str:
    if value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _issue_context_json(
    context: dict[str, str | int | float | bool | None] | None,
) -> str:
    if context is None:
        return "{}"
    if not isinstance(context, dict) or len(context) > MAX_ISSUE_CONTEXT_FIELDS:
        raise RuntimeError("MONITOR_ISSUE_CONTEXT_INVALID")
    normalized: dict[str, str | int | float | bool | None] = {}
    for key, value in context.items():
        if not isinstance(key, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key) is None:
            raise RuntimeError("MONITOR_ISSUE_CONTEXT_KEY_INVALID")
        if isinstance(value, str):
            if (
                len(value) > MAX_ISSUE_CONTEXT_STRING_LENGTH
                or "\n" in value
                or "\r" in value
            ):
                raise RuntimeError("MONITOR_ISSUE_CONTEXT_VALUE_INVALID")
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise RuntimeError("MONITOR_ISSUE_CONTEXT_VALUE_INVALID")
        elif value is not None and not isinstance(value, (bool, int)):
            raise RuntimeError("MONITOR_ISSUE_CONTEXT_VALUE_INVALID")
        normalized[key] = value
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(encoded.encode("utf-8")) > MAX_ISSUE_CONTEXT_BYTES:
        raise RuntimeError("MONITOR_ISSUE_CONTEXT_TOO_LARGE")
    return encoded


@dataclass(frozen=True)
class StoredRun:
    run_id: int
    monitor_id: str
    started_at: datetime
    completed_at: datetime | None
    status: RunStatus
    sample_count: int
    error_code: str | None


@dataclass(frozen=True)
class StoredSample:
    sample_id: int
    run_id: int
    monitor_id: str
    series_key: str
    entity_key: str
    observed_at: datetime
    value_text: str
    unit: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class StoredIssue:
    issue_id: int
    run_id: int
    monitor_id: str
    occurred_at: datetime
    scope: str
    reason_code: str
    context: dict[str, str | int | float | bool | None]


@dataclass(frozen=True)
class StoredArtifact:
    artifact_id: int
    run_id: int
    monitor_id: str
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
class StoredProjectionSnapshot:
    monitor_id: str
    snapshot_key: str
    run_id: int
    observed_at: datetime
    cutoff_at: datetime
    payload: dict[str, Any]


@dataclass(frozen=True)
class StoredConfiguration:
    monitor_id: str
    values: dict[str, Any]
    updated_at: datetime


@dataclass(frozen=True)
class StoredControl:
    monitor_id: str
    enabled: bool
    updated_at: datetime


@dataclass(frozen=True)
class StoredForwardEvaluation:
    evaluation_id: int
    source_run_id: int
    resolved_run_id: int | None
    monitor_id: str
    case_key: str
    entity_key: str
    stage: str
    stage_label: str
    direction: str
    signal_observed_at: datetime
    source_cutoff_at: datetime
    horizon_minutes: int
    due_at: datetime
    entry_price_text: str
    benchmark_entry_price_text: str
    source: str
    status: str
    evaluated_at: datetime | None
    outcome_cutoff_at: datetime | None
    exit_price_text: str | None
    benchmark_exit_price_text: str | None
    forward_return_percent: float | None
    benchmark_return_percent: float | None
    relative_return_percent: float | None
    maximum_favorable_excursion_percent: float | None
    maximum_adverse_excursion_percent: float | None
    verdict: str | None
    reason_code: str | None


@dataclass(frozen=True)
class StoredBuybackDocument:
    sha256: str
    monitor_id: str
    source_key: str
    source_label: str
    source_document_id: str
    source_url: str
    published_at: datetime | None
    observed_at: datetime
    media_type: str
    size_bytes: int
    relative_path: str
    quality_state: str
    metadata: dict[str, Any]
    last_referenced_at: datetime


@dataclass(frozen=True)
class StoredBuybackReview:
    review_id: int
    monitor_id: str
    entity_key: str
    base_revision_no: int
    decision: str
    corrected_event_type: str | None
    program_key: str | None
    program_status: str | None
    note: str
    created_at: datetime


@dataclass(frozen=True)
class StoredBuybackEntity:
    revision_id: int
    revision_no: int
    monitor_id: str
    entity_key: str
    entity_type: str
    effective_at: datetime
    observed_at: datetime
    source_key: str
    document_sha256: str | None
    payload_sha256: str
    payload: dict[str, Any]
    review: StoredBuybackReview | None


@dataclass(frozen=True)
class StoredBuybackSourceState:
    monitor_id: str
    source_key: str
    source_label: str
    status: str
    checked_at: datetime
    source_time: datetime | None
    next_due_at: datetime
    record_count: int | None
    detail_code: str | None
    payload: dict[str, Any]
    last_run_id: int


@dataclass(frozen=True)
class StoredMarketEventRevision:
    revision_id: int
    revision_no: int
    monitor_id: str
    event_key: str
    scheduled_at: datetime
    observed_at: datetime
    state: str
    payload_sha256: str
    payload: dict[str, Any]
    source_run_id: int
    created_at: datetime


@dataclass(frozen=True)
class StoredBtcStructureHistory:
    monitor_id: str
    started_at: datetime
    processed_through_at: datetime
    algorithm_version: str
    last_run_id: int


@dataclass(frozen=True)
class StoredBtcStructureEventRevision:
    revision_id: int
    revision_no: int
    monitor_id: str
    event_key: str
    event_at: datetime
    observed_at: datetime
    state: str
    payload_sha256: str
    payload: dict[str, Any]
    source_run_id: int
    created_at: datetime


@dataclass(frozen=True)
class StoredBtcMonthlyResearchHistory:
    monitor_id: str
    started_at: datetime
    processed_through_at: datetime
    algorithm_version: str
    last_run_id: int


@dataclass(frozen=True)
class StoredBtcMonthlyResearchRevision:
    revision_id: int
    revision_no: int
    monitor_id: str
    signal_key: str
    signal_at: datetime
    observed_at: datetime
    state: str
    payload_sha256: str
    payload: dict[str, Any]
    source_run_id: int
    created_at: datetime


class SQLiteMonitorStore:
    """One SQLite database with WAL and short, atomic write transactions."""

    def __init__(
        self,
        path: Path,
        *,
        buyback_retention_days: int = DEFAULT_BUYBACK_RETENTION_DAYS,
        buyback_evidence_max_bytes: int = DEFAULT_BUYBACK_EVIDENCE_MAX_BYTES,
        market_event_retention_days: int = DEFAULT_MARKET_EVENT_RETENTION_DAYS,
        btc_structure_retention_days: int = DEFAULT_BTC_STRUCTURE_RETENTION_DAYS,
        btc_structure_max_events: int = DEFAULT_BTC_STRUCTURE_MAX_EVENTS,
    ) -> None:
        if buyback_retention_days < 1:
            raise ValueError("buyback_retention_days must be positive")
        if buyback_evidence_max_bytes < 1:
            raise ValueError("buyback_evidence_max_bytes must be positive")
        if market_event_retention_days < 1:
            raise ValueError("market_event_retention_days must be positive")
        if btc_structure_retention_days < 1:
            raise ValueError("btc_structure_retention_days must be positive")
        if btc_structure_max_events < 1:
            raise ValueError("btc_structure_max_events must be positive")
        self.path = path.resolve()
        self.buyback_retention_days = buyback_retention_days
        self.buyback_evidence_max_bytes = buyback_evidence_max_bytes
        self.market_event_retention_days = market_event_retention_days
        self.btc_structure_retention_days = btc_structure_retention_days
        self.btc_structure_max_events = btc_structure_max_events
        self.buyback_evidence_root = self.path.parent / "evidence" / "buyback"

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        try:
            yield connection
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            current_version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
            if current_version not in range(SCHEMA_VERSION + 1):
                raise RuntimeError(
                    f"MONITOR_SCHEMA_UNSUPPORTED current={current_version} expected={SCHEMA_VERSION}"
                )
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS monitor_run (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL CHECK (
                        status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED')
                    ),
                    sample_count INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT
                );

                CREATE INDEX IF NOT EXISTS monitor_run_latest_idx
                    ON monitor_run (monitor_id, run_id DESC);

                CREATE TABLE IF NOT EXISTS monitor_sample (
                    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES monitor_run(run_id) ON DELETE CASCADE,
                    monitor_id TEXT NOT NULL,
                    series_key TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    value_text TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS monitor_sample_series_idx
                    ON monitor_sample (monitor_id, series_key, observed_at DESC);
                CREATE INDEX IF NOT EXISTS monitor_sample_run_idx
                    ON monitor_sample (run_id, sample_id);
                CREATE INDEX IF NOT EXISTS monitor_sample_entity_latest_idx
                    ON monitor_sample (monitor_id, entity_key, sample_id DESC);

                CREATE TABLE IF NOT EXISTS monitor_issue (
                    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES monitor_run(run_id) ON DELETE CASCADE,
                    monitor_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    context_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS monitor_issue_latest_idx
                    ON monitor_issue (monitor_id, issue_id DESC);
                CREATE INDEX IF NOT EXISTS monitor_issue_run_idx
                    ON monitor_issue (run_id, issue_id);

                CREATE TABLE IF NOT EXISTS monitor_artifact (
                    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES monitor_run(run_id) ON DELETE CASCADE,
                    monitor_id TEXT NOT NULL,
                    artifact_key TEXT NOT NULL,
                    source TEXT NOT NULL,
                    request_started_at TEXT NOT NULL,
                    response_completed_at TEXT NOT NULL,
                    http_status INTEGER NOT NULL,
                    business_code TEXT,
                    schema_hash TEXT NOT NULL,
                    response_sha256 TEXT NOT NULL,
                    record_count INTEGER,
                    response_body TEXT NOT NULL,
                    UNIQUE (run_id, artifact_key)
                );

                CREATE INDEX IF NOT EXISTS monitor_artifact_latest_idx
                    ON monitor_artifact (
                        monitor_id, artifact_key, response_completed_at DESC
                    );

                CREATE TABLE IF NOT EXISTS monitor_projection_snapshot (
                    monitor_id TEXT NOT NULL,
                    snapshot_key TEXT NOT NULL,
                    run_id INTEGER NOT NULL
                        REFERENCES monitor_run(run_id) ON DELETE CASCADE,
                    observed_at TEXT NOT NULL,
                    cutoff_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (monitor_id, snapshot_key)
                );

                CREATE INDEX IF NOT EXISTS monitor_projection_snapshot_run_idx
                    ON monitor_projection_snapshot (run_id);

                CREATE TABLE IF NOT EXISTS monitor_configuration (
                    monitor_id TEXT PRIMARY KEY,
                    values_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS monitor_control (
                    monitor_id TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS monitor_forward_evaluation (
                    evaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_run_id INTEGER NOT NULL
                        REFERENCES monitor_run(run_id) ON DELETE CASCADE,
                    resolved_run_id INTEGER
                        REFERENCES monitor_run(run_id) ON DELETE SET NULL,
                    monitor_id TEXT NOT NULL,
                    case_key TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    stage_label TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK (direction IN ('UP', 'DOWN')),
                    signal_observed_at TEXT NOT NULL,
                    source_cutoff_at TEXT NOT NULL,
                    horizon_minutes INTEGER NOT NULL CHECK (horizon_minutes > 0),
                    due_at TEXT NOT NULL,
                    entry_price_text TEXT NOT NULL,
                    benchmark_entry_price_text TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (
                        status IN ('PENDING', 'COMPLETE', 'UNAVAILABLE')
                    ),
                    evaluated_at TEXT,
                    outcome_cutoff_at TEXT,
                    exit_price_text TEXT,
                    benchmark_exit_price_text TEXT,
                    forward_return_percent REAL,
                    benchmark_return_percent REAL,
                    relative_return_percent REAL,
                    maximum_favorable_excursion_percent REAL,
                    maximum_adverse_excursion_percent REAL,
                    verdict TEXT CHECK (
                        verdict IS NULL OR verdict IN (
                            'ALIGNED', 'INCONCLUSIVE', 'OPPOSED', 'UNAVAILABLE'
                        )
                    ),
                    reason_code TEXT,
                    UNIQUE (monitor_id, case_key)
                );

                CREATE INDEX IF NOT EXISTS monitor_forward_evaluation_pending_idx
                    ON monitor_forward_evaluation (monitor_id, status, due_at);
                CREATE INDEX IF NOT EXISTS monitor_forward_evaluation_recent_idx
                    ON monitor_forward_evaluation (
                        monitor_id, source_cutoff_at DESC, evaluation_id DESC
                    );
                CREATE INDEX IF NOT EXISTS monitor_forward_evaluation_entity_idx
                    ON monitor_forward_evaluation (
                        monitor_id, entity_key, evaluation_id DESC
                    );

                CREATE TABLE IF NOT EXISTS buyback_document (
                    sha256 TEXT PRIMARY KEY CHECK (length(sha256) = 64),
                    monitor_id TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    source_label TEXT NOT NULL,
                    source_document_id TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    published_at TEXT,
                    observed_at TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL CHECK (size_bytes > 0),
                    relative_path TEXT NOT NULL UNIQUE,
                    quality_state TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    last_referenced_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS buyback_document_source_idx
                    ON buyback_document (
                        monitor_id, source_key, source_document_id, observed_at DESC
                    );
                CREATE INDEX IF NOT EXISTS buyback_document_reference_idx
                    ON buyback_document (last_referenced_at);

                CREATE TABLE IF NOT EXISTS buyback_entity_revision (
                    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    entity_type TEXT NOT NULL CHECK (
                        entity_type IN ('DISCLOSURE_CANDIDATE', 'HKEX_EXECUTION')
                    ),
                    revision_no INTEGER NOT NULL CHECK (revision_no > 0),
                    effective_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    document_sha256 TEXT REFERENCES buyback_document(sha256)
                        ON DELETE RESTRICT,
                    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                    payload_json TEXT NOT NULL,
                    source_run_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (monitor_id, entity_key, revision_no),
                    UNIQUE (monitor_id, entity_key, payload_sha256)
                );

                CREATE INDEX IF NOT EXISTS buyback_entity_latest_idx
                    ON buyback_entity_revision (
                        monitor_id, entity_key, revision_no DESC
                    );
                CREATE INDEX IF NOT EXISTS buyback_entity_effective_idx
                    ON buyback_entity_revision (
                        monitor_id, effective_at DESC, revision_id DESC
                    );

                CREATE TABLE IF NOT EXISTS buyback_review (
                    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    base_revision_no INTEGER NOT NULL CHECK (base_revision_no > 0),
                    decision TEXT NOT NULL CHECK (
                        decision IN (
                            'CONFIRMED_EVENT', 'REJECTED_EVENT', 'NEEDS_FOLLOW_UP'
                        )
                    ),
                    corrected_event_type TEXT,
                    program_key TEXT,
                    program_status TEXT CHECK (
                        program_status IS NULL OR program_status IN (
                            'PROPOSED', 'APPROVED', 'ACTIVE', 'COMPLETED',
                            'TERMINATED', 'UNKNOWN'
                        )
                    ),
                    note TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS buyback_review_latest_idx
                    ON buyback_review (monitor_id, entity_key, review_id DESC);

                CREATE TABLE IF NOT EXISTS buyback_source_state (
                    monitor_id TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    source_label TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('SUCCESS', 'EMPTY', 'PARTIAL', 'STALE', 'ERROR')
                    ),
                    checked_at TEXT NOT NULL,
                    source_time TEXT,
                    next_due_at TEXT NOT NULL,
                    record_count INTEGER CHECK (
                        record_count IS NULL OR record_count >= 0
                    ),
                    detail_code TEXT,
                    payload_json TEXT NOT NULL,
                    last_run_id INTEGER NOT NULL,
                    PRIMARY KEY (monitor_id, source_key)
                );

                CREATE INDEX IF NOT EXISTS buyback_source_due_idx
                    ON buyback_source_state (monitor_id, next_due_at);

                CREATE TABLE IF NOT EXISTS market_event_history_state (
                    monitor_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS market_event_revision (
                    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    revision_no INTEGER NOT NULL CHECK (revision_no > 0),
                    scheduled_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN (
                            'SCHEDULED', 'AWAITING_OFFICIAL', 'RELEASED',
                            'OCCURRED'
                        )
                    ),
                    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                    payload_json TEXT NOT NULL,
                    source_run_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (monitor_id, event_key, revision_no),
                    UNIQUE (monitor_id, event_key, payload_sha256)
                );

                CREATE INDEX IF NOT EXISTS market_event_revision_latest_idx
                    ON market_event_revision (
                        monitor_id, event_key, revision_no DESC
                    );
                CREATE INDEX IF NOT EXISTS market_event_revision_schedule_idx
                    ON market_event_revision (
                        monitor_id, scheduled_at DESC, revision_id DESC
                    );

                CREATE TABLE IF NOT EXISTS btc_structure_history_state (
                    monitor_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    processed_through_at TEXT NOT NULL,
                    algorithm_version TEXT NOT NULL,
                    last_run_id INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS btc_structure_event_revision (
                    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    revision_no INTEGER NOT NULL CHECK (revision_no > 0),
                    event_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN ('PENDING', 'REACTION', 'BREAK', 'UNRESOLVED')
                    ),
                    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                    payload_json TEXT NOT NULL,
                    source_run_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (monitor_id, event_key, revision_no),
                    UNIQUE (monitor_id, event_key, payload_sha256)
                );

                CREATE INDEX IF NOT EXISTS btc_structure_event_latest_idx
                    ON btc_structure_event_revision (
                        monitor_id, event_key, revision_no DESC
                    );
                CREATE INDEX IF NOT EXISTS btc_structure_event_time_idx
                    ON btc_structure_event_revision (
                        monitor_id, event_at DESC, revision_id DESC
                    );
                CREATE INDEX IF NOT EXISTS btc_structure_event_state_idx
                    ON btc_structure_event_revision (
                        monitor_id, state, event_at DESC
                    );

                CREATE TABLE IF NOT EXISTS btc_monthly_research_history_state (
                    monitor_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    processed_through_at TEXT NOT NULL,
                    algorithm_version TEXT NOT NULL,
                    last_run_id INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS btc_monthly_research_revision (
                    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id TEXT NOT NULL,
                    signal_key TEXT NOT NULL,
                    revision_no INTEGER NOT NULL CHECK (revision_no > 0),
                    signal_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN ('SIGNAL_FROZEN', 'EXECUTION_CAPTURED')
                    ),
                    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                    payload_json TEXT NOT NULL,
                    source_run_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (monitor_id, signal_key, revision_no),
                    UNIQUE (monitor_id, signal_key, payload_sha256)
                );

                CREATE INDEX IF NOT EXISTS btc_monthly_research_latest_idx
                    ON btc_monthly_research_revision (
                        monitor_id, signal_key, revision_no DESC
                    );
                CREATE INDEX IF NOT EXISTS btc_monthly_research_time_idx
                    ON btc_monthly_research_revision (
                        monitor_id, signal_at DESC, revision_id DESC
                    );
                """
            )
            issue_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(monitor_issue)")
            }
            if "context_json" not in issue_columns:
                connection.execute(
                    "ALTER TABLE monitor_issue "
                    "ADD COLUMN context_json TEXT NOT NULL DEFAULT '{}'"
                )
            interrupted = connection.execute(
                "SELECT run_id, monitor_id FROM monitor_run WHERE status = 'RUNNING'"
            ).fetchall()
            interrupted_at = iso_utc(utc_now())
            for row in interrupted:
                connection.execute(
                    """
                    INSERT INTO monitor_issue (
                        run_id, monitor_id, occurred_at, scope, reason_code
                    ) VALUES (?, ?, ?, 'monitor', 'PROCESS_INTERRUPTED')
                    """,
                    (int(row["run_id"]), str(row["monitor_id"]), interrupted_at),
                )
            connection.execute(
                """
                UPDATE monitor_run
                SET status = 'FAILED', completed_at = ?, error_code = 'PROCESS_INTERRUPTED'
                WHERE status = 'RUNNING'
                """,
                (interrupted_at,),
            )
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def ensure_control(
        self,
        monitor_id: str,
        *,
        default_enabled: bool,
        updated_at: datetime | None = None,
    ) -> StoredControl:
        updated = updated_at or utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO monitor_control (monitor_id, enabled, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(monitor_id) DO NOTHING
                """,
                (monitor_id, int(default_enabled), iso_utc(updated)),
            )
            row = connection.execute(
                """
                SELECT monitor_id, enabled, updated_at
                FROM monitor_control
                WHERE monitor_id = ?
                """,
                (monitor_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError("MONITOR_CONTROL_MISSING")
        return self._control_from_row(row)

    def set_enabled(
        self,
        monitor_id: str,
        enabled: bool,
        *,
        updated_at: datetime | None = None,
    ) -> StoredControl:
        updated = updated_at or utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE monitor_control
                SET enabled = ?, updated_at = ?
                WHERE monitor_id = ?
                """,
                (int(enabled), iso_utc(updated), monitor_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("MONITOR_CONTROL_MISSING")
        return StoredControl(monitor_id, enabled, updated)

    def load_control(self, monitor_id: str) -> StoredControl | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT monitor_id, enabled, updated_at
                FROM monitor_control
                WHERE monitor_id = ?
                """,
                (monitor_id,),
            ).fetchone()
        return self._control_from_row(row) if row is not None else None

    def is_enabled(self, monitor_id: str) -> bool:
        control = self.load_control(monitor_id)
        return control.enabled if control is not None else False

    def save_configuration(
        self,
        monitor_id: str,
        values: dict[str, Any],
        *,
        updated_at: datetime | None = None,
    ) -> StoredConfiguration:
        updated = updated_at or utc_now()
        payload = json.dumps(
            values,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO monitor_configuration (monitor_id, values_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(monitor_id) DO UPDATE SET
                    values_json = excluded.values_json,
                    updated_at = excluded.updated_at
                """,
                (monitor_id, payload, iso_utc(updated)),
            )
        return StoredConfiguration(monitor_id, dict(values), updated)

    def load_configuration(self, monitor_id: str) -> StoredConfiguration | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT monitor_id, values_json, updated_at
                FROM monitor_configuration
                WHERE monitor_id = ?
                """,
                (monitor_id,),
            ).fetchone()
        if row is None:
            return None
        values = json.loads(str(row["values_json"]))
        if not isinstance(values, dict):
            raise RuntimeError("MONITOR_CONFIGURATION_PAYLOAD_INVALID")
        return StoredConfiguration(
            monitor_id=str(row["monitor_id"]),
            values=values,
            updated_at=parse_utc(str(row["updated_at"])),
        )

    def _prepare_buyback_documents(
        self,
        monitor_id: str,
        documents: tuple[BuybackEvidenceDocument, ...],
    ) -> dict[str, tuple[BuybackEvidenceDocument, str]]:
        """Write immutable bytes before the DB transaction.

        A later database failure can leave an unreferenced content-addressed file.
        It remains inside the evidence quota and can be inspected deliberately; a
        committed database row can never point at a partially written file because
        the final rename is atomic.
        """

        prepared: dict[str, tuple[BuybackEvidenceDocument, str]] = {}
        for document in documents:
            if not document.body:
                raise RuntimeError("BUYBACK_DOCUMENT_EMPTY")
            if len(document.body) > MAX_BUYBACK_DOCUMENT_BYTES:
                raise RuntimeError("BUYBACK_DOCUMENT_TOO_LARGE")
            suffix = document.file_suffix.casefold()
            if suffix not in BUYBACK_DOCUMENT_SUFFIXES:
                raise RuntimeError("BUYBACK_DOCUMENT_SUFFIX_UNSUPPORTED")
            if not document.source_url.startswith("https://"):
                raise RuntimeError("BUYBACK_DOCUMENT_URL_INVALID")
            if not document.source_key or not document.source_document_id:
                raise RuntimeError("BUYBACK_DOCUMENT_IDENTITY_INVALID")
            iso_utc(document.observed_at)
            if document.published_at is not None:
                iso_utc(document.published_at)
            digest = hashlib.sha256(document.body).hexdigest()
            existing = prepared.get(digest)
            if existing is not None:
                if existing[0].body != document.body:
                    raise RuntimeError("BUYBACK_DOCUMENT_HASH_COLLISION")
                continue
            relative = Path("evidence") / "buyback" / digest[:2] / f"{digest}{suffix}"
            prepared[digest] = (document, relative.as_posix())

        if not prepared:
            return prepared

        current_bytes = self._buyback_evidence_bytes_on_disk()
        incoming_bytes = sum(
            len(document.body)
            for document, relative_path in prepared.values()
            if not (self.path.parent / Path(relative_path)).exists()
        )
        if current_bytes + incoming_bytes > self.buyback_evidence_max_bytes:
            raise RuntimeError("BUYBACK_EVIDENCE_QUOTA_EXCEEDED")

        root = self.buyback_evidence_root.resolve()
        for digest, (document, relative_path) in prepared.items():
            target = (self.path.parent / Path(relative_path)).resolve()
            try:
                target.relative_to(root)
            except ValueError:
                raise RuntimeError("BUYBACK_EVIDENCE_PATH_INVALID") from None
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                if not target.is_file():
                    raise RuntimeError("BUYBACK_EVIDENCE_TARGET_INVALID")
                if (
                    target.stat().st_size != len(document.body)
                    or hashlib.sha256(target.read_bytes()).hexdigest() != digest
                ):
                    raise RuntimeError("BUYBACK_EVIDENCE_CONTENT_MISMATCH")
                continue
            temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
            try:
                with temporary.open("xb") as stream:
                    stream.write(document.body)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                if temporary.exists():
                    temporary.unlink()
        return prepared

    def _buyback_evidence_bytes_on_disk(self) -> int:
        """Count owned evidence without following links or reparse points."""

        root = self.buyback_evidence_root
        if not root.exists():
            return 0
        total = 0
        pending = [root]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.is_symlink():
                        raise RuntimeError("BUYBACK_EVIDENCE_LINK_UNSUPPORTED")
                    stat_result = entry.stat(follow_symlinks=False)
                    if getattr(stat_result, "st_file_attributes", 0) & 0x400:
                        raise RuntimeError("BUYBACK_EVIDENCE_LINK_UNSUPPORTED")
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        total += stat_result.st_size
                    else:
                        raise RuntimeError("BUYBACK_EVIDENCE_ENTRY_INVALID")
        return total

    def storage_metrics(self) -> dict[str, int]:
        """Return path-free, bounded-size diagnostics for periodic local logs."""

        def file_size(path: Path) -> int:
            try:
                stat_result = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                return 0
            if not path.is_file():
                raise RuntimeError("MONITOR_STORAGE_ENTRY_INVALID")
            return int(stat_result.st_size)

        return {
            "database_bytes": file_size(self.path),
            "wal_bytes": file_size(Path(f"{self.path}-wal")),
            "shared_memory_bytes": file_size(Path(f"{self.path}-shm")),
            "buyback_evidence_bytes": self._buyback_evidence_bytes_on_disk(),
        }

    def start_run(self, monitor_id: str, *, started_at: datetime | None = None) -> int:
        started = iso_utc(started_at or utc_now())
        with self._connect() as connection:
            interrupted = connection.execute(
                """
                SELECT run_id
                FROM monitor_run
                WHERE monitor_id = ? AND status = 'RUNNING'
                """,
                (monitor_id,),
            ).fetchall()
            for row in interrupted:
                connection.execute(
                    """
                    INSERT INTO monitor_issue (
                        run_id, monitor_id, occurred_at, scope, reason_code
                    ) VALUES (?, ?, ?, 'monitor', 'WORKER_PREVIOUS_RUN_INTERRUPTED')
                    """,
                    (int(row["run_id"]), monitor_id, started),
                )
            connection.execute(
                """
                UPDATE monitor_run
                SET completed_at = ?, status = 'FAILED',
                    error_code = 'WORKER_PREVIOUS_RUN_INTERRUPTED'
                WHERE monitor_id = ? AND status = 'RUNNING'
                """,
                (started, monitor_id),
            )
            cursor = connection.execute(
                """
                INSERT INTO monitor_run (monitor_id, started_at, status)
                VALUES (?, ?, 'RUNNING')
                """,
                (monitor_id, started),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("MONITOR_RUN_ID_MISSING")
            return int(cursor.lastrowid)

    def finish_run(
        self,
        run_id: int,
        monitor_id: str,
        batch: CollectionBatch,
        *,
        completed_at: datetime | None = None,
    ) -> RunStatus:
        completed = completed_at or utc_now()
        completed_text = iso_utc(completed)
        prepared_documents = self._prepare_buyback_documents(
            monitor_id,
            batch.buyback_documents,
        )
        has_current_result = bool(
            batch.samples
            or batch.projection_snapshots
            or batch.buyback_source_observations
            or batch.market_event_revisions
            or batch.btc_structure_history is not None
            or batch.btc_structure_event_revisions
            or batch.btc_monthly_research_history is not None
            or batch.btc_monthly_research_revisions
        )
        status: RunStatus = (
            "FAILED"
            if batch.issues and not has_current_result
            else "PARTIAL"
            if batch.issues
            else "SUCCESS"
        )
        with self._connect() as connection:
            current = connection.execute(
                "SELECT monitor_id, status FROM monitor_run WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if current is None or str(current["monitor_id"]) != monitor_id:
                raise RuntimeError("MONITOR_RUN_NOT_FOUND")
            if current["status"] != "RUNNING":
                raise RuntimeError("MONITOR_RUN_ALREADY_FINISHED")
            for digest, (document, relative_path) in prepared_documents.items():
                connection.execute(
                    """
                    INSERT INTO buyback_document (
                        sha256, monitor_id, source_key, source_label,
                        source_document_id, source_url, published_at,
                        observed_at, media_type, size_bytes, relative_path,
                        quality_state, metadata_json, last_referenced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sha256) DO UPDATE SET
                        last_referenced_at = excluded.last_referenced_at
                    """,
                    (
                        digest,
                        monitor_id,
                        document.source_key,
                        document.source_label,
                        document.source_document_id,
                        document.source_url,
                        (
                            iso_utc(document.published_at)
                            if document.published_at is not None
                            else None
                        ),
                        iso_utc(document.observed_at),
                        document.media_type,
                        len(document.body),
                        relative_path,
                        document.quality_state,
                        json.dumps(
                            document.metadata,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        completed_text,
                    ),
                )
            for artifact in batch.artifacts:
                connection.execute(
                    """
                    INSERT INTO monitor_artifact (
                        run_id, monitor_id, artifact_key, source,
                        request_started_at, response_completed_at,
                        http_status, business_code,
                        schema_hash, response_sha256, record_count, response_body
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        monitor_id,
                        artifact.artifact_key,
                        artifact.source,
                        iso_utc(artifact.request_started_at),
                        iso_utc(artifact.response_completed_at),
                        artifact.http_status,
                        artifact.business_code,
                        artifact.schema_hash,
                        artifact.response_sha256,
                        artifact.record_count,
                        artifact.response_body,
                    ),
                )
            for sample in batch.samples:
                connection.execute(
                    """
                    INSERT INTO monitor_sample (
                        run_id, monitor_id, series_key, entity_key, observed_at,
                        value_text, unit, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        monitor_id,
                        sample.series_key,
                        sample.entity_key,
                        iso_utc(sample.observed_at),
                        sample.value_text,
                        sample.unit,
                        json.dumps(
                            sample.payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
            for snapshot in batch.projection_snapshots:
                self._upsert_projection_snapshot(
                    connection,
                    run_id=run_id,
                    monitor_id=monitor_id,
                    snapshot=snapshot,
                )
            for observation in batch.buyback_source_observations:
                if (
                    observation.record_count is not None
                    and observation.record_count < 0
                ):
                    raise RuntimeError("BUYBACK_SOURCE_RECORD_COUNT_INVALID")
                connection.execute(
                    """
                    INSERT INTO buyback_source_state (
                        monitor_id, source_key, source_label, status,
                        checked_at, source_time, next_due_at, record_count,
                        detail_code, payload_json, last_run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(monitor_id, source_key) DO UPDATE SET
                        source_label = excluded.source_label,
                        status = excluded.status,
                        checked_at = excluded.checked_at,
                        source_time = excluded.source_time,
                        next_due_at = excluded.next_due_at,
                        record_count = excluded.record_count,
                        detail_code = excluded.detail_code,
                        payload_json = excluded.payload_json,
                        last_run_id = excluded.last_run_id
                    """,
                    (
                        monitor_id,
                        observation.source_key,
                        observation.source_label,
                        observation.status,
                        iso_utc(observation.checked_at),
                        (
                            iso_utc(observation.source_time)
                            if observation.source_time is not None
                            else None
                        ),
                        iso_utc(observation.next_due_at),
                        observation.record_count,
                        observation.detail_code,
                        json.dumps(
                            observation.payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        run_id,
                    ),
                )
            for revision in batch.buyback_revisions:
                if not revision.entity_key or len(revision.entity_key) > 256:
                    raise RuntimeError("BUYBACK_ENTITY_KEY_INVALID")
                if revision.document_sha256 is not None and (
                    len(revision.document_sha256) != 64
                    or any(
                        value not in "0123456789abcdef"
                        for value in revision.document_sha256
                    )
                ):
                    raise RuntimeError("BUYBACK_DOCUMENT_HASH_INVALID")
                payload_json = json.dumps(
                    revision.payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                payload_sha256 = hashlib.sha256(
                    payload_json.encode("utf-8")
                ).hexdigest()
                duplicate = connection.execute(
                    """
                    SELECT 1
                    FROM buyback_entity_revision
                    WHERE monitor_id = ? AND entity_key = ? AND payload_sha256 = ?
                    """,
                    (monitor_id, revision.entity_key, payload_sha256),
                ).fetchone()
                if duplicate is not None:
                    continue
                latest_revision = connection.execute(
                    """
                    SELECT COALESCE(MAX(revision_no), 0)
                    FROM buyback_entity_revision
                    WHERE monitor_id = ? AND entity_key = ?
                    """,
                    (monitor_id, revision.entity_key),
                ).fetchone()
                revision_no = int(latest_revision[0]) + 1
                connection.execute(
                    """
                    INSERT INTO buyback_entity_revision (
                        monitor_id, entity_key, entity_type, revision_no,
                        effective_at, observed_at, source_key,
                        document_sha256, payload_sha256, payload_json,
                        source_run_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        monitor_id,
                        revision.entity_key,
                        revision.entity_type,
                        revision_no,
                        iso_utc(revision.effective_at),
                        iso_utc(revision.observed_at),
                        revision.source_key,
                        revision.document_sha256,
                        payload_sha256,
                        payload_json,
                        run_id,
                        completed_text,
                    ),
                )
                if revision.document_sha256 is not None:
                    connection.execute(
                        """
                        UPDATE buyback_document
                        SET last_referenced_at = ?
                        WHERE sha256 = ?
                        """,
                        (completed_text, revision.document_sha256),
                    )
            if batch.market_event_revisions:
                history_started = min(
                    revision.observed_at for revision in batch.market_event_revisions
                )
                connection.execute(
                    """
                    INSERT INTO market_event_history_state (monitor_id, started_at)
                    VALUES (?, ?)
                    ON CONFLICT(monitor_id) DO NOTHING
                    """,
                    (monitor_id, iso_utc(history_started)),
                )
                state_row = connection.execute(
                    """
                    SELECT started_at
                    FROM market_event_history_state
                    WHERE monitor_id = ?
                    """,
                    (monitor_id,),
                ).fetchone()
                if state_row is None:
                    raise RuntimeError("MARKET_EVENT_HISTORY_STATE_MISSING")
                history_started_at = parse_utc(str(state_row["started_at"]))
                for revision in batch.market_event_revisions:
                    if not revision.event_key or len(revision.event_key) > 256:
                        raise RuntimeError("MARKET_EVENT_KEY_INVALID")
                    if revision.state not in {
                        "SCHEDULED",
                        "AWAITING_OFFICIAL",
                        "RELEASED",
                        "OCCURRED",
                    }:
                        raise RuntimeError("MARKET_EVENT_STATE_INVALID")
                    iso_utc(revision.scheduled_at)
                    iso_utc(revision.observed_at)
                    existing = connection.execute(
                        """
                        SELECT 1
                        FROM market_event_revision
                        WHERE monitor_id = ? AND event_key = ?
                        LIMIT 1
                        """,
                        (monitor_id, revision.event_key),
                    ).fetchone()
                    if existing is None and revision.scheduled_at < history_started_at:
                        continue
                    payload_json = json.dumps(
                        revision.payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    payload_sha256 = hashlib.sha256(
                        payload_json.encode("utf-8")
                    ).hexdigest()
                    duplicate = connection.execute(
                        """
                        SELECT 1
                        FROM market_event_revision
                        WHERE monitor_id = ? AND event_key = ?
                          AND payload_sha256 = ?
                        """,
                        (monitor_id, revision.event_key, payload_sha256),
                    ).fetchone()
                    if duplicate is not None:
                        continue
                    latest_revision = connection.execute(
                        """
                        SELECT COALESCE(MAX(revision_no), 0)
                        FROM market_event_revision
                        WHERE monitor_id = ? AND event_key = ?
                        """,
                        (monitor_id, revision.event_key),
                    ).fetchone()
                    revision_no = int(latest_revision[0]) + 1
                    connection.execute(
                        """
                        INSERT INTO market_event_revision (
                            monitor_id, event_key, revision_no, scheduled_at,
                            observed_at, state, payload_sha256, payload_json,
                            source_run_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            monitor_id,
                            revision.event_key,
                            revision_no,
                            iso_utc(revision.scheduled_at),
                            iso_utc(revision.observed_at),
                            revision.state,
                            payload_sha256,
                            payload_json,
                            run_id,
                            completed_text,
                        ),
                    )
            if batch.btc_structure_history is not None:
                history = batch.btc_structure_history
                if (
                    not history.algorithm_version
                    or len(history.algorithm_version) > 128
                ):
                    raise RuntimeError("BTC_STRUCTURE_HISTORY_INVALID")
                connection.execute(
                    """
                    INSERT INTO btc_structure_history_state (
                        monitor_id, started_at, processed_through_at,
                        algorithm_version, last_run_id
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(monitor_id) DO NOTHING
                    """,
                    (
                        monitor_id,
                        iso_utc(history.started_at),
                        iso_utc(history.processed_through_at),
                        history.algorithm_version,
                        run_id,
                    ),
                )
                stored_history = connection.execute(
                    """
                    SELECT started_at, processed_through_at, algorithm_version
                    FROM btc_structure_history_state
                    WHERE monitor_id = ?
                    """,
                    (monitor_id,),
                ).fetchone()
                if stored_history is None:
                    raise RuntimeError("BTC_STRUCTURE_HISTORY_MISSING")
                if (
                    str(stored_history["algorithm_version"])
                    != history.algorithm_version
                ):
                    if history.started_at < parse_utc(
                        str(stored_history["processed_through_at"])
                    ):
                        raise RuntimeError("BTC_STRUCTURE_VERSION_START_INVALID")
                    connection.execute(
                        """
                        UPDATE btc_structure_history_state
                        SET started_at = ?, processed_through_at = ?,
                            algorithm_version = ?, last_run_id = ?
                        WHERE monitor_id = ?
                        """,
                        (
                            iso_utc(history.started_at),
                            iso_utc(history.processed_through_at),
                            history.algorithm_version,
                            run_id,
                            monitor_id,
                        ),
                    )
                    stored_history = connection.execute(
                        """
                        SELECT started_at, processed_through_at, algorithm_version
                        FROM btc_structure_history_state
                        WHERE monitor_id = ?
                        """,
                        (monitor_id,),
                    ).fetchone()
                    if stored_history is None:
                        raise RuntimeError("BTC_STRUCTURE_HISTORY_MISSING")
                stored_processed = parse_utc(
                    str(stored_history["processed_through_at"])
                )
                if history.processed_through_at > stored_processed:
                    connection.execute(
                        """
                        UPDATE btc_structure_history_state
                        SET processed_through_at = ?, last_run_id = ?
                        WHERE monitor_id = ?
                        """,
                        (
                            iso_utc(history.processed_through_at),
                            run_id,
                            monitor_id,
                        ),
                    )
            if batch.btc_structure_event_revisions:
                history_row = connection.execute(
                    """
                    SELECT started_at, algorithm_version
                    FROM btc_structure_history_state
                    WHERE monitor_id = ?
                    """,
                    (monitor_id,),
                ).fetchone()
                if history_row is None:
                    raise RuntimeError("BTC_STRUCTURE_HISTORY_MISSING")
                history_started_at = parse_utc(str(history_row["started_at"]))
                history_algorithm_version = str(history_row["algorithm_version"])
                for revision in batch.btc_structure_event_revisions:
                    self._insert_btc_structure_revision(
                        connection,
                        run_id=run_id,
                        monitor_id=monitor_id,
                        history_started_at=history_started_at,
                        history_algorithm_version=history_algorithm_version,
                        revision=revision,
                        created_at=completed_text,
                    )
            if batch.btc_monthly_research_history is not None:
                history = batch.btc_monthly_research_history
                if (
                    not history.algorithm_version
                    or len(history.algorithm_version) > 128
                ):
                    raise RuntimeError("BTC_MONTHLY_HISTORY_INVALID")
                connection.execute(
                    """
                    INSERT INTO btc_monthly_research_history_state (
                        monitor_id, started_at, processed_through_at,
                        algorithm_version, last_run_id
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(monitor_id) DO NOTHING
                    """,
                    (
                        monitor_id,
                        iso_utc(history.started_at),
                        iso_utc(history.processed_through_at),
                        history.algorithm_version,
                        run_id,
                    ),
                )
                stored_monthly_history = connection.execute(
                    """
                    SELECT started_at, processed_through_at, algorithm_version
                    FROM btc_monthly_research_history_state
                    WHERE monitor_id = ?
                    """,
                    (monitor_id,),
                ).fetchone()
                if stored_monthly_history is None:
                    raise RuntimeError("BTC_MONTHLY_HISTORY_MISSING")
                if (
                    str(stored_monthly_history["algorithm_version"])
                    != history.algorithm_version
                ):
                    if history.started_at < parse_utc(
                        str(stored_monthly_history["processed_through_at"])
                    ):
                        raise RuntimeError("BTC_MONTHLY_VERSION_START_INVALID")
                    connection.execute(
                        """
                        UPDATE btc_monthly_research_history_state
                        SET started_at = ?, processed_through_at = ?,
                            algorithm_version = ?, last_run_id = ?
                        WHERE monitor_id = ?
                        """,
                        (
                            iso_utc(history.started_at),
                            iso_utc(history.processed_through_at),
                            history.algorithm_version,
                            run_id,
                            monitor_id,
                        ),
                    )
                    stored_monthly_history = connection.execute(
                        """
                        SELECT started_at, processed_through_at, algorithm_version
                        FROM btc_monthly_research_history_state
                        WHERE monitor_id = ?
                        """,
                        (monitor_id,),
                    ).fetchone()
                    if stored_monthly_history is None:
                        raise RuntimeError("BTC_MONTHLY_HISTORY_MISSING")
                stored_monthly_processed = parse_utc(
                    str(stored_monthly_history["processed_through_at"])
                )
                if history.processed_through_at > stored_monthly_processed:
                    connection.execute(
                        """
                        UPDATE btc_monthly_research_history_state
                        SET processed_through_at = ?, last_run_id = ?
                        WHERE monitor_id = ?
                        """,
                        (
                            iso_utc(history.processed_through_at),
                            run_id,
                            monitor_id,
                        ),
                    )
            if batch.btc_monthly_research_revisions:
                monthly_history_row = connection.execute(
                    """
                    SELECT started_at, algorithm_version
                    FROM btc_monthly_research_history_state
                    WHERE monitor_id = ?
                    """,
                    (monitor_id,),
                ).fetchone()
                if monthly_history_row is None:
                    raise RuntimeError("BTC_MONTHLY_HISTORY_MISSING")
                monthly_started_at = parse_utc(str(monthly_history_row["started_at"]))
                monthly_algorithm_version = str(
                    monthly_history_row["algorithm_version"]
                )
                for revision in batch.btc_monthly_research_revisions:
                    self._insert_btc_monthly_research_revision(
                        connection,
                        run_id=run_id,
                        monitor_id=monitor_id,
                        history_started_at=monthly_started_at,
                        history_algorithm_version=monthly_algorithm_version,
                        revision=revision,
                        created_at=completed_text,
                    )
            for case in batch.evaluation_cases:
                connection.execute(
                    """
                    INSERT INTO monitor_forward_evaluation (
                        source_run_id, monitor_id, case_key, entity_key,
                        stage, stage_label, direction,
                        signal_observed_at, source_cutoff_at,
                        horizon_minutes, due_at,
                        entry_price_text, benchmark_entry_price_text, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(monitor_id, case_key) DO NOTHING
                    """,
                    (
                        run_id,
                        monitor_id,
                        case.case_key,
                        case.entity_key,
                        case.stage,
                        case.stage_label,
                        case.direction,
                        iso_utc(case.signal_observed_at),
                        iso_utc(case.source_cutoff_at),
                        case.horizon_minutes,
                        iso_utc(case.due_at),
                        case.entry_price_text,
                        case.benchmark_entry_price_text,
                        case.source,
                    ),
                )
            for result in batch.evaluation_results:
                cursor = connection.execute(
                    """
                    UPDATE monitor_forward_evaluation
                    SET resolved_run_id = ?, status = ?, evaluated_at = ?,
                        outcome_cutoff_at = ?, exit_price_text = ?,
                        benchmark_exit_price_text = ?,
                        forward_return_percent = ?, benchmark_return_percent = ?,
                        relative_return_percent = ?,
                        maximum_favorable_excursion_percent = ?,
                        maximum_adverse_excursion_percent = ?,
                        verdict = ?, reason_code = ?
                    WHERE monitor_id = ? AND case_key = ? AND status = 'PENDING'
                    """,
                    (
                        run_id,
                        result.status,
                        iso_utc(result.evaluated_at),
                        (
                            iso_utc(result.outcome_cutoff_at)
                            if result.outcome_cutoff_at is not None
                            else None
                        ),
                        result.exit_price_text,
                        result.benchmark_exit_price_text,
                        result.forward_return_percent,
                        result.benchmark_return_percent,
                        result.relative_return_percent,
                        result.maximum_favorable_excursion_percent,
                        result.maximum_adverse_excursion_percent,
                        result.verdict,
                        result.reason_code,
                        monitor_id,
                        result.case_key,
                    ),
                )
                if cursor.rowcount == 0:
                    existing = connection.execute(
                        """
                        SELECT status
                        FROM monitor_forward_evaluation
                        WHERE monitor_id = ? AND case_key = ?
                        """,
                        (monitor_id, result.case_key),
                    ).fetchone()
                    if existing is None:
                        raise RuntimeError("MONITOR_EVALUATION_CASE_NOT_FOUND")
            for issue in batch.issues:
                connection.execute(
                    """
                    INSERT INTO monitor_issue (
                        run_id, monitor_id, occurred_at, scope, reason_code,
                        context_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        monitor_id,
                        completed_text,
                        issue.scope,
                        issue.reason_code,
                        _issue_context_json(issue.context),
                    ),
                )
            error_code = batch.issues[0].reason_code if batch.issues else None
            connection.execute(
                """
                UPDATE monitor_run
                SET completed_at = ?, status = ?, sample_count = ?, error_code = ?
                WHERE run_id = ?
                """,
                (completed_text, status, len(batch.samples), error_code, run_id),
            )
        return status

    @staticmethod
    def _upsert_projection_snapshot(
        connection: sqlite3.Connection,
        *,
        run_id: int,
        monitor_id: str,
        snapshot: ProjectionSnapshot,
    ) -> None:
        if (
            not snapshot.snapshot_key
            or len(snapshot.snapshot_key) > 96
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789-_"
                for character in snapshot.snapshot_key
            )
        ):
            raise RuntimeError("MONITOR_PROJECTION_SNAPSHOT_KEY_INVALID")
        observed_at = iso_utc(snapshot.observed_at)
        cutoff_at = iso_utc(snapshot.cutoff_at)
        if snapshot.cutoff_at > snapshot.observed_at + timedelta(minutes=2):
            raise RuntimeError("MONITOR_PROJECTION_SNAPSHOT_CUTOFF_INVALID")
        payload_json = json.dumps(
            snapshot.payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if len(payload_json.encode("utf-8")) > MAX_PROJECTION_SNAPSHOT_BYTES:
            raise RuntimeError("MONITOR_PROJECTION_SNAPSHOT_TOO_LARGE")
        connection.execute(
            """
            INSERT INTO monitor_projection_snapshot (
                monitor_id, snapshot_key, run_id,
                observed_at, cutoff_at, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(monitor_id, snapshot_key) DO UPDATE SET
                run_id = excluded.run_id,
                observed_at = excluded.observed_at,
                cutoff_at = excluded.cutoff_at,
                payload_json = excluded.payload_json
            """,
            (
                monitor_id,
                snapshot.snapshot_key,
                run_id,
                observed_at,
                cutoff_at,
                payload_json,
            ),
        )

    @staticmethod
    def _insert_btc_structure_revision(
        connection: sqlite3.Connection,
        *,
        run_id: int,
        monitor_id: str,
        history_started_at: datetime,
        history_algorithm_version: str,
        revision: BtcStructureEventRevision,
        created_at: str,
    ) -> None:
        if not revision.event_key or len(revision.event_key) > 256:
            raise RuntimeError("BTC_STRUCTURE_EVENT_KEY_INVALID")
        if revision.event_at < history_started_at:
            raise RuntimeError("BTC_STRUCTURE_EVENT_PREDATES_LEDGER")
        if revision.observed_at < revision.event_at:
            raise RuntimeError("BTC_STRUCTURE_EVENT_OBSERVED_BEFORE_EVENT")
        payload_version = revision.payload.get("algorithm_version")
        if payload_version is None or str(payload_version) == "":
            raise RuntimeError("BTC_STRUCTURE_EVENT_VERSION_MISSING")
        if str(payload_version) != history_algorithm_version:
            raise RuntimeError("BTC_STRUCTURE_EVENT_VERSION_CHANGED")
        payload_json = json.dumps(
            revision.payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        duplicate = connection.execute(
            """
            SELECT 1
            FROM btc_structure_event_revision
            WHERE monitor_id = ? AND event_key = ? AND payload_sha256 = ?
            """,
            (monitor_id, revision.event_key, payload_sha256),
        ).fetchone()
        if duplicate is not None:
            return
        latest = connection.execute(
            """
            SELECT revision_no, state, event_at, payload_json
            FROM btc_structure_event_revision
            WHERE monitor_id = ? AND event_key = ?
            ORDER BY revision_no DESC
            LIMIT 1
            """,
            (monitor_id, revision.event_key),
        ).fetchone()
        if latest is None:
            if revision.state != "PENDING":
                raise RuntimeError("BTC_STRUCTURE_EVENT_NOT_FROZEN")
            revision_no = 1
        else:
            if str(latest["state"]) != "PENDING" or revision.state == "PENDING":
                raise RuntimeError("BTC_STRUCTURE_EVENT_TRANSITION_INVALID")
            if parse_utc(str(latest["event_at"])) != revision.event_at:
                raise RuntimeError("BTC_STRUCTURE_EVENT_TIME_CHANGED")
            frozen_payload = json.loads(str(latest["payload_json"]))
            frozen_signal = frozen_payload.get("signal")
            if revision.payload.get("signal") != frozen_signal:
                raise RuntimeError("BTC_STRUCTURE_EVENT_SIGNAL_CHANGED")
            revision_no = int(latest["revision_no"]) + 1
        connection.execute(
            """
            INSERT INTO btc_structure_event_revision (
                monitor_id, event_key, revision_no, event_at, observed_at,
                state, payload_sha256, payload_json, source_run_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                monitor_id,
                revision.event_key,
                revision_no,
                iso_utc(revision.event_at),
                iso_utc(revision.observed_at),
                revision.state,
                payload_sha256,
                payload_json,
                run_id,
                created_at,
            ),
        )

    @staticmethod
    def _insert_btc_monthly_research_revision(
        connection: sqlite3.Connection,
        *,
        run_id: int,
        monitor_id: str,
        history_started_at: datetime,
        history_algorithm_version: str,
        revision: BtcMonthlyResearchRevision,
        created_at: str,
    ) -> None:
        if not revision.signal_key or len(revision.signal_key) > 256:
            raise RuntimeError("BTC_MONTHLY_SIGNAL_KEY_INVALID")
        if revision.signal_at < history_started_at:
            raise RuntimeError("BTC_MONTHLY_SIGNAL_PREDATES_LEDGER")
        if revision.observed_at < revision.signal_at:
            raise RuntimeError("BTC_MONTHLY_SIGNAL_OBSERVED_BEFORE_CLOSE")
        payload_version = revision.payload.get("algorithm_version")
        if str(payload_version or "") != history_algorithm_version:
            raise RuntimeError("BTC_MONTHLY_SIGNAL_VERSION_CHANGED")
        signal = revision.payload.get("signal")
        if not isinstance(signal, dict):
            raise RuntimeError("BTC_MONTHLY_SIGNAL_INVALID")
        try:
            execution_eligible_at = parse_utc(str(signal["execution_eligible_at"]))
        except (KeyError, TypeError, ValueError):
            raise RuntimeError("BTC_MONTHLY_EXECUTION_TIME_INVALID") from None
        payload_json = json.dumps(
            revision.payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        duplicate = connection.execute(
            """
            SELECT 1
            FROM btc_monthly_research_revision
            WHERE monitor_id = ? AND signal_key = ? AND payload_sha256 = ?
            """,
            (monitor_id, revision.signal_key, payload_sha256),
        ).fetchone()
        if duplicate is not None:
            return
        latest = connection.execute(
            """
            SELECT revision_no, state, signal_at, payload_json
            FROM btc_monthly_research_revision
            WHERE monitor_id = ? AND signal_key = ?
            ORDER BY revision_no DESC
            LIMIT 1
            """,
            (monitor_id, revision.signal_key),
        ).fetchone()
        if latest is None:
            if revision.state != "SIGNAL_FROZEN":
                raise RuntimeError("BTC_MONTHLY_SIGNAL_NOT_FROZEN")
            if revision.observed_at >= execution_eligible_at:
                raise RuntimeError("BTC_MONTHLY_SIGNAL_FROZEN_TOO_LATE")
            revision_no = 1
        else:
            if (
                str(latest["state"]) != "SIGNAL_FROZEN"
                or revision.state != "EXECUTION_CAPTURED"
            ):
                raise RuntimeError("BTC_MONTHLY_SIGNAL_TRANSITION_INVALID")
            if parse_utc(str(latest["signal_at"])) != revision.signal_at:
                raise RuntimeError("BTC_MONTHLY_SIGNAL_TIME_CHANGED")
            frozen_payload = json.loads(str(latest["payload_json"]))
            if revision.payload.get("signal") != frozen_payload.get("signal"):
                raise RuntimeError("BTC_MONTHLY_SIGNAL_CHANGED")
            if revision.observed_at < execution_eligible_at:
                raise RuntimeError("BTC_MONTHLY_EXECUTION_OBSERVED_TOO_EARLY")
            if not isinstance(revision.payload.get("execution"), dict):
                raise RuntimeError("BTC_MONTHLY_EXECUTION_INVALID")
            revision_no = int(latest["revision_no"]) + 1
        connection.execute(
            """
            INSERT INTO btc_monthly_research_revision (
                monitor_id, signal_key, revision_no, signal_at, observed_at,
                state, payload_sha256, payload_json, source_run_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                monitor_id,
                revision.signal_key,
                revision_no,
                iso_utc(revision.signal_at),
                iso_utc(revision.observed_at),
                revision.state,
                payload_sha256,
                payload_json,
                run_id,
                created_at,
            ),
        )

    def fail_run(
        self,
        run_id: int,
        monitor_id: str,
        reason_code: str,
        *,
        completed_at: datetime | None = None,
        context: dict[str, str | int | float | bool | None] | None = None,
    ) -> None:
        completed_text = iso_utc(completed_at or utc_now())
        with self._connect() as connection:
            current = connection.execute(
                "SELECT monitor_id, status FROM monitor_run WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if current is None or str(current["monitor_id"]) != monitor_id:
                raise RuntimeError("MONITOR_RUN_NOT_FOUND")
            if current["status"] != "RUNNING":
                raise RuntimeError("MONITOR_RUN_ALREADY_FINISHED")
            connection.execute(
                """
                INSERT INTO monitor_issue (
                    run_id, monitor_id, occurred_at, scope, reason_code,
                    context_json
                ) VALUES (?, ?, ?, 'monitor', ?, ?)
                """,
                (
                    run_id,
                    monitor_id,
                    completed_text,
                    reason_code,
                    _issue_context_json(context),
                ),
            )
            connection.execute(
                """
                UPDATE monitor_run
                SET completed_at = ?, status = 'FAILED', error_code = ?
                WHERE run_id = ?
                """,
                (completed_text, reason_code, run_id),
            )

    def latest_run(self, monitor_id: str) -> StoredRun | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id, monitor_id, started_at, completed_at, status,
                       sample_count, error_code
                FROM monitor_run
                WHERE monitor_id = ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (monitor_id,),
            ).fetchone()
        return self._run_from_row(row) if row is not None else None

    def latest_sample_run(self, monitor_id: str) -> StoredRun | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id, monitor_id, started_at, completed_at, status,
                       sample_count, error_code
                FROM monitor_run
                WHERE monitor_id = ? AND sample_count > 0
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (monitor_id,),
            ).fetchone()
        return self._run_from_row(row) if row is not None else None

    def latest_completed_run(self, monitor_id: str) -> StoredRun | None:
        """Return the newest committed result, including valid empty event scans."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id, monitor_id, started_at, completed_at, status,
                       sample_count, error_code
                FROM monitor_run
                WHERE monitor_id = ? AND status IN ('SUCCESS', 'PARTIAL')
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (monitor_id,),
            ).fetchone()
        return self._run_from_row(row) if row is not None else None

    def latest_finished_run(self, monitor_id: str) -> StoredRun | None:
        """Return the latest terminal run, including collection failures."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id, monitor_id, started_at, completed_at, status,
                       sample_count, error_code
                FROM monitor_run
                WHERE monitor_id = ? AND status != 'RUNNING'
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (monitor_id,),
            ).fetchone()
        return self._run_from_row(row) if row is not None else None

    def latest_successful_run(self, monitor_id: str) -> StoredRun | None:
        """Return the latest issue-free run for diagnostic recovery state."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id, monitor_id, started_at, completed_at, status,
                       sample_count, error_code
                FROM monitor_run
                WHERE monitor_id = ? AND status = 'SUCCESS'
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (monitor_id,),
            ).fetchone()
        return self._run_from_row(row) if row is not None else None

    def samples_for_run(self, run_id: int) -> tuple[StoredSample, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT sample_id, run_id, monitor_id, series_key, entity_key,
                       observed_at, value_text, unit, payload_json
                FROM monitor_sample
                WHERE run_id = ?
                ORDER BY sample_id
                """,
                (run_id,),
            ).fetchall()
        return tuple(self._sample_from_row(row) for row in rows)

    def latest_samples_by_entity(
        self,
        monitor_id: str,
        entity_keys: tuple[str, ...],
    ) -> tuple[StoredSample, ...]:
        if not entity_keys:
            return ()
        placeholders = ",".join("?" for _ in entity_keys)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT sample_id, run_id, monitor_id, series_key, entity_key,
                       observed_at, value_text, unit, payload_json
                FROM monitor_sample AS sample
                WHERE sample.monitor_id = ?
                  AND sample.entity_key IN ({placeholders})
                  AND sample.sample_id = (
                      SELECT MAX(candidate.sample_id)
                      FROM monitor_sample AS candidate
                      WHERE candidate.monitor_id = sample.monitor_id
                        AND candidate.entity_key = sample.entity_key
                  )
                ORDER BY sample.entity_key
                """,
                (monitor_id, *entity_keys),
            ).fetchall()
        return tuple(self._sample_from_row(row) for row in rows)

    def latest_forward_evaluations_by_entity(
        self,
        monitor_id: str,
        entity_keys: tuple[str, ...],
        *,
        source: str | None = None,
    ) -> tuple[StoredForwardEvaluation, ...]:
        if not entity_keys:
            return ()
        placeholders = ",".join("?" for _ in entity_keys)
        source_filter = " AND evaluation.source = ?" if source else ""
        candidate_source_filter = " AND candidate.source = ?" if source else ""
        parameters: list[Any] = [monitor_id, *entity_keys]
        if source:
            parameters.extend((source, source))
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM monitor_forward_evaluation AS evaluation
                WHERE evaluation.monitor_id = ?
                  AND evaluation.entity_key IN ({placeholders})
                  {source_filter}
                  AND evaluation.evaluation_id = (
                      SELECT MAX(candidate.evaluation_id)
                      FROM monitor_forward_evaluation AS candidate
                      WHERE candidate.monitor_id = evaluation.monitor_id
                        AND candidate.entity_key = evaluation.entity_key
                        {candidate_source_filter}
                  )
                ORDER BY evaluation.entity_key
                """,
                parameters,
            ).fetchall()
        return tuple(self._evaluation_from_row(row) for row in rows)

    def pending_forward_evaluations(
        self,
        monitor_id: str,
        *,
        due_before: datetime,
        limit: int,
    ) -> tuple[ForwardEvaluationCase, ...]:
        if limit < 1:
            raise ValueError("evaluation limit must be positive")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM monitor_forward_evaluation
                WHERE monitor_id = ? AND status = 'PENDING' AND due_at <= ?
                ORDER BY due_at, evaluation_id
                LIMIT ?
                """,
                (monitor_id, iso_utc(due_before), limit),
            ).fetchall()
        return tuple(
            ForwardEvaluationCase(
                case_key=str(row["case_key"]),
                entity_key=str(row["entity_key"]),
                stage=str(row["stage"]),
                stage_label=str(row["stage_label"]),
                direction=str(row["direction"]),  # type: ignore[arg-type]
                signal_observed_at=parse_utc(str(row["signal_observed_at"])),
                source_cutoff_at=parse_utc(str(row["source_cutoff_at"])),
                horizon_minutes=int(row["horizon_minutes"]),
                due_at=parse_utc(str(row["due_at"])),
                entry_price_text=str(row["entry_price_text"]),
                benchmark_entry_price_text=str(row["benchmark_entry_price_text"]),
                source=str(row["source"]),
            )
            for row in rows
        )

    def recent_forward_evaluations(
        self,
        monitor_id: str,
        *,
        limit: int = 120,
        source: str | None = None,
    ) -> tuple[StoredForwardEvaluation, ...]:
        source_filter = " AND source = ?" if source else ""
        parameters: list[Any] = [monitor_id]
        if source:
            parameters.append(source)
        parameters.append(limit)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM monitor_forward_evaluation
                WHERE monitor_id = ?
                {source_filter}
                ORDER BY source_cutoff_at DESC, evaluation_id DESC
                LIMIT ?
                """,
                parameters,
            ).fetchall()
        return tuple(self._evaluation_from_row(row) for row in rows)

    def forward_evaluation_summary(
        self,
        monitor_id: str,
        *,
        now: datetime,
        source: str | None = None,
    ) -> dict[str, Any]:
        now_text = iso_utc(now)
        source_filter = " AND source = ?" if source else ""
        count_parameters: list[Any] = [
            now_text,
            now_text,
            now_text,
            monitor_id,
        ]
        group_parameters: list[Any] = [monitor_id]
        if source:
            count_parameters.append(source)
            group_parameters.append(source)
        with self._connect() as connection:
            counts = connection.execute(
                f"""
                SELECT
                    COUNT(*) AS total_cases,
                    SUM(CASE WHEN due_at <= ? THEN 1 ELSE 0 END) AS due_cases,
                    SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END)
                        AS completed_cases,
                    COUNT(DISTINCT CASE WHEN status = 'COMPLETE'
                        THEN source_cutoff_at END) AS distinct_cutoff_count,
                    COUNT(DISTINCT CASE WHEN status = 'COMPLETE'
                        THEN entity_key END) AS distinct_entity_count,
                    MIN(CASE WHEN status = 'COMPLETE'
                        THEN source_cutoff_at END) AS first_cutoff_at,
                    MAX(CASE WHEN status = 'COMPLETE'
                        THEN outcome_cutoff_at END) AS last_outcome_at,
                    SUM(CASE WHEN status = 'UNAVAILABLE' THEN 1 ELSE 0 END)
                        AS unavailable_cases,
                    SUM(CASE WHEN status = 'PENDING' AND due_at <= ? THEN 1 ELSE 0 END)
                        AS pending_due_cases,
                    SUM(CASE WHEN status = 'PENDING' AND due_at > ? THEN 1 ELSE 0 END)
                        AS pending_future_cases
                FROM monitor_forward_evaluation
                WHERE monitor_id = ?
                {source_filter}
                """,
                count_parameters,
            ).fetchone()
            groups = connection.execute(
                f"""
                SELECT stage, stage_label, horizon_minutes,
                       COUNT(*) AS sample_count,
                       COUNT(DISTINCT source_cutoff_at) AS distinct_cutoff_count,
                       COUNT(DISTINCT entity_key) AS distinct_entity_count,
                       MIN(source_cutoff_at) AS first_cutoff_at,
                       MAX(outcome_cutoff_at) AS last_outcome_at,
                       SUM(CASE WHEN verdict = 'ALIGNED' THEN 1 ELSE 0 END)
                           AS aligned_count,
                       AVG(relative_return_percent) AS average_relative_return_percent,
                       AVG(maximum_favorable_excursion_percent)
                           AS average_favorable_excursion_percent,
                       AVG(maximum_adverse_excursion_percent)
                           AS average_adverse_excursion_percent
                FROM monitor_forward_evaluation
                WHERE monitor_id = ? AND status = 'COMPLETE'
                {source_filter}
                GROUP BY stage, stage_label, horizon_minutes
                ORDER BY stage_label, horizon_minutes
                """,
                group_parameters,
            ).fetchall()
        count_payload = {
            key: int(counts[key] or 0)
            for key in (
                "total_cases",
                "due_cases",
                "completed_cases",
                "distinct_cutoff_count",
                "distinct_entity_count",
                "unavailable_cases",
                "pending_due_cases",
                "pending_future_cases",
            )
        }
        return {
            **count_payload,
            "first_cutoff_at": (
                parse_utc(str(counts["first_cutoff_at"]))
                if counts["first_cutoff_at"] is not None
                else None
            ),
            "last_outcome_at": (
                parse_utc(str(counts["last_outcome_at"]))
                if counts["last_outcome_at"] is not None
                else None
            ),
            "groups": [
                {
                    "stage": str(row["stage"]),
                    "stage_label": str(row["stage_label"]),
                    "horizon_minutes": int(row["horizon_minutes"]),
                    "sample_count": int(row["sample_count"]),
                    "distinct_cutoff_count": int(row["distinct_cutoff_count"]),
                    "distinct_entity_count": int(row["distinct_entity_count"]),
                    "first_cutoff_at": parse_utc(str(row["first_cutoff_at"])),
                    "last_outcome_at": parse_utc(str(row["last_outcome_at"])),
                    "aligned_count": int(row["aligned_count"] or 0),
                    "average_relative_return_percent": row[
                        "average_relative_return_percent"
                    ],
                    "average_favorable_excursion_percent": row[
                        "average_favorable_excursion_percent"
                    ],
                    "average_adverse_excursion_percent": row[
                        "average_adverse_excursion_percent"
                    ],
                }
                for row in groups
            ],
        }

    def forward_evaluation_comparison(
        self,
        monitor_id: str,
        *,
        primary_source: str,
        baseline_source: str,
    ) -> dict[str, Any]:
        """Compare predictions frozen for the exact same entity and horizon."""

        join = """
            FROM monitor_forward_evaluation AS primary_evaluation
            JOIN monitor_forward_evaluation AS baseline_evaluation
              ON baseline_evaluation.monitor_id = primary_evaluation.monitor_id
             AND baseline_evaluation.entity_key = primary_evaluation.entity_key
             AND baseline_evaluation.source_cutoff_at =
                 primary_evaluation.source_cutoff_at
             AND baseline_evaluation.horizon_minutes =
                 primary_evaluation.horizon_minutes
            WHERE primary_evaluation.monitor_id = ?
              AND primary_evaluation.source = ?
              AND baseline_evaluation.source = ?
        """
        complete_pair = """
            primary_evaluation.status = 'COMPLETE'
            AND baseline_evaluation.status = 'COMPLETE'
        """
        direction_relation = """
            CASE WHEN primary_evaluation.direction = baseline_evaluation.direction
                 THEN 'SAME_DIRECTION' ELSE 'DIRECTION_FLIP' END
        """
        parameters = (monitor_id, primary_source, baseline_source)
        with self._connect() as connection:
            totals = connection.execute(
                f"""
                SELECT COUNT(*) AS paired_case_count,
                       SUM(CASE WHEN {complete_pair} THEN 1 ELSE 0 END)
                           AS sample_count,
                       SUM(CASE WHEN (
                           primary_evaluation.status = 'UNAVAILABLE'
                           OR baseline_evaluation.status = 'UNAVAILABLE'
                       ) THEN 1 ELSE 0 END) AS unavailable_pair_count,
                       SUM(CASE WHEN
                           primary_evaluation.status != 'UNAVAILABLE'
                           AND baseline_evaluation.status != 'UNAVAILABLE'
                           AND (
                               primary_evaluation.status = 'PENDING'
                               OR baseline_evaluation.status = 'PENDING'
                           ) THEN 1 ELSE 0 END) AS pending_pair_count,
                       SUM(CASE WHEN {complete_pair}
                           AND primary_evaluation.verdict = 'ALIGNED'
                           THEN 1 ELSE 0 END) AS primary_aligned_count,
                       SUM(CASE WHEN {complete_pair}
                           AND primary_evaluation.verdict = 'OPPOSED'
                           THEN 1 ELSE 0 END) AS primary_opposed_count,
                       SUM(CASE WHEN {complete_pair}
                           AND baseline_evaluation.verdict = 'ALIGNED'
                           THEN 1 ELSE 0 END) AS baseline_aligned_count,
                       SUM(CASE WHEN {complete_pair}
                           AND baseline_evaluation.verdict = 'OPPOSED'
                           THEN 1 ELSE 0 END) AS baseline_opposed_count,
                       COUNT(DISTINCT CASE WHEN {complete_pair}
                           THEN primary_evaluation.source_cutoff_at END)
                           AS distinct_cutoff_count,
                       COUNT(DISTINCT CASE WHEN {complete_pair}
                           THEN primary_evaluation.entity_key END)
                           AS distinct_entity_count,
                       MIN(CASE WHEN {complete_pair}
                           THEN primary_evaluation.source_cutoff_at END)
                           AS first_cutoff_at,
                       MAX(CASE WHEN {complete_pair}
                           THEN primary_evaluation.outcome_cutoff_at END)
                           AS last_outcome_at
                {join}
                """,
                parameters,
            ).fetchone()
            relations = connection.execute(
                f"""
                SELECT {direction_relation} AS direction_relation,
                       COUNT(*) AS paired_case_count,
                       SUM(CASE WHEN {complete_pair} THEN 1 ELSE 0 END)
                           AS sample_count,
                       SUM(CASE WHEN (
                           primary_evaluation.status = 'UNAVAILABLE'
                           OR baseline_evaluation.status = 'UNAVAILABLE'
                       ) THEN 1 ELSE 0 END) AS unavailable_pair_count,
                       SUM(CASE WHEN
                           primary_evaluation.status != 'UNAVAILABLE'
                           AND baseline_evaluation.status != 'UNAVAILABLE'
                           AND (
                               primary_evaluation.status = 'PENDING'
                               OR baseline_evaluation.status = 'PENDING'
                           ) THEN 1 ELSE 0 END) AS pending_pair_count,
                       SUM(CASE WHEN {complete_pair}
                           AND primary_evaluation.verdict = 'ALIGNED'
                           THEN 1 ELSE 0 END) AS primary_aligned_count,
                       SUM(CASE WHEN {complete_pair}
                           AND primary_evaluation.verdict = 'OPPOSED'
                           THEN 1 ELSE 0 END) AS primary_opposed_count,
                       SUM(CASE WHEN {complete_pair}
                           AND baseline_evaluation.verdict = 'ALIGNED'
                           THEN 1 ELSE 0 END) AS baseline_aligned_count,
                       SUM(CASE WHEN {complete_pair}
                           AND baseline_evaluation.verdict = 'OPPOSED'
                           THEN 1 ELSE 0 END) AS baseline_opposed_count,
                       COUNT(DISTINCT CASE WHEN {complete_pair}
                           THEN primary_evaluation.source_cutoff_at END)
                           AS distinct_cutoff_count,
                       COUNT(DISTINCT CASE WHEN {complete_pair}
                           THEN primary_evaluation.entity_key END)
                           AS distinct_entity_count,
                       MIN(CASE WHEN {complete_pair}
                           THEN primary_evaluation.source_cutoff_at END)
                           AS first_cutoff_at,
                       MAX(CASE WHEN {complete_pair}
                           THEN primary_evaluation.outcome_cutoff_at END)
                           AS last_outcome_at
                {join}
                GROUP BY {direction_relation}
                """,
                parameters,
            ).fetchall()
            groups = connection.execute(
                f"""
                SELECT primary_evaluation.stage AS stage,
                       primary_evaluation.stage_label AS stage_label,
                       primary_evaluation.horizon_minutes AS horizon_minutes,
                       {direction_relation} AS direction_relation,
                       COUNT(*) AS paired_case_count,
                       SUM(CASE WHEN {complete_pair} THEN 1 ELSE 0 END)
                           AS sample_count,
                       SUM(CASE WHEN (
                           primary_evaluation.status = 'UNAVAILABLE'
                           OR baseline_evaluation.status = 'UNAVAILABLE'
                       ) THEN 1 ELSE 0 END) AS unavailable_pair_count,
                       SUM(CASE WHEN
                           primary_evaluation.status != 'UNAVAILABLE'
                           AND baseline_evaluation.status != 'UNAVAILABLE'
                           AND (
                               primary_evaluation.status = 'PENDING'
                               OR baseline_evaluation.status = 'PENDING'
                           ) THEN 1 ELSE 0 END) AS pending_pair_count,
                       SUM(CASE WHEN {complete_pair}
                           AND primary_evaluation.verdict = 'ALIGNED'
                           THEN 1 ELSE 0 END) AS primary_aligned_count,
                       SUM(CASE WHEN {complete_pair}
                           AND primary_evaluation.verdict = 'OPPOSED'
                           THEN 1 ELSE 0 END) AS primary_opposed_count,
                       SUM(CASE WHEN {complete_pair}
                           AND baseline_evaluation.verdict = 'ALIGNED'
                           THEN 1 ELSE 0 END) AS baseline_aligned_count,
                       SUM(CASE WHEN {complete_pair}
                           AND baseline_evaluation.verdict = 'OPPOSED'
                           THEN 1 ELSE 0 END) AS baseline_opposed_count,
                       COUNT(DISTINCT CASE WHEN {complete_pair}
                           THEN primary_evaluation.source_cutoff_at END)
                           AS distinct_cutoff_count,
                       COUNT(DISTINCT CASE WHEN {complete_pair}
                           THEN primary_evaluation.entity_key END)
                           AS distinct_entity_count,
                       MIN(CASE WHEN {complete_pair}
                           THEN primary_evaluation.source_cutoff_at END)
                           AS first_cutoff_at,
                       MAX(CASE WHEN {complete_pair}
                           THEN primary_evaluation.outcome_cutoff_at END)
                           AS last_outcome_at
                {join}
                GROUP BY primary_evaluation.stage,
                         primary_evaluation.stage_label,
                         primary_evaluation.horizon_minutes,
                         {direction_relation}
                ORDER BY primary_evaluation.stage_label,
                         primary_evaluation.horizon_minutes,
                         direction_relation
                """,
                parameters,
            ).fetchall()

        count_keys = (
            "paired_case_count",
            "sample_count",
            "pending_pair_count",
            "unavailable_pair_count",
            "primary_aligned_count",
            "primary_opposed_count",
            "baseline_aligned_count",
            "baseline_opposed_count",
            "distinct_cutoff_count",
            "distinct_entity_count",
        )

        def metrics(row: sqlite3.Row | None) -> dict[str, Any]:
            if row is None:
                return {
                    **{key: 0 for key in count_keys},
                    "first_cutoff_at": None,
                    "last_outcome_at": None,
                }
            return {
                **{key: int(row[key] or 0) for key in count_keys},
                "first_cutoff_at": (
                    parse_utc(str(row["first_cutoff_at"]))
                    if row["first_cutoff_at"] is not None
                    else None
                ),
                "last_outcome_at": (
                    parse_utc(str(row["last_outcome_at"]))
                    if row["last_outcome_at"] is not None
                    else None
                ),
            }

        relation_rows = {str(row["direction_relation"]): row for row in relations}
        return {
            **metrics(totals),
            "relations": [
                {
                    "direction_relation": relation,
                    **metrics(relation_rows.get(relation)),
                }
                for relation in ("DIRECTION_FLIP", "SAME_DIRECTION")
            ],
            "groups": [
                {
                    "stage": str(row["stage"]),
                    "stage_label": str(row["stage_label"]),
                    "horizon_minutes": int(row["horizon_minutes"]),
                    "direction_relation": str(row["direction_relation"]),
                    **metrics(row),
                }
                for row in groups
            ],
        }

    def history(
        self,
        monitor_id: str,
        series_key: str,
        *,
        since: datetime,
        limit: int = 1000,
    ) -> tuple[StoredSample, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT sample_id, run_id, monitor_id, series_key, entity_key,
                       observed_at, value_text, unit, payload_json
                FROM (
                    SELECT sample_id, run_id, monitor_id, series_key, entity_key,
                           observed_at, value_text, unit, payload_json
                    FROM monitor_sample
                    WHERE monitor_id = ? AND series_key = ? AND observed_at >= ?
                    ORDER BY observed_at DESC, sample_id DESC
                    LIMIT ?
                )
                ORDER BY observed_at ASC, sample_id ASC
                """,
                (monitor_id, series_key, iso_utc(since), limit),
            ).fetchall()
        return tuple(self._sample_from_row(row) for row in rows)

    def recent_issues(
        self, monitor_id: str, *, limit: int = 20
    ) -> tuple[StoredIssue, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT issue_id, run_id, monitor_id, occurred_at, scope,
                       reason_code, context_json
                FROM monitor_issue
                WHERE monitor_id = ?
                ORDER BY issue_id DESC
                LIMIT ?
                """,
                (monitor_id, limit),
            ).fetchall()
        return tuple(self._issue_from_row(row) for row in rows)

    def issues_for_run(self, run_id: int) -> tuple[StoredIssue, ...]:
        """Return every affected scope from one run for in-place UI marking."""

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT issue_id, run_id, monitor_id, occurred_at, scope,
                       reason_code, context_json
                FROM monitor_issue
                WHERE run_id = ?
                ORDER BY issue_id
                """,
                (run_id,),
            ).fetchall()
        return tuple(self._issue_from_row(row) for row in rows)

    def artifacts_for_run(self, run_id: int) -> tuple[StoredArtifact, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT artifact_id, run_id, monitor_id, artifact_key, source,
                       request_started_at, response_completed_at,
                       http_status, business_code,
                       schema_hash, response_sha256, record_count, response_body
                FROM monitor_artifact
                WHERE run_id = ?
                ORDER BY artifact_id
                """,
                (run_id,),
            ).fetchall()
        return tuple(
            StoredArtifact(
                artifact_id=int(row["artifact_id"]),
                run_id=int(row["run_id"]),
                monitor_id=str(row["monitor_id"]),
                artifact_key=str(row["artifact_key"]),
                source=str(row["source"]),
                request_started_at=parse_utc(str(row["request_started_at"])),
                response_completed_at=parse_utc(str(row["response_completed_at"])),
                http_status=int(row["http_status"]),
                business_code=(
                    str(row["business_code"])
                    if row["business_code"] is not None
                    else None
                ),
                schema_hash=str(row["schema_hash"]),
                response_sha256=str(row["response_sha256"]),
                record_count=(
                    int(row["record_count"])
                    if row["record_count"] is not None
                    else None
                ),
                response_body=str(row["response_body"]),
            )
            for row in rows
        )

    def projection_snapshot(
        self,
        monitor_id: str,
        snapshot_key: str,
    ) -> StoredProjectionSnapshot | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT monitor_id, snapshot_key, run_id,
                       observed_at, cutoff_at, payload_json
                FROM monitor_projection_snapshot
                WHERE monitor_id = ? AND snapshot_key = ?
                """,
                (monitor_id, snapshot_key),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("MONITOR_PROJECTION_SNAPSHOT_PAYLOAD_INVALID")
        return StoredProjectionSnapshot(
            monitor_id=str(row["monitor_id"]),
            snapshot_key=str(row["snapshot_key"]),
            run_id=int(row["run_id"]),
            observed_at=parse_utc(str(row["observed_at"])),
            cutoff_at=parse_utc(str(row["cutoff_at"])),
            payload=payload,
        )

    def buyback_source_states(
        self,
        monitor_id: str,
    ) -> tuple[StoredBuybackSourceState, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT monitor_id, source_key, source_label, status,
                       checked_at, source_time, next_due_at, record_count,
                       detail_code, payload_json, last_run_id
                FROM buyback_source_state
                WHERE monitor_id = ?
                ORDER BY source_key
                """,
                (monitor_id,),
            ).fetchall()
        return tuple(self._buyback_source_from_row(row) for row in rows)

    def buyback_document_for_source(
        self,
        monitor_id: str,
        source_key: str,
        source_document_id: str,
    ) -> StoredBuybackDocument | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM buyback_document
                WHERE monitor_id = ? AND source_key = ? AND source_document_id = ?
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (monitor_id, source_key, source_document_id),
            ).fetchone()
        return self._buyback_document_from_row(row) if row is not None else None

    def buyback_document(self, sha256: str) -> StoredBuybackDocument | None:
        if len(sha256) != 64 or any(
            value not in "0123456789abcdef" for value in sha256
        ):
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM buyback_document WHERE sha256 = ?",
                (sha256,),
            ).fetchone()
        return self._buyback_document_from_row(row) if row is not None else None

    def buyback_document_path(
        self,
        document: StoredBuybackDocument,
        *,
        verify_content: bool = True,
    ) -> Path:
        root = self.buyback_evidence_root.resolve()
        candidate = self.path.parent / Path(document.relative_path)
        if candidate.is_symlink():
            raise RuntimeError("BUYBACK_EVIDENCE_LINK_UNSUPPORTED")
        try:
            stat_result = candidate.stat(follow_symlinks=False)
        except FileNotFoundError:
            raise RuntimeError("BUYBACK_EVIDENCE_FILE_MISSING") from None
        if getattr(stat_result, "st_file_attributes", 0) & 0x400:
            raise RuntimeError("BUYBACK_EVIDENCE_LINK_UNSUPPORTED")
        path = candidate.resolve()
        try:
            path.relative_to(root)
        except ValueError:
            raise RuntimeError("BUYBACK_EVIDENCE_PATH_INVALID") from None
        if not path.is_file():
            raise RuntimeError("BUYBACK_EVIDENCE_FILE_MISSING")
        if stat_result.st_size != document.size_bytes:
            raise RuntimeError("BUYBACK_EVIDENCE_SIZE_MISMATCH")
        if verify_content:
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if digest != document.sha256:
                raise RuntimeError("BUYBACK_EVIDENCE_CONTENT_MISMATCH")
        return path

    def latest_buyback_entities(
        self,
        monitor_id: str,
        *,
        limit: int = 1000,
    ) -> tuple[StoredBuybackEntity, ...]:
        if not 1 <= limit <= 20_000:
            raise ValueError("buyback entity limit out of range")
        with self._connect() as connection:
            rows = connection.execute(
                """
                WITH latest AS (
                    SELECT monitor_id, entity_key, MAX(revision_no) AS revision_no
                    FROM buyback_entity_revision
                    WHERE monitor_id = ?
                    GROUP BY monitor_id, entity_key
                )
                SELECT revision.*,
                       review.review_id AS latest_review_id,
                       review.base_revision_no AS latest_review_base_revision_no,
                       review.decision AS latest_review_decision,
                       review.corrected_event_type AS latest_review_event_type,
                       review.program_key AS latest_review_program_key,
                       review.program_status AS latest_review_program_status,
                       review.note AS latest_review_note,
                       review.created_at AS latest_review_created_at
                FROM latest
                JOIN buyback_entity_revision AS revision
                  ON revision.monitor_id = latest.monitor_id
                 AND revision.entity_key = latest.entity_key
                 AND revision.revision_no = latest.revision_no
                LEFT JOIN buyback_review AS review
                  ON review.review_id = (
                      SELECT MAX(candidate.review_id)
                      FROM buyback_review AS candidate
                      WHERE candidate.monitor_id = revision.monitor_id
                        AND candidate.entity_key = revision.entity_key
                        AND candidate.base_revision_no = revision.revision_no
                  )
                ORDER BY revision.effective_at DESC, revision.revision_id DESC
                LIMIT ?
                """,
                (monitor_id, limit),
            ).fetchall()
        return tuple(self._buyback_entity_from_row(row) for row in rows)

    def buyback_projection_version(self, monitor_id: str) -> tuple[int, int, int]:
        """Compact cache key for every durable input to the buyback projection."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COALESCE((
                        SELECT MAX(revision_id)
                        FROM buyback_entity_revision
                        WHERE monitor_id = ?
                    ), 0) AS revision_id,
                    COALESCE((
                        SELECT MAX(review_id)
                        FROM buyback_review
                        WHERE monitor_id = ?
                    ), 0) AS review_id,
                    COALESCE((
                        SELECT MAX(last_run_id)
                        FROM buyback_source_state
                        WHERE monitor_id = ?
                    ), 0) AS source_run_id
                """,
                (monitor_id, monitor_id, monitor_id),
            ).fetchone()
        if row is None:
            return (0, 0, 0)
        return (
            int(row["revision_id"]),
            int(row["review_id"]),
            int(row["source_run_id"]),
        )

    def buyback_entity(
        self,
        monitor_id: str,
        entity_key: str,
    ) -> StoredBuybackEntity | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT revision.*,
                       review.review_id AS latest_review_id,
                       review.base_revision_no AS latest_review_base_revision_no,
                       review.decision AS latest_review_decision,
                       review.corrected_event_type AS latest_review_event_type,
                       review.program_key AS latest_review_program_key,
                       review.program_status AS latest_review_program_status,
                       review.note AS latest_review_note,
                       review.created_at AS latest_review_created_at
                FROM buyback_entity_revision AS revision
                LEFT JOIN buyback_review AS review
                  ON review.review_id = (
                      SELECT MAX(candidate.review_id)
                      FROM buyback_review AS candidate
                      WHERE candidate.monitor_id = revision.monitor_id
                        AND candidate.entity_key = revision.entity_key
                        AND candidate.base_revision_no = revision.revision_no
                  )
                WHERE revision.monitor_id = ? AND revision.entity_key = ?
                ORDER BY revision.revision_no DESC
                LIMIT 1
                """,
                (monitor_id, entity_key),
            ).fetchone()
        return self._buyback_entity_from_row(row) if row is not None else None

    def buyback_entity_revisions(
        self,
        monitor_id: str,
        entity_key: str,
        *,
        limit: int = 100,
    ) -> tuple[StoredBuybackEntity, ...]:
        if not 1 <= limit <= 500:
            raise ValueError("buyback revision limit out of range")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT revision.*,
                       NULL AS latest_review_id,
                       NULL AS latest_review_base_revision_no,
                       NULL AS latest_review_decision,
                       NULL AS latest_review_event_type,
                       NULL AS latest_review_program_key,
                       NULL AS latest_review_program_status,
                       NULL AS latest_review_note,
                       NULL AS latest_review_created_at
                FROM buyback_entity_revision AS revision
                WHERE monitor_id = ? AND entity_key = ?
                ORDER BY revision_no DESC
                LIMIT ?
                """,
                (monitor_id, entity_key, limit),
            ).fetchall()
        return tuple(self._buyback_entity_from_row(row) for row in rows)

    def buyback_reviews(
        self,
        monitor_id: str,
        entity_key: str,
        *,
        limit: int = 100,
    ) -> tuple[StoredBuybackReview, ...]:
        if not 1 <= limit <= 500:
            raise ValueError("buyback review limit out of range")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM buyback_review
                WHERE monitor_id = ? AND entity_key = ?
                ORDER BY review_id DESC
                LIMIT ?
                """,
                (monitor_id, entity_key, limit),
            ).fetchall()
        return tuple(self._buyback_review_from_row(row) for row in rows)

    def save_buyback_review(
        self,
        monitor_id: str,
        entity_key: str,
        *,
        base_revision_no: int,
        decision: str,
        corrected_event_type: str | None,
        program_key: str | None,
        program_status: str | None,
        note: str,
        created_at: datetime | None = None,
    ) -> StoredBuybackReview:
        decisions = {"CONFIRMED_EVENT", "REJECTED_EVENT", "NEEDS_FOLLOW_UP"}
        program_statuses = {
            "PROPOSED",
            "APPROVED",
            "ACTIVE",
            "COMPLETED",
            "TERMINATED",
            "UNKNOWN",
        }
        if decision not in decisions:
            raise ValueError("BUYBACK_REVIEW_DECISION_INVALID")
        if program_status is not None and program_status not in program_statuses:
            raise ValueError("BUYBACK_PROGRAM_STATUS_INVALID")
        if (
            corrected_event_type is not None
            and corrected_event_type not in BUYBACK_REVIEW_EVENT_TYPES
        ):
            raise ValueError("BUYBACK_EVENT_TYPE_INVALID")
        normalized_program_key = (
            program_key.strip() if program_key is not None else None
        )
        if normalized_program_key == "":
            normalized_program_key = None
        if normalized_program_key is not None and len(normalized_program_key) > 120:
            raise ValueError("BUYBACK_PROGRAM_KEY_INVALID")
        normalized_note = note.strip()
        if len(normalized_note) > 1000:
            raise ValueError("BUYBACK_REVIEW_NOTE_TOO_LONG")
        created = created_at or utc_now()
        created_text = iso_utc(created)
        with self._connect() as connection:
            current = connection.execute(
                """
                SELECT MAX(revision_no)
                FROM buyback_entity_revision
                WHERE monitor_id = ? AND entity_key = ?
                """,
                (monitor_id, entity_key),
            ).fetchone()
            current_revision = (
                int(current[0]) if current and current[0] is not None else None
            )
            if current_revision is None:
                raise KeyError("BUYBACK_ENTITY_NOT_FOUND")
            if current_revision != base_revision_no:
                raise RuntimeError("BUYBACK_REVISION_CONFLICT")
            cursor = connection.execute(
                """
                INSERT INTO buyback_review (
                    monitor_id, entity_key, base_revision_no, decision,
                    corrected_event_type, program_key, program_status,
                    note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    monitor_id,
                    entity_key,
                    base_revision_no,
                    decision,
                    corrected_event_type,
                    normalized_program_key,
                    program_status,
                    normalized_note,
                    created_text,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("BUYBACK_REVIEW_ID_MISSING")
            review_id = int(cursor.lastrowid)
        return StoredBuybackReview(
            review_id=review_id,
            monitor_id=monitor_id,
            entity_key=entity_key,
            base_revision_no=base_revision_no,
            decision=decision,
            corrected_event_type=corrected_event_type,
            program_key=normalized_program_key,
            program_status=program_status,
            note=normalized_note,
            created_at=created,
        )

    def prune(self, retention_days: int, *, now: datetime | None = None) -> int:
        observed_now = now or utc_now()
        cutoff = iso_utc(observed_now - timedelta(days=retention_days))
        buyback_cutoff = iso_utc(
            observed_now - timedelta(days=self.buyback_retention_days)
        )
        market_event_cutoff = iso_utc(
            observed_now - timedelta(days=self.market_event_retention_days)
        )
        btc_structure_cutoff = iso_utc(
            observed_now - timedelta(days=self.btc_structure_retention_days)
        )
        removed_evidence_paths: list[str] = []
        with self._connect() as connection:
            cursor = connection.execute(
                """
                DELETE FROM monitor_run
                WHERE completed_at IS NOT NULL AND completed_at < ?
                """,
                (cutoff,),
            )
            removed_runs = int(cursor.rowcount)
            connection.execute(
                """
                DELETE FROM buyback_review
                WHERE (monitor_id, entity_key) IN (
                    SELECT monitor_id, entity_key
                    FROM buyback_entity_revision
                    GROUP BY monitor_id, entity_key
                    HAVING MAX(effective_at) < ?
                )
                """,
                (buyback_cutoff,),
            )
            connection.execute(
                """
                DELETE FROM buyback_entity_revision
                WHERE (monitor_id, entity_key) IN (
                    SELECT monitor_id, entity_key
                    FROM buyback_entity_revision
                    GROUP BY monitor_id, entity_key
                    HAVING MAX(effective_at) < ?
                )
                """,
                (buyback_cutoff,),
            )
            connection.execute(
                """
                DELETE FROM market_event_revision
                WHERE (monitor_id, event_key) IN (
                    SELECT monitor_id, event_key
                    FROM market_event_revision
                    GROUP BY monitor_id, event_key
                    HAVING MAX(scheduled_at) < ?
                )
                """,
                (market_event_cutoff,),
            )
            connection.execute(
                """
                DELETE FROM btc_structure_event_revision
                WHERE (monitor_id, event_key) IN (
                    SELECT monitor_id, event_key
                    FROM btc_structure_event_revision
                    GROUP BY monitor_id, event_key
                    HAVING MAX(event_at) < ?
                )
                """,
                (btc_structure_cutoff,),
            )
            connection.execute(
                """
                DELETE FROM btc_structure_event_revision
                WHERE (monitor_id, event_key) IN (
                    SELECT monitor_id, event_key
                    FROM (
                        SELECT monitor_id, event_key, MAX(event_at) AS latest_at
                        FROM btc_structure_event_revision
                        GROUP BY monitor_id, event_key
                        ORDER BY latest_at DESC, monitor_id, event_key
                        LIMIT -1 OFFSET ?
                    )
                )
                """,
                (self.btc_structure_max_events,),
            )
            connection.execute(
                """
                DELETE FROM btc_monthly_research_revision
                WHERE (monitor_id, signal_key) IN (
                    SELECT monitor_id, signal_key
                    FROM (
                        SELECT monitor_id, signal_key,
                               ROW_NUMBER() OVER (
                                   PARTITION BY monitor_id
                                   ORDER BY MAX(signal_at) DESC, signal_key DESC
                               ) AS signal_rank
                        FROM btc_monthly_research_revision
                        GROUP BY monitor_id, signal_key
                    )
                    WHERE signal_rank > ?
                )
                """,
                (DEFAULT_BTC_MONTHLY_MAX_SIGNALS,),
            )
            removable = connection.execute(
                """
                SELECT relative_path
                FROM buyback_document AS document
                WHERE document.last_referenced_at < ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM buyback_entity_revision AS revision
                      WHERE revision.document_sha256 = document.sha256
                  )
                """,
                (buyback_cutoff,),
            ).fetchall()
            removed_evidence_paths = [str(row["relative_path"]) for row in removable]
            connection.executemany(
                "DELETE FROM buyback_document WHERE relative_path = ?",
                ((value,) for value in removed_evidence_paths),
            )
        root = self.buyback_evidence_root.resolve()
        for relative_path in removed_evidence_paths:
            candidate = self.path.parent / Path(relative_path)
            if candidate.is_symlink():
                raise RuntimeError("BUYBACK_EVIDENCE_LINK_UNSUPPORTED")
            path = candidate.resolve()
            try:
                path.relative_to(root)
            except ValueError:
                raise RuntimeError("BUYBACK_EVIDENCE_PATH_INVALID") from None
            if path.exists():
                if not path.is_file():
                    raise RuntimeError("BUYBACK_EVIDENCE_TARGET_INVALID")
                try:
                    path.unlink()
                except OSError:
                    # The database row is already gone. A bounded orphan pass
                    # below, and every later maintenance cycle, retries without
                    # retaining an unbounded in-memory deletion queue.
                    pass
        self._prune_buyback_evidence_orphans(
            cutoff=observed_now - timedelta(days=self.buyback_retention_days)
        )
        return removed_runs

    def _prune_buyback_evidence_orphans(
        self,
        *,
        cutoff: datetime,
        maximum_entries: int = 20_000,
    ) -> int:
        """Remove old owned files that no longer have a durable database row."""

        if maximum_entries < 1:
            raise ValueError("maximum_entries must be positive")
        root = self.buyback_evidence_root
        if not root.exists():
            return 0
        with self._connect() as connection:
            referenced = {
                str(row["relative_path"])
                for row in connection.execute(
                    "SELECT relative_path FROM buyback_document"
                ).fetchall()
            }
        cutoff_timestamp = cutoff.timestamp()
        removed = 0
        inspected = 0
        pending = [root]
        while pending and inspected < maximum_entries:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    inspected += 1
                    if inspected > maximum_entries:
                        break
                    if entry.is_symlink():
                        raise RuntimeError("BUYBACK_EVIDENCE_LINK_UNSUPPORTED")
                    stat_result = entry.stat(follow_symlinks=False)
                    if getattr(stat_result, "st_file_attributes", 0) & 0x400:
                        raise RuntimeError("BUYBACK_EVIDENCE_LINK_UNSUPPORTED")
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        raise RuntimeError("BUYBACK_EVIDENCE_ENTRY_INVALID")
                    path = Path(entry.path)
                    relative_path = path.relative_to(self.path.parent).as_posix()
                    if (
                        relative_path in referenced
                        or stat_result.st_mtime >= cutoff_timestamp
                    ):
                        continue
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
                    except OSError:
                        continue
                    removed += 1
        return removed

    def market_event_history_started_at(
        self,
        monitor_id: str,
    ) -> datetime | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT started_at
                FROM market_event_history_state
                WHERE monitor_id = ?
                """,
                (monitor_id,),
            ).fetchone()
        return parse_utc(str(row["started_at"])) if row is not None else None

    def btc_structure_history(
        self,
        monitor_id: str,
    ) -> StoredBtcStructureHistory | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT monitor_id, started_at, processed_through_at,
                       algorithm_version, last_run_id
                FROM btc_structure_history_state
                WHERE monitor_id = ?
                """,
                (monitor_id,),
            ).fetchone()
        if row is None:
            return None
        return StoredBtcStructureHistory(
            monitor_id=str(row["monitor_id"]),
            started_at=parse_utc(str(row["started_at"])),
            processed_through_at=parse_utc(str(row["processed_through_at"])),
            algorithm_version=str(row["algorithm_version"]),
            last_run_id=int(row["last_run_id"]),
        )

    def latest_btc_structure_event_revisions(
        self,
        monitor_id: str,
        *,
        limit: int = 100,
        algorithm_version: str | None = None,
    ) -> tuple[StoredBtcStructureEventRevision, ...]:
        if not 1 <= limit <= self.btc_structure_max_events:
            raise ValueError("limit outside BTC structure event bound")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT revision_id, revision_no, monitor_id, event_key,
                       event_at, observed_at, state, payload_sha256,
                       payload_json, source_run_id, created_at
                FROM btc_structure_event_revision AS revision
                WHERE revision.monitor_id = ?
                  AND revision.revision_no = (
                      SELECT MAX(candidate.revision_no)
                      FROM btc_structure_event_revision AS candidate
                      WHERE candidate.monitor_id = revision.monitor_id
                        AND candidate.event_key = revision.event_key
                  )
                ORDER BY event_at DESC, revision_id DESC
                LIMIT ?
                """,
                (
                    monitor_id,
                    self.btc_structure_max_events if algorithm_version else limit,
                ),
            ).fetchall()
        revisions = tuple(self._btc_structure_revision_from_row(row) for row in rows)
        if algorithm_version is not None:
            revisions = tuple(
                revision
                for revision in revisions
                if str(revision.payload.get("algorithm_version")) == algorithm_version
            )[:limit]
        return revisions

    def pending_btc_structure_event_revisions(
        self,
        monitor_id: str,
        *,
        limit: int = 1000,
        algorithm_version: str | None = None,
    ) -> tuple[StoredBtcStructureEventRevision, ...]:
        if not 1 <= limit <= 5000:
            raise ValueError("limit outside pending BTC structure event bound")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT revision_id, revision_no, monitor_id, event_key,
                       event_at, observed_at, state, payload_sha256,
                       payload_json, source_run_id, created_at
                FROM btc_structure_event_revision AS revision
                WHERE revision.monitor_id = ? AND revision.state = 'PENDING'
                  AND revision.revision_no = (
                      SELECT MAX(candidate.revision_no)
                      FROM btc_structure_event_revision AS candidate
                      WHERE candidate.monitor_id = revision.monitor_id
                        AND candidate.event_key = revision.event_key
                  )
                ORDER BY event_at, revision_id
                LIMIT ?
                """,
                (monitor_id, 5000 if algorithm_version else limit),
            ).fetchall()
        revisions = tuple(self._btc_structure_revision_from_row(row) for row in rows)
        if algorithm_version is not None:
            revisions = tuple(
                revision
                for revision in revisions
                if str(revision.payload.get("algorithm_version")) == algorithm_version
            )[:limit]
        return revisions

    def btc_structure_event_summary(
        self,
        monitor_id: str,
        *,
        algorithm_version: str | None = None,
    ) -> dict[str, Any]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT revision.state, revision.event_at, revision.payload_json
                FROM btc_structure_event_revision AS revision
                WHERE revision.monitor_id = ?
                  AND revision.revision_no = (
                      SELECT MAX(candidate.revision_no)
                      FROM btc_structure_event_revision AS candidate
                      WHERE candidate.monitor_id = revision.monitor_id
                        AND candidate.event_key = revision.event_key
                  )
                ORDER BY revision.event_at, revision.revision_id
                """,
                (monitor_id,),
            ).fetchall()
        records: list[tuple[str, datetime, dict[str, Any]]] = []
        for row in rows:
            payload = json.loads(str(row["payload_json"]))
            if (
                algorithm_version is not None
                and str(payload.get("algorithm_version")) != algorithm_version
            ):
                continue
            records.append(
                (
                    str(row["state"]),
                    parse_utc(str(row["event_at"])),
                    payload,
                )
            )
        values = {
            "total_events": len(records),
            "pending_events": sum(state == "PENDING" for state, _, _ in records),
            "reaction_events": sum(state == "REACTION" for state, _, _ in records),
            "break_events": sum(state == "BREAK" for state, _, _ in records),
            "unresolved_events": sum(state == "UNRESOLVED" for state, _, _ in records),
        }
        completed = (
            values["reaction_events"]
            + values["break_events"]
            + values["unresolved_events"]
        )
        completed_payloads = [
            payload for state, _, payload in records if state != "PENDING"
        ]
        support_events = 0
        resistance_events = 0
        probability_scored_events = 0
        volatility_regimes: set[str] = set()
        net_30: list[float] = []
        net_50: list[float] = []
        for payload in completed_payloads:
            signal = payload.get("signal")
            outcome = payload.get("outcome")
            if not isinstance(signal, dict) or not isinstance(outcome, dict):
                continue
            kind = str(signal.get("kind", ""))
            support_events += kind == "SUPPORT"
            resistance_events += kind == "RESISTANCE"
            probability_scored_events += signal.get("p_reaction") is not None
            features = signal.get("features")
            if isinstance(features, dict) and features.get("volatility_regime"):
                volatility_regimes.add(str(features["volatility_regime"]))
            for key, target in (
                ("net_return_30bps_percent", net_30),
                ("net_return_50bps_percent", net_50),
            ):
                try:
                    value = float(outcome[key])
                except (KeyError, TypeError, ValueError, OverflowError):
                    continue
                if math.isfinite(value):
                    target.append(value)
        sample_gate_passed = (
            completed >= 500
            and support_events >= 150
            and resistance_events >= 150
            and len(volatility_regimes) >= 2
        )
        return {
            **values,
            "completed_events": completed,
            "reaction_rate_percent": (
                round(values["reaction_events"] / completed * 100.0, 2)
                if completed
                else None
            ),
            "support_events": support_events,
            "resistance_events": resistance_events,
            "volatility_regimes": sorted(volatility_regimes),
            "probability_scored_events": probability_scored_events,
            "probability_validation_status": (
                "READY"
                if probability_scored_events == completed and completed
                else "NOT_STARTED"
            ),
            "average_net_return_30bps_percent": (
                round(sum(net_30) / len(net_30), 6) if net_30 else None
            ),
            "average_net_return_50bps_percent": (
                round(sum(net_50) / len(net_50), 6) if net_50 else None
            ),
            "sample_gate_passed": sample_gate_passed,
            "promotion_evaluable": sample_gate_passed
            and probability_scored_events == completed,
            "first_event_at": iso_utc(records[0][1]) if records else None,
            "last_event_at": iso_utc(records[-1][1]) if records else None,
        }

    def btc_monthly_research_history(
        self,
        monitor_id: str,
    ) -> StoredBtcMonthlyResearchHistory | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT monitor_id, started_at, processed_through_at,
                       algorithm_version, last_run_id
                FROM btc_monthly_research_history_state
                WHERE monitor_id = ?
                """,
                (monitor_id,),
            ).fetchone()
        if row is None:
            return None
        return StoredBtcMonthlyResearchHistory(
            monitor_id=str(row["monitor_id"]),
            started_at=parse_utc(str(row["started_at"])),
            processed_through_at=parse_utc(str(row["processed_through_at"])),
            algorithm_version=str(row["algorithm_version"]),
            last_run_id=int(row["last_run_id"]),
        )

    def latest_btc_monthly_research_revisions(
        self,
        monitor_id: str,
        *,
        limit: int = DEFAULT_BTC_MONTHLY_MAX_SIGNALS,
        algorithm_version: str | None = None,
    ) -> tuple[StoredBtcMonthlyResearchRevision, ...]:
        if not 1 <= limit <= DEFAULT_BTC_MONTHLY_MAX_SIGNALS:
            raise ValueError("limit outside BTC monthly research bound")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT revision_id, revision_no, monitor_id, signal_key,
                       signal_at, observed_at, state, payload_sha256,
                       payload_json, source_run_id, created_at
                FROM btc_monthly_research_revision AS revision
                WHERE revision.monitor_id = ?
                  AND revision.revision_no = (
                      SELECT MAX(candidate.revision_no)
                      FROM btc_monthly_research_revision AS candidate
                      WHERE candidate.monitor_id = revision.monitor_id
                        AND candidate.signal_key = revision.signal_key
                  )
                ORDER BY signal_at DESC, revision_id DESC
                LIMIT ?
                """,
                (monitor_id, DEFAULT_BTC_MONTHLY_MAX_SIGNALS),
            ).fetchall()
        revisions = tuple(
            self._btc_monthly_research_revision_from_row(row) for row in rows
        )
        if algorithm_version is not None:
            revisions = tuple(
                revision
                for revision in revisions
                if str(revision.payload.get("algorithm_version")) == algorithm_version
            )
        return revisions[:limit]

    def pending_btc_monthly_research_revisions(
        self,
        monitor_id: str,
        *,
        algorithm_version: str,
    ) -> tuple[StoredBtcMonthlyResearchRevision, ...]:
        revisions = self.latest_btc_monthly_research_revisions(
            monitor_id,
            algorithm_version=algorithm_version,
        )
        return tuple(
            revision
            for revision in reversed(revisions)
            if revision.state == "SIGNAL_FROZEN"
        )

    def btc_monthly_research_summary(
        self,
        monitor_id: str,
        *,
        algorithm_version: str,
    ) -> dict[str, Any]:
        revisions = tuple(
            reversed(
                self.latest_btc_monthly_research_revisions(
                    monitor_id,
                    algorithm_version=algorithm_version,
                )
            )
        )
        executed: list[tuple[datetime, int, float]] = []
        for revision in revisions:
            signal = revision.payload.get("signal")
            if not isinstance(signal, dict):
                continue
            try:
                target = int(signal["official_target"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            execution = revision.payload.get("execution")
            if revision.state != "EXECUTION_CAPTURED" or not isinstance(
                execution, dict
            ):
                continue
            try:
                execution_at = parse_utc(str(execution["execution_at"]))
                execution_price = float(execution["price"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(execution_price) and execution_price > 0:
                executed.append((execution_at, target, execution_price))
        executed_targets = [target for _, target, _ in executed]
        target_switches = sum(
            current != prior
            for prior, current in zip(
                executed_targets,
                executed_targets[1:],
                strict=False,
            )
        )
        complete_cycles = 0
        cash_seen = False
        cycle_open = False
        for target in executed_targets:
            if target == 0:
                if cycle_open:
                    complete_cycles += 1
                    cycle_open = False
                cash_seen = True
            elif target == 1 and cash_seen and not cycle_open:
                cycle_open = True
        base_growth: float | None = None
        stress_growth: float | None = None
        buy_hold_growth: float | None = None
        checkpoint_max_drawdown: float | None = None
        worst_complete_cycle: float | None = None
        if len(executed) >= 2:
            base_growth = 1.0
            stress_growth = 1.0
            base_points = [base_growth]
            for index in range(1, len(executed)):
                _, prior_target, prior_price = executed[index - 1]
                _, current_target, current_price = executed[index]
                if prior_target == 1:
                    interval_growth = current_price / prior_price
                    base_growth *= interval_growth
                    stress_growth *= interval_growth
                if current_target != prior_target:
                    base_growth *= 1.0 - 15 / 10_000.0
                    stress_growth *= 1.0 - 30 / 10_000.0
                base_points.append(base_growth)
            buy_hold_growth = executed[-1][2] / executed[0][2]
            peak = base_points[0]
            drawdowns: list[float] = []
            for point in base_points:
                peak = max(peak, point)
                drawdowns.append(point / peak - 1.0)
            checkpoint_max_drawdown = min(drawdowns)
            cycle_returns: list[float] = []
            cash_seen = False
            cycle_start: float | None = None
            for index, target in enumerate(executed_targets):
                if target == 0:
                    if cycle_start is not None:
                        cycle_returns.append(base_points[index] / cycle_start - 1.0)
                        cycle_start = None
                    cash_seen = True
                elif cash_seen and cycle_start is None:
                    cycle_start = base_points[index]
            if cycle_returns:
                worst_complete_cycle = min(cycle_returns)
        return {
            "signal_count": len(revisions),
            "execution_count": len(executed),
            "pending_execution_count": sum(
                revision.state == "SIGNAL_FROZEN" for revision in revisions
            ),
            "completed_months": max(0, len(executed) - 1),
            "target_switches": target_switches,
            "complete_long_cash_cycles": complete_cycles,
            "base_cost_bps": 15,
            "stress_cost_bps": 30,
            "base_net_growth": round(base_growth, 8)
            if base_growth is not None
            else None,
            "stress_net_growth": round(stress_growth, 8)
            if stress_growth is not None
            else None,
            "buy_hold_growth": round(buy_hold_growth, 8)
            if buy_hold_growth is not None
            else None,
            "base_relative_to_buy_hold_percent": (
                round((base_growth / buy_hold_growth - 1.0) * 100.0, 6)
                if base_growth is not None
                and buy_hold_growth is not None
                and buy_hold_growth > 0
                else None
            ),
            "checkpoint_max_drawdown_percent": (
                round(checkpoint_max_drawdown * 100.0, 6)
                if checkpoint_max_drawdown is not None
                else None
            ),
            "worst_complete_cycle_percent": (
                round(worst_complete_cycle * 100.0, 6)
                if worst_complete_cycle is not None
                else None
            ),
            "performance_measurement_frequency": "MONTHLY_EXECUTION_CHECKPOINTS",
            "cost_model_status": "FIXED_RESEARCH_PROXY_ONLY",
            "performance_starts_at": iso_utc(executed[0][0]) if executed else None,
            "performance_ends_at": iso_utc(executed[-1][0]) if executed else None,
        }

    def latest_market_event_revisions(
        self,
        monitor_id: str,
        *,
        limit: int = 1000,
    ) -> tuple[StoredMarketEventRevision, ...]:
        if not 1 <= limit <= 20_000:
            raise ValueError("limit must be between 1 and 20000")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT revision_id, revision_no, monitor_id, event_key,
                       scheduled_at, observed_at, state, payload_sha256,
                       payload_json, source_run_id, created_at
                FROM market_event_revision AS revision
                WHERE revision.monitor_id = ?
                  AND revision.revision_no = (
                      SELECT MAX(candidate.revision_no)
                      FROM market_event_revision AS candidate
                      WHERE candidate.monitor_id = revision.monitor_id
                        AND candidate.event_key = revision.event_key
                  )
                ORDER BY scheduled_at DESC, revision_id DESC
                LIMIT ?
                """,
                (monitor_id, limit),
            ).fetchall()
        return tuple(self._market_event_revision_from_row(row) for row in rows)

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> StoredRun:
        return StoredRun(
            run_id=int(row["run_id"]),
            monitor_id=str(row["monitor_id"]),
            started_at=parse_utc(str(row["started_at"])),
            completed_at=(
                parse_utc(str(row["completed_at"]))
                if row["completed_at"] is not None
                else None
            ),
            status=str(row["status"]),  # type: ignore[arg-type]
            sample_count=int(row["sample_count"]),
            error_code=(
                str(row["error_code"]) if row["error_code"] is not None else None
            ),
        )

    @staticmethod
    def _sample_from_row(row: sqlite3.Row) -> StoredSample:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("MONITOR_SAMPLE_PAYLOAD_INVALID")
        return StoredSample(
            sample_id=int(row["sample_id"]),
            run_id=int(row["run_id"]),
            monitor_id=str(row["monitor_id"]),
            series_key=str(row["series_key"]),
            entity_key=str(row["entity_key"]),
            observed_at=parse_utc(str(row["observed_at"])),
            value_text=str(row["value_text"]),
            unit=str(row["unit"]),
            payload=payload,
        )

    @staticmethod
    def _evaluation_from_row(row: sqlite3.Row) -> StoredForwardEvaluation:
        def optional_time(key: str) -> datetime | None:
            return parse_utc(str(row[key])) if row[key] is not None else None

        def optional_float(key: str) -> float | None:
            return float(row[key]) if row[key] is not None else None

        return StoredForwardEvaluation(
            evaluation_id=int(row["evaluation_id"]),
            source_run_id=int(row["source_run_id"]),
            resolved_run_id=(
                int(row["resolved_run_id"])
                if row["resolved_run_id"] is not None
                else None
            ),
            monitor_id=str(row["monitor_id"]),
            case_key=str(row["case_key"]),
            entity_key=str(row["entity_key"]),
            stage=str(row["stage"]),
            stage_label=str(row["stage_label"]),
            direction=str(row["direction"]),
            signal_observed_at=parse_utc(str(row["signal_observed_at"])),
            source_cutoff_at=parse_utc(str(row["source_cutoff_at"])),
            horizon_minutes=int(row["horizon_minutes"]),
            due_at=parse_utc(str(row["due_at"])),
            entry_price_text=str(row["entry_price_text"]),
            benchmark_entry_price_text=str(row["benchmark_entry_price_text"]),
            source=str(row["source"]),
            status=str(row["status"]),
            evaluated_at=optional_time("evaluated_at"),
            outcome_cutoff_at=optional_time("outcome_cutoff_at"),
            exit_price_text=(
                str(row["exit_price_text"])
                if row["exit_price_text"] is not None
                else None
            ),
            benchmark_exit_price_text=(
                str(row["benchmark_exit_price_text"])
                if row["benchmark_exit_price_text"] is not None
                else None
            ),
            forward_return_percent=optional_float("forward_return_percent"),
            benchmark_return_percent=optional_float("benchmark_return_percent"),
            relative_return_percent=optional_float("relative_return_percent"),
            maximum_favorable_excursion_percent=optional_float(
                "maximum_favorable_excursion_percent"
            ),
            maximum_adverse_excursion_percent=optional_float(
                "maximum_adverse_excursion_percent"
            ),
            verdict=(str(row["verdict"]) if row["verdict"] is not None else None),
            reason_code=(
                str(row["reason_code"]) if row["reason_code"] is not None else None
            ),
        )

    @staticmethod
    def _issue_from_row(row: sqlite3.Row) -> StoredIssue:
        try:
            context = json.loads(str(row["context_json"]))
        except (json.JSONDecodeError, TypeError):
            context = {}
        try:
            context = json.loads(_issue_context_json(context))
        except RuntimeError:
            context = {}
        return StoredIssue(
            issue_id=int(row["issue_id"]),
            run_id=int(row["run_id"]),
            monitor_id=str(row["monitor_id"]),
            occurred_at=parse_utc(str(row["occurred_at"])),
            scope=str(row["scope"]),
            reason_code=str(row["reason_code"]),
            context=context,
        )

    @staticmethod
    def _market_event_revision_from_row(
        row: sqlite3.Row,
    ) -> StoredMarketEventRevision:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("MARKET_EVENT_REVISION_PAYLOAD_INVALID")
        return StoredMarketEventRevision(
            revision_id=int(row["revision_id"]),
            revision_no=int(row["revision_no"]),
            monitor_id=str(row["monitor_id"]),
            event_key=str(row["event_key"]),
            scheduled_at=parse_utc(str(row["scheduled_at"])),
            observed_at=parse_utc(str(row["observed_at"])),
            state=str(row["state"]),
            payload_sha256=str(row["payload_sha256"]),
            payload=payload,
            source_run_id=int(row["source_run_id"]),
            created_at=parse_utc(str(row["created_at"])),
        )

    @staticmethod
    def _btc_structure_revision_from_row(
        row: sqlite3.Row,
    ) -> StoredBtcStructureEventRevision:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("BTC_STRUCTURE_EVENT_PAYLOAD_INVALID")
        return StoredBtcStructureEventRevision(
            revision_id=int(row["revision_id"]),
            revision_no=int(row["revision_no"]),
            monitor_id=str(row["monitor_id"]),
            event_key=str(row["event_key"]),
            event_at=parse_utc(str(row["event_at"])),
            observed_at=parse_utc(str(row["observed_at"])),
            state=str(row["state"]),
            payload_sha256=str(row["payload_sha256"]),
            payload=payload,
            source_run_id=int(row["source_run_id"]),
            created_at=parse_utc(str(row["created_at"])),
        )

    @staticmethod
    def _btc_monthly_research_revision_from_row(
        row: sqlite3.Row,
    ) -> StoredBtcMonthlyResearchRevision:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("BTC_MONTHLY_RESEARCH_PAYLOAD_INVALID")
        return StoredBtcMonthlyResearchRevision(
            revision_id=int(row["revision_id"]),
            revision_no=int(row["revision_no"]),
            monitor_id=str(row["monitor_id"]),
            signal_key=str(row["signal_key"]),
            signal_at=parse_utc(str(row["signal_at"])),
            observed_at=parse_utc(str(row["observed_at"])),
            state=str(row["state"]),
            payload_sha256=str(row["payload_sha256"]),
            payload=payload,
            source_run_id=int(row["source_run_id"]),
            created_at=parse_utc(str(row["created_at"])),
        )

    @staticmethod
    def _buyback_review_from_row(row: sqlite3.Row) -> StoredBuybackReview:
        return StoredBuybackReview(
            review_id=int(row["review_id"]),
            monitor_id=str(row["monitor_id"]),
            entity_key=str(row["entity_key"]),
            base_revision_no=int(row["base_revision_no"]),
            decision=str(row["decision"]),
            corrected_event_type=(
                str(row["corrected_event_type"])
                if row["corrected_event_type"] is not None
                else None
            ),
            program_key=(
                str(row["program_key"]) if row["program_key"] is not None else None
            ),
            program_status=(
                str(row["program_status"])
                if row["program_status"] is not None
                else None
            ),
            note=str(row["note"]),
            created_at=parse_utc(str(row["created_at"])),
        )

    @classmethod
    def _buyback_entity_from_row(cls, row: sqlite3.Row) -> StoredBuybackEntity:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("BUYBACK_ENTITY_PAYLOAD_INVALID")
        review = None
        if row["latest_review_id"] is not None:
            review = StoredBuybackReview(
                review_id=int(row["latest_review_id"]),
                monitor_id=str(row["monitor_id"]),
                entity_key=str(row["entity_key"]),
                base_revision_no=int(row["latest_review_base_revision_no"]),
                decision=str(row["latest_review_decision"]),
                corrected_event_type=(
                    str(row["latest_review_event_type"])
                    if row["latest_review_event_type"] is not None
                    else None
                ),
                program_key=(
                    str(row["latest_review_program_key"])
                    if row["latest_review_program_key"] is not None
                    else None
                ),
                program_status=(
                    str(row["latest_review_program_status"])
                    if row["latest_review_program_status"] is not None
                    else None
                ),
                note=str(row["latest_review_note"]),
                created_at=parse_utc(str(row["latest_review_created_at"])),
            )
        return StoredBuybackEntity(
            revision_id=int(row["revision_id"]),
            revision_no=int(row["revision_no"]),
            monitor_id=str(row["monitor_id"]),
            entity_key=str(row["entity_key"]),
            entity_type=str(row["entity_type"]),
            effective_at=parse_utc(str(row["effective_at"])),
            observed_at=parse_utc(str(row["observed_at"])),
            source_key=str(row["source_key"]),
            document_sha256=(
                str(row["document_sha256"])
                if row["document_sha256"] is not None
                else None
            ),
            payload_sha256=str(row["payload_sha256"]),
            payload=payload,
            review=review,
        )

    @staticmethod
    def _buyback_source_from_row(row: sqlite3.Row) -> StoredBuybackSourceState:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise RuntimeError("BUYBACK_SOURCE_PAYLOAD_INVALID")
        return StoredBuybackSourceState(
            monitor_id=str(row["monitor_id"]),
            source_key=str(row["source_key"]),
            source_label=str(row["source_label"]),
            status=str(row["status"]),
            checked_at=parse_utc(str(row["checked_at"])),
            source_time=(
                parse_utc(str(row["source_time"]))
                if row["source_time"] is not None
                else None
            ),
            next_due_at=parse_utc(str(row["next_due_at"])),
            record_count=(
                int(row["record_count"]) if row["record_count"] is not None else None
            ),
            detail_code=(
                str(row["detail_code"]) if row["detail_code"] is not None else None
            ),
            payload=payload,
            last_run_id=int(row["last_run_id"]),
        )

    @staticmethod
    def _buyback_document_from_row(row: sqlite3.Row) -> StoredBuybackDocument:
        metadata = json.loads(str(row["metadata_json"]))
        if not isinstance(metadata, dict):
            raise RuntimeError("BUYBACK_DOCUMENT_METADATA_INVALID")
        return StoredBuybackDocument(
            sha256=str(row["sha256"]),
            monitor_id=str(row["monitor_id"]),
            source_key=str(row["source_key"]),
            source_label=str(row["source_label"]),
            source_document_id=str(row["source_document_id"]),
            source_url=str(row["source_url"]),
            published_at=(
                parse_utc(str(row["published_at"]))
                if row["published_at"] is not None
                else None
            ),
            observed_at=parse_utc(str(row["observed_at"])),
            media_type=str(row["media_type"]),
            size_bytes=int(row["size_bytes"]),
            relative_path=str(row["relative_path"]),
            quality_state=str(row["quality_state"]),
            metadata=metadata,
            last_referenced_at=parse_utc(str(row["last_referenced_at"])),
        )

    @staticmethod
    def _control_from_row(row: sqlite3.Row) -> StoredControl:
        return StoredControl(
            monitor_id=str(row["monitor_id"]),
            enabled=bool(row["enabled"]),
            updated_at=parse_utc(str(row["updated_at"])),
        )
