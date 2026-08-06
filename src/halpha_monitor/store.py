"""Durable SQLite storage for monitor runs, samples, and collection issues."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from halpha_monitor.contracts import CollectionBatch, ForwardEvaluationCase


RunStatus = Literal["RUNNING", "SUCCESS", "PARTIAL", "FAILED"]
SCHEMA_VERSION = 5


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


class SQLiteMonitorStore:
    """One SQLite database with WAL and short, atomic write transactions."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

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
                    reason_code TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS monitor_issue_latest_idx
                    ON monitor_issue (monitor_id, issue_id DESC);

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
                """
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

    def start_run(self, monitor_id: str, *, started_at: datetime | None = None) -> int:
        started = iso_utc(started_at or utc_now())
        with self._connect() as connection:
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
        status: RunStatus = (
            "FAILED"
            if batch.issues and not batch.samples
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
                        run_id, monitor_id, occurred_at, scope, reason_code
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        monitor_id,
                        completed_text,
                        issue.scope,
                        issue.reason_code,
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

    def fail_run(
        self,
        run_id: int,
        monitor_id: str,
        reason_code: str,
        *,
        completed_at: datetime | None = None,
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
                    run_id, monitor_id, occurred_at, scope, reason_code
                ) VALUES (?, ?, ?, 'monitor', ?)
                """,
                (run_id, monitor_id, completed_text, reason_code),
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
    ) -> tuple[StoredForwardEvaluation, ...]:
        if not entity_keys:
            return ()
        placeholders = ",".join("?" for _ in entity_keys)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM monitor_forward_evaluation AS evaluation
                WHERE evaluation.monitor_id = ?
                  AND evaluation.entity_key IN ({placeholders})
                  AND evaluation.evaluation_id = (
                      SELECT MAX(candidate.evaluation_id)
                      FROM monitor_forward_evaluation AS candidate
                      WHERE candidate.monitor_id = evaluation.monitor_id
                        AND candidate.entity_key = evaluation.entity_key
                  )
                ORDER BY evaluation.entity_key
                """,
                (monitor_id, *entity_keys),
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
                benchmark_entry_price_text=str(
                    row["benchmark_entry_price_text"]
                ),
                source=str(row["source"]),
            )
            for row in rows
        )

    def recent_forward_evaluations(
        self,
        monitor_id: str,
        *,
        limit: int = 120,
    ) -> tuple[StoredForwardEvaluation, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM monitor_forward_evaluation
                WHERE monitor_id = ?
                ORDER BY source_cutoff_at DESC, evaluation_id DESC
                LIMIT ?
                """,
                (monitor_id, limit),
            ).fetchall()
        return tuple(self._evaluation_from_row(row) for row in rows)

    def forward_evaluation_summary(
        self,
        monitor_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]:
        now_text = iso_utc(now)
        with self._connect() as connection:
            counts = connection.execute(
                """
                SELECT
                    COUNT(*) AS total_cases,
                    SUM(CASE WHEN due_at <= ? THEN 1 ELSE 0 END) AS due_cases,
                    SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END)
                        AS completed_cases,
                    SUM(CASE WHEN status = 'UNAVAILABLE' THEN 1 ELSE 0 END)
                        AS unavailable_cases,
                    SUM(CASE WHEN status = 'PENDING' AND due_at <= ? THEN 1 ELSE 0 END)
                        AS pending_due_cases,
                    SUM(CASE WHEN status = 'PENDING' AND due_at > ? THEN 1 ELSE 0 END)
                        AS pending_future_cases
                FROM monitor_forward_evaluation
                WHERE monitor_id = ?
                """,
                (now_text, now_text, now_text, monitor_id),
            ).fetchone()
            groups = connection.execute(
                """
                SELECT stage, stage_label, horizon_minutes,
                       COUNT(*) AS sample_count,
                       SUM(CASE WHEN verdict = 'ALIGNED' THEN 1 ELSE 0 END)
                           AS aligned_count,
                       AVG(relative_return_percent) AS average_relative_return_percent,
                       AVG(maximum_favorable_excursion_percent)
                           AS average_favorable_excursion_percent,
                       AVG(maximum_adverse_excursion_percent)
                           AS average_adverse_excursion_percent
                FROM monitor_forward_evaluation
                WHERE monitor_id = ? AND status = 'COMPLETE'
                GROUP BY stage, stage_label, horizon_minutes
                ORDER BY stage_label, horizon_minutes
                """,
                (monitor_id,),
            ).fetchall()
        count_payload = {
            key: int(counts[key] or 0)
            for key in (
                "total_cases",
                "due_cases",
                "completed_cases",
                "unavailable_cases",
                "pending_due_cases",
                "pending_future_cases",
            )
        }
        return {
            **count_payload,
            "groups": [
                {
                    "stage": str(row["stage"]),
                    "stage_label": str(row["stage_label"]),
                    "horizon_minutes": int(row["horizon_minutes"]),
                    "sample_count": int(row["sample_count"]),
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
                SELECT issue_id, run_id, monitor_id, occurred_at, scope, reason_code
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
                SELECT issue_id, run_id, monitor_id, occurred_at, scope, reason_code
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
                response_completed_at=parse_utc(
                    str(row["response_completed_at"])
                ),
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

    def prune(self, retention_days: int, *, now: datetime | None = None) -> int:
        cutoff = iso_utc((now or utc_now()) - timedelta(days=retention_days))
        with self._connect() as connection:
            cursor = connection.execute(
                """
                DELETE FROM monitor_run
                WHERE completed_at IS NOT NULL AND completed_at < ?
                """,
                (cutoff,),
            )
            return int(cursor.rowcount)

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
            benchmark_return_percent=optional_float(
                "benchmark_return_percent"
            ),
            relative_return_percent=optional_float("relative_return_percent"),
            maximum_favorable_excursion_percent=optional_float(
                "maximum_favorable_excursion_percent"
            ),
            maximum_adverse_excursion_percent=optional_float(
                "maximum_adverse_excursion_percent"
            ),
            verdict=(str(row["verdict"]) if row["verdict"] is not None else None),
            reason_code=(
                str(row["reason_code"])
                if row["reason_code"] is not None
                else None
            ),
        )

    @staticmethod
    def _issue_from_row(row: sqlite3.Row) -> StoredIssue:
        return StoredIssue(
            issue_id=int(row["issue_id"]),
            run_id=int(row["run_id"]),
            monitor_id=str(row["monitor_id"]),
            occurred_at=parse_utc(str(row["occurred_at"])),
            scope=str(row["scope"]),
            reason_code=str(row["reason_code"]),
        )

    @staticmethod
    def _control_from_row(row: sqlite3.Row) -> StoredControl:
        return StoredControl(
            monitor_id=str(row["monitor_id"]),
            enabled=bool(row["enabled"]),
            updated_at=parse_utc(str(row["updated_at"])),
        )
