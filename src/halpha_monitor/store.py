"""Durable SQLite storage for monitor runs, samples, and collection issues."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from halpha_monitor.contracts import CollectionBatch


RunStatus = Literal["RUNNING", "SUCCESS", "PARTIAL", "FAILED"]
SCHEMA_VERSION = 4


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
        return tuple(
            StoredIssue(
                issue_id=int(row["issue_id"]),
                run_id=int(row["run_id"]),
                monitor_id=str(row["monitor_id"]),
                occurred_at=parse_utc(str(row["occurred_at"])),
                scope=str(row["scope"]),
                reason_code=str(row["reason_code"]),
            )
            for row in rows
        )

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
    def _control_from_row(row: sqlite3.Row) -> StoredControl:
        return StoredControl(
            monitor_id=str(row["monitor_id"]),
            enabled=bool(row["enabled"]),
            updated_at=parse_utc(str(row["updated_at"])),
        )
