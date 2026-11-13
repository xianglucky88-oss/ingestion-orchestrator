"""SQLite persistence for run state, attempts, events and usage.

The store is a passive sink: the scheduler reports every transition and the
store records it.  Three views are kept consistent for verification:

* ``task_runs``   – latest state per task (terminal status, attempt count,
  resource group and, when blocked, the explicit blocked reason);
* ``task_attempts`` – one row per actual attempt (running → succeeded /
  failed / cancelled);
* ``task_events`` – the full ordered trace (ready, started, attempt finished,
  back-off, retry ready, blocked, cancelled, completed, failed) plus periodic
  resource-usage snapshots in ``run_usage``.

Old databases created by the previous version (``task_runs(task_id, status,
attempts, mode)`` and ``task_attempts(task_id, attempt, status, mode)``) are
migrated in place by adding the new columns.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import RunConfig, TaskRun


class RunStore:
    """Owns all SQLite reads/writes for a run."""

    def __init__(self, database: str):
        self.database = database
        self.conn = sqlite3.connect(database)
        self.conn.row_factory = sqlite3.Row
        self.run_id: int | None = None
        self._migrate()

    # -- schema ----------------------------------------------------------
    def _migrate(self) -> None:
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS task_runs("
            "task_id TEXT PRIMARY KEY, status TEXT, attempts INTEGER, mode TEXT, "
            "resource_group TEXT, blocked_reason TEXT, started_at REAL, finished_at REAL)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS task_attempts("
            "task_id TEXT NOT NULL, attempt INTEGER NOT NULL, status TEXT NOT NULL, "
            "mode TEXT NOT NULL, resource_group TEXT, started_at REAL, finished_at REAL, detail TEXT, "
            "PRIMARY KEY(task_id, attempt))"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS task_events("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, task_id TEXT, kind TEXT NOT NULL, "
            "status TEXT, attempt INTEGER, resource_group TEXT, blocked_reason TEXT, detail TEXT, at REAL)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS run_usage("
            "run_id INTEGER PRIMARY KEY, max_total INTEGER NOT NULL DEFAULT 0, "
            "max_groups TEXT NOT NULL DEFAULT '{}', updated_at REAL)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS runs("
            "run_id INTEGER PRIMARY KEY AUTOINCREMENT, config TEXT, started_at REAL, "
            "finished_at REAL, summary TEXT)"
        )
        self._ensure_columns("task_runs", {
            "resource_group": "TEXT",
            "blocked_reason": "TEXT",
            "started_at": "REAL",
            "finished_at": "REAL",
        })
        self._ensure_columns("task_attempts", {
            "resource_group": "TEXT",
            "started_at": "REAL",
            "finished_at": "REAL",
            "detail": "TEXT",
        })
        self.conn.commit()

    def _ensure_columns(self, table: str, columns: Mapping[str, str]) -> None:
        existing = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        for name, kind in columns.items():
            if name not in existing:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}")

    # -- run lifecycle ---------------------------------------------------
    def start_run(self, config: RunConfig, started_at: float, cancel_plan: Mapping[str, float] | None = None) -> int:
        payload = {
            "max_parallel": config.max_parallel,
            "group_limits": dict(config.group_limits),
            "default_group_limit": config.default_group_limit,
            "retry_backoff": config.retry_backoff,
            "backoff_multiplier": config.backoff_multiplier,
            "aging_step": config.aging_step,
            "cancellations": dict(config.cancellations),
        }
        cursor = self.conn.execute(
            "INSERT INTO runs(config, started_at) VALUES (?, ?)",
            (json.dumps(payload, sort_keys=True), started_at),
        )
        self.run_id = int(cursor.lastrowid)
        self.conn.execute(
            "INSERT INTO run_usage(run_id, max_total, max_groups, updated_at) VALUES (?, 0, '{}', ?)",
            (self.run_id, started_at),
        )
        self.conn.commit()
        del cancel_plan
        return self.run_id

    def finish_run(self, summary: Mapping[str, int], finished_at: float) -> None:
        if self.run_id is None:
            return
        self.conn.execute(
            "UPDATE runs SET finished_at=?, summary=? WHERE run_id=?",
            (finished_at, json.dumps(dict(summary), sort_keys=True), self.run_id),
        )
        self.conn.commit()

    # -- event sink ------------------------------------------------------
    def apply_event(self, event: Mapping[str, Any]) -> None:
        kind = str(event.get("kind", ""))
        at = event.get("at")
        task_id = event.get("task_id")
        if kind == "usage":
            self._record_usage(event)
            return
        self.conn.execute(
            "INSERT INTO task_events(run_id, task_id, kind, status, attempt, resource_group, "
            "blocked_reason, detail, at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self.run_id,
                task_id,
                kind,
                event.get("status"),
                event.get("attempt"),
                event.get("resource_group"),
                event.get("blocked_reason"),
                event.get("detail"),
                at,
            ),
        )
        if kind == "started":
            self._upsert_attempt_start(event)
            self._upsert_task_row(
                task_id,
                status="running",
                attempts=event.get("attempt") or 0,
                mode=event.get("mode"),
                resource_group=event.get("resource_group"),
                blocked_reason=None,
                started_at=at,
            )
        elif kind == "attempt_finished":
            self._upsert_attempt_finish(event)
        elif kind in ("ready", "retry_ready", "waiting", "backoff", "cancel_requested", "attempt_failed"):
            self._upsert_task_row(
                task_id,
                status=event.get("status"),
                attempts=event.get("attempts"),
                mode=event.get("mode"),
                resource_group=event.get("resource_group"),
                blocked_reason=event.get("blocked_reason"),
            )
        elif kind in ("completed", "failed", "cancelled", "blocked"):
            self._upsert_task_row(
                task_id,
                status=event.get("status") or kind,
                attempts=event.get("attempts"),
                mode=event.get("mode"),
                resource_group=event.get("resource_group"),
                blocked_reason=event.get("blocked_reason") if kind == "blocked" else None,
                finished_at=at,
            )
        self.conn.commit()

    def _upsert_attempt_start(self, event: Mapping[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO task_attempts(task_id, attempt, status, mode, resource_group, started_at) "
            "VALUES (?, ?, 'running', ?, ?, ?) "
            "ON CONFLICT(task_id, attempt) DO UPDATE SET status='running', "
            "mode=excluded.mode, resource_group=excluded.resource_group, started_at=excluded.started_at, "
            "finished_at=NULL, detail=NULL",
            (
                event.get("task_id"),
                event.get("attempt"),
                event.get("mode"),
                event.get("resource_group"),
                event.get("at"),
            ),
        )

    def _upsert_attempt_finish(self, event: Mapping[str, Any]) -> None:
        if event.get("cancelled"):
            status = "cancelled"
        elif event.get("succeeded"):
            status = "succeeded"
        else:
            status = "failed"
        self.conn.execute(
            "UPDATE task_attempts SET status=?, finished_at=?, detail=? "
            "WHERE task_id=? AND attempt=?",
            (
                status,
                event.get("at"),
                event.get("detail"),
                event.get("task_id"),
                event.get("attempt"),
            ),
        )

    def _upsert_task_row(
        self,
        task_id: str,
        *,
        status: str | None,
        attempts: int | None,
        mode: str | None,
        resource_group: str | None,
        blocked_reason: str | None,
        started_at: float | None = None,
        finished_at: float | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT INTO task_runs(task_id, status, attempts, mode, resource_group, blocked_reason, "
            "started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(task_id) DO UPDATE SET "
            "status=COALESCE(excluded.status, task_runs.status), "
            "attempts=COALESCE(excluded.attempts, task_runs.attempts), "
            "mode=COALESCE(excluded.mode, task_runs.mode), "
            "resource_group=COALESCE(excluded.resource_group, task_runs.resource_group), "
            "blocked_reason=excluded.blocked_reason, "
            "started_at=COALESCE(excluded.started_at, task_runs.started_at), "
            "finished_at=COALESCE(excluded.finished_at, task_runs.finished_at)",
            (task_id, status, attempts, mode, resource_group, blocked_reason, started_at, finished_at),
        )

    def _record_usage(self, event: Mapping[str, Any]) -> None:
        if self.run_id is None:
            return
        total = int(event.get("max_total", event.get("total", 0)))
        groups = dict(event.get("max_groups", event.get("groups", {})))
        row = self.conn.execute(
            "SELECT max_total, max_groups FROM run_usage WHERE run_id=?", (self.run_id,)
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO run_usage(run_id, max_total, max_groups, updated_at) VALUES (?, ?, ?, ?)",
                (self.run_id, total, json.dumps(groups), event.get("at")),
            )
        else:
            old_groups = json.loads(row["max_groups"] or "{}")
            merged = {key: max(value, old_groups.get(key, 0)) for key, value in groups.items()}
            for key, value in old_groups.items():
                merged.setdefault(key, value)
            self.conn.execute(
                "UPDATE run_usage SET max_total=?, max_groups=?, updated_at=? WHERE run_id=?",
                (
                    max(int(row["max_total"]), total),
                    json.dumps(merged),
                    event.get("at"),
                    self.run_id,
                ),
            )
        self.conn.commit()

    # -- compatibility ---------------------------------------------------
    def save(self, run: TaskRun) -> None:
        """Final-state upsert (legacy API, also used to persist terminal rows)."""
        self.conn.execute(
            "INSERT INTO task_runs(task_id, status, attempts, mode, resource_group, blocked_reason, "
            "started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(task_id) DO UPDATE SET status=excluded.status, attempts=excluded.attempts, "
            "mode=excluded.mode, resource_group=excluded.resource_group, "
            "blocked_reason=excluded.blocked_reason, started_at=excluded.started_at, "
            "finished_at=excluded.finished_at",
            (
                run.task_id,
                run.status,
                run.attempts,
                run.mode,
                run.resource_group,
                run.blocked_reason,
                run.started_at,
                run.finished_at,
            ),
        )
        self.conn.commit()

    # -- queries used by tests / operators --------------------------------
    def task_run(self, task_id: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM task_runs WHERE task_id=?", (task_id,)).fetchone()
        return dict(row) if row else None

    def task_runs(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute("SELECT * FROM task_runs ORDER BY task_id")]

    def attempts(self, task_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM task_attempts WHERE task_id=? ORDER BY attempt", (task_id,)
        )
        return [dict(row) for row in rows]

    def events(self, task_id: str | None = None, kinds: Iterable[str] | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM task_events"
        clauses: list[str] = []
        params: list[Any] = []
        if task_id is not None:
            clauses.append("task_id=?")
            params.append(task_id)
        if kinds is not None:
            kinds = list(kinds)
            placeholders = ",".join("?" for _ in kinds)
            clauses.append(f"kind IN ({placeholders})")
            params.extend(kinds)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id"
        return [dict(row) for row in self.conn.execute(query, params)]

    def blocked_reason(self, task_id: str) -> str | None:
        row = self.task_run(task_id)
        return None if row is None else row.get("blocked_reason")

    def usage_high_watermark(self) -> tuple[int, dict[str, int]]:
        if self.run_id is None:
            return 0, {}
        row = self.conn.execute(
            "SELECT max_total, max_groups FROM run_usage WHERE run_id=?", (self.run_id,)
        ).fetchone()
        if row is None:
            return 0, {}
        return int(row["max_total"]), json.loads(row["max_groups"] or "{}")

    def run_record(self, run_id: int | None = None) -> dict[str, Any] | None:
        run_id = self.run_id if run_id is None else run_id
        if run_id is None:
            return None
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "RunStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def read_only(database: str) -> sqlite3.Connection:
    """Open a read-only connection; helper for inspection scripts."""
    path = Path(database)
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
