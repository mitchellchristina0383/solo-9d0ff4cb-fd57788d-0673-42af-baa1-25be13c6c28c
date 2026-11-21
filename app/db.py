"""SQLite persistence for the render-farm scheduler.

The database is intentionally small: one row per node and one row per job.
A job owns its whole lease (all slots on one node), so allocations do not
need a separate table.  Only jobs in the ``running`` state consume capacity.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id       TEXT PRIMARY KEY,
    capacity      INTEGER NOT NULL CHECK (capacity > 0),
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    seq              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id           TEXT NOT NULL UNIQUE,
    required_slots   INTEGER NOT NULL CHECK (required_slots > 0),
    max_duration_us  INTEGER NOT NULL CHECK (max_duration_us > 0),
    status           TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'expired')),
    submitted_at_us  INTEGER NOT NULL,
    assigned_node    TEXT REFERENCES nodes(node_id),
    started_at_us    INTEGER,
    expires_at_us    INTEGER,
    ended_at_us      INTEGER,
    CHECK (
        status IN ('queued', 'completed', 'expired')
        OR (assigned_node IS NOT NULL AND started_at_us IS NOT NULL AND expires_at_us IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_seq
    ON jobs (status, seq);
CREATE INDEX IF NOT EXISTS idx_jobs_node_status
    ON jobs (assigned_node, status);
CREATE INDEX IF NOT EXISTS idx_jobs_status_expiry
    ON jobs (status, expires_at_us);

-- Defence in depth: even if a future code path (or a manual statement)
-- tries to overbook a node, SQLite rejects it.  The scheduler's in-memory
-- accounting is still the source of allocation decisions; these triggers
-- only enforce the invariant at commit time.
CREATE TRIGGER IF NOT EXISTS jobs_no_oversubscribe_insert
BEFORE INSERT ON jobs
WHEN NEW.status = 'running' AND NEW.assigned_node IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'node capacity exceeded')
    WHERE (
        SELECT COALESCE(SUM(required_slots), 0)
        FROM jobs
        WHERE status = 'running' AND assigned_node = NEW.assigned_node
    ) + NEW.required_slots > (
        SELECT capacity FROM nodes WHERE node_id = NEW.assigned_node
    );
END;

CREATE TRIGGER IF NOT EXISTS jobs_no_oversubscribe_update
BEFORE UPDATE OF status, required_slots, assigned_node ON jobs
WHEN NEW.status = 'running' AND NEW.assigned_node IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'node capacity exceeded')
    WHERE (
        SELECT COALESCE(SUM(required_slots), 0)
        FROM jobs
        WHERE status = 'running'
          AND assigned_node = NEW.assigned_node
          AND seq <> NEW.seq
    ) + NEW.required_slots > (
        SELECT capacity FROM nodes WHERE node_id = NEW.assigned_node
    );
END;

CREATE TRIGGER IF NOT EXISTS nodes_capacity_not_below_usage
BEFORE UPDATE OF capacity ON nodes
WHEN NEW.capacity < (
    SELECT COALESCE(SUM(required_slots), 0)
    FROM jobs
    WHERE status = 'running' AND assigned_node = NEW.node_id
)
BEGIN
    SELECT RAISE(ABORT, 'node capacity below current usage');
END;
"""


class Store:
    """Small SQLite store using one short-lived connection per transaction.

    ``BEGIN IMMEDIATE`` plus a process-wide lock (held by the API layer) makes
    read-modify-write scheduling decisions atomic both inside one process and
    across processes sharing the same database file.
    """

    def __init__(self, path: str | os.PathLike[str]):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = threading.Lock()
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def initialize(self) -> None:
        with self._init_lock:
            last_error: Exception | None = None
            for attempt in range(5):
                conn = self._connect()
                try:
                    try:
                        conn.execute("PRAGMA journal_mode = WAL")
                    except sqlite3.OperationalError as exc:
                        # Another process may hold the write lock while it is
                        # switching the journal mode; WAL is persistent, so a
                        # failed switch here is harmless as long as the schema
                        # is applied.
                        logger.warning("could not set WAL mode: %s", exc)
                    conn.executescript(SCHEMA)
                    return
                except sqlite3.OperationalError as exc:  # pragma: no cover
                    last_error = exc
                    remaining = 5 - attempt - 1
                    if remaining:
                        time.sleep(0.05 * (attempt + 1))
                finally:
                    conn.close()
            raise RuntimeError(
                f"could not initialise database {self.path!r}"
            ) from last_error

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
        finally:
            conn.close()

    def ping(self) -> None:
        conn = self._connect()
        try:
            conn.execute("SELECT 1").fetchone()
        finally:
            conn.close()
