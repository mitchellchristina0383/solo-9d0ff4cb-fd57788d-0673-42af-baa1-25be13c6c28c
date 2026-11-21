"""Core scheduling logic.

Jobs are queued in submission order.  A job leases all of its slots on a
single node.  Scheduling follows head-of-line reservation:

* expire due leases and release their slots;
* while the queue head can start now, dispatch it;
* once the head is blocked, walk the remaining queue in order and dispatch
  only jobs that can start now *without delaying the head's earliest start*;
* if the head can never fit on any configured node, it remains queued but
  later jobs may still run because no finite start time is being delayed.
  A request is never split across nodes.  Such a head becomes dispatchable
  again if a node's capacity is later increased.

All mutations happen inside ``BEGIN IMMEDIATE`` transactions and are guarded
by a process-wide lock, so planning and allocation cannot interleave.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable

from .db import Store

logger = logging.getLogger(__name__)

US_PER_SECOND = 1_000_000

JOB_STATUSES = ("queued", "running", "completed", "expired")


# ---------------------------------------------------------------------------
# Errors (mapped to HTTP status codes by the API layer)
# ---------------------------------------------------------------------------


class SchedulerError(Exception):
    status_code = 400


class NodeNotFoundError(SchedulerError):
    status_code = 404


class JobNotFoundError(SchedulerError):
    status_code = 404


class DuplicateJobError(SchedulerError):
    status_code = 409


class LeaseNotHeldError(SchedulerError):
    status_code = 409


class CapacityBelowUsageError(SchedulerError):
    status_code = 409


# ---------------------------------------------------------------------------
# Internal records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JobRecord:
    seq: int
    job_id: str
    required_slots: int
    max_duration_us: int
    status: str
    submitted_at_us: int
    assigned_node: str | None
    started_at_us: int | None
    expires_at_us: int | None
    ended_at_us: int | None


def to_iso(timestamp_us: int | None) -> str | None:
    """Render epoch microseconds as an ISO-8601 UTC timestamp."""
    if timestamp_us is None:
        return None
    dt = datetime.fromtimestamp(timestamp_us / US_PER_SECOND, tz=timezone.utc)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _row_to_job(row: sqlite3.Row) -> JobRecord:
    return JobRecord(
        seq=row["seq"],
        job_id=row["job_id"],
        required_slots=row["required_slots"],
        max_duration_us=row["max_duration_us"],
        status=row["status"],
        submitted_at_us=row["submitted_at_us"],
        assigned_node=row["assigned_node"],
        started_at_us=row["started_at_us"],
        expires_at_us=row["expires_at_us"],
        ended_at_us=row["ended_at_us"],
    )


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


class Scheduler:
    def __init__(
        self,
        store: Store,
        clock: Callable[[], float] = time.time,
        tick_seconds: float = 1.0,
    ):
        self.store = store
        self.clock = clock
        self.tick_seconds = tick_seconds
        self._lock = threading.RLock()

    # -- time -------------------------------------------------------------

    def now_us(self) -> int:
        return int(round(self.clock() * US_PER_SECOND))

    # -- commands ---------------------------------------------------------

    def configure_node(self, node_id: str, capacity: int) -> dict:
        with self._lock:
            now = self.now_us()
            with self.store.transaction() as conn:
                used = conn.execute(
                    """
                    SELECT COALESCE(SUM(required_slots), 0) AS used
                    FROM jobs
                    WHERE status = 'running' AND assigned_node = ?
                    """,
                    (node_id,),
                ).fetchone()["used"]
                if capacity < used:
                    raise CapacityBelowUsageError(
                        f"node {node_id!r} has {used} slots leased; "
                        f"capacity cannot be reduced to {capacity}"
                    )
                try:
                    conn.execute(
                        """
                        INSERT INTO nodes (node_id, capacity, created_at_us, updated_at_us)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(node_id) DO UPDATE SET
                            capacity = excluded.capacity,
                            updated_at_us = excluded.updated_at_us
                        """,
                        (node_id, capacity, now, now),
                    )
                except sqlite3.IntegrityError as exc:
                    # The SQL-level guard fired; report it as a conflict
                    # rather than a server error.
                    raise CapacityBelowUsageError(
                        f"node {node_id!r} cannot be reduced to {capacity} "
                        "while leases are held"
                    ) from exc
            self._try_schedule()
            return self.get_node(node_id)

    def submit_job(
        self, job_id: str, required_slots: int, max_duration_seconds: float
    ) -> dict:
        now = self.now_us()
        duration_us = max(1, int(round(max_duration_seconds * US_PER_SECOND)))

        with self._lock:
            with self.store.transaction() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO jobs
                            (job_id, required_slots, max_duration_us, status,
                             submitted_at_us)
                        VALUES (?, ?, ?, 'queued', ?)
                        """,
                        (job_id, required_slots, duration_us, now),
                    )
                except sqlite3.IntegrityError as exc:
                    if "UNIQUE" in str(exc):
                        raise DuplicateJobError(
                            f"job {job_id!r} already exists"
                        ) from exc
                    raise
            # The job is committed as queued even if the immediate dispatch
            # pass fails; the periodic reconciliator retries.
            self._try_schedule()
            return self.get_job(job_id)

    def release_job(self, job_id: str) -> dict:
        """Return a running lease early.

        The release itself is one transaction; the following dispatch pass is
        separate, so a dispatch failure cannot undo the release or leave a
        half-updated queue.
        """
        error: SchedulerError | None = None
        with self._lock:
            with self.store.transaction() as conn:
                self._expire_due(conn, self.now_us(), events=[])
                row = conn.execute(
                    "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
                ).fetchone()
                if row is None:
                    error = JobNotFoundError(f"job {job_id!r} not found")
                elif row["status"] != "running":
                    error = LeaseNotHeldError(
                        f"job {job_id!r} is {row['status']}, not running; "
                        "only an active lease can be returned"
                    )
                else:
                    conn.execute(
                        """
                        UPDATE jobs SET status = 'completed', ended_at_us = ?
                        WHERE seq = ? AND status = 'running'
                        """,
                        (self.now_us(), row["seq"]),
                    )
            # Re-dispatch even when the release request itself was rejected:
            # the expiry sweep inside the transaction may have freed slots.
            events = self._try_schedule()
            if events:
                logger.info("dispatch after release: %s", events)
            if error is not None:
                raise error
            return self.get_job(job_id)

    def tick(self) -> list[dict]:
        """Expire due leases and dispatch everything currently eligible."""
        return self._try_schedule()

    def get_node(self, node_id: str) -> dict:
        with self._lock:
            self._try_schedule()
            with self.store.transaction() as conn:
                row = self._node_rows(conn, node_id)
                if row is None:
                    raise NodeNotFoundError(f"node {node_id!r} not found")
                return row

    def list_nodes(self) -> list[dict]:
        with self._lock:
            self._try_schedule()
            with self.store.transaction() as conn:
                return self._node_rows(conn)

    def get_job(self, job_id: str) -> dict:
        with self._lock:
            self._try_schedule()
            with self.store.transaction() as conn:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
                ).fetchone()
                if row is None:
                    raise JobNotFoundError(f"job {job_id!r} not found")
                positions = self._queue_positions(conn)
                return self._job_dict(_row_to_job(row), positions)

    def list_jobs(self, status: str | None = None, limit: int = 200) -> list[dict]:
        with self._lock:
            self._try_schedule()
            with self.store.transaction() as conn:
                positions = self._queue_positions(conn)
                if status:
                    rows = conn.execute(
                        """
                        SELECT * FROM jobs WHERE status = ?
                        ORDER BY seq LIMIT ?
                        """,
                        (status, limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM jobs ORDER BY seq LIMIT ?", (limit,)
                    ).fetchall()
                return [self._job_dict(_row_to_job(r), positions) for r in rows]

    def head_estimate(self) -> dict:
        with self._lock:
            self._try_schedule()
            with self.store.transaction() as conn:
                return self._head_estimate_in_tx(conn, self.now_us())

    def queue_state(self, limit: int = 200) -> dict:
        with self._lock:
            self._try_schedule()
            with self.store.transaction() as conn:
                estimate = self._head_estimate_in_tx(conn, self.now_us())
                positions = self._queue_positions(conn)
                rows = conn.execute(
                    """
                    SELECT * FROM jobs WHERE status = 'queued'
                    ORDER BY seq LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
                estimate["jobs"] = [
                    self._job_dict(_row_to_job(r), positions) for r in rows
                ]
                return estimate

    # -- scheduling core --------------------------------------------------

    def _try_schedule(self) -> list[dict]:
        with self._lock:
            try:
                with self.store.transaction() as conn:
                    events = self._schedule_in_tx(conn, self.now_us())
                if events:
                    logger.info("scheduler events: %s", events)
                return events
            except Exception:
                # The transaction rolled back, so queue order and occupancy
                # are untouched.  The next tick (or request) retries.
                logger.exception("scheduling pass failed; state left unchanged")
                return []

    def _expire_due(
        self, conn: sqlite3.Connection, now_us: int, events: list[dict]
    ) -> int:
        rows = conn.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'running' AND expires_at_us <= ?
            ORDER BY seq
            """,
            (now_us,),
        ).fetchall()
        for row in rows:
            job = _row_to_job(row)
            conn.execute(
                """
                UPDATE jobs SET status = 'expired', ended_at_us = expires_at_us
                WHERE seq = ? AND status = 'running'
                """,
                (job.seq,),
            )
            events.append(
                {
                    "type": "expired",
                    "job_id": job.job_id,
                    "node_id": job.assigned_node,
                    "slots": job.required_slots,
                    "at": to_iso(job.expires_at_us),
                    "lease_expires_at": None,
                }
            )
        return len(rows)

    def _schedule_in_tx(self, conn: sqlite3.Connection, now_us: int) -> list[dict]:
        events: list[dict] = []
        self._expire_due(conn, now_us, events)

        capacities = {
            row["node_id"]: row["capacity"]
            for row in conn.execute(
                "SELECT node_id, capacity FROM nodes ORDER BY node_id"
            ).fetchall()
        }

        running_by_node: dict[str, list[JobRecord]] = {
            node_id: [] for node_id in capacities
        }
        used_by_node: dict[str, int] = {node_id: 0 for node_id in capacities}
        for row in conn.execute(
            "SELECT * FROM jobs WHERE status = 'running' ORDER BY seq"
        ).fetchall():
            job = _row_to_job(row)
            if job.assigned_node in running_by_node:
                running_by_node[job.assigned_node].append(job)
                used_by_node[job.assigned_node] += job.required_slots
        available = {
            node_id: capacities[node_id] - used_by_node[node_id]
            for node_id in capacities
        }

        queued = [
            _row_to_job(row)
            for row in conn.execute(
                "SELECT * FROM jobs WHERE status = 'queued' ORDER BY seq"
            ).fetchall()
        ]

        while queued:
            head = queued[0]
            head_start = self._earliest_start_us(
                head, capacities, running_by_node, now_us
            )
            node_id = self._best_fit_node(head, available)
            if node_id is not None:
                self._allocate(
                    conn,
                    head,
                    node_id,
                    now_us,
                    running_by_node,
                    available,
                    events,
                )
                queued.pop(0)
                continue

            if head_start is None:
                # No configured node can ever host the head.  There is no
                # finite head start time to protect, so let later jobs that
                # fit right now use otherwise idle slots.  The head keeps
                # queue position 1.
                for candidate in list(queued[1:]):
                    chosen = self._best_fit_node(candidate, available)
                    if chosen is not None:
                        self._allocate(
                            conn,
                            candidate,
                            chosen,
                            now_us,
                            running_by_node,
                            available,
                            events,
                        )
                        queued.remove(candidate)
                break

            # The head is blocked but has a finite earliest start.  Backfill
            # only candidates whose whole lease ends without delaying it.
            for candidate in list(queued[1:]):
                chosen = self._backfill_node(
                    candidate,
                    head,
                    head_start,
                    capacities,
                    running_by_node,
                    available,
                    now_us,
                )
                if chosen is not None:
                    self._allocate(
                        conn,
                        candidate,
                        chosen,
                        now_us,
                        running_by_node,
                        available,
                        events,
                    )
                    queued.remove(candidate)
            break

        return events

    @staticmethod
    def _earliest_start_us(
        job: JobRecord,
        capacities: dict[str, int],
        running_by_node: dict[str, list[JobRecord]],
        now_us: int,
    ) -> int | None:
        """Earliest time ``job`` could start if every lease runs to expiry."""
        best: int | None = None
        for node_id, capacity in capacities.items():
            if capacity < job.required_slots:
                continue
            running = sorted(
                running_by_node.get(node_id, []),
                key=lambda r: (r.expires_at_us or 0, r.seq),
            )
            free = capacity - sum(r.required_slots for r in running)
            if free >= job.required_slots:
                return now_us
            need = job.required_slots - free
            freed = 0
            for running_job in running:
                freed += running_job.required_slots
                if freed >= need:
                    candidate = max(running_job.expires_at_us or now_us, now_us)
                    if best is None or candidate < best:
                        best = candidate
                    break
        return best

    @staticmethod
    def _best_fit_node(job: JobRecord, available: dict[str, int]) -> str | None:
        candidates = [
            (free - job.required_slots, node_id)
            for node_id, free in available.items()
            if free >= job.required_slots
        ]
        if not candidates:
            return None
        return min(candidates)[1]

    def _backfill_node(
        self,
        candidate: JobRecord,
        head: JobRecord,
        head_start_us: int,
        capacities: dict[str, int],
        running_by_node: dict[str, list[JobRecord]],
        available: dict[str, int],
        now_us: int,
    ) -> str | None:
        """Pick a node for ``candidate`` that does not delay the head."""
        fits = sorted(
            (free - candidate.required_slots, node_id)
            for node_id, free in available.items()
            if free >= candidate.required_slots
        )
        for _slack, node_id in fits:
            trial = {n: list(jobs) for n, jobs in running_by_node.items()}
            trial.setdefault(node_id, []).append(
                replace(
                    candidate,
                    status="running",
                    assigned_node=node_id,
                    started_at_us=now_us,
                    expires_at_us=now_us + candidate.max_duration_us,
                )
            )
            new_head_start = self._earliest_start_us(
                head, capacities, trial, now_us
            )
            if new_head_start is not None and new_head_start <= head_start_us:
                return node_id
        return None

    def _allocate(
        self,
        conn: sqlite3.Connection,
        job: JobRecord,
        node_id: str,
        now_us: int,
        running_by_node: dict[str, list[JobRecord]],
        available: dict[str, int],
        events: list[dict],
    ) -> None:
        if available.get(node_id, 0) < job.required_slots:
            raise RuntimeError(
                f"refusing to oversubscribe node {node_id!r} with job "
                f"{job.job_id!r}: {job.required_slots} slots requested, "
                f"{available.get(node_id, 0)} free"
            )
        expires_at = now_us + job.max_duration_us
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = 'running', assigned_node = ?, started_at_us = ?,
                expires_at_us = ?
            WHERE seq = ? AND status = 'queued'
            """,
            (node_id, now_us, expires_at, job.seq),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(
                f"job {job.job_id!r} was not queued at allocation time"
            )
        running_by_node.setdefault(node_id, []).append(
            replace(
                job,
                status="running",
                assigned_node=node_id,
                started_at_us=now_us,
                expires_at_us=expires_at,
            )
        )
        available[node_id] -= job.required_slots
        events.append(
            {
                "type": "dispatched",
                "job_id": job.job_id,
                "node_id": node_id,
                "slots": job.required_slots,
                "at": to_iso(now_us),
                "lease_expires_at": to_iso(expires_at),
            }
        )

    # -- read helpers -----------------------------------------------------

    @staticmethod
    def _queue_positions(conn: sqlite3.Connection) -> dict[str, int]:
        rows = conn.execute(
            "SELECT job_id FROM jobs WHERE status = 'queued' ORDER BY seq"
        ).fetchall()
        return {row["job_id"]: index for index, row in enumerate(rows, start=1)}

    def _head_estimate_in_tx(self, conn: sqlite3.Connection, now_us: int) -> dict:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'queued' ORDER BY seq LIMIT 1"
        ).fetchone()
        if row is None:
            return {
                "head": None,
                "earliest_start_at": None,
                "earliest_start_in_seconds": None,
                "blocked_reason": None,
            }

        head = _row_to_job(row)
        capacities = {
            r["node_id"]: r["capacity"]
            for r in conn.execute("SELECT node_id, capacity FROM nodes").fetchall()
        }
        running_by_node: dict[str, list[JobRecord]] = {}
        for running_row in conn.execute(
            "SELECT * FROM jobs WHERE status = 'running'"
        ).fetchall():
            job = _row_to_job(running_row)
            if job.assigned_node:
                running_by_node.setdefault(job.assigned_node, []).append(job)

        earliest = self._earliest_start_us(
            head, capacities, running_by_node, now_us
        )
        if earliest is None:
            reason = (
                "no_nodes_configured" if not capacities else "insufficient_capacity"
            )
        elif earliest > now_us:
            reason = "waiting_for_free_slots"
        else:
            reason = None

        positions = self._queue_positions(conn)
        return {
            "head": self._job_dict(head, positions, now_us),
            "earliest_start_at": to_iso(earliest),
            "earliest_start_in_seconds": (
                None
                if earliest is None
                else max(0.0, (earliest - now_us) / US_PER_SECOND)
            ),
            "blocked_reason": reason,
        }

    def _job_dict(
        self,
        job: JobRecord,
        positions: dict[str, int] | None = None,
        now_us: int | None = None,
    ) -> dict:
        now = self.now_us() if now_us is None else now_us
        remaining = None
        if job.status == "running" and job.expires_at_us is not None:
            remaining = max(0.0, (job.expires_at_us - now) / US_PER_SECOND)
        return {
            "job_id": job.job_id,
            "required_slots": job.required_slots,
            "max_duration_seconds": job.max_duration_us / US_PER_SECOND,
            "status": job.status,
            "queue_position": (
                positions.get(job.job_id)
                if positions is not None and job.status == "queued"
                else None
            ),
            "submitted_at": to_iso(job.submitted_at_us),
            "assigned_node": job.assigned_node,
            "started_at": to_iso(job.started_at_us),
            "lease_expires_at": to_iso(job.expires_at_us),
            "ended_at": to_iso(job.ended_at_us),
            "remaining_seconds": remaining,
        }

    def _node_rows(
        self, conn: sqlite3.Connection, node_id: str | None = None
    ) -> list[dict] | dict | None:
        where = "WHERE n.node_id = ?" if node_id is not None else ""
        params: tuple = (node_id,) if node_id is not None else ()
        rows = conn.execute(
            f"""
            SELECT n.node_id, n.capacity,
                   COALESCE(SUM(CASE WHEN j.status = 'running'
                                     THEN j.required_slots ELSE 0 END), 0)
                       AS used_slots,
                   COALESCE(SUM(CASE WHEN j.status = 'running'
                                     THEN 1 ELSE 0 END), 0)
                       AS running_jobs
            FROM nodes n
            LEFT JOIN jobs j ON j.assigned_node = n.node_id
            {where}
            GROUP BY n.node_id, n.capacity
            ORDER BY n.node_id
            """,
            params,
        ).fetchall()
        nodes = [
            {
                "node_id": row["node_id"],
                "capacity": row["capacity"],
                "used_slots": row["used_slots"],
                "free_slots": row["capacity"] - row["used_slots"],
                "running_jobs": row["running_jobs"],
            }
            for row in rows
        ]
        if node_id is None:
            return nodes
        return nodes[0] if nodes else None
