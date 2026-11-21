"""Scheduler behaviour: FIFO, backfilling, leases, and failure handling."""

from __future__ import annotations

import sqlite3
import threading

import pytest

from app.db import Store
from app.scheduler import (
    CapacityBelowUsageError,
    DuplicateJobError,
    JobNotFoundError,
    LeaseNotHeldError,
    Scheduler,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def job(api, job_id):
    return api.get_job(job_id)


def occupied(scheduler):
    return sum(node["used_slots"] for node in scheduler.list_nodes())


# ---------------------------------------------------------------------------
# Queue order and single-node packing
# ---------------------------------------------------------------------------


def test_fifo_requires_head_to_go_first(scheduler):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 3, 10)
    scheduler.submit_job("b", 3, 10)

    assert job(scheduler, "a")["status"] == "running"
    b = job(scheduler, "b")
    assert b["status"] == "queued"
    assert b["queue_position"] == 1
    assert b["assigned_node"] is None

    head = scheduler.head_estimate()
    assert head["head"]["job_id"] == "b"
    assert head["earliest_start_in_seconds"] == pytest.approx(10.0)
    assert head["blocked_reason"] == "waiting_for_free_slots"


def test_job_never_spans_nodes(scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.configure_node("n2", 2)
    scheduler.submit_job("wide", 3, 10)

    wide = job(scheduler, "wide")
    assert wide["status"] == "queued"
    assert wide["assigned_node"] is None
    assert wide["queue_position"] == 1
    head = scheduler.head_estimate()
    assert head["blocked_reason"] == "insufficient_capacity"
    assert head["earliest_start_at"] is None
    # Two nodes have 4 free slots between them, but the job must not split.
    assert sum(n["free_slots"] for n in scheduler.list_nodes()) == 4

    # Growing a node makes the waiting job dispatchable again.
    scheduler.configure_node("n2", 3)
    assert job(scheduler, "wide")["status"] == "running"
    assert job(scheduler, "wide")["assigned_node"] == "n2"


def test_head_larger_than_every_node_does_not_block_fitting_backfill(scheduler):
    scheduler.configure_node("n1", 5)
    scheduler.submit_job("a", 2, 30)  # running, 3 slots free
    scheduler.submit_job("giant", 5, 30)  # queued: needs all 5
    scheduler.configure_node("n1", 4)  # shrink is legal (only 2 slots leased)

    head = scheduler.head_estimate()
    assert head["head"]["job_id"] == "giant"
    assert head["blocked_reason"] == "insufficient_capacity"
    assert head["earliest_start_at"] is None

    # With no finite head start time to protect, a fitting job may run in
    # the idle slots; the giant head keeps position 1.
    scheduler.submit_job("small", 2, 5)
    small = job(scheduler, "small")
    assert small["status"] == "running"
    assert small["assigned_node"] == "n1"
    assert job(scheduler, "giant")["queue_position"] == 1
    assert scheduler.list_nodes()[0]["used_slots"] <= 4


# ---------------------------------------------------------------------------
# Leases: expiry and explicit release
# ---------------------------------------------------------------------------


def test_expiry_frees_slots_and_reschedules(scheduler, clock):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, 10)
    scheduler.submit_job("b", 2, 10)
    assert job(scheduler, "b")["status"] == "queued"

    clock.advance(9.9)
    assert scheduler.tick() == []
    assert job(scheduler, "b")["status"] == "queued"

    clock.advance(0.2)
    events = scheduler.tick()
    types = {(e["type"], e["job_id"]) for e in events}
    assert ("expired", "a") in types
    assert ("dispatched", "b") in types

    a = job(scheduler, "a")
    assert a["status"] == "expired"
    assert a["ended_at"] is not None
    b = job(scheduler, "b")
    assert b["status"] == "running"
    assert b["assigned_node"] == "n1"
    assert scheduler.list_nodes()[0]["used_slots"] == 2


def test_release_frees_slots_and_reschedules(scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, 60)
    scheduler.submit_job("b", 2, 60)

    released = scheduler.release_job("a")
    assert released["status"] == "completed"
    assert released["ended_at"] is not None

    b = job(scheduler, "b")
    assert b["status"] == "running"
    assert b["assigned_node"] == "n1"
    assert scheduler.list_nodes()[0]["used_slots"] == 2


def test_release_twice_is_rejected(scheduler):
    scheduler.configure_node("n1", 1)
    scheduler.submit_job("a", 1, 10)
    scheduler.release_job("a")
    with pytest.raises(LeaseNotHeldError):
        scheduler.release_job("a")
    with pytest.raises(JobNotFoundError):
        scheduler.release_job("missing")


def test_release_after_expiry_reports_not_running(scheduler, clock):
    scheduler.configure_node("n1", 1)
    scheduler.submit_job("a", 1, 5)
    clock.advance(5)
    with pytest.raises(LeaseNotHeldError):
        scheduler.release_job("a")
    assert job(scheduler, "a")["status"] == "expired"


def test_duplicate_job_rejected(scheduler):
    scheduler.configure_node("n1", 1)
    scheduler.submit_job("a", 1, 10)
    with pytest.raises(DuplicateJobError):
        scheduler.submit_job("a", 1, 10)


def test_capacity_shrink_below_usage_rejected(scheduler):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 3, 30)
    with pytest.raises(CapacityBelowUsageError):
        scheduler.configure_node("n1", 2)
    assert scheduler.configure_node("n1", 3)["capacity"] == 3


def test_capacity_growth_dispatches_waiting_job(scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, 30)
    scheduler.submit_job("b", 2, 30)
    assert job(scheduler, "b")["status"] == "queued"
    scheduler.configure_node("n1", 4)
    assert job(scheduler, "b")["status"] == "running"


# ---------------------------------------------------------------------------
# Backfilling
# ---------------------------------------------------------------------------


def test_backfill_runs_later_job_that_finishes_before_head_starts(scheduler):
    # n1: 6 slots. A holds 2 for 10s, so a 5-slot head can start at t+10.
    # 4 slots are idle meanwhile.
    scheduler.configure_node("n1", 6)
    scheduler.submit_job("a", 2, 10)
    scheduler.submit_job("head", 5, 10)
    scheduler.submit_job("short", 2, 5)  # ends at t+5, before head start

    assert job(scheduler, "head")["status"] == "queued"
    assert job(scheduler, "head")["queue_position"] == 1
    assert job(scheduler, "short")["status"] == "running"
    assert scheduler.head_estimate()["earliest_start_in_seconds"] == pytest.approx(10)


def test_backfill_skips_job_that_would_outlive_head_start(scheduler):
    # Same setup, but the candidate would still hold slots at t+10.
    scheduler.configure_node("n1", 6)
    scheduler.submit_job("a", 2, 10)
    scheduler.submit_job("head", 5, 10)
    scheduler.submit_job("long", 4, 20)
    scheduler.submit_job("short", 2, 5)

    # The 4-slot long job would leave only 2 free at t+10 (head needs 5), so
    # it must stay queued.  The later short job fits without delaying head.
    assert job(scheduler, "long")["status"] == "queued"
    assert job(scheduler, "long")["queue_position"] == 2
    assert job(scheduler, "short")["status"] == "running"
    assert job(scheduler, "short")["assigned_node"] == "n1"
    assert scheduler.head_estimate()["earliest_start_in_seconds"] == pytest.approx(10)
    assert occupied(scheduler) <= 6


def test_backfill_uses_other_node_without_touching_head_node(scheduler):
    scheduler.configure_node("n1", 4)
    scheduler.configure_node("n2", 2)
    scheduler.submit_job("a", 4, 10)  # n1 fully occupied
    scheduler.submit_job("head", 3, 10)  # needs n1, earliest t+10
    scheduler.submit_job("peek", 2, 5)  # runs on idle n2

    assert job(scheduler, "head")["status"] == "queued"
    peek = job(scheduler, "peek")
    assert peek["status"] == "running"
    assert peek["assigned_node"] == "n2"
    assert scheduler.head_estimate()["earliest_start_in_seconds"] == pytest.approx(10)


def test_backfill_preserves_queue_order_among_candidates(scheduler):
    scheduler.configure_node("n1", 6)
    scheduler.submit_job("a", 2, 10)
    scheduler.submit_job("head", 5, 10)
    scheduler.submit_job("first", 2, 20)  # rejected: outlives head start
    scheduler.submit_job("second", 2, 5)  # accepted

    assert job(scheduler, "first")["status"] == "queued"
    assert job(scheduler, "first")["queue_position"] == 2
    assert job(scheduler, "second")["status"] == "running"


# ---------------------------------------------------------------------------
# Failure handling: no partial state, no oversubscription
# ---------------------------------------------------------------------------


def _raw_jobs(scheduler):
    conn = scheduler.store._connect()
    try:
        rows = conn.execute(
            "SELECT job_id, status, assigned_node, started_at_us, expires_at_us"
            " FROM jobs ORDER BY seq"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def test_dispatch_failure_rolls_back_expiry_and_allocation(scheduler, clock):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 2, 10)
    scheduler.submit_job("b", 2, 10)  # fills n1
    scheduler.submit_job("c", 2, 10)  # queued

    before = _raw_jobs(scheduler)
    clock.advance(10)  # a and b are now due, so the pass would expire them

    def explode(*_args, **_kwargs):
        raise RuntimeError("simulated dispatch failure")

    original = scheduler._allocate
    scheduler._allocate = explode
    try:
        events = scheduler.tick()  # swallowed and rolled back
        after = _raw_jobs(scheduler)
    finally:
        scheduler._allocate = original

    assert events == []
    # Expiry and allocation are in the same transaction: both rolls back.
    assert after == before
    assert {j["job_id"]: j["status"] for j in after} == {
        "a": "running",
        "b": "running",
        "c": "queued",
    }

    # The retry succeeds once dispatch works again.
    events = scheduler.tick()
    types = {(e["type"], e["job_id"]) for e in events}
    assert ("expired", "a") in types and ("expired", "b") in types
    assert ("dispatched", "c") in types


def test_failed_submit_dispatch_keeps_job_queued(scheduler, monkeypatch):
    scheduler.configure_node("n1", 4)

    def explode(*_args, **_kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(scheduler, "_allocate", explode)
    result = scheduler.submit_job("a", 2, 30)
    assert result["status"] == "queued"

    monkeypatch.undo()
    scheduler.tick()
    assert job(scheduler, "a")["status"] == "running"


def test_failed_dispatch_after_release_keeps_release(scheduler, monkeypatch):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, 60)
    scheduler.submit_job("b", 2, 60)

    monkeypatch.setattr(
        scheduler,
        "_allocate",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    released = scheduler.release_job("a")
    assert released["status"] == "completed"
    assert job(scheduler, "b")["status"] == "queued"

    monkeypatch.undo()
    scheduler.tick()
    assert job(scheduler, "b")["status"] == "running"


def test_concurrent_ticking_never_oversubscribes(db_path, clock):
    scheduler = Scheduler(Store(db_path), clock=clock)
    scheduler.configure_node("n1", 5)
    scheduler.configure_node("n2", 3)
    for i in range(20):
        scheduler.submit_job(f"job-{i:02d}", 1 + (i % 3), 5)

    errors: list[BaseException] = []

    def worker() -> None:
        local = Scheduler(Store(db_path), clock=clock)
        try:
            for _ in range(10):
                local.tick()
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    clock.advance(5.5)  # every lease expires at once
    for thread in threads:
        thread.join()

    assert not errors
    nodes = {n["node_id"]: n for n in scheduler.list_nodes()}
    assert nodes["n1"]["used_slots"] <= nodes["n1"]["capacity"]
    assert nodes["n2"]["used_slots"] <= nodes["n2"]["capacity"]

    running = scheduler.list_jobs(status="running")
    assert len({j["job_id"] for j in running}) == len(running)
    assert occupied(scheduler) == sum(j["required_slots"] for j in running)
    for item in running:
        assert item["assigned_node"] in {"n1", "n2"}


def test_concurrent_submissions_keep_fifo_and_capacity(db_path, clock):
    scheduler = Scheduler(Store(db_path), clock=clock)
    scheduler.configure_node("n1", 4)
    start = threading.Barrier(6)
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        local = Scheduler(Store(db_path), clock=clock)
        start.wait()
        try:
            local.submit_job(f"job-{index}", 2, 30)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert scheduler.list_nodes()[0]["used_slots"] <= 4
    running = scheduler.list_jobs(status="running")
    queued = scheduler.list_jobs(status="queued")
    assert sum(j["required_slots"] for j in running) <= 4
    assert len(running) == 2
    assert len(queued) == 4
    submitted = [j["job_id"] for j in scheduler.list_jobs()]
    assert len(set(submitted)) == 6


def test_transaction_rollback_leaves_no_partial_allocation(scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, 30)
    try:
        with scheduler.store.transaction() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'running', assigned_node = 'n1',"
                " started_at_us = 1, expires_at_us = 2 WHERE job_id = 'a'"
            )
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    conn = scheduler.store._connect()
    try:
        row = conn.execute(
            "SELECT status, assigned_node, started_at_us, expires_at_us"
            " FROM jobs WHERE job_id = 'a'"
        ).fetchone()
    finally:
        conn.close()
    assert row["started_at_us"] != 1
    assert row["expires_at_us"] != 2
    # A second job cannot double-book n1.
    scheduler.submit_job("b", 1, 30)
    assert job(scheduler, "b")["status"] == "queued"


def test_sqlite_rejects_invalid_rows(scheduler):
    conn = scheduler.store._connect()
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (job_id, required_slots, max_duration_us,"
                " status, submitted_at_us)"
                " VALUES ('bad', 0, 1, 'queued', 1)"
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (job_id, required_slots, max_duration_us,"
                " status, submitted_at_us, assigned_node)"
                " VALUES ('bad2', 1, 1, 'running', 1, NULL)"
            )
    finally:
        conn.close()
