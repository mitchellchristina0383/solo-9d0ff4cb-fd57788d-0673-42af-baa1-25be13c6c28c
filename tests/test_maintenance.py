"""Maintenance-window scheduling: reservations, blackouts, recovery.

Time is driven by the deterministic ``FakeClock`` (epoch seconds); window
bounds are expressed in microseconds exactly as the scheduler stores them.
"""

from __future__ import annotations

import threading

import pytest

from app.db import Store
from app.scheduler import (
    MaintenanceConflictError,
    MaintenanceInProgressError,
    MaintenanceNotFoundError,
    MaintenanceValidationError,
    NodeNotFoundError,
    Scheduler,
    US_PER_SECOND,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def at(clock_seconds: float) -> int:
    return int(round(clock_seconds * US_PER_SECOND))


def job(scheduler, job_id):
    return scheduler.get_job(job_id)


def reserve(scheduler, node_id, start_s, end_s):
    return scheduler.reserve_maintenance(node_id, at(start_s), at(end_s))


# ---------------------------------------------------------------------------
# Reservation validation and lifecycle
# ---------------------------------------------------------------------------


def test_reserve_requires_known_node(scheduler):
    with pytest.raises(NodeNotFoundError):
        reserve(scheduler, "ghost", 1_700_000_100, 1_700_000_200)


def test_reserve_rejects_past_start_and_bad_bounds(scheduler, clock):
    scheduler.configure_node("n1", 4)
    with pytest.raises(MaintenanceValidationError):
        reserve(scheduler, "n1", clock.now - 1, clock.now + 100)
    with pytest.raises(MaintenanceValidationError):
        reserve(scheduler, "n1", clock.now + 100, clock.now + 50)
    with pytest.raises(MaintenanceValidationError):
        reserve(scheduler, "n1", clock.now + 1, clock.now + 1)


def test_reserve_creates_then_reschedules_before_start(scheduler, clock):
    scheduler.configure_node("n1", 4)
    view, created = reserve(scheduler, "n1", clock.now + 100, clock.now + 200)
    assert created is True
    assert view["status"] == "upcoming"
    assert view["starts_in_seconds"] == pytest.approx(100)
    assert view["ends_in_seconds"] == pytest.approx(200)

    view2, created2 = reserve(scheduler, "n1", clock.now + 150, clock.now + 250)
    assert created2 is False
    assert view2["window_id"] == view["window_id"]
    assert view2["starts_in_seconds"] == pytest.approx(150)


def test_active_window_cannot_be_rescheduled_or_cancelled(scheduler, clock):
    scheduler.configure_node("n1", 4)
    reserve(scheduler, "n1", clock.now + 10, clock.now + 20)
    clock.advance(12)
    with pytest.raises(MaintenanceInProgressError):
        reserve(scheduler, "n1", clock.now + 5, clock.now + 10)
    with pytest.raises(MaintenanceInProgressError):
        scheduler.cancel_maintenance("n1")


def test_cancel_removes_upcoming_window(scheduler, clock):
    scheduler.configure_node("n1", 4)
    reserve(scheduler, "n1", clock.now + 100, clock.now + 200)
    cancelled = scheduler.cancel_maintenance("n1")
    assert cancelled["status"] == "upcoming"
    with pytest.raises(MaintenanceNotFoundError):
        scheduler.get_maintenance_window("n1")
    with pytest.raises(MaintenanceNotFoundError):
        scheduler.cancel_maintenance("n1")


def test_window_status_transitions_upcoming_active_ended(scheduler, clock):
    scheduler.configure_node("n1", 4)
    reserve(scheduler, "n1", clock.now + 10, clock.now + 20)
    assert scheduler.get_maintenance_window("n1")["status"] == "upcoming"

    clock.advance(10)
    scheduler.tick()
    assert scheduler.get_maintenance_window("n1")["status"] == "active"

    clock.advance(20)
    scheduler.tick()
    # Ended windows are no longer exposed as the "current" window.
    with pytest.raises(MaintenanceNotFoundError):
        scheduler.get_maintenance_window("n1")
    # ...but remain in the history listing.
    assert [w["status"] for w in scheduler.list_maintenance_windows()] == ["ended"]

def test_a_second_window_is_allowed_after_the_first_ends(scheduler, clock):
    scheduler.configure_node("n1", 4)
    reserve(scheduler, "n1", clock.now + 10, clock.now + 20)
    clock.advance(21)
    scheduler.tick()
    view, created = reserve(scheduler, "n1", clock.now + 10, clock.now + 20)
    assert created is True
    assert view["status"] == "upcoming"


# ---------------------------------------------------------------------------
# Overlap with active leases: blockers and preservation of the reservation
# ---------------------------------------------------------------------------


def test_reservation_blocked_by_overlapping_lease_returns_job_ids(scheduler, clock):
    scheduler.configure_node("n1", 6)
    scheduler.submit_job("a", 2, 100)  # expires at now+100
    scheduler.submit_job("b", 1, 30)   # expires at now+30

    # Window starts at now+50: lease 'a' (ends +100) overlaps, 'b' does not.
    with pytest.raises(MaintenanceConflictError) as exc_info:
        reserve(scheduler, "n1", clock.now + 50, clock.now + 80)
    assert exc_info.value.blocking_jobs == ["a"]


def test_lease_expiring_exactly_at_window_start_is_allowed(scheduler, clock):
    scheduler.configure_node("n1", 6)
    scheduler.submit_job("a", 2, 100)  # expires exactly at now+100
    view, created = reserve(scheduler, "n1", clock.now + 100, clock.now + 200)
    assert created is True
    assert view["starts_in_seconds"] == pytest.approx(100)


def test_reschedule_conflict_keeps_original_window(scheduler, clock):
    scheduler.configure_node("n1", 6)
    original, _ = reserve(scheduler, "n1", clock.now + 200, clock.now + 300)
    scheduler.submit_job("a", 2, 100)  # expires at now+100

    # Moving the window to now+50 collides with lease 'a'.
    with pytest.raises(MaintenanceConflictError) as exc_info:
        reserve(scheduler, "n1", clock.now + 50, clock.now + 90)
    assert exc_info.value.blocking_jobs == ["a"]

    current = scheduler.get_maintenance_window("n1")
    assert current["window_id"] == original["window_id"]
    assert current["starts_in_seconds"] == pytest.approx(200)
    assert current["ends_in_seconds"] == pytest.approx(300)


def test_expired_leases_do_not_block_reservation(scheduler, clock):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 2, 10)
    clock.advance(10)
    # The reservation sweeps expiries inside its own transaction.
    view, created = reserve(scheduler, "n1", clock.now + 5, clock.now + 15)
    assert created is True
    assert job(scheduler, "a")["status"] == "expired"


# ---------------------------------------------------------------------------
# Dispatch respects the window
# ---------------------------------------------------------------------------


def test_node_does_not_dispatch_during_active_window(scheduler, clock):
    scheduler.configure_node("n1", 4)
    reserve(scheduler, "n1", clock.now + 10, clock.now + 60)
    scheduler.submit_job("j", 2, 5)

    # Before the window the short lease fits (ends at +5 < window start +10).
    assert job(scheduler, "j")["status"] == "running"

    # A 20s lease would cross the window start, so it waits for the window end.
    scheduler.submit_job("queued-before", 2, 20)
    assert job(scheduler, "queued-before")["status"] == "queued"
    clock.advance(10)  # window now active; previous lease has expired too
    scheduler.tick()
    item = job(scheduler, "queued-before")
    assert item["status"] == "queued"
    assert scheduler.list_nodes()[0]["used_slots"] == 0
    assert scheduler.head_estimate()["blocked_reason"] == "maintenance_window"

    # Recovery at the window end is automatic.
    clock.advance(60)
    events = scheduler.tick()
    assert ("dispatched", "queued-before") in {
        (e["type"], e["job_id"]) for e in events
    }
    assert job(scheduler, "queued-before")["status"] == "running"

def test_new_lease_crossing_window_start_is_rejected_but_touching_is_allowed(
    scheduler, clock
):
    scheduler.configure_node("n1", 6)
    reserve(scheduler, "n1", clock.now + 100, clock.now + 200)

    # 200s lease would still be held at the window start: stays queued.
    scheduler.submit_job("too-long", 2, 200)
    assert job(scheduler, "too-long")["status"] == "queued"

    # A 100s lease expires exactly when the window starts: allowed.
    scheduler.submit_job("exact-fit", 2, 100)
    exact = job(scheduler, "exact-fit")
    assert exact["status"] == "running"
    assert exact["lease_expires_at"] is not None


def test_backfill_before_window_uses_only_window_safe_jobs(scheduler, clock):
    # 6 slots. Lease 'a' holds 2 until now+100; the 5-slot head also waits
    # until now+100. A maintenance window starts at now+100, so the head can
    # only run after it ends (now+200); the head's protected earliest start
    # therefore accounts for both the lease and the window.
    scheduler.configure_node("n1", 6)
    scheduler.submit_job("a", 2, 100)
    scheduler.submit_job("head", 5, 10)
    reserve(scheduler, "n1", clock.now + 100, clock.now + 200)

    # A 4-slot / 60s backfill ends at +60, before the window: dispatch it,
    # and it must not push the head beyond its window-aware ETA (+200).
    scheduler.submit_job("short-bf", 4, 60)
    assert job(scheduler, "short-bf")["status"] == "running"

    # A 2-slot / 120s job would cross the window start: it must stay queued
    # even though slots are free right now.
    scheduler.submit_job("long-bf", 2, 120)
    assert job(scheduler, "long-bf")["status"] == "queued"

    head = scheduler.head_estimate()
    assert head["head"]["job_id"] == "head"
    assert head["earliest_start_in_seconds"] == pytest.approx(200)
    assert head["blocked_reason"] == "maintenance_window"


def test_head_estimate_jumps_to_window_end_when_lease_cannot_fit(scheduler, clock):
    scheduler.configure_node("n1", 4)
    # Window starts in 10s; a 30s lease cannot finish in time, so the head's
    # earliest start is the window end (now+60).
    reserve(scheduler, "n1", clock.now + 10, clock.now + 60)
    scheduler.submit_job("head", 2, 30)
    estimate = scheduler.head_estimate()
    assert estimate["blocked_reason"] == "maintenance_window"
    assert estimate["earliest_start_in_seconds"] == pytest.approx(60)


def test_head_estimate_reasons_window_versus_slots(scheduler, clock):
    scheduler.configure_node("n1", 2)
    # Slots are the only blocker: lease fills the node until now+50.
    scheduler.submit_job("fill", 2, 50)
    reserve(scheduler, "n1", clock.now + 100, clock.now + 200)
    scheduler.submit_job("head", 1, 10)
    estimate = scheduler.head_estimate()
    assert estimate["blocked_reason"] == "waiting_for_free_slots"
    assert estimate["earliest_start_in_seconds"] == pytest.approx(50)


def test_window_blocks_only_its_own_node(scheduler, clock):
    scheduler.configure_node("n1", 2)
    scheduler.configure_node("n2", 2)
    reserve(scheduler, "n1", clock.now + 10, clock.now + 60)

    # n1 cannot take a lease crossing the window; n2 is unaffected and gets
    # the job via best-fit... n1 has more free slots but is inadmissible, so
    # the job must land on n2.
    scheduler.submit_job("j", 2, 30)
    placed = job(scheduler, "j")
    assert placed["status"] == "running"
    assert placed["assigned_node"] == "n2"


def test_cancelling_window_unblocks_dispatch(scheduler, clock):
    scheduler.configure_node("n1", 4)
    reserve(scheduler, "n1", clock.now + 10, clock.now + 60)
    scheduler.submit_job("j", 2, 30)
    assert job(scheduler, "j")["status"] == "queued"

    scheduler.cancel_maintenance("n1")
    assert job(scheduler, "j")["status"] == "running"


def test_rescheduling_window_earlier_rechecks_existing_queued_jobs(scheduler, clock):
    scheduler.configure_node("n1", 4)
    reserve(scheduler, "n1", clock.now + 200, clock.now + 300)
    # A 100s lease fits before the far-away window.
    scheduler.submit_job("j", 2, 100)
    assert job(scheduler, "j")["status"] == "running"

    # Pull the window to now+50: no running lease overlaps (lease ends +100?
    # it does overlap) -> conflict, reservation must stay at +200.
    with pytest.raises(MaintenanceConflictError):
        reserve(scheduler, "n1", clock.now + 50, clock.now + 80)
    assert scheduler.get_maintenance_window("n1")["starts_in_seconds"] == pytest.approx(200)


# ---------------------------------------------------------------------------
# Durability and concurrency
# ---------------------------------------------------------------------------


def test_window_survives_scheduler_restart(db_path, clock):
    first = Scheduler(Store(db_path), clock=clock)
    first.configure_node("n1", 4)
    reserve(first, "n1", clock.now + 100, clock.now + 200)
    first.submit_job("j", 2, 300)
    assert job(first, "j")["status"] == "queued"

    restarted = Scheduler(Store(db_path), clock=clock)
    view = restarted.get_maintenance_window("n1")
    assert view["starts_in_seconds"] == pytest.approx(100)
    # The persisted window still constrains dispatch after restart.
    assert job(restarted, "j")["status"] == "queued"


def test_concurrent_reservation_and_dispatch_never_overlap(db_path, clock):
    scheduler = Scheduler(Store(db_path), clock=clock)
    scheduler.configure_node("n1", 6)

    start_barrier = threading.Barrier(4)
    errors: list[BaseException] = []

    def reserve_worker() -> None:
        local = Scheduler(Store(db_path), clock=clock)
        start_barrier.wait()
        try:
            for _ in range(10):
                try:
                    local.reserve_maintenance(
                        "n1", at(clock.now + 30), at(clock.now + 90)
                    )
                except MaintenanceConflictError:
                    pass  # a lease won the race: that is the contract
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    def submit_worker() -> None:
        local = Scheduler(Store(db_path), clock=clock)
        start_barrier.wait()
        try:
            for index in range(10):
                local.submit_job(f"job-{threading.get_ident()}-{index}", 1, 60)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=reserve_worker),
        threading.Thread(target=reserve_worker),
        threading.Thread(target=submit_worker),
        threading.Thread(target=submit_worker),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    # At most one unended window exists.
    windows = [
        w
        for w in scheduler.list_maintenance_windows()
        if w["status"] != "ended"
    ]
    assert len(windows) <= 1

    # Every running lease must be provably clear of the window.
    if windows:
        window = windows[0]
        for item in scheduler.list_jobs(status="running"):
            assert item["lease_expires_at"] <= window["starts_at"] or (
                item["started_at"] >= window["ends_at"]
            )


def test_concurrent_reservations_leave_one_window(db_path, clock):
    scheduler = Scheduler(Store(db_path), clock=clock)
    scheduler.configure_node("n1", 4)
    barrier = threading.Barrier(6)
    errors: list[BaseException] = []

    def worker() -> None:
        local = Scheduler(Store(db_path), clock=clock)
        barrier.wait()
        try:
            local.reserve_maintenance(
                "n1", at(clock.now + 100), at(clock.now + 200)
            )
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    current = [
        w
        for w in scheduler.list_maintenance_windows()
        if w["status"] != "ended"
    ]
    assert len(current) == 1
