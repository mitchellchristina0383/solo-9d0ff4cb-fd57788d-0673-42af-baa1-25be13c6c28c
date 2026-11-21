"""SQLite-level invariants that back up the scheduler's own accounting."""

from __future__ import annotations

import sqlite3

import pytest

from app.scheduler import Scheduler


def test_direct_insert_cannot_oversubscribe(scheduler: Scheduler):
    scheduler.configure_node("n1", 2)
    scheduler.submit_job("a", 2, 60)  # fills n1
    conn = scheduler.store._connect()
    try:
        conn.execute(
            "INSERT INTO jobs (job_id, required_slots, max_duration_us, status,"
            " submitted_at_us) VALUES ('b', 1, 10, 'queued', 1)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE jobs SET status = 'running', assigned_node = 'n1',"
                " started_at_us = 1, expires_at_us = 2 WHERE job_id = 'b'"
            )
    finally:
        conn.close()


def test_direct_insert_of_running_over_capacity_rejected(scheduler: Scheduler):
    scheduler.configure_node("n1", 1)
    conn = scheduler.store._connect()
    try:
        conn.execute(
            "INSERT INTO jobs (job_id, required_slots, max_duration_us, status,"
            " submitted_at_us, assigned_node, started_at_us, expires_at_us)"
            " VALUES ('direct', 2, 10, 'running', 1, 'n1', 1, 11)"
        )
    except sqlite3.IntegrityError as exc:
        assert "capacity" in str(exc)
    else:  # pragma: no cover
        pytest.fail("oversubscription was accepted")
    finally:
        conn.close()


def test_direct_capacity_shrink_below_usage_rejected(scheduler: Scheduler):
    scheduler.configure_node("n1", 4)
    scheduler.submit_job("a", 3, 60)
    conn = scheduler.store._connect()
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE nodes SET capacity = 1 WHERE node_id = 'n1'")
    finally:
        conn.close()


def test_leases_remain_within_capacity_after_mixed_operations(scheduler, clock):
    scheduler.configure_node("n1", 3)
    scheduler.configure_node("n2", 2)
    for index in range(6):
        scheduler.submit_job(f"j{index}", 1 + index % 3, 10)
    for step in range(12):
        clock.advance(1.5)
        scheduler.tick()
        for node in scheduler.list_nodes():
            assert node["used_slots"] <= node["capacity"]
        running = scheduler.list_jobs(status="running")
        by_node: dict[str, int] = {}
        for item in running:
            by_node[item["assigned_node"]] = (
                by_node.get(item["assigned_node"], 0) + item["required_slots"]
            )
        for node_id, used in by_node.items():
            capacity = scheduler.get_node(node_id)["capacity"]
            assert used <= capacity
