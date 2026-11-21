"""HTTP contract tests for the scheduler API."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import FakeClock


@pytest.fixture
def api(db_path: str, clock: FakeClock):
    app = create_app(db_path=db_path, start_background=False, clock=clock)
    with TestClient(app) as client:
        client.clock = clock  # type: ignore[attr-defined]
        yield client


def configure(api, node_id: str, capacity: int):
    response = api.post("/nodes", json={"node_id": node_id, "capacity": capacity})
    assert response.status_code == 200, response.text
    return response.json()


def submit(api, job_id: str, slots: int, duration: float):
    return api.post(
        "/jobs",
        json={
            "job_id": job_id,
            "required_slots": slots,
            "max_duration_seconds": duration,
        },
    )


def test_openapi_and_docs_are_served(api):
    assert api.get("/docs").status_code == 200
    schema = api.get("/openapi.json").json()
    assert schema["info"]["title"] == "Render Farm Slot Scheduler"
    for path in ("/nodes", "/jobs", "/queue/head", "/queue"):
        assert path in schema["paths"]


def test_healthz(api):
    assert api.get("/healthz").json() == {"status": "ok"}


def test_node_crud_and_usage(api):
    assert configure(api, "n1", 4) == {
        "node_id": "n1",
        "capacity": 4,
        "used_slots": 0,
        "free_slots": 4,
        "running_jobs": 0,
    }
    submit(api, "a", 3, 30)
    node = api.get("/nodes/n1").json()
    assert node["used_slots"] == 3 and node["free_slots"] == 1
    assert api.get("/nodes/missing").status_code == 404

    response = api.post("/nodes", json={"node_id": "n1", "capacity": 2})
    assert response.status_code == 409
    assert "leased" in response.json()["detail"]


def test_submit_dispatches_and_reports_lease(api):
    configure(api, "n1", 4)
    response = submit(api, "a", 2, 30)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["status"] == "running"
    assert body["assigned_node"] == "n1"
    assert body["lease_expires_at"] is not None
    assert body["queue_position"] is None
    assert body["started_at"] <= body["lease_expires_at"]
    assert body["remaining_seconds"] == pytest.approx(30, abs=0.01)


def test_invalid_payloads_return_422(api):
    configure(api, "n1", 4)
    assert submit(api, "zero", 0, 10).status_code == 422
    assert submit(api, "bad id", 1, 10).status_code == 422
    assert submit(api, "neg", -1, 10).status_code == 422
    assert submit(api, "dur", 1, 0).status_code == 422
    assert api.post("/nodes", json={"node_id": "n1", "capacity": 0}).status_code == 422
    assert api.post("/nodes", json={"node_id": "n1"}).status_code == 422


def test_duplicate_job_returns_409(api):
    configure(api, "n1", 4)
    assert submit(api, "a", 1, 10).status_code == 201
    response = submit(api, "a", 1, 10)
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]


def test_queue_head_and_queue_view(api):
    configure(api, "n1", 2)
    submit(api, "a", 2, 12)
    submit(api, "b", 2, 20)

    head = api.get("/queue/head").json()
    assert head["head"]["job_id"] == "b"
    assert head["head"]["queue_position"] == 1
    assert head["earliest_start_in_seconds"] == pytest.approx(12, abs=0.01)
    assert head["earliest_start_at"].endswith("Z")

    queue = api.get("/queue").json()
    assert [j["job_id"] for j in queue["jobs"]] == ["b"]
    assert queue["head"]["job_id"] == "b"


def test_queue_head_empty_and_unconfigured(api):
    empty = api.get("/queue/head").json()
    assert empty["head"] is None
    assert empty["earliest_start_at"] is None

    submit(api, "a", 4, 10)
    head = api.get("/queue/head").json()
    assert head["head"]["job_id"] == "a"
    assert head["blocked_reason"] == "no_nodes_configured"
    assert head["earliest_start_at"] is None


def test_job_lookup_and_listing(api):
    configure(api, "n1", 2)
    submit(api, "a", 2, 60)
    submit(api, "b", 2, 60)
    submit(api, "c", 2, 60)

    assert api.get("/jobs/a").json()["status"] == "running"
    assert api.get("/jobs/nope").status_code == 404
    running = api.get("/jobs", params={"status": "running"}).json()
    queued = api.get("/jobs", params={"status": "queued"}).json()
    assert [j["job_id"] for j in running] == ["a"]
    assert [j["job_id"] for j in queued] == ["b", "c"]
    listed = api.get("/jobs").json()
    assert [j["job_id"] for j in listed] == ["a", "b", "c"]


def test_release_releases_and_backfills(api):
    configure(api, "n1", 2)
    submit(api, "a", 2, 60)
    submit(api, "b", 2, 60)

    response = api.post("/jobs/a/release")
    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert api.get("/jobs/b").json()["status"] == "running"
    assert api.post("/jobs/a/release").status_code == 409
    assert api.post("/jobs/missing/release").status_code == 404


def test_expiry_via_tick_endpoint(api):
    configure(api, "n1", 2)
    submit(api, "a", 2, 5)
    submit(api, "b", 2, 5)
    api.clock.advance(5)

    response = api.post("/admin/tick")
    assert response.status_code == 200
    events = response.json()["events"]
    assert {"type": "expired", "job_id": "a"} in [
        {"type": e["type"], "job_id": e["job_id"]} for e in events
    ]
    assert api.get("/jobs/a").json()["status"] == "expired"
    assert api.get("/jobs/b").json()["status"] == "running"


def test_backfill_scenario_end_to_end(api):
    configure(api, "n1", 6)
    submit(api, "a", 2, 10)
    submit(api, "big", 5, 10)
    submit(api, "short", 2, 5)
    submit(api, "long", 4, 20)

    assert api.get("/jobs/big").json()["queue_position"] == 1
    assert api.get("/jobs/short").json()["status"] == "running"
    assert api.get("/jobs/long").json()["status"] == "queued"
    node = api.get("/nodes/n1").json()
    assert node["used_slots"] == 4
    assert node["free_slots"] == 2
    head = api.get("/queue/head").json()
    assert head["head"]["job_id"] == "big"
    assert head["earliest_start_in_seconds"] == pytest.approx(10, abs=0.01)


def test_lease_expiry_automatic_in_background(db_path, clock):
    """The background reaper releases leases without an API call."""
    app = create_app(db_path=db_path, start_background=True, tick_seconds=0.02, clock=clock)
    with TestClient(app) as client:
        configure(client, "n1", 2)
        submit(client, "a", 2, 5)
        submit(client, "b", 2, 5)
        assert client.get("/jobs/b").json()["status"] == "queued"

        clock.advance(5)
        import time as _time

        deadline = _time.time() + 3
        while _time.time() < deadline:
            if client.get("/jobs/b").json()["status"] == "running":
                break
            _time.sleep(0.02)
        assert client.get("/jobs/b").json()["status"] == "running"
        assert client.get("/jobs/a").json()["status"] == "expired"
