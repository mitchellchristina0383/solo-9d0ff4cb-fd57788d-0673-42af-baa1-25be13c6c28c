"""HTTP contract tests for maintenance-window endpoints."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def api(db_path: str, clock):
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


def iso(clock, delta_seconds: float) -> str:
    return (
        datetime.fromtimestamp(clock.now + delta_seconds, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def reserve(api, node_id, start_delta, end_delta):
    return api.put(
        f"/nodes/{node_id}/maintenance",
        json={
            "starts_at": iso(api.clock, start_delta),
            "ends_at": iso(api.clock, end_delta),
        },
    )


# ---------------------------------------------------------------------------
# OpenAPI / validation
# ---------------------------------------------------------------------------


def test_maintenance_routes_published_in_openapi(api):
    schema = api.get("/openapi.json").json()
    for path in (
        "/nodes/{node_id}/maintenance",
        "/maintenance-windows",
    ):
        assert path in schema["paths"]
    assert "put" in schema["paths"]["/nodes/{node_id}/maintenance"]


def test_naive_timestamp_rejected(api):
    configure(api, "n1", 4)
    naive = datetime.fromtimestamp(api.clock.now + 10).isoformat()  # no tz
    response = api.put(
        "/nodes/n1/maintenance",
        json={"starts_at": naive, "ends_at": naive},
    )
    assert response.status_code == 422


def test_reserve_unknown_node_404(api):
    assert reserve(api, "ghost", 100, 200).status_code == 404


# ---------------------------------------------------------------------------
# Create / reschedule / conflict / cancel
# ---------------------------------------------------------------------------


def test_create_returns_201_and_reschedule_returns_200(api):
    configure(api, "n1", 4)
    created = reserve(api, "n1", 100, 200)
    assert created.status_code == 201
    body = created.json()
    assert body["node_id"] == "n1"
    assert body["status"] == "upcoming"
    assert body["starts_in_seconds"] == pytest.approx(100, abs=0.01)

    rescheduled = reserve(api, "n1", 150, 250)
    assert rescheduled.status_code == 200
    assert rescheduled.json()["window_id"] == body["window_id"]
    assert rescheduled.json()["starts_in_seconds"] == pytest.approx(150, abs=0.01)


def test_conflict_returns_blockers_and_keeps_original_window(api):
    configure(api, "n1", 6)
    submit(api, "a", 2, 100)
    submit(api, "b", 1, 30)
    original = reserve(api, "n1", 200, 300).json()

    response = reserve(api, "n1", 50, 90)
    assert response.status_code == 409
    body = response.json()
    assert body["blocking_jobs"] == ["a"]
    assert body["window"]["window_id"] == original["window_id"]
    assert body["window"]["starts_in_seconds"] == pytest.approx(200, abs=0.01)

    current = api.get("/nodes/n1/maintenance").json()
    assert current["window_id"] == original["window_id"]


def test_lease_ending_exactly_at_window_start_does_not_conflict(api):
    configure(api, "n1", 6)
    submit(api, "a", 2, 100)
    response = reserve(api, "n1", 100, 200)
    assert response.status_code == 201, response.text


def test_past_start_rejected_with_422(api):
    configure(api, "n1", 4)
    assert reserve(api, "n1", -5, 10).status_code == 422
    assert reserve(api, "n1", 100, 50).status_code == 422


def test_cancel_and_lifecycle_listing(api):
    configure(api, "n1", 4)
    reserve(api, "n1", 10, 20)

    assert api.delete("/nodes/n1/maintenance").status_code == 200
    assert api.get("/nodes/n1/maintenance").status_code == 404
    assert api.delete("/nodes/n1/maintenance").status_code == 404

    reserve(api, "n1", 10, 20)
    listed = api.get("/maintenance-windows").json()
    assert [w["node_id"] for w in listed] == ["n1"]
    assert listed[0]["status"] == "upcoming"


def test_active_window_rejects_reschedule_and_cancel(api):
    configure(api, "n1", 4)
    reserve(api, "n1", 10, 60)
    api.clock.advance(12)
    api.post("/admin/tick")
    assert api.get("/nodes/n1/maintenance").json()["status"] == "active"
    assert reserve(api, "n1", 100, 200).status_code == 409
    assert api.delete("/nodes/n1/maintenance").status_code == 409


def test_window_history_supports_status_filter(api):
    configure(api, "n1", 4)
    reserve(api, "n1", 10, 20)
    api.clock.advance(25)
    api.post("/admin/tick")
    reserve(api, "n1", 100, 200)

    ended = api.get("/maintenance-windows", params={"status": "ended"}).json()
    upcoming = api.get("/maintenance-windows", params={"status": "upcoming"}).json()
    assert len(ended) == 1 and ended[0]["status"] == "ended"
    assert len(upcoming) == 1 and upcoming[0]["status"] == "upcoming"


# ---------------------------------------------------------------------------
# End-to-end scheduling effects
# ---------------------------------------------------------------------------


def test_dispatch_stops_in_window_and_resumes_at_end(api):
    configure(api, "n1", 4)
    reserve(api, "n1", 10, 60)
    # A 5s lease finishes before the window: it runs now.
    submit(api, "before", 2, 5)
    # A 20s lease would cross the window start: held back.
    submit(api, "waiting", 2, 20)
    assert api.get("/jobs/waiting").json()["status"] == "queued"

    api.clock.advance(10)  # window active
    api.post("/admin/tick")
    node = api.get("/nodes/n1").json()
    assert node["used_slots"] == 0
    head = api.get("/queue/head").json()
    assert head["blocked_reason"] == "maintenance_window"
    assert head["earliest_start_in_seconds"] == pytest.approx(50, abs=0.01)

    api.clock.advance(60)  # window over
    api.post("/admin/tick")
    assert api.get("/jobs/waiting").json()["status"] == "running"


def test_backfill_respects_window_and_head(api):
    configure(api, "n1", 6)
    submit(api, "a", 2, 100)
    submit(api, "head", 5, 10)
    reserve(api, "n1", 100, 200)

    submit(api, "short-bf", 4, 60)   # ends +60: safe, dispatched
    submit(api, "long-bf", 2, 120)   # crosses window: stays queued
    assert api.get("/jobs/short-bf").json()["status"] == "running"
    assert api.get("/jobs/long-bf").json()["status"] == "queued"

    head = api.get("/queue/head").json()
    assert head["head"]["job_id"] == "head"
    assert head["earliest_start_in_seconds"] == pytest.approx(200, abs=0.01)


def test_reservation_survives_app_restart(db_path, clock):
    app = create_app(db_path=db_path, start_background=False, clock=clock)
    with TestClient(app) as first:
        configure(first, "n1", 4)
        response = first.put(
            "/nodes/n1/maintenance",
            json={
                "starts_at": iso(clock, 100),
                "ends_at": iso(clock, 200),
            },
        )
        assert response.status_code == 201
        submit(first, "j", 2, 300)
        assert first.get("/jobs/j").json()["status"] == "queued"

    app2 = create_app(db_path=db_path, start_background=False, clock=clock)
    with TestClient(app2) as second:
        view = second.get("/nodes/n1/maintenance").json()
        assert view["starts_in_seconds"] == pytest.approx(100, abs=0.01)
        assert second.get("/jobs/j").json()["status"] == "queued"


def test_timezone_offset_accepted_and_normalised_to_utc(api):
    configure(api, "n1", 4)
    start = datetime.fromtimestamp(api.clock.now + 100, tz=timezone(timedelta(hours=8)))
    end = datetime.fromtimestamp(api.clock.now + 200, tz=timezone(timedelta(hours=8)))
    response = api.put(
        "/nodes/n1/maintenance",
        json={
            "starts_at": start.isoformat(),
            "ends_at": end.isoformat(),
        },
    )
    assert response.status_code == 201, response.text
    assert response.json()["starts_at"].endswith("Z")
