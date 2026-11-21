# Render Farm Slot Scheduler

A small FastAPI + SQLite scheduling API for a render farm. Queued jobs get
backfilled into CPU slots that are temporarily idle, while the queue head's
earliest start time is preserved. Operators can also reserve **maintenance
windows** (e.g. before swapping a node's disk): overlapping leases block the
reservation, the node stops dispatching while work is underway, and
scheduling resumes automatically afterwards.

Interactive API docs: **http://localhost:8000/docs**

---

## Run with Docker Compose

```bash
docker compose up --build -d
# then open http://localhost:8000/docs
```

The SQLite file lives in the `scheduler-data` volume
(`SCHEDULER_DB_PATH=/data/scheduler.db`). Override the host port with
`SCHEDULER_PORT=9000 docker compose up`, or the reaper interval with the
`SCHEDULER_TICK_SECONDS` environment variable.

Stop and keep data: `docker compose down`.
Reset all data: `docker compose down -v`.

Run the tests in a container:

```bash
docker build -f Dockerfile.test -t render-scheduler-tests .
docker run --rm render-scheduler-tests
```

### Local run without Docker

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:create_app --factory --reload
pytest -q
```

`app.main:create_app` is an application factory, so it must be started with
`--factory` (the Docker image already does this).

---

## Model

* **Node** — a machine with `capacity` CPU slots.
* **Job** — submitted with a client-provided `job_id`, `required_slots`, and
  `max_duration_seconds`. A job leases *all* of its slots on **one** node; a
  request is never split across nodes.
* **Lease** — a running job holds its slots from `started_at` until
  `lease_expires_at` (`started_at + max_duration_seconds`). It can be
  returned early, otherwise it expires automatically.
* **Queue** — jobs are ordered by submission (`seq`). Jobs that cannot start
  stay `queued`; they do not block jobs that fit except as described below.
* **MaintenanceWindow** — one unended row per node with `starts_at` /
  `ends_at`. Overlapping running leases block creation/rescheduling; while a
  window is unended the node cannot take a lease that intersects it.

### Scheduling policy

Every scheduling pass runs in a single `BEGIN IMMEDIATE` SQLite transaction:

1. Expire leases whose `lease_expires_at <= now`, releasing their slots.
2. If the queue head fits on some node right now, dispatch it (best fit:
   smallest node with enough free slots). Repeat.
3. If the head cannot start yet, compute its **earliest start time** from
   leases that are running right now, then walk the rest of the queue in
   order. A later job is dispatched only if it can start now on one node and
   its full lease ends by the head's earliest start — i.e. it provably does
   not delay the head. Jobs that fail this check keep their queue position.
4. If the head can never fit on any configured node, it has no finite start
   time to protect; it stays queued at position 1, and later jobs that fit
   may use idle slots (they run around the head, never put it further back).

Notes:

* Best-fit packing means a job takes the smallest node that can hold it, so
  large jobs keep large nodes available.
* A job larger than every node stays queued and its head estimate reports
  `insufficient_capacity`; increasing a node's capacity dispatches it.
* Node capacity can be changed at any time, but not below the slots currently
  leased on that node (`409`).

### Maintenance windows

A maintenance window reserves one whole node for a future interval (disk
swap, hardware work). The rules:

* Each node has **at most one unended window**. `PUT
  /nodes/{node_id}/maintenance` creates the window; calling it again before
  the start **reschedules** it (returns `200` instead of `201`). A window
  that has started can no longer be changed or cancelled (`409`).
* When the proposed window overlaps an **active lease**, the request fails
  with `409` and the body lists every blocking `job_id`; the transaction
  rolls back, so any existing reservation is preserved byte-for-byte. A
  lease expiring exactly at the window start does not overlap (half-open
  intervals touching at a point are allowed) — the latest a new lease may
  finish before a window is precisely at its start.
* Windows and leases check each other under the same `BEGIN IMMEDIATE`
  serialisation: whichever commits first constrains the other. A lease
  dispatched first participates in the next reservation check; a window
  reserved first forbids any dispatch that would overlap it, including
  backfills.
* From the window start until its end the node **dispatches nothing**; once
  it ends, the normal scheduling pass picks queued jobs again automatically
  (no manual re-enable).
* Head estimates and backfill both account for windows: a lease that cannot
  finish before an upcoming window is treated as starting only after the
  window end, and `blocked_reason` becomes `maintenance_window` when the
  window (rather than slot contention) is the operative blocker. The head's
  earliest start is still protected — a backfill can never push it later.
* Windows live in SQLite, so they survive process restarts and keep
  constraining dispatch afterwards. Ended windows remain in the history
  listing but no longer affect scheduling.
* SQL triggers independently reject any running lease whose
  `[started_at, expires_at)` intersects an unended window.

### Concurrency and failure handling

* In-process: every read-modify-write goes through a process-wide `RLock`.
* Across processes: `BEGIN IMMEDIATE` serialises scheduling passes
  (`busy_timeout=30s`), and the allocation `UPDATE ... WHERE status='queued'`
  is guarded by its row count.
* Database triggers reject any running row that would exceed node capacity,
  and any capacity shrink below current usage — a second safety net behind
  the scheduler's accounting.
* Each dispatch pass is a single transaction that includes the expiry sweep.
  If anything raises, the transaction rolls back: queue order, statuses, and
  occupancy are exactly as before, and the next tick/request retries. A
  failed dispatch never leaves a half-written allocation or a freed slot
  without its replacement.
* Releases are committed before the follow-up dispatch pass, so a dispatch
  failure cannot undo a successful release.
* Duplicate `job_id` submissions return `409`; the unique constraint is the
  final arbiter under concurrent submissions.
* Maintenance reservations run in the same `BEGIN IMMEDIATE` transactions as
  dispatch passes, so a reservation and a lease allocation can never commit
  overlapping intervals: the first to commit constrains the other. A failed
  reservation rolls back entirely, leaving the prior window untouched.
  SQLite triggers reject any lease row intersecting an unended window as a
  final safety net.

---

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/nodes` | Create or update a node's slot capacity |
| `GET` | `/nodes` | List nodes with used/free slots |
| `GET` | `/nodes/{node_id}` | One node's capacity and usage |
| `PUT` | `/nodes/{node_id}/maintenance` | Reserve/reschedule the node's maintenance window |
| `GET` | `/nodes/{node_id}/maintenance` | The node's current unended window |
| `DELETE` | `/nodes/{node_id}/maintenance` | Cancel an upcoming window |
| `GET` | `/maintenance-windows?status=` | List windows (newest first) |
| `POST` | `/jobs` | Submit a job (dispatches immediately if possible) |
| `GET` | `/jobs?status=` | List jobs in submission order |
| `GET` | `/jobs/{job_id}` | Status, assigned node, lease expiry, remaining time |
| `POST` | `/jobs/{job_id}/release` | Return a running lease early |
| `GET` | `/queue/head` | Queue head's estimated earliest start |
| `GET` | `/queue` | Queued jobs plus the head estimate |
| `POST` | `/admin/tick` | Expire due leases and re-dispatch now |
| `GET` | `/healthz` | Liveness + database check |

### Example walkthrough

```bash
# One node with six slots
curl -s -X POST localhost:8000/nodes \
  -H 'content-type: application/json' \
  -d '{"node_id":"farm-1","capacity":6}'

# A job that will hold 2 slots for two minutes
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"plate-001","required_slots":2,"max_duration_seconds":120}'

# Queue head needs 5 slots: it must wait for plate-001 to release its 2
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"plate-002","required_slots":5,"max_duration_seconds":30}'

# The head's ETA (about 120s away)
curl -s localhost:8000/queue/head

# A 2-slot / 5s backfill runs now: it is done long before the head can start
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"preview-001","required_slots":2,"max_duration_seconds":5}'

# A 4-slot / 30s job is *not* dispatched: it would still hold slots when the
# head could start, delaying it.
curl -s -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"job_id":"preview-002","required_slots":4,"max_duration_seconds":30}'

# Return the blocker early; plate-002 starts immediately
curl -s -X POST localhost:8000/jobs/plate-001/release
curl -s localhost:8000/jobs/plate-002   # status: running, node: farm-1
```

`GET /queue/head` returns:

```json
{
  "head": {"job_id": "plate-002", "required_slots": 5, "status": "queued",
           "queue_position": 1, "...": "..."},
  "earliest_start_at": "2026-11-20T12:02:00.123456Z",
  "earliest_start_in_seconds": 119.8,
  "blocked_reason": "waiting_for_free_slots"
}
```

`blocked_reason` is one of `waiting_for_free_slots`,
`maintenance_window` (a window delays the head more than slot contention
does), `insufficient_capacity` (no node is large enough), or
`no_nodes_configured`; it is `null` when the head is not blocked.

### Maintenance walkthrough

```bash
# A window overlapping a running lease is rejected; the blockers are named.
curl -s -X PUT localhost:8000/nodes/farm-1/maintenance \
  -H 'content-type: application/json' \
  -d '{"starts_at":"2026-11-21T10:00:00Z","ends_at":"2026-11-21T10:30:00Z"}'
# 409 -> {"detail": "...", "blocking_jobs": ["plate-001"], "window": {...}}

# Timestamps must carry an explicit timezone; a window after all current
# leases is accepted (201 on create, 200 when rescheduling the existing one).
curl -s -X PUT localhost:8000/nodes/farm-1/maintenance \
  -H 'content-type: application/json' \
  -d '{"starts_at":"2026-11-21T12:00:00Z","ends_at":"2026-11-21T12:30:00Z"}'

# While the window is upcoming, only leases that end by 12:00 are dispatched
# onto farm-1. Between 12:00 and 12:30 the node dispatches nothing; at 12:30
# queued jobs start automatically. Inspect or cancel before start:
curl -s localhost:8000/nodes/farm-1/maintenance
curl -s -X DELETE localhost:8000/nodes/farm-1/maintenance
curl -s 'localhost:8000/maintenance-windows?status=upcoming'
```

---

## Implementation map

```
app/db.py          SQLite schema, transactions, invariant triggers
app/scheduler.py   FIFO + backfill policy, leases, expiry, head estimate,
                   maintenance windows
app/models.py      Pydantic request/response models (drives /docs)
app/main.py        FastAPI routes, error mapping, background reaper
tests/             77 tests: policy, leases, rollback, concurrency, HTTP,
                   SQL invariants, maintenance windows
Dockerfile         production image (non-root, healthcheck)
Dockerfile.test    image that runs pytest
docker-compose.yml single service + named volume for the SQLite file
```

The background reaper wakes every `SCHEDULER_TICK_SECONDS` and runs the same
scheduling pass; every API read also runs a pass first, so the reported state
is current even with a long tick interval.
