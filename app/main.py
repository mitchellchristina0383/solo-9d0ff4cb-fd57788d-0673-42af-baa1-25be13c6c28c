"""FastAPI application for the render-farm slot scheduler."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, Query, Request, status
from fastapi.responses import JSONResponse

from .db import Store
from .models import (
    ErrorResponse,
    HeadEstimate,
    JobSubmitRequest,
    JobView,
    NodeConfigRequest,
    NodeView,
    QueueView,
    SchedulerEvent,
    TickResponse,
)
from .scheduler import Scheduler, SchedulerError

logger = logging.getLogger("render_scheduler")

DEFAULT_DB_PATH = "data/scheduler.db"


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:  # pragma: no cover - config error path
        logger.warning("ignoring invalid %s=%r; using %s", name, raw, default)
        return default


def create_app(
    db_path: str | None = None,
    tick_seconds: float | None = None,
    start_background: bool = True,
    clock=None,
) -> FastAPI:
    resolved_db = db_path or os.environ.get("SCHEDULER_DB_PATH", DEFAULT_DB_PATH)
    resolved_tick = (
        _env_float("SCHEDULER_TICK_SECONDS", 1.0)
        if tick_seconds is None
        else tick_seconds
    )
    store = Store(resolved_db)
    scheduler = Scheduler(
        store,
        clock=clock or time.time,
        tick_seconds=resolved_tick,
    )

    async def _lease_reaper() -> None:
        """Release leases whose max duration elapsed, then re-dispatch."""
        while True:
            await asyncio.sleep(max(resolved_tick, 0.01))
            try:
                await asyncio.to_thread(scheduler.tick)
            except Exception:  # pragma: no cover - defensive
                logger.exception("lease reaper tick failed")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        task = (
            asyncio.create_task(_lease_reaper()) if start_background else None
        )
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    app = FastAPI(
        title="Render Farm Slot Scheduler",
        version="1.0.0",
        description=(
            "Backfills idle CPU slots for queued render jobs while preserving "
            "queue order. Every job leases all of its slots on one node; "
            "leases are returned explicitly or expire automatically."
        ),
        lifespan=lifespan,
    )
    app.state.scheduler = scheduler
    app.state.store = store

    @app.exception_handler(SchedulerError)
    async def scheduler_error_handler(_request: Request, exc: SchedulerError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": str(exc)},
        )

    @app.get(
        "/healthz",
        tags=["system"],
        summary="Liveness and database check",
    )
    def healthz() -> dict:
        store.ping()
        return {"status": "ok"}

    # -- nodes ----------------------------------------------------------

    @app.post(
        "/nodes",
        response_model=NodeView,
        status_code=status.HTTP_200_OK,
        tags=["nodes"],
        summary="Create or update a node's slot capacity",
        responses={409: {"model": ErrorResponse}},
    )
    def configure_node(payload: NodeConfigRequest) -> NodeView:
        return NodeView(**scheduler.configure_node(payload.node_id, payload.capacity))

    @app.get(
        "/nodes",
        response_model=list[NodeView],
        tags=["nodes"],
        summary="List nodes with current slot usage",
    )
    def list_nodes() -> list[NodeView]:
        return [NodeView(**node) for node in scheduler.list_nodes()]

    @app.get(
        "/nodes/{node_id}",
        response_model=NodeView,
        tags=["nodes"],
        summary="Get one node's capacity and usage",
        responses={404: {"model": ErrorResponse}},
    )
    def get_node(node_id: str) -> NodeView:
        return NodeView(**scheduler.get_node(node_id))

    # -- jobs -----------------------------------------------------------

    @app.post(
        "/jobs",
        response_model=JobView,
        status_code=status.HTTP_201_CREATED,
        tags=["jobs"],
        summary="Submit a job and attempt immediate dispatch",
        responses={409: {"model": ErrorResponse}},
    )
    def submit_job(payload: JobSubmitRequest) -> JobView:
        job = scheduler.submit_job(
            job_id=payload.job_id,
            required_slots=payload.required_slots,
            max_duration_seconds=payload.max_duration_seconds,
        )
        return JobView(**job)

    @app.get(
        "/jobs",
        response_model=list[JobView],
        tags=["jobs"],
        summary="List jobs in submission order",
    )
    def list_jobs(
        status_filter: Literal["queued", "running", "completed", "expired"]
        | None = Query(default=None, alias="status"),
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> list[JobView]:
        return [
            JobView(**job)
            for job in scheduler.list_jobs(status=status_filter, limit=limit)
        ]

    @app.get(
        "/jobs/{job_id}",
        response_model=JobView,
        tags=["jobs"],
        summary="Get a job's status, node, and lease expiry",
        responses={404: {"model": ErrorResponse}},
    )
    def get_job(job_id: str) -> JobView:
        return JobView(**scheduler.get_job(job_id))

    @app.post(
        "/jobs/{job_id}/release",
        response_model=JobView,
        tags=["jobs"],
        summary="Return a running lease early and re-dispatch",
        responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    )
    def release_job(job_id: str) -> JobView:
        return JobView(**scheduler.release_job(job_id))

    # -- queue ----------------------------------------------------------

    @app.get(
        "/queue/head",
        response_model=HeadEstimate,
        tags=["queue"],
        summary="Estimated earliest start of the queue head",
    )
    def queue_head() -> HeadEstimate:
        return HeadEstimate(**scheduler.head_estimate())

    @app.get(
        "/queue",
        response_model=QueueView,
        tags=["queue"],
        summary="Queue contents plus the head estimate",
    )
    def queue(limit: int = Query(default=200, ge=1, le=1000)) -> QueueView:
        return QueueView(**scheduler.queue_state(limit=limit))

    @app.post(
        "/admin/tick",
        response_model=TickResponse,
        tags=["system"],
        summary="Expire due leases and re-dispatch now",
    )
    def tick() -> TickResponse:
        events = scheduler.tick()
        return TickResponse(events=[SchedulerEvent(**e) for e in events])

    return app
