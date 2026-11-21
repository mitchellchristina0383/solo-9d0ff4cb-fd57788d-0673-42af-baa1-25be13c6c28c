"""HTTP request/response models (Pydantic v2)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

STATUS_LITERAL = Literal["queued", "running", "completed", "expired"]

# Job and node identifiers are opaque strings; restricting the alphabet keeps
# URLs and logs readable without forbidding useful separators.
IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"

MAX_DURATION_SECONDS = 365 * 24 * 60 * 60

# Maintenance windows are meant for planned work, not multi-year blackouts.
MAX_WINDOW_SECONDS = 90 * 24 * 60 * 60


class NodeConfigRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(pattern=IDENTIFIER_PATTERN)
    capacity: int = Field(ge=1, le=1_000_000)


class NodeView(BaseModel):
    node_id: str
    capacity: int
    used_slots: int
    free_slots: int
    running_jobs: int


class JobSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=IDENTIFIER_PATTERN)
    required_slots: int = Field(ge=1, le=1_000_000)
    max_duration_seconds: float = Field(gt=0, le=MAX_DURATION_SECONDS)


class JobView(BaseModel):
    job_id: str
    required_slots: int
    max_duration_seconds: float
    status: STATUS_LITERAL
    queue_position: int | None
    submitted_at: str
    assigned_node: str | None
    started_at: str | None
    lease_expires_at: str | None
    ended_at: str | None
    remaining_seconds: float | None


class HeadEstimate(BaseModel):
    head: JobView | None
    earliest_start_at: str | None
    earliest_start_in_seconds: float | None
    blocked_reason: Literal[
        "waiting_for_free_slots",
        "maintenance_window",
        "insufficient_capacity",
        "no_nodes_configured",
    ] | None


class QueueView(HeadEstimate):
    jobs: list[JobView]


class SchedulerEvent(BaseModel):
    type: Literal["dispatched", "expired"]
    job_id: str
    node_id: str | None
    slots: int
    at: str | None
    lease_expires_at: str | None


class TickResponse(BaseModel):
    events: list[SchedulerEvent]


class ErrorResponse(BaseModel):
    detail: str


class MaintenanceReservationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    starts_at: datetime = Field(
        description="Window start (UTC). Must be strictly in the future."
    )
    ends_at: datetime = Field(
        description="Window end (UTC). Must be later than starts_at."
    )

    @field_validator("starts_at", "ends_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(
                "timestamp must include an explicit timezone offset, "
                "e.g. '2026-11-21T10:00:00Z'"
            )
        return value


class MaintenanceWindowView(BaseModel):
    window_id: int
    node_id: str
    status: Literal["upcoming", "active", "ended"]
    starts_at: str
    ends_at: str
    starts_in_seconds: float | None
    ends_in_seconds: float | None
    created_at: str
    updated_at: str


class MaintenanceConflictResponse(BaseModel):
    detail: str
    blocking_jobs: list[str]
    window: MaintenanceWindowView | None = None
