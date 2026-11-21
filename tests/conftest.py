"""Shared pytest fixtures and test clock."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import Store  # noqa: E402
from app.scheduler import Scheduler  # noqa: E402


def temporary_db_path() -> str:
    """SQLite ``:memory:`` is per-connection, so tests use throwaway files."""
    return str(Path(tempfile.mkdtemp(prefix="scheduler-test-")) / "scheduler.db")


class FakeClock:
    """Deterministic wall clock measured in seconds."""

    def __init__(self, start: float = 1_700_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def db_path() -> str:
    return temporary_db_path()


@pytest.fixture
def scheduler(db_path: str, clock: FakeClock) -> Scheduler:
    store = Store(db_path)
    return Scheduler(store, clock=clock, tick_seconds=0.01)
