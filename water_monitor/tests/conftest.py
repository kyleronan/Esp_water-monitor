"""Shared pytest fixtures for water_monitor tests.

Uses in-memory SQLite for fast, isolated tests.  The ``river`` library is
stubbed so tests can run in a plain Python environment without Docker.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import types
from typing import Dict, List, Optional

import pytest


# ==========================================================================
# River stub
# ==========================================================================
# ClusterEngine imports `from river import cluster, preprocessing` at the
# module level.  We install lightweight stubs before the first import so
# the test suite can run without the real river library.

class _StubDBSTREAM:
    """Single-cluster DBSTREAM stub.

    Every call to ``learn_one`` updates a single centre at river-id 0.
    Sufficient for gate / isolation tests that only ever need one fixture
    per circuit.
    """

    def __init__(self, **kw):
        self.centers: Dict = {}

    def learn_one(self, x: dict) -> None:
        # Create or overwrite the single centre with the most-recent point.
        # For gate tests this is fine — the gate reads the DB centroid, not
        # the in-memory DBSTREAM centre.
        self.centers[0] = dict(x)


class _StubScaler:
    """Identity StandardScaler — returns features unchanged.

    With no real scaling, raw feature values equal "scaled" values, which
    keeps threshold comparisons predictable in tests.
    """

    def learn_one(self, x: dict) -> None:
        pass

    def transform_one(self, x: dict) -> dict:
        return dict(x)


def _install_river_stub() -> None:
    """Install the river stub once; no-op if already present."""
    if "river.cluster" in sys.modules:
        return
    river_mod = types.ModuleType("river")
    cluster_mod = types.ModuleType("river.cluster")
    pre_mod = types.ModuleType("river.preprocessing")
    cluster_mod.DBSTREAM = _StubDBSTREAM
    pre_mod.StandardScaler = _StubScaler
    sys.modules["river"] = river_mod
    sys.modules["river.cluster"] = cluster_mod
    sys.modules["river.preprocessing"] = pre_mod


_install_river_stub()


# ==========================================================================
# In-memory SQLite helpers
# ==========================================================================

from water_monitor.app.database import _create_schema   # noqa: E402


def make_db() -> sqlite3.Connection:
    """Fresh in-memory DB with the full production schema."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    _create_schema(conn)
    return conn


# ==========================================================================
# ClusterEngine factory helpers
# ==========================================================================

from water_monitor.app.cluster_engine import ClusterEngine   # noqa: E402


class _FakeCircuit:
    def __init__(self, name: str) -> None:
        self.circuit = name
        self.display_name = name


class _FakeCfg:
    def __init__(self, circuit_names: List[str]) -> None:
        self.circuits = [_FakeCircuit(n) for n in circuit_names]


def make_engine(db: sqlite3.Connection,
                circuits: Optional[List[str]] = None) -> ClusterEngine:
    """Build a ClusterEngine wired to ``db`` for the given circuit list."""
    return ClusterEngine(db, _FakeCfg(circuits or ["main"]))


# ==========================================================================
# Canonical event builders
# ==========================================================================

def toilet_event(**overrides) -> dict:
    """Typical toilet event: ~6 L, 45 s, steady flow."""
    base = {
        "avg_flow_lpm":           8.0,
        "peak_flow_lpm":          9.0,
        "duration_seconds":       45.0,
        "volume_litres":          6.0,
        "pressure_delta_psi":     0.0,
        "has_pressure_transient": 0.0,
        "flow_variability":       0.05,
        "hour_sin":               0.0,
        "hour_cos":               1.0,
        "start_ts":               "2026-01-01T08:00:00",
        "end_ts":                 "2026-01-01T08:00:45",
    }
    base.update(overrides)
    return base


def shower_event(**overrides) -> dict:
    """Typical shower event: ~40 L, 5 min, moderate variable flow."""
    base = {
        "avg_flow_lpm":           8.0,
        "peak_flow_lpm":          10.0,
        "duration_seconds":       300.0,
        "volume_litres":          40.0,
        "pressure_delta_psi":     0.5,
        "has_pressure_transient": 1.0,
        "flow_variability":       0.2,
        "hour_sin":               0.0,
        "hour_cos":               1.0,
        "start_ts":               "2026-01-01T07:00:00",
        "end_ts":                 "2026-01-01T07:05:00",
    }
    base.update(overrides)
    return base


# ==========================================================================
# Pytest fixtures
# ==========================================================================

@pytest.fixture
def db():
    return make_db()


@pytest.fixture
def engine_main(db):
    return make_engine(db, ["main"])


@pytest.fixture
def engine_two_circuits(db):
    return make_engine(db, ["main", "irrigation"])


# ==========================================================================
# TrainingManager helpers (for test_training_state.py)
# ==========================================================================

from water_monitor.app.config import AddonConfig, CircuitConfig   # noqa: E402


class StubHaClient:
    """Records notify/set_state calls so tests can assert on them.

    Mirrors the subset of HaClient used by TrainingManager.  No network
    or async side effects.
    """

    def __init__(self) -> None:
        self.notifications: list[dict] = []
        self.states: list[dict] = []

    async def notify(self, title: str, message: str,
                     notification_id: Optional[str] = None) -> None:
        self.notifications.append({
            "title": title, "message": message,
            "notification_id": notification_id,
        })

    async def set_state(self, entity_id: str, state,
                        attrs: Optional[Dict] = None) -> None:
        self.states.append({
            "entity_id": entity_id, "state": state,
            "attrs": attrs or {},
        })


def make_cfg(circuits: Optional[List[str]] = None) -> AddonConfig:
    """Build a minimal AddonConfig with the named circuits.

    Default: ['main', 'irrigation'].  Used by test_training_state.py
    where the circuit lifecycle (start_calibration etc.) needs a real
    AddonConfig, not the bare _FakeCfg used by cluster_engine tests.
    """
    if circuits is None:
        circuits = ["main", "irrigation"]
    return AddonConfig(
        log_level="DEBUG",
        esp_device_name="test_device",
        circuits=[
            CircuitConfig(
                circuit=c,
                circuit_type="zone" if c == "irrigation" else "fixture",
            )
            for c in circuits
        ],
    )


@pytest.fixture
def ha() -> StubHaClient:
    return StubHaClient()


@pytest.fixture
def cfg() -> AddonConfig:
    return make_cfg()


# ==========================================================================
# DEV_MODE — bypass ingress IP guard for all tests
# ==========================================================================

@pytest.fixture(autouse=True, scope="session")
def _dev_mode(tmp_path_factory):
    """Set DEV_MODE=true for the entire test session.

    The ingress IP guard (added in main.py) rejects requests that do not
    come from 172.30.32.2. Test clients connect from 127.0.0.1, so every
    test would fail without this override.
    """
    import os
    os.environ["DEV_MODE"] = "true"
    yield
    os.environ.pop("DEV_MODE", None)
