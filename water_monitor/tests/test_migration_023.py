"""Migration 023 tests — circuit ID rename and circuit_labels creation.

Migration 023 renames 'main'→'circuit_1' and 'irrigation'→'circuit_2' in all
circuit-bearing tables, and creates the circuit_labels table with default values.

Run: pytest water_monitor/tests/test_migration_023.py -v
"""
from __future__ import annotations

import sqlite3

import pytest

from .conftest import make_db
from water_monitor.app.db_migrations import _migrate_023, _has_table


def _base_db() -> sqlite3.Connection:
    """Full production schema DB (uses make_db) — migration 023 requires all tables."""
    return make_db()


def _seed_legacy_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO circuit_entity_map (circuit, role, entity_id) VALUES (?, ?, ?)",
        ("main", "flow_sensor", "sensor.device_flow_main"),
    )
    conn.execute(
        "INSERT INTO circuit_entity_map (circuit, role, entity_id) VALUES (?, ?, ?)",
        ("irrigation", "flow_sensor", "sensor.device_flow_irr"),
    )
    conn.execute(
        "INSERT INTO training_state (circuit, state) VALUES (?, ?)",
        ("main", "active"),
    )
    conn.execute(
        "INSERT INTO events (circuit, start_ts) VALUES (?, ?)",
        ("main", "2026-01-01T00:00:00"),
    )
    conn.execute(
        "INSERT INTO events (circuit, start_ts) VALUES (?, ?)",
        ("irrigation", "2026-01-02T00:00:00"),
    )
    conn.commit()


# =============================================================================
# 1. Basic migration correctness
# =============================================================================

def test_migration_023_creates_circuit_labels_table():
    conn = _base_db()
    assert not _has_table(conn, "circuit_labels")
    _migrate_023(conn)
    assert _has_table(conn, "circuit_labels")


def test_migration_023_seeds_default_labels():
    conn = _base_db()
    _migrate_023(conn)
    labels = {r[0]: r[1] for r in
               conn.execute("SELECT circuit_id, display_name FROM circuit_labels").fetchall()}
    assert "circuit_1" in labels
    assert "circuit_2" in labels


def test_migration_023_renames_main_to_circuit1():
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    rows = conn.execute(
        "SELECT circuit FROM circuit_entity_map WHERE circuit = 'main'"
    ).fetchall()
    assert len(rows) == 0, "Legacy 'main' rows should have been renamed to 'circuit_1'"


def test_migration_023_renames_irrigation_to_circuit2():
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    rows = conn.execute(
        "SELECT circuit FROM circuit_entity_map WHERE circuit = 'irrigation'"
    ).fetchall()
    assert len(rows) == 0


def test_migration_023_renamed_values_are_stable_ids():
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    circuits = {r[0] for r in
                conn.execute("SELECT DISTINCT circuit FROM circuit_entity_map").fetchall()}
    assert circuits == {"circuit_1", "circuit_2"}


def test_migration_023_renames_events_table():
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    legacy = conn.execute(
        "SELECT COUNT(*) FROM events WHERE circuit IN ('main', 'irrigation')"
    ).fetchone()[0]
    assert legacy == 0


def test_migration_023_entity_ids_are_unchanged():
    """Entity IDs (firmware HA entity IDs) must NOT be modified by the migration."""
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    row = conn.execute(
        "SELECT entity_id FROM circuit_entity_map WHERE circuit = 'circuit_1'"
    ).fetchone()
    assert row is not None
    assert row[0] == "sensor.device_flow_main", (
        "Firmware entity IDs must not be renamed — only the 'circuit' column is updated"
    )


# =============================================================================
# 2. Idempotency
# =============================================================================

def test_migration_023_idempotent():
    """Running migration 023 twice must produce the same result with no error."""
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    _migrate_023(conn)  # second run — must be a no-op
    labels = conn.execute("SELECT COUNT(*) FROM circuit_labels").fetchone()[0]
    assert labels == 2, "Second migration run must not duplicate circuit_labels rows"


def test_migration_023_idempotent_no_legacy_rows_after_second_run():
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    _migrate_023(conn)
    legacy = conn.execute(
        "SELECT COUNT(*) FROM circuit_entity_map "
        "WHERE circuit IN ('main', 'irrigation')"
    ).fetchone()[0]
    assert legacy == 0


# =============================================================================
# 3. Empty DB / missing optional tables
# =============================================================================

def test_migration_023_handles_empty_circuit_tables():
    """Migration must succeed when circuit tables have no rows."""
    conn = _base_db()
    _migrate_023(conn)
    labels = conn.execute("SELECT COUNT(*) FROM circuit_labels").fetchone()[0]
    assert labels == 2


def test_migration_023_handles_present_optional_tables():
    """Optional tables (leak_test_history, fixture_clusters) are migrated without error."""
    conn = _base_db()
    # Full schema includes all optional tables
    assert _has_table(conn, "leak_test_history")
    assert _has_table(conn, "fixture_clusters")
    _migrate_023(conn)  # must not raise


def test_migration_023_handles_all_stable_ids_already_present():
    """If all rows already have stable IDs, migration must be a no-op."""
    conn = _base_db()
    conn.execute(
        "INSERT INTO circuit_entity_map (circuit, role, entity_id) VALUES (?, ?, ?)",
        ("circuit_1", "flow_sensor", "sensor.device_flow_main"),
    )
    conn.commit()
    _migrate_023(conn)
    count = conn.execute(
        "SELECT COUNT(*) FROM circuit_entity_map WHERE circuit = 'circuit_1'"
    ).fetchone()[0]
    assert count == 1


# =============================================================================
# 4. No legacy names remain after migration
# =============================================================================

def test_no_legacy_circuit_names_after_migration():
    conn = _base_db()
    _seed_legacy_rows(conn)
    _migrate_023(conn)
    for tbl in ("circuit_entity_map", "training_state", "events"):
        legacy = conn.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE circuit IN ('main', 'irrigation')"
        ).fetchone()[0]
        assert legacy == 0, (
            f"Table '{tbl}' still has legacy circuit values after migration 023"
        )
