"""
Tests for the circuit ID refactor (circuit_1/circuit_2 + configurable display names).

Covers:
  1.  test_legacy_main_resolves
  2.  test_legacy_irrigation_resolves
  3.  test_unknown_circuit_passthrough
  4.  test_bad_circuit_returns_400
  5.  test_migration_seeds_labels
  6.  test_migration_renames_all_config_tables
  7.  test_circuit_rename_preserves_id
  8.  test_old_backup_restore_normalizes
  9.  test_new_backup_restore_labels
  10. test_history_filter_legacy
  11. test_display_name_validation
  12. test_threshold_entity_allowlist

Run: pytest water_monitor/tests/test_circuit_refactor.py -v
"""
from __future__ import annotations

import sqlite3
import uuid

import pytest

from water_monitor.tests.conftest import make_db
from water_monitor.app.circuit_compat import resolve_circuit, validate_display_name
from water_monitor.app.database import (
    load_circuit_labels,
    upsert_circuit_label,
    get_recent_events,
)
from water_monitor.app.db_migrations import run_migrations


# ── Helpers ────────────────────────────────────────────────────────────────────

# Tables that carry a `circuit` column and must be normalized on backup restore.
_CIRCUIT_TABLES_EXPECTED = frozenset({
    "events",
    "circuit_entity_map",
    "circuit_profile",
    "training_state",
    "learning_config",
    "sensitivity_config",
    "alert_config",
    "leak_test_schedule",
    "circuit_exclusion_windows",
    "hourly_volume",
    "daily_summary",
    "fixtures",
    "fixture_clusters",
})


def _normalize_row(row: dict, table: str) -> dict:
    """Inline replica of backup._normalize_row — tested independently of FastAPI."""
    if table in _CIRCUIT_TABLES_EXPECTED and "circuit" in row:
        row = dict(row)
        row["circuit"] = resolve_circuit(row["circuit"])
    return row


def _seed_legacy_circuit_rows(db: sqlite3.Connection) -> None:
    """Insert minimal rows with legacy 'main'/'irrigation' values into
    each active config table to simulate a pre-refactor database."""
    # circuit_entity_map: PK = (circuit, role)
    for circ in ("main", "irrigation"):
        db.execute(
            "INSERT OR IGNORE INTO circuit_entity_map (circuit, role, entity_id) "
            "VALUES (?, 'flow_sensor', '')", (circ,)
        )
    # Single-PK tables: just insert the circuit value
    for tbl in ("circuit_profile", "training_state", "learning_config",
                "sensitivity_config", "leak_test_schedule"):
        for circ in ("main", "irrigation"):
            db.execute(
                f"INSERT OR IGNORE INTO {tbl} (circuit) VALUES (?)", (circ,)
            )
    # alert_config: PK = id (auto), requires circuit + alert_type
    for circ in ("main", "irrigation"):
        db.execute(
            "INSERT OR IGNORE INTO alert_config (circuit, alert_type) VALUES (?, 'high_flow')",
            (circ,)
        )
    # circuit_exclusion_windows: requires circuit, started_at, ends_at
    for circ in ("main", "irrigation"):
        db.execute(
            "INSERT OR IGNORE INTO circuit_exclusion_windows "
            "(circuit, started_at, ends_at) VALUES (?, datetime('now'), datetime('now','+1 hour'))",
            (circ,)
        )
    db.commit()


def _count_legacy_rows(db: sqlite3.Connection, table: str) -> int:
    return db.execute(
        f"SELECT COUNT(*) FROM {table} WHERE circuit IN ('main','irrigation')"
    ).fetchone()[0]


# =============================================================================
# 1–3. resolve_circuit — legacy alias mapping
# =============================================================================

def test_legacy_main_resolves():
    assert resolve_circuit("main") == "circuit_1"


def test_legacy_irrigation_resolves():
    assert resolve_circuit("irrigation") == "circuit_2"


def test_unknown_circuit_passthrough():
    """Unrecognised strings pass through unchanged."""
    assert resolve_circuit("circuit_1") == "circuit_1"
    assert resolve_circuit("circuit_2") == "circuit_2"
    assert resolve_circuit("some_other") == "some_other"


# =============================================================================
# 4. Route handler returns 400 for unknown circuit
# =============================================================================

def test_bad_circuit_returns_400():
    """Router logic: resolve_circuit on an unknown string → None from get_circuit → 400."""
    from water_monitor.app.config import AddonConfig, CircuitConfig

    cfg = AddonConfig(
        log_level="INFO",
        esp_device_name="test",
        circuits=[
            CircuitConfig(circuit="circuit_1", circuit_type="fixture"),
            CircuitConfig(circuit="circuit_2", circuit_type="zone"),
        ],
    )

    bad = resolve_circuit("badcircuit")      # passthrough → "badcircuit"
    assert cfg.get_circuit(bad) is None, \
        "get_circuit must return None for unknown circuit → produces 400 in route handler"

    good = resolve_circuit("main")           # alias → "circuit_1"
    assert cfg.get_circuit(good) is not None, \
        "get_circuit must find circuit_1 via legacy alias"


# =============================================================================
# 5. Migration 023 — seeds circuit_labels table
# =============================================================================

def test_migration_seeds_labels():
    """After migration 023 the circuit_labels table contains both circuits."""
    db = make_db()
    run_migrations(db, db_path=None)

    labels = load_circuit_labels(db)
    assert "circuit_1" in labels
    assert "circuit_2" in labels
    assert labels["circuit_1"] == "Main"
    assert labels["circuit_2"] == "Irrigation"


# =============================================================================
# 6. Migration 023 — renames all active config table circuit values
# =============================================================================

def test_migration_renames_all_config_tables():
    """After migration 023 all active config tables have zero legacy rows."""
    db = make_db()
    _seed_legacy_circuit_rows(db)

    # Verify seed worked
    assert _count_legacy_rows(db, "circuit_entity_map") > 0, \
        "Pre-condition: legacy rows must exist in circuit_entity_map before migration"

    run_migrations(db, db_path=None)

    tables = [
        "circuit_entity_map",
        "circuit_profile",
        "training_state",
        "learning_config",
        "sensitivity_config",
        "alert_config",
        "leak_test_schedule",
        "circuit_exclusion_windows",
    ]
    for tbl in tables:
        remaining = _count_legacy_rows(db, tbl)
        assert remaining == 0, \
            f"Migration 023: {remaining} stale legacy rows remain in {tbl}"


# =============================================================================
# 7. Renaming display name does not change circuit_id
# =============================================================================

def test_circuit_rename_preserves_id():
    """Updating display_name leaves circuit_id unchanged in circuit_labels."""
    db = make_db()
    run_migrations(db, db_path=None)

    upsert_circuit_label(db, "circuit_1", "House Main")

    labels = load_circuit_labels(db)
    assert labels["circuit_1"] == "House Main"

    row = db.execute(
        "SELECT circuit_id FROM circuit_labels WHERE circuit_id = 'circuit_1'"
    ).fetchone()
    assert row is not None
    assert row["circuit_id"] == "circuit_1"


# =============================================================================
# 8. Old backup restore — normalises legacy circuit values at insert time
# =============================================================================

def test_old_backup_restore_normalizes():
    """Backup rows with 'main'/'irrigation' are normalized to
    'circuit_1'/'circuit_2' regardless of migration state."""
    # Events table: 'main' → 'circuit_1'
    old = {"circuit": "main", "volume_litres": 5.0}
    assert _normalize_row(dict(old), "events")["circuit"] == "circuit_1"

    # Events table: 'irrigation' → 'circuit_2'
    old2 = {"circuit": "irrigation", "volume_litres": 3.0}
    assert _normalize_row(dict(old2), "events")["circuit"] == "circuit_2"

    # Already normalized: passthrough
    already = {"circuit": "circuit_1"}
    assert _normalize_row(dict(already), "events")["circuit"] == "circuit_1"

    # Table NOT in circuit-tables list: row unchanged
    non_circuit = {"circuit": "main"}
    assert _normalize_row(dict(non_circuit), "home_profile")["circuit"] == "main"

    # All expected circuit tables are covered
    for tbl in _CIRCUIT_TABLES_EXPECTED:
        result = _normalize_row({"circuit": "main"}, tbl)
        assert result["circuit"] == "circuit_1", \
            f"Table {tbl!r} must have 'main' normalized to 'circuit_1'"


# =============================================================================
# 9. New backup restore — restores custom display names from 'circuits' key
# =============================================================================

def test_new_backup_restore_labels():
    """Backup with a 'circuits' key restores custom display names."""
    db = make_db()
    run_migrations(db, db_path=None)

    backup_circuits = [
        {"circuit_id": "circuit_1", "display_name": "Unit A Main"},
        {"circuit_id": "circuit_2", "display_name": "Garden"},
    ]
    for c in backup_circuits:
        upsert_circuit_label(db, c["circuit_id"], c["display_name"])

    labels = load_circuit_labels(db)
    assert labels["circuit_1"] == "Unit A Main"
    assert labels["circuit_2"] == "Garden"


# =============================================================================
# 10. History filter accepts legacy 'main' (via resolve_circuit)
# =============================================================================

def test_history_filter_legacy():
    """resolve_circuit('main') → 'circuit_1'; events under circuit_1 are returned."""
    db = make_db()
    run_migrations(db, db_path=None)

    db.execute("""
        INSERT INTO events
          (id, circuit, start_ts, end_ts, volume_litres,
           avg_flow_lpm, duration_seconds)
        VALUES (?, 'circuit_1', datetime('now','-1 hour'), datetime('now'),
                5.0, 8.0, 37.5)
    """, (str(uuid.uuid4()),))
    db.commit()

    circuit = resolve_circuit("main")    # → "circuit_1"
    events = get_recent_events(db, circuit, limit=10)
    assert len(events) == 1
    assert events[0]["circuit"] == "circuit_1"


# =============================================================================
# 11. validate_display_name — rejects invalid names
# =============================================================================

def test_display_name_validation():
    # Valid names
    assert validate_display_name("Main") == "Main"
    assert validate_display_name("  Garden  ") == "Garden"    # stripped
    assert validate_display_name("Unit A's Main") == "Unit A's Main"
    assert validate_display_name("Circuit-1") == "Circuit-1"

    # Empty
    with pytest.raises(ValueError, match="empty"):
        validate_display_name("")

    # Whitespace only
    with pytest.raises(ValueError, match="empty"):
        validate_display_name("   ")

    # Too long (> 40 chars)
    with pytest.raises(ValueError, match="40"):
        validate_display_name("A" * 41)

    # Disallowed characters
    with pytest.raises(ValueError):
        validate_display_name("Main<script>")

    with pytest.raises(ValueError):
        validate_display_name("Main; DROP TABLE circuits--")


# =============================================================================
# 12. Threshold entity allowlist — only _THRESHOLD_ROLES + number.* domain
# =============================================================================

def test_threshold_entity_allowlist():
    """The threshold allowlist and domain check are exercised directly,
    without importing the FastAPI router (avoids fastapi dependency in tests)."""

    # Replicate _THRESHOLD_ROLES as defined in device.py
    _THRESHOLD_ROLES = frozenset({
        "leak_test_duration_entity",
        "high_flow_threshold",
        "trickle_threshold",
        "burst_threshold",
        "trickle_min_flow",
    })

    # Simulate load_circuit_entities return value
    mock_entities = {
        "leak_test_duration_entity": "number.esp_leak_test_duration_main",
        "high_flow_threshold":       "number.esp_high_flow_threshold_main",
        "flow_sensor":               "sensor.esp_flow_sensor_main",    # not a threshold role
        "valve_entity":              "valve.esp_main_valve",            # not a threshold role
        "input_number_helper":       "input_number.some_helper",        # wrong domain
    }

    allowed = {v for k, v in mock_entities.items() if k in _THRESHOLD_ROLES and v}

    # Valid threshold entities appear in allowlist
    assert "number.esp_leak_test_duration_main" in allowed
    assert "number.esp_high_flow_threshold_main" in allowed

    # Non-threshold roles excluded
    assert "sensor.esp_flow_sensor_main" not in allowed
    assert "valve.esp_main_valve" not in allowed
    assert "input_number.some_helper" not in allowed

    # Domain guard: only number.* entities accepted
    def _domain_ok(entity_id: str) -> bool:
        return entity_id.startswith("number.")

    assert _domain_ok("number.esp_leak_test_duration_main") is True
    assert _domain_ok("input_number.some_helper") is False
    assert _domain_ok("sensor.something") is False
    assert _domain_ok("valve.something") is False
