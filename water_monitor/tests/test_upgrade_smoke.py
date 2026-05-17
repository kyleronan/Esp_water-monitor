"""Full legacy-install upgrade smoke test.

Simulates an install that has been running with legacy circuit IDs ('main',
'irrigation') and runs migration 023, then verifies the post-upgrade state
matches the requirements of the refactored routes.

Run: pytest water_monitor/tests/test_upgrade_smoke.py -v
"""
from __future__ import annotations

import pytest

from .conftest import make_db
from water_monitor.app.db_migrations import _migrate_023
from water_monitor.app.device_discovery import (
    load_circuit_entities,
    ROLE_PATTERNS,
)
from water_monitor.app.restore_utils import (
    normalize_restore_row,
    safe_insert_rows,
    restore_circuit_labels,
)

_PREFIX = "device_abc"


def _pre_migration_db():
    """DB pre-populated with legacy circuit IDs and realistic firmware entity IDs."""
    db = make_db()

    # Legacy circuit_entity_map entries — circuit column uses 'main'/'irrigation',
    # entity_id values use firmware suffixes '_main'/'_irr' (unchanged by migration).
    legacy_entities = [
        ("main",       "valve_entity",              f"valve.{_PREFIX}_main_water_valve"),
        ("main",       "flow_sensor",               f"sensor.{_PREFIX}_water_flow_rate_main"),
        ("main",       "pressure_sensor",           f"sensor.{_PREFIX}_water_pressure_main"),
        ("main",       "fault_reset_button",        f"button.{_PREFIX}_reset_safety_fault_main"),
        ("main",       "trickle_reset_button",      f"button.{_PREFIX}_reset_trickle_alert_main"),
        ("main",       "alert_high_flow_switch",    f"switch.{_PREFIX}_enable_high_flow_alert_main"),
        ("main",       "burst_threshold",           f"number.{_PREFIX}_burst_threshold_main"),
        ("main",       "pressure_drop_threshold",   f"number.{_PREFIX}_pressure_drop_threshold_main"),
        ("main",       "leak_test_duration_number", f"number.{_PREFIX}_leak_test_duration_main"),
        ("irrigation", "valve_entity",              f"valve.{_PREFIX}_irrigation_water_valve"),
        ("irrigation", "flow_sensor",               f"sensor.{_PREFIX}_water_flow_rate_irrigation"),
        ("irrigation", "fault_reset_button",        f"button.{_PREFIX}_reset_safety_fault_irr"),
        ("irrigation", "burst_threshold",           f"number.{_PREFIX}_burst_threshold_irr"),
    ]
    for circuit, role, entity_id in legacy_entities:
        db.execute(
            """INSERT OR REPLACE INTO circuit_entity_map
               (circuit, role, entity_id, entity_name, confirmed)
               VALUES (?, ?, ?, ?, 1)""",
            (circuit, role, entity_id, role),
        )

    # Legacy event rows
    db.execute(
        "INSERT INTO events (circuit, start_ts, end_ts, avg_flow_lpm, volume_litres) "
        "VALUES (?, ?, ?, ?, ?)",
        ("main", "2026-01-01T00:00:00", "2026-01-01T00:01:00", 5.0, 0.1),
    )
    db.execute(
        "INSERT INTO events (circuit, start_ts, end_ts, avg_flow_lpm, volume_litres) "
        "VALUES (?, ?, ?, ?, ?)",
        ("irrigation", "2026-01-02T00:00:00", "2026-01-02T00:01:00", 2.0, 0.05),
    )
    db.commit()
    return db


# =============================================================================
# 1. Migration 023 renames circuit columns correctly
# =============================================================================

def test_legacy_install_circuit_column_renamed_after_migration():
    db = _pre_migration_db()
    _migrate_023(db)

    circuits = {r[0] for r in
                db.execute("SELECT DISTINCT circuit FROM circuit_entity_map").fetchall()}
    assert "circuit_1" in circuits
    assert "circuit_2" in circuits
    assert "main" not in circuits
    assert "irrigation" not in circuits


def test_legacy_install_events_renamed_after_migration():
    db = _pre_migration_db()
    _migrate_023(db)
    legacy = db.execute(
        "SELECT COUNT(*) FROM events WHERE circuit IN ('main', 'irrigation')"
    ).fetchone()[0]
    assert legacy == 0


# =============================================================================
# 2. Entity IDs are NOT modified by migration (firmware IDs preserved)
# =============================================================================

def test_legacy_entity_ids_unchanged_after_migration():
    """Migration must only rename the 'circuit' column; entity_id values stay as-is."""
    db = _pre_migration_db()
    _migrate_023(db)

    ents_c1 = load_circuit_entities(db, "circuit_1")
    assert ents_c1["fault_reset_button"] == f"button.{_PREFIX}_reset_safety_fault_main"
    assert ents_c1["burst_threshold"] == f"number.{_PREFIX}_burst_threshold_main"

    ents_c2 = load_circuit_entities(db, "circuit_2")
    assert ents_c2["fault_reset_button"] == f"button.{_PREFIX}_reset_safety_fault_irr"
    assert ents_c2["burst_threshold"] == f"number.{_PREFIX}_burst_threshold_irr"


# =============================================================================
# 3. Post-migration: fault reset route uses discovered _main entity
# =============================================================================

def test_fault_reset_uses_main_entity_after_upgrade():
    """After migration, fault_reset_button lookup returns a _main entity_id (not _circuit_1)."""
    db = _pre_migration_db()
    _migrate_023(db)

    entities = load_circuit_entities(db, "circuit_1")
    fault_reset_entity = entities.get("fault_reset_button")

    assert fault_reset_entity is not None, "fault_reset_button must be discoverable after migration"
    assert "_main" in fault_reset_entity, (
        f"fault_reset_button entity_id should contain '_main' (firmware suffix): {fault_reset_entity!r}"
    )
    assert "_circuit_1" not in fault_reset_entity, (
        f"fault_reset_button entity_id must NOT contain '_circuit_1' (synthesized): {fault_reset_entity!r}"
    )


# =============================================================================
# 4. Post-migration: alert toggle uses discovered _main switch entity
# =============================================================================

def test_alert_toggle_uses_main_entity_after_upgrade():
    db = _pre_migration_db()
    _migrate_023(db)

    entities = load_circuit_entities(db, "circuit_1")
    switch_entity = entities.get("alert_high_flow_switch")

    assert switch_entity is not None
    assert "_main" in switch_entity
    assert "_circuit_1" not in switch_entity


# =============================================================================
# 5. Post-migration: threshold update accepts discovered _main number entity
# =============================================================================

def test_threshold_update_accepts_main_number_entity_after_upgrade():
    db = _pre_migration_db()
    _migrate_023(db)

    entities = load_circuit_entities(db, "circuit_1")
    # Simulate threshold allowlist build (same logic as device.py threshold_update)
    _THRESHOLD_ROLES = frozenset({
        "burst_threshold", "pressure_drop_threshold", "leak_pressure_threshold",
        "trickle_min_flow", "trickle_max_flow", "trickle_duration",
        "leak_test_duration_number", "leak_test_duration_sensor",
    })
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    burst_entity = f"number.{_PREFIX}_burst_threshold_main"

    assert burst_entity in allowed, (
        f"Discovered burst_threshold entity '{burst_entity}' must be in the threshold allowlist "
        f"after migration. Allowed set: {allowed}"
    )


def test_threshold_entity_is_number_domain_after_upgrade():
    """All discovered threshold entities must use the 'number.' domain (not input_number)."""
    db = _pre_migration_db()
    _migrate_023(db)

    entities = load_circuit_entities(db, "circuit_1")
    _THRESHOLD_ROLES = frozenset({
        "burst_threshold", "pressure_drop_threshold", "leak_pressure_threshold",
        "trickle_min_flow", "trickle_max_flow", "trickle_duration",
        "leak_test_duration_number", "leak_test_duration_sensor",
    })
    for role in _THRESHOLD_ROLES:
        entity_id = entities.get(role)
        if entity_id:
            assert entity_id.startswith("number."), (
                f"Threshold role '{role}' entity_id '{entity_id}' must start with 'number.'"
            )


# =============================================================================
# 6. circuit_labels created and seeded by migration
# =============================================================================

def test_circuit_labels_created_after_migration():
    db = _pre_migration_db()
    _migrate_023(db)
    labels = {r[0]: r[1] for r in
               db.execute("SELECT circuit_id, display_name FROM circuit_labels").fetchall()}
    assert "circuit_1" in labels
    assert "circuit_2" in labels


# =============================================================================
# 7. Restore parity: normalize_restore_row produces stable IDs
# =============================================================================

def test_restore_from_legacy_backup_normalizes_circuit():
    """Simulates restoring a quick_restore.json with legacy circuit='main' rows."""
    row = {
        "circuit": "main",
        "start_ts": "2026-01-01T00:00:00",
        "end_ts": "2026-01-01T00:01:00",
        "avg_flow_lpm": 5.0,
        "volume_litres": 0.1,
    }
    normalized = normalize_restore_row(row, "events")
    assert normalized["circuit"] == "circuit_1"
    assert normalized["avg_flow_lpm"] == 5.0  # other fields unchanged


def test_restore_from_legacy_backup_safe_insert():
    """safe_insert_rows normalizes circuit='irrigation' to 'circuit_2' on insert."""
    db = make_db()
    rows = [{
        "circuit": "irrigation",
        "start_ts": "2026-01-02T00:00:00",
        "end_ts": "2026-01-02T00:01:00",
        "avg_flow_lpm": 2.0,
        "volume_litres": 0.05,
    }]
    with db:
        safe_insert_rows(db, "events", rows)
    result = db.execute("SELECT circuit FROM events LIMIT 1").fetchone()
    assert result[0] == "circuit_2"
