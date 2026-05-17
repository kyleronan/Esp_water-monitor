"""Restore parity tests — safe_insert_rows, normalize_restore_row, restore_circuit_labels.

These tests verify that both backup and setup restore paths use identical
normalization so that a restore from a legacy backup (with circuit="main")
always ends up with circuit="circuit_1" in the DB.

Run: pytest water_monitor/tests/test_restore_parity.py -v
"""
from __future__ import annotations

import pytest

from .conftest import make_db
from water_monitor.app.restore_utils import (
    normalize_restore_row,
    restore_circuit_labels,
    safe_insert_rows,
    RESTORABLE_TABLES,
)
from water_monitor.app.database import load_circuit_labels, upsert_circuit_label


_CREATE_CIRCUIT_LABELS = """
    CREATE TABLE IF NOT EXISTS circuit_labels (
        circuit_id   TEXT PRIMARY KEY,
        display_name TEXT NOT NULL
    )
"""


def make_db_with_labels():
    """make_db() + the circuit_labels table (added in migration 023)."""
    db = make_db()
    db.execute(_CREATE_CIRCUIT_LABELS)
    db.commit()
    return db


# =============================================================================
# 1. normalize_restore_row
# =============================================================================

def test_normalize_restore_row_main_to_circuit1():
    row = {"circuit": "main", "avg_flow_lpm": 5.0}
    result = normalize_restore_row(row, "events")
    assert result["circuit"] == "circuit_1"


def test_normalize_restore_row_irrigation_to_circuit2():
    row = {"circuit": "irrigation", "volume_litres": 10.0}
    result = normalize_restore_row(row, "events")
    assert result["circuit"] == "circuit_2"


def test_normalize_restore_row_stable_id_unchanged():
    row = {"circuit": "circuit_1", "volume_litres": 10.0}
    result = normalize_restore_row(row, "events")
    assert result["circuit"] == "circuit_1"


def test_normalize_restore_row_no_circuit_column_passes():
    row = {"device_id": "abc", "ha_url": "http://homeassistant.local"}
    result = normalize_restore_row(row, "device_config")
    assert result == row


def test_normalize_restore_row_non_circuit_table_unchanged():
    row = {"circuit": "main"}  # 'device_config' is not in CIRCUIT_TABLES
    result = normalize_restore_row(row, "device_config")
    assert result["circuit"] == "main"


def test_normalize_restore_row_does_not_mutate_original():
    row = {"circuit": "main", "val": 1}
    normalize_restore_row(row, "events")
    assert row["circuit"] == "main"  # original untouched


# =============================================================================
# 2. safe_insert_rows — table allowlist
# =============================================================================

def test_safe_insert_rows_rejects_unknown_table():
    db = make_db()
    with pytest.raises(ValueError, match="not in the restore allowlist"):
        safe_insert_rows(db, "arbitrary_table", [{"col": "val"}])


def test_safe_insert_rows_rejects_sql_injection_attempt():
    db = make_db()
    with pytest.raises(ValueError):
        safe_insert_rows(db, "events; DROP TABLE events--", [{"col": "val"}])


def test_safe_insert_rows_raises_on_zero_valid_columns():
    db = make_db()
    with pytest.raises(ValueError, match="no valid columns"):
        safe_insert_rows(db, "events", [{"nonexistent_col_xyz": "val"}])


def test_safe_insert_rows_returns_zero_for_empty_list():
    db = make_db()
    count = safe_insert_rows(db, "events", [])
    assert count == 0


# =============================================================================
# 3. safe_insert_rows — normalization on insert
# =============================================================================

def test_safe_insert_rows_normalizes_circuit_main():
    db = make_db()
    rows = [{
        "circuit": "main",
        "start_ts": "2026-01-01T00:00:00",
        "end_ts": "2026-01-01T00:01:00",
        "avg_flow_lpm": 5.0,
        "volume_litres": 0.1,
    }]
    with db:
        safe_insert_rows(db, "events", rows)
    result = db.execute("SELECT circuit FROM events LIMIT 1").fetchone()
    assert result[0] == "circuit_1"


def test_safe_insert_rows_normalizes_circuit_irrigation():
    db = make_db()
    rows = [{
        "circuit": "irrigation",
        "start_ts": "2026-01-01T00:00:00",
        "end_ts": "2026-01-01T00:01:00",
        "avg_flow_lpm": 2.0,
        "volume_litres": 0.05,
    }]
    with db:
        safe_insert_rows(db, "events", rows)
    result = db.execute("SELECT circuit FROM events LIMIT 1").fetchone()
    assert result[0] == "circuit_2"


def test_safe_insert_rows_filters_unknown_columns():
    """Columns not in the live schema are silently dropped (not an error)."""
    db = make_db()
    rows = [{
        "circuit": "circuit_1",
        "start_ts": "2026-01-01T00:00:00",
        "end_ts": "2026-01-01T00:01:00",
        "avg_flow_lpm": 5.0,
        "volume_litres": 0.1,
        "future_firmware_column": "ignored",
    }]
    with db:
        count = safe_insert_rows(db, "events", rows)
    assert count == 1


def test_safe_insert_rows_returns_row_count():
    db = make_db()
    rows = [
        {"circuit": "circuit_1", "start_ts": "2026-01-01T00:00:00",
         "end_ts": "2026-01-01T00:01:00", "avg_flow_lpm": 5.0, "volume_litres": 0.1},
        {"circuit": "circuit_2", "start_ts": "2026-01-01T00:02:00",
         "end_ts": "2026-01-01T00:03:00", "avg_flow_lpm": 2.0, "volume_litres": 0.05},
    ]
    with db:
        count = safe_insert_rows(db, "events", rows)
    assert count == 2


# =============================================================================
# 4. restore_circuit_labels
# =============================================================================

def test_restore_circuit_labels_writes_entries():
    db = make_db_with_labels()
    payload = {
        "circuits": [
            {"circuit_id": "circuit_1", "display_name": "Zone A"},
            {"circuit_id": "circuit_2", "display_name": "North Garden"},
        ]
    }
    restore_circuit_labels(db, payload)
    labels = load_circuit_labels(db)
    assert labels["circuit_1"] == "Zone A"
    assert labels["circuit_2"] == "North Garden"


def test_restore_circuit_labels_seeds_defaults_for_legacy_backup():
    """When 'circuits' key is absent, defaults are seeded if no labels exist."""
    db = make_db_with_labels()
    restore_circuit_labels(db, {})
    labels = load_circuit_labels(db)
    assert "circuit_1" in labels
    assert "circuit_2" in labels


def test_restore_circuit_labels_skips_seeding_when_labels_exist():
    """If labels already exist, missing 'circuits' key does not overwrite them."""
    db = make_db_with_labels()
    upsert_circuit_label(db, "circuit_1", "Existing Label")
    restore_circuit_labels(db, {})
    labels = load_circuit_labels(db)
    assert labels["circuit_1"] == "Existing Label"


def test_restore_circuit_labels_ignores_entries_with_missing_fields():
    """Entries with empty circuit_id or display_name are skipped gracefully."""
    db = make_db_with_labels()
    payload = {
        "circuits": [
            {"circuit_id": "circuit_1", "display_name": "Valid"},
            {"circuit_id": "",          "display_name": "No ID"},
            {"circuit_id": "circuit_2", "display_name": ""},
        ]
    }
    restore_circuit_labels(db, payload)
    labels = load_circuit_labels(db)
    assert labels.get("circuit_1") == "Valid"
    assert "circuit_2" not in labels


# =============================================================================
# 5. RESTORABLE_TABLES allowlist integrity
# =============================================================================

def test_restorable_tables_is_frozenset():
    assert isinstance(RESTORABLE_TABLES, frozenset)


def test_restorable_tables_includes_critical_tables():
    critical = {"events", "circuit_entity_map", "circuit_profile", "training_state",
                "alert_config", "leak_test_schedule", "fixtures"}
    assert critical.issubset(RESTORABLE_TABLES)


def test_restorable_tables_excludes_schema_tables():
    """SQLite internal and migration tracking tables must not be restorable."""
    forbidden = {"sqlite_master", "schema_migrations", "sqlite_sequence"}
    assert not forbidden.intersection(RESTORABLE_TABLES)
