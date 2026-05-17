"""Device route logic tests.

Tests the data-layer logic used by device.py routes without importing FastAPI.
FastAPI is not available in the test environment, so these tests exercise the
circuit_entity_map lookup, threshold allowlist, and alert role derivation
directly using the same functions the routes call at runtime.

Run: pytest water_monitor/tests/test_device_routes.py -v
"""
from __future__ import annotations

import sqlite3

import pytest

from .conftest import make_db
from water_monitor.app.device_discovery import (
    load_circuit_entities,
    ROLE_PATTERNS,
)

# Inlined from device.py (cannot import device.py — it requires FastAPI).
# These must be kept in sync with the constants there.
_THRESHOLD_ROLES: frozenset = frozenset({
    "leak_test_duration_number",
    "leak_test_duration_sensor",
    "burst_threshold",
    "pressure_drop_threshold",
    "leak_pressure_threshold",
    "trickle_min_flow",
    "trickle_max_flow",
    "trickle_duration",
})
VALID_ALERT_TYPES: frozenset = frozenset({
    "high_flow", "trickle", "pressure_drop", "leak_test",
})


# ─── helpers ───────────────────────────────────────────────────────────────


_PREFIX = "device_abc"


def _seed_entity(db: sqlite3.Connection, circuit: str, role: str, entity_id: str) -> None:
    db.execute(
        """INSERT OR REPLACE INTO circuit_entity_map
           (circuit, role, entity_id, entity_name, confirmed)
           VALUES (?, ?, ?, ?, 1)""",
        (circuit, role, entity_id, role),
    )
    db.commit()


def _seed_standard_entities(db: sqlite3.Connection) -> None:
    """Populate circuit_entity_map with realistic firmware entity IDs for both circuits."""
    p = _PREFIX
    entries = [
        # circuit_1 — uses _main suffix
        ("circuit_1", "fault_reset_button",         f"button.{p}_reset_safety_fault_main"),
        ("circuit_1", "trickle_reset_button",        f"button.{p}_reset_trickle_alert_main"),
        ("circuit_1", "alert_high_flow_switch",      f"switch.{p}_enable_high_flow_alert_main"),
        ("circuit_1", "alert_pressure_drop_switch",  f"switch.{p}_enable_pressure_drop_alert_main"),
        ("circuit_1", "alert_trickle_switch",        f"switch.{p}_enable_trickle_alert_main"),
        ("circuit_1", "alert_leak_test_switch",      f"switch.{p}_enable_leak_test_alert_main"),
        ("circuit_1", "burst_threshold",             f"number.{p}_burst_threshold_main"),
        ("circuit_1", "pressure_drop_threshold",     f"number.{p}_pressure_drop_threshold_main"),
        ("circuit_1", "trickle_min_flow",            f"number.{p}_trickle_flow_min_threshold_main"),
        ("circuit_1", "trickle_max_flow",            f"number.{p}_trickle_flow_max_threshold_main"),
        ("circuit_1", "trickle_duration",            f"number.{p}_trickle_flow_alert_duration_main"),
        ("circuit_1", "leak_test_duration_number",   f"number.{p}_leak_test_duration_main"),
        # circuit_2 — uses _irr suffix
        ("circuit_2", "fault_reset_button",          f"button.{p}_reset_safety_fault_irr"),
        ("circuit_2", "trickle_reset_button",        f"button.{p}_reset_trickle_alert_irr"),
        ("circuit_2", "alert_high_flow_switch",      f"switch.{p}_enable_high_flow_alert_irr"),
        ("circuit_2", "burst_threshold",             f"number.{p}_burst_threshold_irr"),
    ]
    for circuit, role, entity_id in entries:
        _seed_entity(db, circuit, role, entity_id)


# =============================================================================
# 1. load_circuit_entities — basic lookups
# =============================================================================

def test_load_circuit_entities_returns_seeded_roles():
    db = make_db()
    _seed_standard_entities(db)
    ents = load_circuit_entities(db, "circuit_1")
    assert ents["fault_reset_button"] == f"button.{_PREFIX}_reset_safety_fault_main"
    assert ents["burst_threshold"] == f"number.{_PREFIX}_burst_threshold_main"


def test_load_circuit_entities_empty_when_no_rows():
    db = make_db()
    ents = load_circuit_entities(db, "circuit_1")
    assert ents == {}


def test_load_circuit_entities_circuit2_uses_irr_suffix():
    db = make_db()
    _seed_standard_entities(db)
    ents = load_circuit_entities(db, "circuit_2")
    assert "_irr" in ents["fault_reset_button"]
    assert "_irr" in ents["burst_threshold"]


# =============================================================================
# 2. Threshold allowlist — entity_id must be in discovered roles
# =============================================================================

def test_threshold_allowlist_accepts_discovered_number():
    db = make_db()
    _seed_standard_entities(db)
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    entity_id = f"number.{_PREFIX}_burst_threshold_main"
    assert entity_id in allowed


def test_threshold_allowlist_rejects_unknown_entity():
    db = make_db()
    _seed_standard_entities(db)
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    assert "number.some_random_entity" not in allowed


def test_threshold_allowlist_rejects_input_number():
    db = make_db()
    _seed_standard_entities(db)
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    assert "input_number.some_helper" not in allowed


def test_threshold_allowlist_rejects_sensor():
    db = make_db()
    _seed_standard_entities(db)
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    assert f"sensor.{_PREFIX}_flow_main" not in allowed


def test_threshold_allowlist_rejects_switch():
    db = make_db()
    _seed_standard_entities(db)
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    assert f"switch.{_PREFIX}_enable_high_flow_alert_main" not in allowed


def test_threshold_allowlist_rejects_button():
    db = make_db()
    _seed_standard_entities(db)
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    assert f"button.{_PREFIX}_reset_safety_fault_main" not in allowed


# =============================================================================
# 3. Alert role derivation — role name must match discovered switch key
# =============================================================================

def test_alert_role_for_high_flow_resolves_from_entities():
    db = make_db()
    _seed_standard_entities(db)
    entities = load_circuit_entities(db, "circuit_1")
    alert_type = "high_flow"
    role = f"alert_{alert_type}_switch"
    entity_id = entities.get(role)
    assert entity_id is not None
    assert "enable_high_flow" in entity_id


def test_alert_role_returns_none_when_not_discovered():
    db = make_db()
    entities = load_circuit_entities(db, "circuit_1")
    entity_id = entities.get("alert_high_flow_switch")
    assert entity_id is None


def test_valid_alert_types_all_have_role_in_role_patterns():
    """Every VALID_ALERT_TYPE must correspond to a role in ROLE_PATTERNS."""
    for circuit in ("circuit_1", "circuit_2"):
        patterns = ROLE_PATTERNS[circuit]
        for alert_type in VALID_ALERT_TYPES:
            role = f"alert_{alert_type}_switch"
            assert role in patterns, (
                f"alert_type '{alert_type}' → role '{role}' missing from "
                f"ROLE_PATTERNS['{circuit}']. alert_toggle() will always return 400."
            )


# =============================================================================
# 4. Entity IDs in circuit_entity_map must NOT contain _circuit_1 / _circuit_2
# =============================================================================

def test_no_discovered_entity_uses_circuit_id_suffix():
    """Firmware entity IDs use _main/_irr, never _circuit_1/_circuit_2.

    This is the critical regression class: if any discovered entity_id contains
    _circuit_1 or _circuit_2, it was synthesized from the stable circuit ID
    rather than discovered from firmware — and the HA call will fail silently.
    """
    db = make_db()
    _seed_standard_entities(db)
    for circuit in ("circuit_1", "circuit_2"):
        entities = load_circuit_entities(db, circuit)
        for role, entity_id in entities.items():
            assert "_circuit_1" not in entity_id, (
                f"Entity ID for {circuit}/{role} contains '_circuit_1': {entity_id!r}"
            )
            assert "_circuit_2" not in entity_id, (
                f"Entity ID for {circuit}/{role} contains '_circuit_2': {entity_id!r}"
            )


def test_fault_reset_entity_id_does_not_use_circuit_id_suffix():
    db = make_db()
    _seed_standard_entities(db)
    ents_c1 = load_circuit_entities(db, "circuit_1")
    ents_c2 = load_circuit_entities(db, "circuit_2")
    assert "_circuit_1" not in ents_c1.get("fault_reset_button", "")
    assert "_circuit_2" not in ents_c2.get("fault_reset_button", "")


def test_alert_toggle_entity_id_does_not_use_circuit_id_suffix():
    db = make_db()
    _seed_standard_entities(db)
    ents = load_circuit_entities(db, "circuit_1")
    assert "_circuit_1" not in ents.get("alert_high_flow_switch", "")


# =============================================================================
# 5. _THRESHOLD_ROLES — all role names exist in ROLE_PATTERNS
# =============================================================================

def test_threshold_roles_match_role_patterns():
    """Every _THRESHOLD_ROLES entry must appear in ROLE_PATTERNS for both circuits."""
    for circuit in ("circuit_1", "circuit_2"):
        patterns = ROLE_PATTERNS[circuit]
        for role in _THRESHOLD_ROLES:
            assert role in patterns, (
                f"_THRESHOLD_ROLES entry '{role}' is absent from "
                f"ROLE_PATTERNS['{circuit}']. Threshold update for this role "
                f"will always return 403."
            )


# =============================================================================
# 6. Reset / alert lookup returns None when not seeded → correct 400 path
# =============================================================================

def test_missing_fault_reset_button_returns_none():
    db = make_db()
    # Do NOT seed fault_reset_button
    _seed_entity(db, "circuit_1", "flow_sensor", "sensor.dev_flow_main")
    entities = load_circuit_entities(db, "circuit_1")
    assert entities.get("fault_reset_button") is None


def test_missing_trickle_reset_button_returns_none():
    db = make_db()
    entities = load_circuit_entities(db, "circuit_1")
    assert entities.get("trickle_reset_button") is None
