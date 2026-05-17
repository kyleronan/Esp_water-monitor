"""Template contract tests — static invariants for settings endpoint hardening
and template structure.

These tests use static analysis (string matching on source files) to catch
structural regressions without requiring a running server.

Run: pytest water_monitor/tests/test_template_contracts.py -v
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from .conftest import make_db
from water_monitor.app.device_discovery import (
    load_circuit_entities,
    ROLE_PATTERNS,
)

_TEMPLATES_DIR = Path(__file__).parent.parent / "app" / "templates"

# Inlined from settings.py (cannot import — requires FastAPI).
# Keep in sync with _SETTINGS_MUTABLE_ROLES in settings.py.
_SETTINGS_MUTABLE_ROLES: frozenset = frozenset({
    "burst_threshold",
    "pressure_drop_threshold",
    "leak_pressure_threshold",
    "trickle_min_flow",
    "trickle_max_flow",
    "trickle_duration",
    "leak_test_duration_number",
    "leak_test_duration_sensor",
})


def _seed_mutable_entity(db: sqlite3.Connection, circuit: str,
                          role: str, entity_id: str) -> None:
    db.execute(
        """INSERT OR REPLACE INTO circuit_entity_map
           (circuit, role, entity_id, entity_name, confirmed)
           VALUES (?, ?, ?, ?, 1)""",
        (circuit, role, entity_id, role),
    )
    db.commit()


# =============================================================================
# 1. _SETTINGS_MUTABLE_ROLES — all roles exist in ROLE_PATTERNS
# =============================================================================

def test_settings_mutable_roles_all_in_role_patterns():
    """Every _SETTINGS_MUTABLE_ROLES entry must appear in ROLE_PATTERNS."""
    for circuit in ("circuit_1", "circuit_2"):
        patterns = ROLE_PATTERNS[circuit]
        for role in _SETTINGS_MUTABLE_ROLES:
            assert role in patterns, (
                f"_SETTINGS_MUTABLE_ROLES entry '{role}' absent from "
                f"ROLE_PATTERNS['{circuit}']. Settings entity update will "
                f"always return 403 for this role."
            )


# =============================================================================
# 2. Allowlist logic — correct roles are accepted, others are rejected
# =============================================================================

def test_settings_allowlist_accepts_mutable_number():
    db = make_db()
    entity_id = "number.device_burst_threshold_main"
    _seed_mutable_entity(db, "circuit_1", "burst_threshold", entity_id)

    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _SETTINGS_MUTABLE_ROLES and v}
    assert entity_id in allowed


def test_settings_allowlist_rejects_input_number():
    db = make_db()
    _seed_mutable_entity(db, "circuit_1", "burst_threshold",
                          "number.device_burst_threshold_main")
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _SETTINGS_MUTABLE_ROLES and v}
    assert "input_number.some_helper" not in allowed


def test_settings_allowlist_rejects_select():
    db = make_db()
    _seed_mutable_entity(db, "circuit_1", "burst_threshold",
                          "number.device_burst_threshold_main")
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _SETTINGS_MUTABLE_ROLES and v}
    assert "select.some_option" not in allowed


def test_settings_allowlist_rejects_input_select():
    db = make_db()
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _SETTINGS_MUTABLE_ROLES and v}
    assert "input_select.some_dropdown" not in allowed


def test_settings_allowlist_rejects_arbitrary_entity():
    db = make_db()
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _SETTINGS_MUTABLE_ROLES and v}
    assert "number.some_random_entity_not_in_map" not in allowed


def test_settings_allowlist_rejects_valve():
    db = make_db()
    _seed_mutable_entity(db, "circuit_1", "valve_entity",
                          "valve.device_main_water_valve")
    entities = load_circuit_entities(db, "circuit_1")
    allowed = {v for k, v in entities.items() if k in _SETTINGS_MUTABLE_ROLES and v}
    assert "valve.device_main_water_valve" not in allowed


# =============================================================================
# 3. Template static checks
# =============================================================================

def test_templates_dir_exists():
    assert _TEMPLATES_DIR.is_dir(), f"Templates dir not found: {_TEMPLATES_DIR}"


def test_settings_html_posts_to_device_entity_update():
    """settings.html must submit device entity updates to /settings/device-entity/update."""
    settings_html = (_TEMPLATES_DIR / "settings.html").read_text(encoding="utf-8")
    assert "settings/device-entity/update" in settings_html, (
        "settings.html no longer targets /settings/device-entity/update. "
        "If the endpoint was renamed, update this test."
    )


def test_no_template_contains_hardcoded_main_in_url():
    """No Jinja2 template should contain '/main/' as a hardcoded URL segment.

    Route parameters must be circuit IDs from the DB, not hardcoded legacy names.
    """
    bad = re.compile(r"['\"`]/[^'\"` ]*/main/[^'\"` ]*['\"`]")
    for tpl_path in sorted(_TEMPLATES_DIR.glob("*.html")):
        content = tpl_path.read_text(encoding="utf-8")
        found = bad.findall(content)
        assert not found, (
            f"{tpl_path.name} contains hardcoded '/main/' URL path: {found}"
        )


def test_no_template_contains_hardcoded_irrigation_in_url():
    """No Jinja2 template should contain '/irrigation/' as a hardcoded URL segment."""
    bad = re.compile(r"['\"`]/[^'\"` ]*/irrigation/[^'\"` ]*['\"`]")
    for tpl_path in sorted(_TEMPLATES_DIR.glob("*.html")):
        content = tpl_path.read_text(encoding="utf-8")
        found = bad.findall(content)
        assert not found, (
            f"{tpl_path.name} contains hardcoded '/irrigation/' URL path: {found}"
        )


def test_templates_reference_circuit_label_not_raw_id():
    """Device/dashboard templates should use circuit.label / circuit.display_label,
    not raw circuit IDs, for displayed human-readable names."""
    # Both device.html and dashboard.html must reference 'label' in some form
    for tpl_name in ("device.html", "dashboard.html"):
        tpl = (_TEMPLATES_DIR / tpl_name).read_text(encoding="utf-8")
        has_label = "label" in tpl.lower() or "display_name" in tpl.lower()
        assert has_label, (
            f"{tpl_name} does not appear to use '.label' or '.display_name' "
            f"for circuit display — raw circuit IDs may be shown to the user."
        )
