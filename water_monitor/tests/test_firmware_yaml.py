"""Firmware YAML contract tests.

Uses text-based parsing (no PyYAML required) to assert structural properties
of the firmware YAML that the discovery layer depends on.

Run: pytest water_monitor/tests/test_firmware_yaml.py -v
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from water_monitor.app.device_discovery import ROLE_PATTERNS

_FIRMWARE_YAML = Path(__file__).parent.parent.parent / "firmware" / "esp-water-shut-off-3_6.yaml"


def _firmware_text() -> str:
    return _FIRMWARE_YAML.read_text(encoding="utf-8")


# =============================================================================
# 1. File exists and parses as text
# =============================================================================

def test_firmware_yaml_file_exists():
    assert _FIRMWARE_YAML.exists(), f"Firmware YAML not found: {_FIRMWARE_YAML}"


def test_firmware_yaml_not_empty():
    assert len(_firmware_text()) > 1000, "Firmware YAML appears empty or truncated"


# =============================================================================
# 2. Version
# =============================================================================

def test_firmware_version_is_3_6_0():
    """project.version must be bumped to 3.6.0."""
    text = _firmware_text()
    assert 'version: "3.6.0"' in text, (
        "Firmware project.version must be '3.6.0'. "
        "If the version was changed, update this test."
    )


def test_firmware_yaml_not_old_version():
    text = _firmware_text()
    assert 'version: "3.5.1"' not in text, (
        "Old version string '3.5.1' still present — version was not bumped."
    )


# =============================================================================
# 3. Diagnostic circuit identity sensors
# =============================================================================

def test_firmware_has_circuit_1_id_sensor():
    text = _firmware_text()
    assert "circuit_1_id_sensor" in text, (
        "Firmware must have an ESPHome id 'circuit_1_id_sensor' for circuit 1 identity diagnostics"
    )


def test_firmware_has_circuit_1_label_sensor():
    assert "circuit_1_label_sensor" in _firmware_text()


def test_firmware_has_circuit_2_id_sensor():
    assert "circuit_2_id_sensor" in _firmware_text()


def test_firmware_has_circuit_2_label_sensor():
    assert "circuit_2_label_sensor" in _firmware_text()


def test_firmware_diagnostic_sensors_have_entity_category():
    """The diagnostic sensor block must have entity_category: diagnostic."""
    text = _firmware_text()
    # Find the block around circuit label sensors and confirm entity_category
    idx = text.find("circuit_1_label_sensor")
    assert idx != -1
    surrounding = text[max(0, idx - 300): idx + 300]
    assert "entity_category: diagnostic" in surrounding, (
        "circuit_1_label_sensor block is missing entity_category: diagnostic"
    )


# =============================================================================
# 4. Legacy firmware entity IDs — must be preserved (not renamed)
# =============================================================================

_LEGACY_IDS = [
    "enable_high_flow_main",
    "enable_pressure_drop_main",
    "enable_trickle_main",
    "enable_leak_test_main",
    "enable_high_flow_irr",
    "enable_pressure_drop_irr",
    "enable_trickle_irr",
    "enable_leak_test_irr",
]


@pytest.mark.parametrize("legacy_id", _LEGACY_IDS)
def test_firmware_legacy_internal_ids_unchanged(legacy_id: str):
    """ESPHome internal IDs used as HA entity IDs must not be renamed.

    Discovery patterns rely on these suffixes to find the correct entities.
    """
    assert legacy_id in _firmware_text(), (
        f"ESPHome internal ID '{legacy_id}' not found in firmware. "
        f"Discovery patterns for alert switches will fail to match."
    )


# =============================================================================
# 5. Reset button display names match ROLE_PATTERNS
# =============================================================================

def test_firmware_reset_safety_fault_main_matches_discovery_pattern():
    """'Reset Safety Fault - Main' must match the circuit_1 fault_reset_button pattern."""
    text = _firmware_text()
    assert "Reset Safety Fault" in text
    pattern, _ = ROLE_PATTERNS["circuit_1"]["fault_reset_button"]
    assert re.search(pattern, "Reset Safety Fault - Main", re.IGNORECASE), (
        f"fault_reset_button pattern {pattern!r} does not match 'Reset Safety Fault - Main'"
    )


def test_firmware_reset_trickle_alert_main_matches_discovery_pattern():
    text = _firmware_text()
    assert "Reset Trickle Alert" in text
    pattern, _ = ROLE_PATTERNS["circuit_1"]["trickle_reset_button"]
    assert re.search(pattern, "Reset Trickle Alert - Main", re.IGNORECASE), (
        f"trickle_reset_button pattern {pattern!r} does not match 'Reset Trickle Alert - Main'"
    )


# =============================================================================
# 6. Threshold entity display names match ROLE_PATTERNS
# =============================================================================

_CIRCUIT1_THRESHOLD_NAMES = [
    ("burst_threshold",          "Burst Pipe Flow Threshold - Main"),
    ("pressure_drop_threshold",  "Pressure Drop Threshold - Main"),
    ("leak_pressure_threshold",  "Leak Test Pressure Threshold - Main"),
    ("trickle_min_flow",         "Trickle Flow Min Threshold - Main"),
    ("trickle_max_flow",         "Trickle Flow Max Threshold - Main"),
    ("trickle_duration",         "Trickle Flow Alert Duration - Main"),
    ("leak_test_duration_number","Leak Test Duration - Main"),
]


@pytest.mark.parametrize("role,display_name", _CIRCUIT1_THRESHOLD_NAMES)
def test_firmware_threshold_name_matches_discovery_pattern(role: str, display_name: str):
    """Each threshold display name in firmware must match its ROLE_PATTERNS entry."""
    pattern, _ = ROLE_PATTERNS["circuit_1"][role]
    assert re.search(pattern, display_name, re.IGNORECASE), (
        f"ROLE_PATTERNS['circuit_1']['{role}'] pattern {pattern!r} "
        f"does not match firmware display name {display_name!r}"
    )
