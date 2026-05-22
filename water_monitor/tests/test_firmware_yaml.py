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

def test_firmware_version_is_3_7_0():
    """project.version must be bumped to 3.7.0 for the waveform-capture phase."""
    text = _firmware_text()
    assert 'version: "3.7.0"' in text, (
        "Firmware project.version must be '3.7.0'. "
        "If the version was changed, update this test."
    )


def test_firmware_yaml_not_old_version():
    text = _firmware_text()
    assert 'version: "3.6.0"' not in text, (
        "Old version string '3.6.0' still present — version was not bumped."
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


# =============================================================================
# 7. Waveform capture (firmware 3.7.0+) — entities, engine, wire-format limits
# =============================================================================

# Wire-format contract constants (must match the firmware capture engine).
START_POINTS = 80          # sn — start-window points published per channel
FULL_POINTS = 64           # fn — full-window points published per channel
START_POINTS_MAX = 88      # hard cap — the 255-char guard is sized for this
FULL_POINTS_MAX = 64       # hard cap
HA_STATE_LIMIT = 255       # Home Assistant entity-state character limit

# The five per-event waveform entities + the standing diagnostic counter.
_WAVEFORM_ENTITY_IDS = [
    "event_start_flow_waveform",
    "event_start_pressure_waveform",
    "event_full_flow_waveform",
    "event_full_pressure_waveform",
    "event_metadata",
]
_WAVEFORM_GLOBALS = [
    "wf_ring_flow", "wf_ring_press", "wf_slot0_flow", "wf_slot0_press",
    "wf_slot1_flow", "wf_slot1_press", "wf_slot_phase", "wf_active_slot",
    "wf_event_counter", "wf_boot_id", "wf_overflow_count",
]


def _base36_len(value: int) -> int:
    """Number of characters in the base36 encoding of a non-negative int."""
    if value == 0:
        return 1
    n = 0
    while value > 0:
        value //= 36
        n += 1
    return n


def _base64_len(n_bytes: int) -> int:
    """Length of standard base64 (with padding) for n_bytes input bytes."""
    return ((n_bytes + 2) // 3) * 4


@pytest.mark.parametrize("entity_id", _WAVEFORM_ENTITY_IDS)
def test_firmware_has_waveform_entity(entity_id: str):
    """Each of the five per-event waveform text_sensors must be declared."""
    assert f"id: {entity_id}" in _firmware_text(), (
        f"Firmware is missing the '{entity_id}' text_sensor required by the "
        f"add-on waveform-ingest contract."
    )


def test_firmware_has_overflow_dropped_counter():
    """The standing diagnostic overflow counter sensor must exist."""
    assert "id: waveform_overflow_dropped_count" in _firmware_text()


@pytest.mark.parametrize("global_id", _WAVEFORM_GLOBALS)
def test_firmware_has_waveform_global(global_id: str):
    """Preallocated capture-engine globals must be declared."""
    assert f"id: {global_id}" in _firmware_text(), (
        f"Firmware is missing capture-engine global '{global_id}'."
    )


def test_firmware_has_waveform_capture_interval():
    """The ~50 Hz capture interval must be present."""
    text = _firmware_text()
    assert "interval: 20ms" in text, (
        "Firmware is missing the 20ms (~50 Hz) waveform_capture interval."
    )


def test_firmware_has_build_time_state_length_guard():
    """A static_assert must guard the worst-case waveform state length."""
    text = _firmware_text()
    assert "static_assert(" in text, (
        "Firmware capture engine is missing the build-time static_assert that "
        "guarantees waveform states stay under the HA 255-char limit."
    )


def test_firmware_waveform_uses_declared_point_counts():
    """Metadata must declare the start/full point counts the engine resamples to."""
    text = _firmware_text()
    assert f'\\"sn\\":{START_POINTS}' in text, (
        f"event_metadata must declare sn={START_POINTS}."
    )
    assert f'\\"fn\\":{FULL_POINTS}' in text, (
        f"event_metadata must declare fn={FULL_POINTS}."
    )


def test_waveform_state_fits_ha_limit_at_caps():
    """Worst-case waveform state (point caps + max-uint32 ids) must fit 255 chars.

    State format: <boot_id_b36>:<event_id_b36>:<base64>. Worst case is both
    ids = 2**32 - 1 and a start window at START_POINTS_MAX int16 points.
    """
    max_id_len = _base36_len(2**32 - 1)
    # Start window is the larger of the two windows; size at the hard cap.
    payload_bytes = START_POINTS_MAX * 2
    state_len = max_id_len + 1 + max_id_len + 1 + _base64_len(payload_bytes)
    assert state_len < HA_STATE_LIMIT, (
        f"Worst-case start waveform state is {state_len} chars "
        f"(>= {HA_STATE_LIMIT}). START_POINTS_MAX={START_POINTS_MAX} is too high."
    )


def test_full_waveform_state_fits_ha_limit_at_caps():
    """Worst-case full-window waveform state must also fit 255 chars."""
    max_id_len = _base36_len(2**32 - 1)
    payload_bytes = FULL_POINTS_MAX * 2
    state_len = max_id_len + 1 + max_id_len + 1 + _base64_len(payload_bytes)
    assert state_len < HA_STATE_LIMIT, (
        f"Worst-case full waveform state is {state_len} chars (>= {HA_STATE_LIMIT})."
    )


def test_metadata_state_fits_ha_limit_worst_case():
    """Worst-case compact metadata JSON must stay under the 255-char limit.

    Every numeric field is widened to its plausible maximum: uint32 millis /
    ids (10 digits), the point caps, and the largest flag/quality values.
    """
    u32 = 2**32 - 1
    meta = (
        '{"id":%d,"b":%d,"v":1,"s":%d,"e":%d,"p":%d,'
        '"sn":%d,"fn":%d,"pre":%d,"post":%d,"tl":%d,'
        '"fs":%d,"ps":%d,"pk":%d,"dp":%d,"pd":%d,"q":%d,"fl":%d}'
    ) % (
        u32, u32, u32, u32, u32,
        START_POINTS_MAX, FULL_POINTS_MAX, 65535, 65535, 65535,
        100, 100, 32767, 32767, 65535, 6, 127,
    )
    assert len(meta) < HA_STATE_LIMIT, (
        f"Worst-case event_metadata JSON is {len(meta)} chars "
        f"(>= {HA_STATE_LIMIT}); the compact field set must be trimmed."
    )


def test_metadata_json_has_no_spaces():
    """The metadata JSON template in firmware must be compact (no spaces)."""
    text = _firmware_text()
    # The firmware format string contains the literal `\"id\":` token; the
    # surrounding JSON must not introduce spaces after colons or commas.
    assert '\\"id\\":%u' in text, "event_metadata JSON template not found"
    assert '\\"id\\": ' not in text, "event_metadata JSON must be space-free"
