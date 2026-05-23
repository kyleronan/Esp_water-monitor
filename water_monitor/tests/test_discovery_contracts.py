"""
Discovery contract tests — static invariants between role names used by routers
and what ROLE_PATTERNS actually defines.

These tests exist to catch the class of bug where a router references a role name
that was never added to ROLE_PATTERNS, causing load_circuit_entities() to silently
return nothing and the endpoint to return 403 / send commands to non-existent entities.

Run: pytest water_monitor/tests/test_discovery_contracts.py -v
"""
from __future__ import annotations

import asyncio
import re

import pytest

from water_monitor.app.device_discovery import (
    ROLE_PATTERNS,
    OPTIONAL_ROLES,
    _resolve_labels_from_diagnostics,
    _make_label_pattern,
    match_entities_to_roles,
)

# Waveform roles that are ESPHome text_sensor: entities (domain = "text_sensor")
_WF_TEXT_SENSOR_ROLES = frozenset({
    "wf_start_flow_sensor",
    "wf_start_pressure_sensor",
    "wf_full_flow_sensor",
    "wf_full_pressure_sensor",
    "wf_metadata_sensor",
})


# ---------------------------------------------------------------------------
# Corrected _THRESHOLD_ROLES — must stay in sync with routers/device.py
# ---------------------------------------------------------------------------
_THRESHOLD_ROLES = frozenset({
    "leak_test_duration_number",
    "leak_test_duration_sensor",
    "burst_threshold",
    "pressure_drop_threshold",
    "leak_pressure_threshold",
    "trickle_min_flow",
    "trickle_max_flow",
    "trickle_duration",
})

_ALERT_SWITCH_ROLES = frozenset({
    "alert_high_flow_switch",
    "alert_pressure_drop_switch",
    "alert_trickle_switch",
    "alert_leak_test_switch",
})

_RESET_BUTTON_ROLES = frozenset({
    "fault_reset_button",
    "trickle_reset_button",
})

_ALL_CIRCUITS = ("circuit_1", "circuit_2")


# =============================================================================
# 1. Threshold roles all present in ROLE_PATTERNS
# =============================================================================

def test_threshold_roles_all_in_role_patterns():
    """Every name in _THRESHOLD_ROLES must appear in ROLE_PATTERNS for both circuits."""
    for circuit in _ALL_CIRCUITS:
        patterns = ROLE_PATTERNS[circuit]
        for role in _THRESHOLD_ROLES:
            assert role in patterns, (
                f"Role '{role}' from _THRESHOLD_ROLES is missing from "
                f"ROLE_PATTERNS['{circuit}']. Threshold updates will always "
                f"return 403 until this is fixed."
            )


# =============================================================================
# 2. Alert switch roles in ROLE_PATTERNS
# =============================================================================

def test_alert_switch_roles_in_role_patterns():
    for circuit in _ALL_CIRCUITS:
        patterns = ROLE_PATTERNS[circuit]
        for role in _ALERT_SWITCH_ROLES:
            assert role in patterns, (
                f"Role '{role}' missing from ROLE_PATTERNS['{circuit}']. "
                f"alert_toggle() will return 400 after discovery for this role."
            )


# =============================================================================
# 3. Reset button roles in ROLE_PATTERNS
# =============================================================================

def test_reset_button_roles_in_role_patterns():
    for circuit in _ALL_CIRCUITS:
        patterns = ROLE_PATTERNS[circuit]
        for role in _RESET_BUTTON_ROLES:
            assert role in patterns, (
                f"Role '{role}' missing from ROLE_PATTERNS['{circuit}']. "
                f"fault_reset() / trickle_reset() will return 400 after discovery."
            )


# =============================================================================
# 4. All new roles are in OPTIONAL_ROLES
# =============================================================================

def test_new_roles_in_optional_roles():
    """Reset buttons, alert switches, and threshold numbers are optional so older
    firmware without them does not fail discovery."""
    for role in _RESET_BUTTON_ROLES | _ALERT_SWITCH_ROLES | _THRESHOLD_ROLES:
        # leak_test_duration_sensor is a pre-existing role — may or may not be optional
        if role == "leak_test_duration_sensor":
            continue
        assert role in OPTIONAL_ROLES, (
            f"Role '{role}' should be in OPTIONAL_ROLES so older firmware "
            f"without this entity does not block setup completion."
        )


# =============================================================================
# 5. All ROLE_PATTERNS regex strings compile without error
# =============================================================================

def test_all_role_patterns_have_valid_regex():
    for circuit, patterns in ROLE_PATTERNS.items():
        for role, (pattern, domain) in patterns.items():
            try:
                re.compile(pattern, re.IGNORECASE)
            except re.error as e:
                pytest.fail(
                    f"ROLE_PATTERNS['{circuit}']['{role}'] regex {pattern!r} "
                    f"does not compile: {e}"
                )


# =============================================================================
# 6. circuit_2 alert/reset patterns do NOT match unrelated "irr" substrings
# =============================================================================

def test_circuit2_patterns_do_not_match_random_irr_substring():
    """_irr\\b must not match 'irrigation' display names or unrelated words."""
    unrelated_names = [
        "Irritation Level Sensor",
        "Current Mirror",
        "Irrigation Pump Status",   # contains 'irr' but not at word boundary after _
    ]
    for role, (pattern, domain) in ROLE_PATTERNS["circuit_2"].items():
        compiled = re.compile(pattern, re.IGNORECASE)
        for name in unrelated_names:
            # "Irrigation Pump Status" should only match roles that explicitly
            # match "irrigation" display name (which is expected behaviour).
            # The key invariant: _irr\b in the pattern does NOT match "Irritation".
            if "_irr\\b" in pattern and "irrigation" not in pattern.lower():
                assert not compiled.search(name), (
                    f"ROLE_PATTERNS['circuit_2']['{role}'] pattern {pattern!r} "
                    f"incorrectly matches unrelated name {name!r}"
                )


# =============================================================================
# 7. circuit_2 patterns match "irrigation" display names
# =============================================================================

def test_circuit2_discovery_matches_irrigation_suffix():
    """Entities with 'irrigation' in original_name match circuit_2 roles."""
    compiled = re.compile(ROLE_PATTERNS["circuit_2"]["flow_sensor"][0], re.IGNORECASE)
    assert compiled.search("Water Flow Rate - Irrigation"), (
        "flow_sensor pattern must match 'Water Flow Rate - Irrigation'"
    )


# =============================================================================
# 8. circuit_2 reset button matches _irr entity_id suffix
# =============================================================================

def test_circuit2_reset_button_matches_irr_entity_suffix():
    """_irr\b suffix alternative in reset button pattern matches entity_id."""
    pattern, domain = ROLE_PATTERNS["circuit_2"]["fault_reset_button"]
    compiled = re.compile(pattern, re.IGNORECASE)
    # Entity_id suffix fallback: switch.prefix_reset_safety_fault_irr
    assert compiled.search("reset_safety_fault_irr"), (
        f"fault_reset_button pattern {pattern!r} must match '_irr' entity_id suffix"
    )
    # Must NOT match "irrigation" via the _irr\b path (word boundary prevents it)
    # (it may match via the ".*irrigation" display-name path, which is fine)


# =============================================================================
# 9. Regex fallback for default firmware — "Main" and "Irrigation" names
# =============================================================================

def test_regex_fallback_for_default_firmware():
    """When diagnostic sensors are absent, standard display names still match."""
    flow_c1_pattern, _ = ROLE_PATTERNS["circuit_1"]["flow_sensor"]
    flow_c2_pattern, _ = ROLE_PATTERNS["circuit_2"]["flow_sensor"]
    assert re.search(flow_c1_pattern, "Water Flow Rate - Main", re.IGNORECASE)
    assert re.search(flow_c2_pattern, "Water Flow Rate - Irrigation", re.IGNORECASE)


# =============================================================================
# 10. _make_label_pattern substitution
# =============================================================================

def test_make_label_pattern_circuit1():
    """Label pattern replaces 'main' with the escaped diagnostic label."""
    base = r"water flow rate.*main"
    lp = _make_label_pattern(base, "circuit_1", "Zone A")
    assert lp is not None
    assert re.search(lp, "Water Flow Rate - Zone A", re.IGNORECASE)
    assert not re.search(lp, "Water Flow Rate - Main", re.IGNORECASE)


def test_make_label_pattern_circuit2():
    """Label pattern replaces 'irrigation' in circuit_2 patterns."""
    base = r"burst pipe flow threshold.*irrigation|burst_threshold_irr\b"
    lp = _make_label_pattern(base, "circuit_2", "North Garden")
    assert lp is not None
    assert re.search(lp, "Burst Pipe Flow Threshold - North Garden", re.IGNORECASE)


def test_make_label_pattern_escapes_special_chars():
    """User-controlled labels with regex metacharacters are escaped."""
    base = r"water flow rate.*main"
    lp = _make_label_pattern(base, "circuit_1", "Unit A (North)")
    assert lp is not None
    assert re.search(lp, "Water Flow Rate - Unit A (North)", re.IGNORECASE)
    # The parentheses must be literal, not a regex group
    assert re.search(lp, "Water Flow Rate Unit A (North) sensor", re.IGNORECASE)


def test_make_label_pattern_returns_none_when_no_keyword():
    """Returns None when the circuit keyword is absent from the pattern."""
    base = r"main water valve"   # circuit_2 won't have "irrigation" in this pattern
    result = _make_label_pattern(base, "circuit_2", "Irrigation")
    assert result is None


# =============================================================================
# 11. Diagnostic label resolution — uses metadata, not guessed entity_ids
# =============================================================================

def test_resolve_labels_from_diagnostics_finds_label_sensors():
    """_resolve_labels_from_diagnostics returns labels from matching sensors."""

    class _StubHa:
        async def get_state_value(self, entity_id, default):
            states = {
                "text_sensor.device_circuit_1_label": "Zone A",
                "text_sensor.device_circuit_2_label": "North Garden",
            }
            return states.get(entity_id, default)

    entities = [
        {"entity_id": "text_sensor.device_circuit_1_id",
         "original_name": "Circuit 1 ID"},
        {"entity_id": "text_sensor.device_circuit_1_label",
         "original_name": "Circuit 1 Label"},
        {"entity_id": "text_sensor.device_circuit_2_label",
         "original_name": "Circuit 2 Label"},
        {"entity_id": "sensor.device_flow_main",
         "original_name": "Water Flow Rate - Main"},
    ]

    labels = asyncio.run(_resolve_labels_from_diagnostics(_StubHa(), entities))
    assert labels == {"circuit_1": "Zone A", "circuit_2": "North Garden"}


def test_resolve_labels_returns_empty_for_old_firmware():
    """Returns {} when no diagnostic label sensors exist."""

    class _StubHa:
        async def get_state_value(self, entity_id, default):
            return default

    entities = [
        {"entity_id": "sensor.device_flow_main", "original_name": "Water Flow Rate - Main"},
    ]
    labels = asyncio.run(_resolve_labels_from_diagnostics(_StubHa(), entities))
    assert labels == {}


# =============================================================================
# 12. match_entities_to_roles uses label tier before regex tier
# =============================================================================

def test_diagnostic_label_used_in_matching():
    """match_entities_to_roles() tier-1 label match takes precedence over regex."""
    device_id = "device_abc"
    # Entity has non-default name "Zone A" — would NOT match ".*main" regex
    entities = [
        {
            "device_id": device_id,
            "entity_id": "sensor.dev_water_flow_rate_zone_a",
            "original_name": "Water Flow Rate - Zone A",
        }
    ]
    labels = {"circuit_1": "Zone A"}
    matches, _ = match_entities_to_roles(device_id, entities, ["circuit_1"], labels=labels)
    flow_match = next(m for m in matches["circuit_1"] if m.role == "flow_sensor")
    assert flow_match.matched, (
        "flow_sensor should match via diagnostic label tier when label is 'Zone A'"
    )
    assert flow_match.entity_id == "sensor.dev_water_flow_rate_zone_a"


def test_regex_fallback_used_when_no_labels():
    """Without labels, standard 'main' display name is matched via regex."""
    device_id = "device_abc"
    entities = [
        {
            "device_id": device_id,
            "entity_id": "sensor.dev_water_flow_rate_main",
            "original_name": "Water Flow Rate - Main",
        }
    ]
    matches, _ = match_entities_to_roles(device_id, entities, ["circuit_1"])
    flow_match = next(m for m in matches["circuit_1"] if m.role == "flow_sensor")
    assert flow_match.matched
    assert flow_match.entity_id == "sensor.dev_water_flow_rate_main"


# =============================================================================
# 13. Waveform role domains — ESPHome text_sensor: entities must be "text_sensor"
# =============================================================================

def test_waveform_text_sensor_roles_have_text_sensor_domain():
    """The 5 waveform window/metadata roles must declare domain 'text_sensor',
    not 'sensor', so that match_entities_to_roles() finds them in the HA registry."""
    patterns = ROLE_PATTERNS["circuit_1"]
    for role in _WF_TEXT_SENSOR_ROLES:
        assert role in patterns, f"Role '{role}' missing from ROLE_PATTERNS['circuit_1']"
        _, domain = patterns[role]
        assert domain == "text_sensor", (
            f"ROLE_PATTERNS['circuit_1']['{role}'] has domain {domain!r}; "
            f"expected 'text_sensor' (ESPHome text_sensor: entities are never "
            f"exposed under the 'sensor' domain in HA)."
        )


def test_wf_overflow_count_sensor_has_sensor_domain():
    """The overflow counter is a numeric sensor: entity — domain must stay 'sensor'."""
    _, domain = ROLE_PATTERNS["circuit_1"]["wf_overflow_count_sensor"]
    assert domain == "sensor", (
        f"wf_overflow_count_sensor domain is {domain!r}; expected 'sensor'."
    )


def test_waveform_text_sensor_roles_match_correct_domain_entities():
    """match_entities_to_roles() finds waveform text_sensor entities when domain='text_sensor'."""
    device_id = "dev_123"
    pfx = "esp_water_shut_off_3_"
    # Provide one entity per wf text_sensor role in the correct domain
    entities = [
        {
            "device_id": device_id,
            "entity_id": f"text_sensor.{pfx}event_start_flow_waveform_main",
            "original_name": "Event Start Flow Waveform Main",
        },
        {
            "device_id": device_id,
            "entity_id": f"text_sensor.{pfx}event_start_pressure_waveform_main",
            "original_name": "Event Start Pressure Waveform Main",
        },
        {
            "device_id": device_id,
            "entity_id": f"text_sensor.{pfx}event_full_flow_waveform_main",
            "original_name": "Event Full Flow Waveform Main",
        },
        {
            "device_id": device_id,
            "entity_id": f"text_sensor.{pfx}event_full_pressure_waveform_main",
            "original_name": "Event Full Pressure Waveform Main",
        },
        {
            "device_id": device_id,
            "entity_id": f"text_sensor.{pfx}event_metadata_main",
            "original_name": "Event Metadata Main",
        },
        {
            "device_id": device_id,
            "entity_id": f"sensor.{pfx}waveform_overflow_dropped_count_main",
            "original_name": "Waveform Overflow Dropped Count Main",
        },
    ]
    matches, _ = match_entities_to_roles(device_id, entities, ["circuit_1"])
    for role in _WF_TEXT_SENSOR_ROLES:
        m = next(x for x in matches["circuit_1"] if x.role == role)
        assert m.matched, f"{role} should match a text_sensor entity"
        assert m.entity_id.startswith("text_sensor."), (
            f"{role} matched {m.entity_id!r}; expected a text_sensor entity"
        )

    overflow = next(x for x in matches["circuit_1"] if x.role == "wf_overflow_count_sensor")
    assert overflow.matched
    assert overflow.entity_id.startswith("sensor.")


def test_waveform_sensor_entities_not_matched_to_text_sensor_roles():
    """Entities with domain 'sensor' must NOT match text_sensor waveform roles."""
    device_id = "dev_123"
    pfx = "esp_water_shut_off_3_"
    # Provide entities in the WRONG domain (sensor instead of text_sensor)
    wrong_domain_entities = [
        {
            "device_id": device_id,
            "entity_id": f"sensor.{pfx}event_metadata_main",
            "original_name": "Event Metadata Main",
        },
    ]
    matches, _ = match_entities_to_roles(device_id, wrong_domain_entities, ["circuit_1"])
    meta = next(x for x in matches["circuit_1"] if x.role == "wf_metadata_sensor")
    assert not meta.matched, (
        "wf_metadata_sensor must NOT match a 'sensor' domain entity — "
        "it should only match 'text_sensor' domain entities."
    )
