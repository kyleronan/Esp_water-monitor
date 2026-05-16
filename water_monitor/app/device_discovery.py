"""
Device discovery — queries the HA device and entity registries to
automatically find the ESP device and map its entities to circuit roles.

Flow:
  1. Query device registry for all devices
  2. Find devices matching the configured name
     - Exact match (case-insensitive) → auto-select
     - Partial matches → present as suggestions
     - No matches → show all ESPHome devices
  3. Once a device is selected, query entity registry for its entities
  4. Match entities to circuit roles using name patterns
  5. Store discovered entity IDs in circuit_entity_map (SQLite)
  6. Any unmatched roles are flagged for manual selection in the UI

Matching uses the entity's original_name (from the ESPHome YAML `name:`
field) rather than the entity_id, so it works regardless of how HA
normalises the device name into the entity ID prefix.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# Minimum firmware version required for full feature support.
# Checked against the device registry sw_version field (set via project.version
# in the ESPHome YAML). Non-numeric versions (e.g. "dev") are treated as unknown
# — setup is not blocked, but a warning is shown.
MIN_FIRMWARE_VERSION: tuple = (3, 5, 1)

# Roles that are optional — wizard will show them as optional dropdowns
# and they won't block setup completion if unmatched.
OPTIONAL_ROLES = {
    "fault_reason_sensor",
    "volume_sensor",
    "leak_test_result_sensor",
    "leak_test_duration_sensor",
    "pressure_history_sensor",   # present only after firmware change removing diagnostic
    # Reset buttons (added v3.6)
    "fault_reset_button",
    "trickle_reset_button",
    # Alert enable/disable switches (added v3.6)
    "alert_high_flow_switch",
    "alert_pressure_drop_switch",
    "alert_trickle_switch",
    "alert_leak_test_switch",
    # Threshold number entities (added v3.6)
    "burst_threshold",
    "pressure_drop_threshold",
    "leak_pressure_threshold",
    "trickle_min_flow",
    "trickle_max_flow",
    "trickle_duration",
    "leak_test_duration_number",  # preferred name; leak_test_duration_sensor is the compat alias
}


# ------------------------------------------------------------------
# Role patterns — what entity name pattern maps to which role
# for each circuit.  Patterns are matched case-insensitively against
# the entity's original_name from the HA entity registry.
# ------------------------------------------------------------------

# Default display names for each circuit ID (used in setup wizard).
CIRCUIT_DISPLAY_DEFAULTS: Dict[str, str] = {
    "circuit_1": "Main",
    "circuit_2": "Irrigation",
}

# Role → (name pattern, domain)
# Pattern is matched against original_name (case-insensitive).
# Domain narrows the match when multiple entities share a similar name.
#
# Keys are now stable circuit IDs (circuit_1 / circuit_2).
# Regex patterns still search for "main" and "irrigation" because those are
# the keywords in the DEFAULT firmware entity names (e.g. "Main Water Valve",
# "Water Flow Rate - Irrigation"). For firmware with non-default label
# substitutions (e.g. duplex installs), these patterns will not match and
# the setup wizard's manual entity assignment UI must be used instead.
#
# Discovery priority: diagnostic Circuit ID/Label text sensors (added in
# firmware v3.6+) are checked first; these regex patterns are the fallback
# for older firmware without those sensors.
ROLE_PATTERNS: Dict[str, Dict[str, Tuple[str, str]]] = {
    "circuit_1": {   # was "main" — regex patterns match default firmware names
        "flow_sensor":             (r"water flow rate.*main",                           "sensor"),
        # Lookahead patterns — order-insensitive so "Water Pressure (Fast) Main" and
        # "Water Pressure Main (Fast)" both match without needing a regex update.
        "pressure_fast_sensor":    (r"water pressure(?=.*main)(?=.*fast)",              "sensor"),
        "pressure_avg_sensor":     (r"water pressure(?=.*main)(?=.*averaged)",          "sensor"),
        "pressure_history_sensor": (r"water pressure(?=.*main)(?!.*fast)(?!.*averaged)","sensor"),
        "flow_onset_sensor":       (r"flow pulse onset.*main",                          "binary_sensor"),
        "valve_entity":            (r"main water valve",                                "valve"),
        "fault_sensor":            (r"safety fault.*main",                              "binary_sensor"),
        "fault_reason_sensor":     (r"fault reason.*main|safety fault.*reason.*main",   "sensor"),
        "trickle_sensor":          (r"trickle.*alert.*main",                            "binary_sensor"),
        "leak_test_sensor":        (r"leak test active.*main",                          "binary_sensor"),
        "leak_test_switch":        (r"micro leak test.*main",                           "switch"),
        "leak_test_result_sensor": (r"leak test result.*main",                          "sensor"),
        "leak_test_duration_sensor": (r"leak test duration.*main",                      "number"),   # compat alias
        "volume_sensor":           (r"water volume total.*main",                        "sensor"),
        # Reset buttons (firmware v3.6+)
        "fault_reset_button":         (r"reset safety fault.*main",                    "button"),
        "trickle_reset_button":       (r"reset trickle alert.*main",                   "button"),
        # Alert enable/disable switches (firmware v3.6+)
        "alert_high_flow_switch":     (r"enable high flow alert.*main",                "switch"),
        "alert_pressure_drop_switch": (r"enable pressure drop alert.*main",            "switch"),
        "alert_trickle_switch":       (r"enable trickle alert.*main",                  "switch"),
        "alert_leak_test_switch":     (r"enable leak test alert.*main",                "switch"),
        # Writable threshold number entities (firmware v3.6+)
        "burst_threshold":            (r"burst pipe flow threshold.*main",             "number"),
        "pressure_drop_threshold":    (r"pressure drop threshold.*main",               "number"),
        "leak_pressure_threshold":    (r"leak test pressure threshold.*main",          "number"),
        "trickle_min_flow":           (r"trickle flow min threshold.*main",            "number"),
        "trickle_max_flow":           (r"trickle flow max threshold.*main",            "number"),
        "trickle_duration":           (r"trickle flow alert duration.*main",           "number"),
        "leak_test_duration_number":  (r"leak test duration.*main",                    "number"),
    },
    "circuit_2": {   # was "irrigation" — regex patterns match default firmware names
        "flow_sensor":             (r"water flow rate.*irrigation",                           "sensor"),
        "pressure_fast_sensor":    (r"water pressure(?=.*irrigation)(?=.*fast)",              "sensor"),
        "pressure_avg_sensor":     (r"water pressure(?=.*irrigation)(?=.*averaged)",          "sensor"),
        "pressure_history_sensor": (r"water pressure(?=.*irrigation)(?!.*fast)(?!.*averaged)","sensor"),
        "flow_onset_sensor":       (r"flow pulse onset.*irrigation",                          "binary_sensor"),
        "valve_entity":            (r"irrigation water valve",                                "valve"),
        "fault_sensor":            (r"safety fault.*irrigation",                              "binary_sensor"),
        "fault_reason_sensor":     (r"fault reason.*irrigation|safety fault.*reason.*irrigation", "sensor"),
        "trickle_sensor":          (r"trickle.*alert.*irrigation",                            "binary_sensor"),
        "leak_test_sensor":        (r"leak test active.*irrigation",                          "binary_sensor"),
        "leak_test_switch":        (r"micro leak test.*irrigation",                           "switch"),
        "leak_test_result_sensor": (r"leak test result.*irrigation",                          "sensor"),
        "leak_test_duration_sensor": (r"leak test duration.*irrigation|leak_duration_irr\b",  "number"),   # compat alias
        "volume_sensor":           (r"water volume total.*irrigation",                        "sensor"),
        # Reset buttons (firmware v3.6+)
        # Display names use ${circuit_2_name} → "Irrigation"; entity_id suffix fallback uses _irr\b
        # (_irr appears in ESPHome internal IDs; \b prevents matching "irrigation" display names)
        "fault_reset_button":         (r"reset safety fault.*irrigation|reset safety fault.*_irr\b",           "button"),
        "trickle_reset_button":       (r"reset trickle alert.*irrigation|reset trickle alert.*_irr\b",         "button"),
        # Alert enable/disable switches (firmware v3.6+)
        "alert_high_flow_switch":     (r"enable high flow alert.*irrigation|enable_high_flow_irr\b",           "switch"),
        "alert_pressure_drop_switch": (r"enable pressure drop alert.*irrigation|enable_pressure_drop_irr\b",   "switch"),
        "alert_trickle_switch":       (r"enable trickle alert.*irrigation|enable_trickle_irr\b",               "switch"),
        "alert_leak_test_switch":     (r"enable leak test alert.*irrigation|enable_leak_test_irr\b",           "switch"),
        # Writable threshold number entities (firmware v3.6+)
        "burst_threshold":            (r"burst pipe flow threshold.*irrigation|burst_threshold_irr\b",         "number"),
        "pressure_drop_threshold":    (r"pressure drop threshold.*irrigation|pressure_drop_threshold_irr\b",   "number"),
        "leak_pressure_threshold":    (r"leak test pressure threshold.*irrigation|leak_threshold_psi_irr\b",   "number"),
        "trickle_min_flow":           (r"trickle flow min threshold.*irrigation|trickle_min_flow_irr\b",       "number"),
        "trickle_max_flow":           (r"trickle flow max threshold.*irrigation|trickle_max_flow_irr\b",       "number"),
        "trickle_duration":           (r"trickle flow alert duration.*irrigation|trickle_duration_irr\b",      "number"),
        "leak_test_duration_number":  (r"leak test duration.*irrigation|leak_duration_irr\b",                  "number"),
    },
}


@dataclass
class DiscoveredDevice:
    """A device found in the HA device registry."""
    id: str               # HA device registry ID
    name: str             # device display name
    name_by_user: str     # user-customised name (may be empty)
    model: Optional[str]
    manufacturer: Optional[str]
    identifiers: List[Any] = field(default_factory=list)
    sw_version: Optional[str] = None   # project.version from ESPHome YAML

    @property
    def display_name(self) -> str:
        return self.name_by_user or self.name

    @property
    def is_esphome(self) -> bool:
        return any(
            "esphome" in str(ident).lower()
            for ident in self.identifiers
        )

    @property
    def firmware_ok(self) -> bool:
        """True if sw_version meets MIN_FIRMWARE_VERSION, or version is unknown."""
        if not self.sw_version:
            return True   # can't determine — don't block setup
        try:
            # HA appends "(ESPHome x.y.z)" to the project version — strip it
            version_str = self.sw_version.split("(")[0].strip()
            parts = tuple(int(x) for x in version_str.split(".")[:3])
            return parts >= MIN_FIRMWARE_VERSION
        except ValueError:
            log.warning(
                "Firmware version %r is non-numeric — cannot verify compatibility "
                "(minimum required: %s). Proceeding, but some features may not work.",
                self.sw_version,
                ".".join(str(x) for x in MIN_FIRMWARE_VERSION),
            )
            return True   # non-numeric (e.g. "dev") — don't block setup


@dataclass
class EntityMatch:
    """Result of matching an entity to a circuit role."""
    role: str
    entity_id: str
    original_name: str
    domain: str
    matched: bool
    confidence: str   # 'exact', 'partial', 'unmatched'
    optional: bool = False


@dataclass
class DiscoveryResult:
    """Complete result of discovering entities for all circuits."""
    device: DiscoveredDevice
    circuit_matches: Dict[str, List[EntityMatch]]   # circuit → matches
    esp_device_prefix: str

    @property
    def all_matched(self) -> bool:
        for matches in self.circuit_matches.values():
            if any(not m.matched and not m.optional for m in matches):
                return False
        return True

    @property
    def unmatched_roles(self) -> Dict[str, List[str]]:
        result = {}
        for circuit, matches in self.circuit_matches.items():
            unmatched = [m.role for m in matches
                         if not m.matched and not m.optional]
            if unmatched:
                result[circuit] = unmatched
        return result


def find_matching_devices(
    devices: List[Dict[str, Any]],
    search_name: str,
) -> Tuple[Optional[DiscoveredDevice], List[DiscoveredDevice]]:
    """
    Search for devices matching search_name.

    Returns:
        (exact_match, suggestions)
        - exact_match: single DiscoveredDevice if name matches exactly
                       (case-insensitive), otherwise None
        - suggestions: all devices whose name contains search_name as a
                       substring, or all ESPHome devices if no substring
                       matches found
    """
    all_devices = [_to_device(d) for d in devices]
    search_lower = search_name.strip().lower()

    # Exact match — name or name_by_user equals search_name exactly
    exact = next(
        (d for d in all_devices
         if d.name.lower() == search_lower
         or (d.name_by_user and d.name_by_user.lower() == search_lower)),
        None,
    )
    if exact:
        return exact, []

    # Partial matches — name contains the search term
    partial = [
        d for d in all_devices
        if search_lower in d.name.lower()
        or (d.name_by_user and search_lower in d.name_by_user.lower())
    ]
    if partial:
        return None, partial

    # No matches at all — fall back to all ESPHome devices as suggestions
    esphome_devices = [d for d in all_devices if d.is_esphome]
    suggestions = esphome_devices if esphome_devices else all_devices
    return None, suggestions


async def _resolve_labels_from_diagnostics(
    ha,
    entity_registry_entities: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Fetch live circuit display labels from v3.6+ diagnostic text sensors.

    Looks up "Circuit N Label" sensors by original_name in the entity registry
    (metadata-first — never guesses entity_ids), then fetches their live state
    from HA.  Returns {circuit_id: label_string}, e.g. {"circuit_1": "Zone A"}.
    Returns an empty dict when no diagnostic sensors are present (older firmware).
    """
    labels: Dict[str, str] = {}
    for entity in entity_registry_entities:
        name = (entity.get("original_name") or "").lower()
        if not re.search(r"circuit [12] label", name):
            continue
        state = await ha.get_state_value(entity["entity_id"], None)
        if not state:
            continue
        n = re.search(r"circuit (\d+)", name)
        if n:
            labels[f"circuit_{n.group(1)}"] = state
    if labels:
        log.info("Diagnostic circuit labels resolved: %s", labels)
    return labels


def _make_label_pattern(base_pattern: str, circuit: str, label: str) -> Optional[str]:
    """Return a variant of *base_pattern* with the default firmware keyword
    replaced by *re.escape(label)*, or None if the keyword is not in the pattern.

    circuit_1 patterns contain "main"; circuit_2 patterns contain "irrigation".
    The _irr\\b entity-id suffix alternatives in circuit_2 patterns are left
    unchanged so entity_id fallback matching still works.
    """
    keyword = "main" if circuit == "circuit_1" else "irrigation"
    if keyword not in base_pattern:
        return None
    escaped = re.escape(label)
    return base_pattern.replace(f".*{keyword}", f".*{escaped}").replace(keyword, escaped)


def match_entities_to_roles(
    device_id: str,
    entities: List[Dict[str, Any]],
    circuits: List[str],
    labels: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, List[EntityMatch]], str]:
    """Match entities belonging to device_id to circuit roles.

    *labels* — optional dict of {circuit_id: display_label} from
    :func:`_resolve_labels_from_diagnostics`.  When provided, matching uses
    three ordered tiers per role:

    1. Escaped diagnostic label against ``original_name`` (handles user-renamed
       circuits — ``re.escape`` is applied because labels are user-controlled).
    2. Hardcoded display-name terms ("main" / "irrigation") in ``original_name``.
    3. Entity object_id / entity_id suffix fallback (``_main`` / ``_irr\\b``).

    Tiers 2 and 3 are already encoded in the ROLE_PATTERNS regexes; tier 1 is
    attempted first by substituting the escaped label into the pattern.

    Returns:
        (circuit_matches, esp_device_prefix)
    """
    labels = labels or {}
    # Filter to entities belonging to this device
    device_entities = [e for e in entities if e.get("device_id") == device_id]

    log.info("Device %s has %d registered entities",
             device_id, len(device_entities))

    # Derive ESP device prefix from entity IDs
    # Entity IDs look like: sensor.esp_water_shut_off_3_water_flow_rate_main
    # Prefix is: esp_water_shut_off_3_
    prefix = _derive_prefix(device_entities)

    circuit_matches: Dict[str, List[EntityMatch]] = {}

    for circuit in circuits:
        patterns = ROLE_PATTERNS.get(circuit, {})
        circuit_label = labels.get(circuit)
        matches = []

        for role, (pattern, expected_domain) in patterns.items():
            match = None

            # Tier 1: escaped diagnostic label (non-default firmware names)
            if circuit_label:
                lp = _make_label_pattern(pattern, circuit, circuit_label)
                if lp:
                    match = _find_entity_for_role(device_entities, lp, expected_domain)

            # Tiers 2+3: hardcoded "main"/"irrigation" display term + _irr\b suffix
            if not match:
                match = _find_entity_for_role(device_entities, pattern, expected_domain)

            if match:
                entity_id = match["entity_id"]
                name = match.get("original_name") or match.get("name") or ""
                matches.append(EntityMatch(
                    role=role,
                    entity_id=entity_id,
                    original_name=name,
                    domain=entity_id.split(".")[0],
                    matched=True,
                    confidence="exact",
                    optional=role in OPTIONAL_ROLES,
                ))
                log.debug("[%s] %s → %s", circuit, role, entity_id)
            else:
                log.warning("[%s] no entity found for role '%s'",
                            circuit, role)
                matches.append(EntityMatch(
                    role=role,
                    entity_id="",
                    original_name="",
                    domain="",
                    matched=False,
                    confidence="unmatched",
                    optional=role in OPTIONAL_ROLES,
                ))

        circuit_matches[circuit] = matches

    return circuit_matches, prefix


def _find_entity_for_role(
    entities: List[Dict[str, Any]],
    pattern: str,
    expected_domain: str,
) -> Optional[Dict[str, Any]]:
    """Find the best entity match for a role pattern."""
    compiled = re.compile(pattern, re.IGNORECASE)
    candidates = []

    for entity in entities:
        entity_id = entity.get("entity_id", "")
        domain = entity_id.split(".")[0]
        if domain != expected_domain:
            continue

        # Match against original_name first, then fall back to entity_id
        name = entity.get("original_name") or entity.get("name") or ""
        if compiled.search(name) or compiled.search(entity_id):
            candidates.append(entity)

    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    # Multiple candidates — prefer one where original_name matches over entity_id
    name_matches = [
        e for e in candidates
        if compiled.search(e.get("original_name") or "")
    ]
    return name_matches[0] if name_matches else candidates[0]


def _derive_prefix(entities: List[Dict[str, Any]]) -> str:
    """
    Derive the ESP device prefix from entity IDs.

    e.g. from 'sensor.esp_water_shut_off_3_water_flow_rate_main'
    extracts 'esp_water_shut_off_3_'
    """
    # Known suffixes used to strip the device prefix from entity IDs.
    # If the firmware adds new entity types, extend this list or switch to
    # a longest-common-prefix approach across all device entity IDs.
    known_suffixes = [
        "water_flow_rate_main",
        "water_flow_rate_irrigation",
        "safety_fault_main",
        "safety_fault_irrigation",
        "water_volume_total_main",
    ]

    for entity in entities:
        eid = entity.get("entity_id", "")
        # Strip domain prefix (sensor., binary_sensor., etc.)
        if "." not in eid:
            continue
        local = eid.split(".", 1)[1]  # e.g. esp_water_shut_off_3_water_flow_rate_main

        for suffix in known_suffixes:
            if local.endswith(suffix) and len(local) > len(suffix):
                prefix = local[: len(local) - len(suffix)]
                if prefix:
                    log.debug("Derived ESP prefix: %r", prefix)
                    return prefix

    return ""


def _to_device(raw: Dict[str, Any]) -> DiscoveredDevice:
    return DiscoveredDevice(
        id=raw.get("id", ""),
        name=raw.get("name") or raw.get("name_by_user") or "",
        name_by_user=raw.get("name_by_user") or "",
        model=raw.get("model"),
        manufacturer=raw.get("manufacturer"),
        identifiers=raw.get("identifiers", []),
        sw_version=raw.get("sw_version"),
    )


# ------------------------------------------------------------------
# Database helpers for persisting discovery results
# ------------------------------------------------------------------

def save_discovery(
    db: sqlite3.Connection,
    result: DiscoveryResult,
) -> None:
    """Persist a completed discovery result to SQLite."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    with db:
        db.execute("""
            UPDATE device_config SET
                ha_device_id = ?,
                ha_device_name = ?,
                esp_device_prefix = ?,
                fw_version = ?,
                setup_complete = 0,
                updated_at = ?
            WHERE id = 1
        """, (result.device.id, result.device.display_name,
              result.esp_device_prefix, result.device.sw_version, now))

        db.execute("DELETE FROM circuit_entity_map")

        # Clear fixture data from the previous setup so stale clusters and fixtures
        # don't bleed through into the new setup's labelling flow.
        db.execute("DELETE FROM fixture_clusters")
        # fixture_ha_entity_map and fixture_daily_summary reference fixtures(id)
        # without ON DELETE CASCADE, so they must be cleared before deleting fixtures.
        # NOTE: MQTT Discovery entities already published to HA are not retracted here
        # — the setup wizard does not perform HA teardown on reset.
        db.execute("DELETE FROM fixture_ha_entity_map")
        db.execute("DELETE FROM fixture_daily_summary")
        db.execute("DELETE FROM fixtures")
        db.execute("UPDATE events SET cluster_id = NULL, fixture_id = NULL")
        db.execute("UPDATE training_state SET state = 'idle'")

        for circuit, matches in result.circuit_matches.items():
            for m in matches:
                db.execute("""
                    INSERT INTO circuit_entity_map
                        (circuit, role, entity_id, entity_name, confirmed)
                    VALUES (?, ?, ?, ?, 0)
                """, (circuit, m.role, m.entity_id, m.original_name))


def mark_setup_complete(db: sqlite3.Connection) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    db.execute("""
        UPDATE device_config SET setup_complete = 1, updated_at = ?
        WHERE id = 1
    """, (now,))
    db.commit()


def load_circuit_entities(
    db: sqlite3.Connection,
    circuit: str,
) -> Dict[str, str]:
    """Return {role: entity_id} for a circuit from the DB."""
    rows = db.execute("""
        SELECT role, entity_id FROM circuit_entity_map
        WHERE circuit = ? AND entity_id != ''
    """, (circuit,)).fetchall()
    return {row["role"]: row["entity_id"] for row in rows}


def get_all_device_entities(
    db: sqlite3.Connection,
) -> List[Dict[str, Any]]:
    """Return all discovered entities for the UI selection dropdowns."""
    rows = db.execute("""
        SELECT circuit, role, entity_id, entity_name, confirmed
        FROM circuit_entity_map
        ORDER BY circuit, role
    """).fetchall()
    return [dict(r) for r in rows]


def is_setup_complete(db: sqlite3.Connection) -> bool:
    row = db.execute(
        "SELECT setup_complete FROM device_config WHERE id = 1"
    ).fetchone()
    return bool(row and row["setup_complete"])


def get_device_config(db: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    row = db.execute(
        "SELECT * FROM device_config WHERE id = 1"
    ).fetchone()
    return dict(row) if row else None
