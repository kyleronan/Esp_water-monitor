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

# Roles that are optional — wizard will show them as optional dropdowns
# and they won't block setup completion if unmatched.
OPTIONAL_ROLES = {
    "fault_reason_sensor",
    "volume_sensor",
    "leak_test_result_sensor",
    "leak_test_duration_sensor",
    "pressure_history_sensor",   # present only after firmware change removing diagnostic
}


# ------------------------------------------------------------------
# Role patterns — what entity name pattern maps to which role
# for each circuit.  Patterns are matched case-insensitively against
# the entity's original_name from the HA entity registry.
# ------------------------------------------------------------------

# Role → (name pattern, domain)
# Pattern is matched against original_name (case-insensitive).
# Domain narrows the match when multiple entities share a similar name.
ROLE_PATTERNS: Dict[str, Dict[str, Tuple[str, str]]] = {
    "main": {
        "flow_sensor":             (r"water flow rate.*main",         "sensor"),
        "pressure_fast_sensor":    (r"water pressure.*main.*fast",    "sensor"),
        "pressure_avg_sensor":     (r"water pressure.*main.*averaged","sensor"),
        "pressure_history_sensor": (r"water pressure.*main(?!\s*\((?:fast|averaged))", "sensor"),
        "flow_onset_sensor":       (r"flow pulse onset.*main",        "binary_sensor"),
        "valve_entity":            (r"main water valve",              "valve"),
        "fault_sensor":            (r"safety fault.*main",            "binary_sensor"),
        "fault_reason_sensor":     (r"fault reason.*main|safety fault.*reason.*main", "sensor"),
        "trickle_sensor":          (r"trickle.*alert.*main",          "binary_sensor"),
        "leak_test_sensor":        (r"leak test active.*main",        "binary_sensor"),
        "leak_test_switch":        (r"micro leak test.*main",         "switch"),
        "leak_test_result_sensor": (r"leak test result.*main",        "sensor"),
        "leak_test_duration_sensor": (r"leak test duration.*main",    "number"),
        "volume_sensor":           (r"water volume total.*main",      "sensor"),
    },
    "irrigation": {
        "flow_sensor":             (r"water flow rate.*irrigation",         "sensor"),
        "pressure_fast_sensor":    (r"water pressure.*irrigation.*fast",    "sensor"),
        "pressure_avg_sensor":     (r"water pressure.*irrigation.*averaged","sensor"),
        "pressure_history_sensor": (r"water pressure.*irrigation(?!\s*\((?:fast|averaged))", "sensor"),
        "flow_onset_sensor":       (r"flow pulse onset.*irrigation",        "binary_sensor"),
        "valve_entity":            (r"irrigation water valve",              "valve"),
        "fault_sensor":            (r"safety fault.*irrigation",            "binary_sensor"),
        "fault_reason_sensor":     (r"fault reason.*irrigation|safety fault.*reason.*irrigation", "sensor"),
        "trickle_sensor":          (r"trickle.*alert.*irrigation",          "binary_sensor"),
        "leak_test_sensor":        (r"leak test active.*irrigation",        "binary_sensor"),
        "leak_test_switch":        (r"micro leak test.*irrigation",         "switch"),
        "leak_test_result_sensor": (r"leak test result.*irrigation",        "sensor"),
        "leak_test_duration_sensor": (r"leak test duration.*irrigation",    "number"),
        "volume_sensor":           (r"water volume total.*irrigation",      "sensor"),
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

    @property
    def display_name(self) -> str:
        return self.name_by_user or self.name

    @property
    def is_esphome(self) -> bool:
        return any(
            "esphome" in str(ident).lower()
            for ident in self.identifiers
        )


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
         or d.name_by_user.lower() == search_lower),
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


def match_entities_to_roles(
    device_id: str,
    entities: List[Dict[str, Any]],
    circuits: List[str],
) -> Tuple[Dict[str, List[EntityMatch]], str]:
    """
    Match entities belonging to device_id to circuit roles.

    Returns:
        (circuit_matches, esp_device_prefix)
        circuit_matches: dict of circuit → list of EntityMatch
        esp_device_prefix: the common prefix derived from entity IDs
    """
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
        matches = []

        for role, (pattern, expected_domain) in patterns.items():
            match = _find_entity_for_role(
                device_entities, pattern, expected_domain)
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
    compiled_name = re.compile(pattern, re.IGNORECASE)
    name_matches = [
        e for e in candidates
        if compiled_name.search(e.get("original_name") or "")
    ]
    return name_matches[0] if name_matches else candidates[0]


def _derive_prefix(entities: List[Dict[str, Any]]) -> str:
    """
    Derive the ESP device prefix from entity IDs.

    e.g. from 'sensor.esp_water_shut_off_3_water_flow_rate_main'
    extracts 'esp_water_shut_off_3_'
    """
    # Common known entity name suffixes from the firmware
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
    )


# ------------------------------------------------------------------
# Database helpers for persisting discovery results
# ------------------------------------------------------------------

def save_discovery(
    db: sqlite3.Connection,
    result: DiscoveryResult,
) -> None:
    """Persist a completed discovery result to SQLite."""
    now = __import__("datetime").datetime.utcnow().isoformat()

    db.execute("""
        UPDATE device_config SET
            ha_device_id = ?,
            ha_device_name = ?,
            esp_device_prefix = ?,
            setup_complete = 0,
            updated_at = ?
        WHERE id = 1
    """, (result.device.id, result.device.display_name,
          result.esp_device_prefix, now))

    db.execute("DELETE FROM circuit_entity_map")

    for circuit, matches in result.circuit_matches.items():
        for m in matches:
            db.execute("""
                INSERT INTO circuit_entity_map
                    (circuit, role, entity_id, entity_name, confirmed)
                VALUES (?, ?, ?, ?, 0)
            """, (circuit, m.role, m.entity_id, m.original_name))

    db.commit()


def mark_setup_complete(db: sqlite3.Connection) -> None:
    now = __import__("datetime").datetime.utcnow().isoformat()
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
