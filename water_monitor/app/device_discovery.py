"""
Device discovery — finds the ESP device in the HA device registry, maps its
entities to circuit roles, and persists the mapping in circuit_entity_map;
unmatched required roles are left for manual selection in the setup wizard.

Roles match on the entity's original_name (the ESPHome YAML `name:`), with
entity_id only as a fallback, so matching survives however HA normalises the
device name into the entity_id prefix.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from .database import run_db

log = logging.getLogger(__name__)

# Firmware floor, checked against device-registry sw_version (project.version in
# the ESPHome YAML). 3.13.0 is the last release that changed what the add-on
# READS: 3.12.0 added the flow_meter_ppl number entity (without it the add-on
# runs on the 396 ppl default — 5.5x high on a 72 ppl oval-gear meter, see
# Orchestrator._sync_ppl_and_watch) and 3.13.0 moved flow from pulse_counter to
# pulse_meter.
#
# firmware_status has three states, not two: "too_old" (parsed, < floor) is a
# positive measurement and blocks setup; "unknown" (absent, or not N.N.N — "dev",
# "", "unknown") warns loudly but must NOT block. project.version is free-form
# operator text, HA appends " (ESPHome x.y.z)", and a registry entry created
# before the `project:` block existed has no sw_version at all — every one of
# those is a correctly-flashed device, and refusing would strand a working
# install with no escape hatch inside the wizard.
MIN_FIRMWARE_VERSION: tuple = (3, 13, 0)

#: sw_version strings that carry no version information at all.
_UNKNOWN_FW_SENTINELS = {"", "unknown", "unavailable", "none", "null", "dev"}


def parse_firmware_version(sw_version: Optional[str]) -> Optional[Tuple[int, ...]]:
    """Parse an HA ``sw_version`` into a comparable tuple, or None if unknown.

    Handles the HA suffix ("3.14.0 (ESPHome 2026.6.1)" → ``(3, 14, 0)``).
    Returns None — never a guess — for anything non-numeric, so callers can
    tell "below the floor" apart from "cannot tell".
    """
    if not sw_version:
        return None
    version_str = str(sw_version).split("(")[0].strip()
    if version_str.lower() in _UNKNOWN_FW_SENTINELS:
        return None
    try:
        parts = tuple(int(x) for x in version_str.split(".")[:3])
    except ValueError:
        return None
    return parts or None

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
    # Firmware 3.13.2 — what the leak test actually measured. Optional so older
    # firmware still adopts; without them the scheduler falls back to reading
    # pressure when the result first turns "In progress".
    "leak_test_baseline_sensor",
    "leak_test_closed_sensor",
    "leak_settle_number",
    # "flow_meter_ppl" is deliberately NOT here — it is REQUIRED (see PPL_ROLE).
    # RESCAN_FILLABLE_ROLES still lets the rescan fill it on an older install.
    # Waveform diagnostic counters (firmware 3.7.0+ / 3.9.0+, circuit_1 only).
    # The 5 chunked text sensors were replaced by an HA event in firmware 3.8.0.
    # Chunk drop count was added in 3.9.0 when chunked streaming replaced the
    # single-event transport.
    "wf_overflow_count_sensor",
    "wf_chunk_drop_count_sensor",
    # The firmware's three waveform STAGE counters: captures started → chunks
    # staged → events fired, which with the add-on's own transport_stats
    # account for every missing waveform. Device-wide, not per-circuit, so —
    # like the two counters above — mapped under circuit_1 only.
    "wf_captures_started_sensor",
    "wf_chunks_staged_sensor",
    "wf_events_fired_sensor",
    # The end stops are the only ground truth for where the valve physically
    # is — the `valve.*` entity is a template the firmware publishes FROM them.
    # valve_seal_alert is the firmware's "flow against a closed valve"
    # detector. Read-only: nothing gates on these, they surface on
    # /health/detail.
    "open_end_stop_sensor",
    "closed_end_stop_sensor",
    "valve_seal_alert_sensor",
}

#: The runtime per-circuit flow-meter pulses-per-litre number entity
#: (firmware 3.12.0+, `ppl_main` / `ppl_irr`). Named because three separate
#: things key on it.
PPL_ROLE = "flow_meter_ppl"

# Why PPL is REQUIRED: unbound, the add-on runs on circuit_profile.pulses_per_litre
# (column DEFAULT 396.0) and nothing corrects it — the HA number-entity
# subscription is ppl's only write path and is skipped when unbound. This
# install's MAIN meter is 72-ppl oval-gear: 396 / 72 = 5.5, and every volume, the
# low-flow floor (60 / ppl) and every threshold scaled off them are wrong by 5.5x
# TOGETHER, which is why the result looks plausible rather than broken. A circuit
# rename unbinds it: every entity NAME is `${circuit_N_name}`-interpolated and HA
# derives entity_id from the name, so ~sixty ROLE_PATTERNS regexes fail at once.
#
# "Refuse" means the wizard will not hand back a runnable configuration, not an
# exception or crash-loop (a stuck add-on measures nothing, which is worse): out
# of OPTIONAL_ROLES, an unbound ppl makes DiscoveryResult.all_matched False, so
# setup.html renders the entity <select> `required` and step 3 cannot submit. A
# LIVE install that predates this (ppl row empty) is not killed — it keeps its
# cached ppl, the wizard names the circuits via unbound_ppl_circuits(),
# /health/detail reports them under "metering", and
# Orchestrator._sync_ppl_and_watch marks "flow_meter_ppl" degraded without
# stopping the circuit's detector.

#: Roles the optional-role rescan may FILL IN on an already-configured install.
#: OPTIONAL_ROLES plus `flow_meter_ppl`, which is required at setup but must
#: still self-heal on an install set up before firmware 3.12.0 published the
#: entity. merge_optional_roles is fill-only — it never overwrites a confirmed
#: or non-empty mapping — so widening this cannot clobber anything.
RESCAN_FILLABLE_ROLES = OPTIONAL_ROLES | {PPL_ROLE}


# Role patterns — circuit → role → (name regex, domain). Matched
# case-insensitively against original_name (entity_id as fallback); the domain
# disambiguates similarly-named entities. "main"/"irrigation" are the keywords in
# the DEFAULT firmware entity names; a renamed circuit binds through the
# diagnostic Circuit ID/Label sensors (tier 1 in match_entities_to_roles), which
# are tried first — these regexes are the fallback — and failing that through the
# wizard's manual entity assignment.
ROLE_PATTERNS: Dict[str, Dict[str, Tuple[str, str]]] = {
    "circuit_1": {   # regex patterns match the default firmware entity names
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
        # 3.13.2 — post-settle baseline and the pressure at valve close.
        "leak_test_baseline_sensor": (r"leak test baseline.*main",                      "sensor"),
        "leak_test_closed_sensor":  (r"leak test close pressure.*main",                 "sensor"),
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
        "leak_settle_number":         (r"leak test settle time.*main",                 "number"),
        # Runtime per-circuit flow-meter pulses-per-litre (firmware 3.12.0+).
        "flow_meter_ppl":             (r"flow meter ppl.*main",                        "number"),
        # Waveform diagnostic counters (firmware 3.7.0+ / 3.9.0+, circuit_1 only).
        # The 5 chunked text sensors were replaced by an HA event in firmware 3.8.0;
        # chunk drop count was added in 3.9.0 alongside the chunked streaming transport.
        "wf_overflow_count_sensor":   (r"waveform overflow dropped count.*main",       "sensor"),
        "wf_chunk_drop_count_sensor": (r"waveform chunk drop count.*main",             "sensor"),
        # Firmware waveform stage counters. These carry NO circuit keyword
        # (`name: "Waveform Captures Started"`), so _make_label_pattern has
        # nothing to substitute and the entity_id fallback never fires either —
        # original_name is the only thing that matches. Mapped under circuit_1
        # by convention, like the two counters above.
        "wf_captures_started_sensor": (r"waveform captures started",                   "sensor"),
        "wf_chunks_staged_sensor":    (r"waveform chunks staged",                      "sensor"),
        "wf_events_fired_sensor":     (r"waveform events fired",                       "sensor"),
        # Valve truth (end stops) + the valve-seal alert.
        "open_end_stop_sensor":       (r"open end stop.*main",                         "binary_sensor"),
        "closed_end_stop_sensor":     (r"closed end stop.*main",                       "binary_sensor"),
        "valve_seal_alert_sensor":    (r"valve seal alert.*main",                      "binary_sensor"),
    },
    "circuit_2": {   # regex patterns match the default firmware entity names
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
        # 3.13.2 — post-settle baseline and the pressure at valve close.
        "leak_test_baseline_sensor": (r"leak test baseline.*irrigation",                      "sensor"),
        "leak_test_closed_sensor":  (r"leak test close pressure.*irrigation",                 "sensor"),
        "volume_sensor":           (r"water volume total.*irrigation",                        "sensor"),
        # Reset buttons (firmware v3.6+)
        # Display names use ${circuit_2_name} → "Irrigation"; entity_id suffix fallback uses _irr\b
        # (_irr appears in ESPHome internal IDs; \b prevents matching "irrigation" display names)
        "fault_reset_button":         (r"reset safety fault.*irrigation|reset_safety_fault.*_irr\b",           "button"),
        "trickle_reset_button":       (r"reset trickle alert.*irrigation|reset_trickle_alert.*_irr\b",         "button"),
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
        "leak_settle_number":         (r"leak test settle time.*irrigation|leak_settle_s_irr\b",               "number"),
        # Runtime per-circuit flow-meter pulses-per-litre (firmware 3.12.0+).
        "flow_meter_ppl":             (r"flow meter ppl.*irrigation|ppl_irr\b",                                "number"),
        # The firmware ids are open_end_stop_valve2 / closed_end_stop_valve2,
        # but HA derives the entity_id from the NAME ("Open End Stop -
        # Irrigation"), so the display term is what matches on both tiers.
        "open_end_stop_sensor":       (r"open end stop.*irrigation",                                           "binary_sensor"),
        "closed_end_stop_sensor":     (r"closed end stop.*irrigation",                                         "binary_sensor"),
        "valve_seal_alert_sensor":    (r"valve seal alert.*irrigation",                                        "binary_sensor"),
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
    def firmware_status(self) -> str:
        """One of "ok" / "too_old" / "unknown" — see MIN_FIRMWARE_VERSION."""
        parts = parse_firmware_version(self.sw_version)
        if parts is None:
            log.warning(
                "Firmware version %r cannot be parsed — compatibility is "
                "UNVERIFIED (minimum required: %s). Setup is not blocked, but "
                "nothing has confirmed this device publishes the entities the "
                "add-on reads.",
                self.sw_version,
                ".".join(str(x) for x in MIN_FIRMWARE_VERSION),
            )
            return "unknown"
        return "ok" if parts >= MIN_FIRMWARE_VERSION else "too_old"

    @property
    def firmware_ok(self) -> bool:
        """True ONLY when sw_version was parsed and meets MIN_FIRMWARE_VERSION.

        Strictly a positive statement: an unverifiable version is not ok, but
        it is not a reason to refuse either. Use
        :attr:`firmware_blocks_setup` for the "may this device proceed"
        question.
        """
        return self.firmware_status == "ok"

    @property
    def firmware_blocks_setup(self) -> bool:
        """True only for a VERIFIED sub-floor firmware. See MIN_FIRMWARE_VERSION
        for why "unknown" is warned about rather than blocked."""
        return self.firmware_status == "too_old"


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
    """Search for devices matching search_name.

    Returns ``(exact_match, suggestions)``: a case-insensitive exact name match
    (else None), and otherwise substring matches, falling back to every ESPHome
    device when nothing matches.
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


# The four diagnostic identity sensors are the ONLY entities on the device
# whose own names are not `${circuit_N_name}`-interpolated — the firmware
# hardcodes "Circuit 1 ID" / "Circuit 1 Label" / "Circuit 2 ID" / "Circuit 2
# Label". They are the only rename-stable handles the add-on has.
_CIRCUIT_ID_SENSOR_RE = re.compile(r"circuit\s+(\d+)\s+id\b", re.IGNORECASE)
_CIRCUIT_LABEL_SENSOR_RE = re.compile(r"circuit\s+(\d+)\s+label\b", re.IGNORECASE)
#: The ID sensor publishes a literal circuit key ("circuit_1"). Anything else
#: is a stale/unavailable state and is not trusted.
_CIRCUIT_KEY_RE = re.compile(r"^circuit_\d+$")
#: HA state strings that mean "no value", not a label.
_NO_STATE = {"", "unknown", "unavailable", "none"}


async def resolve_circuit_identity(
    ha,
    entity_registry_entities: List[Dict[str, Any]],
) -> Tuple[Dict[str, str], List[str]]:
    """Resolve circuit identity from the v3.6+ diagnostic sensors.

    Returns ``(labels, circuits_seen)``: ``{circuit_id: display_label}`` and
    every circuit the DEVICE reports, whether or not its label resolved;
    ``({}, [])`` when no diagnostic sensors are present.

    Binding is anchored on the ID sensor's STATE — the firmware's own circuit
    key ("circuit_1") — not on the ordinal in the sensor's name, so a
    duplex/swapped install attaches its label to the circuit the device claims;
    the Label sensor is paired to it by that ordinal (both names are hardcoded
    "Circuit N ..."). ``circuits_seen`` is separate because both are
    `update_interval: 60s` templates: after boot the ID can resolve while the
    Label is still `unknown`, and without it that window is indistinguishable
    from "older firmware, no diagnostic sensors" — the "main"/"irrigation"
    regex fallback, which fails silently on a renamed circuit.
    """
    id_entities: Dict[str, Dict[str, Any]] = {}
    label_entities: Dict[str, Dict[str, Any]] = {}
    for entity in entity_registry_entities:
        name = entity.get("original_name") or entity.get("name") or ""
        m = _CIRCUIT_ID_SENSOR_RE.search(name)
        if m:
            id_entities[m.group(1)] = entity
            continue
        m = _CIRCUIT_LABEL_SENSOR_RE.search(name)
        if m:
            label_entities[m.group(1)] = entity

    # Ordinal ("1") → circuit key ("circuit_1"), from the ID sensor's state.
    identity: Dict[str, str] = {}
    for ordinal, entity in id_entities.items():
        state = (await ha.get_state_value(entity["entity_id"], None) or "")
        state = str(state).strip()
        if _CIRCUIT_KEY_RE.match(state):
            identity[ordinal] = state
        else:
            # Sensor exists (so the circuit exists) but its state is not usable
            # yet. Fall back to the ordinal in its own hardcoded name.
            identity[ordinal] = f"circuit_{ordinal}"

    labels: Dict[str, str] = {}
    for ordinal, entity in label_entities.items():
        circuit_id = identity.get(ordinal, f"circuit_{ordinal}")
        state = (await ha.get_state_value(entity["entity_id"], None) or "")
        state = str(state).strip()
        if state.lower() in _NO_STATE:
            continue
        labels[circuit_id] = state

    circuits_seen = sorted(
        set(identity.values())
        | {f"circuit_{o}" for o in label_entities if o not in identity}
        | set(labels.keys())
    )
    if circuits_seen:
        log.info("Diagnostic circuit identity resolved: circuits=%s labels=%s",
                 circuits_seen, labels)
        for circuit in circuits_seen:
            if circuit not in labels:
                log.warning(
                    "Circuit %s is reported by its diagnostic ID sensor but its "
                    "display label did not resolve — entity matching will fall "
                    "back to the default \"main\"/\"irrigation\" name patterns, "
                    "which do NOT match a renamed circuit.", circuit)
    return labels, circuits_seen


async def _resolve_labels_from_diagnostics(
    ha,
    entity_registry_entities: List[Dict[str, Any]],
) -> Dict[str, str]:
    """{circuit_id: label} from the diagnostic sensors — the labels-only view
    of :func:`resolve_circuit_identity`."""
    labels, _seen = await resolve_circuit_identity(ha, entity_registry_entities)
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

    Three ordered tiers per role: (1) the circuit's diagnostic *label* (from
    :func:`_resolve_labels_from_diagnostics`) substituted into the pattern —
    ``re.escape``d, since labels are user-controlled — which binds a renamed
    circuit; then the ROLE_PATTERNS regex itself, which carries (2) the default
    "main"/"irrigation" display terms and (3) the ``_main`` / ``_irr\\b``
    entity_id suffix fallback.

    Returns ``(circuit_matches, esp_device_prefix)``.
    """
    labels = labels or {}
    device_entities = [e for e in entities if e.get("device_id") == device_id]

    log.info("Device %s has %d registered entities",
             device_id, len(device_entities))

    # sensor.esp_water_shut_off_3_water_flow_rate_main → esp_water_shut_off_3_
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
    # Suffixes in PREFERENCE order. Suffix is the OUTER loop on purpose: with
    # entities outermost the result depends on registry order, because
    # `button.<prefix>reset_safety_fault_main` also ends in "safety_fault_main"
    # and yields a prefix with a bogus "reset_" tail that makes the waveform
    # accumulator reject every chunk (node != expected). "water_flow_rate_*"
    # have no such trap variants. New firmware entity types: extend this list.
    known_suffixes = [
        # The four diagnostic identity sensors first: every other suffix below
        # is `${circuit_N_name}`-derived, so renaming a circuit deletes ALL of
        # them and the prefix silently becomes "" (which, per the note at the
        # bottom of this function, DISABLES the waveform node-identity check).
        # These four are hardcoded in firmware and have no trap variants.
        "circuit_1_id",
        "circuit_2_id",
        "circuit_1_label",
        "circuit_2_label",
        "water_flow_rate_main",
        "water_flow_rate_irrigation",
        "water_volume_total_main",
        "safety_fault_main",
        "safety_fault_irrigation",
    ]

    locals_ = [
        eid.split(".", 1)[1]
        for eid in (e.get("entity_id", "") for e in entities)
        if "." in eid  # strip domain prefix (sensor., binary_sensor., etc.)
    ]
    for suffix in known_suffixes:
        for local in locals_:
            if local.endswith(suffix) and len(local) > len(suffix):
                prefix = local[: len(local) - len(suffix)]
                if prefix:
                    log.debug("Derived ESP prefix: %r", prefix)
                    return prefix

    # Not harmless: the empty prefix is stored as device_config.esp_device_prefix
    # and becomes the waveform accumulator's `expected_node`, whose guard is
    # `if self._expected_node and node != ...` — so it DISABLES the node-identity
    # check for the life of the process rather than rejecting chunks. The only
    # other symptom is WaveformChunkAccumulator.transport_stats()
    # ["node_check_enabled"] on /health/detail.
    log.warning(
        "Could not derive an ESP device prefix from %d entity id(s) — none "
        "ended in a known suffix (%s). The waveform node-identity check will "
        "be DISABLED (any node's chunks are accepted), and anything else "
        "keying on the prefix will misbehave. Re-run device discovery, or "
        "extend _derive_prefix's known_suffixes for this firmware.",
        len(entities), ", ".join(known_suffixes),
    )
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


# Database helpers for persisting discovery results

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

        # Clear fixture data from the previous setup so stale clusters and
        # fixtures don't bleed into the new setup's labelling flow.
        db.execute("DELETE FROM fixture_clusters")
        # fixture_ha_entity_map and fixture_daily_summary reference fixtures(id)
        # without ON DELETE CASCADE, so they must be cleared first. MQTT
        # Discovery entities already published to HA are NOT retracted here.
        db.execute("DELETE FROM fixture_daily_summary")
        # events.fixture_id references fixtures(id) with no ON DELETE action,
        # so events must be unlinked BEFORE fixtures are deleted.
        db.execute("UPDATE events SET cluster_id = NULL, fixture_id = NULL")
        db.execute("DELETE FROM fixtures")
        db.execute("UPDATE training_state SET state = 'idle'")

        for circuit, matches in result.circuit_matches.items():
            for m in matches:
                db.execute("""
                    INSERT INTO circuit_entity_map
                        (circuit, role, entity_id, entity_name, confirmed)
                    VALUES (?, ?, ?, ?, 0)
                """, (circuit, m.role, m.entity_id, m.original_name))

    # This UPDATE cleared setup_complete above — tell every cached copy.
    bump_setup_complete_epoch()


# Wizard-completion epoch. is_setup_complete() is a real SQLite SELECT, which
# ingress_middleware would otherwise run on the EVENT-LOOP thread for every
# non-setup request (app.js polls /api/dashboard/live every 5 s per tab); the
# Orchestrator caches the answer and this counter is how it learns it is stale.
# A module counter rather than per-writer invalidation because every write to
# device_config.setup_complete lives in THIS module (save_discovery,
# mark_setup_complete, unmark_setup_complete). An older epoch reads as unknown
# and is re-read, so the cache never answers from a value that predates a write.
_SETUP_COMPLETE_EPOCH = 0


def setup_complete_epoch() -> int:
    """Monotone counter, bumped on every write to device_config.setup_complete.

    Cheap in-memory read. A cached copy of the flag is valid only while the
    epoch it was read at still matches this.
    """
    return _SETUP_COMPLETE_EPOCH


def bump_setup_complete_epoch() -> None:
    """Invalidate every in-memory copy of the wizard-completion flag.

    The three writers in this module call it themselves. Call it directly from
    anything else that writes device_config.setup_complete another way — a
    table-level restore, or a test poking the column on a side connection.
    """
    global _SETUP_COMPLETE_EPOCH
    _SETUP_COMPLETE_EPOCH += 1


def mark_setup_complete(db: sqlite3.Connection) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    db.execute("""
        UPDATE device_config SET setup_complete = 1, updated_at = ?
        WHERE id = 1
    """, (now,))
    db.commit()
    bump_setup_complete_epoch()


def unmark_setup_complete(db: sqlite3.Connection) -> None:
    """Re-open the setup wizard (Settings → Re-run Setup).

    is_setup_complete() reads device_config.setup_complete — home_profile
    has its own setup_complete column, but the wizard lock only looks here.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    db.execute("""
        UPDATE device_config SET setup_complete = 0, updated_at = ?
        WHERE id = 1
    """, (now,))
    db.commit()
    bump_setup_complete_epoch()


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


def unbound_ppl_circuits(
    db: sqlite3.Connection,
    circuits: Optional[List[str]] = None,
) -> List[str]:
    """Circuits whose flow-meter PPL entity is NOT bound in circuit_entity_map —
    i.e. computing volumes from the UNVERIFIED circuit_profile cache (column
    default 396.0; a silent 5.5x over-count on a 72-ppl oval-gear meter — see
    the PPL_ROLE note).

    Both shapes count as unbound: no row at all (the role never matched) and a
    row with an empty entity_id (matched=False was persisted). Read-only; the
    setup wizard uses it to REPORT the condition rather than let it stay invisible.
    """
    rows = db.execute(
        "SELECT circuit, entity_id FROM circuit_entity_map WHERE role = ?",
        (PPL_ROLE,),
    ).fetchall()
    bound = {r[0] for r in rows if (r[1] or "").strip()}

    if circuits is None:
        known = [r[0] for r in db.execute(
            "SELECT DISTINCT circuit FROM circuit_entity_map ORDER BY circuit"
        ).fetchall()]
    else:
        known = list(circuits)
    return [c for c in known if c not in bound]


# Optional-role re-discovery after firmware upgrades

# sw_version values that must never be written to fw_version.
# Checked case-insensitively after stripping whitespace.
_UNTRUSTWORTHY_FW = {"", "unknown", "unavailable", "none", "null"}


def merge_optional_roles(
    db: sqlite3.Connection,
    circuit: str,
    matches: List[EntityMatch],
) -> int:
    """Fill optional-role rows in circuit_entity_map without overwriting
    user-confirmed or already-mapped entries. Rules, in order per match:

    1. Skip unless ``m.matched``, ``m.entity_id`` and
       ``m.role in RESCAN_FILLABLE_ROLES``.
    2. Row missing → INSERT with confirmed=0.
    3. Row exists, entity_id NULL/empty, confirmed=0 → UPDATE entity_id/entity_name.
    4. Row exists, entity_id non-empty → do NOT overwrite.
    5. Row exists, confirmed=1 → do NOT overwrite, even with an empty entity_id.

    Returns rows inserted or updated; idempotent (a second run returns 0) and
    commits only when something changed.
    """
    changed = 0
    cursor = db.cursor()

    for m in matches:
        if not m.matched or not m.entity_id or m.role not in RESCAN_FILLABLE_ROLES:
            continue

        row = cursor.execute(
            "SELECT entity_id, confirmed FROM circuit_entity_map WHERE circuit=? AND role=?",
            (circuit, m.role),
        ).fetchone()

        if row is None:
            # Rule 2: row missing → INSERT
            cursor.execute(
                """INSERT INTO circuit_entity_map (circuit, role, entity_id, entity_name, confirmed)
                   VALUES (?, ?, ?, ?, 0)""",
                (circuit, m.role, m.entity_id, m.original_name),
            )
            changed += cursor.rowcount
        else:
            existing_id = row[0] or ""
            confirmed = row[1] or 0
            if existing_id or confirmed:
                # Rule 4 or 5: non-empty entity_id OR confirmed=1 → do NOT overwrite
                continue
            # Rule 3: entity_id NULL/empty and confirmed=0 → UPDATE
            cursor.execute(
                """UPDATE circuit_entity_map
                   SET entity_id=?, entity_name=?
                   WHERE circuit=? AND role=?
                     AND (entity_id IS NULL OR entity_id='')
                     AND confirmed=0""",
                (m.entity_id, m.original_name, circuit, m.role),
            )
            changed += cursor.rowcount

    if changed:
        db.commit()

    return changed


@dataclass
class OptionalRoleRescanResult:
    """Result of :func:`rescan_optional_roles`."""
    total_changed: int
    per_circuit: Dict[str, int]
    fw_version_updated: bool = False
    fw_version: Optional[str] = None
    prefix_updated: bool = False


def _rescan_writes_sync(db, circuits, circuit_matches, target_device,
                        _prefix) -> dict:
    """Every write behind an optional-role rescan, in one DB hop.

    Re-reads device_config first: the caller's copy predates the HA registry
    fetch, and both "update if changed" writes below must compare against
    current state.
    """
    cfg = get_device_config(db) or {}
    prefix_updated = False
    stored_prefix = (cfg.get("esp_device_prefix") or "").strip()
    if _prefix and _prefix != stored_prefix:
        db.execute(
            "UPDATE device_config SET esp_device_prefix=? WHERE id=1",
            (_prefix,),
        )
        db.commit()
        prefix_updated = True
        log.info(
            "rescan_optional_roles: esp_device_prefix updated %r → %r",
            stored_prefix, _prefix,
        )

    per_circuit: Dict[str, int] = {}
    total_changed = 0
    for circuit in circuits:
        n = merge_optional_roles(db, circuit, circuit_matches.get(circuit, []))
        per_circuit[circuit] = n
        if n:
            log.info("[%s] %d new optional entities discovered", circuit, n)
        total_changed += n

    # Update stored fw_version if the reported version is trustworthy and different.
    # Never overwrite with empty / sentinel strings.
    fw_version_updated = False
    new_fw: Optional[str] = None
    raw_sw = (target_device.sw_version or "").strip()
    if raw_sw and raw_sw.lower() not in _UNTRUSTWORTHY_FW:
        # Strip HA suffix: "3.7.0 (ESPHome 2024.11.0)" → "3.7.0"
        fw_str = raw_sw.split("(")[0].strip()
        if fw_str:
            stored_fw = (cfg.get("fw_version") or "").strip()
            if fw_str != stored_fw:
                db.execute(
                    "UPDATE device_config SET fw_version=? WHERE id=1",
                    (fw_str,),
                )
                db.commit()
                fw_version_updated = True
                new_fw = fw_str
                log.info(
                    "rescan_optional_roles: fw_version updated %r → %r",
                    stored_fw, fw_str,
                )

    return {"prefix_updated": prefix_updated, "per_circuit": per_circuit,
            "total_changed": total_changed,
            "fw_version_updated": fw_version_updated, "new_fw": new_fw}


async def rescan_optional_roles(
    ha,
    db: sqlite3.Connection,
    circuits: List[str],
) -> OptionalRoleRescanResult:
    """Scan the HA entity registry for optional roles not yet in circuit_entity_map.

    Safe to call on an already-configured system — never invokes
    :func:`save_discovery`, never overwrites confirmed or non-empty mappings.
    Idempotent: a second run against a fully-populated DB returns 0 changed rows.

    Returns an :class:`OptionalRoleRescanResult` describing what changed.
    """
    zero = OptionalRoleRescanResult(
        total_changed=0,
        per_circuit={c: 0 for c in circuits},
    )

    cfg = get_device_config(db)
    if not cfg or not cfg.get("setup_complete") or not cfg.get("ha_device_id"):
        log.debug("rescan_optional_roles: setup not complete or ha_device_id missing — skipping")
        return zero

    ha_device_id: str = cfg["ha_device_id"]

    try:
        devices = await ha.get_devices()
        entity_registry = await ha.get_entity_registry()
    except Exception as exc:  # pragma: no cover
        log.warning("rescan_optional_roles: failed to query HA — %s", exc)
        return zero

    target_device: Optional[DiscoveredDevice] = None
    for raw in devices:
        d = _to_device(raw)
        if d.id == ha_device_id:
            target_device = d
            break

    if target_device is None:
        log.warning(
            "rescan_optional_roles: device %s not found in HA device registry",
            ha_device_id,
        )
        return zero

    # Device-scoped, to prevent cross-device contamination.
    device_entities = [e for e in entity_registry if e.get("device_id") == ha_device_id]

    try:
        diag_labels = await _resolve_labels_from_diagnostics(ha, device_entities)
    except Exception as exc:  # pragma: no cover
        log.warning("rescan_optional_roles: label resolution failed — %s", exc)
        diag_labels = {}

    circuit_matches, _prefix = match_entities_to_roles(
        ha_device_id, device_entities, circuits, labels=diag_labels
    )

    # Heals a stale/mis-derived esp_device_prefix, which save_discovery is
    # otherwise the only writer of — so a bad stored value persists across
    # restarts and the waveform accumulator rejects every chunk (node != expected).
    # The writes re-read device_config inside the callable (see
    # _rescan_writes_sync): a save_discovery landing during the registry fetch
    # above would otherwise be overwritten from the stale `cfg`.
    # merge_optional_roles is fill-only, so it needs no re-check.
    _w = await run_db(_rescan_writes_sync, db, circuits, circuit_matches,
                      target_device, _prefix)
    prefix_updated    = _w["prefix_updated"]
    per_circuit       = _w["per_circuit"]
    total_changed     = _w["total_changed"]
    fw_version_updated = _w["fw_version_updated"]
    new_fw            = _w["new_fw"]

    return OptionalRoleRescanResult(
        total_changed=total_changed,
        per_circuit=per_circuit,
        fw_version_updated=fw_version_updated,
        fw_version=new_fw,
        prefix_updated=prefix_updated,
    )
