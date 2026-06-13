"""
Fixture taxonomy and heuristic suggestion rules.

This module is the single source of truth for the fixture types the addon
recognises, and the rules used to suggest a type for a freshly-clustered
event group based on its centroid features.

The taxonomy is intentionally extensible: adding a new fixture type means
appending to FIXTURE_TYPES and (optionally) adding a heuristic rule.
No database migration is required — the events.fixture_id and
fixtures.fixture_type columns are TEXT, so any string in FIXTURE_TYPES
is valid.

Heuristic rules use centroid features ONLY (the average flow rate, average
duration, average pressure delta, etc. of all events in a cluster).
Per-event features and time-of-day patterns are intentionally not used
here — those are the cluster engine's job.

Rules return (type_string, confidence_float_0_to_1) or None.
The first rule that returns a non-None result wins.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

# ── Taxonomy ──────────────────────────────────────────────────────────────
# Order matters for UI dropdowns (grouped sensibly), but the algorithm
# treats them as a flat set.

FIXTURE_TYPES: List[str] = [
    # User-selectable (household fixture circuit)
    "toilet",
    "shower_tub",
    "tap",
    "washing_machine",
    "dishwasher",
    "water_softener",
    # Zone-circuit only
    "irrigation_zone",
    # Catch-all / internal
    "other",
    "leak_test",
]

# Mapping from the old 23-entry taxonomy to the new 8-entry taxonomy.
# Used by db_migrations to rewrite stored type strings, and importable
# by tests as a regression guard.
LEGACY_TYPE_REMAP: Dict[str, str] = {
    # Unchanged
    "toilet":                "toilet",
    "washing_machine":       "washing_machine",
    "dishwasher":            "dishwasher",
    "irrigation_zone":       "irrigation_zone",
    "other":                 "other",
    "leak_test":             "leak_test",
    # Merged
    "shower":                "shower_tub",
    "bath":                  "shower_tub",
    "bidet":                 "toilet",
    "bathroom_tap":          "tap",
    "kitchen_tap":           "tap",
    "utility_tap":           "tap",
    # Folded into other
    "ice_maker":             "other",
    "refrigerator_water":    "other",
    "ro_drinking_faucet":    "other",
    "humidifier":            "other",
    # dev.24: water_softener is a first-class type again — preserve it through
    # _canonical_fixture_type so user labels + session matches store correctly.
    "water_softener":        "water_softener",
    "ro_system_whole_house": "other",
    "evaporative_cooler":    "other",
    "boiler_makeup":         "other",
    "hose_bib":              "other",
    "outdoor_tap":           "other",
    "pool_fill":             "other",
}

# Friendly display labels (UI shows these instead of the raw key)
FIXTURE_TYPE_LABELS: Dict[str, str] = {
    "toilet":           "Toilet",
    "shower_tub":       "Shower / Tub",
    "tap":              "Tap",
    "washing_machine":  "Washing Machine",
    "dishwasher":       "Dishwasher",
    "water_softener":   "Water Softener",
    "irrigation_zone":  "Irrigation Zone",
    "other":            "Other",
    "leak_test":        "Leak Test (auto)",
}

# Types that are NOT user-selectable in the UI (auto-managed only).
# The leak test scheduler tags its synthetic events with this type so
# they never show up as a real fixture for clustering or HA publishing.
INTERNAL_FIXTURE_TYPES: List[str] = ["leak_test"]

# HA publishing categories. Each fixture type is its own category now —
# the taxonomy IS the category set. Used only in fixture_publisher to
# name HA entities. Never stored in the database.
FIXTURE_CATEGORIES: Dict[str, Optional[str]] = {
    "toilet":           "toilet",
    "shower_tub":       "shower_tub",
    "tap":              "tap",
    "washing_machine":  "washing_machine",
    "dishwasher":       "dishwasher",
    "water_softener":   "water_softener",
    "irrigation_zone":  "irrigation_zone",
    "other":            "other",
    "leak_test":        None,    # never publish
}

FIXTURE_CATEGORY_LABELS: Dict[str, str] = {
    "toilet":           "Toilet",
    "shower_tub":       "Shower / Tub",
    "tap":              "Tap",
    "washing_machine":  "Washing Machine",
    "dishwasher":       "Dishwasher",
    "water_softener":   "Water Softener",
    "irrigation_zone":  "Irrigation Zone",
    "other":            "Other",
}


def get_fixture_category(fixture_type: Optional[str]) -> Optional[str]:
    """Return the HA publishing category for a fixture type.

    Since the taxonomy consolidation (Sprint D) the type IS the category,
    so this function is effectively an identity for known types.
    Returns None for leak_test (never published).
    Returns 'other' for None or any type not in FIXTURE_CATEGORIES.
    """
    if fixture_type is None:
        return "other"
    return FIXTURE_CATEGORIES.get(fixture_type, "other")


def is_valid_fixture_type(name: Optional[str]) -> bool:
    """True if `name` is a recognised fixture type (or None)."""
    return name is None or name in FIXTURE_TYPES


def user_selectable_types() -> List[str]:
    """Fixture types the user can pick from in the UI."""
    return [t for t in FIXTURE_TYPES if t not in INTERNAL_FIXTURE_TYPES]


# ── Circuit type taxonomy ─────────────────────────────────────────────────

CIRCUIT_TYPES: List[str] = ["fixture", "zone"]

CIRCUIT_TYPE_LABELS: Dict[str, str] = {
    "fixture": "Fixture / main shutoff",
    "zone":    "Irrigation / zone valve",
}

CIRCUIT_TYPE_HELP: Dict[str, str] = {
    "fixture": "Learns normal household fixture signatures such as toilets, showers, taps, appliances, and hose bibs.",
    "zone":    "Learns irrigation-zone flow patterns and enables zone-specific alert types.",
}

# Zone-only alert types — used by settings UI to hide irrelevant alerts on fixture circuits.
ZONE_ONLY_ALERT_TYPES: frozenset = frozenset({
    "pre_solenoid_leak",
    "solenoid_weeping",
    "zone_flow_deviation_high",
    "zone_flow_deviation_low",
    "zone_duration_overrun",
})

# Fixture types appropriate for a zone (irrigation) circuit.
# hose_bib and pool_fill were folded into "other" during the Sprint D
# taxonomy consolidation; zone circuits now offer only two choices.
_ZONE_FIXTURE_TYPES: List[str] = ["irrigation_zone", "other"]


def normalize_circuit_type(value: Optional[str]) -> str:
    """Map legacy "irrigation" → "zone"; unknown/None → "fixture"."""
    if value == "irrigation":
        return "zone"
    if value in CIRCUIT_TYPES:
        return value
    return "fixture"


# ── Valve type taxonomy ───────────────────────────────────────────────────
#
# 2-port: standard inline ball valve (the default; micro leak test works).
# 3-port: drain-capable valve (third port supports drain/winterization
#         plumbing). The micro leak test is incompatible because the drain
#         port always vents, so any test would read as a constant leak.
#
# Wording deliberately avoids implying the addon performs auto-drain — only
# that the hardware supports it. Future work may add an auto-drain workflow.

VALVE_TYPES: List[str] = ["2_port", "3_port"]
DEFAULT_VALVE_TYPE: str = "2_port"

VALVE_TYPE_LABELS: Dict[str, str] = {
    "2_port": "2-port (standard)",
    "3_port": "3-port (drain-capable)",
}

VALVE_TYPE_HELP: Dict[str, str] = {
    "2_port": "Standard inline ball valve. Micro leak test enabled.",
    "3_port": ("Third port supports drain/winterization plumbing. "
               "Micro leak test is disabled because a drain port would "
               "always read as a constant leak."),
}


def normalize_valve_type(value: Optional[str]) -> str:
    """FORGIVING fallback for reads / display.

    Returns one of VALVE_TYPES; coerces blanks, unknowns, and legacy values
    to DEFAULT_VALVE_TYPE so callers can always render something without
    crashing.

    Accepts "3-port", "3_port", "3 port", "3 -PORT" etc. via the regex.
    Use parse_valve_type() instead for POST/API writes that must reject
    garbage rather than silently default.
    """
    if not value:
        return DEFAULT_VALVE_TYPE
    v = re.sub(r"[\s-]+", "_", str(value).strip().lower())
    return v if v in VALVE_TYPES else DEFAULT_VALVE_TYPE


def parse_valve_type(value: Optional[str]) -> Optional[str]:
    """STRICT parser for form / API writes.

    Returns one of VALVE_TYPES on valid input, or None on anything invalid
    (including blanks, None, and unknown tokens).

    POST handlers MUST use this rather than normalize_valve_type — otherwise
    garbage like 'banana' would silently become '2_port' and any
    validation error path would be unreachable.
    """
    if value is None:
        return None
    v = re.sub(r"[\s-]+", "_", str(value).strip().lower())
    return v if v in VALVE_TYPES else None


def zone_user_selectable_types() -> List[str]:
    """Fixture types appropriate for a zone (irrigation) circuit."""
    return [t for t in _ZONE_FIXTURE_TYPES if t not in INTERNAL_FIXTURE_TYPES]


def fixture_user_selectable_types() -> List[str]:
    """Fixture types appropriate for a household fixture circuit."""
    _zone_only = {"irrigation_zone"}
    return [t for t in FIXTURE_TYPES if t not in INTERNAL_FIXTURE_TYPES and t not in _zone_only]


def normalize_fixture_type_for_circuit(
    name: Optional[str],
    circuit_kind: str,
) -> str:
    """Coerce any string into a canonical category for the given circuit.

    Used by the Sprint F per-category rollup (Fixtures page) to ensure stored
    type strings — which may include legacy values predating Sprint D, user
    typos, display labels, or wrong-circuit-kind types — always collapse to a
    valid category for the circuit being rendered.

    circuit_kind: ``"fixture"`` (the default) or ``"zone"``. Anything else is
    treated as ``"fixture"``. Unknown / blank / legacy / wrong-kind inputs all
    collapse to ``"other"``. NEVER returns ``"leak_test"`` (internal-only).

    Normalization stages, in order:
      1. None / non-string / empty after strip → ``"other"``.
      2. Strip whitespace, lowercase, collapse hyphens/spaces to ``_``.
      3. Strip a trailing ``/`` and split on ``/`` — take the first segment
         (catches display labels like ``"Shower / Tub"`` → ``"shower"``).
      4. Look up in LEGACY_TYPE_REMAP (Sprint D alias map).
      5. If the result is in the allowed-set for this circuit kind, return
         it; otherwise return ``"other"``.
    """
    if not isinstance(name, str):
        return "other"
    s = name.strip().lower()
    if not s:
        return "other"
    # Take the first '/'-separated segment FIRST so display labels like
    # "Shower / Tub" canonicalize via 'shower' (and not 'shower_/_tub' after
    # whitespace-to-underscore folding). Strip both the leading/trailing '/'
    # and surrounding whitespace before the next stage.
    s = s.strip("/").split("/", 1)[0].strip()
    # Collapse hyphens / internal whitespace to underscores; strip stray
    # leading/trailing underscores.
    s = re.sub(r"[\s\-]+", "_", s).strip("_")
    if not s:
        return "other"
    # Apply Sprint D alias map (shower→shower_tub, bathroom_tap→tap,
    # hose_bib→other, etc.).
    canonical = LEGACY_TYPE_REMAP.get(s, s)
    allowed = (set(zone_user_selectable_types())
               if circuit_kind == "zone"
               else set(fixture_user_selectable_types()))
    return canonical if canonical in allowed else "other"


# ── Variance profiles for type-aware cluster matching ────────────────────
# Each fixture type has fundamentally different variance characteristics:
#
#   - Deterministic fixtures (toilet, ice_maker, refrigerator_water): tank
#     volume + duration are set by physics. Tight Gaussians; high variance
#     is itself a fault signal. Use small match thresholds.
#   - User-driven fixtures (shower, bath, taps, hose_bib): duration and
#     volume are behavioural choices. Anchor on flow rate / pressure delta
#     and let duration / volume "float" so we don't create spurious sub-
#     clusters when the user takes a longer shower.
#   - Programme-driven fixtures (washing_machine, dishwasher, water_softener):
#     multimodal — quick cycle vs. cottons cycle look like two fixtures but
#     are one appliance. The `multimodal` flag here is scaffolding for the
#     deferred Phase 2.2 multi-cluster fixture grouping (see
#     docs/multimodal-fixtures.md when written).
#
# Schema:
#   anchor_weights:  feature_name -> weight (>1 amplifies importance)
#   float_features:  feature names whose differences should be ignored
#                    (assigned weight 0 in cluster_engine._build_match_weights)
#   expected_cv:     feature_name -> coefficient-of-variation expected at
#                    healthy operation. Scaffolding for cluster_metrics.py
#                    (out of scope today; values inform the future drift
#                    monitor).
#   multimodal:      placeholder for the Phase 2.2 multi-cluster grouping.
#
# Live FEATURE_KEYS (cluster_engine.py:45) today:
#   avg_flow_lpm, peak_flow_lpm, duration_seconds, volume_litres,
#   pressure_delta_psi, has_pressure_transient, flow_variability,
#   hour_sin, hour_cos
#
# Some entries below reference forward-looking features (resistance_curve_shape,
# hydraulic_resistance, duration_log, day_of_week, is_weekend) that are not
# yet in FEATURE_KEYS. They are harmless today (the weighted-distance loop
# iterates over FEATURE_KEYS only) and will become active when those features
# are added by feature_extractor.

FIXTURE_VARIANCE_PROFILES: Dict[str, Dict] = {
    # ── Deterministic ────────────────────────────────────────────────────
    "toilet": {
        # Type-level weights: lowered volume/duration/flow from 3.0/3.0/2.0 to
        # 1.5 each so that two cisterns with slightly different flush sizes both
        # match the type centroid. Pressure-transient anchors are unchanged —
        # these remain the sharpest cross-type discriminators.
        "anchor_weights": {
            "volume_litres":              1.5,
            "duration_seconds":           1.5,
            "avg_flow_lpm":               1.5,
            "has_pressure_transient":     2.0,
            "resistance_curve_shape":     2.0,   # forward-looking
            "recovery_overshoot_psi":     1.5,   # water hammer on cistern snap-shut
            "pressure_oscillation_count": 1.5,
            "pressure_onset_ms":          1.0,
        },
        "float_features": {"hour_sin", "hour_cos", "day_of_week", "is_weekend"},
        "expected_cv": {"volume_litres": 0.20, "duration_seconds": 0.25},
        "multimodal": False,
    },

    # ── User-driven ──────────────────────────────────────────────────────
    "shower_tub": {
        # Merged from old shower + bath profiles. Duration and volume are
        # behavioural choices (whether the user showers or fills a tub);
        # anchor on flow rate and pressure delta which are hydraulically
        # stable per installation.
        "anchor_weights": {
            "avg_flow_lpm":           3.0,
            "pressure_delta_psi":     2.0,
            "flow_variability":       1.5,
            "hydraulic_resistance":   2.0,   # forward-looking
            "recovery_overshoot_psi": 1.0,
            "pressure_onset_ms":      1.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.45, "volume_litres": 0.45},
        "multimodal": False,
    },
    "tap": {
        # Merged from old bathroom_tap + kitchen_tap + utility_tap profiles.
        # Short, low-volume events; duration and volume vary with use.
        "anchor_weights": {
            "avg_flow_lpm":           2.5,
            "pressure_delta_psi":     1.5,
            "recovery_overshoot_psi": 1.0,
            "pressure_onset_ms":      1.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.60, "volume_litres": 0.60},
        "multimodal": False,
    },

    # ── Programme-driven (multimodal) ────────────────────────────────────
    "washing_machine": {
        "anchor_weights": {
            "avg_flow_lpm":           2.0,
            "pressure_delta_psi":     1.5,
            "flow_variability":       1.5,
            "resistance_curve_shape": 2.0,   # forward-looking
            "pressure_onset_ms":      1.5,   # solenoid valve = fast onset
            "recovery_overshoot_psi": 1.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.50, "volume_litres": 0.50},
        "multimodal": True,
    },
    "dishwasher": {
        "anchor_weights": {
            "avg_flow_lpm":           2.0,
            "flow_variability":       1.5,
            "resistance_curve_shape": 2.0,   # forward-looking
            "pressure_onset_ms":      1.5,   # solenoid valve = fast onset
            "recovery_overshoot_psi": 1.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.40, "volume_litres": 0.40},
        "multimodal": True,
    },

    # ── Zone / outdoor ───────────────────────────────────────────────────
    "irrigation_zone": {
        "anchor_weights": {
            "avg_flow_lpm":           2.5,
            "pressure_delta_psi":     1.5,
            "pressure_onset_ms":      1.5,   # solenoid valve = fast onset
            "recovery_overshoot_psi": 1.0,
        },
        "float_features": {"duration_seconds", "volume_litres"},
        "expected_cv": {"duration_seconds": 0.25, "volume_litres": 0.25},
        "multimodal": False,
    },

    "water_softener": {
        # dev.24: detected by the SESSION detector (event_rules.detect_softener_
        # sessions), never the k-NN — this profile only keeps the dict in lockstep
        # with FIXTURE_TYPES. Uniform (like 'other'): the k-NN has no
        # water_softener centroid to match against, so the values are inert.
        "anchor_weights": {},
        "float_features": set(),
        "expected_cv": {},
        "multimodal": False,
    },

    # ── Special ──────────────────────────────────────────────────────────
    "leak_test": {
        # Internal type used by the leak-test scheduler. Events tagged with
        # this type never reach the cluster engine (filtered by
        # INTERNAL_FIXTURE_TYPES upstream), but a profile is included so
        # FIXTURE_VARIANCE_PROFILES stays in lockstep with FIXTURE_TYPES.
        "anchor_weights": {
            "volume_litres":    3.0,
            "duration_seconds": 3.0,
            "avg_flow_lpm":     2.0,
        },
        "float_features": set(),
        "expected_cv": {"volume_litres": 0.10, "duration_seconds": 0.10},
        "multimodal": False,
    },
    "other": {
        # Uniform fallback — empty anchor_weights and float_features means
        # _build_match_weights returns the default 1.0 for every feature,
        # so the gate behaves identically to the old global-threshold path.
        "anchor_weights": {},
        "float_features": set(),
        "expected_cv": {},
        "multimodal": False,
    },
}


# Per-fixture-type match thresholds (scaled-feature space).
# Raised from individual-fixture values to type-level values so that two
# fixtures of the same type (e.g., two different cisterns) both match a
# single type centroid rather than splitting into separate clusters.
# "other" is unchanged — it is a catch-all, not a named type.
FIXTURE_MATCH_THRESHOLDS: Dict[str, float] = {
    "toilet":           3.0,
    "shower_tub":       4.5,   # looser of old shower(4.0) + bath(4.5)
    "tap":              3.5,   # same as old bathroom_tap / kitchen_tap
    "washing_machine":  4.5,
    "dishwasher":       4.5,
    "water_softener":   1.5,   # session-detected; neutral (inert for k-NN)
    "irrigation_zone":  3.5,
    "leak_test":        0.8,
    "other":            1.5,
}


def get_variance_profile(fixture_type: Optional[str]) -> Dict:
    """Return the variance profile for a fixture type, or `other` as fallback.

    Accepts None (returned for unconfirmed clusters) and unknown types
    (treats them as `other` — uniform behaviour matching the pre-2.1 path).
    """
    if fixture_type is None:
        return FIXTURE_VARIANCE_PROFILES["other"]
    return FIXTURE_VARIANCE_PROFILES.get(fixture_type,
                                         FIXTURE_VARIANCE_PROFILES["other"])


def get_match_threshold(fixture_type: Optional[str]) -> float:
    """Return the per-type match threshold, or the `other` default as fallback.

    Matches the same fallback semantics as get_variance_profile.
    """
    if fixture_type is None:
        return FIXTURE_MATCH_THRESHOLDS["other"]
    return FIXTURE_MATCH_THRESHOLDS.get(fixture_type,
                                       FIXTURE_MATCH_THRESHOLDS["other"])


# ── Heuristic suggestion rules ────────────────────────────────────────────
# Each rule is a callable: rule(centroid: dict, circuit_type: str) -> Optional[Tuple[str, float]]
# Centroid keys are the feature names used by feature_extractor:
#   avg_flow_lpm, peak_flow_lpm, duration_seconds, volume_litres,
#   pressure_delta_psi, has_pressure_transient (0 or 1 average),
#   flow_variability, hour_sin, hour_cos,
#   cycle_pulse_count (mean # of similar-volume neighbours within ±45 min — the
#     temporal "appliance cycle" signal; see database.recompute_cycle_pulse_counts), ...
#
# Confidence scoring guideline:
#   0.90+ — extremely characteristic, multiple distinguishing features match
#   0.75  — strong match, one or two features could overlap with another type
#   0.60  — plausible match, weak signal
#  <0.50  — don't return; let downstream code label as 'other' or leave NULL

# Helpers ----------------------------------------------------------------

def _between(value: Optional[float], lo: float, hi: float) -> bool:
    """True if value is in [lo, hi]. Returns False for None."""
    return value is not None and lo <= value <= hi


def _safe(centroid: Dict, key: str, default: float = 0.0) -> float:
    """Read a centroid key, returning default for missing or None values."""
    v = centroid.get(key)
    return default if v is None else float(v)


# Individual rules -------------------------------------------------------

def _rule_toilet(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Toilet flushes: 3-10 L volume, 20-120 s fill, sharp pressure transient.

    Bands widened from the originals (4-9 L / 20-60 s / 4-25 L/min) to span
    dual-flush and slow-fill cisterns across houses (observed real fills run to
    ~98 s and down to ~2.8 L) while keeping the has_xt transient signature that
    separates a toilet from a tap.
    """
    if circuit_type == "zone":
        return None
    vol      = _safe(centroid, "volume_litres")
    dur      = _safe(centroid, "duration_seconds")
    has_xt   = _safe(centroid, "has_pressure_transient")
    flow     = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 3, 10) and _between(dur, 20, 120) and has_xt > 0.5:
        if _between(flow, 3, 15):
            return ("toilet", 0.90)
    return None


def _rule_shower_tub(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Shower or bath fill: 15-300 L, 3-60 min, moderate flow.

    Merges shower + bath. Duration ceiling raised from 20 to 60 min (real
    showers run past 50 min) and the flow floor lowered from 5 to 4 L/min for
    low-flow heads.
    """
    if circuit_type == "zone":
        return None
    vol  = _safe(centroid, "volume_litres")
    dur  = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 15, 300) and _between(dur, 180, 3600) and _between(flow, 4, 22):
        return ("shower_tub", 0.80)
    return None


def _rule_tap(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Tap use: 0.3-6 L, 4-75 s — bathroom/kitchen/utility taps.

    Deliberately kept NARROW (balanced posture): tap is the broadest, lowest-
    confidence rule and the last in the chain, so it must not become a catch-all
    that pulls miscellaneous small events out of 'other'.
    """
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    if _between(vol, 0.3, 6) and _between(dur, 4, 75):
        return ("tap", 0.65)
    return None


def _rule_dishwasher(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Dishwasher fill: 8-15 L per fill, 60-180 s, repeats every ~20 min during cycle."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 8, 15) and _between(dur, 60, 180) and _between(flow, 4, 8):
        return ("dishwasher", 0.75)
    return None


def _rule_washing_machine(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Washing machine fill: 30-80 L total per cycle, 60-300 s per fill phase."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 15, 80) and _between(dur, 60, 300) and _between(flow, 8, 18):
        return ("washing_machine", 0.75)
    return None


def _rule_irrigation_zone(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Irrigation: only on zone circuit. Long duration, sustained high flow."""
    if circuit_type != "zone":
        return None
    dur  = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    # Irrigation zones run ~4-60 minutes at sustained flow
    if dur >= 240 and flow >= 5:
        return ("irrigation_zone", 0.85)
    # Short zone events fall through to 'other' (hose_bib removed from taxonomy)
    return None


# Temporal appliance rules -----------------------------------------------
# Dishwasher / washing-machine FILL PULSES look like small taps in a single
# event; the discriminator is that they REPEAT (a cycle of similar-volume
# pulses). ``cycle_pulse_count`` is the centroid-mean count of similar-volume
# neighbours within ±45 min (database.recompute_cycle_pulse_counts). Both rules
# gate on has_pressure_transient < 0.5, so a toilet's sharp transient can never
# satisfy them, and they run BEFORE the broadened toilet/tap bands in the chain.

def _rule_dishwasher_temporal(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Dishwasher fill pulse: repeated low-flow small pulses (a cycle)."""
    if circuit_type == "zone":
        return None
    if (_safe(centroid, "cycle_pulse_count") >= 3
            and _safe(centroid, "has_pressure_transient") < 0.5
            and _between(_safe(centroid, "avg_flow_lpm"), 1, 5)
            and _between(_safe(centroid, "volume_litres"), 0.5, 6)
            and _between(_safe(centroid, "duration_seconds"), 20, 220)):
        return ("dishwasher", 0.80)
    return None


def _rule_washing_machine_temporal(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Washing-machine fill pulse: repeated higher-flow pulses (a cycle)."""
    if circuit_type == "zone":
        return None
    if (_safe(centroid, "cycle_pulse_count") >= 3
            and _safe(centroid, "has_pressure_transient") < 0.5
            and _between(_safe(centroid, "avg_flow_lpm"), 5, 16)
            and _between(_safe(centroid, "volume_litres"), 3, 40)
            and _between(_safe(centroid, "duration_seconds"), 40, 400)):
        return ("washing_machine", 0.80)
    return None


# Rule chain -------------------------------------------------------------
# Order matters: more-specific rules first.
# shower_tub must come before tap: a bath-fill volume would also satisfy
# the broader tap volume range if tap were checked first.

_RULES = [
    _rule_irrigation_zone,
    _rule_dishwasher_temporal,       # temporal appliance cycles first — they
    _rule_washing_machine_temporal,  # gate on has_xt<0.5 so toilets are safe
    _rule_shower_tub,                # high volume, before tap
    _rule_washing_machine,           # centroid-only fallback (legacy clusters)
    _rule_dishwasher,                # centroid-only fallback (legacy clusters)
    _rule_toilet,                    # characteristic; after high-vol/appliance
    _rule_tap,                       # broadest, narrow band, last
]


def suggest_fixture_type(
    centroid: Dict,
    circuit_type: str = "fixture",
) -> Tuple[Optional[str], float]:
    """
    Apply heuristic rules to a cluster centroid.

    Returns (type_string, confidence) or (None, 0.0) if no rule matches.
    The first matching rule wins — order in _RULES matters.
    """
    circuit_type = normalize_circuit_type(circuit_type)
    for rule in _RULES:
        result = rule(centroid, circuit_type)
        if result is not None:
            fixture_type, confidence = result
            return fixture_type, confidence
    return None, 0.0
