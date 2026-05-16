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

from typing import Dict, List, Optional, Tuple

# ── Taxonomy ──────────────────────────────────────────────────────────────
# Order matters for UI dropdowns (grouped sensibly), but the algorithm
# treats them as a flat set.

FIXTURE_TYPES: List[str] = [
    # Bathroom
    "toilet",
    "shower",
    "bath",
    "bathroom_tap",
    "bidet",
    # Kitchen
    "kitchen_tap",
    "dishwasher",
    "ice_maker",
    "refrigerator_water",
    "ro_drinking_faucet",
    # Laundry / utility
    "washing_machine",
    "utility_tap",
    # Outdoor
    "hose_bib",
    "outdoor_tap",
    "irrigation_zone",
    "pool_fill",
    # Whole-house systems
    "humidifier",
    "water_softener",
    "ro_system_whole_house",
    "evaporative_cooler",
    "boiler_makeup",
    # Special / catch-all
    "leak_test",
    "other",
]

# Friendly display labels (UI shows these instead of the raw key)
FIXTURE_TYPE_LABELS: Dict[str, str] = {
    "toilet":                "Toilet",
    "shower":                "Shower",
    "bath":                  "Bath",
    "bathroom_tap":          "Bathroom tap",
    "bidet":                 "Bidet",
    "kitchen_tap":           "Kitchen tap",
    "dishwasher":            "Dishwasher",
    "ice_maker":             "Ice maker",
    "refrigerator_water":    "Refrigerator water",
    "ro_drinking_faucet":    "RO drinking faucet",
    "washing_machine":       "Washing machine",
    "utility_tap":           "Utility tap",
    "hose_bib":              "Hose bib",
    "outdoor_tap":           "Outdoor tap",
    "irrigation_zone":       "Irrigation zone",
    "pool_fill":             "Pool fill",
    "humidifier":            "Humidifier",
    "water_softener":        "Water softener",
    "ro_system_whole_house": "RO system (whole-house)",
    "evaporative_cooler":    "Evaporative cooler",
    "boiler_makeup":         "Boiler make-up",
    "leak_test":             "Leak test (auto)",
    "other":                 "Other",
}

# Types that are NOT user-selectable in the UI (auto-managed only).
# The leak test scheduler tags its synthetic events with this type so
# they never show up as a real fixture for clustering or HA publishing.
INTERNAL_FIXTURE_TYPES: List[str] = ["leak_test"]


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
_ZONE_FIXTURE_TYPES: List[str] = ["irrigation_zone", "hose_bib", "pool_fill", "other"]


def normalize_circuit_type(value: Optional[str]) -> str:
    """Map legacy "irrigation" → "zone"; unknown/None → "fixture"."""
    if value == "irrigation":
        return "zone"
    if value in CIRCUIT_TYPES:
        return value
    return "fixture"


def zone_user_selectable_types() -> List[str]:
    """Fixture types appropriate for a zone (irrigation) circuit."""
    return [t for t in _ZONE_FIXTURE_TYPES if t not in INTERNAL_FIXTURE_TYPES]


def fixture_user_selectable_types() -> List[str]:
    """Fixture types appropriate for a household fixture circuit."""
    _zone_only = {"irrigation_zone"}
    return [t for t in FIXTURE_TYPES if t not in INTERNAL_FIXTURE_TYPES and t not in _zone_only]


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
        "anchor_weights": {
            "volume_litres":          3.0,
            "duration_seconds":       3.0,
            "avg_flow_lpm":           2.0,
            "has_pressure_transient": 2.0,
            "resistance_curve_shape": 2.0,   # forward-looking
        },
        "float_features": {"hour_sin", "hour_cos", "day_of_week", "is_weekend"},
        "expected_cv": {"volume_litres": 0.08, "duration_seconds": 0.10},
        "multimodal": False,
    },
    "ice_maker": {
        "anchor_weights": {
            "volume_litres":    4.0,
            "duration_seconds": 4.0,
            "avg_flow_lpm":     3.0,
        },
        "float_features": {"hour_sin", "hour_cos", "day_of_week"},
        "expected_cv": {"volume_litres": 0.03, "duration_seconds": 0.05},
        "multimodal": False,
    },
    "refrigerator_water": {
        "anchor_weights": {
            "volume_litres":    2.5,
            "duration_seconds": 2.0,
            "avg_flow_lpm":     2.0,
        },
        "float_features": {"hour_sin", "hour_cos"},
        "expected_cv": {"volume_litres": 0.10, "duration_seconds": 0.15},
        "multimodal": False,
    },
    "humidifier": {
        "anchor_weights": {
            "volume_litres": 2.0,
            "avg_flow_lpm":  2.5,
        },
        "float_features": {"duration_seconds", "hour_sin", "hour_cos"},
        "expected_cv": {"volume_litres": 0.20, "duration_seconds": 0.20},
        "multimodal": False,
    },
    "boiler_makeup": {
        "anchor_weights": {
            "volume_litres": 2.5,
            "avg_flow_lpm":  2.0,
        },
        "float_features": {"hour_sin", "hour_cos"},
        "expected_cv": {"volume_litres": 0.15, "duration_seconds": 0.15},
        "multimodal": False,
    },

    # ── User-driven ──────────────────────────────────────────────────────
    "shower": {
        "anchor_weights": {
            "avg_flow_lpm":           3.0,
            "pressure_delta_psi":     2.0,
            "flow_variability":       1.5,
            "hydraulic_resistance":   2.0,   # forward-looking
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.45, "volume_litres": 0.45},
        "multimodal": False,
    },
    "bath": {
        "anchor_weights": {
            "avg_flow_lpm":       3.0,
            "pressure_delta_psi": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.30, "volume_litres": 0.25},
        "multimodal": False,
    },
    "bathroom_tap": {
        "anchor_weights": {
            "avg_flow_lpm":       2.5,
            "pressure_delta_psi": 1.5,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.60, "volume_litres": 0.60},
        "multimodal": False,
    },
    "kitchen_tap": {
        "anchor_weights": {
            "avg_flow_lpm":       2.5,
            "pressure_delta_psi": 1.5,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.55, "volume_litres": 0.55},
        "multimodal": False,
    },
    "utility_tap": {
        "anchor_weights": {
            "avg_flow_lpm": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.65, "volume_litres": 0.65},
        "multimodal": False,
    },
    "bidet": {
        "anchor_weights": {
            "avg_flow_lpm": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.50, "volume_litres": 0.50},
        "multimodal": False,
    },
    "ro_drinking_faucet": {
        "anchor_weights": {
            "avg_flow_lpm": 2.5,
        },
        "float_features": {"duration_seconds", "volume_litres"},
        "expected_cv": {"duration_seconds": 0.50, "volume_litres": 0.50},
        "multimodal": False,
    },

    # ── Programme-driven (multimodal) ────────────────────────────────────
    "washing_machine": {
        "anchor_weights": {
            "avg_flow_lpm":           2.0,
            "pressure_delta_psi":     1.5,
            "flow_variability":       1.5,
            "resistance_curve_shape": 2.0,   # forward-looking
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
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.40, "volume_litres": 0.40},
        "multimodal": True,
    },
    "water_softener": {
        "anchor_weights": {
            "avg_flow_lpm":           2.0,
            "flow_variability":       1.5,
            "resistance_curve_shape": 2.0,   # forward-looking
        },
        "float_features": {"duration_seconds", "volume_litres"},
        "expected_cv": {"duration_seconds": 0.35, "volume_litres": 0.35},
        "multimodal": True,
    },

    # ── Outdoor / programme-ish ──────────────────────────────────────────
    "irrigation_zone": {
        "anchor_weights": {
            "avg_flow_lpm":       2.5,
            "pressure_delta_psi": 1.5,
        },
        "float_features": {"duration_seconds", "volume_litres"},
        "expected_cv": {"duration_seconds": 0.25, "volume_litres": 0.25},
        "multimodal": False,
    },
    "pool_fill": {
        "anchor_weights": {
            "avg_flow_lpm": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres"},
        "expected_cv": {"duration_seconds": 0.20, "volume_litres": 0.20},
        "multimodal": False,
    },
    "hose_bib": {
        "anchor_weights": {
            "avg_flow_lpm": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.55, "volume_litres": 0.55},
        "multimodal": False,
    },
    "outdoor_tap": {
        "anchor_weights": {
            "avg_flow_lpm": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres", "duration_log"},
        "expected_cv": {"duration_seconds": 0.55, "volume_litres": 0.55},
        "multimodal": False,
    },
    "evaporative_cooler": {
        "anchor_weights": {
            "avg_flow_lpm": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres"},
        "expected_cv": {"duration_seconds": 0.30, "volume_litres": 0.30},
        "multimodal": False,
    },
    "ro_system_whole_house": {
        "anchor_weights": {
            "avg_flow_lpm": 2.0,
        },
        "float_features": {"duration_seconds", "volume_litres"},
        "expected_cv": {"duration_seconds": 0.40, "volume_litres": 0.40},
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


# Per-fixture-type match thresholds (scaled-feature space, comparable to
# DBSTREAM_CLUSTERING_THRESHOLD = 1.5). Lower = tighter gate.
#
# Tuned categorically:
#   < 1.0  deterministic — toilets, ice makers, refrigerators
#   1.0–2.0 mostly-deterministic — humidifiers, taps, RO
#   2.0–3.0 user-driven — showers, baths, hose bibs, irrigation
#   3.0+   multimodal — washing machines / dishwashers (loosest)
FIXTURE_MATCH_THRESHOLDS: Dict[str, float] = {
    # Deterministic
    "toilet":              0.6,
    "ice_maker":           0.5,
    "refrigerator_water":  0.7,
    "humidifier":          1.0,
    "boiler_makeup":       1.0,

    # User-driven
    "shower":              2.5,
    "bath":                2.8,
    "bathroom_tap":        1.8,
    "kitchen_tap":         1.8,
    "utility_tap":         2.0,
    "bidet":               1.6,
    "ro_drinking_faucet":  1.5,

    # Programme-driven (multimodal — loosest)
    "washing_machine":     3.0,
    "dishwasher":          2.8,
    "water_softener":      2.5,

    # Outdoor / programme-ish
    "irrigation_zone":     2.2,
    "pool_fill":           1.8,
    "hose_bib":            2.5,
    "outdoor_tap":         2.5,
    "evaporative_cooler":  1.8,
    "ro_system_whole_house": 2.0,

    # Special
    "leak_test":           0.8,
    "other":               1.5,   # = current DBSTREAM_CLUSTERING_THRESHOLD
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
#   flow_variability, hour_sin, hour_cos, ...
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
    """Toilet flushes: 4-9 L volume, 20-60 s duration, sharp pressure transient."""
    if circuit_type == "zone":
        return None
    vol      = _safe(centroid, "volume_litres")
    dur      = _safe(centroid, "duration_seconds")
    has_xt   = _safe(centroid, "has_pressure_transient")
    flow     = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 4, 9) and _between(dur, 20, 60) and has_xt > 0.5:
        # Flow rate sanity check: ~6 L over ~30 s = ~12 L/min average
        if _between(flow, 4, 25):
            return ("toilet", 0.90)
    return None


def _rule_shower(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Shower: 20-80 L, 5-15 min, sustained moderate flow."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 20, 100) and _between(dur, 240, 1200) and _between(flow, 5, 15):
        return ("shower", 0.85)
    return None


def _rule_bath(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Bath fill: 80-200 L, 3-10 min, high flow rate."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 80, 250) and _between(dur, 180, 600) and _between(flow, 12, 25):
        return ("bath", 0.80)
    return None


def _rule_bathroom_tap(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Bathroom tap: 0.3-3 L, 5-30 s, low flow."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    if _between(vol, 0.3, 3) and _between(dur, 5, 30):
        return ("bathroom_tap", 0.65)
    return None


def _rule_kitchen_tap(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Kitchen tap: 0.5-8 L, 5-60 s, variable flow."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    if _between(vol, 0.5, 8) and _between(dur, 5, 60):
        return ("kitchen_tap", 0.60)
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
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    # Irrigation zones run 5-60 minutes at sustained flow
    if dur >= 300 and flow >= 5:
        return ("irrigation_zone", 0.85)
    # Short irrigation events might be a hose bib used outside
    if _between(dur, 30, 300) and flow >= 3:
        return ("hose_bib", 0.55)
    return None


def _rule_ice_maker(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Ice maker: tiny intermittent fills, 50-300 mL, 3-15 s."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    if _between(vol, 0.05, 0.4) and _between(dur, 3, 15):
        return ("ice_maker", 0.65)
    return None


def _rule_humidifier(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Whole-house humidifier: small slow fills, 0.5-3 L, 30-180 s."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 0.5, 3) and _between(dur, 30, 180) and _between(flow, 0.3, 1.5):
        return ("humidifier", 0.55)
    return None


def _rule_water_softener(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Water softener regen: long sustained flow, 150-300 L, 30-90 minutes, scheduled."""
    if circuit_type == "zone":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 100, 400) and _between(dur, 1800, 5400) and _between(flow, 3, 8):
        return ("water_softener", 0.70)
    return None


# Rule chain -------------------------------------------------------------
# Order matters: more-specific rules first.

_RULES = [
    _rule_irrigation_zone,
    _rule_water_softener,    # high volume, must check before shower/bath
    _rule_bath,              # high volume, must check before shower
    _rule_shower,
    _rule_washing_machine,
    _rule_dishwasher,
    _rule_humidifier,
    _rule_toilet,            # very characteristic, but check after high-vol rules
    _rule_bathroom_tap,      # before kitchen_tap because bathroom_tap has lower vol bound
    _rule_kitchen_tap,
    _rule_ice_maker,
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
