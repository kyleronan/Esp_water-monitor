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
    if circuit_type == "irrigation":
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
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 20, 100) and _between(dur, 240, 1200) and _between(flow, 5, 15):
        return ("shower", 0.85)
    return None


def _rule_bath(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Bath fill: 80-200 L, 3-10 min, high flow rate."""
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 80, 250) and _between(dur, 180, 600) and _between(flow, 12, 25):
        return ("bath", 0.80)
    return None


def _rule_bathroom_tap(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Bathroom tap: 0.3-3 L, 5-30 s, low flow."""
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    if _between(vol, 0.3, 3) and _between(dur, 5, 30):
        return ("bathroom_tap", 0.65)
    return None


def _rule_kitchen_tap(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Kitchen tap: 0.5-8 L, 5-60 s, variable flow."""
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    if _between(vol, 0.5, 8) and _between(dur, 5, 60):
        return ("kitchen_tap", 0.60)
    return None


def _rule_dishwasher(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Dishwasher fill: 8-15 L per fill, 60-180 s, repeats every ~20 min during cycle."""
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 8, 15) and _between(dur, 60, 180) and _between(flow, 4, 8):
        return ("dishwasher", 0.75)
    return None


def _rule_washing_machine(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Washing machine fill: 30-80 L total per cycle, 60-300 s per fill phase."""
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 15, 80) and _between(dur, 60, 300) and _between(flow, 8, 18):
        return ("washing_machine", 0.75)
    return None


def _rule_irrigation_zone(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Irrigation: only on irrigation circuit. Long duration, sustained high flow."""
    if circuit_type != "irrigation":
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
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    if _between(vol, 0.05, 0.4) and _between(dur, 3, 15):
        return ("ice_maker", 0.65)
    return None


def _rule_humidifier(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Whole-house humidifier: small slow fills, 0.5-3 L, 30-180 s."""
    if circuit_type == "irrigation":
        return None
    vol = _safe(centroid, "volume_litres")
    dur = _safe(centroid, "duration_seconds")
    flow = _safe(centroid, "avg_flow_lpm")
    if _between(vol, 0.5, 3) and _between(dur, 30, 180) and _between(flow, 0.3, 1.5):
        return ("humidifier", 0.55)
    return None


def _rule_water_softener(centroid: Dict, circuit_type: str) -> Optional[Tuple[str, float]]:
    """Water softener regen: long sustained flow, 150-300 L, 30-90 minutes, scheduled."""
    if circuit_type == "irrigation":
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
    circuit_type: str = "main",
) -> Tuple[Optional[str], float]:
    """
    Apply heuristic rules to a cluster centroid.

    Returns (type_string, confidence) or (None, 0.0) if no rule matches.
    The first matching rule wins — order in _RULES matters.
    """
    for rule in _RULES:
        result = rule(centroid, circuit_type)
        if result is not None:
            fixture_type, confidence = result
            return fixture_type, confidence
    return None, 0.0
