"""
Unit conversion for display.

All data is stored internally in L/min (flow) and PSI (pressure).
These constants convert to the user's preferred display units.

Flow options  : L/min  | gal/min | ft³/min | m³/min
Pressure opts : psi    | bar     | kPa
Volume is derived from the flow unit (L → gal / ft³ / m³).

HA unit system auto-detection maps:
  volume="gal" → flow=gal/min, pressure=psi   (US / imperial)
  volume="L"   → flow=L/min,   pressure=bar   (SI metric)
  volume="m³"  → flow=m³/min,  pressure=kPa   (some EU meters)
"""
from __future__ import annotations
import logging
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

# ── HA volume-unit strings → the canonical vol_label below ──────────────
#
# Home Assistant entities are free to label a volume however their
# integration likes ("gallons", "gal", "ft3"...). Two places used to key on
# EXACT strings and silently treat anything unrecognised as litres, which on
# a gallons meter is a 3.785x error — and on the calibration path that error
# was written back into the firmware as pulses-per-litre.
#
# Keep every spelling here so the parsers cannot drift apart again.
VOL_UNIT_ALIASES: Dict[str, str] = {
    # litres
    "l": "L", "liter": "L", "liters": "L", "litre": "L", "litres": "L",
    # US gallons
    "gal": "gal", "gals": "gal", "gallon": "gal", "gallons": "gal",
    "us gal": "gal", "us gallon": "gal", "us gallons": "gal",
    "us liquid gallon": "gal",
    # cubic feet
    "ft\u00b3": "ft\u00b3", "ft3": "ft\u00b3", "cu ft": "ft\u00b3",
    "cubic foot": "ft\u00b3", "cubic feet": "ft\u00b3",
    # cubic metres
    "m\u00b3": "m\u00b3", "m3": "m\u00b3", "cu m": "m\u00b3",
    "cubic meter": "m\u00b3", "cubic meters": "m\u00b3",
    "cubic metre": "m\u00b3", "cubic metres": "m\u00b3",
}

#: Every spelling that means US gallons (kept as a set for ha_client).
GAL_UNITS = frozenset(k for k, v in VOL_UNIT_ALIASES.items() if v == "gal")

# ── Flow rate options ──────────────────────────────────────────────────────
# factor      : multiply stored L/min value by this to get display value
# vol_label   : unit label for volumes  (L, gal, ft³, m³)
# vol_factor  : multiply stored L value by this to get display volume
# decimals    : display decimal places for flow rate
# vol_decimals: display decimal places for volume

FLOW_OPTIONS: Dict[str, Dict[str, Any]] = {
    "L/min": {
        "label":       "L/min",
        "factor":      1.0,
        "vol_label":   "L",
        "vol_factor":  1.0,
        "decimals":    2,
        "vol_decimals": 1,
    },
    "gal/min": {
        "label":       "gal/min",
        "factor":      0.264172,
        "vol_label":   "gal",
        "vol_factor":  0.264172,
        "decimals":    2,
        "vol_decimals": 1,
    },
    "ft³/min": {
        "label":       "ft³/min",
        "factor":      0.0353147,
        "vol_label":   "ft³",
        "vol_factor":  0.0353147,
        "decimals":    3,
        "vol_decimals": 2,
    },
    "m³/min": {
        "label":       "m³/min",
        "factor":      0.001,
        "vol_label":   "m³",
        "vol_factor":  0.001,
        "decimals":    4,
        "vol_decimals": 3,
    },
}


#: Canonical vol_label -> vol_factor (display value = stored litres x factor).
VOL_LABEL_FACTORS: Dict[str, float] = {
    opt["vol_label"]: opt["vol_factor"] for opt in FLOW_OPTIONS.values()
}


def resolve_vol_factor(unit: str) -> Optional[float]:
    """HA unit_of_measurement string -> vol_factor, or None if unrecognised.

    ``None`` means "do not guess". A caller converting a METER reading must
    treat it as a hard failure: assuming litres for an unrecognised unit is how
    a gallons meter produced a 3.785x-wrong pulses-per-litre value that was then
    written into the firmware, where every downstream volume inherits it.

    An empty/absent unit resolves to litres, matching the firmware's own output.
    """
    u = (unit or "").strip()
    if not u:
        return VOL_LABEL_FACTORS["L"]
    label = VOL_UNIT_ALIASES.get(u.lower())
    if label is None:
        return None
    return VOL_LABEL_FACTORS.get(label)

# ── Pressure options ───────────────────────────────────────────────────────
# factor  : multiply stored PSI value by this to get display value
# decimals: display decimal places

PRESSURE_OPTIONS: Dict[str, Dict[str, Any]] = {
    "psi": {
        "label":    "PSI",
        "factor":   1.0,
        "decimals": 1,
    },
    "bar": {
        "label":    "bar",
        "factor":   0.0689476,
        "decimals": 3,
    },
    "kPa": {
        "label":    "kPa",
        "factor":   6.89476,
        "decimals": 1,
    },
}

# ── HA unit system → default display units ────────────────────────────────
_HA_VOLUME_TO_FLOW: Dict[str, str] = {
    "gal": "gal/min",
    "ft³": "ft³/min",
    "m³":  "m³/min",
    "L":   "L/min",
}
_HA_VOLUME_TO_PRESSURE: Dict[str, str] = {
    "gal": "psi",
    "ft³": "psi",
    "m³":  "kPa",
    "L":   "bar",
}


def defaults_from_ha(ha_volume_unit: str) -> tuple[str, str]:
    """
    Given the HA volume unit string (e.g. 'L', 'gal'), return the
    suggested (flow_unit_key, pressure_unit_key) for the display.
    Falls back to L/min + PSI for unrecognised values.
    """
    flow = _HA_VOLUME_TO_FLOW.get(ha_volume_unit, "L/min")
    pres = _HA_VOLUME_TO_PRESSURE.get(ha_volume_unit, "bar")  # metric default for unrecognised units
    return flow, pres


def build_unit_context(flow_key: str, pressure_key: str) -> Dict[str, Any]:
    """
    Build the template/JS unit context dict from storage keys.
    Returned dict contains all values needed by templates and window.UNITS.
    """
    f = FLOW_OPTIONS.get(flow_key, FLOW_OPTIONS["L/min"])
    p = PRESSURE_OPTIONS.get(pressure_key, PRESSURE_OPTIONS["psi"])
    return {
        # Flow
        "flow_unit":         f["label"],
        "flow_factor":       f["factor"],
        "flow_decimals":     f["decimals"],
        # Volume (derived from flow unit selection)
        "vol_unit":          f["vol_label"],
        "vol_factor":        f["vol_factor"],
        "vol_decimals":      f["vol_decimals"],
        # Pressure
        "pressure_unit":     p["label"],
        "pressure_factor":   p["factor"],
        "pressure_decimals": p["decimals"],
        # Dropdown choices (for settings page)
        "flow_options":          list(FLOW_OPTIONS.keys()),
        "pressure_options":      list(PRESSURE_OPTIONS.keys()),
        "flow_option_labels":    {k: v["label"] for k, v in FLOW_OPTIONS.items()},
        "pressure_option_labels":{k: v["label"] for k, v in PRESSURE_OPTIONS.items()},
        "flow_key":              flow_key,
        "pressure_key":          pressure_key,
    }


# ── Unit context cache ────────────────────────────────────────────────────
# load_unit_context is called on every dashboard poll (once per circuit).
# Cache for TTL seconds to avoid hitting the DB on every 2-second refresh.
_UNIT_CACHE: Optional[Dict[str, Any]] = None
_UNIT_CACHE_AT: float = 0.0
_UNIT_CACHE_TTL: float = 30.0   # seconds
# Consecutive failed reads, so the log reports the condition once rather
# than once per circuit per poll. Reset on the first success.
_UNIT_LOAD_FAILURES: int = 0


def invalidate_unit_cache() -> None:
    """Call after saving unit preferences so the next request re-reads the DB."""
    global _UNIT_CACHE
    _UNIT_CACHE = None


def load_unit_context(db) -> Dict[str, Any]:
    """Load unit preferences from home_profile and return a context dict.
    Result is cached for _UNIT_CACHE_TTL seconds; call invalidate_unit_cache()
    after saving new unit preferences.
    """
    import time
    global _UNIT_CACHE, _UNIT_CACHE_AT
    now = time.monotonic()
    if _UNIT_CACHE is not None and (now - _UNIT_CACHE_AT) < _UNIT_CACHE_TTL:
        return _UNIT_CACHE
    try:
        row = db.execute(
            "SELECT flow_unit, pressure_unit FROM home_profile WHERE id = 1"
        ).fetchone()
        flow_key     = (row["flow_unit"]     if row else None) or "L/min"
        pressure_key = (row["pressure_unit"] if row else None) or "psi"
    except Exception as e:
        # unit 2.33 — this used to swallow silently AND cache the fallback for
        # the full TTL, so one transient DB error rendered EVERY number on the
        # page in the wrong unit for 30 s: a US household reading litres as
        # gallons with nothing on screen or in the log to say so. Wrong units
        # are indistinguishable from wrong data to the person reading them.
        #
        # Return the fallback WITHOUT caching it, so the next request retries
        # and correct units come back the moment the DB does. Log the first
        # occurrence at WARNING and the rest at DEBUG: load_unit_context runs
        # once per circuit on every dashboard poll, so an unconditional
        # warning would flood the log during exactly the outage it reports.
        global _UNIT_LOAD_FAILURES
        _UNIT_LOAD_FAILURES += 1
        if _UNIT_LOAD_FAILURES == 1:
            log.warning(
                "unit preferences could not be read (%s); falling back to "
                "L/min + psi for this request. Displayed numbers may be in "
                "the wrong unit until this clears. Further occurrences at "
                "DEBUG.", e)
        else:
            log.debug("unit preferences unreadable (%d consecutive): %s",
                      _UNIT_LOAD_FAILURES, e)
        return build_unit_context("L/min", "psi")
    _UNIT_LOAD_FAILURES = 0
    _UNIT_CACHE    = build_unit_context(flow_key, pressure_key)
    _UNIT_CACHE_AT = now
    return _UNIT_CACHE


def convert_flow(value: float, uc: Dict[str, Any]) -> str:
    """Format a L/min value using the given unit context."""
    try:
        return f"{float(value) * uc['flow_factor']:.{uc['flow_decimals']}f}"
    except (ValueError, TypeError):
        return "—"


def convert_pressure(value: float, uc: Dict[str, Any]) -> str:
    """Format a PSI value using the given unit context."""
    try:
        return f"{float(value) * uc['pressure_factor']:.{uc['pressure_decimals']}f}"
    except (ValueError, TypeError):
        return "—"


def convert_volume(value: float, uc: Dict[str, Any]) -> str:
    """Format a litre value using the given unit context."""
    try:
        return f"{float(value) * uc['vol_factor']:.{uc['vol_decimals']}f}"
    except (ValueError, TypeError):
        return "—"
