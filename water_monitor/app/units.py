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
from typing import Any, Dict, Optional

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
    except Exception:
        flow_key, pressure_key = "L/min", "psi"
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
