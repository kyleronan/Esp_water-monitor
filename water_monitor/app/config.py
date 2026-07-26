"""
Load and validate add-on options from /data/options.json.
Provides typed configuration objects for circuits and global settings.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

OPTIONS_PATH = Path(os.environ.get("OPTIONS_PATH", "/data/options.json"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "water_monitor.db"

# Phase 1 dev/testing tools (e.g. instant rule-calibration re-fit, bypassing the
# weeks-long recalibration). Gated OFF by default; the whole Settings → Dev tab and
# its routes are hidden unless enabled. Controlled by the `dev_tools` add-on option
# (settable from the HA add-on Configuration UI) with a WM_DEV_TOOLS env override
# for local/dev runs. Read once at import — flip the option then restart the add-on.
def _read_dev_tools() -> bool:
    env = os.environ.get("WM_DEV_TOOLS", "").strip().lower()
    if env in ("1", "true", "yes", "on"):
        return True
    if env in ("0", "false", "no", "off"):
        return False
    try:
        if OPTIONS_PATH.exists():
            with OPTIONS_PATH.open() as f:
                return bool(json.load(f).get("dev_tools", False))
    except Exception:
        pass
    return False


DEV_TOOLS = _read_dev_tools()


@dataclass
class CircuitConfig:
    circuit: str
    circuit_type: str   # 'fixture' or 'zone'

    # User-facing display label — loaded at runtime from circuit_labels table.
    # Empty string means not yet loaded; use .label property to read safely.
    display_name: str = ""

    # Entity IDs — populated at runtime from device_discovery / circuit_entity_map.
    # Empty string means not yet discovered.
    flow_sensor: str = ""
    pressure_fast_sensor: str = ""
    pressure_avg_sensor: str = ""
    pressure_history_sensor: str = ""   # pressure_main — 2Hz, 1.375s smoothing, HA-recorded
    flow_onset_sensor: str = ""
    valve_entity: str = ""
    fault_sensor: str = ""
    fault_reason_sensor: str = ""
    leak_test_duration_entity: str = ""
    trickle_sensor: str = ""
    leak_test_sensor: str = ""
    leak_test_switch: str = ""
    leak_test_result_sensor: str = ""
    volume_sensor: str = ""
    # Runtime per-circuit flow-meter pulses-per-litre number entity (firmware 3.12.0+).
    flow_meter_ppl_entity: str = ""
    # Waveform diagnostic counters — the only wf_* entities after the 3.8.0
    # HA-event transport migration removed the 5 chunked text sensors.
    # Chunk drop count was added in 3.9.0 with chunked streaming.
    wf_overflow_count_sensor: str = ""
    wf_chunk_drop_count_sensor: str = ""
    esp_device_prefix: str = ""

    # Cached pulses-per-litre for this circuit's flow meter. Refreshed at runtime
    # from flow_meter_ppl_entity (the firmware number entity is the source of
    # truth); the circuit_profile.pulses_per_litre DB column is the fallback cache.
    # Default 396 = reference turbine (YF-B5).
    pulses_per_litre: float = 396.0

    @property
    def is_zone_circuit(self) -> bool:
        return self.circuit_type == "zone"

    @property
    def min_flow_lpm(self) -> float:
        """Meter-derived low-flow noise floor: 1 pulse/second = 60 ÷ ppl L/min.
        (≈0.15 for a 396-ppl turbine; ≈0.83 for a 72-ppl oval gear.) Guards a
        bad cached ppl by falling back to the reference-turbine floor."""
        ppl = self.pulses_per_litre
        if not (ppl and ppl >= 1.0):
            ppl = 396.0
        return 60.0 / ppl

    @property
    def label(self) -> str:
        """Human-readable name — stored display_name value, or fallback from circuit ID."""
        return self.display_name or self.circuit.replace("_", " ").title()

    @property
    def display_label(self) -> str:
        """Temporary compat alias for label — remove after one release."""
        return self.label

    @property
    def is_fully_configured(self) -> bool:
        """True when all required entity IDs have been discovered."""
        required = [
            self.flow_sensor, self.pressure_fast_sensor,
            self.pressure_avg_sensor, self.flow_onset_sensor,
            self.valve_entity, self.fault_sensor,
        ]
        return all(required)


@dataclass
class AddonConfig:
    log_level: str
    esp_device_name: str
    circuits: List[CircuitConfig]

    # When true, the event detector emits a full JSON capture blob per flow
    # event so the propagation scan can be replayed offline. Off by default.
    debug_capture_propagation: bool = False

    # RBAC escape hatch (optional). A HA user id that is ALWAYS treated as an
    # add-on admin, even if config/auth/list is unreachable / returns no admins.
    # Prevents an admin lockout when the supervisor token can't read the user
    # list. Empty by default — admins normally come from HA admin status.
    bootstrap_admin_user_id: str = ""

    def get_circuit(self, name: str) -> Optional[CircuitConfig]:
        return next((c for c in self.circuits if c.circuit == name), None)


def load_config() -> AddonConfig:
    """Read /data/options.json. Falls back to defaults for dev/testing."""
    raw: dict = {}
    if OPTIONS_PATH.exists():
        with OPTIONS_PATH.open() as f:
            raw = json.load(f)

    from .circuit_compat import resolve_circuit
    circuits = []
    for c in raw.get("circuits", []):
        circuits.append(CircuitConfig(
            circuit=resolve_circuit(c["circuit"]),
            circuit_type=c.get("circuit_type", "fixture"),
        ))

    # Default circuits if none configured
    if not circuits:
        circuits = [
            CircuitConfig(circuit="circuit_1", circuit_type="fixture"),
            CircuitConfig(circuit="circuit_2", circuit_type="zone"),
        ]

    # RBAC bootstrap admin — env override wins (local/dev), else the add-on option.
    bootstrap_admin = (
        os.environ.get("WM_BOOTSTRAP_ADMIN", "").strip()
        or str(raw.get("bootstrap_admin_user_id", "") or "").strip()
    )

    return AddonConfig(
        log_level=raw.get("log_level", "info"),
        esp_device_name=raw.get("esp_device_name", ""),
        circuits=circuits,
        debug_capture_propagation=bool(raw.get("debug_capture_propagation", False)),
        bootstrap_admin_user_id=bootstrap_admin,
    )


def supervisor_token() -> str:
    return os.environ.get("SUPERVISOR_TOKEN", "")


# Preset threshold values for simple sensitivity levels
SENSITIVITY_PRESETS = {
    "low": {
        "pressure_drop_event_psi": 3.0,
        "min_event_duration_seconds": 5.0,
        "score_alert": 0.80,
        "score_shutoff": 0.95,
        "flow_tolerance_pct": 30.0,
        "duration_tolerance_pct": 50.0,
        "schedule_window_minutes": 30.0,
        "sustained_alert_minutes": 20.0,
        "max_shutoffs_per_12h": 1,
    },
    "medium": {
        "pressure_drop_event_psi": 1.2,
        "min_event_duration_seconds": 3.0,
        "score_alert": 0.60,
        "score_shutoff": 0.80,
        "flow_tolerance_pct": 20.0,
        "duration_tolerance_pct": 30.0,
        "schedule_window_minutes": 15.0,
        "sustained_alert_minutes": 10.0,
        "max_shutoffs_per_12h": 2,
    },
    "high": {
        "pressure_drop_event_psi": 1.0,
        "min_event_duration_seconds": 2.0,
        "score_alert": 0.40,
        "score_shutoff": 0.65,
        "flow_tolerance_pct": 10.0,
        "duration_tolerance_pct": 15.0,
        "schedule_window_minutes": 5.0,
        "sustained_alert_minutes": 5.0,
        "max_shutoffs_per_12h": 3,
    },
}


def compute_suggested_calibration_days(
    bathrooms_full: int,
    bathrooms_half: int,
    floors: int,
    occupants: int,
    supply_type: str,
) -> tuple[int, str]:
    """
    Suggest calibration duration based on home profile.
    Returns (days, reason_string).
    """
    estimated_fixtures = (
        bathrooms_full * 3.2
        + bathrooms_half * 1.2
        + floors * 0.5
        + 2.0
    )

    if estimated_fixtures <= 8:
        base_days, tier = 14, "small"
    elif estimated_fixtures <= 14:
        base_days, tier = 14, "medium"
    elif estimated_fixtures <= 20:
        base_days, tier = 21, "large"
    else:
        base_days, tier = 28, "mansion"

    if occupants >= 6:
        base_days = min(base_days + 7, 28)
    elif occupants >= 4 and tier in ("large", "mansion"):
        base_days = min(base_days + 3, 28)

    if supply_type in ("well", "city_pump"):
        # Pump-pressurized supply (well pump or city booster): oscillating
        # pressure slows the pressure-baseline learning either way.
        base_days = min(base_days + 7, 35)

    return min(base_days, 35), tier


# ── Pump-aware detection (dev21 Phase 1) ───────────────────────────────────────

# Valid home_profile.supply_type values. Routers validate against this — the
# field previously accepted any string.
SUPPLY_TYPES: frozenset = frozenset({"mains", "well", "city_pump"})
# supply_type values that mean "this home is pressurized by a pump".
PUMP_SUPPLY_TYPES: frozenset = frozenset({"well", "city_pump"})

PUMP_PROFILE_VFD = "vfd_constant_pressure"
PUMP_PROFILE_SWITCH_TANK = "switch_tank"


def pump_mode_effective(conn, circuit: str) -> dict:
    """Resolve whether pump-aware behavior is active for ``circuit``.

    Returns ``{"active": bool, "profile": str|None, "source": str}`` where
    source is ``"override"`` (sensitivity_config.pump_mode 'on'/'off'),
    ``"user"`` (supply_type says pump), ``"auto"`` (regime detection confirmed
    via the banner), or ``"none"``.

    Precedence: per-circuit override → supply_type ('city_pump' → profile
    default vfd_constant_pressure; 'well' → profile default switch_tank — a
    well IS a pump home) → detected+confirmed → inactive. The well/city
    defaults are RESOLVE-TIME only: home_profile.pump_profile stays NULL until
    detection or the user writes it, so nightly evidence can later identify a
    constant-pressure well pump.

    v1 ENUMERATION — consumers must NOT assume active ⇒ vfd semantics.
    "Active" with profile 'switch_tank' enables almost nothing in v1: the
    Phase 4 detector gating, the 5b cross-circuit leak-test verdict, and the
    6b pump-failure alert ALL additionally require profile ==
    'vfd_constant_pressure' (a tank system's cycle math and failure signatures
    are different instruments — see the pump plan). Unconfirmed auto-detection
    never activates anything (banner+confirm).
    """
    profile_row = conn.execute(
        "SELECT supply_type, pump_profile, pump_mode_detected, pump_mode_ack "
        "FROM home_profile WHERE id = 1").fetchone()
    stored_profile = profile_row["pump_profile"] if profile_row else None

    sens = conn.execute(
        "SELECT pump_mode FROM sensitivity_config WHERE circuit = ?",
        (circuit,)).fetchone()
    override = (sens["pump_mode"] if sens is not None else None) or "auto"
    if override == "off":
        return {"active": False, "profile": None, "source": "override"}
    if override == "on":
        return {"active": True,
                "profile": stored_profile or PUMP_PROFILE_VFD,
                "source": "override"}

    supply = (profile_row["supply_type"] if profile_row else None) or "mains"
    if supply == "city_pump":
        return {"active": True,
                "profile": stored_profile or PUMP_PROFILE_VFD,
                "source": "user"}
    if supply == "well":
        return {"active": True,
                "profile": stored_profile or PUMP_PROFILE_SWITCH_TANK,
                "source": "user"}

    if (profile_row is not None and profile_row["pump_mode_detected"]
            and profile_row["pump_mode_ack"] == "confirmed"):
        return {"active": True,
                "profile": stored_profile or PUMP_PROFILE_VFD,
                "source": "auto"}

    return {"active": False, "profile": None, "source": "none"}


# Per-invocation TTL cache for pump_mode_effective (pump plan round-1 #15):
# feature_extractor and event_rules resolve the flag on EVERY event, so a raw
# 2-query resolve per finalize is wasteful — but the flag must also flip
# everywhere within ~one TTL of a banner confirm / override change, or the
# event detector (build-time resolution + live reload) and the extractor
# would disagree until restart. Routes that change pump state call
# invalidate_pump_mode_cache() explicitly.
_PUMP_MODE_CACHE: dict = {}
_PUMP_MODE_CACHE_TTL_S = 60.0


def pump_mode_effective_cached(conn, circuit: str) -> dict:
    import time as _time
    now = _time.monotonic()
    hit = _PUMP_MODE_CACHE.get(circuit)
    if hit is not None and (now - hit[0]) < _PUMP_MODE_CACHE_TTL_S:
        return hit[1]
    result = pump_mode_effective(conn, circuit)
    _PUMP_MODE_CACHE[circuit] = (now, result)
    return result


def invalidate_pump_mode_cache() -> None:
    _PUMP_MODE_CACHE.clear()


def pump_gates_active(conn, circuit: str) -> bool:
    """True when the vfd-profile Phase 4 detector gates apply to ``circuit``.
    v1 scope: pump-aware detection adjustments require the vfd profile —
    switch_tank homes' oscillation is NOT confined to quiet windows, so these
    gates would misfire there (see the pump plan's Architecture section)."""
    r = pump_mode_effective_cached(conn, circuit)
    return bool(r["active"] and r["profile"] == PUMP_PROFILE_VFD)


# Zone (irrigation) circuits see only a handful of repetitive cycles per
# day, so the whole-home fixture target below is unreachable for them. A
# small fixed target is enough to characterise their flow signature.
# Tunable.
ZONE_MINIMUM_EVENTS = 20


def compute_minimum_events(
    bathrooms_full: int,
    bathrooms_half: int,
    floors: int,
    circuit_kind: str = "fixture",
) -> int:
    """Minimum event count before calibration can complete.

    ``circuit_kind`` is the circuit taxonomy value ('fixture' or 'zone').
    Zone/irrigation circuits use a small fixed target since they generate
    far fewer discrete events than the whole-home main shutoff.
    """
    if circuit_kind == "zone":
        return ZONE_MINIMUM_EVENTS
    estimated_fixtures = (
        bathrooms_full * 3.2
        + bathrooms_half * 1.2
        + floors * 0.5
        + 2.0
    )
    return max(int(estimated_fixtures * 5 * 3), 50)
