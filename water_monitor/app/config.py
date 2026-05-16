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
    esp_device_prefix: str = ""

    @property
    def is_zone_circuit(self) -> bool:
        return self.circuit_type == "zone"

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

    def get_circuit(self, name: str) -> Optional[CircuitConfig]:
        return next((c for c in self.circuits if c.circuit == name), None)


def load_config() -> AddonConfig:
    """Read /data/options.json. Falls back to defaults for dev/testing."""
    raw: dict = {}
    if OPTIONS_PATH.exists():
        with OPTIONS_PATH.open() as f:
            raw = json.load(f)

    circuits = []
    for c in raw.get("circuits", []):
        circuits.append(CircuitConfig(
            circuit=c["circuit"],
            circuit_type=c.get("circuit_type", "fixture"),
        ))

    # Default circuits if none configured
    if not circuits:
        circuits = [
            CircuitConfig(circuit="circuit_1", circuit_type="fixture"),
            CircuitConfig(circuit="circuit_2", circuit_type="zone"),
        ]

    return AddonConfig(
        log_level=raw.get("log_level", "info"),
        esp_device_name=raw.get("esp_device_name", ""),
        circuits=circuits,
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
        "pressure_drop_event_psi": 2.0,
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

    if supply_type == "well":
        base_days = min(base_days + 7, 35)

    return min(base_days, 35), tier


def compute_minimum_events(
    bathrooms_full: int,
    bathrooms_half: int,
    floors: int,
) -> int:
    """Minimum event count before calibration can complete."""
    estimated_fixtures = (
        bathrooms_full * 3.2
        + bathrooms_half * 1.2
        + floors * 0.5
        + 2.0
    )
    return max(int(estimated_fixtures * 5 * 3), 50)
