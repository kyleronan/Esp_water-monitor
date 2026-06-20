"""Pure flow-calibration math — no FastAPI / HA deps, so it is unit-testable.

The true pulses-per-litre from one run is ``current_ppl × (Δmeasured_L ÷ actual_L)``
(algebraically = pulses ÷ actual_L, self-correcting even if ``current_ppl`` is far off).
Runs pool by VOLUME: ``current_ppl × Σmeasured ÷ Σactual`` (weights longer runs more).
``routers/calibration.py`` is the session / HA glue around this.
"""
from __future__ import annotations

from typing import Any, Dict, List

from .units import FLOW_OPTIONS

# ── Tuning ──────────────────────────────────────────────────────────────────
BUCKET_MIN_L = 2.0              # below this the 0.01 L sensor resolution is too coarse
MUNICIPAL_MIN_L = 38.0         # ~10 US gal — coarse meter resolution is beaten by volume
LARGE_CORRECTION_PCT = 3.0      # a >3% bucket correction triggers the sample-count gate
MIN_RUNS_FOR_LARGE = 3
REBASELINE_PCT = 10.0          # mirrors orchestrator._PPL_REBASELINE_FRACTION (UI message only)
PPL_MIN = 1.0
PPL_MAX = 5000.0

# Volume-unit label → factor (litres = reading / factor). L=1, gal, ft³, m³ from FLOW_OPTIONS.
METER_UNIT_FACTORS: Dict[str, float] = {
    opt["vol_label"]: opt["vol_factor"] for opt in FLOW_OPTIONS.values()
}


def to_litres(value: float, vol_factor: float) -> float:
    """Convert a display / meter volume to litres (stored L × vol_factor = display value)."""
    return value / vol_factor if vol_factor else value


def run_ppl(current_ppl: float, measured_l: float, actual_l: float) -> float:
    """True pulses-per-litre for one run: current_ppl × measured ÷ actual (= pulses ÷ actual)."""
    return current_ppl * measured_l / actual_l if actual_l > 0 else current_ppl


def pooled(runs: List[Dict[str, float]], current_ppl: float, method: str) -> Dict[str, Any]:
    """Volume-pool the runs and apply the method-aware sample-count gate + run spread.

    Each run is ``{measured_l, actual_l, run_ppl}``. Gate: a >3% pooled correction needs
    ≥3 BUCKET runs (averages out fill error); the MUNICIPAL method is satisfied by one run
    (its ≥10-gal minimum beats meter resolution by volume); a ≤3% correction needs one run.
    """
    sm = sum(r["measured_l"] for r in runs)
    sa = sum(r["actual_l"] for r in runs)
    new_ppl = current_ppl * sm / sa if sa > 0 else current_ppl
    new_ppl = max(PPL_MIN, min(PPL_MAX, new_ppl))
    ppls = [r["run_ppl"] for r in runs]
    spread_pct = ((max(ppls) - min(ppls)) / current_ppl * 100.0) if (ppls and current_ppl) else 0.0
    corr_pct = (abs(new_ppl - current_ppl) / current_ppl * 100.0) if current_ppl else 0.0
    if corr_pct > LARGE_CORRECTION_PCT and method == "bucket":
        runs_needed = max(0, MIN_RUNS_FOR_LARGE - len(runs))
    else:
        runs_needed = 0
    return {
        "current_ppl": round(current_ppl, 1),
        "new_ppl": round(new_ppl, 1),
        "correction_pct": round(corr_pct, 1),
        "spread_pct": round(spread_pct, 1),
        "run_ppls": [round(p, 1) for p in ppls],
        "run_count": len(runs),
        "apply_allowed": runs_needed == 0,
        "runs_needed": runs_needed,
        "will_rebaseline": corr_pct >= REBASELINE_PCT,
    }
