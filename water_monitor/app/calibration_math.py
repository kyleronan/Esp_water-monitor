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


def clamp_ppl(value: float) -> float:
    return max(PPL_MIN, min(PPL_MAX, value))


def gate(new_ppl: float, current_ppl: float, method: str, run_count: int) -> Dict[str, Any]:
    """The method-aware sample-count gate + correction + re-baseline flag for an EFFECTIVE
    pulses/litre value. Used both for the pooled suggestion and for a user's manual override
    at apply time, so an edited value is gated on ITS OWN correction: a >3% bucket change
    needs ≥3 runs (averages out fill error); MUNICIPAL is satisfied by one ≥10-gal run; a
    ≤3% change needs one run. (Editing a noisy big suggestion down to a small change unblocks
    apply; editing it up re-blocks.)"""
    corr_pct = (abs(new_ppl - current_ppl) / current_ppl * 100.0) if current_ppl else 0.0
    if corr_pct > LARGE_CORRECTION_PCT and method == "bucket":
        runs_needed = max(0, MIN_RUNS_FOR_LARGE - run_count)
    else:
        runs_needed = 0
    return {
        "correction_pct": round(corr_pct, 1),
        "apply_allowed": runs_needed == 0,
        "runs_needed": runs_needed,
        "will_rebaseline": corr_pct >= REBASELINE_PCT,
    }


def pooled(runs: List[Dict[str, float]], current_ppl: float, method: str) -> Dict[str, Any]:
    """Volume-pool the runs (current_ppl × Σmeasured ÷ Σactual) + per-run PPLs + spread +
    the sample-count gate. Each run is ``{measured_l, actual_l, run_ppl}``."""
    sm = sum(r["measured_l"] for r in runs)
    sa = sum(r["actual_l"] for r in runs)
    new_ppl = clamp_ppl(current_ppl * sm / sa) if sa > 0 else current_ppl
    ppls = [r["run_ppl"] for r in runs]
    spread_pct = ((max(ppls) - min(ppls)) / current_ppl * 100.0) if (ppls and current_ppl) else 0.0
    return {
        "current_ppl": round(current_ppl, 1),
        "new_ppl": round(new_ppl, 1),
        "spread_pct": round(spread_pct, 1),
        "run_ppls": [round(p, 1) for p in ppls],
        "run_count": len(runs),
        **gate(new_ppl, current_ppl, method, len(runs)),
    }
