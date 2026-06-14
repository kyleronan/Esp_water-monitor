"""Phase 2.4 — per-home artifact-detector calibration (SAFETY-CRITICAL).

The phantom / cross-talk detectors ZERO an event's volume; dribble excludes it from
training (volume kept). This module calibrates ONLY the "long-quiet" / dribble
IDENTIFIER thresholds from the home's user-confirmed classifications, frozen at
activation. Three safety invariants:

1. **Leak-safety guards are never calibrated.** The phantom/cross-talk true-flow
   ceilings live in :mod:`feature_extractor` and are deliberately absent from
   ``ARTIFACT_DEFAULTS`` — a real leak moves water and is excluded by them
   regardless of the duration/ΔP tuning, so they stay frozen here too.
2. **Do-no-harm against confirmed-NOT-this-artifact events.** A fitted threshold is
   applied only if the *actual* detector, run with it, flags **zero** of the events
   the user confirmed are NOT this artifact (other-artifact classifications + every
   user-labelled real fixture). So it can never zero confirmed-real water.
3. **Conservative.** A detector needs ≥ ``MIN_POSITIVES`` confirmed positives AND a
   non-empty negative set, else it keeps the shipped default. Most homes → defaults.

Frozen at activation/retrain only — never on ordinary reclassify / live events.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .feature_extractor import (
    ARTIFACT_DEFAULTS,
    _detect_cross_talk,
    _detect_low_flow_dribble,
    _detect_pressure_restoration_phantom,
)

log = logging.getLogger(__name__)

MIN_POSITIVES = 8       # confirmed positives before a detector is calibrated
_MARGIN = 0.10

# Absolute clamps for each calibratable threshold (key → (lo, hi)). The cross-talk
# min-duration only LOWERS toward the floor (catch the home's shorter artifacts);
# dribble ceilings only RAISE toward the cap. Anything fitted is clamped here.
# Phantom duration is intentionally NOT here — its floors became frozen structural
# constants (feature_extractor 2026-06-14) so the legacy floor can never be lowered.
_BOUNDS: Dict[str, Tuple[float, float]] = {
    "XTALK_MIN_DURATION_S":   (60.0, 120.0),
    "DRIBBLE_MAX_VOLUME_L":   (0.5, 2.0),
    "DRIBBLE_MAX_FLOW_LPM":   (1.0, 2.0),
    "DRIBBLE_MAX_DELTA_PSI":  (1.5, 3.0),
}


def _clamp(key: str, val: float) -> float:
    lo, hi = _BOUNDS[key]
    return max(lo, min(hi, val))


def _load_rows(conn: sqlite3.Connection, circuit: str) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT user_fixture_type, COALESCE(user_classified,0) AS uc, "
        "  COALESCE(is_pressure_restoration_phantom,0) AS ph, "
        "  COALESCE(is_cross_talk,0) AS ct, COALESCE(is_low_flow_dribble,0) AS dr, "
        "  duration_seconds, pressure_delta_psi, true_avg_flow_lpm, "
        "  flow_integral_litres, flow_on_ratio, volume_litres, avg_flow_lpm "
        "FROM events WHERE circuit = ?", (circuit,),
    ).fetchall()


def _is_real_label(r) -> bool:
    """The user gave it a real fixture type → confirmed real water (a negative for
    every volume-zeroing detector)."""
    t = r["user_fixture_type"]
    return bool(t) and str(t).strip() != ""


# Each detector: positives flag, the detector fn, and how to call it from a row.
def _phantom(r, calib):
    return _detect_pressure_restoration_phantom(
        r["duration_seconds"], r["pressure_delta_psi"],
        true_avg_flow_lpm=r["true_avg_flow_lpm"],
        flow_integral_litres=r["flow_integral_litres"],
        flow_on_ratio=r["flow_on_ratio"], calib=calib)


def _xtalk(r, calib):
    return _detect_cross_talk(
        r["duration_seconds"], r["pressure_delta_psi"],
        r["flow_integral_litres"], r["flow_on_ratio"], calib=calib)


def _dribble(r, calib):
    return _detect_low_flow_dribble(
        r["volume_litres"], r["avg_flow_lpm"], r["pressure_delta_psi"], calib=calib)


def _fit_one(rows, flag, detect, fit_keys) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """Fit one detector's thresholds + run the do-no-harm safety gate.

    ``fit_keys`` is a dict ``{threshold_key: ('min'|'max', row_field)}`` — 'min'
    lowers the threshold to the *minimum* of the positives' field (catch the
    shortest), 'max' raises it to the *maximum* (catch the largest). Returns
    ``(accepted_calib, report)``."""
    pos = [r for r in rows if r["uc"] and r[flag]]
    # Negatives: every event the user confirmed is NOT this artifact — the other
    # artifact classifications AND every user-labelled real fixture.
    neg = [r for r in rows if (r["uc"] and not r[flag]) or _is_real_label(r)]
    rep: Dict[str, Any] = {"positives": len(pos), "negatives": len(neg)}

    if len(pos) < MIN_POSITIVES or not neg:
        rep["status"] = "insufficient_truth"
        return {}, rep

    candidate: Dict[str, float] = {}
    for key, (mode, field) in fit_keys.items():
        vals = [float(r[field]) for r in pos if r[field] is not None]
        if not vals:
            continue
        default = float(ARTIFACT_DEFAULTS[key])
        if mode == "min":                         # lower to catch the shortest
            fitted = _clamp(key, min(vals) * (1.0 - _MARGIN))
            if fitted < default:                  # only ever loosen toward catching more
                candidate[key] = fitted
        else:                                     # 'max' — raise to catch the largest
            fitted = _clamp(key, max(vals) * (1.0 + _MARGIN))
            if fitted > default:
                candidate[key] = fitted

    if not candidate:
        rep["status"] = "no_change"
        return {}, rep

    # ── DO-NO-HARM SAFETY GATE ──────────────────────────────────────────────────
    # The calibrated detector must flag NONE of the confirmed-not-this-artifact
    # events. If it would, the calibration could zero confirmed-real water → reject.
    harmed = sum(1 for r in neg if detect(r, candidate))
    if harmed:
        rep["status"] = "rejected_would_harm"
        rep["harmed"] = harmed
        return {}, rep

    caught = sum(1 for r in pos if detect(r, candidate))
    caught_default = sum(1 for r in pos if detect(r, None))
    rep["status"] = "fit"
    rep["caught"] = caught
    rep["caught_default"] = caught_default
    rep["keys"] = candidate
    return candidate, rep


def fit_artifact_thresholds(
        conn: sqlite3.Connection,
        circuit: str) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """Fit per-home artifact-detector thresholds from confirmed classifications,
    safety-gated. Returns ``(calib, report)``. Pure read — does not persist."""
    rows = _load_rows(conn, circuit)
    calib: Dict[str, float] = {}
    report: Dict[str, Any] = {}

    for name, flag, detect, fit_keys in (
        # Phantom has no calibratable threshold any more (its duration floors are frozen
        # structural constants). Kept in the loop with empty fit_keys so the report still
        # carries its confirmed-positive count for the UI ("Phantom: defaults").
        ("phantom", "ph", _phantom, {}),
        ("cross_talk", "ct", _xtalk,
         {"XTALK_MIN_DURATION_S": ("min", "duration_seconds")}),
        ("dribble", "dr", _dribble,
         {"DRIBBLE_MAX_VOLUME_L": ("max", "volume_litres"),
          "DRIBBLE_MAX_FLOW_LPM": ("max", "avg_flow_lpm"),
          "DRIBBLE_MAX_DELTA_PSI": ("max", "pressure_delta_psi")}),
    ):
        accepted, rep = _fit_one(rows, flag, detect, fit_keys)
        calib.update(accepted)
        report[name] = rep
    return calib, report


def freeze_artifact_thresholds(conn: sqlite3.Connection, circuit: str,
                               source: str = "activation") -> Dict[str, Any]:
    """Fit + safety-gate + persist (freeze) the artifact-detector calibration."""
    calib, report = fit_artifact_thresholds(conn, circuit)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO artifact_calibration "
        "    (circuit, params, report, source, locked_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(circuit) DO UPDATE SET params=excluded.params, "
        "  report=excluded.report, source=excluded.source, "
        "  locked_at=excluded.locked_at, updated_at=excluded.updated_at",
        (circuit, json.dumps(calib), json.dumps(report), source, now, now),
    )
    conn.commit()
    invalidate_artifact_cache(circuit)
    log.info("[%s] artifact calibration frozen (%s): %s",
             circuit, source, json.dumps(calib) if calib else "all defaults")
    return report


# Per-circuit cache so the live persist / recompute hot paths don't hit the DB per
# event. Calibration only changes at freeze, which invalidates the entry.
_calib_cache: Dict[str, Dict[str, Any]] = {}


def invalidate_artifact_cache(circuit: Optional[str] = None) -> None:
    if circuit is None:
        _calib_cache.clear()
    else:
        _calib_cache.pop(circuit, None)


def load_artifact_calibration(conn: sqlite3.Connection, circuit: str,
                              *, use_cache: bool = True) -> Dict[str, Any]:
    """Return the frozen artifact-threshold overrides for a circuit, or ``{}``."""
    if use_cache and circuit in _calib_cache:
        return _calib_cache[circuit]
    data: Dict[str, Any] = {}
    try:
        row = conn.execute(
            "SELECT params FROM artifact_calibration WHERE circuit = ?", (circuit,)
        ).fetchone()
    except sqlite3.OperationalError:
        return {}
    if row and row["params"]:
        try:
            parsed = json.loads(row["params"])
            if isinstance(parsed, dict):
                data = parsed
        except (json.JSONDecodeError, TypeError):
            data = {}
    if use_cache:
        _calib_cache[circuit] = data
    return data


_DETECTOR_LABELS = (("phantom", "Phantom"), ("cross_talk", "Cross-talk"),
                    ("dribble", "Dribble"))


def _detector_summary(rep: Dict[str, Any]) -> str:
    """One-line, human-readable outcome for a detector's fit report."""
    st = rep.get("status")
    if st == "fit":
        return f"calibrated ({rep.get('caught')}/{rep.get('positives')} caught)"
    if st == "rejected_would_harm":
        return f"defaults — fit would flag {rep.get('harmed')} confirmed-real"
    if st == "insufficient_truth":
        return f"defaults — {rep.get('positives', 0)} confirmed (need {MIN_POSITIVES})"
    return "defaults"


def get_artifact_calibration_meta(conn: sqlite3.Connection,
                                  circuit: str) -> Optional[Dict[str, Any]]:
    """Lock metadata for the UI: when frozen, how many thresholds were calibrated,
    and a per-detector fit-vs-fallback summary. None if never frozen."""
    try:
        row = conn.execute(
            "SELECT params, report, source, locked_at "
            "FROM artifact_calibration WHERE circuit = ?", (circuit,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    try:
        params = json.loads(row["params"]) if row["params"] else {}
    except (json.JSONDecodeError, TypeError):
        params = {}
    try:
        report = json.loads(row["report"]) if row["report"] else {}
    except (json.JSONDecodeError, TypeError):
        report = {}
    return {
        "locked_at": row["locked_at"],
        "source": row["source"],
        "count": len(params),
        "detectors": [
            {"label": label, "summary": _detector_summary(report.get(key, {}))}
            for key, label in _DETECTOR_LABELS
        ],
    }
