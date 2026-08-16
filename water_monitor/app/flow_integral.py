"""Pure flow-integration + active-flow feature helpers.

Shared by the live event detector, the historical importer, and the volume
recompute backfill so all three compute volume and active-flow features
*identically*. No DB / HA / app imports — keep it pure and unit-testable.

Background: an event's water volume must be a TIME-INTEGRAL of the flow, not
``mean(flow_samples) × pressure_event_duration``. The HA flow sensor reports
on-change, so a brief burst trapped inside a long pressure-defined event would
otherwise inflate volume by up to ~180x (the volume-over-count bug: 6 L of real
flow stored as 1,089 L).

Volume unit note: flow is L/min and intervals are in seconds, so a held flow of
``f`` L/min over ``dt`` seconds contributes ``f * (dt / 60.0)`` litres — divide
by 60, NOT 3600.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Dict, List, Tuple

Sample = Tuple[datetime, float]   # (timestamp, flow L/min)

# Noise floor: a real fixture flows above this; below is sensor noise / off.
_ACTIVE_FLOW_MIN_LPM: float = 0.15
# Segment debounce: a sub-2s blip isn't its own "on" segment, and two on-runs
# separated by a < 3s dip are one segment (a steady fixture, not two events).
_ACTIVE_FLOW_MIN_ON_SECONDS: float = 2.0
_ACTIVE_FLOW_MERGE_GAP_SECONDS: float = 3.0
# Cap a single inter-sample interval. A larger gap means the sensor/callback
# stream paused (offline) — holding the last flow across hours would fabricate
# volume. We clamp the interval AND signal ``capped`` so the caller can mark the
# event's integration as degraded (and keep it out of classifier training)
# rather than silently undercounting.
_FLOW_INTEGRAL_MAX_DT_SECONDS: float = 120.0


def _clean(samples) -> List[Sample]:
    """Drop non-finite / negative / unparseable samples; sort by timestamp."""
    out: List[Sample] = []
    for pair in samples or ():
        try:
            t, v = pair
            f = float(v)
        except (TypeError, ValueError):
            continue
        if t is not None and math.isfinite(f) and f >= 0.0:
            out.append((t, f))
    out.sort(key=lambda s: s[0])
    return out


def _dt_seconds(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds()


def integrate_litres(samples) -> Tuple[float, bool]:
    """Time-integrate on-change flow samples to litres.

    ``samples``: iterable of ``(datetime, flow_lpm)``; flow is held (left value)
    until the next sample. Returns ``(litres, capped)`` where ``capped`` is True
    if any inter-sample gap exceeded ``_FLOW_INTEGRAL_MAX_DT_SECONDS`` and was
    clamped — i.e. real water may have been undercounted during an offline gap.
    """
    s = _clean(samples)
    if len(s) < 2:
        return 0.0, False
    litres = 0.0
    capped = False
    for i in range(1, len(s)):
        dt = _dt_seconds(s[i - 1][0], s[i][0])
        if dt <= 0:
            continue
        if dt > _FLOW_INTEGRAL_MAX_DT_SECONDS:
            # Only a clamp over REAL flow undercounts water; clamping an
            # off-interval (held ~0) is lossless, so don't flag those.
            if s[i - 1][1] > _ACTIVE_FLOW_MIN_LPM:
                capped = True
            dt = _FLOW_INTEGRAL_MAX_DT_SECONDS
        litres += s[i - 1][1] * (dt / 60.0)   # L/min × min → L  (÷60)
    return round(litres, 4), capped


# ── dev38: meter registration correction (ANNOTATION ONLY) ────────────────────
# The 2026-08 audit inverted the pressure channel — an independent witness
# that shares none of the oval-gear meter's pulse mechanism — and recovered
# the meter's registration curve on the pre-pump eras (n=1,086 events):
#
#   band (L/min)   metered ÷ true    95% CI
#   >= 8.0            0.999          0.971 – 1.030
#   4.0 – 8.0         0.941          0.908 – 0.986
#   2.5 – 4.0         0.904          0.826 – 0.958
#   1.5 – 2.5         0.732          0.669 – 0.821
#   1.0 – 1.5         0.59           0.316 – 1.767  (weak; n=20)
#
# The curve is RELATIVE to the meter's own >= 8 L/min band (a common-mode
# scale error is invisible to it) and remains pending utility-anchor
# validation — which is why the corrected figure is stored as a separate
# estimate column and NEVER feeds volume_litres, volume_litres_effective or
# any total (the orchestrator's historical-volumes-are-never-recomputed
# invariant). Sub-1 L/min flow gets NO correction: non-registration cannot
# be recovered by a ratio, and extrapolating 0.59 downward would be
# invention — those draws stay governed by the below-meter-floor verdict.
_REGISTRATION_RATIO: Tuple[Tuple[float, float, float], ...] = (
    (8.0, math.inf, 0.999),
    (4.0, 8.0, 0.941),
    (2.5, 4.0, 0.904),
    (1.5, 2.5, 0.732),
    (1.0, 1.5, 0.59),
)
# Store the estimate only when it moves the integral by more than this
# fraction (mirrors the UI display threshold).
_REGISTRATION_MIN_DELTA_FRAC = 0.02

# dev41 (E1): the curve now LIVES IN DATA (registration_curve table, seeded
# v1 = the constants above by migration 20260807) and is loaded here at boot
# via set_registration_curve(). The constants remain only as the identical
# fallback for a pre-20260807 schema. Every stored estimate is stamped with
# the version that produced it (events.registration_curve_version).
_curve_bands: Tuple[Tuple[float, float, float], ...] = _REGISTRATION_RATIO
_curve_version: int = 1
_curve_status: str = "unvalidated"


def set_registration_curve(bands, version: int, status: str) -> None:
    """Install the DB-backed curve: bands = [{band_lo_lpm, band_hi_lpm,
    ratio}, ...] (band_hi_lpm None = unbounded), highest band first."""
    global _curve_bands, _curve_version, _curve_status
    parsed = tuple(
        (float(b["band_lo_lpm"]),
         math.inf if b["band_hi_lpm"] is None else float(b["band_hi_lpm"]),
         float(b["ratio"]))
        for b in bands)
    if parsed:
        _curve_bands = parsed
        _curve_version = int(version)
        _curve_status = str(status)


def registration_curve_version() -> int:
    return _curve_version


def registration_curve_status() -> str:
    """'unvalidated' until a low-flow anchor confirms the curve (E2)."""
    return _curve_status


def _registration_ratio(flow_lpm: float) -> float:
    """metered/true ratio for one sample's flow rate; 1.0 outside 1–∞."""
    for lo, hi, ratio in _curve_bands:
        if lo <= flow_lpm < hi:
            return ratio
    return 1.0


def integrate_litres_registration_corrected(samples) -> Tuple[float, float]:
    """dev38 — ``(raw_litres, corrected_litres)`` over the same sample walk.

    Identical hold semantics to :func:`integrate_litres`; each sample's
    contribution in the 1.0–8.0 L/min correction band is divided by its
    band's registration ratio. Returned pair lets the caller apply the
    2% materiality gate without re-integrating.
    """
    s = _clean(samples)
    if len(s) < 2:
        return 0.0, 0.0
    raw = corrected = 0.0
    for i in range(1, len(s)):
        dt = _dt_seconds(s[i - 1][0], s[i][0])
        if dt <= 0:
            continue
        if dt > _FLOW_INTEGRAL_MAX_DT_SECONDS:
            dt = _FLOW_INTEGRAL_MAX_DT_SECONDS
        flow = s[i - 1][1]
        contrib = flow * (dt / 60.0)
        raw += contrib
        corrected += contrib / _registration_ratio(flow)
    return round(raw, 4), round(corrected, 4)


def registration_estimate(samples):
    """dev38 — the value stored in ``events.registration_est_litres``.

    The corrected integral, or None when the correction is immaterial
    (< 2% above the raw integral) or nothing integrates. None is the common
    case: draws at fixture flow rates (>= 4 L/min peaks with brief ramps)
    barely graze the correction bands.
    """
    raw, corrected = integrate_litres_registration_corrected(samples)
    if raw <= 0 or corrected <= raw * (1.0 + _REGISTRATION_MIN_DELTA_FRAC):
        return None
    return round(corrected, 3)


def _segment_count(intervals: List[Tuple[float, bool]]) -> int:
    """Count distinct flow "on" segments with debounce.

    ``intervals``: list of ``(dt_seconds, is_on)`` (left-held). On-runs separated
    by an off-gap shorter than ``_ACTIVE_FLOW_MERGE_GAP_SECONDS`` are merged; runs
    whose total on-time is below ``_ACTIVE_FLOW_MIN_ON_SECONDS`` are dropped.
    """
    runs: List[float] = []        # duration of each contiguous on-run
    gaps: List[float] = []        # off-gap between run i and run i+1
    in_on = False
    cur_on = 0.0
    cur_off = 0.0
    for dt, on in intervals:
        if on:
            if not in_on:
                if runs:                 # gap that preceded this on-run
                    gaps.append(cur_off)
                in_on = True
                cur_on = 0.0
                cur_off = 0.0
            cur_on += dt
        else:
            if in_on:
                runs.append(cur_on)
                in_on = False
                cur_off = 0.0
            cur_off += dt
    if in_on:
        runs.append(cur_on)

    # Merge runs separated by a sub-threshold gap (absorb the gap into the run).
    merged: List[float] = []
    for idx, r in enumerate(runs):
        if merged and idx - 1 < len(gaps) and gaps[idx - 1] < _ACTIVE_FLOW_MERGE_GAP_SECONDS:
            merged[-1] += r + gaps[idx - 1]
        else:
            merged.append(r)
    return sum(1 for r in merged if r >= _ACTIVE_FLOW_MIN_ON_SECONDS)


def active_flow_features(samples, window_seconds: float) -> Dict[str, float]:
    """Compute the active-flow feature set from timestamped flow samples.

    Returns: active_flow_duration_seconds, true_avg_flow_lpm, flow_integral_litres,
    flow_on_ratio, active_flow_segment_count, flow_cv_on_segments.
    """
    s = _clean(samples)
    litres, _capped = integrate_litres(s)

    intervals: List[Tuple[float, bool]] = []
    active = 0.0
    on_vals: List[float] = []
    for i in range(1, len(s)):
        dt = _dt_seconds(s[i - 1][0], s[i][0])
        if dt <= 0:
            continue
        if dt > _FLOW_INTEGRAL_MAX_DT_SECONDS:
            dt = _FLOW_INTEGRAL_MAX_DT_SECONDS
        v = s[i - 1][1]
        on = v > _ACTIVE_FLOW_MIN_LPM
        intervals.append((dt, on))
        if on:
            active += dt
            on_vals.append(v)

    true_avg = (litres / (active / 60.0)) if active > 0 else 0.0
    on_ratio = (active / window_seconds) if window_seconds and window_seconds > 0 else 0.0
    on_ratio = min(on_ratio, 1.0)
    if len(on_vals) >= 2:
        mean = sum(on_vals) / len(on_vals)
        if mean > 0:
            var = sum((x - mean) ** 2 for x in on_vals) / len(on_vals)
            cv = math.sqrt(var) / mean
        else:
            cv = 0.0
    else:
        cv = 0.0

    return {
        "active_flow_duration_seconds": round(active, 2),
        "true_avg_flow_lpm": round(true_avg, 4),
        "flow_integral_litres": litres,
        "flow_on_ratio": round(on_ratio, 4),
        "active_flow_segment_count": _segment_count(intervals),
        "flow_cv_on_segments": round(cv, 4),
    }
