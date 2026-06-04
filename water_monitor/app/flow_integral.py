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
