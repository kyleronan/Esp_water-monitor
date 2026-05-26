"""
Feature extractor — Phase 2.

Consumes RawEvent objects from the detection queue and:
  1. Computes the full feature vector for each event
  2. Stores the event in the SQLite events table
  3. Updates hourly_volume for the chart
  4. Updates the training state event count
  5. Feeds non-excluded events to ClusterEngine (DBSTREAM) for online
     cluster matching and sequence context recording

Algorithm: DBSTREAM via river.cluster.DBSTREAM (online, no fixed K).
DBSCAN batch clustering was considered and rejected — see ADR 003.

Feature vector:
  Temporal:
    duration_log           log(duration_seconds + 1)
    hour_sin / hour_cos    cyclical hour-of-day encoding
    day_of_week            0=Mon, 6=Sun
    is_weekend             boolean

  Flow:
    avg_flow_lpm           mean flow during event
    peak_flow_lpm          maximum flow during event
    flow_variability       std dev of flow readings

  Pressure:
    pressure_delta_psi     pre-event pressure - min pressure during event
    pre_event_pressure     baseline pressure before event
    resistance_ratio       pressure_delta / avg_flow  (true ΔP/Q)
    resistance_shape       steady/rising/falling/pulsed/unknown

  Detection provenance:
    start_trigger          'flow' | 'pressure' | 'pressure+flow'
    has_pressure_transient whether a pressure transient was captured

  Propagation:
    propagation_delay_s    seconds between event start and flow onset
                           (only meaningful for pressure-triggered events)

Resistance shape classification
---------------------------------
Shape is computed on the TRUE hydraulic resistance curve ΔP/Q, where
ΔP = pre_event_pressure - pressure[i] (the actual pressure drop due to
demand, not the absolute line pressure).

The first and last 20% of readings are excluded before classification
so ramp-up and ramp-down transients don't corrupt the trend analysis.

  steady  — CV < 0.55 and trend change < 15%
             Fixed-orifice fixture: tap, shower, hose.
  rising  — resistance increases by > 15% first→last third
             Filling a vessel against rising back-pressure: toilet cistern,
             bath, header tank.
  falling — resistance decreases by > 15% first→last third
             Zone opening against diminishing restriction: irrigation valve,
             washer fill phase.
  pulsed  — CV >= 0.55 after ramp exclusion
             Genuine cyclic demand: dishwasher spray arm rotation,
             washing machine agitation, sprinkler head sweep.
  unknown — fewer than 10 usable paired readings after ramp exclusion
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
import statistics
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .event_detector import RawEvent, WaveformRecord

log = logging.getLogger(__name__)

from .cluster_engine import SEQUENCE_GAP_MAX_SECONDS as _SEQUENCE_GAP_MAX_S


def _safe_float(values: list, default: float = 0.0) -> float:
    valid = [v for v in values if v is not None and not math.isnan(v)]
    return default if not valid else sum(valid) / len(valid)


def _safe_std(values: list) -> float:
    valid = [v for v in values if v is not None and not math.isnan(v)]
    if len(valid) < 2:
        return 0.0
    return statistics.stdev(valid)


def _classify_resistance_shape(
    pressure_readings: List[float],
    flow_readings: List[float],
    pre_event_pressure_psi: float,
) -> str:
    """
    Classify the hydraulic resistance curve shape.

    Uses TRUE resistance: ΔP/Q where ΔP = pre_event_pressure - pressure[i].
    This isolates the fixture's hydraulic load from the absolute line pressure,
    making the classification independent of household supply pressure.

    Ramp phases (first and last 20% of readings) are excluded so the
    classification reflects steady-state behaviour only.

    Returns one of: steady | rising | falling | pulsed | unknown
    """
    n = min(len(pressure_readings), len(flow_readings))
    if n < 10:
        return "unknown"

    # Exclude ramp-up and ramp-down (first and last 20%)
    ramp = max(1, n // 5)
    p_mid = pressure_readings[ramp: n - ramp]
    f_mid = flow_readings[ramp: n - ramp]

    # Compute true ΔP/Q resistance at each steady-state point.
    # Skip readings where flow is below noise floor to avoid division
    # by near-zero inflating variance.
    MIN_FLOW = 0.15   # L/min noise floor (matches event_detector.MIN_FLOW_LPM)
    resistance = []
    for p, f in zip(p_mid, f_mid):
        if f >= MIN_FLOW:
            delta_p = pre_event_pressure_psi - p   # positive = pressure has dropped
            if delta_p >= 0:                        # only during actual demand
                resistance.append(delta_p / f)

    if len(resistance) < 6:
        return "unknown"

    mean_r = statistics.mean(resistance)
    if mean_r <= 0:
        return "unknown"

    # Coefficient of variation — high CV = genuinely pulsed demand.
    # Threshold 0.55 is calibrated to reject sensor noise at the pressure
    # trough (which inflated CV under the old formula) while still catching
    # real oscillating appliances (dishwashers, washing machines) which
    # typically produce CV > 0.80.
    cv = _safe_std(resistance) / mean_r
    if cv >= 0.55:
        return "pulsed"

    # Trend: compare first and last third of steady-state resistance
    third = max(1, len(resistance) // 3)
    r1 = statistics.mean(resistance[:third])
    r3 = statistics.mean(resistance[-third:])

    # 15% change threshold for rising / falling
    change_ratio = (r3 - r1) / max(abs(r1), 0.01)
    if change_ratio > 0.15:
        return "rising"
    if change_ratio < -0.15:
        return "falling"
    return "steady"


# ─────────────────────────────────────────────────────────────────────────────
# Degraded-supply guard (added 2026-05-26)
#
# Detects when an event was captured during supply-pulsation conditions, in
# which case the paddlewheel flow sensor produces chaotic readings (forward
# and reverse pulses both count as positive; brief zero-velocity transitions
# register as 0 L/min). See plan: zany-yawning-church.md for full design.
#
# Constants below are site-tunable. Defaults calibrated against the
# 2026-05-25 diagnostic session where pulse period was ~4 s; the range
# permits per-installation variation.
# ─────────────────────────────────────────────────────────────────────────────
SUPPLY_PULSE_PERIOD_MIN_S    = 1.0
SUPPLY_PULSE_PERIOD_MAX_S    = 6.0
MIN_CYCLES_FOR_DETECTION     = 2.0
MID_PRESSURE_STD_PULSING_PSI = 0.30    # clean events 0.05-0.12; pulsing ≥ 0.45
PRESSURE_AUTOCORR_THRESHOLD  = 0.5
FLOW_TROUGH_LPM              = 0.20
MIN_TROUGH_EPISODE_RATE_HZ   = 0.15
APPLIANCE_FLOW_PRESSURE_RATIO = 30.0
VOLUME_ENVELOPE_PERCENTILE   = 0.95
VOLUME_ENVELOPE_WINDOW_S     = 5.0
FIXTURE_CYCLE_PERIOD_MAX_S   = 30.0
MAX_WAVEFORM_BINS            = 1000


def _finite_float_series(values) -> List[float]:
    """Strip None/NaN/inf; coerce to float at full precision.

    For detection math (detrending, std-dev, autocorrelation). Do NOT use
    the rounded storage variant — accumulated 3-dp rounding error can
    suppress small but real periodic signals.
    """
    out = []
    for v in values or []:
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            out.append(f)
    return out


def _clean_numeric_series(values) -> List[float]:
    """Storage variant: sanitize and round to 3 dp for JSON serialization.

    Use ONLY for waveform persistence; never for detection math.
    """
    return [round(v, 3) for v in _finite_float_series(values)]


def _detrend_linear(values: List[float]) -> List[float]:
    """Subtract the first-to-last linear trend from `values`.

    Removes slow recovery drift so autocorrelation finds true periodicity
    rather than a slope-induced false peak.
    """
    n = len(values)
    if n < 2:
        return list(values)
    slope = (values[-1] - values[0]) / (n - 1)
    return [v - slope * i for i, v in enumerate(values)]


def _autocorr_at_lag(values: List[float], lag: int) -> float:
    """Normalized autocorrelation of `values` at the given sample lag.

    Returns a value in approximately [-1, 1]; returns 0 when undefined.
    """
    n = len(values) - lag
    if n <= 1:
        return 0.0
    mean = sum(values) / len(values)
    centered = [v - mean for v in values]
    var = sum(c * c for c in centered) / len(centered)
    if var < 1e-9:
        return 0.0
    cov = sum(centered[i] * centered[i + lag] for i in range(n)) / n
    return cov / var


def _dominant_period_s(
    values: List[float],
    sample_rate_hz: float,
    min_period_s: float,
    max_period_s: float,
):
    """Find the dominant period in `values` by autocorrelation peak search.

    Returns ``(period_s, score)``. ``period_s`` is None when no peak in the
    search band exceeds PRESSURE_AUTOCORR_THRESHOLD; ``score`` is the best
    autocorrelation value seen regardless (useful for diagnostics).

    Complexity: O(N · num_lags) — fine for our event sample sizes (10² to
    10⁴ samples, search band ≤ a few hundred lags).
    """
    if not values or sample_rate_hz <= 0:
        return None, 0.0
    min_lag = max(2, int(round(min_period_s * sample_rate_hz)))
    max_lag = int(round(max_period_s * sample_rate_hz))
    if max_lag >= len(values) // 2:
        max_lag = len(values) // 2 - 1
    if max_lag <= min_lag:
        return None, 0.0
    best_lag, best_score = None, 0.0
    for lag in range(min_lag, max_lag + 1):
        score = _autocorr_at_lag(values, lag)
        if score > best_score:
            best_score, best_lag = score, lag
    if best_lag is None or best_score < PRESSURE_AUTOCORR_THRESHOLD:
        return None, best_score
    return best_lag / sample_rate_hz, best_score


def _count_trough_episodes(flow_readings: List[float], threshold: float) -> int:
    """Count contiguous below-threshold runs in flow_readings.

    Each run is ONE episode regardless of length. This counts the number of
    "the flow sensor briefly read zero" events during an otherwise active
    event — a signature of paddlewheel direction reversal during supply
    pulsation.
    """
    count = 0
    in_trough = False
    for f in flow_readings:
        below = f is not None and f < threshold
        if below and not in_trough:
            count += 1
            in_trough = True
        elif not below:
            in_trough = False
    return count


def _detect_degraded_supply(
    pressure_readings: List[float],
    flow_readings: List[float],
    pre_event_pressure_psi: float,
    resistance_shape: str,
    duration_s: float,
):
    """Detect supply-driven pulsation in a captured event.

    Sample-rate aware. Adapts the search range to event duration so short
    events with fast pulses can still be detected.

    Returns ``(is_degraded: bool, diagnostic: dict)``. The diagnostic dict
    always contains a ``reason`` key whose value is one of the canonical
    reason strings; degraded events have ``reason == "pulsing_supply_confirmed"``.
    """
    diag = {}

    # Full-precision sanitize at entry. The detector does autocorrelation
    # and std-dev on these; 3-dp rounding would suppress small periodic
    # signals.
    pressure_readings = _finite_float_series(pressure_readings)
    flow_readings = _finite_float_series(flow_readings)

    # (A) Minimum duration: ≥ 2 cycles of the FASTEST detectable pulse.
    if duration_s < 2 * SUPPLY_PULSE_PERIOD_MIN_S:
        return False, {"reason": "too_short", "duration_s": duration_s}
    if not pressure_readings or pre_event_pressure_psi <= 0:
        return False, {"reason": "no_pressure_baseline"}

    # (B) Adapt searchable max period to event duration. A 6 s pulse can't
    # be detected in an 8 s event (< 2 cycles), but a 2 s pulse can be.
    search_max_period_s = min(
        SUPPLY_PULSE_PERIOD_MAX_S,
        duration_s / MIN_CYCLES_FOR_DETECTION,
    )
    if search_max_period_s < SUPPLY_PULSE_PERIOD_MIN_S:
        return False, {
            "reason": "too_short_for_period_search",
            "duration_s": duration_s,
        }

    pressure_rate_hz = len(pressure_readings) / duration_s
    diag["pressure_rate_hz"] = round(pressure_rate_hz, 2)

    # (C) Middle 70% slice — drop ramp-up and recovery.
    lo = int(len(pressure_readings) * 0.15)
    hi = int(len(pressure_readings) * 0.85)
    mid = pressure_readings[lo:hi]
    # Refine search_max to what the mid slice can actually support.
    # _dominant_period_s caps lag at len(values)//2 - 1; that means the
    # largest detectable period from `mid` is (len(mid)//2) / pressure_rate_hz.
    mid_max_period_s = (len(mid) // 2 - 1) / pressure_rate_hz if pressure_rate_hz > 0 else 0
    search_max_period_s = min(search_max_period_s, mid_max_period_s)
    diag["search_max_period_s"] = round(search_max_period_s, 2)
    if search_max_period_s < SUPPLY_PULSE_PERIOD_MIN_S or len(mid) < 8:
        return False, {
            "reason": "mid_slice_too_short",
            "mid_samples": len(mid),
            "search_max_period_s": round(search_max_period_s, 2),
        }

    # Detrend BEFORE std and autocorrelation — kills slow recovery drift
    # that would otherwise inflate variance and create false periodicity
    # at long lags.
    mid = _detrend_linear(mid)
    mid_std = statistics.pstdev(mid)
    diag["mid_pressure_std_psi"] = round(mid_std, 3)

    # (D) Primary gate — pressure must actually be moving.
    if mid_std < MID_PRESSURE_STD_PULSING_PSI:
        diag["reason"] = "pressure_steady"
        return False, diag

    # (E) Dominant pressure period in the supply-pulse band.
    pressure_period_s, pressure_score = _dominant_period_s(
        mid, pressure_rate_hz,
        min_period_s=SUPPLY_PULSE_PERIOD_MIN_S,
        max_period_s=search_max_period_s,
    )
    diag["pressure_dominant_period_s"] = (
        round(pressure_period_s, 2) if pressure_period_s else None
    )
    diag["pressure_autocorr_score"] = round(pressure_score, 3)
    if not pressure_period_s:
        diag["reason"] = "no_periodic_pressure_in_supply_band"
        return False, diag

    # (F) Flow period match — flow's dominant period must match pressure's
    # (within ±25%, or at the 1:2 / 2:1 harmonic that arises from
    # paddlewheel rectification doubling apparent flow frequency).
    period_matched = False
    flow_period_s = None
    flow_score = 0.0
    if flow_readings and len(flow_readings) >= 8:
        flow_rate_hz = len(flow_readings) / duration_s
        flow_period_s, flow_score = _dominant_period_s(
            _detrend_linear(flow_readings), flow_rate_hz,
            min_period_s=SUPPLY_PULSE_PERIOD_MIN_S,
            max_period_s=FIXTURE_CYCLE_PERIOD_MAX_S,
        )
        diag["flow_dominant_period_s"] = (
            round(flow_period_s, 2) if flow_period_s else None
        )
        diag["flow_autocorr_score"] = round(flow_score, 3)
        if flow_period_s:
            ratio = flow_period_s / pressure_period_s
            diag["period_match_ratio"] = round(ratio, 2)
            if 0.75 <= ratio <= 1.33:
                period_matched = True
                diag["harmonic"] = "1:1"
            elif 0.45 <= ratio <= 0.55:
                period_matched = True
                diag["harmonic"] = "1:2"   # flow at half pressure period
            elif 1.8 <= ratio <= 2.2:
                period_matched = True
                diag["harmonic"] = "2:1"   # flow at double pressure period
            else:
                diag["reason"] = "frequency_mismatch_fixture_cycling"
                return False, diag
        # If flow has no clean dominant period, fall through; the pressure
        # band already showed periodicity. period_matched stays False so
        # the appliance fallback can run.

    # (G) Appliance fallback discriminator — runs ONLY when period match
    # was inconclusive. Real degraded events have very high
    # flow_rel_std / pressure_rel_std ratios, so this MUST NOT run after
    # a confirmed period match.
    if not period_matched:
        avg_flow = (sum(flow_readings) / len(flow_readings)
                    if flow_readings else 0)
        if avg_flow > 0.5 and len(flow_readings) >= 4:
            flow_std_full = statistics.pstdev(flow_readings)
            flow_rel = flow_std_full / avg_flow
            pressure_rel = mid_std / pre_event_pressure_psi
            if pressure_rel > 0 and (flow_rel / pressure_rel) > APPLIANCE_FLOW_PRESSURE_RATIO:
                diag["reason"] = "appliance_cycling"
                diag["flow_rel_std"] = round(flow_rel, 3)
                diag["pressure_rel_std"] = round(pressure_rel, 4)
                return False, diag

    # (H) Confirmatory signals. The final decision is delegated to
    # _evaluate_degraded_from_diag so the reprocess endpoint applies the
    # exact same gates to stored diagnostics.
    trough_count = _count_trough_episodes(flow_readings, FLOW_TROUGH_LPM)
    trough_rate = trough_count / duration_s if duration_s > 0 else 0
    diag["flow_trough_episode_count"] = trough_count
    diag["flow_trough_episode_rate_hz"] = round(trough_rate, 3)
    diag["resistance_shape"] = resistance_shape
    diag["period_matched"] = period_matched

    is_degraded, reason = _evaluate_degraded_from_diag(diag)
    diag["reason"] = reason
    return is_degraded, diag


def _evaluate_degraded_from_diag(diag: dict):
    """Apply the post-detection threshold gates to a diagnostic dict.

    Pure function over the stored diagnostic fields. Used by both the live
    detector (so the gates exist in one place) and the reprocess endpoint
    (so threshold changes apply retroactively to events with stored
    diagnostics, without needing the raw sample series).

    Diagnostics produced by early-exit gates (`pressure_steady`,
    `frequency_mismatch_fixture_cycling`, `appliance_cycling`,
    `too_short`, `no_pressure_baseline`, `mid_slice_too_short`,
    `too_short_for_period_search`, `no_periodic_pressure_in_supply_band`)
    do not have `flow_trough_episode_rate_hz` set — for those, this
    helper preserves the stored reason and returns is_degraded=False.
    Re-evaluating those would require the raw series, which is not
    persisted post-event.
    """
    if "flow_trough_episode_rate_hz" not in diag:
        return False, diag.get("reason", "unknown")

    period_matched = bool(diag.get("period_matched", False))
    flow_score = float(diag.get("flow_autocorr_score") or 0.0)
    trough_rate = float(diag.get("flow_trough_episode_rate_hz") or 0.0)
    resistance_shape = diag.get("resistance_shape") or ""

    if period_matched:
        # Require BOTH a meaningful flow autocorr score AND a meaningful
        # trough rate. A high flow_score alone (as seen in the 16:48 /
        # 19:48 / 20:20 false positives) can arise from low-amplitude
        # noise patterning to ~1 Hz at the 7 Hz sample rate; requiring
        # paddlewheel rectification evidence rules those out.
        if (flow_score < PRESSURE_AUTOCORR_THRESHOLD
                or trough_rate < MIN_TROUGH_EPISODE_RATE_HZ):
            return False, "insufficient_confirmatory_signal"
    else:
        confirmed = (
            trough_rate >= MIN_TROUGH_EPISODE_RATE_HZ
            or resistance_shape == "pulsed"
        )
        if not confirmed:
            return False, "no_confirmatory_signal"

    return True, "pulsing_supply_confirmed"


def reprocess_degraded_supply_verdicts(conn: sqlite3.Connection) -> dict:
    """Re-apply current degraded-supply gates to all events with stored diagnostics.

    Walks every event that has `degraded_diagnostic_json`, re-evaluates via
    `_evaluate_degraded_from_diag`, and updates the event row when the
    verdict changes. The raw sample series are not retained post-event, so
    only the post-detection gate logic can change retroactively — early
    rejections like 'pressure_steady' stay as they were (the helper
    preserves their reason). Events with no stored diag (pre-deploy rows)
    are counted as `skipped_legacy`.

    Volume bookkeeping is kept in sync: events flipping to degraded swap
    `volume_litres_effective` to the envelope estimate; events flipping
    back to clean revert to raw `volume_litres`. The hourly_volume bucket
    is adjusted by the delta so daily totals stay correct.

    Returns a summary dict with the counts the endpoint relays to the UI.
    """
    from .database import _hour_bucket_for, transaction

    rows = conn.execute(
        "SELECT id, circuit, start_ts, degraded_supply, "
        "       degraded_diagnostic_json, volume_litres, "
        "       volume_litres_estimated, hourly_volume_applied_litres, "
        "       hourly_volume_applied_bucket, is_composite "
        "FROM events "
        "WHERE degraded_diagnostic_json IS NOT NULL "
        "  AND degraded_diagnostic_json != ''"
    ).fetchall()

    skipped_legacy_row = conn.execute(
        "SELECT COUNT(*) AS c FROM events "
        "WHERE degraded_diagnostic_json IS NULL "
        "   OR degraded_diagnostic_json = ''"
    ).fetchone()
    skipped_legacy = int(skipped_legacy_row["c"]) if skipped_legacy_row else 0

    flipped_to_degraded = 0
    flipped_to_clean = 0
    evaluated = 0

    for row in rows:
        evaluated += 1
        try:
            diag = json.loads(row["degraded_diagnostic_json"])
        except (ValueError, TypeError):
            log.warning("reprocess: event %s has unparseable diag JSON; skipping",
                        row["id"])
            continue

        new_is_degraded, new_reason = _evaluate_degraded_from_diag(diag)
        old_is_degraded = bool(row["degraded_supply"])
        if new_is_degraded == old_is_degraded:
            continue  # verdict unchanged

        # Verdict flipped — update event + hourly_volume in one transaction.
        diag["reason"] = new_reason
        raw_volume = float(row["volume_litres"] or 0.0)
        envelope_volume = float(row["volume_litres_estimated"] or 0.0)
        new_effective = envelope_volume if new_is_degraded else raw_volume
        new_method = "pulsing_supply_envelope" if new_is_degraded else "raw"

        # match_rejection_reason: 'pulsing_supply' only when flipped TO
        # degraded AND not composite. Flipping AWAY clears the upstream
        # reason; cluster-engine reasons live in a separate field and
        # are not touched here.
        if new_is_degraded and not row["is_composite"]:
            new_rejection = "pulsing_supply"
        else:
            new_rejection = None

        prev_applied = float(row["hourly_volume_applied_litres"] or 0.0)
        prev_bucket = row["hourly_volume_applied_bucket"]
        new_bucket = _hour_bucket_for(row["start_ts"])

        # excluded_from_training mirrors (composite OR degraded). Composite
        # status doesn't change here, so we OR the new degraded verdict in.
        with transaction(conn):
            conn.execute(
                "UPDATE events SET "
                "  degraded_supply = ?, "
                "  volume_litres_effective = ?, "
                "  volume_estimation_method = ?, "
                "  degraded_diagnostic_json = ?, "
                "  match_rejection_reason = ?, "
                "  excluded_from_training = CASE "
                "    WHEN is_composite = 1 OR ? = 1 THEN 1 ELSE 0 END "
                "WHERE id = ?",
                (
                    1 if new_is_degraded else 0,
                    round(new_effective, 3),
                    new_method,
                    json.dumps(diag, allow_nan=False),
                    new_rejection,
                    1 if new_is_degraded else 0,
                    row["id"],
                ),
            )
            if prev_bucket and prev_applied != 0:
                conn.execute(
                    "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT (circuit, hour_ts) "
                    "DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres",
                    (row["circuit"], prev_bucket, -prev_applied),
                )
            if new_bucket and new_effective:
                conn.execute(
                    "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT (circuit, hour_ts) "
                    "DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres",
                    (row["circuit"], new_bucket, new_effective),
                )
            conn.execute(
                "UPDATE events SET "
                "  hourly_volume_applied_litres = ?, "
                "  hourly_volume_applied_bucket = ? "
                "WHERE id = ?",
                (new_effective, new_bucket, row["id"]),
            )

        if new_is_degraded:
            flipped_to_degraded += 1
        else:
            flipped_to_clean += 1
        log.info(
            "reprocess: event %s flipped %s → %s (reason=%s, effective_vol %.3f → %.3f)",
            row["id"],
            "clean" if old_is_degraded is False else "degraded",
            "degraded" if new_is_degraded else "clean",
            new_reason,
            prev_applied,
            new_effective,
        )

    return {
        "evaluated": evaluated,
        "flipped_to_degraded": flipped_to_degraded,
        "flipped_to_clean": flipped_to_clean,
        "skipped_legacy": skipped_legacy,
    }


def _estimate_volume_smoothed(
    flow_readings: List[float],
    duration_s: float,
) -> float:
    """Spike-resistant smoothed volume estimate for degraded events.

    Strategy: cap a rolling-max envelope at the 95th percentile of positive
    samples (rejects single-sample spikes), then take the MEDIAN across
    windows (rejects sub-event high outliers). The result is a "typical
    sustained flow" estimate that's robust to both the artefact zero-troughs
    AND any phantom-pulse spikes that paddlewheel rectification might
    introduce.

    For very short events with too few samples to envelope-smooth, fall
    back to a plain mean times duration.
    """
    if not flow_readings or duration_s <= 0:
        return 0.0
    cleaned = [f for f in _finite_float_series(flow_readings) if f >= 0]
    if not cleaned:
        return 0.0
    if len(cleaned) < 5 or duration_s < 5:
        avg = sum(cleaned) / len(cleaned)
        return max(0.0, avg * (duration_s / 60.0))

    positive = sorted(f for f in cleaned if f > FLOW_TROUGH_LPM)
    if not positive:
        return 0.0
    cap_idx = max(0, int(len(positive) * VOLUME_ENVELOPE_PERCENTILE) - 1)
    cap = positive[cap_idx]

    sample_rate_hz = len(cleaned) / duration_s
    win = max(2, int(round(VOLUME_ENVELOPE_WINDOW_S * sample_rate_hz)))
    windowed = []
    for i in range(0, len(cleaned), win):
        chunk = cleaned[i:i + win]
        if chunk:
            windowed.append(min(max(chunk), cap))
    if not windowed:
        return 0.0
    windowed.sort()
    effective_flow = windowed[len(windowed) // 2]   # median of window-maxes
    return max(0.0, effective_flow * (duration_s / 60.0))


def _bin_min_max(values: list, n_bins: int):
    """Bin `values` into up to n_bins, returning (mins, maxs) per bin.

    Each output bin contains the MIN and MAX of its slice — preserves
    oscillation envelopes that bin-mean would hide. Bin slicing uses
    round-of-fraction indexing so the output length is bounded by n_bins
    (never exceeds, no integer-division off-by-one).
    """
    cleaned = _clean_numeric_series(values)
    if not cleaned:
        return [], []
    if len(cleaned) <= n_bins:
        return list(cleaned), list(cleaned)
    mins, maxs = [], []
    L = len(cleaned)
    for b in range(n_bins):
        start = round(b * L / n_bins)
        end   = round((b + 1) * L / n_bins)
        if end <= start:
            end = start + 1
        chunk = cleaned[start:end]
        if chunk:
            mins.append(round(min(chunk), 3))
            maxs.append(round(max(chunk), 3))
    assert len(mins) <= n_bins and len(maxs) <= n_bins
    return mins, maxs


def _persist_waveform(
    db,
    event_id: str,
    flow_readings: list,
    pressure_readings: list,
    duration_s: float,
) -> None:
    """Write a min/max-binned waveform to event_waveforms.

    Used for the high-resolution waveform display in the event detail modal.
    The 32-point pressure_signature_json / flow_signature_json on the events
    row stays for clustering — these min/max envelopes are display-only.
    Skips silently if both reading lists are empty (historical events).
    """
    flow_min, flow_max = _bin_min_max(flow_readings, MAX_WAVEFORM_BINS)
    pres_min, pres_max = _bin_min_max(pressure_readings, MAX_WAVEFORM_BINS)
    if not flow_min and not pres_min:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        db.execute(
            "INSERT OR REPLACE INTO event_waveforms "
            "(event_id, flow_min_json, flow_max_json, "
            " pressure_min_json, pressure_max_json, "
            " duration_seconds, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                json.dumps(flow_min, allow_nan=False),
                json.dumps(flow_max, allow_nan=False),
                json.dumps(pres_min, allow_nan=False),
                json.dumps(pres_max, allow_nan=False),
                float(duration_s),
                now_iso,
            ),
        )
    except Exception as e:
        log.warning("event_waveforms insert failed for %s: %s", event_id, e)


def _flow_signature(flow_readings: list, peak: float, n: int = 32) -> list:
    """Resample flow_readings to n points, normalize by peak (0–1)."""
    if not flow_readings or peak <= 0:
        return [0.0] * n
    src = flow_readings
    if len(src) == 1:
        return [min(src[0] / peak, 1.0)] * n
    result = []
    for i in range(n):
        pos = i * (len(src) - 1) / (n - 1)
        lo, hi = int(pos), min(int(pos) + 1, len(src) - 1)
        v = src[lo] * (1 - (pos - lo)) + src[hi] * (pos - lo)
        result.append(round(min(v / peak, 1.0), 4))
    return result


def _pressure_signature(
    pressure_readings: list,
    pre_event_pressure_psi: float,
    pressure_delta_psi: float,
    n: int = 32,
) -> list:
    """32-point normalized pressure drop (0 = no drop, 1 = full drop at delta_psi)."""
    if pressure_delta_psi <= 0:
        return [0.0] * n
    drops = []
    for p in pressure_readings:
        try:
            v = float(p)
        except (TypeError, ValueError):
            continue
        drops.append(max(0.0, min(1.0, (pre_event_pressure_psi - v) / pressure_delta_psi)))
    if not drops:
        return [0.0] * n
    if len(drops) == 1:
        return [drops[0]] * n
    result = []
    for i in range(n):
        pos = i * (len(drops) - 1) / (n - 1)
        lo, hi = int(pos), min(int(pos) + 1, len(drops) - 1)
        v = drops[lo] * (1 - (pos - lo)) + drops[hi] * (pos - lo)
        result.append(round(v, 4))
    return result


def _flow_edges(flow_readings: list, peak: float) -> tuple:
    """Count significant direction reversals using zigzag (WaterSense model).

    Prepends 0.0 so the valve-open onset step is visible.
    Fires an edge when cumulative displacement from the last extreme exceeds
    the threshold, so gradual ramps count the same as abrupt steps.
    """
    if len(flow_readings) < 3:
        return 0, 0
    threshold = max(0.3, 0.15 * peak)
    padded = [0.0] + list(flow_readings)
    n = len(padded)
    smoothed = [
        sum(padded[max(0, i - 1): min(n, i + 2)])
        / len(padded[max(0, i - 1): min(n, i + 2)])
        for i in range(n)
    ]
    pos = neg = 0
    last_extreme = smoothed[0]
    direction = None
    for val in smoothed[1:]:
        change = val - last_extreme
        if change >= threshold:
            if direction != 'up':
                pos += 1
                direction = 'up'
            last_extreme = val
        elif change <= -threshold:
            if direction != 'down':
                neg += 1
                direction = 'down'
            last_extreme = val
        else:
            if direction == 'up' and val > last_extreme:
                last_extreme = val
            elif direction == 'down' and val < last_extreme:
                last_extreme = val
    return pos, neg


def _mid_event_flow_drop(flow_readings: list, peak: float) -> float:
    """Largest flow drop that does not terminate the event.

    A 'non-terminal' drop is one where flow remains above 20% of peak after
    the drop — signalling one fixture turning off while another keeps running.
    Returns 0.0 for single-fixture events.
    """
    n = len(flow_readings)
    if n < 3 or peak <= 0:
        return 0.0
    floor = 0.20 * peak
    max_drop = 0.0
    for i in range(1, n):
        drop = flow_readings[i - 1] - flow_readings[i]
        if drop > 0 and flow_readings[i] >= floor:
            max_drop = max(max_drop, drop)
    return round(max_drop, 4)


def _flow_steady_state(flow_readings: list) -> float:
    """Fraction of event time within ±20% of the median flow (0.0–1.0).

    High for steady showers; low for toilet fill curves and pulsed appliances.
    """
    n = len(flow_readings)
    if n < 3:
        return 0.0
    sorted_vals = sorted(flow_readings)
    median = sorted_vals[n // 2]
    if median <= 0:
        return 0.0
    threshold = 0.20 * median
    steady = sum(1 for v in flow_readings if abs(v - median) <= threshold)
    return round(steady / n, 4)


def _pressure_transient_stats(
    pressure_readings: list, pre_event_psi: float, pressure_delta_psi: float
) -> dict:
    """Compute energy and duration of the opening pressure transient.

    pressure_readings is at 40 Hz (25 ms/sample). Returns zeros for
    flow-only events where pressure_readings is empty or no transient occurred.
    """
    if not pressure_readings or pressure_delta_psi <= 0:
        return {'pressure_transient_energy': 0.0, 'pressure_transient_duration_ms': 0.0}
    threshold = 0.10 * pressure_delta_psi
    energy = sum((p - pre_event_psi) ** 2 for p in pressure_readings)
    duration_samples = sum(
        1 for p in pressure_readings if abs(p - pre_event_psi) >= threshold
    )
    return {
        'pressure_transient_energy':     round(energy, 4),
        'pressure_transient_duration_ms': round(duration_samples * 25.0, 1),
    }


def _pressure_shape_features(
    pressure_readings: list, pre_event_psi: float, pressure_delta_psi: float
) -> dict:
    """Transient shape features from the 40 Hz pressure curve.

    pressure_onset_ms        — index of minimum * 25 ms (time to peak drop)
    recovery_overshoot_psi   — max pressure above baseline after the minimum
    pressure_oscillation_count — zero-crossings of (p - pre_event_psi) post-min
    """
    zero = {
        'pressure_onset_ms': 0.0,
        'recovery_overshoot_psi': 0.0,
        'pressure_oscillation_count': 0,
    }
    if not pressure_readings or pressure_delta_psi <= 0:
        return zero

    min_idx = min(range(len(pressure_readings)), key=lambda i: pressure_readings[i])
    onset_ms = round(min_idx * 25.0, 1)

    post_min = pressure_readings[min_idx:]
    overshoot = round(max(0.0, max(post_min) - pre_event_psi), 3)

    deviations = [p - pre_event_psi for p in post_min]
    crossings = sum(
        1 for i in range(1, len(deviations))
        if deviations[i - 1] * deviations[i] < 0
    )

    return {
        'pressure_onset_ms':          onset_ms,
        'recovery_overshoot_psi':     overshoot,
        'pressure_oscillation_count': crossings,
    }


def _flow_dynamics(flow_readings: list, peak: float) -> dict:
    """Rise/fall rates, opening/closing step magnitudes, and 90% ramp times.

    Assumes uniform 1 Hz sampling (1 index = 1 second). For events > 120s the
    event_detector downsamples to 0.2 Hz so timing values are approximate for
    long irrigation runs — acceptable since those are identified by volume/duration.
    """
    zero = {
        'flow_rise_rate_lpm_s': 0.0, 'flow_fall_rate_lpm_s': 0.0,
        'opening_step_lpm': 0.0,     'closing_step_lpm': 0.0,
        'time_to_90pct_flow_seconds': 0.0,
        'time_from_90pct_to_zero_seconds': 0.0,
    }
    n = len(flow_readings)
    if n < 2 or peak <= 0:
        return zero

    peak_idx = max(range(n), key=lambda i: flow_readings[i])
    rise_rate = peak / max(peak_idx, 1)
    fall_rate = peak / max(n - 1 - peak_idx, 1)

    deltas = [flow_readings[i] - flow_readings[i - 1] for i in range(1, n)]
    opening_step = max((d for d in deltas if d > 0), default=0.0)
    closing_step = max((-d for d in deltas if d < 0), default=0.0)

    threshold_90 = 0.9 * peak
    t_rise = next((i for i, v in enumerate(flow_readings) if v >= threshold_90), n - 1)
    t_fall_rev = next(
        (i for i, v in enumerate(reversed(flow_readings)) if v >= threshold_90), 0
    )

    return {
        'flow_rise_rate_lpm_s':            round(rise_rate, 4),
        'flow_fall_rate_lpm_s':            round(fall_rate, 4),
        'opening_step_lpm':                round(opening_step, 4),
        'closing_step_lpm':                round(closing_step, 4),
        'time_to_90pct_flow_seconds':      float(t_rise),
        'time_from_90pct_to_zero_seconds': float(t_fall_rev),
    }


# ─────────────────────────────────────────────────────────────────────────── #
# ESP waveform enrichment (firmware 3.7.0+) — per-group feature routing       #
# ─────────────────────────────────────────────────────────────────────────── #

# Minimum correlation overlap score required to treat a WaveformRecord as
# matching a given RawEvent. Duration-match below this threshold → legacy path.
_WF_MATCH_MIN_SCORE: float = 0.55

# Maximum seconds between the waveform record's assembled timestamp and the
# current processing moment. Guards against stale records from a previous event.
_WF_MATCH_WINDOW_S: float = 90.0

# Waveform flag bits (must match firmware wire format).
_WF_FL_START_COMPLETE:     int = 0x01  # pre-roll covers full start-window span
_WF_FL_FULL_COMPLETE:      int = 0x02  # full-window capture is complete
_WF_FL_RESOLUTION_REDUCED: int = 0x04  # buffer decimated; lower sample rate
_WF_FL_EVENT_TOO_SHORT:    int = 0x10  # firmware duration < 1 s
_WF_FL_EVENT_TOO_LONG:     int = 0x20  # decimation factor >= 4×
_WF_FL_CLAMPED_SAMPLE:     int = 0x40  # at least one sample hit ADC rail

_WF_FLOW_SIG_MIN_PEAK_LPM:   float = 0.05   # ignore near-zero / noisy full_flow arrays
_WF_PRESS_SIG_MIN_DELTA_PSI:  float = 0.15   # ignore pressure noise below this drop


def _wf_millis_sub(a: int, b: int) -> int:
    """Wrap-safe uint32 millis subtraction: (a - b) mod 2**32."""
    return int((a - b) & 0xFFFFFFFF)


def _wf_resample(points: List[float], n: int) -> List[float]:
    """Linearly resample ``points`` to exactly ``n`` output points."""
    src = points
    m = len(src)
    if m == 0:
        return [0.0] * n
    if m == 1:
        return [src[0]] * n
    result = []
    for i in range(n):
        pos = i * (m - 1) / (n - 1)
        lo, hi = int(pos), min(int(pos) + 1, m - 1)
        frac = pos - lo
        result.append(src[lo] * (1.0 - frac) + src[hi] * frac)
    return result


def _wf_overlap_score(event: RawEvent, record: WaveformRecord) -> float:
    """
    Duration-based overlap score for correlating a RawEvent to a WaveformRecord.

    Returns a value in [0, 1]: 1.0 = exact duration match, 0.0 = no overlap.
    Uses wrap-safe millis arithmetic for the firmware-side duration.
    """
    if event.end_ts is None or event.start_ts is None:
        return 0.0
    event_dur_ms = max(0.0, (event.end_ts - event.start_ts).total_seconds() * 1000)
    meta = record.metadata
    # Include the tail window: end_ms marks when flow first drops (phase 1→2),
    # after which the firmware waits tail_ms before finalising.  The software
    # event end_ts includes a similar debounce, so comparing full spans is more
    # accurate than using end_ms - start_ms alone.
    fw_dur_ms = float(_wf_millis_sub(meta.end_ms, meta.start_ms)) + meta.tail_ms
    denom = max(event_dur_ms, fw_dur_ms)
    if denom <= 0:
        return 0.0
    return max(0.0, min(1.0, 1.0 - abs(event_dur_ms - fw_dur_ms) / denom))


def _enrich_from_waveform(
    features: Dict[str, Any],
    record: WaveformRecord,
    overlap_score: float,
) -> None:
    """
    Selectively override features in ``features`` with ESP waveform data.

    Each feature group is routed independently — a missing or low-quality
    window falls back to the already-computed legacy value; no all-or-nothing.

    Mutates ``features`` in place and sets the four waveform A/B fields.
    """
    import time as _time

    meta = record.metadata
    fl   = meta.flags
    any_wf_used = False

    # ── 1. Metadata-sourced features (always, no flag guard needed) ────────
    # The metadata is always valid when we reach this point; replace features
    # that are better measured at 200 Hz / by the firmware's accumulator.
    if meta.peak_flow > 0:
        features["peak_flow_lpm"] = round(meta.peak_flow, 3)
        any_wf_used = True
    if meta.pressure_delta >= 0:
        features["pressure_delta_psi"] = round(meta.pressure_delta, 2)
        any_wf_used = True
    # Propagation delay — firmware measures at ~50 Hz (ISR resolution);
    # -1 means the firmware did not detect a clear onset, keep legacy value.
    if meta.propagation_delay_ms >= 0:
        features["propagation_delay_ms"] = float(meta.propagation_delay_ms)
        any_wf_used = True

    # ── 2. Start-window features (flag bit 0x01 — start_waveform_complete) ─
    if fl & _WF_FL_START_COMPLETE:
        sn = meta.start_points
        dt_start_s = (meta.pre_ms + meta.post_ms) / 1000.0 / sn  # seconds/sample

        # 2a. Start-flow waveform → opening dynamics
        sf = record.start_flow   # L/min, sn points
        if sf and max(sf) > 0:
            peak_wf = max(sf)
            # Onset index: approximately where the pre-roll ends
            onset_idx = round(meta.pre_ms / 1000.0 / dt_start_s)
            onset_idx = min(onset_idx, sn - 1)
            ramp = sf[onset_idx:]
            if ramp:
                # Rise rate from onset to peak
                peak_idx_ramp = max(range(len(ramp)), key=lambda i: ramp[i])
                if peak_idx_ramp > 0:
                    features["flow_rise_rate_lpm_s"] = round(
                        peak_wf / (peak_idx_ramp * dt_start_s), 4)
                # Time to 90% of peak
                t90 = next((i for i, v in enumerate(ramp) if v >= 0.9 * peak_wf), None)
                if t90 is not None:
                    features["time_to_90pct_flow_seconds"] = round(t90 * dt_start_s, 2)
                # Opening step — largest single-sample rise in the ramp
                if len(ramp) >= 2:
                    features["opening_step_lpm"] = round(
                        max((ramp[i] - ramp[i - 1]
                             for i in range(1, len(ramp))
                             if ramp[i] > ramp[i - 1]),
                            default=0.0), 4)
            any_wf_used = True

        # 2b. Start-pressure waveform → onset timing (time from window start to min)
        sp = record.start_pressure   # PSI, sn points
        if sp:
            min_idx = min(range(len(sp)), key=lambda i: sp[i])
            # Onset relative to the start of the window (which begins pre_ms before onset)
            # So the true pressure_onset_ms is time from onset = (min_idx * dt - pre_ms/1000) * 1000
            onset_ms_wf = round((min_idx * dt_start_s * 1000) - meta.pre_ms, 1)
            # Keep non-negative (a negative value means the onset is before the window pre-roll)
            features["pressure_onset_ms"] = max(0.0, onset_ms_wf)
            any_wf_used = True

    # ── 3. Full-window features (flag bit 0x02 — full_waveform_complete) ───
    if fl & _WF_FL_FULL_COMPLETE:
        fw_span_ms = float(_wf_millis_sub(meta.end_ms, meta.start_ms)) + meta.tail_ms
        dt_full_s  = fw_span_ms / 1000.0 / meta.full_points  # seconds/sample

        # 3a. Full-flow waveform → steady-state fraction and variability
        ff = record.full_flow   # L/min, fn points
        if ff:
            n_full = len(ff)
            # Exclude tail samples from steady-state calculation
            tail_pts = round(meta.tail_ms / 1000.0 / dt_full_s) if dt_full_s > 0 else 0
            body = ff[:max(1, n_full - tail_pts)]
            if len(body) >= 3:
                sorted_body = sorted(body)
                med = sorted_body[len(body) // 2]
                if med > 0:
                    thr = 0.20 * med
                    features["steady_state_fraction"] = round(
                        sum(1 for v in body if abs(v - med) <= thr) / len(body), 4)
            if len(ff) >= 2:
                features["flow_variability"] = round(_safe_std(ff), 4)
            any_wf_used = True

        # 3b. Full-pressure waveform → recovery overshoot (from the tail)
        fp = record.full_pressure   # PSI, fn points
        if fp and meta.full_points > 0:
            # Tail starts at the event-end sample
            tail_pts = round(meta.tail_ms / 1000.0 / dt_full_s) if dt_full_s > 0 else 0
            n_full = len(fp)
            body_pts = n_full - tail_pts
            if tail_pts > 0 and body_pts > 0:
                body_press = fp[:body_pts]
                tail_press = fp[body_pts:]
                if body_press and tail_press:
                    baseline = sum(body_press) / len(body_press)
                    overshoot = max(0.0, max(tail_press) - baseline)
                    features["recovery_overshoot_psi"] = round(overshoot, 3)
            any_wf_used = True

    # ── 3b. Shape signatures — firmware arrays are time-aligned, flow starts near zero ──
    # Use separate flags so signature_source reflects exactly what was overridden.
    _flow_sig_overridden  = False
    _press_sig_overridden = False

    # Flow: full_flow is in L/min; guard against zero/noise arrays before overriding.
    if record.full_flow:
        peak_fw = max(record.full_flow)
        if peak_fw >= _WF_FLOW_SIG_MIN_PEAK_LPM:
            features["flow_signature_json"] = json.dumps(
                _flow_signature(record.full_flow, peak_fw)
            )
            _flow_sig_overridden = True
            any_wf_used = True

    # Pressure: derive baseline from pre-roll samples (pressure before flow onset).
    # record.start_pressure and record.full_pressure both use meta.pressure_scale
    # and belong to the same WaveformRecord — units are identical (PSI).
    if record.full_pressure:
        baseline_psi: Optional[float] = None

        # Priority 1: median of pre-roll samples from start_pressure (most accurate —
        # firmware ISR-level capture before flow onset).
        if (fl & _WF_FL_START_COMPLETE) and record.start_pressure \
                and meta.start_points > 0 \
                and (meta.pre_ms + meta.post_ms) > 0 \
                and meta.pre_ms > 0:
            onset_idx = round(
                meta.pre_ms * meta.start_points / (meta.pre_ms + meta.post_ms)
            )
            onset_idx = max(0, min(onset_idx, len(record.start_pressure) - 1))
            pre_roll = record.start_pressure[:onset_idx]
            if len(pre_roll) >= 3:
                sorted_pr = sorted(pre_roll)
                baseline_psi = sorted_pr[len(pre_roll) // 2]  # median

        # Priority 2: median of first few full_pressure samples (still firmware data,
        # but may already include partial onset drop).
        if baseline_psi is None and len(record.full_pressure) >= 3:
            pre_fp = record.full_pressure[:min(5, len(record.full_pressure))]
            sorted_fp = sorted(pre_fp)
            baseline_psi = sorted_fp[len(pre_fp) // 2]

        # Priority 3: software-measured pre-event baseline (different time base,
        # but better than nothing).
        if baseline_psi is None:
            baseline_psi = float(features.get("pre_event_pressure_psi") or 0.0)

        if baseline_psi > 0:
            delta_psi = baseline_psi - min(record.full_pressure)
            if delta_psi >= _WF_PRESS_SIG_MIN_DELTA_PSI:
                features["pressure_signature_json"] = json.dumps(
                    _pressure_signature(record.full_pressure, baseline_psi, delta_psi)
                )
                _press_sig_overridden = True
                any_wf_used = True

    # Set granular signature_source — reflects exactly what was overridden.
    if _flow_sig_overridden and _press_sig_overridden:
        features["signature_source"] = "esp_full_flow_pressure"
    elif _flow_sig_overridden:
        features["signature_source"] = "esp_full_flow"
    elif _press_sig_overridden:
        features["signature_source"] = "esp_full_pressure"
    # else: stays "software" (set in extract_features default)

    # ── 4. Set A/B tracking fields ─────────────────────────────────────────
    features["esp_waveform_used"]     = 1 if any_wf_used else 0
    features["waveform_event_id"]     = meta.event_id
    features["waveform_quality"]      = meta.quality
    features["waveform_overlap_score"] = round(overlap_score, 4)


def extract_features(event: RawEvent) -> Dict[str, Any]:
    """Compute the full feature vector from a RawEvent."""
    duration = 0.0
    if event.end_ts and event.start_ts:
        duration = (event.end_ts - event.start_ts).total_seconds()

    avg_flow = _safe_float(event.flow_readings)
    peak_flow = max(event.flow_readings) if event.flow_readings else 0.0
    flow_variability = _safe_std(event.flow_readings)

    # Clamp pressure_delta_psi ≥ 0: negative values (pressure rose during event)
    # are surge artefacts; the live detector now rejects them, but historical
    # importer events may still arrive with negative delta.
    pressure_delta_psi = max(0.0, float(event.pressure_delta_psi or 0))
    # pre_event_pressure_psi is None for cold-start flow events with no
    # trustworthy baseline — coerce to 0 so pressure-derived features (all
    # gated on a positive pressure_delta_psi) degrade gracefully.
    pre_event_pressure = float(event.pre_event_pressure_psi or 0)

    sig          = _flow_signature(event.flow_readings, peak_flow)
    p_sig        = _pressure_signature(
        event.pressure_readings or [],
        pre_event_pressure,
        pressure_delta_psi,
    )
    pos_edges, neg_edges = _flow_edges(event.flow_readings, peak_flow)
    dynamics     = _flow_dynamics(event.flow_readings, peak_flow)
    mid_drop     = _mid_event_flow_drop(event.flow_readings, peak_flow)
    steady       = _flow_steady_state(event.flow_readings)
    p_stats      = _pressure_transient_stats(
        event.pressure_readings, pre_event_pressure, pressure_delta_psi
    )
    p_shape      = _pressure_shape_features(
        event.pressure_readings, pre_event_pressure, pressure_delta_psi
    )

    # Volume: prefer the firmware's cumulative integration sensor delta (set by
    # the historical importer) over the flow-average approximation, which can
    # overstate volume for long events with fill-pause-fill patterns after
    # downsampling kicks in at 120 s.
    if event.volume_litres_measured is not None:
        volume_litres = event.volume_litres_measured
    else:
        volume_litres = avg_flow * (duration / 60.0) if duration > 0 else 0.0

    # True hydraulic resistance: ΔP / avg_Q
    # Only meaningful when flow is above noise floor and a pressure
    # transient was actually captured.
    resistance: Optional[float] = None
    if avg_flow >= 0.15 and event.has_pressure_transient and event.pressure_delta_psi > 0:
        resistance = event.pressure_delta_psi / avg_flow

    # Resistance curve shape — uses corrected ΔP/Q formula.
    # pressure_readings are at 40 Hz, flow_readings at 1 Hz.  Pair them by
    # averaging each pressure bin that corresponds to one flow sample so the
    # resistance values are time-aligned.  Without downsampling, index-pairing
    # would match the first ~0.75 s of pressure against the full event duration
    # of flow, making every result meaningless.
    pressure_for_shape = event.pressure_readings
    if (event.flow_readings and event.pressure_readings
            and len(event.pressure_readings) > len(event.flow_readings)):
        n_flow = len(event.flow_readings)
        n_pres = len(event.pressure_readings)
        step = n_pres / n_flow          # fractional step to stay evenly spaced
        pressure_for_shape = []
        for i in range(n_flow):
            lo = int(round(i * step))
            hi = int(round((i + 1) * step))
            hi = max(hi, lo + 1)        # guarantee at least one sample per bin
            bin_samples = event.pressure_readings[lo:hi]
            pressure_for_shape.append(sum(bin_samples) / len(bin_samples))

    shape = _classify_resistance_shape(
        pressure_for_shape,
        event.flow_readings,
        pre_event_pressure,
    )

    # ── Degraded-supply guard ─────────────────────────────────────────────
    # Detect supply-pulsation during this event. When detected, substitute
    # an envelope-smoothed volume estimate so daily totals stay sane, and
    # mark the event excluded from clustering (centroid would otherwise be
    # corrupted by the chaotic flow readings). Always compute the smoothed
    # estimate — useful diagnostically even for healthy events.
    volume_litres_estimated = _estimate_volume_smoothed(
        event.flow_readings, duration
    )
    is_degraded, deg_diag = _detect_degraded_supply(
        event.pressure_readings,
        event.flow_readings,
        pre_event_pressure,
        shape,
        duration,
    )
    if is_degraded:
        volume_litres_effective = volume_litres_estimated
        volume_estimation_method = "pulsing_supply_envelope"
    else:
        volume_litres_effective = volume_litres
        volume_estimation_method = "raw"

    # Time features
    ts = event.start_ts
    hour = ts.hour
    dow = ts.weekday()
    hour_radians = 2 * math.pi * hour / 24
    duration_log = math.log(duration + 1)

    # Normalize timestamps to UTC so the UUID5 id and stored start_ts are
    # stable regardless of what timezone the incoming RawEvent carries.
    # This is the single storage point — all paths that write events go
    # through extract_features(), so enforcing UTC here is sufficient.
    _start = event.start_ts
    if _start.tzinfo is None:
        _start = _start.replace(tzinfo=timezone.utc)
    start_utc = _start.astimezone(timezone.utc)
    _end = event.end_ts
    if _end is not None:
        if _end.tzinfo is None:
            _end = _end.replace(tzinfo=timezone.utc)
        end_utc = _end.astimezone(timezone.utc)
    else:
        end_utc = None

    return {
        # Identity — UUID5 keyed on UTC start_ts so re-imports of the same
        # event always produce the same id and INSERT OR REPLACE is a no-op.
        "id": str(uuid.uuid5(uuid.NAMESPACE_OID,
                              f"{event.circuit}/{start_utc.isoformat()}")),
        "circuit": event.circuit,
        "start_ts": start_utc.isoformat(),
        "end_ts": end_utc.isoformat() if end_utc else None,

        # Raw measurements
        "duration_seconds": round(duration, 2),
        "avg_flow_lpm": round(avg_flow, 3),
        "peak_flow_lpm": round(peak_flow, 3),
        "flow_variability": round(flow_variability, 4),
        "pressure_delta_psi": round(pressure_delta_psi, 2),
        "pre_event_pressure_psi": round(pre_event_pressure, 2),
        "min_pressure_psi": round(event.min_pressure_psi, 2),
        "hydraulic_resistance": round(resistance, 3) if resistance is not None else None,
        "resistance_curve_shape": shape,
        "volume_litres": round(volume_litres, 3),

        # Detection provenance — tells Phase 2 how reliable pressure data is
        "start_trigger": event.start_trigger,
        "has_pressure_transient": 1 if event.has_pressure_transient else 0,
        "propagation_delay_ms": (
            round(event.propagation_delay_ms, 1)
            if event.propagation_delay_ms is not None else None
        ),

        # Derived features for ML clustering
        "duration_log": round(duration_log, 4),
        "hour_of_day": hour,
        "day_of_week": dow,
        "hour_sin": round(math.sin(hour_radians), 4),
        "hour_cos": round(math.cos(hour_radians), 4),
        "is_weekend": 1 if dow >= 5 else 0,

        # Composite / training flags
        "is_composite": 1 if event.is_composite else 0,
        "other_valve_open": (
            1 if event.other_valve_open is True
            else 0 if event.other_valve_open is False
            else None
        ),
        # Excluded from cluster training when:
        #   - composite event (multiple fixtures concurrent), OR
        #   - degraded by supply pulsation (chaotic flow readings)
        # Note: clustering exclusion is NOT the same as volume exclusion;
        # the volume_litres_effective field still contributes to daily totals.
        "excluded_from_training": 1 if (event.is_composite or is_degraded) else 0,
        # match_rejection_reason: 'pulsing_supply' on degraded events, BUT
        # only if no other exclusion reason was set first (composite events
        # keep their authoritative reason). The cluster-engine reason values
        # ('no_centers', 'features_missing', etc.) are written separately by
        # match_and_learn; this field carries the *upstream* reason set by
        # feature extraction.
        "match_rejection_reason": (
            "pulsing_supply" if (is_degraded and not event.is_composite)
            else None
        ),

        # Degraded-supply guard
        "degraded_supply":             1 if is_degraded else 0,
        "volume_litres_estimated":     round(volume_litres_estimated, 3),
        "volume_litres_effective":     round(volume_litres_effective, 3),
        "volume_estimation_method":    volume_estimation_method,
        "degraded_diagnostic_json":    json.dumps(deg_diag, allow_nan=False),

        # Flow shape features
        "flow_signature_json":    json.dumps(sig),
        "pressure_signature_json": json.dumps(p_sig),
        "positive_edge_count":    pos_edges,
        "negative_edge_count":    neg_edges,
        "flow_edge_count":        pos_edges + neg_edges,
        **dynamics,
        "mid_event_flow_drop_lpm": mid_drop,
        "steady_state_fraction":  steady,

        # Pressure transient features
        **p_stats,

        # Pressure transient shape features
        "pressure_onset_ms":          p_shape['pressure_onset_ms'],
        "recovery_overshoot_psi":     p_shape['recovery_overshoot_psi'],
        "pressure_oscillation_count": p_shape['pressure_oscillation_count'],

        # ESP waveform A/B fields — overridden by _enrich_from_waveform when
        # firmware 3.7.0+ waveform data is available and correlated.
        "esp_waveform_used":      0,
        "waveform_event_id":      None,
        "waveform_quality":       None,
        "waveform_overlap_score": None,

        # Signature provenance — overridden to "esp_full_*" by _enrich_from_waveform
        # when ESP full_flow / full_pressure arrays are used as canonical signatures.
        "signature_source":       "software",
    }


class FeatureExtractor:
    """
    Consumes RawEvent objects from the queue and stores
    extracted features in SQLite.
    """

    def __init__(self, event_queue: asyncio.Queue,
                 db_conn: sqlite3.Connection, alert_manager=None,
                 ha_client=None, event_detector=None):
        self._queue = event_queue
        self._db = db_conn
        self._alert_manager = alert_manager
        self._ha = ha_client
        # Optional EventDetector — provides WaveformChunkAccumulator access
        # (firmware 3.9.0+). None when running in test / historical-import
        # contexts; _find_waveform handles the missing-detector case.
        self._event_detector = event_detector
        self._running = False
        # Strong references for fire-and-forget tasks (anomaly alerts).
        # Without this, the only ref to the task is whatever
        # asyncio.create_task returns — Python may GC the task before it
        # completes, silently dropping the alert. add_done_callback also
        # gives us a place to observe and log exceptions instead of the
        # default "Task exception was never retrieved" warning.
        self._pending_alert_tasks: set[asyncio.Task] = set()
        # Per-circuit cooldown timestamps for the pulsing-supply alert.
        # In-memory only — resets on addon restart. That's acceptable for now:
        # if a real pulsing episode persists across a restart the user will
        # be re-alerted (mildly annoying, not dangerous). Persist to DB
        # later if it becomes a problem.
        self._last_pulsing_alert_at: dict[str, datetime] = {}
        # Set by orchestrator after ClusterEngine is initialised and rebuilt.
        self.cluster_engine = None

    def _spawn_alert_task(self, coro) -> None:
        """Fire a background alert and keep a strong ref until it completes."""
        t = asyncio.create_task(coro)
        self._pending_alert_tasks.add(t)
        t.add_done_callback(self._pending_alert_tasks.discard)
        # Also log any unobserved exception so a failed alert isn't silent.
        def _log_exc(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                log.error("Anomaly alert task raised: %s", exc, exc_info=exc)
        t.add_done_callback(_log_exc)

    async def run(self) -> None:
        """Process events from the queue until cancelled."""
        self._running = True
        log.info("Feature extractor started")
        while self._running:
            try:
                event: RawEvent = await asyncio.wait_for(
                    self._queue.get(), timeout=5.0)
                await self._process(event)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("Feature extractor error: %s", e, exc_info=True)

    def stop(self) -> None:
        self._running = False

    async def _enrich_propagation_delay(self, event: RawEvent) -> None:
        """Refine propagation_delay_ms with the precise server-side last_changed
        timestamp of the flow-onset entity from HA history.

        propagation_delay_ms is, for every trigger type, the buffer-scan delay
        flow_onset − true_transient_onset.  The true onset is recovered here and
        the flow-onset side is sharpened with the precise HA-history timestamp.
        HA's recorder does not retain the 40 Hz pressure sensor at full
        resolution, so the buffer scan stays authoritative for the pressure side.

          - 'flow' / 'pressure+flow' start_ts IS the flow onset.
          - 'pressure'               flow_onset_ts is the flow onset (start_ts
            is the threshold crossing, not the true transient onset).
        """
        from datetime import timedelta

        if not event.propagation_delay_ms:
            # No measured transient delay — nothing to refine.
            return
        if event.start_trigger == "pressure":
            if event.flow_onset_ts is None:
                return
            pressure_onset = event.flow_onset_ts - timedelta(
                milliseconds=event.propagation_delay_ms)
        else:
            pressure_onset = event.start_ts - timedelta(
                milliseconds=event.propagation_delay_ms)

        window_start = pressure_onset - timedelta(seconds=5)
        window_end   = (event.end_ts or event.start_ts) + timedelta(seconds=15)
        try:
            history = await self._ha.get_history(
                event.flow_onset_entity, window_start, window_end)
            onset = next(
                (h for h in history
                 if h["state"].lower() in ("on", "true", "1")
                 and h["last_changed"] >= pressure_onset),
                None,
            )
            if onset:
                event.propagation_delay_ms = round(
                    max(0.0, (onset["last_changed"] - pressure_onset)
                        .total_seconds() * 1000), 1)
                log.debug("[%s] propagation delay enriched from HA history: %.0f ms",
                          event.circuit, event.propagation_delay_ms)
        except Exception as e:
            log.debug("[%s] propagation delay HA enrichment failed: %s",
                      event.circuit, e)

    def _find_waveform(self, event: RawEvent) -> "Optional[WaveformRecord]":
        """
        Look up the most-recently assembled WaveformRecord for this circuit and
        check whether it correlates with the given RawEvent.

        Returns the record when both the duration-overlap score exceeds
        _WF_MATCH_MIN_SCORE and the record was assembled within
        _WF_MATCH_WINDOW_S seconds of now; otherwise None.
        """
        import time as _time

        if self._event_detector is None:
            return None
        try:
            record = self._event_detector.get_latest_waveform(event.circuit)
        except Exception:
            return None
        if record is None:
            return None
        # Recency guard: if the record was assembled too long ago it belongs to
        # a previous event, not this one.
        age_s = _time.monotonic() - record.received_at
        if age_s > _WF_MATCH_WINDOW_S:
            log.debug(
                "[%s] waveform skip — event_id=%d stale (age=%.1fs > %.0fs)",
                event.circuit, record.metadata.event_id, age_s, _WF_MATCH_WINDOW_S,
            )
            return None
        # Duration overlap guard.
        score = _wf_overlap_score(event, record)
        if score < _WF_MATCH_MIN_SCORE:
            log.debug(
                "[%s] waveform skip — event_id=%d overlap=%.2f < %.2f "
                "(event=%.1fs fw=%.1fs)",
                event.circuit, record.metadata.event_id, score, _WF_MATCH_MIN_SCORE,
                (event.end_ts - event.start_ts).total_seconds() if event.end_ts else 0.0,
                (
                    _wf_millis_sub(record.metadata.end_ms, record.metadata.start_ms)
                    + record.metadata.tail_ms
                ) / 1000.0,
            )
            return None
        return record

    async def _process(self, event: RawEvent) -> None:
        if not event.complete:
            return

        if self._ha and event.flow_onset_entity:
            await self._enrich_propagation_delay(event)

        features = extract_features(event)

        # Attempt to enrich features from ESP waveform capture (firmware 3.7.0+).
        # Per-group routing: each group falls back to the legacy value independently.
        wf_record = self._find_waveform(event)
        if wf_record is not None:
            score = _wf_overlap_score(event, wf_record)
            _enrich_from_waveform(features, wf_record, score)
            log.debug(
                "[%s] waveform enriched — event_id=%d boot_id=%d "
                "overlap=%.2f q=%d fl=0x%02x",
                event.circuit,
                wf_record.metadata.event_id,
                wf_record.metadata.boot_id,
                score,
                wf_record.metadata.quality,
                wf_record.metadata.flags,
            )

        try:
            from .database import (upsert_event_and_apply_hourly_volume,
                                   is_event_in_exclusion_window,
                                   find_overlapping_event)

            # Writer-boundary duplicate guard: two importer catch-up runs can
            # both queue a reconstruction before either has written to the DB,
            # so the importer-side check alone cannot prevent the race.  Check
            # here too, as close to the INSERT as the architecture allows.
            if event.end_ts is not None:
                blocking = find_overlapping_event(
                    self._db, event.circuit,
                    event.start_ts.isoformat(),
                    event.end_ts.isoformat(),
                    exclude_event_id=features["id"],
                )
                if blocking is not None:
                    suffix = ""
                    if blocking.get("user_fixture_type"):
                        suffix = f" (user-labeled '{blocking['user_fixture_type']}')"
                    elif blocking.get("fixture_id") and blocking.get("user_locked"):
                        suffix = f" (user-locked fixture id={blocking['fixture_id']})"
                    log.info(
                        "[%s] dropping queued event %s..%s: overlaps existing "
                        "event id=%s %s..%s%s",
                        event.circuit,
                        event.start_ts.strftime("%H:%M:%S"),
                        event.end_ts.strftime("%H:%M:%S"),
                        blocking["id"],
                        blocking["start_ts"],
                        blocking["end_ts"],
                        suffix,
                    )
                    return

            # Atomic upsert + hourly_volume update. Uses
            # volume_litres_effective (which is volume_litres for healthy
            # events, or the envelope-smoothed estimate for degraded ones).
            # Idempotent: re-imports subtract the prior contribution before
            # adding the new value, so no double-counting.
            effective_volume = float(features.get("volume_litres_effective") or 0)
            is_new_event = upsert_event_and_apply_hourly_volume(
                self._db, features, effective_volume,
            )

            # ── Plumbing-event exclusion window (Phase 2.1) ───────────────
            # If the user opened an exclusion window (e.g. post-winterization
            # flush), flag the event so the cluster engine skips it.  Volume
            # tracking continues — only fixture identification is excluded.
            # Preserves any upstream match_rejection_reason already set by
            # feature extraction (e.g. 'pulsing_supply') — only stamps
            # 'excluded_from_training' when no reason was set.
            start_ts_str = features.get("start_ts")
            if (start_ts_str
                    and is_event_in_exclusion_window(
                        self._db, event.circuit, start_ts_str)):
                existing_reason = features.get("match_rejection_reason")
                new_reason = existing_reason or "excluded_from_training"
                self._db.execute(
                    """UPDATE events
                       SET excluded_from_training  = 1,
                           match_rejection_reason  = ?
                       WHERE id = ?""",
                    (new_reason, features["id"]),
                )
                features["excluded_from_training"] = 1
                features["match_rejection_reason"] = new_reason
                log.debug(
                    "[%s] event excluded from training (exclusion window active)",
                    event.circuit,
                )

            # Persist the hi-res waveform for the event-detail modal. Always
            # called, even for healthy events — useful diagnostic data. Skips
            # silently when readings lists are empty (historical events).
            _persist_waveform(
                self._db,
                features["id"],
                event.flow_readings,
                event.pressure_readings,
                float(features.get("duration_seconds") or 0),
            )

            if is_new_event and not features.get("excluded_from_training"):
                self._db.execute("""
                    UPDATE training_state
                    SET events_collected = events_collected + 1,
                        updated_at = datetime('now')
                    WHERE circuit = ?
                      AND state = 'calibrating'
                """, (event.circuit,))
            self._db.commit()

            # ── Phase 2: sequence context + cluster matching ───────────────
            await self._cluster_event(event.circuit, features)
            # ──────────────────────────────────────────────────────────────

            am = self._alert_manager
            if am and features.get("anomaly_score"):
                score = float(features["anomaly_score"])
                ts_row = self._db.execute(
                    "SELECT state FROM training_state WHERE circuit = ?",
                    (event.circuit,)).fetchone()
                if ts_row and ts_row["state"] == "live" and score >= 0.60:
                    circuit_name = event.circuit.replace("_", " ").title()
                    # Use _spawn_alert_task instead of bare asyncio.create_task
                    # so Python doesn't garbage-collect the task before the
                    # alert is sent (loose-reference antipattern).
                    self._spawn_alert_task(
                        am.alert_flow_anomaly(event.circuit, score, circuit_name))

            # ── Pulsing-supply alert (rate-limited) ────────────────────────
            # Fire at most once per hour per circuit, and only when at least
            # 3 degraded events occurred in the past 30 minutes. Uses
            # Python-computed UTC ISO timestamps for the SQL cutoff so the
            # comparison format matches stored start_ts exactly.
            if am and features.get("degraded_supply"):
                now = datetime.now(timezone.utc)
                cutoff_30min = (now - timedelta(minutes=30)).isoformat()
                try:
                    row = self._db.execute(
                        "SELECT COUNT(*) FROM events "
                        "WHERE circuit = ? AND degraded_supply = 1 "
                        "AND start_ts >= ?",
                        (event.circuit, cutoff_30min),
                    ).fetchone()
                    count = int(row[0]) if row else 0
                except Exception as e:
                    log.warning("[%s] pulsing-supply count query failed: %s",
                                event.circuit, e)
                    count = 0
                last = self._last_pulsing_alert_at.get(event.circuit)
                if count >= 3 and (
                    last is None or now - last >= timedelta(hours=1)
                ):
                    circuit_name = event.circuit.replace("_", " ").title()
                    self._spawn_alert_task(
                        am.alert_pulsing_supply(
                            event.circuit, circuit_name, count
                        )
                    )
                    self._last_pulsing_alert_at[event.circuit] = now

            log.debug(
                "[%s] event stored — duration=%.1fs shape=%s trigger=%s "
                "transient=%s resistance=%.2f",
                event.circuit,
                features["duration_seconds"],
                features["resistance_curve_shape"],
                features["start_trigger"],
                features["has_pressure_transient"],
                features["hydraulic_resistance"] or 0,
            )
        except Exception as e:
            log.error("[%s] failed to store event: %s", event.circuit, e, exc_info=True)

    async def _cluster_event(self, circuit: str, features: dict) -> None:
        """Compute sequence context, run cluster matching, write results back."""
        if features.get("excluded_from_training"):
            return

        event_id    = features["id"]
        start_ts    = features.get("start_ts")

        # 1. Find the previous event on this circuit
        seconds_since_prev = None
        prev_cluster_id    = None
        if start_ts:
            prev = self._db.execute(
                """SELECT id, cluster_id, end_ts FROM events
                   WHERE circuit = ? AND end_ts < ? AND id != ?
                   ORDER BY end_ts DESC LIMIT 1""",
                (circuit, start_ts, event_id)
            ).fetchone()
            if prev and prev["end_ts"]:
                try:
                    ev_start = datetime.fromisoformat(start_ts)
                    prev_end = datetime.fromisoformat(
                        prev["end_ts"].replace("Z", "+00:00"))
                    if ev_start.tzinfo is None:
                        ev_start = ev_start.replace(tzinfo=timezone.utc)
                    if prev_end.tzinfo is None:
                        prev_end = prev_end.replace(tzinfo=timezone.utc)
                    gap = (ev_start - prev_end).total_seconds()
                    if 0 <= gap < _SEQUENCE_GAP_MAX_S:
                        seconds_since_prev = gap
                        prev_cluster_id    = prev["cluster_id"]
                        # Retroactively fill seconds_to_next_event on previous event
                        self._db.execute(
                            "UPDATE events SET seconds_to_next_event = ? WHERE id = ?",
                            (gap, prev["id"])
                        )
                except (ValueError, TypeError):
                    pass

        # 2. Cluster matching (sync DB writes dispatched off the event loop)
        cluster_id_result = None
        match_confidence  = None
        match_level       = None
        match_rejection_reason: Optional[str] = None
        if self.cluster_engine:
            try:
                event_row = self._db.execute(
                    "SELECT * FROM events WHERE id = ?", (event_id,)
                ).fetchone()
                if event_row:
                    import functools
                    loop = asyncio.get_running_loop()
                    (cluster_id_result, match_confidence, match_level,
                     match_rejection_reason) = await loop.run_in_executor(
                        None,
                        functools.partial(
                            self.cluster_engine.match_and_learn,
                            dict(event_row),
                            circuit,
                            prev_cluster_id,
                            seconds_since_prev,
                        )
                    )
            except Exception as e:
                log.error("[%s] cluster matching failed: %s", circuit, e,
                          exc_info=True)

        # 3. Derive anomaly_score and store it in features so the alert
        #    check in _process() can read it.  anomaly_score is intentionally
        #    NOT stored in the events table — it is ephemeral and recalculated
        #    by the live match path only; backfill uses match_confidence directly.
        #    High score = anomalous:
        #      • no match at all           → 1.0
        #      • poor confidence match     → 1.0 - confidence
        #      • good confidence match     → near 0.0
        if match_confidence is not None:
            features["anomaly_score"] = round(1.0 - match_confidence, 3)
        elif cluster_id_result is None and match_rejection_reason not in (
            "type_gate_rejected", "excluded_from_training"
        ):
            # Unmatched event in live state with no explicit rejection reason
            # — treat as fully anomalous.
            features["anomaly_score"] = 1.0

        # Write cluster results back to the event row
        self._db.execute(
            """UPDATE events SET
                 cluster_id               = ?,
                 match_confidence         = ?,
                 match_level              = ?,
                 match_rejection_reason   = ?,
                 seconds_since_prev_event = ?,
                 prev_cluster_id          = ?
               WHERE id = ?""",
            (cluster_id_result, match_confidence, match_level,
             match_rejection_reason,
             seconds_since_prev, prev_cluster_id, event_id)
        )

        # 4. Update fixtures.last_seen_at when this event matched a named fixture
        if cluster_id_result is not None:
            fc_row = self._db.execute(
                """SELECT fixture_id FROM fixture_clusters
                   WHERE circuit = ? AND id = ? AND fixture_id IS NOT NULL""",
                (circuit, cluster_id_result)
            ).fetchone()
            if fc_row and fc_row["fixture_id"]:
                self._db.execute(
                    "UPDATE fixtures SET last_seen_at = ? WHERE id = ?",
                    (datetime.now(timezone.utc).isoformat(), fc_row["fixture_id"])
                )

        self._db.commit()
