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


# ─────────────────────────────────────────────────────────────────────────────
# Resistance-shape classification constants (extracted 2026-05-27 from
# inline magic numbers; original calibration was during the Phase-2
# resistance-shape work circa 2026-04).
#
# All four values are tunable and ALL affect _classify_resistance_shape().
# Keep them here so changes in one place don't drift from related ones
# (e.g. RAMP_EXCLUSION_FRACTION and MIN_READINGS_FOR_SHAPE both have to
# leave enough samples that the third-vs-third trend analysis is
# statistically meaningful).
# ─────────────────────────────────────────────────────────────────────────────

# Minimum total sample count before we'll attempt classification at all.
# Below this, return "unknown" rather than producing noisy labels for
# very short events. 10 ≈ 5 s at the 2 Hz pressure publish rate.
MIN_READINGS_FOR_SHAPE       = 10

# Fraction of samples to drop from each end before classifying. Ramp-up
# and ramp-down phases distort both CV and trend; cutting 20% off each
# end leaves the steady-state middle 60% for analysis.
RAMP_EXCLUSION_FRACTION      = 5     # used as n // RAMP_EXCLUSION_FRACTION

# Minimum samples LEFT after ramp exclusion + low-flow filtering before
# we proceed. Below this, return "unknown" — the trend analysis splits
# into thirds and needs at least 2 samples per third.
MIN_RESISTANCE_SAMPLES       = 6

# Coefficient of variation threshold for "pulsed". 0.55 is calibrated to
# reject sensor noise at the pressure trough while still catching real
# oscillating appliances (dishwashers, washing machines) which typically
# produce CV > 0.80. Was raised from 0.40 when the autocorr
# normalisation bug was discovered — see plan A-3 in
# zany-yawning-church.md.
RESISTANCE_PULSED_CV         = 0.55

# Trend threshold for rising / falling. ΔP/Q changing by more than this
# fraction between the first third and the last third of the steady-
# state samples is classified as a trend; otherwise "steady". 15% is a
# conservative bar — too low and noise gets called rising; too high and
# real toilet-fill rising-resistance signatures get classified steady.
RESISTANCE_TREND_RATIO       = 0.15


def _safe_std(values: list) -> float:
    valid = [v for v in values if v is not None and not math.isnan(v)]
    if len(valid) < 2:
        return 0.0
    return statistics.stdev(valid)


def _bin_pressure_to_flow(flow_readings: List[float],
                          pressure_readings: List[float]) -> List[float]:
    """Index-bin the denser pressure series down to the flow sample count.

    pressure_readings run at 40 Hz (live) / 1–2 Hz (importer), flow_readings at
    1 Hz — pairing by raw index would match the first ~0.75 s of pressure
    against the whole event. Each flow sample instead gets the MEAN of its
    corresponding pressure bin, so the two series are time-aligned by position.
    A pressure series no longer than flow is returned as-is (already aligned).
    """
    if not flow_readings or not pressure_readings:
        return list(pressure_readings or [])
    if len(pressure_readings) <= len(flow_readings):
        return list(pressure_readings)
    n_flow = len(flow_readings)
    n_pres = len(pressure_readings)
    step = n_pres / n_flow              # fractional step to stay evenly spaced
    binned: List[float] = []
    for i in range(n_flow):
        lo = int(round(i * step))
        hi = int(round((i + 1) * step))
        hi = max(hi, lo + 1)            # guarantee at least one sample per bin
        seg = pressure_readings[lo:hi]
        binned.append(sum(seg) / len(seg))
    return binned


# Minimum aligned samples before the correlation is meaningful; below this the
# rise-phantom discriminator returns None = NO VERDICT (leak-safe default: an
# event without pressure signal is always kept as real water).
_CORR_MIN_SAMPLES: int = 4


def _flow_pressure_correlation(flow_readings: Optional[List[float]],
                               pressure_readings: Optional[List[float]],
                               ) -> Optional[float]:
    """Pearson correlation of flow vs (index-binned) pressure over the event.

    The rising-pressure phantom discriminator (dev14, validated over the full
    2026-05-17..07-03 history against 551 labelled events): real demand pulls
    pressure DOWN while flow runs (strongly negative r; audited real draws sat
    at −0.88/−0.24), while a city-pressure RISE that spins the turbine shows
    flow tracking the pressure ramp (positive r; the audited 07-02 14:01
    phantom was +0.67). Index-binned alignment agreed with timestamp-aligned
    correlation on 92% of bursts, so no RawEvent timestamp change is needed.

    Returns None (= no verdict, never a 0.0 that could look meaningful) when
    either series is missing/short (< ``_CORR_MIN_SAMPLES`` finite pairs) or
    has zero variance (flat line — undefined correlation).
    """
    if not flow_readings or not pressure_readings:
        return None
    press = _bin_pressure_to_flow(flow_readings, pressure_readings)
    pairs = []
    for f, p in zip(flow_readings, press):
        try:
            fx, px = float(f), float(p)
        except (TypeError, ValueError):
            continue
        if math.isfinite(fx) and math.isfinite(px):
            pairs.append((fx, px))
    n = len(pairs)
    if n < _CORR_MIN_SAMPLES:
        return None
    mean_f = sum(f for f, _ in pairs) / n
    mean_p = sum(p for _, p in pairs) / n
    var_f = sum((f - mean_f) ** 2 for f, _ in pairs)
    var_p = sum((p - mean_p) ** 2 for _, p in pairs)
    if var_f <= 0.0 or var_p <= 0.0:
        return None
    cov = sum((f - mean_f) * (p - mean_p) for f, p in pairs)
    return cov / math.sqrt(var_f * var_p)


def _classify_resistance_shape(
    pressure_readings: List[float],
    flow_readings: List[float],
    pre_event_pressure_psi: float,
    min_flow: float = 0.15,
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
    if n < MIN_READINGS_FOR_SHAPE:
        return "unknown"

    # Exclude ramp-up and ramp-down (the leading and trailing
    # RAMP_EXCLUSION_FRACTION of the series).
    ramp = max(1, n // RAMP_EXCLUSION_FRACTION)
    p_mid = pressure_readings[ramp: n - ramp]
    f_mid = flow_readings[ramp: n - ramp]

    # Compute true ΔP/Q resistance at each steady-state point.
    # Skip readings where flow is below noise floor to avoid division
    # by near-zero inflating variance.
    # L/min noise floor — per-circuit, matches event_detector MIN_FLOW_LPM (60 ÷ ppl).
    resistance = []
    for p, f in zip(p_mid, f_mid):
        if f >= min_flow:
            delta_p = pre_event_pressure_psi - p   # positive = pressure has dropped
            if delta_p >= 0:                        # only during actual demand
                resistance.append(delta_p / f)

    if len(resistance) < MIN_RESISTANCE_SAMPLES:
        return "unknown"

    mean_r = statistics.mean(resistance)
    if mean_r <= 0:
        return "unknown"

    # Coefficient of variation — high CV = genuinely pulsed demand.
    cv = _safe_std(resistance) / mean_r
    if cv >= RESISTANCE_PULSED_CV:
        return "pulsed"

    # Trend: compare first and last third of steady-state resistance
    third = max(1, len(resistance) // 3)
    r1 = statistics.mean(resistance[:third])
    r3 = statistics.mean(resistance[-third:])

    change_ratio = (r3 - r1) / max(abs(r1), 0.01)
    if change_ratio > RESISTANCE_TREND_RATIO:
        return "rising"
    if change_ratio < -RESISTANCE_TREND_RATIO:
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

# ─────────────────────────────────────────────────────────────────────────────
# Pressure-restoration phantom guard (added 2026-05-28)
#
# City supply-pressure restoration or regulator hunting can hold the
# paddlewheel above the flow threshold for many minutes while almost no real
# water is drawn — and crucially with NO fixture pressure load. These events
# look "steady" so the degraded-supply detector does not catch them, yet they
# inflate daily volume totals badly (one confirmed event was 287 gal of false
# volume over 135 min). The fingerprint is a long duration combined with a
# near-zero pressure drop. Circuit-agnostic — real zone irrigation produces a
# genuine solenoid pressure drop (> 2 PSI) so it is not affected.
# Two FROZEN duration floors (2026-06-14) — NEITHER is per-home calibratable; both behave
# like the leak-safety guards below. When the honest active-flow metrics (flow_integral +
# flow_on_ratio) are present they PROVE no water moved, so even a 2-min event is unambiguous
# (the same justification cross-talk uses for its 120 s floor) → _PHANTOM_NOFLOW_MIN_DURATION_S.
# A LEGACY event predating the active-flow columns has NULL metrics and no no-flow proof, so it
# must keep the conservative 30-min floor: without the proof a shorter low-ΔP event cannot be
# told apart from a real slow draw → _PHANTOM_MIN_DURATION_S. (PHANTOM_MIN_DURATION_S is read
# from the module constant directly, never via _ac/calib, so it can never be lowered.)
_PHANTOM_MIN_DURATION_S: float = 1800.0          # 30 min — frozen LEGACY (no-metrics) floor
_PHANTOM_NOFLOW_MIN_DURATION_S: float = 120.0    # frozen metric-present floor (no-flow proof exists)
_PHANTOM_MAX_DELTA_PSI:  float = 2.0
# Brief-burst guard (was the "true-flow" guard, added 2026-06-04; re-scoped 2026-06-14). A SHORT
# window (< _PHANTOM_MIN_DURATION_S) that reaches a real-fixture flow rate is most likely a genuine
# brief draw caught inside a long pressure window (validated: a66e0d63, an 8.4 s / 16.9 lpm burst
# moving 0.80 L) — NOT a regulator/restoration artifact — so true_avg over its active segment
# rescues it. At/above the 30-min floor the long span is itself dispositive (no real draw runs that
# long at < 5 % flow-on), so this guard is LIFTED there. _PHANTOM_MAX_FLOW_INTEGRAL_L /
# _PHANTOM_MAX_FLOW_ON_RATIO below are the FROZEN no-flow leak-safety guards (a real leak is
# continuous ⇒ high flow_on_ratio): ANY above its ceiling means real water moved → NOT a phantom.
_PHANTOM_MAX_TRUE_FLOW_LPM:   float = 2.0
_PHANTOM_MAX_FLOW_INTEGRAL_L: float = 1.0
_PHANTOM_MAX_FLOW_ON_RATIO:   float = 0.05

# Suppression-averted guard (2026-07 audit, Phase 2b — FROZEN, never calibrated).
# The no-flow leak-safety ceilings above only run when the flow metrics are
# non-NULL; a legacy/import event with NULL metrics used to sail past them and
# could zero a large REAL draw (observed: a 141 L shower zeroed as a phantom).
# Backstop at the verdict site: a would-be phantom whose measured volume_litres
# is at/above this threshold is NOT zeroed — the volume is KEPT and the event is
# flagged for review (anomaly_type 'suppression_averted'). Keeping volume can
# never mask a leak (only zeroing could), so this is strictly leak-safer.
_PHANTOM_REVIEW_FLAG_LITRES:  float = 10.0

# Pulsing-supply envelope cap (2026-07 audit, Phase 2a). The envelope estimate
# measured 2.86x reality in aggregate (worst single case: 336 L claimed for 2 L
# real). Cap it against the best available measured-flow evidence: the estimate
# may exceed the flow integral (the flow meter under-reads during pulsing —
# that's why the envelope estimator exists) but not by more than the multiplier;
# the floor keeps tiny events from being clamped into meaninglessness.
_ENVELOPE_CAP_FLOW_MULT: float = 1.5
_ENVELOPE_CAP_FLOOR_L:   float = 2.0

# Sparse envelope (Fix 4): a LONG event that is almost entirely idle — a brief real draw
# followed by a long no-flow tail the pressure-defined boundary never closed. Real water
# moved (so NOT a phantom — its volume is kept), but the envelope's duration/shape are
# unreliable, so it is kept out of training and carries no fixture identity. Distinct from a
# phantom (≈no water moved) and a dribble (BRIEF). flow_on_ratio is the discriminator: a real
# slow draw flows continuously (high ratio); <= 0.10 over >= 10 min is >= 90 % idle. (An
# 8113 s "shower" with 46 s of flow on the real export was polluting the shower signature.)
_SPARSE_ENVELOPE_MIN_DURATION_S:    float = 600.0
_SPARSE_ENVELOPE_MAX_FLOW_ON_RATIO: float = 0.10

# Cross-talk (migration 20260540): a long event registered via a REAL pressure drop
# (ΔP >= _PHANTOM_MAX_DELTA_PSI) with essentially no real flow on this circuit —
# another circuit's draw pulled the shared-supply pressure down. Reuses the phantom's
# no-flow ceilings (_PHANTOM_MAX_FLOW_INTEGRAL_L / _PHANTOM_MAX_FLOW_ON_RATIO); the only
# NEW constant is a shorter min duration — the no-flow metrics make a 2-min event
# unambiguous, unlike the 30-min near-zero-ΔP phantom rule.
_XTALK_MIN_DURATION_S: float = 120.0

# ─────────────────────────────────────────────────────────────────────────────
# Low-flow "dribble" guard (added 2026-05-31)
#
# A DIFFERENT phenomenon from the long-duration phantom above: brief, tiny-
# volume, low-flow trickles with no real pressure load — sensor noise or
# pressure-equalisation blips registering as flow. In the May-2026 ground-truth
# export the events the user hand-marked as artifacts clustered at median
# duration 12 s, volume 0.10 L, avg_flow 0.30 lpm, ΔP 0.00 — the long-duration
# rule caught only 1 of 49. These thresholds (derived from that labelled set,
# ~76% recall / ~73% precision) catch them.
#
# A dribble is a VOLUME-ZEROING verdict (changed 2026-06-19): it sets
# is_low_flow_dribble + excluded_from_training AND zeroes volume_litres_effective,
# removing the brief blip from totals like a phantom does. (Previously it kept the
# volume as a benign "Drip"; users were hand-marking these short ~0-ΔP blips as
# "not real use" to suppress them, so the auto-verdict now does it.) Leak-safe
# because the flow<1 L·min⁻¹ gate excludes real small draws (icemaker / fridge
# dispenser run ~3 L·min⁻¹) and a sustained slow flow accumulates past
# _DRIBBLE_MAX_VOLUME_L, so it stays a counted long event — a continuous leak is
# never fragmented into zeroed dribbles (verified on the archive: 0 / 347 auto
# dribbles moved >= SUSPECT_ZERO_LITRES of real HA flow). detector_validation now
# holds dribble to the same suspect-zeroing leak-safety bar as phantom/cross-talk.
# Re-tune here as more labelled data arrives.
_DRIBBLE_MAX_VOLUME_L:  float = 0.5
_DRIBBLE_MAX_FLOW_LPM:  float = 1.0
_DRIBBLE_MAX_DELTA_PSI: float = 1.5

# Coarse-meter safety guard (runtime-PPL meters). When a circuit's meter-derived
# low-flow floor (60 ÷ ppl) is at/above this, the meter measures sub-1-L/min flows
# RELIABLY (e.g. a 72-ppl oval gear → 0.83 L/min), so a low-flow reading is real
# water — NOT the turbine quantization-noise the dribble heuristic suppresses.
# Above this floor we never dribble-zero (bias conservative: "never make real water
# invisible"). The 396-ppl turbine floor (0.15) is far below, so turbine behaviour
# is unchanged. Tune with real positive-displacement data.
_DRIBBLE_RELIABLE_METER_FLOOR_LPM: float = 0.5

# ─────────────────────────────────────────────────────────────────────────────
# Rising-pressure phantom (dev14, 2026-07-03)
#
# A SHORT small burst whose flow TRACKED a city-pressure RISE: the supply
# pressure climbs, the expanding line pushes a slug through the turbine, and a
# 3–50 s "real" draw is logged. Physically the opposite of demand — a real draw
# pulls pressure DOWN while flow runs — so the flow↔pressure Pearson correlation
# (events.flow_pressure_corr) separates them cleanly. Validated over the full
# 2026-05-17..07-03 HA history against 551 labelled events (0 hard FPs at these
# thresholds; hand-audited anchors: real draws −0.88/−0.24, the 07-02 14:01 rise
# phantom +0.67; the closest SPECIFIC-fixture-labelled real event sat at +0.48,
# hence the 0.6 cutoff). Complements — never overlaps — the existing detectors:
# the long phantom needs >= _PHANTOM_NOFLOW_MIN_DURATION_S (120 s) while this
# caps AT 120 s (the two partition the duration axis); dribble needs
# flow < 1 L/min while these bursts peak well above it.
#
# LEAK-SAFETY (frozen, never calibrated):
#   • volume cap is STRICT < 1.0 L — a zeroed rise phantom can never reach
#     detector_validation's SUSPECT_ZERO_LITRES (1.0 L) leak bar;
#   • a leak is sustained flow + sustained pressure DROP (strongly negative
#     corr) — the corr >= 0.6 gate is the opposite signature by construction;
#   • corr is None (no/short pressure signal) ⇒ NO verdict — the water stays
#     counted. Degraded-supply events are skipped (their pressure is unreliable).
_RISE_PHANTOM_MIN_CORR:       float = 0.6    # calibratable KEY exists; default frozen
_RISE_PHANTOM_MAX_VOLUME_L:   float = 1.0    # STRICT < ; frozen leak guard
_RISE_PHANTOM_MAX_DURATION_S: float = 120.0  # frozen; pairs with the 120 s phantom floor
# match_rejection_reason value — distinct provenance while the flag/method reuse
# the pressure_restoration_phantom family (same UI pill, hide-toggle, guards).
RISE_PHANTOM_REASON: str = "rising_pressure_phantom"

# ─────────────────────────────────────────────────────────────────────────────
# Irrigation zone-switch cross-talk (2026-06-28)
#
# A SECOND, distinct cross-talk phenomenon from the long-no-flow rule above. When
# irrigation is running, every zone-valve switch fires a water-hammer transient
# through the shared supply manifold that briefly spins the MAIN flow impeller —
# logging a flurry of tiny ~Tap / Other main events (1–11 s, ≤~0.8 L) that are NOT
# real household water. The fingerprint that separates these from a genuine main
# draw that merely overlaps irrigation is the PRESSURE-SWING RATIO: a zone-switch
# transient originates on the irrigation branch, so the irrigation-circuit pressure
# swing is LARGER than the main-circuit swing (ratio ≥ 1.3, typically 1.7–3.2),
# whereas a real main draw pulls the MAIN branch down (ratio ≤ ~1.1, shared ≈ 1.0).
# Validated across all 12 irrigation days in the May–Jun 2026 raw HA history: the
# rule flags the zone-switch bursts and keeps every real draw, incl. a 123 L/25-min
# dawn shower and toilets that overlapped irrigation.
#
# This verdict is applied OUT-OF-BAND by the historical importer's reconciliation
# pass (historical_importer._reconcile_irrigation_cross_talk), NOT the live
# per-circuit detector — the live detector has only ITS circuit's pressure buffer,
# while the importer can pull the irrigation pressure sensor from HA history. It
# reuses the is_cross_talk flag (hide + volume-zeroing + training-exclusion) but
# carries a DISTINCT match_rejection_reason (_IRRIGATION_XTALK_REASON) so:
#   • it never pollutes the long-no-flow cross-talk calibration (uc=0 → not a
#     fit positive), and
#   • _finalize_derived_verdicts can PRESERVE it across a reprocess (the main-only
#     _detect_cross_talk can't reproduce a short event), instead of clobbering it.
#
# All four thresholds are FROZEN module constants (none are _ac-calibratable). The
# volume cap is the hard "never make real water invisible" guard — a draw above it
# is never zeroed regardless of ratio. The ratio is frozen for v1: the verdict is
# automatic (uc=0) so there are no user-confirmed positives to fit it from.
_XTALK_IRR_MAX_VOLUME_L:      float = 1.5   # hard safety cap — never zero a larger draw
_XTALK_IRR_MIN_MAIN_DELTA_PSI: float = 2.0  # need a real main pressure swing to compare
_XTALK_IRR_PRESSURE_RATIO:    float = 1.3   # irrigation swing ≥ 1.3× main swing
_XTALK_IRR_MIN_FLOW_LPM:      float = 5.0   # irrigation "running" flow floor (interval build)
_IRRIGATION_XTALK_REASON: str = "irrigation_cross_talk"
# The "brief use, long idle tail" inflated-envelope reason. ONE constant for every
# writer/matcher (finalizer, sparse-reprocess scan, capped re-include, auto-split
# candidate query) — the dev6 bug was one predicate not knowing this string.
SPARSE_ENVELOPE_REASON: str = "sparse_envelope"


# ── Per-home artifact-detector calibration (Phase 2.4) ──────────────────────────
# A frozen per-home calib (artifact_calibration.py) may override ONLY the cross-talk
# min-duration and the dribble IDENTIFIER thresholds below. Two sets stay FROZEN /
# never calibratable and so are absent from ARTIFACT_DEFAULTS:
#   • the phantom duration floors (_PHANTOM_MIN_DURATION_S / _PHANTOM_NOFLOW_MIN_DURATION_S) —
#     structural constants since 2026-06-14 (the legacy floor must never be lowerable, else a
#     no-metrics legacy event could be zeroed on duration+ΔP alone);
#   • the leak-safety guards (_PHANTOM_MAX_TRUE_FLOW_LPM brief-burst guard,
#     _PHANTOM_MAX_FLOW_INTEGRAL_L / _PHANTOM_MAX_FLOW_ON_RATIO no-flow ceilings) — a real leak
#     moves water and is excluded by them regardless of any duration/ΔP tuning.
# (XTALK_MIN_DURATION_S stays calibratable: cross-talk ALWAYS requires the no-flow metrics, so a
# lowered floor still demands no-flow proof — no legacy hazard.) ARTIFACT_DEFAULTS is the single
# source of truth (artifact_calibration imports it).
ARTIFACT_DEFAULTS: Dict[str, float] = {
    "PHANTOM_MAX_DELTA_PSI":  _PHANTOM_MAX_DELTA_PSI,
    "XTALK_MIN_DURATION_S":   _XTALK_MIN_DURATION_S,
    "DRIBBLE_MAX_VOLUME_L":   _DRIBBLE_MAX_VOLUME_L,
    "DRIBBLE_MAX_FLOW_LPM":   _DRIBBLE_MAX_FLOW_LPM,
    "DRIBBLE_MAX_DELTA_PSI":  _DRIBBLE_MAX_DELTA_PSI,
    # dev14 rise phantom. In DEFAULTS for _ac() consistency but deliberately NOT
    # in artifact_calibration._BOUNDS (frozen v1 — the PHANTOM_MAX_DELTA_PSI
    # precedent): the validation margin to the nearest labelled real draw
    # (+0.48 vs 0.6) is too thin to hand a fit loosening rights.
    "RISE_PHANTOM_MIN_CORR":  _RISE_PHANTOM_MIN_CORR,
}


def _ac(calib, key):
    """Resolve an artifact-detector threshold: the per-home calibrated value if the
    frozen ``calib`` carries it, else the shipped default. The leak-safety true-flow
    guards are intentionally absent from ARTIFACT_DEFAULTS → never overridable."""
    if calib is not None:
        v = calib.get(key)
        if v is not None:
            return v
    return ARTIFACT_DEFAULTS[key]


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

    Returns an overlap-weighted Pearson correlation in [-1, 1] between
    the head window ``values[:n]`` and the tail window
    ``values[lag:lag+n]``, where ``n = len(values) - lag``.

    The previous implementation normalised the covariance (computed
    over only the first ``n`` samples) by the variance of the FULL
    array. With unequal sample windows the result wasn't a proper
    correlation coefficient and was biased for any signal with
    non-zero DC drift in the tail.

    This version centres each window by its own mean and divides by
    ``sqrt(var_head * var_tail)`` so the result is a proper Pearson
    correlation, then multiplies by ``n / len(values)`` (an overlap
    weight). The weight preserves the older implementation's bias
    toward shorter lags — important because for a periodic signal the
    fundamental and every harmonic score equally on raw Pearson, but
    the fundamental has more overlap and should be preferred by the
    peak-pick in ``_dominant_period_s``.

    Returns 0.0 when the lag is too large to evaluate or when one of
    the windows is constant.
    """
    n = len(values) - lag
    if n <= 1:
        return 0.0

    head = values[:n]
    tail = values[lag:lag + n]

    mean_head = sum(head) / n
    mean_tail = sum(tail) / n

    ch = [v - mean_head for v in head]
    ct = [v - mean_tail for v in tail]

    var_h = sum(v * v for v in ch)
    var_t = sum(v * v for v in ct)
    denom = (var_h * var_t) ** 0.5
    if denom < 1e-9:
        return 0.0

    pearson = sum(a * b for a, b in zip(ch, ct)) / denom
    overlap_weight = n / len(values)
    return pearson * overlap_weight


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


def _detect_pressure_restoration_phantom(
    duration_s, pressure_delta_psi,
    true_avg_flow_lpm=None, flow_integral_litres=None, flow_on_ratio=None,
    calib=None,
) -> bool:
    """True when an event's duration + near-zero pressure drop AND near-zero
    real flow indicate a city-pressure restoration / oscillation artifact.

    Duration floor (FROZEN, never calibrated):
      • metrics present (flow_integral + flow_on_ratio non-None) ⇒ the no-flow proof
        makes even a 2-min event unambiguous → ``_PHANTOM_NOFLOW_MIN_DURATION_S`` (120 s);
      • legacy event (NULL metrics) ⇒ no proof, keep the conservative 30-min
        ``_PHANTOM_MIN_DURATION_S`` floor (read directly, so calib can never lower it).

    Leak-safety no-flow guards (FROZEN): ANY of flow_integral_litres /
    flow_on_ratio at-or-above its ceiling means real water moved → NOT a phantom.
    A real leak is continuous (high flow_on_ratio) so it can never be zeroed.

    Brief-burst guard: a SHORT event (< ``_PHANTOM_MIN_DURATION_S``) whose active-segment
    mean ``true_avg_flow_lpm`` reaches a real-fixture rate (``>= _PHANTOM_MAX_TRUE_FLOW_LPM``)
    is most likely a genuine brief draw caught in a long pressure window, so it is rescued.
    This guard is LIFTED at/above the 30-min floor, where the long span at < 5 % flow-on is
    itself dispositive — that is what lets long near-zero-water events through.

    Bad inputs (None / non-numeric / NaN / inf) → False. Negative delta is
    INTENTIONALLY treated as phantom (a `< 2.0` threshold includes negatives).
    """
    try:
        duration = float(duration_s)
        delta = float(pressure_delta_psi)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(duration) or not math.isfinite(delta):
        return False
    have_noflow_metrics = flow_integral_litres is not None and flow_on_ratio is not None
    floor = _PHANTOM_NOFLOW_MIN_DURATION_S if have_noflow_metrics else _PHANTOM_MIN_DURATION_S
    if not (duration >= floor and delta < _ac(calib, "PHANTOM_MAX_DELTA_PSI")):
        return False
    # No-flow leak-safety guards (FROZEN): real water moved ⇒ not a phantom.
    for val, ceil in (
        (flow_integral_litres, _PHANTOM_MAX_FLOW_INTEGRAL_L),
        (flow_on_ratio,       _PHANTOM_MAX_FLOW_ON_RATIO),
    ):
        if val is not None:
            try:
                if float(val) >= ceil:
                    return False
            except (TypeError, ValueError):
                pass
    # Brief-burst guard — only below the long-quiet regime (see docstring).
    if duration < _PHANTOM_MIN_DURATION_S and true_avg_flow_lpm is not None:
        try:
            if float(true_avg_flow_lpm) >= _PHANTOM_MAX_TRUE_FLOW_LPM:
                return False
        except (TypeError, ValueError):
            pass
    return True


def _detect_low_flow_dribble(volume_litres, avg_flow_lpm, pressure_delta_psi,
                             calib=None, min_flow_lpm: float = 0.15) -> bool:
    """True when an event is a brief low-flow trickle (sensor / pressure-
    equalisation noise) rather than real water use.

    Fingerprint: tiny volume AND low average flow AND near-zero pressure drop
    (all three below their thresholds). Distinct from the long-duration
    pressure-restoration phantom — these are short blips. Calibrated from the
    May-2026 labelled export (see the constants above).

    Returns False if ANY input is None / non-numeric / non-finite. That keeps
    the verdict conservative: a parse error never excludes a real event, and a
    legacy caller that passes nothing (e.g. the phantom-only repair path) gets
    a clean False rather than a spurious dribble flag.
    """
    if volume_litres is None or avg_flow_lpm is None or pressure_delta_psi is None:
        return False
    try:
        vol = float(volume_litres)
        flow = float(avg_flow_lpm)
        delta = float(pressure_delta_psi)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(vol) and math.isfinite(flow) and math.isfinite(delta)):
        return False
    # Coarse-meter safety: a meter that reliably measures down to min_flow_lpm
    # (high resolution floor) produces real low-flow readings, not the quantization
    # noise this heuristic suppresses — never dribble-zero on such a meter.
    if min_flow_lpm >= _DRIBBLE_RELIABLE_METER_FLOOR_LPM:
        return False
    return (
        vol < _ac(calib, "DRIBBLE_MAX_VOLUME_L")
        and flow < _ac(calib, "DRIBBLE_MAX_FLOW_LPM")
        and delta < _ac(calib, "DRIBBLE_MAX_DELTA_PSI")
    )


def _detect_rising_pressure_phantom(duration_s, volume_litres,
                                    flow_pressure_corr, calib=None) -> bool:
    """True when a SHORT small burst's flow TRACKED a pressure RISE — i.e. the
    turbine was spun by climbing supply pressure, not by demand (dev14).

    Fingerprint: flow↔pressure correlation at/above the validated cutoff
    (``RISE_PHANTOM_MIN_CORR``; a real draw is strongly NEGATIVE), volume
    STRICTLY under ``_RISE_PHANTOM_MAX_VOLUME_L`` (frozen — keeps every zeroed
    event below detector_validation's SUSPECT_ZERO_LITRES leak bar), and
    duration at/under ``_RISE_PHANTOM_MAX_DURATION_S`` (frozen — the long
    phantom owns >= 120 s).

    Bad/missing inputs (None / non-numeric / NaN / inf) → False: an event with
    no computable correlation keeps its water. Zero/negative volume → False
    (nothing to remove — don't claim identity over a no-water row).
    """
    if duration_s is None or volume_litres is None or flow_pressure_corr is None:
        return False
    try:
        duration = float(duration_s)
        vol = float(volume_litres)
        corr = float(flow_pressure_corr)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(duration) and math.isfinite(vol) and math.isfinite(corr)):
        return False
    return (
        corr >= _ac(calib, "RISE_PHANTOM_MIN_CORR")
        and 0.0 < vol < _RISE_PHANTOM_MAX_VOLUME_L
        and duration <= _RISE_PHANTOM_MAX_DURATION_S
    )


def _circuit_min_flow(conn, circuit: str) -> float:
    """Per-circuit meter-derived low-flow floor (60 ÷ ppl) from the cached PPL.
    Feeds the coarse-meter dribble guard on reprocess paths. Falls back to the
    396-ppl turbine floor on any error."""
    try:
        from .database import get_circuit_pulses_per_litre
        ppl = get_circuit_pulses_per_litre(conn, circuit)
        if ppl and ppl >= 1.0:
            return 60.0 / ppl
    except Exception:
        pass
    return 0.15


def _detect_cross_talk(duration_s, pressure_delta_psi,
                       flow_integral_litres, flow_on_ratio, calib=None) -> bool:
    """True when a multi-minute event registered via a REAL pressure drop but moved
    essentially no water through THIS meter — i.e. another circuit's draw pulled the
    shared-supply pressure down (cross-talk), not water use here.

    Fingerprint: long enough (>= _XTALK_MIN_DURATION_S) AND near-zero integrated volume
    (flow_integral < _PHANTOM_MAX_FLOW_INTEGRAL_L) AND flow on only a tiny fraction of
    the window (flow_on_ratio < _PHANTOM_MAX_FLOW_ON_RATIO) AND a REAL pressure drop
    (delta >= _PHANTOM_MAX_DELTA_PSI). The ΔP floor is exactly what separates this from
    the near-zero-ΔP pressure-restoration phantom (delta < _PHANTOM_MAX_DELTA_PSI), so
    the two are mutually exclusive. Reuses the phantom's already-calibrated no-flow
    ceilings.

    Returns False on any None / non-numeric / non-finite input — conservative, so a
    parse error never zeroes a real event.
    """
    if (duration_s is None or pressure_delta_psi is None
            or flow_integral_litres is None or flow_on_ratio is None):
        return False
    try:
        duration = float(duration_s)
        delta = float(pressure_delta_psi)
        integ = float(flow_integral_litres)
        onr = float(flow_on_ratio)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(duration) and math.isfinite(delta)
            and math.isfinite(integ) and math.isfinite(onr)):
        return False
    return (
        duration >= _ac(calib, "XTALK_MIN_DURATION_S")
        and integ < _PHANTOM_MAX_FLOW_INTEGRAL_L       # frozen no-flow guard
        and onr < _PHANTOM_MAX_FLOW_ON_RATIO           # frozen no-flow guard
        and delta >= _ac(calib, "PHANTOM_MAX_DELTA_PSI")
    )


def _detect_irrigation_cross_talk(volume_litres, duration_s,
                                  main_pressure_delta_psi,
                                  other_pressure_delta_psi,
                                  irrigation_active) -> bool:
    """True when a MAIN event is an irrigation zone-switch water-hammer transient,
    not real water — see the _XTALK_IRR_* constants block for the physics.

    Fingerprint (ALL required):
      • ``irrigation_active`` — the event window overlaps a run of irrigation flow;
      • ``volume_litres <= _XTALK_IRR_MAX_VOLUME_L`` — the HARD safety cap: a larger
        draw is never zeroed, whatever the ratio (protects the dawn shower / toilet
        that overlap irrigation);
      • ``main_pressure_delta_psi >= _XTALK_IRR_MIN_MAIN_DELTA_PSI`` — a real swing to
        measure the ratio against (a near-zero-ΔP blip is a dribble/phantom, handled
        elsewhere); AND
      • ``other_pressure_delta_psi >= ratio * main_pressure_delta_psi`` — the
        irrigation-branch swing dominates, the signature of a manifold transient
        rather than a main-branch draw.

    ``duration_s`` is accepted for symmetry / future use but the volume cap + ratio
    are the discriminators. Returns False on any None / non-numeric / non-finite
    input — conservative, so a parse error never zeroes a real event.
    """
    if not irrigation_active:
        return False
    if (volume_litres is None or main_pressure_delta_psi is None
            or other_pressure_delta_psi is None):
        return False
    try:
        vol = float(volume_litres)
        pmain = float(main_pressure_delta_psi)
        pother = float(other_pressure_delta_psi)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(vol) and math.isfinite(pmain) and math.isfinite(pother)):
        return False
    return (
        vol <= _XTALK_IRR_MAX_VOLUME_L
        and pmain >= _XTALK_IRR_MIN_MAIN_DELTA_PSI
        and pother >= _XTALK_IRR_PRESSURE_RATIO * pmain
    )


def _is_sparse_envelope(duration_s, flow_on_ratio, is_phantom: bool) -> bool:
    """True when a LONG event is almost entirely idle — a brief real draw plus a long
    no-flow tail the pressure-defined boundary never closed (the 37-min envelope around a
    45 s draw). Real water moved, so it is NOT a phantom and its volume is kept; but the
    envelope's duration/shape are unreliable, so the caller excludes it from training and
    gives it no fixture identity. flow_on_ratio is the discriminator: a real slow draw flows
    continuously (high ratio), so <= _SPARSE_ENVELOPE_MAX_FLOW_ON_RATIO over
    >= _SPARSE_ENVELOPE_MIN_DURATION_S is >= 90% idle. Single-sourced so the live finalizer
    and the batch reprocess never disagree on the boundary. Conservative on bad input: a
    NULL/absent ratio is 'not sparse'."""
    if is_phantom or flow_on_ratio is None or duration_s is None:
        return False
    try:
        dur = float(duration_s)
        onr = float(flow_on_ratio)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(dur) and math.isfinite(onr)):
        return False
    return (dur >= _SPARSE_ENVELOPE_MIN_DURATION_S
            and 0 < onr <= _SPARSE_ENVELOPE_MAX_FLOW_ON_RATIO)


def _cap_envelope_estimate(est: float, features: dict):
    """Cap a pulsing-supply envelope estimate against measured-flow evidence.

    Returns ``(capped_litres, diag_or_None)``. Cap base = flow_integral_litres
    when usable, else raw volume_litres; cap = max(mult × base, floor). When
    neither base exists (fully degraded capture) the estimate stands uncapped —
    better an honest estimate than a made-up clamp. diag carries the audit
    trail merged into degraded_diagnostic_json.
    """
    base = None
    for key in ("flow_integral_litres", "volume_litres"):
        v = features.get(key)
        try:
            v = float(v) if v is not None else None
        except (TypeError, ValueError):
            v = None
        if v is not None and math.isfinite(v) and v > 0:
            base = v
            break
    if base is None:
        return est, None
    cap = max(_ENVELOPE_CAP_FLOW_MULT * base, _ENVELOPE_CAP_FLOOR_L)
    if est <= cap:
        return est, None
    return cap, {"envelope_cap_applied": True,
                 "envelope_uncapped_litres": round(float(est), 3),
                 "envelope_cap_base_litres": round(base, 3)}


def _merge_degraded_diag(features: dict, extra: dict) -> None:
    """Merge keys into degraded_diagnostic_json (tolerant of absent/bad JSON)."""
    try:
        diag = json.loads(features.get("degraded_diagnostic_json") or "{}")
        if not isinstance(diag, dict):
            diag = {}
    except (TypeError, ValueError):
        diag = {}
    diag.update(extra)
    features["degraded_diagnostic_json"] = json.dumps(diag, allow_nan=False)


def _finalize_derived_verdicts(features: dict, calib=None,
                               min_flow_lpm: float = 0.15) -> None:
    """Single source of truth for the phantom verdict + its dependent fields.

    ``calib`` is the frozen per-home artifact-detector calibration (Phase 2.4) —
    overrides only the long-quiet / dribble identifier thresholds; the leak-safety
    true-flow guards are never calibrated. None → shipped defaults.

    Recomputes, IN PLACE, from the CURRENT values in ``features``:
        is_pressure_restoration_phantom, volume_litres_effective,
        volume_estimation_method, excluded_from_training, match_rejection_reason

    Idempotent — safe to call again after ``_enrich_from_waveform`` mutates
    pressure_delta_psi / peak_flow_lpm, so the stored verdict always matches the
    stored pressure (fixes the late-ESP-waveform staleness bug where a real
    long shower kept a stale phantom flag + zeroed volume).

    Skips rows the user has manually classified (``user_classified`` == 1) —
    their category flags are authoritative and must never be auto-overridden.

    Reads: duration_seconds, pressure_delta_psi, degraded_supply, is_composite,
    user_ignored, volume_litres, volume_litres_estimated.
    """
    if features.get("user_classified"):
        return  # manual classification wins — never auto-override

    # Durable irrigation zone-switch cross-talk (set out-of-band by the importer's
    # reconciliation pass). The main-only detectors below CANNOT reproduce it (it is
    # a short event identified via the IRRIGATION circuit's pressure, which this
    # function never sees), so a recompute would wrongly clear it and restore the
    # false volume. Preserve it here — unless the user has since given it a real
    # fixture type, in which case real water wins and the verdict is dropped.
    if (features.get("match_rejection_reason") == _IRRIGATION_XTALK_REASON
            and not str(features.get("user_fixture_type") or "").strip()):
        features["is_cross_talk"] = 1
        features["volume_litres_effective"] = 0.0
        features["volume_estimation_method"] = "cross_talk"
        features["excluded_from_training"] = 1
        features["match_rejection_reason"] = _IRRIGATION_XTALK_REASON
        features["is_pressure_restoration_phantom"] = 0
        features["is_low_flow_dribble"] = 0
        features["phantom_suppression_averted"] = 0
        return

    is_degraded  = bool(features.get("degraded_supply"))
    user_ignored = bool(features.get("user_ignored"))
    # A user-applied fixture type means "confirmed real water" (mirrors
    # artifact_calibration._is_real_label). The VOLUME-ZEROING verdicts (phantom,
    # cross-talk) must never override it — so they are gated off below. Dribble /
    # degraded (non-zeroing) are left as-is.
    has_user_type = bool(str(features.get("user_fixture_type") or "").strip())
    # is_composite is now a DIAGNOSTIC-only signal (deprecated 2026-06-04): it no
    # longer excludes the event from training or sets a rejection reason. Combined
    # usage is classified as the dominant fixture (or 'other') by the k-NN.
    is_phantom = (
        not has_user_type
        and _detect_pressure_restoration_phantom(
            features.get("duration_seconds"), features.get("pressure_delta_psi"),
            true_avg_flow_lpm=features.get("true_avg_flow_lpm"),
            flow_integral_litres=features.get("flow_integral_litres"),
            flow_on_ratio=features.get("flow_on_ratio"), calib=calib)
    )
    # Suppression-averted backstop (Phase 2b): a would-be phantom carrying a
    # LARGE measured volume is never silently zeroed. This closes the
    # NULL-metrics hole (legacy/import events skip the frozen no-flow guards
    # entirely) — the volume is KEPT (falls through to the raw/degraded branch
    # below) and score_event_anomaly surfaces it as 'suppression_averted' for
    # review. Excluded from training until the user weighs in.
    phantom_averted = False
    if is_phantom:
        try:
            _measured_l = float(features.get("volume_litres") or 0.0)
        except (TypeError, ValueError):
            _measured_l = 0.0
        if _measured_l >= _PHANTOM_REVIEW_FLAG_LITRES:
            is_phantom = False
            phantom_averted = True
    # Rising-pressure phantom (dev14): a SHORT small burst whose flow TRACKED a
    # city-pressure RISE (positive flow↔pressure correlation) — the turbine spun
    # on climbing supply pressure, not demand. Same zeroing family as the long
    # phantom (shares the flag/method; distinct match_rejection_reason keeps the
    # provenance). Gated off degraded (pressure unreliable) and user labels like
    # every zeroing verdict; a None correlation can never fire it.
    is_rise_phantom = (
        not is_phantom and not is_degraded and not has_user_type
        and _detect_rising_pressure_phantom(
            features.get("duration_seconds"), features.get("volume_litres"),
            features.get("flow_pressure_corr"), calib=calib)
    )
    # Low-flow dribble: only for events that are NOT a phantom and NOT degraded
    # (a degraded event's low flow is a measurement artifact, not a true
    # trickle, and is already excluded). Folds into the exclusion set WITHOUT
    # zeroing volume — see the constants block.
    # Cross-talk: a long no-flow event with a REAL pressure drop (ΔP >= 2.0) — the
    # other circuit's draw pulled this circuit's pressure down. ΔP-exclusive with the
    # phantom (ΔP < 2.0). Gated off degraded events (their flow metrics are unreliable,
    # so the no-flow signal can't be trusted). Zeroes volume + excludes, like a phantom.
    is_cross_talk = (
        not is_phantom and not is_rise_phantom and not is_degraded
        and not has_user_type
        and _detect_cross_talk(
            features.get("duration_seconds"), features.get("pressure_delta_psi"),
            features.get("flow_integral_litres"), features.get("flow_on_ratio"),
            calib=calib)
    )
    is_dribble = (
        not is_phantom and not is_rise_phantom and not is_cross_talk
        and not is_degraded
        and _detect_low_flow_dribble(
            features.get("volume_litres"), features.get("avg_flow_lpm"),
            features.get("pressure_delta_psi"), calib=calib,
            min_flow_lpm=min_flow_lpm)
    )

    raw = float(features.get("volume_litres") or 0.0)
    est = features.get("volume_litres_estimated")
    est = float(est) if est is not None else raw

    # Effective volume: phantom → 0 (false water); cross-talk → 0; degraded →
    # envelope estimate; low-flow dribble → 0; else raw. Phantom takes precedence
    # over degraded. A dribble (brief sub-0.5 L / sub-1 L·min⁻¹ blip at ~0 ΔP) is
    # sensor / pressure-equalisation noise, not real use, so its volume is removed
    # from totals like a phantom's — and the flow<1 L·min⁻¹ gate keeps real small
    # draws (icemaker / fridge dispenser run ~3 L·min⁻¹) out of this branch. A
    # sustained slow flow accumulates past 0.5 L and never reaches here (it stays a
    # counted long event), so this cannot silently zero a continuous leak.
    if is_phantom or is_rise_phantom:
        features["volume_litres_effective"]  = 0.0
        features["volume_estimation_method"] = "pressure_restoration_phantom"
    elif is_cross_talk:
        features["volume_litres_effective"]  = 0.0
        features["volume_estimation_method"] = "cross_talk"
    elif is_degraded:
        # Phase 2a: cap the envelope estimate against measured-flow evidence
        # (measured 2.86x inflation uncapped). The cap decision is audited in
        # degraded_diagnostic_json so a clamped event is explainable.
        _capped, _cap_diag = _cap_envelope_estimate(est, features)
        features["volume_litres_effective"]  = round(_capped, 3)
        features["volume_estimation_method"] = "pulsing_supply_envelope"
        if _cap_diag:
            _merge_degraded_diag(features, _cap_diag)
    elif is_dribble:
        features["volume_litres_effective"]  = 0.0
        features["volume_estimation_method"] = "low_flow_dribble"
    else:
        features["volume_litres_effective"]  = round(raw, 3)
        features["volume_estimation_method"] = "raw"

    # Fix 3 — 'capped' only means a sample gap may have UNDER-counted volume; the event is
    # real and well-shaped, so keep its identity (the 27.7-gal shower that was wrongly greyed
    # out). Only a genuinely 'degraded' integration is unusable for training. NULL = unknown/
    # legacy = treated as not-unusable (unchanged: the prior guard also listed None).
    integration_unusable = features.get("integration_quality") == "degraded"
    # Fix 4 — a long, almost-entirely-idle envelope (a brief draw + a long no-flow tail the
    # boundary never closed). Real water moved (NOT a phantom, so the raw-volume branch above
    # is kept), but duration/shape are unreliable: out of training, no identity. A short event
    # or a continuously-flowing draw (high on-ratio, or a NULL/legacy ratio) is exempt.
    is_sparse_envelope = _is_sparse_envelope(
        features.get("duration_seconds"), features.get("flow_on_ratio"),
        is_phantom or is_rise_phantom)
    features["is_pressure_restoration_phantom"] = (
        1 if (is_phantom or is_rise_phantom) else 0)
    features["is_cross_talk"] = 1 if is_cross_talk else 0
    features["is_low_flow_dribble"] = 1 if is_dribble else 0
    # Kept-but-questioned draw: volume kept (raw/degraded branch above), out of
    # training until reviewed, surfaced via score_event_anomaly.
    features["phantom_suppression_averted"] = 1 if phantom_averted else 0
    features["excluded_from_training"] = (
        1 if (is_degraded or is_phantom or is_rise_phantom or is_cross_talk
              or is_dribble or user_ignored or integration_unusable
              or is_sparse_envelope or phantom_averted)
        else 0
    )
    # Upstream rejection reason (cluster-engine reasons are written separately).
    # is_low_flow_dribble is the authoritative dribble state; this reason string
    # is a secondary signal kept consistent with the live finalizer. The rise
    # phantom shares the flag but keeps its own reason — the only place its
    # provenance is recorded.
    features["match_rejection_reason"] = (
        "pressure_restoration_phantom" if is_phantom
        else RISE_PHANTOM_REASON if is_rise_phantom
        else "cross_talk" if is_cross_talk
        else "pulsing_supply" if is_degraded
        else "low_flow_dribble" if is_dribble
        else SPARSE_ENVELOPE_REASON if is_sparse_envelope
        else None
    )


def repair_artifact_flag_consistency(conn: sqlite3.Connection) -> dict:
    """One-time idempotent repair of cross-cutting artifact-flag invariants (P2).

    Two fixes, both safe to re-run:
      A. Any row with a volume-ZEROING flag (phantom or cross-talk) set must have
         ``excluded_from_training = 1`` — repairs rows where a recompute path left a
         zeroed event still feeding training (the is_cross_talk=1 / excluded=0 case).
      B. A row must not carry more than one of the mutually-exclusive verdict flags
         {phantom, cross_talk, dribble}. Stale auto-flags can be left set under a later
         manual classification. Resolve by the row's RECORDED EFFECT — its stored
         ``volume_litres_effective`` — rather than guessing intent:
           veff == 0                       -> a zeroing verdict (phantom / cross_talk /
                                              dribble — all three now zero volume) was
                                              operative. veff alone can't say which, so
                                              pick by the user's match_rejection_reason
                                              (manual rows) or by RE-RUNNING the live
                                              detectors (auto rows), priority
                                              phantom > cross_talk > dribble — never the
                                              stale flag bits, so a fossil bit can't win
                                              over the current verdict.
           veff == volume_litres_estimated -> degraded was operative
           veff == volume_litres (raw)     -> a pre-zeroing dribble (or none) was operative
         Keep the flag that matches that effect and clear the others (and recompute
         excluded_from_training + match_rejection_reason to suit). A row whose veff
         matches no branch is left UNTOUCHED and logged for manual review.

    Column-guarded so it is a no-op on a schema predating is_cross_talk /
    is_low_flow_dribble. Returns
    ``{"excluded_fixed", "pairs_resolved", "unresolved"}``.
    """
    if not (_events_has_column(conn, "is_cross_talk")
            and _events_has_column(conn, "is_low_flow_dribble")):
        return {"excluded_fixed": 0, "pairs_resolved": 0, "unresolved": 0}

    EPS = 1e-6

    # ── A: a zeroing flag ⇒ excluded_from_training = 1 ──────────────────────────
    cur = conn.execute(
        "UPDATE events SET excluded_from_training = 1 "
        "WHERE (COALESCE(is_pressure_restoration_phantom,0) = 1 "
        "       OR COALESCE(is_cross_talk,0) = 1) "
        "  AND COALESCE(excluded_from_training,0) = 0"
    )
    excluded_fixed = cur.rowcount or 0

    # ── B: resolve mutually-exclusive flag collisions by recorded effect ────────
    # flow_pressure_corr is column-guarded (added 20260554, after ct/dr in a
    # sequential upgrade) — without it the rise re-detect below simply can't fire.
    has_corr = _events_has_column(conn, "flow_pressure_corr")
    corr_select = ", flow_pressure_corr" if has_corr else ""
    rows = conn.execute(
        "SELECT id, user_ignored AS ui, COALESCE(user_classified,0) AS uc, "
        "  match_rejection_reason AS mrr, "
        "  COALESCE(is_pressure_restoration_phantom,0) AS ph, "
        "  COALESCE(is_cross_talk,0) AS ct, "
        "  COALESCE(is_low_flow_dribble,0) AS dr, "
        "  COALESCE(degraded_supply,0) AS dg, "
        "  duration_seconds, pressure_delta_psi, true_avg_flow_lpm, "
        "  flow_integral_litres, flow_on_ratio, avg_flow_lpm, "
        "  volume_litres, volume_litres_estimated, volume_litres_effective AS veff"
        + corr_select +
        " FROM events "
        "WHERE (COALESCE(is_pressure_restoration_phantom,0) "
        "     + COALESCE(is_cross_talk,0) "
        "     + COALESCE(is_low_flow_dribble,0)) >= 2"
    ).fetchall()

    pairs_resolved = 0
    unresolved = 0
    for r in rows:
        veff, raw, est = r["veff"], r["volume_litres"], r["volume_litres_estimated"]
        if veff is not None and abs(float(veff)) < EPS:
            # All three zeroing verdicts (phantom / cross-talk / dribble) record
            # veff==0, so the stored effect can't disambiguate them.
            if r["uc"]:
                # Manual classification is authoritative — never re-derive it. The
                # user's verdict is in match_rejection_reason; honour it, falling back
                # to flag-priority only if it names no zeroing verdict.
                mrr = r["mrr"]
                keep = ("phantom"    if mrr == "pressure_restoration_phantom"
                        else "rise"       if mrr == RISE_PHANTOM_REASON
                        else "cross_talk" if mrr == "cross_talk"
                        else "dribble"    if mrr == "low_flow_dribble"
                        else "phantom"    if r["ph"]
                        else "cross_talk" if r["ct"]
                        else "dribble")
            else:
                # Auto row: keep the verdict the CURRENT detectors produce (priority
                # phantom > rise > cross-talk > dribble), not the fossilised flag
                # bits — a stale bit can never win over the live verdict. calib=None
                # (shipped defaults) is enough to pick the category for a rare
                # collision row.
                if _detect_pressure_restoration_phantom(
                        r["duration_seconds"], r["pressure_delta_psi"],
                        true_avg_flow_lpm=r["true_avg_flow_lpm"],
                        flow_integral_litres=r["flow_integral_litres"],
                        flow_on_ratio=r["flow_on_ratio"]):
                    keep = "phantom"
                elif has_corr and _detect_rising_pressure_phantom(
                        r["duration_seconds"], r["volume_litres"],
                        r["flow_pressure_corr"]):
                    keep = "rise"
                elif _detect_cross_talk(
                        r["duration_seconds"], r["pressure_delta_psi"],
                        r["flow_integral_litres"], r["flow_on_ratio"]):
                    keep = "cross_talk"
                elif _detect_low_flow_dribble(
                        r["volume_litres"], r["avg_flow_lpm"],
                        r["pressure_delta_psi"]):
                    keep = "dribble"
                else:
                    # veff==0 but no current detector fires (stale/legacy zeroing):
                    # keep the highest-priority flag actually set, deterministically.
                    keep = ("phantom" if r["ph"]
                            else "cross_talk" if r["ct"] else "dribble")
        elif (r["dg"] and est is not None and veff is not None
              and abs(float(veff) - float(est)) < EPS):
            # Envelope estimate AND the event is actually degraded — degraded won.
            # (The dg requirement prevents a clean row where est==raw==veff from being
            # mis-read as degraded by the estimate-match alone.)
            keep = "degraded"
        elif (raw is not None and veff is not None
              and abs(float(veff) - float(raw)) < EPS):
            keep = "dribble" if r["dr"] else "none"
        else:
            unresolved += 1
            log.warning("flag-repair: event %s contradictory flags "
                        "(ph=%d ct=%d dr=%d) but veff=%s matches no verdict effect "
                        "(raw=%s est=%s) — left for manual review",
                        r["id"], r["ph"], r["ct"], r["dr"], veff, raw, est)
            continue

        new_ph = 1 if keep in ("phantom", "rise") else 0
        new_ct = 1 if keep == "cross_talk" else 0
        new_dr = 1 if keep == "dribble" else 0
        new_excluded = 1 if (new_ph or new_ct or new_dr or r["dg"] or r["ui"]) else 0
        reason = (RISE_PHANTOM_REASON if keep == "rise"
                  else "pressure_restoration_phantom" if new_ph
                  else "cross_talk" if new_ct
                  else "pulsing_supply" if r["dg"]
                  else "low_flow_dribble" if new_dr
                  else None)
        conn.execute(
            "UPDATE events SET is_pressure_restoration_phantom = ?, "
            "  is_cross_talk = ?, is_low_flow_dribble = ?, "
            "  excluded_from_training = ?, match_rejection_reason = ? WHERE id = ?",
            (new_ph, new_ct, new_dr, new_excluded, reason, r["id"]),
        )
        pairs_resolved += 1

    if excluded_fixed or pairs_resolved or unresolved:
        conn.commit()
        log.info("flag-repair: %d excluded-from-training fixed, %d flag collisions "
                 "resolved, %d unresolved", excluded_fixed, pairs_resolved, unresolved)
    return {"excluded_fixed": excluded_fixed, "pairs_resolved": pairs_resolved,
            "unresolved": unresolved}


def reprocess_event_exclusion_verdicts(conn: sqlite3.Connection) -> dict:
    """Recompute the auto exclusion verdicts over all events.

    Two independent scans, in order:
      1. Pressure-restoration phantoms (long duration + near-zero ΔP) — flag
         them, ZERO volume_litres_effective, mark excluded_from_training, and
         reverse any prior hourly_volume contribution so daily totals shed the
         false volume.
      2. Low-flow dribbles (brief low-flow / low-volume / near-zero ΔP) — flag
         is_low_flow_dribble + excluded_from_training, ZERO volume_litres_effective,
         and reverse any prior hourly_volume contribution (same shape as scan 1):
         a dribble is sensor / pressure-equalisation noise, removed from totals.

    Both scans skip ``user_classified`` rows (manual classification wins) and
    are idempotent — already-flagged events are excluded by their WHERE clauses.

    The dribble scan is guarded by a column-existence check so this stays safe
    to call from the phantom-only wrapper during a sequential upgrade where the
    is_low_flow_dribble column hasn't been added yet (caution A: the migration
    only adds DDL; this backfill runs from startup / import / manual paths).

    Returns {"flagged": <phantoms>, "dribbles_flagged": <dribbles>}.

    Derived-stats notes (phantom scan):
      • Volume aggregates — hourly_volume and daily_summary.total_volume_litres
        — are both corrected here (hourly_volume via the reversal INSERT,
        daily_summary via a recompute of each affected day). Both now read
        volume_litres_effective, which is zeroed for phantoms.
      • fixture_type_signatures: upsert_fixture_signature already excludes
        excluded_from_training=1 rows, so flagged phantoms can never pollute a
        signature centroid; no rebuild needed.
      • Cluster centroids: maintained as an incremental running mean. NEW
        phantoms are gated out of match_and_learn by excluded_from_training
        before they ever reach the clusterer, so they never contribute. Any
        pre-existing phantom already folded into a centroid is left in place —
        cluster_id is intentionally NOT nulled (preserving existing
        assignments), and the residual influence of at most a handful of
        extreme outliers is negligible and not worth a full retrain. This is a
        deliberate scope decision, not an oversight.
    """
    from .database import transaction, compute_daily_summary, apply_effective_volume

    # Repair any cross-cutting flag-consistency violations first (P2): zeroed events
    # that slipped through still feeding training, and stale mutually-exclusive flags.
    repair = repair_artifact_flag_consistency(conn)

    # Skip manually-classified rows so a startup re-run never re-flags an event
    # the user deliberately un-marked. Column-guarded because the back-compat
    # wrapper is called from the 20260532 migration, before user_classified
    # exists in a sequential upgrade.
    uc_guard = (
        " AND COALESCE(user_classified, 0) = 0"
        if _events_has_column(conn, "user_classified") else ""
    )
    # The hardened phantom guard needs the active-flow features so a REAL
    # high-flow low-ΔP run (e.g. irrigation at ΔP ~1.2) is never zeroed. Without
    # them this scan re-applies the bare duration+ΔP rule on every startup and
    # re-zeroes real runs — even ones a Recompute just restored. Column-guarded
    # because this backfill also runs from an early migration, before the
    # 20260536 active-flow columns exist in a sequential upgrade.
    has_af = _events_has_column(conn, "true_avg_flow_lpm")
    af_select = (", true_avg_flow_lpm, flow_integral_litres, flow_on_ratio"
                 if has_af else "")
    # A user-applied fixture type means confirmed-real water — never auto-zero it
    # (mirrors _is_real_label + the live finalizer's has_user_type gate).
    uft_guard = (
        " AND (user_fixture_type IS NULL OR user_fixture_type = '')"
        if _events_has_column(conn, "user_fixture_type") else ""
    )
    # Duration prefilter is metric-gated: legacy rows (no active-flow metrics) stay at the
    # frozen 1800 s floor, while rows that HAVE the no-flow metrics also qualify from 120 s.
    # The canonical _detect_pressure_restoration_phantom re-runs inside the loop and does the
    # real gating, so legacy rows pulled in at 120 s are correctly rejected (they fall back to
    # the 1800 s floor there) — no flip-flop.
    if has_af:
        dur_clause = (
            "(duration_seconds >= ? "
            "   OR (duration_seconds >= ? AND flow_integral_litres IS NOT NULL "
            "       AND flow_on_ratio IS NOT NULL))"
        )
        dur_params = (_PHANTOM_MIN_DURATION_S, _PHANTOM_NOFLOW_MIN_DURATION_S)
    else:
        dur_clause = "duration_seconds >= ?"
        dur_params = (_PHANTOM_MIN_DURATION_S,)
    rows = conn.execute(
        "SELECT id, circuit, start_ts, duration_seconds, pressure_delta_psi, "
        "       hourly_volume_applied_litres, hourly_volume_applied_bucket"
        + af_select +
        " FROM events "
        "WHERE " + dur_clause +
        "  AND pressure_delta_psi < ? "
        "  AND (is_pressure_restoration_phantom = 0 "
        "       OR is_pressure_restoration_phantom IS NULL)"
        + uc_guard + uft_guard,
        dur_params + (_PHANTOM_MAX_DELTA_PSI,),
    ).fetchall()

    flagged = 0
    affected_days: set = set()   # (circuit, 'YYYY-MM-DD') to recompute
    for row in rows:
        # Re-run the canonical detector rather than trusting the SQL filter
        # alone — keeps the threshold logic in one place and guards bad data.
        if not _detect_pressure_restoration_phantom(
            row["duration_seconds"], row["pressure_delta_psi"],
            true_avg_flow_lpm=(row["true_avg_flow_lpm"] if has_af else None),
            flow_integral_litres=(row["flow_integral_litres"] if has_af else None),
            flow_on_ratio=(row["flow_on_ratio"] if has_af else None),
        ):
            continue

        with transaction(conn):
            conn.execute(
                "UPDATE events SET "
                "  is_pressure_restoration_phantom = 1, "
                "  is_cross_talk = 0, is_low_flow_dribble = 0, "   # phantom has precedence
                "  volume_litres_effective = 0, "
                "  volume_estimation_method = 'pressure_restoration_phantom', "
                "  excluded_from_training = 1, "
                "  match_rejection_reason = 'pressure_restoration_phantom' "
                "WHERE id = ?",
                (row["id"],),
            )
            # §2.5 — zero the ledger contribution via the one chokepoint.
            apply_effective_volume(conn, row["id"], row["circuit"], row["start_ts"], 0)

        flagged += 1
        day = (row["start_ts"] or "")[:10]   # UTC date portion of ISO ts
        if day:
            affected_days.add((row["circuit"], day))
        log.info(
            "phantom-reprocess: event %s flagged (duration=%.0fs ΔP=%.2f); "
            "reversed %.3f L from hourly bucket %s",
            row["id"], row["duration_seconds"] or 0.0,
            row["pressure_delta_psi"] or 0.0,
            float(row["hourly_volume_applied_litres"] or 0.0),
            row["hourly_volume_applied_bucket"],
        )

    # Recompute the daily_summary for every affected day so the History
    # charts/totals shed the false volume immediately (compute_daily_summary
    # now reads volume_litres_effective, which we just zeroed). hourly_volume
    # was already corrected by the per-event reversal above.
    for circ, day in affected_days:
        compute_daily_summary(conn, circ, day)
    if affected_days:
        conn.commit()

    if flagged:
        log.info("phantom-reprocess: flagged %d event(s) total across %d day(s)",
                 flagged, len(affected_days))

    # ── Scan 2: low-flow dribbles ────────────────────────────────────────────
    # ZEROES volume + excludes, reversing any prior hourly contribution via the
    # §2.5 chokepoint — same shape as the cross-talk scan below. A brief sub-0.5 L
    # / sub-1 L·min⁻¹ blip at ~0 ΔP is sensor / pressure-equalisation noise, not
    # real use, so it is removed from totals like a phantom. The flow<1 L·min⁻¹
    # gate keeps real small draws (icemaker / dispenser ~3 L·min⁻¹) counted, and a
    # sustained slow flow exceeds 0.5 L so it never matches here (it stays a counted
    # long event) — a continuous leak cannot be silently zeroed. ΔP<1.5 also makes
    # this disjoint from cross-talk (ΔP>=2.0). Guarded by the column check so the
    # phantom-only wrapper stays safe mid-upgrade (column added later in the chain).
    #
    # The row filter selects an event when it is EITHER not yet flagged a dribble OR
    # flagged but still carrying volume (volume_litres_effective > 0). The second arm
    # is the one-time self-healing backfill for the 2026-06-19 semantics change
    # (dribble used to keep its volume): on the next startup reprocess, every
    # already-flagged dribble gets its volume zeroed + ledger reversed. Idempotent —
    # once veff is 0 the row no longer matches, so re-runs are no-ops.
    dribbles_flagged = 0
    dr_days: set = set()
    if _events_has_column(conn, "is_low_flow_dribble"):
        drows = conn.execute(
            "SELECT id, circuit, start_ts, duration_seconds, pressure_delta_psi, "
            "       volume_litres, avg_flow_lpm "
            "FROM events "
            "WHERE (is_low_flow_dribble = 0 OR is_low_flow_dribble IS NULL "
            "       OR COALESCE(volume_litres_effective, volume_litres, 0) > 0) "
            "  AND COALESCE(user_classified, 0) = 0 "
            "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
            "  AND COALESCE(is_cross_talk, 0) = 0 "
            "  AND COALESCE(degraded_supply, 0) = 0 "
            "  AND volume_litres < ? AND avg_flow_lpm < ? "
            "  AND pressure_delta_psi < ?",
            (_DRIBBLE_MAX_VOLUME_L, _DRIBBLE_MAX_FLOW_LPM, _DRIBBLE_MAX_DELTA_PSI),
        ).fetchall()
        for row in drows:
            # Re-run the canonical detector (SQL is only a prefilter).
            if not _detect_low_flow_dribble(
                row["volume_litres"], row["avg_flow_lpm"], row["pressure_delta_psi"],
                min_flow_lpm=_circuit_min_flow(conn, row["circuit"])
            ):
                continue
            with transaction(conn):
                conn.execute(
                    "UPDATE events SET "
                    "  is_low_flow_dribble = 1, "
                    "  volume_litres_effective = 0, "
                    "  volume_estimation_method = 'low_flow_dribble', "
                    "  excluded_from_training = 1, "
                    "  match_rejection_reason = 'low_flow_dribble' "
                    "WHERE id = ?",
                    (row["id"],),
                )
                # §2.5 — zero the ledger contribution via the one chokepoint.
                apply_effective_volume(conn, row["id"], row["circuit"],
                                       row["start_ts"], 0)
            dribbles_flagged += 1
            day = (row["start_ts"] or "")[:10]
            if day:
                dr_days.add((row["circuit"], day))
        for circ, day in dr_days:
            compute_daily_summary(conn, circ, day)
        if dribbles_flagged:
            conn.commit()
            log.info("dribble-reprocess: flagged %d low-flow dribble event(s) "
                     "across %d day(s)", dribbles_flagged, len(dr_days))

    # ── Scan 3: cross-talk (no real flow + a REAL pressure drop ≥ 2.0) ────────
    # Another circuit's draw pulled this circuit's pressure down — registered but
    # no water through this meter. ZEROES volume (like a phantom) + excludes, and
    # reverses any prior hourly_volume contribution. Excludes already-phantom rows
    # so a row is never double-zeroed (auto-phantoms have ΔP<2 and never match this
    # rule anyway, and the reversal is idempotent on an already-zeroed bucket).
    cross_talk_flagged = 0
    xt_days: set = set()
    if _events_has_column(conn, "is_cross_talk"):
        xrows = conn.execute(
            "SELECT id, circuit, start_ts, duration_seconds, pressure_delta_psi, "
            "       flow_integral_litres, flow_on_ratio, "
            "       hourly_volume_applied_litres, hourly_volume_applied_bucket "
            "FROM events "
            "WHERE (is_cross_talk = 0 OR is_cross_talk IS NULL) "
            "  AND COALESCE(user_classified, 0) = 0 "
            "  AND (user_fixture_type IS NULL OR user_fixture_type = '') "
            "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
            "  AND COALESCE(degraded_supply, 0) = 0 "
            "  AND duration_seconds >= ? "
            "  AND flow_integral_litres < ? AND flow_on_ratio < ? "
            "  AND pressure_delta_psi >= ?",
            (_XTALK_MIN_DURATION_S, _PHANTOM_MAX_FLOW_INTEGRAL_L,
             _PHANTOM_MAX_FLOW_ON_RATIO, _PHANTOM_MAX_DELTA_PSI),
        ).fetchall()
        for row in xrows:
            # Re-run the canonical detector (SQL is only a prefilter).
            if not _detect_cross_talk(
                row["duration_seconds"], row["pressure_delta_psi"],
                row["flow_integral_litres"], row["flow_on_ratio"]
            ):
                continue
            with transaction(conn):
                conn.execute(
                    "UPDATE events SET "
                    "  is_cross_talk = 1, is_low_flow_dribble = 0, "   # xtalk has precedence
                    "  volume_litres_effective = 0, "
                    "  volume_estimation_method = 'cross_talk', "
                    "  excluded_from_training = 1, "
                    "  match_rejection_reason = 'cross_talk' "
                    "WHERE id = ?",
                    (row["id"],),
                )
                # §2.5 — zero the ledger contribution via the one chokepoint.
                apply_effective_volume(conn, row["id"], row["circuit"],
                                       row["start_ts"], 0)
            cross_talk_flagged += 1
            day = (row["start_ts"] or "")[:10]
            if day:
                xt_days.add((row["circuit"], day))
        for circ, day in xt_days:
            compute_daily_summary(conn, circ, day)
        if cross_talk_flagged:
            conn.commit()
            log.info("cross-talk-reprocess: flagged %d event(s) across %d day(s)",
                     cross_talk_flagged, len(xt_days))

    # ── Scan 3b: rising-pressure phantoms (dev14) ─────────────────────────────
    # Applies the corr-gated verdict wherever a stored flow_pressure_corr exists
    # (new events store it at extraction; historical events get it from the
    # rise_corr_backfill worker; late ESP waveforms may refresh it). Runs from
    # every existing caller of this function, so late-waveform verdict drift and
    # backfilled corrs reconcile on the same cadence as the other scans.
    rise = reprocess_rising_pressure_phantoms(conn)

    # ── Scan 4: sparse envelopes (Fix 4) ─────────────────────────────────────
    # A long event almost entirely idle (a brief draw + a long no-flow tail). Real water
    # moved (NOT a phantom — volume is PRESERVED, so no hourly/daily resync, like the
    # dribble scan), but the envelope is unreliable: exclude from training, no identity.
    # Needs flow_on_ratio (an active-flow column) → has_af. Only claims rows with NO
    # existing artifact reason (NULL) so it never overrides a phantom/cross-talk/dribble
    # verdict — matching the live finalizer's match_rejection_reason precedence.
    sparse_flagged = 0
    if has_af:
        sprows = conn.execute(
            "SELECT id, duration_seconds, flow_on_ratio FROM events "
            "WHERE duration_seconds >= ? "
            "  AND flow_on_ratio > 0 AND flow_on_ratio <= ? "
            "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
            "  AND match_rejection_reason IS NULL"
            + uc_guard + uft_guard,
            (_SPARSE_ENVELOPE_MIN_DURATION_S, _SPARSE_ENVELOPE_MAX_FLOW_ON_RATIO),
        ).fetchall()
        for row in sprows:
            # Re-run the canonical predicate (SQL is only a prefilter) — single-sourced
            # with the live finalizer so the two paths never disagree on the boundary.
            if not _is_sparse_envelope(row["duration_seconds"], row["flow_on_ratio"],
                                       False):
                continue
            conn.execute(
                "UPDATE events SET excluded_from_training = 1, "
                "  match_rejection_reason = ? "
                "WHERE id = ?",
                (SPARSE_ENVELOPE_REASON, row["id"]),
            )
            sparse_flagged += 1
        if sparse_flagged:
            conn.commit()
            log.info("sparse-envelope-reprocess: flagged %d event(s)", sparse_flagged)

    # ── Scan 5: un-exclude capped-only events (Fix 3) ────────────────────────
    # 'capped' integration only means a sample gap may have UNDER-counted volume — the
    # event is still a real, well-shaped draw and must not be excluded just for that.
    # Re-include rows excluded ONLY because of 'capped': no other artifact flag, not a
    # sparse envelope (Scan 4 may have just claimed one), not user-classified, with valid
    # active-flow features (mirrors cleanup_composite_flags' guard). 'degraded' integration
    # is untouched — it still excludes.
    capped_reincluded = 0
    if has_af and _events_has_column(conn, "integration_quality"):
        cur = conn.execute(
            "UPDATE events SET excluded_from_training = 0 "
            "WHERE integration_quality = 'capped' "
            "  AND COALESCE(excluded_from_training, 0) = 1 "
            "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
            "  AND COALESCE(is_cross_talk, 0) = 0 "
            "  AND COALESCE(is_low_flow_dribble, 0) = 0 "
            "  AND COALESCE(degraded_supply, 0) = 0 "
            "  AND COALESCE(user_ignored, 0) = 0 "
            "  AND COALESCE(user_classified, 0) = 0 "
            "  AND (match_rejection_reason IS NULL "
            "       OR match_rejection_reason <> ?) "
            "  AND true_avg_flow_lpm IS NOT NULL",
            (SPARSE_ENVELOPE_REASON,),
        )
        capped_reincluded = cur.rowcount or 0
        if capped_reincluded:
            conn.commit()
            log.info("capped-reprocess: re-included %d capped-only event(s)",
                     capped_reincluded)

    return {"flagged": flagged, "dribbles_flagged": dribbles_flagged,
            "cross_talk_flagged": cross_talk_flagged,
            "rise_flagged": rise["rise_flagged"],
            "sparse_flagged": sparse_flagged,
            "capped_reincluded": capped_reincluded,
            "excluded_fixed": repair["excluded_fixed"],
            "flag_pairs_resolved": repair["pairs_resolved"],
            "flag_pairs_unresolved": repair["unresolved"]}


def reprocess_rising_pressure_phantoms(conn: sqlite3.Connection) -> dict:
    """Apply the rising-pressure phantom verdict to stored events (dev14).

    Scans events carrying a stored ``flow_pressure_corr`` that the canonical
    ``_detect_rising_pressure_phantom`` fires on, and — mirroring the phantom /
    dribble / cross-talk scans in ``reprocess_event_exclusion_verdicts`` —
    flags them, ZEROES ``volume_litres_effective`` through the §2.5
    ``apply_effective_volume`` chokepoint (hourly ledger reversed), excludes
    them from training, and stamps ``match_rejection_reason =
    'rising_pressure_phantom'``. Affected days get their daily_summary
    recomputed.

    Guards (all mirrored from the live finalizer): user-classified rows, real
    fixture labels, degraded supply, and rows already carrying any zeroing
    verdict are never touched; the detector's own frozen caps (< 1.0 L,
    <= 120 s, corr >= 0.6) bound what can be zeroed. Column-guarded so the
    pre-20260554 back-compat wrapper path stays safe mid-upgrade. Idempotent —
    a flagged row no longer matches the WHERE. Standalone (not folded into the
    caller's loop) because the rise_corr_backfill worker also calls it directly
    after each batch of freshly computed correlations.

    Returns ``{"rise_flagged": <n>}``.
    """
    from .database import transaction, compute_daily_summary, apply_effective_volume

    if not _events_has_column(conn, "flow_pressure_corr"):
        return {"rise_flagged": 0}

    rows = conn.execute(
        "SELECT id, circuit, start_ts, duration_seconds, volume_litres, "
        "       flow_pressure_corr "
        "FROM events "
        "WHERE flow_pressure_corr IS NOT NULL "
        "  AND flow_pressure_corr >= ? "
        "  AND duration_seconds <= ? "
        "  AND volume_litres > 0 AND volume_litres < ? "
        "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
        "  AND COALESCE(is_cross_talk, 0) = 0 "
        "  AND COALESCE(is_low_flow_dribble, 0) = 0 "
        "  AND COALESCE(degraded_supply, 0) = 0 "
        "  AND COALESCE(user_classified, 0) = 0 "
        "  AND (user_fixture_type IS NULL OR user_fixture_type = '')",
        (_RISE_PHANTOM_MIN_CORR, _RISE_PHANTOM_MAX_DURATION_S,
         _RISE_PHANTOM_MAX_VOLUME_L),
    ).fetchall()

    rise_flagged = 0
    days: set = set()
    for row in rows:
        # Re-run the canonical detector (SQL is only a prefilter) — single
        # source of truth for the thresholds, and it re-rejects bad data.
        if not _detect_rising_pressure_phantom(
                row["duration_seconds"], row["volume_litres"],
                row["flow_pressure_corr"]):
            continue
        with transaction(conn):
            conn.execute(
                "UPDATE events SET "
                "  is_pressure_restoration_phantom = 1, "
                "  is_cross_talk = 0, is_low_flow_dribble = 0, "
                "  volume_litres_effective = 0, "
                "  volume_estimation_method = 'pressure_restoration_phantom', "
                "  excluded_from_training = 1, "
                "  match_rejection_reason = ? "
                "WHERE id = ?",
                (RISE_PHANTOM_REASON, row["id"]),
            )
            # §2.5 — zero the ledger contribution via the one chokepoint.
            apply_effective_volume(conn, row["id"], row["circuit"],
                                   row["start_ts"], 0)
        rise_flagged += 1
        day = (row["start_ts"] or "")[:10]
        if day:
            days.add((row["circuit"], day))
        log.info("rise-phantom-reprocess: event %s flagged "
                 "(corr=%+.2f dur=%.0fs vol=%.3f L)",
                 row["id"], row["flow_pressure_corr"] or 0.0,
                 row["duration_seconds"] or 0.0, row["volume_litres"] or 0.0)

    for circ, day in days:
        compute_daily_summary(conn, circ, day)
    if rise_flagged:
        conn.commit()
        log.info("rise-phantom-reprocess: flagged %d event(s) across %d day(s)",
                 rise_flagged, len(days))
    return {"rise_flagged": rise_flagged}


def reprocess_pressure_restoration_phantoms(conn: sqlite3.Connection) -> dict:
    """Back-compat alias for the phantom component of the exclusion reprocess.

    Retained because the 20260532 migration calls this name. Delegates to
    ``reprocess_event_exclusion_verdicts``; the dribble half is a no-op there
    when the is_low_flow_dribble column doesn't exist yet (sequential upgrade).
    """
    return reprocess_event_exclusion_verdicts(conn)


def _events_has_column(conn: sqlite3.Connection, col: str) -> bool:
    """True if the events table has ``col``. Used to make the dribble scan
    safe to call before its migration has added the column."""
    try:
        return any(r[1] == col for r in conn.execute("PRAGMA table_info(events)"))
    except sqlite3.Error:
        return False


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
    from .database import _hour_bucket_for, transaction, apply_effective_volume

    rows = conn.execute(
        "SELECT id, circuit, start_ts, degraded_supply, "
        "       degraded_diagnostic_json, volume_litres, "
        "       volume_litres_estimated, hourly_volume_applied_litres, "
        "       hourly_volume_applied_bucket, is_composite "
        "FROM events "
        "WHERE degraded_diagnostic_json IS NOT NULL "
        "  AND degraded_diagnostic_json != '' "
        # Phantom takes precedence over degraded: never let a degraded
        # re-verdict un-zero a pressure-restoration phantom's volume.
        "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0"
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
            # §2.5 — reverse/apply/bookkeep via the one chokepoint.
            apply_effective_volume(conn, row["id"], row["circuit"], row["start_ts"],
                                   new_effective)

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
    """Resample flow_readings to n points, normalize by peak (0–1).

    The series is anchored to start from no-flow: every stored event begins
    with the fixture closed, but firmware 3.13's pulse_meter publishes a full
    instantaneous rate on the first pulse period (no windowed ramp-up), and
    idle 0.0 rarely republishes so the detector's pre-trigger seed ages out —
    the raw series then OPENS at peak and the sparkline draws a vertical wall
    with the onset "clipped off". Prepending the physical 0 restores the
    onset. Safe for consumers: peak/low-flow features are computed from the
    raw readings upstream, and classify_flow_shape drops the leading 20%.
    """
    if not flow_readings or peak <= 0:
        return [0.0] * n
    src = flow_readings
    if src[0] > 0:
        src = [0.0] + list(src)
    if len(src) == 1:
        return [min(src[0] / peak, 1.0)] * n
    result = []
    for i in range(n):
        pos = i * (len(src) - 1) / (n - 1)
        lo, hi = int(pos), min(int(pos) + 1, len(src) - 1)
        v = src[lo] * (1 - (pos - lo)) + src[hi] * (pos - lo)
        result.append(round(min(v / peak, 1.0), 4))
    return result


def classify_flow_shape(signature, *, steady_state_fraction=None,
                        flow_rise_rate=None, flow_fall_rate=None,
                        mid_event_flow_drop=None, peak=None) -> str:
    """Describe the FLOW waveform shape for DISPLAY — what the History sparkline
    actually draws — so the label matches the picture.

    Distinct from ``_classify_resistance_shape`` (which describes the ΔP/Q
    hydraulic-load curve, an internal feature): a steady shower is a flat-topped
    FLOW rectangle even when its pressure-per-flow ratio wobbles. The primary input
    is the peak-normalised flow ``signature`` (the same 0–1 array the sparkline
    renders), so the returned word provably matches the drawn shape; when no usable
    signature is present it falls back to the stored scalar flow features.

    Returns one of: steady | rising | falling | pulsed | unknown. Thresholds are
    presentation heuristics (tunable) pinned by the unit tests.
    """
    sig = [float(v) for v in (signature or []) if isinstance(v, (int, float))]
    if len(sig) >= 6 and max(sig) > 0.0:
        n = len(sig)
        ramp = max(1, n // 5)                 # drop leading/trailing 20% ramp
        mid = sig[ramp:n - ramp] or sig
        # Oscillation — count significant direction reversals (vs the last
        # significant value, so sub-EPS noise / slow drift never counts).
        EPS = 0.12                            # of peak (sig is 0..1)
        reversals, last_dir, prev = 0, 0, mid[0]
        for v in mid[1:]:
            d = v - prev
            if abs(d) >= EPS:
                cur_dir = 1 if d > 0 else -1
                if last_dir and cur_dir != last_dir:
                    reversals += 1
                last_dir, prev = cur_dir, v
        if reversals >= 2:
            return "pulsed"
        third = max(1, len(mid) // 3)
        slope = statistics.mean(mid[-third:]) - statistics.mean(mid[:third])
        if slope > 0.15:
            return "rising"
        if slope < -0.15:
            return "falling"
        return "steady"

    # ── Fallback: no usable signature → stored scalar flow features ──────────────
    def _f(x):
        try:
            return float(x) if x is not None else None
        except (TypeError, ValueError):
            return None
    ssf, pk = _f(steady_state_fraction), _f(peak)
    drop, rr, fr = _f(mid_event_flow_drop) or 0.0, _f(flow_rise_rate) or 0.0, \
        _f(flow_fall_rate) or 0.0
    if pk and pk > 0 and drop >= 0.2 * pk:
        return "pulsed"
    if ssf is not None and ssf >= 0.6:
        return "steady"
    if rr > 2.0 and rr > fr:
        return "rising"
    if fr > 2.0 and fr > rr:
        return "falling"
    if ssf is not None and ssf > 0:
        return "steady"
    return "unknown"


# ── Sparkline size tiers ─────────────────────────────────────────────────────
# The History sparkline (and the detail-modal flow chart) draws the peak-
# normalised flow_signature, so every event fills the same height and conveys no
# sense of size. These tiers scale the drawn waveform's vertical band so a
# user can tell big draws from trickles at a glance. Defined in STORED units
# (L/min peak flow, litres volume) so they're unit-independent; presentation
# heuristics, tunable. Blended: an event is as big as its LARGER dimension, so a
# brief high-flow spike and a long slow high-volume fill both read large.
_MAG_FLOW_LPM = (2.0, 6.0, 15.0)     # trickle ≤2 < small ≤6 < medium ≤15 < large
_MAG_VOLUME_L = (1.0, 8.0, 40.0)     # trickle ≤1 < small ≤8 < medium ≤40 < large
_MAG_TIERS = ("trickle", "small", "medium", "large")


def classify_magnitude_tier(peak_flow_lpm=None, volume_litres=None) -> str:
    """4-tier event-size bucket for the History sparkline's vertical scale.

    Blends peak flow and volume by taking the LARGER of the two per-dimension
    tiers, so a high-flow-short event and a low-flow-long event both read big.
    Inputs are in stored units (L/min, litres). Returns one of:
    trickle | small | medium | large | unknown (when neither input is usable).
    """
    def _tier(val, bounds):
        try:
            v = float(val) if val is not None else None
        except (TypeError, ValueError):
            return None
        if v is None:
            return None
        for i, b in enumerate(bounds):
            if v <= b:
                return i
        return len(bounds)            # above the top bound → last tier index
    idxs = [i for i in (_tier(peak_flow_lpm, _MAG_FLOW_LPM),
                        _tier(volume_litres, _MAG_VOLUME_L)) if i is not None]
    if not idxs:
        return "unknown"
    return _MAG_TIERS[max(idxs)]


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

    Mutates ``features`` in place. At the end, sets the four waveform A/B
    tracking fields so the History page can show whether the firmware
    waveform was actually consulted for this event:

      * ``esp_waveform_used``      — 1 if any feature was sourced from the
                                     waveform, 0 if every group fell back
                                     to the legacy software path.
      * ``waveform_event_id``      — firmware-side ID of the streamed event.
      * ``waveform_quality``       — firmware self-reported quality 0–100.
      * ``waveform_overlap_score`` — fraction of the event window covered
                                     by valid waveform samples (0.0–1.0).
    """
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
    #
    # Phase 3 (§1.3) — TRAIN-ON-A-HOLE GUARD. The flow/pressure SIGNATURES feed the
    # cluster engine + the per-home fit, so they must come only from a fully-reliable
    # firmware waveform. A capture that is incomplete, resolution-reduced (samples
    # dropped because the buffer filled — the wf_chunk_drop_count path), or
    # self-reported low quality is a HOLE: it must NOT replace the software signature
    # or flip signature_source to esp_*. The firmware-computed metadata above
    # (peak/ΔP/propagation) stays — it is onboard-accurate at 200 Hz regardless of
    # buffer/transport loss; only the sample-array-derived signatures are gated.
    _sig_usable = (bool(fl & _WF_FL_FULL_COMPLETE)
                   and not (fl & _WF_FL_RESOLUTION_REDUCED)
                   and meta.quality == 0)
    _flow_sig_overridden  = False
    _press_sig_overridden = False

    # Flow: full_flow is in L/min; guard against zero/noise arrays before overriding.
    if record.full_flow and _sig_usable:
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
    # Gated by _sig_usable (Phase 3) — same train-on-a-hole guard as the flow signature.
    if record.full_pressure and _sig_usable:
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

    # Rise-phantom discriminator recomputed from the firmware arrays (dev14) —
    # same train-on-a-hole quality gate as the signatures: a lossy/partial
    # waveform must never overwrite the software-computed correlation. The
    # firmware pair is time-aligned at source, so this is the highest-fidelity
    # corr available; _finalize_derived_verdicts re-runs after enrich and keeps
    # the verdict in sync. Deliberately does NOT flip any_wf_used — A/B
    # provenance tracks the signatures only.
    if record.full_flow and record.full_pressure and _sig_usable:
        _wf_corr = _flow_pressure_correlation(record.full_flow,
                                              record.full_pressure)
        if _wf_corr is not None:
            features["flow_pressure_corr"] = round(_wf_corr, 4)

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


# --------------------------------------------------------------------------- #
# Late-waveform upgrade (Fix 1) — flip a recent software-signature event to ESP
# provenance once its chunked waveform finishes assembling. The ESP streams the
# waveform in ~30 s chunks, so a short event finalises 'software' before its
# waveform is ready and the immediate _find_waveform lookup misses it. This
# reverse path re-matches the assembled record to that event and upgrades the
# signature/provenance + shape columns ONLY — never volume, user labels, or
# hourly bookkeeping; the derived verdict is left to the periodic reprocess.
# --------------------------------------------------------------------------- #

# Exactly the columns _enrich_from_waveform writes. Pinned disjoint from
# _EVENT_USER_COLUMNS / _EVENT_APPLIED_BOOKKEEPING_COLUMNS / volume columns by
# test_late_waveform_upgrade's column-guard (which also asserts _enrich's live
# output keys stay within this set, so a future _enrich edit can't silently
# write a volume/user/bookkeeping column through this path).
_WF_UPGRADE_COLUMNS = (
    "signature_source", "flow_signature_json", "pressure_signature_json",
    "esp_waveform_used", "waveform_event_id", "waveform_quality",
    "waveform_overlap_score",
    "peak_flow_lpm", "pressure_delta_psi", "propagation_delay_ms",
    "flow_rise_rate_lpm_s", "time_to_90pct_flow_seconds", "opening_step_lpm",
    "pressure_onset_ms", "steady_state_fraction", "flow_variability",
    "recovery_overshoot_psi",
    # dev14 — recomputed from the firmware flow+pressure arrays under the same
    # quality gate as the signatures; the periodic exclusion reprocess (rise
    # scan) reconciles any verdict drift, exactly like the other columns here.
    "flow_pressure_corr",
)


def _parse_iso(ts):
    """Parse a stored ISO timestamp to an aware UTC datetime, or None."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _late_waveform_upgrade_job(conn, circuit: str, record: WaveformRecord):
    """Reverse-match a just-assembled waveform to a recent software-signature event
    on ``circuit`` and upgrade ONLY its signature/provenance columns to ESP.

    Runs on a private, write-locked connection (one lock acquisition = the SELECT
    match AND the UPDATE here, not select-release-update): a concurrent assembly
    that already upgraded the row makes the ``signature_source='software'`` WHERE a
    no-op, so there is no double-upgrade and no downgrade. Volume / user-label /
    hourly-bookkeeping columns are NEVER written, and ``_finalize_derived_verdicts``
    is NOT re-run — the periodic reprocess reconciles any verdict drift.

    Returns the upgraded event id, or None when nothing matched / nothing flipped.
    """
    from types import SimpleNamespace

    now = datetime.now(timezone.utc)
    rows = conn.execute(
        "SELECT * FROM events WHERE circuit = ? AND signature_source = 'software' "
        "ORDER BY end_ts DESC LIMIT 50",
        (circuit,),
    ).fetchall()

    best = None
    best_score = 0.0
    best_end: Optional[datetime] = None
    for row in rows:
        start_dt = _parse_iso(row["start_ts"])
        end_dt = _parse_iso(row["end_ts"])
        if start_dt is None or end_dt is None:
            continue
        if (now - end_dt).total_seconds() > _WF_MATCH_WINDOW_S:
            continue   # outside the 90 s window — belongs to a prior event
        score = _wf_overlap_score(
            SimpleNamespace(start_ts=start_dt, end_ts=end_dt), record)
        # Best score wins; tie-break to the most-recent end_ts (rows are already
        # end_ts-DESC, so the first max-score row is the most recent among ties).
        if score > best_score or (
                score == best_score and best_end is not None and end_dt > best_end):
            best, best_score, best_end = row, score, end_dt

    if best is None or best_score < _WF_MATCH_MIN_SCORE:
        return None

    features = dict(best)
    _enrich_from_waveform(features, record, best_score)
    # Only persist a genuine signature flip (software → esp_*). A no-flow / no-
    # signal waveform leaves signature_source 'software' → nothing to upgrade, so
    # artifact rows (phantom/cross-talk/dribble) are never rewritten through here.
    if not str(features.get("signature_source") or "software").startswith("esp"):
        return None

    cols = [c for c in _WF_UPGRADE_COLUMNS if c in features]
    set_clause = ", ".join(f"{c} = ?" for c in cols)
    params = [features[c] for c in cols] + [best["id"]]
    cur = conn.execute(
        f"UPDATE events SET {set_clause} WHERE id = ? AND signature_source = 'software'",
        params,
    )
    conn.commit()
    return best["id"] if cur.rowcount > 0 else None


def extract_features(event: RawEvent, *, min_flow_lpm: float = 0.15) -> Dict[str, Any]:
    """Compute the full feature vector from a RawEvent.

    ``min_flow_lpm`` is the per-circuit meter-derived low-flow floor (60 ÷ ppl);
    it gates which steady-state points feed the resistance-shape classifier so a
    coarse meter's single-pulse quantization doesn't inflate the variance. Defaults
    to the 396-ppl turbine floor for callers that don't supply it.
    """
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

    # Volume = TIME-INTEGRAL of the timestamped flow samples (not mean × duration,
    # which over-counts a brief burst trapped in a long pressure-defined event).
    # Prefer the firmware's cumulative integration sensor when present; fall back
    # to the old approximation only for legacy events with no timestamped samples.
    from .flow_integral import integrate_litres, active_flow_features
    flow_integral_litres, _integral_capped = integrate_litres(event.flow_samples)
    active = active_flow_features(event.flow_samples, duration)
    if event.volume_litres_measured is not None:
        volume_litres = event.volume_litres_measured
    elif event.flow_samples:
        volume_litres = flow_integral_litres
    else:
        volume_litres = avg_flow * (duration / 60.0) if duration > 0 else 0.0
    integration_quality = "capped" if _integral_capped else "ok"

    # True hydraulic resistance: ΔP / avg_Q
    # Only meaningful when flow is above noise floor and a pressure
    # transient was actually captured.
    resistance: Optional[float] = None
    if avg_flow >= 0.15 and event.has_pressure_transient and event.pressure_delta_psi > 0:
        resistance = event.pressure_delta_psi / avg_flow

    # Resistance curve shape — uses corrected ΔP/Q formula.
    # pressure_readings are at 40 Hz, flow_readings at 1 Hz — index-bin the
    # pressure down to the flow sample count so the resistance values are
    # time-aligned (see _bin_pressure_to_flow).
    pressure_for_shape = _bin_pressure_to_flow(
        event.flow_readings, event.pressure_readings or [])

    shape = _classify_resistance_shape(
        pressure_for_shape,
        event.flow_readings,
        pre_event_pressure,
        min_flow=min_flow_lpm,
    )

    # Rising-pressure phantom discriminator (dev14): Pearson r of flow vs the
    # same binned pressure. Stored on every event (NULL when uncomputable) —
    # the verdict itself is decided in _finalize_derived_verdicts.
    flow_pressure_corr = _flow_pressure_correlation(
        event.flow_readings, event.pressure_readings or [])

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
    # The phantom verdict + volume_litres_effective + volume_estimation_method
    # + excluded_from_training + match_rejection_reason are ALL derived by
    # _finalize_derived_verdicts() on the assembled dict below — the single
    # source of truth, re-run after ESP-waveform enrichment in _process().

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

    result = {
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
        # Rise-phantom discriminator (dev14) — NULL when uncomputable, never 0.
        "flow_pressure_corr": (round(flow_pressure_corr, 4)
                               if flow_pressure_corr is not None else None),
        "volume_litres": round(volume_litres, 3),

        # Active-flow features (timestamped-flow integral). Drive classification
        # and the hardened phantom guard; NULL only for legacy/no-sample events.
        "flow_integral_litres": round(flow_integral_litres, 3),
        "active_flow_duration_seconds": active["active_flow_duration_seconds"],
        "true_avg_flow_lpm": active["true_avg_flow_lpm"],
        "flow_on_ratio": active["flow_on_ratio"],
        "active_flow_segment_count": active["active_flow_segment_count"],
        "flow_cv_on_segments": active["flow_cv_on_segments"],
        "integration_quality": integration_quality,

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
        # The next five fields are DERIVED — provisional values here, then
        # overwritten by _finalize_derived_verdicts() before return (and again
        # after ESP-waveform enrichment in _process). Single source of truth
        # there; do not duplicate the verdict logic in this literal.
        "excluded_from_training":          0,       # set by finalizer
        "match_rejection_reason":          None,    # set by finalizer
        "volume_litres_effective":         round(volume_litres, 3),  # finalizer recomputes
        "volume_estimation_method":        "raw",   # finalizer recomputes
        "is_pressure_restoration_phantom": 0,       # set by finalizer

        # Degraded-supply guard — INPUTS the finalizer reads.
        "degraded_supply":             1 if is_degraded else 0,
        "volume_litres_estimated":     round(volume_litres_estimated, 3),
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
    # Derive the phantom verdict + volume_effective + exclusion from the
    # assembled feature values (single source of truth).
    _finalize_derived_verdicts(result, min_flow_lpm=min_flow_lpm)
    return result


# Phase 2.3 — minimum gap between anomaly NOTIFY pushes per circuit (the shut-off
# path is governed separately by the persistent per-12h cap, not this cooldown).
_ANOMALY_ALERT_COOLDOWN_MIN = 15


class FeatureExtractor:
    """
    Consumes RawEvent objects from the queue and stores
    extracted features in SQLite.
    """

    def __init__(self, event_queue: asyncio.Queue,
                 db_conn: sqlite3.Connection, alert_manager=None,
                 ha_client=None, event_detector=None, ha_tz=None,
                 is_calibrating=None):
        self._queue = event_queue
        self._db = db_conn
        self._alert_manager = alert_manager
        self._ha = ha_client
        # Callback → True while a bucket / municipal calibration test runs on a circuit.
        # The deliberate test draw must not trip auto-shutoff or feed training / anomaly.
        self._is_calibrating = is_calibrating or (lambda c: False)
        # Home timezone (dev.24) — converts UTC-stored event timestamps to LOCAL
        # for the water-softener regen band match. None → compare in UTC (the
        # batch reclassify will still detect it once a tz-aware caller runs).
        self._ha_tz = ha_tz
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
        # Strong refs for the late-waveform upgrade tasks (Fix 1) — same GC-safety
        # pattern as the alert tasks above.
        self._pending_wf_tasks: set[asyncio.Task] = set()
        # Per-circuit cooldown timestamps for the pulsing-supply alert.
        # In-memory only — resets on addon restart. That's acceptable for now:
        # if a real pulsing episode persists across a restart the user will
        # be re-alerted (mildly annoying, not dangerous). Persist to DB
        # later if it becomes a problem.
        self._last_pulsing_alert_at: dict[str, datetime] = {}
        # Per-circuit cooldown for the Phase 2.3 anomaly NOTIFY path (in-memory is
        # fine — at worst a few extra notifications after a restart). The shut-off
        # rate limit is PERSISTENT (anomaly_shutoff_log), not in-memory.
        self._last_anomaly_alert_at: dict[str, datetime] = {}
        # Set by orchestrator after ClusterEngine is initialised and rebuilt.
        self.cluster_engine = None
        # dev.23: per-circuit circuit_type cache for the structural rules tier
        # (one DB read per circuit per process lifetime, not per event).
        self._circuit_type_cache: dict[str, str] = {}

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

    def handle_late_waveform(self, circuit: str, record: WaveformRecord) -> None:
        """Sink (wired by the orchestrator to EventDetector) for a freshly-assembled
        ESP waveform. Schedules a background, write-locked upgrade of a recent
        software-signature event to ESP provenance. Runs on the loop's WS callback;
        no-op without a running loop (test/import contexts)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        t = loop.create_task(self._upgrade_event_from_late_waveform(circuit, record))
        self._pending_wf_tasks.add(t)
        t.add_done_callback(self._pending_wf_tasks.discard)

        def _log_exc(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                log.warning("[%s] late-waveform upgrade task raised: %s", circuit, exc)
        t.add_done_callback(_log_exc)

    async def _upgrade_event_from_late_waveform(
            self, circuit: str, record: WaveformRecord) -> None:
        """Run the reverse-match + signature upgrade off the event loop, serialised
        through the write lock on its OWN connection (never the shared one)."""
        from .config import DB_PATH
        from .database import run_isolated_write

        def _job(conn):
            return _late_waveform_upgrade_job(conn, circuit, record)

        try:
            upgraded_id = await run_isolated_write(DB_PATH, _job)
        except Exception as e:
            log.warning("[%s] late-waveform upgrade failed (non-fatal): %s", circuit, e)
            return
        if upgraded_id is not None:
            log.debug("[%s] late-waveform upgrade — event %s software→esp",
                      circuit, upgraded_id)

    async def _process(self, event: RawEvent) -> None:
        if not event.complete:
            return

        if self._ha and event.flow_onset_entity:
            await self._enrich_propagation_delay(event)

        # Per-circuit low-flow floor (60 ÷ ppl) from the live detector; falls back
        # to the 396-ppl turbine default if the detector isn't wired (tests/import).
        min_flow_lpm = 0.15
        if self._event_detector is not None:
            min_flow_lpm = self._event_detector.min_flow_for(event.circuit)
        features = extract_features(event, min_flow_lpm=min_flow_lpm)

        # Attempt to enrich features from ESP waveform capture (firmware 3.7.0+).
        # Per-group routing: each group falls back to the legacy value independently.
        wf_record = self._find_waveform(event)
        if wf_record is not None:
            score = _wf_overlap_score(event, wf_record)
            _enrich_from_waveform(features, wf_record, score)
            # NOTE: enrichment overwrites pressure_delta_psi / peak_flow_lpm
            # with ESP-measured values. The phantom verdict is re-derived
            # below (after the existing-row read), so a real long event — e.g.
            # a 40-min shower whose pressure drop wasn't captured in the
            # software pass — is NOT left with a stale phantom flag + zeroed
            # volume.
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

            # ── Honour stored user intent (Sprint H) ──────────────────────
            # extract_features() computes verdicts fresh from the RawEvent and
            # has no knowledge of stored user choices. Pull them from the
            # existing row (if any) and apply BEFORE the upsert:
            #   • user_ignored — folded into excluded_from_training (which is
            #     a derived column, no longer preserved by the upsert, so a
            #     re-import would otherwise silently un-ignore the event).
            #   • user_classified — manual classification is authoritative:
            #     copy the stored category flags and SKIP the auto finalizer
            #     so auto-detection / waveform enrichment never overrides it.
            existing = self._db.execute(
                "SELECT user_ignored, user_classified, "
                "       is_pressure_restoration_phantom, degraded_supply, "
                "       is_cross_talk, is_low_flow_dribble, "
                "       is_composite, volume_litres_effective "
                "FROM events WHERE id = ?",
                (features["id"],),
            ).fetchone()
            if existing is not None and existing["user_classified"]:
                features["user_classified"] = 1
                features["user_ignored"] = int(existing["user_ignored"] or 0)
                features["is_pressure_restoration_phantom"] = \
                    int(existing["is_pressure_restoration_phantom"] or 0)
                features["degraded_supply"] = int(existing["degraded_supply"] or 0)
                # Carry ALL volume-affecting verdict flags back from the stored manual
                # classification — not just phantom/degraded — so excluded_from_training
                # stays consistent with a user-marked cross-talk or dribble (P2 fix).
                features["is_cross_talk"] = int(existing["is_cross_talk"] or 0)
                features["is_low_flow_dribble"] = int(existing["is_low_flow_dribble"] or 0)
                features["is_composite"] = int(existing["is_composite"] or 0)
                features["volume_litres_effective"] = \
                    float(existing["volume_litres_effective"] or 0.0)
                features["excluded_from_training"] = 1 if (
                    features["is_pressure_restoration_phantom"]
                    or features["degraded_supply"]
                    or features["is_cross_talk"]
                    or features["is_low_flow_dribble"]
                    or features["is_composite"]
                    or features["user_ignored"]
                ) else 0
            else:
                # Not manually classified — re-derive verdicts with the
                # preserved Ignore intent (and post-enrichment pressure), applying
                # any frozen per-home artifact calibration (Phase 2.4).
                features["user_ignored"] = (
                    int(existing["user_ignored"] or 0) if existing is not None else 0
                )
                from .artifact_calibration import load_artifact_calibration
                _acal = load_artifact_calibration(self._db, features.get("circuit"))
                _finalize_derived_verdicts(features, _acal or None,
                                           min_flow_lpm=min_flow_lpm)

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

            # Calibration test draw — a deliberate, known bucket / municipal run is NOT
            # organic usage: exclude it from training + anomaly stats so it can't pollute
            # the frozen baseline. (Notify / shut-off are separately suppressed in
            # _apply_anomaly_response.)
            if self._is_calibrating(event.circuit):
                reason = features.get("match_rejection_reason") or "calibration"
                self._db.execute(
                    "UPDATE events SET excluded_from_training = 1, "
                    "match_rejection_reason = ? WHERE id = ?",
                    (reason, features["id"]),
                )
                features["excluded_from_training"] = 1
                features["match_rejection_reason"] = reason
                log.info("[%s] event excluded from training — calibration test draw",
                         event.circuit)

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

            # 2b training-helper capture: if a capture is armed on this circuit,
            # record this just-completed event as a candidate (writes NO label —
            # the user confirms in the wizard). Cheap (one indexed SELECT; free when
            # idle). Lazy import matches this module's circular-import-avoidance
            # pattern (cf. the database imports above); best-effort — a capture-logic
            # bug must never block event storage.
            try:
                from .database import record_training_candidate
                record_training_candidate(self._db, event.circuit, features)
            except Exception as e:
                log.warning("[%s] training-capture hook failed (non-fatal): %s",
                            event.circuit, e)

            # ── Phase 2.3 anomaly response (frozen-baseline deviation) ─────────
            # Replaces the old match-confidence alert (which fired on anything that
            # didn't strongly match — the false-positive source). The verdict was
            # scored + stored in _cluster_event; here we apply the user's graduated
            # response, but ONLY in the locked 'live' state. A circuit calibrating /
            # labelling / mid-recalibration has no trustworthy baseline → no notify,
            # no shut-off. Shut-off carries extra guardrails (see _apply_anomaly_response).
            am = self._alert_manager
            anomaly = features.get("_anomaly") or {}
            if am and anomaly.get("is_anomalous"):
                ts_row = self._db.execute(
                    "SELECT state FROM training_state WHERE circuit = ?",
                    (event.circuit,)).fetchone()
                if ts_row and ts_row["state"] == "live":
                    await self._apply_anomaly_response(
                        event.circuit, features, anomaly)

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

    def _score_anomaly(self, circuit: str, features: dict) -> dict:
        """Score an event against the FROZEN baseline (Phase 2.3). Read-only — the
        notify / shut-off response is applied separately in ``_process`` behind a
        'live' state gate. Returns the inert verdict for artifact / excluded events
        or when no baseline exists."""
        from .anomaly_baseline import load_usage_baselines, score_event_anomaly
        from .database import get_sensitivity_config
        baselines = load_usage_baselines(self._db, circuit)
        sens = get_sensitivity_config(self._db, circuit)
        return score_event_anomaly(features, baselines, sens)

    async def _apply_anomaly_response(self, circuit: str, features: dict,
                                      anomaly: dict) -> None:
        """Phase 2.3 — graduated response to a LIVE baseline-deviation event.

        The shut-off paths carry guardrails the notify paths do not: a thin/default
        baseline (``shutoff_ok_*`` False) or a circuit that has not been live for
        ``MIN_LIVE_DAYS_FOR_SHUTOFF`` degrades shut-off to notify, and the per-12h
        shut-off cap is read from the PERSISTENT ``anomaly_shutoff_log`` (it survives
        the very restart a pathological run could otherwise use to reset it).
        """
        # A deliberate calibration test draw is not organic usage — never notify or
        # shut off in response to it (the event is also excluded from training).
        if self._is_calibrating(circuit):
            return
        from .database import get_sensitivity_config
        from .anomaly_baseline import _row_get, MIN_LIVE_DAYS_FOR_SHUTOFF
        sens = get_sensitivity_config(self._db, circuit)
        response = (_row_get(sens, "anomaly_response", "notify") or "notify")
        if response == "off":
            return
        circuit_name = circuit.replace("_", " ").title()
        score = float(anomaly.get("score") or 0.0)
        atype = anomaly.get("anomaly_type")
        event_id = features.get("id")

        want_shutoff = (
            (response == "shutoff_any" and anomaly.get("shutoff_ok_any"))
            or (response == "notify_shutoff_severe" and anomaly.get("shutoff_ok_severe"))
        )
        if (want_shutoff
                and self._anomaly_shutoff_state_ok(circuit)
                and self._anomaly_seasoned(sens, MIN_LIVE_DAYS_FOR_SHUTOFF)
                and self._anomaly_shutoff_rate_ok(circuit, sens)):
            if await self._auto_shutoff(circuit, circuit_name, score, atype, event_id):
                return   # the shut-off path already notified (why + reopen)
        # Off-ramp: degrade to / default notify, rate-limited so it cannot spam.
        self._notify_anomaly(circuit, circuit_name, score, atype, event_id)

    def _anomaly_seasoned(self, sens, min_days: int) -> bool:
        """Earned-trust gate — the baseline has had ≥ ``min_days`` of real usage since
        it was frozen at activation (``baseline_computed_at``). Unseasoned → no shut-off."""
        from .anomaly_baseline import _row_get
        ts = _row_get(sens, "baseline_computed_at")
        if not ts:
            return False
        try:
            frozen = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return False
        if frozen.tzinfo is None:
            frozen = frozen.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - frozen) >= timedelta(days=min_days)

    def _anomaly_shutoff_rate_ok(self, circuit: str, sens) -> bool:
        """Persistent per-12h shut-off cap (counted from anomaly_shutoff_log so it
        survives a restart)."""
        from .anomaly_baseline import _row_get
        cap = int(_row_get(sens, "max_shutoffs_per_12h", 2) or 2)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
        row = self._db.execute(
            "SELECT COUNT(*) FROM anomaly_shutoff_log "
            "WHERE circuit = ? AND closed_at >= ?", (circuit, cutoff)).fetchone()
        return (int(row[0]) if row else 0) < cap

    def _anomaly_shutoff_state_ok(self, circuit: str) -> bool:
        """HARD safety gate — an automated valve close is permitted ONLY when the
        circuit is locked ('live') AND not in an active (re)calibration / accelerated-
        adaptation window. Blocks auto-shutoff during setup, learning, labelling, and
        BOTH full recalibration (state ≠ 'live') and partial recalibration (stays
        'live' but opens a 14-day adaptation window): the system must never cut the
        user's water while it is still (re)learning what normal looks like.

        Delegates to ``database.is_baseline_locked`` — the ONE definition of
        "baseline locked", shared with the label-reclassify skip gate, so the two
        notions of locked can't drift (they used to be line-for-line copies)."""
        from .database import is_baseline_locked
        return is_baseline_locked(self._db, circuit)

    def _fingerprint_enabled(self) -> bool:
        """home_profile toggle for the fingerprint label tier (default ON).
        A mid-migration DB without the column reads as ON (the schema default);
        any other failure disables the tier for this event (never fatal)."""
        try:
            row = self._db.execute(
                "SELECT fingerprint_labeling_enabled FROM home_profile "
                "WHERE id = 1").fetchone()
            return bool(row["fingerprint_labeling_enabled"]) if row else True
        except sqlite3.OperationalError:
            return True
        except Exception:  # noqa: BLE001
            return False

    def _resolve_valve_entity(self, circuit: str) -> Optional[str]:
        row = self._db.execute(
            "SELECT entity_id FROM circuit_entity_map "
            "WHERE circuit = ? AND role = 'valve_entity'", (circuit,)).fetchone()
        return row[0] if row and row[0] else None

    async def _auto_shutoff(self, circuit: str, circuit_name: str, score: float,
                            atype, event_id) -> bool:
        """Close the valve, log it (persistent), and notify with why + a one-action
        reopen. Returns False (→ caller falls back to notify) when no valve is
        configured or the close fails — a shut-off must never silently swallow."""
        # HARD final gate at the actuation chokepoint: the valve can NEVER close
        # unless the circuit is live and settled (not learning / setup / recalibrating),
        # independent of how this method was reached.
        if not self._anomaly_shutoff_state_ok(circuit):
            log.warning("[%s] anomaly auto-shutoff refused — circuit is not in a live, "
                        "settled state (learning / setup / recalibrating)", circuit)
            return False
        valve = self._resolve_valve_entity(circuit)
        if not valve or self._ha is None:
            log.warning("[%s] anomaly auto-shutoff requested but no valve entity / "
                        "ha client — degrading to notify", circuit)
            return False
        try:
            ok = await self._ha.close_valve(valve)
        except Exception as e:
            log.error("[%s] anomaly auto-shutoff close_valve failed: %s", circuit, e)
            return False
        if not ok:
            return False
        # Store closed_at as an explicit UTC ISO timestamp (NOT the CURRENT_TIMESTAMP
        # default, whose 'YYYY-MM-DD HH:MM:SS' space format sorts BELOW the
        # 'YYYY-MM-DDT…+00:00' cutoff in _anomaly_shutoff_rate_ok — the rate limit
        # would never trip). Both ends must use the same ISO format.
        self._db.execute(
            "INSERT INTO anomaly_shutoff_log "
            "    (circuit, event_id, anomaly_type, score, closed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (circuit, event_id, atype, score,
             datetime.now(timezone.utc).isoformat()))
        self._db.commit()
        log.warning("[%s] ANOMALY AUTO-SHUTOFF — closed valve %s (event %s, %s, "
                    "score %.2f)", circuit, valve, event_id, atype, score)
        am = self._alert_manager
        if am:
            self._spawn_alert_task(am.alert_unusual_usage(
                circuit, score, atype, circuit_name, shutoff=True,
                event_id=event_id, valve_entity=valve))
        return True

    def _notify_anomaly(self, circuit: str, circuit_name: str, score: float,
                        atype, event_id) -> None:
        """Notify (persistent + push) with a per-circuit cooldown so a stream of
        anomalous events can't spam."""
        am = self._alert_manager
        if not am:
            return
        now = datetime.now(timezone.utc)
        last = self._last_anomaly_alert_at.get(circuit)
        if last is not None and now - last < timedelta(minutes=_ANOMALY_ALERT_COOLDOWN_MIN):
            return
        self._last_anomaly_alert_at[circuit] = now
        self._spawn_alert_task(am.alert_unusual_usage(
            circuit, score, atype, circuit_name, shutoff=False, event_id=event_id))

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

        # NOTE (Phase 2.3): the old match-confidence anomaly_score (1.0 - confidence)
        # was retired here — it fired on anything that didn't strongly match a known
        # fixture, which is most of what a real home produces (the false-positive
        # source). The stored anomaly_score / anomaly_type / flagged columns are now
        # the FROZEN-BASELINE deviation verdict, computed below once the type is known
        # (see _score_anomaly + the write-back UPDATE).

        # dev.23 — structural rules tier (rules-first; Pass-5 semantics). Runs
        # BEFORE the k-NN regardless of cluster strength, mirroring the batch
        # reclassify. The trailing washer scan is pre-gated to fixture circuits
        # AND peaks inside the family envelope, so micro/gentle events skip it.
        # Caught broadly: a rules failure must NEVER block the cluster_id write.
        matched_fixture_type: Optional[str] = None
        matched_via: Optional[str] = None
        cycle_group_id: Optional[str] = None
        washer_members: dict = {}
        softener_members: dict = {}
        dishwasher_members: dict = {}
        try:
            from .event_rules import (
                WASHER_FAMILY_PK_ENVELOPE, detect_dishwasher_cycles,
                detect_softener_sessions, detect_washer_cycles, get_home_timezone,
                parse_hhmm_to_minutes, rule_classify_event,
            )
            from .database import get_home_profile
            from .rule_calibration import load_rule_calibration
            ctype = self._circuit_type_cache.get(circuit)
            if ctype is None:
                from .database import get_circuit_type
                ctype = get_circuit_type(self._db, circuit)
                self._circuit_type_cache[circuit] = ctype
            # Frozen per-home rule bands (empty → shipped defaults). Read fresh so
            # an activation / recalibration takes effect on the next event.
            calib = load_rule_calibration(self._db, circuit)
            # dev.24 — water-softener session (precedence: softener → washer →
            # rules → knn). Profile read FRESH (NOT cached) so a Settings toggle
            # takes effect on the next event with no restart. Hard-gated.
            prof = get_home_profile(self._db)
            if (prof is not None and prof["has_water_softener"]
                    and (prof["softener_circuit"] or "main") == circuit):
                band = parse_hhmm_to_minutes(prof["softener_regen_start"])
                if band is not None:
                    s_since = (datetime.now(timezone.utc)
                               - timedelta(hours=3.5)).isoformat()
                    softener_members = detect_softener_sessions(
                        self._db, circuit, band, since_ts=s_since,
                        tz=(get_home_timezone() or self._ha_tz), calib=calib)
            pk = features.get("peak_flow_lpm")
            if (ctype != "zone" and pk is not None
                    and WASHER_FAMILY_PK_ENVELOPE[0] <= pk
                    <= WASHER_FAMILY_PK_ENVELOPE[1]):
                since = (datetime.now(timezone.utc)
                         - timedelta(minutes=50)).isoformat()
                washer_members = detect_washer_cycles(
                    self._db, circuit, since_ts=since, limit=400, calib=calib)
            # dev.39 — dishwasher cycle: only worth scanning when THIS event is a
            # gentle small fill (a cheap, deliberately-loose pre-gate; the detector
            # then applies the precise calib-aware band AND needs >=3 such fills
            # chained). Span covers a full cycle (~2.5h lookback). The loose bounds
            # are DERIVED from the same calib values the detector uses (×1.4
            # slack) — hardcoded 5.0s silently stopped invoking the detector for
            # homes whose fitted DW_* band was calibrated wider, flipping labels
            # between the live path and batch reclassify.
            from .event_rules import _cv as _rule_cv
            _dw_vol_hi = float(_rule_cv(calib, "DW_VOL_L")[1]) * 1.4
            _dw_pk_hi = float(_rule_cv(calib, "DW_MAX_PK_LPM")) * 1.4
            vol = features.get("volume_litres")
            if (ctype != "zone" and event_id not in softener_members
                    and event_id not in washer_members
                    and pk is not None and pk <= max(5.0, _dw_pk_hi)
                    and vol is not None and 0.0 < vol <= max(5.0, _dw_vol_hi)):
                dw_since = (datetime.now(timezone.utc)
                            - timedelta(hours=2.5)).isoformat()
                dishwasher_members = detect_dishwasher_cycles(
                    self._db, circuit, since_ts=dw_since, calib=calib,
                    exclude_ids=set(washer_members) | set(softener_members))
            if event_id in softener_members:
                matched_fixture_type, matched_via = ("water_softener",
                                                     "softener_session")
                cycle_group_id = softener_members[event_id][1]
            elif event_id in washer_members:
                matched_fixture_type, matched_via = ("washing_machine",
                                                     "washer_cycle")
                cycle_group_id = washer_members[event_id][1]
            elif event_id in dishwasher_members:
                matched_fixture_type, matched_via = ("dishwasher",
                                                     "dishwasher_cycle")
                cycle_group_id = dishwasher_members[event_id][1]
            else:
                rule_hit = rule_classify_event(features, ctype, calib=calib)
                if rule_hit is not None:
                    matched_fixture_type, matched_via = rule_hit
        except Exception as e:
            log.warning("[%s] structural rules tier failed (non-fatal): %s",
                        circuit, e)

        # Sprint C — signature-matcher (k-NN) residual. Runs when no structural
        # rule claimed the event AND the cluster matcher either returned no
        # cluster_id or a low-confidence match. Caught broadly because
        # matcher-or-DB failure must NEVER block the regular cluster_id write.
        weak_match = (
            cluster_id_result is None
            or (match_confidence is not None and match_confidence < 0.5)
        )
        # Fingerprint tier (2026-07 audit Phase 3) — whole-waveform NN against
        # USER-labeled events at a tight self-calibrated threshold. Runs under
        # the same condition as the k-NN residual and outranks it (stronger
        # evidence); a fingerprint hit short-circuits the k-NN below. The
        # event's waveform was stored just before _cluster_event, so it is
        # readable here. Never fatal.
        if matched_fixture_type is None and weak_match \
                and self._fingerprint_enabled():
            try:
                from .fingerprint_matcher import match_event_fingerprint
                fp_hit = match_event_fingerprint(self._db, circuit, event_id)
                if fp_hit is not None:
                    matched_fixture_type = fp_hit["fixture_type"]
                    matched_via = "fingerprint"
                    log.info(
                        "[%s] event %s fingerprint-matched %s "
                        "(dist=%.3f <= thr=%.3f, neighbor %s)",
                        circuit, event_id, matched_fixture_type,
                        fp_hit["distance"], fp_hit["threshold"],
                        fp_hit["neighbor_event_id"],
                    )
            except Exception as e:
                log.warning("[%s] fingerprint tier failed (non-fatal): %s",
                            circuit, e)
        if matched_fixture_type is None and weak_match:
            try:
                from .database import match_event_to_signature_knn
                from .event_rules import CYCLE_ONLY_FIXTURE_TYPES
                sig_hit = match_event_to_signature_knn(
                    self._db, circuit, features
                )
                if sig_hit is None:
                    pass
                elif sig_hit["fixture_type"] in CYCLE_ONLY_FIXTURE_TYPES:
                    # Multi-fill appliance from a LONE signature — needs cycle context
                    # (washer_cycle / dishwasher rule), so leave it unlabelled. A real
                    # cycle's first fill is re-stamped by the retro-scan on completion.
                    log.info("[%s] event %s: suppressed lone k-NN %s (no cycle context)",
                             circuit, event_id, sig_hit["fixture_type"])
                else:
                    matched_fixture_type = sig_hit["fixture_type"]
                    matched_via = "knn"
                    log.info(
                        "[%s] event %s matched signature %s (dist=%.2f, "
                        "trained on %d events)",
                        circuit, event_id, matched_fixture_type,
                        sig_hit["distance"], sig_hit["member_count"],
                    )
            except Exception as e:
                log.warning(
                    "[%s] signature-match fallback failed (non-fatal): %s",
                    circuit, e,
                )

        # Phase 2.3 — score the event against the FROZEN baseline now that its type
        # is known, and persist the verdict (reviving the dormant anomaly columns +
        # the daily anomaly_count rollup). Stash it on `features` so the _process
        # response policy reads the same verdict without re-scoring. flagged=1 marks
        # a genuine (non-artifact) anomaly. Side effects (notify / shut-off) are
        # NOT done here — only in _process, behind the 'live' state gate.
        features["matched_fixture_type"] = matched_fixture_type
        anomaly = self._score_anomaly(circuit, features)
        features["_anomaly"] = anomaly

        # Write cluster results back to the event row
        self._db.execute(
            """UPDATE events SET
                 cluster_id               = ?,
                 match_confidence         = ?,
                 match_level              = ?,
                 match_rejection_reason   = ?,
                 seconds_since_prev_event = ?,
                 prev_cluster_id          = ?,
                 matched_fixture_type     = ?,
                 matched_via              = ?,
                 cycle_group_id           = ?,
                 anomaly_score            = ?,
                 anomaly_type             = ?,
                 flagged                  = ?
               WHERE id = ?""",
            (cluster_id_result, match_confidence, match_level,
             match_rejection_reason,
             seconds_since_prev, prev_cluster_id, matched_fixture_type,
             matched_via if matched_fixture_type is not None else None,
             cycle_group_id if matched_fixture_type is not None else None,
             anomaly.get("score"), anomaly.get("anomaly_type"),
             1 if anomaly.get("is_anomalous") else 0,
             event_id)
        )

        # dev.23/dev.24/dev.39 — trailing retro-scan: a washer cycle (~45 min), a
        # softener session (~3 h), and a dishwasher cycle (~2 h) COMPLETE over time,
        # so earlier members were classified before the family reached its >=3-fill
        # threshold (there is no periodic reprocess on the live path). Retro-stamp the
        # window's members now WITH their cycle_group_id. Cycle/session context outranks
        # a per-event machine match, so this MAY overwrite a prior knn/rule_* match
        # (e.g. a backwash mis-typed shower_tub); user labels are never touched.
        for _members, _mtype, _mvia in (
                (softener_members, "water_softener", "softener_session"),
                (washer_members, "washing_machine", "washer_cycle"),
                (dishwasher_members, "dishwasher", "dishwasher_cycle")):
            for _eid, _rolegid in _members.items():
                if _eid == event_id:
                    continue
                _gid = _rolegid[1] if isinstance(_rolegid, tuple) else None
                try:
                    self._db.execute(
                        "UPDATE events SET matched_fixture_type = ?, "
                        "       matched_via = ?, cycle_group_id = ? "
                        "WHERE circuit = ? AND id = ? "
                        "  AND user_fixture_type IS NULL "
                        "  AND COALESCE(matched_via, '') <> ?",
                        (_mtype, _mvia, _gid, circuit, _eid, _mvia),
                    )
                except Exception as e:
                    log.warning("[%s] cycle retro-scan failed (non-fatal): %s",
                                circuit, e)

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
