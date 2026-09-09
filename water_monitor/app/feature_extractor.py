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

# Re-exported: feature_extractor_service (unit 7.2) is now the only reader of
# this constant, and it takes it from here rather than from cluster_engine.
from .cluster_engine import SEQUENCE_GAP_MAX_SECONDS as _SEQUENCE_GAP_MAX_S  # noqa: F401


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

# dev33 — constant-pressure (VFD) booster-pump ripple exemption.
#
# The ESYBOX's control loop hunts around its setpoint at ~1 Hz with ±1.4-2.1 psi
# of mid-event pressure wobble. That is invisible at a fixture and leaves the
# meter perfectly trustworthy, but it satisfies every pulsing-supply gate — the
# 2026-07 install drove degraded events from 0 to 27-68/week, each one needlessly
# volume-estimated and excluded.
#
# 1.5 s is INSIDE the observed distribution, not a gap: measured pump ripple runs
# 0.93-2.0 s (all but 2 of 121 post-pump events below 1.5 s), and pre-pump GENUINE
# pulsing runs 0.81-2.0 s. Period alone therefore cannot discriminate — which is
# why the exemption is additionally gated on the pump ERA (see
# supply_regime.pump_era_start). Re-derive this constant if the pump's setpoint
# or hunting frequency changes: histogram pressure_dominant_period_s over
# degraded events in the pump era and take the upper edge of the fast mode.
#
# Interim measure. The principled fix is a narrowband spectral discriminator
# (FFT the 6.2 Hz idle-pressure stream and gate on the ripple line) — that also
# addresses the premature-close and volume-inflation members of this family.
_VFD_RIPPLE_MAX_PERIOD_S: float = 1.5
VFD_RIPPLE_EXEMPT_REASON: str = "vfd_ripple_exempt"

# dev33 — "every classification tier evaluated this event and abstained".
# Distinct from the artifact reasons (which say the event isn't real water) and
# from the cluster-tier reasons ('no_centers' / 'features_missing' /
# 'type_gate_rejected'); this one says the event IS real, was evaluated, and
# nothing recognised it. Written only into an empty reason, and retracted as
# soon as any pass matches the event — a marker that couldn't retract would
# make a classification RECOVERY as invisible as the outage it measures.
NO_TIER_MATCHED_REASON: str = "no_tier_matched"

VOLUME_ENVELOPE_PERCENTILE   = 0.95
VOLUME_ENVELOPE_WINDOW_S     = 5.0
FIXTURE_CYCLE_PERIOD_MAX_S   = 30.0
MAX_WAVEFORM_BINS            = 1000

# Points in the stored flow/pressure shape signatures (events.flow_signature_json
# / pressure_signature_json). 32 → 64 (2026-07-05) so short pulses (washer fill
# pauses) survive; 64 → 256 (2026-07-06) so LONG events stop collapsing into
# rectangles (at 64 pts a 45-min shower gets one point per ~42 s — valve ramps
# and fill tapers vanish; at 256 it's ~10.5 s/pt, matching the software capture
# cadence). Every consumer (sparkline, classify_flow_shape, cluster_engine's
# resample-on-expand, the History display-upgrade path) is length-agnostic, so
# historical shorter rows keep working; the dev18 one-shot backfill regenerates
# them at 256 from event_waveforms where a finer envelope exists.
SIGNATURE_POINTS             = 256

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

# Pulsing-supply envelope cap (2026-07 audit Phase 2a; TIGHTENED dev33 §8.5).
# The envelope estimate measured 2.86x reality in aggregate (worst single case:
# 336 L claimed for 2 L real). Phase 2a capped it at 1.5x the measured-flow
# evidence — but the 2026-08-02 audit showed that ceiling IS the remaining
# inflation: effective > raw > true-history on every checked pulsing event
# (11.73 -> 18.04 L = exactly 1.5x the flow integral; +105 L across the live
# DB). The premise that "the flow meter under-reads during pulsing" did not
# survive: the meter agreed with raw HA history to ~1% on those same events.
#
# So the multiplier is 1.0 — for a degraded event the estimate can only REDUCE
# or match the metered volume. Genuine meter gaps are still covered by the
# `base is None` branch below (meter read NOTHING at all). A PARTIAL meter gap
# (meter read something, but under-read) is uncorrectable upward by design:
# a chosen trade, and an empirical bet on this house's meters, not a property
# of the design. `_ENVELOPE_CAP_FLOOR_L` still protects tiny events.
_ENVELOPE_CAP_FLOW_MULT: float = 1.0
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
_DRIBBLE_MAX_VOLUME_L:  float = 0.5   # legacy calib key — no longer read by the detector
_DRIBBLE_MAX_FLOW_LPM:  float = 1.0   # legacy calib key — no longer read by the detector
_DRIBBLE_MAX_DELTA_PSI: float = 1.5   # legacy calib key — no longer read by the detector

# ─────────────────────────────────────────────────────────────────────────────
# Meter registration floor (2026-07-05) — REPLACES the dribble triple-gate.
#
# Every flow meter has a PHYSICAL registration threshold, distinct from its
# 60÷ppl pulse-resolution floor: below some rate the water passes UNMETERED —
# through gear running-clearance on a positive-displacement meter (no perfect
# seals), or past a friction/magnetic-drag-stalled rotor on a turbine. Readings
# an event produced entirely below that floor are outside the meter's valid
# operating regime and are FALSE INFORMATION whether or not real water was
# behind them. Controlled ground-truth tests on this install (2026-07-05):
# the K=72 oval gear registered ~2% of 2.0 L drawn across three sub-floor
# draws and was fully SILENT at a sustained 1.11 L/min — while the pressure
# sensors resolved every draw. Keeping such values (e.g. 0.04 L of a 500 ml
# dispense) is false precision; an honest "below meter floor" zero+badge wins.
# Some real water in this band is deliberately miscounted — accepted tradeoff
# (the band already under-registers ~90%, trivial vs daily usage), and drip/
# leak duty lives in the pressure-decay leak test + firmware trickle sensor,
# both independent of events (standing leak-safety invariant).
#
# PD floor = demonstrated (meter silent at 1.11 L/min); turbine floor = YF-B5
# spec startup flow (~1 L/min; untested here, and equal to the archive-
# calibrated 1.0 dribble gate it replaces). Meter class is selected by the
# same resolution predicate the old coarse-meter guard used: a 60÷ppl floor
# >= 0.5 L/min (ppl <= 120) means a positive-displacement meter in this
# product line.
_METER_FLOOR_PD_LPM:      float = 1.1
_METER_FLOOR_TURBINE_LPM: float = 1.0
_PD_CLASS_MIN_FLOW_FLOOR_LPM: float = 0.5
BELOW_METER_FLOOR_REASON: str = "below_meter_floor"

# dev33 (§1.1) — minimum raw volume for the one-shot relabel-repair scan to
# auto-restore a user-classified zeroed row. Set at 1.0 L: the production
# census showed the two genuine losses at 685.3 L and 3.9 L, and every
# false-positive candidate at <= 0.2 L, so this sits an order of magnitude
# clear of both. A conservative gate by design — a row below it that really
# was water is restored the moment the user relabels it (see
# database.revert_artifact_zeroing_on_relabel).
_RELABEL_REPAIR_MIN_VOLUME_L: float = 1.0


def _meter_registration_floor(min_flow_lpm: float) -> float:
    """Physical registration floor for the circuit's meter class."""
    return (_METER_FLOOR_PD_LPM
            if (min_flow_lpm or 0.0) >= _PD_CLASS_MIN_FLOW_FLOOR_LPM
            else _METER_FLOOR_TURBINE_LPM)


# ─────────────────────────────────────────────────────────────────────────────
# Pressure-silent flow phantom (2026-07-05)
#
# Flow the meter registered IN its valid regime, with NO supply response — a
# physical impossibility for a real draw on this class of system: ground truth
# shows even 0.75 L/min real flow dips this home 1.57 PSI, and the weakest real
# draw ever measured (1.03 L/min) dipped 1.85 PSI. The signature is air purge /
# valve-test shuttle / transient jiggle spinning the meter after line work
# (observed cluster: board-swap install afternoon 2026-07-04 — sporadic pulse
# bursts at 3–17 L/min with pressure FLAT within 0.6 PSI for minutes).
#
# LEAK-SAFETY (frozen, never calibrated):
#   • ΔP gate < 0.8 PSI sits > 2× below the weakest measured real-draw dip
#     (1.2 PSI at 1.6 L/min); a real leak at meterable rates always dips more;
#   • corr is REQUIRED (< _PSILENT_MAX_CORR) — None (no/short pressure signal,
#     flow-only imports) can never fire the verdict, mirroring the rise
#     phantom's None-is-safe rule; a real draw's corr is strongly negative;
#   • has_pressure_transient must be unset — a detected transient means the
#     supply responded;
#   • volume cap <= 5 L bounds the blast radius of any misfire;
#   • duration cap <= 300 s — observed artifacts are 20–160 s.
_PSILENT_MAX_DELTA_PSI:  float = 0.8
_PSILENT_MAX_CORR:       float = 0.3
_PSILENT_MAX_DURATION_S: float = 300.0
_PSILENT_MAX_VOLUME_L:   float = 5.0
PRESSURE_SILENT_REASON: str = "pressure_silent_flow"

# Rise-phantom volume cap on positive-displacement meters. The 1.0 L turbine
# cap was frozen against turbine-era archive data; a PD meter registers MORE of
# a pressure-rise slug (it meters everything the clearance doesn't bypass), so
# its rise phantoms are bigger. corr / rising-pressure / duration gates stay
# frozen and identical.
_RISE_PHANTOM_MAX_VOLUME_PD_L: float = 2.5

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
# Leak-test reopen refill (2026-08-04). Mirrored from leak_test_refill rather
# than imported: that module imports database, which lazily imports this one.
# test_leak_test_refill asserts the two strings are equal.
_LEAK_TEST_REFILL_REASON: str = "leak_test_refill"
# SQL guard the artifact reprocess scans append so they cannot steal a refill's
# provenance. Those scans filter on the three artifact FLAG bits, and this
# verdict deliberately sets none of them (it stays visible), so without this
# they would re-claim a refill whose shape happens to trip another detector.
_LEAK_REFILL_GUARD_SQL: str = (
    "  AND COALESCE(match_rejection_reason, '') <> 'leak_test_refill' ")
# The rest of the verdict guards every artifact reprocess scan re-tests. They
# were typed out at ~30 sites and one hand-typed pair silently dropped the
# refill guard above (unit 6.5), so they live here once, exactly as the scans
# spelled them. Each is a self-contained AND clause with a leading AND trailing
# space, safe to concatenate after any WHERE term; where a guard would be the
# FIRST term the scan opens "WHERE 1=1". Two NULL-explicit spellings of the
# same tests stay inline where they lead a WHERE (Scans 1 and 3) — SQLite
# treats "x = 0 OR x IS NULL" and "COALESCE(x,0) = 0" identically.
_NO_PHANTOM_SQL: str = (
    "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 ")
_NO_CROSS_TALK_SQL: str = "  AND COALESCE(is_cross_talk, 0) = 0 "
_NO_DRIBBLE_SQL: str = "  AND COALESCE(is_low_flow_dribble, 0) = 0 "
_NO_DEGRADED_SQL: str = "  AND COALESCE(degraded_supply, 0) = 0 "
_NOT_USER_CLASSIFIED_SQL: str = "  AND COALESCE(user_classified, 0) = 0 "
_NOT_USER_IGNORED_SQL: str = "  AND COALESCE(user_ignored, 0) = 0 "
_NO_USER_FIXTURE_TYPE_SQL: str = (
    "  AND (user_fixture_type IS NULL OR user_fixture_type = '') ")
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
    *,
    pump_mode: bool = False,
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

    if pump_mode:
        diag["pump_mode"] = True      # provenance: persisted in the stored diag
    is_degraded, reason = _evaluate_degraded_from_diag(diag, pump_mode)
    diag["reason"] = reason
    return is_degraded, diag


def _evaluate_degraded_from_diag(diag: dict, pump_mode: bool = False):
    """Apply the post-detection threshold gates to a diagnostic dict.

    Pure function over the stored diagnostic fields. Used by both the live
    detector (so the gates exist in one place) and the reprocess endpoint
    (so threshold changes apply retroactively to events with stored
    diagnostics, without needing the raw sample series).

    ``pump_mode`` marks an event captured while a constant-pressure (VFD)
    booster pump was in service — see ``_VFD_RIPPLE_MAX_PERIOD_S``.

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

    # dev33 — VFD ripple exemption. A constant-pressure pump hunts around its
    # setpoint at ~1 Hz; that ripple pattern-matches "pulsing supply" while the
    # meter stays perfectly trustworthy, and it drove degraded events from 0 to
    # 27-68/week after the 2026-07 install. Exempt the fast band, but ONLY for
    # events captured in the pump era: pre-pump genuine pulsing occupies the
    # SAME 0.8-2.0 s band, so period alone cannot discriminate — era + period
    # can. Slower supply pulsation (>= 1.5 s) stays detectable under the pump.
    if pump_mode:
        _p = diag.get("pressure_dominant_period_s")
        try:
            _p = float(_p) if _p is not None else None
        except (TypeError, ValueError):
            _p = None
        if _p is not None and _p < _VFD_RIPPLE_MAX_PERIOD_S:
            return False, VFD_RIPPLE_EXEMPT_REASON

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
                             calib=None, min_flow_lpm: float = 0.15,
                             true_avg_flow_lpm=None, peak_flow_lpm=None) -> bool:
    """True when the meter never operated in its valid regime during the event
    — the below-meter-floor rule (2026-07-05, replaces the old volume/flow/ΔP
    triple-gate on ALL meter classes; see the registration-floor block above).

    The rate test uses ACTIVE-flow metrics (true_avg_flow_lpm, peak) — never
    the zero-diluted whole-event average. Canonical framing: 0.5 L in 10 s
    (3 L/min) = real; 0.5 L spread continuously over 2 min (0.25 L/min) =
    false data; a 10 s / 3 L/min burst inside a 2-min zero-padded event window
    has the same whole-event average as the false case but was validly metered
    while flowing → kept. The verdict fires only when EVERY available active
    metric sits below the floor (max of them < floor), so one valid-regime
    burst anywhere in the event vetoes it. Volume, ΔP, and duration are
    deliberately NOT gates: crossing 0.5 L doesn't make sub-floor readings
    real, and pressure corroboration proves water moved, not how much.

    ``volume_litres`` / ``pressure_delta_psi`` are retained in the signature
    for caller compatibility and the None-guard only. Legacy callers that pass
    no active metrics fall back to the whole-event average — conservative in
    the only risky direction (padding can only make avg LOWER, and those
    callers re-derive properly on the next full reprocess).
    """
    if avg_flow_lpm is None and true_avg_flow_lpm is None and peak_flow_lpm is None:
        return False
    rates = []
    for v in (true_avg_flow_lpm, peak_flow_lpm):
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            rates.append(f)
    if not rates:
        try:
            f = float(avg_flow_lpm)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(f):
            return False
        rates = [f]
    return max(rates) < _meter_registration_floor(min_flow_lpm)


def _detect_pressure_silent_flow(duration_s, volume_litres, pressure_delta_psi,
                                 flow_pressure_corr, has_pressure_transient,
                                 true_avg_flow_lpm=None, peak_flow_lpm=None,
                                 avg_flow_lpm=None,
                                 min_flow_lpm: float = 0.15) -> bool:
    """True when validly-metered flow produced NO supply response — physically
    impossible for a real draw (see the pressure-silent constants block).

    Requires ALL of: an active-flow metric at/above the meter registration
    floor (below-floor events belong to _detect_low_flow_dribble), ΔP under
    the frozen 0.8 PSI gate, no detected pressure transient, a PRESENT
    flow↔pressure correlation under 0.3 (None = no pressure evidence = never
    fires), duration <= 300 s, and volume in (0, 5] L. Every input None-safe
    in the conservative direction.
    """
    vals = {}
    for name, v in (("dur", duration_s), ("vol", volume_litres),
                    ("dp", pressure_delta_psi), ("corr", flow_pressure_corr)):
        if v is None:
            return False
        try:
            f = float(v)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(f):
            return False
        vals[name] = f
    if has_pressure_transient:
        return False
    rates = []
    for v in (true_avg_flow_lpm, peak_flow_lpm, avg_flow_lpm):
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            rates.append(f)
    if not rates or max(rates) < _meter_registration_floor(min_flow_lpm):
        return False
    return (
        vals["dur"] <= _PSILENT_MAX_DURATION_S
        and 0.0 < vals["vol"] <= _PSILENT_MAX_VOLUME_L
        and vals["dp"] < _PSILENT_MAX_DELTA_PSI
        and vals["corr"] < _PSILENT_MAX_CORR
    )


# ── Pump-recharge absorber (dev24, pump plan Phase 4 — vfd profile only) ──────
# A booster pump's recharge slug: a brief, small metered burst pushed toward a
# downstream leak while supply pressure RISES. Only active when pump mode is
# confirmed with the vfd profile (config.pump_gates_active). The 2026-07 storm
# scattered these across four artifact classes (below_meter_floor /
# pressure_silent_flow / rising_pressure_phantom / pulsing_supply); this class
# names them and replaces the two whose static-supply premises are false under
# a sawtooth. NOTE: the metered slug is ~half the true slug (street-meter
# calibration factor 1.9) — PUMP_SLUG_MAX_L bounds the METERED number.
PUMP_RECHARGE_REASON = "pump_recharge"
# dev51 (Phase 5) — provenance for exclusions that used to be SILENT. The
# finalizer ORed these three causes into excluded_from_training but its reason
# chain had no branch for them, the dribble-restore path actively NULLed the
# reason on every boot, and the Ignore button wrote the flag bare. ~198 rows
# on the 2026-09-01 export were excluded with no recorded reason — exactly the
# unexplained exclusion the quarantine columns exist to prevent.
USER_IGNORED_REASON = "user_ignored"
INTEGRATION_DEGRADED_REASON = "integration_degraded"
PHANTOM_AVERTED_REASON = "phantom_averted"
COMPOSITE_REASON = "composite"
LEGACY_EXCLUDED_REASON = "excluded_legacy"      # backfill: cause not recoverable
PUMP_SLUG_MAX_L: float = 0.6          # metered; 2x the largest observed slug
_PUMP_SLUG_MAX_DURATION_S: float = 60.0
_PUMP_SLUG_MIN_CORR: float = 0.5      # flow-during-rise (phase-aligned)
_PUMP_SLUG_SILENT_DP_PSI: float = 0.8 # or: too brief for a pressure verdict
# Third prong (2026-08, validated on the 08-09 production export): the
# sawtooth micro-cycle. The bypass leak bleeds the line down SLOWLY until the
# pump restarts — a pressure-triggered start whose measured drop is just the
# restart deadband (~1.2-1.7 PSI) reached at leak-decay speed, far below
# demand speed (bench: demand 5-12 PSI/s vs leak 0.37). These fail both
# original prongs: dP sits above the 0.8 "quiet" bar, and the corr is diluted
# toward 0 by the decay-then-recover pressure curve. Export eval: 69 matches
# over 21 days, spread around the clock (leak-consistent), 1/69 ever
# user-touched, ≤0.6 L each (leak-safe under the frozen slug cap).
_PUMP_SAWTOOTH_MIN_DURATION_S: float = 5.0   # sharp blips stay with prongs 1/2
_PUMP_SAWTOOTH_MAX_DP_PSI: float = 2.5       # restart deadband, with margin
_PUMP_SAWTOOTH_MAX_FALL_PSI_S: float = 0.7   # leak-decay speed ceiling
_PUMP_SAWTOOTH_MIN_CORR: float = -0.1        # a clearly demand-shaped corr vetoes


def _detect_pump_recharge(duration_s, volume_litres, flow_pressure_corr,
                          pressure_delta_psi,
                          pressure_transient_duration_ms=None,
                          start_trigger=None) -> bool:
    """True = this event is a pump recharge slug (pump mode only — caller
    gates). Three signatures, matching how the storm actually presented:
      * phase-aligned: flow coincided with the pressure RISE (corr >= 0.5 —
        the same signal the rise-phantom detector reads, but under a pump the
        physical cause is the pump pushing water, and the water is real);
      * pressure-quiet: the slug is too brief/small for a meaningful supply
        response (|dP| <= 0.8 — the pressure_silent signature);
      * sawtooth micro-cycle: a pressure-TRIGGERED start whose drop is small
        (restart deadband) and was reached at leak-decay speed, with no
        demand-shaped (strongly negative) correlation to veto it. Callers
        that can't supply transient duration / trigger simply never fire
        this prong — prongs 1 and 2 are unchanged.
    A short small draw with a REAL pressure dip and negative correlation (an
    icemaker fill between recharges) matches none and stays a normal
    event. Known v1 limitation: a micro-draw that TRIGGERS a recharge merges
    with it and classifies half-wrong either way (plan round-4 #8)."""
    try:
        dur = float(duration_s or 0.0)
        vol = float(volume_litres or 0.0)
    except (TypeError, ValueError):
        return False
    if not (0.0 < vol <= PUMP_SLUG_MAX_L and 0.0 < dur <= _PUMP_SLUG_MAX_DURATION_S):
        return False
    corr = flow_pressure_corr
    if corr is not None:
        try:
            if float(corr) >= _PUMP_SLUG_MIN_CORR:
                return True
        except (TypeError, ValueError):
            corr = None
    try:
        dp = abs(float(pressure_delta_psi)) if pressure_delta_psi is not None else None
    except (TypeError, ValueError):
        dp = None
    if dp is not None and dp <= _PUMP_SLUG_SILENT_DP_PSI:
        return True
    # Prong 3 — sawtooth micro-cycle.
    if (not str(start_trigger or "").startswith("pressure")
            or dur < _PUMP_SAWTOOTH_MIN_DURATION_S
            or dp is None or dp > _PUMP_SAWTOOTH_MAX_DP_PSI):
        return False
    try:
        transient_ms = float(pressure_transient_duration_ms or 0.0)
    except (TypeError, ValueError):
        transient_ms = 0.0
    if transient_ms <= 0.0:
        return False   # no fall-rate verdict possible → keep the water
    if dp / (transient_ms / 1000.0) > _PUMP_SAWTOOTH_MAX_FALL_PSI_S:
        return False   # pressure fell at demand speed — a real draw
    if corr is not None:
        try:
            if float(corr) <= _PUMP_SAWTOOTH_MIN_CORR:
                return False   # demand-shaped: flow pulled pressure down
        except (TypeError, ValueError):
            pass
    return True


def _detect_rising_pressure_phantom(duration_s, volume_litres,
                                    flow_pressure_corr, calib=None,
                                    min_flow_lpm: float = 0.15) -> bool:
    """True when a SHORT small burst's flow TRACKED a pressure RISE — i.e. the
    meter was spun by climbing supply pressure, not by demand (dev14).

    Fingerprint: flow↔pressure correlation at/above the validated cutoff
    (``RISE_PHANTOM_MIN_CORR``; a real draw is strongly NEGATIVE), volume
    STRICTLY under the meter-class cap (frozen — turbine 1.0 L keeps every
    zeroed event below detector_validation's SUSPECT_ZERO_LITRES leak bar;
    PD meters use 2.5 L because they register more of a pressure-rise slug —
    see _RISE_PHANTOM_MAX_VOLUME_PD_L), and duration at/under
    ``_RISE_PHANTOM_MAX_DURATION_S`` (frozen — the long phantom owns >= 120 s).
    Callers that don't know the circuit (legacy/repair paths) default to the
    turbine cap — the conservative smaller one.

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
    vol_cap = (_RISE_PHANTOM_MAX_VOLUME_PD_L
               if (min_flow_lpm or 0.0) >= _PD_CLASS_MIN_FLOW_FLOOR_LPM
               else _RISE_PHANTOM_MAX_VOLUME_L)
    return (
        corr >= _ac(calib, "RISE_PHANTOM_MIN_CORR")
        and 0.0 < vol < vol_cap
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


def apply_pinned_verdict(features: dict) -> bool:
    """dev56 — enforce ``verdict_pin`` on a features dict. Returns True when a
    pin decided the volume (callers return early). The overlap family: the
    effective volume is whatever the pin prescribes (0 for a full duplicate,
    the uncovered remainder for a partial one), the row stays out of training
    and carries no phantom bit. A user fixture label does NOT lift it —
    labelling a wrapper says what it was, not that its water should count twice
    (the children still exist); release comes from the overlap guard when the
    children disappear, or from deleting the row. Identity fields are left
    alone. Unknown pin values are ignored (a later family may claim them)."""
    if features.get("verdict_pin") != "overlap_duplicate":
        return False
    veff = float(features.get("verdict_pin_veff") or 0.0)
    raw = float(features.get("volume_litres") or 0.0)
    features["volume_litres_effective"] = round(min(veff, raw) if raw else veff, 3)
    features["volume_estimation_method"] = "overlap_duplicate"
    features["match_rejection_reason"] = "overlap_duplicate"
    features["excluded_from_training"] = 1
    features["is_pressure_restoration_phantom"] = 0
    features["is_cross_talk"] = 0
    features["is_low_flow_dribble"] = 0
    features["phantom_suppression_averted"] = 0
    return True


def _finalize_derived_verdicts(features: dict, calib=None,
                               min_flow_lpm: float = 0.15,
                               pump_gates: bool = False) -> None:
    """Single source of truth for the phantom verdict + its dependent fields.

    ``calib`` is the frozen per-home artifact-detector calibration (Phase 2.4) —
    overrides only the long-quiet / dribble identifier thresholds; the leak-safety
    true-flow guards are never calibrated. None → shipped defaults.

    Recomputes, IN PLACE, from the CURRENT values in ``features``:
        is_pressure_restoration_phantom, volume_litres_effective,
        volume_estimation_method, excluded_from_training, match_rejection_reason,
        hydraulic_resistance (dev38 — always ΔP/avg from the CURRENT ΔP)

    Idempotent — safe to call again after ``_enrich_from_waveform`` mutates
    pressure_delta_psi / peak_flow_lpm, so the stored verdict always matches the
    stored pressure (fixes the late-ESP-waveform staleness bug where a real
    long shower kept a stale phantom flag + zeroed volume).

    Skips rows the user has manually classified (``user_classified`` == 1) —
    their category flags are authoritative and must never be auto-overridden.

    Reads: duration_seconds, pressure_delta_psi, degraded_supply, is_composite,
    user_ignored, volume_litres, volume_litres_estimated.
    """
    # dev38: hydraulic_resistance must always match the CURRENT ΔP. ESP
    # enrichment overwrites pressure_delta_psi AFTER extract_features computed
    # resistance, so the stored ratio went stale on every ESP-enriched event
    # (1,324 rows in the 2026-08 audit). Pinned definition — identical to
    # extract_features: ΔP / avg_flow_lpm, gated on avg >= 0.15,
    # has_pressure_transient, ΔP > 0; NULL otherwise, including NULL ΔP on
    # de-enriched shared-capture rows. Runs BEFORE the user_classified guard:
    # resistance is a measurement, not a classification, so a manual label
    # must not preserve a stale ratio.
    _res_dp = features.get("pressure_delta_psi")
    _res_avg = features.get("avg_flow_lpm")
    if (_res_dp is not None and _res_avg is not None and _res_avg >= 0.15
            and features.get("has_pressure_transient") and _res_dp > 0):
        features["hydraulic_resistance"] = round(_res_dp / _res_avg, 3)
    else:
        features["hydraulic_resistance"] = None

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

    # Durable leak-test reopen refill — set out-of-band by
    # leak_test_refill.reconcile_leak_test_refills from the add-on's OWN test
    # timing. No single-event detector can reproduce it (the event looks like a
    # small correctly-metered draw, because that is exactly what it is), so a
    # recompute would restore the volume and drop the provenance. Preserve —
    # unless the user has since applied a real fixture label. Note this branch
    # sets no artifact flag bit: the verdict zeroes volume and excludes from
    # training but stays VISIBLE in History (see the leak_test_refill module).
    if (features.get("match_rejection_reason") == _LEAK_TEST_REFILL_REASON
            and not str(features.get("user_fixture_type") or "").strip()):
        features["volume_litres_effective"] = 0.0
        features["volume_estimation_method"] = _LEAK_TEST_REFILL_REASON
        features["excluded_from_training"] = 1
        features["match_rejection_reason"] = _LEAK_TEST_REFILL_REASON
        features["is_pressure_restoration_phantom"] = 0
        features["is_cross_talk"] = 0
        features["is_low_flow_dribble"] = 0
        features["phantom_suppression_averted"] = 0
        return

    # dev56 — a PINNED verdict (cross-event evidence the single-event detectors
    # below cannot reproduce) outranks everything from here on. It used to be
    # keyed on match_rejection_reason, which a re-store never loaded, and it
    # forced veff to 0 even for a partial-remainder wrapper (PIPELINE 7d hazard).
    if apply_pinned_verdict(features):
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
    # Pump-recharge absorber (dev24, ``pump_gates`` = confirmed vfd pump mode):
    # runs FIRST among the small-event verdicts and REPLACES the two detectors
    # whose static-supply premises are false under a pump sawtooth
    # (rising_pressure_phantom: real draws coinciding with a recharge upswing
    # get positive corr; pressure_silent_flow: a real draw during the upswing
    # can look pressure-silent). Skipping them can only ADD events — zeroing
    # never expands — so the swap is leak-safe. The recharge water is real
    # (it feeds the leak) but is not fixture usage: zero effective volume like
    # the artifact family; leak ACCOUNTING lives in the street-calibrated
    # Phase 5 estimator, not the usage totals.
    is_pump_recharge = (
        pump_gates and not is_phantom and not is_degraded and not has_user_type
        and _detect_pump_recharge(
            features.get("duration_seconds"), features.get("volume_litres"),
            features.get("flow_pressure_corr"),
            features.get("pressure_delta_psi"),
            pressure_transient_duration_ms=features.get(
                "pressure_transient_duration_ms"),
            start_trigger=features.get("start_trigger"))
    )
    is_rise_phantom = (
        not pump_gates
        and not is_phantom and not is_degraded and not has_user_type
        and _detect_rising_pressure_phantom(
            features.get("duration_seconds"), features.get("volume_litres"),
            features.get("flow_pressure_corr"), calib=calib,
            min_flow_lpm=min_flow_lpm)
    )
    # Cross-talk: a long no-flow event with a REAL pressure drop (ΔP >= 2.0) — the
    # other circuit's draw pulled this circuit's pressure down. ΔP-exclusive with the
    # phantom (ΔP < 2.0). Gated off degraded events (their flow metrics are unreliable,
    # so the no-flow signal can't be trusted). Zeroes volume + excludes, like a phantom.
    is_cross_talk = (
        not is_phantom and not is_rise_phantom and not is_pump_recharge
        and not is_degraded
        and not has_user_type
        and _detect_cross_talk(
            features.get("duration_seconds"), features.get("pressure_delta_psi"),
            features.get("flow_integral_litres"), features.get("flow_on_ratio"),
            calib=calib)
    )
    # Below-meter-floor (2026-07-05, replaces the dribble triple-gate): the
    # meter never operated in its valid regime during the event — the reading
    # is false information regardless of ΔP, volume, or duration. Zeroes +
    # excludes. Reuses the is_low_flow_dribble flag/UI plumbing with a
    # distinct reason. Gated off user types like every zeroing verdict.
    is_dribble = (
        not is_phantom and not is_rise_phantom and not is_pump_recharge
        and not is_cross_talk
        and not is_degraded and not has_user_type
        and _detect_low_flow_dribble(
            features.get("volume_litres"), features.get("avg_flow_lpm"),
            features.get("pressure_delta_psi"), calib=calib,
            min_flow_lpm=min_flow_lpm,
            true_avg_flow_lpm=features.get("true_avg_flow_lpm"),
            peak_flow_lpm=features.get("peak_flow_lpm"))
    )
    # Pressure-silent flow (2026-07-05): validly-metered flow with NO supply
    # response — physically impossible as a real draw (see constants block).
    # Partition: below-floor owns rates under the registration floor; this
    # verdict owns rates at/above it.
    is_pressure_silent = (
        not pump_gates
        and not is_phantom and not is_rise_phantom and not is_cross_talk
        and not is_dribble and not is_degraded and not has_user_type
        and _detect_pressure_silent_flow(
            features.get("duration_seconds"), features.get("volume_litres"),
            features.get("pressure_delta_psi"),
            features.get("flow_pressure_corr"),
            features.get("has_pressure_transient"),
            true_avg_flow_lpm=features.get("true_avg_flow_lpm"),
            peak_flow_lpm=features.get("peak_flow_lpm"),
            avg_flow_lpm=features.get("avg_flow_lpm"),
            min_flow_lpm=min_flow_lpm)
    )

    raw = float(features.get("volume_litres") or 0.0)
    est = features.get("volume_litres_estimated")
    est = float(est) if est is not None else raw

    # Effective volume: phantom family → 0 (false water); cross-talk → 0;
    # below-meter-floor → 0 (sub-floor readings are outside the meter's valid
    # regime — false information whether or not real water was behind them;
    # 2026-07-05 registration-floor doctrine); pressure-silent → 0 (physically
    # impossible as a real draw); degraded → envelope estimate; else raw.
    # Leak-safety for the zeroing branches lives in the detectors' frozen
    # gates plus the standing invariant: drip/leak duty is the firmware
    # trickle sensor + pressure-decay leak test, independent of events.
    if is_phantom or is_rise_phantom:
        features["volume_litres_effective"]  = 0.0
        features["volume_estimation_method"] = "pressure_restoration_phantom"
    elif is_pump_recharge:
        features["volume_litres_effective"]  = 0.0
        features["volume_estimation_method"] = PUMP_RECHARGE_REASON
    elif is_pressure_silent:
        features["volume_litres_effective"]  = 0.0
        features["volume_estimation_method"] = PRESSURE_SILENT_REASON
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
        features["volume_estimation_method"] = BELOW_METER_FLOOR_REASON
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
    # pump_recharge joins the phantom FLAG family so the hide-toggle / zeroing
    # plumbing applies unchanged; its distinct reason keeps provenance.
    features["is_pressure_restoration_phantom"] = (
        1 if (is_phantom or is_rise_phantom or is_pressure_silent
              or is_pump_recharge) else 0)
    features["is_cross_talk"] = 1 if is_cross_talk else 0
    features["is_low_flow_dribble"] = 1 if is_dribble else 0
    # Kept-but-questioned draw: volume kept (raw/degraded branch above), out of
    # training until reviewed, surfaced via score_event_anomaly.
    features["phantom_suppression_averted"] = 1 if phantom_averted else 0
    features["excluded_from_training"] = (
        1 if (is_degraded or is_phantom or is_rise_phantom or is_pressure_silent
              or is_pump_recharge
              or is_cross_talk or is_dribble or user_ignored
              or integration_unusable or is_sparse_envelope or phantom_averted)
        else 0
    )
    # Upstream rejection reason (cluster-engine reasons are written separately).
    # is_low_flow_dribble is the authoritative dribble state; this reason string
    # is a secondary signal kept consistent with the live finalizer. The rise
    # and pressure-silent phantoms share the phantom flag but keep their own
    # reasons — the only place their provenance is recorded; below_meter_floor
    # likewise shares the dribble flag with a distinct reason.
    features["match_rejection_reason"] = (
        "pressure_restoration_phantom" if is_phantom
        else RISE_PHANTOM_REASON if is_rise_phantom
        else PUMP_RECHARGE_REASON if is_pump_recharge
        else PRESSURE_SILENT_REASON if is_pressure_silent
        else "cross_talk" if is_cross_talk
        else "pulsing_supply" if is_degraded
        else BELOW_METER_FLOOR_REASON if is_dribble
        else SPARSE_ENVELOPE_REASON if is_sparse_envelope
        # dev51 (Phase 5) — the three causes the exclusion ORs in but this
        # chain never named. Ranked below every physical-artifact reason so an
        # artifact keeps its more specific provenance; an exclusion may now
        # never be written without a reason from this finalizer.
        else USER_IGNORED_REASON if user_ignored
        else INTEGRATION_DEGRADED_REASON if integration_unusable
        else PHANTOM_AVERTED_REASON if phantom_averted
        else None
    )


# ── Batch-pass driver for the zeroing-verdict sweeps (unit 6.4) ──────────────
# Five passes below re-derive a VOLUME-ZEROING verdict over stored events and
# were, line for line, the same walk: skip pump-gated circuits, re-run the
# canonical detector (the SQL is only ever a prefilter), write the verdict in
# the row's OWN transaction, zero the ledger through the §2.5
# ``apply_effective_volume`` chokepoint, count it, remember the home-local day,
# then rebuild each affected day's summary once and commit.
#
# The driver owns ONLY that mechanism. Each pass still supplies its own
# candidate query, its own canonical predicate, its own SET clause and its own
# counters/log lines, and the passes are never merged into one another: they
# are separate verdicts and stay separately auditable. Two behaviours are
# deliberately NOT generalised here — the bidirectional dribble scan (which
# also RESTORES volume) and the relabel-repair scan (which restores rather than
# zeroes) keep their own loops.
#
# Rule N2a: one write = one transaction ending in its own commit. The per-row
# ``transaction(conn)`` below is exactly that, and is what lets a user's label
# save win the write lock between rows — so this driver must never be
# "simplified" into a single transaction around the whole walk.

def _pump_gate_blocks(conn, row, label: str) -> bool:
    """True when a sweep must SKIP ``row`` on pump-gate grounds (dev25/§2.29).

    In confirmed vfd pump mode the pressure-silent and rise-phantom premises
    are false (a real draw on a recharge upswing looks like both), so the live
    path routes those through the pump_recharge absorber and the sweep must not
    re-apply the verdict out-of-band. If the gate cannot be EVALUATED we do not
    know whether the premise holds, and unusable data must not authorise the
    destructive action — skip loudly and keep the volume.
    """
    try:
        from .config import pump_gates_active as _pga_sweep
        return bool(_pga_sweep(conn, row["circuit"]))
    except Exception as e:   # noqa: BLE001
        log.warning("[%s] %s sweep: pump-gate check failed for event %s (%s) "
                    "— SKIPPING the row, volume kept (re-run the sweep once "
                    "the pump state is readable)",
                    row["circuit"], label, row["id"], e)
        return True


def _sweep_zeroing_verdict(conn, rows, *, detect, set_sql, set_params=None,
                           pump_gate=None, on_flag=None, veff_key=None):
    """Walk ``rows``, applying one zeroing verdict. See the block comment above.

    ``detect(row)``      canonical predicate; the SQL is only a prefilter.
    ``set_sql``          the UPDATE's SET clause body (no ``WHERE``).
    ``set_params(row)``  params for ``set_sql``'s placeholders, if any.
    ``pump_gate``        sweep label enabling the dev25 pump-gate skip.
    ``on_flag(row)``     per-row logging, called after the row is counted.
    ``veff_key``         row key whose value accumulates into ``litres``.

    Returns ``(flagged, litres, days)`` — ``days`` is the set of
    ``(circuit, home-local day)`` pairs whose summary was rebuilt.
    """
    from .database import (transaction, compute_daily_summary,
                           apply_effective_volume, local_day_of)
    flagged = 0
    litres = 0.0
    days: set = set()
    for row in rows:
        if pump_gate and _pump_gate_blocks(conn, row, pump_gate):
            continue
        if not detect(row):
            continue
        params = tuple(set_params(row)) if set_params else ()
        with transaction(conn):
            conn.execute("UPDATE events SET " + set_sql + " WHERE id = ?",
                         params + (row["id"],))
            # §2.5 — zero the ledger contribution via the one chokepoint.
            apply_effective_volume(conn, row["id"], row["circuit"],
                                   row["start_ts"], 0)
        flagged += 1
        if veff_key:
            litres += float(row[veff_key] or 0.0)
        day = local_day_of(row["start_ts"])
        if day:
            days.add((row["circuit"], day))
        if on_flag:
            on_flag(row)
    for circ, day in days:
        compute_daily_summary(conn, circ, day)
    if flagged:
        conn.commit()
    return flagged, litres, days


def backfill_silent_exclusion_reasons(conn: sqlite3.Connection) -> dict:
    """dev51 (Phase 5) — give every excluded-without-reason row a reason.

    Idempotent and cheap (one indexed-ish scan of the excluded set). The
    reason is derived from the row's own flags in the same priority order the
    live finalizer now uses; a row whose cause is not recoverable from its
    columns gets LEGACY_EXCLUDED_REASON so it stops being *silent* even though
    it cannot be *explained*. Never touches a row that already has a reason,
    and never changes ``excluded_from_training`` itself.
    """
    from .database import transaction
    cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    if "match_rejection_reason" not in cols:
        return {"backfilled": 0}
    has = lambda c: c in cols                                           # noqa: E731
    cases = []
    if has("user_ignored"):
        cases.append(f"WHEN COALESCE(user_ignored,0)=1 THEN '{USER_IGNORED_REASON}'")
    if has("integration_quality"):
        cases.append(f"WHEN integration_quality='degraded' "
                     f"THEN '{INTEGRATION_DEGRADED_REASON}'")
    if has("phantom_suppression_averted"):
        cases.append(f"WHEN COALESCE(phantom_suppression_averted,0)=1 "
                     f"THEN '{PHANTOM_AVERTED_REASON}'")
    if has("is_composite"):
        cases.append(f"WHEN COALESCE(is_composite,0)=1 THEN '{COMPOSITE_REASON}'")
    case_sql = " ".join(cases)
    with transaction(conn):
        n = conn.execute(
            "UPDATE events SET match_rejection_reason = CASE " + case_sql +
            f" ELSE '{LEGACY_EXCLUDED_REASON}' END "
            "WHERE COALESCE(excluded_from_training,0) = 1 "
            "  AND match_rejection_reason IS NULL").rowcount or 0
    if n:
        log.info("exclusion provenance backfill: %d excluded event(s) had no recorded "
                 "reason — now stamped from their own flags", n)
    return {"backfilled": n}


def repair_artifact_flag_consistency(conn: sqlite3.Connection) -> dict:
    """One-time idempotent repair of cross-cutting artifact-flag invariants (P2).

    Two fixes, both safe to re-run:
      A. Any row with a volume-ZEROING flag (phantom, cross-talk or dribble) set
         must have ``excluded_from_training = 1`` — repairs rows where a recompute
         path left a zeroed event still feeding training (the is_cross_talk=1 /
         excluded=0 case).
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
    ``{"excluded_fixed", "pairs_resolved", "unresolved", "rezeroed"}``.
    """
    if not (_events_has_column(conn, "is_cross_talk")
            and _events_has_column(conn, "is_low_flow_dribble")):
        return {"excluded_fixed": 0, "pairs_resolved": 0, "unresolved": 0,
                "rezeroed": 0}

    EPS = 1e-6

    # ── A: a zeroing flag ⇒ excluded_from_training = 1 ──────────────────────────
    # dev57 (§2.31) — is_low_flow_dribble belongs in this list. Section B's own
    # docstring above says all three zeroing verdicts zero volume (the dribble
    # flag has zeroed since 2026-06-19 / below_meter_floor), and section B's
    # ``new_excluded`` already ORs new_dr in — but section A, which is what fixes
    # the SINGLE-flag rows section B never looks at (it selects >= 2 flags only),
    # named just two of the three, so a dribble-flagged row left excluded=0 by a
    # recompute path went on feeding training with a reading the meter's own
    # registration floor says is false information.
    cur = conn.execute(
        "UPDATE events SET excluded_from_training = 1 "
        "WHERE (COALESCE(is_pressure_restoration_phantom,0) = 1 "
        "       OR COALESCE(is_cross_talk,0) = 1 "
        "       OR COALESCE(is_low_flow_dribble,0) = 1) "
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
        "  flow_integral_litres, flow_on_ratio, avg_flow_lpm, peak_flow_lpm, "
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
                        else "psilent"    if mrr == PRESSURE_SILENT_REASON
                        else "cross_talk" if mrr == "cross_talk"
                        else "dribble"    if mrr in ("low_flow_dribble",
                                                     BELOW_METER_FLOOR_REASON)
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
                        r["pressure_delta_psi"],
                        true_avg_flow_lpm=r["true_avg_flow_lpm"],
                        peak_flow_lpm=r["peak_flow_lpm"]):
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

        new_ph = 1 if keep in ("phantom", "rise", "psilent") else 0
        new_ct = 1 if keep == "cross_talk" else 0
        new_dr = 1 if keep == "dribble" else 0
        new_excluded = 1 if (new_ph or new_ct or new_dr or r["dg"] or r["ui"]) else 0
        reason = (RISE_PHANTOM_REASON if keep == "rise"
                  else PRESSURE_SILENT_REASON if keep == "psilent"
                  else "pressure_restoration_phantom" if new_ph
                  else "cross_talk" if new_ct
                  else "pulsing_supply" if r["dg"]
                  else BELOW_METER_FLOOR_REASON if new_dr
                  else None)
        conn.execute(
            "UPDATE events SET is_pressure_restoration_phantom = ?, "
            "  is_cross_talk = ?, is_low_flow_dribble = ?, "
            "  excluded_from_training = ?, match_rejection_reason = ? WHERE id = ?",
            (new_ph, new_ct, new_dr, new_excluded, reason, r["id"]),
        )
        pairs_resolved += 1

    # C (dev56). A single zeroing flag with water still counted: the row says
    # "not real water" and counts it anyway. Section B only sees >= 2 flags, so
    # this shape sat contradictory forever (two rows on the reference home).
    #
    # dev57 (§2.36) — this call STAYS on the boot path, deliberately. It was
    # proposed for removal as "a one-shot repair that now runs on every boot",
    # but the rule has to have a carrier here: migration 20260818 is the only
    # other caller, and a home already stamped at 20260818 never re-runs it (see
    # test_verdict_pin.test_boot_flag_repair_rezeroes_a_lone_zeroing_flag_with_volume
    # — that is how the reference home's own two rows were repaired, in e4316da,
    # by THIS path and not by the migration). The cost objection does not survive
    # contact with the function it is made about: sections A and B above already
    # scan the same table twice on the same boot, so C's scan is not a new order
    # of cost. What DID need fixing is the flattening — that scan rewrote
    # match_rejection_reason with its flag's generic family name, erasing
    # irrigation_cross_talk / rising_pressure_phantom / pump_recharge, whose
    # survival across reprocessing rests entirely on that string. Fixed inside
    # rezero_rows_with_zeroing_flag, which fixes it for the migration caller too
    # — fencing this call would only have hidden it from one of the two.
    from .database import rezero_rows_with_zeroing_flag
    rezeroed = rezero_rows_with_zeroing_flag(conn)
    if excluded_fixed or pairs_resolved or unresolved or rezeroed:
        conn.commit()
        log.info("flag-repair: %d excluded-from-training fixed, %d flag collisions "
                 "resolved, %d unresolved, %d zeroing-flag rows re-zeroed",
                 excluded_fixed, pairs_resolved, unresolved, rezeroed)
    # dev57 (§2.36) — ``rezeroed`` is returned, not just logged. It was counted,
    # named in the log line, and then dropped from the dict, so the only caller
    # that reports this repair (reprocess_event_exclusion_verdicts) could not.
    return {"excluded_fixed": excluded_fixed, "pairs_resolved": pairs_resolved,
            "unresolved": unresolved, "rezeroed": rezeroed}


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
    from .database import (transaction, compute_daily_summary,
                           apply_effective_volume, local_day_of)

    # Repair any cross-cutting flag-consistency violations first (P2): zeroed events
    # that slipped through still feeding training, and stale mutually-exclusive flags.
    repair = repair_artifact_flag_consistency(conn)

    # Skip manually-classified rows so a startup re-run never re-flags an event
    # the user deliberately un-marked. Column-guarded because the back-compat
    # wrapper is called from the 20260532 migration, before user_classified
    # exists in a sequential upgrade.
    uc_guard = (
        _NOT_USER_CLASSIFIED_SQL
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
        _NO_USER_FIXTURE_TYPE_SQL
        if _events_has_column(conn, "user_fixture_type") else ""
    )
    # A leak-test reopen refill already has its verdict, and its FLAG bits are
    # deliberately clear (it stays visible), so the scans' flag filters below
    # would otherwise let another detector re-claim it and lose the provenance.
    #
    # ⚠️ This used to be `uft_guard += _LEAK_REFILL_GUARD_SQL`, and that one
    # line is the root cause of the same bug three times over. Appending it
    # INTO a variable named for the user-fixture-type guard meant any scan that
    # legitimately did not need a fixture-type guard dropped the refill guard
    # with it, invisibly. Scan 3 hand-typed the user guards and lost it (6.5);
    # the capped re-include never had it; the degraded-supply scan never had
    # any user guard at all and so silently erased provenance on a route with
    # no self-heal behind it.
    #
    # So the guards a zeroing scan must carry now have ONE name that says so.
    # Concatenation order is unchanged, so every emitted statement is
    # byte-identical to before — this buys nothing at runtime and makes the
    # omission visible in review, which is where all three were missed.
    scan_guards = uc_guard + uft_guard + _LEAK_REFILL_GUARD_SQL
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
        + scan_guards,
        dur_params + (_PHANTOM_MAX_DELTA_PSI,),
    ).fetchall()

    # The driver re-runs the canonical detector rather than trusting the SQL
    # prefilter — thresholds stay in one place and bad data is guarded — and
    # recomputes the daily_summary for every affected day so the History
    # charts/totals shed the false volume immediately (compute_daily_summary
    # reads volume_litres_effective, which the driver just zeroed; the hourly
    # ledger was already corrected by its per-event reversal).
    flagged, _litres, affected_days = _sweep_zeroing_verdict(
        conn, rows,
        detect=lambda row: _detect_pressure_restoration_phantom(
            row["duration_seconds"], row["pressure_delta_psi"],
            true_avg_flow_lpm=(row["true_avg_flow_lpm"] if has_af else None),
            flow_integral_litres=(row["flow_integral_litres"] if has_af else None),
            flow_on_ratio=(row["flow_on_ratio"] if has_af else None),
        ),
        set_sql=("  is_pressure_restoration_phantom = 1, "
                 "  is_cross_talk = 0, is_low_flow_dribble = 0, "  # phantom wins
                 "  volume_litres_effective = 0, "
                 "  volume_estimation_method = 'pressure_restoration_phantom', "
                 "  excluded_from_training = 1, "
                 "  match_rejection_reason = 'pressure_restoration_phantom' "),
        on_flag=lambda row: log.info(
            "phantom-reprocess: event %s flagged (duration=%.0fs ΔP=%.2f); "
            "reversed %.3f L from hourly bucket %s",
            row["id"], row["duration_seconds"] or 0.0,
            row["pressure_delta_psi"] or 0.0,
            float(row["hourly_volume_applied_litres"] or 0.0),
            row["hourly_volume_applied_bucket"],
        ),
    )

    if flagged:
        log.info("phantom-reprocess: flagged %d event(s) total across %d day(s)",
                 flagged, len(affected_days))

    # ── Scan 2: below-meter-floor (2026-07-05 — replaces the dribble triple-gate)
    # An event whose ACTIVE flow never reached the circuit meter's registration
    # floor is zeroed + excluded regardless of ΔP, volume, or duration: the
    # reading is outside the meter's valid operating regime and is false
    # information either way (see the registration-floor constants block).
    # BIDIRECTIONAL (retroactive-backfill semantics, user-approved): rows the
    # OLD triple-gate flagged that the floor rule does NOT match (e.g. a brief
    # valid-regime burst that averaged low) are UN-flagged and their raw volume
    # restored through the same ledger chokepoint. user_classified rows and
    # user_fixture_type rows are never touched. Idempotent both directions.
    dribbles_flagged = 0
    dribbles_restored = 0
    litres_zeroed = 0.0
    dr_days: set = set()
    if _events_has_column(conn, "is_low_flow_dribble"):
        af_cols = (", true_avg_flow_lpm" if has_af else "")
        # Loose SQL prefilter (1.2 covers both class floors); the canonical
        # detector decides exactly, per-circuit. Arms: (a) not-yet-flagged
        # sub-floor candidates, (b) every currently-flagged auto dribble —
        # for re-zeroing (veff>0 self-heal) or restoration under the new rule.
        drows = conn.execute(
            "SELECT id, circuit, start_ts, volume_litres, avg_flow_lpm, "
            "       peak_flow_lpm, pressure_delta_psi, integration_quality, "
            "       COALESCE(user_ignored, 0) AS ui, "
            "       COALESCE(is_low_flow_dribble, 0) AS dr, "
            "       COALESCE(volume_litres_effective, volume_litres, 0) AS veff"
            + af_cols +
            " FROM events "
            "WHERE 1=1 "
            + _NOT_USER_CLASSIFIED_SQL + uft_guard + _LEAK_REFILL_GUARD_SQL
            + _NO_PHANTOM_SQL + _NO_CROSS_TALK_SQL + _NO_DEGRADED_SQL +
            "  AND (COALESCE(is_low_flow_dribble, 0) = 1 "
            "       OR (COALESCE(peak_flow_lpm, avg_flow_lpm, 0) < 1.2 "
            "           AND COALESCE(true_avg_flow_lpm, avg_flow_lpm, 0) < 1.2))"
            if has_af else
            "SELECT id, circuit, start_ts, volume_litres, avg_flow_lpm, "
            "       peak_flow_lpm, pressure_delta_psi, integration_quality, "
            "       COALESCE(user_ignored, 0) AS ui, "
            "       COALESCE(is_low_flow_dribble, 0) AS dr, "
            "       COALESCE(volume_litres_effective, volume_litres, 0) AS veff "
            "FROM events "
            "WHERE 1=1 "
            + _NOT_USER_CLASSIFIED_SQL + uft_guard + _LEAK_REFILL_GUARD_SQL
            + _NO_PHANTOM_SQL + _NO_CROSS_TALK_SQL + _NO_DEGRADED_SQL +
            "  AND (COALESCE(is_low_flow_dribble, 0) = 1 "
            "       OR COALESCE(peak_flow_lpm, avg_flow_lpm, 0) < 1.2)"
        ).fetchall()
        for row in drows:
            below = _detect_low_flow_dribble(
                row["volume_litres"], row["avg_flow_lpm"],
                row["pressure_delta_psi"],
                min_flow_lpm=_circuit_min_flow(conn, row["circuit"]),
                true_avg_flow_lpm=(row["true_avg_flow_lpm"] if has_af else None),
                peak_flow_lpm=row["peak_flow_lpm"],
            )
            if below and (not row["dr"] or float(row["veff"] or 0.0) > 0.0):
                with transaction(conn):
                    conn.execute(
                        "UPDATE events SET "
                        "  is_low_flow_dribble = 1, "
                        "  volume_litres_effective = 0, "
                        "  volume_estimation_method = ?, "
                        "  excluded_from_training = 1, "
                        "  match_rejection_reason = ? "
                        "WHERE id = ?",
                        (BELOW_METER_FLOOR_REASON, BELOW_METER_FLOOR_REASON,
                         row["id"]),
                    )
                    # §2.5 — zero the ledger contribution via the one chokepoint.
                    apply_effective_volume(conn, row["id"], row["circuit"],
                                           row["start_ts"], 0)
                dribbles_flagged += 1
                litres_zeroed += float(row["veff"] or 0.0)
            elif not below and row["dr"]:
                # Old-rule dribble the floor rule does not match — restore.
                raw_vol = float(row["volume_litres"] or 0.0)
                restored_excluded = 1 if (
                    row["ui"] or row["integration_quality"] == "degraded") else 0
                # dev51 (Phase 5): an exclusion that survives the restore keeps
                # a reason. This ran on EVERY boot and wrote NULL regardless —
                # the busiest producer of "excluded, no reason recorded".
                restored_reason = (
                    USER_IGNORED_REASON if row["ui"]
                    else INTEGRATION_DEGRADED_REASON if restored_excluded
                    else None)
                with transaction(conn):
                    conn.execute(
                        "UPDATE events SET "
                        "  is_low_flow_dribble = 0, "
                        "  volume_litres_effective = ?, "
                        "  volume_estimation_method = 'raw', "
                        "  excluded_from_training = ?, "
                        "  match_rejection_reason = ? "
                        "WHERE id = ?",
                        (round(raw_vol, 3), restored_excluded, restored_reason,
                         row["id"]),
                    )
                    apply_effective_volume(conn, row["id"], row["circuit"],
                                           row["start_ts"], raw_vol)
                dribbles_restored += 1
            else:
                continue
            day = local_day_of(row["start_ts"])
            if day:
                dr_days.add((row["circuit"], day))
        for circ, day in dr_days:
            compute_daily_summary(conn, circ, day)
        if dribbles_flagged or dribbles_restored:
            conn.commit()
            log.info("below-meter-floor reprocess: %d event(s) zeroed "
                     "(%.2f L removed), %d old-rule dribble(s) restored, "
                     "across %d day(s)", dribbles_flagged, litres_zeroed,
                     dribbles_restored, len(dr_days))

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
            # uc_guard/uft_guard, NOT a re-typed pair: uft_guard is where
            # _LEAK_REFILL_GUARD_SQL rides. Typed out by hand this scan
            # silently dropped that guard, and a leak-test reopen refill
            # sets none of the artifact FLAG bits (it stays visible), so
            # nothing else here held it off: a refill whose shape tripped
            # cross-talk was re-claimed and its provenance overwritten.
            + scan_guards
            + _NO_PHANTOM_SQL + _NO_DEGRADED_SQL +
            "  AND duration_seconds >= ? "
            "  AND flow_integral_litres < ? AND flow_on_ratio < ? "
            "  AND pressure_delta_psi >= ?",
            (_XTALK_MIN_DURATION_S, _PHANTOM_MAX_FLOW_INTEGRAL_L,
             _PHANTOM_MAX_FLOW_ON_RATIO, _PHANTOM_MAX_DELTA_PSI),
        ).fetchall()
        cross_talk_flagged, _litres, xt_days = _sweep_zeroing_verdict(
            conn, xrows,
            detect=lambda row: _detect_cross_talk(
                row["duration_seconds"], row["pressure_delta_psi"],
                row["flow_integral_litres"], row["flow_on_ratio"]),
            set_sql=("  is_cross_talk = 1, is_low_flow_dribble = 0, "  # xtalk wins
                     "  volume_litres_effective = 0, "
                     "  volume_estimation_method = 'cross_talk', "
                     "  excluded_from_training = 1, "
                     "  match_rejection_reason = 'cross_talk' "),
        )
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

    # ── Scan 3c: pressure-silent flow (2026-07-05) ────────────────────────────
    # Validly-metered flow with NO supply response — physically impossible as a
    # real draw (see the pressure-silent constants block). Same zeroing family
    # as the phantom (shares the flag, distinct reason). corr is REQUIRED so
    # flow-only imports (no pressure evidence) can never be zeroed. Idempotent:
    # a flagged row fails the phantom=0 filter on the next run.
    psilent_flagged = 0
    ps_litres = 0.0
    ps_days: set = set()
    if has_af and _events_has_column(conn, "flow_pressure_corr"):
        psrows = conn.execute(
            "SELECT id, circuit, start_ts, duration_seconds, volume_litres, "
            "       pressure_delta_psi, flow_pressure_corr, "
            "       COALESCE(has_pressure_transient, 0) AS hpt, "
            "       true_avg_flow_lpm, peak_flow_lpm, avg_flow_lpm, "
            "       COALESCE(volume_litres_effective, volume_litres, 0) AS veff "
            "FROM events "
            "WHERE 1=1 "
            + _NO_PHANTOM_SQL + _NO_CROSS_TALK_SQL
            + _NO_DRIBBLE_SQL + _NO_DEGRADED_SQL +
            "  AND flow_pressure_corr IS NOT NULL AND flow_pressure_corr < ? "
            "  AND pressure_delta_psi IS NOT NULL AND pressure_delta_psi < ? "
            "  AND COALESCE(has_pressure_transient, 0) = 0 "
            "  AND duration_seconds <= ? "
            "  AND volume_litres > 0 AND volume_litres <= ?"
            + scan_guards,
            (_PSILENT_MAX_CORR, _PSILENT_MAX_DELTA_PSI,
             _PSILENT_MAX_DURATION_S, _PSILENT_MAX_VOLUME_L),
        ).fetchall()
        # The driver's pump_gate carries the dev25/§2.29 skip, and its detect
        # re-runs the canonical predicate (SQL is only a prefilter) — that adds
        # the registration-floor requirement SQL can't express per-circuit.
        psilent_flagged, ps_litres, ps_days = _sweep_zeroing_verdict(
            conn, psrows, pump_gate="pressure-silent", veff_key="veff",
            detect=lambda row: _detect_pressure_silent_flow(
                row["duration_seconds"], row["volume_litres"],
                row["pressure_delta_psi"], row["flow_pressure_corr"],
                row["hpt"],
                true_avg_flow_lpm=row["true_avg_flow_lpm"],
                peak_flow_lpm=row["peak_flow_lpm"],
                avg_flow_lpm=row["avg_flow_lpm"],
                min_flow_lpm=_circuit_min_flow(conn, row["circuit"]),
            ),
            set_sql=("  is_pressure_restoration_phantom = 1, "
                     "  is_cross_talk = 0, is_low_flow_dribble = 0, "
                     "  volume_litres_effective = 0, "
                     "  volume_estimation_method = ?, "
                     "  excluded_from_training = 1, "
                     "  match_rejection_reason = ? "),
            set_params=lambda row: (PRESSURE_SILENT_REASON,
                                    PRESSURE_SILENT_REASON),
        )
        if psilent_flagged:
            conn.commit()
            log.info("pressure-silent reprocess: flagged %d event(s) "
                     "(%.2f L removed) across %d day(s)",
                     psilent_flagged, ps_litres, len(ps_days))

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
            + _NO_PHANTOM_SQL +
            "  AND match_rejection_reason IS NULL"
            + scan_guards,
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
            + _NO_PHANTOM_SQL + _NO_CROSS_TALK_SQL + _NO_DRIBBLE_SQL
            + _NO_DEGRADED_SQL + _NOT_USER_IGNORED_SQL
            + _NOT_USER_CLASSIFIED_SQL +
            "  AND (match_rejection_reason IS NULL "
            "       OR match_rejection_reason <> ?) "
            "  AND true_avg_flow_lpm IS NOT NULL"
            # A refill clears every other term here (no flag bit, not
            # user-classified, reason != sparse_envelope), so without this
            # a zero-volume event the add-on itself caused was handed back
            # to training.
            + _LEAK_REFILL_GUARD_SQL,
            (SPARSE_ENVELOPE_REASON,),
        )
        capped_reincluded = cur.rowcount or 0
        if capped_reincluded:
            conn.commit()
            log.info("capped-reprocess: re-included %d capped-only event(s)",
                     capped_reincluded)

    # ── Scan 6 (dev33 §1.1): restore user-labelled real water that an artifact
    # verdict zeroed while `user_classified=1` held every other sweep off it.
    #
    # These rows are unreachable by Scans 1-5 (all carry uc_guard/uft_guard) and
    # by repair_artifact_flag_consistency (which HONOURS mrr on user_classified
    # rows, cementing the bad state). They exist because the History modal
    # pre-checked its classification boxes from the row's AUTO flags and posted
    # them back as manual verdicts on save — fixed for new saves in the same
    # build (history.html clsTouched), so this pass is one-shot in practice: a
    # restored row no longer matches, and no new rows of this shape are created.
    #
    # Deliberately CONSERVATIVE — a real fixture label alone is not enough,
    # because a user may legitimately label a genuine artifact (e.g. naming a
    # 0.2 L phantom 'toilet'). Restoration additionally requires real-water
    # evidence: >= _RELABEL_REPAIR_MIN_VOLUME_L AND active flow at or above the
    # circuit's meter registration floor. On the 2026-08-02 production census
    # (24 candidate rows) this restores exactly the two unambiguous ones — a
    # 685.3 L / 8.7 LPM draw and a 3.9 L / 6.3 LPM toilet — and leaves 18
    # sub-0.2 L micro-phantoms zeroed. Rows with real-water shape but NO fixture
    # type are reported for manual review, never auto-restored.
    from .database import _RELABEL_REVERTIBLE_REASONS
    relabel_restored = 0
    relabel_review: list = []
    if _events_has_column(conn, "user_classified") and has_af:
        rl_rows = conn.execute(
            "SELECT id, circuit, start_ts, volume_litres, volume_litres_effective, "
            "       true_avg_flow_lpm, peak_flow_lpm, user_fixture_type, "
            "       match_rejection_reason, is_composite "
            "FROM events "
            "WHERE COALESCE(user_classified, 0) = 1 "
            + _NOT_USER_IGNORED_SQL +
            "  AND volume_litres > ? "
            "  AND COALESCE(volume_litres_effective, 0) < volume_litres "
            "  AND match_rejection_reason IS NOT NULL",
            (_RELABEL_REPAIR_MIN_VOLUME_L,),
        ).fetchall()
        rl_days: set = set()
        for row in rl_rows:
            if row["match_rejection_reason"] not in _RELABEL_REVERTIBLE_REASONS:
                continue
            floor = _meter_registration_floor(
                _circuit_min_flow(conn, row["circuit"]))
            flow = max(float(row["true_avg_flow_lpm"] or 0.0),
                       float(row["peak_flow_lpm"] or 0.0))
            if flow < floor:
                continue          # sub-meter-floor: the zeroing stands
            _label = row["user_fixture_type"]
            if not (_label and str(_label).strip()):
                relabel_review.append({
                    "id": row["id"], "circuit": row["circuit"],
                    "volume_litres": round(float(row["volume_litres"] or 0.0), 2),
                    "reason": row["match_rejection_reason"],
                })
                continue
            raw = float(row["volume_litres"] or 0.0)
            with transaction(conn):
                conn.execute(
                    "UPDATE events SET "
                    "  is_pressure_restoration_phantom = 0, is_low_flow_dribble = 0, "
                    "  is_cross_talk = 0, phantom_suppression_averted = 0, "
                    "  volume_litres_effective = ?, volume_estimation_method = 'raw', "
                    "  match_rejection_reason = NULL, "
                    "  excluded_from_training = "
                    "    CASE WHEN is_composite = 1 THEN 1 ELSE 0 END, "
                    "  volume_recomputed_at = ? "
                    "WHERE id = ?",
                    (round(raw, 3), datetime.now(timezone.utc).isoformat(), row["id"]),
                )
                apply_effective_volume(conn, row["id"], row["circuit"],
                                       row["start_ts"], raw)
            relabel_restored += 1
            day = local_day_of(row["start_ts"])
            if day:
                rl_days.add((row["circuit"], day))
            log.info("relabel-repair: restored %.3f L on %s (was %s, label=%r)",
                     raw, row["id"], row["match_rejection_reason"],
                     row["user_fixture_type"])
        for circ, day in rl_days:
            compute_daily_summary(conn, circ, day)
        if rl_days:
            conn.commit()
        if relabel_review:
            log.info("relabel-repair: %d user-classified row(s) look like real "
                     "water but carry no fixture label — relabel them in History "
                     "to restore: %s", len(relabel_review),
                     ", ".join(f"{r['id'][:8]} ({r['volume_litres']} L, "
                               f"{r['reason']})" for r in relabel_review))

    # ── Scan 7: leak-test reopen refills ──────────────────────────────────────
    # Runs LAST so it sees the verdicts the scans above just settled and can
    # take the automatic ones over: inside a leak test's reopen window the test
    # is ground truth about causation, while those detectors are inferring from
    # shape. The scans in turn skip rows already tagged here (uft_guard carries
    # _LEAK_REFILL_GUARD_SQL), so the precedence holds in both directions.
    # Self-healing: a reprocess that dropped a refill's verdict gets it back.
    try:
        from .leak_test_refill import reconcile_leak_test_refills
        refill = reconcile_leak_test_refills(conn)
    except Exception as e:
        log.warning("leak-test refill reconcile failed (non-fatal): %s", e)
        refill = {"tagged": 0}

    return {"flagged": flagged, "dribbles_flagged": dribbles_flagged,
            "dribbles_restored": dribbles_restored,
            "psilent_flagged": psilent_flagged,
            "cross_talk_flagged": cross_talk_flagged,
            "leak_test_refills": refill.get("tagged", 0),
            "rise_flagged": rise["rise_flagged"],
            "sparse_flagged": sparse_flagged,
            "capped_reincluded": capped_reincluded,
            "relabel_restored": relabel_restored,
            "relabel_review": relabel_review,
            "excluded_fixed": repair["excluded_fixed"],
            "flag_pairs_resolved": repair["pairs_resolved"],
            "flag_pairs_unresolved": repair["unresolved"],
            # dev57 (§2.36) — section C's count reaches the caller now.
            "flag_rows_rezeroed": repair.get("rezeroed", 0)}


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
        + _NO_PHANTOM_SQL + _NO_CROSS_TALK_SQL + _NO_DRIBBLE_SQL
        + _NO_DEGRADED_SQL + _NOT_USER_CLASSIFIED_SQL
        + _NO_USER_FIXTURE_TYPE_SQL + _LEAK_REFILL_GUARD_SQL,
        (_RISE_PHANTOM_MIN_CORR, _RISE_PHANTOM_MAX_DURATION_S,
         _RISE_PHANTOM_MAX_VOLUME_PD_L),   # loose prefilter = the larger PD cap;
                                           # the detector applies the per-circuit one
    ).fetchall()

    # pump_gate carries the dev25 skip (the live path routes a pump-era row
    # through the pump_recharge absorber instead — a real draw on a recharge
    # upswing earns positive corr and would be wrongly zeroed here) plus the
    # §2.29 fail-closed rule. detect re-runs the canonical predicate — single
    # source of truth for the thresholds, and it re-rejects bad data; min_flow
    # selects the meter-class volume cap (PD 2.5 L / turbine 1.0 L).
    rise_flagged, _litres, days = _sweep_zeroing_verdict(
        conn, rows, pump_gate="rise-phantom",
        detect=lambda row: _detect_rising_pressure_phantom(
            row["duration_seconds"], row["volume_litres"],
            row["flow_pressure_corr"],
            min_flow_lpm=_circuit_min_flow(conn, row["circuit"])),
        set_sql=("  is_pressure_restoration_phantom = 1, "
                 "  is_cross_talk = 0, is_low_flow_dribble = 0, "
                 "  volume_litres_effective = 0, "
                 "  volume_estimation_method = 'pressure_restoration_phantom', "
                 "  excluded_from_training = 1, "
                 "  match_rejection_reason = ? "),
        set_params=lambda row: (RISE_PHANTOM_REASON,),
        on_flag=lambda row: log.info(
            "rise-phantom-reprocess: event %s flagged "
            "(corr=%+.2f dur=%.0fs vol=%.3f L)",
            row["id"], row["flow_pressure_corr"] or 0.0,
            row["duration_seconds"] or 0.0, row["volume_litres"] or 0.0),
    )
    if rise_flagged:
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
    `volume_litres_effective` to the CAPPED envelope estimate (dev33 §8.5 —
    this path used to write the raw uncapped one); events flipping back to
    clean revert to raw `volume_litres`. The hourly_volume bucket is adjusted
    by the delta, and every affected day's `daily_summary` is rebuilt — this
    sweep was the only one of five that skipped the rebuild, so flips silently
    left daily totals and every chart reading them stale.

    dev33 also gates the VFD-ripple exemption here (`pump_mode`), resolved
    per row from the PINNED `pump_era_start` — never from live pump-gate
    state, which would re-flag every exempted event the moment gates flipped
    off. Rows flipping degraded→clean are additionally re-run through
    `_detect_pump_recharge`: they were suppressed by the finalizer's
    `not is_degraded` guard and would otherwise become raw "Other" events
    forever.

    Returns a summary dict with the counts the endpoint relays to the UI.
    """
    from .database import (transaction, apply_effective_volume,
                           compute_daily_summary, local_day_of)
    from .supply_regime import pump_era_start
    era_start = pump_era_start(conn)

    rows = conn.execute(
        "SELECT id, circuit, start_ts, degraded_supply, "
        "       degraded_diagnostic_json, volume_litres, flow_integral_litres, "
        "       volume_litres_estimated, hourly_volume_applied_litres, "
        "       hourly_volume_applied_bucket, is_composite, "
        "       duration_seconds, flow_pressure_corr, pressure_delta_psi, "
        "       pressure_transient_duration_ms, start_trigger "
        "FROM events "
        "WHERE degraded_diagnostic_json IS NOT NULL "
        "  AND degraded_diagnostic_json != '' "
        # Phantom takes precedence over degraded: never let a degraded
        # re-verdict un-zero a pressure-restoration phantom's volume.
        + _NO_PHANTOM_SQL
        # And never re-verdict a row whose provenance this scan would erase.
        # The UPDATE below writes match_rejection_reason unconditionally - None
        # on the not-degraded branch - and recomputes excluded_from_training
        # from (composite OR degraded) alone. For a leak-test refill that means
        # the verdict string is wiped AND the row is re-admitted to training at
        # zero volume.
        #
        # A refill reaches this scan because degraded_diagnostic_json is
        # written for EVERY extracted event, not just degraded ones, and a
        # refill deliberately sets none of the three artifact bits, so
        # _NO_PHANTOM_SQL does not exclude it. That is the same premise unit
        # 6.5 established for the cross-talk scan.
        #
        # Guarded at the SELECT, the way the other scans do it, rather than by
        # special-casing the UPDATE: this scan should not form an opinion about
        # a row it must not rewrite.
        + _LEAK_REFILL_GUARD_SQL
        + _NOT_USER_CLASSIFIED_SQL
        + _NO_USER_FIXTURE_TYPE_SQL
    ).fetchall()

    skipped_legacy_row = conn.execute(
        "SELECT COUNT(*) AS c FROM events "
        "WHERE degraded_diagnostic_json IS NULL "
        "   OR degraded_diagnostic_json = ''"
    ).fetchone()
    skipped_legacy = int(skipped_legacy_row["c"]) if skipped_legacy_row else 0

    flipped_to_degraded = 0
    flipped_to_clean = 0
    ripple_exempted = 0
    recharge_rederived = 0
    evaluated = 0
    affected_days: set = set()

    for row in rows:
        evaluated += 1
        try:
            diag = json.loads(row["degraded_diagnostic_json"])
        except (ValueError, TypeError):
            log.warning("reprocess: event %s has unparseable diag JSON; skipping",
                        row["id"])
            continue

        # Era-only pump gate (see the docstring): historical question,
        # historical answer.
        row_pump = bool(era_start and (row["start_ts"] or "") >= era_start)
        new_is_degraded, new_reason = _evaluate_degraded_from_diag(diag, row_pump)
        old_is_degraded = bool(row["degraded_supply"])
        if new_is_degraded == old_is_degraded:
            continue  # verdict unchanged

        # Verdict flipped — update event + hourly_volume in one transaction.
        diag["reason"] = new_reason
        raw_volume = float(row["volume_litres"] or 0.0)
        envelope_volume = float(row["volume_litres_estimated"] or 0.0)
        if new_is_degraded:
            # dev33 §8.5: route through the SAME cap as the live finalizer.
            # This path wrote the raw uncapped `volume_litres_estimated`
            # straight to effective, so an admin-triggered re-check could
            # restore pre-cap inflation (the 336 L-for-2 L class of error).
            new_effective, cap_diag = _cap_envelope_estimate(
                envelope_volume,
                {"flow_integral_litres": row["flow_integral_litres"],
                 "volume_litres": row["volume_litres"]})
            if cap_diag:
                diag.update(cap_diag)
        else:
            new_effective = raw_volume
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

        day = local_day_of(row["start_ts"])
        if day:
            affected_days.add((row["circuit"], day))

        if new_is_degraded:
            flipped_to_degraded += 1
        else:
            flipped_to_clean += 1
            if new_reason == VFD_RIPPLE_EXEMPT_REASON:
                ripple_exempted += 1
            # dev33: this row was held back from the recharge detector by the
            # finalizer's `not is_degraded` guard. Give it one fair pass at the
            # canonical arms now that it is clean, otherwise a genuine pump
            # top-up becomes a raw "Other" event forever (and never anchors the
            # recharge population).
            if _detect_pump_recharge(row["duration_seconds"],
                                     raw_volume,
                                     row["flow_pressure_corr"],
                                     row["pressure_delta_psi"],
                                     pressure_transient_duration_ms=row[
                                         "pressure_transient_duration_ms"],
                                     start_trigger=row["start_trigger"]):
                with transaction(conn):
                    conn.execute(
                        "UPDATE events SET is_pressure_restoration_phantom = 1, "
                        "  volume_litres_effective = 0.0, "
                        "  volume_estimation_method = ?, "
                        "  match_rejection_reason = ?, "
                        "  excluded_from_training = 1 "
                        "WHERE id = ?",
                        (PUMP_RECHARGE_REASON, PUMP_RECHARGE_REASON, row["id"]),
                    )
                    apply_effective_volume(conn, row["id"], row["circuit"],
                                           row["start_ts"], 0.0)
                recharge_rederived += 1
                new_effective = 0.0
        log.info(
            "reprocess: event %s flipped %s → %s (reason=%s, effective_vol %.3f → %.3f)",
            row["id"],
            "clean" if old_is_degraded is False else "degraded",
            "degraded" if new_is_degraded else "clean",
            new_reason,
            prev_applied,
            new_effective,
        )

    # Rebuild every affected day ONCE (batched): this sweep moves real volume
    # — both the un-degraded rows reverting to raw and the dev33 cap change —
    # and was the only reprocess of five that never refreshed daily_summary.
    for circ, day in affected_days:
        compute_daily_summary(conn, circ, day)
    if affected_days:
        conn.commit()
    if ripple_exempted:
        log.info("reprocess: %d event(s) exempted as VFD pump ripple "
                 "(period < %.1f s inside the pump era)",
                 ripple_exempted, _VFD_RIPPLE_MAX_PERIOD_S)

    return {
        "evaluated": evaluated,
        "flipped_to_degraded": flipped_to_degraded,
        "flipped_to_clean": flipped_to_clean,
        "ripple_exempted": ripple_exempted,
        "recharge_rederived": recharge_rederived,
        "days_rebuilt": len(affected_days),
        "skipped_legacy": skipped_legacy,
    }


def backfill_sawtooth_pump_recharge(conn) -> dict:
    """One-shot re-verdict of stored pump-era events under the sawtooth prong
    of ``_detect_pump_recharge`` (migration 20260572; idempotent — flagged
    rows are excluded from the candidate query, so a re-run finds nothing).

    Scope mirrors the live finalizer's gates exactly: pump era only, never
    degraded rows, never user-touched rows, never rows some other artifact
    verdict already owns (candidates are mrr NULL / 'no_tier_matched' with
    volume still applied). Volume moves through ``apply_effective_volume``
    and every affected day's summary is rebuilt, same as the dev33 sweep.
    """
    from .supply_regime import pump_era_start
    era_start = pump_era_start(conn)
    if not era_start:
        return {"tagged": 0, "days_rebuilt": 0}

    rows = conn.execute(
        "SELECT id, circuit, start_ts, duration_seconds, volume_litres, "
        "       flow_pressure_corr, pressure_delta_psi, "
        "       pressure_transient_duration_ms, start_trigger "
        "FROM events "
        "WHERE start_ts >= ? "
        "  AND start_trigger LIKE 'pressure%' "
        + _NO_DEGRADED_SQL + _NO_PHANTOM_SQL + _NOT_USER_CLASSIFIED_SQL +
        # Deliberately NOT _NO_USER_FIXTURE_TYPE_SQL. This sweep tests IS NULL
        # only, which is STRICTER than the shared guard's "IS NULL OR = ''" —
        # widening it would newly zero empty-string-labelled rows, so it stays
        # as typed (unit 6.5).
        "  AND user_fixture_type IS NULL "
        "  AND (match_rejection_reason IS NULL "
        "       OR match_rejection_reason = 'no_tier_matched') "
        "  AND COALESCE(volume_litres_effective, 0) > 0",
        (era_start,),
    ).fetchall()

    tagged, _litres, affected_days = _sweep_zeroing_verdict(
        conn, rows,
        detect=lambda row: _detect_pump_recharge(
            row["duration_seconds"], row["volume_litres"],
            row["flow_pressure_corr"], row["pressure_delta_psi"],
            pressure_transient_duration_ms=row["pressure_transient_duration_ms"],
            start_trigger=row["start_trigger"]),
        set_sql=("is_pressure_restoration_phantom = 1, "
                 "  volume_litres_effective = 0.0, "
                 "  volume_estimation_method = ?, "
                 "  match_rejection_reason = ?, "
                 "  excluded_from_training = 1 "),
        set_params=lambda row: (PUMP_RECHARGE_REASON, PUMP_RECHARGE_REASON),
    )
    log.info("sawtooth recharge backfill: tagged %d of %d candidate(s), "
             "%d day summar(ies) rebuilt", tagged, len(rows),
             len(affected_days))
    return {"tagged": tagged, "candidates": len(rows),
            "days_rebuilt": len(affected_days)}


def _estimate_volume_smoothed(
    flow_readings: List[float],
    duration_s: float,
    flow_integral_litres: Optional[float] = None,
) -> float:
    """Spike-resistant smoothed volume estimate for degraded events.

    Strategy: cap a rolling-max envelope at the 95th percentile of positive
    samples (rejects single-sample spikes), then take the MEDIAN across
    windows (rejects sub-event high outliers). The result is a "typical
    sustained flow" estimate that's robust to both the artefact zero-troughs
    AND any phantom-pulse spikes that paddlewheel rectification might
    introduce.

    dev33 (§8.5): this became load-bearing when the envelope cap dropped to
    1.0x, because the `base is None` branch of ``_cap_envelope_estimate`` is
    now the ONLY path whose result may exceed the metered volume. Two honesty
    fixes:

      * few-sample / very short events used ``mean x duration``, which for a
        burst-shaped draw over-counts the idle remainder — prefer the measured
        flow integral when one exists;
      * the windowed path takes the median of window MAXES, which for an event
        with only a handful of windows approximates ``peak x duration``. Use
        the window MEAN so a short event's estimate reflects sustained flow
        rather than its peak. (Trough rejection is preserved: the percentile
        cap still clips spikes, and the median across windows still rejects
        sub-event outliers.)
    """
    if not flow_readings or duration_s <= 0:
        return 0.0
    cleaned = [f for f in _finite_float_series(flow_readings) if f >= 0]
    if not cleaned:
        return 0.0
    try:
        integral = (float(flow_integral_litres)
                    if flow_integral_litres is not None else None)
    except (TypeError, ValueError):
        integral = None
    if integral is not None and (not math.isfinite(integral) or integral <= 0):
        integral = None

    if len(cleaned) < 5 or duration_s < 5:
        if integral is not None:
            return integral      # measured beats mean x duration
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
            windowed.append(min(sum(chunk) / len(chunk), cap))
    if not windowed:
        return 0.0
    windowed.sort()
    effective_flow = windowed[len(windowed) // 2]   # median of window means
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


def _wf_full_res_usable(record: "Optional[WaveformRecord]") -> bool:
    """True when the record's full-window arrays are display-trustworthy —
    same complete/not-reduced/quality gate as the signature override."""
    if record is None or not record.full_flow:
        return False
    fl = record.metadata.flags
    return (bool(fl & _WF_FL_FULL_COMPLETE)
            and not (fl & _WF_FL_RESOLUTION_REDUCED)
            and record.metadata.quality == 0)


def _persist_waveform(
    db,
    event_id: str,
    flow_readings: list,
    pressure_readings: list,
    duration_s: float,
    esp_record: "Optional[WaveformRecord]" = None,
) -> None:
    """Write a min/max-binned waveform to event_waveforms.

    Used for the high-resolution waveform display in the event detail modal.
    The 32-point pressure_signature_json / flow_signature_json on the events
    row stays for clustering — these min/max envelopes are display-only.
    Skips silently if both reading lists are empty (historical events).

    When a usable ESP capture matched the event, its full-window arrays
    (~50 Hz onboard, ~thousands of points) replace the add-on's HA-sampled
    readings — the detector series is ~5 s cadence and every-5th downsampled
    past 120 s, which erases short pulses (washer fill pauses) that the ESP
    capture resolves.
    """
    # dev38 — per-channel source metadata (audit §3.5): the two channels are
    # binned from streams of different cadences, so a renderer needs to know
    # each channel's source count and (when fixed-rate) sample frequency to
    # build an honest time axis. hz stays NULL for the event-driven software
    # series, whose spacing has no recoverable axis.
    # ~50 Hz, NOT 200 Hz. The firmware's waveform_capture interval is 20 ms
    # (`- interval: 20ms`); 200 Hz is the pressure ADC read loop, which the
    # firmware header explicitly distinguishes from capture. This value is
    # stored as event_waveforms.flow_src_hz / press_src_hz and is what a
    # renderer divides by to build the time axis, so 200 rendered every ESP
    # waveform 4x time-compressed. The 20 ms figure is confirmed by the
    # function-local _SAMPLE_MS in event_waveform.py (moved there by 7.3);
    # it is not an importable event_detector attribute.
    _ESP_HZ = 50.0
    flow_hz = press_hz = None
    if _wf_full_res_usable(esp_record):
        flow_readings = esp_record.full_flow
        flow_hz = _ESP_HZ
        if esp_record.full_pressure:
            pressure_readings = esp_record.full_pressure
            press_hz = _ESP_HZ
    flow_src_n = len(flow_readings or [])
    press_src_n = len(pressure_readings or [])
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
            " duration_seconds, created_at, "
            " flow_src_n, press_src_n, flow_src_hz, press_src_hz) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                json.dumps(flow_min, allow_nan=False),
                json.dumps(flow_max, allow_nan=False),
                json.dumps(pres_min, allow_nan=False),
                json.dumps(pres_max, allow_nan=False),
                float(duration_s),
                now_iso,
                flow_src_n or None,
                press_src_n or None,
                flow_hz,
                press_hz,
            ),
        )
    except Exception as e:
        log.warning("event_waveforms insert failed for %s: %s", event_id, e)


def _flow_signature(flow_readings: list, peak: float, n: int = SIGNATURE_POINTS) -> list:
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


# ── Edge signatures (dev19) ──────────────────────────────────────────────────
# Fixed-TIME onset/offset shape vectors for the k-NN matcher. Unlike the
# PROPORTIONAL 256-pt signature (point i = i/256 through the event — a 45-min
# shower still gets ~10 s/pt), each edge cell is exactly EDGE_SIG_CELL_SECONDS
# of absolute time anchored at the event's start (onset) or end (offset), so
# valve ramps / closing steps / toilet fill-tapers align across durations.
# Validated LOO over 344 labelled events (see tools/validate_edge_signatures):
# 32 cells × 1 s (a 32 s window each end — wide enough for toilet fill-tapers,
# which is where the win came from: toilet recall 0.783→0.870, shower
# 0.878→0.927, tap 0.429→0.486) beat 16-cell and 0.5 s-cell variants; finer
# cells added nothing even on ESP-captured events. Zero-padding past a short
# event's extent mirrors the fingerprint grid's convention.
EDGE_SIG_CELLS: int = 32
EDGE_SIG_CELL_SECONDS: float = 1.0


def resample_absolute(values, duration_s: float, cell_s: float, n_cells: int,
                      from_end: bool = False) -> Optional[list]:
    """Mean-pool a uniform-grid series onto an ABSOLUTE-time cell grid anchored
    at the series start (or end). Cells past the series' extent are zero-filled.
    Returns None on unusable input. Pure — shared by the live extractor, the
    waveform backfill, and the offline validation harness."""
    if not values or duration_s <= 0:
        return None
    n = len(values)
    dt = duration_s / n
    out = []
    for k in range(n_cells):
        if from_end:
            t1 = duration_s - k * cell_s
            t0 = t1 - cell_s
        else:
            t0 = k * cell_s
            t1 = t0 + cell_s
        i0 = max(0, int(math.floor(t0 / dt)))
        i1 = min(n, int(math.ceil(t1 / dt)))
        if i1 <= i0 or t0 >= duration_s or t1 <= 0:
            out.append(0.0)
        else:
            seg = values[i0:i1]
            out.append(sum(seg) / len(seg))
    if from_end:
        out.reverse()               # chronological order within the tail window
    return out


def _edge_signature_pair(flow_values, duration_s: float,
                         ) -> "Optional[Tuple[list, list]]":
    """(onset, offset) edge signatures from a uniform-grid flow series —
    peak-normalized to [0, 1] like _flow_signature (shape, not magnitude),
    rounded for storage. None when the series can't support them."""
    vals = []
    for v in flow_values or []:
        try:
            f = float(v)
        except (TypeError, ValueError):
            f = 0.0
        vals.append(f if math.isfinite(f) else 0.0)
    peak = max(vals, default=0.0)
    if peak <= 0 or duration_s <= 0:
        return None
    norm = [max(0.0, min(1.0, v / peak)) for v in vals]
    on = resample_absolute(norm, duration_s, EDGE_SIG_CELL_SECONDS,
                           EDGE_SIG_CELLS)
    off = resample_absolute(norm, duration_s, EDGE_SIG_CELL_SECONDS,
                            EDGE_SIG_CELLS, from_end=True)
    if on is None or off is None:
        return None
    return ([round(v, 4) for v in on], [round(v, 4) for v in off])


def rebuild_edge_signatures_from_waveforms(conn) -> dict:
    """dev19 one-shot: backfill onset/offset edge signatures from each event's
    ``event_waveforms`` envelope (migration 20260557; version stamp = the
    one-shot guard; the function is idempotent — rows with edges are skipped).

    Only fills NULLs. Deliberately NO envelope-coarseness gate: the validated
    LOO study computed edges from every stored envelope, including the coarse
    ones long events get (a 1000-bin envelope on a 45-min event is ~2.7 s/bin —
    mean-pooling those bins onto the 1 s grid smears, it doesn't fabricate, and
    the study's numbers INCLUDE that smearing). Gating them out was measured to
    cost accuracy (production-path eval 0.663 vs 0.677) because it disengaged
    the tier on the long events the feature targets. Events whose envelope
    can't produce a pair at all stay NULL and never engage the tier.
    Returns ``{"scanned", "edges_filled"}``.
    """
    rows = conn.execute(
        "SELECT e.id, w.flow_max_json, w.duration_seconds AS wf_dur "
        "FROM events e JOIN event_waveforms w ON w.event_id = e.id "
        "WHERE e.onset_signature_json IS NULL"
    ).fetchall()
    scanned = filled = 0
    for r in rows:
        scanned += 1
        try:
            bins = json.loads(r["flow_max_json"] or "[]")
            wf_dur = float(r["wf_dur"] or 0.0)
        except (TypeError, ValueError):
            continue
        if not bins or wf_dur <= 0:
            continue
        pair = _edge_signature_pair(bins, wf_dur)
        if pair is None:
            continue
        conn.execute(
            "UPDATE events SET onset_signature_json = ?, "
            "offset_signature_json = ? WHERE id = ?",
            (json.dumps(pair[0]), json.dumps(pair[1]), r["id"]))
        filled += 1
    conn.commit()
    if filled:
        log.info("edge-signature backfill: %d/%d events filled "
                 "(%d cells × %.1f s)", filled, scanned,
                 EDGE_SIG_CELLS, EDGE_SIG_CELL_SECONDS)
    return {"scanned": scanned, "edges_filled": filled}


def rebuild_signatures_from_waveforms(conn) -> dict:
    """dev18 one-shot: regenerate stored flow/pressure signatures at the current
    SIGNATURE_POINTS from each event's ``event_waveforms`` envelope.

    Called by migration 20260556 (the version stamp is the one-shot guard;
    the function itself is idempotent — an already-current signature is
    skipped). Only upgrades: the stored signature must be SHORTER than
    SIGNATURE_POINTS and the waveform envelope FINER than the stored signature,
    so a rebuild can never degrade fidelity. Flow rebuilds from ``flow_max_json``
    (peak-normalized, `_flow_signature`); pressure rebuilds from
    ``pressure_min_json`` (deepest drop per bin) with the event's stored
    baseline/delta (`_pressure_signature`) — both the exact production
    functions, so rebuilt rows are indistinguishable from natively-256 ones.
    ``signature_source`` is untouched: the envelope was written from the same
    source the provenance already names. Events with no waveform row (pre-
    retention-window) keep their shorter signatures — every consumer resamples
    on load, so mixed lengths remain fine.

    Returns ``{"scanned", "flow_upgraded", "pressure_upgraded"}``.
    """
    rows = conn.execute(
        "SELECT e.id, e.flow_signature_json, e.pressure_signature_json, "
        "       e.pre_event_pressure_psi, e.pressure_delta_psi, "
        "       w.flow_max_json, w.pressure_min_json "
        "FROM events e JOIN event_waveforms w ON w.event_id = e.id"
    ).fetchall()

    def _arr(text):
        try:
            v = json.loads(text or "[]")
            return v if isinstance(v, list) else []
        except (TypeError, ValueError):
            return []

    scanned = flow_up = press_up = 0
    for r in rows:
        scanned += 1
        updates: dict = {}
        cur = _arr(r["flow_signature_json"])
        bins = _arr(r["flow_max_json"])
        if len(cur) < SIGNATURE_POINTS and len(bins) > len(cur):
            try:
                peak = max(float(v) for v in bins)
            except (TypeError, ValueError):
                peak = 0.0
            if peak > 0:
                updates["flow_signature_json"] = json.dumps(
                    _flow_signature(bins, peak))
        pcur = _arr(r["pressure_signature_json"])
        pbins = _arr(r["pressure_min_json"])
        try:
            pre = float(r["pre_event_pressure_psi"] or 0.0)
            delta = float(r["pressure_delta_psi"] or 0.0)
        except (TypeError, ValueError):
            pre = delta = 0.0
        if (len(pcur) < SIGNATURE_POINTS and len(pbins) > len(pcur)
                and pre > 0 and delta > 0):
            updates["pressure_signature_json"] = json.dumps(
                _pressure_signature(pbins, pre, delta))
        if updates:
            conn.execute(
                "UPDATE events SET "
                + ", ".join(f"{k} = ?" for k in updates)
                + " WHERE id = ?",
                (*updates.values(), r["id"]),
            )
            flow_up += 1 if "flow_signature_json" in updates else 0
            press_up += 1 if "pressure_signature_json" in updates else 0
    conn.commit()
    if flow_up or press_up:
        log.info("signature rebuild: %d scanned, %d flow / %d pressure "
                 "signatures regenerated at %d pts",
                 scanned, flow_up, press_up, SIGNATURE_POINTS)
    return {"scanned": scanned, "flow_upgraded": flow_up,
            "pressure_upgraded": press_up}


def flow_plateau_lpm(series) -> Optional[float]:
    """The rate this fixture runs at once it is running, or None.

    Neither of the flow numbers already stored answers that question. The
    average is diluted by ramp-up and by any off-time — a 14 s washer top-off
    with flow_on_ratio 0.42 averages 7.4 L/min while actually running at 9.7 —
    and the peak is a single sample, so it takes the spikiest reading in the
    event. The plateau is the median of the samples that are actually flowing,
    which is a property of the valve and the supply pressure rather than of how
    long the draw happened to last.

    Measured on the reference home (3-fold day-grouped CV, 583 labelled events
    carrying a waveform): +1.3 accuracy points, +2.3 excluding 'other'. It is
    NOT uniformly better than what it joins — dishwasher is tighter on plain
    average, because a pulsed fill has no plateau to speak of, and shower is
    tighter still because people adjust the tap mid-flow. It earns its place by
    being a DIFFERENT measurement, not a better one, and the booster picks per
    class which to lean on.

    None when there is no usable waveform; the model treats that as missing
    rather than as zero, which is the whole reason it is NaN-native.
    """
    try:
        vals = [float(v) for v in series if v is not None]
    except (TypeError, ValueError):
        return None
    on = sorted(v for v in vals if v > 0)
    if len(on) < 4:
        return None

    def _median(seq):
        mid = len(seq) // 2
        return seq[mid] if len(seq) % 2 else (seq[mid - 1] + seq[mid]) / 2.0

    # The reference is the MEDIAN of the flowing samples, not their max. A
    # max-anchored floor is defeated by exactly one spike: half of a 22 L/min
    # transient is 11, which excludes a genuine 9 L/min plateau and hands back
    # the spike — reproducing the very weakness of peak_flow_lpm this feature
    # exists to avoid. Taking the upper half is stable under a single outlier
    # and agrees with the max-anchored definition on a clean plateau, which is
    # the shape the +2.3 point measurement was taken on.
    upper = [v for v in on if v >= _median(on)]
    return round(_median(upper), 4) if upper else None


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
    n: int = SIGNATURE_POINTS,
) -> list:
    """Normalized pressure drop, n points (0 = no drop, 1 = full drop at delta_psi)."""
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

# Physical-consistency floor for a record's metadata peak. A record whose peak
# flow is below the event's own volume-derived active average cannot describe
# this draw — the average of a series can never exceed its maximum — so the
# record belongs to a DIFFERENT event and every field it carries (pressure
# delta, propagation delay, signatures, display envelope) is equally wrong.
#
# Deliberately permissive at 0.95: the record's peak is firmware-measured while
# true_avg comes from the HA flow stream, so healthy records legitimately sit
# just above 1.0× on short draws (production p1 = 1.007–1.09 by duration
# bucket). Only provable mismatches are rejected; see the [wf-sanity-reject]
# log tag for the observed false-reject rate.
_WF_PEAK_SANITY_RATIO: float = 0.95

# Waveform flag bits (must match firmware wire format).
_WF_FL_START_COMPLETE:     int = 0x01  # pre-roll covers full start-window span
_WF_FL_FULL_COMPLETE:      int = 0x02  # full-window capture is complete
# _WF_FL_RESOLUTION_REDUCED is NOT redefined here. It is one bit in the
# firmware's wire format, and it was defined twice - once here and once in
# the detector - with nothing pinning the two equal. A wire-format
# vocabulary split across two modules is a vocabulary that will drift, and
# the drift would be silent: the two copies only disagree once the firmware
# changes the bit. Unit 7.3 left the detector side with a single definition
# in event_waveform; this takes it from there.
#
# Import direction is the reason it comes from event_waveform and not from
# the event_detector facade: event_waveform imports only
# event_detector_core, whereas this module already does
# `from .event_detector import RawEvent, WaveformRecord` at module level,
# so sourcing it from there would close a real cycle.
from .event_waveform import _WF_FL_RESOLUTION_REDUCED  # noqa: E402

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
) -> bool:
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

    Returns True when the record was applied, False when it was rejected
    wholesale by the physical-consistency gate (``_WF_PEAK_SANITY_RATIO``) —
    in which case ``features`` is left completely untouched and the caller
    must NOT hand this record to ``_persist_waveform`` either.
    """
    meta = record.metadata
    fl   = meta.flags
    any_wf_used = False

    # ── 0. Physical-consistency gate ───────────────────────────────────────
    # peak < the event's own active average is impossible for a single draw,
    # so this record describes a different event (duration-only matching lets
    # two same-length draws both clear the score gate). Reject the WHOLE
    # record: pressure delta, propagation delay, signatures and the display
    # envelope all come from that same wrong capture.
    true_avg = float(features.get("true_avg_flow_lpm") or 0.0)
    if true_avg > 0 and meta.peak_flow > 0 and (
            meta.peak_flow < _WF_PEAK_SANITY_RATIO * true_avg):
        log.info(
            "[wf-sanity-reject] fw event_id=%d boot=%d peak=%.2f < true_avg=%.2f "
            "L/min — record describes a different draw, falling back to software",
            meta.event_id, meta.boot_id, meta.peak_flow, true_avg,
        )
        return False

    # ── 1. Metadata-sourced features (always, no flag guard needed) ────────
    # The metadata is always valid when we reach this point; replace features
    # that are better measured at 200 Hz / by the firmware's accumulator.
    if meta.peak_flow > 0:
        features["peak_flow_lpm"] = round(meta.peak_flow, 3)
        any_wf_used = True
    if meta.pressure_delta >= 0:
        features["pressure_delta_psi"] = round(meta.pressure_delta, 2)
        any_wf_used = True
        # dev38: resistance is ΔP-derived, so it must follow the overwrite —
        # same pinned definition as extract_features / _finalize_derived_verdicts
        # (ΔP / avg_flow_lpm, gated avg >= 0.15 + has_pressure_transient + ΔP > 0).
        # Keeps the LATE upgrade path consistent too (it never calls the
        # finalizer); the live path's finalizer recomputes identically.
        _r_avg = features.get("avg_flow_lpm")
        _r_dp = features["pressure_delta_psi"]
        if (_r_avg is not None and _r_avg >= 0.15
                and features.get("has_pressure_transient") and _r_dp > 0):
            features["hydraulic_resistance"] = round(_r_dp / _r_avg, 3)
        else:
            features["hydraulic_resistance"] = None
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
            plateau = flow_plateau_lpm(body)
            if plateau is not None:
                features["flow_plateau_lpm"] = plateau
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
            # dev19 — edge signatures ride the same quality gate: the firmware
            # array is the finest onset/offset source available, and the
            # uniform-grid assumption holds (fixed onboard sample cadence).
            _dur = float(features.get("duration_seconds") or 0.0)
            _edges = _edge_signature_pair(record.full_flow, _dur)
            if _edges is not None:
                features["onset_signature_json"] = json.dumps(_edges[0])
                features["offset_signature_json"] = json.dumps(_edges[1])
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
    # boot_id completes the claim key: the firmware event counter restarts at
    # every reboot, so (boot_id, event_id) — not event_id alone — identifies a
    # capture. Persisted so _wf_already_claimed can enforce one-record-one-event
    # across restarts and across the live/late-upgrade paths.
    features["waveform_boot_id"]      = meta.boot_id
    # dev41 insert-path assertion (D4): every NEW ESP-sourced row must carry a
    # boot_id — the claim ledger's (boot_id, event_id) key degrades to the
    # 48-hour same-circuit probe without it (boot_id is NOT NVS-monotonic;
    # the probe is load-bearing, not a stopgap — see PIPELINE.md). Legacy
    # NULLs stay as honest unknowns; this only flags new writes. Non-fatal:
    # enrichment must not die on a firmware omission.
    if any_wf_used and meta.boot_id is None:
        log.warning("ESP waveform claimed with NULL boot_id (event_id=%s) — "
                    "claim dedup falls back to the 48h probe", meta.event_id)
    features["waveform_quality"]      = meta.quality
    features["waveform_overlap_score"] = round(overlap_score, 4)
    return True


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
    "esp_waveform_used", "waveform_event_id", "waveform_boot_id",
    "waveform_quality", "waveform_overlap_score",
    "peak_flow_lpm", "pressure_delta_psi", "propagation_delay_ms",
    # dev38 — ΔP-derived, recomputed inside _enrich_from_waveform right after
    # the ΔP overwrite so the late path can't leave a stale ratio behind.
    "hydraulic_resistance",
    "flow_rise_rate_lpm_s", "time_to_90pct_flow_seconds", "opening_step_lpm",
    "pressure_onset_ms", "steady_state_fraction", "flow_variability",
    # dev48 — a late waveform is a BETTER measurement of the same draw, and
    # the plateau is derived from exactly that series. Leaving it off this list
    # would compute the improved value and then drop it on the floor.
    "flow_plateau_lpm",
    "recovery_overshoot_psi",
    # dev14 — recomputed from the firmware flow+pressure arrays under the same
    # quality gate as the signatures; the periodic exclusion reprocess (rise
    # scan) reconciles any verdict drift, exactly like the other columns here.
    "flow_pressure_corr",
    # dev19 — edge signatures recomputed from the firmware flow array under the
    # same quality gate as flow_signature_json.
    "onset_signature_json", "offset_signature_json",
)


def _wf_already_claimed(conn, circuit: str, boot_id, fw_event_id) -> bool:
    """True when some stored event already claimed this firmware capture.

    One capture describes one draw, so it may enrich exactly one event. The
    events table IS the ledger — no side table, and it survives restarts.
    Claims never expire: ``boot_id`` is a per-boot ``random_uint32()`` from the
    firmware, so a stale claim cannot collide with a future capture.

    dev38 hardening: a NULL ``boot_id`` no longer short-circuits to "not
    claimed". The audit traced 619 shared-capture events to exactly this hole
    — 90.7% of ESP-enriched rows carry a NULL boot_id (never persisted before
    20260573), so the composite key was degenerate and could neither claim
    nor block. The fallback mirrors the repair sweep's boot-NULL duplicate
    probe: a same-circuit event that claimed this ``waveform_event_id``
    within the last 48 h blocks the claim (the per-boot counter cannot wrap
    to the same id that fast on real hardware, while cross-reboot collisions
    older than that are exactly what the boot_id exists to disambiguate).
    """
    if fw_event_id is None:
        return False
    try:
        if boot_id is not None:
            row = conn.execute(
                "SELECT 1 FROM events WHERE circuit = ? AND waveform_boot_id = ? "
                "AND waveform_event_id = ? AND esp_waveform_used = 1 LIMIT 1",
                (circuit, boot_id, fw_event_id),
            ).fetchone()
        else:
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(hours=48)).isoformat()
            row = conn.execute(
                "SELECT 1 FROM events WHERE circuit = ? "
                "AND waveform_event_id = ? AND esp_waveform_used = 1 "
                "AND start_ts >= ? LIMIT 1",
                (circuit, fw_event_id, cutoff),
            ).fetchone()
    except sqlite3.Error:
        # Pre-migration schema (no waveform_boot_id column) → no ledger yet.
        return False
    return row is not None


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

    # Exclusivity: this capture may already have enriched the live event that
    # finalised just before this assembly. The SELECT and the UPDATE below run
    # inside ONE write-lock acquisition, so no concurrent late job can slip
    # between them. (The live path is a different task, so a live upsert can
    # still interleave here — see the note in _process; the consequence is one
    # duplicate claim, i.e. exactly the pre-fix behaviour, which the sanity
    # gate and the repair sweep both catch. Cross-task locking is not worth it.)
    if _wf_already_claimed(conn, circuit, record.metadata.boot_id,
                           record.metadata.event_id):
        return None

    features = dict(best)
    if not _enrich_from_waveform(features, record, best_score):
        return None   # physically inconsistent record — leave the row alone
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
    if cur.rowcount > 0 and _wf_full_res_usable(record):
        # Bring the display envelope along with the signature upgrade — empty
        # reading lists are replaced by the record's full-res arrays inside.
        _persist_waveform(
            conn, best["id"], [], [],
            float(best["duration_seconds"] or 0), esp_record=record,
        )
    conn.commit()
    return best["id"] if cur.rowcount > 0 else None


def _get_home_tz_or_utc():
    """Home timezone for time-of-day features, UTC until detection has run.

    Deferred import mirrors the file's database-import style and avoids any
    import-order coupling with event_rules at module load.
    """
    from .event_rules import get_home_timezone
    return get_home_timezone() or timezone.utc


def extract_features(event: RawEvent, *, min_flow_lpm: float = 0.15,
                     pump_mode: bool = False) -> Dict[str, Any]:
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
    # dev19 — fixed-time onset/offset edge signatures for the k-NN matcher.
    # flow_readings are the 1 Hz uniform series (live + importer), so the
    # absolute grid maps directly; None when the series can't support them
    # (the matcher's edge tier then simply doesn't engage for this event).
    edge_pair    = _edge_signature_pair(event.flow_readings, duration)
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
    from .flow_integral import (integrate_litres, active_flow_features,
                                registration_estimate,
                                registration_curve_version)
    flow_integral_litres, _integral_capped = integrate_litres(event.flow_samples)
    active = active_flow_features(event.flow_samples, duration)
    # dev38 ANNOTATION ONLY: registration-corrected estimate when flow spent
    # material time in the meter's under-registration band (1–8 L/min). Never
    # feeds volume_litres/effective or any total.
    registration_est = registration_estimate(event.flow_samples)
    # dev41 (E1): stamp which curve version produced the estimate.
    registration_curve_ver = (registration_curve_version()
                              if registration_est is not None else None)

    # dev38 consistency clamp: true_avg comes from the TIMESTAMPED flow_samples
    # while peak comes from the (differently sampled) flow_readings list, so
    # true_avg > peak was reachable on short pulsed draws — 825 physically
    # impossible rows in the 2026-08 audit, none of them ESP-enriched. Raise
    # peak to true_avg (ceil to 3 dp, the dev37 repair convention); never
    # lower true_avg, which is the volume-consistent figure.
    _ta = active.get("true_avg_flow_lpm")
    if _ta is not None and _ta > peak_flow:
        peak_flow = math.ceil(_ta * 1000.0) / 1000.0
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
        event.flow_readings, duration, flow_integral_litres
    )
    is_degraded, deg_diag = _detect_degraded_supply(
        event.pressure_readings,
        event.flow_readings,
        pre_event_pressure,
        shape,
        duration,
        pump_mode=pump_mode,
    )
    # The phantom verdict + volume_litres_effective + volume_estimation_method
    # + excluded_from_training + match_rejection_reason are ALL derived by
    # _finalize_derived_verdicts() on the assembled dict below — the single
    # source of truth, re-run after ESP-waveform enrichment in _process().

    duration_log = math.log(duration + 1)

    # Normalize timestamps to UTC so the UUID5 id and stored start_ts are
    # stable regardless of what timezone the incoming RawEvent carries.
    # This is the single storage point — all paths that write events go
    # through extract_features(), so enforcing UTC here is sufficient.
    _start = event.start_ts
    if _start.tzinfo is None:
        _start = _start.replace(tzinfo=timezone.utc)
    start_utc = _start.astimezone(timezone.utc)

    # Time features — computed in the HOME timezone, not UTC (dev38 fix:
    # the 2026-08 audit found hour_of_day matched the UTC hour on 100% of
    # events and day_of_week was wrong on 30%; any event after 18:00 local
    # fell on the next UTC day). Falls back to UTC only when tz detection
    # hasn't run yet; the deferred backfill task re-stamps those rows once
    # the tz is known (events.time_features_tz marker).
    _home_tz = _get_home_tz_or_utc()
    _local = start_utc.astimezone(_home_tz)
    hour = _local.hour
    dow = _local.weekday()
    hour_radians = 2 * math.pi * hour / 24
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
        # dev46 (46i) — the real captured span of each signature channel.
        # Forward-only: None for importer-reconstructed events (whose true
        # spans are unknowable), and those rows keep the proportional render.
        "flow_sig_span_s": getattr(event, "flow_sig_span_s", None),
        "pressure_sig_span_s": getattr(event, "pressure_sig_span_s", None),
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
        # dev38 annotate-only meter-registration estimate (see flow_integral).
        "registration_est_litres": registration_est,
        "registration_curve_version": registration_curve_ver,
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

        # Derived features for ML clustering — HOME-local time basis (dev38);
        # time_features_tz records which zone produced them so the deferred
        # backfill can detect rows written under a different (or no) zone.
        "duration_log": round(duration_log, 4),
        "hour_of_day": hour,
        "day_of_week": dow,
        "hour_sin": round(math.sin(hour_radians), 4),
        "hour_cos": round(math.cos(hour_radians), 4),
        "is_weekend": 1 if dow >= 5 else 0,
        "time_features_tz": str(_home_tz) if _home_tz is not timezone.utc else None,

        # Composite / training flags
        "is_composite": 1 if event.is_composite else 0,
        "other_valve_open": (
            1 if event.other_valve_open is True
            else 0 if event.other_valve_open is False
            else None
        ),
        # dev41 provenance for the tri-state (NULL on legacy/unknown).
        "other_valve_open_source": getattr(
            event, "other_valve_open_source", None),
        "other_valve_open_set_at": getattr(
            event, "other_valve_open_set_at", None),
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
        # dev19 edge signatures (absolute-time onset/offset; NULL = uncomputable)
        "onset_signature_json":   (json.dumps(edge_pair[0])
                                   if edge_pair else None),
        "offset_signature_json":  (json.dumps(edge_pair[1])
                                   if edge_pair else None),
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


# ── back-compat re-export (unit 7.2) ─────────────────────────────────────────
# ``FeatureExtractor`` and its alert cooldown moved to
# :mod:`.feature_extractor_service`. Every existing
# ``from .feature_extractor import FeatureExtractor`` keeps resolving through
# this hook, so the split cost no caller a change except the one real importer.
#
# A PEP 562 module ``__getattr__`` rather than a plain bottom-of-file import ON
# PURPOSE: the service module imports 14 names OUT of this module at its top
# level. An eager import here would close that loop and raise ImportError
# whenever the service module was imported first — which is exactly what
# ``orchestrator`` does. Resolving lazily keeps the edge one-way at import time.
_MOVED_TO_SERVICE = ("FeatureExtractor", "_ANOMALY_ALERT_COOLDOWN_MIN")


def __getattr__(name: str):
    if name in _MOVED_TO_SERVICE:
        from . import feature_extractor_service as _svc
        return getattr(_svc, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
