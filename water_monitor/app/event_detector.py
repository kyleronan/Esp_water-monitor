"""
Event detector — Phase 1.

Subscribes to real-time state_changed events from HA for:
  - flow rate sensors       (1Hz smoothed)
  - pressure_*_fast         (40Hz, 50ms sliding window)
  - flow_pulse_onset_*      (template binary, 8s delayed_off)

Event lifecycle
---------------
START — triggered by ANY of the following, whichever fires first:

  1. FLOW   — flow rate >= MIN_FLOW_LPM sustained for >= FLOW_START_SECONDS.
               Covers appliances with slow ramp-up, slow-flow fixtures, and
               flows that were already running when the addon connected.

  2. PRESSURE — pressure drops >= pressure_drop_threshold_psi in the fast
                sensor rolling window. Typically the earliest indicator for
                fixtures that open quickly (taps, toilets, washing machines).

  3. PRESSURE+FLOW — both signals arrive close together. The first to cross
                     its threshold opens the event; the second enriches it.

END — flow_pulse_onset transitions OFF *and* flow_rate < MIN_FLOW_LPM.
      Both conditions must be met to prevent false-ends during slow flows
      where the 8s delayed_off causes the binary sensor to flicker.

Pressure transient as enrichment
---------------------------------
A pressure transient is not required for a valid event. When present it adds:
  - pre_event_pressure_psi / pressure_delta_psi  (fixture load signature)
  - propagation_delay_seconds                    (pipe distance heuristic)
  - pressure_readings[]                          (transient shape)
  - is_composite flag                            (multiple fixtures opened)

The start_trigger field on RawEvent records which signal(s) opened the event
so the feature extractor can weight pressure data appropriately.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import struct
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Deque, List, Literal, Optional, Tuple

from .event_rules import LOWFLOW_OFF_GRACE_S, is_low_flow_chatter

log = logging.getLogger(__name__)

StartTrigger = Literal["flow", "pressure", "pressure+flow"]


@dataclass
class RawEvent:
    """Intermediate event record — populated during detection, consumed by FeatureExtractor."""
    circuit: str
    start_ts: datetime
    start_trigger: StartTrigger = "flow"

    end_ts: Optional[datetime] = None

    # Pressure transient fields — populated only when a transient is detected.
    # May be absent for flow-only events.
    # pre_event_pressure_psi is None when the baseline was not trustworthy
    # (e.g. cold start before a settled baseline exists) — see _start_flow_event.
    has_pressure_transient: bool = False
    pre_event_pressure_psi: Optional[float] = 0.0
    min_pressure_psi: float = 0.0
    max_pressure_psi: float = 0.0
    pressure_delta_psi: float = 0.0
    pressure_readings: List[float] = field(default_factory=list)

    # Flow onset relative to event start (only meaningful for pressure-started events)
    flow_onset_ts: Optional[datetime] = None
    propagation_delay_ms: Optional[float] = None
    # Entity ID of the flow_pulse_onset sensor — used by FeatureExtractorWorker
    # to fetch the precise HA-history timestamp after the event closes.
    flow_onset_entity: Optional[str] = None

    # 1Hz flow readings collected during the event
    flow_readings: List[float] = field(default_factory=list)

    # Coalesced timestamped flow samples (ts, L/min) for the volume TIME-INTEGRAL
    # (flow_integral.integrate_litres / active_flow_features). Distinct from the
    # downsampled flow_readings used for the 32-pt signature: volume must be a
    # time-integral, not mean(flow) × pressure-window duration.
    flow_samples: List[Tuple[datetime, float]] = field(default_factory=list)

    # True if any other circuit's valve was open when this event started.
    # Helps distinguish main-circuit irrigation bleed-through from household demand.
    other_valve_open: Optional[bool] = None

    # Measured volume from the firmware's cumulative integration sensor.
    # Set by the historical importer when the volume_sensor entity is available.
    # Preferred over the flow-average approximation in feature extraction.
    volume_litres_measured: Optional[float] = None

    is_composite: bool = False
    complete: bool = False

    # dev.24 low-flow off-grace: when a sustained low draw dips below MIN_FLOW,
    # the event is held open until this deadline instead of finalizing, so the
    # turbine's low-flow chatter doesn't fragment one draw into many events.
    # Cleared by on_flow_rate the instant flow resumes (bridging the dip).
    low_flow_hold_until: Optional[datetime] = None


# --------------------------------------------------------------------------- #
# Propagation-delay scan — shared by live detection and the offline replay tool
# --------------------------------------------------------------------------- #

def _read_addon_version() -> Optional[str]:
    """Best-effort add-on version from config.yaml — None if unavailable."""
    try:
        cfg = Path(__file__).resolve().parents[1] / "config.yaml"
        for line in cfg.read_text(encoding="utf-8").splitlines():
            if line.startswith("version:"):
                return line.split(":", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return None


def _read_git_commit() -> Optional[str]:
    """Best-effort git short commit — None when not in a git checkout."""
    try:
        git_dir = Path(__file__).resolve().parents[2] / ".git"
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref:"):
            ref = head.split(":", 1)[1].strip()
            return (git_dir / ref).read_text(encoding="utf-8").strip()[:12]
        return head[:12]
    except Exception:
        return None


_ADDON_VERSION = _read_addon_version()
_GIT_COMMIT = _read_git_commit()

# Propagation scan parameters.  The fast-pressure sensor is event-driven
# (publishes on change), so the buffer is variable-rate — all timing is done
# from real per-sample timestamps, never a sample-count assumption.
_PROP_MAX_LOOKBACK_S = 12.0      # only search this far back from flow onset
_PROP_BASELINE_GUARD_S = 5.0     # samples older than (flow_onset - this) form the baseline
_PROP_MA_HALF_S = 0.5            # centered moving-average half-width (-> 1 s window)
_PROP_NOISE_BAND = 0.10          # PSI below the local baseline that marks transient onset
_PROP_ABOVE_RUN = 5              # consecutive at-baseline samples confirming pre-drop
_PROP_MIN_BASELINE_SAMPLES = 5   # minimum samples required in the baseline-guard region


def _median(values: List[float]) -> float:
    s = sorted(values)
    k = len(s)
    mid = k // 2
    return s[mid] if k % 2 else (s[mid - 1] + s[mid]) / 2.0


@dataclass
class PropagationScanResult:
    """Outcome of scan_propagation_delay — the delay plus the diagnostics
    needed to see *why* the scan produced it, in logs or offline replay."""
    delay_ms: Optional[float]           # value to store; None when undetermined
    status: str                         # "valid" | "clamped" | "unknown"
    stop_reason: str                    # no_timestamps|window_too_short|no_baseline|
                                        # gate_failed|no_onset|above_run|window_start
    sample_count: int
    buffer_span_s: Optional[float]
    baseline_psi: Optional[float]       # local resting baseline (median of guard region)
    min_pressure_psi: Optional[float]
    min_smoothed_psi: Optional[float]
    magnitude_gate_passed: bool
    onset_index: Optional[int]          # samples back from the newest sample
    onset_ts: Optional[datetime]
    raw_delay_ms: Optional[float]       # before the >= 0 clamp
    final_delay_ms: Optional[float]     # after clamp; mirrors delay_ms


def scan_propagation_delay(
    pressure: List[float],
    timestamps: Optional[List[datetime]],
    flow_onset_ts: Optional[datetime],
    propagation_onset_psi: float,
) -> PropagationScanResult:
    """Find the pressure-transient onset and derive the propagation delay
    (flow onset minus transient onset, in ms).

    This is the single scan implementation used by both live detection
    (CircuitEventDetector._start_flow_event) and the offline replay harness —
    never duplicate it.

    The fast-pressure buffer is event-driven and variable-rate, so the scan is
    fully timestamp-based — there is no samples-per-second assumption:

      1. Restrict to a recent window (_PROP_MAX_LOOKBACK_S before flow onset)
         so the search cannot wander back across earlier events.
      2. Centered 1-second time-windowed moving average to reject noise.
      3. Local resting baseline = median of the smoothed samples older than
         _PROP_BASELINE_GUARD_S before flow onset (guaranteed pre-drop for
         realistic 1-3 s delays) — NOT the global max, which sits above the
         noisy / incompletely-recovered resting level.
      4. Walk newest->oldest to the transient onset (last sample at the local
         baseline); delay = flow_onset_ts - onset_ts from the real timestamps.
    """
    n = len(pressure)
    ts_ok = bool(timestamps) and len(timestamps) == n and n > 0
    span = (timestamps[-1] - timestamps[0]).total_seconds() if ts_ok else None

    def _result(**kw) -> PropagationScanResult:
        base = dict(
            delay_ms=None, status="unknown", stop_reason="unknown",
            sample_count=n, buffer_span_s=span, baseline_psi=None,
            min_pressure_psi=None, min_smoothed_psi=None,
            magnitude_gate_passed=False, onset_index=None, onset_ts=None,
            raw_delay_ms=None, final_delay_ms=None,
        )
        base.update(kw)
        return PropagationScanResult(**base)

    if not ts_ok:
        return _result(stop_reason="no_timestamps")
    if flow_onset_ts is None:
        return _result(stop_reason="no_flow_onset")

    # 1. Bound to a recent window before flow onset.
    win_start = flow_onset_ts - timedelta(seconds=_PROP_MAX_LOOKBACK_S)
    win = [(t, p) for t, p in zip(timestamps, pressure) if t >= win_start]
    if len(win) < _PROP_ABOVE_RUN + _PROP_MIN_BASELINE_SAMPLES:
        return _result(stop_reason="window_too_short")
    win_ts = [t for t, _ in win]
    win_p = [p for _, p in win]
    m = len(win)

    # 2. Centered 1-second time-windowed moving average.
    smoothed: List[float] = []
    for i in range(m):
        lo = win_ts[i] - timedelta(seconds=_PROP_MA_HALF_S)
        hi = win_ts[i] + timedelta(seconds=_PROP_MA_HALF_S)
        seg = [win_p[j] for j in range(m) if lo <= win_ts[j] <= hi]
        smoothed.append(sum(seg) / len(seg))

    min_smoothed = min(smoothed)
    min_pressure = min(win_p)

    # 3. Local resting baseline — median of the pre-drop guard region.
    guard_cut = flow_onset_ts - timedelta(seconds=_PROP_BASELINE_GUARD_S)
    guard = [smoothed[i] for i in range(m) if win_ts[i] <= guard_cut]
    if len(guard) < _PROP_MIN_BASELINE_SAMPLES:
        return _result(stop_reason="no_baseline", min_pressure_psi=min_pressure,
                       min_smoothed_psi=min_smoothed)
    baseline = _median(guard)

    # Magnitude gate: a real transient must fall >= onset PSI below baseline.
    if baseline - min_smoothed < propagation_onset_psi:
        return _result(stop_reason="gate_failed", baseline_psi=baseline,
                       min_pressure_psi=min_pressure, min_smoothed_psi=min_smoothed)

    # 4. Walk newest->oldest to the transient onset.
    onset_threshold = baseline - _PROP_NOISE_BAND
    above_run = 0
    onset_i: Optional[int] = None
    stop_reason = "window_start"
    for i in range(m - 1, -1, -1):
        if smoothed[i] >= onset_threshold:
            above_run += 1
            if above_run >= _PROP_ABOVE_RUN:
                stop_reason = "above_run"
                break
        else:
            above_run = 0
            onset_i = i

    if onset_i is None:
        return _result(stop_reason="no_onset", baseline_psi=baseline,
                       min_pressure_psi=min_pressure, min_smoothed_psi=min_smoothed,
                       magnitude_gate_passed=True)

    onset_ts = win_ts[onset_i]
    raw_delay_ms = (flow_onset_ts - onset_ts).total_seconds() * 1000.0
    if raw_delay_ms > 0:
        final_delay_ms = round(raw_delay_ms, 1)
        status = "valid"
    else:
        final_delay_ms = 0.0
        status = "clamped"

    return _result(
        delay_ms=final_delay_ms, status=status, stop_reason=stop_reason,
        baseline_psi=baseline, min_pressure_psi=min_pressure,
        min_smoothed_psi=min_smoothed, magnitude_gate_passed=True,
        onset_index=m - 1 - onset_i, onset_ts=onset_ts,
        raw_delay_ms=round(raw_delay_ms, 1), final_delay_ms=final_delay_ms,
    )


class CircuitEventDetector:
    """
    Event detector for a single circuit.

    All three start triggers (flow, pressure, combined) are first-class.
    Pressure transient data enriches the event when available but is never
    required for an event to be recorded.
    """

    # ------------------------------------------------------------------ #
    # Tuning constants                                                     #
    # ------------------------------------------------------------------ #

    # Pressure history buffer.
    # At 40 Hz (25 ms/sample) this holds 10 seconds of readings.
    # A large buffer is needed so we can look back far enough to get a
    # clean pre-transient baseline even when the dip takes 2-5 seconds
    # to fully develop.  A short rolling baseline would start chasing
    # the dip and underestimate the actual pressure drop.
    PRESSURE_BUFFER_SIZE: int = 400         # 10 s x 40 Hz

    # Historical baseline window.
    # When checking for a transient we compare the current pressure against
    # an average of samples BASELINE_LOOKBACK_SAMPLES to
    # BASELINE_LOOKBACK_SAMPLES + BASELINE_WINDOW_SAMPLES old.
    #
    # With the defaults below:
    #   baseline source : pressure from 3-5 s ago
    #   lookback start  : 3 s  (120 samples x 25 ms)
    #   lookback window : 2 s  ( 80 samples x 25 ms)
    #
    # A transient that takes up to 5 s to reach minimum is still compared
    # against a baseline that pre-dates the dip entirely.
    # Detection begins once LOOKBACK + WINDOW samples have accumulated
    # (~5 s warm-up, well inside the 30 s firmware startup grace period).
    BASELINE_LOOKBACK_SAMPLES: int = 120    # 3 s lookback
    BASELINE_WINDOW_SAMPLES: int = 80       # 2 s averaging window

    # Minimum flow rate considered real flow (filters ADC noise). This CLASS value
    # is the 396-ppl turbine default (60 ÷ 396 ≈ 0.15 L/min); __init__ overrides it
    # per circuit with the meter-derived floor (60 ÷ ppl) — see CircuitConfig.min_flow_lpm.
    MIN_FLOW_LPM: float = 0.15

    # Maximum physically plausible flow rate for any residential/light-commercial
    # system.  Readings above this are treated as sensor overflow / firmware error
    # values (e.g. 1.58e+36 L/min from ESP ADC overflow) and clamped to 0.0.
    # 1000 L/min ≈ 264 gal/min — well beyond any domestic water supply.
    MAX_FLOW_LPM: float = 200.0   # matches firmware v3.5 ADC overflow clamp ceiling

    # Minimum physically meaningful flow rate from this sensor.
    # The pulse counter cannot produce a non-zero value below 1 pulse/second,
    # which converts to 60 counts/min ÷ ppl (≈0.15 L/min at 396 ppl, ≈0.83 at
    # 72 ppl) — that floor is the per-circuit MIN_FLOW_LPM. MIN_NOISE_LPM below is a
    # meter-independent float-underflow guard: values in (0, MIN_NOISE_LPM) are
    # noise (e.g. 1.58e-36 L/min from ESPHome ADC underflow) and are treated as zero.
    MIN_NOISE_LPM: float = 0.05

    # Seconds of sustained flow required to open a flow-triggered event
    FLOW_START_SECONDS: float = 2.0

    # Consecutive sub-MIN_FLOW samples tolerated mid-formation before the sustain
    # timer is abandoned. The turbine chatters / drops out for a sample or two on a
    # slow ramp-up, and a single glitch must not reset a nearly-complete timer.
    FLOW_START_DIP_TOLERANCE: int = 2

    # Maximum silent gap between flow samples an armed sustain timer survives.
    # The dip tolerance counts SAMPLES, but flow_rate can simply stop ticking
    # after a brief burst (fewer zero samples arrive than the tolerance), so a
    # timer armed by a ~10 s slug can stay armed for minutes; the NEXT burst
    # then instantly satisfies FLOW_START_SECONDS and _start_flow_event
    # backdates start_ts across the whole quiet gap, and the volume integral
    # forward-fills that gap at the old burst's flow (observed: booster-pump
    # top-up slugs ~5 min apart merging into one ~300 s / ~5.4 L event). The
    # firmware pulse_meter reports within 10 s of flow stopping, so a 30 s
    # sample gap while the timer is armed can only mean the sensor went quiet.
    FLOW_START_STALE_GAP_S: float = 30.0

    # Composite: second transient must be >= this multiple of primary threshold
    COMPOSITE_TRANSIENT_MULTIPLIER: float = 1.5

    # Minimum seconds the settled baseline must be stable before a pressure
    # drop can open an event.  Prevents oscillation peaks (rising→flat→falling)
    # from being mistaken for a fixture open — real house pressure is stable for
    # minutes before any tap is turned on.
    PRESSURE_STABLE_DURATION_S: float = 10.0

    # Pressure-recovery END for pressure-triggered events.
    # A pulsed-flow event stays open while the dip persists; it closes once the
    # dip has recovered to ≤ FRACTION of its starting magnitude for this many seconds.
    PRESSURE_RECOVERY_FRACTION: float = 0.5
    PRESSURE_RECOVERY_DURATION_S: float = 10.0
    # Flow-override END: if pressure has been recovered for this much longer AND
    # the flow reading is STALE (no sample within FLOW_SAMPLE_STALE_S), end the
    # event despite the last flow value. Covers a flow sensor that reports
    # high while flowing but never reports 0 when it stops, leaving
    # _current_flow_lpm stale-high so the flow<MIN gate never fires — the cause of
    # the 27.6 h irrigation event. Pressure (40 Hz) is the authority once it has
    # sat at baseline this long; a real run keeps pressure DROPPED so the timer
    # only completes when the draw is genuinely over.
    PRESSURE_RECOVERY_FLOW_OVERRIDE_S: float = 300.0   # 5 min
    # The override additionally requires the flow READING itself to be stale —
    # a live, healthy flow sample vetoes the "pressure says we're done"
    # heuristic. A constant-pressure (VFD booster pump) home restores line
    # pressure DURING a draw, so without this guard the override chopped one
    # 42-minute shower into three events (2026-07-31: force-closed at 5 min of
    # pump-held baseline pressure with 5.6 L/min still flowing, twice). The
    # stuck-sensor case the override exists for goes SILENT (HA fires only on
    # state change), so sample staleness is the honest proxy for "the flow
    # reading can't be trusted". 120 s tolerates a perfectly steady reading
    # that publishes rarely; a genuinely stuck sensor is silent for hours.
    FLOW_SAMPLE_STALE_S: float = 120.0
    # Absolute hard cap on event duration (watchdog). The longest legitimate run
    # (a multi-zone irrigation cycle) is ~2.8 h; anything past this is a missed
    # end signal, so force-close. Bounds the blast radius of ANY unclosed event.
    MAX_EVENT_DURATION_S: float = 21600.0              # 6 h
    # Fast-close for a pure-pressure transient that never moved water. A small
    # dip whose pressure SETTLES below the recovery line (common on the irrigation
    # circuit when a zone solenoid shifts the steady pressure) never satisfies the
    # recovery END, so without this it stays open until the 6 h watchdog above —
    # and while open it blocks every new event on the circuit (the _active_event
    # is None gate), so the next real draw / irrigation run is missed live. Real
    # draws register flow (>= MIN_FLOW) or keep pressure actively dipping, so they
    # are never closed here. Conservative: flow has been zero for the whole event.
    SETTLED_NOFLOW_CLOSE_S: float = 60.0

    # Minimum event volume.  Events whose computed volume (avg_flow × duration)
    # is below this threshold are discarded as noise.  1 mL is a sanity floor —
    # no real water-use event produces less than 1 mL.
    MIN_EVENT_VOLUME_L: float = 0.001

    # Pressure-surge phantom rejection.  If the maximum pressure seen during an
    # event is more than this amount ABOVE the pre-event baseline AND no net
    # pressure drop occurred (pressure_delta_psi <= 0), the event is a turbine
    # artefact caused by a surge (pump, water hammer) rather than real flow.
    PRESSURE_SURGE_PHANTOM_PSI: float = 0.5

    # Gate for updating the settled-pressure baseline.
    # Only accept a sample as "resting" when historical vs. current pressure
    # is within this margin — blocks updates during post-event recovery where
    # the historical baseline still lags below the rising actual pressure.
    SETTLED_STABILITY_PSI: float = 0.3
    # Minimum pressure drop (PSI below baseline) that marks the onset of a
    # pressure event when scanning the buffer to compute propagation delay.
    PROPAGATION_ONSET_PSI: float = 0.2

    def __init__(
        self,
        circuit: str,
        pressure_drop_threshold_psi: float,
        min_event_duration_seconds: float,
        event_queue: asyncio.Queue,
        get_other_valve_open: Optional[Callable[[], Optional[bool]]] = None,
        flow_onset_entity: Optional[str] = None,
        debug_capture_propagation: bool = False,
        min_flow_lpm: float = 0.15,
    ) -> None:
        self.circuit = circuit
        self.pressure_drop_threshold = pressure_drop_threshold_psi
        self.min_event_duration = min_event_duration_seconds
        # Per-circuit meter-derived low-flow floor (60 ÷ ppl), overriding the class
        # default. A coarser meter (72-ppl oval gear → 0.83) must not treat
        # single-pulse quantization as real flow; the 396-ppl turbine stays at 0.15.
        self.MIN_FLOW_LPM = min_flow_lpm
        self._event_queue = event_queue
        self._flow_onset_entity: Optional[str] = flow_onset_entity
        # Callable provided by parent EventDetector to read other-circuit valve states
        self._get_other_valve_open: Callable[[], Optional[bool]] = (
            get_other_valve_open or (lambda: None)
        )

        self._debug_capture_propagation: bool = debug_capture_propagation

        # dev25 (pump plan Phase 4b) — pump-mode oscillation gate. None = off.
        # When set (confirmed vfd pump mode), PRESSURE-initiated event starts
        # are suppressed while the rolling 60 s pressure peak-to-peak exceeds
        # this gate: a recharge sawtooth crosses the 1.2 PSI drop trigger on
        # every cycle and each blip-opened event can swallow a real draw that
        # starts before the 60 s settled-noflow close (the 2026-07-20 10:03
        # double-counted flush). The FLOW path is untouched and remains the
        # primary detector; firmware trickle detection is independent → the
        # suppression can never mask a leak. The gate value is amplitude-
        # derived by the parent (max(2.0, 0.15 × measured band)) so a milder
        # pump than the incident's 12 PSI band still gates correctly.
        self.pump_osc_gate_psi: Optional[float] = None
        # Surge-phantom rejection threshold as an INSTANCE attr: in pump mode
        # a recharge upswing during a real event is exactly the "max pressure
        # rose above baseline" pattern this rejects, so the gate is widened to
        # effectively-off (20 PSI > any recharge band) instead of 0.5.
        self.pressure_surge_phantom_psi: float = self.PRESSURE_SURGE_PHANTOM_PSI
        # Rolling ~60 s of (ts, psi) for the oscillation gate. At 40 Hz this
        # is ~2400 entries; p2p is only computed when a pressure start would
        # otherwise fire, so the steady-state cost is just the appends.
        self._minute_pressure: Deque[Tuple[datetime, float]] = deque(maxlen=2600)

        # ── dev27 (Phase 6) low-pressure alert state machines ────────────────
        # 6a zone under-load floor (zone circuits only; None = off): sustained
        # low pressure WHILE a zone is flowing — heads may not pop up. Two-
        # stage timing: fill grace after run start AND after any significant
        # flow step (multi-zone controllers transition zones without flow
        # hitting zero), then a multi-minute sustain.
        self.zone_low_floor_psi: Optional[float] = None
        self.LOW_PSI_FILL_GRACE_S: float = 120.0
        self.LOW_PSI_SUSTAIN_S: float = 180.0
        self.LOW_PSI_STEP_FRACTION: float = 0.30
        self._lp_run_started: Optional[datetime] = None
        self._lp_grace_until: Optional[datetime] = None
        self._lp_below_since: Optional[datetime] = None
        self._lp_fired_this_run: bool = False
        self._lp_prev_flow: float = 0.0
        self._lp_flow_low_since: Optional[datetime] = None
        self.low_pressure_cb: Optional[Callable[[str, float], None]] = None

        # 6b pump-failure floor (armed vfd pump homes, main circuit; None =
        # off): pressure sustained below the pump's normal band. A recharge
        # rise inside the window resets it (pump alive); at fire time the
        # current flow branches failure vs overload copy (a maxed-out VFD
        # serving heavy demand is NOT a dead pump).
        self.pump_fail_floor_psi: Optional[float] = None
        self.PUMP_FAIL_SUSTAIN_S: float = 300.0
        self.PUMP_FAIL_RISE_RESET_PSI: float = 2.0
        self.PUMP_FAIL_OVERLOAD_FLOW_LPM: float = 2.0
        self._pf_below_since: Optional[datetime] = None
        self._pf_window_min: Optional[float] = None
        self._pf_fired: bool = False
        self.pump_fail_cb: Optional[Callable[[str, float, str], None]] = None

        self._pressure_buf: Deque[float] = deque(maxlen=self.PRESSURE_BUFFER_SIZE)
        # Per-sample arrival timestamps, kept exactly parallel to _pressure_buf
        # (same maxlen, appended/cleared together) — diagnostic use only.
        self._pressure_ts_buf: Deque[datetime] = deque(maxlen=self.PRESSURE_BUFFER_SIZE)
        self._settled_pressure_psi: Optional[float] = None
        self._settled_pressure_since: Optional[datetime] = None
        self._active_event: Optional[RawEvent] = None
        self._current_flow_lpm: float = 0.0
        self._flow_sustained_since: Optional[datetime] = None
        self._flow_start_dips: int = 0   # consecutive sub-threshold samples mid-start
        self._last_flow_sample_ts: Optional[datetime] = None  # any flow_rate sample
        self._pressure_recovered_since: Optional[datetime] = None

        # Downsampling: keep all readings for the first N seconds, then every Kth.
        # Prevents 290k-sample lists for 2-hour irrigation events.
        self._DOWNSAMPLE_AFTER_SECONDS: float = 120.0
        self._DOWNSAMPLE_KEEP_EVERY: int = 5
        self._flow_sample_count: int = 0
        self._pressure_sample_count: int = 0

        # Coalescing for the timestamped flow_samples (volume integral): keep a
        # sample only when the flow changes by > delta, crosses the on/off
        # threshold, or a heartbeat elapses. The heartbeat (< the integrator's
        # 120s dt cap) guarantees a steady flow never trips the offline-gap cap,
        # while bounding memory to ~O(seconds of change) instead of O(callbacks).
        self._FLOW_SAMPLE_MIN_DELTA_LPM: float = 0.2
        self._FLOW_SAMPLE_HEARTBEAT_S: float = 30.0

        # Pre-trigger onset buffer: idle (ts, flow_lpm) kept for ~5 s so the
        # opening ramp the FLOW_START_SECONDS sustain trigger discards can seed
        # flow_readings at event start. SIGNATURE-ONLY — it never feeds
        # flow_samples, so the volume integral is byte-identical with/without it.
        # The deque maxlen is the hard count cap; the time-trim keeps only ~5 s.
        self._PRETRIGGER_WINDOW_S: float = 5.0
        self._pretrigger_flow: Deque[Tuple[datetime, float]] = deque(maxlen=64)

    # ------------------------------------------------------------------ #
    # Public                                                               #
    # ------------------------------------------------------------------ #

    def update_threshold(self, threshold_psi: float) -> None:
        self.pressure_drop_threshold = threshold_psi

    def update_pump_gate(self, gate_psi: Optional[float]) -> None:
        """Set/clear the pump-mode oscillation gate (dev25). Also widens the
        surge-phantom rejection to effectively-off while pump mode is on."""
        self.pump_osc_gate_psi = gate_psi
        self.pressure_surge_phantom_psi = (
            20.0 if gate_psi is not None else self.PRESSURE_SURGE_PHANTOM_PSI)

    def update_low_pressure_config(self, zone_floor_psi: Optional[float],
                                   pump_fail_floor_psi: Optional[float]) -> None:
        """Set/clear the Phase 6 low-pressure floors (dev27)."""
        self.zone_low_floor_psi = zone_floor_psi
        self.pump_fail_floor_psi = pump_fail_floor_psi

    def settled_pressure(self) -> Optional[Tuple[float, datetime]]:
        """Current idle-line (settled) pressure as ``(psi, stable_since)``, or
        None when there is no trustworthy baseline — during an active event,
        after a sensor blip cleared the buffer, or before the buffer warmed up.
        Read-only accessor for the supply-regime tracker; inherits the settled-
        baseline trust semantics of on_pressure_fast for free."""
        if self._active_event is not None or self._settled_pressure_psi is None:
            return None
        return (self._settled_pressure_psi, self._settled_pressure_since)

    def _track_zone_flow(self, now: datetime, flow: float) -> None:
        """6a run/grace bookkeeping, called from on_flow_rate on zone
        circuits. A run starts at the first above-floor flow after idle; the
        fill grace re-arms on any >=30% flow step (zone transition)."""
        if flow >= self.MIN_FLOW_LPM:
            self._lp_flow_low_since = None
            if self._lp_run_started is None:
                self._lp_run_started = now
                self._lp_grace_until = now + timedelta(
                    seconds=self.LOW_PSI_FILL_GRACE_S)
                self._lp_below_since = None
                self._lp_fired_this_run = False
            elif (self._lp_prev_flow >= self.MIN_FLOW_LPM
                    and abs(flow - self._lp_prev_flow)
                    >= self.LOW_PSI_STEP_FRACTION
                    * max(self._lp_prev_flow, 0.001)):
                self._lp_grace_until = now + timedelta(
                    seconds=self.LOW_PSI_FILL_GRACE_S)
                self._lp_below_since = None
            self._lp_prev_flow = flow
        else:
            self._lp_prev_flow = flow
            if self._lp_run_started is not None:
                if self._lp_flow_low_since is None:
                    self._lp_flow_low_since = now
                elif (now - self._lp_flow_low_since).total_seconds() >= 60:
                    self._lp_run_started = None      # run over
                    self._lp_below_since = None

    def _eval_zone_low_pressure(self, now: datetime, pressure: float) -> None:
        """6a: floor check while a zone run is active and past its grace."""
        if (self.zone_low_floor_psi is None or self._lp_run_started is None
                or self._lp_fired_this_run):
            return
        if self._lp_grace_until is not None and now < self._lp_grace_until:
            return
        if pressure >= self.zone_low_floor_psi:
            self._lp_below_since = None              # excursion resets sustain
            return
        if self._lp_below_since is None:
            self._lp_below_since = now
            return
        if ((now - self._lp_below_since).total_seconds()
                >= self.LOW_PSI_SUSTAIN_S):
            self._lp_fired_this_run = True           # one alert per zone run
            if self.low_pressure_cb is not None:
                try:
                    self.low_pressure_cb(self.circuit, pressure)
                except Exception as e:
                    log.warning("[%s] low-pressure callback failed: %s",
                                self.circuit, e)

    def _eval_pump_fail(self, now: datetime, pressure: float) -> None:
        """6b: sustained sub-floor pressure = pump failed/off/overloaded."""
        floor = self.pump_fail_floor_psi
        if floor is None:
            return
        if pressure >= floor:
            self._pf_below_since = None
            self._pf_window_min = None
            if pressure >= floor + self.PUMP_FAIL_RISE_RESET_PSI:
                self._pf_fired = False               # recovered — re-arm
            return
        if self._pf_below_since is None:
            self._pf_below_since = now
            self._pf_window_min = pressure
            return
        self._pf_window_min = min(self._pf_window_min, pressure)
        if pressure - self._pf_window_min >= self.PUMP_FAIL_RISE_RESET_PSI:
            # Recharge rise inside the window — the pump is alive; restart.
            self._pf_below_since = now
            self._pf_window_min = pressure
            return
        if (not self._pf_fired
                and (now - self._pf_below_since).total_seconds()
                >= self.PUMP_FAIL_SUSTAIN_S):
            self._pf_fired = True
            kind = ("overload"
                    if self._current_flow_lpm >= self.PUMP_FAIL_OVERLOAD_FLOW_LPM
                    else "failure")
            if self.pump_fail_cb is not None:
                try:
                    self.pump_fail_cb(self.circuit, pressure, kind)
                except Exception as e:
                    log.warning("[%s] pump-fail callback failed: %s",
                                self.circuit, e)

    def _minute_p2p(self, now: datetime) -> float:
        """Peak-to-peak PSI over the trailing 60 s (prunes in place)."""
        cutoff = now - timedelta(seconds=60)
        buf = self._minute_pressure
        while buf and buf[0][0] < cutoff:
            buf.popleft()
        if len(buf) < 2:
            return 0.0
        vals = [p for _, p in buf]
        return max(vals) - min(vals)

    def update_min_flow(self, min_flow_lpm: float) -> None:
        """Update the meter-derived low-flow floor live (after a PPL change)."""
        self.MIN_FLOW_LPM = min_flow_lpm

    # ------------------------------------------------------------------ #
    # HA state_changed callbacks                                           #
    # ------------------------------------------------------------------ #

    def on_flow_rate(self, entity_id: str, state: str, attributes: dict) -> None:
        """
        1 Hz smoothed flow rate.

        - Appends reading to active event.
        - Drives the FLOW start trigger via a sustained-flow timer.
        - Resets the timer when flow drops below MIN_FLOW_LPM.
        """
        try:
            raw_flow = float(state)
        except (ValueError, TypeError):
            raw_flow = 0.0

        # Guard against ESP firmware ADC garbage values. Two failure modes:
        #   HIGH: overflow produces huge values (e.g. 1.58e+36 L/min).
        #   LOW:  underflow/noise produces tiny near-zero values (e.g. 1.58e-36)
        #         that are positive but below the minimum meaningful reading.
        # Both are treated as zero so event end detection is not blocked.
        if raw_flow > self.MAX_FLOW_LPM or raw_flow < 0.0 or (
                0.0 < raw_flow < self.MIN_NOISE_LPM):
            log.warning(
                "[%s] flow rate sensor returned implausible value %.3g L/min "
                "— treating as 0.0 (ADC overflow, underflow, or firmware error)",
                self.circuit, raw_flow,
            )
            raw_flow = 0.0
        self._current_flow_lpm = raw_flow

        now = datetime.now(timezone.utc)

        # Stale-timer guard: an armed sustain timer must not survive a silent
        # sensor gap (see FLOW_START_STALE_GAP_S). Reset it so this sample is
        # judged as a fresh start instead of "sustained" since minutes ago.
        if (self._flow_sustained_since is not None
                and self._last_flow_sample_ts is not None
                and (now - self._last_flow_sample_ts).total_seconds()
                > self.FLOW_START_STALE_GAP_S):
            log.debug(
                "[%s] flow start timer reset — stale (%.0f s since last flow "
                "sample, armed %s)", self.circuit,
                (now - self._last_flow_sample_ts).total_seconds(),
                self._flow_sustained_since.isoformat(),
            )
            self._flow_sustained_since = None
            self._flow_start_dips = 0
        self._last_flow_sample_ts = now

        # Phase 6a zone-run bookkeeping (no-op unless a zone floor is set).
        if self.zone_low_floor_psi is not None:
            self._track_zone_flow(now, raw_flow)

        if self._active_event is not None:
            ev = self._active_event
            # dev.24 low-flow off-grace: flow resuming bridges a held dip (the
            # same event continues); a hold whose grace expired with no resume
            # finalizes here. Otherwise fall through and record as before.
            if ev.low_flow_hold_until is not None and self._current_flow_lpm >= self.MIN_FLOW_LPM:
                ev.low_flow_hold_until = None
                log.debug("[%s] low-flow hold released — flow resumed (%.3f L/min)",
                          self.circuit, self._current_flow_lpm)
            elif self._maybe_finalize_held_low_flow(now):
                return
            if self._maybe_force_close_overlong(now):        # over-long watchdog
                return
            elapsed = (now - self._active_event.start_ts).total_seconds()
            self._flow_sample_count += 1
            if elapsed < self._DOWNSAMPLE_AFTER_SECONDS or self._flow_sample_count % self._DOWNSAMPLE_KEEP_EVERY == 0:
                self._active_event.flow_readings.append(self._current_flow_lpm)
            # Coalesced timestamped capture for the volume integral.
            fs = self._active_event.flow_samples
            v = self._current_flow_lpm
            if not fs:
                fs.append((now, v))
            else:
                lt, lv = fs[-1]
                crossed = (lv > self.MIN_FLOW_LPM) != (v > self.MIN_FLOW_LPM)
                if (crossed
                        or abs(v - lv) > self._FLOW_SAMPLE_MIN_DELTA_LPM
                        or (now - lt).total_seconds() >= self._FLOW_SAMPLE_HEARTBEAT_S):
                    fs.append((now, v))
            self._flow_sustained_since = None
            return

        # No active event — manage flow start timer.
        # Pre-trigger onset buffer (signature-only): record idle flow regardless
        # of the MIN_FLOW gate so the sub-threshold opening ramp — lost to the
        # FLOW_START_SECONDS sustain delay — can seed flow_readings. NEVER touches
        # flow_samples (the volume integral).
        self._pretrigger_flow.append((now, self._current_flow_lpm))
        _pt_cutoff = now - timedelta(seconds=self._PRETRIGGER_WINDOW_S)
        while self._pretrigger_flow and self._pretrigger_flow[0][0] < _pt_cutoff:
            self._pretrigger_flow.popleft()

        if self._current_flow_lpm >= self.MIN_FLOW_LPM:
            self._flow_start_dips = 0
            if self._flow_sustained_since is None:
                self._flow_sustained_since = now
                log.debug("[%s] flow start timer begins (%.3f L/min)",
                          self.circuit, self._current_flow_lpm)
            elif (now - self._flow_sustained_since).total_seconds() >= self.FLOW_START_SECONDS:
                self._start_flow_event(now)
        elif self._flow_sustained_since is not None:
            # Tolerate a few consecutive sub-threshold samples mid-formation: a dip
            # does NOT advance the timer, but it must not reset a nearly-complete one
            # either, until the tolerance is exceeded.
            self._flow_start_dips += 1
            if self._flow_start_dips > self.FLOW_START_DIP_TOLERANCE:
                log.debug("[%s] flow start timer reset after %d dip(s) (%.3f L/min)",
                          self.circuit, self._flow_start_dips, self._current_flow_lpm)
                self._flow_sustained_since = None
                self._flow_start_dips = 0

    def on_pressure_fast(self, entity_id: str, state: str, attributes: dict) -> None:
        """
        40 Hz fast pressure sensor.

        - Maintains a 10-second rolling history buffer.
        - Computes baseline from samples 3-5 seconds in the past so that
          a slow transient (2-5 s dip) is always compared against clean
          pre-event pressure, not against a baseline that has started
          tracking the dip itself.
        - Fires PRESSURE start trigger if a transient is detected while idle.
        - Enriches an active flow-event with transient metadata if one arrives.
        - Detects composite events (second significant transient) using a
          short within-event baseline so the settled post-drop pressure is
          the reference, not the original pre-event baseline.
        """
        if state in ("unavailable", "unknown"):
            # ESP reconnected or went offline — stale buffer readings would mix
            # with new data and could trigger a false pressure transient.
            self._pressure_buf.clear()
            self._pressure_ts_buf.clear()
            self._settled_pressure_psi = None
            log.debug("[%s] pressure sensor %s — buffer cleared", self.circuit, state)
            return

        try:
            pressure = float(state)
        except (ValueError, TypeError):
            return

        now = datetime.now(timezone.utc)
        self._pressure_buf.append(pressure)
        self._pressure_ts_buf.append(now)
        if self.pump_osc_gate_psi is not None:
            self._minute_pressure.append((now, pressure))
        # Phase 6 low-pressure monitors (no-ops unless a floor is configured).
        self._eval_zone_low_pressure(now, pressure)
        self._eval_pump_fail(now, pressure)

        # Need LOOKBACK + WINDOW samples before baseline is meaningful.
        # At 40 Hz this is ~5 seconds — well inside the firmware grace period.
        min_samples = self.BASELINE_LOOKBACK_SAMPLES + self.BASELINE_WINDOW_SAMPLES
        if len(self._pressure_buf) < min_samples:
            return

        buf = list(self._pressure_buf)

        # Historical baseline: average of a window that ends LOOKBACK samples
        # before now.  With LOOKBACK=120 (3 s) and WINDOW=80 (2 s) this
        # sources the baseline from 3-5 seconds ago — safely before any
        # transient that takes up to 5 s to fully develop.
        b_end   = len(buf) - self.BASELINE_LOOKBACK_SAMPLES
        b_start = b_end - self.BASELINE_WINDOW_SAMPLES
        baseline = sum(buf[b_start:b_end]) / self.BASELINE_WINDOW_SAMPLES
        drop = baseline - pressure   # positive = pressure has fallen

        if self._active_event is None:
            if abs(drop) < self.SETTLED_STABILITY_PSI:
                if (self._settled_pressure_psi is None
                        or abs(baseline - self._settled_pressure_psi) >= 0.1):
                    self._settled_pressure_since = now
                self._settled_pressure_psi = baseline
            if drop >= self.pressure_drop_threshold:
                stable_secs = (
                    0.0 if self._settled_pressure_since is None
                    else (now - self._settled_pressure_since).total_seconds()
                )
                if stable_secs < self.PRESSURE_STABLE_DURATION_S:
                    log.debug(
                        "[%s] pressure drop %.1f PSI suppressed — baseline not yet stable "
                        "(%.1fs < %.1fs required)",
                        self.circuit, drop, stable_secs, self.PRESSURE_STABLE_DURATION_S,
                    )
                elif (self.pump_osc_gate_psi is not None
                        and self._minute_p2p(now) > self.pump_osc_gate_psi):
                    # dev25 pump-mode oscillation gate: the supply is mid-
                    # sawtooth — a pressure-only start here is a recharge
                    # artifact wrapper waiting to swallow a real draw. Flow
                    # starts are unaffected.
                    log.debug(
                        "[%s] pressure start suppressed — pump oscillation "
                        "(60 s p2p %.1f > %.1f PSI gate)",
                        self.circuit, self._minute_p2p(now),
                        self.pump_osc_gate_psi,
                    )
                else:
                    self._start_pressure_event(now, baseline, pressure)
        else:
            # dev.24 low-flow off-grace backstop: flow_rate can stop ticking at 0
            # during a dip, but the fast-pressure sensor keeps sampling — so a
            # held event whose grace expired is finalized here too.
            if self._maybe_finalize_held_low_flow(now):
                return
            if self._maybe_force_close_overlong(now):    # over-long watchdog
                return
            if self._maybe_close_settled_noflow(now):    # stuck no-flow phantom
                return
            elapsed_p = (now - self._active_event.start_ts).total_seconds()
            self._pressure_sample_count += 1
            # Track max on every sample (before downsample gate).
            self._active_event.max_pressure_psi = max(self._active_event.max_pressure_psi, pressure)
            if elapsed_p < self._DOWNSAMPLE_AFTER_SECONDS or self._pressure_sample_count % self._DOWNSAMPLE_KEEP_EVERY == 0:
                self._active_event.pressure_readings.append(pressure)

            ev = self._active_event
            if ev.has_pressure_transient and ev.pressure_delta_psi > 0:
                recovery_line = (
                    ev.pre_event_pressure_psi
                    - ev.pressure_delta_psi * self.PRESSURE_RECOVERY_FRACTION
                )
                if pressure >= recovery_line:
                    if self._pressure_recovered_since is None:
                        self._pressure_recovered_since = now
                    else:
                        rec_secs = (now - self._pressure_recovered_since).total_seconds()
                        flow_stopped = self._current_flow_lpm < self.MIN_FLOW_LPM
                        # Normal END: pressure back >= 10 s AND flow has stopped.
                        # Flow-override END: pressure back for a LONG time AND the
                        # flow reading is STALE — a flow sensor that never reports
                        # 0 on stop goes silent (HA fires on change only), leaving
                        # _current_flow_lpm stale-high so flow<MIN never fires (the
                        # 27.6 h irrigation event). Pressure is the authority once
                        # it has sat at baseline this long WITH no fresh flow data.
                        # A live flow sample vetoes the override: a VFD booster
                        # pump restores line pressure mid-draw, and closing on
                        # pressure alone split one 42-min shower into three events.
                        flow_stale = (
                            self._last_flow_sample_ts is None
                            or (now - self._last_flow_sample_ts).total_seconds()
                            >= self.FLOW_SAMPLE_STALE_S)
                        if ((rec_secs >= self.PRESSURE_RECOVERY_DURATION_S
                             and flow_stopped)
                                or (rec_secs >= self.PRESSURE_RECOVERY_FLOW_OVERRIDE_S
                                    and (flow_stopped or flow_stale))):
                            log.info(
                                "[%s] pressure recovered (>= %.2f PSI for %.0f s, "
                                "flow=%.3f%s) — ending pressure-triggered event",
                                self.circuit, recovery_line, rec_secs,
                                self._current_flow_lpm,
                                "" if flow_stopped else " [flow-override, stale]",
                            )
                            # Definitive "draw is over" — bypasses the off-grace hold.
                            self._end_event(now, force=True)
                            return
                else:
                    self._pressure_recovered_since = None

            if not self._active_event.has_pressure_transient:
                # First transient seen during this event — enrich the record.
                # Use the same historical baseline so the delta is accurate.
                if drop >= self.pressure_drop_threshold:
                    self._record_pressure_transient(now, baseline, pressure)
            else:
                # Composite detection: look for a second significant drop
                # relative to the recently settled pressure (not the original
                # baseline), so we catch a second fixture opening mid-event.
                n = len(self._active_event.pressure_readings)
                if n >= 8 and not self._active_event.is_composite:
                    recent_baseline = (
                        sum(self._active_event.pressure_readings[-8:-3]) / 5
                    )
                    if (recent_baseline - pressure
                            >= self.pressure_drop_threshold
                            * self.COMPOSITE_TRANSIENT_MULTIPLIER):
                        self._active_event.is_composite = True
                        log.debug("[%s] composite transient detected", self.circuit)

    def on_flow_onset(self, entity_id: str, state: str, attributes: dict) -> None:
        """
        flow_pulse_onset binary sensor (8 s delayed_off in firmware).

        ON  — records flow onset time for pressure-triggered events.
        OFF — ends the event ONLY if flow_rate is also below MIN_FLOW_LPM,
              preventing false-ends caused by the binary sensor flickering
              at slow flow rates (< 1 pulse per 8 s).
        """
        now = datetime.now(timezone.utc)
        flow_on = state.lower() in ("on", "true", "1")

        if flow_on:
            ev = self._active_event
            if ev is not None and ev.flow_onset_ts is None:
                # Pressure-triggered event: flow is only being detected now.
                # Scan the buffer for the TRUE transient onset (earlier than
                # the threshold crossing that opened the event) so the delay
                # is measured the same way as for flow-triggered events.
                ev.flow_onset_ts = now
                scan = self._run_propagation_scan(
                    ev.start_trigger, ev.start_ts, now)
                if scan.delay_ms is not None:
                    ev.propagation_delay_ms = scan.delay_ms
                else:
                    # Scan could not locate an onset — fall back to the
                    # threshold-crossing delay so the field is still set.
                    ev.propagation_delay_ms = round(
                        max(0.0, (now - ev.start_ts).total_seconds() * 1000.0), 1)
                log.debug("[%s] flow onset — propagation_delay=%.0f ms",
                          self.circuit, ev.propagation_delay_ms)
        else:
            if self._active_event is not None:
                if self._current_flow_lpm < self.MIN_FLOW_LPM:
                    self._end_event(now)
                else:
                    log.debug(
                        "[%s] flow_pulse_onset OFF suppressed — "
                        "flow_rate still %.3f L/min (slow-flow flicker)",
                        self.circuit, self._current_flow_lpm,
                    )

    # ------------------------------------------------------------------ #
    # Internal lifecycle                                                   #
    # ------------------------------------------------------------------ #

    def _run_propagation_scan(
        self, trigger: str, event_start_ts: datetime, flow_onset_ts: datetime,
    ) -> PropagationScanResult:
        """Scan the pressure buffer for the transient onset, emit the DEBUG
        instrumentation line, and (when enabled) a capture blob.

        Shared by flow-triggered (_start_flow_event, at event start) and
        pressure-triggered (on_flow_onset, when flow is finally detected)
        events.  The caller stores the resulting delay on the event.
        """
        pressure_samples = list(self._pressure_buf)
        pressure_ts = list(self._pressure_ts_buf)
        scan = scan_propagation_delay(
            pressure_samples, pressure_ts, flow_onset_ts,
            self.PROPAGATION_ONSET_PSI,
        )
        self._log_propagation_scan(trigger, flow_onset_ts, scan)
        if self._debug_capture_propagation:
            self._emit_propagation_capture(
                trigger, event_start_ts, flow_onset_ts,
                pressure_samples, pressure_ts, scan)
        return scan

    def _start_flow_event(self, now: datetime) -> None:
        start_ts = self._flow_sustained_since or now
        self._flow_sustained_since = None
        self._pressure_recovered_since = None

        # Warmup gate — pressure-derived fields are only trustworthy once a
        # settled baseline has been established.  Shortly after an addon
        # restart (or a sensor-unavailable buffer clear) there is no clean
        # pre-event reference, so any computed drop/delay would be fabricated.
        # In that case the event is still recorded as a real flow event, but
        # its pressure fields are left as honest unknowns.
        trustworthy = self._settled_pressure_psi is not None

        if trustworthy:
            baseline: Optional[float] = self._settled_pressure_psi
            # For a flow-triggered event, start_ts IS the flow onset.
            scan = self._run_propagation_scan("flow", start_ts, start_ts)
            propagation_delay_ms = scan.delay_ms
        else:
            baseline = None
            propagation_delay_ms = None
            log.debug(
                "[%s] propagation scan skipped — pressure baseline not "
                "trustworthy (no settled pressure / buffer not warm)",
                self.circuit,
            )
            self._log_propagation_scan("flow", start_ts, None)
            if self._debug_capture_propagation:
                self._emit_propagation_capture(
                    "flow", start_ts, start_ts,
                    list(self._pressure_buf), list(self._pressure_ts_buf), None)

        delay_str = ("unknown" if propagation_delay_ms is None
                     else f"{propagation_delay_ms:.0f} ms")
        log.info(
            "[%s] event start (FLOW) — %.3f L/min for >= %.1f s "
            "propagation_delay=%s", self.circuit, self._current_flow_lpm,
            self.FLOW_START_SECONDS, delay_str,
        )

        self._flow_sample_count = 0
        self._pressure_sample_count = 0
        # Seed the flow signature series with the recovered pre-onset ramp
        # (samples strictly before start_ts) so the "front" the sustain
        # trigger cut off is restored. flow_samples (volume) is untouched.
        flow_seed = [(t, v) for (t, v) in self._pretrigger_flow if t < start_ts]
        # Pressure gets the SAME pre-onset window. A flow seed alone starts the
        # two signature series at different times, and every consumer that
        # index-aligns them — the modal waveform overlay and the software
        # flow_pressure_corr — then reads a time-shifted pressure curve (the
        # "garbled pressure" micro-events, 2026-08).
        seed_t0 = flow_seed[0][0] if flow_seed else start_ts
        pressure_seed = [
            p for (t, p) in zip(self._pressure_ts_buf, self._pressure_buf)
            if seed_t0 <= t < start_ts
        ]
        self._active_event = RawEvent(
            circuit=self.circuit,
            start_ts=start_ts,
            start_trigger="flow",
            flow_onset_ts=start_ts,
            propagation_delay_ms=propagation_delay_ms,
            flow_onset_entity=self._flow_onset_entity,
            pre_event_pressure_psi=baseline,
            min_pressure_psi=baseline if baseline is not None else 0.0,
            max_pressure_psi=baseline if baseline is not None else 0.0,
            flow_readings=[v for (_, v) in flow_seed] + [self._current_flow_lpm],
            pressure_readings=pressure_seed,
            flow_samples=[(start_ts, self._current_flow_lpm)],
            other_valve_open=self._get_other_valve_open(),
        )

    def _log_propagation_scan(
        self, trigger: str, flow_onset_ts: datetime,
        scan: Optional[PropagationScanResult],
    ) -> None:
        """Emit one compact DEBUG line describing the propagation scan."""
        if scan is None:
            log.debug("[%s] propagation scan (%s) — skipped "
                      "(baseline untrustworthy)", self.circuit, trigger)
            return

        def _f(v: Optional[float], fmt: str) -> str:
            return fmt % v if v is not None else "n/a"

        onset_ts = (scan.onset_ts.strftime("%H:%M:%S.%f")[:-3]
                    if scan.onset_ts is not None else "n/a")
        log.debug(
            "[%s] propagation scan (%s) — samples=%d span=%s flow_onset=%s "
            "baseline=%s min_p=%s min_sm=%s gate=%s onset_idx=%s onset_ts=%s "
            "stop=%s raw_delay=%s final=%s status=%s",
            self.circuit, trigger, scan.sample_count,
            _f(scan.buffer_span_s, "%.1fs"),
            flow_onset_ts.strftime("%H:%M:%S.%f")[:-3],
            _f(scan.baseline_psi, "%.2f"), _f(scan.min_pressure_psi, "%.2f"),
            _f(scan.min_smoothed_psi, "%.2f"),
            "pass" if scan.magnitude_gate_passed else "fail",
            scan.onset_index if scan.onset_index is not None else "n/a",
            onset_ts, scan.stop_reason,
            _f(scan.raw_delay_ms, "%.0fms"), _f(scan.final_delay_ms, "%.0fms"),
            scan.status,
        )

    def _emit_propagation_capture(
        self, trigger: str, event_start_ts: datetime, flow_onset_ts: datetime,
        pressure_samples: List[float], pressure_ts: List[datetime],
        scan: Optional[PropagationScanResult],
    ) -> None:
        """Emit one compact JSON capture blob (debug_capture_propagation only)
        so a real event can be replayed offline against scan_propagation_delay.

        event_start_ts is when the event opened (flow-confirm time, or the
        pressure threshold crossing); flow_onset_ts is what the scan measures
        against — for pressure-triggered events the two differ.
        """
        try:
            n = len(pressure_samples)
            ts_ok = len(pressure_ts) == n and n > 0
            t0 = pressure_ts[0] if ts_ok else None
            downsample = 2 if n > 600 else 1
            samples = []
            for i in range(0, n, downsample):
                off = (round((pressure_ts[i] - t0).total_seconds() * 1000, 1)
                       if t0 is not None else None)
                samples.append([off, round(pressure_samples[i], 4)])
            blob = {
                "capture": "propagation_delay",
                "meta": {
                    "version": _ADDON_VERSION,
                    "git": _GIT_COMMIT,
                    "circuit": self.circuit,
                    "start_trigger": trigger,
                    "pressure_drop_threshold_psi": self.pressure_drop_threshold,
                    "flow_start_seconds": self.FLOW_START_SECONDS,
                    "propagation_onset_psi": self.PROPAGATION_ONSET_PSI,
                    "ma_half_s": _PROP_MA_HALF_S,
                    "sample_count": n,
                    "buffer_span_s": scan.buffer_span_s if scan else None,
                    "downsample": downsample,
                    "trustworthy_baseline": scan is not None,
                },
                "samples_t0": t0.isoformat() if t0 is not None else None,
                "start_ts": event_start_ts.isoformat(),
                "flow_onset_ts": flow_onset_ts.isoformat(),
                "samples": samples,
                "result": None if scan is None else {
                    "delay_ms": scan.delay_ms,
                    "status": scan.status,
                    "stop_reason": scan.stop_reason,
                    "baseline_psi": scan.baseline_psi,
                    "min_pressure_psi": scan.min_pressure_psi,
                    "min_smoothed_psi": scan.min_smoothed_psi,
                    "magnitude_gate_passed": scan.magnitude_gate_passed,
                    "onset_index": scan.onset_index,
                    "onset_ts": (scan.onset_ts.isoformat()
                                 if scan.onset_ts is not None else None),
                    "raw_delay_ms": scan.raw_delay_ms,
                    "final_delay_ms": scan.final_delay_ms,
                },
            }
            log.debug("[%s] PROPAGATION_CAPTURE %s", self.circuit,
                      json.dumps(blob, separators=(",", ":")))
        except Exception as e:   # never let diagnostics break detection
            log.debug("[%s] propagation capture failed: %s", self.circuit, e)

    def _start_pressure_event(self, now: datetime, baseline: float,
                              current_pressure: float) -> None:
        self._pressure_recovered_since = None
        if self._settled_pressure_psi is not None:
            baseline = self._settled_pressure_psi
        drop = baseline - current_pressure
        log.info("[%s] event start (PRESSURE) — %.1f PSI drop (%.1f -> %.1f PSI)",
                 self.circuit, drop, baseline, current_pressure)

        self._flow_sample_count = 0
        self._pressure_sample_count = 0
        self._active_event = RawEvent(
            circuit=self.circuit,
            start_ts=now,
            start_trigger="pressure",
            has_pressure_transient=True,
            pre_event_pressure_psi=baseline,
            min_pressure_psi=current_pressure,
            max_pressure_psi=current_pressure,
            pressure_delta_psi=drop,
            pressure_readings=[current_pressure],
            # One flow sample at the trigger so both signature series share
            # the trigger-time origin — without it the flow series starts at
            # the FIRST post-trigger flow update (seconds later on a short
            # event) and index-aligned consumers read it time-stretched.
            flow_readings=[self._current_flow_lpm],
            flow_samples=[(now, self._current_flow_lpm)],
            flow_onset_entity=self._flow_onset_entity,
            other_valve_open=self._get_other_valve_open(),
        )
        self._flow_sustained_since = None

    def _record_pressure_transient(self, now: datetime, baseline: float,
                                   current_pressure: float) -> None:
        """Enrich a flow-triggered event with a pressure transient that arrived late."""
        ev = self._active_event
        if ev is None:
            return

        if self._settled_pressure_psi is not None:
            baseline = self._settled_pressure_psi
        drop = baseline - current_pressure
        ev.has_pressure_transient = True
        ev.start_trigger = "pressure+flow"
        ev.pre_event_pressure_psi = baseline
        ev.min_pressure_psi = min(ev.min_pressure_psi or baseline, current_pressure)
        ev.pressure_delta_psi = drop

        log.debug("[%s] pressure transient enriched active event — %.1f PSI drop",
                  self.circuit, drop)

    def _is_low_flow_event(self, ev: RawEvent) -> bool:
        """Low-flow per the shared chatter predicate, measured over the ACTIVE
        flow readings (v >= MIN_FLOW_LPM). Filtering to active flow excludes both
        the sub-threshold pre-trigger ramp and any mid-event zero-flow dips — the
        coalesced flow_samples' sparse leading/trailing zeros would otherwise
        skew the mean (a steady 1.6 L/min event with one trailing 0 sample would
        average to ~0.5 and read as low-flow)."""
        active = [v for v in ev.flow_readings if v >= self.MIN_FLOW_LPM]
        if not active:
            return False
        return is_low_flow_chatter(sum(active) / len(active), max(active))

    def _should_hold_low_flow(self, ev: RawEvent, ts: datetime) -> bool:
        """Whether to hold a low-flow event open through a sub-threshold dip.
        Returns False once the grace deadline passes (-> finalize) and for
        normal-flow events (which end immediately, exactly as before dev.24)."""
        if ev.low_flow_hold_until is not None and ts >= ev.low_flow_hold_until:
            return False
        return self._is_low_flow_event(ev)

    def _maybe_finalize_held_low_flow(self, now: datetime) -> bool:
        """Finalize a held low-flow event whose grace expired with no resume.
        end_ts is the dip time (deadline - grace), not ``now``, so the trailing
        grace wait never inflates the event's duration/volume. Returns True when
        it finalized (the caller must then stop touching self._active_event)."""
        ev = self._active_event
        if (ev is not None and ev.low_flow_hold_until is not None
                and self._current_flow_lpm < self.MIN_FLOW_LPM
                and now >= ev.low_flow_hold_until):
            dip_ts = ev.low_flow_hold_until - timedelta(seconds=LOWFLOW_OFF_GRACE_S)
            self._end_event(dip_ts, force=True)
            return True
        return False

    def _maybe_force_close_overlong(self, now: datetime) -> bool:
        """Watchdog: force-close an event that has exceeded MAX_EVENT_DURATION_S —
        a missed end signal (e.g. a flow sensor that never reports 0 on stop, or a
        stuck zone valve) must never leave an event open for hours/days. Returns
        True when it finalized (caller must stop touching self._active_event)."""
        ev = self._active_event
        if (ev is not None
                and (now - ev.start_ts).total_seconds() > self.MAX_EVENT_DURATION_S):
            log.warning(
                "[%s] force-closing over-long event: %.1f h > %.1f h cap "
                "(missed end signal — e.g. flow sensor not reporting 0 on stop)",
                self.circuit, (now - ev.start_ts).total_seconds() / 3600.0,
                self.MAX_EVENT_DURATION_S / 3600.0,
            )
            self._end_event(now, force=True)
            return True
        return False

    def _maybe_close_settled_noflow(self, now: datetime) -> bool:
        """Close a pure-pressure transient that never moved water once pressure
        has SETTLED (stable, even at a shifted baseline below the recovery line).

        Without this, a small pressure dip that settles below the recovery line —
        e.g. an irrigation zone solenoid that nudges the steady pressure — never
        satisfies the recovery END and stays open until the 6 h watchdog, blinding
        the circuit to new events the whole time (observed: chronic 6 h force-closes
        on circuit_2 blocking irrigation starts). A real draw registers flow or
        keeps pressure actively dipping, so it is never closed here. The closed
        event carries ~0 volume and is discarded by _end_event. Returns True when
        it finalized (the caller must then stop touching self._active_event)."""
        ev = self._active_event
        if ev is None:
            return False
        # Only a PURE pressure transient that never developed flow: flow-triggered
        # and pressure+flow events had water; an onset or any flow_rate >= MIN_FLOW
        # means water moved. All excluded.
        if ev.start_trigger != "pressure" or ev.flow_onset_ts is not None:
            return False
        if self._current_flow_lpm >= self.MIN_FLOW_LPM:
            return False
        if ev.flow_readings and max(ev.flow_readings) >= self.MIN_FLOW_LPM:
            return False
        # Give a real draw time for flow to follow the pressure drop before closing.
        if (now - ev.start_ts).total_seconds() < self.SETTLED_NOFLOW_CLOSE_S:
            return False
        # Pressure must have SETTLED — peak-to-peak of the recent window is small,
        # i.e. the dip is over and not deepening, even if it never returned to the
        # recovery line. A real ongoing draw keeps pressure depressed-and-moving.
        if len(self._pressure_buf) < self.BASELINE_WINDOW_SAMPLES:
            return False
        recent = list(self._pressure_buf)[-self.BASELINE_WINDOW_SAMPLES:]
        if max(recent) - min(recent) > self.SETTLED_STABILITY_PSI:
            return False
        log.info(
            "[%s] closing settled no-flow pressure transient (open %.0fs, flow=0, "
            "pressure stable within %.2f PSI) — phantom; freeing circuit instead "
            "of holding it until the %.0f h watchdog",
            self.circuit, (now - ev.start_ts).total_seconds(),
            self.SETTLED_STABILITY_PSI, self.MAX_EVENT_DURATION_S / 3600.0,
        )
        self._end_event(now, force=True)
        return True

    def _end_event(self, ts: datetime, force: bool = False) -> None:
        ev = self._active_event
        if ev is None:
            return
        # dev.24 low-flow off-grace: a sustained low draw the turbine chatters on
        # must not finalize on a brief sub-threshold dip. Hold the event open
        # until the grace deadline; on_flow_rate clears the hold the instant flow
        # resumes, bridging the dip into one event. force=True (grace expired)
        # skips the hold so the held event actually finalizes.
        if not force and self._should_hold_low_flow(ev, ts):
            if ev.low_flow_hold_until is None:
                ev.low_flow_hold_until = ts + timedelta(seconds=LOWFLOW_OFF_GRACE_S)
                log.debug("[%s] low-flow event held open at dip %s (grace %.0f s)",
                          self.circuit, ts.isoformat(), LOWFLOW_OFF_GRACE_S)
            return
        self._pressure_recovered_since = None

        duration = (ts - ev.start_ts).total_seconds()

        if duration < self.min_event_duration:
            log.debug("[%s] discarding short event (%.1f s < %.1f s)",
                      self.circuit, duration, self.min_event_duration)
            self._active_event = None
            self._flow_sample_count = 0
            self._pressure_sample_count = 0
            return

        ev.end_ts = ts
        # Close the timestamped flow series with an end sample so the final
        # interval to end_ts is integrated at the real (low) end flow. The end
        # condition guarantees flow < MIN_FLOW here, so the tail integrates to ~0.
        ev.flow_samples.append((ts, self._current_flow_lpm))
        # Use `is not None` — pre_event_pressure_psi defaults to 0.0,
        # which is falsy but valid for zero-baseline (unpressurised) systems.
        if ev.pressure_readings:
            ev.min_pressure_psi = min(ev.pressure_readings)
            # Keep pressure_delta_psi from detection time (initial transient magnitude).
            # Only set it here as a fallback when it was never captured at detection.
            if ev.pre_event_pressure_psi is not None and ev.pressure_delta_psi == 0.0:
                ev.pressure_delta_psi = ev.pre_event_pressure_psi - ev.min_pressure_psi
        ev.complete = True
        self._active_event = None
        self._flow_sample_count = 0
        self._pressure_sample_count = 0

        # Discard gate uses the TIME-INTEGRAL of the timestamped flow samples,
        # not mean(flow_readings) × duration (which over-counts brief bursts in
        # long pressure-defined events). The stored volume is recomputed the same
        # way in feature_extractor.
        from .flow_integral import integrate_litres
        volume_l, _capped = integrate_litres(ev.flow_samples)
        # Degenerate-timestamp guard: if every flow sample shares ~one instant
        # (synthetic/test injection — live on-change samples always span the
        # event), fall back to the duration estimate for the DISCARD decision
        # only, so a real event isn't dropped. Stored volume still uses the
        # integral. Inert in production (span is always >> 1s for a real event).
        if (len(ev.flow_samples) >= 2
                and (ev.flow_samples[-1][0] - ev.flow_samples[0][0]).total_seconds() < 1.0
                and ev.flow_readings):
            est = (sum(ev.flow_readings) / len(ev.flow_readings)) * (duration / 60.0)
            volume_l = max(volume_l, est)
        if volume_l < self.MIN_EVENT_VOLUME_L:
            log.debug(
                "[%s] discarding near-zero-volume event (%.5f L < %.3f L)",
                self.circuit, volume_l, self.MIN_EVENT_VOLUME_L,
            )
            return

        # Reject pressure-surge phantoms: turbine artefacts from pump surges or
        # water hammer where pressure rose above baseline and never dropped.
        if (
            ev.pressure_readings
            and ev.pre_event_pressure_psi is not None
            and ev.pre_event_pressure_psi > 0
            and ev.max_pressure_psi > 0
        ):
            # Instance attr (dev25): widened to effectively-off in pump mode —
            # a recharge upswing during a real event is exactly this pattern.
            pressure_rise = ev.max_pressure_psi - ev.pre_event_pressure_psi
            if pressure_rise > self.pressure_surge_phantom_psi and ev.pressure_delta_psi <= 0:
                log.info(
                    "[%s] rejecting pressure-surge phantom: rose %.2f PSI "
                    "(max=%.1f baseline=%.1f delta=%.2f) duration=%.1f s",
                    self.circuit, pressure_rise, ev.max_pressure_psi,
                    ev.pre_event_pressure_psi, ev.pressure_delta_psi, duration,
                )
                self._pressure_recovered_since = None
                return

        log.info(
            "[%s] event complete — trigger=%s duration=%.1f s volume=%.2f L "
            "(%d flow samples) pressure_drop=%.1f PSI has_transient=%s",
            self.circuit, ev.start_trigger, duration, volume_l,
            len(ev.flow_samples), ev.pressure_delta_psi, ev.has_pressure_transient,
        )
        try:
            self._event_queue.put_nowait(ev)
        except asyncio.QueueFull:
            log.warning(
                "[%s] event queue full — dropping event start_ts=%s "
                "(consider increasing queue size or reducing event rate)",
                self.circuit, ev.start_ts,
            )

    def reset(self) -> None:
        """Reset all state — call when valve closes or on explicit reset."""
        self._active_event = None
        self._flow_sample_count = 0
        self._pressure_sample_count = 0
        self._pressure_buf.clear()
        self._pressure_ts_buf.clear()
        self._current_flow_lpm = 0.0
        self._flow_sustained_since = None
        self._pressure_recovered_since = None
        self._settled_pressure_psi = None
        self._settled_pressure_since = None
        self._pretrigger_flow.clear()


# ------------------------------------------------------------------------------- #
# Per-event waveform capture (firmware 3.9.0+) — chunked HA-event accumulator    #
# ------------------------------------------------------------------------------- #
# Each event is delivered as a stream of esphome.water_monitor_waveform_chunk
# events. A chunk fires every time a firmware-side 1500-sample buffer fills
# (~30 s @ ~50 Hz) and also on event end. The accumulator stitches chunks
# keyed on (boot_id, event_id) and emits a WaveformRecord when the final
# chunk arrives and all expected seqs are present. Native cadence is
# preserved; arrays are variable-length — feature_extractor.py is fine with
# variable lengths already.

# Wire-format / safety constants.
_WF_START_SAMPLES: int = 150            # first slice of full_flow used as start_flow (3 s @ ~50 Hz)
_WF_MAX_RECORDS: int = 30               # max assembled records kept per circuit
_WF_FLAG_VALID_MASK: int = 0x7F         # bits 0-6 only; bit 7 must be 0
_WF_INFLIGHT_TTL_S: float = 7200.0      # 2 h — absolute upper bound on supported event length
# Phase 3 — once the FINAL chunk has arrived (so `total` is known), a set still
# missing chunks is a transport GAP: give up after this grace (rather than holding
# memory for the full 2 h TTL) and count it. Generous enough for a delayed chunk.
_WF_FINAL_GAP_TIMEOUT_S: float = 120.0
_WF_FL_RESOLUTION_REDUCED: int = 0x04   # firmware dropped/decimated samples (a hole)
_WF_MAX_CHUNK_SAMPLES: int = 1500       # firmware buffer cap; bound decoded-payload length defensively
_WF_MAX_TOTAL_CHUNKS: int = 600         # bound for the 'total' field (~5 hour event at 30s cadence)


@dataclass
class WaveformMetadata:
    """Per-event waveform metadata, populated from the final chunk."""
    event_id: int           # id  — monotonic per-boot event counter
    boot_id: int            # b   — ESP session id
    start_ms: int           # event_s — event-wide start millis()
    end_ms: int             # event_e — event-wide end millis()
    start_points: int       # sn — len(start_flow) (derived: min(_WF_START_SAMPLES, full))
    full_points: int        # fn — len(full_flow)  (sum of chunk samples)
    flow_scale: int         # int16 → L/min divisor (constant 100 today)
    pressure_scale: int     # int16 → PSI divisor (constant 100 today)
    peak_flow: float        # pk — peak flow (×100 in wire, /100 here)
    pressure_delta: float   # dp — pressure delta (×100 in wire, /100 here)
    propagation_delay_ms: int   # pd — onset propagation delay (ms; -1 = not detected)
    quality: int            # q  — 0 ok, 1 incomplete, firmware never publishes 2-6
    flags: int              # fl — bitfield (see plan)
    # Onset position. ``onset_seq`` and ``onset_idx`` are the source chunk
    # and within-chunk sample index as reported by firmware; ``onset_index``
    # is the linear index into the concatenated full_flow/full_pressure and
    # is resolved by the accumulator once chunk lengths are known.
    #
    # Default to -1 (not 0) so a record constructed without onset fields —
    # malformed parse, future schema change, hand-built test fixture —
    # cannot silently look like "onset at sample 0" and have downstream
    # feature_extractor treat the pre-roll as the post-onset ramp. _assemble
    # also validates that (onset_seq, onset_idx) is in bounds for the
    # received chunks and writes -1 if not.
    onset_seq: int = -1
    onset_idx: int = -1
    onset_index: int = -1
    pressure_onset_seq: int = -1
    pressure_onset_idx: int = -1
    pressure_onset_index: int = -1
    # Legacy fields kept at zero for compat — pre/post/tail no longer meaningful.
    pre_ms: int = 0
    post_ms: int = 0
    tail_ms: int = 0
    version: int = 1


@dataclass
class WaveformRecord:
    """Fully assembled and decoded per-event waveform — ready for feature extraction."""
    circuit: str
    boot_id: int
    event_id: int
    metadata: WaveformMetadata
    # Variable-length lists — feature_extractor.py guards each access with `if x:`.
    start_flow: List[float]       # L/min — first _WF_START_SAMPLES of full_flow
    start_pressure: List[float]   # PSI
    full_flow: List[float]        # L/min — concatenated across all chunks
    full_pressure: List[float]    # PSI
    received_at: float            # time.monotonic() when the final chunk assembled


@dataclass
class _InflightChunkSet:
    """Per-event scratchpad: chunks seen, final metadata once it arrives, TTL stamp."""
    chunks: Dict[int, Tuple[List[float], List[float]]] = field(default_factory=dict)
    total: Optional[int] = None
    final_metadata: Optional[WaveformMetadata] = None
    first_received_at: float = 0.0
    # Track which (seq) we've already DEBUG-logged a duplicate for, so the
    # log doesn't spam if a chunk arrives 3+ times.
    duplicate_logged: set = field(default_factory=set)


def _parse_wire_bool(value: Any) -> bool:
    """Parse a wire-format boolean tolerantly.

    HA may pass back the literal string from the template, or coerce to a
    native bool. Accept "true"/"false" (any case) and native True/False;
    raise ValueError on anything else so the caller can DEBUG-log + reject.
    """
    if value is True:
        return True
    if value is False:
        return False
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    raise ValueError(f"invalid wire bool: {value!r}")


def _normalize_int_field(d: dict, key: str) -> Optional[int]:
    """Get d[key] and normalize to int. Returns None on missing/invalid.

    Accepts str or native int; rejects bool (Python bool is int subclass).
    Logs DEBUG with the rejection reason; the caller just checks for None.
    """
    raw = d.get(key)
    if raw is None:
        log.debug("waveform: chunk missing field %r", key)
        return None
    if isinstance(raw, bool):
        log.debug("waveform: chunk field %r has unexpected bool value", key)
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.debug("waveform: chunk field %r not castable to int: %.40r", key, raw)
        return None


def _parse_chunk_scalars(data: dict) -> Optional[dict]:
    """Parse and range-check the per-chunk scalar fields. Returns a dict of
    normalized ints (b, id, seq, s, e, fs, ps) or None on failure."""
    out: dict = {}
    for key in ("b", "id", "seq", "s", "e", "fs", "ps"):
        v = _normalize_int_field(data, key)
        if v is None:
            return None
        out[key] = v
    if out["b"] < 0 or out["id"] < 0 or out["seq"] < 0:
        log.debug("waveform: chunk negative b/id/seq")
        return None
    if out["fs"] <= 0 or out["ps"] <= 0:
        log.debug("waveform: chunk fs/ps must be positive")
        return None
    return out


def _parse_final_metadata(
    data: dict, circuit: str, full_len: int, start_len: int,
) -> Optional[Tuple[WaveformMetadata, int]]:
    """Build a WaveformMetadata from the final chunk's extra fields.

    `full_len` is the total sample count after concatenation; `start_len` is
    the prefix slice length (min of _WF_START_SAMPLES and full_len).

    Returns ``(metadata, total)`` so the caller does not re-parse ``total``
    (a defensive re-parse risks silently substituting 0 and assembling an
    incomplete record).
    """
    b = _normalize_int_field(data, "b")
    eid = _normalize_int_field(data, "id")
    evs = _normalize_int_field(data, "event_s")
    eve = _normalize_int_field(data, "event_e")
    total = _normalize_int_field(data, "total")
    onset_seq = _normalize_int_field(data, "onset_seq")
    onset_idx = _normalize_int_field(data, "onset_idx")
    press_onset_seq = _normalize_int_field(data, "press_onset_seq")
    press_onset_idx = _normalize_int_field(data, "press_onset_idx")
    pk = _normalize_int_field(data, "pk")
    dp = _normalize_int_field(data, "dp")
    pd = _normalize_int_field(data, "pd")
    q = _normalize_int_field(data, "q")
    fl = _normalize_int_field(data, "fl")
    fs = _normalize_int_field(data, "fs")
    ps = _normalize_int_field(data, "ps")
    if None in (b, eid, evs, eve, total, onset_seq, onset_idx,
                pk, dp, pd, q, fl, fs, ps,
                press_onset_seq, press_onset_idx):
        return None
    if total <= 0 or total > _WF_MAX_TOTAL_CHUNKS:
        log.debug("waveform[%s]: chunk total=%d out of range", circuit, total)
        return None
    if not (0 <= q <= 6):
        log.debug("waveform[%s]: final q=%d out of range", circuit, q)
        return None
    if fl & ~_WF_FLAG_VALID_MASK:
        log.debug("waveform[%s]: final fl=%d has invalid high bits", circuit, fl)
        return None
    # Onset position is reported here in (seq, idx) form; the linear index
    # into the concatenated full array is resolved by the accumulator once
    # all chunk lengths are known (`_assemble`).
    meta = WaveformMetadata(
        event_id=eid,
        boot_id=b,
        start_ms=evs,
        end_ms=eve,
        start_points=start_len,
        full_points=full_len,
        flow_scale=fs,
        pressure_scale=ps,
        peak_flow=pk / 100.0,
        pressure_delta=dp / 100.0,
        propagation_delay_ms=pd,
        quality=q,
        flags=fl,
        onset_seq=onset_seq,
        onset_idx=onset_idx,
        pressure_onset_seq=press_onset_seq,
        pressure_onset_idx=press_onset_idx if press_onset_seq >= 0 else -1,
    )
    return meta, total


def _decode_waveform(
    b64_payload: str,
    scale: int,
    expected_pts: Optional[int] = None,
) -> Optional[List[float]]:
    """Decode a base64-encoded little-endian int16 waveform payload.

    Returns the decoded floats, or None when malformed. If ``expected_pts``
    is given, the byte length must match exactly; if None, any whole int16
    count is accepted (used by the chunk path, where chunk length varies).
    """
    if not b64_payload:
        log.debug("waveform: empty base64 payload")
        return None
    try:
        raw = base64.b64decode(b64_payload, validate=True)
    except Exception:
        log.debug("waveform: base64 decode error for payload %.40r", b64_payload)
        return None
    if len(raw) % 2 != 0:
        log.debug("waveform: payload length %d not a whole int16 count", len(raw))
        return None
    if expected_pts is not None and len(raw) != expected_pts * 2:
        log.debug(
            "waveform: payload length %d != expected %d bytes (%d pts × 2)",
            len(raw), expected_pts * 2, expected_pts,
        )
        return None
    pts = len(raw) // 2
    if pts > _WF_MAX_CHUNK_SAMPLES:
        log.debug(
            "waveform: payload %d samples exceeds max chunk size %d",
            pts, _WF_MAX_CHUNK_SAMPLES,
        )
        return None
    values: List[float] = []
    for i in range(pts):
        (v,) = struct.unpack_from("<h", raw, i * 2)
        values.append(v / scale)
    return values


def _normalize_node_name(name: str) -> str:
    """Normalize an ESPHome node name for identity comparison.

    App.get_name() may use hyphens; HA entity prefixes use underscores.
    Both sides of the comparison must be normalized the same way.
    """
    return name.strip().lower().replace("-", "_")


class WaveformChunkAccumulator:
    """
    Per-circuit accumulator for chunked waveform delivery (firmware 3.9.0+).

    The firmware streams `esphome.water_monitor_waveform_chunk` events keyed
    by (boot_id, event_id, seq). Each non-final chunk carries a slice of the
    flow + pressure waveform; the final chunk carries the last slice (which
    may be empty) plus event-wide metadata + the expected `total` chunk count.

    Lifecycle per event:
      1. Chunks arrive in any order; held in an in-flight set per (boot, id).
      2. Final chunk sets `total` and the WaveformMetadata.
      3. When all seqs 0..total-1 are present, the accumulator concatenates
         them and emits a WaveformRecord. The in-flight entry is removed.
      4. Missing seqs at final time → DEBUG log, no record (a delayed chunk
         can still complete the set later until TTL eviction).

    **Durability:** chunk accumulation is in-memory only. An add-on restart
    during an active event drops in-flight chunks for that event; the final
    chunk arriving after restart will be missing predecessors and won't
    assemble. EventDetector continues normal event detection without
    waveform enrichment in that specific case — no persistence layer.

    Exposes get_record / latest_record / pop_record so EventDetector's
    public accessors are a thin pass-through (see EventDetector below).
    """

    def __init__(self, circuit: str, expected_node: str,
                 on_record_assembled: Optional[Callable[[WaveformRecord], None]] = None) -> None:
        self._circuit = circuit
        # Normalize once so every comparison is cheap.
        self._expected_node = _normalize_node_name(expected_node)
        self._inflight: Dict[Tuple[int, int], _InflightChunkSet] = {}
        self._records: List[WaveformRecord] = []
        # Phase 3 transport-health counters (since boot/restart): total assembled,
        # assembled-but-firmware-flagged-degraded (quality / resolution-reduced), and
        # transport GAPS (final arrived but chunks lost → waveform discarded).
        self._n_assembled = 0
        self._n_degraded = 0
        self._n_gaps = 0
        # Optional sink fired once a record finishes assembling (late-waveform
        # upgrade, Fix 1). Kept DB-free; invoked in a try/except in _assemble so a
        # sink bug can never corrupt assembly. None in most tests.
        self._on_record_assembled = on_record_assembled

    # ------------------------------------------------------------------
    # Public callback
    # ------------------------------------------------------------------

    def on_waveform_chunk(self, data: dict) -> None:
        """Process a single esphome.water_monitor_waveform_chunk event."""
        now = time.monotonic()
        self._evict_stale(now)

        # Identity & schema guards.
        if data.get("schema") != "esp_water_monitor_waveform_chunk":
            log.debug("waveform[%s]: chunk rejected — unexpected schema %r",
                      self._circuit, data.get("schema"))
            return
        if str(data.get("transport_version", "")) != "1":
            log.debug("waveform[%s]: chunk rejected — unsupported transport_version %r",
                      self._circuit, data.get("transport_version"))
            return
        node = _normalize_node_name(data.get("node", ""))
        if self._expected_node and node != self._expected_node:
            log.debug("waveform[%s]: chunk rejected — node %r != expected %r",
                      self._circuit, node, self._expected_node)
            return
        if data.get("circuit") != self._circuit:
            return  # silently skip other circuits

        # Scalars.
        sc = _parse_chunk_scalars(data)
        if sc is None:
            return
        try:
            is_final = _parse_wire_bool(data.get("final", "false"))
        except ValueError:
            log.debug("waveform[%s]: chunk rejected — invalid 'final' value %r",
                      self._circuit, data.get("final"))
            return

        # Decode flow/press payloads. On a final chunk an empty payload is
        # explicitly allowed (firmware flushed the last sample in a prior
        # chunk); on a non-final chunk it's a wire-format error.
        flow_b64 = data.get("flow", "")
        press_b64 = data.get("press", "")
        if is_final and flow_b64 == "" and press_b64 == "":
            flow: List[float] = []
            press: List[float] = []
        else:
            decoded_flow = _decode_waveform(flow_b64, sc["fs"])
            decoded_press = _decode_waveform(press_b64, sc["ps"])
            if decoded_flow is None or decoded_press is None:
                log.debug("waveform[%s]: chunk seq=%d rejected — payload decode failed",
                          self._circuit, sc["seq"])
                return
            if len(decoded_flow) != len(decoded_press):
                log.debug(
                    "waveform[%s]: chunk seq=%d rejected — flow/press length mismatch (%d vs %d)",
                    self._circuit, sc["seq"], len(decoded_flow), len(decoded_press),
                )
                return
            flow, press = decoded_flow, decoded_press

        key = (sc["b"], sc["id"])
        cs = self._inflight.get(key)
        if cs is None:
            cs = _InflightChunkSet(first_received_at=now)
            self._inflight[key] = cs

        seq = sc["seq"]

        # Reject seq values that exceed any already-known total (catches
        # corrupted/duplicate-event_id misroutes).
        if cs.total is not None and seq >= cs.total:
            log.debug(
                "waveform[%s]: chunk seq=%d rejected — exceeds known total=%d (boot=%d id=%d)",
                self._circuit, seq, cs.total, sc["b"], sc["id"],
            )
            return

        # Duplicate handling.
        if seq in cs.chunks:
            prev_flow, _prev_press = cs.chunks[seq]
            if len(prev_flow) == len(flow):
                if seq not in cs.duplicate_logged:
                    log.debug(
                        "waveform[%s]: duplicate chunk seq=%d for boot=%d id=%d — same length, replacing",
                        self._circuit, seq, sc["b"], sc["id"],
                    )
                    cs.duplicate_logged.add(seq)
                cs.chunks[seq] = (flow, press)
            else:
                log.debug(
                    "waveform[%s]: duplicate chunk seq=%d for boot=%d id=%d — length mismatch (%d vs %d), rejecting",
                    self._circuit, seq, sc["b"], sc["id"], len(prev_flow), len(flow),
                )
                return
        else:
            cs.chunks[seq] = (flow, press)

        # On final, capture metadata + total and try to assemble.
        if is_final:
            # Compute provisional full length to feed metadata builder.
            chunks_in_order = sorted(cs.chunks.items())
            full_len = sum(len(f) for _, (f, _p) in chunks_in_order)
            start_len = min(_WF_START_SAMPLES, full_len)
            parsed = _parse_final_metadata(data, self._circuit, full_len, start_len)
            if parsed is None:
                # Don't pop the in-flight entry — TTL will GC it. The DEBUG
                # log already explained why.
                return
            meta, total = parsed
            # Use the already-validated total from _parse_final_metadata
            # rather than re-parsing the wire field (a re-parse failure
            # would silently substitute 0 and assemble an empty record).
            cs.total = total
            cs.final_metadata = meta

        # Try to assemble if we have a known total and all seqs are present.
        if cs.total is not None and cs.final_metadata is not None:
            missing = [s for s in range(cs.total) if s not in cs.chunks]
            if missing:
                log.debug(
                    "waveform[%s]: cannot assemble event %d yet — missing %d/%d chunk(s): seqs %s",
                    self._circuit, sc["id"], len(missing), cs.total, missing,
                )
                return
            self._assemble(key, cs)

    # ------------------------------------------------------------------
    # Assembly
    # ------------------------------------------------------------------

    def _assemble(self, key: Tuple[int, int], cs: _InflightChunkSet) -> None:
        meta = cs.final_metadata
        assert meta is not None
        chunks_in_order = sorted(cs.chunks.items())

        full_flow: List[float] = []
        full_press: List[float] = []
        # Resolve the (onset_seq, onset_idx) pair to a linear index into the
        # concatenated full_flow / full_pressure arrays by summing the lengths
        # of all chunks strictly before onset_seq. This is robust to a future
        # firmware that extends the pre-roll across multiple chunks or fires
        # an event without flow pre-roll.
        prefix_len: Dict[int, int] = {}
        chunk_lengths: Dict[int, int] = {}
        running = 0
        for seq, (flow, press) in chunks_in_order:
            prefix_len[seq] = running
            chunk_lengths[seq] = len(flow)
            full_flow.extend(flow)
            full_press.extend(press)
            running += len(flow)

        def _linear(seq: int, idx: int) -> int:
            """Resolve (seq, idx) to a linear index into full_flow/full_press.

            Returns -1 when:
              - seq < 0 (firmware reported "no onset")
              - seq isn't among the received chunks (corrupt metadata)
              - idx isn't a valid position inside that chunk

            Downstream feature_extractor treats -1 as "use legacy onset
            detection from the waveform itself" rather than trusting a
            silently-clamped value (the old behaviour clamped any negative
            idx to 0, which made a missing-onset record look like onset
            lived at sample 0 — wrong for any event with a pre-roll).
            """
            if seq < 0:
                return -1
            if seq not in chunk_lengths:
                return -1
            if not (0 <= idx < chunk_lengths[seq]):
                return -1
            return prefix_len[seq] + idx

        meta.onset_index = _linear(meta.onset_seq, meta.onset_idx)
        meta.pressure_onset_index = _linear(
            meta.pressure_onset_seq, meta.pressure_onset_idx)

        start_flow = full_flow[:_WF_START_SAMPLES]
        start_press = full_press[:_WF_START_SAMPLES]

        # Update lengths in metadata after concat in case _parse_final_metadata
        # under-counted (shouldn't happen but defensive).
        meta.full_points = len(full_flow)
        meta.start_points = len(start_flow)

        # feature_extractor derives per-sample dt from (pre_ms + post_ms) /
        # start_points. The firmware streams at native ~50 Hz cadence (20 ms
        # per sample); populate pre_ms / post_ms so dt resolves to 0.020 s
        # and the onset position is recoverable. When the resolved onset lies
        # within the start window we expose it via pre_ms/post_ms; otherwise
        # we just describe the start window's full extent and downstream code
        # falls back to legacy onset detection from the waveform itself.
        _SAMPLE_MS = 20
        if meta.start_points > 0:
            if 0 <= meta.onset_index < meta.start_points:
                onset_in_start = meta.onset_index
                meta.pre_ms = onset_in_start * _SAMPLE_MS
                meta.post_ms = (meta.start_points - onset_in_start) * _SAMPLE_MS
            else:
                # Onset is either unknown (onset_index == -1 after the
                # bounds-check above) or lies outside the start window.
                # Describe the window's full extent only; feature_extractor
                # falls back to detecting onset from the waveform itself.
                meta.pre_ms = 0
                meta.post_ms = meta.start_points * _SAMPLE_MS
        # tail_ms = 0: chunked records do not capture a post-event recovery
        # tail. The full waveform spans onset → event end only. Setting this
        # to anything else feeds the wrong "tail" into feature_extractor's
        # recovery_overshoot_psi / steady_state_fraction full-window math.
        # When tail_ms is 0, those blocks fall through and the legacy
        # _pressure_shape_features value survives (which is correct — it
        # measures over the full pressure_readings window).
        meta.tail_ms = 0

        record = WaveformRecord(
            circuit=self._circuit,
            boot_id=meta.boot_id,
            event_id=meta.event_id,
            metadata=meta,
            start_flow=start_flow,
            start_pressure=start_press,
            full_flow=full_flow,
            full_pressure=full_press,
            received_at=time.monotonic(),
        )
        self._records.append(record)
        if len(self._records) > _WF_MAX_RECORDS:
            self._records = self._records[-_WF_MAX_RECORDS:]
        # Phase 3 — count it; flag if the firmware self-reported a degraded capture
        # (these still assemble, but their SIGNATURE is gated in _enrich_from_waveform).
        self._n_assembled += 1
        if meta.quality != 0 or (meta.flags & _WF_FL_RESOLUTION_REDUCED):
            self._n_degraded += 1

        # Done — remove the in-flight entry so late stray chunks for this
        # event_id don't keep growing memory.
        self._inflight.pop(key, None)

        log.debug(
            "waveform[%s]: assembled event %d from %d chunk(s) "
            "(boot=%d full=%d start=%d q=%d fl=0x%02x pk=%.2f dp=%.2f pd=%dms)",
            self._circuit, meta.event_id, cs.total, meta.boot_id,
            meta.full_points, meta.start_points,
            meta.quality, meta.flags,
            meta.peak_flow, meta.pressure_delta, meta.propagation_delay_ms,
        )

        # Notify the optional sink (late-waveform upgrade). Wrapped so a sink
        # error can never corrupt assembly or escape the WS callback.
        if self._on_record_assembled is not None:
            try:
                self._on_record_assembled(record)
            except Exception as e:
                log.warning("waveform[%s]: on_record_assembled sink raised "
                            "(non-fatal): %s", self._circuit, e)

    # ------------------------------------------------------------------
    # TTL eviction
    # ------------------------------------------------------------------

    def _evict_stale(self, now: float) -> None:
        # A set whose FINAL chunk has arrived (total known) but is still incomplete
        # is a transport GAP — evict it after a short grace and COUNT it. A set still
        # awaiting its final chunk uses the long TTL (the event may still be running).
        stale: List[Tuple[Tuple[int, int], bool]] = []
        for k, cs in self._inflight.items():
            final_incomplete = cs.final_metadata is not None
            ttl = _WF_FINAL_GAP_TIMEOUT_S if final_incomplete else _WF_INFLIGHT_TTL_S
            if (now - cs.first_received_at) > ttl:
                stale.append((k, final_incomplete))
        for k, final_incomplete in stale:
            cs = self._inflight.pop(k, None)
            if cs is None:
                continue
            if final_incomplete:
                self._n_gaps += 1
                missing = ([s for s in range(cs.total) if s not in cs.chunks]
                           if cs.total else [])
                log.info(
                    "waveform[%s]: GAP — event %d lost %d/%s chunk(s) in transport "
                    "(missing seqs %s); waveform discarded",
                    self._circuit, k[1], len(missing), cs.total, missing,
                )
            else:
                log.debug(
                    "waveform[%s]: in-flight chunk set evicted (boot=%d id=%d, %d chunk(s), total=%s)",
                    self._circuit, k[0], k[1], len(cs.chunks), cs.total,
                )

    def transport_stats(self) -> Dict[str, int]:
        """Phase 3 waveform-transport health since boot: how many waveforms
        assembled, how many were firmware-flagged degraded, and how many were lost
        to transport gaps (a final chunk arrived but predecessors never did)."""
        return {"assembled": self._n_assembled,
                "degraded": self._n_degraded,
                "gaps": self._n_gaps}

    # ------------------------------------------------------------------
    # Lookup interface
    # ------------------------------------------------------------------

    def get_record(self, boot_id: int, event_id: int) -> Optional[WaveformRecord]:
        for rec in reversed(self._records):
            if rec.boot_id == boot_id and rec.event_id == event_id:
                return rec
        return None

    def latest_record(self) -> Optional[WaveformRecord]:
        return self._records[-1] if self._records else None

    def recent_records(self) -> List[WaveformRecord]:
        """All buffered records, newest last. The caller filters by recency/
        overlap — a burst of short ESP captures (e.g. washer fill pauses
        splitting one add-on event into several firmware events) means the
        newest record is often NOT the one that matches."""
        return list(self._records)

    def pop_record(self, boot_id: int, event_id: int) -> Optional[WaveformRecord]:
        for i in range(len(self._records) - 1, -1, -1):
            if self._records[i].boot_id == boot_id and self._records[i].event_id == event_id:
                return self._records.pop(i)
        return None


class EventDetector:
    """
    Top-level coordinator. Owns one CircuitEventDetector per circuit
    and wires their callbacks to the HaClient subscriptions.
    """

    def __init__(
        self,
        circuits: List[Any],
        ha_client: Any,
        event_queue: asyncio.Queue,
        sensitivity_getter: Callable[[str], dict],
        debug_capture_propagation: bool = False,
        pump_gate_getter: Optional[Callable[[str], Optional[float]]] = None,
        low_pressure_getter: Optional[Callable[[str], tuple]] = None,
        low_pressure_cb: Optional[Callable[[str, float], None]] = None,
        pump_fail_cb: Optional[Callable[[str, float, str], None]] = None,
    ) -> None:
        self._circuits = circuits
        self._ha = ha_client
        self._queue = event_queue
        self._sensitivity_getter = sensitivity_getter
        # dev25: returns the pump-mode oscillation gate (PSI) for a circuit,
        # or None when pump-aware suppression is off. Wired by the
        # orchestrator; None default keeps tests/imports gate-free.
        self._pump_gate_getter = pump_gate_getter or (lambda _c: None)
        # dev27 (Phase 6): returns (zone_floor_psi|None, pump_fail_floor|None)
        # per circuit; the callbacks fire the alerts (orchestrator wires them
        # to AlertManager via create_task).
        self._low_pressure_getter = low_pressure_getter or (
            lambda _c: (None, None))
        self._low_pressure_cb = low_pressure_cb
        self._pump_fail_cb = pump_fail_cb
        self._debug_capture_propagation = debug_capture_propagation
        self._detectors: Dict[str, CircuitEventDetector] = {}
        # Tracks live valve open/closed state per circuit for cross-circuit feature
        self._valve_open: Dict[str, bool] = {}
        # Chunked HA-event waveform accumulators (firmware 3.9.0+) — sole transport.
        self._chunk_accumulators: Dict[str, WaveformChunkAccumulator] = {}
        self._wf_event_subscribed: bool = False
        self._is_configured = False
        # Optional sink for late-assembled waveforms (signature upgrade, Fix 1).
        # Wired by the orchestrator to FeatureExtractor.handle_late_waveform;
        # left None in tests / import → assembly notifies nothing.
        self._waveform_upgrade_sink: Optional[Callable[[str, WaveformRecord], None]] = None

    def min_flow_for(self, circuit: str) -> float:
        """Per-circuit meter-derived low-flow floor (60 ÷ ppl). Falls back to the
        396-ppl turbine default if the circuit's detector isn't built yet."""
        det = self._detectors.get(circuit)
        return det.MIN_FLOW_LPM if det is not None else 0.15

    def set_min_flow(self, circuit: str, min_flow_lpm: float) -> None:
        """Update a circuit detector's low-flow floor live (after a PPL change)."""
        det = self._detectors.get(circuit)
        if det is not None:
            det.update_min_flow(min_flow_lpm)

    def settled_pressure(self, circuit: str) -> Optional[Tuple[float, datetime]]:
        """Idle-line settled pressure for one circuit — see
        CircuitEventDetector.settled_pressure. None when the circuit's
        detector isn't built or has no trustworthy baseline."""
        det = self._detectors.get(circuit)
        return det.settled_pressure() if det is not None else None

    def setup(self) -> None:
        """Instantiate detectors and register HA entity subscriptions.

        Idempotent — safe to call more than once (e.g. after the setup
        wizard completes on an already-running system).  The second call
        is a no-op so duplicate HA subscriptions are never registered.
        """
        if self._is_configured:
            log.debug("Event detector already configured — skipping re-setup")
            return
        self._is_configured = True
        for cfg in self._circuits:
            sens = self._sensitivity_getter(cfg.circuit)
            detector = CircuitEventDetector(
                circuit=cfg.circuit,
                pressure_drop_threshold_psi=sens.get("pressure_drop_event_psi", 1.2),
                min_event_duration_seconds=sens.get("min_event_duration_seconds", 3.0),
                event_queue=self._queue,
                get_other_valve_open=(
                    lambda c=cfg.circuit: self._get_other_valve_open(c)
                ),
                flow_onset_entity=cfg.flow_onset_sensor,
                debug_capture_propagation=self._debug_capture_propagation,
                min_flow_lpm=getattr(cfg, "min_flow_lpm", 0.15),
            )
            self._detectors[cfg.circuit] = detector
            try:
                detector.update_pump_gate(self._pump_gate_getter(cfg.circuit))
            except Exception as e:
                log.warning("[%s] pump-gate resolve failed (non-fatal): %s",
                            cfg.circuit, e)
            detector.low_pressure_cb = self._low_pressure_cb
            detector.pump_fail_cb = self._pump_fail_cb
            try:
                zf, pf = self._low_pressure_getter(cfg.circuit)
                detector.update_low_pressure_config(zf, pf)
            except Exception as e:
                log.warning("[%s] low-pressure resolve failed (non-fatal): %s",
                            cfg.circuit, e)

            if cfg.flow_sensor:
                self._ha.subscribe_entity(cfg.flow_sensor,          detector.on_flow_rate)
            if cfg.pressure_fast_sensor:
                self._ha.subscribe_entity(cfg.pressure_fast_sensor, detector.on_pressure_fast)
            if cfg.flow_onset_sensor:
                self._ha.subscribe_entity(cfg.flow_onset_sensor,    detector.on_flow_onset)
            # Track valve states so we can record other-circuit valve open at event start
            if cfg.valve_entity:
                self._ha.subscribe_entity(
                    cfg.valve_entity,
                    lambda eid, state, attrs, c=cfg.circuit: self._on_valve_state(c, state),
                )

            # Per-event waveform capture (firmware 3.9.0+, chunked streaming).
            # esp_device_prefix is e.g. "esp_water_main_"; strip the trailing
            # underscore to get the normalized node name for identity comparison.
            # removesuffix("_") strips exactly one trailing underscore;
            # rstrip("_") was over-eager and would strip multiple if a
            # prefix were ever configured with a double-underscore tail.
            expected_node = cfg.esp_device_prefix.removesuffix("_")
            accumulator = WaveformChunkAccumulator(
                cfg.circuit, expected_node=expected_node,
                on_record_assembled=self._on_waveform_assembled,
            )
            self._chunk_accumulators[cfg.circuit] = accumulator

            # Register the HA event subscription once (shared across all circuits).
            if not self._wf_event_subscribed:
                self._ha.subscribe_event(
                    "esphome.water_monitor_waveform_chunk",
                    self._on_waveform_chunk,
                )
                self._wf_event_subscribed = True

            log.info(
                "[%s] event detector ready — triggers: "
                "flow (>= %.2f L/min for %.1f s) | "
                "pressure (>= %.1f PSI drop) | combined",
                cfg.circuit,
                detector.MIN_FLOW_LPM,
                detector.FLOW_START_SECONDS,
                sens.get("pressure_drop_event_psi", 1.2),
            )

        log.info(
            "propagation-delay capture: %s",
            "ENABLED — flow events emit PROPAGATION_CAPTURE blobs"
            if self._debug_capture_propagation else "disabled",
        )

    def update_thresholds(self) -> None:
        """Reload thresholds from config after sensitivity settings change
        (also re-resolves the pump-mode oscillation gate — the banner-confirm
        route calls this so pump suppression flips without a restart)."""
        for circuit, detector in self._detectors.items():
            sens = self._sensitivity_getter(circuit)
            detector.update_threshold(sens.get("pressure_drop_event_psi", 1.2))
            detector.min_event_duration = sens.get("min_event_duration_seconds", 3.0)
            try:
                detector.update_pump_gate(self._pump_gate_getter(circuit))
            except Exception as e:
                log.warning("[%s] pump-gate resolve failed (non-fatal): %s",
                            circuit, e)
            try:
                zf, pf = self._low_pressure_getter(circuit)
                detector.update_low_pressure_config(zf, pf)
            except Exception as e:
                log.warning("[%s] low-pressure resolve failed (non-fatal): %s",
                            circuit, e)

    def _on_waveform_chunk(self, data: dict) -> None:
        """Route an esphome.water_monitor_waveform_chunk event to the correct circuit."""
        circuit = data.get("circuit", "")
        accumulator = self._chunk_accumulators.get(circuit)
        if accumulator is not None:
            accumulator.on_waveform_chunk(data)
        # Wrong or missing circuit is handled silently by the accumulator itself.

    def _on_waveform_assembled(self, record: WaveformRecord) -> None:
        """Forward a freshly-assembled waveform to the late-upgrade sink (if wired).
        Runs on the event loop (WS callback); the sink schedules its own background
        write. Wrapped so a sink error can't escape into assembly."""
        sink = self._waveform_upgrade_sink
        if sink is None:
            return
        try:
            sink(record.circuit, record)
        except Exception as e:
            log.warning("[%s] late-waveform sink raised (non-fatal): %s",
                        record.circuit, e)

    def _on_valve_state(self, circuit: str, state: str) -> None:
        """Update tracked valve state for cross-circuit feature."""
        self._valve_open[circuit] = state in ("open", "on")

    def _get_other_valve_open(self, this_circuit: str) -> Optional[bool]:
        """Return True if any other circuit's valve is currently open, False if all
        are closed, or None if no other valve states have been received yet."""
        others = {c: v for c, v in self._valve_open.items() if c != this_circuit}
        if not others:
            return None   # not yet observed
        return any(others.values())

    def reset_circuit(self, circuit: str) -> None:
        """Reset a single circuit (e.g. after valve close)."""
        if circuit in self._detectors:
            self._detectors[circuit].reset()

    def get_active_event(self, circuit: str) -> Optional[RawEvent]:
        detector = self._detectors.get(circuit)
        return detector._active_event if detector else None

    def get_waveform_record(
        self,
        circuit: str,
        boot_id: int,
        event_id: int,
    ) -> Optional[WaveformRecord]:
        """Return the assembled WaveformRecord for (boot_id, event_id), or None."""
        accumulator = self._chunk_accumulators.get(circuit)
        return accumulator.get_record(boot_id, event_id) if accumulator else None

    def get_latest_waveform(self, circuit: str) -> Optional[WaveformRecord]:
        """Return the most-recently assembled WaveformRecord for a circuit, or None."""
        accumulator = self._chunk_accumulators.get(circuit)
        return accumulator.latest_record() if accumulator else None

    def get_recent_waveforms(self, circuit: str) -> "List[WaveformRecord]":
        """Return all buffered WaveformRecords for a circuit, newest last."""
        accumulator = self._chunk_accumulators.get(circuit)
        return accumulator.recent_records() if accumulator else []

    def waveform_transport_stats(self, circuit: str) -> Dict[str, int]:
        """Phase 3 — per-circuit waveform-transport health counters (or zeros)."""
        accumulator = self._chunk_accumulators.get(circuit)
        if accumulator is None:
            return {"assembled": 0, "degraded": 0, "gaps": 0}
        return accumulator.transport_stats()

    def pop_waveform_record(
        self,
        circuit: str,
        boot_id: int,
        event_id: int,
    ) -> Optional[WaveformRecord]:
        """Remove and return the WaveformRecord for (boot_id, event_id), or None."""
        accumulator = self._chunk_accumulators.get(circuit)
        return accumulator.pop_record(boot_id, event_id) if accumulator else None
