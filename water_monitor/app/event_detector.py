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

    # True if any other circuit's valve was open when this event started.
    # Helps distinguish main-circuit irrigation bleed-through from household demand.
    other_valve_open: Optional[bool] = None

    # Measured volume from the firmware's cumulative integration sensor.
    # Set by the historical importer when the volume_sensor entity is available.
    # Preferred over the flow-average approximation in feature extraction.
    volume_litres_measured: Optional[float] = None

    is_composite: bool = False
    complete: bool = False


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

    # Minimum flow rate considered real flow (filters ADC noise)
    MIN_FLOW_LPM: float = 0.15

    # Maximum physically plausible flow rate for any residential/light-commercial
    # system.  Readings above this are treated as sensor overflow / firmware error
    # values (e.g. 1.58e+36 L/min from ESP ADC overflow) and clamped to 0.0.
    # 1000 L/min ≈ 264 gal/min — well beyond any domestic water supply.
    MAX_FLOW_LPM: float = 200.0   # matches firmware v3.5 ADC overflow clamp ceiling

    # Minimum physically meaningful flow rate from this sensor.
    # The pulse counter cannot produce a non-zero value below 1 pulse/second,
    # which converts to 60 counts/min / 396 ≈ 0.15 L/min.  Values in the
    # range (0, MIN_NOISE_LPM) are floating-point noise (e.g. 1.58e-36 L/min
    # from ESPHome ADC underflow) and should be treated as zero.
    MIN_NOISE_LPM: float = 0.05

    # Seconds of sustained flow required to open a flow-triggered event
    FLOW_START_SECONDS: float = 2.0

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
    ) -> None:
        self.circuit = circuit
        self.pressure_drop_threshold = pressure_drop_threshold_psi
        self.min_event_duration = min_event_duration_seconds
        self._event_queue = event_queue
        self._flow_onset_entity: Optional[str] = flow_onset_entity
        # Callable provided by parent EventDetector to read other-circuit valve states
        self._get_other_valve_open: Callable[[], Optional[bool]] = (
            get_other_valve_open or (lambda: None)
        )

        self._debug_capture_propagation: bool = debug_capture_propagation

        self._pressure_buf: Deque[float] = deque(maxlen=self.PRESSURE_BUFFER_SIZE)
        # Per-sample arrival timestamps, kept exactly parallel to _pressure_buf
        # (same maxlen, appended/cleared together) — diagnostic use only.
        self._pressure_ts_buf: Deque[datetime] = deque(maxlen=self.PRESSURE_BUFFER_SIZE)
        self._settled_pressure_psi: Optional[float] = None
        self._settled_pressure_since: Optional[datetime] = None
        self._active_event: Optional[RawEvent] = None
        self._current_flow_lpm: float = 0.0
        self._flow_sustained_since: Optional[datetime] = None
        self._pressure_recovered_since: Optional[datetime] = None

        # Downsampling: keep all readings for the first N seconds, then every Kth.
        # Prevents 290k-sample lists for 2-hour irrigation events.
        self._DOWNSAMPLE_AFTER_SECONDS: float = 120.0
        self._DOWNSAMPLE_KEEP_EVERY: int = 5
        self._flow_sample_count: int = 0
        self._pressure_sample_count: int = 0

    # ------------------------------------------------------------------ #
    # Public                                                               #
    # ------------------------------------------------------------------ #

    def update_threshold(self, threshold_psi: float) -> None:
        self.pressure_drop_threshold = threshold_psi

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

        if self._active_event is not None:
            elapsed = (now - self._active_event.start_ts).total_seconds()
            self._flow_sample_count += 1
            if elapsed < self._DOWNSAMPLE_AFTER_SECONDS or self._flow_sample_count % self._DOWNSAMPLE_KEEP_EVERY == 0:
                self._active_event.flow_readings.append(self._current_flow_lpm)
            self._flow_sustained_since = None
            return

        # No active event — manage flow start timer
        if self._current_flow_lpm >= self.MIN_FLOW_LPM:
            if self._flow_sustained_since is None:
                self._flow_sustained_since = now
                log.debug("[%s] flow start timer begins (%.3f L/min)",
                          self.circuit, self._current_flow_lpm)
            elif (now - self._flow_sustained_since).total_seconds() >= self.FLOW_START_SECONDS:
                self._start_flow_event(now)
        else:
            if self._flow_sustained_since is not None:
                log.debug("[%s] flow start timer reset (%.3f L/min)",
                          self.circuit, self._current_flow_lpm)
            self._flow_sustained_since = None

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
                else:
                    self._start_pressure_event(now, baseline, pressure)
        else:
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
                    elif (
                        (now - self._pressure_recovered_since).total_seconds()
                        >= self.PRESSURE_RECOVERY_DURATION_S
                        and self._current_flow_lpm < self.MIN_FLOW_LPM
                    ):
                        log.debug(
                            "[%s] pressure recovered (>= %.2f PSI for %.1f s, flow=%.3f) "
                            "— ending pressure-triggered event",
                            self.circuit, recovery_line,
                            (now - self._pressure_recovered_since).total_seconds(),
                            self._current_flow_lpm,
                        )
                        self._end_event(now)
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
            flow_readings=[self._current_flow_lpm],
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

    def _end_event(self, ts: datetime) -> None:
        ev = self._active_event
        if ev is None:
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

        avg_flow = (
            sum(ev.flow_readings) / len(ev.flow_readings) if ev.flow_readings else 0.0
        )
        volume_l = avg_flow * duration / 60.0
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
            pressure_rise = ev.max_pressure_psi - ev.pre_event_pressure_psi
            if pressure_rise > self.PRESSURE_SURGE_PHANTOM_PSI and ev.pressure_delta_psi <= 0:
                log.info(
                    "[%s] rejecting pressure-surge phantom: rose %.2f PSI "
                    "(max=%.1f baseline=%.1f delta=%.2f) duration=%.1f s",
                    self.circuit, pressure_rise, ev.max_pressure_psi,
                    ev.pre_event_pressure_psi, ev.pressure_delta_psi, duration,
                )
                self._pressure_recovered_since = None
                return

        log.info(
            "[%s] event complete — trigger=%s duration=%.1f s avg_flow=%.3f L/min "
            "pressure_drop=%.1f PSI has_transient=%s composite=%s",
            self.circuit, ev.start_trigger, duration, avg_flow,
            ev.pressure_delta_psi, ev.has_pressure_transient, ev.is_composite,
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


# ------------------------------------------------------------------------------- #
# Per-event waveform capture (firmware 3.8.0+) — HA-event handler and wire-format
# ------------------------------------------------------------------------------- #

# Wire-format constants — must match firmware build-time contract.
_WF_START_POINTS_MAX: int = 88      # hard cap on start-window point count
_WF_FULL_POINTS_MAX: int = 64       # hard cap on full-window point count
_WF_MAX_RECORDS: int = 30           # maximum assembled records to keep per circuit
_WF_FLAG_VALID_MASK: int = 0x7F     # bits 0-6 only; bit 7 must be 0
_WF_SUPPORTED_VERSIONS = {1}        # set of wire-format versions we handle


@dataclass
class WaveformMetadata:
    """Decoded and validated event_metadata JSON."""
    event_id: int           # id — monotonic per-boot waveform event counter
    boot_id: int            # b  — ESP session id
    version: int            # v  — wire-format version
    start_ms: int           # s  — event start millis()
    end_ms: int             # e  — event end millis()
    publish_ms: int         # p  — publish millis()
    start_points: int       # sn — resampled points in each start window
    full_points: int        # fn — resampled points in each full window
    pre_ms: int             # pre — start-window pre-event portion (ms)
    post_ms: int            # post — start-window post-event portion (ms)
    tail_ms: int            # tl — full-window post-event tail (ms)
    flow_scale: int         # fs — int16 → L/min divisor
    pressure_scale: int     # ps — int16 → PSI divisor
    peak_flow: float        # pk — peak flow (×100 in wire, /100 here)
    pressure_delta: float   # dp — pressure delta (×100 in wire, /100 here)
    propagation_delay_ms: int   # pd — onset propagation delay (ms; -1 = not detected)
    quality: int            # q  — 0 ok, 1 incomplete, firmware never publishes 2-6
    flags: int              # fl — bitfield (see plan)


@dataclass
class WaveformRecord:
    """Fully assembled and decoded per-event waveform — ready for feature extraction."""
    circuit: str
    boot_id: int
    event_id: int
    metadata: WaveformMetadata
    # Each list has exactly metadata.start_points or metadata.full_points float values.
    start_flow: List[float]       # L/min
    start_pressure: List[float]   # PSI
    full_flow: List[float]        # L/min
    full_pressure: List[float]    # PSI
    received_at: float            # time.monotonic() when the record was assembled


def _parse_metadata(source) -> Optional[WaveformMetadata]:
    """
    Parse and validate waveform metadata.

    Accepts either a JSON string (text-sensor path) or a pre-parsed dict
    (HA event path where values may be strings or native JSON numbers).
    Returns None (with a DEBUG log) on any validation failure.
    """
    if isinstance(source, str):
        try:
            d = json.loads(source)
        except (json.JSONDecodeError, ValueError):
            log.debug("waveform: metadata JSON decode failed: %.80r", source)
            return None
    else:
        d = source

    required_int = ("id", "b", "v", "s", "e", "p", "sn", "fn",
                    "pre", "post", "tl", "fs", "ps", "pk", "dp", "pd", "q", "fl")
    normalized: dict = {}
    for key in required_int:
        raw = d.get(key)
        if raw is None:
            log.debug("waveform: metadata missing field %r", key)
            return None
        # Reject bool — Python bool is an int subclass, so must check explicitly.
        if isinstance(raw, bool):
            log.debug("waveform: metadata field %r has unexpected bool value", key)
            return None
        try:
            normalized[key] = int(raw)
        except (TypeError, ValueError):
            log.debug("waveform: metadata field %r not castable to int: %.40r", key, raw)
            return None
    d = normalized

    # Field-range validation
    ev   = d["id"];  b = d["b"];   v = d["v"]
    sn   = d["sn"];  fn = d["fn"]
    fs   = d["fs"];  ps = d["ps"]
    q    = d["q"];   fl = d["fl"]

    if ev < 0 or b < 0:
        log.debug("waveform: metadata negative id/boot_id")
        return None
    if v not in _WF_SUPPORTED_VERSIONS:
        log.debug("waveform: metadata unsupported version %d", v)
        return None
    if sn <= 0 or sn > _WF_START_POINTS_MAX:
        log.debug("waveform: metadata sn=%d out of range", sn)
        return None
    if fn <= 0 or fn > _WF_FULL_POINTS_MAX:
        log.debug("waveform: metadata fn=%d out of range", fn)
        return None
    if fs <= 0 or ps <= 0:
        log.debug("waveform: metadata fs/ps must be positive")
        return None
    if not (0 <= q <= 6):
        log.debug("waveform: metadata q=%d out of range", q)
        return None
    if fl & ~_WF_FLAG_VALID_MASK:
        log.debug("waveform: metadata fl=%d has invalid high bits", fl)
        return None

    return WaveformMetadata(
        event_id=ev,
        boot_id=b,
        version=v,
        start_ms=d["s"],
        end_ms=d["e"],
        publish_ms=d["p"],
        start_points=sn,
        full_points=fn,
        pre_ms=d["pre"],
        post_ms=d["post"],
        tail_ms=d["tl"],
        flow_scale=fs,
        pressure_scale=ps,
        peak_flow=d["pk"] / 100.0,
        pressure_delta=d["dp"] / 100.0,
        propagation_delay_ms=d["pd"],
        quality=q,
        flags=fl,
    )


def _decode_waveform(
    b64_payload: str,
    expected_pts: int,
    scale: int,
) -> Optional[List[float]]:
    """
    Decode a base64-encoded little-endian int16 waveform payload.

    Returns a list of ``expected_pts`` float values (int16 ÷ scale), or None
    when the payload is empty, malformed, or not exactly the expected byte length.
    """
    if not b64_payload:
        log.debug("waveform: empty base64 payload")
        return None
    try:
        raw = base64.b64decode(b64_payload, validate=True)
    except Exception:
        log.debug("waveform: base64 decode error for payload %.40r", b64_payload)
        return None
    expected_bytes = expected_pts * 2
    if len(raw) != expected_bytes:
        log.debug(
            "waveform: payload length %d != expected %d bytes (%d pts × 2)",
            len(raw), expected_bytes, expected_pts,
        )
        return None
    values: List[float] = []
    for i in range(expected_pts):
        (v,) = struct.unpack_from("<h", raw, i * 2)
        values.append(v / scale)
    return values


def _normalize_node_name(name: str) -> str:
    """Normalize an ESPHome node name for identity comparison.

    App.get_name() may use hyphens; HA entity prefixes use underscores.
    Both sides of the comparison must be normalized the same way.
    """
    return name.strip().lower().replace("-", "_")


class WaveformEventHandler:
    """
    Per-circuit handler for waveform delivery via HA event (firmware 3.8.0+).

    A single ``esphome.water_monitor_waveform`` event carries all four
    waveform payloads and the metadata in one atomic delivery — there are no
    chunks to reassemble.  Replaced the five-text-sensor transport in 3.8.0.

    - No initial grace window — HA events are ephemeral bus events, never
      restored HA state.  Stale delivery is prevented by schema/identity
      validation instead.
    - All scalar values arrive as strings (or native JSON numbers); both are
      accepted and normalized to int.
    - Validation failures log at DEBUG so hardware testing is debuggable
      without INFO spam.
    """

    def __init__(self, circuit: str, expected_node: str) -> None:
        self._circuit = circuit
        # Normalize once so every comparison is cheap.
        self._expected_node = _normalize_node_name(expected_node)
        self._known_boot_id: Optional[int] = None
        self._records: List[WaveformRecord] = []

    def on_waveform_event(self, data: dict) -> None:
        """Process a single esphome.water_monitor_waveform event payload."""
        # Schema fingerprint guard.
        if data.get("schema") != "esp_water_monitor_waveform":
            log.debug("waveform[%s]: event rejected — unexpected schema %r",
                      self._circuit, data.get("schema"))
            return

        # HA may coerce numeric template strings back to int — accept either.
        if str(data.get("transport_version", "")) != "1":
            log.debug("waveform[%s]: event rejected — unsupported transport_version %r",
                      self._circuit, data.get("transport_version"))
            return

        # Device identity guard (normalized comparison).
        node = _normalize_node_name(data.get("node", ""))
        if self._expected_node and node != self._expected_node:
            log.debug("waveform[%s]: event rejected — node %r != expected %r",
                      self._circuit, node, self._expected_node)
            return

        # Circuit routing — wrong circuit is quietly ignored (normal when a
        # second ESP device fires on the same HA bus).
        if data.get("circuit") != self._circuit:
            return

        # Parse and validate metadata (shared with the text-sensor path).
        meta = _parse_metadata(data)
        if meta is None:
            # _parse_metadata already logged the rejection reason.
            return

        # Decode the four waveform payloads.
        start_flow = _decode_waveform(
            data.get("sf", ""), meta.start_points, meta.flow_scale)
        start_pressure = _decode_waveform(
            data.get("sp", ""), meta.start_points, meta.pressure_scale)
        full_flow = _decode_waveform(
            data.get("ff", ""), meta.full_points, meta.flow_scale)
        full_pressure = _decode_waveform(
            data.get("fp", ""), meta.full_points, meta.pressure_scale)

        if any(w is None for w in (start_flow, start_pressure, full_flow, full_pressure)):
            log.debug("waveform[%s]: event %d rejected — waveform decode failed",
                      self._circuit, meta.event_id)
            return

        # Boot-session guard.
        if self._known_boot_id is not None and meta.boot_id != self._known_boot_id:
            log.debug("waveform[%s]: boot_id changed %d→%d",
                      self._circuit, self._known_boot_id, meta.boot_id)
        self._known_boot_id = meta.boot_id

        record = WaveformRecord(
            circuit=self._circuit,
            boot_id=meta.boot_id,
            event_id=meta.event_id,
            metadata=meta,
            start_flow=start_flow,
            start_pressure=start_pressure,
            full_flow=full_flow,
            full_pressure=full_pressure,
            received_at=time.monotonic(),
        )
        self._records.append(record)
        if len(self._records) > _WF_MAX_RECORDS:
            self._records = self._records[-_WF_MAX_RECORDS:]

        log.debug(
            "waveform[%s]: assembled event %d via HA event (boot=%d q=%d fl=0x%02x "
            "sn=%d fn=%d pk=%.2f dp=%.2f pd=%dms)",
            self._circuit, meta.event_id, meta.boot_id,
            meta.quality, meta.flags,
            meta.start_points, meta.full_points,
            meta.peak_flow, meta.pressure_delta, meta.propagation_delay_ms,
        )

    def get_record(self, boot_id: int, event_id: int) -> Optional[WaveformRecord]:
        for rec in reversed(self._records):
            if rec.boot_id == boot_id and rec.event_id == event_id:
                return rec
        return None

    def latest_record(self) -> Optional[WaveformRecord]:
        return self._records[-1] if self._records else None

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
    ) -> None:
        self._circuits = circuits
        self._ha = ha_client
        self._queue = event_queue
        self._sensitivity_getter = sensitivity_getter
        self._debug_capture_propagation = debug_capture_propagation
        self._detectors: Dict[str, CircuitEventDetector] = {}
        # Tracks live valve open/closed state per circuit for cross-circuit feature
        self._valve_open: Dict[str, bool] = {}
        # HA-event waveform handlers (firmware 3.8.0+) — sole transport.
        self._event_handlers: Dict[str, WaveformEventHandler] = {}
        self._wf_event_subscribed: bool = False
        self._is_configured = False

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
            )
            self._detectors[cfg.circuit] = detector

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

            # Per-event waveform capture (firmware 3.8.0+, circuit_1 only).
            # esp_device_prefix is e.g. "esp_water_main_"; strip the trailing
            # underscore to get the normalized node name for identity comparison.
            expected_node = cfg.esp_device_prefix.rstrip("_")
            handler = WaveformEventHandler(cfg.circuit, expected_node=expected_node)
            self._event_handlers[cfg.circuit] = handler

            # Register the HA event subscription once (shared across all circuits).
            if not self._wf_event_subscribed:
                self._ha.subscribe_event(
                    "esphome.water_monitor_waveform",
                    self._on_waveform_event,
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
        """Reload thresholds from config after sensitivity settings change."""
        for circuit, detector in self._detectors.items():
            sens = self._sensitivity_getter(circuit)
            detector.update_threshold(sens.get("pressure_drop_event_psi", 1.2))
            detector.min_event_duration = sens.get("min_event_duration_seconds", 3.0)

    def _on_waveform_event(self, data: dict) -> None:
        """Route an esphome.water_monitor_waveform event to the correct circuit handler."""
        circuit = data.get("circuit", "")
        handler = self._event_handlers.get(circuit)
        if handler is not None:
            handler.on_waveform_event(data)
        # Wrong or missing circuit is handled silently by the handler itself.

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
        handler = self._event_handlers.get(circuit)
        return handler.get_record(boot_id, event_id) if handler else None

    def get_latest_waveform(self, circuit: str) -> Optional[WaveformRecord]:
        """Return the most-recently assembled WaveformRecord for a circuit, or None."""
        handler = self._event_handlers.get(circuit)
        return handler.latest_record() if handler else None

    def pop_waveform_record(
        self,
        circuit: str,
        boot_id: int,
        event_id: int,
    ) -> Optional[WaveformRecord]:
        """Remove and return the WaveformRecord for (boot_id, event_id), or None."""
        handler = self._event_handlers.get(circuit)
        return handler.pop_record(boot_id, event_id) if handler else None
