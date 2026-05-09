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
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Deque, List, Literal, Optional

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
    has_pressure_transient: bool = False
    pre_event_pressure_psi: float = 0.0
    min_pressure_psi: float = 0.0
    pressure_delta_psi: float = 0.0
    pressure_readings: List[float] = field(default_factory=list)

    # Flow onset relative to event start (only meaningful for pressure-started events)
    flow_onset_ts: Optional[datetime] = None
    propagation_delay_seconds: Optional[float] = None

    # 1Hz flow readings collected during the event
    flow_readings: List[float] = field(default_factory=list)

    # True if any other circuit's valve was open when this event started.
    # Helps distinguish main-circuit irrigation bleed-through from household demand.
    other_valve_open: Optional[bool] = None

    is_composite: bool = False
    complete: bool = False


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
    MIN_FLOW_LPM: float = 0.05

    # Seconds of sustained flow required to open a flow-triggered event
    FLOW_START_SECONDS: float = 2.0

    # Composite: second transient must be >= this multiple of primary threshold
    COMPOSITE_TRANSIENT_MULTIPLIER: float = 1.5

    def __init__(
        self,
        circuit: str,
        pressure_drop_threshold_psi: float,
        min_event_duration_seconds: float,
        event_queue: asyncio.Queue,
        get_other_valve_open: Optional[Callable[[], Optional[bool]]] = None,
    ) -> None:
        self.circuit = circuit
        self.pressure_drop_threshold = pressure_drop_threshold_psi
        self.min_event_duration = min_event_duration_seconds
        self._event_queue = event_queue
        # Callable provided by parent EventDetector to read other-circuit valve states
        self._get_other_valve_open: Callable[[], Optional[bool]] = (
            get_other_valve_open or (lambda: None)
        )

        self._pressure_buf: Deque[float] = deque(maxlen=self.PRESSURE_BUFFER_SIZE)
        self._active_event: Optional[RawEvent] = None
        self._current_flow_lpm: float = 0.0
        self._flow_sustained_since: Optional[datetime] = None

        # Downsampling: keep all readings for the first N seconds, then every Kth.
        # Prevents 290k-sample lists for 2-hour irrigation events.
        self._DOWNSAMPLE_AFTER_SECONDS: float = 120.0
        self._DOWNSAMPLE_KEEP_EVERY: int = 5

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
            self._current_flow_lpm = float(state)
        except (ValueError, TypeError):
            self._current_flow_lpm = 0.0

        now = datetime.now(timezone.utc)

        if self._active_event is not None:
            elapsed = (now - self._active_event.start_ts).total_seconds()
            n = len(self._active_event.flow_readings)
            if elapsed < self._DOWNSAMPLE_AFTER_SECONDS or n % self._DOWNSAMPLE_KEEP_EVERY == 0:
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
        try:
            pressure = float(state)
        except (ValueError, TypeError):
            return

        now = datetime.now(timezone.utc)
        self._pressure_buf.append(pressure)

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
            if drop >= self.pressure_drop_threshold:
                self._start_pressure_event(now, baseline, pressure)
        else:
            elapsed_p = (now - self._active_event.start_ts).total_seconds()
            np = len(self._active_event.pressure_readings)
            if elapsed_p < self._DOWNSAMPLE_AFTER_SECONDS or np % self._DOWNSAMPLE_KEEP_EVERY == 0:
                self._active_event.pressure_readings.append(pressure)

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
            if (self._active_event is not None
                    and self._active_event.flow_onset_ts is None):
                self._active_event.flow_onset_ts = now
                delay = (now - self._active_event.start_ts).total_seconds()
                self._active_event.propagation_delay_seconds = delay
                log.debug("[%s] flow onset — propagation delay %.2f s",
                          self.circuit, delay)
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

    def _start_flow_event(self, now: datetime) -> None:
        start_ts = self._flow_sustained_since or now
        self._flow_sustained_since = None

        baseline = (
            sum(self._pressure_buf) / len(self._pressure_buf)
            if self._pressure_buf else 0.0
        )

        log.info("[%s] event start (FLOW) — %.3f L/min for >= %.1f s",
                 self.circuit, self._current_flow_lpm, self.FLOW_START_SECONDS)

        self._active_event = RawEvent(
            circuit=self.circuit,
            start_ts=start_ts,
            start_trigger="flow",
            flow_onset_ts=start_ts,
            propagation_delay_seconds=0.0,
            pre_event_pressure_psi=baseline,
            min_pressure_psi=baseline,
            flow_readings=[self._current_flow_lpm],
            other_valve_open=self._get_other_valve_open(),
        )

    def _start_pressure_event(self, now: datetime, baseline: float,
                              current_pressure: float) -> None:
        drop = baseline - current_pressure
        log.info("[%s] event start (PRESSURE) — %.1f PSI drop (%.1f -> %.1f PSI)",
                 self.circuit, drop, baseline, current_pressure)

        self._active_event = RawEvent(
            circuit=self.circuit,
            start_ts=now,
            start_trigger="pressure",
            has_pressure_transient=True,
            pre_event_pressure_psi=baseline,
            min_pressure_psi=current_pressure,
            pressure_delta_psi=drop,
            pressure_readings=[current_pressure],
            other_valve_open=self._get_other_valve_open(),
        )
        self._flow_sustained_since = None

    def _record_pressure_transient(self, now: datetime, baseline: float,
                                   current_pressure: float) -> None:
        """Enrich a flow-triggered event with a pressure transient that arrived late."""
        ev = self._active_event
        if ev is None:
            return

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

        duration = (ts - ev.start_ts).total_seconds()

        if duration < self.min_event_duration:
            log.debug("[%s] discarding short event (%.1f s < %.1f s)",
                      self.circuit, duration, self.min_event_duration)
            self._active_event = None
            return

        ev.end_ts = ts
        # Use `is not None` — pre_event_pressure_psi defaults to 0.0,
        # which is falsy but valid for zero-baseline (unpressurised) systems.
        if ev.pressure_readings and ev.pre_event_pressure_psi is not None:
            ev.min_pressure_psi = min(ev.pressure_readings)
            ev.pressure_delta_psi = ev.pre_event_pressure_psi - ev.min_pressure_psi
        ev.complete = True
        self._active_event = None

        avg_flow = (
            sum(ev.flow_readings) / len(ev.flow_readings) if ev.flow_readings else 0.0
        )
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
        self._pressure_buf.clear()
        self._current_flow_lpm = 0.0
        self._flow_sustained_since = None


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
    ) -> None:
        self._circuits = circuits
        self._ha = ha_client
        self._queue = event_queue
        self._sensitivity_getter = sensitivity_getter
        self._detectors: Dict[str, CircuitEventDetector] = {}
        # Tracks live valve open/closed state per circuit for cross-circuit feature
        self._valve_open: Dict[str, bool] = {}
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
                pressure_drop_threshold_psi=sens.get("pressure_drop_event_psi", 2.0),
                min_event_duration_seconds=sens.get("min_event_duration_seconds", 3.0),
                event_queue=self._queue,
                get_other_valve_open=(
                    lambda c=cfg.circuit: self._get_other_valve_open(c)
                ),
            )
            self._detectors[cfg.circuit] = detector

            self._ha.subscribe_entity(cfg.flow_sensor,          detector.on_flow_rate)
            self._ha.subscribe_entity(cfg.pressure_fast_sensor, detector.on_pressure_fast)
            self._ha.subscribe_entity(cfg.flow_onset_sensor,    detector.on_flow_onset)
            # Track valve states so we can record other-circuit valve open at event start
            if cfg.valve_entity:
                self._ha.subscribe_entity(
                    cfg.valve_entity,
                    lambda eid, state, attrs, c=cfg.circuit: self._on_valve_state(c, state),
                )

            log.info(
                "[%s] event detector ready — triggers: "
                "flow (>= %.2f L/min for %.1f s) | "
                "pressure (>= %.1f PSI drop) | combined",
                cfg.circuit,
                detector.MIN_FLOW_LPM,
                detector.FLOW_START_SECONDS,
                sens.get("pressure_drop_event_psi", 2.0),
            )

    def update_thresholds(self) -> None:
        """Reload thresholds from config after sensitivity settings change."""
        for circuit, detector in self._detectors.items():
            sens = self._sensitivity_getter(circuit)
            detector.update_threshold(sens.get("pressure_drop_event_psi", 2.0))
            detector.min_event_duration = sens.get("min_event_duration_seconds", 3.0)

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
