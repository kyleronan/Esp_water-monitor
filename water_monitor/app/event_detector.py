"""Event detector — top-level coordinator (``EventDetector``).

The subsystem is three modules:

  ``event_detector_core``  — ``RawEvent``, the propagation-delay scan and
                             ``CircuitEventDetector`` (the per-circuit state
                             machine, including the single shared
                             ``_run_close_ladder`` that both the flow and the
                             pressure callback run).
  ``event_waveform``       — the firmware waveform wire format and
                             ``WaveformChunkAccumulator``.
  this module              — ``EventDetector``, which owns one
                             ``CircuitEventDetector`` and one accumulator per
                             circuit and wires them to HaClient subscriptions.

Dependencies run ONE way (this module -> event_waveform -> event_detector_core);
``test_unit73_event_detector_split`` fails if that re-tangles.

Moved names stay readable here via the PEP 562 ``__getattr__`` at the bottom.
Two things it deliberately does NOT do:

  * invent an attribute — an unknown name raises ``AttributeError``, so
    ``hasattr`` still answers False for symbols that were deleted;
  * offer a WRITE path. ``monkeypatch.setattr(event_detector, "datetime", ...)``
    sets a *local* attribute the moved code never reads; patch the module the
    code actually lives in (see ``test_flow_start_stale_guard``).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from .event_detector_core import CircuitEventDetector, RawEvent, log
from .event_waveform import WaveformChunkAccumulator, WaveformRecord


# ── Firmware signals the add-on subscribes to for OBSERVABILITY ──────────────
#
# Mirrored into EventDetector._device_signals and surfaced on /health/detail.
#
# HARD RULE: nothing in this process may branch on them. They are not gates,
# not preconditions, and not inputs to any actuation decision. Adding a read of
# _device_signals to a code path that opens or closes a valve is a behaviour
# change.
#
# Per-circuit. Keys are roles in circuit_entity_map (see device_discovery).
_DEVICE_TRUTH_ROLES: Tuple[str, ...] = (
    # The only ground truth for valve position: physical microswitches. The
    # `valve.*` entity is a firmware template PUBLISHED FROM these, so it can
    # report a position the valve never reached (stall mid-travel, dead
    # switch); the stops themselves cannot.
    "open_end_stop_sensor",
    "closed_end_stop_sensor",
    # device_class: problem — flow still detected 90 s after the CLOSED end
    # stop went active, i.e. the valve did not seat. The one signal that says
    # a close command "succeeded" but the water did not stop.
    "valve_seal_alert_sensor",
)

# Device-wide waveform stage counters (firmware publishes them once, and
# discovery maps them under circuit_1). Read alongside the accumulator's own
# transport_stats they make the pipeline arithmetic: captures started →
# chunks staged → events fired (firmware) → assembled / gaps (add-on). A stage
# where the number stops moving is the stage that is broken.
_DEVICE_DIAG_ROLES: Tuple[str, ...] = (
    "wf_captures_started_sensor",
    "wf_chunks_staged_sensor",
    "wf_events_fired_sensor",
    "wf_overflow_count_sensor",
    "wf_chunk_drop_count_sensor",
)


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
        winterized_getter: Optional[Callable[[str], bool]] = None,
        pump_fail_cb: Optional[Callable[[str, float, str], None]] = None,
        entities_getter: Optional[Callable[[str], Dict[str, str]]] = None,
        subsystem_degraded_cb: Optional[Callable[..., None]] = None,
    ) -> None:
        self._circuits = circuits
        self._ha = ha_client
        self._queue = event_queue
        self._sensitivity_getter = sensitivity_getter
        # Returns the pump-mode oscillation gate (PSI) for a circuit, or None
        # when pump-aware suppression is off. Wired by the orchestrator; None
        # default keeps tests/imports gate-free.
        self._pump_gate_getter = pump_gate_getter or (lambda _c: None)
        # Returns (zone_floor_psi|None, pump_fail_floor|None) per circuit; the
        # callbacks fire the alerts (orchestrator wires them to AlertManager
        # via create_task).
        self._low_pressure_getter = low_pressure_getter or (
            lambda _c: (None, None))
        self._low_pressure_cb = low_pressure_cb
        # Injected like the other DB-backed getters — this class has no
        # connection of its own, and giving it one would break the
        # single-connection invariant. Optional so every existing construction
        # site (tests, tools) keeps working.
        self._winterized_getter = winterized_getter
        self._pump_fail_cb = pump_fail_cb
        self._debug_capture_propagation = debug_capture_propagation
        self._detectors: Dict[str, CircuitEventDetector] = {}
        # Tracks live valve open/closed state per circuit for cross-circuit feature
        self._valve_open: Dict[str, bool] = {}
        # (source, set_at_iso) provenance per circuit for the above.
        self._valve_meta: Dict[str, Tuple[str, str]] = {}
        # Chunked HA-event waveform accumulators (firmware 3.9.0+) — sole transport.
        self._chunk_accumulators: Dict[str, WaveformChunkAccumulator] = {}
        self._wf_event_subscribed: bool = False
        self._is_configured = False
        # Optional sink for late-assembled waveforms (signature upgrade).
        # Wired by the orchestrator to FeatureExtractor.handle_late_waveform;
        # left None in tests / import → assembly notifies nothing.
        self._waveform_upgrade_sink: Optional[Callable[[str, WaveformRecord], None]] = None
        # ── Device-truth signals ──────────────────────────────────────────
        # Returns {role: entity_id} for a circuit. Injected like the other
        # DB-backed getters (this class owns no connection); invoked ONLY from
        # collect_circuit_inputs, which the callers submit through run_db.
        # audit-ok(run_db).
        self._entities_getter = entities_getter
        # Wired by the orchestrator to mark_subsystem_degraded, so a transport
        # condition lands in the SAME worker_health record /health/detail
        # already reads rather than in a second bespoke surface.
        self._subsystem_degraded_cb = subsystem_degraded_cb
        #: circuit -> role -> {entity_id, state, changed_at}. Mirror of the
        #: firmware's end stops / valve-seal alerts / waveform stage counters.
        #: OBSERVABILITY ONLY — nothing in this process may branch on it.
        self._device_signals: Dict[str, Dict[str, dict]] = {}
        self._device_signals_subscribed: bool = False

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

    def collect_circuit_inputs(self, circuits=None) -> dict:
        """Every DB-backed input the detector needs, in one hop.

        The getters injected by the orchestrator (sensitivity, pump gate,
        low-pressure floors, winterized, entities) each query the shared
        connection, so they must not be invoked inline from ``setup`` /
        ``update_thresholds``, which run on the event loop.

        Collected here so the loop-side code stays where it is: object
        construction and HA subscription must NOT move to the DB thread, which
        would trade a DB race for a subscription-registry one.
        """
        out = {}
        for circuit in (circuits if circuits is not None
                        else [c.circuit for c in self._circuits]):
            try:
                gate = self._pump_gate_getter(circuit)                     if self._pump_gate_getter else None
            except Exception:
                gate = None
            try:
                floors = self._low_pressure_getter(circuit)                     if self._low_pressure_getter else None
            except Exception:
                floors = None
            try:
                winterized = (bool(self._winterized_getter(circuit))
                              if self._winterized_getter else False)
            except Exception:
                winterized = False
            # {role: entity_id} for this circuit, read on the DB thread with
            # everything else. Best-effort — an unconfigured / partially-mapped
            # install yields {} and the device-signal subscriptions are skipped.
            try:
                entities = (dict(self._entities_getter(circuit))
                            if self._entities_getter else {})
            except Exception:
                entities = {}
            out[circuit] = {"sens": self._sensitivity_getter(circuit),
                            "gate": gate, "floors": floors,
                            "winterized": winterized,
                            "entities": entities}
        return out

    async def setup(self, inputs=None) -> None:
        """Instantiate detectors and register HA entity subscriptions.

        Idempotent — safe to call more than once (e.g. after the setup
        wizard completes on an already-running system).  The second call
        is a no-op so duplicate HA subscriptions are never registered.
        """
        if self._is_configured:
            log.debug("Event detector already configured — skipping re-setup")
            return
        self._is_configured = True
        if inputs is None:
            from .database import run_db
            inputs = await run_db(self.collect_circuit_inputs)
        for cfg in self._circuits:
            sens = inputs[cfg.circuit]["sens"]
            detector = CircuitEventDetector(
                circuit=cfg.circuit,
                pressure_drop_threshold_psi=sens.get("pressure_drop_event_psi", 1.2),
                min_event_duration_seconds=sens.get("min_event_duration_seconds", 3.0),
                event_queue=self._queue,
                get_other_valve_open=(
                    lambda c=cfg.circuit: self._get_other_valve_open(c)
                ),
                get_other_valve_meta=(
                    lambda c=cfg.circuit: self._get_other_valve_meta(c)
                ),
                flow_onset_entity=cfg.flow_onset_sensor,
                debug_capture_propagation=self._debug_capture_propagation,
                min_flow_lpm=getattr(cfg, "min_flow_lpm", 0.15),
            )
            self._detectors[cfg.circuit] = detector
            detector.update_pump_gate(inputs[cfg.circuit]["gate"])
            detector.set_winterized(inputs[cfg.circuit].get("winterized"))
            detector.low_pressure_cb = self._low_pressure_cb
            detector.pump_fail_cb = self._pump_fail_cb
            if inputs[cfg.circuit]["floors"] is not None:
                zf, pf = inputs[cfg.circuit]["floors"]
                detector.update_low_pressure_config(zf, pf)

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
            # underscore for the normalized node name. removesuffix("_") strips
            # exactly one — rstrip("_") would strip several from a prefix
            # configured with a double-underscore tail.
            expected_node = cfg.esp_device_prefix.removesuffix("_")
            accumulator = WaveformChunkAccumulator(
                cfg.circuit, expected_node=expected_node,
                on_record_assembled=self._on_waveform_assembled,
                on_degraded=self._on_transport_degraded,
            )
            self._chunk_accumulators[cfg.circuit] = accumulator

            # Mirror the firmware's valve-truth + waveform stage entities.
            # Read-only: see _DEVICE_TRUTH_ROLES.
            self._subscribe_device_signals(
                cfg.circuit, inputs[cfg.circuit].get("entities") or {})

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

    async def update_thresholds(self, inputs=None) -> None:
        """Reload thresholds from config after sensitivity settings change
        (also re-resolves the pump-mode oscillation gate — the banner-confirm
        route calls this so pump suppression flips without a restart)."""
        if inputs is None:
            from .database import run_db
            inputs = await run_db(self.collect_circuit_inputs,
                                  list(self._detectors))
        for circuit, detector in self._detectors.items():
            sens = inputs[circuit]["sens"]
            detector.update_threshold(sens.get("pressure_drop_event_psi", 1.2))
            detector.min_event_duration = sens.get("min_event_duration_seconds", 3.0)
            detector.set_winterized(inputs[circuit].get("winterized"))
            try:
                detector.update_pump_gate(inputs[circuit]["gate"])
            except Exception as e:
                log.warning("[%s] pump-gate apply failed (non-fatal): %s",
                            circuit, e)
            try:
                zf, pf = inputs[circuit]["floors"]
                detector.update_low_pressure_config(zf, pf)
            except Exception as e:
                log.warning("[%s] low-pressure resolve failed (non-fatal): %s",
                            circuit, e)

    # ------------------------------------------------------------------
    # Device-truth signals (OBSERVABILITY ONLY)
    # ------------------------------------------------------------------

    def _subscribe_device_signals(self, circuit: str,
                                  entities: Dict[str, str]) -> None:
        """Register state subscriptions for this circuit's truth/diag roles.

        Cheap by construction. `HaClient.subscribe_entities` puts every id into
        ONE ``subscribe_entities`` WebSocket subscription, so this adds list
        entries, not connections or round-trips; unmapped roles (older
        firmware, partial discovery) are simply skipped.
        """
        slots = self._device_signals.setdefault(circuit, {})
        # The waveform diagnostics are device-wide and discovery maps them
        # under the first circuit only; asking for them on circuit_2 would
        # just skip every role.
        first = self._circuits[0].circuit if self._circuits else circuit
        roles = _DEVICE_TRUTH_ROLES + (
            _DEVICE_DIAG_ROLES if circuit == first else ())
        for role in roles:
            entity_id = (entities.get(role) or "").strip()
            if not entity_id or role in slots:
                continue
            slots[role] = {"entity_id": entity_id, "state": None,
                           "changed_at": None}
            self._ha.subscribe_entity(
                entity_id,
                lambda eid, state, attrs, c=circuit, r=role:
                    self._on_device_signal(c, r, state),
            )
            self._device_signals_subscribed = True
        if slots:
            log.info("[%s] device-truth signals mirrored (read-only): %s",
                     circuit, ", ".join(sorted(slots)))

    def _on_device_signal(self, circuit: str, role: str, state: str) -> None:
        """Record a device-signal state change. Records; decides nothing."""
        slot = self._device_signals.get(circuit, {}).get(role)
        if slot is None:
            return
        slot["state"] = state
        slot["changed_at"] = datetime.now(timezone.utc).isoformat()

    async def prime_device_signals(self) -> None:
        """Seed device-signal states from current HA state.

        Same subscribe-then-prime shape as prime_valve_states: an end stop that
        never changes after boot would otherwise read `null` forever, which is
        indistinguishable from "not mapped". Best-effort per entity.
        """
        for circuit, slots in self._device_signals.items():
            for role, slot in slots.items():
                if slot["state"] is not None:
                    continue   # a real change event beat us here — it wins
                try:
                    state = await self._ha.get_state_value(slot["entity_id"])
                except Exception as e:
                    log.debug("[%s] %s prime failed (non-fatal): %s",
                              circuit, role, e)
                    continue
                if state is None:
                    continue
                if slot["state"] is None:      # re-check after the await
                    slot["state"] = str(state)
                    slot["changed_at"] = datetime.now(timezone.utc).isoformat()

    def device_signals(self) -> Dict[str, Dict[str, dict]]:
        """Snapshot of the mirrored firmware signals, for /health/detail.

        Returns copies: a caller must not be able to write back into the live
        mirror, and this is read from a request handler while the WS callback
        thread may be updating it.
        """
        return {c: {r: dict(slot) for r, slot in slots.items()}
                for c, slots in self._device_signals.items()}

    def waveform_transport_stats_all(self) -> Dict[str, Dict[str, Any]]:
        """Per-circuit transport_stats for every accumulator built so far.

        In-memory only — safe to call from a request handler (/health/detail
        does no I/O, deliberately).
        """
        return {c: acc.transport_stats()
                for c, acc in self._chunk_accumulators.items()}

    def _on_transport_degraded(self, key: str, message: str) -> None:
        """Bridge an accumulator transport condition to worker_health.

        Reuses Orchestrator.mark_subsystem_degraded rather than inventing a
        second health surface: the name lands in /health/detail's ``unhealthy``
        list and the endpoint's status drops "pass" → "warn". Reporting only —
        no detection, actuation or storage path consults this.
        """
        cb = self._subsystem_degraded_cb
        if cb is None:
            log.warning("waveform transport degraded (%s): %s", key, message)
            return
        try:
            cb("waveform_transport", RuntimeError(message), detail=key)
        except Exception as e:  # pragma: no cover - never fatal
            log.warning("degraded-report failed (non-fatal): %s", e)

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
        # Provenance: how/when this circuit's state was established.
        self._valve_meta[circuit] = (
            "state_change", datetime.now(timezone.utc).isoformat())

    async def prime_valve_states(self) -> None:
        """Seed other-valve tracking from current HA state.

        Subscribe-then-prime: setup() wires the change subscriptions FIRST,
        then this fills ``self._valve_open`` for valves that never change state
        after boot. Without it the dict stays empty until the first valve
        transition, so ``_get_other_valve_open`` returns None ("unknown")
        indefinitely and ``other_valve_open`` can never record a confirmed 0 —
        an audit found the column was only ever 1 or NULL across all 6,124
        events. A change event racing in during priming wins (checked before
        AND after the await).

        Also primes the mirrored device-truth signals, rather than from a new
        call site: this method is already invoked on BOTH paths that follow
        ``setup()`` — orchestrator boot and the setup wizard's completion
        handler — so there is no third place to remember.
        """
        try:
            await self.prime_device_signals()
        except Exception as e:      # pragma: no cover - never fatal
            # An observability prime must never cost us the valve-state seed
            # that this method exists for.
            log.warning("device-signal prime failed (non-fatal): %s", e)
        for cfg in self._circuits:
            if not cfg.valve_entity or cfg.circuit in self._valve_open:
                continue
            try:
                state = await self._ha.get_state_value(cfg.valve_entity)
            except Exception as e:
                log.debug("[%s] valve-state prime failed (non-fatal): %s",
                          cfg.circuit, e)
                continue
            s = str(state or "").lower()
            if s in ("open", "on"):
                seeded = True
            elif s in ("closed", "off"):
                seeded = False
            else:
                continue  # unknown/unavailable — leave unseeded
            # Recheck after the await: a real change event during the fetch
            # is fresher than the polled state and must not be overwritten.
            if cfg.circuit not in self._valve_open:
                self._valve_open[cfg.circuit] = seeded
                self._valve_meta[cfg.circuit] = (
                    "ha_prime", datetime.now(timezone.utc).isoformat())
                log.info("[%s] valve state primed: %s", cfg.circuit,
                         "open" if seeded else "closed")

    def _get_other_valve_open(self, this_circuit: str) -> Optional[bool]:
        """Return True if any other circuit's valve is currently open, False if all
        are closed, or None if no other valve states have been received yet."""
        others = {c: v for c, v in self._valve_open.items() if c != this_circuit}
        if not others:
            return None   # not yet observed
        return any(others.values())

    def _get_other_valve_meta(
            self, this_circuit: str) -> Optional[Tuple[str, str]]:
        """(source, set_at_iso) provenance for the aggregate the method above
        returns: the most recently established other-circuit state (in a
        two-circuit home there is exactly one). None when no other state has
        been observed."""
        metas = [m for c, m in self._valve_meta.items()
                 if c != this_circuit and c in self._valve_open]
        if not metas:
            return None
        return max(metas, key=lambda m: m[1])

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

    def waveform_transport_stats(self, circuit: str) -> Dict[str, Any]:
        """Per-circuit waveform-transport health counters (or zeros).

        The no-accumulator fallback carries every key transport_stats() emits,
        so a caller that reads one by name (routers/device.py, /health/detail)
        can't KeyError on an unconfigured circuit."""
        accumulator = self._chunk_accumulators.get(circuit)
        if accumulator is None:
            return {"assembled": 0, "degraded": 0, "gaps": 0,
                    "rejected_seq": 0, "overflow_evicted": 0,
                    "rejected_transport_version": 0,
                    "node_check_enabled": False, "expected_node": ""}
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


# --------------------------------------------------------------------------- #
# Back-compat surface for the split (PEP 562)                                 #
# --------------------------------------------------------------------------- #
# Everything below moved out of this file. Eager ``from .event_waveform import
# X`` re-export lines raise ImportError on one import order; a module
# ``__getattr__`` resolves LAZILY, so no order can catch a partially-initialised
# module. The fallback branch must keep RAISING — returning None there would
# make every ``hasattr`` in the suite answer True.
_MOVED_TO_CORE = (
    "StartTrigger",
    "_valve_meta_kwargs",
    "_read_addon_version",
    "_read_git_commit",
    "_ADDON_VERSION",
    "_GIT_COMMIT",
    "_PROP_MAX_LOOKBACK_S",
    "_PROP_BASELINE_GUARD_S",
    "_PROP_MA_HALF_S",
    "_PROP_NOISE_BAND",
    "_PROP_ABOVE_RUN",
    "_PROP_MIN_BASELINE_SAMPLES",
    "_median",
    "PropagationScanResult",
    "scan_propagation_delay",
)

_MOVED_TO_WAVEFORM = (
    "_WF_START_SAMPLES",
    "_WF_MAX_RECORDS",
    "_WF_FLAG_VALID_MASK",
    "_WF_INFLIGHT_TTL_S",
    "_WF_FINAL_GAP_TIMEOUT_S",
    "_WF_FL_RESOLUTION_REDUCED",
    "_WF_MAX_CHUNK_SAMPLES",
    "_WF_MAX_TOTAL_CHUNKS",
    "_WF_MAX_INFLIGHT_SETS",
    "_WF_INFLIGHT_LOW_WATER",
    "WaveformMetadata",
    "_InflightChunkSet",
    "_parse_wire_bool",
    "_normalize_int_field",
    "_parse_chunk_scalars",
    "_parse_final_metadata",
    "_decode_waveform",
    "_normalize_node_name",
)


def __getattr__(name: str):
    if name in _MOVED_TO_CORE:
        from . import event_detector_core as _m
        return getattr(_m, name)
    if name in _MOVED_TO_WAVEFORM:
        from . import event_waveform as _m
        return getattr(_m, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
