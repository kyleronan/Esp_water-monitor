"""Feature-extractor SERVICE half: ``FeatureExtractor``, the queue-consuming,
DB-writing, alert-firing service only :mod:`orchestrator` imports. The pure
computation half lives in :mod:`feature_extractor`.

The dependency runs ONE WAY (this module imports from ``feature_extractor``,
never the reverse). The back-compat re-export in ``feature_extractor`` must
stay a lazy PEP 562 ``__getattr__``: an eager import closes the loop and raises
ImportError when this module is imported first, as ``orchestrator`` does. The
``database`` <-> ``feature_extractor`` cycle sits on the pure half and is lazy
on both sides.
"""
from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from .event_detector import RawEvent, WaveformRecord
from .feature_extractor import (
    NO_TIER_MATCHED_REASON,
    _SEQUENCE_GAP_MAX_S,
    _WF_MATCH_MIN_SCORE,
    _WF_MATCH_WINDOW_S,
    _enrich_from_waveform,
    _events_has_column,
    _finalize_derived_verdicts,
    _late_waveform_upgrade_job,
    _persist_waveform,
    _wf_already_claimed,
    _wf_millis_sub,
    _wf_overlap_score,
    extract_features,
    # The class's log records keep the `...app.feature_extractor` channel name:
    # operators grep the add-on log for it.
    log,
)
from .config import DB_PATH, pump_gates_active, pump_gates_active as _pga
from .database import (
    drain_daily_summary_dirty,
    find_overlapping_event,
    get_circuit_type,
    get_home_profile,
    get_sensitivity_config,
    get_toilet_flush_cap_litres,
    is_baseline_locked,
    is_event_in_exclusion_window,
    is_retryable_db_error,
    match_event_to_signature_knn,
    record_training_candidate,
    run_db,
    run_isolated_write,
    upsert_event_and_apply_hourly_volume)


# Minimum gap between anomaly NOTIFY pushes per circuit (the shut-off path is
# governed separately by the persistent per-12h cap, not this cooldown).
# Real valve travel on this hardware is 38-62 s and the firmware allows 90 s
# before it calls a motor fault, so a shorter confirmation window would report
# a healthy slow close as a failure.
_VALVE_CONFIRM_TIMEOUT_S: float = 100.0
_VALVE_CONFIRM_POLL_S: float = 5.0
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
        # Home timezone — converts UTC-stored event timestamps to LOCAL
        # for the water-softener regen band match. None → compare in UTC (the
        # batch reclassify will still detect it once a tz-aware caller runs).
        self._ha_tz = ha_tz
        # Optional EventDetector — provides WaveformChunkAccumulator access
        # (firmware 3.9.0+). None when running in test / historical-import
        # contexts; _find_waveform handles the missing-detector case.
        self._event_detector = event_detector
        self._running = False
        # Strong references for fire-and-forget tasks (anomaly alerts). Without
        # them the only ref is whatever asyncio.create_task returns, and Python
        # may GC the task before it completes, silently dropping the alert.
        # add_done_callback also gives a place to log exceptions instead of the
        # default "Task exception was never retrieved" warning.
        self._pending_alert_tasks: set[asyncio.Task] = set()
        # Strong refs for the late-waveform upgrade tasks (Fix 1) — same GC-safety
        # pattern as the alert tasks above.
        self._pending_wf_tasks: set[asyncio.Task] = set()
        # Per-circuit cooldown timestamps for the pulsing-supply alert.
        # In-memory only — resets on addon restart, so a pulsing episode that
        # persists across a restart re-alerts the user.
        self._last_pulsing_alert_at: dict[str, datetime] = {}
        # Per-circuit cooldown for the Phase 2.3 anomaly NOTIFY path (in-memory is
        # fine — at worst a few extra notifications after a restart). The shut-off
        # rate limit is PERSISTENT (anomaly_shutoff_log), not in-memory.
        self._last_anomaly_alert_at: dict[str, datetime] = {}
        # Set by orchestrator after ClusterEngine is initialised and rebuilt.
        self.cluster_engine = None
        # Per-circuit circuit_type cache for the structural rules tier
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
                await self._maybe_drain_closed_days()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("Feature extractor error: %s", e, exc_info=True)

    def stop(self) -> None:
        self._running = False

    _CLOSED_DAY_DRAIN_EVERY_S = 60.0

    async def _maybe_drain_closed_days(self) -> None:
        """A reprocessed or backfilled event lands on a
        CLOSED day, whose cached summary then reads low until the 03:00 pruner
        pass. Recompute the dirty closed days as soon as the queue drains, and
        at most once a minute during a long backfill (once per distinct day, not
        per event). Today's open day keeps the nightly path
        (drain_daily_summary_dirty skips it). Best-effort."""
        import time as _time
        now = _time.monotonic()
        last = getattr(self, "_closed_day_drain_at", 0.0)
        if not self._queue.empty() and now - last < self._CLOSED_DAY_DRAIN_EVERY_S:
            return
        self._closed_day_drain_at = now
        try:
            res = await run_db(drain_daily_summary_dirty, self._db)
            if res.get("recomputed"):
                log.info("daily summary refreshed for %d closed day(s) after a store",
                         res["recomputed"])
        except Exception as e:                              # noqa: BLE001
            log.debug("closed-day summary drain skipped: %s", e)

    async def _enrich_propagation_delay(self, event: RawEvent) -> None:
        """Sharpen propagation_delay_ms (flow_onset − true transient onset) with
        the flow-onset entity's server-side last_changed from HA history.

        Only the flow side is refined: HA's recorder does not keep the 40 Hz
        pressure sensor at full resolution, so the buffer scan stays
        authoritative for the pressure onset. For a 'pressure' trigger the flow
        onset is flow_onset_ts (start_ts is the threshold crossing, not the
        transient onset); for 'flow' / 'pressure+flow' it is start_ts.
        """

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
        Find the buffered WaveformRecord that best overlaps this RawEvent.

        Scans ALL buffered records, not just the latest: a pulsed fixture
        (washer fill pauses) splits one event into several firmware captures,
        and a tiny trailing capture can land AFTER the spanning one (a washer's
        full=7249 capture was superseded by a 9-second one), so "latest"
        discards the real match. Returns the best duration-overlap record
        assembled within _WF_MATCH_WINDOW_S when its score reaches
        _WF_MATCH_MIN_SCORE, else None; records already claimed by a stored
        event are skipped so the runner-up can still win.
        """
        import time as _time

        if self._event_detector is None:
            return None
        try:
            records = self._event_detector.get_recent_waveforms(event.circuit)
        except Exception:
            return None
        if not records:
            return None
        now_mono = _time.monotonic()
        best: Optional[WaveformRecord] = None
        best_score = -1.0
        for record in records:
            # Recency guard: a record assembled too long ago belongs to a
            # previous event, not this one.
            if now_mono - record.received_at > _WF_MATCH_WINDOW_S:
                continue
            # Exclusivity: one capture enriches one event. Skipping (rather
            # than rejecting after the fact) lets the runner-up record win.
            if _wf_already_claimed(self._db, event.circuit,
                                   record.metadata.boot_id,
                                   record.metadata.event_id):
                continue
            score = _wf_overlap_score(event, record)
            if score > best_score:
                best, best_score = record, score
        if best is None:
            log.debug(
                "[%s] waveform skip — %d buffered record(s), none within %.0fs",
                event.circuit, len(records), _WF_MATCH_WINDOW_S,
            )
            return None
        if best_score < _WF_MATCH_MIN_SCORE:
            log.debug(
                "[%s] waveform skip — best event_id=%d overlap=%.2f < %.2f "
                "(event=%.1fs fw=%.1fs)",
                event.circuit, best.metadata.event_id, best_score, _WF_MATCH_MIN_SCORE,
                (event.end_ts - event.start_ts).total_seconds() if event.end_ts else 0.0,
                (
                    _wf_millis_sub(best.metadata.end_ms, best.metadata.start_ms)
                    + best.metadata.tail_ms
                ) / 1000.0,
            )
            return None
        return best

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

    def _pre_store_reads_sync(self, event) -> dict:
        """Pump-era/gate state plus the waveform lookup, one hop."""
        from .supply_regime import pump_era_start
        try:
            era = pump_era_start(self._db)
        except Exception:
            era = None
        try:
            gates = pump_gates_active(self._db, event.circuit)
        except Exception:
            gates = False
        return {"era": era, "pump_gates": gates,
                "wf_record": self._find_waveform(event)}

    def _store_preflight_sync(self, event, event_id: str) -> dict:
        """The duplicate guard and the stored-intent read.

        Both are reads taken as close to the INSERT as the architecture
        allows; bundling them keeps that property while removing two
        loop-thread touches.
        """
        blocking = None
        if event.end_ts is not None:
            blocking = find_overlapping_event(
                self._db, event.circuit,
                event.start_ts.isoformat(),
                event.end_ts.isoformat(),
                exclude_event_id=event_id,
            )
        # The pin columns ride along so the finalizer can honour them
        # on a re-store; guarded for hand-rolled test schemas that predate them.
        _pin_cols = (", verdict_pin, verdict_pin_veff"
                     if _events_has_column(self._db, "verdict_pin") else "")
        existing = self._db.execute(
            "SELECT user_ignored, user_classified, "
            "       is_pressure_restoration_phantom, degraded_supply, "
            "       is_cross_talk, is_low_flow_dribble, "
            "       is_composite, volume_litres_effective" + _pin_cols + " "
            "FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        return {"blocking": blocking, "existing": existing}

    def _verdict_inputs_sync(self, circuit) -> dict:
        """Frozen artifact calibration + pump-gate state."""
        from .artifact_calibration import load_artifact_calibration
        acal = load_artifact_calibration(self._db, circuit)
        try:
            pump = pump_gates_active(self._db, circuit)
        except Exception:
            pump = False
        return {"acal": acal or None, "pump": pump}

    def _post_store_sync(self, event, features: dict, wf_record, wf_applied,
                         is_new_event) -> None:
        """Post-upsert DB work, one hop, one transaction.

        Mutates ``features`` in place (exclusion flags) — safe because the
        caller is awaiting this call and nothing else reads the dict meanwhile.
        """
            # ── Plumbing-event exclusion window ───────────────
        # If the user opened an exclusion window (e.g. post-winterization
        # flush), flag the event so the cluster engine skips it. Volume tracking
        # continues — only fixture identification is excluded. Preserves any
        # upstream match_rejection_reason (e.g. 'pulsing_supply'); only stamps
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
        # A record rejected by the sanity gate describes a different draw —
        # it must not substitute the display envelope either, or the modal
        # shows another event's waveform.
        _persist_waveform(
            self._db,
            features["id"],
            event.flow_readings,
            event.pressure_readings,
            float(features.get("duration_seconds") or 0),
            esp_record=wf_record if wf_applied else None,
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


    def _post_cluster_sync(self, circuit: str, features: dict) -> dict:
        """Training-capture hook + the live-state read, one hop.

        The capture hook is best-effort exactly as it was inline: a bug in
        capture logic must never block event storage.
        """
        try:
            record_training_candidate(self._db, circuit, features)
        except Exception as e:      # noqa: BLE001 — never block storage
            log.warning("[%s] training-capture hook failed (non-fatal): %s",
                        circuit, e)
        row = self._db.execute(
            "SELECT state FROM training_state WHERE circuit = ?",
            (circuit,)).fetchone()
        return {"state": row["state"] if row else None}

    def _degraded_count_sync(self, circuit: str, cutoff_30min: str) -> int:
        """Degraded-event count for the pulsing-supply limiter."""
        row = self._db.execute(
            "SELECT COUNT(*) FROM events "
            "WHERE circuit = ? AND degraded_supply = 1 "
            "AND start_ts >= ?",
            (circuit, cutoff_30min),
        ).fetchone()
        return int(row[0]) if row else 0

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
        # VFD ripple exemption (see _VFD_RIPPLE_MAX_PERIOD_S). Live and
        # retroactive verdicts must answer the SAME question, so the live path
        # takes the era predicate OR'd with current gate state. Keep the gate
        # term even though the OR is permanently true once an era is pinned:
        # it is the only branch that fires with pump mode confirmed but no era
        # pinned yet.
        _pre = await run_db(self._pre_store_reads_sync, event)
        _pump_ripple = False
        try:
            _era = _pre["era"]
            _pump_ripple = bool(_pre["pump_gates"]) or (
                _era is not None
                and (event.start_ts.isoformat() if hasattr(event.start_ts,
                                                           "isoformat")
                     else str(event.start_ts)) >= _era)
        except Exception as e:      # never fatal — worst case: legacy behavior
            log.debug("[%s] pump-era resolve failed (non-fatal): %s",
                      event.circuit, e)
        features = extract_features(event, min_flow_lpm=min_flow_lpm,
                                    pump_mode=_pump_ripple)

        # Attempt to enrich features from ESP waveform capture (firmware 3.7.0+).
        # Per-group routing: each group falls back to the legacy value independently.
        wf_record = _pre["wf_record"]
        wf_applied = False
        if wf_record is not None:
            score = _wf_overlap_score(event, wf_record)
            wf_applied = _enrich_from_waveform(features, wf_record, score)
            # Enrichment overwrites pressure_delta_psi / peak_flow_lpm with
            # ESP-measured values. The phantom verdict is re-derived below
            # (after the existing-row read), so a real long event — e.g. a
            # 40-min shower whose pressure drop wasn't captured in the software
            # pass — is NOT left with a stale phantom flag + zeroed volume.
            if wf_applied:
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

            # Writer-boundary duplicate guard: two importer catch-up runs can
            # both queue a reconstruction before either has written to the DB,
            # so the importer-side check alone cannot prevent the race. Check
            # here too, as close to the INSERT as the architecture allows. The
            # duplicate guard and the stored-intent read are both reads with
            # nothing but pure logic between them — one hop.
            _pf = await run_db(self._store_preflight_sync, event,
                               features["id"])
            blocking = _pf["blocking"]
            existing = _pf["existing"]
            if event.end_ts is not None:
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

            # ── Honour stored user intent ──────────────────────
            # extract_features() knows nothing of stored user choices, so apply
            # them from the existing row BEFORE the upsert: user_ignored folds
            # into excluded_from_training (a derived column the upsert does not
            # preserve, so a re-import would silently un-ignore the event), and
            # user_classified copies the stored flags and SKIPS the auto
            # finalizer so enrichment never overrides a manual classification.
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
                # Carry the pinned verdict into the re-derive (the
                # upsert preserves the columns; the finalizer must SEE them).
                if existing is not None and "verdict_pin" in existing.keys():
                    features["verdict_pin"] = existing["verdict_pin"]
                    features["verdict_pin_veff"] = existing["verdict_pin_veff"]
                _vi = await run_db(self._verdict_inputs_sync,
                                   features.get("circuit"))
                _acal, _pump = _vi["acal"], _vi["pump"]
                _finalize_derived_verdicts(features, _acal or None,
                                           min_flow_lpm=min_flow_lpm,
                                           pump_gates=_pump)

            # Atomic upsert + hourly_volume update. Uses
            # volume_litres_effective (which is volume_litres for healthy
            # events, or the envelope-smoothed estimate for degraded ones).
            # Idempotent: re-imports subtract the prior contribution before
            # adding the new value, so no double-counting.
            effective_volume = float(features.get("volume_litres_effective") or 0)
            # Lock-tolerant store: a long admin write (the ~30 s "Apply my
            # labels" reclassify) holds the DB past busy_timeout and a live
            # event's store then fails "database is locked". Retries with async
            # sleeps so the event loop stays responsive; total wait comfortably
            # exceeds the longest observed admin write.
            is_new_event = None
            for _attempt in range(6):
                try:
                    # One run_db hop PER ATTEMPT. The retry loop
                    # and its sleep stay on the event loop deliberately —
                    # sleeping inside the DB worker would block the single
                    # thread this retry is waiting on, turning a recoverable
                    # lock into a self-inflicted deadlock.
                    is_new_event = await run_db(
                        upsert_event_and_apply_hourly_volume,
                        self._db, features, effective_volume,
                    )
                    break
                except sqlite3.DatabaseError as _db_err:
                    # DatabaseError, not OperationalError: "another row
                    # available" is a bare DatabaseError, a SIBLING of
                    # OperationalError. A narrower catch here lets it through to
                    # the outer handler, which logs it and DROPS the event — a
                    # real draw was lost that way.
                    if not is_retryable_db_error(_db_err) or _attempt == 5:
                        raise
                    log.info("[%s] event store hit a transient DB fault (%s) "
                             "— retry %d/5 in 8 s",
                             event.circuit, _db_err, _attempt + 1)
                    await asyncio.sleep(8)

            # Exclusion window, calibration exclusion, waveform
            # persist and the training-state increment are one contiguous run
            # of DB work — ONE hop, one transaction (N2a). It mutates
            # ``features`` in place; that is safe because the loop is awaiting
            # this call, so there is no concurrent reader.
            await run_db(self._post_store_sync, event, features, wf_record,
                         wf_applied, is_new_event)

            # Consume the capture so no later event can claim it (the durable
            # ledger covers restarts; this also drops it from the in-memory
            # buffer immediately). Only on success — a rejected record may
            # still legitimately belong to the NEXT event to finalise.
            if wf_applied and self._event_detector is not None:
                try:
                    self._event_detector.pop_waveform_record(
                        event.circuit,
                        wf_record.metadata.boot_id,
                        wf_record.metadata.event_id,
                    )
                except Exception as e:
                    log.debug("[%s] waveform consume failed (non-fatal): %s",
                              event.circuit, e)


            # ── Sequence context + cluster matching ───────────────
            await self._cluster_event(event.circuit, features)

            # Training-helper capture: if a capture is armed on this circuit,
            # record the event as a candidate (writes NO label — the user
            # confirms in the wizard). Cheap: one indexed SELECT, free when
            # idle. No re-check after the _cluster_event await:
            # record_training_candidate re-reads the armed-capture row at write
            # time (that IS its gate), so a capture disarmed during clustering
            # records nothing.
            _post = await run_db(self._post_cluster_sync, event.circuit,
                                 features)
            _live_state = _post["state"]

            # ── Anomaly response (frozen-baseline deviation) ─────────
            # The verdict was scored + stored in _cluster_event; here the user's
            # graduated response is applied, but ONLY in the locked 'live'
            # state. A circuit calibrating / labelling / mid-recalibration has no
            # trustworthy baseline → no notify, no shut-off. Shut-off carries
            # extra guardrails (see _apply_anomaly_response).
            am = self._alert_manager
            anomaly = features.get("_anomaly") or {}
            if am and anomaly.get("is_anomalous"):
                if _live_state == "live":
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
                    # A READ after the anomaly-response await.
                    # No re-check needed — re-checks guard stale WRITES, and
                    # this only feeds a rate-limited alert decision.
                    count = await run_db(self._degraded_count_sync,
                                         event.circuit, cutoff_30min)
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
            # This drops a real measurement. Say so — "failed to store event"
            # reads like a skipped step, and the volume it carried is missing
            # from every total downstream until the catch-up importer re-derives
            # it from HA history.
            log.error("[%s] EVENT LOST — could not store it after retries: %s. "
                      "Its volume is missing from totals until the historical "
                      "importer re-derives it from HA history.",
                      event.circuit, e, exc_info=True)

    def _score_anomaly(self, circuit: str, features: dict) -> dict:
        """Score an event against the FROZEN baseline (Phase 2.3). Read-only — the
        notify / shut-off response is applied separately in ``_process`` behind a
        'live' state gate. Returns the inert verdict for artifact / excluded events
        or when no baseline exists."""
        from .anomaly_baseline import load_usage_baselines, score_event_anomaly
        baselines = load_usage_baselines(self._db, circuit)
        sens = get_sensitivity_config(self._db, circuit)
        return score_event_anomaly(features, baselines, sens)

    async def _apply_anomaly_response(self, circuit: str, features: dict,
                                      anomaly: dict) -> None:
        """Graduated response to a LIVE baseline-deviation event.

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
        from .anomaly_baseline import _row_get, MIN_LIVE_DAYS_FOR_SHUTOFF
        sens = await run_db(get_sensitivity_config, self._db, circuit)
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
        # The two DB-backed shut-off gates go over the wall
        # together, and ONLY when want_shutoff is true — evaluating them
        # eagerly would preserve thread-safety but change behaviour, adding
        # two reads to every anomaly event on a notify-only install. The
        # seasoned check is pure (it reads the already-fetched sens row).
        if (want_shutoff
                and self._anomaly_seasoned(sens, MIN_LIVE_DAYS_FOR_SHUTOFF)
                and await run_db(self._anomaly_shutoff_gates_sync, circuit,
                                 sens)):
            if await self._auto_shutoff(circuit, circuit_name, score, atype, event_id):
                return   # the shut-off path already notified (why + reopen)
        # Off-ramp: degrade to / default notify, rate-limited so it cannot spam.
        self._notify_anomaly(circuit, circuit_name, score, atype, event_id)

    def _anomaly_shutoff_gates_sync(self, circuit: str, sens) -> bool:
        """The DB-backed shut-off gate, one hop.

        Deliberately NO per-12h rate limit: the add-on has no automatic reopen
        (the only open path is the manual, operator-gated
        `/device/valve/{circuit}/open` route), so such a counter could only
        reach 2 if the operator reopened in between — it would bound overruling
        a human, not guard an unnoticed runaway. A shut valve is the loudest
        notification the system has. ``anomaly_shutoff_log`` is still written
        as the audit record that the valve was physically closed.
        """
        return self._anomaly_shutoff_state_ok(circuit)

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

    def _anomaly_shutoff_state_ok(self, circuit: str) -> bool:
        """HARD safety gate — an automated valve close is permitted ONLY when the
        circuit is 'live' AND outside any (re)calibration / accelerated-adaptation
        window: setup, learning, labelling, full recalibration (state ≠ 'live')
        and partial recalibration (stays 'live' but opens a 14-day adaptation
        window) all block it, so the water is never cut while the system is
        still (re)learning what normal looks like. Delegates to
        ``database.is_baseline_locked``, the ONE definition of "baseline locked"
        (shared with the label-reclassify skip gate) so the two cannot drift."""
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

    def _resolve_role_entity(self, circuit: str, role: str) -> Optional[str]:
        """Any bound entity for a circuit, by discovery role."""
        row = self._db.execute(
            "SELECT entity_id FROM circuit_entity_map "
            "WHERE circuit = ? AND role = ?", (circuit, role)).fetchone()
        return row[0] if row and row[0] else None

    async def _confirm_valve_closed(self, circuit: str, circuit_name: str,
                                    valve: str, event_id) -> Optional[bool]:
        """Read the valve position back after commanding a close.

        ``close_valve`` returning True means HTTP 200 — "HA accepted the
        request", not "the valve closed" — and this installation has a
        documented close-path hardware fault, so without this read-back the
        add-on would report the water shut off while it is still running.

        Returns True (confirmed shut), False (did not confirm), or None (no
        end-stop entity bound — cannot verify, and says so). Two distinct faults
        are checked: the closed end stop never reads on (the valve did not
        travel), and the end stop reads on but ``valve_seal_alert`` is also on
        (shut, yet water still moving past it).
        """
        stop_entity = await run_db(
            self._resolve_role_entity, circuit, "closed_end_stop_sensor")
        if not stop_entity:
            log.warning(
                "[%s] valve close NOT VERIFIED — no closed-end-stop entity is "
                "bound, so the add-on cannot tell whether the valve moved. The "
                "notification says the close was commanded, which is all that "
                "is known.", circuit)
            return None

        seal_entity = await run_db(
            self._resolve_role_entity, circuit, "valve_seal_alert_sensor")

        # Monotonic, not wall clock: an NTP step on a host without an RTC would
        # otherwise stretch or truncate this window (audit §8E.5).
        deadline = time.monotonic() + _VALVE_CONFIRM_TIMEOUT_S
        seated = False
        while time.monotonic() < deadline:
            try:
                state = await self._ha.get_state_value(stop_entity)
            except Exception as exc:                       # noqa: BLE001
                log.warning("[%s] end-stop read failed while confirming the "
                            "close: %s", circuit, exc)
                state = None
            if str(state).lower() == "on":
                seated = True
                break
            await asyncio.sleep(_VALVE_CONFIRM_POLL_S)

        leaking = False
        if seated and seal_entity:
            try:
                seal = await self._ha.get_state_value(seal_entity)
                leaking = str(seal).lower() == "on"
            except Exception as exc:                       # noqa: BLE001
                log.warning("[%s] valve-seal read failed: %s", circuit, exc)

        if seated and not leaking:
            log.info("[%s] valve close CONFIRMED at the closed end stop (%s)",
                     circuit, stop_entity)
            return True

        why = ("the valve reports shut but flow is still detected past it "
               "(valve seal alert)" if leaking else
               "the closed end stop never reported seated within %.0f s"
               % _VALVE_CONFIRM_TIMEOUT_S)
        log.error("[%s] VALVE CLOSE NOT CONFIRMED — %s. Water may still be "
                  "flowing (event %s).", circuit, why, event_id)

        am = self._alert_manager
        if am:
            await am.fire(
                circuit, "unusual_usage",
                title=f"\u26a0\ufe0f Water may still be running \u2014 {circuit_name}",
                message=(
                    f"The add-on commanded an automatic shut-off for "
                    f"{circuit_name}, but could not confirm it: {why}. "
                    f"Treat the water as STILL ON and check the valve. "
                    f"The earlier notification said the shut-off was "
                    f"commanded, not that it completed."),
                # Distinct id so this does NOT overwrite the shut-off notice.
                notification_id=f"water_shutoff_unconfirmed_{circuit}",
                # critical bypasses the per-type enable: an unverified close on
                # a leak is exactly the case a user preference must not mute.
                critical=True)
        return False

    def _shutoff_preflight_sync(self, circuit: str) -> dict:
        """The two DB reads that must precede actuation.

        Returned together so the hard state gate and the valve lookup describe
        the same instant; the caller still checks the gate first.
        """
        return {"state_ok": self._anomaly_shutoff_state_ok(circuit),
                "valve":    self._resolve_valve_entity(circuit)}

    def _log_shutoff_sync(self, circuit: str, event_id, atype,
                          score: float) -> None:
        """Append the shut-off audit row, on the DB thread.

        closed_at is an explicit UTC ISO timestamp, NOT the CURRENT_TIMESTAMP
        default: SQLite renders that 'YYYY-MM-DD HH:MM:SS', and ' ' (0x20) sorts
        below 'T' (0x54), so a space-format row compares EARLIER than every
        T-format row whatever instant it records and range queries mis-select.
        """
        self._db.execute(
            "INSERT INTO anomaly_shutoff_log "
            "    (circuit, event_id, anomaly_type, score, closed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (circuit, event_id, atype, score,
             datetime.now(timezone.utc).isoformat()))
        self._db.commit()

    async def _auto_shutoff(self, circuit: str, circuit_name: str, score: float,
                            atype, event_id) -> bool:
        """Close the valve, log it (persistent), and notify with why + a one-action
        reopen. Returns False (→ caller falls back to notify) when no valve is
        configured or the close fails — a shut-off must never silently swallow."""
        # HARD final gate at the actuation chokepoint: the valve can NEVER
        # close unless the circuit is live and settled (not learning / setup /
        # recalibrating), independent of how this method was reached. The gate
        # and the valve lookup are both DB reads that must happen immediately
        # before actuation, so they cross together in ONE hop — the gate stays
        # the last thing checked before the valve moves.
        gate = await run_db(self._shutoff_preflight_sync, circuit)
        if not gate["state_ok"]:
            log.warning("[%s] anomaly auto-shutoff refused — circuit is not in a live, "
                        "settled state (learning / setup / recalibrating)", circuit)
            return False
        valve = gate["valve"]
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
        # Deliberately NO state re-check after the valve await: the valve is
        # ALREADY CLOSED, and this append-only row (the log only grows, so no
        # interleaved write can make it wrong or redundant) is the only durable
        # record of that physical action — a re-check that could skip the
        # write would make the add-on's bookkeeping lie about the valve.
        await run_db(self._log_shutoff_sync, circuit, event_id, atype, score)
        log.warning("[%s] ANOMALY AUTO-SHUTOFF — closed valve %s (event %s, %s, "
                    "score %.2f)", circuit, valve, event_id, atype, score)
        am = self._alert_manager
        if am:
            self._spawn_alert_task(am.alert_unusual_usage(
                circuit, score, atype, circuit_name, shutoff=True,
                event_id=event_id, valve_entity=valve))
        # Verify OUT OF BAND. Travel is 38-62 s and the confirmation window is
        # 100 s; awaiting that here would hold the event pipeline for the whole
        # of it. The operator is already notified above, so the only thing
        # still owed is the truth about whether the valve actually moved.
        self._spawn_alert_task(
            self._confirm_valve_closed(circuit, circuit_name, valve, event_id))
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
        """Compute sequence context, run cluster matching, write results back.

        The body is ~360 lines of DB work whose ONLY await was
        the matcher hop, so the whole thing goes over the wall as ONE
        callable rather than 23 separate loop-thread touches. Bundling also
        makes the write-back atomic: sequence context, cluster assignment,
        suggestion recompute and fixtures.last_seen_at now commit together
        (rule N2a) instead of as a scatter of independent statements.
        """
        await run_db(self._cluster_event_sync, circuit, features)

    def _cluster_event_sync(self, circuit: str, features: dict) -> None:
        """The clustering write-back, on the single DB thread."""
        if features.get("excluded_from_training"):
            # Record WHY, instead of returning in silence: without it an
            # excluded row and a row the classifier evaluated and declined are
            # indistinguishable downstream. Never clobber an artifact reason,
            # which is both more specific and the cause of the exclusion.
            if not features.get("match_rejection_reason"):
                try:
                    self._db.execute(
                        "UPDATE events SET match_rejection_reason = "
                        "'excluded_from_training' "
                        "WHERE id = ? AND match_rejection_reason IS NULL",
                        (features.get("id"),))
                    self._db.commit()
                except Exception as e:      # visibility must never be fatal
                    log.debug("[%s] excluded-reason stamp failed: %s", circuit, e)
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
                    # We are ALREADY on the DB thread, so
                    # the matcher is called directly. Re-submitting to run_db
                    # from inside a run_db callable would deadlock the single
                    # worker — the no-re-entry rule, and this is exactly the
                    # site where a mechanical conversion would have broken it.
                    (cluster_id_result, match_confidence, match_level,
                     match_rejection_reason) =                         self.cluster_engine.match_and_learn(
                            dict(event_row),
                            circuit,
                            prev_cluster_id,
                            seconds_since_prev,
                        )
            except Exception as e:
                log.error("[%s] cluster matching failed: %s", circuit, e,
                          exc_info=True)

        # anomaly_score / anomaly_type / flagged hold the FROZEN-BASELINE
        # deviation verdict, computed below once the type is known (see
        # _score_anomaly + the write-back UPDATE). Do NOT reinstate the old
        # match-confidence score (1.0 - confidence): it fires on anything that
        # doesn't strongly match a known fixture, which is most of what a real
        # home produces.

        # Structural rules tier (rules-first; Pass-5 semantics). Runs
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
            from .rule_calibration import load_rule_calibration
            ctype = self._circuit_type_cache.get(circuit)
            if ctype is None:
                ctype = get_circuit_type(self._db, circuit)
                self._circuit_type_cache[circuit] = ctype
            # Frozen per-home rule bands (empty → shipped defaults). Read fresh so
            # an activation / recalibration takes effect on the next event.
            # Regime-aware: a live event belongs to the CURRENT supply regime,
            # so its bands win when fitted (fallback: legacy regime-0 row).
            from .supply_regime import get_current_regime_id
            calib = load_rule_calibration(self._db, circuit,
                                          regime_id=get_current_regime_id(self._db))
            # Water-softener session (precedence: softener → washer →
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
            # Dishwasher cycle: scan only when THIS event is a gentle small fill
            # (the 2.5 h lookback spans a full cycle). The pre-gate is loose —
            # the detector applies the precise calib-aware band and needs >=3
            # chained fills — but DERIVED from the detector's own calib values
            # (×1.4 slack): a hardcoded bound would stop invoking it for homes
            # whose fitted DW_* band is wider, flipping labels between the live
            # path and batch reclassify.
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
                try:
                    _pump = _pga(self._db, circuit)
                except Exception:
                    _pump = False
                # Burst context for the toilet rule's appliance veto. IMMATURE
                # by necessity, the same constraint the model tier below works
                # under: a washer's FIRST fill has no siblings yet, so the veto
                # cannot fire live on it — it fires on the batch re-derive once
                # the rest of the cycle exists. Failing to read context must
                # never cost the claim, so the veto abstains rather than guesses.
                _burst = None
                try:
                    from . import burst_features as _bf
                    _burst = _bf.compute_for_events(
                        self._db, circuit, [event_id],
                        config=_bf.CONFIG_IMMATURE).get(event_id)
                except Exception as _bf_exc:           # noqa: BLE001
                    log.debug("[%s] burst context unavailable for %s: %s",
                              circuit, event_id, _bf_exc)
                rule_hit = rule_classify_event(features, ctype, calib=calib,
                                               pump_mode=_pump, burst=_burst)
                if rule_hit is not None:
                    matched_fixture_type, matched_via = rule_hit
                else:
                    # Same reporting as the batch pass. Live there is no run to
                    # summarise, so DEBUG only; the aggregate appears on the
                    # re-derive, which is also where the veto does most of its
                    # work — a washer's first fill has no siblings yet.
                    from .event_rules import toilet_burst_veto_reason
                    _bw = toilet_burst_veto_reason(features, calib, _pump, _burst)
                    if _bw:
                        log.debug("[%s] event %s: toilet match vetoed by burst "
                                  "context — %s", circuit, event_id, _bw)
        except Exception as e:
            log.warning("[%s] structural rules tier failed (non-fatal): %s",
                        circuit, e)

        # TinyModel tier sits between the label-free anchors above and the k-NN
        # residual below by design: the anchors work on day one, the per-home
        # model beats the ladder's house-tuned scales once it has labels, and
        # the k-NN catches what the model abstains on. `immature` burst
        # features by necessity — a fill's siblings have not happened yet; the
        # deferred re-classify revisits with `mature` ones.
        if matched_fixture_type is None:
            try:
                from . import tinymodel as _tm
                hit = _tm.classify(self._db, circuit, event_id, features,
                                   burst_config=_tm.bf.CONFIG_IMMATURE)
                if hit is not None:
                    matched_fixture_type, _conf = hit
                    matched_via = "tinymodel"
                    match_confidence = _conf
                    log.info("[%s] event %s matched %s by tinymodel "
                             "(confidence %.2f)", circuit, event_id,
                             matched_fixture_type, _conf)
            except Exception as e:
                log.warning("[%s] tinymodel tier failed (non-fatal): %s",
                            circuit, e)

        # Signature-matcher (k-NN) residual. Runs when no structural
        # rule claimed the event AND the cluster matcher either returned no
        # cluster_id or a low-confidence match. Caught broadly because
        # matcher-or-DB failure must NEVER block the regular cluster_id write.
        weak_match = (
            cluster_id_result is None
            or (match_confidence is not None and match_confidence < 0.5)
        )
        # Fingerprint tier — whole-waveform NN against USER-labeled events at a
        # tight self-calibrated threshold. Runs under
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
                    matched_via = ("knn_invariant"
                                   if sig_hit.get("match_source")
                                   == "invariant_features" else "knn")
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

        # Toilet physics veto: whatever tier proposed 'toilet' (rule /
        # fingerprint / k-NN), the event must be physically able to BE a single
        # cistern refill — hard volume floor, era-capped ceiling (EPA flush
        # standards keyed on home_profile.build_year), peak floor, one segment.
        # Vetoed → abstain (never re-guess another type). Never fatal.
        if matched_fixture_type == "toilet":
            try:
                from .event_rules import toilet_veto_reason
                cap = get_toilet_flush_cap_litres(self._db)
                why = toilet_veto_reason(features, cap)
                if why:
                    log.info("[%s] event %s: toilet match (%s) vetoed by flush "
                             "physics — %s (vol=%s L, peak=%s lpm)",
                             circuit, event_id, matched_via, why,
                             features.get("volume_litres"),
                             features.get("peak_flow_lpm"))
                    matched_fixture_type, matched_via = None, None
            except Exception as e:
                log.warning("[%s] toilet physics veto failed (non-fatal): %s",
                            circuit, e)

        # Score the event against the FROZEN baseline now that its type is
        # known, and persist the verdict. Stashed on `features` so the _process
        # response policy reads the same verdict without re-scoring. flagged=1
        # marks a genuine (non-artifact) anomaly. Side effects (notify /
        # shut-off) are NOT done here — only in _process, behind the 'live'
        # state gate.
        features["matched_fixture_type"] = matched_fixture_type
        anomaly = self._score_anomaly(circuit, features)
        features["_anomaly"] = anomaly

        # Record classification-tier abstention. Without a reason, "the
        # classifier looked and declined" is indistinguishable from "never
        # evaluated" — 933 events failed that way silently while the cluster
        # tier was dead. Only fills an EMPTY reason (artifact and cluster-tier
        # reasons are more specific and must win), and is cleared symmetrically
        # the moment a later pass matches the event.
        if matched_fixture_type is None and not match_rejection_reason:
            match_rejection_reason = NO_TIER_MATCHED_REASON
        elif (matched_fixture_type is not None
                and match_rejection_reason == NO_TIER_MATCHED_REASON):
            match_rejection_reason = None

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

        # Trailing retro-scan: cycles complete over time (washer ~45 min,
        # softener ~3 h, dishwasher ~2 h), so earlier members were classified
        # before the family reached its >=3-fill threshold and there is no
        # periodic reprocess on the live path. Cycle context outranks a
        # per-event machine match, so this MAY overwrite a prior knn/rule_*
        # match (e.g. a backwash mis-typed shower_tub); user labels are never
        # touched.
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
