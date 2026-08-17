"""
Training state machine.

States:
  idle         — no training in progress
  calibrating  — collecting events, timer running
  labelling    — training period ended, user reviewing clusters (Phase 2)
  live         — fixture library active, anomaly detection running

Phase 1 implements idle → calibrating → live directly
(skipping labelling until Phase 2 clustering is available).

Publishes HA sensor entities for each circuit:
  sensor.water_training_status_<circuit>
    state: idle / calibrating / labelling / live
    attrs: days_elapsed, days_remaining, events_collected,
           minimum_events, calibration_ends_at, percent_complete
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from .config import AddonConfig, compute_suggested_calibration_days, compute_minimum_events
from .database import (get_training_state, upsert_training_state,
                       get_home_profile, ensure_circuit_defaults,
                       get_circuit_type, get_event_cadence_seconds, run_db)
from .ha_client import HaClient

log = logging.getLogger(__name__)

# Startup defaults vs. a concurrent heavy admin write (dev34). A user-triggered
# regime recalibration / recompute runs on a private connection under the write
# lock and can hold the SQLite file lock for tens of seconds — longer than the
# 5 s busy_timeout. Before this, the resulting OperationalError crashed the
# whole supervised training task, which restarted every 5 s and collided again
# for the duration of the admin job (observed 2026-08-03: ~8 crash cycles).
# Wait it out instead — ensure_circuit_defaults is idempotent.
_INIT_LOCK_RETRIES: int = 12
_INIT_LOCK_BACKOFF_S: float = 5.0

# Auto-activate a circuit stuck in 'labelling' after this many days of
# user inaction, so anomaly detection isn't blocked indefinitely waiting
# for the user to review clusters.
LABELLING_AUTO_TIMEOUT_DAYS = 7

# When a circuit has passed its calibration deadline but hasn't collected enough
# events, re-checking happens every 60 s poll — but re-warning that often is
# pointless for a sparse circuit (e.g. irrigation that only runs every few days).
# Instead, stay quiet for roughly one observed inter-event interval between
# warnings. These bound that quiet period.
RECHECK_GAP_FACTOR       = 1.25        # head-room past the median inter-event gap
RECHECK_MIN_SECONDS      = 3600        # 1 h  — hard anti-spam floor
RECHECK_MAX_SECONDS      = 7 * 86400   # 7 d  — cap on the quiet period
RECHECK_FALLBACK_SECONDS = 12 * 3600   # 12 h — cold start (< 3 prior events)


def _compute_recheck_interval(cadence_seconds: Optional[float]) -> timedelta:
    """Quiet period before re-warning an under-target calibration, derived from
    the circuit's observed inter-event cadence and clamped to sane bounds."""
    if cadence_seconds is None:
        secs: float = float(RECHECK_FALLBACK_SECONDS)
    else:
        secs = cadence_seconds * RECHECK_GAP_FACTOR
    secs = max(float(RECHECK_MIN_SECONDS), min(secs, float(RECHECK_MAX_SECONDS)))
    return timedelta(seconds=secs)


class TrainingManager:
    """
    Manages the training state machine for all circuits.
    Runs a background task that checks progress every 60 seconds.
    """

    def __init__(self, cfg: AddonConfig, db: sqlite3.Connection,
                 ha: HaClient):
        self._cfg = cfg
        self._db = db
        self._ha = ha
        self._stop = asyncio.Event()
        # Per-circuit timestamp of the last "under target" calibration warning,
        # so a circuit past its deadline but short on events isn't re-warned on
        # every 60 s poll (see _compute_recheck_interval). In-memory by design:
        # one warning after a restart is fine, so this resets on restart.
        self._last_undertarget_warn: Dict[str, datetime] = {}
        # Set by orchestrator after ClusterEngine is initialised
        self.cluster_engine = None
        # Serializes cluster re-seeds. The re-seed runs ~1 min in an executor
        # thread on the shared SQLite connection; the Settings button's page
        # sits loading that whole time, so users re-click, and two concurrent
        # _work() bodies on one connection die with sqlite3.InterfaceError
        # ("bad parameter or other API misuse") mid-backfill — observed
        # 2026-08-15 11:56. One lock for ALL circuits: the engine + connection
        # are shared, so even different-circuit re-seeds must not overlap.
        self._reseed_lock = asyncio.Lock()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Background loop — check calibration progress every 60s."""
        # Initial setup. A heavy admin write (regime recalibration, recompute)
        # triggered while startup is still running can hold the SQLite write
        # lock for longer than the 5 s busy_timeout — and an OperationalError
        # here crashed the whole supervised task, which then retried every 5 s
        # and collided again for as long as the admin job ran. Wait it out
        # instead: the defaults are idempotent and nothing downstream needs
        # them in the first seconds.
        for circuit_cfg in self._cfg.circuits:
            for attempt in range(_INIT_LOCK_RETRIES):
                try:
                    # dev46 (46a): one hop per attempt; the backoff sleep
                    # stays on the loop so it never blocks the DB worker.
                    await run_db(ensure_circuit_defaults, self._db,
                                 circuit_cfg.circuit, circuit_cfg.circuit_type)
                    break
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e).lower():
                        raise
                    log.info("[%s] circuit defaults: DB busy (%s) — retry %d/%d",
                             circuit_cfg.circuit, e, attempt + 1,
                             _INIT_LOCK_RETRIES)
                    await asyncio.sleep(_INIT_LOCK_BACKOFF_S)
            else:
                log.warning("[%s] circuit defaults skipped — DB stayed locked; "
                            "the next startup applies them (idempotent)",
                            circuit_cfg.circuit)

        # Lower stale whole-home event targets for in-progress zone
        # calibrations so they aren't stuck forever (see method docstring).
        await run_db(self._reconcile_calibration_thresholds)

        # Initial publish
        for circuit_cfg in self._cfg.circuits:
            await self._publish_status(circuit_cfg.circuit)

        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
                return
            except asyncio.TimeoutError:
                pass

            for circuit_cfg in self._cfg.circuits:
                try:
                    await self._check_progress(circuit_cfg.circuit)
                    await self._publish_status(circuit_cfg.circuit)
                except Exception as e:
                    log.error("[%s] training manager error: %s",
                              circuit_cfg.circuit, e)

    def _reconcile_calibration_thresholds(self) -> None:
        """Lower stale ``minimum_events`` for in-progress calibrations.

        Earlier builds applied the whole-home event target to every
        circuit, leaving low-traffic zone (irrigation) circuits unable to
        ever reach it — they froze at a time-capped 100% forever. For any
        circuit still ``calibrating``, recompute the type-aware target and
        lower the stored value if it now sits below the old one. Only ever
        lower it, never raise, so a mid-run fixture circuit can't be
        re-stuck. ``started_at``/``calibration_ends_at``/``events_collected``
        are left untouched; the next ``_check_progress`` tick completes any
        circuit whose time has already elapsed.
        """
        profile = get_home_profile(self._db)
        for circuit_cfg in self._cfg.circuits:
            circuit = circuit_cfg.circuit
            state_row = get_training_state(self._db, circuit)
            if not state_row or state_row["state"] != "calibrating":
                continue
            circuit_kind = get_circuit_type(
                self._db, circuit, default=circuit_cfg.circuit_type)
            new_min = compute_minimum_events(
                profile["bathrooms_full"] or 2,
                profile["bathrooms_half"] or 0,
                profile["floors"] or 1,
                circuit_kind=circuit_kind,
            )
            current_min = state_row["minimum_events"] or 0
            if new_min < current_min:
                # Already ON the DB thread — this whole method is submitted via
                # run_db by its caller (N2b: no re-entry from inside a callable).
                upsert_training_state(
                    self._db, circuit, minimum_events=new_min)
                log.info(
                    "[%s] lowered calibration target %d → %d (%s circuit)",
                    circuit, current_min, new_min, circuit_kind)

    def _set_reseed_marker(self, circuit: str, active: bool) -> None:
        """dev42 (F-C2) — persisted reseed-in-progress marker. Set at
        clear-time, cleared ONLY on success: a crash mid-replay leaves it
        stamped, and the boot / post-rebuild health checks warn loudly that
        the model is untrusted until a rerun succeeds. Best-effort on a
        pre-20260808 schema."""
        from datetime import datetime, timezone
        try:
            self._db.execute(
                "UPDATE training_state SET reseed_in_progress = ? "
                "WHERE circuit = ?",
                (datetime.now(timezone.utc).isoformat() if active else None,
                 circuit))
            self._db.commit()
        except sqlite3.Error as e:
            log.warning("[%s] reseed marker write failed (non-fatal): %s",
                        circuit, e)

    def _clear_unconfirmed_clusters(self, circuit: str) -> int:
        """dev42 (U4) — delete this circuit's unconfirmed cluster rows AND
        null their event references in the same transaction, mirroring
        ``delete_cluster``'s semantics.

        The old bare DELETE left events pointing at the removed rows — and
        because the engine's id map is rebuilt from those events' stored
        votes, live matching kept assigning NEW events to the dead ids: the
        self-perpetuating orphan loop behind the 990 → 1,772
        events_orphaned climb (clusters 42/43 after the 8/15
        reseed→recalibrate sequence). Confirmed clusters
        (fixture_id IS NOT NULL) are untouched."""
        unref = self._db.execute(
            "UPDATE events SET cluster_id = NULL, match_confidence = NULL, "
            "       match_level = NULL "
            "WHERE circuit = ? AND cluster_id IN ("
            "  SELECT id FROM fixture_clusters "
            "  WHERE circuit = ? AND fixture_id IS NULL)",
            (circuit, circuit),
        ).rowcount
        deleted = self._db.execute(
            "DELETE FROM fixture_clusters "
            "WHERE circuit = ? AND fixture_id IS NULL",
            (circuit,),
        ).rowcount
        self._db.commit()
        if deleted:
            log.info("[%s] calibration start: cleared %d orphan cluster(s), "
                     "unreferenced %d event(s)", circuit, deleted, unref)
        return deleted

    def _profile_and_kind_sync(self, circuit: str, default_kind: str) -> dict:
        """dev46 (46a) — home profile + circuit kind, one hop."""
        return {"profile": get_home_profile(self._db),
                "kind": get_circuit_type(self._db, circuit,
                                         default=default_kind)}

    async def start_calibration(self, circuit: str,
                                calibration_days: int) -> bool:
        """
        Start calibration for a circuit. Returns True if started.

        Only starts a fresh calibration from 'idle'.  If the circuit is
        already calibrating with a valid ``started_at`` (e.g. the row was
        just restored from a backup and the setup wizard re-ran), this is
        a no-op so we don't clobber the existing timer or
        ``events_collected`` counter.
        """
        state_row = await run_db(get_training_state, self._db, circuit)
        current = state_row["state"] if state_row else "idle"

        if current not in ("idle", "calibrating", "labelling"):
            log.warning("[%s] cannot start calibration from state '%s'",
                        circuit, current)
            return False

        # If calibration is already in progress with a real start time,
        # preserve it — re-running the setup wizard after a restore must
        # not reset the timer back to "now" or zero the event counter.
        if current == "calibrating" and state_row and state_row["started_at"]:
            log.info("[%s] calibration already in progress — "
                     "preserving existing timer (started_at=%s)",
                     circuit, state_row["started_at"])
            return True

        circuit_cfg = self._cfg.get_circuit(circuit)
        # dev46 (46a): profile + circuit kind are adjacent reads — one hop.
        _pk = await run_db(self._profile_and_kind_sync, circuit,
                           circuit_cfg.circuit_type if circuit_cfg else "fixture")
        profile, circuit_kind = _pk["profile"], _pk["kind"]
        minimum_events = compute_minimum_events(
            profile["bathrooms_full"] or 2,
            profile["bathrooms_half"] or 0,
            profile["floors"] or 1,
            circuit_kind=circuit_kind,
        )

        now = datetime.now(timezone.utc)
        ends_at = now + timedelta(days=calibration_days)

        await run_db(
            upsert_training_state,
            self._db, circuit,
            state="calibrating",
            calibration_days=calibration_days,
            started_at=now.isoformat(),
            calibration_ends_at=ends_at.isoformat(),
            minimum_events=minimum_events,
            events_collected=0,
        )

        # Clear unconfirmed clusters from any previous calibration cycle so
        # stale micro-clusters don't pollute the new run.  Confirmed
        # clusters (fixture_id IS NOT NULL) are kept — they represent
        # user-labelled fixtures that should survive recalibration.
        await run_db(self._clear_unconfirmed_clusters, circuit)

        # Reset in-memory DBSTREAM/scaler so the engine starts fresh
        # alongside the cleared cluster table.  Confirmed clusters in DB
        # are re-seeded automatically by _init_circuit's MAX(id) lookup.
        if self.cluster_engine is not None:
            try:
                self.cluster_engine.reset_circuit(circuit)
                # A (re)calibration re-opens learning — unfreeze so the new period
                # adapts again until it re-locks at the next activation.
                self.cluster_engine.unfreeze_circuit(circuit)
            except Exception as e:
                log.warning("[%s] reset_circuit failed (non-fatal): %s",
                            circuit, e)

        await self._publish_status(circuit)
        log.info("[%s] calibration started — %d days, minimum %d events",
                 circuit, calibration_days, minimum_events)
        return True

    async def stop_calibration(self, circuit: str) -> None:
        """Cancel calibration and return to idle."""
        await run_db(
            upsert_training_state,
            self._db, circuit,
            state="idle",
            started_at=None,
            calibration_ends_at=None,
        )
        await self._publish_status(circuit)
        log.info("[%s] calibration cancelled", circuit)

    async def complete_calibration(self, circuit: str) -> None:
        """Transition calibrating → labelling.

        After calibration finishes the circuit enters a review window.
        The user confirms or removes detected clusters on the Fixtures
        page, then explicitly activates the circuit (labelling → live).
        If no review happens within LABELLING_AUTO_TIMEOUT_DAYS, the
        circuit auto-activates so anomaly detection isn't stuck waiting
        for the user to come back.
        """
        now = datetime.now(timezone.utc)
        await run_db(
            upsert_training_state,
            self._db, circuit,
            state="labelling",
            completed_at=now.isoformat(),
        )
        await self._publish_status(circuit)

        # Backfill any events that accumulated before the engine was first
        # instantiated (e.g. installs that upgraded from v0.1.x mid-calibration)
        if self.cluster_engine is not None:
            try:
                backfilled = await run_db(
                    self.cluster_engine.backfill_unmatched, circuit)
                if backfilled:
                    log.info("[%s] post-calibration backfill: %d events matched",
                             circuit, backfilled)
            except Exception as e:
                log.warning("[%s] post-calibration backfill failed (non-fatal): %s",
                            circuit, e)

        # Merge same-type clusters so the labelling page shows one card per type
        if self.cluster_engine is not None:
            try:
                await run_db(
                    self.cluster_engine.auto_merge_same_type_clusters, circuit)
            except Exception as e:
                log.warning("[%s] post-calibration type-merge failed (non-fatal): %s",
                            circuit, e)

        circuit_cfg = self._cfg.get_circuit(circuit)
        if circuit_cfg:
            await self._ha.notify(
                title=f"Water Monitor — {circuit_cfg.label} training complete",
                message=(
                    f"Training complete! Visit Fixtures to confirm what was "
                    f"detected on the {circuit_cfg.label.lower()} circuit, "
                    f"then tap 'Activate' to go live."
                ),
                notification_id=f"water_calibration_complete_{circuit}",
            )
        log.info("[%s] calibration complete — transitioning to labelling",
                 circuit)

    async def activate_fixtures(self, circuit: str) -> bool:
        """Transition labelling → live.

        Called by the Fixtures router when the user clicks
        'Activate fixtures' after reviewing detected clusters.  Returns
        False (no-op) if the circuit is not in labelling state, so a
        stale browser tab can't accidentally activate.
        """
        state_row = await run_db(get_training_state, self._db, circuit)
        if not state_row or state_row["state"] != "labelling":
            log.warning(
                "[%s] activate_fixtures called from state '%s' — ignored",
                circuit,
                state_row["state"] if state_row else "none",
            )
            return False
        now = datetime.now(timezone.utc)
        await run_db(
            upsert_training_state,
            self._db, circuit,
            state="live",
            completed_at=now.isoformat(),
        )
        await self._publish_status(circuit)
        # Phase 1: fit the per-home rule bands off this home's labels and FREEZE
        # them + the cluster engine. The reference is now locked — live events match
        # against it but never reshape it (the basis for leak / odd-usage detection).
        report = await self._fit_and_lock(circuit, source="activation")
        await self._notify_calibration_report(circuit, report)
        log.info("[%s] fixtures activated — now live (locked)", circuit)
        return True

    # ── Fit + freeze (Phase 1) ──────────────────────────────────────────────────

    def _fit_and_lock_sync(self, conn: sqlite3.Connection, circuit: str,
                           source: str) -> Dict[str, Any]:
        """Shared fit → sanity-gate → freeze path (sync; runs on a PRIVATE connection
        via run_isolated_write). Used by BOTH activation and the dev ``retrain`` so the
        activation sanity gate (in rule_calibration.fit_and_freeze) can never be skipped.

        The private ``conn`` (never the shared orch.db) keeps these heavy reclassify /
        freeze writes off the live async writers — sharing the orchestrator connection
        across this executor thread was the SQLITE_MISUSE ("bad parameter or other API
        misuse") that silently skipped post-lock reclassify + anomaly rescore."""
        from .rule_calibration import fit_and_freeze
        from .database import reclassify_all_events_from_signatures
        # Regime-aware: fit the bands for the CURRENT supply regime (falls back
        # to the legacy whole-history fit / regime_id 0 when none is recorded).
        try:
            from .supply_regime import get_current_regime
            regime = get_current_regime(conn)
        except Exception:
            regime = None
        report = fit_and_freeze(conn, circuit, source=source, regime=regime)
        # Re-type events with the freshly-frozen rules so matched_* reflects the fit.
        try:
            reclassify_all_events_from_signatures(conn, circuit)
        except Exception as e:
            log.warning("[%s] post-lock reclassify failed (non-fatal): %s",
                        circuit, e)
        # Phase 2: freeze the per-home usage baselines (envelopes + overall volume
        # percentiles) AFTER reclassify so matched types are fresh. Frozen reference
        # for future leak / odd-usage detection.
        try:
            from .anomaly_baseline import freeze_usage_baselines
            freeze_usage_baselines(conn, circuit, source=source)
        except Exception as e:
            log.warning("[%s] usage-baseline freeze failed (non-fatal): %s",
                        circuit, e)
        # Phase 2.4: freeze the per-home artifact-detector thresholds (phantom /
        # cross-talk / dribble identifiers), safety-gated so a calibration can never
        # zero a confirmed-real event. Applies to new events + future recomputes.
        try:
            from .artifact_calibration import freeze_artifact_thresholds
            freeze_artifact_thresholds(conn, circuit, source=source)
        except Exception as e:
            log.warning("[%s] artifact-threshold freeze failed (non-fatal): %s",
                        circuit, e)
        # Phase 2.3: re-score anomalies now the baseline is frozen. The FIRST
        # reclassify ran before the baseline existed (it had to — the baseline is fit
        # FROM its matched types), so its anomaly verdicts were inert. This second
        # pass scores history against the freshly-frozen baseline (the freeze
        # invalidated the cache; invalidate again defensively). Idempotent on types.
        try:
            from .anomaly_baseline import invalidate_baseline_cache
            invalidate_baseline_cache(circuit)
            reclassify_all_events_from_signatures(conn, circuit)
        except Exception as e:
            log.warning("[%s] post-baseline anomaly rescore failed (non-fatal): %s",
                        circuit, e)
        if self.cluster_engine is not None:
            try:
                self.cluster_engine.freeze_circuit(circuit)
            except Exception as e:
                log.warning("[%s] freeze_circuit failed (non-fatal): %s",
                            circuit, e)
        return report

    async def _fit_and_lock(self, circuit: str, source: str) -> Dict[str, Any]:
        from .config import DB_PATH
        from .database import start_job, finish_job, run_isolated_write
        circuit_cfg = self._cfg.get_circuit(circuit)
        label = circuit_cfg.label if circuit_cfg else circuit
        # §2.4 — track the (slow) re-lock so the UI can toast its success/failure.
        # Covers BOTH activation and the dev retrain (both route through here).
        job = await run_db(start_job, self._db, "calibration", circuit,
                           f"Calibrating {label}…")
        try:
            # Private connection + write lock (run_isolated_write) so the fit /
            # reclassify / freeze writes never share the orchestrator connection with
            # the live async writers — the SQLITE_MISUSE that silently skipped
            # post-lock reclassify + anomaly rescore. Mirrors feature_extractor /
            # maturity_recheck, which use the same helper for the same reason.
            report = await run_isolated_write(
                DB_PATH,
                lambda conn: self._fit_and_lock_sync(conn, circuit, source))
            n_fit = sum(1 for r in report.values()
                        if isinstance(r, dict) and r.get("status") == "fit")
            await run_db(finish_job, self._db, job, "done",
                         f"{label}: calibration locked ({n_fit} home-fit)")
            # P6: validate the just-frozen detectors against HA history (diagnostic
            # only — never writes a threshold). Best-effort; a failure must not affect
            # the freeze.
            try:
                from .detector_validation import run_detector_validation
                await run_detector_validation(
                    self._db, self._ha, self._cfg, circuit,
                    datetime.now(timezone.utc), source=source)
            except Exception as e:
                log.warning("[%s] detector self-validation failed (non-fatal): %s",
                            circuit, e)
            return report
        except Exception as e:
            log.error("[%s] fit-and-lock failed: %s", circuit, e)
            await run_db(finish_job, self._db, job, "error",
                         f"{label}: calibration failed")
            return {}

    async def validate_detectors(self, circuit: str) -> Dict[str, Any]:
        """Run the detector self-validation against HA history on demand (dev tab).
        Diagnostic only — writes no thresholds. Returns the report dict."""
        from .detector_validation import run_detector_validation
        return await run_detector_validation(
            self._db, self._ha, self._cfg, circuit,
            datetime.now(timezone.utc), source="manual")

    async def _maybe_interim_validation(self, circuit: str, state_row,
                                        now: datetime) -> None:
        """Fire the ~day-7 advisory once per learning period (keyed by started_at)."""
        from .detector_validation import (INTERIM_VALIDATION_DAY, build_interim_advisory,
                                          interim_already_sent, mark_interim_sent,
                                          run_detector_validation)
        started_str = state_row["started_at"]
        if not started_str:
            return
        try:
            started = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return
        if (now - started).days < INTERIM_VALIDATION_DAY:
            return
        if await run_db(interim_already_sent, self._db, circuit, started_str):
            return
        try:
            report = await run_detector_validation(
                self._db, self._ha, self._cfg, circuit, now, source="interim")
            if report.get("error"):
                return
            title, message = await run_db(build_interim_advisory, self._db,
                                          circuit, report)
            await self._ha.notify(
                title=title, message=message,
                notification_id=f"water_interim_validate_{circuit}")
            # Mark sent only AFTER a successful notify, so a transient failure retries.
            await run_db(mark_interim_sent, self._db, circuit, started_str)
            log.info("[%s] interim (day-%d) detector advisory sent",
                     circuit, INTERIM_VALIDATION_DAY)
        except Exception as e:
            log.warning("[%s] interim validation advisory failed (non-fatal): %s",
                        circuit, e)

    async def _notify_calibration_report(self, circuit: str,
                                         report: Dict[str, Any]) -> None:
        """Surface the per-type fit-vs-fallback so a sparse/bad fit is visible, not
        silent — the signal to watch on the first real activation."""
        if not report:
            return
        fit = [t for t, r in report.items() if r.get("status") == "fit"]
        fell = [t for t, r in report.items() if r.get("status") != "fit"]
        circuit_cfg = self._cfg.get_circuit(circuit)
        label = circuit_cfg.label if circuit_cfg else circuit
        msg = (f"{label}: rule calibration locked. "
               f"Home-fit: {', '.join(sorted(fit)) or 'none'}. "
               f"Using defaults: {', '.join(sorted(fell)) or 'none'}.")
        log.info("[%s] calibration report — %s", circuit, msg)
        try:
            await self._ha.notify(
                title=f"Water Monitor — {label} calibration locked",
                message=msg,
                notification_id=f"water_calibration_locked_{circuit}")
        except Exception as e:
            log.warning("[%s] calibration report notify failed: %s", circuit, e)

    def _reseed_prepare_sync(self, eng, circuit: str, since_ts: str) -> int:
        """dev46 (46a) — everything a reseed does BEFORE the replay, one hop.

        F-C2 marker, F-C1 deferral, the pressure-blind mode persist, and the
        stale-assignment clear. One transaction: a crash between the marker
        and the clear would otherwise leave the model half-torn with nothing
        recording it. Returns the number of assignments cleared.
        """
        self._set_reseed_marker(circuit, True)      # F-C2, at clear
        eng.begin_reseed(circuit)                   # F-C1 defer
        # Persist the feature mode FIRST so a crash mid-seed restarts
        # into the same space and the re-run is a clean redo.
        eng.set_pressure_blind(circuit, True)
        # Post-anchor rows: drop stale assignments (they point at
        # pre-pump centers) so the replay below re-derives them.
        cleared = self._db.execute(
            """UPDATE events SET cluster_id = NULL,
                   match_confidence = NULL, match_level = NULL
               WHERE circuit = ? AND start_ts >= ?
                 AND cluster_id IS NOT NULL""",
            (circuit, since_ts)).rowcount
        self._db.commit()
        return cleared

    async def reseed_clusters_for_regime(self, circuit: str,
                                         since_ts: str) -> Dict[str, Any]:
        """dev34 B2 — rebuild this circuit's cluster space from PUMP-ERA events
        only, in the pressure-blind feature space.

        Why rebuild_from_db can't do this: it replays only rows that already
        HAVE a cluster_id, so once live matching stops (the 2026-07 pump moved
        the features out of every learned band), the replay pool drains and
        'no_centers' becomes permanent — production sat at 0% assignment with
        no cluster gaining a member after 07-21. This function replays the
        post-anchor events through learning instead, with every
        pressure-derived dimension excluded (see PRESSURE_FEATURE_KEYS: under
        a VFD pump those dims measure the pump, not the fixture, and would
        demand another re-seed at every setpoint change).

        `since_ts` should be the pinned pump-era anchor
        (supply_regime.pump_era_start) — NOT the current regime's start, which
        a later recenter/merge can move.

        Pre-pump events keep their historical cluster_ids (those clusters and
        their stats remain valid history); post-anchor assignments are cleared
        and re-derived in the new space. Ends frozen (locked reference).
        """
        if self.cluster_engine is None:
            return {"error": "cluster engine not wired"}
        # Lazy fallback: test fixtures build the manager without __init__.
        if not hasattr(self, "_reseed_lock"):
            self._reseed_lock = asyncio.Lock()
        if self._reseed_lock.locked():
            # A double-submit must not start a second concurrent replay (see
            # the lock's init comment) — surface it instead of queueing, so
            # the user sees "already running" rather than a doubled run.
            return {"error": "a cluster re-seed is already running — "
                             "wait for it to finish, then retry if needed"}
        eng = self.cluster_engine

        # dev42 (F1): the replay runs ON the event loop in chunks — the 8/15
        # crash was the executor thread and the loop sharing one SQLite
        # connection (InterfaceError). dev42 (F2): any failure returns
        # {"error": ...} instead of a raw 500. dev42 (F-C1/F-C2): live
        # matches defer while the model is half-built, and a persisted
        # marker survives a crash so an incomplete reseed is loud.
        async with self._reseed_lock:
            try:
                # dev46 (46a): marker stamp, engine mode persist and the
                # stale-assignment clear are one contiguous DB run — ONE hop,
                # one transaction, so a crash cannot leave the marker set
                # without the clear (or vice versa).
                cleared = await run_db(self._reseed_prepare_sync, eng,
                                       circuit, since_ts)
                eng.reset_circuit(circuit)
                eng.unfreeze_circuit(circuit)
                # Learning replay, chronological, WINDOWED to the anchor —
                # the cleared set plus the never-matched backlog, nothing
                # older: pre-anchor rows predate the fragmentation fixes and
                # the supply change, and must not become centers.
                assigned = await eng.backfill_unmatched_async(
                    circuit, since_ts=since_ts)
                eng.freeze_circuit(circuit)
                # F-C1 flush: live events that arrived mid-replay were
                # stamped 'reseed_deferred' — re-match them through the
                # COMPLETED model now.
                eng.end_reseed(circuit)
                flushed = await eng.backfill_unmatched_async(
                    circuit, since_ts=since_ts, only_deferred=True)
                await run_db(self._set_reseed_marker, circuit, False)  # success only
                result = {"cleared": cleared, "assigned": assigned,
                          "flushed": flushed}
            except Exception as e:
                # F2: no raw ASGI 500. F-C2: the marker stays SET — the model
                # is part-cleared and untrusted until a rerun succeeds; the
                # boot/health check warns loudly. F-C1: the deferral flag
                # also deliberately stays on (see begin_reseed) so a
                # half-built model never matches live traffic.
                log.error("[%s] cluster re-seed FAILED mid-replay — model "
                          "incomplete, marker left set, rerun required: %s",
                          circuit, e)
                return {"error": f"re-seed failed mid-replay ({e}) — the "
                                 "cluster model is incomplete; rerun the "
                                 "re-seed"}
        log.info("[%s] cluster re-seed (pump era, pressure-blind): "
                 "%s stale assignment(s) cleared, %s event(s) clustered",
                 circuit, result.get("cleared"), result.get("assigned"))
        return result

    async def retrain(self, circuit: str) -> Dict[str, Any]:
        """DEV/testing only: re-fit + re-lock immediately against CURRENT labels —
        no new learning period. Reuses the shared fit+freeze path (so the sanity
        gate applies). Exposed only behind the feature-flagged Settings → Dev tab;
        recalibration remains the normal long-term mechanism."""
        if self.cluster_engine is not None:
            try:
                self.cluster_engine.unfreeze_circuit(circuit)
                await run_db(self.cluster_engine.rebuild_from_db, circuit)
            except Exception as e:
                log.warning("[%s] retrain rebuild failed (non-fatal): %s",
                            circuit, e)
        report = await self._fit_and_lock(circuit, source="retrain")
        await self._notify_calibration_report(circuit, report)
        log.info("[%s] dev retrain complete — reference re-locked", circuit)
        return report

    async def trigger_full_recalibration(self, circuit: str,
                                         days: int) -> bool:
        """Reset to idle then start fresh calibration."""
        await run_db(
            upsert_training_state,
            self._db, circuit,
            state="idle",
            events_collected=0,
        )
        # Clear all per-circuit data so history charts and volume totals
        # start fresh — daily_summary and import_state are included so the
        # importer re-scans and the chart doesn't show pre-reset data.
        # fixture_clusters is cleared too: a full recalibration resets the
        # DBSTREAM engine (via start_calibration → reset_circuit), so any
        # confirmed cluster centroids left in the DB would be inconsistent
        # with the empty in-memory model — new events would be type-gate-
        # rejected instead of matched against the stale centroid rows.
        # dev46 (46a): the whole clear is ONE hop and ONE transaction — a
        # half-cleared recalibration (some tables dropped, others not) is a
        # far worse state than either endpoint.
        await run_db(self._clear_for_recalibration_sync, circuit)
        return await self.start_calibration(circuit, days)

    def _clear_for_recalibration_sync(self, circuit: str) -> None:
        """dev46 (46a) — drop this circuit's learned state, on the DB thread."""
        for table, col in [
            ("events",           "circuit"),
            ("hourly_volume",    "circuit"),
            ("daily_summary",    "circuit"),
            ("import_state",     "circuit"),
            ("volume_snapshots", "circuit"),
            ("fixture_clusters", "circuit"),  # must come before start_calibration
        ]:
            try:
                self._db.execute(
                    f"DELETE FROM {table} WHERE {col} = ?", (circuit,))
            except Exception as e:
                log.warning("[%s] recalibration clear %s: %s", circuit, table, e)
        self._db.commit()

    def _away_mode_row_sync(self):
        """dev46 (46a) — the away-mode flag read."""
        return self._db.execute(
            "SELECT away_mode FROM home_profile WHERE id = 1").fetchone()

    async def trigger_partial_recalibration(self, circuit: str) -> None:
        """
        Partial recalibration — reset behavioural patterns but keep
        fixture signatures. In Phase 1 this just resets the training
        state to idle and starts a new accelerated adaptation window.
        """
        from .database import upsert_learning_config
        now = datetime.now(timezone.utc)
        accel_until = (now + timedelta(days=14)).isoformat()
        await run_db(
            upsert_learning_config,
            self._db, circuit,
            accelerated_adaptation_until=accel_until,
            accelerated_adaptation_reason="partial_recalibration",
        )
        log.info("[%s] partial recalibration — accelerated adaptation for 14 days",
                 circuit)

    async def _check_progress(self, circuit: str) -> None:
        """Check if calibration should complete automatically, or whether
        a labelling-state circuit has been stuck in review long enough to
        auto-activate.

        Pauses the calibration timer while away mode is active — the
        calibration_ends_at timestamp is extended by 1 day for every day
        spent in away mode so the learning period reflects actual occupancy.
        """
        state_row = await run_db(get_training_state, self._db, circuit)
        if not state_row:
            return

        # Auto-activate labelling circuits after the timeout window so
        # anomaly detection isn't blocked indefinitely if the user
        # never reviews their clusters.
        if state_row["state"] == "labelling":
            completed_str = state_row["completed_at"]
            if completed_str:
                try:
                    completed = datetime.fromisoformat(
                        completed_str.replace("Z", "+00:00"))
                    if completed.tzinfo is None:
                        completed = completed.replace(tzinfo=timezone.utc)
                    age = datetime.now(timezone.utc) - completed
                    if age.days >= LABELLING_AUTO_TIMEOUT_DAYS:
                        log.warning(
                            "[%s] labelling timed out after %d days — "
                            "auto-activating",
                            circuit, LABELLING_AUTO_TIMEOUT_DAYS)
                        await self.activate_fixtures(circuit)
                        circuit_cfg = self._cfg.get_circuit(circuit)
                        if circuit_cfg:
                            await self._ha.notify(
                                title=(
                                    f"Water Monitor — "
                                    f"{circuit_cfg.label} auto-activated"
                                ),
                                message=(
                                    f"Fixtures were activated automatically "
                                    f"after {LABELLING_AUTO_TIMEOUT_DAYS} days "
                                    f"of no review."
                                ),
                                notification_id=f"water_auto_activate_{circuit}",
                            )
                except (ValueError, TypeError) as e:
                    log.warning("[%s] labelling timeout check failed: %s",
                                circuit, e)
            return

        if state_row["state"] != "calibrating":
            return

        # Check away mode — pause calibration timer while away.
        # The timer is extended by the true away duration when the occupant
        # returns (see orchestrator.set_away_mode), so we just early-return here.
        try:
            profile = await run_db(self._away_mode_row_sync)
            if profile and profile["away_mode"]:
                log.debug("[%s] away mode active — calibration check deferred",
                          circuit)
                return
        except Exception as e:
            log.warning("[%s] away mode check failed: %s", circuit, e)

        now = datetime.now(timezone.utc)

        # P6: one-time interim (~day 7) self-validation ADVISORY so the user can label
        # better before the freeze locks calibration. Best-effort, diagnostic only.
        await self._maybe_interim_validation(circuit, state_row, now)

        # Check time elapsed
        ends_at_str = state_row["calibration_ends_at"]
        if ends_at_str:
            ends_at = datetime.fromisoformat(ends_at_str.replace("Z", "+00:00"))
            if ends_at.tzinfo is None:
                ends_at = ends_at.replace(tzinfo=timezone.utc)
            time_elapsed = now >= ends_at
        else:
            time_elapsed = False

        events_ok = (state_row["events_collected"] >=
                     state_row["minimum_events"])

        if time_elapsed and events_ok:
            log.info("[%s] calibration criteria met — completing",
                     circuit)
            await self.complete_calibration(circuit)
        elif time_elapsed and not events_ok:
            # Past the deadline but short on events. Don't re-warn on every 60 s
            # poll — for a sparse circuit (e.g. irrigation that only runs every
            # few days) stay quiet for roughly one observed inter-event interval
            # between warnings. Completion still fires promptly: the deadline is
            # already in the past, so the next events_ok tick completes on the
            # branch above. The deadline itself is intentionally NOT moved.
            last = self._last_undertarget_warn.get(circuit)
            cadence = await run_db(get_event_cadence_seconds, self._db,
                                   circuit,
                                   since_iso=state_row["started_at"])
            interval = _compute_recheck_interval(cadence)
            if last is None or (now - last) >= interval:
                self._last_undertarget_warn[circuit] = now
                log.warning(
                    "[%s] calibration under target (%d/%d events) — sparse "
                    "circuit; next notice in ~%.1f h (cadence=%s)",
                    circuit,
                    state_row["events_collected"],
                    state_row["minimum_events"],
                    interval.total_seconds() / 3600.0,
                    f"{cadence / 3600.0:.1f}h" if cadence else "unknown",
                )
                circuit_cfg = self._cfg.get_circuit(circuit)
                if circuit_cfg:
                    await self._ha.notify(
                        title="Water Monitor — Training extended",
                        message=(
                            f"{circuit_cfg.label}: training period elapsed but only "
                            f"{state_row['events_collected']} of "
                            f"{state_row['minimum_events']} events collected. "
                            f"Training continues automatically."
                        ),
                        notification_id=f"water_training_extended_{circuit}",
                    )
            # else: under target but still within the quiet interval — stay silent.

    async def _publish_status(self, circuit: str) -> None:
        """Publish training status sensor to HA."""
        state_row = await run_db(get_training_state, self._db, circuit)
        if not state_row:
            return

        state = state_row["state"]
        now = datetime.now(timezone.utc)

        attrs: Dict[str, Any] = {
            "friendly_name": f"Water Training Status - {circuit}",
            "icon": "mdi:school",
            "circuit": circuit,
            "events_collected": state_row["events_collected"] or 0,
            "minimum_events": state_row["minimum_events"] or 0,
        }

        if state == "calibrating" and state_row["started_at"]:
            started = datetime.fromisoformat(
                state_row["started_at"].replace("Z", "+00:00"))
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            elapsed_days = (now - started).days

            ends_str = state_row["calibration_ends_at"]
            if ends_str:
                ends_at = datetime.fromisoformat(
                    ends_str.replace("Z", "+00:00"))
                if ends_at.tzinfo is None:
                    ends_at = ends_at.replace(tzinfo=timezone.utc)
                remaining_td   = max(ends_at - now, timedelta(0))
                remaining_days = remaining_td.days
                remaining_hours = remaining_td.seconds // 3600
                total_days = state_row["calibration_days"] or 14
                time_pct = min(100, int(elapsed_days / max(total_days, 1) * 100))
                # Progress is purely time-based for user-facing display.
                # An earlier hybrid scheme also computed event_pct from
                # events_collected / minimum_events, but the events
                # metric is internal only and is no longer surfaced.
                pct = time_pct
            else:
                remaining_days  = 0
                remaining_hours = 0
                pct = 0

            attrs.update({
                "days_elapsed":      elapsed_days,
                "days_remaining":    remaining_days,
                "hours_remaining":   remaining_hours,
                "percent_complete":  pct,
                "calibration_ends_at": state_row["calibration_ends_at"],
            })
        elif state in ("labelling", "live"):
            # Calibration is 100% done; labelling means awaiting user
            # review, live means anomaly detection is active.
            attrs.update({
                "percent_complete":  100,
                "days_remaining":    0,
                "hours_remaining":   0,
            })

        entity_id = f"sensor.water_training_status_{circuit}"
        await self._ha.set_state(entity_id, state, attrs)

    def get_training_info(self, circuit: str) -> Dict[str, Any]:
        """Return training state info for the web UI."""
        state_row = get_training_state(self._db, circuit)
        if not state_row:
            return {"state": "idle", "percent_complete": 0}

        now = datetime.now(timezone.utc)
        result = dict(state_row)

        if state_row["state"] == "calibrating" and state_row["calibration_ends_at"]:
            ends_str = state_row["calibration_ends_at"]
            ends_at = datetime.fromisoformat(ends_str.replace("Z", "+00:00"))
            if ends_at.tzinfo is None:
                ends_at = ends_at.replace(tzinfo=timezone.utc)

            started_str = state_row["started_at"]
            if started_str:
                started = datetime.fromisoformat(
                    started_str.replace("Z", "+00:00"))
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                elapsed = (now - started).total_seconds()
                total = (ends_at - started).total_seconds()
                time_pct = min(100, int(elapsed / max(total, 1) * 100))
            else:
                time_pct = 0

            remaining_td = max(ends_at - now, timedelta(0))
            result["days_remaining"]  = remaining_td.days
            result["hours_remaining"] = remaining_td.seconds // 3600
            # Percent complete is purely time-based — events_collected
            # / minimum_events is an internal metric and doesn't affect
            # the displayed progress. (Earlier code computed an event_pct
            # here and discarded it.)
            result["percent_complete"] = time_pct
        else:
            # 'labelling' is post-calibration review — calibration itself
            # is 100% done, so the progress bar reads full.  'live' is
            # also 100%.  'idle' is 0%.
            if state_row["state"] in ("live", "labelling"):
                result["percent_complete"] = 100
            else:
                result["percent_complete"] = 0
            result["days_remaining"]    = 0
            result["hours_remaining"]   = 0

        return result

    @staticmethod
    def suggest_calibration_days(
        bathrooms_full: int,
        bathrooms_half: int,
        floors: int,
        occupants: int,
        supply_type: str,
    ) -> tuple[int, str]:
        return compute_suggested_calibration_days(
            bathrooms_full, bathrooms_half, floors, occupants, supply_type)
