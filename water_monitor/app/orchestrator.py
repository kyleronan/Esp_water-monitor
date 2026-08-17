"""
Orchestrator — ties together all Phase 1 components:
  - HaClient (WebSocket + REST)
  - EventDetector (pressure transient + flow onset detection)
  - FeatureExtractor (event → SQLite)
  - TrainingManager (state machine + HA sensor publish)
  - LeakTestScheduler (scheduled + on-demand leak tests)

Also publishes live sensor status to HA for the web UI and
external automations.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from .config import AddonConfig, SENSITIVITY_PRESETS, DB_PATH
from .database import (get_sensitivity_config, ensure_circuit_defaults, init_db)
from .device_discovery import (load_circuit_entities, is_setup_complete,
                                get_device_config, rescan_optional_roles)
from .event_detector import EventDetector
from .feature_extractor import FeatureExtractor
from .ha_client import HaClient
from .leak_test_scheduler import LeakTestScheduler
from .training_manager import TrainingManager
from .data_pruner import DataPruner
from .alert_manager import AlertManager
from .presence_watcher import PresenceWatcher
from .historical_importer import HistoricalImporter
from .cluster_metrics import ClusterMetrics
from .maturity_recheck import MaturityRecheck
from .rise_corr_backfill import RiseCorrBackfill
from .wf_repair_backfill import WfRepairBackfill
from .fixture_publisher import FixturePublisher

log = logging.getLogger(__name__)


async def _timed_startup_job(name: str, awaitable):
    """dev46 (46a/C2a) — time one startup job and log how long it held.

    C2a chunked the two jobs known to be expensive (reclassify, cluster
    backfill). The rest were left whole on a JUDGEMENT that they are cheap,
    which is exactly the kind of claim that should be a number instead. Each
    boot now prints its own evidence, so the tripwires recorded in the plan
    can be checked against a real boot log rather than re-argued.

    Failure is not swallowed — the timing line is emitted either way and the
    exception propagates to the caller's existing handler.
    """
    import time
    t0 = time.monotonic()
    try:
        return await awaitable
    finally:
        log.info("startup job %s took %.1fs", name, time.monotonic() - t0)


def _fmt_sensor(
    raw: Optional[str],
    decimals: int = 1,
    fallback: str = "—",
    factor: float = 1.0,
) -> str:
    """Format a raw HA state string to a fixed-decimal string, applying unit conversion.
    Returns fallback when the value is missing, 'unknown', or non-numeric."""
    if raw is None or raw in ("", "unknown", "unavailable"):
        return fallback
    try:
        return f"{float(raw) * factor:.{decimals}f}"
    except (ValueError, TypeError):
        return fallback


# A flow-meter PPL change at/above this RELATIVE magnitude triggers a full re-baseline
# (meter swap / correcting a wrong default); a smaller change is a calibration trim —
# value + floor updated and the frozen anomaly percentiles re-scaled, with NO multi-day
# shut-off pause. See _apply_ppl_change.
_PPL_REBASELINE_FRACTION: float = 0.10


class Orchestrator:
    """Top-level runtime — owns all components."""

    def __init__(self, cfg: AddonConfig):
        self._cfg = cfg
        self._db: Optional[sqlite3.Connection] = None
        self._ha: Optional[HaClient] = None
        self._event_queue: Optional[asyncio.Queue] = None
        self._event_detector: Optional[EventDetector] = None
        self._feature_extractor: Optional[FeatureExtractor] = None
        self._training_manager: Optional[TrainingManager] = None
        self._data_pruner: Optional[DataPruner] = None
        self._alert_manager: Optional[AlertManager] = None
        self._presence_watcher: Optional[PresenceWatcher] = None
        self._leak_test_scheduler: Optional[LeakTestScheduler] = None
        self._historical_importer: Optional[HistoricalImporter] = None
        self._cluster_engine = None
        self._cluster_metrics: Optional[ClusterMetrics] = None
        self._maturity_recheck: Optional[MaturityRecheck] = None
        self._rise_corr_backfill: Optional[RiseCorrBackfill] = None
        self._wf_repair_backfill: Optional[WfRepairBackfill] = None
        self._fixture_publisher: Optional[FixturePublisher] = None
        self._stop = asyncio.Event()
        self._live_state_cache: Dict[str, Any] = {}
        self._ha_tz = timezone.utc
        # Strong refs for fire-and-forget PPL-change re-baseline tasks (scheduled
        # from the sync HA state_changed callback). Without this the only reference
        # is create_task's return value, which Python may GC before the task runs.
        self._ppl_tasks: set = set()
        # Circuits with an active calibration session (bucket / municipal test). A
        # deliberate calibration draw must not trip auto-shutoff or pollute training /
        # anomaly stats — see is_calibrating + the FeatureExtractor suppression.
        self._calibrating: set = set()
        # RBAC role sets, read by ingress_middleware on every request (membership
        # test only — no per-request HA call / DB read). admin_ids = last-known-good
        # HA admin set (refreshed by _run_role_sync); operator_ids = the allow-list.
        # Both populated at startup via load_roles_from_db(). _seen_uids is the
        # in-memory first-sight guard so seen_users is logged once per user/process.
        self.admin_ids: set = set()
        self.operator_ids: set = set()
        # dev46 (46h) — per-circuit post-winterization grace starts.
        self._winterize_cleared_at: dict = {}
        self._seen_uids: set = set()

    @property
    def db(self) -> sqlite3.Connection:
        return self._db

    @property
    def ha(self) -> HaClient:
        return self._ha

    @property
    def training_manager(self) -> TrainingManager:
        return self._training_manager

    @property
    def data_pruner(self) -> DataPruner:
        return self._data_pruner

    @property
    def alert_manager(self) -> AlertManager:
        return self._alert_manager

    @property
    def away_mode(self) -> bool:
        """True if the home is currently in away/vacation mode."""
        try:
            row = self._db.execute(
                "SELECT away_mode FROM home_profile WHERE id = 1").fetchone()
            return bool(row["away_mode"]) if row else False
        except Exception:
            return False

    async def set_away_mode(self, enabled: bool) -> None:
        """Enable or disable away mode. Notifies via HA when toggled."""
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        # dev46 (46a): every DB touch in this method — the away-since read,
        # the per-circuit calibration extension, and the profile write — goes
        # over the wall in ONE hop. They all precede the single HA await at
        # the end, so there is nothing to split: one callable, one
        # transaction (rule N2a).
        from .database import run_db
        await run_db(self._set_away_mode_sync, enabled, now, now_iso)

        if self._alert_manager and enabled:
            await self._alert_manager.alert_away_mode_on()
        log.info("Away mode %s", "enabled" if enabled else "disabled")

    def _set_away_mode_sync(self, enabled: bool, now, now_iso: str) -> None:
        """dev46 (46a) — the away-mode DB work, on the DB thread.

        Self-contained transaction: the calibration extensions and the
        home_profile flip commit together, so a crash between them cannot
        leave timers extended for an away period that never got recorded.
        """
        from datetime import datetime, timezone, timedelta

        if not enabled:
            # Extend calibration timers by the actual time spent away so the
            # learning period reflects real occupancy — handles offline periods too.
            try:
                row = self._db.execute(
                    "SELECT away_since FROM home_profile WHERE id = 1"
                ).fetchone()
                if row and row["away_since"]:
                    away_since = datetime.fromisoformat(
                        row["away_since"].replace("Z", "+00:00"))
                    if away_since.tzinfo is None:
                        away_since = away_since.replace(tzinfo=timezone.utc)
                    away_duration = now - away_since
                    if timedelta(0) < away_duration < timedelta(days=90):
                        for cfg in self._cfg.circuits:
                            ts_row = self._db.execute(
                                "SELECT state, calibration_ends_at "
                                "FROM training_state WHERE circuit = ?",
                                (cfg.circuit,)).fetchone()
                            if ts_row and ts_row["state"] == "calibrating" \
                                    and ts_row["calibration_ends_at"]:
                                ends_at = datetime.fromisoformat(
                                    ts_row["calibration_ends_at"].replace(
                                        "Z", "+00:00"))
                                if ends_at.tzinfo is None:
                                    ends_at = ends_at.replace(tzinfo=timezone.utc)
                                new_end = ends_at + away_duration
                                self._db.execute(
                                    "UPDATE training_state SET calibration_ends_at=? "
                                    "WHERE circuit=?",
                                    (new_end.isoformat(), cfg.circuit))
                                log.info(
                                    "[%s] calibration extended by %s (away duration)",
                                    cfg.circuit,
                                    str(away_duration).split(".")[0])
            except Exception as e:
                log.warning("Away-mode calibration extension failed: %s", e)

        # Explicit `? = 1` comparison rather than relying on SQLite's
        # implicit truthiness of Python bool bindings — same behaviour,
        # but makes the intent unambiguous and survives any future
        # binding-type changes.
        enabled_int = 1 if enabled else 0
        self._db.execute("""
            UPDATE home_profile SET
                away_mode  = ?,
                away_since = CASE WHEN ? = 1 THEN ? ELSE NULL END,
                updated_at = ?
            WHERE id = 1
        """, (enabled_int, enabled_int, now_iso, now_iso))
        self._db.commit()

    def reload_presence_watcher(self) -> None:
        """Re-subscribe after the user updates presence entity settings."""
        if self._presence_watcher:
            self._presence_watcher.reload()
            log.info("Presence watcher reloaded")

    @property
    def leak_test_scheduler(self) -> LeakTestScheduler:
        return self._leak_test_scheduler

    @property
    def historical_importer(self) -> Optional[HistoricalImporter]:
        return self._historical_importer

    @property
    def cluster_engine(self):
        return self._cluster_engine

    @property
    def event_detector(self) -> EventDetector:
        return self._event_detector

    @property
    def setup_complete(self) -> bool:
        """True once the setup wizard has been completed."""
        if not self._db:
            return False
        return is_setup_complete(self._db)

    def reload_circuit_entities(self) -> None:
        """
        Re-load entity IDs from circuit_entity_map into the live
        CircuitConfig objects. Called after the setup wizard completes
        or after manual entity overrides.
        """
        if not self._db:
            return
        device_cfg = get_device_config(self._db)
        prefix = device_cfg.get("esp_device_prefix", "") if device_cfg else ""

        for circuit_cfg in self._cfg.circuits:
            entities = load_circuit_entities(self._db, circuit_cfg.circuit)
            circuit_cfg.flow_sensor = entities.get("flow_sensor", "")
            circuit_cfg.pressure_fast_sensor = entities.get(
                "pressure_fast_sensor", "")
            circuit_cfg.pressure_avg_sensor = entities.get(
                "pressure_avg_sensor", "")
            circuit_cfg.pressure_history_sensor = entities.get(
                "pressure_history_sensor", "")
            circuit_cfg.flow_onset_sensor = entities.get(
                "flow_onset_sensor", "")
            circuit_cfg.valve_entity = entities.get("valve_entity", "")
            circuit_cfg.fault_sensor = entities.get("fault_sensor", "")
            circuit_cfg.fault_reason_sensor = entities.get(
                "fault_reason_sensor", "")
            circuit_cfg.leak_test_duration_entity = entities.get(
                "leak_test_duration_sensor", "")
            circuit_cfg.trickle_sensor = entities.get("trickle_sensor", "")
            circuit_cfg.leak_test_sensor = entities.get(
                "leak_test_sensor", "")
            circuit_cfg.leak_test_switch = entities.get(
                "leak_test_switch", "")
            circuit_cfg.leak_test_result_sensor = entities.get(
                "leak_test_result_sensor", "")
            circuit_cfg.leak_test_baseline_sensor = entities.get(
                "leak_test_baseline_sensor", "")
            circuit_cfg.leak_test_closed_sensor = entities.get(
                "leak_test_closed_sensor", "")
            circuit_cfg.leak_threshold_entity = entities.get(
                "leak_pressure_threshold", "")
            circuit_cfg.volume_sensor = entities.get("volume_sensor", "")
            circuit_cfg.flow_meter_ppl_entity = entities.get("flow_meter_ppl", "")
            # Waveforms now arrive via chunked HA events (firmware 3.9.0+).
            # Only diagnostic counters remain as discoverable entities.
            circuit_cfg.wf_overflow_count_sensor = entities.get("wf_overflow_count_sensor", "")
            circuit_cfg.wf_chunk_drop_count_sensor = entities.get("wf_chunk_drop_count_sensor", "")
            circuit_cfg.esp_device_prefix = prefix

            log.debug("[%s] entity IDs loaded from DB — fully_configured=%s",
                      circuit_cfg.circuit, circuit_cfg.is_fully_configured)

    def reload_circuit_labels(self) -> None:
        """
        Re-load circuit display names from circuit_labels into the live
        CircuitConfig objects. Called on startup, after setup wizard
        naming step, after settings rename, and after backup restore.
        """
        if not self._db:
            return
        from .database import load_circuit_labels
        labels = load_circuit_labels(self._db)
        for circuit_cfg in self._cfg.circuits:
            circuit_cfg.display_name = labels.get(circuit_cfg.circuit, "")
            log.debug("[%s] display_name loaded: %r",
                      circuit_cfg.circuit, circuit_cfg.display_name)

    def load_roles_from_db(self, db) -> None:
        """Populate admin_ids (last-known-good cache) + operator_ids from the DB.

        Unions the configured ``bootstrap_admin_user_id`` so a misconfigured /
        unreachable ``config/auth/list`` can never lock the admin out. Called at
        lifespan startup (with the migration connection, so the sets are ready
        before serving) and again inside ``run()`` once the orchestrator's own
        connection is open.
        """
        from .database import load_operator_ids, load_cached_admin_ids
        try:
            self.operator_ids = load_operator_ids(db)
            admins = load_cached_admin_ids(db)
        except Exception as e:  # pragma: no cover - defensive
            log.warning("load_roles_from_db failed (keeping current sets): %s", e)
            return
        boot = (getattr(self._cfg, "bootstrap_admin_user_id", "") or "").strip()
        self.admin_ids = (admins | {boot}) if boot else admins
        log.info("RBAC roles loaded — %d admin(s) cached, %d operator(s)",
                 len(self.admin_ids), len(self.operator_ids))

    def reload_operator_ids(self) -> None:
        """Refresh the operator allow-list from the DB into the live set.

        Called by the Access page after an admin grants/revokes operator so the
        middleware sees the change immediately (no restart)."""
        from .database import load_operator_ids
        if self._db:
            self.operator_ids = load_operator_ids(self._db)

    async def _refresh_admin_ids_once(self) -> bool:
        """One HA admin-set refresh from ``config/auth/list``; persists a
        last-known-good cache. Returns True if it updated the live set.

        Never raises and never clears: on transport/permission failure OR an empty
        result the existing cached ``admin_ids`` is kept, so a transient HA hiccup
        or a token without user-list access can never downgrade a real admin to
        viewer."""
        try:
            users = await self._ha.get_users()
        except Exception as e:
            log.warning("role-sync fetch failed (keeping cached admin set): %s", e)
            return False
        from .database import save_admin_ids_cache
        from .auth import admin_ids_from_users
        new_ids = admin_ids_from_users(users)
        if not new_ids:
            log.warning("role-sync: config/auth/list returned no admins — keeping "
                        "cached set (%d)", len(self.admin_ids))
            return False
        boot = (getattr(self._cfg, "bootstrap_admin_user_id", "") or "").strip()
        self.admin_ids = (new_ids | {boot}) if boot else new_ids
        # Persist only on CHANGE: the sync runs every 10 min forever and the
        # admin set almost never changes — an unconditional DELETE+INSERT is
        # ~144 pointless write transactions/day of SD-card/eMMC wear plus write-
        # lock churn on the shared connection.
        admins = [(u.get("id"), u.get("name") or "")
                  for u in users if u.get("is_admin")]
        if admins != getattr(self, "_last_saved_admins", None):
            # dev46 (46a): change-gated write, over the wall like every other.
            from .database import run_db
            await run_db(save_admin_ids_cache, self._db,
                         [u for u in users if u.get("is_admin")])
            self._last_saved_admins = admins
            log.info("role-sync: cached %d HA admin(s)", len(new_ids))
        return True

    async def _run_role_sync(self) -> None:
        """Keep the HA admin set fresh (every 10 min). Runs under ``_supervise``
        (returns only on stop). The FIRST refresh is kicked off early in ``run()``
        so the admin set is ready before normal traffic, not after full startup."""
        while not self._stop.is_set():
            await self._refresh_admin_ids_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=600)
                return
            except asyncio.TimeoutError:
                pass

    def reload_circuit_profiles(self) -> None:
        """Re-read circuit_type from circuit_profile into the live CircuitConfig objects.

        Called after startup seeding, the setup wizard circuit-names step,
        and the settings circuit-type endpoint. Falls back to the value already
        on CircuitConfig (originally from options.json) if no DB row exists.
        """
        if not self._db:
            return
        from .database import get_circuit_type, get_circuit_pulses_per_litre
        for circuit_cfg in self._cfg.circuits:
            circuit_cfg.circuit_type = get_circuit_type(
                self._db,
                circuit_cfg.circuit,
                default=circuit_cfg.circuit_type,
            )
            # Cached pulses-per-litre. The firmware number entity is the source of
            # truth; the async refresh (_refresh_ppl_from_entities) updates both this
            # field and the DB cache when HA is reachable. This sync read keeps a sane
            # floor (min_flow_lpm = 60 ÷ ppl) when HA/firmware is briefly unavailable.
            circuit_cfg.pulses_per_litre = get_circuit_pulses_per_litre(
                self._db,
                circuit_cfg.circuit,
                default=circuit_cfg.pulses_per_litre,
            )

    async def _sync_ppl_and_watch(self) -> None:
        """Sync each circuit's runtime flow-meter PPL from its HA number entity and
        subscribe for changes. The firmware number entity is the single source of
        truth; circuit_profile.pulses_per_litre is the local cache."""
        if not self._ha:
            return
        watched = False
        for cfg in self._cfg.circuits:
            entity = getattr(cfg, "flow_meter_ppl_entity", "")
            if not entity:
                continue
            raw = await self._ha.get_state_value(entity, None)
            await self._apply_ppl_change(cfg, raw, reason="startup")
            self._ha.subscribe_entity(
                entity,
                lambda eid, state, attrs, c=cfg.circuit: self._on_ppl_state(c, state),
            )
            watched = True
        if watched:
            # Pick up the new subscriptions on the already-running WS connection.
            self._ha.request_reconnect()

    def _on_ppl_state(self, circuit: str, state: Any) -> None:
        """Sync HA state_changed callback → schedule the async PPL-change handler.

        PPL is read-only in the add-on, so the HA number entity is the ONLY write
        path: this subscription is the complete re-baseline trigger surface."""
        cfg = self._cfg.get_circuit(circuit)
        if cfg is None:
            return
        task = asyncio.create_task(
            self._apply_ppl_change(cfg, state, reason="runtime"))
        self._ppl_tasks.add(task)
        task.add_done_callback(self._ppl_tasks.discard)

    async def _apply_ppl_change(self, cfg: Any, raw_value: Any, *,
                                reason: str) -> None:
        """Apply an observed flow-meter PPL for a circuit.

        Re-baseline IFF the value differs from the cached one, so an NVS-restore of
        the same value on a firmware reboot is a no-op (debounce). On a genuine
        change: update the cache + the live detector floor, then force a
        NON-DESTRUCTIVE partial recalibration (keeps history + opens the
        accelerated-adaptation window that suppresses auto-shutoff). NEVER recomputes
        historical volumes and NEVER uses full recalibration (which deletes events).
        """
        try:
            new_ppl = float(raw_value)
        except (TypeError, ValueError):
            return  # 'unknown' / 'unavailable' / None — ignore
        if not (1.0 <= new_ppl <= 5000.0):
            return
        cached = float(getattr(cfg, "pulses_per_litre", 396.0) or 396.0)
        if abs(new_ppl - cached) < 0.5:
            return  # same value (NVS restore / redundant publish) — no-op
        circuit = cfg.circuit
        frac = abs(new_ppl - cached) / cached if cached else 1.0
        big = frac >= _PPL_REBASELINE_FRACTION
        log.warning(
            "[%s] flow-meter PPL %.1f → %.1f (%s, %.1f%%) — %s. Past event volumes are NOT recomputed.",
            circuit, cached, new_ppl, reason, frac * 100.0,
            "re-baselining (auto-shutoff paused until matured)" if big
            else "small trim, re-scaling anomaly thresholds (no relearn)")
        # 1) Persist to the DB cache + live config.
        try:
            from .database import run_db, set_circuit_pulses_per_litre
            await run_db(set_circuit_pulses_per_litre, self._db, circuit,
                         new_ppl)
        except Exception as e:
            log.warning("[%s] PPL cache write failed: %s", circuit, e)
        cfg.pulses_per_litre = new_ppl
        # 2) Update the live detector low-flow floor (60 ÷ ppl).
        if self._event_detector is not None:
            self._event_detector.set_min_flow(circuit, cfg.min_flow_lpm)
        # 3) Re-baseline decision (relative threshold). LARGE change (meter swap /
        #    correcting a wrong default) → non-destructive PARTIAL re-baseline (keeps
        #    history; opens the accelerated-adaptation window that suppresses auto-shutoff;
        #    full recalibration deletes events, so it is NEVER used here). SMALL trim (a
        #    calibration correction) → keep the frozen anomaly thresholds aligned with the
        #    new scale by re-scaling the volume percentiles — NO multi-day shut-off pause.
        if big:
            if self._training_manager is not None:
                try:
                    await self._training_manager.trigger_partial_recalibration(circuit)
                except Exception as e:
                    log.warning("[%s] PPL re-baseline trigger failed: %s", circuit, e)
            note = ("Re-learning the baseline — automatic shut-off is paused until it "
                    "matures. Past usage totals are unchanged.")
        else:
            # Adjusts forward-looking DETECTION thresholds to the new scale; does NOT
            # recompute historical event volumes (the never-recompute invariant holds).
            try:
                from .anomaly_baseline import rescale_anomaly_percentiles
                from .database import run_db
                await run_db(rescale_anomaly_percentiles, self._db, circuit,
                             cached / new_ppl)
            except Exception as e:
                log.warning("[%s] anomaly-percentile rescale failed: %s", circuit, e)
            note = ("Small calibration trim applied — no re-learning needed. Past usage "
                    "totals are unchanged.")
        # 4) Surface to the user (best-effort).
        if self._ha is not None:
            try:
                await self._ha.notify(
                    "Flow meter updated",
                    f"{cfg.label}: flow-meter pulses/litre set to {new_ppl:.1f}. {note}")
            except Exception as e:
                log.debug("[%s] PPL change notify failed: %s", circuit, e)

    def set_calibrating(self, circuit: str, active: bool) -> None:
        """Mark/clear an active calibration session for a circuit (called by the
        calibration router on start / stop / apply / cancel + stale timeout)."""
        if active:
            self._calibrating.add(circuit)
        else:
            self._calibrating.discard(circuit)

    def is_calibrating(self, circuit: str) -> bool:
        """True while a bucket / municipal calibration test runs on this circuit — the
        detector suppresses auto-shutoff and excludes the deliberate test draw."""
        return circuit in self._calibrating

    def stop(self) -> None:
        self._stop.set()
        if self._feature_extractor:
            self._feature_extractor.stop()
        if self._training_manager:
            self._training_manager.stop()
        if self._data_pruner:
            self._data_pruner.stop()
        if self._maturity_recheck:
            self._maturity_recheck.stop()
        if self._rise_corr_backfill:
            self._rise_corr_backfill.stop()
        if self._wf_repair_backfill:
            self._wf_repair_backfill.stop()
        if self._leak_test_scheduler:
            self._leak_test_scheduler.stop()
        if getattr(self, "_supply_regime", None):
            self._supply_regime.stop()
        if self._ha:
            self._ha.stop()

    def _setup_complete_sync(self) -> bool:
        """dev46 (46a) — ``setup_complete`` is a PROPERTY that queries the DB
        on every access, so reading it from the loop is a connection touch
        like any other. This wrapper gives run_db something to call."""
        return self.setup_complete

    def _reload_config_and_roles_sync(self) -> None:
        """dev46 (46a) — entity/label/profile reloads + RBAC roles, one hop.

        Load entity IDs, display labels, and circuit types from the DB into
        the circuit configs, then refresh the RBAC role sets against the
        orchestrator's own connection (the lifespan pre-loaded them from the
        migration connection) and re-apply the bootstrap admin.
        """
        self.reload_circuit_entities()
        self.reload_circuit_labels()
        self.reload_circuit_profiles()
        self.load_roles_from_db(self._db)

    def _boot_db_preamble_sync(self) -> None:
        """dev46 (46a) — the boot DB work that precedes the HA client.

        Per-circuit defaults, the read-only orphan-reference check, and the
        F-C2 incomplete-reseed warning. One callable, one transaction
        (rule N2a); each sub-step keeps its own best-effort try/except so a
        failure in one does not skip the others.
        """
        # Ensure per-circuit defaults exist
        for circuit_cfg in self._cfg.circuits:
            ensure_circuit_defaults(
                self._db, circuit_cfg.circuit, circuit_cfg.circuit_type)

        # Sprint A — boot-time orphan-reference integrity check.
        # Migration 20260528 already ran the repair once. This pass is
        # READ-ONLY (repair=False) — it only logs when something has
        # drifted since (e.g. user restored an older backup, or future
        # bugs introduce new orphans). The fix-at-boot button is left
        # off intentionally so a surprising DB state stays observable
        # rather than getting silently mutated; the Fixtures-page banner
        # is the user-facing fix path.
        try:
            from .database import find_orphaned_cluster_references
            _orphans = find_orphaned_cluster_references(self._db, repair=False)
            if any(_orphans.values()):
                log.warning(
                    "Orphan-reference integrity check: events_orphaned=%d "
                    "fixtures_unbacked=%d clusters_dangling=%d — see "
                    "the Water Use page for the relink banner",
                    _orphans["events_orphaned"],
                    _orphans["fixtures_unbacked"],
                    _orphans["clusters_dangling"],
                )
        except Exception as _e:
            log.warning(
                "Orphan-reference integrity check failed (non-fatal): %s", _e
            )

        # dev42 (F-C2): a reseed that crashed mid-replay leaves its marker
        # stamped — the cluster model was part-cleared and never finished
        # rebuilding. Warn on every boot until a rerun succeeds.
        try:
            for _r in self._db.execute(
                    "SELECT circuit, reseed_in_progress FROM training_state "
                    "WHERE reseed_in_progress IS NOT NULL"):
                log.warning(
                    "[%s] reseed incomplete (started %s, never finished) — "
                    "cluster model untrusted; rerun 'Rebuild fixture "
                    "grouping for current pressure' in Settings",
                    _r["circuit"], _r["reseed_in_progress"])
        except Exception:
            pass    # pre-20260808 schema


    async def run(self) -> None:
        """Initialise and run all components concurrently."""
        # Database
        self._db = init_db(DB_PATH)

        # dev46 (46a): the whole pre-HA boot block — per-circuit defaults, the
        # orphan-reference integrity check, and the incomplete-reseed warning —
        # is one contiguous run of DB work with no awaits in it, so it goes
        # over the wall in ONE hop. NOTE this is NOT startup-before-loop:
        # main.py creates run() as a task and immediately yields to uvicorn, so
        # request handlers are already submitting to run_db while this executes.
        from .database import run_db
        await run_db(self._boot_db_preamble_sync)

        # HA client
        self._ha = HaClient()
        await self._ha.__aenter__()

        # dev46 (46a): the three config reloads and the RBAC role load are all
        # DB reads with no awaits between them — one hop. Each of these
        # reaches the connection through a closed-over helper rather than a
        # conn argument, which is exactly the class the attribute-only audit
        # could not see.
        await run_db(self._reload_config_and_roles_sync)
        # Kick off the FIRST HA admin-set fetch NOW (right after the HA client is
        # up), before the heavy startup work below, so admins are recognised before
        # normal traffic rather than only after _run_role_sync's first tick at the
        # end of startup. Best-effort — keeps the cached set on failure.
        try:
            await self._refresh_admin_ids_once()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("initial role refresh failed (non-fatal): %s", e)

        # On already-configured systems, scan for optional roles that may have appeared
        # after a firmware upgrade (e.g. waveform entities added in 3.7.0).
        # This runs before EventDetector is constructed so that CircuitConfig waveform
        # fields are populated before setup() subscribes entities.
        # Never calls save_discovery() — never clears existing mappings.
        if await run_db(self._setup_complete_sync):
            try:
                _rescan = await rescan_optional_roles(
                    self._ha,
                    self._db,
                    [c.circuit for c in self._cfg.circuits],
                )
                if _rescan.total_changed or _rescan.prefix_updated:
                    log.info(
                        "Optional-role rescan: %d new entity mapping(s), "
                        "prefix_updated=%s — reloading",
                        _rescan.total_changed, _rescan.prefix_updated,
                    )
                    await run_db(self.reload_circuit_entities)
            except Exception as _e:
                log.warning("Optional-role rescan failed (non-fatal): %s", _e)

        # Event queue
        self._event_queue = asyncio.Queue(maxsize=1000)

        # Training manager
        self._training_manager = TrainingManager(
            self._cfg, self._db, self._ha)
        self._data_pruner = DataPruner(self._db, db_path=DB_PATH)
        self._alert_manager = AlertManager(self._db, self._ha)
        self._presence_watcher = PresenceWatcher(
            self._db, self._ha, self.set_away_mode)
        await self._presence_watcher.setup()
        await self._presence_watcher.sync_initial_state()

        # Fetch the HA instance timezone so the leak-test scheduler and "Today"
        # calculations use local time rather than UTC.
        try:
            await self._init_ha_timezone()
        except Exception as e:
            log.warning("HA timezone fetch failed (using UTC): %s", e)

        # Leak test scheduler
        self._leak_test_scheduler = LeakTestScheduler(
            self._cfg, self._db, self._ha, self._alert_manager,
            ha_tz=self._ha_tz)

        # Recompute every enabled leak-test schedule on startup so that
        # stale next_run_at values (from prior bad scheduler state,
        # timezone changes, or the same-day-duplicate bug) are corrected
        # before the scheduler task starts polling.
        try:
            await self._recompute_leak_test_schedules()
        except Exception as e:
            log.warning("Leak-test schedule recompute failed (non-fatal): %s", e)

        # Historical importer — backfills missed events and runs periodic catch-up.
        # Pass `self` so the importer can consult the live EventDetector and skip
        # candidate periods that overlap a currently-active event (C0 guard).
        self._historical_importer = HistoricalImporter(
            self._cfg, self._db, self._ha, self._event_queue,
            orchestrator=self)

        # Event detector — only if setup is complete and entities are loaded
        self._event_detector = EventDetector(
            circuits=self._cfg.circuits,
            ha_client=self._ha,
            event_queue=self._event_queue,
            sensitivity_getter=self._get_sensitivity,  # audit-ok(run_db): invoked only inside EventDetector.collect_circuit_inputs, which is submitted via run_db
            debug_capture_propagation=self._cfg.debug_capture_propagation,
            pump_gate_getter=self._get_pump_osc_gate,  # audit-ok(run_db): invoked only inside EventDetector.collect_circuit_inputs, which is submitted via run_db
            low_pressure_getter=self._get_low_pressure_floors,  # audit-ok(run_db): invoked only inside EventDetector.collect_circuit_inputs, which is submitted via run_db
            low_pressure_cb=self._on_low_pressure_alert,
            winterized_getter=self._is_circuit_winterized,  # audit-ok(run_db): invoked only inside EventDetector.collect_circuit_inputs, which is submitted via run_db
            pump_fail_cb=self._on_pump_fail_alert,
        )
        if await run_db(self._setup_complete_sync):
            await self._event_detector.setup()
            log.info("Event detection active")
            # dev38: seed valve states AFTER the change subscriptions are wired
            # (subscribe-then-prime) so other_valve_open can record a confirmed
            # 0 even for valves that never transition after boot.
            try:
                await self._event_detector.prime_valve_states()
            except Exception as e:
                log.warning("Valve-state prime failed (non-fatal): %s", e)
            # Runtime per-circuit flow-meter PPL: sync current values from the firmware
            # number entities (the source of truth) into the cache + detector floor, and
            # watch for changes. A change forces a non-destructive re-baseline.
            try:
                await self._sync_ppl_and_watch()
            except Exception as e:
                log.warning("Flow-meter PPL sync/watch failed (non-fatal): %s", e)
        else:
            log.info("Setup not complete — event detection paused until wizard finishes")

        # Feature extractor
        self._feature_extractor = FeatureExtractor(
            self._event_queue, self._db, self._alert_manager,
            ha_client=self._ha,
            event_detector=self._event_detector,
            is_calibrating=self.is_calibrating)

        # Reverse link: a late-assembled ESP waveform upgrades a recent
        # software-signature event to ESP provenance (signature-only, Fix 1).
        # Optional sink → a no-op until both sides exist.
        self._event_detector._waveform_upgrade_sink = (
            self._feature_extractor.handle_late_waveform
        )

        # Cluster engine — instantiate and rebuild state from the last 60 days
        # of already-matched events so DBSTREAM + scaler are warm on startup.
        try:
            from .cluster_engine import ClusterEngine
            from .database import run_db
            self._cluster_engine = ClusterEngine(self._db, self._cfg)
            for c in self._cfg.circuits:
                # dev46 (46a): every DB touch goes through the single DB
                # thread. Interleave-safe by construction — the live matching
                # path can't reach the engine yet (feature_extractor's
                # cluster_engine is wired below, and it guards on that).
                count = await _timed_startup_job(
                    f"rebuild_from_db[{c.circuit}]",
                    run_db(self._cluster_engine.rebuild_from_db, c.circuit))
                log.info("[%s] cluster state rebuilt — %d events replayed",
                         c.circuit, count)
                # Backfill events that had no cluster_id (e.g. v0.1.x upgrades).
                # dev46 (46a/C2a): the chunked variant — it already submits one
                # run_db call per batch, so a queued page render interleaves at
                # each chunk instead of waiting out the whole backlog.
                backfilled = await _timed_startup_job(
                    f"backfill_unmatched[{c.circuit}]",
                    self._cluster_engine.backfill_unmatched_async(c.circuit))
                if backfilled:
                    log.info("[%s] backfilled cluster_id on %d previously unmatched events",
                             c.circuit, backfilled)
            self._feature_extractor.cluster_engine = self._cluster_engine
            # Wire to training_manager so complete_calibration can trigger backfill
            self._training_manager.cluster_engine = self._cluster_engine
            log.info("ClusterEngine initialised and wired to feature extractor")
        except Exception as e:
            log.error("ClusterEngine init failed (non-fatal): %s", e, exc_info=True)

        # Auto-exclusion verdicts + label-trained fixture typing. Runs after the
        # cluster engine so matched_fixture_type reflects the newest user labels.
        # Both passes are idempotent and best-effort — a failure must not block
        # boot. The 20260535 migration only adds the dribble column (lightweight
        # DDL); the verdict + typing backfill lands here.
        # dev46 (46a/C2a) — these passes run on the single DB worker via
        # run_db, and the expensive one is submitted CHUNK-WISE.
        #
        # With one DB worker, a monolithic submission makes every queued page
        # render wait for the whole pass — the ~2-minute startup reclassify
        # would turn the old crash into a hang. reclassify_..._async slices the
        # row loop into batches (chunk = transaction = one run_db call), so the
        # queue gets a seam every ~200 rows and small pages stay responsive
        # through boot. The whole-circuit cycle detectors still run once, in
        # the pass's prepare step — they were never the obstacle to batching.
        try:
            from .database import run_db
            from .feature_extractor import reprocess_event_exclusion_verdicts
            res = await _timed_startup_job(
                "reprocess_event_exclusion_verdicts",
                run_db(reprocess_event_exclusion_verdicts, self._db))
            if res.get("dribbles_flagged"):
                log.info("startup: flagged %d low-flow dribble event(s)",
                         res["dribbles_flagged"])
            from .database import (reclassify_all_events_from_signatures_async,
                                   recompute_cycle_pulse_counts,
                                   resuggest_all_clusters,
                                   recompute_all_user_label_suggestions)
            for c in self._cfg.circuits:
                # dev.22: cycle-pulse backfill MUST precede reclassify so the
                # matcher's cycle_pulse_count feature is populated before it types.
                cyc = await _timed_startup_job(
                    f"recompute_cycle_pulse_counts[{c.circuit}]",
                    run_db(recompute_cycle_pulse_counts, self._db, c.circuit))
                r = await _timed_startup_job(
                    f"reclassify[{c.circuit}]",
                    reclassify_all_events_from_signatures_async(
                        self._db, c.circuit))
                if r.get("events_matched") or r.get("events_cleared"):
                    log.info(
                        "[%s] startup reclassify: %d matched, %d abstained, "
                        "%d stale cleared", c.circuit, r["events_matched"],
                        r["events_abstained"], r["events_cleared"])
                # dev.37 — backfill cluster_id over events the reprocess just
                # un-excluded (capped-rescued) and any accumulated NULL backlog. The
                # ~line-440 backfill ran BEFORE reprocess, so those events never got a
                # cluster and user labels on them had nothing to propagate into. This
                # pass assigns them. Idempotent: backfill_unmatched only touches
                # cluster_id IS NULL rows, so it skips everything already clustered.
                if self._cluster_engine is not None:
                    bf = await self._cluster_engine.backfill_unmatched_async(
                        c.circuit)
                    if bf:
                        log.info("[%s] startup post-reprocess backfill: "
                                 "%d event(s) clustered", c.circuit, bf)
                # Re-run the heuristic suggestion over the patched centroids, then
                # the GATED user-label suggestion to un-poison mixed clusters
                # (dev.22) — the only path that clears a stale 'user_labels' vote.
                rs = await _timed_startup_job(
                    f"resuggest_all_clusters[{c.circuit}]",
                    run_db(resuggest_all_clusters, self._db, c.circuit))
                ul = await _timed_startup_job(
                    f"recompute_all_user_label_suggestions[{c.circuit}]",
                    run_db(recompute_all_user_label_suggestions, self._db,
                           c.circuit))
                if cyc.get("updated") or rs.get("updated") or ul.get("cleared"):
                    log.info("[%s] startup reprocess: %d cycle events, %d heuristic "
                             "+ %d user-label suggestion(s) re-derived (%d cleared)",
                             c.circuit, cyc["updated"], rs["updated"],
                             ul["suggested"] + ul["abstained"], ul["cleared"])
        except Exception as e:
            log.warning("startup reclassify/reprocess failed (non-fatal): %s", e)

        # dev44 — the startup cluster work above (rebuild → reclassify →
        # backfill) runs in executor threads against a snapshot of the DB
        # taken at boot. Any repair that mutates cluster references while
        # it's in flight gets silently overwritten when the stale replay
        # finishes (observed: the stale-link repair clicked 15 s after a
        # restart lost the race and the orphans returned). Routes that
        # rebuild engine state gate on this flag.
        self.startup_cluster_work_done = True

        # Initialise daily/weekly volume baselines from HA history so that
        # the dashboard shows accurate totals from the first page load.
        # force=True so a restart/redeploy re-derives the midnight readings and
        # CORRECTS a stale or wrong-but-nonzero baseline immediately, instead of
        # leaving the dashboard inflated until the next midnight rollover. When
        # HA history is unavailable the existing value is left untouched.
        try:
            await self._init_volume_baselines(force=True)
        except Exception as e:
            log.warning("Volume baseline init failed (non-fatal): %s", e)

        # Auto-detect HA unit system and apply defaults if the user hasn't
        # explicitly chosen units yet (flow_unit still at schema default).
        try:
            await self._init_display_units()
        except Exception as e:
            log.warning("Unit auto-detection failed (non-fatal): %s", e)

        # Cluster quality metrics — background task writing to cluster_metrics_history
        self._cluster_metrics = ClusterMetrics(self._db, self._cfg)

        # Maturity re-check — periodically confirms/retracts provisional appliance
        # labels once an event's cycle-mates have had time to occur (Branch-2.2).
        self._maturity_recheck = MaturityRecheck(self._db, self._cfg, self._ha,
                                                 orch=self)

        # One-shot rise-corr backfill (dev14) — fills flow_pressure_corr for
        # historical candidate events from HA history, then stamps itself done.
        self._rise_corr_backfill = RiseCorrBackfill(self._db, self._cfg, self._ha)

        # One-shot waveform mis-attachment repair (migration 20260573) — fixes
        # stored events that a wrongly-matched firmware capture left reporting
        # an average above their own peak, then replays the affected circuits'
        # cluster state so the in-memory scaler stops carrying those outliers.
        self._wf_repair_backfill = WfRepairBackfill(
            self._db, self._cluster_engine)

        # Nightly pump-regime detector (dev23, pump plan Phase 3) — analyzes
        # the quiet-hour pressure/flow window with the study-validated math
        # and maintains the banner+confirm home flag. Detection only; no
        # detector behavior changes until the user confirms (Phase 4 gates on
        # pump_mode_effective, which ignores unconfirmed detection).
        from .pump_regime_detector import PumpRegimeDetector
        self._pump_regime = PumpRegimeDetector(self._db, self._cfg, self._ha,
                                               ha_tz=self._ha_tz,
                                               alert_manager=self._alert_manager)

        # Supply-pressure regime tracker — persists idle-line pressure daily,
        # detects sustained supply shifts (pump install/removal, PRV change)
        # and banners a per-regime rule recalibration. Detection only; nothing
        # adapts until the user confirms.
        from .supply_regime import SupplyRegimeTracker
        self._supply_regime = SupplyRegimeTracker(
            self._db, self._cfg,
            settled_getter=self._event_detector.settled_pressure,
            ha_tz=self._ha_tz, alert_manager=self._alert_manager)

        # Fixture publisher — MQTT Discovery for confirmed fixtures
        self._fixture_publisher = FixturePublisher(self._db, self._cfg, self._ha)
        try:
            await self._fixture_publisher.start()
        except Exception as e:
            log.warning("FixturePublisher start failed (non-fatal): %s", e)

        # Run all background tasks concurrently, each under its own supervisor
        # so a crash in one subsystem restarts only that subsystem instead of
        # taking down the entire orchestrator.
        try:
            await asyncio.gather(
                self._supervise("ha_event_loop",       self._ha.run_event_loop),
                self._supervise("feature_extractor",   self._feature_extractor.run),
                self._supervise("training_manager",    self._training_manager.run),
                self._supervise("data_pruner",         self._data_pruner.run),
                self._supervise("leak_test_scheduler", self._leak_test_scheduler.run),
                self._supervise("historical_importer", self._historical_importer.run),
                self._supervise("cluster_metrics",     self._cluster_metrics.run),
                self._supervise("maturity_recheck",    self._maturity_recheck.run),
                self._supervise("rise_corr_backfill",  self._rise_corr_backfill.run),
                self._supervise("wf_repair_backfill",  self._wf_repair_backfill.run),
                self._supervise("pump_regime_detector", self._pump_regime.run),
                self._supervise("supply_regime_tracker", self._supply_regime.run),
                self._supervise("waveform_purger",     self._run_waveform_purger),
                self._supervise("volume_baseline_rollover",
                                self._run_volume_baseline_rollover),
                self._supervise("role_sync",           self._run_role_sync),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._ha.__aexit__(None, None, None)

    async def _supervise(self, name: str, coro_fn) -> None:
        """Run coro_fn() in a restart loop. A crash restarts after 5s."""
        while not self._stop.is_set():
            try:
                await coro_fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s crashed — restarting in 5s", name)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=5)
                    return
                except asyncio.TimeoutError:
                    pass

    def _get_pump_osc_gate(self, circuit: str):
        """Pump-mode oscillation gate for the live detector (dev25), or None.

        Active only under confirmed vfd pump mode. Amplitude-derived
        (plan round-1 #14): max(2.0, 0.15 × latest measured sawtooth band)
        from pump_regime_nightly, fallback 2.0 when detection hasn't measured
        a band yet (setup-declared pump homes)."""
        try:
            from .config import pump_gates_active
            if not pump_gates_active(self._db, circuit):
                return None
            from .database import get_pump_regime_nights
            nights = get_pump_regime_nights(self._db, limit=10)
            amp = next((n["amplitude_psi"] for n in nights
                        if n["any_detected"] and n["amplitude_psi"]), None)
            return max(2.0, 0.15 * float(amp)) if amp else 2.0
        except Exception as e:
            log.warning("[%s] pump-gate resolution failed (non-fatal): %s",
                        circuit, e)
            return None

    def _get_low_pressure_floors(self, circuit: str):
        """Phase 6 (dev27): (zone_floor_psi|None, pump_fail_floor_psi|None).

        6a zone floor: zone circuits only, from sensitivity_config (default
        25). 6b pump-fail floor: the FIRST fixture circuit only (shared
        supply — ANY-circuit resolution), and only when vfd pump mode is
        active AND the alert is ARMED (post-feature supply answer or
        persisted evidence stamp — the arming rule). NULL user floor resolves
        the per-supply default (city_pump 40) at read time."""
        zone_floor = None
        pump_floor = None
        try:
            from .database import get_circuit_type, get_home_profile
            sens = self._get_sensitivity(circuit)
            if get_circuit_type(self._db, circuit, default="fixture") == "zone":
                zone_floor = float(
                    sens.get("low_pressure_alert_psi") or 25.0)
            else:
                fixture_circuits = [
                    c.circuit for c in self._cfg.circuits
                    if get_circuit_type(self._db, c.circuit,
                                        default=c.circuit_type) == "fixture"]
                if fixture_circuits and circuit == fixture_circuits[0]:
                    from .config import pump_gates_active
                    if pump_gates_active(self._db, circuit):
                        prof = get_home_profile(self._db)
                        armed = bool(prof and (
                            prof["pump_alert_armed_at"]
                            or (prof["supply_type"] == "city_pump"
                                and prof["supply_type_set_at"])))
                        if armed:
                            user_floor = sens.get("pump_low_pressure_alert_psi")
                            pump_floor = (float(user_floor)
                                          if user_floor is not None else 40.0)
        except Exception as e:
            log.warning("[%s] low-pressure floor resolve failed: %s",
                        circuit, e)
        return zone_floor, pump_floor

    def _on_low_pressure_alert(self, circuit: str, psi: float) -> None:
        """6a callback (runs on the event loop from the WS callback)."""
        if self._alert_manager is None:
            return
        name = self.get_display_name(circuit) if hasattr(
            self, "get_display_name") else circuit
        # dev46 (46a): the WS handler invokes this callback ON THE EVENT
        # LOOP, so the pump-mode read cannot happen here. It moves into the
        # task that was already being spawned — no behaviour change, the
        # alert was always fired asynchronously.
        asyncio.ensure_future(
            self._low_pressure_alert_async(circuit, psi, name))

    async def _low_pressure_alert_async(self, circuit: str, psi: float,
                                        name: str) -> None:
        """dev46 (46a) — the DB half of the 6a low-pressure alert."""
        from .config import pump_mode_effective_cached
        from .database import run_db
        try:
            pump_active = (await run_db(pump_mode_effective_cached,
                                        self._db, circuit))["active"]
        except Exception:
            pump_active = False
        await self._alert_manager.alert_low_pressure_supply(
            circuit, psi, name, pump_active)

    def _on_pump_fail_alert(self, circuit: str, psi: float, kind: str) -> None:
        """6b callback (runs on the event loop from the WS callback)."""
        if self._alert_manager is None:
            return
        name = self.get_display_name(circuit) if hasattr(
            self, "get_display_name") else circuit
        asyncio.ensure_future(self._alert_manager.alert_pump_low_pressure(
            circuit, psi, kind, name))

    def note_winterize_cleared(self, circuit: str) -> None:
        """dev46 (46h) — start the post-winterization grace for ``circuit``.

        Refilling a drained line looks exactly like the catastrophic pressure
        event the detector exists to catch, so alarms stay quiet briefly after
        the flag clears. In-memory on purpose: the grace is a boot-scoped
        courtesy, and a schema column would outlive its usefulness every
        spring. A restart mid-refill costs at most one dismissible alarm.
        """
        from datetime import datetime, timezone
        self._winterize_cleared_at[circuit] = datetime.now(timezone.utc)

    def winterize_grace_active(self, circuit: str) -> bool:
        """True while ``circuit`` is inside its post-winterization grace."""
        from datetime import datetime, timezone
        from .database import WINTERIZE_UNSET_GRACE_S
        ts = self._winterize_cleared_at.get(circuit)
        if ts is None:
            return False
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age < WINTERIZE_UNSET_GRACE_S

    def _is_circuit_winterized(self, circuit: str) -> bool:
        """dev46 (46h) — is this circuit deliberately drained for the season?

        Handed to EventDetector as a getter (it has no connection of its own),
        alongside the sensitivity / pump-gate / low-pressure getters.
        """
        from .database import is_circuit_winterized
        return is_circuit_winterized(self._db, circuit)

    def _get_sensitivity(self, circuit: str) -> dict:
        """Return effective sensitivity settings for a circuit."""
        if not self._db:
            return SENSITIVITY_PRESETS["medium"]

        row = get_sensitivity_config(self._db, circuit)
        if not row:
            return SENSITIVITY_PRESETS["medium"]

        # Build effective values: column value or preset fallback
        level = row["simple_level"] or "medium"
        preset = SENSITIVITY_PRESETS.get(level, SENSITIVITY_PRESETS["medium"])

        # Use `is not None` rather than truthiness — 0.0 is a valid user-set
        # threshold but is falsy, so `row[x] or preset[x]` would silently
        # revert a user-set zero back to the preset value.
        def _eff(key: str):
            v = row[key]
            return v if v is not None else preset[key]

        return {
            "pressure_drop_event_psi":    _eff("pressure_drop_event_psi"),
            "min_event_duration_seconds": _eff("min_event_duration_seconds"),
            "score_alert":                _eff("score_alert"),
            "score_shutoff":              _eff("score_shutoff"),
            "flow_tolerance_pct":         _eff("flow_tolerance_pct"),
            "duration_tolerance_pct":     _eff("duration_tolerance_pct"),
            "schedule_window_minutes":    _eff("schedule_window_minutes"),
            "sustained_alert_minutes":    _eff("sustained_alert_minutes"),
            "max_shutoffs_per_12h":       _eff("max_shutoffs_per_12h"),
        }

    def get_live_state(self, circuit: str) -> Dict[str, Any]:
        """
        Fetch current live state for a circuit from HA.
        Returns a dict for use in web UI templates.
        This is called synchronously from route handlers —
        the actual HA calls happen async in background tasks
        and results are cached implicitly via HA's state machine.
        """
        return {"circuit": circuit}

    async def get_live_state_async(self, circuit: str) -> Dict[str, Any]:
        """Async version — fetches fresh state from HA REST API, cached for 3s."""
        import time
        now_ts = time.monotonic()

        # Return cached result if it's fresh enough (3 second window)
        cached = self._live_state_cache.get(circuit)
        if cached and now_ts - cached.get("_fetched_at", 0) < 3.0:
            return cached

        result = await self._fetch_live_state(circuit)
        result["_fetched_at"] = now_ts
        self._live_state_cache[circuit] = result
        return result

    async def _init_display_units(self) -> None:
        """
        Query the HA unit system and set sensible display unit defaults the
        first time the addon runs.  Skips if the user has already saved any
        preference that differs from the migration schema defaults
        (flow_unit='L/min', pressure_unit='psi').

        Uses INSERT … ON CONFLICT DO UPDATE so the detection also works on a
        fresh install where home_profile row may not exist yet.
        """
        # dev46 (46a) — TWO-HOP handler: read, await HA, write.
        # HOP-2 RE-CHECK (named): _write_display_units_sync re-asserts
        # "both units still at schema defaults" INSIDE the write callable.
        # Without it, a user who picks gal/min on the Settings page while
        # get_ha_unit_system() is in flight would have their choice silently
        # overwritten by HA's default — the exact stale-premises class R1
        # exists to prevent. This is NOT exemption-class material: the write
        # is a value-set, not monotonic, so an interleaved write absolutely
        # can change the right outcome.
        from .database import run_db
        from .units import defaults_from_ha, invalidate_unit_cache
        if not await run_db(self._display_units_are_default_sync):
            return
        ha_units = await self._ha.get_ha_unit_system()
        ha_vol   = ha_units.get("volume", "L")
        flow_key, pressure_key = defaults_from_ha(ha_vol)
        if not await run_db(self._write_display_units_sync, flow_key,
                            pressure_key):
            log.info("Display-unit auto-detect skipped — a preference was set "
                     "while HA's unit system was being read")
            return
        invalidate_unit_cache()
        log.info("Display units auto-detected from HA: flow=%s pressure=%s",
                 flow_key, pressure_key)

    def _display_units_are_default_sync(self) -> bool:
        """True while both display units are still at their schema defaults."""
        row = self._db.execute(
            "SELECT flow_unit, pressure_unit FROM home_profile WHERE id = 1"
        ).fetchone()
        if row and (
            (row["flow_unit"]     and row["flow_unit"]     != "L/min") or
            (row["pressure_unit"] and row["pressure_unit"] != "psi")
        ):
            return False
        return True

    def _write_display_units_sync(self, flow_key: str,
                                  pressure_key: str) -> bool:
        """Write the HA-derived display units, re-checking the precondition.

        dev46 (46a) hop-2 re-check: the gate is re-evaluated inside the same
        callable as the write, so a preference saved during the HA await wins
        instead of being clobbered. Returns False when it declined to write.
        """
        if not self._display_units_are_default_sync():
            return False
        # ON CONFLICT handles both fresh install (no row) and existing row
        self._db.execute("""
            INSERT INTO home_profile (id, flow_unit, pressure_unit)
            VALUES (1, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                flow_unit     = excluded.flow_unit,
                pressure_unit = excluded.pressure_unit
        """, (flow_key, pressure_key))
        self._db.commit()
        return True

    async def _init_ha_timezone(self) -> None:
        """Fetch the HA instance's configured timezone and cache it for volume queries."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]
        from .event_rules import set_home_timezone
        tz_name = "UTC"
        try:
            ha_cfg = await self._ha.get_ha_config()
            tz_name = ha_cfg.get("time_zone") or "UTC"
            self._ha_tz = ZoneInfo(tz_name)
            log.info("HA timezone: %s", tz_name)
        except Exception as e:
            from datetime import timezone as _tz
            self._ha_tz = _tz.utc
            log.warning("Could not determine HA timezone (%s) — using UTC", e)
        # dev.24 — cache for the softener regen-band match (reclassify + live path
        # read this without threading a tzinfo through every caller).
        set_home_timezone(self._ha_tz)
        await self._resync_daily_summary_boundary(tz_name)
        await self._backfill_time_features(tz_name)

    def _resync_boundary_sync(self, tz_name: str):
        """dev46 (46a) — read the stored zone, rebuild day totals, stamp it.

        One DB-thread callable, one transaction (rule N2a). Returns ``None``
        when the stored zone already matches, so the caller can skip its log
        line; otherwise the rebuild's result dict.
        """
        from .database import (get_home_profile, rebuild_daily_summaries,
                               update_home_profile)
        profile = get_home_profile(self._db)
        stored = (dict(profile or {}) or {}).get("daily_summary_tz")
        if stored == tz_name:
            return None
        log.info("daily_summary day boundary: %s → %s — rebuilding",
                 stored or "UTC (pre-20260571)", tz_name)
        res = rebuild_daily_summaries(self._db)
        update_home_profile(self._db, daily_summary_tz=tz_name)
        return res

    async def _resync_daily_summary_boundary(self, tz_name: str) -> None:
        """Rebuild daily_summary when its day boundary no longer matches the home.

        Daily rollups are keyed on the LOCAL day, but the timezone only becomes
        known here — after migrations, after HA answers. A stored bucketing zone
        that differs from the detected one means every historical day total is
        cut at the wrong instant (the pre-20260571 rows are cut at UTC midnight,
        i.e. 18:00 local in Denver), so they're recomputed once and the zone is
        stamped. Runs off-loop: the rebuild touches every day of history.

        Best-effort — a failure leaves the stamp alone, so the next boot retries
        rather than silently keeping mis-bucketed rows.
        """
        try:
            # dev46 (46a): read the stored zone, rebuild, and stamp the new
            # zone in ONE hop. Bundling is not only about thread-safety here —
            # the stamp must not outlive a failed rebuild, and inside a single
            # callable it cannot (rule N2a: one callable, one transaction).
            # None means the stored zone already matches and nothing was done.
            from .database import run_db
            res = await run_db(self._resync_boundary_sync, tz_name)
            if res is None:
                return
            log.info("daily_summary rebuild complete: %s", res)
        except Exception as e:
            log.warning("daily_summary boundary resync failed (retried next "
                        "boot, non-fatal): %s", e)

    async def _backfill_time_features(self, tz_name: str) -> None:
        """dev38 — deferred local-time rewrite of the per-event time features.

        Same shape as _resync_daily_summary_boundary: migrations run before HA
        answers, so migration 20260801 only added the events.time_features_tz
        marker and the rewrite happens here once the zone is known. The
        function itself only touches rows whose marker mismatches, so this is
        a fast no-op on every boot after the first. Best-effort — a failure
        leaves markers alone and the next boot retries. MUST run before the
        waveform-repair workers' cluster rebuild (their supervised chain
        starts at +240 s; this completes in seconds for ~6k rows).
        """
        from .database import backfill_time_features_tz, run_db
        try:
            res = await run_db(backfill_time_features_tz, self._db, tz_name)
            if res.get("rewritten"):
                log.info("Time-feature tz backfill (%s): %s", tz_name, res)
        except Exception as e:
            log.warning("Time-feature tz backfill failed (retried next boot, "
                        "non-fatal): %s", e)

    def _local_midnight_utc(self, days_ago: int = 0) -> str:
        """Return the UTC equivalent of local midnight (or N days ago) as a naive ISO string.

        Uses the cached HA timezone from _init_ha_timezone(); falls back to UTC.
        """
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        ha_tz = getattr(self, "_ha_tz", _tz.utc)
        now_local = _dt.now(ha_tz)
        midnight_local = now_local.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - _td(days=days_ago)
        midnight_utc = midnight_local.astimezone(_tz.utc).replace(tzinfo=None)
        return midnight_utc.isoformat(timespec="seconds")

    def _seconds_until_next_local_midnight(self) -> float:
        """Seconds from now until the next local midnight (DST-aware)."""
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        ha_tz = getattr(self, "_ha_tz", _tz.utc)
        now_local = _dt.now(ha_tz)
        next_midnight_local = now_local.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + _td(days=1)
        delta = next_midnight_local.astimezone(_tz.utc) - _dt.now(_tz.utc)
        return max(0.0, delta.total_seconds())

    async def _init_volume_baselines(self, force: bool = False) -> None:
        """
        Query HA history to set accurate midnight baselines for daily/weekly
        volume calculations.  Called once at startup, then again after every
        local-midnight rollover (with ``force=True``) by
        ``_run_volume_baseline_rollover``.

        Without a per-day refresh, after the local day ticks over the "today"
        baseline key has no snapshot; _get_volume_baseline() then seeds it from
        the current reading, which is only accurate for the just-started "today"
        period — the rolling 7-day baseline must come from HA history. This is
        why the rollover re-derives both, force-overwriting stale values.

        ``force``: when False (startup) an existing non-zero baseline is left
        untouched. When True (rollover) the freshly-fetched HA-history value
        overwrites whatever is there, so a value lazily seeded by
        _get_volume_baseline (current reading) is corrected to the real midnight.

        period_ts keys are the UTC equivalent of local midnight, stored as naive
        ISO strings, matching the keys produced by compute_ha_daily/weekly_volume.
        """
        from datetime import datetime, timezone, timedelta

        today_midnight_ts   = self._local_midnight_utc(days_ago=0)
        seven_days_ago_ts   = self._local_midnight_utc(days_ago=7)

        # Reconstruct aware UTC datetimes for the get_history() call window
        _utc = timezone.utc
        today_midnight_dt   = datetime.fromisoformat(today_midnight_ts).replace(tzinfo=_utc)
        seven_days_ago_dt   = datetime.fromisoformat(seven_days_ago_ts).replace(tzinfo=_utc)

        from .ha_client import vol_to_litres as _v2l

        for cfg in self._cfg.circuits:
            if not cfg.volume_sensor:
                continue

            circuit = cfg.circuit

            # get_history() is fetched with no_attributes=True, so historical
            # readings carry NO unit_of_measurement. Converting them with that
            # empty unit left a gallon reading UNCONVERTED in the litres column,
            # so "today" (= meter_litres − baseline_gallons) inflated ~3.8x. The
            # sensor's unit doesn't change over time, so fetch the current unit
            # and convert the historical readings with it.
            sensor_unit = ""
            try:
                cur_state = await self._ha.get_state(cfg.volume_sensor)
                sensor_unit = ((cur_state or {}).get("attributes") or {}).get(
                    "unit_of_measurement", "")
            except Exception as e:
                log.debug("[%s] could not fetch volume sensor unit: %s", circuit, e)

            for period_start, period_ts, label in [
                (today_midnight_dt,  today_midnight_ts,  "today"),
                (seven_days_ago_dt,  seven_days_ago_ts,  "past 7 days"),
            ]:

                # At startup only fix baselines still at the 0.0 placeholder; on
                # a forced rollover always re-derive from HA history.
                # dev46 (46a) — hop 1 of a two-hop handler (the HA history
                # fetch below is the non-DB await).
                from .database import run_db
                if not await run_db(self._volume_baseline_needs_fix_sync,
                                    circuit, period_ts, force):
                    continue   # already set to a real value

                # Query HA history for the earliest reading at/after midnight
                try:
                    hist = await self._ha.get_history(
                        cfg.volume_sensor,
                        period_start,
                        period_start + timedelta(hours=2),
                    )
                    if hist:
                        first = hist[0]
                        # Convert with the sensor's real unit (the historical
                        # entry has none — see sensor_unit above).
                        midnight_val = _v2l(float(first["state"]), sensor_unit)
                    else:
                        # No HA history for this window — do NOT write a 0.0
                        # baseline (that resurrects the full-cumulative-total
                        # bug). Leave the row absent so _get_volume_baseline
                        # seeds it safely from the current reading instead.
                        continue
                except Exception as e:
                    log.debug("[%s] could not fetch volume history for %s: %s",
                              circuit, label, e)
                    continue

                # last_reading tracks with the baseline: a re-derived baseline
                # invalidates any high-water mark measured against the old one.
                # The next live read raises it to the true maximum within
                # seconds, so the only exposure is a reset in that window —
                # which carries 0 L, never invented water.
                # HOP-2 RE-CHECK (named): _write_volume_baseline_sync
                # re-evaluates the same "still a 0.0 placeholder (or forced)"
                # gate inside the write callable. If a live read seeded a real
                # baseline while the HA history fetch was in flight, this
                # write stands down rather than overwriting it from stale
                # premises — and since a baseline shifts every daily total
                # measured against it, that is volume-accounting correctness,
                # not just tidiness. Not exemption-class: the write is a
                # value-set, so an interleaved write changes the right answer.
                if not await run_db(self._write_volume_baseline_sync, circuit,
                                    period_ts, midnight_val, force):
                    log.info("[%s] volume baseline for %s left alone — a real "
                             "value landed while HA history was being read",
                             circuit, label)
                    continue
                log.info("[%s] volume baseline set for %s: %.2f L",
                         circuit, label, midnight_val)

    def _volume_baseline_needs_fix_sync(self, circuit: str, period_ts,
                                        force: bool) -> bool:
        """True when this period's baseline should be re-derived from HA.

        Startup fixes only rows still at the 0.0 placeholder; a forced
        rollover always re-derives.
        """
        row = self._db.execute(
            "SELECT ha_volume FROM volume_snapshots "
            "WHERE circuit=? AND period_ts=?",
            (circuit, period_ts),
        ).fetchone()
        return bool(force or row is None or row[0] == 0.0)

    def _write_volume_baseline_sync(self, circuit: str, period_ts,
                                    midnight_val: float,
                                    force: bool) -> bool:
        """Write an HA-derived baseline, re-checking the precondition.

        dev46 (46a) hop-2 re-check: the gate is re-evaluated inside the same
        callable as the write, so a real baseline seeded during the HA await
        is not overwritten from stale premises. Returns False when it declined.
        """
        if not self._volume_baseline_needs_fix_sync(circuit, period_ts, force):
            return False
        self._db.execute("""
            INSERT INTO volume_snapshots (circuit, period_ts, ha_volume,
                                          last_reading)
            VALUES (?,?,?,?)
            ON CONFLICT (circuit, period_ts)
            DO UPDATE SET ha_volume    = excluded.ha_volume,
                          last_reading = excluded.last_reading
        """, (circuit, period_ts, midnight_val, midnight_val))
        self._db.commit()
        return True

    async def _recompute_leak_test_schedules(self) -> None:
        """Recompute next_run_at for every enabled leak-test schedule.

        Called once on startup so stale next_run_at values — from prior
        bad scheduler state, timezone changes, or the auto-learn same-
        day-duplicate bug — are corrected before the scheduler task
        starts polling. Unconditional by design: the cost (one
        learn_best_hour pass per circuit) is small and the upside
        (deterministic, predictable next-run after boot) is worth it.

        Invalid or unparsable existing values are logged and overwritten;
        naive datetimes are treated as UTC for the diff log.
        """
        from .database import get_leak_test_schedule, run_db

        for circuit_cfg in self._cfg.circuits:
            circuit = circuit_cfg.circuit
            try:
                schedule = await run_db(get_leak_test_schedule, self._db,
                                        circuit)
            except Exception as e:
                log.warning("[%s] could not read leak_test_schedule: %s",
                            circuit, e)
                continue
            if not schedule or not schedule["enabled"]:
                continue

            prior_str = schedule["next_run_at"]
            prior_dt: Optional[datetime] = None
            if prior_str:
                try:
                    prior_dt = datetime.fromisoformat(
                        prior_str.replace("Z", "+00:00"))
                    if prior_dt.tzinfo is None:
                        prior_dt = prior_dt.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    log.warning("[%s] unparsable next_run_at %r — recomputing",
                                circuit, prior_str)

            try:
                await self._leak_test_scheduler._update_next_run(
                    circuit, schedule)
            except Exception as e:
                log.warning("[%s] startup next-run recompute failed: %s",
                            circuit, e)
                continue

            # Read-back after the scheduler's await. No hop-2 re-check is
            # needed: re-checks exist to stop a STALE WRITE landing, and
            # nothing is written here — this read only feeds a log line.
            new_row = await run_db(get_leak_test_schedule, self._db, circuit)
            new_str = new_row["next_run_at"] if new_row else None
            if new_str and new_str != prior_str:
                log.info("[%s] startup recompute: next_run_at %s → %s",
                         circuit, prior_str or "(none)", new_str)

    def _live_state_db_sync(self, circuit: str, ha_volume_total,
                            today_ts, week_ts):
        """dev46 (46a) — every DB read behind one live-state poll, one hop.

        Read-only, so no transaction to own. Bundling also makes the poll
        internally consistent: the volumes, valve type and degraded/anomaly
        state now all describe the same instant rather than whatever each
        separate touch happened to see.
        """
        from .database import (compute_ha_daily_volume,
                               compute_ha_weekly_volume, get_daily_volume,
                               get_weekly_volume)
        from .units import load_unit_context
        if ha_volume_total is not None and ha_volume_total >= 0:
            volume_daily = compute_ha_daily_volume(
                self._db, circuit, ha_volume_total, period_ts=today_ts)
            volume_weekly = compute_ha_weekly_volume(
                self._db, circuit, ha_volume_total, period_ts=week_ts)
        else:
            volume_daily = get_daily_volume(self._db, circuit,
                                            since_utc=today_ts)
            volume_weekly = get_weekly_volume(self._db, circuit,
                                              since_utc=week_ts)
        return {
            "volume_daily":   volume_daily,
            "volume_weekly":  volume_weekly,
            "unit_context":   load_unit_context(self._db),
            "setup_complete": self.setup_complete,
            "valve_type":     self._valve_type_for(circuit),
            "degraded":       self._degraded_state_for(circuit),
            "anomaly":        self._anomaly_state_for(circuit),
        }

    async def _fetch_live_state(self, circuit: str) -> Dict[str, Any]:
        circuit_cfg = self._cfg.get_circuit(circuit)
        if not circuit_cfg or not self._ha:
            return {}

        # Fetch multiple states concurrently — include full state object
        # for leak_test_sensor so we can read last_changed for ETC
        entities = [
            circuit_cfg.valve_entity,
            circuit_cfg.pressure_avg_sensor,
            circuit_cfg.pressure_history_sensor,   # 2Hz, 1.375s smoothing — preferred for display
            circuit_cfg.flow_sensor,
            circuit_cfg.fault_sensor,
            circuit_cfg.fault_reason_sensor,
            circuit_cfg.trickle_sensor,
            circuit_cfg.leak_test_sensor,
            circuit_cfg.leak_test_result_sensor,
            circuit_cfg.volume_sensor,
        ]
        entities = [e for e in entities if e]

        results = await asyncio.gather(
            *[self._ha.get_state(eid) for eid in entities],
            return_exceptions=True,
        )
        full_states = {}
        states = {}
        for eid, result in zip(entities, results):
            if isinstance(result, Exception) or result is None:
                states[eid] = "unknown"
            else:
                full_states[eid] = result
                states[eid] = result.get("state", "unknown")

        # Daily / weekly volumes — prefer the authoritative HA cumulative
        # volume sensor (accurate to every pulse) over the internal
        # hourly_volume table (which only counts detected events).
        # Fall back to the internal table if the sensor isn't configured or
        # hasn't returned a usable reading yet.
        from .database import (get_daily_volume, get_weekly_volume,
                               compute_ha_daily_volume, compute_ha_weekly_volume)
        ha_volume_raw = states.get(circuit_cfg.volume_sensor, "")
        try:
            if ha_volume_raw not in ("", "unknown", None):
                from .ha_client import vol_to_litres as _v2l
                raw_f    = float(ha_volume_raw)
                vol_attrs = (full_states.get(circuit_cfg.volume_sensor) or {}).get("attributes") or {}
                vol_unit  = vol_attrs.get("unit_of_measurement", "")
                ha_volume_total = _v2l(raw_f, vol_unit)
            else:
                ha_volume_total = None
        except (ValueError, TypeError):
            ha_volume_total = None

        today_ts = self._local_midnight_utc(days_ago=0)
        week_ts  = self._local_midnight_utc(days_ago=7)

        # dev46 (46a): every DB read this method needs — volumes, unit
        # context, setup flag, valve type, degraded + anomaly state — is
        # gathered in ONE hop. They are all reads and all independent of the
        # HA state fetch above, so there is no split to make; done separately
        # they were nine event-loop-thread touches on a dashboard poll that
        # runs for every circuit on a timer.
        from .database import run_db
        dbst = await run_db(self._live_state_db_sync, circuit,
                            ha_volume_total, today_ts, week_ts)
        volume_daily  = dbst["volume_daily"]
        volume_weekly = dbst["volume_weekly"]

        fault_active = states.get(circuit_cfg.fault_sensor) == "on"

        # Fault reason — try dedicated reason sensor first, then
        # fall back to attributes on the binary sensor itself
        fault_reason = ""
        if fault_active:
            if circuit_cfg.fault_reason_sensor:
                r = states.get(circuit_cfg.fault_reason_sensor, "")
                if r and r not in ("unknown", "unavailable", ""):
                    fault_reason = r
            if not fault_reason and circuit_cfg.fault_sensor in full_states:
                attrs = full_states[circuit_cfg.fault_sensor].get(
                    "attributes", {})
                fault_reason = (
                    attrs.get("reason") or
                    attrs.get("fault_reason") or
                    attrs.get("cause") or ""
                )
        leak_test_active = states.get(circuit_cfg.leak_test_sensor) == "on"
        leak_test_etc = None
        leak_test_started_at = None
        leak_test_duration_secs = None
        if leak_test_active and circuit_cfg.leak_test_sensor in full_states:
            try:
                leak_test_started_at, leak_test_duration_secs, leak_test_etc = \
                    await self._compute_leak_test_etc(
                        circuit_cfg, full_states[circuit_cfg.leak_test_sensor])
            except Exception as e:
                log.warning("[%s] ETC computation error: %s", circuit, e)

        uc = dbst["unit_context"]

        _vt_raw = states.get(circuit_cfg.volume_sensor, "")
        try:
            _vt = f"{float(_vt_raw) * uc['vol_factor']:.{uc['vol_decimals']}f}" \
                  if _vt_raw not in ("", "unknown", "unavailable") else "—"
        except (ValueError, TypeError):
            _vt = "—"

        return {
            "circuit": circuit,
            "circuit_type": circuit_cfg.circuit_type,
            "display_name": circuit_cfg.label,
            "valve_state": states.get(circuit_cfg.valve_entity, "unknown"),
            "pressure": _fmt_sensor(
                states.get(circuit_cfg.pressure_history_sensor)
                if circuit_cfg.pressure_history_sensor
                and states.get(circuit_cfg.pressure_history_sensor)
                not in (None, "unknown", "unavailable", "")
                else states.get(circuit_cfg.pressure_avg_sensor),
                decimals=uc["pressure_decimals"], fallback="—",
                factor=uc["pressure_factor"]),
            "flow_rate": _fmt_sensor(
                states.get(circuit_cfg.flow_sensor),
                decimals=uc["flow_decimals"], fallback="0.00",
                factor=uc["flow_factor"]),
            "fault_active": fault_active,
            "fault_reason": fault_reason,
            "trickle_active": states.get(circuit_cfg.trickle_sensor) == "on",
            "leak_test_active": leak_test_active,
            "leak_test_etc": leak_test_etc,
            "leak_test_started_at": leak_test_started_at,   # ISO string for JS
            "leak_test_duration_secs": leak_test_duration_secs,  # float for JS
            "leak_test_result": states.get(
                circuit_cfg.leak_test_result_sensor, "No test run"),
            "volume_total": _vt,
            "volume_daily":  f"{volume_daily  * uc['vol_factor']:.{uc['vol_decimals']}f}",
            "volume_weekly": f"{volume_weekly * uc['vol_factor']:.{uc['vol_decimals']}f}",
            "leak_test_running": self._leak_test_scheduler.is_running(circuit)
            if self._leak_test_scheduler else False,
            "setup_complete": dbst["setup_complete"],
            # Valve type for this circuit; the device template uses it to
            # disable the manual leak-test button for 3-port valves.
            "valve_type": dbst["valve_type"],
            # Degraded-supply guard status. Python-computed UTC ISO cutoffs
            # so the comparison format matches stored start_ts exactly.
            **dbst["degraded"],
            # Unreviewed-anomaly triage count for the dashboard card.
            **dbst["anomaly"],
        }

    def _valve_type_for(self, circuit: str) -> str:
        """Return the current valve_type for a circuit (forgiving)."""
        try:
            from .database import get_valve_type
            return get_valve_type(self._db, circuit)
        except Exception as e:
            log.warning("[%s] valve_type lookup failed: %s", circuit, e)
            return "2_port"

    def _degraded_state_for(self, circuit: str) -> Dict[str, Any]:
        """Return {degraded_active, degraded_events_24h} for the dashboard."""
        try:
            now = datetime.now(timezone.utc)
            cutoff_30min = (now - timedelta(minutes=30)).isoformat()
            cutoff_24h   = (now - timedelta(hours=24)).isoformat()
            active = self._db.execute(
                "SELECT 1 FROM events WHERE circuit = ? "
                "AND degraded_supply = 1 AND start_ts >= ? LIMIT 1",
                (circuit, cutoff_30min),
            ).fetchone()
            day_row = self._db.execute(
                "SELECT COUNT(*) AS n FROM events WHERE circuit = ? "
                "AND degraded_supply = 1 AND start_ts >= ?",
                (circuit, cutoff_24h),
            ).fetchone()
            return {
                "degraded_active":     bool(active),
                "degraded_events_24h": int(day_row["n"] or 0) if day_row else 0,
            }
        except Exception as e:
            log.warning("[%s] degraded-state query failed: %s", circuit, e)
            return {"degraded_active": False, "degraded_events_24h": 0}

    def _anomaly_state_for(self, circuit: str) -> Dict[str, Any]:
        """Return {anomalies_unreviewed} — flagged events awaiting triage.

        No time window: an unreviewed anomaly stays on the card until the user
        marks it reviewed (or relabels it), so nothing flagged can silently
        age out unseen. Mirrors _degraded_state_for's forgiving error shape.
        """
        try:
            row = self._db.execute(
                "SELECT COUNT(*) AS n FROM events WHERE circuit = ? "
                "AND flagged = 1 AND COALESCE(user_reviewed, 0) = 0",
                (circuit,),
            ).fetchone()
            return {"anomalies_unreviewed": int(row["n"] or 0) if row else 0}
        except Exception as e:
            log.warning("[%s] anomaly-state query failed: %s", circuit, e)
            return {"anomalies_unreviewed": 0}

    async def _run_volume_baseline_rollover(self) -> None:
        """Re-capture the daily + weekly volume baselines at each local midnight.

        ``_init_volume_baselines`` runs once at startup; without a rollover, after
        the local day ticks over the dashboard's "today" baseline key has no
        snapshot and the per-day volume balloons toward the full cumulative meter
        total. This re-derives both baselines (force-overwriting) from HA history
        shortly after each local midnight so the daily / 7-day figures stay
        accurate without a restart.
        """
        while not self._stop.is_set():
            # Sleep until just after the next local midnight (+120s so HA's
            # recorder has the post-midnight reading on hand). Interruptible by
            # the stop event so shutdown isn't blocked for hours.
            delay = self._seconds_until_next_local_midnight() + 120.0
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                return  # stop requested during the wait
            except asyncio.TimeoutError:
                pass
            try:
                await self._init_volume_baselines(force=True)
                log.info("Volume baselines refreshed after local-midnight rollover")
            except Exception as e:
                log.warning("Volume baseline rollover failed (non-fatal): %s", e)

    async def _run_waveform_purger(self) -> None:
        """Daily housekeeping: drop event_waveforms rows older than 60 days.

        The full-resolution flow/pressure waveforms are kept for the event
        detail modal but cost ~28 KB/event. Retention bounds storage. The
        underlying event row is untouched (cascade is from event to waveform,
        not the other way).

        DELETE is offloaded to a worker thread — on a populated DB it can
        touch thousands of rows in one shot, which would otherwise stall
        every other ingress request for the duration.
        """
        WAVEFORM_RETENTION_DAYS = 60
        # Wait ~30s after startup so the rest of the boot sequence finishes
        # before the first purge runs.
        await asyncio.sleep(30)
        from .database import run_db
        while not self._stop.is_set():
            try:
                cutoff = (datetime.now(timezone.utc)
                          - timedelta(days=WAVEFORM_RETENTION_DAYS)).isoformat()
                rowcount = await run_db(self._purge_waveforms_sync, cutoff)
                if rowcount:
                    log.info("Purged %d waveform row(s) older than %d days",
                             rowcount, WAVEFORM_RETENTION_DAYS)
            except Exception as e:
                log.warning("Waveform purge failed (non-fatal): %s", e)
            # Sleep 24h, exiting promptly if stop is signaled.
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=24 * 3600)
            except asyncio.TimeoutError:
                pass

    def _purge_waveforms_sync(self, cutoff: str) -> int:
        """DELETE old event_waveforms rows. Runs on the single DB thread —
        the calling async wrapper submits it via run_db (dev46 46a)."""
        cur = self._db.execute(
            "DELETE FROM event_waveforms WHERE created_at < ?",
            (cutoff,),
        )
        self._db.commit()
        return cur.rowcount

    async def _compute_leak_test_etc(
        self,
        circuit_cfg,
        leak_test_state: dict,
    ) -> tuple:
        """
        Returns (started_at_iso, total_duration_secs, etc_string).
        started_at_iso : ISO timestamp when the test switch went ON
        total_duration_secs : 60s settle + test duration in seconds (for JS)
        etc_string : human-readable remaining time string (server-side initial)
        Returns (None, None, None) if not computable.

        The ESP firmware sequence from switch-on:
          0s   — valve closes, preparing flag set
          60s  — settle complete, monitoring begins
          60+N — monitoring ends (N = leak_test_duration entity value, in MINUTES)

        The Leak Test Active binary sensor is ON throughout the full period.
        last_changed on that sensor is therefore the switch-on moment.
        """
        import datetime as dt

        last_changed_str = leak_test_state.get("last_changed")
        if not last_changed_str:
            return None, None, None
        try:
            started = dt.datetime.fromisoformat(
                last_changed_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None, None, None

        # Fetch the leak test duration from the stored entity ID.
        # The entity value is in MINUTES (e.g. 10 = 10 minutes).
        duration_mins = None
        duration_eid = circuit_cfg.leak_test_duration_entity
        if duration_eid:
            val = await self._ha.get_state_value(duration_eid)
            if val and val not in ("unknown", "unavailable"):
                try:
                    duration_mins = float(val)
                except (ValueError, TypeError):
                    pass

        if duration_mins is None:
            # Fallback: entity not yet discovered — use firmware's initial_value.
            duration_mins = 10.0
            log.warning("[%s] leak test duration entity not found (id=%r), "
                        "using 10min default — re-run setup wizard to fix",
                        circuit_cfg.circuit, duration_eid)

        # Total time from switch-on: 60s settle + test duration
        SETTLE_SECS = 60
        total_secs = SETTLE_SECS + duration_mins * 60

        now = dt.datetime.now(dt.timezone.utc)
        elapsed = (now - started).total_seconds()
        remaining = total_secs - elapsed

        if elapsed < SETTLE_SECS:
            settle_left = int(SETTLE_SECS - elapsed)
            etc_str = f"Settling… {settle_left}s"
        elif remaining <= 0:
            etc_str = "Completing…"
        else:
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            etc_str = (f"{mins}m {secs:02d}s remaining"
                       if mins > 0 else f"{secs}s remaining")

        return last_changed_str, total_secs, etc_str
