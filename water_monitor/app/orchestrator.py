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
from typing import Any, Dict, Optional

from .config import AddonConfig, SENSITIVITY_PRESETS, DB_PATH
from .database import (get_sensitivity_config, ensure_circuit_defaults, init_db,
                       dedup_events)
from .device_discovery import (load_circuit_entities, is_setup_complete,
                                get_device_config)
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
from .fixture_publisher import FixturePublisher

log = logging.getLogger(__name__)


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
        self._fixture_publisher: Optional[FixturePublisher] = None
        self._stop = asyncio.Event()

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

        self._db.execute("""
            UPDATE home_profile SET
                away_mode  = ?,
                away_since = CASE WHEN ? THEN ? ELSE NULL END,
                updated_at = ?
            WHERE id = 1
        """, (1 if enabled else 0, enabled, now_iso, now_iso))
        self._db.commit()

        if self._alert_manager and enabled:
            await self._alert_manager.alert_away_mode_on()
        log.info("Away mode %s", "enabled" if enabled else "disabled")

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
            circuit_cfg.volume_sensor = entities.get("volume_sensor", "")
            circuit_cfg.esp_device_prefix = prefix

            log.debug("[%s] entity IDs loaded from DB — fully_configured=%s",
                      circuit_cfg.circuit, circuit_cfg.is_fully_configured)

    def stop(self) -> None:
        self._stop.set()
        if self._feature_extractor:
            self._feature_extractor.stop()
        if self._training_manager:
            self._training_manager.stop()
        if self._data_pruner:
            self._data_pruner.stop()
        if self._leak_test_scheduler:
            self._leak_test_scheduler.stop()
        if self._ha:
            self._ha.stop()

    async def run(self) -> None:
        """Initialise and run all components concurrently."""
        # Database
        self._db = init_db(DB_PATH)

        # Ensure per-circuit defaults exist
        for circuit_cfg in self._cfg.circuits:
            ensure_circuit_defaults(
                self._db, circuit_cfg.circuit, circuit_cfg.circuit_type)

        # Startup dedup — cleans up any residual duplicates that slipped through
        # before migration 021 added the UNIQUE(circuit, start_ts) constraint.
        # This is a no-op on clean databases; it exists as a safety net for
        # legacy data that migrated from pre-fix installs.
        try:
            _deduped = dedup_events(self._db)
            if _deduped:
                log.warning("startup dedup: removed %d duplicate event(s)", _deduped)
        except Exception as _e:
            log.warning("startup dedup failed (non-fatal): %s", _e)

        # HA client
        self._ha = HaClient()
        await self._ha.__aenter__()

        # Load entity IDs from DB into circuit configs
        self.reload_circuit_entities()

        # Event queue
        self._event_queue = asyncio.Queue(maxsize=1000)

        # Training manager
        self._training_manager = TrainingManager(
            self._cfg, self._db, self._ha)
        self._data_pruner = DataPruner(self._db, db_path=DB_PATH)
        self._alert_manager = AlertManager(self._db, self._ha)
        self._presence_watcher = PresenceWatcher(
            self._db, self._ha, self.set_away_mode)
        self._presence_watcher.setup()
        await self._presence_watcher.sync_initial_state()

        # Leak test scheduler
        self._leak_test_scheduler = LeakTestScheduler(
            self._cfg, self._db, self._ha, self._alert_manager)

        # Historical importer — backfills missed events and runs periodic catch-up
        self._historical_importer = HistoricalImporter(
            self._cfg, self._db, self._ha, self._event_queue)

        # Event detector — only if setup is complete and entities are loaded
        self._event_detector = EventDetector(
            circuits=self._cfg.circuits,
            ha_client=self._ha,
            event_queue=self._event_queue,
            sensitivity_getter=self._get_sensitivity,
        )
        if self.setup_complete:
            self._event_detector.setup()
            log.info("Event detection active")
        else:
            log.info("Setup not complete — event detection paused until wizard finishes")

        # Feature extractor
        self._feature_extractor = FeatureExtractor(
            self._event_queue, self._db, self._alert_manager)

        # Cluster engine — instantiate and rebuild state from the last 60 days
        # of already-matched events so DBSTREAM + scaler are warm on startup.
        try:
            from .cluster_engine import ClusterEngine
            loop = asyncio.get_running_loop()
            self._cluster_engine = ClusterEngine(self._db, self._cfg)
            for c in self._cfg.circuits:
                count = await loop.run_in_executor(
                    None, self._cluster_engine.rebuild_from_db, c.circuit
                )
                log.info("[%s] cluster state rebuilt — %d events replayed",
                         c.circuit, count)
                # Backfill events that had no cluster_id (e.g. v0.1.x upgrades)
                backfilled = await loop.run_in_executor(
                    None, self._cluster_engine.backfill_unmatched, c.circuit
                )
                if backfilled:
                    log.info("[%s] backfilled cluster_id on %d previously unmatched events",
                             c.circuit, backfilled)
            self._feature_extractor.cluster_engine = self._cluster_engine
            # Wire to training_manager so complete_calibration can trigger backfill
            self._training_manager.cluster_engine = self._cluster_engine
            log.info("ClusterEngine initialised and wired to feature extractor")
        except Exception as e:
            log.error("ClusterEngine init failed (non-fatal): %s", e, exc_info=True)

        # Initialise daily/weekly volume baselines from HA history so that
        # the dashboard shows accurate totals from the first page load.
        try:
            await self._init_volume_baselines()
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

        # Fixture publisher — MQTT Discovery for confirmed fixtures
        self._fixture_publisher = FixturePublisher(self._db, self._cfg, self._ha)
        try:
            await self._fixture_publisher.start()
        except Exception as e:
            log.warning("FixturePublisher start failed (non-fatal): %s", e)

        # Run all background tasks concurrently
        try:
            await asyncio.gather(
                self._ha.run_event_loop(),
                self._feature_extractor.run(),
                self._training_manager.run(),
                self._data_pruner.run(),
                self._leak_test_scheduler.run(),
                self._historical_importer.run(),
                self._cluster_metrics.run(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._ha.__aexit__(None, None, None)

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

        return {
            "pressure_drop_event_psi": (
                row["pressure_drop_event_psi"]
                or preset["pressure_drop_event_psi"]
            ),
            "min_event_duration_seconds": (
                row["min_event_duration_seconds"]
                or preset["min_event_duration_seconds"]
            ),
            "score_alert": row["score_alert"] or preset["score_alert"],
            "score_shutoff": row["score_shutoff"] or preset["score_shutoff"],
            "flow_tolerance_pct": (
                row["flow_tolerance_pct"] or preset["flow_tolerance_pct"]
            ),
            "duration_tolerance_pct": (
                row["duration_tolerance_pct"] or preset["duration_tolerance_pct"]
            ),
            "schedule_window_minutes": (
                row["schedule_window_minutes"] or preset["schedule_window_minutes"]
            ),
            "sustained_alert_minutes": (
                row["sustained_alert_minutes"] or preset["sustained_alert_minutes"]
            ),
            "max_shutoffs_per_12h": (
                row["max_shutoffs_per_12h"] or preset["max_shutoffs_per_12h"]
            ),
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
        cache_key = f"_live_state_{circuit}"
        cached = getattr(self, cache_key, None)
        if cached and now_ts - cached.get("_fetched_at", 0) < 3.0:
            return cached

        result = await self._fetch_live_state(circuit)
        result["_fetched_at"] = now_ts
        setattr(self, cache_key, result)
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
        from .units import defaults_from_ha, invalidate_unit_cache
        row = self._db.execute(
            "SELECT flow_unit, pressure_unit FROM home_profile WHERE id = 1"
        ).fetchone()
        # Skip if either unit has been explicitly changed from schema defaults
        if row and (
            (row["flow_unit"]     and row["flow_unit"]     != "L/min") or
            (row["pressure_unit"] and row["pressure_unit"] != "psi")
        ):
            return
        ha_units = await self._ha.get_ha_unit_system()
        ha_vol   = ha_units.get("volume", "L")
        flow_key, pressure_key = defaults_from_ha(ha_vol)
        # ON CONFLICT handles both fresh install (no row) and existing row
        self._db.execute("""
            INSERT INTO home_profile (id, flow_unit, pressure_unit)
            VALUES (1, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                flow_unit     = excluded.flow_unit,
                pressure_unit = excluded.pressure_unit
        """, (flow_key, pressure_key))
        self._db.commit()
        invalidate_unit_cache()
        log.info("Display units auto-detected from HA: flow=%s pressure=%s",
                 flow_key, pressure_key)

    async def _init_volume_baselines(self) -> None:
        """
        Query HA history to set accurate midnight baselines for daily/weekly
        volume calculations.  Called once at startup before the main loop.

        Without this, _get_volume_baseline() uses 0.0 as a placeholder on
        the first call, which causes the dashboard to show the full cumulative
        sensor total rather than just today's volume.  This method overwrites
        the placeholder with the real midnight reading from HA history.

        period_ts keys MUST use local-time midnight (no tzinfo) to match the
        keys produced by compute_ha_daily_volume / compute_ha_weekly_volume in
        database.py.  HA history queries use UTC datetimes separately.
        """
        from datetime import datetime, timezone, timedelta

        # Local midnight — matches compute_ha_daily_volume key format
        now_local       = datetime.now()
        today_midnight  = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        week_monday     = today_midnight - timedelta(days=today_midnight.weekday())

        # UTC equivalents for HA history queries
        now_utc              = datetime.now(timezone.utc)
        today_midnight_utc   = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        week_monday_utc      = today_midnight_utc - timedelta(days=today_midnight_utc.weekday())

        for cfg in self._cfg.circuits:
            if not cfg.volume_sensor:
                continue

            circuit = cfg.circuit

            for period_start_local, period_start_utc, label in [
                (today_midnight,  today_midnight_utc,  "today"),
                (week_monday,     week_monday_utc,     "this week"),
            ]:
                period_ts = period_start_local.isoformat(timespec="seconds")

                # Only fix baselines that are still at the 0.0 placeholder
                row = self._db.execute(
                    "SELECT ha_volume FROM volume_snapshots "
                    "WHERE circuit=? AND period_ts=?",
                    (circuit, period_ts),
                ).fetchone()

                if row is not None and row[0] != 0.0:
                    continue   # already set to a real value

                # Query HA history for the earliest reading at/after midnight
                try:
                    hist = await self._ha.get_history(
                        cfg.volume_sensor,
                        period_start_utc,
                        period_start_utc + timedelta(hours=2),
                    )
                    if hist:
                        midnight_val = float(hist[0]["state"])
                    else:
                        midnight_val = 0.0
                except Exception as e:
                    log.debug("[%s] could not fetch volume history for %s: %s",
                              circuit, label, e)
                    continue

                self._db.execute("""
                    INSERT INTO volume_snapshots (circuit, period_ts, ha_volume)
                    VALUES (?,?,?)
                    ON CONFLICT (circuit, period_ts)
                    DO UPDATE SET ha_volume = excluded.ha_volume
                """, (circuit, period_ts, midnight_val))
                self._db.commit()
                log.info("[%s] volume baseline set for %s: %.2f L",
                         circuit, label, midnight_val)

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
            ha_volume_total = float(ha_volume_raw) if ha_volume_raw not in ("", "unknown", None) else None
        except (ValueError, TypeError):
            ha_volume_total = None

        if ha_volume_total is not None and ha_volume_total >= 0:
            volume_daily  = compute_ha_daily_volume(self._db, circuit, ha_volume_total)
            volume_weekly = compute_ha_weekly_volume(self._db, circuit, ha_volume_total)
        else:
            volume_daily  = get_daily_volume(self._db, circuit)
            volume_weekly = get_weekly_volume(self._db, circuit)

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

        from .units import load_unit_context
        uc = load_unit_context(self._db)

        return {
            "circuit": circuit,
            "circuit_type": circuit_cfg.circuit_type,
            "display_name": circuit_cfg.display_name,
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
            "volume_total": states.get(circuit_cfg.volume_sensor, "0"),
            "volume_daily":  f"{volume_daily  * uc['vol_factor']:.{uc['vol_decimals']}f}",
            "volume_weekly": f"{volume_weekly * uc['vol_factor']:.{uc['vol_decimals']}f}",
            "leak_test_running": self._leak_test_scheduler.is_running(circuit)
            if self._leak_test_scheduler else False,
            "setup_complete": self.setup_complete,
        }

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
