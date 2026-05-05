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
                       get_home_profile, ensure_circuit_defaults)
from .ha_client import HaClient

log = logging.getLogger(__name__)


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
        # Set by orchestrator after ClusterEngine is initialised
        self.cluster_engine = None

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Background loop — check calibration progress every 60s."""
        # Initial setup
        for circuit_cfg in self._cfg.circuits:
            ensure_circuit_defaults(
                self._db, circuit_cfg.circuit, circuit_cfg.circuit_type)

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

    async def start_calibration(self, circuit: str,
                                calibration_days: int) -> bool:
        """
        Start calibration for a circuit. Returns True if started.
        Can only start from 'idle' state.
        """
        state_row = get_training_state(self._db, circuit)
        current = state_row["state"] if state_row else "idle"

        if current not in ("idle", "calibrating"):
            log.warning("[%s] cannot start calibration from state '%s'",
                        circuit, current)
            return False

        profile = get_home_profile(self._db)
        minimum_events = compute_minimum_events(
            profile["bathrooms_full"] or 2,
            profile["bathrooms_half"] or 0,
            profile["floors"] or 1,
        )

        now = datetime.now(timezone.utc)
        ends_at = now + timedelta(days=calibration_days)

        upsert_training_state(
            self._db, circuit,
            state="calibrating",
            calibration_days=calibration_days,
            started_at=now.isoformat(),
            calibration_ends_at=ends_at.isoformat(),
            minimum_events=minimum_events,
            events_collected=0,
        )

        await self._publish_status(circuit)
        log.info("[%s] calibration started — %d days, minimum %d events",
                 circuit, calibration_days, minimum_events)
        return True

    async def stop_calibration(self, circuit: str) -> None:
        """Cancel calibration and return to idle."""
        upsert_training_state(
            self._db, circuit,
            state="idle",
            started_at=None,
            calibration_ends_at=None,
        )
        await self._publish_status(circuit)
        log.info("[%s] calibration cancelled", circuit)

    async def complete_calibration(self, circuit: str) -> None:
        """Transition calibrating → live."""
        now = datetime.now(timezone.utc)
        upsert_training_state(
            self._db, circuit,
            state="live",
            completed_at=now.isoformat(),
        )
        await self._publish_status(circuit)

        # Backfill any events that accumulated before the engine was first
        # instantiated (e.g. installs that upgraded from v0.1.x mid-calibration)
        if self.cluster_engine is not None:
            try:
                import asyncio as _asyncio, functools
                loop = _asyncio.get_running_loop()
                backfilled = await loop.run_in_executor(
                    None,
                    functools.partial(self.cluster_engine.backfill_unmatched, circuit),
                )
                if backfilled:
                    log.info("[%s] post-calibration backfill: %d events matched",
                             circuit, backfilled)
            except Exception as e:
                log.warning("[%s] post-calibration backfill failed (non-fatal): %s",
                            circuit, e)

        circuit_cfg = self._cfg.get_circuit(circuit)
        if circuit_cfg:
            await self._ha.notify(
                title=f"Water Monitor — {circuit_cfg.display_name} training complete",
                message=(
                    f"The {circuit_cfg.display_name.lower()} circuit has finished "
                    f"its training period. Visit Fixtures to review detected clusters."
                ),
                notification_id=f"water_calibration_complete_{circuit}",
            )
        log.info("[%s] calibration complete — transitioning to live", circuit)

    async def trigger_full_recalibration(self, circuit: str,
                                         days: int) -> bool:
        """Reset to idle then start fresh calibration."""
        upsert_training_state(
            self._db, circuit,
            state="idle",
            events_collected=0,
        )
        # Clear all per-circuit data so history charts and volume totals
        # start fresh — daily_summary and import_state are included so the
        # importer re-scans and the chart doesn't show pre-reset data.
        for table, col in [
            ("events",          "circuit"),
            ("hourly_volume",   "circuit"),
            ("daily_summary",   "circuit"),
            ("import_state",    "circuit"),
            ("volume_snapshots","circuit"),
        ]:
            try:
                self._db.execute(
                    f"DELETE FROM {table} WHERE {col} = ?", (circuit,))
            except Exception as e:
                log.warning("[%s] recalibration clear %s: %s", circuit, table, e)
        self._db.commit()
        return await self.start_calibration(circuit, days)

    async def trigger_partial_recalibration(self, circuit: str) -> None:
        """
        Partial recalibration — reset behavioural patterns but keep
        fixture signatures. In Phase 1 this just resets the training
        state to idle and starts a new accelerated adaptation window.
        """
        from .database import upsert_learning_config
        now = datetime.now(timezone.utc)
        accel_until = (now + timedelta(days=14)).isoformat()
        upsert_learning_config(
            self._db, circuit,
            accelerated_adaptation_until=accel_until,
            accelerated_adaptation_reason="partial_recalibration",
        )
        log.info("[%s] partial recalibration — accelerated adaptation for 14 days",
                 circuit)

    async def _check_progress(self, circuit: str) -> None:
        """Check if calibration should complete automatically.

        Pauses the calibration timer while away mode is active — the
        calibration_ends_at timestamp is extended by 1 day for every day
        spent in away mode so the learning period reflects actual occupancy.
        """
        state_row = get_training_state(self._db, circuit)
        if not state_row or state_row["state"] != "calibrating":
            return

        # Check away mode — pause calibration timer while away.
        # The timer is extended by the true away duration when the occupant
        # returns (see orchestrator.set_away_mode), so we just early-return here.
        try:
            profile = self._db.execute(
                "SELECT away_mode FROM home_profile WHERE id = 1"
            ).fetchone()
            if profile and profile["away_mode"]:
                log.debug("[%s] away mode active — calibration check deferred",
                          circuit)
                return
        except Exception as e:
            log.warning("[%s] away mode check failed: %s", circuit, e)

        now = datetime.now(timezone.utc)

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
            # Extend calibration — notify user
            log.warning(
                "[%s] calibration time elapsed but only %d/%d events collected — extending",
                circuit,
                state_row["events_collected"],
                state_row["minimum_events"],
            )
            circuit_cfg = self._cfg.get_circuit(circuit)
            if circuit_cfg:
                await self._ha.notify(
                    title=f"Water Monitor — Training extended",
                    message=(
                        f"{circuit_cfg.display_name}: training period elapsed but only "
                        f"{state_row['events_collected']} of "
                        f"{state_row['minimum_events']} events collected. "
                        f"Training continues automatically."
                    ),
                    notification_id=f"water_training_extended_{circuit}",
                )

    async def _publish_status(self, circuit: str) -> None:
        """Publish training status sensor to HA."""
        state_row = get_training_state(self._db, circuit)
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
                event_pct = min(100, int(
                    (state_row["events_collected"] or 0) /
                    max(state_row["minimum_events"] or 1, 1) * 100
                ))
                # Progress is purely time-based for user-facing display
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

            event_pct = min(100, int(
                (state_row["events_collected"] or 0) /
                max(state_row["minimum_events"] or 1, 1) * 100
            ))
            remaining_td = max(ends_at - now, timedelta(0))
            result["days_remaining"]  = remaining_td.days
            result["hours_remaining"] = remaining_td.seconds // 3600
            # Percent complete is purely time-based — events are an internal
            # metric and don't affect the displayed progress.
            result["percent_complete"] = time_pct
        else:
            result["percent_complete"]  = 100 if state_row["state"] == "live" else 0
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
