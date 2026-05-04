"""
HA Presence Watcher — auto-toggles away mode from entity state changes.

Watches one or more HA entities (person.*, device_tracker.*,
input_boolean.*, alarm_control_panel.*) and mirrors their combined
presence state into the Water Monitor away mode.

Logic:
  - When ALL watched entities are in ha_away_state → enable away mode
  - When ANY watched entity reaches ha_home_state  → disable away mode
  - If no entities are configured                 → does nothing (manual only)

The watcher subscribes to real-time state_changed events via the
persistent HA WebSocket connection already maintained by HaClient.

Supported entity types and their typical state values:
  person.*              home / not_home  (HA standard)
  device_tracker.*      home / not_home  (HA standard)
  input_boolean.*       on   / off       (set away_state=on, home_state=off)
  alarm_control_panel.* armed_away / disarmed
  binary_sensor.*       off  / on        (occupancy sensors — inverted)
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Dict, List, Optional

log = logging.getLogger(__name__)


class PresenceWatcher:
    """
    Subscribes to HA presence entity state changes and calls
    orchestrator.set_away_mode() when the household departs or returns.
    """

    def __init__(self, db: sqlite3.Connection, ha_client,
                 set_away_mode_cb):
        self._db  = db
        self._ha  = ha_client
        self._cb  = set_away_mode_cb
        self._states: Dict[str, str] = {}
        self._configured = False
        self._pending_eval: Optional[asyncio.Task] = None

    # ── Setup ──────────────────────────────────────────────────────────

    def setup(self) -> None:
        """Register HA callbacks for all configured presence entities."""
        profile = self._load_profile()
        if not profile:
            return

        entities = self._parse_entities(profile.get("ha_presence_entities", ""))
        if not entities:
            log.debug("Presence watcher: no entities configured")
            return

        for eid in entities:
            self._ha.subscribe_entity(eid, self._on_state_changed)
            log.info("Presence watcher: watching %s", eid)

        self._configured = True
        log.info("Presence watcher active — %d entity/entities", len(entities))

    async def sync_initial_state(self) -> None:
        """
        Called once at startup — read current entity states from HA and
        immediately sync away mode without waiting for a state_changed event.
        """
        profile = self._load_profile()
        if not profile:
            return

        entities = self._parse_entities(profile.get("ha_presence_entities", ""))
        if not entities:
            return

        for eid in entities:
            val = await self._ha.get_state_value(eid)
            if val:
                self._states[eid] = val
                log.debug("Presence watcher initial state: %s = %s", eid, val)

        await self._evaluate(profile)

    def reload(self) -> None:
        """
        Re-read config and re-subscribe.
        Call this after the user saves new presence settings.
        """
        profile = self._load_profile()
        if not profile:
            return
        entities = self._parse_entities(profile.get("ha_presence_entities", ""))
        for eid in entities:
            self._ha.subscribe_entity(eid, self._on_state_changed)
        self._configured = bool(entities)

    # ── State change callback ──────────────────────────────────────────

    def _on_state_changed(self, entity_id: str, state: str,
                          attributes: dict) -> None:
        """Called by HaClient on every state_changed event for watched entities."""
        if not state:
            return

        old = self._states.get(entity_id)
        self._states[entity_id] = state

        if old == state:
            return   # no change

        log.info("Presence: %s → %s", entity_id, state)

        profile = self._load_profile()
        if not profile:
            return

        # Cancel any in-flight evaluation — rapid state changes (e.g. flapping
        # sensor) should only trigger one evaluation, not dozens concurrently.
        if self._pending_eval and not self._pending_eval.done():
            self._pending_eval.cancel()
        self._pending_eval = asyncio.create_task(self._evaluate(profile))

    # ── Evaluation logic ───────────────────────────────────────────────

    async def _evaluate(self, profile: dict) -> None:
        """
        Decide whether to toggle away mode based on current entity states.
        """
        entities    = self._parse_entities(profile.get("ha_presence_entities", ""))
        away_state  = profile.get("ha_away_state", "not_home")
        home_state  = profile.get("ha_home_state", "home")
        current_away = bool(profile.get("away_mode", 0))

        if not entities:
            return

        # Fill in states we don't have yet
        for eid in entities:
            if eid not in self._states:
                val = await self._ha.get_state_value(eid)
                if val:
                    self._states[eid] = val

        known_states = {eid: self._states.get(eid) for eid in entities}

        all_away = all(
            s == away_state for s in known_states.values() if s
        )
        any_home = any(
            s == home_state for s in known_states.values() if s
        )

        log.debug(
            "Presence eval: states=%s all_away=%s any_home=%s current_away=%s",
            known_states, all_away, any_home, current_away,
        )

        if all_away and not current_away:
            log.info("Presence: all occupants away — enabling away mode")
            await self._cb(True)
        elif any_home and current_away:
            log.info("Presence: occupant returned — disabling away mode")
            await self._cb(False)

    # ── Helpers ────────────────────────────────────────────────────────

    def _load_profile(self) -> Optional[dict]:
        try:
            row = self._db.execute(
                "SELECT * FROM home_profile WHERE id = 1").fetchone()
            return dict(row) if row else None
        except Exception as e:
            log.warning("PresenceWatcher: could not read home_profile: %s", e)
            return None

    @staticmethod
    def _parse_entities(raw: str) -> List[str]:
        """Parse comma-separated entity list, filter empties."""
        if not raw:
            return []
        return [e.strip() for e in raw.split(",") if e.strip()]
