"""
Fixture publisher — Phase 2 (Gap 6).

Publishes water fixture activity to Home Assistant via MQTT Discovery.
After the Sprint D taxonomy consolidation each fixture type is its own
HA category, so per circuit the publisher creates up to one entity set
per type present:

  sensor.water_monitor_{circuit}_{type}_count_today
  sensor.water_monitor_{circuit}_{type}_volume_today
  binary_sensor.water_monitor_{circuit}_{type}_running

Types / categories: toilet, shower_tub, tap, washing_machine,
dishwasher, irrigation_zone, other.

NOTE: The old "appliance" and "irrigation" super-categories were
removed in Sprint D. Any HA entities created under those slugs will
be orphaned and should be deleted from HA by the user.

Broker credentials are fetched from the HA supervisor at startup:
  GET http://supervisor/services/mqtt  (requires SUPERVISOR_TOKEN env var)

State updates run every 60 seconds for all published circuits.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
from typing import Dict, Optional

log = logging.getLogger(__name__)

_DISCOVERY_PREFIX = "homeassistant"
_NODE_ID          = "water_monitor"


def _slugify(name: str) -> str:
    """Convert a name to a safe HA entity slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_") or "fixture"


def _category_slug(circuit: str, category: str) -> str:
    """Return the HA entity slug for a circuit + category."""
    return f"{_slugify(circuit)}_{category}"


class FixturePublisher:
    """Publishes fixture category states to HA via MQTT Discovery."""

    def __init__(self, db: sqlite3.Connection, cfg, ha_client):
        self._db  = db
        self._cfg = cfg
        self._ha  = ha_client
        self._client = None
        self._connected = False
        self._status = "not_configured"

    def status(self) -> str:
        """Return connection status for the Settings page."""
        return self._status

    async def start(self) -> None:
        """Fetch MQTT broker credentials from supervisor and connect."""
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            log.warning("paho-mqtt not installed — fixture publishing disabled")
            self._status = "not_configured"
            return

        creds = await self._get_broker_creds()
        if not creds:
            self._status = "not_configured"
            return

        self._loop = asyncio.get_running_loop()

        try:
            client = mqtt.Client(client_id=f"water_monitor_{os.getpid()}")
            if creds.get("username"):
                client.username_pw_set(creds["username"], creds.get("password"))
            client.on_connect    = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.connect(creds["host"], int(creds.get("port", 1883)), keepalive=60)
            client.loop_start()
            self._client = client
            log.info("Fixture publisher connecting to MQTT broker %s:%s",
                     creds["host"], creds.get("port", 1883))
        except Exception as e:
            log.error("Fixture publisher MQTT connect failed: %s", e)
            self._status = "broker_error"

    async def _get_broker_creds(self) -> Optional[dict]:
        """Query HA supervisor for MQTT broker service config."""
        supervisor_token = os.environ.get("SUPERVISOR_TOKEN", "")
        if not supervisor_token:
            log.debug("No SUPERVISOR_TOKEN — MQTT discovery unavailable")
            return None
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "http://supervisor/services/mqtt",
                    headers={"Authorization": f"Bearer {supervisor_token}"},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        log.warning("Supervisor MQTT query returned %d", resp.status)
                        return None
                    data = await resp.json()
                    return data.get("data", {})
        except Exception as e:
            log.warning("Supervisor MQTT query failed: %s", e)
            return None

    def _on_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            self._connected = True
            self._status = "connected"
            log.info("Fixture publisher connected to MQTT broker")
            loop = getattr(self, "_loop", None)
            if loop and loop.is_running():
                loop.call_soon_threadsafe(self._publish_all_confirmed_sync)
            else:
                log.warning("Fixture publisher: event loop unavailable on connect "
                            "— deferred re-publish skipped")
        else:
            self._connected = False
            self._status = "broker_error"
            log.warning("Fixture publisher MQTT connect failed, rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc) -> None:
        self._connected = False
        self._status = "disconnected"
        log.warning("Fixture publisher disconnected from MQTT broker (rc=%d)", rc)

    def _is_publishing_enabled(self) -> bool:
        """Check global mqtt_publish_enabled flag from home_profile."""
        try:
            row = self._db.execute(
                "SELECT mqtt_publish_enabled FROM home_profile WHERE id = 1"
            ).fetchone()
            return bool(row and row["mqtt_publish_enabled"])
        except Exception:
            return False

    # ── Category helpers ────────────────────────────────────────────────────────

    def _categories_for_circuit(self, circuit: str) -> Dict[str, dict]:
        """Return {category: {label, circuit_label}} for categories that have
        any non-phantom events on this circuit AND are publish_to_ha=1 in the
        category_publish table.

        Sprint F change: source of truth is now ``category_publish`` (per-
        (circuit, fixture_type) gate), not per-fixture ``fixtures.publish_to_ha``.
        Categories with no row in category_publish default to publish=True.
        """
        from .database import get_category_publish_map
        from .fixtures import (FIXTURE_CATEGORY_LABELS,
                               fixture_user_selectable_types,
                               normalize_fixture_type_for_circuit,
                               zone_user_selectable_types)

        circ_label = circuit
        circuit_kind = "fixture"
        for c in self._cfg.circuits:
            if c.circuit == circuit:
                circ_label = getattr(c, "label", circuit)
                if getattr(c, "circuit_type", None) == "zone":
                    circuit_kind = "zone"
                break

        # Effective-type set present on the circuit (excluding phantoms),
        # normalized into the canonical allowed-set for this circuit kind.
        rows = self._db.execute(
            """SELECT DISTINCT
                 COALESCE(e.user_fixture_type, f.fixture_type, fc.suggested_type,
                          e.matched_fixture_type, 'other') AS eff_type
               FROM events e
               LEFT JOIN fixtures f          ON e.fixture_id = f.id
               LEFT JOIN fixture_clusters fc ON fc.circuit = e.circuit AND fc.id = e.cluster_id
               WHERE e.circuit = ?
                 AND COALESCE(e.is_pressure_restoration_phantom, 0) = 0""",
            (circuit,),
        ).fetchall()

        allowed = set(zone_user_selectable_types() if circuit_kind == "zone"
                      else fixture_user_selectable_types())
        present_types = {
            normalize_fixture_type_for_circuit(r["eff_type"], circuit_kind)
            for r in rows
        }
        present_types &= allowed   # belt-and-braces; normalizer already enforces this

        publish_map = get_category_publish_map(self._db, circuit)
        cats: Dict[str, dict] = {}
        for typ in present_types:
            # Missing row → default ON (the documented contract for
            # get_category_publish_map; pinned by a test).
            if not publish_map.get(typ, True):
                continue
            cats[typ] = {
                "label":         FIXTURE_CATEGORY_LABELS.get(typ, typ.replace("_", " ").title()),
                "circuit_label": circ_label,
            }
        return cats

    def _publish_category_discovery(self, circuit: str, category: str,
                                    cat_label: str) -> None:
        """Publish MQTT Discovery payloads for one circuit+category."""
        slug  = _category_slug(circuit, category)
        name  = cat_label

        device = {
            "identifiers":  [f"{_NODE_ID}_{_slugify(circuit)}"],
            "name":         f"Water Monitor — {getattr(self._cfg, 'home_name', 'Home')}",
            "manufacturer": "Water Monitor",
            "model":        "Water Usage Monitor",
        }

        configs = [
            (
                "sensor",
                f"{_NODE_ID}_{slug}_count_today",
                f"{name} — uses today",
                f"{_DISCOVERY_PREFIX}/sensor/{_NODE_ID}_{slug}_count_today/state",
                None, None, "mdi:counter",
            ),
            (
                "sensor",
                f"{_NODE_ID}_{slug}_volume_today",
                f"{name} — volume today",
                f"{_DISCOVERY_PREFIX}/sensor/{_NODE_ID}_{slug}_volume_today/state",
                "water", "L", "mdi:water",
            ),
            (
                "binary_sensor",
                f"{_NODE_ID}_{slug}_running",
                f"{name} — running",
                f"{_DISCOVERY_PREFIX}/binary_sensor/{_NODE_ID}_{slug}_running/state",
                None, None, "mdi:water-pump",
            ),
        ]

        for component, object_id, friendly_name, state_topic, device_class, unit, icon in configs:
            payload: dict = {
                "name":        friendly_name,
                "unique_id":   f"{_NODE_ID}_{object_id}",
                "state_topic": state_topic,
                "device":      device,
                "icon":        icon,
            }
            if device_class:
                payload["device_class"] = device_class
            if unit:
                payload["unit_of_measurement"] = unit
                payload["state_class"] = "total_increasing"

            topic = f"{_DISCOVERY_PREFIX}/{component}/{object_id}/config"
            self._client.publish(topic, json.dumps(payload), retain=True)

        log.debug("Published Discovery for %s/%s (%s)", circuit, category, slug)

    def _publish_circuit_discovery(self, circuit: str) -> None:
        """Publish the per-circuit (device-level) anomaly binary_sensor (Phase 2.3).

        Not tied to a fixture type — it mirrors whether a recent event deviated from
        the home's FROZEN baseline. Automation-friendly (device_class 'problem') so a
        user can build their own response (dashboard card, valve shutoff, etc.); the
        built-in graduated response lives in feature_extractor, not here.
        """
        cslug = _slugify(circuit)
        object_id = f"{_NODE_ID}_{cslug}_unusual_usage"
        circ_label = circuit
        for c in self._cfg.circuits:
            if c.circuit == circuit:
                circ_label = getattr(c, "label", circuit)
                break
        device = {
            "identifiers":  [f"{_NODE_ID}_{cslug}"],
            "name":         f"Water Monitor — {getattr(self._cfg, 'home_name', 'Home')}",
            "manufacturer": "Water Monitor",
            "model":        "Water Usage Monitor",
        }
        payload = {
            "name":         f"{circ_label} — unusual usage",
            "unique_id":    f"{_NODE_ID}_{object_id}",
            "state_topic":  f"{_DISCOVERY_PREFIX}/binary_sensor/{object_id}/state",
            "device":       device,
            "device_class": "problem",
            "icon":         "mdi:water-alert",
        }
        topic = f"{_DISCOVERY_PREFIX}/binary_sensor/{object_id}/config"
        self._client.publish(topic, json.dumps(payload), retain=True)
        log.debug("Published unusual-usage Discovery for %s", circuit)

    def _retract_category_entity(self, circuit: str, category: str) -> None:
        """Send empty-payload retract for a category's three HA entities."""
        slug = _category_slug(circuit, category)
        for component, suffix in [
            ("sensor",        "count_today"),
            ("sensor",        "volume_today"),
            ("binary_sensor", "running"),
        ]:
            object_id = f"{_NODE_ID}_{slug}_{suffix}"
            topic = f"{_DISCOVERY_PREFIX}/{component}/{object_id}/config"
            self._client.publish(topic, "", retain=True)
        log.info("Retracted HA entities for %s/%s", circuit, category)

    # ── Public API ──────────────────────────────────────────────────────────────

    # ── Sprint F: category-level publish/retract (immediate flips) ────────────
    #
    # These are the entry points the new Fixtures POST route calls when the
    # user toggles a category. They wrap the existing per-category Discovery
    # machinery and bypass the once-per-60-s state-update tick so the HA
    # entity flips right away.

    def publish_category(self, circuit: str, fixture_type: str) -> None:
        """Publish HA Discovery for one (circuit, fixture_type) now."""
        if not self._connected or not self._is_publishing_enabled():
            return
        from .fixtures import FIXTURE_CATEGORY_LABELS
        label = FIXTURE_CATEGORY_LABELS.get(
            fixture_type, fixture_type.replace("_", " ").title()
        )
        self._publish_category_discovery(circuit, fixture_type, label)

    def retract_category(self, circuit: str, fixture_type: str) -> None:
        """Retract HA Discovery for one (circuit, fixture_type) now."""
        if not self._connected:
            return
        self._retract_category_entity(circuit, fixture_type)

    # ── LEGACY per-fixture hooks ──────────────────────────────────────────────
    #
    # Compatibility no-ops since Sprint F. Category publish state is now
    # controlled only by category_publish and publish_category /
    # retract_category. Existing callers in cluster_engine and the legacy
    # per-cluster routes still invoke these — we keep the signatures so they
    # don't break, but the calls have no MQTT effect. A debug log per call
    # makes the reason discoverable when a developer chases "why didn't this
    # publish?".

    def publish_fixture(self, fixture_id: str) -> None:
        log.debug("publish_fixture(%s): no-op since Sprint F; category_publish "
                  "is now the source of truth (toggle on the Water Use page).",
                  fixture_id)

    def retract_fixture(self, fixture_id: str) -> None:
        log.debug("retract_fixture(%s): no-op since Sprint F; category_publish "
                  "is now the source of truth (toggle on the Water Use page).",
                  fixture_id)

    def _publish_all_confirmed_sync(self) -> None:
        """Re-publish Discovery configs for all circuits and their active categories."""
        if not self._is_publishing_enabled():
            return
        try:
            circuits = [c.circuit for c in self._cfg.circuits]
            for circuit in circuits:
                # Phase 2.3 — the per-circuit unusual-usage sensor exists even before
                # any fixture category does, so publish it unconditionally per circuit.
                self._publish_circuit_discovery(circuit)
                cats = self._categories_for_circuit(circuit)
                for category, info in cats.items():
                    self._publish_category_discovery(circuit, category, info["label"])
            if any(self._categories_for_circuit(c) for c in circuits):
                log.info("Re-published Discovery for %d circuit(s)", len(circuits))
        except Exception as e:
            log.error("publish_all_confirmed failed: %s", e)

    async def update_state(self, circuit: str) -> None:
        """Push today's aggregated state per category for this circuit.

        Sprint F: aggregates by EFFECTIVE TYPE (via the same COALESCE chain
        the Fixtures page uses) and gates on the per-category publish map.
        A category toggled off mid-day will NOT have its state republished
        by the next tick — this is the second half of the off-toggle gate.
        """
        if not self._connected or not self._is_publishing_enabled():
            return

        from datetime import datetime, timedelta, timezone
        from .database import get_category_publish_map
        from .fixtures import (fixture_user_selectable_types,
                               normalize_fixture_type_for_circuit,
                               zone_user_selectable_types)

        # Determine circuit kind for normalizer.
        circuit_kind = "fixture"
        for c in self._cfg.circuits:
            if c.circuit == circuit:
                if getattr(c, "circuit_type", None) == "zone":
                    circuit_kind = "zone"
                break
        allowed = set(zone_user_selectable_types() if circuit_kind == "zone"
                      else fixture_user_selectable_types())

        # Read publish gates once. Categories missing from the map default
        # to True (publish on) — same contract as the Fixtures page.
        publish_map = get_category_publish_map(self._db, circuit)

        # Today's per-event-by-effective-type aggregates. Excludes phantoms,
        # matches Sprint F's get_category_rollup semantics. UTC-midnight
        # boundary matches the existing publisher behaviour (HA-side daily
        # sensors anchor on HA's own day handling).
        utc_midnight = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00+00:00")
        try:
            rows = self._db.execute(
                """SELECT
                     COALESCE(e.user_fixture_type, f.fixture_type, fc.suggested_type,
                              e.matched_fixture_type, 'other') AS eff_type,
                     COUNT(*) AS event_count,
                     COALESCE(SUM(COALESCE(e.volume_litres_effective, e.volume_litres, 0)), 0)
                              AS total_volume_l,
                     SUM(CASE WHEN e.end_ts IS NULL THEN 1 ELSE 0 END) AS running_count
                   FROM events e
                   LEFT JOIN fixtures f          ON e.fixture_id = f.id
                   LEFT JOIN fixture_clusters fc ON fc.circuit = e.circuit AND fc.id = e.cluster_id
                   WHERE e.circuit = ?
                     AND e.start_ts >= ?
                     AND COALESCE(e.is_pressure_restoration_phantom, 0) = 0
                   GROUP BY eff_type""",
                (circuit, utc_midnight),
            ).fetchall()
        except Exception as e:
            log.warning("[%s] fixture state update failed: %s", circuit, e)
            return

        # Normalize + bucket by canonical category, gated by publish_map.
        cat_counts:  Dict[str, int]   = {}
        cat_volumes: Dict[str, float] = {}
        cat_running: Dict[str, bool]  = {}
        for r in rows:
            cat = normalize_fixture_type_for_circuit(r["eff_type"], circuit_kind)
            if cat not in allowed:
                continue
            if not publish_map.get(cat, True):
                continue   # publish gate is off — skip this category entirely
            cat_counts[cat]  = cat_counts.get(cat, 0)  + int(r["event_count"] or 0)
            cat_volumes[cat] = cat_volumes.get(cat, 0.0) + float(r["total_volume_l"] or 0.0)
            if (r["running_count"] or 0) > 0:
                cat_running[cat] = True

        for cat in cat_counts:
            slug   = _category_slug(circuit, cat)
            count  = cat_counts[cat]
            volume = round(cat_volumes.get(cat, 0.0), 2)
            running = cat_running.get(cat, False)

            self._client.publish(
                f"{_DISCOVERY_PREFIX}/sensor/{_NODE_ID}_{slug}_count_today/state",
                str(count),
            )
            self._client.publish(
                f"{_DISCOVERY_PREFIX}/sensor/{_NODE_ID}_{slug}_volume_today/state",
                str(volume),
            )
            self._client.publish(
                f"{_DISCOVERY_PREFIX}/binary_sensor/{_NODE_ID}_{slug}_running/state",
                "ON" if running else "OFF",
            )

        # Phase 2.3 — per-circuit unusual-usage flag: ON when a recent event (by
        # start_ts, last 15 min) is flagged anomalous. STATE MIRROR ONLY — the notify
        # / shut-off response is applied live in feature_extractor, never from this
        # tick, and the by-start_ts window means a backfill re-flagging an OLD event
        # cannot make the sensor fire.
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).strftime(
            "%Y-%m-%dT%H:%M:%S+00:00")
        try:
            row = self._db.execute(
                "SELECT COUNT(*) FROM events WHERE circuit = ? AND start_ts >= ? "
                "AND COALESCE(flagged, 0) = 1", (circuit, cutoff)).fetchone()
            unusual = bool(row and row[0])
        except Exception:
            unusual = False
        self._client.publish(
            f"{_DISCOVERY_PREFIX}/binary_sensor/{_NODE_ID}_{_slugify(circuit)}_unusual_usage/state",
            "ON" if unusual else "OFF",
        )
