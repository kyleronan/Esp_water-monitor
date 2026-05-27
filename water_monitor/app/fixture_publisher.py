"""
Fixture publisher — Phase 2 (Gap 6).

Publishes water fixture activity to Home Assistant via MQTT Discovery using
broad categories rather than individual fixture names.  Per circuit the
publisher creates up to six HA entity sets (one per broad category present):

  sensor.water_monitor_{circuit}_{category}_count_today
  sensor.water_monitor_{circuit}_{category}_volume_today
  binary_sensor.water_monitor_{circuit}_{category}_running

Categories: toilet, shower_tub, tap, appliance, irrigation, other.

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
from typing import Dict, Optional, Set

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
        at least one confirmed publish_to_ha=1 fixture on this circuit.

        Also includes categories inferred from suggested_type on unconfirmed
        clusters so that the publisher tracks all active usage, not just
        user-confirmed fixtures.
        """
        from .fixtures import get_fixture_category, FIXTURE_CATEGORY_LABELS

        circ_label = circuit
        for c in self._cfg.circuits:
            if c.circuit == circuit:
                circ_label = getattr(c, "label", circuit)
                break

        rows = self._db.execute(
            """SELECT COALESCE(f.fixture_type, fc.suggested_type) AS ftype
               FROM fixture_clusters fc
               LEFT JOIN fixtures f ON fc.fixture_id = f.id
               WHERE fc.circuit = ?
                 AND (f.publish_to_ha = 1 OR (f.publish_to_ha IS NULL AND fc.suggested_type IS NOT NULL))
                 AND fc.member_count >= 5""",
            (circuit,),
        ).fetchall()

        cats: Dict[str, dict] = {}
        for r in rows:
            cat = get_fixture_category(r["ftype"])
            if cat and cat not in cats:
                cats[cat] = {
                    "label":         FIXTURE_CATEGORY_LABELS.get(cat, cat.replace("_", " ").title()),
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

    def publish_fixture(self, fixture_id: str) -> None:
        """Publish/update HA Discovery for the category this fixture belongs to."""
        if not self._connected or not self._is_publishing_enabled():
            return
        row = self._db.execute(
            "SELECT * FROM fixtures WHERE id = ? AND publish_to_ha = 1",
            (fixture_id,),
        ).fetchone()
        if not row:
            return
        from .fixtures import get_fixture_category, FIXTURE_CATEGORY_LABELS
        category = get_fixture_category(row["fixture_type"])
        if not category:
            return
        cat_label = FIXTURE_CATEGORY_LABELS.get(category, category.replace("_", " ").title())
        self._publish_category_discovery(row["circuit"], category, cat_label)

    def retract_fixture(self, fixture_id: str) -> None:
        """Retract the category HA entity only if no other fixtures remain in it."""
        if not self._connected:
            return
        row = self._db.execute(
            "SELECT * FROM fixtures WHERE id = ?", (fixture_id,)
        ).fetchone()
        if not row:
            return
        from .fixtures import get_fixture_category
        category = get_fixture_category(row["fixture_type"])
        if not category:
            return
        circuit = row["circuit"]

        # Check if any other publish_to_ha=1 fixture in the same category remains
        remaining_rows = self._db.execute(
            """SELECT f.fixture_type FROM fixtures f
               WHERE f.circuit = ? AND f.publish_to_ha = 1 AND f.id != ?""",
            (circuit, fixture_id),
        ).fetchall()
        remaining_categories: Set[str] = {
            get_fixture_category(r["fixture_type"])
            for r in remaining_rows
            if get_fixture_category(r["fixture_type"])
        }
        if category not in remaining_categories:
            self._retract_category_entity(circuit, category)

    def _publish_all_confirmed_sync(self) -> None:
        """Re-publish Discovery configs for all circuits and their active categories."""
        if not self._is_publishing_enabled():
            return
        try:
            circuits = [c.circuit for c in self._cfg.circuits]
            for circuit in circuits:
                cats = self._categories_for_circuit(circuit)
                for category, info in cats.items():
                    self._publish_category_discovery(circuit, category, info["label"])
            if any(self._categories_for_circuit(c) for c in circuits):
                log.info("Re-published Discovery for %d circuit(s)", len(circuits))
        except Exception as e:
            log.error("publish_all_confirmed failed: %s", e)

    async def update_state(self, circuit: str) -> None:
        """Push current aggregated state for all categories on this circuit."""
        if not self._connected or not self._is_publishing_enabled():
            return

        from .fixtures import get_fixture_category

        try:
            rows = self._db.execute(
                """SELECT f.fixture_type, fc.suggested_type,
                          COALESCE(fds.event_count, 0)          AS event_count,
                          COALESCE(fds.total_volume_litres, 0.0) AS total_volume,
                          f.id AS fixture_id
                   FROM fixture_clusters fc
                   LEFT JOIN fixtures f ON fc.fixture_id = f.id
                   LEFT JOIN fixture_daily_summary fds
                       ON fds.fixture_id = f.id AND fds.day = date('now')
                   WHERE fc.circuit = ?
                     AND (f.publish_to_ha = 1
                          OR (f.publish_to_ha IS NULL AND fc.suggested_type IS NOT NULL))
                     AND fc.member_count >= 5""",
                (circuit,),
            ).fetchall()
        except Exception as e:
            log.warning("[%s] fixture state update failed: %s", circuit, e)
            return

        # Aggregate per category
        cat_counts:  Dict[str, int]   = {}
        cat_volumes: Dict[str, float] = {}
        cat_running: Dict[str, bool]  = {}

        for row in rows:
            ftype = row["fixture_type"] or row["suggested_type"]
            cat   = get_fixture_category(ftype)
            if not cat:
                continue
            cat_counts[cat]  = cat_counts.get(cat, 0)  + (row["event_count"] or 0)
            cat_volumes[cat] = cat_volumes.get(cat, 0.0) + (row["total_volume"] or 0.0)

            # Check if currently running
            if row["fixture_id"] and not cat_running.get(cat):
                try:
                    active = self._db.execute(
                        """SELECT 1 FROM events
                           WHERE circuit = ? AND fixture_id = ?
                             AND end_ts IS NULL LIMIT 1""",
                        (circuit, row["fixture_id"]),
                    ).fetchone()
                    if active:
                        cat_running[cat] = True
                except Exception:
                    pass

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
