"""
Fixture publisher — Phase 2 (Gap 6).

Publishes confirmed fixtures to Home Assistant via MQTT Discovery so they
appear as native HA entities (Energy panel, automations, dashboards).

Per fixture the publisher creates three HA entities:
  sensor.water_monitor_{slug}_count_today        — daily event count (integer)
  sensor.water_monitor_{slug}_volume_today       — daily volume (device_class: water)
  binary_sensor.water_monitor_{slug}_running     — water currently running

Broker credentials are fetched from the HA supervisor at startup:
  GET http://supervisor/services/mqtt  (requires SUPERVISOR_TOKEN env var)

State updates run every 60 seconds for all published fixtures.
Fixtures with publish_to_ha = 0 are silently skipped.
When MQTT publishing is disabled globally (mqtt_publish_enabled = 0 in
home_profile) no messages are sent, but the broker connection is kept alive.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

_DISCOVERY_PREFIX = "homeassistant"
_NODE_ID          = "water_monitor"


def _slugify(name: str) -> str:
    """Convert a fixture display name to a safe HA entity slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_") or "fixture"


class FixturePublisher:
    """Publishes confirmed fixtures to HA via MQTT Discovery."""

    def __init__(self, db: sqlite3.Connection, cfg, ha_client):
        self._db  = db
        self._cfg = cfg
        self._ha  = ha_client
        self._client = None      # paho MQTT client
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
            # Re-publish all confirmed fixtures on reconnect
            self._publish_all_confirmed_sync()
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

    def publish_fixture(self, fixture_id: str) -> None:
        """Publish HA Discovery config for all entities of one fixture."""
        if not self._connected or not self._is_publishing_enabled():
            return
        row = self._db.execute(
            "SELECT * FROM fixtures WHERE id = ? AND publish_to_ha = 1",
            (fixture_id,)
        ).fetchone()
        if not row:
            return
        self._publish_discovery(dict(row))

    def retract_fixture(self, fixture_id: str) -> None:
        """Publish empty payload to remove a fixture's HA entities."""
        if not self._connected:
            return
        row = self._db.execute(
            "SELECT * FROM fixtures WHERE id = ?", (fixture_id,)
        ).fetchone()
        if not row:
            return
        slug = _slugify(row["display_name"] or row["name"] or fixture_id)
        for component, object_id in [
            ("sensor",        f"{_NODE_ID}_{slug}_count_today"),
            ("sensor",        f"{_NODE_ID}_{slug}_volume_today"),
            ("binary_sensor", f"{_NODE_ID}_{slug}_running"),
        ]:
            topic = f"{_DISCOVERY_PREFIX}/{component}/{object_id}/config"
            self._client.publish(topic, "", retain=True)
        # Mark as retracted in the entity map
        now = datetime.now(timezone.utc).isoformat()
        self._db.execute(
            """UPDATE fixture_ha_entity_map SET retracted_at = ?
               WHERE fixture_id = ?""",
            (now, fixture_id)
        )
        self._db.commit()
        log.info("Retracted HA entities for fixture %s", fixture_id)

    def _publish_discovery(self, fixture: dict) -> None:
        """Publish MQTT Discovery payloads for count, volume, and running sensors."""
        slug = _slugify(fixture.get("display_name") or fixture.get("name") or fixture["id"])
        name = fixture.get("display_name") or fixture.get("name") or slug
        fid  = fixture["id"]
        now  = datetime.now(timezone.utc).isoformat()

        device = {
            "identifiers":    [f"{_NODE_ID}_{fid}"],
            "name":           f"Water Monitor — {name}",
            "manufacturer":   "Water Monitor",
            "model":          fixture.get("fixture_type", "fixture"),
        }

        configs = [
            (
                "sensor",
                f"{_NODE_ID}_{slug}_count_today",
                f"{name} — uses today",
                f"{_DISCOVERY_PREFIX}/sensor/{_NODE_ID}_{slug}_count_today/state",
                None,        # no device_class for event count
                None,
                "mdi:counter",
            ),
            (
                "sensor",
                f"{_NODE_ID}_{slug}_volume_today",
                f"{name} — volume today",
                f"{_DISCOVERY_PREFIX}/sensor/{_NODE_ID}_{slug}_volume_today/state",
                "water",
                "L",
                "mdi:water",
            ),
            (
                "binary_sensor",
                f"{_NODE_ID}_{slug}_running",
                f"{name} — running",
                f"{_DISCOVERY_PREFIX}/binary_sensor/{_NODE_ID}_{slug}_running/state",
                None,
                None,
                "mdi:water-pump",
            ),
        ]

        for component, object_id, friendly_name, state_topic, device_class, unit, icon in configs:
            payload: dict = {
                "name":         friendly_name,
                "unique_id":    f"{_NODE_ID}_{object_id}",
                "state_topic":  state_topic,
                "device":       device,
                "icon":         icon,
            }
            if device_class:
                payload["device_class"] = device_class
            if unit:
                payload["unit_of_measurement"] = unit
                payload["state_class"] = "total_increasing"

            topic = f"{_DISCOVERY_PREFIX}/{component}/{object_id}/config"
            self._client.publish(topic, json.dumps(payload), retain=True)

            # Record in entity map
            try:
                self._db.execute(
                    """INSERT INTO fixture_ha_entity_map
                           (fixture_id, ha_entity_id, device_class,
                            unit_of_measurement, last_published_at, retracted_at)
                       VALUES (?, ?, ?, ?, ?, NULL)
                       ON CONFLICT (fixture_id, ha_entity_id) DO UPDATE SET
                           last_published_at = excluded.last_published_at,
                           retracted_at      = NULL""",
                    (fid, f"{component}.{object_id}", device_class, unit, now)
                )
            except Exception as e:
                log.warning("fixture_ha_entity_map write failed: %s", e)

        self._db.commit()
        log.debug("Published Discovery for fixture %s (%s)", fid, name)

    def _publish_all_confirmed_sync(self) -> None:
        """Re-publish Discovery configs for all publish_to_ha fixtures."""
        if not self._is_publishing_enabled():
            return
        try:
            rows = self._db.execute(
                "SELECT * FROM fixtures WHERE publish_to_ha = 1"
            ).fetchall()
            for row in rows:
                self._publish_discovery(dict(row))
            if rows:
                log.info("Re-published Discovery for %d fixture(s)", len(rows))
        except Exception as e:
            log.error("publish_all_confirmed failed: %s", e)

    async def update_state(self, circuit: str) -> None:
        """Push current state for all confirmed fixtures on this circuit."""
        if not self._connected or not self._is_publishing_enabled():
            return
        try:
            rows = self._db.execute(
                """SELECT f.*, fds.event_count, fds.total_volume_litres
                   FROM fixtures f
                   LEFT JOIN fixture_daily_summary fds
                       ON fds.fixture_id = f.id
                       AND fds.day = date('now')
                   WHERE f.circuit = ? AND f.publish_to_ha = 1""",
                (circuit,)
            ).fetchall()
        except Exception as e:
            log.warning("[%s] fixture state update failed: %s", circuit, e)
            return

        for row in rows:
            slug   = _slugify(row["display_name"] or row["name"] or row["id"])
            count  = row["event_count"] or 0
            volume = round(row["total_volume_litres"] or 0.0, 2)

            # Determine if currently running — check for active event on this circuit
            running = False
            try:
                active = self._db.execute(
                    """SELECT 1 FROM events
                       WHERE circuit = ? AND fixture_id = ?
                         AND end_ts IS NULL LIMIT 1""",
                    (circuit, row["id"])
                ).fetchone()
                running = active is not None
            except Exception:
                pass

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
