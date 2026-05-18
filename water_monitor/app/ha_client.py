"""
Home Assistant client.

Provides:
  - Persistent WebSocket connection using subscribe_entities for targeted
    real-time updates (used by the event detector for pressure/flow monitoring)
  - Short-lived WebSocket connections for history queries
  - REST API for state reads, state publishing, and service calls
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import websockets

from .config import supervisor_token

log = logging.getLogger(__name__)

WS_URL = "ws://supervisor/core/websocket"
REST_URL = "http://supervisor/core/api"

_GAL_UNITS = {"gal", "us gal", "gallon", "gallons", "us gallon", "us liquid gallon"}


def vol_to_litres(value: float, unit: str) -> float:
    """Convert a volume value to litres; pass-through if already in L or unknown."""
    if (unit or "").strip().lower() in _GAL_UNITS:
        return value * 3.785411784
    return value

# Type for state-changed callbacks: (entity_id, new_state, attributes) -> None
StateCallback = Callable[[str, str, dict], None]


class HaClient:
    """
    Async HA client.

    Usage:
        async with HaClient() as client:
            await client.subscribe_entities(["sensor.foo"], my_callback)
            await client.run()
    """

    def __init__(self) -> None:
        self._token = supervisor_token()
        self._http: Optional[aiohttp.ClientSession] = None
        self._subscriptions: Dict[str, List[StateCallback]] = {}
        self._ws_msg_id = 1
        self._ws_lock = asyncio.Lock()
        self._ws_oneshot: Optional[Any] = None   # persistent connection for ws_request
        self._running = False
        self._stop_event = asyncio.Event()

    async def __aenter__(self) -> "HaClient":
        self._http = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=aiohttp.ClientTimeout(total=30),
        )
        return self

    async def __aexit__(self, *args) -> None:
        self._stop_event.set()
        if self._ws_oneshot:
            with contextlib.suppress(Exception):
                await self._ws_oneshot.close()
        if self._http:
            await self._http.close()

    def stop(self) -> None:
        self._stop_event.set()

    def subscribe_entity(self, entity_id: str, callback: StateCallback) -> None:
        """Register a callback for state_changed events on entity_id."""
        if entity_id not in self._subscriptions:
            self._subscriptions[entity_id] = []
        self._subscriptions[entity_id].append(callback)

    def subscribe_entities(self, entity_ids: List[str],
                           callback: StateCallback) -> None:
        for eid in entity_ids:
            self.subscribe_entity(eid, callback)

    # ------------------------------------------------------------------
    # Persistent event subscription loop
    # ------------------------------------------------------------------
    async def run_event_loop(self) -> None:
        """
        Maintain a persistent WebSocket connection and dispatch
        state_changed events to registered callbacks.
        Reconnects automatically on disconnect.
        """
        while not self._stop_event.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning("HA WebSocket disconnected: %s — reconnecting in 10s", e)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=10)
                    return
                except asyncio.TimeoutError:
                    pass

    async def _connect_and_listen(self) -> None:
        async with websockets.connect(WS_URL, max_size=2**24,
                                      ping_interval=30) as ws:
            await self._auth(ws)
            sub_id = await self._subscribe_entity_states(ws)
            log.info("HA WebSocket connected, monitoring %d entities",
                     len(self._subscriptions))

            while not self._stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                except asyncio.TimeoutError:
                    continue
                msg = json.loads(raw)
                if sub_id and msg.get("type") == "event" and msg.get("id") == sub_id:
                    self._dispatch_entity_change(msg.get("event", {}))

    async def _auth(self, ws) -> None:
        hello = json.loads(await ws.recv())
        if hello.get("type") != "auth_required":
            raise RuntimeError(f"Unexpected hello: {hello}")
        await ws.send(json.dumps({
            "type": "auth",
            "access_token": self._token,
        }))
        result = json.loads(await ws.recv())
        if result.get("type") != "auth_ok":
            raise RuntimeError(f"Auth failed: {result}")

    async def _subscribe_entity_states(self, ws) -> int:
        """Subscribe to targeted entity state updates via subscribe_entities.

        Returns the subscription message ID so the recv loop can match
        incoming events. Returns 0 if there are no registered entity IDs.
        """
        entity_ids = list(self._subscriptions.keys())
        if not entity_ids:
            return 0
        msg_id = self._next_id()
        await ws.send(json.dumps({
            "id": msg_id,
            "type": "subscribe_entities",
            "entity_ids": entity_ids,
        }))
        # Wait for subscription confirmation.
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
            except asyncio.TimeoutError:
                raise RuntimeError(
                    "Timeout waiting for subscribe_entities confirmation")
            msg = json.loads(raw)
            if msg.get("id") == msg_id:
                if msg.get("type") == "result" and msg.get("success"):
                    return msg_id
                raise RuntimeError(f"subscribe_entities failed: {msg}")

    def _dispatch_entity_change(self, event: dict) -> None:
        """Dispatch subscribe_entities change events to registered callbacks.

        HA sends changes as: {"c": {"entity_id": {"+": {"s": state, "a": attrs}}}}
        Attributes are partial (only changed keys) — callbacks that need only
        the state value are unaffected.
        """
        for entity_id, diff in event.get("c", {}).items():
            changes = diff.get("+", {})
            if not changes:
                continue
            state = changes.get("s", "")
            attributes = changes.get("a", {})
            for cb in self._subscriptions.get(entity_id, []):
                try:
                    cb(entity_id, state, attributes)
                except Exception as e:
                    log.error("Callback error for %s: %s", entity_id, e)

    def _next_id(self) -> int:
        self._ws_msg_id += 1
        return self._ws_msg_id

    # ------------------------------------------------------------------
    # Generic one-shot WebSocket request
    # ------------------------------------------------------------------
    async def ws_request(self, msg_type: str, **kwargs) -> Any:
        """
        Make a single request/response WebSocket call using a persistent
        connection. The connection is established lazily on first use and
        reused for subsequent calls (avoiding a full auth handshake per
        request). Reconnects transparently on failure.
        Used for registry queries and other one-off requests.
        """
        async with self._ws_lock:
            # Lazy connect / reconnect on closed or failed connection
            if self._ws_oneshot is None or not self._ws_oneshot.open:
                if self._ws_oneshot is not None:
                    with contextlib.suppress(Exception):
                        await self._ws_oneshot.close()
                self._ws_oneshot = await websockets.connect(WS_URL, max_size=2**24)
                await self._auth(self._ws_oneshot)
            ws = self._ws_oneshot
            msg_id = self._next_id()
            await ws.send(json.dumps({"id": msg_id, "type": msg_type, **kwargs}))
            # Timeout prevents hanging forever on a network stall.
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        f"Timeout waiting for WS response to '{msg_type}'")
                msg = json.loads(raw)
                if msg.get("id") == msg_id:
                    if not msg.get("success"):
                        raise RuntimeError(
                            f"WS request '{msg_type}' failed: {msg}")
                    return msg.get("result")

    async def get_devices(self) -> List[Dict[str, Any]]:
        """Return all devices from the HA device registry."""
        result = await self.ws_request("config/device_registry/list")
        return result or []

    async def get_entity_registry(self) -> List[Dict[str, Any]]:
        """Return all entities from the HA entity registry."""
        result = await self.ws_request("config/entity_registry/list")
        return result or []

    # ------------------------------------------------------------------
    # History queries (short-lived WS connections)
    # ------------------------------------------------------------------
    def _parse_history_entries(
        self, entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Normalise raw HA history entries to [{state, last_changed}]."""
        out = []
        for e in entries:
            state = e.get("s") if "s" in e else e.get("state")
            ts_field = e.get("lu") if "lu" in e else e.get("last_updated")
            if ts_field is None:
                continue
            if isinstance(ts_field, (int, float)):
                ts = dt.datetime.fromtimestamp(ts_field, tz=dt.timezone.utc)
            else:
                ts = dt.datetime.fromisoformat(
                    str(ts_field).replace("Z", "+00:00"))
            out.append({"state": state, "last_changed": ts})
        return out

    async def get_history(
        self,
        entity_id: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> List[Dict[str, Any]]:
        """Fetch state history for one entity. Returns [{state, last_changed}]."""
        result = await self.ws_request(
            "history/history_during_period",
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            entity_ids=[entity_id],
            minimal_response=True,
            no_attributes=True,
        )
        return self._parse_history_entries((result or {}).get(entity_id, []))

    async def get_history_batch(
        self,
        entity_ids: List[str],
        start: dt.datetime,
        end: dt.datetime,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch history for multiple entities in a single WS request/connection.
        Returns {entity_id: [{state, last_changed}]}.
        """
        if not entity_ids:
            return {}
        result = await self.ws_request(
            "history/history_during_period",
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            entity_ids=entity_ids,
            minimal_response=True,
            no_attributes=True,
        )
        return {
            eid: self._parse_history_entries((result or {}).get(eid, []))
            for eid in entity_ids
        }

    # ------------------------------------------------------------------
    # REST API — state reads
    # ------------------------------------------------------------------
    async def get_state(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """GET /api/states/<entity_id>."""
        if not self._http:
            return None
        url = f"{REST_URL}/states/{entity_id}"
        try:
            async with self._http.get(url) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as e:
            log.warning("get_state %s failed: %s", entity_id, e)
            return None

    async def get_state_value(self, entity_id: str,
                              default: Any = None) -> Any:
        """Get just the state string for an entity."""
        result = await self.get_state(entity_id)
        if result is None:
            return default
        return result.get("state", default)

    async def get_multiple_states(
        self, entity_ids: List[str]
    ) -> Dict[str, Optional[Dict]]:
        """Fetch multiple entity states concurrently."""
        results = await asyncio.gather(
            *[self.get_state(eid) for eid in entity_ids],
            return_exceptions=True,
        )
        return {
            eid: (None if isinstance(r, Exception) else r)
            for eid, r in zip(entity_ids, results)
        }

    async def get_ha_config(self) -> Dict[str, Any]:
        """GET /api/config — returns HA instance config including time_zone."""
        if not self._http:
            return {}
        try:
            async with self._http.get(f"{REST_URL}/config") as resp:
                if resp.status != 200:
                    return {}
                return await resp.json()
        except Exception as e:
            log.warning("get_ha_config failed: %s", e)
            return {}

    async def get_all_states(self) -> List[Dict[str, Any]]:
        """GET /api/states — returns all entity states."""
        if not self._http:
            return []
        try:
            async with self._http.get(f"{REST_URL}/states") as resp:
                if resp.status != 200:
                    return []
                return await resp.json()
        except Exception as e:
            log.warning("get_all_states failed: %s", e)
            return []

    async def get_ha_unit_system(self) -> Dict[str, str]:
        """
        GET /api/config — returns the HA unit system dict.
        e.g. {"volume": "L", "pressure": "hPa", "temperature": "°C", ...}
        Returns empty dict on failure.
        """
        if not self._http:
            return {}
        try:
            async with self._http.get(f"{REST_URL}/config") as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
                return data.get("unit_system", {})
        except Exception as e:
            log.warning("get_ha_unit_system failed: %s", e)
            return {}

    async def get_device_configurable_entities(
        self, device_prefix: str
    ) -> List[Dict[str, Any]]:
        """
        Return all number.* and select.* entities belonging to the device
        (identified by entity_id starting with the device prefix).
        Each entry has: entity_id, domain, state, attributes (min/max/step/options etc.)
        """
        if not device_prefix:
            return []
        configurable_domains = {"number", "select", "input_number", "input_select"}
        all_states = await self.get_all_states()
        results = []
        for s in all_states:
            eid = s.get("entity_id", "")
            domain = eid.split(".", 1)[0]
            if domain not in configurable_domains:
                continue
            # Match by prefix (entity_id local part starts with device prefix)
            local = eid.split(".", 1)[1] if "." in eid else ""
            if not local.startswith(device_prefix):
                continue
            attrs = s.get("attributes", {})
            results.append({
                "entity_id": eid,
                "domain": domain,
                "state": s.get("state"),
                "friendly_name": attrs.get("friendly_name", eid),
                "min": attrs.get("min"),
                "max": attrs.get("max"),
                "step": attrs.get("step"),
                "unit": attrs.get("unit_of_measurement", ""),
                "options": attrs.get("options", []),
                "mode": attrs.get("mode", "box"),
            })
        return sorted(results, key=lambda x: x["friendly_name"])

    async def set_number(self, entity_id: str, value: float) -> bool:
        """Set a number entity value."""
        domain = entity_id.split(".", 1)[0]
        service = "set_value"
        return await self.call_service(domain, service,
                                       {"entity_id": entity_id, "value": value})

    async def set_select(self, entity_id: str, option: str) -> bool:
        """Set a select entity option."""
        domain = entity_id.split(".", 1)[0]
        service = "select_option"
        return await self.call_service(domain, service,
                                       {"entity_id": entity_id, "option": option})

    # ------------------------------------------------------------------
    # REST API — state publish
    # ------------------------------------------------------------------
    async def set_state(self, entity_id: str, state: Any,
                        attributes: Optional[dict] = None) -> bool:
        """POST to /api/states/<entity_id>."""
        if not self._http:
            return False
        body = {"state": str(state)}
        if attributes:
            body["attributes"] = attributes
        url = f"{REST_URL}/states/{entity_id}"
        try:
            async with self._http.post(url, json=body) as resp:
                return resp.status in (200, 201)
        except Exception as e:
            log.warning("set_state %s failed: %s", entity_id, e)
            return False

    # ------------------------------------------------------------------
    # REST API — service calls
    # ------------------------------------------------------------------
    async def call_service(
        self,
        domain: str,
        service: str,
        data: Optional[dict] = None,
    ) -> bool:
        """POST to /api/services/<domain>/<service>."""
        if not self._http:
            return False
        url = f"{REST_URL}/services/{domain}/{service}"
        try:
            async with self._http.post(url, json=data or {}) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("call_service %s.%s data=%s → HTTP %d: %s",
                                domain, service, data, resp.status,
                                body[:200])
                    return False
                return True
        except Exception as e:
            log.warning("call_service %s.%s failed: %s", domain, service, e)
            return False

    async def turn_on(self, entity_id: str) -> bool:
        domain = entity_id.split(".")[0]
        return await self.call_service(domain, "turn_on",
                                       {"entity_id": entity_id})

    async def turn_off(self, entity_id: str) -> bool:
        domain = entity_id.split(".")[0]
        return await self.call_service(domain, "turn_off",
                                       {"entity_id": entity_id})

    async def set_number_value(self, entity_id: str, value: float) -> bool:
        return await self.call_service("number", "set_value",
                                       {"entity_id": entity_id, "value": value})

    async def open_valve(self, entity_id: str) -> bool:
        """
        Open a valve. Supports multiple HA domains:
          valve.*  → valve.open_valve
          cover.*  → cover.open_cover
          switch.* → switch.turn_on (assumes switch ON = valve open)
        Logs the action and returns success/failure.
        """
        domain = entity_id.split(".", 1)[0]
        if domain == "valve":
            ok = await self.call_service("valve", "open_valve",
                                          {"entity_id": entity_id})
        elif domain == "cover":
            ok = await self.call_service("cover", "open_cover",
                                          {"entity_id": entity_id})
        elif domain == "switch":
            ok = await self.call_service("switch", "turn_on",
                                          {"entity_id": entity_id})
        else:
            log.error("Cannot open valve %s — unsupported domain %r",
                      entity_id, domain)
            return False
        log.info("open_valve(%s) → %s", entity_id,
                 "OK" if ok else "FAILED")
        return ok

    async def close_valve(self, entity_id: str) -> bool:
        """
        Close a valve. Supports multiple HA domains:
          valve.*  → valve.close_valve
          cover.*  → cover.close_cover
          switch.* → switch.turn_off (assumes switch OFF = valve closed)
        Logs the action and returns success/failure.
        """
        domain = entity_id.split(".", 1)[0]
        if domain == "valve":
            ok = await self.call_service("valve", "close_valve",
                                          {"entity_id": entity_id})
        elif domain == "cover":
            ok = await self.call_service("cover", "close_cover",
                                          {"entity_id": entity_id})
        elif domain == "switch":
            ok = await self.call_service("switch", "turn_off",
                                          {"entity_id": entity_id})
        else:
            log.error("Cannot close valve %s — unsupported domain %r",
                      entity_id, domain)
            return False
        log.info("close_valve(%s) → %s", entity_id,
                 "OK" if ok else "FAILED")
        return ok

    async def notify(self, title: str, message: str,
                     notification_id: Optional[str] = None) -> bool:
        data: dict = {"title": title, "message": message}
        if notification_id:
            data["notification_id"] = notification_id
        return await self.call_service(
            "persistent_notification", "create", data)
