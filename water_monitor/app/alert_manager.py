"""
Alert manager — fires HA notifications when monitored conditions are met.

Reads alert_config rows (enabled/disabled per alert type per circuit) and
sends both persistent_notification (HA sidebar) and mobile push notifications
(via notify.mobile_app_* services listed in home_profile.mobile_notify_targets).

Alert types handled here:
  pressure_drop    — rapid pressure drop detected
  high_flow        — flow rate exceeds burst threshold
  trickle          — sustained low flow (running toilet / dripping tap)
  flow_anomaly     — flow pattern doesn't match any known fixture
  leak_test        — leak test failed or detected pressure decay

Called by:
  - FeatureExtractor._process() for event-based alerts
  - LeakTestScheduler on test completion
  - The HA event callback when ESP safety fault sensor fires
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Optional

log = logging.getLogger(__name__)


class AlertManager:

    def __init__(self, db: sqlite3.Connection, ha_client):
        self._db = db
        self._ha  = ha_client

    def _is_enabled(self, circuit: str, alert_type: str) -> bool:
        """Return True if this alert type is enabled for the circuit."""
        row = self._db.execute(
            "SELECT enabled FROM alert_config "
            "WHERE circuit = ? AND alert_type = ?",
            (circuit, alert_type),
        ).fetchone()
        # Default True if no row exists (graceful for new alert types)
        return bool(row["enabled"]) if row else True

    def _mobile_targets(self) -> list[str]:
        """Return list of HA notify service names from home_profile."""
        row = self._db.execute(
            "SELECT mobile_notify_targets FROM home_profile WHERE id = 1"
        ).fetchone()
        if not row or not row["mobile_notify_targets"]:
            return []
        return [t.strip() for t in row["mobile_notify_targets"].split(",")
                if t.strip()]

    def _away_mode(self) -> bool:
        row = self._db.execute(
            "SELECT away_mode FROM home_profile WHERE id = 1").fetchone()
        return bool(row["away_mode"]) if row else False

    async def fire(
        self,
        circuit: str,
        alert_type: str,
        title: str,
        message: str,
        notification_id: Optional[str] = None,
        critical: bool = False,
    ) -> None:
        """
        Send a notification if alert_type is enabled for circuit.

        critical=True bypasses the enabled check — used for safety shutoffs
        where we always want to notify regardless of user preference.
        """
        if not critical and not self._is_enabled(circuit, alert_type):
            log.debug("[%s] alert '%s' suppressed (disabled in config)",
                      circuit, alert_type)
            return

        nid = notification_id or f"water_{alert_type}_{circuit}"

        # 1. HA persistent notification (sidebar)
        await self._ha.notify(title=title, message=message,
                              notification_id=nid)

        # 2. Mobile push (all configured targets)
        for target in self._mobile_targets():
            try:
                await self._ha.call_service(
                    "notify", target,
                    {
                        "title":   title,
                        "message": message,
                        "data": {
                            "notification_id": nid,
                            "tag":             nid,
                        },
                    },
                )
                log.debug("Mobile push sent via notify.%s", target)
            except Exception as e:
                log.warning("Mobile push failed (notify.%s): %s", target, e)

    # ── Convenience methods for each alert type ────────────────────────

    async def alert_pressure_drop(self, circuit: str, drop_psi: float,
                                   circuit_name: str) -> None:
        from .units import load_unit_context, convert_pressure
        uc  = load_unit_context(self._db)
        val = convert_pressure(drop_psi, uc)
        await self.fire(
            circuit, "pressure_drop",
            title=f"⚠ Pressure drop — {circuit_name}",
            message=(f"Rapid pressure drop of {val} {uc['pressure_unit']} detected. "
                     "Possible burst pipe or demand surge."),
        )

    async def alert_high_flow(self, circuit: str, flow_lpm: float,
                               threshold_lpm: float,
                               circuit_name: str) -> None:
        from .units import load_unit_context, convert_flow
        uc  = load_unit_context(self._db)
        val = convert_flow(flow_lpm, uc)
        thr = convert_flow(threshold_lpm, uc)
        await self.fire(
            circuit, "high_flow",
            title=f"🚨 High flow alert — {circuit_name}",
            message=(f"Flow rate {val} {uc['flow_unit']} exceeds "
                     f"threshold {thr} {uc['flow_unit']}. "
                     "Possible burst pipe. Valve has been closed."),
            critical=True,
        )

    async def alert_trickle(self, circuit: str, duration_min: float,
                             flow_lpm: float, circuit_name: str) -> None:
        from .units import load_unit_context, convert_flow
        uc  = load_unit_context(self._db)
        val = convert_flow(flow_lpm, uc)
        await self.fire(
            circuit, "trickle",
            title=f"💧 Trickle flow alert — {circuit_name}",
            message=(f"Sustained low flow of {val} {uc['flow_unit']} "
                     f"for {duration_min:.0f} minutes. "
                     "Possible running toilet or dripping tap."),
        )

    async def alert_flow_anomaly(self, circuit: str, score: float,
                                  circuit_name: str) -> None:
        await self.fire(
            circuit, "flow_anomaly",
            title=f"🔍 Unusual flow pattern — {circuit_name}",
            message=(f"Flow pattern did not match any known fixture "
                     f"(anomaly score {score:.0%}). "
                     "Review the History page for details."),
        )

    async def alert_leak_test_failed(self, circuit: str,
                                      pressure_drop_psi: float,
                                      circuit_name: str) -> None:
        from .units import load_unit_context, convert_pressure
        uc  = load_unit_context(self._db)
        val = convert_pressure(pressure_drop_psi, uc)
        await self.fire(
            circuit, "leak_test",
            title=f"🔴 Leak detected — {circuit_name}",
            message=(f"Micro leak test detected pressure decay of "
                     f"{val} {uc['pressure_unit']}. "
                     "A slow leak may be present. Check the History page."),
            critical=True,
        )

    async def alert_leak_test_passed(self, circuit: str,
                                      duration_min: float,
                                      circuit_name: str) -> None:
        await self.fire(
            circuit, "leak_test",
            title=f"✅ Leak test passed — {circuit_name}",
            message=(f"No leak detected in {duration_min:.0f}-minute test. "
                     "Pressure was stable throughout."),
        )

    async def alert_away_mode_on(self) -> None:
        """Notify when away mode is activated."""
        await self._ha.notify(
            title="🏖 Away mode activated — Water Monitor",
            message="Leak tests continue. Baseline learning paused until you return.",
            notification_id="water_away_mode",
        )
        for target in self._mobile_targets():
            try:
                await self._ha.call_service(
                    "notify", target,
                    {"title": "🏖 Away mode activated — Water Monitor",
                     "message": "Leak tests continue. Baseline learning paused.",
                     "data": {"tag": "water_away_mode"}},
                )
            except Exception:
                pass
