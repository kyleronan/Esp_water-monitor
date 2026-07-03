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

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

log = logging.getLogger(__name__)


# After this many consecutive failures, treat the target as broken and
# skip further pushes for COOLDOWN duration. Stops a permanently
# misconfigured `notify.mobile_app_*` from spamming WARN every time
# any event fires.
_TARGET_FAILURE_THRESHOLD = 3
_TARGET_BACKOFF = timedelta(hours=1)

# Exception types we expect when calling HA's notify service. Anything
# outside this set falls through to a broad-except that logs at ERROR
# (with type and traceback) so unknown failure modes don't get
# swallowed silently.
_EXPECTED_PUSH_EXCEPTIONS = (asyncio.TimeoutError, OSError)
try:
    import aiohttp
    _EXPECTED_PUSH_EXCEPTIONS = _EXPECTED_PUSH_EXCEPTIONS + (
        aiohttp.ClientError,
    )
except ImportError:  # pragma: no cover — aiohttp is a hard dep, defensive only
    pass


def _humanize_anomaly_type(atype: Optional[str]) -> str:
    """Turn the stored anomaly_type tag into a human reason for the notification."""
    a = atype or ""
    vol = "high_volume" in a
    shape = "envelope" in a or "abnormal_shape" in a
    if vol and shape:
        return "Unusually high volume and an abnormal pattern"
    if vol:
        return "Unusually high water volume"
    if shape:
        return "An abnormal usage pattern for this fixture"
    return "Unusual water usage"


class AlertManager:

    def __init__(self, db: sqlite3.Connection, ha_client):
        self._db = db
        self._ha  = ha_client
        # Per-target consecutive-failure tracking. Reset on success.
        self._target_failure_counts: Dict[str, int] = {}
        self._target_backoff_until:  Dict[str, datetime] = {}

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
    ) -> bool:
        """
        Send a notification if alert_type is enabled for circuit.

        critical=True bypasses the enabled check — used for safety shutoffs
        where we always want to notify regardless of user preference.

        Returns True when the notification was dispatched, False when it was
        suppressed by the per-type enable config — so callers can record
        "this event actually notified" (events.triggered_alert).
        """
        if not critical and not self._is_enabled(circuit, alert_type):
            log.debug("[%s] alert '%s' suppressed (disabled in config)",
                      circuit, alert_type)
            return False

        nid = notification_id or f"water_{alert_type}_{circuit}"

        # 1. HA persistent notification (sidebar)
        await self._ha.notify(title=title, message=message,
                              notification_id=nid)

        # 2. Mobile push (all configured targets)
        for target in self._mobile_targets():
            await self._send_mobile_push(
                target, title, message, data={
                    "notification_id": nid,
                    "tag":             nid,
                },
            )
        return True

    async def _send_mobile_push(
        self,
        target: str,
        title: str,
        message: str,
        data: Optional[dict] = None,
    ) -> bool:
        """Send a mobile push to a single notify.* target with backoff.

        Returns True on success, False on failure (or when the target
        is currently in its post-failure cooldown). Never raises — push
        failures must never break the calling event-processing path.

        Failure handling:
          - Expected exceptions (network / timeout / aiohttp client
            errors) log at WARNING with the exception type + message.
          - Unexpected exceptions log at ERROR with full traceback so
            unknown failure modes are visible without swallowing them.
          - After ``_TARGET_FAILURE_THRESHOLD`` consecutive failures,
            the target is marked broken and pushes are skipped for
            ``_TARGET_BACKOFF`` (one entry per target). The cooldown
            resets on the next successful push.
        """
        # Skip if this target is currently in its post-failure cooldown
        now = datetime.now(timezone.utc)
        backoff_until = self._target_backoff_until.get(target)
        if backoff_until and now < backoff_until:
            log.debug(
                "Skipping notify.%s — in cooldown until %s",
                target, backoff_until.isoformat(timespec="seconds"),
            )
            return False

        try:
            await self._ha.call_service(
                "notify", target,
                {"title": title, "message": message, "data": data or {}},
            )
        except _EXPECTED_PUSH_EXCEPTIONS as e:
            self._record_push_failure(target, e, level="warning")
            return False
        except Exception as e:  # pragma: no cover — defensive only
            # Unknown exception type — log at ERROR with traceback so we
            # can add the type to _EXPECTED_PUSH_EXCEPTIONS next release.
            log.error(
                "Mobile push raised unexpected %s on notify.%s: %s",
                type(e).__name__, target, e, exc_info=True,
            )
            self._record_push_failure(target, e, level=None)
            return False

        # Success — reset failure tracking for this target
        if self._target_failure_counts.pop(target, 0):
            log.info("notify.%s recovered — push delivered", target)
        self._target_backoff_until.pop(target, None)
        log.debug("Mobile push sent via notify.%s", target)
        return True

    def _record_push_failure(
        self,
        target: str,
        exc: BaseException,
        level: Optional[str],
    ) -> None:
        """Update failure counters and emit the right log line.

        ``level`` is the log level to use for the failure message
        itself; pass None to suppress (used when the caller already
        logged at ERROR with a traceback).
        """
        count = self._target_failure_counts.get(target, 0) + 1
        self._target_failure_counts[target] = count
        if level == "warning":
            log.warning(
                "Mobile push failed (notify.%s): %s: %s "
                "(consecutive failures: %d)",
                target, type(exc).__name__, exc, count,
            )
        if count == _TARGET_FAILURE_THRESHOLD:
            backoff_until = datetime.now(timezone.utc) + _TARGET_BACKOFF
            self._target_backoff_until[target] = backoff_until
            log.error(
                "notify.%s reached %d consecutive failures — backing off "
                "until %s. Check that the mobile_app integration is still "
                "paired in Home Assistant.",
                target, _TARGET_FAILURE_THRESHOLD,
                backoff_until.isoformat(timespec="seconds"),
            )

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

    async def alert_unusual_usage(self, circuit: str, score: float,
                                  anomaly_type: Optional[str], circuit_name: str,
                                  shutoff: bool = False,
                                  event_id: Optional[str] = None,
                                  valve_entity: Optional[str] = None) -> None:
        """Phase 2.3 — unusual-usage alert (frozen-baseline deviation).

        On ``shutoff`` the valve was auto-closed: the message MUST say WHY and HOW to
        reopen in one action (toggle the named valve entity), and is sent ``critical``
        so a safety shut-off always notifies regardless of the user's alert config —
        an opaque or suppressed shut-off would be worse than no feature.
        """
        reason = _humanize_anomaly_type(anomaly_type)
        if shutoff:
            where = (f"open “{valve_entity}” in Home Assistant"
                     if valve_entity else "open your main water valve in Home Assistant")
            dispatched = await self.fire(
                circuit, "unusual_usage",
                title=f"\U0001f6b1 Water shut off — {circuit_name}",
                message=(f"Automatic shut-off: {reason} (anomaly score {score:.0%}). "
                         f"Water to {circuit_name} was closed as a precaution. "
                         f"To restore water, {where}, or use the Open Valve button on "
                         f"the Water Monitor Valve & Tests page. If this was normal usage, "
                         f"recalibrate so it is not flagged again."),
                notification_id=f"water_unusual_shutoff_{circuit}",
                critical=True,
            )
        else:
            dispatched = await self.fire(
                circuit, "unusual_usage",
                title=f"\U0001f50d Unusual water usage — {circuit_name}",
                message=(f"{reason} (anomaly score {score:.0%}). This did not fit "
                         f"{circuit_name}'s learned pattern — review the History "
                         f"page, under the Unusual events filter. Lower Detection "
                         f"Sensitivity if these are false alarms."),
            )
        # Audit trail: record that this event actually notified. Best-effort —
        # a DB hiccup must never fail (or retry-spam) the alert itself.
        if dispatched and event_id:
            try:
                self._db.execute(
                    "UPDATE events SET triggered_alert = 1 WHERE id = ?",
                    (event_id,))
                self._db.commit()
            except Exception as e:  # noqa: BLE001 — audit write is non-critical
                log.warning("triggered_alert stamp failed for %s: %s", event_id, e)

    async def alert_pulsing_supply(self, circuit: str,
                                    circuit_name: str,
                                    event_count_30min: int) -> None:
        """Notify the user that recent events were captured during pulsing
        supply conditions and are flagged as degraded.

        Tone is intentionally neutral about cause — the same symptom can
        come from supply-side pulsation OR sensor-reversal artifacts. The
        message lists both with a quick diagnostic at the pre-PRV gauge.
        """
        await self.fire(
            circuit, "pulsing_supply",
            title=f"⚠ Degraded supply — {circuit_name}",
            message=(
                f"{event_count_30min} water events in the past 30 minutes "
                "were captured with chaotic pressure/flow readings — "
                "pattern is consistent with supply pulsation or sensor "
                "reversal artifacts. Volume totals for those events are "
                "estimated.\n\n"
                "Possible causes:\n"
                "• PRV (pressure-reducing valve) failure or chatter\n"
                "• Well pump short-cycling\n"
                "• Booster pump oscillation\n"
                "• Municipal supply pulsation\n"
                "• Flow sensor wiring noise or reversal artifacts\n\n"
                "Diagnostic: listen at the pre-PRV gauge — a steady hiss "
                "is normal; rhythmic ticking (period 1–6 s) indicates "
                "supply-side pulsation. If the gauge is steady but the "
                "addon still detects pulsing, suspect sensor or wiring."
            ),
            notification_id=f"water_pulsing_supply_{circuit}",
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
        # Route via the shared mobile-push helper so failures land in
        # the same failure-counter / cooldown machinery as event alerts
        # rather than being swallowed silently.
        for target in self._mobile_targets():
            await self._send_mobile_push(
                target,
                title="🏖 Away mode activated — Water Monitor",
                message="Leak tests continue. Baseline learning paused.",
                data={"tag": "water_away_mode"},
            )
