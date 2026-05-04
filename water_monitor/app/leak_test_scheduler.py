"""
Leak test scheduler.

The scheduler decides WHEN to run — it learns the quietest hour of the day
from historical usage data and schedules tests at that time (e.g. 1am).

Key design decision: there is NO quiet-period flow check inside _execute_test.
Waiting for the house to go quiet is the wrong approach — instead, the
scheduler picks a time when the house is statistically quiet.

Two code paths:
  run_now(triggered_by="manual")    — immediate, from web UI
  run_now(triggered_by="scheduled") — called by _check_schedule at the
                                       learned quiet hour
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from .config import AddonConfig
from .database import (get_leak_test_schedule, upsert_leak_test_schedule,
                       insert_leak_test_history, get_leak_test_history)
from .ha_client import HaClient

log = logging.getLogger(__name__)

# Result strings published by the ESP firmware that indicate the test is done
TERMINAL_RESULTS = {
    "Passed — no leak detected",
    "Failed — leak detected",
    "Aborted — external pressure influence",
    "Stopped manually",
    "Not run — valve was closed (open valve first)",
    "Not run — fault active (reset fault first)",
    "Aborted — demand detected (sudden pressure drop)",
}


class LeakTestScheduler:
    """Manages scheduled and on-demand leak tests for all circuits."""

    def __init__(self, cfg: AddonConfig, db: sqlite3.Connection,
                 ha: HaClient, alert_manager=None):
        self._cfg = cfg
        self._db  = db
        self._ha  = ha
        self._alert_manager = alert_manager
        self._stop = asyncio.Event()
        self._running_tests: Dict[str, bool] = {}

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Background loop — check for due scheduled tests every 60s."""
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
                return
            except asyncio.TimeoutError:
                pass

            for circuit_cfg in self._cfg.circuits:
                try:
                    await self._check_schedule(circuit_cfg.circuit)
                except Exception as e:
                    log.error("[%s] leak test scheduler error: %s",
                              circuit_cfg.circuit, e)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run_now(self, circuit: str,
                      triggered_by: str = "manual") -> Dict[str, Any]:
        """
        Run a leak test immediately.

        The scheduler has already chosen the right time via learn_best_hour,
        so there is no quiet-period wait here — the test starts immediately.
        """
        if self._running_tests.get(circuit):
            raise ValueError(f"Leak test already running on {circuit}")

        circuit_cfg = self._cfg.get_circuit(circuit)
        if not circuit_cfg:
            raise ValueError(f"Unknown circuit: {circuit}")

        schedule    = get_leak_test_schedule(self._db, circuit)
        notify_pass = schedule["notify_on_pass"] if schedule else True
        notify_fail = schedule["notify_on_fail"] if schedule else True

        self._running_tests[circuit] = True
        result = {}
        try:
            result = await self._execute_test(
                circuit_cfg,
                notify_pass=notify_pass,
                notify_fail=notify_fail,
                triggered_by=triggered_by,
            )
        finally:
            self._running_tests[circuit] = False

        return result

    def learn_best_hour(self, circuit: str) -> Optional[int]:
        """
        Analyse hourly_volume history to find the quietest hour of day.

        Queries the last 60 days of data, averages usage per hour (0-23),
        and returns the hour with the lowest average.  Prefers 0-5am when
        two hours are equally quiet.  Returns None if < 7 days of data exist.
        """
        rows = self._db.execute("""
            SELECT
                CAST(strftime('%H', hour_ts) AS INTEGER) AS hr,
                AVG(volume_litres)                       AS avg_vol,
                COUNT(*)                                 AS days
            FROM hourly_volume
            WHERE circuit = ?
              AND hour_ts >= datetime('now', '-60 days')
            GROUP BY hr
            HAVING days >= 7
            ORDER BY avg_vol ASC, hr ASC
        """, (circuit,)).fetchall()

        if not rows:
            return None

        best_hr  = None
        best_avg = None
        for row in rows:
            hr, avg_vol = row["hr"], row["avg_vol"]
            if best_avg is None or avg_vol < best_avg * 0.95:
                best_hr  = hr
                best_avg = avg_vol
            elif avg_vol < best_avg * 1.05 and 0 <= hr <= 5:
                # Similar usage but overnight — prefer it
                best_hr  = hr
                best_avg = avg_vol

        log.info("[%s] learned best test hour: %02d:00 (avg %.3f L/h)",
                 circuit, best_hr, best_avg)
        return best_hr

    def is_running(self, circuit: str) -> bool:
        return self._running_tests.get(circuit, False)

    def cancel(self, circuit: str) -> None:
        """
        Mark a circuit's test as no longer running.
        Called by the abort endpoint so is_running() returns False immediately,
        letting the live-state poll reflect the abort without delay.
        The background _execute_test task still finishes cleanly (stores the
        'Stopped manually' result from the firmware) but the UI won't wait for it.
        """
        self._running_tests[circuit] = False
        log.info("[%s] leak test cancelled via abort", circuit)

    def get_history(self, circuit: str, limit: int = 20) -> list:
        return get_leak_test_history(self._db, circuit, limit)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _check_schedule(self, circuit: str) -> None:
        """Check if a scheduled test is due and run it if so."""
        schedule = get_leak_test_schedule(self._db, circuit)
        if not schedule or not schedule["enabled"]:
            return

        next_run_str = schedule["next_run_at"]
        if not next_run_str:
            await self._update_next_run(circuit, schedule)
            return

        next_run = datetime.fromisoformat(next_run_str.replace("Z", "+00:00"))
        if next_run.tzinfo is None:
            next_run = next_run.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        if now >= next_run:
            log.info("[%s] scheduled leak test due — running", circuit)

            async def _run_guarded():
                try:
                    await self.run_now(circuit, triggered_by="scheduled")
                except ValueError as e:
                    log.warning("[%s] scheduled leak test skipped: %s", circuit, e)
                except Exception as e:
                    log.error("[%s] scheduled leak test error: %s",
                              circuit, e, exc_info=True)

            asyncio.create_task(_run_guarded())
            await self._update_next_run(circuit, schedule)

    async def _update_next_run(self, circuit: str, schedule: Any) -> None:
        """
        Compute and store the next run timestamp.
        Re-learns the best quiet hour from usage history before scheduling.
        """
        best_hour = self.learn_best_hour(circuit)
        if best_hour is not None:
            current_hour = schedule.get("run_hour") or 2
            if best_hour != current_hour:
                log.info("[%s] updating run_hour %02d→%02d from usage history",
                         circuit, current_hour, best_hour)
                upsert_leak_test_schedule(self._db, circuit, run_hour=best_hour)
                schedule = dict(schedule)
                schedule["run_hour"] = best_hour

        next_run = _compute_next_run(schedule)
        if next_run:
            upsert_leak_test_schedule(self._db, circuit,
                                      next_run_at=next_run.isoformat())

    async def _execute_test(
        self,
        circuit_cfg: Any,
        notify_pass: bool,
        notify_fail: bool,
        triggered_by: str,
    ) -> Dict[str, Any]:
        """
        Execute a leak test on one circuit.

        The scheduler already chose a statistically quiet time, so there is
        no flow-based gating here — just valve/fault pre-checks then start.
        """
        circuit = circuit_cfg.circuit
        name    = circuit_cfg.display_name
        run_at  = datetime.now(timezone.utc)

        # --- Pre-check: valve open ---
        valve_state = await self._ha.get_state_value(
            circuit_cfg.valve_entity, "unknown")
        if valve_state != "open":
            log.info("[%s] leak test skipped — valve not open", circuit)
            result = "Not run — valve was closed (open valve first)"
            await self._store_result(circuit, run_at, triggered_by, result,
                                     None, None, None, None)
            return {"result": result, "skipped": True}

        # --- Pre-check: no active fault ---
        fault = await self._ha.get_state_value(circuit_cfg.fault_sensor, "off")
        if fault == "on":
            log.info("[%s] leak test skipped — fault active", circuit)
            result = "Not run — fault active (reset fault first)"
            await self._store_result(circuit, run_at, triggered_by, result,
                                     None, None, None, None)
            return {"result": result, "skipped": True}

        # --- Baseline pressure ---
        pressure_str = await self._ha.get_state_value(
            circuit_cfg.pressure_avg_sensor, "0")
        try:
            baseline_psi = float(pressure_str)
        except (ValueError, TypeError):
            baseline_psi = None

        # --- Start the firmware leak test ---
        log.info("[%s] starting leak test (triggered_by=%s)", circuit, triggered_by)
        await self._ha.turn_on(circuit_cfg.leak_test_switch)

        # Wait for result — firmware takes 60s settle + up to 30min test + 2min buffer.
        # A hard cap prevents infinite polling if firmware changes result strings.
        schedule    = get_leak_test_schedule(self._db, circuit) if 'schedule' not in dir() else schedule
        cfg_duration = (schedule["duration_minutes"] if schedule else 30)
        max_wait_seconds = 60 + (cfg_duration * 60) + 120
        start_wait   = datetime.now(timezone.utc)
        final_result = "In progress"
        timed_out    = False

        while (datetime.now(timezone.utc) - start_wait).total_seconds() < max_wait_seconds:
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=10)
                return {"result": "cancelled"}
            except asyncio.TimeoutError:
                pass

            result_str = await self._ha.get_state_value(
                circuit_cfg.leak_test_result_sensor, "In progress")
            if result_str in TERMINAL_RESULTS:
                final_result = result_str
                break
        else:
            timed_out = True
            log.warning(
                "[%s] leak test timed out after %.0f s — firmware result '%s' not in "
                "TERMINAL_RESULTS. Check firmware version or update TERMINAL_RESULTS.",
                circuit,
                (datetime.now(timezone.utc) - start_wait).total_seconds(),
                result_str if 'result_str' in dir() else "unknown",
            )
            final_result = "Timed out — no terminal result received"

        # --- Final pressure ---
        final_pressure_str = await self._ha.get_state_value(
            circuit_cfg.pressure_avg_sensor, "0")
        try:
            final_psi = float(final_pressure_str)
        except (ValueError, TypeError):
            final_psi = None

        pressure_drop = (
            round(baseline_psi - final_psi, 2)
            if baseline_psi is not None and final_psi is not None
            else None
        )
        duration_minutes = round(
            (datetime.now(timezone.utc) - run_at).total_seconds() / 60, 1)

        # --- Persist result ---
        await self._store_result(
            circuit, run_at, triggered_by, final_result,
            duration_minutes, baseline_psi, final_psi, pressure_drop,
        )

        upsert_leak_test_schedule(self._db, circuit,
                                  last_run_at=run_at.isoformat(),
                                  last_result=final_result)

        # --- HA notification via AlertManager ---
        passed = final_result == "Passed — no leak detected"
        am = self._alert_manager
        if am and passed and notify_pass:
            await am.alert_leak_test_passed(
                circuit, duration_minutes or 0, name)
        elif am and not passed and notify_fail and "Passed" not in final_result:
            if "Failed" in final_result or "leak" in final_result.lower():
                await am.alert_leak_test_failed(
                    circuit, pressure_drop or 0, name)
            else:
                # Other non-pass results (aborted, skipped)
                await am.fire(
                    circuit, "leak_test",
                    title=f"Water Monitor — Leak test ({name})",
                    message=f"Result: {final_result}.",
                    notification_id=f"water_leak_test_result_{circuit}",
                )

        log.info("[%s] leak test complete — %s (%.1f min, %.2f PSI drop)",
                 circuit, final_result, duration_minutes or 0, pressure_drop or 0)

        return {
            "result":           final_result,
            "duration_minutes": duration_minutes,
            "baseline_psi":     baseline_psi,
            "final_psi":        final_psi,
            "pressure_drop_psi": pressure_drop,
            "passed":           passed,
        }

    async def _store_result(
        self, circuit: str, run_at: datetime, triggered_by: str,
        result: str, duration: Optional[float],
        baseline_psi: Optional[float], final_psi: Optional[float],
        pressure_drop: Optional[float],
    ) -> None:
        insert_leak_test_history(
            self._db,
            circuit=circuit,
            run_at=run_at.isoformat(),
            triggered_by=triggered_by,
            result=result,
            duration_minutes=duration,
            baseline_psi=baseline_psi,
            final_psi=final_psi,
            pressure_drop_psi=pressure_drop,
        )


# ------------------------------------------------------------------
# Schedule computation helpers
# ------------------------------------------------------------------

def _compute_next_run(schedule: Any) -> Optional[datetime]:
    """Compute the next scheduled run datetime from a schedule row."""
    now        = datetime.now(timezone.utc)
    freq       = schedule["frequency"]
    run_hour   = schedule["run_hour"]   or 2
    run_minute = schedule["run_minute"] or 0

    if freq == "custom" and schedule["custom_interval_days"]:
        last = schedule["last_run_at"]
        if last:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            candidate = last_dt + timedelta(days=schedule["custom_interval_days"])
        else:
            candidate = now + timedelta(days=schedule["custom_interval_days"])
        return candidate.replace(hour=run_hour, minute=run_minute,
                                 second=0, microsecond=0)

    if freq == "daily":
        # Today at run_hour:run_minute, or tomorrow if that time has already passed
        candidate = now.replace(hour=run_hour, minute=run_minute,
                                second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate

    target_dow = schedule["day_of_week"] or 0   # 0=Monday
    days_ahead = (target_dow - now.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    if freq == "fortnightly":
        days_ahead += 7

    candidate = (now + timedelta(days=days_ahead)).replace(
        hour=run_hour, minute=run_minute, second=0, microsecond=0)

    if freq == "monthly":
        week_of_month = schedule["week_of_month"] or 1
        candidate = _nth_weekday_of_month(
            now.year, now.month, target_dow, week_of_month, run_hour, run_minute)
        if candidate <= now:
            if now.month == 12:
                candidate = _nth_weekday_of_month(
                    now.year + 1, 1, target_dow, week_of_month, run_hour, run_minute)
            else:
                candidate = _nth_weekday_of_month(
                    now.year, now.month + 1, target_dow, week_of_month,
                    run_hour, run_minute)

    return candidate


def _nth_weekday_of_month(
    year: int, month: int, weekday: int, n: int, hour: int, minute: int,
) -> datetime:
    """Return the nth occurrence of weekday in the given month."""
    first = datetime(year, month, 1, hour, minute, tzinfo=timezone.utc)
    days_ahead = (weekday - first.weekday()) % 7
    first_occurrence = first + timedelta(days=days_ahead)

    if n == -1:
        while (first_occurrence + timedelta(weeks=1)).month == month:
            first_occurrence += timedelta(weeks=1)
        return first_occurrence

    return first_occurrence + timedelta(weeks=n - 1)
