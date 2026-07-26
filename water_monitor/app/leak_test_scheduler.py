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
from datetime import tzinfo

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
    "Not run — water softener regenerating (deferred)",
}

# dev.24 — water-softener regeneration blackout. A regen draws on the softener's
# circuit (Main) and would read as a leak, so a leak test is kept out of
# [regen_start − lead, regen_start + window] (local). In-sample placeholders: the
# observed regen runs ~2.5 h; 210 min leaves margin. Static (deliberately NOT
# coupled to the live softener detector) — a generous fixed window is simpler and
# safer than per-run dynamic precision.
_SOFTENER_REGEN_LEAD_MIN: int = 20
_SOFTENER_REGEN_WINDOW_MIN: int = 210


def _row_get(row, key, default=None):
    """Safe key access for a sqlite3.Row (raises IndexError on a missing column)
    OR a plain dict (hand-built schedules in tests / a missing key)."""
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def learn_quiet_hour(db, circuit: str, ha_tz,
                     blackout_min=None) -> "Optional[int]":
    """Quietest local hour from the last 60 days of hourly_volume.

    Shared inference (leak-test scheduling AND the pump-regime detector's
    nightly analysis window — do not duplicate). hourly_volume only stores
    rows when water actually flowed, so hours absent from the table are
    genuinely silent. Score = (active_sample_count, avg_volume_lph, hour);
    lower wins, `hour` as deterministic tiebreak. Returns None when the
    table has no rows for this circuit (no signal yet).

    0–5 AM preference: pick the best-scoring night hour whose active count
    is within +1 of the global minimum. ``blackout_min`` is an optional
    (lo, hi) local-minutes window (softener regen) never to pick from.
    """
    rows = db.execute("""
        SELECT hour_ts, volume_litres
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= datetime('now', '-60 days')
    """, (circuit,)).fetchall()

    if not rows:
        return None

    counts = {hr: 0 for hr in range(24)}
    sums = {hr: 0.0 for hr in range(24)}
    for row in rows:
        try:
            local_hour = _parse_utc_ts(row["hour_ts"]).astimezone(ha_tz).hour
            counts[local_hour] += 1
            sums[local_hour] += float(row["volume_litres"] or 0.0)
        except (ValueError, AttributeError, TypeError):
            continue

    def score(hr: int) -> tuple:
        n = counts[hr]
        avg = sums[hr] / n if n else 0.0
        return (n, avg, hr)

    def _blacked(hr: int) -> bool:
        if blackout_min is None:
            return False
        lo, hi = blackout_min
        return hr * 60 < hi and (hr + 1) * 60 > lo   # hour overlaps [lo, hi)

    allowed = [hr for hr in range(24) if not _blacked(hr)]
    global_min_hr = min(allowed or list(range(24)), key=score)
    global_min_cnt = counts[global_min_hr]

    candidates = [hr for hr in range(0, 6)
                  if not _blacked(hr) and counts[hr] <= global_min_cnt + 1]
    best_hr = min(candidates, key=score) if candidates else global_min_hr

    avg = sums[best_hr] / counts[best_hr] if counts[best_hr] else 0.0
    log.info("[%s] learned best test hour: %02d:00 local "
             "(count %d, avg %.3f L/h)",
             circuit, best_hr, counts[best_hr], avg)
    return best_hr


class LeakTestScheduler:
    """Manages scheduled and on-demand leak tests for all circuits."""

    def __init__(self, cfg: AddonConfig, db: sqlite3.Connection,
                 ha: HaClient, alert_manager=None,
                 ha_tz: Optional[tzinfo] = None):
        self._cfg = cfg
        self._db  = db
        self._ha  = ha
        self._alert_manager = alert_manager
        self._ha_tz = ha_tz or timezone.utc
        self._stop = asyncio.Event()
        self._running_tests: Dict[str, bool] = {}
        self._tasks: Dict[str, Optional[asyncio.Task]] = {}
        # Strong refs for fire-and-forget guarded-run tasks so the GC can't
        # drop them mid-flight. _check_schedule spawns one per due test; the
        # task removes itself from the set when done.
        self._pending_scheduled: set[asyncio.Task] = set()

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
        task = asyncio.create_task(self._execute_test(
            circuit_cfg,
            notify_pass=notify_pass,
            notify_fail=notify_fail,
            triggered_by=triggered_by,
        ))
        self._tasks[circuit] = task
        try:
            result = await task
        except asyncio.CancelledError:
            result = {"result": "cancelled"}
        finally:
            self._running_tests[circuit] = False
            self._tasks.pop(circuit, None)

        return result

    def learn_best_hour(self, circuit: str) -> Optional[int]:
        """Quietest local hour for this circuit (shared helper — see
        ``learn_quiet_hour``; the pump-regime detector uses the same
        inference, so the logic lives once at module level)."""
        return learn_quiet_hour(self._db, circuit, self._ha_tz,
                                self._softener_blackout_min(circuit))

    def _softener_blackout_min(self, circuit: str):
        """Return ``(lo, hi)`` local minutes-since-midnight of the softener regen
        blackout for ``circuit``, or None when no softener is configured here.
        Hard-gated: ``has_water_softener`` AND this is the softener's circuit.
        Window = ``[regen_start − lead, regen_start + window]``; callers test a
        local minute m with ``lo <= m < hi`` (no-wrap — regen is overnight)."""
        try:
            from .database import get_home_profile
            from .event_rules import parse_hhmm_to_minutes
            prof = get_home_profile(self._db)
            if not (prof is not None and prof["has_water_softener"]
                    and (prof["softener_circuit"] or "main") == circuit):
                return None
            start = parse_hhmm_to_minutes(prof["softener_regen_start"])
            if start is None:
                return None
            return (max(0, start - _SOFTENER_REGEN_LEAD_MIN),
                    start + _SOFTENER_REGEN_WINDOW_MIN)
        except Exception as e:
            log.warning("[%s] softener blackout lookup failed (non-fatal): %s",
                        circuit, e)
            return None

    def _push_past_softener_blackout(self, candidate, circuit: str):
        """If ``candidate`` (local, tz-aware) lands in the softener regen
        blackout, move it to just after the window the same day; no-op otherwise.
        This keeps the scheduler from ever PLANNING a test inside the regen."""
        window = self._softener_blackout_min(circuit)
        if window is None:
            return candidate
        lo, hi = window
        cand_min = candidate.hour * 60 + candidate.minute
        if lo <= cand_min < hi:
            midnight = candidate.replace(hour=0, minute=0, second=0, microsecond=0)
            return midnight + timedelta(minutes=hi)
        return candidate

    def is_running(self, circuit: str) -> bool:
        return self._running_tests.get(circuit, False)

    def cancel(self, circuit: str) -> None:
        """
        Cancel a running leak test.  Cancels the asyncio task so _execute_test
        stops polling immediately; is_running() returns False right away so the
        UI reflects the abort without waiting for the next firmware poll cycle.
        The firmware has already received the stop command from the abort route
        before this method is called.
        """
        task = self._tasks.get(circuit)
        if task and not task.done():
            task.cancel()
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

        # 3-port valves: silently skip scheduled checks. We don't write a
        # skip row every cycle (would spam History); the user-facing
        # explanation lives in Settings + Device-page UI annotations. The
        # schedule row itself is preserved so switching back to 2_port
        # resumes the schedule without reconfiguration. Manual triggers
        # via run_now() DO record a skip row — see _execute_test().
        from .database import get_valve_type
        if get_valve_type(self._db, circuit, default="2_port") == "3_port":
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

            # Keep a strong reference so the GC can't drop the task before
            # run_now completes — bare asyncio.create_task() is a known
            # antipattern.
            t = asyncio.create_task(_run_guarded())
            self._pending_scheduled.add(t)
            t.add_done_callback(self._pending_scheduled.discard)
            # next_run_at is advanced here, immediately after dispatch —
            # not after completion. A failed background start (rare) will
            # still consume the slot. after_trigger=True prevents auto-
            # learn from scheduling another run today if it picked a
            # later hour. TODO: track dispatched vs completed if the
            # consumed-slot edge case becomes a real problem.
            await self._update_next_run(circuit, schedule, after_trigger=True)

    async def _update_next_run(self, circuit: str, schedule: Any,
                               *, after_trigger: bool = False) -> None:
        """
        Compute and store the next run timestamp.

        If auto_learn_hour is enabled (default), re-learn the best quiet
        hour from usage history before scheduling.  If disabled, respect
        the manually configured run_hour as-is.
        """
        # Pull the flag from the schedule (defaults to True for backward compat)
        # SQLite Row supports .get-equivalent via try/except; dicts have .get()
        try:
            auto_learn = schedule["auto_learn_hour"]
            if auto_learn is None:
                auto_learn = True
            else:
                auto_learn = bool(auto_learn)
        except (KeyError, IndexError):
            auto_learn = True

        if auto_learn:
            # learn_best_hour queries up to 60 days of hourly_volume and
            # does Python-side aggregation — offload to a worker thread
            # so the scheduler's async loop stays responsive while it
            # runs.
            loop = asyncio.get_running_loop()
            best_hour = await loop.run_in_executor(
                None, self.learn_best_hour, circuit,
            )
            if best_hour is not None:
                current_hour = schedule.get("run_hour") if isinstance(schedule, dict) \
                    else schedule["run_hour"]
                current_hour = current_hour or 2
                if best_hour != current_hour:
                    log.info("[%s] auto-learn updating run_hour %02d→%02d from usage history",
                             circuit, current_hour, best_hour)
                    upsert_leak_test_schedule(self._db, circuit, run_hour=best_hour)
                    schedule = dict(schedule)
                    schedule["run_hour"] = best_hour

        next_run = self._compute_next_run(schedule, after_trigger=after_trigger)
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
        name    = circuit_cfg.label
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

        # --- Pre-check: valve type compatibility (3-port has a drain port
        # that always reads as a leak, so the micro leak test does not
        # apply). The Device-page button is also disabled for 3-port
        # circuits, but the route is intentionally still reachable so
        # automations / direct POSTs produce a visible skip row here. ---
        from .database import get_valve_type
        if get_valve_type(self._db, circuit, default="2_port") == "3_port":
            log.info("[%s] leak test skipped — valve_type='3_port' "
                     "(drain-capable; micro leak test not applicable)",
                     circuit)
            result = ("Not run — 3-port valve (drain-capable; "
                      "micro leak test not applicable)")
            await self._store_result(circuit, run_at, triggered_by, result,
                                     None, None, None, None)
            return {"result": result, "skipped": True}

        # --- Pre-check: water-softener regeneration window (dev.24) ---
        # A regen draws on this (Main) circuit and would read as a leak. Defer
        # WITHOUT closing the valve, record a history row, and reschedule just
        # past the window so the test still runs and the cadence is preserved.
        _blackout = self._softener_blackout_min(circuit)
        if _blackout is not None:
            _now_local = run_at.astimezone(self._ha_tz)
            _m = _now_local.hour * 60 + _now_local.minute
            if _blackout[0] <= _m < _blackout[1]:
                log.info("[%s] leak test deferred — water softener regenerating",
                         circuit)
                result = "Not run — water softener regenerating (deferred)"
                await self._store_result(circuit, run_at, triggered_by, result,
                                         None, None, None, None)
                try:
                    _midnight = _now_local.replace(hour=0, minute=0, second=0,
                                                   microsecond=0)
                    _next = (_midnight + timedelta(minutes=_blackout[1])
                             ).astimezone(timezone.utc)
                    upsert_leak_test_schedule(self._db, circuit,
                                              next_run_at=_next.isoformat())
                except Exception as _ex:
                    log.warning("[%s] could not reschedule deferred test: %s",
                                circuit, _ex)
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

        # Wait for result — firmware takes 60s settle + up to configured duration + 2min buffer.
        # Fetch the firmware-configured test duration from the HA sensor entity.
        # (duration_minutes is NOT a DB column; it lives on the ESP firmware entity.)
        # An earlier version of this function also fetched the DB
        # leak_test_schedules row here, but never read from it — duration
        # always comes from the live HA entity below, so the extra DB
        # roundtrip was deleted.
        try:
            dur_str = await self._ha.get_state_value(
                circuit_cfg.leak_test_duration_entity, "30")
            cfg_duration = int(float(dur_str)) if dur_str else 30
        except (ValueError, TypeError):
            cfg_duration = 30
        max_wait_seconds = 60 + (cfg_duration * 60) + 120
        start_wait   = datetime.now(timezone.utc)
        final_result = "In progress"
        result_str   = "unknown"   # initialised before loop so timeout log is safe

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
            log.warning(
                "[%s] leak test timed out after %.0f s — firmware result '%s' not in "
                "TERMINAL_RESULTS. Check firmware version or update TERMINAL_RESULTS.",
                circuit,
                (datetime.now(timezone.utc) - start_wait).total_seconds(),
                result_str,
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

    def _compute_next_run(
        self, schedule: Any, now: Optional[datetime] = None,
        *, after_trigger: bool = False,
    ) -> Optional[datetime]:
        """Compute the next scheduled run datetime (returned in UTC).

        ``now`` must be timezone-aware if provided; raises ``ValueError``
        for naive values.  Defaults to ``datetime.now(self._ha_tz)``.
        The candidate is built in local (HA) time and converted to UTC so
        that ``run_hour=2`` always means 2 AM local, not 2 AM UTC.

        ``after_trigger`` is set by ``_check_schedule`` immediately after
        dispatching a test, so auto-learn moving ``run_hour`` to a later
        time today can't schedule a second test on the same day. It only
        affects the daily branch; weekly/fortnightly/monthly already have
        ≥7-day natural gaps.
        """
        if now is None:
            now = datetime.now(self._ha_tz)
        elif now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        now_local  = now.astimezone(self._ha_tz)
        freq       = schedule["frequency"]
        run_hour   = schedule["run_hour"]   or 2
        run_minute = schedule["run_minute"] or 0

        if freq == "daily":
            candidate = now_local.replace(hour=run_hour, minute=run_minute,
                                          second=0, microsecond=0)
            if candidate <= now_local:
                candidate += timedelta(days=1)
            if after_trigger and candidate - now_local < timedelta(hours=18):
                candidate += timedelta(days=1)
            candidate = self._push_past_softener_blackout(
                candidate, _row_get(schedule, "circuit"))
            return candidate.astimezone(timezone.utc)

        target_dow = schedule["day_of_week"] or 0   # 0=Monday
        days_ahead = (target_dow - now_local.weekday()) % 7
        # If today is the target weekday, only push to next week if today's
        # run_hour has already passed — otherwise today at run_hour is the
        # correct next run (mirroring the daily branch's same-day handling).
        candidate = (now_local + timedelta(days=days_ahead)).replace(
            hour=run_hour, minute=run_minute, second=0, microsecond=0)
        if days_ahead == 0 and candidate <= now_local:
            days_ahead = 7
            candidate = (now_local + timedelta(days=days_ahead)).replace(
                hour=run_hour, minute=run_minute, second=0, microsecond=0)
        if freq == "fortnightly" and days_ahead != 0:
            # Fortnightly = same weekday two weeks out, unless we already
            # picked "today" (no extra week needed in that case).
            candidate += timedelta(days=7)

        if freq == "monthly":
            week_of_month = schedule["week_of_month"] or 1
            candidate = _nth_weekday_of_month(
                now_local.year, now_local.month, target_dow, week_of_month,
                run_hour, run_minute, self._ha_tz)
            if candidate.astimezone(timezone.utc) <= now.astimezone(timezone.utc):
                if now_local.month == 12:
                    candidate = _nth_weekday_of_month(
                        now_local.year + 1, 1, target_dow, week_of_month,
                        run_hour, run_minute, self._ha_tz)
                else:
                    candidate = _nth_weekday_of_month(
                        now_local.year, now_local.month + 1, target_dow,
                        week_of_month, run_hour, run_minute, self._ha_tz)

        candidate = self._push_past_softener_blackout(
            candidate, _row_get(schedule, "circuit"))
        return candidate.astimezone(timezone.utc)

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


def _parse_utc_ts(ts_str: str) -> datetime:
    """Parse an hour_ts string (UTC, possibly naive or Z-suffixed) to UTC datetime."""
    s = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _nth_weekday_of_month(
    year: int, month: int, weekday: int, n: int, hour: int, minute: int,
    tz: tzinfo = timezone.utc,
) -> datetime:
    """Return the nth occurrence of weekday in the given month, in timezone ``tz``."""
    first = datetime(year, month, 1, hour, minute, tzinfo=tz)
    days_ahead = (weekday - first.weekday()) % 7
    first_occurrence = first + timedelta(days=days_ahead)

    if n == -1:
        while (first_occurrence + timedelta(weeks=1)).month == month:
            first_occurrence += timedelta(weeks=1)
        return first_occurrence

    return first_occurrence + timedelta(weeks=n - 1)
