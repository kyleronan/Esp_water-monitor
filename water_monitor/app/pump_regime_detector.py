"""Nightly pump-regime detector (pump plan Phase 3, dev23).

Analyzes each circuit's quiet-hour pressure/flow window from HA history with
the STUDY-VALIDATED math in pump_regime_math (2026-07-21: 13/13 negative
nights incl. softener regens, 2/2 positive) and records one row per circuit
per EVALUATED night in pump_regime_nightly. Skipped nights (HA outage, no
usable quiet window) write NO row and are invisible to the hysteresis
counters by design.

Home-level flag (banner + confirm — detection NEVER silently enables
pump-aware behavior):
  * SET pump_mode_detected=1 after >=2 detected of the last 3 evaluated
    nights (ANY circuit — the pump is upstream of all of them).
  * CLEAR after 7 consecutive evaluated quiet nights; clearing resets
    pump_mode_ack so a future re-detection re-banners. A user-confirmed
    supply_type keeps pump mode on regardless of clearing.
  * pump_detect_period_s refreshes on EVERY detected night (a stale period
    drifts exactly when the Phase 5 period-shrink trigger cares); the
    detecting circuit's period wins, circuit_1 preferred deterministically.
  * pump_profile is written ('vfd_constant_pressure') only when currently
    NULL — never over a user-set or previously-detected profile. (v1
    detection can only emit the vfd profile; switch_tank arrives via the
    supply answer — see the plan's round-3 #7 correction.)

SCOPE HONESTY (plan finding #11): this detector structurally detects
*pump + leak*, not pumps — a healthy pump on a tight home never cycles
overnight, so auto-detection never fires for it. The setup supply question
covers deliberate installs; detection exists for the mid-life-install case
(exactly how this feature was born: 2026-07-19, ESYBOX + zone-valve leak).
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .database import run_db

log = logging.getLogger(__name__)

_STARTUP_DELAY_S = 240      # stagger past rise_corr_backfill's 180 s boot fetch
_WINDOW_HOURS = 3           # analysis window length starting at the quiet hour
_DEFAULT_QUIET_HOUR = 2     # local fallback when hourly_volume has no signal
_RECHECK_ON_ERROR_S = 3600  # retry delay after an HA fetch failure
_SET_LOOKBACK = 3           # hysteresis: >=2 detected of last 3 evaluated
_SET_REQUIRED = 2
_CLEAR_QUIET_NIGHTS = 7     # consecutive evaluated quiet nights to clear
_REBANNER_EVALUATED_NIGHTS = 30   # recurring re-banner cadence post-dismissal

# ── Phase 5a leak estimation + alert ──────────────────────────────────────────
# Street-meter calibration (2026-07-25, same-window iPERL vs home estimator):
# the oval-gear registers ~half of each recharge slug, so metered estimates
# scale by 1.9 to report TRUE leak rate. Per-installation constant; re-derive
# if the meter changes.
PUMP_SLUG_CALIBRATION_FACTOR: float = 1.9
PUMP_LEAK_ALERT_LPD: float = 20.0   # calibrated L/day threshold
_ALERT_CONSEC_NIGHTS = 3            # evaluated nights at/above threshold
_PERIOD_SHRINK_RATIO = 0.7          # last-7 vs prev-7 evaluated median
_PERIOD_TREND_NIGHTS = 14


def evaluate_leak_alert(nights: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Alert decision from home-level EVALUATED nights (newest first, each
    carrying est_leak_lpd + period_s). TRANSITION-ONLY: fires when the
    condition holds now but did NOT hold one night earlier, so a persistent
    leak alerts once instead of nightly (the HA notification_id keeps the
    sidebar entry alive regardless). Two triggers (plan 5a):
      * threshold: est >= PUMP_LEAK_ALERT_LPD on 3 consecutive evaluated
        nights;
      * period-shrink: median period over the last 7 evaluated nights < 0.7 x
        the previous 7 (the leak is growing) — "week" = evaluated nights
        (plan round-1 #21).
    """
    def _threshold_at(offset: int) -> bool:
        window = nights[offset:offset + _ALERT_CONSEC_NIGHTS]
        return (len(window) >= _ALERT_CONSEC_NIGHTS
                and all((n.get("est_leak_lpd") or 0) >= PUMP_LEAK_ALERT_LPD
                        for n in window))

    def _shrink_at(offset: int) -> bool:
        window = nights[offset:offset + _PERIOD_TREND_NIGHTS]
        periods = [n.get("period_s") for n in window if n.get("period_s")]
        if len(window) < _PERIOD_TREND_NIGHTS or len(periods) < 10:
            return False
        recent = sorted(p for n in window[:7] if (p := n.get("period_s")))
        prior = sorted(p for n in window[7:14] if (p := n.get("period_s")))
        if len(recent) < 4 or len(prior) < 4:
            return False
        med_r = recent[len(recent) // 2]
        med_p = prior[len(prior) // 2]
        return med_r < _PERIOD_SHRINK_RATIO * med_p

    if _threshold_at(0) and not _threshold_at(1):
        lpd = float(nights[0].get("est_leak_lpd") or 0.0)
        return {"reason": "threshold", "lpd": lpd,
                "period_s": nights[0].get("period_s")}
    if _shrink_at(0) and not _shrink_at(1):
        lpd = float(nights[0].get("est_leak_lpd") or 0.0)
        return {"reason": "period_shrink", "lpd": lpd,
                "period_s": nights[0].get("period_s")}
    return None


def evaluate_hysteresis(nights: List[Dict[str, Any]],
                        currently_detected: bool) -> Optional[str]:
    """'set' / 'clear' / None from home-level EVALUATED nights (newest first,
    each {'night_date', 'any_detected'}). Skipped nights simply aren't in the
    list — consecutive-ness is over evaluated nights only (plan finding #7).
    """
    if not currently_detected:
        recent = nights[:_SET_LOOKBACK]
        if (len(recent) >= _SET_REQUIRED
                and sum(1 for n in recent if n["any_detected"]) >= _SET_REQUIRED):
            return "set"
        return None
    recent = nights[:_CLEAR_QUIET_NIGHTS]
    if (len(recent) >= _CLEAR_QUIET_NIGHTS
            and all(not n["any_detected"] for n in recent)):
        return "clear"
    return None


def pump_banner_state(db: sqlite3.Connection) -> Dict[str, Any]:
    """Whether the 'booster pump detected?' banner should show, plus copy
    inputs. Conditions (plan round-3 #5, round-1 #5, round-1 #9/round-3 #11):
    detected AND supply_type NOT a pump type (well homes are ALREADY pump
    homes — Yes must never overwrite 'well') AND not confirmed AND no circuit
    forced pump_mode='off' AND (never dismissed, or detection persisted >=30
    evaluated nights since the dismissal — recurring, self-limiting).
    """
    from .config import PUMP_SUPPLY_TYPES
    from .database import get_home_profile, get_pump_regime_nights
    prof = get_home_profile(db)
    if not prof or not prof["pump_mode_detected"]:
        return {"show": False}
    if (prof["supply_type"] or "mains") in PUMP_SUPPLY_TYPES:
        return {"show": False}
    ack = prof["pump_mode_ack"] or ""
    if ack.startswith("confirmed"):
        return {"show": False}
    off = db.execute(
        "SELECT 1 FROM sensitivity_config WHERE pump_mode = 'off' LIMIT 1"
    ).fetchone()
    if off:
        return {"show": False}
    if ack.startswith("dismissed"):
        # Recurring re-banner: only after >=30 evaluated nights SINCE the
        # dismissal, all while detection persists. The dismissal night is
        # carried inside the ack value ('dismissed:<YYYY-MM-DD>') — the
        # 20260558 columns shipped before this detail existed, and the ack
        # string was the designated free slot.
        since = ack.split(":", 1)[1] if ":" in ack else None
        nights = get_pump_regime_nights(db, limit=200)
        evaluated_since = [n for n in nights
                          if since is None or n["night_date"] > since]
        if len(evaluated_since) < _REBANNER_EVALUATED_NIGHTS:
            return {"show": False}
    period = prof["pump_detect_period_s"]
    return {
        "show": True,
        "period_min": round(period / 60.0, 1) if period else None,
    }


def pump_floor_hint(db: sqlite3.Connection) -> Optional[float]:
    """Phase 6b suggested pump-failure floor: median quiet-window minimum
    pressure (≈ pump cut-in on leak nights; the bottom of the normal band on
    healthy nights) minus 5 PSI. Uses ANY historical evaluated nights —
    detected nights preferred — so a home that fixes its leak early still
    gets a hint (plan round-1 #18 / round-5 #3). Never auto-applied; the
    settings page offers it as a one-tap apply."""
    rows = db.execute(
        "SELECT min_psi, detected FROM pump_regime_nightly "
        "WHERE min_psi IS NOT NULL ORDER BY night_date DESC LIMIT 30"
    ).fetchall()
    if not rows:
        return None
    detected = sorted(float(r[0]) for r in rows if r[1])
    pool = detected or sorted(float(r[0]) for r in rows)
    return round(pool[len(pool) // 2] - 5.0, 1)


class PumpRegimeDetector:
    """Supervised nightly worker. One pass per local day, shortly after the
    quiet-hour window ends; also runs a catch-up pass at boot when last
    night was never evaluated (deploys shouldn't wait a day)."""

    def __init__(self, db: sqlite3.Connection, cfg, ha, ha_tz=None,
                 alert_manager=None):
        self._db = db
        self._cfg = cfg
        self._ha = ha
        self._ha_tz = ha_tz or timezone.utc
        self._alert_manager = alert_manager   # Phase 5a; None in tests
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def set_timezone(self, tz) -> None:
        self._ha_tz = tz

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        if self._ha is None:
            await self._stop.wait()
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=_STARTUP_DELAY_S)
            return
        except asyncio.TimeoutError:
            pass

        while not self._stop.is_set():
            try:
                ran = await self._analyze_due_nights()
            except Exception as e:
                log.warning("pump-regime pass failed (non-fatal): %s", e)
                ran = False
            delay = (await run_db(self._seconds_until_next_pass)
                     if ran else _RECHECK_ON_ERROR_S)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                continue

    # ── scheduling ───────────────────────────────────────────────────────────

    def _quiet_hour(self) -> int:
        """Shared quiet-hour inference (leak_test_scheduler helper) on the
        main circuit; deterministic fallback when there's no signal yet."""
        try:
            from .leak_test_scheduler import learn_quiet_hour
            circuits = [c.circuit for c in self._cfg.circuits] or ["circuit_1"]
            hr = learn_quiet_hour(self._db, circuits[0], self._ha_tz)
            return hr if hr is not None else _DEFAULT_QUIET_HOUR
        except Exception:
            return _DEFAULT_QUIET_HOUR

    def _seconds_until_next_pass(self) -> float:
        """Sleep until ~30 min after the next quiet window ends."""
        now = datetime.now(self._ha_tz)
        qh = self._quiet_hour()          # already on the DB thread (N2b)
        target = now.replace(hour=(qh + _WINDOW_HOURS) % 24, minute=30,
                             second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return max(60.0, (target - now).total_seconds())

    def _last_night_date(self) -> str:
        """The local calendar date whose quiet window most recently ENDED."""
        now = datetime.now(self._ha_tz)
        qh = self._quiet_hour()
        night = now.date()
        if now.hour < (qh + _WINDOW_HOURS) % 24 or now.hour < qh:
            night = night - timedelta(days=1)
        return night.isoformat()

    # ── nightly analysis ─────────────────────────────────────────────────────

    async def _analyze_due_nights(self) -> bool:
        """Evaluate last night for every circuit missing a row. Returns True
        when the pass completed without HA fetch errors (a clean 'nothing to
        do' also counts). No row is written for unevaluable windows."""
        night = await run_db(self._last_night_date)
        ok = True
        wrote_any = False
        for circuit_cfg in self._cfg.circuits:
            circuit = circuit_cfg.circuit
            exists = await run_db(self._night_row_exists_sync, circuit,
                                  night)
            if exists:
                continue
            try:
                wrote_any |= await self._analyze_circuit_night(
                    circuit_cfg, night)
            except Exception as e:
                log.warning("[%s] pump-regime analysis failed for %s: %s "
                            "(night skipped — invisible to hysteresis)",
                            circuit, night, e)
                ok = False
        if wrote_any:
            await run_db(self._apply_hysteresis)
            await self._maybe_leak_alert()
        return ok

    def _night_row_exists_sync(self, circuit: str, night: str):
        """dev46 (46a) — has this night already been evaluated?"""
        return self._db.execute(
            "SELECT 1 FROM pump_regime_nightly "
            "WHERE circuit = ? AND night_date = ?",
            (circuit, night)).fetchone()

    def _leak_alert_inputs_sync(self, circuits) -> dict:
        """dev46 (46a) — pump-gate check + the nightly history, one hop."""
        from .config import pump_gates_active
        from .database import get_pump_regime_nights
        return {"any_pump": any(pump_gates_active(self._db, c)
                                for c in circuits),
                "nights": get_pump_regime_nights(self._db, limit=40)}

    async def _maybe_leak_alert(self) -> None:
        """Phase 5a: transition-only slow-leak alert. Gated on ARMED pump
        mode (the arming rule — a confirmed/post-feature answer or observed
        evidence; pump_gates_active implies the confirmed half) and
        notify-only by standing decision."""
        if self._alert_manager is None:
            return
        try:
            from .config import pump_gates_active
            from .database import get_pump_regime_nights
            circuits = [c.circuit for c in self._cfg.circuits]
            # dev46 (46a): the gate check and the nights read are adjacent —
            # one hop.
            _la = await run_db(self._leak_alert_inputs_sync, circuits)
            if not _la["any_pump"]:
                return
            nights = _la["nights"]
            verdict = evaluate_leak_alert(nights)
            if verdict is None:
                return
            period_min = (verdict["period_s"] / 60.0
                          if verdict.get("period_s") else None)
            circuit = circuits[0] if circuits else "circuit_1"
            cfg = self._cfg.get_circuit(circuit) if hasattr(
                self._cfg, "get_circuit") else None
            name = cfg.label if cfg else circuit
            await self._alert_manager.alert_pump_leak_suspected(
                circuit, verdict["lpd"], period_min, name)
            log.info("pump-leak alert fired (%s): ~%.0f L/day",
                     verdict["reason"], verdict["lpd"])
        except Exception as e:
            log.warning("pump-leak alert evaluation failed (non-fatal): %s", e)

    # Flow minutes on a sibling circuit that invalidate a leak estimate. A few
    # scattered minutes (a night flush) leave the pressure trace usable; the
    # 7/28 irrigation program ran for well over an hour inside the window.
    _CONTAMINATION_MIN_MINUTES: float = 5.0

    async def _sibling_flow_minutes(self, circuit: str, w_start, w_end):
        """(sibling_circuit, minutes) when ANOTHER circuit drew water inside
        this analysis window, else None.

        The leak estimate reads the pressure decline of the whole house, so it
        is only meaningful when the whole house is quiet — but the quiet-window
        search only sees the analysed circuit's own meter. Best-effort: an HA
        history failure returns None (estimate proceeds as before).
        """
        others = [c for c in self._cfg.circuits if c.circuit != circuit
                  and getattr(c, "flow_sensor", None)]
        if not others:
            return None
        try:
            hist = await self._ha.get_history_batch(
                [c.flow_sensor for c in others], w_start, w_end)
        except Exception as e:
            log.debug("[%s] sibling-flow check failed (non-fatal): %s",
                      circuit, e)
            return None
        n = max(1, int((w_end - w_start).total_seconds()))
        for c in others:
            rows = hist.get(c.flow_sensor) or []
            if not rows:
                continue
            series = _ffill_1hz(rows, w_start, n, default=0.0)
            minutes = float((series > 0.0).sum()) / 60.0
            if minutes >= self._CONTAMINATION_MIN_MINUTES:
                return (c.circuit, minutes)
        return None

    async def _analyze_circuit_night(self, circuit_cfg, night: str) -> bool:
        import numpy as np
        from .pump_regime_math import (detect_pump_regime,
                                       estimate_leak_rate_lph, quiet_windows)

        pressure_entity = (getattr(circuit_cfg, "pressure_history_sensor", None)
                           or getattr(circuit_cfg, "pressure_avg_sensor", None))
        flow_entity = getattr(circuit_cfg, "flow_sensor", None)
        if not pressure_entity or not flow_entity:
            return False

        qh = await run_db(self._quiet_hour)
        local_start = datetime.fromisoformat(night).replace(
            hour=qh, tzinfo=self._ha_tz)
        start = local_start.astimezone(timezone.utc)
        end = start + timedelta(hours=_WINDOW_HOURS)

        hist = await self._ha.get_history_batch(
            [pressure_entity, flow_entity], start, end)
        p_rows = hist.get(pressure_entity) or []
        f_rows = hist.get(flow_entity) or []
        if len(p_rows) < 100:
            log.debug("[%s] %s: too little pressure history — night skipped",
                      circuit_cfg.circuit, night)
            return False

        n = int((end - start).total_seconds())
        pres = _ffill_1hz(p_rows, start, n)
        flow = _ffill_1hz(f_rows, start, n, default=0.0)
        spans = quiet_windows(flow)
        if not spans:
            log.debug("[%s] %s: no usable quiet window — night skipped",
                      circuit_cfg.circuit, night)
            return False
        s, e = max(spans, key=lambda x: x[1] - x[0])
        v = detect_pump_regime(pres[s:e], flow[s:e])

        # dev33 — SIBLING-CIRCUIT contamination gate. quiet_windows() only
        # screens THIS circuit's flow, so a draw on the other circuit leaves
        # the window looking quiet while its pressure signature is anything
        # but. The 2026-07-28 "110 L/day leak" was the 02:00 irrigation
        # program on circuit_2: main pressure declined for four hours while
        # the main meter (correctly) read 1.4 L. Gate on in-window flow
        # MINUTES, not "an event happened that night" — a 21-minute softener
        # regen does not explain a 240-minute window.
        contaminated_by = await self._sibling_flow_minutes(
            circuit_cfg.circuit, start + timedelta(seconds=int(s)),
            start + timedelta(seconds=int(e)))

        from .database import upsert_pump_regime_night
        # Store the ROBUST period (median inter-rise spacing) — the raw
        # autocorr peak locked onto a 62 s sub-harmonic on the first
        # production night while the actual rises were ~259 s apart.
        period = v.reported_period_s
        # Phase 5a: leak estimate on detected nights only, scaled by the
        # street-meter calibration (the home meter registers ~half of each
        # slug) so the stored/alerted number is the TRUE rate.
        est_lpd = None
        if v.detected and contaminated_by is None:
            est = estimate_leak_rate_lph(pres[s:e], flow[s:e])
            est_lpd = round(
                est["cycle_lph"] * 24.0 * PUMP_SLUG_CALIBRATION_FACTOR, 1)
        elif v.detected:
            log.info("[%s] %s: leak estimate suppressed — %s drew water inside "
                     "the analysis window (%.1f min)", circuit_cfg.circuit,
                     night, contaminated_by[0], contaminated_by[1])
        await run_db(
            upsert_pump_regime_night,
            self._db, circuit_cfg.circuit, night,
            detected=1 if v.detected else 0,
            period_s=period, amplitude_psi=v.p2p_psi, sd_psi=v.sd_psi,
            cycles=v.cycles, window_s=int(e - s), est_leak_lpd=est_lpd,
            # UTC bounds of the analyzed sub-window, so the leak-watch banner
            # can name the actual time range instead of "the night of".
            window_start_ts=(start + timedelta(seconds=int(s))).isoformat(),
            window_end_ts=(start + timedelta(seconds=int(e))).isoformat(),
            # Quiet-window pressure floor ≈ pump cut-in — feeds the Phase 6b
            # suggested-floor hint (works on healthy homes too: the quiet
            # minimum is the bottom of the normal band).
            min_psi=round(float(pres[s:e].min()), 2))
        log.info("[%s] pump-regime night %s: %s (period=%s cycles=%d "
                 "p2p=%.1f window=%dm)",
                 circuit_cfg.circuit, night,
                 "DETECTED" if v.detected else "quiet",
                 f"{period:.0f}s" if period else "-",
                 v.cycles, v.p2p_psi, (e - s) // 60)
        return True

    # ── home-level flag ──────────────────────────────────────────────────────

    def _apply_hysteresis(self) -> None:
        from .database import (get_home_profile, get_pump_regime_nights,
                               update_home_profile)
        prof = get_home_profile(self._db)
        if prof is None:
            return
        nights = get_pump_regime_nights(self._db, limit=40)
        currently = bool(prof["pump_mode_detected"])
        action = evaluate_hysteresis(nights, currently)
        now_iso = datetime.now(timezone.utc).isoformat()

        # Refresh the stored period on EVERY detected latest night, flag
        # state changes aside (plan finding #20).
        latest = nights[0] if nights else None
        updates: Dict[str, Any] = {}
        if latest and latest["any_detected"] and latest["period_s"]:
            updates["pump_detect_period_s"] = float(latest["period_s"])

        # Phase 6b arming stamp (persisted — recomputing from history would
        # silently disarm when the HA fidelity window ages out): evidence =
        # any detected night, or a post-feature pump supply answer.
        if not prof["pump_alert_armed_at"]:
            has_evidence = any(n["any_detected"] for n in nights)
            post_feature_answer = (
                prof["supply_type"] in ("city_pump", "well")
                and bool(prof["supply_type_set_at"]))
            if has_evidence or post_feature_answer:
                updates["pump_alert_armed_at"] = now_iso
                log.info("pump alerts ARMED (%s)",
                         "detected-night evidence" if has_evidence
                         else "post-feature supply answer")

        if action == "set":
            updates.update(pump_mode_detected=1, pump_mode_detected_at=now_iso)
            if not prof["pump_profile"]:
                # v1 detection can only identify the vfd signature; never
                # overwrite a user-set/previous profile.
                from .config import PUMP_PROFILE_VFD
                updates["pump_profile"] = PUMP_PROFILE_VFD
            log.info("pump-regime: home flag SET (>=%d of last %d evaluated "
                     "nights) — banner pending user confirmation",
                     _SET_REQUIRED, _SET_LOOKBACK)
        elif action == "clear":
            updates.update(pump_mode_detected=0, pump_mode_detected_at=now_iso,
                           pump_mode_ack=None)
            log.info("pump-regime: home flag CLEARED (%d consecutive "
                     "evaluated quiet nights); ack reset for future "
                     "re-detection", _CLEAR_QUIET_NIGHTS)
        if updates:
            update_home_profile(self._db, **updates)


def _ffill_1hz(rows, start_dt, n, default: float = 0.0):
    """HA history rows ({'state','last_changed'}) → 1 Hz forward-filled
    numpy array of length n starting at start_dt (UTC)."""
    import numpy as np
    t0 = start_dt.timestamp()
    grid = np.full(n, default, dtype=float)
    pts = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(
                str(r.get("last_changed")).replace("Z", "+00:00")).timestamp()
            pts.append((ts, float(r.get("state"))))
        except (ValueError, TypeError):
            continue
    pts.sort()
    last = default
    j = 0
    for i in range(n):
        while j < len(pts) and pts[j][0] - t0 <= i:
            last = pts[j][1]
            j += 1
        grid[i] = last
    return grid
