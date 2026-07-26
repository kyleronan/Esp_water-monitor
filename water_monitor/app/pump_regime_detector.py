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

log = logging.getLogger(__name__)

_STARTUP_DELAY_S = 240      # stagger past rise_corr_backfill's 180 s boot fetch
_WINDOW_HOURS = 3           # analysis window length starting at the quiet hour
_DEFAULT_QUIET_HOUR = 2     # local fallback when hourly_volume has no signal
_RECHECK_ON_ERROR_S = 3600  # retry delay after an HA fetch failure
_SET_LOOKBACK = 3           # hysteresis: >=2 detected of last 3 evaluated
_SET_REQUIRED = 2
_CLEAR_QUIET_NIGHTS = 7     # consecutive evaluated quiet nights to clear
_REBANNER_EVALUATED_NIGHTS = 30   # recurring re-banner cadence post-dismissal


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


class PumpRegimeDetector:
    """Supervised nightly worker. One pass per local day, shortly after the
    quiet-hour window ends; also runs a catch-up pass at boot when last
    night was never evaluated (deploys shouldn't wait a day)."""

    def __init__(self, db: sqlite3.Connection, cfg, ha, ha_tz=None):
        self._db = db
        self._cfg = cfg
        self._ha = ha
        self._ha_tz = ha_tz or timezone.utc
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
            delay = self._seconds_until_next_pass() if ran else _RECHECK_ON_ERROR_S
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
        qh = self._quiet_hour()
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
        night = self._last_night_date()
        ok = True
        wrote_any = False
        for circuit_cfg in self._cfg.circuits:
            circuit = circuit_cfg.circuit
            exists = self._db.execute(
                "SELECT 1 FROM pump_regime_nightly "
                "WHERE circuit = ? AND night_date = ?",
                (circuit, night)).fetchone()
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
            self._apply_hysteresis()
        return ok

    async def _analyze_circuit_night(self, circuit_cfg, night: str) -> bool:
        import numpy as np
        from .pump_regime_math import detect_pump_regime, quiet_windows

        pressure_entity = (getattr(circuit_cfg, "pressure_history_sensor", None)
                           or getattr(circuit_cfg, "pressure_avg_sensor", None))
        flow_entity = getattr(circuit_cfg, "flow_sensor", None)
        if not pressure_entity or not flow_entity:
            return False

        qh = self._quiet_hour()
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

        from .database import upsert_pump_regime_night
        # Store the ROBUST period (median inter-rise spacing) — the raw
        # autocorr peak locked onto a 62 s sub-harmonic on the first
        # production night while the actual rises were ~259 s apart.
        period = v.reported_period_s
        upsert_pump_regime_night(
            self._db, circuit_cfg.circuit, night,
            detected=1 if v.detected else 0,
            period_s=period, amplitude_psi=v.p2p_psi, sd_psi=v.sd_psi,
            cycles=v.cycles, window_s=int(e - s))
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
