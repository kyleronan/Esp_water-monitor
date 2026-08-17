"""Supply-pressure regime tracker.

Persists the home's idle-line (settled) pressure as daily medians and derives
discrete SUPPLY REGIMES from it — sustained pressure bands such as "city
~46 psi" vs "booster pump ~59 psi". A regime shift (pump installed/removed,
PRV swapped, municipal change) is detected with pump-detector-style hysteresis
and surfaced as a dashboard banner prompting a per-regime rule recalibration;
nothing adapts silently. This keeps the locked-baseline anti-drift philosophy
(thresholds are fitted once per regime, never continuously) while classification
stops degrading when the supply physics change — the 2026-07 ESYBOX install
shifted settled pressure 46→59 psi and pushed the unmatched share of eligible
events from 16% to 35% before this existed.

Design notes:
  * The sample source is CircuitEventDetector's settled-pressure baseline via
    EventDetector.settled_pressure() — in-memory, honest-None during events /
    sensor blips / cold start — so the tracker inherits its trust semantics.
  * A day with fewer than _MIN_DAY_SAMPLES samples is NOT evaluated; like the
    pump detector's skipped nights it is invisible to the shift hysteresis.
  * A regime shift is a NEW regime, not a flappable flag: if pressure returns
    to the old band, that's simply another shift (the 3-of-4 / 5 psi
    hysteresis prevents oscillation).
  * First run with an empty supply_regime table BOOTSTRAPS history from
    events.pre_event_pressure_psi daily medians and replays the evaluator, so
    a shift that already happened (e.g. the 2026-07-19 pump install) is
    reconstructed and bannered immediately after deploy.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

_STARTUP_DELAY_S = 180        # let the detectors warm their pressure buffers
_SAMPLE_INTERVAL_S = 600      # one settled-pressure sample every 10 min
_MIN_DAY_SAMPLES = 6          # fewer → day not evaluated (invisible to shifts)
_SHIFT_WINDOW_DAYS = 4        # hysteresis window over EVALUATED days
_SHIFT_REQUIRED = 3           # >=3 of the last 4 evaluated days must deviate
_SHIFT_DELTA_PSI = 5.0        # sustained |median - center| to count as deviating
_SETTLE_FIT_DAYS = 5          # evaluated days before a new regime's band is fit
_BOOTSTRAP_LOOKBACK_DAYS = 120


# ── pure math ─────────────────────────────────────────────────────────────────

def _median(vals: List[float]) -> float:
    s = sorted(vals)
    mid = len(s) // 2
    return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2.0


def _pctl(vals: List[float], frac: float) -> float:
    s = sorted(vals)
    idx = min(len(s) - 1, max(0, int(round(frac * (len(s) - 1)))))
    return s[idx]


def evaluate_regime_shift(days_newest_first: List[Dict[str, Any]],
                          center_psi: float) -> Optional[Dict[str, Any]]:
    """Shift decision from daily settled-pressure rows (newest first, each
    ``{'day_date', 'median_psi', 'sample_count'}``).

    Returns ``{'new_center', 'shift_day', 'deviating'}`` when >=_SHIFT_REQUIRED
    of the last _SHIFT_WINDOW_DAYS EVALUATED days deviate more than
    _SHIFT_DELTA_PSI from ``center_psi``, all in the same direction — else
    None. Unevaluated days (sample_count < _MIN_DAY_SAMPLES) are skipped
    entirely, mirroring the pump detector's evaluated-nights convention.
    """
    evaluated = [d for d in days_newest_first
                 if (d.get("sample_count") or 0) >= _MIN_DAY_SAMPLES]
    window = evaluated[:_SHIFT_WINDOW_DAYS]
    if len(window) < _SHIFT_REQUIRED:
        return None
    deviating = [d for d in window
                 if abs(float(d["median_psi"]) - center_psi) > _SHIFT_DELTA_PSI]
    if len(deviating) < _SHIFT_REQUIRED:
        return None
    signs = {1 if float(d["median_psi"]) > center_psi else -1 for d in deviating}
    if len(signs) != 1:
        return None
    medians = [float(d["median_psi"]) for d in deviating]
    return {
        "new_center": round(_median(medians), 1),
        "shift_day": min(d["day_date"] for d in deviating),
        "deviating": len(deviating),
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

def upsert_supply_pressure_day(db: sqlite3.Connection, circuit: str,
                               day_date: str, samples: List[float],
                               source: str = "settled") -> None:
    """Recompute one day's row from the full sample list (idempotent)."""
    if not samples:
        return
    db.execute(
        "INSERT INTO supply_pressure_daily "
        " (circuit, day_date, sample_count, median_psi, p10_psi, p90_psi, "
        "  source, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?) "
        "ON CONFLICT(circuit, day_date) DO UPDATE SET "
        "  sample_count=excluded.sample_count, median_psi=excluded.median_psi, "
        "  p10_psi=excluded.p10_psi, p90_psi=excluded.p90_psi, "
        "  source=excluded.source, updated_at=excluded.updated_at",
        (circuit, day_date, len(samples), round(_median(samples), 2),
         round(_pctl(samples, 0.10), 2), round(_pctl(samples, 0.90), 2),
         source, datetime.now(timezone.utc).isoformat()))
    db.commit()


def get_supply_days(db: sqlite3.Connection, circuit: str,
                    limit: int = 60) -> List[Dict[str, Any]]:
    """Daily rows, NEWEST first."""
    rows = db.execute(
        "SELECT day_date, sample_count, median_psi, p10_psi, p90_psi, source "
        "FROM supply_pressure_daily WHERE circuit = ? "
        "ORDER BY day_date DESC LIMIT ?", (circuit, limit)).fetchall()
    return [dict(zip(("day_date", "sample_count", "median_psi", "p10_psi",
                      "p90_psi", "source"), r)) for r in rows]


def get_regimes(db: sqlite3.Connection) -> List[Dict[str, Any]]:
    """All regimes ordered oldest→newest. Empty list on a pre-migration DB."""
    try:
        rows = db.execute(
            "SELECT id, started_at, ended_at, center_psi, band_lo_psi, "
            "       band_hi_psi, source, detected_at, confirmed_at, "
            "       dismissed_at, note "
            "FROM supply_regime ORDER BY started_at").fetchall()
    except sqlite3.OperationalError:
        return []
    keys = ("id", "started_at", "ended_at", "center_psi", "band_lo_psi",
            "band_hi_psi", "source", "detected_at", "confirmed_at",
            "dismissed_at", "note")
    return [dict(zip(keys, r)) for r in rows]


def get_current_regime(db: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    regimes = get_regimes(db)
    for r in reversed(regimes):
        if r["ended_at"] is None:
            return r
    return None


_PUMP_ERA_MIN_RISE_PSI: float = 5.0


def pump_era_start(db: sqlite3.Connection) -> Optional[str]:
    """UTC-ISO timestamp from which this home has run a booster pump, PINNED.

    The retroactive VFD-ripple exemption and any other pump-era-scoped sweep
    ask a HISTORICAL question ("did the pump exist when this event was
    captured?"), so they must not consult live pump-gate state — gates
    flipping off would silently re-flag every exempted event on the next
    reprocess. They also must not use the *current* regime: a later supply
    transition would shrink the era and produce exactly the same churn.

    Resolution, once, then stored in ``home_profile.pump_era_start``:

      1. the stored value (authoritative thereafter — regimes can be
         re-bootstrapped, merged or pruned; a pinned anchor cannot drift);
      2. else the LATEST 'detected' regime that opened with a >= 5 psi centre
         rise over its predecessor AND began before ``pump_mode_detected_at``
         — "latest preceding" so an unrelated earlier step (a PRV change, a
         municipal shift) can't pull the anchor back, while still beating
         detection lag (this home: install 2026-07-18, detection 2026-07-26 —
         8 days of ripple events the detected_at anchor would have missed);
      3. else ``home_profile.pump_mode_detected_at``;
      4. else None → callers never exempt/absorb anything.

    Logs which branch produced the value: if a home's regime table doesn't
    yield the step, the fallback is loud rather than silently narrower.
    """
    try:
        prof = db.execute(
            "SELECT pump_era_start, pump_mode_detected_at FROM home_profile "
            "WHERE id = 1").fetchone()
    except sqlite3.OperationalError:
        return None
    if prof is None:
        return None
    stored = prof["pump_era_start"] if "pump_era_start" in prof.keys() else None
    if stored:
        return str(stored)

    detected_at = prof["pump_mode_detected_at"]
    resolved, branch = None, "none"
    regimes = get_regimes(db)
    for prev, cur in zip(regimes, regimes[1:]):
        if cur.get("source") != "detected":
            continue
        if float(cur["center_psi"] or 0) - float(prev["center_psi"] or 0) \
                < _PUMP_ERA_MIN_RISE_PSI:
            continue
        if detected_at and cur["started_at"] > str(detected_at):
            continue          # after detection → not the install step
        resolved, branch = cur["started_at"], "regime_step"
    if resolved is None and detected_at:
        resolved, branch = str(detected_at), "detected_at_fallback"

    if resolved:
        try:
            db.execute("UPDATE home_profile SET pump_era_start = ? WHERE id = 1",
                       (resolved,))
            db.commit()
        except sqlite3.OperationalError:
            pass              # pre-migration DB — resolve fresh next time
    log.info("supply-regime: pump era start resolved to %s (%s)",
             resolved or "none", branch)
    return resolved


_RECENTER_MIN_DAYS: int = 5
_RECENTER_NOTE_PREFIX: str = "recentered"


def recenter_current_regime(db: sqlite3.Connection, circuit: str, tz,
                            *, reason: str = "post-repair") -> Optional[float]:
    """Re-fit the CURRENT regime's centre from its recent settled pressure.

    Why this exists (dev33): a regime's centre is settle-fit from its first
    days, so a regime opened while a plumbing defect was active inherits the
    defect. This home's regime 2 was fitted across 7/19-7/26, when the supply
    was sawtoothing 58 -> 54.4 psi on a leak-driven pump cycle: its centre
    (54.3) is an artifact of the leak, and when the valve was repaired the
    idle pressure settled at its TRUE value ~59-60 — more than the 5 psi shift
    threshold above the artifact centre. Left alone the tracker would open a
    regime 3 and encode *a valve repair* as a supply change, splitting the
    per-regime rule-fit pools days after they were refit.

    Uses the MEDIAN of recent daily medians: the thermal ratchet puts ~5% of
    idle time above 65 psi with +11 psi excursions, and a mean would chase
    that tail. Do not "improve" this to a mean.

    The band WIDTH is carried across (translated), not refitted: a width fit
    from the handful of post-repair days would be tight enough for ordinary
    seasonal drift to re-trigger a shift.

    Returns the new centre, or None when nothing was changed.
    """
    current = get_current_regime(db)
    if current is None:
        return None
    days = [d for d in get_supply_days(db, circuit, limit=30)
            if (d["sample_count"] or 0) >= _MIN_DAY_SAMPLES]
    if len(days) < _RECENTER_MIN_DAYS:
        return None
    medians = [float(d["median_psi"]) for d in days[:_RECENTER_MIN_DAYS * 2]]
    new_center = round(_median(medians), 1)
    old_center = float(current["center_psi"] or 0.0)
    if abs(new_center - old_center) < _SHIFT_DELTA_PSI:
        return None                       # nothing meaningful to recentre
    lo, hi = current.get("band_lo_psi"), current.get("band_hi_psi")
    shift = new_center - old_center
    new_lo = round(float(lo) + shift, 1) if lo is not None else None
    new_hi = round(float(hi) + shift, 1) if hi is not None else None
    note = (f"{_RECENTER_NOTE_PREFIX} {old_center}->{new_center} psi "
            f"({reason}); band width carried")
    db.execute(
        "UPDATE supply_regime SET center_psi = ?, band_lo_psi = ?, "
        " band_hi_psi = ?, note = ? WHERE id = ?",
        (new_center, new_lo, new_hi, note, current["id"]))
    db.commit()
    log.info("supply-regime: regime %s recentred %.1f -> %.1f psi (%s)",
             current["id"], old_center, new_center, reason)
    return new_center


def merge_spurious_regime(db: sqlite3.Connection) -> bool:
    """Undo a regime opened by the recenter step itself.

    If the tracker fires before ``recenter_current_regime`` lands, the recovery
    is to merge that regime back rather than adopt it: close it, re-open the
    predecessor at the merged centre. Rule fits keep targeting the merged
    regime. Genuine later shifts (a pump setpoint change, a pump removal) still
    open regimes normally against the recentred value.

    Returns True when a merge happened.
    """
    regimes = get_regimes(db)
    if len(regimes) < 2:
        return False
    current, prev = regimes[-1], regimes[-2]
    if current["ended_at"] is not None or current["source"] != "detected":
        return False
    if current.get("confirmed_at"):
        return False              # the user accepted it — leave it alone
    merged_center = round((float(current["center_psi"] or 0.0)
                           + float(prev["center_psi"] or 0.0)) / 2.0, 1)
    db.execute("DELETE FROM supply_regime WHERE id = ?", (current["id"],))
    db.execute(
        "UPDATE supply_regime SET ended_at = NULL, center_psi = ?, note = ? "
        "WHERE id = ?",
        (merged_center, f"{_RECENTER_NOTE_PREFIX}: merged regime "
                        f"{current['id']} back (recenter artifact)", prev["id"]))
    db.commit()
    log.info("supply-regime: merged spurious regime %s back into %s "
             "(centre now %.1f psi)", current["id"], prev["id"], merged_center)
    return True


def get_current_regime_id(db: sqlite3.Connection) -> int:
    """Id of the open (current) regime, or 0 — the legacy/pre-regime id — when
    none exists or the table predates migration 20260564. Cheap single-row
    read, safe on the live per-event path."""
    try:
        row = db.execute(
            "SELECT id FROM supply_regime WHERE ended_at IS NULL "
            "ORDER BY started_at DESC LIMIT 1").fetchone()
    except sqlite3.OperationalError:
        return 0
    return int(row[0]) if row else 0


def resolve_regime_for_ts(regimes: List[Dict[str, Any]],
                          ts: Optional[str]) -> int:
    """Regime id covering UTC-ISO timestamp ``ts`` (interval
    [started_at, ended_at)), or 0 — the legacy/pre-regime id — when no regime
    covers it. ``regimes`` is get_regimes() output (oldest→newest)."""
    if not ts:
        return 0
    for r in reversed(regimes):
        if ts >= r["started_at"] and (r["ended_at"] is None
                                      or ts < r["ended_at"]):
            return int(r["id"])
    return 0


def _open_regime(db: sqlite3.Connection, started_at: str, center_psi: float,
                 source: str, note: Optional[str] = None,
                 detected: bool = False) -> int:
    cur = db.execute(
        "INSERT INTO supply_regime (started_at, center_psi, source, "
        " detected_at, note) VALUES (?,?,?,?,?)",
        (started_at, round(center_psi, 1), source,
         datetime.now(timezone.utc).isoformat() if detected else None, note))
    db.commit()
    return int(cur.lastrowid)


def _close_regime(db: sqlite3.Connection, regime_id: int,
                  ended_at: str) -> None:
    db.execute("UPDATE supply_regime SET ended_at = ? WHERE id = ?",
               (ended_at, regime_id))
    db.commit()


def refit_regime_band(db: sqlite3.Connection, circuit: str,
                      regime: Dict[str, Any], tz) -> bool:
    """Fit center/band from the first _SETTLE_FIT_DAYS evaluated days inside a
    still-unfitted regime (band_lo_psi IS NULL). Returns True when fitted."""
    if regime.get("band_lo_psi") is not None:
        return False
    start_day = _utc_iso_to_local_day(regime["started_at"], tz)
    rows = [d for d in reversed(get_supply_days(db, circuit, limit=400))
            if d["day_date"] >= start_day
            and (d["sample_count"] or 0) >= _MIN_DAY_SAMPLES]
    if len(rows) < _SETTLE_FIT_DAYS:
        return False
    fit = rows[:_SETTLE_FIT_DAYS]
    medians = [float(d["median_psi"]) for d in fit]
    db.execute(
        "UPDATE supply_regime SET center_psi = ?, band_lo_psi = ?, "
        " band_hi_psi = ? WHERE id = ?",
        (round(_median(medians), 1),
         round(min(float(d["p10_psi"] or d["median_psi"]) for d in fit), 1),
         round(max(float(d["p90_psi"] or d["median_psi"]) for d in fit), 1),
         regime["id"]))
    db.commit()
    return True


# ── banner / labeling nudge ───────────────────────────────────────────────────

def post_regime_label_counts(db: sqlite3.Connection, circuit: str,
                             since_ts: str) -> Dict[str, int]:
    """Explicit (user/training) label counts per canonical type since a regime
    started — the per-type fit fuel gauge for the recalibration nudge."""
    rows = db.execute(
        "SELECT user_fixture_type, COUNT(*) FROM events "
        "WHERE circuit = ? AND start_ts >= ? "
        "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
        "  AND fixture_label_source IN ('user', 'training') "
        "  AND COALESCE(excluded_from_training, 0) = 0 "
        "GROUP BY user_fixture_type", (circuit, since_ts)).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def regime_labels_needed(db: sqlite3.Connection, circuit: str,
                         regime: Dict[str, Any]) -> str:
    """Human-readable list of rule-fitted fixture types still short of the
    per-regime fit gate ('tap' isn't rule-fitted; irrigation is zone-only) —
    empty string when every fittable type has enough post-shift labels."""
    from .rule_calibration import _FITTERS, MIN_EXPLICIT_LABELS
    counts = post_regime_label_counts(db, circuit, regime["started_at"])
    short = []
    for ftype in sorted(_FITTERS):
        if ftype == "irrigation_zone":
            continue
        have = counts.get(ftype, 0)
        if have < MIN_EXPLICIT_LABELS:
            pretty = ftype.replace("_", " ")
            short.append(f"{pretty} ({have}/{MIN_EXPLICIT_LABELS})")
    return ", ".join(short)


def supply_banner_state(db: sqlite3.Connection,
                        circuit: Optional[str] = None) -> Dict[str, Any]:
    """Whether the 'supply pressure changed — recalibrate?' banner shows, plus
    copy inputs. Shows while the CURRENT regime was auto-detected and the user
    has neither confirmed (recalibrated) nor dismissed it. With a ``circuit``,
    also reports which fixture types still need post-shift labels."""
    regimes = get_regimes(db)
    current = next((r for r in reversed(regimes) if r["ended_at"] is None), None)
    if (current is None or current["source"] != "detected"
            or current["confirmed_at"] or current["dismissed_at"]):
        return {"show": False}
    prev = next((r for r in reversed(regimes)
                 if r["ended_at"] is not None), None)
    state = {
        "show": True,
        "regime_id": current["id"],
        "old_psi": round(prev["center_psi"]) if prev else None,
        "new_psi": round(current["center_psi"]),
        "since": current["started_at"][:10],
    }
    if circuit:
        try:
            state["labels_needed"] = regime_labels_needed(db, circuit, current)
        except Exception:
            state["labels_needed"] = ""
    return state


# ── local-day helpers ─────────────────────────────────────────────────────────

def _utc_iso_to_local_day(ts: str, tz) -> str:
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).date().isoformat()
    except (ValueError, TypeError):
        return str(ts)[:10]


def _local_day_to_utc_iso(day_date: str, tz) -> str:
    try:
        local_midnight = datetime.fromisoformat(day_date).replace(tzinfo=tz)
        return local_midnight.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return f"{day_date}T00:00:00+00:00"


# ── bootstrap ─────────────────────────────────────────────────────────────────

def bootstrap_from_events(db: sqlite3.Connection, circuit: str, tz) -> int:
    """First-run history reconstruction: daily medians of
    events.pre_event_pressure_psi (usage-biased but the only persisted record)
    over the last _BOOTSTRAP_LOOKBACK_DAYS, then a chronological replay of the
    shift evaluator. Creates the initial regime (source 'bootstrap') plus one
    'detected' regime per reconstructed shift — the newest of which banners.
    No-op unless the supply_regime table is empty. Returns regimes created."""
    if get_regimes(db):
        return 0
    since = (datetime.now(timezone.utc)
             - timedelta(days=_BOOTSTRAP_LOOKBACK_DAYS)).isoformat()
    rows = db.execute(
        "SELECT start_ts, pre_event_pressure_psi FROM events "
        "WHERE circuit = ? AND pre_event_pressure_psi >= 5.0 "
        "  AND start_ts >= ? ORDER BY start_ts", (circuit, since)).fetchall()
    by_day: Dict[str, List[float]] = {}
    for ts, psi in rows:
        by_day.setdefault(_utc_iso_to_local_day(ts, tz), []).append(float(psi))
    for day, samples in by_day.items():
        # Never clobber a real settled-sample row with the cruder event proxy.
        exists = db.execute(
            "SELECT source FROM supply_pressure_daily "
            "WHERE circuit = ? AND day_date = ?", (circuit, day)).fetchone()
        if exists and exists[0] == "settled":
            continue
        upsert_supply_pressure_day(db, circuit, day, samples,
                                   source="event_backfill")

    days_old_to_new = [d for d in reversed(get_supply_days(db, circuit, 400))
                       if (d["sample_count"] or 0) >= _MIN_DAY_SAMPLES]
    if len(days_old_to_new) < _SHIFT_REQUIRED:
        return 0

    # Initial regime from the earliest settle window.
    seed = days_old_to_new[:_SETTLE_FIT_DAYS]
    center = _median([float(d["median_psi"]) for d in seed])
    _open_regime(db, _local_day_to_utc_iso(seed[0]["day_date"], tz), center,
                 source="bootstrap", note="bootstrapped from event history")
    created = 1

    # Chronological replay of the nightly evaluation step.
    for i in range(len(days_old_to_new)):
        newest_first = list(reversed(days_old_to_new[:i + 1]))
        current = get_current_regime(db)
        if current is None:
            break
        verdict = evaluate_regime_shift(newest_first, current["center_psi"])
        if verdict is not None:
            shift_at = _local_day_to_utc_iso(verdict["shift_day"], tz)
            _close_regime(db, current["id"], shift_at)
            _open_regime(db, shift_at, verdict["new_center"],
                         source="detected", detected=True,
                         note="reconstructed by bootstrap replay")
            created += 1
        else:
            current = get_current_regime(db)
            if current is not None:
                refit_regime_band(db, circuit, current, tz)
    # Final refit chance for the last regime.
    current = get_current_regime(db)
    if current is not None:
        refit_regime_band(db, circuit, current, tz)
    log.info("supply-regime bootstrap: %d regime(s) reconstructed from %d "
             "evaluated day(s)", created, len(days_old_to_new))
    return created


# ── worker ────────────────────────────────────────────────────────────────────

class SupplyRegimeTracker:
    """Supervised worker: samples the primary circuit's settled pressure every
    _SAMPLE_INTERVAL_S, maintains the day's supply_pressure_daily row, and on
    local-day rollover runs the shift evaluator (+ band settle-fit). A restart
    loses the in-memory partial-day bucket; the row already written stands and
    the day resumes accumulating — acceptable by design."""

    def __init__(self, db: sqlite3.Connection, cfg,
                 settled_getter: Callable[[str], Optional[Tuple[float, datetime]]],
                 ha_tz=None, alert_manager=None):
        self._db = db
        self._cfg = cfg
        self._settled_getter = settled_getter
        self._ha_tz = ha_tz or timezone.utc
        self._alert_manager = alert_manager
        self._stop = asyncio.Event()
        self._bucket_day: Optional[str] = None
        self._bucket: List[float] = []

    def stop(self) -> None:
        self._stop.set()

    def set_timezone(self, tz) -> None:
        self._ha_tz = tz

    def _primary_circuit(self) -> Optional[str]:
        circuits = [c.circuit for c in self._cfg.circuits]
        return circuits[0] if circuits else None

    def _bootstrap_and_banner_sync(self, circuit: str):
        """dev46 (46a) — reconstruct a missed shift, then read the banner."""
        created = bootstrap_from_events(self._db, circuit, self._ha_tz)
        banner = supply_banner_state(self._db) if created else {}
        return created, banner

    def _recentre_sync(self, circuit: str) -> None:
        """dev46 (46a) — the dev33 one-shot recentre pair, in order."""
        merge_spurious_regime(self._db)
        recenter_current_regime(self._db, circuit, self._ha_tz)

    async def run(self) -> None:
        circuit = self._primary_circuit()
        if circuit is None:
            await self._stop.wait()
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=_STARTUP_DELAY_S)
            return
        except asyncio.TimeoutError:
            pass
        try:
            # dev46 (46a): bootstrap + banner read are adjacent — one hop.
            from .database import run_db
            created, banner = await run_db(
                self._bootstrap_and_banner_sync, circuit)
            if created:
                if banner.get("show"):
                    log.info("supply-regime: reconstructed shift pending user "
                             "confirmation (%s→%s psi around %s)",
                             banner.get("old_psi"), banner.get("new_psi"),
                             banner.get("since"))
        except Exception as e:
            log.warning("supply-regime bootstrap failed (non-fatal): %s", e)

        # dev33 one-shot: a regime whose centre was settle-fit while a plumbing
        # defect was active carries that defect. Recentre it (and merge back a
        # regime the recentre step itself provoked) BEFORE the sampling loop can
        # evaluate a shift against the stale centre. Both helpers are no-ops
        # once applied, and both are guarded per their own preconditions.
        try:
            # One hop: both are guarded one-shots that must run in order.
            await run_db(self._recentre_sync, circuit)
        except Exception as e:
            log.warning("supply-regime recentre failed (non-fatal): %s", e)

        while not self._stop.is_set():
            try:
                await run_db(self._sample, circuit)
            except Exception as e:
                log.warning("supply-regime sample failed (non-fatal): %s", e)
                if "locked" in str(e).lower():
                    from .database import note_locked_write
                    note_locked_write("supply_regime.sampler")
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       timeout=_SAMPLE_INTERVAL_S)
                return
            except asyncio.TimeoutError:
                continue

    # ── sampling / rollover ──────────────────────────────────────────────────

    def _sample(self, circuit: str) -> None:
        today = datetime.now(self._ha_tz).date().isoformat()
        if self._bucket_day is not None and today != self._bucket_day:
            self._evaluate(circuit)
            self._bucket = []
        self._bucket_day = today
        reading = self._settled_getter(circuit)
        if reading is None:
            return
        psi, _since = reading
        if psi < 5.0:
            return
        self._bucket.append(float(psi))
        upsert_supply_pressure_day(self._db, circuit, today, self._bucket)

    def _evaluate(self, circuit: str) -> None:
        """Day-rollover step: shift detection + settle-fit + alert."""
        current = get_current_regime(self._db)
        days = get_supply_days(self._db, circuit, limit=30)
        if current is None:
            # No bootstrap material existed at startup; open the first regime
            # once enough evaluated live days accumulate.
            evaluated = [d for d in days
                         if (d["sample_count"] or 0) >= _MIN_DAY_SAMPLES]
            if len(evaluated) >= _SETTLE_FIT_DAYS:
                seed = evaluated[:_SETTLE_FIT_DAYS]
                center = _median([float(d["median_psi"]) for d in seed])
                _open_regime(self._db,
                             _local_day_to_utc_iso(min(d["day_date"]
                                                       for d in seed),
                                                   self._ha_tz),
                             center, source="bootstrap",
                             note="first live regime")
                log.info("supply-regime: initial regime opened at %.1f psi",
                         center)
            return
        verdict = evaluate_regime_shift(days, current["center_psi"])
        if verdict is not None:
            shift_at = _local_day_to_utc_iso(verdict["shift_day"], self._ha_tz)
            _close_regime(self._db, current["id"], shift_at)
            _open_regime(self._db, shift_at, verdict["new_center"],
                         source="detected", detected=True)
            log.info("supply-regime: SHIFT detected %.1f→%.1f psi (since %s) — "
                     "banner pending user confirmation",
                     current["center_psi"], verdict["new_center"],
                     verdict["shift_day"])
            if self._alert_manager is not None:
                try:
                    asyncio.get_running_loop().create_task(
                        self._alert_manager.alert_supply_regime_shift(
                            circuit, current["center_psi"],
                            verdict["new_center"]))
                except RuntimeError:
                    pass   # no loop (sync tests) — banner still shows

        else:
            refit_regime_band(self._db, circuit, current, self._ha_tz)
