"""Phase 3 §2 — recorder volume reconciliation ("recorder as truth").

The live event detector integrates the HA flow-sensor samples for an event's volume — the
same at-most-once stream that drops samples on a WS glitch / addon restart, so a live event
can permanently UNDER-count. The firmware also publishes a CUMULATIVE volume integration
sensor whose recorder DELTA over the event window is authoritative: a cumulative delta only
depends on the two endpoints, so dropped INTERMEDIATE samples don't corrupt it. This module
reconciles each settled live event's volume against that delta.

Run hourly via MaturityRecheck, over HEALTHY events in a settled window. Auto-correct
(recorder wins) by default; a per-circuit toggle switches to flag-only.

SAFETY — the recorder shares the very lossiness this phase fights, so a recorder gap AT a
window boundary makes ``b − a`` span the wrong interval (plausibly-wrong, not absurd, so a
magnitude guard misses it). The **endpoint-gap guard** requires the recorder samples to
bracket the event edges within ``ENDPOINT_TOL_S``; insufficient coverage → decline (never
correct — declining beats overwriting with a gap-computed value, especially on the
unreviewed auto path). Every write routes through ``apply_effective_volume`` (the §2.5
ledger chokepoint); compromised-flow verdicts (phantom/cross-talk/dribble/degraded/excluded)
are never reconciled; user verdicts are preserved.
"""
from __future__ import annotations

import logging
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

SETTLE_HORIZON_H: float = 6.0      # never look back past this (matches MaturityRecheck)
SETTLE_LAG_S: float = 20 * 60      # recorder catch-up grace — only reconcile events this old
ENDPOINT_TOL_S: float = 120.0      # recorder samples must bracket the event edges within this
RECONCILE_MIN_L: float = 0.5       # absolute divergence floor
RECONCILE_FRAC: float = 0.20       # relative divergence floor
_FETCH_MARGIN_S: float = 60.0      # widen the HA fetch a touch past the window


def _to_utc(ts: Any) -> Optional[datetime]:
    """ISO/datetime → aware UTC datetime (naive ⇒ UTC), or None."""
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return d.astimezone(timezone.utc) if d.tzinfo else d.replace(tzinfo=timezone.utc)


def firmware_volume_delta(volume_hist: Any, start: datetime, end: datetime,
                          vol_unit: str) -> Optional[Tuple[float, datetime, datetime]]:
    """Cumulative firmware-volume-sensor delta over ``[start, end]`` →
    ``(litres, a_ts, b_ts)`` or None.

    Robust to dropped INTERMEDIATE samples (only the two endpoints matter). Rejects resets
    / absurd values (``0 < raw_delta < 10_000``). ``a_ts`` / ``b_ts`` are the in-window
    first/last sample times, returned so the caller can apply the endpoint-gap guard. The
    historical importer reuses this and consumes ONLY the litres → its behaviour is
    unchanged.
    """
    pairs: List[Tuple[datetime, float]] = []
    for e in volume_hist or ():
        ts = _to_utc(e.get("last_changed"))
        if ts is None or not (start <= ts <= end):
            continue
        try:
            v = float(e.get("state"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(v):
            pairs.append((ts, v))
    if len(pairs) < 2:
        return None
    pairs.sort(key=lambda p: p[0])
    raw_delta = pairs[-1][1] - pairs[0][1]
    if not (0 < raw_delta < 10_000):     # reject resets + absurd magnitudes
        return None
    from .ha_client import vol_to_litres
    return (round(vol_to_litres(raw_delta, vol_unit), 3), pairs[0][0], pairs[-1][0])


def is_significant_divergence(recorder: float, stored: float) -> bool:
    d = abs(recorder - stored)
    return d > RECONCILE_MIN_L and d > RECONCILE_FRAC * max(recorder, stored, 1e-9)


# Verdicts that mark an event's flow signal / volume as special — never reconciled.
_COMPROMISED_SQL = (
    "COALESCE(is_pressure_restoration_phantom,0)=0 AND COALESCE(is_cross_talk,0)=0 "
    "AND COALESCE(is_low_flow_dribble,0)=0 AND COALESCE(degraded_supply,0)=0 "
    "AND COALESCE(excluded_from_training,0)=0"
)


async def reconcile_circuit_volumes(db_path: str, ha: Any, cfg: Any, circuit: str,
                                    *, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Reconcile settled HEALTHY events' volume against the firmware recorder delta.

    Async (fetches HA history); the per-event corrections run under the write lock. Returns
    ``{scanned, corrections, flagged, skipped?}``."""
    circuit_cfg = cfg.get_circuit(circuit) if cfg else None
    if circuit_cfg is None or not getattr(circuit_cfg, "volume_sensor", ""):
        return {"skipped": "no_volume_sensor"}
    now = now or datetime.now(timezone.utc)
    win_end = now - timedelta(seconds=SETTLE_LAG_S)
    floor = now - timedelta(hours=SETTLE_HORIZON_H)

    # 1. Read the checkpoint + the settled-window events + the auto toggle (short read).
    from .database import get_reconcile_state, get_sensitivity_config
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        st = get_reconcile_state(conn, circuit)
        through = (_to_utc(st["through_ts"]) if st and st["through_ts"] else None) or floor
        win_start = max(through, floor)
        if win_end <= win_start:
            return {"skipped": "empty_window"}
        events = conn.execute(
            "SELECT id, start_ts, end_ts, COALESCE(volume_litres,0) AS vol "
            "FROM events WHERE circuit = ? AND end_ts > ? AND end_ts <= ? "
            "  AND " + _COMPROMISED_SQL + " ORDER BY end_ts",
            (circuit, win_start.isoformat(), win_end.isoformat()),
        ).fetchall()
        sens = get_sensitivity_config(conn, circuit)
        auto = bool(sens["recorder_reconcile_auto"]) if (
            sens and "recorder_reconcile_auto" in sens.keys()
            and sens["recorder_reconcile_auto"] is not None) else True
    finally:
        conn.close()

    # 2. Fetch the firmware volume-sensor recorder history covering the window.
    margin = timedelta(seconds=_FETCH_MARGIN_S)
    try:
        hist = await ha.get_history(circuit_cfg.volume_sensor,
                                    win_start - margin, win_end + margin)
        vs = await ha.get_state(circuit_cfg.volume_sensor)
        vol_unit = (vs.get("attributes") or {}).get("unit_of_measurement", "") if vs else ""
    except Exception as e:
        log.warning("[%s] recorder-reconcile history fetch failed: %s", circuit, e)
        return {"skipped": "history_fetch_failed"}

    # 3. Decide per event (pure). Each → (id, start_ts, recorder_litres, stored, significant).
    decisions: List[Tuple[str, str, float, float, bool]] = []
    for e in events:
        a, b = _to_utc(e["start_ts"]), _to_utc(e["end_ts"])
        if not (a and b and b > a):
            continue
        vd = firmware_volume_delta(hist, a, b, vol_unit)
        if vd is None:
            continue
        rec_litres, a_ts, b_ts = vd
        # ── ENDPOINT-GAP GUARD — the recorder samples must bracket the event edges,
        # else a boundary gap made the delta span the wrong interval → decline.
        if (a_ts - a).total_seconds() > ENDPOINT_TOL_S \
                or (b - b_ts).total_seconds() > ENDPOINT_TOL_S:
            continue
        stored = float(e["vol"] or 0.0)
        decisions.append((e["id"], e["start_ts"], rec_litres, stored,
                          is_significant_divergence(rec_litres, stored)))

    # 4. Apply under the write lock: store the recorder value always; correct (auto) or
    #    just count (flag) the significant divergences; advance the checkpoint.
    from .database import (run_isolated_write, apply_effective_volume,
                           set_reconcile_state, start_job, finish_job)
    win_end_iso = win_end.isoformat()
    name = getattr(circuit_cfg, "label", circuit) or circuit

    def _apply(c: sqlite3.Connection):
        corrections = flagged = 0
        last_delta: Optional[float] = None
        for eid, start_ts, rec_litres, stored, sig in decisions:
            c.execute("UPDATE events SET volume_recorder_litres = ? WHERE id = ?",
                      (rec_litres, eid))
            if not sig:
                continue
            if auto:
                row = c.execute("SELECT volume_litres_original FROM events WHERE id = ?",
                                (eid,)).fetchone()
                if row is not None and row["volume_litres_original"] is None:
                    c.execute("UPDATE events SET volume_litres_original = ? WHERE id = ?",
                              (stored, eid))
                # Healthy event ⇒ effective == raw; write both, then route the ledger
                # through the §2.5 chokepoint with the SAME value (its contract).
                c.execute("UPDATE events SET volume_litres = ?, "
                          "volume_litres_effective = ?, volume_recomputed_at = ? "
                          "WHERE id = ?",
                          (rec_litres, rec_litres,
                           datetime.now(timezone.utc).isoformat(), eid))
                apply_effective_volume(c, eid, circuit, start_ts, rec_litres)
                corrections += 1
                last_delta = round(rec_litres - stored, 3)
            else:
                flagged += 1
        set_reconcile_state(c, circuit, through_ts=win_end_iso,
                            corrections_delta=corrections, flagged_delta=flagged,
                            last_delta_litres=last_delta)
        if corrections:
            jid = start_job(c, "reconcile", circuit, "Reconciling volumes…")
            finish_job(c, jid, "done",
                       f"{name}: corrected {corrections} event volume(s) from recorder")
        return corrections, flagged

    corrections, flagged = await run_isolated_write(db_path, _apply)
    if corrections or flagged:
        log.info("[%s] recorder-reconcile: scanned %d, corrected %d, flagged %d",
                 circuit, len(events), corrections, flagged)
    return {"scanned": len(events), "corrections": corrections, "flagged": flagged}


def count_flagged_backlog(conn: sqlite3.Connection, circuit: str) -> int:
    """How many healthy events have a stored recorder value that still diverges from the
    stored volume — i.e. how many ``apply_flagged_backlog`` would correct. Drives whether
    the device-page "Apply recorder corrections now" button is shown (read-only)."""
    rows = conn.execute(
        "SELECT COALESCE(volume_litres,0) AS vol, volume_recorder_litres rec "
        "FROM events WHERE circuit = ? AND volume_recorder_litres IS NOT NULL AND "
        + _COMPROMISED_SQL,
        (circuit,),
    ).fetchall()
    return sum(1 for r in rows
               if is_significant_divergence(float(r["rec"]), float(r["vol"] or 0.0)))


def apply_flagged_backlog(conn: sqlite3.Connection, circuit: str) -> Dict[str, int]:
    """Flag-mode review→apply: correct the ENTIRE backlog of healthy events that already
    have a stored ``volume_recorder_litres`` and still diverge significantly — directly
    from the stored value (no HA re-fetch, no window limit). Runs under the caller's write
    lock. Returns ``{"applied": n}``."""
    from .database import apply_effective_volume, set_reconcile_state
    rows = conn.execute(
        "SELECT id, start_ts, COALESCE(volume_litres,0) AS vol, volume_recorder_litres rec, "
        "       volume_litres_original orig "
        "FROM events WHERE circuit = ? AND volume_recorder_litres IS NOT NULL AND "
        + _COMPROMISED_SQL,
        (circuit,),
    ).fetchall()
    applied = 0
    for r in rows:
        rec = float(r["rec"])
        stored = float(r["vol"] or 0.0)
        if not is_significant_divergence(rec, stored):
            continue
        if r["orig"] is None:
            conn.execute("UPDATE events SET volume_litres_original = ? WHERE id = ?",
                         (stored, r["id"]))
        # Healthy event ⇒ effective == raw; keep both in step with the ledger value.
        conn.execute("UPDATE events SET volume_litres = ?, volume_litres_effective = ?, "
                     "volume_recomputed_at = ? WHERE id = ?",
                     (rec, rec, datetime.now(timezone.utc).isoformat(), r["id"]))
        apply_effective_volume(conn, r["id"], circuit, r["start_ts"], rec)
        applied += 1
    if applied:
        set_reconcile_state(conn, circuit, corrections_delta=applied)
    conn.commit()
    return {"applied": applied}
