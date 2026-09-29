"""Per-circuit sparkline size tiers, fitted to each home's own peak flows.

The History sparkline's height is a peak-flow tier (classify_magnitude_tier),
and fixed gpm bounds only suit the home they were measured on. So each circuit
refits its own fifths once a day: real events only (not-real excluded), from
the current supply regime (a pump or regulator change refits from scratch),
capped at the last FIT_WINDOW_DAYS. A circuit starts on its type's default,
blends toward its own fifths from FIT_MIN_EVENTS, and is fully its own from
FIT_FULL_EVENTS. Derived state — never backed up, recomputed after a restore.
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Tuple

from .database import _NOT_REAL_SQL, get_circuit_type

log = logging.getLogger(__name__)

GPM_TO_LPM = 3.78541
# Starting cuts per circuit type, gpm: this home's measured fifths (2026-09-28
# export) — main line since the booster pump, and its irrigation runs.
DEFAULT_CUTS_GPM = {
    "fixture": (1.3, 1.85, 2.25, 3.0),
    "zone":    (8.0, 10.4, 12.2, 13.0),
}
FIT_MIN_EVENTS = 50      # start blending toward the circuit's own fifths
FIT_FULL_EVENTS = 250    # ~50 per tier: fully the circuit's own
FIT_WINDOW_DAYS = 90
ROUND_GPM = 0.05
MOVE_TOLERANCE = 0.05    # replace stored cuts only when one moves > 5%

Cuts = Tuple[float, float, float, float]


def default_cuts_lpm(circuit_type: Optional[str]) -> Cuts:
    gpm = DEFAULT_CUTS_GPM.get(circuit_type or "", DEFAULT_CUTS_GPM["fixture"])
    return tuple(g * GPM_TO_LPM for g in gpm)


def fit_cuts(peaks_lpm: Iterable, default_lpm: Cuts) -> Tuple[Cuts, float, int]:
    """Blend the sample's fifths toward ``default_lpm`` by sample size.

    Returns (cuts L/min, weight 0–1 given to the sample, usable n). Below
    FIT_MIN_EVENTS the default stands; the weight is n / FIT_FULL_EVENTS, so
    50 events move the cuts a fifth of the way. Cuts are rounded to ROUND_GPM
    and kept strictly ascending.
    """
    vals = sorted(float(v) for v in peaks_lpm
                  if v is not None and math.isfinite(float(v)) and float(v) > 0)
    n = len(vals)
    if n < FIT_MIN_EVENTS:
        return tuple(default_lpm), 0.0, n
    w = min(1.0, n / FIT_FULL_EVENTS)
    fifths = [vals[min(n - 1, int(f * n))] for f in (0.2, 0.4, 0.6, 0.8)]
    step = ROUND_GPM * GPM_TO_LPM
    out: list = []
    for d, x in zip(default_lpm, fifths):
        c = round(round((w * x + (1.0 - w) * d) / step) * step, 4)
        if out and c <= out[-1]:          # rounding tie — keep tiers distinct
            c = round(out[-1] + step, 4)
        out.append(c)
    return tuple(out), round(w, 3), n


def moved_enough(old: Optional[Cuts], new: Cuts) -> bool:
    """True when any cut moved more than MOVE_TOLERANCE — the hysteresis that
    stops sparkline heights shifting a pixel every day."""
    if not old or len(old) != len(new):
        return True
    return any(abs(b - a) > MOVE_TOLERANCE * a for a, b in zip(old, new))


def _parse_utc(ts) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(ts))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def window_start(conn: sqlite3.Connection, now: datetime) -> str:
    """UTC-ISO start of the fitting window: the newest confirmed-or-detected
    supply regime (never bootstrap or dismissed, as learning_loop.latest_regime)
    or FIT_WINDOW_DAYS ago, whichever is later."""
    start = now - timedelta(days=FIT_WINDOW_DAYS)
    try:
        row = conn.execute(
            "SELECT started_at FROM supply_regime "
            "WHERE source <> 'bootstrap' AND dismissed_at IS NULL "
            "ORDER BY started_at DESC LIMIT 1").fetchone()
    except sqlite3.OperationalError:
        row = None
    regime = _parse_utc(row[0]) if row else None
    if regime and regime > start:
        start = regime
    return start.astimezone(timezone.utc).isoformat()


def load_cuts(conn: sqlite3.Connection, circuit: str) -> Optional[Cuts]:
    """Stored cuts for the circuit, or None (no row yet, pre-migration DB, or
    a row that is not four ascending positive numbers)."""
    try:
        row = conn.execute(
            "SELECT cuts_json FROM sparkline_tier_cuts WHERE circuit = ?",
            (circuit,)).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    try:
        cuts = tuple(float(v) for v in json.loads(row[0]))
    except (TypeError, ValueError):
        return None
    if (len(cuts) != 4 or not all(math.isfinite(c) and c > 0 for c in cuts)
            or any(b <= a for a, b in zip(cuts, cuts[1:]))):
        return None
    return cuts


def cuts_for(conn: sqlite3.Connection, circuit: str,
             circuit_type: Optional[str] = None) -> Cuts:
    """The cuts History should draw with: stored, else the type's default."""
    return (load_cuts(conn, circuit)
            or default_cuts_lpm(get_circuit_type(conn, circuit,
                                                 default=circuit_type or "fixture")))


def refit_circuit(conn: sqlite3.Connection, circuit: str,
                  circuit_type: Optional[str] = None,
                  now: Optional[datetime] = None) -> dict:
    """Refit one circuit's cuts and store them. Runs on the DB worker.

    The sample size, weight and window are refreshed every run; the cuts
    themselves are replaced only when one moved more than MOVE_TOLERANCE.
    """
    now = now or datetime.now(timezone.utc)
    ctype = get_circuit_type(conn, circuit, default=circuit_type or "fixture")
    start = window_start(conn, now)
    peaks = [r[0] for r in conn.execute(
        f"SELECT e.peak_flow_lpm FROM events e WHERE e.circuit = ? "
        f"AND e.start_ts >= ? AND e.peak_flow_lpm > 0 AND {_NOT_REAL_SQL} = 0",
        (circuit, start))]
    cuts, weight, n = fit_cuts(peaks, default_cuts_lpm(ctype))
    old = load_cuts(conn, circuit)
    changed = moved_enough(old, cuts)
    stamp = now.astimezone(timezone.utc).isoformat()
    meta = (n, weight, start, stamp)
    if changed:
        conn.execute(
            "INSERT INTO sparkline_tier_cuts (circuit, cuts_json, sample_n, "
            " fit_weight, window_start, checked_at, cuts_updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(circuit) DO UPDATE SET cuts_json = excluded.cuts_json, "
            " sample_n = excluded.sample_n, fit_weight = excluded.fit_weight, "
            " window_start = excluded.window_start, "
            " checked_at = excluded.checked_at, "
            " cuts_updated_at = excluded.cuts_updated_at",
            (circuit, json.dumps(cuts), *meta, stamp))
    else:
        conn.execute(
            "UPDATE sparkline_tier_cuts SET sample_n = ?, fit_weight = ?, "
            " window_start = ?, checked_at = ? WHERE circuit = ?",
            (*meta, circuit))
    conn.commit()
    if changed:
        log.info("[%s] sparkline tiers %s: %s gpm (%d real events since %s, "
                 "weight %.2f)", circuit, "set" if old is None else "refit",
                 "/".join(f"{c / GPM_TO_LPM:.2f}" for c in cuts), n,
                 start[:10], weight)
    return {"circuit": circuit, "cuts": cuts if changed else old,
            "sample_n": n, "fit_weight": weight, "changed": changed}


def load_status(conn: sqlite3.Connection, circuit: str) -> Optional[dict]:
    """Stored row for display (Dev Tools), or None."""
    cuts = load_cuts(conn, circuit)
    if cuts is None:
        return None
    row = conn.execute(
        "SELECT sample_n, fit_weight, window_start, checked_at, cuts_updated_at "
        "FROM sparkline_tier_cuts WHERE circuit = ?", (circuit,)).fetchone()
    return {"cuts": cuts, "sample_n": row[0], "fit_weight": row[1],
            "window_start": row[2], "checked_at": row[3],
            "cuts_updated_at": row[4]}
