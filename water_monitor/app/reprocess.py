"""dev.26 — shared reprocess-window orchestration.

A "reprocess" deletes a circuit's purely-machine-derived events overlapping a time
window (reversing their volume) and re-imports that window from HA flow history, so
a garbled stored event — e.g. an irrigation run that failed to close and absorbed a
whole day — is rebuilt as the real runs. Two UIs drive it through the SAME core:

  * the History event modal (window = the clicked event's own span ± a buffer), and
  * the Settings → Dev tools date tool (window = a local calendar range).

Keeping the delete + auto-widen + import logic here (not duplicated in each router)
guarantees both paths behave identically. Reuses ``delete_events_in_range`` (the
overlap-aware, volume-reversing, label-preserving delete),
``historical_importer.import_range`` (HA reconstruction), and the
``run_isolated_write`` / ``get_write_lock`` admin-write serialisation.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple

from .config import DB_PATH
from .database import (delete_events_in_range, get_home_profile, get_write_lock,
                       run_isolated_write)

log = logging.getLogger(__name__)

# ── dev.38 guarded auto-split ────────────────────────────────────────────────
# Over-merged events (the live detector welds draws 30 s–5 min apart into one
# envelope; the importer reconstructs at 15 s granularity). These gate which stored
# events are CANDIDATES and confirm a real over-merge via a dry-run reconstruction.
_SPLIT_MIN_IDLE_S: float = 60.0   # internal idle gap (dur − active) the importer's 15 s splits
_SPLIT_MIN_PERIODS: int = 2       # dry-run must find >= 2 draws (1 = single draw, skip)
_SPLIT_MAX_PERIODS: int = 10      # ...and <= K — more is chatter (e.g. softener brine), skip
_SPLIT_LOOKBACK_H: int = 24       # only scan recently-settled events (bounds re-checks)
_SPLIT_SETTLE_MIN: int = 60       # ...older than this, so the event is done being extended
_SPLIT_DEFAULT_LIMIT: int = 20    # per-pass cap (HA-history rate-limit)


def _parse_utc(value: str) -> datetime:
    """Parse a stored ISO timestamp to an aware UTC datetime (assume UTC if naive)."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def compute_widened_window(
    from_dt: datetime,
    to_dt: datetime,
    span_start: Optional[str],
    span_end: Optional[str],
) -> Tuple[datetime, datetime, bool]:
    """Widen ``[from_dt, to_dt]`` to engulf the full span of whatever was deleted.

    ``span_start`` / ``span_end`` are the ISO bounds reported by
    ``delete_events_in_range`` (or ``None`` when nothing was deleted). A deleted
    event that extends *outside* the picked window (the 27.6 h case that started the
    day before) must be re-imported across its whole span — otherwise the part
    outside the window would be lost. Returns ``(imp_from, imp_to, widened)``.
    """
    imp_from, imp_to = from_dt, to_dt
    if span_start:
        s = _parse_utc(span_start)
        if s < imp_from:
            imp_from = s
    if span_end:
        e = _parse_utc(span_end)
        if e > imp_to:
            imp_to = e
    widened = imp_from != from_dt or imp_to != to_dt
    return imp_from, imp_to, widened


async def reprocess_window(
    orch: Any, circuit: str, from_dt: datetime, to_dt: datetime,
) -> Dict[str, Any]:
    """Delete ``circuit``'s machine events overlapping ``[from_dt, to_dt]`` and
    re-import the (auto-widened) span from HA history.

    Returns ``{"deleted", "imported", "widened", "from", "to"}``, or
    ``{"busy": True}`` when another admin write is already running. Deliberately
    does NOT call ``update_import_state`` — re-importing a past range must never
    move the catch-up checkpoint backward.
    """
    importer = getattr(orch, "historical_importer", None)
    if importer is None:
        raise RuntimeError("historical importer unavailable")
    # Fast-fail for UX: another recompute/reclassify/reprocess is mid-flight.
    if get_write_lock().locked():
        return {"busy": True}

    from_iso, to_iso = from_dt.isoformat(), to_dt.isoformat()
    # 1) Delete the window's machine events under the write lock (sync, isolated).
    res = await run_isolated_write(
        DB_PATH, lambda c: delete_events_in_range(c, circuit, from_iso, to_iso))
    # 2) Widen the import to the true deleted span, then reconstruct from HA. The
    #    reconstructed events queue onto the live pipeline; the FeatureExtractor
    #    worker stores + classifies them.
    imp_from, imp_to, widened = compute_widened_window(
        from_dt, to_dt, res["span_start"], res["span_end"])
    imported = await importer.import_range(circuit, imp_from, imp_to)
    log.info(
        "[%s] reprocess %s..%s (widened=%s → %s..%s): deleted %d, re-imported %d",
        circuit, from_iso, to_iso, widened,
        imp_from.isoformat(), imp_to.isoformat(), res["deleted"], imported,
    )
    return {
        "deleted": res["deleted"],
        "imported": imported,
        "widened": widened,
        "from": imp_from.isoformat(),
        "to": imp_to.isoformat(),
    }


def _auto_split_enabled(conn: sqlite3.Connection) -> bool:
    """Read the dev.38 opt-in flag fresh (so a Settings toggle takes effect with no
    restart). Defaults OFF / absent-column-safe."""
    try:
        prof = get_home_profile(conn)
        return bool(prof is not None and prof["auto_split_enabled"])
    except (sqlite3.Error, IndexError, KeyError, TypeError):
        return False


async def auto_split_merged_events(
    orch: Any, circuit: str, limit: int = _SPLIT_DEFAULT_LIMIT,
    checked: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Guarded auto-split of over-merged events (dev.38). OFF unless
    ``home_profile.auto_split_enabled``.

    Scans recently-settled, UNLABELLED, multi-segment events with a large internal idle
    gap (likely several distinct draws welded into one envelope), confirms each via a
    DRY-RUN reconstruction (``importer.count_candidate_periods`` — the importer's
    15 s-granular period detection, no delete/store), and only then re-imports it split
    via ``reprocess_window``. Guards: user-labelled / user-classified / user-ignored and
    softener-brine (``softener_session`` / ``water_softener``) events are never candidates;
    the dry-run gate (``_SPLIT_MIN_PERIODS..._SPLIT_MAX_PERIODS``) skips single draws and
    many-pulse chatter; volume stays balanced through ``reprocess_window``'s ledger
    chokepoint. Re-imported sub-draws are single-segment, so they never re-trigger
    (no oscillation — structural, restart-safe). ``checked`` (an in-memory id set the
    caller carries across passes) avoids re-fetching a settled non-split candidate every
    pass. Best-effort. Returns ``{"scanned","split","skipped","disabled?"}``."""
    importer = getattr(orch, "historical_importer", None)
    if importer is None:
        return {"scanned": 0, "split": 0, "skipped": 0}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if not _auto_split_enabled(conn):
            return {"scanned": 0, "split": 0, "skipped": 0, "disabled": True}
        now = datetime.now(timezone.utc)
        lo = (now - timedelta(hours=_SPLIT_LOOKBACK_H)).isoformat()
        hi = (now - timedelta(minutes=_SPLIT_SETTLE_MIN)).isoformat()
        rows = conn.execute(
            "SELECT id, start_ts, end_ts FROM events "
            "WHERE circuit = ? AND end_ts >= ? AND end_ts <= ? "
            "  AND user_fixture_type IS NULL AND COALESCE(user_classified, 0) = 0 "
            "  AND COALESCE(user_ignored, 0) = 0 "
            "  AND COALESCE(excluded_from_training, 0) = 0 "
            "  AND COALESCE(active_flow_segment_count, 0) >= 2 "
            "  AND (duration_seconds - COALESCE(active_flow_duration_seconds, 0)) >= ? "
            "  AND COALESCE(matched_via, '') <> 'softener_session' "
            "  AND COALESCE(matched_fixture_type, '') <> 'water_softener' "
            "ORDER BY start_ts ASC LIMIT ?",
            (circuit, lo, hi, _SPLIT_MIN_IDLE_S, limit),
        ).fetchall()
    finally:
        conn.close()

    if checked is None:
        checked = set()
    scanned = split = skipped = 0
    for r in rows:
        eid = r["id"]
        if eid in checked:
            continue
        checked.add(eid)
        scanned += 1
        s_dt = _parse_utc(r["start_ts"])
        e_dt = _parse_utc(r["end_ts"] or r["start_ts"])
        # Dry-run: how many distinct draws does the importer reconstruct here?
        periods = await importer.count_candidate_periods(circuit, s_dt, e_dt)
        if not (_SPLIT_MIN_PERIODS <= len(periods) <= _SPLIT_MAX_PERIODS):
            skipped += 1
            continue
        res = await reprocess_window(orch, circuit, s_dt, e_dt)
        if res.get("busy"):
            break                       # another admin write is mid-flight; retry next pass
        split += 1
        log.info("[%s] auto-split: event %s (%s..%s) → %d draws, re-imported %d",
                 circuit, eid, r["start_ts"][:19], (r["end_ts"] or "")[:19],
                 len(periods), res.get("imported", 0))
    if split or skipped:
        log.info("[%s] auto-split pass: %d scanned, %d split, %d skipped",
                 circuit, scanned, split, skipped)
    return {"scanned": scanned, "split": split, "skipped": skipped}
