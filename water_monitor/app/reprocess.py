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
                       restore_deleted_events, run_isolated_write)

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

    Atomicity: EXCEPTION-atomic. A re-import failure (incl. an HA history-fetch error,
    surfaced by ``import_range(strict=True)``) restores the deleted events verbatim, so
    no water is lost. It is NOT crash-atomic: a hard process kill in the (sub-second)
    window between the committed delete and the re-import would leave the events deleted
    (volume under-counted) with no restore. Full crash recovery (a durable pending-
    reprocess journal replayed at startup) is deliberate future work — the window is
    tiny and the auto-hygiene only targets low-value garbled events.
    """
    importer = getattr(orch, "historical_importer", None)
    if importer is None:
        raise RuntimeError("historical importer unavailable")
    # Fast-fail for UX: another recompute/reclassify/reprocess is mid-flight.
    if get_write_lock().locked():
        return {"busy": True}

    from_iso, to_iso = from_dt.isoformat(), to_dt.isoformat()
    # 1) Delete the window's machine events under the write lock (sync, isolated).
    #    capture=True snapshots the deleted rows so step 2 can restore them if the
    #    re-import fails — making delete-then-import atomic (no lost water).
    res = await run_isolated_write(
        DB_PATH,
        lambda c: delete_events_in_range(c, circuit, from_iso, to_iso, capture=True))
    # 2) Widen the import to the true deleted span, then reconstruct from HA. The
    #    reconstructed events queue onto the live pipeline; the FeatureExtractor
    #    worker stores + classifies them. strict=True so a history-fetch failure
    #    RAISES (instead of silently returning 0) — we then restore the deleted
    #    events so the reprocess is all-or-nothing.
    imp_from, imp_to, widened = compute_widened_window(
        from_dt, to_dt, res["span_start"], res["span_end"])
    try:
        imported = await importer.import_range(circuit, imp_from, imp_to, strict=True)
    except Exception:
        deleted_rows = res.get("deleted_rows") or []
        if deleted_rows:
            restored = await run_isolated_write(
                DB_PATH, lambda c: restore_deleted_events(c, deleted_rows))
            log.error("[%s] reprocess re-import FAILED after delete — restored %d "
                      "event(s); no data lost", circuit, restored)
        raise
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
            # dev.39 LEAK-SAFETY (adversarial-review fix): never auto-reprocess an event
            # the anomaly detector has FLAGGED. Splitting/shrinking a flagged event could
            # strip its leak signal (a long, unusual-duration event becomes several
            # individually-normal fragments). A flagged event is left exactly as-is; only
            # unremarkable, un-flagged garbled events are auto-cleaned.
            "  AND COALESCE(flagged, 0) = 0 "
            # dev.39: >=1 (was >=2) so an INFLATED single event — one short draw a
            # spurious pressure-dip envelope stretched across a long idle (the 20-min /
            # 0.3 L-blips bug) — is a candidate too, not just multi-draw merges. The
            # big idle gap below is the real selector; the dry-run gate decides.
            "  AND COALESCE(active_flow_segment_count, 0) >= 1 "
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
        # Dry-run: what does the importer reconstruct here (with the dev.39 gate)?
        periods = await importer.count_candidate_periods(circuit, s_dt, e_dt)
        stored_dur = (e_dt - s_dt).total_seconds()
        biggest = max(((pe - ps).total_seconds() for ps, pe in periods), default=0.0)
        # Reprocess when the re-import would meaningfully DE-BLOAT this event:
        #   • SPLIT  — 2..K reconstructed draws (the original merged case), OR
        #   • SHRINK — exactly 1 reconstructed draw that is >= _SPLIT_MIN_IDLE_S shorter
        #     than the stored span (an inflated single event — e.g. two blips a spurious
        #     pressure-dip welded into one long event — collapses to its real use).
        # > K draws is chatter (skip). A clean event reconstructs to ~itself (1 draw,
        # biggest ≈ stored) → skipped, no churn. Volume is preserved by reprocess_window's
        # atomic re-import either way.
        is_split = _SPLIT_MIN_PERIODS <= len(periods) <= _SPLIT_MAX_PERIODS
        is_shrink = (len(periods) == 1 and biggest <= stored_dur - _SPLIT_MIN_IDLE_S)
        if not (is_split or is_shrink):
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
