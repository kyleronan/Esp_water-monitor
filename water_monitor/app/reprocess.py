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
from .detector_validation import HA_HIGH_FIDELITY_DAYS
from .event_rules import NOT_ARTIFACT_SQL
from .feature_extractor import SPARSE_ENVELOPE_REASON
from .database import (delete_events_in_range, get_home_profile, get_write_lock,
                       preview_events_in_range, restore_deleted_events, run_db,
                       run_isolated_write)

log = logging.getLogger(__name__)

# ── dev.38 guarded auto-split ────────────────────────────────────────────────
# Over-merged events (the live detector welds draws 30 s–5 min apart into one
# envelope; the importer reconstructs at 15 s granularity). These gate which stored
# events are CANDIDATES and confirm a real over-merge via a dry-run reconstruction.
_SPLIT_MIN_IDLE_S: float = 60.0   # internal idle gap (dur − active) the importer's 15 s splits
_SPLIT_MIN_PERIODS: int = 2       # dry-run must find >= 2 draws (1 = single draw, skip)
_SPLIT_MAX_PERIODS: int = 10      # ...and <= K — more is chatter (e.g. softener brine), skip
# dev.50 — was 24 h, which made this job purely FORWARD-looking: it cleaned events as
# they settled and never revisited a backlog, so an over-merged event that was missed
# (or one freed later by a label being cleared) was never reconsidered. Scan the whole
# window HA can still rebuild from instead. This is a fast-path SKIP HINT, never a
# correctness boundary: purge_keep_days is user-configurable and NOT queryable
# (ha_client.get_ha_config wraps HA's core config, which does not expose recorder
# options), and HA's purge runs on a daily schedule, so a window "9.8 days old" may
# already be gone. _probe_refusal decides per window; the margin below just keeps this
# hint on the safe side of that schedule so we do not spend fetches on dead candidates.
_SPLIT_RETENTION_MARGIN_H: int = 12
_SPLIT_LOOKBACK_H: int = HA_HIGH_FIDELITY_DAYS * 24 - _SPLIT_RETENTION_MARGIN_H
_SPLIT_SETTLE_MIN: int = 60       # ...older than this, so the event is done being extended
_SPLIT_DEFAULT_LIMIT: int = 20    # per-pass cap (HA-history rate-limit)
_SPLIT_MIN_VOLUME_COVERAGE: float = 0.9   # dev.41: reconstructed flow must account for
                                          # this share of the stored volume, else the
                                          # window's history can't be trusted (skip)


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


def _probe_refusal(dry: Dict[str, Any], stored_volume_l: float) -> Optional[str]:
    """dev.50 — why this window must NOT be rebuilt, or ``None`` to proceed.

    The single place both reprocess UIs and the hourly auto-split decide whether HA's
    history can be trusted to reproduce what a delete would remove. Fails CLOSED: any
    doubt is a refusal, because the cost of a wrong "yes" is deleted water and the cost
    of a wrong "no" is an event left exactly as it is.

    Reasons are stable identifiers — routers map them to user-facing text and the
    auto-split records them in ``events.split_evaluation_outcome``.
    """
    if dry.get("fetch_failed"):
        return "fetch_failed"          # transient — the caller may retry later
    if not (dry.get("periods") or []):
        return "no_history"            # window is past recorder retention, or empty
    if dry.get("gappy"):
        return "gappy_history"         # recorder outage — reads as flow-off
    if stored_volume_l > 0.0:
        rebuilt = float(dry.get("flow_volume_l") or 0.0)
        if rebuilt < _SPLIT_MIN_VOLUME_COVERAGE * stored_volume_l:
            return "volume_unaccounted"
    return None


async def reprocess_window(
    orch: Any, circuit: str, from_dt: datetime, to_dt: datetime,
    probe: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Delete ``circuit``'s machine events overlapping ``[from_dt, to_dt]`` and
    re-import the (auto-widened) span from HA history.

    Returns ``{"deleted", "imported", "widened", "from", "to"}``, or
    ``{"busy": True}`` when another admin write is already running, or
    ``{"refused": <reason>}`` when the probe below says the rebuild cannot be
    trusted. Deliberately does NOT call ``update_import_state`` — re-importing a
    past range must never move the catch-up checkpoint backward.

    dev.50 — PROBE FIRST. The order used to be delete → fetch → hope, and an
    empty-but-SUCCESSFUL fetch is not an error: ``import_range`` returns 0 without
    raising, so the restore path never fired and the events stayed deleted with
    their volume reversed. Reprocessing anything past the HA recorder's window did
    exactly that. So the fetch now happens BEFORE the delete, via the importer's
    existing ``dry_run_reconstruction``, and the delete only proceeds against
    history proven able to rebuild the water:

      * the fetch succeeded (``fetch_failed``),
      * it found something to rebuild (non-empty ``periods``),
      * it has no recorder-gap markers (``gappy``), and
      * its re-integrated flow accounts for >= ``_SPLIT_MIN_VOLUME_COVERAGE`` of
        the stored volume the delete would remove.

    That last gate is the dev.41 trust check, promoted out of the auto-split so BOTH
    UIs inherit it. Note what this deliberately does NOT depend on: any assumption
    about ``purge_keep_days``, which is user-configurable and not queryable. A home
    keeping 3 days is as safe as one keeping 30 — the probe answers per window.

    ``probe`` lets a caller that has ALREADY dry-run this exact window pass the
    result in (the hourly auto-split has one in hand), avoiding a second fetch. It
    is ignored if the window widens below, because then it covers the wrong span.

    Atomicity: probe-first makes the empty-rebuild case impossible rather than
    recoverable, and ``restore_deleted_events`` stays as the defence for what remains
    — an exception mid-rebuild, or a purge landing between the probe and the import.
    Still not crash-atomic: a hard kill in the sub-second window between the committed
    delete and the re-import leaves the events deleted with no restore. A durable
    pending-reprocess journal remains deliberate future work.
    """
    importer = getattr(orch, "historical_importer", None)
    if importer is None:
        raise RuntimeError("historical importer unavailable")
    # Fast-fail for UX, and BEFORE spending an HA fetch: another
    # recompute/reclassify/reprocess is mid-flight.
    if get_write_lock().locked():
        return {"busy": True}

    from_iso, to_iso = from_dt.isoformat(), to_dt.isoformat()
    # 1) READ-ONLY: what would the delete take? Count, stored volume and true span,
    #    with nothing yet touched. Shares its selection with delete_events_in_range,
    #    so the numbers the probe is gated on describe exactly the rows at risk.
    preview = await run_db(preview_events_in_range, orch.db, circuit,
                           from_iso, to_iso)
    if not preview["count"]:
        log.info("[%s] reprocess %s..%s: no machine events in window "
                 "(user-labelled events are never reprocessed)",
                 circuit, from_iso, to_iso)
        return {"deleted": 0, "imported": 0, "widened": False,
                "from": from_iso, "to": to_iso, "refused": "nothing_to_do"}

    # 2) Widen to the true span FIRST (a deleted event can start before the picked
    #    window), so the probe covers everything the rebuild will have to reproduce.
    imp_from, imp_to, widened = compute_widened_window(
        from_dt, to_dt, preview["span_start"], preview["span_end"])

    # 3) PROBE the window. A caller-supplied probe only describes the requested
    #    window, so it is discarded when the widen moved the bounds.
    dry = probe if (probe is not None and not widened) else None
    if dry is None:
        dry = await importer.dry_run_reconstruction(circuit, imp_from, imp_to)
    refused = _probe_refusal(dry, preview["volume_litres"])
    if refused is not None:
        log.warning(
            "[%s] reprocess %s..%s REFUSED (%s) — %d event(s) / %.1f L left intact; "
            "rebuilt flow would be %.1f L across %d period(s)",
            circuit, imp_from.isoformat(), imp_to.isoformat(), refused,
            preview["count"], preview["volume_litres"],
            dry.get("flow_volume_l", 0.0), len(dry.get("periods") or []))
        return {"deleted": 0, "imported": 0, "widened": widened,
                "from": imp_from.isoformat(), "to": imp_to.isoformat(),
                "refused": refused}

    # 4) Only now delete, under the write lock (sync, isolated). The deleted_rows
    #    snapshot still backs a restore if the re-import raises.
    res = await run_isolated_write(
        DB_PATH,
        lambda c: delete_events_in_range(c, circuit, from_iso, to_iso))
    # 5) Reconstruct from HA. The reconstructed events queue onto the live pipeline;
    #    the FeatureExtractor worker stores + classifies them. import_range RAISES on
    #    a history-fetch failure — we then restore, so the reprocess is all-or-nothing.
    #    A zero return can now only mean the history changed under us between probe
    #    and import (a purge landing mid-rebuild), which is treated the same way.
    try:
        imported = await importer.import_range(circuit, imp_from, imp_to)
        if res["deleted"] and not imported:
            raise RuntimeError(
                "re-import produced 0 events for a window the probe said was "
                "rebuildable — history changed under the reprocess")
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


def _record_split_evaluations(conn: sqlite3.Connection, memos: list) -> int:
    """dev.50 — persist the over-merge job's SETTLED decisions (migration 20260814).

    Written once per pass rather than per event, so the whole memo costs ONE write-lock
    acquisition. Purely an optimisation — losing it costs re-evaluation, never
    correctness — so a pre-migration schema degrades quietly rather than breaking the
    pass that just did useful work.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        for eid, outcome in memos:
            conn.execute(
                "UPDATE events SET split_evaluated_at = ?, "
                "split_evaluation_outcome = ? WHERE id = ?", (now, outcome, eid))
        conn.commit()
    except sqlite3.Error as e:
        log.debug("split-evaluation memo skipped (pre-20260814 schema?): %s", e)
        return 0
    return len(memos)


async def auto_split_merged_events(
    orch: Any, circuit: str, limit: int = _SPLIT_DEFAULT_LIMIT,
    checked: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Guarded auto-split of over-merged events (dev.38). OFF unless
    ``home_profile.auto_split_enabled``.

    Scans recently-settled, UNLABELLED, multi-segment events with a large internal idle
    gap (likely several distinct draws welded into one envelope), confirms each via a
    DRY-RUN reconstruction (``importer.dry_run_reconstruction`` — the importer's
    15 s-granular period detection, no delete/store), and only then re-imports it split
    via ``reprocess_window``. Candidates include inflated ``sparse_envelope`` singles (the
    "brief use, long idle tail" events — excluded_from_training=1 but exactly what this
    cleans). Guards: user-labelled / user-classified / user-ignored, ARTIFACT verdicts
    (phantom / cross-talk / dribble — never reprocessed, they may carry a zeroed volume),
    anomaly-flagged, and softener-brine (``softener_session`` / ``water_softener``) events
    are never candidates; the dry-run gate (split ``_SPLIT_MIN_PERIODS..._SPLIT_MAX_PERIODS``
    or a single-draw SHRINK) skips clean singles and many-pulse chatter; and — dev.41 —
    a window whose history is UNTRUSTWORTHY (gap markers, or a reconstructed flow volume
    that can't account for ~90% of the stored volume) is never reprocessed, so incomplete
    recorder data can never shrink away real recorded water. Volume stays balanced
    through ``reprocess_window``'s ledger chokepoint. Re-imported sub-draws are
    single-segment, so they never re-trigger (no oscillation — structural, restart-safe).
    ``checked`` (an in-memory id set the caller carries across passes) avoids re-fetching
    a SETTLED decision every pass; transient outcomes (fetch failure, writer busy) are
    deliberately NOT added, so they retry on the next pass.
    Best-effort. Returns ``{"scanned","split","skipped","disabled?"}``."""
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
            "SELECT id, start_ts, end_ts, volume_litres FROM events "
            "WHERE circuit = ? AND end_ts >= ? AND end_ts <= ? "
            "  AND user_fixture_type IS NULL AND COALESCE(user_classified, 0) = 0 "
            "  AND COALESCE(user_ignored, 0) = 0 "
            # dev.40: the inflated "brief use, long idle tail" events this hygiene was
            # built to clean are flagged sparse_envelope, which sets
            # excluded_from_training=1 — so the old `excluded_from_training = 0` filter
            # screened out the very events it targets. Let sparse_envelope back in, but
            # keep benching the real ARTIFACT verdicts that excluded_from_training=0 used
            # to cover — phantom / cross-talk / dribble — via explicit flags, so we never
            # auto-reprocess a zeroed or artifact event. sparse_envelope keeps its volume,
            # so this stays volume- and leak-neutral; the dry-run gate still decides.
            "  AND (COALESCE(excluded_from_training, 0) = 0 "
            "       OR COALESCE(match_rejection_reason, '') = ?) "
            "  AND " + NOT_ARTIFACT_SQL + " "
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
            # dev.50 — the persisted decision memo (migration 20260814). The
            # checked-set below is in-memory, so before this every restart re-ran
            # the dry run for the whole backlog; harmless at a 24 h lookback, but
            # this now scans the entire recorder window and each re-check costs an
            # HA history fetch. A settled decision is written once and filtered here,
            # i.e. BEFORE any fetch — so a restart costs one query, not a fetch storm.
            "  AND split_evaluated_at IS NULL "
            # Newest first: an event the user is actually looking at gets the pass's
            # budget, and the backlog drains behind it.
            "ORDER BY end_ts DESC LIMIT ?",
            (circuit, lo, hi, SPARSE_ENVELOPE_REASON, _SPLIT_MIN_IDLE_S, limit),
        ).fetchall()
    finally:
        conn.close()

    if checked is None:
        checked = set()
    scanned = split = skipped = 0
    memos: list = []            # (event_id, outcome) — persisted in ONE write below
    for r in rows:
        eid = r["id"]
        if eid in checked:
            continue
        scanned += 1
        s_dt = _parse_utc(r["start_ts"])
        e_dt = _parse_utc(r["end_ts"] or r["start_ts"])
        stored_vol = float(r["volume_litres"] or 0.0)
        # Dry-run: what does the importer reconstruct here (with the dev.39 gate)?
        dry = await importer.dry_run_reconstruction(circuit, s_dt, e_dt)
        # dev.41/49 VOLUME-SAFETY, now via the shared chokepoint _probe_refusal: the
        # window's history must prove it can reproduce the stored water before this
        # event is deleted — no recorder-gap markers, something to rebuild, and
        # >= ~90% of the stored volume re-integrated.
        refused = _probe_refusal(dry, stored_vol)
        if refused == "fetch_failed":
            skipped += 1
            continue                    # transient — neither checked NOR memoed
        if refused is not None:
            # SETTLED, not a retry: missing history and gap markers are in the
            # recorded past forever. Memoed so no restart re-fetches this window.
            checked.add(eid)
            memos.append((eid, refused))
            skipped += 1
            continue
        periods = dry["periods"]
        stored_dur = (e_dt - s_dt).total_seconds()
        biggest = max(((pe - ps).total_seconds() for ps, pe in periods), default=0.0)
        # Reprocess when the re-import would meaningfully DE-BLOAT this event:
        #   • SPLIT  — 2..K reconstructed draws (the original merged case), OR
        #   • SHRINK — exactly 1 reconstructed draw that is >= _SPLIT_MIN_IDLE_S shorter
        #     than the stored span (an inflated single event — e.g. two blips a spurious
        #     pressure-dip welded into one long event — collapses to its real use).
        # > K draws is chatter (skip). A clean event reconstructs to ~itself (1 draw,
        # biggest ≈ stored) → skipped, no churn.
        is_split = _SPLIT_MIN_PERIODS <= len(periods) <= _SPLIT_MAX_PERIODS
        is_shrink = (len(periods) == 1 and biggest <= stored_dur - _SPLIT_MIN_IDLE_S)
        if not (is_split or is_shrink):
            checked.add(eid)
            memos.append((eid, "clean"))     # nothing to gain — settled
            skipped += 1
            continue
        # Hand the probe on: reprocess_window re-gates on it and this saves an
        # identical second fetch of the same window.
        res = await reprocess_window(orch, circuit, s_dt, e_dt, probe=dry)
        if res.get("busy"):
            break                       # another admin write is mid-flight; retry next
                                        # pass — eid deliberately NOT in checked
        if res.get("refused"):
            checked.add(eid)
            memos.append((eid, res["refused"]))
            skipped += 1
            continue
        checked.add(eid)                # split events are GONE (new ids) — never memoed
        split += 1
        log.info("[%s] auto-split: event %s (%s..%s) → %d draws, re-imported %d",
                 circuit, eid, r["start_ts"][:19], (r["end_ts"] or "")[:19],
                 len(periods), res.get("imported", 0))
    if memos:
        await run_isolated_write(
            DB_PATH, lambda c: _record_split_evaluations(c, memos))
    if split or skipped:
        # dev.50 — the split count is now load-bearing, not a cleanup statistic: with
        # the live detector still welding on a pump-held line, this job is what
        # de-bloats those events, so a dead or starved pass must be visible in the log.
        log.info("[%s] auto-split pass: %d scanned, %d split, %d skipped "
                 "(%d decision(s) memoed)",
                 circuit, scanned, split, skipped, len(memos))
    return {"scanned": scanned, "split": split, "skipped": skipped,
            "memoed": len(memos)}
