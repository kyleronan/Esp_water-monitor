"""Shared reprocess-window orchestration.

A reprocess deletes a circuit's purely-machine-derived events overlapping a time
window (reversing their volume) and re-imports that window from HA flow history,
so a garbled stored event (an irrigation run that failed to close and absorbed a
whole day) is rebuilt as the real runs. The History event modal (window = the
clicked event's span ± a buffer) and the Settings → Dev tools date tool (window =
a local calendar range) both drive this ONE core, so the delete + auto-widen +
import path cannot diverge between them. Built on ``delete_events_in_range``
(overlap-aware, volume-reversing, label-preserving), ``import_range`` (HA
reconstruction) and the ``run_isolated_write`` / ``get_write_lock`` admin-write
serialisation.
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
from .overlap_guard import VOLUME_COVERAGE_FRACTION
from .database import (delete_events_in_range, find_overlapping_event,
                       get_home_profile, get_write_lock, preview_events_in_range,
                       restore_deleted_events, run_db, run_isolated_write)

log = logging.getLogger(__name__)

# ── guarded auto-split ───────────────────────────────────────────────────────
# Over-merged events (the live detector welds draws 30 s–5 min apart into one
# envelope; the importer reconstructs at 15 s granularity). These gate which stored
# events are CANDIDATES and confirm a real over-merge via a dry-run reconstruction.
_SPLIT_MIN_IDLE_S: float = 60.0   # internal idle gap (dur − active) the importer's 15 s splits
_SPLIT_MIN_PERIODS: int = 2       # dry-run must find >= 2 draws (1 = single draw, skip)
_SPLIT_MAX_PERIODS: int = 10      # ...and <= K — more is chatter (e.g. softener brine), skip
# Scan the whole window HA can still rebuild from (a missed over-merge, or one
# freed by a cleared label, must be reconsidered). A fast-path SKIP HINT, never a
# correctness boundary: purge_keep_days is user-configurable and NOT queryable
# (ha_client.get_ha_config does not expose recorder options) and HA purges on a
# daily schedule, so a window "9.8 days old" may already be gone. _probe_refusal
# decides per window; the margin keeps this hint clear of that schedule.
_SPLIT_RETENTION_MARGIN_H: int = 12
_SPLIT_LOOKBACK_H: int = HA_HIGH_FIDELITY_DAYS * 24 - _SPLIT_RETENTION_MARGIN_H
_SPLIT_SETTLE_MIN: int = 60       # ...older than this, so the event is done being extended
_SPLIT_DEFAULT_LIMIT: int = 20    # per-pass cap (HA-history rate-limit)
_SPLIT_MIN_VOLUME_COVERAGE: float = VOLUME_COVERAGE_FRACTION   # ONE object with the importer's containment rule: reconstructed flow must account for
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
    """Why this window must NOT be rebuilt, or ``None`` to proceed.

    The single place both reprocess UIs and the hourly auto-split decide whether
    HA's history can reproduce what a delete would remove. Fails CLOSED: a wrong
    "yes" is deleted water, a wrong "no" is an event left exactly as it is.
    Reasons are stable identifiers — routers map them to user-facing text and
    the auto-split records them in ``events.split_evaluation_outcome``.
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


# Reasons the KEPT rows (not HA history) refuse a rebuild. Literals are
# mirrored in database._KEPT_EVENT_MEMO_REASONS (label changes re-open these memos).
BLOCKED_BY_KEPT_EVENTS = "blocked_by_kept_events"
KEPT_EVENTS_UNDERFIT = "kept_events_underfit"


def _kept_event_blockers(
    conn: sqlite3.Connection, circuit: str, periods: list, deletable_ids: list,
    min_duration_s: float,
) -> Tuple[int, list]:
    """Simulate the importer's insert-time overlap skip BEFORE the delete.

    ``_import_range`` drops a reconstructed period shorter than its minimum and
    skips one that overlaps an existing event (``find_overlapping_event``,
    most-protected row first; a contained machine row blocks like any other).
    After a reprocess delete the only rows left to collide with are the ones it
    KEEPS — user-labelled, user-classified, user-ignored, or machine rows outside
    its selection (cycle/anchor-labelled rows are machine output and ARE in the
    selection, so they do not block) — so the deletable ids are excluded
    in-query and the answer is the importer's answer. One connection, one loop,
    never one ``run_db`` per period (the interleave window). Returns
    ``(rebuildable_count, blockers)``, blockers as ``(period_index, row)``.
    """
    rebuildable = 0
    blockers: list = []
    for idx, (ps, pe) in enumerate(periods):
        if (pe - ps).total_seconds() < min_duration_s:
            continue
        row = find_overlapping_event(
            conn, circuit, ps.isoformat(), pe.isoformat(),
            exclude_event_ids=deletable_ids)
        if row is None:
            rebuildable += 1
        else:
            blockers.append((idx, row))
    return rebuildable, blockers


def _kept_event_refusal(
    dry: Dict[str, Any], rebuildable: int, blockers: list,
) -> Tuple[Optional[str], float, float]:
    """Turn ``_kept_event_blockers``'s answer into a refusal reason.

    No rebuildable period at all → ``blocked_by_kept_events`` (otherwise: delete,
    0 imported, restore, "see addon log"). Some periods blocked → weigh the water
    HA shows in them against the stored water on the distinct rows blocking them:
    if the kept rows cover it (same tolerance as the volume gate) the rebuild may
    proceed — the importer skips those periods and their water is already on the
    record; if not, ``kept_events_underfit``, because deleting the wrapper would
    make the difference vanish and real water must never become invisible. A
    dry-run without ``period_volumes_l`` (an older caller) cannot be weighed, so
    it is refused only on the no-period case. Both volumes are computed BEFORE
    any branch returns, so the caller's log never prints an unmeasured figure.
    Returns ``(reason_or_None, blocked_period_volume_l, blocker_volume_l)``.
    """
    # Weigh the water FIRST, so every branch reports what it actually measured.
    # The caller logs both figures; a branch that returns a hard-coded 0.0 reads
    # to the operator as water having gone missing.
    vols = dry.get("period_volumes_l")
    blocked_l = 0.0
    if vols:
        for idx, _row in blockers:
            try:
                blocked_l += float(vols[idx] or 0.0)
            except (IndexError, TypeError, ValueError):
                pass
    seen: Set[str] = set()
    kept_l = 0.0
    for _idx, row in blockers:
        if row["id"] in seen:
            continue
        seen.add(row["id"])
        kept_l += float(row.get("volume_litres") or 0.0)

    if rebuildable == 0:
        # Nothing could be rebuilt at all: the delete would remove the event
        # and the importer would insert nothing in its place.
        return BLOCKED_BY_KEPT_EVENTS, blocked_l, kept_l
    if not blockers:
        return None, 0.0, 0.0
    if not vols:
        return None, 0.0, kept_l
    if blocked_l > 0.0 and kept_l < _SPLIT_MIN_VOLUME_COVERAGE * blocked_l:
        return KEPT_EVENTS_UNDERFIT, blocked_l, kept_l
    return None, blocked_l, kept_l


async def reprocess_window(
    orch: Any, circuit: str, from_dt: datetime, to_dt: datetime,
    probe: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Delete ``circuit``'s machine events overlapping ``[from_dt, to_dt]`` and
    re-import the (auto-widened) span from HA history.

    Returns ``{"deleted", "imported", "widened", "from", "to"}``, ``{"busy":
    True}`` when another admin write is running, or ``{"refused": <reason>}``
    when the probe says the rebuild cannot be trusted. Never calls
    ``update_import_state`` — re-importing a past range must not move the
    catch-up checkpoint backward.

    PROBE FIRST. ``import_range`` returns 0 on an empty-but-successful fetch
    without raising, so delete-then-fetch would leave the events deleted, their
    volume reversed, and the restore path never firing — exactly what
    reprocessing past the HA recorder's window does. The fetch
    (``dry_run_reconstruction``) therefore precedes the delete, which proceeds
    only against history proven able to rebuild the water: fetch succeeded,
    periods found, no recorder-gap markers, and >= ``_SPLIT_MIN_VOLUME_COVERAGE``
    of the stored volume re-integrated. Shared so BOTH UIs inherit it, and
    independent of ``purge_keep_days`` (user-configurable, not queryable): the
    probe answers per window, so 3 days of retention is as safe as 30.

    ``probe`` lets a caller that has ALREADY dry-run this exact window (the
    hourly auto-split) pass the result in; it is ignored if the window widens,
    since it then covers the wrong span.

    Atomicity: probe-first makes the empty-rebuild case impossible rather than
    recoverable; ``restore_deleted_events`` covers what remains (an exception
    mid-rebuild, a purge between probe and import). NOT crash-atomic: a hard
    kill between the committed delete and the re-import leaves the events
    deleted with no restore; a durable pending-reprocess journal is deliberate
    future work.
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
    # The stored volume is overlap-aware: rows stacked on the same seconds (a
    # garbled parent and the children recorded inside it) count once, so a
    # duplicated span can be un-duplicated instead of being refused BECAUSE it is
    # duplicated. Non-overlapping rows are summed exactly.
    refused = _probe_refusal(dry, preview["volume_litres"])
    if refused is not None:
        overlap_note = ""
        if preview.get("overlapping"):
            overlap_note = (" (rows overlap: %.1f L summed, counted once per overlap "
                            "group)" % float(preview.get("volume_litres_summed") or 0.0))
        log.warning(
            "[%s] reprocess %s..%s REFUSED (%s) — %d event(s) / %.1f L left intact%s; "
            "rebuilt flow would be %.1f L across %d period(s)",
            circuit, imp_from.isoformat(), imp_to.isoformat(), refused,
            preview["count"], preview["volume_litres"], overlap_note,
            dry.get("flow_volume_l", 0.0), len(dry.get("periods") or []))
        return {"deleted": 0, "imported": 0, "widened": widened,
                "from": imp_from.isoformat(), "to": imp_to.isoformat(),
                "refused": refused}

    # 3b) Would the rows the delete KEEPS block the rebuild? History said
    #     the water is there; this asks whether the importer would be ALLOWED to
    #     insert it once the machine rows are gone. Answered against the exact
    #     surviving row set (deletable ids excluded in-query), one connection.
    min_dur = float(getattr(importer, "MIN_DURATION_SECONDS", 0.0) or 0.0)
    rebuildable, blockers = await run_db(
        _kept_event_blockers, orch.db, circuit, list(dry.get("periods") or []),
        list(preview.get("ids") or []), min_dur)
    kept_reason, blocked_l, kept_l = _kept_event_refusal(dry, rebuildable, blockers)
    if kept_reason is not None:
        who = ", ".join(
            f"{row['id'][:8]}({row.get('user_fixture_type') or 'kept'})"
            for _i, row in blockers) or "none"
        log.warning(
            "[%s] reprocess %s..%s REFUSED (%s) — %d event(s) / %.1f L left intact; "
            "%d of %d rebuilt period(s) would be blocked by kept event(s) %s; "
            "blocked periods carry %.1f L, kept blockers hold %.1f L",
            circuit, imp_from.isoformat(), imp_to.isoformat(), kept_reason,
            preview["count"], preview["volume_litres"],
            len(blockers), len(blockers) + rebuildable, who, blocked_l, kept_l)
        return {"deleted": 0, "imported": 0, "widened": widened,
                "from": imp_from.isoformat(), "to": imp_to.isoformat(),
                "refused": kept_reason}

    # 4) Only now delete, under the write lock (sync, isolated). The deleted_rows
    #    snapshot still backs a restore if the re-import raises.
    res = await run_isolated_write(
        DB_PATH,
        lambda c: delete_events_in_range(c, circuit, from_iso, to_iso))
    # 5) Reconstruct from HA. The reconstructed events queue onto the live pipeline;
    #    the FeatureExtractor worker stores + classifies them. import_range RAISES on
    #    a history-fetch failure — we then restore, so the reprocess is all-or-nothing.
    #    A zero return means the history was purged or changed between probe and
    #    import (kept rows blocking every period are refused at 3b), which is
    #    treated the same way.
    try:
        imported = await importer.import_range(circuit, imp_from, imp_to)
        if res["deleted"] and not imported:
            raise RuntimeError(
                "re-import produced 0 events after the probe passed — history "
                "purged or changed between probe and import")
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
    """Read the opt-in flag fresh (so a Settings toggle takes effect with no
    restart). Defaults OFF / absent-column-safe."""
    try:
        prof = get_home_profile(conn)
        return bool(prof is not None and prof["auto_split_enabled"])
    except (sqlite3.Error, IndexError, KeyError, TypeError):
        return False


def _record_split_evaluations(conn: sqlite3.Connection, memos: list) -> int:
    """Persist the over-merge job's SETTLED decisions (migration 20260814).

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
    """Guarded auto-split of over-merged events. OFF unless
    ``home_profile.auto_split_enabled``.

    Scans recently-settled, UNLABELLED events with a large internal idle gap —
    several draws welded into one envelope, or an inflated ``sparse_envelope``
    single (excluded_from_training=1, but exactly what this cleans) — confirms
    each by a DRY-RUN reconstruction, and only then rebuilds it via
    ``reprocess_window`` (which keeps the volume ledger balanced). Never
    candidates: user-labelled / user-classified / user-ignored rows, ARTIFACT
    verdicts (phantom / cross-talk / dribble may carry a zeroed volume),
    anomaly-flagged events, and softener brine. The dry-run gate
    (``_SPLIT_MIN_PERIODS..._SPLIT_MAX_PERIODS`` draws, or a single-draw
    SHRINK) skips clean singles and chatter; ``_probe_refusal`` refuses gappy
    or volume-unaccounted history, so incomplete recorder data can never shrink
    real recorded water. Re-imported sub-draws are single-segment, so they
    never re-trigger (no oscillation, restart-safe). ``checked`` is carried
    across passes to skip SETTLED decisions; transient outcomes (fetch failure,
    writer busy) are deliberately NOT added, so they retry. Best-effort.
    Returns ``{"scanned","split","skipped","disabled?"}``."""
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
            # The inflated "brief use, long idle tail" events this hygiene cleans are
            # flagged sparse_envelope, which sets excluded_from_training=1 — so an
            # `excluded_from_training = 0` filter would screen out the very events it
            # targets. sparse_envelope is admitted; the real ARTIFACT verdicts (phantom
            # / cross-talk / dribble) stay benched via explicit flags, so a zeroed or
            # artifact event is never auto-reprocessed. sparse_envelope keeps its
            # volume, so this stays volume- and leak-neutral; the dry-run gate decides.
            "  AND (COALESCE(excluded_from_training, 0) = 0 "
            "       OR COALESCE(match_rejection_reason, '') = ?) "
            "  AND " + NOT_ARTIFACT_SQL + " "
            # LEAK-SAFETY: never auto-reprocess an event
            # the anomaly detector has FLAGGED. Splitting/shrinking a flagged event could
            # strip its leak signal (a long, unusual-duration event becomes several
            # individually-normal fragments). A flagged event is left exactly as-is; only
            # unremarkable, un-flagged garbled events are auto-cleaned.
            "  AND COALESCE(flagged, 0) = 0 "
            # >=1, not >=2, so an INFLATED single event — one short draw a spurious
            # pressure-dip envelope stretched across a long idle (the 20-min /
            # 0.3 L-blips case) — is a candidate too, not just multi-draw merges. The
            # big idle gap below is the real selector; the dry-run gate decides.
            "  AND COALESCE(active_flow_segment_count, 0) >= 1 "
            "  AND (duration_seconds - COALESCE(active_flow_duration_seconds, 0)) >= ? "
            "  AND COALESCE(matched_via, '') <> 'softener_session' "
            "  AND COALESCE(matched_fixture_type, '') <> 'water_softener' "
            # The persisted decision memo (migration 20260814). The checked-set
            # below is in-memory, and this scans the entire recorder window where
            # each re-check costs an HA history fetch, so a settled decision is
            # written once and filtered here — BEFORE any fetch. A restart then
            # costs one query, not a fetch storm.
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
        # Dry-run: what does the importer reconstruct here?
        dry = await importer.dry_run_reconstruction(circuit, s_dt, e_dt)
        # VOLUME-SAFETY, via the shared chokepoint _probe_refusal: the
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
        # The split count is load-bearing, not a cleanup statistic: with
        # the live detector still welding on a pump-held line, this job is what
        # de-bloats those events, so a dead or starved pass must be visible in the log.
        log.info("[%s] auto-split pass: %d scanned, %d split, %d skipped "
                 "(%d decision(s) memoed)",
                 circuit, scanned, split, skipped, len(memos))
    return {"scanned": scanned, "split": split, "skipped": skipped,
            "memoed": len(memos)}
