"""Same-circuit event-overlap guard.

INVARIANT: one circuit can have at most one event at any instant — a single
per-circuit detector state machine cannot produce two concurrent draws, so any
same-circuit overlap means the same water was recorded twice. Event ids are
UUID5 over (circuit, start_ts), so the ON CONFLICT(id) dedup only catches EXACT
same-start re-imports; a 0.3 s boundary shift (live detector vs importer) keeps
both rows. The 2026-07 pump incident weaponized it — recharge-blip "wrapper"
events swallowed real draws that were ALSO recorded separately (~127 L
double-counted; verified against raw flow: 2026-07-20 10:03 UTC, wrapper
f5cd02c7 3.92 L vs real flush 5268d5e7 3.82 L, one draw on the meter).

Overlaps are PERMITTED by design: insertion is never blocked here. The guard
only decides whose volume counts.

Resolution policy (shared by the live guard and the one-shot cleanup):
  * WRAPPER: an event whose span CONTAINS (>=70%) the other member(s) and
    whose raw volume reconciles with theirs (within 40% of the larger side)
    AND whose UNACCOUNTED remainder (raw minus the de-duplicated child volumes)
    is at most _FULL_DUPLICATE_REMAINDER_FRACTION of its own raw volume
    describes the same water — its effective volume is zeroed through the
    ledger chokepoint with match_rejection_reason='overlap_duplicate', and the
    tight member(s) keep theirs.
    The gate is on VOLUME, not span. There is NO span-coverage gate:
    _union_coverage is computed only to be printed in the decision's log line
    and audit row. Span coverage is measurably the WRONG discriminator (see the
    constants block below: the good case covers 47% of the wrapper, the 704.7 L
    bad case 72%), so introducing a span gate would zero real water.
  * PARTIALLY-COVERED WRAPPER: children that do not span the wrapper only
    account for part of it — the wrapper keeps the UNCOVERED remainder (raw
    minus the de-duplicated child volumes) instead of being zeroed outright.
    Zeroing these dropped 704.7 L of real irrigation on 2026-07-25. Child
    volumes are de-duplicated by span nesting first, so equal-start / nested
    members are subtracted once.
  * USER-LABELED wrappers are never zeroed — audit row only.
  * A wrapper reduced to a remainder is RE-EXAMINED on later writes, so a group
    completed by a child that arrived after the wrapper still resolves. Its
    volume moves in EITHER direction (a removed child must be reabsorbed, or that
    water leaves the books), capped at the wrapper's own raw volume. But a wrapper
    another verdict reduced — phantom / cross-talk / dribble / degraded — may only
    be lowered here: this module never reads history, so it cannot know a
    measurement was wrong, and raising one would override that verdict and clear
    its flag.
  * AMBIGUOUS partial overlaps keep both volumes (over-count + flag beats
    silently dropping possibly-real water) — audit row only.
Every decision writes an overlap_audit row.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from .database import (
    apply_effective_volume,
    compute_daily_summary,
    local_day_of,
    mark_daily_summary_dirty)

log = logging.getLogger(__name__)

OVERLAP_DUPLICATE_REASON = "overlap_duplicate"
# 0.70, not 0.90: the verified 10:03 incident wrapper only contains 77% of the
# real flush's span (the genuine draw's tail extends past the wrapper's close).
# Volume reconciliation is the strong second gate; 0.70 span containment matches
# the observed physics. Public spelling — the importer's containment rule
# (historical_importer._split_period_around_rows) asks the SAME question of the
# same spans and must read this threshold from here, not keep its own copy.
CONTAINMENT_FRACTION: float = 0.70
_CONTAINMENT_FRACTION = CONTAINMENT_FRACTION
_VOL_TOLERANCE = 0.40
# The two numbers the importer's containment rule, the reprocess probe and the
# overlap summary all share. They live HERE (a module that imports nothing from
# the app at import time) so the importer never has to import reprocess, and so
# "the same water" means one thing everywhere:
#   * VOLUME_COVERAGE_FRACTION — stored rows (or rebuilt flow) account for a
#     span's water once they reach this share of it.
#   * OVERLAP_NEGLIGIBLE_L — below this the importer will not mint a remainder
#     event, and an overlap group counts as resolved ("below what the importer
#     would even record as a draw").
VOLUME_COVERAGE_FRACTION: float = 0.9
OVERLAP_NEGLIGIBLE_L: float = 0.20
# How much of the wrapper's water may go UNACCOUNTED FOR by its children and
# still be called the same draw. Derived from the two verified incidents, which
# span coverage almost identically and are separated cleanly only by this
# number:
#   * 2026-07-20 10:03 (confirmed one draw on the meter): wrapper 3.92 L vs
#     child 3.82 L — remainder 2.6%.
#   * 2026-07-25 02:00 irrigation (704.7 L of REAL water lost): wrapper
#     3536.6 L vs children 2831.9 L — remainder 20%.
# Span coverage is the intuitive discriminator and is the WRONG one: the good
# case covers 47% of the wrapper, the bad one 72%. Per-event volume accuracy is
# ±1% (2026-08-02 audit), so a few percent is re-recording noise while 20% is a
# span no child accounts for. Above this fraction the wrapper keeps the
# remainder instead of being zeroed outright; union coverage is still recorded
# in the audit row for diagnosis.
_FULL_DUPLICATE_REMAINDER_FRACTION = 0.10
# Litre comparisons; stored volumes are rounded to 3 dp upstream.
_EPS = 1e-6

# Every read and write here assumes the pin columns (verdict_pin,
# verdict_pin_veff, verdict_pin_set_at) exist:
# db_migrations._ensure_verdict_pin_columns runs at the top of BOTH 20260817 and
# 20260818 (hoisted DDL — the migration numbers are deliberately NOT swapped;
# see that helper's docstring for why a swap bricks a DB stamped exactly
# 20260817).
_EVENT_COLS = ("id, circuit, start_ts, end_ts, volume_litres, "
               "volume_litres_effective, user_fixture_type, user_reviewed, "
               "match_rejection_reason, verdict_pin, verdict_pin_veff")


def _ts(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _span(row) -> Optional[tuple]:
    s = _ts(row["start_ts"])
    e = _ts(row["end_ts"]) or s
    return (s, e) if s is not None else None


def _contained_fraction(inner, outer) -> float:
    """Fraction of `inner`'s span inside `outer`'s span (1.0 for instants)."""
    lo = max(inner[0], outer[0])
    hi = min(inner[1], outer[1])
    dur = (inner[1] - inner[0]).total_seconds()
    if dur <= 0:
        return 1.0 if outer[0] <= inner[0] <= outer[1] else 0.0
    return max(0.0, (hi - lo).total_seconds()) / dur


def contained_fraction(inner, outer) -> float:
    """Public spelling of ``_contained_fraction`` for the importer."""
    return _contained_fraction(inner, outer)


def _union_coverage(outer, spans) -> float:
    """Fraction of `outer`'s span covered by the UNION of `spans`.

    Union, not sum: nested/overlapping children must count once (e.g. a
    softener-regen wrapper with 20 nested children).
    """
    lo, hi = outer
    total = (hi - lo).total_seconds()
    if total <= 0:
        return 1.0
    clipped = sorted((max(s, lo), min(e, hi)) for s, e in spans
                     if e > lo and s < hi)
    covered, merged = 0.0, []
    for s, e in clipped:
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    for s, e in merged:
        covered += (e - s).total_seconds()
    return min(1.0, covered / total)


def _top_level(children: List[dict], spans: dict) -> List[dict]:
    """Drop children that sit inside another child, so their volume is counted
    ONCE. Three events sharing an exact start timestamp defeat a strict
    `child.start > parent.start` containment test, letting a wrapper keep its
    volume while 20 nested children keep theirs (+114.6 L double-counted).

    Nesting is decided with `>=` containment plus deterministic tiebreaks
    (longer span wins; then larger volume; then id) so equal-start — and even
    equal-span — pairs resolve the same way every run.
    """
    def _rank(r):
        s, e = spans[r["id"]]
        return ((e - s).total_seconds(), float(r["volume_litres"] or 0.0),
                str(r["id"]))

    out = []
    for c in children:
        inside_other = any(
            o["id"] != c["id"]
            and _contained_fraction(spans[c["id"]], spans[o["id"]])
            >= _CONTAINMENT_FRACTION
            and _rank(o) > _rank(c)
            for o in children)
        if not inside_other:
            out.append(c)
    return out


def find_overlap_groups(conn: sqlite3.Connection,
                        circuit: Optional[str] = None) -> List[List[dict]]:
    """Transitive same-circuit overlap groups, oldest first."""
    where = "WHERE end_ts IS NOT NULL"
    params: list = []
    if circuit:
        where += " AND circuit = ?"
        params.append(circuit)
    rows = [dict(r) for r in conn.execute(
        f"SELECT {_EVENT_COLS} FROM events {where} "
        "ORDER BY circuit, start_ts", params)]
    groups: List[List[dict]] = []
    cur: List[dict] = []
    cur_end: Optional[datetime] = None
    cur_circuit: Optional[str] = None
    for r in rows:
        span = _span(r)
        if span is None:
            continue
        s, e = span
        if cur and r["circuit"] == cur_circuit and cur_end and s < cur_end:
            cur.append(r)
            cur_end = max(cur_end, e)
        else:
            if len(cur) > 1:
                groups.append(cur)
            cur = [r]
            cur_end = e
            cur_circuit = r["circuit"]
    if len(cur) > 1:
        groups.append(cur)
    return groups


def _audit(conn, circuit: str, wrapper_id: str, kept_ids: List[str],
           vol_zeroed: float, resolution: str, source: str) -> None:
    # A re-application refreshes the row (kept ids, litres, timestamp) and
    # revives a stale one, so the History "counted by" chips point at the
    # children as they stand now. The UNIQUE key is (wrapper, resolution).
    conn.execute(
        "INSERT INTO overlap_audit "
        "(circuit, wrapper_event_id, kept_event_ids, vol_zeroed, "
        " resolution, source, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(wrapper_event_id, resolution) DO UPDATE SET "
        "  kept_event_ids = excluded.kept_event_ids, vol_zeroed = excluded.vol_zeroed, "
        "  source = excluded.source, created_ts = excluded.created_ts, "
        "  stale_reason = NULL, stale_at = NULL",
        (circuit, wrapper_id, json.dumps(kept_ids), round(vol_zeroed, 3),
         resolution, source, datetime.now(timezone.utc).isoformat()))


def _refresh_daily_summary(conn: sqlite3.Connection, circuit: str,
                           start_ts, *, where: str) -> None:
    """Recompute ``start_ts``'s local daily_summary after a volume-changing write.

    The drift class that costs (17 of 92 days out by 468.5 L, worst day
    -93.2 L) is exactly "a volume moved and the day's cached summary did not",
    so a failure here must never be SILENT and must never be the last word: it
    is logged, and the day is left marked dirty so the pruner's
    ``drain_daily_summary_dirty`` pass recomputes it. Still best-effort by
    contract — a summary refresh may not break a de-duplication that has already
    written the ledger.
    """
    day = local_day_of(start_ts)
    if not day:
        return
    try:
        compute_daily_summary(conn, circuit, day)
    except Exception as e:                                      # noqa: BLE001
        log.warning("[%s] daily summary for %s not recomputed after %s (%s) — "
                    "left marked dirty for the pruner's drain pass",
                    circuit, day, where, e)
        mark_daily_summary_dirty(conn, circuit, start_ts)


def resolve_group(conn: sqlite3.Connection, group: List[dict],
                  source: str) -> Dict[str, Any]:
    """Apply the resolution policy to one overlap group. Returns counters.
    Idempotent: an already-zeroed wrapper (mrr='overlap_duplicate') is a
    no-op, and audit rows are INSERT OR IGNORE on the wrapper id."""
    stats = {"wrappers_zeroed": 0, "flag_only": 0, "ambiguous": 0,
             "partial_remainder": 0, "litres_recovered": 0.0}
    spans = {r["id"]: _span(r) for r in group}
    # Largest span first — the wrapper is the superset.
    ordered = sorted(group, key=lambda r: (
        (spans[r["id"]][1] - spans[r["id"]][0]).total_seconds()), reverse=True)
    resolved = False
    for w in ordered:
        w_span = spans[w["id"]]
        contained = [r for r in group if r["id"] != w["id"]
                     and _contained_fraction(spans[r["id"]], w_span)
                     >= _CONTAINMENT_FRACTION]
        if not contained:
            continue
        vol_w = float(w["volume_litres"] or 0.0)
        # Count each child's water ONCE. This sum is the subtrahend for the
        # partial-coverage remainder, so the dedup must run FIRST — the
        # ordering is load-bearing.
        top = _top_level(contained, spans)
        vol_sum = sum(float(r["volume_litres"] or 0.0) for r in top)
        if vol_w <= 0 or vol_sum <= 0:
            continue
        # How much of the WRAPPER do the children actually account for? A
        # wrapper zeroed while its children start 42 minutes in silently
        # dropped 704.7 L of real irrigation.
        coverage = _union_coverage(w_span, [spans[r["id"]] for r in top])
        reconciles = abs(vol_w - vol_sum) <= _VOL_TOLERANCE * max(vol_w, vol_sum)
        remainder = max(0.0, round(vol_w - vol_sum, 3))
        # Full zero only when the children account for essentially ALL of the
        # wrapper's water. Otherwise it keeps the unaccounted remainder —
        # over-count-and-flag is this module's tie-break, and dropping metered
        # water is the worse error.
        full_duplicate = reconciles and (
            vol_w <= 0 or remainder / vol_w <= _FULL_DUPLICATE_REMAINDER_FRACTION)
        if not full_duplicate and remainder <= 0:
            continue          # nothing to keep and not a clean duplicate
        kept = [r["id"] for r in top]
        # Only a FULLY-ZEROED wrapper is finished. A wrapper merely reduced to a
        # remainder is re-examined against the current children below — children
        # arriving later must still be able to shrink it — and may only ever
        # shrink.
        already_zeroed = (w["match_rejection_reason"] == OVERLAP_DUPLICATE_REASON
                          and float(w["volume_litres_effective"] or 0.0) <= 0.0)
        if already_zeroed:
            resolved = True                       # already handled (idempotent)
            break
        if (str(w["user_fixture_type"] or "").strip()
                or w["user_reviewed"]):
            _audit(conn, w["circuit"], w["id"], kept, 0.0,
                   "user_labeled_flag_only", source)
            stats["flag_only"] += 1
            resolved = True
            break
        prior_eff = float(w["volume_litres_effective"]
                          if w["volume_litres_effective"] is not None
                          else vol_w)
        new_eff = min(0.0 if full_duplicate else remainder, vol_w)
        # Which direction may this move?
        #
        # This function never reads recorder history; it only subtracts stored
        # child volumes from a stored wrapper volume. So it can never discover
        # that an earlier measurement was wrong — that is reprocess's job, and it
        # raises volumes through the event-write path, not here.
        #
        #   * Prior reduction was OURS: recompute freely, up or down, capped at
        #     the row's own raw volume. A child removed by reprocess must let the
        #     wrapper reabsorb the litres that child was accounting for, or that
        #     water silently leaves the books.
        #   * Prior reduction came from ANOTHER verdict (phantom / cross-talk /
        #     dribble / degraded, all of which record veff below raw): only lower.
        #     Raising it would override a detector that has already judged this
        #     raw volume unreal — and the UPDATE below would clear that detector's
        #     flag while doing it (measured: ~10.9 L put back onto 10
        #     phantom-zeroed wrappers exactly this way).
        ours = (w["match_rejection_reason"] == OVERLAP_DUPLICATE_REASON
                or w.get("verdict_pin") == OVERLAP_DUPLICATE_REASON)
        foreign_reduction = not ours and prior_eff < vol_w - _EPS
        if ours and abs(new_eff - prior_eff) <= _EPS:
            resolved = True                       # already correct
            break
        if foreign_reduction and new_eff >= prior_eff - _EPS:
            log.debug("[%s] overlap wrapper %s left alone: %s already reduced it "
                      "to %.2f L and de-duplication would not lower that",
                      w["circuit"], w["id"],
                      w["match_rejection_reason"] or "another verdict", prior_eff)
            resolved = True
            break
        # Wrappers are NOT phantoms: their water is real, merely counted by
        # another row. Marking them phantom drags them into the phantom repair
        # (which restores on a real ΔP — exactly what a wrapper of a real draw
        # has) and the phantom pill. The verdict lives in the PIN (preserved
        # across re-stores) and the UI keys on match_rejection_reason.
        conn.execute(
            "UPDATE events SET is_pressure_restoration_phantom = 0, "
            "  volume_litres_effective = ?, "
            "  volume_estimation_method = ?, excluded_from_training = 1, "
            "  match_rejection_reason = ?, matched_fixture_type = NULL, "
            "  matched_via = NULL, verdict_pin = ?, verdict_pin_veff = ?, "
            "  verdict_pin_set_at = ? WHERE id = ?",
            (new_eff, OVERLAP_DUPLICATE_REASON, OVERLAP_DUPLICATE_REASON,
             OVERLAP_DUPLICATE_REASON, new_eff,
             datetime.now(timezone.utc).isoformat(), w["id"]))
        apply_effective_volume(conn, w["id"], w["circuit"], w["start_ts"],
                               new_eff)
        _audit(conn, w["circuit"], w["id"], kept, prior_eff - new_eff,
               "wrapper_zeroed" if full_duplicate else "wrapper_partial_remainder",
               source)
        _refresh_daily_summary(conn, w["circuit"], w["start_ts"],
                               where="overlap de-duplication")
        stats["wrappers_zeroed"] += 1
        if not full_duplicate:
            stats["partial_remainder"] += 1
        stats["litres_recovered"] += prior_eff - new_eff
        # A rise reads as a reabsorb, not a negative de-duplication: logging
        # "-3.57 L de-duplicated" is how an override bug hides in plain sight.
        moved = prior_eff - new_eff
        verb = ("zeroed" if full_duplicate
                else "reduced to remainder" if moved >= 0
                else "reabsorbed a removed child's water")
        log.info("[%s] overlap wrapper %s (%s): %s — %.2f L %s, "
                 "%.2f L kept (coverage %.0f%%), children %s",
                 w["circuit"], verb, source, w["id"], abs(moved),
                 "de-duplicated" if moved >= 0 else "restored", new_eff,
                 100.0 * coverage, kept)
        resolved = True
        break
    if not resolved:
        anchor = group[0]
        _audit(conn, anchor["circuit"], anchor["id"],
               [r["id"] for r in group[1:]], 0.0,
               "flagged_ambiguous", source)
        stats["ambiguous"] += 1
    return stats


def guard_new_event(conn: sqlite3.Connection, event_id: str, circuit: str,
                    start_ts: str, end_ts) -> None:
    """Live/import write guard: after an event is written, resolve any
    same-circuit overlap it created or completed. Insertion is never blocked
    here — overlaps are permitted by design; the guard only decides whose volume
    counts (symmetric across orderings). Best-effort by contract: a guard failure
    must never break the write.

    Runs on every write that has an end_ts, not only genuinely-new rows: the
    write that completes a group is often an upsert of a row that already
    existed. Returns immediately when fewer than two rows share the span, so the
    usual cost is one indexed query. Refusing an outright duplicate is
    find_overlapping_event's job (database.py), upstream of this."""
    if not end_ts:
        return
    rows = [dict(r) for r in conn.execute(
        f"SELECT {_EVENT_COLS} FROM events "
        "WHERE circuit = ? AND end_ts IS NOT NULL "
        "  AND start_ts < ? AND end_ts > ?",
        (circuit, end_ts, start_ts))]
    if len(rows) < 2:
        return
    resolve_group(conn, rows, source="live_guard")
    conn.commit()


def reevaluate_event(conn: sqlite3.Connection, event_id: str,
                     source: str = "reevaluate") -> Optional[dict]:
    """Re-derive one row's overlap standing against the rows that overlap it
    NOW. A pinned wrapper that no longer contains any other row has lost its
    covering children: the pin is RELEASED and the water comes back to this row
    (it is the only record of that draw again). Otherwise the group is resolved
    with the normal policy (the guard's own reduction may move up or down; a
    foreign reduction only down)."""
    row = conn.execute(f"SELECT {_EVENT_COLS} FROM events WHERE id = ?",
                       (event_id,)).fetchone()
    if row is None or not row["end_ts"]:
        return None
    row = dict(row)
    group = [dict(r) for r in conn.execute(
        f"SELECT {_EVENT_COLS} FROM events "
        "WHERE circuit = ? AND end_ts IS NOT NULL AND start_ts < ? AND end_ts > ?",
        (row["circuit"], row["end_ts"], row["start_ts"]))]
    pinned = row.get("verdict_pin") == OVERLAP_DUPLICATE_REASON
    if pinned:
        w_span = _span(row)
        still_covering = [r for r in group if r["id"] != row["id"]
                          and _span(r) is not None and w_span is not None
                          and _contained_fraction(_span(r), w_span) >= _CONTAINMENT_FRACTION]
        if not still_covering:
            return release_verdict_pin(conn, row, reason="wrapper_released")
    if len(group) < 2:
        return None
    return resolve_group(conn, group, source=source)


def reevaluate_containing_wrappers(conn: sqlite3.Connection, circuit: str,
                                   start_ts, end_ts, source: str) -> int:
    """After a row over ``[start_ts, end_ts]`` changed or vanished, re-derive
    every PINNED wrapper on the circuit whose span intersects it. One
    indexed read (circuit, verdict_pin); returns how many were re-examined."""
    if not start_ts:
        return 0
    end_ts = end_ts or start_ts
    ids = [r[0] for r in conn.execute(
        "SELECT id FROM events WHERE circuit = ? AND verdict_pin IS NOT NULL "
        "  AND end_ts IS NOT NULL AND start_ts < ? AND end_ts > ?",
        (circuit, end_ts, start_ts)).fetchall()]
    for wid in ids:
        reevaluate_event(conn, wid, source=source)
    return len(ids)


def release_verdict_pin(conn: sqlite3.Connection, row: dict, *,
                        reason: str) -> dict:
    """The covering children are gone: this row is the only record of its draw
    again. Clear the pin; if the reduction was the guard's own, restore the raw
    volume through the ledger chokepoint and clear the reason / exclusion (a
    user Ignore still excludes). A FOREIGN reduction (phantom,
    cross-talk, dribble …) is left exactly as that detector decided — only the
    pin columns are cleared. The wrapper's live audit rows are marked stale with
    ``reason`` (MARK, never delete — provenance)."""
    ours = row.get("match_rejection_reason") == OVERLAP_DUPLICATE_REASON
    raw = float(row.get("volume_litres") or 0.0)
    if ours:
        conn.execute(
            "UPDATE events SET verdict_pin = NULL, verdict_pin_veff = NULL, "
            "  verdict_pin_set_at = NULL, volume_litres_effective = ?, "
            "  volume_estimation_method = 'raw', match_rejection_reason = NULL, "
            "  excluded_from_training = CASE WHEN COALESCE(user_ignored, 0) = 1 "
            "                              THEN 1 ELSE 0 END "
            "WHERE id = ?", (raw, row["id"]))
        apply_effective_volume(conn, row["id"], row["circuit"], row["start_ts"], raw)
    else:
        conn.execute(
            "UPDATE events SET verdict_pin = NULL, verdict_pin_veff = NULL, "
            "  verdict_pin_set_at = NULL WHERE id = ?", (row["id"],))
    conn.execute(
        "UPDATE overlap_audit SET stale_reason = COALESCE(stale_reason, ?), "
        "  stale_at = COALESCE(stale_at, ?) WHERE wrapper_event_id = ? "
        "  AND stale_reason IS NULL",
        (reason, datetime.now(timezone.utc).isoformat(), row["id"]))
    _refresh_daily_summary(conn, row["circuit"], row["start_ts"],
                           where="overlap pin release")
    log.info("[%s] overlap pin released on %s (%s): %s", row["circuit"], row["id"],
             reason, ("%.2f L restored" % raw) if ours else "other verdict kept")
    return {"released": row["id"], "restored_l": raw if ours else 0.0,
            "reason": reason}


def group_excess_litres(group: List[dict]) -> float:
    """Litres this group still counts twice: everything APPLIED to the
    hourly ledger beyond the largest member (one meter, one draw)."""
    applied = sorted((float(r.get("hourly_volume_applied_litres") or 0.0) for r in group),
                     reverse=True)
    return round(sum(applied[1:]), 2) if len(applied) > 1 else 0.0


def classify_group(conn: sqlite3.Connection, group: List[dict]) -> str:
    """One vocabulary for the offline scanner and the in-app surfaces:
    ``resolved`` (a de-duplication stands AND nothing meaningful is still counted
    twice), ``double_zeroed`` (two members zeroed), ``user_flagged`` (a
    user-labelled wrapper keeps its litres by policy), ``ambiguous`` (partial
    overlap, both kept), else ``unresolved``. An audit row alone is NOT proof of
    resolution — 49 wrappers that had to be restored all had one."""
    zeroed = [r for r in group if r.get("match_rejection_reason") == OVERLAP_DUPLICATE_REASON
              and float(r.get("volume_litres_effective") or 0.0) <= OVERLAP_NEGLIGIBLE_L]
    excess = group_excess_litres(group)
    if len(zeroed) > 1:
        return "double_zeroed"
    if excess < OVERLAP_NEGLIGIBLE_L:
        return "resolved"
    if any(str(r.get("user_fixture_type") or "").strip() for r in group):
        return "user_flagged"
    try:
        ph = ",".join("?" * len(group))
        res = {r[0] for r in conn.execute(
            f"SELECT DISTINCT resolution FROM overlap_audit WHERE wrapper_event_id IN ({ph}) "
            "AND stale_reason IS NULL", [r["id"] for r in group]).fetchall()}
    except sqlite3.Error:
        res = set()
    if "flagged_ambiguous" in res:
        return "ambiguous"
    return "unresolved"


def summarize_overlap_groups(conn: sqlite3.Connection, circuit: Optional[str] = None,
                             retention_hours: Optional[float] = None) -> List[dict]:
    """Every same-circuit overlap group with what the operator needs to decide
    about it: span, members, litres counted twice, state, and whether HA
    history still reaches it (``retention_hours``, the caller passes
    reprocess._SPLIT_LOOKBACK_H — this module must not import reprocess)."""
    where = "WHERE end_ts IS NOT NULL"
    params: list = []
    if circuit:
        where += " AND circuit = ?"
        params.append(circuit)
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT id, circuit, start_ts, end_ts, volume_litres, volume_litres_effective, "
            "       hourly_volume_applied_litres, user_fixture_type, user_classified, "
            "       user_ignored, match_rejection_reason, verdict_pin "
            f"FROM events {where} ORDER BY circuit, start_ts", params)]
    except sqlite3.Error:
        return []
    cutoff = None
    if retention_hours:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=retention_hours)).isoformat()
    out: List[dict] = []
    cur: List[dict] = []
    cur_end: Optional[datetime] = None
    cur_circuit: Optional[str] = None

    def _flush(g):
        if len(g) < 2:
            return
        applied = [float(r.get("hourly_volume_applied_litres") or 0.0) for r in g]
        out.append({
            "circuit": g[0]["circuit"],
            "span_start": min(r["start_ts"] for r in g),
            "span_end": max(r["end_ts"] for r in g),
            "member_ids": [r["id"] for r in g], "n": len(g),
            "applied_l": round(sum(applied), 2), "max_l": round(max(applied), 2),
            "excess_l": group_excess_litres(g),
            "state": classify_group(conn, g),
            "within_retention": bool(cutoff and min(r["start_ts"] for r in g) >= cutoff),
            "has_user_rows": any(str(r.get("user_fixture_type") or "").strip()
                                 or r.get("user_classified") or r.get("user_ignored")
                                 for r in g),
        })

    for r in rows:
        span = _span(r)
        if span is None:
            continue
        s0, e0 = span
        if cur and r["circuit"] == cur_circuit and cur_end and s0 < cur_end:
            cur.append(r)
            cur_end = max(cur_end, e0)
        else:
            _flush(cur)
            cur, cur_end, cur_circuit = [r], e0, r["circuit"]
    _flush(cur)
    return out


def cleanup_all_overlaps(conn: sqlite3.Connection,
                         source: str = "cleanup_migration") -> Dict[str, Any]:
    """One-shot sweep over all history (also the 20260561 migration body)."""
    totals = {"groups": 0, "wrappers_zeroed": 0, "flag_only": 0,
              "ambiguous": 0, "partial_remainder": 0, "litres_recovered": 0.0}
    for group in find_overlap_groups(conn):
        s = resolve_group(conn, group, source=source)
        totals["groups"] += 1
        for k in ("wrappers_zeroed", "flag_only", "ambiguous",
                  "partial_remainder"):
            totals[k] += s[k]
        totals["litres_recovered"] += s["litres_recovered"]
    conn.commit()
    if totals["groups"]:
        log.info("overlap cleanup: %d group(s) — %d wrapper(s) zeroed "
                 "(%.1f L recovered), %d user-labeled flagged, %d ambiguous",
                 totals["groups"], totals["wrappers_zeroed"],
                 totals["litres_recovered"], totals["flag_only"],
                 totals["ambiguous"])
    return totals
