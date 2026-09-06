"""Same-circuit event-overlap guard (dev28, plan overlap-guard-invariant).

INVARIANT: one circuit can have at most one event at any instant — a single
per-circuit detector state machine cannot produce two concurrent draws, so
any same-circuit overlap means the same water was recorded twice. Nothing
enforced this before: event ids are UUID5 over (circuit, start_ts), so the
ON CONFLICT(id) dedup only catches EXACT same-start re-imports; a 0.3 s
boundary shift (live detector vs importer) kept both rows. The 2026-07 pump
incident weaponized it — recharge-blip "wrapper" events swallowed real draws
that were ALSO recorded separately (~127 L double-counted; verified against
raw flow: 2026-07-20 10:03 UTC, wrapper f5cd02c7 3.92 L vs real flush
5268d5e7 3.82 L, one draw on the meter).

Resolution policy (shared by the live guard and the one-shot cleanup):
  * WRAPPER: an event whose span CONTAINS (>=70%) the other member(s) and
    whose raw volume reconciles with theirs (within 40% of the larger side)
    AND which those member(s) SPAN (>=90% union coverage, dev33) describes the
    same water — its effective volume is zeroed through the §2.5 ledger
    chokepoint with match_rejection_reason='overlap_duplicate' (it joins the
    phantom flag family so hide/zero plumbing applies), and the tight
    member(s) keep theirs.
  * PARTIALLY-COVERED WRAPPER (dev33 §1.2): children that do not span the
    wrapper only account for part of it — the wrapper keeps the UNCOVERED
    remainder (raw minus the de-duplicated child volumes) instead of being
    zeroed outright. Zeroing these dropped 704.7 L of real irrigation on
    2026-07-25. Child volumes are de-duplicated by span nesting first
    (dev33 §1.3), so equal-start / nested members are subtracted once.
  * USER-LABELED wrappers are never zeroed — audit row only.
  * dev55: a wrapper reduced to a remainder is RE-EXAMINED on later writes, so a
    group completed by a child that arrived after the wrapper still resolves. Its
    volume moves in EITHER direction (a removed child must be reabsorbed, or that
    water leaves the books), capped at the wrapper's own raw volume. But a wrapper
    another verdict reduced — phantom / cross-talk / dribble / degraded — may only
    be lowered here: this module never reads history, so it cannot know a
    measurement was wrong, and raising one would override that verdict and clear
    its flag.
  * AMBIGUOUS partial overlaps keep both volumes (over-count + flag beats
    silently dropping possibly-real water) — audit row only.
Every decision writes an overlap_audit row (cross_talk_audit precedent).
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

OVERLAP_DUPLICATE_REASON = "overlap_duplicate"
# Containment relaxed from the plan's 0.90 prior: the VERIFIED 10:03 incident
# wrapper only contains 77% of the real flush's span (the genuine draw's tail
# extends past the wrapper's close). Volume reconciliation is the strong
# second gate; 0.70 span containment matches the observed physics.
_CONTAINMENT_FRACTION = 0.70
_VOL_TOLERANCE = 0.40
# dev55 — the two numbers the importer's containment rule, the reprocess probe
# and the overlap summary all share. They live HERE (a module that imports
# nothing from the app at import time) so the importer never has to import
# reprocess, and so "the same water" means one thing everywhere:
#   * VOLUME_COVERAGE_FRACTION — stored rows (or rebuilt flow) account for a
#     span's water once they reach this share of it (was reprocess'
#     _SPLIT_MIN_VOLUME_COVERAGE; same value, now one object).
#   * OVERLAP_NEGLIGIBLE_L — below this the importer will not mint a remainder
#     event, and an overlap group counts as resolved ("below what the importer
#     would even record as a draw").
VOLUME_COVERAGE_FRACTION: float = 0.9
OVERLAP_NEGLIGIBLE_L: float = 0.20
# dev33 §1.2 — how much of the wrapper's water may go UNACCOUNTED FOR by its
# children and still be called the same draw. Derived from the two verified
# incidents, which span coverage almost identically and are separated cleanly
# only by this number:
#   * 2026-07-20 10:03 (the case this module was built for, confirmed one draw
#     on the meter): wrapper 3.92 L vs child 3.82 L — remainder 2.6%.
#   * 2026-07-25 02:00 irrigation (704.7 L of REAL water lost): wrapper
#     3536.6 L vs children 2831.9 L — remainder 20%.
# Span coverage is the intuitive discriminator and is the WRONG one: the good
# case covers 47% of the wrapper, the bad one 72%. Per-event volume accuracy is
# ±1% (2026-08-02 audit), so a few percent is re-recording noise while 20% is a
# span no child accounts for. Above this fraction the wrapper keeps the
# remainder instead of being zeroed outright; union coverage is still recorded
# in the audit row for diagnosis.
_FULL_DUPLICATE_REMAINDER_FRACTION = 0.10
# dev55 — litre comparisons; stored volumes are rounded to 3 dp upstream.
_EPS = 1e-6

_EVENT_COLS = ("id, circuit, start_ts, end_ts, volume_litres, "
               "volume_litres_effective, user_fixture_type, user_reviewed, "
               "match_rejection_reason")


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
    """Public spelling of ``_contained_fraction`` for the importer (dev55)."""
    return _contained_fraction(inner, outer)


def _union_coverage(outer, spans) -> float:
    """Fraction of `outer`'s span covered by the UNION of `spans`.

    Union, not sum: nested/overlapping children must count once (dev33 §1.3 —
    a softener-regen wrapper with 20 nested children).
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
    ONCE (dev33 §1.3: three events sharing an exact start timestamp defeated a
    strict `child.start > parent.start` containment test, and a wrapper kept its
    volume while 20 nested children kept theirs — +114.6 L double-counted).

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
    conn.execute(
        "INSERT OR IGNORE INTO overlap_audit "
        "(circuit, wrapper_event_id, kept_event_ids, vol_zeroed, "
        " resolution, source, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (circuit, wrapper_id, json.dumps(kept_ids), round(vol_zeroed, 3),
         resolution, source, datetime.now(timezone.utc).isoformat()))


def resolve_group(conn: sqlite3.Connection, group: List[dict],
                  source: str) -> Dict[str, Any]:
    """Apply the resolution policy to one overlap group. Returns counters.
    Idempotent: an already-zeroed wrapper (mrr='overlap_duplicate') is a
    no-op, and audit rows are INSERT OR IGNORE on the wrapper id."""
    from .database import (apply_effective_volume, compute_daily_summary,
                           local_day_of)
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
        # dev33 §1.3 — count each child's water ONCE. Nested children (the
        # equal-start softener case) previously inflated nothing here because
        # the sum was only used for a tolerance test, but it is now also the
        # subtrahend for the partial-coverage remainder, so the dedup must run
        # FIRST and its ordering is load-bearing.
        top = _top_level(contained, spans)
        vol_sum = sum(float(r["volume_litres"] or 0.0) for r in top)
        if vol_w <= 0 or vol_sum <= 0:
            continue
        # dev33 §1.2 — how much of the WRAPPER do the children actually
        # account for? A wrapper zeroed while its children start 42 minutes in
        # silently dropped 704.7 L of real irrigation.
        coverage = _union_coverage(w_span, [spans[r["id"]] for r in top])
        reconciles = abs(vol_w - vol_sum) <= _VOL_TOLERANCE * max(vol_w, vol_sum)
        remainder = max(0.0, round(vol_w - vol_sum, 3))
        # Full zero only when the children account for essentially ALL of the
        # wrapper's water. Otherwise it keeps the unaccounted remainder —
        # over-count-and-flag has always been this module's tie-break, and
        # dropping metered water is the worse error.
        full_duplicate = reconciles and (
            vol_w <= 0 or remainder / vol_w <= _FULL_DUPLICATE_REMAINDER_FRACTION)
        if not full_duplicate and remainder <= 0:
            continue          # nothing to keep and not a clean duplicate
        kept = [r["id"] for r in top]
        # dev55 — was: any wrapper already carrying the reason is finished. That
        # also caught a wrapper merely REDUCED TO A REMAINDER against the child
        # set as it stood at the time, so children arriving later never shrank it
        # again and the group stayed part-duplicated forever. Only a fully-zeroed
        # wrapper is finished; a remainder is re-examined against the current
        # children below, and may only ever shrink.
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
        # dev55 — which direction may this move?
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
        #     flag while doing it. Measured on 2026-09-06: the re-sweep put ~10.9 L
        #     back onto 10 phantom-zeroed wrappers exactly this way.
        ours = w["match_rejection_reason"] == OVERLAP_DUPLICATE_REASON
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
        # The phantom flag marks the ZEROED family (hide/zero UI plumbing); a
        # wrapper that keeps an uncovered remainder still carries real water,
        # so it is excluded from training but NOT flagged as zeroed.
        conn.execute(
            "UPDATE events SET is_pressure_restoration_phantom = ?, "
            "  volume_litres_effective = ?, "
            "  volume_estimation_method = ?, excluded_from_training = 1, "
            "  match_rejection_reason = ?, matched_fixture_type = NULL, "
            "  matched_via = NULL WHERE id = ?",
            (1 if full_duplicate else 0, new_eff, OVERLAP_DUPLICATE_REASON,
             OVERLAP_DUPLICATE_REASON, w["id"]))
        apply_effective_volume(conn, w["id"], w["circuit"], w["start_ts"],
                               new_eff)
        _audit(conn, w["circuit"], w["id"], kept, prior_eff - new_eff,
               "wrapper_zeroed" if full_duplicate else "wrapper_partial_remainder",
               source)
        day = local_day_of(w["start_ts"])
        if day:
            try:
                compute_daily_summary(conn, w["circuit"], day)
            except Exception:
                pass
        stats["wrappers_zeroed"] += 1
        if not full_duplicate:
            stats["partial_remainder"] += 1
        stats["litres_recovered"] += prior_eff - new_eff
        # dev55 — a rise reads as a reabsorb, not a negative de-duplication. The
        # old wording logged "-3.57 L de-duplicated", which is how the override
        # bug hid in plain sight.
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
    same-circuit overlap it created or completed. Insertion is never blocked here
    — the guard only decides whose volume counts (symmetric across orderings).
    Best-effort by contract: a guard failure must never break the write.

    dev55 — runs on every write that has an end_ts, not only genuinely-new rows:
    the write that completes a group is often an upsert of a row that already
    existed, and those groups were never resolved at all. Returns immediately
    when fewer than two rows share the span, so the usual cost is one indexed
    query. Refusing an outright duplicate is find_overlapping_event's job
    (database.py), upstream of this."""
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
