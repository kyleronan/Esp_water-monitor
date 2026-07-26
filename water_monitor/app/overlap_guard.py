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
  * WRAPPER: an event whose span CONTAINS (>=90%) the other member(s) and
    whose raw volume reconciles with theirs (within 40% of the larger side)
    describes the same water — its effective volume is zeroed through the
    §2.5 ledger chokepoint with match_rejection_reason='overlap_duplicate'
    (it joins the phantom flag family so hide/zero plumbing applies), and
    the tight member(s) keep theirs.
  * USER-LABELED wrappers are never zeroed — audit row only.
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
    from .database import apply_effective_volume, compute_daily_summary
    stats = {"wrappers_zeroed": 0, "flag_only": 0, "ambiguous": 0,
             "litres_recovered": 0.0}
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
        vol_sum = sum(float(r["volume_litres"] or 0.0) for r in contained)
        if vol_w <= 0 or vol_sum <= 0:
            continue
        if abs(vol_w - vol_sum) > _VOL_TOLERANCE * max(vol_w, vol_sum):
            continue
        kept = [r["id"] for r in contained]
        if w["match_rejection_reason"] == OVERLAP_DUPLICATE_REASON:
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
        conn.execute(
            "UPDATE events SET is_pressure_restoration_phantom = 1, "
            "  volume_litres_effective = 0, "
            "  volume_estimation_method = ?, excluded_from_training = 1, "
            "  match_rejection_reason = ?, matched_fixture_type = NULL, "
            "  matched_via = NULL WHERE id = ?",
            (OVERLAP_DUPLICATE_REASON, OVERLAP_DUPLICATE_REASON, w["id"]))
        apply_effective_volume(conn, w["id"], w["circuit"], w["start_ts"], 0)
        _audit(conn, w["circuit"], w["id"], kept, prior_eff,
               "wrapper_zeroed", source)
        day = (w["start_ts"] or "")[:10]
        if day:
            try:
                compute_daily_summary(conn, w["circuit"], day)
            except Exception:
                pass
        stats["wrappers_zeroed"] += 1
        stats["litres_recovered"] += prior_eff
        log.info("[%s] overlap wrapper zeroed (%s): %s — %.2f L recovered, "
                 "kept %s", w["circuit"], source, w["id"], prior_eff, kept)
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
    """Live/import write guard: after a genuinely-new event is inserted,
    resolve any same-circuit overlap it created. Insertion is never blocked —
    the guard only decides whose volume counts (symmetric across orderings).
    Best-effort by contract: a guard failure must never break the write."""
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
              "ambiguous": 0, "litres_recovered": 0.0}
    for group in find_overlap_groups(conn):
        s = resolve_group(conn, group, source=source)
        totals["groups"] += 1
        for k in ("wrappers_zeroed", "flag_only", "ambiguous"):
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
