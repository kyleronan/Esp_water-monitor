"""Leak-test reopen refill — the add-on's own valve cycle, not household use.

A leak test closes the main valve, watches the isolated section decay, then
reopens it. When the valve reopens the section refills, and that refill runs
through the meter as a short flow burst: the detector logs a normal-looking
event (2026-08-04 01:56 — 9 s, 0.041 L, peak 2.95 L/min, pressure climbing back
from 52 to 56 PSI). It is real water in the pipe but it is NOT a fixture, and
it is not a sensor phantom either — the meter measured it correctly, so none of
the artifact detectors can or should fire on it. The only thing that knows what
it was is the scheduler that caused it.

So this verdict is applied OUT-OF-BAND from ``leak_test_history`` timing, the
same shape as the irrigation cross-talk reconcile: the single-event detectors
cannot reproduce it, so ``_finalize_derived_verdicts`` preserves it and the
reconcile below is idempotent and re-runnable (it re-derives every verdict from
the test rows, so a reprocess that clears one self-heals on the next sweep).

Deliberately NOT part of the ``is_pressure_restoration_phantom`` /
``is_cross_talk`` / ``is_low_flow_dribble`` flag family: those flags drive the
"Hide not-real-use events" toggle, and a refill is worth SEEING — at most one
per day, and its size is a free read on the isolated section (a refill that
suddenly grows means the section is losing more between tests). Volume is still
zeroed and training still excludes it; only the hiding is opted out of.

LEAK-SAFETY:
  * the volume cap is the demand bar the reopen-slug backstop already uses
    (``POST_RESTORE_DEMAND_L``) — above it the scheduler itself calls the test
    "a fixture was running", so this verdict must not claim the event;
  * the cap is also the ``detector_validation.SUSPECT_ZERO_LITRES`` bar, so a
    zeroed refill can never hide a litre-scale draw from the leak audit;
  * the window is bounded by the test's own reopen moment, not a heuristic;
  * ``draw_verdict == 'demand'`` disables the verdict for that test entirely;
  * per-test the tagged volume is BUDGETED to the cap, so a real draw that
    starts while the line is refilling is never absorbed;
  * user labels / manual classification / ignore intent always win.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

# ── Reopen watch window (single source of truth) ─────────────────────────────
# Owned here rather than in leak_test_scheduler because two callers need the
# same numbers: the scheduler's live post-restore watch, and this reconcile
# (which re-derives the window for tests that ran while the add-on was down, and
# for the one-time backfill over history). The scheduler re-exports these names.
#
# Measured 2026-07-26: a flapper-scale refill is ~0.04 L, a pump recharge pushes
# 0.3-0.5 L, and a toilet flushed during the test pulls 3-8 L as it finishes
# filling. 1 L sits in the gap with an order of magnitude either side.
POST_RESTORE_WATCH_S: int = 120
POST_RESTORE_LEAD_S: int = 15      # covers the <=10 s poll gap plus valve travel
POST_RESTORE_DEMAND_L: float = 1.0

# The verdict string. Distinct provenance, own UI pill, own History note filter.
LEAK_TEST_REFILL_REASON: str = "leak_test_refill"

# How far back the periodic reconcile re-derives verdicts. Generous: the sweep
# is cheap (one indexed range query per test) and a long tail costs nothing,
# while a short one would strand events from a multi-day outage.
_RECONCILE_LOOKBACK_DAYS: int = 90


def _parse_ts(raw) -> Optional[datetime]:
    """ISO timestamp → aware UTC datetime. None when unparseable."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def refill_window(run_at, duration_minutes) -> Optional[tuple]:
    """The (start, end) UTC window in which a test's reopen refill can land.

    The valve reopens once the test finishes, and ``duration_minutes`` is
    measured from ``run_at`` to exactly that point (leak_test_scheduler stamps
    it just before the reopen), so the reopen moment is run_at + duration.
    ``POST_RESTORE_LEAD_S`` absorbs the poll granularity and valve travel on the
    early side; ``POST_RESTORE_WATCH_S`` bounds the late side, matching the
    scheduler's own watch.

    Returns None when either input is missing — an unfinished or malformed test
    row must not produce a window (an open-ended one would swallow real draws).
    """
    start = _parse_ts(run_at)
    if start is None or duration_minutes is None:
        return None
    try:
        dur_s = float(duration_minutes) * 60.0
    except (TypeError, ValueError):
        return None
    if dur_s < 0:
        return None
    reopen = start + timedelta(seconds=dur_s)
    return (reopen - timedelta(seconds=POST_RESTORE_LEAD_S),
            reopen + timedelta(seconds=POST_RESTORE_WATCH_S))


def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        return any(r[1] == col
                   for r in conn.execute(f"PRAGMA table_info({table})"))
    except sqlite3.Error:
        return False


def reconcile_leak_test_refills(
    conn: sqlite3.Connection,
    circuit: Optional[str] = None,
    lookback_days: int = _RECONCILE_LOOKBACK_DAYS,
    since: Optional[str] = None,
) -> dict:
    """Tag reopen-refill events for recent leak tests. Idempotent.

    Walks ``leak_test_history`` (optionally one circuit / a custom lookback or
    an explicit ``since`` cutoff, which the one-time backfill passes as None to
    cover all history), derives each test's reopen window, and marks the events
    inside it as ``leak_test_refill`` — up to the per-test volume budget.

    Returns ``{"tagged": n, "tests_scanned": n}``.
    """
    from .database import mark_event_leak_test_refill

    if not _has_column(conn, "events", "leak_test_id"):
        return {"tagged": 0, "tests_scanned": 0}   # pre-20260570 schema

    sql = ("SELECT id, circuit, run_at, duration_minutes, draw_verdict "
           "FROM leak_test_history WHERE 1=1")
    params: list = []
    if circuit:
        sql += " AND circuit = ?"
        params.append(circuit)
    if since is not None:
        sql += " AND run_at >= ?"
        params.append(since)
    elif lookback_days:
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(days=lookback_days)).isoformat()
        sql += " AND run_at >= ?"
        params.append(cutoff)
    sql += " ORDER BY run_at"

    try:
        tests = conn.execute(sql, params).fetchall()
    except sqlite3.Error as e:
        log.warning("leak-test-refill reconcile: cannot read history: %s", e)
        return {"tagged": 0, "tests_scanned": 0}

    tagged = 0
    for t in tests:
        # A test the reopen-slug backstop called 'demand' had a fixture running,
        # so what came back through the meter is that fixture finishing — real
        # water. Claim nothing for this test.
        if (t["draw_verdict"] or "") == "demand":
            continue
        window = refill_window(t["run_at"], t["duration_minutes"])
        if window is None:
            continue
        w_start, w_end = window
        rows = conn.execute(
            "SELECT id, start_ts, volume_litres, match_rejection_reason "
            "FROM events "
            "WHERE circuit = ? AND start_ts >= ? AND start_ts <= ? "
            "ORDER BY start_ts",
            (t["circuit"], w_start.isoformat(), w_end.isoformat()),
        ).fetchall()

        # Per-test volume budget: the refill total cannot exceed the demand bar
        # (above it the test would have been called 'demand' in the first
        # place). Walking in time order and stopping at the budget means a real
        # draw that begins while the line is still refilling is never absorbed.
        budget = POST_RESTORE_DEMAND_L
        for row in rows:
            raw = float(row["volume_litres"] or 0.0)
            # An already-tagged row spent its share of the budget on a previous
            # run — charge it and move on, so re-running can't widen the claim.
            if row["match_rejection_reason"] == LEAK_TEST_REFILL_REASON:
                budget -= raw
                continue
            if raw >= budget:
                break
            if mark_event_leak_test_refill(conn, row["id"], t["circuit"],
                                           leak_test_id=t["id"]):
                budget -= raw
                tagged += 1

    if tagged:
        log.info("leak-test-refill reconcile: tagged %d event(s) across %d test(s)",
                 tagged, len(tests))
    return {"tagged": tagged, "tests_scanned": len(tests)}
