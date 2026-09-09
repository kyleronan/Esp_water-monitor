"""Burst-context features: what ELSE was running nearby.

WHY THIS EXISTS
---------------
The 2026-08-22 holdout study measured the add-on's classification ladder at
36/48 against the operator's own labels, and eight of its twelve misses were
dishwasher fills. Not because a dishwasher fill is hard to measure — because it
is genuinely ambiguous ALONE. A 1.5 L, 50 s, 6 L/min draw is a dishwasher fill
or a tap run depending entirely on whether three more just like it arrive over
the next hour. Judged one at a time, the information simply is not in the row.

These nine features put it there. They are computed from the event stream's own
timing and magnitudes — no labels, no per-home fitting — so they work on a home
that has never been labelled, which is what makes them worth more than another
tuned constant. The variant-house test measured them helping MOST when
fixtures are re-shaped (+3.0 points over base features): when you can no longer
trust what a dishwasher fill looks like, the fact that it arrives in a rhythm
with its siblings is the signal that survives.

THE TWO CONFIGS
---------------
A fill's siblings arrive AFTER it, so the full picture does not exist when the
event is first classified:

* ``immature`` — trailing side only. What is knowable the moment an event ends.
* ``mature``   — both sides. Used by the deferred re-classify (~2 h later) and
  by all training.

Both are applied identically to the query event and to every neighbour, at fit
time and at serve time. That symmetry is the whole contract: a feature computed
one way in training and another way in production is a silent accuracy leak,
which is why ``_knn_transform`` exists in database.py and why there is no third,
ad-hoc variant here.

COST
----
Set-based by construction: one windowed query per call, then a linear sweep with
two moving pointers. A per-event-query shape makes sweeps O(N) expensive on this
table, so callers pass the whole batch they care about.
"""
from __future__ import annotations

import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

log = logging.getLogger(__name__)

# ── pinned window constants ─────────────────────────────────────────────────
# Fixed before any result was inspected (the 2026-08-22 study measured with
# exactly these); changing one invalidates comparison with every stored
# benchmark number, so they live in code, not in config.
NEIGHBOUR_WINDOW_S: float = 5400.0     # +/- 90 min: two dishwasher fills apart
CHAIN_GAP_S: float = 4500.0            # <= 75 min between siblings = one burst
NEAR_BIG_WINDOW_S: float = 1800.0      # +/- 30 min for "was something big on?"
HEAVY_WINDOW_S: float = 3600.0         # +/- 60 min for the appliance-pulse count
SIM_LOG_VOL: float = 0.9               # |dlog(volume)| — about a 2.5x band
SIM_LOG_PEAK: float = 0.6              # |dlog(peak flow)| — about a 1.8x band
HEAVY_MIN_L, HEAVY_MAX_L, HEAVY_MIN_PEAK = 3.0, 25.0, 8.0
GAP_CAP_MIN: float = 120.0             # "no sibling" sentinel, in minutes

# ── context exclusions ──────────────────────────────────────────────────────
# Verdicts that make a row NOT a draw but deliberately set none
# of the three artifact BITS ``compute_for_events`` filters on, so nothing but
# the reason string can exclude them. Spelled literally rather than imported:
# this module is imported by the DB worker and stays free of app-layer imports
# at module scope (the strings are OVERLAP_DUPLICATE_REASON in overlap_guard and
# LEAK_TEST_REFILL_REASON in leak_test_refill; the round-trip is asserted in
# test_dev57_ledger_verdict.py so a rename cannot drift them apart silently).
_EXCLUDED_REASONS: tuple = ("overlap_duplicate", "leak_test_refill")

FEATURE_NAMES: tuple = (
    "n_ev_30m", "n_sim_90m", "gap_prev_sim", "gap_next_sim", "burst_size",
    "burst_pos", "burst_span", "near_big_draw", "n_heavy_2h",
)

CONFIG_MATURE = "mature"
CONFIG_IMMATURE = "immature"
_CONFIGS = frozenset({CONFIG_MATURE, CONFIG_IMMATURE})


def _similar(a: dict, b: dict) -> bool:
    """Same order of magnitude in volume AND peak flow.

    Deliberately crude. This is a "could these be the same fixture repeating?"
    test, not a classifier — anything sharper would start encoding this home's
    fixtures, which is exactly what these features exist to avoid.
    """
    va, vb = max(a["v"], 0.05), max(b["v"], 0.05)
    pa, pb = max(a["pf"], 0.1), max(b["pf"], 0.1)
    return (abs(math.log(va / vb)) < SIM_LOG_VOL
            and abs(math.log(pa / pb)) < SIM_LOG_PEAK)


def compute_from_stream(stream: Sequence[dict],
                        config: str = CONFIG_MATURE,
                        targets: Optional[set] = None) -> Dict[str, dict]:
    """Core computation over an in-memory, time-ordered stream.

    ``stream`` items need ``id``, ``t`` (epoch seconds), ``v`` (litres) and
    ``pf`` (peak L/min). ``targets`` limits which ids get features (the rest
    still serve as context — that distinction is the point of the window).
    """
    if config not in _CONFIGS:
        raise ValueError(
            f"unknown burst config {config!r}; expected {sorted(_CONFIGS)}")
    forward = config == CONFIG_MATURE
    rows = sorted(stream, key=lambda r: r["t"])
    n = len(rows)
    out: Dict[str, dict] = {}
    lo = 0
    for i, e in enumerate(rows):
        t = e["t"]
        while lo < i and t - rows[lo]["t"] > NEIGHBOUR_WINDOW_S:
            lo += 1
        if targets is not None and e["id"] not in targets:
            continue
        hi = i
        if forward:
            while hi + 1 < n and rows[hi + 1]["t"] - t <= NEIGHBOUR_WINDOW_S:
                hi += 1
        neigh = [rows[j] for j in range(lo, hi + 1) if j != i]
        sims = [x for x in neigh if _similar(e, x)]

        prev_gaps = [t - x["t"] for x in sims if x["t"] < t]
        next_gaps = [x["t"] - t for x in sims if x["t"] > t]

        members = sorted(sims + [e], key=lambda x: x["t"])
        runs: List[List[dict]] = []
        cur = [members[0]]
        for x in members[1:]:
            if x["t"] - cur[-1]["t"] <= CHAIN_GAP_S:
                cur.append(x)
            else:
                runs.append(cur)
                cur = [x]
        runs.append(cur)
        run = next(r for r in runs if any(x is e for x in r))

        near = [x["v"] for x in neigh if abs(x["t"] - t) <= NEAR_BIG_WINDOW_S]
        heavy = sum(1 for x in neigh
                    if abs(x["t"] - t) <= HEAVY_WINDOW_S
                    and HEAVY_MIN_L <= x["v"] <= HEAVY_MAX_L
                    and x["pf"] >= HEAVY_MIN_PEAK)

        out[e["id"]] = {
            "n_ev_30m": sum(1 for x in neigh
                            if abs(x["t"] - t) <= NEAR_BIG_WINDOW_S),
            "n_sim_90m": len(sims),
            "gap_prev_sim": min(prev_gaps) / 60.0 if prev_gaps else GAP_CAP_MIN,
            "gap_next_sim": min(next_gaps) / 60.0 if next_gaps else GAP_CAP_MIN,
            "burst_size": len(run),
            "burst_pos": next(k for k, x in enumerate(run) if x is e),
            "burst_span": (run[-1]["t"] - run[0]["t"]) / 60.0,
            "near_big_draw": math.log1p(max(near) if near else 0.0),
            "n_heavy_2h": heavy,
        }
    return out


def _epoch(ts) -> Optional[float]:
    if ts is None:
        return None
    try:
        s = str(ts).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (TypeError, ValueError):
        return None


def _has_column(conn: sqlite3.Connection, column: str) -> bool:
    """True when ``events`` carries ``column``. ``verdict_pin`` only exists from
    migration 20260818 on, and a missing-column OperationalError here would take
    the whole burst-feature read down."""
    try:
        return any(r[1] == column for r in
                   conn.execute("PRAGMA table_info(events)").fetchall())
    except sqlite3.Error:
        return False


def compute_for_events(conn: sqlite3.Connection, circuit: str,
                       event_ids: Optional[Iterable[str]] = None,
                       config: str = CONFIG_MATURE,
                       span: Optional[tuple] = None) -> Dict[str, dict]:
    """Burst features for events on ``circuit``, read set-wise from the DB.

    ``event_ids`` selects the targets; ``span`` (start_ts, end_ts ISO strings)
    selects them by time instead. Either way the CONTEXT window is widened by
    ``NEIGHBOUR_WINDOW_S`` on both sides, because an event's neighbours are part
    of its own feature values — narrowing the read to just the targets would
    silently compute every edge event as if the stream started there.

    Artifact rows are excluded from the context: they are not draws, and
    counting them would inflate every neighbour statistic. This mirrors the pool
    filters used everywhere else. Three flag bits are not the whole set —
    ``_EXCLUDED_REASONS`` covers the two verdicts that deliberately carry NO bit:

      * ``overlap_duplicate`` — a wrapper is the SAME water as the children
        inside it, recorded twice. Left in, it is a phantom sibling of
        every child it wraps: it inflates ``n_ev_30m`` / ``n_sim_90m``, splices
        two real bursts into one, and shrinks ``gap_prev_sim``/``gap_next_sim``.
        It carries no flag by design (the phantom bit is cleared — wrappers
        are not phantoms), so the bit filter cannot see it.
      * ``leak_test_refill`` — the refill after a leak test is real water but
        not fixture usage, and it stays VISIBLE in History on purpose, so it too
        carries no bit (see ``_finalize_derived_verdicts``).

    Synchronous by design — callers submit it through ``run_db``.
    """
    targets: Optional[set] = None
    if event_ids is not None:
        targets = {str(e) for e in event_ids}
        if not targets:
            return {}
        ids = sorted(targets)
        placeholders = ",".join("?" * len(ids))
        bounds = conn.execute(
            f"SELECT MIN(start_ts), MAX(start_ts) FROM events "
            f"WHERE circuit = ? AND id IN ({placeholders})",
            (circuit, *ids)).fetchone()
    elif span is not None:
        bounds = (span[0], span[1])
    else:
        bounds = conn.execute(
            "SELECT MIN(start_ts), MAX(start_ts) FROM events WHERE circuit = ?",
            (circuit,)).fetchone()
    if not bounds or bounds[0] is None:
        return {}

    lo_t, hi_t = _epoch(bounds[0]), _epoch(bounds[1])
    if lo_t is None or hi_t is None:
        return {}
    pad = NEIGHBOUR_WINDOW_S + 60.0
    lo_iso = datetime.fromtimestamp(lo_t - pad, timezone.utc).isoformat()
    hi_iso = datetime.fromtimestamp(hi_t + pad, timezone.utc).isoformat()

    reason_ph = ",".join("?" * len(_EXCLUDED_REASONS))
    reason_sql, reason_params = "", []
    # match_rejection_reason carries the live verdict; verdict_pin (20260818)
    # carries it across a re-store, and a row can have the pin while the
    # reason has been overwritten — so both are read.
    for col in ("match_rejection_reason", "verdict_pin"):
        if _has_column(conn, col):
            reason_sql += f"  AND COALESCE({col},'') NOT IN ({reason_ph}) "
            reason_params.extend(_EXCLUDED_REASONS)
    stream = []
    for r in conn.execute(
            "SELECT id, start_ts, volume_litres, peak_flow_lpm FROM events "
            "WHERE circuit = ? AND start_ts >= ? AND start_ts <= ? "
            "  AND COALESCE(is_pressure_restoration_phantom,0) = 0 "
            "  AND COALESCE(is_low_flow_dribble,0) = 0 "
            "  AND COALESCE(is_cross_talk,0) = 0 "
            + reason_sql +
            "ORDER BY start_ts",
            (circuit, lo_iso, hi_iso, *reason_params)):
        t = _epoch(r[1])
        if t is None:
            continue
        stream.append({"id": str(r[0]), "t": t,
                       "v": float(r[2] or 0.0), "pf": float(r[3] or 0.0)})
    if not stream:
        return {}
    if targets is None and span is not None:
        s_lo, s_hi = _epoch(span[0]), _epoch(span[1])
        if s_lo is not None and s_hi is not None:
            targets = {x["id"] for x in stream if s_lo <= x["t"] <= s_hi}
    return compute_from_stream(stream, config=config, targets=targets)


NEUTRAL: dict = {"n_ev_30m": 0, "n_sim_90m": 0, "gap_prev_sim": GAP_CAP_MIN,
                 "gap_next_sim": GAP_CAP_MIN, "burst_size": 1, "burst_pos": 0,
                 "burst_span": 0.0, "near_big_draw": 0.0, "n_heavy_2h": 0}


def attach(rows: Sequence[dict], features: Dict[str, dict],
           id_key: str = "id") -> None:
    """Merge computed features onto row dicts IN PLACE.

    Missing ids get the neutral "nothing nearby" values rather than NULLs: a
    row the window could not cover is genuinely isolated as far as the model can
    tell, and NaNs here would be indistinguishable from a feature outage.
    """
    for r in rows:
        r.update(features.get(str(r.get(id_key)), NEUTRAL))
