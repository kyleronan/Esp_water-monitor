"""Below-floor draws that match the home's own labels stay real.

The meter-floor rule (feature_extractor._detect_low_flow_dribble) zeroes draws
the meter cannot measure. When the operator labels such a draw as a fixture,
that label says this home has a real fixture running below the floor (an ice
maker's timed fill is the case that motivated this). A zeroed draw that closely
matches at least ``MIN_TWINS`` of those labels of ONE type is treated the way a
label treats it: kept real at its registered volume and named that type
(matched_via ``label_twin``). The volume is not corrected — below the floor the
meter under-registers, and this keeps exactly what a label would keep.

Per home by construction: no fixture numbers live here, only how close a draw
must be to the home's own labelled ones.
"""
from __future__ import annotations

import sqlite3
from collections import Counter
from typing import List, Mapping, Optional, Sequence

MIN_TWINS: int = 2
# Relative tolerances. Duration and rate are what a timed valve repeats; the
# pressure dip separates it from a look-alike on another line (a same-length,
# same-rate draw dipping 1.3 PSI is not the fridge dipping 12).
_TOL_DURATION = 0.15
_TOL_RATE = 0.25
_TOL_DELTA_P = 0.25

_COLS = ("id, user_fixture_type, duration_seconds, volume_litres, "
         "avg_flow_lpm, true_avg_flow_lpm, peak_flow_lpm, pressure_delta_psi")


def _rate(r: Mapping) -> Optional[float]:
    v = r.get("true_avg_flow_lpm")
    return v if v is not None else r.get("avg_flow_lpm")


def _close(a, b, tol: float) -> bool:
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return False
    return abs(a - b) <= tol * abs(b)


def is_below_floor(r: Mapping, min_flow_lpm: float) -> bool:
    from .feature_extractor import _detect_low_flow_dribble
    return _detect_low_flow_dribble(
        r.get("volume_litres"), r.get("avg_flow_lpm"), r.get("pressure_delta_psi"),
        min_flow_lpm=min_flow_lpm, true_avg_flow_lpm=r.get("true_avg_flow_lpm"),
        peak_flow_lpm=r.get("peak_flow_lpm"))


def load_exemplars(conn: sqlite3.Connection, circuit: str) -> List[dict]:
    """Human labels on draws the floor rule would zero. Machine-propagated and
    ignored rows are not the operator's word, so they never count."""
    from .feature_extractor import _circuit_min_flow
    from .tinymodel import MACHINE_LABEL_SOURCES
    try:
        rows = conn.execute(
            f"SELECT {_COLS} FROM events WHERE circuit = ? "
            "AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
            "AND COALESCE(user_ignored,0) = 0 "
            f"AND COALESCE(fixture_label_source,'direct') NOT IN "
            f"({','.join('?' * len(MACHINE_LABEL_SOURCES))}) "
            # Loose prefilter (1.2 covers both meter floors); the detector decides.
            "AND COALESCE(peak_flow_lpm, avg_flow_lpm, 0) < 1.2",
            (circuit, *MACHINE_LABEL_SOURCES)).fetchall()
    except sqlite3.OperationalError:            # pre-migration schema
        return []
    floor = _circuit_min_flow(conn, circuit)
    return [dict(r) for r in rows if is_below_floor(dict(r), floor)]


def twin_type(r: Mapping, exemplars: Sequence[Mapping]) -> Optional[str]:
    """The type ``r`` matches at least MIN_TWINS exemplars of, or None (also on
    a tie between types)."""
    if not exemplars:
        return None
    hits: Counter = Counter()
    for e in exemplars:
        if r.get("id") is not None and e.get("id") == r.get("id"):
            continue
        if (_close(r.get("duration_seconds"), e.get("duration_seconds"), _TOL_DURATION)
                and _close(_rate(r), _rate(e), _TOL_RATE)
                and _close(r.get("pressure_delta_psi"), e.get("pressure_delta_psi"),
                           _TOL_DELTA_P)):
            hits[e["user_fixture_type"]] += 1
    top = hits.most_common(2)
    if not top or top[0][1] < MIN_TWINS or (len(top) > 1 and top[1][1] == top[0][1]):
        return None
    return top[0][0]


def twin_for(r: Mapping, exemplars: Sequence[Mapping],
             min_flow_lpm: float) -> Optional[str]:
    """``twin_type`` for a draw the floor rule would zero; None otherwise."""
    if not exemplars or not is_below_floor(r, min_flow_lpm):
        return None
    return twin_type(r, exemplars)


def twin_for_event(conn: sqlite3.Connection, circuit: str,
                   r: Mapping) -> Optional[str]:
    """``twin_for`` with the exemplars loaded only when the draw is below the floor."""
    from .feature_extractor import _circuit_min_flow
    if not is_below_floor(r, _circuit_min_flow(conn, circuit)):
        return None
    return twin_type(r, load_exemplars(conn, circuit))
