"""Per-home rule calibration (Phase 1 — fit-once-at-activation, then frozen).

The structural-rules-tier bands in :mod:`event_rules` ship as developer-tuned
module constants shaped by one home's data. At **activation** (labelling → live)
those bands are re-fit from THIS home's labelled events and stored, frozen, in
the ``rule_calibration`` table. The :mod:`event_rules` predicates read the fitted
values through an optional ``calib`` dict, falling back to their module-default
constant for any band this home lacks enough labels to fit.

Design notes:

* **Fit once, then freeze.** Calibration is written only at activation and on an
  explicit re-train — never on ordinary reclassify or on incoming live events. A
  locked baseline is what lets future leak / odd-usage detection treat a
  non-conforming event as anomalous instead of silently learning it as normal.
* **No import cycle.** This module never imports :mod:`event_rules`. It only
  produces / loads the dict of overridable keys; ``event_rules`` owns the
  defaults and the fallback. The key names here mirror the ``event_rules``
  constant names (sans leading underscore).
* **Graceful fallback.** A fixture type with fewer than ``MIN_LABELS_FOR_FIT``
  clean labels contributes nothing to the dict, so its rule keeps the shipped
  default — no type is ever left worse off than today.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# A fixture type needs at least this many clean (non-excluded) labels before we
# trust a per-home fit over the shipped default.
MIN_LABELS_FOR_FIT = 5

# Robust percentiles for range fits; ranges are padded outward by _RANGE_PAD so a
# slightly-out-of-sample event of the same type isn't rejected by a too-tight band.
_LO_PCT = 10.0
_HI_PCT = 90.0
_RANGE_PAD = 0.10


def _percentile(vals: List[Optional[float]], pct: float) -> Optional[float]:
    """Linear-interpolated percentile of the non-None values, or None if empty."""
    xs = sorted(float(v) for v in vals if v is not None)
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _range(vals: List[Optional[float]]) -> Optional[List[float]]:
    """Padded [p10, p90] range as a 2-list (JSON-friendly), or None if empty."""
    lo = _percentile(vals, _LO_PCT)
    hi = _percentile(vals, _HI_PCT)
    if lo is None or hi is None:
        return None
    span = max(hi - lo, 0.0)
    return [max(0.0, lo - span * _RANGE_PAD), hi + span * _RANGE_PAD]


def _clean_label_rows(conn: sqlite3.Connection, circuit: str,
                      fixture_type: str) -> List[sqlite3.Row]:
    """Clean (training-eligible) labelled events of one type on one circuit."""
    return conn.execute(
        "SELECT volume_litres, duration_seconds, peak_flow_lpm "
        "FROM events WHERE circuit = ? AND user_fixture_type = ? "
        "  AND COALESCE(excluded_from_training, 0) = 0",
        (circuit, fixture_type),
    ).fetchall()


def fit_rule_constants(conn: sqlite3.Connection, circuit: str) -> Dict[str, Any]:
    """Derive per-home rule bands from this circuit's labelled events.

    Returns a dict of overridable keys (mirroring the ``event_rules`` constant
    names without the leading underscore). Only types with at least
    ``MIN_LABELS_FOR_FIT`` clean labels contribute; everything else is omitted so
    the rule keeps its shipped default. Pure read — does not persist.
    """
    calib: Dict[str, Any] = {}

    def cols(ftype: str):
        rows = _clean_label_rows(conn, circuit, ftype)
        return (rows,
                [r["volume_litres"] for r in rows],
                [r["duration_seconds"] for r in rows],
                [r["peak_flow_lpm"] for r in rows])

    # ── Toilet → flush bands ────────────────────────────────────────────────
    rows, vol, dur, pk = cols("toilet")
    if len(rows) >= MIN_LABELS_FOR_FIT:
        if (v := _range(vol)) is not None:
            calib["FLUSH_VOL_L"] = v
        if (d := _range(dur)) is not None:
            calib["FLUSH_DUR_S"] = d
        p_lo = _percentile(pk, _LO_PCT)
        if p_lo is not None:
            calib["FLUSH_MIN_PK_LPM"] = max(0.0, p_lo * (1.0 - _RANGE_PAD))

    # ── Dishwasher → volume band + peak ceiling ─────────────────────────────
    rows, vol, dur, pk = cols("dishwasher")
    if len(rows) >= MIN_LABELS_FOR_FIT:
        if (v := _range(vol)) is not None:
            calib["DW_VOL_L"] = v
        p_hi = _percentile(pk, _HI_PCT)
        if p_hi is not None:
            calib["DW_MAX_PK_LPM"] = p_hi * (1.0 + _RANGE_PAD)

    # ── Shower/tub → big-branch floors (vol/dur/peak minimums) ──────────────
    rows, vol, dur, pk = cols("shower_tub")
    if len(rows) >= MIN_LABELS_FOR_FIT:
        v_lo = _percentile(vol, _LO_PCT)
        d_lo = _percentile(dur, _LO_PCT)
        p_lo = _percentile(pk, _LO_PCT)
        if v_lo is not None:
            calib["SHOWER_BIG_VOL_L"] = v_lo * (1.0 - _RANGE_PAD)
        if d_lo is not None:
            calib["SHOWER_BIG_DUR_S"] = d_lo * (1.0 - _RANGE_PAD)
        if p_lo is not None:
            calib["SHOWER_BIG_MIN_PK"] = max(0.0, p_lo * (1.0 - _RANGE_PAD))

    # ── Irrigation zone → duration/peak floors ──────────────────────────────
    rows, vol, dur, pk = cols("irrigation_zone")
    if len(rows) >= MIN_LABELS_FOR_FIT:
        d_lo = _percentile(dur, _LO_PCT)
        p_lo = _percentile(pk, _LO_PCT)
        if d_lo is not None:
            calib["ZONE_MIN_DUR_S"] = d_lo * (1.0 - _RANGE_PAD)
        if p_lo is not None:
            calib["ZONE_MIN_PK_LPM"] = max(0.0, p_lo * (1.0 - _RANGE_PAD))

    # ── Washing machine → anchor (main-fill) bands ──────────────────────────
    # Washer labels mix big fills (anchors) with small top-offs, so fit the
    # anchor bands only over the upper half by volume (>= median) — the top-offs
    # are matched as same-peak family members, not anchors.
    rows, vol, dur, pk = cols("washing_machine")
    if len(rows) >= MIN_LABELS_FOR_FIT:
        v_med = _percentile(vol, 50.0)
        if v_med is not None:
            anchors = [r for r in rows
                       if r["volume_litres"] is not None
                       and r["volume_litres"] >= v_med]
            calib["WASHER_ANCHOR_MIN_VOL_L"] = v_med * (1.0 - _RANGE_PAD)
            a_dur = _range([r["duration_seconds"] for r in anchors])
            a_pk = _range([r["peak_flow_lpm"] for r in anchors])
            if a_dur is not None:
                calib["WASHER_ANCHOR_DUR_S"] = a_dur
            if a_pk is not None:
                calib["WASHER_ANCHOR_PK_LPM"] = a_pk

    return calib


def save_rule_calibration(conn: sqlite3.Connection, circuit: str,
                          calib: Dict[str, Any],
                          source: str = "activation") -> None:
    """Persist (freeze) the fitted calibration for a circuit with a lock stamp."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO rule_calibration (circuit, params, source, locked_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(circuit) DO UPDATE SET "
        "  params = excluded.params, source = excluded.source, "
        "  locked_at = excluded.locked_at, updated_at = excluded.updated_at",
        (circuit, json.dumps(calib), source, now, now),
    )
    conn.commit()
    log.info("[%s] rule calibration frozen (%s): %d band(s) fit — %s",
             circuit, source, len(calib), ", ".join(sorted(calib)) or "none")


def load_rule_calibration(conn: sqlite3.Connection, circuit: str) -> Dict[str, Any]:
    """Return the frozen calibration dict for a circuit, or ``{}`` if none.

    Resilient to a missing table / corrupt JSON — returns ``{}`` so callers fall
    back to the shipped defaults rather than crashing the classification path.
    """
    try:
        row = conn.execute(
            "SELECT params FROM rule_calibration WHERE circuit = ?",
            (circuit,),
        ).fetchone()
    except sqlite3.OperationalError:
        return {}
    if not row or not row["params"]:
        return {}
    try:
        data = json.loads(row["params"])
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def get_rule_calibration_meta(conn: sqlite3.Connection,
                              circuit: str) -> Optional[Dict[str, Any]]:
    """Lock metadata for the UI/diagnostics: when it was frozen and how many
    bands were fit. Returns None if the circuit has never been calibrated."""
    try:
        row = conn.execute(
            "SELECT params, source, locked_at FROM rule_calibration WHERE circuit = ?",
            (circuit,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    calib = load_rule_calibration(conn, circuit)
    return {
        "locked_at": row["locked_at"],
        "source": row["source"],
        "fit_keys": sorted(calib),
        "fit_count": len(calib),
    }
