"""Per-home rule calibration (Phase 1 — fit-once-at-activation, then frozen).

The structural-rules-tier bands in :mod:`event_rules` ship as developer-tuned
module constants shaped by one home's data. At **activation** (labelling → live,
or an explicit dev re-train) those bands are re-fit from THIS home's labelled +
accepted-auto events and stored, frozen, in the ``rule_calibration`` table. The
:mod:`event_rules` predicates read the fitted values through an optional ``calib``
dict, falling back to ``event_rules.RULE_DEFAULTS`` for any band this home lacks
enough labels to fit (or whose fit fails the sanity gate).

Design (per the approved plan):

* **Fit once, then freeze.** Calibration is written only at activation / explicit
  re-train — never on ordinary reclassify or live events. A locked baseline is
  what lets future leak / odd-usage detection treat a non-conforming event as
  anomalous instead of silently learning it as normal.
* **Weighted, inclusive pool.** Every typed event contributes, weighted by
  provenance (``fixture_label_source`` / ``matched_via``): explicit ``user`` /
  ``training`` = 1.0, ``cycle`` = 0.75, ``knn`` = 0.5, base structural detectors
  = 0.25. Explicit labels *authorize* a per-type fit; auto-labels only *refine*
  the band.
* **Dual gate.** A type is fit only with ≥ ``MIN_EXPLICIT_LABELS`` explicit
  labels AND ≥ ``MIN_FIT_WEIGHT`` weighted mass — so a type can't be frozen onto
  unconfirmed auto-labels the explicit-only eval can't validate.
* **Artifacts.** Explicit labels count even when ``excluded_from_training=1`` (a
  human label overrides the auto artifact flag); artifact-flagged events with no
  explicit label are dropped (their flow/volume features are unreliable). The
  phantom/dribble/cross-talk *detectors* are calibrated separately in Phase 2.
* **Bounded-expansion sanity gate (shared path).** Each fitted band is checked
  against absolute physical limits and a span cap relative to the default; a band
  that fails falls back to the default for that type. The gate lives HERE in the
  shared fit path so BOTH activation and the dev ``retrain()`` enforce it.
* **No event_rules → rule_calibration import.** This module imports
  ``RULE_DEFAULTS`` from event_rules (one-way); event_rules never imports back.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .event_rules import RULE_DEFAULTS

log = logging.getLogger(__name__)

# ── Gates / knobs (placeholders until the held-out eval locks them on real data) ─
MIN_EXPLICIT_LABELS = 5        # ≥ this many user/training labels to AUTHORISE a fit
MIN_FIT_WEIGHT = 8.0           # AND ≥ this weighted label mass
_FIT_MAX_SPAN_FACTOR = 2.0     # a fitted range span may be at most this × default span

_LO_PCT = 10.0
_HI_PCT = 90.0
_RANGE_PAD = 0.10              # widen fitted [lo, hi] ranges by 10% each side

# Provenance → (weight, is_explicit). Explicit labels authorise; the rest refine.
_W_EXPLICIT = 1.0
_W_CYCLE = 0.75
_W_KNN = 0.5
_W_RULE = 0.25

# Per-key sanity metadata: kind + absolute physical bounds. Range keys also get a
# span cap relative to RULE_DEFAULTS. Floor/ceiling keys are scalar.
#   kind: "range" (a [lo, hi] tuple) | "floor" | "ceiling" (a scalar)
_SANITY: Dict[str, Tuple[str, float, float]] = {
    "FLUSH_VOL_L":             ("range",   0.0,   30.0),
    "FLUSH_DUR_S":             ("range",   0.0,  600.0),
    "FLUSH_MIN_PK_LPM":        ("floor",   0.0,   30.0),
    "DW_VOL_L":                ("range",   0.0,   30.0),
    "DW_MAX_PK_LPM":           ("ceiling", 0.0,   30.0),
    "SHOWER_BIG_VOL_L":        ("floor",   0.0,  600.0),
    "SHOWER_BIG_DUR_S":        ("floor",   0.0, 7200.0),
    "SHOWER_BIG_MIN_PK":       ("floor",   0.0,   60.0),
    "ZONE_MIN_DUR_S":          ("floor",   0.0, 7200.0),
    "ZONE_MIN_PK_LPM":         ("floor",   0.0,   60.0),
    "WASHER_ANCHOR_MIN_VOL_L": ("floor",   0.0,  200.0),
    "WASHER_ANCHOR_DUR_S":     ("range",   0.0, 1200.0),
    "WASHER_ANCHOR_PK_LPM":    ("range",   0.0,   60.0),
}


# ── Weighted percentile helpers ─────────────────────────────────────────────────

def _wpct(pairs: List[Tuple[float, float]], pct: float) -> Optional[float]:
    """Weighted percentile of (value, weight) pairs, or None if empty.

    Uses the standard weighted-percentile convention: order by value, walk the
    cumulative weight, and interpolate at ``pct`` of the total weight.
    """
    pts = sorted((float(v), float(w)) for v, w in pairs if v is not None and w > 0)
    if not pts:
        return None
    if len(pts) == 1:
        return pts[0][0]
    total = sum(w for _, w in pts)
    if total <= 0:
        return None
    target = (pct / 100.0) * total
    cum = 0.0
    prev_v = pts[0][0]
    for v, w in pts:
        if cum + w >= target:
            # linear interpolate between prev_v and v across this segment
            span = w if w > 0 else 1.0
            frac = max(0.0, min(1.0, (target - cum) / span))
            return prev_v + (v - prev_v) * frac
        cum += w
        prev_v = v
    return pts[-1][0]


def _wrange(pairs: List[Tuple[float, float]]) -> Optional[List[float]]:
    """Padded weighted [p10, p90] as a JSON-friendly 2-list, or None if empty."""
    lo = _wpct(pairs, _LO_PCT)
    hi = _wpct(pairs, _HI_PCT)
    if lo is None or hi is None:
        return None
    span = max(hi - lo, 0.0)
    return [max(0.0, lo - span * _RANGE_PAD), hi + span * _RANGE_PAD]


# ── Fit pool ────────────────────────────────────────────────────────────────────

def _provenance_weight(uft, src, mft, via) -> Optional[Tuple[str, float, bool]]:
    """Return (effective_type, weight, is_explicit) for one event row, or None to
    skip (no usable type)."""
    if uft:
        if src == "cycle":
            return (uft, _W_CYCLE, False)
        # 'user', 'training', or legacy NULL source on a user-set label → explicit
        return (uft, _W_EXPLICIT, True)
    if mft:
        if via == "knn":
            return (mft, _W_KNN, False)
        # rule_*, washer_cycle, softener_session, zone_default, legacy cluster
        return (mft, _W_RULE, False)
    return None


def _pool_from_rows(rows: List[dict]) -> Dict[str, List[dict]]:
    """Group typed-event row dicts by effective type, each entry carrying its
    flow/volume features + provenance weight. Artifact-flagged events are kept only
    when they carry an explicit (user/training) label. Each row dict needs:
    user_fixture_type, fixture_label_source, matched_fixture_type, matched_via,
    excluded_from_training, volume_litres, duration_seconds, peak_flow_lpm."""
    pool: Dict[str, List[dict]] = {}
    for r in rows:
        tagged = _provenance_weight(
            r.get("user_fixture_type"), r.get("fixture_label_source"),
            r.get("matched_fixture_type"), r.get("matched_via"))
        if tagged is None:
            continue
        ftype, weight, explicit = tagged
        # Artifact-flagged auto event with no explicit label → unreliable features.
        if (r.get("excluded_from_training") or 0) and not explicit:
            continue
        pool.setdefault(ftype, []).append({
            "vol": r.get("volume_litres"),
            "dur": r.get("duration_seconds"),
            "pk":  r.get("peak_flow_lpm"),
            "weight": weight,
            "explicit": explicit,
        })
    return pool


def _fit_pool(conn: sqlite3.Connection, circuit: str) -> Dict[str, List[dict]]:
    """Build the fit pool from all of one circuit's events."""
    rows = conn.execute(
        "SELECT user_fixture_type, fixture_label_source, matched_fixture_type, "
        "       matched_via, "
        "       COALESCE(excluded_from_training, 0) AS excluded_from_training, "
        "       volume_litres, duration_seconds, peak_flow_lpm "
        "FROM events WHERE circuit = ?",
        (circuit,),
    ).fetchall()
    return _pool_from_rows([dict(r) for r in rows])


def _pairs(entries: List[dict], field: str) -> List[Tuple[float, float]]:
    return [(e[field], e["weight"]) for e in entries if e.get(field) is not None]


# ── Per-type fitters → candidate bands (pre-sanity) ─────────────────────────────

def _fit_toilet(e):
    out = {}
    if (v := _wrange(_pairs(e, "vol"))) is not None:
        out["FLUSH_VOL_L"] = v
    if (d := _wrange(_pairs(e, "dur"))) is not None:
        out["FLUSH_DUR_S"] = d
    p = _wpct(_pairs(e, "pk"), _LO_PCT)
    if p is not None:
        out["FLUSH_MIN_PK_LPM"] = max(0.0, p * (1.0 - _RANGE_PAD))
    return out


def _fit_dishwasher(e):
    out = {}
    if (v := _wrange(_pairs(e, "vol"))) is not None:
        out["DW_VOL_L"] = v
    p = _wpct(_pairs(e, "pk"), _HI_PCT)
    if p is not None:
        out["DW_MAX_PK_LPM"] = p * (1.0 + _RANGE_PAD)
    return out


def _fit_shower(e):
    out = {}
    v = _wpct(_pairs(e, "vol"), _LO_PCT)
    d = _wpct(_pairs(e, "dur"), _LO_PCT)
    p = _wpct(_pairs(e, "pk"), _LO_PCT)
    if v is not None:
        out["SHOWER_BIG_VOL_L"] = max(0.0, v * (1.0 - _RANGE_PAD))
    if d is not None:
        out["SHOWER_BIG_DUR_S"] = max(0.0, d * (1.0 - _RANGE_PAD))
    if p is not None:
        out["SHOWER_BIG_MIN_PK"] = max(0.0, p * (1.0 - _RANGE_PAD))
    return out


def _fit_zone(e):
    out = {}
    d = _wpct(_pairs(e, "dur"), _LO_PCT)
    p = _wpct(_pairs(e, "pk"), _LO_PCT)
    if d is not None:
        out["ZONE_MIN_DUR_S"] = max(0.0, d * (1.0 - _RANGE_PAD))
    if p is not None:
        out["ZONE_MIN_PK_LPM"] = max(0.0, p * (1.0 - _RANGE_PAD))
    return out


def _fit_washer(e):
    """Anchor bands fit only over the upper-half-by-volume events (top-offs are
    family members, not anchors)."""
    out = {}
    med = _wpct(_pairs(e, "vol"), 50.0)
    if med is None:
        return out
    out["WASHER_ANCHOR_MIN_VOL_L"] = max(0.0, med * (1.0 - _RANGE_PAD))
    anchors = [x for x in e if x.get("vol") is not None and x["vol"] >= med]
    if (d := _wrange(_pairs(anchors, "dur"))) is not None:
        out["WASHER_ANCHOR_DUR_S"] = d
    if (p := _wrange(_pairs(anchors, "pk"))) is not None:
        out["WASHER_ANCHOR_PK_LPM"] = p
    return out


# type → (fitter, list of keys it produces) — keys also drive the report.
_FITTERS = {
    "toilet":          _fit_toilet,
    "dishwasher":      _fit_dishwasher,
    "shower_tub":      _fit_shower,
    "irrigation_zone": _fit_zone,
    "washing_machine": _fit_washer,
}


# ── Sanity gate (bounded expansion + absolute bounds) ───────────────────────────

def _is_sane(key: str, value: Any) -> bool:
    spec = _SANITY.get(key)
    if spec is None:
        return True
    kind, lo_abs, hi_abs = spec
    if kind == "range":
        if not (isinstance(value, (list, tuple)) and len(value) == 2):
            return False
        lo, hi = float(value[0]), float(value[1])
        if not (lo >= lo_abs and hi <= hi_abs and hi > lo):
            return False
        default = RULE_DEFAULTS[key]
        default_span = float(default[1]) - float(default[0])
        if default_span > 0 and (hi - lo) > _FIT_MAX_SPAN_FACTOR * default_span:
            return False  # implausibly wide → noisy labels, fall back
        return True
    # floor / ceiling scalar
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    return lo_abs <= v <= hi_abs


# ── Public API ──────────────────────────────────────────────────────────────────

def _fit_from_pool(pool: Dict[str, List[dict]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Run the per-type fit + dual gate + sanity gate over a prepared pool.
    Returns ``(calib, report)``. Shared by the DB-backed ``fit_rule_constants`` and
    the row-backed ``fit_rule_constants_from_rows`` (which powers held-out eval)."""
    calib: Dict[str, Any] = {}
    report: Dict[str, Any] = {}

    for ftype, fitter in _FITTERS.items():
        entries = pool.get(ftype, [])
        explicit_n = sum(1 for x in entries if x["explicit"])
        weight = sum(x["weight"] for x in entries)
        rep: Dict[str, Any] = {
            "explicit": explicit_n,
            "weight": round(weight, 2),
            "keys_fit": [],
            "keys_fallback": [],
        }
        if explicit_n < MIN_EXPLICIT_LABELS or weight < MIN_FIT_WEIGHT:
            rep["status"] = "insufficient_labels"
            report[ftype] = rep
            continue
        candidates = fitter(entries)
        for key, value in candidates.items():
            if _is_sane(key, value):
                calib[key] = value
                rep["keys_fit"].append(key)
            else:
                rep["keys_fallback"].append(key)
        rep["status"] = "fit" if rep["keys_fit"] else "sanity_fallback"
        report[ftype] = rep

    return calib, report


def fit_rule_constants(conn: sqlite3.Connection,
                       circuit: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Derive per-home rule bands from this circuit's weighted, gated, sanity-
    checked label pool. Returns ``(calib, report)``; pure read — does not persist."""
    return _fit_from_pool(_fit_pool(conn, circuit))


def fit_rule_constants_from_rows(
        rows: List[dict]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Same fit as ``fit_rule_constants`` but over an explicit list of event row
    dicts (e.g. a k-fold train split, excluding the held-out events). Used by the
    held-out eval so the held-out events never leak into their own fit."""
    norm = [r if isinstance(r, dict) else dict(r) for r in rows]
    return _fit_from_pool(_pool_from_rows(norm))


def save_rule_calibration(conn: sqlite3.Connection, circuit: str,
                          calib: Dict[str, Any],
                          report: Optional[Dict[str, Any]] = None,
                          source: str = "activation") -> None:
    """Persist (freeze) the fitted calibration + report for a circuit with a stamp."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO rule_calibration "
        "    (circuit, params, report, source, locked_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(circuit) DO UPDATE SET "
        "  params = excluded.params, report = excluded.report, "
        "  source = excluded.source, locked_at = excluded.locked_at, "
        "  updated_at = excluded.updated_at",
        (circuit, json.dumps(calib), json.dumps(report or {}), source, now, now),
    )
    conn.commit()
    log.info("[%s] rule calibration frozen (%s): %d band(s) fit — %s",
             circuit, source, len(calib), ", ".join(sorted(calib)) or "none")


def fit_and_freeze(conn: sqlite3.Connection, circuit: str,
                   source: str = "activation") -> Dict[str, Any]:
    """Shared fit → sanity → persist path used by BOTH activation and the dev
    ``retrain()``. Returns the per-type report (for UI display / logging)."""
    calib, report = fit_rule_constants(conn, circuit)
    save_rule_calibration(conn, circuit, calib, report=report, source=source)
    return report


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
    """Lock metadata for the UI/diagnostics: when it was frozen, how many bands
    were fit, and the per-type fit-vs-fallback report. None if never calibrated."""
    try:
        row = conn.execute(
            "SELECT report, source, locked_at FROM rule_calibration WHERE circuit = ?",
            (circuit,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    calib = load_rule_calibration(conn, circuit)
    try:
        report = json.loads(row["report"]) if row["report"] else {}
    except (json.JSONDecodeError, TypeError):
        report = {}
    return {
        "locked_at": row["locked_at"],
        "source": row["source"],
        "fit_keys": sorted(calib),
        "fit_count": len(calib),
        "report": report,
    }
