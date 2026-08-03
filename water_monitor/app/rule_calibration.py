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
# Do-no-harm noise margin (dev34). The frozen default must beat the fit by BOTH
# of these before the fit is discarded — a 1-of-25 difference is a coin flip,
# and treating it as evidence kept pre-pump toilet constants in force through a
# regime whose ΔP had risen 2.6×. Both gates matter: the absolute one protects
# small test sets, the fractional one protects large ones.
_DO_NO_HARM_MIN_MARGIN = 2      # events
_DO_NO_HARM_MIN_FRACTION = 0.05  # 5% of the held-out set
_FIT_MAX_SPAN_FACTOR = 2.0     # a fitted range span may be at most this × default span
# Bounded LOOSENING for scalar floor/ceiling bands (dev34 — see _is_sane).
_FIT_MIN_FLOOR_FRACTION = 0.5   # a fitted floor may not drop below half the default
_FIT_MAX_CEILING_FACTOR = 2.5   # a fitted ceiling may not exceed 2.5× the default

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


def _regime_window(regime: Optional[Dict[str, Any]]) -> Tuple[str, tuple]:
    """SQL fragment + args restricting events to one supply regime's TIME
    window [started_at, ended_at). A TIME filter, not a pressure filter — a
    NULL-pressure event still belongs to whatever regime was in force when it
    happened, and the regime boundary is a temporal fact (the day the pump
    went in). ``regime`` is a supply_regime row dict or None (no filter)."""
    if not regime or not regime.get("started_at"):
        return "", ()
    sql = " AND start_ts >= ?"
    args: tuple = (regime["started_at"],)
    if regime.get("ended_at"):
        sql += " AND start_ts < ?"
        args += (regime["ended_at"],)
    return sql, args


def _fit_pool(conn: sqlite3.Connection, circuit: str,
              regime: Optional[Dict[str, Any]] = None) -> Dict[str, List[dict]]:
    """Build the fit pool from one circuit's events (optionally one regime's)."""
    rsql, rargs = _regime_window(regime)
    rows = conn.execute(
        "SELECT user_fixture_type, fixture_label_source, matched_fixture_type, "
        "       matched_via, "
        "       COALESCE(excluded_from_training, 0) AS excluded_from_training, "
        "       volume_litres, duration_seconds, peak_flow_lpm "
        f"FROM events WHERE circuit = ?{rsql}",
        (circuit,) + rargs,
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


# type → fitter.
_FITTERS = {
    "toilet":          _fit_toilet,
    "dishwasher":      _fit_dishwasher,
    "shower_tub":      _fit_shower,
    "irrigation_zone": _fit_zone,
    "washing_machine": _fit_washer,
}

# Types the per-event rule tier (rule_classify_event) can predict — used by the
# do-no-harm held-out check. washing_machine is matched by detect_washer_cycles,
# not rule_classify_event, so it isn't validated this way (its fitted anchor bands
# are kept as-is).
_RULE_TYPES = ("toilet", "dishwasher", "shower_tub", "irrigation_zone")
_TYPE_KEYS = {
    "toilet":          ("FLUSH_VOL_L", "FLUSH_DUR_S", "FLUSH_MIN_PK_LPM"),
    "dishwasher":      ("DW_VOL_L", "DW_MAX_PK_LPM"),
    "shower_tub":      ("SHOWER_BIG_VOL_L", "SHOWER_BIG_DUR_S", "SHOWER_BIG_MIN_PK"),
    "irrigation_zone": ("ZONE_MIN_DUR_S", "ZONE_MIN_PK_LPM"),
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
    default = RULE_DEFAULTS.get(key)
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    if not (lo_abs <= v <= hi_abs):
        return False
    # dev34 — BOUNDED LOOSENING. The absolute bounds above are deliberately
    # wide (a shower floor may legitimately sit anywhere in 0-600 L), which
    # let a fit collapse a discriminative threshold to nothing: the 2026-08-02
    # regime-2 refit produced SHOWER_BIG_VOL_L = 2.13 L and ZONE_MIN_DUR_S =
    # 14.6 s against defaults of 30 L and 240 s — a "big shower" floor small
    # enough to swallow a toilet flush. The cause is a percentile fit over a
    # pool that contains fragments and the occasional mislabel; the p10 then
    # sits far below the class's real lower edge.
    #
    # This is the scalar analogue of _FIT_MAX_SPAN_FACTOR for ranges: a fit may
    # tighten a threshold freely (that only ever makes the rule more selective)
    # but may only loosen it within a bounded factor of the shipped default,
    # which encodes the physics of the class. LOWERING a floor loosens; RAISING
    # a ceiling loosens. 2.5x on ceilings keeps the legitimate pump-driven
    # dishwasher change (3.6 -> 7.59 LPM = 2.1x) while still catching runaways.
    if default is not None:
        try:
            d = float(default)
        except (TypeError, ValueError):
            return True
        if d > 0:
            if kind == "floor" and v < _FIT_MIN_FLOOR_FRACTION * d:
                return False
            if kind == "ceiling" and v > _FIT_MAX_CEILING_FACTOR * d:
                return False
    return True


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


def fit_rule_constants(conn: sqlite3.Connection, circuit: str,
                       regime: Optional[Dict[str, Any]] = None,
                       ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Derive per-home rule bands from this circuit's weighted, gated, sanity-
    checked label pool — optionally restricted to one supply regime's time
    window. Returns ``(calib, report)``; pure read — does not persist."""
    return _fit_from_pool(_fit_pool(conn, circuit, regime))


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
                          source: str = "activation",
                          regime_id: int = 0) -> None:
    """Persist (freeze) the fitted calibration + report for a (circuit, supply
    regime) with a stamp. ``regime_id`` 0 is the legacy/pre-regime row."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO rule_calibration "
        "    (circuit, regime_id, params, report, source, locked_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(circuit, regime_id) DO UPDATE SET "
        "  params = excluded.params, report = excluded.report, "
        "  source = excluded.source, locked_at = excluded.locked_at, "
        "  updated_at = excluded.updated_at",
        (circuit, int(regime_id), json.dumps(calib), json.dumps(report or {}),
         source, now, now),
    )
    conn.commit()
    log.info("[%s] rule calibration frozen (%s, regime %d): %d band(s) fit — %s",
             circuit, source, regime_id, len(calib),
             ", ".join(sorted(calib)) or "none")


def _load_pool_rows(conn: sqlite3.Connection, circuit: str,
                    regime: Optional[Dict[str, Any]] = None) -> List[dict]:
    rsql, rargs = _regime_window(regime)
    return [dict(r) for r in conn.execute(
        "SELECT id, user_fixture_type, fixture_label_source, matched_fixture_type, "
        "       matched_via, "
        "       COALESCE(excluded_from_training,0) AS excluded_from_training, "
        "       volume_litres, duration_seconds, peak_flow_lpm "
        f"FROM events WHERE circuit = ?{rsql}",
        (circuit,) + rargs).fetchall()]


def _load_explicit_rows(conn: sqlite3.Connection, circuit: str,
                        regime: Optional[Dict[str, Any]] = None) -> List[dict]:
    rsql, rargs = _regime_window(regime)
    return [dict(r) for r in conn.execute(
        "SELECT id, user_fixture_type, volume_litres, duration_seconds, "
        "       peak_flow_lpm, cycle_pulse_count, has_pressure_transient, "
        "       pressure_delta_psi "
        "FROM events WHERE circuit = ? AND user_fixture_type IS NOT NULL "
        "  AND user_fixture_type <> '' AND COALESCE(excluded_from_training,0)=0 "
        f"  AND fixture_label_source IN ('user','training'){rsql} ORDER BY id",
        (circuit,) + rargs).fetchall()]


def kfold_type_accuracy(pool_rows: List[dict], explicit_rows: List[dict],
                        circuit_type: str, k: int = 5) -> Dict[str, Dict[str, int]]:
    """Per-type held-out recall of each type's FITTED bands (isolated) vs the FROZEN
    defaults, scored on the explicit (user/training) test rows. Deterministic
    round-robin folds; a held-out row is excluded from its fold's fit. Returns
    ``{type: {"fitted": n, "frozen": n, "n": n}}`` over the per-event rule types."""
    from .database import _canonical_fixture_type as _canon
    from .event_rules import rule_classify_event
    test = [e for e in explicit_rows
            if _canon(e.get("user_fixture_type")) in _RULE_TYPES]
    folds: List[List[dict]] = [[] for _ in range(k)]
    for i, e in enumerate(sorted(test, key=lambda r: str(r.get("id")))):
        folds[i % k].append(e)
    acc: Dict[str, Dict[str, int]] = {}
    for fold in folds:
        if not fold:
            continue
        held_ids = {e["id"] for e in fold}
        train = [r for r in pool_rows if r.get("id") not in held_ids]
        fitted_full, _ = fit_rule_constants_from_rows(train)
        for e in fold:
            actual = _canon(e.get("user_fixture_type"))
            keys = _TYPE_KEYS.get(actual)
            if keys is None:
                continue
            # Isolate THIS type's fitted bands (others stay default) so the check
            # measures only this type's marginal effect.
            calib_t = {kk: fitted_full[kk] for kk in keys if kk in fitted_full}
            a = acc.setdefault(actual, {"fitted": 0, "frozen": 0, "n": 0})
            a["n"] += 1
            hf = rule_classify_event(e, circuit_type, calib=calib_t)
            if hf and hf[0] == actual:
                a["fitted"] += 1
            hz = rule_classify_event(e, circuit_type, calib=None)
            if hz and hz[0] == actual:
                a["frozen"] += 1
    return acc


def _do_no_harm(conn: sqlite3.Connection, circuit: str,
                calib: Dict[str, Any],
                report: Dict[str, Any],
                regime: Optional[Dict[str, Any]] = None,
                ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Drop fitted bands that MEANINGFULLY reduce held-out recall vs the frozen
    default for their type — so the per-home fit can only help an atypical home,
    never regress a well-tuned one. (washing_machine isn't validated here —
    cycle detector.) With a ``regime``, both the folds' fits and the test rows
    stay inside the regime's window so the check measures within-regime
    performance.

    dev34 — the comparison now needs a MARGIN. Strict `fitted < frozen` made a
    one-event difference decisive: on the 2026-08-02 regime-2 refit, toilet
    scored 23 vs 24 correct of 25 and the fit was discarded, leaving pre-pump
    constants in force in a regime where toilet ΔP had gone 4.37 → 11.32 psi.
    A single held-out event is noise, not evidence. The fit is now kept unless
    the frozen default wins by at least ``_DO_NO_HARM_MIN_MARGIN`` events AND
    ``_DO_NO_HARM_MIN_FRACTION`` of the test set — so a real regression (which
    shows up as several events, e.g. the 30% collapses this gate was built to
    catch) still discards the fit, while a coin flip no longer does.
    """
    try:
        from .database import get_circuit_type
        circuit_type = get_circuit_type(conn, circuit)
    except Exception:
        circuit_type = "fixture"
    acc = kfold_type_accuracy(_load_pool_rows(conn, circuit, regime),
                              _load_explicit_rows(conn, circuit, regime),
                              circuit_type)
    for ftype, keys in _TYPE_KEYS.items():
        a = acc.get(ftype)
        if not a:
            continue
        deficit = a["frozen"] - a["fitted"]
        n = max(1, a["n"])
        regressed = (deficit >= _DO_NO_HARM_MIN_MARGIN
                     and (deficit / n) >= _DO_NO_HARM_MIN_FRACTION)
        if regressed:
            for key in keys:
                calib.pop(key, None)
            if ftype in report:
                report[ftype]["status"] = "regressed_kept_default"
                report[ftype]["held_out"] = {
                    "fitted": a["fitted"], "frozen": a["frozen"], "n": a["n"]}
            log.info("[%s] do-no-harm: dropped %s fit (held-out %d<%d of %d) "
                     "— kept default", circuit, ftype, a["fitted"],
                     a["frozen"], a["n"])
        elif deficit > 0 and ftype in report:
            # Within the margin: the fit ships, but record that it was behind
            # so a report reader can see it wasn't a clean win.
            report[ftype]["held_out"] = {
                "fitted": a["fitted"], "frozen": a["frozen"], "n": a["n"],
                "kept_within_margin": True}
            log.info("[%s] do-no-harm: %s fit kept despite held-out %d<%d of "
                     "%d (within the %d-event / %.0f%% noise margin)",
                     circuit, ftype, a["fitted"], a["frozen"], a["n"],
                     _DO_NO_HARM_MIN_MARGIN, 100 * _DO_NO_HARM_MIN_FRACTION)
    return calib, report


def fit_and_freeze(conn: sqlite3.Connection, circuit: str,
                   source: str = "activation",
                   do_no_harm: bool = True,
                   regime: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Shared fit → sanity → do-no-harm → persist path used by activation, the
    dev ``retrain()``, and the regime-shift recalibration. With a ``regime``
    (a supply_regime row dict) the fit pool, the held-out check, and the saved
    row are all scoped to that regime; without one, behavior is exactly the
    legacy whole-history fit saved as regime_id 0. Returns the per-type report."""
    calib, report = fit_rule_constants(conn, circuit, regime)
    if do_no_harm and calib:
        calib, report = _do_no_harm(conn, circuit, calib, report, regime)
    save_rule_calibration(conn, circuit, calib, report=report, source=source,
                          regime_id=int(regime["id"]) if regime else 0)
    return report


def _select_calibration_row(conn: sqlite3.Connection, circuit: str,
                            columns: str,
                            regime_id: Optional[int]) -> Optional[sqlite3.Row]:
    """Resolution chain: exact (circuit, regime_id) → legacy (circuit, 0) →
    None. ``regime_id`` None or 0 reads the legacy row directly. Tolerates a
    pre-20260565 table (no regime_id column) by falling back to the plain
    per-circuit read."""
    try:
        if regime_id:
            row = conn.execute(
                f"SELECT {columns} FROM rule_calibration "
                "WHERE circuit = ? AND regime_id = ?",
                (circuit, int(regime_id))).fetchone()
            if row:
                return row
        return conn.execute(
            f"SELECT {columns} FROM rule_calibration "
            "WHERE circuit = ? AND regime_id = 0", (circuit,)).fetchone()
    except sqlite3.OperationalError:
        try:
            return conn.execute(
                f"SELECT {columns} FROM rule_calibration WHERE circuit = ?",
                (circuit,)).fetchone()
        except sqlite3.OperationalError:
            return None


def load_rule_calibration(conn: sqlite3.Connection, circuit: str,
                          regime_id: Optional[int] = None) -> Dict[str, Any]:
    """Return the frozen calibration dict for a circuit (preferring the given
    supply regime's row, falling back to the legacy regime-0 row), or ``{}``.

    Resilient to a missing table / corrupt JSON — returns ``{}`` so callers fall
    back to the shipped defaults rather than crashing the classification path.
    """
    row = _select_calibration_row(conn, circuit, "params", regime_id)
    if not row or not row["params"]:
        return {}
    try:
        data = json.loads(row["params"])
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def get_rule_calibration_meta(conn: sqlite3.Connection, circuit: str,
                              regime_id: Optional[int] = None,
                              ) -> Optional[Dict[str, Any]]:
    """Lock metadata for the UI/diagnostics: when it was frozen, how many bands
    were fit, and the per-type fit-vs-fallback report. None if never calibrated.
    Same regime resolution chain as ``load_rule_calibration``."""
    row = _select_calibration_row(
        conn, circuit, "report, source, locked_at", regime_id)
    if not row:
        return None
    calib = load_rule_calibration(conn, circuit, regime_id)
    try:
        report = json.loads(row["report"]) if row["report"] else {}
    except (json.JSONDecodeError, TypeError):
        report = {}
    return {
        "locked_at": row["locked_at"],
        "source": row["source"],
        "regime_id": regime_id or 0,
        "fit_keys": sorted(calib),
        "fit_count": len(calib),
        "report": report,
    }
