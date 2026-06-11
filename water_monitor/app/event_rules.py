"""Event-level structural rules tier (dev.23) — runs BEFORE the k-NN matcher.

Ports the three-pass audit's findings into production:

* ``detect_washer_cycles`` — a washing-machine cycle is a SAME-PEAK family with
  wildly varying volumes: main fills (>=9 L, 80-400 s) plus sub-2.5 L top-offs,
  all at ~constant peak (+/-15-30%), 2-45 min apart. Volume-ratio approaches
  (``cycle_pulse_count``, the dishwasher cycle propagation) structurally split
  these cycles (15x fill-to-top-off spread); keying the family on PEAK is what
  made the audit's detector hit 0.73 recall with zero toilet contamination.
* ``rule_classify_event`` — high-precision per-event rules (toilet / dishwasher /
  shower / zone-default) that the audit showed classify their shapes better than
  the k-NN does (toilet 0.95-1.00 vs 0.75), leaving the k-NN as the residual.

Everything here writes/feeds ``matched_fixture_type`` (the machine-opinion column)
only — user labels are never touched, and every verdict is recomputed on each
reclassify, so the tier is fully reversible.

⚠ CALIBRATION: every constant below is shaped by THIS home's labeled data (15
washer labels, one washer, one supply pressure). A structural rule asserts rather
than abstains, so these do NOT generalize automatically — a different home needs
its own calibration or multi-home re-validation. tools/eval_knn_classifier.py
--with-rules is the (in-sample) fit gate; its output overrides these numbers.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# ── Washer cycle detector constants (audit Pass 4; in-sample, eval-gated) ──────
_WASHER_ANCHOR_MIN_VOL_L: float = 9.0
_WASHER_ANCHOR_DUR_S: Tuple[float, float] = (80.0, 400.0)
_WASHER_ANCHOR_PK_LPM: Tuple[float, float] = (7.5, 15.0)
_WASHER_FAMILY_PK_RATIO: Tuple[float, float] = (0.8, 1.3)
_WASHER_FAMILY_WINDOW_MIN: float = 45.0
_WASHER_FAMILY_MIN_GAP_MIN: float = 2.0
_WASHER_SIBLING_MIN_VOL_L: float = 0.5
_WASHER_SIBLING_MAX_DUR_S: float = 400.0
# Live-path pre-gate: an event can only participate in a family if its peak lies
# inside [min anchor pk x low ratio, max anchor pk x high ratio].
WASHER_FAMILY_PK_ENVELOPE: Tuple[float, float] = (
    _WASHER_ANCHOR_PK_LPM[0] * _WASHER_FAMILY_PK_RATIO[0],   # 6.0
    _WASHER_ANCHOR_PK_LPM[1] * _WASHER_FAMILY_PK_RATIO[1],   # 19.5
)

# ── Other rule constants ───────────────────────────────────────────────────────
_FLUSH_VOL_L: Tuple[float, float] = (2.2, 8.5)
_FLUSH_DUR_S: Tuple[float, float] = (20.0, 150.0)
_FLUSH_MIN_PK_LPM: float = 5.0
_FLUSH_MIN_DELTA_PSI: float = 1.5

_DW_VOL_L: Tuple[float, float] = (0.2, 3.5)
# LOCKED by the eval sweep (tools/eval_knn_classifier.py --with-rules): 4.2 let
# the rule claim gentle tap fills (tap recall fell below the k-NN baseline);
# 3.6 — the audit's strict gentle-train cut — restores tap while keeping
# dishwasher at 0.933 and overall LOO at 0.685 (baseline 0.624).
_DW_MAX_PK_LPM: float = 3.6
_DW_MIN_CYCLE_PULSES: int = 2

_SHOWER_BIG_VOL_L: float = 30.0
_SHOWER_BIG_DUR_S: float = 300.0
_SHOWER_BIG_MIN_PK: float = 6.0
_SHOWER_SMALL_VOL_L: Tuple[float, float] = (15.0, 30.0)
_SHOWER_SMALL_DUR_S: float = 240.0

_ZONE_MIN_DUR_S: float = 240.0
_ZONE_MIN_PK_LPM: float = 5.0


def _f(features: Dict[str, Any], key: str) -> Optional[float]:
    """Feature as float, or None (missing / non-numeric)."""
    v = features.get(key)
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def is_flush_shaped(features: Dict[str, Any]) -> bool:
    """THE shared flush predicate — both the toilet rule's positive match AND the
    washer sweep's exclusion. Single-sourcing it makes the invariant structural:
    the washer family can never claim an event the toilet rule would claim (a
    flush during laundry is irreducibly ambiguous, so it stays with the
    per-event tiers)."""
    vol = _f(features, "volume_litres")
    dur = _f(features, "duration_seconds")
    pk = _f(features, "peak_flow_lpm")
    if vol is None or dur is None or pk is None:
        return False
    if not (_FLUSH_VOL_L[0] <= vol <= _FLUSH_VOL_L[1]):
        return False
    if not (_FLUSH_DUR_S[0] <= dur <= _FLUSH_DUR_S[1]):
        return False
    if pk < _FLUSH_MIN_PK_LPM:
        return False
    transient = features.get("has_pressure_transient")
    delta = _f(features, "pressure_delta_psi")
    return bool(transient) or (delta is not None and delta >= _FLUSH_MIN_DELTA_PSI)


def _is_washer_anchor(vol, dur, pk) -> bool:
    return (vol is not None and dur is not None and pk is not None
            and vol >= _WASHER_ANCHOR_MIN_VOL_L
            and _WASHER_ANCHOR_DUR_S[0] <= dur <= _WASHER_ANCHOR_DUR_S[1]
            and _WASHER_ANCHOR_PK_LPM[0] <= pk <= _WASHER_ANCHOR_PK_LPM[1])


def detect_washer_cycles(
    conn: sqlite3.Connection,
    circuit: str,
    since_ts: Optional[str] = None,
    limit: int = 4000,
) -> Dict[str, str]:
    """Find washer-cycle members on ``circuit``: anchors (main fills) that have at
    least one same-peak sibling 2-45 min away, plus the family's non-flush-shaped
    members (top-offs + secondary fills). Returns ``{event_id: 'anchor'|'member'}``.

    ``since_ts`` bounds the scan for the live trailing pass (the window also
    extends one family-width before since_ts so an anchor just before the bound
    still claims members inside it). Reads ONLY feature/timestamp columns — never
    a label column — so the eval harness's leave-one-out stays honest.
    """
    where = "WHERE circuit = ?"
    params: list = [circuit]
    if since_ts is not None:
        where += " AND start_ts >= ?"
        params.append(since_ts)
    params.append(limit)
    rows = conn.execute(
        "SELECT id, start_ts, duration_seconds, volume_litres, peak_flow_lpm, "
        "       has_pressure_transient, pressure_delta_psi "
        f"FROM events {where} ORDER BY start_ts LIMIT ?",
        params,
    ).fetchall()

    evs = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["start_ts"])
        except (TypeError, ValueError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        evs.append((ts, r))

    out: Dict[str, str] = {}
    win = _WASHER_FAMILY_WINDOW_MIN * 60.0
    min_gap = _WASHER_FAMILY_MIN_GAP_MIN * 60.0
    for ts_a, a in evs:
        if not _is_washer_anchor(a["volume_litres"], a["duration_seconds"],
                                 a["peak_flow_lpm"]):
            continue
        pk_a = a["peak_flow_lpm"]
        family = []
        for ts_o, o in evs:
            if o["id"] == a["id"]:
                continue
            gap = abs((ts_o - ts_a).total_seconds())
            if not (min_gap <= gap <= win):
                continue
            vol_o, dur_o, pk_o = (o["volume_litres"], o["duration_seconds"],
                                  o["peak_flow_lpm"])
            if vol_o is None or pk_o is None or vol_o < _WASHER_SIBLING_MIN_VOL_L:
                continue
            if dur_o is not None and dur_o > _WASHER_SIBLING_MAX_DUR_S:
                continue
            if not (_WASHER_FAMILY_PK_RATIO[0] * pk_a <= pk_o
                    <= _WASHER_FAMILY_PK_RATIO[1] * pk_a):
                continue
            family.append(o)
        if not family:
            continue                       # lone big fill (sink/tub) — not a cycle
        out[a["id"]] = "anchor"
        for o in family:
            feats = {"volume_litres": o["volume_litres"],
                     "duration_seconds": o["duration_seconds"],
                     "peak_flow_lpm": o["peak_flow_lpm"],
                     "has_pressure_transient": o["has_pressure_transient"],
                     "pressure_delta_psi": o["pressure_delta_psi"]}
            if is_flush_shaped(feats):
                continue                   # flush during laundry — leave it alone
            out.setdefault(o["id"], "member")
    return out


def rule_classify_event(
    features: Dict[str, Any], circuit_type: str = "fixture",
) -> Optional[Tuple[str, str]]:
    """Ordered structural rules — first hit wins; None = no rule claims the event
    (falls through to the k-NN residual). Washer is deliberately NOT here: it
    requires cycle context and comes only from ``detect_washer_cycles``."""
    vol = _f(features, "volume_litres")
    dur = _f(features, "duration_seconds")
    pk = _f(features, "peak_flow_lpm")

    if circuit_type == "zone":
        # Zone circuits host no toilets/dishwashers/showers; the only rule is the
        # irrigation default (fixes the audit's 0/6: the k-NN can never fire
        # under its 10-label floor on a young zone circuit).
        if (dur is not None and dur >= _ZONE_MIN_DUR_S
                and pk is not None and pk >= _ZONE_MIN_PK_LPM):
            return "irrigation_zone", "zone_default"
        return None

    if is_flush_shaped(features):
        return "toilet", "rule_toilet"

    cyc = _f(features, "cycle_pulse_count")
    if (vol is not None and _DW_VOL_L[0] <= vol <= _DW_VOL_L[1]
            and pk is not None and pk <= _DW_MAX_PK_LPM
            and cyc is not None and cyc >= _DW_MIN_CYCLE_PULSES):
        return "dishwasher", "rule_dishwasher"

    if vol is not None and dur is not None and pk is not None:
        if vol >= _SHOWER_BIG_VOL_L and dur >= _SHOWER_BIG_DUR_S and pk >= _SHOWER_BIG_MIN_PK:
            return "shower_tub", "rule_shower"
        if _SHOWER_SMALL_VOL_L[0] <= vol < _SHOWER_SMALL_VOL_L[1] and dur >= _SHOWER_SMALL_DUR_S:
            return "shower_tub", "rule_shower"

    return None
