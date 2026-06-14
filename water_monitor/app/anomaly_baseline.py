"""Phase 2 — locked statistical baseline (leak / odd-usage foundation).

At activation (alongside the rule calibration) this freezes a per-home model of
"normal":

* **Per-fixture usage envelopes** — padded [p5, p95] bands of volume / duration /
  peak for each fixture type, from this home's labelled + matched events. Stored
  frozen in ``usage_baseline``.
* **Overall volume percentiles** — p85/p95/p99 of per-event effective volume,
  written into the dormant ``sensitivity_config.baseline_anomaly_p*`` columns.

Because the baseline is FROZEN at activation, a slow leak cannot drift it (the
boiling-frog protection). A future leak / odd-usage detector compares a live event
against its type's frozen envelope (``event_novelty``); this module lays that
foundation — it does not itself raise alerts.

Frozen at activation/retrain only — never on ordinary reclassify or live events.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

MIN_EVENTS_FOR_ENVELOPE = 8     # a type needs this many events to fit an envelope
_LO_PCT = 5.0
_HI_PCT = 95.0
_PAD = 0.10                     # widen each band by 10% of its span


def _pct(vals: List[Optional[float]], p: float) -> Optional[float]:
    xs = sorted(float(v) for v in vals if v is not None)
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _band(vals: List[Optional[float]]) -> Optional[List[float]]:
    lo = _pct(vals, _LO_PCT)
    hi = _pct(vals, _HI_PCT)
    if lo is None or hi is None:
        return None
    span = max(hi - lo, 0.0)
    return [max(0.0, lo - span * _PAD), hi + span * _PAD]


def fit_usage_baselines(
        conn: sqlite3.Connection,
        circuit: str) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """Compute (per-type envelopes, overall volume percentiles) from this circuit's
    labelled + matched, non-excluded events. Pure read — does not persist."""
    rows = conn.execute(
        "SELECT COALESCE(user_fixture_type, matched_fixture_type) AS t, "
        "       volume_litres, duration_seconds, peak_flow_lpm, "
        "       COALESCE(volume_litres_effective, volume_litres) AS eff_vol "
        "FROM events WHERE circuit = ? "
        "  AND COALESCE(user_fixture_type, matched_fixture_type) IS NOT NULL "
        "  AND COALESCE(excluded_from_training, 0) = 0",
        (circuit,),
    ).fetchall()

    by_type: Dict[str, Dict[str, List]] = {}
    eff_vols: List[float] = []
    for r in rows:
        t = r["t"]
        d = by_type.setdefault(t, {"vol": [], "dur": [], "pk": []})
        d["vol"].append(r["volume_litres"])
        d["dur"].append(r["duration_seconds"])
        d["pk"].append(r["peak_flow_lpm"])
        if r["eff_vol"] is not None:
            eff_vols.append(r["eff_vol"])

    envelopes: Dict[str, Any] = {}
    for t, d in by_type.items():
        if len(d["vol"]) < MIN_EVENTS_FOR_ENVELOPE:
            continue
        env = {"n": len(d["vol"])}
        for key, src in (("vol", "vol"), ("dur", "dur"), ("peak", "pk")):
            b = _band(d[src])
            if b is not None:
                env[key] = b
        envelopes[t] = env

    overall: Dict[str, float] = {}
    for label, p in (("baseline_anomaly_p85", 85.0),
                     ("baseline_anomaly_p95", 95.0),
                     ("baseline_anomaly_p99", 99.0)):
        v = _pct(eff_vols, p)
        if v is not None:
            overall[label] = round(v, 3)
    return envelopes, overall


def freeze_usage_baselines(conn: sqlite3.Connection, circuit: str,
                           source: str = "activation") -> Dict[str, Any]:
    """Fit + persist (freeze) the usage baselines for a circuit. Returns the
    per-type envelope dict."""
    envelopes, overall = fit_usage_baselines(conn, circuit)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO usage_baseline (circuit, params, source, locked_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(circuit) DO UPDATE SET params=excluded.params, "
        "  source=excluded.source, locked_at=excluded.locked_at, "
        "  updated_at=excluded.updated_at",
        (circuit, json.dumps(envelopes), source, now, now),
    )
    if overall:
        from .database import upsert_sensitivity_config
        upsert_sensitivity_config(conn, circuit, **overall)
    conn.commit()
    log.info("[%s] usage baseline frozen (%s): %d type envelope(s); overall %s",
             circuit, source, len(envelopes), overall or "n/a")
    return envelopes


def load_usage_baselines(conn: sqlite3.Connection, circuit: str) -> Dict[str, Any]:
    """Return the frozen per-type envelopes for a circuit, or ``{}``."""
    try:
        row = conn.execute(
            "SELECT params FROM usage_baseline WHERE circuit = ?", (circuit,)
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


def event_novelty(features: Dict[str, Any],
                  baselines: Dict[str, Any]) -> Dict[str, Any]:
    """Score a typed event against its type's FROZEN envelope (read-only).

    Returns ``{fixture_type, fits_baseline, novelty, outside}``:
      * ``fits_baseline`` True when vol/dur/peak all fall inside the envelope,
        False when any is outside, or None when there's no envelope for the type.
      * ``novelty`` = fraction of checked metrics outside the band (0.0–1.0), or
        None when unscorable. This is the hook the future leak/odd-usage detector
        consumes — it is NOT an alert by itself.
    """
    ftype = features.get("user_fixture_type") or features.get("matched_fixture_type")
    env = baselines.get(ftype) if ftype else None
    if not env:
        return {"fixture_type": ftype, "fits_baseline": None,
                "novelty": None, "outside": []}
    checks = (("vol", "volume_litres"), ("dur", "duration_seconds"),
              ("peak", "peak_flow_lpm"))
    checked = 0
    outside: List[str] = []
    for ekey, fkey in checks:
        band = env.get(ekey)
        val = features.get(fkey)
        if band is None or val is None:
            continue
        checked += 1
        if not (band[0] <= float(val) <= band[1]):
            outside.append(ekey)
    if checked == 0:
        return {"fixture_type": ftype, "fits_baseline": None,
                "novelty": None, "outside": []}
    return {"fixture_type": ftype, "fits_baseline": not outside,
            "novelty": round(len(outside) / checked, 3), "outside": outside}
