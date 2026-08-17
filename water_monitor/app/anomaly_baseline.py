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
# dev34 B3 — the overall volume percentiles may window to the pump era only
# when the era holds at least this many events; below it the era estimate of a
# p99 is noise and the all-time fit (more data, slightly stale distribution)
# is the lesser error. Deliberately equals MIN_N_FOR_SHUTOFF: an era window
# thinner than what may authorise a shut-off shouldn't re-anchor the
# percentiles that feed one.
MIN_EVENTS_FOR_OVERALL = 30
_LO_PCT = 5.0
_HI_PCT = 95.0
_PAD = 0.10                     # widen each band by 10% of its span

# Phase 2.3 — anomaly scoring / shut-off guardrails.
# Which overall-volume percentile the NOTIFY threshold uses, by sensitivity level
# (low = least sensitive → only the most extreme 1% alert).
_NOTIFY_PCT_BY_LEVEL = {"low": "p99", "medium": "p95", "high": "p85"}
# A signal may authorise an automated VALVE CLOSE only if the percentile/envelope
# behind it was fit from at least this many events — a thin or default baseline
# must never close the user's water.
MIN_N_FOR_SHUTOFF = 30
# A shut-off response also requires the baseline to have been live (seen real usage)
# for at least this many days since activation — earned trust before it can close
# the user's water. Below this, shut-off levels degrade to notify.
MIN_LIVE_DAYS_FOR_SHUTOFF = 7
# Verdict flags that mark an event as already-known-not-real-water (or explicitly
# excluded). Such an event is inert for anomaly scoring — it must never score or
# shut off (a cross-talk pressure transient closing the main would be absurd).
_ARTIFACT_FLAGS = ("is_pressure_restoration_phantom", "is_cross_talk",
                   "is_low_flow_dribble", "excluded_from_training", "user_ignored")


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
        circuit: str,
        era_start: Optional[str] = None
        ) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """Compute (per-type envelopes, overall volume percentiles) from this circuit's
    labelled + matched, non-excluded events. Pure read — does not persist.

    Events the user reviewed as 'unknown' ("I don't recognise this") are held
    out: a draw the user couldn't identify must never stretch a fixture
    envelope or the overall percentiles toward "normal", even when a machine
    label matched it. A later relabel clears the verdict and readmits it.

    era_start (dev34 B3): when given (the PINNED pump-era anchor — the era,
    not the current regime, which a recenter/merge can move), each fit prefers
    era-only events and falls back PER TYPE to all-time when the era pool is
    too thin: toilet durations shortened 2.6× under the pump, so a pre-pump
    toilet envelope flags every normal post-pump flush — but a type with 3
    era events can't re-fit yet, and keeping its stale envelope (stamped
    ``era: False``) beats having none. The overall percentiles window the same
    way at MIN_EVENTS_FOR_OVERALL.
    """
    rows = conn.execute(
        "SELECT COALESCE(user_fixture_type, matched_fixture_type) AS t, "
        "       start_ts, volume_litres, duration_seconds, peak_flow_lpm, "
        "       COALESCE(volume_litres_effective, volume_litres) AS eff_vol "
        "FROM events WHERE circuit = ? "
        "  AND COALESCE(user_fixture_type, matched_fixture_type) IS NOT NULL "
        "  AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND training_quarantine_reason IS NULL "
        "  AND COALESCE(training_excluded_by_user, 0) = 0 "
        "  AND COALESCE(review_verdict, '') <> 'unknown'",
        (circuit,),
    ).fetchall()

    def _collect(only_era: bool):
        by_type: Dict[str, Dict[str, List]] = {}
        vols: List[float] = []
        for r in rows:
            if only_era and era_start and r["start_ts"] < era_start:
                continue
            d = by_type.setdefault(r["t"], {"vol": [], "dur": [], "pk": []})
            d["vol"].append(r["volume_litres"])
            d["dur"].append(r["duration_seconds"])
            d["pk"].append(r["peak_flow_lpm"])
            if r["eff_vol"] is not None:
                vols.append(r["eff_vol"])
        return by_type, vols

    all_types, all_vols = _collect(only_era=False)
    era_types, era_vols = ((all_types, all_vols) if not era_start
                           else _collect(only_era=True))

    envelopes: Dict[str, Any] = {}
    for t in all_types:
        # Per-type: era window when it can support a fit, all-time otherwise.
        use_era = (era_start is not None
                   and len(era_types.get(t, {}).get("vol", []))
                   >= MIN_EVENTS_FOR_ENVELOPE)
        d = era_types[t] if use_era else all_types[t]
        if len(d["vol"]) < MIN_EVENTS_FOR_ENVELOPE:
            continue
        env = {"n": len(d["vol"])}
        if era_start is not None:
            env["era"] = use_era
        for key, src in (("vol", "vol"), ("dur", "dur"), ("peak", "pk")):
            b = _band(d[src])
            if b is not None:
                env[key] = b
        envelopes[t] = env

    use_era_overall = (era_start is not None
                       and len(era_vols) >= MIN_EVENTS_FOR_OVERALL)
    eff_vols = era_vols if use_era_overall else all_vols
    overall: Dict[str, float] = {}
    for label, p in (("baseline_anomaly_p85", 85.0),
                     ("baseline_anomaly_p95", 95.0),
                     ("baseline_anomaly_p99", 99.0)):
        v = _pct(eff_vols, p)
        if v is not None:
            overall[label] = round(v, 3)
    # Event count behind the percentiles — the shut-off confidence gate reads this
    # (always written, even 0, so a thin baseline is distinguishable from "no row").
    overall["baseline_anomaly_n"] = len(eff_vols)
    if era_start is not None and not use_era_overall:
        log.info("[%s] usage baseline: era window too thin for overall "
                 "percentiles (%d < %d) — all-time fit kept", circuit,
                 len(era_vols), MIN_EVENTS_FOR_OVERALL)
    return envelopes, overall


def snapshot_usage_baselines(conn: sqlite3.Connection, circuit: str,
                             reason: str) -> None:
    """dev34 B3 — copy the current frozen baseline (envelopes + overall
    anomaly percentiles) into baseline_snapshot before an overwrite, so a
    regime refit that lands badly is revertable (restore_usage_baselines).
    Keeps the newest 10 per circuit. No-op when nothing is frozen yet."""
    row = conn.execute("SELECT params, source, locked_at FROM usage_baseline "
                       "WHERE circuit = ?", (circuit,)).fetchone()
    if row is None:
        return
    sens = conn.execute(
        "SELECT baseline_anomaly_p85, baseline_anomaly_p95, "
        "       baseline_anomaly_p99, baseline_anomaly_n "
        "FROM sensitivity_config WHERE circuit = ?", (circuit,)).fetchone()
    conn.execute(
        "INSERT INTO baseline_snapshot (circuit, reason, params, source, "
        "  locked_at, sensitivity_json) VALUES (?, ?, ?, ?, ?, ?)",
        (circuit, reason, row["params"], row["source"], row["locked_at"],
         json.dumps(dict(sens) if sens else {})))
    conn.execute(
        "DELETE FROM baseline_snapshot WHERE circuit = ? AND id NOT IN "
        "(SELECT id FROM baseline_snapshot WHERE circuit = ? "
        " ORDER BY id DESC LIMIT 10)", (circuit, circuit))
    conn.commit()


def restore_usage_baselines(conn: sqlite3.Connection, circuit: str,
                            snapshot_id: Optional[int] = None) -> bool:
    """Restore the frozen baseline from a snapshot (newest by default).
    Returns False when no snapshot exists. The replaced state is itself
    snapshotted first, so a restore is undoable."""
    q = "SELECT * FROM baseline_snapshot WHERE circuit = ?"
    args: list = [circuit]
    if snapshot_id is not None:
        q += " AND id = ?"
        args.append(snapshot_id)
    row = conn.execute(q + " ORDER BY id DESC LIMIT 1", args).fetchone()
    if row is None:
        return False
    snapshot_usage_baselines(conn, circuit, reason="pre_restore")
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO usage_baseline (circuit, params, source, locked_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(circuit) DO UPDATE SET params=excluded.params, "
        "  source=excluded.source, locked_at=excluded.locked_at, "
        "  updated_at=excluded.updated_at",
        (circuit, row["params"], f"restored:{row['source']}",
         row["locked_at"], now))
    sens = json.loads(row["sensitivity_json"] or "{}")
    sens = {k: v for k, v in sens.items() if v is not None}
    if sens:
        from .database import upsert_sensitivity_config
        upsert_sensitivity_config(conn, circuit, baseline_computed_at=now,
                                  **sens)
    conn.commit()
    invalidate_baseline_cache(circuit)
    log.info("[%s] usage baseline restored from snapshot %s", circuit,
             row["id"])
    return True


def freeze_usage_baselines(conn: sqlite3.Connection, circuit: str,
                           source: str = "activation") -> Dict[str, Any]:
    """Fit + persist (freeze) the usage baselines for a circuit. Returns the
    per-type envelope dict.

    dev34 B3: in a pump-era home the fit windows on the PINNED era anchor
    (per-type/overall fallback inside fit_usage_baselines), and the previous
    frozen state is snapshotted first so a refit is revertable."""
    era = None
    try:
        from .supply_regime import pump_era_start
        era = pump_era_start(conn)
    except Exception:
        era = None
    try:
        snapshot_usage_baselines(conn, circuit, reason=source)
    except Exception as e:
        log.warning("[%s] baseline snapshot failed (freeze continues): %s",
                    circuit, e)
    envelopes, overall = fit_usage_baselines(conn, circuit, era_start=era)
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
        upsert_sensitivity_config(conn, circuit, baseline_computed_at=now, **overall)
    conn.commit()
    invalidate_baseline_cache(circuit)
    log.info("[%s] usage baseline frozen (%s): %d type envelope(s); overall %s",
             circuit, source, len(envelopes), overall or "n/a")
    return envelopes


def rescale_anomaly_percentiles(conn: sqlite3.Connection, circuit: str,
                                ratio: float) -> bool:
    """Scale the frozen overall-volume anomaly percentiles (p85/p95/p99) by ``ratio``.

    Used after a SMALL flow-meter PPL (calibration) trim that did not warrant a full
    re-baseline: future volume readings shift scale by ppl_old/ppl_new, so the frozen
    shut-off / notify thresholds are multiplied by the same factor (``ratio = ppl_old /
    ppl_new``) to stay aligned with the new scale WITHOUT a re-learning window. This
    adjusts forward-looking DETECTION thresholds only — it does NOT touch historical
    event volumes (the never-recompute invariant holds). No-op on a non-finite /
    out-of-range ratio or when no frozen percentiles exist. Returns True if it rescaled.
    """
    if not (0.0 < ratio < 1e6):
        return False
    try:
        row = conn.execute(
            "SELECT baseline_anomaly_p85, baseline_anomaly_p95, baseline_anomaly_p99 "
            "FROM sensitivity_config WHERE circuit = ?", (circuit,)).fetchone()
    except sqlite3.OperationalError:
        return False
    if row is None:
        return False
    updates: Dict[str, Any] = {}
    for col in ("baseline_anomaly_p85", "baseline_anomaly_p95", "baseline_anomaly_p99"):
        v = row[col]
        if v is not None:
            try:
                updates[col] = round(float(v) * ratio, 3)
            except (TypeError, ValueError):
                pass
    if not updates:
        return False
    from .database import upsert_sensitivity_config
    upsert_sensitivity_config(conn, circuit, **updates)
    conn.commit()
    invalidate_baseline_cache(circuit)
    log.info("[%s] anomaly percentiles re-scaled ×%.4f (calibration trim, no relearn)",
             circuit, ratio)
    return True


# Per-circuit cache so the live persist / reclassify hot paths don't read the DB
# per event. The baseline only changes at freeze, which invalidates the entry.
_baseline_cache: Dict[str, Dict[str, Any]] = {}


def invalidate_baseline_cache(circuit: Optional[str] = None) -> None:
    if circuit is None:
        _baseline_cache.clear()
    else:
        _baseline_cache.pop(circuit, None)


def load_usage_baselines(conn: sqlite3.Connection, circuit: str,
                         *, use_cache: bool = True) -> Dict[str, Any]:
    """Return the frozen per-type envelopes for a circuit, or ``{}``."""
    if use_cache and circuit in _baseline_cache:
        return _baseline_cache[circuit]
    data: Dict[str, Any] = {}
    try:
        row = conn.execute(
            "SELECT params FROM usage_baseline WHERE circuit = ?", (circuit,)
        ).fetchone()
    except sqlite3.OperationalError:
        return {}
    if row and row["params"]:
        try:
            parsed = json.loads(row["params"])
            if isinstance(parsed, dict):
                data = parsed
        except (json.JSONDecodeError, TypeError):
            data = {}
    if use_cache:
        _baseline_cache[circuit] = data
    return data


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
        # len(band) < 2 guard: a malformed/partial stored envelope band (e.g. an
        # empty list) must skip, not raise IndexError on band[0]/band[1] below.
        if band is None or len(band) < 2 or val is None:
            continue
        checked += 1
        if not (band[0] <= float(val) <= band[1]):
            outside.append(ekey)
    if checked == 0:
        return {"fixture_type": ftype, "fits_baseline": None,
                "novelty": None, "outside": []}
    return {"fixture_type": ftype, "fits_baseline": not outside,
            "novelty": round(len(outside) / checked, 3), "outside": outside}


def _is_artifact(features: Dict[str, Any]) -> bool:
    """True when the event is already known-not-real-water or explicitly excluded."""
    return any(bool(features.get(f)) for f in _ARTIFACT_FLAGS)


def _row_get(row, key, default=None):
    """Read a key from a sqlite3.Row OR a dict, tolerant of missing columns."""
    if row is None:
        return default
    try:
        keys = row.keys()  # sqlite3.Row
    except AttributeError:
        return row.get(key, default)  # dict
    if key not in keys:
        return default
    v = row[key]
    return v if v is not None else default


_INERT = {"score": None, "anomaly_type": None, "is_anomalous": False,
          "is_severe": False, "shutoff_ok_severe": False, "shutoff_ok_any": False}


def score_event_anomaly(features: Dict[str, Any], baselines: Dict[str, Any],
                        sens_row) -> Dict[str, Any]:
    """Score one event against the FROZEN baseline (read-only).

    Returns ``{score, anomaly_type, is_anomalous, is_severe, shutoff_ok_severe,
    shutoff_ok_any}``:
      * ``is_anomalous`` — crossed the sensitivity NOTIFY threshold (volume beyond
        the level's percentile, or shape beyond ``score_alert``).
      * ``is_severe`` — crossed the SEVERE threshold (volume beyond p99, or shape
        beyond ``score_shutoff``).
      * ``shutoff_ok_*`` — the firing signal is backed by a baseline fit from
        ≥ ``MIN_N_FOR_SHUTOFF`` events, so it may authorise a valve close. A thin /
        default baseline yields False → the response degrades to notify.

    Inert (everything False/None) for artifact / excluded events, or when no
    baseline exists for the event. NEVER raises an alert or closes a valve — the
    response policy in feature_extractor does that, behind a 'live' state gate.
    """
    # Suppression-averted (Phase 2b): the phantom guard would have zeroed a
    # LARGE measured draw; the volume was kept and the event needs the user's
    # eyes. Checked BEFORE the artifact gate (the event is excluded_from_training
    # until reviewed, which would otherwise make it inert) and deterministic
    # across every rescore (column-driven). Never authorises a shut-off — the
    # draw is presumed real use pending review.
    if features.get("phantom_suppression_averted"):
        return {"score": 1.0, "anomaly_type": "suppression_averted",
                "is_anomalous": True, "is_severe": False,
                "shutoff_ok_severe": False, "shutoff_ok_any": False}
    if _is_artifact(features):
        return dict(_INERT)

    level = (_row_get(sens_row, "simple_level", "medium") or "medium")
    score_alert = float(_row_get(sens_row, "score_alert", 0.60))
    score_shutoff = float(_row_get(sens_row, "score_shutoff", 0.80))
    p85 = _row_get(sens_row, "baseline_anomaly_p85")
    p95 = _row_get(sens_row, "baseline_anomaly_p95")
    p99 = _row_get(sens_row, "baseline_anomaly_p99")
    notify_p = {"p85": p85, "p95": p95, "p99": p99}.get(
        _NOTIFY_PCT_BY_LEVEL.get(level, "p95"))

    eff_vol = features.get("volume_litres_effective")
    if eff_vol is None:
        eff_vol = features.get("volume_litres")
    eff_vol = float(eff_vol or 0.0)

    nov = event_novelty(features, baselines or {})
    shape = nov.get("novelty")            # 0..1 or None (no envelope for the type)
    outside = nov.get("outside") or []

    vol_notify = notify_p is not None and eff_vol > float(notify_p)
    vol_severe = p99 is not None and eff_vol > float(p99)
    shape_notify = shape is not None and shape >= score_alert
    shape_severe = shape is not None and shape >= score_shutoff

    is_anomalous = vol_notify or shape_notify
    is_severe = vol_severe or shape_severe
    if not is_anomalous and not is_severe:
        return dict(_INERT)

    # ── Shut-off confidence gate — the firing signal must be WELL-FIT ────────────
    baseline_n = _row_get(sens_row, "baseline_anomaly_n")
    n_ok = baseline_n is not None and int(baseline_n) >= MIN_N_FOR_SHUTOFF
    type_env = (baselines or {}).get(nov.get("fixture_type")) or {}
    env_n_ok = int(type_env.get("n", 0)) >= MIN_N_FOR_SHUTOFF
    shutoff_ok_severe = (vol_severe and n_ok) or (shape_severe and env_n_ok)
    shutoff_ok_any = (vol_notify and n_ok) or (shape_notify and env_n_ok)

    score = max(shape or 0.0, 1.0 if vol_notify else 0.0)
    reasons: List[str] = []
    if vol_notify:
        reasons.append("high_volume")
    if shape_notify:
        reasons.append("envelope_" + "_".join(outside) if outside else "abnormal_shape")
    return {"score": round(score, 3), "anomaly_type": "+".join(reasons) or None,
            "is_anomalous": is_anomalous, "is_severe": is_severe,
            "shutoff_ok_severe": shutoff_ok_severe, "shutoff_ok_any": shutoff_ok_any}
