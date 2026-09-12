"""Locked statistical baseline (leak / odd-usage foundation).

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
import math
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from .database import upsert_sensitivity_config

log = logging.getLogger(__name__)

MIN_EVENTS_FOR_ENVELOPE = 8     # a type needs this many events to fit an envelope
# The overall volume percentiles may window to the pump era only
# when the era holds at least this many events; below it the era estimate of a
# p99 is noise and the all-time fit (more data, slightly stale distribution)
# is the lesser error. Deliberately equals MIN_N_FOR_SHUTOFF: an era window
# thinner than what may authorise a shut-off shouldn't re-anchor the
# percentiles that feed one.
MIN_EVENTS_FOR_OVERALL = 30
_LO_PCT = 5.0
_HI_PCT = 95.0
_PAD = 0.10                     # widen each band by 10% of its span

# Anomaly scoring / shut-off guardrails.
# Which overall-volume percentile the NOTIFY threshold uses, by sensitivity level
# (low = least sensitive → only the most extreme 1% alert).
_NOTIFY_PCT_BY_LEVEL = {"low": "p99", "medium": "p95", "high": "p85"}
# A signal may authorise an automated VALVE CLOSE only if the percentile/envelope
# behind it was fit from at least this many events — a thin or default baseline
# must never close the user's water.
MIN_N_FOR_SHUTOFF = 30
# A typed event is SEVERE on volume only when it grossly exceeds its own type —
# beyond this multiple of the type's band ceiling AND beyond the pooled p99.
# Rule-tier labels are floor-only (rule_shower is ">= 30 L, >= 300 s, >= 6 L/min",
# no ceiling), so a burst hose at 8 L/min is typed "shower_tub" and its peak
# sits inside the shower band: the 2oo3 vote alone never reaches severe. 3x
# keeps every normal shower on record (max 432 L vs a ~250 L ceiling) clear.
_GROSS_EXCEEDANCE = 3.0
# A shut-off response also requires the baseline to have been live (seen real usage)
# for at least this many days since activation — earned trust before it can close
# the user's water. Below this, shut-off levels degrade to notify.
MIN_LIVE_DAYS_FOR_SHUTOFF = 7
# 2oo3 voting for the shape (envelope) channel. At least this many of
# {volume, duration, peak} must be usable before ``event_novelty`` scores at all;
# below it the channel abstains. Otherwise a 1-of-1 outlier reads as novelty 1.0
# → severe → shut-off authorised: maximal confidence from minimal evidence.
_MIN_METRICS_FOR_SHAPE = 2
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

    era_start: when given (the PINNED pump-era anchor — the era,
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
    """Copy the current frozen baseline (envelopes + overall
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

    In a pump-era home the fit windows on the PINNED era anchor
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


def _finite(v: Any) -> Optional[float]:
    """Validate at the BOUNDARY (MISRA C:2023 Dir 4.15).

    Returns ``v`` as a float when it is a real, finite number; ``None`` for
    anything unusable (None, non-numeric, bool, NaN, ±inf).

    A NaN must never reach an ordered comparison. IEEE 754 makes every ordered
    comparison against NaN false, so whether a guard treats it as "in band" or
    "out of band" is an accident of how the comparison happens to be written: a
    NaN metric scores as *outside* and could authorise a valve close, while a
    NaN volume scores as *not exceeding* and suppresses the alert entirely. The
    answer is not to pick a side; it is to stop the value at the boundary so
    both paths ABSTAIN.
    """
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _usable_band(band: Any) -> Optional[Tuple[float, float]]:
    """A stored envelope band as a finite (lo, hi) pair, or None when the band is
    malformed (missing, too short, non-numeric, NaN/inf, or inverted)."""
    if not isinstance(band, (list, tuple)) or len(band) < 2:
        return None
    lo, hi = _finite(band[0]), _finite(band[1])
    if lo is None or hi is None or lo > hi:
        return None
    return lo, hi


def event_novelty(features: Dict[str, Any],
                  baselines: Dict[str, Any]) -> Dict[str, Any]:
    """Score a typed event against its type's FROZEN envelope (read-only).

    Returns ``{fixture_type, fits_baseline, novelty, outside, checked,
    n_metrics, evidence, unusable}``:
      * ``fits_baseline`` True when the usable metrics all fall inside the
        envelope, False when any is outside, or None when unscorable.
      * ``novelty`` = fraction of checked metrics outside the band (0.0–1.0), or
        None when unscorable. This is the hook the leak/odd-usage detector
        consumes — it is NOT an alert by itself.
      * ``evidence`` = the count WITH its denominator ("2 of 3"). A bare ratio
        erases the information that says what the number is worth: 1/1 and 3/3
        are both 1.0, and only one of them is evidence.

    2oo3 voting. ``novelty`` feeds the severe/shut-off threshold, so a single
    populated metric with a single outlier would otherwise yield novelty = 1.0 →
    severe → shut-off authorised: *maximal* confidence from the weakest possible
    evidence. At least ``_MIN_METRICS_FOR_SHAPE`` of {volume, duration, peak}
    must be usable or the shape channel ABSTAINS (novelty None) rather than
    scoring. With the existing shut-off threshold (0.80) an abstain floor of 2
    also means severe requires ≥2 metrics actually exceeded — 2 of 2, or 3 of 3;
    2 of 3 is 0.667, which notifies but cannot close the valve.
    """
    ftype = features.get("user_fixture_type") or features.get("matched_fixture_type")
    env = baselines.get(ftype) if ftype else None
    checks = (("vol", "volume_litres"), ("dur", "duration_seconds"),
              ("peak", "peak_flow_lpm"))
    n_metrics = len(checks)

    def _abstain(checked: int, unusable: List[str]) -> Dict[str, Any]:
        return {"fixture_type": ftype, "fits_baseline": None, "novelty": None,
                "outside": [], "checked": checked, "n_metrics": n_metrics,
                "evidence": f"abstained ({checked} of {n_metrics} usable)",
                "unusable": unusable}

    if not env:
        return _abstain(0, [])
    checked = 0
    outside: List[str] = []
    unusable: List[str] = []
    for ekey, fkey in checks:
        # Boundary validation, both sides: a malformed/partial stored band (e.g.
        # an empty list — the IndexError that broke rescore during retrain) and a
        # non-finite value are BOTH unusable, and an unusable metric is skipped,
        # never counted as evidence in either direction.
        bounds = _usable_band(env.get(ekey))
        raw = features.get(fkey)
        val = _finite(raw)
        if bounds is None or val is None:
            if raw is not None and val is None:
                unusable.append(ekey)      # present but not a finite number
            continue
        checked += 1
        if not (bounds[0] <= val <= bounds[1]):
            outside.append(ekey)
    if unusable:
        log.warning("novelty: %s metric(s) %s are present but not finite — "
                    "excluded from scoring (they can neither raise nor suppress)",
                    ftype, unusable)
    if checked < _MIN_METRICS_FOR_SHAPE:
        # Not enough independent evidence to vote. Abstain — a thin shape channel
        # must never authorise an actuation, and must never mask one either.
        log.debug("novelty: %s abstained — only %d of %d metric(s) usable "
                  "(need %d)", ftype, checked, n_metrics, _MIN_METRICS_FOR_SHAPE)
        return _abstain(checked, unusable)
    return {"fixture_type": ftype, "fits_baseline": not outside,
            "novelty": round(len(outside) / checked, 3), "outside": outside,
            "checked": checked, "n_metrics": n_metrics,
            "evidence": f"{len(outside)} of {checked}", "unusable": unusable}


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
          "is_severe": False, "shutoff_ok_severe": False, "shutoff_ok_any": False,
          "data_quality": None, "shape_evidence": None}


def _threshold(sens_row, key: str, default: float) -> float:
    """A configured threshold, boundary-validated. A NaN/inf/garbage
    threshold silently disables the comparison it guards (every ordered
    comparison against NaN is false), so fall back to the documented default
    and say so, rather than running with a dead gate."""
    raw = _row_get(sens_row, key, default)
    v = _finite(raw)
    if v is None:
        log.warning("sensitivity_config.%s is not a finite number (%r) — using "
                    "the default %s", key, raw, default)
        return default
    return v


def score_event_anomaly(features: Dict[str, Any], baselines: Dict[str, Any],
                        sens_row) -> Dict[str, Any]:
    """Score one event against the FROZEN baseline (read-only).

    Returns ``{score, anomaly_type, is_anomalous, is_severe, shutoff_ok_severe,
    shutoff_ok_any}``:
      * ``is_anomalous`` — crossed the NOTIFY threshold: volume beyond the
        event's reference, or shape beyond ``score_alert``.
      * ``is_severe`` — crossed the SEVERE threshold: volume beyond the pooled
        p99 (untyped events only), or shape beyond ``score_shutoff``.

    The volume REFERENCE depends on whether the event has a fixture type with a
    frozen envelope. A typed event (a toilet, a shower, a washer…) is judged
    against ITS OWN type's volume band — a toilet that starts using more water
    than toilets do notifies; a 35-gal shower does not, even though it dwarfs
    the circuit's pooled percentile, which taps and flushes dominate. The
    pooled percentiles (``baseline_anomaly_p85/p95/p99``, chosen by the
    sensitivity level) apply only to events that fit no known fixture —
    untyped or 'other' — which is where a hose filling a pool, or a leak, lands.
    A typed event is severe on volume only by GROSS exceedance — beyond both
    the pooled p99 and ``_GROSS_EXCEEDANCE`` x its type's ceiling — or by the
    envelope's 2oo3 vote; a type whose envelope is too thin to back a close
    (n < MIN_N_FOR_SHUTOFF) notifies on its own band but defers the severe /
    shut-off decision to the pooled rule. 'other' keeps the pooled volume rule
    and its own envelope for the shape vote.
      * ``shutoff_ok_*`` — the firing signal is backed by a baseline fit from
        ≥ ``MIN_N_FOR_SHUTOFF`` events, so it may authorise a valve close. A thin /
        default baseline yields False → the response degrades to notify.
      * ``data_quality`` — DIAGNOSTIC channel: a '+'-joined tag naming the
        input that could not be scored (``non_finite_volume``,
        ``non_finite_metric``, ``thin_shape_evidence``), or None. Deliberately
        SEPARATE from ``is_anomalous``/``is_severe``: per IEC 61511 degraded-mode
        handling, unusable data must never authorise an actuation and must never
        be folded into the leak alarm either — it abstains and reports itself.
      * ``shape_evidence`` — the envelope vote with its denominator ("2 of 3").

    Inert (everything False/None) for artifact / excluded events, or when no
    baseline exists for the event. NEVER raises an alert or closes a valve — the
    response policy in feature_extractor does that, behind a 'live' state gate.
    """
    # Suppression-averted: the phantom guard would have zeroed a
    # LARGE measured draw; the volume was kept and the event needs the user's
    # eyes. Checked BEFORE the artifact gate (the event is excluded_from_training
    # until reviewed, which would otherwise make it inert) and deterministic
    # across every rescore (column-driven). Never authorises a shut-off — the
    # draw is presumed real use pending review.
    if features.get("phantom_suppression_averted"):
        return {"score": 1.0, "anomaly_type": "suppression_averted",
                "is_anomalous": True, "is_severe": False,
                "shutoff_ok_severe": False, "shutoff_ok_any": False,
                "data_quality": None, "shape_evidence": None}
    if _is_artifact(features):
        return dict(_INERT)

    level = (_row_get(sens_row, "simple_level", "medium") or "medium")
    score_alert = _threshold(sens_row, "score_alert", 0.60)
    score_shutoff = _threshold(sens_row, "score_shutoff", 0.80)
    # Percentiles are boundary-validated too: a non-finite stored percentile
    # makes every ``eff_vol > p`` false, i.e. it silently disables the volume
    # channel. Treat it as absent (unscorable) and say so.
    p85 = _finite(_row_get(sens_row, "baseline_anomaly_p85"))
    p95 = _finite(_row_get(sens_row, "baseline_anomaly_p95"))
    p99 = _finite(_row_get(sens_row, "baseline_anomaly_p99"))
    notify_p = {"p85": p85, "p95": p95, "p99": p99}.get(
        _NOTIFY_PCT_BY_LEVEL.get(level, "p95"))

    # ── the volume boundary ─────────────────────────────────────────────────────
    # A non-finite volume makes ``eff_vol > p`` false, which would suppress the
    # leak alert ENTIRELY — the mirror image of the envelope path, where the same
    # NaN counts as "outside" and could close the valve. Both abstain, and bad
    # data raises its OWN diagnostic instead of being folded into the alarm.
    raw_vol = features.get("volume_litres_effective")
    if raw_vol is None:
        raw_vol = features.get("volume_litres")
    dq: List[str] = []
    if raw_vol is None:
        eff_vol: Optional[float] = 0.0        # absent volume: scores as zero, as before
    else:
        eff_vol = _finite(raw_vol)
        if eff_vol is None:
            dq.append("non_finite_volume")
            log.warning("anomaly scoring: event volume is not a finite number "
                        "(%r) — the volume channel ABSTAINS (no alert, no "
                        "shut-off) pending data repair", raw_vol)

    nov = event_novelty(features, baselines or {})
    shape = _finite(nov.get("novelty"))   # 0..1 or None (unscorable / abstained)
    outside = nov.get("outside") or []
    if nov.get("unusable"):
        dq.append("non_finite_metric")
    if nov.get("checked", 0) and nov.get("novelty") is None:
        dq.append("thin_shape_evidence")

    vol_scorable = eff_vol is not None
    ftype = nov.get("fixture_type")
    type_env = (baselines or {}).get(ftype) if ftype else None
    env_n_ok = int((type_env or {}).get("n", 0)) >= MIN_N_FOR_SHUTOFF
    type_vol = (_usable_band(type_env.get("vol"))
                if type_env and ftype != "other" else None)
    if type_vol is not None and type_vol[1] <= type_vol[0]:
        type_vol = None     # identical training volumes: no width, no reference
    pooled_notify = vol_scorable and notify_p is not None and eff_vol > notify_p
    pooled_severe = vol_scorable and p99 is not None and eff_vol > p99
    if type_vol is not None:
        vol_notify = vol_scorable and eff_vol > type_vol[1]
        gross = _GROSS_EXCEEDANCE * type_vol[1]
        vol_severe = (vol_scorable and eff_vol > max(gross, p99 if p99 is not None else gross)
                      if env_n_ok else pooled_severe)
        vol_tag = "high_volume_type"
    else:
        vol_notify, vol_severe, vol_tag = pooled_notify, pooled_severe, "high_volume"
    shape_notify = shape is not None and shape >= score_alert
    shape_severe = shape is not None and shape >= score_shutoff

    is_anomalous = vol_notify or shape_notify
    is_severe = vol_severe or shape_severe
    if not is_anomalous and not is_severe:
        out = dict(_INERT)
        out["data_quality"] = "+".join(dq) or None
        out["shape_evidence"] = nov.get("evidence")
        return out

    # ── Shut-off confidence gate — the firing signal must be WELL-FIT ────────────
    baseline_n = _row_get(sens_row, "baseline_anomaly_n")
    n_ok = baseline_n is not None and int(baseline_n) >= MIN_N_FOR_SHUTOFF
    if type_vol is not None and env_n_ok:
        shutoff_ok_severe = vol_severe or shape_severe
        shutoff_ok_any = vol_notify or shape_notify
    elif type_vol is not None:
        # Thin type: its band may notify, but only pooled evidence may close.
        shutoff_ok_severe = pooled_severe and n_ok
        shutoff_ok_any = pooled_notify and n_ok
    else:
        shutoff_ok_severe = (vol_severe and n_ok) or (shape_severe and env_n_ok)
        shutoff_ok_any = (vol_notify and n_ok) or (shape_notify and env_n_ok)

    score = max(shape or 0.0, 1.0 if vol_notify else 0.0)
    reasons: List[str] = []
    if vol_notify:
        reasons.append(vol_tag)
    if shape_notify:
        reasons.append("envelope_" + "_".join(outside) if outside else "abnormal_shape")
    return {"score": round(score, 3), "anomaly_type": "+".join(reasons) or None,
            "is_anomalous": is_anomalous, "is_severe": is_severe,
            "shutoff_ok_severe": shutoff_ok_severe, "shutoff_ok_any": shutoff_ok_any,
            # The count WITH its denominator ("2 of 3"), and a diagnostic
            # channel for unusable input that is deliberately kept OUT of
            # is_anomalous / is_severe so bad data can never masquerade as a leak.
            "data_quality": "+".join(dq) or None,
            "shape_evidence": nov.get("evidence")}


def load_sensitivity_row(conn: sqlite3.Connection, circuit: str) -> Optional[Dict[str, Any]]:
    """The circuit's sensitivity_config row as a dict (None when absent),
    independent of the connection's row factory."""
    cur = conn.execute("SELECT * FROM sensitivity_config WHERE circuit = ?", (circuit,))
    row = cur.fetchone()
    return dict(zip([d[0] for d in cur.description], tuple(row))) if row else None


# Every column the scorer reads off a stored event row.
SCORE_COLUMNS = ("volume_litres_effective", "volume_litres", "duration_seconds",
                 "peak_flow_lpm", "phantom_suppression_averted",
                 "user_fixture_type", "matched_fixture_type") + _ARTIFACT_FLAGS


def rescore_stored_event(conn: sqlite3.Connection, circuit: str, event_id: str,
                         baselines: Optional[Dict[str, Any]] = None,
                         sens_row=None) -> Optional[Dict[str, Any]]:
    """Re-judge ONE stored event against the frozen baseline and persist the
    verdict (anomaly_score / anomaly_type / flagged). Storage only — never
    notifies or closes a valve; the live response ran when the event was
    stored. Used when the event's TYPE changes under it (a user label) and by
    the one-shot migration that re-judged old flags. ``baselines`` / ``sens_row``
    may be passed in by a caller looping over many events. Returns the scorer's
    result, or None when the event does not exist. Does not commit.
    """
    row = conn.execute(
        f"SELECT {', '.join(SCORE_COLUMNS)} FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit)).fetchone()
    if row is None:
        return None
    if baselines is None:
        baselines = load_usage_baselines(conn, circuit)
    if sens_row is None:
        sens_row = load_sensitivity_row(conn, circuit)
    # Positional, so a plain-tuple connection scores the same as a Row one.
    av = score_event_anomaly(dict(zip(SCORE_COLUMNS, tuple(row))), baselines, sens_row)
    conn.execute(
        "UPDATE events SET anomaly_score = ?, anomaly_type = ?, flagged = ? "
        "WHERE id = ? AND circuit = ?",
        (av.get("score"), av.get("anomaly_type"),
         1 if av.get("is_anomalous") else 0, event_id, circuit))
    return av
