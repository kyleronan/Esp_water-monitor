"""Locked statistical baseline — what "unusual" is judged against.

Frozen at activation / retrain / regime re-fit (``freeze_usage_baselines``),
never on an ordinary event, so a slow leak cannot drift it. Stored per circuit
in ``usage_baseline.params`` as a dict of per-fixture-type envelopes plus one
reserved ``_novelty`` key:

* per type — the padded ``[p5, p95]`` ``vol``/``dur``/``peak`` bands (advisory:
  they only explain a flag), ``vol_p99`` (the size reference), ``hour_bins``
  (48 half-hour counts: when the fixture normally runs), and what the user has
  said since the freeze — ``user_ok_hi`` (largest volume reviewed "Normal use")
  and ``user_hour_bins``. The user keys are recomputed from the DB on every
  event edit (``refresh_user_knowledge``), so a verdict teaches the same day.
* ``_novelty`` — the home-wide "does anything on file look like this?" check:
  a standardised 10-feature shape space, ``stage1`` (what the user labelled or
  cleared, all-time, live-refreshed) and ``stage2`` (what the app matched on
  its own, current era, capped), and the pool's typical spacing.
* the pooled ``baseline_anomaly_p85/p95/p99`` volume percentiles live in
  ``sensitivity_config`` and judge events that fit no fixture.

``score_event_anomaly`` combines four channels into one verdict: fixture size
(volume beyond the fixture's tail), fixture hour (an hour the fixture has never
run), home-wide novelty (a shape unlike anything known) and the pooled rule for
untyped draws. Only the size channel can authorise a valve close. Nothing is
judged while the circuit is (re)learning — ``judged_from``.
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from .database import upsert_sensitivity_config, is_baseline_locked

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
_PAD = 0.10                     # widen each advisory band by 10% of its span

# ── Size channel ──────────────────────────────────────────────────────────────
# Which overall-volume percentile the pooled NOTIFY threshold uses, by
# sensitivity level (low = least sensitive → only the most extreme 1% alert).
_NOTIFY_PCT_BY_LEVEL = {"low": "p99", "medium": "p95", "high": "p85"}
# A typed event's notify ceiling is its fixture's p99 volume times this margin.
# p99, not p95: a p95 ceiling flags the top 5% of NORMAL use forever; p99 is
# about the second-largest value up to n≈200, so one mislabelled outlier
# barely moves it, and the margin gives the tail room.
_TYPE_CEILING_PCT = 99.0
_TYPE_MARGIN_BY_LEVEL = {"low": 2.0, "medium": 1.5, "high": 1.25}
# SEVERE on volume = beyond this multiple of the notify ceiling AND beyond the
# pooled p99 — a runaway typed as a shower (a hose left on) still goes severe.
_GROSS_EXCEEDANCE = 2.0
# A signal may authorise an automated VALVE CLOSE only if the percentile/envelope
# behind it was fit from at least this many events — a thin or default baseline
# must never close the user's water.
MIN_N_FOR_SHUTOFF = 30
# A shut-off response also requires the baseline to have been live (seen real usage)
# for at least this many days since activation — earned trust before it can close
# the user's water. Below this, shut-off levels degrade to notify.
MIN_LIVE_DAYS_FOR_SHUTOFF = 7
# The advisory envelope vote needs this many usable metrics before it says
# anything ("1 of 1" is not evidence).
_MIN_METRICS_FOR_SHAPE = 2
# Verdict flags that mark an event as already-known-not-real-water (or explicitly
# excluded). Such an event is inert for anomaly scoring — it must never score or
# shut off (a cross-talk pressure transient closing the main would be absurd).
_ARTIFACT_FLAGS = ("is_pressure_restoration_phantom", "is_cross_talk",
                   "is_low_flow_dribble", "excluded_from_training", "user_ignored")

# ── Hour channel ──────────────────────────────────────────────────────────────
_HOUR_BINS = 48                 # half-hour bins of local start hour
# An hour is known when the frozen histogram holds this many events within
# ±window of it, or ONE user statement (a label or a "Normal use" verdict).
_HOUR_MIN_SUPPORT = 2
_HOUR_WINDOW_BY_LEVEL = {"low": 2.0, "medium": 1.5, "high": 1.0}

# ── Novelty channel ───────────────────────────────────────────────────────────
# (column, transform): the 10 always-populated shape columns; no pressure (it
# moves with the supply regime) and no time (that is the hour channel's job).
_NOVELTY_FEATURES: Tuple[Tuple[str, str], ...] = (
    ("volume_litres", "log"), ("duration_seconds", "log"),
    ("peak_flow_lpm", "log"), ("true_avg_flow_lpm", "log"),
    ("flow_variability", "log1p"), ("steady_state_fraction", "raw"),
    ("flow_on_ratio", "raw"), ("active_flow_segment_count", "log1p"),
    ("flow_edge_count", "log1p"), ("flow_cv_on_segments", "log1p"))
_NOVELTY_MIN_VOLUME_L = 2.5     # below this the shape columns are noise
_NOVELTY_MIN_POOL = 100         # calibration needs this many pooled events
_NOVELTY_MIN_STAGE = 20         # a stage thinner than this cannot vouch
_NOVELTY_STAGE2_CAP = 250       # most recent machine matches kept per type
_NOVELTY_NEIGHBOURS = 3         # stage 2 needs the 3rd-nearest within tolerance
_NOVELTY_VOUCH_MIN = 2          # stage-1 neighbours sharing a label to vouch a type
# Tolerance = K × the pool's typical (median) leave-one-out spacing. A median,
# not a tail percentile: p99 inflates on a thin or mixed-era pool and let a
# power washer pass as known.
_NOVELTY_K_BY_LEVEL = {"low": 6.0, "medium": 4.0, "high": 3.0}
# The spacing is measured on a fixed-size, time-even subsample so the
# tolerance means the same thing in a two-month-old home and a two-year-old
# one (nearest-neighbour spacing shrinks as a pool densifies).
_NOVELTY_CALIB_N = 300
_NOVELTY_SCALE_FLOOR = 0.05

# ── Selections ────────────────────────────────────────────────────────────────
# Rows that may define "normal". An open flag (flagged, not yet reviewed
# "Normal use") is a question, not an answer: it is held out of every reference
# where ONE event would vouch — the hour histogram and the machine-match pool —
# but not of the size fit, whose p99 is robust to a few outliers by
# construction (holding it out there is circular: the flags ARE the tail).
_BASE_EXCLUSIONS = (
    "COALESCE(excluded_from_training, 0) = 0 "
    "AND training_quarantine_reason IS NULL "
    "AND COALESCE(training_excluded_by_user, 0) = 0 "
    "AND COALESCE(review_verdict, '') <> 'unknown' "
    "AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
    "AND COALESCE(is_cross_talk, 0) = 0 "
    "AND COALESCE(is_low_flow_dribble, 0) = 0 "
    "AND COALESCE(user_ignored, 0) = 0")
_OPEN_FLAG = "(COALESCE(flagged, 0) = 1 AND COALESCE(review_verdict, '') <> 'normal')"
_FIT_WHERE = ("COALESCE(user_fixture_type, matched_fixture_type) IS NOT NULL AND "
              + _BASE_EXCLUSIONS)
# What the user has said: a label, or a "Normal use" verdict.
_USER_SAID = "(user_fixture_type IS NOT NULL OR review_verdict = 'normal')"
_EFF_VOL = "COALESCE(volume_litres_effective, volume_litres)"
_NOVELTY_COLS = ", ".join(c for c, _ in _NOVELTY_FEATURES)
_NOVELTY_NOT_NULL = " AND ".join(f"{c} IS NOT NULL" for c, _ in _NOVELTY_FEATURES)


def _pct(vals: Sequence[Optional[float]], p: float) -> Optional[float]:
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


def _hour_bins(hours: Sequence[Optional[float]]) -> List[int]:
    bins = [0] * _HOUR_BINS
    for h in hours:
        v = _finite(h)
        if v is None:
            continue
        bins[int((v % 24.0) * 2) % _HOUR_BINS] += 1
    return bins


def _type_keys(params: Dict[str, Any]) -> List[str]:
    """Fixture-type keys of a params dict (reserved ``_`` keys skipped)."""
    return [k for k in params if not k.startswith("_")]


# ── Novelty feature space ─────────────────────────────────────────────────────

def _novelty_vector(features: Dict[str, Any]) -> Optional[List[float]]:
    """The raw (untransformed-then-transformed) shape vector, or None when any
    input is missing / non-finite — an unscorable event abstains, it never
    counts as far or near."""
    out: List[float] = []
    for col, tf in _NOVELTY_FEATURES:
        v = _finite(features.get(col))
        if v is None:
            return None
        if tf == "log":
            out.append(math.log(max(v, 0.01)))
        elif tf == "log1p":
            out.append(math.log1p(max(v, 0.0)))
        else:
            out.append(v)
    return out


def _standardise(vec: Sequence[float], center: Sequence[float],
                 scale: Sequence[float]) -> List[float]:
    return [(v - c) / s for v, c, s in zip(vec, center, scale)]


def _sq_dist(a: Sequence[float], b: Sequence[float]) -> float:
    return sum((x - y) * (x - y) for x, y in zip(a, b))


def _kth_nearest(z: Sequence[float], pool: Sequence[Sequence[float]], k: int,
                 offset: int = 0) -> Optional[float]:
    """Distance to the k-th nearest pool vector (vectors start at ``offset``)."""
    if len(pool) < k:
        return None
    ds = sorted(_sq_dist(z, row[offset:]) for row in pool)
    return math.sqrt(ds[k - 1])


def _fit_novelty_space(calib_rows: List[List[float]]) -> Optional[Dict[str, Any]]:
    """Centre/scale the space on ``calib_rows`` and measure its typical
    leave-one-out spacing. None when the pool is too thin to calibrate."""
    if len(calib_rows) < _NOVELTY_MIN_POOL:
        return None
    dims = len(_NOVELTY_FEATURES)
    cols = [[r[i] for r in calib_rows] for i in range(dims)]
    center = [_pct(c, 50.0) or 0.0 for c in cols]
    scale = [max(((_pct(c, 95.0) or 0.0) - (_pct(c, 5.0) or 0.0)) / 3.29,
                 _NOVELTY_SCALE_FLOOR) for c in cols]
    zc = [_standardise(r, center, scale) for r in calib_rows]
    try:
        import numpy as np
        arr = np.asarray(zc)
        d = np.sqrt(((arr[:, None, :] - arr[None, :, :]) ** 2).sum(-1))
        np.fill_diagonal(d, np.inf)
        kth = np.sort(d, axis=1)[:, _NOVELTY_NEIGHBOURS - 1]
        spacing = float(np.median(kth[np.isfinite(kth)]))
    except ImportError:                     # pragma: no cover — numpy is a hard dep
        kths = []
        for i, z in enumerate(zc):
            ds = sorted(_sq_dist(z, o) for j, o in enumerate(zc) if j != i)
            kths.append(math.sqrt(ds[_NOVELTY_NEIGHBOURS - 1]))
        spacing = _pct(kths, 50.0) or 0.0
    if not (spacing > 0.0):
        return None
    return {"features": [c for c, _ in _NOVELTY_FEATURES],
            "center": [round(c, 4) for c in center],
            "scale": [round(s, 4) for s in scale],
            "spacing": round(spacing, 4), "n": len(calib_rows)}


def _stage1_rows(conn: sqlite3.Connection, circuit: str,
                 center: Sequence[float], scale: Sequence[float]) -> List[List[Any]]:
    """[label, z1..z10] for every event the user labelled or cleared, all-time.
    ``label`` is the user's fixture label ('' for a verdict-only row) — only a
    label may vouch a type for an untyped repeat."""
    rows = conn.execute(
        f"SELECT COALESCE(user_fixture_type, '') AS lbl, {_NOVELTY_COLS} "
        f"FROM events WHERE circuit = ? AND {_USER_SAID} AND {_BASE_EXCLUSIONS} "
        f"  AND {_EFF_VOL} >= ? AND {_NOVELTY_NOT_NULL}",
        (circuit, _NOVELTY_MIN_VOLUME_L)).fetchall()
    out: List[List[Any]] = []
    for r in rows:
        v = _novelty_vector({c: r[c] for c, _ in _NOVELTY_FEATURES})
        if v is not None:
            out.append([r["lbl"]] + [round(x, 3) for x in _standardise(v, center, scale)])
    return out


def _stage1_raw(conn: sqlite3.Connection, circuit: str) -> List[List[float]]:
    rows = conn.execute(
        f"SELECT {_NOVELTY_COLS}, start_ts FROM events WHERE circuit = ? AND {_USER_SAID} "
        f"  AND {_BASE_EXCLUSIONS} AND {_EFF_VOL} >= ? AND {_NOVELTY_NOT_NULL}",
        (circuit, _NOVELTY_MIN_VOLUME_L)).fetchall()
    out = []
    for r in rows:
        v = _novelty_vector({c: r[c] for c, _ in _NOVELTY_FEATURES})
        if v is not None:
            out.append((r["start_ts"], v))
    return out


def _stage2_raw(conn: sqlite3.Connection, circuit: str,
                era_start: Optional[str]) -> List[Tuple[str, List[float]]]:
    """(start_ts, vector) for the app's own matches: current era, most recent
    ``_NOVELTY_STAGE2_CAP`` per type, open flags held out."""
    types = [r[0] for r in conn.execute(
        "SELECT DISTINCT matched_fixture_type FROM events WHERE circuit = ? "
        "  AND user_fixture_type IS NULL AND matched_fixture_type IS NOT NULL",
        (circuit,))]
    out: List[Tuple[str, List[float]]] = []
    for t in types:
        args: List[Any] = [circuit, t, _NOVELTY_MIN_VOLUME_L]
        era_sql = ""
        if era_start:
            era_sql = " AND start_ts >= ?"
            args.append(era_start)
        args.append(_NOVELTY_STAGE2_CAP)
        rows = conn.execute(
            f"SELECT start_ts, {_NOVELTY_COLS} FROM events WHERE circuit = ? "
            f"  AND user_fixture_type IS NULL AND matched_fixture_type = ? "
            f"  AND {_BASE_EXCLUSIONS} AND NOT {_OPEN_FLAG} "
            f"  AND {_EFF_VOL} >= ? AND {_NOVELTY_NOT_NULL}{era_sql} "
            f"ORDER BY start_ts DESC LIMIT ?", args).fetchall()
        for r in rows:
            v = _novelty_vector({c: r[c] for c, _ in _NOVELTY_FEATURES})
            if v is not None:
                out.append((r["start_ts"], v))
    return out


def _time_even_subsample(rows: List[Tuple[str, List[float]]],
                         n: int) -> List[List[float]]:
    """Every k-th row in time order, ``n`` of them (all when fewer)."""
    ordered = [v for _, v in sorted(rows, key=lambda tv: tv[0])]
    if len(ordered) <= n:
        return ordered
    return [ordered[int(i * len(ordered) / n)] for i in range(n)]


def fit_novelty(conn: sqlite3.Connection, circuit: str,
                era_start: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """The ``_novelty`` block for a circuit, or None when too thin. Pure read."""
    s1 = _stage1_raw(conn, circuit)
    s2 = _stage2_raw(conn, circuit, era_start)
    all_rows = s1 + s2
    era_rows = ([tv for tv in s1 if tv[0] >= era_start] + s2) if era_start else all_rows
    use_era = bool(era_start) and len(era_rows) >= _NOVELTY_MIN_POOL
    calib = era_rows if use_era else all_rows
    fitted = _fit_novelty_space(_time_even_subsample(calib, _NOVELTY_CALIB_N))
    if fitted is None:
        return None
    center, scale = fitted["center"], fitted["scale"]
    fitted["stage1"] = _stage1_rows(conn, circuit, center, scale)
    fitted["stage2"] = [[round(x, 3) for x in _standardise(v, center, scale)] for _, v in s2]
    fitted["era"] = use_era
    return fitted


# ── What the user has said ────────────────────────────────────────────────────

def user_clearances(conn: sqlite3.Connection, circuit: str) -> Dict[str, float]:
    """Per type, the largest effective volume the user reviewed "Normal use"."""
    rows = conn.execute(
        f"SELECT COALESCE(user_fixture_type, matched_fixture_type) AS t, "
        f"       MAX({_EFF_VOL}) AS m FROM events "
        f"WHERE circuit = ? AND review_verdict = 'normal' AND {_BASE_EXCLUSIONS} "
        f"  AND COALESCE(user_fixture_type, matched_fixture_type) IS NOT NULL "
        f"GROUP BY 1", (circuit,)).fetchall()
    out: Dict[str, float] = {}
    for r in rows:
        m = _finite(r["m"])
        if m is not None and m > 0:
            out[r["t"]] = round(m, 3)
    return out


def user_hours(conn: sqlite3.Connection, circuit: str) -> Dict[str, List[int]]:
    """Per type, half-hour bins of the hours the user labelled or cleared."""
    rows = conn.execute(
        f"SELECT COALESCE(user_fixture_type, matched_fixture_type) AS t, hour_of_day "
        f"FROM events WHERE circuit = ? AND {_USER_SAID} AND {_BASE_EXCLUSIONS} "
        f"  AND COALESCE(user_fixture_type, matched_fixture_type) IS NOT NULL "
        f"  AND hour_of_day IS NOT NULL", (circuit,)).fetchall()
    by: Dict[str, List[float]] = {}
    for r in rows:
        by.setdefault(r["t"], []).append(r["hour_of_day"])
    return {t: _hour_bins(hs) for t, hs in by.items()}


def refresh_user_knowledge(conn: sqlite3.Connection, circuit: str) -> bool:
    """Recompute the user-taught keys of the frozen params from the DB —
    ``user_ok_hi`` / ``user_hour_bins`` per type and the novelty ``stage1``
    pool — and persist them. Recomputed, never incremented, so clearing a
    verdict lowers a ceiling again (a type with nothing left loses the key).
    The fitted keys are untouched. Does not commit. False when nothing is
    frozen for the circuit."""
    row = conn.execute("SELECT params FROM usage_baseline WHERE circuit = ?",
                       (circuit,)).fetchone()
    if row is None or not row["params"]:
        return False
    try:
        params = json.loads(row["params"])
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(params, dict):
        return False
    ok_hi = user_clearances(conn, circuit)
    hours = user_hours(conn, circuit)
    for t in _type_keys(params):
        env = params[t]
        if not isinstance(env, dict):
            continue
        if t in ok_hi:
            env["user_ok_hi"] = ok_hi[t]
        else:
            env.pop("user_ok_hi", None)
        if t in hours:
            env["user_hour_bins"] = hours[t]
        else:
            env.pop("user_hour_bins", None)
    nov = params.get("_novelty")
    if isinstance(nov, dict) and nov.get("center") and nov.get("scale"):
        nov["stage1"] = _stage1_rows(conn, circuit, nov["center"], nov["scale"])
    conn.execute("UPDATE usage_baseline SET params = ?, updated_at = ? WHERE circuit = ?",
                 (json.dumps(params), datetime.now(timezone.utc).isoformat(), circuit))
    invalidate_baseline_cache(circuit)
    return True


# ── Fit / freeze ──────────────────────────────────────────────────────────────

def fit_usage_baselines(
        conn: sqlite3.Connection,
        circuit: str,
        era_start: Optional[str] = None
        ) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """Compute (per-type envelopes + ``_novelty``, overall volume percentiles)
    from this circuit's labelled + matched events. Pure read — does not persist.

    Held out: events the user reviewed "unknown" (everywhere) and open flags
    (from the hour histogram — a question must not make an hour known).

    era_start: when given (the PINNED pump-era anchor — the era, not the
    current regime, which a recenter/merge can move), each fit prefers
    era-only events and falls back PER TYPE to all-time when the era pool is
    too thin: toilet durations shortened 2.6× under the pump, so a pre-pump
    toilet envelope flags every normal post-pump flush — but a type with 3
    era events can't re-fit yet, and keeping its stale envelope (stamped
    ``era: False``) beats having none. The overall percentiles window the same
    way at MIN_EVENTS_FOR_OVERALL. Hours are all-time (habits don't move with
    the pump); the hour histogram reads the stored ``hour_of_day``, which the
    boot-time tz backfill keeps in the home zone.
    """
    rows = conn.execute(
        "SELECT COALESCE(user_fixture_type, matched_fixture_type) AS t, "
        "       start_ts, volume_litres, duration_seconds, peak_flow_lpm, "
        f"       {_EFF_VOL} AS eff_vol, hour_of_day, "
        f"       {_OPEN_FLAG} AS open_flag "
        f"FROM events WHERE circuit = ? AND {_FIT_WHERE}",
        (circuit,),
    ).fetchall()

    def _collect(only_era: bool):
        by_type: Dict[str, Dict[str, List]] = {}
        vols: List[float] = []
        for r in rows:
            if only_era and era_start and r["start_ts"] < era_start:
                continue
            d = by_type.setdefault(r["t"], {"vol": [], "dur": [], "pk": [], "eff": []})
            d["vol"].append(r["volume_litres"])
            d["dur"].append(r["duration_seconds"])
            d["pk"].append(r["peak_flow_lpm"])
            if r["eff_vol"] is not None:
                d["eff"].append(r["eff_vol"])
                vols.append(r["eff_vol"])
        return by_type, vols

    all_types, all_vols = _collect(only_era=False)
    era_types, era_vols = ((all_types, all_vols) if not era_start
                           else _collect(only_era=True))
    hours_by_type: Dict[str, List[float]] = {}
    for r in rows:
        if not r["open_flag"]:
            hours_by_type.setdefault(r["t"], []).append(r["hour_of_day"])
    ok_hi = user_clearances(conn, circuit)
    u_hours = user_hours(conn, circuit)

    envelopes: Dict[str, Any] = {}
    for t in all_types:
        if t.startswith("_"):
            continue            # a reserved name can never be a fixture type
        # Per-type: era window when it can support a fit, all-time otherwise.
        use_era = (era_start is not None
                   and len(era_types.get(t, {}).get("vol", []))
                   >= MIN_EVENTS_FOR_ENVELOPE)
        d = era_types[t] if use_era else all_types[t]
        if len(d["vol"]) < MIN_EVENTS_FOR_ENVELOPE:
            continue
        env: Dict[str, Any] = {"n": len(d["vol"])}
        if era_start is not None:
            env["era"] = use_era
        for key, src in (("vol", "vol"), ("dur", "dur"), ("peak", "pk")):
            b = _band(d[src])
            if b is not None:
                env[key] = b
        p99 = _pct(d["eff"], _TYPE_CEILING_PCT)
        if p99 is not None and p99 > 0:
            env["vol_p99"] = round(p99, 3)
        env["hour_bins"] = _hour_bins(hours_by_type.get(t, []))
        if t in ok_hi:
            env["user_ok_hi"] = ok_hi[t]
        if t in u_hours:
            env["user_hour_bins"] = u_hours[t]
        envelopes[t] = env
    nov = fit_novelty(conn, circuit, era_start)
    if nov is not None:
        envelopes["_novelty"] = nov

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
    snapshotted first, so a restore is undoable. The user-taught keys are
    re-derived from today's verdicts, not the snapshot's."""
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
    try:
        refresh_user_knowledge(conn, circuit)
    except Exception as e:                          # noqa: BLE001
        log.warning("[%s] user-knowledge refresh after restore failed: %s", circuit, e)
    conn.commit()
    invalidate_baseline_cache(circuit)
    log.info("[%s] usage baseline restored from snapshot %s", circuit,
             row["id"])
    return True


def freeze_usage_baselines(conn: sqlite3.Connection, circuit: str,
                           source: str = "activation") -> Dict[str, Any]:
    """Fit + persist (freeze) the usage baselines for a circuit. Returns the
    params dict (per-type envelopes + ``_novelty``).

    In a pump-era home the fit windows on the PINNED era anchor
    (per-type/overall fallback inside fit_usage_baselines), and the previous
    frozen state is snapshotted first so a refit is revertable. The circuit's
    currently-flagged events are then re-judged against the new reference so
    a flag never outlives the envelope that raised it (clears only)."""
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
    log.info("[%s] usage baseline frozen (%s): %d type envelope(s), novelty %s; "
             "overall %s", circuit, source, len(_type_keys(envelopes)),
             "fit" if "_novelty" in envelopes else "too thin", overall or "n/a")
    try:
        cleared, kept = rejudge_flagged_events(conn, circuit)
        if cleared or kept:
            log.info("[%s] flagged events re-judged after freeze: %d cleared, "
                     "%d still flagged", circuit, cleared, kept)
    except Exception as e:                          # noqa: BLE001 — the freeze itself is done
        log.warning("[%s] post-freeze re-judge failed (baseline is frozen): %s",
                    circuit, e)
    return envelopes


def rescale_anomaly_percentiles(conn: sqlite3.Connection, circuit: str,
                                ratio: float) -> bool:
    """Scale every frozen volume reference by ``ratio``: the pooled p85/p95/p99
    and each type's ``vol`` band, ``vol_p99`` and ``user_ok_hi``.

    Used after a SMALL flow-meter PPL (calibration) trim that did not warrant a
    full re-baseline: future volume readings shift scale by ppl_old/ppl_new, so
    the frozen thresholds are multiplied by the same factor (``ratio = ppl_old /
    ppl_new``) to stay aligned WITHOUT a re-learning window. Forward-looking
    detection thresholds only — historical event volumes are never recomputed.
    The novelty pool's log-volume axis shifts by ln(ratio), far inside its
    scale for a trim, and is left alone. No-op on a non-finite / out-of-range
    ratio or when nothing is frozen. Returns True if it rescaled.
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
    params = load_usage_baselines(conn, circuit, use_cache=False)
    if params:
        for t in _type_keys(params):
            env = params[t]
            if not isinstance(env, dict):
                continue
            band = _usable_band(env.get("vol"))
            if band is not None:
                env["vol"] = [round(band[0] * ratio, 3), round(band[1] * ratio, 3)]
            for key in ("vol_p99", "user_ok_hi"):
                v = _finite(env.get(key))
                if v is not None:
                    env[key] = round(v * ratio, 3)
        conn.execute("UPDATE usage_baseline SET params = ? WHERE circuit = ?",
                     (json.dumps(params), circuit))
    conn.commit()
    invalidate_baseline_cache(circuit)
    log.info("[%s] anomaly volume references re-scaled ×%.4f (calibration trim, "
             "no relearn)", circuit, ratio)
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
    """Return the frozen params for a circuit (per-type envelopes plus the
    reserved ``_novelty`` key), or ``{}``."""
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


# ── Boundary validation ───────────────────────────────────────────────────────

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


def _parse_ts(v: Any) -> Optional[datetime]:
    """An aware UTC datetime from a stored ISO timestamp, or None."""
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t


# ── Part 0: when is anything judged at all ────────────────────────────────────

def judged_from(conn: sqlite3.Connection, circuit: str) -> Optional[datetime]:
    """The instant from which this circuit's events are judged, or None while
    nothing is: the circuit must be live with its baseline locked
    (``database.is_baseline_locked`` — no open recalibration / adaptation
    window), and only events that started after the most recent learning
    window closed — ``max(training_state.completed_at,
    learning_config.accelerated_adaptation_until)`` — are judged. Keyed on the
    event's own start time, so a later re-scan can never back-flag an event
    that happened while the home was (re)learning."""
    try:
        row = conn.execute(
            "SELECT state, completed_at FROM training_state WHERE circuit = ?",
            (circuit,)).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row or row["state"] != "live":
        return None
    if not is_baseline_locked(conn, circuit):
        return None
    marks = [_parse_ts(row["completed_at"])]
    try:
        lc = conn.execute(
            "SELECT accelerated_adaptation_until FROM learning_config WHERE circuit = ?",
            (circuit,)).fetchone()
        if lc:
            marks.append(_parse_ts(lc["accelerated_adaptation_until"]))
    except sqlite3.OperationalError:
        pass
    marks = [m for m in marks if m is not None]
    return max(marks) if marks else datetime(1970, 1, 1, tzinfo=timezone.utc)


def is_judged(start_ts: Any, judged: Optional[datetime]) -> bool:
    """True when an event that started at ``start_ts`` is judged at all."""
    if judged is None:
        return False
    t = _parse_ts(start_ts)
    return t is not None and t >= judged


# ── Channels ──────────────────────────────────────────────────────────────────

def event_novelty(features: Dict[str, Any],
                  baselines: Dict[str, Any]) -> Dict[str, Any]:
    """Advisory envelope vote: which of {volume, duration, peak} sit ABOVE the
    fixture's frozen ``[p5, p95]`` band. One-sided — a smaller, shorter or
    slower draw is not unusual use. It explains a flag (``+envelope_vol_dur``
    tags, ``shape_evidence``) and never decides one.

    Returns ``{fixture_type, fits_baseline, novelty, outside, checked,
    n_metrics, evidence, unusable}``; ``novelty`` = fraction of checked metrics
    above their band, or None when fewer than ``_MIN_METRICS_FOR_SHAPE`` are
    usable (abstain: "1 of 1" is not evidence). ``evidence`` keeps the
    denominator ("2 of 3").
    """
    ftype = features.get("user_fixture_type") or features.get("matched_fixture_type")
    env = baselines.get(ftype) if ftype and not str(ftype).startswith("_") else None
    checks = (("vol", "volume_litres"), ("dur", "duration_seconds"),
              ("peak", "peak_flow_lpm"))
    n_metrics = len(checks)

    def _abstain(checked: int, unusable: List[str]) -> Dict[str, Any]:
        return {"fixture_type": ftype, "fits_baseline": None, "novelty": None,
                "outside": [], "checked": checked, "n_metrics": n_metrics,
                "evidence": f"abstained ({checked} of {n_metrics} usable)",
                "unusable": unusable}

    if not isinstance(env, dict):
        return _abstain(0, [])
    checked = 0
    outside: List[str] = []
    unusable: List[str] = []
    for ekey, fkey in checks:
        # Boundary validation, both sides: a malformed/partial stored band and
        # a non-finite value are BOTH unusable, and an unusable metric is
        # skipped, never counted as evidence in either direction.
        bounds = _usable_band(env.get(ekey))
        raw = features.get(fkey)
        val = _finite(raw)
        if bounds is None or val is None:
            if raw is not None and val is None:
                unusable.append(ekey)      # present but not a finite number
            continue
        checked += 1
        if val > bounds[1]:
            outside.append(ekey)
    if unusable:
        log.warning("novelty: %s metric(s) %s are present but not finite — "
                    "excluded from scoring (they can neither raise nor suppress)",
                    ftype, unusable)
    if checked < _MIN_METRICS_FOR_SHAPE:
        log.debug("novelty: %s abstained — only %d of %d metric(s) usable "
                  "(need %d)", ftype, checked, n_metrics, _MIN_METRICS_FOR_SHAPE)
        return _abstain(checked, unusable)
    return {"fixture_type": ftype, "fits_baseline": not outside,
            "novelty": round(len(outside) / checked, 3), "outside": outside,
            "checked": checked, "n_metrics": n_metrics,
            "evidence": f"{len(outside)} of {checked}", "unusable": unusable}


def hour_known(features: Dict[str, Any], env: Optional[Dict[str, Any]],
               level: str) -> Optional[bool]:
    """Has this fixture run at this local hour before? True / False, or None
    when it cannot say (no hour, no histogram). Known = ``_HOUR_MIN_SUPPORT``
    frozen events within ±window (circular), or ONE user statement within it —
    what the user labelled or cleared counts on its own."""
    hour = _finite(features.get("hour_of_day")) if features else None
    bins = env.get("hour_bins") if isinstance(env, dict) else None
    if hour is None or not isinstance(bins, list) or len(bins) != _HOUR_BINS:
        return None
    if sum(int(_finite(b) or 0) for b in bins) < _HOUR_MIN_SUPPORT:
        return None                 # no hour reference for this fixture at all
    window = _HOUR_WINDOW_BY_LEVEL.get(level, _HOUR_WINDOW_BY_LEVEL["medium"])
    hour %= 24.0
    idx = []
    for i in range(_HOUR_BINS):
        centre = (i + 0.5) / 2.0
        d = abs(centre - hour) % 24.0
        if min(d, 24.0 - d) <= window:
            idx.append(i)
    frozen = sum(int(_finite(bins[i]) or 0) for i in idx)
    if frozen >= _HOUR_MIN_SUPPORT:
        return True
    ubins = env.get("user_hour_bins")
    if isinstance(ubins, list) and len(ubins) == _HOUR_BINS:
        if sum(int(_finite(ubins[i]) or 0) for i in idx) >= 1:
            return True
    return False


def novelty_check(features: Dict[str, Any], baselines: Dict[str, Any],
                  level: str) -> Dict[str, Any]:
    """Does anything on file look like this event? Staged: stage 1 (what the
    user labelled or cleared) passes on its nearest match; stage 2 (what the
    app matched on its own) needs the ``_NOVELTY_NEIGHBOURS``-th nearest within
    tolerance. No match at either stage → novel.

    Returns ``{novel, d1, d3, tolerance, stage, vouch}``: ``novel`` None when
    the channel abstains (nothing fitted, a thin pool, a draw under
    ``_NOVELTY_MIN_VOLUME_L``, a missing feature); ``stage`` names the stage
    that vouched; ``vouch`` is the user label that at least
    ``_NOVELTY_VOUCH_MIN`` of the nearest stage-1 matches share (for an untyped
    repeat of something the user has logged), else None."""
    out: Dict[str, Any] = {"novel": None, "d1": None, "d3": None,
                           "tolerance": None, "stage": None, "vouch": None}
    nov = (baselines or {}).get("_novelty")
    if not isinstance(nov, dict):
        return out
    eff = _finite(features.get("volume_litres_effective"))
    if eff is None:
        eff = _finite(features.get("volume_litres"))
    if eff is None or eff < _NOVELTY_MIN_VOLUME_L:
        return out
    spacing = _finite(nov.get("spacing"))
    center, scale = nov.get("center"), nov.get("scale")
    dims = len(_NOVELTY_FEATURES)
    if (spacing is None or spacing <= 0 or not isinstance(center, list)
            or not isinstance(scale, list) or len(center) != dims or len(scale) != dims):
        return out
    raw = _novelty_vector(features)
    if raw is None:
        return out
    z = _standardise(raw, center, scale)
    tol = spacing * _NOVELTY_K_BY_LEVEL.get(level, _NOVELTY_K_BY_LEVEL["medium"])
    stage1 = nov.get("stage1") or []
    stage2 = nov.get("stage2") or []
    usable1 = len(stage1) >= _NOVELTY_MIN_STAGE
    usable2 = len(stage2) >= _NOVELTY_MIN_STAGE + _NOVELTY_NEIGHBOURS
    if not usable1 and not usable2:
        return out
    out["tolerance"] = round(tol, 4)
    if usable1:
        scored = sorted((_sq_dist(z, row[1:]), row[0]) for row in stage1)
        d1 = math.sqrt(scored[0][0])
        out["d1"] = round(d1, 4)
        if d1 <= tol:
            out["novel"] = False
            out["stage"] = "stage1"
            near = [lbl for d, lbl in scored[:_NOVELTY_NEIGHBOURS]
                    if math.sqrt(d) <= tol and lbl]
            for lbl in set(near):
                if near.count(lbl) >= _NOVELTY_VOUCH_MIN:
                    out["vouch"] = lbl
                    break
            return out
    if usable2:
        d3 = _kth_nearest(z, stage2, _NOVELTY_NEIGHBOURS)
        if d3 is not None:
            out["d3"] = round(d3, 4)
            if d3 <= tol:
                out["novel"] = False
                out["stage"] = "stage2"
                return out
    out["novel"] = True
    return out


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
          "data_quality": None, "shape_evidence": None, "novelty": None,
          "hour_known": None}


def inert_verdict() -> Dict[str, Any]:
    """The verdict of an event that is not judged at all."""
    return dict(_INERT)


def _type_ceiling(env: Optional[Dict[str, Any]], margin: float) -> Optional[float]:
    """A typed event's notify ceiling from its envelope: ``vol_p99 × margin``,
    or — for a params blob frozen before ``vol_p99`` existed — the old band
    ceiling (never the pooled rule, which would flag every shower of the
    type); raised to ``user_ok_hi`` when the user has cleared something bigger."""
    if not isinstance(env, dict):
        return None
    p99 = _finite(env.get("vol_p99"))
    ceiling: Optional[float] = None
    if p99 is not None and p99 > 0:
        ceiling = p99 * margin
    else:
        band = _usable_band(env.get("vol"))
        if band is not None and band[1] > band[0]:
            ceiling = band[1]
    if ceiling is None:
        return None
    ok_hi = _finite(env.get("user_ok_hi"))
    if ok_hi is not None and ok_hi > ceiling:
        ceiling = ok_hi
    return ceiling


def score_event_anomaly(features: Dict[str, Any], baselines: Dict[str, Any],
                        sens_row) -> Dict[str, Any]:
    """Score one event against the FROZEN baseline (read-only).

    Returns ``{score, anomaly_type, is_anomalous, is_severe, shutoff_ok_severe,
    shutoff_ok_any, data_quality, shape_evidence, novelty, hour_known}``.

    Four channels, OR-ed into ``is_anomalous``:
      * size — a typed event beyond its fixture's ceiling (``vol_p99`` × the
        level's margin, or anything the user has cleared as normal); an
        untyped / ``other`` draw beyond the pooled percentile the level picks
        (``other`` may be raised by its own clearances; an untyped draw that
        stage 1 of the novelty check vouches for a user label is judged as
        that label). Tags ``high_volume_type`` / ``high_volume``.
      * hour — a typed event at an hour its fixture has never run
        (``hour_known``). Tag ``odd_hours``.
      * novelty — a shape unlike anything the user labelled or the app matched
        (``novelty_check``). Tag ``novel_pattern``.
      * the envelope vote is advisory only: ``+envelope_…`` tags explain a flag.
    ``is_severe`` (and any shut-off authorisation) comes from the size channel
    alone: gross exceedance — beyond ``_GROSS_EXCEEDANCE`` × the ceiling AND
    the pooled p99 — or, for untyped draws, beyond the pooled p99. A type
    whose envelope is too thin to back a close (n < MIN_N_FOR_SHUTOFF) notifies
    on its own ceiling but defers severe / shut-off to the pooled rule.
    ``shutoff_ok_*`` add the confidence gate: the reference behind the firing
    signal was fit from ≥ MIN_N_FOR_SHUTOFF events. ``score`` is the
    exceedance ratio (≥ 1.0 when flagged, None otherwise). ``data_quality``
    names input that could not be scored (``non_finite_volume``,
    ``non_finite_metric``, ``thin_shape_evidence``) — abstention is reported,
    never folded into the alarm.

    Inert for artifact / excluded events or when no baseline exists. NEVER
    raises an alert or closes a valve — the response policy in
    feature_extractor does that, behind the live-state gate.
    """
    # Suppression-averted: the phantom guard would have zeroed a LARGE
    # measured draw; the volume was kept and the event needs the user's eyes.
    # Checked BEFORE the artifact gate (the event is excluded_from_training
    # until reviewed) and deterministic across every rescore. Never authorises
    # a shut-off — the draw is presumed real use pending review.
    if features.get("phantom_suppression_averted"):
        out = dict(_INERT)
        out.update({"score": 1.0, "anomaly_type": "suppression_averted",
                    "is_anomalous": True})
        return out
    if _is_artifact(features):
        return dict(_INERT)

    level = (_row_get(sens_row, "simple_level", "medium") or "medium")
    margin = _TYPE_MARGIN_BY_LEVEL.get(level, _TYPE_MARGIN_BY_LEVEL["medium"])
    # Percentiles are boundary-validated: a non-finite stored percentile makes
    # every ``eff_vol > p`` false, i.e. it silently disables the volume
    # channel. Treat it as absent (unscorable).
    p85 = _finite(_row_get(sens_row, "baseline_anomaly_p85"))
    p95 = _finite(_row_get(sens_row, "baseline_anomaly_p95"))
    p99 = _finite(_row_get(sens_row, "baseline_anomaly_p99"))
    notify_p = {"p85": p85, "p95": p95, "p99": p99}.get(
        _NOTIFY_PCT_BY_LEVEL.get(level, "p95"))

    # ── the volume boundary ─────────────────────────────────────────────────
    raw_vol = features.get("volume_litres_effective")
    if raw_vol is None:
        raw_vol = features.get("volume_litres")
    dq: List[str] = []
    if raw_vol is None:
        eff_vol: Optional[float] = 0.0        # absent volume: scores as zero
    else:
        eff_vol = _finite(raw_vol)
        if eff_vol is None:
            dq.append("non_finite_volume")
            log.warning("anomaly scoring: event volume is not a finite number "
                        "(%r) — the volume channel ABSTAINS (no alert, no "
                        "shut-off) pending data repair", raw_vol)
    vol_scorable = eff_vol is not None

    baselines = baselines or {}
    nov = event_novelty(features, baselines)
    outside = nov.get("outside") or []
    if nov.get("unusable"):
        dq.append("non_finite_metric")
    if nov.get("checked", 0) and nov.get("novelty") is None:
        dq.append("thin_shape_evidence")
    ftype = nov.get("fixture_type")
    type_env = baselines.get(ftype) if ftype and not str(ftype).startswith("_") else None
    if not isinstance(type_env, dict):
        type_env = None
    env_n_ok = int((type_env or {}).get("n", 0) or 0) >= MIN_N_FOR_SHUTOFF

    nv = novelty_check(features, baselines, level)
    novel = nv.get("novel") is True

    # ── size channel ────────────────────────────────────────────────────────
    ceiling: Optional[float] = None
    judged_as = ftype
    if type_env is not None and ftype != "other":
        ceiling = _type_ceiling(type_env, margin)
    elif not ftype and nv.get("vouch"):
        vouch_env = baselines.get(nv["vouch"])
        if isinstance(vouch_env, dict) and nv["vouch"] != "other":
            ceiling = _type_ceiling(vouch_env, margin)
            if ceiling is not None:
                judged_as = nv["vouch"]
                type_env, env_n_ok = vouch_env, int(vouch_env.get("n", 0) or 0) >= MIN_N_FOR_SHUTOFF
    typed = ceiling is not None
    pooled_notify = vol_scorable and notify_p is not None and eff_vol > notify_p
    pooled_severe = vol_scorable and p99 is not None and eff_vol > p99
    if typed:
        vol_notify = vol_scorable and eff_vol > ceiling
        # Severe needs BOTH lines; an unusable pooled p99 abstains rather than
        # letting the type line alone authorise a close.
        gross = _GROSS_EXCEEDANCE * ceiling
        vol_severe = (vol_notify and p99 is not None and eff_vol > max(gross, p99)
                      if env_n_ok else (vol_notify and pooled_severe))
        vol_tag = "high_volume_type"
    else:
        # Untyped / other / thin: the pooled rule. 'other' is only ever a user
        # label, so what the user cleared as normal raises its line; a vouched
        # 'other' repeat is judged the same way.
        pooled_line = notify_p
        other_key = "other" if (ftype == "other" or nv.get("vouch") == "other") else None
        if other_key and isinstance(baselines.get(other_key), dict):
            ok_hi = _finite(baselines[other_key].get("user_ok_hi"))
            if ok_hi is not None and pooled_line is not None and ok_hi > pooled_line:
                pooled_line = ok_hi
        vol_notify = vol_scorable and pooled_line is not None and eff_vol > pooled_line
        vol_severe = vol_notify and pooled_severe
        vol_tag = "high_volume"
        ceiling = pooled_line

    # ── hour channel (typed events with a fixture of their own) ─────────────
    hk = None
    if type_env is not None and judged_as and judged_as != "other" and ftype:
        hk = hour_known(features, type_env, level)
    odd_hours = hk is False

    is_anomalous = bool(vol_notify or novel or odd_hours)
    is_severe = bool(vol_severe)
    if not is_anomalous:
        out = dict(_INERT)
        out["data_quality"] = "+".join(dq) or None
        out["shape_evidence"] = nov.get("evidence")
        out["novelty"] = nv
        out["hour_known"] = hk
        return out

    # ── Shut-off confidence gate — only the size channel may close ──────────
    baseline_n = _row_get(sens_row, "baseline_anomaly_n")
    n_ok = baseline_n is not None and int(baseline_n) >= MIN_N_FOR_SHUTOFF
    shutoff_ok_severe = shutoff_ok_any = False
    if vol_notify:
        if typed and env_n_ok:
            shutoff_ok_severe, shutoff_ok_any = vol_severe, True
        elif typed:
            # Thin type: its ceiling may notify, but only pooled evidence may close.
            shutoff_ok_severe = pooled_severe and n_ok
            shutoff_ok_any = pooled_notify and n_ok
        else:
            shutoff_ok_severe, shutoff_ok_any = vol_severe and n_ok, n_ok

    if vol_notify and ceiling:
        score = eff_vol / ceiling
    elif novel and nv.get("tolerance"):
        dist = min(d for d in (nv.get("d1"), nv.get("d3")) if d is not None)
        score = dist / nv["tolerance"]
    else:
        score = 1.0
    reasons: List[str] = []
    if vol_notify:
        reasons.append(vol_tag)
    if novel:
        reasons.append("novel_pattern")
    if odd_hours:
        reasons.append("odd_hours")
    if outside:
        reasons.append("envelope_" + "_".join(outside))
    return {"score": round(max(score, 1.0), 3), "anomaly_type": "+".join(reasons),
            "is_anomalous": True, "is_severe": is_severe,
            "shutoff_ok_severe": shutoff_ok_severe, "shutoff_ok_any": shutoff_ok_any,
            "data_quality": "+".join(dq) or None,
            "shape_evidence": nov.get("evidence"),
            "novelty": nv, "hour_known": hk}


def load_sensitivity_row(conn: sqlite3.Connection, circuit: str) -> Optional[Dict[str, Any]]:
    """The circuit's sensitivity_config row as a dict (None when absent),
    independent of the connection's row factory."""
    cur = conn.execute("SELECT * FROM sensitivity_config WHERE circuit = ?", (circuit,))
    row = cur.fetchone()
    return dict(zip([d[0] for d in cur.description], tuple(row))) if row else None


# Every column the scorer reads off a stored event row.
SCORE_COLUMNS = (("volume_litres_effective", "volume_litres", "duration_seconds",
                  "peak_flow_lpm", "phantom_suppression_averted",
                  "user_fixture_type", "matched_fixture_type", "hour_of_day")
                 + tuple(c for c, _ in _NOVELTY_FEATURES
                         if c not in ("volume_litres", "duration_seconds", "peak_flow_lpm"))
                 + _ARTIFACT_FLAGS)


def rescore_stored_event(conn: sqlite3.Connection, circuit: str, event_id: str,
                         baselines: Optional[Dict[str, Any]] = None,
                         sens_row=None, judged: Any = "unset") -> Optional[Dict[str, Any]]:
    """Re-judge ONE stored event against the frozen baseline and persist the
    verdict (anomaly_score / anomaly_type / flagged). Storage only — never
    notifies or closes a valve; the live response ran when the event was
    stored. An event outside the judged window (``judged_from``) gets the
    inert verdict. ``baselines`` / ``sens_row`` / ``judged`` may be passed in
    by a caller looping over many events. Returns the scorer's result, or None
    when the event does not exist. Does not commit.
    """
    row = conn.execute(
        f"SELECT start_ts, {', '.join(SCORE_COLUMNS)} FROM events "
        "WHERE id = ? AND circuit = ?", (event_id, circuit)).fetchone()
    if row is None:
        return None
    if judged == "unset":
        judged = judged_from(conn, circuit)
    if baselines is None:
        baselines = load_usage_baselines(conn, circuit)
    if sens_row is None:
        sens_row = load_sensitivity_row(conn, circuit)
    # Positional, so a plain-tuple connection scores the same as a Row one.
    vals = tuple(row)
    if is_judged(vals[0], judged):
        av = score_event_anomaly(dict(zip(SCORE_COLUMNS, vals[1:])), baselines, sens_row)
    else:
        av = dict(_INERT)
    conn.execute(
        "UPDATE events SET anomaly_score = ?, anomaly_type = ?, flagged = ? "
        "WHERE id = ? AND circuit = ?",
        (av.get("score"), av.get("anomaly_type"),
         1 if av.get("is_anomalous") else 0, event_id, circuit))
    return av


def rejudge_flagged_events(conn: sqlite3.Connection, circuit: str,
                           baselines: Optional[Dict[str, Any]] = None,
                           sens_row=None, commit: bool = True) -> Tuple[int, int]:
    """Re-judge every currently-flagged event of a circuit against the CURRENT
    frozen reference. Visits only ``flagged = 1`` rows, so it can clear a flag
    (a normal draw under a new ceiling, a verdict that raised one, a stale flag
    from a superseded baseline, an event from before go-live) but never raises
    one. Skipped when nothing is frozen — with no reference, un-flagging would
    just erase the flags. Returns (cleared, kept). Commits unless told not to
    (the freeze callers run on a private connection that is closed afterwards,
    so an uncommitted re-judge would be lost)."""
    frozen = conn.execute("SELECT 1 FROM usage_baseline WHERE circuit = ?",
                          (circuit,)).fetchone()
    if sens_row is None:
        sens_row = load_sensitivity_row(conn, circuit)
    if frozen is None or sens_row is None or sens_row.get("baseline_anomaly_p95") is None:
        return (0, 0)
    if baselines is None:
        baselines = load_usage_baselines(conn, circuit, use_cache=False)
    judged = judged_from(conn, circuit)
    ids = [r[0] for r in conn.execute(
        "SELECT id FROM events WHERE circuit = ? AND flagged = 1", (circuit,))]
    cleared = kept = 0
    for event_id in ids:
        try:
            av = rescore_stored_event(conn, circuit, event_id, baselines, sens_row,
                                      judged=judged)
        except Exception as e:                       # best-effort per row
            log.warning("[%s] re-judge of %s failed: %s", circuit, event_id, e)
            continue
        if av is not None and not av.get("is_anomalous"):
            cleared += 1
        else:
            kept += 1
    if commit:
        conn.commit()
    return cleared, kept
