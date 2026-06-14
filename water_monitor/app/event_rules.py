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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

# ── Low-flow chatter predicate (dev.24; shared by the detector off-grace AND the
#    history coalesce so both agree on what a "sustained low draw" is) ──────────
# The turbine flow sensor can't hold a reading at very low flow, so a continuous
# low draw chatters into many tiny events. The live detector holds an event open
# through sub-threshold dips (event_detector); the post-hoc coalesce merges any
# fragments that slipped through (database). Both gate on THIS one predicate so
# the boundary can never drift apart. eval-gated placeholders.
LOWFLOW_CEIL_LPM: float = 1.5        # mean flow below this == low-flow regime
LOWFLOW_PK_CEIL_LPM: float = 4.0     # ...and no spike reaches this (excludes dishwasher fills/toilets)
LOWFLOW_OFF_GRACE_S: float = 120.0   # detector hold window AND coalesce max inter-fragment gap


def is_low_flow_chatter(mean_flow: Optional[float], peak: Optional[float]) -> bool:
    """True when an event's flow profile is a sustained LOW draw the turbine
    fragments: mean below ``LOWFLOW_CEIL_LPM`` and no spike reaching
    ``LOWFLOW_PK_CEIL_LPM``. Single-sourced so the live off-grace and the
    post-hoc coalesce never disagree on the boundary."""
    if mean_flow is None or peak is None:
        return False
    return mean_flow < LOWFLOW_CEIL_LPM and peak < LOWFLOW_PK_CEIL_LPM


def parse_hhmm_to_minutes(value: Optional[str]) -> Optional[int]:
    """Parse 'HH:MM' (24-hour, local) to minutes-since-midnight, or None if
    invalid/blank. Single source shared by the setup + settings validators, the
    softener session detector's band center, and the leak-test regen blackout —
    so 'is this a valid regen time' and 'what minute is it' never diverge."""
    if not value:
        return None
    parts = str(value).strip().split(":")
    if len(parts) != 2:
        return None
    try:
        h, m = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    return h * 60 + m if (0 <= h < 24 and 0 <= m < 60) else None


# ── Home timezone (dev.24) ────────────────────────────────────────────────────
# The softener regen band is a LOCAL clock time, but events are stored in UTC.
# The orchestrator caches the HA timezone here ONCE (set_home_timezone) at
# tz-detection, so reclassify + the live path can match the band in local time
# without every caller having to thread a tzinfo through. None → compare in UTC
# (tests / pre-detection); the explicit ``ha_tz`` argument still overrides it.
_HOME_TZ = None


def set_home_timezone(tz) -> None:
    """Cache the home timezone (a tzinfo) for the softener regen-band match."""
    global _HOME_TZ
    _HOME_TZ = tz


def get_home_timezone():
    """Return the cached home timezone, or None if not yet detected."""
    return _HOME_TZ


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
) -> Dict[str, Tuple[str, str]]:
    """Find washer-cycle members on ``circuit``: anchors (main fills) that have at
    least one same-peak sibling 2-45 min away, plus the family's non-flush-shaped
    members (top-offs + secondary fills). Returns ``{event_id: (role, group_id)}``
    where role is ``'anchor'``/``'member'`` and group_id is the anchor's event id
    (the History cycle-rollup key, dev.24 §7).

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

    out: Dict[str, Tuple[str, str]] = {}
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
        out[a["id"]] = ("anchor", a["id"])
        for o in family:
            feats = {"volume_litres": o["volume_litres"],
                     "duration_seconds": o["duration_seconds"],
                     "peak_flow_lpm": o["peak_flow_lpm"],
                     "has_pressure_transient": o["has_pressure_transient"],
                     "pressure_delta_psi": o["pressure_delta_psi"]}
            if is_flush_shaped(feats):
                continue                   # flush during laundry — leave it alone
            out.setdefault(o["id"], ("member", a["id"]))
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


# ── Water-softener session detector (dev.24; in-sample, eval-gated) ────────────
# A regeneration is a long LOW-flow brine draw at a fixed overnight clock time,
# followed by one or more steady backwash/rinse fills. It is demand-initiated
# (runs every ~2 weeks, not nightly) but ALWAYS starts at the same time — so the
# configured start band + the >=90-min span are the discriminators, NOT a pulse
# count (coalescing reduces the brine to a few long events, so a count gate would
# be fragile). The backwash looks like a shower (vol ~219 L, peak ~20) and only
# SESSION CONTEXT disambiguates it — within a confirmed session window a non-flush
# fill IS the backwash.
_SOFTENER_LOWFLOW_CEIL_LPM: float = 1.5     # brine draw mean-flow ceiling
_SOFTENER_MIN_SPAN_MIN: float = 90.0        # a real regen runs ~2.5 h; >=90 min gate
_SOFTENER_MAX_SPAN_MIN: float = 210.0       # ...and <=3.5 h — caps the chain so it
#                                             can't walk into late-morning low-flow
#                                             activity (observed over-chain to 4.5 h)
_SOFTENER_CHAIN_GAP_MIN: float = 45.0       # max gap between consecutive session events
_SOFTENER_START_BAND_MIN: float = 20.0      # +/- around the configured regen start
_SOFTENER_BACKWASH_TAIL_MIN: float = 30.0   # grab a trailing backwash this long after
_SOFTENER_POST_BACKWASH_LOWFLOW_MIN: float = 10.0  # after the refill, stop chaining
#                                             low-flow beyond this short rinse tail
#                                             (post-regen morning activity is not it)
_SOFTENER_BACKWASH_MIN_VOL_L: float = 30.0  # a TERMINAL backwash/refill is a big fill
#                                             (~220 L observed); a brief high-peak
#                                             blip mid-brine (<30 L) is not, so it must
#                                             not trip the post-backwash low-flow cutoff


def _softener_feat(r) -> Dict[str, Any]:
    """Build the is_flush_shaped feature dict from an event row."""
    return {
        "volume_litres": r["volume_litres"],
        "duration_seconds": r["duration_seconds"],
        "peak_flow_lpm": r["peak_flow_lpm"],
        "has_pressure_transient": r["has_pressure_transient"],
        "pressure_delta_psi": r["pressure_delta_psi"],
    }


def _softener_mean(r) -> Optional[float]:
    """Active mean flow for the low-flow gate (true_avg preferred, avg fallback)."""
    m = r["true_avg_flow_lpm"]
    return r["avg_flow_lpm"] if m is None else m


def detect_softener_sessions(
    conn: sqlite3.Connection,
    circuit: str,
    band_center_min: int,
    since_ts: Optional[str] = None,
    tz=None,
) -> Dict[str, Tuple[str, str]]:
    """Find water-softener regeneration sessions on ``circuit``.

    Returns ``{event_id: (role, group_id)}`` where role is ``'span'`` (a low-flow
    brine/rinse event) or ``'backwash'`` (a non-flush steady fill within the
    session window), and group_id is the session's first chain-event id (the
    History rollup key, §7).

    A session is a run of consecutive NON-flush events — each within
    ``_SOFTENER_CHAIN_GAP_MIN`` of the previous (and within ``_SOFTENER_MAX_SPAN_
    MIN`` of the start) — that STARTS with a low-flow event whose local clock time
    falls within ``band_center_min`` ± ``_SOFTENER_START_BAND_MIN``, AND whose
    LOW-FLOW brine itself spans >= ``_SOFTENER_MIN_SPAN_MIN`` (a single low-flow
    blip followed by moderate-flow draws is incidental morning activity, not a
    regen). A flush-shaped event ends the run (a 3 am flush during laundry/regen
    is irreducibly ambiguous, left to the per-event tiers). A trailing backwash up
    to ``_SOFTENER_BACKWASH_TAIL_MIN`` past the run is also claimed.

    ``band_center_min`` is minutes-since-LOCAL-midnight (parse_hhmm_to_minutes of
    the user's regen time). ``tz`` is the home timezone used to convert each
    event's stored-UTC start_ts to local for the band test — pass it so the band
    is DST-correct; tz=None compares in the stored (UTC) clock (tests/eval).
    ``since_ts`` bounds the scan for the live trailing pass. Reads ONLY
    feature/timestamp columns — never a label — so the eval's LOO stays honest.
    """
    where = "WHERE circuit = ?"
    params: list = [circuit]
    if since_ts is not None:
        where += " AND start_ts >= ?"
        params.append(since_ts)
    rows = conn.execute(
        "SELECT id, start_ts, end_ts, duration_seconds, volume_litres, "
        "       avg_flow_lpm, true_avg_flow_lpm, peak_flow_lpm, "
        "       has_pressure_transient, pressure_delta_psi "
        f"FROM events {where} ORDER BY start_ts ASC",
        params,
    ).fetchall()

    def _dt(v):
        try:
            d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d

    evs = []
    for r in rows:
        sdt = _dt(r["start_ts"])
        if sdt is None:
            continue
        edt = _dt(r["end_ts"]) or sdt
        evs.append((sdt, edt, r))

    def _is_low(r) -> bool:
        m = _softener_mean(r)
        return m is not None and m < _SOFTENER_LOWFLOW_CEIL_LPM

    def _in_band(dt) -> bool:
        local = dt.astimezone(tz) if tz is not None else dt
        minute = local.hour * 60 + local.minute
        diff = abs(minute - band_center_min) % 1440
        return min(diff, 1440 - diff) <= _SOFTENER_START_BAND_MIN

    chain_gap = _SOFTENER_CHAIN_GAP_MIN * 60.0
    max_span = _SOFTENER_MAX_SPAN_MIN * 60.0
    tail = timedelta(minutes=_SOFTENER_BACKWASH_TAIL_MIN)
    n = len(evs)
    out: Dict[str, Tuple[str, str]] = {}
    claimed = set()

    for i in range(n):
        sdt, edt, r = evs[i]
        if r["id"] in claimed or not _is_low(r) or not _in_band(sdt):
            continue
        # Walk the run of consecutive non-flush events (a flush ends it).
        chain = [(sdt, edt, r)]
        last_end = edt
        last_low_end = edt                        # the band start is low by construction
        last_bw_end = None                        # end of the last backwash/refill fill
        post_bw = _SOFTENER_POST_BACKWASH_LOWFLOW_MIN * 60.0
        j = i + 1
        while j < n:
            s2, e2, r2 = evs[j]
            if is_flush_shaped(_softener_feat(r2)):
                break
            if (s2 - last_end).total_seconds() > chain_gap:
                break
            if (s2 - sdt).total_seconds() > max_span:
                break                             # cap span — a regen isn't all morning
            low = _is_low(r2)
            # The TERMINAL backwash/refill (a big non-low fill, ~220 L) is the
            # regen's last phase. Once it has happened, stop absorbing LOW-flow
            # events more than a short rinse-tail past it — those are post-regen
            # morning activity, not the softener. A brief high-peak blip mid-brine
            # (<30 L) is NOT a terminal backwash, so it doesn't trip the cutoff; a
            # later big fill IS, keeping a genuine multi-backwash cycle.
            if (low and last_bw_end is not None
                    and (s2 - last_bw_end).total_seconds() > post_bw):
                break
            chain.append((s2, e2, r2))
            last_end = max(last_end, e2)
            if low:
                last_low_end = max(last_low_end, e2)
            elif (r2["volume_litres"] or 0.0) >= _SOFTENER_BACKWASH_MIN_VOL_L:
                last_bw_end = e2 if last_bw_end is None else max(last_bw_end, e2)
            j += 1
        # The BRINE (the low-flow draw) must itself span >= MIN_SPAN. A single
        # low-flow blip at the regen time followed by moderate-flow draws (which a
        # 45-min chain would otherwise absorb into a fake "session") is NOT a regen
        # — this is what separates a real overnight regen from incidental morning
        # activity that merely starts near the configured time.
        if (last_low_end - sdt).total_seconds() / 60.0 < _SOFTENER_MIN_SPAN_MIN:
            continue                              # brine too short to be a regen
        group_id = r["id"]
        for (_s, _e, rc) in chain:
            out[rc["id"]] = ("span" if _is_low(rc) else "backwash", group_id)
            claimed.add(rc["id"])
        # Trailing backwash: a non-flush, non-low fill just past the run (but never
        # beyond the max-span cap from the session start).
        win_end = min(last_end + tail, sdt + timedelta(minutes=_SOFTENER_MAX_SPAN_MIN))
        for (s2, e2, r2) in evs:
            if (r2["id"] in claimed or s2 <= last_end or s2 > win_end
                    or _is_low(r2) or is_flush_shaped(_softener_feat(r2))):
                continue
            out[r2["id"]] = ("backwash", group_id)
            claimed.add(r2["id"])
    return out
