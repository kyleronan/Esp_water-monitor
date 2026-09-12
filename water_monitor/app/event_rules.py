"""Event-level structural rules tier — runs BEFORE the k-NN matcher.

* ``detect_washer_cycles`` keys a washer cycle on PEAK, not volume: main fills
  and sub-2.5 L top-offs share a ~constant peak while their volumes spread 15x,
  so volume-ratio approaches (``cycle_pulse_count``, the dishwasher cycle
  propagation) split the cycle. Same-peak families reach 0.73 recall with zero
  toilet contamination.
* ``rule_classify_event`` — per-event toilet / dishwasher / shower / zone-default
  rules that beat the k-NN on their shapes (toilet 0.95-1.00 vs 0.75); the k-NN
  is the residual.

Writes only ``matched_fixture_type`` (the machine opinion): user labels are never
touched and every verdict is recomputed on each reclassify, so the tier is fully
reversible.

⚠ CALIBRATION: every constant below is fit to THIS home (15 washer labels, one
washer, one supply pressure). A structural rule asserts rather than abstains, so
nothing here generalizes on its own — another home needs its own calibration or
multi-home re-validation. tools/eval_knn_classifier.py --with-rules is the
in-sample fit gate; its output overrides these numbers.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

# ── Low-flow chatter predicate (shared by the detector off-grace AND the
#    history coalesce so both agree on what a "sustained low draw" is) ─────────
# The turbine flow sensor can't hold a reading at very low flow, so a continuous
# low draw chatters into many tiny events. The live detector holds an event open
# through sub-threshold dips (event_detector); the post-hoc coalesce merges any
# fragments that slipped through (database). Both gate on THIS one predicate so
# the boundary can never drift apart. eval-gated placeholders.
LOWFLOW_CEIL_LPM: float = 1.5        # mean flow below this == low-flow regime
LOWFLOW_PK_CEIL_LPM: float = 4.0     # ...and no spike reaches this (excludes dishwasher fills/toilets)
LOWFLOW_OFF_GRACE_S: float = 120.0   # detector hold window AND coalesce max inter-fragment gap


def is_low_flow_chatter(mean_flow: Optional[float], peak: Optional[float],
                        calib: Optional[Dict[str, Any]] = None) -> bool:
    """True when an event's flow profile is a sustained LOW draw the turbine
    fragments: mean below ``LOWFLOW_CEIL_LPM`` and no spike reaching
    ``LOWFLOW_PK_CEIL_LPM``. Single-sourced so the live off-grace and the
    post-hoc coalesce never disagree on the boundary."""
    if mean_flow is None or peak is None:
        return False
    return (mean_flow < _cv(calib, "LOWFLOW_CEIL_LPM")
            and peak < _cv(calib, "LOWFLOW_PK_CEIL_LPM"))


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


# ── Home timezone ─────────────────────────────────────────────────────────────
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


def home_timezone_or_utc():
    """The home timezone, or UTC when detection has not run yet.

    THE definition — ``database._home_tz`` and the feature extractor's
    time-of-day features both resolve here, so a day boundary can never be
    computed against two different zones.

    Goes through ``get_home_timezone()`` rather than reading ``_HOME_TZ``: the
    function is the seam tests patch to pin a zone.
    """
    return get_home_timezone() or timezone.utc


# ── Washer cycle detector constants (audit Pass 4; in-sample, eval-gated) ──────
_WASHER_ANCHOR_MIN_VOL_L: float = 9.0
_WASHER_ANCHOR_DUR_S: Tuple[float, float] = (80.0, 400.0)
_WASHER_ANCHOR_PK_LPM: Tuple[float, float] = (7.5, 15.0)
_WASHER_FAMILY_PK_RATIO: Tuple[float, float] = (0.8, 1.3)
_WASHER_FAMILY_WINDOW_MIN: float = 45.0
_WASHER_FAMILY_MIN_GAP_MIN: float = 2.0
_WASHER_FAMILY_MIN_SIBLINGS: int = 2    # anchor + >=2 siblings = a real >=3-fill cycle;
#                                         one sibling (2 draws) is a coincidental pair,
#                                         not laundry. STRUCTURAL — never in RULE_DEFAULTS.
_WASHER_SIBLING_MIN_VOL_L: float = 0.5
_WASHER_SIBLING_MAX_DUR_S: float = 400.0
# Live-path pre-gate: an event can only participate in a family if its peak lies
# inside [min anchor pk x low ratio, max anchor pk x high ratio].
WASHER_FAMILY_PK_ENVELOPE: Tuple[float, float] = (
    _WASHER_ANCHOR_PK_LPM[0] * _WASHER_FAMILY_PK_RATIO[0],   # 6.0
    _WASHER_ANCHOR_PK_LPM[1] * _WASHER_FAMILY_PK_RATIO[1],   # 19.5
)

# Cycle/session fixtures (CYCLE_ONLY_FIXTURE_TYPES, below) are NEVER typed from a
# lone k-NN signature: washing_machine comes only from detect_washer_cycles,
# dishwasher from its cycle-pulse rule, water_softener from detect_softener_sessions.
# A lone draw resembling one is a tap / quick fill / slow trickle. Shared by the
# reclassify and live k-NN write paths so the guard cannot drift; a real member
# suppressed here is re-stamped by its detector (washer retro-scan live,
# softener/washer sweep on the next full reclassify).
# ── detector era ────────────────────────────────────────────────────────────
# The date the newest DETECTOR change shipped. Verdicts on user-labelled events
# are frozen (`reclassify_all_events_from_signatures` skips labelled rows), so an
# archive-wide precision figure averages every code era ever run: dishwasher_cycle
# reads 0.742 archive-wide vs 1.000 (14/14) since the T5 shape gate. BUMP THIS
# whenever a detector or rule constant changes; `measure_anchor_precision`
# reporting "not enough current-era data" afterwards is the honest state, not a
# regression.
DETECTOR_ERA_START: str = "2026-08-22"   # toilet average-flow floor

CYCLE_ONLY_FIXTURE_TYPES: frozenset = frozenset(
    {"washing_machine", "dishwasher", "water_softener"})

# ── Other rule constants ───────────────────────────────────────────────────────
_FLUSH_VOL_L: Tuple[float, float] = (2.2, 8.5)
_FLUSH_DUR_S: Tuple[float, float] = (20.0, 150.0)
# PEAK floor for a toilet claim. Across 116 reviewed toilet claims every one of
# the 90 genuine flushes peaks ≥ 7.11 L/min (a flush valve dumps at full line
# rate), while the slow steady draws (taps/appliance fills) sit below it.
# Reviewed-set precision 0.81 → 0.87 at 89/90 recall. Calibratable: a per-regime
# fit with explicit labels may lower it, and do-no-harm arbitrates.
_FLUSH_MIN_PK_LPM: float = 7.5
# AVERAGE-flow floor for a toilet CLAIM: catches the draw that is slow THROUGHOUT
# yet inside the flush volume/duration box, which the peak floor misses. Labelled
# archive: the rule's false positives sit at median true-avg 5.2 L/min vs 8.8 for
# real flushes; the floor lifts precision 0.763 -> 0.842 at recall 0.992 (128/129,
# n=169; post-T5 n=30: 0.700 -> 0.778 at recall 1.0), rejecting 6 'other', 6 tap,
# 2 dishwasher, 2 washer, 1 toilet. 5.0 rather than 5.5: 5.5 buys ~3 points of
# precision for 3 more vetoed flushes, and recall on the house's most frequent
# fixture is the expensive side. Calibratable, never auto-fit.
_FLUSH_MIN_AVG_FLOW_LPM: float = 5.0

# Burst veto on the toilet rule: a flush judged alone is a volume and a rate;
# judged in company it is often an appliance filling in stages. The threshold is
# set by BASE RATE, not widest separation — >3 heavy neighbours fires on 40% of
# events and denies the rule 29 of 177 real flushes, and a veto that routine is
# not a veto. Whole stream (3,979 events, 177 labelled toilets, 128 washers):
#
#     heavy >    all events   labelled toilets   labelled washers
#        3          20.2%           6.8%              46.9%
#        4          11.5%           1.7%              19.5%
#        6           2.5%           0.0%               0.8%
#
# 6 costs no labelled flush and still catches its case; on the 4 claims it vetoes
# the rule was right 0 times, the model 3. Do NOT add an n_ev_30m clause — counting
# every draw fires on 36% of events and measures "busy household", not "appliance
# cycle". n_heavy_2h counts only fill-sized neighbours (3-25 L, >=8 L/min).
_TOILET_VETO_HEAVY_2H: int = 6
       # any draws within +/-30 min
_FLUSH_MIN_DELTA_PSI: float = 1.5

_DW_VOL_L: Tuple[float, float] = (0.2, 3.5)
# LOCKED by the eval sweep (tools/eval_knn_classifier.py --with-rules): 4.2 let
# the rule claim gentle tap fills (tap recall fell below the k-NN baseline);
# 3.6 — the audit's strict gentle-train cut — restores tap while keeping
# dishwasher at 0.933 and overall LOO at 0.685 (baseline 0.624).
_DW_MAX_PK_LPM: float = 3.6
_DW_MIN_CYCLE_PULSES: int = 3   # >=3 similar-volume neighbours in ±45 min == a real
#                                 cycle. Raised from 2 (one coincidental neighbour was
#                                 enough); aligns with the fixtures.py temporal rules.
#                                 MITIGATION: cycle_pulse_count is still volume-only — a
#                                 shape-aware count is the root fix (see plan follow-up).
#                                 STRUCTURAL gate — never add to RULE_DEFAULTS.

# Dishwasher CYCLE detector (companion to detect_washer_cycles). The per-event
# rule above needs cycle_pulse_count >= 3, but gentle dishwasher fills FAIL the
# fill-shaped gate inside that counter and sit at cpc<3 — so a real cycle (e.g. a
# dishwasher run concurrent with a washer) goes unlabelled. This detector instead chains
# a run of >=_DW_CYCLE_MIN_MEMBERS small, gentle fills (vol in _DW_VOL_L, peak <=
# _DW_MAX_PK_LPM, not flush-shaped), each within _DW_CYCLE_CHAIN_GAP_MIN of the previous
# and the whole run within _DW_CYCLE_MAX_SPAN_MIN. STRUCTURAL — never in RULE_DEFAULTS.
_DW_CYCLE_CHAIN_GAP_MIN: float = 30.0   # consecutive fills <= this apart chain together
_DW_CYCLE_MAX_SPAN_MIN: float = 180.0   # cap a session — a cycle isn't all afternoon
_DW_CYCLE_MIN_MEMBERS: int = 3          # >=3 chained gentle fills == a cycle
# Per-candidate shape gate (T5). The tier's failure mode is burst-chaining —
# short spiky faucet draws strung into a fake cycle (9/19 precision pre-outage,
# 1/10 post-reseed) — and a genuine fill is steady. Validated out-of-sample at
# recall 0.889 / precision 0.727. CONFIGURED CONSTANTS, never auto-fit (LOO:
# thresholds weakly identified at n=50). STRUCTURAL — never in RULE_DEFAULTS.
_DW_CYCLE_MAX_FLOW_VARIABILITY: float = 1.6
_DW_CYCLE_MIN_STEADY_FRACTION: float = 0.4

_SHOWER_BIG_VOL_L: float = 30.0
_SHOWER_BIG_DUR_S: float = 300.0
_SHOWER_BIG_MIN_PK: float = 6.0
_SHOWER_SMALL_VOL_L: Tuple[float, float] = (15.0, 30.0)
_SHOWER_SMALL_DUR_S: float = 240.0

_ZONE_MIN_DUR_S: float = 240.0
_ZONE_MIN_PK_LPM: float = 5.0

# ── Toilet physics veto ─────────────────────────────────────────────────────────
# A flush is a SINGLE continuous cistern refill with a manufactured volume floor
# and an era-bounded ceiling; a 'toilet' proposal outside those bounds is wrong by
# construction, so the veto turns it into an abstention (the event falls to the
# "Other" catch-all, never to another fixture guess). STRUCTURAL — never in
# RULE_DEFAULTS: manufacturing/regulatory facts, not per-home behaviour.
# Floor: the smallest flush ever manufactured is 0.8 gpf ≈ 3.0 L (dual-flush
# half-flush bottoms out there too); 2.8 L is that minus a rating-vs-metered
# margin. Ceilings (US EPA history, _TOILET_ERA_CAPS_L) apply from
# home_profile.build_year when epa_flush_cap_enabled is on. A home older than its
# toilets only over-allows, so the build year is a safe upper-bound proxy;
# renovated homes can turn the cap off.
TOILET_MIN_FLUSH_L: float = 2.8
TOILET_VETO_MIN_PK_LPM: float = 3.0     # matches the cluster toilet rule's flow floor
TOILET_VETO_MAX_SEGMENTS: int = 2       # one refill; allow 2 for sampling jitter
_TOILET_CAP_MARGIN: float = 1.15        # bowl refill + mfg rating tolerance
_TOILET_ERA_CAPS_L: Tuple[Tuple[int, float], ...] = (
    (1994, 6.1),    # 1.6 gpf — Energy Policy Act of 1992 (effective 1994)
    (1982, 13.2),   # 3.5 gpf era
)
_TOILET_CAP_FALLBACK_L: float = 26.5    # 7 gpf — pre-1982 / year unknown / cap off


def toilet_flush_cap_litres(build_year: Optional[int] = None,
                            cap_enabled: bool = True) -> float:
    """Upper bound (litres, margin included) a single flush can meter in this home.

    ``cap_enabled`` off, or an unknown/implausible ``build_year``, falls back to
    the pre-1982 ceiling — the veto then only rejects events no toilet in
    history could produce.
    """
    if cap_enabled and build_year:
        for year, cap in _TOILET_ERA_CAPS_L:
            if build_year >= year:
                return cap * _TOILET_CAP_MARGIN
    return _TOILET_CAP_FALLBACK_L * _TOILET_CAP_MARGIN


def toilet_veto_reason(features: Dict[str, Any],
                       cap_litres: float) -> Optional[str]:
    """Which physics test rejects this event as a single flush, or None.

    A reason string, not a bool, so the log names the condition that fired — a
    2.5 L event rejected by the 2.8 L floor otherwise logs as "vol=2.5 L,
    cap=30.5 L", indistinguishable from a pass. A missing feature never vetoes
    (no evidence, no veto). Deliberately NOT symmetric with is_flush_shaped:
    that says "looks like a flush", this says "cannot be one" — only the latter
    may override another tier's positive evidence (e.g. a k-NN vote).
    """
    vol = _f(features, "volume_litres")
    if vol is not None:
        if vol < TOILET_MIN_FLUSH_L:
            return f"below the {TOILET_MIN_FLUSH_L} L manufactured flush floor"
        if vol > cap_litres:
            return f"above this home's {cap_litres:.1f} L era cap"
    pk = _f(features, "peak_flow_lpm")
    if pk is not None and pk < TOILET_VETO_MIN_PK_LPM:
        return (f"peak flow {pk:.2f} < {TOILET_VETO_MIN_PK_LPM} L/min "
                "(too weak for a cistern refill)")
    seg = _f(features, "active_flow_segment_count")
    if seg is not None and seg > TOILET_VETO_MAX_SEGMENTS:
        return (f"{int(seg)} flow segments > {TOILET_VETO_MAX_SEGMENTS} "
                "(a refill is one continuous segment)")
    return None


# ── Per-home calibration plumbing ───────────────────────────────────────────────
# event_rules ships the defaults above; a frozen per-home fit (rule_calibration.py)
# may override any subset via an optional ``calib`` dict threaded through the
# predicates below. ``RULE_DEFAULTS`` is the single source of truth for BOTH the
# fallback here AND the sanity-gate span comparison in rule_calibration (which
# imports it) — so the two can never drift apart. Keys mirror the constant names
# without the leading underscore.
RULE_DEFAULTS: Dict[str, Any] = {
    "FLUSH_VOL_L":             _FLUSH_VOL_L,
    "FLUSH_DUR_S":             _FLUSH_DUR_S,
    "FLUSH_MIN_PK_LPM":        _FLUSH_MIN_PK_LPM,
    "FLUSH_MIN_AVG_FLOW_LPM":  _FLUSH_MIN_AVG_FLOW_LPM,
    "DW_VOL_L":                _DW_VOL_L,
    "DW_MAX_PK_LPM":           _DW_MAX_PK_LPM,
    "SHOWER_BIG_VOL_L":        _SHOWER_BIG_VOL_L,
    "SHOWER_BIG_DUR_S":        _SHOWER_BIG_DUR_S,
    "SHOWER_BIG_MIN_PK":       _SHOWER_BIG_MIN_PK,
    "SHOWER_SMALL_VOL_L":      _SHOWER_SMALL_VOL_L,
    "SHOWER_SMALL_DUR_S":      _SHOWER_SMALL_DUR_S,
    "ZONE_MIN_DUR_S":          _ZONE_MIN_DUR_S,
    "ZONE_MIN_PK_LPM":         _ZONE_MIN_PK_LPM,
    "WASHER_ANCHOR_MIN_VOL_L": _WASHER_ANCHOR_MIN_VOL_L,
    "WASHER_ANCHOR_DUR_S":     _WASHER_ANCHOR_DUR_S,
    "WASHER_ANCHOR_PK_LPM":    _WASHER_ANCHOR_PK_LPM,
    "WASHER_FAMILY_PK_RATIO":  _WASHER_FAMILY_PK_RATIO,
    "LOWFLOW_CEIL_LPM":        LOWFLOW_CEIL_LPM,
    "LOWFLOW_PK_CEIL_LPM":     LOWFLOW_PK_CEIL_LPM,
}


# The volume-ZEROING artifact verdicts as one "not an artifact" SQL fragment.
# Any query that chains or reprocesses events must exclude these (their stored
# volume may be zeroed — chaining one into a cycle or re-importing it would
# resurrect false water). When a NEW zeroing flag column is added, extend THIS
# fragment rather than hand-copying the flag list somewhere else.
NOT_ARTIFACT_SQL: str = (
    "COALESCE(is_pressure_restoration_phantom, 0) = 0 "
    "AND COALESCE(is_cross_talk, 0) = 0 "
    "AND COALESCE(is_low_flow_dribble, 0) = 0")


def _cv(calib: Optional[Dict[str, Any]], key: str) -> Any:
    """Resolve a rule constant: the per-home calibrated value if the frozen
    ``calib`` carries it, else the shipped default. Tuples round-trip through JSON
    as lists, so normalise a list back to a tuple when the default is a tuple."""
    default = RULE_DEFAULTS[key]
    if calib is not None:
        v = calib.get(key)
        if v is not None:
            if isinstance(default, tuple) and isinstance(v, list):
                return tuple(v)
            return v
    return default


def _f(features: Dict[str, Any], key: str) -> Optional[float]:
    """Feature as float, or None (missing / non-numeric)."""
    v = features.get(key)
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def is_flush_shaped(features: Dict[str, Any],
                    calib: Optional[Dict[str, Any]] = None,
                    pump_mode: bool = False) -> bool:
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
    flush_vol = _cv(calib, "FLUSH_VOL_L")
    flush_dur = _cv(calib, "FLUSH_DUR_S")
    if not (flush_vol[0] <= vol <= flush_vol[1]):
        return False
    if not (flush_dur[0] <= dur <= flush_dur[1]):
        return False
    if pk < _cv(calib, "FLUSH_MIN_PK_LPM"):
        return False
    transient = features.get("has_pressure_transient")
    delta = _f(features, "pressure_delta_psi")
    if pump_mode:
        # Under a booster pump the flush's pressure signature rides the recharge
        # sawtooth — the delta depends on where in the cycle the flush lands — so
        # pressure corroboration is waived and the flow-only gates above decide.
        return True
    return bool(transient) or (delta is not None and delta >= _FLUSH_MIN_DELTA_PSI)


def in_appliance_burst(burst: Optional[Dict[str, Any]]) -> bool:
    """Is this draw sitting inside a run of activity a flush would not be in?

    An appliance fills in stages, so its members arrive surrounded by siblings;
    a flush arrives alone. The threshold counts NEIGHBOURS, never the event
    itself, so a lone flush, however large, can never trip it. ``burst`` is the
    label-free burst-context dict; ``None`` (a fitting path, or a stream too
    short to compute it) answers False — silence is not evidence of a burst.
    """
    if not burst:
        return False

    def _n(key):
        v = burst.get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    heavy = _n("n_heavy_2h")
    return heavy is not None and heavy > _TOILET_VETO_HEAVY_2H


def toilet_burst_veto_reason(features: Dict[str, Any],
                             calib: Optional[Dict[str, Any]] = None,
                             pump_mode: bool = False,
                             burst: Optional[Dict[str, Any]] = None
                             ) -> Optional[str]:
    """Why the burst veto suppressed a toilet claim, or None if it did not.

    ``rule_classify_event`` returns None without a reason, so a caller that
    wants to REPORT a veto asks here (as with ``toilet_veto_reason``): a guard
    nobody can see firing is indistinguishable from one that is not there.
    Answers only for draws that WOULD otherwise have been claimed — a veto
    reported on a non-flush-shaped event would inflate the count with cases the
    veto had no part in.
    """
    if not in_appliance_burst(burst):
        return None
    if not (is_flush_shaped(features, calib, pump_mode=pump_mode)
            and has_flush_flow_signature(features, calib)):
        return None
    n = (burst or {}).get("n_heavy_2h")
    return (f"{n} fill-sized draws within the hour "
            f"(> {_TOILET_VETO_HEAVY_2H}; a flush arrives alone)")


def has_flush_flow_signature(features: Dict[str, Any],
                             calib: Optional[Dict[str, Any]] = None) -> bool:
    """Does this draw move water at the RATE a flush does, throughout?

    Deliberately NOT folded into ``is_flush_shaped``: the toilet rule uses that
    to CLAIM, but the washer, dishwasher-cycle and softener detectors use it to
    EXCLUDE, so a floor there would narrow "flush" and quietly LOOSEN all three —
    an unmeasured change. The floor lives here, where a claim is made.
    ``true_avg_flow_lpm`` is preferred (no pressure-window padding, unlike
    ``avg_flow_lpm``); legacy rows fall back to it, and rows with neither pass
    unchallenged rather than being delabelled by a feature they never had.
    """
    flow = _f(features, "true_avg_flow_lpm")
    # A draw that moved water cannot average zero. A non-positive value here
    # means the active-flow computation did not run for this event, not that
    # the flow was zero — the same coerced-0.0 trap the pressure feature
    # documents (`_PRESSURE_VALID_MIN_PSI`). Reading it as a measurement makes
    # this floor reject EVERY event: a genuine toilet in the recording corpus
    # replays with true_avg 0.0 and avg_flow 8.0.
    if flow is None or flow <= 0.0:
        flow = _f(features, "avg_flow_lpm")
    if flow is None or flow <= 0.0:
        return True
    return flow >= _cv(calib, "FLUSH_MIN_AVG_FLOW_LPM")


def _is_washer_anchor(vol, dur, pk, calib: Optional[Dict[str, Any]] = None) -> bool:
    anchor_dur = _cv(calib, "WASHER_ANCHOR_DUR_S")
    anchor_pk = _cv(calib, "WASHER_ANCHOR_PK_LPM")
    return (vol is not None and dur is not None and pk is not None
            and vol >= _cv(calib, "WASHER_ANCHOR_MIN_VOL_L")
            and anchor_dur[0] <= dur <= anchor_dur[1]
            and anchor_pk[0] <= pk <= anchor_pk[1])


def detect_washer_cycles(
    conn: sqlite3.Connection,
    circuit: str,
    since_ts: Optional[str] = None,
    limit: int = 4000,
    calib: Optional[Dict[str, Any]] = None,
) -> Dict[str, Tuple[str, str]]:
    """Find washer-cycle members on ``circuit``: anchors (main fills) with at least
    ``_WASHER_FAMILY_MIN_SIBLINGS`` same-peak siblings 2-45 min away, plus the
    family's non-flush-shaped members (top-offs + secondary fills). Returns
    ``{event_id: (role, group_id)}``, role ``'anchor'``/``'member'``, group_id the
    anchor's event id (the History cycle-rollup key).

    ``since_ts`` bounds the live trailing pass; callers back it off by a family
    width so an anchor just before the bound still claims members inside it.
    Reads ONLY feature/timestamp columns — never a label — so the eval harness's
    leave-one-out stays honest.
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
    fam_ratio = _cv(calib, "WASHER_FAMILY_PK_RATIO")
    for ts_a, a in evs:
        if not _is_washer_anchor(a["volume_litres"], a["duration_seconds"],
                                 a["peak_flow_lpm"], calib):
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
            if not (fam_ratio[0] * pk_a <= pk_o <= fam_ratio[1] * pk_a):
                continue
            family.append(o)
        if len(family) < _WASHER_FAMILY_MIN_SIBLINGS:
            continue                       # <2 siblings: a lone big fill (sink/tub) or a
            #                                coincidental pair — not a multi-fill cycle
        out[a["id"]] = ("anchor", a["id"])
        for o in family:
            feats = {"volume_litres": o["volume_litres"],
                     "duration_seconds": o["duration_seconds"],
                     "peak_flow_lpm": o["peak_flow_lpm"],
                     "has_pressure_transient": o["has_pressure_transient"],
                     "pressure_delta_psi": o["pressure_delta_psi"]}
            if is_flush_shaped(feats, calib):
                continue                   # flush during laundry — leave it alone
            out.setdefault(o["id"], ("member", a["id"]))
    return out


def detect_dishwasher_cycles(
    conn: sqlite3.Connection,
    circuit: str,
    since_ts: Optional[str] = None,
    limit: int = 4000,
    calib: Optional[Dict[str, Any]] = None,
    exclude_ids: Optional[set] = None,
) -> Dict[str, Tuple[str, str]]:
    """Find dishwasher-cycle members on ``circuit``: a chain of gentle fills as
    defined by the ``_DW_CYCLE_*`` constants. Returns ``{event_id: (role, group_id)}``,
    role ``'anchor'``/``'member'``, group_id the session's first event id (the
    History cycle-rollup key, like the washer detector).

    Exists because gentle fills fail the fill-shaped gate inside ``cycle_pulse_count``
    (cpc<3), so the per-event rule misses a real cycle — e.g. one concurrent with a
    washer. Reads ONLY feature/timestamp columns — never a label — so the eval
    harness's leave-one-out stays honest. Skips artifact-flagged events (phantom /
    cross-talk / dribble / excluded) and ``exclude_ids`` (washer/softener members
    the caller already claimed)."""
    where = ("WHERE circuit = ? AND COALESCE(excluded_from_training, 0) = 0 "
             "AND " + NOT_ARTIFACT_SQL)
    params: list = [circuit]
    if since_ts is not None:
        where += " AND start_ts >= ?"
        params.append(since_ts)
    params.append(limit)
    rows = conn.execute(
        "SELECT id, start_ts, duration_seconds, volume_litres, peak_flow_lpm, "
        "       has_pressure_transient, pressure_delta_psi, "
        "       flow_variability, steady_state_fraction "
        f"FROM events {where} ORDER BY start_ts LIMIT ?",
        params,
    ).fetchall()

    dw_vol = _cv(calib, "DW_VOL_L")
    dw_pk = _cv(calib, "DW_MAX_PK_LPM")
    exclude_ids = exclude_ids or set()
    cand = []
    for r in rows:
        if r["id"] in exclude_ids:
            continue                       # already a washer/softener member
        v, pk = r["volume_litres"], r["peak_flow_lpm"]
        if v is None or pk is None or not (dw_vol[0] <= v <= dw_vol[1]) or pk > dw_pk:
            continue
        # Per-candidate shape gate (see _DW_CYCLE_MAX_FLOW_VARIABILITY): burst-
        # chaining rides on spiky, unsteady candidates; a genuine fill is steady.
        # NULL features (legacy rows) pass unchallenged — the gate must not
        # silently delabel history the features can't vet.
        fv, ssf = r["flow_variability"], r["steady_state_fraction"]
        if fv is not None and fv > _DW_CYCLE_MAX_FLOW_VARIABILITY:
            continue
        if ssf is not None and ssf < _DW_CYCLE_MIN_STEADY_FRACTION:
            continue
        feats = {"volume_litres": v, "duration_seconds": r["duration_seconds"],
                 "peak_flow_lpm": pk, "has_pressure_transient": r["has_pressure_transient"],
                 "pressure_delta_psi": r["pressure_delta_psi"]}
        if is_flush_shaped(feats, calib):
            continue                       # a quick flush, not a gentle appliance fill
        try:
            ts = datetime.fromisoformat(r["start_ts"])
        except (TypeError, ValueError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        cand.append((ts, r["id"]))

    out: Dict[str, Tuple[str, str]] = {}
    chain_gap = _DW_CYCLE_CHAIN_GAP_MIN * 60.0
    max_span = _DW_CYCLE_MAX_SPAN_MIN * 60.0
    i, n = 0, len(cand)
    while i < n:
        j = i + 1
        while (j < n
               and (cand[j][0] - cand[j - 1][0]).total_seconds() <= chain_gap
               and (cand[j][0] - cand[i][0]).total_seconds() <= max_span):
            j += 1
        members = cand[i:j]
        if len(members) >= _DW_CYCLE_MIN_MEMBERS:
            gid = members[0][1]
            for k, (_, eid) in enumerate(members):
                out[eid] = ("anchor" if k == 0 else "member", gid)
        i = j
    return out


def rule_classify_event(
    features: Dict[str, Any], circuit_type: str = "fixture",
    calib: Optional[Dict[str, Any]] = None,
    pump_mode: bool = False,
    burst: Optional[Dict[str, Any]] = None,
) -> Optional[Tuple[str, str]]:
    """Ordered structural rules — first hit wins; None = no rule claims the event
    (falls through to the k-NN residual). Washer is deliberately NOT here: it
    requires cycle context and comes only from ``detect_washer_cycles``.

    ``pump_mode`` (confirmed vfd pump homes only): waives the toilet
    rule's pressure-corroboration requirement — under a recharge sawtooth a
    flush's pressure delta depends on where in the pump cycle it lands, so
    only the flow-shape gates decide. Fitting/calibration paths keep the
    default False (era-agnostic fits stay conservative)."""
    vol = _f(features, "volume_litres")
    dur = _f(features, "duration_seconds")
    pk = _f(features, "peak_flow_lpm")

    if circuit_type == "zone":
        # Zone circuits host no toilets/dishwashers/showers; the only rule is the
        # irrigation default (fixes the audit's 0/6: the k-NN can never fire
        # under its 10-label floor on a young zone circuit).
        if (dur is not None and dur >= _cv(calib, "ZONE_MIN_DUR_S")
                and pk is not None and pk >= _cv(calib, "ZONE_MIN_PK_LPM")):
            return "irrigation_zone", "zone_default"
        return None

    # Two vetoes for two mistakes: the flow signature rejects the wrong SIZE or
    # SHAPE, the burst check the right size in the wrong COMPANY (a washer's
    # middle fill is a flush in isolation). A vetoed claim falls through to the
    # model tier, which has the burst features and was right 53% of the time on
    # these against the rule's 24%.
    if (is_flush_shaped(features, calib, pump_mode=pump_mode)
            and has_flush_flow_signature(features, calib)
            and not in_appliance_burst(burst)):
        return "toilet", "rule_toilet"

    dw_vol = _cv(calib, "DW_VOL_L")
    cyc = _f(features, "cycle_pulse_count")
    if (vol is not None and dw_vol[0] <= vol <= dw_vol[1]
            and pk is not None and pk <= _cv(calib, "DW_MAX_PK_LPM")
            and cyc is not None and cyc >= _DW_MIN_CYCLE_PULSES):
        return "dishwasher", "rule_dishwasher"

    if vol is not None and dur is not None and pk is not None:
        shower_small_vol = _cv(calib, "SHOWER_SMALL_VOL_L")
        if (vol >= _cv(calib, "SHOWER_BIG_VOL_L")
                and dur >= _cv(calib, "SHOWER_BIG_DUR_S")
                and pk >= _cv(calib, "SHOWER_BIG_MIN_PK")):
            return "shower_tub", "rule_shower"
        if (shower_small_vol[0] <= vol < shower_small_vol[1]
                and dur >= _cv(calib, "SHOWER_SMALL_DUR_S")):
            return "shower_tub", "rule_shower"

    return None


# ── Water-softener session detector (in-sample, eval-gated) ───────────────────
# A regen is a long LOW-flow brine draw at a fixed overnight clock time, then one
# or more steady backwash/rinse fills. Demand-initiated (every ~2 weeks) but ALWAYS
# the same start time, so the discriminators are the start band + the >=90-min
# span — NOT a pulse count, which coalescing (brine -> a few long events) makes
# fragile. The backwash looks like a shower (~219 L, peak ~20 L/min); only session
# context says otherwise: a non-flush fill inside a confirmed session IS the backwash.
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
# NOTE: a real regen is multi-draw (brine + >=1 backwash/rinse), but the gate is a
# REQUIRED backwash (see detect_softener_sessions), not an event count — a lone low-flow
# span and a multi-fragment low-flow chain are BOTH rejected when no >=30 L fill exists.


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
    calib: Optional[Dict[str, Any]] = None,
) -> Dict[str, Tuple[str, str]]:
    """Find water-softener regeneration sessions on ``circuit``.

    Returns ``{event_id: (role, group_id)}``: role ``'span'`` (a low-flow
    brine/rinse event) or ``'backwash'`` (a non-flush steady fill in the session
    window), group_id the session's first chain-event id (the History rollup key).

    A session is a run of consecutive NON-flush events (chain gap / max span per
    the ``_SOFTENER_*`` constants) that STARTS with a low-flow event inside
    ``band_center_min`` ± ``_SOFTENER_START_BAND_MIN`` (local clock), whose brine
    spans >= ``_SOFTENER_MIN_SPAN_MIN``, and that contains a REAL backwash (a
    >= ``_SOFTENER_BACKWASH_MIN_VOL_L`` non-low fill, in-chain or trailing up to
    ``_SOFTENER_BACKWASH_TAIL_MIN`` past the run). A flush-shaped event ends the
    run — a 3 am flush during a regen is irreducibly ambiguous and stays with the
    per-event tiers.

    ``band_center_min`` is minutes-since-LOCAL-midnight (parse_hhmm_to_minutes);
    ``tz`` converts each stored-UTC start_ts to local so the band is DST-correct,
    and tz=None compares in UTC (tests/eval). ``since_ts`` bounds the live
    trailing pass. Reads feature/timestamp columns plus the phantom/cross-talk
    ARTIFACT verdicts, never a fixture LABEL, so the eval's label-free LOO stays
    honest. Phantoms and cross-talk are EXCLUDED because they moved no real water
    (volume_litres_effective == 0): a 66-min phantom must not anchor a session or
    bridge an 80-min gap between unrelated drips — a phantom-bridged chain once
    walked 3 h and absorbed a real 97 L shower as a fake "backwash", clearing even
    the backwash gate.
    """
    where = ("WHERE circuit = ? "
             "AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
             "AND COALESCE(is_cross_talk, 0) = 0")
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
            if is_flush_shaped(_softener_feat(r2), calib):
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
        # Trailing-backwash window (also feeds the multi-draw gate below): a non-flush,
        # non-low fill just past the run, never beyond the max-span cap from the start.
        win_end = min(last_end + tail, sdt + timedelta(minutes=_SOFTENER_MAX_SPAN_MIN))

        def _is_trailing_bw(s2, r2) -> bool:
            return not (r2["id"] in claimed or s2 <= last_end or s2 > win_end
                        or _is_low(r2)
                        or (r2["volume_litres"] or 0.0) < _SOFTENER_BACKWASH_MIN_VOL_L
                        or is_flush_shaped(_softener_feat(r2), calib))

        # A real regen ALWAYS ends with a backwash/refill (~220 L), in-chain
        # (last_bw_end) or trailing (_is_trailing_bw); a low-flow chain with no such
        # fill — e.g. overnight drips bridged by a zero-volume phantom — is incidental
        # however many fragments it has. Safe to require: the backwash is high-flow
        # (~15 L/min), so low-flow coalescing can never fold it into the brine span.
        has_backwash = last_bw_end is not None or any(
            _is_trailing_bw(s2, r2) for (s2, _e2, r2) in evs)
        if not has_backwash:
            continue
        group_id = r["id"]
        for (_s, _e, rc) in chain:
            out[rc["id"]] = ("span" if _is_low(rc) else "backwash", group_id)
            claimed.add(rc["id"])
        # Trailing backwash: claim the non-flush, non-low fills just past the run.
        for (s2, _e2, r2) in evs:
            if _is_trailing_bw(s2, r2):
                out[r2["id"]] = ("backwash", group_id)
                claimed.add(r2["id"])
    return out
