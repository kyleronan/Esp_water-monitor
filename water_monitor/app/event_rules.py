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

# Cycle/session-detected fixtures are NEVER typed from a lone k-NN signature:
# washing_machine comes only from detect_washer_cycles (anchor + >=2 same-peak fills),
# dishwasher only from its cycle-pulse rule, and water_softener only from detect_softener_
# sessions (a scheduled multi-draw regen). The k-NN residual must NOT stamp these onto a
# single event — a lone draw resembling one is a tap / quick fill / slow trickle. Shared by
# the reclassify and live k-NN write paths so the guard can never drift apart. A real
# cycle/session member suppressed here is re-stamped by its session detector (the washer
# retro-scan live, or the softener/washer sweep on the next full reclassify).
# ── detector era ────────────────────────────────────────────────────────────
# The date the newest DETECTOR change shipped. It exists because a stored
# machine verdict is frozen the moment the operator labels that event —
# `reclassify_all_events_from_signatures` skips user-labelled rows by design —
# so verdicts on labelled events are never re-derived and an archive-wide
# precision figure silently averages every code era the add-on has ever run.
#
# Measured 2026-08-22, the difference is not academic: dishwasher_cycle reads
# 0.742 archive-wide and 1.000 (14/14) since the dev42/T5 shape gate shipped.
# The first number describes code that no longer exists.
#
# BUMP THIS whenever a detector or rule constant changes, and expect
# `measure_anchor_precision` to report "not enough current-era data" for a
# while afterwards — that is the honest state, not a regression.
DETECTOR_ERA_START: str = "2026-08-22"   # dev47: toilet average-flow floor

CYCLE_ONLY_FIXTURE_TYPES: frozenset = frozenset(
    {"washing_machine", "dishwasher", "water_softener"})

# ── Other rule constants ───────────────────────────────────────────────────────
_FLUSH_VOL_L: Tuple[float, float] = (2.2, 8.5)
_FLUSH_DUR_S: Tuple[float, float] = (20.0, 150.0)
# dev45 — raised 5.0 → 7.5, validated against all 116 reviewed toilet claims
# (2026-08-16 export): every one of the 90 genuine flushes peaks ≥ 7.11 L/min
# (a flush valve dumps at full line rate), while the mislabeled slow steady
# draws (taps/appliance fills the rule was claiming at 5–7.5) sit below it.
# Reviewed-set precision 0.81 → 0.87 at 89/90 recall. Calibratable: a per-
# regime fit with explicit labels may lower it, and do-no-harm arbitrates.
_FLUSH_MIN_PK_LPM: float = 7.5
# dev47 (47e) — AVERAGE-flow floor for a toilet CLAIM.
#
# The peak floor above (dev45) removed slow draws with one fast spike. What it
# could not remove is the draw that is slow THROUGHOUT and still lands inside
# the flush volume/duration box: measured on the labelled archive, the toilet
# rule's false positives sit at median true-avg 5.2 L/min against 8.8 for real
# flushes — a flush empties a cistern at a rate a tap or an appliance fill does
# not sustain.
#
# Measured effect of this floor on rule_toilet's claims (labelled archive,
# n=169): precision 0.763 -> 0.842 while keeping 128 of 129 genuine flushes
# (recall 0.992). On post-T5 events alone (n=30): 0.700 -> 0.778 with recall
# 1.000. The 17 events it rejects are 6 'other', 6 tap, 2 dishwasher, 2 washer
# and 1 toilet.
#
# 5.0 rather than the 5.5 the sweep also supports: recall on the highest-
# frequency fixture in the house is the expensive side of this trade (dev45,
# 46d), and 5.5 buys ~3 more points of precision for 3 more vetoed flushes.
# Revisit with more labels; calibratable, never auto-fit.
_FLUSH_MIN_AVG_FLOW_LPM: float = 5.0

# dev48 — burst veto on the toilet rule. A flush judged alone is a volume and a
# flow rate; judged in company it is often an appliance filling in stages.
#
# The threshold is set by BASE RATE, not by the widest separation. An earlier
# draft used >3 heavy neighbours plus a second clause on n_ev_30m, chosen for
# the biggest precision gap over 159 labelled rule_toilet claims — and fired on
# 40% of every event on the reference home, denying the rule 29 of 177 real
# flushes. A veto that routine is not a veto. Measured across the whole stream
# instead (3,979 events, 177 labelled toilets, 128 labelled washers):
#
#     heavy >    all events   labelled toilets   labelled washers
#        3          20.2%           6.8%              46.9%
#        4          11.5%           1.7%              19.5%
#        6           2.5%           0.0%               0.8%
#
# 6 costs NO labelled flush at all while still catching the case it exists for,
# and on the claims it does veto the rule was right 0 times out of 4 and the
# model 3. The n_ev_30m clause is gone: it fired on 36% of events by counting
# any draw, including tiny taps, so it measured "busy household" rather than
# "appliance cycle". n_heavy_2h is the targeted signal — it counts only
# neighbours that are themselves fill-sized (3-25 L, >=8 L/min).
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

# dev.39 — dishwasher CYCLE detector (companion to detect_washer_cycles). The per-event
# rule above needs cycle_pulse_count >= 3, but gentle dishwasher fills FAIL the
# fill-shaped gate inside that counter and sit at cpc<3 — so a real cycle (e.g. a
# dishwasher run concurrent with a washer) goes unlabelled. This detector instead chains
# a run of >=_DW_CYCLE_MIN_MEMBERS small, gentle fills (vol in _DW_VOL_L, peak <=
# _DW_MAX_PK_LPM, not flush-shaped), each within _DW_CYCLE_CHAIN_GAP_MIN of the previous
# and the whole run within _DW_CYCLE_MAX_SPAN_MIN. STRUCTURAL — never in RULE_DEFAULTS.
_DW_CYCLE_CHAIN_GAP_MIN: float = 30.0   # consecutive fills <= this apart chain together
_DW_CYCLE_MAX_SPAN_MIN: float = 180.0   # cap a session — a cycle isn't all afternoon
_DW_CYCLE_MIN_MEMBERS: int = 3          # >=3 chained gentle fills == a cycle
# dev42 (T5) — per-candidate shape gate. The tier's measured failure mode was
# burst-chaining (short spiky faucet draws strung into a fake cycle; 9/19
# precision pre-outage, 1/10 post-reseed). A genuine fill is steady:
# validated out-of-sample on the pre-outage reviews at recall 0.889 /
# precision 0.727. CONFIGURED CONSTANTS, never auto-fit (LOO: thresholds
# weakly identified at n=50). STRUCTURAL — never in RULE_DEFAULTS.
_DW_CYCLE_MAX_FLOW_VARIABILITY: float = 1.6
_DW_CYCLE_MIN_STEADY_FRACTION: float = 0.4

_SHOWER_BIG_VOL_L: float = 30.0
_SHOWER_BIG_DUR_S: float = 300.0
_SHOWER_BIG_MIN_PK: float = 6.0
_SHOWER_SMALL_VOL_L: Tuple[float, float] = (15.0, 30.0)
_SHOWER_SMALL_DUR_S: float = 240.0

_ZONE_MIN_DUR_S: float = 240.0
_ZONE_MIN_PK_LPM: float = 5.0

# ── Toilet physics veto (dev17) ─────────────────────────────────────────────────
# A toilet flush is a SINGLE continuous cistern refill with a hard physical
# volume floor and an era-bounded ceiling. Any tier proposing 'toilet' for an
# event that violates these bounds is wrong by construction — the veto turns
# that proposal into an abstention (the event falls to the "Other" catch-all,
# never to a different fixture guess). STRUCTURAL — never in RULE_DEFAULTS:
# these are manufacturing/regulatory facts, not per-home behaviour to calibrate.
#
# Floor: the smallest flush ever manufactured is 0.8 gpf ≈ 3.0 L (ultra-high-
# efficiency full flush; dual-flush half-flush bottoms out at the same 0.8 gal).
# 2.8 L = that floor minus a margin for mfg-rating-vs-metered mismatch.
#
# Era ceilings (US federal/EPA history), applied from home_profile.build_year
# when the epa_flush_cap_enabled toggle is on:
#   pre-1982 homes ..... conventional cisterns up to 7 gpf (26.5 L)
#   1982–1993 .......... 3.5 gpf (13.2 L) reduced-flush era
#   1994+ .............. Energy Policy Act of 1992: 1.6 gpf (6.1 L) legal max
# Each ceiling gets a margin for bowl-refill draw + rating tolerance. A home
# older than its toilets only over-allows (never vetoes a real flush), so the
# build year is a safe upper-bound proxy; renovated homes can turn the cap off.
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

    Same logic as ``toilet_physics_veto`` (which wraps this); split out so the
    log line can name the condition that actually fired. It previously printed
    only the volume and the era cap, so a 2.5 L event rejected by the 2.8 L
    manufactured floor logged as "vol=2.5 L, cap=30.5 L" and read like a
    passing event — which is no help at all when several dozen of them scroll
    past in one reclassify.
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


def toilet_physics_veto(features: Dict[str, Any], cap_litres: float) -> bool:
    """True = this event physically cannot be a single toilet flush.

    Reads ``volume_litres``, ``peak_flow_lpm`` and ``active_flow_segment_count``
    from ``features``; a missing/None value never vetoes (no evidence, no veto).
    Deliberately NOT symmetric with is_flush_shaped: that rule says "looks like
    a flush", this one says "cannot be a flush" — only the latter may override
    another tier's positive evidence (e.g. a k-NN vote).
    """
    return toilet_veto_reason(features, cap_litres) is not None


# ── Per-home calibration plumbing (Phase 1) ─────────────────────────────────────
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
# fragment — the pattern so far (phantom, cross-talk, dribble) each grew its own
# column, and hand-copied flag lists are how the dev6 hygiene pass went stale.
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
        # dev24 (pump plan Phase 4): under a booster pump the flush's pressure
        # signature rides the recharge sawtooth — the delta depends on where in
        # the cycle the flush lands, so the pressure corroboration requirement
        # is waived and the flow-only shape gates above decide.
        return True
    return bool(transient) or (delta is not None and delta >= _FLUSH_MIN_DELTA_PSI)


def in_appliance_burst(burst: Optional[Dict[str, Any]]) -> bool:
    """Is this draw sitting inside a run of activity that a flush would not be?

    A toilet flushes and then the room goes quiet. An appliance fills in stages,
    so its members arrive surrounded by siblings. The threshold counts
    NEIGHBOURS, never the event itself, so a lone flush can never trip it
    however large it is.

    ``burst`` is the label-free burst-context dict (47a). ``None`` means the
    caller could not compute it — a fitting path, or a stream too short — and
    the answer is then False, so the rule behaves exactly as it did before this
    veto existed. Silence is not evidence of a burst.
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


def has_flush_flow_signature(features: Dict[str, Any],
                             calib: Optional[Dict[str, Any]] = None) -> bool:
    """Does this draw move water at the RATE a flush does, throughout?

    Deliberately NOT folded into ``is_flush_shaped``. That predicate is shared:
    the toilet rule uses it to CLAIM, while the washer sweep, the dishwasher
    cycle detector and the softener session detector use it to EXCLUDE. Adding
    a floor there would narrow what counts as a flush everywhere, which quietly
    LOOSENS all three of those detectors — a change to three tiers that has not
    been measured. So the floor lives here and is required only where a claim
    is made.

    ``true_avg_flow_lpm`` is preferred (it excludes the pressure-window padding
    in ``avg_flow_lpm``); legacy rows without it fall back, and rows with
    neither pass unchallenged rather than being delabelled by a feature they
    never had.
    """
    flow = _f(features, "true_avg_flow_lpm")
    # A draw that moved water cannot average zero. A non-positive value here
    # means the active-flow computation did not run for this event, not that
    # the flow was zero — the same coerced-0.0 trap the pressure feature
    # documents (`_PRESSURE_VALID_MIN_PSI`). Reading it as a measurement makes
    # this floor reject EVERY event, which is exactly what the 46r recording
    # corpus caught when this gate was first written: the genuine toilet in the
    # corpus replays with true_avg 0.0 and avg_flow 8.0.
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
    """Find dishwasher-cycle members on ``circuit``: a chain of >= _DW_CYCLE_MIN_MEMBERS
    small, gentle fills (vol in ``DW_VOL_L``, peak <= ``DW_MAX_PK_LPM``, not flush-shaped),
    each within ``_DW_CYCLE_CHAIN_GAP_MIN`` of the previous and the whole run within
    ``_DW_CYCLE_MAX_SPAN_MIN``. Returns ``{event_id: (role, group_id)}`` where role is
    ``'anchor'``/``'member'`` and group_id is the session's first event id (the History
    cycle-rollup key, like the washer detector).

    Catches dishwasher runs the per-event cycle-pulse rule misses: gentle fills fail the
    fill-shaped gate inside ``cycle_pulse_count`` and sit at cpc<3, so a real cycle (e.g.
    running concurrent with a washer) goes unlabelled. Reads ONLY feature/timestamp
    columns — never a label — so the eval harness's leave-one-out stays honest. Skips
    artifact-flagged events (phantom / cross-talk / dribble / excluded) and any id in
    ``exclude_ids`` (the washer/softener members the caller already claimed)."""
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
        # dev42 (T5) — per-candidate shape gate: a genuine fill is a gentle,
        # steady draw; the burst-chaining failure mode (short faucet bursts
        # strung into a fake fill-and-drain sequence — measured 1/10
        # precision post-reseed) rides on spiky, unsteady candidates.
        # Validated out-of-sample on the 7/01–7/21 pre-outage reviews:
        # recall 0.889, precision 0.727 (G6, dev42 plan §10.6). Configured
        # constants, NOT auto-fit — LOO showed thresholds are weakly
        # identified at n=50. NULL features (legacy rows) pass unchallenged:
        # the gate must not silently delabel history the features can't vet.
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

    ``pump_mode`` (dev24, confirmed vfd pump homes only): waives the toilet
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

    # Two vetoes, and they catch different mistakes. The flow signature rejects
    # draws that are the wrong SIZE or SHAPE for a flush; the burst check
    # rejects draws that are the right size in the wrong COMPANY — a washer
    # filling in stages produces a middle draw indistinguishable from a flush
    # in isolation, which is exactly what a per-event rule cannot see. A vetoed
    # claim is not a rejection: it falls through to the model tier, which has
    # the burst features and was measured to be right 53% of the time on these
    # against the rule's 24%.
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
# NOTE: a real regen is multi-draw (brine + >=1 backwash/rinse), but the gate is now a
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
    regen), AND that contains a REAL backwash — a >= ``_SOFTENER_BACKWASH_MIN_VOL_L``
    non-low fill, in-chain or trailing. A low-flow chain with no such fill (however
    many fragments) is rejected: a real regen always ends with a backwash/refill.
    A flush-shaped event ends the run (a 3 am flush during laundry/regen is
    irreducibly ambiguous, left to the per-event tiers). A trailing backwash up to
    ``_SOFTENER_BACKWASH_TAIL_MIN`` past the run is also claimed.

    ``band_center_min`` is minutes-since-LOCAL-midnight (parse_hhmm_to_minutes of
    the user's regen time). ``tz`` is the home timezone used to convert each
    event's stored-UTC start_ts to local for the band test — pass it so the band
    is DST-correct; tz=None compares in the stored (UTC) clock (tests/eval).
    ``since_ts`` bounds the scan for the live trailing pass. Reads only feature/
    timestamp columns plus the phantom/cross-talk ARTIFACT verdicts (never a fixture
    LABEL), so the eval's label-free LOO stays honest.

    Pressure-restoration phantoms and cross-talk are EXCLUDED from the candidate
    stream: both moved no real water on this circuit (volume_litres_effective == 0),
    so neither can be part of a brine draw. A 66-min phantom must not anchor a session
    or bridge an 80-min gap between unrelated drips — the 2026-06-16 false positive,
    where the phantom-bridged chain walked 3 h and absorbed a real 97 L shower as a
    fake "backwash", clearing even the backwash gate.
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

        # A real regen ALWAYS ends with a backwash/refill — a big (>= _SOFTENER_BACKWASH_
        # MIN_VOL_L), non-low, non-flush fill (~220 L observed), either in the chain
        # (last_bw_end, the >=30 L test above) or just past it (_is_trailing_bw, same
        # floor). A multi-event low-flow chain with NO such fill — e.g. scattered overnight
        # drips bridged across a gap by a zero-volume pressure-restoration phantom — is
        # incidental activity, not a regen, however many fragments it has. The backwash is
        # high-flow (~15 L/min) so coalescing (low-flow chatter only) can never absorb it
        # into the brine span, so requiring it never drops a real coalesced regen.
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
