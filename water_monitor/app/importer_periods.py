"""Pure period derivation for the historical importer (no instance state).

Reconstructing "when was water flowing?" from HA history is a self-contained
calculation over three sample series (flow-pulse onset, flow rate, pressure)
plus a set of thresholds. It reads nothing from the importer beyond those
thresholds -- no DB handle, no HA client, no queue, no ``_running`` flag -- so
it lives here as free functions and ``HistoricalImporter`` keeps thin methods
that forward to them.

Every function takes ``p`` first: any object exposing the threshold constants
(``MERGE_GAP_SECONDS``, ``MIN_FLOW_LPM``, ``PRESSURE_DIP_*`` ...). In production
that is the ``HistoricalImporter`` instance, so a per-instance or per-class
override of a threshold still takes effect exactly as it did when these were
methods. It is NOT used for anything else, which is what makes this module pure.

IMPORT DIRECTION IS ONE-WAY: this module must never import
``historical_importer``. ``historical_importer`` imports the primitives it still
uses (``_parse_ts``, ``_is_gap_marker``) from here at module level, and serves
``_GAP_MARKER_STATES`` / ``_merge_periods`` -- which only its callers still want
-- through a PEP 562 module ``__getattr__``. Adding an import back the other way
would recreate the partially-initialised-module ImportError that the
``feature_extractor`` / ``feature_extractor_service`` split had to solve with the
same ``__getattr__``. ``test_importer_periods_seam.py`` fails if it is added.

Sibling calls inside this module resolve through the MODULE globals, not through
``p``, so a test that wants to substitute one (e.g. feeding a synthetic pressure
envelope into ``_find_flow_periods``) must patch the attribute on THIS module:

    monkeypatch.setattr(importer_periods, "_pressure_to_periods", fake)

Patching the importer instance instead binds a new attribute nobody reads --
the substitution silently does nothing and the test measures nothing.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .overlap_guard import CONTAINMENT_FRACTION, contained_fraction

log = logging.getLogger(__name__)


_GAP_MARKER_STATES = frozenset({"unavailable", "unknown", "none", ""})


def _is_gap_marker(entry: Dict) -> bool:
    """True when an HA history entry marks a recorder/sensor gap rather than a
    reading ('unavailable' at HA restarts, 'unknown' on dropouts). Windows holding
    these must not be trusted to reproduce live-recorded water (dev.41)."""
    return str(entry.get("state", "")).strip().lower() in _GAP_MARKER_STATES


def _parse_ts(ts_value: Any) -> Optional[datetime]:
    if ts_value is None:
        return None
    if isinstance(ts_value, datetime):
        return ts_value if ts_value.tzinfo else ts_value.replace(tzinfo=timezone.utc)
    try:
        s = str(ts_value).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


def _merge_periods(
    periods: List[Tuple[datetime, datetime]],
    gap_seconds: int,
) -> List[Tuple[datetime, datetime]]:
    """
    Merge adjacent or overlapping periods separated by <= gap_seconds.
    Input must be sorted by start time.
    """
    if not periods:
        return []
    merged = [periods[0]]
    for start, end in periods[1:]:
        prev_start, prev_end = merged[-1]
        gap = (start - prev_end).total_seconds()
        if gap <= gap_seconds:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


# ------------------------------------------------------------------ #
# Period derivation                                                   #
# ------------------------------------------------------------------ #


def _split_period_around_rows(
    p, period: Tuple[datetime, datetime], rows: List[Dict],
    flow_rate_hist: List[Dict],
    flow_fragments: List[Tuple[datetime, datetime]],
) -> Tuple[str, List[Tuple[datetime, datetime]], Dict]:
    """Pure decision for one reconstructed period against stored rows.

    Returns ``("keep", [period], {})`` when no stored row is >=70 % inside the
    period; ``("drop", [], info)`` when the contained rows' RAW volume reaches
    ``CONTAINED_ROWS_COVERAGE`` of the period's integrated flow; otherwise
    ``("split", remainders, info)`` — the flow-rate fragments lying in the parts
    of the period NOT covered by the union of the contained rows, clipped to
    those gaps and re-merged with ``MERGE_GAP_SECONDS`` (a gap is not a draw; a
    pressure sag tail alone is not a draw), each kept only if it lasts
    ``MIN_DURATION_SECONDS`` and integrates to at least
    ``CONTAINED_REMAINDER_MIN_L``; the ``MAX_REMAINDERS_PER_PERIOD`` largest by
    volume survive, returned in time order."""
    ps, pe = period
    contained: List[Tuple[datetime, datetime, float]] = []
    for r in rows:
        s0 = _parse_ts(r.get("start_ts"))
        e0 = _parse_ts(r.get("end_ts"))
        if s0 is None or e0 is None or e0 <= s0:
            continue
        # dev57 (§2.36) — the guard's own threshold, not a second copy of
        # the number: this is the same containment question over the same
        # spans, so it must move with _CONTAINMENT_FRACTION.
        if contained_fraction((s0, e0), (ps, pe)) >= CONTAINMENT_FRACTION:
            contained.append((s0, e0, float(r.get("volume_litres") or 0.0)))
    if not contained:
        return "keep", [period], {}
    # top-level rows only: a row nested inside another contained row is the
    # same water again (dev33 §1.3) and must not inflate the stored total.
    contained.sort(key=lambda t: (t[0], -(t[1] - t[0]).total_seconds()))
    top: List[Tuple[datetime, datetime, float]] = []
    for s0, e0, v in contained:
        if top and s0 >= top[-1][0] and e0 <= top[-1][1]:
            continue
        top.append((s0, e0, v))
    stored_l = sum(v for _, _, v in top)
    period_l = _flow_volume_in_period(p, flow_rate_hist, ps, pe)
    info: Dict = {"n_rows": len(top), "stored_l": round(stored_l, 3),
                  "period_l": round(period_l, 3), "kept_l": 0.0, "dropped": 0}
    if period_l <= 0.0 or stored_l >= p.CONTAINED_ROWS_COVERAGE * period_l:
        return "drop", [], info
    # complement of the union of the top-level spans inside the period
    merged: List[List[datetime]] = []
    for s0, e0, _ in top:
        if merged and s0 <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e0)
        else:
            merged.append([s0, e0])
    gaps: List[Tuple[datetime, datetime]] = []
    cursor = ps
    for s0, e0 in merged:
        if s0 > cursor:
            gaps.append((cursor, min(s0, pe)))
        cursor = max(cursor, e0)
    if cursor < pe:
        gaps.append((cursor, pe))
    # A gap between stored rows is NOT itself a draw: a 30-minute idle with an
    # 8 s pressure-sag tail at each end must not become a 30-minute event of
    # 0.3 L. Candidates are the flow-rate fragments INSIDE the gap, clipped to
    # it and re-merged with the importer's own gap rule, so each real draw
    # between two stored rows becomes one tight remainder and a sag tail is
    # judged alone (and fails the volume floor).
    kept: List[Tuple[datetime, datetime, float]] = []
    dropped = 0
    for gs, ge in gaps:
        inside = sorted((max(fs, gs), min(fe, ge)) for fs, fe in flow_fragments
                        if fe > gs and fs < ge)
        if not inside:
            dropped += 1                    # envelope only — no flow
            continue
        for cs, ce in _merge_periods(inside, p.MERGE_GAP_SECONDS):
            dur = (ce - cs).total_seconds()
            vol = _flow_volume_in_period(p, flow_rate_hist, cs, ce)
            if (dur >= p.MIN_DURATION_SECONDS
                    and vol >= p.CONTAINED_REMAINDER_MIN_L):
                kept.append((cs, ce, vol))
            else:
                dropped += 1
    kept.sort(key=lambda t: -t[2])
    dropped += max(0, len(kept) - p.MAX_REMAINDERS_PER_PERIOD)
    kept = sorted(kept[:p.MAX_REMAINDERS_PER_PERIOD], key=lambda t: t[0])
    info["kept_l"] = round(sum(v for _, _, v in kept), 3)
    info["dropped"] = dropped
    return "split", [(gs, ge) for gs, ge, _ in kept], info

def _trailing_active_start(
    p,
    onset_hist: List[Dict],
    flow_rate_hist: List[Dict],
) -> Optional[datetime]:
    """Start of a flow period still ON at the end of the fetched history.

    Mirrors the still-active detection in _onset_to_periods / _rate_to_periods
    (a trailing ON run with no closing OFF transition) but reports the run's
    START instead of dropping it, so _import_range can hold the catch-up
    checkpoint back. Returns the earliest such start across the onset and
    flow-rate signals, or None if flow had clearly stopped by window end.
    """
    def _trailing_open(history: List[Dict], is_on) -> Optional[datetime]:
        open_start: Optional[datetime] = None
        for entry in history:
            ts = _parse_ts(entry.get("last_changed"))
            if ts is None:
                continue
            if is_on(entry):
                if open_start is None:
                    open_start = ts
            else:
                open_start = None
        return open_start

    def _onset_on(entry: Dict) -> bool:
        return str(entry.get("state", "")).lower() in ("on", "true", "1")

    def _rate_on(entry: Dict) -> bool:
        try:
            return float(entry["state"]) >= p.MIN_FLOW_LPM
        except (ValueError, TypeError, KeyError):
            return False

    starts = [s for s in (_trailing_open(onset_hist, _onset_on),
                          _trailing_open(flow_rate_hist, _rate_on))
              if s is not None]
    return min(starts) if starts else None

def _find_flow_periods(
    p,
    onset_hist: List[Dict],
    flow_rate_hist: List[Dict],
    query_end: Optional[datetime] = None,
    pressure_hist: Optional[List[Dict]] = None,
    using_avg_pressure: bool = False,
) -> List[Tuple[datetime, datetime]]:
    """
    Merge flow_pulse_onset ON periods, flow_rate > threshold periods, and
    (optionally) sustained pressure-dip periods into a unified, gap-filled,
    deduplicated list.

    The pressure-dip source bridges pulsed-flow events whose bursts are too
    far apart for MERGE_GAP_SECONDS — the continuous dip envelope covers the
    full event window and _merge_periods fuses it with the flow fragments.
    """
    onset_periods    = _onset_to_periods(p, onset_hist, query_end=query_end)
    rate_periods     = _rate_to_periods(p, flow_rate_hist, query_end=query_end)
    pressure_periods = _pressure_to_periods(
        p, pressure_hist or [], query_end=query_end,
        using_avg_pressure=using_avg_pressure,
    )
    # dev.39 — drop long pressure-dip envelopes that only bridge trivial noise
    # (the bug: two ~0.3 L blips 20 min apart fused into one 20-min event).
    # PROVABLY LEAK-NEUTRAL (hardened after an adversarial review): a dip is
    # dropped ONLY when every condition holds, so dropping it can NEVER lose or
    # under-count flow —
    #   (a) it is long (>= LONG_SPAN), and
    #   (b) the measured flow inside is trivial (< MIN_VOLUME), and
    #   (c) there IS real flow inside (>=1 overlapping rate/onset period), and
    #   (d) EVERY overlapping flow fragment already survives on its own
    #       (>= MIN_DURATION_SECONDS) once the bridge is removed.
    # So we only ever remove the empty *span between* fragments that stand alone.
    # A pure-pressure dip with no flow (c fails) is KEPT → handled by the phantom
    # guard at feature extraction, never silently dropped. A fragment too short to
    # survive un-bridged (d fails) KEEPS its bridge, so it is never orphaned —
    # closing the "sub-MIN_DURATION blip vanishes" and "recorder-gap masks a leak"
    # holes the review raised (a real leak is flow >= MIN_FLOW → a surviving rate
    # period, never only an empty envelope).
    flow_only = onset_periods + rate_periods
    kept_dips = []
    for s, e in pressure_periods:
        overlap = [(fs, fe) for fs, fe in flow_only if fe > s and fs < e]
        span_vol = (_flow_volume_in_period(p, flow_rate_hist, s, e)
                    if (e - s).total_seconds()
                    >= p.PRESSURE_DIP_BRIDGE_LONG_SPAN_S else None)
        if (span_vol is not None
                and span_vol < p.PRESSURE_DIP_BRIDGE_MIN_VOLUME_L
                and overlap
                and all((fe - fs).total_seconds() >= p.MIN_DURATION_SECONDS
                        for fs, fe in overlap)):
            log.info("[importer] dropping %.0f-min pressure-dip bridge with only "
                     "%.2f L flow across %d p-standing fragment(s) — noise, not "
                     "a real bridged event",
                     (e - s).total_seconds() / 60.0, span_vol, len(overlap))
            continue
        # dev.50 — a kept bridge may still span a LONG proven-idle gap; break it
        # there so the draws either side reconstruct as the separate events they are.
        kept_dips.extend(_split_dip_on_idle_gaps(
            p, s, e, overlap, flow_rate_hist, onset_hist))
    pressure_periods = kept_dips

    all_periods = onset_periods + rate_periods + pressure_periods
    if not all_periods:
        return []

    merged = _merge_periods(sorted(all_periods), p.MERGE_GAP_SECONDS)
    return [(s, e) for s, e in merged
            if (e - s).total_seconds() >= p.MIN_DURATION_SECONDS]

def _flow_stopped_across(
    p,
    flow_rate_hist: List[Dict],
    onset_hist: List[Dict],
    gap_start: datetime,
    gap_end: datetime,
) -> bool:
    """dev.50 — does the history PROVE flow stopped across ``[gap_start, gap_end]``?

    A flow sensor that goes dark mid-draw looks EXACTLY like an idle here, and the
    distinction decides whether it is safe to break a pressure-dip bridge. HA's
    recorder logs on CHANGE, so "samples exist and read zero" cannot be the test —
    a genuine idle emits no samples either. Worse, ``_rate_to_periods`` refuses to
    flush a period it never saw close, so a dropout mid-draw leaves no fragment at
    all and the stretch reads as pure idle.

    The honest discriminator is the last sample AT OR BEFORE the gap — the same
    reasoning the live detector uses for ``FLOW_SAMPLE_STALE_S`` ("the stuck-sensor
    case goes SILENT"). Fails CLOSED: a recorder-gap marker inside, real flow
    inside, or no sample to judge by at all all mean "not proven", and the caller
    leaves the envelope whole.
    """
    last_before: Optional[float] = None
    for entry in flow_rate_hist:
        ts = _parse_ts(entry.get("last_changed"))
        if ts is None:
            continue
        if _is_gap_marker(entry):
            if gap_start <= ts < gap_end:
                return False        # recorder outage — the history is blind here
            continue
        try:
            rate = float(entry["state"])
        except (ValueError, TypeError, KeyError):
            continue
        # The gap is the OPEN interval between two fragments: the sample AT
        # gap_end is the next fragment's own opening sample, not flow "inside".
        if ts <= gap_start:
            last_before = rate
        elif ts < gap_end and rate >= p.MIN_FLOW_LPM:
            return False            # real flow inside — not an idle at all
    for entry in onset_hist:        # an onset dropout blinds us the same way
        if not _is_gap_marker(entry):
            continue
        ts = _parse_ts(entry.get("last_changed"))
        if ts is not None and gap_start <= ts < gap_end:
            return False
    if last_before is None:
        return False                # nothing to judge by — refuse to split
    return last_before < p.MIN_FLOW_LPM

def _split_dip_on_idle_gaps(
    p,
    dip_start: datetime,
    dip_end: datetime,
    overlap: List[Tuple[datetime, datetime]],
    flow_rate_hist: List[Dict],
    onset_hist: List[Dict],
) -> List[Tuple[datetime, datetime]]:
    """dev.50 — break one kept dip envelope wherever it bridges a long PROVEN-idle
    gap, returning the sub-envelopes (``[(dip_start, dip_end)]`` when it bridges
    nothing).

    Boundaries land on the flow fragments themselves, so only the EMPTY span
    between them is removed — never flow, keeping the volume and leak reasoning of
    the drop-gate above intact. A dip with NO overlapping flow is returned whole:
    that is a pure-pressure envelope, which the phantom guard at feature extraction
    handles and this must never silently drop. The outer bounds stay at the dip's
    own, so a lead-in / tail-out is preserved.
    """
    if not overlap:
        return [(dip_start, dip_end)]
    frags = sorted((max(fs, dip_start), min(fe, dip_end)) for fs, fe in overlap)
    out: List[Tuple[datetime, datetime]] = []
    seg_start = dip_start
    run_end = frags[0][1]            # running max end — fragments may nest
    for next_start, next_end in frags[1:]:
        # Both sides must survive the MIN_DURATION filter at the end of
        # _find_flow_periods. Splitting a bridge whose fragments are shorter than
        # that ORPHANS them — the sub-envelopes are dropped and the flow vanishes,
        # which is precisely the leak-safety hole dev.39's condition (d) closed.
        # Too short on either side → keep bridging, exactly as the drop-gate does.
        if ((next_start - run_end).total_seconds()
                >= p.PRESSURE_DIP_BRIDGE_MAX_GAP_S
                and (run_end - seg_start).total_seconds()
                >= p.MIN_DURATION_SECONDS
                and (dip_end - next_start).total_seconds()
                >= p.MIN_DURATION_SECONDS
                and _flow_stopped_across(
                    p, flow_rate_hist, onset_hist, run_end, next_start)):
            out.append((seg_start, run_end))
            seg_start = next_start
        run_end = max(run_end, next_end)
    out.append((seg_start, dip_end))
    if len(out) > 1:
        log.info("[importer] split a %.0f-min pressure-dip bridge into %d envelope(s) "
                 "at %d proven-idle gap(s) >= %.0f s — the bridge spans inter-burst "
                 "gaps, not idles",
                 (dip_end - dip_start).total_seconds() / 60.0, len(out),
                 len(out) - 1, p.PRESSURE_DIP_BRIDGE_MAX_GAP_S)
    return out

def _flow_volume_in_period(
    p, flow_rate_hist: List[Dict], start: datetime, end: datetime,
) -> float:
    """Integrate flow_rate (L/min) over [start, end] from history → litres. Used
    to decide whether a long pressure-dip envelope contains real flow (bridge
    gate) and whether a dry-run reconstruction can account for a stored event's
    volume (auto-split trust gate).

    Delegates to ``flow_integral.integrate_litres`` — the shared integrator the
    live detector and volume recompute already use — so the semantics can't
    drift: LEFT-HOLD step integration (HA logs on change; trapezoidal would
    invent volume across no-flow gaps) plus the 120 s offline-gap clamp (a
    recorder outage can't fabricate held-flow volume). The history is clipped
    to the window, carrying the pre-window held rate in at ``start`` and
    holding the last rate out to ``end``."""
    from .flow_integral import integrate_litres

    samples: List[Tuple[datetime, float]] = []
    pre: Optional[Tuple[datetime, float]] = None
    for entry in flow_rate_hist:
        ts = _parse_ts(entry.get("last_changed"))
        if ts is None:
            continue
        try:
            rate = float(entry["state"])
        except (ValueError, TypeError, KeyError):
            rate = 0.0
        if ts < start:
            pre = (start, rate)      # last change before the window, held into it
        elif ts <= end:
            samples.append((ts, rate))
    if pre is not None:
        samples.insert(0, pre)
    if samples:
        samples.append((end, 0.0))   # close the window so the last hold counts
    litres, _capped = integrate_litres(samples)
    return litres

def _onset_to_periods(
    p,
    history: List[Dict],
    query_end: Optional[datetime] = None,
) -> List[Tuple[datetime, datetime]]:
    """
    Extract ON periods from flow_pulse_onset binary sensor history.
    Handles pre-existing ON state at window start (state at first entry).

    query_end: if the sensor is still ON at the end of the history window,
    the period is closed at query_end (the original request end time) rather
    than at the last history entry's timestamp, preventing spurious
    zero-duration periods when the last entry IS the onset itself.
    """
    periods: List[Tuple[datetime, datetime]] = []
    current_start: Optional[datetime] = None

    for entry in history:
        state = str(entry.get("state", "")).lower()
        ts = _parse_ts(entry.get("last_changed"))
        if ts is None:
            continue

        if state in ("on", "true", "1"):
            if current_start is None:
                current_start = ts
        else:
            if current_start is not None:
                periods.append((current_start, ts))
                current_start = None

    # Still ON at end of window — DO NOT flush. Emitting a period here
    # would insert a partial event that then blocks the real event from
    # being stored when the live detector finishes it (overlap rule fires
    # at ratio = 1.0 since the partial is fully contained). The next
    # importer run will see the full closed period and import it
    # correctly, or the live detector will store the complete event.
    if current_start is not None:
        log.info(
            "[importer] skipping still-active onset period "
            "(start=%s, query_end=%s) — deferring to next run / live detector",
            current_start.isoformat(),
            query_end.isoformat() if query_end is not None else "?",
        )

    return periods

def _rate_to_periods(
    p, history: List[Dict],
    query_end: Optional[datetime] = None,
) -> List[Tuple[datetime, datetime]]:
    """
    Extract periods where flow_rate >= MIN_FLOW_LPM from 1Hz history.
    """
    periods: List[Tuple[datetime, datetime]] = []
    current_start: Optional[datetime] = None
    # (no last_ts tracking — see the comment in the loop body
    # below; off-transition `ts` is used directly.)

    for entry in history:
        ts = _parse_ts(entry.get("last_changed"))
        if ts is None:
            continue
        if _is_gap_marker(entry):
            # A recorder/sensor gap is the ABSENCE of a reading, not a
            # reading of zero. Falling through to `rate = 0.0` below closed
            # the period at the dropout, truncating a draw that was still
            # running — the water after the gap then became a separate event
            # or none at all. _flow_stopped_across already refuses to read
            # absence of data as absence of water ("a dark sensor looks
            # EXACTLY like an idle here"); this applies the same rule where
            # the periods are BUILT rather than where they are judged.
            #
            # Bridging is bounded, not open-ended: flow_integral clamps any
            # inter-sample gap to _FLOW_INTEGRAL_MAX_DT_SECONDS (120 s) and
            # sets `capped`, so an outage adds at most 120 s of the last
            # known rate to the volume and flags that it did.
            continue
        try:
            rate = float(entry["state"])
        except (ValueError, TypeError, KeyError):
            # Unparseable but NOT a gap marker — genuinely garbage numeric
            # state. Keep the original behaviour and treat it as off; only
            # the "we are blind here" case changes above.
            rate = 0.0

        if rate >= p.MIN_FLOW_LPM:
            if current_start is None:
                current_start = ts
        else:
            if current_start is not None:
                # Use ts (the off-transition) not last_ts, consistent with
                # _onset_to_periods which closes at the OFF timestamp.
                periods.append((current_start, ts))
                current_start = None
        # (last_ts tracking removed — the off-transition `ts` is used
        # directly above, per the same convention as _onset_to_periods.)

    # Same rationale as _onset_to_periods: do NOT flush still-active
    # flow-rate periods at query_end. The live detector / next importer
    # run will handle them once flow actually drops.
    if current_start is not None:
        log.info(
            "[importer] skipping still-active flow-rate period "
            "(start=%s, query_end=%s) — deferring to next run / live detector",
            current_start.isoformat(),
            query_end.isoformat() if query_end is not None else "?",
        )

    return periods

def _pressure_to_periods(
    p,
    history: List[Dict],
    query_end: Optional[datetime] = None,
    using_avg_pressure: bool = False,
) -> List[Tuple[datetime, datetime]]:
    """Emit (start, end) periods for each sustained pressure dip.

    State machine over time-ordered pressure samples:
      IDLE      — rolling baseline from the last PRESSURE_DIP_BASELINE_WINDOW_S
      CANDIDATE — freeze baseline; require sustained dip before opening
      OPEN      — period active; watch for recovery
      RECOVERING — sustained recovery required before closing

    The frozen-baseline design prevents the baseline from drifting downward
    inside a real dip, which would cause the dip to look smaller than it is.
    """
    if not history:
        return []

    # Effective threshold and open-sustain depend on sensor quality.
    if using_avg_pressure:
        effective_thr  = max(p.PRESSURE_DIP_AVG_MIN_THRESHOLD_PSI,
                             p.PRESSURE_DIP_PERIOD_PSI * 0.3)
        open_duration  = p.PRESSURE_DIP_AVG_OPEN_DURATION_S
    else:
        effective_thr  = p.PRESSURE_DIP_PERIOD_PSI
        open_duration  = p.PRESSURE_DIP_OPEN_DURATION_S
    close_duration = p.PRESSURE_DIP_CLOSE_DURATION_S

    # State
    STATE_IDLE       = 0
    STATE_CANDIDATE  = 1
    STATE_OPEN       = 2
    STATE_RECOVERING = 3

    state           = STATE_IDLE
    idle_window: List[Tuple[datetime, float]] = []  # (ts, psi) for rolling mean
    frozen_baseline : float = 0.0
    candidate_start : Optional[datetime] = None
    recovery_start  : Optional[datetime] = None
    periods: List[Tuple[datetime, datetime]] = []

    samples = []
    for entry in history:
        ts = _parse_ts(entry.get("last_changed"))
        if ts is None:
            continue
        try:
            psi = float(entry["state"])
        except (ValueError, TypeError, KeyError):
            continue
        if not math.isfinite(psi):
            continue
        samples.append((ts, psi))

    if not samples:
        return []

    last_ts = samples[-1][0]

    for ts, psi in samples:

        # ── IDLE: maintain rolling baseline ──────────────────────────
        if state == STATE_IDLE:
            cutoff = ts - timedelta(seconds=p.PRESSURE_DIP_BASELINE_WINDOW_S)
            idle_window = [(t, v) for t, v in idle_window if t >= cutoff]
            # Compute baseline from the PREVIOUS window (before this sample) so
            # a dip at exactly the threshold still triggers rather than diluting
            # the baseline and causing a missed detection.
            if idle_window:
                baseline = sum(v for _, v in idle_window) / len(idle_window)
                span_s = (
                    (idle_window[-1][0] - idle_window[0][0]).total_seconds()
                    if len(idle_window) > 1 else 0.0
                )
                if (
                    len(idle_window) >= p.PRESSURE_DIP_MIN_BASELINE_SAMPLES
                    and span_s >= p.PRESSURE_DIP_MIN_BASELINE_SPAN_S
                    and psi <= baseline - effective_thr
                ):
                    # Freeze baseline and start the candidate clock;
                    # do NOT add the dip sample to the idle window.
                    frozen_baseline = baseline
                    candidate_start = ts
                    state = STATE_CANDIDATE
                    continue
            idle_window.append((ts, psi))

        # ── CANDIDATE: check sustain (do NOT update idle_window) ─────
        elif state == STATE_CANDIDATE:
            assert candidate_start is not None
            if psi > frozen_baseline - effective_thr:
                # Cancelled before sustained — return to IDLE
                # Re-feed this sample into the idle window
                cutoff = ts - timedelta(seconds=p.PRESSURE_DIP_BASELINE_WINDOW_S)
                idle_window = [(t, v) for t, v in idle_window if t >= cutoff]
                idle_window.append((ts, psi))
                state = STATE_IDLE
            elif (ts - candidate_start).total_seconds() >= open_duration:
                state = STATE_OPEN

        # ── OPEN: watch for start of recovery ────────────────────────
        elif state == STATE_OPEN:
            recovery_line = frozen_baseline - effective_thr * 0.5
            if psi >= recovery_line:
                recovery_start = ts
                state = STATE_RECOVERING

        # ── RECOVERING: require sustained recovery before closing ─────
        elif state == STATE_RECOVERING:
            assert recovery_start is not None
            assert candidate_start is not None
            recovery_line = frozen_baseline - effective_thr * 0.5
            if psi < recovery_line:
                # Pressure dipped again — cancel recovery, stay OPEN
                recovery_start = None
                state = STATE_OPEN
            elif (ts - recovery_start).total_seconds() >= close_duration:
                periods.append((candidate_start, recovery_start))
                # Reset to IDLE; re-feed this sample as start of idle window
                idle_window = [(ts, psi)]
                state = STATE_IDLE
                candidate_start = None
                recovery_start  = None
                frozen_baseline = 0.0

    # End-of-history flush — only flush STATE_RECOVERING, NOT STATE_OPEN.
    # If the dip is still open at query_end the event is likely still in
    # progress; emitting it now would insert a partial event that blocks
    # the full one when the importer runs again after the event ends.
    if state == STATE_RECOVERING and candidate_start is not None:
        close_ts = recovery_start or last_ts
        if query_end is not None:
            close_ts = min(close_ts, query_end)
        if close_ts > candidate_start:
            periods.append((candidate_start, close_ts))

    return periods
