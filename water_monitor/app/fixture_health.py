"""dev47 (47i) — fixture health: attribution adapts, baselines lock.

THE PROBLEM THIS SOLVES
-----------------------
A toilet flapper degrades. Refill volume creeps from 1.6 to 2.3 gal. Every one
of those events is still, correctly, a toilet — so the operator labels them
"toilet", the model retrains, and the new normal is learned. The home quietly
loses ~26 L/day (the same magnitude as the July valve fault) and every accuracy
metric says everything is fine, because the labels ARE fine. The label schema
conflates *which fixture* with *is it healthy*.

The fix is separation, not re-locking the classifier:

  * **Attribution adapts.** The model may absorb drift; that is desirable,
    because an absorbed event stays in its own class where this module can see
    it. A leaking toilet is still a toilet.
  * **Health baselines lock.** Detection compares the CLASSIFIED event stream
    against a baseline frozen from a pinned pre-drift window — never against
    the model, its confidence, or the training pool.

The two enemies are therefore **reference contamination** (comparing against
anything that adapts) and **attribution discontinuity** (drift walking events
across a class boundary, out of their own trend line).

WHAT THE SIMULATIONS MEASURED, AND WHAT IT CHANGED
--------------------------------------------------
V6 (2026-08-22) showed absorption does NOT blind detection under this design:
the volume alarm fired on the same day with weekly retrains on or off. V6d then
added a realistic review card and found the opposite risk — with a capped card,
a FROZEN model never detected a +40% flapper at all in 21 days, because the
events it rejected never reached the trend line. Adaptive classification is a
detection *dependency* here, not a threat.

Three detector requirements come directly from those runs:

1. **Rolling-EVENT windows, never calendar-day medians.** At real label density
   most days hold 0-1 toilet events, so a "N consecutive days above threshold"
   rule never accumulates: the daily-median detector fired NEVER in 21 days
   while the rolling-15 detector fired on day 12.5.
2. **The unsolicited-refill rate is the fast signal** (fired day 3 vs day 12-14
   for the volume trend) and is immune to the dilution confound below.
3. **Class share must NOT use median ± k·MAD.** That formulation computed a
   floor of −0.022 on this home — a negative threshold on a proportion, i.e. a
   detector structurally incapable of firing, which passes silently forever.
   It is a relative-drop test against the frozen share instead, and
   :func:`share_alarm_is_live` exists so that class of bug cannot recur.

LATENCY IS QUOTED IN EVENTS, NOT DAYS
-------------------------------------
"Day 12" is arithmetic, not physics: at 2.3 toilets/day it takes ~18 qualifying
events to fill a rolling window and sustain a trend. A guest bath at 0.3
events/day needs the same 18 events and therefore ~2 MONTHS. Reporting days
alone would let a quiet fixture's silence read as good health, so every alarm
carries its events-to-fire and every fixture carries its expected latency.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

log = logging.getLogger(__name__)

# ── detector constants (pinned; the V5/V6/V6d runs used exactly these) ──────
ROLLING_WINDOW: int = 15          # class events per rolling median
SUSTAIN: int = 3                  # consecutive evaluations above threshold
MAD_K: float = 3.0                # robust sigma multiplier for the volume trend
MAD_SCALE: float = 1.4826         # MAD -> sigma for a normal
MIN_BASELINE_EVENTS: int = 20     # below this a baseline is not trustworthy
MIN_TREND_EVENTS: int = 10        # need a partial window before evaluating

# Volume trend only considers real draws of the class; a 0.2 L dribble is not a
# refill and would drag the median toward nothing.
MIN_TREND_VOLUME_L: float = 2.5

# Unsolicited refills: refill-SHAPED draws with no preceding flush. Recognised
# by shape rule, deliberately independent of attribution (47i-1) — a phantom
# refill that the classifier mislabels must still count.
UNSOLICITED_WINDOW_DAYS: int = 7
UNSOLICITED_ALARM_COUNT: int = 8
UNSOLICITED_PRECEDING_FLUSH_S: float = 900.0
# A leaking flapper does not produce FLUSHES — it produces partial top-ups as
# the fill valve replaces water that seeped away. So the candidate band is
# deliberately BELOW a full flush: anything of normal flush size is just a
# flush, whatever preceded it.
UNSOLICITED_MIN_FRACTION: float = 0.25
UNSOLICITED_MAX_FRACTION: float = 0.75
# ...and it must run like the fixture. Volume alone is far too permissive on a
# real stream: measured on this home, a volume-only rule counted ordinary small
# tap draws as "unsolicited toilet refills" and fired within nine events. A
# refill is driven by the same fill valve as a flush, so its PEAK flow sits
# close to the fixture's; a tap or a gentle appliance fill does not.
UNSOLICITED_PEAK_LO_FRACTION: float = 0.6
UNSOLICITED_PEAK_HI_FRACTION: float = 1.4

# Class share: relative drop against the FROZEN share. Never median +/- k*MAD
# on a proportion (V6d: that computed a negative floor).
SHARE_WINDOW_DAYS: int = 14
SHARE_DROP_RATIO: float = 0.8     # sustained below 80% of the frozen share
SHARE_MIN_EVENTS: int = 20        # a window too thin to read

SIGNAL_VOLUME = "volume_trend"
SIGNAL_DURATION = "duration_trend"
SIGNAL_UNSOLICITED = "unsolicited_refills"
SIGNAL_SHARE = "class_share"
SIGNAL_ANCHOR = "anchor_claim_rate"

# Reason codes for unlocking a baseline (47i-2). A baseline may move only by
# explicit confirmation, and the code is what distinguishes the three kinds of
# drift the whole plan turns on.
REASON_FIXTURE_REPLACED = "fixture_replaced"
REASON_REPAIRED = "fixture_repaired"
REASON_FALSE_ALARM = "false_alarm"
UNLOCK_REASONS = frozenset({REASON_FIXTURE_REPLACED, REASON_REPAIRED,
                            REASON_FALSE_ALARM})


class ReferenceContamination(RuntimeError):
    """An adaptive or post-pin value was offered as a frozen reference."""


# ── the frozen baseline ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class FrozenBaseline:
    """A fixture's normal, measured once over a pinned window.

    Immutable after construction, and it refuses input from outside its own
    window. That refusal is the point: every way this design can fail quietly
    routes through some adaptive value creeping into the reference, so the
    reference rejects them structurally rather than by convention.
    """

    circuit: str
    fixture_type: str
    window_start: str
    window_end: str
    n_events: int
    volume_median: float
    volume_mad: float
    duration_median: float
    duration_mad: float
    peak_median: float
    share: Optional[float] = None
    cadence_days: Optional[float] = None
    baseline_hash: str = ""
    pinned_at: str = ""

    def covers(self, ts: str) -> bool:
        return self.window_start <= str(ts) < self.window_end

    def volume_threshold(self, k: float = MAD_K) -> float:
        return self.volume_median + k * self.volume_mad

    def is_usable(self) -> bool:
        """Is this reference tight enough to detect anything?

        Measured on real data: the dishwasher class mixes ~1 L fills with much
        larger aggregate draws, giving MAD 14.5 L against a median of 13.7 L.
        The resulting threshold (median + 3*MAD = 57 L) could never fire, so
        the baseline would sit there looking like protection while providing
        none. A class whose spread exceeds its centre is not one fixture's
        signature; say so instead of pretending.
        """
        return (self.n_events >= MIN_BASELINE_EVENTS
                and self.volume_median > 0
                and self.volume_mad <= self.volume_median)

    def duration_threshold(self, k: float = MAD_K) -> float:
        return self.duration_median + k * self.duration_mad

    def share_floor(self, ratio: float = SHARE_DROP_RATIO) -> Optional[float]:
        """Relative drop, NOT median - k*MAD.

        On this home the MAD form produced a floor of -0.022: a proportion can
        never be negative, so the alarm could not fire in any scenario and the
        silence read as health. A ratio floor is always attainable, which is
        what :func:`share_alarm_is_live` asserts.
        """
        return None if self.share is None else self.share * ratio

    def to_json(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_json(cls, d: dict) -> "FrozenBaseline":
        return cls(**{k: v for k, v in d.items()
                      if k in cls.__dataclass_fields__})


def _median(xs: Sequence[float]) -> float:
    s = sorted(xs)
    n = len(s)
    if not n:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _mad(xs: Sequence[float], centre: Optional[float] = None) -> float:
    if not xs:
        return 0.0
    c = _median(xs) if centre is None else centre
    return _median([abs(x - c) for x in xs]) * MAD_SCALE


def build_baseline(circuit: str, fixture_type: str, events: Sequence[dict],
                   window_start: str, window_end: str,
                   share: Optional[float] = None) -> FrozenBaseline:
    """Freeze a fixture's normal from events inside the pinned window.

    Raises :class:`ReferenceContamination` if any event lies outside the
    window. Callers must filter first — silently dropping strays would let a
    drifted event widen the very reference meant to detect it.
    """
    stray = [e for e in events
             if not (window_start <= str(e.get("start_ts")) < window_end)]
    if stray:
        raise ReferenceContamination(
            f"{len(stray)} event(s) outside the pinned window "
            f"[{window_start}, {window_end}) offered to the {fixture_type} "
            "baseline; a frozen reference may only see its own window")
    vols = [float(e["volume_litres"]) for e in events
            if e.get("volume_litres") is not None
            and float(e["volume_litres"]) >= MIN_TREND_VOLUME_L]
    durs = [float(e["duration_seconds"]) for e in events
            if e.get("duration_seconds") is not None]
    peaks = [float(e["peak_flow_lpm"]) for e in events
             if e.get("peak_flow_lpm") is not None]
    vol_med = _median(vols)
    dur_med = _median(durs)
    payload = {
        "circuit": circuit, "fixture_type": fixture_type,
        "window_start": window_start, "window_end": window_end,
        "n_events": len(events),
        "volume_median": round(vol_med, 4),
        "volume_mad": round(_mad(vols, vol_med), 4),
        "duration_median": round(dur_med, 4),
        "duration_mad": round(_mad(durs, dur_med), 4),
        "peak_median": round(_median(peaks), 4),
        "share": None if share is None else round(share, 5),
        "cadence_days": _cadence_days(events),
    }
    h = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return FrozenBaseline(baseline_hash=h[:16],
                          pinned_at=datetime.now(timezone.utc).isoformat(),
                          **payload)


def _cadence_days(events: Sequence[dict]) -> Optional[float]:
    """Median days between occurrences — meaningful for cyclic fixtures only
    (softener regens, and the 47f cadence watch that is its first instance)."""
    ts = sorted(str(e.get("start_ts")) for e in events if e.get("start_ts"))
    if len(ts) < 3:
        return None
    gaps = []
    for a, b in zip(ts, ts[1:]):
        try:
            da = datetime.fromisoformat(a.replace("Z", "+00:00"))
            db_ = datetime.fromisoformat(b.replace("Z", "+00:00"))
        except ValueError:
            continue
        gap = (db_ - da).total_seconds() / 86400.0
        if gap > 0.25:                 # same-session repeats are not a cycle
            gaps.append(gap)
    return round(_median(gaps), 3) if gaps else None


# ── the detectors ───────────────────────────────────────────────────────────
@dataclass
class Alarm:
    signal: str
    fixture_type: str
    fired: bool
    events_to_fire: Optional[int] = None
    fired_at: Optional[str] = None
    observed: Optional[float] = None
    threshold: Optional[float] = None
    divergence_at: Optional[str] = None
    detail: str = ""
    series: List[tuple] = field(default_factory=list)

    def as_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "series"}
        d["series_tail"] = self.series[-8:]
        return d


def _rolling_alarm(values: Sequence[Tuple[str, float]], threshold: float,
                   soft_threshold: float, signal: str, fixture_type: str,
                   detail: str) -> Alarm:
    """Shared shape: rolling median over the last ROLLING_WINDOW class events,
    alarm after SUSTAIN consecutive evaluations above ``threshold``."""
    alarm = Alarm(signal=signal, fixture_type=fixture_type, fired=False,
                  threshold=round(threshold, 4), detail=detail)
    streak = 0
    diverged_at = None
    for i in range(len(values)):
        if i + 1 < MIN_TREND_EVENTS:
            continue
        window = [v for _, v in values[max(0, i - ROLLING_WINDOW + 1):i + 1]]
        med = _median(window)
        ts = values[i][0]
        alarm.series.append((ts, round(med, 3)))
        if diverged_at is None and med > soft_threshold:
            diverged_at = ts
        streak = streak + 1 if med > threshold else 0
        if streak >= SUSTAIN and not alarm.fired:
            alarm.fired = True
            alarm.events_to_fire = i + 1
            alarm.fired_at = ts
            alarm.observed = round(med, 4)
            alarm.divergence_at = diverged_at
    if not alarm.fired and alarm.series:
        alarm.observed = alarm.series[-1][1]
        alarm.divergence_at = diverged_at
    return alarm


def volume_trend_alarm(baseline: FrozenBaseline,
                       attributed: Sequence[dict]) -> Alarm:
    """Rolling median of the ATTRIBUTED class stream vs the frozen baseline.

    ``attributed`` is the classified stream: each item needs ``start_ts`` and
    ``volume_litres``. It is deliberately the attributed stream and not
    ground truth — that is what makes absorption harmless and what makes a
    starved trend line (V6d's frozen arm) visible as silence.
    """
    values = [(str(e["start_ts"]), float(e["volume_litres"]))
              for e in sorted(attributed, key=lambda e: str(e["start_ts"]))
              if e.get("volume_litres") is not None
              and float(e["volume_litres"]) >= MIN_TREND_VOLUME_L]
    return _rolling_alarm(
        values, baseline.volume_threshold(),
        baseline.volume_median + baseline.volume_mad,
        SIGNAL_VOLUME, baseline.fixture_type,
        f"rolling-{ROLLING_WINDOW} median litres vs frozen "
        f"{baseline.volume_median:.2f} (MAD {baseline.volume_mad:.3f})")


def duration_trend_alarm(baseline: FrozenBaseline,
                         attributed: Sequence[dict]) -> Alarm:
    values = [(str(e["start_ts"]), float(e["duration_seconds"]))
              for e in sorted(attributed, key=lambda e: str(e["start_ts"]))
              if e.get("duration_seconds") is not None]
    return _rolling_alarm(
        values, baseline.duration_threshold(),
        baseline.duration_median + baseline.duration_mad,
        SIGNAL_DURATION, baseline.fixture_type,
        f"rolling-{ROLLING_WINDOW} median seconds vs frozen "
        f"{baseline.duration_median:.1f}")


def unsolicited_refill_alarm(refills: Sequence[dict],
                             fixture_type: str = "toilet") -> Alarm:
    """The fast channel: refill-shaped draws with no preceding flush.

    Fired day 3 in simulation against day 12-14 for the volume trend, and it is
    immune to the dilution confound (a second, lighter fixture in the same class
    drags a median; it cannot manufacture unsolicited refills).
    """
    alarm = Alarm(signal=SIGNAL_UNSOLICITED, fixture_type=fixture_type,
                  fired=False, threshold=UNSOLICITED_ALARM_COUNT,
                  detail=f">= {UNSOLICITED_ALARM_COUNT} unsolicited refills in "
                         f"any trailing {UNSOLICITED_WINDOW_DAYS} days")
    stamps = sorted(str(r["start_ts"]) for r in refills)
    for i, ts in enumerate(stamps):
        try:
            end = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        count = 0
        for other in stamps[:i + 1]:
            try:
                t = datetime.fromisoformat(other.replace("Z", "+00:00"))
            except ValueError:
                continue
            if 0 <= (end - t).total_seconds() <= UNSOLICITED_WINDOW_DAYS * 86400:
                count += 1
        alarm.series.append((ts, count))
        if count >= UNSOLICITED_ALARM_COUNT and not alarm.fired:
            alarm.fired = True
            alarm.events_to_fire = i + 1
            alarm.fired_at = ts
            alarm.observed = count
    if not alarm.fired and alarm.series:
        alarm.observed = alarm.series[-1][1]
    return alarm


def find_unsolicited_refills(events: Sequence[dict],
                             baseline: FrozenBaseline) -> List[dict]:
    """Refill-shaped draws with no flush shortly before them.

    Shape-based and attribution-independent by design (47i-1): a phantom refill
    the classifier gets wrong must still be counted, or the fast channel would
    inherit the classifier's blind spots.

    "Refill-shaped" means a PARTIAL draw — between
    ``UNSOLICITED_MIN_FRACTION`` and ``UNSOLICITED_MAX_FRACTION`` of the frozen
    median. A full-flush-sized draw is a flush; counting those would flag every
    healthy toilet in the house on its first event, since nothing precedes it.
    """
    ordered = sorted(events, key=lambda e: str(e.get("start_ts")))
    lo = baseline.volume_median * UNSOLICITED_MIN_FRACTION
    hi = baseline.volume_median * UNSOLICITED_MAX_FRACTION
    pk_lo = baseline.peak_median * UNSOLICITED_PEAK_LO_FRACTION
    pk_hi = baseline.peak_median * UNSOLICITED_PEAK_HI_FRACTION
    out: List[dict] = []
    for i, e in enumerate(ordered):
        vol = e.get("volume_litres")
        if vol is None or not (lo <= float(vol) <= hi):
            continue
        pk = e.get("peak_flow_lpm")
        if baseline.peak_median > 0 and (
                pk is None or not (pk_lo <= float(pk) <= pk_hi)):
            continue          # right size, wrong plumbing — not this fixture
        try:
            t = datetime.fromisoformat(str(e["start_ts"]).replace("Z", "+00:00"))
        except (ValueError, KeyError):
            continue
        preceded = False
        for prev in reversed(ordered[:i]):
            try:
                tp = datetime.fromisoformat(
                    str(prev["start_ts"]).replace("Z", "+00:00"))
            except (ValueError, KeyError):
                continue
            gap = (t - tp).total_seconds()
            if gap > UNSOLICITED_PRECEDING_FLUSH_S:
                break
            pv = prev.get("volume_litres")
            if pv is not None and float(pv) >= baseline.volume_median * 0.6:
                preceded = True
                break
        if not preceded:
            out.append(e)
    return out


def class_share_alarm(baseline: FrozenBaseline, attributed: Sequence[dict],
                      fixture_type: Optional[str] = None) -> Alarm:
    """Sustained relative drop in a class's share of the attributed stream.

    This is the signal that survives when the volume trend cannot fire: under a
    capped review card, drifted events the model rejects never reach the trend
    line at all, and their ABSENCE is then the only evidence. V6d measured the
    share sagging .222 -> .167-.192 in exactly those arms.
    """
    cls = fixture_type or baseline.fixture_type
    floor = baseline.share_floor()
    alarm = Alarm(signal=SIGNAL_SHARE, fixture_type=cls, fired=False,
                  threshold=None if floor is None else round(floor, 4),
                  detail=f"trailing-{SHARE_WINDOW_DAYS}d share below "
                         f"{SHARE_DROP_RATIO:.0%} of frozen "
                         f"{baseline.share if baseline.share is not None else 'n/a'}")
    if floor is None:
        alarm.detail = "no frozen share recorded — share signal unavailable"
        return alarm
    ordered = sorted(attributed, key=lambda e: str(e.get("start_ts")))
    streak = 0
    for i, e in enumerate(ordered):
        try:
            end = datetime.fromisoformat(
                str(e["start_ts"]).replace("Z", "+00:00"))
        except (ValueError, KeyError):
            continue
        window = []
        for other in ordered[:i + 1]:
            try:
                t = datetime.fromisoformat(
                    str(other["start_ts"]).replace("Z", "+00:00"))
            except (ValueError, KeyError):
                continue
            if 0 <= (end - t).total_seconds() <= SHARE_WINDOW_DAYS * 86400:
                window.append(other)
        if len(window) < SHARE_MIN_EVENTS:
            continue
        share = sum(1 for w in window if w.get("cls") == cls) / len(window)
        alarm.series.append((str(e["start_ts"]), round(share, 4)))
        streak = streak + 1 if share < floor else 0
        if streak >= SUSTAIN and not alarm.fired:
            alarm.fired = True
            alarm.events_to_fire = i + 1
            alarm.fired_at = str(e["start_ts"])
            alarm.observed = round(share, 4)
    if not alarm.fired and alarm.series:
        alarm.observed = alarm.series[-1][1]
    return alarm


def share_alarm_is_live(baseline: FrozenBaseline) -> bool:
    """Can this share alarm fire AT ALL?

    Exists because of a real bug class: the previous median − 3·MAD formulation
    produced a NEGATIVE floor on this home, so the detector was structurally
    incapable of firing and its permanent silence was indistinguishable from
    good health. Any proportion-valued signal added later gets the same check.
    """
    floor = baseline.share_floor()
    return floor is not None and 0.0 < floor <= 1.0


# ── latency, quoted honestly ────────────────────────────────────────────────
def expected_latency_days(events_per_day: float,
                          events_needed: int = ROLLING_WINDOW + SUSTAIN
                          ) -> Optional[float]:
    """How long this fixture needs to produce enough events to alarm.

    A guest bath at 0.3 events/day needs ~2 months to fill the same window a
    main toilet fills in under a fortnight. Surfacing that is what stops a quiet
    fixture's silence being read as health.
    """
    if events_per_day <= 0:
        return None
    return round(events_needed / events_per_day, 1)


def observed_rate(events: Sequence[dict]) -> float:
    """Events per day over the span actually covered."""
    stamps = sorted(str(e.get("start_ts")) for e in events if e.get("start_ts"))
    if len(stamps) < 2:
        return 0.0
    try:
        a = datetime.fromisoformat(stamps[0].replace("Z", "+00:00"))
        b = datetime.fromisoformat(stamps[-1].replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    days = (b - a).total_seconds() / 86400.0
    return round(len(stamps) / days, 3) if days > 0 else 0.0


# ── persistence ─────────────────────────────────────────────────────────────
def save_baseline(conn: sqlite3.Connection, baseline: FrozenBaseline) -> None:
    conn.execute(
        "INSERT INTO fixture_baseline (circuit, fixture_type, baseline_hash, "
        " pinned_at, window_start, window_end, n_events, stats_json, locked) "
        "VALUES (?,?,?,?,?,?,?,?,1) "
        "ON CONFLICT(circuit, fixture_type) DO UPDATE SET "
        " baseline_hash=excluded.baseline_hash, pinned_at=excluded.pinned_at, "
        " window_start=excluded.window_start, window_end=excluded.window_end, "
        " n_events=excluded.n_events, stats_json=excluded.stats_json, "
        " locked=1, unlocked_reason=NULL, unlocked_at=NULL",
        (baseline.circuit, baseline.fixture_type, baseline.baseline_hash,
         baseline.pinned_at, baseline.window_start, baseline.window_end,
         baseline.n_events, json.dumps(baseline.to_json())))


def load_baseline(conn: sqlite3.Connection, circuit: str,
                  fixture_type: str) -> Optional[FrozenBaseline]:
    row = conn.execute(
        "SELECT stats_json FROM fixture_baseline "
        "WHERE circuit = ? AND fixture_type = ?",
        (circuit, fixture_type)).fetchone()
    if not row or not row[0]:
        return None
    try:
        return FrozenBaseline.from_json(json.loads(row[0]))
    except (ValueError, TypeError) as exc:
        log.warning("baseline for %s/%s unreadable: %s", circuit,
                    fixture_type, exc)
        return None


def unlock_baseline(conn: sqlite3.Connection, circuit: str, fixture_type: str,
                    reason: str) -> bool:
    """Release a baseline so it can be re-pinned (47i-2).

    Only by explicit confirmation, and only with a reason code — the code is
    what separates the three kinds of drift: infrastructure (adapt), fixture
    replacement (re-baseline), fixture failure (never adapt, alert).
    """
    if reason not in UNLOCK_REASONS:
        raise ValueError(f"unknown unlock reason {reason!r}; "
                         f"expected one of {sorted(UNLOCK_REASONS)}")
    cur = conn.execute(
        "UPDATE fixture_baseline SET locked = 0, unlocked_reason = ?, "
        " unlocked_at = ? WHERE circuit = ? AND fixture_type = ?",
        (reason, datetime.now(timezone.utc).isoformat(), circuit, fixture_type))
    return bool(cur.rowcount)


def record_nightly_stats(conn: sqlite3.Connection, circuit: str,
                         fixture_type: str, as_of_day: str,
                         stats: dict) -> None:
    """Append one day's observed statistics (the nightly job's output).

    Append-only: the series is the evidence a health card is built from, and
    rewriting history here would make an alarm unexplainable after the fact.
    """
    conn.execute(
        "INSERT INTO fixture_health_stat (circuit, fixture_type, as_of_day, "
        " stats_json) VALUES (?,?,?,?) "
        "ON CONFLICT(circuit, fixture_type, as_of_day) DO UPDATE SET "
        " stats_json = excluded.stats_json",
        (circuit, fixture_type, as_of_day, json.dumps(stats)))


def open_alert(conn: sqlite3.Connection, circuit: str, fixture_type: str,
               signal: str, detail: dict) -> Optional[int]:
    """Open an alert unless one for this (fixture, signal) is already open."""
    existing = conn.execute(
        "SELECT id FROM fixture_health_alert WHERE circuit = ? "
        "AND fixture_type = ? AND signal = ? AND resolved_at IS NULL",
        (circuit, fixture_type, signal)).fetchone()
    if existing:
        return int(existing[0])
    cur = conn.execute(
        "INSERT INTO fixture_health_alert (circuit, fixture_type, signal, "
        " opened_at, detail_json) VALUES (?,?,?,?,?)",
        (circuit, fixture_type, signal,
         datetime.now(timezone.utc).isoformat(), json.dumps(detail)))
    return int(cur.lastrowid) if cur.lastrowid else None


def resolve_alert(conn: sqlite3.Connection, alert_id: int,
                  resolution: str = "") -> bool:
    cur = conn.execute(
        "UPDATE fixture_health_alert SET resolved_at = ?, resolution = ? "
        "WHERE id = ? AND resolved_at IS NULL",
        (datetime.now(timezone.utc).isoformat(), resolution, alert_id))
    return bool(cur.rowcount)


def open_alerts(conn: sqlite3.Connection, circuit: str) -> List[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT id, fixture_type, signal, opened_at, detail_json "
        "FROM fixture_health_alert WHERE circuit = ? AND resolved_at IS NULL "
        "ORDER BY opened_at", (circuit,))]


def alerting_classes(conn: sqlite3.Connection, circuit: str) -> set:
    """Fixture classes with an open alert.

    The retrain reads this: while a class is alerting, its recent events are
    excluded from the referee's holdout (hygiene) and can be quarantined from
    the pool during post-repair cleanup. Note this is NOT the detection
    mechanism — detection already happened, downstream, against the frozen
    baseline. See the plan's rev-4 demotion of quarantine.
    """
    return {r["fixture_type"] for r in open_alerts(conn, circuit)}
