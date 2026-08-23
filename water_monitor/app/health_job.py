"""dev47 (47i) — the nightly fixture-health pass.

This is the job that makes the frozen baselines actually watch something. Once
a night it reads the CLASSIFIED event stream, compares each fixture against its
own locked reference, appends the day's observations, and opens or resolves
alerts.

WHAT IT READS, AND WHY THAT EXACT THING
---------------------------------------
The input is ``COALESCE(user_fixture_type, matched_fixture_type)`` — the
attributed stream, the add-on's own answer about what ran. Deliberately not
ground truth (there isn't any, most events are never labelled) and deliberately
not filtered by confidence (a low-confidence toilet is still the system's best
account of that water). Detection sits DOWNSTREAM of attribution: the model may
absorb a drifting fixture and that is fine, because an absorbed event stays in
its own class, on its own trend line, where the frozen baseline still sees it.

THE ONE THING THIS JOB MUST NEVER DO
------------------------------------
Update a baseline from current data. A reference that follows the data detects
nothing — a failing fixture would simply redefine normal, which is the entire
failure mode dev47 exists to catch. Baselines are pinned once from an explicit
window and move only by an operator unlock with a reason code. This job creates
one when none exists and otherwise treats it as read-only.

COVERAGE IS PART OF THE READING
-------------------------------
"No alarm" is only good news if events were actually arriving. V6d measured a
frozen classifier starving its own trend line — the drifted events were
rejected, never reached the card, and the detector went quiet while the fixture
got worse. So every result carries the event count and the fixture's expected
latency, and a class whose stream has dried up is reported as UNDER-COVERED
rather than healthy.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

from . import fixture_health as fh

log = logging.getLogger(__name__)

# Classes worth watching first: the anchor-backed four, where the cycle
# detectors keep the stream well-populated and a baseline means something.
DEFAULT_WATCHED: tuple = ("toilet", "dishwasher", "washing_machine",
                          "water_softener")

# A baseline needs a window that is CLOSED — recent events must not define the
# reference they will later be judged against.
BASELINE_WINDOW_DAYS: int = 30
BASELINE_MIN_AGE_DAYS: int = 14

# Below this share of its expected event count, a class is under-covered and
# its silence is not evidence of health. Measured over a TRAILING window, not
# the whole post-baseline history: a fixture that ran normally for months and
# went quiet last month has a perfectly healthy average and a real problem.
COVERAGE_WARN_RATIO: float = 0.4
COVERAGE_WINDOW_DAYS: int = 30

# The class-share signal is OFF by default on the nightly pass, and that is a
# measured decision rather than caution. In simulation (V6d) share-sag was a
# clean signal because classification coverage was held fixed. On the real
# database it is not: the share of the attributed stream held by every class
# moves as the backlog gets classified and as new tiers come online, so a
# nightly run fired share alarms for toilet, dishwasher AND washing-machine at
# once — three fixtures do not fail on the same night. Share only means
# something when the denominator is stable, so it stays available for the
# harness (where coverage IS controlled) and is opt-in here.
SHARE_SIGNAL_DEFAULT: bool = False

# The unsolicited-refill channel is OFF by default for a different and more
# fundamental reason: on this meter its premise does not hold. It assumes a
# cistern refill is a SEPARATE measurable draw from the flush that caused it.
# Measured on the labelled archive, 157 of 173 toilet events are a single
# active flow segment — the flush and its refill are one draw of ~5.7 L over
# ~50 s. There is no separate refill event to find, so the rule instead flags
# ordinary small household draws: enabled, it reported 245 "unsolicited
# refills" across every class at 4/day.
#
# The simulation that promoted this to the "fast signal" (V6d, alarm on day 3)
# injected phantom refills as distinct events, which is the plumbing model this
# meter does not observe. A degrading flapper still shows up here — as the
# flush event itself growing — and that is exactly what the volume trend reads.
# Re-enable only against a meter (or a fixture) where refills are separately
# metered, or with a formulation that keys on repetition during quiet periods
# rather than on the absence of a preceding draw.
UNSOLICITED_SIGNAL_DEFAULT: bool = False


@dataclass
class FixtureResult:
    fixture_type: str
    n_events: int
    baseline_hash: Optional[str] = None
    alarms: List[fh.Alarm] = field(default_factory=list)
    opened: List[int] = field(default_factory=list)
    under_covered: bool = False
    expected_latency_days: Optional[float] = None
    events_per_day: float = 0.0
    note: str = ""

    def as_dict(self) -> dict:
        return {"fixture_type": self.fixture_type, "n_events": self.n_events,
                "baseline_hash": self.baseline_hash,
                "alarms": [a.as_dict() for a in self.alarms if a.fired],
                "opened_alert_ids": self.opened,
                "under_covered": self.under_covered,
                "events_per_day": self.events_per_day,
                "expected_latency_days": self.expected_latency_days,
                "note": self.note}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_attributed_stream(conn: sqlite3.Connection, circuit: str,
                           since: Optional[str] = None) -> List[dict]:
    """The classified stream: the add-on's own account of what ran.

    User labels take precedence over machine verdicts (a person who looked is
    better evidence than a model that guessed), but an unlabelled machine
    verdict counts — otherwise the trend line would consist only of events
    someone happened to review, which is a biased sample of exactly the events
    that looked odd.
    """
    where = ("WHERE circuit = ? "
             "AND COALESCE(is_pressure_restoration_phantom,0) = 0 "
             "AND COALESCE(is_low_flow_dribble,0) = 0 "
             "AND COALESCE(is_cross_talk,0) = 0")
    params: list = [circuit]
    if since:
        where += " AND start_ts >= ?"
        params.append(since)
    rows = []
    for r in conn.execute(
            "SELECT id, start_ts, volume_litres, duration_seconds, "
            "       peak_flow_lpm, "
            "       COALESCE(NULLIF(user_fixture_type,''), "
            "                NULLIF(matched_fixture_type,'')) AS cls "
            f"FROM events {where} ORDER BY start_ts", params):
        d = dict(r)
        if not d.get("cls"):
            continue
        d["cls"] = str(d["cls"]).strip().lower()
        rows.append(d)
    return rows


def ensure_baseline(conn: sqlite3.Connection, circuit: str, fixture_type: str,
                    stream: Sequence[dict],
                    window_days: int = BASELINE_WINDOW_DAYS,
                    min_age_days: int = BASELINE_MIN_AGE_DAYS
                    ) -> Optional[fh.FrozenBaseline]:
    """Return the locked baseline, pinning one from history if none exists.

    The window is the EARLIEST ``window_days`` of this class's history, and it
    must have closed at least ``min_age_days`` ago. Both conditions exist for
    the same reason: a reference pinned over data that might already be
    drifting is not a reference. Earliest-available is the best proxy for
    "before the problem started" that an unattended job can justify; an
    operator who knows better can re-pin explicitly.
    """
    existing = fh.load_baseline(conn, circuit, fixture_type)
    if existing is not None:
        return existing
    events = [e for e in stream if e["cls"] == fixture_type]
    if len(events) < fh.MIN_BASELINE_EVENTS:
        return None
    first = str(events[0]["start_ts"])
    try:
        start = datetime.fromisoformat(first.replace("Z", "+00:00"))
    except ValueError:
        return None
    end = start + timedelta(days=window_days)
    if (_utc_now() - end).days < min_age_days:
        return None                     # window has not closed long enough ago
    window = [e for e in events
              if str(e["start_ts"]) < end.isoformat()]
    if len(window) < fh.MIN_BASELINE_EVENTS:
        return None
    in_window_all = [e for e in stream if str(e["start_ts"]) < end.isoformat()
                     and str(e["start_ts"]) >= start.isoformat()]
    share = (len(window) / len(in_window_all)) if in_window_all else None
    baseline = fh.build_baseline(circuit, fixture_type, window,
                                 start.isoformat(), end.isoformat(),
                                 share=share)
    fh.save_baseline(conn, baseline)
    log.info("[%s] pinned %s health baseline %s over [%s, %s): median %.2f L, "
             "%d events", circuit, fixture_type, baseline.baseline_hash,
             start.date(), end.date(), baseline.volume_median, len(window))
    return baseline


def evaluate_fixture(conn: sqlite3.Connection, circuit: str, fixture_type: str,
                     stream: Sequence[dict], baseline: fh.FrozenBaseline,
                     enable_share: bool = SHARE_SIGNAL_DEFAULT,
                     enable_unsolicited: bool = UNSOLICITED_SIGNAL_DEFAULT
                     ) -> FixtureResult:
    """Run every enabled signal for one fixture against its frozen reference."""
    after = [e for e in stream if str(e["start_ts"]) >= baseline.window_end]
    mine = [e for e in after if e["cls"] == fixture_type]
    rate = fh.observed_rate(mine)

    # Coverage is judged on the TRAILING window only. The lifetime average
    # would hide exactly the case that matters: a stream that was healthy for
    # months and dried up recently.
    recent_cutoff = (_utc_now() - timedelta(days=COVERAGE_WINDOW_DAYS)).isoformat()
    recent = [e for e in mine if str(e["start_ts"]) >= recent_cutoff]
    recent_rate = round(len(recent) / COVERAGE_WINDOW_DAYS, 3)

    result = FixtureResult(
        fixture_type=fixture_type, n_events=len(mine),
        baseline_hash=baseline.baseline_hash, events_per_day=recent_rate,
        expected_latency_days=fh.expected_latency_days(recent_rate))

    baseline_rate = (baseline.n_events / max(BASELINE_WINDOW_DAYS, 1))
    if baseline_rate > 0 and recent_rate < baseline_rate * COVERAGE_WARN_RATIO:
        result.under_covered = True
        result.note = (f"stream thinned to {recent_rate:.2f}/day over the last "
                       f"{COVERAGE_WINDOW_DAYS} days, from a baseline "
                       f"{baseline_rate:.2f}/day — silence here is not "
                       "evidence of health")

    if baseline.is_usable():
        result.alarms.append(fh.volume_trend_alarm(baseline, mine))
        result.alarms.append(fh.duration_trend_alarm(baseline, mine))
    else:
        result.note = ((result.note + " | ") if result.note else "") + (
            f"baseline spread (MAD {baseline.volume_mad:.1f} L) exceeds its "
            f"centre ({baseline.volume_median:.1f} L) — this class does not "
            "have one signature, so its volume trend is not read")
    if enable_share and fh.share_alarm_is_live(baseline):
        result.alarms.append(fh.class_share_alarm(baseline, after, fixture_type))
    if enable_unsolicited and fixture_type == "toilet":
        refills = fh.find_unsolicited_refills(after, baseline)
        result.alarms.append(fh.unsolicited_refill_alarm(refills, fixture_type))

    for alarm in result.alarms:
        if not alarm.fired:
            continue
        alert_id = fh.open_alert(conn, circuit, fixture_type, alarm.signal,
                                 alarm.as_dict())
        if alert_id is not None:
            result.opened.append(alert_id)
            log.warning("[%s] fixture health: %s %s — observed %s vs "
                        "threshold %s after %s events", circuit, fixture_type,
                        alarm.signal, alarm.observed, alarm.threshold,
                        alarm.events_to_fire)
    return result


def run_nightly(conn: sqlite3.Connection, circuit: str,
                watched: Sequence[str] = DEFAULT_WATCHED,
                as_of_day: Optional[str] = None,
                enable_share: bool = SHARE_SIGNAL_DEFAULT,
                enable_unsolicited: bool = UNSOLICITED_SIGNAL_DEFAULT
                ) -> Dict[str, dict]:
    """One night's pass over every watched fixture on a circuit.

    Synchronous — the caller submits it through ``run_db`` (46a). Returns a
    per-fixture summary; the caller logs or surfaces it. Never raises for the
    ordinary "not enough history yet" cases, which are states, not errors.
    """
    day = as_of_day or _utc_now().strftime("%Y-%m-%d")
    stream = load_attributed_stream(conn, circuit)
    out: Dict[str, dict] = {}
    if not stream:
        return out
    for fixture_type in watched:
        baseline = ensure_baseline(conn, circuit, fixture_type, stream)
        if baseline is None:
            mine = [e for e in stream if e["cls"] == fixture_type]
            out[fixture_type] = FixtureResult(
                fixture_type=fixture_type, n_events=len(mine),
                note="no baseline yet — needs a closed window with "
                     f"{fh.MIN_BASELINE_EVENTS}+ events").as_dict()
            continue
        result = evaluate_fixture(conn, circuit, fixture_type, stream, baseline,
                                  enable_share=enable_share,
                                  enable_unsolicited=enable_unsolicited)
        fh.record_nightly_stats(conn, circuit, fixture_type, day, {
            "n_events": result.n_events,
            "events_per_day": result.events_per_day,
            "under_covered": result.under_covered,
            "baseline_hash": baseline.baseline_hash,
            "alarms": [a.signal for a in result.alarms if a.fired],
            "observed": {a.signal: a.observed for a in result.alarms},
        })
        out[fixture_type] = result.as_dict()
    return out
