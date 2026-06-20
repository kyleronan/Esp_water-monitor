"""Detector self-validation against HA history (P6) — DIAGNOSTIC ONLY.

Reconciles the detected events against the raw flow the addon can re-scrape from Home
Assistant, to confirm the phantom / cross-talk / dribble settings are behaving well —
WITHOUT ever auto-tuning a threshold. Auto-tuning from a self-check would reintroduce the
online-adaptation boiling-frog risk that the freeze-at-activation design exists to avoid; this
module only produces a per-detector health report and a human decides whether to act.

``validate_detectors_against_history`` is PURE / sync: it takes already-fetched, UTC-normalised
flow series, so it is unit-testable offline and is a direct port of the manual audit. The async
HA fetch + the high-fidelity-retention clamp live in the caller (``training_manager``).

The single most important output is ``suspect_zeroings`` — a volume-zeroed event (phantom /
cross-talk) that actually moved real water. That is the one outcome that must never happen, so it
is the continuous, week-over-week proof on real data that the leak-safety invariant still holds.
Its window integration is deliberately exact (UTC-aligned, strictly in-window, with an edge guard).
"""
from __future__ import annotations

import bisect
import json
import logging
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

log = logging.getLogger(__name__)

# HA's recorder keeps FULL-resolution sensor history only this long (``purge_keep_days``,
# default 10); older data is rolled into 5-min / hourly averages that destroy the burst
# structure the volume integral needs. The caller clamps the scrape window to this.
HA_HIGH_FIDELITY_DAYS = 10

# Day-7 interim advisory (a NEW learning period): the moment a user can still label before
# the ~day-14 freeze locks calibration.
INTERIM_VALIDATION_DAY = 7

MIN_FLOW_LPM = 0.15            # flow noise floor (matches event_detector.MIN_FLOW_LPM)

# Each raw sample's flow holds until the next sample, capped here so a sensor dropout / purge
# gap with a non-zero last value is not integrated as continuous flow. Active flow samples are
# < 1 s apart, so 30 s never clips real flow — it only bounds the effect of a gap.
MAX_SAMPLE_HOLD_S = 30.0

# Volume-coverage WARNING is a deliberately-LOOSE floor, NOT this home's in-sample number — the
# diagnostic value is the coverage figure a human reads. Warn only when coverage drops well below
# normal per-home variation AND a non-trivial absolute volume is unaccounted for.
COVERAGE_WARN_BELOW = 0.95
UNCOVERED_WARN_LITRES = 5.0

# A volume-zeroed event that moved at least this much real water = leak-safety red flag.
SUSPECT_ZERO_LITRES = 1.0
# Flow within this band of an event edge is attributed to a neighbouring draw, not the event.
EDGE_GUARD_S = 2.0


# ── time + integration helpers ──────────────────────────────────────────────────

def _to_utc(ts: Any) -> Optional[datetime]:
    """Parse an ISO timestamp (or datetime) to an aware UTC datetime. Naive ⇒ assumed UTC."""
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return d.astimezone(timezone.utc) if d.tzinfo else d.replace(tzinfo=timezone.utc)


def normalize_series(series: Sequence[Tuple[Any, Any]]) -> List[Tuple[datetime, float]]:
    """``[(ts, flow_lpm)]`` → sorted ``[(utc_dt, flow>=0)]`` with bad rows dropped."""
    out: List[Tuple[datetime, float]] = []
    for ts, f in series or []:
        d = _to_utc(ts)
        if d is None:
            continue
        try:
            fv = float(f)
        except (TypeError, ValueError):
            continue
        if math.isfinite(fv):
            out.append((d, max(0.0, fv)))
    out.sort(key=lambda x: x[0])
    return out


def _integrate(series: List[Tuple[datetime, float]], times: List[datetime],
               t0: datetime, t1: datetime,
               max_hold_s: float = MAX_SAMPLE_HOLD_S) -> float:
    """Rectangular (state-holds) integral of a flow series (L/min) over ``[t0, t1]`` → litres.

    Each sample's flow holds until the next sample, capped at ``max_hold_s`` (gap-cap), and the
    contribution is clipped to ``[t0, t1]``. ``times`` is the parallel list of sample datetimes
    (for bisect). O(samples in window)."""
    if not series or t1 <= t0:
        return 0.0
    n = len(series)
    i = bisect.bisect_right(times, t0) - 1     # the sample whose hold may cover t0
    if i < 0:
        i = 0
    total = 0.0
    while i < n:
        ts, f = series[i]
        if ts >= t1:
            break
        nxt = times[i + 1] if i + 1 < n else ts + timedelta(seconds=max_hold_s)
        hold_end = min(nxt, ts + timedelta(seconds=max_hold_s))
        a = ts if ts > t0 else t0
        b = hold_end if hold_end < t1 else t1
        if b > a:
            total += f * (b - a).total_seconds() / 60.0
        i += 1
    return total


def _merge_intervals(intervals: List[Tuple[datetime, datetime]]
                     ) -> List[Tuple[datetime, datetime]]:
    """Merge overlapping/adjacent ``[start, end]`` intervals."""
    clean = sorted((a, b) for a, b in intervals if a and b and b > a)
    merged: List[Tuple[datetime, datetime]] = []
    for a, b in clean:
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    return merged


# ── event loading ───────────────────────────────────────────────────────────────

def _load_events(conn: sqlite3.Connection, circuit: str,
                 t0: datetime, t1: datetime) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        "SELECT id, start_ts, end_ts, duration_seconds, "
        "  COALESCE(volume_litres_effective, volume_litres, 0) AS veff, "
        "  COALESCE(is_pressure_restoration_phantom,0) AS ph, "
        "  COALESCE(is_cross_talk,0) AS ct, "
        "  COALESCE(is_low_flow_dribble,0) AS dr, "
        "  COALESCE(degraded_supply,0) AS dg, "
        "  flow_integral_litres, flow_on_ratio, pressure_delta_psi, "
        "  COALESCE(user_classified,0) AS uc, user_fixture_type AS uft "
        "FROM events WHERE circuit = ? AND start_ts >= ? AND start_ts < ? "
        "ORDER BY start_ts",
        (circuit, t0.isoformat(), t1.isoformat()),
    ).fetchall()


# ── the validation ──────────────────────────────────────────────────────────────

def validate_detectors_against_history(
        conn: sqlite3.Connection,
        circuit: str,
        flow_histories: Dict[str, Sequence[Tuple[Any, Any]]],
        window: Tuple[Any, Any]) -> Dict[str, Any]:
    """Reconcile detected events against raw HA flow. Pure / read-only — returns a report dict.

    ``flow_histories``: ``{"main": [(ts, lpm), ...], "irrigation": [(ts, lpm), ...]}`` — already
    fetched and (ideally) clamped to <= HA_HIGH_FIDELITY_DAYS by the caller. ``window``: the
    ``(start, end)`` actually validated. No threshold is ever written; this is diagnostic only.
    """
    t0, t1 = _to_utc(window[0]), _to_utc(window[1])
    main = normalize_series(flow_histories.get("main", []))
    irrig = normalize_series(flow_histories.get("irrigation", []))
    main_t = [s[0] for s in main]
    irrig_t = [s[0] for s in irrig]

    report: Dict[str, Any] = {
        "window": {"start": t0.isoformat() if t0 else None,
                   "end": t1.isoformat() if t1 else None,
                   "days": round((t1 - t0).total_seconds() / 86400.0, 2) if (t0 and t1) else None},
        "detectors": {}, "suspect_zeroings": [],
    }
    if not (t0 and t1 and t1 > t0):
        report["error"] = "invalid_window"
        return report
    if not main:
        report["error"] = "no_main_flow_history"
        return report

    events = _load_events(conn, circuit, t0, t1)

    # ── 1. Volume reconciliation ────────────────────────────────────────────────
    # Coverage = the fraction of RAW main-flow volume that falls inside a detected event
    # window (covered / raw). This is the robust, leak-relevant metric ("did we detect the
    # water that flowed") and is invariant to per-event volume accounting / zeroing. The
    # events' own effective volume is reported alongside it for context, but is NOT the
    # coverage ratio (the ESP integral and the HA-recorded flow sensor can differ).
    raw_volume = _integrate(main, main_t, t0, t1)
    events_effective = sum(float(e["veff"] or 0.0) for e in events)
    ev_intervals = [(_to_utc(e["start_ts"]), _to_utc(e["end_ts"])) for e in events]
    covered = sum(_integrate(main, main_t, a, b)
                  for a, b in _merge_intervals(ev_intervals))
    raw_uncovered = max(0.0, raw_volume - covered)
    coverage = (covered / raw_volume) if raw_volume > 1e-9 else None

    cov_warn = (coverage is not None and coverage < COVERAGE_WARN_BELOW
                and raw_uncovered > UNCOVERED_WARN_LITRES)
    report["volume"] = {
        "raw_litres": round(raw_volume, 2),
        "covered_litres": round(covered, 2),
        "events_effective_litres": round(events_effective, 2),
        "coverage": round(coverage, 4) if coverage is not None else None,
        "raw_uncovered_litres": round(raw_uncovered, 2),
        "status": "warn" if cov_warn else "ok",
        "summary": (f"{coverage*100:.1f}% of raw main-flow volume is inside an event window"
                    if coverage is not None else "no raw volume in window"),
    }

    # ── 2. Suspect zeroings — the leak-safety regression guard (EXACT window) ────
    # A volume-zeroed event (phantom / cross-talk / dribble — all three now zero the
    # event's volume) that actually moved real water. Split by provenance:
    #   • AUTO  (user_classified=0): the DETECTOR zeroed real water → a genuine leak-safety
    #     red flag, and what drives the per-detector WARN — this is the continuous proof the
    #     auto-detector still honours the invariant.
    #   • USER  (user_classified=1): the user authoritatively marked it (e.g. they know the
    #     "flow" is a paddlewheel misread / cross-talk) → informational only, never a WARN.
    suspects: List[Dict[str, Any]] = []
    user_zeroed: List[Dict[str, Any]] = []
    zeroed = [e for e in events
              if (e["ph"] or e["ct"] or e["dr"]) and float(e["veff"] or 0.0) == 0.0]
    for e in zeroed:
        a, b = _to_utc(e["start_ts"]), _to_utc(e["end_ts"])
        if not (a and b and b > a):
            continue
        guard = timedelta(seconds=EDGE_GUARD_S)
        # Strictly in-window (inset by the edge guard so a neighbour's draw touching the edge
        # is never attributed to this event); edge-band flow reported separately.
        inner_a = a + guard if (b - a) > 2 * guard else a
        inner_b = b - guard if (b - a) > 2 * guard else b
        raw_in = _integrate(main, main_t, inner_a, inner_b)
        edge = (_integrate(main, main_t, a - guard, inner_a)
                + _integrate(main, main_t, inner_b, b + guard))
        if raw_in >= SUSPECT_ZERO_LITRES:
            entry = {
                "id": e["id"],
                "kind": ("phantom" if e["ph"] else "cross_talk" if e["ct"]
                         else "dribble"),
                "raw_litres": round(raw_in, 3), "edge_litres": round(edge, 3),
                "start_ts": e["start_ts"],
            }
            (user_zeroed if e["uc"] else suspects).append(entry)
    report["suspect_zeroings"] = suspects                # AUTO — leak-safety
    report["user_zeroed_with_flow"] = user_zeroed        # informational

    auto_zeroed = [e for e in zeroed if not e["uc"]]
    ph_n = sum(1 for e in auto_zeroed if e["ph"])
    ct_n = sum(1 for e in auto_zeroed if e["ct"])
    dr_n = sum(1 for e in auto_zeroed if e["dr"])
    ph_bad = sum(1 for s in suspects if s["kind"] == "phantom")
    ct_bad = sum(1 for s in suspects if s["kind"] == "cross_talk")
    dr_bad = sum(1 for s in suspects if s["kind"] == "dribble")
    report["detectors"]["phantom"] = {
        "status": "warn" if ph_bad else "ok", "zeroed": ph_n, "suspect": ph_bad,
        "summary": (f"{ph_bad} of {ph_n} zeroed phantom(s) actually moved >= "
                    f"{SUSPECT_ZERO_LITRES:g} L of raw water" if ph_bad
                    else f"{ph_n} zeroed phantom(s), 0 suspect"),
    }

    # ── 3. Cross-talk corroboration vs raw irrigation draws ─────────────────────
    irr_on = _on_windows(irrig, irrig_t) if irrig else []
    ct_events = [e for e in events if e["ct"]]
    ct_overlap = sum(1 for e in ct_events
                     if _overlaps_any(_to_utc(e["start_ts"]), _to_utc(e["end_ts"]), irr_on))
    frac = (ct_overlap / len(ct_events)) if ct_events else None
    report["detectors"]["cross_talk"] = {
        "status": "warn" if ct_bad else "ok",
        "zeroed": ct_n, "suspect": ct_bad,
        "overlap_irrigation": ct_overlap, "total": len(ct_events),
        "overlap_fraction": round(frac, 3) if frac is not None else None,
        "summary": (f"{ct_bad} of {ct_n} zeroed cross-talk(s) moved real water" if ct_bad
                    else (f"{ct_overlap}/{len(ct_events)} cross-talk events coincide with a "
                          f"raw irrigation draw" if ct_events else "no cross-talk events")),
    }

    # ── 4. Dribble — now a VOLUME-ZEROING verdict (it removes the event's volume
    #       from totals), so it is held to the SAME leak-safety bar as phantom /
    #       cross-talk via the suspect-zeroing pass above (a zeroed dribble that moved
    #       >= SUSPECT_ZERO_LITRES of real water is a red flag and a WARN), not the old
    #       low-stakes 2 L sanity note. ────────────────────────────────────────────
    report["detectors"]["dribble"] = {
        "status": "warn" if dr_bad else "ok", "zeroed": dr_n, "suspect": dr_bad,
        "summary": (f"{dr_bad} of {dr_n} zeroed dribble(s) actually moved >= "
                    f"{SUSPECT_ZERO_LITRES:g} L of raw water" if dr_bad
                    else f"{dr_n} zeroed dribble(s), 0 suspect"),
    }

    # ── 5. Unflagged-no-flow sentinel (the gap-class scan, run continuously) ─────
    # Carry per-event detail (not just IDs) so the Settings panel can list the
    # specific events for the user to open in History and relabel.
    sentinel = [
        {"id": e["id"], "start_ts": e["start_ts"],
         "duration_s": e["duration_seconds"],
         "delta_psi": e["pressure_delta_psi"],
         "integral_l": e["flow_integral_litres"]}
        for e in events
        if not (e["ph"] or e["ct"] or e["dr"] or e["dg"] or e["uc"] or e["uft"])
        and (e["flow_integral_litres"] is not None and e["flow_on_ratio"] is not None)
        and float(e["flow_integral_litres"]) < 1.0 and float(e["flow_on_ratio"]) < 0.05
        and float(e["duration_seconds"] or 0) >= 120
    ]
    report["unflagged_no_flow"] = {"count": len(sentinel), "events": sentinel[:50]}

    # ── overall verdict ─────────────────────────────────────────────────────────
    # `suspects` now includes dribble suspects (dribble zeroes volume), so it drives
    # the dribble WARN too — no separate low-stakes signal.
    report["overall"] = (
        "warn" if (suspects or cov_warn or sentinel) else "ok")
    return report


def _on_windows(series: List[Tuple[datetime, float]], times: List[datetime]
                ) -> List[Tuple[datetime, datetime]]:
    """Contiguous runs where flow >= MIN_FLOW_LPM → merged ``[start, end]`` intervals."""
    raw: List[Tuple[datetime, datetime]] = []
    start = None
    n = len(series)
    for i, (ts, f) in enumerate(series):
        if f >= MIN_FLOW_LPM:
            if start is None:
                start = ts
        else:
            if start is not None:
                raw.append((start, ts))
                start = None
    if start is not None:
        raw.append((start, times[-1]))
    return _merge_intervals(raw)


def _overlaps_any(a: Optional[datetime], b: Optional[datetime],
                  intervals: List[Tuple[datetime, datetime]]) -> bool:
    if not (a and b):
        return False
    for cs, ce in intervals:
        if not (b < cs or a > ce):
            return True
    return False


# ── persistence (single latest report per circuit) ───────────────────────────────

def persist_validation_report(conn: sqlite3.Connection, circuit: str,
                              report: Dict[str, Any]) -> None:
    """Store the latest report for a circuit — a SINGLE slot, upserted (never
    append-only), so the on-demand dev endpoint can't grow a table."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS detector_validation ("
        "  circuit TEXT PRIMARY KEY, status TEXT, report TEXT, validated_at TEXT)")
    conn.execute(
        "INSERT INTO detector_validation (circuit, status, report, validated_at) "
        "VALUES (?,?,?,?) ON CONFLICT(circuit) DO UPDATE SET "
        "  status = excluded.status, report = excluded.report, "
        "  validated_at = excluded.validated_at",
        (circuit, report.get("overall", "ok"), json.dumps(report),
         report.get("validated_at")),
    )
    conn.commit()


def _label_counts(conn: sqlite3.Connection, circuit: str) -> Dict[str, int]:
    """Per-detector count of user-CONFIRMED examples (the calibration fit input)."""
    row = conn.execute(
        "SELECT "
        "  SUM(CASE WHEN COALESCE(is_pressure_restoration_phantom,0)=1 THEN 1 ELSE 0 END) ph, "
        "  SUM(CASE WHEN COALESCE(is_cross_talk,0)=1 THEN 1 ELSE 0 END) ct, "
        "  SUM(CASE WHEN COALESCE(is_low_flow_dribble,0)=1 THEN 1 ELSE 0 END) dr "
        "FROM events WHERE circuit = ? AND COALESCE(user_classified,0)=1",
        (circuit,)).fetchone()
    return {"phantom": int((row[0] or 0)), "cross_talk": int((row[1] or 0)),
            "dribble": int((row[2] or 0))}


def build_interim_advisory(conn: sqlite3.Connection, circuit: str,
                           report: Dict[str, Any]) -> Tuple[str, str]:
    """Compose the day-7 advisory (title, message). It SPLITS by actionability: at day 7 the
    detectors still run on DEFAULTS (the fit freezes ~day 14), so per-event labelling /
    readiness is what the user can act on now, while aggregate threshold behaviour is
    informational (labelling won't move it until the freeze). Pure — testable offline.
    """
    from .artifact_calibration import MIN_POSITIVES
    counts = _label_counts(conn, circuit)
    actionable: List[str] = []
    for key, name in (("phantom", "Phantom"), ("cross_talk", "Cross-talk"),
                      ("dribble", "Dribble")):
        n = counts[key]
        if n < MIN_POSITIVES:
            actionable.append(f"- {name}: {n}/{MIN_POSITIVES} labelled — label more "
                              f"before activation, or it will use safe defaults.")
    sentinel = (report.get("unflagged_no_flow") or {}).get("count", 0)
    if sentinel:
        actionable.append(f"- {sentinel} event(s) look like artifacts — consider "
                          f"classifying them.")
    user_zeroed = report.get("user_zeroed_with_flow") or []
    if user_zeroed:
        actionable.append(f"- {len(user_zeroed)} event(s) you marked as no-water actually "
                          f"show flow — worth a second look.")
    if not actionable:
        actionable.append("- Labels look on track — nothing needed right now.")

    vol = report.get("volume") or {}
    cov = vol.get("coverage")
    info = (f"Detector behaviour so far (informational, re-checked at activation): "
            f"{cov*100:.0f}% of measured water is captured by events."
            if cov is not None else "")

    title = "Water Monitor — learning halfway: a quick label check"
    message = ("You're about halfway through the learning period. A few things you can do "
               "now to improve detection before it locks in:\n"
               + "\n".join(actionable)
               + (("\n\n" + info) if info else ""))
    return title, message


# ── day-7 "fire once" marker (per learning period) ───────────────────────────────

def interim_already_sent(conn: sqlite3.Connection, circuit: str,
                         started_at: str) -> bool:
    """True if the interim advisory was already sent for THIS learning period
    (keyed by the period's started_at, so a new learning period re-arms it)."""
    try:
        row = conn.execute(
            "SELECT sent_for FROM detector_validation_interim WHERE circuit = ?",
            (circuit,)).fetchone()
    except sqlite3.OperationalError:
        return False
    return bool(row and row[0] == started_at)


def mark_interim_sent(conn: sqlite3.Connection, circuit: str, started_at: str) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS detector_validation_interim ("
        "  circuit TEXT PRIMARY KEY, sent_for TEXT)")
    conn.execute(
        "INSERT INTO detector_validation_interim (circuit, sent_for) VALUES (?,?) "
        "ON CONFLICT(circuit) DO UPDATE SET sent_for = excluded.sent_for",
        (circuit, started_at))
    conn.commit()


def load_validation_report(conn: sqlite3.Connection,
                           circuit: str) -> Optional[Dict[str, Any]]:
    """Return the latest stored report for a circuit, or None."""
    try:
        row = conn.execute(
            "SELECT report FROM detector_validation WHERE circuit = ?",
            (circuit,)).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0])
    except (ValueError, TypeError):
        return None


# ── async orchestration (fetch + clamp + validate + persist) ──────────────────────

async def run_detector_validation(db: sqlite3.Connection, ha: Any, cfg: Any,
                                   circuit: str, now_utc: datetime, *,
                                   source: str = "freeze",
                                   persist: bool = True) -> Dict[str, Any]:
    """Fetch the high-fidelity flow history from HA (clamped to <= HA_HIGH_FIDELITY_DAYS,
    because older recorder data is rolled into averages that ruin the volume integral),
    validate, and persist a single latest-per-circuit report. Best-effort and
    DIAGNOSTIC ONLY — never writes a threshold. Returns the report (or {"error": ...})."""
    circuit_cfg = cfg.get_circuit(circuit) if cfg else None
    if circuit_cfg is None or not circuit_cfg.flow_sensor:
        return {"error": "no_flow_sensor", "source": source}

    end = now_utc
    start = end - timedelta(days=HA_HIGH_FIDELITY_DAYS)
    main_entity = circuit_cfg.flow_sensor
    # Sibling ZONE (irrigation) circuits' flow → the cross-talk corroboration signal.
    irrig_entities = [c.flow_sensor for c in (cfg.circuits or [])
                      if c.circuit != circuit and getattr(c, "is_zone_circuit", False)
                      and c.flow_sensor]
    entities = [main_entity] + irrig_entities
    try:
        hist = await ha.get_history_batch(entities, start, end)
    except Exception as e:
        log.warning("[%s] detector-validation history fetch failed: %s", circuit, e)
        return {"error": "history_fetch_failed", "source": source}

    def _series(eid):
        return [(h.get("last_changed"), h.get("state")) for h in (hist.get(eid) or [])]

    irrig: List[Tuple[Any, Any]] = []
    for e in irrig_entities:
        irrig.extend(_series(e))
    flow_histories = {"main": _series(main_entity), "irrigation": irrig}

    report = validate_detectors_against_history(db, circuit, flow_histories, (start, end))
    report["source"] = source
    report["validated_at"] = now_utc.isoformat()
    if persist:
        try:
            persist_validation_report(db, circuit, report)
        except Exception as e:
            log.warning("[%s] detector-validation persist failed: %s", circuit, e)
    log.info("[%s] detector self-validation (%s): overall=%s, %d suspect zeroing(s), "
             "%d unflagged no-flow", circuit, source, report.get("overall"),
             len(report.get("suspect_zeroings", [])),
             (report.get("unflagged_no_flow") or {}).get("count", 0))
    return report
