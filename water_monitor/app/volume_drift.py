"""Whole-day volume drift — dev46 (46v), roadmap Phase 3 §2.

NOT the same thing as ``recorder_reconcile.py``, and the difference is the
entire point of this module. That one reconciles INDIVIDUAL events against the
firmware's recorder delta and can correct an under-counted event. It is blind
to the failure that matters most: when the detector produces NO events at all,
there is nothing for it to reconcile, and every stored number stays perfectly
self-consistent while water goes unrecorded.

This compares a whole DAY in aggregate, so silence itself becomes visible. It
is the only check in the add-on that measures our numbers against something we
did not produce — the firmware's cumulative total, derived from the same pulses
but never touched by detection, labelling or artifact logic.

Two real incidents motivate it: the July live-miss class, and dev46's own boot
regression, which recorded nothing for hours while every page looked healthy.

WHAT IT DOES NOT DO
-------------------
It never rewrites a volume. Correcting history from an aggregate would violate
annotate-don't-modify, and would also destroy the evidence the next run needs
to see the same problem. Per-event correction is ``recorder_reconcile``'s job,
where the endpoint-gap guard makes it safe.

DRIFT DIRECTION CARRIES MEANING
-------------------------------
* meter > events  →  water the meter saw that we never turned into events
  (missed/undercounted — the dangerous direction, because it is invisible)
* events > meter  →  counted twice, or another circuit's draw attributed here
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

# Below this the comparison is noise: meter quantisation, a draw straddling the
# day boundary, and the oval-gear meter's sub-threshold non-registration all
# live down here. BOTH gates must trip to alarm, so a quiet day cannot raise an
# alert on a fraction of a litre and a busy day is not judged in absolutes.
DRIFT_MIN_LITRES: float = 2.0
DRIFT_MIN_FRACTION: float = 0.10


def classify_drift(recorder_l: Optional[float],
                   events_l: float) -> Dict[str, Any]:
    """Pure verdict for one window. No DB, no HA — unit-testable.

    ``recorder_l`` None means the firmware total was unavailable for the window
    (HA outage, sensor restart): that is *unknown*, never zero. Calling an
    unavailable witness "0 L" would manufacture a 100% drift and alarm loudest
    exactly when the add-on is least able to explain itself — which trains the
    operator to ignore the one check that can detect silence.
    """
    if recorder_l is None:
        return {"status": "unavailable", "drift_litres": None,
                "drift_fraction": None, "direction": None,
                "note": "firmware total unavailable for this window"}

    drift = recorder_l - events_l
    denom = max(recorder_l, events_l, 1e-9)
    frac = drift / denom

    if abs(drift) < DRIFT_MIN_LITRES or abs(frac) < DRIFT_MIN_FRACTION:
        return {"status": "ok", "drift_litres": round(drift, 3),
                "drift_fraction": round(frac, 4), "direction": None,
                "note": "within tolerance"}

    if drift > 0:
        return {"status": "drift", "drift_litres": round(drift, 3),
                "drift_fraction": round(frac, 4), "direction": "missed_events",
                "note": ("the meter counted water this add-on never turned "
                         "into events")}
    return {"status": "drift", "drift_litres": round(drift, 3),
            "drift_fraction": round(frac, 4), "direction": "over_counted",
            "note": "events claim more water than the meter counted"}


def counter_was_reset(prev_total: Optional[float],
                      current_total: Optional[float]) -> bool:
    """True when the firmware's cumulative counter went BACKWARDS.

    A reflash or power event restarts the total at zero. Differencing across
    that point yields a hugely negative delta which would read as a colossal
    over-count — a spectacular false alarm at the moment the operator is
    already mid-task. Monotonic by contract otherwise.
    """
    if prev_total is None or current_total is None:
        return False
    return current_total < prev_total - 1e-6


def events_volume_sync(conn, circuit: str, start_utc: str,
                       end_utc: str) -> float:
    """Sum of ALL detected volume in the window, whatever its label.

    Deliberately unfiltered: artifacts, 'other', ignored and user-excluded
    events all count. The question is "how much water did we observe", not
    "how much do we like it" — a reconciliation that excluded the rows it
    distrusts would hide precisely the drift it exists to find.

    Uses volume_litres_effective, the same figure every total uses (the step-12
    chokepoint), so this compares like with like: a zeroed phantom contributes
    zero here exactly as it does to the dashboard.
    """
    row = conn.execute(
        "SELECT COALESCE(SUM(COALESCE(volume_litres_effective, "
        "                            volume_litres, 0)), 0) "
        "FROM events WHERE circuit = ? AND start_ts >= ? AND start_ts < ?",
        (circuit, start_utc, end_utc)).fetchone()
    return float(row[0] or 0.0)


async def check_yesterdays_drift(orch, circuit: str) -> Dict[str, Any]:
    """Compare yesterday's events against the firmware total. Annotate-only.

    Yesterday, not today: a complete HA-local day (the dev36 boundary every
    other total uses), so a draw still in progress cannot read as drift.
    """
    from .database import is_circuit_winterized, run_db, set_reconcile_state

    cfg = orch._cfg.get_circuit(circuit)
    if cfg is None or not getattr(cfg, "volume_sensor", None):
        return {"status": "skipped", "note": "no volume sensor configured"}

    # dev46 (46h): a drained circuit has a frozen counter and no events. That
    # is not drift, and dividing by it is not a comparison.
    if await run_db(is_circuit_winterized, orch.db, circuit):
        return {"status": "skipped", "note": "circuit is winterized"}

    start_utc = orch._local_midnight_utc(days_ago=1)
    end_utc = orch._local_midnight_utc(days_ago=0)

    # The counter is cumulative, so its DELTA across the window is the water —
    # two boundary reads, not a sum. Same get_history path the volume
    # baselines use.
    t0 = t1 = None
    try:
        import datetime as _dt
        s_dt = _dt.datetime.fromisoformat(start_utc).replace(tzinfo=_dt.timezone.utc)
        e_dt = _dt.datetime.fromisoformat(end_utc).replace(tzinfo=_dt.timezone.utc)
        vals = []
        for h in (await orch.ha.get_history(cfg.volume_sensor, s_dt, e_dt)) or []:
            v = h.get("state")
            if v in (None, "", "unknown", "unavailable"):
                continue
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                continue
        if len(vals) >= 2:
            t0, t1 = vals[0], vals[-1]
    except Exception as e:                      # noqa: BLE001 — HA is optional
        log.debug("[%s] volume drift: firmware total unavailable: %s", circuit, e)
        t0 = t1 = None

    if counter_was_reset(t0, t1):
        log.info("[%s] volume drift: firmware counter reset (%.1f -> %.1f) — "
                 "re-anchoring, no verdict for this window", circuit, t0, t1)
        await run_db(set_reconcile_state, orch.db, circuit,
                     through_ts=end_utc, last_delta_litres=None)
        return {"status": "reset", "note": "firmware counter restarted"}

    recorder_l = (t1 - t0) if (t0 is not None and t1 is not None) else None
    events_l = await run_db(events_volume_sync, orch.db, circuit,
                            start_utc, end_utc)
    verdict = classify_drift(recorder_l, events_l)
    verdict.update({"circuit": circuit, "window_start": start_utc,
                    "window_end": end_utc, "events_litres": round(events_l, 3),
                    "recorder_litres": (round(recorder_l, 3)
                                        if recorder_l is not None else None)})

    # flagged_delta only — `corrections` stays at zero from this path by
    # design; this module reports, it never rewrites.
    await run_db(set_reconcile_state, orch.db, circuit, through_ts=end_utc,
                 flagged_delta=1 if verdict["status"] == "drift" else 0,
                 last_delta_litres=verdict["drift_litres"])

    if verdict["status"] == "drift":
        log.warning("[%s] volume drift: meter %.1f L vs events %.1f L "
                    "(%+.1f L, %s) — %s", circuit, recorder_l, events_l,
                    verdict["drift_litres"], verdict["direction"],
                    verdict["note"])
    else:
        log.info("[%s] volume drift: %s (meter %s vs events %.1f L)",
                 circuit, verdict["status"],
                 f"{recorder_l:.1f} L" if recorder_l is not None else "n/a",
                 events_l)
    return verdict
