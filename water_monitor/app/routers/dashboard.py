"""Dashboard router — main status page."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ._helpers import run_blocking, startup_gate

router = APIRouter()
log = logging.getLogger(__name__)


def _get_orchestrator(request: Request):
    return request.app.state.orchestrator


# Leak-watch freshness: the estimate must come from one of the N most recent
# EVALUATED nights. "Evaluated" (not calendar) nights matches the convention in
# evaluate_hysteresis/evaluate_leak_alert — an HA outage that skips a night
# must not age out a live reading.
#
# dev34: the tile previously took the newest night carrying an estimate out of
# the last 14, with no age test at all. When the 2026-07 pump cycling stopped
# after the valve service, the last night that HAD an estimate kept winning and
# the banner asserted an active leak in the present tense for six days — and,
# because dev33's contamination gate only stops FUTURE writes, the number it
# was showing was the one the audit had already attributed to the 02:00
# irrigation program. Three evaluated nights of no estimate now clears it,
# which is the behavior the tile's docstring always claimed.
_LEAK_WATCH_MAX_AGE_NIGHTS: int = 3


def _fresh_leak_estimate(nights: list, ack: str | None) -> Dict[str, Any] | None:
    """Newest evaluated night carrying a leak estimate, or None.

    Three gates: the night must be within _LEAK_WATCH_MAX_AGE_NIGHTS of the
    most recent evaluated night, it must be newer than any night the user has
    dismissed, and the evaluated night immediately BEFORE it must also have
    detected cycling (corroboration). Dismissal is per-READING — a later night
    with a fresh estimate re-shows the tile, so one click can never silence a
    real leak.

    The corroboration gate (2026-08-11): a real leak regime cycles every
    night (the 2026-07 incident detected on 7/25 AND 7/26), while one-off
    contamination — a softener regen shrinking the quiet window (8/10,
    "1114 L/day"), the 02:00 irrigation program (7/28, "110 L/day") — shows
    up as a single detected night surrounded by quiet ones. Both historical
    false banners fail this gate; the real incident's banner passes it.
    """
    dismissed_through = None
    if ack and str(ack).startswith("dismissed:"):
        dismissed_through = str(ack).split(":", 1)[1]
    for i, n in enumerate(nights[:_LEAK_WATCH_MAX_AGE_NIGHTS]):
        if not n.get("est_leak_lpd"):
            continue
        if dismissed_through and n["night_date"] <= dismissed_through:
            return None      # this reading, or an older one, was acknowledged
        prev = nights[i + 1] if i + 1 < len(nights) else None
        if not (prev and prev.get("any_detected")):
            return None      # uncorroborated single night — likely a
                             # contaminated window, not a leak regime
        return n
    return None


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    orch = _get_orchestrator(request)

    # dev46 (46c) — the LANDING page needs the gate most of all, and the
    # original three-page scope missed it. This is where ingress drops the
    # operator after every restart, so it is the first thing hit while the
    # boot pass owns the single DB worker; the page's own first hop then sits
    # in the queue behind ~21 s of cluster replay and the operator sees the
    # ingress spinner on a blank frame with nothing to explain it (observed
    # 2026-08-17 19:22). A "still starting" notice is a worse-looking page and
    # a far better answer.
    gated = startup_gate(request, "dashboard", "Dashboard", "/")
    if gated is not None:
        return gated

    cfg = orch._cfg

    # dev46 (46a) — ONE hop for every DB read this page needs (charts,
    # profile, per-circuit training + leak schedules, both banners, the
    # pump-regime nights). Nothing below may query the shared connection
    # inline: the loop thread and the DB worker must never touch it at once.
    from ..database import get_home_profile
    dashboard_payload = await run_blocking(
        _build_dashboard_sync_payload, orch.db, cfg.circuits, get_home_profile,
        orch.training_manager,
    )
    chart_data = dashboard_payload["chart_data"]
    profile = dashboard_payload["profile"]
    pump_banner = dashboard_payload["pump_banner"]
    supply_banner = dashboard_payload["supply_banner"]
    nights = dashboard_payload["nights"]

    _idle_training = {"state": "idle", "events_collected": 0,
                      "minimum_events": 0, "days_remaining": 0,
                      "percent_complete": 0}

    # Fetch live state for all circuits (HA I/O — natively async, stays here).
    circuit_states = []
    for circuit_cfg in cfg.circuits:
        state = await orch.get_live_state_async(circuit_cfg.circuit)
        state["training"] = (
            dashboard_payload["training"].get(circuit_cfg.circuit)
            or _idle_training)

        sched = dashboard_payload["schedules"].get(circuit_cfg.circuit)
        state["next_leak_test"] = sched["next_run_at"] if sched else None
        state["last_leak_test"] = sched["last_run_at"] if sched else None
        state["last_leak_result"] = sched["last_result"] if sched else None

        circuit_states.append(state)

    templates = request.app.state.templates

    from ..fixtures import CIRCUIT_TYPE_LABELS

    # Phase 5a leak-watch tile: latest nightly street-calibrated estimate,
    # shown only when pump mode is armed and a RECENT evaluated night carried
    # an estimate. Best-effort. (Nights were fetched in the bundle above; the
    # freshness/format work below is pure Python.)
    leak_watch = None
    try:
        if nights:
            latest = _fresh_leak_estimate(nights, profile.get("leak_watch_ack"))
            if latest:
                # Local wall-clock range of the analyzed window ("1:05 AM to
                # 2:11 AM"), so the banner names WHEN the cycling was seen —
                # quiet hours differ per home and aren't always at night.
                window_range = None
                try:
                    if latest.get("window_start_ts") and latest.get("window_end_ts"):
                        _tz = getattr(orch, "_ha_tz", None) or timezone.utc
                        _fmt = lambda iso: datetime.fromisoformat(iso).astimezone(
                            _tz).strftime("%I:%M %p").lstrip("0")
                        window_range = (f"{_fmt(latest['window_start_ts'])} to "
                                        f"{_fmt(latest['window_end_ts'])}")
                except (ValueError, TypeError):
                    window_range = None
                leak_watch = {
                    "lpd": latest["est_leak_lpd"],
                    "gpd": round(latest["est_leak_lpd"] / 3.785, 1),
                    "night": latest["night_date"],
                    "window_range": window_range,
                    "period_min": (round(latest["period_s"] / 60.0, 1)
                                   if latest.get("period_s") else None),
                    "nights": [
                        {"night_date": n["night_date"],
                         "est": n.get("est_leak_lpd")}
                        for n in nights],
                }
    except Exception:
        leak_watch = None

    return templates.TemplateResponse("dashboard.html", {
        "request":             request,
        "circuits":            circuit_states,
        "chart_data_json":     json.dumps(chart_data),
        "page":                "dashboard",
        "profile":             profile,
        "away_mode":           profile.get("away_mode", False),
        "circuit_type_labels": CIRCUIT_TYPE_LABELS,
        "pump_banner":         pump_banner,
        "supply_banner":       supply_banner,
        "leak_watch":          leak_watch,
    })


@router.get("/api/dashboard/live")
async def dashboard_live(request: Request):
    """JSON endpoint for polling live state (used by JS auto-refresh)."""
    orch = _get_orchestrator(request)
    cfg = orch._cfg

    # dev46 (46a): get_training_info reads training_state — a DB touch. This
    # endpoint is polled by the dashboard's auto-refresh, so an inline query
    # here was the most frequent loop-thread contact with the shared
    # connection in the whole addon. One run_db hop covers every circuit.
    from ..database import run_db
    _idle = {"state": "idle", "events_collected": 0, "minimum_events": 0,
             "days_remaining": 0, "percent_complete": 0}
    tm = orch.training_manager
    if tm is not None:
        training = await run_db(
            lambda: {c.circuit: tm.get_training_info(c.circuit)
                     for c in cfg.circuits})
    else:
        training = {}

    result = {}
    for circuit_cfg in cfg.circuits:
        state = await orch.get_live_state_async(circuit_cfg.circuit)
        state["training"] = training.get(circuit_cfg.circuit) or _idle
        result[circuit_cfg.circuit] = state

    return JSONResponse(result)


@router.get("/api/jobs")
async def jobs_poll(request: Request, since: int = 0):
    """Recent background-job statuses with id > ``since`` for the UI poll-and-toast
    (§2.4 reclassify / calibration feedback). Newest first."""
    orch = _get_orchestrator(request)
    from ..database import get_jobs_since, run_db
    # dev46 (46a): polled endpoint — off the loop thread, onto the DB worker.
    jobs = await run_db(get_jobs_since, orch.db, since_id=since)
    return JSONResponse({"jobs": jobs})


@router.get("/api/chart/{circuit}")
async def chart_data(circuit: str, request: Request):
    """Return hourly volume data for chart refresh."""
    from ..circuit_compat import resolve_circuit
    circuit = resolve_circuit(circuit)
    orch = _get_orchestrator(request)
    data = await run_blocking(_build_chart_data, orch.db, circuit)
    return JSONResponse(data)


@router.get("/api/dashboard/pressure/{circuit}")
async def dashboard_pressure(circuit: str, request: Request):
    """Last-24h pressure series (from the HA recorder) + resting baseline for the
    modal chart. Reads live from HA — the addon stores no pressure time series. Never
    500s; failure modes are surfaced via `error` so the modal shows the right hint."""
    from ..circuit_compat import resolve_circuit
    from ..database import downsample_pressure_series, recent_pressure_baseline
    orch = _get_orchestrator(request)
    circuit = resolve_circuit(circuit)

    def _fail(err: str, baseline=None):
        return JSONResponse({"available": False, "error": err, "points": [],
                             "baseline_psi": baseline, "unit": "psi"})

    cfg = orch._cfg.get_circuit(circuit)
    entity = (cfg.pressure_history_sensor or cfg.pressure_avg_sensor) if cfg else ""
    if not entity:
        return _fail("no_entity")

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    try:
        states = await asyncio.wait_for(
            orch.ha.get_history(entity, start, end, significant_changes_only=True),
            timeout=8.0)
    except Exception as e:                       # timeout / HA down / WS error
        log.warning("[%s] pressure history fetch failed: %s", circuit, e)
        return _fail("ha_unreachable")

    points = downsample_pressure_series(
        states, start.timestamp(), end.timestamp(), buckets=288)
    baseline = await run_blocking(
        recent_pressure_baseline, orch.db, circuit, start.isoformat())
    if not any(p["v"] is not None for p in points):
        return _fail("no_history", baseline)
    return JSONResponse({"available": True, "points": points,
                         "baseline_psi": baseline, "unit": "psi"})


def _build_dashboard_sync_payload(db, circuits, get_home_profile,
                                  training_manager=None) -> dict:
    """Synchronous bundle of the dashboard's DB work.

    Combined into one executor hop so a multi-circuit refresh doesn't
    bounce in and out of the thread pool for every per-circuit query.
    Returns the chart_data dict (keyed by circuit id) plus the resolved
    home_profile row.

    dev46 (46a) — this bundle grew to cover EVERY DB read the dashboard
    needs: training state, leak-test schedules, both banners and the
    pump-regime nights used to have their own inline queries on the event
    loop. Inline sync queries touch the shared connection from the loop
    thread while the DB worker may be mid-statement on it — the same
    two-thread window that produced the 8/15 + 8/16 InterfaceErrors. One
    hop, one thread. Each best-effort block keeps its own try/except so a
    single failing widget still can't break the dashboard.
    """
    chart_data: Dict[str, Any] = {}
    for c in circuits:
        chart_data[c.circuit] = _build_chart_data(db, c.circuit)
    profile = dict(get_home_profile(db) or {})

    # Per-circuit training state + leak-test schedule.
    from ..database import get_leak_test_schedule
    training: Dict[str, Any] = {}
    schedules: Dict[str, Any] = {}
    for c in circuits:
        if training_manager is not None:
            try:
                training[c.circuit] = training_manager.get_training_info(c.circuit)
            except Exception:
                training[c.circuit] = None
        try:
            schedules[c.circuit] = get_leak_test_schedule(db, c.circuit)
        except Exception:
            schedules[c.circuit] = None

    # Pump-regime detection banner (dev23) — banner+confirm: shows only while
    # detection is unacknowledged; confirming is the sole auto-path into pump
    # mode. Best-effort; a failure must never break the dashboard.
    try:
        from ..pump_regime_detector import pump_banner_state
        pump_banner = pump_banner_state(db)
    except Exception:
        pump_banner = {"show": False}

    # Supply-pressure regime banner — shows while the current regime was
    # auto-detected and neither confirmed (recalibrated) nor dismissed.
    try:
        from ..supply_regime import supply_banner_state
        supply_banner = supply_banner_state(
            db, circuits[0].circuit if circuits else None)
    except Exception:
        supply_banner = {"show": False}

    # Phase 5a leak-watch tile: the nights feeding the latest nightly
    # street-calibrated estimate, only when pump mode is armed.
    try:
        from ..config import pump_gates_active
        from ..database import get_pump_regime_nights
        nights = (get_pump_regime_nights(db, limit=14)
                  if any(pump_gates_active(db, c.circuit) for c in circuits)
                  else [])
    except Exception:
        nights = []

    return {
        "chart_data":    chart_data,
        "profile":       profile,
        "training":      training,
        "schedules":     schedules,
        "pump_banner":   pump_banner,
        "supply_banner": supply_banner,
        "nights":        nights,
    }


def _build_chart_data(db, circuit: str) -> Dict[str, Any]:
    """
    Build hourly volume chart data for the past 24 hours (rolling).
    Returns {labels: [...], values: [...], total: float}.

    Buckets are UTC hours (that's how hourly_volume is keyed) but the LABELS
    are the home's local clock. They used to be the raw UTC hour, so a 05:00
    shower was drawn at "11:00" and the whole chart read six hours out of step
    with every other time on the page.
    """
    from ..database import get_hourly_volumes
    from ..event_rules import get_home_timezone
    rows = get_hourly_volumes(db, circuit, hours=24)

    # Build exactly 24 slots: hours 23..1 back + current partial hour (i=0)
    now = datetime.now(timezone.utc)
    slots: Dict[str, float] = {}
    for i in range(23, -1, -1):
        slot_time = (now - timedelta(hours=i)).replace(
            minute=0, second=0, microsecond=0)
        slots[slot_time.isoformat()[:13]] = 0.0

    # Fill in stored data
    for row in rows:
        key = row["hour_ts"][:13]  # YYYY-MM-DDTHH
        if key in slots:
            slots[key] = row["volume_litres"]

    tz = get_home_timezone() or timezone.utc
    ordered = sorted(slots.keys())

    def _label(key: str) -> str:
        # key is 'YYYY-MM-DDTHH' in UTC. Zones offset by whole hours land on
        # the hour; the half-hour zones (India, parts of Australia) keep their
        # real minutes rather than being rounded into a lie.
        dt = datetime.strptime(key, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
        return dt.astimezone(tz).strftime("%H:%M")

    labels = [_label(k) for k in ordered]
    values = [round(slots[k], 2) for k in ordered]
    total = round(sum(values), 1)

    return {"labels": labels, "values": values, "total": total}
