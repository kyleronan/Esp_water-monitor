"""Dashboard router — main status page."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ._helpers import run_blocking

router = APIRouter()
log = logging.getLogger(__name__)


def _get_orchestrator(request: Request):
    return request.app.state.orchestrator


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    orch = _get_orchestrator(request)
    cfg = orch._cfg

    # Fetch live state for all circuits
    circuit_states = []
    for circuit_cfg in cfg.circuits:
        state = await orch.get_live_state_async(circuit_cfg.circuit)
        training = (
            orch.training_manager.get_training_info(circuit_cfg.circuit)
            if orch.training_manager else {"state": "idle", "events_collected": 0,
                                           "minimum_events": 0, "days_remaining": 0,
                                           "percent_complete": 0}
        )
        state["training"] = training

        # Leak test schedule
        from ..database import get_leak_test_schedule
        sched = get_leak_test_schedule(orch.db, circuit_cfg.circuit)
        state["next_leak_test"] = sched["next_run_at"] if sched else None
        state["last_leak_test"] = sched["last_run_at"] if sched else None
        state["last_leak_result"] = sched["last_result"] if sched else None

        circuit_states.append(state)

    # Volume chart data for each circuit. Pull the sync DB queries
    # for ALL circuits + the home_profile read in a single executor
    # hop so the dashboard refresh doesn't make the event loop fight
    # for control across N circuits' worth of `get_hourly_volumes`.
    from ..database import get_home_profile
    dashboard_payload = await run_blocking(
        _build_dashboard_sync_payload, orch.db, cfg.circuits, get_home_profile,
    )
    chart_data = dashboard_payload["chart_data"]
    profile = dashboard_payload["profile"]

    templates = request.app.state.templates

    from ..fixtures import CIRCUIT_TYPE_LABELS
    return templates.TemplateResponse("dashboard.html", {
        "request":             request,
        "circuits":            circuit_states,
        "chart_data_json":     json.dumps(chart_data),
        "page":                "dashboard",
        "profile":             profile,
        "away_mode":           profile.get("away_mode", False),
        "circuit_type_labels": CIRCUIT_TYPE_LABELS,
    })


@router.get("/api/dashboard/live")
async def dashboard_live(request: Request):
    """JSON endpoint for polling live state (used by JS auto-refresh)."""
    orch = _get_orchestrator(request)
    cfg = orch._cfg

    result = {}
    for circuit_cfg in cfg.circuits:
        state = await orch.get_live_state_async(circuit_cfg.circuit)
        training = (
            orch.training_manager.get_training_info(circuit_cfg.circuit)
            if orch.training_manager else {"state": "idle", "events_collected": 0,
                                           "minimum_events": 0, "days_remaining": 0,
                                           "percent_complete": 0}
        )
        state["training"] = training
        result[circuit_cfg.circuit] = state

    return JSONResponse(result)


@router.get("/api/jobs")
async def jobs_poll(request: Request, since: int = 0):
    """Recent background-job statuses with id > ``since`` for the UI poll-and-toast
    (§2.4 reclassify / calibration feedback). Newest first."""
    orch = _get_orchestrator(request)
    from ..database import get_jobs_since
    return JSONResponse({"jobs": get_jobs_since(orch.db, since_id=since)})


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


def _build_dashboard_sync_payload(db, circuits, get_home_profile) -> dict:
    """Synchronous bundle of the dashboard's DB work.

    Combined into one executor hop so a multi-circuit refresh doesn't
    bounce in and out of the thread pool for every per-circuit query.
    Returns the chart_data dict (keyed by circuit id) plus the resolved
    home_profile row.
    """
    chart_data: Dict[str, Any] = {}
    for c in circuits:
        chart_data[c.circuit] = _build_chart_data(db, c.circuit)
    return {
        "chart_data": chart_data,
        "profile":    dict(get_home_profile(db) or {}),
    }


def _build_chart_data(db, circuit: str) -> Dict[str, Any]:
    """
    Build hourly volume chart data for the past 24 hours (rolling).
    Returns {labels: [...], values: [...], total: float}.
    """
    from ..database import get_hourly_volumes
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

    labels = [k[-2:] + ":00" for k in sorted(slots.keys())]
    values = [round(v, 2) for v in
               [slots[k] for k in sorted(slots.keys())]]
    total = round(sum(values), 1)

    return {"labels": labels, "values": values, "total": total}
