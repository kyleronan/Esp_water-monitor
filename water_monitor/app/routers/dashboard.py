"""Dashboard router — main status page."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ._helpers import run_blocking

router = APIRouter()


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


@router.get("/api/chart/{circuit}")
async def chart_data(circuit: str, request: Request):
    """Return hourly volume data for chart refresh."""
    from ..circuit_compat import resolve_circuit
    circuit = resolve_circuit(circuit)
    orch = _get_orchestrator(request)
    data = await run_blocking(_build_chart_data, orch.db, circuit)
    return JSONResponse(data)


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
