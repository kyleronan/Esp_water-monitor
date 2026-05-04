"""History router — event log and leak test history."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

log = logging.getLogger(__name__)
router = APIRouter(prefix="/history")

DEFAULT_EVENT_LIMIT = 100


def _orch(r): return r.app.state.orchestrator
def _tmpl(r): return r.app.state.templates


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def history_page(request: Request):
    try:
        return await _history_page(request)
    except Exception as e:
        log.error("History page error: %s", e, exc_info=True)
        return HTMLResponse(
            f"<h1>History page error</h1><pre>{e}</pre>"
            "<p>Check the addon log for details.</p>",
            status_code=500,
        )


async def _history_page(request: Request):
    orch = _orch(request)
    cfg  = orch._cfg

    date_from   = request.query_params.get("from", "").strip()
    date_to     = request.query_params.get("to",   "").strip()
    chart_range = request.query_params.get("range", "30d")
    # 30d | 6m | 1y | all | monthly | yearly | yoy
    using_range = bool(date_from or date_to)

    from datetime import date, timedelta as td
    today = date.today()
    chart_from_map = {
        "30d":    (today - td(days=30)).isoformat(),
        "6m":     (today - td(days=183)).isoformat(),
        "1y":     (today - td(days=365)).isoformat(),
        "all":    None,
        "monthly": today.replace(day=1).isoformat(),
        "yearly":  today.replace(month=1, day=1).isoformat(),
        "yoy":    (today - td(days=730)).isoformat(),
    }
    chart_from = chart_from_map.get(chart_range, chart_from_map["30d"])

    circuit_history = []
    for circuit_cfg in cfg.circuits:
        from ..database import (get_recent_events, get_leak_test_history,
                                get_daily_summaries)
        events = get_recent_events(
            orch.db, circuit_cfg.circuit,
            limit=DEFAULT_EVENT_LIMIT,
            date_from=date_from or None,
            date_to=date_to or None,
        )
        leak_tests = get_leak_test_history(orch.db, circuit_cfg.circuit, limit=20)
        summaries  = get_daily_summaries(
            orch.db, circuit_cfg.circuit,
            date_from=chart_from,
        )

        # For YoY: also fetch prior-year summaries (shifted by 365 days)
        prior_summaries = []
        if chart_range == "yoy" and chart_from:
            from datetime import date as _date, timedelta as _td
            prior_from = (_date.fromisoformat(chart_from)
                          - _td(days=365)).isoformat()
            prior_to   = (today - _td(days=365)).isoformat()
            prior_summaries = get_daily_summaries(
                orch.db, circuit_cfg.circuit,
                date_from=prior_from,
                date_to=prior_to,
            )

        # Hourly volume fallback: aggregate to daily for chart when no summaries yet
        hv_daily = {}
        if not summaries:
            hv_rows = orch.db.execute("""
                SELECT date(hour_ts) AS day, SUM(volume_litres) AS vol
                FROM hourly_volume
                WHERE circuit = ?
                  AND (? IS NULL OR hour_ts >= ?)
                GROUP BY date(hour_ts)
                ORDER BY day ASC
            """, (circuit_cfg.circuit, chart_from, chart_from)).fetchall()
            hv_daily = {r["day"]: r["vol"] for r in hv_rows}

        circuit_history.append({
            "circuit":         circuit_cfg.circuit,
            "display_name":    circuit_cfg.display_name,
            "events":          events,
            "leak_tests":      leak_tests,
            "event_count":     len(events),
            "summaries":       summaries,
            "prior_summaries": prior_summaries,
            "hv_daily":        hv_daily,
        })

    return _tmpl(request).TemplateResponse("history.html", {
        "request":         request,
        "circuit_history": circuit_history,
        "page":            "history",
        "date_from":       date_from,
        "date_to":         date_to,
        "using_range":     using_range,
        "default_limit":   DEFAULT_EVENT_LIMIT,
        "chart_range":     chart_range,
    })


@router.get("/api/events/{circuit}")
async def events_api(
    circuit: str,
    request: Request,
    limit: int = DEFAULT_EVENT_LIMIT,
    date_from: str = "",
    date_to: str = "",
):
    orch = _orch(request)
    from ..database import get_recent_events
    events = get_recent_events(
        orch.db, circuit,
        limit=limit,
        date_from=date_from or None,
        date_to=date_to or None,
    )
    return JSONResponse(events)
