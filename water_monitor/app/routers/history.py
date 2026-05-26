"""History router — event log and leak test history."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ..circuit_compat import resolve_circuit
from ..fixtures import FIXTURE_TYPE_LABELS, user_selectable_types
from ..database import patch_event as _patch_event

_VALID_USER_FIXTURE_TYPES: frozenset = frozenset(user_selectable_types())

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
    except Exception:
        # Log with full traceback at ERROR; the user-facing page stays
        # generic so we don't leak exception text / stack info into the
        # HTML. Pointing at the addon log is enough actionable detail
        # for the user; the rest belongs in the log only.
        log.error("History page error", exc_info=True)
        return HTMLResponse(
            "<h1>History temporarily unavailable</h1>"
            "<p>Something went wrong rendering this page. "
            "Check the addon log for details, or refresh to retry.</p>",
            status_code=500,
        )


async def _history_page(request: Request):
    orch = _orch(request)
    cfg  = orch._cfg

    date_from   = request.query_params.get("from", "").strip()
    date_to     = request.query_params.get("to",   "").strip()
    chart_range = request.query_params.get("range", "30d")
    # Optional filters surfaced by the Dashboard "Degraded supply" banner
    # link. `filter=degraded` restricts the per-circuit events list to
    # degraded-supply rows; `circuit=...` further scopes to one circuit.
    filter_param  = request.query_params.get("filter",  "").strip().lower()
    filter_circuit = resolve_circuit(request.query_params.get("circuit", "").strip())
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
        # If a specific circuit filter is set, skip rendering the other ones.
        if filter_circuit and circuit_cfg.circuit != filter_circuit:
            continue
        from ..database import (get_recent_events, get_leak_test_history,
                                get_daily_summaries)
        events = get_recent_events(
            orch.db, circuit_cfg.circuit,
            limit=DEFAULT_EVENT_LIMIT,
            date_from=date_from or None,
            date_to=date_to or None,
        )
        # Filter to only degraded events when the page is opened from the
        # dashboard "Degraded supply" View link. Post-filter rather than a
        # dedicated SQL path because the rendering machinery is already in
        # place to handle a filtered list; this avoids touching the DB layer.
        if filter_param == "degraded":
            events = [e for e in events if dict(e).get("degraded_supply")]
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
            "display_name":    circuit_cfg.label,
            "events":          events,
            "leak_tests":      leak_tests,
            "event_count":     len(events),
            "summaries":       summaries,
            "prior_summaries": prior_summaries,
            "hv_daily":        hv_daily,
        })

    fixture_type_options = [
        {"value": k, "label": FIXTURE_TYPE_LABELS.get(k, k.replace("_", " ").title())}
        for k in user_selectable_types()
    ]

    return _tmpl(request).TemplateResponse("history.html", {
        "request":              request,
        "circuit_history":      circuit_history,
        "page":                 "history",
        "date_from":            date_from,
        "date_to":              date_to,
        "using_range":          using_range,
        "default_limit":        DEFAULT_EVENT_LIMIT,
        "chart_range":          chart_range,
        "fixture_type_options": fixture_type_options,
        "fixture_type_labels":  FIXTURE_TYPE_LABELS,
        "filter_param":         filter_param,
        "filter_circuit":       filter_circuit,
    })


@router.get("/api/event/{event_id}/waveform")
async def event_waveform(event_id: str, request: Request):
    """Return the high-resolution min/max waveform for one event.

    Source: the `event_waveforms` table, populated by feature_extractor's
    `_persist_waveform` after each new event is processed. Historical events
    captured before this migration won't have a waveform row — clients get
    a 404 and should fall back to the existing 32-point signature display.
    """
    import json as _json
    orch = _orch(request)
    row = orch.db.execute(
        """SELECT w.flow_min_json, w.flow_max_json,
                  w.pressure_min_json, w.pressure_max_json,
                  w.duration_seconds,
                  e.start_ts, e.degraded_supply
           FROM event_waveforms w
           JOIN events e ON w.event_id = e.id
           WHERE w.event_id = ?""",
        (event_id,),
    ).fetchone()
    if not row:
        return JSONResponse({"error": "waveform not found"}, status_code=404)
    try:
        return JSONResponse({
            "event_id":         event_id,
            "start_ts":         row["start_ts"],
            "duration_seconds": row["duration_seconds"],
            "degraded_supply":  bool(row["degraded_supply"]),
            "flow_min":         _json.loads(row["flow_min_json"]),
            "flow_max":         _json.loads(row["flow_max_json"]),
            "pressure_min":     _json.loads(row["pressure_min_json"]),
            "pressure_max":     _json.loads(row["pressure_max_json"]),
        })
    except (ValueError, TypeError) as e:
        log.warning("Waveform JSON decode failed for %s: %s", event_id, e)
        return JSONResponse({"error": "waveform corrupted"}, status_code=500)


@router.get("/api/events/{circuit}")
async def events_api(
    circuit: str,
    request: Request,
    limit: int = DEFAULT_EVENT_LIMIT,
    date_from: str = "",
    date_to: str = "",
):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    from ..database import get_recent_events
    events = get_recent_events(
        orch.db, circuit,
        limit=limit,
        date_from=date_from or None,
        date_to=date_to or None,
    )
    return JSONResponse(events)


@router.patch("/api/events/{circuit}/{event_id}")
async def patch_event_api(circuit: str, event_id: str, request: Request):
    """Update user-editable fields on a single event.

    Accepted payload keys (all optional):
      user_fixture_type (str | null) — assign or clear a fixture type label.
      excluded_from_training (bool)  — ignore / restore the event.
    """
    payload = await request.json()
    db = _orch(request).db

    # Validate fixture type before touching the DB
    if "user_fixture_type" in payload:
        ftype = payload["user_fixture_type"] or None
        if ftype is not None and ftype not in _VALID_USER_FIXTURE_TYPES:
            return JSONResponse(
                {"error": f"Invalid fixture type: {ftype!r}"},
                status_code=400,
            )

    kwargs: dict = {}
    if "user_fixture_type" in payload:
        kwargs["user_fixture_type"] = payload["user_fixture_type"] or None
    if "excluded_from_training" in payload:
        kwargs["excluded_from_training"] = bool(payload["excluded_from_training"])

    found = _patch_event(db, event_id, circuit, **kwargs)
    if not found:
        return JSONResponse({"error": "Event not found"}, status_code=404)
    return JSONResponse({"ok": True})
