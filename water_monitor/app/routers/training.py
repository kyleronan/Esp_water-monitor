"""Training-helper wizard (2b) — capture one clean sample per fixture type.

A finite, resumable onboarding checklist: the user runs each fixture once, the
event-completion hook (feature_extractor) records the resulting event(s) as
candidates, and the user confirms/accepts to write ~100%-pure 'training' labels
that seed the k-NN. Instant types (toilet/tap) capture the next event; windowed
types (shower/washer/dishwasher/irrigation) monitor for a user-chosen window.
"""
from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..fixtures import (FIXTURE_TYPE_LABELS, fixture_user_selectable_types,
                        zone_user_selectable_types)
from ..database import (
    get_circuit_type, training_capturable_types, training_window_options,
    arm_training_capture, cancel_training_capture, confirm_training_capture,
    reject_training_capture, extend_training_capture,
    expire_stale_training_captures, get_active_training_capture,
    get_training_checklist, training_capture_band_match,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/training")

# Per-circuit last monotonic time live flow was seen > floor — lets the poll
# endpoint distinguish 'waiting' from 'processing'. Process-local (single-instance
# addon), best-effort; never persisted.
_last_flow_positive: dict = {}
_FLOW_FLOOR = 0.15
_INSTANT_TYPES = ("toilet", "tap")


def _orch(r): return r.app.state.orchestrator
def _tmpl(r): return r.app.state.templates


async def _json(request: Request) -> dict:
    try:
        return await request.json() or {}
    except Exception:
        return {}


def _applicable_types(orch, circuit) -> list:
    """The wizard checklist types for a circuit: its user-selectable types,
    restricted to the ones the helper can capture (drops 'other')."""
    kind = get_circuit_type(orch.db, circuit)
    base = (zone_user_selectable_types() if kind == "zone"
            else fixture_user_selectable_types())
    cap = training_capturable_types()
    return [t for t in base if t in cap]


async def _bg_reclassify_training(circuit: str) -> None:
    """Single deferred reclassify after a checklist item is accepted/rejected —
    serialized + private connection (dev.8), so it never races live writes."""
    try:
        from ..database import (reclassify_all_events_from_signatures,
                                run_isolated_write)
        from ..config import DB_PATH
        await run_isolated_write(
            DB_PATH, lambda c: reclassify_all_events_from_signatures(c, circuit))
    except Exception as e:
        log.warning("[%s] training reclassify failed: %s", circuit, e)


# ── Page ──────────────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def training_page(request: Request):
    orch = _orch(request)
    circuits = []
    for cc in orch._cfg.circuits:
        types = _applicable_types(orch, cc.circuit)
        checklist = get_training_checklist(orch.db, cc.circuit, types)
        items = [{
            "type": t,
            "label": FIXTURE_TYPE_LABELS.get(t, t.replace("_", " ").title()),
            "windows": training_window_options(t),   # [] = instant
            "status": checklist[t]["status"],
            "captured_count": checklist[t]["captured_count"],
        } for t in types]
        circuits.append({
            "circuit": cc.circuit,
            "display_name": getattr(cc, "label", cc.circuit),
            # NB: NOT "items" — Jinja's `circ.items` resolves attribute-first to
            # dict.items() (a method), never the key. Use a non-colliding name.
            "fixtures": items,
        })
    return _tmpl(request).TemplateResponse("training.html", {
        "request": request, "page": "training", "circuits": circuits,
    })


# ── Live poll ─────────────────────────────────────────────────────────────────

def _sub_state(circuit, cap, live_flow):
    if cap is None:
        return None
    if cap.get("candidate_count", 0) > 0 or cap.get("status") == "ready":
        return "ready"
    now = time.monotonic()
    if live_flow and live_flow > _FLOW_FLOOR:
        _last_flow_positive[circuit] = now
        return "capturing"
    if now - _last_flow_positive.get(circuit, 0.0) <= 15.0:
        return "processing"
    return "waiting"


@router.get("/api/state")
async def training_state_api(request: Request):
    orch = _orch(request)
    db = orch.db
    expire_stale_training_captures(db)
    out = {}
    for cc in orch._cfg.circuits:
        circ = cc.circuit
        cap = get_active_training_capture(db, circ)
        try:
            live = await orch.get_live_state_async(circ)
            flow = float(str(live.get("flow_rate", "0")).replace(",", ""))
        except Exception:
            flow = 0.0
        entry = {
            "capture_sub_state": _sub_state(circ, cap, flow),
            "live_flow_lpm": flow,
            "status": cap["status"] if cap else None,
            "fixture_type": cap["fixture_type"] if cap else None,
            "captured_count": cap["candidate_count"] if cap else 0,
            "seconds_remaining": cap.get("seconds_remaining") if cap else None,
            "capture_id": cap["id"] if cap else None,
        }
        if cap and cap["fixture_type"] in _INSTANT_TYPES and cap["candidate_count"]:
            cand = db.execute(
                "SELECT e.volume_litres, e.duration_seconds "
                "FROM training_capture_candidates tc "
                "JOIN events e ON e.id = tc.event_id "
                "WHERE tc.capture_id = ? ORDER BY tc.rowid DESC LIMIT 1",
                (cap["id"],)).fetchone()
            if cand:
                entry["matches"] = training_capture_band_match(
                    cap["fixture_type"], cand["volume_litres"], cand["duration_seconds"])
                entry["candidate_volume"] = cand["volume_litres"]
        out[circ] = entry
    return JSONResponse(out)


# ── Actions (JSON, CSRF via X-CSRF-Token middleware) ──────────────────────────

@router.post("/api/{circuit}/arm")
async def arm_api(circuit: str, request: Request):
    orch = _orch(request)
    if get_active_training_capture(orch.db, circuit):
        return JSONResponse(
            {"error": f"Another capture is in progress for {circuit} — "
                      "complete or cancel it first."}, status_code=409)
    payload = await _json(request)
    ftype = payload.get("fixture_type")
    if ftype not in _applicable_types(orch, circuit):
        return JSONResponse({"error": "fixture type not applicable to this circuit"},
                            status_code=400)
    try:
        cap = arm_training_capture(orch.db, circuit, ftype,
                                   window_minutes=payload.get("window_minutes"))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return JSONResponse({"ok": True, "capture": cap})


@router.post("/api/{circuit}/cancel")
async def cancel_api(circuit: str, request: Request):
    n = cancel_training_capture(_orch(request).db, circuit)
    return JSONResponse({"ok": True, "cancelled": n})


@router.post("/api/{circuit}/confirm")
@router.post("/api/{circuit}/done")
async def confirm_api(circuit: str, request: Request):
    res = confirm_training_capture(_orch(request).db, circuit)
    if res.get("labeled"):
        import asyncio
        asyncio.create_task(_bg_reclassify_training(circuit))
    return JSONResponse({"ok": True, **res})


@router.post("/api/{circuit}/reject")
async def reject_api(circuit: str, request: Request):
    orch = _orch(request)
    payload = await _json(request)
    cid = payload.get("capture_id")
    if cid is None:
        cap = get_active_training_capture(orch.db, circuit)
        cid = cap["id"] if cap else None
    if cid is None:
        return JSONResponse({"error": "no capture to reject"}, status_code=400)
    res = reject_training_capture(orch.db, circuit, int(cid))
    if res.get("cleared"):
        import asyncio
        asyncio.create_task(_bg_reclassify_training(circuit))
    return JSONResponse({"ok": True, **res})


@router.post("/api/{circuit}/extend")
async def extend_api(circuit: str, request: Request):
    payload = await _json(request)
    res = extend_training_capture(_orch(request).db, circuit,
                                  payload.get("add_minutes", 15))
    return JSONResponse({"ok": True, **res})
