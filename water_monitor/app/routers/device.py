"""Device router — valve controls, thresholds, alert toggles, leak tests."""
from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ._helpers import ingress_redirect

log = logging.getLogger(__name__)

router = APIRouter(prefix="/device")


def _orch(request: Request):
    return request.app.state.orchestrator


def _templates(request: Request):
    return request.app.state.templates


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def device_page(request: Request):
    orch = _orch(request)
    cfg = orch._cfg

    circuit_states = []
    for circuit_cfg in cfg.circuits:
        state = await orch.get_live_state_async(circuit_cfg.circuit)

        # Leak test schedule
        from ..database import get_leak_test_schedule
        sched = get_leak_test_schedule(orch.db, circuit_cfg.circuit)
        state["schedule"] = dict(sched) if sched else {}

        circuit_states.append(state)

    return _templates(request).TemplateResponse("device.html", {
        "request": request,
        "circuits": circuit_states,
        "page": "device",
    })


# ------------------------------------------------------------------
# Valve control
# ------------------------------------------------------------------
@router.post("/valve/{circuit}/open")
async def valve_open(circuit: str, request: Request):
    log.info(">>> valve_open called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)
    if not cfg or not cfg.valve_entity:
        return JSONResponse(
            {"status": "error",
             "message": f"No valve entity configured for circuit '{circuit}'. "
                        "Re-run the setup wizard."},
            status_code=400,
        )
    ok = await orch.ha.open_valve(cfg.valve_entity)
    return JSONResponse({
        "status": "ok" if ok else "error",
        "entity_id": cfg.valve_entity,
        "message": "Valve open command sent." if ok
                   else f"Failed to open valve {cfg.valve_entity}. "
                        "Check the addon log for details.",
    })


@router.post("/valve/{circuit}/close")
async def valve_close(circuit: str, request: Request):
    log.info(">>> valve_close called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)
    if not cfg or not cfg.valve_entity:
        return JSONResponse(
            {"status": "error",
             "message": f"No valve entity configured for circuit '{circuit}'. "
                        "Re-run the setup wizard."},
            status_code=400,
        )
    ok = await orch.ha.close_valve(cfg.valve_entity)
    return JSONResponse({
        "status": "ok" if ok else "error",
        "entity_id": cfg.valve_entity,
        "message": "Valve close command sent." if ok
                   else f"Failed to close valve {cfg.valve_entity}. "
                        "Check the addon log for details.",
    })


# ------------------------------------------------------------------
# Fault resets
# ------------------------------------------------------------------
@router.post("/fault/{circuit}/reset")
async def fault_reset(circuit: str, request: Request):
    log.info(">>> fault_reset called for circuit=%s", circuit)
    orch = _orch(request)
    circuit_cfg = orch._cfg.get_circuit(circuit)
    p = circuit_cfg.esp_device_prefix if circuit_cfg else ""
    await orch.ha.call_service(
        "button", "press",
        {"entity_id": f"button.{p}reset_safety_fault_{circuit}"}
    )
    return JSONResponse({"status": "reset"})


@router.post("/trickle/{circuit}/reset")
async def trickle_reset(circuit: str, request: Request):
    log.info(">>> trickle_reset called for circuit=%s", circuit)
    orch = _orch(request)
    circuit_cfg = orch._cfg.get_circuit(circuit)
    p = circuit_cfg.esp_device_prefix if circuit_cfg else ""
    await orch.ha.call_service(
        "button", "press",
        {"entity_id": f"button.{p}reset_trickle_alert_{circuit}"}
    )
    return JSONResponse({"status": "reset"})


# ------------------------------------------------------------------
# Threshold updates
# ------------------------------------------------------------------
@router.post("/threshold/{circuit}/update")
async def threshold_update(
    circuit: str,
    request: Request,
    entity_id: str = Form(...),
    value: float = Form(...),
):
    orch = _orch(request)
    await orch.ha.set_number_value(entity_id, value)
    return JSONResponse({"status": "updated", "entity_id": entity_id, "value": value})


# ------------------------------------------------------------------
# Alert toggle
# ------------------------------------------------------------------
@router.post("/alert/{circuit}/{alert_type}/toggle")
async def alert_toggle(
    circuit: str, alert_type: str, request: Request,
    enabled: bool = Form(...),
):
    orch = _orch(request)
    circuit_cfg = orch._cfg.get_circuit(circuit)
    p = circuit_cfg.esp_device_prefix if circuit_cfg else ""
    entity_id = f"switch.{p}enable_{alert_type}_alert_{circuit}"
    if enabled:
        await orch.ha.turn_on(entity_id)
    else:
        await orch.ha.turn_off(entity_id)

    # Also update local alert_config
    from ..database import set_alert_enabled
    set_alert_enabled(orch.db, f"{alert_type}_{circuit}", enabled)

    return JSONResponse({"status": "updated", "enabled": enabled})


# ------------------------------------------------------------------
# Leak test — run now
# ------------------------------------------------------------------
@router.post("/leaktest/{circuit}/run")
async def leaktest_run(circuit: str, request: Request):
    log.info(">>> leaktest_run called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)

    if not cfg:
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=400,
        )

    if not cfg.leak_test_switch:
        return JSONResponse(
            {"status": "error",
             "message": "No leak test switch configured. Re-run the setup wizard."},
            status_code=400,
        )

    # Quick pre-flight checks for immediate user feedback
    valve_state = await orch.ha.get_state_value(cfg.valve_entity, "unknown")
    if valve_state != "open":
        return JSONResponse({
            "status": "skipped",
            "message": "Valve is not open. Open the valve first, then run the leak test.",
        })

    fault = await orch.ha.get_state_value(cfg.fault_sensor, "off")
    if fault == "on":
        return JSONResponse({
            "status": "skipped",
            "message": "Safety fault is active. Reset the fault first.",
        })

    if not orch.leak_test_scheduler:
        return JSONResponse(
            {"status": "error", "message": "Scheduler not ready — try again in a moment."},
            status_code=503,
        )

    if orch.leak_test_scheduler.is_running(circuit):
        return JSONResponse({
            "status": "skipped",
            "message": "A leak test is already running on this circuit.",
        })

    # Delegate to the scheduler — it triggers the switch, monitors the result
    # sensor, saves to leak_test_history, and sends the HA notification.
    asyncio.create_task(
        orch.leak_test_scheduler.run_now(circuit, triggered_by="manual")
    )
    log.info("Leak test scheduled via run_now for circuit=%s", circuit)

    return JSONResponse({
        "status": "started",
        "message": "Leak test started. The device will handle the test sequence automatically. "
                   "Watch the Dashboard for results.",
    })


# ------------------------------------------------------------------
# Leak test schedule update
# ------------------------------------------------------------------
@router.post("/leaktest/{circuit}/abort")
async def leaktest_abort(circuit: str, request: Request):
    log.info(">>> leaktest_abort called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)

    if not cfg:
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=400,
        )

    # Turn off the leak test switch on the ESP (stops the test)
    errors = []
    if cfg.leak_test_switch:
        domain = cfg.leak_test_switch.split(".", 1)[0]
        ok = await orch.ha.call_service(domain, "turn_off",
                                        {"entity_id": cfg.leak_test_switch})
        log.info("leaktest abort switch %s → %s",
                 cfg.leak_test_switch, "OK" if ok else "FAILED")
        if not ok:
            errors.append(f"Could not turn off leak test switch ({cfg.leak_test_switch})")

    # Mark the scheduler as no longer running so is_running() clears immediately
    if orch.leak_test_scheduler:
        orch.leak_test_scheduler.cancel(circuit)

    # Re-open the valve (firmware also does this, belt-and-braces)
    if cfg.valve_entity:
        ok = await orch.ha.open_valve(cfg.valve_entity)
        if not ok:
            errors.append(f"Could not re-open valve ({cfg.valve_entity})")

    if errors:
        return JSONResponse({
            "status": "error",
            "message": "Abort sent but some commands failed: " + "; ".join(errors),
        })

    return JSONResponse({
        "status": "aborted",
        "message": "Leak test aborted. Valve is reopening.",
    })


# ------------------------------------------------------------------
# Leak test schedule update
# ------------------------------------------------------------------
@router.post("/leaktest/{circuit}/schedule")
async def leaktest_schedule(circuit: str, request: Request):
    form = await request.form()
    orch = _orch(request)

    from ..database import upsert_leak_test_schedule
    upsert_leak_test_schedule(
        orch.db, circuit,
        enabled=form.get("enabled") == "on",
        auto_learn_hour=form.get("auto_learn_hour") == "on",
        frequency=form.get("frequency", "monthly"),
        day_of_week=int(form.get("day_of_week", 0)),
        week_of_month=int(form.get("week_of_month", 1)),
        run_hour=int(form.get("run_hour", 2)),
        run_minute=int(form.get("run_minute", 0)),
        notify_on_pass=form.get("notify_on_pass") == "on",
        notify_on_fail=form.get("notify_on_fail") == "on",
    )
    return ingress_redirect(request, "/device")
