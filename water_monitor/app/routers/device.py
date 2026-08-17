"""Device router — valve controls, thresholds, alert toggles, leak tests."""
from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ._helpers import coerce_int, ingress_redirect
from ..circuit_compat import resolve_circuit

log = logging.getLogger(__name__)

router = APIRouter(prefix="/device")

# Immutable set of alert types accepted by the firmware — rejects arbitrary strings
# that would be silently interpolated into entity IDs sent to HA.
VALID_ALERT_TYPES: frozenset[str] = frozenset({
    "high_flow", "trickle", "pressure_drop", "leak_test",
})

# Only ESPHome number.* roles that carry writable threshold values.
# Names must match keys in ROLE_PATTERNS so load_circuit_entities() returns them.
# Sensors, valves, switches, binary_sensors, and input_number helpers are excluded.
_THRESHOLD_ROLES: frozenset[str] = frozenset({
    "leak_test_duration_number",   # preferred name (firmware v3.6+)
    "leak_test_duration_sensor",   # compat alias — remove after one release
    "burst_threshold",
    "pressure_drop_threshold",
    "leak_pressure_threshold",
    "trickle_min_flow",
    "trickle_max_flow",
    "trickle_duration",
})


def _orch(request: Request):
    return request.app.state.orchestrator


def _templates(request: Request):
    return request.app.state.templates


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def device_page(request: Request):
    orch = _orch(request)
    cfg = orch._cfg

    # dev46 (46a): every DB read this page needs, in ONE hop off the loop
    # thread. The per-circuit loop below still awaits HA (natively async) but
    # never touches the shared connection.
    from ..database import run_db
    db_payload = await run_db(_device_db_payload, orch.db, list(cfg.circuits))

    circuit_states = []
    for circuit_cfg in cfg.circuits:
        state = await orch.get_live_state_async(circuit_cfg.circuit)

        # Leak test schedule
        sched = db_payload["schedules"].get(circuit_cfg.circuit)
        state["schedule"] = dict(sched) if sched else {}

        # Phase 3 — waveform-transport health: addon-side counters (assembled /
        # firmware-flagged degraded / transport gaps) + the firmware's own dropped-
        # sample count. Surfaces the silently-lossy waveform stream.
        ed = orch.event_detector
        wf = (ed.waveform_transport_stats(circuit_cfg.circuit)
              if ed else {"assembled": 0, "degraded": 0, "gaps": 0})
        wf["fw_chunk_drops"] = None
        drop_sensor = getattr(circuit_cfg, "wf_chunk_drop_count_sensor", "")
        if drop_sensor and orch.ha:
            raw = await orch.ha.get_state_value(drop_sensor, None)
            if raw not in (None, "unknown", "unavailable", ""):
                try:
                    wf["fw_chunk_drops"] = int(float(raw))
                except (TypeError, ValueError):
                    pass
        state["waveform"] = wf

        # Phase 3 §2 — recorder volume reconciliation surface: cumulative correction /
        # flag counts + last-run time (from reconcile_state) and the current flag backlog
        # (healthy events whose stored volume still diverges from the recorder's). Only
        # shown when the firmware publishes a cumulative volume sensor.
        if getattr(circuit_cfg, "volume_sensor", ""):
            reconcile = db_payload["reconcile"].get(circuit_cfg.circuit)
            if reconcile is not None:
                state["reconcile"] = reconcile

        circuit_states.append(state)

    return _templates(request).TemplateResponse("device.html", {
        "request": request,
        "circuits": circuit_states,
        "page": "device",
    })


def _device_db_payload(db, circuits) -> dict:
    """Every DB read the device page needs, bundled into one DB-thread call.

    dev46 (46a): these ran inline on the event loop, once per circuit —
    concurrent statements on the shared connection whenever a background job
    was mid-write. Best-effort per circuit so one bad row can't blank the page.
    """
    from ..database import (get_leak_test_schedule, get_reconcile_state,
                            get_sensitivity_config)
    from ..recorder_reconcile import count_flagged_backlog
    from ..routers.settings import _fmt_local_ts
    from ..event_rules import get_home_timezone

    schedules: dict = {}
    reconcile: dict = {}
    tz = get_home_timezone()
    for c in circuits:
        try:
            schedules[c.circuit] = get_leak_test_schedule(db, c.circuit)
        except Exception:
            schedules[c.circuit] = None
        # Phase 3 §2 — recorder volume reconciliation surface: cumulative
        # correction / flag counts + last-run time (from reconcile_state) and
        # the current flag backlog (healthy events whose stored volume still
        # diverges from the recorder's). Only meaningful when the firmware
        # publishes a cumulative volume sensor — the caller gates on that.
        if not getattr(c, "volume_sensor", ""):
            continue
        try:
            rs = get_reconcile_state(db, c.circuit)
            sens = get_sensitivity_config(db, c.circuit)
            auto = True
            if sens and "recorder_reconcile_auto" in sens.keys() \
                    and sens["recorder_reconcile_auto"] is not None:
                auto = bool(sens["recorder_reconcile_auto"])
            reconcile[c.circuit] = {
                "auto": auto,
                "corrections": (rs["corrections"] if rs else 0) or 0,
                "flagged": (rs["flagged"] if rs else 0) or 0,
                "last_run": _fmt_local_ts(rs["last_run_at"] if rs else None, tz),
                "last_delta": (rs["last_delta_litres"] if rs else None),
                "backlog": count_flagged_backlog(db, c.circuit),
            }
        except Exception:
            reconcile[c.circuit] = None
    return {"schedules": schedules, "reconcile": reconcile}


# ------------------------------------------------------------------
# Valve control
# ------------------------------------------------------------------
@router.post("/valve/{circuit}/open")
async def valve_open(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    log.info(">>> valve_open called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)
    if not cfg or not cfg.valve_entity:
        return JSONResponse(
            {"status": "error",
             "message": f"No valve entity configured for circuit '{circuit}'. "
                        "Go to Settings → Advanced → Re-discover devices "
                        "(or re-run Setup if first install)."},
            status_code=404,
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
    circuit = resolve_circuit(circuit)
    log.info(">>> valve_close called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)
    if not cfg or not cfg.valve_entity:
        return JSONResponse(
            {"status": "error",
             "message": f"No valve entity configured for circuit '{circuit}'. "
                        "Go to Settings → Advanced → Re-discover devices "
                        "(or re-run Setup if first install)."},
            status_code=404,
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
    circuit = resolve_circuit(circuit)
    log.info(">>> fault_reset called for circuit=%s", circuit)
    orch = _orch(request)
    from ..device_discovery import load_circuit_entities
    from ..database import run_db
    entities = await run_db(load_circuit_entities, orch.db, circuit)  # dev46 (46a)
    entity_id = entities.get("fault_reset_button")
    if not entity_id:
        return JSONResponse(
            {"status": "error",
             "message": "Fault reset button not found for this circuit. "
                        "Re-run device discovery to map the entity."},
            status_code=404,
        )
    ok = await orch.ha.call_service("button", "press", {"entity_id": entity_id})
    if not ok:
        return JSONResponse(
            {"status": "error", "message": "HA button press failed — check device connectivity"},
            status_code=502,
        )
    return JSONResponse({"status": "reset"})


@router.post("/trickle/{circuit}/reset")
async def trickle_reset(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    log.info(">>> trickle_reset called for circuit=%s", circuit)
    orch = _orch(request)
    from ..device_discovery import load_circuit_entities
    from ..database import run_db
    entities = await run_db(load_circuit_entities, orch.db, circuit)  # dev46 (46a)
    entity_id = entities.get("trickle_reset_button")
    if not entity_id:
        return JSONResponse(
            {"status": "error",
             "message": "Trickle reset button not found for this circuit. "
                        "Re-run device discovery to map the entity."},
            status_code=404,
        )
    ok = await orch.ha.call_service("button", "press", {"entity_id": entity_id})
    if not ok:
        return JSONResponse(
            {"status": "error", "message": "HA button press failed — check device connectivity"},
            status_code=502,
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
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    circuit_cfg = orch._cfg.get_circuit(circuit)
    if not circuit_cfg:
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=404,
        )

    # Build allowlist from only the writable threshold roles for this circuit
    from ..device_discovery import load_circuit_entities
    from ..database import run_db
    entities = await run_db(load_circuit_entities, orch.db, circuit)  # dev46 (46a)
    allowed = {v for k, v in entities.items() if k in _THRESHOLD_ROLES and v}
    if entity_id not in allowed:
        # 400 not 403 — the request is well-formed, but the supplied
        # entity_id failed enum validation. This is input validation,
        # not an authorization check.
        return JSONResponse(
            {"status": "error", "message": "Entity not in allowed set for this circuit"},
            status_code=400,
        )

    # Runtime domain guard — only ESPHome number.* entities are accepted.
    # input_number.* helpers are NOT allowed in safety-critical firmware paths.
    if not entity_id.startswith("number."):
        return JSONResponse(
            {"status": "error",
             "message": "Only ESPHome number.* entities are accepted for threshold updates"},
            status_code=400,
        )

    ok = await orch.ha.set_number_value(entity_id, value)
    if not ok:
        return JSONResponse(
            {"status": "error", "message": "HA number update failed — check device connectivity"},
            status_code=502,
        )
    return JSONResponse({"status": "updated", "entity_id": entity_id, "value": value})


# ------------------------------------------------------------------
# Alert toggle
# ------------------------------------------------------------------
@router.post("/alert/{circuit}/{alert_type}/toggle")
async def alert_toggle(
    circuit: str, alert_type: str, request: Request,
    enabled: bool = Form(...),
):
    circuit = resolve_circuit(circuit)
    if alert_type not in VALID_ALERT_TYPES:
        return JSONResponse(
            {"status": "error", "message": f"Unknown alert type: {alert_type!r}"},
            status_code=400,
        )
    orch = _orch(request)
    circuit_cfg = orch._cfg.get_circuit(circuit)
    if not circuit_cfg:
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=404,
        )
    from ..device_discovery import load_circuit_entities
    from ..database import run_db
    entities = await run_db(load_circuit_entities, orch.db, circuit)  # dev46 (46a)
    role = f"alert_{alert_type}_switch"
    entity_id = entities.get(role)
    if not entity_id:
        return JSONResponse(
            {"status": "error",
             "message": f"Alert switch for '{alert_type}' not found for this circuit. "
                        "Re-run device discovery to map the entity."},
            status_code=404,
        )
    ok = await orch.ha.turn_on(entity_id) if enabled else await orch.ha.turn_off(entity_id)
    if not ok:
        return JSONResponse(
            {"status": "error", "message": "HA alert switch update failed — check device connectivity"},
            status_code=502,
        )

    # Update local alert_config only after HA confirms
    from ..database import set_alert_enabled
    await run_db(set_alert_enabled, orch.db,                  # dev46 (46a)
                 f"{alert_type}_{circuit}", enabled)

    return JSONResponse({"status": "updated", "enabled": enabled})


# ------------------------------------------------------------------
# Leak test — run now
# ------------------------------------------------------------------
@router.post("/leaktest/{circuit}/run")
async def leaktest_run(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    log.info(">>> leaktest_run called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)

    if not cfg:
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=404,
        )

    if not cfg.leak_test_switch:
        return JSONResponse(
            {"status": "error",
             "message": "No leak test switch configured. Go to "
                        "Settings → Advanced → Re-discover devices "
                        "(or re-run Setup if first install)."},
            status_code=404,
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
    circuit = resolve_circuit(circuit)
    log.info(">>> leaktest_abort called for circuit=%s", circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)

    if not cfg:
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=404,
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

    # Firmware owns the valve restore decision — leak_test_restore_main checks
    # !fault_main before reopening, so a concurrent safety fault keeps the valve
    # closed correctly. Sending an unconditional open here would bypass that guard.

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
    circuit = resolve_circuit(circuit)
    form = await request.form()
    orch = _orch(request)

    from ..database import run_db, upsert_leak_test_schedule
    # Bounded coercion: out-of-range form values (run_hour=99,
    # day_of_week=-1, etc.) fall back to the listed default rather
    # than being persisted as garbage. The numeric bounds match the
    # semantic ranges enforced by the scheduler:
    #   day_of_week    0..6   (Mon..Sun)
    #   week_of_month  1..5
    #   run_hour       0..23
    #   run_minute     0..59
    await run_db(                                             # dev46 (46a)
        upsert_leak_test_schedule,
        orch.db, circuit,
        enabled=form.get("enabled") == "on",
        auto_learn_hour=form.get("auto_learn_hour") == "on",
        frequency=form.get("frequency", "monthly"),
        day_of_week=coerce_int(form.get("day_of_week"), lo=0, hi=6, default=0),
        week_of_month=coerce_int(form.get("week_of_month"), lo=1, hi=5, default=1),
        run_hour=coerce_int(form.get("run_hour"), lo=0, hi=23, default=2),
        run_minute=coerce_int(form.get("run_minute"), lo=0, hi=59, default=0),
        notify_on_pass=form.get("notify_on_pass") == "on",
        notify_on_fail=form.get("notify_on_fail") == "on",
    )
    return ingress_redirect(request, "/device")


# ------------------------------------------------------------------
# Recorder volume reconciliation — apply the flagged backlog (Phase 3 §2)
# ------------------------------------------------------------------
@router.post("/reconcile/{circuit}/apply")
async def reconcile_apply(circuit: str, request: Request):
    """Flag-mode review→apply: correct every healthy event whose stored recorder value
    still diverges from its volume, from the STORED recorder value (no HA re-fetch).
    Runs under the write lock (serialized with recompute/reclassify)."""
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    from ..database import run_isolated_write
    from ..config import DB_PATH
    from ..recorder_reconcile import apply_flagged_backlog

    def _job(conn):
        return apply_flagged_backlog(conn, circuit)

    res = await run_isolated_write(DB_PATH, _job)
    log.info("[%s] recorder reconcile backlog applied: %s", circuit, res)
    return ingress_redirect(request, "/device")
