"""Settings router."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ._helpers import ingress_redirect

from ..config import SENSITIVITY_PRESETS
from ..database import get_data_retention, update_data_retention, get_home_profile

router = APIRouter(prefix="/settings")


def _orch(request: Request):
    return request.app.state.orchestrator


def _tmpl(request: Request):
    return request.app.state.templates


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
    orch = _orch(request)
    from ..database import (get_home_profile, get_sensitivity_config,
                            get_learning_config, get_alert_configs)
    from ..device_discovery import get_device_config

    profile = dict(get_home_profile(orch.db))

    # Fetch configurable device entities (number + select) from HA
    device_cfg = get_device_config(orch.db)
    prefix = device_cfg.get("esp_device_prefix", "") if device_cfg else ""
    try:
        device_entities = await orch.ha.get_device_configurable_entities(prefix)
    except Exception:
        device_entities = []

    # Group by circuit using the prefix — strip prefix then check suffix
    def circuit_of(entity_id: str) -> str:
        local = entity_id.split(".", 1)[1] if "." in entity_id else entity_id
        stem = local[len(prefix):] if local.startswith(prefix) else local
        for c in orch._cfg.circuits:
            if stem.endswith(f"_{c.circuit}") or stem.endswith(f"_{c.circuit.replace('_', '')}"):
                return c.circuit
        return "general"

    # Short labels and descriptions for known ESP entity name patterns.
    # Keys are matched against the entity stem (entity_id with prefix and
    # circuit suffix stripped, underscores replaced with spaces, lowercased).
    ENTITY_META = {
        "burst pipe flow threshold":   ("Burst Pipe Threshold",    "Flow rate that triggers an emergency shutoff. Raise if getting false alarms, lower for faster detection."),
        "leak test duration":          ("Leak Test Duration",       "How long the micro leak test monitors pressure after the valve closes. Longer = more sensitive."),
        "leak test pressure threshold":("Leak Test Sensitivity",    "Minimum pressure drop during the test that counts as a detected leak (PSI)."),
        "pressure drop threshold":     ("Pressure Drop Threshold",  "Rapid pressure drop (PSI in 5s) that triggers a burst-pipe safety fault."),
        "trickle flow alert duration": ("Trickle Alert Duration",   "How many minutes of continuous low flow before a trickle alert fires."),
        "trickle flow max threshold":  ("Trickle Max Flow",         "Upper flow limit for trickle detection (L/min). Flow above this is not considered a trickle."),
        "trickle flow min threshold":  ("Trickle Min Flow",         "Lower flow limit for trickle detection (L/min). Flow below this is ignored as sensor noise."),
        "alert high flow":             ("High Flow Alert",          "Enable or disable the high-flow alert independently of the safety shutoff."),
        "alert trickle":               ("Trickle Alert",            "Enable or disable trickle flow alerts."),
        "alert pressure drop":         ("Pressure Drop Alert",      "Enable or disable pressure-drop alerts."),
        "burst threshold":             ("Burst Pipe Threshold",     "Flow rate that triggers an emergency shutoff."),
    }

    def _enrich_entity(e: dict, prefix: str, circuit: str) -> dict:
        """Add short label and description to a device entity dict."""
        eid   = e["entity_id"]
        local = eid.split(".", 1)[1] if "." in eid else eid
        stem  = local[len(prefix):] if local.startswith(prefix) else local
        # Strip circuit suffix
        for suffix in (f"_{circuit}", f"_{circuit.replace('_', '')}"):
            if stem.endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        stem_readable = stem.replace("_", " ").strip().lower()

        for pattern, (label, desc) in ENTITY_META.items():
            if pattern in stem_readable:
                e = dict(e)
                e["short_label"] = f"{label} — {circuit.replace('_', ' ').title()}"
                e["description"] = desc
                return e

        # Fallback: clean up the raw friendly name
        name = e.get("friendly_name", stem_readable)
        # Strip device name prefix from friendly name (e.g. "ESP Water Shut off 3.4 ")
        if prefix:
            friendly_prefix = prefix.replace("_", " ").strip().title()
            name = name.replace(friendly_prefix, "").strip()
        # Strip circuit suffix (e.g. " - Main", " Main")
        for suffix in (f" - {circuit.title()}", f" {circuit.title()}",
                       f"- {circuit.title()}"):
            if name.endswith(suffix):
                name = name[: -len(suffix)].strip()
        e = dict(e)
        e["short_label"] = f"{name.title()} — {circuit.replace('_', ' ').title()}"
        e["description"] = ""
        return e

    entities_by_circuit: dict = {"general": []}
    for c in orch._cfg.circuits:
        entities_by_circuit[c.circuit] = []
    for e in device_entities:
        circ = circuit_of(e["entity_id"])
        enriched = _enrich_entity(e, prefix, circ)
        entities_by_circuit.setdefault(circ, []).append(enriched)

    circuits = []
    for circuit_cfg in orch._cfg.circuits:
        c = circuit_cfg.circuit
        sens = get_sensitivity_config(orch.db, c)
        learn = get_learning_config(orch.db, c)
        alerts = [dict(a) for a in get_alert_configs(orch.db, c)]
        training = (
            orch.training_manager.get_training_info(c)
            if orch.training_manager else {
                "state": "idle", "events_collected": 0,
                "minimum_events": 0, "days_remaining": 0,
                "percent_complete": 0,
            }
        )

        circuits.append({
            "circuit": c,
            "display_name": circuit_cfg.display_name,
            "circuit_type": circuit_cfg.circuit_type,
            "sensitivity": dict(sens) if sens else {},
            "learning": dict(learn) if learn else {},
            "alerts": alerts,
            "training": training,
            "device_entities": entities_by_circuit.get(c, []),
        })

    return _tmpl(request).TemplateResponse("settings.html", {
        "request": request,
        "profile": profile,
        "circuits": circuits,
        "general_entities": entities_by_circuit.get("general", []),
        "presets": SENSITIVITY_PRESETS,
        "retention": get_data_retention(orch.db),
        "profile": dict(get_home_profile(orch.db) or {}),
        "page": "settings",
    })


# ------------------------------------------------------------------
# Home profile
# ------------------------------------------------------------------
@router.post("/profile/update")
async def profile_update(request: Request):
    form = await request.form()
    orch = _orch(request)

    try:
        build_year = int(form.get("build_year", "") or 0) or None
    except (ValueError, TypeError):
        build_year = None

    from ..database import update_home_profile
    update_home_profile(
        orch.db,
        bathrooms_full=int(form.get("bathrooms_full", 1) or 1),
        bathrooms_half=int(form.get("bathrooms_half", 0) or 0),
        sqft=int(form.get("sqft", 0) or 0),
        floors=int(form.get("floors", 1) or 1),
        occupants=int(form.get("occupants", 2) or 2),
        build_year=build_year,
        supply_type=form.get("supply_type", "mains"),
        setup_complete=1,
    )
    return ingress_redirect(request, "/settings")


# ------------------------------------------------------------------
# Sensitivity
# ------------------------------------------------------------------
@router.post("/sensitivity/{circuit}/update")
async def sensitivity_update(circuit: str, request: Request):
    form = await request.form()
    orch = _orch(request)
    from ..database import upsert_sensitivity_config

    mode = form.get("mode", "simple")
    level = form.get("simple_level", "medium")
    preset = SENSITIVITY_PRESETS.get(level, SENSITIVITY_PRESETS["medium"])

    if mode == "simple":
        upsert_sensitivity_config(
            orch.db, circuit,
            mode=mode,
            simple_level=level,
            **preset,
        )
    else:
        # Advanced — read individual fields
        upsert_sensitivity_config(
            orch.db, circuit,
            mode=mode,
            simple_level="custom",
            pressure_drop_event_psi=float(form.get(
                "pressure_drop_event_psi", preset["pressure_drop_event_psi"])),
            min_event_duration_seconds=float(form.get(
                "min_event_duration_seconds", preset["min_event_duration_seconds"])),
            score_alert=float(form.get("score_alert", preset["score_alert"])),
            score_shutoff=float(form.get("score_shutoff", preset["score_shutoff"])),
            flow_tolerance_pct=float(form.get(
                "flow_tolerance_pct", preset["flow_tolerance_pct"])),
            duration_tolerance_pct=float(form.get(
                "duration_tolerance_pct", preset["duration_tolerance_pct"])),
            schedule_window_minutes=float(form.get(
                "schedule_window_minutes", preset["schedule_window_minutes"])),
            sustained_alert_minutes=float(form.get(
                "sustained_alert_minutes", preset["sustained_alert_minutes"])),
            max_shutoffs_per_12h=int(form.get(
                "max_shutoffs_per_12h", preset["max_shutoffs_per_12h"])),
        )

    # Refresh event detector thresholds
    if orch.event_detector:
        orch.event_detector.update_thresholds()

    return ingress_redirect(request, "/settings")


# ------------------------------------------------------------------
# Learning mode
# ------------------------------------------------------------------
@router.post("/learning/{circuit}/update")
async def learning_update(circuit: str, request: Request):
    form = await request.form()
    orch = _orch(request)
    from ..database import upsert_learning_config

    upsert_learning_config(
        orch.db, circuit,
        learning_mode=form.get("learning_mode", "adaptive"),
    )
    return ingress_redirect(request, "/settings")


# ------------------------------------------------------------------
# Recalibration
# ------------------------------------------------------------------
@router.post("/recalibrate/{circuit}")
async def recalibrate(circuit: str, request: Request):
    form = await request.form()
    orch = _orch(request)

    fixtures_changed = form.get("fixtures_changed") == "yes"
    occupants_changed = form.get("occupants_changed") == "yes"
    calibration_days = int(form.get("calibration_days", 14))

    if not orch.training_manager:
        return JSONResponse(
            {"error": "System still starting up — try again in a moment"},
            status_code=503,
        )

    if fixtures_changed:
        # Full recalibration — wipe events and restart from scratch
        await orch.training_manager.trigger_full_recalibration(
            circuit, calibration_days)
    else:
        # Partial — keep fixture signatures, reset behavioural patterns
        await orch.training_manager.trigger_partial_recalibration(circuit)
        if occupants_changed:
            # Household composition changed — reset to idle so
            # start_calibration can proceed, then begin a new run
            from ..database import upsert_training_state
            upsert_training_state(orch.db, circuit, state="idle",
                                  events_collected=0)
            await orch.training_manager.start_calibration(
                circuit, calibration_days)

    return ingress_redirect(request, "/settings")


@router.get("/recalibrate/{circuit}/suggest")
async def suggest_days(circuit: str, request: Request):
    """Return suggested calibration days based on home profile."""
    orch = _orch(request)
    from ..database import get_home_profile
    from ..config import compute_suggested_calibration_days

    profile = get_home_profile(orch.db)
    days, tier = compute_suggested_calibration_days(
        profile["bathrooms_full"] or 1,
        profile["bathrooms_half"] or 0,
        profile["floors"] or 1,
        profile["occupants"] or 2,
        profile["supply_type"] or "mains",
    )
    return JSONResponse({"suggested_days": days, "tier": tier})


# ------------------------------------------------------------------
# Alert enable/disable
# ------------------------------------------------------------------
@router.post("/alert/{circuit}/{alert_id}/toggle")
async def alert_toggle(circuit: str, alert_id: str, request: Request):
    form = await request.form()
    orch = _orch(request)
    enabled = form.get("enabled") == "true"
    from ..database import set_alert_enabled
    set_alert_enabled(orch.db, alert_id, enabled)
    return JSONResponse({"status": "updated", "enabled": enabled})


# ------------------------------------------------------------------
# Device entity updates (number and select entities on the ESP)
# ------------------------------------------------------------------
@router.post("/device-entity/update")
async def device_entity_update(request: Request):
    """Update a number or select entity value on the ESP via HA."""
    form = await request.form()
    orch = _orch(request)
    entity_id = form.get("entity_id", "").strip()
    value = form.get("value", "").strip()
    domain = entity_id.split(".", 1)[0] if entity_id else ""

    if not entity_id or not value:
        return JSONResponse(
            {"status": "error", "message": "entity_id and value required"},
            status_code=400,
        )

    if domain in ("number", "input_number"):
        try:
            ok = await orch.ha.set_number(entity_id, float(value))
        except ValueError:
            return JSONResponse(
                {"status": "error", "message": f"Invalid number: {value}"},
                status_code=400,
            )
    elif domain in ("select", "input_select"):
        ok = await orch.ha.set_select(entity_id, value)
    else:
        return JSONResponse(
            {"status": "error",
             "message": f"Unsupported entity domain: {domain}"},
            status_code=400,
        )

    return JSONResponse({
        "status": "ok" if ok else "error",
        "entity_id": entity_id,
        "value": value,
        "message": "Updated." if ok
                   else f"Failed to update {entity_id}. Check the addon log.",
    })


# ── Data retention ─────────────────────────────────────────────────────────

@router.post("/retention/update")
async def retention_update(request: Request):
    orch = _orch(request)
    form = await request.form()
    update_data_retention(
        orch.db,
        events_retain_years=int(form.get("events_retain_years", 1)),
        hourly_volume_retain_years=int(form.get("hourly_volume_retain_years", 2)),
        enabled=1 if form.get("enabled") == "1" else 0,
        auto_backup_enabled=1 if form.get("auto_backup_enabled") == "1" else 0,
        auto_backup_path=form.get("auto_backup_path",
                                   "/share/water_monitor_backups").strip(),
        auto_backup_day_of_week=int(form.get("auto_backup_day_of_week", 0)),
    )
    return ingress_redirect(request, "/settings")


@router.post("/retention/prune-now")
async def retention_prune_now(request: Request):
    orch = _orch(request)
    if not orch.data_pruner:
        return JSONResponse({"ok": False, "error": "Pruner not available"}, status_code=503)
    deleted = orch.data_pruner.prune_now()
    return JSONResponse({"ok": True, "deleted": deleted})


# ── Away mode ──────────────────────────────────────────────────────────────

@router.post("/away-mode/toggle")
async def away_mode_toggle(request: Request):
    orch = _orch(request)
    form = await request.form()
    enabled = form.get("enabled") == "1"
    # Await the call so the database write completes before we redirect
    # back to /settings — otherwise the rendered page can show stale state.
    await orch.set_away_mode(enabled)
    return ingress_redirect(request, "/settings")


# ── Mobile notify targets ──────────────────────────────────────────────────

@router.post("/mobile-notify/update")
async def mobile_notify_update(request: Request):
    orch = _orch(request)
    form = await request.form()
    targets = form.get("mobile_notify_targets", "").strip()
    orch.db.execute(
        "UPDATE home_profile SET mobile_notify_targets = ?, updated_at = datetime('now') WHERE id = 1",
        (targets,))
    orch.db.commit()
    return ingress_redirect(request, "/settings")


# ── HA Presence tracking ────────────────────────────────────────────────────

@router.post("/presence/update")
async def presence_update(request: Request):
    orch = _orch(request)
    form = await request.form()
    entities = form.get("ha_presence_entities", "").strip()
    away_state = form.get("ha_away_state", "not_home").strip()
    home_state = form.get("ha_home_state", "home").strip()
    orch.db.execute("""
        UPDATE home_profile
        SET ha_presence_entities = ?,
            ha_away_state        = ?,
            ha_home_state        = ?,
            updated_at = datetime('now')
        WHERE id = 1
    """, (entities, away_state, home_state))
    orch.db.commit()
    orch.reload_presence_watcher()
    return ingress_redirect(request, "/settings")

# ── Display units ─────────────────────────────────────────────────────────────

@router.post("/units/update")
async def units_update(request: Request):
    from ..units import FLOW_OPTIONS, PRESSURE_OPTIONS
    orch = _orch(request)
    form = await request.form()
    flow_key     = form.get("flow_unit", "L/min")
    pressure_key = form.get("pressure_unit", "psi")
    # Validate against known keys
    if flow_key not in FLOW_OPTIONS:
        flow_key = "L/min"
    if pressure_key not in PRESSURE_OPTIONS:
        pressure_key = "psi"
    orch.db.execute(
        "UPDATE home_profile SET flow_unit=?, pressure_unit=? WHERE id=1",
        (flow_key, pressure_key),
    )
    orch.db.commit()
    from ..units import invalidate_unit_cache
    invalidate_unit_cache()
    return ingress_redirect(request, "/settings")


@router.get("/units/detect")
async def units_detect(request: Request):
    """Query HA unit system and return the suggested unit keys."""
    from ..units import defaults_from_ha
    from fastapi.responses import JSONResponse
    orch = _orch(request)
    try:
        ha_units = await orch.ha.get_ha_unit_system()
        ha_vol   = ha_units.get("volume", "L")
        flow_key, pressure_key = defaults_from_ha(ha_vol)
        return JSONResponse({"ok": True,
                             "flow_unit": flow_key,
                             "pressure_unit": pressure_key,
                             "ha_volume": ha_vol})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
