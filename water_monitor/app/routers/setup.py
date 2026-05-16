"""
Setup wizard router.

Handles the first-run configuration flow:
  Step 1 — Search for the ESP device by name
  Step 2 — Confirm device (or pick from suggestions)
  Step 3 — Review discovered entities (fix any unmatched)
  Step 4 — Home details (bathrooms, floors, occupants, supply type)
  Step 5 — Complete setup and start calibration
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ._helpers import ingress_redirect

from ..device_discovery import (
    find_matching_devices,
    match_entities_to_roles,
    _resolve_labels_from_diagnostics,
    save_discovery,
    mark_setup_complete,
    load_circuit_entities,
    get_device_config,
    DiscoveryResult,
    DiscoveredDevice,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/setup")


def _orch(r: Request):
    return r.app.state.orchestrator


def _tmpl(r: Request):
    return r.app.state.templates


# ------------------------------------------------------------------
# Step 1 — landing page (search box)
# ------------------------------------------------------------------
@router.get("", response_class=HTMLResponse)   # matches /setup
@router.get("/", response_class=HTMLResponse)  # matches /setup/
async def setup_home(request: Request):
    orch = _orch(request)
    device_cfg = get_device_config(orch.db)
    initial_name = (
        device_cfg.get("esp_device_name") or orch._cfg.esp_device_name or ""
    )
    # Show the new/restore choice screen first (step 0)
    return _tmpl(request).TemplateResponse("setup.html", {
        "request": request,
        "step": 0,
        "initial_name": initial_name,
        "restore_error": request.query_params.get("restore_error"),
        "restore_ok": request.query_params.get("restore_ok"),
        "page": "setup",
    })


# ------------------------------------------------------------------
# Step 0a — user chose "New setup" → go to device search
# ------------------------------------------------------------------
@router.get("/new", response_class=HTMLResponse)
async def setup_new(request: Request):
    orch = _orch(request)
    device_cfg = get_device_config(orch.db)
    initial_name = (
        device_cfg.get("esp_device_name") or orch._cfg.esp_device_name or ""
    )
    return _tmpl(request).TemplateResponse("setup.html", {
        "request": request,
        "step": 1,
        "initial_name": initial_name,
        "page": "setup",
    })


# ------------------------------------------------------------------
# Step 0b — restore from backup file then continue to device search
# ------------------------------------------------------------------
@router.post("/restore")
async def setup_restore(request: Request):
    import json as _json
    from urllib.parse import quote_plus as _qp
    from fastapi import File, UploadFile
    from ..routers.backup import (
        QUICK_RESTORE_TABLES, QUICK_RESTORE_RECENT, MAX_BACKUP_BYTES,
    )
    from ..database import normalize_events_utc, dedup_events
    from ..restore_utils import safe_insert_rows, restore_circuit_labels

    orch = _orch(request)
    form = await request.form()
    file = form.get("file")
    import_settings = form.get("import_settings", "")
    import_history  = form.get("import_history",  "")

    if not file or not hasattr(file, "read"):
        return ingress_redirect(request,
            "/setup?restore_error=No+file+uploaded")

    try:
        # Enforce a 50 MB hard limit to prevent OOM on malformed uploads.
        raw = await file.read(MAX_BACKUP_BYTES + 1)
        if len(raw) > MAX_BACKUP_BYTES:
            return ingress_redirect(request,
                "/setup?restore_error=File+too+large+(max+50+MB)")
        payload = _json.loads(raw)
    except Exception as e:
        return ingress_redirect(request,
            f"/setup?restore_error={_qp(str(e))}")

    tables = payload.get("tables", {})
    if not tables:
        return ingress_redirect(request,
            "/setup?restore_error=No+table+data+found+in+backup")

    groups_to_restore = []
    if import_settings == "1":
        groups_to_restore += QUICK_RESTORE_TABLES
    if import_history == "1":
        groups_to_restore += QUICK_RESTORE_RECENT

    if not groups_to_restore:
        return ingress_redirect(request,
            "/setup?restore_error=No+import+options+selected")

    errors = []
    total  = 0
    db     = orch.db

    # PRAGMA foreign_keys must be set outside the transaction — SQLite ignores
    # it when a transaction is already open.  We disable it for the bulk
    # restore so that cross-table FK ordering (e.g. events → fixtures) does
    # not block the DELETE pass, then re-enable immediately after.
    db.execute("PRAGMA foreign_keys = OFF")
    # Wrap the entire restore in a single transaction so a partial failure
    # leaves the database unchanged rather than in a half-restored state.
    try:
        with db:
            for tbl in groups_to_restore:
                # Always clear the table first so the DB reflects the exact
                # state of the backup — stale rows from a prior restore cannot
                # bleed through when the backup has an empty array or the table
                # is absent from the backup entirely.
                db.execute(f"DELETE FROM {tbl}")
                rows = tables.get(tbl)
                if not rows:
                    continue
                total += safe_insert_rows(db, tbl, rows)

            # Normalize and deduplicate events after import so the DB is
            # consistent even if the backup contained pre-dedup duplicates
            # or mixed-timezone timestamps.
            if import_history == "1" and tables.get("events"):
                normalize_events_utc(db)
                removed = dedup_events(db)
                if removed:
                    log.warning(
                        "Setup restore: removed %d duplicate event(s) from backup",
                        removed,
                    )

            # Restore circuit display labels atomically with the table data.
            restore_circuit_labels(db, payload)
    except Exception as e:
        log.error("Setup restore failed — transaction rolled back: %s", e)
        errors.append("(transaction rolled back)")
        return ingress_redirect(request,
            f"/setup?restore_error={_qp(str(e))}")
    finally:
        db.execute("PRAGMA foreign_keys = ON")

    try:
        orch.reload_circuit_entities()
    except Exception as e:
        log.warning("Restore: reload_circuit_entities: %s", e)

    # Re-run unit auto-detection after restore.
    # The backup may contain flow_unit='L/min' (schema default) which would
    # overwrite the correctly auto-detected value from startup.  Re-running
    # here ensures the right units are active: if the backup had explicit
    # non-default units the skip condition in _init_display_units preserves
    # them; if it had defaults, detection runs again and picks the right units.
    try:
        await orch._init_display_units()
        from ..units import invalidate_unit_cache
        invalidate_unit_cache()
    except Exception as e:
        log.warning("Restore: unit re-detection failed (non-fatal): %s", e)

    if errors:
        return ingress_redirect(request,
            f"/setup?restore_error={_qp('Some tables failed: ' + ', '.join(errors))}")

    log.info("Backup restored — %d rows across %d tables", total,
             len(groups_to_restore))

    # Pull the saved device name so step 1 pre-fills it
    device_cfg   = get_device_config(orch.db)
    initial_name = (
        device_cfg.get("esp_device_name") or orch._cfg.esp_device_name or ""
    )

    return _tmpl(request).TemplateResponse("setup.html", {
        "request":      request,
        "step":         1,
        "initial_name": initial_name,
        "restore_ok":   f"Backup restored ({total} rows). "
                        "Now verify your device connection below.",
        "page":         "setup",
    })


# ------------------------------------------------------------------
# Step 1 POST — search for device
# ------------------------------------------------------------------
@router.post("/search")
async def setup_search(
    request: Request,
    device_name: str = Form(...),
):
    orch = _orch(request)

    # Persist the searched name
    orch.db.execute("""
        UPDATE device_config SET esp_device_name = ?, updated_at = datetime('now')
        WHERE id = 1
    """, (device_name.strip(),))
    orch.db.commit()

    try:
        devices = await orch.ha.get_devices()
    except Exception as e:
        log.error("Failed to fetch device registry: %s", e)
        return _tmpl(request).TemplateResponse("setup.html", {
            "request": request,
            "step": 1,
            "initial_name": device_name,
            "error": f"Could not reach Home Assistant: {e}",
            "page": "setup",
        })

    exact, suggestions = find_matching_devices(devices, device_name)

    if exact:
        # Auto-proceed to entity discovery for the exact match
        return ingress_redirect(request, f"/setup/discover/{exact.id}")

    if not suggestions:
        return _tmpl(request).TemplateResponse("setup.html", {
            "request": request,
            "step": 1,
            "initial_name": device_name,
            "error": "No devices found. Check the device name and try again.",
            "page": "setup",
        })

    # Show suggestions
    return _tmpl(request).TemplateResponse("setup.html", {
        "request": request,
        "step": 2,
        "initial_name": device_name,
        "suggestions": [_device_to_dict(d) for d in suggestions],
        "page": "setup",
    })


# ------------------------------------------------------------------
# Step 2 — user picks a device from suggestions
# ------------------------------------------------------------------
@router.post("/select")
async def setup_select(
    request: Request,
    device_id: str = Form(...),
):
    return ingress_redirect(request, f"/setup/discover/{device_id}")


# ------------------------------------------------------------------
# Step 3 — discover entities for the selected device
# ------------------------------------------------------------------
@router.get("/discover/{device_id}", response_class=HTMLResponse)
async def setup_discover(device_id: str, request: Request):
    orch = _orch(request)
    circuits = [c.circuit for c in orch._cfg.circuits]

    try:
        devices = await orch.ha.get_devices()
        entities = await orch.ha.get_entity_registry()
    except Exception as e:
        log.error("Registry fetch failed: %s", e)
        return ingress_redirect(request, "/setup?error=registry_failed")

    # Find the selected device
    device_raw = next((d for d in devices if d.get("id") == device_id), None)
    if not device_raw:
        return ingress_redirect(request, "/setup")

    from ..device_discovery import _to_device
    device = _to_device(device_raw)

    # Resolve display labels from v3.6+ diagnostic sensors before regex matching
    # so non-default circuit names (e.g. "Zone A") are recognised in tier 1.
    diag_labels = await _resolve_labels_from_diagnostics(orch.ha, entities)
    if diag_labels:
        from ..database import upsert_circuit_label
        for cid, lbl in diag_labels.items():
            upsert_circuit_label(orch.db, cid, lbl)
        orch.reload_circuit_labels()

    circuit_matches, prefix = match_entities_to_roles(
        device_id, entities, circuits, labels=diag_labels)

    # Get all device entities for fallback dropdowns
    device_entity_list = [
        e for e in entities if e.get("device_id") == device_id
    ]

    # Temporarily save so the user can override individual entities
    from ..device_discovery import DiscoveryResult
    result = DiscoveryResult(
        device=device,
        circuit_matches=circuit_matches,
        esp_device_prefix=prefix,
    )
    save_discovery(orch.db, result)

    # Convert matches to serialisable form
    circuit_data = {}
    for circuit, matches in circuit_matches.items():
        circuit_data[circuit] = [
            {
                "role": m.role,
                "entity_id": m.entity_id,
                "original_name": m.original_name,
                "matched": m.matched,
                "confidence": m.confidence,
                "optional": m.optional,
                "label": _role_label(m.role),
            }
            for m in matches
        ]

    from ..device_discovery import MIN_FIRMWARE_VERSION
    min_fw = ".".join(str(x) for x in MIN_FIRMWARE_VERSION)

    return _tmpl(request).TemplateResponse("setup.html", {
        "request": request,
        "step": 3,
        "device": _device_to_dict(device),
        "circuit_data": circuit_data,
        "device_entities": [
            {
                "entity_id": e.get("entity_id", ""),
                "name": e.get("original_name") or e.get("name") or e.get("entity_id", ""),
                "domain": (e.get("entity_id") or "").split(".")[0],
            }
            for e in device_entity_list
        ],
        "all_matched": result.all_matched,
        "unmatched_roles": result.unmatched_roles,
        "prefix": prefix,
        "min_fw": min_fw,
        "page": "setup",
    })


# ------------------------------------------------------------------
# Step 3 POST — save entity overrides and advance to home details
# ------------------------------------------------------------------
@router.post("/confirm/{device_id}")
async def setup_confirm(device_id: str, request: Request):
    orch = _orch(request)
    form = await request.form()

    # Update any manually-overridden entity IDs.
    # Validate circuit and role against the known ROLE_PATTERNS allowlist so
    # arbitrary form fields cannot inject unrecognised column names or circuits.
    from ..device_discovery import ROLE_PATTERNS
    _valid_circuits = set(ROLE_PATTERNS.keys())
    # Union of all role names across every circuit type
    _valid_roles = {r for roles in ROLE_PATTERNS.values() for r in roles}

    for key, value in form.items():
        # Form fields are named: circuit__role  e.g. main__flow_sensor
        if "__" in key and value:
            parts = key.split("__", 1)
            if len(parts) == 2:
                circuit, role = parts
                if circuit not in _valid_circuits or role not in _valid_roles:
                    log.warning(
                        "setup_confirm: ignoring unknown circuit/role pair "
                        "%r/%r", circuit, role)
                    continue
                orch.db.execute("""
                    INSERT INTO circuit_entity_map (circuit, role, entity_id, confirmed)
                    VALUES (?, ?, ?, 1)
                    ON CONFLICT (circuit, role)
                    DO UPDATE SET entity_id = excluded.entity_id,
                                  confirmed = 1
                """, (circuit, role, value.strip()))

    orch.db.commit()
    log.info("Entity mapping confirmed for device %s — proceeding to circuit names", device_id)
    return ingress_redirect(request, "/setup/circuit-names")


# ------------------------------------------------------------------
# Step 3b — circuit display names
# ------------------------------------------------------------------

@router.get("/circuit-names", response_class=HTMLResponse)
async def setup_circuit_names(request: Request):
    """Step 3b — let the user name their circuits before configuring home details."""
    orch = _orch(request)
    from ..device_discovery import CIRCUIT_DISPLAY_DEFAULTS
    circuits = [
        {
            "circuit":      c.circuit,
            "display_name": c.label,
        }
        for c in orch._cfg.circuits
    ]
    return _tmpl(request).TemplateResponse("setup.html", {
        "request":  request,
        "step":     "3b",
        "circuits": circuits,
        "page":     "setup",
    })


@router.post("/circuit-names")
async def setup_circuit_names_save(request: Request):
    """Save circuit display names and advance to unit selection."""
    from ..circuit_compat import validate_display_name
    from ..database import upsert_circuit_label
    orch = _orch(request)
    form = await request.form()

    errors = []
    for c in orch._cfg.circuits:
        raw = form.get(f"label_{c.circuit}", "").strip()
        if not raw:
            # Keep existing label if input was blank
            continue
        try:
            display_name = validate_display_name(raw)
        except ValueError as exc:
            errors.append(f"{c.circuit}: {exc}")
            continue
        upsert_circuit_label(orch.db, c.circuit, display_name)

    if errors:
        circuits = [
            {"circuit": c.circuit, "display_name": c.label}
            for c in orch._cfg.circuits
        ]
        return _tmpl(request).TemplateResponse("setup.html", {
            "request":  request,
            "step":     "3b",
            "circuits": circuits,
            "errors":   errors,
            "page":     "setup",
        })

    orch.reload_circuit_labels()
    log.info("Setup: circuit names saved")
    return ingress_redirect(request, "/setup/units")


# ------------------------------------------------------------------
# Step 4 — home details form
# ------------------------------------------------------------------


@router.get("/units", response_class=HTMLResponse)
async def setup_units(request: Request):
    """Step 4 — choose preferred display units."""
    orch = _orch(request)
    from ..database import get_home_profile
    from ..units import load_unit_context, FLOW_OPTIONS, PRESSURE_OPTIONS
    profile = dict(get_home_profile(orch.db) or {})
    uc = load_unit_context(orch.db)
    return _tmpl(request).TemplateResponse("setup.html", {
        "request":          request,
        "step":             4,
        "profile":          profile,
        "flow_key":         uc["flow_key"],
        "pressure_key":     uc["pressure_key"],
        "flow_options":     list(FLOW_OPTIONS.keys()),
        "pressure_options": list(PRESSURE_OPTIONS.keys()),
        "flow_option_labels":     {k: v["label"] for k, v in FLOW_OPTIONS.items()},
        "pressure_option_labels": {k: v["label"] for k, v in PRESSURE_OPTIONS.items()},
        "ha_detected":      True,   # we always pre-fill from HA detection
    })


@router.post("/units")
async def setup_units_save(request: Request):
    """Save display unit preferences and advance to home details."""
    from ..units import FLOW_OPTIONS, PRESSURE_OPTIONS, invalidate_unit_cache
    orch = _orch(request)
    form = await request.form()
    flow_key     = form.get("flow_unit", "L/min")
    pressure_key = form.get("pressure_unit", "psi")
    if flow_key     not in FLOW_OPTIONS:     flow_key     = "L/min"
    if pressure_key not in PRESSURE_OPTIONS: pressure_key = "psi"
    orch.db.execute(
        "UPDATE home_profile SET flow_unit=?, pressure_unit=? WHERE id=1",
        (flow_key, pressure_key),
    )
    orch.db.commit()
    invalidate_unit_cache()
    log.info("Setup: units saved — flow=%s pressure=%s", flow_key, pressure_key)
    return ingress_redirect(request, "/setup/home")

@router.get("/home", response_class=HTMLResponse)
async def setup_home_details(request: Request):
    orch = _orch(request)
    from ..database import get_home_profile
    profile = dict(get_home_profile(orch.db) or {})
    return _tmpl(request).TemplateResponse("setup.html", {
        "request": request,
        "step": 5,
        "profile": profile,
        "page": "setup",
    })


@router.post("/home")
async def setup_home_details_save(request: Request):
    orch = _orch(request)
    form = await request.form()

    from ..database import update_home_profile
    from ..config import compute_suggested_calibration_days

    bathrooms_full = int(form.get("bathrooms_full", 1) or 1)
    bathrooms_half = int(form.get("bathrooms_half", 0) or 0)
    floors         = int(form.get("floors", 1) or 1)
    occupants      = int(form.get("occupants", 2) or 2)
    supply_type    = form.get("supply_type", "mains")
    build_year_raw = form.get("build_year", "")
    build_year     = int(build_year_raw) if build_year_raw.strip().isdigit() else None
    sqft           = int(form.get("sqft", 0) or 0)

    update_home_profile(
        orch.db,
        bathrooms_full=bathrooms_full,
        bathrooms_half=bathrooms_half,
        sqft=sqft,
        floors=floors,
        occupants=occupants,
        build_year=build_year,
        supply_type=supply_type,
        setup_complete=1,
    )

    # Mark setup complete and reload entity IDs into live circuit configs
    mark_setup_complete(orch.db)
    orch.reload_circuit_entities()

    # Activate event detection now that entity IDs are known.
    # At startup, event_detector.setup() is skipped when setup is not yet
    # complete.  Calling it here ensures the first real day of monitoring
    # is not lost — without this, subscriptions stay at 0 until restart.
    if orch.event_detector:
        try:
            orch.event_detector.setup()
            log.info("Event detection activated after setup wizard completion")
        except Exception as e:
            log.warning("Event detector setup failed (non-fatal): %s", e)

    # Fetch midnight volume baselines from HA history so the dashboard
    # shows accurate daily/weekly totals from the first page load.
    try:
        await orch._init_volume_baselines()
    except Exception as e:
        log.warning("Volume baseline init after setup failed (non-fatal): %s", e)

    # Sync presence watcher state so away mode is correct immediately
    # (without this it waits for the next state_changed event from HA).
    if orch._presence_watcher:
        try:
            await orch._presence_watcher.sync_initial_state()
        except Exception as e:
            log.warning("Presence watcher sync after setup failed (non-fatal): %s", e)

    # Compute calibration duration from the home profile and auto-start
    # training for every circuit.  The user sees the result on step 5.
    cal_days, cal_reason = compute_suggested_calibration_days(
        bathrooms_full=bathrooms_full,
        bathrooms_half=bathrooms_half,
        floors=floors,
        occupants=occupants,
        supply_type=supply_type,
    )

    if orch.training_manager:
        for circuit_cfg in orch._cfg.circuits:
            try:
                await orch.training_manager.start_calibration(
                    circuit_cfg.circuit, calibration_days=cal_days)
                log.info("[%s] calibration started — %d days (%s)",
                         circuit_cfg.circuit, cal_days, cal_reason)
            except Exception as e:
                log.warning("[%s] could not start calibration: %s",
                            circuit_cfg.circuit, e)

    log.info("Setup complete — calibration started (%d days)", cal_days)
    return ingress_redirect(
        request,
        f"/setup/complete?cal_days={cal_days}&cal_reason={cal_reason}"
    )


# ------------------------------------------------------------------
# Step 5 — complete
# ------------------------------------------------------------------
@router.get("/complete", response_class=HTMLResponse)
async def setup_complete(request: Request):
    orch = _orch(request)
    cfg = get_device_config(orch.db)

    # Pick up calibration info passed as query params from the /home POST,
    # or read from DB if the user refreshes the page.
    cal_days   = request.query_params.get("cal_days")
    cal_reason = request.query_params.get("cal_reason", "")

    training_info = {}
    for circuit_cfg in orch._cfg.circuits:
        if orch.training_manager:
            info = orch.training_manager.get_training_info(circuit_cfg.circuit)
            training_info[circuit_cfg.circuit] = info
            if not cal_days and info.get("calibration_days"):
                cal_days = info["calibration_days"]

    return _tmpl(request).TemplateResponse("setup.html", {
        "request":       request,
        "step":          6,
        "device_name":   cfg.get("ha_device_name") if cfg else "your device",
        "circuits": [
            {
                "circuit":      c.circuit,
                "display_name": c.label,
                "configured":   c.is_fully_configured,
                "training":     training_info.get(c.circuit, {}),
            }
            for c in orch._cfg.circuits
        ],
        "cal_days":   (int(cal_days) if str(cal_days).isdigit() else 14) if cal_days else 14,
        "cal_reason": cal_reason,
        "page":       "setup",
    })


# ------------------------------------------------------------------
# API — re-run discovery for one circuit (after manual override)
# ------------------------------------------------------------------
@router.post("/api/rediscover/{device_id}/{circuit}")
async def rediscover_circuit(device_id: str, circuit: str, request: Request):
    from ..circuit_compat import resolve_circuit
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    try:
        entities = await orch.ha.get_entity_registry()
        diag_labels = await _resolve_labels_from_diagnostics(orch.ha, entities)
        circuit_matches, _ = match_entities_to_roles(
            device_id, entities, [circuit], labels=diag_labels)
        matches = circuit_matches.get(circuit, [])
        return JSONResponse({
            "matches": [
                {"role": m.role, "entity_id": m.entity_id,
                 "matched": m.matched}
                for m in matches
            ]
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _device_to_dict(d: DiscoveredDevice) -> Dict[str, Any]:
    return {
        "id": d.id,
        "name": d.display_name,
        "model": d.model or "",
        "manufacturer": d.manufacturer or "",
        "is_esphome": d.is_esphome,
        "sw_version": d.sw_version or "",
        "firmware_ok": d.firmware_ok,
    }


def _role_label(role: str) -> str:
    labels = {
        "flow_sensor":             "Flow Rate Sensor",
        "pressure_fast_sensor":    "Pressure (Fast)",
        "pressure_avg_sensor":     "Pressure (Averaged)",
        "flow_onset_sensor":       "Flow Pulse Onset",
        "valve_entity":            "Water Valve",
        "fault_sensor":            "Safety Fault",
        "fault_reason_sensor":     "Fault Reason",
        "trickle_sensor":          "Trickle Flow Alert",
        "leak_test_sensor":        "Leak Test Active",
        "leak_test_switch":          "Leak Test Switch",
        "leak_test_result_sensor":   "Leak Test Result",
        "leak_test_duration_sensor": "Leak Test Duration",
        "volume_sensor":           "Volume Total",
    }
    return labels.get(role, role.replace("_", " ").title())

