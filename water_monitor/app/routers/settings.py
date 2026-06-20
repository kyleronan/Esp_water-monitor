"""Settings router."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ._helpers import coerce_int, ingress_redirect

from ..circuit_compat import resolve_circuit
from ..config import SENSITIVITY_PRESETS
from ..database import get_data_retention, update_data_retention

log = logging.getLogger(__name__)

router = APIRouter(prefix="/settings")


def _orch(request: Request):
    return request.app.state.orchestrator


def _tmpl(request: Request):
    return request.app.state.templates


def _fmt_local_ts(iso, tz) -> "str | None":
    """ISO-UTC timestamp → 'YYYY-MM-DD HH:MM' in the home timezone (or UTC if none).
    Returns None for blank/unparseable input."""
    if not iso:
        return None
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if tz is not None:
        try:
            dt = dt.astimezone(tz)
        except Exception:
            pass
    return dt.strftime("%Y-%m-%d %H:%M")


def _localize_validation(dv: dict, tz) -> dict:
    """Shallow-copy a detector-validation report with local-time strings added so the
    Settings panel can list the flagged events: ``validated_at_local`` plus a
    ``time_local`` on each leak-safety suspect and gap-sentinel event. Never mutates
    the cached/stored report dict."""
    out = dict(dv)
    out["validated_at_local"] = _fmt_local_ts(dv.get("validated_at"), tz)

    def _stamp(items):
        return [{**it, "time_local": _fmt_local_ts(it.get("start_ts"), tz)}
                for it in (items or [])]

    out["suspect_zeroings"] = _stamp(dv.get("suspect_zeroings"))
    unf = dv.get("unflagged_no_flow") or {}
    out["unflagged_no_flow"] = {**unf, "events": _stamp(unf.get("events"))}
    return out


def _anomaly_shutoff_ready(sdict: dict) -> bool:
    """Display helper (Phase 2.3): is auto-shutoff currently ARMED, or held back
    (degraded to notify) until the baseline earns trust? Mirrors the feature_extractor
    guardrails — fit from enough events AND seasoned long enough. Display-only; the
    real gate lives in _process."""
    from datetime import datetime, timezone
    from ..anomaly_baseline import MIN_N_FOR_SHUTOFF, MIN_LIVE_DAYS_FOR_SHUTOFF
    n = sdict.get("baseline_anomaly_n")
    if n is None or n < MIN_N_FOR_SHUTOFF:
        return False
    ts = sdict.get("baseline_computed_at")
    if not ts:
        return False
    try:
        frozen = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False
    if frozen.tzinfo is None:
        frozen = frozen.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - frozen).days >= MIN_LIVE_DAYS_FOR_SHUTOFF


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
    orch = _orch(request)
    from ..database import (get_home_profile, get_sensitivity_config,
                            get_alert_configs)
    from ..device_discovery import get_device_config



    # Fetch configurable device entities (number + select) from HA
    device_cfg = get_device_config(orch.db)
    prefix = device_cfg.get("esp_device_prefix", "") if device_cfg else ""
    try:
        device_entities = await orch.ha.get_device_configurable_entities(prefix)
    except Exception:
        device_entities = []

    # Legacy entity_id suffixes per stable circuit ID.
    # Firmware entity_ids still end with _main / _irrigation / _irr even after
    # the Python stable IDs were renamed to circuit_1 / circuit_2.
    _LEGACY_SUFFIXES: dict = {
        "circuit_1": ("main",),
        "circuit_2": ("irrigation", "irr"),
    }

    # Group by circuit using the prefix — strip prefix then check suffix.
    # Checks canonical suffixes (_circuit_1, _circuit1) and legacy aliases
    # (_main, _irrigation, _irr) so firmware entities are grouped correctly.
    def circuit_of(entity_id: str) -> str:
        local = entity_id.split(".", 1)[1] if "." in entity_id else entity_id
        stem = local[len(prefix):] if local.startswith(prefix) else local
        for c in orch._cfg.circuits:
            canonical = (f"_{c.circuit}", f"_{c.circuit.replace('_', '')}")
            legacy = tuple(f"_{s}" for s in _LEGACY_SUFFIXES.get(c.circuit, ()))
            if any(stem.endswith(sfx) for sfx in canonical + legacy):
                return c.circuit
        return "general"

    # Load unit context once so descriptions and state values are shown in
    # the user's chosen units (e.g. gal/min instead of L/min).
    from ..units import load_unit_context
    _uc              = load_unit_context(orch.db)
    _flow_label      = _uc["flow_unit"]       # e.g. "gal/min"
    _flow_factor     = _uc["flow_factor"]     # multiply L/min → display
    _pressure_label  = _uc["pressure_unit"]   # e.g. "bar"
    _pressure_factor = _uc["pressure_factor"] # multiply PSI → display

    # Entity patterns that carry flow or pressure values (internal L/min / PSI).
    _FLOW_PATTERNS     = {"burst pipe flow threshold", "burst threshold",
                          "trickle flow max threshold", "trickle flow min threshold"}
    _PRESSURE_PATTERNS = {"leak test pressure threshold", "pressure drop threshold"}

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
        """Add short label, unit-converted description and state value."""
        eid   = e["entity_id"]
        local = eid.split(".", 1)[1] if "." in eid else eid
        stem  = local[len(prefix):] if local.startswith(prefix) else local
        # Strip circuit suffix — check canonical names and legacy firmware aliases
        _strip_candidates = (
            (f"_{circuit}", f"_{circuit.replace('_', '')}")
            + tuple(f"_{s}" for s in _LEGACY_SUFFIXES.get(circuit, ()))
        )
        for suffix in _strip_candidates:
            if stem.endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        stem_readable = stem.replace("_", " ").strip().lower()

        for pattern, (label, desc) in ENTITY_META.items():
            if pattern in stem_readable:
                e = dict(e)
                e["short_label"] = f"{label} — {circuit.replace('_', ' ').title()}"
                # Replace hardcoded unit strings in descriptions
                e["description"] = (desc
                    .replace("(L/min)", f"({_flow_label})")
                    .replace("L/min",   _flow_label)
                    .replace("(PSI)",   f"({_pressure_label})")
                    .replace("PSI",     _pressure_label))
                # Pre-convert numeric state to display units; also convert
                # min/max/step so the HTML input constraints match display units.
                # native_step is preserved so the POST handler can round to it.
                if pattern in _FLOW_PATTERNS:
                    e["unit_type"]   = "flow"
                    e["unit"]        = _flow_label
                    e["native_step"] = e.get("step")
                    try:
                        e["state"] = round(float(e["state"]) * _flow_factor, 3)
                    except (TypeError, ValueError):
                        pass
                    for attr in ("min", "max", "step"):
                        if e.get(attr) is not None:
                            try:
                                e[attr] = round(float(e[attr]) * _flow_factor, 3)
                            except (TypeError, ValueError):
                                pass
                elif pattern in _PRESSURE_PATTERNS:
                    e["unit_type"]   = "pressure"
                    e["unit"]        = _pressure_label
                    e["native_step"] = e.get("step")
                    try:
                        e["state"] = round(float(e["state"]) * _pressure_factor, 3)
                    except (TypeError, ValueError):
                        pass
                    for attr in ("min", "max", "step"):
                        if e.get(attr) is not None:
                            try:
                                e[attr] = round(float(e[attr]) * _pressure_factor, 3)
                            except (TypeError, ValueError):
                                pass
                else:
                    e["unit_type"]   = None
                    e["native_step"] = e.get("step")
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
        e["unit_type"]   = None
        e["native_step"] = e.get("step")
        return e

    # Only render number.* entities — select.* support requires explicit mutable
    # select roles and a separate hardened endpoint; exclude them here so the
    # template never calls the number-only device-entity/update with a select.
    number_entities = [
        e for e in device_entities
        if (e.get("entity_id") or "").split(".")[0] == "number"
    ]

    entities_by_circuit: dict = {"general": []}
    for c in orch._cfg.circuits:
        entities_by_circuit[c.circuit] = []
    for e in number_entities:
        circ = circuit_of(e["entity_id"])
        enriched = _enrich_entity(e, prefix, circ)
        entities_by_circuit.setdefault(circ, []).append(enriched)

    from ..event_rules import get_home_timezone
    from ..rule_calibration import get_rule_calibration_meta
    from ..artifact_calibration import get_artifact_calibration_meta
    from ..detector_validation import load_validation_report
    from ..anomaly_baseline import MIN_N_FOR_SHUTOFF, MIN_LIVE_DAYS_FOR_SHUTOFF
    _home_tz = get_home_timezone()
    circuits = []
    for circuit_cfg in orch._cfg.circuits:
        c = circuit_cfg.circuit
        sens = get_sensitivity_config(orch.db, c)
        sdict = dict(sens) if sens else {}
        alerts = [dict(a) for a in get_alert_configs(orch.db, c)]
        training = (
            orch.training_manager.get_training_info(c)
            if orch.training_manager else {
                "state": "idle", "events_collected": 0,
                "minimum_events": 0, "days_remaining": 0,
                "percent_complete": 0,
            }
        )
        cal_meta = get_rule_calibration_meta(orch.db, c)
        art_meta = get_artifact_calibration_meta(orch.db, c)
        dv = load_validation_report(orch.db, c)

        from ..database import get_active_exclusion_window, get_valve_type
        circuits.append({
            "circuit": c,
            "display_name": circuit_cfg.label,
            "circuit_type": circuit_cfg.circuit_type,
            "valve_type": get_valve_type(orch.db, c),
            "sensitivity": sdict,
            # Phase 2.3 anomaly response level + whether auto-shutoff is currently
            # armed or held back (degraded to notify until the baseline earns trust).
            "anomaly_response": sdict.get("anomaly_response") or "notify",
            "anomaly_shutoff_ready": _anomaly_shutoff_ready(sdict),
            "anomaly_shutoff_min_n": MIN_N_FOR_SHUTOFF,
            "anomaly_shutoff_min_days": MIN_LIVE_DAYS_FOR_SHUTOFF,
            # Phase 3 §2 — recorder volume reconciliation. Auto-correct is the default
            # (NULL ⇒ on); the toggle only renders when a cumulative volume sensor exists.
            "has_volume_sensor": bool(getattr(circuit_cfg, "volume_sensor", "")),
            "recorder_reconcile_auto": (
                0 if sdict.get("recorder_reconcile_auto") == 0 else 1),
            "alerts": alerts,
            "training": training,
            # When the last learning period completed (UTC→local). After activation
            # this is the go-live time; while 'labelling' it's when learning ended.
            "last_completed": _fmt_local_ts(
                (training or {}).get("completed_at"), _home_tz),
            # When the per-home rule reference was last frozen (activation/retrain).
            "calibration": {
                "locked_at": _fmt_local_ts(cal_meta["locked_at"], _home_tz),
                "source": cal_meta["source"],
                "fit_count": cal_meta["fit_count"],
            } if cal_meta else None,
            # Phase 2.4 artifact-detector calibration lock status (Dev Tools).
            "artifact_calibration": {
                "locked_at": _fmt_local_ts(art_meta["locked_at"], _home_tz),
                "source": art_meta["source"],
                "count": art_meta["count"],
                "detectors": art_meta["detectors"],
            } if art_meta else None,
            # P6 detector self-validation against HA history (Dev Tools, diagnostic).
            "detector_validation": _localize_validation(dv, _home_tz) if dv else None,
            "device_entities": entities_by_circuit.get(c, []),
            "active_exclusion": get_active_exclusion_window(orch.db, c),
        })

    # MQTT status for the Integrations section status pill
    mqtt_status = None
    fp = getattr(orch, "_fixture_publisher", None)
    if fp is not None:
        mqtt_status = fp.status()

    from ..fixtures import (CIRCUIT_TYPES, CIRCUIT_TYPE_LABELS, CIRCUIT_TYPE_HELP,
                            ZONE_ONLY_ALERT_TYPES,
                            VALVE_TYPES, VALVE_TYPE_LABELS, VALVE_TYPE_HELP)
    from ..config import DEV_TOOLS
    return _tmpl(request).TemplateResponse("settings.html", {
        "request": request,
        "dev_tools": DEV_TOOLS,
        "profile": dict(get_home_profile(orch.db) or {}),
        "circuits": circuits,
        "general_entities": entities_by_circuit.get("general", []),
        "presets": SENSITIVITY_PRESETS,
        "retention": get_data_retention(orch.db),
        "mqtt_status": mqtt_status,
        "circuit_types": CIRCUIT_TYPES,
        "circuit_type_labels": CIRCUIT_TYPE_LABELS,
        "circuit_type_help": CIRCUIT_TYPE_HELP,
        "valve_types": VALVE_TYPES,
        "valve_type_labels": VALVE_TYPE_LABELS,
        "valve_type_help": VALVE_TYPE_HELP,
        "zone_only_alert_types": ZONE_ONLY_ALERT_TYPES,
        "page": "settings",
    })


# ------------------------------------------------------------------
# Home profile
# ------------------------------------------------------------------
@router.post("/profile/update")
async def profile_update(request: Request):
    form = await request.form()
    orch = _orch(request)

    # Bounds match the setup wizard's home-details form (see setup.py).
    # Out-of-range values fall back to the listed default instead of
    # poisoning the DB; build_year is Optional[int] so a blank field
    # stays None.
    build_year_raw = form.get("build_year", "").strip()
    if build_year_raw:
        build_year = coerce_int(build_year_raw, lo=1800, hi=2100, default=0) or None
    else:
        build_year = None

    from ..database import update_home_profile
    update_home_profile(
        orch.db,
        bathrooms_full=coerce_int(form.get("bathrooms_full"), lo=0, hi=20, default=1),
        bathrooms_half=coerce_int(form.get("bathrooms_half"), lo=0, hi=20, default=0),
        sqft=coerce_int(form.get("sqft"),                     lo=0, hi=100000, default=0),
        floors=coerce_int(form.get("floors"),                 lo=1, hi=10, default=1),
        occupants=coerce_int(form.get("occupants"),           lo=1, hi=30, default=2),
        build_year=build_year,
        supply_type=form.get("supply_type", "mains"),
        setup_complete=1,
    )
    return ingress_redirect(request, "/settings#profile")


# ------------------------------------------------------------------
# Sensitivity
# ------------------------------------------------------------------
@router.post("/sensitivity/{circuit}/update")
async def sensitivity_update(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
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

    return ingress_redirect(request, f"/settings#circuit-{circuit}")


# ------------------------------------------------------------------
# Anomaly response (Phase 2.3)
# ------------------------------------------------------------------
_ANOMALY_RESPONSE_LEVELS = {"off", "notify", "notify_shutoff_severe", "shutoff_any"}


@router.post("/anomaly/{circuit}/update")
async def anomaly_update(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    form = await request.form()
    orch = _orch(request)
    from ..database import upsert_sensitivity_config

    level = form.get("anomaly_response", "notify")
    if level not in _ANOMALY_RESPONSE_LEVELS:
        level = "notify"
    upsert_sensitivity_config(orch.db, circuit, anomaly_response=level)
    return ingress_redirect(request, f"/settings#circuit-{circuit}")


@router.post("/reconcile/{circuit}/update")
async def reconcile_update(circuit: str, request: Request):
    """Phase 3 §2 — per-circuit recorder reconciliation mode. Checkbox present ⇒
    auto-correct; absent ⇒ flag-only (record divergences, don't change volumes)."""
    circuit = resolve_circuit(circuit)
    form = await request.form()
    orch = _orch(request)
    from ..database import upsert_sensitivity_config

    auto = 1 if form.get("recorder_reconcile_auto") == "on" else 0
    upsert_sensitivity_config(orch.db, circuit, recorder_reconcile_auto=auto)
    return ingress_redirect(request, f"/settings#circuit-{circuit}")


# ------------------------------------------------------------------
# Recalibration
# ------------------------------------------------------------------
@router.post("/recalibrate/{circuit}")
async def recalibrate(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    form = await request.form()
    orch = _orch(request)

    fixtures_changed = form.get("fixtures_changed") == "yes"
    occupants_changed = form.get("occupants_changed") == "yes"
    # 1..365 days — anything outside that range almost certainly a typo
    # or stale form; default-back rather than persist an absurd window.
    calibration_days = coerce_int(
        form.get("calibration_days"), lo=1, hi=365, default=14,
    )

    if not orch.training_manager:
        return JSONResponse(
            {"error": "System still starting up — try again in a moment"},
            status_code=503,
        )

    kind = "full" if fixtures_changed else "partial"
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

    # §2.4 — confirm the (fast) recalibration trigger; the slow re-lock at the next
    # activation is tracked separately by _fit_and_lock.
    from ..database import start_job, finish_job
    job = start_job(orch.db, "recalibration", circuit, "Recalibration…")
    finish_job(orch.db, job, "done",
               f"{circuit}: {kind} recalibration started — new learning period")
    return ingress_redirect(request, f"/settings#circuit-{circuit}")


@router.post("/dev/retrain/{circuit}")
async def dev_retrain(circuit: str, request: Request):
    """DEV/testing only — instant rule-calibration re-fit + re-lock against the
    CURRENT labels (no new learning period), reusing the shared activation
    fit+freeze path (so the sanity gate still applies). Gated behind the
    ``dev_tools`` add-on option. Plain form POST → redirects back to the Dev tab;
    the reloaded page shows the updated 'Locked …' status. The per-type
    fit/fallback is surfaced via the HA 'calibration locked' notification + log
    (kept off the JS path so a stale app.js can't break the button)."""
    from ..config import DEV_TOOLS
    if not DEV_TOOLS:
        return JSONResponse({"error": "dev tools disabled"}, status_code=404)
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    if orch.training_manager:
        await orch.training_manager.retrain(circuit)
    return ingress_redirect(request, "/settings#sett-dev")


@router.post("/dev/validate-detectors/{circuit}")
async def dev_validate_detectors(circuit: str, request: Request):
    """DEV/testing only — run the detector self-validation (P6) against HA history on
    demand, WITHOUT re-freezing. Reconciles detected events vs the raw flow the addon
    re-scrapes from HA to confirm the phantom / cross-talk / dribble settings behave.
    Diagnostic only — writes no thresholds. Returns the report JSON. Gated behind
    ``dev_tools``."""
    from ..config import DEV_TOOLS
    if not DEV_TOOLS:
        return JSONResponse({"error": "dev tools disabled"}, status_code=404)
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    if orch.training_manager:
        # Persists a single latest-per-circuit report; the reloaded page renders it
        # (plain form POST → redirect, like dev_retrain — kept off the JS path).
        await orch.training_manager.validate_detectors(circuit)
    return ingress_redirect(request, "/settings#sett-dev")


@router.post("/dev/reimport-range/{circuit}")
async def dev_reimport_range(circuit: str, request: Request):
    """DEV/testing only — delete this circuit's purely-machine-derived events in a
    LOCAL date range and rebuild them from HA flow history. The bulk counterpart to
    the per-event Reprocess on the History modal (both share ``reprocess_window``);
    fixes a garbled/unclosed event that absorbed a whole day. User-labelled /
    classified / ignored events are preserved. Gated behind ``dev_tools``."""
    from ..config import DEV_TOOLS
    if not DEV_TOOLS:
        return JSONResponse({"error": "dev tools disabled"}, status_code=404)
    from datetime import datetime, timezone
    from ..reprocess import reprocess_window
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    # The date inputs are LOCAL calendar days — span them in the home tz, then to
    # UTC so the window matches the user's day (and the stored UTC timestamps).
    tz = getattr(orch, "_ha_tz", timezone.utc) or timezone.utc
    try:
        from_dt = (datetime.fromisoformat((payload.get("range_start") or "") + "T00:00:00")
                   .replace(tzinfo=tz).astimezone(timezone.utc))
        to_dt = (datetime.fromisoformat((payload.get("range_end") or "") + "T23:59:59")
                 .replace(tzinfo=tz).astimezone(timezone.utc))
    except (TypeError, ValueError):
        return JSONResponse({"error": "Invalid date range"}, status_code=400)
    if to_dt < from_dt:
        return JSONResponse({"error": "End date is before start date"},
                            status_code=400)
    try:
        result = await reprocess_window(orch, circuit, from_dt, to_dt)
    except Exception as e:
        log.error("[%s] dev_reimport_range failed: %s", circuit, e, exc_info=True)
        return JSONResponse({"error": "Reprocess failed — see addon log."},
                            status_code=500)
    if result.get("busy"):
        return JSONResponse(
            {"error": "Another volume operation is running — try again shortly."},
            status_code=409)
    return JSONResponse({"ok": True, **result})


@router.get("/recalibrate/{circuit}/suggest")
async def suggest_days(circuit: str, request: Request):
    """Return suggested calibration days based on home profile."""
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    from ..database import get_home_profile
    from ..config import compute_suggested_calibration_days

    profile = get_home_profile(orch.db) or {}
    days, tier = compute_suggested_calibration_days(
        profile.get("bathrooms_full") or 1,
        profile.get("bathrooms_half") or 0,
        profile.get("floors") or 1,
        profile.get("occupants") or 2,
        profile.get("supply_type") or "mains",
    )
    return JSONResponse({"suggested_days": days, "tier": tier})


# ------------------------------------------------------------------
# Alert enable/disable
# ------------------------------------------------------------------
@router.post("/alert/{circuit}/{alert_id}/toggle")
async def alert_toggle(circuit: str, alert_id: str, request: Request):
    circuit = resolve_circuit(circuit)
    form = await request.form()
    orch = _orch(request)
    enabled = form.get("enabled") == "true"
    from ..database import set_alert_enabled
    set_alert_enabled(orch.db, alert_id, enabled)
    return JSONResponse({"status": "updated", "enabled": enabled})


# Roles whose discovered entity_ids may be written via /settings/device-entity/update.
# Only ESPHome number.* entities — select.* support requires a separate endpoint.
# Keep in sync with _THRESHOLD_ROLES in device.py.
_SETTINGS_MUTABLE_ROLES: frozenset[str] = frozenset({
    "burst_threshold",
    "pressure_drop_threshold",
    "leak_pressure_threshold",
    "trickle_min_flow",
    "trickle_max_flow",
    "trickle_duration",
    "leak_test_duration_number",   # preferred name (firmware v3.6+)
    "leak_test_duration_sensor",   # compat alias — remove after one release
})


# ------------------------------------------------------------------
# Device entity updates (number entities on the ESP)
# ------------------------------------------------------------------
@router.post("/device-entity/update")
async def device_entity_update(request: Request):
    """Update a number entity value on the ESP via HA.

    Only ESPHome number.* entities that belong to an explicitly mutable
    role are accepted.  input_number, select, and input_select are rejected.
    """
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

    if domain != "number":
        # 400 not 403 — domain mismatch is input validation, not auth.
        return JSONResponse(
            {"status": "error",
             "message": "Only ESPHome number.* entities are accepted"},
            status_code=400,
        )

    # Build the allowlist from discovered entities in mutable roles only.
    from ..device_discovery import load_circuit_entities
    allowed: set[str] = set()
    for c in orch._cfg.circuits:
        ents = load_circuit_entities(orch.db, c.circuit)
        allowed.update(v for k, v in ents.items() if k in _SETTINGS_MUTABLE_ROLES and v)

    if entity_id not in allowed:
        # 400 not 403 — entity_id failed enum validation against the
        # discovered allowlist. The request is well-formed; the value
        # is just not in the accepted set.
        return JSONResponse(
            {"status": "error", "message": "Entity not in allowed set for this device"},
            status_code=400,
        )

    native_step = form.get("native_step", "").strip()
    try:
        numeric = float(value)
        # Convert from display units back to internal units (L/min / PSI)
        # before sending to HA/ESP.
        _FLOW_KEYWORDS     = ("flow_threshold", "burst_threshold")
        _PRESSURE_KEYWORDS = ("pressure_threshold", "pressure_drop")
        if any(k in entity_id for k in _FLOW_KEYWORDS):
            from ..units import load_unit_context as _luc
            numeric = numeric / _luc(orch.db)["flow_factor"]
        elif any(k in entity_id for k in _PRESSURE_KEYWORDS):
            from ..units import load_unit_context as _luc
            numeric = numeric / _luc(orch.db)["pressure_factor"]
        # Round to the entity's native step to satisfy HA validation
        if native_step:
            try:
                ns = float(native_step)
                if ns > 0:
                    numeric = round(round(numeric / ns) * ns, 6)
            except ValueError:
                numeric = round(numeric, 4)
        else:
            numeric = round(numeric, 4)
        ok = await orch.ha.set_number(entity_id, numeric)
    except ValueError:
        return JSONResponse(
            {"status": "error", "message": f"Invalid number: {value}"},
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

    def _int(key: str, default: int) -> int:
        try:
            return int(form.get(key, default))
        except (ValueError, TypeError):
            return default

    update_data_retention(
        orch.db,
        events_retain_years=_int("events_retain_years", 1),
        hourly_volume_retain_years=_int("hourly_volume_retain_years", 2),
        enabled=1 if form.get("enabled") == "1" else 0,
        auto_backup_enabled=1 if form.get("auto_backup_enabled") == "1" else 0,
        auto_backup_path=form.get("auto_backup_path",
                                   "/share/water_monitor_backups").strip(),
        auto_backup_day_of_week=_int("auto_backup_day_of_week", 0),
    )
    return ingress_redirect(request, "/settings#retention")


@router.post("/retention/prune-now")
async def retention_prune_now(request: Request):
    import asyncio
    orch = _orch(request)
    if not orch.data_pruner:
        return JSONResponse({"ok": False, "error": "Pruner not available"}, status_code=503)
    # prune_now() runs synchronous SQLite DELETEs that can block for several
    # seconds on large tables.  Run it in a thread-pool executor so the
    # asyncio event loop stays responsive during the operation.
    loop = asyncio.get_running_loop()
    deleted = await loop.run_in_executor(None, orch.data_pruner.prune_now)
    return JSONResponse({"ok": True, "deleted": deleted})


@router.post("/reprocess/degraded-supply")
async def reprocess_degraded_supply(request: Request):
    """Re-apply current degraded-supply thresholds to every event with
    stored diagnostics. Used after the guard's gates are tuned so past
    verdicts (and the daily-volume totals derived from them) match the
    new policy without waiting for new traffic."""
    import asyncio
    from ..feature_extractor import reprocess_degraded_supply_verdicts

    orch = _orch(request)
    loop = asyncio.get_running_loop()
    summary = await loop.run_in_executor(
        None, reprocess_degraded_supply_verdicts, orch.db
    )
    log.info(
        "Degraded-supply reprocess: evaluated=%d flipped_to_degraded=%d "
        "flipped_to_clean=%d skipped_legacy=%d",
        summary["evaluated"], summary["flipped_to_degraded"],
        summary["flipped_to_clean"], summary["skipped_legacy"],
    )
    return JSONResponse({"ok": True, **summary})


# ── Re-run setup wizard ────────────────────────────────────────────────────
# Flips home_profile.setup_complete back to 0 so the setup wizard's
# state-mutating endpoints (guarded by _block_if_setup_complete()) accept
# POSTs again. The wizard's final step (/setup/home) sets it back to 1 on
# completion, re-locking the mutators. The route lives in the CSRF-covered
# settings router (only /setup/* is CSRF-exempted in main.py), so a stray
# cross-origin POST cannot trigger this.

@router.post("/setup/unlock")
async def setup_unlock(request: Request):
    from ..database import update_home_profile
    orch = _orch(request)
    update_home_profile(orch.db, setup_complete=0)
    log.warning("Setup wizard unlocked via Settings → Advanced — /setup/* mutators are open until the wizard completes again")
    return ingress_redirect(request, "/setup")


# ── Away mode ──────────────────────────────────────────────────────────────

@router.post("/away-mode/toggle")
async def away_mode_toggle(request: Request):
    orch = _orch(request)
    form = await request.form()
    enabled = form.get("enabled") == "1"
    # Await the call so the database write completes before we redirect
    # back to /settings — otherwise the rendered page can show stale state.
    await orch.set_away_mode(enabled)
    return ingress_redirect(request, "/settings#away")


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
    return ingress_redirect(request, "/settings#notifications")


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
    return ingress_redirect(request, "/settings#away")

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
    # History display: one unified "Hide not-real-use events" toggle hides every
    # volume-zeroing verdict (phantom / cross-talk / dribble). Display-only — never
    # affects totals (their volume is zeroed at detection regardless of this flag).
    # The single checkbox drives BOTH legacy columns in lockstep so any reader stays
    # consistent and a legacy split state is normalised on first save.
    hide_not_real = 1 if form.get("hide_pressure_artifact_events") == "1" else 0
    hide_phantoms = hide_not_real
    hide_cross_talk = hide_not_real
    # dev.38 — guarded auto-split opt-in (read fresh by the periodic maturity pass).
    auto_split = 1 if form.get("auto_split_enabled") == "1" else 0
    orch.db.execute(
        "UPDATE home_profile SET flow_unit=?, pressure_unit=?, "
        "hide_pressure_artifact_events=?, hide_cross_talk_events=?, "
        "auto_split_enabled=? WHERE id=1",
        (flow_key, pressure_key, hide_phantoms, hide_cross_talk, auto_split),
    )
    orch.db.commit()
    from ..units import invalidate_unit_cache
    invalidate_unit_cache()
    return ingress_redirect(request, "/settings#units")


@router.post("/water-softener/update")
async def water_softener_update(request: Request):
    """dev.24 — persist the opt-in water-softener config. The regen start time is
    REQUIRED when enabled (the session detector + leak-test blackout key on it),
    so an enabled-but-invalid submission is rejected rather than stored blank.
    The live path reads this profile FRESH per event, so the toggle takes effect
    on the next event with no restart."""
    from ..event_rules import parse_hhmm_to_minutes
    from ..database import update_home_profile
    orch = _orch(request)
    form = await request.form()
    enabled = form.get("has_water_softener") == "1"
    start = (form.get("softener_regen_start") or "").strip()
    circuit = (form.get("softener_circuit") or "").strip() or None
    if enabled and parse_hhmm_to_minutes(start) is None:
        return ingress_redirect(
            request, "/settings?msg=softener_time_required#water-softener")
    update_home_profile(
        orch.db,
        has_water_softener=1 if enabled else 0,
        softener_regen_start=start if enabled else None,
        softener_circuit=circuit if enabled else None,
    )
    return ingress_redirect(request, "/settings#water-softener")


@router.post("/integrations/update")
async def integrations_update(request: Request):
    form = await request.form()
    enabled = 1 if form.get("mqtt_publish_enabled") == "1" else 0
    orch = _orch(request)
    orch.db.execute(
        """UPDATE home_profile SET mqtt_publish_enabled = ? WHERE id = 1""",
        (enabled,)
    )
    orch.db.commit()
    return ingress_redirect(request, "/settings#integrations")


# ------------------------------------------------------------------
# Circuit display name rename
# ------------------------------------------------------------------

@router.post("/circuit/{circuit}/rename")
async def circuit_rename(circuit: str, request: Request):
    """Update the human-readable display name for a circuit."""
    circuit = resolve_circuit(circuit)
    form = await request.form()
    orch = _orch(request)

    # Validate circuit exists
    circuit_cfg = orch._cfg.get_circuit(circuit)
    if not circuit_cfg:
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=404,
        )

    from ..circuit_compat import validate_display_name
    try:
        display_name = validate_display_name(form.get("display_name", ""))
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)

    from ..database import upsert_circuit_label
    upsert_circuit_label(orch.db, circuit, display_name)
    orch.reload_circuit_labels()

    return JSONResponse({"status": "renamed", "circuit": circuit, "display_name": display_name})


@router.post("/circuit/{circuit}/type")
async def circuit_type_update(circuit: str, request: Request):
    """Update the circuit_type for a circuit.

    Zone alert rows are seeded automatically when switching to 'zone'.
    They are never deleted when switching back to 'fixture' — the UI
    simply hides them via the zone_only_alert_types template filter.
    """
    from ..circuit_compat import resolve_circuit
    circuit = resolve_circuit(circuit)
    orch = _orch(request)

    if not orch._cfg.get_circuit(circuit):
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=404,
        )

    form = await request.form()
    from ..fixtures import (
        normalize_circuit_type, CIRCUIT_TYPES, zone_user_selectable_types,
    )
    from ..database import set_circuit_type

    raw_type = form.get("circuit_type", "").strip()
    circuit_type = normalize_circuit_type(raw_type)
    if circuit_type not in CIRCUIT_TYPES:
        return JSONResponse(
            {"status": "error",
             "message": f"Invalid circuit_type {raw_type!r}. Must be one of: {', '.join(sorted(CIRCUIT_TYPES))}"},
            status_code=400,
        )

    # When switching to 'zone', refuse if the circuit already has
    # confirmed fixtures whose type is not appropriate for a zone
    # (e.g. toilet, shower, kitchen_tap). Otherwise the type system
    # silently goes inconsistent — a "zone" circuit with toilet
    # fixtures attached. Suggested types from clustering are NOT
    # blockers: only user-confirmed `fixtures.fixture_type` counts.
    # Zone → fixture is always allowed (zone alert rows are preserved
    # and just hidden by the template filter).
    if circuit_type == "zone":
        allowed_zone_types = set(zone_user_selectable_types())
        rows = orch.db.execute(
            """SELECT f.id, f.fixture_type, COALESCE(f.display_name, f.name) AS name
               FROM fixture_clusters fc
               JOIN fixtures f ON fc.fixture_id = f.id
              WHERE fc.circuit = ?
                AND f.fixture_type IS NOT NULL
                AND f.fixture_type != ''""",
            (circuit,),
        ).fetchall()
        conflicts = [
            dict(r) for r in rows
            if r["fixture_type"] not in allowed_zone_types
        ]
        if conflicts:
            # Summarise per type so the message stays short when many
            # fixtures share a non-zone type (e.g. 4 toilets).
            from collections import Counter
            type_counts = Counter(c["fixture_type"] for c in conflicts)
            summary = ", ".join(
                f"{count}× {t}" if count > 1 else t
                for t, count in sorted(type_counts.items())
            )
            log.info(
                "[%s] zone-flip refused — %d non-zone fixture(s): %s",
                circuit, len(conflicts), summary,
            )
            return JSONResponse(
                {"status": "error",
                 "message": (
                     f"Cannot switch to zone — this circuit has "
                     f"{len(conflicts)} confirmed fixture(s) with "
                     f"non-zone types ({summary}). Reassign or delete "
                     f"them under Fixtures first."
                 ),
                 "conflicts": conflicts},
                status_code=409,  # 409 Conflict — request well-formed but state forbids
            )

    try:
        set_circuit_type(orch.db, circuit, circuit_type)
    except Exception as exc:
        log.error("[%s] set_circuit_type failed: %s", circuit, exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)

    orch.reload_circuit_profiles()
    log.info("[%s] circuit_type changed to %r", circuit, circuit_type)

    return JSONResponse({
        "status": "updated",
        "circuit": circuit,
        "circuit_type": circuit_type,
    })


@router.post("/circuit/{circuit}/valve-type")
async def circuit_valve_type_update(circuit: str, request: Request):
    """Update the valve_type for a circuit.

    Strict validation: invalid input returns HTTP 400. Mirrors the
    circuit_type endpoint's CSRF behavior (the project uses
    middleware-driven CSRF on the settings router — see main.py).
    Switching to '3_port' disables the micro leak test for this circuit
    (see leak_test_scheduler._execute_test and _check_schedule); the
    existing leak-test schedule row is preserved so switching back to
    '2_port' resumes the prior schedule without reconfiguration.
    """
    from ..circuit_compat import resolve_circuit
    circuit = resolve_circuit(circuit)
    orch = _orch(request)

    if not orch._cfg.get_circuit(circuit):
        return JSONResponse(
            {"status": "error", "message": f"Unknown circuit: {circuit}"},
            status_code=404,
        )

    form = await request.form()
    from ..fixtures import parse_valve_type
    from ..database import set_valve_type, get_valve_type

    raw_vt = form.get("valve_type", "").strip()
    parsed = parse_valve_type(raw_vt)
    if parsed is None:
        return JSONResponse(
            {"status": "error",
             "message": f"Invalid valve_type {raw_vt!r}. Must be '2_port' or '3_port'."},
            status_code=400,
        )

    # No-change short-circuit — keeps the log clean on idempotent posts.
    current = get_valve_type(orch.db, circuit)
    if current == parsed:
        return JSONResponse({
            "status": "no_change",
            "circuit": circuit,
            "valve_type": parsed,
        })

    try:
        set_valve_type(orch.db, circuit, parsed)
    except Exception as exc:
        log.error("[%s] set_valve_type failed: %s", circuit, exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)

    log.info("[%s] valve_type changed to %r", circuit, parsed)
    return JSONResponse({
        "status": "updated",
        "circuit": circuit,
        "valve_type": parsed,
    })


# ------------------------------------------------------------------
# Plumbing-event exclusion windows
# ------------------------------------------------------------------

@router.post("/circuit/{circuit}/exclusion_window")
async def start_exclusion_window(request: Request, circuit: str):
    """Open an exclusion window so events during a plumbing flush are not
    used for fixture training.  Duration: 5–60 min (clamped server-side)."""
    circuit = resolve_circuit(circuit)
    from ..database import create_exclusion_window
    form    = await request.form()
    # 5..60 minutes — same clamp the old code applied, now expressed
    # as one helper call so out-of-range AND malformed inputs both
    # fall back to the 15 minute default.
    minutes = coerce_int(form.get("minutes"), lo=5, hi=60, default=15)
    reason  = (form.get("reason") or "plumbing").strip() or "plumbing"
    create_exclusion_window(_orch(request).db, circuit, minutes, reason)
    log.info("[%s] exclusion window started — %d min (%s)", circuit, minutes, reason)
    return ingress_redirect(request, "/settings#maintenance")


@router.post("/circuit/{circuit}/exclusion_window/cancel")
async def cancel_exclusion_window_endpoint(request: Request, circuit: str):
    """End the active exclusion window immediately."""
    circuit = resolve_circuit(circuit)
    from ..database import cancel_exclusion_window
    cancel_exclusion_window(_orch(request).db, circuit)
    log.info("[%s] exclusion window cancelled", circuit)
    return ingress_redirect(request, "/settings#maintenance")


@router.post("/circuit/{circuit}/exclusion_window/extend")
async def extend_exclusion_window_endpoint(request: Request, circuit: str):
    """Add 15 minutes to the active exclusion window (capped at 60 min from start)."""
    circuit = resolve_circuit(circuit)
    from ..database import extend_exclusion_window
    extend_exclusion_window(_orch(request).db, circuit, extra_minutes=15)
    log.info("[%s] exclusion window extended +15 min", circuit)
    return ingress_redirect(request, "/settings#maintenance")


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
