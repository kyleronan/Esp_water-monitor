"""Flow-meter calibration helper — guided bucket / municipal-meter test (session + HA glue).

Pure math lives in ``calibration_math`` (unit-tested). This module is the FastAPI/HA layer:
per-circuit in-memory sessions, reading the cumulative volume sensor over a Start→Stop
window, and applying the result by writing the firmware PPL entity (single source of truth)
then routing through the orchestrator's PPL-change handler (relative-threshold re-baseline;
cache-before-subscription so it can't double-handle).

A calibration run is a deliberate large draw, so the orchestrator marks the circuit
`calibrating` for the session — auto-shutoff is suppressed and the test event is excluded
from training/anomaly. Sessions are process-local + never persisted; if the add-on restarts
mid-test the session is gone and every endpoint returns a clean "session expired", not a 500.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..circuit_compat import resolve_circuit
from ..units import load_unit_context
from ..calibration_math import (
    BUCKET_MIN_L, MUNICIPAL_MIN_L, METER_UNIT_FACTORS, gate, pooled, run_ppl, to_litres,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/calibrate")

_SESSION_TTL_S = 1800.0      # stale-session auto-expiry (30 min)
_sessions: Dict[str, Dict[str, Any]] = {}   # per-circuit, never persisted


def _orch(r): return r.app.state.orchestrator


async def _json(request: Request) -> dict:
    try:
        return await request.json() or {}
    except Exception:
        return {}


def _err(msg: str, code: int = 400):
    return JSONResponse({"ok": False, "error": msg}, status_code=code)


def _session(circuit: str) -> Optional[dict]:
    s = _sessions.get(circuit)
    if s and (time.monotonic() - s["started_at"]) > _SESSION_TTL_S:
        _sessions.pop(circuit, None)
        return None
    return s


async def _read_volume_l(orch, cfg) -> Optional[float]:
    """Read the circuit's cumulative volume sensor as litres (defensive about its unit)."""
    ent = getattr(cfg, "volume_sensor", "")
    if not ent or orch.ha is None:
        return None
    st = await orch.ha.get_state(ent)
    if not st:
        return None
    try:
        val = float(st.get("state"))
    except (TypeError, ValueError):
        return None  # 'unknown' / 'unavailable'
    unit = ((st.get("attributes") or {}).get("unit_of_measurement") or "L").strip()
    factor = METER_UNIT_FACTORS.get(unit)   # firmware reports L; convert if HA exposed another
    if factor and factor != 1.0:
        val = to_litres(val, factor)
    return val


def _ready_cfg(orch, circuit):
    """Return the CircuitConfig, or None (unknown) / False (missing prerequisites)."""
    cfg = orch._cfg.get_circuit(circuit)
    if cfg is None:
        return None
    if not getattr(cfg, "flow_meter_ppl_entity", "") or not getattr(cfg, "volume_sensor", ""):
        return False
    return cfg


@router.post("/{circuit}/start")
async def start(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    cfg = _ready_cfg(orch, circuit)
    if cfg is None:
        return _err("Unknown circuit")
    if cfg is False:
        return _err("Calibration needs the Flow Meter PPL entity and the volume sensor — "
                    "update firmware to 3.12.0+ and re-run device discovery.")
    body = await _json(request)
    method = (body.get("method") or "bucket").strip()
    if method not in ("bucket", "municipal"):
        return _err("Unknown method")

    units = load_unit_context(orch.db)
    sess = _session(circuit)
    if sess is None or sess.get("method") != method:
        sess = {"method": method, "runs": [], "actual_l": None, "meter_unit": None,
                "current_ppl": float(getattr(cfg, "pulses_per_litre", 396.0) or 396.0)}
        _sessions[circuit] = sess

    if method == "bucket":
        if body.get("known_volume") not in (None, ""):
            try:
                known = float(body.get("known_volume"))
            except (TypeError, ValueError):
                return _err("Enter the container volume")
            actual_l = to_litres(known, units.get("vol_factor") or 1.0)
            if actual_l < BUCKET_MIN_L:
                return _err(f"Use a larger container (≥ {BUCKET_MIN_L:.0f} L / ~0.5 gal) "
                            "so the measurement resolves.")
            sess["actual_l"] = actual_l
        if sess["actual_l"] is None:
            return _err("Enter the container volume")
        sess["meter_start"] = None
    else:  # municipal — the start reading is captured per run
        try:
            meter_start = float(body.get("meter_start"))
        except (TypeError, ValueError):
            return _err("Enter the starting meter reading")
        meter_unit = (body.get("meter_unit") or "").strip()
        if meter_unit not in METER_UNIT_FACTORS:
            return _err("Choose the meter's unit (gal / ft³ / m³ / L)")
        sess["meter_unit"] = meter_unit
        sess["meter_start"] = meter_start

    baseline = await _read_volume_l(orch, cfg)
    if baseline is None:
        return _err("Could not read the volume sensor — is the device online?")
    sess["baseline_l"] = baseline
    sess["started_at"] = time.monotonic()
    orch.set_calibrating(circuit, True)
    return JSONResponse({"ok": True, "method": method, "run_count": len(sess["runs"]),
                         "vol_unit": units.get("vol_unit", "L")})


@router.get("/{circuit}/poll")
async def poll(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)
    sess = _session(circuit) if cfg is not None else None
    if sess is None:
        return _err("session expired — start over", 409)
    now_l = await _read_volume_l(orch, cfg)
    measured = max(0.0, (now_l - sess["baseline_l"]) if now_l is not None else 0.0)
    units = load_unit_context(orch.db)
    factor = units.get("vol_factor") or 1.0
    return JSONResponse({"ok": True, "measured_l": round(measured, 3),
                         "measured_display": round(measured * factor, 2),
                         "vol_unit": units.get("vol_unit", "L")})


@router.post("/{circuit}/stop")
async def stop(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)
    sess = _session(circuit) if cfg is not None else None
    if sess is None:
        return _err("session expired — start over", 409)
    now_l = await _read_volume_l(orch, cfg)
    if now_l is None:
        return _err("Could not read the volume sensor")
    measured = now_l - sess["baseline_l"]
    if measured <= 0:
        return _err("No flow measured — did water run through this meter on this circuit?")

    if sess["method"] == "bucket":
        actual_l = sess["actual_l"]
    else:  # municipal — read the final meter value
        body = await _json(request)
        try:
            meter_end = float(body.get("meter_end"))
        except (TypeError, ValueError):
            return _err("Enter the final meter reading")
        factor = METER_UNIT_FACTORS.get(sess["meter_unit"], 1.0)
        actual_l = to_litres(meter_end - sess["meter_start"], factor)
        if actual_l < MUNICIPAL_MIN_L:
            return _err(f"Run more water — at least ~10 gal (the meter delta was only "
                        f"{actual_l:.1f} L) so the reading resolves.")

    sess["runs"].append({
        "measured_l": measured, "actual_l": actual_l,
        "run_ppl": run_ppl(sess["current_ppl"], measured, actual_l),
    })
    return JSONResponse({"ok": True, **pooled(sess["runs"], sess["current_ppl"], sess["method"])})


@router.post("/{circuit}/apply")
async def apply(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    cfg = orch._cfg.get_circuit(circuit)
    sess = _session(circuit) if cfg is not None else None
    if sess is None or not sess.get("runs"):
        return _err("session expired — start over", 409)
    body = await _json(request)
    # The user may adjust the suggested value before applying; else use the pooled value.
    if body.get("ppl") not in (None, ""):
        try:
            new_ppl = round(float(body.get("ppl")), 1)
        except (TypeError, ValueError):
            return _err("Enter a valid pulses/litre value")
        if not (1.0 <= new_ppl <= 5000.0):
            return _err("Pulses/litre must be between 1 and 5000")
    else:
        new_ppl = pooled(sess["runs"], sess["current_ppl"], sess["method"])["new_ppl"]
    # Gate the EFFECTIVE value (override or suggestion) on its OWN correction.
    g = gate(new_ppl, sess["current_ppl"], sess["method"], len(sess["runs"]))
    if not g["apply_allowed"]:
        return _err(f"Run at least {g['runs_needed']} more sample(s) for a "
                    f"{g['correction_pct']}% change.")
    entity = getattr(cfg, "flow_meter_ppl_entity", "")
    ok = await orch.ha.set_number(entity, new_ppl) if (entity and orch.ha) else False
    if not ok:
        return _err("Could not write the meter setting to the device.", 502)
    # Authoritative local update (cache + floor + relative-threshold re-baseline). The
    # later entity-subscription fire sees cache == new_ppl → debounces to a no-op.
    try:
        await orch._apply_ppl_change(cfg, str(new_ppl), reason="calibration")
    except Exception as e:
        log.warning("[%s] calibration apply post-write failed: %s", circuit, e)
    _sessions.pop(circuit, None)
    orch.set_calibrating(circuit, False)
    return JSONResponse({"ok": True, "new_ppl": new_ppl,
                         "rebaselined": g["will_rebaseline"]})


@router.post("/{circuit}/cancel")
async def cancel(circuit: str, request: Request):
    circuit = resolve_circuit(circuit)
    orch = _orch(request)
    _sessions.pop(circuit, None)
    orch.set_calibrating(circuit, False)
    return JSONResponse({"ok": True})
