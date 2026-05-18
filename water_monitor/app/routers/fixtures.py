"""Fixtures router — Phase 2."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ._helpers import ingress_redirect
from ..circuit_compat import resolve_circuit

log = logging.getLogger(__name__)

router = APIRouter(prefix="/fixtures")


def _orch(request: Request):
    return request.app.state.orchestrator


def _tmpl(request: Request):
    return request.app.state.templates


def _valid_circuit(circuit: str, request: Request) -> str:
    """FastAPI dependency — normalises legacy aliases then validates against configured circuits."""
    circuit = resolve_circuit(circuit)
    cfg = _orch(request)._cfg
    if circuit not in {c.circuit for c in cfg.circuits}:
        raise HTTPException(status_code=404, detail=f"Unknown circuit: {circuit!r}")
    return circuit


# ── Page ──────────────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def fixtures_page(request: Request, preview: bool = False):
    orch = _orch(request)
    from ..database import get_clusters_with_fixtures, get_all_cluster_stats
    from ..fixtures import (FIXTURE_TYPE_LABELS, user_selectable_types,
                            zone_user_selectable_types, fixture_user_selectable_types)

    circuits_ctx = []
    total_unreviewed = 0
    circuit_type_selectable = {}

    for circ_cfg in orch._cfg.circuits:
        c = circ_cfg.circuit
        training = (
            orch.training_manager.get_training_info(c)
            if orch.training_manager
            else {"state": "idle"}
        )
        clusters_raw = get_clusters_with_fixtures(orch.db, c)
        all_stats = get_all_cluster_stats(orch.db, c)
        clusters = [{**cl, **all_stats.get(cl["id"], {})} for cl in clusters_raw]

        state = training.get("state", "idle")
        unreviewed = sum(1 for cl in clusters if not cl.get("fixture_id"))
        # Only count clusters from circuits whose grid is actually rendered.
        # Circuits in idle/calibrating states show a "Still calibrating" stub
        # instead of the cluster grid, so counting their clusters in the
        # review banner produces a contradiction ("7 need review" with
        # nothing visible to review below).  Labelling and live circuits
        # both render the grid and so do contribute to the count.
        if state not in ("idle", "calibrating"):
            total_unreviewed += unreviewed
        from ..database import get_active_exclusion_window
        circuits_ctx.append({
            "circuit":          c,
            "display_name":     circ_cfg.label,
            "training_state":   state,
            "clusters":         clusters,
            "unreviewed_count": unreviewed,
            "active_exclusion": get_active_exclusion_window(orch.db, c),
        })

        if circ_cfg.circuit_type == "zone":
            circuit_type_selectable[c] = zone_user_selectable_types()
        else:
            circuit_type_selectable[c] = fixture_user_selectable_types()

    # Differentiates banner copy + CSS treatment so users in the labelling
    # phase get a clearer call to action ("confirm then activate") than
    # users in the live phase ("confirm or remove").
    any_labelling = any(c["training_state"] == "labelling"
                        for c in circuits_ctx)

    return _tmpl(request).TemplateResponse("fixtures.html", {
        "request":                 request,
        "page":                    "fixtures",
        "circuits":                circuits_ctx,
        "total_unreviewed":        total_unreviewed,
        "any_labelling":           any_labelling,
        "fixture_type_labels":     FIXTURE_TYPE_LABELS,
        "user_selectable_types":   user_selectable_types(),
        "circuit_type_selectable": circuit_type_selectable,
        "preview":                 preview,
    })


# ── Re-run clustering ─────────────────────────────────────────────────────────

@router.post("/{circuit}/cluster")
async def retrigger_cluster(request: Request, circuit: str = Depends(_valid_circuit)):
    """Rebuild DBSTREAM state from DB — resets in-memory engine and replays
    the last 60 days so the fixture_clusters table reflects current history."""
    orch   = _orch(request)
    engine = getattr(orch, "cluster_engine", None)
    if not engine:
        return ingress_redirect(request, "/fixtures?msg=error")
    try:
        import asyncio, functools
        loop = asyncio.get_running_loop()
        count = await loop.run_in_executor(
            None, functools.partial(engine.rebuild_from_db, circuit)
        )
        log.info("[%s] manual rebuild: %d events replayed", circuit, count)
        if count == 0:
            return ingress_redirect(request, "/fixtures?msg=too_few_events")
        msg = "reclustered"
    except Exception as e:
        log.error("[%s] re-cluster error: %s", circuit, e, exc_info=True)
        msg = "error"
    return ingress_redirect(request, f"/fixtures?msg={msg}")


# ── Activate fixtures (labelling → live) ──────────────────────────────────────

@router.post("/{circuit}/activate")
async def activate_circuit(request: Request, circuit: str = Depends(_valid_circuit)):
    """Transition labelling → live when the user is satisfied with their
    cluster labels.  No-op (with error flash) if circuit isn't currently
    in labelling state — typically a stale browser tab."""
    orch = _orch(request)
    tm = orch.training_manager
    if not tm:
        return ingress_redirect(request, "/fixtures?msg=error")
    ok = await tm.activate_fixtures(circuit)
    if not ok:
        return ingress_redirect(request, "/fixtures?msg=error")
    return ingress_redirect(request, "/fixtures?msg=activated")


# ── Confirm / save a cluster label ────────────────────────────────────────────

@router.post("/{circuit}/cluster/{cluster_id}/confirm")
async def confirm_cluster(request: Request, cluster_id: int, circuit: str = Depends(_valid_circuit)):
    form         = await request.form()
    name         = (form.get("name") or "").strip()
    fixture_type = (form.get("fixture_type") or "other").strip()
    publish      = 1 if form.get("publish_to_ha") else 0

    if not name:
        name = fixture_type.replace("_", " ").title()

    orch = _orch(request)
    from ..database import upsert_fixture_from_cluster
    fixture_id = upsert_fixture_from_cluster(
        orch.db, circuit, cluster_id, name, fixture_type, publish
    )
    if publish and fixture_id:
        fp = getattr(orch, "_fixture_publisher", None)
        if fp:
            fp.publish_fixture(fixture_id)
    # Notify the cluster engine so the type-aware match gate takes effect
    # immediately — no restart needed.
    engine = orch.cluster_engine
    if engine:
        engine.notify_fixture_confirmed(circuit, cluster_id, fixture_type)
    return ingress_redirect(request, "/fixtures")


# ── Delete a cluster ──────────────────────────────────────────────────────────

@router.post("/{circuit}/cluster/{cluster_id}/delete")
async def delete_cluster_endpoint(request: Request, cluster_id: int, circuit: str = Depends(_valid_circuit)):
    orch = _orch(request)
    from ..database import delete_cluster, get_fixture_id_for_cluster
    fixture_id = get_fixture_id_for_cluster(orch.db, circuit, cluster_id)
    delete_cluster(orch.db, circuit, cluster_id)
    if fixture_id:
        fp = getattr(orch, "_fixture_publisher", None)
        if fp:
            fp.retract_fixture(fixture_id)
    # Drop the cluster from the type cache so the gate no longer applies
    # to any subsequent river center that re-maps to this slot.
    engine = orch.cluster_engine
    if engine:
        engine.notify_fixture_removed(circuit, cluster_id)
    return ingress_redirect(request, "/fixtures")


# ── Merge clusters ────────────────────────────────────────────────────────────

@router.get("/{circuit}/merge", response_class=HTMLResponse)
async def merge_page(request: Request, circuit: str = Depends(_valid_circuit)):
    """Show a confirmation page for merging the selected clusters."""
    raw_ids = request.query_params.get("ids", "")
    try:
        ids = list(dict.fromkeys(int(i) for i in raw_ids.split(",") if i.strip()))
    except ValueError:
        return ingress_redirect(request, "/fixtures?msg=error")
    if len(ids) < 2:
        return ingress_redirect(request, "/fixtures?msg=error")

    orch = _orch(request)
    from ..database import get_clusters_with_fixtures, get_all_cluster_stats
    from ..fixtures import FIXTURE_TYPE_LABELS
    clusters_raw = get_clusters_with_fixtures(orch.db, circuit)
    all_stats = get_all_cluster_stats(orch.db, circuit)
    clusters_by_id = {
        cl["id"]: {**cl, **all_stats.get(cl["id"], {})}
        for cl in clusters_raw
    }

    selected = [clusters_by_id[i] for i in ids if i in clusters_by_id]
    if len(selected) < 2:
        return ingress_redirect(request, "/fixtures?msg=error")

    default_survivor = max(selected, key=lambda c: c.get("member_count") or 0)
    total_events = sum(c.get("event_count") or 0 for c in selected)
    total_members = sum(c.get("member_count") or 0 for c in selected)
    confirmed_count = sum(1 for c in selected if c.get("fixture_id"))

    return _tmpl(request).TemplateResponse("fixtures_merge.html", {
        "request":          request,
        "page":             "fixtures",
        "circuit":          circuit,
        "clusters":         selected,
        "default_survivor": default_survivor["id"],
        "total_events":     total_events,
        "total_members":    total_members,
        "confirmed_count":  confirmed_count,
        "fixture_type_labels": FIXTURE_TYPE_LABELS,
    })


@router.post("/{circuit}/merge")
async def merge_clusters_endpoint(
    request: Request, circuit: str = Depends(_valid_circuit)
):
    """Execute a multi-cluster merge after user confirmation."""
    form = await request.form()
    try:
        survivor_id = int(form.get("survivor_id") or 0)
        ids = list(dict.fromkeys(
            int(v) for v in form.getlist("ids") if str(v).strip()
        ))
    except (ValueError, TypeError):
        return ingress_redirect(request, "/fixtures?msg=error")

    if survivor_id not in ids or len(ids) < 2:
        return ingress_redirect(request, "/fixtures?msg=error")

    orch = _orch(request)
    from ..database import merge_clusters, get_fixture_id_for_cluster

    # Collect non-survivor fixture IDs before merge, for HA retraction only.
    deleted_ids = [i for i in ids if i != survivor_id]
    retract_fixture_ids = [
        fid for i in deleted_ids
        if (fid := get_fixture_id_for_cluster(orch.db, circuit, i))
    ]

    try:
        summary = merge_clusters(orch.db, circuit, survivor_id, ids)
    except ValueError as e:
        log.warning("[%s] merge validation failed: %s", circuit, e)
        return ingress_redirect(request, "/fixtures?msg=merge_failed")
    except Exception as e:
        log.error("[%s] merge_clusters error: %s", circuit, e, exc_info=True)
        return ingress_redirect(request, "/fixtures?msg=merge_failed")

    # Retract deleted HA fixture entities.
    fp = getattr(orch, "_fixture_publisher", None)
    if fp:
        for fid in retract_fixture_ids:
            try:
                fp.retract_fixture(fid)
            except Exception as e:
                log.error("[%s] retract_fixture %s failed: %s", circuit, fid, e)

    # Rebuild engine state to heal river_id_map and type_cache.
    engine = getattr(orch, "cluster_engine", None)
    if engine:
        try:
            import asyncio, functools
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, functools.partial(engine.rebuild_from_db, circuit)
            )
        except Exception as e:
            log.error("[%s] rebuild_from_db after merge failed: %s", circuit, e)

    log.info(
        "[%s] merged fixture clusters: survivor=%d, deleted=%s, "
        "events_relinked=%d, fixtures_removed=%d",
        circuit, survivor_id, deleted_ids,
        summary["events_relinked"], summary["fixtures_removed"],
    )
    return ingress_redirect(request, "/fixtures?msg=merged")


# ── JSON API ──────────────────────────────────────────────────────────────────

@router.get("/api/{circuit}/clusters")
async def api_clusters(request: Request, circuit: str = Depends(_valid_circuit)):
    from ..database import get_clusters_with_fixtures, get_all_cluster_stats
    db = _orch(request).db
    all_stats = get_all_cluster_stats(db, circuit)
    clusters = [{**cl, **all_stats.get(cl["id"], {})}
                for cl in get_clusters_with_fixtures(db, circuit)]
    return JSONResponse(clusters)
