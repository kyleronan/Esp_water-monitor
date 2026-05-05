"""Fixtures router — Phase 2."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ._helpers import ingress_redirect

log = logging.getLogger(__name__)

router = APIRouter(prefix="/fixtures")


def _orch(request: Request):
    return request.app.state.orchestrator


def _tmpl(request: Request):
    return request.app.state.templates


# ── Page ──────────────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def fixtures_page(request: Request):
    orch = _orch(request)
    from ..database import get_clusters_with_fixtures, get_cluster_stats
    from ..fixtures import FIXTURE_TYPE_LABELS, user_selectable_types

    circuits_ctx = []
    total_unreviewed = 0

    for circ_cfg in orch._cfg.circuits:
        c = circ_cfg.circuit
        training = (
            orch.training_manager.get_training_info(c)
            if orch.training_manager
            else {"state": "idle"}
        )
        clusters_raw = get_clusters_with_fixtures(orch.db, c)
        clusters = []
        for cl in clusters_raw:
            stats = get_cluster_stats(orch.db, c, cl["id"])
            clusters.append({**cl, **stats})

        unreviewed = sum(1 for cl in clusters if not cl.get("fixture_id"))
        total_unreviewed += unreviewed
        circuits_ctx.append({
            "circuit":          c,
            "display_name":     circ_cfg.display_name,
            "training_state":   training.get("state", "idle"),
            "clusters":         clusters,
            "unreviewed_count": unreviewed,
        })

    return _tmpl(request).TemplateResponse("fixtures.html", {
        "request":               request,
        "page":                  "fixtures",
        "circuits":              circuits_ctx,
        "total_unreviewed":      total_unreviewed,
        "fixture_type_labels":   FIXTURE_TYPE_LABELS,
        "user_selectable_types": user_selectable_types(),
    })


# ── Re-run clustering ─────────────────────────────────────────────────────────

@router.post("/{circuit}/cluster")
async def retrigger_cluster(request: Request, circuit: str):
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


# ── Confirm / save a cluster label ────────────────────────────────────────────

@router.post("/{circuit}/cluster/{cluster_id}/confirm")
async def confirm_cluster(request: Request, circuit: str, cluster_id: int):
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
    return ingress_redirect(request, "/fixtures")


# ── Delete a cluster ──────────────────────────────────────────────────────────

@router.post("/{circuit}/cluster/{cluster_id}/delete")
async def delete_cluster_endpoint(request: Request, circuit: str, cluster_id: int):
    orch = _orch(request)
    from ..database import delete_cluster, get_fixture_id_for_cluster
    fixture_id = get_fixture_id_for_cluster(orch.db, circuit, cluster_id)
    delete_cluster(orch.db, circuit, cluster_id)
    if fixture_id:
        fp = getattr(orch, "_fixture_publisher", None)
        if fp:
            fp.retract_fixture(fixture_id)
    return ingress_redirect(request, "/fixtures")


# ── JSON API ──────────────────────────────────────────────────────────────────

@router.get("/api/{circuit}/clusters")
async def api_clusters(request: Request, circuit: str):
    from ..database import get_clusters_with_fixtures, get_cluster_stats
    db = _orch(request).db
    clusters = []
    for cl in get_clusters_with_fixtures(db, circuit):
        stats = get_cluster_stats(db, circuit, cl["id"])
        clusters.append({**cl, **stats})
    return JSONResponse(clusters)
