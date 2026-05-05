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
    orch = _orch(request)
    if not orch.training_manager:
        return ingress_redirect(request, "/fixtures?msg=no_tm")
    try:
        await orch.training_manager.retrigger_clustering(circuit)
        msg = "reclustered"
    except ValueError as e:
        log.info("[%s] re-cluster skipped: %s", circuit, e)
        msg = "too_few_events"
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

    from ..database import upsert_fixture_from_cluster
    upsert_fixture_from_cluster(
        _orch(request).db, circuit, cluster_id, name, fixture_type, publish
    )
    return ingress_redirect(request, "/fixtures")


# ── Delete a cluster ──────────────────────────────────────────────────────────

@router.post("/{circuit}/cluster/{cluster_id}/delete")
async def delete_cluster_endpoint(request: Request, circuit: str, cluster_id: int):
    from ..database import delete_cluster
    delete_cluster(_orch(request).db, circuit, cluster_id)
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
