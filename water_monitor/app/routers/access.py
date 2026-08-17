"""Access router — admin-only RBAC management (Settings → Access).

Lets an admin grant/revoke the **operator** tier (read-only + open/close the main
valve) to non-admin Home Assistant users. Admin status itself is derived
automatically from HA (config/auth/list) and is NOT editable here.

The whole router is admin-gated by ``require_admin`` (defence in depth — the
central mutation gate in ``ingress_middleware`` also blocks non-admins from these
POSTs, and the nav link is hidden from non-admins).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse

from ..auth import (ADMIN, OPERATOR, REMOTE_USER_ID_HEADER, require_admin,
                    resolve_role)
from ..database import (
    add_operator,
    list_seen_users,
    load_operator_ids,
    remove_operator,
)
from ._helpers import ingress_redirect

log = logging.getLogger(__name__)

router = APIRouter(prefix="/access", dependencies=[Depends(require_admin)])


def _orch(request: Request):
    return request.app.state.orchestrator


def _templates(request: Request):
    return request.app.state.templates


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def access_page(request: Request):
    """Render the access-management page: every known HA user with its resolved
    role and an operator toggle for non-admins."""
    orch = _orch(request)
    from ..database import run_db
    db = orch.db
    # dev46 (46a): every DB read on this page goes through the single DB
    # thread. The seen-users fallback below is fetched here too — one hop,
    # and the branch that uses it is decided after the HA call returns.
    operator_ids, seen_users = await run_db(
        lambda: (load_operator_ids(db), list_seen_users(db)))
    admin_ids = set(getattr(orch, "admin_ids", set()))

    # Primary source: live HA user list. Falls back to the seen-users log if the
    # add-on token can't read config/auth/list (so the page is still usable).
    ha_users = []
    ha_error = None
    if getattr(orch, "ha", None) is not None:
        try:
            ha_users = await orch.ha.get_users()
        except Exception as e:
            ha_error = str(e)
            log.warning("Access page: config/auth/list failed: %s", e)

    # ONE role decision (auth.resolve_role — the same precedence the middleware
    # enforces; a private copy here could drift and display roles the mutation
    # gate doesn't actually grant). Live HA admins are unioned into the set the
    # same way the role-sync does before the middleware ever sees them.
    admin_ids |= {u["id"] for u in ha_users if u.get("is_admin") and u.get("id")}

    users = []
    if ha_users:
        for u in ha_users:
            uid = u["id"]
            users.append({
                "id": uid,
                "name": u.get("name") or uid,
                "is_ha_admin": bool(u.get("is_admin")),
                "role": resolve_role(uid, admin_ids, operator_ids),
            })
    else:
        # Fallback: users we've actually seen open the add-on.
        for s in seen_users:
            uid = s["user_id"]
            users.append({
                "id": uid,
                "name": s.get("display_name") or uid,
                "is_ha_admin": uid in admin_ids,
                "role": resolve_role(uid, admin_ids, operator_ids),
            })
        # Make sure current operators always show even if never "seen".
        shown = {u["id"] for u in users}
        for uid in operator_ids - shown:
            users.append({"id": uid, "name": uid, "is_ha_admin": False,
                          "role": OPERATOR})

    users.sort(key=lambda u: (u["role"] != ADMIN, u["role"] != OPERATOR,
                              u["name"].lower()))

    return _templates(request).TemplateResponse("access.html", {
        "request": request,
        "page": "access",
        "users": users,
        "ha_error": ha_error,
        "operator_count": len(operator_ids),
        "admin_count": sum(1 for u in users if u["role"] == ADMIN),
    })


@router.post("/operator/grant")
async def grant_operator(
    request: Request,
    user_id: str = Form(...),
    display_name: str = Form(default=""),
):
    """Grant the operator tier (read + valve control) to a HA user."""
    orch = _orch(request)
    actor = request.headers.get(REMOTE_USER_ID_HEADER, "")
    from ..database import run_db

    def _grant():
        # dev46 (46a): the write AND the allow-list reload it invalidates are
        # both DB work — one callable on the single DB thread keeps them
        # together (reload_operator_ids re-SELECTs the operator set).
        add_operator(orch.db, user_id.strip(), display_name.strip(), actor)
        orch.reload_operator_ids()

    await run_db(_grant)
    log.info("RBAC: operator granted to %r by %r", user_id, actor)
    return ingress_redirect(request, "/access")


@router.post("/operator/revoke")
async def revoke_operator(
    request: Request,
    user_id: str = Form(...),
):
    """Revoke the operator tier from a HA user (they fall back to viewer)."""
    orch = _orch(request)
    actor = request.headers.get(REMOTE_USER_ID_HEADER, "")
    from ..database import run_db

    def _revoke():
        # dev46 (46a): write + allow-list reload in one DB-thread callable.
        remove_operator(orch.db, user_id.strip())
        orch.reload_operator_ids()

    await run_db(_revoke)
    log.info("RBAC: operator revoked from %r by %r", user_id, actor)
    return ingress_redirect(request, "/access")
