"""Help / operator runbook page (dev46 46o).

Deliberately its own router with NO admin dependency: this page explains what
the controls do, and the person most likely to need that explanation is the
one with the fewest permissions. It reads nothing and writes nothing, so there
is nothing to gate.

The content lives in the template rather than in a markdown file rendered at
request time — it ships with the build, so it can never describe a version
other than the one running.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

log = logging.getLogger(__name__)
router = APIRouter(prefix="/help")


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def help_page(request: Request):
    """Static page — no DB access, so no readiness gate and no run_db hop."""
    return request.app.state.templates.TemplateResponse("help.html", {
        "request": request,
        "page": "help",
    })
