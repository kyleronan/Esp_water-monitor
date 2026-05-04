"""Shared helpers for routers."""
from __future__ import annotations

from fastapi import Request
from fastapi.responses import RedirectResponse


def ingress_redirect(
    request: Request,
    path: str,
    status_code: int = 303,
) -> RedirectResponse:
    """
    Build a RedirectResponse that includes the HA ingress prefix.
    Without this, redirects break behind HA's ingress proxy because
    they go to absolute paths that don't include the ingress token.
    """
    ingress_path = getattr(request.state, "ingress_path", "")
    return RedirectResponse(f"{ingress_path}{path}", status_code=status_code)
