"""Shared helpers for routers.

HTTP status-code convention (plan C-IQ-24)
==========================================

Routers should follow these codes consistently. If you find yourself
reaching for something outside this list, add a comment explaining why.

  400  Bad Request    — request payload is malformed, missing a required
                        field, fails enum / format / range validation,
                        or otherwise fails an input check that doesn't
                        depend on server state.

  403  Forbidden      — authentication / authorization failure. CSRF
                        token mismatch (handled by ingress middleware)
                        is the main case here; route handlers rarely
                        need to return 403 directly.

  404  Not Found      — the resource referenced by the path / form
                        doesn't exist. Use for unknown circuit, missing
                        HA entity in the addon's role map, event id
                        not in the events table, waveform row not in
                        event_waveforms, etc.

  409  Conflict       — request is well-formed AND the user is allowed
                        to make it in principle, but the *current
                        server state* forbids it. Examples: trying to
                        flip a circuit to "zone" while non-zone
                        fixtures are attached; trying to re-enter
                        setup after it's already complete.

  413  Payload Too Large — body exceeds the size limit (backup upload).

  502  Bad Gateway    — upstream service (Home Assistant REST / WS)
                        failed. Used by valve / button / number calls
                        that round-trip through HA.

  503  Service Unavailable — orchestrator is still starting up; the
                        component the route needs isn't ready yet.
"""
from __future__ import annotations

from fastapi import Request
from fastapi.responses import RedirectResponse

# Re-export coerce_int so existing `from ._helpers import coerce_int`
# imports keep working. The implementation lives in `..forms` so it
# can be unit-tested without pulling in FastAPI.
from ..forms import coerce_int  # noqa: F401


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
