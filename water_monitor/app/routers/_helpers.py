"""Shared helpers for routers.

Async-safety convention (plan C-IQ-4 follow-up)
================================================

sqlite3 is sync. Multi-second queries inside an `async def` path
block the event loop and stall every other ingress request for the
duration. The orchestrator's startup hot path
(`rebuild_from_db` / `backfill_unmatched`) already wraps with
`loop.run_in_executor(...)`; route handlers should do the same when
their DB work is non-trivial.

Cheap single-row UPSERTs / SELECTs (e.g. `set_circuit_type`,
`upsert_circuit_label`) are NOT worth wrapping — the executor
context-switch overhead outweighs the actual query time. Wrap when:

  - The handler iterates over circuits / events doing N queries
    inline (e.g. history page assembles event lists, leak-test
    history, and daily summaries for every circuit).
  - The handler runs a join or aggregation that touches more than
    a few thousand rows.
  - The handler does any blocking I/O other than SQLite (file
    write, subprocess, etc.) — wrap to keep the loop responsive.

Use `run_blocking(fn, *args, **kwargs)` for one-off offloads. For
hot paths, extract a `_xxx_sync(...)` helper that bundles ALL the
sync DB calls so the executor hop happens once.


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

import asyncio
from typing import Any, Callable, TypeVar

from fastapi import Request
from fastapi.responses import RedirectResponse

# Re-export coerce_int so existing `from ._helpers import coerce_int`
# imports keep working. The implementation lives in `..forms` so it
# can be unit-tested without pulling in FastAPI.
from ..forms import coerce_int

__all__ = ["coerce_int", "ingress_redirect", "run_blocking"]


T = TypeVar("T")


async def run_blocking(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run `fn(*args, **kwargs)` on the default thread executor.

    Thin wrapper over ``loop.run_in_executor`` for the common case
    "call this sync helper and await its result". Use it to offload
    heavy SQLite work (or any other blocking I/O) from async route
    handlers so a long-running DB query doesn't stall every other
    ingress request.

    See the module docstring for guidance on when to wrap and when
    to leave sync calls inline.
    """
    loop = asyncio.get_running_loop()
    # functools.partial avoids creating a closure per call; lambda is
    # fine here too and keeps the surface area small.
    return await loop.run_in_executor(
        None, lambda: fn(*args, **kwargs),
    )


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
