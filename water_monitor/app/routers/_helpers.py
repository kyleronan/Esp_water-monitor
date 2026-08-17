"""Shared helpers for routers.

Async-safety convention (plan C-IQ-4 follow-up)
================================================

sqlite3 is sync. Multi-second queries inside an `async def` path
block the event loop and stall every other ingress request for the
duration. The orchestrator's startup hot path
(`rebuild_from_db` / `backfill_unmatched`) dispatches via
`database.run_db(...)`; route handlers should do the same when
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

dev46 (46a) — run_blocking is DB-ONLY
-------------------------------------
Every caller of `run_blocking` passes a helper that takes `orch.db`,
so it now dispatches to `database.run_db()` — the single-thread DB
executor. The shared connection is `check_same_thread=False` and must
be touched from exactly ONE thread, ever.

Blocking work that does NOT touch the DB (HA I/O, file writes,
subprocess) must NOT use this helper — call
`loop.run_in_executor(None, ...)` directly so it stays on the default
pool. Putting non-DB work here would serialize it behind DB traffic
for no reason, and — because a `run_db` callable may never itself
submit to `run_db` and wait (single worker → deadlock) — it would
also create a re-entrancy foot-gun.


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

from typing import Any, Callable, TypeVar

from fastapi import Request
from fastapi.responses import RedirectResponse

# Re-export coerce_int so existing `from ._helpers import coerce_int`
# imports keep working. The implementation lives in `..forms` so it
# can be unit-tested without pulling in FastAPI.
from ..forms import coerce_int

__all__ = ["coerce_int", "ingress_redirect", "run_blocking", "startup_gate"]


T = TypeVar("T")


async def run_blocking(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a blocking **DB** helper on the single DB thread and await it.

    dev46 (46a): dispatches to ``database.run_db`` — the one-worker DB
    executor — so page renders can never touch the shared connection
    concurrently with startup/reseed work (the 8/15 + 8/16
    ``InterfaceError``). All four callers pass ``orch.db``-taking
    helpers, so the wholesale routing is correct.

    NOT for non-DB blocking work: see the module docstring.
    """
    from ..database import run_db
    return await run_db(fn, *args, **kwargs)


def startup_gate(request: Request, page: str, title: str,
                 retry_path: str):
    """dev46 (46c) — readiness gate for pages with heavy DB work.

    Returns a rendered "still starting" response when the orchestrator's
    startup replay is still running, else ``None`` (caller proceeds).

    Why a PROACTIVE check rather than an exception handler: 46a routes every
    DB touch through ONE worker thread, so a page opened during startup no
    longer 500s with an ``InterfaceError`` — it QUEUES behind the boot pass.
    Checking readiness BEFORE submitting means the user gets an instant,
    honest answer instead of a request that hangs until it times out.

    Wording is shared with the Water Use page's 'starting' flash so the
    add-on says the same thing wherever this state surfaces.
    """
    orch = getattr(request.app.state, "orchestrator", None)
    if orch is None or getattr(orch, "startup_cluster_work_done", True):
        return None
    templates = request.app.state.templates
    return templates.TemplateResponse("starting.html", {
        "request":    request,
        "page":       page,
        "page_title": title,
        "retry_path": retry_path,
    }, status_code=503)


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
