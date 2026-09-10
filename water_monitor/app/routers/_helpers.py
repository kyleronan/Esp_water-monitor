"""Shared helpers for routers.

Async-safety convention
=======================

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

run_blocking is DB-ONLY
-----------------------
`run_blocking` dispatches to `database.run_db()` — the single-thread
DB executor. The shared connection is `check_same_thread=False` and
must be touched from exactly ONE thread, ever.

Blocking work that does NOT touch the DB (HA I/O, file writes,
subprocess) must NOT use this helper — call
`loop.run_in_executor(None, ...)` directly so it stays on the default
pool. Putting non-DB work here would serialize it behind DB traffic
for no reason, and — because a `run_db` callable may never itself
submit to `run_db` and wait (single worker → deadlock) — it would
also create a re-entrancy foot-gun.


HTTP status-code convention
===========================

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

import logging
from typing import Any, Callable, TypeVar

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

# Re-export coerce_int so existing `from ._helpers import coerce_int`
# imports keep working. The implementation lives in `..forms` so it
# can be unit-tested without pulling in FastAPI.
from ..forms import coerce_float, coerce_int
from ..config import DB_PATH
from ..database import (finish_job, is_baseline_locked,
                        run_db, run_isolated_write, start_job)

__all__ = ["_json", "_orch", "_tmpl", "coerce_float", "coerce_int",
           "dev_tools_disabled", "ingress_redirect", "reclassify_in_background",
           "run_blocking", "startup_gate", "unknown_circuit"]

log = logging.getLogger(__name__)


T = TypeVar("T")


def _orch(request: Request):
    """The Orchestrator, off app.state. Every router reaches for it."""
    return request.app.state.orchestrator


def _tmpl(request: Request):
    """The Jinja environment, off app.state."""
    return request.app.state.templates


async def _json(request: Request) -> dict:
    """The request body as a dict, or ``{}`` for anything unparseable.

    A missing or malformed body is a 400 the handler raises itself with a
    field-specific message; this only has to not blow up first.
    """
    try:
        return await request.json() or {}
    except Exception:
        return {}


async def reclassify_in_background(circuit: str, *, skip_when_baseline_locked: bool,
                                   what: str) -> None:
    """Fire-and-forget k-NN reclassify, offloaded so the POST returns at once.

    Runs on a private connection under the write lock (``run_isolated_write``)
    so it never races live writes, and is tracked as a job so a FAILURE reaches
    the UI; success is silent.

    ``skip_when_baseline_locked`` is the label-change caller's rule. The
    classifier is fit-once-at-activation then hard-locked, so a relabel after
    the training window must not re-walk history — it spreads only to the
    event's own cycle-mates. The full pass still runs during the training
    window, at startup, and on explicit recalibration. The training checklist
    has no such rule and passes False.
    """
    from ..reclassify import reclassify_all_events_from_signatures

    def _work(c):
        if skip_when_baseline_locked and is_baseline_locked(c, circuit):
            log.info("[%s] label-triggered reclassify skipped — baseline locked "
                     "(training window closed); relabel stays local", circuit)
            return
        job = start_job(c, "reclassify", circuit, "Reclassifying events…")
        try:
            reclassify_all_events_from_signatures(c, circuit)
            finish_job(c, job, "done", "Reclassify complete")
        except Exception:
            finish_job(c, job, "error", "Reclassify failed — see addon log")
            raise

    try:
        await run_isolated_write(DB_PATH, _work)
    except Exception as e:
        log.warning("[%s] %s reclassify failed: %s", circuit, what, e)


def dev_tools_disabled() -> JSONResponse:
    """The 404 the ``/dev/*`` routes return when the add-on option is off.

    Six handlers in settings.py wrote this body inline. Kept as a returned
    response rather than a ``Depends`` that raises, because ``HTTPException``
    renders ``{"detail": ...}`` and these routes answer ``{"error": ...}``.
    """
    return JSONResponse({"error": "dev tools disabled"}, status_code=404)


def unknown_circuit(circuit: str) -> JSONResponse:
    """The 404 a circuit-scoped route returns when the circuit is not configured.

    Eight handlers across device.py and settings.py returned this same body and
    status inline; one definition keeps them from drifting.
    """
    return JSONResponse(
        {"status": "error", "message": f"Unknown circuit: {circuit}"},
        status_code=404,
    )


async def run_blocking(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a blocking **DB** helper on the single DB thread and await it.

    Dispatches to ``database.run_db`` — the one-worker DB executor — so
    page renders can never touch the shared connection concurrently with
    startup/reseed work (the 8/15 + 8/16 ``InterfaceError``).

    NOT for non-DB blocking work: see the module docstring.
    """
    return await run_db(fn, *args, **kwargs)


def startup_gate(request: Request, page: str, title: str,
                 retry_path: str):
    """Readiness gate for pages with heavy DB work.

    Returns a rendered "still starting" response when the orchestrator's
    startup replay is still running, else ``None`` (caller proceeds).

    PROACTIVE rather than an exception handler: every DB touch goes through
    ONE worker thread, so a page opened during startup QUEUES behind the boot
    pass instead of failing. Checking readiness BEFORE submitting gives the
    user an instant, honest answer instead of a request that hangs.

    Wording is shared with the Water Use page's 'starting' flash so the
    add-on says the same thing wherever this state surfaces.
    """
    orch = getattr(request.app.state, "orchestrator", None)
    # `startup_pages_ready`, NOT `startup_cluster_work_done`. The two answer
    # different questions and the difference is ~145 s: the cluster-work flag
    # means "every job that touches cluster references has finished" (what the
    # repair route and the study export need), while a page only needs the
    # cluster engine rebuilt and wired. Gating pages on the stricter flag
    # leaves the operator staring at a notice for the whole background
    # classification pass.
    if orch is None or getattr(orch, "startup_pages_ready", True):
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
