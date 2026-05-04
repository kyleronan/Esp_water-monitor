"""FastAPI entrypoint — boots the orchestrator and serves the web UI."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import load_config
from .db_migrations import run_migrations
from .orchestrator import Orchestrator
from .routers import dashboard, device, history, fixtures, settings, setup, backup

APP_DIR = Path(__file__).resolve().parent
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load_config()
    log_level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Suppress noisy third-party loggers regardless of app log level
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("websockets.client").setLevel(logging.WARNING)
    logging.getLogger("websockets.server").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("multipart").setLevel(logging.WARNING)
    logging.getLogger("multipart.multipart").setLevel(logging.WARNING)
    log = logging.getLogger(__name__)
    log.info("Water Monitor starting — %d circuits configured",
             len(cfg.circuits))

    orch = Orchestrator(cfg)
    app.state.orchestrator = orch

    # Initialise DB and run migrations before the orchestrator's async run()
    # so the schema is fully up to date before any component starts.
    from .database import init_db
    from .config import DB_PATH
    _db = init_db(DB_PATH)
    try:
        run_migrations(_db)
    except Exception as e:
        log.error("DB migration error (non-fatal): %s", e)

    # Custom Jinja2Templates that automatically injects ingress_path
    # into every template context so nav links and form actions work
    # correctly behind the HA ingress proxy.
    class IngressTemplates(Jinja2Templates):
        # Cache one CSRF token per process — rotated every hour
        _csrf_cache: dict = {"token": None, "expires": 0}

        def TemplateResponse(self, name, context, **kwargs):
            request = context.get("request")
            if request:
                context.setdefault(
                    "ingress_path",
                    getattr(request.state, "ingress_path", "")
                )
                # Return a cached CSRF token — generate a new one only when
                # the cache is empty or the token has expired (1 hour).
                # This avoids a DB write on every page render while still
                # rotating tokens regularly.
                import time
                now = time.time()
                cache = IngressTemplates._csrf_cache
                orch_ref = getattr(request.app.state, "orchestrator", None)
                if not cache["token"] or now > cache["expires"]:
                    if orch_ref:
                        from .database import generate_csrf_token
                        cache["token"]   = generate_csrf_token(orch_ref.db)
                        cache["expires"] = now + 3600  # 1 hour
                context.setdefault("csrf_token", cache["token"] or "")
            # Inject unit conversion context so every template and the JS
            # window.UNITS global have the correct factors and labels.
            if orch_ref and orch_ref.db:
                from .units import load_unit_context
                uc = load_unit_context(orch_ref.db)
                for k, v in uc.items():
                    context.setdefault(k, v)
            else:
                # Fallback defaults before DB is ready
                from .units import build_unit_context
                for k, v in build_unit_context("L/min", "psi").items():
                    context.setdefault(k, v)
            return super().TemplateResponse(name, context, **kwargs)

    app.state.templates = IngressTemplates(
        directory=str(APP_DIR / "templates"))

    # Register tojson filter (not included by default in FastAPI's Jinja2).
    # Must return Markup so Jinja2 autoescape does not HTML-encode the JSON
    # (which would turn & → &amp; and break JavaScript parsing).
    import json as _json
    from markupsafe import Markup as _Markup
    app.state.templates.env.filters["tojson"] = (
        lambda v: _Markup(_json.dumps(v))
    )

    runner = asyncio.create_task(orch.run())

    try:
        yield
    finally:
        log.info("Water Monitor shutting down")
        orch.stop()
        runner.cancel()
        try:
            await runner
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(
    lifespan=lifespan,
    title="Water Monitor",
    redirect_slashes=False,  # prevent /setup → /setup/ redirects that break ingress
)

app.mount(
    "/static",
    StaticFiles(directory=str(APP_DIR / "static")),
    name="static",
)


@app.middleware("http")
async def ingress_middleware(request: Request, call_next):
    # Store ingress path from HA proxy header for use in templates
    ingress_path = request.headers.get("X-Ingress-Path", "").rstrip("/")
    request.state.ingress_path = ingress_path

    # Log every POST so we can see what's reaching the addon
    if request.method == "POST":
        log.info("POST %s (ingress=%r)", request.url.path, ingress_path)

    # CSRF protection for state-changing form POSTs.
    # JSON API endpoints (Content-Type: application/json) are exempt —
    # they can only be called from same-origin JS in the HA ingress context.
    # Setup and backup endpoints are exempt (backup import uses multipart
    # without a session token; setup is first-run).
    #
    # IMPORTANT: BaseHTTPMiddleware (used by @app.middleware) consumes the
    # request body when it parses the form. If we don't replay the bytes,
    # the downstream route handler's own `await request.form()` returns
    # empty. We read the body once, validate CSRF on a parsed copy, then
    # rewrite the request._receive callable so the handler sees the
    # original body intact.
    csrf_exempt = ("/setup", "/backup", "/api/", "/static", "/health")
    if request.method == "POST" and not any(
            request.url.path.startswith(p) for p in csrf_exempt):
        ct = request.headers.get("Content-Type", "")
        if "application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct:
            # Drain the body once
            body_bytes = await request.body()

            # Make subsequent reads return the same bytes
            async def receive_replay():
                return {
                    "type": "http.request",
                    "body": body_bytes,
                    "more_body": False,
                }
            request._receive = receive_replay

            # Parse a copy of the form just for CSRF validation
            form_data = await request.form()
            token = form_data.get("_csrf", "")
            orch = getattr(request.app.state, "orchestrator", None)
            if orch:
                from .database import validate_csrf_token
                if not validate_csrf_token(orch.db, token):
                    log.warning("CSRF token invalid on POST %s",
                                request.url.path)
                    return HTMLResponse(
                        "<h1>403 — Invalid or missing security token</h1>"
                        "<p>Please reload the page and try again.</p>",
                        status_code=403,
                    )

            # Reset the form cache so the handler re-parses from the
            # replayed body. Starlette caches the parsed form on the
            # request; clearing the cache forces a fresh parse.
            if hasattr(request, "_form"):
                request._form = None

    path = request.url.path
    skip_paths = ("/setup", "/static", "/health", "/backup")
    if not any(path.startswith(p) for p in skip_paths):
        orch = getattr(request.app.state, "orchestrator", None)
        if orch and not orch.setup_complete:
            ingress_path = getattr(request.state, "ingress_path", "")
            # Sanitise the HA-supplied ingress path before embedding in HTML.
            # Allow only URL-safe path characters; strip anything else to
            # prevent header-injection attacks.
            import re as _re
            ingress_path = _re.sub(r"[^/a-zA-Z0-9_\-]", "", ingress_path)
            setup_url = f"{ingress_path}/setup"
            return HTMLResponse(
                content=(
                    f"<!doctype html><html><head>"
                    f'<meta http-equiv="refresh" content="0; url={setup_url}">'
                    f"</head><body>"
                    f'<script>window.location.replace("{setup_url}");</script>'
                    f"Redirecting to setup\u2026"
                    f"</body></html>"
                ),
                status_code=200,
            )

    response = await call_next(request)
    return response


app.include_router(setup.router)
app.include_router(dashboard.router)
app.include_router(device.router)
app.include_router(history.router)
app.include_router(fixtures.router)
app.include_router(settings.router)
app.include_router(backup.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
