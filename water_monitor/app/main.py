"""FastAPI entrypoint — boots the orchestrator and serves the web UI."""
from __future__ import annotations

import asyncio
import logging
import re as _re
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import (
    http_exception_handler as fastapi_http_exception_handler)
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import select_autoescape

import os as _os

from .auth import (
    ADMIN,
    OPERATOR,
    REMOTE_USER_ID_HEADER,
    REMOTE_USER_NAME_HEADER,
    VIEWER,
    check_csrf_token,
    is_mutation_allowed,
    issue_csrf_token,
    role_for_request,
)
from .config import load_config
from .database import (
    get_or_create_csrf_server_secret,
    record_seen_user,
    run_isolated_write,
)

# Ingress IP guard — only accept requests from the HA supervisor ingress proxy.
# Override via env vars for non-standard deployments and local/pytest runs.
_INGRESS_IP  = _os.environ.get("INGRESS_ALLOWED_IP", "172.30.32.2")
_DEV_MODE    = _os.environ.get("DEV_MODE", "false").lower() in ("true", "1", "yes")

# Session cookie used to bind a browser to its CSRF token via HMAC
# double-submit. Persistent (30 days) and re-set on first response only.
#
# The name changed from "wm_session" in unit 2.28, and it had to. The old cookie
# was scoped path="/", and a browser that already holds it keeps sending it —
# so the middleware never sees a cookie-less client, never calls set_cookie
# again, and the new path/SameSite would have reached NOBODY who had ever opened
# the add-on. A new name forces exactly one re-issue per browser, under the new
# scope; the stale one is explicitly deleted below.
SESSION_COOKIE         = "wm_sid"
LEGACY_SESSION_COOKIE  = "wm_session"   # pre-2.28, path="/" — deleted on sight
SESSION_COOKIE_MAX_AGE = 30 * 86400  # 30 days

# Session ids are 64 hex chars (32 bytes). Anything shorter / different
# format means the cookie was tampered with or rotated — treat as new.
SESSION_COOKIE_MIN_LEN = 16

# Cookie path. The Supervisor serves every ingress add-on under
# /api/hassio_ingress/<token>/, so this prefix is the narrowest scope that is
# still STABLE: the <token> segment is per-add-on and the add-on cannot assume
# it is durable, and it only ever reaches us through the X-Ingress-Path REQUEST
# header, which this codebase already treats as untrusted (see the sanitising
# re.sub on the setup redirect below). Deriving a cookie path from that header
# would mean one odd value breaks every POST in the app — for zero security
# gain, because scoping to our own token does NOT keep a sibling add-on out
# (see the ACCEPTED RISK note in ingress_middleware).
#
# What it does buy, which path="/" did not: the cookie stops riding along on
# Home Assistant's OWN requests — /api/websocket, /api/states, /auth/*, and
# every frontend fetch on the HA origin.
INGRESS_PATH_PREFIX = "/api/hassio_ingress/"


def _new_session_id() -> str:
    import secrets as _s
    return _s.token_hex(32)


def _is_secure_request(request: Request) -> bool:
    """Whether the outer connection (browser -> ingress) was HTTPS.

    HA ingress terminates TLS and the proxied request reaches the addon
    over plain HTTP, so checking ``request.url.scheme`` alone is
    misleading. Trust ``X-Forwarded-Proto`` when present, and default
    to Secure in production (the addon is normally served behind
    ingress over HTTPS). ``DEV_MODE=true`` opts out for local pytest
    runs against plain HTTP loopback.
    """
    if _DEV_MODE:
        return False
    proto = request.headers.get("X-Forwarded-Proto", "").lower()
    if proto in ("https", "http"):
        return proto == "https"
    return True


def _cookie_path(request: Request) -> str:
    """Scope for the session cookie — see INGRESS_PATH_PREFIX.

    Returns the ingress prefix when this request actually arrived through the
    Supervisor ingress proxy, and "/" otherwise (DEV_MODE / local runs / pytest,
    where the app is served from the root and a prefix-scoped cookie would never
    come back). Only ever one of two constant values, so a browser can never end
    up holding two same-named cookies at different paths.
    """
    if _DEV_MODE:
        return "/"
    if request.headers.get("X-Ingress-Path", "").startswith(
            INGRESS_PATH_PREFIX):
        return INGRESS_PATH_PREFIX
    # Reached only if HA ever moves ingress off /api/hassio_ingress/. Falling
    # back to the pre-2.28 scope keeps the add-on working rather than 403-ing
    # every POST behind a cookie the browser will not send back.
    return "/"


def _set_session_cookie(response, request: Request, session_id: str) -> None:
    """Attach the session cookie. Shared by the normal new-session response and the
    CSRF-reject 403 so a cookie-less client always leaves with a session it can reuse.
    The reject path returns before the normal cookie-set, so without this a client
    whose cookie never round-trips could never present a matching token — the frontend
    reloads on 403, which then derives a valid token for this same session.

    ``samesite="strict"`` (was "lax", unit 2.28): ingress is served from the Home
    Assistant origin itself, and the panel is entered as a same-origin iframe from
    the HA sidebar, so every legitimate request to this add-on — navigation, form
    POST and fetch alike — is same-site and carries a Strict cookie. The one case
    Strict withholds it that Lax would not is a top-level navigation arriving from
    a genuinely different site (an emailed deep link); that request simply mints a
    new session and renders a page whose token matches it, so nothing breaks.
    """
    response.set_cookie(
        SESSION_COOKIE,
        session_id,
        httponly=True,
        samesite="strict",
        secure=_is_secure_request(request),
        max_age=SESSION_COOKIE_MAX_AGE,
        path=_cookie_path(request),
    )
    # One-time cleanup of the pre-2.28 path="/" cookie, which is still being sent
    # to every path on the HA origin until it expires. Conditional on the browser
    # actually presenting it, so this costs nothing once the fleet has rolled over.
    if LEGACY_SESSION_COOKIE in request.cookies:
        # secure/httponly mirrored from the original set so the overwrite is not
        # refused by a browser's "leave secure cookies alone" rule.
        response.delete_cookie(
            LEGACY_SESSION_COOKIE, path="/",
            secure=_is_secure_request(request), httponly=True)


def _is_health_path(path: str) -> bool:
    """Exact-match /health (with optional trailing slash). Prefix
    `/health-anything` is NOT exempt — that was the old startswith bug.
    """
    return path == "/health" or path == "/health/"


def _is_static_path(path: str) -> bool:
    """/static/foo.css is an asset path. GET-only by design; POST/PUT
    to /static are never legitimate routes."""
    return path.startswith("/static/")


from .db_migrations import run_migrations
from .orchestrator import Orchestrator
from .routers import (dashboard, device, history, fixtures, settings, setup,
                      backup, help, training, calibration, access)
from .units import build_unit_context, load_unit_context

APP_DIR = Path(__file__).resolve().parent
log = logging.getLogger(__name__)


def _read_addon_version() -> str:
    """Read the addon `version:` from config.yaml (one line, no YAML dep).

    Used as the static-asset cache-buster so a version bump on deploy busts
    the browser's cached styles.css/JS. Falls back to 'dev' if unreadable.
    """
    try:
        cfg_path = APP_DIR.parent / "config.yaml"
        for line in cfg_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s.startswith("version:"):
                return s.split(":", 1)[1].strip().strip("\"'") or "dev"
    except Exception:
        pass
    return "dev"


class IngressTemplates(Jinja2Templates):
    """Jinja2Templates that auto-injects ingress_path, CSRF token, and
    unit context into every template response.

    CSRF token comes from ``request.state.csrf_token`` which the
    middleware computes once per request from the browser's session_id
    cookie + the persisted server secret. No DB writes per render and
    no shared process-wide cache.
    """

    def TemplateResponse(self, name, context, **kwargs):
        request = context.get("request")
        orch_ref = None
        if request:
            context.setdefault(
                "ingress_path",
                getattr(request.state, "ingress_path", "")
            )
            orch_ref = getattr(request.app.state, "orchestrator", None)
            context.setdefault(
                "csrf_token",
                getattr(request.state, "csrf_token", ""),
            )
            # RBAC role flags — templates use these to hide controls the current
            # role can't use (enforcement is server-side; this is presentation).
            _role = getattr(request.state, "role", VIEWER)
            context.setdefault("role", _role)
            context.setdefault("is_admin", _role == ADMIN)
            context.setdefault("can_control_valve", _role in (ADMIN, OPERATOR))
            # Static-asset cache-buster (see _read_addon_version). Defaults to
            # 'dev' before lifespan sets it / outside the app context.
            context.setdefault(
                "asset_version",
                getattr(request.app.state, "asset_version", "dev"),
            )
            # Hide the top-level Setup tab once initial setup is complete;
            # the wizard is locked anyway, so the tab would be a dead-end.
            # Re-runs go through Settings -> Re-run Setup, which flips this
            # back to 0 and the tab reappears.
            context.setdefault(
                "setup_complete",
                bool(getattr(orch_ref, "setup_complete", False)),
            )
        # Inject unit conversion context so every template and the JS
        # window.UNITS global have the correct factors and labels.
        if orch_ref and orch_ref.db:
            uc = load_unit_context(orch_ref.db)
            for k, v in uc.items():
                context.setdefault(k, v)
        else:
            # Fallback defaults before DB is ready
            for k, v in build_unit_context("L/min", "psi").items():
                context.setdefault(k, v)
        return super().TemplateResponse(name, context, **kwargs)


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
    # dev46 (46g): announce the running build. Every deploy in the 2026-08
    # arc needed migration log lines as a proxy for "did the new image
    # actually start" — twice the supervisor served a stale build and the
    # only tell was a missing migration. Best-effort: version/commit read
    # failures must never block boot.
    try:
        from .event_detector import _read_addon_version, _read_git_commit
        _ver = _read_addon_version() or "unknown"
        _commit = _read_git_commit()
        _build = f"v{_ver}" + (f" ({_commit})" if _commit else "")
        # dev46 (46k) — the build fingerprint, so two rebuilds of the SAME dev
        # version are distinguishable in the log. The container has no .git, so
        # without this every rebuild of a dev cycle prints an identical line and
        # "did my change actually deploy?" cannot be answered from the log —
        # which cost real time on 2026-08-17. Same value the verdict stamp uses,
        # so a changed fingerprint here also explains why that boot re-derived
        # every label.
        from .database import _code_fingerprint
        _build += f" build:{_code_fingerprint()[:8]}"
    except Exception:
        _build = "version unknown"
    log.info("Water Monitor %s starting — %d circuits configured",
             _build, len(cfg.circuits))
    # dev47 — say at BOOT whether the model tier can serve. It is optional by
    # design (an image without scikit-learn falls back to the kNN ladder), and
    # scikit-learn ships no musllinux wheel, so whether it is present depends on
    # Alpine's py3-scikit-learn resolving at build time. Without this line the
    # only way to find out is to wait for the weekly retrain — which is exactly
    # the "is my deploy actually doing what I think" problem the version line
    # above was added to solve.
    try:
        from .tinymodel import sklearn_available
        if sklearn_available():
            import sklearn as _sk
            log.info("TinyModel tier available (scikit-learn %s) — it serves "
                     "once a circuit has enough labels", _sk.__version__)
        else:
            # Report WHY, not just that. "No module named 'sklearn'" means the
            # package never installed; anything else (typically a numpy ABI
            # complaint) means it installed but cannot load — two different
            # problems with two different fixes, and the build log cannot tell
            # them apart because Supervisor only dumps build output on failure.
            try:
                import sklearn  # noqa: F401
                why = "imported but reported unavailable"
            except Exception as _why:
                why = f"{type(_why).__name__}: {_why}"
            log.info("TinyModel tier NOT available — the kNN ladder serves, "
                     "exactly as before dev47, and everything else in dev47 is "
                     "unaffected. Reason: %s", why)
    except Exception as _tm_exc:            # never block boot on a report
        log.debug("could not report TinyModel availability: %s", _tm_exc)
    # RBAC trusts the ingress X-Remote-User-Id header ONLY because the ingress-IP
    # guard rejects non-Supervisor clients. If that guard is disabled in production
    # (INGRESS_ALLOWED_IP cleared while DEV_MODE is off), the header could be forged
    # to claim any role — loudly flag the misconfiguration.
    if not _DEV_MODE and not _INGRESS_IP:
        log.critical("SECURITY: ingress-IP guard is DISABLED (INGRESS_ALLOWED_IP "
                     "empty) but DEV_MODE is off — X-Remote-User-Id can be spoofed "
                     "to gain any role. Restore INGRESS_ALLOWED_IP or set DEV_MODE.")

    orch = Orchestrator(cfg)
    app.state.orchestrator = orch

    # Initialise DB and run migrations before the orchestrator's async run()
    # so the schema is fully up to date before any component starts.
    # Close the connection immediately after — orchestrator.run() opens its
    # own connection (self._db) which is the one used by all components.
    # Leaving this first connection open leaks a SQLite handle and holds
    # a shared lock that can interfere with WAL checkpointing.
    from .database import init_db
    from .config import DB_PATH
    _db = init_db(DB_PATH)
    try:
        run_migrations(_db, db_path=DB_PATH)
        # Cache the CSRF HMAC secret on app.state so the middleware
        # doesn't hit the DB on every request. The orchestrator's own
        # connection will be used for any post-startup reads.
        app.state.csrf_server_secret = get_or_create_csrf_server_secret(_db)
        # Pre-load RBAC role sets from the migration connection so the middleware
        # has them BEFORE serving (orchestrator.run() refreshes them again against
        # its own connection, then _run_role_sync keeps the admin set current).
        try:
            orch.load_roles_from_db(_db)
        except Exception as e:
            log.warning("RBAC role pre-load failed (non-fatal): %s", e)
        # dev41 (E1): install the DB-backed registration curve so every
        # stored estimate is stamped with the version that produced it. The
        # code-constant fallback is byte-identical to seeded v1, so a load
        # failure changes nothing.
        try:
            from .database import get_registration_curve
            from . import flow_integral
            _ver, _bands, _status = get_registration_curve(_db)
            if _bands:
                flow_integral.set_registration_curve(_bands, _ver, _status)
        except Exception as e:
            log.warning("Registration-curve load failed (non-fatal): %s", e)
    except Exception as e:
        log.critical("DB migration failed — cannot start: %s", e)
        raise
    finally:
        _db.close()

    # autoescape pinned explicitly. Starlette's Jinja2Templates default
    # has been select_autoescape(["html", "htm"]) for a long time, but
    # the safety belt is cheap and survives any future framework
    # default change. Every {{ … }} in our .html templates is escaped
    # by default; |safe is required to opt out.
    app.state.templates = IngressTemplates(
        directory=str(APP_DIR / "templates"),
        autoescape=select_autoescape(["html", "htm"]),
    )

    # Static-asset cache-buster.
    #
    # It used to be the addon version alone, and that is stable for a whole dev
    # cycle: every rebuild of 0.3.1-dev46 emitted the same `?v=0.3.1-dev46`, so
    # the browser kept serving a styles.css / app.js it had cached days and
    # many deploys earlier. A front-end fix could be deployed repeatedly and
    # never reach the page — with nothing in any log to say so.
    #
    # The build fingerprint changes whenever any module does, which is exactly
    # the condition under which cached assets must be discarded. (Same root
    # cause as the verdict stamp's code component — a version string is not a
    # build identity inside a dev cycle.)
    try:
        from .database import _code_fingerprint
        app.state.asset_version = (f"{_read_addon_version()}-"
                                   f"{_code_fingerprint()[:8]}")
    except Exception:                       # noqa: BLE001 — never block boot
        app.state.asset_version = _read_addon_version()

    # Register tojson filter (not included by default in FastAPI's Jinja2).
    #
    # Use Jinja's OWN htmlsafe_json_dumps rather than json.dumps wrapped in
    # Markup. json.dumps escapes " and \ but NOT <, >, & or U+2028/2029, and
    # the Markup wrapper then suppresses autoescape — so any value reaching a
    # <script> block through this filter could close the tag and execute.
    # dev49 (P0-5) found that live at three sinks: the cluster name in
    # fixtures_merge.html, ?range= via CHART_RANGE, and the X-Ingress-Path
    # header via window.INGRESS_PATH.
    #
    # htmlsafe_json_dumps escapes <, >, & and ' as \uXXXX and still returns
    # Markup, so the JSON stays parseable and cannot break out of the tag.
    # It applies the same policy as Jinja's built-in tojson.
    from jinja2.utils import htmlsafe_json_dumps as _htmlsafe_json_dumps
    app.state.templates.env.filters["tojson"] = _htmlsafe_json_dumps

    # fixture_icon: maps a cluster dict to an emoji for the fixture type.
    _FX_ICONS = {
        "toilet":          "\U0001F6BD",  # 🚽
        "shower_tub":      "\U0001F6BF",  # 🚿
        "tap":             "\U0001F6B0",  # 🚰
        "washing_machine": "\U0001F455",  # 👕
        "dishwasher":      "\U0001F37D",  # 🍽
        "irrigation_zone": "\U0001F4A7",  # 💧
        "other":           "❓",
        "leak_test":       "\U0001F50D",  # 🔍
    }
    app.state.templates.env.filters["fixture_icon"] = (
        lambda cl: _FX_ICONS.get(
            (cl.get("user_type") or cl.get("suggested_type") or "other"),
            "❓"
        )
    )

    runner = asyncio.create_task(orch.run())

    # Without this the add-on can be dead and green at the same time. The
    # lifespan frame keeps `runner` referenced, so asyncio never emits its
    # "exception was never retrieved" warning either — orch.run() could raise on
    # the first tick and the only symptom would be that nothing ever happens.
    def _runner_done(task: "asyncio.Task") -> None:
        if task.cancelled():
            return
        # A clean shutdown reaches here NOT cancelled, twice over: orch.stop()
        # runs before runner.cancel(), so the supervised workers can exit on
        # their own and gather() returns normally; and run()'s own
        # `except CancelledError: pass` absorbs the cancel when it does land.
        # Either way task.cancelled() is False and exception() is None, so
        # without this the "no monitoring is happening" CRITICAL fired on every
        # restart — which is exactly how an alarm gets ignored when it is real.
        if getattr(app.state, "shutting_down", False):
            log.info("Orchestrator stopped as part of shutdown")
            return
        exc = task.exception()
        if exc is None:
            app.state.orchestrator_state = "stopped"
            log.critical("Orchestrator run() RETURNED — no monitoring is "
                         "happening. The web UI will keep serving stale data.")
            return
        app.state.orchestrator_state = "crashed"
        app.state.orchestrator_error = "%s: %s" % (type(exc).__name__, exc)
        log.critical("Orchestrator run() CRASHED — no monitoring is happening",
                     exc_info=exc)

    app.state.orchestrator_state = "running"
    app.state.orchestrator_error = None
    app.state.shutting_down = False
    runner.add_done_callback(_runner_done)

    try:
        yield
    finally:
        log.info("Water Monitor shutting down")
        app.state.shutting_down = True      # before stop(): the callback reads it
        orch.stop()
        runner.cancel()
        try:
            await runner
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(
    lifespan=lifespan,
    title="Water Monitor",
    redirect_slashes=False,  # prevent /setup -> /setup/ redirects that break ingress
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

    path = request.url.path

    # Reject requests that did not arrive through the HA ingress proxy.
    # /health (exact) is exempt so Docker and HA health probes (which
    # come directly, not through ingress) continue to work. The previous
    # startswith check also exempted /health-anything — that's fixed
    # here by using an exact-match.
    # Disabled when DEV_MODE=true or INGRESS_ALLOWED_IP="" for local
    # dev/tests.
    if (not _DEV_MODE and _INGRESS_IP
            and not _is_health_path(path)):
        client_ip = request.client.host if request.client else ""
        if client_ip != _INGRESS_IP:
            log.warning("Rejected request from non-ingress IP %s on %s",
                        client_ip, path)
            from fastapi.responses import Response as _Resp
            return _Resp(status_code=403, content=b"Forbidden")

    # Log every POST so we can see what's reaching the addon
    if request.method == "POST":
        log.info("POST %s (ingress=%r)", path, ingress_path)

    # ----- Session + CSRF token derivation ---------------------------
    # Stateless HMAC double-submit (see plan A-1, revised by unit 2.28):
    #   - browser carries a random session_id in a cookie
    #   - server caches the persistent HMAC secret on app.state
    #   - csrf_token = "<nonce>.<HMAC(server_secret, session_id + '!' + nonce)>"
    #     with a fresh nonce per request (auth.issue_csrf_token)
    # No DB write per request; no shared process-wide cache.
    #
    # ACCEPTED RISK — a compromised sibling add-on. Every ingress add-on is
    # served from the Home Assistant ORIGIN, not just the same site. A page from
    # any other ingress add-on is therefore same-origin with this UI: it can
    # fetch() our pages with credentials, read the CSRF token straight out of the
    # returned HTML, and POST with it. No CSRF token, no SameSite value, no
    # Origin/Referer check and no cookie path can prevent that — it is a property
    # of the ingress architecture, and the only real mitigations live in Home
    # Assistant (per-add-on origins) or in not installing untrusted add-ons.
    # This is recorded deliberately and is NOT to be engineered around here; the
    # measures below defend against genuinely CROSS-site attackers, which is the
    # threat they can actually address.
    orch = getattr(request.app.state, "orchestrator", None)
    server_secret: str = getattr(
        request.app.state, "csrf_server_secret", ""
    )
    if not server_secret and orch and getattr(orch, "db", None):
        # dev46 (46a): once-per-process lazy init, but it still runs on the
        # event-loop thread on whichever request happens to be first — so it
        # goes over the wall like every other DB touch. The app-state cache
        # above means this costs one hop on that first request only.
        from .database import run_db
        server_secret = await run_db(get_or_create_csrf_server_secret, orch.db)
        request.app.state.csrf_server_secret = server_secret

    session_id = request.cookies.get(SESSION_COOKIE, "")
    new_session = False
    if not session_id or len(session_id) < SESSION_COOKIE_MIN_LEN:
        session_id = _new_session_id()
        new_session = True
    request.state.session_id = session_id
    # Fresh nonce per request: the token a page renders is no longer the same
    # string for the cookie's entire 30-day life.
    request.state.csrf_token = issue_csrf_token(server_secret, session_id)

    # ----- Role resolution + RBAC mutation gate ----------------------
    # Resolve the caller's role from the trusted ingress user header (see auth.py)
    # against the orchestrator's cached admin/operator sets. Membership test only —
    # no HA call or DB read on the hot path.
    admin_ids = getattr(orch, "admin_ids", frozenset())
    operator_ids = getattr(orch, "operator_ids", frozenset())
    role = role_for_request(request, admin_ids, operator_ids)
    request.state.role = role

    # First-sight seen-user log: once per user id per process (NOT per request), so
    # the Access page can list everyone who's opened the add-on even when the
    # config/auth/list lookup isn't available to the add-on token.
    uid = request.headers.get(REMOTE_USER_ID_HEADER, "").strip()
    if uid and orch is not None and getattr(orch, "db", None) is not None:
        seen = getattr(orch, "_seen_uids", None)
        if seen is not None and uid not in seen:
            seen.add(uid)
            # Write on a PRIVATE connection via the serialized writer — never on
            # the shared orch.db from the request path (that risks a transaction
            # clash with the orchestrator's inline writers). Fire-and-forget: the
            # serialized writer queues on the global admin write lock, which long
            # jobs (fit + reclassify) can hold for minutes — the page request must
            # NOT wait on it. On failure the uid is put back so a later request
            # retries. First-sight only → at most one task per user per process.
            _uname = request.headers.get(REMOTE_USER_NAME_HEADER, "")

            async def _log_seen_user(u: str, name: str) -> None:
                try:
                    from .config import DB_PATH
                    await run_isolated_write(
                        DB_PATH, lambda c: record_seen_user(c, u, name))
                except Exception:
                    seen.discard(u)   # retry on a later request

            asyncio.get_running_loop().create_task(_log_seen_user(uid, _uname))

    # Central mutation gate: reject any state-changing request the role isn't
    # allowed to make (viewer: none; operator: valve open/close only; admin: all).
    # Single chokepoint — no mutating route can be missed. Runs BEFORE CSRF so a
    # denied role gets a clean 403 without the form body-replay dance.
    if not (_is_health_path(path) or _is_static_path(path)) and \
            not is_mutation_allowed(role, request.method, path):
        log.warning("RBAC denied role=%s on %s %s", role, request.method, path)
        resp = JSONResponse(
            {"status": "error", "error": "forbidden",
             "message": "You don't have permission to do that."},
            status_code=403,
        )
        if new_session:
            _set_session_cookie(resp, request, session_id)
        return resp

    # ----- CSRF validation -------------------------------------------
    # Only state-changing methods need a token. Exempt:
    #   - exact /health  (probes — no token possible)
    #   - /static/*      (asset paths — POSTs are illegitimate)
    # Setup-wizard POSTs are NO LONGER exempt — the first GET sets the
    # session cookie and provides the token.
    if request.method in ("POST", "PUT", "PATCH", "DELETE") and not (
            _is_health_path(path) or _is_static_path(path)):
        # 0. Fetch Metadata (defence in depth, ~98% of browsers). The browser —
        #    not the page — states where the request came from, so it cannot be
        #    spoofed by script. "cross-site" can never describe a legitimate
        #    request to this add-on: the UI is served from the HA origin and only
        #    ever talks to itself. Absent header (old browser, curl, our own
        #    TestClient) falls through to the CSRF token check, which is still
        #    the primary control. Note this does NOT help against the sibling
        #    add-on above — that attacker is same-origin, so it sends
        #    Sec-Fetch-Site: same-origin.
        if request.headers.get("Sec-Fetch-Site", "") == "cross-site":
            log.warning("Fetch-Metadata rejected cross-site %s %s",
                        request.method, path)
            resp = JSONResponse(
                {"status": "error", "error": "cross_site",
                 "message": "Cross-site requests are not accepted."},
                status_code=403,
            )
            if new_session:
                _set_session_cookie(resp, request, session_id)
            return resp

        # 1. Header first (covers JSON, no-body, and all fetch POSTs)
        token = request.headers.get("X-CSRF-Token", "")
        # 2. Fall back to form/multipart body
        if not token:
            ct = request.headers.get("Content-Type", "")
            if ("application/x-www-form-urlencoded" in ct
                    or "multipart/form-data" in ct):
                # Drain the body once, replay it for downstream handlers
                body_bytes = await request.body()

                async def receive_replay():
                    return {
                        "type": "http.request",
                        "body": body_bytes,
                        "more_body": False,
                    }
                request._receive = receive_replay

                form_data = await request.form()
                token = form_data.get("_csrf", "")

                # Clear Starlette's form cache so the route handler
                # re-parses from the replayed body
                if hasattr(request, "_form"):
                    request._form = None

        if not check_csrf_token(server_secret, session_id, token):
            log.warning("CSRF invalid on %s %s", request.method, path)
            resp = HTMLResponse(
                "<h1>403 — Invalid or missing security token</h1>"
                "<p>Please reload the page and try again.</p>",
                status_code=403,
            )
            # A cookie-less client (new_session) must still leave with a session,
            # else it can never present a matching token on retry. The frontend
            # reloads on 403, which then derives a valid token for this session.
            if new_session:
                _set_session_cookie(resp, request, session_id)
            return resp

    # ----- Setup-complete redirect -----------------------------------
    # First-run users get bounced to the setup wizard until it's done.
    # The redirect-skip set still uses startswith for /setup so wizard
    # sub-paths don't bounce-redirect into themselves; /health is now
    # exact-match.
    skip_redirect = (
        path.startswith("/setup")
        or _is_static_path(path)
        or _is_health_path(path)
    )
    if not skip_redirect:
        if orch and not orch.setup_complete:
            # Sanitise the HA-supplied ingress path before embedding in HTML.
            # Allow only URL-safe path characters; strip anything else to
            # prevent header-injection attacks.
            ingress_path = _re.sub(r"[^/a-zA-Z0-9_\-]", "", ingress_path)
            setup_url = f"{ingress_path}/setup"
            return HTMLResponse(
                content=(
                    f"<!doctype html><html><head>"
                    f'<meta http-equiv="refresh" content="0; url={setup_url}">'
                    f"</head><body>"
                    f'<script>window.location.replace("{setup_url}");</script>'
                    f"Redirecting to setup…"
                    f"</body></html>"
                ),
                status_code=200,
            )

    response = await call_next(request)

    # Set the session cookie on the response if this was a new browser.
    # Attach it to every response shape (template, redirect, JSON, 4xx)
    # so the next request always carries it.
    if new_session:
        _set_session_cookie(response, request, session_id)
    return response


# Defense-in-depth response headers (plan C-IQ-18). Applied to every
# response on top of the per-route content. Behind HA ingress these
# mostly guard against accidental drift (a future template that loads
# a third-party script, a same-host XSS proxying through us); they
# also harden the addon if a user ever exposes it directly.
#
# CSP is deliberately permissive: the current UI has ~60 inline event
# handlers (onclick="…") and a few inline <script> / <style> blocks,
# plus Chart.js loaded from cdnjs. A strict CSP would require
# refactoring all of those to external files + per-request nonces.
# What we ship today catches the easy wins (frame-ancestors,
# object-src, base-uri) and pins the CDN origin so a compromised
# template can't pull script from anywhere new. Future tightening
# (drop 'unsafe-inline' for script-src, self-host Chart.js) is its
# own sprint.
_CHART_CDN = "https://cdnjs.cloudflare.com"
# styles.css line 4 does `@import url('https://fonts.googleapis.com/...')`, and
# the CSP did not allow it — so the webfonts were blocked on every page load
# since the policy shipped, silently falling back to system fonts and logging a
# console violation each time. Pin the two origins Google Fonts actually uses:
# the stylesheet comes from fonts.googleapis.com, the font files it references
# from fonts.gstatic.com, so allowing only the first still blocks the fonts.
_FONT_CSS_CDN = "https://fonts.googleapis.com"
_FONT_FILE_CDN = "https://fonts.gstatic.com"
_CSP_DIRECTIVES = (
    "default-src 'self'; "
    f"script-src 'self' 'unsafe-inline' {_CHART_CDN}; "
    f"style-src 'self' 'unsafe-inline' {_FONT_CSS_CDN}; "
    "img-src 'self' data:; "
    f"font-src 'self' data: {_FONT_FILE_CDN}; "
    "connect-src 'self'; "
    "frame-ancestors 'self'; "
    "form-action 'self'; "
    "base-uri 'self'; "
    "object-src 'none'"
)


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    """Set defense-in-depth response headers on every response.

    Skipping `/static/*` would also be defensible, but static assets
    serving CSS/JS benefit from `X-Content-Type-Options: nosniff` too,
    so headers are applied uniformly. `/health` keeps the headers
    since they don't break a JSON response.
    """
    response = await call_next(request)
    # X-Frame-Options: SAMEORIGIN keeps HA ingress (which iframes the
    # addon inside the HA dashboard) working while blocking cross-origin
    # embedding. `frame-ancestors 'self'` in the CSP gives the same
    # guarantee for modern browsers; XFO stays as belt-and-suspenders
    # for older clients.
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Content-Security-Policy", _CSP_DIRECTIVES)
    return response


app.include_router(setup.router)
app.include_router(dashboard.router)
app.include_router(device.router)
app.include_router(history.router)
app.include_router(fixtures.router)
app.include_router(settings.router)
app.include_router(backup.router)
app.include_router(help.router)
app.include_router(training.router)
app.include_router(calibration.router)
app.include_router(access.router)


@app.exception_handler(HTTPException)
async def _http_403_html(request: Request, exc: HTTPException):
    """Browser-friendly 403 for page GETs.

    require_admin/require_operator raise plain HTTPExceptions, whose default
    rendering is raw JSON ``{"detail": "..."}`` — what a viewer hits from a
    bookmark, a stale tab, or (pre-setup) the wizard redirect. Render the small
    403 page instead, but ONLY for GETs that want HTML: API calls and the
    middleware's mutation-gate JSON (which the frontend keys on) are untouched.
    """
    if (exc.status_code == 403 and request.method == "GET"
            and "text/html" in (request.headers.get("accept") or "")):
        orch = getattr(request.app.state, "orchestrator", None)
        return request.app.state.templates.TemplateResponse("403.html", {
            "request": request,
            "page": "403",
            "detail": exc.detail,
            "setup_pending": not bool(getattr(orch, "setup_complete", True)),
        }, status_code=403)
    return await fastapi_http_exception_handler(request, exc)


@app.get("/health")
async def health():
    """LIVENESS ONLY — deliberately shallow. Do not add dependency checks.

    This is what `watchdog:` in config.yaml polls. Making it assert that HA is
    reachable, or that a worker is alive, turns a soft dependency into a hard
    one: a brief HA outage would restart the whole add-on, and a restart LOSES
    the in-flight water event (the detector holds it in memory only). Depth
    belongs in /health/detail, which is a human surface, not a probe.
    """
    return {"status": "ok"}


@app.get("/health/detail")
async def health_detail(request: Request):
    """Per-subsystem state for humans. NOT a probe.

    Deliberately NOT exempted from the ingress-IP and RBAC guards: subsystem
    names, versions and error strings should not be readable by anything that
    can reach the port. `_is_health_path` matches "/health" exactly, so this
    path falls through to the normal guarded pipeline — which is why it cannot
    be used as a watchdog target.

    Reads only in-memory state written by Orchestrator._supervise, so it does
    no I/O and cannot itself be the thing that hangs.
    """
    orch = getattr(request.app.state, "orchestrator", None)
    workers = dict(getattr(orch, "worker_health", {}) or {}) if orch else {}
    unhealthy = sorted(n for n, h in workers.items()
                       if h.get("state") in ("crashed", "stopped"))
    run_state = getattr(request.app.state, "orchestrator_state", "unknown")
    return {
        # draft-inadarei-api-health-check shape: pass / warn / fail
        "status": ("fail" if run_state != "running"
                   else "warn" if unhealthy else "pass"),
        "orchestrator": {
            "state": run_state,
            "error": getattr(request.app.state, "orchestrator_error", None),
        },
        "workers": workers,
        "unhealthy": unhealthy,
    }
