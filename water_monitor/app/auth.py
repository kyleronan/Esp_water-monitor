"""Role-based access control: viewer / operator / admin.

Roles are derived from the Home Assistant user identity that the Supervisor
ingress proxy injects as the ``X-Remote-User-Id`` request header. That header is
trustworthy *here* only because ``ingress_middleware`` rejects any request whose
client IP is not the Supervisor ingress proxy (``main._INGRESS_IP``) — the add-on
is not directly reachable (``ports: 8765/tcp: null``), so a browser cannot forge
the header.

Tiers
-----
  admin    — user id is in the HA admin set (auto, from ``config/auth/list``,
             cached) or the configured ``bootstrap_admin_user_id``.
  operator — user id is on the add-on operator allow-list (Settings → Access).
  viewer   — everyone else. **Default-deny**: an unknown / missing id is a viewer.

Capabilities: admin = everything; operator = viewer + open / close the main
valve; viewer = read-only.

Enforcement is centralised in two places (defence in depth):
  1. ``ingress_middleware`` mutation gate — the single chokepoint that rejects any
     state-changing request a role isn't allowed to make. No mutating route can be
     missed because it keys on the HTTP method, not a per-route opt-in.
  2. ``require_admin`` / ``require_operator`` dependencies — attached to sensitive
     *GET* routers (settings/backup/setup/…), which the method-based gate can't catch.

This module is import-light (no DB / FastAPI app imports beyond the request type)
so it stays unit-testable offline. The CSRF helpers at the bottom pull two pure
HMAC functions out of ``database`` lazily, inside the call, so importing this
module still costs nothing.
"""
from __future__ import annotations

import os
import re
from typing import Iterable
from .database import derive_csrf_token, validate_csrf_token

try:
    # Runtime import (NOT TYPE_CHECKING-only): FastAPI resolves the
    # `request: Request` annotation on require_admin/require_operator via
    # get_type_hints at request time. With `from __future__ import annotations`
    # the hint is a string, so `Request` MUST exist in this module's runtime
    # globals — otherwise FastAPI can't recognise it as the request object and
    # mis-reads `request` as a required query param (HTTP 422 "Field required").
    # The except keeps the module importable (and the pure role functions
    # unit-testable) in environments without FastAPI, where the deps are unused.
    from fastapi import Request
except ImportError:  # pragma: no cover
    Request = None  # type: ignore

# Role names (also used as the WM_DEV_ROLE values and template strings).
ADMIN = "admin"
OPERATOR = "operator"
VIEWER = "viewer"

_ROLES = (ADMIN, OPERATOR, VIEWER)

# Headers the HA Supervisor ingress proxy injects for the authenticated user.
REMOTE_USER_ID_HEADER = "X-Remote-User-Id"
REMOTE_USER_NAME_HEADER = "X-Remote-User-Display-Name"

# The ONLY state-changing requests an operator may make: open / close a valve.
# Path is the app-internal path (ingress prefix already stripped by the proxy),
# e.g. ``/device/valve/circuit_1/open``.
_OPERATOR_WRITE_RE = re.compile(r"^/device/valve/[^/]+/(open|close)/?$")

_MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _dev_mode() -> bool:
    return os.environ.get("DEV_MODE", "false").lower() in ("true", "1", "yes")


def resolve_role(user_id: str,
                 admin_ids: Iterable[str],
                 operator_ids: Iterable[str]) -> str:
    """Pure role decision for a user id. Default-deny.

    ``admin_ids`` / ``operator_ids`` are membership-testable containers (sets).
    An empty / falsy ``user_id`` (no ingress header) is always a viewer.
    """
    if user_id:
        if user_id in admin_ids:
            return ADMIN
        if user_id in operator_ids:
            return OPERATOR
    return VIEWER


def role_for_request(request: Request,
                     admin_ids: Iterable[str],
                     operator_ids: Iterable[str]) -> str:
    """Resolve the role for an incoming request.

    Honours the ``WM_DEV_ROLE`` escape hatch ONLY when ``DEV_MODE`` is on (local
    / pytest runs that have no ingress header), so a developer isn't locked out
    and tests can force a role. In production both are unset and the role comes
    from the trusted ``X-Remote-User-Id`` header.
    """
    if _dev_mode():
        dev = os.environ.get("WM_DEV_ROLE", "").strip().lower()
        if dev in _ROLES:
            return dev
    uid = request.headers.get(REMOTE_USER_ID_HEADER, "").strip()
    return resolve_role(uid, admin_ids, operator_ids)


def is_mutation_allowed(role: str, method: str, path: str) -> bool:
    """Whether ``role`` may perform ``method`` on ``path``.

    Non-mutating methods are always allowed (read access is gated separately by
    ``require_admin`` on sensitive GET routes). Admin may do anything; operator
    may only open/close a valve; viewer may not mutate at all.
    """
    if method not in _MUTATING_METHODS:
        return True
    if role == ADMIN:
        return True
    # Operator's ONE capability is fully pinned: POST to a valve open/close path.
    # Pinning the method (not just the path) future-proofs against a PUT/PATCH/
    # DELETE handler ever being mounted under the same path.
    if role == OPERATOR and method == "POST" and _OPERATOR_WRITE_RE.match(path):
        return True
    return False


def admin_ids_from_users(users: Iterable[dict]) -> set:
    """Extract the set of admin HA user ids from a ``config/auth/list`` result
    (already normalised by ``HaClient.get_users`` to carry ``id`` + ``is_admin``)."""
    out: set = set()
    for u in users or []:
        if u.get("is_admin") and u.get("id"):
            out.add(u["id"])
    return out


# ── FastAPI dependencies (route-level guards for sensitive GET pages) ──────────
# HTTPException is imported lazily so this module stays importable (and the pure
# role functions stay unit-testable) in environments without FastAPI installed.

def require_admin(request: Request) -> None:
    """Dependency: 403 unless the request's resolved role is admin."""
    if getattr(request.state, "role", VIEWER) != ADMIN:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Admin access required.")


def require_admin_or_bootstrap(request: Request) -> None:
    """Dependency for the setup wizard: 403 unless admin — EXCEPT in the
    fresh-install bootstrap state, where nobody CAN be admin yet.

    Bootstrap state = setup is not complete AND the add-on knows of no admin at
    all (empty ``orch.admin_ids``: fresh ``admin_ids_cache``, first
    ``config/auth/list`` fetch not yet landed — or never landing because the
    token can't read the user list — and no ``bootstrap_admin_user_id``).
    Without this carve-out the first-run wizard 403s for everyone, including
    the genuine HA admin, and the add-on is dead on arrival. The window closes
    the moment any admin becomes known or setup completes; every later visit is
    admin-gated as normal."""
    if getattr(request.state, "role", VIEWER) == ADMIN:
        return
    orch = getattr(request.app.state, "orchestrator", None)
    if (orch is not None
            and not getattr(orch, "setup_complete", True)
            and not getattr(orch, "admin_ids", None)):
        return
    from fastapi import HTTPException
    raise HTTPException(status_code=403, detail="Admin access required.")


def require_operator(request: Request) -> None:
    """Dependency: 403 unless the request's role can control the valve
    (operator or admin)."""
    if getattr(request.state, "role", VIEWER) not in (ADMIN, OPERATOR):
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Operator access required.")


# ── CSRF token: OWASP "HMAC CSRF Token" recipe ────────────────────────────────
#
# Signed double-submit: the OWASP-recommended pattern (only the *naive* unsigned
# variant is deprecated), and it needs no server-side state.
#
# message = session_id + "!" + nonce, transmitted as "<nonce>.<hmac>". The nonce
# travels inside the token, so the server verifies it with nothing but the secret
# and the cookie — the double-submit property (no server state) is preserved. A
# fresh nonce is minted per request, so no two renders emit the same token.
#
# NOT included: OWASP's optional timestamp component. It would bound an
# individual token's lifetime, but at the price of a hard expiry on any page
# left open (a wall-mounted HA dashboard is a normal deployment here) for a
# threat — a token leaked out of the page's own HTML — that the same-origin
# sibling-add-on exposure documented in main.py makes moot anyway. The lever for
# bounding token lifetime is ``main.SESSION_COOKIE_MAX_AGE``, which bounds the
# session the token is tied to.
CSRF_NONCE_BYTES = 16
# The message separator is a character the nonce can never contain (the nonce is
# hex, enforced by _CSRF_NONCE_RE below), so the message determines exactly one
# (session_id, nonce) pair: a token for session "ab" + nonce "cd" can never also
# be a valid token for session "abcd" + nonce "". Without a separator that
# ambiguity would be real, since the cookie value is attacker-chosen.
_CSRF_MSG_SEP = "!"
_CSRF_TOKEN_SEP = "."
# 8..64 hex chars: accepts our own 32-char nonce, rejects anything oversized
# before it reaches the HMAC.
_CSRF_NONCE_RE = re.compile(r"^[0-9a-f]{8,64}$")


def _csrf_message(session_id: str, nonce: str) -> str:
    return f"{session_id}{_CSRF_MSG_SEP}{nonce}"


def issue_csrf_token(server_secret: str, session_id: str,
                     nonce: str = "") -> str:
    """Mint a CSRF token for ``session_id``.

    Returns ``"<nonce>.<hmac>"``, or "" when there is no secret/session yet
    (the pre-boot window — templates render an empty token and the middleware
    rejects every mutation, which is fail-closed).

    ``nonce`` is for tests only; production always mints a fresh one.
    """
    if not server_secret or not session_id:
        return ""
    if not nonce:
        import secrets as _secrets
        nonce = _secrets.token_hex(CSRF_NONCE_BYTES)
    # Reuse database.derive_csrf_token rather than re-implementing the HMAC, so
    # the two can never drift. Imported lazily to keep this module import-light.
    mac = derive_csrf_token(server_secret, _csrf_message(session_id, nonce))
    return f"{nonce}{_CSRF_TOKEN_SEP}{mac}"


def check_csrf_token(server_secret: str, session_id: str, token: str) -> bool:
    """Verify a token minted by :func:`issue_csrf_token`. Fails closed.

    A bare HMAC with no nonce is NOT accepted: no separator, so it falls out at
    the split. A browser holding one gets a single 403, and both the 403 page
    and ``app.js``'s fetch wrapper tell it to reload.
    """
    if not server_secret or not session_id or not token:
        return False
    nonce, sep, mac = token.partition(_CSRF_TOKEN_SEP)
    if not sep or not _CSRF_NONCE_RE.match(nonce) or not mac:
        return False
    return validate_csrf_token(
        server_secret, _csrf_message(session_id, nonce), mac)
