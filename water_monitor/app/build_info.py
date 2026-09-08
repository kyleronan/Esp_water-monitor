"""Build identity — the add-on version and git commit, read once from disk.

Both readers used to exist TWICE: ``event_detector._read_addon_version``
(returning ``Optional[str]``) and ``main._read_addon_version`` (returning
``str`` with a ``'dev'`` fallback), and ``main.py`` called BOTH — the
event_detector copy for the boot banner and its own copy for the static-asset
cache-buster. Two parsers of one file is one parser too many: a fix to the
version-line handling could land in either copy and silently not reach the
other.

This module is deliberately dependency-free (``pathlib`` only). It is imported
on the boot path by ``main``, by ``database._code_fingerprint`` (through
``event_detector``'s re-export), and by ``routers/backup``, so it must never
drag app logic in behind it.

Both functions are best-effort by contract: they return ``None`` rather than
raise, because every caller is either a log line or a cache key and none of
them may block boot. Callers that need a display string supply their own
fallback (``or "dev"`` / ``or "unknown"``) — the sentinel stays out of here so
that "config.yaml was unreadable" is distinguishable from "the version really
is the string 'dev'".
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional


def _read_addon_version() -> Optional[str]:
    """Best-effort add-on version from config.yaml — None if unavailable.

    One line, no YAML dependency: the add-on's ``version:`` is a top-level
    key, so an unindented prefix match is exact enough and cannot be fooled by
    a nested ``version:`` under some other block.
    """
    try:
        cfg = Path(__file__).resolve().parents[1] / "config.yaml"
        for line in cfg.read_text(encoding="utf-8").splitlines():
            if line.startswith("version:"):
                return line.split(":", 1)[1].strip().strip('"').strip("'")
    except Exception:                       # noqa: BLE001 — never block boot
        pass
    return None


def _read_git_commit() -> Optional[str]:
    """Best-effort git short commit — None when not in a git checkout.

    The shipped container has no ``.git``, so None is the NORMAL production
    answer, not an error case.
    """
    try:
        git_dir = Path(__file__).resolve().parents[2] / ".git"
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref:"):
            ref = head.split(":", 1)[1].strip()
            return (git_dir / ref).read_text(encoding="utf-8").strip()[:12]
        return head[:12]
    except Exception:                       # noqa: BLE001
        return None
