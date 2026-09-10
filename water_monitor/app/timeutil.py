"""Timestamp parsing shared across modules that read stored ISO strings.

Deliberately a leaf: imports nothing from the package.

Everything in the database is STORED in UTC as an ISO string, sometimes with a
``Z`` suffix and sometimes with an offset, and a few paths hand back a real
``datetime`` instead. ``to_utc`` is the one place that reconciles those.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def to_utc(ts: Any) -> Optional[datetime]:
    """ISO string or datetime → aware UTC datetime, or None if unparseable.

    A NAIVE input is assumed to already be UTC, which is what the stored format
    means. Callers that need to tell "naive" from "aware" apart must check
    before calling — this deliberately erases the difference.
    """
    if isinstance(ts, datetime):
        return (ts.astimezone(timezone.utc) if ts.tzinfo
                else ts.replace(tzinfo=timezone.utc))
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return (d.astimezone(timezone.utc) if d.tzinfo
            else d.replace(tzinfo=timezone.utc))
