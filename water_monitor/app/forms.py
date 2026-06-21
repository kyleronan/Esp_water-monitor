"""Pure-stdlib input-validation helpers used by route handlers.

Kept FastAPI-free so they can be unit-tested without pulling in the
whole web stack — see tests/test_coerce_int.py.
"""
from __future__ import annotations

from typing import Any, Optional


def coerce_int(
    value: Any,
    lo: Optional[int] = None,
    hi: Optional[int] = None,
    default: int = 0,
) -> int:
    """Parse a form value into an int bounded to ``[lo, hi]``.

    Returns ``default`` if the value is missing, empty, non-numeric, or
    falls outside the bounds. The previous pattern,
    ``int(form.get(key, default) or default)``, silently accepted
    out-of-range values (negative bathrooms, run_hour=99, etc.) which
    then leaked into the DB. This helper centralises the parse so
    out-of-range inputs round-trip to a sane default rather than
    poisoning storage.

    ``lo`` / ``hi`` may be omitted for an unbounded check. Pass them
    whenever the column has a semantic range (e.g. run_hour in [0, 23],
    day_of_week in [0, 6], bathrooms in [0, 20]).
    """
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return default
        parsed = int(value)
    except (ValueError, TypeError):
        return default
    if lo is not None and parsed < lo:
        return default
    if hi is not None and parsed > hi:
        return default
    return parsed
