"""Pure-stdlib input-validation helpers used by route handlers.

Kept FastAPI-free so they can be unit-tested without pulling in the
whole web stack — see tests/test_coerce_int.py.
"""
from __future__ import annotations

from typing import Any, Optional, TypeVar, Union

# The fallback's type flows straight through to the return type, so a caller
# that passes `default=None` is typed `float | None` while every caller that
# passes a real number is still typed `float`. See coerce_float.
_D = TypeVar("_D")


def coerce_int(
    value: Any,
    lo: Optional[int] = None,
    hi: Optional[int] = None,
    default: int = 0,
) -> int:
    """Parse a form value into an int bounded to ``[lo, hi]``.

    Returns ``default`` if the value is missing, empty, non-numeric, or
    falls outside the bounds. A bare ``int(form.get(key, default) or
    default)`` silently accepts out-of-range values (negative bathrooms,
    run_hour=99) which then leak into the DB; this centralises the parse
    so they round-trip to a sane default instead.

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


def coerce_float(
    value: Any,
    lo: Optional[float] = None,
    hi: Optional[float] = None,
    default: _D = 0.0,          # type: ignore[assignment]
) -> Union[float, _D]:
    """Parse a form value into a float bounded to ``[lo, hi]``.

    The float twin of :func:`coerce_int`, and it exists for the same reason:
    bare ``float(form.get(key, preset))`` raises on any non-numeric POST (a
    500, not a 4xx) and silently accepts anything that parses, however absurd.

    That matters most on the sensitivity form, whose values authorise an
    automatic valve close. Those inputs declare ``min``/``max`` in the HTML,
    which is client-side only; passing the same bounds here makes the form's
    declared range real.

    NaN and infinity are rejected: they compare False against every bound, so
    an unguarded range check would let them through, and a NaN threshold makes
    every later comparison against it false in ways that differ by how the
    comparison happens to be written.

    ``default`` is returned as given, so ``default=None`` makes this an
    "optional float" parse: ``None`` back means *this field was not usable*,
    which is distinct from any value the field could legitimately hold. The
    history filter-bar callers rely on that — they must tell "no filter" apart
    from "a filter of zero", and a fallback number there would silently invent
    a filter the user never asked for.
    """
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return default
        parsed = float(value)
    except (ValueError, TypeError):
        return default
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return default
    if lo is not None and parsed < lo:
        return default
    if hi is not None and parsed > hi:
        return default
    return parsed
