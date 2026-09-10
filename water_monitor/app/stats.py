"""Small numeric helpers shared across the detector, health and regime code.

Deliberately a leaf: it imports nothing from the package, so the bottom of the
detector stack can use it without opening an import cycle.
"""
from __future__ import annotations

from typing import Sequence


def median(values: Sequence[float]) -> float:
    """Median with the STANDARD even-n convention: the mean of the two middle
    elements, not the upper one.

    ``s[len(s) // 2]`` — the upper middle on an even-length list — biases a MAD,
    and therefore any noise floor derived from it, HIGH. That is why this lives
    in one place: four copies of this function existed and one of them had the
    convention wrong.

    Returns 0.0 for empty input. Callers that need to tell "no data" apart from
    "a median of zero" must check first — see ``leak_test_scheduler._median``,
    which returns None on purpose and is deliberately NOT this function.
    """
    s = sorted(values)
    n = len(s)
    if not n:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0
