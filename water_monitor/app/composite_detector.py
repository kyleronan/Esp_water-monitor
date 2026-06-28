"""Embedded-fixture (composite) detection from a flow waveform.

The production type classifier reads scalar summaries, so a second fixture that
runs *during* a sustained event — a toilet flushed mid-shower — is folded into
the parent's single label (or left unlabelled). The audit found 22 such toilet
flushes buried inside long showers across the record, each absorbed by the
add-on into one ``shower_tub`` (or ``(none)``).

This module recovers them. It takes the high-resolution flow waveform the add-on
already stores (``event_waveforms.flow_max`` — the per-bin peak envelope, up to
1000 bins), estimates the sustained baseline with a rolling low percentile, and
integrates the excursions *above* that baseline. Each excursion is the volume of
a draw superimposed on the shower; by its size + peak it is classed toilet-sized
vs tap-sized.

PURE + annotate-only: this never changes any event's volume or its primary label.
Its output is stored as metadata (``events.embedded_fixtures_json``) and surfaced
in the History modal ("contains: toilet ×2 (~9 L)"). The parent keeps its volume
intact; when the parent matches no single fixture it may be labelled ``other``.

Resolution honesty: the waveform's resolution varies (the ESP doesn't always
deliver a hi-res capture — some events store a single bin). ``detect_from_envelope``
ABSTAINS (returns ``None``) when the waveform is too coarse to resolve a ~45 s
draw, so a low-res event is never given a spurious composite annotation.
"""
from __future__ import annotations

import bisect
from typing import List, Optional, Sequence, Tuple

# ── Tunables (validated against the raw-flow prototype the user reviewed) ─────
# A toilet fill is ~4–6 L over ~45 s peaking ~6–12 L/min above a ~5 L/min shower.
_BASELINE_WINDOW_S: float = 75.0      # rolling window for the sustained level
_BASELINE_PCTL: float = 0.35          # low percentile → brief spikes don't lift it
_EXCURSION_START_ABS: float = 2.0     # L/min above baseline to OPEN an excursion
_EXCURSION_START_REL: float = 0.35    # …or this fraction of the baseline
_EXCURSION_END_ABS: float = 1.2       # hysteresis: stay open until below this
_EXCURSION_END_REL: float = 0.25
_MIN_EXCURSION_S: float = 6.0         # shorter = sensor noise, not a fixture
_MAX_EXCURSION_S: float = 150.0       # longer = the baseline itself drifting
_MIN_EXCESS_L: float = 0.5            # ignore sub-litre wiggles (leak-safe: tiny)
# Embedded-fixture classes by excess volume + peak-above-baseline.
_TOILET_MIN_L, _TOILET_MAX_L = 3.0, 8.0
_TOILET_MIN_PEAK_LPM = 3.0
# Resolution gate for an envelope: need enough bins, fine enough, to see a draw.
_MIN_BINS: int = 30
_MAX_SECONDS_PER_BIN: float = 15.0


def _rolling_baseline(ts: Sequence[float], fl: Sequence[float],
                      window_s: float = _BASELINE_WINDOW_S,
                      pctl: float = _BASELINE_PCTL) -> List[float]:
    """Per-sample low-percentile of flow over a centred time window — the
    sustained (shower) level, robust to brief superimposed spikes."""
    out: List[float] = []
    for i, t in enumerate(ts):
        lo = bisect.bisect_left(ts, t - window_s / 2.0)
        hi = bisect.bisect_right(ts, t + window_s / 2.0)
        w = sorted(fl[lo:hi])
        out.append(w[max(0, int(pctl * (len(w) - 1)))] if w else 0.0)
    return out


def detect_embedded_fixtures(
    series: Sequence[Tuple[float, float]],
) -> List[dict]:
    """Find draws superimposed on a sustained baseline in a (t_seconds, lpm) series.

    Returns a list of embedded fixtures, each::

        {"kind": "toilet"|"tap", "offset_s": int, "duration_s": int,
         "excess_litres": float, "peak_excess_lpm": float}

    ``excess_litres`` is the integral of (flow − baseline) over the excursion —
    the extra water the embedded draw added on top of the ongoing event. Empty
    list when nothing rises convincingly above the baseline.
    """
    pts = [(float(t), float(f)) for t, f in series
           if t is not None and f is not None]
    if len(pts) < 10:
        return []
    pts.sort(key=lambda p: p[0])
    ts = [p[0] for p in pts]
    fl = [p[1] for p in pts]
    base = _rolling_baseline(ts, fl)

    found: List[dict] = []
    i, n = 0, len(pts)
    while i < n:
        bl = base[i]
        if fl[i] - bl >= max(_EXCURSION_START_ABS, _EXCURSION_START_REL * bl):
            j = i
            while j < n and (fl[j] - base[j] >=
                             max(_EXCURSION_END_ABS, _EXCURSION_END_REL * base[j])):
                j += 1
            dur = ts[j - 1] - ts[i]
            if _MIN_EXCURSION_S <= dur <= _MAX_EXCURSION_S:
                excess = 0.0
                for k in range(i, j - 1):
                    excess += max(0.0, fl[k] - base[k]) * (ts[k + 1] - ts[k]) / 60.0
                peak = max(fl[k] - base[k] for k in range(i, j))
                if excess >= _MIN_EXCESS_L:
                    found.append({
                        "kind": _classify_excursion(excess, peak),
                        "offset_s": int(round(ts[i])),
                        "duration_s": int(round(dur)),
                        "excess_litres": round(excess, 2),
                        "peak_excess_lpm": round(peak, 1),
                    })
            i = j
        else:
            i += 1
    return found


def _classify_excursion(excess_l: float, peak_lpm: float) -> str:
    if _TOILET_MIN_L <= excess_l <= _TOILET_MAX_L and peak_lpm >= _TOILET_MIN_PEAK_LPM:
        return "toilet"
    return "tap"


def _envelope_to_series(flow_max: Sequence[float],
                        duration_seconds: float) -> List[Tuple[float, float]]:
    """Reconstruct an evenly-spaced (t_seconds, lpm) series from a binned
    ``flow_max`` envelope spanning ``duration_seconds``."""
    n = len(flow_max)
    if n < 2 or duration_seconds <= 0:
        return []
    step = duration_seconds / (n - 1)
    return [(i * step, float(v)) for i, v in enumerate(flow_max)]


def detect_from_envelope(
    flow_max: Optional[Sequence[float]],
    duration_seconds: Optional[float],
) -> Optional[List[dict]]:
    """Run embedded detection on a stored ``event_waveforms.flow_max`` envelope.

    Returns the embedded-fixture list, or ``None`` when the waveform is too
    coarse to resolve a draw (too few bins, or each bin spans too much time) —
    so a low-resolution event is never given a spurious composite annotation.
    """
    if not flow_max or not duration_seconds or duration_seconds <= 0:
        return None
    n = len(flow_max)
    if n < _MIN_BINS or (duration_seconds / n) > _MAX_SECONDS_PER_BIN:
        return None
    return detect_embedded_fixtures(_envelope_to_series(flow_max, duration_seconds))


def summarize_embedded(embedded: Sequence[dict]) -> dict:
    """Roll an embedded-fixture list into a compact summary for display/scoring::

        {"toilet": 2, "tap": 1, "total": 3, "embedded_litres": 9.4,
         "label": "toilet ×2, tap ×1 (~9 L)"}

    Empty input → ``{"total": 0, ...}`` with an empty label.
    """
    counts: dict = {}
    total_l = 0.0
    for e in embedded:
        counts[e["kind"]] = counts.get(e["kind"], 0) + 1
        total_l += float(e.get("excess_litres") or 0.0)
    total = sum(counts.values())
    parts = []
    for kind in ("toilet", "tap"):
        if counts.get(kind):
            parts.append(f"{kind} ×{counts[kind]}")
    for kind, c in counts.items():           # any other kinds, stable after the known two
        if kind not in ("toilet", "tap"):
            parts.append(f"{kind} ×{c}")
    label = ""
    if parts:
        label = ", ".join(parts) + f" (~{round(total_l):g} L)"
    return {**counts, "total": total, "embedded_litres": round(total_l, 2),
            "label": label}
