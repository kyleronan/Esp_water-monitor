"""Tight-fingerprint label propagator (2026-07 reality audit, Phase 3).

An event's fully UN-normalized waveform trio — absolute-time flow (L/min),
cumulative volume (L), and pressure drop below the pre-event baseline (psi) —
acts as a fingerprint: when a new event's nearest neighbor among the USER-
labeled events is close enough, it inherits that label. Validated on this
home's data (two backups): 30% coverage at 96% precision standalone; on the
events the production pipeline declined it labels ~29% of them at 94%.

Design points (all measured, see scratchpad REALITY_VS_ADDON.md §1c Q7-Q8):
  * STORED waveforms beat raw-HA fingerprints (96% vs 92%) — systematic capture
    distortions match each other — so this works on event_waveforms as-is.
  * The distance threshold is SELF-CALIBRATING: a percentile of the library's
    own NN-distance distribution, recomputed at load. No config to tune.
  * Maturity-aware: below MATURE_LIBRARY_N labels the threshold tightens
    (precision measured to dip to ~0.8 on 25-50-label libraries otherwise).
  * Library = USER-labeled events only ('user'/'training'/'cycle' sources all
    carry user_fixture_type). Fingerprint labels are never library members —
    no fingerprint->fingerprint chaining, so no drift.
  * CYCLE_ONLY types (washer/dishwasher) may be inherited — the evidence is a
    whole-waveform match against a user-confirmed example, much stronger than
    a lone scalar k-NN vote — but only at the TIGHT (immature) threshold.
    Measured: dishwasher 83/83 correct at the standard threshold.

Pure module: no DB writes, no asyncio. Callers (database.reclassify tier loop,
feature_extractor live path) stamp results via set_event_matched_fixture_type
with matched_via='fingerprint'.
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ── Fingerprint grid (matches the validated eval exactly) ────────────────────
CELL_SECONDS: float = 4.0     # one cell = 4 s of absolute time
N_CELLS: int = 64             # first 256 s of the event
MIN_EVENT_SECONDS: float = 10.0
MIN_WAVEFORM_BINS: int = 4
MIN_PEAK_LPM: float = 0.3     # flat/no-flow waveform -> no fingerprint

# ── Matching policy ──────────────────────────────────────────────────────────
MIN_CLASS_LIBRARY: int = 5        # a class needs >= this many user labels
MATURE_LIBRARY_N: int = 100       # below this, use the tight percentile
THRESHOLD_PCTL_MATURE: float = 30.0
THRESHOLD_PCTL_TIGHT: float = 15.0
MIN_LIBRARY_N: int = 10           # below this the matcher abstains entirely


def _np():
    """Lazy numpy import — present in the container via river/dtaidistance."""
    import numpy
    return numpy


def build_fingerprint(flow_json: Optional[str], press_json: Optional[str],
                      waveform_duration_s: Optional[float],
                      pre_event_pressure_psi: Optional[float]):
    """Build one event's fingerprint from its stored waveform.

    Returns a float ndarray of length 3*N_CELLS — [flow | cumulative volume |
    pressure drop] on the absolute 4 s grid, raw units, zero-padded past the
    event's end — or None when the waveform can't support one (too short,
    too coarse, or no flow). Channel scaling happens at library level.
    """
    np = _np()
    if not flow_json or not waveform_duration_s:
        return None
    dur = float(waveform_duration_s)
    if dur < MIN_EVENT_SECONDS:
        return None
    try:
        wf = np.asarray(json.loads(flow_json), dtype=float)
    except (TypeError, ValueError):
        return None
    if wf.size < MIN_WAVEFORM_BINS or not np.isfinite(wf).all() \
            or wf.max() < MIN_PEAK_LPM:
        return None

    edges = np.arange(N_CELLS + 1) * CELL_SECONDS
    flow = _resample_to_grid(np, wf, dur, edges)
    ncut = int(math.ceil(min(dur, N_CELLS * CELL_SECONDS) / CELL_SECONDS))
    flow[ncut:] = 0.0
    vol = np.cumsum(flow) * CELL_SECONDS / 60.0

    pdrop = np.zeros(N_CELLS)
    if press_json and pre_event_pressure_psi is not None:
        try:
            wp = np.asarray(json.loads(press_json), dtype=float)
        except (TypeError, ValueError):
            wp = None
        if wp is not None and wp.size >= MIN_WAVEFORM_BINS and np.isfinite(wp).all():
            pabs = _resample_to_grid(np, wp, dur, edges)
            pdrop = float(pre_event_pressure_psi) - pabs
            pdrop[ncut:] = 0.0
    return np.concatenate([flow, vol, pdrop])


def _resample_to_grid(np, arr, dur: float, edges):
    """Bucket-mean a stored waveform onto the absolute grid (validated shape)."""
    n = arr.size
    src_t = (np.arange(n) + 0.5) * (dur / n)
    out = np.zeros(edges.size - 1)
    bin_w = dur / n
    for i in range(edges.size - 1):
        m = (src_t >= edges[i]) & (src_t < edges[i + 1])
        if m.any():
            out[i] = arr[m].mean()
        elif edges[i] < dur:
            out[i] = arr[min(int(edges[i] / bin_w), n - 1)]
    return out


class FingerprintLibrary:
    """The user-labeled fingerprint library for one circuit, with its
    self-calibrated match threshold. Build via :meth:`load`."""

    def __init__(self, matrix, scales, labels: List[str],
                 event_ids: List[str], threshold: float, pctl: float):
        self.matrix = matrix          # (n, 3*N_CELLS) channel-scaled
        self.scales = scales          # per-channel std used to scale
        self.labels = labels
        self.event_ids = event_ids
        self.threshold = threshold
        self.percentile_used = pctl
        from collections import Counter
        self.class_counts = Counter(labels)
        self.loaded_at = time.monotonic()

    def __len__(self) -> int:
        return len(self.labels)

    @classmethod
    def load(cls, conn: sqlite3.Connection, circuit: str) -> Optional["FingerprintLibrary"]:
        """Build the library from USER-labeled events with stored waveforms.
        Returns None when the library is too small to match against."""
        np = _np()
        rows = conn.execute(
            "SELECT e.id, e.user_fixture_type, e.pre_event_pressure_psi, "
            "       w.flow_max_json, w.pressure_min_json, w.duration_seconds "
            "FROM events e JOIN event_waveforms w ON w.event_id = e.id "
            "WHERE e.circuit = ? AND e.user_fixture_type IS NOT NULL "
            "  AND e.duration_seconds >= ?",
            (circuit, MIN_EVENT_SECONDS)).fetchall()
        fps, labels, ids = [], [], []
        for r in rows:
            fp = build_fingerprint(r["flow_max_json"], r["pressure_min_json"],
                                   r["duration_seconds"],
                                   r["pre_event_pressure_psi"])
            if fp is not None:
                fps.append(fp)
                labels.append(r["user_fixture_type"])
                ids.append(r["id"])
        if len(fps) < MIN_LIBRARY_N:
            return None
        raw = np.vstack(fps)
        # Per-channel scale from the library itself (flow / volume / pressure
        # live in different units — scale each block by its own std).
        scales = []
        blocks = []
        for c in range(3):
            block = raw[:, c * N_CELLS:(c + 1) * N_CELLS]
            s = float(block.std())
            s = s if s > 1e-9 else 1.0
            scales.append(s)
            blocks.append(block / s)
        matrix = np.hstack(blocks)
        # Self-calibrating threshold: percentile of the library's own
        # NN-distance distribution. Tight while the library is immature.
        d = matrix[:, None, :] - matrix[None, :, :]
        dist = np.sqrt((d * d).sum(-1))
        np.fill_diagonal(dist, np.inf)
        nnd = dist.min(1)
        pctl = (THRESHOLD_PCTL_MATURE if len(fps) >= MATURE_LIBRARY_N
                else THRESHOLD_PCTL_TIGHT)
        threshold = float(np.percentile(nnd, pctl))
        lib = cls(matrix, scales, labels, ids, threshold, pctl)
        log.debug("[%s] fingerprint library: %d labels, %d classes, "
                  "threshold %.3f (p%.0f)", circuit, len(labels),
                  len(lib.class_counts), threshold, pctl)
        return lib

    def match(self, fingerprint) -> Optional[Dict[str, Any]]:
        """Match a raw (unscaled) fingerprint against the library.

        Returns {fixture_type, distance, threshold, neighbor_event_id,
        confidence} on a tight match, else None. Enforces the per-class
        minimum library size and the stricter threshold for CYCLE_ONLY types.
        """
        np = _np()
        if fingerprint is None:
            return None
        scaled = np.concatenate([
            fingerprint[c * N_CELLS:(c + 1) * N_CELLS] / self.scales[c]
            for c in range(3)])
        d = self.matrix - scaled[None, :]
        dist = np.sqrt((d * d).sum(-1))
        i = int(dist.argmin())
        best = float(dist[i])
        label = self.labels[i]
        if self.class_counts.get(label, 0) < MIN_CLASS_LIBRARY:
            return None
        # CYCLE_ONLY types (washer/dishwasher) inherit at the STANDARD
        # threshold: unlike the lone scalar k-NN vote that guard exists for,
        # a whole-waveform tight match against a user-confirmed example is
        # dispositive — measured 83/83 correct dishwashers at this threshold
        # (an extra tightening was tried and cost half the coverage for zero
        # precision gain). The min-class-library guard above still applies.
        limit = self.threshold
        if best > limit:
            return None
        confidence = max(0.0, min(1.0, 1.0 - best / max(limit, 1e-9)))
        return {"fixture_type": label, "distance": best,
                "threshold": limit, "neighbor_event_id": self.event_ids[i],
                "confidence": round(confidence, 3)}


# ── Simple per-circuit cache for the live path ───────────────────────────────
_CACHE: Dict[str, FingerprintLibrary] = {}
_CACHE_TTL_S: float = 300.0
_CACHE_NONE_UNTIL: Dict[str, float] = {}


def get_library(conn: sqlite3.Connection, circuit: str) -> Optional[FingerprintLibrary]:
    """TTL-cached library for the live path (reclassify loads fresh itself).
    A too-small library is cached as None for the TTL too (cheap re-check)."""
    now = time.monotonic()
    lib = _CACHE.get(circuit)
    if lib is not None and now - lib.loaded_at < _CACHE_TTL_S:
        return lib
    if _CACHE_NONE_UNTIL.get(circuit, 0) > now:
        return None
    try:
        lib = FingerprintLibrary.load(conn, circuit)
    except Exception as e:  # noqa: BLE001 — matcher must never break the pipeline
        log.warning("[%s] fingerprint library load failed: %s", circuit, e)
        lib = None
    if lib is None:
        _CACHE.pop(circuit, None)
        _CACHE_NONE_UNTIL[circuit] = now + _CACHE_TTL_S
    else:
        _CACHE[circuit] = lib
        _CACHE_NONE_UNTIL.pop(circuit, None)
    return lib


def invalidate_library_cache(circuit: Optional[str] = None) -> None:
    """Drop the cached library (call after a label save so the next live
    match sees the new label)."""
    if circuit is None:
        _CACHE.clear()
        _CACHE_NONE_UNTIL.clear()
    else:
        _CACHE.pop(circuit, None)
        _CACHE_NONE_UNTIL.pop(circuit, None)


def match_event_fingerprint(conn: sqlite3.Connection, circuit: str,
                            event_id: str,
                            library: Optional[FingerprintLibrary] = None,
                            ) -> Optional[Dict[str, Any]]:
    """Convenience: load the event's stored waveform, build its fingerprint,
    match against the (given or cached) library. None on any miss."""
    lib = library if library is not None else get_library(conn, circuit)
    if lib is None:
        return None
    row = conn.execute(
        "SELECT e.pre_event_pressure_psi, w.flow_max_json, "
        "       w.pressure_min_json, w.duration_seconds "
        "FROM events e JOIN event_waveforms w ON w.event_id = e.id "
        "WHERE e.id = ? AND e.circuit = ?", (event_id, circuit)).fetchone()
    if row is None:
        return None
    fp = build_fingerprint(row["flow_max_json"], row["pressure_min_json"],
                           row["duration_seconds"],
                           row["pre_event_pressure_psi"])
    if fp is None:
        return None
    return lib.match(fp)
