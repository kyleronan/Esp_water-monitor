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
  * VOLUME FLOOR (post-3.13 era correction): the numbers above were measured
    on coarse-meter data whose sub-2 L draws never became events. The
    pulse_meter firmware DOES eventize them, and the first fresh-DB review
    (2026-07-08) overturned every fingerprint stamp — all 46 were sub-2 L
    micro-draws (0/11 on the reviewed subset). Events under
    MIN_MATCH_VOLUME_L effective litres neither join the library nor get
    matched, restoring the event population the validation actually covered.

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

# ── dev46 (46q) — era weighting ──────────────────────────────────────────────
#
# The library spans a supply change: the booster pump went in 2026-07-19 and
# moved every fixture's geometry (toilet ΔP by 2.6×). A pre-pump exemplar is
# not a worse example of a toilet — it is an accurate example of a DIFFERENT
# hydraulic regime, and letting it win a match imports that regime's shape.
#
# AGE IS EVENT-ERA, NEVER WALL CLOCK: the gap between the candidate event's
# own timestamp and the library member's. Wall-clock age would make matching a
# function of when you happened to run it — the same event would drift to
# different answers month over month with no config change, boot reclassify
# would silently re-label history, and no "classifier fingerprint" could ever
# certify a stored match as still valid. Era age keeps matching a pure
# function of stored data. It is also simply more correct: a July event should
# be judged against July.
#
# Applied as a distance MULTIPLIER, so an old exemplar has to be
# proportionally closer to win. Constants are documented and hand-set — never
# auto-fit (the same rule the T5 and flush-floor gates follow).
FP_AGE_HALFLIFE_DAYS: float = 60.0   # penalty reaches half its range here
FP_AGE_MAX_PENALTY: float = 4.0      # asymptote: 4x distance for ancient eras


def _era_penalty(age_days: float) -> float:
    """Distance multiplier for an exemplar ``age_days`` from the candidate.

    1.0 at zero age, rising asymptotically to FP_AGE_MAX_PENALTY. Symmetric:
    a member from the wrong era is equally wrong whichever side it falls on
    (reclassify walks history, so members are not always older).
    """
    if age_days <= 0:
        return 1.0
    decayed = 0.5 ** (age_days / FP_AGE_HALFLIFE_DAYS)
    return 1.0 + (FP_AGE_MAX_PENALTY - 1.0) * (1.0 - decayed)


def _parse_ts(value) -> Optional[float]:
    """ISO timestamp -> epoch seconds; None on anything unparseable."""
    if not value:
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None
# Micro-draws are noise-alike: they cluster tightly, drag the self-calibrated
# threshold down, and inherit each other's labels. Both library membership and
# query events must carry at least this much EFFECTIVE volume (falls back to
# raw volume when no ledger verdict exists yet).
MIN_MATCH_VOLUME_L: float = 2.0


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
                 event_ids: List[str], threshold: float, pctl: float,
                 start_ts: Optional[List[Optional[float]]] = None):
        self.matrix = matrix          # (n, 3*N_CELLS) channel-scaled
        self.scales = scales          # per-channel std used to scale
        self.labels = labels
        self.event_ids = event_ids
        # dev46 (46q): each member's OWN event time (epoch seconds), for
        # era weighting. None where a row had no parseable timestamp — such
        # members simply take no penalty rather than being excluded.
        self.start_ts = start_ts if start_ts is not None else [None] * len(labels)
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
            "       e.start_ts, "
            "       w.flow_max_json, w.pressure_min_json, w.duration_seconds "
            "FROM events e JOIN event_waveforms w ON w.event_id = e.id "
            "WHERE e.circuit = ? AND e.user_fixture_type IS NOT NULL "
            "  AND e.training_quarantine_reason IS NULL "
            "  AND COALESCE(e.training_excluded_by_user, 0) = 0 "
            "  AND e.duration_seconds >= ? "
            "  AND COALESCE(e.volume_litres_effective, e.volume_litres) >= ?",
            (circuit, MIN_EVENT_SECONDS, MIN_MATCH_VOLUME_L)).fetchall()
        fps, labels, ids, stamps = [], [], [], []
        for r in rows:
            fp = build_fingerprint(r["flow_max_json"], r["pressure_min_json"],
                                   r["duration_seconds"],
                                   r["pre_event_pressure_psi"])
            if fp is not None:
                fps.append(fp)
                labels.append(r["user_fixture_type"])
                ids.append(r["id"])
                stamps.append(_parse_ts(r["start_ts"]))
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
        lib = cls(matrix, scales, labels, ids, threshold, pctl, stamps)
        log.debug("[%s] fingerprint library: %d labels, %d classes, "
                  "threshold %.3f (p%.0f)", circuit, len(labels),
                  len(lib.class_counts), threshold, pctl)
        return lib

    def match(self, fingerprint,
              event_ts: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Match a raw (unscaled) fingerprint against the library.

        Returns {fixture_type, distance, threshold, neighbor_event_id,
        confidence} on a tight match, else None. Enforces the per-class
        minimum library size and the stricter threshold for CYCLE_ONLY types.

        ``event_ts`` is the CANDIDATE event's own time (epoch seconds). When
        given, exemplars from a different era are pushed away in proportion to
        the gap (dev46 46q) — an old exemplar must be proportionally closer to
        win. Omitted, or where a member has no timestamp, no penalty applies,
        so this can never make the matcher stricter than it was.
        """
        np = _np()
        if fingerprint is None:
            return None
        scaled = np.concatenate([
            fingerprint[c * N_CELLS:(c + 1) * N_CELLS] / self.scales[c]
            for c in range(3)])
        d = self.matrix - scaled[None, :]
        dist = np.sqrt((d * d).sum(-1))
        # dev46 (46q): era weighting BEFORE picking the nearest neighbour —
        # penalising only the winner would let a stale exemplar shut out a
        # better-era one that was a hair further away in raw distance.
        raw_dist = dist
        if event_ts is not None:
            pen = np.ones(len(self.labels), dtype=float)
            for k, member_ts in enumerate(self.start_ts):
                if member_ts is None:
                    continue
                pen[k] = _era_penalty(abs(event_ts - member_ts) / 86400.0)
            dist = dist * pen
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
                # The unweighted distance, for diagnostics: a match that only
                # passed because it was same-era looks different from one that
                # was simply close.
                "raw_distance": float(raw_dist[i]),
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
    match against the (given or cached) library. None on any miss, including
    events below MIN_MATCH_VOLUME_L effective litres (the tier's floor —
    every production call path routes through here)."""
    lib = library if library is not None else get_library(conn, circuit)
    if lib is None:
        return None
    row = conn.execute(
        "SELECT e.pre_event_pressure_psi, e.start_ts, w.flow_max_json, "
        "       w.pressure_min_json, w.duration_seconds, "
        "       COALESCE(e.volume_litres_effective, e.volume_litres) AS vol_eff "
        "FROM events e JOIN event_waveforms w ON w.event_id = e.id "
        "WHERE e.id = ? AND e.circuit = ?", (event_id, circuit)).fetchone()
    if row is None:
        return None
    if row["vol_eff"] is None or row["vol_eff"] < MIN_MATCH_VOLUME_L:
        return None   # micro-draw below the tier's floor — abstain
    fp = build_fingerprint(row["flow_max_json"], row["pressure_min_json"],
                           row["duration_seconds"],
                           row["pre_event_pressure_psi"])
    if fp is None:
        return None
    # dev46 (46q): the candidate's OWN time drives era weighting — never
    # "now", so a stored match stays valid regardless of when it is re-derived.
    return lib.match(fp, event_ts=_parse_ts(row["start_ts"]))
