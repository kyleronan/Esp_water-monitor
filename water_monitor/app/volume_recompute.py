"""Historical volume + active-flow backfill.

Re-derives ``volume_litres`` and the active-flow features for existing events by
re-fetching the raw HA flow history per event and time-integrating it (the same
``flow_integral`` path the live detector and importer use). Needed because the
raw per-event flow series isn't persisted — only a normalized 32-pt signature.

Design (review guardrails):
  * HA fetch is INJECTED (``fetch_flow_history``) so this module stays free of
    HA-client deps and is unit-testable.
  * VALIDATE before overwriting trusted data: unavailable/empty history → skip;
    missing pre-start state or large gaps → recompute but mark
    ``integration_quality='degraded'`` (kept out of classifier training); an
    extreme old→new ratio is logged.
  * AUDIT: ``volume_litres_original`` is captured once; ``volume_recomputed_at``
    stamped; old→new logged. Easy verify / rollback.
  * MANUAL-SAFE: ``user_classified`` rows keep their manual verdict; only the raw
    + effective volume are corrected.
  * Retention-limited: events whose flow history is gone are skipped (logged),
    keeping their stored volume.
"""
from __future__ import annotations

import bisect
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple

from .flow_integral import integrate_litres, active_flow_features

log = logging.getLogger(__name__)

# A flow sample = (timestamp, L/min). The fetch fn returns samples over a window
# that should extend a little before ``start`` (for the pre-start value), or
# None when history is unavailable / units are wrong (→ skip, don't overwrite).
FetchFlowHistory = Callable[[datetime, datetime], Optional[List[Tuple[datetime, float]]]]

_PRESTART_LOOKBACK_S: float = 600.0     # how far before start to seek a baseline
_MAX_GAP_DEGRADED_S: float = 300.0      # an in-window gap larger than this → degraded
_EXTREME_RATIO: float = 5.0             # log when |new/old| crosses this


async def build_flow_fetch(ha, flow_sensor: str, range_start: datetime,
                           range_end: datetime) -> FetchFlowHistory:
    """Fetch the circuit's flow history ONCE (async) and return a sync closure
    ``fetch(start, end)`` for ``recompute_volume_and_active_flow`` (which runs in
    an executor). Keeps the async HA call out of the sync recompute loop.

    Flow-rate readings are used as-is (L/min) — the same assumption the live
    detector makes; only the cumulative volume sensor needs unit conversion.
    """
    hist = await ha.get_history(flow_sensor, range_start, range_end)
    samples: List[Tuple[datetime, float]] = []
    for e in hist or ():
        t = _parse_ts(e.get("last_changed"))
        if t is None:
            continue
        try:
            v = float(e.get("state"))
        except (TypeError, ValueError):
            continue
        if v >= 0:
            samples.append((t, v))
    samples.sort(key=lambda s: s[0])
    times = [t for t, _ in samples]

    def fetch(start: datetime, end: datetime):
        lo = bisect.bisect_left(times, start - timedelta(seconds=_PRESTART_LOOKBACK_S))
        hi = bisect.bisect_right(times, end)
        window = samples[lo:hi]
        return window if window else None     # None → caller skips (no overwrite)

    return fetch


def _parse_ts(s) -> Optional[datetime]:
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _prepare_samples(
    raw: List[Tuple[datetime, float]], start: datetime, end: datetime,
) -> Tuple[List[Tuple[datetime, float]], bool, float]:
    """Clip to [start, end], pad start/end. Returns (samples, no_prestate, max_gap)."""
    pts = sorted((t, float(v)) for t, v in raw if t is not None)
    before = [(t, v) for t, v in pts if t <= start]
    in_win = [(t, v) for t, v in pts if start < t <= end]
    no_prestate = not before
    prestart_v = before[-1][1] if before else (in_win[0][1] if in_win else 0.0)
    samples = [(start, prestart_v)] + in_win
    if samples[-1][0] < end:
        samples.append((end, samples[-1][1]))
    # Only gaps where REAL flow was held can lose water; a long zero-flow tail
    # (e.g. a brief burst inside a long pressure window) is not degraded.
    max_gap = 0.0
    for i in range(1, len(samples)):
        if samples[i - 1][1] > 0.15:
            gap = (samples[i][0] - samples[i - 1][0]).total_seconds()
            max_gap = max(max_gap, gap)
    return samples, no_prestate, max_gap


def recompute_volume_and_active_flow(
    conn: sqlite3.Connection,
    circuit: str,
    fetch_flow_history: FetchFlowHistory,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, int]:
    """Backfill volume + active-flow for every event on ``circuit``.

    Returns counts: recomputed, skipped (no history), degraded, unchanged.
    """
    from .database import (transaction, compute_daily_summary,
                           apply_effective_volume, local_day_of)
    from .feature_extractor import _finalize_derived_verdicts
    from .artifact_calibration import load_artifact_calibration

    _acal = load_artifact_calibration(conn, circuit) or None  # Phase 2.4
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    rows = conn.execute(
        "SELECT id, start_ts, end_ts, duration_seconds, pressure_delta_psi, "
        "       volume_litres, volume_litres_estimated, volume_litres_original, "
        "       volume_recorder_litres, flow_pressure_corr, "
        "       degraded_supply, is_composite, user_classified, user_ignored, "
        "       is_pressure_restoration_phantom, is_cross_talk, is_low_flow_dribble, "
        "       excluded_from_training, match_rejection_reason, user_fixture_type, "
        "       hourly_volume_applied_litres, hourly_volume_applied_bucket "
        "FROM events WHERE circuit = ? ORDER BY start_ts",
        (circuit,),
    ).fetchall()

    counts = {"recomputed": 0, "skipped": 0, "degraded": 0, "unchanged": 0}
    affected_days: set = set()

    for r in rows:
        start = _parse_ts(r["start_ts"])
        end = _parse_ts(r["end_ts"])
        if start is None or end is None or end <= start:
            counts["skipped"] += 1
            continue

        raw = fetch_flow_history(start, end)
        if not raw:                       # None or empty → don't overwrite
            counts["skipped"] += 1
            continue

        samples, no_prestate, max_gap = _prepare_samples(raw, start, end)
        new_litres, capped = integrate_litres(samples)
        af = active_flow_features(samples, (end - start).total_seconds())
        degraded = capped or no_prestate or max_gap > _MAX_GAP_DEGRADED_S
        quality = "degraded" if degraded else "ok"

        # §2 coherence: the recorder cumulative-sensor delta is the authoritative volume
        # for a healthy event — prefer it over this (re-integrated, lossy) value so a
        # manual Recompute can't undo a recorder reconcile. Healthy only (un-flagged +
        # not degraded); the verdict/effective logic below is unchanged.
        rec = r["volume_recorder_litres"]
        if (rec is not None and float(rec) > 0 and not degraded
                and not (r["is_pressure_restoration_phantom"] or r["is_cross_talk"]
                         or r["is_low_flow_dribble"] or r["excluded_from_training"])):
            new_litres = float(rec)

        old_volume = float(r["volume_litres"] or 0.0)
        if old_volume > 0 and (
            new_litres / old_volume > _EXTREME_RATIO
            or old_volume / max(new_litres, 1e-9) > _EXTREME_RATIO
        ):
            log.info("[%s] recompute %s: volume %.1f L → %.1f L (ratio outlier)",
                     circuit, r["id"], old_volume, new_litres)

        # Effective volume + verdicts (manual-safe).
        est = r["volume_litres_estimated"]
        if r["user_classified"]:
            # Keep the manual phantom/degraded verdict; correct the volume only.
            if r["is_pressure_restoration_phantom"]:
                eff, method = 0.0, "pressure_restoration_phantom"
            elif r["degraded_supply"]:
                eff = float(est) if est is not None else new_litres
                method = "pulsing_supply_envelope"
            else:
                eff, method = new_litres, "raw"
            feat = None
        else:
            feat = {
                "duration_seconds": r["duration_seconds"],
                "pressure_delta_psi": r["pressure_delta_psi"],
                "degraded_supply": r["degraded_supply"],
                "is_composite": r["is_composite"],
                "user_ignored": r["user_ignored"],
                "user_classified": 0,
                "volume_litres": new_litres,
                "volume_litres_estimated": est,
                "true_avg_flow_lpm": af["true_avg_flow_lpm"],
                "flow_integral_litres": new_litres,
                "flow_on_ratio": af["flow_on_ratio"],
                "integration_quality": quality,
                # Carry the provenance so a durable irrigation zone-switch cross-talk
                # verdict survives a manual full recompute (else _finalize would clear
                # it — the main-only detector can't reproduce a short event).
                "match_rejection_reason": r["match_rejection_reason"],
                # Carry the stored correlation (dev14) — waveforms are gone at
                # recompute time, so without this the rise-phantom verdict would
                # silently clear (corr None ⇒ detector can't fire) and the false
                # volume would return. _finalize re-derives the verdict from it.
                "flow_pressure_corr": r["flow_pressure_corr"],
                # ...and the user's label, so _finalize's "a real fixture label wins
                # over the cross-talk zeroing" escape hatch can actually fire here
                # (without it a recompute re-zeroed a user-corrected event).
                "user_fixture_type": r["user_fixture_type"],
            }
            from .config import pump_gates_active as _pga
            try:
                _pump = _pga(conn, circuit)
            except Exception:
                _pump = False
            _finalize_derived_verdicts(feat, _acal, pump_gates=_pump)
            eff = feat["volume_litres_effective"]
            method = feat["volume_estimation_method"]

        original = r["volume_litres_original"]
        if original is None:
            original = old_volume

        with transaction(conn):
            conn.execute(
                "UPDATE events SET "
                "  volume_litres = ?, flow_integral_litres = ?, "
                "  active_flow_duration_seconds = ?, true_avg_flow_lpm = ?, "
                "  flow_on_ratio = ?, active_flow_segment_count = ?, "
                "  flow_cv_on_segments = ?, integration_quality = ?, "
                "  volume_litres_original = ?, volume_recomputed_at = ?, "
                "  volume_litres_effective = ?, volume_estimation_method = ? "
                + ("" if feat is None else
                   ", is_pressure_restoration_phantom = ?, is_low_flow_dribble = ?, "
                   "is_cross_talk = ?, phantom_suppression_averted = ?, "
                   "excluded_from_training = ?, match_rejection_reason = ?")
                + " WHERE id = ? AND circuit = ?",
                (
                    round(new_litres, 3), round(new_litres, 3),
                    af["active_flow_duration_seconds"], af["true_avg_flow_lpm"],
                    af["flow_on_ratio"], af["active_flow_segment_count"],
                    af["flow_cv_on_segments"], quality,
                    round(original, 3), stamp,
                    round(eff, 3), method,
                    *(() if feat is None else (
                        feat["is_pressure_restoration_phantom"],
                        feat["is_low_flow_dribble"],
                        # write is_cross_talk back too: _finalize either preserves
                        # the durable irrigation verdict (=1) or, when a user label
                        # confirms real water, drops it (=0) — without this column
                        # the flag stayed 1 while the volume was restored, a
                        # half-reverted row (hidden in History, counted in totals).
                        feat["is_cross_talk"],
                        feat.get("phantom_suppression_averted", 0),
                        feat["excluded_from_training"],
                        feat["match_rejection_reason"],
                    )),
                    r["id"], circuit,
                ),
            )
            # §2.5 — reverse/apply/bookkeep via the one chokepoint.
            apply_effective_volume(conn, r["id"], circuit, r["start_ts"], eff)

        day = local_day_of(r["start_ts"])
        if day:
            affected_days.add(day)
        counts["recomputed"] += 1
        if degraded:
            counts["degraded"] += 1

    for day in affected_days:
        compute_daily_summary(conn, circuit, day)
    if affected_days:
        conn.commit()

    log.info("[%s] volume recompute: %s", circuit, counts)
    return counts
