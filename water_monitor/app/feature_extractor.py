"""
Feature extractor — Phase 2.

Consumes RawEvent objects from the detection queue and:
  1. Computes the full feature vector for each event
  2. Stores the event in the SQLite events table
  3. Updates hourly_volume for the chart
  4. Updates the training state event count
  5. Feeds non-excluded events to ClusterEngine (DBSTREAM) for online
     cluster matching and sequence context recording

Algorithm: DBSTREAM via river.cluster.DBSTREAM (online, no fixed K).
DBSCAN batch clustering was considered and rejected — see ADR 003.

Feature vector:
  Temporal:
    duration_log           log(duration_seconds + 1)
    hour_sin / hour_cos    cyclical hour-of-day encoding
    day_of_week            0=Mon, 6=Sun
    is_weekend             boolean

  Flow:
    avg_flow_lpm           mean flow during event
    peak_flow_lpm          maximum flow during event
    flow_variability       std dev of flow readings

  Pressure:
    pressure_delta_psi     pre-event pressure - min pressure during event
    pre_event_pressure     baseline pressure before event
    resistance_ratio       pressure_delta / avg_flow  (true ΔP/Q)
    resistance_shape       steady/rising/falling/pulsed/unknown

  Detection provenance:
    start_trigger          'flow' | 'pressure' | 'pressure+flow'
    has_pressure_transient whether a pressure transient was captured

  Propagation:
    propagation_delay_s    seconds between event start and flow onset
                           (only meaningful for pressure-triggered events)

Resistance shape classification
---------------------------------
Shape is computed on the TRUE hydraulic resistance curve ΔP/Q, where
ΔP = pre_event_pressure - pressure[i] (the actual pressure drop due to
demand, not the absolute line pressure).

The first and last 20% of readings are excluded before classification
so ramp-up and ramp-down transients don't corrupt the trend analysis.

  steady  — CV < 0.55 and trend change < 15%
             Fixed-orifice fixture: tap, shower, hose.
  rising  — resistance increases by > 15% first→last third
             Filling a vessel against rising back-pressure: toilet cistern,
             bath, header tank.
  falling — resistance decreases by > 15% first→last third
             Zone opening against diminishing restriction: irrigation valve,
             washer fill phase.
  pulsed  — CV >= 0.55 after ramp exclusion
             Genuine cyclic demand: dishwasher spray arm rotation,
             washing machine agitation, sprinkler head sweep.
  unknown — fewer than 10 usable paired readings after ramp exclusion
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
import statistics
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .event_detector import RawEvent

log = logging.getLogger(__name__)

from .cluster_engine import SEQUENCE_GAP_MAX_SECONDS as _SEQUENCE_GAP_MAX_S


def _safe_float(values: list, default: float = 0.0) -> float:
    valid = [v for v in values if v is not None and not math.isnan(v)]
    return default if not valid else sum(valid) / len(valid)


def _safe_std(values: list) -> float:
    valid = [v for v in values if v is not None and not math.isnan(v)]
    if len(valid) < 2:
        return 0.0
    return statistics.stdev(valid)


def _classify_resistance_shape(
    pressure_readings: List[float],
    flow_readings: List[float],
    pre_event_pressure_psi: float,
) -> str:
    """
    Classify the hydraulic resistance curve shape.

    Uses TRUE resistance: ΔP/Q where ΔP = pre_event_pressure - pressure[i].
    This isolates the fixture's hydraulic load from the absolute line pressure,
    making the classification independent of household supply pressure.

    Ramp phases (first and last 20% of readings) are excluded so the
    classification reflects steady-state behaviour only.

    Returns one of: steady | rising | falling | pulsed | unknown
    """
    n = min(len(pressure_readings), len(flow_readings))
    if n < 10:
        return "unknown"

    # Exclude ramp-up and ramp-down (first and last 20%)
    ramp = max(1, n // 5)
    p_mid = pressure_readings[ramp: n - ramp]
    f_mid = flow_readings[ramp: n - ramp]

    # Compute true ΔP/Q resistance at each steady-state point.
    # Skip readings where flow is below noise floor to avoid division
    # by near-zero inflating variance.
    MIN_FLOW = 0.05   # L/min noise floor
    resistance = []
    for p, f in zip(p_mid, f_mid):
        if f >= MIN_FLOW:
            delta_p = pre_event_pressure_psi - p   # positive = pressure has dropped
            if delta_p >= 0:                        # only during actual demand
                resistance.append(delta_p / f)

    if len(resistance) < 6:
        return "unknown"

    mean_r = statistics.mean(resistance)
    if mean_r <= 0:
        return "unknown"

    # Coefficient of variation — high CV = genuinely pulsed demand.
    # Threshold 0.55 is calibrated to reject sensor noise at the pressure
    # trough (which inflated CV under the old formula) while still catching
    # real oscillating appliances (dishwashers, washing machines) which
    # typically produce CV > 0.80.
    cv = _safe_std(resistance) / mean_r
    if cv >= 0.55:
        return "pulsed"

    # Trend: compare first and last third of steady-state resistance
    third = max(1, len(resistance) // 3)
    r1 = statistics.mean(resistance[:third])
    r3 = statistics.mean(resistance[-third:])

    # 15% change threshold for rising / falling
    change_ratio = (r3 - r1) / max(abs(r1), 0.01)
    if change_ratio > 0.15:
        return "rising"
    if change_ratio < -0.15:
        return "falling"
    return "steady"


def _flow_signature(flow_readings: list, peak: float, n: int = 32) -> list:
    """Resample flow_readings to n points, normalize by peak (0–1)."""
    if not flow_readings or peak <= 0:
        return [0.0] * n
    src = flow_readings
    if len(src) == 1:
        return [min(src[0] / peak, 1.0)] * n
    result = []
    for i in range(n):
        pos = i * (len(src) - 1) / (n - 1)
        lo, hi = int(pos), min(int(pos) + 1, len(src) - 1)
        v = src[lo] * (1 - (pos - lo)) + src[hi] * (pos - lo)
        result.append(round(min(v / peak, 1.0), 4))
    return result


def _flow_edges(flow_readings: list, peak: float) -> tuple:
    """Count significant rising/falling step changes after 3-point smoothing."""
    if len(flow_readings) < 3:
        return 0, 0
    threshold = max(0.3, 0.15 * peak)
    smoothed = [
        sum(flow_readings[max(0, i - 1): min(len(flow_readings), i + 2)])
        / len(flow_readings[max(0, i - 1): min(len(flow_readings), i + 2)])
        for i in range(len(flow_readings))
    ]
    pos = neg = 0
    for i in range(1, len(smoothed)):
        d = smoothed[i] - smoothed[i - 1]
        if d >= threshold:
            pos += 1
        elif d <= -threshold:
            neg += 1
    return pos, neg


def _mid_event_flow_drop(flow_readings: list, peak: float) -> float:
    """Largest flow drop that does not terminate the event.

    A 'non-terminal' drop is one where flow remains above 20% of peak after
    the drop — signalling one fixture turning off while another keeps running.
    Returns 0.0 for single-fixture events.
    """
    n = len(flow_readings)
    if n < 3 or peak <= 0:
        return 0.0
    floor = 0.20 * peak
    max_drop = 0.0
    for i in range(1, n):
        drop = flow_readings[i - 1] - flow_readings[i]
        if drop > 0 and flow_readings[i] >= floor:
            max_drop = max(max_drop, drop)
    return round(max_drop, 4)


def _flow_steady_state(flow_readings: list) -> float:
    """Fraction of event time within ±20% of the median flow (0.0–1.0).

    High for steady showers; low for toilet fill curves and pulsed appliances.
    """
    n = len(flow_readings)
    if n < 3:
        return 0.0
    sorted_vals = sorted(flow_readings)
    median = sorted_vals[n // 2]
    if median <= 0:
        return 0.0
    threshold = 0.20 * median
    steady = sum(1 for v in flow_readings if abs(v - median) <= threshold)
    return round(steady / n, 4)


def _pressure_transient_stats(
    pressure_readings: list, pre_event_psi: float, pressure_delta_psi: float
) -> dict:
    """Compute energy and duration of the opening pressure transient.

    pressure_readings is at 40 Hz (25 ms/sample). Returns zeros for
    flow-only events where pressure_readings is empty or no transient occurred.
    """
    if not pressure_readings or pressure_delta_psi <= 0:
        return {'pressure_transient_energy': 0.0, 'pressure_transient_duration_ms': 0.0}
    threshold = 0.10 * pressure_delta_psi
    energy = sum((p - pre_event_psi) ** 2 for p in pressure_readings)
    duration_samples = sum(
        1 for p in pressure_readings if abs(p - pre_event_psi) >= threshold
    )
    return {
        'pressure_transient_energy':     round(energy, 4),
        'pressure_transient_duration_ms': round(duration_samples * 25.0, 1),
    }


def _pressure_shape_features(
    pressure_readings: list, pre_event_psi: float, pressure_delta_psi: float
) -> dict:
    """Transient shape features from the 40 Hz pressure curve.

    pressure_onset_ms        — index of minimum * 25 ms (time to peak drop)
    recovery_overshoot_psi   — max pressure above baseline after the minimum
    pressure_oscillation_count — zero-crossings of (p - pre_event_psi) post-min
    """
    zero = {
        'pressure_onset_ms': 0.0,
        'recovery_overshoot_psi': 0.0,
        'pressure_oscillation_count': 0,
    }
    if not pressure_readings or pressure_delta_psi <= 0:
        return zero

    min_idx = min(range(len(pressure_readings)), key=lambda i: pressure_readings[i])
    onset_ms = round(min_idx * 25.0, 1)

    post_min = pressure_readings[min_idx:]
    overshoot = round(max(0.0, max(post_min) - pre_event_psi), 3)

    deviations = [p - pre_event_psi for p in post_min]
    crossings = sum(
        1 for i in range(1, len(deviations))
        if deviations[i - 1] * deviations[i] < 0
    )

    return {
        'pressure_onset_ms':          onset_ms,
        'recovery_overshoot_psi':     overshoot,
        'pressure_oscillation_count': crossings,
    }


def _flow_dynamics(flow_readings: list, peak: float) -> dict:
    """Rise/fall rates, opening/closing step magnitudes, and 90% ramp times.

    Assumes uniform 1 Hz sampling (1 index = 1 second). For events > 120s the
    event_detector downsamples to 0.2 Hz so timing values are approximate for
    long irrigation runs — acceptable since those are identified by volume/duration.
    """
    zero = {
        'flow_rise_rate_lpm_s': 0.0, 'flow_fall_rate_lpm_s': 0.0,
        'opening_step_lpm': 0.0,     'closing_step_lpm': 0.0,
        'time_to_90pct_flow_seconds': 0.0,
        'time_from_90pct_to_zero_seconds': 0.0,
    }
    n = len(flow_readings)
    if n < 2 or peak <= 0:
        return zero

    peak_idx = max(range(n), key=lambda i: flow_readings[i])
    rise_rate = peak / max(peak_idx, 1)
    fall_rate = peak / max(n - 1 - peak_idx, 1)

    deltas = [flow_readings[i] - flow_readings[i - 1] for i in range(1, n)]
    opening_step = max((d for d in deltas if d > 0), default=0.0)
    closing_step = max((-d for d in deltas if d < 0), default=0.0)

    threshold_90 = 0.9 * peak
    t_rise = next((i for i, v in enumerate(flow_readings) if v >= threshold_90), n - 1)
    t_fall_rev = next(
        (i for i, v in enumerate(reversed(flow_readings)) if v >= threshold_90), 0
    )

    return {
        'flow_rise_rate_lpm_s':            round(rise_rate, 4),
        'flow_fall_rate_lpm_s':            round(fall_rate, 4),
        'opening_step_lpm':                round(opening_step, 4),
        'closing_step_lpm':                round(closing_step, 4),
        'time_to_90pct_flow_seconds':      float(t_rise),
        'time_from_90pct_to_zero_seconds': float(t_fall_rev),
    }


def extract_features(event: RawEvent) -> Dict[str, Any]:
    """Compute the full feature vector from a RawEvent."""
    duration = 0.0
    if event.end_ts and event.start_ts:
        duration = (event.end_ts - event.start_ts).total_seconds()

    avg_flow = _safe_float(event.flow_readings)
    peak_flow = max(event.flow_readings) if event.flow_readings else 0.0
    flow_variability = _safe_std(event.flow_readings)

    sig          = _flow_signature(event.flow_readings, peak_flow)
    pos_edges, neg_edges = _flow_edges(event.flow_readings, peak_flow)
    dynamics     = _flow_dynamics(event.flow_readings, peak_flow)
    mid_drop     = _mid_event_flow_drop(event.flow_readings, peak_flow)
    steady       = _flow_steady_state(event.flow_readings)
    p_stats      = _pressure_transient_stats(
        event.pressure_readings, event.pre_event_pressure_psi, event.pressure_delta_psi
    )
    p_shape      = _pressure_shape_features(
        event.pressure_readings, event.pre_event_pressure_psi, event.pressure_delta_psi
    )

    # Volume: prefer the firmware's cumulative integration sensor delta (set by
    # the historical importer) over the flow-average approximation, which can
    # overstate volume for long events with fill-pause-fill patterns after
    # downsampling kicks in at 120 s.
    if event.volume_litres_measured is not None:
        volume_litres = event.volume_litres_measured
    else:
        volume_litres = avg_flow * (duration / 60.0) if duration > 0 else 0.0

    # True hydraulic resistance: ΔP / avg_Q
    # Only meaningful when flow is above noise floor and a pressure
    # transient was actually captured.
    resistance: Optional[float] = None
    if avg_flow >= 0.05 and event.has_pressure_transient and event.pressure_delta_psi > 0:
        resistance = event.pressure_delta_psi / avg_flow

    # Resistance curve shape — uses corrected ΔP/Q formula.
    # pressure_readings are at 40 Hz, flow_readings at 1 Hz.  Pair them by
    # averaging each pressure bin that corresponds to one flow sample so the
    # resistance values are time-aligned.  Without downsampling, index-pairing
    # would match the first ~0.75 s of pressure against the full event duration
    # of flow, making every result meaningless.
    pressure_for_shape = event.pressure_readings
    if (event.flow_readings and event.pressure_readings
            and len(event.pressure_readings) > len(event.flow_readings)):
        n_flow = len(event.flow_readings)
        n_pres = len(event.pressure_readings)
        step = n_pres / n_flow          # fractional step to stay evenly spaced
        pressure_for_shape = []
        for i in range(n_flow):
            lo = int(round(i * step))
            hi = int(round((i + 1) * step))
            hi = max(hi, lo + 1)        # guarantee at least one sample per bin
            bin_samples = event.pressure_readings[lo:hi]
            pressure_for_shape.append(sum(bin_samples) / len(bin_samples))

    shape = _classify_resistance_shape(
        pressure_for_shape,
        event.flow_readings,
        event.pre_event_pressure_psi,
    )

    # Time features
    ts = event.start_ts
    hour = ts.hour
    dow = ts.weekday()
    hour_radians = 2 * math.pi * hour / 24
    duration_log = math.log(duration + 1)

    # Normalize timestamps to UTC so the UUID5 id and stored start_ts are
    # stable regardless of what timezone the incoming RawEvent carries.
    # This is the single storage point — all paths that write events go
    # through extract_features(), so enforcing UTC here is sufficient.
    _start = event.start_ts
    if _start.tzinfo is None:
        _start = _start.replace(tzinfo=timezone.utc)
    start_utc = _start.astimezone(timezone.utc)
    _end = event.end_ts
    if _end is not None:
        if _end.tzinfo is None:
            _end = _end.replace(tzinfo=timezone.utc)
        end_utc = _end.astimezone(timezone.utc)
    else:
        end_utc = None

    return {
        # Identity — UUID5 keyed on UTC start_ts so re-imports of the same
        # event always produce the same id and INSERT OR REPLACE is a no-op.
        "id": str(uuid.uuid5(uuid.NAMESPACE_OID,
                              f"{event.circuit}/{start_utc.isoformat()}")),
        "circuit": event.circuit,
        "start_ts": start_utc.isoformat(),
        "end_ts": end_utc.isoformat() if end_utc else None,

        # Raw measurements
        "duration_seconds": round(duration, 2),
        "avg_flow_lpm": round(avg_flow, 3),
        "peak_flow_lpm": round(peak_flow, 3),
        "flow_variability": round(flow_variability, 4),
        "pressure_delta_psi": round(event.pressure_delta_psi, 2),
        "pre_event_pressure_psi": round(event.pre_event_pressure_psi, 2),
        "min_pressure_psi": round(event.min_pressure_psi, 2),
        "hydraulic_resistance": round(resistance, 3) if resistance is not None else None,
        "resistance_curve_shape": shape,
        "volume_litres": round(volume_litres, 3),

        # Detection provenance — tells Phase 2 how reliable pressure data is
        "start_trigger": event.start_trigger,
        "has_pressure_transient": 1 if event.has_pressure_transient else 0,
        "propagation_delay_ms": (
            round(event.propagation_delay_ms, 1)
            if event.propagation_delay_ms is not None else None
        ),

        # Derived features for ML clustering
        "duration_log": round(duration_log, 4),
        "hour_of_day": hour,
        "day_of_week": dow,
        "hour_sin": round(math.sin(hour_radians), 4),
        "hour_cos": round(math.cos(hour_radians), 4),
        "is_weekend": 1 if dow >= 5 else 0,

        # Composite / training flags
        "is_composite": 1 if event.is_composite else 0,
        "other_valve_open": (
            1 if event.other_valve_open is True
            else 0 if event.other_valve_open is False
            else None
        ),
        "excluded_from_training": 1 if event.is_composite else 0,

        # Flow shape features
        "flow_signature_json":    json.dumps(sig),
        "positive_edge_count":    pos_edges,
        "negative_edge_count":    neg_edges,
        "flow_edge_count":        pos_edges + neg_edges,
        **dynamics,
        "mid_event_flow_drop_lpm": mid_drop,
        "steady_state_fraction":  steady,

        # Pressure transient features
        **p_stats,

        # Pressure transient shape features
        "pressure_onset_ms":          p_shape['pressure_onset_ms'],
        "recovery_overshoot_psi":     p_shape['recovery_overshoot_psi'],
        "pressure_oscillation_count": p_shape['pressure_oscillation_count'],
    }


class FeatureExtractor:
    """
    Consumes RawEvent objects from the queue and stores
    extracted features in SQLite.
    """

    def __init__(self, event_queue: asyncio.Queue,
                 db_conn: sqlite3.Connection, alert_manager=None,
                 ha_client=None):
        self._queue = event_queue
        self._db = db_conn
        self._alert_manager = alert_manager
        self._ha = ha_client
        self._running = False
        # Set by orchestrator after ClusterEngine is initialised and rebuilt.
        self.cluster_engine = None

    async def run(self) -> None:
        """Process events from the queue until cancelled."""
        self._running = True
        log.info("Feature extractor started")
        while self._running:
            try:
                event: RawEvent = await asyncio.wait_for(
                    self._queue.get(), timeout=5.0)
                await self._process(event)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("Feature extractor error: %s", e, exc_info=True)

    def stop(self) -> None:
        self._running = False

    async def _enrich_propagation_delay(self, event: RawEvent) -> None:
        """Replace the jittery WebSocket-callback propagation_delay_ms with the
        precise server-side last_changed timestamp from HA history."""
        from datetime import timedelta
        window_start = event.start_ts - timedelta(seconds=5)
        window_end   = (event.end_ts or event.start_ts) + timedelta(seconds=15)
        try:
            history = await self._ha.get_history(
                event.flow_onset_entity, window_start, window_end)
            onset = next(
                (h for h in history
                 if h["state"].lower() in ("on", "true", "1")
                 and h["last_changed"] >= event.start_ts),
                None,
            )
            if onset:
                event.propagation_delay_ms = round(
                    (onset["last_changed"] - event.start_ts).total_seconds() * 1000, 1)
                log.debug("[%s] propagation delay enriched from HA history: %.0f ms",
                          event.circuit, event.propagation_delay_ms)
        except Exception as e:
            log.debug("[%s] propagation delay HA enrichment failed: %s",
                      event.circuit, e)

    async def _process(self, event: RawEvent) -> None:
        if not event.complete:
            return

        if (self._ha and event.flow_onset_entity
                and event.start_trigger in ("pressure", "pressure+flow")):
            await self._enrich_propagation_delay(event)

        features = extract_features(event)

        try:
            from .database import (insert_event, update_hourly_volume,
                                   is_event_in_exclusion_window)
            is_new_event = insert_event(self._db, features)

            # ── Plumbing-event exclusion window (Phase 2.1) ───────────────
            # If the user opened an exclusion window (e.g. post-winterization
            # flush), flag the event so the cluster engine skips it.  Volume
            # tracking continues — only fixture identification is excluded.
            start_ts_str = features.get("start_ts")
            if (start_ts_str
                    and is_event_in_exclusion_window(
                        self._db, event.circuit, start_ts_str)):
                self._db.execute(
                    """UPDATE events
                       SET excluded_from_training  = 1,
                           match_rejection_reason  = 'excluded_from_training'
                       WHERE id = ?""",
                    (features["id"],),
                )
                features["excluded_from_training"] = 1
                log.debug(
                    "[%s] event excluded from training (exclusion window active)",
                    event.circuit,
                )

            # Only accumulate volume and training counts for genuinely new events.
            # Re-imports (INSERT OR REPLACE replacing an existing row) must not
            # add to these totals again — that would inflate the hourly chart and
            # the training progress bar on every addon restart.
            if is_new_event and event.start_ts and features.get("volume_litres", 0) > 0:
                # Normalize to UTC and strip timezone info so hour_ts matches
                # the format that DB queries use: strftime('%Y-%m-%dT%H:00:00', …)
                # produces no timezone suffix.  Mixing +00:00 suffixed values
                # with bare datetime strings breaks lexicographic comparison in
                # get_daily_volume / get_weekly_volume / data pruner.
                _hdt = event.start_ts
                if _hdt.tzinfo is None:
                    _hdt = _hdt.replace(tzinfo=timezone.utc)
                hour_ts = _hdt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:00:00')
                update_hourly_volume(
                    self._db,
                    event.circuit,
                    hour_ts,
                    features["volume_litres"],
                )

            if is_new_event and not features.get("excluded_from_training"):
                self._db.execute("""
                    UPDATE training_state
                    SET events_collected = events_collected + 1,
                        updated_at = datetime('now')
                    WHERE circuit = ?
                      AND state = 'calibrating'
                """, (event.circuit,))
            self._db.commit()

            # ── Phase 2: sequence context + cluster matching ───────────────
            await self._cluster_event(event.circuit, features)
            # ──────────────────────────────────────────────────────────────

            am = self._alert_manager
            if am and features.get("anomaly_score"):
                score = float(features["anomaly_score"])
                ts_row = self._db.execute(
                    "SELECT state FROM training_state WHERE circuit = ?",
                    (event.circuit,)).fetchone()
                if ts_row and ts_row["state"] == "live" and score >= 0.60:
                    circuit_name = event.circuit.replace("_", " ").title()
                    asyncio.create_task(
                        am.alert_flow_anomaly(event.circuit, score, circuit_name))

            log.debug(
                "[%s] event stored — duration=%.1fs shape=%s trigger=%s "
                "transient=%s resistance=%.2f",
                event.circuit,
                features["duration_seconds"],
                features["resistance_curve_shape"],
                features["start_trigger"],
                features["has_pressure_transient"],
                features["hydraulic_resistance"] or 0,
            )
        except Exception as e:
            log.error("[%s] failed to store event: %s", event.circuit, e, exc_info=True)

    async def _cluster_event(self, circuit: str, features: dict) -> None:
        """Compute sequence context, run cluster matching, write results back."""
        if features.get("excluded_from_training"):
            return

        event_id    = features["id"]
        start_ts    = features.get("start_ts")

        # 1. Find the previous event on this circuit
        seconds_since_prev = None
        prev_cluster_id    = None
        if start_ts:
            prev = self._db.execute(
                """SELECT id, cluster_id, end_ts FROM events
                   WHERE circuit = ? AND end_ts < ? AND id != ?
                   ORDER BY end_ts DESC LIMIT 1""",
                (circuit, start_ts, event_id)
            ).fetchone()
            if prev and prev["end_ts"]:
                try:
                    ev_start = datetime.fromisoformat(start_ts)
                    prev_end = datetime.fromisoformat(
                        prev["end_ts"].replace("Z", "+00:00"))
                    if ev_start.tzinfo is None:
                        ev_start = ev_start.replace(tzinfo=timezone.utc)
                    if prev_end.tzinfo is None:
                        prev_end = prev_end.replace(tzinfo=timezone.utc)
                    gap = (ev_start - prev_end).total_seconds()
                    if 0 <= gap < _SEQUENCE_GAP_MAX_S:
                        seconds_since_prev = gap
                        prev_cluster_id    = prev["cluster_id"]
                        # Retroactively fill seconds_to_next_event on previous event
                        self._db.execute(
                            "UPDATE events SET seconds_to_next_event = ? WHERE id = ?",
                            (gap, prev["id"])
                        )
                except (ValueError, TypeError):
                    pass

        # 2. Cluster matching (sync DB writes dispatched off the event loop)
        cluster_id_result = None
        match_confidence  = None
        match_level       = None
        match_rejection_reason: Optional[str] = None
        if self.cluster_engine:
            try:
                event_row = self._db.execute(
                    "SELECT * FROM events WHERE id = ?", (event_id,)
                ).fetchone()
                if event_row:
                    import functools
                    loop = asyncio.get_running_loop()
                    (cluster_id_result, match_confidence, match_level,
                     match_rejection_reason) = await loop.run_in_executor(
                        None,
                        functools.partial(
                            self.cluster_engine.match_and_learn,
                            dict(event_row),
                            circuit,
                            prev_cluster_id,
                            seconds_since_prev,
                        )
                    )
            except Exception as e:
                log.error("[%s] cluster matching failed: %s", circuit, e,
                          exc_info=True)

        # 3. Derive anomaly_score and store it in features so the alert
        #    check in _process() can read it.  anomaly_score is intentionally
        #    NOT stored in the events table — it is ephemeral and recalculated
        #    by the live match path only; backfill uses match_confidence directly.
        #    High score = anomalous:
        #      • no match at all           → 1.0
        #      • poor confidence match     → 1.0 - confidence
        #      • good confidence match     → near 0.0
        if match_confidence is not None:
            features["anomaly_score"] = round(1.0 - match_confidence, 3)
        elif cluster_id_result is None and match_rejection_reason not in (
            "type_gate_rejected", "excluded_from_training"
        ):
            # Unmatched event in live state with no explicit rejection reason
            # — treat as fully anomalous.
            features["anomaly_score"] = 1.0

        # Write cluster results back to the event row
        self._db.execute(
            """UPDATE events SET
                 cluster_id               = ?,
                 match_confidence         = ?,
                 match_level              = ?,
                 match_rejection_reason   = ?,
                 seconds_since_prev_event = ?,
                 prev_cluster_id          = ?
               WHERE id = ?""",
            (cluster_id_result, match_confidence, match_level,
             match_rejection_reason,
             seconds_since_prev, prev_cluster_id, event_id)
        )

        # 4. Update fixtures.last_seen_at when this event matched a named fixture
        if cluster_id_result is not None:
            fc_row = self._db.execute(
                """SELECT fixture_id FROM fixture_clusters
                   WHERE circuit = ? AND id = ? AND fixture_id IS NOT NULL""",
                (circuit, cluster_id_result)
            ).fetchone()
            if fc_row and fc_row["fixture_id"]:
                self._db.execute(
                    "UPDATE fixtures SET last_seen_at = ? WHERE id = ?",
                    (datetime.now(timezone.utc).isoformat(), fc_row["fixture_id"])
                )

        self._db.commit()
