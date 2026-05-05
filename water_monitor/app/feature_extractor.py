"""
Feature extractor — Phase 1.

Consumes RawEvent objects from the detection queue and:
  1. Computes the full feature vector for each event
  2. Stores the event in the SQLite events table
  3. Updates hourly_volume for the chart
  4. Updates the training state event count

Phase 2 will add DBSCAN clustering and fixture matching on top of
this stored data. Phase 1 just makes sure we're collecting the right
features from day one.

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
import logging
import math
import sqlite3
import statistics
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .event_detector import RawEvent

log = logging.getLogger(__name__)

# Maximum gap between consecutive events that counts as a sequence (seconds).
# Must match cluster_engine.SEQUENCE_GAP_MAX_SECONDS.
_SEQUENCE_GAP_MAX_S = 300


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


def extract_features(event: RawEvent) -> Dict[str, Any]:
    """Compute the full feature vector from a RawEvent."""
    duration = 0.0
    if event.end_ts and event.start_ts:
        duration = (event.end_ts - event.start_ts).total_seconds()

    avg_flow = _safe_float(event.flow_readings)
    peak_flow = max(event.flow_readings) if event.flow_readings else 0.0
    flow_variability = _safe_std(event.flow_readings)

    # Volume: avg_flow (L/min) × duration (min)
    volume_litres = avg_flow * (duration / 60.0) if duration > 0 else 0.0

    # True hydraulic resistance: ΔP / avg_Q
    # Only meaningful when flow is above noise floor and a pressure
    # transient was actually captured.
    resistance: Optional[float] = None
    if avg_flow >= 0.05 and event.has_pressure_transient and event.pressure_delta_psi > 0:
        resistance = event.pressure_delta_psi / avg_flow

    # Resistance curve shape — uses corrected ΔP/Q formula
    shape = _classify_resistance_shape(
        event.pressure_readings,
        event.flow_readings,
        event.pre_event_pressure_psi,
    )

    # Time features
    ts = event.start_ts
    hour = ts.hour
    dow = ts.weekday()
    hour_radians = 2 * math.pi * hour / 24
    duration_log = math.log(duration + 1)

    return {
        # Identity
        "id": str(uuid.uuid5(uuid.NAMESPACE_OID,
                              f"{event.circuit}/{event.start_ts.isoformat()}")),
        "circuit": event.circuit,
        "start_ts": event.start_ts.isoformat(),
        "end_ts": event.end_ts.isoformat() if event.end_ts else None,

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
        "propagation_delay_seconds": (
            round(event.propagation_delay_seconds, 2)
            if event.propagation_delay_seconds is not None else None
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
    }


class FeatureExtractor:
    """
    Consumes RawEvent objects from the queue and stores
    extracted features in SQLite.
    """

    def __init__(self, event_queue: asyncio.Queue,
                 db_conn: sqlite3.Connection, alert_manager=None):
        self._queue = event_queue
        self._db = db_conn
        self._alert_manager = alert_manager
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

    async def _process(self, event: RawEvent) -> None:
        if not event.complete:
            return

        features = extract_features(event)

        try:
            from .database import insert_event, update_hourly_volume
            insert_event(self._db, features)

            if event.start_ts and features.get("volume_litres", 0) > 0:
                hour_ts = event.start_ts.replace(
                    minute=0, second=0, microsecond=0
                ).isoformat()
                update_hourly_volume(
                    self._db,
                    event.circuit,
                    hour_ts,
                    features["volume_litres"],
                )

            if not features.get("excluded_from_training"):
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
                    ev_start   = datetime.fromisoformat(start_ts)
                    prev_end   = datetime.fromisoformat(
                        prev["end_ts"].replace("Z", "+00:00"))
                    if ev_start.tzinfo is None:
                        from datetime import timezone
                        ev_start = ev_start.replace(tzinfo=timezone.utc)
                    if prev_end.tzinfo is None:
                        from datetime import timezone
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

        # 2. Cluster matching
        cluster_id_result = None
        match_confidence  = None
        match_level       = None
        if self.cluster_engine:
            try:
                event_row = self._db.execute(
                    "SELECT * FROM events WHERE id = ?", (event_id,)
                ).fetchone()
                if event_row:
                    cluster_id_result, match_confidence, match_level = \
                        self.cluster_engine.match_and_learn(
                            dict(event_row), circuit)
            except Exception as e:
                log.error("[%s] cluster matching failed: %s", circuit, e,
                          exc_info=True)

        # 3. Write results back to the event row
        self._db.execute(
            """UPDATE events SET
                 cluster_id              = ?,
                 match_confidence        = ?,
                 match_level             = ?,
                 seconds_since_prev_event = ?,
                 prev_cluster_id         = ?
               WHERE id = ?""",
            (cluster_id_result, match_confidence, match_level,
             seconds_since_prev, prev_cluster_id, event_id)
        )
        self._db.commit()
