"""
Historical event importer.

Reconstructs water usage events from HA sensor history and stores any
that are missing from the addon database. Fills gaps caused by addon
restarts, HA recorder downtime, or the initial setup period before the
addon was installed.

Event detection strategy
------------------------
Primary: flow_pulse_onset ON/OFF transitions
  - HA records every binary-sensor transition (event-driven, not polled)
  - Short gaps between ON periods (< MERGE_GAP_SECONDS=15s) are bridged
    to handle slow-flow sensor flicker (~2.6s gap at 0.86 L/min)

Secondary: flow_rate > MIN_FLOW_LPM sustained readings
  - Fills in when flow_pulse_onset history has gaps (HA restart, etc.)
  - Consecutive above-threshold 1Hz readings with < MERGE_GAP_SECONDS gap

Both sets of detected periods are merged and deduplicated.

Pressure data
-------------
Tries pressure_history_sensor (pressure_main, 2Hz, 1.375s smoothing)
first — available after the firmware change that removed entity_category:
diagnostic.  Falls back to pressure_avg_sensor (pressure_main_avg, 1Hz,
25s smoothing) if the history sensor entity isn't available.

Historical events are always flagged start_trigger='flow'. If a clear
pressure drop is detected from the 1Hz or 2Hz recorded data, the event
is additionally flagged has_pressure_transient=True with the measured
delta. Without the 40Hz fast sensor, transient *shape* is unavailable
for fixture fingerprinting, but duration / flow / pressure delta are
accurate and suitable for volume accounting and coarse clustering.

Duplicate prevention
--------------------
Before queuing any reconstructed event, checks whether an event with
start_ts within ±DUPLICATE_WINDOW_SECONDS already exists. Safe to run
multiple times over the same window.

Scheduling
----------
  Startup backfill — runs once at addon start, covering from the most
    recent event in the DB back to at most MAX_BACKFILL_DAYS ago (HA
    recorder default retention = 10 days).

  Periodic catch-up — runs every CHECK_INTERVAL_MINUTES, covering the
    window since last_check_ts stored in the import_state table.

  Manual import — callable from the settings UI with an arbitrary
    date range; returns count of events imported.
"""
from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import AddonConfig, CircuitConfig
from .event_detector import RawEvent
from .database import (
    get_import_state, update_import_state,
    get_last_event_ts, event_exists_near,
)

log = logging.getLogger(__name__)


def _is_numeric(value: Any) -> bool:
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def _clamp_flow(v: float) -> float:
    """Match firmware v3.5 clamping: reject non-finite or out-of-range flow values."""
    if not math.isfinite(v) or v > 200.0 or (0 < v < 0.01):
        return 0.0
    return v


def _clamp_pressure(v: float) -> float:
    """Reject clearly invalid pressure readings (negative or implausibly large)."""
    if not math.isfinite(v) or v < 0.0 or v > 500.0:
        return 0.0
    return v


def _parse_ts(ts_value: Any) -> Optional[datetime]:
    if ts_value is None:
        return None
    if isinstance(ts_value, datetime):
        return ts_value if ts_value.tzinfo else ts_value.replace(tzinfo=timezone.utc)
    try:
        s = str(ts_value).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


class HistoricalImporter:
    """
    Reconstructs events from HA history and queues missing ones for
    feature extraction and DB insertion.
    """

    MAX_BACKFILL_DAYS: int = 10
    CHECK_INTERVAL_MINUTES: int = 30
    MERGE_GAP_SECONDS: int = 15       # bridge flow_pulse_onset gaps shorter than this
    MIN_DURATION_SECONDS: float = 3.0
    DUPLICATE_WINDOW_SECONDS: int = 30
    MIN_FLOW_LPM: float = 0.05
    PRE_PRESSURE_WINDOW_SECONDS: int = 30   # look-back for baseline pressure
    MIN_PRESSURE_DROP_PSI: float = 0.8      # min drop to flag has_pressure_transient

    def __init__(
        self,
        cfg: AddonConfig,
        db: sqlite3.Connection,
        ha_client: Any,
        event_queue: asyncio.Queue,
    ) -> None:
        self._cfg = cfg
        self._db = db
        self._ha = ha_client
        self._event_queue = event_queue
        self._running = False

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        self._running = True

        # Startup backfill — run before the first periodic sleep
        try:
            await self._backfill()
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error("Historical importer startup backfill failed: %s", e,
                      exc_info=True)

        # Periodic catch-up loop
        while self._running:
            try:
                await asyncio.sleep(self.CHECK_INTERVAL_MINUTES * 60)
                await self._catch_up()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("Historical importer periodic check failed: %s", e,
                          exc_info=True)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------ #
    # Public API (settings UI / setup wizard)                              #
    # ------------------------------------------------------------------ #

    async def import_range(
        self,
        circuit: str,
        start: datetime,
        end: datetime,
    ) -> int:
        """
        Import events for one circuit over an arbitrary date range.
        Returns count of events imported.
        Called from the settings UI manual import trigger.
        """
        cfg = self._cfg.get_circuit(circuit)
        if not cfg or not self._circuit_has_sensors(cfg):
            log.warning("[%s] import_range: circuit not configured", circuit)
            return 0
        n, _ = await self._import_range(cfg, start, end)
        return n

    async def import_all_circuits_range(
        self,
        start: datetime,
        end: datetime,
    ) -> int:
        """Import for all circuits over a date range. Returns total count."""
        total = 0
        for cfg in self._cfg.circuits:
            if self._circuit_has_sensors(cfg):
                n, _ = await self._import_range(cfg, start, end)
                total += n
        return total

    # ------------------------------------------------------------------ #
    # Scheduled operations                                                 #
    # ------------------------------------------------------------------ #

    async def _backfill(self) -> None:
        """
        On startup: import from the last recorded event (or MAX_BACKFILL_DAYS)
        through now for every configured circuit.
        """
        now = datetime.now(timezone.utc)
        for cfg in self._cfg.circuits:
            if not self._circuit_has_sensors(cfg):
                continue
            last_ts = get_last_event_ts(self._db, cfg.circuit)
            if last_ts:
                try:
                    start = datetime.fromisoformat(
                        last_ts.replace("Z", "+00:00")
                    )
                    # Overlap by 5 min to catch events that straddled the boundary
                    start = start - timedelta(minutes=5)
                except ValueError:
                    start = now - timedelta(days=self.MAX_BACKFILL_DAYS)
            else:
                start = now - timedelta(days=self.MAX_BACKFILL_DAYS)

            # Respect any import_state checkpoint — e.g. stamped at setup time
            # when the user chose to skip historical import.  Clamp so we never
            # reach before that cutoff, even across restarts.
            state = get_import_state(self._db, cfg.circuit)
            cutoff_ts = state.get("last_check_ts") if state else None
            if cutoff_ts:
                try:
                    cutoff = datetime.fromisoformat(cutoff_ts.replace("Z", "+00:00"))
                    if start < cutoff:
                        log.info("[%s] backfill clamped to import_state checkpoint %s",
                                 cfg.circuit, cutoff_ts)
                        start = cutoff
                except ValueError:
                    pass

            log.info("[%s] backfill: importing %s → now",
                     cfg.circuit, start.isoformat())
            # Chunk into 1-day windows so each WS response stays small enough
            # to fit within the WebSocket max_size limit.
            total = 0
            window_start = start
            while window_start < now:
                window_end = min(window_start + timedelta(days=1), now)
                n, _ = await self._import_range(cfg, window_start, window_end)
                total += n
                window_start = window_end
            if total:
                log.info("[%s] backfill: imported %d event(s)", cfg.circuit, total)

    async def _catch_up(self) -> None:
        """
        Periodic: import any events missed since last_check_ts.
        """
        now = datetime.now(timezone.utc)
        for cfg in self._cfg.circuits:
            if not self._circuit_has_sensors(cfg):
                continue
            state = get_import_state(self._db, cfg.circuit)
            last = state.get("last_check_ts")
            if last:
                try:
                    start = datetime.fromisoformat(last.replace("Z", "+00:00"))
                    start = start - timedelta(minutes=2)   # small overlap
                except ValueError:
                    start = now - timedelta(hours=2)
            else:
                start = now - timedelta(hours=2)

            n, retry_from = await self._import_range(cfg, start, now)
            # If any events were dropped due to a full queue, use the earliest
            # dropped timestamp as the checkpoint so the next catch-up covers
            # from that point rather than from now - 2 min (which might miss it).
            checkpoint = retry_from.isoformat() if retry_from else now.isoformat()
            update_import_state(self._db, cfg.circuit, checkpoint, n)
            if n:
                log.info("[%s] catch-up: imported %d new event(s)",
                         cfg.circuit, n)

    # ------------------------------------------------------------------ #
    # Core import logic                                                    #
    # ------------------------------------------------------------------ #

    async def _import_range(
        self,
        cfg: CircuitConfig,
        start: datetime,
        end: datetime,
    ) -> Tuple[int, Optional[datetime]]:
        """
        Fetch HA history for [start, end] and import any missing events.
        Returns (count_queued, retry_from) where retry_from is the earliest
        dropped event start if any events were lost to QueueFull — the caller
        should use it as the next catch-up checkpoint so those events are retried.
        """
        # Choose the best available pressure sensor for history
        pressure_entity = cfg.pressure_history_sensor or cfg.pressure_avg_sensor
        if not pressure_entity:
            log.debug("[%s] no pressure sensor for history — flow only",
                      cfg.circuit)

        entities_to_fetch = [
            e for e in [
                cfg.flow_onset_sensor,
                cfg.flow_sensor,
                pressure_entity,
                cfg.volume_sensor,
            ] if e
        ]
        if not cfg.flow_onset_sensor and not cfg.flow_sensor:
            log.warning("[%s] no flow entities — cannot import history",
                        cfg.circuit)
            return 0, None

        # Single WS request/connection for all entities in this window.
        try:
            histories = await self._ha.get_history_batch(entities_to_fetch, start, end)
        except Exception as exc:
            log.warning("[%s] history batch fetch failed: %s", cfg.circuit, exc)
            return 0, None

        onset_hist     = histories.get(cfg.flow_onset_sensor, [])
        flow_rate_hist = histories.get(cfg.flow_sensor, [])
        pressure_hist  = histories.get(pressure_entity, []) if pressure_entity else []
        volume_hist    = histories.get(cfg.volume_sensor, []) if cfg.volume_sensor else []

        # Resolve volume sensor unit from live state — history is fetched with
        # no_attributes=True so attributes are stripped from volume_hist entries.
        vol_unit = ""
        if cfg.volume_sensor:
            try:
                vs = await self._ha.get_state(cfg.volume_sensor)
                vol_unit = (vs.get("attributes") or {}).get("unit_of_measurement", "") if vs else ""
            except Exception:
                pass

        # Detect flow periods
        periods = self._find_flow_periods(onset_hist, flow_rate_hist, query_end=end)
        if not periods:
            return 0, None

        log.debug("[%s] found %d candidate period(s) in history window",
                  cfg.circuit, len(periods))

        imported = 0
        retry_from: Optional[datetime] = None
        for period_start, period_end in periods:
            duration = (period_end - period_start).total_seconds()
            if duration < self.MIN_DURATION_SECONDS:
                continue

            # Skip if already in DB
            if event_exists_near(
                self._db, cfg.circuit,
                period_start.isoformat(),
                self.DUPLICATE_WINDOW_SECONDS,
            ):
                continue

            raw = self._reconstruct_event(
                cfg.circuit, period_start, period_end,
                flow_rate_hist, pressure_hist, volume_hist,
                using_avg_pressure=(pressure_entity == cfg.pressure_avg_sensor),
                vol_unit=vol_unit,
            )
            if raw is None:
                continue

            # put_nowait raises QueueFull immediately rather than blocking
            # forever — a blocked await here would stall the entire import
            # loop (and the event loop) with no log and no way to recover.
            try:
                self._event_queue.put_nowait(raw)
            except asyncio.QueueFull:
                # Track the earliest dropped start so the next catch-up cycle
                # can cover from that point, not just now - 2 min.
                if retry_from is None or period_start < retry_from:
                    retry_from = period_start
                log.warning(
                    "[%s] event queue full — historical event dropped "
                    "(start=%s); will retry on next catch-up cycle",
                    cfg.circuit,
                    period_start.strftime("%H:%M:%S"),
                )
                continue
            imported += 1
            log.debug(
                "[%s] queued historical event %s → %s (%.0fs, %.2f L/min avg)",
                cfg.circuit,
                period_start.strftime("%H:%M:%S"),
                period_end.strftime("%H:%M:%S"),
                duration,
                sum(raw.flow_readings) / max(len(raw.flow_readings), 1),
            )

        return imported, retry_from

    # ------------------------------------------------------------------ #
    # Period detection                                                     #
    # ------------------------------------------------------------------ #

    def _find_flow_periods(
        self,
        onset_hist: List[Dict],
        flow_rate_hist: List[Dict],
        query_end: Optional[datetime] = None,
    ) -> List[Tuple[datetime, datetime]]:
        """
        Merge flow_pulse_onset ON periods and flow_rate > threshold periods
        into a unified, gap-filled, deduplicated list.
        """
        onset_periods = self._onset_to_periods(onset_hist, query_end=query_end)
        rate_periods  = self._rate_to_periods(flow_rate_hist, query_end=query_end)

        all_periods = onset_periods + rate_periods
        if not all_periods:
            return []

        merged = _merge_periods(sorted(all_periods), self.MERGE_GAP_SECONDS)
        return [(s, e) for s, e in merged
                if (e - s).total_seconds() >= self.MIN_DURATION_SECONDS]

    def _onset_to_periods(
        self,
        history: List[Dict],
        query_end: Optional[datetime] = None,
    ) -> List[Tuple[datetime, datetime]]:
        """
        Extract ON periods from flow_pulse_onset binary sensor history.
        Handles pre-existing ON state at window start (state at first entry).

        query_end: if the sensor is still ON at the end of the history window,
        the period is closed at query_end (the original request end time) rather
        than at the last history entry's timestamp, preventing spurious
        zero-duration periods when the last entry IS the onset itself.
        """
        periods: List[Tuple[datetime, datetime]] = []
        current_start: Optional[datetime] = None

        for entry in history:
            state = str(entry.get("state", "")).lower()
            ts = _parse_ts(entry.get("last_changed"))
            if ts is None:
                continue

            if state in ("on", "true", "1"):
                if current_start is None:
                    current_start = ts
            else:
                if current_start is not None:
                    periods.append((current_start, ts))
                    current_start = None

        # Still ON at end of window — close at query_end (most accurate) or
        # the last history entry if query_end wasn't provided.
        if current_start is not None:
            close_ts = query_end
            if close_ts is None and history:
                close_ts = _parse_ts(history[-1].get("last_changed"))
            if close_ts and close_ts > current_start:
                periods.append((current_start, close_ts))

        return periods

    def _rate_to_periods(
        self, history: List[Dict],
        query_end: Optional[datetime] = None,
    ) -> List[Tuple[datetime, datetime]]:
        """
        Extract periods where flow_rate >= MIN_FLOW_LPM from 1Hz history.
        """
        periods: List[Tuple[datetime, datetime]] = []
        current_start: Optional[datetime] = None
        last_ts: Optional[datetime] = None

        for entry in history:
            ts = _parse_ts(entry.get("last_changed"))
            if ts is None:
                continue
            try:
                rate = float(entry["state"])
            except (ValueError, TypeError, KeyError):
                rate = 0.0

            if rate >= self.MIN_FLOW_LPM:
                if current_start is None:
                    current_start = ts
            else:
                if current_start is not None:
                    # Use ts (the off-transition) not last_ts, consistent with
                    # _onset_to_periods which closes at the OFF timestamp.
                    periods.append((current_start, ts))
                    current_start = None

            last_ts = ts

        if current_start is not None:
            close_ts = query_end if query_end is not None else last_ts
            if close_ts is not None and close_ts > current_start:
                periods.append((current_start, close_ts))

        return periods

    # ------------------------------------------------------------------ #
    # Event reconstruction                                                 #
    # ------------------------------------------------------------------ #

    def _reconstruct_event(
        self,
        circuit: str,
        start: datetime,
        end: datetime,
        flow_rate_hist: List[Dict],
        pressure_hist: List[Dict],
        volume_hist: List[Dict],
        using_avg_pressure: bool = False,
        vol_unit: str = "",
    ) -> Optional[RawEvent]:
        """
        Build a RawEvent from slices of history data.
        Returns None if there is insufficient flow data.
        """
        # ── Flow readings during the period ───────────────────────────
        flow_readings = [
            _clamp_flow(float(e["state"]))
            for e in flow_rate_hist
            if _is_numeric(e.get("state"))
            and start <= (_parse_ts(e.get("last_changed")) or start) <= end
        ]
        if not flow_readings or max(flow_readings) < self.MIN_FLOW_LPM:
            return None

        # ── Pressure readings during the period ───────────────────────
        pressure_readings = [
            _clamp_pressure(float(e["state"]))
            for e in pressure_hist
            if _is_numeric(e.get("state"))
            and start <= (_parse_ts(e.get("last_changed")) or start) <= end
        ]

        # ── Pre-event pressure baseline (look-back window) ────────────
        pre_start = start - timedelta(seconds=self.PRE_PRESSURE_WINDOW_SECONDS)
        pre_readings = [
            _clamp_pressure(float(e["state"]))
            for e in pressure_hist
            if _is_numeric(e.get("state"))
            and pre_start <= (_parse_ts(e.get("last_changed")) or pre_start) <= start
        ]
        if pre_readings:
            pre_event_pressure = statistics.mean(pre_readings)
        elif pressure_readings:
            # Fallback: use first few readings of the event as approximate baseline
            pre_event_pressure = statistics.mean(pressure_readings[:3])
        else:
            pre_event_pressure = 0.0

        min_pressure = min(pressure_readings) if pressure_readings else pre_event_pressure
        pressure_delta = max(0.0, pre_event_pressure - min_pressure)

        # When using the 25s-averaged sensor, dampen the drop threshold —
        # the heavy smoothing will have muted the true delta significantly.
        effective_threshold = (
            self.MIN_PRESSURE_DROP_PSI * 0.3
            if using_avg_pressure
            else self.MIN_PRESSURE_DROP_PSI
        )
        has_transient = (
            bool(pressure_readings)
            and pressure_delta >= effective_threshold
        )

        # ── Volume from firmware integration sensor ────────────────────
        # Prefer the cumulative sensor delta over avg_flow × duration to avoid
        # downsampling errors in long events with fill-pause-fill patterns.
        volume_litres_measured: Optional[float] = None
        volume_in_period = [
            float(e["state"])
            for e in volume_hist
            if _is_numeric(e.get("state"))
            and math.isfinite(float(e["state"]))
            and start <= (_parse_ts(e.get("last_changed")) or start) <= end
        ]
        if len(volume_in_period) >= 2:
            delta = volume_in_period[-1] - volume_in_period[0]
            if 0 < delta < 10_000:   # sanity: reject resets and absurd values
                from .ha_client import vol_to_litres as _v2l
                volume_litres_measured = round(_v2l(delta, vol_unit), 3)

        return RawEvent(
            circuit=circuit,
            start_ts=start,
            end_ts=end,
            start_trigger="flow",
            other_valve_open=None,  # not available from history
            has_pressure_transient=has_transient,
            pre_event_pressure_psi=round(pre_event_pressure, 2),
            min_pressure_psi=round(min_pressure, 2),
            pressure_delta_psi=round(pressure_delta, 2),
            pressure_readings=pressure_readings,
            flow_onset_ts=start,
            propagation_delay_ms=0.0,
            flow_readings=flow_readings,
            volume_litres_measured=volume_litres_measured,
            complete=True,
        )

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _circuit_has_sensors(cfg: CircuitConfig) -> bool:
        """True if the circuit has at least flow sensors configured."""
        return bool(cfg.flow_onset_sensor or cfg.flow_sensor)


def _merge_periods(
    periods: List[Tuple[datetime, datetime]],
    gap_seconds: int,
) -> List[Tuple[datetime, datetime]]:
    """
    Merge adjacent or overlapping periods separated by <= gap_seconds.
    Input must be sorted by start time.
    """
    if not periods:
        return []
    merged = [periods[0]]
    for start, end in periods[1:]:
        prev_start, prev_end = merged[-1]
        gap = (start - prev_end).total_seconds()
        if gap <= gap_seconds:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged
