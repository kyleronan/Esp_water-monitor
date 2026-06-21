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
Before queuing any reconstructed event, checks whether a meaningfully
overlapping event already exists (overlap >= 30 s, or >= 10 s and >= 80 %
of the shorter event). Safe to run multiple times over the same window.

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
from .event_detector import RawEvent, CircuitEventDetector as _CED
from .database import (
    get_import_state, update_import_state,
    get_last_event_ts, find_overlapping_event,
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
    MIN_FLOW_LPM: float = _CED.MIN_FLOW_LPM
    MIN_EVENT_VOLUME_L: float = _CED.MIN_EVENT_VOLUME_L
    PRE_PRESSURE_WINDOW_SECONDS: int = 30   # look-back for baseline pressure
    MIN_PRESSURE_DROP_PSI: float = 0.8      # min drop to flag has_pressure_transient

    # Pressure-dip-as-period-source constants.
    # The state machine below emits a (start, end) period for each contiguous
    # sustained dip — these are merged with flow periods by _merge_periods so
    # pulsed-flow events that produce a clean pressure envelope still register.
    PRESSURE_DIP_PERIOD_PSI: float = 1.0          # threshold for exact-pressure sensor
    PRESSURE_DIP_OPEN_DURATION_S: float = 5.0     # sustain before opening a period
    PRESSURE_DIP_CLOSE_DURATION_S: float = 5.0    # sustain before closing a period
    PRESSURE_DIP_BASELINE_WINDOW_S: float = 30.0  # idle look-back for rolling baseline
    PRESSURE_DIP_AVG_MIN_THRESHOLD_PSI: float = 0.5   # floor when using avg sensor
    PRESSURE_DIP_AVG_OPEN_DURATION_S: float = 10.0    # longer sustain for avg sensor
    # Minimum idle history required before an IDLE→CANDIDATE transition is allowed.
    # Guards against a manual import that starts mid-dip computing the baseline
    # from only one or two pre-dip samples and triggering a spurious period.
    PRESSURE_DIP_MIN_BASELINE_SAMPLES: int = 3
    PRESSURE_DIP_MIN_BASELINE_SPAN_S: float = 5.0

    def __init__(
        self,
        cfg: AddonConfig,
        db: sqlite3.Connection,
        ha_client: Any,
        event_queue: asyncio.Queue,
        orchestrator: Any = None,
    ) -> None:
        self._cfg = cfg
        self._db = db
        self._ha = ha_client
        self._event_queue = event_queue
        # Optional back-reference to the orchestrator so _import_range can
        # consult the live EventDetector and skip periods that overlap a
        # currently-active event (which would otherwise be reconstructed as
        # a partial stub and then block the real event via overlap rules).
        self._orch = orchestrator
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

    async def count_candidate_periods(
        self,
        circuit: str,
        start: datetime,
        end: datetime,
    ) -> list:
        """Dry-run: return the candidate flow PERIODS the importer would reconstruct over
        [start, end] WITHOUT deleting or storing anything. The guarded auto-split (dev.38)
        uses the period count to decide whether ONE stored event is really several distinct
        draws (2..K periods) vs a single draw (1) or chatter (> K). Returns a list of
        ``(start_dt, end_dt)`` tuples; empty on an unconfigured circuit or a fetch failure
        (fail-safe — a dry-run that can't see history must never trigger a split)."""
        cfg = self._cfg.get_circuit(circuit)
        if not cfg or not self._circuit_has_sensors(cfg):
            return []
        pressure_entity = cfg.pressure_history_sensor or cfg.pressure_avg_sensor
        entities = [e for e in (cfg.flow_onset_sensor, cfg.flow_sensor,
                                pressure_entity) if e]
        if not cfg.flow_onset_sensor and not cfg.flow_sensor:
            return []
        try:
            histories = await self._ha.get_history_batch(entities, start, end)
        except Exception as exc:
            log.warning("[%s] auto-split dry-run history fetch failed: %s", circuit, exc)
            return []
        onset_hist     = histories.get(cfg.flow_onset_sensor, [])
        flow_rate_hist = histories.get(cfg.flow_sensor, [])
        pressure_hist  = histories.get(pressure_entity, []) if pressure_entity else []
        using_avg_pressure = (pressure_entity == cfg.pressure_avg_sensor)
        return self._find_flow_periods(
            onset_hist, flow_rate_hist, query_end=end,
            pressure_hist=pressure_hist, using_avg_pressure=using_avg_pressure,
        ) or []

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
            # retry_from holds the checkpoint back when either (a) events were
            # dropped to a full queue or (b) a flow period was still active at
            # `now` (an event longer than the catch-up interval). Without (b) the
            # checkpoint would advance past the in-progress event's start and only
            # a later startup backfill could recover it. When set, the next
            # catch-up re-covers from that point; otherwise advance to now.
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

        # Checkpoint watermark for the periodic catch-up: the start of any flow
        # period still ON at the end of this window (onset still ON, or flow_rate
        # still >= MIN_FLOW_LPM with no OFF transition after). _find_flow_periods
        # correctly refuses to flush a still-active period, but the catch-up loop
        # advances last_check_ts to `now` regardless — so an event LONGER than the
        # catch-up interval would have its start march behind the checkpoint and
        # could then only be recovered by a much-later startup backfill (observed:
        # a 133-min irrigation run recovered 4 days late). Returning this start as
        # retry_from holds the checkpoint at the event's start until it actually
        # ends, so the next catch-up after it closes reconstructs the full period.
        # Flow signals only (not the pressure-dip state machine) so a stuck/shifted
        # pressure baseline can never pin the checkpoint indefinitely.
        active_since = self._trailing_active_start(onset_hist, flow_rate_hist)

        # Detect flow periods — pressure_hist is passed so the state machine can
        # emit a dip-envelope period that bridges pulsed-flow bursts too far apart
        # for MERGE_GAP_SECONDS (e.g. fridge dispenser with ~40 s inter-burst gap).
        using_avg_pressure = (pressure_entity == cfg.pressure_avg_sensor)
        periods = self._find_flow_periods(
            onset_hist, flow_rate_hist, query_end=end,
            pressure_hist=pressure_hist,
            using_avg_pressure=using_avg_pressure,
        )
        if not periods:
            return 0, active_since

        # Belt-and-braces guard: if the live EventDetector currently has an
        # active event on this circuit, drop any candidate period that overlaps
        # it. Fix 1 (drop trailing still-active emissions in _onset/_rate) is
        # the primary defence; this catches the rare race where the sensor
        # briefly flickered OFF mid-event and the helper closed a period
        # honestly inside the event window.
        ev_detector = self._orch.event_detector if self._orch is not None else None
        active = ev_detector.get_active_event(cfg.circuit) if ev_detector else None
        if active is not None and active.start_ts is not None:
            active_start = active.start_ts
            active_end = datetime.now(timezone.utc)
            before = len(periods)
            periods = [(s, e) for (s, e) in periods
                       if e <= active_start or s >= active_end]
            if len(periods) < before:
                log.info(
                    "[%s] importer: dropped %d candidate period(s) overlapping "
                    "live event (start=%s)",
                    cfg.circuit, before - len(periods), active_start.isoformat(),
                )
            if not periods:
                return 0, active_since

        log.debug("[%s] found %d candidate period(s) in history window",
                  cfg.circuit, len(periods))

        imported = 0
        retry_from: Optional[datetime] = None
        for period_start, period_end in periods:
            duration = (period_end - period_start).total_seconds()
            if duration < self.MIN_DURATION_SECONDS:
                continue

            # Skip if a meaningfully-overlapping event already exists.
            # Meaningful = overlap >= 30 s OR >= 50% of the shorter event.
            # This catches importer catch-up duplicates whose start_ts drifted
            # by minutes — well beyond the old ±30 s point-match.
            existing = find_overlapping_event(
                self._db, cfg.circuit,
                period_start.isoformat(),
                period_end.isoformat(),
            )
            if existing is not None:
                suffix = ""
                if existing.get("user_fixture_type"):
                    suffix = f" (user-labeled '{existing['user_fixture_type']}')"
                elif existing.get("fixture_id") and existing.get("user_locked"):
                    suffix = f" (user-locked fixture id={existing['fixture_id']})"
                log.info(
                    "[%s] skipping reconstruction %s..%s: overlaps existing "
                    "event id=%s %s..%s%s",
                    cfg.circuit,
                    period_start.strftime("%H:%M:%S"),
                    period_end.strftime("%H:%M:%S"),
                    existing["id"],
                    existing["start_ts"],
                    existing["end_ts"],
                    suffix,
                )
                continue

            raw = self._reconstruct_event(
                cfg.circuit, period_start, period_end,
                flow_rate_hist, pressure_hist, volume_hist,
                using_avg_pressure=using_avg_pressure,
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

        # Hold the catch-up checkpoint at the earliest of (a) any event dropped to
        # a full queue and (b) a flow period still active at window end — whichever
        # is earlier must be re-covered next cycle.
        if active_since is not None:
            retry_from = (active_since if retry_from is None
                          else min(retry_from, active_since))
        return imported, retry_from

    # ------------------------------------------------------------------ #
    # Period detection                                                     #
    # ------------------------------------------------------------------ #

    def _trailing_active_start(
        self,
        onset_hist: List[Dict],
        flow_rate_hist: List[Dict],
    ) -> Optional[datetime]:
        """Start of a flow period still ON at the end of the fetched history.

        Mirrors the still-active detection in _onset_to_periods / _rate_to_periods
        (a trailing ON run with no closing OFF transition) but reports the run's
        START instead of dropping it, so _import_range can hold the catch-up
        checkpoint back. Returns the earliest such start across the onset and
        flow-rate signals, or None if flow had clearly stopped by window end.
        """
        def _trailing_open(history: List[Dict], is_on) -> Optional[datetime]:
            open_start: Optional[datetime] = None
            for entry in history:
                ts = _parse_ts(entry.get("last_changed"))
                if ts is None:
                    continue
                if is_on(entry):
                    if open_start is None:
                        open_start = ts
                else:
                    open_start = None
            return open_start

        def _onset_on(entry: Dict) -> bool:
            return str(entry.get("state", "")).lower() in ("on", "true", "1")

        def _rate_on(entry: Dict) -> bool:
            try:
                return float(entry["state"]) >= self.MIN_FLOW_LPM
            except (ValueError, TypeError, KeyError):
                return False

        starts = [s for s in (_trailing_open(onset_hist, _onset_on),
                              _trailing_open(flow_rate_hist, _rate_on))
                  if s is not None]
        return min(starts) if starts else None

    def _find_flow_periods(
        self,
        onset_hist: List[Dict],
        flow_rate_hist: List[Dict],
        query_end: Optional[datetime] = None,
        pressure_hist: Optional[List[Dict]] = None,
        using_avg_pressure: bool = False,
    ) -> List[Tuple[datetime, datetime]]:
        """
        Merge flow_pulse_onset ON periods, flow_rate > threshold periods, and
        (optionally) sustained pressure-dip periods into a unified, gap-filled,
        deduplicated list.

        The pressure-dip source bridges pulsed-flow events whose bursts are too
        far apart for MERGE_GAP_SECONDS — the continuous dip envelope covers the
        full event window and _merge_periods fuses it with the flow fragments.
        """
        onset_periods    = self._onset_to_periods(onset_hist, query_end=query_end)
        rate_periods     = self._rate_to_periods(flow_rate_hist, query_end=query_end)
        pressure_periods = self._pressure_to_periods(
            pressure_hist or [], query_end=query_end,
            using_avg_pressure=using_avg_pressure,
        )

        all_periods = onset_periods + rate_periods + pressure_periods
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

        # Still ON at end of window — DO NOT flush. Emitting a period here
        # would insert a partial event that then blocks the real event from
        # being stored when the live detector finishes it (overlap rule fires
        # at ratio = 1.0 since the partial is fully contained). The next
        # importer run will see the full closed period and import it
        # correctly, or the live detector will store the complete event.
        if current_start is not None:
            log.info(
                "[importer] skipping still-active onset period "
                "(start=%s, query_end=%s) — deferring to next run / live detector",
                current_start.isoformat(),
                query_end.isoformat() if query_end is not None else "?",
            )

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
        # (no last_ts tracking — see the comment in the loop body
        # below; off-transition `ts` is used directly.)

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
            # (last_ts tracking removed — the off-transition `ts` is used
            # directly above, per the same convention as _onset_to_periods.)

        # Same rationale as _onset_to_periods: do NOT flush still-active
        # flow-rate periods at query_end. The live detector / next importer
        # run will handle them once flow actually drops.
        if current_start is not None:
            log.info(
                "[importer] skipping still-active flow-rate period "
                "(start=%s, query_end=%s) — deferring to next run / live detector",
                current_start.isoformat(),
                query_end.isoformat() if query_end is not None else "?",
            )

        return periods

    def _pressure_to_periods(
        self,
        history: List[Dict],
        query_end: Optional[datetime] = None,
        using_avg_pressure: bool = False,
    ) -> List[Tuple[datetime, datetime]]:
        """Emit (start, end) periods for each sustained pressure dip.

        State machine over time-ordered pressure samples:
          IDLE      — rolling baseline from the last PRESSURE_DIP_BASELINE_WINDOW_S
          CANDIDATE — freeze baseline; require sustained dip before opening
          OPEN      — period active; watch for recovery
          RECOVERING — sustained recovery required before closing

        The frozen-baseline design prevents the baseline from drifting downward
        inside a real dip, which would cause the dip to look smaller than it is.
        """
        if not history:
            return []

        # Effective threshold and open-sustain depend on sensor quality.
        if using_avg_pressure:
            effective_thr  = max(self.PRESSURE_DIP_AVG_MIN_THRESHOLD_PSI,
                                 self.PRESSURE_DIP_PERIOD_PSI * 0.3)
            open_duration  = self.PRESSURE_DIP_AVG_OPEN_DURATION_S
        else:
            effective_thr  = self.PRESSURE_DIP_PERIOD_PSI
            open_duration  = self.PRESSURE_DIP_OPEN_DURATION_S
        close_duration = self.PRESSURE_DIP_CLOSE_DURATION_S

        # State
        STATE_IDLE       = 0
        STATE_CANDIDATE  = 1
        STATE_OPEN       = 2
        STATE_RECOVERING = 3

        state           = STATE_IDLE
        idle_window: List[Tuple[datetime, float]] = []  # (ts, psi) for rolling mean
        frozen_baseline : float = 0.0
        candidate_start : Optional[datetime] = None
        recovery_start  : Optional[datetime] = None
        periods: List[Tuple[datetime, datetime]] = []

        samples = []
        for entry in history:
            ts = _parse_ts(entry.get("last_changed"))
            if ts is None:
                continue
            try:
                psi = float(entry["state"])
            except (ValueError, TypeError, KeyError):
                continue
            if not math.isfinite(psi):
                continue
            samples.append((ts, psi))

        if not samples:
            return []

        last_ts = samples[-1][0]

        for ts, psi in samples:

            # ── IDLE: maintain rolling baseline ──────────────────────────
            if state == STATE_IDLE:
                cutoff = ts - timedelta(seconds=self.PRESSURE_DIP_BASELINE_WINDOW_S)
                idle_window = [(t, v) for t, v in idle_window if t >= cutoff]
                # Compute baseline from the PREVIOUS window (before this sample) so
                # a dip at exactly the threshold still triggers rather than diluting
                # the baseline and causing a missed detection.
                if idle_window:
                    baseline = sum(v for _, v in idle_window) / len(idle_window)
                    span_s = (
                        (idle_window[-1][0] - idle_window[0][0]).total_seconds()
                        if len(idle_window) > 1 else 0.0
                    )
                    if (
                        len(idle_window) >= self.PRESSURE_DIP_MIN_BASELINE_SAMPLES
                        and span_s >= self.PRESSURE_DIP_MIN_BASELINE_SPAN_S
                        and psi <= baseline - effective_thr
                    ):
                        # Freeze baseline and start the candidate clock;
                        # do NOT add the dip sample to the idle window.
                        frozen_baseline = baseline
                        candidate_start = ts
                        state = STATE_CANDIDATE
                        continue
                idle_window.append((ts, psi))

            # ── CANDIDATE: check sustain (do NOT update idle_window) ─────
            elif state == STATE_CANDIDATE:
                assert candidate_start is not None
                if psi > frozen_baseline - effective_thr:
                    # Cancelled before sustained — return to IDLE
                    # Re-feed this sample into the idle window
                    cutoff = ts - timedelta(seconds=self.PRESSURE_DIP_BASELINE_WINDOW_S)
                    idle_window = [(t, v) for t, v in idle_window if t >= cutoff]
                    idle_window.append((ts, psi))
                    state = STATE_IDLE
                elif (ts - candidate_start).total_seconds() >= open_duration:
                    state = STATE_OPEN

            # ── OPEN: watch for start of recovery ────────────────────────
            elif state == STATE_OPEN:
                recovery_line = frozen_baseline - effective_thr * 0.5
                if psi >= recovery_line:
                    recovery_start = ts
                    state = STATE_RECOVERING

            # ── RECOVERING: require sustained recovery before closing ─────
            elif state == STATE_RECOVERING:
                assert recovery_start is not None
                assert candidate_start is not None
                recovery_line = frozen_baseline - effective_thr * 0.5
                if psi < recovery_line:
                    # Pressure dipped again — cancel recovery, stay OPEN
                    recovery_start = None
                    state = STATE_OPEN
                elif (ts - recovery_start).total_seconds() >= close_duration:
                    periods.append((candidate_start, recovery_start))
                    # Reset to IDLE; re-feed this sample as start of idle window
                    idle_window = [(ts, psi)]
                    state = STATE_IDLE
                    candidate_start = None
                    recovery_start  = None
                    frozen_baseline = 0.0

        # End-of-history flush — only flush STATE_RECOVERING, NOT STATE_OPEN.
        # If the dip is still open at query_end the event is likely still in
        # progress; emitting it now would insert a partial event that blocks
        # the full one when the importer runs again after the event ends.
        if state == STATE_RECOVERING and candidate_start is not None:
            close_ts = recovery_start or last_ts
            if query_end is not None:
                close_ts = min(close_ts, query_end)
            if close_ts > candidate_start:
                periods.append((candidate_start, close_ts))

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
        # Build all raw flow entries (sorted) then resample to 1 Hz so that
        # FeatureExtractor sees a uniform time series rather than sparse
        # HA state-change events.  Last known rate before start is used as
        # the default so inter-sample gaps are correctly forward-filled.
        all_flow_entries = sorted(
            (
                (_parse_ts(e.get("last_changed")), _clamp_flow(float(e["state"])))
                for e in flow_rate_hist
                if _is_numeric(e.get("state"))
                and _parse_ts(e.get("last_changed")) is not None
            ),
            key=lambda x: x[0],
        )
        # Last known rate before start (forward-fill default)
        flow_before = [v for t, v in all_flow_entries if t < start]
        flow_default = flow_before[-1] if flow_before else 0.0

        # Entries within [start, end] for the time-weighted volume check
        flow_entries = [(t, v) for t, v in all_flow_entries if start <= t <= end]

        if not flow_entries or max(v for _, v in flow_entries) < self.MIN_FLOW_LPM:
            return None

        # Time-weighted average for volume and reject-by-avg gate.
        total_vol = 0.0
        for i, (ts, rate) in enumerate(flow_entries):
            next_ts = flow_entries[i + 1][0] if i + 1 < len(flow_entries) else end
            seg_min = max(0.0, (min(next_ts, end) - ts).total_seconds()) / 60.0
            total_vol += rate * seg_min
        event_min = (end - start).total_seconds() / 60.0
        if event_min <= 0 or (total_vol / event_min) < self.MIN_FLOW_LPM:
            return None

        # 1 Hz resampled flow readings (uniform time base for FeatureExtractor)
        flow_readings = _resample_step_function_1hz(
            [(t, v) for t, v in all_flow_entries],
            start, end, default=flow_default,
        )

        # ── Pressure readings during the period ───────────────────────
        # Resample pressure to the same 1 Hz grid so pressure_signature_json
        # is generated from a time-correct series, not sparse HA change events.
        all_pres_entries = sorted(
            (
                (_parse_ts(e.get("last_changed")), _clamp_pressure(float(e["state"])))
                for e in pressure_hist
                if _is_numeric(e.get("state"))
                and _parse_ts(e.get("last_changed")) is not None
            ),
            key=lambda x: x[0],
        )
        pres_before = [v for t, v in all_pres_entries if t < start]
        pres_default = pres_before[-1] if pres_before else 0.0
        pressure_readings = _resample_step_function_1hz(
            all_pres_entries, start, end, default=pres_default,
        )

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
        # downsampling errors in long events with fill-pause-fill patterns. The
        # cumulative-delta computation is shared with the §2 recorder reconcile
        # (single source of truth); the importer consumes only the litres.
        from .recorder_reconcile import firmware_volume_delta
        _vd = firmware_volume_delta(volume_hist, start, end, vol_unit)
        volume_litres_measured: Optional[float] = _vd[0] if _vd else None

        # Volume floor — mirrors CircuitEventDetector._end_event
        avg_flow = sum(flow_readings) / len(flow_readings) if flow_readings else 0.0
        volume_l = avg_flow * (end - start).total_seconds() / 60.0
        if volume_l < self.MIN_EVENT_VOLUME_L:
            return None

        # ── Propagation delay ─────────────────────────────────────────
        # Scan pressure samples strictly within [start, end] for the first
        # sample that crosses baseline - PROPAGATION_ONSET_PSI.  Return None
        # (rendered as "—" in the UI) when:
        #   • pressure history is absent
        #   • using_avg_pressure (25 s smoothing makes ms-precision meaningless)
        #   • no sample within the window crosses the threshold
        propagation_delay_ms: Optional[float] = None
        if pressure_hist and not using_avg_pressure and pre_event_pressure > 0:
            onset_threshold = pre_event_pressure - _CED.PROPAGATION_ONSET_PSI
            for entry in pressure_hist:
                ts = _parse_ts(entry.get("last_changed"))
                if ts is None or not (start <= ts <= end):
                    continue
                try:
                    psi = float(entry["state"])
                except (ValueError, TypeError, KeyError):
                    continue
                if psi <= onset_threshold:
                    propagation_delay_ms = round(
                        (ts - start).total_seconds() * 1000.0, 1
                    )
                    break

        # Timestamped flow samples for the volume TIME-INTEGRAL — identical code
        # path to the live detector (so volume + active-flow features match).
        # Pad with the pre-start flow and an end sample so the first/last
        # intervals integrate correctly.
        flow_samples = [(start, flow_default)]
        flow_samples += [(t, v) for t, v in all_flow_entries if start <= t <= end]
        if flow_samples[-1][0] < end:
            flow_samples.append((end, flow_samples[-1][1]))

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
            propagation_delay_ms=propagation_delay_ms,
            flow_readings=flow_readings,
            flow_samples=flow_samples,
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


def _resample_step_function_1hz(
    samples: List[Tuple[datetime, float]],
    start: datetime,
    end: datetime,
    default: float = 0.0,
) -> List[float]:
    """Resample a step-function history to 1 Hz over [start, end] (inclusive).

    HA records state-change events only, not a regular time series.  This
    function forward-fills the last known value at each integer second so
    that FeatureExtractor sees a uniform time series rather than sparse
    change-event indices.

    ``samples`` must be sorted by timestamp.  ``default`` is used before the
    first sample (e.g. the last known value before ``start``).

    Output length = int((end - start).total_seconds()) + 1.
    Returns [default] when end <= start.
    """
    total_s = int((end - start).total_seconds())
    if total_s <= 0:
        return [default]

    length = total_s + 1
    out: List[float] = []
    si = 0
    current = default

    for tick in range(length):
        t = start + timedelta(seconds=tick)
        # Advance through samples whose timestamp <= t (last one wins)
        while si < len(samples) and samples[si][0] <= t:
            current = samples[si][1]
            si += 1
        out.append(current)

    return out


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
