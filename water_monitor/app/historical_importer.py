"""
Historical event importer.

Reconstructs water usage events from HA sensor history and stores any
that are missing from the addon database. Fills gaps caused by addon
restarts, HA recorder downtime, or the initial setup period before the
addon was installed.

Event detection strategy
------------------------
(The derivation itself lives in ``importer_periods`` — this class keeps thin
forwarding methods. See that module's docstring for the seam's rules.)

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
import bisect
import logging
import math
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import AddonConfig, CircuitConfig
from .event_detector import RawEvent, CircuitEventDetector as _CED
# CONTAINMENT_FRACTION / contained_fraction went with _split_period_around_rows
# into importer_periods (plan unit 7.4) and are no longer imported here — the
# threshold is pinned to overlap_guard's at the call site's own module.
from .overlap_guard import OVERLAP_NEGLIGIBLE_L, VOLUME_COVERAGE_FRACTION
from . import importer_periods as _periods
from .importer_periods import _is_gap_marker, _parse_ts
from .database import (
    run_db, contained_stored_rows,
    get_import_state, update_import_state,
    get_last_event_ts, find_overlapping_event,
    mark_event_irrigation_cross_talk,
)
from .feature_extractor import (
    _detect_irrigation_cross_talk, _XTALK_IRR_MIN_FLOW_LPM, _XTALK_IRR_MAX_VOLUME_L,
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


# ── Irrigation cross-talk reconcile helpers (module-level so they are pure +
#    unit-testable without an importer instance) ──────────────────────────────
def _numeric_series(entries, clamp) -> List[Tuple[datetime, float]]:
    """Parse an HA history list ([{state,last_changed}]) into a sorted, clamped
    (ts, value) series, dropping unparseable / non-numeric rows."""
    out: List[Tuple[datetime, float]] = []
    for e in entries or []:
        ts = _parse_ts(e.get("last_changed"))
        if ts is None or not _is_numeric(e.get("state")):
            continue
        out.append((ts, clamp(float(e["state"]))))
    out.sort(key=lambda x: x[0])
    return out


def _merge_active_intervals(
    series: List[Tuple[datetime, float]],
    threshold: float,
    gap_s: float,
    min_block_s: float,
) -> List[Tuple[datetime, datetime]]:
    """Merged [start, end] intervals where ``value > threshold``, bridging gaps
    shorter than ``gap_s`` and keeping only blocks lasting >= ``min_block_s``."""
    raw: List[List[datetime]] = []
    cur: Optional[List[datetime]] = None
    for ts, val in series:
        if val > threshold:
            if cur is None:
                cur = [ts, ts]
            else:
                cur[1] = ts
        elif cur is not None:
            raw.append(cur)
            cur = None
    if cur is not None:
        raw.append(cur)
    merged: List[List[datetime]] = []
    for s, e in raw:
        if merged and (s - merged[-1][1]).total_seconds() < gap_s:
            merged[-1][1] = e
        else:
            merged.append([s, e])
    return [(s, e) for s, e in merged
            if (e - s).total_seconds() >= min_block_s]


def _overlaps_any(s: datetime, e: datetime,
                  intervals: List[Tuple[datetime, datetime]]) -> bool:
    return any(s <= ie and e >= is_ for is_, ie in intervals)


def _containing_interval(
    s: datetime, e: datetime, intervals: List[Tuple[datetime, datetime]],
) -> Optional[Tuple[datetime, datetime]]:
    for is_, ie in intervals:
        if s <= ie and e >= is_:
            return (is_, ie)
    return None


def _pressure_swing(series: List[Tuple[datetime, float]],
                    t0: datetime, t1: datetime) -> float:
    """max − min of the values whose timestamp falls in [t0, t1]; 0.0 if none.

    ``series`` is sorted (``_numeric_series``), so the window is located with
    bisect — a day of 4 Hz pressure logging is easily 10⁵ samples and this runs
    twice per candidate event on the event loop; a linear scan per call stalls
    the UI during backfills."""
    lo = bisect.bisect_left(series, t0, key=lambda x: x[0])
    hi = bisect.bisect_right(series, t1, lo=lo, key=lambda x: x[0])
    if lo >= hi:
        return 0.0
    vals = [v for _, v in series[lo:hi]]
    return max(vals) - min(vals)


class HistoricalImporter:
    """
    Reconstructs events from HA history and queues missing ones for
    feature extraction and DB insertion.
    """

    MAX_BACKFILL_DAYS: int = 10
    CHECK_INTERVAL_MINUTES: int = 30
    MERGE_GAP_SECONDS: int = 15       # bridge flow_pulse_onset gaps shorter than this
    MIN_DURATION_SECONDS: float = 3.0
    # dev56 — containment rule (docs/PIPELINE.md "Duplicate gate"). A reconstructed
    # period that CONTAINS rows already stored is either already on the record
    # (stored rows hold >= CONTAINED_ROWS_COVERAGE of its water → dropped) or is
    # split AROUND those rows so only the water nobody recorded becomes an event.
    # Runs before find_overlapping_event, whose dev55 coverage block stays behind
    # it as defence in depth. Remainders below the negligible floor, without a
    # flow-rate fragment inside them (a pressure sag tail is not a draw), or beyond
    # the per-period cap are dropped and logged — never written on top of a row.
    CONTAINED_ROWS_COVERAGE: float = VOLUME_COVERAGE_FRACTION
    CONTAINED_REMAINDER_MIN_L: float = OVERLAP_NEGLIGIBLE_L
    MAX_REMAINDERS_PER_PERIOD: int = 10     # = reprocess._SPLIT_MAX_PERIODS (a test pins it)
    MIN_FLOW_LPM: float = _CED.MIN_FLOW_LPM
    MIN_EVENT_VOLUME_L: float = _CED.MIN_EVENT_VOLUME_L
    PRE_PRESSURE_WINDOW_SECONDS: int = 30   # look-back for baseline pressure
    MIN_PRESSURE_DROP_PSI: float = 0.8      # min drop to flag has_pressure_transient

    # ── Irrigation zone-switch cross-talk reconciliation (2026-06-28) ──────────
    # Bounded by HA recorder retention: the reconciler needs the IRRIGATION
    # pressure history to compute the swing ratio, so it can only reach as far back
    # as MAX_BACKFILL_DAYS (older addon events can't be re-fetched). Self-healing &
    # idempotent — candidates exclude already-flagged rows, so the rolling overlap
    # never double-acts and a clobbered verdict re-applies on the next pass.
    XTALK_RECONCILE_GAP_S: float = 120.0          # merge irrigation flow bursts < this
    XTALK_RECONCILE_MIN_BLOCK_S: float = 300.0    # a real irrigation run is >= 5 min
    XTALK_RECONCILE_PRESSURE_PAD_S: float = 2.0   # widen the swing window each side
    XTALK_RECONCILE_MARGIN_HOURS: float = 6.0     # watermark lag (>= one irrigation block)

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
    # dev.39 — anti-noise bridge gate. The pressure-dip source exists to BRIDGE the
    # gaps between real flow bursts (pulsed irrigation). But a LONG dip envelope that
    # contains almost NO flow just stitches unrelated trivial blips into one bogus
    # multi-minute event (observed: two ~0.3 L blips 20 min apart fused into a 20 min
    # event). So a dip period only earns its bridge when (a) it is long AND (b) the
    # real flow volume inside it is trivial → drop it; the underlying flow fragments
    # still import on their own (subject to MIN_DURATION). Leak-safe: a real draw or a
    # running-toilet leak's fills carry real volume and never trip this; we never drop
    # flow, only an empty pressure envelope. Short dips and dips with real flow are
    # untouched.
    PRESSURE_DIP_BRIDGE_LONG_SPAN_S: float = 300.0   # "long" envelope (5 min)
    PRESSURE_DIP_BRIDGE_MIN_VOLUME_L: float = 2.0    # real flow needed to earn a long bridge
    # dev.50 — the volume gate above only drops a bridge whose flow is TRIVIAL, so a
    # dip carrying real water bridges without limit: on a pump-held line the dip never
    # recovers to its frozen baseline between draws, so _pressure_to_periods emits ONE
    # envelope (observed: 182 min spanning ~14 min of flow and ~99 L — far over the 2 L
    # gate) and _merge_periods welds every draw into a single event that no reprocess
    # could split. A bridge exists to span the ~40 s inter-burst gaps named in this
    # class's docstring, not a 90-minute idle, so cap the gap it may span. 300 s sits
    # well above those bursts and above the 92 s maximum internal washer/dishwasher gap
    # the dev.38 sawtooth study measured. Same leak/volume reasoning as the gate above:
    # only EMPTY span is removed, never flow — and only when the history PROVES the
    # flow stopped (see _flow_stopped_across; a dark sensor is not an idle).
    PRESSURE_DIP_BRIDGE_MAX_GAP_S: float = 300.0     # bridges inter-burst gaps, not idles

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

        # Irrigation cross-talk reconcile — first pass doubles as the backfill
        # (separate try so an import failure above doesn't skip it, and vice versa).
        try:
            await self._reconcile_cross_talk()
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error("Irrigation cross-talk backfill failed: %s", e, exc_info=True)

        # Periodic catch-up loop
        while self._running:
            try:
                await asyncio.sleep(self.CHECK_INTERVAL_MINUTES * 60)
                await self._catch_up()
                await self._reconcile_cross_talk()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("Historical importer periodic check failed: %s", e,
                          exc_info=True)
                if "locked" in str(e).lower():
                    from .database import note_locked_write
                    note_locked_write("historical_importer.catch_up")

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
        Called from the settings UI manual import trigger and the atomic
        reprocess. RAISES on a history-fetch failure — so a fetch failure after
        the reprocess delete is detectable and the deleted events restored (the
        old default silently returned 0, indistinguishable from "nothing to
        import").
        """
        cfg = self._cfg.get_circuit(circuit)
        if not cfg or not self._circuit_has_sensors(cfg):
            log.warning("[%s] import_range: circuit not configured", circuit)
            return 0
        # retry_from is intentionally dropped: this is a caller-driven range
        # import with no checkpoint of its own, and the caller only wants the
        # count. Anything left unstored is re-covered by the next catch-up.
        n, _retry_from = await self._import_range(cfg, start, end)
        return n

    async def dry_run_reconstruction(
        self,
        circuit: str,
        start: datetime,
        end: datetime,
    ) -> dict:
        """Dry-run: what would the importer reconstruct over [start, end], WITHOUT
        deleting or storing anything. The guarded auto-split (dev.38/41) uses this to
        decide whether ONE stored event is really several distinct draws — and, dev.41,
        whether the window's history can be TRUSTED to reproduce the stored water.

        Returns ``{"periods": [(start_dt, end_dt), ...],
                   "period_volumes_l": [<litres per period, same order>],
                   "flow_volume_l": <integrated flow over the window>,
                   "fetch_failed": bool,   # transient — retry next pass
                   "gappy": bool}``        # 'unavailable'/'unknown' samples in window
        dev52 — ``period_volumes_l`` integrates the SAME history slice per period, so
        the reprocess probe can weigh a period a kept event would block against that
        event's stored water without re-deriving anything from stored rows.
        On an unconfigured circuit or a fetch failure the periods are empty and
        ``fetch_failed`` is set (fail-safe — a dry-run that can't see history must
        never trigger a split)."""
        cfg = self._cfg.get_circuit(circuit)
        pressure_entity = cfg and (cfg.pressure_history_sensor or cfg.pressure_avg_sensor)
        if (not cfg or not self._circuit_has_sensors(cfg)
                or not (cfg.flow_onset_sensor or cfg.flow_sensor)):
            return {"periods": [], "period_volumes_l": [], "flow_volume_l": 0.0,
                    "fetch_failed": True, "gappy": False}
        entities = [e for e in (cfg.flow_onset_sensor, cfg.flow_sensor,
                                pressure_entity) if e]
        try:
            histories = await self._ha.get_history_batch(entities, start, end)
        except Exception as exc:
            log.warning("[%s] auto-split dry-run history fetch failed: %s", circuit, exc)
            return {"periods": [], "period_volumes_l": [], "flow_volume_l": 0.0,
                    "fetch_failed": True, "gappy": False}
        onset_hist     = histories.get(cfg.flow_onset_sensor, [])
        flow_rate_hist = histories.get(cfg.flow_sensor, [])
        pressure_hist  = histories.get(pressure_entity, []) if pressure_entity else []
        using_avg_pressure = (pressure_entity == cfg.pressure_avg_sensor)
        periods = self._find_flow_periods(
            onset_hist, flow_rate_hist, query_end=end,
            pressure_hist=pressure_hist, using_avg_pressure=using_avg_pressure,
        ) or []
        # dev.41: recorder-gap markers ('unavailable'/'unknown' at HA restarts, sensor
        # dropouts) mean the window's history is INCOMPLETE — a reconstruction from it
        # would read the gap as flow-off and shrink/split away real recorded water.
        gappy = any(_is_gap_marker(e) for h in (onset_hist, flow_rate_hist,
                                                pressure_hist) for e in h)
        return {"periods": periods,
                "period_volumes_l": [
                    self._flow_volume_in_period(flow_rate_hist, ps, pe)
                    for ps, pe in periods],
                "flow_volume_l": self._flow_volume_in_period(
                    flow_rate_hist, start, end),
                "fetch_failed": False, "gappy": gappy}

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
            last_ts = await run_db(get_last_event_ts, self._db, cfg.circuit)
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
            state = await run_db(get_import_state, self._db, cfg.circuit)
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
                retry_from = None
                try:
                    n, retry_from = await self._import_range(
                        cfg, window_start, window_end)
                except Exception as exc:
                    # Best-effort per chunk (the backfill keeps no checkpoint to
                    # hold); the periodic catch-up / next restart re-covers it.
                    log.warning("[%s] backfill chunk %s..%s fetch failed: %s",
                                cfg.circuit, window_start.isoformat(),
                                window_end.isoformat(), exc)
                    n = 0
                total += n
                # A draw crossing a chunk boundary was being stored as its TAIL
                # only: the period builders never emit a still-active period, so
                # this chunk stored nothing for it, and the next chunk saw it
                # already running and opened the event at the boundary. That
                # truncated stub is exactly the shape the 3x overlap heal was
                # invented to work around.
                #
                # `retry_from` is the earliest point whose events were NOT
                # stored — it is set only when an event was dropped to a full
                # queue (which `continue`s before `imported += 1`) or when a
                # period was still active at the window end (never emitted). So
                # rewinding to it re-fetches ONLY things that were never
                # written, and cannot duplicate. That is what makes this safe
                # here even though the backfill, unlike _catch_up, has no
                # persistent checkpoint to hold.
                #
                # The `> window_start` test is the loop-progress guard: an event
                # active from the very start of a chunk would otherwise rewind
                # to where we already are and spin forever. That case means a
                # draw longer than the chunk itself, and is left to a later run.
                if retry_from is not None and retry_from > window_start:
                    log.info(
                        "[%s] backfill: rewinding chunk boundary %s → %s "
                        "(an event was still running there; advancing would "
                        "store only its tail)",
                        cfg.circuit, window_end.isoformat(),
                        retry_from.isoformat())
                    window_start = retry_from
                else:
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
            state = await run_db(get_import_state, self._db, cfg.circuit)
            last = state.get("last_check_ts")
            if last:
                try:
                    start = datetime.fromisoformat(last.replace("Z", "+00:00"))
                    start = start - timedelta(minutes=2)   # small overlap
                except ValueError:
                    start = now - timedelta(hours=2)
            else:
                start = now - timedelta(hours=2)

            try:
                n, retry_from = await self._import_range(cfg, start, now)
            except Exception as exc:
                # Checkpoint deliberately NOT advanced: the unfetched window is
                # re-covered next tick (a swallowed failure used to advance it
                # to `now`, permanently skipping the outage window).
                log.warning("[%s] catch-up fetch failed (checkpoint held): %s",
                            cfg.circuit, exc)
                continue
            # retry_from holds the checkpoint back when either (a) events were
            # dropped to a full queue or (b) a flow period was still active at
            # `now` (an event longer than the catch-up interval). Without (b) the
            # checkpoint would advance past the in-progress event's start and only
            # a later startup backfill could recover it. When set, the next
            # catch-up re-covers from that point; otherwise advance to now.
            checkpoint = retry_from.isoformat() if retry_from else now.isoformat()
            await run_db(update_import_state, self._db, cfg.circuit,
                         checkpoint, n)
            if n:
                log.info("[%s] catch-up: imported %d new event(s)",
                         cfg.circuit, n)

    # ------------------------------------------------------------------ #
    # Irrigation zone-switch cross-talk reconciliation                     #
    # ------------------------------------------------------------------ #

    def _xtalk_watermark_key(self, main_circuit: str) -> str:
        """Synthetic import_state key for the per-main-circuit reconcile watermark
        (kept distinct from the real import checkpoint)."""
        return f"__xtalk::{main_circuit}"

    def _irrigation_circuit(self) -> Optional[CircuitConfig]:
        """The zone circuit whose pressure transients alias onto the main meter."""
        for cfg in self._cfg.circuits:
            if cfg.is_zone_circuit and self._circuit_has_sensors(cfg):
                if cfg.pressure_history_sensor or cfg.pressure_avg_sensor:
                    return cfg
        return None

    async def _reconcile_cross_talk(self) -> None:
        """Irrigation cross-talk reconciliation — ONE watermark-driven pass that is
        both the backfill and the periodic catch-up (dev.41: they were two
        near-identical methods whose watermark advanced even when every history
        fetch failed, permanently skipping outage windows).

        Per eligible main circuit: reconcile [watermark − margin, now] — or the full
        HA-retention window when no watermark exists yet — in ≤1-day chunks (a
        full-day 4 Hz pressure pull would overflow the WS frame). The watermark
        advances ONLY past chunks whose history fetch succeeded
        (``_reconcile_irrigation_cross_talk`` returns None on a failed fetch); a
        failure stops the pass so the remaining window is retried next tick, never
        recorded as done. Idempotent: candidates exclude already-flagged rows, so
        the margin overlap re-scans harmlessly."""
        irr = self._irrigation_circuit()
        if irr is None:
            return
        now = datetime.now(timezone.utc)
        margin = timedelta(hours=self.XTALK_RECONCILE_MARGIN_HOURS)
        for cfg in self._cfg.circuits:
            if cfg.is_zone_circuit or not self._circuit_has_sensors(cfg):
                continue
            if not (cfg.pressure_history_sensor or cfg.pressure_avg_sensor):
                continue
            key = self._xtalk_watermark_key(cfg.circuit)
            state = await run_db(get_import_state, self._db, key)
            wm = _parse_ts(state.get("last_check_ts")) if state else None
            start = (wm - margin) if wm else (
                now - timedelta(days=self.MAX_BACKFILL_DAYS))
            total = 0
            window_start = start
            while window_start < now:
                window_end = min(window_start + timedelta(days=1), now)
                n = await self._reconcile_irrigation_cross_talk(
                    cfg, irr, window_start, window_end)
                if n is None:
                    log.warning("[%s] cross-talk reconcile: fetch failed for "
                                "%s..%s — will retry from here next pass",
                                cfg.circuit, window_start.isoformat(),
                                window_end.isoformat())
                    break
                total += n
                # Watermark = end of the last SUCCESSFULLY fetched chunk, lagged by
                # the margin so the trailing edge (events still being written) is
                # re-scanned next pass. Never past `now − margin`.
                wm_out = max(window_end - margin, start)
                await run_db(update_import_state, self._db, key,
                             wm_out.isoformat(), total)
                window_start = window_end
            if total:
                log.info("[%s] cross-talk reconcile: flagged %d zone-switch "
                         "event(s)", cfg.circuit, total)

    def _xtalk_candidates_sync(self, circuit: str, start_iso: str,
                               end_iso: str):
        """dev46 (46a) — cross-talk reconcile candidates, one hop."""
        return self._db.execute(
            "SELECT id, start_ts, end_ts, duration_seconds, volume_litres "
            "FROM events "
            "WHERE circuit = ? AND COALESCE(is_cross_talk,0) = 0 "
            "  AND COALESCE(user_classified,0) = 0 "
            "  AND COALESCE(excluded_from_training,0) = 0 "
            "  AND COALESCE(volume_litres,0) <= ? "
            "  AND start_ts >= ? AND start_ts < ?",
            (circuit, _XTALK_IRR_MAX_VOLUME_L, start_iso, end_iso),
        ).fetchall()

    def _recompute_days_sync(self, circuit: str, days) -> None:
        """dev46 (46a) — one daily-summary recompute per affected day."""
        from .database import compute_daily_summary
        for day in days:
            compute_daily_summary(self._db, circuit, day)
        self._db.commit()

    async def _reconcile_irrigation_cross_talk(
        self,
        main_cfg: CircuitConfig,
        irr_cfg: CircuitConfig,
        start: datetime,
        end: datetime,
    ) -> Optional[int]:
        """Flag main events in [start, end] that are irrigation zone-switch cross-talk.

        Single coherent activity definition (merged irrigation-flow intervals) is used
        for BOTH candidate selection and the detector's ``irrigation_active``; PiΔ and
        PmΔ are computed from one history batch over the SAME padded window per event.
        Returns the count flagged — or **None on a history-fetch failure**, so the
        caller can hold the watermark and retry (a swallowed failure used to be
        recorded as "reconciled", permanently skipping outage windows).

        Cheapest checks first (this runs every catch-up tick, mostly finding
        nothing): the local candidate SQL, then the small irrigation-flow series,
        and only when both hit does it pull the two heavy 4 Hz pressure series."""
        main_press_e = main_cfg.pressure_history_sensor or main_cfg.pressure_avg_sensor
        irr_press_e = irr_cfg.pressure_history_sensor or irr_cfg.pressure_avg_sensor
        irr_flow_e = irr_cfg.flow_sensor
        if not (main_press_e and irr_press_e and irr_flow_e):
            return 0

        # dev46 (46a): the candidate read happens BEFORE the HA history
        # fetches below — its own hop.
        rows = await run_db(self._xtalk_candidates_sync, main_cfg.circuit,
                            start.isoformat(), end.isoformat())
        if not rows:
            return 0

        try:
            hist = await self._ha.get_history_batch([irr_flow_e], start, end)
        except Exception as exc:
            log.warning("cross-talk reconcile irrigation-flow fetch failed: %s", exc)
            return None
        irr_flow = _numeric_series(hist.get(irr_flow_e, []), _clamp_flow)
        intervals = _merge_active_intervals(
            irr_flow, _XTALK_IRR_MIN_FLOW_LPM,
            self.XTALK_RECONCILE_GAP_S, self.XTALK_RECONCILE_MIN_BLOCK_S)
        if not intervals:
            return 0

        cands = []
        for ev in rows:
            s = _parse_ts(ev["start_ts"])
            if s is None:
                continue
            e = _parse_ts(ev["end_ts"]) or s
            if _overlaps_any(s, e, intervals):
                cands.append((ev, s, e))
        if not cands:
            return 0

        try:
            hist = await self._ha.get_history_batch(
                [main_press_e, irr_press_e], start, end)
        except Exception as exc:
            log.warning("cross-talk reconcile pressure fetch failed: %s", exc)
            return None
        main_press = _numeric_series(hist.get(main_press_e, []), _clamp_pressure)
        irr_press = _numeric_series(hist.get(irr_press_e, []), _clamp_pressure)

        pad = timedelta(seconds=self.XTALK_RECONCILE_PRESSURE_PAD_S)
        flagged = 0
        affected_days: set = set()
        for ev, s, e in cands:
            pmd = _pressure_swing(main_press, s - pad, e + pad)
            pid = _pressure_swing(irr_press, s - pad, e + pad)
            if not _detect_irrigation_cross_talk(
                    ev["volume_litres"], ev["duration_seconds"],
                    pmd, pid, irrigation_active=True):
                continue
            iv = _containing_interval(s, e, intervals)
            if await run_db(
                    mark_event_irrigation_cross_talk,
                    self._db, ev["id"], main_cfg.circuit,
                    reconciled_at=datetime.now(timezone.utc).isoformat(),
                    interval_start=iv[0].isoformat() if iv else None,
                    interval_end=iv[1].isoformat() if iv else None,
                    main_delta_psi=round(pmd, 3), other_delta_psi=round(pid, 3),
                    ratio=round(pid / pmd, 3) if pmd > 0 else None,
                    recompute_summary=False):
                flagged += 1
                from .database import local_day_of
                day = local_day_of(ev["start_ts"])
                if day:
                    affected_days.add(day)
        # One daily-summary recompute per affected DAY, not per event (the
        # 2026-08-13 backfill flagged 185 events over 12 days — 185 full-table
        # scans where 12 would do). dev46 (46a): one hop for the whole set.
        if affected_days:
            await run_db(self._recompute_days_sync, main_cfg.circuit,
                         sorted(affected_days))
        return flagged

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
        RAISES on a history-fetch failure (see the fetch note below) — callers
        that loop over circuits/chunks catch per iteration and hold their
        checkpoint so the window is retried, never silently skipped.
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

        # Single WS request/connection for all entities in this window. A fetch
        # failure RAISES — one error contract for every caller. The old
        # `strict` flag made silent-swallow (`return 0, None`) the default,
        # which is exactly the bug class the atomic reprocess fixed: "imported
        # 0" after a failed fetch reads as "nothing to import" and, worse, let
        # the catch-up advance its checkpoint past an unfetched window. Loop
        # callers catch per circuit/chunk and hold their checkpoints.
        histories = await self._ha.get_history_batch(entities_to_fetch, start, end)

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

        # dev56 — a period that contains rows already stored is not new water.
        # Drop it when they account for it, split it around them when they do
        # not; the per-period gate below then sees only genuinely new spans.
        periods = await self._apply_containment_rule(
            cfg, periods, flow_rate_hist, query_end=end)
        if not periods:
            return 0, active_since

        imported = 0
        retry_from: Optional[datetime] = None
        for period_start, period_end in periods:
            duration = (period_end - period_start).total_seconds()
            if duration < self.MIN_DURATION_SECONDS:
                continue

            # Skip if a meaningfully-overlapping event already exists.
            # Meaningful = overlap >= 30 s, OR >= 10 s and >= 80% of the shorter
            # event (the comment said 50% for a long time; the code has always
            # used 80% — see find_overlapping_event's docstring).
            # This catches importer catch-up duplicates whose start_ts drifted
            # by minutes — well beyond the old ±30 s point-match. dev55: it also
            # now refuses a long reconstruction whose span existing unlabeled
            # rows already account for.
            existing = await run_db(
                find_overlapping_event,
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
    # dev56 — containment rule                                            #
    # ------------------------------------------------------------------ #

    async def _apply_containment_rule(
        self, cfg, periods: List[Tuple[datetime, datetime]],
        flow_rate_hist: List[Dict], query_end: Optional[datetime] = None,
    ) -> List[Tuple[datetime, datetime]]:
        """Expand ``periods`` into the spans that are genuinely new water.

        For each period: the stored closed rows intersecting it are fetched (one
        DB hop), and ``_split_period_around_rows`` decides keep / drop / split.
        Only ``_import_range`` calls this — ``dry_run_reconstruction`` describes
        history and must NOT be split (the reprocess probe compares it against
        the stored rows itself)."""
        fragments = self._rate_to_periods(flow_rate_hist, query_end=query_end)
        out: List[Tuple[datetime, datetime]] = []
        for ps, pe in periods:
            if (pe - ps).total_seconds() < self.MIN_DURATION_SECONDS:
                out.append((ps, pe))            # the gate below drops it as today
                continue
            rows = await run_db(contained_stored_rows, self._db, cfg.circuit,
                                ps.isoformat(), pe.isoformat())
            action, subs, info = self._split_period_around_rows(
                (ps, pe), rows, flow_rate_hist, fragments)
            dur = int((pe - ps).total_seconds())
            if action == "keep":
                out.append((ps, pe))
            elif action == "drop":
                log.info("[%s] importer: dropping %ds reconstruction %s..%s — %d stored "
                         "event(s) already account for %.2f of %.2f L",
                         cfg.circuit, dur, ps.strftime("%H:%M:%S"), pe.strftime("%H:%M:%S"),
                         info["n_rows"], info["stored_l"], info["period_l"])
            else:
                log.info("[%s] importer: split %ds reconstruction %s..%s around %d stored "
                         "event(s) (%.2f of %.2f L on record) → %d remainder(s) kept "
                         "(%.2f L), %d dropped (<%.0fs, <%.2f L, no flow, or over the cap)",
                         cfg.circuit, dur, ps.strftime("%H:%M:%S"), pe.strftime("%H:%M:%S"),
                         info["n_rows"], info["stored_l"], info["period_l"], len(subs),
                         info["kept_l"], info["dropped"], self.MIN_DURATION_SECONDS,
                         self.CONTAINED_REMAINDER_MIN_L)
                out.extend(subs)
        return out

    # ------------------------------------------------------------------ #
    # Period derivation  (bodies live in importer_periods -- see 7.4)     #
    # ------------------------------------------------------------------ #
    # These forward to the pure module. ``self`` is passed only as the
    # threshold provider, so an instance- or class-level override of any
    # constant below still reaches the calculation.
    #
    # NOTE FOR TESTS: the pure functions call each other through
    # ``importer_periods``' own globals, NOT back through ``self``. Rebinding
    # e.g. ``imp._pressure_to_periods`` therefore intercepts nothing --
    # ``monkeypatch.setattr(importer_periods, "_pressure_to_periods", ...)``.

    def _split_period_around_rows(
        self, period: Tuple[datetime, datetime], rows: List[Dict],
        flow_rate_hist: List[Dict],
        flow_fragments: List[Tuple[datetime, datetime]],
    ) -> Tuple[str, List[Tuple[datetime, datetime]], Dict]:
        return _periods._split_period_around_rows(
            self, period, rows, flow_rate_hist, flow_fragments)

    def _trailing_active_start(
        self,
        onset_hist: List[Dict],
        flow_rate_hist: List[Dict],
    ) -> Optional[datetime]:
        return _periods._trailing_active_start(self, onset_hist, flow_rate_hist)

    def _find_flow_periods(
        self,
        onset_hist: List[Dict],
        flow_rate_hist: List[Dict],
        query_end: Optional[datetime] = None,
        pressure_hist: Optional[List[Dict]] = None,
        using_avg_pressure: bool = False,
    ) -> List[Tuple[datetime, datetime]]:
        return _periods._find_flow_periods(
            self, onset_hist, flow_rate_hist, query_end=query_end,
            pressure_hist=pressure_hist, using_avg_pressure=using_avg_pressure)

    def _flow_stopped_across(
        self,
        flow_rate_hist: List[Dict],
        onset_hist: List[Dict],
        gap_start: datetime,
        gap_end: datetime,
    ) -> bool:
        return _periods._flow_stopped_across(
            self, flow_rate_hist, onset_hist, gap_start, gap_end)

    def _split_dip_on_idle_gaps(
        self,
        dip_start: datetime,
        dip_end: datetime,
        overlap: List[Tuple[datetime, datetime]],
        flow_rate_hist: List[Dict],
        onset_hist: List[Dict],
    ) -> List[Tuple[datetime, datetime]]:
        return _periods._split_dip_on_idle_gaps(
            self, dip_start, dip_end, overlap, flow_rate_hist, onset_hist)

    def _flow_volume_in_period(
        self, flow_rate_hist: List[Dict], start: datetime, end: datetime,
    ) -> float:
        return _periods._flow_volume_in_period(self, flow_rate_hist, start, end)

    def _onset_to_periods(
        self,
        history: List[Dict],
        query_end: Optional[datetime] = None,
    ) -> List[Tuple[datetime, datetime]]:
        return _periods._onset_to_periods(self, history, query_end=query_end)

    def _rate_to_periods(
        self, history: List[Dict],
        query_end: Optional[datetime] = None,
    ) -> List[Tuple[datetime, datetime]]:
        return _periods._rate_to_periods(self, history, query_end=query_end)

    def _pressure_to_periods(
        self,
        history: List[Dict],
        query_end: Optional[datetime] = None,
        using_avg_pressure: bool = False,
    ) -> List[Tuple[datetime, datetime]]:
        return _periods._pressure_to_periods(
            self, history, query_end=query_end,
            using_avg_pressure=using_avg_pressure)

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
        # (single source of truth).
        #
        # ENDPOINT-GAP GUARD — the importer used to consume only the litres and
        # discard a_ts/b_ts, which firmware_volume_delta returns *specifically*
        # so the caller can apply this. The delta is measured between the FIRST
        # and LAST recorder samples inside the window, so any water that moved
        # before the first sample or after the last is simply not in it. That is
        # a silent UNDER-COUNT, and it wins: feature_extractor prefers
        # volume_litres_measured over the flow integral. Declining here falls
        # back to the integral, which is the honest answer when the recorder did
        # not bracket the event.
        #
        # Known residual: the tolerance is absolute (120 s), and a sample can
        # only land inside the window, so the guard cannot fire on an event
        # shorter than the tolerance. A 30 s draw whose first sample arrives 25 s
        # in still yields a delta covering ~5 s. Bounding the long-event case is
        # a strict improvement on bounding nothing; a coverage-fraction rule
        # would need a threshold nobody has measured yet.
        from .recorder_reconcile import ENDPOINT_TOL_S, firmware_volume_delta
        _vd = firmware_volume_delta(volume_hist, start, end, vol_unit)
        volume_litres_measured: Optional[float] = None
        if _vd:
            _litres, _a_ts, _b_ts = _vd
            _lead = (_a_ts - start).total_seconds()
            _lag = (end - _b_ts).total_seconds()
            if _lead > ENDPOINT_TOL_S or _lag > ENDPOINT_TOL_S:
                log.debug(
                    "[%s] firmware volume delta declined for %s–%s: recorder "
                    "samples do not bracket the event (%.0f s lead, %.0f s lag, "
                    "tolerance %.0f s) — using the flow integral instead.",
                    circuit, start.isoformat(), end.isoformat(),
                    _lead, _lag, ENDPOINT_TOL_S)
            else:
                volume_litres_measured = _litres

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


# ── Moved to importer_periods (plan unit 7.4) ─────────────────────────────────
# ``_parse_ts`` / ``_is_gap_marker`` are imported eagerly above because THIS
# module's own functions still call them (a module ``__getattr__`` is not
# consulted for a global-name lookup inside a function body -- that would be a
# NameError). The two below have no caller left here, only importers of this
# module, so they resolve lazily.
#
# PEP 562, deliberately, not a bottom-of-file ``from .importer_periods import``:
# an eager re-export line is the shape that broke the feature_extractor split --
# it closes the loop the moment the other module is imported first. The direction
# here is already one-way, and this keeps it that way by construction.
#
# The ``raise AttributeError`` fall-through is load-bearing: returning None would
# make ``hasattr(historical_importer, <anything>)`` answer True.
_MOVED_TO_PERIODS = ("_GAP_MARKER_STATES", "_merge_periods")


def __getattr__(name: str):
    if name in _MOVED_TO_PERIODS:
        from . import importer_periods as _m
        return getattr(_m, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
