"""One-shot rising-pressure-corr backfill (dev14).

Historical events predate ``events.flow_pressure_corr``, and their stored
32-point signatures were VALIDATED as unusable for re-deriving it (the
pressure signature clamps drop-fraction to [0,1], erasing above-baseline
rises — 74% verdict agreement vs raw history). So this worker computes the
correlation the only trustworthy way: re-fetching each candidate event's flow
+ pressure history from the HA recorder, resampling both onto the importer's
1 Hz grid, and running the same ``_flow_pressure_correlation`` the live path
uses. Stored correlations then get the verdict applied by the same sync
repair pass (``reprocess_rising_pressure_phantoms``) that reconciles live
events — one verdict chokepoint, one ledger path (§2.5
``apply_effective_volume``).

Candidates are ONLY events the live detector could have flagged: short
(<= 120 s), small (0 < vol < 1 L), un-labelled, un-flagged, non-degraded,
carrying no artifact verdict, with no stored corr yet. Everything else is
out of scope forever (see the detector's frozen guards).

One-shot semantics: sweeps candidates oldest-first; when a full sweep
completes with zero HA fetch errors it stamps
``home_profile.rise_corr_backfill_done = 1`` and never runs again (events
whose history is gone/uncomputable stay counted — leak-safe default). A sweep
interrupted by fetch errors retries after a pause; already-stored corrs
don't match the candidate query, so re-sweeps only touch the remainder.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from .config import AddonConfig, DB_PATH

from .database import run_db

log = logging.getLogger(__name__)

_STARTUP_DELAY_S = 180        # let boot catch-up import / migrations settle first
_BATCH_SIZE = 25              # events per batch (one write-lock hop per batch)
_EVENT_PAUSE_S = 0.4          # pause between per-event HA fetches (recorder courtesy)
_BATCH_PAUSE_S = 5.0          # pause between batches
_ERROR_RETRY_S = 600          # re-sweep delay after a sweep that hit fetch errors
_FETCH_PAD_S = 15.0           # history pad before start (forward-fill baseline)


def _parse_event_ts(value: Optional[str]) -> Optional[datetime]:
    """Stored ISO event timestamp → aware UTC datetime (None on garbage)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


class RiseCorrBackfill:
    """One-shot supervised worker: fill flow_pressure_corr for historical
    candidate events from HA history, applying the rise verdict per batch."""

    def __init__(self, db: sqlite3.Connection, cfg: AddonConfig, ha=None,
                 db_path=DB_PATH):
        self._db = db
        self._cfg = cfg
        self._ha = ha
        self._db_path = db_path      # injectable for tests (run_isolated_write)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def _idle(self) -> None:
        """Park until shutdown — _supervise restarts a returned coroutine."""
        await self._stop.wait()

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        # NOTE: the orchestrator's _supervise re-invokes a coroutine that
        # returns normally — a finished one-shot must therefore PARK on the
        # stop event (self._idle) instead of returning, or it would hot-loop.
        if self._ha is None:
            await self._idle()
            return
        try:
            if await run_db(self._done):
                log.debug("rise-corr backfill already stamped done — idle")
                await self._idle()
                return
        except sqlite3.Error as e:
            log.warning("rise-corr backfill: cannot read done-stamp (%s) — "
                        "not running", e)
            await self._idle()
            return

        # Let the boot settle (catch-up import shares the HA recorder).
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=_STARTUP_DELAY_S)
            return                                    # stopped during the delay
        except asyncio.TimeoutError:
            pass

        while not self._stop.is_set():
            stored, errors, exhausted = await self._sweep()
            if self._stop.is_set():
                return
            if exhausted and errors == 0:
                # Clean full sweep: everything computable is stored + verdicts
                # applied. Whatever remains NULL is permanently uncomputable
                # (history pruned / no pressure signal) and stays counted.
                await self._apply_verdicts()          # final reconcile
                await run_db(self._stamp_done)
                log.info("rise-corr backfill COMPLETE — stamped done "
                         "(%d corr(s) stored this sweep)", stored)
                await self._idle()
                return
            # Fetch errors — retry the remainder after a pause.
            log.info("rise-corr backfill sweep incomplete (%d stored, %d fetch "
                     "error(s)) — retrying in %ds", stored, errors, _ERROR_RETRY_S)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_ERROR_RETRY_S)
                return
            except asyncio.TimeoutError:
                continue

    # ── one full sweep ───────────────────────────────────────────────────────

    async def _sweep(self) -> Tuple[int, int, bool]:
        """Walk all candidates oldest-first once.
        Returns (corrs_stored, fetch_errors, exhausted)."""
        stored_total = 0
        errors_total = 0
        for circuit_cfg in self._cfg.circuits:
            flow_entity = circuit_cfg.flow_sensor
            pressure_entity = (circuit_cfg.pressure_history_sensor
                               or circuit_cfg.pressure_avg_sensor)
            if not flow_entity or not pressure_entity:
                continue                    # no usable sensors → nothing to do
            watermark = ""                  # start_ts cursor within this sweep
            while not self._stop.is_set():
                rows = await run_db(self._candidates,
                                    circuit_cfg.circuit, watermark)
                if not rows:
                    break
                stored, errors = await self._process_batch(
                    circuit_cfg.circuit, rows, flow_entity, pressure_entity)
                stored_total += stored
                errors_total += errors
                if stored:
                    await self._apply_verdicts()
                watermark = rows[-1]["start_ts"]
                try:
                    await asyncio.wait_for(self._stop.wait(),
                                           timeout=_BATCH_PAUSE_S)
                    return stored_total, errors_total, False
                except asyncio.TimeoutError:
                    pass
        return stored_total, errors_total, not self._stop.is_set()

    def _candidates(self, circuit: str, after_start_ts: str) -> List[sqlite3.Row]:
        """Next batch of corr-less candidate events (live-detector scope only)."""
        from .feature_extractor import (_RISE_PHANTOM_MAX_DURATION_S,
                                        _RISE_PHANTOM_MAX_VOLUME_L)
        cur = self._db.execute(
            "SELECT id, start_ts, end_ts FROM events "
            "WHERE circuit = ? AND start_ts > ? "
            "  AND flow_pressure_corr IS NULL "
            "  AND duration_seconds <= ? "
            "  AND volume_litres > 0 AND volume_litres < ? "
            "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
            "  AND COALESCE(is_cross_talk, 0) = 0 "
            "  AND COALESCE(is_low_flow_dribble, 0) = 0 "
            "  AND COALESCE(degraded_supply, 0) = 0 "
            "  AND COALESCE(flagged, 0) = 0 "
            "  AND COALESCE(user_classified, 0) = 0 "
            "  AND COALESCE(user_ignored, 0) = 0 "
            "  AND (user_fixture_type IS NULL OR user_fixture_type = '') "
            "ORDER BY start_ts ASC LIMIT ?",
            (circuit, after_start_ts, _RISE_PHANTOM_MAX_DURATION_S,
             _RISE_PHANTOM_MAX_VOLUME_L, _BATCH_SIZE),
        )
        cur.row_factory = sqlite3.Row
        return cur.fetchall()

    async def _process_batch(self, circuit: str, rows: List[sqlite3.Row],
                             flow_entity: str, pressure_entity: str,
                             ) -> Tuple[int, int]:
        """Fetch + compute + store corr for one batch. Returns (stored, errors)."""
        from .database import run_isolated_write
        updates: List[Tuple[float, str]] = []
        errors = 0
        for r in rows:
            if self._stop.is_set():
                break
            corr, err = await self._corr_for_event(
                r["start_ts"], r["end_ts"], flow_entity, pressure_entity)
            if err:
                errors += 1
            elif corr is not None:
                updates.append((round(corr, 4), r["id"]))
            # else: fetch fine but uncomputable (no pressure variance / too
            # short) — permanently skipped, the event stays counted.
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_EVENT_PAUSE_S)
                break
            except asyncio.TimeoutError:
                pass

        stored = 0
        if updates:
            def _write(conn: sqlite3.Connection) -> int:
                n = 0
                for corr, eid in updates:
                    # IS NULL guard: never clobber a corr a concurrent ESP
                    # late-waveform upgrade (higher fidelity) already wrote.
                    cur = conn.execute(
                        "UPDATE events SET flow_pressure_corr = ? "
                        "WHERE id = ? AND flow_pressure_corr IS NULL",
                        (corr, eid))
                    n += cur.rowcount or 0
                conn.commit()
                return n
            stored = await run_isolated_write(self._db_path, _write)
            log.info("[%s] rise-corr backfill: stored %d/%d corr(s) this batch",
                     circuit, stored, len(rows))
        return stored, errors

    async def _corr_for_event(self, start_ts: str, end_ts: Optional[str],
                              flow_entity: str, pressure_entity: str,
                              ) -> Tuple[Optional[float], bool]:
        """Fetch one event's history and compute corr.
        Returns (corr_or_None, fetch_error)."""
        from .feature_extractor import _flow_pressure_correlation
        from .historical_importer import (_is_numeric, _parse_ts,
                                          _resample_step_function_1hz)
        start = _parse_event_ts(start_ts)
        end = _parse_event_ts(end_ts) if end_ts else None
        if start is None or end is None or end <= start:
            return None, False              # unusable span — settled skip
        try:
            hist = await self._ha.get_history_batch(
                [flow_entity, pressure_entity],
                start - timedelta(seconds=_FETCH_PAD_S),
                end + timedelta(seconds=2),
            )
        except Exception as e:
            log.debug("rise-corr backfill: history fetch failed for %s..%s: %s",
                      start_ts, end_ts, e)
            return None, True               # transient — retried next sweep

        def _series(entity: str) -> List[Tuple[datetime, float]]:
            return sorted(
                ((_parse_ts(e.get("last_changed")), float(e["state"]))
                 for e in (hist.get(entity) or [])
                 if _is_numeric(e.get("state"))
                 and _parse_ts(e.get("last_changed")) is not None),
                key=lambda x: x[0],
            )

        flow_entries = _series(flow_entity)
        pres_entries = _series(pressure_entity)
        if not flow_entries or not pres_entries:
            return None, False              # history pruned — settled skip
        flow_default = next((v for t, v in reversed(flow_entries) if t < start), 0.0)
        pres_default = next((v for t, v in reversed(pres_entries) if t < start), 0.0)
        flow_1hz = _resample_step_function_1hz(flow_entries, start, end,
                                               default=flow_default)
        pres_1hz = _resample_step_function_1hz(pres_entries, start, end,
                                               default=pres_default)
        return _flow_pressure_correlation(flow_1hz, pres_1hz), False

    # ── verdicts + stamp ─────────────────────────────────────────────────────

    async def _apply_verdicts(self) -> None:
        """Run the sync rise repair pass under the write lock (idempotent)."""
        from .database import run_isolated_write
        from .feature_extractor import reprocess_rising_pressure_phantoms
        try:
            res = await run_isolated_write(
                self._db_path, reprocess_rising_pressure_phantoms)
            if res.get("rise_flagged"):
                log.info("rise-corr backfill: verdict pass flagged %d event(s)",
                         res["rise_flagged"])
        except Exception as e:
            log.error("rise-corr backfill verdict pass failed: %s", e,
                      exc_info=True)

    def _done(self) -> bool:
        row = self._db.execute(
            "SELECT COALESCE(rise_corr_backfill_done, 0) AS d "
            "FROM home_profile WHERE id = 1").fetchone()
        return bool(row and row["d"])

    def _stamp_done(self) -> None:
        try:
            self._db.execute(
                "UPDATE home_profile SET rise_corr_backfill_done = 1 WHERE id = 1")
            self._db.commit()
        except sqlite3.Error as e:
            log.error("rise-corr backfill: failed to stamp done: %s", e)
