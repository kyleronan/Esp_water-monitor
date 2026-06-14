"""
Maturity re-check — deferred appliance-label confirmation (Branch-2.2).

Background task that periodically re-runs the full-context classification pipeline
(``recompute_cycle_pulse_counts`` → ``reclassify_all_events_from_signatures`` →
``resuggest_all_clusters``) over a RECENT window, so a provisional live label assigned
before an event's cycle-mates existed is CONFIRMED or RETRACTED once the surrounding
draws have actually happened.

Why this exists: at event-completion time the future draws of a cycle haven't occurred
yet (the live path computes ``cycle_pulse_count`` with ``past_only=True``), so an
isolated draw and the first pulse of a real multi-draw appliance cycle look identical.
The tightened multi-draw detectors (``event_rules`` / ``fixtures``) only reject an
isolated draw once they can see the whole window — which this pass gives them.
``reclassify_all_events_from_signatures`` clears a stale match by writing NULL and NEVER
touches ``user_fixture_type``, so a wrong auto-label is retracted while a manual
correction is preserved.

Settle window: an event is immutable once older than the longest detector window (the
softener's ~3.5 h max session span) — no later event can fall inside any detector's
window. ``_SETTLE_HORIZON_HOURS`` (6 h) is comfortably larger, so every event is
re-checked a few times while maturing and then ages out of the window settled — there
is no forever-churn and the ``cycle_group_id`` rollup parent stabilises.

Writes go through ``database.run_isolated_write`` (write lock + private connection),
exactly like the manual ``/recompute`` route — NOT the direct shared-connection write
``ClusterMetrics`` uses for its one tiny row/hour (a heavy reclassify on the shared
connection from an executor thread is the race ``run_isolated_write`` was built to fix).
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from .config import AddonConfig, DB_PATH

log = logging.getLogger(__name__)

_INTERVAL_SECONDS = 3600          # re-check hourly (matches the "an hour later" intent)
_SETTLE_HORIZON_HOURS = 6         # only re-evaluate events newer than this; > the 3.5 h
#                                   softener max span, so a matured event ages out settled


class MaturityRecheck:
    """Periodically confirms or retracts provisional appliance labels on matured events."""

    def __init__(self, db: sqlite3.Connection, cfg: AddonConfig):
        self._db = db
        self._cfg = cfg
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Background loop — re-check matured events once per hour."""
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_INTERVAL_SECONDS)
                return
            except asyncio.TimeoutError:
                pass

            for circuit_cfg in self._cfg.circuits:
                try:
                    await self._recheck_circuit(circuit_cfg.circuit)
                except Exception as e:
                    log.error("[%s] maturity re-check error: %s",
                              circuit_cfg.circuit, e, exc_info=True)

    async def _recheck_circuit(self, circuit: str) -> None:
        from .database import (get_write_lock, recompute_cycle_pulse_counts,
                               reclassify_all_events_from_signatures,
                               resuggest_all_clusters, run_isolated_write)
        # Skip this tick if a manual reprocess (or a still-running re-check) holds the
        # write lock — try again next hour rather than queue behind a multi-second job
        # (mirrors the /recompute route's best-effort busy guard).
        if get_write_lock().locked():
            log.debug("[%s] maturity re-check skipped — write lock busy", circuit)
            return
        window_start = (datetime.now(timezone.utc)
                        - timedelta(hours=_SETTLE_HORIZON_HOURS)).isoformat()

        def _job(conn):
            # Order matters: cycle-pulse backfill (full past+future window) MUST run
            # before reclassify so the dishwasher rule reads the matured count; resuggest
            # then re-types cluster centroids off the patched means. reclassify is
            # idempotent and only rewrites changed rows, so a settled event is a no-op.
            recompute_cycle_pulse_counts(conn, circuit, since_ts=window_start)
            r = reclassify_all_events_from_signatures(conn, circuit,
                                                      since_ts=window_start)
            resuggest_all_clusters(conn, circuit)
            return r

        res = await run_isolated_write(DB_PATH, _job)
        if res and (res.get("events_matched") or res.get("events_cleared")):
            log.info("[%s] maturity re-check (last %dh): %d matched, %d cleared",
                     circuit, _SETTLE_HORIZON_HOURS, res.get("events_matched", 0),
                     res.get("events_cleared", 0))
