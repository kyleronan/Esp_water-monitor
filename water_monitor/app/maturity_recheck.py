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

    def __init__(self, db: sqlite3.Connection, cfg: AddonConfig, ha=None, orch=None):
        self._db = db
        self._cfg = cfg
        self._ha = ha          # HA client — needed by the §2 recorder reconcile pass
        self._orch = orch      # orchestrator — needed by the dev.38 auto-split pass
        self._auto_split_checked: set = set()   # ids dry-run-checked this process (efficiency)
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
            # dev46 (46k) — drain a SLICE of the global backlog.
            #
            # The pass above is windowed to the settle horizon, so it can
            # never work off the events a global change invalidated (a new
            # build, a rules re-fit). That backlog used to be cleared in one
            # ~150 s burst at boot, which is the worst possible moment: the
            # operator has just deployed and wants to look at the add-on, and
            # nobody is waiting on the work itself.
            #
            # So it drains here instead, a bounded slice per hour, alongside
            # the other quiet-time jobs this add-on already runs. Rows with a
            # NULL stamp — new events, ones still settling, peers freed by a
            # label — sort first inside the budget, because those DO have
            # someone waiting.
            from .database import _VERDICT_BACKLOG_PER_PASS
            b = reclassify_all_events_from_signatures(
                conn, circuit, backlog_limit=_VERDICT_BACKLOG_PER_PASS)
            resuggest_all_clusters(conn, circuit)
            r["backlog_scanned"] = b.get("events_scanned", 0)
            r["backlog_remaining"] = b.get("events_backlog_remaining", 0)
            return r

        res = await run_isolated_write(DB_PATH, _job)
        if res and (res.get("events_matched") or res.get("events_cleared")):
            log.info("[%s] maturity re-check (last %dh): %d matched, %d cleared",
                     circuit, _SETTLE_HORIZON_HOURS, res.get("events_matched", 0),
                     res.get("events_cleared", 0))
        # dev46 (46k) — say what the trickle did and what is still queued. A
        # backlog that drains silently is one nobody can tell has stalled.
        if res and (res.get("backlog_scanned") or res.get("backlog_remaining")):
            log.info("[%s] backlog re-derive: %d event(s) this hour, %d still "
                     "queued", circuit, res.get("backlog_scanned", 0),
                     res.get("backlog_remaining", 0))

        # §2 recorder reconciliation — best-effort, AFTER the reclassify job released the
        # write lock (the reconcile pass takes its own isolated write). A reconcile bug
        # must never break the maturity reclassify above, so it runs in its own guard.
        if self._ha is not None:
            try:
                from .recorder_reconcile import reconcile_circuit_volumes
                await reconcile_circuit_volumes(DB_PATH, self._ha, self._cfg, circuit)
            except Exception as e:
                log.error("[%s] recorder reconcile error: %s", circuit, e, exc_info=True)

        # dev.38 — guarded auto-split of over-merged events (OFF unless
        # home_profile.auto_split_enabled). AFTER reconcile, in its own guard so a split
        # bug never breaks the maturity reclassify. Carries an in-memory checked-set across
        # passes to avoid re-fetching settled non-split candidates. The function self-gates
        # on the flag and takes its own write lock per split via reprocess_window.
        if self._orch is not None:
            try:
                from .reprocess import auto_split_merged_events
                await auto_split_merged_events(
                    self._orch, circuit, checked=self._auto_split_checked)
            except Exception as e:
                log.error("[%s] auto-split error: %s", circuit, e, exc_info=True)
