"""The two background jobs that keep the loop turning.

Both run daily, both are best-effort, and both do their DB work inside
``run_db``. They are deliberately separate from the classification path:
nothing here is on the critical path of recording an event, so a failure in
either degrades the add-on to "no model refresh tonight" rather than to
"events stop being classified".

WHY DAILY, AND WHY OFFSET FROM EACH OTHER
-----------------------------------------
The health pass reads the classified stream; the retrain rewrites part of it.
Running the health pass FIRST, then the retrain, means the night's health
reading is taken against a stream the day's classifier produced, rather than
half-way through a re-derive. They are an hour apart for that reason, not for
load.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from .config import DATA_DIR
from .database import finish_job, get_write_lock, run_db, start_job

log = logging.getLogger(__name__)

_HEALTH_HOUR_UTC = 9          # after the usual overnight quiet window
_RETRAIN_HOUR_UTC = 10
_ON_ERROR_S = 3600.0


def _seconds_until(hour_utc: int) -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return max((target - now).total_seconds(), 60.0)


class LearningScheduler:
    """Nightly fixture-health pass + a weekly referee'd retrain."""

    def __init__(self, db, cfg, orch=None):
        self._db = db
        self._cfg = cfg
        self._orch = orch
        self._stop = asyncio.Event()
        self._last_retrain_day: Optional[str] = None

    def stop(self) -> None:
        self._stop.set()

    # ── the health pass ─────────────────────────────────────────────────────
    async def run_health(self) -> None:
        while not self._stop.is_set():
            delay = _seconds_until(_HEALTH_HOUR_UTC)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._health_pass()
            except Exception as e:                  # noqa: BLE001
                log.warning("fixture-health pass failed (non-fatal): %s", e)
                await asyncio.sleep(_ON_ERROR_S)

    async def _health_pass(self) -> None:
        from .health_job import run_nightly
        for circ in self._cfg.circuits:
            if self._stop.is_set():
                return
            async with get_write_lock():
                out = await run_db(run_nightly, self._db, circ.circuit)
            fired = {k: v["alarms"] for k, v in (out or {}).items() if v["alarms"]}
            if fired:
                log.warning("[%s] fixture health raised: %s", circ.circuit,
                            {k: [a["signal"] for a in v] for k, v in fired.items()})
            else:
                log.info("[%s] fixture health: nothing raised (%d fixture(s) "
                         "watched)", circ.circuit, len(out or {}))

    # ── the retrain ─────────────────────────────────────────────────────────
    async def run_retrain(self) -> None:
        while not self._stop.is_set():
            delay = _seconds_until(_RETRAIN_HOUR_UTC)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._retrain_pass()
            except Exception as e:                  # noqa: BLE001
                log.warning("tinymodel retrain failed (non-fatal): %s", e)
                await asyncio.sleep(_ON_ERROR_S)

    async def _retrain_pass(self) -> None:
        """Weekly, and only ever through the referee.

        A retrain is not automatically an improvement — the referee decides,
        and a rejected challenger leaves the incumbent serving. Both outcomes
        are logged: a run of rejections means the model has stopped improving,
        which is something the operator should be able to see.
        """
        from . import tinymodel as tm
        from .learning_loop import weekly_retrain_recorded

        today = datetime.now(timezone.utc).strftime("%G-W%V")
        if self._last_retrain_day == today:
            return
        if not tm.sklearn_available():
            log.info("tinymodel retrain skipped: scikit-learn not installed "
                     "in this image — the kNN ladder is serving")
            self._last_retrain_day = today
            return
        # The in-memory marker above is lost on every redeploy, which lets the
        # identical challenger be re-judged on consecutive nights. The ledger is
        # the durable record of "already ran this week".
        if await run_db(weekly_retrain_recorded, self._db, today):
            log.info("tinymodel retrain already recorded for %s — skipping", today)
            self._last_retrain_day = today
            return
        for circ in self._cfg.circuits:
            if self._stop.is_set():
                return
            await self.retrain_circuit(circ.circuit, "weekly scheduled retrain",
                                       trigger="weekly")
        self._last_retrain_day = today

    async def retrain_circuit(self, circuit: str, reason: str,
                              trigger: Optional[str] = None):
        """Put ONE circuit through the referee, and return its outcome.

        ``trigger`` is what the ledger records — ``weekly`` or ``on_demand``,
        derived from ``reason`` when not given.

        Shared by the weekly pass and the on-demand Dev Tools button, so the
        two cannot drift: same write lock, same single DB hop, same cache
        invalidation afterwards. On-demand does NOT touch ``_last_retrain_day``
        — forcing a retrain today should not silently cancel this week's
        scheduled one.

        Note this forces a DECISION, not a swap. The referee still rules, so a
        challenger that is not better leaves the incumbent serving; ``status``
        says which happened.
        """
        from . import tinymodel as tm
        from .learning_loop import (STALL_STREAK, activate_pending_benchmark,
                                    benchmark_ids_for_circuit, learning_status,
                                    maybe_auto_pin_benchmark,
                                    record_retrain_decision, retrain)

        trigger = trigger or ("weekly" if reason.startswith("weekly") else "on_demand")
        # Every outcome gets a job row, not just the ones that swap the model.
        # A referee that rejects week after week is a model that has stopped
        # improving, and that failure is SILENT: a frozen champion serves
        # exactly like a healthy one, and nothing reads the log. This is the
        # one place both the weekly pass and the Dev Tools button go through,
        # so recording it here covers both.
        job = await run_db(start_job, self._db, "tinymodel_retrain", circuit,
                           "Re-fitting the learned model…")
        try:
            async with get_write_lock():
                # A home that has enough labels pins its own benchmark here,
                # BEFORE the benchmark is read and BEFORE retrain runs, so
                # the very first champion is trained with the set already
                # reserved. No-op once a benchmark exists (replacing one is an
                # operator decision). A pin fault must not fail the retrain.
                try:
                    pinned = await run_db(maybe_auto_pin_benchmark, self._db, circuit)
                    if pinned:
                        log.info("[%s] referee benchmark auto-pinned: %d event(s) over "
                                 "%d day(s), hash %s (from %d human labels)", circuit,
                                 pinned.get("requested_n"), pinned.get("n_days"),
                                 pinned.get("source_hash"), pinned.get("pinned_from_n"))
                except Exception as e:                      # noqa: BLE001
                    log.warning("[%s] benchmark auto-pin failed (non-fatal); retrain "
                                "continues without it: %s", circuit, e)
                # The pinned benchmark lives in the DB (referee_benchmark,
                # imported once via Dev Tools) and is the referee's PRIMARY leg.
                # An empty table yields [] → the benchmark leg abstains, and an
                # abstaining referee KEEPS the incumbent rather than promoting.
                ref = await run_db(benchmark_ids_for_circuit, self._db, circuit)
                out = await run_db(retrain, self._db, circuit,
                                   str(DATA_DIR), ref["ids"],
                                   tm.DEFAULT_TARGET_PRECISION, None, True,
                                   reason, benchmark_meta=ref)
        except Exception as e:                              # noqa: BLE001
            await run_db(finish_job, self._db, job, "error",
                         f"{circuit}: retrain failed — {e}")
            raise
        tm.invalidate_cache(circuit)
        log.info("[%s] retrain: %s — %s", circuit, out.status, out.reason)
        await run_db(finish_job, self._db, job,
                     "error" if out.status == "unavailable" else "done",
                     f"{circuit}: {out.status} — {out.reason}")
        # The durable record (the job row above is pruned after two days), and
        # the stall check nothing else would raise. Both best-effort: a ledger
        # problem must not fail a finished retrain.
        try:
            # The decision row is written FIRST and carries the benchmark hash
            # the leg was actually scored on (captured inside retrain); only
            # then may a pending re-pin take over. A promotion
            # is never recorded under a hash that did not judge it.
            await run_db(record_retrain_decision, self._db, circuit, trigger, out)
            if getattr(out, "swapped", False):
                async with get_write_lock():
                    act = await run_db(activate_pending_benchmark, self._db, circuit,
                                       str(DATA_DIR))
                if act:
                    log.info("[%s] pending benchmark took over after the promotion: "
                             "%s -> %s%s", circuit, act.get("from_hash"),
                             act.get("to_hash"),
                             " (re-selected: the pending set had decayed)"
                             if act.get("reselected") else "")
            status = await run_db(learning_status, self._db, circuit)
            if status.get("stalled"):
                log.warning("[%s] the referee has kept the incumbent %d time(s) in "
                            "a row (threshold %d) — the learned model has stopped "
                            "improving. Check that a benchmark is pinned and that "
                            "new labels are arriving.", circuit,
                            status.get("kept_streak"), STALL_STREAK)
        except Exception as e:                              # noqa: BLE001
            log.warning("[%s] retrain ledger/status step failed (non-fatal): %s",
                        circuit, e)
        return out
