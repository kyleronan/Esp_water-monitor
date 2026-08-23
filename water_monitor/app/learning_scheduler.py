"""dev47 — the two background jobs that keep the loop turning.

Both run daily, both are best-effort, and both do their DB work inside
``run_db`` (46a). They are deliberately separate from the classification path:
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
        from .database import get_write_lock, run_db
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
        from .config import DATA_DIR
        from .database import get_write_lock, run_db
        from .learning_loop import retrain

        today = datetime.now(timezone.utc).strftime("%G-W%V")
        if self._last_retrain_day == today:
            return
        if not tm.sklearn_available():
            log.info("tinymodel retrain skipped: scikit-learn not installed "
                     "in this image — the kNN ladder is serving")
            self._last_retrain_day = today
            return
        for circ in self._cfg.circuits:
            if self._stop.is_set():
                return
            async with get_write_lock():
                out = await run_db(retrain, self._db, circ.circuit,
                                   str(DATA_DIR), None,
                                   tm.DEFAULT_TARGET_PRECISION, None, True,
                                   "weekly scheduled retrain")
            tm.invalidate_cache(circ.circuit)
            log.info("[%s] retrain: %s — %s", circ.circuit, out.status, out.reason)
        self._last_retrain_day = today
