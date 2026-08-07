"""
Data pruner — runs once daily at 03:00.

Responsibilities:
  1. Backfill daily_summary for any day with events but no summary
  2. Prune raw events older than retention window (training era protected)
  3. Prune hourly_volume older than retention window (training era protected)
  4. Prune zone_flow_history and threshold_history (no fence needed)
  5. Write auto-backup Quick Restore JSON if enabled and due

On first startup (empty daily_summary table) a full backfill runs immediately
so the history chart is populated without waiting until 03:00.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .database import (
    get_data_retention, update_data_retention, compute_daily_summary,
    local_day_of, local_day_bounds_utc,
)

log = logging.getLogger(__name__)


class DataPruner:

    def __init__(self, db: sqlite3.Connection, db_path: Path = None):
        self._db      = db
        self._db_path = db_path   # needed for auto-backup
        self._stop    = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """
        On first start: run a full backfill immediately if daily_summary is empty.
        Then wait until 03:00 and run the full nightly job daily.

        prune_now() and _startup_backfill() do many sync SQLite operations
        (DELETEs + daily-summary computation across all events) which can
        block the event loop for several seconds on a populated DB. They're
        offloaded to a worker thread via run_in_executor so the rest of the
        addon stays responsive to ingress requests.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._startup_backfill_sync)

        await self._wait_until_3am()
        while not self._stop.is_set():
            try:
                await loop.run_in_executor(None, self.prune_now)
                await self._run_auto_backup()
            except Exception as e:
                log.error("Data pruner nightly error: %s", e, exc_info=True)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=86400)
            except asyncio.TimeoutError:
                pass

    # ── Startup backfill ────────────────────────────────────────────────────

    def _startup_backfill_sync(self) -> None:
        """Synchronous variant of the startup backfill — invoked from
        run() via run_in_executor so the heavy summary computation
        doesn't block the event loop.

        If daily_summary is empty (first install or fresh DB), compute
        summaries for all historical events immediately so the history
        chart isn't blank.
        """
        try:
            count = self._db.execute(
                "SELECT COUNT(*) FROM daily_summary").fetchone()[0]
        except Exception:
            count = 0

        now = datetime.now(timezone.utc)
        if count == 0:
            log.info("daily_summary is empty — running startup backfill")
            self._compute_missing_summaries(now, full_backfill=True)
        else:
            # Also catch any days missed since last run (e.g. after an update)
            self._compute_missing_summaries(now)
        self._compute_fixture_daily_summaries(now)

    async def _startup_backfill(self) -> None:
        """Async wrapper kept for any external callers that expect the
        original signature. Delegates to the sync variant via the loop's
        default executor so the heavy work happens off the event loop."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._startup_backfill_sync)

    # ── Nightly job ─────────────────────────────────────────────────────────

    def prune_now(self) -> dict:
        """Run pruning immediately. Returns counts of deleted rows."""
        cfg = get_data_retention(self._db)
        if not cfg.get("enabled", 1):
            log.info("Data pruning disabled — skipping")
            return {}

        events_retain_years        = int(cfg.get("events_retain_years", 1))
        hourly_volume_retain_years = int(cfg.get("hourly_volume_retain_years", 2))

        now           = datetime.now(timezone.utc)
        events_cutoff = (now - timedelta(days=events_retain_years * 365)).isoformat()
        volume_cutoff = (now - timedelta(days=hourly_volume_retain_years * 365)).isoformat()

        deleted = {}

        # Step 1: compute summaries BEFORE pruning raw events
        self._compute_missing_summaries(now)

        # Step 2: prune raw events (training window protected)
        # Only events within [calibration_started_at, calibration_ends_at] are
        # protected — events predating the device installation are not preserved.
        try:
            cur = self._db.execute("""
                DELETE FROM events
                WHERE start_ts < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM training_state ts
                      WHERE ts.circuit = events.circuit
                        AND ts.started_at IS NOT NULL
                        AND ts.calibration_ends_at IS NOT NULL
                        AND events.start_ts BETWEEN ts.started_at
                                                AND ts.calibration_ends_at
                  )
            """, (events_cutoff,))
            deleted["events"] = cur.rowcount
        except Exception as e:
            log.error("Pruning events: %s", e)
            deleted["events"] = 0

        # Step 3: prune hourly_volume (training window protected)
        try:
            cur = self._db.execute("""
                DELETE FROM hourly_volume
                WHERE hour_ts < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM training_state ts
                      WHERE ts.circuit = hourly_volume.circuit
                        AND ts.started_at IS NOT NULL
                        AND ts.calibration_ends_at IS NOT NULL
                        AND hourly_volume.hour_ts BETWEEN ts.started_at
                                                      AND ts.calibration_ends_at
                  )
            """, (volume_cutoff,))
            deleted["hourly_volume"] = cur.rowcount
        except Exception as e:
            log.error("Pruning hourly_volume: %s", e)
            deleted["hourly_volume"] = 0

        # Step 4: prune auxiliary tables (no training fence needed)
        for tbl, col in [
            ("zone_flow_history",     "recorded_at"),
            ("threshold_history",     "recorded_at"),
            ("cluster_cooccurrence",  "last_seen_at"),   # Phase 2
        ]:
            try:
                cur = self._db.execute(
                    f"DELETE FROM {tbl} WHERE {col} < ?", (events_cutoff,))
                deleted[tbl] = cur.rowcount
            except Exception as e:
                log.error("Pruning %s: %s", tbl, e)
                deleted[tbl] = 0

        # fixture_daily_summary stores DATE (not TIMESTAMP) — use date() to avoid
        # string-comparison boundary issue where 'YYYY-MM-DD' < 'YYYY-MM-DDT...'
        try:
            cur = self._db.execute(
                "DELETE FROM fixture_daily_summary WHERE day < date(?)", (events_cutoff,))
            deleted["fixture_daily_summary"] = cur.rowcount
        except Exception as e:
            log.error("Pruning fixture_daily_summary: %s", e)
            deleted["fixture_daily_summary"] = 0

        # cluster_metrics_history — hard 90-day retention window
        metrics_cutoff = (now - timedelta(days=90)).isoformat()
        try:
            cur = self._db.execute(
                "DELETE FROM cluster_metrics_history WHERE measured_at < ?",
                (metrics_cutoff,)
            )
            deleted["cluster_metrics_history"] = cur.rowcount
        except Exception as e:
            log.error("Pruning cluster_metrics_history: %s", e)
            deleted["cluster_metrics_history"] = 0

        # circuit_exclusion_windows — transient state, keep 30 days
        excl_cutoff = (now - timedelta(days=30)).isoformat()
        try:
            cur = self._db.execute(
                "DELETE FROM circuit_exclusion_windows WHERE ends_at < ?",
                (excl_cutoff,)
            )
            deleted["circuit_exclusion_windows"] = cur.rowcount
        except Exception as e:
            log.error("Pruning circuit_exclusion_windows: %s", e)
            deleted["circuit_exclusion_windows"] = 0

        # training_capture — transient wizard state, keep 30 days. Orphaned
        # candidate rows (parent pruned) are swept too.
        try:
            cur = self._db.execute(
                "DELETE FROM training_capture WHERE created_at < ?", (excl_cutoff,))
            deleted["training_capture"] = cur.rowcount
            self._db.execute(
                "DELETE FROM training_capture_candidates WHERE capture_id NOT IN "
                "(SELECT id FROM training_capture)")
        except Exception as e:
            log.error("Pruning training_capture: %s", e)
            deleted["training_capture"] = 0

        # Step 5: compute per-fixture daily summaries for any gaps
        self._compute_fixture_daily_summaries(now)

        self._db.commit()
        update_data_retention(self._db, last_pruned_at=now.isoformat())

        total = sum(deleted.values())
        if total:
            log.info("Pruning complete — %d rows deleted: %s", total,
                     ", ".join(f"{t}={n}" for t, n in deleted.items() if n))
        else:
            log.info("Pruning complete — nothing to delete")

        return deleted

    # ── Daily summary computation ───────────────────────────────────────────

    def _local_days_with_events(self, circuit: str) -> list:
        """Every LOCAL calendar day this circuit has events on, oldest first.

        Derived from the circuit's first/last event rather than
        `GROUP BY date(start_ts)`: start_ts is UTC, and one UTC day straddles
        two local days, so a UTC-keyed group can't address a local-day summary
        row. Two scalar queries + a date walk beats a full-table group-by.
        """
        row = self._db.execute(
            "SELECT MIN(start_ts) AS lo, MAX(start_ts) AS hi "
            "FROM events WHERE circuit = ?", (circuit,)).fetchone()
        if not row or not row["lo"]:
            return []
        first = local_day_of(row["lo"])
        last  = local_day_of(row["hi"])
        if not first or not last:
            return []
        out, d = [], datetime.strptime(first, "%Y-%m-%d")
        end = datetime.strptime(last, "%Y-%m-%d")
        while d <= end:
            out.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        return out

    def _compute_missing_summaries(self, now: datetime,
                                   full_backfill: bool = False) -> None:
        """
        Compute daily summaries for days that are missing or stale.
        full_backfill=True processes all historical days (used at startup).

        Days are the home's LOCAL calendar days (see database.local_day_of) —
        the same key `daily_summary.day` is written under.
        """
        # Bounds are LOCAL days (local_day_of), so `now` — which arrives in UTC
        # — has to be converted before slicing, or the cutoff lands on the wrong
        # side of the boundary for the six hours after local midnight.
        yesterday = local_day_of((now - timedelta(days=1)).isoformat())
        week_ago  = local_day_of((now - timedelta(days=7)).isoformat())
        lower = "" if full_backfill else week_ago

        try:
            circuits = [r["circuit"] for r in self._db.execute(
                "SELECT DISTINCT circuit FROM events").fetchall()]
            gaps = []
            for circuit in circuits:
                existing = {
                    r["day"]: r["computed_at"] for r in self._db.execute(
                        "SELECT day, computed_at FROM daily_summary "
                        "WHERE circuit = ?", (circuit,)).fetchall()}
                for day in self._local_days_with_events(circuit):
                    if day > yesterday or (lower and day < lower):
                        continue
                    if day not in existing:
                        gaps.append((circuit, day))
                        continue
                    # Stale: summarised before the day was over, so later
                    # events on it were never counted. The day's end is a UTC
                    # instant, directly comparable to the stored computed_at.
                    computed_at = existing[day]
                    _, day_end = local_day_bounds_utc(day)
                    if not computed_at or str(computed_at)[:19] < day_end:
                        gaps.append((circuit, day))
        except Exception as e:
            log.warning("Summary gap query: %s", e)
            return

        computed = 0
        for circuit, day in gaps:
            try:
                if compute_daily_summary(self._db, circuit, day):
                    computed += 1
            except Exception as e:
                log.warning("Summary compute [%s/%s]: %s", circuit, day, e)

        if computed:
            self._db.commit()
            log.info("Daily summaries computed: %d day(s)%s",
                     computed, " (backfill)" if full_backfill else "")

    # ── Fixture daily summaries (F1) ────────────────────────────────────────

    def _compute_fixture_daily_summaries(self, now: datetime) -> None:
        """
        Populate fixture_daily_summary for any (circuit, fixture_id, day)
        triples that have events but no summary row.  Runs nightly and on
        the startup backfill so analytics are available from day one.
        """
        yesterday = local_day_of((now - timedelta(days=1)).isoformat())
        try:
            gaps = []
            circuits = [r["circuit"] for r in self._db.execute(
                "SELECT DISTINCT circuit FROM events "
                "WHERE fixture_id IS NOT NULL").fetchall()]
            for circuit in circuits:
                have = {(r["fixture_id"], r["day"]) for r in self._db.execute(
                    "SELECT fixture_id, day FROM fixture_daily_summary "
                    "WHERE circuit = ?", (circuit,)).fetchall()}
                for day in self._local_days_with_events(circuit):
                    if day > yesterday:
                        continue
                    lo, hi = local_day_bounds_utc(day)
                    for r in self._db.execute(
                        "SELECT DISTINCT fixture_id FROM events "
                        "WHERE circuit = ? AND fixture_id IS NOT NULL "
                        "  AND start_ts >= ? AND start_ts < ?",
                            (circuit, lo, hi)).fetchall():
                        if (r["fixture_id"], day) not in have:
                            gaps.append((circuit, r["fixture_id"], day, lo, hi))
        except Exception as e:
            log.warning("fixture_daily_summary gap query: %s", e)
            return

        computed = 0
        for circuit, fixture_id, day, lo, hi in gaps:
            try:
                self._db.execute("""
                    INSERT OR REPLACE INTO fixture_daily_summary
                        (circuit, fixture_id, day, event_count,
                         total_volume_litres, avg_flow_lpm, peak_flow_lpm)
                    SELECT circuit, fixture_id,
                           ?                 AS day,
                           COUNT(*)          AS event_count,
                           COALESCE(SUM(COALESCE(volume_litres_effective,
                                                 volume_litres, 0)), 0)
                                             AS total_volume_litres,
                           AVG(avg_flow_lpm)               AS avg_flow_lpm,
                           MAX(peak_flow_lpm)              AS peak_flow_lpm
                    FROM events
                    WHERE circuit = ? AND fixture_id = ?
                      AND start_ts >= ? AND start_ts < ?
                    GROUP BY circuit, fixture_id
                """, (day, circuit, fixture_id, lo, hi))
                computed += 1
            except Exception as e:
                log.warning("fixture_daily_summary [%s/%s/%s]: %s",
                            circuit, fixture_id, day, e)

        if computed:
            self._db.commit()
            log.info("Fixture daily summaries computed: %d row(s)", computed)

    # ── Auto-backup ─────────────────────────────────────────────────────────

    async def _run_auto_backup(self) -> None:
        """Write a Quick Restore JSON to the filesystem if due."""
        cfg = get_data_retention(self._db)
        if not cfg.get("auto_backup_enabled"):
            return

        target_dow = int(cfg.get("auto_backup_day_of_week", 0))
        if datetime.now().weekday() != target_dow:
            return

        backup_path = Path(cfg.get("auto_backup_path",
                                   "/share/water_monitor_backups"))
        try:
            backup_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            log.error("Auto-backup: cannot create directory %s: %s",
                      backup_path, e)
            return

        try:
            from .routers.backup import (
                QUICK_RESTORE_TABLES, QUICK_RESTORE_DAYS)
            from datetime import timedelta

            cutoff = (datetime.now(timezone.utc)
                      - timedelta(days=QUICK_RESTORE_DAYS)).isoformat()
            tables = {}

            for tbl in QUICK_RESTORE_TABLES:
                rows = self._db.execute(f"SELECT * FROM {tbl}").fetchall()
                tables[tbl] = [dict(r) for r in rows]

            for tbl, col in [("events", "start_ts"),
                              ("hourly_volume", "hour_ts")]:
                rows = self._db.execute(
                    f"SELECT * FROM {tbl} WHERE {col} >= ?",
                    (cutoff,)).fetchall()
                tables[tbl] = [dict(r) for r in rows]

            payload = {
                "backup_type":  "quick_restore",
                "version":      3,
                "exported_at":  datetime.now(timezone.utc).isoformat(),
                "history_days": QUICK_RESTORE_DAYS,
                "tables":       tables,
            }
            ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = backup_path / f"wm_auto_{ts}.json"
            filename.write_text(
                json.dumps(payload, indent=2, default=str), encoding="utf-8")

            update_data_retention(
                self._db,
                last_auto_backup_at=datetime.now(timezone.utc).isoformat())

            # Keep only the 4 most recent auto-backups
            backups = sorted(backup_path.glob("wm_auto_*.json"))
            for old in backups[:-4]:
                try:
                    old.unlink()
                except Exception:
                    pass

            log.info("Auto-backup written: %s", filename)

        except Exception as e:
            log.error("Auto-backup failed: %s", e, exc_info=True)

    async def _wait_until_3am(self) -> None:
        """Sleep until 03:00 local time.  Recalculates in 1-hour chunks so
        DST transitions (spring-forward / fall-back) never cause the job to
        be skipped or fire an hour early."""
        while True:
            now    = datetime.now()
            target = now.replace(hour=3, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            # Sleep at most 1 hour at a time so a DST change is picked up
            # within the next chunk rather than after the full calculated gap.
            sleep_secs = min((target - now).total_seconds(), 3600)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=sleep_secs)
                return   # stop requested
            except asyncio.TimeoutError:
                if datetime.now() >= target:
                    return   # it's 03:00 (or past it)
