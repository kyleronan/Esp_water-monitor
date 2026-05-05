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
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .database import (
    get_data_retention, update_data_retention, compute_daily_summary,
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
        """
        await self._startup_backfill()

        await self._wait_until_3am()
        while not self._stop.is_set():
            try:
                self.prune_now()
                await self._run_auto_backup()
            except Exception as e:
                log.error("Data pruner nightly error: %s", e, exc_info=True)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=86400)
            except asyncio.TimeoutError:
                pass

    # ── Startup backfill ────────────────────────────────────────────────────

    async def _startup_backfill(self) -> None:
        """
        If daily_summary is empty (first install or fresh DB), compute summaries
        for all historical events immediately so the history chart isn't blank.
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

    def _compute_missing_summaries(self, now: datetime,
                                   full_backfill: bool = False) -> None:
        """
        Compute daily summaries for days that are missing or stale.
        full_backfill=True processes all historical days (used at startup).
        """
        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            if full_backfill:
                # All days in events that don't have a summary yet
                gaps = self._db.execute("""
                    SELECT e.circuit, date(e.start_ts) AS day
                    FROM events e
                    LEFT JOIN daily_summary ds
                        ON ds.circuit = e.circuit
                        AND ds.day    = date(e.start_ts)
                    WHERE ds.day IS NULL
                      AND date(e.start_ts) <= ?
                    GROUP BY e.circuit, date(e.start_ts)
                    ORDER BY e.circuit, day ASC
                """, (yesterday,)).fetchall()
            else:
                # Only recent gaps (last 7 days) to catch any missed runs
                week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
                gaps = self._db.execute("""
                    SELECT e.circuit, date(e.start_ts) AS day
                    FROM events e
                    LEFT JOIN daily_summary ds
                        ON ds.circuit = e.circuit
                        AND ds.day    = date(e.start_ts)
                    WHERE (ds.day IS NULL
                           OR ds.computed_at < date(e.start_ts, '+1 day'))
                      AND date(e.start_ts) BETWEEN ? AND ?
                    GROUP BY e.circuit, date(e.start_ts)
                    ORDER BY e.circuit, day ASC
                """, (week_ago, yesterday)).fetchall()
        except Exception as e:
            log.warning("Summary gap query: %s", e)
            return

        computed = 0
        for row in gaps:
            circuit, day = row["circuit"], row["day"]
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
        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            gaps = self._db.execute("""
                SELECT e.circuit, e.fixture_id, date(e.start_ts) AS day
                FROM events e
                LEFT JOIN fixture_daily_summary fds
                    ON fds.circuit    = e.circuit
                    AND fds.fixture_id = e.fixture_id
                    AND fds.day        = date(e.start_ts)
                WHERE e.fixture_id IS NOT NULL
                  AND fds.day IS NULL
                  AND date(e.start_ts) <= ?
                GROUP BY e.circuit, e.fixture_id, date(e.start_ts)
            """, (yesterday,)).fetchall()
        except Exception as e:
            log.warning("fixture_daily_summary gap query: %s", e)
            return

        computed = 0
        for row in gaps:
            circuit, fixture_id, day = row["circuit"], row["fixture_id"], row["day"]
            try:
                self._db.execute("""
                    INSERT OR REPLACE INTO fixture_daily_summary
                        (circuit, fixture_id, day, event_count,
                         total_volume_litres, avg_flow_lpm, peak_flow_lpm)
                    SELECT circuit, fixture_id,
                           date(start_ts)    AS day,
                           COUNT(*)          AS event_count,
                           COALESCE(SUM(volume_litres), 0) AS total_volume_litres,
                           AVG(avg_flow_lpm)               AS avg_flow_lpm,
                           MAX(peak_flow_lpm)              AS peak_flow_lpm
                    FROM events
                    WHERE circuit = ? AND fixture_id = ? AND date(start_ts) = ?
                    GROUP BY circuit, fixture_id, date(start_ts)
                """, (circuit, fixture_id, day))
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
                QUICK_RESTORE_TABLES, QUICK_RESTORE_RECENT, QUICK_RESTORE_DAYS)
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
