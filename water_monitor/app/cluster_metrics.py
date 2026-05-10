"""
Cluster quality metrics — Phase 2.

Background task that computes hourly health metrics for each circuit's
clustering state and writes them to cluster_metrics_history.  The Fixtures
page and future health-scoring features read from this table.

Metrics computed per circuit:
  cluster_count         — number of active fixture_clusters rows
  coverage_pct          — % of events in last 24h that have a cluster_id
  avg_purity            — not computed yet (requires ground-truth labels);
                          set to avg(suggested_confidence) as a proxy
  avg_stability         — avg suggested_confidence across clusters with > 0 members
  unmatched_recent_24h  — event count where cluster_id IS NULL in last 24h
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from .config import AddonConfig

log = logging.getLogger(__name__)

_INTERVAL_SECONDS = 3600   # run every hour
_WINDOW_HOURS     = 24     # look-back for coverage / unmatched counts


class ClusterMetrics:
    """Computes and persists hourly cluster quality metrics."""

    def __init__(self, db: sqlite3.Connection, cfg: AddonConfig):
        self._db  = db
        self._cfg = cfg
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Background loop — compute metrics once per hour."""
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_INTERVAL_SECONDS)
                return
            except asyncio.TimeoutError:
                pass

            for circuit_cfg in self._cfg.circuits:
                try:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, self.compute_and_store, circuit_cfg.circuit
                    )
                except Exception as e:
                    log.error("[%s] cluster metrics error: %s",
                              circuit_cfg.circuit, e, exc_info=True)

    def compute_and_store(self, circuit: str) -> None:
        """Compute metrics for one circuit and insert a row into the history table."""
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(hours=_WINDOW_HOURS)).isoformat()

        # cluster_count — number of clusters that have ever had a member
        cluster_count = self._db.execute(
            "SELECT COUNT(*) FROM fixture_clusters WHERE circuit = ? AND member_count > 0",
            (circuit,)
        ).fetchone()[0] or 0

        # coverage_pct — events with a cluster_id vs total events in last 24h
        total_row = self._db.execute(
            "SELECT COUNT(*) FROM events WHERE circuit = ? AND start_ts >= ?",
            (circuit, cutoff)
        ).fetchone()
        total_recent = total_row[0] if total_row else 0

        matched_row = self._db.execute(
            """SELECT COUNT(*) FROM events
               WHERE circuit = ? AND start_ts >= ? AND cluster_id IS NOT NULL""",
            (circuit, cutoff)
        ).fetchone()
        matched_recent = matched_row[0] if matched_row else 0

        coverage_pct = (matched_recent / total_recent * 100.0) if total_recent > 0 else 0.0

        # unmatched_recent_24h
        unmatched_recent = total_recent - matched_recent

        # avg_stability / avg_purity — use suggested_confidence as proxy
        stability_row = self._db.execute(
            """SELECT AVG(suggested_confidence)
               FROM fixture_clusters
               WHERE circuit = ? AND member_count > 0 AND suggested_confidence IS NOT NULL""",
            (circuit,)
        ).fetchone()
        avg_stability = float(stability_row[0]) if stability_row and stability_row[0] else 0.0
        avg_purity = avg_stability  # proxy until ground-truth labels exist

        try:
            self._db.execute(
                """INSERT INTO cluster_metrics_history
                       (measured_at, circuit, cluster_count, coverage_pct,
                        avg_purity, avg_stability, unmatched_recent_24h)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (now.isoformat(), circuit, cluster_count, coverage_pct,
                 avg_purity, avg_stability, unmatched_recent)
            )
            self._db.commit()
            log.debug(
                "[%s] cluster metrics: count=%d coverage=%.1f%% stability=%.2f unmatched=%d",
                circuit, cluster_count, coverage_pct, avg_stability, unmatched_recent,
            )
        except Exception as e:
            log.warning("[%s] cluster metrics write failed: %s", circuit, e)
