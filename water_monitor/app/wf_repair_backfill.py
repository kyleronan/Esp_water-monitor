"""One-shot repair of ESP-waveform mis-attachment (migration 20260573).

``_enrich_from_waveform`` overwrote ``peak_flow_lpm`` / ``pressure_delta_psi`` /
``propagation_delay_ms`` from whichever buffered firmware capture scored best on
DURATION similarity. Nothing stopped two same-length draws from both claiming
the same capture, so an event could be stamped with a *different* draw's
measurements. The 2026-08-09 production export carried 110 such events, every
one of them detectable because it ended up with ``true_avg_flow_lpm >
peak_flow_lpm`` — impossible for a single draw, since an average can never
exceed its own maximum.

Two verdicts, both decided from stored evidence alone (no HA fetch):

``misattached``
    The capture provably belonged to another event: it is claimed by a second
    ``esp_waveform_used=1`` event on the same circuit, OR the violation is deep
    (true_avg >= 1.5x peak, far outside the healthy cross-sensor spread whose
    1st percentile sits at 1.01-1.09x). Everything the capture wrote is wrong,
    so pressure delta and propagation delay are cleared, ESP provenance is
    dropped, and — when the signatures came from that capture too — the event
    is pulled out of training and its display envelope deleted. On the export
    this covers 107 of the 110 (77 by duplicate claim, 84 by depth).

``floor_only``
    A shallow violation on an unshared capture: the ESP peak and the flow-stream
    average simply disagree at the margin, which healthy events also do. Only
    the impossible number is corrected; provenance, signatures and cluster
    membership are all kept.

Why the peak is NOT rebuilt from HA history: a dry run against the recorder
archive reproduced the software peak on just 59% of known-good rows and
*over*-estimated 33% of them (worst 8-10x). ``start_ts`` is backdated to the
pressure onset on pressure-triggered events while ``flow_readings`` only
accumulate from event creation, so ``max(flow over [start_ts, end_ts])`` sweeps
in pre-flow samples the software chain never saw — on cross-talk rows, the
other circuit's water hammer. The repaired peak therefore comes from the
event's own flow chain: ``max(true_avg_flow_lpm, avg_flow_lpm)``, a hard
physical floor that introduces no differently-scaled feature into a corpus
where peak feeds clustering and the k-NN tiers.

Idempotent and self-exhausting: repaired rows get ``wf_repair_at`` stamped and
``esp_waveform_used`` cleared, so neither predicate matches them again. The
sweep costs one indexed query when there is nothing to do, which is why it
needs no done-flag of its own.
"""
from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict

from .config import DB_PATH

log = logging.getLogger(__name__)

_STARTUP_DELAY_S = 240.0   # after the catch-up import and the cluster rebuild

# Depth beyond which a violation cannot be cross-sensor disagreement. The
# healthy ESP population's peak/true_avg 1st percentile is 1.007-1.09 by
# duration bucket, so 1.5x sits far outside it.
_MISATTACH_RATIO = 1.5


def repair_misattached_waveforms(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Repair every stored event whose ESP peak is below its own active average.

    Synchronous and self-contained so it can be driven straight from a test or
    an offline script. Commits once. Returns a summary dict:
    ``{"misattached", "floor_only", "excluded", "envelopes_deleted",
    "circuits"}``.
    """
    try:
        rows = conn.execute(
            """SELECT id, circuit, waveform_event_id, waveform_boot_id,
                      peak_flow_lpm, pressure_delta_psi, propagation_delay_ms,
                      true_avg_flow_lpm, avg_flow_lpm, signature_source,
                      cluster_id, prev_cluster_id, match_rejection_reason
               FROM events
               WHERE esp_waveform_used = 1
                 AND wf_repair_at IS NULL
                 AND true_avg_flow_lpm IS NOT NULL
                 AND true_avg_flow_lpm > 0
                 AND peak_flow_lpm IS NOT NULL
                 AND peak_flow_lpm > 0
                 AND peak_flow_lpm < true_avg_flow_lpm
               ORDER BY start_ts"""
        ).fetchall()
    except sqlite3.Error as e:
        # Pre-migration schema (no wf_repair_at) — nothing to do yet.
        log.debug("wf-repair: candidate query unavailable (%s)", e)
        return {"misattached": 0, "floor_only": 0, "excluded": 0,
                "envelopes_deleted": 0, "circuits": []}

    if not rows:
        return {"misattached": 0, "floor_only": 0, "excluded": 0,
                "envelopes_deleted": 0, "circuits": []}

    now_iso = datetime.now(timezone.utc).isoformat()
    counts = {"misattached": 0, "floor_only": 0}
    excluded = 0
    envelopes = 0
    circuits: set = set()

    for r in rows:
        true_avg = float(r["true_avg_flow_lpm"] or 0.0)
        avg_flow = float(r["avg_flow_lpm"] or 0.0)
        peak = float(r["peak_flow_lpm"] or 0.0)

        # Direct evidence: is this capture claimed by another ESP-enriched
        # event on the same circuit? Matched on boot_id too when it is known —
        # rows written before 20260573 have none, and the firmware event
        # counter restarts each boot, so event_id alone would over-match.
        if r["waveform_boot_id"] is not None:
            shared = conn.execute(
                """SELECT 1 FROM events
                   WHERE circuit = ? AND waveform_event_id = ?
                     AND waveform_boot_id = ? AND esp_waveform_used = 1
                     AND id != ? LIMIT 1""",
                (r["circuit"], r["waveform_event_id"], r["waveform_boot_id"],
                 r["id"]),
            ).fetchone()
        else:
            shared = conn.execute(
                """SELECT 1 FROM events
                   WHERE circuit = ? AND waveform_event_id = ?
                     AND esp_waveform_used = 1 AND id != ? LIMIT 1""",
                (r["circuit"], r["waveform_event_id"], r["id"]),
            ).fetchone()

        deep = peak > 0 and (true_avg / peak) >= _MISATTACH_RATIO
        verdict = "misattached" if (shared is not None or deep) else "floor_only"

        # The floor is applied in BOTH branches: it is the whole point of the
        # sweep that no repaired row can still report an average above its peak.
        # Rounded UP, never to-nearest — round(5.9715, 3) can land below the
        # floor it was derived from and leave the row violating (38 of the 110
        # did exactly that on the first pass over the export).
        new_peak = math.ceil(max(true_avg, avg_flow, peak) * 1000.0) / 1000.0

        sets = [
            "peak_flow_lpm_pre_repair = COALESCE(peak_flow_lpm_pre_repair, peak_flow_lpm)",
            "peak_flow_lpm = ?",
            "wf_repair_at = ?",
            "wf_repair_verdict = ?",
        ]
        params: list = [new_peak, now_iso, verdict]

        if verdict == "misattached":
            # Everything else the capture wrote is equally wrong. NULL rather
            # than a reconstructed number: a differently-scaled pressure delta
            # would poison a feature that feeds clustering more quietly than a
            # missing one does (readers already gate on > 0).
            sets += [
                "pressure_delta_psi_pre_repair = COALESCE(pressure_delta_psi_pre_repair, pressure_delta_psi)",
                "propagation_delay_ms_pre_repair = COALESCE(propagation_delay_ms_pre_repair, propagation_delay_ms)",
                "pressure_delta_psi = NULL",
                "propagation_delay_ms = NULL",
                "esp_waveform_used = 0",
                "waveform_event_id = NULL",
                "waveform_boot_id = NULL",
                "waveform_quality = NULL",
                "waveform_overlap_score = NULL",
            ]
            if str(r["signature_source"] or "").startswith("esp"):
                # The signatures came from the wrong capture and the software
                # originals were overwritten in place, so they are gone. Take
                # the event out of training rather than let a wrong shape keep
                # feeding the clusterer. cluster_id is cleared as well because
                # rebuild_from_db replays on cluster_id IS NOT NULL and does
                # not consult excluded_from_training.
                sets += [
                    "excluded_from_training = 1",
                    "match_rejection_reason = COALESCE(match_rejection_reason, "
                    "'wf_enrichment_repaired')",
                    "prev_cluster_id = COALESCE(prev_cluster_id, cluster_id)",
                    "cluster_id = NULL",
                ]
                excluded += 1
                cur = conn.execute(
                    "DELETE FROM event_waveforms WHERE event_id = ?", (r["id"],))
                envelopes += cur.rowcount or 0
            circuits.add(r["circuit"])

        conn.execute(
            f"UPDATE events SET {', '.join(sets)} WHERE id = ?",
            (*params, r["id"]),
        )
        counts[verdict] += 1

    conn.commit()
    log.info(
        "wf-repair: %d misattached (%d excluded from training, %d envelope(s) "
        "dropped), %d floor-only — %d event(s) total",
        counts["misattached"], excluded, envelopes, counts["floor_only"],
        len(rows),
    )
    return {
        "misattached": counts["misattached"],
        "floor_only": counts["floor_only"],
        "excluded": excluded,
        "envelopes_deleted": envelopes,
        "circuits": sorted(circuits),
    }


class WfRepairBackfill:
    """Supervised one-shot: run the repair sweep once after boot, then park.

    Runs on its own write-locked connection (``run_isolated_write``), then
    replays the cluster engine for any circuit whose corpus changed — the
    in-memory scaler and DBSTREAM state were built at startup from the
    corrupted rows and repairing SQLite alone would leave those moments
    carrying a 46x peak outlier until the next restart.
    """

    def __init__(self, db: sqlite3.Connection, cluster_engine=None,
                 db_path=DB_PATH):
        self._db = db
        self._cluster_engine = cluster_engine
        self._db_path = db_path      # injectable for tests
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def _idle(self) -> None:
        """Park until shutdown — _supervise re-invokes a coroutine that returns."""
        await self._stop.wait()

    async def run(self) -> None:
        # Let boot settle: the catch-up import and the startup cluster rebuild
        # both touch the same rows.
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=_STARTUP_DELAY_S)
            return                                   # stopped during the delay
        except asyncio.TimeoutError:
            pass

        try:
            from .database import run_isolated_write
            res = await run_isolated_write(self._db_path,
                                           repair_misattached_waveforms)
        except Exception as e:
            log.error("wf-repair sweep failed: %s", e, exc_info=True)
            await self._idle()
            return

        if res.get("misattached") and self._cluster_engine is not None:
            await self._replay_clusters(res.get("circuits") or [])

        await self._idle()

    async def _replay_clusters(self, circuits) -> None:
        """Rebuild in-memory cluster state so it stops carrying the outliers."""
        import functools
        loop = asyncio.get_running_loop()
        for circuit in circuits:
            if self._stop.is_set():
                return
            try:
                await loop.run_in_executor(
                    None,
                    functools.partial(self._cluster_engine.rebuild_from_db,
                                      circuit))
                log.info("wf-repair: cluster state replayed for %s", circuit)
            except Exception as e:
                log.warning("wf-repair: cluster replay failed for %s "
                            "(non-fatal, next restart recovers): %s", circuit, e)
