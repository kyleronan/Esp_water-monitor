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



# ── dev38: shared-capture sweep ────────────────────────────────────────────────
# Winner-selection mismatch gate (pinned a priori): if even the best
# candidate's |duration − capture_span| exceeds max(0.25·duration, 30 s), the
# rightful owner was likely pruned/merged/zeroed and NO member keeps the claim.
_SHARED_NO_WINNER_FRAC = 0.25
_SHARED_NO_WINNER_MIN_S = 30.0
_ESP_CAPTURE_HZ = 200.0
_SHARED_MIN_ARRAY_CHARS = 300      # matches the audit's LEN>300 fingerprint gate


def repair_shared_captures(conn: sqlite3.Connection) -> Dict[str, Any]:
    """dev38 — repair events that share one ESP capture with another event.

    The 2026-08 audit hashed every stored ``flow_max_json`` and found 269
    groups / 619 events with byte-identical arrays — 615 of them with
    *different* durations, which one draw cannot produce. The dev37 sweep
    missed all of them: its predicate keys on ``true_avg > peak``, but a
    shared capture usually overwrites BOTH events' peaks with the same
    plausible value. Array identity is the decisive test.

    Winner rule (deterministic): within a group, the claim stays with the
    event whose ``|duration_seconds − n_points/200 Hz|`` is smallest; ties
    break on earliest ``start_ts``. Escape hatch: when even the best mismatch
    exceeds ``max(0.25·duration, 30 s)`` the group has NO winner — the
    rightful owner is gone (pruned/merged/zeroed) — and every member is
    de-enriched.

    Loser action set = the dev37 ``misattached`` set, verdict
    ``'shared_capture'``, plus (VERIFIED PROVENANCE, dev38): the flow/
    pressure/edge signatures of an esp-labelled row were regenerated FROM the
    capture arrays, i.e. they are a foreign draw's shape — so they are NULLed
    and ``signature_source`` set NULL, never relabelled 'software' (which
    would launder contaminated shape data under a trusted label). The shared
    envelope row is deleted in all cases (it is contaminated regardless of
    the signature label). ``hydraulic_resistance`` is NULLed with ΔP.

    Self-exhausting without stamping winners: losers lose their envelope
    rows, so a repaired group can never form again.
    """
    try:
        rows = conn.execute(
            """SELECT e.id, e.circuit, e.start_ts, e.duration_seconds,
                      e.peak_flow_lpm, e.true_avg_flow_lpm, e.avg_flow_lpm,
                      e.signature_source, e.cluster_id, e.prev_cluster_id,
                      e.match_rejection_reason, e.wf_repair_verdict,
                      w.flow_max_json
               FROM event_waveforms w
               JOIN events e ON e.id = w.event_id
               WHERE LENGTH(w.flow_max_json) > ?
               ORDER BY e.start_ts""",
            (_SHARED_MIN_ARRAY_CHARS,),
        ).fetchall()
    except sqlite3.Error as e:
        log.debug("wf-shared: candidate query unavailable (%s)", e)
        return {"groups": 0, "losers": 0, "winners": 0, "no_winner_groups": 0,
                "envelopes_deleted": 0, "circuits": []}

    groups: Dict[str, list] = {}
    for r in rows:
        groups.setdefault(r["flow_max_json"], []).append(r)
    shared_groups = [g for g in groups.values() if len(g) > 1]
    if not shared_groups:
        return {"groups": 0, "losers": 0, "winners": 0, "no_winner_groups": 0,
                "envelopes_deleted": 0, "circuits": []}

    now_iso = datetime.now(timezone.utc).isoformat()
    losers = winners = no_winner_groups = envelopes = 0
    circuits: set = set()

    for members in shared_groups:
        n_pts = members[0]["flow_max_json"].count(",") + 1
        span_s = n_pts / _ESP_CAPTURE_HZ

        def _mismatch(m) -> float:
            return abs(float(m["duration_seconds"] or 0.0) - span_s)

        best = min(members, key=lambda m: (_mismatch(m), str(m["start_ts"])))
        dur_best = float(best["duration_seconds"] or 0.0)
        gate = max(_SHARED_NO_WINNER_FRAC * dur_best, _SHARED_NO_WINNER_MIN_S)
        has_winner = _mismatch(best) <= gate
        if not has_winner:
            no_winner_groups += 1

        for m in members:
            if has_winner and m["id"] == best["id"]:
                winners += 1
                continue
            true_avg = float(m["true_avg_flow_lpm"] or 0.0)
            avg_flow = float(m["avg_flow_lpm"] or 0.0)
            peak = float(m["peak_flow_lpm"] or 0.0)
            new_peak = math.ceil(max(true_avg, avg_flow, peak) * 1000.0) / 1000.0

            sets = [
                "peak_flow_lpm_pre_repair = COALESCE(peak_flow_lpm_pre_repair, peak_flow_lpm)",
                "pressure_delta_psi_pre_repair = COALESCE(pressure_delta_psi_pre_repair, pressure_delta_psi)",
                "propagation_delay_ms_pre_repair = COALESCE(propagation_delay_ms_pre_repair, propagation_delay_ms)",
                "peak_flow_lpm = ?",
                "pressure_delta_psi = NULL",
                "propagation_delay_ms = NULL",
                "hydraulic_resistance = NULL",     # ΔP-derived — follows ΔP
                "esp_waveform_used = 0",
                "waveform_event_id = NULL",
                "waveform_boot_id = NULL",
                "waveform_quality = NULL",
                "waveform_overlap_score = NULL",
                "wf_repair_at = COALESCE(wf_repair_at, ?)",
                "wf_repair_verdict = 'shared_capture'",
                "excluded_from_training = 1",
                "match_rejection_reason = COALESCE(match_rejection_reason, "
                "'wf_enrichment_repaired')",
                "prev_cluster_id = COALESCE(prev_cluster_id, cluster_id)",
                "cluster_id = NULL",
            ]
            params: list = [new_peak, now_iso]
            if str(m["signature_source"] or "").startswith("esp"):
                # Foreign shape under an esp label — NULL, never relabel.
                sets += [
                    "flow_signature_json = NULL",
                    "pressure_signature_json = NULL",
                    "onset_signature_json = NULL",
                    "offset_signature_json = NULL",
                    "signature_source = NULL",
                ]
            conn.execute(
                f"UPDATE events SET {', '.join(sets)} WHERE id = ?",
                (*params, m["id"]),
            )
            cur = conn.execute(
                "DELETE FROM event_waveforms WHERE event_id = ?", (m["id"],))
            envelopes += cur.rowcount or 0
            losers += 1
            circuits.add(m["circuit"])

    conn.commit()
    log.info(
        "wf-shared: %d group(s) — %d loser(s) de-enriched (%d envelope(s) "
        "dropped), %d winner(s) kept, %d group(s) with no credible owner",
        len(shared_groups), losers, envelopes, winners, no_winner_groups,
    )
    return {
        "groups": len(shared_groups), "losers": losers, "winners": winners,
        "no_winner_groups": no_winner_groups, "envelopes_deleted": envelopes,
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

        # dev38: one worker chain — the dev37 mis-attachment sweep first (its
        # stamps land before the shared-capture predicate runs), then the
        # array-identity sweep. The tz-feature backfill has already completed
        # by now (it runs at HA-tz detection, seconds after boot; this worker
        # starts at +240 s), so the cluster replay below trains on local-time
        # features.
        affected: set = set()
        try:
            from .database import run_isolated_write
            res = await run_isolated_write(self._db_path,
                                           repair_misattached_waveforms)
            if res.get("misattached"):
                affected.update(res.get("circuits") or [])
        except Exception as e:
            log.error("wf-repair sweep failed: %s", e, exc_info=True)

        try:
            from .database import run_isolated_write
            res2 = await run_isolated_write(self._db_path,
                                            repair_shared_captures)
            if res2.get("losers"):
                affected.update(res2.get("circuits") or [])
        except Exception as e:
            log.error("wf-shared sweep failed: %s", e, exc_info=True)

        if affected and self._cluster_engine is not None:
            await self._replay_clusters(sorted(affected))

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
