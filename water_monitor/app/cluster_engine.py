"""
Online clustering engine — Phase 2.1 Stage 2.

Per-circuit DBSTREAM + StandardScaler (river library).  Called from
feature_extractor.py after each event is stored.  The orchestrator
instantiates this class on startup and calls rebuild_from_db() to
replay the last 60 days of already-matched events to reconstruct
in-memory state.

State persistence: rebuild from DB, never pickle.  See ADR 008.
Algorithm choice: DBSTREAM, not batch DBSCAN.  See ADR 003.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

from river import cluster, preprocessing

log = logging.getLogger(__name__)

# ── Tunable constants ──────────────────────────────────────────────────────────
SEQUENCE_GAP_MAX_SECONDS      = 300
# Stage 3: multiply candidate confidence by this when cooccurrence count >= 10
SEQUENCE_BOOST_WEIGHT         = 1.5
DBSTREAM_CLUSTERING_THRESHOLD = 1.5
FADING_FACTOR                 = 0.05
DTW_TEMPLATE_MIN_MEMBERS      = 10   # Stage 3
DTW_DISTANCE_WEIGHT           = 0.4  # Stage 3
LEVEL_PRELIMINARY_MAX         = 50
LEVEL_LEARNING_MAX            = 200
METRICS_WINDOW_HOURS          = 24

# ── Stage 3 hook: DTW transient templates ─────────────────────────────────────
# dtaidistance is installed (see Dockerfile).  Stage 3 will add:
#   - transient_template stored per cluster once member_count >= DTW_TEMPLATE_MIN_MEMBERS
#   - at match time: dtw_dist = dtaidistance.dtw(event_transient, template)
#   - final confidence = (1 - DTW_DISTANCE_WEIGHT) * feature_conf
#                       + DTW_DISTANCE_WEIGHT * exp(-dtw_dist / scale)
# Nothing below touches dtaidistance yet.

FEATURE_KEYS = [
    'avg_flow_lpm', 'peak_flow_lpm', 'duration_seconds',
    'volume_litres', 'pressure_delta_psi', 'has_pressure_transient',
    'flow_variability', 'hour_sin', 'hour_cos',
]


class ClusterEngine:
    """Per-circuit DBSTREAM clustering engine."""

    def __init__(self, db, cfg):
        self._db  = db
        self._cfg = cfg
        self._streams: Dict[str, cluster.DBSTREAM]             = {}
        self._scalers: Dict[str, preprocessing.StandardScaler] = {}
        self._next_cluster_id: Dict[str, int]                  = {}
        # In-memory map: circuit -> {river_internal_id -> db_cluster_id}
        # Rebuilt from centroid similarity after each rebuild_from_db().
        self._river_id_map: Dict[str, Dict[int, int]]          = {}

        for c in cfg.circuits:
            self._init_circuit(c.circuit)

    # ── Initialisation ─────────────────────────────────────────────────────────

    def _init_circuit(self, circuit: str) -> None:
        self._streams[circuit] = cluster.DBSTREAM(
            clustering_threshold=DBSTREAM_CLUSTERING_THRESHOLD,
            fading_factor=FADING_FACTOR,
        )
        self._scalers[circuit] = preprocessing.StandardScaler()
        self._river_id_map[circuit] = {}
        row = self._db.execute(
            "SELECT MAX(id) FROM fixture_clusters WHERE circuit = ?",
            (circuit,)
        ).fetchone()
        self._next_cluster_id[circuit] = (row[0] or -1) + 1

    # ── Feature extraction ─────────────────────────────────────────────────────

    def _extract_features(self, event: dict) -> Optional[Dict[str, float]]:
        """Build the 9-feature dict from an event row.  Returns None if unusable."""
        if event.get('avg_flow_lpm') is None or not event.get('duration_seconds'):
            return None
        start_ts = event.get('start_ts')
        if start_ts:
            try:
                hour = datetime.fromisoformat(str(start_ts)).hour
            except (ValueError, TypeError):
                hour = 0
        else:
            hour = 0
        hour_sin = math.sin(2 * math.pi * hour / 24)
        hour_cos = math.cos(2 * math.pi * hour / 24)
        return {
            'avg_flow_lpm':           float(event.get('avg_flow_lpm')           or 0),
            'peak_flow_lpm':          float(event.get('peak_flow_lpm')          or 0),
            'duration_seconds':       float(event.get('duration_seconds')       or 0),
            'volume_litres':          float(event.get('volume_litres')          or 0),
            'pressure_delta_psi':     float(event.get('pressure_delta_psi')    or 0),
            'has_pressure_transient': float(event.get('has_pressure_transient') or 0),
            'flow_variability':       float(event.get('flow_variability')       or 0),
            'hour_sin':               hour_sin,
            'hour_cos':               hour_cos,
        }

    # ── Cluster confidence ─────────────────────────────────────────────────────

    @staticmethod
    def _confidence_level(member_count: int) -> str:
        if member_count < LEVEL_PRELIMINARY_MAX:
            return 'preliminary'
        if member_count < LEVEL_LEARNING_MAX:
            return 'learning'
        return 'confirmed'

    # ── Nearest-centre lookup ──────────────────────────────────────────────────

    @staticmethod
    def _nearest_center(stream, x: dict) -> Tuple[Optional[int], float]:
        best_id, best_dist = None, float('inf')
        for cid, center in stream.centers.items():
            dist = math.sqrt(sum(
                (x.get(k, 0.0) - center.get(k, 0.0)) ** 2
                for k in FEATURE_KEYS
            ))
            if dist < best_dist:
                best_id, best_dist = cid, dist
        return best_id, best_dist

    # ── DB helpers ─────────────────────────────────────────────────────────────

    def _upsert_cluster(self, circuit: str, river_id: int) -> int:
        """Return the stable DB cluster ID for a river internal ID.
        Allocates a new DB row on first occurrence."""
        mapping = self._river_id_map[circuit]
        if river_id not in mapping:
            our_id = self._next_cluster_id[circuit]
            now = datetime.now(timezone.utc).isoformat()
            cursor = self._db.execute(
                """INSERT OR IGNORE INTO fixture_clusters
                   (circuit, id, member_count, confidence_level, created_at, last_match_at)
                   VALUES (?, ?, 0, 'preliminary', ?, ?)""",
                (circuit, our_id, now, now)
            )
            if cursor.rowcount > 0:
                # INSERT succeeded — claim this ID
                self._next_cluster_id[circuit] += 1
                mapping[river_id] = our_id
            else:
                # Row already existed (shouldn't normally happen); don't ghost the counter.
                # Use the existing DB row that matches our_id rather than creating a duplicate.
                log.warning(
                    "[%s] _upsert_cluster: INSERT OR IGNORE skipped for id=%d — "
                    "mapping to existing row",
                    circuit, our_id,
                )
                mapping[river_id] = our_id
        return mapping[river_id]

    def _increment_member_count(self, circuit: str, cluster_id: int) -> int:
        """Increment member_count, update confidence_level, return new count."""
        now = datetime.now(timezone.utc).isoformat()
        self._db.execute(
            """UPDATE fixture_clusters
               SET member_count  = member_count + 1,
                   last_match_at = ?,
                   confidence_level = CASE
                     WHEN member_count + 1 < ? THEN 'preliminary'
                     WHEN member_count + 1 < ? THEN 'learning'
                     ELSE 'confirmed'
                   END
               WHERE circuit = ? AND id = ?""",
            (now, LEVEL_PRELIMINARY_MAX, LEVEL_LEARNING_MAX, circuit, cluster_id)
        )
        row = self._db.execute(
            "SELECT member_count FROM fixture_clusters WHERE circuit = ? AND id = ?",
            (circuit, cluster_id)
        ).fetchone()
        return row[0] if row else 1

    def _update_cluster_centroid(self, circuit: str, cluster_id: int,
                                 features: dict, member_count: int) -> None:
        """Update the stored centroid as a running mean in original feature space."""
        n_old = member_count - 1
        if n_old <= 0:
            new_centroid = dict(features)
        else:
            row = self._db.execute(
                "SELECT centroid FROM fixture_clusters WHERE circuit = ? AND id = ?",
                (circuit, cluster_id)
            ).fetchone()
            try:
                old_centroid = json.loads(row["centroid"]) if row and row["centroid"] else {}
            except (json.JSONDecodeError, TypeError):
                old_centroid = {}
            new_centroid = {
                k: (old_centroid.get(k, 0.0) * n_old + v) / member_count
                for k, v in features.items()
            }
        self._db.execute(
            "UPDATE fixture_clusters SET centroid = ? WHERE circuit = ? AND id = ?",
            (json.dumps(new_centroid), circuit, cluster_id)
        )

    def _run_suggest_type_if_needed(self, circuit: str, cluster_id: int,
                                    member_count: int) -> None:
        """Call suggest_fixture_type on the centroid at event 1 and every 10 events."""
        if member_count != 1 and member_count % 10 != 0:
            return
        row = self._db.execute(
            "SELECT centroid FROM fixture_clusters WHERE circuit = ? AND id = ?",
            (circuit, cluster_id)
        ).fetchone()
        if not row or not row["centroid"]:
            return
        try:
            centroid = json.loads(row["centroid"])
        except (json.JSONDecodeError, TypeError):
            return
        ct_row = self._db.execute(
            "SELECT circuit_type FROM circuit_profile WHERE circuit = ?",
            (circuit,)
        ).fetchone()
        circuit_type = ct_row["circuit_type"] if ct_row else "main"
        try:
            from .fixtures import suggest_fixture_type
            suggested_type, confidence = suggest_fixture_type(centroid, circuit_type)
            self._db.execute(
                """UPDATE fixture_clusters
                   SET suggested_type = ?, suggested_confidence = ?
                   WHERE circuit = ? AND id = ?""",
                (suggested_type, confidence, circuit, cluster_id)
            )
        except Exception as e:
            log.warning("[%s] suggest_fixture_type failed: %s", circuit, e)

    def _update_cooccurrence(self, circuit: str, from_id: int, to_id: int,
                             gap_seconds: float) -> None:
        """Record a cluster→cluster transition in the cooccurrence table.
        Uses a running mean for median_gap_seconds (approximation; exact median
        would require storing all gaps, which is not worth the cost here).
        Stage 3 will read this table to apply a confidence boost when a
        candidate cluster frequently follows the previous event's cluster.
        """
        now = datetime.now(timezone.utc).isoformat()
        try:
            self._db.execute(
                """INSERT INTO cluster_cooccurrence
                       (circuit, from_cluster_id, to_cluster_id, count,
                        median_gap_seconds, last_seen_at)
                   VALUES (?, ?, ?, 1, ?, ?)
                   ON CONFLICT (circuit, from_cluster_id, to_cluster_id) DO UPDATE SET
                       count              = count + 1,
                       median_gap_seconds = (median_gap_seconds * count + ?) / (count + 1),
                       last_seen_at       = excluded.last_seen_at""",
                (circuit, from_id, to_id, gap_seconds, now, gap_seconds)
            )
        except Exception as e:
            log.warning("[%s] cooccurrence update failed: %s", circuit, e)

    # ── Core: match and learn ──────────────────────────────────────────────────

    def match_and_learn(
        self,
        event: dict,
        circuit: str,
        prev_cluster_id: Optional[int] = None,
        seconds_since_prev: Optional[float] = None,
    ) -> Tuple[Optional[int], float, str]:
        """
        Feed one event through DBSTREAM.  Returns (cluster_id, confidence, level).
        cluster_id is None if features are missing or DBSTREAM has no centres yet.

        prev_cluster_id / seconds_since_prev: when provided and the gap is within
        SEQUENCE_GAP_MAX_SECONDS, the cooccurrence table is updated.
        Stage 3 will apply a confidence boost from this table.
        """
        features = self._extract_features(event)
        if features is None:
            return (None, 0.0, '')

        scaler = self._scalers[circuit]
        scaler.learn_one(features)
        x = scaler.transform_one(features)

        stream = self._streams[circuit]
        stream.learn_one(x)

        if not stream.centers:
            return (None, 0.0, '')

        nearest_id, distance = self._nearest_center(stream, x)
        if nearest_id is None:
            return (None, 0.0, '')

        confidence = math.exp(-distance / DBSTREAM_CLUSTERING_THRESHOLD)
        # Stage 3: multiply confidence by SEQUENCE_BOOST_WEIGHT when
        # cooccurrence count for (prev_cluster_id → cluster_id) >= 10.

        cluster_id   = self._upsert_cluster(circuit, nearest_id)
        member_count = self._increment_member_count(circuit, cluster_id)
        self._update_cluster_centroid(circuit, cluster_id, features, member_count)
        self._run_suggest_type_if_needed(circuit, cluster_id, member_count)

        # Record cooccurrence transition (write path; boost applied in Stage 3)
        if (prev_cluster_id is not None
                and seconds_since_prev is not None
                and seconds_since_prev < SEQUENCE_GAP_MAX_SECONDS):
            self._update_cooccurrence(
                circuit, prev_cluster_id, cluster_id, seconds_since_prev
            )

        self._db.commit()

        level = self._confidence_level(member_count)
        log.info(
            "[%s] matched cluster %d (confidence=%.2f, level=%s, members=%d)",
            circuit, cluster_id, confidence, level, member_count,
        )
        return (cluster_id, confidence, level)

    # ── Startup rebuild ────────────────────────────────────────────────────────

    def rebuild_from_db(self, circuit: str, days: int = 60) -> int:
        """
        Replay recent matched events to reconstruct DBSTREAM + scaler state.
        Called once per circuit at startup (via run_in_executor).
        Does not modify the database — DB rows are already correct.

        After replaying, attempts to rebuild the river→DB ID mapping by
        comparing each DBSTREAM centre to the stored centroids so that
        new events continue updating existing clusters rather than creating
        duplicates.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self._db.execute(
            """SELECT * FROM events
               WHERE circuit = ? AND end_ts >= ? AND cluster_id IS NOT NULL
               ORDER BY end_ts ASC""",
            (circuit, cutoff)
        ).fetchall()

        count = 0
        for row in rows:
            features = self._extract_features(dict(row))
            if features is None:
                continue
            scaler = self._scalers[circuit]
            scaler.learn_one(features)
            x = scaler.transform_one(features)
            self._streams[circuit].learn_one(x)
            count += 1

        if count > 0:
            self._rebuild_id_map_from_centroids(circuit)

        log.info("Restored %d events into circuit '%s' state", count, circuit)
        return count

    def backfill_unmatched(self, circuit: str) -> int:
        """
        Assign cluster_id to events that were collected before the DBSTREAM
        engine existed (v0.1.x upgrades) or before a full recalibration.
        Processes cluster_id IS NULL events in chronological order, feeding
        each through match_and_learn and writing the result back.

        Safe to call multiple times — only processes unmatched rows.
        Called from orchestrator after rebuild_from_db and from
        training_manager after calibration completes.
        """
        rows = self._db.execute(
            """SELECT * FROM events
               WHERE circuit = ? AND cluster_id IS NULL
                 AND excluded_from_training = 0
                 AND end_ts IS NOT NULL
               ORDER BY start_ts ASC""",
            (circuit,)
        ).fetchall()

        if not rows:
            return 0

        count = 0
        for row in rows:
            event = dict(row)
            cluster_id, confidence, level = self.match_and_learn(event, circuit)
            if cluster_id is None:
                continue
            self._db.execute(
                """UPDATE events
                   SET cluster_id = ?, match_confidence = ?, match_level = ?
                   WHERE id = ?""",
                (cluster_id, confidence, level, event["id"])
            )
            count += 1

        if count:
            self._db.commit()
            log.info("[%s] backfill_unmatched: assigned cluster_id to %d events",
                     circuit, count)
        return count

    def _rebuild_id_map_from_centroids(self, circuit: str) -> None:
        """
        After rebuild_from_db, the river→DB ID map is empty.  Match each
        DBSTREAM centre to the nearest stored DB centroid (in scaled space)
        to avoid creating duplicate cluster rows for new events.
        """
        stream = self._streams[circuit]
        scaler = self._scalers[circuit]
        if not stream.centers:
            return

        db_rows = self._db.execute(
            """SELECT id, centroid FROM fixture_clusters
               WHERE circuit = ? AND centroid IS NOT NULL""",
            (circuit,)
        ).fetchall()
        if not db_rows:
            return

        mapping = self._river_id_map[circuit]

        for river_id, river_center in stream.centers.items():
            if river_id in mapping:
                continue
            best_db_id, best_dist = None, float('inf')
            for db_row in db_rows:
                try:
                    db_orig = json.loads(db_row["centroid"])
                    db_feat = {k: float(db_orig.get(k, 0)) for k in FEATURE_KEYS}
                    db_scaled = scaler.transform_one(db_feat)
                except Exception:
                    continue
                dist = math.sqrt(sum(
                    (river_center.get(k, 0.0) - db_scaled.get(k, 0.0)) ** 2
                    for k in FEATURE_KEYS
                ))
                if dist < best_dist:
                    best_db_id, best_dist = int(db_row["id"]), dist

            # Only accept match if within 2× threshold to avoid false positives
            if best_db_id is not None and best_dist < DBSTREAM_CLUSTERING_THRESHOLD * 2:
                mapping[river_id] = best_db_id
                log.debug(
                    "[%s] post-rebuild: river cluster %d → DB cluster %d (dist=%.3f)",
                    circuit, river_id, best_db_id, best_dist,
                )
