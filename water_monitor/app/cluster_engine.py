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
    # Core hydraulic scalars
    'avg_flow_lpm', 'peak_flow_lpm', 'duration_seconds',
    'volume_litres', 'pressure_delta_psi', 'has_pressure_transient',
    'flow_variability', 'hour_sin', 'hour_cos',
    'propagation_delay_ms',
    # Flow shape — 32-point normalized signature
    'flow_sig_00', 'flow_sig_01', 'flow_sig_02', 'flow_sig_03',
    'flow_sig_04', 'flow_sig_05', 'flow_sig_06', 'flow_sig_07',
    'flow_sig_08', 'flow_sig_09', 'flow_sig_10', 'flow_sig_11',
    'flow_sig_12', 'flow_sig_13', 'flow_sig_14', 'flow_sig_15',
    'flow_sig_16', 'flow_sig_17', 'flow_sig_18', 'flow_sig_19',
    'flow_sig_20', 'flow_sig_21', 'flow_sig_22', 'flow_sig_23',
    'flow_sig_24', 'flow_sig_25', 'flow_sig_26', 'flow_sig_27',
    'flow_sig_28', 'flow_sig_29', 'flow_sig_30', 'flow_sig_31',
    # Edge complexity
    'flow_edge_count',
    # Open/close dynamics
    'flow_rise_rate_lpm_s', 'flow_fall_rate_lpm_s',
    'opening_step_lpm', 'closing_step_lpm',
    'time_to_90pct_flow_seconds', 'time_from_90pct_to_zero_seconds',
    # Flow summary stats (steady_state_fraction + mid_event stored; ratio/cv derived)
    'steady_state_fraction', 'mid_event_flow_drop_lpm',
    'peak_to_avg_flow_ratio', 'flow_cv',
    # Compound event signals (already stored in events table)
    'is_composite', 'other_valve_open',
    # Pressure scalars (pre_event/min/resistance already stored; energy/duration new)
    'pre_event_pressure_psi', 'min_pressure_psi', 'hydraulic_resistance',
    'pressure_transient_energy', 'pressure_transient_duration_ms',
    # Pressure transient shape features
    'pressure_onset_ms', 'recovery_overshoot_psi', 'pressure_oscillation_count',
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
        # Phase 2.1 — type-aware match gate.
        # circuit -> {db_cluster_id -> fixture_type}.
        # Populated at startup from confirmed fixtures (see _init_circuit
        # and _refresh_type_cache); mutated live by notify_fixture_confirmed
        # / notify_fixture_removed when the user labels a cluster.
        # Unconfirmed clusters are intentionally absent — match_and_learn
        # bypasses the type gate when the lookup returns None.
        self._type_cache: Dict[str, Dict[int, str]]            = {}

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
        self._type_cache[circuit]   = {}
        row = self._db.execute(
            "SELECT MAX(id) FROM fixture_clusters WHERE circuit = ?",
            (circuit,)
        ).fetchone()
        self._next_cluster_id[circuit] = (row[0] if row[0] is not None else -1) + 1
        self._refresh_type_cache(circuit)

    def _refresh_type_cache(self, circuit: str) -> None:
        """(Re)load the {cluster_id -> fixture_type} map from the DB.

        Called from ``_init_circuit`` at startup and from ``rebuild_from_db``
        as belt-and-braces protection against drift if a future code path
        mutates ``fixtures.confirmed`` without going through
        ``notify_fixture_confirmed`` / ``notify_fixture_removed``.
        """
        cache: Dict[int, str] = {}
        try:
            rows = self._db.execute(
                """SELECT fc.id, f.fixture_type
                   FROM fixture_clusters fc
                   JOIN fixtures f ON fc.fixture_id = f.id
                   WHERE fc.circuit = ?
                     AND f.confirmed = 1
                     AND f.fixture_type IS NOT NULL""",
                (circuit,),
            ).fetchall()
            for r in rows:
                cache[int(r["id"])] = r["fixture_type"]
        except Exception as e:
            log.warning("[%s] _refresh_type_cache failed: %s", circuit, e)
        self._type_cache[circuit] = cache
        if cache:
            log.debug("[%s] type cache: %d confirmed fixtures",
                      circuit, len(cache))

    def reset_circuit(self, circuit: str) -> None:
        """
        Clear all in-memory state for one circuit and re-seed from DB.

        Called by training_manager.start_calibration() when a new
        calibration cycle begins, so DBSTREAM and the scaler don't carry
        over state from the previous run that has just had its
        unconfirmed clusters wiped.

        Confirmed clusters in fixture_clusters (fixture_id IS NOT NULL)
        are unaffected — only the in-memory DBSTREAM, scaler,
        river_id_map and next_cluster_id sequence are reset.  The
        next_cluster_id is re-derived from MAX(id) across surviving
        rows so confirmed cluster IDs don't collide.
        """
        self._init_circuit(circuit)

    # ── Feature extraction ─────────────────────────────────────────────────────

    def _extract_features(self, event: dict) -> Optional[Dict[str, float]]:
        """Build the full feature dict from an event DB row. Returns None if unusable."""
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

        avg_flow = float(event.get('avg_flow_lpm') or 0)
        peak_flow = float(event.get('peak_flow_lpm') or 0)
        variability = float(event.get('flow_variability') or 0)

        features = {
            # Core hydraulic scalars
            'avg_flow_lpm':           avg_flow,
            'peak_flow_lpm':          peak_flow,
            'duration_seconds':       float(event.get('duration_seconds')       or 0),
            'volume_litres':          float(event.get('volume_litres')          or 0),
            'pressure_delta_psi':     float(event.get('pressure_delta_psi')    or 0),
            'has_pressure_transient': float(event.get('has_pressure_transient') or 0),
            'flow_variability':       variability,
            'hour_sin':               hour_sin,
            'hour_cos':               hour_cos,
            'propagation_delay_ms':      float(event.get('propagation_delay_ms')      or 0),
            # Edge complexity
            'flow_edge_count':        float(event.get('flow_edge_count')        or 0),
            # Open/close dynamics
            'flow_rise_rate_lpm_s':   float(event.get('flow_rise_rate_lpm_s')  or 0),
            'flow_fall_rate_lpm_s':   float(event.get('flow_fall_rate_lpm_s')  or 0),
            'opening_step_lpm':       float(event.get('opening_step_lpm')      or 0),
            'closing_step_lpm':       float(event.get('closing_step_lpm')      or 0),
            'time_to_90pct_flow_seconds':      float(event.get('time_to_90pct_flow_seconds')      or 0),
            'time_from_90pct_to_zero_seconds': float(event.get('time_from_90pct_to_zero_seconds') or 0),
            # Flow summary stats
            'steady_state_fraction':  float(event.get('steady_state_fraction') or 0),
            'mid_event_flow_drop_lpm': float(event.get('mid_event_flow_drop_lpm') or 0),
            # Pure derived — computed from already-stored columns, no DB column needed
            'peak_to_avg_flow_ratio': peak_flow / avg_flow if avg_flow > 0 else 0.0,
            'flow_cv':                variability / avg_flow if avg_flow > 0 else 0.0,
            # Compound event signals
            'is_composite':           float(event.get('is_composite')           or 0),
            'other_valve_open':       float(event.get('other_valve_open')       or 0),
            # Pressure scalars
            'pre_event_pressure_psi': float(event.get('pre_event_pressure_psi') or 0),
            'min_pressure_psi':       float(event.get('min_pressure_psi')       or 0),
            'hydraulic_resistance':   float(event.get('hydraulic_resistance')   or 0),
            'pressure_transient_energy':     float(event.get('pressure_transient_energy')     or 0),
            'pressure_transient_duration_ms': float(event.get('pressure_transient_duration_ms') or 0),
            'pressure_onset_ms':             float(event.get('pressure_onset_ms')             or 0),
            'recovery_overshoot_psi':        float(event.get('recovery_overshoot_psi')        or 0),
            'pressure_oscillation_count':    float(event.get('pressure_oscillation_count')    or 0),
        }

        # Expand JSON signature → flow_sig_00 … flow_sig_31
        sig_json = event.get('flow_signature_json')
        if sig_json:
            try:
                sig = json.loads(sig_json)
                for i, v in enumerate(sig[:32]):
                    features[f'flow_sig_{i:02d}'] = float(v)
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        for i in range(32):
            features.setdefault(f'flow_sig_{i:02d}', 0.0)

        return features

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

    # ── Type-aware match gate (Phase 2.1) ──────────────────────────────────────

    def notify_fixture_confirmed(self, circuit: str, cluster_id: int,
                                 fixture_type: str) -> None:
        """Cache invalidation hook — called by routers/fixtures.py after a
        cluster is labelled. Takes effect immediately, no restart required.
        """
        self._type_cache.setdefault(circuit, {})[int(cluster_id)] = fixture_type
        log.info("[%s] type cache: cluster %d → %s",
                 circuit, cluster_id, fixture_type)

    def notify_fixture_removed(self, circuit: str, cluster_id: int) -> None:
        """Cache invalidation hook — called when a fixture/cluster is deleted
        or unconfirmed. The gate falls back to the global threshold for the
        cluster on the next event.
        """
        removed = self._type_cache.get(circuit, {}).pop(int(cluster_id), None)
        if removed is not None:
            log.info("[%s] type cache: cluster %d removed (was %s)",
                     circuit, cluster_id, removed)

    def _build_match_weights(self, fixture_type: str) -> Dict[str, float]:
        """Per-feature weight vector for a fixture type. Default 1.0.

        Anchor features (volume for toilets, flow/pressure for showers) get
        amplified. Float features (duration for showers, hour-of-day for ice
        makers) are zeroed so they don't push the distance over the gate.
        Forward-looking feature names (e.g. ``resistance_curve_shape``) that
        are not yet in FEATURE_KEYS are silently ignored.
        """
        from .fixtures import get_variance_profile
        profile = get_variance_profile(fixture_type)
        weights: Dict[str, float] = {k: 1.0 for k in FEATURE_KEYS}
        for k, w in profile.get("anchor_weights", {}).items():
            if k in weights:
                weights[k] = float(w)
        for k in profile.get("float_features", set()):
            if k in weights:
                weights[k] = 0.0
        return weights

    @staticmethod
    def _weighted_distance(a: Dict[str, float], b: Dict[str, float],
                           weights: Dict[str, float]) -> float:
        """Weighted Euclidean over FEATURE_KEYS. Default weight 1.0.

        Both ``a`` and ``b`` are expected to be in scaled feature space so
        the distance is comparable to ``DBSTREAM_CLUSTERING_THRESHOLD`` and
        the per-type thresholds in ``FIXTURE_MATCH_THRESHOLDS``.
        """
        return math.sqrt(sum(
            weights.get(k, 1.0) * (a.get(k, 0.0) - b.get(k, 0.0)) ** 2
            for k in FEATURE_KEYS
        ))

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
                   (circuit, id, centroid, feature_std,
                    member_count, confidence_level, created_at, last_match_at)
                   VALUES (?, ?, '{}', '{}', 0, 'preliminary', ?, ?)""",
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
        circuit_type = ct_row["circuit_type"] if ct_row else "fixture"
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
    ) -> Tuple[Optional[int], float, str, Optional[str]]:
        """
        Feed one event through DBSTREAM.

        Returns ``(cluster_id, confidence, level, rejection_reason)``.

        * On success: cluster_id is the DB row id, rejection_reason is None.
        * On rejection: cluster_id is None and rejection_reason is one of
          ``'features_missing'``, ``'no_centers'``, ``'type_gate_rejected'``.
          The caller writes ``rejection_reason`` into
          ``events.match_rejection_reason`` so the events page can explain
          why a row has ``cluster_id IS NULL``.

        prev_cluster_id / seconds_since_prev: when provided and the gap is
        within SEQUENCE_GAP_MAX_SECONDS, the cooccurrence table is updated.
        Stage 3 will apply a confidence boost from this table.
        """
        features = self._extract_features(event)
        if features is None:
            return (None, 0.0, '', 'features_missing')

        scaler = self._scalers[circuit]
        scaler.learn_one(features)
        x = scaler.transform_one(features)

        stream = self._streams[circuit]
        stream.learn_one(x)

        if not stream.centers:
            return (None, 0.0, '', 'no_centers')

        nearest_id, distance = self._nearest_center(stream, x)
        if nearest_id is None:
            return (None, 0.0, '', 'no_centers')

        # ── Type-aware gate (Phase 2.1) ────────────────────────────────────
        # If the nearest river center already maps to a confirmed fixture,
        # apply a per-type weighted-distance gate before accepting the match.
        # Unconfirmed clusters bypass this and use the global threshold path.
        candidate_id = self._river_id_map.get(circuit, {}).get(nearest_id)
        fixture_type = (
            self._type_cache.get(circuit, {}).get(candidate_id)
            if candidate_id is not None else None
        )
        if fixture_type:
            try:
                from .fixtures import get_match_threshold
                row = self._db.execute(
                    "SELECT centroid FROM fixture_clusters "
                    "WHERE circuit = ? AND id = ?",
                    (circuit, candidate_id),
                ).fetchone()
                if row and row["centroid"]:
                    db_orig   = json.loads(row["centroid"])
                    db_feat   = {k: float(db_orig.get(k, 0)) for k in FEATURE_KEYS}
                    db_scaled = scaler.transform_one(db_feat)
                    weights   = self._build_match_weights(fixture_type)
                    wdist     = self._weighted_distance(x, db_scaled, weights)
                    threshold = get_match_threshold(fixture_type)
                    if wdist > threshold:
                        log.info(
                            "[%s] event rejected from cluster %d (%s): "
                            "weighted_dist=%.2f > threshold=%.2f",
                            circuit, candidate_id, fixture_type,
                            wdist, threshold,
                        )
                        # Leave event unmatched so backfill_unmatched can
                        # retry it later if a better-fitting cluster appears
                        # OR the threshold is loosened. Critically we do NOT
                        # call _increment_member_count / _update_centroid —
                        # the wrong-fit event must not pollute this fixture's
                        # learned shape.
                        return (None, 0.0, '', 'type_gate_rejected')
            except Exception as e:
                # Fail open: if the gate itself crashes (corrupt JSON,
                # missing column, etc.) we'd rather match than lose the
                # event entirely. The error is logged for follow-up.
                log.warning(
                    "[%s] type-aware gate failed for cluster %s "
                    "(falling through to default match): %s",
                    circuit, candidate_id, e,
                )

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
        return (cluster_id, confidence, level, None)

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

        # Belt-and-braces: re-derive the type cache from the DB after a
        # rebuild so any drift (e.g. a UI/import path that toggled
        # fixtures.confirmed without going through the notify hooks) heals.
        self._refresh_type_cache(circuit)

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
            cluster_id, confidence, level, reason = \
                self.match_and_learn(event, circuit)
            if cluster_id is None:
                # Record why the backfill couldn't place this event so the
                # events page can surface "no_centers" vs.
                # "type_gate_rejected" vs. "features_missing" without us
                # having to re-run match_and_learn for the explanation.
                # Leave cluster_id NULL so a future backfill can retry.
                self._db.execute(
                    "UPDATE events SET match_rejection_reason = ? WHERE id = ?",
                    (reason, event["id"]),
                )
                continue
            self._db.execute(
                """UPDATE events
                   SET cluster_id = ?,
                       match_confidence = ?,
                       match_level = ?,
                       match_rejection_reason = NULL
                   WHERE id = ?""",
                (cluster_id, confidence, level, event["id"])
            )
            count += 1

        if count or rows:
            self._db.commit()
        if count:
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

        # Per-type acceptance bound: confirmed fixtures use their per-type
        # match threshold so a noisy river center can't be re-attached to
        # (e.g.) a confirmed toilet cluster at scaled distance 2.5 — the
        # gate would later reject every legitimate toilet event landing on
        # that mis-mapped river center until DBSTREAM split it.
        from .fixtures import get_match_threshold
        type_cache = self._type_cache.get(circuit, {})

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

            if best_db_id is None:
                continue

            # Confirmed clusters: accept only within the per-type gate.
            # Unconfirmed clusters: keep the historical 2× threshold so
            # behaviour is unchanged for the discovery path.
            ftype = type_cache.get(best_db_id)
            bound = (get_match_threshold(ftype) if ftype
                     else DBSTREAM_CLUSTERING_THRESHOLD * 2)
            if best_dist < bound:
                mapping[river_id] = best_db_id
                log.debug(
                    "[%s] post-rebuild: river cluster %d → DB cluster %d "
                    "(dist=%.3f, bound=%.2f, type=%s)",
                    circuit, river_id, best_db_id, best_dist, bound,
                    ftype or "<unconfirmed>",
                )
