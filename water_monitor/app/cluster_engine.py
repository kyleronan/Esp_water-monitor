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
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

# `river` is a runtime dependency of ClusterEngine but not of the rest of
# the package, and importing it eagerly pulls in scipy — ~1.3 s of import
# cost. Nothing outside ClusterEngine needs it: feature_extractor imports
# this module only for a constant, and most tests never instantiate the
# engine. So defer the import to ClusterEngine.__init__ via _load_river();
# importing this module (or feature_extractor, or event_detector) no longer
# pays the river/scipy cost, which is the single biggest chunk of the test
# suite's fixed startup tax. It also keeps collection working where river
# isn't installed (local runs without the add-on's full Dockerfile pip set):
# only code that actually constructs the engine raises a clear error.
cluster = None                         # populated by _load_river()
preprocessing = None                   # populated by _load_river()
_RIVER_IMPORT_ERROR: Optional[BaseException] = None


def _load_river() -> None:
    """Import river on first ClusterEngine construction; cache the result.

    Idempotent: after the first call ``cluster``/``preprocessing`` are bound
    (success) or ``_RIVER_IMPORT_ERROR`` is set (failure), and subsequent
    calls are a cheap module-global check.
    """
    global cluster, preprocessing, _RIVER_IMPORT_ERROR
    if cluster is not None or _RIVER_IMPORT_ERROR is not None:
        return                         # already attempted
    try:
        from river import cluster as _cluster, preprocessing as _preprocessing
        cluster = _cluster
        preprocessing = _preprocessing
    except ImportError as _exc:        # pragma: no cover - import guard
        _RIVER_IMPORT_ERROR = _exc

log = logging.getLogger(__name__)

# ── Tunable constants ──────────────────────────────────────────────────────────
SEQUENCE_GAP_MAX_SECONDS      = 300
# Stage 3: multiply candidate confidence by this when cooccurrence count >= 10
SEQUENCE_BOOST_WEIGHT         = 1.5
DBSTREAM_CLUSTERING_THRESHOLD = 2.0
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

# Points in the signature feature block. Matches feature_extractor's
# SIGNATURE_POINTS for NEW events; stored signatures of any other length
# (historical 32/64-pt rows) are linearly resampled to this length at feature-
# expansion time, so old and new events remain directly comparable.
SIG_POINTS = 256

FEATURE_KEYS = [
    # Core hydraulic scalars
    'avg_flow_lpm', 'peak_flow_lpm', 'duration_seconds',
    'volume_litres', 'pressure_delta_psi', 'has_pressure_transient',
    'flow_variability', 'hour_sin', 'hour_cos',
    'propagation_delay_ms',
] + [
    # Flow shape — SIG_POINTS-point normalized signature
    f'flow_sig_{i:02d}' for i in range(SIG_POINTS)
] + [
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
] + [
    # Pressure drop signature — SIG_POINTS-point normalized drop curve
    f'pressure_sig_{i:02d}' for i in range(SIG_POINTS)
]

# Every pressure-derived dimension, for the pump-era "pressure-blind" cluster
# space (dev34 B2). Under a VFD constant-pressure pump the supply servos
# pressure flat, so ΔP measures pump droop-and-recovery instead of the fixture:
# corr(ΔP, peak_flow²) fell 0.721 → 0.060 across the 2026-07 pump install, and
# the class-matched F-ratio of the pressure-signature block — 36.6% of the
# cluster distance, its largest single share — fell 5.53 → 2.15 (noise = 1.0)
# while flow shape held at 6.38. Re-seeding on pressure features would also
# mean re-seeding again at every pump setpoint change. propagation_delay_ms
# rides along: it is a pressure-arrival measure (and was already ~noise,
# F ≈ 0.2). has_pressure_transient stays — valve slam is a fixture property.
PRESSURE_FEATURE_KEYS: frozenset = frozenset(
    {'pressure_delta_psi', 'pre_event_pressure_psi', 'min_pressure_psi',
     'hydraulic_resistance', 'pressure_transient_energy',
     'pressure_transient_duration_ms', 'pressure_onset_ms',
     'recovery_overshoot_psi', 'pressure_oscillation_count',
     'propagation_delay_ms'}
    | {f'pressure_sig_{i:02d}' for i in range(SIG_POINTS)})

# Per-dimension weights for weighted Euclidean distance.
# Pressure shape > flow shape; scalars default 1.0; hour sinusoids 0.2.
# Per-dim values scale inversely with SIG_POINTS so the TOTAL shape weight in
# the distance is unchanged across the 32→64→256 widenings: 32-pt era 0.4/0.8,
# 64-pt 0.2/0.4, 256-pt 0.05/0.1 (×4 dims ⇒ ÷4 per dim).
BASE_FEATURE_WEIGHTS: Dict[str, float] = {k: 1.0 for k in FEATURE_KEYS}
for _i in range(SIG_POINTS):
    BASE_FEATURE_WEIGHTS[f'flow_sig_{_i:02d}']     = 0.05
    BASE_FEATURE_WEIGHTS[f'pressure_sig_{_i:02d}'] = 0.1
BASE_FEATURE_WEIGHTS['hour_sin'] = 0.2
BASE_FEATURE_WEIGHTS['hour_cos'] = 0.2

# Discrimination tuning — derived from a labelled-event analysis (18 events,
# 6 fixtures, 3 types). Flow rate was by far the strongest type discriminator
# (F-ratio ~55-66); time-to-peak-flow and flow edge count were the next tier
# (~8-9); propagation delay was the weakest feature measured (F-ratio ~0.2 —
# effectively noise, swinging 1.6-3.6 s across flushes of one toilet).
BASE_FEATURE_WEIGHTS['avg_flow_lpm']               = 2.0
BASE_FEATURE_WEIGHTS['peak_flow_lpm']              = 2.0
BASE_FEATURE_WEIGHTS['time_to_90pct_flow_seconds'] = 1.5
BASE_FEATURE_WEIGHTS['flow_edge_count']            = 1.5
BASE_FEATURE_WEIGHTS['propagation_delay_ms']       = 0.1


def _resample_sig(points, n):
    """Linearly resample a signature to exactly n points (identity if already n)."""
    if len(points) == n:
        return points
    if not points:
        return [0.0] * n
    if len(points) == 1:
        return [points[0]] * n
    out = []
    for i in range(n):
        pos = i * (len(points) - 1) / (n - 1)
        lo = int(pos)
        hi = min(lo + 1, len(points) - 1)
        out.append(points[lo] * (1 - (pos - lo)) + points[hi] * (pos - lo))
    return out


def _load_centroid(text) -> dict:
    """json.loads a stored centroid and resample its sig blocks to SIG_POINTS.

    Centroids written before the 32→64 signature change carry flow_sig_00..31 /
    pressure_sig_00..31; comparing them raw against a 64-pt event would zero-
    fill dims 32..63 and inflate the distance. Raises on invalid JSON — callers
    keep their existing error handling."""
    centroid = json.loads(text)
    for prefix in ('flow_sig', 'pressure_sig'):
        keys = sorted(k for k in centroid if k.startswith(prefix + '_'))
        if len(keys) == SIG_POINTS:
            continue
        vals = _resample_sig([float(centroid[k]) for k in keys], SIG_POINTS)
        for k in keys:
            del centroid[k]
        for i, v in enumerate(vals):
            centroid[f'{prefix}_{i:02d}'] = v
    return centroid


class ClusterEngine:
    """Per-circuit DBSTREAM clustering engine."""

    def __init__(self, db, cfg):
        _load_river()
        if _RIVER_IMPORT_ERROR is not None:
            raise RuntimeError(
                "ClusterEngine requires the 'river' package, which is not "
                "installed in this environment. Install it (it is part of "
                "the add-on Dockerfile's pip set) before constructing the "
                f"engine. Original ImportError: {_RIVER_IMPORT_ERROR}"
            )
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
        # Phase 1 hard lock: circuit -> frozen?  None/absent = derive lazily from
        # the training state (frozen iff 'live'). freeze_circuit/unfreeze_circuit
        # set it explicitly on lifecycle transitions.
        self._frozen: Dict[str, bool]                          = {}
        # Protects merge + rebuild sequences so concurrent executor threads
        # cannot read stale river→DB ID maps while a merge is in progress.
        self._merge_lock = threading.Lock()
        # Phase 5 §2.3 — count type-gate crashes. The gate now fails CLOSED (a crash
        # rejects the match rather than silently accepting a possibly-wrong one), so
        # this surfaces a gate that's quietly erroring on every event.
        self._type_gate_errors: Dict[str, int] = {}
        # dev34 B2 — circuit -> pressure-blind feature mode (see
        # PRESSURE_FEATURE_KEYS). Loaded from training_state in _init_circuit.
        self._pressure_blind: Dict[str, bool] = {}
        # dev39 — count frozen matches landing on river centers the id-map
        # rebuild failed to attach (rejection 'unmapped_center'). Nonzero and
        # climbing = the restart desync; used to rate-limit the warning.
        self._unmapped_center_hits: Dict[str, int] = {}
        # dev42 (F-C1) — circuits with a reseed replay in flight. While set,
        # live match_and_learn calls DEFER (store NULL + 'reseed_deferred')
        # instead of matching a partially rebuilt model; the reseed flushes
        # them through the completed model after freeze. Never stores a
        # wrong id — annotate-don't-modify favors no id over a bad one.
        self._reseed_active: Dict[str, bool] = {}

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
        # dev34 B2 — the feature mode is persisted so a restart rebuilds the
        # SAME space the centers were seeded in. A pressure-blind center set
        # replayed with pressure features on (or vice versa) is silent
        # nonsense: every distance shifts and the id-map rebuild mismatches.
        try:
            r = self._db.execute(
                "SELECT cluster_features_mode FROM training_state "
                "WHERE circuit = ?", (circuit,)).fetchone()
            self._pressure_blind[circuit] = bool(
                r and r["cluster_features_mode"] == "pressure_blind")
        except Exception:
            self._pressure_blind[circuit] = False
        if self._pressure_blind.get(circuit):
            log.info("[%s] cluster space is pressure-blind (pump-era seed)",
                     circuit)

    def set_pressure_blind(self, circuit: str, on: bool) -> None:
        """Persist + apply the cluster feature mode. The caller MUST rebuild
        the circuit's cluster state afterwards (reset + replay) — flipping the
        mode under live centers changes every distance."""
        self._db.execute(
            "UPDATE training_state SET cluster_features_mode = ? "
            "WHERE circuit = ?",
            ("pressure_blind" if on else "full", circuit))
        self._db.commit()
        self._pressure_blind[circuit] = on
        log.info("[%s] cluster feature mode -> %s", circuit,
                 "pressure_blind" if on else "full")

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

    def _extract_features(self, event: dict,
                          circuit: Optional[str] = None
                          ) -> Optional[Dict[str, float]]:
        """Build the full feature dict from an event DB row. Returns None if unusable."""
        if event.get('avg_flow_lpm') is None or not event.get('duration_seconds'):
            return None
        start_ts = event.get('start_ts')
        if start_ts:
            try:
                # dev38: stored start_ts is UTC — convert to the HOME timezone
                # before taking .hour (second instance of the audit's UTC
                # time-feature bug; naive timestamps are treated as UTC, the
                # storage convention).
                _dt = datetime.fromisoformat(str(start_ts))
                if _dt.tzinfo is None:
                    _dt = _dt.replace(tzinfo=timezone.utc)
                from .event_rules import get_home_timezone
                hour = _dt.astimezone(get_home_timezone() or timezone.utc).hour
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
            # dev41: `or 0` DELIBERATELY conflates NULL (unknown) with 0
            # (closed) — splitting into two features (value + known) changes
            # the clustering feature space, which requires a full re-seed
            # (the dev39 outage was a feature-space shift). A future split is
            # reseed-gated: it may only ship in the same build as a pending
            # re-seed, never standalone.
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

        # Expand JSON signatures → flow_sig_* / pressure_sig_* (SIG_POINTS each).
        # Stored signatures may be any length (historical rows are 32-pt) —
        # resample to SIG_POINTS so old and new events share one feature space.
        for prefix, col in (('flow_sig', 'flow_signature_json'),
                            ('pressure_sig', 'pressure_signature_json')):
            sig_json = event.get(col)
            if sig_json:
                try:
                    sig = [float(v) for v in json.loads(sig_json)]
                    for i, v in enumerate(_resample_sig(sig, SIG_POINTS)):
                        features[f'{prefix}_{i:02d}'] = v
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
            for i in range(SIG_POINTS):
                features.setdefault(f'{prefix}_{i:02d}', 0.0)

        # dev34 B2 — pressure-blind mode: every pressure-derived dimension is
        # pinned to 0.0 for EVERY event, so the block carries no variance and
        # no distance. Zeroing at extraction (not in the weights) is what makes
        # it reach DBSTREAM's internal clustering, which ignores the weight
        # table.
        key = circuit if circuit is not None else str(event.get('circuit') or '')
        if self._pressure_blind.get(key):
            for k in PRESSURE_FEATURE_KEYS:
                features[k] = 0.0

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
        weights: Dict[str, float] = dict(BASE_FEATURE_WEIGHTS)
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
                old_centroid = _load_centroid(row["centroid"]) if row and row["centroid"] else {}
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
        """Call suggest_fixture_type on the centroid at event 1 and every 10 events.

        If the suggested type changes to a non-None value, schedules an
        auto-merge pass so sibling clusters of the same type are consolidated.
        """
        if member_count != 1 and member_count % 10 != 0:
            return
        row = self._db.execute(
            "SELECT centroid, suggested_type FROM fixture_clusters "
            "WHERE circuit = ? AND id = ?",
            (circuit, cluster_id)
        ).fetchone()
        if not row or not row["centroid"]:
            return
        try:
            centroid = _load_centroid(row["centroid"])
        except (json.JSONDecodeError, TypeError):
            return
        ct_row = self._db.execute(
            "SELECT circuit_type FROM circuit_profile WHERE circuit = ?",
            (circuit,)
        ).fetchone()
        circuit_type = ct_row["circuit_type"] if ct_row else "fixture"
        try:
            from .fixtures import suggest_fixture_type
            old_type = row["suggested_type"]
            suggested_type, confidence = suggest_fixture_type(centroid, circuit_type)
            # Sprint B: only overwrite when the existing suggestion was
            # NULL or heuristic. A user-labels suggestion is a stronger
            # signal than the centroid heuristic and must not be silently
            # replaced when the heuristic runs every 10 events.
            current_src_row = self._db.execute(
                "SELECT suggestion_source FROM fixture_clusters "
                "WHERE circuit = ? AND id = ?",
                (circuit, cluster_id)
            ).fetchone()
            current_src = current_src_row["suggestion_source"] if current_src_row else None
            if current_src == "user_labels":
                # Leave the user-labels suggestion in place. Skip the
                # auto-merge trigger too — the user's call wins.
                return
            self._db.execute(
                """UPDATE fixture_clusters
                   SET suggested_type = ?, suggested_confidence = ?,
                       suggestion_source = CASE WHEN ? IS NOT NULL
                                                THEN 'heuristic'
                                                ELSE NULL END
                   WHERE circuit = ? AND id = ?""",
                (suggested_type, confidence, suggested_type,
                 circuit, cluster_id)
            )
            # If the type is newly assigned, try to merge sibling clusters.
            if suggested_type and suggested_type != old_type:
                try:
                    self.auto_merge_same_type_clusters(circuit)
                except Exception as merge_exc:
                    log.warning(
                        "[%s] auto_merge after type suggestion failed (non-fatal): %s",
                        circuit, merge_exc,
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

    # ── Freeze / unfreeze (Phase 1 hard lock) ───────────────────────────────────
    # A circuit FREEZES once it reaches the 'live' training state: the reference
    # (scaler + DBSTREAM centers + cluster centroids) stops adapting and the engine
    # only MATCHES against the locked reference — this is what stops a post-
    # activation event (incl. a slow leak) from being silently learned as "normal".
    # Per the plan's state→{frozen|unfrozen} table: unfrozen for every pre-'live'
    # state (idle / calibrating / labelling), frozen once 'live'; fault/disabled
    # inherit (no transition ⇒ no change). freeze_circuit/unfreeze_circuit set it
    # on lifecycle transitions; the lazy fallback reads training_state so a fresh
    # process is correct without waiting for a transition.

    def freeze_circuit(self, circuit: str) -> None:
        """Lock the circuit: match-only, no learning (called at activation)."""
        self._frozen[circuit] = True
        log.info("[%s] cluster engine frozen (locked reference)", circuit)

    def unfreeze_circuit(self, circuit: str) -> None:
        """Unlock so a restarted learning period can adapt again (recalibration)."""
        self._frozen[circuit] = False
        log.info("[%s] cluster engine unfrozen (learning resumed)", circuit)

    def type_gate_error_count(self, circuit: str) -> int:
        """Total type-gate crashes for a circuit (fail-closed rejections, §2.3)."""
        return self._type_gate_errors.get(circuit, 0)

    def _is_frozen(self, circuit: str) -> bool:
        cached = self._frozen.get(circuit)
        if cached is not None:
            return cached
        frozen = False
        try:
            row = self._db.execute(
                "SELECT state FROM training_state WHERE circuit = ?",
                (circuit,),
            ).fetchone()
            frozen = bool(row and row["state"] == "live")
        except Exception as e:
            log.debug("[%s] freeze-state lookup failed (assuming unfrozen): %s",
                      circuit, e)
        self._frozen[circuit] = frozen
        return frozen

    def _match_frozen(
        self, features: dict, circuit: str,
    ) -> Tuple[Optional[int], float, str, Optional[str]]:
        """Match-only path for a locked circuit: classify against the frozen
        reference WITHOUT learning. No scaler.learn_one, no stream.learn_one, and
        no centroid / member-count / cooccurrence writes — so a post-activation
        event can never reshape what 'normal' means."""
        scaler = self._scalers[circuit]
        x = scaler.transform_one(features)          # transform only — NOT learn_one
        stream = self._streams[circuit]
        if not stream.centers:
            return (None, 0.0, '', 'no_centers')
        nearest_id, distance = self._nearest_center(stream, x)
        if nearest_id is None:
            return (None, 0.0, '', 'no_centers')

        candidate_id = self._river_id_map.get(circuit, {}).get(nearest_id)
        if candidate_id is None:
            # dev39 — an in-memory center with no DB mapping. Before this,
            # the frozen path returned (None, conf, level, None): the caller
            # stored cluster_id NULL with NO rejection reason, so a dead id
            # map after a restart was indistinguishable from "never
            # evaluated" (production: matching silently stopped 2026-08-13
            # while the DB showed 30 healthy clusters). Reject explicitly.
            n = self._unmapped_center_hits.get(circuit, 0) + 1
            self._unmapped_center_hits[circuit] = n
            if n == 1 or n % 25 == 0:
                log.warning(
                    "[%s] frozen match landed on UNMAPPED river center %s "
                    "(%d hit(s) since start) — the id map lost this center "
                    "at the last rebuild; re-seed from Settings if this "
                    "persists.", circuit, nearest_id, n)
            return (None, 0.0, '', 'unmapped_center')
        fixture_type = self._type_cache.get(circuit, {}).get(candidate_id)
        if fixture_type:
            try:
                from .fixtures import get_match_threshold
                row = self._db.execute(
                    "SELECT centroid FROM fixture_clusters "
                    "WHERE circuit = ? AND id = ?",
                    (circuit, candidate_id),
                ).fetchone()
                if row and row["centroid"]:
                    db_orig   = _load_centroid(row["centroid"])
                    db_feat   = {k: float(db_orig.get(k, 0)) for k in FEATURE_KEYS}
                    db_scaled = scaler.transform_one(db_feat)
                    weights   = self._build_match_weights(fixture_type)
                    wdist     = self._weighted_distance(x, db_scaled, weights)
                    if wdist > get_match_threshold(fixture_type):
                        return (None, 0.0, '', 'type_gate_rejected')
            except Exception as e:
                self._type_gate_errors[circuit] = (
                    self._type_gate_errors.get(circuit, 0) + 1)
                log.warning("[%s] frozen type-gate failed — rejecting (fail-closed, "
                            "%d total): %s",
                            circuit, self._type_gate_errors[circuit], e)
                return (None, 0.0, '', 'type_gate_error')

        confidence = math.exp(-distance / DBSTREAM_CLUSTERING_THRESHOLD)
        member_count = 0
        if candidate_id is not None:
            try:
                r = self._db.execute(
                    "SELECT member_count FROM fixture_clusters "
                    "WHERE circuit = ? AND id = ?",
                    (circuit, candidate_id),
                ).fetchone()
                member_count = int(r["member_count"]) if r and r["member_count"] else 0
            except Exception:
                member_count = 0
        return (candidate_id, confidence, self._confidence_level(member_count), None)

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
        # dev42 (F-C1): a reseed replay is rebuilding this circuit's model —
        # matching now would consult a half-built center set and store a
        # wrong-or-NULL id indistinguishable from an honest match. Defer:
        # the reseed flushes 'reseed_deferred' rows through the completed
        # model right after freeze. (The replay itself calls the internal
        # path below, not this public method.)
        if self._reseed_active.get(circuit):
            return (None, 0.0, '', 'reseed_deferred')
        return self._match_and_learn_impl(
            event, circuit, prev_cluster_id, seconds_since_prev)

    def begin_reseed(self, circuit: str) -> None:
        """dev42 (F-C1) — defer live matching while a reseed replay runs.
        Deliberately NOT cleared by a crashed reseed: a half-built model must
        not resume matching; the F-C2 marker tells the user to rerun."""
        self._reseed_active[circuit] = True

    def end_reseed(self, circuit: str) -> None:
        self._reseed_active[circuit] = False

    def _match_and_learn_impl(
        self,
        event: dict,
        circuit: str,
        prev_cluster_id: Optional[int] = None,
        seconds_since_prev: Optional[float] = None,
    ) -> Tuple[Optional[int], float, str, Optional[str]]:
        features = self._extract_features(event, circuit)
        if features is None:
            return (None, 0.0, '', 'features_missing')

        # Phase 1 hard lock: a frozen (live) circuit matches against the locked
        # reference without learning — never reshaping "normal".
        if self._is_frozen(circuit):
            return self._match_frozen(features, circuit)

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
                    db_orig   = _load_centroid(row["centroid"])
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
                # Fail CLOSED (§2.3): a crashed gate must not silently accept a
                # possibly-wrong match. Reject + count; backfill_unmatched can retry
                # the event later once the underlying issue is fixed.
                self._type_gate_errors[circuit] = (
                    self._type_gate_errors.get(circuit, 0) + 1)
                log.warning(
                    "[%s] type-aware gate failed for cluster %s — rejecting "
                    "(fail-closed, %d total): %s",
                    circuit, candidate_id, self._type_gate_errors[circuit], e,
                )
                return (None, 0.0, '', 'type_gate_error')

        confidence = math.exp(-distance / DBSTREAM_CLUSTERING_THRESHOLD)
        # Stage 3: multiply confidence by SEQUENCE_BOOST_WEIGHT when
        # cooccurrence count for (prev_cluster_id → cluster_id) >= 10.

        cluster_id   = self._upsert_cluster(circuit, nearest_id)
        member_count = self._increment_member_count(circuit, cluster_id)
        # Attach the temporal appliance-cycle signal to the feature dict ONLY
        # now — after clustering — so it rides into the centroid running-mean but
        # never reaches the scaler / DBSTREAM (it is intentionally absent from
        # FEATURE_KEYS). Best-effort past-only online; the startup / manual batch
        # recompute fills the full ±45 min window authoritatively.
        try:
            from .database import cycle_pulse_count_for_event
            features['cycle_pulse_count'] = float(cycle_pulse_count_for_event(
                self._db, circuit, event.get('id'),
                event.get('start_ts'), event.get('volume_litres'), past_only=True))
        except Exception as e:
            log.debug("[%s] online cycle_pulse_count failed: %s", circuit, e)
            features.setdefault('cycle_pulse_count', 0.0)
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
        Called once per circuit at startup (via run_db — dev46 46a).
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
        replayed: List[Tuple[dict, int]] = []
        for row in rows:
            features = self._extract_features(dict(row), circuit)
            if features is None:
                continue
            scaler = self._scalers[circuit]
            scaler.learn_one(features)
            x = scaler.transform_one(features)
            self._streams[circuit].learn_one(x)
            replayed.append((features, int(row["cluster_id"])))
            count += 1

        if count == 0 and self._is_frozen(circuit):
            # dev34 B2 — the death spiral, named. This replay reads only
            # cluster_id IS NOT NULL rows; once live matching stops (e.g. a
            # supply change moves the features), the pool drains, the next
            # restart replays nothing, and every subsequent event rejects with
            # 'no_centers' — permanently, and silently but for this line.
            # Production hit exactly this after the 2026-07 pump install:
            # weekly assignment went 75% → 58% → 45% → 8% → 0% and no cluster
            # gained a member after 07-21. rebuild_from_db CANNOT recover it
            # (nothing left to replay) — run the cluster re-seed
            # (training_manager.reseed_clusters_for_regime) instead.
            n_unmatched = self._db.execute(
                "SELECT COUNT(*) FROM events WHERE circuit = ? "
                "AND cluster_id IS NULL AND end_ts >= ?",
                (circuit, cutoff)).fetchone()[0]
            log.warning(
                "[%s] cluster replay pool is EMPTY while the circuit is "
                "frozen — matching is dead (every event will reject with "
                "no_centers; %d unmatched events in the window). "
                "rebuild_from_db cannot recover this; re-seed the clusters "
                "from Settings.", circuit, n_unmatched)

        if count > 0:
            # dev39 — ground the map in the replayed rows' OWN cluster_ids
            # (majority vote) before falling back to centroid proximity.
            # The proximity method alone silently killed live matching on
            # 2026-08-13: any river center whose best DB centroid sat outside
            # the acceptance bound stayed unmapped, and every live event
            # landing on it stored cluster_id NULL with no rejection reason.
            self._rebuild_id_map_from_assignments(circuit, replayed)
            self._rebuild_id_map_from_centroids(circuit)
            if (self._streams[circuit].centers
                    and not self._river_id_map[circuit]):
                log.warning(
                    "[%s] cluster id map is EMPTY after replaying %d event(s) "
                    "— live matching will reject every event with "
                    "'unmapped_center' until a re-seed rebuilds the space "
                    "(Settings → Rebuild fixture grouping).", circuit, count)

        # Belt-and-braces: re-derive the type cache from the DB after a
        # rebuild so any drift (e.g. a UI/import path that toggled
        # fixtures.confirmed without going through the notify hooks) heals.
        self._refresh_type_cache(circuit)

        log.info("Restored %d events into circuit '%s' state", count, circuit)
        return count

    def backfill_unmatched(self, circuit: str,
                           since_ts: Optional[str] = None) -> int:
        """
        Assign cluster_id to events that were collected before the DBSTREAM
        engine existed (v0.1.x upgrades) or before a full recalibration.
        Processes cluster_id IS NULL events in chronological order, feeding
        each through match_and_learn and writing the result back.

        Safe to call multiple times — only processes unmatched rows.
        Called from orchestrator after rebuild_from_db and from
        training_manager after calibration completes.

        since_ts (dev34 B2): window the pool to events at/after this
        timestamp. The pump-era re-seed uses it so pre-anchor rows — recorded
        before the fragmentation fixes and under the old supply — cannot seed
        the new space; fragments below the training support would become
        centers.
        """
        window_sql = "AND start_ts >= ?" if since_ts else ""
        params = (circuit, since_ts) if since_ts else (circuit,)
        rows = self._db.execute(
            f"""SELECT * FROM events
               WHERE circuit = ? AND cluster_id IS NULL
                 AND excluded_from_training = 0
                 AND end_ts IS NOT NULL
                 {window_sql}
               ORDER BY start_ts ASC""",
            params
        ).fetchall()

        if not rows:
            return 0

        count = 0
        for row in rows:
            event = dict(row)
            # dev42: the internal path — the replay must keep working while
            # the F-C1 deferral flag is set for live traffic.
            cluster_id, confidence, level, reason = \
                self._match_and_learn_impl(event, circuit)
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

    async def backfill_unmatched_async(self, circuit: str,
                                       since_ts: Optional[str] = None,
                                       batch: int = 100,
                                       only_deferred: bool = False) -> int:
        """dev42 (F1) — ``backfill_unmatched`` in chunks, one chunk per hop.

        dev46 (46a/C1) — each chunk's DB work is submitted to ``run_db``, the
        SINGLE DB thread. dev42 moved this ON the event loop to escape the
        8/15 crash (two threads sharing one ``check_same_thread=False``
        connection → InterfaceError), but on-loop is only serialized against
        other loop code: a concurrent page render on the DB executor would
        reopen the exact same window. Routing every touch through the one DB
        worker closes it for good, and hands the event loop back for the
        reseed's whole duration (reversing dev42's responsiveness cost).

        Chunk boundary == transaction boundary (rule N2a): each
        ``_backfill_chunk_sync`` call opens, writes, and commits entirely
        inside one run_db callable, so no foreign statement can land inside
        an open transaction.

        ``only_deferred``: process only rows the F-C1 deferral stamped
        'reseed_deferred' — the post-freeze flush. Without it, a second full
        pass would re-run every abstained row in the window.
        """
        from .database import run_db
        conds = ["circuit = ?", "cluster_id IS NULL",
                 "excluded_from_training = 0", "end_ts IS NOT NULL"]
        params: list = [circuit]
        if since_ts:
            conds.append("start_ts >= ?")
            params.append(since_ts)
        if only_deferred:
            conds.append("match_rejection_reason = 'reseed_deferred'")
        sql = (f"SELECT id FROM events WHERE {' AND '.join(conds)} "
               "ORDER BY start_ts ASC")
        ids = await run_db(
            lambda: [r[0] for r in self._db.execute(sql, params).fetchall()])
        count = 0
        for i in range(0, len(ids), batch):
            count += await run_db(self._backfill_chunk_sync,
                                  ids[i:i + batch], circuit)
        if count:
            log.info("[%s] backfill_unmatched_async: assigned cluster_id "
                     "to %d events%s", circuit, count,
                     " (deferred flush)" if only_deferred else "")
        return count

    def _backfill_chunk_sync(self, chunk: list, circuit: str) -> int:
        """One chunk of the async backfill — runs on the single DB thread.

        Self-contained transaction (dev46 rule N2a): every statement for this
        chunk, plus its commit, happens inside this one callable. Returns the
        number of events that received a cluster_id.
        """
        if not chunk:
            return 0
        ph = ",".join("?" * len(chunk))
        rows = self._db.execute(
            f"SELECT * FROM events WHERE id IN ({ph}) "
            "ORDER BY start_ts ASC", chunk).fetchall()
        count = 0
        for row in rows:
            event = dict(row)
            cluster_id, confidence, level, reason = \
                self._match_and_learn_impl(event, circuit)
            if cluster_id is None:
                self._db.execute(
                    "UPDATE events SET match_rejection_reason = ? "
                    "WHERE id = ?", (reason, event["id"]))
                continue
            self._db.execute(
                """UPDATE events
                   SET cluster_id = ?, match_confidence = ?,
                       match_level = ?, match_rejection_reason = NULL
                   WHERE id = ?""",
                (cluster_id, confidence, level, event["id"]))
            count += 1
        self._db.commit()
        return count

    # ── Type-level auto-merge ──────────────────────────────────────────────────

    def auto_merge_same_type_clusters(self, circuit: str) -> int:
        """Merge clusters that have converged to the same fixture type.

        Conservative safety gate — all conditions must hold before merging:
        - Effective type is not None and not 'other'
        - Both clusters have member_count >= 5
        - If neither is confirmed: suggested_confidence >= 0.75 for both
        - Centroid weighted-distance <= get_match_threshold(effective_type)

        Survivor selection is deterministic: confirmed fixture row first,
        then highest member_count, then most recent last_match_at, then
        lowest cluster_id.

        Returns the number of merge operations executed.  Protected by
        _merge_lock so concurrent executor threads see consistent state.
        """
        from .database import merge_clusters
        from .fixtures import get_match_threshold

        with self._merge_lock:
            rows = self._db.execute(
                """SELECT fc.id, fc.member_count,
                          fc.suggested_type, fc.suggested_confidence,
                          fc.centroid, fc.last_match_at,
                          f.fixture_type AS user_type,
                          CASE WHEN f.confirmed = 1 THEN 1 ELSE 0 END AS is_confirmed
                   FROM fixture_clusters fc
                   LEFT JOIN fixtures f ON fc.fixture_id = f.id
                   WHERE fc.circuit = ?
                   ORDER BY fc.id""",
                (circuit,),
            ).fetchall()

            # Group by effective type
            by_type: Dict[str, List[dict]] = {}
            for r in rows:
                eff = r["user_type"] or r["suggested_type"]
                if not eff or eff == "other":
                    continue
                by_type.setdefault(eff, []).append(dict(r))

            scaler = self._scalers.get(circuit)
            merges = 0

            for ftype, clusters in by_type.items():
                if len(clusters) < 2:
                    continue

                # Apply safety gate to each cluster; collect eligible ones
                threshold = get_match_threshold(ftype)
                weights   = self._build_match_weights(ftype)
                eligible: List[dict] = []
                for cl in clusters:
                    if (cl["member_count"] or 0) < 5:
                        continue
                    if not cl["is_confirmed"] and (cl["suggested_confidence"] or 0) < 0.75:
                        continue
                    eligible.append(cl)

                if len(eligible) < 2:
                    continue

                # Check centroid distance between all pairs; only proceed if
                # at least one pair is close enough to merge.
                if scaler:
                    centroids_scaled = {}
                    for cl in eligible:
                        try:
                            raw = _load_centroid(cl["centroid"] or "{}")
                            feat = {k: float(raw.get(k, 0)) for k in FEATURE_KEYS}
                            centroids_scaled[cl["id"]] = scaler.transform_one(feat)
                        except Exception:
                            pass

                    any_close = False
                    ids = [cl["id"] for cl in eligible]
                    for i, id_a in enumerate(ids):
                        for id_b in ids[i + 1:]:
                            if id_a not in centroids_scaled or id_b not in centroids_scaled:
                                continue
                            dist = self._weighted_distance(
                                centroids_scaled[id_a], centroids_scaled[id_b], weights
                            )
                            if dist <= threshold:
                                any_close = True
                                break
                        if any_close:
                            break
                    if not any_close:
                        log.debug(
                            "[%s] auto_merge: skipping '%s' — no pair within "
                            "threshold %.2f", circuit, ftype, threshold,
                        )
                        continue

                # Pick survivor deterministically: confirmed first, then
                # member_count desc, then last_match_at desc, then id asc.
                # last_match_at is a TEXT ISO-8601 timestamp — it can't be
                # negated into a single descending tuple key, so apply
                # staged stable sorts, least-significant key first.
                eligible.sort(key=lambda c: c["id"])
                eligible.sort(key=lambda c: c["last_match_at"] or "", reverse=True)
                eligible.sort(key=lambda c: c["member_count"] or 0, reverse=True)
                eligible.sort(key=lambda c: c["is_confirmed"], reverse=True)
                survivor_id = eligible[0]["id"]
                all_ids = [cl["id"] for cl in eligible]

                log.info(
                    "[%s] auto_merge: merging %d '%s' clusters → survivor=%d",
                    circuit, len(all_ids), ftype, survivor_id,
                )
                try:
                    merge_clusters(self._db, circuit, survivor_id, all_ids)
                    merges += 1
                except Exception as exc:
                    log.warning(
                        "[%s] auto_merge: merge_clusters failed for '%s': %s",
                        circuit, ftype, exc,
                    )

            if merges > 0:
                self._db.commit()
                self.rebuild_from_db(circuit)
                log.info("[%s] auto_merge: %d merge(s) completed, engine rebuilt",
                         circuit, merges)

            return merges

    def _rebuild_id_map_from_assignments(
        self, circuit: str, replayed: List[Tuple[dict, int]],
    ) -> None:
        """Map river centers to DB clusters by majority vote of the replayed
        rows' stored cluster_ids.

        The replay pool is `cluster_id IS NOT NULL` rows, so the DB already
        says which cluster each event belongs to — re-deriving that link by
        nearest-centroid proximity (the only method before dev39) threw that
        truth away and depended on the freshly-replayed scaler placing old
        stored centroids within an acceptance bound. Any drift (tz-feature
        rewrite, scaler-stat shift across restarts) left centers unmapped and
        live matching silently dead. A vote from actual assignments cannot
        drift: the same rows that built each center name its cluster.

        Runs BEFORE the centroid fallback; centers with no votes (formed but
        never nearest to any replayed row) still get the proximity attempt.
        """
        stream = self._streams[circuit]
        scaler = self._scalers[circuit]
        if not stream.centers or not replayed:
            return
        votes: Dict[int, Dict[int, int]] = {}
        for features, db_cluster_id in replayed:
            x = scaler.transform_one(features)
            river_id, _ = self._nearest_center(stream, x)
            if river_id is None:
                continue
            votes.setdefault(river_id, {})
            votes[river_id][db_cluster_id] = (
                votes[river_id].get(db_cluster_id, 0) + 1)
        mapping = self._river_id_map[circuit]
        for river_id, tally in votes.items():
            if river_id in mapping:
                continue
            winner = max(tally.items(), key=lambda kv: kv[1])[0]
            mapping[river_id] = winner
            # Health metric (dev40): a healthy river↔DB mapping is a
            # SUPERMAJORITY — the members that built a center agree on its
            # cluster. A sub-50% plurality means the center sits across
            # several DB clusters' members (degenerate geometry), which is
            # the cheap early warning that fires before cluster-count
            # collapse is visible (2026-08-15: three circuit_1 centers all
            # plurality-mapped to one DB cluster at 40-49%).
            frac = tally[winner] / max(sum(tally.values()), 1)
            log.debug("[%s] post-rebuild: river cluster %d → DB cluster %d "
                      "(%d/%d votes)", circuit, river_id, winner,
                      tally[winner], sum(tally.values()))
            if frac < 0.50:
                log.warning("[%s] post-rebuild health: river cluster %d "
                            "mapped to DB cluster %d on a %.0f%% PLURALITY "
                            "(%d/%d votes) — sub-majority id-map votes "
                            "indicate degenerate cluster geometry; consider "
                            "a river-model re-seed", circuit, river_id,
                            winner, frac * 100, tally[winner],
                            sum(tally.values()))
        self._log_cluster_diversity_48h(circuit)

    def _log_cluster_diversity_48h(self, circuit: str) -> None:
        """Health metric (dev40): distinct DB clusters actually receiving
        events in the last rolling 48 h. Logged at rebuild time next to the
        vote fractions — a collapse from the circuit's usual spread down to
        1-2 clusters is the other face of the same degeneracy the sub-50%
        plurality warning catches. Log/metric only; never changes behavior."""
        try:
            row = self._db.execute(
                "SELECT COUNT(DISTINCT cluster_id), COUNT(*) FROM events "
                "WHERE circuit = ? AND cluster_id IS NOT NULL "
                "  AND start_ts >= strftime('%Y-%m-%dT%H:%M:%S', "
                "                           'now', '-48 hours')",
                (circuit,)).fetchone()
            log.info("[%s] post-rebuild health: %d distinct DB cluster(s) "
                     "over %d clustered event(s) in the last 48h",
                     circuit, row[0], row[1])
        except Exception as e:
            log.debug("[%s] 48h cluster-diversity metric failed "
                      "(non-fatal): %s", circuit, e)
        # dev42 (F-C2): a stale reseed marker means a re-seed crashed
        # mid-replay and never finished — the model this rebuild just
        # replayed is part-cleared. Warn at every health pass until a rerun
        # succeeds.
        try:
            r = self._db.execute(
                "SELECT reseed_in_progress FROM training_state "
                "WHERE circuit = ? AND reseed_in_progress IS NOT NULL",
                (circuit,)).fetchone()
            if r:
                log.warning("[%s] post-rebuild health: reseed incomplete "
                            "(started %s, never finished) — model untrusted, "
                            "rerun required", circuit, r[0])
        except Exception:
            pass    # pre-20260808 schema

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
                    db_orig = _load_centroid(db_row["centroid"])
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
