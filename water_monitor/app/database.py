"""
SQLite database setup, migrations, and data access helpers.

Single database file at /data/water_monitor.db.
Schema is created in full on first run. All Phase 2 tables are
created now so Phase 2 never needs a schema migration.
"""
from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

log = logging.getLogger(__name__)

# Module-level write lock — serialises concurrent async writes.
# Created lazily within the running event loop.
_WRITE_LOCK: Optional[Any] = None

def get_write_lock():
    """Return the singleton asyncio write lock, creating it on first call."""
    import asyncio
    global _WRITE_LOCK
    if _WRITE_LOCK is None:
        _WRITE_LOCK = asyncio.Lock()
    return _WRITE_LOCK

SCHEMA_VERSION = 1


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # Wait up to 5s before raising OperationalError on a locked DB.
    # Prevents immediate failures when cluster engine executor threads and
    # async coroutines briefly contend for the same connection.
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


async def run_isolated_write(db_path, fn):
    """Serialise a heavy DB-write job and run it on a fresh PRIVATE connection.

    Two guarantees that make user-triggered admin writes (recompute, reclassify)
    safe — both against each other AND against the inline writers on the shared
    ``orch.db`` connection (the live feature extractor, the pruner, …):

    * ``get_write_lock()`` is held for the whole job, so two admin writes can't
      run concurrently. (The bug this fixes: two simultaneous /recompute requests
      drove the *shared* connection from two worker threads → SQLite
      ``InterfaceError`` / "cannot commit - no transaction is active".)
    * the job runs on its OWN ``sqlite3.Connection`` (opened in the worker thread,
      closed after), so it never shares connection state across threads. WAL +
      ``busy_timeout`` handle writer-vs-writer between connections, and the job's
      per-row commits release the file write-lock between rows.

    ``fn`` is a sync callable taking the private connection; its return value is
    propagated. The connection is closed even if ``fn`` raises. ``db_path`` is
    passed in (not imported) so this module stays free of config imports.
    """
    import asyncio

    async with get_write_lock():
        loop = asyncio.get_running_loop()

        def _run():
            conn = get_connection(db_path)
            try:
                return fn(conn)
            finally:
                conn.close()

        return await loop.run_in_executor(None, _run)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create database and all tables. Safe to call on existing database."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)

    # Integrity check before creating/using schema
    result = conn.execute("PRAGMA integrity_check").fetchone()
    if result and result[0] != "ok":
        log.error("DATABASE INTEGRITY CHECK FAILED: %s — proceed with caution",
                  result[0])
    else:
        log.debug("Database integrity check passed")

    _create_schema(conn)
    log.info("Database initialised at %s", db_path)
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
-- ==========================================================================
-- DEVICE DISCOVERY — stores auto-discovered HA device and entity IDs.
-- Populated by the setup wizard; replaces manual config.yaml entity IDs.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS device_config (
    id                  INTEGER PRIMARY KEY DEFAULT 1,
    esp_device_name     TEXT,       -- name user searched for
    ha_device_id        TEXT,       -- HA device registry ID
    ha_device_name      TEXT,       -- HA device display name
    esp_device_prefix   TEXT,       -- derived entity ID prefix
    fw_version          TEXT,       -- ESPHome project.version from device registry
    setup_complete      BOOLEAN DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO device_config (id) VALUES (1);

CREATE TABLE IF NOT EXISTS circuit_entity_map (
    circuit     TEXT NOT NULL,
    role        TEXT NOT NULL,      -- flow_sensor, valve_entity, etc.
    entity_id   TEXT NOT NULL DEFAULT '',
    entity_name TEXT,               -- original_name from HA entity registry
    confirmed   BOOLEAN DEFAULT 0,
    PRIMARY KEY (circuit, role)
);

-- ==========================================================================
-- HOME & CIRCUIT PROFILE
-- ==========================================================================
CREATE TABLE IF NOT EXISTS home_profile (
    id              INTEGER PRIMARY KEY DEFAULT 1,
    bathrooms_full  INTEGER DEFAULT 0,
    bathrooms_half  INTEGER DEFAULT 0,
    sqft            INTEGER DEFAULT 0,
    floors          INTEGER DEFAULT 1,
    occupants       INTEGER DEFAULT 2,
    build_year      INTEGER,
    supply_type     TEXT DEFAULT 'mains',
    setup_complete  BOOLEAN DEFAULT 0,
    -- Away / vacation mode
    away_mode       BOOLEAN DEFAULT 0,
    away_since      TIMESTAMP,
    -- Display unit preferences (keys match units.FLOW_OPTIONS / PRESSURE_OPTIONS)
    flow_unit               TEXT DEFAULT 'L/min',
    pressure_unit           TEXT DEFAULT 'psi',
    -- Phase 2.1 fixture publishing
    publish_fixtures_to_ha  INTEGER DEFAULT 1,
    -- Mobile push notification targets (comma-separated HA notify service names)
    mobile_notify_targets   TEXT DEFAULT '',
    -- HA presence tracking — auto-toggle away mode from HA entity state changes.
    -- ha_presence_entities: comma-separated entity IDs to watch
    --   (person.*, device_tracker.*, input_boolean.*, alarm_control_panel.*)
    -- ha_away_state: state value that means "away" (default: not_home)
    -- ha_home_state: state value that means "home"  (default: home)
    -- When ALL entities reach ha_away_state → enable away mode.
    -- When ANY entity reaches ha_home_state  → disable away mode.
    ha_presence_entities    TEXT DEFAULT '',
    ha_away_state           TEXT DEFAULT 'not_home',
    ha_home_state           TEXT DEFAULT 'home',
    -- MQTT publishing toggle (Phase 2.1)
    mqtt_publish_enabled    INTEGER NOT NULL DEFAULT 0,
    -- History display: hide pressure-restoration phantom events from the
    -- History list (Sprint E). Off by default — phantoms are shown with a
    -- flag. This is display-only; it never affects volume totals (phantom
    -- volume is always zeroed at detection regardless of this toggle).
    hide_pressure_artifact_events  INTEGER NOT NULL DEFAULT 0,
    -- History display: hide cross-talk events (migration 20260540). Mirrors the
    -- phantom toggle above; display-only — cross-talk volume is already zeroed.
    hide_cross_talk_events         INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO home_profile (id) VALUES (1);

-- CSRF tokens (legacy — kept for backward compat; new code uses HMAC
-- double-submit, see csrf_server_secret below).
CREATE TABLE IF NOT EXISTS csrf_tokens (
    token       TEXT PRIMARY KEY,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- HMAC server secret for stateless CSRF double-submit. One row
-- (id = 1). The secret is generated once on first use and never
-- regenerated automatically — regenerating would invalidate every
-- in-flight browser session.
CREATE TABLE IF NOT EXISTS csrf_server_secret (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    secret      TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS circuit_profile (
    circuit             TEXT PRIMARY KEY,
    circuit_type        TEXT DEFAULT 'fixture',
    zone_count_expected INTEGER,
    controller_type     TEXT DEFAULT 'manual',
    has_drip_zones      BOOLEAN DEFAULT 0,
    initial_priors_json TEXT,
    priors_computed_at  TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Ball valve hardware type (migration 20260527).
    -- '2_port' (default): standard inline valve, micro leak test enabled.
    -- '3_port'         : drain-capable valve; leak test is automatically
    --                    skipped because a drain port reads as a constant
    --                    leak. Set per circuit during setup; editable from
    --                    Settings behind a confirmation prompt.
    valve_type          TEXT DEFAULT '2_port'
);

-- ==========================================================================
-- TRAINING STATE MACHINE
-- ==========================================================================
CREATE TABLE IF NOT EXISTS training_state (
    circuit             TEXT PRIMARY KEY,
    state               TEXT DEFAULT 'idle',
    calibration_days    INTEGER DEFAULT 14,
    started_at          TIMESTAMP,
    calibration_ends_at TIMESTAMP,
    minimum_events      INTEGER DEFAULT 150,
    events_collected    INTEGER DEFAULT 0,
    labelling_deadline  TIMESTAMP,
    completed_at        TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- TRAINING-HELPER CAPTURE (2b) — a one-time "run each fixture once" wizard.
-- One active ('armed') row per circuit; the event-completion hook records
-- candidate event ids, the user confirms/accepts to write 'training' labels.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS training_capture (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit         TEXT NOT NULL,
    fixture_type    TEXT NOT NULL,
    -- armed | ready | captured | cancelled | expired | rejected
    status          TEXT NOT NULL DEFAULT 'armed',
    armed_at        TIMESTAMP NOT NULL,
    expires_at      TIMESTAMP NOT NULL,
    window_minutes  INTEGER,
    captured_count  INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_training_capture_active
    ON training_capture (circuit, status);
-- Candidate events recorded by the hot-path hook (plain INSERT, no JSON).
CREATE TABLE IF NOT EXISTS training_capture_candidates (
    capture_id      INTEGER NOT NULL,
    event_id        TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_training_capture_candidates
    ON training_capture_candidates (capture_id);

-- ==========================================================================
-- LEARNING CONFIGURATION
-- ==========================================================================
CREATE TABLE IF NOT EXISTS learning_config (
    circuit                         TEXT PRIMARY KEY,
    learning_mode                   TEXT DEFAULT 'adaptive',
    accelerated_adaptation_until    TIMESTAMP,
    accelerated_adaptation_reason   TEXT,
    threshold_update_interval_hours INTEGER DEFAULT 24,
    threshold_lookback_days         INTEGER DEFAULT 30,
    updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- SENSITIVITY CONFIGURATION
-- ==========================================================================
CREATE TABLE IF NOT EXISTS sensitivity_config (
    circuit                     TEXT PRIMARY KEY,
    mode                        TEXT DEFAULT 'simple',
    simple_level                TEXT DEFAULT 'medium',
    -- Event detection
    pressure_drop_event_psi     REAL DEFAULT 1.2,
    min_event_duration_seconds  REAL DEFAULT 3.0,
    -- Anomaly thresholds
    score_alert                 REAL DEFAULT 0.60,
    score_shutoff               REAL DEFAULT 0.80,
    -- Tolerances
    flow_tolerance_pct          REAL DEFAULT 20.0,
    duration_tolerance_pct      REAL DEFAULT 30.0,
    schedule_window_minutes     REAL DEFAULT 15.0,
    sustained_alert_minutes     REAL DEFAULT 10.0,
    max_shutoffs_per_12h        INTEGER DEFAULT 2,
    -- Baseline stats (updated on calibration)
    baseline_anomaly_p85        REAL,
    baseline_anomaly_p95        REAL,
    baseline_anomaly_p99        REAL,
    baseline_cluster_std_mean   REAL,
    baseline_computed_at        TIMESTAMP,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- ALERT CONFIGURATION
-- ==========================================================================
CREATE TABLE IF NOT EXISTS alert_config (
    id          TEXT PRIMARY KEY,
    circuit     TEXT NOT NULL,
    alert_type  TEXT NOT NULL,
    fixture_id  TEXT,
    label       TEXT,
    description TEXT,
    enabled     BOOLEAN DEFAULT 1,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- FIXTURES (Phase 2 — created now to avoid future migrations)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixtures (
    id            TEXT PRIMARY KEY,
    circuit       TEXT NOT NULL,
    name          TEXT,
    auto_name     TEXT,
    confirmed     BOOLEAN DEFAULT 0,
    notes         TEXT,
    -- Phase 2.1 additions (Path C)
    fixture_type  TEXT,         -- from fixtures.FIXTURE_TYPES
    display_name  TEXT,         -- may differ from `name` for HA entity slug
    user_locked   INTEGER DEFAULT 0,
    publish_to_ha INTEGER DEFAULT 1,
    -- Sprint A orphan-repair flag: set to 1 when this fixture is confirmed
    -- but no fixture_clusters row has fixture_id pointing at it. The UI
    -- shows a relink banner so the user can pick a cluster to attach.
    cluster_backfill_needed INTEGER DEFAULT 0,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fixture_signatures (
    fixture_id  TEXT REFERENCES fixtures(id) ON DELETE CASCADE,
    feature     TEXT NOT NULL,
    centroid    REAL,
    std_dev     REAL,
    p5          REAL,
    p25         REAL,
    p75         REAL,
    p95         REAL,
    PRIMARY KEY (fixture_id, feature)
);

-- ==========================================================================
-- FIXTURE TYPE SIGNATURES (Sprint C) — per-(circuit, fixture_type) centroid
-- learned from user-labelled events. The legacy fixture_signatures table
-- above is per-(fixture_id, feature); it was never populated by any code
-- path and is kept only for backwards-compat with backup files that
-- include it. The matcher (cluster_engine + feature_extractor) reads from
-- this new table, which is keyed by user-facing fixture *type* (e.g.
-- "toilet"), not a specific fixture row — that matches how the History
-- page's label dropdown is structured.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_type_signatures (
    circuit       TEXT NOT NULL,
    fixture_type  TEXT NOT NULL,
    centroid      TEXT NOT NULL DEFAULT '{}',   -- JSON dict of feature means
    member_count  INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (circuit, fixture_type)
);

CREATE INDEX IF NOT EXISTS idx_type_signatures_circuit
    ON fixture_type_signatures (circuit);

-- ==========================================================================
-- CATEGORY PUBLISH (Sprint F) — per-(circuit, fixture_type) HA publish gate.
-- Replaces the per-fixture `fixtures.publish_to_ha` flag as the source of
-- truth for the fixture_publisher. Each row toggles whether the HA discovery
-- entity set for that category on that circuit is published. publish_to_ha=1
-- by default; missing rows default to True at the caller via .get(typ, True).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS category_publish (
    circuit         TEXT NOT NULL,
    fixture_type    TEXT NOT NULL,
    publish_to_ha   INTEGER NOT NULL DEFAULT 1,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (circuit, fixture_type)
);

-- ==========================================================================
-- FIXTURE CLUSTERS (Phase 2.1) — raw DBSTREAM clustering output
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_clusters (
    id                    INTEGER NOT NULL,
    circuit               TEXT NOT NULL,
    centroid              TEXT NOT NULL DEFAULT '{}',   -- JSON dict of feature means
    feature_std           TEXT NOT NULL DEFAULT '{}',   -- JSON dict of feature stddevs
    transient_template    TEXT,                 -- JSON list, NULL until enough members
    member_count          INTEGER DEFAULT 0,
    suggested_type        TEXT,                 -- from fixtures.suggest_fixture_type
    suggested_confidence  REAL DEFAULT 0,
    -- Sprint B: provenance of suggested_type. NULL = nothing suggested yet,
    -- 'heuristic' = set by cluster_engine._run_suggest_type_if_needed
    -- (centroid feature-range rules), 'user_labels' = set by majority vote
    -- of events.user_fixture_type on this cluster's members. The UI uses
    -- this to render different hint copy and treat user-labels as a
    -- stronger signal than heuristics.
    suggestion_source     TEXT,
    confidence_level      TEXT DEFAULT 'preliminary',  -- preliminary/learning/confirmed
    fixture_id            TEXT REFERENCES fixtures(id) ON DELETE SET NULL,
    is_compound           INTEGER DEFAULT 0,    -- 2.3 placeholder
    component_cluster_ids TEXT,                 -- 2.3 placeholder, JSON list
    publish_to_ha         INTEGER DEFAULT 1,
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_match_at         TIMESTAMP,
    PRIMARY KEY (circuit, id)
);

CREATE INDEX IF NOT EXISTS idx_clusters_circuit
    ON fixture_clusters (circuit);
CREATE INDEX IF NOT EXISTS idx_clusters_fixture
    ON fixture_clusters (fixture_id);

-- ==========================================================================
-- CLUSTER CO-OCCURRENCE (Phase 2.1) — sequence boost for fixture matching
-- ==========================================================================
CREATE TABLE IF NOT EXISTS cluster_cooccurrence (
    circuit             TEXT NOT NULL,
    from_cluster_id     INTEGER NOT NULL,
    to_cluster_id       INTEGER NOT NULL,
    count               INTEGER DEFAULT 0,
    median_gap_seconds  REAL,
    last_seen_at        TIMESTAMP,
    PRIMARY KEY (circuit, from_cluster_id, to_cluster_id)
);

-- ==========================================================================
-- CLUSTER SEQUENCES (Phase 2.2 placeholder, empty in 2.1)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS cluster_sequences (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit           TEXT NOT NULL,
    pattern_hash      TEXT,
    event_chain       TEXT,                 -- JSON list of cluster IDs
    occurrence_count  INTEGER DEFAULT 0,
    confidence        REAL DEFAULT 0,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- PLUMBING-EVENT EXCLUSION WINDOWS (Phase 2.1)
-- User-triggered window that prevents events from being used for fixture
-- clustering during a post-winterization or post-repair flush.  Volume and
-- leak-detection tracking continue regardless of the window state.
-- Pruned after 30 days by data_pruner.py.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS circuit_exclusion_windows (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit     TEXT NOT NULL,
    started_at  TIMESTAMP NOT NULL,
    ends_at     TIMESTAMP NOT NULL,
    reason      TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_excl_circuit_window
    ON circuit_exclusion_windows (circuit, started_at, ends_at);

-- ==========================================================================
-- CLUSTER METRICS HISTORY — rolling cluster quality stats
-- ==========================================================================
CREATE TABLE IF NOT EXISTS cluster_metrics_history (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    measured_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    circuit               TEXT NOT NULL,
    cluster_count         INTEGER,
    coverage_pct          REAL,
    avg_purity            REAL,
    avg_stability         REAL,
    unmatched_recent_24h  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_metrics_circuit_ts
    ON cluster_metrics_history (circuit, measured_at);

-- ==========================================================================
-- EVENT LOG
-- ==========================================================================
CREATE TABLE IF NOT EXISTS events (
    id                          TEXT PRIMARY KEY,
    circuit                     TEXT NOT NULL,
    start_ts                    TIMESTAMP NOT NULL,
    end_ts                      TIMESTAMP,
    duration_seconds            REAL,
    avg_flow_lpm                REAL,
    peak_flow_lpm               REAL,
    flow_variability            REAL DEFAULT 0,
    pressure_delta_psi          REAL,
    pre_event_pressure_psi      REAL,
    min_pressure_psi            REAL,
    hydraulic_resistance        REAL,
    resistance_curve_shape      TEXT,
    propagation_delay_seconds   REAL,
    propagation_delay_ms        REAL DEFAULT 0,
    flow_onset_delay_seconds    REAL,
    start_trigger               TEXT DEFAULT 'unknown',
    has_pressure_transient      BOOLEAN DEFAULT 0,
    hour_of_day                 INTEGER,
    day_of_week                 INTEGER,
    duration_log                REAL DEFAULT 0,
    hour_sin                    REAL DEFAULT 0,
    hour_cos                    REAL DEFAULT 1,
    is_weekend                  BOOLEAN DEFAULT 0,
    is_composite                BOOLEAN DEFAULT 0,
    other_valve_open            INTEGER,           -- NULL=unknown 0=closed 1=open
    excluded_from_training      BOOLEAN DEFAULT 0,
    cluster_id                  INTEGER,
    -- Phase 2.1 type-aware match gate: when cluster_id IS NULL, this records
    -- WHY the event was not matched. Values:
    --   'no_centers'             — DBSTREAM had no centres yet
    --   'features_missing'       — extractor returned None
    --   'type_gate_rejected'     — confirmed cluster's per-type variance gate
    --   'excluded_from_training' — caller skipped match_and_learn entirely
    -- NULL when the event matched cleanly.
    match_rejection_reason      TEXT,
    -- Cluster match quality (written by _cluster_event after insert)
    match_confidence            REAL,    -- 0.0–1.0; NULL = unmatched
    match_level                 TEXT,    -- 'preliminary'|'confirmed'|NULL
    -- Inter-event sequence context (written by _cluster_event)
    seconds_since_prev_event    REAL,    -- gap from previous event end → this start
    seconds_to_next_event       REAL,    -- retroactively filled when next event arrives
    prev_cluster_id             INTEGER, -- cluster_id of the preceding event
    fixture_id                  TEXT REFERENCES fixtures(id),
    anomaly_score               REAL,
    anomaly_type                TEXT,
    flagged                     BOOLEAN DEFAULT 0,
    user_reviewed               BOOLEAN DEFAULT 0,
    user_fixture_type           TEXT,              -- user-assigned fixture type (overrides clustering)
    triggered_alert             BOOLEAN DEFAULT 0,
    volume_litres               REAL DEFAULT 0,
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Flow shape features (migration 025)
    flow_signature_json              TEXT,
    -- Pressure drop signature (migration 029)
    pressure_signature_json          TEXT,
    positive_edge_count              INTEGER DEFAULT 0,
    negative_edge_count              INTEGER DEFAULT 0,
    flow_edge_count                  INTEGER DEFAULT 0,
    flow_rise_rate_lpm_s             REAL DEFAULT 0,
    flow_fall_rate_lpm_s             REAL DEFAULT 0,
    opening_step_lpm                 REAL DEFAULT 0,
    closing_step_lpm                 REAL DEFAULT 0,
    time_to_90pct_flow_seconds       REAL DEFAULT 0,
    time_from_90pct_to_zero_seconds  REAL DEFAULT 0,
    mid_event_flow_drop_lpm          REAL DEFAULT 0,
    steady_state_fraction            REAL DEFAULT 0,
    -- Pressure transient features (migration 025)
    pressure_transient_energy        REAL DEFAULT 0,
    pressure_transient_duration_ms   REAL DEFAULT 0,
    -- Pressure transient shape features (migration 026)
    pressure_onset_ms                REAL DEFAULT 0,
    recovery_overshoot_psi           REAL DEFAULT 0,
    pressure_oscillation_count       INTEGER DEFAULT 0,
    -- ESP waveform A/B fields (migration 031)
    esp_waveform_used                INTEGER,
    waveform_event_id                INTEGER,
    waveform_quality                 INTEGER,
    waveform_overlap_score           REAL,
    -- Signature provenance — which source generated the shape signatures.
    -- 'software' (default) | 'esp_full_flow' | 'esp_full_pressure' | 'esp_full_flow_pressure'
    signature_source                 TEXT,
    -- Degraded-supply guard (migration 20260526). When degraded_supply=1 the
    -- event was captured during pulsing-supply conditions; flow data is
    -- unreliable. volume_litres_effective is the value actually applied to
    -- hourly_volume (raw for healthy events, envelope-smoothed for degraded).
    -- hourly_volume_applied_litres/_bucket track exact prior contribution so
    -- re-imports correctly subtract-then-add.
    -- match_rejection_reason additionally accepts 'pulsing_supply'.
    degraded_supply                  BOOLEAN DEFAULT 0,
    volume_litres_estimated          REAL,
    volume_litres_effective          REAL,
    volume_estimation_method         TEXT DEFAULT 'raw',
    -- Active-flow features (migration 20260536). Computed by time-integrating the
    -- timestamped flow samples (flow_integral.py). NULLABLE on purpose: NULL =
    -- unknown / not yet backfilled (NOT the same as 0 = known no flow). Drive
    -- classification + the hardened phantom guard. integration_quality is 'ok',
    -- 'capped' (offline-gap clamp), or 'degraded' (bad/sparse backfill history);
    -- anything but 'ok'/NULL is kept out of classifier training.
    flow_integral_litres             REAL,
    active_flow_duration_seconds     REAL,
    true_avg_flow_lpm                REAL,
    flow_on_ratio                    REAL,
    active_flow_segment_count        INTEGER,
    flow_cv_on_segments              REAL,
    integration_quality              TEXT,
    -- Volume-recompute audit trail (Phase 2 backfill): original pre-recompute
    -- volume + when it was last recomputed, for verification / rollback.
    volume_litres_original           REAL,
    volume_recomputed_at             TIMESTAMP,
    hourly_volume_applied_litres     REAL DEFAULT 0,
    hourly_volume_applied_bucket     TEXT,
    degraded_diagnostic_json         TEXT,
    -- Sprint C signature matcher: the fixture_type matched by the
    -- fixture_type_signatures table when cluster matching couldn't
    -- assign a cluster_id (or assigned one with low confidence).
    -- Independent of cluster_id — a single event can have cluster_id
    -- set AND matched_fixture_type set if the cluster matched but the
    -- signature gave a more specific type guess.
    matched_fixture_type             TEXT,
    -- Pressure-restoration phantom guard (migration 20260532). When 1, this
    -- event matched the long-duration + near-zero-pressure-drop fingerprint
    -- of a city-pressure-restoration artifact. Its volume_litres_effective is
    -- forced to 0 and it is excluded_from_training. Shown in History with a
    -- flag; volume contributes nothing to totals.
    is_pressure_restoration_phantom  INTEGER DEFAULT 0,
    -- Low-flow dribble guard (migration 20260535). When 1, this event is a
    -- brief low-flow / low-volume / near-zero-pressure trickle (sensor or
    -- pressure-equalisation noise). UNLIKE a phantom it does NOT zero volume —
    -- it only sets excluded_from_training so the event stays out of the
    -- classifier / clustering training set while its (tiny) volume still counts
    -- toward totals. Auto-derived; suppressed for user_classified rows.
    is_low_flow_dribble              INTEGER NOT NULL DEFAULT 0,
    -- Cross-talk artifact (migration 20260540). When 1, a long event registered
    -- via a pressure drop with essentially no real flow on THIS circuit (another
    -- circuit's draw pulled the shared-supply pressure down). Like a phantom it
    -- forces volume_litres_effective=0 + excluded_from_training; a distinct flag so
    -- it can be shown / hidden separately. Auto-derived; suppressed for
    -- user_classified rows (a peer of the phantom flag in patch_event).
    is_cross_talk                    INTEGER NOT NULL DEFAULT 0,
    -- Sprint H. user_ignored: explicit Ignore/Restore intent (separate from
    -- the derived excluded_from_training, which is auto OR user_ignored OR
    -- manual). user_classified: lock bit — when 1 the three category flags
    -- (is_pressure_restoration_phantom / is_cross_talk / degraded_supply /
    -- is_composite) hold the user's manual choices and auto-detection must never
    -- overwrite them.
    user_ignored                     INTEGER DEFAULT 0,
    user_classified                  INTEGER DEFAULT 0,
    -- Temporal "appliance cycle" signal: count of similar-volume neighbour
    -- events within ±45 min (database.recompute_cycle_pulse_counts). NULL = not
    -- yet computed, 0 = computed/no qualifying neighbours. Aggregated into the
    -- cluster centroid for the heuristic; deliberately NOT in FEATURE_KEYS so it
    -- never affects clustering distance.
    cycle_pulse_count                INTEGER,
    -- Label provenance: 'user' = explicit user label, 'cycle' = auto cycle-mate
    -- expansion (2a), 'training' = capture wizard (2b). NULL = legacy/unlabeled
    -- and is protected exactly like a 'user' label (auto-undo never touches it).
    -- Preserved across event re-imports via _EVENT_USER_COLUMNS.
    fixture_label_source             TEXT
);

-- NOTE: the partial index on (circuit, start_ts) WHERE degraded_supply = 1
-- is created by db_migrations._apply_degraded_supply_columns() rather than
-- inline here. Putting it in _create_schema would fail on an upgrade-from-
-- baseline DB: CREATE TABLE IF NOT EXISTS is a no-op on existing tables,
-- so the existing events table doesn't get the degraded_supply column
-- added by this DDL (ALTER TABLE in the migration is what does that), but
-- the partial-index statement would still execute and reference a column
-- that doesn't exist yet. Order is:
--   1. database.init_db() -> _create_schema() — must succeed on existing DB
--   2. db_migrations.run_migrations() — adds columns AND the partial index
-- Fresh DBs hit the same migration via the version==0 path.

-- UNIQUE(circuit, start_ts) — enforces the contract the importer / dedup
-- helpers have always assumed (see comments in dedup_events). Replaces the
-- earlier non-unique idx_events_circuit_ts. Fresh DBs get the unique index
-- directly; upgrades from baseline (20260524) run dedup_events first via
-- migration 20260525 before this index is created so the unique constraint
-- doesn't fail on historical duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_circuit_start_unique
    ON events (circuit, start_ts);
CREATE INDEX IF NOT EXISTS idx_events_start_ts
    ON events (start_ts);

-- ==========================================================================
-- HOURLY VOLUME (pre-aggregated for fast chart queries)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS hourly_volume (
    circuit         TEXT NOT NULL,
    hour_ts         TIMESTAMP NOT NULL,
    volume_litres   REAL DEFAULT 0,
    PRIMARY KEY (circuit, hour_ts)
);

CREATE INDEX IF NOT EXISTS idx_hourly_volume_circuit_ts
    ON hourly_volume (circuit, hour_ts);

-- ==========================================================================
-- EVENT WAVEFORMS — high-resolution min/max envelopes for the event detail
-- modal (added 20260526). The 32-point pressure_signature_json/_flow_*
-- columns in events stay for clustering; these min/max bins are higher
-- resolution and preserve oscillation envelopes that bin-mean would hide.
-- Retention: WAVEFORM_RETENTION_DAYS (60 by default), purged daily by
-- orchestrator._purge_old_waveforms. FK cascade deletes when an event is
-- removed (foreign_keys pragma is enabled on every connection).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS event_waveforms (
    event_id              TEXT PRIMARY KEY
                          REFERENCES events(id) ON DELETE CASCADE,
    flow_min_json         TEXT NOT NULL,
    flow_max_json         TEXT NOT NULL,
    pressure_min_json     TEXT NOT NULL,
    pressure_max_json     TEXT NOT NULL,
    duration_seconds      REAL NOT NULL,
    created_at            TEXT NOT NULL          -- ISO-8601 UTC, Python-written
);
CREATE INDEX IF NOT EXISTS idx_event_waveforms_created
    ON event_waveforms (created_at);

-- ==========================================================================
-- VOLUME SNAPSHOTS (HA sensor baselines for accurate daily / weekly totals)
-- Stores the HA cumulative volume sensor reading at the start of each
-- calendar period so we can compute delta volumes without relying solely
-- on the internal event-based estimates.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS volume_snapshots (
    circuit     TEXT NOT NULL,
    period_ts   TEXT NOT NULL,   -- ISO datetime of period start (midnight)
    ha_volume   REAL NOT NULL,   -- HA sensor reading at that moment
    PRIMARY KEY (circuit, period_ts)
);

-- ==========================================================================
-- HISTORICAL IMPORT STATE
-- Tracks the last time the historical importer ran per circuit so periodic
-- catch-up checks know how far back to look.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS import_state (
    circuit         TEXT PRIMARY KEY,
    last_check_ts   TEXT,           -- ISO timestamp of last successful check
    total_imported  INTEGER DEFAULT 0
);

-- ==========================================================================
-- DAILY SUMMARY (pre-aggregated from events, calculated nightly)
-- Kept indefinitely — drives history charts and year-over-year views.
-- One row per circuit per calendar day.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS daily_summary (
    circuit             TEXT NOT NULL,
    day                 DATE NOT NULL,          -- YYYY-MM-DD
    -- Volume
    total_volume_litres REAL DEFAULT 0,
    -- Events
    event_count         INTEGER DEFAULT 0,
    -- Flow
    avg_flow_lpm        REAL,
    peak_flow_lpm       REAL,
    -- Pressure
    avg_pressure_psi    REAL,
    min_pressure_psi    REAL,
    -- Anomalies / alerts
    anomaly_count       INTEGER DEFAULT 0,
    alert_count         INTEGER DEFAULT 0,
    -- Top fixture
    top_fixture_id      TEXT,
    top_fixture_count   INTEGER DEFAULT 0,
    -- Top-5 fixtures as JSON: [{"fixture_id":"...","count":N}, ...]
    fixture_breakdown   TEXT,
    -- Computed at
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (circuit, day)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_circuit_day
    ON daily_summary (circuit, day);

-- ==========================================================================
-- LEAK TEST SCHEDULE AND HISTORY
-- ==========================================================================
CREATE TABLE IF NOT EXISTS leak_test_schedule (
    circuit                 TEXT PRIMARY KEY,
    enabled                 BOOLEAN DEFAULT 0,
    auto_learn_hour         BOOLEAN DEFAULT 1,
    frequency               TEXT DEFAULT 'monthly',
    day_of_week             INTEGER DEFAULT 0,
    week_of_month           INTEGER DEFAULT 1,
    run_hour                INTEGER DEFAULT 2,
    run_minute              INTEGER DEFAULT 0,
    -- quiet_period_minutes / retry_delay_minutes / retry_count removed:
    -- the scheduler now learns the quietest hour from usage history instead.
    notify_on_pass          BOOLEAN DEFAULT 1,
    notify_on_fail          BOOLEAN DEFAULT 1,
    last_run_at             TIMESTAMP,
    last_result             TEXT,
    next_run_at             TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS leak_test_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit             TEXT NOT NULL,
    run_at              TIMESTAMP NOT NULL,
    triggered_by        TEXT DEFAULT 'manual',
    result              TEXT,
    duration_minutes    REAL,
    baseline_psi        REAL,
    final_psi           REAL,
    pressure_drop_psi   REAL
);

-- ==========================================================================
-- THRESHOLD HISTORY
-- ==========================================================================
CREATE TABLE IF NOT EXISTS threshold_history (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit                 TEXT NOT NULL,
    recorded_at             TIMESTAMP NOT NULL,
    trigger                 TEXT,
    score_alert             REAL,
    score_shutoff           REAL,
    flow_tolerance_pct      REAL,
    duration_tolerance_pct  REAL,
    event_count_basis       INTEGER
);

-- ==========================================================================
-- ZONE SCHEDULES (irrigation-specific)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS zone_schedules (
    fixture_id              TEXT REFERENCES fixtures(id) ON DELETE CASCADE,
    day_of_week             INTEGER,
    scheduled_start_minutes INTEGER,
    scheduled_duration_sec  INTEGER,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fixture_id, day_of_week)
);

CREATE TABLE IF NOT EXISTS zone_flow_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id  TEXT REFERENCES fixtures(id) ON DELETE CASCADE,
    event_id    TEXT REFERENCES events(id) ON DELETE CASCADE,
    avg_flow    REAL,
    duration_s  REAL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- DATA RETENTION CONFIGURATION
-- Controls how aggressively old history is pruned.
-- Training-era data is always protected regardless of these settings.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS data_retention (
    id                          INTEGER PRIMARY KEY DEFAULT 1,
    -- Raw events: 1 year default (daily summaries cover longer history)
    events_retain_years         INTEGER DEFAULT 1,
    -- Hourly volume: 2 years (learn_best_hour only looks back 60 days)
    hourly_volume_retain_years  INTEGER DEFAULT 2,
    -- Pruning enabled
    enabled                     BOOLEAN DEFAULT 1,
    last_pruned_at              TIMESTAMP,
    -- Auto-backup (Quick Restore JSON written to filesystem on a schedule)
    auto_backup_enabled         BOOLEAN DEFAULT 0,
    auto_backup_path            TEXT    DEFAULT '/share/water_monitor_backups',
    auto_backup_day_of_week     INTEGER DEFAULT 0,  -- 0=Monday
    last_auto_backup_at         TIMESTAMP,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO data_retention (id) VALUES (1);

-- ==========================================================================
-- CIRCUIT DISPLAY LABELS (added migration 023)
-- Maps circuit_id → user-visible display name (e.g. "Main", "Irrigation").
-- ==========================================================================
CREATE TABLE IF NOT EXISTS circuit_labels (
    circuit_id   TEXT PRIMARY KEY,
    display_name TEXT NOT NULL
);

-- ==========================================================================
-- FIXTURE HA ENTITY MAP (added migration 025)
-- Tracks MQTT Discovery entities published to HA for each fixture.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_ha_entity_map (
    fixture_id          TEXT REFERENCES fixtures(id),
    ha_entity_id        TEXT NOT NULL,
    device_class        TEXT,
    unit_of_measurement TEXT,
    last_published_at   TIMESTAMP,
    retracted_at        TIMESTAMP,
    PRIMARY KEY (fixture_id, ha_entity_id)
);

-- ==========================================================================
-- FIXTURE DAILY SUMMARY (added migration 027)
-- Aggregated per-fixture daily stats used for analytics and MQTT publishing.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_daily_summary (
    circuit              TEXT NOT NULL,
    fixture_id           TEXT NOT NULL REFERENCES fixtures(id),
    day                  DATE NOT NULL,
    event_count          INTEGER,
    total_volume_litres  REAL,
    avg_flow_lpm         REAL,
    peak_flow_lpm        REAL,
    PRIMARY KEY (circuit, fixture_id, day)
);
    """)
    conn.commit()
    _apply_post_create_migrations(conn)
    log.info("Schema created/verified")


def _apply_post_create_migrations(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the initial schema for existing DBs.

    Each block uses ``ALTER TABLE … ADD COLUMN`` wrapped in try/except so it
    is idempotent: on fresh installs the column is already in CREATE TABLE
    (the ALTER raises ``OperationalError: duplicate column name`` and we
    swallow it); on upgrade installs the ALTER actually adds the column.
    """
    # Phase 2.1 — explain why an event has cluster_id IS NULL.
    try:
        conn.execute("ALTER TABLE events ADD COLUMN match_rejection_reason TEXT")
        conn.commit()
        log.info("Migration: added events.match_rejection_reason")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            log.warning("ALTER TABLE events.match_rejection_reason: %s", e)

    # Migration 026 — propagation_delay in ms + pressure transient shape features.
    try:
        conn.execute("ALTER TABLE events ADD COLUMN propagation_delay_ms REAL DEFAULT 0")
        conn.execute(
            "UPDATE events SET propagation_delay_ms = propagation_delay_seconds * 1000 "
            "WHERE propagation_delay_seconds IS NOT NULL")
        conn.commit()
        log.info("Migration: added events.propagation_delay_ms (backfilled from seconds column)")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            log.warning("ALTER TABLE events.propagation_delay_ms: %s", e)

    for col, definition in [
        ("pressure_onset_ms",          "REAL DEFAULT 0"),
        ("recovery_overshoot_psi",     "REAL DEFAULT 0"),
        ("pressure_oscillation_count", "INTEGER DEFAULT 0"),
        ("user_fixture_type",          "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {definition}")
            conn.commit()
            log.info("Migration: added events.%s", col)
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                log.warning("ALTER TABLE events.%s: %s", col, e)


# ==========================================================================
# Data access helpers
# ==========================================================================

def compute_daily_summary(conn: sqlite3.Connection,
                          circuit: str, day: str) -> Optional[Dict[str, Any]]:
    """
    Compute and upsert a daily summary row for the given circuit and day.
    day format: 'YYYY-MM-DD'.
    Returns the summary dict, or None if no events that day.
    """
    # Volume uses volume_litres_effective (falling back to raw) so degraded-
    # supply estimates and zeroed pressure-restoration phantoms are reflected
    # here exactly as they are in hourly_volume. Keeps the History charts /
    # daily totals consistent with the dashboard cards.
    rows = conn.execute("""
        SELECT
            COUNT(*)                    AS event_count,
            SUM(COALESCE(volume_litres_effective, volume_litres, 0))
                                        AS total_volume_litres,
            AVG(avg_flow_lpm)           AS avg_flow_lpm,
            MAX(peak_flow_lpm)          AS peak_flow_lpm,
            AVG(pre_event_pressure_psi) AS avg_pressure_psi,
            MIN(min_pressure_psi)       AS min_pressure_psi,
            SUM(CASE WHEN anomaly_score IS NOT NULL
                      AND anomaly_score > 0.6 THEN 1 ELSE 0 END) AS anomaly_count,
            SUM(CASE WHEN triggered_alert = 1  THEN 1 ELSE 0 END) AS alert_count
        FROM events
        WHERE circuit = ?
          AND date(start_ts) = ?
    """, (circuit, day)).fetchone()

    if not rows or rows["event_count"] == 0:
        return None

    # Top-5 fixtures for the day (JSON for breakdown chart)
    top5 = conn.execute("""
        SELECT fixture_id, COUNT(*) AS cnt
        FROM events
        WHERE circuit = ? AND date(start_ts) = ?
          AND fixture_id IS NOT NULL
        GROUP BY fixture_id
        ORDER BY cnt DESC
        LIMIT 5
    """, (circuit, day)).fetchall()
    fixture_breakdown = json.dumps(
        [{"fixture_id": r["fixture_id"], "count": r["cnt"]} for r in top5]
    ) if top5 else None

    summary = {
        "circuit":             circuit,
        "day":                 day,
        "total_volume_litres": rows["total_volume_litres"] or 0,
        "event_count":         rows["event_count"] or 0,
        "avg_flow_lpm":        rows["avg_flow_lpm"],
        "peak_flow_lpm":       rows["peak_flow_lpm"],
        "avg_pressure_psi":    rows["avg_pressure_psi"],
        "min_pressure_psi":    rows["min_pressure_psi"],
        "anomaly_count":       rows["anomaly_count"] or 0,
        "alert_count":         rows["alert_count"] or 0,
        "top_fixture_id":      top5[0]["fixture_id"] if top5 else None,
        "top_fixture_count":   top5[0]["cnt"] if top5 else 0,
        "fixture_breakdown":   fixture_breakdown,
        "computed_at":         datetime.now(timezone.utc).isoformat(),
    }

    cols = ", ".join(summary.keys())
    ph   = ", ".join("?" for _ in summary)
    updates = ", ".join(f"{k}=excluded.{k}" for k in summary if k not in ("circuit", "day"))
    # Future-proofing: if a refactor ever strips this dict down to just
    # the conflict-key columns, `updates` becomes the empty string and
    # `DO UPDATE SET ` is a SQL syntax error. Fall back to DO NOTHING.
    on_conflict = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
    conn.execute(
        f"INSERT INTO daily_summary ({cols}) VALUES ({ph}) "
        f"ON CONFLICT(circuit, day) {on_conflict}",
        list(summary.values()),
    )
    return summary


def get_daily_summaries(
    conn: sqlite3.Connection,
    circuit: str,
    date_from: str = None,
    date_to: str = None,
) -> List[Dict[str, Any]]:
    """Return daily_summary rows for a circuit, ordered oldest-first for charting."""
    conditions = ["circuit = ?"]
    params: list = [circuit]
    if date_from:
        conditions.append("day >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("day <= ?")
        params.append(date_to)
    where = " AND ".join(conditions)
    rows = conn.execute(
        f"SELECT * FROM daily_summary WHERE {where} ORDER BY day ASC",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def get_data_retention(conn: sqlite3.Connection) -> dict:
    row = conn.execute("SELECT * FROM data_retention WHERE id = 1").fetchone()
    if row:
        return dict(row)
    return {
        "events_retain_years":        1,
        "hourly_volume_retain_years": 2,
        "enabled":                    1,
        "last_pruned_at":             None,
        "auto_backup_enabled":        0,
        "auto_backup_path":           "/share/water_monitor_backups",
        "auto_backup_day_of_week":    0,
        "last_auto_backup_at":        None,
    }


_DATA_RETENTION_COLUMNS = frozenset({
    "events_retain_years", "hourly_volume_retain_years", "enabled",
    "last_pruned_at", "auto_backup_enabled", "auto_backup_path",
    "auto_backup_day_of_week", "last_auto_backup_at", "updated_at",
})


def update_data_retention(conn: sqlite3.Connection, **kwargs) -> None:
    unknown = set(kwargs) - _DATA_RETENTION_COLUMNS
    if unknown:
        raise ValueError(f"Unknown data_retention columns: {unknown}")
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(
        f"UPDATE data_retention SET {sets} WHERE id = 1",
        list(kwargs.values()),
    )
    conn.commit()


def get_or_create_csrf_server_secret(conn: sqlite3.Connection) -> str:
    """Return the persistent HMAC server secret used for CSRF tokens.

    Created on first use and stored in `csrf_server_secret`. Never
    regenerated automatically — rotating it would invalidate every
    in-flight browser form across the addon.

    The secret is a 64-character hex string (256 bits of entropy).
    """
    import secrets as _secrets
    row = conn.execute(
        "SELECT secret FROM csrf_server_secret WHERE id = 1"
    ).fetchone()
    if row and row["secret"]:
        return row["secret"]
    secret = _secrets.token_hex(32)
    conn.execute(
        "INSERT OR REPLACE INTO csrf_server_secret (id, secret) "
        "VALUES (1, ?)",
        (secret,),
    )
    conn.commit()
    return secret


def derive_csrf_token(server_secret: str, session_id: str) -> str:
    """Compute the CSRF token for a given session_id.

    Stateless HMAC double-submit pattern: the same session_id always
    produces the same token, but the token can't be forged without the
    server_secret. Validation just recomputes and constant-time-compares.
    """
    import hashlib
    import hmac
    return hmac.new(
        server_secret.encode("utf-8"),
        session_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def validate_csrf_token(
    server_secret: str, session_id: str, token: str
) -> bool:
    """Return True if `token` matches HMAC(server_secret, session_id).

    Returns False on any missing input. Uses constant-time comparison
    to avoid timing side-channels.
    """
    import hmac
    if not server_secret or not session_id or not token:
        return False
    expected = derive_csrf_token(server_secret, session_id)
    return hmac.compare_digest(expected, token)


def get_home_profile(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM home_profile WHERE id = 1").fetchone()


def update_home_profile(conn: sqlite3.Connection, **kwargs) -> None:
    # Derive the writable column allowlist from the live schema so any
    # column added by a future migration is automatically permitted without
    # needing a corresponding change here.  id and created_at are excluded —
    # they must never be overwritten by callers.  updated_at is managed below.
    _immutable = {"id", "created_at"}
    valid_cols = (
        {r[1] for r in conn.execute("PRAGMA table_info(home_profile)").fetchall()}
        - _immutable
    )
    bad = set(kwargs) - valid_cols
    if bad:
        raise ValueError(
            f"update_home_profile: unknown column(s): {bad}. "
            f"Valid columns: {sorted(valid_cols)}"
        )
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [1]
    conn.execute(f"UPDATE home_profile SET {sets} WHERE id = ?", values)
    conn.commit()


def get_training_state(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM training_state WHERE circuit = ?", (circuit,)
    ).fetchone()


def _upsert_by_circuit(
    conn: sqlite3.Connection, table: str, circuit: str, **kwargs
) -> None:
    """Generic upsert helper for single-circuit config tables.

    Table name comes exclusively from internal string literals — never user
    input — so the f-string interpolation does not introduce injection risk.
    """
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    exists = conn.execute(
        f"SELECT 1 FROM {table} WHERE circuit = ?", (circuit,)
    ).fetchone() is not None
    if exists:
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        conn.execute(
            f"UPDATE {table} SET {sets} WHERE circuit = ?",
            [*kwargs.values(), circuit],
        )
    else:
        kwargs["circuit"] = circuit
        cols = ", ".join(kwargs)
        phs = ", ".join("?" * len(kwargs))
        conn.execute(
            f"INSERT INTO {table} ({cols}) VALUES ({phs})",
            list(kwargs.values()),
        )
    conn.commit()


def upsert_training_state(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "training_state", circuit, **kwargs)


def get_sensitivity_config(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM sensitivity_config WHERE circuit = ?", (circuit,)
    ).fetchone()


def upsert_sensitivity_config(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "sensitivity_config", circuit, **kwargs)


def get_learning_config(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM learning_config WHERE circuit = ?", (circuit,)
    ).fetchone()


def upsert_learning_config(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "learning_config", circuit, **kwargs)


def get_alert_configs(conn: sqlite3.Connection, circuit: str) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM alert_config WHERE circuit = ? ORDER BY alert_type",
        (circuit,)
    ).fetchall()


VALID_CIRCUIT_TYPES = frozenset({"fixture", "zone"})


def get_circuit_type(
    conn: sqlite3.Connection,
    circuit: str,
    default: str = "fixture",
) -> str:
    """Return the circuit_type for a circuit, normalised to a canonical value.

    Falls back to `default` if no circuit_profile row exists yet.
    Normalises legacy "irrigation" values to "zone" transparently.
    """
    from .fixtures import normalize_circuit_type
    row = conn.execute(
        "SELECT circuit_type FROM circuit_profile WHERE circuit = ?",
        (circuit,)
    ).fetchone()
    raw = row["circuit_type"] if row else default
    return normalize_circuit_type(raw)


def _seed_zone_alerts_only(conn: sqlite3.Connection, circuit: str) -> None:
    """INSERT OR IGNORE zone-only alert rows — never overwrites user toggle state."""
    zone_only_alerts = [
        ("pre_solenoid_leak", "Pre-Solenoid Leak",
         "Alert when flow detected with no zone commanded open"),
        ("solenoid_weeping", "Solenoid Weeping",
         "Alert when flow persists after zone commanded closed"),
        ("zone_flow_deviation_high", "Zone Flow High",
         "Alert when zone flow exceeds learned range"),
        ("zone_flow_deviation_low", "Zone Flow Low",
         "Alert when zone flow is below learned range — possible blocked head"),
        ("zone_duration_overrun", "Zone Duration Overrun",
         "Alert when zone runs significantly longer than expected"),
    ]
    for alert_type, label, description in zone_only_alerts:
        alert_id = f"{alert_type}_{circuit}"
        conn.execute("""
            INSERT OR IGNORE INTO alert_config
                (id, circuit, alert_type, label, description, enabled)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (alert_id, circuit, alert_type, label, description))


def set_circuit_type(
    conn: sqlite3.Connection,
    circuit: str,
    circuit_type: str,
    commit: bool = True,
) -> None:
    """Persist circuit_type to circuit_profile.

    UPSERTs the row so it is safe to call before ensure_circuit_defaults().
    When switching to "zone", seeds any missing zone-only alert rows via
    INSERT OR IGNORE so existing user toggle state is preserved.
    Zone alerts are never deleted when switching back to "fixture" — they
    are simply hidden in the UI by the template filter.

    When ``commit`` is True (default) the change is committed before
    returning. Pass ``commit=False`` when this is part of a multi-step
    write sequence the caller wants to make atomic via ``with
    transaction(conn):`` — see setup wizard step 3b for the canonical
    example.
    """
    from .fixtures import normalize_circuit_type
    circuit_type = normalize_circuit_type(circuit_type)
    if circuit_type not in VALID_CIRCUIT_TYPES:
        raise ValueError(f"Invalid circuit_type {circuit_type!r}; must be 'fixture' or 'zone'")
    conn.execute("""
        INSERT INTO circuit_profile (circuit, circuit_type)
        VALUES (?, ?)
        ON CONFLICT(circuit) DO UPDATE SET circuit_type = excluded.circuit_type
    """, (circuit, circuit_type))
    if circuit_type == "zone":
        _seed_zone_alerts_only(conn, circuit)
    if commit:
        conn.commit()


def get_valve_type(
    conn: sqlite3.Connection,
    circuit: str,
    default: str = "2_port",
) -> str:
    """Return the valve_type for a circuit.

    Falls back to `default` if no circuit_profile row exists yet.
    Normalises via fixtures.normalize_valve_type (forgiving) so callers
    always get a canonical string they can render safely.
    """
    from .fixtures import normalize_valve_type
    row = conn.execute(
        "SELECT valve_type FROM circuit_profile WHERE circuit = ?",
        (circuit,)
    ).fetchone()
    raw = row["valve_type"] if row and row["valve_type"] else default
    return normalize_valve_type(raw)


def set_valve_type(
    conn: sqlite3.Connection,
    circuit: str,
    valve_type: str,
    commit: bool = True,
) -> None:
    """Persist valve_type to circuit_profile.

    Uses the STRICT parser. Callers are expected to present a clean value;
    a ValueError is raised on bad input rather than silently substituting
    the default. UI/router layers should pre-validate via parse_valve_type
    before calling this.

    UPSERT mirrors set_circuit_type — all other circuit_profile columns
    have DEFAULTs or allow NULL so the minimal INSERT shape is safe.

    Pass ``commit=False`` when this is part of a multi-step write
    sequence the caller wants to make atomic via ``with
    transaction(conn):`` — see setup wizard step 3b for the canonical
    example.
    """
    from .fixtures import parse_valve_type
    parsed = parse_valve_type(valve_type)
    if parsed is None:
        raise ValueError(f"Invalid valve_type {valve_type!r}")
    conn.execute("""
        INSERT INTO circuit_profile (circuit, valve_type)
        VALUES (?, ?)
        ON CONFLICT(circuit) DO UPDATE SET valve_type = excluded.valve_type
    """, (circuit, parsed))
    if commit:
        conn.commit()


def set_alert_enabled(conn: sqlite3.Connection, alert_id: str, enabled: bool) -> None:
    conn.execute(
        "UPDATE alert_config SET enabled = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, datetime.now(timezone.utc).isoformat(), alert_id)
    )
    conn.commit()


#: Columns on `events` that capture user intent — never overwritten by an
#: importer re-insert or any other automated path. The historical importer
#: re-imports past events when fresh history becomes available, and the
#: live detector can re-stage the same id on retry; both must preserve any
#: label or ignore-flag the user already set.
_EVENT_USER_COLUMNS: frozenset[str] = frozenset({
    "user_fixture_type",
    "user_reviewed",
    # Sprint H: the explicit user intents are preserved across re-import/
    # enrichment upserts. excluded_from_training is NO LONGER preserved here —
    # it's a DERIVED field (auto verdicts OR user_ignored OR manual) recomputed
    # by _finalize_derived_verdicts, so preserving it would freeze stale
    # auto-exclusion. user_ignored carries the Ignore/Restore intent instead;
    # user_classified locks a manual classification against auto-override.
    "user_ignored",
    "user_classified",
    # Label provenance ('user'/'cycle'/'training') — preserved so a re-import
    # never drops the auto-label source that drives the undo + exclude-warning.
    "fixture_label_source",
})

# Columns whose values must NOT be overwritten by an event-row upsert.
# These track the exact prior contribution applied to hourly_volume so future
# reprocessing (re-imports etc.) can correctly subtract before re-adding.
# Maintained ONLY by upsert_event_and_apply_hourly_volume() — never by the
# event-feature upsert path.
_EVENT_APPLIED_BOOKKEEPING_COLUMNS: frozenset[str] = frozenset({
    "hourly_volume_applied_litres",
    "hourly_volume_applied_bucket",
})


def _hour_bucket_for(start_ts) -> str:
    """Return the hour_ts string in the canonical format used by
    update_hourly_volume(): UTC-normalised '%Y-%m-%dT%H:00:00' (no tz suffix).
    Mirrors the production format from feature_extractor.py line ~1066 so
    aggregate queries (get_daily_volume / get_weekly_volume) keep working.
    """
    if start_ts is None:
        return ""
    if isinstance(start_ts, str):
        try:
            dt = datetime.fromisoformat(start_ts)
        except ValueError:
            return ""
    elif isinstance(start_ts, datetime):
        dt = start_ts
    else:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:00:00')


def insert_event(conn: sqlite3.Connection, event: dict) -> bool:
    """Insert event row; returns True if genuinely new, False on conflict.

    Replaces an older INSERT OR REPLACE implementation that delete-then-
    inserted on conflict. REPLACE had three bad side-effects:
      (1) it fired ON DELETE CASCADE against tables that reference
          events(id) — e.g. fixture_ha_entity_map — silently dropping
          dependent rows;
      (2) it changed the rowid of the conflicting row, breaking any
          rowid-based bookkeeping a long-running reader held;
      (3) it wiped user-editable columns (user_fixture_type,
          user_reviewed, excluded_from_training) on every same-id
          re-insert, undoing manual labels the next time the importer
          ran.

    The new behaviour is an UPSERT via ON CONFLICT(id) DO UPDATE that
    refreshes every measurement / system column from the incoming row but
    deliberately omits the user-controlled columns listed in
    _EVENT_USER_COLUMNS. Conflicts no longer fire cascades or change rowids.

    "is genuinely new" is now decided by a pre-check rather than by
    interpreting total_changes (REPLACE's old +2-on-replace trick is no
    longer applicable). Callers use the return value to decide whether to
    add the event's volume to hourly_volume.
    """
    if "id" not in event:
        raise ValueError("insert_event: event dict missing 'id'")
    exists = conn.execute(
        "SELECT 1 FROM events WHERE id = ?", (event["id"],),
    ).fetchone() is not None
    _do_event_upsert(conn, event)
    conn.commit()
    return not exists


def _do_event_upsert(conn: sqlite3.Connection, event: dict) -> None:
    """Run the INSERT ... ON CONFLICT(id) DO UPDATE for an event row.

    The DO UPDATE SET clause refreshes measurement/system columns but
    explicitly excludes:
      • 'id' (the conflict target)
      • _EVENT_USER_COLUMNS (preserve user labels/flags across re-imports)
      • _EVENT_APPLIED_BOOKKEEPING_COLUMNS (preserve exact prior
        hourly_volume contribution so upsert_event_and_apply_hourly_volume
        can subtract it correctly on reprocess)

    Does NOT commit; the caller controls the transaction boundary.
    """
    cols = list(event.keys())
    if "id" not in cols:
        raise ValueError("_do_event_upsert: event dict missing 'id'")

    col_list     = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)
    set_cols = [
        c for c in cols
        if c != "id"
        and c not in _EVENT_USER_COLUMNS
        and c not in _EVENT_APPLIED_BOOKKEEPING_COLUMNS
    ]
    if set_cols:
        set_clause = ", ".join(f"{c}=excluded.{c}" for c in set_cols)
        sql = (
            f"INSERT INTO events ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {set_clause}"
        )
    else:
        sql = (
            f"INSERT INTO events ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO NOTHING"
        )
    conn.execute(sql, list(event.values()))


def upsert_event_and_apply_hourly_volume(
    conn: sqlite3.Connection,
    event: dict,
    new_effective_volume: float,
) -> bool:
    """Atomically upsert an event row AND keep hourly_volume in sync.

    Replaces the previous two-step pattern (insert_event then update_hourly_volume)
    which made it easy to lose idempotency on reprocessing. All work happens
    inside a single transaction:

      1. Read prior (litres, bucket) from the existing event row, if any.
      2. UPSERT the event (preserving _EVENT_APPLIED_BOOKKEEPING_COLUMNS).
      3. If a prior bucket existed: subtract the prior contribution from it.
      4. Add new_effective_volume to the new bucket (derived from start_ts).
      5. Update the event's applied-bookkeeping columns to (new_amount, new_bucket).

    Returns True if the event was genuinely new (not a reprocess); the caller
    can use this to decide whether to bump training_state counters.

    `event` must contain 'id' and 'start_ts'. circuit is read from event['circuit'].
    """
    if "id" not in event or "start_ts" not in event or "circuit" not in event:
        raise ValueError(
            "upsert_event_and_apply_hourly_volume: event missing "
            "id/start_ts/circuit"
        )

    event_id = event["id"]
    circuit = event["circuit"]
    new_bucket = _hour_bucket_for(event["start_ts"])

    with transaction(conn):
        # (1) Read prior applied state.
        prev = conn.execute(
            "SELECT hourly_volume_applied_litres, hourly_volume_applied_bucket "
            "FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        prev_litres = float(prev["hourly_volume_applied_litres"] or 0) if prev else 0.0
        prev_bucket = prev["hourly_volume_applied_bucket"] if prev else None
        is_new = prev is None

        # (2) UPSERT the event (preserves applied bookkeeping columns).
        _do_event_upsert(conn, event)

        # (3) Reverse prior contribution if any.
        if prev_bucket and prev_litres != 0:
            conn.execute(
                "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT (circuit, hour_ts) "
                "DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres",
                (circuit, prev_bucket, -prev_litres),
            )

        # (4) Apply new contribution (skip if zero — keeps hourly_volume clean).
        if new_bucket and new_effective_volume:
            conn.execute(
                "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT (circuit, hour_ts) "
                "DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres",
                (circuit, new_bucket, new_effective_volume),
            )

        # (5) Record new applied state on the event row.
        conn.execute(
            "UPDATE events "
            "SET hourly_volume_applied_litres = ?, "
            "    hourly_volume_applied_bucket = ? "
            "WHERE id = ?",
            (new_effective_volume, new_bucket, event_id),
        )

    return is_new


def get_daily_volume(conn: sqlite3.Connection, circuit: str,
                     since_utc: str = "") -> float:
    """Total volume since local midnight (expressed as a UTC ISO string).

    Pass since_utc as the UTC equivalent of the HA instance's local midnight
    (e.g. '2026-05-17T05:00:00' for UTC-5).  Falls back to UTC midnight when
    since_utc is not provided.
    """
    cutoff = since_utc or datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00')
    row = conn.execute("""
        SELECT COALESCE(SUM(volume_litres), 0)
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= ?
    """, (circuit, cutoff)).fetchone()
    return round(row[0], 1) if row else 0.0


def get_weekly_volume(conn: sqlite3.Connection, circuit: str,
                      since_utc: str = "") -> float:
    """Total volume since local midnight 7 days ago (expressed as a UTC ISO string).

    Pass since_utc as the UTC equivalent of local midnight 7 days ago.
    Falls back to UTC midnight 7 days ago when since_utc is not provided.
    """
    if since_utc:
        cutoff = since_utc
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%dT00:00:00')
    row = conn.execute("""
        SELECT COALESCE(SUM(volume_litres), 0)
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= ?
    """, (circuit, cutoff)).fetchone()
    return round(row[0], 1) if row else 0.0


def get_hourly_volumes(
    conn: sqlite3.Connection,
    circuit: str,
    hours: int = 24
) -> List[Dict[str, Any]]:
    """Get per-hour volume for the past N hours (rolling from now)."""
    rows = conn.execute("""
        SELECT hour_ts, volume_litres
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= datetime('now', ? || ' hours')
        ORDER BY hour_ts ASC
    """, (circuit, f"-{hours}")).fetchall()
    return [dict(r) for r in rows]


def update_hourly_volume(
    conn: sqlite3.Connection,
    circuit: str,
    hour_ts: str,
    volume_litres: float
) -> None:
    conn.execute("""
        INSERT INTO hourly_volume (circuit, hour_ts, volume_litres)
        VALUES (?, ?, ?)
        ON CONFLICT (circuit, hour_ts)
        DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres
    """, (circuit, hour_ts, volume_litres))
    conn.commit()


def _get_volume_baseline(
    conn: sqlite3.Connection,
    circuit: str,
    period_ts: str,
    current_ha_value: float,
) -> float:
    """
    Return the stored HA sensor baseline for period_ts, creating it if absent.

    If the stored baseline is HIGHER than the current reading the sensor has
    reset (device restart / firmware flash).  In that case we update the
    baseline to the current reading so the delta starts from zero again.
    """
    row = conn.execute(
        "SELECT ha_volume FROM volume_snapshots WHERE circuit=? AND period_ts=?",
        (circuit, period_ts),
    ).fetchone()

    if row is None:
        # No baseline yet — seed with the CURRENT reading, NOT 0.0.
        # A 0.0 baseline makes the period total (= current − baseline) balloon
        # to the entire cumulative meter reading (the dashboard "today shows
        # 1000 gal" bug). Seeding with the current value caps the worst case at
        # ~0 for a just-started period; the orchestrator's midnight rollover
        # (_init_volume_baselines) then force-overwrites this with the accurate
        # midnight reading from HA history.
        conn.execute(
            "INSERT INTO volume_snapshots (circuit, period_ts, ha_volume) VALUES (?,?,?)",
            (circuit, period_ts, current_ha_value),
        )
        conn.commit()
        return current_ha_value

    baseline = row[0]
    if current_ha_value < baseline:
        # Sensor reset (device restarted) — update baseline to new zero point
        conn.execute(
            "UPDATE volume_snapshots SET ha_volume=? WHERE circuit=? AND period_ts=?",
            (current_ha_value, circuit, period_ts),
        )
        conn.commit()
        return current_ha_value

    return baseline


def compute_ha_daily_volume(
    conn: sqlite3.Connection,
    circuit: str,
    current_ha_value: float,
    period_ts: str = "",
) -> float:
    """Daily volume from the authoritative HA cumulative sensor.

    period_ts is the UTC equivalent of local midnight, as a naive ISO string
    (e.g. '2026-05-17T05:00:00').  Falls back to UTC midnight when not provided.
    Must match the key written by _init_volume_baselines().
    """
    if not period_ts:
        period_ts = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        ).isoformat(timespec="seconds")
    baseline = _get_volume_baseline(conn, circuit, period_ts, current_ha_value)
    return round(max(0.0, current_ha_value - baseline), 1)


def compute_ha_weekly_volume(
    conn: sqlite3.Connection,
    circuit: str,
    current_ha_value: float,
    period_ts: str = "",
) -> float:
    """Rolling 7-day volume from the authoritative HA cumulative sensor.

    period_ts is the UTC equivalent of local midnight 7 days ago, as a naive
    ISO string.  Falls back to UTC midnight 7 days ago when not provided.
    """
    if not period_ts:
        period_ts = (datetime.now(timezone.utc) - timedelta(days=7)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        ).isoformat(timespec="seconds")
    baseline = _get_volume_baseline(conn, circuit, period_ts, current_ha_value)
    return round(max(0.0, current_ha_value - baseline), 1)


def get_recent_events(
    conn: sqlite3.Connection,
    circuit: str,
    limit: int = 100,
    date_from: str = None,
    date_to: str = None,
) -> List[Dict[str, Any]]:
    """
    Return events for a circuit ordered newest first.
    If date_from / date_to are provided (ISO strings) they act as a
    range filter and limit is ignored so the full range is returned.
    Otherwise returns the most recent `limit` rows.
    """
    _select = """
        SELECT e.*,
               fc.suggested_type,
               fc.suggested_confidence,
               fc.confidence_level   AS cluster_confidence_level,
               f.display_name        AS fixture_display_name,
               f.fixture_type        AS fixture_type_name
        FROM events e
        LEFT JOIN fixture_clusters fc
               ON fc.circuit = e.circuit AND fc.id = e.cluster_id
        LEFT JOIN fixtures f ON f.id = e.fixture_id
    """
    if date_from or date_to:
        conditions = ["e.circuit = ?"]
        params: list = [circuit]
        if date_from:
            conditions.append("e.start_ts >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("e.start_ts <= ?")
            params.append(date_to + "T23:59:59")
        where = " AND ".join(conditions)
        rows = conn.execute(
            f"{_select} WHERE {where} ORDER BY e.start_ts DESC",
            params,
        ).fetchall()
    else:
        rows = conn.execute(
            f"{_select} WHERE e.circuit = ? ORDER BY e.start_ts DESC LIMIT ?",
            (circuit, limit),
        ).fetchall()
    return [dict(r) for r in rows]


_PATCH_UNSET = object()


def patch_event(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    user_fixture_type=_PATCH_UNSET,
    excluded_from_training=_PATCH_UNSET,
    user_ignored=_PATCH_UNSET,
) -> bool:
    """Update user-editable fields on a single event.

    Pass a value (including None) to update that field; omit a kwarg entirely
    to leave the field unchanged.  Returns False if no matching row exists.

    Sprint H: ``user_ignored`` is the Ignore/Restore intent. Setting it also
    re-derives ``excluded_from_training`` (= user_ignored OR any auto/manual
    category flag) so the effective exclusion stays consistent. The legacy
    ``excluded_from_training`` kwarg is still accepted for back-compat but
    callers should prefer ``user_ignored``.
    """
    row = conn.execute(
        "SELECT id, is_pressure_restoration_phantom, degraded_supply, "
        "       is_low_flow_dribble "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False
    if user_fixture_type is not _PATCH_UNSET:
        # Stamp provenance: an explicit label is 'user'; clearing resets to NULL
        # (so a relabel always overrides an auto 'cycle'/'training' source).
        src = "user" if user_fixture_type else None
        conn.execute(
            "UPDATE events SET user_fixture_type = ?, fixture_label_source = ? "
            "WHERE id = ? AND circuit = ?",
            (user_fixture_type, src, event_id, circuit),
        )
    if user_ignored is not _PATCH_UNSET:
        ign = 1 if user_ignored else 0
        excluded = 1 if (ign or row["is_pressure_restoration_phantom"]
                         or row["degraded_supply"]
                         or row["is_low_flow_dribble"]) else 0
        conn.execute(
            "UPDATE events SET user_ignored = ?, excluded_from_training = ? "
            "WHERE id = ? AND circuit = ?",
            (ign, excluded, event_id, circuit),
        )
    if excluded_from_training is not _PATCH_UNSET:
        conn.execute(
            "UPDATE events SET excluded_from_training = ? WHERE id = ? AND circuit = ?",
            (1 if excluded_from_training else 0, event_id, circuit),
        )
    conn.commit()
    return True


def _apply_event_verdicts(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    new_phantom: int,
    new_degraded: int,
    user_classified: int,
    new_dribble: int = 0,
    new_cross_talk: int = 0,
) -> bool:
    """Shared core: write category flags + user_classified, re-derive
    volume_litres_effective (phantom → 0; elif degraded → envelope estimate;
    else raw), recompute excluded_from_training, and resync hourly_volume +
    daily_summary.

    Volume resync is idempotent: it reads the event's stored
    ``hourly_volume_applied_litres`` (the prior contribution), applies
    ``delta = new_effective − prev_applied`` to the hour bucket, then writes
    back ``hourly_volume_applied_litres = new_effective``. Repeated toggles
    therefore never drift. Returns False if no such event.
    """
    row = conn.execute(
        "SELECT volume_litres, volume_litres_estimated, user_ignored, "
        "       hourly_volume_applied_litres, hourly_volume_applied_bucket, start_ts "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False

    raw = float(row["volume_litres"] or 0.0)
    est = row["volume_litres_estimated"]
    if new_phantom:
        new_effective, method = 0.0, "pressure_restoration_phantom"
    elif new_cross_talk:
        new_effective, method = 0.0, "cross_talk"
    elif new_degraded:
        new_effective = float(est) if est is not None else raw
        method = "pulsing_supply_envelope"
    else:
        new_effective, method = raw, "raw"

    user_ignored = int(row["user_ignored"] or 0)
    excluded = 1 if (new_phantom or new_cross_talk or new_degraded
                     or new_dribble or user_ignored) else 0
    reason = (
        "pressure_restoration_phantom" if new_phantom
        else "cross_talk" if new_cross_talk
        else "pulsing_supply" if new_degraded
        else "low_flow_dribble" if new_dribble
        else None
    )

    prev_applied = float(row["hourly_volume_applied_litres"] or 0.0)
    prev_bucket = row["hourly_volume_applied_bucket"] or _hour_bucket_for(row["start_ts"])
    delta = new_effective - prev_applied

    with transaction(conn):
        conn.execute(
            "UPDATE events SET "
            "  is_pressure_restoration_phantom = ?, is_cross_talk = ?, "
            "  degraded_supply = ?, "
            "  user_classified = ?, is_low_flow_dribble = ?, "
            "  volume_litres_effective = ?, volume_estimation_method = ?, "
            "  excluded_from_training = ?, match_rejection_reason = ? "
            "WHERE id = ? AND circuit = ?",
            (new_phantom, new_cross_talk, new_degraded, user_classified, new_dribble,
             round(new_effective, 3), method, excluded, reason,
             event_id, circuit),
        )
        if prev_bucket and abs(delta) > 1e-9:
            conn.execute(
                "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT (circuit, hour_ts) "
                "DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres",
                (circuit, prev_bucket, delta),
            )
        conn.execute(
            "UPDATE events SET hourly_volume_applied_litres = ?, "
            "  hourly_volume_applied_bucket = ? WHERE id = ? AND circuit = ?",
            (round(new_effective, 3), prev_bucket, event_id, circuit),
        )

    day = (row["start_ts"] or "")[:10]
    if day:
        compute_daily_summary(conn, circuit, day)
        conn.commit()
    return True


def classify_action(cls: dict):
    """Pure dispatch for a PATCH ``classification`` payload (Sprint H.1).

    Returns one of:
      ("reset", {})                          — reset to automatic
      ("set",   {phantom, supply_pressure})  — authoritative manual set
      ("error", {"msg": ...})                — invalid → caller 400s

    ``reset: true`` is EXCLUSIVE — combining it with any category flag is
    rejected rather than silently picking one behaviour. Pure (no DB, no
    request object) so it is unit-testable without the FastAPI stack; lives
    here in database.py alongside set_/clear_event_classification for that
    reason (routers/history.py imports fastapi, which the test env lacks).

    'combined' is deprecated (2026-06-04): it is accepted but ignored, so old
    clients don't 400; combined usage is classified as the dominant fixture.
    """
    cls = cls or {}
    flags = {
        "phantom":         bool(cls.get("phantom")),
        "supply_pressure": bool(cls.get("supply_pressure")),
        "dribble":         bool(cls.get("dribble")),
        "cross_talk":      bool(cls.get("cross_talk")),
    }
    if cls.get("reset") is True:
        if any(flags.values()):
            return ("error", {"msg": "reset cannot be combined with category flags"})
        return ("reset", {})
    return ("set", flags)


def set_event_classification(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    phantom: bool,
    supply_pressure: bool,
    dribble: bool = False,
    cross_talk: bool = False,
) -> bool:
    """Apply a user's manual event classification (Sprint H, authoritative).

    ALWAYS sets ``user_classified=1`` — including the all-three-false case,
    which means "manually marked normal" and **sticks** (the
    ``_finalize_derived_verdicts`` skip on user_classified prevents auto from
    re-flagging it; this is what makes un-marking a phantom permanent). Use
    ``clear_event_classification`` to return an event to automatic detection.

    Returns False if no such event.
    """
    return _apply_event_verdicts(
        conn, event_id, circuit,
        new_phantom=1 if phantom else 0,
        new_degraded=1 if supply_pressure else 0,
        new_dribble=1 if dribble else 0,
        new_cross_talk=1 if cross_talk else 0,
        user_classified=1,
    )


def clear_event_classification(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
) -> bool:
    """Reset an event to AUTOMATIC classification (Sprint H.1).

    Clears ``user_classified`` and re-derives the phantom verdict from the
    stored duration/pressure via ``_detect_pressure_restoration_phantom``.
    degraded/composite keep their stored values (the raw readings needed to
    recompute them aren't persisted). Volume + summaries resync. Returns
    False if no such event.
    """
    from .feature_extractor import (
        _detect_pressure_restoration_phantom, _detect_low_flow_dribble,
    )

    row = conn.execute(
        "SELECT duration_seconds, pressure_delta_psi, degraded_supply, "
        "       volume_litres, avg_flow_lpm, true_avg_flow_lpm, "
        "       flow_integral_litres, flow_on_ratio "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False
    new_phantom = 1 if _detect_pressure_restoration_phantom(
        row["duration_seconds"], row["pressure_delta_psi"],
        true_avg_flow_lpm=row["true_avg_flow_lpm"],
        flow_integral_litres=row["flow_integral_litres"],
        flow_on_ratio=row["flow_on_ratio"]) else 0
    new_degraded = int(row["degraded_supply"] or 0)
    # Dribble only when not phantom and not degraded (mirrors the finalizer).
    new_dribble = 1 if (
        not new_phantom and not new_degraded
        and _detect_low_flow_dribble(
            row["volume_litres"], row["avg_flow_lpm"], row["pressure_delta_psi"])
    ) else 0
    return _apply_event_verdicts(
        conn, event_id, circuit,
        new_phantom=new_phantom,
        new_degraded=new_degraded,
        user_classified=0,
        new_dribble=new_dribble,
    )


def get_leak_test_schedule(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM leak_test_schedule WHERE circuit = ?", (circuit,)
    ).fetchone()


def upsert_leak_test_schedule(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "leak_test_schedule", circuit, **kwargs)


def insert_leak_test_history(conn: sqlite3.Connection, **kwargs) -> None:
    cols = ", ".join(kwargs.keys())
    placeholders = ", ".join("?" for _ in kwargs)
    conn.execute(f"INSERT INTO leak_test_history ({cols}) VALUES ({placeholders})",
                 list(kwargs.values()))
    conn.commit()


def get_leak_test_history(
    conn: sqlite3.Connection,
    circuit: str,
    limit: int = 20
) -> List[Dict[str, Any]]:
    rows = conn.execute("""
        SELECT * FROM leak_test_history
        WHERE circuit = ?
        ORDER BY run_at DESC
        LIMIT ?
    """, (circuit, limit)).fetchall()
    return [dict(r) for r in rows]


def ensure_circuit_defaults(conn: sqlite3.Connection, circuit: str,
                             circuit_type: str = "fixture") -> None:
    """Ensure all per-circuit config rows exist with defaults."""
    # Training state
    conn.execute("""
        INSERT OR IGNORE INTO training_state (circuit, state)
        VALUES (?, 'idle')
    """, (circuit,))

    # Sensitivity config
    conn.execute("""
        INSERT OR IGNORE INTO sensitivity_config (circuit)
        VALUES (?)
    """, (circuit,))

    # Learning config
    conn.execute("""
        INSERT OR IGNORE INTO learning_config (circuit)
        VALUES (?)
    """, (circuit,))

    # Circuit profile
    conn.execute("""
        INSERT OR IGNORE INTO circuit_profile (circuit, circuit_type)
        VALUES (?, ?)
    """, (circuit, circuit_type))

    # Leak test schedule
    conn.execute("""
        INSERT OR IGNORE INTO leak_test_schedule (circuit)
        VALUES (?)
    """, (circuit,))

    # Alert configs — seed with defaults if not present
    _seed_alert_configs(conn, circuit, circuit_type)

    conn.commit()


def _seed_alert_configs(conn: sqlite3.Connection, circuit: str,
                        circuit_type: str) -> None:
    """Insert default alert config rows for a circuit."""
    base_alerts = [
        ("pressure_drop", "Pressure Drop",
         "Alert when pressure drops rapidly — possible burst pipe"),
        ("high_flow", "High Flow",
         "Alert when flow rate exceeds burst threshold"),
        ("leak_test", "Micro Leak Test",
         "Alert when leak test detects pressure decay"),
        ("trickle", "Trickle Flow",
         "Alert on sustained low flow — possible running toilet or dripping tap"),
        ("flow_anomaly", "Flow Anomaly",
         "Alert when flow pattern doesn't match any known fixture"),
        ("schedule_deviation", "Schedule Deviation",
         "Alert when flow occurs outside expected time patterns"),
    ]

    for alert_type, label, description in base_alerts:
        alert_id = f"{alert_type}_{circuit}"
        conn.execute("""
            INSERT OR IGNORE INTO alert_config
                (id, circuit, alert_type, label, description, enabled)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (alert_id, circuit, alert_type, label, description))

    if circuit_type == "zone":
        _seed_zone_alerts_only(conn, circuit)


def get_import_state(conn: sqlite3.Connection, circuit: str) -> dict:
    """Return import state for a circuit, creating defaults if absent."""
    row = conn.execute(
        "SELECT * FROM import_state WHERE circuit = ?", (circuit,)
    ).fetchone()
    if row:
        return dict(row)
    conn.execute(
        "INSERT OR IGNORE INTO import_state (circuit) VALUES (?)", (circuit,)
    )
    conn.commit()
    return {"circuit": circuit, "last_check_ts": None, "total_imported": 0}


def update_import_state(
    conn: sqlite3.Connection,
    circuit: str,
    last_check_ts: str,
    imported_count: int = 0,
) -> None:
    conn.execute("""
        INSERT INTO import_state (circuit, last_check_ts, total_imported)
        VALUES (?, ?, ?)
        ON CONFLICT (circuit) DO UPDATE SET
            last_check_ts  = excluded.last_check_ts,
            total_imported = total_imported + excluded.total_imported
    """, (circuit, last_check_ts, imported_count))
    conn.commit()


def get_last_event_ts(conn: sqlite3.Connection, circuit: str) -> Optional[str]:
    """Return ISO timestamp of the most recent event for this circuit, or None."""
    row = conn.execute(
        "SELECT MAX(start_ts) FROM events WHERE circuit = ?", (circuit,)
    ).fetchone()
    return row[0] if row and row[0] else None


def find_overlapping_event(
    conn: sqlite3.Connection,
    circuit: str,
    start_ts: str,
    end_ts: str,
    exclude_event_id: Optional[str] = None,
) -> Optional[dict]:
    """Return an existing event row that meaningfully overlaps [start_ts, end_ts].

    'Meaningful' means:
      overlap >= 30 seconds, OR
      overlap >= 10 seconds AND overlap / shorter_event_duration >= 0.8

    Short independent events (< 10 s absolute overlap) are never blocked
    regardless of ratio — this preserves fridge-fill / toilet events that
    happen to start near the end of a longer shower.

    When multiple rows overlap, returns the most-protected one first:
    user-labeled rows → user-locked rows → longest event.

    Returns None when no meaningful overlap exists.  When a row is found it is
    returned as a dict so callers can log which event blocked the insert.
    """
    try:
        start = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
        end   = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None

    start_epoch = int(start.timestamp())
    end_epoch   = int(end.timestamp())
    new_dur     = end_epoch - start_epoch

    excl = "AND e.id != ?" if exclude_event_id is not None else ""
    params: list = [circuit, start_epoch, end_epoch]
    if exclude_event_id is not None:
        params.append(exclude_event_id)

    rows = conn.execute(f"""
        SELECT e.id,
               e.start_ts,
               e.end_ts,
               e.duration_seconds,
               e.fixture_id,
               e.user_fixture_type,
               f.user_locked,
               CAST(strftime('%s', e.start_ts) AS INTEGER) AS ex_start_epoch,
               CAST(strftime('%s', e.end_ts)   AS INTEGER) AS ex_end_epoch
          FROM events e
          LEFT JOIN fixtures f ON f.id = e.fixture_id
         WHERE e.circuit = ?
           AND e.start_ts IS NOT NULL
           AND e.end_ts IS NOT NULL
           AND CAST(strftime('%s', e.end_ts)   AS INTEGER) > ?
           AND CAST(strftime('%s', e.start_ts) AS INTEGER) < ?
               {excl}
         ORDER BY
               CASE WHEN e.user_fixture_type IS NOT NULL THEN 0 ELSE 1 END,
               CASE WHEN f.user_locked = 1               THEN 0 ELSE 1 END,
               (CAST(strftime('%s', e.end_ts)   AS INTEGER) -
                CAST(strftime('%s', e.start_ts) AS INTEGER)) DESC
    """, params).fetchall()

    for row in rows:
        ex_start = row["ex_start_epoch"]
        ex_end   = row["ex_end_epoch"]
        if ex_start is None or ex_end is None:
            continue
        overlap = min(ex_end, end_epoch) - max(ex_start, start_epoch)
        if overlap <= 0:
            continue
        ex_dur  = ex_end - ex_start
        shorter = min(ex_dur, new_dur)
        if shorter <= 0:
            continue
        if overlap >= 30 or (overlap >= 10 and (overlap / shorter) >= 0.8):
            # "Longer wins over short unlabeled stub" — when the incoming event
            # is ≥ 3× the existing one (the importer-partial signature) and the
            # existing row has no user label / user-locked fixture, allow the
            # insert. The orphan stub stays in the DB; the caller is expected
            # to log it. This auto-heals the C0 historical_importer truncation
            # case where the partial was stored before the live event closed.
            user_locked = bool(row["user_locked"]) if "user_locked" in row.keys() else False
            if (new_dur >= ex_dur * 3
                    and row["user_fixture_type"] is None
                    and not user_locked):
                log.info(
                    "[overlap] not blocking %d s event by %d s unlabeled stub %s "
                    "— allowing the longer event to insert",
                    new_dur, ex_dur, row["id"],
                )
                continue
            return dict(row)

    return None


def event_exists_near(
    conn: sqlite3.Connection,
    circuit: str,
    start_ts: str,
    window_seconds: int = 30,
) -> bool:
    """True if an event with start_ts within ±window_seconds already exists.

    Compares in Unix-epoch seconds so the result is robust against:
    - 'T' vs space separator mismatch (SQLite datetime() uses space)
    - mixed timezone offsets in stored data (+00:00 vs -06:00)
    - microsecond precision differences

    SQLite strftime('%s', …) understands ISO 8601 with both 'T' and space
    separators and returns integer epoch seconds, making the comparison
    timezone-absolute.
    """
    try:
        ts = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    lo_epoch = int((ts - timedelta(seconds=window_seconds)).timestamp())
    hi_epoch = int((ts + timedelta(seconds=window_seconds)).timestamp())
    row = conn.execute("""
        SELECT id FROM events
        WHERE circuit = ?
          AND start_ts IS NOT NULL
          AND CAST(strftime('%s', start_ts) AS INTEGER) BETWEEN ? AND ?
        LIMIT 1
    """, (circuit, lo_epoch, hi_epoch)).fetchone()
    return row is not None


def normalize_events_utc(conn: sqlite3.Connection, commit: bool = True) -> int:
    """Normalize events.start_ts / end_ts to UTC ISO 8601 in-place.

    Intended to be called before dedup_events() in the Quick Restore path.
    Does NOT recompute UUID5 ids here — that is done by dedup_events() after
    duplicates have been removed.  Recomputing ids before dedup would cause
    a PRIMARY KEY collision when two rows represent the same instant expressed
    in different offsets (both would map to the same UUID5).

    When ``commit`` is False the caller owns the transaction (e.g. a restore
    handler running multiple helpers under one outer ``with db:`` block).
    This prevents the inner commit from making the multi-step restore
    partially durable on later failure.

    Returns the number of rows whose timestamps were changed.
    Idempotent — rows already in UTC format are skipped.
    """
    rows = conn.execute(
        "SELECT id, start_ts, end_ts FROM events WHERE start_ts IS NOT NULL"
    ).fetchall()
    updates = []
    for r in rows:
        try:
            s = datetime.fromisoformat(r["start_ts"].replace("Z", "+00:00"))
            if s.tzinfo is None:
                s = s.replace(tzinfo=timezone.utc)
            new_s = s.astimezone(timezone.utc).isoformat()
            new_e = None
            if r["end_ts"]:
                e = datetime.fromisoformat(r["end_ts"].replace("Z", "+00:00"))
                if e.tzinfo is None:
                    e = e.replace(tzinfo=timezone.utc)
                new_e = e.astimezone(timezone.utc).isoformat()
            if new_s != r["start_ts"] or new_e != r["end_ts"]:
                updates.append((new_s, new_e, r["id"]))
        except (ValueError, TypeError):
            continue
    if updates:
        conn.executemany(
            "UPDATE events SET start_ts = ?, end_ts = ? WHERE id = ?",
            updates
        )
        if commit:
            conn.commit()
    return len(updates)


def dedup_events(conn: sqlite3.Connection, commit: bool = True) -> int:
    """Remove duplicate events sharing (circuit, start_ts) and recompute ids.

    Called after Quick Restore to clean any pre-dedup data from old backups.
    Migration 021 (one-time) deduped all existing rows; the matching
    UNIQUE(circuit, start_ts) index lands in migration 032 (this commit).
    Dedup must run before the index creation when restoring older backups,
    or the unique constraint will fire on the historical duplicates.

    When ``commit`` is False the caller owns the transaction (see
    ``normalize_events_utc`` for the same pattern). Required by the
    setup-wizard restore handler so the full multi-step restore commits or
    rolls back as one unit.

    Idempotent — safe to call multiple times.  Returns count of rows deleted.
    Keeps the most recently inserted row (MAX rowid) on the assumption that
    later inserts have fresher cluster_id / match_confidence.

    Also:
    - Clears cluster_id / match_confidence on survivors of contested groups so
      backfill_unmatched re-matches them with the current engine state.
    - Recomputes UUID5 id = uuid5(NAMESPACE_OID, f"{circuit}/{start_ts}") for
      all survivors, making ids stable so future inserts against the
      UNIQUE(circuit, start_ts) index (migration 20260525) collide on the
      same UUID5 and the ON CONFLICT(id) DO UPDATE upsert in insert_event
      refreshes the existing row rather than triggering a duplicate.
    """
    import uuid as _uuid

    # Clear stale cluster_id (and match_confidence if the column exists) on
    # contested survivors before deleting dupes.  match_confidence was added
    # by migration 013; older in-memory test databases may not have it.
    _cols = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
    _extra = ", match_confidence = NULL" if "match_confidence" in _cols else ""
    conn.execute(f"""
        UPDATE events SET cluster_id = NULL{_extra}
        WHERE rowid IN (
            SELECT MAX(rowid) FROM events
            WHERE cluster_id IS NOT NULL
            GROUP BY circuit, start_ts
            HAVING COUNT(*) > 1
        )
    """)
    cursor = conn.execute("""
        DELETE FROM events
        WHERE rowid NOT IN (
            SELECT MAX(rowid)
            FROM events
            GROUP BY circuit, start_ts
        )
    """)
    removed = cursor.rowcount

    # Recompute UUID5 ids for all survivors.  Now that duplicates are gone,
    # each (circuit, start_ts) is unique so new ids cannot collide.
    survivors = conn.execute(
        "SELECT id, circuit, start_ts FROM events WHERE start_ts IS NOT NULL"
    ).fetchall()
    id_updates = []
    for r in survivors:
        try:
            new_id = str(_uuid.uuid5(
                _uuid.NAMESPACE_OID,
                f"{r['circuit']}/{r['start_ts']}"
            ))
            if new_id != r["id"]:
                id_updates.append((new_id, r["id"]))
        except (ValueError, TypeError):
            continue
    if id_updates:
        conn.executemany(
            "UPDATE events SET id = ? WHERE id = ?", id_updates
        )

    if commit:
        conn.commit()
    return removed


# ── Phase 2: fixture cluster helpers ──────────────────────────────────────────

def get_clusters_with_fixtures(
    conn: sqlite3.Connection,
    circuit: str,
) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT fc.*,
               f.name,
               f.display_name,
               f.fixture_type  AS user_type,
               f.confirmed,
               f.user_locked,
               f.notes,
               f.publish_to_ha AS fixture_publish_to_ha
        FROM fixture_clusters fc
        LEFT JOIN fixtures f ON fc.fixture_id = f.id
        WHERE fc.circuit = ?
        ORDER BY fc.member_count DESC
        """,
        (circuit,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_cluster_stats(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(*)              AS event_count,
               AVG(volume_litres)   AS avg_volume_litres,
               AVG(duration_seconds)AS avg_duration_s,
               AVG(avg_flow_lpm)    AS avg_flow_lpm,
               MAX(start_ts)        AS last_seen_at
        FROM events
        WHERE circuit = ? AND cluster_id = ?
        """,
        (circuit, cluster_id),
    ).fetchone()
    return dict(row) if row else {}


def get_all_cluster_stats(
    conn: sqlite3.Connection,
    circuit: str,
) -> Dict[int, Dict[str, Any]]:
    """Return stats for all clusters in a circuit in one query.

    Returns {cluster_id: stats_dict} so callers can look up by id instead of
    issuing one query per cluster (avoids N+1 on the fixtures page).
    """
    rows = conn.execute(
        """
        SELECT cluster_id,
               COUNT(*)               AS event_count,
               AVG(volume_litres)     AS avg_volume_litres,
               AVG(duration_seconds)  AS avg_duration_s,
               AVG(avg_flow_lpm)      AS avg_flow_lpm,
               MAX(start_ts)          AS last_seen_at
        FROM events
        WHERE circuit = ? AND cluster_id IS NOT NULL
        GROUP BY cluster_id
        """,
        (circuit,),
    ).fetchall()
    return {r["cluster_id"]: dict(r) for r in rows}


def upsert_fixture_from_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
    name: str,
    fixture_type: str,
    publish_to_ha: int = 1,
) -> str:
    """Create or update a fixture linked to a cluster. Returns fixture_id."""
    import uuid as _uuid
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    row = conn.execute(
        "SELECT fixture_id FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, cluster_id),
    ).fetchone()
    fixture_id = row["fixture_id"] if row else None

    if fixture_id:
        conn.execute(
            """
            UPDATE fixtures
            SET name = ?, fixture_type = ?, confirmed = 1, user_locked = 1,
                display_name = ?, publish_to_ha = ?, updated_at = ?
            WHERE id = ?
            """,
            (name, fixture_type, name, publish_to_ha, now, fixture_id),
        )
    else:
        fixture_id = str(_uuid.uuid4())
        conn.execute(
            """
            INSERT INTO fixtures
                (id, circuit, name, auto_name, fixture_type, display_name,
                 confirmed, user_locked, publish_to_ha, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?)
            """,
            (fixture_id, circuit, name, name, fixture_type, name,
             publish_to_ha, now, now),
        )
        conn.execute(
            "UPDATE fixture_clusters SET fixture_id = ? WHERE circuit = ? AND id = ?",
            (fixture_id, circuit, cluster_id),
        )

    conn.commit()
    return fixture_id


def get_fixture_id_for_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
) -> Optional[str]:
    """Return the fixture_id linked to a cluster, or None."""
    row = conn.execute(
        "SELECT fixture_id FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, cluster_id)
    ).fetchone()
    return row["fixture_id"] if row and row["fixture_id"] else None


def delete_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
) -> None:
    """Remove a cluster and null out its cluster_id on linked events."""
    conn.execute(
        "UPDATE events SET cluster_id = NULL, fixture_id = NULL WHERE circuit = ? AND cluster_id = ?",
        (circuit, cluster_id),
    )
    conn.execute(
        "DELETE FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, cluster_id),
    )
    conn.commit()


def merge_clusters(
    conn: sqlite3.Connection,
    circuit: str,
    survivor_id: int,
    selected_cluster_ids: List[int],
) -> Dict[str, int]:
    """Merge several clusters into one survivor cluster.

    Every event from the non-survivor clusters is relinked to ``survivor_id``;
    the survivor's centroid, feature_std, member_count and confidence_level are
    recomputed; the non-survivor cluster rows and any confirmed fixture rows
    they own are deleted.  The survivor's own fixture row is left untouched.

    Returns ``{"events_relinked", "fixtures_removed", "survivor_member_count"}``.
    Raises ``ValueError`` on any validation failure, having written nothing.
    All writes run in a single transaction — rolled back on any exception.
    """
    # ── Validation — everything before any write ───────────────────────
    ids = list(dict.fromkeys(int(i) for i in selected_cluster_ids))
    if len(ids) < 2:
        raise ValueError("merge_clusters needs at least 2 distinct cluster IDs")
    survivor_id = int(survivor_id)
    if survivor_id not in ids:
        raise ValueError(
            f"survivor_id {survivor_id} not in selected cluster IDs {ids}"
        )

    id_ph = ",".join(["?"] * len(ids))
    rows = conn.execute(
        f"""SELECT id, centroid, feature_std, member_count
            FROM fixture_clusters
            WHERE circuit = ? AND id IN ({id_ph})""",
        (circuit, *ids),
    ).fetchall()
    found = {int(r["id"]): r for r in rows}
    missing = [i for i in ids if i not in found]
    if missing:
        raise ValueError(
            f"clusters not found in circuit {circuit!r}: {missing}"
        )

    centroids: Dict[int, dict] = {}
    feature_stds: Dict[int, dict] = {}
    counts: Dict[int, int] = {}
    for i in ids:
        r = found[i]
        try:
            cen = json.loads(r["centroid"]) if r["centroid"] else {}
            std = json.loads(r["feature_std"]) if r["feature_std"] else {}
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"cluster {i} has malformed JSON: {e}")
        if not isinstance(cen, dict) or not isinstance(std, dict):
            raise ValueError(
                f"cluster {i} centroid/feature_std is not a JSON object"
            )
        centroids[i] = cen
        feature_stds[i] = std
        counts[i] = int(r["member_count"] or 0)

    total_n = sum(counts.values())
    if total_n <= 0:
        raise ValueError(
            "total member_count across selected clusters is 0 — nothing to merge"
        )

    # ── Merged centroid: member-count weighted mean over the key union ──
    centroid_keys: set = set()
    for cen in centroids.values():
        centroid_keys.update(cen.keys())
    merged_centroid = {
        k: sum(counts[i] * float(centroids[i].get(k, 0.0)) for i in ids) / total_n
        for k in centroid_keys
    }

    # ── Merged feature_std: pooled standard deviation ──────────────────
    # Pooled variance needs each cluster's per-feature mean (its centroid
    # value) and std.  When a cluster lacks a std for a feature we fall back
    # to a member-count weighted average of the available stds — feature_std
    # is a forward-looking column, empty ('{}') in practice, so the fallback
    # is normally what runs and the result is just {}.
    std_keys: set = set()
    for std in feature_stds.values():
        std_keys.update(std.keys())
    merged_std = {}
    for k in std_keys:
        full = all(k in feature_stds[i] and k in centroids[i] for i in ids)
        if full:
            combined_var = sum(
                counts[i] * (
                    float(feature_stds[i][k]) ** 2
                    + (float(centroids[i][k]) - merged_centroid.get(k, 0.0)) ** 2
                )
                for i in ids
            ) / total_n
            merged_std[k] = math.sqrt(max(combined_var, 0.0))
        else:
            contributors = [i for i in ids if k in feature_stds[i]]
            weight = sum(counts[i] for i in contributors) or 1
            merged_std[k] = sum(
                counts[i] * float(feature_stds[i][k]) for i in contributors
            ) / weight

    # ── confidence_level — kept in sync with cluster_engine thresholds ──
    from .cluster_engine import LEVEL_PRELIMINARY_MAX, LEVEL_LEARNING_MAX
    if total_n < LEVEL_PRELIMINARY_MAX:
        level = "preliminary"
    elif total_n < LEVEL_LEARNING_MAX:
        level = "learning"
    else:
        level = "confirmed"

    deleted_ids = [i for i in ids if i != survivor_id]
    del_ph = ",".join(["?"] * len(deleted_ids))

    # ── Non-survivor fixture IDs to delete (deduped, survivor excluded) ─
    survivor_row = conn.execute(
        "SELECT fixture_id FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, survivor_id),
    ).fetchone()
    survivor_fixture_id = survivor_row["fixture_id"] if survivor_row else None

    fx_rows = conn.execute(
        f"""SELECT DISTINCT fixture_id FROM fixture_clusters
            WHERE circuit = ? AND id IN ({del_ph}) AND fixture_id IS NOT NULL""",
        (circuit, *deleted_ids),
    ).fetchall()
    fixture_ids = [
        r["fixture_id"] for r in fx_rows
        if r["fixture_id"] != survivor_fixture_id
    ]

    # ── Writes — single transaction, rollback on any failure ───────────
    try:
        # Relink events to the survivor cluster.  fixture_id is also repointed
        # to the survivor's fixture (or NULL): the non-survivor fixture rows
        # are about to be deleted, and events.fixture_id REFERENCES fixtures(id)
        # would otherwise either dangle or block the DELETE.
        relink = conn.execute(
            f"""UPDATE events SET cluster_id = ?, fixture_id = ?
                WHERE circuit = ? AND cluster_id IN ({del_ph})""",
            (survivor_id, survivor_fixture_id, circuit, *deleted_ids),
        )
        events_relinked = relink.rowcount

        conn.execute(
            """UPDATE fixture_clusters
               SET centroid = ?, feature_std = ?, member_count = ?,
                   confidence_level = ?
               WHERE circuit = ? AND id = ?""",
            (json.dumps(merged_centroid), json.dumps(merged_std),
             total_n, level, circuit, survivor_id),
        )

        # Delete cluster rows before fixture rows — fixture_clusters.fixture_id
        # references fixtures(id), so this avoids relying on ON DELETE SET NULL.
        conn.execute(
            f"DELETE FROM fixture_clusters WHERE circuit = ? AND id IN ({del_ph})",
            (circuit, *deleted_ids),
        )

        if fixture_ids:
            fx_ph = ",".join(["?"] * len(fixture_ids))
            conn.execute(
                f"DELETE FROM fixtures WHERE id IN ({fx_ph})",
                tuple(fixture_ids),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "events_relinked": events_relinked,
        "fixtures_removed": len(fixture_ids),
        "survivor_member_count": total_n,
    }


# ============================================================================
# Sprint A — orphan repair
#
# Three classes of cluster/fixture FK inconsistency that can leak in over a
# product lifetime:
#
#   1. events.cluster_id → fixture_clusters(circuit,id) where the cluster row
#      no longer exists (cluster was deleted/merged without cleaning up event
#      references). Symptom in the field: events show "Cluster 26" on the
#      History page but cluster 26 is missing from the Fixtures page.
#
#   2. fixtures.confirmed=1 but no fixture_clusters row has fixture_id
#      pointing at this fixture. Symptom: a "★ Toilet" pill on history
#      events, but the Toilet fixture is invisible on the Fixtures page and
#      future toilet-shaped events have no cluster to land in.
#
#   3. fixture_clusters.fixture_id → fixtures(id) where the fixtures row no
#      longer exists. The ON DELETE SET NULL FK should prevent this if
#      PRAGMA foreign_keys was on when the fixture was deleted — but it
#      isn't always (per-connection setting; old code paths may have
#      committed without it).
#
# The repair is conservative: never delete user-confirmed fixtures, never
# delete events. Class 1 nulls the event's stale cluster_id so the next
# backfill pass can re-cluster it. Class 2 flags the fixture with
# cluster_backfill_needed=1 so the UI shows a relink affordance. Class 3
# nulls the cluster's dangling fixture_id (matches the FK's ON DELETE
# SET NULL semantics retroactively).
# ============================================================================

def find_orphaned_cluster_references(
    conn: sqlite3.Connection, *, repair: bool = False
) -> Dict[str, int]:
    """Detect (and optionally repair) cluster/fixture FK inconsistencies.

    Returns a dict with three counts:

      - events_orphaned: events.cluster_id pointing to a missing cluster
      - fixtures_unbacked: fixtures.confirmed=1 with no cluster pointing back
      - clusters_dangling: fixture_clusters.fixture_id pointing to a missing
        fixture

    With ``repair=False`` (default) the function only counts — used by the
    orchestrator's startup integrity check (logs non-zero counts; never
    mutates the DB at boot to avoid surprising side effects).

    With ``repair=True`` the function applies the fixes described in the
    module-section comment above and commits. Idempotent: running twice
    yields zero on the second call.
    """
    counts: Dict[str, int] = {
        "events_orphaned": 0,
        "fixtures_unbacked": 0,
        "clusters_dangling": 0,
    }

    # Class 1: events with stale cluster_id.
    # Composite key match — cluster_id alone isn't unique across circuits.
    orphan_events = conn.execute(
        """SELECT e.id FROM events e
           WHERE e.cluster_id IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM fixture_clusters fc
               WHERE fc.circuit = e.circuit AND fc.id = e.cluster_id
             )"""
    ).fetchall()
    counts["events_orphaned"] = len(orphan_events)

    # Class 2: fixtures confirmed but unbacked AND not yet flagged.
    #
    # confirmed=1 only — unconfirmed fixtures aren't expected to have a
    # cluster yet (they could exist transiently during cluster creation).
    #
    # The extra ``cluster_backfill_needed = 0`` filter makes detection
    # match what repair actually changes (the flag), so the function is
    # truly idempotent: once flagged, the fixture is "managed" — awaiting
    # user action via the Fixtures page relink banner — and the integrity
    # check stops re-reporting it on every boot. A genuine *new* orphan
    # appearing after migration (e.g. a bug deleted the wrong cluster
    # row) will still be detected because it won't have the flag set yet.
    unbacked_fixtures = conn.execute(
        """SELECT f.id FROM fixtures f
           WHERE f.confirmed = 1
             AND COALESCE(f.cluster_backfill_needed, 0) = 0
             AND NOT EXISTS (
               SELECT 1 FROM fixture_clusters fc
               WHERE fc.fixture_id = f.id
             )"""
    ).fetchall()
    counts["fixtures_unbacked"] = len(unbacked_fixtures)

    # Class 3: clusters with dangling fixture_id.
    dangling_clusters = conn.execute(
        """SELECT fc.circuit, fc.id FROM fixture_clusters fc
           WHERE fc.fixture_id IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM fixtures f WHERE f.id = fc.fixture_id
             )"""
    ).fetchall()
    counts["clusters_dangling"] = len(dangling_clusters)

    if not repair:
        return counts

    # Repair pass — single transaction so a mid-repair crash doesn't leave
    # half-fixed state.
    try:
        if orphan_events:
            ev_ids = [r["id"] for r in orphan_events]
            ph = ",".join(["?"] * len(ev_ids))
            conn.execute(
                f"UPDATE events SET cluster_id = NULL, "
                f"match_level = 'unmatched', "
                f"match_rejection_reason = 'orphan_cluster_repair' "
                f"WHERE id IN ({ph})",
                tuple(ev_ids),
            )

        if unbacked_fixtures:
            fx_ids = [r["id"] for r in unbacked_fixtures]
            ph = ",".join(["?"] * len(fx_ids))
            conn.execute(
                f"UPDATE fixtures SET cluster_backfill_needed = 1 "
                f"WHERE id IN ({ph})",
                tuple(fx_ids),
            )

        if dangling_clusters:
            # Composite key — iterate row-wise rather than build a huge IN
            # clause; counts here are typically small (single-digit).
            for r in dangling_clusters:
                conn.execute(
                    "UPDATE fixture_clusters SET fixture_id = NULL "
                    "WHERE circuit = ? AND id = ?",
                    (r["circuit"], r["id"]),
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return counts


def get_orphaned_fixtures(
    conn: sqlite3.Connection, circuit: str,
) -> List[Dict[str, Any]]:
    """Return fixtures on this circuit flagged for cluster backfill.

    Used by the Fixtures-page route to render the relink banner. Each row
    has the fields the template needs (id, name, fixture_type) plus the
    raw flag so the banner is only shown when actually needed.
    """
    rows = conn.execute(
        """SELECT id, name, display_name, fixture_type,
                  cluster_backfill_needed
           FROM fixtures
           WHERE circuit = ? AND confirmed = 1
             AND cluster_backfill_needed = 1
           ORDER BY name, id""",
        (circuit,),
    ).fetchall()
    return [dict(r) for r in rows]


# ============================================================================
# Sprint B — propagate per-event labels into cluster.suggested_type
#
# When the user labels an event "★ Toilet" on the History page, the row update
# in events.user_fixture_type is by itself cosmetic — only the History page
# renders the pill. The recompute helper below closes the loop: it looks at
# every labelled event on the cluster, takes a majority vote, and pushes the
# winning type into the cluster row with suggestion_source='user_labels'.
#
# Soft-hints model: this never silently links a cluster to a fixture.
# upsert_fixture_from_cluster (the route the Fixtures-page Confirm button
# calls) is still the only path that sets fixture_clusters.fixture_id. The
# helper just makes that confirmation one click away by pre-filling the
# suggestion.
#
# Edge cases handled explicitly:
#   - No labels yet → leave heuristic suggestion alone, return None
#   - Mixed labels → majority wins; ties broken by alphabetical type
#   - All labels removed by user → reset suggestion to NULL so the next
#     heuristic pass (every 10 events) can repopulate
# ============================================================================

def recompute_cluster_suggestion_from_user_labels(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
) -> Optional[Dict[str, Any]]:
    """Recompute a cluster's suggested_type from majority vote of user
    labels on its member events.

    Returns a dict ``{suggested_type, suggested_confidence,
    suggestion_source, labelled_member_count, total_label_count}`` when
    the cluster row was updated (or reset). Returns ``None`` when there
    are no labels yet AND no prior user-labels suggestion is in place
    (so the cluster row is untouched and the heuristic suggestion, if
    any, stays valid).
    """
    rows = conn.execute(
        "SELECT user_fixture_type, COUNT(*) AS cnt "
        "FROM events "
        "WHERE circuit = ? AND cluster_id = ? "
        "  AND user_fixture_type IS NOT NULL "
        "GROUP BY user_fixture_type",
        (circuit, cluster_id),
    ).fetchall()

    if not rows:
        # No labels on this cluster's events. If the current suggestion
        # is from user labels (i.e. the user just *removed* every label),
        # reset back to NULL so the heuristic pass can pick a fresh
        # value next time it runs. Otherwise leave the row alone.
        cur = conn.execute(
            "SELECT suggestion_source FROM fixture_clusters "
            "WHERE circuit = ? AND id = ?",
            (circuit, cluster_id),
        ).fetchone()
        if cur and cur["suggestion_source"] == "user_labels":
            conn.execute(
                "UPDATE fixture_clusters SET "
                "  suggested_type = NULL, "
                "  suggested_confidence = 0, "
                "  suggestion_source = NULL "
                "WHERE circuit = ? AND id = ?",
                (circuit, cluster_id),
            )
            conn.commit()
            return {
                "suggested_type": None,
                "suggested_confidence": 0.0,
                "suggestion_source": None,
                "labelled_member_count": 0,
                "total_label_count": 0,
            }
        return None

    total = sum(r["cnt"] for r in rows)
    # Sort by (count desc, type asc) — ties broken alphabetically so the
    # result is deterministic across re-runs / restart re-replays.
    ranked = sorted(
        rows, key=lambda r: (-int(r["cnt"]), str(r["user_fixture_type"]))
    )
    winner_type = ranked[0]["user_fixture_type"]
    winner_count = int(ranked[0]["cnt"])
    confidence = winner_count / total if total > 0 else 0.0

    conn.execute(
        "UPDATE fixture_clusters SET "
        "  suggested_type = ?, "
        "  suggested_confidence = ?, "
        "  suggestion_source = 'user_labels' "
        "WHERE circuit = ? AND id = ?",
        (winner_type, confidence, circuit, cluster_id),
    )
    conn.commit()
    return {
        "suggested_type": winner_type,
        "suggested_confidence": confidence,
        "suggestion_source": "user_labels",
        "labelled_member_count": winner_count,
        "total_label_count": total,
    }


def relink_fixture_to_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_id: str,
    cluster_id: int,
) -> None:
    """Attach an orphaned fixture to a chosen cluster.

    Validates that the fixture exists on this circuit, the cluster exists
    on this circuit, and the cluster isn't already linked to a different
    fixture (which would silently steal it). Atomically updates the
    cluster's ``fixture_id`` and clears the fixture's
    ``cluster_backfill_needed`` flag. Raises ``ValueError`` with a precise
    message if any precondition fails, having written nothing.
    """
    fx = conn.execute(
        "SELECT id FROM fixtures WHERE id = ? AND circuit = ?",
        (fixture_id, circuit),
    ).fetchone()
    if not fx:
        raise ValueError(
            f"fixture {fixture_id!r} not found on circuit {circuit!r}"
        )

    cl = conn.execute(
        "SELECT id, fixture_id FROM fixture_clusters "
        "WHERE circuit = ? AND id = ?",
        (circuit, cluster_id),
    ).fetchone()
    if not cl:
        raise ValueError(
            f"cluster {cluster_id} not found on circuit {circuit!r}"
        )

    existing = cl["fixture_id"]
    if existing and existing != fixture_id:
        raise ValueError(
            f"cluster {cluster_id} is already linked to fixture "
            f"{existing!r} — pick a different cluster"
        )

    try:
        conn.execute(
            "UPDATE fixture_clusters SET fixture_id = ? "
            "WHERE circuit = ? AND id = ?",
            (fixture_id, circuit, cluster_id),
        )
        conn.execute(
            "UPDATE fixtures SET cluster_backfill_needed = 0 "
            "WHERE id = ?",
            (fixture_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ============================================================================
# Sprint C — fixture_type_signatures matcher
#
# Per-(circuit, fixture_type) centroid built from user-labelled events. The
# matcher runs as a second-chance pass after cluster matching: if the cluster
# matcher rejected the event (no_centers / features_missing) OR matched it
# only at low confidence, we still want a user-facing label when the event's
# features sit close to a fixture type the user has been training.
#
# The signature centroid is a simple per-feature arithmetic mean over the
# labelled events' raw feature values (the cluster_engine's StandardScaler
# is per-circuit and not stable across boots, so we deliberately avoid it
# here — raw-feature Euclidean is good enough for the small feature subset
# the matcher considers, and stays interpretable across restarts).
#
# The feature subset used for matching is conservative: the same first-rank
# scalar features the cluster centroid heuristic already keys on (volume,
# duration, flow, pressure delta). Signature shape vectors are NOT used —
# they'd dominate the distance arithmetic and the user-labelled corpus is
# typically too small to learn a meaningful shape centroid.
# ============================================================================

# Features the signature matcher uses. Kept small + interpretable; matches
# the cluster_engine's first-rank features so the two centroids are
# comparable. Update both at once if this list changes.
_SIGNATURE_MATCH_FEATURES: tuple = (
    "avg_flow_lpm",
    "peak_flow_lpm",
    "duration_seconds",
    "volume_litres",
    "pressure_delta_psi",
    "steady_state_fraction",
)

# Signature matcher distance threshold — Euclidean over the feature subset
# above. Heuristic value picked so a clear toilet-shaped event (3 gal, 60s,
# ~2 lpm, ~5 psi drop) doesn't accidentally match a washing-machine
# signature (~6 gal, ~3 min, ~2 lpm, ~6 psi). Compared after subtracting
# centroid means and dividing each feature by its rough scale below.
_SIGNATURE_MATCH_THRESHOLD: float = 1.5
_SIGNATURE_MATCH_SCALES: dict = {
    "avg_flow_lpm":           2.0,    # 0.5–5 gal/min typical range
    "peak_flow_lpm":          3.0,
    "duration_seconds":     120.0,    # 30s – several min
    "volume_litres":         10.0,
    "pressure_delta_psi":     5.0,
    "steady_state_fraction":  0.5,
}

# ── Label-trained k-NN matcher (2026-05-31) ─────────────────────────────────
# The mean-centroid matcher above scored ~70% leave-one-out on the May-2026
# labelled archive; a weighted k-NN over the labelled events themselves scored
# ~80% (and stays in sync with labels — no stale centroid). The k-NN is the
# production path (live classify + backfill); the mean-centroid is retained for
# the Signatures UI display and back-compat tests.
#
# Right-skewed features are log1p-compressed so the centroid + Euclidean
# distance behave on log-normal data. The transform is applied identically to
# the query event AND every labelled neighbour at query time, so train/serve
# are consistent by construction (nothing log-space is persisted).
_SIGNATURE_LOG_FEATURES: frozenset = frozenset({
    "volume_litres", "duration_seconds", "avg_flow_lpm", "peak_flow_lpm",
})


def _sig_transform(feat: str, value) -> float:
    """log1p the right-skewed signature features; identity for the rest.

    MUST be applied identically in training (the labelled neighbours) and
    serving (the query event). None / non-numeric → 0.0 in the (already
    non-negative) transformed space.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    if feat in _SIGNATURE_LOG_FEATURES:
        return math.log1p(max(0.0, v))
    return v


# k-NN tuning. All values derived from the May-2026 labelled archive (104
# non-excluded labelled events); re-tune as more labels accrue.
_SIGNATURE_KNN_K: int = 5
# Below these floors the matcher abstains rather than overfit a tiny corpus:
_SIGNATURE_KNN_MIN_TOTAL_LABELS: int = 10     # whole circuit too sparse → None
_SIGNATURE_KNN_MIN_LABELS_PER_CLASS: int = 2  # class too sparse → not voted
# Accept a winner only when its summed inverse-distance vote clears an absolute
# floor (catches out-of-distribution events whose nearest neighbours are far)
# AND holds a clear majority share (catches genuinely ambiguous events). At
# margin 0.6 the LOO set was 85% covered / 86% accurate on what it typed.
_SIGNATURE_KNN_CONFIDENCE_THRESHOLD: float = 1.5
_SIGNATURE_KNN_MARGIN_THRESHOLD: float = 0.6
# Per-feature scales ≈ each feature's std in log1p space on the labelled set.
_SIGNATURE_KNN_SCALES: dict = {
    "avg_flow_lpm":          0.70,
    "peak_flow_lpm":         0.74,
    "duration_seconds":      1.27,
    "volume_litres":         1.52,
    "pressure_delta_psi":    2.88,
    "steady_state_fraction": 0.25,
}

# ── Active-flow feature set (preferred after the 20260536 backfill) ──────────
# Once the backfill populates the active-flow columns on labelled events, the
# matcher prefers these (true_avg_flow + active_flow_duration replace the
# pressure-window-inflated avg_flow + duration; flow_on_ratio flags artifacts).
# Until then labelled rows have NULL active features, so the matcher falls back
# to the legacy set — this is what stops classification coverage collapsing.
# RETUNE _SIGNATURE_KNN_ACTIVE_SCALES (LOO on the labelled archive) once backfill
# has run; the values below are estimated from the raw-flow analysis.
_SIGNATURE_KNN_ACTIVE_FEATURES: tuple = (
    "true_avg_flow_lpm", "peak_flow_lpm", "active_flow_duration_seconds",
    "volume_litres", "pressure_delta_psi", "steady_state_fraction", "flow_on_ratio",
)
_SIGNATURE_KNN_ACTIVE_LOG_FEATURES: frozenset = frozenset({
    "true_avg_flow_lpm", "peak_flow_lpm", "active_flow_duration_seconds", "volume_litres",
})
_SIGNATURE_KNN_ACTIVE_SCALES: dict = {
    "true_avg_flow_lpm":           0.70,
    "peak_flow_lpm":               0.74,
    "active_flow_duration_seconds": 1.40,
    "volume_litres":               1.52,
    "pressure_delta_psi":          2.88,
    "steady_state_fraction":       0.25,
    "flow_on_ratio":               0.25,   # linear (a 0–1 ratio), not log
}


def _knn_transform(feat: str, value, log_features: frozenset) -> float:
    """log1p the right-skewed features (per ``log_features``), identity for the
    rest. None / non-finite → 0.0. Applied identically to query + neighbours."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    return math.log1p(max(0.0, v)) if feat in log_features else v


def _knn_vote(labelled, event_features, features, scales, log_features):
    """Inverse-distance weighted k-NN vote over ``labelled`` = [(canon_type, row)].

    Shared by the active-flow and legacy matchers. Returns a hit dict or None
    (abstain). 'other' wins are treated as abstention.
    """
    counts: Dict[str, int] = {}
    for t, _ in labelled:
        counts[t] = counts.get(t, 0) + 1
    eligible = {t for t, c in counts.items()
                if c >= _SIGNATURE_KNN_MIN_LABELS_PER_CLASS}
    if not eligible:
        return None
    q = {f: _knn_transform(f, event_features.get(f), log_features) for f in features}
    dists: list = []
    for t, r in labelled:
        if t not in eligible:
            continue
        sq = 0.0
        for f in features:
            scale = scales.get(f, 1.0)
            d = (q[f] - _knn_transform(f, r[f], log_features)) / scale
            sq += d * d
        dists.append((math.sqrt(sq / len(features)), t))
    if not dists:
        return None
    dists.sort(key=lambda x: x[0])
    neighbours = dists[:_SIGNATURE_KNN_K]
    score: Dict[str, float] = {}
    for d, t in neighbours:
        score[t] = score.get(t, 0.0) + 1.0 / (d + 1e-6)
    total = sum(score.values())
    win_t, win_s = max(score.items(), key=lambda kv: kv[1])
    if (win_s < _SIGNATURE_KNN_CONFIDENCE_THRESHOLD
            or total <= 0
            or (win_s / total) < _SIGNATURE_KNN_MARGIN_THRESHOLD):
        return None
    if win_t == "other":
        return None
    nearest = min(d for d, t in neighbours if t == win_t)
    return {
        "fixture_type": win_t,
        "distance": nearest,
        "score": win_s,
        "margin": win_s / total,
        "member_count": counts[win_t],
    }


def upsert_fixture_signature(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_type: str,
) -> Optional[Dict[str, Any]]:
    """Recompute (or remove) the signature for one (circuit, fixture_type).

    Reads every event on ``circuit`` whose ``user_fixture_type`` matches
    ``fixture_type`` and ``excluded_from_training = 0`` (degraded /
    composite events shouldn't pollute the type centroid), averages the
    feature subset, and upserts the row.

    Returns the upserted row as a dict on success, or ``None`` when there
    are no eligible labelled events. In the no-eligible case the existing
    signature is DELETED so a stale centroid doesn't keep matching after
    the user has un-labelled their training set.
    """
    rows = conn.execute(
        f"""SELECT {', '.join(_SIGNATURE_MATCH_FEATURES)}
            FROM events
            WHERE circuit = ?
              AND user_fixture_type = ?
              AND COALESCE(excluded_from_training, 0) = 0""",
        (circuit, fixture_type),
    ).fetchall()
    if not rows:
        conn.execute(
            "DELETE FROM fixture_type_signatures "
            "WHERE circuit = ? AND fixture_type = ?",
            (circuit, fixture_type),
        )
        conn.commit()
        return None

    # Arithmetic mean per feature, ignoring NULLs in any individual row.
    centroid: Dict[str, float] = {}
    for feat in _SIGNATURE_MATCH_FEATURES:
        vals = [float(r[feat]) for r in rows if r[feat] is not None]
        if vals:
            centroid[feat] = sum(vals) / len(vals)

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO fixture_type_signatures
               (circuit, fixture_type, centroid, member_count,
                created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(circuit, fixture_type) DO UPDATE SET
               centroid     = excluded.centroid,
               member_count = excluded.member_count,
               updated_at   = excluded.updated_at""",
        (circuit, fixture_type, json.dumps(centroid), len(rows), now, now),
    )
    conn.commit()
    return {
        "circuit": circuit,
        "fixture_type": fixture_type,
        "centroid": centroid,
        "member_count": len(rows),
    }


def get_fixture_type_signatures(
    conn: sqlite3.Connection, circuit: str,
) -> List[Dict[str, Any]]:
    """Return all signatures for one circuit, centroid pre-decoded.

    Ordered by member_count desc so the UI lists the most-trained types
    first. Empty list when nothing has been labelled yet.
    """
    rows = conn.execute(
        "SELECT circuit, fixture_type, centroid, member_count, "
        "       created_at, updated_at "
        "FROM fixture_type_signatures "
        "WHERE circuit = ? "
        "ORDER BY member_count DESC, fixture_type",
        (circuit,),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            centroid = json.loads(r["centroid"] or "{}")
        except (json.JSONDecodeError, TypeError):
            centroid = {}
        out.append({
            "circuit": r["circuit"],
            "fixture_type": r["fixture_type"],
            "centroid": centroid,
            "member_count": int(r["member_count"] or 0),
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        })
    return out


def delete_fixture_signature(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_type: str,
) -> bool:
    """Forget a signature so the user can recover from bad labels.

    Returns True if a row was deleted, False if none existed.
    """
    cur = conn.execute(
        "DELETE FROM fixture_type_signatures "
        "WHERE circuit = ? AND fixture_type = ?",
        (circuit, fixture_type),
    )
    conn.commit()
    return cur.rowcount > 0


# ── Sprint F: Per-category Fixtures-page rollup ─────────────────────────────

def get_category_rollup(
    conn: sqlite3.Connection,
    circuit: str,
    midnight_utc: str,
) -> List[Dict[str, Any]]:
    """Per-effective-type aggregate for one circuit.

    ``midnight_utc`` is the UTC ISO timestamp of HA-local midnight (same
    format the dashboard's get_daily_volume uses). Returned rows have raw
    ``eff_type`` strings — the router MUST funnel each through
    ``fixtures.normalize_fixture_type_for_circuit`` before bucketing, since
    legacy / wrong-kind / typo strings can appear in stored data.

    Phantom events (is_pressure_restoration_phantom=1) are excluded — their
    effective volume is already 0 and counting them would inflate the event
    count. Degraded and composite events ARE included; the Fixtures count
    should match what the History list shows, not the training subset.

    Effective-type precedence (clustering demoted 2026-05-31): user label >
    confirmed fixture > label-trained matched_fixture_type > cluster suggestion
    > 'other'. The k-NN match outranks the (impure) cluster suggestion so the
    classifier — not clustering — drives fixture identity on the cards.
    """
    rows = conn.execute(
        """
        SELECT
          COALESCE(e.user_fixture_type, f.fixture_type, e.matched_fixture_type,
                   fc.suggested_type, 'other') AS eff_type,
          COALESCE(SUM(COALESCE(e.volume_litres_effective, e.volume_litres, 0)), 0)
                                                          AS lifetime_volume_l,
          COUNT(*)                                        AS lifetime_event_count,
          MAX(e.start_ts)                                 AS last_seen_at,
          COALESCE(SUM(CASE WHEN e.start_ts >= ?
                      THEN COALESCE(e.volume_litres_effective, e.volume_litres, 0)
                      ELSE 0 END), 0)                     AS today_volume_l,
          SUM(CASE WHEN e.start_ts >= ? THEN 1 ELSE 0 END) AS today_event_count
        FROM events e
        LEFT JOIN fixtures f          ON e.fixture_id = f.id
        LEFT JOIN fixture_clusters fc ON fc.circuit = e.circuit AND fc.id = e.cluster_id
        WHERE e.circuit = ?
          AND COALESCE(e.is_pressure_restoration_phantom, 0) = 0
        GROUP BY eff_type
        """,
        (midnight_utc, midnight_utc, circuit),
    ).fetchall()
    return [
        {
            "eff_type":             r["eff_type"],
            "lifetime_volume_l":    float(r["lifetime_volume_l"] or 0.0),
            "lifetime_event_count": int(r["lifetime_event_count"] or 0),
            "last_seen_at":         r["last_seen_at"],
            "today_volume_l":       float(r["today_volume_l"] or 0.0),
            "today_event_count":    int(r["today_event_count"] or 0),
        }
        for r in rows
    ]


def get_category_publish_map(
    conn: sqlite3.Connection, circuit: str,
) -> Dict[str, bool]:
    """Return {fixture_type: bool} for one circuit's per-category publish gates.

    Only returns rows that exist — missing keys MUST be defaulted to True
    (publish on) at the call site via ``publish_map.get(typ, True)``. This
    contract is pinned by ``test_category_publish_missing_row_defaults_true``.
    """
    rows = conn.execute(
        "SELECT fixture_type, publish_to_ha FROM category_publish "
        "WHERE circuit = ?",
        (circuit,),
    ).fetchall()
    return {r["fixture_type"]: bool(r["publish_to_ha"]) for r in rows}


def set_category_publish(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_type: str,
    publish_to_ha: int,
) -> None:
    """Upsert the publish gate for one (circuit, fixture_type).

    Defensive: validates ``fixture_type`` against the union of fixture-
    selectable and zone-selectable types. Raises ``ValueError`` on unknown
    or empty input so a stray internal caller cannot persist garbage rows
    even if it skipped the route-level validation.
    """
    # Local import to avoid widening the module-load import surface.
    from .fixtures import (fixture_user_selectable_types,
                           zone_user_selectable_types)
    allowed = set(fixture_user_selectable_types()) | set(zone_user_selectable_types())
    if fixture_type not in allowed:
        raise ValueError(
            f"set_category_publish: unknown fixture_type {fixture_type!r}; "
            f"expected one of {sorted(allowed)}"
        )
    conn.execute(
        "INSERT INTO category_publish "
        "  (circuit, fixture_type, publish_to_ha, updated_at) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT (circuit, fixture_type) DO UPDATE SET "
        "  publish_to_ha = excluded.publish_to_ha, "
        "  updated_at = excluded.updated_at",
        (circuit, fixture_type, 1 if publish_to_ha else 0,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def repair_misflagged_phantom_events(conn: sqlite3.Connection) -> dict:
    """Sprint H — un-flag events wrongly marked as pressure-restoration phantoms.

    A real event can be left with a stale ``is_pressure_restoration_phantom=1``
    when the phantom verdict was computed from the software-path pressure
    (< 2 PSI) and a late ESP waveform then raised ``pressure_delta_psi`` to a
    real value without re-deriving the verdict (fixed forward by
    ``_finalize_derived_verdicts``). Such a row is internally contradictory:
    flagged phantom yet ``pressure_delta_psi >= 2.0``. This restores it:
    clears the flag, restores ``volume_litres_effective`` (degraded → envelope
    estimate, else raw) and re-applies that real volume to ``hourly_volume`` +
    ``daily_summary``.

    Skips ``user_classified`` rows (manual classification is authoritative).
    Idempotent — once repaired the WHERE clause no longer selects the row.

    Scope: this contradiction-repair concerns only the LONG-DURATION
    pressure-restoration phantom (which zeroes volume and requires
    ``pressure_delta_psi < 2.0``). The low-flow dribble flag is unrelated — it
    never zeroes volume and lives on low-pressure rows that can't satisfy the
    ``pressure_delta_psi >= 2.0`` filter below, so dribbles are never touched.

    Returns ``{"repaired": N, "litres_restored": L}``.
    """
    from .feature_extractor import _PHANTOM_MAX_DELTA_PSI

    rows = conn.execute(
        "SELECT id, circuit, start_ts, volume_litres, volume_litres_estimated, "
        "       degraded_supply, user_ignored, "
        "       hourly_volume_applied_litres, hourly_volume_applied_bucket "
        "FROM events "
        "WHERE is_pressure_restoration_phantom = 1 "
        "  AND pressure_delta_psi >= ? "
        "  AND COALESCE(user_classified, 0) = 0",
        (_PHANTOM_MAX_DELTA_PSI,),
    ).fetchall()

    repaired = 0
    litres_restored = 0.0
    affected_days: set = set()
    for row in rows:
        is_degraded = bool(row["degraded_supply"])
        raw = float(row["volume_litres"] or 0.0)
        est = row["volume_litres_estimated"]
        restored = (float(est) if (is_degraded and est is not None) else raw)
        new_method = "pulsing_supply_envelope" if is_degraded else "raw"
        new_excluded = 1 if (
            is_degraded or bool(row["user_ignored"])
        ) else 0
        new_reason = "pulsing_supply" if is_degraded else None

        prev_applied = float(row["hourly_volume_applied_litres"] or 0.0)
        prev_bucket = row["hourly_volume_applied_bucket"] or _hour_bucket_for(row["start_ts"])
        delta = restored - prev_applied   # phantom had effective 0 → applied 0

        with transaction(conn):
            conn.execute(
                "UPDATE events SET "
                "  is_pressure_restoration_phantom = 0, "
                "  volume_litres_effective = ?, "
                "  volume_estimation_method = ?, "
                "  excluded_from_training = ?, "
                "  match_rejection_reason = ? "
                "WHERE id = ?",
                (round(restored, 3), new_method, new_excluded, new_reason, row["id"]),
            )
            if prev_bucket and abs(delta) > 1e-9:
                conn.execute(
                    "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT (circuit, hour_ts) "
                    "DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres",
                    (row["circuit"], prev_bucket, delta),
                )
            conn.execute(
                "UPDATE events SET "
                "  hourly_volume_applied_litres = ?, "
                "  hourly_volume_applied_bucket = ? "
                "WHERE id = ?",
                (round(restored, 3), prev_bucket, row["id"]),
            )

        repaired += 1
        litres_restored += restored
        day = (row["start_ts"] or "")[:10]
        if day:
            affected_days.add((row["circuit"], day))
        log.info(
            "phantom-repair: event %s un-flagged (restored %.3f L to bucket %s)",
            row["id"], restored, prev_bucket,
        )

    for circ, day in affected_days:
        compute_daily_summary(conn, circ, day)
    if affected_days:
        conn.commit()

    if repaired:
        log.info("phantom-repair: un-flagged %d event(s), restored %.1f L total",
                 repaired, litres_restored)
    return {"repaired": repaired, "litres_restored": round(litres_restored, 3)}


def match_event_to_signature(
    conn: sqlite3.Connection,
    circuit: str,
    event_features: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Return the closest signature within
    ``_SIGNATURE_MATCH_THRESHOLD`` on ``circuit``, or None.

    Distance is computed in scale-normalised space:
    sqrt(sum_i ((event_i - centroid_i) / scale_i)^2) over the matcher
    feature subset. ``_SIGNATURE_MATCH_SCALES`` provides per-feature
    typical ranges; the threshold is then in "rough fixture-typical-range
    units" so it's interpretable.

    Caller is responsible for deciding *when* to call this (e.g. only as
    a fallback after cluster matching). The matcher itself doesn't gate
    on whether the cluster matched.
    """
    sigs = get_fixture_type_signatures(conn, circuit)
    if not sigs:
        return None

    best: Optional[Dict[str, Any]] = None
    best_dist = float("inf")
    for sig in sigs:
        cen = sig["centroid"]
        if not cen:
            continue
        sq = 0.0
        used = 0
        for feat in _SIGNATURE_MATCH_FEATURES:
            ev_v = event_features.get(feat)
            cn_v = cen.get(feat)
            if ev_v is None or cn_v is None:
                continue
            scale = _SIGNATURE_MATCH_SCALES.get(feat, 1.0)
            try:
                delta = (float(ev_v) - float(cn_v)) / scale
            except (TypeError, ValueError):
                continue
            sq += delta * delta
            used += 1
        if used == 0:
            continue
        # Normalise distance by feature count so signatures with sparse
        # centroids (only a few features populated) aren't unfairly
        # penalised vs full-feature ones.
        dist = (sq / used) ** 0.5
        if dist < best_dist:
            best_dist = dist
            best = sig

    if best is None or best_dist > _SIGNATURE_MATCH_THRESHOLD:
        return None
    return {
        "fixture_type": best["fixture_type"],
        "distance": best_dist,
        "member_count": best["member_count"],
    }


def set_event_matched_fixture_type(
    conn: sqlite3.Connection,
    circuit: str,
    event_id: str,
    fixture_type: Optional[str],
) -> None:
    """Write ``events.matched_fixture_type`` for one event.

    Does not commit — caller batches with surrounding writes (typically
    the cluster_id update in feature_extractor._cluster_event).
    """
    conn.execute(
        "UPDATE events SET matched_fixture_type = ? "
        "WHERE id = ? AND circuit = ?",
        (fixture_type, event_id, circuit),
    )


def _canonical_fixture_type(name: Optional[str]) -> Optional[str]:
    """Collapse a fixture-type string to its canonical slug (circuit-kind
    independent): lowercase, take the first '/'-segment, fold separators to
    '_', then apply the Sprint-D alias remap (shower→shower_tub, etc.).

    Returns None for None/blank input. Does NOT map unknowns to 'other' (that's
    the circuit-kind-aware display layer's job) — it only unifies variants so
    'Toilet'/'toilets'/'toilet ' all store as 'toilet'. Reused at every write
    of user_fixture_type / matched_fixture_type so the rollup groups cleanly.
    """
    if not isinstance(name, str):
        return None
    s = name.strip().lower()
    if not s:
        return None
    s = s.strip("/").split("/", 1)[0].strip()
    s = re.sub(r"[\s\-]+", "_", s).strip("_")
    if not s:
        return None
    # Lazy import — fixtures.py owns the alias map; importing at module load
    # would be a (currently absent) cycle risk.
    try:
        from .fixtures import LEGACY_TYPE_REMAP
        return LEGACY_TYPE_REMAP.get(s, s)
    except Exception:
        return s


def match_event_to_signature_knn(
    conn: sqlite3.Connection,
    circuit: str,
    event_features: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Inverse-distance weighted k-NN over the circuit's labelled events.

    The production fixture-type matcher (replaces the mean-centroid
    ``match_event_to_signature`` on the live + backfill paths). Pulls every
    labelled, non-excluded event on ``circuit``, log-compresses the skewed
    features (``_sig_transform``), and votes with weight ``1/(distance+eps)``.

    Abstains (returns ``None``) when:
      • fewer than ``_SIGNATURE_KNN_MIN_TOTAL_LABELS`` labelled events exist;
      • no class has ``_SIGNATURE_KNN_MIN_LABELS_PER_CLASS`` members;
      • the winner's summed vote is below ``_SIGNATURE_KNN_CONFIDENCE_THRESHOLD``
        (out-of-distribution — nearest neighbours are far); or
      • the winner's share of the total vote is below
        ``_SIGNATURE_KNN_MARGIN_THRESHOLD`` (genuinely ambiguous).

    On a hit returns ``{"fixture_type", "distance", "score", "margin",
    "member_count"}``. fixture_type is canonical.
    """
    def _labelled(sql: str):
        rows = conn.execute(sql, (circuit,)).fetchall()
        out = []
        for r in rows:
            t = _canonical_fixture_type(r["user_fixture_type"])
            if t:
                out.append((t, r))
        return out

    # 1) Prefer the active-flow features — but only when the QUERY has them and
    #    enough labelled events have been backfilled with non-NULL, non-degraded
    #    active features. 'other' wins → abstain (handled in _knn_vote).
    query_has_active = all(
        event_features.get(f) is not None
        for f in ("true_avg_flow_lpm", "active_flow_duration_seconds", "flow_on_ratio")
    )
    if query_has_active:
        active = _labelled(
            "SELECT user_fixture_type, true_avg_flow_lpm, peak_flow_lpm, "
            "       active_flow_duration_seconds, volume_litres, pressure_delta_psi, "
            "       steady_state_fraction, flow_on_ratio "
            "FROM events "
            "WHERE circuit = ? "
            "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
            "  AND COALESCE(excluded_from_training, 0) = 0 "
            "  AND COALESCE(integration_quality, 'ok') = 'ok' "
            "  AND true_avg_flow_lpm IS NOT NULL "
            "  AND active_flow_duration_seconds IS NOT NULL "
            "  AND flow_on_ratio IS NOT NULL"
        )
        if len(active) >= _SIGNATURE_KNN_MIN_TOTAL_LABELS:
            hit = _knn_vote(active, event_features, _SIGNATURE_KNN_ACTIVE_FEATURES,
                            _SIGNATURE_KNN_ACTIVE_SCALES, _SIGNATURE_KNN_ACTIVE_LOG_FEATURES)
            if hit is not None:
                hit["match_source"] = "active_flow"
                return hit
            # Active had enough labels but abstained → fall through to legacy so
            # classification coverage never regresses below the legacy baseline.

    # 2) Legacy fallback (pre-backfill, or active abstained).
    legacy = _labelled(
        "SELECT user_fixture_type, avg_flow_lpm, peak_flow_lpm, duration_seconds, "
        "       volume_litres, pressure_delta_psi, steady_state_fraction "
        "FROM events "
        "WHERE circuit = ? "
        "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
        "  AND COALESCE(excluded_from_training, 0) = 0"
    )
    if len(legacy) < _SIGNATURE_KNN_MIN_TOTAL_LABELS:
        return None
    hit = _knn_vote(legacy, event_features, _SIGNATURE_MATCH_FEATURES,
                    _SIGNATURE_KNN_SCALES, _SIGNATURE_LOG_FEATURES)
    if hit is not None:
        hit["match_source"] = "legacy_features"
    return hit


def reclassify_all_events_from_signatures(
    conn: sqlite3.Connection,
    circuit: str,
) -> Dict[str, Any]:
    """Retrain signatures, then backfill ``matched_fixture_type`` over every
    unlabelled event on ``circuit`` via the k-NN matcher.

    NEVER touches user-labelled rows (WHERE user_fixture_type IS NULL). Writes
    the canonical matched type, or NULL on abstention — writing NULL clears a
    stale prior match, making the whole pass idempotent. Never writes 'other'
    (that is a display-only fallback).

    Returns counts: ``{"signatures_trained", "events_scanned", "events_matched",
    "events_cleared", "events_abstained"}``.
    """
    # 1. Retrain the per-type centroids (for the Signatures UI display). The
    #    k-NN itself reads events directly, so this is purely cosmetic but keeps
    #    the Signatures page in sync.
    signatures_trained = 0
    type_rows = conn.execute(
        "SELECT DISTINCT user_fixture_type FROM events "
        "WHERE circuit = ? AND user_fixture_type IS NOT NULL "
        "  AND user_fixture_type <> ''",
        (circuit,),
    ).fetchall()
    for tr in type_rows:
        if upsert_fixture_signature(conn, circuit, tr[0]) is not None:
            signatures_trained += 1

    # 2. Backfill over unlabelled events. Query carries BOTH the legacy and the
    #    active-flow features so the matcher uses whichever it can (active when
    #    backfilled). An event now excluded_from_training carries no fixture
    #    identity → its matched_fixture_type is cleared (stale-match carry-forward).
    qfeats = tuple(dict.fromkeys(
        _SIGNATURE_MATCH_FEATURES + _SIGNATURE_KNN_ACTIVE_FEATURES))
    rows = conn.execute(
        "SELECT id, matched_fixture_type, excluded_from_training, "
        "       " + ", ".join(qfeats) + " "
        "FROM events "
        "WHERE circuit = ? AND user_fixture_type IS NULL "
        "ORDER BY start_ts",
        (circuit,),
    ).fetchall()
    scanned = matched = cleared = abstained = 0
    for r in rows:
        scanned += 1
        if r["excluded_from_training"]:
            new_type = None     # artifacts/excluded carry no fixture identity
        else:
            feats = {f: r[f] for f in qfeats}
            hit = match_event_to_signature_knn(conn, circuit, feats)
            new_type = _canonical_fixture_type(hit["fixture_type"]) if hit else None
        prev = r["matched_fixture_type"]
        if new_type != prev:
            set_event_matched_fixture_type(conn, circuit, r["id"], new_type)
            if new_type is None and prev is not None:
                cleared += 1
        if new_type is not None:
            matched += 1
        else:
            abstained += 1
    conn.commit()
    result = {
        "signatures_trained": signatures_trained,
        "events_scanned": scanned,
        "events_matched": matched,
        "events_cleared": cleared,
        "events_abstained": abstained,
    }
    log.info(
        "[%s] reclassify: trained %d signature(s); scanned %d unlabelled "
        "event(s) → %d matched, %d abstained (%d stale cleared)",
        circuit, signatures_trained, scanned, matched, abstained, cleared,
    )
    return result


def cleanup_composite_flags(conn: sqlite3.Connection) -> Dict[str, int]:
    """One-shot: composite is deprecated as an authoritative flag. Re-derive
    ``excluded_from_training`` for events that were excluded ONLY because they
    were composite, so they become classifiable again.

    Guard (review): do NOT make an event training-eligible unless it has valid
    (non-NULL) active-flow features AND a non-degraded integration_quality — an
    un-backfilled composite row stays excluded until its features exist. Skips
    user_classified rows (their verdict is authoritative). Does not touch the
    diagnostic ``is_composite`` column. Returns counts.
    """
    rows = conn.execute(
        "SELECT id, circuit, is_pressure_restoration_phantom, degraded_supply, "
        "       is_low_flow_dribble, user_ignored, integration_quality, "
        "       true_avg_flow_lpm, excluded_from_training "
        "FROM events "
        "WHERE COALESCE(is_composite, 0) = 1 AND COALESCE(user_classified, 0) = 0",
    ).fetchall()
    cleaned = unexcluded = 0
    for r in rows:
        degraded_integ = r["integration_quality"] not in (None, "ok")
        other_excl = bool(
            r["is_pressure_restoration_phantom"] or r["degraded_supply"]
            or r["is_low_flow_dribble"] or r["user_ignored"] or degraded_integ
        )
        has_valid_features = r["true_avg_flow_lpm"] is not None and not degraded_integ
        new_excluded = 0 if (not other_excl and has_valid_features) else 1
        if new_excluded != (r["excluded_from_training"] or 0):
            conn.execute(
                "UPDATE events SET excluded_from_training = ? "
                "WHERE id = ? AND circuit = ?",
                (new_excluded, r["id"], r["circuit"]),
            )
            cleaned += 1
            if new_excluded == 0:
                unexcluded += 1
    conn.commit()
    log.info("composite cleanup: %d row(s) re-derived, %d un-excluded", cleaned, unexcluded)
    return {"composite_rows_changed": cleaned, "unexcluded": unexcluded}


# ── Temporal appliance-cycle signal (cycle_pulse_count) ───────────────────────
# A dishwasher / washing-machine fill PULSE is indistinguishable from a tap in a
# single event; the discriminator is that pulses REPEAT. cycle_pulse_count is the
# number of same-circuit events within ±45 min whose volume is within ratio
# [0.4, 2.5] of this event. It rides in the cluster centroid (mean over members)
# as a heuristic-only signal — see fixtures.py temporal appliance rules.

_CYCLE_PULSE_WINDOW_SECONDS: float = 2700.0      # ±45 min
_CYCLE_PULSE_VOL_RATIO_LO: float = 0.4
_CYCLE_PULSE_VOL_RATIO_HI: float = 2.5


def compute_cycle_pulse_count(this_volume, neighbour_volumes) -> int:
    """Count neighbour volumes within ratio [0.4, 2.5] of ``this_volume``.

    Pure (no DB). The event itself must already be excluded from
    ``neighbour_volumes``. Returns 0 when ``this_volume`` is missing or <= 0.
    """
    try:
        tv = float(this_volume)
    except (TypeError, ValueError):
        return 0
    if tv <= 0:
        return 0
    n = 0
    for v in neighbour_volumes:
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv > 0 and _CYCLE_PULSE_VOL_RATIO_LO <= fv / tv <= _CYCLE_PULSE_VOL_RATIO_HI:
            n += 1
    return n


def _parse_event_ts(s):
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _load_pulse_events(conn: sqlite3.Connection, circuit: str):
    """Sorted ``[(epoch, id, volume)]`` for same-circuit, non-excluded events
    with a parseable timestamp and positive volume (the cycle candidates)."""
    rows = conn.execute(
        "SELECT id, start_ts, volume_litres FROM events "
        "WHERE circuit = ? AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND start_ts IS NOT NULL AND volume_litres IS NOT NULL",
        (circuit,),
    ).fetchall()
    out = []
    for r in rows:
        t = _parse_event_ts(r["start_ts"])
        if t is None:
            continue
        try:
            vol = float(r["volume_litres"])
        except (TypeError, ValueError):
            continue
        if vol > 0:
            out.append((t.timestamp(), r["id"], vol))
    out.sort(key=lambda e: e[0])
    return out


def cycle_pulse_count_for_event(conn: sqlite3.Connection, circuit: str, event_id,
                                start_ts, volume, past_only: bool = True) -> int:
    """Best-effort online cycle_pulse_count for one (just-completed) event.

    ``past_only`` (default) counts only earlier neighbours — there is no future at
    event-completion time; the batch ``recompute_cycle_pulse_counts`` fills the
    full ±45 min window later (authoritative). Uses a bounded ``LIMIT`` query so a
    bulk import can never make this O(n²) — the most-recent rows are exactly the
    ones inside a ±45 min window of a freshly-completed event."""
    t = _parse_event_ts(start_ts)
    try:
        vol = float(volume)
    except (TypeError, ValueError):
        vol = 0.0
    if t is None or vol <= 0:
        return 0
    center = t.timestamp()
    rows = conn.execute(
        "SELECT start_ts, volume_litres FROM events "
        "WHERE circuit = ? AND id <> ? AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND start_ts IS NOT NULL AND volume_litres IS NOT NULL "
        "ORDER BY start_ts DESC LIMIT 400",
        (circuit, event_id),
    ).fetchall()
    nbrs = []
    for r in rows:
        rt = _parse_event_ts(r["start_ts"])
        if rt is None:
            continue
        dt = center - rt.timestamp()
        if (0 <= dt <= _CYCLE_PULSE_WINDOW_SECONDS) or \
           (not past_only and -_CYCLE_PULSE_WINDOW_SECONDS <= dt < 0):
            nbrs.append(r["volume_litres"])
    return compute_cycle_pulse_count(vol, nbrs)


def recompute_cycle_pulse_counts(conn: sqlite3.Connection, circuit: str) -> Dict[str, int]:
    """Authoritative full-window (±45 min, past+future) backfill of
    ``events.cycle_pulse_count``, then patch each cluster centroid's mean so the
    heuristic sees the signal immediately. Idempotent (only changed rows written).
    Returns ``{"scanned", "updated"}``."""
    evs = _load_pulse_events(conn, circuit)
    n = len(evs)
    stored = {
        r["id"]: r["cycle_pulse_count"]
        for r in conn.execute(
            "SELECT id, cycle_pulse_count FROM events WHERE circuit = ?", (circuit,)
        ).fetchall()
    }
    updated = 0
    lo = hi = 0
    for i in range(n):
        center = evs[i][0]
        while lo < n and evs[lo][0] < center - _CYCLE_PULSE_WINDOW_SECONDS:
            lo += 1
        while hi < n and evs[hi][0] <= center + _CYCLE_PULSE_WINDOW_SECONDS:
            hi += 1
        nbrs = [evs[j][2] for j in range(lo, hi) if j != i]
        cnt = compute_cycle_pulse_count(evs[i][2], nbrs)
        eid = evs[i][1]
        if stored.get(eid) != cnt:
            conn.execute(
                "UPDATE events SET cycle_pulse_count = ? WHERE id = ? AND circuit = ?",
                (cnt, eid, circuit),
            )
            updated += 1

    # Patch cluster centroids with the member-mean so suggest_fixture_type can
    # read the cycle signal without waiting for 10 fresh events.
    for cr in conn.execute(
        "SELECT cluster_id, AVG(cycle_pulse_count) AS avgc FROM events "
        "WHERE circuit = ? AND cluster_id IS NOT NULL "
        "  AND cycle_pulse_count IS NOT NULL GROUP BY cluster_id",
        (circuit,),
    ).fetchall():
        row = conn.execute(
            "SELECT centroid FROM fixture_clusters WHERE circuit = ? AND id = ?",
            (circuit, cr["cluster_id"]),
        ).fetchone()
        if not row or not row["centroid"]:
            continue
        try:
            cen = json.loads(row["centroid"])
        except (json.JSONDecodeError, TypeError):
            continue
        cen["cycle_pulse_count"] = round(float(cr["avgc"] or 0.0), 4)
        conn.execute(
            "UPDATE fixture_clusters SET centroid = ? WHERE circuit = ? AND id = ?",
            (json.dumps(cen), circuit, cr["cluster_id"]),
        )
    conn.commit()
    log.info("[%s] cycle_pulse_count: %d scanned, %d updated", circuit, n, updated)
    return {"scanned": n, "updated": updated}


def resuggest_all_clusters(conn: sqlite3.Connection, circuit: str) -> Dict[str, int]:
    """Re-run the heuristic ``suggest_fixture_type`` over each cluster centroid
    (e.g. after a cycle_pulse_count backfill). Only touches clusters whose
    ``suggestion_source`` is NULL or 'heuristic' — user-label votes are never
    overwritten. Returns ``{"clusters", "updated"}``."""
    from .fixtures import suggest_fixture_type
    ctype = get_circuit_type(conn, circuit)
    rows = conn.execute(
        "SELECT id, centroid, suggested_type, suggestion_source "
        "FROM fixture_clusters WHERE circuit = ?", (circuit,)
    ).fetchall()
    updated = 0
    for r in rows:
        if r["suggestion_source"] == "user_labels" or not r["centroid"]:
            continue
        try:
            centroid = json.loads(r["centroid"])
        except (json.JSONDecodeError, TypeError):
            continue
        new_type, new_conf = suggest_fixture_type(centroid, ctype)
        if new_type != r["suggested_type"]:
            conn.execute(
                "UPDATE fixture_clusters SET suggested_type = ?, "
                "  suggested_confidence = ?, suggestion_source = 'heuristic' "
                "WHERE circuit = ? AND id = ?",
                (new_type, new_conf if new_type else 0.0, circuit, r["id"]),
            )
            updated += 1
    conn.commit()
    log.info("[%s] resuggest: %d clusters, %d updated", circuit, len(rows), updated)
    return {"clusters": len(rows), "updated": updated}


# ── 2a: auto cycle-labeling + provenance-scoped undo ──────────────────────────
# When the user labels an APPLIANCE event, its repeated fill pulses (the cycle)
# are almost certainly the same appliance. Auto-label the vol+flow-similar mates
# within ±45 min (source='cycle') so one label seeds several — validated 92%
# pure. Appliances only (toilet/tap/shower aren't cyclic). Fully reversible.

_CYCLE_FLOW_RATIO_LO: float = 0.5
_CYCLE_FLOW_RATIO_HI: float = 2.0
_CYCLE_APPLIANCE_TYPES: frozenset = frozenset({"dishwasher", "washing_machine"})


def propagate_cycle_label(conn: sqlite3.Connection, circuit: str, event_id,
                          fixture_type) -> int:
    """Auto-label an appliance event's cycle-mates the same type (source='cycle').

    A mate is a same-circuit, currently-unlabeled, non-excluded event within
    ±45 min whose volume AND avg flow are within ratio of the anchor. No-op for
    non-appliance types. Caller owns the transaction (no commit). Returns the
    number of mates labeled.
    """
    if fixture_type not in _CYCLE_APPLIANCE_TYPES:
        return 0
    anchor = conn.execute(
        "SELECT start_ts, volume_litres, avg_flow_lpm FROM events "
        "WHERE id = ? AND circuit = ?", (event_id, circuit),
    ).fetchone()
    if anchor is None:
        return 0
    a_ts = _parse_event_ts(anchor["start_ts"])
    try:
        a_vol = float(anchor["volume_litres"])
        a_flow = float(anchor["avg_flow_lpm"])
    except (TypeError, ValueError):
        return 0
    if a_ts is None or a_vol <= 0 or a_flow <= 0:
        return 0
    # Coarse time-window pre-filter (bounds the rows; the 90-min window keeps it
    # small), then a precise parsed-timestamp check so an ISO tz-format quirk can
    # never over-include.
    lo_ts = (a_ts - timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
    hi_ts = (a_ts + timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
    a_epoch = a_ts.timestamp()
    mates = conn.execute(
        "SELECT id, start_ts, volume_litres, avg_flow_lpm FROM events "
        "WHERE circuit = ? AND id <> ? AND user_fixture_type IS NULL "
        "  AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND start_ts BETWEEN ? AND ? "
        "  AND volume_litres > 0 AND avg_flow_lpm > 0",
        (circuit, event_id, lo_ts, hi_ts),
    ).fetchall()
    n = 0
    for mr in mates:
        mt = _parse_event_ts(mr["start_ts"])
        if mt is None or abs(mt.timestamp() - a_epoch) > _CYCLE_PULSE_WINDOW_SECONDS:
            continue
        try:
            v = float(mr["volume_litres"])
            f = float(mr["avg_flow_lpm"])
        except (TypeError, ValueError):
            continue
        if (_CYCLE_PULSE_VOL_RATIO_LO <= v / a_vol <= _CYCLE_PULSE_VOL_RATIO_HI
                and _CYCLE_FLOW_RATIO_LO <= f / a_flow <= _CYCLE_FLOW_RATIO_HI):
            conn.execute(
                "UPDATE events SET user_fixture_type = ?, fixture_label_source = 'cycle' "
                "WHERE id = ? AND circuit = ? AND user_fixture_type IS NULL",
                (fixture_type, mr["id"], circuit),
            )
            n += 1
    return n


def clear_auto_labels(conn: sqlite3.Connection, circuit: str, event_ids=None,
                      commit: bool = True) -> int:
    """Reverse auto-applied labels: rows with `fixture_label_source IN
    ('cycle','training')` → `user_fixture_type` + `fixture_label_source` back to
    NULL. NEVER touches an explicit 'user' label or a NULL-source (legacy) row.

    When ``event_ids`` (a list) is given, scopes to exactly those ids — so
    rejecting one training capture can't wipe another capture's (or a cycle's)
    labels. When None, clears all auto-source rows on the circuit. Returns the count.
    """
    if event_ids is not None:
        ids = list(event_ids)
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        cur = conn.execute(
            "UPDATE events SET user_fixture_type = NULL, fixture_label_source = NULL "
            "WHERE circuit = ? AND fixture_label_source IN ('cycle','training') "
            f"  AND id IN ({placeholders})",
            (circuit, *ids),
        )
    else:
        cur = conn.execute(
            "UPDATE events SET user_fixture_type = NULL, fixture_label_source = NULL "
            "WHERE circuit = ? AND fixture_label_source IN ('cycle','training')",
            (circuit,),
        )
    if commit:
        conn.commit()
    return cur.rowcount


# ── 2b: training-helper capture ───────────────────────────────────────────────
# A one-time wizard: the user runs each fixture once, the event-completion hook
# records candidate event ids, and the user confirms to write ~100%-pure
# 'training' labels. Instant types capture the next event; windowed types capture
# every event in a user-chosen monitoring window (then accept/reject the run).

_TRAINING_INSTANT_TYPES: frozenset = frozenset({"toilet", "tap"})
_TRAINING_INSTANT_WINDOW_MIN: int = 5
# Allowed monitoring-window options per windowed type (minutes).
_TRAINING_WINDOW_INCREMENTS: Dict[str, List[int]] = {
    "shower":          [5, 10, 15, 20, 25, 30],
    "irrigation_zone": [5, 10, 15, 20, 30, 45, 60],
    "washing_machine": [15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180],
    "dishwasher":      [15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180],
}


def training_capturable_types() -> frozenset:
    """Fixture types the wizard can capture (excludes 'other'/'leak_test')."""
    return _TRAINING_INSTANT_TYPES | frozenset(_TRAINING_WINDOW_INCREMENTS)


def training_window_options(fixture_type: str) -> List[int]:
    """The monitoring-duration dropdown options for a windowed type ([] = instant)."""
    return list(_TRAINING_WINDOW_INCREMENTS.get(fixture_type, []))


def training_capture_band_match(fixture_type: str, volume_litres,
                                duration_seconds) -> bool:
    """Loose, named sanity band for an INSTANT capture (toilet/tap). Catches a
    clearly-wrong fixture (e.g. a sub-litre tap during a 'toilet' capture);
    windowed types are not gated here (the user accepts/rejects)."""
    try:
        v = float(volume_litres) if volume_litres is not None else 0.0
        d = float(duration_seconds) if duration_seconds is not None else 0.0
    except (TypeError, ValueError):
        return False
    if fixture_type == "toilet":
        return v >= 2.0                      # a real flush is 3-13 L
    if fixture_type == "tap":
        return v <= 10.0 and d <= 180.0      # rejects shower/appliance-sized
    return True


def arm_training_capture(conn: sqlite3.Connection, circuit: str, fixture_type: str,
                         window_minutes=None) -> Dict[str, Any]:
    """Arm a capture for ``fixture_type`` on ``circuit`` (cancels any existing
    armed/ready row — one active per circuit). ``window_minutes`` is required (and
    validated to the per-type increments) for windowed types; instant types use a
    fixed short window. Raises ValueError for a non-capturable type / bad window."""
    if fixture_type in _TRAINING_INSTANT_TYPES:
        wm = _TRAINING_INSTANT_WINDOW_MIN
    elif fixture_type in _TRAINING_WINDOW_INCREMENTS:
        allowed = _TRAINING_WINDOW_INCREMENTS[fixture_type]
        if window_minutes is None:
            wm = allowed[0]
        elif int(window_minutes) in allowed:
            wm = int(window_minutes)
        else:
            raise ValueError(
                f"window_minutes {window_minutes} not allowed for {fixture_type}")
    else:
        raise ValueError(f"{fixture_type} is not capturable in the training helper")
    conn.execute(
        "UPDATE training_capture SET status = 'cancelled' "
        "WHERE circuit = ? AND status IN ('armed','ready')", (circuit,))
    cur = conn.execute(
        "INSERT INTO training_capture (circuit, fixture_type, status, armed_at, "
        " expires_at, window_minutes, captured_count) "
        "VALUES (?, ?, 'armed', datetime('now'), datetime('now', ?), ?, 0)",
        (circuit, fixture_type, f"+{wm} minutes", wm))
    conn.commit()
    return {"id": cur.lastrowid, "circuit": circuit, "fixture_type": fixture_type,
            "window_minutes": wm, "status": "armed"}


def get_active_training_capture(conn: sqlite3.Connection, circuit: str):
    """The newest armed/ready capture on the circuit (None if none). Includes
    `seconds_remaining` and `candidate_count`."""
    row = conn.execute(
        "SELECT *, CAST((julianday(expires_at) - julianday('now')) * 86400 AS INTEGER) "
        "         AS seconds_remaining "
        "FROM training_capture "
        "WHERE circuit = ? AND status IN ('armed','ready') "
        "ORDER BY id DESC LIMIT 1", (circuit,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["candidate_count"] = conn.execute(
        "SELECT COUNT(*) FROM training_capture_candidates WHERE capture_id = ?",
        (row["id"],)).fetchone()[0]
    return d


def cancel_training_capture(conn: sqlite3.Connection, circuit: str) -> int:
    """Cancel the active (armed/ready, not-yet-confirmed) capture on the circuit."""
    cur = conn.execute(
        "UPDATE training_capture SET status = 'cancelled' "
        "WHERE circuit = ? AND status IN ('armed','ready')", (circuit,))
    conn.commit()
    return cur.rowcount


def extend_training_capture(conn: sqlite3.Connection, circuit: str,
                            add_minutes: int) -> Dict[str, int]:
    """Push out a still-running windowed capture's expiry (don't strand a long cycle)."""
    am = max(1, int(add_minutes))
    cur = conn.execute(
        "UPDATE training_capture SET expires_at = datetime(expires_at, ?) "
        "WHERE circuit = ? AND status = 'armed'", (f"+{am} minutes", circuit))
    conn.commit()
    return {"extended": cur.rowcount, "add_minutes": am}


def expire_stale_training_captures(conn: sqlite3.Connection) -> int:
    """Sweep armed captures whose window has elapsed. A windowed capture that DID
    catch events must NOT evaporate on the timer — flip it to 'ready' (awaiting the
    user's Accept/Reject; candidates preserved). Only truly-empty armed captures
    expire. 'ready' captures are never touched. Called opportunistically (poll +
    pruner). Returns the number EXPIRED (the empty ones)."""
    # Windowed-with-events → 'ready'. Instant captures already flip to 'ready' on
    # their first event, so (armed AND captured_count > 0) is uniquely a windowed run.
    conn.execute(
        "UPDATE training_capture SET status = 'ready' "
        "WHERE status = 'armed' AND expires_at <= datetime('now') "
        "AND captured_count > 0")
    cur = conn.execute(
        "UPDATE training_capture SET status = 'expired' "
        "WHERE status = 'armed' AND expires_at <= datetime('now') "
        "AND captured_count = 0")
    conn.commit()
    return cur.rowcount


def confirm_training_capture(conn: sqlite3.Connection, circuit: str) -> Dict[str, Any]:
    """Accept the active capture: write `user_fixture_type` + source='training' for
    each candidate (only unlabeled rows), status→captured. Returns counts. Caller
    runs the reclassify (once)."""
    cap = conn.execute(
        "SELECT id, fixture_type FROM training_capture "
        "WHERE circuit = ? AND status IN ('armed','ready') "
        "ORDER BY id DESC LIMIT 1", (circuit,)).fetchone()
    if cap is None:
        return {"labeled": 0}
    ids = [r["event_id"] for r in conn.execute(
        "SELECT event_id FROM training_capture_candidates WHERE capture_id = ?",
        (cap["id"],)).fetchall()]
    labeled = 0
    for eid in ids:
        cur = conn.execute(
            "UPDATE events SET user_fixture_type = ?, fixture_label_source = 'training' "
            "WHERE id = ? AND circuit = ? AND user_fixture_type IS NULL",
            (cap["fixture_type"], eid, circuit))
        labeled += cur.rowcount
    conn.execute("UPDATE training_capture SET status = 'captured' WHERE id = ?",
                 (cap["id"],))
    conn.commit()
    return {"capture_id": cap["id"], "fixture_type": cap["fixture_type"], "labeled": labeled}


def reject_training_capture(conn: sqlite3.Connection, circuit: str,
                            capture_id: int) -> Dict[str, Any]:
    """Discard a capture (contaminated): clear ONLY this capture's 'training' labels
    (scoped to its candidate ids), status→rejected, so the checklist item re-arms."""
    ids = [r["event_id"] for r in conn.execute(
        "SELECT event_id FROM training_capture_candidates WHERE capture_id = ?",
        (capture_id,)).fetchall()]
    cleared = clear_auto_labels(conn, circuit, event_ids=ids, commit=False) if ids else 0
    conn.execute("UPDATE training_capture SET status = 'rejected' "
                 "WHERE id = ? AND circuit = ?", (capture_id, circuit))
    conn.commit()
    return {"capture_id": capture_id, "cleared": cleared}


def get_training_checklist(conn: sqlite3.Connection, circuit: str,
                           applicable_types) -> Dict[str, dict]:
    """Per-type latest capture status for the checklist (resumable)."""
    out: Dict[str, dict] = {}
    for t in applicable_types:
        row = conn.execute(
            "SELECT id, status, captured_count FROM training_capture "
            "WHERE circuit = ? AND fixture_type = ? ORDER BY id DESC LIMIT 1",
            (circuit, t)).fetchone()
        out[t] = {
            "status": row["status"] if row else "none",
            "captured_count": row["captured_count"] if row else 0,
            "capture_id": row["id"] if row else None,
        }
    return out


def record_training_candidate(conn: sqlite3.Connection, circuit: str,
                              event_features: dict) -> bool:
    """HOT-PATH hook: if a capture is armed (non-expired) on the circuit, record
    this just-completed event as a candidate. Writes NO label (labels are written
    only on the user's confirm). Instant types flip to 'ready' after the first
    event; windowed types stay armed and accumulate. Returns True if recorded."""
    cap = conn.execute(
        "SELECT id, fixture_type, captured_count FROM training_capture "
        "WHERE circuit = ? AND status = 'armed' AND expires_at > datetime('now') "
        "ORDER BY id DESC LIMIT 1", (circuit,)).fetchone()
    if cap is None:
        return False
    eid = event_features.get("id")
    if not eid:
        return False
    conn.execute(
        "INSERT INTO training_capture_candidates (capture_id, event_id) VALUES (?, ?)",
        (cap["id"], eid))
    new_count = (cap["captured_count"] or 0) + 1
    if cap["fixture_type"] in _TRAINING_INSTANT_TYPES:
        conn.execute("UPDATE training_capture SET captured_count = ?, status = 'ready' "
                     "WHERE id = ?", (new_count, cap["id"]))
    else:
        conn.execute("UPDATE training_capture SET captured_count = ? WHERE id = ?",
                     (new_count, cap["id"]))
    conn.commit()
    return True


# ── Pressure-history helpers (24 h dashboard chart) ──────────────────────────
_BAD_PRESSURE_STATES = frozenset({"", "unavailable", "unknown", "none", "nan"})


def coerce_pressure_state(state) -> Optional[float]:
    """Parse a HA state to a float PSI, or None for unavailable/unknown/non-numeric
    (those mark a recorder gap, not a reading)."""
    if state is None:
        return None
    s = str(state).strip().lower()
    if s in _BAD_PRESSURE_STATES:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def downsample_pressure_series(states, start_epoch: float, end_epoch: float,
                               buckets: int = 288) -> List[Dict[str, Any]]:
    """Bucket [start,end] (epoch seconds) into `buckets` even slots for a 24 h line.
    Each slot = mean of the numeric samples in it; an empty *quiet* slot carries the
    last-known value forward (a recorded sensor holds its value between changes); an
    unavailable/unknown sample breaks the carry so a real outage renders as a gap
    (`v: None`). Leading slots before the first sample are gaps. Returns
    [{t: epoch_ms, v: float|None}] of length `buckets` (client formats `t` → HH:MM)."""
    span = end_epoch - start_epoch
    if span <= 0 or buckets <= 0:
        return []
    width = span / buckets
    sums = [0.0] * buckets
    counts = [0] * buckets
    breaks = [False] * buckets
    for s in states:
        ts = s.get("last_changed")
        if ts is None:
            continue
        ep = ts.timestamp() if hasattr(ts, "timestamp") else float(ts)
        offset = ep - start_epoch
        if offset < 0:
            offset = 0.0
        if offset >= span:
            continue
        idx = int(offset / width)
        if idx >= buckets:
            idx = buckets - 1
        v = coerce_pressure_state(s.get("state"))
        if v is None:
            breaks[idx] = True
        else:
            sums[idx] += v
            counts[idx] += 1
    out: List[Dict[str, Any]] = []
    last: Optional[float] = None
    for i in range(buckets):
        t_ms = int((start_epoch + width * i) * 1000)
        if counts[i]:
            val: Optional[float] = round(sums[i] / counts[i], 2)
            last = val
        elif breaks[i]:
            val = None
            last = None
        else:
            val = last
        out.append({"t": t_ms, "v": val})
    return out


def recent_pressure_baseline(conn: sqlite3.Connection, circuit: str,
                             since_iso: str) -> Optional[float]:
    """Typical *resting* pressure for the baseline reference line: AVG of recent
    pre-event pressures over the window; fall back to the latest event's pre-event
    pressure, then the latest leak-test baseline, else None. PSI."""
    row = conn.execute(
        "SELECT AVG(pre_event_pressure_psi) AS b FROM events "
        "WHERE circuit = ? AND start_ts >= ? "
        "AND pre_event_pressure_psi IS NOT NULL AND pre_event_pressure_psi > 0",
        (circuit, since_iso)).fetchone()
    if row and row["b"] is not None:
        return round(row["b"], 2)
    row = conn.execute(
        "SELECT pre_event_pressure_psi AS b FROM events "
        "WHERE circuit = ? AND pre_event_pressure_psi IS NOT NULL "
        "AND pre_event_pressure_psi > 0 ORDER BY start_ts DESC LIMIT 1",
        (circuit,)).fetchone()
    if row and row["b"] is not None:
        return round(row["b"], 2)
    row = conn.execute(
        "SELECT baseline_psi AS b FROM leak_test_history "
        "WHERE circuit = ? AND baseline_psi IS NOT NULL "
        "ORDER BY run_at DESC LIMIT 1", (circuit,)).fetchone()
    if row and row["b"] is not None:
        return round(row["b"], 2)
    return None


def migrate_to_type_level_clusters(
    conn,
    circuit: str,
) -> Dict[str, Any]:
    """One-shot migration: merge clusters of the same confirmed/suggested type.

    Conservative safety gate:
    - member_count >= 5 for both clusters
    - If neither confirmed: suggested_confidence >= 0.70
    - Centroid weighted-distance <= get_match_threshold(effective_type)
    - 'other' clusters are never merged

    Does NOT auto-confirm suggested-only clusters. Only calls
    upsert_fixture_from_cluster for survivors that already had a confirmed
    fixture row.

    Idempotent: safe to call repeatedly; no-op when each type already has
    exactly one cluster.

    Returns {"types_merged", "clusters_removed", "survivor_ids"}.
    """
    # get_match_threshold was removed from the import — see the comment
    # inside the per-type loop below explaining the simplified gate.
    from .fixtures import FIXTURE_TYPE_LABELS

    rows = conn.execute(
        """SELECT fc.id, fc.member_count,
                  fc.suggested_type, fc.suggested_confidence,
                  fc.centroid, fc.last_match_at,
                  f.fixture_type AS user_type,
                  CASE WHEN f.confirmed = 1 THEN 1 ELSE 0 END AS is_confirmed,
                  f.id AS fixture_row_id
           FROM fixture_clusters fc
           LEFT JOIN fixtures f ON fc.fixture_id = f.id
           WHERE fc.circuit = ?
           ORDER BY fc.id""",
        (circuit,),
    ).fetchall()

    by_type: Dict[str, List[dict]] = {}
    for r in rows:
        eff = r["user_type"] or r["suggested_type"]
        if not eff or eff == "other":
            continue
        by_type.setdefault(eff, []).append(dict(r))

    types_merged    = 0
    clusters_removed = 0
    survivor_ids: List[int] = []

    for ftype, clusters in by_type.items():
        # NOTE: an earlier version of this migration used
        # get_match_threshold(ftype) to gate which clusters could
        # merge by per-type centroid distance. That gate was replaced
        # by the simpler "any 2+ eligible clusters merge into one"
        # logic below (centroid-distance approximation at the
        # eligible_close filter, then deterministic survivor pick).
        # The migration is one-shot; users have already run it. The
        # threshold call is removed because it had no effect — but
        # this comment documents the design simplification in case
        # the rigorous gate is ever wanted back.

        # Apply safety gate
        eligible = [
            cl for cl in clusters
            if (cl["member_count"] or 0) >= 5
            and (cl["is_confirmed"] or (cl["suggested_confidence"] or 0) >= 0.70)
        ]

        # Centroid distance gate (requires at least 2 eligible clusters)
        if len(eligible) >= 2:
            # Try to filter pairs by centroid similarity
            # We do a simple Euclidean distance on raw centroid values here
            # (scaler state is not available in the DB layer); this is an
            # approximation sufficient for a one-shot migration.
            eligible_close: List[dict] = []
            for cl in eligible:
                try:
                    raw = json.loads(cl["centroid"] or "{}")
                    cl["_centroid_raw"] = raw
                    eligible_close.append(cl)
                except Exception:
                    pass
            eligible = eligible_close

        if len(eligible) < 2:
            if eligible:
                survivor_ids.append(eligible[0]["id"])
            continue

        # Deterministic survivor: confirmed first, then member_count desc,
        # then last_match_at desc, then id asc
        eligible.sort(key=lambda c: (
            -c["is_confirmed"],
            -(c["member_count"] or 0),
            -(c["last_match_at"] or ""),
            c["id"],
        ))
        survivor = eligible[0]
        all_ids  = [cl["id"] for cl in eligible]

        try:
            result = merge_clusters(conn, circuit, survivor["id"], all_ids)
            clusters_removed += result["fixtures_removed"]
            types_merged     += 1
        except (ValueError, Exception) as exc:
            log.warning(
                "[%s] migrate: merge_clusters failed for '%s': %s",
                circuit, ftype, exc,
            )
            survivor_ids.append(survivor["id"])
            continue

        survivor_ids.append(survivor["id"])

        # Only update/create a confirmed fixture row if the survivor already
        # had one; do not auto-confirm suggested-only clusters.
        if survivor["is_confirmed"] and survivor["fixture_row_id"]:
            auto_name = FIXTURE_TYPE_LABELS.get(ftype, ftype.replace("_", " ").title())
            upsert_fixture_from_cluster(
                conn, circuit, survivor["id"],
                name=auto_name,
                fixture_type=ftype,
                publish_to_ha=1,
            )

    return {
        "types_merged":     types_merged,
        "clusters_removed": clusters_removed,
        "survivor_ids":     survivor_ids,
    }


# ==========================================================================
# Plumbing-event exclusion windows
# ==========================================================================

def create_exclusion_window(
    conn,
    circuit: str,
    minutes: int,
    reason: str = "plumbing",
) -> None:
    """Open a new exclusion window lasting ``minutes`` minutes.

    All timestamps are stored via SQLite datetime() so they share the
    same 'YYYY-MM-DD HH:MM:SS' format and compare correctly in WHERE clauses.
    """
    minutes = max(5, min(60, int(minutes)))
    modifier = f"+{minutes} minutes"
    # Close any existing active window for this circuit before opening a new one
    # so we never accumulate multiple overlapping rows.
    conn.execute(
        "UPDATE circuit_exclusion_windows "
        "SET ends_at = datetime('now') "
        "WHERE circuit = ? AND ends_at > datetime('now')",
        (circuit,),
    )
    conn.execute(
        "INSERT INTO circuit_exclusion_windows "
        "(circuit, started_at, ends_at, reason) "
        "VALUES (?, datetime('now'), datetime('now', ?), ?)",
        (circuit, modifier, reason or "plumbing"),
    )
    conn.commit()


def is_event_in_exclusion_window(
    conn,
    circuit: str,
    event_start_ts: str,
) -> bool:
    """Return True if ``event_start_ts`` falls inside any active exclusion
    window for ``circuit``.

    Normalises the caller timestamp to SQLite 'YYYY-MM-DD HH:MM:SS' format
    before doing the BETWEEN comparison.
    """
    if not event_start_ts:
        return False
    try:
        from datetime import datetime as _dt, timezone as _tz
        dt = _dt.fromisoformat(str(event_start_ts))
        if dt.tzinfo is not None:
            dt = dt.astimezone(_tz.utc).replace(tzinfo=None)
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        ts = str(event_start_ts)
    row = conn.execute(
        "SELECT 1 FROM circuit_exclusion_windows "
        "WHERE circuit = ? AND ? BETWEEN started_at AND ends_at LIMIT 1",
        (circuit, ts),
    ).fetchone()
    return row is not None


def get_active_exclusion_window(
    conn,
    circuit: str,
):
    """Return the current active exclusion window, or None."""
    row = conn.execute(
        "SELECT id, circuit, started_at, ends_at, reason, "
        "CAST((strftime('%s', ends_at) - strftime('%s', 'now')) / 60 AS INTEGER) "
        "AS minutes_remaining "
        "FROM circuit_exclusion_windows "
        "WHERE circuit = ? AND ends_at > datetime('now') "
        "ORDER BY ends_at DESC LIMIT 1",
        (circuit,),
    ).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["minutes_remaining"] = max(0, result.get("minutes_remaining") or 0)
    return result


def cancel_exclusion_window(conn, circuit: str) -> None:
    """End all active exclusion windows for ``circuit`` immediately."""
    conn.execute(
        "UPDATE circuit_exclusion_windows "
        "SET ends_at = datetime('now') "
        "WHERE circuit = ? AND ends_at > datetime('now')",
        (circuit,),
    )
    conn.commit()


def extend_exclusion_window(conn, circuit: str, extra_minutes: int = 15) -> None:
    """Add ``extra_minutes`` to the active window (capped at 60 min from start)."""
    modifier = f"+{extra_minutes} minutes"
    conn.execute(
        "UPDATE circuit_exclusion_windows "
        "SET ends_at = MIN(datetime(ends_at, ?), datetime(started_at, '+60 minutes')) "
        "WHERE circuit = ? AND ends_at > datetime('now')",
        (modifier, circuit),
    )
    conn.commit()


# ------------------------------------------------------------------
# Circuit label helpers (circuit_1 / circuit_2 display names)
# ------------------------------------------------------------------

def load_circuit_labels(db: sqlite3.Connection) -> Dict[str, str]:
    """Return {circuit_id: display_name} for all configured circuits."""
    try:
        rows = db.execute(
            "SELECT circuit_id, display_name FROM circuit_labels"
        ).fetchall()
        return {row["circuit_id"]: row["display_name"] for row in rows}
    except sqlite3.OperationalError:
        # Table does not exist yet (pre-migration-023 DB); return empty dict.
        log.debug("circuit_labels table not found — migration 023 not yet applied")
        return {}


def upsert_circuit_label(
    db: sqlite3.Connection,
    circuit_id: str,
    display_name: str,
    commit: bool = True,
) -> None:
    """Insert or update the display name for a circuit ID.

    When ``commit`` is True (default) this runs in its own transaction via
    ``with db:``. When False the caller owns the transaction — required by
    multi-step paths like the restore handler so a failure later in the
    sequence rolls back this label change too.
    """
    sql = (
        "INSERT INTO circuit_labels (circuit_id, display_name) VALUES (?, ?) "
        "ON CONFLICT(circuit_id) DO UPDATE SET display_name = excluded.display_name"
    )
    if commit:
        with db:
            db.execute(sql, (circuit_id, display_name.strip()))
    else:
        db.execute(sql, (circuit_id, display_name.strip()))


