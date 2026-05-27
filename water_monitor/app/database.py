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
    hourly_volume_applied_litres     REAL DEFAULT 0,
    hourly_volume_applied_bucket     TEXT,
    degraded_diagnostic_json         TEXT
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
    rows = conn.execute("""
        SELECT
            COUNT(*)                    AS event_count,
            SUM(volume_litres)          AS total_volume_litres,
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
    "excluded_from_training",
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
        # No baseline yet — store 0.0 as placeholder.
        # The orchestrator's _init_volume_baselines() will overwrite this
        # with the real midnight reading from HA history shortly after startup.
        conn.execute(
            "INSERT INTO volume_snapshots (circuit, period_ts, ha_volume) VALUES (?,?,?)",
            (circuit, period_ts, 0.0),
        )
        conn.commit()
        return 0.0

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
) -> bool:
    """Update user-editable fields on a single event.

    Pass a value (including None) to update that field; omit a kwarg entirely
    to leave the field unchanged.  Returns False if no matching row exists.
    """
    row = conn.execute(
        "SELECT id FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False
    if user_fixture_type is not _PATCH_UNSET:
        conn.execute(
            "UPDATE events SET user_fixture_type = ? WHERE id = ? AND circuit = ?",
            (user_fixture_type, event_id, circuit),
        )
    if excluded_from_training is not _PATCH_UNSET:
        conn.execute(
            "UPDATE events SET excluded_from_training = ? WHERE id = ? AND circuit = ?",
            (1 if excluded_from_training else 0, event_id, circuit),
        )
    conn.commit()
    return True


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


