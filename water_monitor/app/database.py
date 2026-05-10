"""
SQLite database setup, migrations, and data access helpers.

Single database file at /data/water_monitor.db.
Schema is created in full on first run. All Phase 2 tables are
created now so Phase 2 never needs a schema migration.
"""
from __future__ import annotations

import json
import logging
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

-- CSRF tokens (one per browser session, rotated on use)
CREATE TABLE IF NOT EXISTS csrf_tokens (
    token       TEXT PRIMARY KEY,
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
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    pressure_drop_event_psi     REAL DEFAULT 2.0,
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
    fixture_id                  TEXT REFERENCES fixtures(id),
    anomaly_score               REAL,
    anomaly_type                TEXT,
    flagged                     BOOLEAN DEFAULT 0,
    user_reviewed               BOOLEAN DEFAULT 0,
    triggered_alert             BOOLEAN DEFAULT 0,
    volume_litres               REAL DEFAULT 0,
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_circuit_ts
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
    conn.execute(
        f"INSERT INTO daily_summary ({cols}) VALUES ({ph}) "
        f"ON CONFLICT(circuit, day) DO UPDATE SET {updates}",
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


def update_data_retention(conn: sqlite3.Connection, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(
        f"UPDATE data_retention SET {sets} WHERE id = 1",
        list(kwargs.values()),
    )
    conn.commit()


def generate_csrf_token(conn: sqlite3.Connection) -> str:
    """Generate and store a new CSRF token. Cleans up tokens older than 24h."""
    import secrets
    conn.execute(
        "DELETE FROM csrf_tokens WHERE created_at < datetime('now', '-1 day')")
    token = secrets.token_hex(32)
    conn.execute("INSERT INTO csrf_tokens (token) VALUES (?)", (token,))
    conn.commit()
    return token


def validate_csrf_token(conn: sqlite3.Connection, token: str) -> bool:
    """Return True if the token exists and is less than 24h old."""
    if not token:
        return False
    row = conn.execute(
        "SELECT token FROM csrf_tokens "
        "WHERE token = ? AND created_at >= datetime('now', '-1 day')",
        (token,)
    ).fetchone()
    return row is not None


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


def set_alert_enabled(conn: sqlite3.Connection, alert_id: str, enabled: bool) -> None:
    conn.execute(
        "UPDATE alert_config SET enabled = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, datetime.now(timezone.utc).isoformat(), alert_id)
    )
    conn.commit()


def insert_event(conn: sqlite3.Connection, event: dict) -> None:
    cols = ", ".join(event.keys())
    placeholders = ", ".join("?" for _ in event)
    conn.execute(f"INSERT OR REPLACE INTO events ({cols}) VALUES ({placeholders})",
                 list(event.values()))
    conn.commit()


def get_daily_volume(conn: sqlite3.Connection, circuit: str) -> float:
    """
    Total volume since midnight UTC today.
    Computed from the internal hourly_volume table.
    """
    row = conn.execute("""
        SELECT COALESCE(SUM(volume_litres), 0)
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= strftime('%Y-%m-%dT00:00:00', 'now')
    """, (circuit,)).fetchone()
    return round(row[0], 1) if row else 0.0


def get_weekly_volume(conn: sqlite3.Connection, circuit: str) -> float:
    """
    Total volume since the most recent Monday midnight UTC.
    Computed from the internal hourly_volume table.
    """
    # Compute days since Monday: strftime('%w') = 0(Sun)…6(Sat).
    # (w + 6) % 7 maps Mon→0, Tue→1, …, Sun→6 — subtract from 'now' to get this Monday.
    row = conn.execute("""
        SELECT COALESCE(SUM(volume_litres), 0)
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= strftime('%Y-%m-%dT00:00:00',
                         date('now', '-' || ((CAST(strftime('%w', 'now') AS INTEGER) + 6) % 7) || ' days'))
    """, (circuit,)).fetchone()
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
) -> float:
    """
    Daily volume from the authoritative HA cumulative sensor.
    Uses midnight local-time today as the baseline period.
    """
    today_midnight = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).isoformat(timespec="seconds")
    baseline = _get_volume_baseline(conn, circuit, today_midnight, current_ha_value)
    return round(max(0.0, current_ha_value - baseline), 1)


def compute_ha_weekly_volume(
    conn: sqlite3.Connection,
    circuit: str,
    current_ha_value: float,
) -> float:
    """
    Weekly volume from the authoritative HA cumulative sensor.
    Uses Monday midnight local-time as the baseline period.
    """
    now = datetime.now()
    monday = now - timedelta(days=now.weekday())
    week_midnight = monday.replace(
        hour=0, minute=0, second=0, microsecond=0
    ).isoformat(timespec="seconds")
    baseline = _get_volume_baseline(conn, circuit, week_midnight, current_ha_value)
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

    alerts = base_alerts
    if circuit_type == "zone":
        alerts = alerts + zone_only_alerts

    for alert_type, label, description in alerts:
        alert_id = f"{alert_type}_{circuit}"
        conn.execute("""
            INSERT OR IGNORE INTO alert_config
                (id, circuit, alert_type, label, description, enabled)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (alert_id, circuit, alert_type, label, description))


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


def normalize_events_utc(conn: sqlite3.Connection) -> int:
    """Normalize events.start_ts / end_ts to UTC ISO 8601 in-place.

    Intended to be called before dedup_events() in the Quick Restore path.
    Does NOT recompute UUID5 ids here — that is done by dedup_events() after
    duplicates have been removed.  Recomputing ids before dedup would cause
    a PRIMARY KEY collision when two rows represent the same instant expressed
    in different offsets (both would map to the same UUID5).

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
        conn.commit()
    return len(updates)


def dedup_events(conn: sqlite3.Connection) -> int:
    """Remove duplicate events sharing (circuit, start_ts) and recompute ids.

    Called after Quick Restore to clean any pre-dedup data from old backups.
    Migration 021 (one-time) deduped all existing rows and added a
    UNIQUE(circuit, start_ts) index that prevents write-time duplicates going
    forward — this function is now only needed in the Quick Restore path.

    Idempotent — safe to call multiple times.  Returns count of rows deleted.
    Keeps the most recently inserted row (MAX rowid) on the assumption that
    later inserts have fresher cluster_id / match_confidence.

    Also:
    - Clears cluster_id / match_confidence on survivors of contested groups so
      backfill_unmatched re-matches them with the current engine state.
    - Recomputes UUID5 id = uuid5(NAMESPACE_OID, f"{circuit}/{start_ts}") for
      all survivors, making ids stable so future INSERT OR REPLACE on
      UNIQUE(circuit, start_ts) keeps the row (and its fixture_id) intact.
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


