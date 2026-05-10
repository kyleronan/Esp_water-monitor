"""
Database migration runner.

Applies schema changes to existing databases that were created before
a new column or table was added. Safe to run on every startup —
each migration is idempotent (CREATE IF NOT EXISTS, ADD COLUMN IF NOT EXISTS).

How to add a new migration:
  1. Add a function _migrate_NNN(conn) that applies the change
  2. Add it to MIGRATIONS list at the bottom
  3. Increment the version number

The current schema version is stored in a simple key-value table.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Callable, List, Tuple

log = logging.getLogger(__name__)


def _get_version(conn: sqlite3.Connection) -> int:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _schema_version (
            version INTEGER NOT NULL DEFAULT 0
        )""")
    row = conn.execute("SELECT version FROM _schema_version").fetchone()
    if not row:
        conn.execute("INSERT INTO _schema_version VALUES (0)")
        conn.commit()
        return 0
    return row[0]


def _set_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute("UPDATE _schema_version SET version = ?", (version,))
    conn.commit()


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,)).fetchone())


# ── Individual migrations ──────────────────────────────────────────────────

def _migrate_001(conn: sqlite3.Connection) -> None:
    """
    Fix #1 & #2: Fix events_retain_years default to 1 year,
    hourly_volume_retain_years default to 2 years.
    Remove dead quiet-period columns from leak_test_schedule (kept for
    existing installs as SQLite can't drop columns before 3.35; we just
    zero them out and document they're unused).
    """
    # Update data_retention defaults for any row that still has the old default of 3
    conn.execute("""
        UPDATE data_retention
        SET events_retain_years        = 1,
            hourly_volume_retain_years = 2
        WHERE events_retain_years = 3
          AND hourly_volume_retain_years = 3
    """)
    log.info("Migration 001: updated data_retention defaults")


def _migrate_002(conn: sqlite3.Connection) -> None:
    """
    Add fixture_breakdown JSON column to daily_summary (improvement #10).
    Stores top-5 fixtures as JSON: [{"fixture_id": "...", "count": N}, ...]
    """
    if not _has_column(conn, "daily_summary", "fixture_breakdown"):
        conn.execute(
            "ALTER TABLE daily_summary ADD COLUMN fixture_breakdown TEXT")
        log.info("Migration 002: added daily_summary.fixture_breakdown")


def _migrate_003(conn: sqlite3.Connection) -> None:
    """
    Add auto_backup settings to data_retention table (improvement #7).
    """
    if not _has_column(conn, "data_retention", "auto_backup_enabled"):
        conn.execute(
            "ALTER TABLE data_retention ADD COLUMN "
            "auto_backup_enabled BOOLEAN DEFAULT 0")
    if not _has_column(conn, "data_retention", "auto_backup_path"):
        conn.execute(
            "ALTER TABLE data_retention ADD COLUMN "
            "auto_backup_path TEXT DEFAULT '/share/water_monitor_backups'")
    if not _has_column(conn, "data_retention", "auto_backup_day_of_week"):
        conn.execute(
            "ALTER TABLE data_retention ADD COLUMN "
            "auto_backup_day_of_week INTEGER DEFAULT 0")  # 0=Monday
    if not _has_column(conn, "data_retention", "last_auto_backup_at"):
        conn.execute(
            "ALTER TABLE data_retention ADD COLUMN "
            "last_auto_backup_at TIMESTAMP")
    log.info("Migration 003: added auto-backup columns to data_retention")


def _migrate_004(conn: sqlite3.Connection) -> None:
    """Ensure daily_summary index exists."""
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_summary_circuit_day
        ON daily_summary (circuit, day)""")
    log.info("Migration 004: ensured daily_summary index")


def _migrate_005(conn: sqlite3.Connection) -> None:
    """
    Add away_mode, mobile_notify, and csrf_tokens to home_profile.
    (Budget/cost columns added here are dropped by migration 012.)
    """
    new_cols = [
        ("away_mode",              "BOOLEAN DEFAULT 0"),
        ("away_since",             "TIMESTAMP"),
        ("away_until",             "TIMESTAMP"),
        ("monthly_budget_litres",  "REAL DEFAULT 0"),  # removed in migration 012
        ("water_cost_per_litre",   "REAL DEFAULT 0"),  # removed in migration 012
        ("water_cost_currency",    "TEXT DEFAULT 'USD'"),  # removed in migration 012
        ("mobile_notify_targets",  "TEXT DEFAULT ''"),
    ]
    for col, defn in new_cols:
        if not _has_column(conn, "home_profile", col):
            conn.execute(
                f"ALTER TABLE home_profile ADD COLUMN {col} {defn}")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS csrf_tokens (
            token       TEXT PRIMARY KEY,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
    log.info("Migration 005: added away_mode, mobile_notify, csrf to home_profile")


# ── Migration registry ─────────────────────────────────────────────────────

def _migrate_006(conn: sqlite3.Connection) -> None:
    """Add HA presence tracking fields to home_profile."""
    new_cols = [
        ("ha_presence_entities", "TEXT DEFAULT ''"),
        ("ha_away_state",        "TEXT DEFAULT 'not_home'"),
        ("ha_home_state",        "TEXT DEFAULT 'home'"),
    ]
    for col, defn in new_cols:
        if not _has_column(conn, "home_profile", col):
            conn.execute(
                f"ALTER TABLE home_profile ADD COLUMN {col} {defn}")
    log.info("Migration 006: added ha_presence tracking to home_profile")


def _migrate_007(conn: sqlite3.Connection) -> None:
    """Add feature extractor v2 columns to events table.

    New columns:
      start_trigger          — which signal(s) opened the event
      has_pressure_transient — whether a pressure transient was captured
      flow_variability       — std dev of 1 Hz flow readings
      duration_log           — log(duration + 1) for ML clustering
      hour_sin / hour_cos    — cyclical time encoding
      is_weekend             — boolean day-type flag
    """
    new_cols = [
        ("start_trigger",          "TEXT    DEFAULT 'unknown'"),
        ("has_pressure_transient", "BOOLEAN DEFAULT 0"),
        ("flow_variability",       "REAL    DEFAULT 0"),
        ("duration_log",           "REAL    DEFAULT 0"),
        ("hour_sin",               "REAL    DEFAULT 0"),
        ("hour_cos",               "REAL    DEFAULT 1"),
        ("is_weekend",             "BOOLEAN DEFAULT 0"),
    ]
    for col, defn in new_cols:
        if not _has_column(conn, "events", col):
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {defn}")
    log.info("Migration 007: added feature extractor v2 columns to events")


def _migrate_008(conn: sqlite3.Connection) -> None:
    """Add volume_snapshots table for accurate HA-sensor-based daily/weekly volumes."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS volume_snapshots (
            circuit     TEXT NOT NULL,
            period_ts   TEXT NOT NULL,
            ha_volume   REAL NOT NULL,
            PRIMARY KEY (circuit, period_ts)
        )
    """)
    log.info("Migration 008: created volume_snapshots table")


def _migrate_009(conn: sqlite3.Connection) -> None:
    """Add import_state table for historical event importer."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS import_state (
            circuit         TEXT PRIMARY KEY,
            last_check_ts   TEXT,
            total_imported  INTEGER DEFAULT 0
        )
    """)
    log.info("Migration 009: created import_state table")


def _migrate_010(conn: sqlite3.Connection) -> None:
    """Add other_valve_open to events for cross-circuit fixture fingerprinting."""
    try:
        conn.execute("ALTER TABLE events ADD COLUMN other_valve_open INTEGER")
        log.info("Migration 010: added other_valve_open to events")
    except sqlite3.OperationalError:
        pass   # column already exists (idempotent)



def _migrate_011(conn: sqlite3.Connection) -> None:
    """Add flow_unit and pressure_unit to home_profile for display unit preferences."""
    for col, default in [
        ("flow_unit",     "L/min"),
        ("pressure_unit", "psi"),
    ]:
        try:
            conn.execute(
                f"ALTER TABLE home_profile ADD COLUMN {col} TEXT DEFAULT '{default}'"
            )
        except sqlite3.OperationalError:
            pass   # column already exists (idempotent)
    log.info("Migration 011: added flow_unit and pressure_unit to home_profile")


def _migrate_012(conn: sqlite3.Connection) -> None:
    """Drop Water Budget & Cost columns — feature removed in 0.1.2."""
    for col in ("monthly_budget_litres", "water_cost_per_litre", "water_cost_currency"):
        try:
            conn.execute(f"ALTER TABLE home_profile DROP COLUMN {col}")
        except Exception:
            pass   # column already gone or SQLite < 3.35 (idempotent)
    log.info("Migration 012: removed budget/cost columns from home_profile")


def _migrate_013(conn: sqlite3.Connection) -> None:
    """
    Phase 2.1 — Fixture clustering foundation.

    Adds:
    - sequence-context columns to events
    - 2.3 placeholder columns to events
    - match_confidence and match_level on events
    - new tables: fixture_clusters, cluster_cooccurrence,
      cluster_sequences (empty placeholder), cluster_metrics_history
    - publish_fixtures_to_ha column on home_profile
    - extension columns on the existing fixtures table

    The pre-existing fixture_id column on events is reused for
    user-confirmed fixtures (Path C — clusters and fixtures coexist).
    cluster_id is reused (already INTEGER) for raw DBSTREAM cluster IDs.
    """
    # events table — sequence and clustering metadata
    new_event_cols = [
        ("match_confidence",          "REAL"),
        ("match_level",               "TEXT"),
        ("seconds_since_prev_event",  "REAL"),
        ("prev_cluster_id",           "INTEGER"),
        ("seconds_to_next_event",     "REAL"),
        ("parent_compound_id",        "TEXT"),
        ("compound_phase",            "TEXT"),
    ]
    for col, sql_type in new_event_cols:
        if not _has_column(conn, "events", col):
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {sql_type}")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_cluster_id "
        "ON events (circuit, cluster_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_prev_cluster "
        "ON events (circuit, prev_cluster_id)"
    )

    # fixture_clusters: raw DBSTREAM output, one row per cluster
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fixture_clusters (
            id                    INTEGER NOT NULL,
            circuit               TEXT NOT NULL,
            centroid              TEXT NOT NULL,
            feature_std           TEXT NOT NULL,
            transient_template    TEXT,
            member_count          INTEGER DEFAULT 0,
            suggested_type        TEXT,
            suggested_confidence  REAL DEFAULT 0,
            confidence_level      TEXT DEFAULT 'preliminary',
            fixture_id            TEXT REFERENCES fixtures(id) ON DELETE SET NULL,
            is_compound           INTEGER DEFAULT 0,
            component_cluster_ids TEXT,
            publish_to_ha         INTEGER DEFAULT 1,
            created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_match_at         TIMESTAMP,
            PRIMARY KEY (circuit, id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_clusters_circuit "
        "ON fixture_clusters (circuit)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_clusters_fixture "
        "ON fixture_clusters (fixture_id)"
    )

    # cluster_cooccurrence: Option F sequence boost
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cluster_cooccurrence (
            circuit             TEXT NOT NULL,
            from_cluster_id     INTEGER NOT NULL,
            to_cluster_id       INTEGER NOT NULL,
            count               INTEGER DEFAULT 0,
            median_gap_seconds  REAL,
            last_seen_at        TIMESTAMP,
            PRIMARY KEY (circuit, from_cluster_id, to_cluster_id)
        )
    """)

    # cluster_sequences: 2.2 placeholder, empty in 2.1
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cluster_sequences (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            circuit           TEXT NOT NULL,
            pattern_hash      TEXT,
            event_chain       TEXT,
            occurrence_count  INTEGER DEFAULT 0,
            confidence        REAL DEFAULT 0,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # cluster_metrics_history: rolling cluster quality metrics
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cluster_metrics_history (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            measured_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            circuit               TEXT NOT NULL,
            cluster_count         INTEGER,
            coverage_pct          REAL,
            avg_purity            REAL,
            avg_stability         REAL,
            unmatched_recent_24h  INTEGER
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_metrics_circuit_ts "
        "ON cluster_metrics_history (circuit, measured_at)"
    )

    # home_profile: master toggle for HA fixture publishing
    if not _has_column(conn, "home_profile", "publish_fixtures_to_ha"):
        conn.execute(
            "ALTER TABLE home_profile ADD COLUMN "
            "publish_fixtures_to_ha INTEGER DEFAULT 1"
        )

    # fixtures table — extend the existing Path C target
    new_fixture_cols = [
        ("fixture_type",  "TEXT"),
        ("display_name",  "TEXT"),
        ("user_locked",   "INTEGER DEFAULT 0"),
        ("publish_to_ha", "INTEGER DEFAULT 1"),
    ]
    for col, sql_type in new_fixture_cols:
        if not _has_column(conn, "fixtures", col):
            conn.execute(f"ALTER TABLE fixtures ADD COLUMN {col} {sql_type}")

    log.info("Migration 013: Phase 2.1 fixture clustering foundation")


def _migrate_014(conn: sqlite3.Connection) -> None:
    """
    Cleanup pass — drop dead columns, add leak test auto/manual toggle.

    - home_profile.away_until: never wired up to any UI, redundant with
      ha_presence_entities. Drop.
    - leak_test_schedule.custom_interval_days: 'custom' frequency was
      never exposed in the UI. Drop and remove the dead code path.
    - leak_test_schedule.auto_learn_hour: NEW — controls whether the
      scheduler should auto-pick the quietest hour from usage history
      (default ON, matches previous behaviour) or use the manually
      configured run_hour/run_minute (when toggled OFF).
    """
    # Drop dead columns (idempotent on SQLite < 3.35 — exception swallowed)
    for table, col in (
        ("home_profile",        "away_until"),
        ("leak_test_schedule",  "custom_interval_days"),
    ):
        try:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
        except Exception:
            pass

    # Add new auto/manual toggle (default 1 = preserve existing behaviour)
    if not _has_column(conn, "leak_test_schedule", "auto_learn_hour"):
        conn.execute(
            "ALTER TABLE leak_test_schedule ADD COLUMN "
            "auto_learn_hour BOOLEAN DEFAULT 1"
        )

    log.info("Migration 014: cleaned up dead columns, added auto_learn_hour")


def _migrate_015(conn: sqlite3.Connection) -> None:
    """Remove duplicate events that share (circuit, start_ts).

    Before the uuid5 fix in feature_extractor.py, each re-processing of the
    same raw event generated a fresh uuid4, so INSERT OR REPLACE never matched
    the existing row and inserted a duplicate instead.  Keep the row with the
    lowest rowid (first inserted) and delete the rest.
    """
    conn.execute("""
        DELETE FROM events
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM events
            GROUP BY circuit, start_ts
        )
    """)
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    log.info("Migration 015: removed %d duplicate event row(s)", deleted)


def _migrate_016(conn: sqlite3.Connection) -> None:
    """Add fixture lookup indexes for Phase 2 query performance."""
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_fixture_id ON events (fixture_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fixtures_circuit ON fixtures (circuit)"
    )
    log.info("Migration 016: fixture lookup indexes added")


def _migrate_017(conn: sqlite3.Connection) -> None:
    """Add mqtt_publish_enabled to home_profile for the Integrations settings section."""
    existing = [row[1] for row in conn.execute("PRAGMA table_info(home_profile)")]
    if "mqtt_publish_enabled" not in existing:
        conn.execute(
            "ALTER TABLE home_profile ADD COLUMN mqtt_publish_enabled INTEGER NOT NULL DEFAULT 0"
        )
    log.info("Migration 017: added mqtt_publish_enabled to home_profile")


def _migrate_018(conn: sqlite3.Connection) -> None:
    """Add fixtures.last_seen_at for fixture health scoring and Fixtures page sort."""
    existing = [row[1] for row in conn.execute("PRAGMA table_info(fixtures)")]
    if "last_seen_at" not in existing:
        conn.execute("ALTER TABLE fixtures ADD COLUMN last_seen_at TIMESTAMP")
    log.info("Migration 018: added fixtures.last_seen_at")


def _migrate_019(conn: sqlite3.Connection) -> None:
    """Create fixture_daily_summary for per-fixture analytics."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fixture_daily_summary (
            circuit             TEXT    NOT NULL,
            fixture_id          TEXT    NOT NULL REFERENCES fixtures(id),
            day                 DATE    NOT NULL,
            event_count         INTEGER NOT NULL DEFAULT 0,
            total_volume_litres REAL    NOT NULL DEFAULT 0,
            avg_flow_lpm        REAL,
            peak_flow_lpm       REAL,
            alert_count         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (circuit, fixture_id, day)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_fixture_daily_circuit_day
            ON fixture_daily_summary (circuit, day)
    """)
    log.info("Migration 019: created fixture_daily_summary table")


def _migrate_020(conn: sqlite3.Connection) -> None:
    """Create fixture_ha_entity_map for MQTT Discovery state tracking."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fixture_ha_entity_map (
            fixture_id          TEXT NOT NULL REFERENCES fixtures(id),
            ha_entity_id        TEXT NOT NULL,
            device_class        TEXT,
            unit_of_measurement TEXT,
            last_published_at   TIMESTAMP,
            retracted_at        TIMESTAMP,
            PRIMARY KEY (fixture_id, ha_entity_id)
        )
    """)
    log.info("Migration 020: created fixture_ha_entity_map table")


def _migrate_021(conn: sqlite3.Connection) -> None:
    """Normalize events to UTC, dedup, add UNIQUE(circuit, start_ts) index.

    Fixes three root-cause bugs that produced duplicate event rows:

    1.  Pre-fix code generated UUID4 ids — same event re-imported twice
        produced two rows because INSERT OR REPLACE matched on PRIMARY KEY
        (id) only, not on (circuit, start_ts).

    2.  event_exists_near() used SQLite datetime() which returns a space-
        separated string ('YYYY-MM-DD HH:MM:SS') while stored start_ts uses
        ISO 8601 'T' separator. ASCII 'T' (84) > ' ' (32) so the upper-
        bound comparison always failed and every event was re-imported on
        every historical catch-up cycle.

    3.  Migration 015 removed duplicates once (MIN rowid) but was one-shot;
        Quick Restore brought pre-dedup data back via INSERT OR REPLACE
        keyed on id, so migration 015 never ran again on those rows.

    Fix applied here:
      a. Normalize all events.start_ts / end_ts to UTC ISO 8601 (+00:00)
         and recompute each row's UUID5 id against the new UTC start_ts so
         future INSERT OR REPLACE on UNIQUE(circuit, start_ts) keeps the
         existing row rather than deleting it and losing fixture_id.
      b. Same UTC normalization for hourly_volume.hour_ts.
      c. Clear cluster_id / match_confidence on dedup survivors where
         multiple rows existed, so backfill_unmatched re-matches them with
         the current engine state.
      d. Delete duplicate rows (keep MAX rowid — newest insert).
      e. Drop the old non-unique idx_events_circuit_ts (superseded).
      f. Create UNIQUE INDEX idx_events_circuit_start_unique — prevents
         any future duplicate at write time.

    The entire migration runs inside a single transaction; a failure rolls
    back without leaving the DB in a half-normalized state.
    """
    import uuid as _uuid
    from datetime import datetime, timezone

    with conn:  # one transaction — rolls back on any exception
        # ── a. Normalize events.start_ts / end_ts to UTC (id unchanged here)
        #    Separating the id recomputation from the timestamp normalization
        #    avoids a PRIMARY KEY collision when two rows represent the same
        #    instant in different offsets and would therefore get the same
        #    new UUID5 id.  We dedup first (step d) then recompute ids (step e).
        rows = conn.execute(
            "SELECT id, circuit, start_ts, end_ts FROM events"
            " WHERE start_ts IS NOT NULL"
        ).fetchall()
        ts_updates = []
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
                    ts_updates.append((new_s, new_e, r["id"]))
            except (ValueError, TypeError):
                continue
        conn.executemany(
            "UPDATE events SET start_ts = ?, end_ts = ? WHERE id = ?",
            ts_updates
        )
        log.info("Migration 021: normalized %d event row(s) to UTC", len(ts_updates))

        # ── b. Normalize hourly_volume ───────────────────────────────────────
        hv_rows = conn.execute(
            "SELECT rowid, hour_ts FROM hourly_volume WHERE hour_ts IS NOT NULL"
        ).fetchall()
        hv_updates = []
        for r in hv_rows:
            try:
                s = datetime.fromisoformat(r["hour_ts"].replace("Z", "+00:00"))
                if s.tzinfo is None:
                    s = s.replace(tzinfo=timezone.utc)
                # Use strftime format (no timezone suffix) to match the
                # storage format written by feature_extractor.py and all
                # DB queries that use SQLite's strftime('%Y-%m-%dT%H:00:00').
                new_s = s.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:00:00')
                if new_s != r["hour_ts"]:
                    hv_updates.append((new_s, r["rowid"]))
            except (ValueError, TypeError):
                continue
        if hv_updates:
            conn.executemany(
                "UPDATE hourly_volume SET hour_ts = ? WHERE rowid = ?",
                hv_updates
            )
            log.info("Migration 021: normalized %d hourly_volume row(s) to UTC",
                     len(hv_updates))

        # ── c. Clear stale cluster_id on contested dedup survivors ───────────
        _cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(events)").fetchall()}
        _mc = ", match_confidence = NULL" if "match_confidence" in _cols else ""
        conn.execute(f"""
            UPDATE events SET cluster_id = NULL{_mc}
            WHERE rowid IN (
                SELECT MAX(rowid) FROM events
                WHERE cluster_id IS NOT NULL
                GROUP BY circuit, start_ts
                HAVING COUNT(*) > 1
            )
        """)

        # ── d. Dedup (keep MAX rowid = newest insert) ────────────────────────
        conn.execute("""
            DELETE FROM events
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM events GROUP BY circuit, start_ts
            )
        """)
        deleted = conn.execute("SELECT changes()").fetchone()[0]
        if deleted:
            log.info("Migration 021: removed %d duplicate event(s)", deleted)

        # ── e. Recompute UUID5 ids for all survivors ──────────────────────────
        #    Now that duplicates are gone and start_ts is UTC, each
        #    (circuit, start_ts) is unique, so new ids cannot collide.
        #    Recomputation is critical: after normalization the old id
        #    (keyed on the pre-UTC string) no longer matches what future
        #    extract_features() generates, so INSERT OR REPLACE on
        #    UNIQUE(circuit, start_ts) would delete the existing row and
        #    lose its fixture_id.  Post-recomputation the ids are stable.
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
            log.info("Migration 021: recomputed UUID5 id on %d event(s)", len(id_updates))

        # ── f. Drop superseded index ──────────────────────────────────────────
        conn.execute("DROP INDEX IF EXISTS idx_events_circuit_ts")

        # ── f. UNIQUE index — prevents future duplicates at write time ───────
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_events_circuit_start_unique
                ON events (circuit, start_ts)
        """)
    log.info("Migration 021: UNIQUE(circuit, start_ts) index created")


def _migrate_022(conn: sqlite3.Connection) -> None:
    """Add fw_version to device_config for firmware version tracking."""
    try:
        conn.execute("ALTER TABLE device_config ADD COLUMN fw_version TEXT")
    except sqlite3.OperationalError:
        pass   # column already exists (idempotent)
    log.info("Migration 022: added fw_version to device_config")


MIGRATIONS: List[Tuple[int, Callable]] = [
    (1, _migrate_001),
    (2, _migrate_002),
    (3, _migrate_003),
    (4, _migrate_004),
    (5, _migrate_005),
    (6, _migrate_006),
    (7, _migrate_007),
    (8, _migrate_008),
    (9, _migrate_009),
    (10, _migrate_010),
    (11, _migrate_011),
    (12, _migrate_012),
    (13, _migrate_013),
    (14, _migrate_014),
    (15, _migrate_015),
    (16, _migrate_016),
    (17, _migrate_017),
    (18, _migrate_018),
    (19, _migrate_019),
    (20, _migrate_020),
    (21, _migrate_021),
    (22, _migrate_022),
]


def run_migrations(conn: sqlite3.Connection) -> None:
    """
    Apply all pending migrations in order. Called once at startup.
    """
    current = _get_version(conn)
    pending  = [(v, fn) for v, fn in MIGRATIONS if v > current]

    if not pending:
        log.debug("Database schema up to date (version %d)", current)
        return

    log.info("Running %d database migration(s) from version %d",
             len(pending), current)

    for version, fn in pending:
        try:
            fn(conn)
            conn.commit()
            _set_version(conn, version)
            log.info("Migration %03d applied", version)
        except Exception as e:
            log.error("Migration %03d failed: %s", version, e, exc_info=True)
            raise
