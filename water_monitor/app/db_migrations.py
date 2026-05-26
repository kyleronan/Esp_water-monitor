"""
Database schema version guard — squashed baseline 20260523.

Startup order (confirmed from main.py):
  1. database.py init_db() → _create_schema() creates all tables
  2. run_migrations() is called → verifies/stamps version

Fresh database: tables created by step 1 include all baseline columns
(including signature_source); this module stamps baseline version.

Old pre-squash database: startup fails fast. Delete the DB and restart.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_BASELINE_VERSION: int = 20260523
# Version bumps:
#   20260524 — retired text-sensor waveform roles
#   20260525 — added UNIQUE(circuit, start_ts) on events (dedup first)
#   20260526 — degraded-supply guard: new event columns, event_waveforms
#              table, rebuild hourly_volume from events
#   20260527 — per-circuit valve_type column on circuit_profile
_CURRENT_VERSION: int = 20260527
# Intermediate stepping-stone version for the dedup-then-unique-index
# migration. Existing DBs at this version have had their wf rows dropped
# but still need the unique index applied.
_VERSION_PRE_UNIQUE_INDEX: int = 20260524
# Intermediate stepping-stone for the degraded-supply migration. Existing
# DBs at this version have the unique index but lack the degraded columns.
_VERSION_PRE_DEGRADED: int = 20260525

# Roles removed when the firmware switched waveform delivery from 5 chunked
# text sensors to a single HA event (firmware 3.8.0). Old DBs may still carry
# circuit_entity_map rows for these — the migration deletes them so the
# discovery wizard doesn't display stale entries.
_RETIRED_WF_ROLES: tuple = (
    "wf_start_flow_sensor",
    "wf_start_pressure_sensor",
    "wf_full_flow_sensor",
    "wf_full_pressure_sensor",
    "wf_metadata_sensor",
)


def _drop_retired_wf_entity_map_rows(conn: sqlite3.Connection) -> None:
    placeholders = ",".join("?" * len(_RETIRED_WF_ROLES))
    cur = conn.execute(
        f"DELETE FROM circuit_entity_map WHERE role IN ({placeholders})",
        _RETIRED_WF_ROLES,
    )
    conn.commit()
    if cur.rowcount:
        log.info("Removed %d stale waveform text-sensor row(s) from circuit_entity_map",
                 cur.rowcount)


def _apply_unique_events_index(conn: sqlite3.Connection) -> None:
    """Add UNIQUE(circuit, start_ts) on events after deduping any historical
    duplicates left behind by older code paths.

    The dedup pass runs first because a CREATE UNIQUE INDEX would fail with
    IntegrityError if the existing data has duplicate (circuit, start_ts)
    pairs. dedup_events keeps the most recently inserted row (MAX rowid),
    clears stale cluster_id on contested groups, and recomputes UUID5 ids.
    Idempotent on its own.

    The non-unique idx_events_circuit_ts is also dropped — its sole purpose
    was the index range scan that the new unique index now serves.
    """
    # Import here so the module remains importable without database.py side
    # effects during test collection.
    from .database import dedup_events
    removed = dedup_events(conn, commit=False)
    if removed:
        log.info(
            "Migration: dedup_events removed %d duplicate row(s) before "
            "applying UNIQUE(circuit, start_ts)", removed,
        )
    conn.execute("DROP INDEX IF EXISTS idx_events_circuit_ts")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_circuit_start_unique "
        "ON events (circuit, start_ts)"
    )
    conn.commit()


def _apply_degraded_supply_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260526.

    Adds 7 new columns on events, creates idx_events_degraded, creates the
    event_waveforms table + index, backfills volume_litres_effective and
    hourly_volume_applied_* for existing rows, then REBUILDS hourly_volume
    from events as source of truth.

    Rebuild filter is INTENTIONALLY broad — every event with a positive
    volume contributes, including those with excluded_from_training=1.
    Clustering exclusion is NOT the same as volume exclusion; degraded
    events still count toward water-usage totals (with their estimated
    value).

    Idempotent on its own (column adds are guarded by _has_column; the
    backfill UPDATEs only touch rows with NULL/0 in the new fields).
    """
    new_cols = (
        ("degraded_supply",               "BOOLEAN DEFAULT 0"),
        ("volume_litres_estimated",       "REAL"),
        ("volume_litres_effective",       "REAL"),
        ("volume_estimation_method",      "TEXT DEFAULT 'raw'"),
        ("hourly_volume_applied_litres",  "REAL DEFAULT 0"),
        ("hourly_volume_applied_bucket",  "TEXT"),
        ("degraded_diagnostic_json",      "TEXT"),
    )
    for col, decl in new_cols:
        if not _has_column(conn, "events", col):
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {decl}")
            log.info("Added events.%s", col)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_degraded "
        "ON events (circuit, start_ts) WHERE degraded_supply = 1"
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS event_waveforms (
            event_id              TEXT PRIMARY KEY
                                  REFERENCES events(id) ON DELETE CASCADE,
            flow_min_json         TEXT NOT NULL,
            flow_max_json         TEXT NOT NULL,
            pressure_min_json     TEXT NOT NULL,
            pressure_max_json     TEXT NOT NULL,
            duration_seconds      REAL NOT NULL,
            created_at            TEXT NOT NULL
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_waveforms_created "
        "ON event_waveforms (created_at)"
    )
    conn.commit()

    # Backfill effective volume + method for existing rows BEFORE the
    # rebuild reads from this column.
    conn.execute(
        "UPDATE events "
        "SET volume_litres_effective = COALESCE(volume_litres, 0), "
        "    volume_estimation_method = 'raw' "
        "WHERE volume_litres_effective IS NULL"
    )

    # Backfill applied bookkeeping so future re-imports subtract correctly.
    # hour_ts format must match _hour_bucket_for() in database.py:
    # '%Y-%m-%dT%H:00:00' UTC, no tz suffix.
    conn.execute(
        "UPDATE events "
        "SET hourly_volume_applied_litres = "
        "      COALESCE(volume_litres_effective, volume_litres, 0), "
        "    hourly_volume_applied_bucket = "
        "      strftime('%Y-%m-%dT%H:00:00', start_ts) "
        "WHERE hourly_volume_applied_bucket IS NULL"
    )
    conn.commit()

    # Rebuild hourly_volume from events as the source of truth.
    # CRITICAL: no excluded_from_training filter — degraded events still
    # count toward volume totals.
    #
    # Temp-table swap pattern: build the new rows into a TEMP table
    # first, only THEN clear hourly_volume and copy across. Anything that
    # raises before the final COMMIT is rolled back atomically, leaving
    # the original hourly_volume intact. If the process dies mid-rebuild,
    # SQLite's transaction durability does the same thing automatically.
    try:
        conn.execute(
            "CREATE TEMP TABLE hourly_volume_rebuild AS "
            "SELECT circuit, "
            "       strftime('%Y-%m-%dT%H:00:00', start_ts) AS hour_ts, "
            "       SUM(COALESCE(volume_litres_effective, volume_litres, 0)) "
            "         AS volume_litres "
            "FROM events "
            "WHERE start_ts IS NOT NULL "
            "GROUP BY circuit, strftime('%Y-%m-%dT%H:00:00', start_ts)"
        )
        conn.execute("DELETE FROM hourly_volume")
        conn.execute(
            "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
            "SELECT circuit, hour_ts, volume_litres "
            "FROM hourly_volume_rebuild"
        )
        conn.execute("DROP TABLE hourly_volume_rebuild")
        conn.commit()
    except Exception:
        conn.rollback()
        # Best-effort cleanup — DROP IF EXISTS so rerun is safe.
        try:
            conn.execute("DROP TABLE IF EXISTS hourly_volume_rebuild")
            conn.commit()
        except Exception:
            pass
        raise
    log.info("Migration 20260526: rebuilt hourly_volume from events; "
             "added 7 degraded-supply columns + event_waveforms table")


def _apply_valve_type_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260527.

    Adds circuit_profile.valve_type with DEFAULT '2_port'. Idempotent —
    column add guarded by _has_column. The DEFAULT clause on ADD COLUMN
    gives all existing rows the value automatically; a defensive backfill
    afterward handles any oddly migrated DB where the new column ended up
    NULL or empty.
    """
    if not _has_column(conn, "circuit_profile", "valve_type"):
        conn.execute(
            "ALTER TABLE circuit_profile "
            "ADD COLUMN valve_type TEXT DEFAULT '2_port'"
        )
        log.info("Added circuit_profile.valve_type (default '2_port')")
    # Defensive backfill — handles legacy / hand-altered rows.
    conn.execute(
        "UPDATE circuit_profile SET valve_type = '2_port' "
        "WHERE valve_type IS NULL OR valve_type = ''"
    )
    conn.commit()


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
    return any(
        row[1] == column
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    )


# Required columns in the events table that only exist in the baseline schema.
# Checking multiple columns is more robust — an old DB might have some
# backfilled but not others.
_BASELINE_EVENT_COLUMNS: frozenset = frozenset({
    "signature_source",        # new in squash (never present in pre-squash DBs)
    "esp_waveform_used",       # added in migration 031
    "pressure_signature_json", # added in migration 029
    "waveform_overlap_score",  # added in migration 031
})

# Columns added by the 20260526 degraded-supply migration. Used to verify the
# migration has actually run on a DB claiming version 20260526 (catches the
# case where _schema_version was stamped without the migration applying).
_DEGRADED_EVENT_COLUMNS: frozenset = frozenset({
    "degraded_supply",
    "volume_litres_effective",
    "hourly_volume_applied_bucket",
})


def _missing_degraded_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _DEGRADED_EVENT_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260527 valve-type migration. Verified the same way
# as the degraded-supply columns — catches a DB whose _schema_version was
# stamped without the migration body running.
_VALVE_TYPE_COLUMNS: frozenset = frozenset({"valve_type"})


def _missing_valve_type_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _VALVE_TYPE_COLUMNS
        if not _has_column(conn, "circuit_profile", col)
    }


def _missing_baseline_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the set of required baseline columns absent from the events table."""
    return {
        col for col in _BASELINE_EVENT_COLUMNS
        if not _has_column(conn, "events", col)
    }


def run_migrations(
    conn: sqlite3.Connection,
    db_path: Optional[Path] = None,
) -> None:
    """
    Enforce baseline schema version. Called once at startup after init_db().

    CRITICAL: tables are already created by database.py before this is called.
    Version 0 is ambiguous — could be fresh DB OR old pre-squash DB without
    _schema_version. Distinguish by checking for ALL required baseline columns:
      - All present  → fresh DB created by current schema → stamp baseline
      - Any absent   → old pre-squash DB → fail fast
    """
    version = _get_version(conn)
    _db_hint = f" DB file: {db_path}" if db_path else ""

    if version == _CURRENT_VERSION:
        missing = (
            _missing_baseline_columns(conn)
            | _missing_degraded_columns(conn)
            | _missing_valve_type_columns(conn)
        )
        if missing:
            raise RuntimeError(
                "Database claims current schema version but is missing required "
                f"columns: {', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )
        log.debug("Database at schema version %d", _CURRENT_VERSION)
        return

    if version == _BASELINE_VERSION:
        # Forward step 1: drop retired text-sensor waveform roles.
        missing = _missing_baseline_columns(conn)
        if missing:
            raise RuntimeError(
                "Database claims baseline schema version but is missing required "
                f"columns: {', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )
        _drop_retired_wf_entity_map_rows(conn)
        # Forward step 2: dedup events and apply the unique index.
        _apply_unique_events_index(conn)
        # Forward step 3: degraded-supply columns + waveform table + rebuild.
        _apply_degraded_supply_columns(conn)
        # Forward step 4: per-circuit valve_type column.
        if version < 20260527:
            _apply_valve_type_column(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded %d → %d", _BASELINE_VERSION, _CURRENT_VERSION)
        return

    if version == _VERSION_PRE_UNIQUE_INDEX:
        _apply_unique_events_index(conn)
        _apply_degraded_supply_columns(conn)
        if version < 20260527:
            _apply_valve_type_column(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded %d → %d",
                 _VERSION_PRE_UNIQUE_INDEX, _CURRENT_VERSION)
        return

    if version == _VERSION_PRE_DEGRADED:
        # DB has the unique index but lacks the degraded-supply columns.
        _apply_degraded_supply_columns(conn)
        if version < 20260527:
            _apply_valve_type_column(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded %d → %d",
                 _VERSION_PRE_DEGRADED, _CURRENT_VERSION)
        return

    if version == 20260526:
        # DB has the degraded-supply migration but lacks valve_type.
        _apply_valve_type_column(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260526 → %d", _CURRENT_VERSION)
        return

    if version == 0:
        # Distinguish fresh DB from pre-squash DB via baseline columns.
        missing = _missing_baseline_columns(conn)
        if missing:
            raise RuntimeError(
                "Existing pre-squash database detected. Missing baseline columns: "
                f"{', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )
        # Fresh DB created by current _create_schema() — has all current
        # columns including the degraded-supply additions. Stamp at current.
        # Defensively verify the degraded columns are present too.
        missing_deg = _missing_degraded_columns(conn)
        if missing_deg:
            raise RuntimeError(
                "Fresh DB missing expected degraded-supply columns: "
                f"{', '.join(sorted(missing_deg))}. "
                f"Schema definition is out of sync.{_db_hint}"
            )
        # The degraded-supply partial index isn't in _create_schema (would
        # fail on existing-DB upgrades — see comment there). Apply the full
        # migration step here too; idempotent and a no-op on empty tables.
        _apply_degraded_supply_columns(conn)
        # Same pattern for valve_type — idempotent, ensures the column and
        # the defensive backfill ran even on fresh DBs.
        _apply_valve_type_column(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("New database — schema version %d applied", _CURRENT_VERSION)
        return

    # Any version 1–31: old incremental migration DB.
    raise RuntimeError(
        f"Database schema version {version} is a pre-squash version. "
        f"Delete the database file and restart the add-on to create a fresh "
        f"schema. (Expected {_CURRENT_VERSION}, found {version}.){_db_hint}"
    )
