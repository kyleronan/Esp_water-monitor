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
#   20260528 — Sprint A orphan repair: fixtures.cluster_backfill_needed
#              column + one-shot repair of orphaned cluster/fixture refs
#   20260529 — Sprint B label propagation: fixture_clusters.suggestion_source
#              column ('heuristic' | 'user_labels' | NULL)
#   20260530 — Sprint C signature matcher: fixture_type_signatures table +
#              events.matched_fixture_type column
#   20260531 — Sprint D taxonomy consolidation: 23 → 8 fixture types;
#              rewrites stored type strings in events, fixtures,
#              fixture_clusters, and clears fixture_type_signatures
#   20260532 — Sprint E pressure-restoration phantom guard:
#              events.is_pressure_restoration_phantom +
#              home_profile.hide_pressure_artifact_events columns; one-shot
#              reprocess zeros phantom volume + reverses hourly_volume
#   20260533 — Sprint F per-category Fixtures rollup: new category_publish
#              table (per-(circuit, fixture_type) HA publish gate). Seeded
#              from MIN(fixtures.publish_to_ha) so any existing off
#              preference carries over to the new category-level gate.
#   20260534 — Sprint H phantom misclassification fix + manual classification:
#              events.user_ignored + events.user_classified columns; one-shot
#              repair un-flags wrongly-flagged phantoms (delta>=2.0) and
#              restores their real volume to hourly_volume + daily_summary.
_CURRENT_VERSION: int = 20260534
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


def _apply_signature_matcher(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260530 — Sprint C signature matcher.

    Adds two artefacts:

      1. ``fixture_type_signatures`` table (per-circuit, per-fixture-type
         centroid learned from user-labelled events). The legacy
         ``fixture_signatures`` table (per-fixture, per-feature) was never
         populated by any code path; it's left in place for backup-restore
         compat but the matcher reads from the new table.

      2. ``events.matched_fixture_type`` column — populated when the
         signature matcher tags an event with a fixture_type, independent
         of cluster_id.

    Idempotent — both creates are guarded.
    """
    conn.execute(
        """CREATE TABLE IF NOT EXISTS fixture_type_signatures (
            circuit       TEXT NOT NULL,
            fixture_type  TEXT NOT NULL,
            centroid      TEXT NOT NULL DEFAULT '{}',
            member_count  INTEGER NOT NULL DEFAULT 0,
            created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (circuit, fixture_type)
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_type_signatures_circuit "
        "ON fixture_type_signatures (circuit)"
    )
    if not _has_column(conn, "events", "matched_fixture_type"):
        conn.execute(
            "ALTER TABLE events ADD COLUMN matched_fixture_type TEXT"
        )
        log.info("Added events.matched_fixture_type (TEXT, NULL)")
    conn.commit()
    log.info("Migration 20260530: signature-matcher infrastructure ready")


def _apply_fixture_taxonomy_consolidation(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260531 — Sprint D taxonomy consolidation.

    Collapses the old 23-entry fixture type list down to 8 coarse types.
    Applies LEGACY_TYPE_REMAP (from fixtures.py) to every stored type string
    in four columns, then clears fixture_type_signatures so that centroids
    are rebuilt against the new type names on the next user label save.

    This is a pure data migration — no schema changes. Idempotent: old type
    strings no longer appear after the first run, so subsequent UPDATEs
    affect zero rows.
    """
    from .fixtures import LEGACY_TYPE_REMAP

    cols_tables = [
        ("events",           "user_fixture_type"),
        ("events",           "matched_fixture_type"),
        ("fixtures",         "fixture_type"),
        ("fixture_clusters", "suggested_type"),
    ]

    total_updated = 0
    for old_type, new_type in LEGACY_TYPE_REMAP.items():
        if old_type == new_type:
            continue  # nothing to rewrite
        for table, col in cols_tables:
            cur = conn.execute(
                f"UPDATE {table} SET {col} = ? WHERE {col} = ?",
                (new_type, old_type),
            )
            total_updated += cur.rowcount

    conn.commit()

    # Clear signatures — centroids were computed against old type strings
    # and will be rebuilt on the next label save or on demand.
    cur = conn.execute("DELETE FROM fixture_type_signatures")
    sig_count = cur.rowcount
    conn.commit()

    log.info(
        "Migration 20260531: taxonomy consolidation rewrote %d stored type "
        "value(s) across 4 columns; cleared %d fixture_type_signatures row(s)",
        total_updated, sig_count,
    )


def _apply_phantom_event_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260532 — Sprint E phantom guard.

    Adds two columns:
      1. ``events.is_pressure_restoration_phantom`` — flags events matching
         the long-duration + near-zero-pressure-drop fingerprint of a city-
         pressure-restoration artifact.
      2. ``home_profile.hide_pressure_artifact_events`` — backs the Settings
         toggle that hides flagged events from the History list.

    After the column adds, runs ``reprocess_pressure_restoration_phantoms``
    to retroactively flag existing events and reverse their hourly_volume
    contributions so historical daily totals shed the false volume.

    Idempotent — both column adds are guarded by ``_has_column``; the
    reprocess helper skips already-flagged events.
    """
    if not _has_column(conn, "events", "is_pressure_restoration_phantom"):
        conn.execute(
            "ALTER TABLE events "
            "ADD COLUMN is_pressure_restoration_phantom INTEGER DEFAULT 0"
        )
        log.info("Added events.is_pressure_restoration_phantom (default 0)")
    if not _has_column(conn, "home_profile", "hide_pressure_artifact_events"):
        conn.execute(
            "ALTER TABLE home_profile "
            "ADD COLUMN hide_pressure_artifact_events INTEGER NOT NULL DEFAULT 0"
        )
        log.info("Added home_profile.hide_pressure_artifact_events (default 0)")
    conn.commit()

    # Lazy import — keeps this module importable without feature_extractor
    # side effects during test collection.
    from .feature_extractor import reprocess_pressure_restoration_phantoms
    result = reprocess_pressure_restoration_phantoms(conn)
    log.info(
        "Migration 20260532: phantom guard ready; flagged %d existing event(s)",
        result.get("flagged", 0),
    )


def _apply_category_publish_table(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260533 — Sprint F category rollup.

    Creates the ``category_publish`` table (per-(circuit, fixture_type) HA
    publish gate) and seeds it from existing confirmed fixtures so any
    previously-disabled HA entity stays disabled under the new model.

    Seeding rule: ``publish_to_ha = MIN(fixtures.publish_to_ha)`` across each
    (circuit, fixture_type). MIN, not MAX, so if the user previously
    silenced ANY fixture in a category, the new category gate starts off —
    we never surprise-republish an HA entity the user had disabled.

    Idempotent — CREATE IF NOT EXISTS guards the table; INSERT OR IGNORE
    guards the seed (subsequent runs leave existing rows alone).
    """
    conn.execute(
        """CREATE TABLE IF NOT EXISTS category_publish (
            circuit         TEXT NOT NULL,
            fixture_type    TEXT NOT NULL,
            publish_to_ha   INTEGER NOT NULL DEFAULT 1,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (circuit, fixture_type)
        )"""
    )
    conn.commit()

    # Seed from confirmed fixtures' per-fixture publish flags. MIN preserves
    # any explicit user "off" preference at the category level.
    cur = conn.execute(
        "INSERT OR IGNORE INTO category_publish "
        "  (circuit, fixture_type, publish_to_ha) "
        "SELECT circuit, fixture_type, COALESCE(MIN(publish_to_ha), 1) "
        "FROM fixtures "
        "WHERE confirmed = 1 "
        "  AND fixture_type IS NOT NULL "
        "  AND fixture_type != '' "
        "GROUP BY circuit, fixture_type"
    )
    seeded = cur.rowcount
    conn.commit()
    log.info(
        "Migration 20260533: category_publish table ready; seeded %d "
        "(circuit, fixture_type) row(s) from existing fixtures.publish_to_ha (MIN)",
        seeded,
    )


def _apply_manual_classification_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260534 — Sprint H.

    Adds ``events.user_ignored`` (explicit Ignore/Restore intent, split out of
    the now-derived ``excluded_from_training``) and ``events.user_classified``
    (lock bit guarding a manual classification). Then runs a one-shot repair
    of phantom misclassifications: a real event that got a stale
    ``is_pressure_restoration_phantom=1`` despite ``pressure_delta_psi >= 2.0``
    (e.g. a long shower flagged before its ESP-waveform pressure landed) is
    un-flagged and its real volume restored to hourly_volume + daily_summary.

    Idempotent — column adds guarded by ``_has_column``; the repair skips rows
    that are already consistent and rows the user has manually classified.
    """
    if not _has_column(conn, "events", "user_ignored"):
        conn.execute("ALTER TABLE events ADD COLUMN user_ignored INTEGER DEFAULT 0")
        log.info("Added events.user_ignored (default 0)")
        # Backfill the explicit Ignore intent from the legacy combined column:
        # rows excluded with no auto reason were excluded by a user Ignore.
        conn.execute(
            "UPDATE events SET user_ignored = 1 "
            "WHERE excluded_from_training = 1 "
            "  AND COALESCE(is_composite, 0) = 0 "
            "  AND COALESCE(degraded_supply, 0) = 0 "
            "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
            "  AND (match_rejection_reason IS NULL "
            "       OR match_rejection_reason = 'excluded_from_training')"
        )
    if not _has_column(conn, "events", "user_classified"):
        conn.execute("ALTER TABLE events ADD COLUMN user_classified INTEGER DEFAULT 0")
        log.info("Added events.user_classified (default 0)")
    conn.commit()

    from .database import repair_misflagged_phantom_events
    result = repair_misflagged_phantom_events(conn)
    log.info(
        "Migration 20260534: manual-classification columns ready; repaired %d "
        "misflagged phantom event(s), restored %.1f L to totals",
        result.get("repaired", 0), result.get("litres_restored", 0.0),
    )


def _apply_suggestion_source_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260529 — Sprint B label propagation.

    Adds ``fixture_clusters.suggestion_source`` (TEXT, nullable). Values:
    ``NULL`` (no suggestion yet), ``'heuristic'`` (set by the centroid
    feature-range rules in cluster_engine), ``'user_labels'`` (set by the
    majority-vote helper in ``database.recompute_cluster_suggestion_from_user_labels``).

    Backfill: clusters that already had a non-NULL ``suggested_type`` get
    ``suggestion_source = 'heuristic'`` — historically that's the only
    code path that could have set it. The new majority-vote helper hasn't
    run yet, so we know nothing in the DB is from user labels.

    Idempotent — column add is guarded by ``_has_column``; the backfill
    only touches rows where ``suggestion_source IS NULL``.
    """
    if not _has_column(conn, "fixture_clusters", "suggestion_source"):
        conn.execute(
            "ALTER TABLE fixture_clusters ADD COLUMN suggestion_source TEXT"
        )
        log.info("Added fixture_clusters.suggestion_source (TEXT, NULL)")
    conn.execute(
        "UPDATE fixture_clusters SET suggestion_source = 'heuristic' "
        "WHERE suggestion_source IS NULL AND suggested_type IS NOT NULL"
    )
    conn.commit()
    log.info("Migration 20260529: suggestion_source column added + backfilled")


def _apply_orphan_repair(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260528 — Sprint A orphan repair.

    Adds ``fixtures.cluster_backfill_needed`` (INTEGER DEFAULT 0) and
    runs a one-shot pass that:

      1. NULLs ``events.cluster_id`` where the referenced cluster row
         no longer exists (so the next backfill pass re-clusters them).
      2. Flags ``fixtures.cluster_backfill_needed = 1`` for confirmed
         fixtures that have no cluster pointing back at them — surfaces
         the relink banner on the Fixtures page.
      3. NULLs ``fixture_clusters.fixture_id`` where the referenced
         fixture row no longer exists.

    Idempotent — column add is guarded by ``_has_column``; the repair
    helper itself yields zero counts on a second invocation.
    """
    if not _has_column(conn, "fixtures", "cluster_backfill_needed"):
        conn.execute(
            "ALTER TABLE fixtures "
            "ADD COLUMN cluster_backfill_needed INTEGER DEFAULT 0"
        )
        log.info("Added fixtures.cluster_backfill_needed (default 0)")
    conn.commit()

    # Lazy import — keeps this module importable without database.py
    # side effects during test collection.
    from .database import find_orphaned_cluster_references
    counts = find_orphaned_cluster_references(conn, repair=True)
    total = sum(counts.values())
    if total:
        log.info(
            "Migration 20260528: orphan-repair fixed %d event(s), flagged "
            "%d unbacked fixture(s), nulled %d dangling cluster fixture_id(s)",
            counts["events_orphaned"],
            counts["fixtures_unbacked"],
            counts["clusters_dangling"],
        )
    else:
        log.info("Migration 20260528: orphan-repair found nothing to fix")


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


# Columns added by the 20260528 orphan-repair migration. Same verification
# pattern as above — catches a DB whose _schema_version was stamped without
# the migration body running.
_ORPHAN_REPAIR_COLUMNS: frozenset = frozenset({"cluster_backfill_needed"})


def _missing_orphan_repair_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _ORPHAN_REPAIR_COLUMNS
        if not _has_column(conn, "fixtures", col)
    }


# Columns added by the 20260529 suggestion-source migration (Sprint B).
_SUGGESTION_SOURCE_COLUMNS: frozenset = frozenset({"suggestion_source"})


def _missing_suggestion_source_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _SUGGESTION_SOURCE_COLUMNS
        if not _has_column(conn, "fixture_clusters", col)
    }


# Columns added by the 20260530 signature-matcher migration (Sprint C).
_SIGNATURE_MATCHER_COLUMNS: frozenset = frozenset({"matched_fixture_type"})


def _missing_signature_matcher_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _SIGNATURE_MATCHER_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Column added by the 20260532 phantom-guard migration (Sprint E) on events.
_PHANTOM_COLUMNS: frozenset = frozenset({"is_pressure_restoration_phantom"})


def _missing_phantom_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _PHANTOM_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Table added by the 20260533 category-publish migration (Sprint F).
# A "missing column" here is actually a missing TABLE check — the verifier
# treats the table's absence as a single missing-column-equivalent entry.
def _missing_category_publish_columns(conn: sqlite3.Connection) -> set[str]:
    row = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name = 'category_publish'"
    ).fetchone()
    return set() if row else {"category_publish"}


# Columns added by the 20260534 manual-classification migration (Sprint H).
_MANUAL_CLASSIFICATION_COLUMNS: frozenset = frozenset({"user_ignored", "user_classified"})


def _missing_manual_classification_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _MANUAL_CLASSIFICATION_COLUMNS
        if not _has_column(conn, "events", col)
    }


def _missing_baseline_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the set of required baseline columns absent from the events table."""
    return {
        col for col in _BASELINE_EVENT_COLUMNS
        if not _has_column(conn, "events", col)
    }


def _log_schema_state(conn: sqlite3.Connection) -> None:
    """Emit a single INFO line summarising the current schema.

    Plan C-IQ-15 / C-IQ-22 (lightweight variant). Walks `sqlite_master`
    for user tables and reports each table's column count alongside
    the stamped schema version. A divergent DB (e.g. a partially
    restored backup, or a hand-edited database) will be loud in the
    logs without forcing a hard-fail boot abort — which the plan
    downgraded over dev-time false-alarm risk.

    Format chosen so the line is greppable but compact:
        Schema v=20260527  tables: events(56), fixtures(11), ...

    Best-effort: any SQL error here is swallowed so a deeply broken DB
    doesn't keep the addon from starting in the diagnose-and-restore
    path. The migration verification block above is the real guard.
    """
    try:
        version = _get_version(conn)
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' "
            "  AND name NOT LIKE 'sqlite_%' "
            "  AND name NOT LIKE '_schema_version' "
            "ORDER BY name"
        ).fetchall()
        parts = []
        for r in rows:
            tbl = r[0]
            try:
                cols = conn.execute(
                    f"PRAGMA table_info({tbl})"
                ).fetchall()
                parts.append(f"{tbl}({len(cols)})")
            except sqlite3.OperationalError:
                # Table dropped between SELECT and PRAGMA — rare.
                parts.append(f"{tbl}(?)")
        log.info(
            "Schema v=%d  tables: %s",
            version, ", ".join(parts) or "(none)",
        )
    except Exception as exc:
        # Schema diagnostic must never fail the boot. Log the error
        # itself at INFO so a developer running locally can spot it.
        log.info("Schema diagnostic failed (non-fatal): %s", exc)


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

    Always emits the schema-state diagnostic line at the end (via the
    `finally` below), even when migration aborts with a RuntimeError —
    that way the supervisor logs show exactly what tables/columns the
    on-disk DB had at the moment things went wrong.
    """
    try:
        _run_migrations_impl(conn, db_path)
    finally:
        _log_schema_state(conn)


def _run_migrations_impl(
    conn: sqlite3.Connection,
    db_path: Optional[Path] = None,
) -> None:
    """Actual migration dispatch. Kept separate so run_migrations can
    log the schema state unconditionally via try/finally."""
    version = _get_version(conn)
    _db_hint = f" DB file: {db_path}" if db_path else ""

    if version == _CURRENT_VERSION:
        missing = (
            _missing_baseline_columns(conn)
            | _missing_degraded_columns(conn)
            | _missing_valve_type_columns(conn)
            | _missing_orphan_repair_columns(conn)
            | _missing_suggestion_source_columns(conn)
            | _missing_signature_matcher_columns(conn)
            | _missing_phantom_columns(conn)
            | _missing_category_publish_columns(conn)
            | _missing_manual_classification_columns(conn)
        )
        if missing:
            raise RuntimeError(
                "Database claims current schema version but is missing required "
                f"columns: {', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )
        log.debug("Database at schema version %d", _CURRENT_VERSION)
        return

    if version == 20260533:
        # DB has category_publish but lacks the manual-classification columns
        # + the phantom-misflag repair.
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260533 → %d", _CURRENT_VERSION)
        return

    if version == 20260532:
        # DB has the phantom guard but lacks category_publish + manual class.
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260532 → %d", _CURRENT_VERSION)
        return

    if version == 20260531:
        # DB has the taxonomy consolidation but lacks the phantom guard +
        # category_publish + manual class.
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260531 → %d", _CURRENT_VERSION)
        return

    if version == 20260530:
        # DB has signature-matcher infrastructure but hasn't had the taxonomy
        # consolidation, phantom guard, category_publish, or manual class.
        _apply_fixture_taxonomy_consolidation(conn)
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260530 → %d", _CURRENT_VERSION)
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
        _apply_valve_type_column(conn)
        # Forward step 5: orphan repair.
        _apply_orphan_repair(conn)
        # Forward step 6: suggestion_source column.
        _apply_suggestion_source_column(conn)
        # Forward step 7: signature-matcher table + column.
        _apply_signature_matcher(conn)
        # Forward step 8: taxonomy consolidation (23 → 8 types).
        _apply_fixture_taxonomy_consolidation(conn)
        # Forward step 9: phantom guard columns + reprocess.
        _apply_phantom_event_column(conn)
        # Forward step 10: category_publish table + seed.
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded %d → %d", _BASELINE_VERSION, _CURRENT_VERSION)
        return

    if version == _VERSION_PRE_UNIQUE_INDEX:
        _apply_unique_events_index(conn)
        _apply_degraded_supply_columns(conn)
        _apply_valve_type_column(conn)
        _apply_orphan_repair(conn)
        _apply_suggestion_source_column(conn)
        _apply_signature_matcher(conn)
        _apply_fixture_taxonomy_consolidation(conn)
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded %d → %d",
                 _VERSION_PRE_UNIQUE_INDEX, _CURRENT_VERSION)
        return

    if version == _VERSION_PRE_DEGRADED:
        # DB has the unique index but lacks the degraded-supply columns.
        _apply_degraded_supply_columns(conn)
        _apply_valve_type_column(conn)
        _apply_orphan_repair(conn)
        _apply_suggestion_source_column(conn)
        _apply_signature_matcher(conn)
        _apply_fixture_taxonomy_consolidation(conn)
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded %d → %d",
                 _VERSION_PRE_DEGRADED, _CURRENT_VERSION)
        return

    if version == 20260526:
        # DB has the degraded-supply migration but lacks valve_type.
        _apply_valve_type_column(conn)
        _apply_orphan_repair(conn)
        _apply_suggestion_source_column(conn)
        _apply_signature_matcher(conn)
        _apply_fixture_taxonomy_consolidation(conn)
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260526 → %d", _CURRENT_VERSION)
        return

    if version == 20260527:
        # DB has the valve_type column but lacks the orphan-repair column
        # and hasn't run the one-shot orphan cleanup yet.
        _apply_orphan_repair(conn)
        _apply_suggestion_source_column(conn)
        _apply_signature_matcher(conn)
        _apply_fixture_taxonomy_consolidation(conn)
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260527 → %d", _CURRENT_VERSION)
        return

    if version == 20260528:
        # DB has the orphan-repair column but lacks suggestion_source.
        _apply_suggestion_source_column(conn)
        _apply_signature_matcher(conn)
        _apply_fixture_taxonomy_consolidation(conn)
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260528 → %d", _CURRENT_VERSION)
        return

    if version == 20260529:
        # DB has suggestion_source but lacks the signature-matcher
        # infrastructure (table + matched_fixture_type column).
        _apply_signature_matcher(conn)
        _apply_fixture_taxonomy_consolidation(conn)
        _apply_phantom_event_column(conn)
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("Database upgraded 20260529 → %d", _CURRENT_VERSION)
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
        # Same pattern for orphan-repair — column add is guarded, and the
        # repair scan finds nothing on an empty DB.
        _apply_orphan_repair(conn)
        # Same pattern for suggestion_source — column add guarded, backfill
        # only touches non-NULL suggestion rows.
        _apply_suggestion_source_column(conn)
        # Same pattern for signature-matcher — CREATE TABLE IF NOT EXISTS
        # and the column-add guard mean this is a no-op on fresh DBs.
        _apply_signature_matcher(conn)
        # Taxonomy consolidation is a data-only pass; no-op on empty DBs.
        _apply_fixture_taxonomy_consolidation(conn)
        # Phantom guard — columns guarded by _has_column; reprocess scan
        # finds nothing on an empty DB.
        _apply_phantom_event_column(conn)
        # Category publish table — CREATE IF NOT EXISTS, seed pulls from
        # empty fixtures table on a fresh DB so no rows are inserted.
        _apply_category_publish_table(conn)
        _apply_manual_classification_columns(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("New database — schema version %d applied", _CURRENT_VERSION)
        return

    # Any version 1–31: old incremental migration DB.
    raise RuntimeError(
        f"Database schema version {version} is a pre-squash version. "
        f"Delete the database file and restart the add-on to create a fresh "
        f"schema. (Expected {_CURRENT_VERSION}, found {version}.){_db_hint}"
    )
