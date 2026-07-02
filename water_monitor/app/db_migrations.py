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
#   20260535 — Low-flow dribble exclusion: events.is_low_flow_dribble column
#              (non-zeroing training-exclusion flag for brief low-flow
#              trickles) + two indexes backing the label-training and
#              reclassify-backfill queries. Lightweight DDL only; the verdict
#              backfill runs from the startup / import / manual reprocess paths.
#   20260536 — Active-flow features: flow_integral_litres, active_flow_duration_
#              seconds, true_avg_flow_lpm, flow_on_ratio, active_flow_segment_
#              count, flow_cv_on_segments, integration_quality + the volume audit
#              columns (volume_litres_original, volume_recomputed_at). All
#              NULLABLE. Lightweight DDL only; the per-event recompute runs from
#              the startup / import / manual recompute paths after migration.
#   20260537 — Temporal appliance signal: events.cycle_pulse_count (nullable
#              INTEGER, count of similar-volume neighbours within ±45 min). DDL
#              only; the count backfill + cluster re-suggest run from the startup
#              / manual recompute paths after migration.
#   20260538 — Label provenance: events.fixture_label_source (nullable TEXT,
#              'user'/'cycle'/'training'; NULL = legacy/explicit). DDL only — no
#              backfill (NULL is the correct default for pre-existing labels).
#   20260539 — Training-helper capture (2b): training_capture +
#              training_capture_candidates tables. DDL only.
#   20260540 — Cross-talk event category: events.is_cross_talk +
#              home_profile.hide_cross_talk_events. DDL only; the flag backfill
#              runs from the startup / manual recompute paths after migration.
#   20260541 — Match provenance: events.matched_via (nullable TEXT — 'knn',
#              'washer_cycle', 'rule_toilet', 'rule_dishwasher', 'rule_shower',
#              'zone_default'; NULL = legacy/cluster). DDL only — no backfill
#              (NULL is correct for pre-existing matches; the rules tier and
#              reclassify stamp it going forward).
#   20260542 — dev.24: opt-in water-softener config (home_profile.has_water_softener
#              + softener_regen_start + softener_circuit) and the History cycle-rollup
#              grouping key (events.cycle_group_id). DDL only — no backfill (softener
#              off until enabled; cycle_group_id stamped by the next reclassify).
#   20260543 — Phase 2.3 anomaly response: sensitivity_config.anomaly_response
#              (TEXT DEFAULT 'notify') + sensitivity_config.baseline_anomaly_n
#              (INTEGER — event count behind the frozen percentiles, read by the
#              shut-off confidence gate). DDL only; no backfill (defaults are
#              correct — 'notify', and NULL n until the next activation freeze).
#   20260544 — Phase 3 §2 recorder volume reconciliation: events.volume_recorder_litres
#              (REAL — the firmware cumulative-sensor delta for the event; NULL = not
#              reconciled) + sensitivity_config.recorder_reconcile_auto (INTEGER DEFAULT 1
#              — per-circuit auto-correct vs flag-only toggle). DDL only; no backfill
#              (NULL/default are correct; reconciliation fills them going forward).
#   20260545 — dev.38: guarded auto-split opt-in (home_profile.auto_split_enabled)
#   20260546 — runtime per-circuit flow meter: circuit_profile.pulses_per_litre
#              (REAL DEFAULT 396.0) — add-on cache of the firmware PPL number
#              entity; the low-flow floor is derived (60 ÷ ppl). DDL only; the
#              DEFAULT is correct for existing rows (reference turbine).
#   20260547 — RBAC (viewer/operator/admin): operator_users (operator allow-list),
#              admin_ids_cache (last-known-good HA admin set), seen_users (Access
#              page fallback pick-list). DDL only — CREATE TABLE IF NOT EXISTS,
#              idempotent; no backfill (empty allow-list = everyone non-admin is a
#              viewer until promoted).
#   20260548 — composite labeling: events.embedded_fixtures_json (nullable TEXT —
#              JSON array of fixtures found superimposed on a sustained event's
#              waveform by composite_detector; metadata only, never alters volume
#              or the primary label). DDL only; the annotation backfill runs from
#              the startup / manual reprocess reclassify path after migration.
#   20260549 — dev.39: enable auto-hygiene by default — backfill
#              home_profile.auto_split_enabled = 1 (the background over-merged/inflated
#              event re-import, now safe to run by default: reprocess is atomic +
#              dry-run-gated). Value backfill only; no DDL (column exists since 20260545).
_CURRENT_VERSION: int = 20260550
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


def _apply_low_flow_dribble_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260535 — low-flow dribble exclusion.

    Adds ``events.is_low_flow_dribble`` (a verdict flag for brief low-flow /
    low-volume / near-zero-pressure trickles) plus two indexes that back the
    label-training and reclassify-backfill queries. (The verdict was originally
    non-zeroing; since 2026-06-19 it also zeroes volume_litres_effective — see
    feature_extractor._finalize_derived_verdicts. This DDL is unaffected.)

    LIGHTWEIGHT DDL ONLY. Unlike the phantom migration above, this does NOT
    import feature_extractor or run a data backfill — the per-event verdict is
    backfilled idempotently from the startup / import / manual reprocess paths
    (``reprocess_event_exclusion_verdicts``) AFTER migration completes, so this
    module stays free of heavy app-logic imports (avoids circular imports).

    Idempotent — the column add is guarded by ``_has_column`` and both indexes
    use ``CREATE INDEX IF NOT EXISTS``.
    """
    if not _has_column(conn, "events", "is_low_flow_dribble"):
        conn.execute(
            "ALTER TABLE events "
            "ADD COLUMN is_low_flow_dribble INTEGER NOT NULL DEFAULT 0"
        )
        log.info("Added events.is_low_flow_dribble (default 0)")
    # Indexes live here (not _create_schema) because idx_events_unlabelled_
    # reclassify references matched_fixture_type, which a baseline DB doesn't
    # have until _apply_signature_matcher runs earlier in the same upgrade.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_training_labels "
        "ON events (circuit, user_fixture_type, excluded_from_training)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_unlabelled_reclassify "
        "ON events (circuit, user_fixture_type, matched_fixture_type)"
    )
    conn.commit()
    log.info("Migration 20260535: low-flow dribble column + reclassify indexes ready")


_ACTIVE_FLOW_NEW_COLUMNS: tuple = (
    ("flow_integral_litres", "REAL"),
    ("active_flow_duration_seconds", "REAL"),
    ("true_avg_flow_lpm", "REAL"),
    ("flow_on_ratio", "REAL"),
    ("active_flow_segment_count", "INTEGER"),
    ("flow_cv_on_segments", "REAL"),
    ("integration_quality", "TEXT"),
    ("volume_litres_original", "REAL"),
    ("volume_recomputed_at", "TIMESTAMP"),
)


def _apply_active_flow_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260536 — active-flow features.

    Adds the timestamped-flow-integral feature columns + the volume-recompute
    audit columns, ALL NULLABLE (NULL = not yet backfilled, distinct from 0 =
    known no flow). LIGHTWEIGHT DDL ONLY — the per-event recompute (from raw HA
    flow history) runs from the startup / import / manual recompute paths after
    migration, so this stays free of heavy app-logic imports.

    Idempotent — each add is guarded by ``_has_column``.
    """
    for col, decl in _ACTIVE_FLOW_NEW_COLUMNS:
        if not _has_column(conn, "events", col):
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {decl}")
            log.info("Added events.%s (%s)", col, decl)
    conn.commit()
    log.info("Migration 20260536: active-flow feature columns ready")


_CYCLE_PULSE_NEW_COLUMNS: tuple = (
    ("cycle_pulse_count", "INTEGER"),
)


def _apply_cycle_pulse_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260537 — temporal appliance signal.

    Adds ``events.cycle_pulse_count`` (nullable INTEGER; NULL = not yet computed,
    0 = computed/no qualifying neighbours). LIGHTWEIGHT DDL ONLY — the count
    backfill (``database.recompute_cycle_pulse_counts``) + cluster re-suggest run
    from the startup / manual recompute paths after migration.

    Idempotent — each add is guarded by ``_has_column``.
    """
    for col, decl in _CYCLE_PULSE_NEW_COLUMNS:
        if not _has_column(conn, "events", col):
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {decl}")
            log.info("Added events.%s (%s)", col, decl)
    conn.commit()
    log.info("Migration 20260537: cycle_pulse_count column ready")


_LABEL_SOURCE_NEW_COLUMNS: tuple = (
    ("fixture_label_source", "TEXT"),
)


def _apply_label_source_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260538 — label provenance.

    Adds ``events.fixture_label_source`` (nullable TEXT; 'user'/'cycle'/'training';
    NULL = legacy/explicit). LIGHTWEIGHT DDL ONLY — no backfill (NULL is the correct
    default for pre-existing labels, and is protected like a 'user' label).

    Idempotent — each add is guarded by ``_has_column``.
    """
    for col, decl in _LABEL_SOURCE_NEW_COLUMNS:
        if not _has_column(conn, "events", col):
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {decl}")
            log.info("Added events.%s (%s)", col, decl)
    conn.commit()
    log.info("Migration 20260538: fixture_label_source column ready")


def _apply_training_capture_tables(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260539 — training-helper capture (2b).

    Creates ``training_capture`` + ``training_capture_candidates`` (+ indexes),
    all ``CREATE TABLE/INDEX IF NOT EXISTS`` so it is idempotent. DDL only.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS training_capture (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            circuit         TEXT NOT NULL,
            fixture_type    TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'armed',
            armed_at        TIMESTAMP NOT NULL,
            expires_at      TIMESTAMP NOT NULL,
            window_minutes  INTEGER,
            captured_count  INTEGER NOT NULL DEFAULT 0,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_training_capture_active "
                 "ON training_capture (circuit, status)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS training_capture_candidates (
            capture_id      INTEGER NOT NULL,
            event_id        TEXT NOT NULL,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_training_capture_candidates "
                 "ON training_capture_candidates (capture_id)")
    conn.commit()
    log.info("Migration 20260539: training_capture tables ready")


def _apply_cross_talk_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260540 — cross-talk event category.

    Adds ``events.is_cross_talk`` (INTEGER, default 0 — a long no-flow event whose
    pressure dropped because *another* circuit drew water: registered but no real
    flow through this meter, so it is zeroed + excluded like a phantom) and
    ``home_profile.hide_cross_talk_events`` (the Settings 'hide from History' toggle,
    mirroring ``hide_pressure_artifact_events``). DDL only; idempotent (each add is
    guarded by ``_has_column``). The flag backfill runs from the startup /
    manual-recompute paths.
    """
    if not _has_column(conn, "events", "is_cross_talk"):
        conn.execute(
            "ALTER TABLE events ADD COLUMN is_cross_talk INTEGER NOT NULL DEFAULT 0")
        log.info("Added events.is_cross_talk (INTEGER DEFAULT 0)")
    if not _has_column(conn, "home_profile", "hide_cross_talk_events"):
        conn.execute(
            "ALTER TABLE home_profile ADD COLUMN hide_cross_talk_events "
            "INTEGER NOT NULL DEFAULT 0")
        log.info("Added home_profile.hide_cross_talk_events (default 0)")
    conn.commit()
    log.info("Migration 20260540: cross-talk columns ready")


def _apply_matched_via_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260541 — match provenance (dev.23 rules tier).

    Adds ``events.matched_via`` (TEXT, nullable): how ``matched_fixture_type`` was
    produced — ``'knn'`` (signature k-NN), ``'washer_cycle'`` (anchor + same-peak
    family detector), ``'rule_toilet'``/``'rule_dishwasher'``/``'rule_shower'``
    (structural event rules), ``'zone_default'`` (zone-circuit fallback), or NULL
    (legacy / cluster-tier / user-labeled rows). Machine-derived — NOT in
    ``_EVENT_USER_COLUMNS`` (recomputed by every reclassify). DDL only; idempotent
    (guarded by ``_has_column``); no backfill — NULL is correct for existing rows.
    The live trailing washer scan is served by the existing
    ``idx_events_circuit_start_unique`` (circuit, start_ts) index, so no new index.
    """
    if not _has_column(conn, "events", "matched_via"):
        conn.execute("ALTER TABLE events ADD COLUMN matched_via TEXT")
        log.info("Added events.matched_via (TEXT, nullable)")
    conn.commit()
    log.info("Migration 20260541: matched_via column ready")


def _apply_dev24_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260542 — dev.24.

    Adds the opt-in water-softener config to ``home_profile``:
      • ``has_water_softener``   (INTEGER NOT NULL DEFAULT 0 — the setup opt-in)
      • ``softener_regen_start`` (TEXT, 'HH:MM' local — REQUIRED when enabled)
      • ``softener_circuit``     (TEXT — which circuit the softener is on; Main)
    and the History cycle-rollup grouping key to ``events``:
      • ``cycle_group_id``       (TEXT, nullable — washer anchor id / softener
        session id / dishwasher cycle anchor id; NULL = ungrouped singleton).
    DDL only; idempotent (each add guarded by ``_has_column``). No backfill —
    softener detection is off until enabled, and cycle_group_id is stamped by
    the next reclassify.
    """
    if not _has_column(conn, "home_profile", "has_water_softener"):
        conn.execute("ALTER TABLE home_profile ADD COLUMN has_water_softener "
                     "INTEGER NOT NULL DEFAULT 0")
        log.info("Added home_profile.has_water_softener (default 0)")
    if not _has_column(conn, "home_profile", "softener_regen_start"):
        conn.execute("ALTER TABLE home_profile ADD COLUMN softener_regen_start TEXT")
        log.info("Added home_profile.softener_regen_start (TEXT)")
    if not _has_column(conn, "home_profile", "softener_circuit"):
        conn.execute("ALTER TABLE home_profile ADD COLUMN softener_circuit TEXT")
        log.info("Added home_profile.softener_circuit (TEXT)")
    if not _has_column(conn, "events", "cycle_group_id"):
        conn.execute("ALTER TABLE events ADD COLUMN cycle_group_id TEXT")
        log.info("Added events.cycle_group_id (TEXT, nullable)")
    conn.commit()
    log.info("Migration 20260542: dev.24 columns ready")


def _apply_anomaly_response_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260543 — Phase 2.3 anomaly response.

    Adds two ``sensitivity_config`` columns:
      • ``anomaly_response`` (TEXT DEFAULT 'notify') — the graduated response level
        ('off' | 'notify' | 'notify_shutoff_severe' | 'shutoff_any').
      • ``baseline_anomaly_n`` (INTEGER) — event count behind the frozen volume
        percentiles, read by the shut-off confidence gate so a thin/default
        baseline can never close the valve.
    DDL only; idempotent (each add guarded by ``_has_column``). No backfill —
    'notify' is the correct default and ``baseline_anomaly_n`` is written by the
    next activation freeze (NULL until then → shut-off degrades to notify).
    """
    if not _has_column(conn, "sensitivity_config", "anomaly_response"):
        conn.execute("ALTER TABLE sensitivity_config "
                     "ADD COLUMN anomaly_response TEXT DEFAULT 'notify'")
        log.info("Added sensitivity_config.anomaly_response (default 'notify')")
    if not _has_column(conn, "sensitivity_config", "baseline_anomaly_n"):
        conn.execute("ALTER TABLE sensitivity_config "
                     "ADD COLUMN baseline_anomaly_n INTEGER")
        log.info("Added sensitivity_config.baseline_anomaly_n (INTEGER)")
    conn.commit()
    log.info("Migration 20260543: anomaly-response columns ready")


def _apply_recorder_reconcile_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260544 — Phase 3 §2 recorder volume reconciliation.

    Adds:
      • ``events.volume_recorder_litres`` (REAL) — the authoritative firmware cumulative
        volume-sensor delta for the event's window (NULL = not yet reconciled / sensor
        unavailable). The audit + what flag-mode review/apply uses.
      • ``sensitivity_config.recorder_reconcile_auto`` (INTEGER DEFAULT 1) — per-circuit
        toggle: 1 = auto-correct the volume from the recorder, 0 = flag-only.
    DDL only; idempotent (each add guarded by ``_has_column``). No backfill — NULL /
    default 1 are correct; the hourly reconcile pass fills them going forward.
    """
    if not _has_column(conn, "events", "volume_recorder_litres"):
        conn.execute("ALTER TABLE events ADD COLUMN volume_recorder_litres REAL")
        log.info("Added events.volume_recorder_litres (REAL)")
    if not _has_column(conn, "sensitivity_config", "recorder_reconcile_auto"):
        conn.execute("ALTER TABLE sensitivity_config "
                     "ADD COLUMN recorder_reconcile_auto INTEGER DEFAULT 1")
        log.info("Added sensitivity_config.recorder_reconcile_auto (default 1)")
    conn.commit()
    log.info("Migration 20260544: recorder-reconcile columns ready")


def _apply_dev38_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260545 — dev.38 guarded auto-split.

    Adds the opt-in flag ``home_profile.auto_split_enabled`` (INTEGER NOT NULL
    DEFAULT 0). DDL only; idempotent. OFF until the user enables it — the first
    automated, destructive split pass must be opt-in.
    """
    # home_profile is created by _create_schema before migrations in every real upgrade;
    # guard for minimal synthetic DBs that walk the chain without it.
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='home_profile'"
    ).fetchone()
    if has_table and not _has_column(conn, "home_profile", "auto_split_enabled"):
        conn.execute("ALTER TABLE home_profile ADD COLUMN auto_split_enabled "
                     "INTEGER NOT NULL DEFAULT 0")
        log.info("Added home_profile.auto_split_enabled (default 0)")
    conn.commit()
    log.info("Migration 20260545: dev.38 auto-split flag ready")


def _apply_ppl_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260546 — runtime per-circuit flow meter.

    Adds circuit_profile.pulses_per_litre with DEFAULT 396.0 (reference turbine).
    This column is the add-on's CACHE of the firmware's runtime PPL number entity
    (firmware is the source of truth); the low-flow floor is derived as 60 ÷ ppl.
    Idempotent — column add guarded by _has_column; the DEFAULT gives existing
    rows 396.0 automatically, with a defensive backfill for legacy NULL/0 rows.
    """
    # circuit_profile is created by _create_schema before migrations in every real
    # upgrade; guard for minimal synthetic DBs that walk the chain without it.
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='circuit_profile'"
    ).fetchone()
    if has_table:
        if not _has_column(conn, "circuit_profile", "pulses_per_litre"):
            conn.execute(
                "ALTER TABLE circuit_profile "
                "ADD COLUMN pulses_per_litre REAL DEFAULT 396.0"
            )
            log.info("Added circuit_profile.pulses_per_litre (default 396.0)")
        # Defensive backfill — handles legacy / hand-altered rows.
        conn.execute(
            "UPDATE circuit_profile SET pulses_per_litre = 396.0 "
            "WHERE pulses_per_litre IS NULL OR pulses_per_litre <= 0"
        )
    conn.commit()


def _apply_rbac_tables(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260547 — role-based access (RBAC).

    Creates the three RBAC tables (``operator_users``, ``admin_ids_cache``,
    ``seen_users``). DDL only, all ``CREATE TABLE IF NOT EXISTS`` so it is
    idempotent and a no-op on a fresh DB (where ``_create_schema`` already made
    them). No backfill — an empty operator allow-list means every non-admin HA
    user is a viewer until an admin promotes them on the Access page.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS operator_users (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            added_by      TEXT,
            added_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_ids_cache (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            cached_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_users (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            first_seen    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    log.info("Migration 20260547: RBAC tables ready")


def _apply_embedded_fixtures_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260548 — composite (embedded-fixture) labeling.

    Adds ``events.embedded_fixtures_json`` (nullable TEXT). The reclassify path
    populates it with a JSON array of draws found superimposed on a sustained
    event's stored waveform (a toilet flushed mid-shower). Metadata ONLY — it
    never alters the parent event's volume or primary label. Idempotent — the
    column add is guarded by ``_has_column``; no backfill here (the annotation is
    written by ``recompute_embedded_fixtures`` on the next reclassify pass, which
    needs the event_waveforms rows the bare DDL step doesn't touch).
    """
    if not _has_column(conn, "events", "embedded_fixtures_json"):
        conn.execute(
            "ALTER TABLE events ADD COLUMN embedded_fixtures_json TEXT"
        )
        log.info("Added events.embedded_fixtures_json (TEXT, NULL)")
    conn.commit()
    log.info("Migration 20260548: embedded_fixtures_json column ready")


def _apply_auto_split_default(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260549 — enable auto-hygiene by default.

    Sets ``home_profile.auto_split_enabled = 1`` on existing rows so the background
    over-merged/inflated-event re-import runs without the user flipping a toggle. It is
    safe to default on as of dev.39: the reprocess is ATOMIC (a failed re-import restores
    the deleted events) and dry-run-gated, and user-labelled events are never touched.
    Value backfill only — the column has existed since 20260545; idempotent.
    """
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='home_profile'"
    ).fetchone()
    if has_table and _has_column(conn, "home_profile", "auto_split_enabled"):
        conn.execute(
            "UPDATE home_profile SET auto_split_enabled = 1 "
            "WHERE COALESCE(auto_split_enabled, 0) = 0")
    conn.commit()
    log.info("Migration 20260549: auto-hygiene enabled by default")


def _apply_cross_talk_audit_table(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260550 — irrigation zone-switch cross-talk.

    Creates ``cross_talk_audit`` — the evidence trail the importer's reconciliation
    pass (``historical_importer._reconcile_irrigation_cross_talk``) writes BEFORE it
    zeroes a main event it has identified as irrigation zone-switch cross-talk. One
    row per action records the pre-zero volume + the pressure-swing evidence so a
    false positive is auditable and reversible. DDL is ``CREATE TABLE IF NOT EXISTS``
    so it is idempotent and a no-op on a fresh DB (``_create_schema`` already made it).

    Also flips ``home_profile.hide_cross_talk_events`` on by default (value backfill,
    one-time): the user opted into hiding zone-switch cross-talk from History, and the
    same toggle already governs the long-no-flow cross-talk category. Guarded so it
    only flips an unset (0) value.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cross_talk_audit (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        TEXT NOT NULL,
            circuit         TEXT NOT NULL,
            reconciled_at   TEXT NOT NULL,
            interval_start  TEXT,
            interval_end    TEXT,
            main_delta_psi  REAL,
            other_delta_psi REAL,
            ratio           REAL,
            volume_litres   REAL,
            action          TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cross_talk_audit_event "
        "ON cross_talk_audit(event_id)")
    has_profile = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='home_profile'"
    ).fetchone()
    if has_profile and _has_column(conn, "home_profile", "hide_cross_talk_events"):
        conn.execute(
            "UPDATE home_profile SET hide_cross_talk_events = 1 "
            "WHERE COALESCE(hide_cross_talk_events, 0) = 0")
    conn.commit()
    log.info("Migration 20260550: cross_talk_audit table ready + cross-talk hidden")


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


# Column added by the 20260535 low-flow-dribble migration on events.
_LOW_FLOW_DRIBBLE_COLUMNS: frozenset = frozenset({"is_low_flow_dribble"})


def _missing_low_flow_dribble_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _LOW_FLOW_DRIBBLE_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260536 active-flow migration on events.
_ACTIVE_FLOW_COLUMNS: frozenset = frozenset(c for c, _ in _ACTIVE_FLOW_NEW_COLUMNS)


def _missing_active_flow_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _ACTIVE_FLOW_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260537 cycle-pulse migration on events.
_CYCLE_PULSE_COLUMNS: frozenset = frozenset(c for c, _ in _CYCLE_PULSE_NEW_COLUMNS)


def _missing_cycle_pulse_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _CYCLE_PULSE_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260538 label-provenance migration on events.
_LABEL_SOURCE_COLUMNS: frozenset = frozenset(c for c, _ in _LABEL_SOURCE_NEW_COLUMNS)


def _missing_label_source_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _LABEL_SOURCE_COLUMNS
        if not _has_column(conn, "events", col)
    }


def _missing_training_capture_table(conn: sqlite3.Connection) -> set[str]:
    """Return any of the 20260539 training-capture tables that are absent."""
    needed = {"training_capture", "training_capture_candidates"}
    present = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('training_capture','training_capture_candidates')"
        ).fetchall()
    }
    return needed - present


def _missing_cross_talk_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260540 cross-talk columns that are absent (events + home_profile)."""
    missing: set[str] = set()
    if not _has_column(conn, "events", "is_cross_talk"):
        missing.add("events.is_cross_talk")
    if not _has_column(conn, "home_profile", "hide_cross_talk_events"):
        missing.add("home_profile.hide_cross_talk_events")
    return missing


def _missing_matched_via_column(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260541 provenance column if absent from events."""
    if not _has_column(conn, "events", "matched_via"):
        return {"events.matched_via"}
    return set()


def _missing_dev24_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260542 dev.24 columns that are absent (home_profile + events).

    Spans TWO tables — without the events check a DB missing only
    ``events.cycle_group_id`` would pass and the rollup would fail at runtime.
    """
    missing: set[str] = set()
    for col in ("has_water_softener", "softener_regen_start", "softener_circuit"):
        if not _has_column(conn, "home_profile", col):
            missing.add(f"home_profile.{col}")
    if not _has_column(conn, "events", "cycle_group_id"):
        missing.add("events.cycle_group_id")
    return missing


def _missing_anomaly_response_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260543 anomaly-response columns absent from sensitivity_config."""
    return {
        f"sensitivity_config.{col}"
        for col in ("anomaly_response", "baseline_anomaly_n")
        if not _has_column(conn, "sensitivity_config", col)
    }


def _missing_recorder_reconcile_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260544 recorder-reconcile columns absent (events + sensitivity_config)."""
    missing: set[str] = set()
    if not _has_column(conn, "events", "volume_recorder_litres"):
        missing.add("events.volume_recorder_litres")
    if not _has_column(conn, "sensitivity_config", "recorder_reconcile_auto"):
        missing.add("sensitivity_config.recorder_reconcile_auto")
    return missing


def _missing_dev38_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260545 dev.38 auto-split flag if absent from home_profile."""
    if not _has_column(conn, "home_profile", "auto_split_enabled"):
        return {"home_profile.auto_split_enabled"}
    return set()


def _missing_ppl_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260546 pulses_per_litre column if absent from circuit_profile."""
    if not _has_column(conn, "circuit_profile", "pulses_per_litre"):
        return {"circuit_profile.pulses_per_litre"}
    return set()


def _missing_rbac_tables(conn: sqlite3.Connection) -> set[str]:
    """Return any of the 20260547 RBAC tables that are absent."""
    needed = {"operator_users", "admin_ids_cache", "seen_users"}
    present = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('operator_users','admin_ids_cache','seen_users')"
        ).fetchall()
    }
    return needed - present


def _missing_embedded_fixtures_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260548 embedded_fixtures_json column if absent from events."""
    if not _has_column(conn, "events", "embedded_fixtures_json"):
        return {"events.embedded_fixtures_json"}
    return set()


def _missing_cross_talk_audit_table(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260550 cross_talk_audit table if absent."""
    present = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cross_talk_audit'"
    ).fetchone()
    return set() if present else {"cross_talk_audit"}


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


# ── Ordered forward-migration chain ────────────────────────────────────────────
# (introduced_in_version, apply_fn): a DB stamped at version V already HAS every
# step with introduced <= V and needs exactly the steps with introduced > V,
# applied in this order. Every apply fn is idempotent, so the whole dispatch is
# one loop — adding a migration is ONE line here (plus bumping _CURRENT_VERSION),
# not an edit to ~28 hand-maintained per-version branches (the old ladder, where
# one missed branch stamped a DB current while silently missing a table).
_MIGRATIONS: tuple = (
    (20260524, _drop_retired_wf_entity_map_rows),
    (20260525, _apply_unique_events_index),
    (20260526, _apply_degraded_supply_columns),
    (20260527, _apply_valve_type_column),
    (20260528, _apply_orphan_repair),
    (20260529, _apply_suggestion_source_column),
    (20260530, _apply_signature_matcher),
    (20260531, _apply_fixture_taxonomy_consolidation),
    (20260532, _apply_phantom_event_column),
    (20260533, _apply_category_publish_table),
    (20260534, _apply_manual_classification_columns),
    (20260535, _apply_low_flow_dribble_column),
    (20260536, _apply_active_flow_columns),
    (20260537, _apply_cycle_pulse_column),
    (20260538, _apply_label_source_column),
    (20260539, _apply_training_capture_tables),
    (20260540, _apply_cross_talk_columns),
    (20260541, _apply_matched_via_column),
    (20260542, _apply_dev24_columns),
    (20260543, _apply_anomaly_response_columns),
    (20260544, _apply_recorder_reconcile_columns),
    (20260545, _apply_dev38_columns),
    (20260546, _apply_ppl_column),
    (20260547, _apply_rbac_tables),
    (20260548, _apply_embedded_fixtures_column),
    (20260549, _apply_auto_split_default),
    (20260550, _apply_cross_talk_audit_table),
)

# Versions a DB may legitimately be stamped with and still be upgradeable.
_UPGRADEABLE_VERSIONS: frozenset = frozenset(
    {_BASELINE_VERSION} | {v for v, _ in _MIGRATIONS})


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
            | _missing_low_flow_dribble_columns(conn)
            | _missing_active_flow_columns(conn)
            | _missing_cycle_pulse_columns(conn)
            | _missing_label_source_columns(conn)
            | _missing_training_capture_table(conn)
            | _missing_cross_talk_columns(conn)
            | _missing_matched_via_column(conn)
            | _missing_dev24_columns(conn)
            | _missing_anomaly_response_columns(conn)
            | _missing_recorder_reconcile_columns(conn)
            | _missing_dev38_columns(conn)
            | _missing_ppl_columns(conn)
            | _missing_rbac_tables(conn)
            | _missing_embedded_fixtures_columns(conn)
            | _missing_cross_talk_audit_table(conn)
        )
        if missing:
            raise RuntimeError(
                "Database claims current schema version but is missing required "
                f"columns: {', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )
        log.debug("Database at schema version %d", _CURRENT_VERSION)
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
        # Run the chain anyway (idempotent, near-no-op on an empty DB): a few
        # steps create things _create_schema deliberately omits (partial
        # indexes, one-shot backfills). _apply_unique_events_index is skipped —
        # the fresh schema already ships the unique index and the dedup scan
        # would only rescan an empty table.
        for _v, _fn in _MIGRATIONS:
            if _fn is not _apply_unique_events_index:
                _fn(conn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("New database — schema version %d applied", _CURRENT_VERSION)
        return

    if version not in _UPGRADEABLE_VERSIONS:
        # Any version 1–31 (or an unknown stamp): old incremental pre-squash DB.
        raise RuntimeError(
            f"Database schema version {version} is a pre-squash version. "
            f"Delete the database file and restart the add-on to create a fresh "
            f"schema. (Expected {_CURRENT_VERSION}, found {version}.){_db_hint}"
        )

    if version == _BASELINE_VERSION:
        # Baseline sanity check before the chain runs (the chain itself starts
        # with the drop-retired-waveform-roles step this version predates).
        missing = _missing_baseline_columns(conn)
        if missing:
            raise RuntimeError(
                "Database claims baseline schema version but is missing required "
                f"columns: {', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )

    # Ordered chain: apply exactly the steps this version predates (see
    # _MIGRATIONS — every fn is idempotent, so a re-run after a mid-chain
    # crash is safe).
    steps = [fn for v, fn in _MIGRATIONS if v > version]
    for fn in steps:
        fn(conn)
    _set_version(conn, _CURRENT_VERSION)
    log.info("Database upgraded %d → %d (%d forward step(s))",
             version, _CURRENT_VERSION, len(steps))
