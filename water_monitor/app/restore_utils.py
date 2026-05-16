"""Shared restore utilities used by both backup and setup routers.

Extracted here so the two routers cannot drift apart and develop different
normalization behaviour on import.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# Tables that contain a 'circuit' column and require normalization on restore.
# Maps legacy 'main' / 'irrigation' values to stable circuit IDs.
CIRCUIT_TABLES: frozenset[str] = frozenset({
    "events", "circuit_entity_map", "circuit_profile", "training_state",
    "learning_config", "sensitivity_config", "alert_config", "leak_test_schedule",
    "circuit_exclusion_windows", "hourly_volume", "daily_summary",
    "fixtures", "fixture_clusters", "fixture_daily_summary", "leak_test_history",
    "volume_snapshots", "cluster_cooccurrence", "cluster_metrics_history",
    "import_state",
})

# Explicit allowlist of tables that may be restored from a backup file.
# Table names from backup payloads must be validated against this set before
# being interpolated into any SQL string — never trust user-supplied names.
RESTORABLE_TABLES: frozenset[str] = frozenset({
    # quick restore — settings
    "device_config", "circuit_entity_map", "home_profile", "circuit_profile",
    "learning_config", "sensitivity_config", "alert_config", "leak_test_schedule",
    "zone_schedules", "data_retention", "training_state", "fixtures",
    "fixture_signatures", "fixture_clusters", "cluster_cooccurrence",
    "leak_test_history", "threshold_history", "daily_summary",
    "fixture_ha_entity_map", "fixture_daily_summary",
    # quick restore — recent history
    "events", "hourly_volume",
    # history archive
    "zone_flow_history",
})


def normalize_restore_row(row: dict, table: str) -> dict:
    """Normalize the 'circuit' field of a restored row to the stable circuit ID.

    Maps legacy values ('main' → 'circuit_1', 'irrigation' → 'circuit_2').
    Applied to every row in every table that carries a 'circuit' column.
    """
    if table in CIRCUIT_TABLES and "circuit" in row:
        from .circuit_compat import resolve_circuit
        row = dict(row)
        row["circuit"] = resolve_circuit(row["circuit"])
    return row


def restore_circuit_labels(db, payload: dict) -> None:
    """Write circuit display labels from a backup payload into the DB.

    Must be called inside the caller's transaction so the full restore is
    atomic.  If the payload has no 'circuits' key (legacy backup format),
    seeds default labels only when the circuit_labels table is empty.
    """
    from .database import load_circuit_labels, upsert_circuit_label
    circuit_entries = payload.get("circuits", [])
    if circuit_entries:
        for entry in circuit_entries:
            cid   = entry.get("circuit_id", "")
            label = entry.get("display_name", "")
            if cid and label:
                upsert_circuit_label(db, cid, label)
        log.info("Restore: restored %d circuit label(s)", len(circuit_entries))
    else:
        existing = load_circuit_labels(db)
        if not existing:
            upsert_circuit_label(db, "circuit_1", "Main")
            upsert_circuit_label(db, "circuit_2", "Irrigation")
            log.info("Restore: seeded default circuit labels (legacy backup)")


def safe_insert_rows(db, tbl: str, rows: list) -> int:
    """Insert rows into *tbl* with allowlist validation, schema filtering, and
    circuit normalization.

    Safety contract:
    - *tbl* must be in RESTORABLE_TABLES — raises ValueError otherwise.
    - Column names are validated against the live schema via PRAGMA table_info;
      only columns present in both the payload and the live schema are written.
    - If zero columns survive schema filtering a clear ValueError is raised —
      silent skips can make a restore appear successful while dropping data.
    - Does NOT commit; must be called inside the caller's transaction so the
      full restore remains atomic.

    Returns the count of rows inserted.
    """
    if not rows:
        return 0
    if tbl not in RESTORABLE_TABLES:
        raise ValueError(f"Table {tbl!r} is not in the restore allowlist")
    valid_cols = {r[1] for r in db.execute(f"PRAGMA table_info({tbl})").fetchall()}
    cols = [c for c in rows[0].keys() if c in valid_cols]
    if not cols:
        raise ValueError(
            f"Restore error: table '{tbl}' has no valid columns after schema "
            f"filtering — payload columns {list(rows[0].keys())} do not match "
            f"the live schema.  The backup may be from an incompatible version."
        )
    ph  = ",".join("?" for _ in cols)
    cn  = ",".join(cols)
    sql = f"INSERT OR REPLACE INTO {tbl} ({cn}) VALUES ({ph})"
    for row in rows:
        row = normalize_restore_row(row, tbl)
        db.execute(sql, [row.get(c) for c in cols])
    return len(rows)
