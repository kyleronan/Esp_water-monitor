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
      The column set is the union of keys across all rows so heterogeneous
      payloads don't silently drop fields that appear only in later rows.
    - If zero columns survive schema filtering a clear ValueError is raised —
      silent skips can make a restore appear successful while dropping data.
    - Conflicting rows are upserted via ``ON CONFLICT(<pk>) DO UPDATE``.
      ``INSERT OR REPLACE`` deletes-then-inserts, which fires
      ``ON DELETE CASCADE`` on dependent tables and changes rowids — for
      ``fixtures`` that would drop fixture_signatures / fixture_clusters and
      for ``events`` it would drop fixture_ha_entity_map rows. Upsert avoids
      both. Tables without a PRIMARY KEY fall back to INSERT OR REPLACE (no
      restorable tables hit this today).
    - Does NOT commit; must be called inside the caller's transaction so the
      full restore remains atomic.

    Returns the count of rows inserted.
    """
    if not rows:
        return 0
    if tbl not in RESTORABLE_TABLES:
        raise ValueError(f"Table {tbl!r} is not in the restore allowlist")
    info = db.execute(f"PRAGMA table_info({tbl})").fetchall()
    valid_cols = {r[1] for r in info}
    # PRAGMA table_info: r[5] (pk) is 0 for non-PK, 1..n for ordered PK columns.
    pk_cols = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5] > 0]

    # Union of keys across all rows — late rows with extra columns are honored.
    payload_keys: set = set()
    for r in rows:
        payload_keys.update(r.keys())
    cols = [c for c in payload_keys if c in valid_cols]
    if not cols:
        raise ValueError(
            f"Restore error: table '{tbl}' has no valid columns after schema "
            f"filtering — payload columns {sorted(payload_keys)} do not match "
            f"the live schema.  The backup may be from an incompatible version."
        )
    # Stable column order so generated SQL is deterministic (helps tests / logs).
    cols = sorted(cols)

    ph = ",".join("?" for _ in cols)
    cn = ",".join(cols)

    if pk_cols and all(c in cols for c in pk_cols):
        non_pk = [c for c in cols if c not in pk_cols]
        if non_pk:
            set_clause = ", ".join(f"{c}=excluded.{c}" for c in non_pk)
            conflict = ",".join(pk_cols)
            sql = (
                f"INSERT INTO {tbl} ({cn}) VALUES ({ph}) "
                f"ON CONFLICT({conflict}) DO UPDATE SET {set_clause}"
            )
        else:
            # All payload columns are PK columns — nothing to update on conflict.
            conflict = ",".join(pk_cols)
            sql = (
                f"INSERT INTO {tbl} ({cn}) VALUES ({ph}) "
                f"ON CONFLICT({conflict}) DO NOTHING"
            )
    else:
        # No declared PK (or payload missing PK cols) — fall back to the legacy
        # REPLACE path. None of the current RESTORABLE_TABLES hit this branch.
        sql = f"INSERT OR REPLACE INTO {tbl} ({cn}) VALUES ({ph})"

    for row in rows:
        row = normalize_restore_row(row, tbl)
        db.execute(sql, [row.get(c) for c in cols])
    return len(rows)
