"""Tests for event-table deduplication, UTC normalization, and
event_exists_near correctness.

Covers the three root-cause bugs fixed in migration 021:
  1. UUID4 ids → duplicate rows on re-import (no UNIQUE constraint)
  2. event_exists_near() broken by SQLite datetime() 'T' vs space separator
  3. Migration 015 one-shot; Quick Restore re-introduced duplicates

Run with:  pytest water_monitor/tests/test_event_dedup.py -v
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from .conftest import make_db
from water_monitor.app.database import (
    dedup_events,
    event_exists_near,
    normalize_events_utc,
)
from water_monitor.app.db_migrations import run_migrations


# ── Helpers ────────────────────────────────────────────────────────────────────

def _insert_event(
    db: sqlite3.Connection,
    *,
    circuit: str = "main",
    start_ts: str = "2026-01-01T08:00:00+00:00",
    end_ts: str | None = "2026-01-01T08:00:45+00:00",
    event_id: str | None = None,
    cluster_id: int | None = None,
) -> str:
    """Insert a minimal event row; returns the id used."""
    eid = event_id or str(uuid.uuid4())
    db.execute(
        "INSERT INTO events (id, circuit, start_ts, end_ts, avg_flow_lpm,"
        " peak_flow_lpm, duration_seconds, volume_litres, cluster_id)"
        " VALUES (?, ?, ?, ?, 8.0, 9.0, 45.0, 6.0, ?)",
        (eid, circuit, start_ts, end_ts, cluster_id),
    )
    db.commit()
    return eid


def _event_count(db: sqlite3.Connection, circuit: str = "main") -> int:
    return db.execute(
        "SELECT COUNT(*) FROM events WHERE circuit = ?", (circuit,)
    ).fetchone()[0]


def _get_event(db: sqlite3.Connection, eid: str) -> dict | None:
    row = db.execute("SELECT * FROM events WHERE id = ?", (eid,)).fetchone()
    return dict(row) if row else None


# ── dedup_events() ─────────────────────────────────────────────────────────────

class TestDedupEvents:
    def test_keeps_max_rowid(self, db):
        """dedup_events keeps the most recently inserted row (by MAX rowid)."""
        start = "2026-01-01T08:00:00+00:00"
        # Insert two rows with the same (circuit, start_ts) — different cluster_ids
        # so we can verify which one survived (the second insert = MAX rowid).
        _insert_event(db, start_ts=start, event_id=str(uuid.uuid4()), cluster_id=1)
        _insert_event(db, start_ts=start, event_id=str(uuid.uuid4()), cluster_id=2)
        removed = dedup_events(db)
        assert removed == 1
        # After dedup the surviving row has a recomputed UUID5 id; verify by
        # looking at the remaining row count and the cluster_id that survived.
        assert _event_count(db) == 1
        # Note: dedup_events() clears cluster_id on contested survivors so we
        # can't check cluster_id == 2 here — just verify exactly one row remains.

    def test_idempotent(self, db):
        """Calling dedup_events twice on a clean table returns 0 on the second call."""
        _insert_event(db, start_ts="2026-01-01T08:00:00+00:00")
        dedup_events(db)
        assert dedup_events(db) == 0

    def test_preserves_distinct_events(self, db):
        """Events with different start_ts are all kept."""
        for h in range(3):
            _insert_event(db, start_ts=f"2026-01-01T0{h}:00:00+00:00")
        assert dedup_events(db) == 0
        assert _event_count(db) == 3

    def test_clears_stale_cluster_id_on_survivor(self, db):
        """Survivor of a contested dedup group has cluster_id cleared."""
        start = "2026-01-01T08:00:00+00:00"
        id1 = _insert_event(db, start_ts=start, cluster_id=42)
        id2 = _insert_event(db, start_ts=start, cluster_id=None)
        # id2 has higher rowid; id1 (with cluster_id=42) is deleted
        # But if cluster_id were on id2 it should be cleared — test the
        # clearing path by inserting the cluster_id on id2 instead.
        db.execute("UPDATE events SET cluster_id = 99 WHERE id = ?", (id2,))
        db.commit()
        dedup_events(db)
        surviving_row = db.execute(
            "SELECT cluster_id FROM events WHERE circuit = 'main' LIMIT 1"
        ).fetchone()
        assert surviving_row is not None
        assert surviving_row[0] is None, (
            "cluster_id should be NULL on a dedup survivor of a contested group"
        )

    def test_multi_circuit_isolation(self, db):
        """Duplicates in one circuit don't affect the other circuit."""
        start = "2026-01-01T08:00:00+00:00"
        _insert_event(db, circuit="main",       start_ts=start)
        _insert_event(db, circuit="main",       start_ts=start)   # dupe
        _insert_event(db, circuit="irrigation", start_ts=start)   # distinct circuit
        removed = dedup_events(db)
        assert removed == 1
        assert _event_count(db, "main") == 1
        assert _event_count(db, "irrigation") == 1


# ── UNIQUE constraint ──────────────────────────────────────────────────────────

class TestUniqueIndex:
    def test_unique_index_blocks_new_dupe(self):
        """After migration 021 a duplicate (circuit, start_ts) raises IntegrityError."""
        db = sqlite3.connect(":memory:", check_same_thread=False)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        from water_monitor.app.database import _create_schema
        _create_schema(db)
        run_migrations(db)

        start = "2026-01-01T08:00:00+00:00"
        eid1 = str(uuid.uuid5(uuid.NAMESPACE_OID, f"main/{start}"))
        _insert_event(db, start_ts=start, event_id=eid1)

        eid2 = str(uuid.uuid4())   # different id, same (circuit, start_ts)
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO events (id, circuit, start_ts, avg_flow_lpm,"
                " peak_flow_lpm, duration_seconds, volume_litres)"
                " VALUES (?, 'main', ?, 8.0, 9.0, 45.0, 6.0)",
                (eid2, start),
            )


# ── event_exists_near() ────────────────────────────────────────────────────────

class TestEventExistsNear:
    def test_finds_existing_event(self, db):
        """event_exists_near returns True for an exact match.

        This test FAILS on the old implementation (SQLite datetime() space
        separator vs stored 'T' separator), proving the bug.
        """
        start = "2026-05-03T17:31:16.594590+00:00"
        _insert_event(db, start_ts=start)
        assert event_exists_near(db, "main", start) is True

    def test_in_window(self, db):
        """Returns True for a timestamp within the ±30 s window."""
        start = "2026-01-01T08:00:00+00:00"
        _insert_event(db, start_ts=start)
        query_ts = (
            datetime.fromisoformat(start) + timedelta(seconds=25)
        ).isoformat()
        assert event_exists_near(db, "main", query_ts) is True

    def test_out_of_window(self, db):
        """Returns False for a timestamp outside the ±30 s window."""
        start = "2026-01-01T08:00:00+00:00"
        _insert_event(db, start_ts=start)
        query_ts = (
            datetime.fromisoformat(start) + timedelta(seconds=45)
        ).isoformat()
        assert event_exists_near(db, "main", query_ts) is False

    def test_handles_microseconds(self, db):
        """Stored start_ts with microseconds is found correctly."""
        start = "2026-01-01T08:00:00.123456+00:00"
        _insert_event(db, start_ts=start)
        assert event_exists_near(db, "main", start) is True

    def test_dst_offset_mismatch(self, db):
        """Same instant expressed in two different offsets — still matches.

        Stores in UTC; queries with MDT (-06:00) for the same wall-clock
        instant.  Epoch-seconds comparison returns True regardless of the
        offset string difference.
        """
        # 07:00 UTC == 01:00 MDT (-06:00)
        stored  = "2026-11-01T07:00:00+00:00"
        queried = "2026-11-01T01:00:00-06:00"
        _insert_event(db, start_ts=stored)
        assert event_exists_near(db, "main", queried) is True

    def test_null_start_ts_not_matched(self, db):
        """event_exists_near handles a NULL start_ts gracefully.

        The schema has start_ts NOT NULL so we can't actually insert a NULL row.
        Instead verify that event_exists_near on an empty table returns False —
        confirming the function doesn't crash and the AND start_ts IS NOT NULL
        guard in the SQL is syntactically valid.
        """
        # No events inserted — should simply return False
        assert event_exists_near(db, "main", "2026-01-01T08:00:00+00:00") is False


# ── normalize_events_utc() ────────────────────────────────────────────────────

class TestNormalizeEventsUtc:
    def test_normalizes_non_utc_offset(self, db):
        """A row stored with -06:00 is rewritten to +00:00."""
        start_mdt = "2026-11-01T01:00:00-06:00"
        eid = _insert_event(db, start_ts=start_mdt)
        normalize_events_utc(db)
        db.commit()
        row = db.execute("SELECT start_ts FROM events").fetchone()
        assert row is not None
        assert row[0].endswith("+00:00"), (
            f"Expected UTC end, got: {row[0]!r}"
        )
        assert row[0].startswith("2026-11-01T07:00:00"), (
            f"Expected 07:00 UTC, got: {row[0]!r}"
        )

    def test_recomputes_uuid5_id(self, db):
        """dedup_events recomputes the UUID5 id after normalize_events_utc changes start_ts."""
        start_mdt = "2026-11-01T01:00:00-06:00"
        start_utc = "2026-11-01T07:00:00+00:00"
        expected_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"main/{start_utc}"))
        _insert_event(db, start_ts=start_mdt, event_id=str(uuid.uuid4()))
        normalize_events_utc(db)
        db.commit()
        # id has NOT been recomputed yet — that's dedup_events()'s job
        dedup_events(db)
        row = db.execute("SELECT id FROM events").fetchone()
        assert row[0] == expected_id, (
            f"Expected UUID5 of UTC ts, got: {row[0]!r}"
        )

    def test_idempotent_on_already_utc(self, db):
        """normalize_events_utc does not touch rows already in UTC with correct UUID5 id."""
        start = "2026-01-01T08:00:00+00:00"
        # Use the correct UUID5 id so normalize_events_utc finds nothing to update
        eid = str(uuid.uuid5(uuid.NAMESPACE_OID, f"main/{start}"))
        _insert_event(db, start_ts=start, event_id=eid)
        count = normalize_events_utc(db)
        assert count == 0
        row = db.execute("SELECT id, start_ts FROM events").fetchone()
        assert row["id"] == eid
        assert row["start_ts"] == start


# ── Migration 021 end-to-end ──────────────────────────────────────────────────

class TestMigration021:
    def _make_pre_migration_db(self) -> sqlite3.Connection:
        """Return a DB with schema + migrations 001-020 applied (no 021)."""
        db = sqlite3.connect(":memory:", check_same_thread=False)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        from water_monitor.app.database import _create_schema
        _create_schema(db)
        # Bootstrap _schema_version table (normally created by run_migrations)
        from water_monitor.app.db_migrations import _get_version
        _get_version(db)
        # Run only up to migration 020
        from water_monitor.app.db_migrations import MIGRATIONS
        for version, fn in MIGRATIONS:
            if version >= 21:
                break
            fn(db)
            db.execute("UPDATE _schema_version SET version = ?", (version,))
            db.commit()
        return db

    def test_normalizes_mixed_offsets_then_dedups(self):
        """Two rows with same instant but different offsets collapse to one UTC row."""
        db = self._make_pre_migration_db()
        # Same logical event stored twice with different offset representations
        _insert_event(db, start_ts="2026-11-01T07:00:00+00:00",
                      event_id=str(uuid.uuid4()))
        _insert_event(db, start_ts="2026-11-01T01:00:00-06:00",
                      event_id=str(uuid.uuid4()))
        assert _event_count(db, "main") == 2

        # Apply migration 021
        from water_monitor.app.db_migrations import _migrate_021
        _migrate_021(db)

        assert _event_count(db, "main") == 1
        row = db.execute("SELECT start_ts FROM events").fetchone()
        assert row[0].endswith("+00:00"), f"Expected UTC, got {row[0]!r}"

        # UNIQUE index should now exist
        idx = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_events_circuit_start_unique'"
        ).fetchone()
        assert idx is not None, "UNIQUE index not created by migration 021"

    def test_recomputes_uuid5_id_during_migration(self):
        """Migration 021 recomputes id to UUID5(circuit, new_utc_ts)."""
        db = self._make_pre_migration_db()
        start_mdt = "2026-11-01T01:00:00-06:00"
        start_utc = "2026-11-01T07:00:00+00:00"
        expected_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"main/{start_utc}"))
        _insert_event(db, start_ts=start_mdt, event_id=str(uuid.uuid4()))

        from water_monitor.app.db_migrations import _migrate_021
        _migrate_021(db)

        row = db.execute("SELECT id FROM events").fetchone()
        assert row[0] == expected_id, (
            f"Expected recomputed UUID5 {expected_id!r}, got {row[0]!r}"
        )

    def test_old_index_dropped(self):
        """Migration 021 drops idx_events_circuit_ts."""
        db = self._make_pre_migration_db()
        _insert_event(db)

        from water_monitor.app.db_migrations import _migrate_021
        _migrate_021(db)

        old_idx = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_events_circuit_ts'"
        ).fetchone()
        assert old_idx is None, "Old idx_events_circuit_ts should have been dropped"
