"""
Unit tests for database.merge_clusters() — Phase 2 cluster merging.

Covers merge correctness (event relink, centroid/std/member_count recompute,
fixture cleanup), validation failures (no write), and mid-merge rollback.

Run:  pytest water_monitor/tests/test_merge_clusters.py -v
"""
from __future__ import annotations

import json
import math
import sqlite3

import pytest

from water_monitor.app.cluster_engine import (
    LEVEL_LEARNING_MAX,
    LEVEL_PRELIMINARY_MAX,
)
from water_monitor.app.database import merge_clusters

from .conftest import make_db


# ==========================================================================
# Helpers
# ==========================================================================

def _add_cluster(conn, cid, circuit="main", *, centroid=None, feature_std=None,
                 member_count=10, fixture_id=None):
    conn.execute(
        """INSERT INTO fixture_clusters
               (id, circuit, centroid, feature_std, member_count,
                confidence_level, fixture_id, created_at)
           VALUES (?, ?, ?, ?, ?, 'preliminary', ?, '2026-01-01T00:00:00')""",
        (cid, circuit, json.dumps(centroid or {}),
         json.dumps(feature_std or {}), member_count, fixture_id),
    )
    conn.commit()


def _add_fixture(conn, fid, circuit="main", name="Fixture"):
    conn.execute(
        "INSERT INTO fixtures (id, circuit, name, confirmed) VALUES (?, ?, ?, 1)",
        (fid, circuit, name),
    )
    conn.commit()


def _add_event(conn, eid, cluster_id, circuit="main", fixture_id=None):
    conn.execute(
        """INSERT INTO events (id, circuit, start_ts, cluster_id, fixture_id)
           VALUES (?, ?, '2026-01-01T08:00:00', ?, ?)""",
        (eid, circuit, cluster_id, fixture_id),
    )
    conn.commit()


def _cluster_ids(conn, circuit="main"):
    return sorted(
        r["id"] for r in conn.execute(
            "SELECT id FROM fixture_clusters WHERE circuit = ?", (circuit,)
        ).fetchall()
    )


def _event_cluster(conn, eid):
    return conn.execute(
        "SELECT cluster_id FROM events WHERE id = ?", (eid,)
    ).fetchone()["cluster_id"]


# ==========================================================================
# 1. Basic merge — events relinked, member_count summed, losers removed
# ==========================================================================

def test_merge_basic():
    conn = make_db()
    _add_cluster(conn, 1, member_count=10)
    _add_cluster(conn, 2, member_count=15)
    _add_event(conn, "e1", 1)
    _add_event(conn, "e2", 2)
    _add_event(conn, "e3", 2)

    summary = merge_clusters(conn, "main", 1, [1, 2])

    assert summary["events_relinked"] == 2
    assert summary["survivor_member_count"] == 25
    assert _cluster_ids(conn) == [1]
    assert _event_cluster(conn, "e1") == 1
    assert _event_cluster(conn, "e2") == 1
    assert _event_cluster(conn, "e3") == 1

    row = conn.execute(
        "SELECT member_count FROM fixture_clusters WHERE circuit='main' AND id=1"
    ).fetchone()
    assert row["member_count"] == 25


# ==========================================================================
# 2. Centroid is the member-count weighted mean
# ==========================================================================

def test_merge_centroid_weighted_mean():
    conn = make_db()
    _add_cluster(conn, 1, centroid={"volume_litres": 4.0}, member_count=10)
    _add_cluster(conn, 2, centroid={"volume_litres": 10.0}, member_count=30)

    merge_clusters(conn, "main", 1, [1, 2])

    centroid = json.loads(conn.execute(
        "SELECT centroid FROM fixture_clusters WHERE circuit='main' AND id=1"
    ).fetchone()["centroid"])
    # (10*4 + 30*10) / 40 = 8.5
    assert centroid["volume_litres"] == pytest.approx(8.5)


# ==========================================================================
# 3. confidence_level recomputed from the summed member_count
# ==========================================================================

def test_merge_confidence_level_crosses_threshold():
    conn = make_db()
    # Two small 'preliminary' clusters that sum past LEVEL_PRELIMINARY_MAX.
    n1 = LEVEL_PRELIMINARY_MAX - 10
    n2 = 20
    assert n1 + n2 >= LEVEL_PRELIMINARY_MAX
    assert n1 + n2 < LEVEL_LEARNING_MAX
    _add_cluster(conn, 1, member_count=n1)
    _add_cluster(conn, 2, member_count=n2)

    merge_clusters(conn, "main", 1, [1, 2])

    level = conn.execute(
        "SELECT confidence_level FROM fixture_clusters WHERE circuit='main' AND id=1"
    ).fetchone()["confidence_level"]
    assert level == "learning"


# ==========================================================================
# 4. Pooled standard deviation when every cluster has std + mean
# ==========================================================================

def test_merge_pooled_std():
    conn = make_db()
    _add_cluster(conn, 1, centroid={"f": 0.0}, feature_std={"f": 2.0},
                 member_count=10)
    _add_cluster(conn, 2, centroid={"f": 0.0}, feature_std={"f": 4.0},
                 member_count=10)

    merge_clusters(conn, "main", 1, [1, 2])

    std = json.loads(conn.execute(
        "SELECT feature_std FROM fixture_clusters WHERE circuit='main' AND id=1"
    ).fetchone()["feature_std"])
    # means equal → combined_var = (10*4 + 10*16)/20 = 10 → std = sqrt(10)
    assert std["f"] == pytest.approx(math.sqrt(10.0))


# ==========================================================================
# 5. Non-survivor fixtures deleted, survivor fixture kept
# ==========================================================================

def test_merge_deletes_loser_fixtures_keeps_survivor():
    conn = make_db()
    _add_fixture(conn, "fx-survivor", name="Shower")
    _add_fixture(conn, "fx-loser", name="Stray")
    _add_cluster(conn, 1, member_count=20, fixture_id="fx-survivor")
    _add_cluster(conn, 2, member_count=5, fixture_id="fx-loser")
    _add_event(conn, "e1", 2, fixture_id="fx-loser")

    summary = merge_clusters(conn, "main", 1, [1, 2])

    assert summary["fixtures_removed"] == 1
    fixtures = {r["id"] for r in conn.execute(
        "SELECT id FROM fixtures"
    ).fetchall()}
    assert fixtures == {"fx-survivor"}
    # Relinked event now points at the survivor cluster + survivor fixture.
    ev = conn.execute(
        "SELECT cluster_id, fixture_id FROM events WHERE id='e1'"
    ).fetchone()
    assert ev["cluster_id"] == 1
    assert ev["fixture_id"] == "fx-survivor"


# ==========================================================================
# 6. Shared fixture_id never deletes the survivor's fixture
# ==========================================================================

def test_merge_shared_fixture_id_not_deleted():
    conn = make_db()
    _add_fixture(conn, "fx-shared", name="Shared")
    # Malformed data: both clusters point at the same fixture.
    _add_cluster(conn, 1, member_count=20, fixture_id="fx-shared")
    _add_cluster(conn, 2, member_count=5, fixture_id="fx-shared")

    summary = merge_clusters(conn, "main", 1, [1, 2])

    assert summary["fixtures_removed"] == 0
    assert conn.execute(
        "SELECT COUNT(*) c FROM fixtures WHERE id='fx-shared'"
    ).fetchone()["c"] == 1


# ==========================================================================
# 7. Validation failures raise ValueError and write nothing
# ==========================================================================

def _seed_two_circuits():
    conn = make_db()
    _add_cluster(conn, 1, circuit="main", member_count=10)
    _add_cluster(conn, 2, circuit="main", member_count=10)
    _add_cluster(conn, 5, circuit="irrigation", member_count=10)
    _add_event(conn, "e1", 1, circuit="main")
    _add_event(conn, "e2", 2, circuit="main")
    return conn


def _assert_untouched(conn):
    assert _cluster_ids(conn, "main") == [1, 2]
    assert _event_cluster(conn, "e1") == 1
    assert _event_cluster(conn, "e2") == 2


@pytest.mark.parametrize("survivor,ids", [
    (1, [1]),               # fewer than 2 IDs
    (1, [1, 1]),            # duplicates collapse to a single ID
    (9, [1, 2]),            # survivor not among selected IDs
    (1, [1, 999]),          # unknown cluster ID
    (1, [1, 5]),            # cluster 5 belongs to a different circuit
])
def test_merge_validation_failures_write_nothing(survivor, ids):
    conn = _seed_two_circuits()
    with pytest.raises(ValueError):
        merge_clusters(conn, "main", survivor, ids)
    _assert_untouched(conn)


def test_merge_zero_member_count_rejected():
    conn = make_db()
    _add_cluster(conn, 1, member_count=0)
    _add_cluster(conn, 2, member_count=0)
    _add_event(conn, "e1", 2)
    with pytest.raises(ValueError):
        merge_clusters(conn, "main", 1, [1, 2])
    assert _cluster_ids(conn) == [1, 2]
    assert _event_cluster(conn, "e1") == 2


# ==========================================================================
# 8. Mid-merge failure rolls back every write
# ==========================================================================

class _FailingConn:
    """Wraps a sqlite3.Connection and raises on the Nth execute() call.

    Used to inject a failure *after* the event-relink write has run, so the
    test exercises the rollback path rather than pre-write validation.
    """

    def __init__(self, conn: sqlite3.Connection, fail_on_sql_substr: str):
        self._conn = conn
        self._fail_on = fail_on_sql_substr

    def execute(self, sql, *args, **kwargs):
        if self._fail_on in sql:
            raise sqlite3.OperationalError("injected mid-merge failure")
        return self._conn.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_merge_mid_failure_rolls_back():
    conn = make_db()
    _add_cluster(conn, 1, centroid={"f": 1.0}, member_count=10)
    _add_cluster(conn, 2, centroid={"f": 2.0}, member_count=15)
    _add_event(conn, "e1", 1)
    _add_event(conn, "e2", 2)

    # Fail on the DELETE FROM fixture_clusters step — after the event relink
    # and the survivor UPDATE have already been issued in this transaction.
    failing = _FailingConn(conn, "DELETE FROM fixture_clusters")
    with pytest.raises(sqlite3.OperationalError):
        merge_clusters(failing, "main", 1, [1, 2])

    # Everything must be back to the pre-merge state.
    assert _cluster_ids(conn) == [1, 2]
    assert _event_cluster(conn, "e1") == 1
    assert _event_cluster(conn, "e2") == 2
    row = conn.execute(
        "SELECT member_count, centroid FROM fixture_clusters "
        "WHERE circuit='main' AND id=1"
    ).fetchone()
    assert row["member_count"] == 10
    assert json.loads(row["centroid"]) == {"f": 1.0}
