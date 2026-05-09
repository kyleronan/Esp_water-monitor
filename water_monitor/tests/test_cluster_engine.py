"""
Unit tests for cluster_engine.py — Phase 2.1 type-aware match gate.

Tests 1–8 and 11–14 cover the variance-profile gate (Commits 1–3).
Tests on exclusion windows (plan items 13 / 15) live in
test_exclusion_windows.py alongside their Commit 5 code.

Run:  pytest water_monitor/tests/test_cluster_engine.py -v
"""
from __future__ import annotations

import json
import math
import sqlite3
from typing import Dict

import pytest

from water_monitor.app.cluster_engine import (
    DBSTREAM_CLUSTERING_THRESHOLD,
    FEATURE_KEYS,
    ClusterEngine,
)
from water_monitor.app.fixtures import (
    FIXTURE_TYPES,
    FIXTURE_VARIANCE_PROFILES,
    FIXTURE_MATCH_THRESHOLDS,
    get_match_threshold,
)

# Helpers imported from conftest via pytest fixture injection.
# Direct imports for non-fixture helpers:
from .conftest import (
    make_db,
    make_engine,
    toilet_event,
    shower_event,
)


# ==========================================================================
# 1. Weighted distance — uniform weights equal plain Euclidean
# ==========================================================================

def test_weighted_distance_uniform_when_no_weights():
    """All weights 1.0 → same as plain Euclidean over FEATURE_KEYS."""
    a = {k: 0.0 for k in FEATURE_KEYS}
    b = {k: 0.0 for k in FEATURE_KEYS}
    a["volume_litres"] = 3.0
    a["duration_seconds"] = 4.0
    # sqrt(3² + 4²) = 5.0
    weights = {k: 1.0 for k in FEATURE_KEYS}
    result = ClusterEngine._weighted_distance(a, b, weights)
    assert abs(result - 5.0) < 1e-9


# ==========================================================================
# 2. Weighted distance — anchors amplify, floats contribute zero
# ==========================================================================

def test_weighted_distance_anchors_amplify_and_floats_zero():
    """Anchor weight 4.0 amplifies; float weight 0.0 ignores that feature."""
    a = {k: 0.0 for k in FEATURE_KEYS}
    b = {k: 0.0 for k in FEATURE_KEYS}
    a["volume_litres"] = 1.0     # anchor: weight 4.0 → contributes 4 * 1² = 4
    a["duration_seconds"] = 1.0  # float: weight 0.0  → contributes 0

    weights = {k: 1.0 for k in FEATURE_KEYS}
    weights["volume_litres"] = 4.0
    weights["duration_seconds"] = 0.0

    result = ClusterEngine._weighted_distance(a, b, weights)
    # sqrt(4 * 1²) = 2.0  (duration_seconds contributes nothing)
    assert abs(result - 2.0) < 1e-9


# ==========================================================================
# 3. _build_match_weights for toilet — anchor ↑, float = 0, others = 1.0
# ==========================================================================

def test_build_match_weights_for_toilet(engine_main):
    weights = engine_main._build_match_weights("toilet")

    # Anchor features for toilet (from FIXTURE_VARIANCE_PROFILES)
    assert weights["volume_litres"] > 1.0
    assert weights["duration_seconds"] > 1.0
    assert weights["avg_flow_lpm"] > 1.0

    # Float features zeroed (time-of-day is behaviour, not physics)
    assert weights["hour_sin"] == 0.0
    assert weights["hour_cos"] == 0.0

    # Unmodified features stay at 1.0
    assert weights["peak_flow_lpm"] == 1.0
    assert weights["pressure_delta_psi"] == 1.0


# ==========================================================================
# 4. Type cache populated from confirmed fixtures at startup
# ==========================================================================

def test_type_cache_populated_from_confirmed_fixtures():
    db = make_db()

    # Insert a confirmed fixture linked to cluster 0
    db.execute(
        "INSERT INTO fixtures (id, circuit, confirmed, fixture_type) "
        "VALUES ('f1', 'main', 1, 'toilet')"
    )
    db.execute(
        "INSERT INTO fixture_clusters "
        "(circuit, id, centroid, feature_std, member_count, confidence_level, fixture_id) "
        "VALUES ('main', 0, '{}', '{}', 10, 'confirmed', 'f1')"
    )
    db.commit()

    engine = make_engine(db, ["main"])
    assert engine._type_cache["main"] == {0: "toilet"}


# ==========================================================================
# 5. notify_fixture_confirmed / removed mutate cache live
# ==========================================================================

def test_notify_fixture_confirmed_updates_cache(engine_main):
    engine = engine_main

    # Cache starts empty (no confirmed fixtures in the fresh DB)
    assert engine._type_cache["main"] == {}

    engine.notify_fixture_confirmed("main", 0, "toilet")
    assert engine._type_cache["main"][0] == "toilet"

    engine.notify_fixture_removed("main", 0)
    assert 0 not in engine._type_cache["main"]


def test_notify_fixture_confirmed_overwrites_existing_type(engine_main):
    """Re-confirming a cluster with a different type updates the cache."""
    engine = engine_main
    engine.notify_fixture_confirmed("main", 0, "toilet")
    engine.notify_fixture_confirmed("main", 0, "shower")
    assert engine._type_cache["main"][0] == "shower"


def test_notify_fixture_removed_no_op_when_absent(engine_main):
    """Removing a cluster that was never confirmed doesn't raise."""
    engine_main.notify_fixture_removed("main", 999)  # should not raise


# ==========================================================================
# 6. Gate rejects event with oversize volume on confirmed toilet cluster
# ==========================================================================

def _seed_confirmed_toilet(db: sqlite3.Connection, engine: ClusterEngine,
                           circuit: str = "main") -> int:
    """Feed one normal toilet event to create cluster 0, then confirm it as
    toilet.  Returns the DB cluster_id (always 0 for the stub)."""
    ev = toilet_event()
    cluster_id, confidence, level, reason = engine.match_and_learn(ev, circuit)
    assert cluster_id is not None, "first event should create a cluster"

    # Confirm the cluster as toilet so the gate activates
    fixture_id = "f_toilet"
    db.execute(
        "INSERT OR REPLACE INTO fixtures "
        "(id, circuit, confirmed, fixture_type) VALUES (?, ?, 1, 'toilet')",
        (fixture_id, circuit),
    )
    db.execute(
        "UPDATE fixture_clusters SET fixture_id = ? "
        "WHERE circuit = ? AND id = ?",
        (fixture_id, circuit, cluster_id),
    )
    db.commit()
    engine.notify_fixture_confirmed(circuit, cluster_id, "toilet")
    return cluster_id


def test_match_rejects_oversize_volume_for_toilet(db, engine_main):
    engine = engine_main
    cluster_id = _seed_confirmed_toilet(db, engine)

    # Sanity: a normal-volume toilet event still passes
    ev_normal = toilet_event()
    cid, conf, level, reason = engine.match_and_learn(ev_normal, "main")
    assert cid == cluster_id, "normal toilet should match"
    assert reason is None

    # Read the member_count before feeding the bad event
    row = db.execute(
        "SELECT member_count, centroid FROM fixture_clusters "
        "WHERE circuit = 'main' AND id = ?", (cluster_id,)
    ).fetchone()
    count_before = row["member_count"]
    centroid_before = row["centroid"]

    # Oversize-volume event: 12 L vs 6 L centroid.
    # Weighted diff: weight=3.0, diff=6.0 → dist = sqrt(3*(6²)) ≈ 10.4 >> 0.6
    ev_big = toilet_event(volume_litres=12.0)
    cid_bad, _, _, reason_bad = engine.match_and_learn(ev_big, "main")
    assert cid_bad is None, "oversize-volume event must be rejected"
    assert reason_bad == "type_gate_rejected"

    # Verify centroid and member_count were NOT mutated
    row2 = db.execute(
        "SELECT member_count, centroid FROM fixture_clusters "
        "WHERE circuit = 'main' AND id = ?", (cluster_id,)
    ).fetchone()
    assert row2["member_count"] == count_before, \
        "rejected event must not increment member_count"
    assert row2["centroid"] == centroid_before, \
        "rejected event must not update centroid"


# ==========================================================================
# 7. Gate accepts long shower — duration is a float feature for showers
# ==========================================================================

def _seed_confirmed_shower(db: sqlite3.Connection, engine: ClusterEngine,
                            circuit: str = "main") -> int:
    """Feed one standard shower event, confirm cluster as shower."""
    ev = shower_event()
    cluster_id, _, _, reason = engine.match_and_learn(ev, circuit)
    assert cluster_id is not None

    fixture_id = "f_shower"
    db.execute(
        "INSERT OR REPLACE INTO fixtures "
        "(id, circuit, confirmed, fixture_type) VALUES (?, ?, 1, 'shower')",
        (fixture_id, circuit),
    )
    db.execute(
        "UPDATE fixture_clusters SET fixture_id = ? "
        "WHERE circuit = ? AND id = ?",
        (fixture_id, circuit, cluster_id),
    )
    db.commit()
    engine.notify_fixture_confirmed(circuit, cluster_id, "shower")
    return cluster_id


def test_match_accepts_long_shower(db, engine_main):
    """Duration is a float feature for showers — a 25-min shower still
    matches when flow rate and other anchor features are identical."""
    engine = engine_main
    cluster_id = _seed_confirmed_shower(db, engine)

    # Long shower: 25 min / 200 L but same flow / pressure as centroid.
    # Shower float_features include duration_seconds & volume_litres (weight=0).
    # Anchor features (avg_flow, pressure_delta, flow_variability) are identical.
    ev_long = shower_event(
        duration_seconds=1500.0,
        volume_litres=200.0,
    )
    cid, conf, level, reason = engine.match_and_learn(ev_long, "main")
    assert cid == cluster_id, \
        "long-duration shower must match when anchor features agree"
    assert reason is None


# ==========================================================================
# 8. Unconfirmed cluster — gate does not trigger, behaviour unchanged
# ==========================================================================

def test_unconfirmed_cluster_unchanged(db, engine_main):
    """No confirmed fixtures → _type_cache empty → gate never runs.
    Events cluster normally regardless of feature values.
    """
    engine = engine_main
    assert engine._type_cache["main"] == {}

    # Feed several events — all should cluster without rejection
    for _ in range(3):
        ev = toilet_event()
        cid, conf, level, reason = engine.match_and_learn(ev, "main")
        # First event returns None (DBSTREAM has no centres BEFORE learn_one
        # populates them? Actually the stub always has centres after learn_one.
        # If no centres yet the engine returns no_centers — just check no crash)
        if cid is not None:
            assert reason is None, "unconfirmed event must never be gate-rejected"

    # The type cache must still be empty — no spurious entries
    assert engine._type_cache["main"] == {}


# ==========================================================================
# 9. Schema drift guard — every FIXTURE_TYPES entry has a profile
# ==========================================================================

def test_all_fixture_types_have_profiles():
    """Catches the 'added a new fixture type, forgot the profile' mistake."""
    for ftype in FIXTURE_TYPES:
        assert ftype in FIXTURE_VARIANCE_PROFILES, \
            f"Missing FIXTURE_VARIANCE_PROFILES entry for '{ftype}'"
        assert ftype in FIXTURE_MATCH_THRESHOLDS, \
            f"Missing FIXTURE_MATCH_THRESHOLDS entry for '{ftype}'"


def test_get_match_threshold_fallback():
    """Unknown / None types fall back to the 'other' threshold (= DBSTREAM global)."""
    assert get_match_threshold(None) == FIXTURE_MATCH_THRESHOLDS["other"]
    assert get_match_threshold("flux_capacitor") == FIXTURE_MATCH_THRESHOLDS["other"]
    assert get_match_threshold("other") == DBSTREAM_CLUSTERING_THRESHOLD


# ==========================================================================
# 10. Calibrating circuit — gate does not interfere with unconfirmed events
# ==========================================================================

def test_calibration_phase_unaffected_by_gate(db, engine_main):
    """During calibration no fixtures are confirmed, so the gate is never
    triggered.  Events must cluster (or return no_centers) but never
    receive 'type_gate_rejected'."""
    engine = engine_main

    results = []
    for i in range(5):
        ev = toilet_event(volume_litres=float(i + 4))   # slight variation
        cid, conf, level, reason = engine.match_and_learn(ev, "main")
        if cid is None:
            assert reason != "type_gate_rejected", \
                "gate must not fire during calibration with no confirmed clusters"
        results.append(cid)

    # At least some events should have been clustered (stub always produces a centre)
    assert any(r is not None for r in results)


# ==========================================================================
# 11. Re-confirming a cluster with a different type changes gate behaviour
# ==========================================================================

def test_reconfirm_changes_gate_behaviour(db, engine_main):
    """Confirm as toilet (tight), reject shower-shaped event.
    Then re-confirm as shower (loose), same event should match.
    No restart between the two confirmations.
    """
    engine = engine_main
    cluster_id = _seed_confirmed_toilet(db, engine)

    # A shower-shaped event has much higher volume (40 L vs toilet 6 L).
    # Toilet gate: sqrt(3*(34²)) ≈ 58.9 >> 0.6 → rejected.
    ev_shower_shaped = toilet_event(
        avg_flow_lpm=8.0,
        volume_litres=40.0,
        duration_seconds=300.0,
    )
    cid1, _, _, reason1 = engine.match_and_learn(ev_shower_shaped, "main")
    assert cid1 is None and reason1 == "type_gate_rejected", \
        "toilet gate must reject shower-shaped event"

    # Re-confirm the same cluster as shower (threshold 2.5, duration/volume float)
    db.execute(
        "UPDATE fixtures SET fixture_type = 'shower' WHERE id = 'f_toilet'"
    )
    db.commit()
    engine.notify_fixture_confirmed("main", cluster_id, "shower")

    # Now the same shower-shaped event: shower float_features include
    # volume_litres and duration_seconds (both weight=0).
    # Anchor diff on avg_flow_lpm: 8.0 vs 8.0 → 0. All others match centroid.
    # Expected weighted dist ≈ 0 < 2.5 → matches.
    cid2, _, _, reason2 = engine.match_and_learn(ev_shower_shaped, "main")
    assert cid2 == cluster_id, \
        "after re-confirming as shower, same event must match"
    assert reason2 is None


# ==========================================================================
# 12. Corrupt or empty centroid fails safe (no crash)
# ==========================================================================

def test_empty_or_corrupt_centroid_fails_safe(db, engine_main):
    """When the stored centroid is '{}' or corrupt JSON the gate must not
    crash.  The fail-open path lets the event through to normal matching."""
    engine = engine_main
    cluster_id = _seed_confirmed_toilet(db, engine)

    # Corrupt the stored centroid
    db.execute(
        "UPDATE fixture_clusters SET centroid = 'NOT_JSON' "
        "WHERE circuit = 'main' AND id = ?",
        (cluster_id,),
    )
    db.commit()

    # Should not raise; gate catches the JSON error and falls through
    ev = toilet_event()
    cid, conf, level, reason = engine.match_and_learn(ev, "main")
    # Fail-open: event is matched (not rejected)
    assert cid == cluster_id
    assert reason is None


def test_empty_string_centroid_fails_safe(db, engine_main):
    """When stored centroid is an empty string the gate skips the check.

    The ``fixture_clusters.centroid`` column is NOT NULL (DEFAULT '{}'), so
    NULL is never stored in production.  An empty string IS valid SQL and
    evaluates to falsy in Python, exercising the
    ``if row and row["centroid"]`` short-circuit without a NOT NULL error.
    """
    engine = engine_main
    cluster_id = _seed_confirmed_toilet(db, engine)

    # Set centroid to empty string — falsy, so gate skips the distance check
    db.execute(
        "UPDATE fixture_clusters SET centroid = '' "
        "WHERE circuit = 'main' AND id = ?",
        (cluster_id,),
    )
    db.commit()

    ev = toilet_event()
    # Gate: row["centroid"] is '' → `if row and row["centroid"]` is False
    # → gate does NOT reject → falls through to normal match
    cid, conf, level, reason = engine.match_and_learn(ev, "main")
    # No crash; fail-open means the event matches (or returns no_centers
    # in pathological DB states — both are acceptable vs. a crash)
    assert reason != "type_gate_rejected"


# ==========================================================================
# 14. Multi-circuit isolation — circuits use their own type caches
# ==========================================================================

def test_multi_circuit_isolation(db):
    """main has confirmed toilet (threshold 0.6);
    irrigation has confirmed irrigation_zone (threshold 2.2).
    A toilet-shaped event on irrigation must not be gated by the toilet
    threshold, and vice versa.
    """
    engine = make_engine(db, ["main", "irrigation"])

    # ── Confirm toilet on main ──────────────────────────────────────────
    ev_toilet = toilet_event()
    cid_main, _, _, _ = engine.match_and_learn(ev_toilet, "main")
    assert cid_main is not None
    db.execute(
        "INSERT OR REPLACE INTO fixtures (id, circuit, confirmed, fixture_type) "
        "VALUES ('f_toilet', 'main', 1, 'toilet')"
    )
    db.execute(
        "UPDATE fixture_clusters SET fixture_id = 'f_toilet' "
        "WHERE circuit = 'main' AND id = ?", (cid_main,)
    )
    db.commit()
    engine.notify_fixture_confirmed("main", cid_main, "toilet")

    # ── Confirm irrigation_zone on irrigation ───────────────────────────
    ev_irrigation = dict(
        avg_flow_lpm=15.0, peak_flow_lpm=16.0, duration_seconds=1800.0,
        volume_litres=450.0, pressure_delta_psi=1.0, has_pressure_transient=0.0,
        flow_variability=0.1, hour_sin=0.0, hour_cos=1.0,
        start_ts="2026-06-01T06:00:00", end_ts="2026-06-01T06:30:00",
    )
    cid_irr, _, _, _ = engine.match_and_learn(ev_irrigation, "irrigation")
    assert cid_irr is not None
    db.execute(
        "INSERT OR REPLACE INTO fixtures "
        "(id, circuit, confirmed, fixture_type) "
        "VALUES ('f_irr', 'irrigation', 1, 'irrigation_zone')"
    )
    db.execute(
        "UPDATE fixture_clusters SET fixture_id = 'f_irr' "
        "WHERE circuit = 'irrigation' AND id = ?", (cid_irr,)
    )
    db.commit()
    engine.notify_fixture_confirmed("irrigation", cid_irr, "irrigation_zone")

    # ── main: oversize-volume toilet event is gated by toilet threshold ─
    ev_bad_toilet = toilet_event(volume_litres=50.0)  # way off toilet centroid
    cid_reject, _, _, reason = engine.match_and_learn(ev_bad_toilet, "main")
    assert cid_reject is None and reason == "type_gate_rejected", \
        "main circuit must gate on toilet threshold, not irrigation_zone threshold"

    # ── irrigation: same oversize event is NOT gated (different circuit) ─
    # The irrigation circuit has a 450 L centroid; a 50 L event has a large
    # volume diff but irrigation_zone has volume_litres as a FLOAT feature
    # (weight=0) so it still passes.
    cid_irr2, _, _, reason2 = engine.match_and_learn(
        dict(ev_irrigation, volume_litres=50.0), "irrigation"
    )
    # The irrigation gate may accept or have no_centers; it must NOT gate-reject
    assert reason2 != "type_gate_rejected", \
        "irrigation circuit must not apply toilet gate"

    # Type caches are strictly separated: each circuit's dict contains exactly
    # its own confirmed type and nothing else.  (Both circuits independently
    # assign cluster id=0 to their first cluster, so the meaningful invariant
    # is the equality check — not a membership check across the two dicts.)
    assert engine._type_cache["main"] == {cid_main: "toilet"}, \
        "main type cache must contain only the toilet cluster"
    assert engine._type_cache["irrigation"] == {cid_irr: "irrigation_zone"}, \
        "irrigation type cache must contain only the irrigation_zone cluster"
    # The two dicts are independent objects — mutating one must not affect the other
    assert engine._type_cache["main"] is not engine._type_cache["irrigation"], \
        "circuit type caches must be separate dict objects"
