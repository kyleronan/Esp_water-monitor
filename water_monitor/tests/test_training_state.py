"""
Unit tests for the training state machine — calibration → labelling → live.

Tests:
  1. test_start_calibration_clears_orphan_clusters
  2. test_start_calibration_preserves_confirmed_clusters
  3. test_complete_calibration_enters_labelling
  4. test_activate_from_labelling_goes_live
  5. test_activate_from_wrong_state_returns_false
  6. test_auto_timeout_activates_after_seven_days
  7. test_get_training_info_labelling_is_complete
  8. test_publish_status_labelling_attrs

Run: pytest water_monitor/tests/test_training_state.py -v
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from water_monitor.app.database import (
    get_training_state, upsert_training_state,
)
from water_monitor.app.training_manager import (
    TrainingManager, LABELLING_AUTO_TIMEOUT_DAYS,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _run(coro):
    """Drive an async function from a synchronous test."""
    return asyncio.new_event_loop().run_until_complete(coro)


def _insert_cluster(db, circuit, cluster_id, fixture_id=None):
    """Insert a fixture_clusters row.  fixture_id=None → orphan."""
    db.execute(
        """INSERT INTO fixture_clusters
           (circuit, id, centroid, feature_std,
            member_count, confidence_level, fixture_id, created_at)
           VALUES (?, ?, '{}', '{}', 5, 'preliminary', ?, datetime('now'))""",
        (circuit, cluster_id, fixture_id),
    )
    db.commit()


def _insert_fixture(db, fixture_id, circuit="main",
                    name="Test Toilet", fixture_type="toilet"):
    db.execute(
        """INSERT INTO fixtures
           (id, circuit, name, fixture_type, confirmed, created_at)
           VALUES (?, ?, ?, ?, 1, datetime('now'))""",
        (fixture_id, circuit, name, fixture_type),
    )
    db.commit()


# ── Commit 1: Clean-start tests ──────────────────────────────────────────────

def test_start_calibration_clears_orphan_clusters(db, ha, cfg):
    """start_calibration() removes unconfirmed clusters from a previous run
    so stale micro-clusters don't pollute the new calibration."""
    # 7 orphan clusters on main (mirrors the screenshot scenario)
    for i in range(7):
        _insert_cluster(db, "main", i, fixture_id=None)

    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))

    rows = db.execute(
        "SELECT COUNT(*) AS n FROM fixture_clusters WHERE circuit='main'"
    ).fetchone()
    assert rows["n"] == 0, "all 7 orphan clusters should be cleared"


def test_start_calibration_preserves_confirmed_clusters(db, ha, cfg):
    """Clusters with fixture_id IS NOT NULL represent user-labelled fixtures
    and must survive recalibration."""
    _insert_fixture(db, "fx_toilet_1", "Master Toilet", "toilet")
    _insert_cluster(db, "main", 0, fixture_id="fx_toilet_1")  # confirmed
    _insert_cluster(db, "main", 1, fixture_id=None)            # orphan
    _insert_cluster(db, "main", 2, fixture_id=None)            # orphan

    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))

    rows = db.execute(
        "SELECT id, fixture_id FROM fixture_clusters "
        "WHERE circuit='main' ORDER BY id"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == 0
    assert rows[0]["fixture_id"] == "fx_toilet_1"


# ── Commit 2: Labelling state tests ──────────────────────────────────────────

def test_complete_calibration_enters_labelling(db, ha, cfg):
    """Calibration completion → labelling (NOT live).  User must
    explicitly activate to transition labelling → live."""
    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))
    _run(tm.complete_calibration("main"))

    state = get_training_state(db, "main")
    assert state["state"] == "labelling", \
        "complete_calibration must transition to labelling, not live"
    assert state["completed_at"] is not None


def test_activate_from_labelling_goes_live(db, ha, cfg):
    """activate_fixtures() on a labelling circuit → live, returns True."""
    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))
    _run(tm.complete_calibration("main"))

    ok = _run(tm.activate_fixtures("main"))
    assert ok is True

    state = get_training_state(db, "main")
    assert state["state"] == "live"


def test_activate_from_wrong_state_returns_false(db, ha, cfg):
    """activate_fixtures() called from calibrating, idle, or live must
    return False without changing state — protects against stale browser
    tabs hitting the endpoint at the wrong moment."""
    tm = TrainingManager(cfg, db, ha)

    # idle
    ok = _run(tm.activate_fixtures("main"))
    assert ok is False

    # calibrating
    _run(tm.start_calibration("main", calibration_days=14))
    ok = _run(tm.activate_fixtures("main"))
    assert ok is False
    assert get_training_state(db, "main")["state"] == "calibrating"

    # live (already activated)
    _run(tm.complete_calibration("main"))
    _run(tm.activate_fixtures("main"))   # labelling → live
    ok = _run(tm.activate_fixtures("main"))   # live → no-op
    assert ok is False
    assert get_training_state(db, "main")["state"] == "live"


def test_auto_timeout_activates_after_seven_days(db, ha, cfg):
    """A labelling circuit that hasn't been activated within
    LABELLING_AUTO_TIMEOUT_DAYS days must auto-activate so anomaly
    detection isn't blocked indefinitely."""
    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))

    # Force-set state to labelling with a stale completed_at (8 days ago)
    stale_completed = (datetime.now(timezone.utc)
                       - timedelta(days=LABELLING_AUTO_TIMEOUT_DAYS + 1))
    upsert_training_state(
        db, "main",
        state="labelling",
        completed_at=stale_completed.isoformat(),
    )

    _run(tm._check_progress("main"))

    state = get_training_state(db, "main")
    assert state["state"] == "live", \
        f"expected auto-activation after {LABELLING_AUTO_TIMEOUT_DAYS} days, got '{state['state']}'"

    # An auto-activate notification should have been sent
    assert any("auto-activated" in n["title"].lower()
               for n in ha.notifications), \
        "auto-activation must send a HA notification"


def test_auto_timeout_does_not_fire_too_early(db, ha, cfg):
    """A labelling circuit that completed less than the timeout window ago
    must remain in labelling."""
    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))

    fresh_completed = (datetime.now(timezone.utc)
                       - timedelta(days=LABELLING_AUTO_TIMEOUT_DAYS - 1))
    upsert_training_state(
        db, "main",
        state="labelling",
        completed_at=fresh_completed.isoformat(),
    )

    _run(tm._check_progress("main"))

    assert get_training_state(db, "main")["state"] == "labelling"


def test_get_training_info_labelling_is_complete(db, ha, cfg):
    """get_training_info() should report 100% complete for both
    labelling and live states — calibration itself is done."""
    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))
    _run(tm.complete_calibration("main"))

    info = tm.get_training_info("main")
    assert info["state"] == "labelling"
    assert info["percent_complete"] == 100
    assert info["days_remaining"] == 0


def test_publish_status_labelling_attrs(db, ha, cfg):
    """The HA sensor for a labelling circuit must publish state='labelling'
    with percent_complete=100 — calibration is done, awaiting review."""
    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))
    _run(tm.complete_calibration("main"))

    # complete_calibration already calls _publish_status — last set_state
    # for the main circuit is what we want.
    main_states = [s for s in ha.states
                   if s["entity_id"] == "sensor.water_training_status_main"]
    assert main_states, "expected at least one set_state call for main"
    last = main_states[-1]
    assert last["state"] == "labelling"
    assert last["attrs"].get("percent_complete") == 100


# ── Cross-cutting: start_calibration allowed from labelling ──────────────────

def test_recalibrate_from_labelling_is_allowed(db, ha, cfg):
    """A user reviewing clusters in labelling state must be able to
    trigger recalibration directly without first resetting to idle."""
    tm = TrainingManager(cfg, db, ha)
    _run(tm.start_calibration("main", calibration_days=14))
    _run(tm.complete_calibration("main"))
    assert get_training_state(db, "main")["state"] == "labelling"

    started = _run(tm.start_calibration("main", calibration_days=14))
    assert started is True
    assert get_training_state(db, "main")["state"] == "calibrating"
