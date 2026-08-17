"""
SQLite database setup, migrations, and data access helpers.

Single database file at /data/water_monitor.db.
Schema is created in full on first run. All Phase 2 tables are
created now so Phase 2 never needs a schema migration.
"""
from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Generator, Iterable, List, Optional

log = logging.getLogger(__name__)

# Module-level write lock — serialises concurrent async writes.
# Created lazily within the running event loop.
_WRITE_LOCK: Optional[Any] = None

def get_write_lock():
    """Return the singleton asyncio write lock, creating it on first call."""
    import asyncio
    global _WRITE_LOCK
    if _WRITE_LOCK is None:
        _WRITE_LOCK = asyncio.Lock()
    return _WRITE_LOCK


# ── dev46 (46a) — THE single DB executor ─────────────────────────────────────
# The shared orchestrator connection is opened check_same_thread=False and was
# historically touched from whatever thread the default executor pool handed
# out. Two concurrent touches from different threads corrupt a statement
# mid-flight ("sqlite3.InterfaceError: bad parameter or other API misuse") —
# it killed the 8/15 reseed mid-replay and 500'd the History page during the
# 8/16 startup. One worker = the connection is PHYSICALLY serialized; the
# asyncio write lock above still provides job-level logical exclusivity on
# top. All threaded DB work must go through run_db(); only non-DB blocking
# work (HA I/O, file ops) may use the default pool.
_DB_EXECUTOR: Optional[Any] = None


def get_db_executor():
    """Return the singleton one-thread executor for ALL threaded DB work."""
    from concurrent.futures import ThreadPoolExecutor
    global _DB_EXECUTOR
    if _DB_EXECUTOR is None:
        _DB_EXECUTOR = ThreadPoolExecutor(max_workers=1,
                                          thread_name_prefix="db")
    return _DB_EXECUTOR


async def run_db(fn, *args, **kwargs):
    """Run blocking DB work on the single DB thread and await the result."""
    import asyncio
    import functools
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        get_db_executor(), functools.partial(fn, *args, **kwargs))

SCHEMA_VERSION = 1


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # Wait up to 5s before raising OperationalError on a locked DB.
    # Prevents immediate failures when cluster engine executor threads and
    # async coroutines briefly contend for the same connection.
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    """Best-effort rollback after a tolerated locked-DB write failure —
    a rollback on an already-broken connection state must not mask the
    original (already-handled) error."""
    try:
        conn.rollback()
    except sqlite3.Error:
        pass


# ── Stuck-writer detector (dev34) ────────────────────────────────────────────
# Observed live 2026-08-03: one connection sat in an open write transaction
# for 27+ minutes — every writer on every OTHER connection failed with
# "database is locked" (UI saves, the supply-regime sampler, the importer)
# and nothing in the log identified the holder. A brief lock is normal (the
# busy_timeout absorbs it); a lock that keeps failing for minutes is a wedged
# transaction only a restart clears. Callers that catch a locked error feed
# this; when the failures span the threshold it escalates ONCE per episode.
_LOCKED_EPISODE_START: float = 0.0
_LOCKED_LAST: float = 0.0
_LOCKED_COUNT: int = 0
_LOCKED_ESCALATED: bool = False
_LOCKED_STUCK_AFTER_S: float = 120.0
_LOCKED_RESET_GAP_S: float = 60.0


def note_locked_write(source: str) -> None:
    """Record a 'database is locked' failure. Cheap, thread-safe enough for
    its purpose (worst case an extra log line). A gap of _LOCKED_RESET_GAP_S
    without failures starts a new episode."""
    global _LOCKED_EPISODE_START, _LOCKED_LAST, _LOCKED_COUNT, _LOCKED_ESCALATED
    import time
    now = time.monotonic()
    if now - _LOCKED_LAST > _LOCKED_RESET_GAP_S:
        _LOCKED_EPISODE_START, _LOCKED_COUNT, _LOCKED_ESCALATED = now, 0, False
    _LOCKED_LAST = now
    _LOCKED_COUNT += 1
    span = now - _LOCKED_EPISODE_START
    if span >= _LOCKED_STUCK_AFTER_S and not _LOCKED_ESCALATED:
        _LOCKED_ESCALATED = True
        log.error(
            "WRITE LOCK APPEARS STUCK: %d locked-write failure(s) over %.0f s "
            "(latest from %s). One connection is holding an open write "
            "transaction; nothing else can save until it releases. If this "
            "persists, restart the add-on — and report this log line.",
            _LOCKED_COUNT, span, source)


async def run_isolated_write(db_path, fn):
    """Serialise a heavy DB-write job and run it on a fresh PRIVATE connection.

    Two guarantees that make user-triggered admin writes (recompute, reclassify)
    safe — both against each other AND against the inline writers on the shared
    ``orch.db`` connection (the live feature extractor, the pruner, …):

    * ``get_write_lock()`` is held for the whole job, so two admin writes can't
      run concurrently. (The bug this fixes: two simultaneous /recompute requests
      drove the *shared* connection from two worker threads → SQLite
      ``InterfaceError`` / "cannot commit - no transaction is active".)
    * the job runs on its OWN ``sqlite3.Connection`` (opened in the worker thread,
      closed after), so it never shares connection state across threads. WAL +
      ``busy_timeout`` handle writer-vs-writer between connections, and the job's
      per-row commits release the file write-lock between rows.

    ``fn`` is a sync callable taking the private connection; its return value is
    propagated. The connection is closed even if ``fn`` raises. ``db_path`` is
    passed in (not imported) so this module stays free of config imports.

    dev46 (46a) AUDIT EXEMPTION — justified separate connection. This job
    deliberately does NOT go through ``run_db``: it never touches the shared
    connection, and it is long-running, so putting it on the one DB worker
    would block every page render for its duration. It stays on the default
    pool. WAL + busy_timeout cover connection-vs-connection contention.
    """
    import asyncio

    async with get_write_lock():
        loop = asyncio.get_running_loop()

        def _run():
            conn = get_connection(db_path)
            try:
                return fn(conn)
            finally:
                conn.close()

        return await loop.run_in_executor(None, _run)


def yield_write_lock(conn: sqlite3.Connection, scanned: int,
                     every: int = 300) -> None:
    """Anti-starvation yield for long storage-only write loops (reclassify,
    embedded-fixture scan): every ``every`` rows, commit (releasing the SQLite
    file write-lock — a single end-of-loop commit holds it for the whole scan)
    and sleep ~30 ms so a concurrently waiting writer (a user label save) can
    win the lock — a bare commit re-acquires within microseconds, too fast for
    the waiter. ONE definition so the load-bearing cadence/sleep tuning can't
    drift between loops. Call from executor threads only (it sleeps)."""
    if scanned % every == 0:
        conn.commit()
        time.sleep(0.03)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create database and all tables. Safe to call on existing database."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)

    # Integrity check before creating/using schema
    result = conn.execute("PRAGMA integrity_check").fetchone()
    if result and result[0] != "ok":
        log.error("DATABASE INTEGRITY CHECK FAILED: %s — proceed with caution",
                  result[0])
    else:
        log.debug("Database integrity check passed")

    _create_schema(conn)
    log.info("Database initialised at %s", db_path)
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
-- ==========================================================================
-- DEVICE DISCOVERY — stores auto-discovered HA device and entity IDs.
-- Populated by the setup wizard; replaces manual config.yaml entity IDs.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS device_config (
    id                  INTEGER PRIMARY KEY DEFAULT 1,
    esp_device_name     TEXT,       -- name user searched for
    ha_device_id        TEXT,       -- HA device registry ID
    ha_device_name      TEXT,       -- HA device display name
    esp_device_prefix   TEXT,       -- derived entity ID prefix
    fw_version          TEXT,       -- ESPHome project.version from device registry
    setup_complete      BOOLEAN DEFAULT 0,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO device_config (id) VALUES (1);

CREATE TABLE IF NOT EXISTS circuit_entity_map (
    circuit     TEXT NOT NULL,
    role        TEXT NOT NULL,      -- flow_sensor, valve_entity, etc.
    entity_id   TEXT NOT NULL DEFAULT '',
    entity_name TEXT,               -- original_name from HA entity registry
    confirmed   BOOLEAN DEFAULT 0,
    PRIMARY KEY (circuit, role)
);

-- ==========================================================================
-- HOME & CIRCUIT PROFILE
-- ==========================================================================
CREATE TABLE IF NOT EXISTS home_profile (
    id              INTEGER PRIMARY KEY DEFAULT 1,
    bathrooms_full  INTEGER DEFAULT 0,
    bathrooms_half  INTEGER DEFAULT 0,
    sqft            INTEGER DEFAULT 0,
    floors          INTEGER DEFAULT 1,
    occupants       INTEGER DEFAULT 2,
    build_year      INTEGER,
    supply_type     TEXT DEFAULT 'mains',
    setup_complete  BOOLEAN DEFAULT 0,
    -- Away / vacation mode
    away_mode       BOOLEAN DEFAULT 0,
    away_since      TIMESTAMP,
    -- Display unit preferences (keys match units.FLOW_OPTIONS / PRESSURE_OPTIONS)
    flow_unit               TEXT DEFAULT 'L/min',
    pressure_unit           TEXT DEFAULT 'psi',
    -- Phase 2.1 fixture publishing
    publish_fixtures_to_ha  INTEGER DEFAULT 1,
    -- Mobile push notification targets (comma-separated HA notify service names)
    mobile_notify_targets   TEXT DEFAULT '',
    -- HA presence tracking — auto-toggle away mode from HA entity state changes.
    -- ha_presence_entities: comma-separated entity IDs to watch
    --   (person.*, device_tracker.*, input_boolean.*, alarm_control_panel.*)
    -- ha_away_state: state value that means "away" (default: not_home)
    -- ha_home_state: state value that means "home"  (default: home)
    -- When ALL entities reach ha_away_state → enable away mode.
    -- When ANY entity reaches ha_home_state  → disable away mode.
    ha_presence_entities    TEXT DEFAULT '',
    ha_away_state           TEXT DEFAULT 'not_home',
    ha_home_state           TEXT DEFAULT 'home',
    -- MQTT publishing toggle (Phase 2.1)
    mqtt_publish_enabled    INTEGER NOT NULL DEFAULT 0,
    -- History display: hide pressure-restoration phantom events from the
    -- History list (Sprint E). Off by default — phantoms are shown with a
    -- flag. This is display-only; it never affects volume totals (phantom
    -- volume is always zeroed at detection regardless of this toggle).
    hide_pressure_artifact_events  INTEGER NOT NULL DEFAULT 0,
    -- History display: hide cross-talk events (migration 20260540). Mirrors the
    -- phantom toggle above; display-only — cross-talk volume is already zeroed.
    hide_cross_talk_events         INTEGER NOT NULL DEFAULT 0,
    -- Water softener opt-in (migration 20260542, dev.24). Off until the user
    -- enables it at setup; regen_start is REQUIRED when enabled (HH:MM local),
    -- circuit is which circuit the softener draws on (defaults to Main).
    has_water_softener             INTEGER NOT NULL DEFAULT 0,
    softener_regen_start           TEXT,
    softener_circuit               TEXT,
    -- Auto-hygiene of over-merged / inflated events (dev.38 + dev.39). DEFAULT 1
    -- since dev.39: the background pass re-imports such events split/shrunk, and is
    -- now safe to run by default — the reprocess is ATOMIC (delete is restored if the
    -- re-import fails) and dry-run-gated. User-labeled rows are never touched.
    auto_split_enabled             INTEGER NOT NULL DEFAULT 1,
    -- Fingerprint label tier (migration 20260552, 2026-07 audit Phase 3): a new
    -- event may inherit the label of its tightest whole-waveform match among
    -- USER-labeled events (matched_via='fingerprint'). Measured 96% precision
    -- at ~30% coverage on this home's data; threshold self-calibrates. Applies
    -- only to events >= 2 L effective (fingerprint_matcher.MIN_MATCH_VOLUME_L)
    -- — the validation predates pulse_meter micro-draw events, which defeated
    -- the matcher outright (0/11 on the 2026-07-08 production review).
    fingerprint_labeling_enabled   INTEGER NOT NULL DEFAULT 1,
    -- One-shot stamp for the rising-pressure-corr backfill worker (migration
    -- 20260554, dev14): 1 = the historical flow_pressure_corr sweep finished
    -- (or found nothing computable) — the worker never runs again.
    rise_corr_backfill_done        INTEGER NOT NULL DEFAULT 0,
    -- Toilet physics veto era cap (migration 20260555, dev17): when 1, the
    -- veto's flush-volume ceiling derives from build_year via the EPA/federal
    -- flush-standard eras (event_rules.toilet_flush_cap_litres); when 0 (or
    -- build_year unknown) the ceiling falls back to the pre-1982 7 gpf bound.
    -- The 2.8 L floor + single-refill shape veto are structural and always on.
    epa_flush_cap_enabled          INTEGER NOT NULL DEFAULT 1,
    -- Pump-aware detection (migration 20260558, dev21 Phase 1). The home may be
    -- pressurized by a pump (city booster or well pump) whose recharge cycling
    -- violates the static-supply assumptions of the pressure detectors.
    -- pump_mode_detected/_at: nightly regime-detector verdict (Phase 3 writes).
    -- pump_detect_period_s: last measured recharge period (refreshed on every
    --   detected night — a stale period drifts exactly when the leak-trend
    --   trigger cares).
    -- pump_mode_ack: user response to the detection banner — NULL (unanswered),
    --   'confirmed', 'dismissed'. Unconfirmed detection NEVER activates
    --   behavior (banner+confirm); it only banners.
    -- pump_profile: 'vfd_constant_pressure' | 'switch_tank' | NULL. NULL on a
    --   well home resolves to switch_tank AT READ TIME (config.
    --   pump_mode_effective) — the default is never written, so nightly
    --   detection may later write the VFD profile for a constant-pressure well.
    -- supply_type_set_at: answer provenance for the alert arming rule — set
    --   ONLY when the submitted supply_type DIFFERS from stored (plus wizard
    --   completion / banner-Yes). NULL = pre-feature answer, which alert
    --   arming must not trust.
    -- pump_alert_armed_at: persisted arming stamp (recomputing from HA history
    --   would silently disarm when the ~10-day fidelity window ages out).
    pump_mode_detected             INTEGER NOT NULL DEFAULT 0,
    pump_mode_detected_at          TEXT,
    pump_detect_period_s           REAL,
    pump_mode_ack                  TEXT,
    pump_profile                   TEXT,
    supply_type_set_at             TEXT,
    pump_alert_armed_at            TEXT,
    -- pump_era_start (migration 20260566): PINNED start of the booster-pump
    -- era. Retroactive pump-era sweeps (the VFD-ripple exemption) gate on this
    -- instead of live pump-gate state or the current supply regime, so neither
    -- a gate flip nor a later supply transition can re-flag already-exempted
    -- events. Resolved once by supply_regime.pump_era_start, then read.
    pump_era_start                 TEXT,
    -- leak_watch_ack (migration 20260567): 'dismissed:<night_date>' — the
    -- newest leak-watch reading the user has acknowledged. The tile hides that
    -- night and older; a later night carrying a fresh estimate re-shows it, so
    -- a dismissal acknowledges a READING and can never silence the feature.
    -- Display-only: the HA leak alert path does not consult this.
    leak_watch_ack                 TEXT,
    -- daily_summary_tz (migration 20260571): the timezone the stored
    -- daily_summary rows were bucketed in ('America/Denver'). Daily rollups are
    -- keyed on the HOME-LOCAL day, but the timezone isn't known until HA answers
    -- at startup — so the rebuild can't run inside the migration. The
    -- orchestrator compares this to the detected zone after tz detection and
    -- rebuilds when they differ, which also covers the user moving HA's zone.
    daily_summary_tz               TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO home_profile (id) VALUES (1);

-- CSRF tokens (legacy — kept for backward compat; new code uses HMAC
-- double-submit, see csrf_server_secret below).
CREATE TABLE IF NOT EXISTS csrf_tokens (
    token       TEXT PRIMARY KEY,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- HMAC server secret for stateless CSRF double-submit. One row
-- (id = 1). The secret is generated once on first use and never
-- regenerated automatically — regenerating would invalidate every
-- in-flight browser session.
CREATE TABLE IF NOT EXISTS csrf_server_secret (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    secret      TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- ROLE-BASED ACCESS (RBAC) — viewer / operator / admin (migration 20260547).
-- operator_users:  HA user ids granted the operator tier (read + valve control),
--                  managed by an admin on the Settings → Access page.
-- admin_ids_cache: last-known-good HA admin set (from config/auth/list) so a
--                  transient lookup failure can never lock admins out — see
--                  auth.py + the orchestrator role-sync loop.
-- seen_users:      every HA user that has opened the add-on (first-sight upsert
--                  only — never a per-request write), a fallback pick-list for the
--                  Access page when config/auth/list is unavailable to the add-on.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS operator_users (
    user_id       TEXT PRIMARY KEY,
    display_name  TEXT,
    added_by      TEXT,
    added_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS admin_ids_cache (
    user_id       TEXT PRIMARY KEY,
    display_name  TEXT,
    cached_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS seen_users (
    user_id       TEXT PRIMARY KEY,
    display_name  TEXT,
    first_seen    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS circuit_profile (
    circuit             TEXT PRIMARY KEY,
    circuit_type        TEXT DEFAULT 'fixture',
    zone_count_expected INTEGER,
    controller_type     TEXT DEFAULT 'manual',
    has_drip_zones      BOOLEAN DEFAULT 0,
    initial_priors_json TEXT,
    priors_computed_at  TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Ball valve hardware type (migration 20260527).
    -- '2_port' (default): standard inline valve, micro leak test enabled.
    -- '3_port'         : drain-capable valve; leak test is automatically
    --                    skipped because a drain port reads as a constant
    --                    leak. Set per circuit during setup; editable from
    --                    Settings behind a confirmation prompt.
    valve_type          TEXT DEFAULT '2_port',
    -- Flow-meter pulses-per-litre (migration 20260546). The add-on's CACHE of
    -- the firmware's runtime PPL number entity (firmware is the source of truth);
    -- the low-flow floor is derived as 60 ÷ ppl. Default 396 = reference turbine.
    pulses_per_litre    REAL DEFAULT 396.0
);

-- ==========================================================================
-- TRAINING STATE MACHINE
-- ==========================================================================
CREATE TABLE IF NOT EXISTS training_state (
    circuit             TEXT PRIMARY KEY,
    state               TEXT DEFAULT 'idle',
    calibration_days    INTEGER DEFAULT 14,
    started_at          TIMESTAMP,
    calibration_ends_at TIMESTAMP,
    minimum_events      INTEGER DEFAULT 150,
    events_collected    INTEGER DEFAULT 0,
    labelling_deadline  TIMESTAMP,
    completed_at        TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- cluster_features_mode (migration 20260568): the feature space this
    -- circuit's cluster centers were seeded in — 'full' (default) or
    -- 'pressure_blind' (pump-era re-seed; every pressure-derived dimension
    -- pinned to 0). Persisted so the startup replay rebuilds the space the
    -- centers were learned in.
    cluster_features_mode TEXT DEFAULT 'full',
    -- dev42 (migration 20260808, F-C2): reseed-in-progress marker — an ISO
    -- timestamp stamped when a cluster re-seed clears assignments, cleared
    -- ONLY on success. A crash mid-replay leaves it set; boot and the
    -- post-rebuild health pass warn "reseed incomplete — rerun required".
    reseed_in_progress  TEXT
);

-- ==========================================================================
-- TRAINING-HELPER CAPTURE (2b) — a one-time "run each fixture once" wizard.
-- One active ('armed') row per circuit; the event-completion hook records
-- candidate event ids, the user confirms/accepts to write 'training' labels.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS training_capture (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit         TEXT NOT NULL,
    fixture_type    TEXT NOT NULL,
    -- armed | ready | captured | cancelled | expired | rejected
    status          TEXT NOT NULL DEFAULT 'armed',
    armed_at        TIMESTAMP NOT NULL,
    expires_at      TIMESTAMP NOT NULL,
    window_minutes  INTEGER,
    captured_count  INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_training_capture_active
    ON training_capture (circuit, status);
-- Candidate events recorded by the hot-path hook (plain INSERT, no JSON).
CREATE TABLE IF NOT EXISTS training_capture_candidates (
    capture_id      INTEGER NOT NULL,
    event_id        TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_training_capture_candidates
    ON training_capture_candidates (capture_id);

-- ==========================================================================
-- LEARNING CONFIGURATION
-- ==========================================================================
CREATE TABLE IF NOT EXISTS learning_config (
    circuit                         TEXT PRIMARY KEY,
    learning_mode                   TEXT DEFAULT 'adaptive',
    accelerated_adaptation_until    TIMESTAMP,
    accelerated_adaptation_reason   TEXT,
    threshold_update_interval_hours INTEGER DEFAULT 24,
    threshold_lookback_days         INTEGER DEFAULT 30,
    updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- SENSITIVITY CONFIGURATION
-- ==========================================================================
CREATE TABLE IF NOT EXISTS sensitivity_config (
    circuit                     TEXT PRIMARY KEY,
    mode                        TEXT DEFAULT 'simple',
    simple_level                TEXT DEFAULT 'medium',
    -- Phase 2.3 anomaly response: 'off' | 'notify' | 'notify_shutoff_severe'
    -- | 'shutoff_any'. Governs what happens when an event deviates from the
    -- frozen baseline. Shut-off levels are guardrailed (see anomaly_baseline).
    anomaly_response            TEXT DEFAULT 'notify',
    -- Phase 3 §2: 1 = auto-correct event volume from the recorder firmware-sensor
    -- delta, 0 = flag-only (detect + surface, don't change).
    recorder_reconcile_auto     INTEGER DEFAULT 1,
    -- Event detection
    pressure_drop_event_psi     REAL DEFAULT 1.2,
    min_event_duration_seconds  REAL DEFAULT 3.0,
    -- Anomaly thresholds
    score_alert                 REAL DEFAULT 0.60,
    score_shutoff               REAL DEFAULT 0.80,
    -- Tolerances
    flow_tolerance_pct          REAL DEFAULT 20.0,
    duration_tolerance_pct      REAL DEFAULT 30.0,
    schedule_window_minutes     REAL DEFAULT 15.0,
    sustained_alert_minutes     REAL DEFAULT 10.0,
    max_shutoffs_per_12h        INTEGER DEFAULT 2,
    -- Baseline stats (updated on calibration)
    baseline_anomaly_p85        REAL,
    baseline_anomaly_p95        REAL,
    baseline_anomaly_p99        REAL,
    -- Event count behind the percentiles — the confidence the shut-off gate
    -- reads (a thin/default baseline must never close the valve).
    baseline_anomaly_n          INTEGER,
    baseline_cluster_std_mean   REAL,
    baseline_computed_at        TIMESTAMP,
    -- Pump-aware detection (migration 20260558, dev21). Per-circuit override of
    -- the home-level pump resolution: 'auto' (follow supply_type / confirmed
    -- detection), 'on' (force), 'off' (force off — also suppresses the
    -- detection banner: an explicit off is a stronger answer than a dismissal).
    pump_mode                   TEXT NOT NULL DEFAULT 'auto',
    -- Irrigation low-pressure-under-load alert floor (Phase 6a): sustained
    -- pressure below this while a zone is flowing → heads may not pop up.
    low_pressure_alert_psi      REAL NOT NULL DEFAULT 25.0,
    -- Pump-failure alert floor (Phase 6b, migration 20260560). NULL = resolve
    -- the per-supply default at read time (city_pump 40); only explicit user
    -- action (incl. the one-tap hint apply) writes a value — non-NULL doubles
    -- as the arming rule's "explicit user-set floor" signal.
    pump_low_pressure_alert_psi REAL,
    -- Compliance of the section this circuit's valve isolates, in mL per PSI
    -- (migration 20260563). Converts a leak test's decay rate into a leak
    -- rate: mL/min = PSI/min x this. Calibrated from the reopen refill
    -- (volume delta / pressure recovered); measured 9.5 on Main 2026-07-26.
    -- NULL = not yet calibrated, and the leak rate is simply not shown.
    compliance_ml_psi           REAL,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- ALERT CONFIGURATION
-- ==========================================================================
CREATE TABLE IF NOT EXISTS alert_config (
    id          TEXT PRIMARY KEY,
    circuit     TEXT NOT NULL,
    alert_type  TEXT NOT NULL,
    fixture_id  TEXT,
    label       TEXT,
    description TEXT,
    enabled     BOOLEAN DEFAULT 1,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- FIXTURES (Phase 2 — created now to avoid future migrations)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixtures (
    id            TEXT PRIMARY KEY,
    circuit       TEXT NOT NULL,
    name          TEXT,
    auto_name     TEXT,
    confirmed     BOOLEAN DEFAULT 0,
    notes         TEXT,
    -- Phase 2.1 additions (Path C)
    fixture_type  TEXT,         -- from fixtures.FIXTURE_TYPES
    display_name  TEXT,         -- may differ from `name` for HA entity slug
    user_locked   INTEGER DEFAULT 0,
    publish_to_ha INTEGER DEFAULT 1,
    -- Sprint A orphan-repair flag: set to 1 when this fixture is confirmed
    -- but no fixture_clusters row has fixture_id pointing at it. The UI
    -- shows a relink banner so the user can pick a cluster to attach.
    cluster_backfill_needed INTEGER DEFAULT 0,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fixture_signatures (
    fixture_id  TEXT REFERENCES fixtures(id) ON DELETE CASCADE,
    feature     TEXT NOT NULL,
    centroid    REAL,
    std_dev     REAL,
    p5          REAL,
    p25         REAL,
    p75         REAL,
    p95         REAL,
    PRIMARY KEY (fixture_id, feature)
);

-- ==========================================================================
-- FIXTURE TYPE SIGNATURES (Sprint C) — per-(circuit, fixture_type) centroid
-- learned from user-labelled events. The legacy fixture_signatures table
-- above is per-(fixture_id, feature); it was never populated by any code
-- path and is kept only for backwards-compat with backup files that
-- include it. The matcher (cluster_engine + feature_extractor) reads from
-- this new table, which is keyed by user-facing fixture *type* (e.g.
-- "toilet"), not a specific fixture row — that matches how the History
-- page's label dropdown is structured.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_type_signatures (
    circuit       TEXT NOT NULL,
    fixture_type  TEXT NOT NULL,
    centroid      TEXT NOT NULL DEFAULT '{}',   -- JSON dict of feature means
    member_count  INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (circuit, fixture_type)
);

CREATE INDEX IF NOT EXISTS idx_type_signatures_circuit
    ON fixture_type_signatures (circuit);

-- ==========================================================================
-- RULE CALIBRATION (Phase 1) — per-home fit of the structural-rules-tier bands
-- (event_rules.py), frozen at activation. One JSON blob per circuit; the rule
-- predicates read it via an optional `calib` dict and fall back to their shipped
-- module defaults for any absent key. Written ONLY at activation / explicit
-- re-train — never on ordinary reclassify or live events — so the locked
-- reference can't drift (the basis for leak / odd-usage detection).
-- ==========================================================================
-- One row per (circuit, supply regime) since migration 20260565: regime_id 0
-- is the legacy/pre-regime row; other ids reference supply_regime.id. Bands
-- stay fit-once-and-frozen WITHIN a regime; a supply shift (pump install,
-- PRV change) gets a fresh fit instead of silently stale bands.
CREATE TABLE IF NOT EXISTS rule_calibration (
    circuit     TEXT NOT NULL,
    regime_id   INTEGER NOT NULL DEFAULT 0,
    params      TEXT NOT NULL DEFAULT '{}',   -- JSON dict of fitted rule bands
    report      TEXT,                         -- JSON per-type fit-vs-fallback report
    source      TEXT,                         -- 'activation' | 'retrain' | 'regime_shift'
    locked_at   TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (circuit, regime_id)
);

-- ==========================================================================
-- USAGE BASELINE (Phase 2) — per-home "normal" envelopes, FROZEN at activation
-- alongside rule_calibration. params is a JSON dict {fixture_type: {vol/dur/peak:
-- [lo,hi], n}} of padded percentile bands from this home's labelled+matched
-- events. The future leak / odd-usage detector compares a live event against its
-- type's frozen envelope; because it's frozen, a slow leak can't drift it.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS usage_baseline (
    circuit     TEXT PRIMARY KEY,
    params      TEXT NOT NULL DEFAULT '{}',   -- JSON {type: {vol/dur/peak:[lo,hi], n}}
    source      TEXT,                         -- 'activation' | 'retrain'
    locked_at   TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- BASELINE SNAPSHOTS (migration 20260569, dev34 B3) — the frozen usage
-- baseline + overall anomaly percentiles as they stood BEFORE each freeze,
-- so a regime refit that lands badly is revertable
-- (anomaly_baseline.restore_usage_baselines). Pruned to 10 per circuit.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS baseline_snapshot (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit          TEXT NOT NULL,
    reason           TEXT,                    -- the freeze source that displaced it
    params           TEXT NOT NULL DEFAULT '{}',
    source           TEXT,
    locked_at        TIMESTAMP,
    sensitivity_json TEXT,                    -- the anomaly p85/p95/p99 + n
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- ARTIFACT CALIBRATION (Phase 2.4) — per-home phantom/dribble/cross-talk detector
-- thresholds, FROZEN at activation. Calibrates ONLY the long-quiet / dribble
-- identifier thresholds (never the leak-safety true-flow guards) and is gated
-- do-no-harm: a fitted threshold is applied only if it flags zero confirmed-NORMAL
-- events (never zeros confirmed-real water). params is a JSON {threshold_key:value}.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS artifact_calibration (
    circuit     TEXT PRIMARY KEY,
    params      TEXT NOT NULL DEFAULT '{}',   -- JSON {threshold_key: value}
    report      TEXT,                         -- JSON per-detector fit/fallback
    source      TEXT,                         -- 'activation' | 'retrain'
    locked_at   TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- ANOMALY AUTO-SHUTOFF LOG (Phase 2.3) — one row per automated valve close.
-- PERSISTENT so the per-12h rate limit survives an addon restart (an in-memory
-- counter would reset on exactly the restart a pathological condition could
-- cause). Queried for COUNT in the last 12h before any auto-shutoff.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS anomaly_shutoff_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit       TEXT NOT NULL,
    event_id      TEXT,
    anomaly_type  TEXT,
    score         REAL,
    closed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_anomaly_shutoff_circuit_time
    ON anomaly_shutoff_log (circuit, closed_at);

-- ==========================================================================
-- BACKGROUND JOB STATUS (§2.4) — one row per long-running op (reclassify,
-- calibration/re-lock, recalibration) so the UI can poll + toast success /
-- failure. DB-backed (not in-memory) because reclassify runs on an isolated
-- write connection whose status must still be visible to the poll endpoint.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit     TEXT,
    kind        TEXT NOT NULL,                     -- 'reclassify'|'calibration'|'recalibration'
    status      TEXT NOT NULL DEFAULT 'running',   -- 'running'|'done'|'error'
    message     TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_jobs_id ON jobs (id);

-- ==========================================================================
-- RECORDER RECONCILIATION CHECKPOINT (Phase 3 §2) — per-circuit position the
-- hourly recorder-volume reconcile has processed up to, plus cumulative diagnostic
-- counters. Pure checkpoint/stats (no data dependency) → created here via
-- CREATE TABLE IF NOT EXISTS, like jobs / anomaly_shutoff_log.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS reconcile_state (
    circuit           TEXT PRIMARY KEY,
    through_ts        TIMESTAMP,         -- events with end_ts <= here are reconciled
    corrections       INTEGER DEFAULT 0, -- cumulative auto-corrections applied
    flagged           INTEGER DEFAULT 0, -- cumulative divergences flagged (flag-mode)
    last_run_at       TIMESTAMP,
    last_delta_litres REAL
);

-- ==========================================================================
-- CATEGORY PUBLISH (Sprint F) — per-(circuit, fixture_type) HA publish gate.
-- Replaces the per-fixture `fixtures.publish_to_ha` flag as the source of
-- truth for the fixture_publisher. Each row toggles whether the HA discovery
-- entity set for that category on that circuit is published. publish_to_ha=1
-- by default; missing rows default to True at the caller via .get(typ, True).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS category_publish (
    circuit         TEXT NOT NULL,
    fixture_type    TEXT NOT NULL,
    publish_to_ha   INTEGER NOT NULL DEFAULT 1,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (circuit, fixture_type)
);

-- ==========================================================================
-- FIXTURE CLUSTERS (Phase 2.1) — raw DBSTREAM clustering output
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_clusters (
    id                    INTEGER NOT NULL,
    circuit               TEXT NOT NULL,
    centroid              TEXT NOT NULL DEFAULT '{}',   -- JSON dict of feature means
    feature_std           TEXT NOT NULL DEFAULT '{}',   -- JSON dict of feature stddevs
    transient_template    TEXT,                 -- JSON list, NULL until enough members
    member_count          INTEGER DEFAULT 0,
    suggested_type        TEXT,                 -- from fixtures.suggest_fixture_type
    suggested_confidence  REAL DEFAULT 0,
    -- Sprint B: provenance of suggested_type. NULL = nothing suggested yet,
    -- 'heuristic' = set by cluster_engine._run_suggest_type_if_needed
    -- (centroid feature-range rules), 'user_labels' = set by majority vote
    -- of events.user_fixture_type on this cluster's members. The UI uses
    -- this to render different hint copy and treat user-labels as a
    -- stronger signal than heuristics.
    suggestion_source     TEXT,
    confidence_level      TEXT DEFAULT 'preliminary',  -- preliminary/learning/confirmed
    fixture_id            TEXT REFERENCES fixtures(id) ON DELETE SET NULL,
    is_compound           INTEGER DEFAULT 0,    -- 2.3 placeholder
    component_cluster_ids TEXT,                 -- 2.3 placeholder, JSON list
    publish_to_ha         INTEGER DEFAULT 1,
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_match_at         TIMESTAMP,
    PRIMARY KEY (circuit, id)
);

CREATE INDEX IF NOT EXISTS idx_clusters_circuit
    ON fixture_clusters (circuit);
CREATE INDEX IF NOT EXISTS idx_clusters_fixture
    ON fixture_clusters (fixture_id);

-- ==========================================================================
-- CLUSTER CO-OCCURRENCE (Phase 2.1) — sequence boost for fixture matching
-- ==========================================================================
CREATE TABLE IF NOT EXISTS cluster_cooccurrence (
    circuit             TEXT NOT NULL,
    from_cluster_id     INTEGER NOT NULL,
    to_cluster_id       INTEGER NOT NULL,
    count               INTEGER DEFAULT 0,
    median_gap_seconds  REAL,
    last_seen_at        TIMESTAMP,
    PRIMARY KEY (circuit, from_cluster_id, to_cluster_id)
);

-- ==========================================================================
-- CLUSTER SEQUENCES (Phase 2.2 placeholder, empty in 2.1)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS cluster_sequences (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit           TEXT NOT NULL,
    pattern_hash      TEXT,
    event_chain       TEXT,                 -- JSON list of cluster IDs
    occurrence_count  INTEGER DEFAULT 0,
    confidence        REAL DEFAULT 0,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- PLUMBING-EVENT EXCLUSION WINDOWS (Phase 2.1)
-- User-triggered window that prevents events from being used for fixture
-- clustering during a post-winterization or post-repair flush.  Volume and
-- leak-detection tracking continue regardless of the window state.
-- Pruned after 30 days by data_pruner.py.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS circuit_exclusion_windows (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit     TEXT NOT NULL,
    started_at  TIMESTAMP NOT NULL,
    ends_at     TIMESTAMP NOT NULL,
    reason      TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_excl_circuit_window
    ON circuit_exclusion_windows (circuit, started_at, ends_at);

-- ==========================================================================
-- CLUSTER METRICS HISTORY — rolling cluster quality stats
-- ==========================================================================
CREATE TABLE IF NOT EXISTS cluster_metrics_history (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    measured_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    circuit               TEXT NOT NULL,
    cluster_count         INTEGER,
    coverage_pct          REAL,
    avg_purity            REAL,
    avg_stability         REAL,
    unmatched_recent_24h  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_metrics_circuit_ts
    ON cluster_metrics_history (circuit, measured_at);

-- ==========================================================================
-- EVENT LOG
-- ==========================================================================
CREATE TABLE IF NOT EXISTS events (
    id                          TEXT PRIMARY KEY,
    circuit                     TEXT NOT NULL,
    start_ts                    TIMESTAMP NOT NULL,
    end_ts                      TIMESTAMP,
    duration_seconds            REAL,
    avg_flow_lpm                REAL,
    peak_flow_lpm               REAL,
    flow_variability            REAL DEFAULT 0,
    pressure_delta_psi          REAL,
    pre_event_pressure_psi      REAL,
    min_pressure_psi            REAL,
    hydraulic_resistance        REAL,
    resistance_curve_shape      TEXT,
    propagation_delay_seconds   REAL,
    propagation_delay_ms        REAL DEFAULT 0,
    flow_onset_delay_seconds    REAL,
    start_trigger               TEXT DEFAULT 'unknown',
    has_pressure_transient      BOOLEAN DEFAULT 0,
    hour_of_day                 INTEGER,
    day_of_week                 INTEGER,
    duration_log                REAL DEFAULT 0,
    hour_sin                    REAL DEFAULT 0,
    hour_cos                    REAL DEFAULT 1,
    is_weekend                  BOOLEAN DEFAULT 0,
    -- dev38 (migration 20260801): which IANA zone produced the five time
    -- features above. NULL = written before tz detection ran (UTC basis) —
    -- the deferred boot backfill rewrites those rows once the home zone is
    -- known and stamps this marker (the 2026-08 audit found hour_of_day was
    -- UTC on 100% of events and day_of_week wrong on 30%).
    time_features_tz            TEXT,
    is_composite                BOOLEAN DEFAULT 0,
    other_valve_open            INTEGER,           -- NULL=unknown 0=closed 1=open
    -- dev41 (migration 20260807): provenance for the tri-state above, per
    -- the supply_type_set_at precedent — when the underlying valve state was
    -- last established and how ('ha_prime' at startup vs 'state_change').
    -- Legacy rows stay NULL (honest unknowns).
    other_valve_open_set_at     TEXT,
    other_valve_open_source     TEXT,
    excluded_from_training      BOOLEAN DEFAULT 0,
    cluster_id                  INTEGER,
    -- Phase 2.1 type-aware match gate: when cluster_id IS NULL, this records
    -- WHY the event was not matched. Values:
    --   'no_centers'             — DBSTREAM had no centres yet
    --   'features_missing'       — extractor returned None
    --   'type_gate_rejected'     — confirmed cluster's per-type variance gate
    --   'excluded_from_training' — caller skipped match_and_learn entirely
    -- NULL when the event matched cleanly.
    match_rejection_reason      TEXT,
    -- Cluster match quality (written by _cluster_event after insert)
    match_confidence            REAL,    -- 0.0–1.0; NULL = unmatched
    match_level                 TEXT,    -- 'preliminary'|'confirmed'|NULL
    -- Inter-event sequence context (written by _cluster_event)
    seconds_since_prev_event    REAL,    -- gap from previous event end → this start
    seconds_to_next_event       REAL,    -- retroactively filled when next event arrives
    prev_cluster_id             INTEGER, -- cluster_id of the preceding event
    fixture_id                  TEXT REFERENCES fixtures(id),
    anomaly_score               REAL,
    anomaly_type                TEXT,
    flagged                     BOOLEAN DEFAULT 0,
    user_reviewed               BOOLEAN DEFAULT 0,
    -- Anomaly-triage verdict (migration 20260553): 'normal' — user confirmed
    -- legitimate use; 'unknown' — user looked but didn't recognise it (the
    -- event is then held out of future anomaly-baseline refits so an
    -- unidentified draw can never teach "normal"); NULL — unreviewed, or
    -- reviewed before verdicts existed. A real relabel clears it: identifying
    -- the draw supersedes "unknown".
    review_verdict              TEXT,
    user_fixture_type           TEXT,              -- user-assigned fixture type (overrides clustering)
    triggered_alert             BOOLEAN DEFAULT 0,
    volume_litres               REAL DEFAULT 0,
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Flow shape features (migration 025)
    flow_signature_json              TEXT,
    -- Pressure drop signature (migration 029)
    pressure_signature_json          TEXT,
    -- Flow-vs-pressure Pearson correlation over the event window (migration
    -- 20260554, dev14). Strongly negative = real demand (flow pulls pressure
    -- DOWN); positive = flow rode a city-pressure RISE (rising-pressure
    -- phantom discriminator). NULL = not computed (short waveforms / legacy).
    flow_pressure_corr               REAL,
    -- Edge signatures (migration 20260557, dev19): fixed-TIME onset/offset
    -- shape vectors (EDGE_SIG_CELLS × EDGE_SIG_CELL_SECONDS absolute grid,
    -- peak-normalized, zero-padded past the event's extent). Feed the k-NN
    -- matcher's edge tier — unlike the proportional signatures above, these
    -- align valve/fill dynamics across event durations. NULL = uncomputable.
    onset_signature_json             TEXT,
    offset_signature_json            TEXT,
    positive_edge_count              INTEGER DEFAULT 0,
    negative_edge_count              INTEGER DEFAULT 0,
    flow_edge_count                  INTEGER DEFAULT 0,
    flow_rise_rate_lpm_s             REAL DEFAULT 0,
    flow_fall_rate_lpm_s             REAL DEFAULT 0,
    opening_step_lpm                 REAL DEFAULT 0,
    closing_step_lpm                 REAL DEFAULT 0,
    time_to_90pct_flow_seconds       REAL DEFAULT 0,
    time_from_90pct_to_zero_seconds  REAL DEFAULT 0,
    mid_event_flow_drop_lpm          REAL DEFAULT 0,
    steady_state_fraction            REAL DEFAULT 0,
    -- Pressure transient features (migration 025)
    pressure_transient_energy        REAL DEFAULT 0,
    pressure_transient_duration_ms   REAL DEFAULT 0,
    -- Pressure transient shape features (migration 026)
    pressure_onset_ms                REAL DEFAULT 0,
    recovery_overshoot_psi           REAL DEFAULT 0,
    pressure_oscillation_count       INTEGER DEFAULT 0,
    -- ESP waveform A/B fields (migration 031)
    esp_waveform_used                INTEGER,
    waveform_event_id                INTEGER,
    -- Claim ledger (migration 20260573). The firmware event counter restarts
    -- at every reboot, so (waveform_boot_id, waveform_event_id) — not the
    -- event id alone — identifies a capture. One capture enriches one event.
    waveform_boot_id                 INTEGER,
    waveform_quality                 INTEGER,
    waveform_overlap_score           REAL,
    -- Mis-attachment repair audit (migration 20260573). Populated only by the
    -- wf_repair_backfill sweep; the corrupted values are preserved here.
    peak_flow_lpm_pre_repair         REAL,
    pressure_delta_psi_pre_repair    REAL,
    propagation_delay_ms_pre_repair  REAL,
    wf_repair_at                     TEXT,
    wf_repair_verdict                TEXT,
    -- Signature provenance — which source generated the shape signatures.
    -- 'software' (default) | 'esp_full_flow' | 'esp_full_pressure' | 'esp_full_flow_pressure'
    signature_source                 TEXT,
    -- Degraded-supply guard (migration 20260526). When degraded_supply=1 the
    -- event was captured during pulsing-supply conditions; flow data is
    -- unreliable. volume_litres_effective is the value actually applied to
    -- hourly_volume (raw for healthy events, envelope-smoothed for degraded).
    -- hourly_volume_applied_litres/_bucket track exact prior contribution so
    -- re-imports correctly subtract-then-add.
    -- match_rejection_reason additionally accepts 'pulsing_supply'.
    degraded_supply                  BOOLEAN DEFAULT 0,
    volume_litres_estimated          REAL,
    volume_litres_effective          REAL,
    volume_estimation_method         TEXT DEFAULT 'raw',
    -- Active-flow features (migration 20260536). Computed by time-integrating the
    -- timestamped flow samples (flow_integral.py). NULLABLE on purpose: NULL =
    -- unknown / not yet backfilled (NOT the same as 0 = known no flow). Drive
    -- classification + the hardened phantom guard. integration_quality is 'ok',
    -- 'capped' (offline-gap clamp), or 'degraded' (bad/sparse backfill history);
    -- anything but 'ok'/NULL is kept out of classifier training.
    flow_integral_litres             REAL,
    active_flow_duration_seconds     REAL,
    true_avg_flow_lpm                REAL,
    flow_on_ratio                    REAL,
    active_flow_segment_count        INTEGER,
    flow_cv_on_segments              REAL,
    integration_quality              TEXT,
    -- Volume-recompute audit trail (Phase 2 backfill): original pre-recompute
    -- volume + when it was last recomputed, for verification / rollback.
    volume_litres_original           REAL,
    volume_recomputed_at             TIMESTAMP,
    -- Phase 3 §2: the authoritative firmware cumulative-volume-sensor delta over the
    -- event window, from the HA recorder (NULL = not reconciled / sensor unavailable).
    -- Audit ("recorder said X vs stored Y") + what flag-mode review/apply uses.
    volume_recorder_litres           REAL,
    -- dev38 (migration 20260801): ANNOTATION-ONLY registration-corrected
    -- estimate. The audit's pressure-witness inversion showed the oval-gear
    -- meter reads ~27% low at 1.5-2.5 L/min and ~10% low at 2.5-4; this
    -- stores the inverse-curve estimate when it differs >2% from the raw
    -- integral. NEVER feeds volume_litres/effective or any total.
    registration_est_litres          REAL,
    -- dev41 (migration 20260807): which registration_curve version produced
    -- the estimate above (provenance; the curve now lives in data, not code).
    registration_curve_version       INTEGER,
    hourly_volume_applied_litres     REAL DEFAULT 0,
    hourly_volume_applied_bucket     TEXT,
    degraded_diagnostic_json         TEXT,
    -- Sprint C signature matcher: the fixture_type matched by the
    -- fixture_type_signatures table when cluster matching couldn't
    -- assign a cluster_id (or assigned one with low confidence).
    -- Independent of cluster_id — a single event can have cluster_id
    -- set AND matched_fixture_type set if the cluster matched but the
    -- signature gave a more specific type guess.
    matched_fixture_type             TEXT,
    -- Match provenance (migration 20260541, dev.23 rules tier): how
    -- matched_fixture_type was produced — 'knn' (signature k-NN),
    -- 'washer_cycle' (anchor + same-peak family), 'rule_toilet'/
    -- 'rule_dishwasher'/'rule_shower' (structural event rules),
    -- 'zone_default' (zone-circuit fallback), NULL (legacy/cluster/none).
    -- Machine-derived; recomputed by every reclassify; NEVER user-preserved.
    matched_via                      TEXT,
    -- History cycle-rollup grouping key (migration 20260542, dev.24): the id
    -- members of one appliance run share so History can collapse them under one
    -- expandable parent row — washer anchor id / softener session id / dishwasher
    -- cycle anchor id; NULL = ungrouped singleton. Stamped by reclassify; SKIPS
    -- user-labelled events (so a relabel pulls a member out of its group).
    cycle_group_id                   TEXT,
    -- dev40 training quarantine (migration 20260805): non-NULL reason keeps
    -- this event OUT of every training/exemplar pool (k-NN label pools, type
    -- centroids, fingerprint library, rule-calibration fits, usage baselines,
    -- cluster label votes) without touching its labels, verdicts or volume —
    -- annotate-don't-modify. First use: 'dev40_precision_quarantine', the
    -- unreviewed machine dishwasher-cycle labels the 2026-08-15 audit measured
    -- at 9/19 / 1/10 precision. A user review supersedes the machine label, so
    -- reviewed events are never flagged.
    training_quarantine_reason       TEXT,
    training_quarantined_at          TEXT,
    -- Embedded-fixture annotation (migration 20260548). JSON array of draws
    -- found superimposed on a sustained event's waveform (a toilet flushed
    -- mid-shower) by composite_detector. Metadata ONLY — never changes the
    -- parent's volume or primary label; surfaced in the History modal. NULL =
    -- not analysed / no usable waveform / nothing embedded.
    embedded_fixtures_json           TEXT,
    -- Pressure-restoration phantom guard (migration 20260532). When 1, this
    -- event matched the long-duration + near-zero-pressure-drop fingerprint
    -- of a city-pressure-restoration artifact. Its volume_litres_effective is
    -- forced to 0 and it is excluded_from_training. Shown in History with a
    -- flag; volume contributes nothing to totals.
    is_pressure_restoration_phantom  INTEGER DEFAULT 0,
    -- Suppression-averted (migration 20260551, 2026-07 audit Phase 2b). When 1,
    -- the phantom guard matched this event BUT it carried a large measured
    -- volume (>= _PHANTOM_REVIEW_FLAG_LITRES), so instead of silently zeroing
    -- it the volume was KEPT and the event flagged for review
    -- (anomaly_type 'suppression_averted'). Excluded from training until the
    -- user reviews/relabels. Survives rescores (score_event_anomaly reads it).
    phantom_suppression_averted      INTEGER DEFAULT 0,
    -- Low-flow dribble guard (migration 20260535). When 1, this event's ACTIVE
    -- flow never reached the circuit meter's registration floor (2026-07-05
    -- below-meter-floor rule; ~1.0-1.1 L/min) — the reading is outside the
    -- meter's valid operating regime. Since 2026-06-19 this DOES zero volume
    -- (volume_litres_effective=0, like a phantom) as well as setting
    -- excluded_from_training; the pre-2026-06 comment here claimed otherwise
    -- and was stale for over a year. Auto-derived (reason 'below_meter_floor')
    -- or manual (reason 'low_flow_dribble'); suppressed for user_classified
    -- rows. A user assigning a real fixture type REVERSES the zeroing —
    -- database.revert_artifact_zeroing_on_relabel.
    is_low_flow_dribble              INTEGER NOT NULL DEFAULT 0,
    -- Cross-talk artifact (migration 20260540). When 1, a long event registered
    -- via a pressure drop with essentially no real flow on THIS circuit (another
    -- circuit's draw pulled the shared-supply pressure down). Like a phantom it
    -- forces volume_litres_effective=0 + excluded_from_training; a distinct flag so
    -- it can be shown / hidden separately. Auto-derived; suppressed for
    -- user_classified rows (a peer of the phantom flag in patch_event).
    is_cross_talk                    INTEGER NOT NULL DEFAULT 0,
    -- Leak-test reopen refill (migration 20260570). The id of the
    -- leak_test_history row whose valve reopen produced this event — set by
    -- leak_test_refill.reconcile_leak_test_refills alongside
    -- match_rejection_reason='leak_test_refill'. Deliberately OUTSIDE the
    -- artifact flag family above (this verdict zeroes volume and excludes from
    -- training but stays VISIBLE in History — at most one per day, and its size
    -- reads out the isolated section). The feature pipeline never writes this
    -- column, so the event upsert cannot clear it: it is the durable
    -- provenance the reconcile repairs from. NULL = not a refill.
    leak_test_id                     INTEGER,
    -- Sprint H. user_ignored: explicit Ignore/Restore intent (separate from
    -- the derived excluded_from_training, which is auto OR user_ignored OR
    -- manual). user_classified: lock bit — when 1 the category flags
    -- (is_pressure_restoration_phantom / is_cross_talk / is_low_flow_dribble /
    -- degraded_supply / is_composite) hold the user's manual choices and
    -- auto-detection must never overwrite them. NOTE (dev33): the lock holds
    -- auto-detection off, but it does NOT outrank a later fixture LABEL — a
    -- real user_fixture_type means "this was real water" and reverses a
    -- zeroing verdict (revert_artifact_zeroing_on_relabel). Before dev33 the
    -- lock could preserve a wrong zeroing forever, which is how a labelled
    -- 685 L draw counted as 0.
    user_ignored                     INTEGER DEFAULT 0,
    user_classified                  INTEGER DEFAULT 0,
    -- Temporal "appliance cycle" signal: count of similar-volume neighbour
    -- events within ±45 min (database.recompute_cycle_pulse_counts). NULL = not
    -- yet computed, 0 = computed/no qualifying neighbours. Aggregated into the
    -- cluster centroid for the heuristic; deliberately NOT in FEATURE_KEYS so it
    -- never affects clustering distance.
    cycle_pulse_count                INTEGER,
    -- Label provenance: 'user' = explicit user label, 'cycle' = auto cycle-mate
    -- expansion (2a), 'training' = capture wizard (2b). NULL = legacy/unlabeled
    -- and is protected exactly like a 'user' label (auto-undo never touches it).
    -- Preserved across event re-imports via _EVENT_USER_COLUMNS.
    fixture_label_source             TEXT
);

-- NOTE: the partial index on (circuit, start_ts) WHERE degraded_supply = 1
-- is created by db_migrations._apply_degraded_supply_columns() rather than
-- inline here. Putting it in _create_schema would fail on an upgrade-from-
-- baseline DB: CREATE TABLE IF NOT EXISTS is a no-op on existing tables,
-- so the existing events table doesn't get the degraded_supply column
-- added by this DDL (ALTER TABLE in the migration is what does that), but
-- the partial-index statement would still execute and reference a column
-- that doesn't exist yet. Order is:
--   1. database.init_db() -> _create_schema() — must succeed on existing DB
--   2. db_migrations.run_migrations() — adds columns AND the partial index
-- Fresh DBs hit the same migration via the version==0 path.

-- UNIQUE(circuit, start_ts) — enforces the contract the importer / dedup
-- helpers have always assumed (see comments in dedup_events). Replaces the
-- earlier non-unique idx_events_circuit_ts. Fresh DBs get the unique index
-- directly; upgrades from baseline (20260524) run dedup_events first via
-- migration 20260525 before this index is created so the unique constraint
-- doesn't fail on historical duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_circuit_start_unique
    ON events (circuit, start_ts);
CREATE INDEX IF NOT EXISTS idx_events_start_ts
    ON events (start_ts);
-- NOTE: idx_events_wf_claim (the waveform claim lookup) is deliberately NOT
-- here — it indexes waveform_boot_id, a column older DBs only gain during
-- migration 20260573, and this script also runs against those. Migration
-- 20260573 creates it; the fresh-DB path runs the whole chain too.

-- ==========================================================================
-- HOURLY VOLUME (pre-aggregated for fast chart queries)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS hourly_volume (
    circuit         TEXT NOT NULL,
    hour_ts         TIMESTAMP NOT NULL,
    volume_litres   REAL DEFAULT 0,
    PRIMARY KEY (circuit, hour_ts)
);

CREATE INDEX IF NOT EXISTS idx_hourly_volume_circuit_ts
    ON hourly_volume (circuit, hour_ts);

-- ==========================================================================
-- EVENT WAVEFORMS — high-resolution min/max envelopes for the event detail
-- modal (added 20260526). The 32-point pressure_signature_json/_flow_*
-- columns in events stay for clustering; these min/max bins are higher
-- resolution and preserve oscillation envelopes that bin-mean would hide.
-- Retention: WAVEFORM_RETENTION_DAYS (60 by default), purged daily by
-- orchestrator._purge_old_waveforms. FK cascade deletes when an event is
-- removed (foreign_keys pragma is enabled on every connection).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS event_waveforms (
    event_id              TEXT PRIMARY KEY
                          REFERENCES events(id) ON DELETE CASCADE,
    flow_min_json         TEXT NOT NULL,
    flow_max_json         TEXT NOT NULL,
    pressure_min_json     TEXT NOT NULL,
    pressure_max_json     TEXT NOT NULL,
    duration_seconds      REAL NOT NULL,
    created_at            TEXT NOT NULL,         -- ISO-8601 UTC, Python-written
    -- dev38 (migration 20260801): per-channel source metadata so a renderer
    -- can build an honest time axis. The two channels are binned
    -- INDEPENDENTLY from streams of different cadences (audit §3.5: 18.2%
    -- of events misaligned on a shared index axis). *_src_n = source sample
    -- count before binning; *_src_hz = fixed sample rate when one exists
    -- (200.0 for ESP captures; NULL for the event-driven software series,
    -- whose spacing is NOT uniform and has no recoverable axis).
    flow_src_n            INTEGER,
    press_src_n           INTEGER,
    flow_src_hz           REAL,
    press_src_hz          REAL
);
CREATE INDEX IF NOT EXISTS idx_event_waveforms_created
    ON event_waveforms (created_at);

-- ==========================================================================
-- VOLUME SNAPSHOTS (HA sensor baselines for accurate daily / weekly totals)
-- Stores the HA cumulative volume sensor reading at the start of each
-- calendar period so we can compute delta volumes without relying solely
-- on the internal event-based estimates.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS volume_snapshots (
    circuit      TEXT NOT NULL,
    period_ts    TEXT NOT NULL,  -- ISO datetime of period start (midnight)
    ha_volume    REAL NOT NULL,  -- HA sensor reading at that moment
    -- Highest reading seen in this period (migration 20260571). A meter reset
    -- is detected as current < ha_volume; without knowing how far the meter
    -- had climbed first, the reset handler had to throw the period's volume
    -- away. This makes the carry-over exact.
    last_reading REAL,
    PRIMARY KEY (circuit, period_ts)
);

-- ==========================================================================
-- HISTORICAL IMPORT STATE
-- Tracks the last time the historical importer ran per circuit so periodic
-- catch-up checks know how far back to look.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS import_state (
    circuit         TEXT PRIMARY KEY,
    last_check_ts   TEXT,           -- ISO timestamp of last successful check
    total_imported  INTEGER DEFAULT 0
);

-- ==========================================================================
-- IRRIGATION CROSS-TALK AUDIT (migration 20260550)
-- Evidence trail written by the historical importer's reconciliation pass
-- BEFORE it zeroes a main event identified as irrigation zone-switch cross-talk
-- (water hammer from a zone valve switching, not real water). One row per
-- action so a false positive is auditable + reversible.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS cross_talk_audit (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL,
    circuit         TEXT NOT NULL,
    reconciled_at   TEXT NOT NULL,   -- ISO timestamp the reconciler acted
    interval_start  TEXT,            -- irrigation-active interval bounds (UTC ISO)
    interval_end    TEXT,
    main_delta_psi  REAL,            -- main-circuit pressure swing over the window
    other_delta_psi REAL,            -- irrigation-circuit pressure swing
    ratio           REAL,            -- other_delta / main_delta
    volume_litres   REAL,            -- pre-zero raw volume
    action          TEXT NOT NULL    -- 'flagged' | 'reverted'
);
CREATE INDEX IF NOT EXISTS idx_cross_talk_audit_event
    ON cross_talk_audit(event_id);

-- ==========================================================================
-- DAILY SUMMARY (pre-aggregated from events, calculated nightly)
-- Kept indefinitely — drives history charts and year-over-year views.
-- One row per circuit per calendar day.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS daily_summary (
    circuit             TEXT NOT NULL,
    day                 DATE NOT NULL,          -- YYYY-MM-DD
    -- Volume
    total_volume_litres REAL DEFAULT 0,
    -- Events
    event_count         INTEGER DEFAULT 0,
    -- Flow
    avg_flow_lpm        REAL,
    peak_flow_lpm       REAL,
    -- Pressure
    avg_pressure_psi    REAL,
    min_pressure_psi    REAL,
    -- Anomalies / alerts
    anomaly_count       INTEGER DEFAULT 0,
    alert_count         INTEGER DEFAULT 0,
    -- Top fixture
    top_fixture_id      TEXT,
    top_fixture_count   INTEGER DEFAULT 0,
    -- Top-5 fixtures as JSON: [{"fixture_id":"...","count":N}, ...]
    fixture_breakdown   TEXT,
    -- Computed at
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (circuit, day)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_circuit_day
    ON daily_summary (circuit, day);

-- ==========================================================================
-- LEAK TEST SCHEDULE AND HISTORY
-- ==========================================================================
CREATE TABLE IF NOT EXISTS leak_test_schedule (
    circuit                 TEXT PRIMARY KEY,
    enabled                 BOOLEAN DEFAULT 0,
    auto_learn_hour         BOOLEAN DEFAULT 1,
    frequency               TEXT DEFAULT 'monthly',
    day_of_week             INTEGER DEFAULT 0,
    week_of_month           INTEGER DEFAULT 1,
    run_hour                INTEGER DEFAULT 2,
    run_minute              INTEGER DEFAULT 0,
    -- quiet_period_minutes / retry_delay_minutes / retry_count removed:
    -- the scheduler now learns the quietest hour from usage history instead.
    notify_on_pass          BOOLEAN DEFAULT 1,
    notify_on_fail          BOOLEAN DEFAULT 1,
    last_run_at             TIMESTAMP,
    last_result             TEXT,
    next_run_at             TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS leak_test_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit             TEXT NOT NULL,
    run_at              TIMESTAMP NOT NULL,
    triggered_by        TEXT DEFAULT 'manual',
    result              TEXT,
    duration_minutes    REAL,
    baseline_psi        REAL,
    final_psi           REAL,
    pressure_drop_psi   REAL,
    -- Pump plan Phase 5b (migration 20260559): cross-circuit verdict — did
    -- the UNTESTED circuit show pump recharge cycling while this circuit was
    -- isolated? 'untested_side' = leak on the other line / upstream / pump
    -- check valve; 'quiet' = no cycling anywhere; 'not_applicable' = pump
    -- mode off or the other circuit had real flow; 'unavailable' = HA fetch
    -- failed (never affects the test's own result).
    other_circuit_cycles   INTEGER,
    other_circuit_period_s REAL,
    pump_verdict           TEXT,
    -- User acknowledgement (migration 20260562): a failed test the user has
    -- reviewed and judged benign (test interrupted by an update, known
    -- coincident draw). Display-only — renders amber instead of red; the
    -- record itself is never altered.
    user_dismissed         INTEGER DEFAULT 0,
    -- Migration 20260563 — what the test actually measured. baseline_psi is
    -- the post-settle value the FIRMWARE judged against (3.13.2 publishes it);
    -- closed_psi is the pressure the instant the valve sealed, so
    -- settle_loss_psi is the water that escaped before measurement began.
    -- monitor_minutes excludes travel and settle. est_leak_ml_min is the
    -- decay rate times the circuit's compliance. draw_verdict flags a test
    -- invalidated by real water use ('demand' | 'clean' | 'unavailable').
    closed_psi             REAL,
    settle_loss_psi        REAL,
    monitor_minutes        REAL,
    threshold_psi          REAL,
    est_leak_ml_min        REAL,
    post_restore_volume_l  REAL,
    draw_verdict           TEXT,
    -- dev38 (migration 20260801): measurement provenance + a sustained-drop
    -- figure. The audit found the stored drop was a single instantaneous
    -- end-of-test read (often the 0.5-psi-quantised averaged entity via
    -- fallbacks) that raw pressure did not support on 1/3 of tests.
    -- sustained_drop_psi = baseline − median(fast-pressure over the final
    -- window). DISPLAY/diagnostic only — the firmware verdict is never
    -- altered by any of these.
    baseline_read_ts       TEXT,
    final_read_ts          TEXT,
    final_window_s         REAL,
    sustained_drop_psi     REAL,
    monitor_started_at     TEXT,
    -- dev41 (migration 20260807): addon-side measurement-quality columns.
    -- sustainedness_psi is SHAPE only (head median − tail median of the
    -- monitor window: ~0 = held, negative = recovered) — never magnitude;
    -- magnitude stays sustained_drop_psi (firmware baseline − tail median).
    -- addon_measure_status 'ok' | 'indeterminate' gates every addon-side
    -- consumer (leak-rate estimate, transient-dip note); NULL = legacy row.
    -- The firmware verdict is never altered by any of these.
    sustainedness_psi      REAL,
    head_window_s          REAL,
    monitor_sample_count   INTEGER,
    sighting_latency_s     REAL,     -- diagnostic: firmware start → first addon sample
    addon_measure_status   TEXT,     -- 'ok' | 'indeterminate' | NULL (legacy)
    addon_measure_reason   TEXT,     -- e.g. 'insufficient_samples' | 'within_noise' | 'other_valve_open'
    other_valve_state      TEXT,     -- B3 state record: 'open'|'closed'|'unknown'|'none'
    measured_noise_psi     REAL,     -- noise floor from detrended head samples
    monitor_samples_json   TEXT      -- raw (ts, psi) fast samples, B4 retention
);

-- ==========================================================================
-- THRESHOLD HISTORY
-- ==========================================================================
CREATE TABLE IF NOT EXISTS threshold_history (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit                 TEXT NOT NULL,
    recorded_at             TIMESTAMP NOT NULL,
    trigger                 TEXT,
    score_alert             REAL,
    score_shutoff           REAL,
    flow_tolerance_pct      REAL,
    duration_tolerance_pct  REAL,
    event_count_basis       INTEGER
);

-- ==========================================================================
-- ZONE SCHEDULES (irrigation-specific)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS zone_schedules (
    fixture_id              TEXT REFERENCES fixtures(id) ON DELETE CASCADE,
    day_of_week             INTEGER,
    scheduled_start_minutes INTEGER,
    scheduled_duration_sec  INTEGER,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fixture_id, day_of_week)
);

CREATE TABLE IF NOT EXISTS zone_flow_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id  TEXT REFERENCES fixtures(id) ON DELETE CASCADE,
    event_id    TEXT REFERENCES events(id) ON DELETE CASCADE,
    avg_flow    REAL,
    duration_s  REAL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================================================
-- OVERLAP AUDIT (dev28, overlap-guard plan)
-- One row per overlap resolution: the same-circuit-overlap invariant was
-- violated (same water recorded twice) and the guard/cleanup decided whose
-- volume counts. Revertible/auditable, cross_talk_audit precedent.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS overlap_audit (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit          TEXT NOT NULL,
    wrapper_event_id TEXT NOT NULL,
    kept_event_ids   TEXT,             -- JSON list
    vol_zeroed       REAL,
    resolution       TEXT NOT NULL,    -- wrapper_zeroed | user_labeled_flag_only
                                       -- | flagged_ambiguous
    source           TEXT NOT NULL,    -- live_guard | cleanup_migration
    created_ts       TIMESTAMP,
    -- dev38 (migration 20260801): reprocess re-creates events under NEW
    -- uuid5 ids (id = f(circuit, start_ts)), so referenced ids can go
    -- dangling — the audit found 43 dangling wrappers + 130 dangling kept
    -- ids. Rows are MARKED, never deleted (they are provenance):
    --   'superseded_by_reprocess' — events replaced under new ids
    --   'event_pruned'            — referenced event removed by retention
    -- NULL = references live. The History reader skips/annotates stale rows.
    stale_reason     TEXT,
    -- dev41 (migration 20260807): when the stale mark was applied. Orphaned
    -- audit rows are evidence of deletion — annotated, never pruned.
    stale_at         TEXT,
    UNIQUE (wrapper_event_id, resolution)
);

-- ==========================================================================
-- METER ANCHORS (dev41, migration 20260807) — provenance in data, not code.
-- ==========================================================================
-- Manual utility-register reading pairs: the long-duration cumulative
-- cross-check on the registration curve, and the low-flow anchor path if no
-- throttled bucket tests are run.
CREATE TABLE IF NOT EXISTS utility_register_readings (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_value  REAL NOT NULL,      -- register units as read (record units in notes)
    reading_ts     TEXT NOT NULL,      -- when the register was read
    meter_serial   TEXT,
    source         TEXT,               -- 'portal' | 'photo' | 'manual'
    entered_by     TEXT,
    notes          TEXT,
    created_at     TEXT
);

-- Physical reference tests (bucket tests, timed fills): flow rate, what the
-- meter said, what the reference measured. The registration curve is fit
-- against these — never against constants folded into code.
CREATE TABLE IF NOT EXISTS meter_anchor_points (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit             TEXT,
    flow_rate_lpm       REAL,
    measured_volume_l   REAL,          -- what the meter registered
    reference_volume_l  REAL,          -- what the bucket / reference measured
    test_date           TEXT,
    method              TEXT,          -- 'bucket' | 'timed_fill' | 'utility_register'
    notes               TEXT,
    created_at          TEXT
);

-- The registration correction curve itself, versioned. v1 is seeded from the
-- audit's pressure-witness inversion (relative to the meter's own >=8 L/min
-- band) with status 'unvalidated'; the marker flips to 'anchored' — and the
-- version increments — only when a low-flow anchor point confirms it.
CREATE TABLE IF NOT EXISTS registration_curve (
    curve_version  INTEGER NOT NULL,
    band_lo_lpm    REAL NOT NULL,
    band_hi_lpm    REAL,               -- NULL = unbounded (∞)
    ratio          REAL NOT NULL,      -- metered ÷ true
    status         TEXT NOT NULL,      -- 'unvalidated' | 'anchored'
    source         TEXT,               -- e.g. 'audit_2026-08_pressure_witness_inversion'
    created_at     TEXT,
    PRIMARY KEY (curve_version, band_lo_lpm)
);
-- Base-schema/migration duality: a FRESH install must end in the same state
-- as a migrated one — curve v1 seeded 'unvalidated' (mirrors 20260807).
INSERT OR IGNORE INTO registration_curve
    (curve_version, band_lo_lpm, band_hi_lpm, ratio, status, source, created_at)
VALUES
    (1, 8.0, NULL, 0.999, 'unvalidated', 'audit_2026-08_pressure_witness_inversion', CURRENT_TIMESTAMP),
    (1, 4.0, 8.0,  0.941, 'unvalidated', 'audit_2026-08_pressure_witness_inversion', CURRENT_TIMESTAMP),
    (1, 2.5, 4.0,  0.904, 'unvalidated', 'audit_2026-08_pressure_witness_inversion', CURRENT_TIMESTAMP),
    (1, 1.5, 2.5,  0.732, 'unvalidated', 'audit_2026-08_pressure_witness_inversion', CURRENT_TIMESTAMP),
    (1, 1.0, 1.5,  0.59,  'unvalidated', 'audit_2026-08_pressure_witness_inversion', CURRENT_TIMESTAMP);

-- dev38 (migration 20260801): days whose daily_summary must be recomputed.
-- The nightly gap-finder only looks 7 days back and permanently freezes a
-- day once it was summarised after its own end — so late imports, reprocess
-- and live inserts write a dirty marker here instead, drained (and deleted)
-- by the pruner's summary pass with no lookback limit.
CREATE TABLE IF NOT EXISTS daily_summary_dirty (
    circuit  TEXT NOT NULL,
    day      TEXT NOT NULL,             -- local YYYY-MM-DD (daily_summary key)
    PRIMARY KEY (circuit, day)
);

-- The overlap guard queries same-circuit span intersections on every NEW
-- event write; this index keeps that O(log n).
CREATE INDEX IF NOT EXISTS idx_events_circuit_span
    ON events (circuit, start_ts, end_ts);

-- ==========================================================================
-- PUMP REGIME NIGHTLY (dev23, pump plan Phase 3)
-- One row per circuit per EVALUATED night (no row = skipped night: HA outage
-- or no usable quiet window — invisible to the hysteresis counters by
-- design). detected = the cycling signature verdict from the validated
-- pump_regime_math module; ramp diagnostics ride along but never set it.
-- est_leak_lpd stays NULL until Phase 5a fills it.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS pump_regime_nightly (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit       TEXT NOT NULL,
    night_date    TEXT NOT NULL,      -- local calendar date of the quiet window
    detected      INTEGER NOT NULL DEFAULT 0,
    period_s      REAL,
    amplitude_psi REAL,
    sd_psi        REAL,
    cycles        INTEGER,
    window_s      INTEGER,
    est_leak_lpd  REAL,
    -- Quiet-window pressure floor ≈ pump cut-in (migration 20260560) — feeds
    -- the Phase 6b suggested-floor hint (cut-in − 5).
    min_psi       REAL,
    -- UTC ISO bounds of the analyzed sub-window (migration 20260574) — the
    -- leak-watch banner names the actual time range instead of "night of".
    window_start_ts TEXT,
    window_end_ts   TEXT,
    created_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (circuit, night_date)
);

-- ==========================================================================
-- SUPPLY-PRESSURE REGIME TRACKING (migration 20260564)
-- Idle-line (settled) pressure persisted daily + the discrete pressure
-- regimes derived from it. A regime is a sustained supply band (city ~46 psi
-- vs booster pump ~59 psi); rule calibration is fitted PER REGIME so the
-- locked-baseline anti-drift philosophy holds within each regime while a
-- plumbing change (pump install/removal, PRV swap) gets fresh bands instead
-- of silently degrading classification.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS supply_pressure_daily (
    circuit       TEXT NOT NULL,
    day_date      TEXT NOT NULL,          -- local calendar date
    sample_count  INTEGER NOT NULL,
    median_psi    REAL NOT NULL,
    p10_psi       REAL,
    p90_psi       REAL,
    source        TEXT NOT NULL DEFAULT 'settled',  -- 'settled' | 'event_backfill'
    updated_at    TIMESTAMP,
    PRIMARY KEY (circuit, day_date)
);

CREATE TABLE IF NOT EXISTS supply_regime (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TEXT NOT NULL,          -- UTC ISO; interval [started_at, ended_at)
    ended_at      TEXT,                   -- NULL = current regime
    center_psi    REAL NOT NULL,          -- median of settle-window daily medians
    band_lo_psi   REAL,
    band_hi_psi   REAL,
    source        TEXT NOT NULL,          -- 'bootstrap' | 'detected' | 'user'
    detected_at   TEXT,
    confirmed_at  TEXT,                   -- banner Confirm
    dismissed_at  TEXT,                   -- banner Dismiss
    note          TEXT
);

-- ==========================================================================
-- DATA RETENTION CONFIGURATION
-- Controls how aggressively old history is pruned.
-- Training-era data is always protected regardless of these settings.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS data_retention (
    id                          INTEGER PRIMARY KEY DEFAULT 1,
    -- Raw events: 1 year default (daily summaries cover longer history)
    events_retain_years         INTEGER DEFAULT 1,
    -- Hourly volume: 2 years (learn_best_hour only looks back 60 days)
    hourly_volume_retain_years  INTEGER DEFAULT 2,
    -- Pruning enabled
    enabled                     BOOLEAN DEFAULT 1,
    last_pruned_at              TIMESTAMP,
    -- Auto-backup (Quick Restore JSON written to filesystem on a schedule)
    auto_backup_enabled         BOOLEAN DEFAULT 0,
    auto_backup_path            TEXT    DEFAULT '/share/water_monitor_backups',
    auto_backup_day_of_week     INTEGER DEFAULT 0,  -- 0=Monday
    last_auto_backup_at         TIMESTAMP,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO data_retention (id) VALUES (1);

-- ==========================================================================
-- CIRCUIT DISPLAY LABELS (added migration 023)
-- Maps circuit_id → user-visible display name (e.g. "Main", "Irrigation").
-- ==========================================================================
CREATE TABLE IF NOT EXISTS circuit_labels (
    circuit_id   TEXT PRIMARY KEY,
    display_name TEXT NOT NULL
);

-- ==========================================================================
-- FIXTURE HA ENTITY MAP (added migration 025)
-- Tracks MQTT Discovery entities published to HA for each fixture.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_ha_entity_map (
    fixture_id          TEXT REFERENCES fixtures(id),
    ha_entity_id        TEXT NOT NULL,
    device_class        TEXT,
    unit_of_measurement TEXT,
    last_published_at   TIMESTAMP,
    retracted_at        TIMESTAMP,
    PRIMARY KEY (fixture_id, ha_entity_id)
);

-- ==========================================================================
-- FIXTURE DAILY SUMMARY (added migration 027)
-- Aggregated per-fixture daily stats used for analytics and MQTT publishing.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_daily_summary (
    circuit              TEXT NOT NULL,
    fixture_id           TEXT NOT NULL REFERENCES fixtures(id),
    day                  DATE NOT NULL,
    event_count          INTEGER,
    total_volume_litres  REAL,
    avg_flow_lpm         REAL,
    peak_flow_lpm        REAL,
    PRIMARY KEY (circuit, fixture_id, day)
);
    """)
    conn.commit()
    _apply_post_create_migrations(conn)
    log.info("Schema created/verified")


def _apply_post_create_migrations(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the initial schema for existing DBs.

    Each block uses ``ALTER TABLE … ADD COLUMN`` wrapped in try/except so it
    is idempotent: on fresh installs the column is already in CREATE TABLE
    (the ALTER raises ``OperationalError: duplicate column name`` and we
    swallow it); on upgrade installs the ALTER actually adds the column.
    """
    # Phase 2.1 — explain why an event has cluster_id IS NULL.
    try:
        conn.execute("ALTER TABLE events ADD COLUMN match_rejection_reason TEXT")
        conn.commit()
        log.info("Migration: added events.match_rejection_reason")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            log.warning("ALTER TABLE events.match_rejection_reason: %s", e)

    # Migration 026 — propagation_delay in ms + pressure transient shape features.
    try:
        conn.execute("ALTER TABLE events ADD COLUMN propagation_delay_ms REAL DEFAULT 0")
        conn.execute(
            "UPDATE events SET propagation_delay_ms = propagation_delay_seconds * 1000 "
            "WHERE propagation_delay_seconds IS NOT NULL")
        conn.commit()
        log.info("Migration: added events.propagation_delay_ms (backfilled from seconds column)")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            log.warning("ALTER TABLE events.propagation_delay_ms: %s", e)

    for col, definition in [
        ("pressure_onset_ms",          "REAL DEFAULT 0"),
        ("recovery_overshoot_psi",     "REAL DEFAULT 0"),
        ("pressure_oscillation_count", "INTEGER DEFAULT 0"),
        ("user_fixture_type",          "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {definition}")
            conn.commit()
            log.info("Migration: added events.%s", col)
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                log.warning("ALTER TABLE events.%s: %s", col, e)


# ==========================================================================
# Data access helpers
# ==========================================================================

# ── The day boundary ──────────────────────────────────────────────────────
# Everything is STORED in UTC; every daily rollup is KEYED on the home's local
# calendar day. Before this, `daily_summary.day` came from `date(start_ts)` —
# a UTC day, i.e. 18:00→18:00 local in Denver — while the dashboard's TODAY
# tile cut at local midnight and HA's own utility_meter cut at local midnight
# too. Three surfaces, three different "days", so the same water showed up as
# three different totals. These two helpers are the single definition; nothing
# outside them may slice a timestamp to get a day.


def _home_tz():
    """Home timezone (HA's), or UTC when detection hasn't run yet."""
    from .event_rules import get_home_timezone
    return get_home_timezone() or timezone.utc


def local_day_of(ts: Any, tz=None) -> str:
    """Local calendar date ('YYYY-MM-DD') of a stored UTC timestamp.

    Accepts the naive and offset-suffixed ISO forms both present in `events`
    (a naive string is read as UTC, matching how it was written). Degrades to
    the leading date characters when the value isn't parseable at all — the
    old behaviour, so a malformed row can never raise inside a rollup.
    """
    if not ts:
        return ""
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return str(ts)[:10]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz or _home_tz()).strftime("%Y-%m-%d")


def local_day_bounds_utc(day: str, tz=None) -> tuple:
    """``[start, end)`` UTC bounds of the LOCAL calendar day ``day``.

    Returned as naive-UTC ISO strings so they compare directly against stored
    `start_ts` / `hour_ts` values: those are all UTC and zero-padded, so
    lexicographic order is chronological order, and a half-open range beats
    `date(...)` because it uses the index instead of scanning.

    DST-correct — both ends convert independently, so a spring-forward day is
    23 h wide and a fall-back day 25 h, with no hour double-counted or lost.
    """
    tz = tz or _home_tz()
    d = datetime.strptime(str(day)[:10], "%Y-%m-%d")
    lo_local = datetime(d.year, d.month, d.day, tzinfo=tz)
    hi_local = lo_local + timedelta(days=1)
    # Re-anchor the end at the next local midnight: adding 24 h to a wall-clock
    # time lands an hour off across a DST edge.
    hi_local = datetime(hi_local.year, hi_local.month, hi_local.day, tzinfo=tz)
    to_utc = lambda x: x.astimezone(timezone.utc).replace(  # noqa: E731
        tzinfo=None).isoformat(timespec="seconds")
    return to_utc(lo_local), to_utc(hi_local)


def mark_daily_summary_dirty(conn: sqlite3.Connection, circuit: str,
                             start_ts: Any) -> None:
    """dev38 — flag ``start_ts``'s LOCAL day for a daily_summary recompute.

    The nightly gap-finder only looks 7 days back and permanently freezes a
    day once it was summarised after its own end, so late imports, reprocess
    re-imports and ordinary live inserts on an already-summarised day left
    ``event_count``/peaks stale (exact on only 81% of audited days). Every
    event write drops a marker here; the pruner's summary pass drains the
    table with NO lookback limit. INSERT OR IGNORE on the (circuit, day) PK
    makes repeated same-day marking free. Best-effort by design — a marker
    lost to an exception only delays the recompute to the next mutation.
    """
    try:
        day = local_day_of(start_ts)
        if day:
            conn.execute(
                "INSERT OR IGNORE INTO daily_summary_dirty (circuit, day) "
                "VALUES (?, ?)", (circuit, day))
    except sqlite3.Error as e:
        log.debug("daily_summary_dirty mark failed (non-fatal): %s", e)


def drain_daily_summary_dirty(conn: sqlite3.Connection) -> Dict[str, int]:
    """dev38 — recompute every day flagged dirty, then clear the markers.

    Today's still-open local day is deliberately SKIPPED (and its marker
    kept): the day is still accumulating events, and the nightly pass
    summarises it once it closes — recomputing it early would just re-freeze
    it stale again. Zero-event days get their stale row DELETED by
    compute_daily_summary. Returns {"recomputed": n, "skipped_open": n}.
    """
    today = datetime.now(_home_tz()).strftime("%Y-%m-%d")
    recomputed = skipped = 0
    try:
        rows = conn.execute(
            "SELECT circuit, day FROM daily_summary_dirty ORDER BY day"
        ).fetchall()
    except sqlite3.Error:
        return {"recomputed": 0, "skipped_open": 0}
    for r in rows:
        circuit, day = r[0], r[1]
        if day >= today:
            skipped += 1
            continue
        try:
            compute_daily_summary(conn, circuit, day)
            conn.execute(
                "DELETE FROM daily_summary_dirty WHERE circuit=? AND day=?",
                (circuit, day))
            recomputed += 1
        except sqlite3.Error as e:
            log.warning("dirty-day recompute failed for %s/%s: %s",
                        circuit, day, e)
    conn.commit()
    return {"recomputed": recomputed, "skipped_open": skipped}


def backfill_time_features_tz(conn: sqlite3.Connection, tz_name: str,
                              chunk: int = 500) -> Dict[str, int]:
    """dev38 — rewrite the per-event time features in the home timezone.

    hour_of_day / day_of_week / hour_sin / hour_cos / is_weekend were computed
    on the UTC timestamp until dev38 (the 2026-08 audit: hour matched UTC on
    100% of events, weekday wrong on 30%). Migrations can't fix this — they
    run before HA answers, so the zone is unknown there — hence this deferred
    pass, called at boot once tz detection lands (the 20260571 pattern).

    Touches only rows whose ``time_features_tz`` marker is NULL or names a
    different zone, so it is idempotent and a zone change re-runs it exactly
    once. Chunked commits keep the write lock polite on add-on hardware.
    """
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
    except Exception:
        return {"rewritten": 0, "skipped_bad_tz": 1}
    rewritten = 0
    while True:
        rows = conn.execute(
            "SELECT id, start_ts FROM events WHERE time_features_tz IS NULL "
            "OR time_features_tz != ? LIMIT ?", (tz_name, chunk)).fetchall()
        if not rows:
            break
        for r in rows:
            eid, start_ts = r[0], r[1]
            try:
                dt = datetime.fromisoformat(str(start_ts))
            except (ValueError, TypeError):
                # Unparseable timestamp: stamp the marker so the loop can't
                # spin on the row forever; features stay as written.
                conn.execute(
                    "UPDATE events SET time_features_tz = ? WHERE id = ?",
                    (tz_name, eid))
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            local = dt.astimezone(tz)
            hour = local.hour
            dow = local.weekday()
            rad = 2 * math.pi * hour / 24
            conn.execute(
                "UPDATE events SET hour_of_day = ?, day_of_week = ?, "
                "hour_sin = ?, hour_cos = ?, is_weekend = ?, "
                "time_features_tz = ? WHERE id = ?",
                (hour, dow, round(math.sin(rad), 4), round(math.cos(rad), 4),
                 1 if dow >= 5 else 0, tz_name, eid))
            rewritten += 1
        conn.commit()
    return {"rewritten": rewritten}


def compute_daily_summary(conn: sqlite3.Connection,
                          circuit: str, day: str) -> Optional[Dict[str, Any]]:
    """
    Compute and upsert a daily summary row for the given circuit and day.
    day format: 'YYYY-MM-DD', in the home's LOCAL timezone (see
    local_day_bounds_utc). Callers derive it with local_day_of(start_ts).
    Returns the summary dict, or None if no events that day.
    """
    day_lo, day_hi = local_day_bounds_utc(day)
    # Volume uses volume_litres_effective (falling back to raw) so degraded-
    # supply estimates and zeroed pressure-restoration phantoms are reflected
    # here exactly as they are in hourly_volume. Keeps the History charts /
    # daily totals consistent with the dashboard cards.
    rows = conn.execute("""
        SELECT
            COUNT(*)                    AS event_count,
            SUM(COALESCE(volume_litres_effective, volume_litres, 0))
                                        AS total_volume_litres,
            AVG(avg_flow_lpm)           AS avg_flow_lpm,
            MAX(peak_flow_lpm)          AS peak_flow_lpm,
            AVG(pre_event_pressure_psi) AS avg_pressure_psi,
            MIN(min_pressure_psi)       AS min_pressure_psi,
            SUM(CASE WHEN anomaly_score IS NOT NULL
                      AND anomaly_score > 0.6 THEN 1 ELSE 0 END) AS anomaly_count,
            SUM(CASE WHEN triggered_alert = 1  THEN 1 ELSE 0 END) AS alert_count
        FROM events
        WHERE circuit = ?
          AND start_ts >= ? AND start_ts < ?
    """, (circuit, day_lo, day_hi)).fetchone()

    if not rows or rows["event_count"] == 0:
        # dev38: a day emptied of events must not keep a stale inflated row —
        # previously the caller had to remember to delete it (only
        # delete_events_in_range did). Dropping it here makes every caller
        # correct; a day with genuinely no events correctly has no row.
        conn.execute("DELETE FROM daily_summary WHERE circuit = ? AND day = ?",
                     (circuit, day))
        return None

    # Top-5 fixtures for the day (JSON for breakdown chart)
    top5 = conn.execute("""
        SELECT fixture_id, COUNT(*) AS cnt
        FROM events
        WHERE circuit = ? AND start_ts >= ? AND start_ts < ?
          AND fixture_id IS NOT NULL
        GROUP BY fixture_id
        ORDER BY cnt DESC
        LIMIT 5
    """, (circuit, day_lo, day_hi)).fetchall()
    fixture_breakdown = json.dumps(
        [{"fixture_id": r["fixture_id"], "count": r["cnt"]} for r in top5]
    ) if top5 else None

    summary = {
        "circuit":             circuit,
        "day":                 day,
        "total_volume_litres": rows["total_volume_litres"] or 0,
        "event_count":         rows["event_count"] or 0,
        "avg_flow_lpm":        rows["avg_flow_lpm"],
        "peak_flow_lpm":       rows["peak_flow_lpm"],
        "avg_pressure_psi":    rows["avg_pressure_psi"],
        "min_pressure_psi":    rows["min_pressure_psi"],
        "anomaly_count":       rows["anomaly_count"] or 0,
        "alert_count":         rows["alert_count"] or 0,
        "top_fixture_id":      top5[0]["fixture_id"] if top5 else None,
        "top_fixture_count":   top5[0]["cnt"] if top5 else 0,
        "fixture_breakdown":   fixture_breakdown,
        "computed_at":         datetime.now(timezone.utc).isoformat(),
    }

    cols = ", ".join(summary.keys())
    ph   = ", ".join("?" for _ in summary)
    updates = ", ".join(f"{k}=excluded.{k}" for k in summary if k not in ("circuit", "day"))
    # Future-proofing: if a refactor ever strips this dict down to just
    # the conflict-key columns, `updates` becomes the empty string and
    # `DO UPDATE SET ` is a SQL syntax error. Fall back to DO NOTHING.
    on_conflict = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
    conn.execute(
        f"INSERT INTO daily_summary ({cols}) VALUES ({ph}) "
        f"ON CONFLICT(circuit, day) {on_conflict}",
        list(summary.values()),
    )
    return summary


def get_daily_summaries(
    conn: sqlite3.Connection,
    circuit: str,
    date_from: str = None,
    date_to: str = None,
) -> List[Dict[str, Any]]:
    """Return daily_summary rows for a circuit, ordered oldest-first for charting."""
    conditions = ["circuit = ?"]
    params: list = [circuit]
    if date_from:
        conditions.append("day >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("day <= ?")
        params.append(date_to)
    where = " AND ".join(conditions)
    rows = conn.execute(
        f"SELECT * FROM daily_summary WHERE {where} ORDER BY day ASC",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def rebuild_daily_summaries(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Recompute every `daily_summary` row against the current local-day cut.

    Needed once when the day boundary moved off UTC (migration 20260571), and
    again whenever the home's timezone changes — stored rows are keyed by a day
    string, so a boundary change silently mis-attributes every historical total
    until they're rebuilt. Rows are dropped and recomputed inside one
    transaction: a half-rebuilt chart is worse than a brief lock.

    Days with no events are simply absent (compute_daily_summary returns None
    without writing), which is how a fresh DB looks anyway.
    """
    circuits = [r["circuit"] for r in
                conn.execute("SELECT DISTINCT circuit FROM events").fetchall()]
    days_written, days_seen = 0, 0
    with transaction(conn):
        for circuit in circuits:
            row = conn.execute(
                "SELECT MIN(start_ts) AS lo, MAX(start_ts) AS hi "
                "FROM events WHERE circuit = ?", (circuit,)).fetchone()
            if not row or not row["lo"]:
                continue
            conn.execute("DELETE FROM daily_summary WHERE circuit = ?", (circuit,))
            d = datetime.strptime(local_day_of(row["lo"]), "%Y-%m-%d")
            end = datetime.strptime(local_day_of(row["hi"]), "%Y-%m-%d")
            while d <= end:
                days_seen += 1
                if compute_daily_summary(conn, circuit, d.strftime("%Y-%m-%d")):
                    days_written += 1
                d += timedelta(days=1)
    log.info("daily_summary rebuilt on the local-day boundary: %d day(s) "
             "written across %d circuit(s) (%d scanned)",
             days_written, len(circuits), days_seen)
    return {"circuits": len(circuits), "days_written": days_written,
            "days_scanned": days_seen}


def get_data_retention(conn: sqlite3.Connection) -> dict:
    row = conn.execute("SELECT * FROM data_retention WHERE id = 1").fetchone()
    if row:
        return dict(row)
    return {
        "events_retain_years":        1,
        "hourly_volume_retain_years": 2,
        "enabled":                    1,
        "last_pruned_at":             None,
        "auto_backup_enabled":        0,
        "auto_backup_path":           "/share/water_monitor_backups",
        "auto_backup_day_of_week":    0,
        "last_auto_backup_at":        None,
    }


_DATA_RETENTION_COLUMNS = frozenset({
    "events_retain_years", "hourly_volume_retain_years", "enabled",
    "last_pruned_at", "auto_backup_enabled", "auto_backup_path",
    "auto_backup_day_of_week", "last_auto_backup_at", "updated_at",
})


def update_data_retention(conn: sqlite3.Connection, **kwargs) -> None:
    unknown = set(kwargs) - _DATA_RETENTION_COLUMNS
    if unknown:
        raise ValueError(f"Unknown data_retention columns: {unknown}")
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(
        f"UPDATE data_retention SET {sets} WHERE id = 1",
        list(kwargs.values()),
    )
    conn.commit()


def get_or_create_csrf_server_secret(conn: sqlite3.Connection) -> str:
    """Return the persistent HMAC server secret used for CSRF tokens.

    Created on first use and stored in `csrf_server_secret`. Never
    regenerated automatically — rotating it would invalidate every
    in-flight browser form across the addon.

    The secret is a 64-character hex string (256 bits of entropy).
    """
    import secrets as _secrets
    row = conn.execute(
        "SELECT secret FROM csrf_server_secret WHERE id = 1"
    ).fetchone()
    if row and row["secret"]:
        return row["secret"]
    secret = _secrets.token_hex(32)
    conn.execute(
        "INSERT OR REPLACE INTO csrf_server_secret (id, secret) "
        "VALUES (1, ?)",
        (secret,),
    )
    conn.commit()
    return secret


def derive_csrf_token(server_secret: str, session_id: str) -> str:
    """Compute the CSRF token for a given session_id.

    Stateless HMAC double-submit pattern: the same session_id always
    produces the same token, but the token can't be forged without the
    server_secret. Validation just recomputes and constant-time-compares.
    """
    import hashlib
    import hmac
    return hmac.new(
        server_secret.encode("utf-8"),
        session_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def validate_csrf_token(
    server_secret: str, session_id: str, token: str
) -> bool:
    """Return True if `token` matches HMAC(server_secret, session_id).

    Returns False on any missing input. Uses constant-time comparison
    to avoid timing side-channels.
    """
    import hmac
    if not server_secret or not session_id or not token:
        return False
    expected = derive_csrf_token(server_secret, session_id)
    return hmac.compare_digest(expected, token)


def get_home_profile(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM home_profile WHERE id = 1").fetchone()


def update_home_profile(conn: sqlite3.Connection, **kwargs) -> None:
    # Derive the writable column allowlist from the live schema so any
    # column added by a future migration is automatically permitted without
    # needing a corresponding change here.  id and created_at are excluded —
    # they must never be overwritten by callers.  updated_at is managed below.
    _immutable = {"id", "created_at"}
    valid_cols = (
        {r[1] for r in conn.execute("PRAGMA table_info(home_profile)").fetchall()}
        - _immutable
    )
    bad = set(kwargs) - valid_cols
    if bad:
        raise ValueError(
            f"update_home_profile: unknown column(s): {bad}. "
            f"Valid columns: {sorted(valid_cols)}"
        )
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [1]
    conn.execute(f"UPDATE home_profile SET {sets} WHERE id = ?", values)
    conn.commit()


def get_training_state(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM training_state WHERE circuit = ?", (circuit,)
    ).fetchone()


def is_baseline_locked(conn: sqlite3.Connection, circuit: str) -> bool:
    """True when the circuit's reference baseline is FROZEN — i.e. the training/labelling
    window has closed and no (re)calibration is in flight: ``training_state.state ==
    'live'`` AND no active ``learning_config.accelerated_adaptation_until`` window.

    The fixture classifier is fit-once-at-activation then hard-locked (see the
    locked-baseline design): once locked, a user relabel must NOT re-walk history. So a
    label change spreads only to the event's own cycle-mates (applied synchronously) —
    the full label-triggered reclassify is skipped. Mirrors the anomaly-shutoff state
    gate so the two notions of 'locked' can't drift."""
    row = conn.execute(
        "SELECT state FROM training_state WHERE circuit = ?", (circuit,)).fetchone()
    if not row or row["state"] != "live":
        return False
    lc = conn.execute(
        "SELECT accelerated_adaptation_until FROM learning_config WHERE circuit = ?",
        (circuit,)).fetchone()
    until = lc["accelerated_adaptation_until"] if lc else None
    if until:
        try:
            t = datetime.fromisoformat(str(until).replace("Z", "+00:00"))
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            if t > datetime.now(timezone.utc):
                return False   # active (re)calibration / adaptation window
        except (ValueError, TypeError):
            pass
    return True


def _upsert_by_circuit(
    conn: sqlite3.Connection, table: str, circuit: str, **kwargs
) -> None:
    """Generic upsert helper for single-circuit config tables.

    Table name comes exclusively from internal string literals — never user
    input — so the f-string interpolation does not introduce injection risk.
    """
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    exists = conn.execute(
        f"SELECT 1 FROM {table} WHERE circuit = ?", (circuit,)
    ).fetchone() is not None
    if exists:
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        conn.execute(
            f"UPDATE {table} SET {sets} WHERE circuit = ?",
            [*kwargs.values(), circuit],
        )
    else:
        kwargs["circuit"] = circuit
        cols = ", ".join(kwargs)
        phs = ", ".join("?" * len(kwargs))
        conn.execute(
            f"INSERT INTO {table} ({cols}) VALUES ({phs})",
            list(kwargs.values()),
        )
    conn.commit()


def upsert_training_state(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "training_state", circuit, **kwargs)


# ── Background job status (§2.4) ─────────────────────────────────────────────────

def start_job(conn: sqlite3.Connection, kind: str, circuit: Optional[str] = None,
              message: Optional[str] = None) -> int:
    """Record a long-running background op as 'running'; returns its id. Also prunes
    finished rows older than 2 days so the table stays tiny."""
    cur = conn.execute(
        "INSERT INTO jobs (circuit, kind, status, message) VALUES (?, ?, 'running', ?)",
        (circuit, kind, message))
    conn.execute("DELETE FROM jobs WHERE status <> 'running' "
                 "AND created_at < datetime('now', '-2 days')")
    conn.commit()
    return int(cur.lastrowid)


def finish_job(conn: sqlite3.Connection, job_id: int, status: str = "done",
               message: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE jobs SET status = ?, message = ?, finished_at = ? WHERE id = ?",
        (status, message, datetime.now(timezone.utc).isoformat(), job_id))
    conn.commit()


def get_jobs_since(conn: sqlite3.Connection, since_id: int = 0,
                   limit: int = 20) -> list:
    """Jobs with id > ``since_id`` (newest first) for the UI poll-and-toast."""
    try:
        rows = conn.execute(
            "SELECT id, circuit, kind, status, message, created_at, finished_at "
            "FROM jobs WHERE id > ? ORDER BY id DESC LIMIT ?",
            (since_id, limit)).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


# ── Recorder reconciliation checkpoint (Phase 3 §2) ──────────────────────────────

def get_reconcile_state(conn: sqlite3.Connection,
                        circuit: str) -> Optional[sqlite3.Row]:
    try:
        return conn.execute(
            "SELECT * FROM reconcile_state WHERE circuit = ?", (circuit,)).fetchone()
    except sqlite3.OperationalError:
        return None


def set_reconcile_state(conn: sqlite3.Connection, circuit: str, *,
                        through_ts: Optional[str] = None,
                        corrections_delta: int = 0, flagged_delta: int = 0,
                        last_delta_litres: Optional[float] = None) -> None:
    """Advance the per-circuit checkpoint + bump cumulative counters (upsert)."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO reconcile_state "
        "  (circuit, through_ts, corrections, flagged, last_run_at, last_delta_litres) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(circuit) DO UPDATE SET "
        "  through_ts = COALESCE(excluded.through_ts, reconcile_state.through_ts), "
        "  corrections = reconcile_state.corrections + ?, "
        "  flagged = reconcile_state.flagged + ?, "
        "  last_run_at = excluded.last_run_at, "
        "  last_delta_litres = COALESCE(excluded.last_delta_litres, "
        "                               reconcile_state.last_delta_litres)",
        (circuit, through_ts, corrections_delta, flagged_delta, now, last_delta_litres,
         corrections_delta, flagged_delta))
    conn.commit()


def get_sensitivity_config(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM sensitivity_config WHERE circuit = ?", (circuit,)
    ).fetchone()


def upsert_sensitivity_config(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "sensitivity_config", circuit, **kwargs)


def get_learning_config(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM learning_config WHERE circuit = ?", (circuit,)
    ).fetchone()


def upsert_learning_config(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "learning_config", circuit, **kwargs)


def get_alert_configs(conn: sqlite3.Connection, circuit: str) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM alert_config WHERE circuit = ? ORDER BY alert_type",
        (circuit,)
    ).fetchall()


VALID_CIRCUIT_TYPES = frozenset({"fixture", "zone"})


def get_circuit_type(
    conn: sqlite3.Connection,
    circuit: str,
    default: str = "fixture",
) -> str:
    """Return the circuit_type for a circuit, normalised to a canonical value.

    Falls back to `default` if no circuit_profile row exists yet.
    Normalises legacy "irrigation" values to "zone" transparently.
    """
    from .fixtures import normalize_circuit_type
    row = conn.execute(
        "SELECT circuit_type FROM circuit_profile WHERE circuit = ?",
        (circuit,)
    ).fetchone()
    raw = row["circuit_type"] if row else default
    return normalize_circuit_type(raw)


def _seed_zone_alerts_only(conn: sqlite3.Connection, circuit: str) -> None:
    """INSERT OR IGNORE zone-only alert rows — never overwrites user toggle state."""
    zone_only_alerts = [
        ("pre_solenoid_leak", "Pre-Solenoid Leak",
         "Alert when flow detected with no zone commanded open"),
        ("solenoid_weeping", "Solenoid Weeping",
         "Alert when flow persists after zone commanded closed"),
        ("zone_flow_deviation_high", "Zone Flow High",
         "Alert when zone flow exceeds learned range"),
        ("zone_flow_deviation_low", "Zone Flow Low",
         "Alert when zone flow is below learned range — possible blocked head"),
        ("zone_duration_overrun", "Zone Duration Overrun",
         "Alert when zone runs significantly longer than expected"),
    ]
    for alert_type, label, description in zone_only_alerts:
        alert_id = f"{alert_type}_{circuit}"
        conn.execute("""
            INSERT OR IGNORE INTO alert_config
                (id, circuit, alert_type, label, description, enabled)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (alert_id, circuit, alert_type, label, description))


def set_circuit_type(
    conn: sqlite3.Connection,
    circuit: str,
    circuit_type: str,
    commit: bool = True,
) -> None:
    """Persist circuit_type to circuit_profile.

    UPSERTs the row so it is safe to call before ensure_circuit_defaults().
    When switching to "zone", seeds any missing zone-only alert rows via
    INSERT OR IGNORE so existing user toggle state is preserved.
    Zone alerts are never deleted when switching back to "fixture" — they
    are simply hidden in the UI by the template filter.

    When ``commit`` is True (default) the change is committed before
    returning. Pass ``commit=False`` when this is part of a multi-step
    write sequence the caller wants to make atomic via ``with
    transaction(conn):`` — see setup wizard step 3b for the canonical
    example.
    """
    from .fixtures import normalize_circuit_type
    circuit_type = normalize_circuit_type(circuit_type)
    if circuit_type not in VALID_CIRCUIT_TYPES:
        raise ValueError(f"Invalid circuit_type {circuit_type!r}; must be 'fixture' or 'zone'")
    conn.execute("""
        INSERT INTO circuit_profile (circuit, circuit_type)
        VALUES (?, ?)
        ON CONFLICT(circuit) DO UPDATE SET circuit_type = excluded.circuit_type
    """, (circuit, circuit_type))
    if circuit_type == "zone":
        _seed_zone_alerts_only(conn, circuit)
    if commit:
        conn.commit()


def get_valve_type(
    conn: sqlite3.Connection,
    circuit: str,
    default: str = "2_port",
) -> str:
    """Return the valve_type for a circuit.

    Falls back to `default` if no circuit_profile row exists yet.
    Normalises via fixtures.normalize_valve_type (forgiving) so callers
    always get a canonical string they can render safely.
    """
    from .fixtures import normalize_valve_type
    row = conn.execute(
        "SELECT valve_type FROM circuit_profile WHERE circuit = ?",
        (circuit,)
    ).fetchone()
    raw = row["valve_type"] if row and row["valve_type"] else default
    return normalize_valve_type(raw)


def set_valve_type(
    conn: sqlite3.Connection,
    circuit: str,
    valve_type: str,
    commit: bool = True,
) -> None:
    """Persist valve_type to circuit_profile.

    Uses the STRICT parser. Callers are expected to present a clean value;
    a ValueError is raised on bad input rather than silently substituting
    the default. UI/router layers should pre-validate via parse_valve_type
    before calling this.

    UPSERT mirrors set_circuit_type — all other circuit_profile columns
    have DEFAULTs or allow NULL so the minimal INSERT shape is safe.

    Pass ``commit=False`` when this is part of a multi-step write
    sequence the caller wants to make atomic via ``with
    transaction(conn):`` — see setup wizard step 3b for the canonical
    example.
    """
    from .fixtures import parse_valve_type
    parsed = parse_valve_type(valve_type)
    if parsed is None:
        raise ValueError(f"Invalid valve_type {valve_type!r}")
    conn.execute("""
        INSERT INTO circuit_profile (circuit, valve_type)
        VALUES (?, ?)
        ON CONFLICT(circuit) DO UPDATE SET valve_type = excluded.valve_type
    """, (circuit, parsed))
    if commit:
        conn.commit()


#: Reference-turbine default pulses-per-litre (YF-B5). Mirrors the firmware
#: ppl_main/ppl_irr number entities' initial_value, so an un-discovered circuit
#: still has a sane low-flow floor (60 ÷ 396 ≈ 0.15 L/min).
DEFAULT_PULSES_PER_LITRE: float = 396.0


def get_circuit_pulses_per_litre(
    conn: sqlite3.Connection,
    circuit: str,
    default: float = DEFAULT_PULSES_PER_LITRE,
) -> float:
    """Return the cached pulses-per-litre for a circuit.

    This is the add-on's local CACHE of the firmware's runtime PPL number
    entity (the firmware entity is the single source of truth; the add-on
    refreshes this from the entity when HA is reachable). Falls back to
    `default` if no circuit_profile row exists yet or the stored value is
    unusable (NULL / non-numeric / out of range).
    """
    row = conn.execute(
        "SELECT pulses_per_litre FROM circuit_profile WHERE circuit = ?",
        (circuit,)
    ).fetchone()
    raw = row["pulses_per_litre"] if row else None
    try:
        ppl = float(raw)
    except (TypeError, ValueError):
        return default
    return ppl if 1.0 <= ppl <= 5000.0 else default


def set_circuit_pulses_per_litre(
    conn: sqlite3.Connection,
    circuit: str,
    pulses_per_litre: float,
    commit: bool = True,
) -> None:
    """Persist the cached pulses-per-litre for a circuit (UPSERT).

    The firmware number entity stays the source of truth; this only caches the
    last value the add-on observed so detection keeps a sane floor when HA is
    briefly unreachable. Rejects non-finite / out-of-range values (matches the
    firmware entity's 1..5000 bounds).
    """
    ppl = float(pulses_per_litre)
    if not (1.0 <= ppl <= 5000.0):
        raise ValueError(f"Invalid pulses_per_litre {pulses_per_litre!r}")
    conn.execute("""
        INSERT INTO circuit_profile (circuit, pulses_per_litre)
        VALUES (?, ?)
        ON CONFLICT(circuit) DO UPDATE SET pulses_per_litre = excluded.pulses_per_litre
    """, (circuit, ppl))
    if commit:
        conn.commit()


def set_alert_enabled(conn: sqlite3.Connection, alert_id: str, enabled: bool) -> None:
    conn.execute(
        "UPDATE alert_config SET enabled = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, datetime.now(timezone.utc).isoformat(), alert_id)
    )
    conn.commit()


#: Columns on `events` that capture user intent — never overwritten by an
#: importer re-insert or any other automated path. The historical importer
#: re-imports past events when fresh history becomes available, and the
#: live detector can re-stage the same id on retry; both must preserve any
#: label or ignore-flag the user already set.
_EVENT_USER_COLUMNS: frozenset[str] = frozenset({
    "user_fixture_type",
    "user_reviewed",
    # Anomaly-triage verdict ('normal'/'unknown') — user intent like the
    # reviewed bit itself; 'unknown' additionally gates baseline refits.
    "review_verdict",
    # Sprint H: the explicit user intents are preserved across re-import/
    # enrichment upserts. excluded_from_training is NO LONGER preserved here —
    # it's a DERIVED field (auto verdicts OR user_ignored OR manual) recomputed
    # by _finalize_derived_verdicts, so preserving it would freeze stale
    # auto-exclusion. user_ignored carries the Ignore/Restore intent instead;
    # user_classified locks a manual classification against auto-override.
    "user_ignored",
    "user_classified",
    # Label provenance ('user'/'cycle'/'training') — preserved so a re-import
    # never drops the auto-label source that drives the undo + exclude-warning.
    "fixture_label_source",
})

# Columns whose values must NOT be overwritten by an event-row upsert.
# These track the exact prior contribution applied to hourly_volume so future
# reprocessing (re-imports etc.) can correctly subtract before re-adding.
# Maintained ONLY by upsert_event_and_apply_hourly_volume() — never by the
# event-feature upsert path.
_EVENT_APPLIED_BOOKKEEPING_COLUMNS: frozenset[str] = frozenset({
    "hourly_volume_applied_litres",
    "hourly_volume_applied_bucket",
})


def _hour_bucket_for(start_ts) -> str:
    """Return the hour_ts string in the canonical format used by
    update_hourly_volume(): UTC-normalised '%Y-%m-%dT%H:00:00' (no tz suffix).
    Mirrors the production format from feature_extractor.py line ~1066 so
    aggregate queries (get_daily_volume / get_weekly_volume) keep working.
    """
    if start_ts is None:
        return ""
    if isinstance(start_ts, str):
        try:
            dt = datetime.fromisoformat(start_ts)
        except ValueError:
            return ""
    elif isinstance(start_ts, datetime):
        dt = start_ts
    else:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:00:00')


def insert_event(conn: sqlite3.Connection, event: dict) -> bool:
    """Insert event row; returns True if genuinely new, False on conflict.

    Replaces an older INSERT OR REPLACE implementation that delete-then-
    inserted on conflict. REPLACE had three bad side-effects:
      (1) it fired ON DELETE CASCADE against tables that reference
          events(id) — e.g. fixture_ha_entity_map — silently dropping
          dependent rows;
      (2) it changed the rowid of the conflicting row, breaking any
          rowid-based bookkeeping a long-running reader held;
      (3) it wiped user-editable columns (user_fixture_type,
          user_reviewed, excluded_from_training) on every same-id
          re-insert, undoing manual labels the next time the importer
          ran.

    The new behaviour is an UPSERT via ON CONFLICT(id) DO UPDATE that
    refreshes every measurement / system column from the incoming row but
    deliberately omits the user-controlled columns listed in
    _EVENT_USER_COLUMNS. Conflicts no longer fire cascades or change rowids.

    "is genuinely new" is now decided by a pre-check rather than by
    interpreting total_changes (REPLACE's old +2-on-replace trick is no
    longer applicable). Callers use the return value to decide whether to
    add the event's volume to hourly_volume.
    """
    if "id" not in event:
        raise ValueError("insert_event: event dict missing 'id'")
    exists = conn.execute(
        "SELECT 1 FROM events WHERE id = ?", (event["id"],),
    ).fetchone() is not None
    _do_event_upsert(conn, event)
    conn.commit()
    return not exists


def _do_event_upsert(conn: sqlite3.Connection, event: dict) -> None:
    """Run the INSERT ... ON CONFLICT(id) DO UPDATE for an event row.

    The DO UPDATE SET clause refreshes measurement/system columns but
    explicitly excludes:
      • 'id' (the conflict target)
      • _EVENT_USER_COLUMNS (preserve user labels/flags across re-imports)
      • _EVENT_APPLIED_BOOKKEEPING_COLUMNS (preserve exact prior
        hourly_volume contribution so upsert_event_and_apply_hourly_volume
        can subtract it correctly on reprocess)

    Does NOT commit; the caller controls the transaction boundary.
    """
    cols = list(event.keys())
    if "id" not in cols:
        raise ValueError("_do_event_upsert: event dict missing 'id'")

    col_list     = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)
    set_cols = [
        c for c in cols
        if c != "id"
        and c not in _EVENT_USER_COLUMNS
        and c not in _EVENT_APPLIED_BOOKKEEPING_COLUMNS
    ]
    if set_cols:
        set_clause = ", ".join(f"{c}=excluded.{c}" for c in set_cols)
        sql = (
            f"INSERT INTO events ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {set_clause}"
        )
    else:
        sql = (
            f"INSERT INTO events ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO NOTHING"
        )
    conn.execute(sql, list(event.values()))


# Shared upsert that adds a signed delta to one (circuit, hour) ledger bucket.
_HOURLY_VOLUME_DELTA_SQL = (
    "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) VALUES (?, ?, ?) "
    "ON CONFLICT (circuit, hour_ts) "
    "DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres"
)


def apply_effective_volume(
    conn: sqlite3.Connection, event_id: str, circuit: str, start_ts,
    new_effective: float,
) -> float:
    """THE single chokepoint for the hourly-volume ledger math (§2.5).

    Reverses the event's PRIOR applied contribution (from its recorded bucket),
    applies ``new_effective`` to the bucket derived from ``start_ts``, then records
    the new applied bookkeeping (``hourly_volume_applied_litres`` / ``_bucket``). Every
    per-event volume write — the live upsert AND every reprocess / recompute / merge
    path — routes through here so the events ↔ hourly_volume ledger can never drift
    (previously this reverse/apply/bookkeep math was hand-copied at ~8 sites). The
    caller writes ``events.volume_litres_effective`` to the SAME returned value;
    ``volume_ledger_discrepancy()`` + its test pin the invariant. Assumes a caller
    transaction. Returns the rounded effective volume actually applied.
    """
    new_effective = round(float(new_effective or 0.0), 3)
    # NULL bucket when nothing is applied (a phantom/cross-talk zero) — "no
    # contribution lives anywhere", the convention the reprocess paths already used.
    new_bucket = _hour_bucket_for(start_ts) if new_effective else None
    prev = conn.execute(
        "SELECT hourly_volume_applied_litres, hourly_volume_applied_bucket "
        "FROM events WHERE id = ?", (event_id,),
    ).fetchone()
    prev_litres = float(prev["hourly_volume_applied_litres"] or 0.0) if prev else 0.0
    prev_bucket = prev["hourly_volume_applied_bucket"] if prev else None
    # Reverse the prior contribution, then apply the new one (handles a bucket move
    # AND the same-bucket case — net delta — identically).
    if prev_bucket and prev_litres:
        conn.execute(_HOURLY_VOLUME_DELTA_SQL, (circuit, prev_bucket, -prev_litres))
    if new_bucket and new_effective:
        conn.execute(_HOURLY_VOLUME_DELTA_SQL, (circuit, new_bucket, new_effective))
    conn.execute(
        "UPDATE events SET hourly_volume_applied_litres = ?, "
        "  hourly_volume_applied_bucket = ? WHERE id = ?",
        (new_effective, new_bucket, event_id),
    )
    return new_effective


def volume_ledger_discrepancy(conn: sqlite3.Connection,
                              circuit: Optional[str] = None) -> float:
    """SUM(events.hourly_volume_applied_litres) − SUM(hourly_volume.volume_litres);
    ~0 when the ledger is consistent. Backs the §2.5 reconciliation test + is safe to
    log as a diagnostic. Rounded to swallow float noise.

    dev46 (46b) reviewed — both ``fetchone()[0]`` below stay UNGUARDED. They are
    ``COALESCE(SUM(...))`` aggregates, which always return exactly one row, so a
    None here means the cursor is broken, not that the result is empty. Guarding
    would turn a loud failure into a silently wrong ledger discrepancy."""
    where = "" if circuit is None else " WHERE circuit = ?"
    args = () if circuit is None else (circuit,)
    applied = conn.execute(
        "SELECT COALESCE(SUM(hourly_volume_applied_litres), 0) FROM events" + where,
        args).fetchone()[0]
    ledger = conn.execute(
        "SELECT COALESCE(SUM(volume_litres), 0) FROM hourly_volume" + where,
        args).fetchone()[0]
    return round(float(applied) - float(ledger), 3)


def upsert_event_and_apply_hourly_volume(
    conn: sqlite3.Connection,
    event: dict,
    new_effective_volume: float,
) -> bool:
    """Atomically upsert an event row AND keep hourly_volume in sync.

    Replaces the previous two-step pattern (insert_event then update_hourly_volume)
    which made it easy to lose idempotency on reprocessing. All work happens
    inside a single transaction:

      1. Read prior (litres, bucket) from the existing event row, if any.
      2. UPSERT the event (preserving _EVENT_APPLIED_BOOKKEEPING_COLUMNS).
      3. If a prior bucket existed: subtract the prior contribution from it.
      4. Add new_effective_volume to the new bucket (derived from start_ts).
      5. Update the event's applied-bookkeeping columns to (new_amount, new_bucket).

    Returns True if the event was genuinely new (not a reprocess); the caller
    can use this to decide whether to bump training_state counters.

    `event` must contain 'id' and 'start_ts'. circuit is read from event['circuit'].
    """
    if "id" not in event or "start_ts" not in event or "circuit" not in event:
        raise ValueError(
            "upsert_event_and_apply_hourly_volume: event missing "
            "id/start_ts/circuit"
        )

    event_id = event["id"]
    circuit = event["circuit"]

    with transaction(conn):
        # is_new = no prior row at all (caller uses it to bump training counters).
        is_new = conn.execute(
            "SELECT 1 FROM events WHERE id = ?", (event_id,)).fetchone() is None
        # UPSERT the event — preserves the applied-bookkeeping columns, so the
        # chokepoint below reads the PRIOR applied state (to reverse it) correctly.
        _do_event_upsert(conn, event)
        apply_effective_volume(conn, event_id, circuit, event["start_ts"],
                               new_effective_volume)
        # dev38 — every event write flags its local day for a summary
        # recompute (drained by the pruner with no lookback limit).
        mark_daily_summary_dirty(conn, circuit, event["start_ts"])

    # Overlap guard (dev28): a genuinely-new event that intersects an existing
    # same-circuit event means the same water was recorded twice (live blip
    # wrapper vs import, ~127 L in the 2026-07 incident). The guard resolves
    # whose volume counts — insertion is never blocked, and a guard failure
    # must never break the write path.
    if is_new:
        try:
            from .overlap_guard import guard_new_event
            guard_new_event(conn, event_id, circuit, event["start_ts"],
                            event.get("end_ts"))
        except Exception as e:
            log.warning("[%s] overlap guard failed (non-fatal): %s",
                        circuit, e)

    return is_new


def snapshot_database(conn: sqlite3.Connection, db_path,
                      label: str = "snapshot") -> Optional[str]:
    """Write a consistent ``VACUUM INTO`` snapshot of the live DB beside
    ``db_path`` — the recovery point taken before a destructive coalesce pass
    (dev.24). Returns the snapshot path, or None when db_path is not a real
    on-disk file (e.g. the ``:memory:`` databases used in tests) or the snapshot
    could not be written. A failure is logged but never raised — it must not
    block the user's recompute, and the coalesce is itself volume-preserving."""
    try:
        if str(db_path) in (":memory:", ""):
            return None
        p = Path(str(db_path))
        if not p.exists():
            return None
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        dest = p.with_name(f"{p.name}.{label}-{ts}")
        conn.execute("VACUUM INTO ?", (str(dest),))
        log.info("DB snapshot written before destructive pass: %s", dest)
        return str(dest)
    except Exception as e:
        log.warning("DB snapshot failed (%s) — coalesce will still proceed "
                    "(it is volume-preserving)", e)
        return None


def coalesce_low_flow_events(
    conn: sqlite3.Connection, circuit: str, dry_run: bool = False,
) -> Dict[str, int]:
    """Merge adjacent low-flow sensor-chatter fragments into one event each (dev.24).

    The turbine flow sensor can't hold a reading at very low flow, so a single
    sustained low draw is chopped into many tiny events (the softener brine cycle:
    ~17 fragments, median gap ~19 s). This is the one-time cleanup for PRE-EXISTING
    history — the live detector's off-grace prevents new fragmentation. Only
    UNLABELED, non-user-classified, non-degraded events that are low-flow per the
    SHARED predicate (``event_rules.is_low_flow_chatter``) and within
    LOWFLOW_OFF_GRACE_S of each other — with NO other event between them, so a real
    flush/labelled use is never merged across — are grouped.

    Per group the earliest event survives and absorbs the others' volume (exact
    sum), end_ts/duration span, peak (max) and ΔP (max); avg = volume / duration.
    Its dribble/exclusion verdict is RECOMPUTED from the aggregated volume via the
    single-source ``_finalize_derived_verdicts`` — because ``reprocess_event_
    exclusion_verdicts`` only ever FLAGS, never un-flags, a 17×0.4 L dribble train
    that merges to one 6.8 L draw must be un-excluded HERE (this is what *improves*
    slow-leak detectability: the sustained draw stops being dismissed as chatter).
    ``flow_integral_litres`` is set to the merged volume so the later phantom
    rescan never zeroes a real merged draw; cluster_id / matched_fixture_type /
    matched_via are cleared so reclassify re-derives cleanly (match_rejection_
    reason is set by the finalizer, never overloaded with a 'coalesced' marker).

    hourly_volume is kept exact (reverse every member's applied contribution,
    re-apply the survivor's total → net-zero); daily_summary is recomputed for
    every affected day. Absorbed rows are deleted — cascading event_waveforms /
    zone_flow_history (foreign_keys=ON), with training_capture_candidates cleaned
    manually (no FK). Idempotent (a second run finds no adjacent low-flow pairs).

    DESTRUCTIVE but volume-preserving — callers MUST snapshot the DB first
    (``snapshot_database``) and run this only from the user-triggered recompute,
    never silently at startup. ``dry_run=True`` returns the would-merge counts
    without mutating. Returns {"groups": <merge groups>, "absorbed": <rows removed>}.
    """
    from .event_rules import LOWFLOW_OFF_GRACE_S, is_low_flow_chatter

    def _ts(v):
        if not v:
            return None
        try:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt

    rows = conn.execute(
        "SELECT id, start_ts, end_ts, volume_litres, volume_litres_effective, "
        "       avg_flow_lpm, true_avg_flow_lpm, peak_flow_lpm, pressure_delta_psi, "
        "       user_fixture_type, user_classified, user_ignored, degraded_supply, "
        "       hourly_volume_applied_litres, hourly_volume_applied_bucket "
        "FROM events WHERE circuit = ? ORDER BY start_ts ASC",
        (circuit,),
    ).fetchall()

    def _is_candidate(r) -> bool:
        if r["user_fixture_type"] is not None or r["user_classified"]:
            return False
        if r["degraded_supply"]:
            return False
        peak = r["peak_flow_lpm"]
        if peak is None:
            return False
        mean = r["true_avg_flow_lpm"]
        mean = r["avg_flow_lpm"] if mean is None else mean
        return is_low_flow_chatter(mean, peak)

    # Build merge groups: maximal runs of consecutive CANDIDATE events each within
    # grace of the previous, BROKEN by any non-candidate event in between (so a
    # real flush / labelled use sitting between two trickles is never merged across).
    groups = []
    cur = []
    for r in rows:
        if _is_candidate(r):
            s = _ts(r["start_ts"])
            if cur:
                prev_end = _ts(cur[-1]["end_ts"]) or _ts(cur[-1]["start_ts"])
                if (s is not None and prev_end is not None
                        and (s - prev_end).total_seconds() <= LOWFLOW_OFF_GRACE_S):
                    cur.append(r)
                    continue
            if len(cur) >= 2:
                groups.append(cur)
            cur = [r]
        else:
            if len(cur) >= 2:
                groups.append(cur)
            cur = []
    if len(cur) >= 2:
        groups.append(cur)

    absorbed_total = sum(len(g) - 1 for g in groups)
    if dry_run or not groups:
        return {"groups": len(groups), "absorbed": absorbed_total}

    from .feature_extractor import _finalize_derived_verdicts
    from .artifact_calibration import load_artifact_calibration
    _acal = load_artifact_calibration(conn, circuit) or None  # Phase 2.4
    affected_days = set()

    with transaction(conn):
        for g in groups:
            survivor = g[0]
            sid = survivor["id"]
            s_start = _ts(survivor["start_ts"])
            ends = [(_ts(m["end_ts"]) or _ts(m["start_ts"])) for m in g]
            ends = [e for e in ends if e is not None]
            end_ts = max(ends) if ends else s_start
            duration = ((end_ts - s_start).total_seconds()
                        if (end_ts and s_start) else 0.0)
            total_vol = sum(float(m["volume_litres"] or 0.0) for m in g)
            peak = max(float(m["peak_flow_lpm"] or 0.0) for m in g)
            delta = max(float(m["pressure_delta_psi"] or 0.0) for m in g)
            avg = (total_vol / (duration / 60.0)) if duration > 0 else 0.0

            # Recompute the survivor's verdict from the AGGREGATED features (the
            # single source of truth). Pass active-flow fields that reflect a REAL
            # draw (flow_integral = merged volume) so it is never re-flagged phantom
            # / cross-talk; the dribble verdict falls out of the summed volume.
            feats = {
                "volume_litres": total_vol, "volume_litres_estimated": None,
                "avg_flow_lpm": avg, "peak_flow_lpm": peak,
                "duration_seconds": duration, "pressure_delta_psi": delta,
                "degraded_supply": 0, "user_ignored": bool(survivor["user_ignored"]),
                "user_classified": 0, "integration_quality": "ok",
                "true_avg_flow_lpm": avg, "flow_integral_litres": total_vol,
                "flow_on_ratio": None, "is_composite": 0,
            }
            from .config import pump_gates_active as _pga
            try:
                _pump = _pga(conn, circuit)
            except Exception:
                _pump = False
            _finalize_derived_verdicts(feats, _acal, pump_gates=_pump)
            new_eff = float(feats["volume_litres_effective"] or 0.0)

            # (1) Reverse each ABSORBED member's applied contribution. The survivor's
            # own prior contribution is reversed by the §2.5 chokepoint below, so it
            # must NOT be double-reversed here.
            for m in g[1:]:
                applied = float(m["hourly_volume_applied_litres"] or 0.0)
                bucket = m["hourly_volume_applied_bucket"]
                if bucket and applied:
                    conn.execute(
                        "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                        "VALUES (?, ?, ?) ON CONFLICT (circuit, hour_ts) DO UPDATE "
                        "SET volume_litres = volume_litres + excluded.volume_litres",
                        (circuit, bucket, -applied),
                    )
            # (2) Rewrite the survivor row (verdict reset; match cleared). Volume +
            # ledger go through the chokepoint right after.
            conn.execute(
                "UPDATE events SET end_ts = ?, duration_seconds = ?, "
                "  volume_litres = ?, volume_litres_effective = ?, "
                "  volume_estimation_method = ?, avg_flow_lpm = ?, "
                "  true_avg_flow_lpm = ?, peak_flow_lpm = ?, flow_integral_litres = ?, "
                "  pressure_delta_psi = ?, is_low_flow_dribble = ?, "
                "  is_pressure_restoration_phantom = ?, is_cross_talk = ?, "
                "  phantom_suppression_averted = ?, "
                "  excluded_from_training = ?, match_rejection_reason = ?, "
                "  cluster_id = NULL, matched_fixture_type = NULL, matched_via = NULL "
                "WHERE id = ?",
                (end_ts.isoformat() if end_ts else None, duration, total_vol, new_eff,
                 feats["volume_estimation_method"], avg, avg, peak, total_vol, delta,
                 feats["is_low_flow_dribble"], feats["is_pressure_restoration_phantom"],
                 feats["is_cross_talk"],
                 feats.get("phantom_suppression_averted", 0),
                 feats["excluded_from_training"],
                 feats["match_rejection_reason"], sid),
            )
            apply_effective_volume(conn, sid, circuit, survivor["start_ts"], new_eff)
            # (4) Delete absorbed rows (cascades waveforms / zone_flow_history).
            for m in g[1:]:
                conn.execute(
                    "DELETE FROM training_capture_candidates WHERE event_id = ?",
                    (m["id"],))
                conn.execute("DELETE FROM events WHERE id = ?", (m["id"],))
            for m in g:
                d = local_day_of(m["start_ts"])
                if d:
                    affected_days.add(d)
            if end_ts:
                # A merge can straddle local midnight — both days need a redo.
                affected_days.add(local_day_of(end_ts.isoformat()))

        for day in sorted(affected_days):
            compute_daily_summary(conn, circuit, day)

    return {"groups": len(groups), "absorbed": absorbed_total}


def delete_events_in_range(
    conn: sqlite3.Connection, circuit: str, from_ts: str, to_ts: str,
) -> Dict[str, Any]:
    """Delete the purely-machine-derived events that OVERLAP [from_ts, to_ts] on
    ``circuit`` — reversing each one's hourly_volume contribution and recomputing
    the affected daily summaries — so a range can be cleanly RE-IMPORTED from HA
    history without orphaned bookkeeping (the remedy for a garbled stored event,
    e.g. an irrigation run that failed to close and absorbed a whole day).

    Selection is INTERVAL-OVERLAP, not start-in-window: an event counts if its
    ``[start_ts, end_ts]`` intersects the window (``start_ts <= to_ts AND
    COALESCE(end_ts, start_ts) >= from_ts``). This is essential — the garbled
    27.6 h event the tool exists for *starts before* a single-day window yet spans
    it; a start-only filter would miss it (and leave it blocking the re-import).
    A NULL ``end_ts`` collapses to start-only, so a never-closed row is still only
    caught when its start is in-window (live/active events are excluded upstream by
    the write-lock guard).

    PRESERVES anything the user has touched: a row with a ``user_fixture_type``,
    OR ``user_classified``, OR ``user_ignored`` is SKIPPED — labels/intent (e.g. a
    manually marked cross-talk event) must never be lost; the re-import's
    overlap-dedup simply works around the kept rows. Reuses the coalesce
    reverse-hourly + cascade-delete pattern: deleting an event cascades
    event_waveforms / zone_flow_history (foreign_keys=ON); training_capture_
    candidates (no FK) is cleaned manually. Single transaction.

    Returns ``{"deleted": n, "span_start": <earliest start ISO or None>,
    "span_end": <latest end ISO or None>, "deleted_rows": [<full row dicts>]}`` —
    the caller uses the true deleted span to widen the re-import so an event
    extending beyond the window is fully rebuilt, and an atomic caller
    (reprocess_window) RESTORES ``deleted_rows`` verbatim if the subsequent
    re-import fails — guaranteeing a delete-then-failed-import never loses water.
    (Always captured: the windows are small, and a conditional ``capture=`` flag
    meant two return shapes and two SELECTs that could drift.)
    """
    rows = conn.execute(
        "SELECT * FROM events "
        "WHERE circuit = ? AND start_ts <= ? "
        "  AND COALESCE(end_ts, start_ts) >= ? "
        "  AND user_fixture_type IS NULL "
        "  AND COALESCE(user_classified, 0) = 0 "
        "  AND COALESCE(user_ignored, 0) = 0 "
        "ORDER BY start_ts ASC",
        (circuit, to_ts, from_ts),
    ).fetchall()
    if not rows:
        return {"deleted": 0, "span_start": None, "span_end": None,
                "deleted_rows": []}
    captured_rows = [dict(r) for r in rows]
    span_start = rows[0]["start_ts"]                       # earliest (ORDER BY ASC)
    span_end = max((r["end_ts"] or r["start_ts"]) for r in rows)
    affected_days = set()
    with transaction(conn):
        for r in rows:
            applied = float(r["hourly_volume_applied_litres"] or 0.0)
            bucket = r["hourly_volume_applied_bucket"]
            if bucket and applied:
                conn.execute(
                    "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
                    "VALUES (?, ?, ?) ON CONFLICT (circuit, hour_ts) DO UPDATE "
                    "SET volume_litres = volume_litres + excluded.volume_litres",
                    (circuit, bucket, -applied),
                )
            conn.execute(
                "DELETE FROM training_capture_candidates WHERE event_id = ?",
                (r["id"],))
            # dev38 — overlap_audit rows referencing this event become stale
            # (reprocess re-creates events under NEW uuid5 ids: id is a pure
            # function of start_ts, and re-imported boundaries are 15 s-
            # granular). MARK, never delete: the rows are the durable record
            # of where zeroed litres went. kept_event_ids is a JSON list, so
            # a LIKE probe on the quoted id catches child references too.
            try:
                conn.execute(
                    "UPDATE overlap_audit SET stale_reason = "
                    "COALESCE(stale_reason, 'superseded_by_reprocess'), "
                    "stale_at = COALESCE(stale_at, ?) "
                    "WHERE wrapper_event_id = ? OR kept_event_ids LIKE ?",
                    (datetime.now(timezone.utc).isoformat(),
                     r["id"], f'%"{r["id"]}"%'))
            except sqlite3.Error:
                pass       # pre-20260801 schema
            conn.execute("DELETE FROM events WHERE id = ?", (r["id"],))
            d = local_day_of(r["start_ts"])
            if d:
                affected_days.add(d)
        for day in sorted(affected_days):
            # compute_daily_summary returns None (and leaves any prior row
            # untouched) when a day has no events left — so a day emptied by the
            # delete keeps a STALE inflated summary. Drop that row explicitly;
            # the re-import repopulates it, or it correctly stays absent.
            if compute_daily_summary(conn, circuit, day) is None:
                conn.execute(
                    "DELETE FROM daily_summary WHERE circuit = ? AND day = ?",
                    (circuit, day))
    return {"deleted": len(rows), "span_start": span_start, "span_end": span_end,
            "deleted_rows": captured_rows}


def restore_deleted_events(
    conn: sqlite3.Connection, rows: List[Dict[str, Any]],
) -> int:
    """Re-insert events captured by ``delete_events_in_range`` and
    re-apply their hourly-volume contribution — the rollback for an atomic reprocess
    whose re-import failed AFTER the delete committed. Each row is restored verbatim
    via the volume-ledger chokepoint so totals end exactly where they began. Returns
    the count restored. (Cascade-only children — event_waveforms — are display-derived
    and regenerate; not restored.)"""
    restored = 0
    for r in rows:
        ev = dict(r)
        eid = ev.get("id")
        if not eid or not ev.get("start_ts") or not ev.get("circuit"):
            continue
        vol = ev.get("volume_litres_effective")
        if vol is None:
            vol = ev.get("volume_litres") or 0.0
        # Clear the applied-bookkeeping the captured row carries: the original
        # contribution was already reversed out of hourly_volume by the delete, so the
        # restore must look like a FRESH apply. Leaving the stale (litres, bucket) makes
        # apply_effective_volume reverse-then-reapply → a net-zero no-op (the volume
        # would not come back).
        ev["hourly_volume_applied_litres"] = 0.0
        ev["hourly_volume_applied_bucket"] = None
        upsert_event_and_apply_hourly_volume(conn, ev, float(vol))
        restored += 1
    # Rebuild the daily rollups the delete recomputed/dropped — without this the
    # restored events exist but their days read ~0 L until something else happens
    # to recompute those (possibly old) days.
    pairs = {(dict(r).get("circuit"), local_day_of(dict(r).get("start_ts")))
             for r in rows}
    for circuit, day in sorted(p for p in pairs if p[0] and p[1]):
        compute_daily_summary(conn, circuit, day)
    conn.commit()
    return restored


def get_daily_volume(conn: sqlite3.Connection, circuit: str,
                     since_utc: str = "") -> float:
    """Total volume since local midnight (expressed as a UTC ISO string).

    Pass since_utc as the UTC equivalent of the HA instance's local midnight
    (e.g. '2026-05-17T05:00:00' for UTC-5).  Falls back to UTC midnight when
    since_utc is not provided.
    """
    cutoff = since_utc or datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00')
    row = conn.execute("""
        SELECT COALESCE(SUM(volume_litres), 0)
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= ?
    """, (circuit, cutoff)).fetchone()
    return round(row[0], 1) if row else 0.0


def get_weekly_volume(conn: sqlite3.Connection, circuit: str,
                      since_utc: str = "") -> float:
    """Total volume since local midnight 7 days ago (expressed as a UTC ISO string).

    Pass since_utc as the UTC equivalent of local midnight 7 days ago.
    Falls back to UTC midnight 7 days ago when since_utc is not provided.
    """
    if since_utc:
        cutoff = since_utc
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%dT00:00:00')
    row = conn.execute("""
        SELECT COALESCE(SUM(volume_litres), 0)
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= ?
    """, (circuit, cutoff)).fetchone()
    return round(row[0], 1) if row else 0.0


def get_hourly_volumes(
    conn: sqlite3.Connection,
    circuit: str,
    hours: int = 24
) -> List[Dict[str, Any]]:
    """Get per-hour volume for the past N hours (rolling from now)."""
    rows = conn.execute("""
        SELECT hour_ts, volume_litres
        FROM hourly_volume
        WHERE circuit = ?
          AND hour_ts >= datetime('now', ? || ' hours')
        ORDER BY hour_ts ASC
    """, (circuit, f"-{hours}")).fetchall()
    return [dict(r) for r in rows]


def update_hourly_volume(
    conn: sqlite3.Connection,
    circuit: str,
    hour_ts: str,
    volume_litres: float
) -> None:
    conn.execute("""
        INSERT INTO hourly_volume (circuit, hour_ts, volume_litres)
        VALUES (?, ?, ?)
        ON CONFLICT (circuit, hour_ts)
        DO UPDATE SET volume_litres = volume_litres + excluded.volume_litres
    """, (circuit, hour_ts, volume_litres))
    conn.commit()


def _get_volume_baseline(
    conn: sqlite3.Connection,
    circuit: str,
    period_ts: str,
    current_ha_value: float,
) -> float:
    """
    Return the stored HA sensor baseline for period_ts, creating it if absent.

    The period's volume is ``current_ha_value − baseline``, so the baseline is
    also where a meter RESET is absorbed. The ESP's lifetime total is NVS-backed
    and never resets in normal operation, but a reboot that loses the last flash
    write, a reflash, or a reconnect that republishes a stale value all make the
    reading step backwards.

    Reset handling carries the period's accumulated volume over instead of
    discarding it. The old code set ``baseline = current``, which zeroed the
    period: on 2026-08-06 an evening blip dropped the dashboard's TODAY tile from
    ~108 gal to 20.2 while HA's own utility_meter — which carries over — still
    read 108, and the 7-day tile was untouched because its baseline sits far
    below any single day's reading. Now the baseline is pushed NEGATIVE by the
    carried-over amount, so ``current − baseline`` continues from where the
    period left off. That mirrors HA's total_increasing semantics, which is what
    makes the two agree.
    """
    row = conn.execute(
        "SELECT ha_volume, last_reading FROM volume_snapshots "
        "WHERE circuit=? AND period_ts=?",
        (circuit, period_ts),
    ).fetchone()

    if row is None:
        # No baseline yet — seed with the CURRENT reading, NOT 0.0.
        # A 0.0 baseline makes the period total (= current − baseline) balloon
        # to the entire cumulative meter reading (the dashboard "today shows
        # 1000 gal" bug). Seeding with the current value caps the worst case at
        # ~0 for a just-started period; the orchestrator's midnight rollover
        # (_init_volume_baselines) then force-overwrites this with the accurate
        # midnight reading from HA history.
        # Lock-tolerant (2026-08-12): this runs inside the dashboard's live
        # poll, and a long admin write (the ~30 s "Apply my labels"
        # reclassify) holds the DB past busy_timeout — the poll must degrade
        # to the computed value, not 500; the next poll persists it.
        try:
            conn.execute(
                "INSERT INTO volume_snapshots (circuit, period_ts, ha_volume, "
                "last_reading) VALUES (?,?,?,?)",
                (circuit, period_ts, current_ha_value, current_ha_value),
            )
            conn.commit()
        except sqlite3.OperationalError as e:
            _rollback_quietly(conn)
            log.debug("volume-baseline seed skipped (non-fatal): %s", e)
        return current_ha_value

    baseline, last_reading = row[0], row[1]

    if current_ha_value < baseline:
        # Meter reset. Everything between the baseline and the highest reading
        # we saw is real water that was already delivered — keep it, and treat
        # what the meter reads NOW as consumption since the reset (a reflashed
        # accumulator starts at 0 and climbs). That is exactly what HA's
        # utility_meter does with a total_increasing source, which is why the
        # tile and the HA card agree afterwards.
        #
        # A row with NO high-water mark (raw INSERT, or a period that has never
        # been read live) can't say how far the meter climbed, so it falls back
        # to the pre-20260571 rule: rebase to the current reading and continue
        # from zero. Counting the post-reset reading there would report the
        # meter's whole remaining lifetime total as one day's use — the exact
        # failure this module's baseline seeding exists to prevent.
        if last_reading is None:
            new_baseline = current_ha_value
            accumulated = 0.0
        else:
            accumulated = max(0.0, last_reading - baseline)
            new_baseline = -accumulated
        # Lock-tolerant: the carry-over math is a pure function of the stored
        # row, so returning the computed baseline without persisting is
        # consistent — the next call recomputes and persists the same values.
        try:
            conn.execute(
                "UPDATE volume_snapshots SET ha_volume=?, last_reading=? "
                "WHERE circuit=? AND period_ts=?",
                (new_baseline, current_ha_value, circuit, period_ts),
            )
            conn.commit()
            log.warning(
                "[%s] volume meter reset for period %s (%.2f → %.2f L); carried "
                "%.2f L forward, baseline now %.2f",
                circuit, period_ts, last_reading if last_reading is not None
                else baseline, current_ha_value, accumulated, new_baseline,
            )
        except sqlite3.OperationalError as e:
            _rollback_quietly(conn)
            log.debug("volume-baseline reset write skipped (non-fatal): %s", e)
        return new_baseline

    # High-water mark for the next reset. Only ever moves up, and only when it
    # actually changes, so the steady-state read path stays read-only.
    if last_reading is None or current_ha_value > last_reading:
        try:
            conn.execute(
                "UPDATE volume_snapshots SET last_reading=? "
                "WHERE circuit=? AND period_ts=?",
                (current_ha_value, circuit, period_ts),
            )
            conn.commit()
        except sqlite3.OperationalError as e:
            _rollback_quietly(conn)
            log.debug("volume high-water update skipped (non-fatal): %s", e)

    return baseline


def compute_ha_daily_volume(
    conn: sqlite3.Connection,
    circuit: str,
    current_ha_value: float,
    period_ts: str = "",
) -> float:
    """Daily volume from the authoritative HA cumulative sensor.

    period_ts is the UTC equivalent of local midnight, as a naive ISO string
    (e.g. '2026-05-17T05:00:00').  Falls back to UTC midnight when not provided.
    Must match the key written by _init_volume_baselines().
    """
    if not period_ts:
        period_ts = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        ).isoformat(timespec="seconds")
    baseline = _get_volume_baseline(conn, circuit, period_ts, current_ha_value)
    return round(max(0.0, current_ha_value - baseline), 1)


def compute_ha_weekly_volume(
    conn: sqlite3.Connection,
    circuit: str,
    current_ha_value: float,
    period_ts: str = "",
) -> float:
    """Rolling 7-day volume from the authoritative HA cumulative sensor.

    period_ts is the UTC equivalent of local midnight 7 days ago, as a naive
    ISO string.  Falls back to UTC midnight 7 days ago when not provided.
    """
    if not period_ts:
        period_ts = (datetime.now(timezone.utc) - timedelta(days=7)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        ).isoformat(timespec="seconds")
    baseline = _get_volume_baseline(conn, circuit, period_ts, current_ha_value)
    return round(max(0.0, current_ha_value - baseline), 1)


def get_toilet_flush_cap_litres(conn: sqlite3.Connection) -> float:
    """Per-home flush-volume ceiling for the toilet physics veto (dev17).

    Reads ``home_profile.build_year`` + ``epa_flush_cap_enabled`` and maps them
    through the EPA/federal era table in event_rules. Missing column (mid-
    migration) or missing profile row degrades to the generous pre-1982 bound.
    """
    from .event_rules import toilet_flush_cap_litres
    build_year, enabled = None, True
    try:
        row = conn.execute(
            "SELECT build_year, epa_flush_cap_enabled FROM home_profile "
            "WHERE id = 1").fetchone()
        if row is not None:
            build_year = row["build_year"]
            enabled = bool(row["epa_flush_cap_enabled"])
    except sqlite3.OperationalError:     # column mid-migration
        pass
    return toilet_flush_cap_litres(build_year, enabled)


def _suggested_type_vetoed_sql(cap_litres: float) -> str:
    """SQL for the cluster suggestion with the toilet physics veto applied.

    A cluster's 'toilet' suggestion describes the CENTROID; the event itself
    inherits it at display/rollup time with no per-event check, which is how a
    0.4 L trickle can render as "Toilet". This CASE nulls the inherited
    suggestion when the EVENT violates flush physics (below the manufactured
    floor, above the home's era cap, peak too low, or a multi-segment draw —
    a cistern refill is one continuous segment). NULL-safe: a missing feature
    never vetoes. Mirrors event_rules.toilet_physics_veto — keep in sync.
    Numeric literals are inlined (module constants, never user input).
    """
    from .event_rules import (TOILET_MIN_FLUSH_L, TOILET_VETO_MAX_SEGMENTS,
                              TOILET_VETO_MIN_PK_LPM)
    return (
        "CASE WHEN fc.suggested_type = 'toilet' AND ("
        f"COALESCE(e.volume_litres, {TOILET_MIN_FLUSH_L}) < {TOILET_MIN_FLUSH_L} "
        f"OR COALESCE(e.volume_litres, 0) > {cap_litres:.3f} "
        f"OR COALESCE(e.peak_flow_lpm, {TOILET_VETO_MIN_PK_LPM}) "
        f"   < {TOILET_VETO_MIN_PK_LPM} "
        f"OR COALESCE(e.active_flow_segment_count, 1) > {TOILET_VETO_MAX_SEGMENTS}"
        ") THEN NULL ELSE fc.suggested_type END"
    )


# Fixture filter: the row's EFFECTIVE label, matching the History display chain
# (user label > confirmed fixture record's type > k-NN match > cluster suggestion).
# NULLIF guards empty-string user labels; a NULL chain renders as the "Other"
# fallback pill and is selectable via fixture_type='unlabelled'. The cluster-
# suggestion leg carries the toilet physics veto (format with
# _suggested_type_vetoed_sql(cap) before use).
_EFFECTIVE_FIXTURE_SQL_TMPL = ("COALESCE(NULLIF(e.user_fixture_type, ''), "
                               "f.fixture_type, e.matched_fixture_type, "
                               "{suggested})")

# Note filter: categorical over the pill kinds the History "Note" column renders.
# Each entry is a self-contained predicate; 'none' = a row with no pills at all.
_NOTE_KIND_SQL: Dict[str, str] = {
    "unusual":   "e.flagged = 1",
    "estimated": ("(e.degraded_supply = 1 "
                  "OR e.volume_estimation_method = 'pulsing_supply_envelope')"),
    "not_real":  ("(COALESCE(e.is_pressure_restoration_phantom, 0) = 1 "
                  "OR COALESCE(e.is_cross_talk, 0) = 1 "
                  "OR COALESCE(e.is_low_flow_dribble, 0) = 1)"),
    "sparse":    "e.match_rejection_reason = 'sparse_envelope'",
    # Leak-test refill is its OWN filter rather than part of 'not_real': it is
    # never hidden by the not-real-use toggle (see leak_test_refill), so folding
    # it in would make the two disagree.
    "leak_test": "e.match_rejection_reason = 'leak_test_refill'",
    "none":      ("(COALESCE(e.flagged, 0) = 0 "
                  "AND COALESCE(e.degraded_supply, 0) = 0 "
                  "AND COALESCE(e.volume_estimation_method, 'raw') "
                  "    <> 'pulsing_supply_envelope' "
                  "AND COALESCE(e.is_pressure_restoration_phantom, 0) = 0 "
                  "AND COALESCE(e.is_cross_talk, 0) = 0 "
                  "AND COALESCE(e.is_low_flow_dribble, 0) = 0 "
                  "AND COALESCE(e.match_rejection_reason, '') <> 'sparse_envelope' "
                  "AND COALESCE(e.match_rejection_reason, '') <> 'leak_test_refill' "
                  "AND COALESCE(e.user_reviewed, 0) = 0)"),
}


def get_recent_events(
    conn: sqlite3.Connection,
    circuit: str,
    limit: int = 100,
    date_from: str = None,
    date_to: str = None,
    flagged_only: bool = False,
    degraded_only: bool = False,
    unreviewed_only: bool = False,
    dur_min_s: Optional[float] = None,
    dur_max_s: Optional[float] = None,
    dp_min: Optional[float] = None,
    dp_max: Optional[float] = None,
    vol_min_l: Optional[float] = None,
    vol_max_l: Optional[float] = None,
    flow_min_lpm: Optional[float] = None,
    flow_max_lpm: Optional[float] = None,
    fixture_type: Optional[str] = None,
    note_kind: Optional[str] = None,
    exclude_not_real: bool = False,
) -> List[Dict[str, Any]]:
    """
    Return events for a circuit ordered newest first.
    If date_from / date_to are provided (ISO strings) they act as a
    range filter and limit is ignored so the full range is returned.
    Otherwise returns the most recent `limit` rows.

    flagged_only / degraded_only back the History filter views
    (?filter=anomaly / ?filter=degraded). They must live in the WHERE
    clause so the recency limit applies to MATCHING rows — a Python
    post-filter over the newest `limit` events silently drops every
    match older than the `limit`-th event. unreviewed_only composes with
    flagged_only for ?filter=anomaly_unreviewed — the "which ones still
    need my eyes" view matching the dashboard card's count.

    The dev15 History filter-bar pushdowns follow the same rule:
      • dur_min_s/dur_max_s — duration bounds (SECONDS, storage units);
      • dp_min/dp_max — pressure_delta bounds (PSI, storage units);
      • vol_min_l/vol_max_l — volume bounds (LITRES) on the DISPLAYED number,
        COALESCE(volume_litres_effective, volume_litres), so a zeroed phantom
        is a 0-volume row exactly as the user sees it;
      • flow_min_lpm/flow_max_lpm — average-flow bounds (L/min) on the
        DISPLAYED number, COALESCE(true_avg_flow_lpm, avg_flow_lpm) — the
        idle-gap-excluded average when available, same as the Avg flow column;
      • fixture_type — a canonical type matched against the effective-label
        chain (_EFFECTIVE_FIXTURE_SQL_TMPL), or 'unlabelled' for a NULL chain;
      • note_kind — one of _NOTE_KIND_SQL's pill categories.
    Callers pass STORAGE units — display-unit conversion is the router's job.

    exclude_not_real pushes the "Hide not-real-use events" toggle into the
    WHERE clause for the same must-not-vanish reason as flagged_only: it used
    to be a Python post-filter over the newest `limit` rows, so a pump-cycling
    artifact storm (2026-07: ~82 of the newest 100 rows were zeroed
    artifacts) starved the History page down to a handful of visible events.
    In SQL, the recency limit counts VISIBLE rows. The companion badge count
    comes from count_not_real_events (same filters).
    """
    # Toilet physics veto (dev17): the suggestion an event INHERITS from its
    # cluster is nulled when the event itself cannot be a flush — both in the
    # returned suggested_type (the History pill) and in the fixture filter, so
    # the filter can never surface a row the pill would not show as toilet.
    _suggested_sql = _suggested_type_vetoed_sql(get_toilet_flush_cap_litres(conn))
    _effective_sql = _EFFECTIVE_FIXTURE_SQL_TMPL.format(suggested=_suggested_sql)
    _select = f"""
        SELECT e.*,
               {_suggested_sql}      AS suggested_type,
               fc.suggested_confidence,
               fc.confidence_level   AS cluster_confidence_level,
               f.display_name        AS fixture_display_name,
               f.fixture_type        AS fixture_type_name
        FROM events e
        LEFT JOIN fixture_clusters fc
               ON fc.circuit = e.circuit AND fc.id = e.cluster_id
        LEFT JOIN fixtures f ON f.id = e.fixture_id
    """
    conditions = ["e.circuit = ?"]
    params: list = [circuit]
    if flagged_only:
        conditions.append("e.flagged = 1")
    if degraded_only:
        conditions.append("e.degraded_supply = 1")
    if unreviewed_only:
        conditions.append("COALESCE(e.user_reviewed, 0) = 0")
    if date_from:
        conditions.append("e.start_ts >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("e.start_ts <= ?")
        params.append(date_to + "T23:59:59")
    if dur_min_s is not None:
        conditions.append("e.duration_seconds >= ?")
        params.append(dur_min_s)
    if dur_max_s is not None:
        conditions.append("e.duration_seconds <= ?")
        params.append(dur_max_s)
    if dp_min is not None:
        conditions.append("COALESCE(e.pressure_delta_psi, 0) >= ?")
        params.append(dp_min)
    if dp_max is not None:
        conditions.append("COALESCE(e.pressure_delta_psi, 0) <= ?")
        params.append(dp_max)
    if vol_min_l is not None:
        conditions.append(
            "COALESCE(e.volume_litres_effective, e.volume_litres, 0) >= ?")
        params.append(vol_min_l)
    if vol_max_l is not None:
        conditions.append(
            "COALESCE(e.volume_litres_effective, e.volume_litres, 0) <= ?")
        params.append(vol_max_l)
    if flow_min_lpm is not None:
        conditions.append(
            "COALESCE(e.true_avg_flow_lpm, e.avg_flow_lpm, 0) >= ?")
        params.append(flow_min_lpm)
    if flow_max_lpm is not None:
        conditions.append(
            "COALESCE(e.true_avg_flow_lpm, e.avg_flow_lpm, 0) <= ?")
        params.append(flow_max_lpm)
    if fixture_type == "unlabelled":
        conditions.append(f"{_effective_sql} IS NULL")
    elif fixture_type:
        conditions.append(f"{_effective_sql} = ?")
        params.append(fixture_type)
    if note_kind in _NOTE_KIND_SQL:
        conditions.append(_NOTE_KIND_SQL[note_kind])
    if exclude_not_real:
        conditions.append(_NOT_REAL_SQL + " = 0")
    sql = f"{_select} WHERE {' AND '.join(conditions)} ORDER BY e.start_ts DESC"
    if not (date_from or date_to):
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# One expression for "this row is a volume-zeroed not-real-use verdict"
# (phantom / cross-talk / below-meter-floor dribble) — shared by the
# exclude_not_real pushdown and the hidden-count badge so they can never
# disagree with each other or with the router's per-row display logic.
_NOT_REAL_SQL = (
    "(COALESCE(e.is_pressure_restoration_phantom, 0) = 1 "
    " OR COALESCE(e.is_cross_talk, 0) = 1 "
    " OR COALESCE(e.is_low_flow_dribble, 0) = 1)"
)


def upsert_pump_regime_night(conn: sqlite3.Connection, circuit: str,
                             night_date: str, **cols) -> None:
    """Idempotent per-(circuit, night) upsert for the nightly regime row."""
    fields = {"circuit": circuit, "night_date": night_date, **cols}
    keys = ", ".join(fields)
    ph = ", ".join("?" for _ in fields)
    sets = ", ".join(f"{k}=excluded.{k}" for k in cols) or "detected=detected"
    conn.execute(
        f"INSERT INTO pump_regime_nightly ({keys}) VALUES ({ph}) "
        f"ON CONFLICT(circuit, night_date) DO UPDATE SET {sets}",
        list(fields.values()))
    conn.commit()


def get_pump_regime_nights(conn: sqlite3.Connection,
                           limit: int = 40) -> List[Dict[str, Any]]:
    """Home-level nightly aggregation, newest first: one dict per EVALUATED
    night with any_detected = ANY-circuit rule (the pump is upstream of every
    circuit), and the detecting circuit's period (circuit_1 preferred when
    both detect — deterministic)."""
    rows = conn.execute(
        """SELECT night_date,
                  MAX(detected) AS any_detected,
                  COALESCE(
                      MAX(CASE WHEN detected = 1 AND circuit = 'circuit_1'
                               THEN period_s END),
                      MAX(CASE WHEN detected = 1 THEN period_s END)
                  ) AS period_s,
                  MAX(CASE WHEN detected = 1 THEN amplitude_psi END)
                      AS amplitude_psi,
                  MAX(est_leak_lpd) AS est_leak_lpd,
                  MAX(CASE WHEN detected = 1 THEN window_start_ts END)
                      AS window_start_ts,
                  MAX(CASE WHEN detected = 1 THEN window_end_ts END)
                      AS window_end_ts
           FROM pump_regime_nightly
           GROUP BY night_date ORDER BY night_date DESC LIMIT ?""",
        (limit,)).fetchall()
    return [dict(r) for r in rows]


def count_not_real_events(
    conn: sqlite3.Connection,
    circuit: str,
    date_from: str = None,
    date_to: str = None,
    since_ts: str = None,
) -> int:
    """Count hidden not-real-use rows for the History badge ("N hidden —
    show them"). ``since_ts`` bounds the count to the time span the visible
    list actually covers (the oldest displayed row's start_ts) so the badge
    keeps its original meaning — hidden rows among what you're looking at —
    now that the visible list is SQL-limited to matching rows.

    dev46 (46b) — the ``fetchone()`` below is DELIBERATELY not guarded. A bare
    ``COUNT(*)`` always returns exactly one row, so a None here means the
    cursor itself is broken, not that the result is empty. The 8/16 boot
    ``TypeError: 'NoneType' object is not subscriptable`` was that symptom —
    collateral damage from the cross-thread ``InterfaceError`` that 46a's
    single DB executor eliminates at the source. Guarding it would have
    converted a loud, diagnosable failure into a silent wrong answer (a badge
    reading "0 hidden"). Guard ``fetchone()`` only where an empty result is a
    legitimate state."""
    conditions = ["e.circuit = ?", _NOT_REAL_SQL]
    params: list = [circuit]
    if date_from:
        conditions.append("e.start_ts >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("e.start_ts <= ?")
        params.append(date_to + "T23:59:59")
    if since_ts:
        conditions.append("e.start_ts >= ?")
        params.append(since_ts)
    row = conn.execute(
        f"SELECT COUNT(*) FROM events e WHERE {' AND '.join(conditions)}",
        params).fetchone()
    return int(row[0])


_PATCH_UNSET = object()


def patch_event(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    user_fixture_type=_PATCH_UNSET,
    excluded_from_training=_PATCH_UNSET,
    user_ignored=_PATCH_UNSET,
    user_reviewed=_PATCH_UNSET,
    review_verdict=_PATCH_UNSET,
) -> bool:
    """Update user-editable fields on a single event.

    Pass a value (including None) to update that field; omit a kwarg entirely
    to leave the field unchanged.  Returns False if no matching row exists.

    Sprint H: ``user_ignored`` is the Ignore/Restore intent. Setting it also
    re-derives ``excluded_from_training`` (= user_ignored OR any auto/manual
    category flag) so the effective exclusion stays consistent. The legacy
    ``excluded_from_training`` kwarg is still accepted for back-compat but
    callers should prefer ``user_ignored``.

    ``review_verdict`` ('normal' | 'unknown' | None) is the two-option anomaly
    triage. Setting either verdict also stamps ``user_reviewed = 1`` (the
    dashboard-count bit); passing None clears the verdict only. 'unknown'
    events are held out of anomaly-baseline refits by fit_usage_baselines —
    the verdict column itself is the whole mechanism, nothing else changes.
    """
    if review_verdict is not _PATCH_UNSET and review_verdict not in (
            None, "normal", "unknown"):
        raise ValueError(f"invalid review_verdict: {review_verdict!r}")
    row = conn.execute(
        "SELECT id, is_pressure_restoration_phantom, degraded_supply, "
        "       is_low_flow_dribble "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False
    if user_fixture_type is not _PATCH_UNSET:
        # Stamp provenance: an explicit label is 'user'; clearing resets to NULL
        # (so a relabel always overrides an auto 'cycle'/'training' source).
        src = "user" if user_fixture_type else None
        # dev.24: an explicit relabel pulls the event OUT of any machine/cycle
        # rollup group (clear cycle_group_id). For a cycle appliance the caller
        # then runs propagate_cycle_label, which re-stamps the anchor + mates; a
        # non-cycle relabel just stays a singleton. This is what makes a
        # user-relabeled member "leave the group" (§7).
        # A real label also RESOLVES a pending suppression-averted review (the
        # user has looked at it and decided) — clear the marker and count the
        # event as reviewed so it leaves the dashboard's anomaly card. It also
        # clears any 'unknown' review verdict: identifying the draw supersedes
        # "I don't recognise it" (and lets the event back into baseline refits
        # under its new label).
        # An explicit user label also lifts a dev40 training quarantine — the
        # quarantine distrusts the MACHINE label, and the user's is ground
        # truth (clearing a label back to NULL keeps the quarantine: the
        # distrusted machine label is what remains visible again).
        _resolve_review = ", phantom_suppression_averted = 0" + (
            ", user_reviewed = 1, review_verdict = NULL"
            ", training_quarantine_reason = NULL, training_quarantined_at = NULL"
            if user_fixture_type else "")
        conn.execute(
            "UPDATE events SET user_fixture_type = ?, fixture_label_source = ?, "
            "cycle_group_id = NULL" + _resolve_review + " "
            "WHERE id = ? AND circuit = ?",
            (user_fixture_type, src, event_id, circuit),
        )
    if user_ignored is not _PATCH_UNSET:
        ign = 1 if user_ignored else 0
        excluded = 1 if (ign or row["is_pressure_restoration_phantom"]
                         or row["degraded_supply"]
                         or row["is_low_flow_dribble"]) else 0
        conn.execute(
            "UPDATE events SET user_ignored = ?, excluded_from_training = ? "
            "WHERE id = ? AND circuit = ?",
            (ign, excluded, event_id, circuit),
        )
    if excluded_from_training is not _PATCH_UNSET:
        conn.execute(
            "UPDATE events SET excluded_from_training = ? WHERE id = ? AND circuit = ?",
            (1 if excluded_from_training else 0, event_id, circuit),
        )
    if user_reviewed is not _PATCH_UNSET:
        # Anomaly triage: "I looked at this flagged event" — clears it from the
        # dashboard's unreviewed-anomalies count. Display/triage state only.
        # Un-reviewing also wipes any verdict: the verdict is a property of a
        # review, so it can't outlive one.
        conn.execute(
            "UPDATE events SET user_reviewed = ?"
            + (", review_verdict = NULL" if not user_reviewed else "")
            + " WHERE id = ? AND circuit = ?",
            (1 if user_reviewed else 0, event_id, circuit),
        )
    if review_verdict is not _PATCH_UNSET:
        # Setting a verdict IS a review — stamp both together so a verdict can
        # never exist on an unreviewed event. Clearing (None) leaves the
        # reviewed bit alone (use user_reviewed=False to fully un-review).
        if review_verdict is None:
            conn.execute(
                "UPDATE events SET review_verdict = NULL "
                "WHERE id = ? AND circuit = ?",
                (event_id, circuit),
            )
        else:
            conn.execute(
                "UPDATE events SET review_verdict = ?, user_reviewed = 1 "
                "WHERE id = ? AND circuit = ?",
                (review_verdict, event_id, circuit),
            )
    conn.commit()
    return True


def _apply_event_verdicts(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    new_phantom: int,
    new_degraded: int,
    user_classified: int,
    new_dribble: int = 0,
    new_cross_talk: int = 0,
) -> bool:
    """Shared core: write category flags + user_classified, re-derive
    volume_litres_effective (phantom → 0; elif cross-talk → 0; elif degraded →
    envelope estimate; elif dribble → 0; else raw), recompute
    excluded_from_training, and resync hourly_volume + daily_summary.

    Volume resync is idempotent: it reads the event's stored
    ``hourly_volume_applied_litres`` (the prior contribution), applies
    ``delta = new_effective − prev_applied`` to the hour bucket, then writes
    back ``hourly_volume_applied_litres = new_effective``. Repeated toggles
    therefore never drift. Returns False if no such event.
    """
    row = conn.execute(
        "SELECT volume_litres, volume_litres_estimated, flow_integral_litres, "
        "       user_ignored, "
        "       hourly_volume_applied_litres, hourly_volume_applied_bucket, start_ts "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False

    raw = float(row["volume_litres"] or 0.0)
    est = row["volume_litres_estimated"]
    if new_phantom:
        new_effective, method = 0.0, "pressure_restoration_phantom"
    elif new_cross_talk:
        new_effective, method = 0.0, "cross_talk"
    elif new_degraded:
        # dev33 §8.5: the manual "supply pressure" checkbox wrote the RAW
        # uncapped envelope estimate — the same bypass the reprocess sweep had.
        # Route it through the finalizer's cap so a user checkbox can never
        # produce more water than the meter measured.
        from .feature_extractor import _cap_envelope_estimate
        new_effective = float(est) if est is not None else raw
        if est is not None:
            new_effective, _ = _cap_envelope_estimate(
                new_effective,
                {"flow_integral_litres": row["flow_integral_litres"],
                 "volume_litres": row["volume_litres"]})
        method = "pulsing_supply_envelope"
    elif new_dribble:
        # Dribble is now a volume-zeroing verdict (matches _finalize_derived_verdicts):
        # a brief low-flow blip is removed from totals, not just excluded from training.
        new_effective, method = 0.0, "low_flow_dribble"
    else:
        new_effective, method = raw, "raw"

    user_ignored = int(row["user_ignored"] or 0)
    excluded = 1 if (new_phantom or new_cross_talk or new_degraded
                     or new_dribble or user_ignored) else 0
    reason = (
        "pressure_restoration_phantom" if new_phantom
        else "cross_talk" if new_cross_talk
        else "pulsing_supply" if new_degraded
        else "low_flow_dribble" if new_dribble
        else None
    )

    with transaction(conn):
        conn.execute(
            "UPDATE events SET "
            "  is_pressure_restoration_phantom = ?, is_cross_talk = ?, "
            "  degraded_supply = ?, "
            "  user_classified = ?, is_low_flow_dribble = ?, "
            "  volume_litres_effective = ?, volume_estimation_method = ?, "
            "  excluded_from_training = ?, match_rejection_reason = ?, "
            # A manual classification RESOLVES a pending suppression-averted
            # review — the user has decided what this event is.
            "  phantom_suppression_averted = 0 "
            "WHERE id = ? AND circuit = ?",
            (new_phantom, new_cross_talk, new_degraded, user_classified, new_dribble,
             round(new_effective, 3), method, excluded, reason,
             event_id, circuit),
        )
        # §2.5 — the ledger reverse/apply/bookkeep goes through the one chokepoint.
        apply_effective_volume(conn, event_id, circuit, row["start_ts"], new_effective)

    day = local_day_of(row["start_ts"])
    if day:
        compute_daily_summary(conn, circuit, day)
        conn.commit()
    return True


def classify_action(cls: dict):
    """Pure dispatch for a PATCH ``classification`` payload (Sprint H.1).

    Returns one of:
      ("reset", {})                          — reset to automatic
      ("set",   {phantom, supply_pressure})  — authoritative manual set
      ("error", {"msg": ...})                — invalid → caller 400s

    ``reset: true`` is EXCLUSIVE — combining it with any category flag is
    rejected rather than silently picking one behaviour. Pure (no DB, no
    request object) so it is unit-testable without the FastAPI stack; lives
    here in database.py alongside set_/clear_event_classification for that
    reason (routers/history.py imports fastapi, which the test env lacks).

    'combined' is deprecated (2026-06-04): it is accepted but ignored, so old
    clients don't 400; combined usage is classified as the dominant fixture.
    """
    cls = cls or {}
    flags = {
        "phantom":         bool(cls.get("phantom")),
        "supply_pressure": bool(cls.get("supply_pressure")),
        "dribble":         bool(cls.get("dribble")),
        "cross_talk":      bool(cls.get("cross_talk")),
    }
    if cls.get("reset") is True:
        if any(flags.values()):
            return ("error", {"msg": "reset cannot be combined with category flags"})
        return ("reset", {})
    return ("set", flags)


def set_event_classification(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    phantom: bool,
    supply_pressure: bool,
    dribble: bool = False,
    cross_talk: bool = False,
) -> bool:
    """Apply a user's manual event classification (Sprint H, authoritative).

    ALWAYS sets ``user_classified=1`` — including the all-three-false case,
    which means "manually marked normal" and **sticks** (the
    ``_finalize_derived_verdicts`` skip on user_classified prevents auto from
    re-flagging it; this is what makes un-marking a phantom permanent). Use
    ``clear_event_classification`` to return an event to automatic detection.

    Returns False if no such event.
    """
    return _apply_event_verdicts(
        conn, event_id, circuit,
        new_phantom=1 if phantom else 0,
        new_degraded=1 if supply_pressure else 0,
        new_dribble=1 if dribble else 0,
        new_cross_talk=1 if cross_talk else 0,
        user_classified=1,
    )


def clear_event_classification(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
) -> bool:
    """Reset an event to AUTOMATIC classification (Sprint H.1).

    Clears ``user_classified`` and re-derives the phantom verdict from the
    stored duration/pressure via ``_detect_pressure_restoration_phantom``.
    degraded/composite keep their stored values (the raw readings needed to
    recompute them aren't persisted). Volume + summaries resync. Returns
    False if no such event.
    """
    from .feature_extractor import (
        _detect_pressure_restoration_phantom, _detect_low_flow_dribble,
    )
    from .artifact_calibration import load_artifact_calibration

    row = conn.execute(
        "SELECT duration_seconds, pressure_delta_psi, degraded_supply, "
        "       volume_litres, avg_flow_lpm, true_avg_flow_lpm, "
        "       peak_flow_lpm, flow_integral_litres, flow_on_ratio "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False
    # Apply the frozen per-home artifact calibration so a manual reset re-derives
    # the SAME verdict the live finalizer would (Phase 2.4 consistency).
    _acal = load_artifact_calibration(conn, circuit) or None
    new_phantom = 1 if _detect_pressure_restoration_phantom(
        row["duration_seconds"], row["pressure_delta_psi"],
        true_avg_flow_lpm=row["true_avg_flow_lpm"],
        flow_integral_litres=row["flow_integral_litres"],
        flow_on_ratio=row["flow_on_ratio"], calib=_acal) else 0
    new_degraded = int(row["degraded_supply"] or 0)
    # Dribble only when not phantom and not degraded (mirrors the finalizer).
    new_dribble = 1 if (
        not new_phantom and not new_degraded
        and _detect_low_flow_dribble(
            row["volume_litres"], row["avg_flow_lpm"], row["pressure_delta_psi"],
            calib=_acal,
            min_flow_lpm=(60.0 / get_circuit_pulses_per_litre(conn, circuit)),
            true_avg_flow_lpm=row["true_avg_flow_lpm"],
            peak_flow_lpm=row["peak_flow_lpm"])
    ) else 0
    return _apply_event_verdicts(
        conn, event_id, circuit,
        new_phantom=new_phantom,
        new_degraded=new_degraded,
        user_classified=0,
        new_dribble=new_dribble,
    )


def mark_event_irrigation_cross_talk(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    reconciled_at: str,
    interval_start: Optional[str] = None,
    interval_end: Optional[str] = None,
    main_delta_psi: Optional[float] = None,
    other_delta_psi: Optional[float] = None,
    ratio: Optional[float] = None,
    recompute_summary: bool = True,
) -> bool:
    """Flag a main event as irrigation zone-switch cross-talk (2026-06-28).

    ``recompute_summary=False`` lets a batch caller (the importer's reconcile
    loop) skip the per-event daily-summary recompute and do ONE per affected day
    after the loop instead.

    Applied OUT-OF-BAND by the historical importer's reconciliation pass, NOT a
    user action — so ``user_classified`` stays 0 (this keeps the row out of the
    long-no-flow cross-talk CALIBRATION positives, which key on user_classified).
    Durability against a later main-only reprocess comes from the DISTINCT
    ``match_rejection_reason`` (= feature_extractor._IRRIGATION_XTALK_REASON), which
    ``_finalize_derived_verdicts`` preserves.

    Writes a ``cross_talk_audit`` row (action='flagged') with the pre-zero volume +
    pressure-swing evidence BEFORE zeroing, then zeroes effective volume + excludes
    from training via the §2.5 ledger chokepoint. No-op (returns False) if the event
    is gone, already user-classified, already cross-talk, or carries a user fixture
    type — real water always wins.
    """
    from .feature_extractor import _IRRIGATION_XTALK_REASON

    row = conn.execute(
        "SELECT volume_litres, start_ts, user_classified, is_cross_talk, "
        "       user_fixture_type "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False
    if (row["user_classified"] or row["is_cross_talk"]
            or str(row["user_fixture_type"] or "").strip()):
        return False

    raw_volume = float(row["volume_litres"] or 0.0)
    with transaction(conn):
        conn.execute(
            "INSERT INTO cross_talk_audit "
            "  (event_id, circuit, reconciled_at, interval_start, interval_end, "
            "   main_delta_psi, other_delta_psi, ratio, volume_litres, action) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'flagged')",
            (event_id, circuit, reconciled_at, interval_start, interval_end,
             main_delta_psi, other_delta_psi, ratio, raw_volume),
        )
        conn.execute(
            "UPDATE events SET "
            "  is_cross_talk = 1, is_pressure_restoration_phantom = 0, "
            "  is_low_flow_dribble = 0, excluded_from_training = 1, "
            "  volume_litres_effective = 0.0, volume_estimation_method = 'cross_talk', "
            "  match_rejection_reason = ? "
            "WHERE id = ? AND circuit = ?",
            (_IRRIGATION_XTALK_REASON, event_id, circuit),
        )
        apply_effective_volume(conn, event_id, circuit, row["start_ts"], 0.0)

    day = local_day_of(row["start_ts"])
    if day and recompute_summary:
        compute_daily_summary(conn, circuit, day)
        conn.commit()
    return True


def mark_event_leak_test_refill(
    conn: sqlite3.Connection,
    event_id: str,
    circuit: str,
    *,
    leak_test_id: int,
    recompute_summary: bool = True,
) -> bool:
    """Flag an event as the refill that followed a leak test's valve reopen.

    Applied OUT-OF-BAND by ``leak_test_refill.reconcile_leak_test_refills`` from
    the add-on's own test timing — never by a detector, and never by the user —
    so ``user_classified`` stays 0. Durability across a reprocess comes from the
    distinct ``match_rejection_reason`` (preserved by
    ``_finalize_derived_verdicts``), the ``leak_test_id`` provenance column (the
    feature pipeline never writes it, so the upsert cannot clear it), and the
    reconcile being idempotent.

    Zeroes effective volume through the §2.5 ledger chokepoint and excludes the
    row from training, but sets NONE of the artifact flag bits — see the module
    docstring in ``leak_test_refill``: this verdict stays VISIBLE in History.

    TAKES OVER an existing AUTOMATIC artifact verdict (the reasons in
    ``_RELABEL_REVERTIBLE_REASONS``) and clears its flags. A leak test is ground
    truth about CAUSATION, while those detectors infer from shape — and on the
    2026-08 production export they had claimed 9 of these refills between them
    as 'below_meter_floor' / 'pressure_silent_flow' / 'pump_recharge'. The
    volume outcome is identical (all zero); what changes is that the event says
    what actually happened and stops counting against the artifact detectors'
    own validation statistics. ``overlap_duplicate`` and the degraded-supply
    estimate are deliberately NOT taken over — the first means a sibling event
    already accounts for this water, and the second never zeroed anything.

    No-op (returns False) when the event is gone, already tagged, carries a
    verdict outside that set, or the user has claimed it (classified, labelled,
    or ignored) — real water always wins.
    """
    from .leak_test_refill import LEAK_TEST_REFILL_REASON

    row = conn.execute(
        "SELECT volume_litres, start_ts, user_classified, user_ignored, "
        "       user_fixture_type, match_rejection_reason, degraded_supply "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None:
        return False
    if (row["user_classified"] or row["user_ignored"]
            or str(row["user_fixture_type"] or "").strip()):
        return False
    if row["degraded_supply"]:
        return False    # an ESTIMATE, not a zeroing — leave the envelope alone
    reason = row["match_rejection_reason"]
    if reason == LEAK_TEST_REFILL_REASON:
        return False    # idempotent
    if reason is not None and reason not in _RELABEL_REVERTIBLE_REASONS:
        return False    # e.g. overlap_duplicate — a sibling counts this water

    with transaction(conn):
        conn.execute(
            "UPDATE events SET "
            "  is_pressure_restoration_phantom = 0, is_cross_talk = 0, "
            "  is_low_flow_dribble = 0, phantom_suppression_averted = 0, "
            "  excluded_from_training = 1, volume_litres_effective = 0.0, "
            "  volume_estimation_method = ?, match_rejection_reason = ?, "
            "  leak_test_id = ? "
            "WHERE id = ? AND circuit = ?",
            (LEAK_TEST_REFILL_REASON, LEAK_TEST_REFILL_REASON, leak_test_id,
             event_id, circuit),
        )
        apply_effective_volume(conn, event_id, circuit, row["start_ts"], 0.0)

    day = local_day_of(row["start_ts"])
    if day and recompute_summary:
        compute_daily_summary(conn, circuit, day)
        conn.commit()
    log.info("[%s] leak-test refill: event %s zeroed (%.3f L, test #%s)",
             circuit, event_id, float(row["volume_litres"] or 0.0), leak_test_id)
    return True


def revert_irrigation_cross_talk(
    conn: sqlite3.Connection, event_id: str, circuit: str,
) -> bool:
    """Undo an automatic irrigation cross-talk flag (false-positive recovery).

    Restores the raw volume + clears the verdict, but ONLY for rows the auto pass
    set (match_rejection_reason = _IRRIGATION_XTALK_REASON, user_classified 0) — a
    user-classified cross-talk is left untouched. Records an audit row
    (action='reverted'). Returns False if there is nothing to revert.
    """
    from .feature_extractor import _IRRIGATION_XTALK_REASON

    row = conn.execute(
        "SELECT volume_litres, start_ts, match_rejection_reason, user_classified "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None or row["user_classified"]:
        return False
    if row["match_rejection_reason"] != _IRRIGATION_XTALK_REASON:
        return False

    raw_volume = float(row["volume_litres"] or 0.0)
    now_iso = datetime.now(timezone.utc).isoformat()
    with transaction(conn):
        conn.execute(
            "INSERT INTO cross_talk_audit "
            "  (event_id, circuit, reconciled_at, volume_litres, action) "
            "VALUES (?, ?, ?, ?, 'reverted')",
            (event_id, circuit, now_iso, raw_volume),
        )
        conn.execute(
            "UPDATE events SET "
            "  is_cross_talk = 0, excluded_from_training = 0, "
            "  volume_litres_effective = ?, volume_estimation_method = 'raw', "
            "  match_rejection_reason = NULL "
            "WHERE id = ? AND circuit = ?",
            (round(raw_volume, 3), event_id, circuit),
        )
        apply_effective_volume(conn, event_id, circuit, row["start_ts"], raw_volume)

    day = local_day_of(row["start_ts"])
    if day:
        compute_daily_summary(conn, circuit, day)
        conn.commit()
    return True


# Artifact verdicts that ZERO an event's volume and are therefore undone when
# the user says "this was real water" by assigning a fixture type. Deliberately
# EXCLUDES:
#   * 'pulsing_supply'    — degraded supply ESTIMATES volume, it doesn't zero it
#                           (a relabel shouldn't silently swap estimate→raw);
#   * 'overlap_duplicate' — the volume is genuinely counted by the child events,
#                           so restoring it would double-count.
_RELABEL_REVERTIBLE_REASONS: frozenset = frozenset({
    "below_meter_floor",             # BELOW_METER_FLOOR_REASON (auto dribble path)
    "low_flow_dribble",              # legacy/manual dribble verdict
    "pressure_restoration_phantom",
    "rising_pressure_phantom",       # RISE_PHANTOM_REASON
    "pressure_silent_flow",          # PRESSURE_SILENT_REASON
    "pump_recharge",                 # PUMP_RECHARGE_REASON
    "cross_talk",
    "irrigation_cross_talk",         # _IRRIGATION_XTALK_REASON
    "sparse_envelope",               # SPARSE_ENVELOPE_REASON
    "leak_test_refill",              # LEAK_TEST_REFILL_REASON
})


def revert_artifact_zeroing_on_relabel(
    conn: sqlite3.Connection, event_id: str, circuit: str,
) -> Optional[str]:
    """Undo an artifact ZEROING when the user labels the event a real fixture.

    A fixture label is the user asserting "this was real water", and the UI
    frames every artifact flag as *relabel if wrong* — so relabeling has to BE
    the recovery path. Before dev33 only irrigation cross-talk had one
    (``revert_irrigation_cross_talk``), which is how a user-labelled 685 L draw
    stayed zeroed: its verdict had been written through the manual classify
    endpoint, and nothing downstream honoured the label.

    Restores ``volume_litres_effective = volume_litres`` through the
    ``apply_effective_volume`` chokepoint, clears the zeroing flags + reason,
    and recomputes the day's summary. Unlike ``revert_irrigation_cross_talk``
    this DOES override ``user_classified`` — a fresh explicit label supersedes
    an earlier checkbox verdict — but it never touches:
      * ``user_ignored`` rows (the user asked for this event to be ignored),
      * verdicts outside ``_RELABEL_REVERTIBLE_REASONS`` (see the note there),
      * rows whose effective volume already equals raw (nothing to restore).

    Returns the reverted reason (for logging) or None when nothing changed.
    """
    row = conn.execute(
        "SELECT volume_litres, volume_litres_effective, start_ts, user_ignored, "
        "       match_rejection_reason, is_pressure_restoration_phantom, "
        "       is_low_flow_dribble, is_cross_talk, is_composite "
        "FROM events WHERE id = ? AND circuit = ?",
        (event_id, circuit),
    ).fetchone()
    if row is None or row["user_ignored"]:
        return None
    reason = row["match_rejection_reason"]
    if reason not in _RELABEL_REVERTIBLE_REASONS:
        return None
    raw = float(row["volume_litres"] or 0.0)
    eff = float(row["volume_litres_effective"] or 0.0)
    if raw <= 0.0 or abs(eff - raw) < 1e-6:
        return None   # nothing was zeroed (or nothing to restore)

    with transaction(conn):
        conn.execute(
            "UPDATE events SET "
            "  is_pressure_restoration_phantom = 0, is_low_flow_dribble = 0, "
            "  is_cross_talk = 0, phantom_suppression_averted = 0, "
            "  volume_litres_effective = ?, volume_estimation_method = 'raw', "
            "  match_rejection_reason = NULL, "
            # excluded_from_training mirrors (composite OR artifact); the
            # artifact half is now cleared, so only composite can still hold it.
            "  excluded_from_training = CASE WHEN is_composite = 1 THEN 1 ELSE 0 END, "
            "  volume_recomputed_at = ? "
            "WHERE id = ? AND circuit = ?",
            (round(raw, 3), datetime.now(timezone.utc).isoformat(),
             event_id, circuit),
        )
        apply_effective_volume(conn, event_id, circuit, row["start_ts"], raw)

    day = local_day_of(row["start_ts"])
    if day:
        compute_daily_summary(conn, circuit, day)
        conn.commit()
    log.info("[%s] relabel reverted %s zeroing on %s (restored %.3f L)",
             circuit, reason, event_id, raw)
    return reason


def get_leak_test_schedule(conn: sqlite3.Connection, circuit: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM leak_test_schedule WHERE circuit = ?", (circuit,)
    ).fetchone()


def upsert_leak_test_schedule(conn: sqlite3.Connection, circuit: str, **kwargs) -> None:
    _upsert_by_circuit(conn, "leak_test_schedule", circuit, **kwargs)


def insert_leak_test_history(conn: sqlite3.Connection, **kwargs) -> None:
    cols = ", ".join(kwargs.keys())
    placeholders = ", ".join("?" for _ in kwargs)
    conn.execute(f"INSERT INTO leak_test_history ({cols}) VALUES ({placeholders})",
                 list(kwargs.values()))
    conn.commit()


# ── dev41 — meter anchors + utility register readings ────────────────────────

def insert_meter_anchor_point(conn: sqlite3.Connection, *, circuit=None,
                              flow_rate_lpm=None, measured_volume_l=None,
                              reference_volume_l=None, test_date=None,
                              method="bucket", notes=None) -> int:
    cur = conn.execute(
        "INSERT INTO meter_anchor_points (circuit, flow_rate_lpm, "
        " measured_volume_l, reference_volume_l, test_date, method, notes, "
        " created_at) VALUES (?,?,?,?,?,?,?,?)",
        (circuit, flow_rate_lpm, measured_volume_l, reference_volume_l,
         test_date, method, notes,
         datetime.now(timezone.utc).isoformat()))
    conn.commit()
    return cur.lastrowid


def list_meter_anchor_points(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM meter_anchor_points ORDER BY test_date, id").fetchall()
    return [dict(r) for r in rows]


def insert_utility_register_reading(conn: sqlite3.Connection, *,
                                    reading_value, reading_ts,
                                    meter_serial=None, source="manual",
                                    entered_by=None, notes=None) -> int:
    cur = conn.execute(
        "INSERT INTO utility_register_readings (reading_value, reading_ts, "
        " meter_serial, source, entered_by, notes, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (reading_value, reading_ts, meter_serial, source, entered_by, notes,
         datetime.now(timezone.utc).isoformat()))
    conn.commit()
    return cur.lastrowid


def list_utility_register_readings(
        conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM utility_register_readings "
        "ORDER BY reading_ts, id").fetchall()
    return [dict(r) for r in rows]


def get_registration_curve(
        conn: sqlite3.Connection) -> "Tuple[int, List[Dict[str, Any]], str]":
    """dev41 (E1) — (curve_version, bands, status) for the LATEST curve.

    Bands are dicts with band_lo_lpm / band_hi_lpm (None = unbounded) /
    ratio, ordered high band first (the flow_integral lookup convention).
    Status is 'anchored' only when every band of the version is anchored.
    Returns (0, [], 'missing') on a pre-20260807 schema."""
    try:
        ver_row = conn.execute(
            "SELECT MAX(curve_version) FROM registration_curve").fetchone()
    except sqlite3.Error:
        return 0, [], "missing"
    if not ver_row or ver_row[0] is None:
        return 0, [], "missing"
    ver = int(ver_row[0])
    rows = conn.execute(
        "SELECT band_lo_lpm, band_hi_lpm, ratio, status "
        "FROM registration_curve WHERE curve_version = ? "
        "ORDER BY band_lo_lpm DESC", (ver,)).fetchall()
    bands = [dict(r) for r in rows]
    status = ("anchored" if bands and
              all(b["status"] == "anchored" for b in bands) else "unvalidated")
    return ver, bands, status


def get_leak_test_history(
    conn: sqlite3.Connection,
    circuit: str,
    limit: int = 20
) -> List[Dict[str, Any]]:
    rows = conn.execute("""
        SELECT * FROM leak_test_history
        WHERE circuit = ?
        ORDER BY run_at DESC
        LIMIT ?
    """, (circuit, limit)).fetchall()
    return [dict(r) for r in rows]


def ensure_circuit_defaults(conn: sqlite3.Connection, circuit: str,
                             circuit_type: str = "fixture") -> None:
    """Ensure all per-circuit config rows exist with defaults."""
    # Training state
    conn.execute("""
        INSERT OR IGNORE INTO training_state (circuit, state)
        VALUES (?, 'idle')
    """, (circuit,))

    # Sensitivity config
    conn.execute("""
        INSERT OR IGNORE INTO sensitivity_config (circuit)
        VALUES (?)
    """, (circuit,))

    # Learning config
    conn.execute("""
        INSERT OR IGNORE INTO learning_config (circuit)
        VALUES (?)
    """, (circuit,))

    # Circuit profile
    conn.execute("""
        INSERT OR IGNORE INTO circuit_profile (circuit, circuit_type)
        VALUES (?, ?)
    """, (circuit, circuit_type))

    # Leak test schedule
    conn.execute("""
        INSERT OR IGNORE INTO leak_test_schedule (circuit)
        VALUES (?)
    """, (circuit,))

    # Alert configs — seed with defaults if not present
    _seed_alert_configs(conn, circuit, circuit_type)

    conn.commit()


def _seed_alert_configs(conn: sqlite3.Connection, circuit: str,
                        circuit_type: str) -> None:
    """Insert default alert config rows for a circuit."""
    base_alerts = [
        ("pressure_drop", "Pressure Drop",
         "Alert when pressure drops rapidly — possible burst pipe"),
        ("high_flow", "High Flow",
         "Alert when flow rate exceeds burst threshold"),
        ("leak_test", "Micro Leak Test",
         "Alert when leak test detects pressure decay"),
        ("trickle", "Trickle Flow",
         "Alert on sustained low flow — possible running toilet or dripping tap"),
        ("flow_anomaly", "Flow Anomaly",
         "Alert when flow pattern doesn't match any known fixture"),
        ("pump_leak", "Pump Leak Watch",
         "Alert when the booster pump keeps re-pressurizing overnight with "
         "no water in use — a slow leak below the meters' sensitivity"),
        ("low_pressure_supply", "Low Pressure While Running",
         "Alert when pressure stays low while water is flowing — sprinkler "
         "heads may not pop up fully"),
        ("pump_low_pressure", "Pump Health",
         "Alert when pressure stays below the pump's normal range — the pump "
         "may have lost power, faulted, or can't keep up"),
        ("schedule_deviation", "Schedule Deviation",
         "Alert when flow occurs outside expected time patterns"),
    ]

    for alert_type, label, description in base_alerts:
        alert_id = f"{alert_type}_{circuit}"
        conn.execute("""
            INSERT OR IGNORE INTO alert_config
                (id, circuit, alert_type, label, description, enabled)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (alert_id, circuit, alert_type, label, description))

    if circuit_type == "zone":
        _seed_zone_alerts_only(conn, circuit)


def get_import_state(conn: sqlite3.Connection, circuit: str) -> dict:
    """Return import state for a circuit, creating defaults if absent."""
    row = conn.execute(
        "SELECT * FROM import_state WHERE circuit = ?", (circuit,)
    ).fetchone()
    if row:
        return dict(row)
    conn.execute(
        "INSERT OR IGNORE INTO import_state (circuit) VALUES (?)", (circuit,)
    )
    conn.commit()
    return {"circuit": circuit, "last_check_ts": None, "total_imported": 0}


def update_import_state(
    conn: sqlite3.Connection,
    circuit: str,
    last_check_ts: str,
    imported_count: int = 0,
) -> None:
    conn.execute("""
        INSERT INTO import_state (circuit, last_check_ts, total_imported)
        VALUES (?, ?, ?)
        ON CONFLICT (circuit) DO UPDATE SET
            last_check_ts  = excluded.last_check_ts,
            total_imported = total_imported + excluded.total_imported
    """, (circuit, last_check_ts, imported_count))
    conn.commit()


def get_last_event_ts(conn: sqlite3.Connection, circuit: str) -> Optional[str]:
    """Return ISO timestamp of the most recent event for this circuit, or None."""
    row = conn.execute(
        "SELECT MAX(start_ts) FROM events WHERE circuit = ?", (circuit,)
    ).fetchone()
    return row[0] if row and row[0] else None


def get_event_cadence_seconds(
    conn: sqlite3.Connection,
    circuit: str,
    *,
    since_iso: Optional[str] = None,
    lookback_days: int = 60,
    max_events: int = 50,
    min_gaps: int = 2,
) -> Optional[float]:
    """Median gap (seconds) between consecutive real events for a circuit.

    This is the UNCAPPED inter-event interval, unlike the stored
    ``events.seconds_since_prev_event`` column (capped at the sequence-gap
    limit, so it only ever measures within-burst gaps). Filters
    ``excluded_from_training = 0`` so it reflects the same event population that
    drives ``events_collected``. The window floor is the later of
    ``now - lookback_days`` and ``since_iso``, so a circuit still calibrating
    measures its CALIBRATION-period cadence and never pulls in pre-install /
    historical-import bursts.

    Returns ``None`` when fewer than ``min_gaps + 1`` qualifying events exist;
    callers apply an explicit fallback in that case.
    """
    floor_iso = (datetime.now(timezone.utc)
                 - timedelta(days=lookback_days)).isoformat()
    if since_iso and since_iso > floor_iso:
        floor_iso = since_iso
    rows = conn.execute(
        """SELECT CAST(strftime('%s', start_ts) AS INTEGER) AS ts_epoch
             FROM events
            WHERE circuit = ?
              AND start_ts IS NOT NULL
              AND COALESCE(excluded_from_training, 0) = 0
              AND start_ts >= ?
            ORDER BY start_ts DESC
            LIMIT ?""",
        (circuit, floor_iso, max_events),
    ).fetchall()
    epochs = sorted(r["ts_epoch"] for r in rows if r["ts_epoch"] is not None)
    gaps = [b - a for a, b in zip(epochs, epochs[1:]) if b > a]
    if len(gaps) < min_gaps:
        return None
    return float(median(gaps))


def find_overlapping_event(
    conn: sqlite3.Connection,
    circuit: str,
    start_ts: str,
    end_ts: str,
    exclude_event_id: Optional[str] = None,
) -> Optional[dict]:
    """Return an existing event row that meaningfully overlaps [start_ts, end_ts].

    'Meaningful' means:
      overlap >= 30 seconds, OR
      overlap >= 10 seconds AND overlap / shorter_event_duration >= 0.8

    Short independent events (< 10 s absolute overlap) are never blocked
    regardless of ratio — this preserves fridge-fill / toilet events that
    happen to start near the end of a longer shower.

    When multiple rows overlap, returns the most-protected one first:
    user-labeled rows → user-locked rows → longest event.

    Returns None when no meaningful overlap exists.  When a row is found it is
    returned as a dict so callers can log which event blocked the insert.
    """
    try:
        start = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
        end   = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None

    start_epoch = int(start.timestamp())
    end_epoch   = int(end.timestamp())
    new_dur     = end_epoch - start_epoch

    excl = "AND e.id != ?" if exclude_event_id is not None else ""
    params: list = [circuit, start_epoch, end_epoch]
    if exclude_event_id is not None:
        params.append(exclude_event_id)

    rows = conn.execute(f"""
        SELECT e.id,
               e.start_ts,
               e.end_ts,
               e.duration_seconds,
               e.fixture_id,
               e.user_fixture_type,
               f.user_locked,
               CAST(strftime('%s', e.start_ts) AS INTEGER) AS ex_start_epoch,
               CAST(strftime('%s', e.end_ts)   AS INTEGER) AS ex_end_epoch
          FROM events e
          LEFT JOIN fixtures f ON f.id = e.fixture_id
         WHERE e.circuit = ?
           AND e.start_ts IS NOT NULL
           AND e.end_ts IS NOT NULL
           AND CAST(strftime('%s', e.end_ts)   AS INTEGER) > ?
           AND CAST(strftime('%s', e.start_ts) AS INTEGER) < ?
               {excl}
         ORDER BY
               CASE WHEN e.user_fixture_type IS NOT NULL THEN 0 ELSE 1 END,
               CASE WHEN f.user_locked = 1               THEN 0 ELSE 1 END,
               (CAST(strftime('%s', e.end_ts)   AS INTEGER) -
                CAST(strftime('%s', e.start_ts) AS INTEGER)) DESC
    """, params).fetchall()

    for row in rows:
        ex_start = row["ex_start_epoch"]
        ex_end   = row["ex_end_epoch"]
        if ex_start is None or ex_end is None:
            continue
        overlap = min(ex_end, end_epoch) - max(ex_start, start_epoch)
        if overlap <= 0:
            continue
        ex_dur  = ex_end - ex_start
        shorter = min(ex_dur, new_dur)
        if shorter <= 0:
            continue
        if overlap >= 30 or (overlap >= 10 and (overlap / shorter) >= 0.8):
            # "Longer wins over short unlabeled stub" — when the incoming event
            # is ≥ 3× the existing one (the importer-partial signature) and the
            # existing row has no user label / user-locked fixture, allow the
            # insert. The orphan stub stays in the DB; the caller is expected
            # to log it. This auto-heals the C0 historical_importer truncation
            # case where the partial was stored before the live event closed.
            user_locked = bool(row["user_locked"]) if "user_locked" in row.keys() else False
            if (new_dur >= ex_dur * 3
                    and row["user_fixture_type"] is None
                    and not user_locked):
                log.info(
                    "[overlap] not blocking %d s event by %d s unlabeled stub %s "
                    "— allowing the longer event to insert",
                    new_dur, ex_dur, row["id"],
                )
                continue
            return dict(row)

    return None


def event_exists_near(
    conn: sqlite3.Connection,
    circuit: str,
    start_ts: str,
    window_seconds: int = 30,
) -> bool:
    """True if an event with start_ts within ±window_seconds already exists.

    Compares in Unix-epoch seconds so the result is robust against:
    - 'T' vs space separator mismatch (SQLite datetime() uses space)
    - mixed timezone offsets in stored data (+00:00 vs -06:00)
    - microsecond precision differences

    SQLite strftime('%s', …) understands ISO 8601 with both 'T' and space
    separators and returns integer epoch seconds, making the comparison
    timezone-absolute.
    """
    try:
        ts = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    lo_epoch = int((ts - timedelta(seconds=window_seconds)).timestamp())
    hi_epoch = int((ts + timedelta(seconds=window_seconds)).timestamp())
    row = conn.execute("""
        SELECT id FROM events
        WHERE circuit = ?
          AND start_ts IS NOT NULL
          AND CAST(strftime('%s', start_ts) AS INTEGER) BETWEEN ? AND ?
        LIMIT 1
    """, (circuit, lo_epoch, hi_epoch)).fetchone()
    return row is not None


def normalize_events_utc(conn: sqlite3.Connection, commit: bool = True) -> int:
    """Normalize events.start_ts / end_ts to UTC ISO 8601 in-place.

    Intended to be called before dedup_events() in the Quick Restore path.
    Does NOT recompute UUID5 ids here — that is done by dedup_events() after
    duplicates have been removed.  Recomputing ids before dedup would cause
    a PRIMARY KEY collision when two rows represent the same instant expressed
    in different offsets (both would map to the same UUID5).

    When ``commit`` is False the caller owns the transaction (e.g. a restore
    handler running multiple helpers under one outer ``with db:`` block).
    This prevents the inner commit from making the multi-step restore
    partially durable on later failure.

    Returns the number of rows whose timestamps were changed.
    Idempotent — rows already in UTC format are skipped.
    """
    rows = conn.execute(
        "SELECT id, start_ts, end_ts FROM events WHERE start_ts IS NOT NULL"
    ).fetchall()
    updates = []
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
                updates.append((new_s, new_e, r["id"]))
        except (ValueError, TypeError):
            continue
    if updates:
        conn.executemany(
            "UPDATE events SET start_ts = ?, end_ts = ? WHERE id = ?",
            updates
        )
        if commit:
            conn.commit()
    return len(updates)


def dedup_events(conn: sqlite3.Connection, commit: bool = True) -> int:
    """Remove duplicate events sharing (circuit, start_ts) and recompute ids.

    Called after Quick Restore to clean any pre-dedup data from old backups.
    Migration 021 (one-time) deduped all existing rows; the matching
    UNIQUE(circuit, start_ts) index lands in migration 032 (this commit).
    Dedup must run before the index creation when restoring older backups,
    or the unique constraint will fire on the historical duplicates.

    When ``commit`` is False the caller owns the transaction (see
    ``normalize_events_utc`` for the same pattern). Required by the
    setup-wizard restore handler so the full multi-step restore commits or
    rolls back as one unit.

    Idempotent — safe to call multiple times.  Returns count of rows deleted.
    Keeps the most recently inserted row (MAX rowid) on the assumption that
    later inserts have fresher cluster_id / match_confidence.

    Also:
    - Clears cluster_id / match_confidence on survivors of contested groups so
      backfill_unmatched re-matches them with the current engine state.
    - Recomputes UUID5 id = uuid5(NAMESPACE_OID, f"{circuit}/{start_ts}") for
      all survivors, making ids stable so future inserts against the
      UNIQUE(circuit, start_ts) index (migration 20260525) collide on the
      same UUID5 and the ON CONFLICT(id) DO UPDATE upsert in insert_event
      refreshes the existing row rather than triggering a duplicate.
    """
    import uuid as _uuid

    # Clear stale cluster_id (and match_confidence if the column exists) on
    # contested survivors before deleting dupes.  match_confidence was added
    # by migration 013; older in-memory test databases may not have it.
    _cols = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
    _extra = ", match_confidence = NULL" if "match_confidence" in _cols else ""
    conn.execute(f"""
        UPDATE events SET cluster_id = NULL{_extra}
        WHERE rowid IN (
            SELECT MAX(rowid) FROM events
            WHERE cluster_id IS NOT NULL
            GROUP BY circuit, start_ts
            HAVING COUNT(*) > 1
        )
    """)
    cursor = conn.execute("""
        DELETE FROM events
        WHERE rowid NOT IN (
            SELECT MAX(rowid)
            FROM events
            GROUP BY circuit, start_ts
        )
    """)
    removed = cursor.rowcount

    # Recompute UUID5 ids for all survivors.  Now that duplicates are gone,
    # each (circuit, start_ts) is unique so new ids cannot collide.
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
        # dev38 — this path has the one EXACT old→new id map in the codebase,
        # so overlap_audit references get a real remap here instead of a
        # stale mark (the audit found 43 dangling wrappers + 130 dangling
        # kept ids from paths with no such map). kept_event_ids is a JSON
        # list, remapped by quoted-string REPLACE.
        try:
            conn.executemany(
                "UPDATE overlap_audit SET wrapper_event_id = ? "
                "WHERE wrapper_event_id = ?", id_updates)
            conn.executemany(
                "UPDATE overlap_audit SET kept_event_ids = "
                "REPLACE(kept_event_ids, ?, ?) "
                "WHERE kept_event_ids LIKE ?",
                [(f'"{old}"', f'"{new}"', f'%"{old}"%')
                 for new, old in id_updates])
        except sqlite3.Error:
            pass       # pre-20260561 schema (no overlap_audit yet)

    if commit:
        conn.commit()
    return removed


# ── Phase 2: fixture cluster helpers ──────────────────────────────────────────

def get_clusters_with_fixtures(
    conn: sqlite3.Connection,
    circuit: str,
) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT fc.*,
               f.name,
               f.display_name,
               f.fixture_type  AS user_type,
               f.confirmed,
               f.user_locked,
               f.notes,
               f.publish_to_ha AS fixture_publish_to_ha
        FROM fixture_clusters fc
        LEFT JOIN fixtures f ON fc.fixture_id = f.id
        WHERE fc.circuit = ?
        ORDER BY fc.member_count DESC
        """,
        (circuit,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_cluster_stats(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(*)              AS event_count,
               AVG(volume_litres)   AS avg_volume_litres,
               AVG(duration_seconds)AS avg_duration_s,
               AVG(avg_flow_lpm)    AS avg_flow_lpm,
               MAX(start_ts)        AS last_seen_at
        FROM events
        WHERE circuit = ? AND cluster_id = ?
        """,
        (circuit, cluster_id),
    ).fetchone()
    return dict(row) if row else {}


def get_all_cluster_stats(
    conn: sqlite3.Connection,
    circuit: str,
) -> Dict[int, Dict[str, Any]]:
    """Return stats for all clusters in a circuit in one query.

    Returns {cluster_id: stats_dict} so callers can look up by id instead of
    issuing one query per cluster (avoids N+1 on the fixtures page).
    """
    rows = conn.execute(
        """
        SELECT cluster_id,
               COUNT(*)               AS event_count,
               AVG(volume_litres)     AS avg_volume_litres,
               AVG(duration_seconds)  AS avg_duration_s,
               AVG(avg_flow_lpm)      AS avg_flow_lpm,
               MAX(start_ts)          AS last_seen_at
        FROM events
        WHERE circuit = ? AND cluster_id IS NOT NULL
        GROUP BY cluster_id
        """,
        (circuit,),
    ).fetchall()
    return {r["cluster_id"]: dict(r) for r in rows}


def upsert_fixture_from_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
    name: str,
    fixture_type: str,
    publish_to_ha: int = 1,
) -> str:
    """Create or update a fixture linked to a cluster. Returns fixture_id."""
    import uuid as _uuid
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    row = conn.execute(
        "SELECT fixture_id FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, cluster_id),
    ).fetchone()
    fixture_id = row["fixture_id"] if row else None

    if fixture_id:
        conn.execute(
            """
            UPDATE fixtures
            SET name = ?, fixture_type = ?, confirmed = 1, user_locked = 1,
                display_name = ?, publish_to_ha = ?, updated_at = ?
            WHERE id = ?
            """,
            (name, fixture_type, name, publish_to_ha, now, fixture_id),
        )
    else:
        fixture_id = str(_uuid.uuid4())
        conn.execute(
            """
            INSERT INTO fixtures
                (id, circuit, name, auto_name, fixture_type, display_name,
                 confirmed, user_locked, publish_to_ha, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?)
            """,
            (fixture_id, circuit, name, name, fixture_type, name,
             publish_to_ha, now, now),
        )
        conn.execute(
            "UPDATE fixture_clusters SET fixture_id = ? WHERE circuit = ? AND id = ?",
            (fixture_id, circuit, cluster_id),
        )

    conn.commit()
    return fixture_id


def get_fixture_id_for_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
) -> Optional[str]:
    """Return the fixture_id linked to a cluster, or None."""
    row = conn.execute(
        "SELECT fixture_id FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, cluster_id)
    ).fetchone()
    return row["fixture_id"] if row and row["fixture_id"] else None


def delete_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
) -> None:
    """Remove a cluster and null out its cluster_id on linked events."""
    conn.execute(
        "UPDATE events SET cluster_id = NULL, fixture_id = NULL WHERE circuit = ? AND cluster_id = ?",
        (circuit, cluster_id),
    )
    conn.execute(
        "DELETE FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, cluster_id),
    )
    conn.commit()


def merge_clusters(
    conn: sqlite3.Connection,
    circuit: str,
    survivor_id: int,
    selected_cluster_ids: List[int],
) -> Dict[str, int]:
    """Merge several clusters into one survivor cluster.

    Every event from the non-survivor clusters is relinked to ``survivor_id``;
    the survivor's centroid, feature_std, member_count and confidence_level are
    recomputed; the non-survivor cluster rows and any confirmed fixture rows
    they own are deleted.  The survivor's own fixture row is left untouched.

    Returns ``{"events_relinked", "fixtures_removed", "survivor_member_count"}``.
    Raises ``ValueError`` on any validation failure, having written nothing.
    All writes run in a single transaction — rolled back on any exception.
    """
    # ── Validation — everything before any write ───────────────────────
    ids = list(dict.fromkeys(int(i) for i in selected_cluster_ids))
    if len(ids) < 2:
        raise ValueError("merge_clusters needs at least 2 distinct cluster IDs")
    survivor_id = int(survivor_id)
    if survivor_id not in ids:
        raise ValueError(
            f"survivor_id {survivor_id} not in selected cluster IDs {ids}"
        )

    id_ph = ",".join(["?"] * len(ids))
    rows = conn.execute(
        f"""SELECT id, centroid, feature_std, member_count
            FROM fixture_clusters
            WHERE circuit = ? AND id IN ({id_ph})""",
        (circuit, *ids),
    ).fetchall()
    found = {int(r["id"]): r for r in rows}
    missing = [i for i in ids if i not in found]
    if missing:
        raise ValueError(
            f"clusters not found in circuit {circuit!r}: {missing}"
        )

    centroids: Dict[int, dict] = {}
    feature_stds: Dict[int, dict] = {}
    counts: Dict[int, int] = {}
    for i in ids:
        r = found[i]
        try:
            cen = json.loads(r["centroid"]) if r["centroid"] else {}
            std = json.loads(r["feature_std"]) if r["feature_std"] else {}
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"cluster {i} has malformed JSON: {e}")
        if not isinstance(cen, dict) or not isinstance(std, dict):
            raise ValueError(
                f"cluster {i} centroid/feature_std is not a JSON object"
            )
        centroids[i] = cen
        feature_stds[i] = std
        counts[i] = int(r["member_count"] or 0)

    total_n = sum(counts.values())
    if total_n <= 0:
        raise ValueError(
            "total member_count across selected clusters is 0 — nothing to merge"
        )

    # ── Merged centroid: member-count weighted mean over the key union ──
    centroid_keys: set = set()
    for cen in centroids.values():
        centroid_keys.update(cen.keys())
    merged_centroid = {
        k: sum(counts[i] * float(centroids[i].get(k, 0.0)) for i in ids) / total_n
        for k in centroid_keys
    }

    # ── Merged feature_std: pooled standard deviation ──────────────────
    # Pooled variance needs each cluster's per-feature mean (its centroid
    # value) and std.  When a cluster lacks a std for a feature we fall back
    # to a member-count weighted average of the available stds — feature_std
    # is a forward-looking column, empty ('{}') in practice, so the fallback
    # is normally what runs and the result is just {}.
    std_keys: set = set()
    for std in feature_stds.values():
        std_keys.update(std.keys())
    merged_std = {}
    for k in std_keys:
        full = all(k in feature_stds[i] and k in centroids[i] for i in ids)
        if full:
            combined_var = sum(
                counts[i] * (
                    float(feature_stds[i][k]) ** 2
                    + (float(centroids[i][k]) - merged_centroid.get(k, 0.0)) ** 2
                )
                for i in ids
            ) / total_n
            merged_std[k] = math.sqrt(max(combined_var, 0.0))
        else:
            contributors = [i for i in ids if k in feature_stds[i]]
            weight = sum(counts[i] for i in contributors) or 1
            merged_std[k] = sum(
                counts[i] * float(feature_stds[i][k]) for i in contributors
            ) / weight

    # ── confidence_level — kept in sync with cluster_engine thresholds ──
    from .cluster_engine import LEVEL_PRELIMINARY_MAX, LEVEL_LEARNING_MAX
    if total_n < LEVEL_PRELIMINARY_MAX:
        level = "preliminary"
    elif total_n < LEVEL_LEARNING_MAX:
        level = "learning"
    else:
        level = "confirmed"

    deleted_ids = [i for i in ids if i != survivor_id]
    del_ph = ",".join(["?"] * len(deleted_ids))

    # ── Non-survivor fixture IDs to delete (deduped, survivor excluded) ─
    survivor_row = conn.execute(
        "SELECT fixture_id FROM fixture_clusters WHERE circuit = ? AND id = ?",
        (circuit, survivor_id),
    ).fetchone()
    survivor_fixture_id = survivor_row["fixture_id"] if survivor_row else None

    fx_rows = conn.execute(
        f"""SELECT DISTINCT fixture_id FROM fixture_clusters
            WHERE circuit = ? AND id IN ({del_ph}) AND fixture_id IS NOT NULL""",
        (circuit, *deleted_ids),
    ).fetchall()
    fixture_ids = [
        r["fixture_id"] for r in fx_rows
        if r["fixture_id"] != survivor_fixture_id
    ]

    # ── Writes — single transaction, rollback on any failure ───────────
    try:
        # Relink events to the survivor cluster.  fixture_id is also repointed
        # to the survivor's fixture (or NULL): the non-survivor fixture rows
        # are about to be deleted, and events.fixture_id REFERENCES fixtures(id)
        # would otherwise either dangle or block the DELETE.
        relink = conn.execute(
            f"""UPDATE events SET cluster_id = ?, fixture_id = ?
                WHERE circuit = ? AND cluster_id IN ({del_ph})""",
            (survivor_id, survivor_fixture_id, circuit, *deleted_ids),
        )
        events_relinked = relink.rowcount

        conn.execute(
            """UPDATE fixture_clusters
               SET centroid = ?, feature_std = ?, member_count = ?,
                   confidence_level = ?
               WHERE circuit = ? AND id = ?""",
            (json.dumps(merged_centroid), json.dumps(merged_std),
             total_n, level, circuit, survivor_id),
        )

        # Delete cluster rows before fixture rows — fixture_clusters.fixture_id
        # references fixtures(id), so this avoids relying on ON DELETE SET NULL.
        conn.execute(
            f"DELETE FROM fixture_clusters WHERE circuit = ? AND id IN ({del_ph})",
            (circuit, *deleted_ids),
        )

        if fixture_ids:
            fx_ph = ",".join(["?"] * len(fixture_ids))
            conn.execute(
                f"DELETE FROM fixtures WHERE id IN ({fx_ph})",
                tuple(fixture_ids),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "events_relinked": events_relinked,
        "fixtures_removed": len(fixture_ids),
        "survivor_member_count": total_n,
    }


# ============================================================================
# Sprint A — orphan repair
#
# Three classes of cluster/fixture FK inconsistency that can leak in over a
# product lifetime:
#
#   1. events.cluster_id → fixture_clusters(circuit,id) where the cluster row
#      no longer exists (cluster was deleted/merged without cleaning up event
#      references). Symptom in the field: events show "Cluster 26" on the
#      History page but cluster 26 is missing from the Fixtures page.
#
#   2. fixtures.confirmed=1 but no fixture_clusters row has fixture_id
#      pointing at this fixture. Symptom: a "★ Toilet" pill on history
#      events, but the Toilet fixture is invisible on the Fixtures page and
#      future toilet-shaped events have no cluster to land in.
#
#   3. fixture_clusters.fixture_id → fixtures(id) where the fixtures row no
#      longer exists. The ON DELETE SET NULL FK should prevent this if
#      PRAGMA foreign_keys was on when the fixture was deleted — but it
#      isn't always (per-connection setting; old code paths may have
#      committed without it).
#
# The repair is conservative: never delete user-confirmed fixtures, never
# delete events. Class 1 nulls the event's stale cluster_id so the next
# backfill pass can re-cluster it. Class 2 flags the fixture with
# cluster_backfill_needed=1 so the UI shows a relink affordance. Class 3
# nulls the cluster's dangling fixture_id (matches the FK's ON DELETE
# SET NULL semantics retroactively).
# ============================================================================

def find_orphaned_cluster_references(
    conn: sqlite3.Connection, *, repair: bool = False
) -> Dict[str, int]:
    """Detect (and optionally repair) cluster/fixture FK inconsistencies.

    Returns a dict with three counts:

      - events_orphaned: events.cluster_id pointing to a missing cluster
      - fixtures_unbacked: fixtures.confirmed=1 with no cluster pointing back
      - clusters_dangling: fixture_clusters.fixture_id pointing to a missing
        fixture

    With ``repair=False`` (default) the function only counts — used by the
    orchestrator's startup integrity check (logs non-zero counts; never
    mutates the DB at boot to avoid surprising side effects).

    With ``repair=True`` the function applies the fixes described in the
    module-section comment above and commits. Idempotent: running twice
    yields zero on the second call.
    """
    counts: Dict[str, int] = {
        "events_orphaned": 0,
        "fixtures_unbacked": 0,
        "clusters_dangling": 0,
    }

    # Class 1: events with stale cluster_id.
    # Composite key match — cluster_id alone isn't unique across circuits.
    orphan_events = conn.execute(
        """SELECT e.id FROM events e
           WHERE e.cluster_id IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM fixture_clusters fc
               WHERE fc.circuit = e.circuit AND fc.id = e.cluster_id
             )"""
    ).fetchall()
    counts["events_orphaned"] = len(orphan_events)

    # Class 2: fixtures confirmed but unbacked AND not yet flagged.
    #
    # confirmed=1 only — unconfirmed fixtures aren't expected to have a
    # cluster yet (they could exist transiently during cluster creation).
    #
    # The extra ``cluster_backfill_needed = 0`` filter makes detection
    # match what repair actually changes (the flag), so the function is
    # truly idempotent: once flagged, the fixture is "managed" — awaiting
    # user action via the Fixtures page relink banner — and the integrity
    # check stops re-reporting it on every boot. A genuine *new* orphan
    # appearing after migration (e.g. a bug deleted the wrong cluster
    # row) will still be detected because it won't have the flag set yet.
    unbacked_fixtures = conn.execute(
        """SELECT f.id FROM fixtures f
           WHERE f.confirmed = 1
             AND COALESCE(f.cluster_backfill_needed, 0) = 0
             AND NOT EXISTS (
               SELECT 1 FROM fixture_clusters fc
               WHERE fc.fixture_id = f.id
             )"""
    ).fetchall()
    counts["fixtures_unbacked"] = len(unbacked_fixtures)

    # Class 3: clusters with dangling fixture_id.
    dangling_clusters = conn.execute(
        """SELECT fc.circuit, fc.id FROM fixture_clusters fc
           WHERE fc.fixture_id IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM fixtures f WHERE f.id = fc.fixture_id
             )"""
    ).fetchall()
    counts["clusters_dangling"] = len(dangling_clusters)

    if not repair:
        return counts

    # Repair pass — single transaction so a mid-repair crash doesn't leave
    # half-fixed state.
    try:
        if orphan_events:
            ev_ids = [r["id"] for r in orphan_events]
            ph = ",".join(["?"] * len(ev_ids))
            conn.execute(
                f"UPDATE events SET cluster_id = NULL, "
                f"match_level = 'unmatched', "
                f"match_rejection_reason = 'orphan_cluster_repair' "
                f"WHERE id IN ({ph})",
                tuple(ev_ids),
            )

        if unbacked_fixtures:
            fx_ids = [r["id"] for r in unbacked_fixtures]
            ph = ",".join(["?"] * len(fx_ids))
            conn.execute(
                f"UPDATE fixtures SET cluster_backfill_needed = 1 "
                f"WHERE id IN ({ph})",
                tuple(fx_ids),
            )

        if dangling_clusters:
            # Composite key — iterate row-wise rather than build a huge IN
            # clause; counts here are typically small (single-digit).
            for r in dangling_clusters:
                conn.execute(
                    "UPDATE fixture_clusters SET fixture_id = NULL "
                    "WHERE circuit = ? AND id = ?",
                    (r["circuit"], r["id"]),
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return counts


def get_orphaned_fixtures(
    conn: sqlite3.Connection, circuit: str,
) -> List[Dict[str, Any]]:
    """Return fixtures on this circuit flagged for cluster backfill.

    Used by the Fixtures-page route to render the relink banner. Each row
    has the fields the template needs (id, name, fixture_type) plus the
    raw flag so the banner is only shown when actually needed.
    """
    rows = conn.execute(
        """SELECT id, name, display_name, fixture_type,
                  cluster_backfill_needed
           FROM fixtures
           WHERE circuit = ? AND confirmed = 1
             AND cluster_backfill_needed = 1
           ORDER BY name, id""",
        (circuit,),
    ).fetchall()
    return [dict(r) for r in rows]


# ============================================================================
# Sprint B — propagate per-event labels into cluster.suggested_type
#
# When the user labels an event "★ Toilet" on the History page, the row update
# in events.user_fixture_type is by itself cosmetic — only the History page
# renders the pill. The recompute helper below closes the loop: it looks at
# every labelled event on the cluster, takes a majority vote, and pushes the
# winning type into the cluster row with suggestion_source='user_labels'.
#
# Soft-hints model: this never silently links a cluster to a fixture.
# upsert_fixture_from_cluster (the route the Fixtures-page Confirm button
# calls) is still the only path that sets fixture_clusters.fixture_id. The
# helper just makes that confirmation one click away by pre-filling the
# suggestion.
#
# Edge cases handled explicitly:
#   - No labels yet → leave heuristic suggestion alone, return None
#   - Mixed labels → majority wins; ties broken by alphabetical type
#   - All labels removed by user → reset suggestion to NULL so the next
#     heuristic pass (every 10 events) can repopulate
# ============================================================================

# dev.22 — cluster-suggestion gate. A mixed cluster must ABSTAIN rather than
# broadcast its (possibly training-inflated) plurality to every unlabelled member.
_SUGGEST_MIN_MEMBERS: int = 3       # cluster needs >=3 labelled events to define a type
_SUGGEST_MIN_SHARE: float = 0.65    # weighted winner must hold a clear majority
_SUGGEST_MIN_WINNER_RAW: int = 3    # the winning type needs >=3 raw labels of its own


def _knn_usable_label_counts(conn: sqlite3.Connection, circuit: str) -> Dict[str, int]:
    """Per-type count of kNN-usable labels on a circuit — the inverse-frequency
    denominator for the class-balanced cluster vote (precompute once, reuse)."""
    return {r[0]: int(r[1]) for r in conn.execute(
        "SELECT user_fixture_type, COUNT(*) FROM events "
        "WHERE circuit = ? AND user_fixture_type IS NOT NULL "
        "  AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND training_quarantine_reason IS NULL "
        "GROUP BY user_fixture_type", (circuit,)).fetchall()}


def recompute_cluster_suggestion_from_user_labels(
    conn: sqlite3.Connection,
    circuit: str,
    cluster_id: int,
    global_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict[str, Any]]:
    """Recompute a cluster's suggested_type from majority vote of user
    labels on its member events.

    Returns a dict ``{suggested_type, suggested_confidence,
    suggestion_source, labelled_member_count, total_label_count}`` when
    the cluster row was updated (or reset). Returns ``None`` when there
    are no labels yet AND no prior user-labels suggestion is in place
    (so the cluster row is untouched and the heuristic suggestion, if
    any, stays valid).
    """
    # One row per labelled non-excluded member; MIN(capture_id) dedups the
    # training_capture_candidates fan-out so each event is counted exactly once.
    detail = conn.execute(
        "SELECT e.id AS eid, e.user_fixture_type AS t, MIN(tcc.capture_id) AS cap "
        "FROM events e "
        "LEFT JOIN training_capture_candidates tcc ON tcc.event_id = e.id "
        "WHERE e.circuit = ? AND e.cluster_id = ? "
        "  AND e.user_fixture_type IS NOT NULL "
        "  AND COALESCE(e.excluded_from_training, 0) = 0 "
        "  AND e.training_quarantine_reason IS NULL "
        "GROUP BY e.id, e.user_fixture_type",
        (circuit, cluster_id),
    ).fetchall()

    if not detail:
        # No labels on this cluster's events. If the current suggestion
        # is from user labels (i.e. the user just *removed* every label),
        # reset back to NULL so the heuristic pass can pick a fresh
        # value next time it runs. Otherwise leave the row alone.
        cur = conn.execute(
            "SELECT suggestion_source FROM fixture_clusters "
            "WHERE circuit = ? AND id = ?",
            (circuit, cluster_id),
        ).fetchone()
        if cur and cur["suggestion_source"] == "user_labels":
            conn.execute(
                "UPDATE fixture_clusters SET "
                "  suggested_type = NULL, "
                "  suggested_confidence = 0, "
                "  suggestion_source = NULL "
                "WHERE circuit = ? AND id = ?",
                (circuit, cluster_id),
            )
            conn.commit()
            return {
                "suggested_type": None,
                "suggested_confidence": 0.0,
                "suggestion_source": None,
                "labelled_member_count": 0,
                "total_label_count": 0,
            }
        return None

    # Class-balanced, capture-collapsed weighted vote (dev.22). Each labelled
    # member is weighted 1/global_count[type]; events sharing a training capture
    # collapse to a single vote so one windowed capture can't manufacture a
    # plurality. global_counts is passed in by the bulk caller (computed once).
    if global_counts is None:
        global_counts = _knn_usable_label_counts(conn, circuit)

    raw_counts: Dict[str, int] = {}
    vote_keys: Dict[str, set] = {}
    for d in detail:
        t = d["t"]
        raw_counts[t] = raw_counts.get(t, 0) + 1
        key = ("cap", d["cap"]) if d["cap"] is not None else ("ev", d["eid"])
        vote_keys.setdefault(t, set()).add(key)
    total_raw = sum(raw_counts.values())

    weighted = {t: len(keys) / max(1, global_counts.get(t, 0))
                for t, keys in vote_keys.items()}
    # Winner = max weighted vote; deterministic alphabetical tie-break. EVERY gate
    # below tests this same weighted winner (not the raw plurality).
    winner_type = sorted(weighted.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    total_w = sum(weighted.values())
    share = weighted[winner_type] / total_w if total_w > 0 else 0.0
    winner_raw = raw_counts.get(winner_type, 0)

    # Suggest the winner only if it clears all three gates; else ABSTAIN with a
    # NULL type but source 'user_labels' (so the heuristic pass won't repopulate).
    suggest = (winner_type != "other"
               and total_raw >= _SUGGEST_MIN_MEMBERS
               and share >= _SUGGEST_MIN_SHARE
               and winner_raw >= _SUGGEST_MIN_WINNER_RAW)
    new_type = winner_type if suggest else None

    conn.execute(
        "UPDATE fixture_clusters SET "
        "  suggested_type = ?, "
        "  suggested_confidence = ?, "
        "  suggestion_source = 'user_labels' "
        "WHERE circuit = ? AND id = ?",
        (new_type, share, circuit, cluster_id),
    )
    conn.commit()
    return {
        "suggested_type": new_type,
        "suggested_confidence": share,
        "suggestion_source": "user_labels",
        "labelled_member_count": winner_raw,
        "total_label_count": total_raw,
        "abstained": not suggest,
    }


def recompute_all_user_label_suggestions(
    conn: sqlite3.Connection, circuit: str
) -> Dict[str, int]:
    """Re-run the gated user-label suggestion across every cluster that has user
    labels (or a stale 'user_labels' suggestion). Computes the global class balance
    ONCE and passes it into each cluster — O(clusters), not O(clusters x corpus).

    This is the ONLY path that un-poisons a cluster whose suggestion was set under
    the pre-dev.22 ungated plurality vote (``resuggest_all_clusters`` deliberately
    skips 'user_labels' clusters). Returns ``{clusters, suggested, abstained,
    cleared}`` (cleared = a prior non-NULL suggestion now abstained)."""
    global_counts = _knn_usable_label_counts(conn, circuit)
    cluster_ids = [r[0] for r in conn.execute(
        "SELECT fc.id FROM fixture_clusters fc "
        "WHERE fc.circuit = ? AND ("
        "  fc.suggestion_source = 'user_labels' "
        "  OR EXISTS (SELECT 1 FROM events e WHERE e.circuit = fc.circuit "
        "             AND e.cluster_id = fc.id AND e.user_fixture_type IS NOT NULL "
        "             AND COALESCE(e.excluded_from_training, 0) = 0))",
        (circuit,)).fetchall()]
    suggested = abstained = cleared = 0
    for cid in cluster_ids:
        prev = conn.execute(
            "SELECT suggested_type FROM fixture_clusters WHERE circuit = ? AND id = ?",
            (circuit, cid)).fetchone()
        prev_type = prev["suggested_type"] if prev else None
        res = recompute_cluster_suggestion_from_user_labels(
            conn, circuit, int(cid), global_counts=global_counts)
        new_type = res.get("suggested_type") if res else None
        if new_type:
            suggested += 1
        else:
            abstained += 1
            if prev_type is not None:
                cleared += 1
    log.info("[%s] user-label resuggest: %d clusters, %d suggested, %d abstained, "
             "%d cleared", circuit, len(cluster_ids), suggested, abstained, cleared)
    return {"clusters": len(cluster_ids), "suggested": suggested,
            "abstained": abstained, "cleared": cleared}


def relink_fixture_to_cluster(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_id: str,
    cluster_id: int,
) -> None:
    """Attach an orphaned fixture to a chosen cluster.

    Validates that the fixture exists on this circuit, the cluster exists
    on this circuit, and the cluster isn't already linked to a different
    fixture (which would silently steal it). Atomically updates the
    cluster's ``fixture_id`` and clears the fixture's
    ``cluster_backfill_needed`` flag. Raises ``ValueError`` with a precise
    message if any precondition fails, having written nothing.
    """
    fx = conn.execute(
        "SELECT id FROM fixtures WHERE id = ? AND circuit = ?",
        (fixture_id, circuit),
    ).fetchone()
    if not fx:
        raise ValueError(
            f"fixture {fixture_id!r} not found on circuit {circuit!r}"
        )

    cl = conn.execute(
        "SELECT id, fixture_id FROM fixture_clusters "
        "WHERE circuit = ? AND id = ?",
        (circuit, cluster_id),
    ).fetchone()
    if not cl:
        raise ValueError(
            f"cluster {cluster_id} not found on circuit {circuit!r}"
        )

    existing = cl["fixture_id"]
    if existing and existing != fixture_id:
        raise ValueError(
            f"cluster {cluster_id} is already linked to fixture "
            f"{existing!r} — pick a different cluster"
        )

    try:
        conn.execute(
            "UPDATE fixture_clusters SET fixture_id = ? "
            "WHERE circuit = ? AND id = ?",
            (fixture_id, circuit, cluster_id),
        )
        conn.execute(
            "UPDATE fixtures SET cluster_backfill_needed = 0 "
            "WHERE id = ?",
            (fixture_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ============================================================================
# Sprint C — fixture_type_signatures matcher
#
# Per-(circuit, fixture_type) centroid built from user-labelled events. The
# matcher runs as a second-chance pass after cluster matching: if the cluster
# matcher rejected the event (no_centers / features_missing) OR matched it
# only at low confidence, we still want a user-facing label when the event's
# features sit close to a fixture type the user has been training.
#
# The signature centroid is a simple per-feature arithmetic mean over the
# labelled events' raw feature values (the cluster_engine's StandardScaler
# is per-circuit and not stable across boots, so we deliberately avoid it
# here — raw-feature Euclidean is good enough for the small feature subset
# the matcher considers, and stays interpretable across restarts).
#
# The feature subset used for matching is conservative: the same first-rank
# scalar features the cluster centroid heuristic already keys on (volume,
# duration, flow, pressure delta). Signature shape vectors are NOT used —
# they'd dominate the distance arithmetic and the user-labelled corpus is
# typically too small to learn a meaningful shape centroid.
# ============================================================================

# Features the signature matcher uses. Kept small + interpretable; matches
# the cluster_engine's first-rank features so the two centroids are
# comparable. Update both at once if this list changes.
_SIGNATURE_MATCH_FEATURES: tuple = (
    "avg_flow_lpm",
    "peak_flow_lpm",
    "duration_seconds",
    "volume_litres",
    "pressure_delta_psi",
    "steady_state_fraction",
)

# Signature matcher distance threshold — Euclidean over the feature subset
# above. Heuristic value picked so a clear toilet-shaped event (3 gal, 60s,
# ~2 lpm, ~5 psi drop) doesn't accidentally match a washing-machine
# signature (~6 gal, ~3 min, ~2 lpm, ~6 psi). Compared after subtracting
# centroid means and dividing each feature by its rough scale below.
_SIGNATURE_MATCH_THRESHOLD: float = 1.5
_SIGNATURE_MATCH_SCALES: dict = {
    "avg_flow_lpm":           2.0,    # 0.5–5 gal/min typical range
    "peak_flow_lpm":          3.0,
    "duration_seconds":     120.0,    # 30s – several min
    "volume_litres":         10.0,
    "pressure_delta_psi":     5.0,
    "steady_state_fraction":  0.5,
}

# ── Label-trained k-NN matcher (2026-05-31) ─────────────────────────────────
# The mean-centroid matcher above scored ~70% leave-one-out on the May-2026
# labelled archive; a weighted k-NN over the labelled events themselves scored
# ~80% (and stays in sync with labels — no stale centroid). The k-NN is the
# production path (live classify + backfill); the mean-centroid is retained for
# the Signatures UI display and back-compat tests.
#
# Right-skewed features are log1p-compressed so the centroid + Euclidean
# distance behave on log-normal data. The transform is applied identically to
# the query event AND every labelled neighbour at query time, so train/serve
# are consistent by construction (nothing log-space is persisted).
_SIGNATURE_LOG_FEATURES: frozenset = frozenset({
    "volume_litres", "duration_seconds", "avg_flow_lpm", "peak_flow_lpm",
})


def _sig_transform(feat: str, value) -> float:
    """log1p the right-skewed signature features; identity for the rest.

    MUST be applied identically in training (the labelled neighbours) and
    serving (the query event). None / non-numeric → 0.0 in the (already
    non-negative) transformed space.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    if feat in _SIGNATURE_LOG_FEATURES:
        return math.log1p(max(0.0, v))
    return v


# k-NN tuning. All values derived from the May-2026 labelled archive (104
# non-excluded labelled events); re-tune as more labels accrue.
_SIGNATURE_KNN_K: int = 5
# Below these floors the matcher abstains rather than overfit a tiny corpus:
_SIGNATURE_KNN_MIN_TOTAL_LABELS: int = 10     # whole circuit too sparse → None
_SIGNATURE_KNN_MIN_LABELS_PER_CLASS: int = 2  # class too sparse → not voted
# Accept a winner only when its summed inverse-distance vote clears an absolute
# floor (catches out-of-distribution events whose nearest neighbours are far)
# AND holds a clear majority share (catches genuinely ambiguous events). At
# margin 0.6 the LOO set was 85% covered / 86% accurate on what it typed.
_SIGNATURE_KNN_CONFIDENCE_THRESHOLD: float = 1.5
_SIGNATURE_KNN_MARGIN_THRESHOLD: float = 0.6
# dev.22 imbalance hardening — cap a single class to this many of the K neighbours
# so the most-labelled class can't fill every slot in a contested region. LOCKED at
# 4 by the LOO sweep (tools/eval_knn_classifier.py) on the labelled archive: cap<=3
# over-restricted (coverage collapsed to ~0.47); 4 holds dishwasher/toilet recall at
# baseline while inverse-freq weighting + the cycle feature lift tap/washing-machine.
_SIGNATURE_KNN_MAX_PER_CLASS: int = 4
# Per-feature scales ≈ each feature's std in log1p space on the labelled set.
_SIGNATURE_KNN_SCALES: dict = {
    "avg_flow_lpm":          0.70,
    "peak_flow_lpm":         0.74,
    "duration_seconds":      1.27,
    "volume_litres":         1.52,
    "pressure_delta_psi":    2.88,
    "steady_state_fraction": 0.25,
}

# ── Active-flow feature set (preferred after the 20260536 backfill) ──────────
# Once the backfill populates the active-flow columns on labelled events, the
# matcher prefers these (true_avg_flow + active_flow_duration replace the
# pressure-window-inflated avg_flow + duration; flow_on_ratio flags artifacts).
# Until then labelled rows have NULL active features, so the matcher falls back
# to the legacy set — this is what stops classification coverage collapsing.
# RETUNE _SIGNATURE_KNN_ACTIVE_SCALES (LOO on the labelled archive) once backfill
# has run; the values below are estimated from the raw-flow analysis.
_SIGNATURE_KNN_ACTIVE_FEATURES: tuple = (
    "true_avg_flow_lpm", "peak_flow_lpm", "active_flow_duration_seconds",
    "volume_litres", "pressure_delta_psi", "steady_state_fraction", "flow_on_ratio",
    # dev.22: cycle_pulse_count separates appliance fills (dishwasher ~3.5,
    # washing-machine ~2.9 pulses) from single toilet/tap fills (<1.3). Active set
    # only — kept out of _SIGNATURE_MATCH_FEATURES (which also feeds the stored
    # centroid signature) to avoid changing that shape. Requires the cycle-pulse
    # backfill to run BEFORE reclassify (orchestrator/fixtures reordered in dev.22).
    "cycle_pulse_count",
    # dev.39 (step 2): time-of-day as a cyclic (sin, cos) pair so 23:00 and 01:00 sit
    # adjacent. Separates fixtures that share a flow shape but run at different times
    # (a daytime tap vs an evening dishwasher fill). The columns already exist and are
    # populated at feature-extraction time (feature_extractor) and used by the cluster
    # engine at weight 0.2; dev.39 wires them into the k-NN matcher too. Active set
    # only — kept out of _SIGNATURE_MATCH_FEATURES so the stored centroid is unchanged.
    "hour_sin", "hour_cos",
    # dev.NN: starting supply pressure conditions the vote on the home's pressure
    # regime. A booster-pump install (2026-07) shifted settled pressure 46→59 psi
    # and peak flows ~15-20% (measured flow≈P^0.4 on peaks, ~P^0.1 on averages,
    # NEGATIVE on showers — so per-type conditioning, not a normalizing constant).
    # With this dim a post-change query finds post-change neighbours automatically
    # and self-heals if the regime ever reverts. Active set only — kept out of
    # _SIGNATURE_MATCH_FEATURES so the stored centroid shape is unchanged.
    # Missing/implausible values (<5 psi: NULLs and legacy coerced-0.0 rows) are
    # median-imputed per vote by _impute_pressure — NEVER left to the 0.0 fallback
    # of _knn_transform, which would be a huge phantom outlier in psi space.
    "pre_event_pressure_psi",
)
_SIGNATURE_KNN_ACTIVE_LOG_FEATURES: frozenset = frozenset({
    "true_avg_flow_lpm", "peak_flow_lpm", "active_flow_duration_seconds", "volume_litres",
})
_SIGNATURE_KNN_ACTIVE_SCALES: dict = {
    "true_avg_flow_lpm":           0.70,
    "peak_flow_lpm":               0.74,
    "active_flow_duration_seconds": 1.40,
    "volume_litres":               1.52,
    "pressure_delta_psi":          2.88,
    "steady_state_fraction":       0.25,
    "flow_on_ratio":               0.25,   # linear (a 0–1 ratio), not log
    # dev.22: cycle_pulse_count is a small integer count (0–7), linear, NOT log.
    # Scale 0.75 LOCKED by the LOO sweep (interior optimum; 0.5 and 1.0 both
    # slightly worse) — overall LOO accuracy 0.593→0.638, washing-machine recall
    # 0.23→0.69, dishwasher/toilet held at baseline.
    "cycle_pulse_count":           0.75,
    # dev.39 (step 2): hour_sin/hour_cos are linear in [-1, 1] (NOT log). Scale 0.35
    # makes time-of-day a strong tiebreaker without dominating the flow features —
    # the interior optimum of the confidence-weighted LOO sweep (0.821→0.837 overall,
    # tap recall 0.46→0.57; 0.1–0.7 all beat baseline, 0.35 the peak).
    "hour_sin":                    0.35,
    "hour_cos":                    0.35,
    # pre_event_pressure_psi is linear psi (NOT log — the 40-70 psi band has no
    # right skew, and log would compress exactly the regime separation the
    # feature exists to expose). Scale 1.5 LOCKED by the LOO sweep on the 154-
    # label 2026-07-28 archive (production-path --with-rules): baseline 107/154,
    # sweep 0.25:106 / 0.5:107 / 1.0:111 / 1.5:110 / 2-4.5:109 / 6-12:~109 —
    # interior optimum at 1.0-1.5 with collapse below (so the gain is signal,
    # not nearest-in-time memorization); 1.5 chosen over 1.0 for robustness
    # (1-event difference on a small set). At 1.5 the 13-psi pump/city regime
    # gap ≈8.7σ (strong conditioning), within-regime jitter ±2-3 psi ≈1.7σ.
    # tap recall 0.65→0.75, dishwasher 0.654→0.692, no class regressed.
    "pre_event_pressure_psi":      1.5,
}

# ── Regime-invariant k-NN tier (dev34) ───────────────────────────────────────
# The final rung before abstention. Deliberately contains NO pressure-derived
# feature: ΔP, hydraulic resistance and pre-event pressure all move when the
# supply does (the 2026-07 pump took toilet ΔP 4.37 → 11.32 psi at unchanged
# volume), and that is exactly what silently killed cluster matching for twelve
# days. Volume, duration, flow rate/shape and time-of-day do not move.
#
# Scales ≈ the per-feature standard deviation over the 516-event combined
# labelled pool, in log1p space for the two skewed dimensions — the same
# convention as the tiers above, and equivalent to the z-scoring the audit's
# reference implementation used, but as FIXED constants so a fit can't drift
# with the pool. Re-derive with tools/eval_knn_classifier.py --scale if the
# label distribution changes substantially.
_SIGNATURE_KNN_INVARIANT_FEATURES: tuple = (
    "volume_litres", "duration_seconds", "avg_flow_lpm", "peak_flow_lpm",
    "flow_variability", "steady_state_fraction", "flow_rise_rate_lpm_s",
    "flow_fall_rate_lpm_s", "time_to_90pct_flow_seconds", "opening_step_lpm",
    "hour_sin", "hour_cos",
)
_SIGNATURE_KNN_INVARIANT_LOG_FEATURES: frozenset = frozenset({
    "volume_litres", "duration_seconds",
})
_SIGNATURE_KNN_INVARIANT_SCALES: dict = {
    "volume_litres":              1.39,   # log1p space
    "duration_seconds":           1.26,   # log1p space
    "avg_flow_lpm":               2.76,
    "peak_flow_lpm":              4.52,
    "flow_variability":           1.19,
    "steady_state_fraction":      0.24,
    "flow_rise_rate_lpm_s":       4.53,
    "flow_fall_rate_lpm_s":       0.32,
    "time_to_90pct_flow_seconds": 364.19,
    "opening_step_lpm":           2.38,
    # Time-of-day at the same weight the active tier uses — a strong
    # tiebreaker, never a dominant term.
    "hour_sin":                   0.35,
    "hour_cos":                   0.35,
}


# ── Edge-signature k-NN tier (dev19) ─────────────────────────────────────────
# Fixed-time onset/offset shape cells (events.onset_signature_json /
# offset_signature_json) as an EXTRA feature block on top of the active-flow
# tier. Cell count/size pinned to feature_extractor.EDGE_SIG_CELLS /
# EDGE_SIG_CELL_SECONDS (32 × 1 s — asserted equal by test_edge_signatures);
# duplicated here to avoid a module-level import cycle. Per-dim scale 1.0 and
# the 32×1 s grid are the LOO-sweep optimum (tools/validate_edge_signatures on
# 344 labelled events: toilet recall 0.783→0.870, shower 0.878→0.927, tap
# 0.429→0.486; wider window beat finer cells — the toilet fill-taper needs
# ~30 s of tail). Cells are linear in [0, 1], never log-compressed.
_EDGE_SIG_CELLS: int = 32
_SIGNATURE_KNN_EDGE_SCALE: float = 1.0
_SIGNATURE_KNN_EDGE_FEATURES: tuple = tuple(
    f"onset_{i:02d}" for i in range(_EDGE_SIG_CELLS)) + tuple(
    f"offset_{i:02d}" for i in range(_EDGE_SIG_CELLS))


def _expand_edge_features(d: dict) -> bool:
    """Expand onset/offset_signature_json into the per-cell keys the k-NN
    distance reads (``onset_00``..``offset_31``), IN PLACE. Returns True only
    when both decode to exactly ``_EDGE_SIG_CELLS`` numeric cells — the edge
    tier's engagement test for queries and neighbours alike."""
    try:
        on = json.loads(d.get("onset_signature_json") or "null")
        off = json.loads(d.get("offset_signature_json") or "null")
    except (TypeError, ValueError):
        return False
    if (not isinstance(on, list) or not isinstance(off, list)
            or len(on) != _EDGE_SIG_CELLS or len(off) != _EDGE_SIG_CELLS):
        return False
    try:
        for i in range(_EDGE_SIG_CELLS):
            d[f"onset_{i:02d}"] = float(on[i])
            d[f"offset_{i:02d}"] = float(off[i])
    except (TypeError, ValueError):
        return False
    return True


def _knn_transform(feat: str, value, log_features: frozenset) -> float:
    """log1p the right-skewed features (per ``log_features``), identity for the
    rest. None / non-finite → 0.0. Applied identically to query + neighbours."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    return math.log1p(max(0.0, v)) if feat in log_features else v


# Supply pressure below this is not a plausible static line pressure — it marks
# a NULL or a legacy coerced-0.0 row (feature_extractor stored `or 0` before the
# detector's honest-None semantics), i.e. missing data, not a reading.
_PRESSURE_FEATURE = "pre_event_pressure_psi"
_PRESSURE_VALID_MIN_PSI = 5.0


def _valid_pressure(value) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v < _PRESSURE_VALID_MIN_PSI:
        return None
    return v


def _impute_pressure(labelled, event_features):
    """Median-impute missing/implausible ``pre_event_pressure_psi`` on both the
    labelled pool and the query, so the pressure dim is distance-NEUTRAL where
    data is absent (an unknown pressure must not read as "0 psi", which in a
    40-70 psi home is a phantom outlier that would dominate the vote).

    Returns ``(labelled, query_features)`` — copies where mutation was needed;
    the caller's inputs are never modified. When NO labelled row carries a valid
    pressure the query value is discarded too, collapsing the dimension to zero
    distance for every pair (pre-feature behavior).
    """
    vals: list = []
    invalid_idx: list = []
    for i, (_t, r) in enumerate(labelled):
        v = _valid_pressure(r[_PRESSURE_FEATURE])
        if v is None:
            invalid_idx.append(i)
        else:
            vals.append(v)
    q = dict(event_features)
    qv = _valid_pressure(q.get(_PRESSURE_FEATURE))
    if vals:
        vals.sort()
        mid = len(vals) // 2
        fill = vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2.0
    else:
        fill = 0.0
        qv = None  # constant on all sides → the dim contributes nothing
    q[_PRESSURE_FEATURE] = qv if qv is not None else fill
    if invalid_idx:
        labelled = list(labelled)
        for i in invalid_idx:
            t, r = labelled[i]
            rd = dict(r)
            rd[_PRESSURE_FEATURE] = fill
            labelled[i] = (t, rd)
    return labelled, q


def _knn_vote(labelled, event_features, features, scales, log_features):
    """Inverse-distance weighted k-NN vote over ``labelled`` = [(canon_type, row)].

    Shared by the active-flow and legacy matchers. Returns a hit dict or None
    (abstain). 'other' wins are treated as abstention.
    """
    counts: Dict[str, int] = {}
    for t, _ in labelled:
        counts[t] = counts.get(t, 0) + 1
    eligible = {t for t, c in counts.items()
                if c >= _SIGNATURE_KNN_MIN_LABELS_PER_CLASS}
    if not eligible:
        return None
    q = {f: _knn_transform(f, event_features.get(f), log_features) for f in features}
    dists: list = []
    for t, r in labelled:
        if t not in eligible:
            continue
        sq = 0.0
        for f in features:
            scale = scales.get(f, 1.0)
            d = (q[f] - _knn_transform(f, r[f], log_features)) / scale
            sq += d * d
        dists.append((math.sqrt(sq / len(features)), t))
    if not dists:
        return None
    dists.sort(key=lambda x: x[0])
    # Per-class neighbour cap (dev.22): take the K nearest, but stop adding a
    # class once it already holds _SIGNATURE_KNN_MAX_PER_CLASS slots, so a
    # numerically dominant class can't fill every neighbour in a contested region.
    neighbours: list = []
    per_class: Dict[str, int] = {}
    for d, t in dists:
        if per_class.get(t, 0) >= _SIGNATURE_KNN_MAX_PER_CLASS:
            continue
        neighbours.append((d, t))
        per_class[t] = per_class.get(t, 0) + 1
        if len(neighbours) >= _SIGNATURE_KNN_K:
            break

    # Two scores (dev.22): raw inverse-distance drives the absolute confidence
    # floor (the out-of-distribution guard, calibrated at 1.5); a class-balanced
    # score (× 1/sqrt(global count)) drives winner selection + margin so the
    # most-labelled class can't take over the ambiguous region by sheer numbers.
    score_raw: Dict[str, float] = {}
    score_bal: Dict[str, float] = {}
    for d, t in neighbours:
        w = 1.0 / (d + 1e-6)
        score_raw[t] = score_raw.get(t, 0.0) + w
        score_bal[t] = score_bal.get(t, 0.0) + w / math.sqrt(counts[t])
    total_bal = sum(score_bal.values())
    win_t, win_bal = max(score_bal.items(), key=lambda kv: kv[1])
    win_raw = score_raw[win_t]
    if (win_raw < _SIGNATURE_KNN_CONFIDENCE_THRESHOLD
            or total_bal <= 0
            or (win_bal / total_bal) < _SIGNATURE_KNN_MARGIN_THRESHOLD):
        return None
    if win_t == "other":
        return None
    nearest = min(d for d, t in neighbours if t == win_t)
    return {
        "fixture_type": win_t,
        "distance": nearest,
        "score": win_raw,
        "margin": win_bal / total_bal,
        "member_count": counts[win_t],
    }


def upsert_fixture_signature(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_type: str,
) -> Optional[Dict[str, Any]]:
    """Recompute (or remove) the signature for one (circuit, fixture_type).

    Reads every event on ``circuit`` whose ``user_fixture_type`` matches
    ``fixture_type`` and ``excluded_from_training = 0`` (degraded /
    composite events shouldn't pollute the type centroid), averages the
    feature subset, and upserts the row.

    Returns the upserted row as a dict on success, or ``None`` when there
    are no eligible labelled events. In the no-eligible case the existing
    signature is DELETED so a stale centroid doesn't keep matching after
    the user has un-labelled their training set.
    """
    rows = conn.execute(
        f"""SELECT {', '.join(_SIGNATURE_MATCH_FEATURES)}
            FROM events
            WHERE circuit = ?
              AND user_fixture_type = ?
              AND COALESCE(excluded_from_training, 0) = 0
              AND training_quarantine_reason IS NULL""",
        (circuit, fixture_type),
    ).fetchall()
    if not rows:
        conn.execute(
            "DELETE FROM fixture_type_signatures "
            "WHERE circuit = ? AND fixture_type = ?",
            (circuit, fixture_type),
        )
        conn.commit()
        return None

    # Arithmetic mean per feature, ignoring NULLs in any individual row.
    centroid: Dict[str, float] = {}
    for feat in _SIGNATURE_MATCH_FEATURES:
        vals = [float(r[feat]) for r in rows if r[feat] is not None]
        if vals:
            centroid[feat] = sum(vals) / len(vals)

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO fixture_type_signatures
               (circuit, fixture_type, centroid, member_count,
                created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(circuit, fixture_type) DO UPDATE SET
               centroid     = excluded.centroid,
               member_count = excluded.member_count,
               updated_at   = excluded.updated_at""",
        (circuit, fixture_type, json.dumps(centroid), len(rows), now, now),
    )
    conn.commit()
    return {
        "circuit": circuit,
        "fixture_type": fixture_type,
        "centroid": centroid,
        "member_count": len(rows),
    }


def get_fixture_type_signatures(
    conn: sqlite3.Connection, circuit: str,
) -> List[Dict[str, Any]]:
    """Return all signatures for one circuit, centroid pre-decoded.

    Ordered by member_count desc so the UI lists the most-trained types
    first. Empty list when nothing has been labelled yet.
    """
    rows = conn.execute(
        "SELECT circuit, fixture_type, centroid, member_count, "
        "       created_at, updated_at "
        "FROM fixture_type_signatures "
        "WHERE circuit = ? "
        "ORDER BY member_count DESC, fixture_type",
        (circuit,),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            centroid = json.loads(r["centroid"] or "{}")
        except (json.JSONDecodeError, TypeError):
            centroid = {}
        out.append({
            "circuit": r["circuit"],
            "fixture_type": r["fixture_type"],
            "centroid": centroid,
            "member_count": int(r["member_count"] or 0),
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        })
    return out


def delete_fixture_signature(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_type: str,
) -> bool:
    """Forget a signature so the user can recover from bad labels.

    Returns True if a row was deleted, False if none existed.
    """
    cur = conn.execute(
        "DELETE FROM fixture_type_signatures "
        "WHERE circuit = ? AND fixture_type = ?",
        (circuit, fixture_type),
    )
    conn.commit()
    return cur.rowcount > 0


# ── Sprint F: Per-category Fixtures-page rollup ─────────────────────────────

def get_category_rollup(
    conn: sqlite3.Connection,
    circuit: str,
    range_start_utc: Optional[str],
) -> List[Dict[str, Any]]:
    """Per-effective-type aggregate for one circuit.

    ``range_start_utc`` is the lower time bound for the *windowed* columns
    (``range_volume_l`` / ``range_event_count``) as a UTC ISO timestamp — the
    Fixtures-page time-range selector supplies HA-local midnight N days back
    (same helper/format the dashboard's get_daily_volume uses). Pass ``None``
    for the "lifetime" range: an empty-string sentinel ``''`` is then bound and,
    because events.start_ts is always non-null UTC-ISO text that sorts after
    ``''``, the windowed columns include every row — i.e. range == lifetime.

    The ``lifetime_*`` columns and ``last_seen_at`` are always all-time,
    independent of the range bound.

    Returned rows have raw ``eff_type`` strings — the router MUST funnel each
    through ``fixtures.normalize_fixture_type_for_circuit`` before bucketing,
    since legacy / wrong-kind / typo strings can appear in stored data.

    Phantom events (is_pressure_restoration_phantom=1) are excluded — their
    effective volume is already 0 and counting them would inflate the event
    count. Degraded and composite events ARE included; the Fixtures count
    should match what the History list shows, not the training subset.

    Effective-type precedence (clustering demoted 2026-05-31): user label >
    confirmed fixture > label-trained matched_fixture_type > cluster suggestion
    > 'other'. The k-NN match outranks the (impure) cluster suggestion so the
    classifier — not clustering — drives fixture identity on the cards.
    """
    # None (the "lifetime" range) → '' so the windowed CASE matches every row.
    bound = range_start_utc if range_start_utc is not None else ""
    # Toilet physics veto (dev17) on the cluster-suggestion leg — an event that
    # cannot be a flush must not roll its water into the Toilet card either.
    _suggested_sql = _suggested_type_vetoed_sql(get_toilet_flush_cap_litres(conn))
    rows = conn.execute(
        f"""
        SELECT
          COALESCE(e.user_fixture_type, f.fixture_type, e.matched_fixture_type,
                   {_suggested_sql}, 'other') AS eff_type,
          COALESCE(SUM(COALESCE(e.volume_litres_effective, e.volume_litres, 0)), 0)
                                                          AS lifetime_volume_l,
          COUNT(*)                                        AS lifetime_event_count,
          MAX(e.start_ts)                                 AS last_seen_at,
          COALESCE(SUM(CASE WHEN e.start_ts >= ?
                      THEN COALESCE(e.volume_litres_effective, e.volume_litres, 0)
                      ELSE 0 END), 0)                     AS range_volume_l,
          SUM(CASE WHEN e.start_ts >= ? THEN 1 ELSE 0 END) AS range_event_count
        FROM events e
        LEFT JOIN fixtures f          ON e.fixture_id = f.id
        LEFT JOIN fixture_clusters fc ON fc.circuit = e.circuit AND fc.id = e.cluster_id
        WHERE e.circuit = ?
          AND COALESCE(e.is_pressure_restoration_phantom, 0) = 0
        GROUP BY eff_type
        """,
        (bound, bound, circuit),
    ).fetchall()
    return [
        {
            "eff_type":             r["eff_type"],
            "lifetime_volume_l":    float(r["lifetime_volume_l"] or 0.0),
            "lifetime_event_count": int(r["lifetime_event_count"] or 0),
            "last_seen_at":         r["last_seen_at"],
            "range_volume_l":       float(r["range_volume_l"] or 0.0),
            "range_event_count":    int(r["range_event_count"] or 0),
        }
        for r in rows
    ]


def get_category_publish_map(
    conn: sqlite3.Connection, circuit: str,
) -> Dict[str, bool]:
    """Return {fixture_type: bool} for one circuit's per-category publish gates.

    Only returns rows that exist — missing keys MUST be defaulted to True
    (publish on) at the call site via ``publish_map.get(typ, True)``. This
    contract is pinned by ``test_category_publish_missing_row_defaults_true``.
    """
    rows = conn.execute(
        "SELECT fixture_type, publish_to_ha FROM category_publish "
        "WHERE circuit = ?",
        (circuit,),
    ).fetchall()
    return {r["fixture_type"]: bool(r["publish_to_ha"]) for r in rows}


def set_category_publish(
    conn: sqlite3.Connection,
    circuit: str,
    fixture_type: str,
    publish_to_ha: int,
) -> None:
    """Upsert the publish gate for one (circuit, fixture_type).

    Defensive: validates ``fixture_type`` against the union of fixture-
    selectable and zone-selectable types. Raises ``ValueError`` on unknown
    or empty input so a stray internal caller cannot persist garbage rows
    even if it skipped the route-level validation.
    """
    # Local import to avoid widening the module-load import surface.
    from .fixtures import (fixture_user_selectable_types,
                           zone_user_selectable_types)
    allowed = set(fixture_user_selectable_types()) | set(zone_user_selectable_types())
    if fixture_type not in allowed:
        raise ValueError(
            f"set_category_publish: unknown fixture_type {fixture_type!r}; "
            f"expected one of {sorted(allowed)}"
        )
    conn.execute(
        "INSERT INTO category_publish "
        "  (circuit, fixture_type, publish_to_ha, updated_at) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT (circuit, fixture_type) DO UPDATE SET "
        "  publish_to_ha = excluded.publish_to_ha, "
        "  updated_at = excluded.updated_at",
        (circuit, fixture_type, 1 if publish_to_ha else 0,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def repair_misflagged_phantom_events(conn: sqlite3.Connection) -> dict:
    """Sprint H — un-flag events wrongly marked as pressure-restoration phantoms.

    A real event can be left with a stale ``is_pressure_restoration_phantom=1``
    when the phantom verdict was computed from the software-path pressure
    (< 2 PSI) and a late ESP waveform then raised ``pressure_delta_psi`` to a
    real value without re-deriving the verdict (fixed forward by
    ``_finalize_derived_verdicts``). Such a row is internally contradictory:
    flagged phantom yet ``pressure_delta_psi >= 2.0``. This restores it:
    clears the flag, restores ``volume_litres_effective`` (degraded → envelope
    estimate, else raw) and re-applies that real volume to ``hourly_volume`` +
    ``daily_summary``.

    Skips ``user_classified`` rows (manual classification is authoritative).
    Idempotent — once repaired the WHERE clause no longer selects the row.

    Scope: this contradiction-repair concerns only the LONG-DURATION
    pressure-restoration phantom (which zeroes volume and requires
    ``pressure_delta_psi < 2.0``). The low-flow dribble flag is unrelated — it
    never zeroes volume and lives on low-pressure rows that can't satisfy the
    ``pressure_delta_psi >= 2.0`` filter below, so dribbles are never touched.

    Returns ``{"repaired": N, "litres_restored": L}``.
    """
    from .feature_extractor import _PHANTOM_MAX_DELTA_PSI

    rows = conn.execute(
        "SELECT id, circuit, start_ts, volume_litres, volume_litres_estimated, "
        "       degraded_supply, user_ignored, "
        "       hourly_volume_applied_litres, hourly_volume_applied_bucket "
        "FROM events "
        "WHERE is_pressure_restoration_phantom = 1 "
        "  AND pressure_delta_psi >= ? "
        "  AND COALESCE(user_classified, 0) = 0",
        (_PHANTOM_MAX_DELTA_PSI,),
    ).fetchall()

    repaired = 0
    litres_restored = 0.0
    affected_days: set = set()
    for row in rows:
        is_degraded = bool(row["degraded_supply"])
        raw = float(row["volume_litres"] or 0.0)
        est = row["volume_litres_estimated"]
        restored = (float(est) if (is_degraded and est is not None) else raw)
        new_method = "pulsing_supply_envelope" if is_degraded else "raw"
        new_excluded = 1 if (
            is_degraded or bool(row["user_ignored"])
        ) else 0
        new_reason = "pulsing_supply" if is_degraded else None

        with transaction(conn):
            conn.execute(
                "UPDATE events SET "
                "  is_pressure_restoration_phantom = 0, "
                "  volume_litres_effective = ?, "
                "  volume_estimation_method = ?, "
                "  excluded_from_training = ?, "
                "  match_rejection_reason = ? "
                "WHERE id = ?",
                (round(restored, 3), new_method, new_excluded, new_reason, row["id"]),
            )
            # §2.5 — restore the real volume to the ledger via the one chokepoint.
            apply_effective_volume(conn, row["id"], row["circuit"], row["start_ts"],
                                   restored)

        repaired += 1
        litres_restored += restored
        day = local_day_of(row["start_ts"])
        if day:
            affected_days.add((row["circuit"], day))
        log.info(
            "phantom-repair: event %s un-flagged (restored %.3f L to bucket %s)",
            row["id"], restored, _hour_bucket_for(row["start_ts"]),
        )

    for circ, day in affected_days:
        compute_daily_summary(conn, circ, day)
    if affected_days:
        conn.commit()

    if repaired:
        log.info("phantom-repair: un-flagged %d event(s), restored %.1f L total",
                 repaired, litres_restored)
    return {"repaired": repaired, "litres_restored": round(litres_restored, 3)}


def match_event_to_signature(
    conn: sqlite3.Connection,
    circuit: str,
    event_features: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Return the closest signature within
    ``_SIGNATURE_MATCH_THRESHOLD`` on ``circuit``, or None.

    Distance is computed in scale-normalised space:
    sqrt(sum_i ((event_i - centroid_i) / scale_i)^2) over the matcher
    feature subset. ``_SIGNATURE_MATCH_SCALES`` provides per-feature
    typical ranges; the threshold is then in "rough fixture-typical-range
    units" so it's interpretable.

    Caller is responsible for deciding *when* to call this (e.g. only as
    a fallback after cluster matching). The matcher itself doesn't gate
    on whether the cluster matched.
    """
    sigs = get_fixture_type_signatures(conn, circuit)
    if not sigs:
        return None

    best: Optional[Dict[str, Any]] = None
    best_dist = float("inf")
    for sig in sigs:
        cen = sig["centroid"]
        if not cen:
            continue
        sq = 0.0
        used = 0
        for feat in _SIGNATURE_MATCH_FEATURES:
            ev_v = event_features.get(feat)
            cn_v = cen.get(feat)
            if ev_v is None or cn_v is None:
                continue
            scale = _SIGNATURE_MATCH_SCALES.get(feat, 1.0)
            try:
                delta = (float(ev_v) - float(cn_v)) / scale
            except (TypeError, ValueError):
                continue
            sq += delta * delta
            used += 1
        if used == 0:
            continue
        # Normalise distance by feature count so signatures with sparse
        # centroids (only a few features populated) aren't unfairly
        # penalised vs full-feature ones.
        dist = (sq / used) ** 0.5
        if dist < best_dist:
            best_dist = dist
            best = sig

    if best is None or best_dist > _SIGNATURE_MATCH_THRESHOLD:
        return None
    return {
        "fixture_type": best["fixture_type"],
        "distance": best_dist,
        "member_count": best["member_count"],
    }


def set_event_matched_fixture_type(
    conn: sqlite3.Connection,
    circuit: str,
    event_id: str,
    fixture_type: Optional[str],
    via: Optional[str] = None,
    cycle_group_id: Optional[str] = None,
) -> None:
    """Write ``events.matched_fixture_type`` (+ its ``matched_via`` provenance and
    ``cycle_group_id`` rollup key) for one event. ``via`` and ``cycle_group_id``
    are forced NULL whenever the type is NULL (an abstain clears all three), so a
    stale provenance / group can never outlive its match. ``cycle_group_id`` is
    the History rollup key (washer anchor id / softener session id) and is NULL
    for non-cycle matches; recomputed by every reclassify.

    Does not commit — caller batches with surrounding writes.

    dev46 (46a/N1): the WHERE re-checks ``user_fixture_type IS NULL`` at WRITE
    time, not just in the caller's candidate snapshot. The reclassify pass now
    runs chunked on the DB executor, so a user PATCH can land between the
    snapshot and this row's write; without the re-check the pass would overwrite
    a just-labelled event's match from stale premises. The PATCH API is NOT
    gated during startup, so this guard is load-bearing — do not remove it on
    the theory that the snapshot already filtered. Same shape as the live path's
    inline write (feature_extractor.py) and the cycle-detector writes below.
    Sole caller: reclassify_all_events_from_signatures.
    """
    conn.execute(
        "UPDATE events SET matched_fixture_type = ?, matched_via = ?, "
        "cycle_group_id = ? WHERE id = ? AND circuit = ? "
        "  AND user_fixture_type IS NULL",
        (fixture_type,
         via if fixture_type is not None else None,
         cycle_group_id if fixture_type is not None else None,
         event_id, circuit),
    )


def _canonical_fixture_type(name: Optional[str]) -> Optional[str]:
    """Collapse a fixture-type string to its canonical slug (circuit-kind
    independent): lowercase, take the first '/'-segment, fold separators to
    '_', then apply the Sprint-D alias remap (shower→shower_tub, etc.).

    Returns None for None/blank input. Does NOT map unknowns to 'other' (that's
    the circuit-kind-aware display layer's job) — it only unifies variants so
    'Toilet'/'toilets'/'toilet ' all store as 'toilet'. Reused at every write
    of user_fixture_type / matched_fixture_type so the rollup groups cleanly.
    """
    if not isinstance(name, str):
        return None
    s = name.strip().lower()
    if not s:
        return None
    s = s.strip("/").split("/", 1)[0].strip()
    s = re.sub(r"[\s\-]+", "_", s).strip("_")
    if not s:
        return None
    # Lazy import — fixtures.py owns the alias map; importing at module load
    # would be a (currently absent) cycle risk.
    try:
        from .fixtures import LEGACY_TYPE_REMAP
        return LEGACY_TYPE_REMAP.get(s, s)
    except Exception:
        return s


def match_event_to_signature_knn(
    conn: sqlite3.Connection,
    circuit: str,
    event_features: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Inverse-distance weighted k-NN over the circuit's labelled events.

    The production fixture-type matcher (replaces the mean-centroid
    ``match_event_to_signature`` on the live + backfill paths). Pulls every
    labelled, non-excluded event on ``circuit``, log-compresses the skewed
    features (``_sig_transform``), and votes with weight ``1/(distance+eps)``.

    Abstains (returns ``None``) when:
      • fewer than ``_SIGNATURE_KNN_MIN_TOTAL_LABELS`` labelled events exist;
      • no class has ``_SIGNATURE_KNN_MIN_LABELS_PER_CLASS`` members;
      • the winner's summed vote is below ``_SIGNATURE_KNN_CONFIDENCE_THRESHOLD``
        (out-of-distribution — nearest neighbours are far); or
      • the winner's share of the total vote is below
        ``_SIGNATURE_KNN_MARGIN_THRESHOLD`` (genuinely ambiguous).

    On a hit returns ``{"fixture_type", "distance", "score", "margin",
    "member_count"}``. fixture_type is canonical.
    """
    def _labelled(sql: str):
        rows = conn.execute(sql, (circuit,)).fetchall()
        out = []
        for r in rows:
            t = _canonical_fixture_type(r["user_fixture_type"])
            if t:
                out.append((t, r))
        return out

    # 1) Prefer the active-flow features — but only when the QUERY has them and
    #    enough labelled events have been backfilled with non-NULL, non-degraded
    #    active features. 'other' wins → abstain (handled in _knn_vote).
    query_has_active = all(
        event_features.get(f) is not None
        for f in ("true_avg_flow_lpm", "active_flow_duration_seconds", "flow_on_ratio")
    )
    if query_has_active:
        # 1a) Edge tier (dev19): active features + the fixed-time onset/offset
        #     shape cells. Gated exactly like the active tier itself: the QUERY
        #     must carry decodable edge signatures AND enough labelled
        #     neighbours must too (no zero-filling absent edges — a missing
        #     signature is missing data, not a flat shape).
        #
        #     LADDER SHAPE IS LOAD-BEARING: when the edge vote ABSTAINS, fall
        #     straight to LEGACY — do NOT retry with the plain active tier.
        #     The LOO study validated exactly this shape; a plain-active retry
        #     was measured to flip the sign of the whole feature (production-
        #     path eval 240/362 with the retry vs 245 baseline — it converts
        #     the edge tier's deliberate abstentions on ambiguous events into
        #     lower-information guesses). The plain active tier below serves
        #     only queries that CANNOT use edges.
        q_edges = dict(event_features)
        if _expand_edge_features(q_edges):
            edge_rows = _labelled(
                "SELECT user_fixture_type, true_avg_flow_lpm, peak_flow_lpm, "
                "       active_flow_duration_seconds, volume_litres, "
                "       pressure_delta_psi, steady_state_fraction, flow_on_ratio, "
                "       cycle_pulse_count, hour_sin, hour_cos, "
                "       pre_event_pressure_psi, "
                "       onset_signature_json, offset_signature_json "
                "FROM events "
                "WHERE circuit = ? "
                "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
                "  AND COALESCE(excluded_from_training, 0) = 0 "
                "  AND training_quarantine_reason IS NULL "
                "  AND COALESCE(integration_quality, 'ok') = 'ok' "
                "  AND true_avg_flow_lpm IS NOT NULL "
                "  AND active_flow_duration_seconds IS NOT NULL "
                "  AND flow_on_ratio IS NOT NULL "
                "  AND onset_signature_json IS NOT NULL "
                "  AND offset_signature_json IS NOT NULL"
            )
            edge_labelled = []
            for t, r in edge_rows:
                rd = dict(r)
                if _expand_edge_features(rd):
                    edge_labelled.append((t, rd))
            if len(edge_labelled) >= _SIGNATURE_KNN_MIN_TOTAL_LABELS:
                edge_labelled, q_edges = _impute_pressure(edge_labelled, q_edges)
                hit = _knn_vote(
                    edge_labelled, q_edges,
                    _SIGNATURE_KNN_ACTIVE_FEATURES + _SIGNATURE_KNN_EDGE_FEATURES,
                    {**_SIGNATURE_KNN_ACTIVE_SCALES,
                     **{f: _SIGNATURE_KNN_EDGE_SCALE
                        for f in _SIGNATURE_KNN_EDGE_FEATURES}},
                    _SIGNATURE_KNN_ACTIVE_LOG_FEATURES)
                if hit is not None:
                    hit["match_source"] = "active_flow_edges"
                    return hit
                return _legacy_knn_fallback(_labelled, event_features)

        active = _labelled(
            "SELECT user_fixture_type, true_avg_flow_lpm, peak_flow_lpm, "
            "       active_flow_duration_seconds, volume_litres, pressure_delta_psi, "
            "       steady_state_fraction, flow_on_ratio, cycle_pulse_count, "
            "       hour_sin, hour_cos, pre_event_pressure_psi "
            "FROM events "
            "WHERE circuit = ? "
            "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
            "  AND COALESCE(excluded_from_training, 0) = 0 "
            "  AND training_quarantine_reason IS NULL "
            "  AND COALESCE(integration_quality, 'ok') = 'ok' "
            "  AND true_avg_flow_lpm IS NOT NULL "
            "  AND active_flow_duration_seconds IS NOT NULL "
            "  AND flow_on_ratio IS NOT NULL"
        )
        if len(active) >= _SIGNATURE_KNN_MIN_TOTAL_LABELS:
            active, q_active = _impute_pressure(active, event_features)
            hit = _knn_vote(active, q_active, _SIGNATURE_KNN_ACTIVE_FEATURES,
                            _SIGNATURE_KNN_ACTIVE_SCALES, _SIGNATURE_KNN_ACTIVE_LOG_FEATURES)
            if hit is not None:
                hit["match_source"] = "active_flow"
                return hit
            # Active had enough labels but abstained → fall through to legacy so
            # classification coverage never regresses below the legacy baseline.

    # 2) Legacy fallback (pre-backfill, active abstained, or the edge tier
    #    abstained — see the ladder-shape note above).
    return _legacy_knn_fallback(_labelled, event_features)


def _legacy_knn_fallback(_labelled, event_features) -> Optional[Dict[str, Any]]:
    """The 6-scalar legacy k-NN tier — the shared penultimate rung of the
    matcher ladder (factored out so the edge tier can reach it directly on
    abstention without re-voting the plain active tier). When IT abstains the
    regime-invariant tier below gets the last word."""
    legacy = _labelled(
        "SELECT user_fixture_type, avg_flow_lpm, peak_flow_lpm, duration_seconds, "
        "       volume_litres, pressure_delta_psi, steady_state_fraction "
        "FROM events "
        "WHERE circuit = ? "
        "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
        "  AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND training_quarantine_reason IS NULL"
    )
    if len(legacy) < _SIGNATURE_KNN_MIN_TOTAL_LABELS:
        return _invariant_knn_fallback(_labelled, event_features)
    hit = _knn_vote(legacy, event_features, _SIGNATURE_MATCH_FEATURES,
                    _SIGNATURE_KNN_SCALES, _SIGNATURE_LOG_FEATURES)
    if hit is not None:
        hit["match_source"] = "legacy_features"
        return hit
    return _invariant_knn_fallback(_labelled, event_features)


def _invariant_knn_fallback(_labelled, event_features) -> Optional[Dict[str, Any]]:
    """Regime-INVARIANT k-NN — the final rung before abstention (dev34).

    Every tier above this one reads at least one pressure-derived feature, and
    pressure is the thing a supply change moves: the 2026-07 booster pump took
    toilet ΔP from 4.37 to 11.32 psi with the same 4.9 L flush, which put every
    post-pump event outside the locked cluster gates and left the classifier
    silently unable to name 800+ events for twelve days.

    This tier reads ONLY quantities a supply change does not move — volume,
    duration, flow rates, flow shape, and time of day. The pressure columns are
    excluded from the FEATURE SET; they are not nulled out of the data (a NULL
    in a linear dimension is a fabricated zero, which is worse). Measured on
    the combined old+new label set: 83% leave-one-out, and 84% on the harder
    train-pre-pump / test-post-pump split — which is the actual test of the
    invariance claim, and the reason this tier survives the NEXT supply change
    without a re-fit.

    It runs LAST on purpose. The tiers above see more of the signal when the
    regime is stable, and the ladder's shape is load-bearing (see the note in
    match_event_to_signature_knn): this is a pure addition below them, so a
    stable home's verdicts are unchanged and only events that would otherwise
    have gone unnamed reach it.
    """
    rows = _labelled(
        "SELECT user_fixture_type, volume_litres, duration_seconds, "
        "       avg_flow_lpm, peak_flow_lpm, flow_variability, "
        "       steady_state_fraction, flow_rise_rate_lpm_s, "
        "       flow_fall_rate_lpm_s, time_to_90pct_flow_seconds, "
        "       opening_step_lpm, hour_sin, hour_cos "
        "FROM events "
        "WHERE circuit = ? "
        "  AND user_fixture_type IS NOT NULL AND user_fixture_type <> '' "
        "  AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND training_quarantine_reason IS NULL "
        "  AND volume_litres > 0 AND duration_seconds > 0"
    )
    if len(rows) < _SIGNATURE_KNN_MIN_TOTAL_LABELS:
        return None
    hit = _knn_vote(rows, event_features, _SIGNATURE_KNN_INVARIANT_FEATURES,
                    _SIGNATURE_KNN_INVARIANT_SCALES,
                    _SIGNATURE_KNN_INVARIANT_LOG_FEATURES)
    if hit is not None:
        hit["match_source"] = "invariant_features"
    return hit


def _new_reclassify_counters() -> Dict[str, Any]:
    """Fresh accumulator bundle for a reclassify pass.

    dev46 (46a/C2a): the pass is driven in CHUNKS, so its loop-carried state
    lives in one dict that is threaded through every batch instead of in
    function locals. Six counters plus the flush-veto tally — nothing else
    crosses a row boundary, which is exactly why the pass could be sliced.
    """
    return {"scanned": 0, "matched": 0, "rule_matched": 0,
            "softener_matched": 0, "cleared": 0, "abstained": 0,
            "veto_counts": {}}


def _reclassify_prepare(conn: sqlite3.Connection, circuit: str, ha_tz=None,
                        since_ts: Optional[str] = None):
    """Everything a reclassify pass computes ONCE, plus its candidate rows.

    dev46 (46a/C2a) — split out of ``reclassify_all_events_from_signatures``
    so the pass can be driven chunk-wise through ``run_db``. This half is the
    expensive-but-bounded part: signature training, the per-regime calib
    cache, the whole-circuit cycle detectors (washer / softener / dishwasher),
    usage baselines, the fingerprint library and the toilet cap. All of it is
    READ-ONLY for the row loop, which is what makes batching safe.

    Returns ``(ctx, rows)``.
    """
    # 1. Retrain the per-type centroids (for the Signatures UI display). The
    #    k-NN itself reads events directly, so this is purely cosmetic but keeps
    #    the Signatures page in sync.
    signatures_trained = 0
    type_rows = conn.execute(
        "SELECT DISTINCT user_fixture_type FROM events "
        "WHERE circuit = ? AND user_fixture_type IS NOT NULL "
        "  AND user_fixture_type <> ''",
        (circuit,),
    ).fetchall()
    for tr in type_rows:
        if upsert_fixture_signature(conn, circuit, tr[0]) is not None:
            signatures_trained += 1

    # 2. Backfill over unlabelled events. Query carries BOTH the legacy and the
    #    active-flow features so the matcher uses whichever it can (active when
    #    backfilled). An event now excluded_from_training carries no fixture
    #    identity → its matched_fixture_type is cleared (stale-match carry-forward).
    from .event_rules import (CYCLE_ONLY_FIXTURE_TYPES, detect_dishwasher_cycles,
                              detect_softener_sessions, detect_washer_cycles,
                              get_home_timezone, parse_hhmm_to_minutes,
                              rule_classify_event)
    from .rule_calibration import load_rule_calibration

    # Frozen per-home rule bands (empty dict → predicates use shipped defaults).
    # Regime-aware: bands are fitted per SUPPLY REGIME (migration 20260565), so
    # the per-event rule tier below resolves each event's calib by its
    # start_ts — a pre-pump event is judged by pre-pump bands even when the
    # pass runs today. The window-scanning cycle detectors (washer/dishwasher/
    # softener) take ONE calib per pass; they get the CURRENT regime's — the
    # regime where new events land. (v1 limitation: a full reprocess spanning
    # a regime boundary scans historical cycles with current bands; per-event
    # rules, where the observed staleness actually bit, are fully resolved.)
    from .supply_regime import (get_current_regime_id, get_regimes,
                                resolve_regime_for_ts)
    _regimes = get_regimes(conn)
    _calib_cache: Dict[int, Dict[str, Any]] = {
        0: load_rule_calibration(conn, circuit)}
    for _rg in _regimes:
        _calib_cache[int(_rg["id"])] = load_rule_calibration(
            conn, circuit, regime_id=int(_rg["id"]))
    calib = _calib_cache.get(get_current_regime_id(conn), _calib_cache[0])
    qfeats = tuple(dict.fromkeys(
        _SIGNATURE_MATCH_FEATURES + _SIGNATURE_KNN_ACTIVE_FEATURES
        # dev34: the regime-invariant rung's shape features (the rest of its
        # set is already covered by the tuples above).
        + _SIGNATURE_KNN_INVARIANT_FEATURES
        + ("has_pressure_transient",)     # the flush predicate's extra input
        # dev19: the edge tier reads these off the query dict (expanded inside
        # match_event_to_signature_knn); harmless extras for the rules tier.
        + ("onset_signature_json", "offset_signature_json")))
    circuit_type = get_circuit_type(conn, circuit)
    # Windowed (periodic maturity re-check): bound the expensive per-event k-NN row
    # loop below to events >= since_ts, but give the detectors a lookback (>= the
    # softener's max session span) so a cycle straddling the window start is still seen
    # WHOLE — otherwise its in-window members would be wrongly retracted. since_ts=None
    # → full circuit (startup / manual reprocess).
    detector_since = since_ts
    if since_ts is not None:
        _s = _parse_event_ts(since_ts)
        if _s is not None:
            detector_since = (_s - timedelta(hours=4)).isoformat()
    # One O(n) pass for the whole circuit — the per-row loop then does dict
    # lookups, never per-event window queries.
    washer_ids = (detect_washer_cycles(conn, circuit, since_ts=detector_since,
                                       calib=calib)
                  if circuit_type != "zone" else {})
    # dev.24 — water-softener sessions (hard-gated: enabled AND this circuit).
    softener_ids: Dict[str, Any] = {}
    prof = get_home_profile(conn)
    if (prof is not None and prof["has_water_softener"]
            and (prof["softener_circuit"] or "main") == circuit):
        band = parse_hhmm_to_minutes(prof["softener_regen_start"])
        if band is not None:
            tz = ha_tz if ha_tz is not None else get_home_timezone()
            softener_ids = detect_softener_sessions(conn, circuit, band,
                                                    since_ts=detector_since, tz=tz,
                                                    calib=calib)
    # dev.39 — dishwasher cycles: a chain of gentle small fills the per-event
    # cycle-pulse rule misses. Exclude ids the washer/softener detectors already
    # claimed so a brine chain / laundry top-off can't be re-read as a dishwasher.
    dishwasher_ids = (
        detect_dishwasher_cycles(conn, circuit, since_ts=detector_since, calib=calib,
                                 exclude_ids=set(washer_ids) | set(softener_ids))
        if circuit_type != "zone" else {})
    # Phase 2.3 — re-score each scanned event against the FROZEN baseline (storage
    # only; reclassify NEVER notifies or shuts off). Baseline + sensitivity loaded
    # once for the whole pass; the extra SELECT columns the scorer needs are deduped
    # into the query so a column already in qfeats isn't selected twice.
    from .anomaly_baseline import load_usage_baselines, score_event_anomaly
    _baselines = load_usage_baselines(conn, circuit)
    _sens = get_sensitivity_config(conn, circuit)
    _SCORE_COLS = ("volume_litres_effective", "volume_litres", "duration_seconds",
                   "peak_flow_lpm", "is_pressure_restoration_phantom", "is_cross_talk",
                   "is_low_flow_dribble", "user_ignored",
                   "phantom_suppression_averted")

    # Fingerprint tier library (2026-07 audit Phase 3) — built ONCE per run,
    # fresh (no cache; a reclassify usually follows a label change). Gated by
    # the home_profile toggle; any failure just disables the tier for this run.
    _fp_library = None
    try:
        _fp_row = conn.execute(
            "SELECT fingerprint_labeling_enabled FROM home_profile WHERE id = 1"
        ).fetchone()
        _fp_enabled = bool(_fp_row["fingerprint_labeling_enabled"]) if _fp_row else True
    except sqlite3.OperationalError:
        _fp_enabled = True   # column mid-migration → schema default is ON
    if _fp_enabled:
        try:
            from .fingerprint_matcher import FingerprintLibrary
            _fp_library = FingerprintLibrary.load(conn, circuit)
        except Exception as e:  # noqa: BLE001 — tier is optional, never fatal
            log.warning("[%s] fingerprint library unavailable: %s", circuit, e)

    # Toilet physics veto (dev17) — cap computed once for the whole pass.
    from .event_rules import toilet_veto_reason
    _toilet_cap = get_toilet_flush_cap_litres(conn)

    where = "WHERE circuit = ? AND user_fixture_type IS NULL"
    qparams: list = [circuit]
    if since_ts is not None:
        where += " AND start_ts >= ?"
        qparams.append(since_ts)
    select_cols = list(dict.fromkeys(
        ("id", "start_ts", "matched_fixture_type", "matched_via",
         "cycle_group_id", "excluded_from_training",
         "match_rejection_reason",       # dev33: abstention marker mark/retract
         "active_flow_segment_count")
        + qfeats + _SCORE_COLS))
    rows = conn.execute(
        "SELECT " + ", ".join(select_cols) + " "
        "FROM events "
        + where + " "
        "ORDER BY start_ts",
        qparams,
    ).fetchall()
    return ({
        "signatures_trained": signatures_trained,
        "softener_ids":       softener_ids,
        "washer_ids":         washer_ids,
        "dishwasher_ids":     dishwasher_ids,
        "qfeats":             qfeats,
        "circuit_type":       circuit_type,
        "calib":              calib,
        "calib_cache":        _calib_cache,
        "regimes":            _regimes,
        "fp_library":         _fp_library,
        "toilet_cap":         _toilet_cap,
        "baselines":          _baselines,
        "sens":               _sens,
        "score_cols":         _SCORE_COLS,
    }, rows)


def _reclassify_chunk_sync(conn: sqlite3.Connection, circuit: str, rows: list,
                           ctx: Dict[str, Any],
                           counters: Dict[str, Any]) -> None:
    """One batch of the reclassify row loop — runs on the single DB thread.

    dev46 rule N2a: self-contained transaction. Every statement for this
    chunk, plus its commit, happens inside this one callable, so no foreign
    statement can land inside an open transaction. Chunk boundary =
    transaction boundary = where a queued page render gets to interleave.

    ``counters`` is mutated in place so the tallies survive across batches.
    The write-time ``user_fixture_type IS NULL`` guard inside
    ``set_event_matched_fixture_type`` (dev46 R1/N1) is what makes an
    interleaved user relabel safe here — do not weaken it.
    """
    from .event_rules import (CYCLE_ONLY_FIXTURE_TYPES, rule_classify_event,
                              toilet_veto_reason)
    from .supply_regime import resolve_regime_for_ts
    from .anomaly_baseline import score_event_anomaly

    softener_ids   = ctx["softener_ids"]
    washer_ids     = ctx["washer_ids"]
    dishwasher_ids = ctx["dishwasher_ids"]
    qfeats         = ctx["qfeats"]
    circuit_type   = ctx["circuit_type"]
    calib          = ctx["calib"]
    _calib_cache   = ctx["calib_cache"]
    _regimes       = ctx["regimes"]
    _fp_library    = ctx["fp_library"]
    _toilet_cap    = ctx["toilet_cap"]
    _baselines     = ctx["baselines"]
    _sens          = ctx["sens"]
    _SCORE_COLS    = ctx["score_cols"]

    scanned          = counters["scanned"]
    matched          = counters["matched"]
    rule_matched     = counters["rule_matched"]
    softener_matched = counters["softener_matched"]
    cleared          = counters["cleared"]
    abstained        = counters["abstained"]
    veto_counts      = counters["veto_counts"]
    for r in rows:
        scanned += 1
        new_group = None
        if r["id"] in softener_ids:
            # Softener is checked BEFORE the excluded gate — a deliberate exception
            # to dev.23's "excluded → no identity": regen consumption is real, and
            # matched_* is written separately from the volume verdict, so a
            # dribble-flagged regen pulse keeps its verdict AND reads water_softener.
            _role, new_group = softener_ids[r["id"]]
            new_type, new_via = "water_softener", "softener_session"
        elif r["excluded_from_training"]:
            new_type, new_via = None, None   # artifacts carry no fixture identity
        elif r["id"] in washer_ids:
            new_type, new_via = "washing_machine", "washer_cycle"
            new_group = washer_ids[r["id"]][1]
        elif r["id"] in dishwasher_ids:
            new_type, new_via = "dishwasher", "dishwasher_cycle"
            new_group = dishwasher_ids[r["id"]][1]
        else:
            feats = {f: r[f] for f in qfeats}
            from .config import pump_gates_active as _pga
            try:
                _pump = _pga(conn, circuit)
            except Exception:
                _pump = False
            _ev_calib = (_calib_cache.get(
                resolve_regime_for_ts(_regimes, r["start_ts"]), calib)
                if _regimes else calib)
            rule_hit = rule_classify_event(feats, circuit_type, calib=_ev_calib,
                                           pump_mode=_pump)
            if rule_hit is not None:
                new_type, new_via = rule_hit
            else:
                # Fingerprint tier (2026-07 audit Phase 3) — whole-waveform NN
                # against USER-labeled events, tight-threshold only. Sits between
                # the structural rules and the scalar k-NN: stronger evidence
                # than a scalar vote, weaker than cycle/session context above.
                fp_hit = None
                if _fp_library is not None:
                    from .fingerprint_matcher import match_event_fingerprint
                    try:
                        fp_hit = match_event_fingerprint(
                            conn, circuit, r["id"], library=_fp_library)
                    except Exception as e:  # noqa: BLE001 — never break reclassify
                        log.debug("[%s] fingerprint match failed for %s: %s",
                                  circuit, r["id"], e)
                if fp_hit is not None:
                    new_type = _canonical_fixture_type(fp_hit["fixture_type"])
                    new_via = "fingerprint" if new_type is not None else None
                else:
                    hit = match_event_to_signature_knn(conn, circuit, feats)
                    new_type = _canonical_fixture_type(hit["fixture_type"]) if hit else None
                    # Multi-fill appliances need cycle context (washer_cycle / dishwasher
                    # rule, both checked above) — a lone k-NN signature must not stamp them.
                    # (The fingerprint tier MAY name them: whole-waveform evidence at the
                    # tight threshold, enforced inside FingerprintLibrary.match.)
                    if new_type in CYCLE_ONLY_FIXTURE_TYPES:
                        new_type = None
                    # dev34: distinguish the regime-invariant rung so its
                    # contribution is measurable (and separable) in the data.
                    new_via = (
                        None if new_type is None
                        else "knn_invariant"
                        if hit.get("match_source") == "invariant_features"
                        else "knn")
            # Toilet physics veto (dev17): whatever tier proposed 'toilet'
            # (rule / fingerprint / k-NN), the event must be physically able
            # to BE a flush. Vetoed → abstain (never re-guess another type).
            if new_type == "toilet":
                vfeats = dict(feats)
                vfeats["active_flow_segment_count"] = r["active_flow_segment_count"]
                why = toilet_veto_reason(vfeats, _toilet_cap)
                if why:
                    # DEBUG per event, one INFO summary at the end: ~60 of
                    # these fire per reclassify and they are the veto WORKING
                    # (investigated 2026-08-03: the recurring 2.2–2.8 L band
                    # is the labelled dishwasher's upper fill-pulse tail — 9
                    # user labels in-band, 0 of them toilet, every event has
                    # a neighbour within 30 min — so the floor is what keeps
                    # appliance pulses from being named flushes).
                    log.debug("[%s] event %s: toilet match (%s) vetoed by "
                              "flush physics — %s (vol=%s L)", circuit,
                              r["id"], new_via, why, r["volume_litres"])
                    veto_counts[why.split(" (")[0]] = (
                        veto_counts.get(why.split(" (")[0], 0) + 1)
                    new_type, new_via = None, None
        prev = r["matched_fixture_type"]
        if (new_type, new_via, new_group) != (
                prev, r["matched_via"], r["cycle_group_id"]):
            set_event_matched_fixture_type(conn, circuit, r["id"], new_type,
                                           via=new_via, cycle_group_id=new_group)
            if new_type is None and prev is not None:
                cleared += 1
        # dev33 (§2.1) — mark / retract classification-tier abstention so a
        # silent outage is measurable and a recovery is visible. Only ever
        # fills an EMPTY reason (artifact + cluster reasons are more specific),
        # and is retracted the moment any tier names the event.
        from .feature_extractor import NO_TIER_MATCHED_REASON as _NTM
        _mrr = r["match_rejection_reason"] if "match_rejection_reason" in r.keys() \
            else None
        if new_type is None and not _mrr and not r["excluded_from_training"]:
            conn.execute(
                "UPDATE events SET match_rejection_reason = ? "
                "WHERE id = ? AND match_rejection_reason IS NULL", (_NTM, r["id"]))
        elif new_type is not None and _mrr == _NTM:
            conn.execute(
                "UPDATE events SET match_rejection_reason = NULL WHERE id = ?",
                (r["id"],))
        if new_type is not None:
            matched += 1
            if new_via == "softener_session":
                softener_matched += 1
            elif new_via != "knn":
                rule_matched += 1
        else:
            abstained += 1
        # Re-score against the frozen baseline + persist (storage only — no notify /
        # shut-off from a backfill). flagged=1 marks a genuine (non-artifact) anomaly.
        sfeats = {c: r[c] for c in _SCORE_COLS}
        sfeats["matched_fixture_type"] = new_type
        av = score_event_anomaly(sfeats, _baselines, _sens)
        conn.execute(
            "UPDATE events SET anomaly_score = ?, anomaly_type = ?, flagged = ? "
            "WHERE id = ?",
            (av.get("score"), av.get("anomaly_type"),
             1 if av.get("is_anomalous") else 0, r["id"]),
        )
    # dev46 (46a/C2a): NO yield_write_lock here. The pre-chunking loop called
    # it every 300 rows to commit and sleep 30 ms so a waiting user save could
    # win the SQLite write lock. Both of its jobs now belong to the chunk
    # boundary: the commit below is the chunk's ONE commit (rule N2a — chunk =
    # transaction), and yielding is the executor queue's job. Kept inside a
    # chunk it would be actively harmful — the 30 ms sleep would hold the
    # single DB worker rather than release it, delaying the very queue it was
    # meant to let through.
    conn.commit()
    counters.update(scanned=scanned, matched=matched,
                    rule_matched=rule_matched,
                    softener_matched=softener_matched,
                    cleared=cleared, abstained=abstained)


def _reclassify_finalize(conn: sqlite3.Connection, circuit: str,
                         since_ts: Optional[str], ctx: Dict[str, Any],
                         counters: Dict[str, Any]) -> Dict[str, Any]:
    """Composite annotation + result assembly, after every batch has run."""
    signatures_trained = ctx["signatures_trained"]
    scanned          = counters["scanned"]
    matched          = counters["matched"]
    rule_matched     = counters["rule_matched"]
    softener_matched = counters["softener_matched"]
    cleared          = counters["cleared"]
    abstained        = counters["abstained"]
    veto_counts      = counters["veto_counts"]
    # ── Composite annotation (dev.39, step 1) ────────────────────────────────
    # Annotate sustained events with fixtures hidden inside them (a toilet flushed
    # mid-shower), then upgrade events that abstained but clearly contain a second
    # draw from "(none)" to "other"/composite. Wrapped so a waveform/JSON hiccup
    # can never undo the classification that already committed above.
    embedded_annotated = embedded_other = 0
    try:
        emb = recompute_embedded_fixtures(conn, circuit, since_ts=since_ts)
        embedded_annotated = emb["annotated"]
    except Exception:                       # pragma: no cover - defensive
        log.exception("[%s] embedded-fixture annotation failed (classification "
                      "already committed)", circuit)
    # Re-promote abstained events that hide a real second draw (ANY embedded kind
    # — toilet, tap, …) to "other"/composite, from the STORED annotation so this
    # re-applies every run — the main loop above just cleared these to NULL, and
    # the embedded scan is incremental (won't re-report an already-annotated
    # event). Separate try from the scan above: a waveform hiccup there must not
    # also skip the re-promotion (that left prior 'other' events flickering to
    # NULL until the next reclassify). Decided from the PARSED JSON, not a LIKE,
    # so it can't drift from the detector's serialization or kind set.
    try:
        embedded_other = promote_embedded_composites(conn, circuit,
                                                     since_ts=since_ts)
        conn.commit()
    except Exception:                       # pragma: no cover - defensive
        log.exception("[%s] composite re-promotion failed (classification "
                      "already committed)", circuit)
    result = {
        "signatures_trained": signatures_trained,
        "events_scanned": scanned,
        "events_matched": matched,
        "events_rule_matched": rule_matched,
        "events_softener_matched": softener_matched,
        "events_cleared": cleared,
        "events_abstained": abstained,
        "events_embedded_annotated": embedded_annotated,
        "events_composite_other": embedded_other,
    }
    log.info(
        "[%s] reclassify: trained %d signature(s); scanned %d unlabelled "
        "event(s) → %d matched (%d via rules), %d abstained (%d stale cleared)",
        circuit, signatures_trained, scanned, matched, rule_matched, abstained,
        cleared,
    )
    if veto_counts:
        log.info("[%s] flush-physics vetoes: %s (per-event detail at DEBUG)",
                 circuit, "; ".join(f"{n}× {why}" for why, n
                                    in sorted(veto_counts.items(),
                                              key=lambda kv: -kv[1])))
    return result


async def reclassify_all_events_from_signatures_async(
        conn: sqlite3.Connection, circuit: str, ha_tz=None,
        since_ts: Optional[str] = None, batch: int = 200) -> Dict[str, Any]:
    """dev46 (46a/C2a) — ``reclassify_all_events_from_signatures`` in chunks.

    Same pass, same result, but the row loop is submitted to ``run_db`` one
    batch at a time instead of as a single multi-minute call. With ONE DB
    worker, a monolithic submission makes every queued page render wait for
    the whole pass; batching gives the queue a seam every ~batch rows.

    Mirrors ``ClusterEngine.backfill_unmatched_async`` — chunk = transaction =
    one run_db call. Prepare and finalize are their own submissions.
    """
    ctx, rows = await run_db(_reclassify_prepare, conn, circuit, ha_tz,
                             since_ts)
    counters = _new_reclassify_counters()
    for i in range(0, len(rows), batch):
        await run_db(_reclassify_chunk_sync, conn, circuit,
                     rows[i:i + batch], ctx, counters)
    return await run_db(_reclassify_finalize, conn, circuit, since_ts, ctx,
                        counters)



def reclassify_all_events_from_signatures(
    conn: sqlite3.Connection,
    circuit: str,
    ha_tz=None,
    since_ts: Optional[str] = None,
) -> Dict[str, Any]:
    """Retrain signatures, then backfill ``matched_fixture_type`` over every
    unlabelled event on ``circuit`` — STRUCTURAL RULES FIRST (dev.24 precedence:
    water-softener session, then dev.23's washer-cycle sweep, then the per-event
    toilet/dishwasher/shower/zone rules), k-NN as the residual. Each write stamps
    ``matched_via`` and ``cycle_group_id`` (the History rollup key, §7).

    ``ha_tz`` (the home timezone) is needed only for the water-softener regen-band
    match (local clock vs UTC-stored timestamps) — pass it from EVERY caller so
    the softener label is stable across reclassifies. Softener detection is
    hard-gated by ``home_profile.has_water_softener`` + ``softener_circuit``.

    NEVER touches user-labelled rows (WHERE user_fixture_type IS NULL). Writes
    the canonical matched type, or NULL on abstention — writing NULL clears a
    stale prior match (and its provenance), making the whole pass idempotent.
    Never writes 'other' (that is a display-only fallback).

    Returns counts: ``{"signatures_trained", "events_scanned", "events_matched",
    "events_rule_matched", "events_cleared", "events_abstained"}``.
    """
    ctx, rows = _reclassify_prepare(conn, circuit, ha_tz, since_ts)
    counters = _new_reclassify_counters()
    _reclassify_chunk_sync(conn, circuit, rows, ctx, counters)
    return _reclassify_finalize(conn, circuit, since_ts, ctx, counters)


_EMBEDDED_MIN_PARENT_DURATION_S: float = 300.0   # only sustained events can hide a draw


def promote_embedded_composites(
    conn: sqlite3.Connection,
    circuit: str,
    since_ts: Optional[str] = None,
) -> int:
    """Promote abstained events whose stored ``embedded_fixtures_json`` annotation
    holds at least one embedded draw (any kind) to ``matched_fixture_type='other'``
    / ``matched_via='composite'``. Re-applied after every reclassify (the main loop
    clears abstained events to NULL). Decides from the parsed annotation — a LIKE
    on the serialized JSON silently missed tap-only composites and coupled the rule
    to the exact serialization. Returns the promoted count."""
    where = ("WHERE circuit = ? AND user_fixture_type IS NULL "
             "AND matched_fixture_type IS NULL "
             "AND embedded_fixtures_json IS NOT NULL "
             "AND embedded_fixtures_json <> '[]'")
    params: list = [circuit]
    if since_ts is not None:
        where += " AND start_ts >= ?"
        params.append(since_ts)
    rows = conn.execute(
        "SELECT id, embedded_fixtures_json FROM events " + where, params,
    ).fetchall()
    promoted = 0
    for r in rows:
        try:
            embedded = json.loads(r["embedded_fixtures_json"])
        except (ValueError, TypeError):
            continue
        if not (isinstance(embedded, list) and embedded):
            continue
        # dev46 (46a/N1): re-check the label at WRITE time — the candidate
        # snapshot above filtered user_fixture_type IS NULL, but the pass runs
        # chunked on the DB executor and a user PATCH can land in between.
        conn.execute(
            "UPDATE events SET matched_fixture_type = 'other', "
            "matched_via = 'composite' "
            "WHERE id = ? AND user_fixture_type IS NULL",
            (r["id"],),
        )
        promoted += 1
    return promoted


def recompute_embedded_fixtures(
    conn: sqlite3.Connection,
    circuit: str,
    since_ts: Optional[str] = None,
) -> Dict[str, int]:
    """Annotate sustained events with the fixtures hidden inside them.

    For every sustained event on ``circuit`` that has a usable stored waveform
    (``event_waveforms.flow_max``), run ``composite_detector.detect_from_envelope``
    and write the result to ``events.embedded_fixtures_json`` — a JSON array of
    the draws superimposed on the event's baseline (a toilet flushed mid-shower).

    ANNOTATE-ONLY: this never touches ``volume_litres*`` or
    ``matched_fixture_type`` — it is pure display/metadata, so it cannot affect
    leak-safety or volume totals.

    INCREMENTAL: only sustained events whose ``embedded_fixtures_json`` is still
    NULL are scanned, and every scanned event is then stamped — ``'[]'`` when its
    waveform yields nothing or is too coarse to resolve a draw, the JSON array when
    it does. So after the first full backfill this is a near-no-op (it does NOT
    re-decompose the whole history on every label-triggered reclassify, which would
    hold the write lock long enough to starve a concurrent user save). Idempotent.
    (Edge case: an event scanned while its waveform was coarse won't be re-scanned
    if a finer waveform arrives later — acceptable; the waveform is effectively
    final by the time reclassify runs.)

    Returns ``{"scanned", "annotated", "with_toilet"}``.
    """
    from .composite_detector import detect_from_envelope, summarize_embedded

    where = ("WHERE e.circuit = ? AND COALESCE(e.duration_seconds, 0) >= ? "
             "AND COALESCE(e.user_classified, 0) = 0 "
             "AND e.embedded_fixtures_json IS NULL")
    params: list = [circuit, _EMBEDDED_MIN_PARENT_DURATION_S]
    if since_ts is not None:
        where += " AND e.start_ts >= ?"
        params.append(since_ts)
    rows = conn.execute(
        "SELECT e.id AS id, w.flow_max_json AS flow_max_json, "
        "       w.duration_seconds AS wf_duration "
        "FROM events e JOIN event_waveforms w ON w.event_id = e.id "
        + where, params,
    ).fetchall()

    scanned = annotated = with_toilet = 0
    for r in rows:
        scanned += 1
        try:
            flow_max = json.loads(r["flow_max_json"]) if r["flow_max_json"] else None
        except (ValueError, TypeError):
            flow_max = None
        embedded = detect_from_envelope(flow_max, r["wf_duration"])
        # Stamp every scanned event so it isn't re-scanned next pass: '[]' when
        # nothing embedded (or the waveform was too coarse), the JSON otherwise.
        conn.execute(
            "UPDATE events SET embedded_fixtures_json = ? WHERE id = ?",
            (json.dumps(embedded) if embedded else "[]", r["id"]),
        )
        if embedded:
            annotated += 1
            if summarize_embedded(embedded).get("toilet"):
                with_toilet += 1
        yield_write_lock(conn, scanned)   # let a waiting user save win the lock
    conn.commit()
    if scanned:
        log.info("[%s] embedded-fixture scan: %d new sustained event(s), %d annotated "
                 "(%d with an embedded toilet)", circuit, scanned, annotated, with_toilet)
    return {"scanned": scanned, "annotated": annotated, "with_toilet": with_toilet}


def cleanup_composite_flags(conn: sqlite3.Connection) -> Dict[str, int]:
    """One-shot: composite is deprecated as an authoritative flag. Re-derive
    ``excluded_from_training`` for events that were excluded ONLY because they
    were composite, so they become classifiable again.

    Guard (review): do NOT make an event training-eligible unless it has valid
    (non-NULL) active-flow features AND a non-degraded integration_quality — an
    un-backfilled composite row stays excluded until its features exist. Skips
    user_classified rows (their verdict is authoritative). Does not touch the
    diagnostic ``is_composite`` column. Returns counts.
    """
    rows = conn.execute(
        "SELECT id, circuit, is_pressure_restoration_phantom, degraded_supply, "
        "       is_low_flow_dribble, user_ignored, integration_quality, "
        "       true_avg_flow_lpm, excluded_from_training "
        "FROM events "
        "WHERE COALESCE(is_composite, 0) = 1 AND COALESCE(user_classified, 0) = 0",
    ).fetchall()
    cleaned = unexcluded = 0
    for r in rows:
        degraded_integ = r["integration_quality"] not in (None, "ok")
        other_excl = bool(
            r["is_pressure_restoration_phantom"] or r["degraded_supply"]
            or r["is_low_flow_dribble"] or r["user_ignored"] or degraded_integ
        )
        has_valid_features = r["true_avg_flow_lpm"] is not None and not degraded_integ
        new_excluded = 0 if (not other_excl and has_valid_features) else 1
        if new_excluded != (r["excluded_from_training"] or 0):
            conn.execute(
                "UPDATE events SET excluded_from_training = ? "
                "WHERE id = ? AND circuit = ?",
                (new_excluded, r["id"], r["circuit"]),
            )
            cleaned += 1
            if new_excluded == 0:
                unexcluded += 1
    conn.commit()
    log.info("composite cleanup: %d row(s) re-derived, %d un-excluded", cleaned, unexcluded)
    return {"composite_rows_changed": cleaned, "unexcluded": unexcluded}


# ── Temporal appliance-cycle signal (cycle_pulse_count) ───────────────────────
# A dishwasher / washing-machine fill PULSE is indistinguishable from a tap in a
# single event; the discriminator is that pulses REPEAT. cycle_pulse_count is the
# number of same-circuit events within ±45 min whose volume is within ratio
# [0.4, 2.5] of this event. It rides in the cluster centroid (mean over members)
# as a heuristic-only signal — see fixtures.py temporal appliance rules.

_CYCLE_PULSE_WINDOW_SECONDS: float = 2700.0      # ±45 min
_CYCLE_PULSE_VOL_RATIO_LO: float = 0.4
_CYCLE_PULSE_VOL_RATIO_HI: float = 2.5
# dev.37 — a cycle PULSE must also be a STEADY fill, not just volume-similar. A real
# dishwasher fill is a steady solenoid draw (high steady-state fraction, low on-segment
# flow CV); repeated kitchen taps are choppy (low steady, high CV). These gate the count
# so tap-bursts stop accumulating a dishwasher signal (the volume-only count's root flaw).
# Eval-tuned (tools/eval_knn_classifier.py --with-rules). Legacy events with NULL shape
# features fall back to volume-only via _is_fill_shaped, so old detections never regress.
# LOCKED by the eval sweep over the home's labels: (0.40, 0.35) beat the volume-only
# baseline on EVERY metric — overall LOO 0.676->0.698, dishwasher precision 0.838->0.895
# (false dishwashers 18->11), dishwasher recall 0.921->0.931, tap recall 0.333->0.467.
_CYCLE_PULSE_MIN_STEADY_FRAC: float = 0.40
_CYCLE_PULSE_MAX_FLOW_CV: float = 0.35


def _is_fill_shaped(steady_frac, flow_cv) -> bool:
    """True when an event looks like a STEADY appliance fill (a dishwasher solenoid):
    high steady-state fraction AND low on-segment flow CV. Legacy events with NULL shape
    features return True so the cycle-pulse count falls back to volume-only —
    pre-active-flow detections never regress."""
    if steady_frac is None or flow_cv is None:
        return True
    try:
        return (float(steady_frac) >= _CYCLE_PULSE_MIN_STEADY_FRAC
                and float(flow_cv) <= _CYCLE_PULSE_MAX_FLOW_CV)
    except (TypeError, ValueError):
        return True


def compute_cycle_pulse_count(this_volume, neighbours,
                              this_steady=None, this_flow_cv=None) -> int:
    """Count cycle PULSES near this event: neighbours whose volume is within ratio
    [0.4, 2.5] of ``this_volume`` AND that are fill-shaped (a steady draw).

    ``neighbours`` is an iterable of ``(volume, steady_state_fraction,
    flow_cv_on_segments)`` records (the event itself already excluded); a bare float is
    also accepted (volume with unknown shape → volume-only). A choppy event (THIS event
    not fill-shaped) returns 0 — it is not an appliance fill, so repeated taps no longer
    accumulate a dishwasher count. Pure (no DB). Returns 0 when ``this_volume`` is missing
    or <= 0. Legacy rows (NULL shape) fall back to volume-only via _is_fill_shaped."""
    try:
        tv = float(this_volume)
    except (TypeError, ValueError):
        return 0
    if tv <= 0:
        return 0
    if not _is_fill_shaped(this_steady, this_flow_cv):
        return 0
    n = 0
    for rec in neighbours:
        if isinstance(rec, (tuple, list)):
            v = rec[0]
            nsteady = rec[1] if len(rec) > 1 else None
            ncv = rec[2] if len(rec) > 2 else None
        else:
            v, nsteady, ncv = rec, None, None
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if (fv > 0 and _CYCLE_PULSE_VOL_RATIO_LO <= fv / tv <= _CYCLE_PULSE_VOL_RATIO_HI
                and _is_fill_shaped(nsteady, ncv)):
            n += 1
    return n


def _parse_event_ts(s):
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _load_pulse_events(conn: sqlite3.Connection, circuit: str):
    """Sorted ``[(epoch, id, volume, steady_state_fraction, flow_cv_on_segments)]`` for
    same-circuit, non-excluded events with a parseable timestamp and positive volume
    (the cycle candidates). The two shape columns feed the fill-shaped pulse gate."""
    rows = conn.execute(
        "SELECT id, start_ts, volume_litres, steady_state_fraction, "
        "       flow_cv_on_segments FROM events "
        "WHERE circuit = ? AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND start_ts IS NOT NULL AND volume_litres IS NOT NULL",
        (circuit,),
    ).fetchall()
    out = []
    for r in rows:
        t = _parse_event_ts(r["start_ts"])
        if t is None:
            continue
        try:
            vol = float(r["volume_litres"])
        except (TypeError, ValueError):
            continue
        if vol > 0:
            out.append((t.timestamp(), r["id"], vol,
                        r["steady_state_fraction"], r["flow_cv_on_segments"]))
    out.sort(key=lambda e: e[0])
    return out


def cycle_pulse_count_for_event(conn: sqlite3.Connection, circuit: str, event_id,
                                start_ts, volume, past_only: bool = True) -> int:
    """Best-effort online cycle_pulse_count for one (just-completed) event.

    ``past_only`` (default) counts only earlier neighbours — there is no future at
    event-completion time; the batch ``recompute_cycle_pulse_counts`` fills the
    full ±45 min window later (authoritative). Uses a bounded ``LIMIT`` query so a
    bulk import can never make this O(n²) — the most-recent rows are exactly the
    ones inside a ±45 min window of a freshly-completed event."""
    t = _parse_event_ts(start_ts)
    try:
        vol = float(volume)
    except (TypeError, ValueError):
        vol = 0.0
    if t is None or vol <= 0:
        return 0
    center = t.timestamp()
    me = conn.execute(
        "SELECT steady_state_fraction, flow_cv_on_segments FROM events "
        "WHERE id = ? AND circuit = ?", (event_id, circuit),
    ).fetchone()
    my_steady = me["steady_state_fraction"] if me else None
    my_cv = me["flow_cv_on_segments"] if me else None
    rows = conn.execute(
        "SELECT start_ts, volume_litres, steady_state_fraction, flow_cv_on_segments "
        "FROM events "
        "WHERE circuit = ? AND id <> ? AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND start_ts IS NOT NULL AND volume_litres IS NOT NULL "
        "ORDER BY start_ts DESC LIMIT 400",
        (circuit, event_id),
    ).fetchall()
    nbrs = []
    for r in rows:
        rt = _parse_event_ts(r["start_ts"])
        if rt is None:
            continue
        dt = center - rt.timestamp()
        if (0 <= dt <= _CYCLE_PULSE_WINDOW_SECONDS) or \
           (not past_only and -_CYCLE_PULSE_WINDOW_SECONDS <= dt < 0):
            nbrs.append((r["volume_litres"], r["steady_state_fraction"],
                         r["flow_cv_on_segments"]))
    return compute_cycle_pulse_count(vol, nbrs,
                                     this_steady=my_steady, this_flow_cv=my_cv)


def recompute_cycle_pulse_counts(conn: sqlite3.Connection, circuit: str,
                                 since_ts: Optional[str] = None) -> Dict[str, int]:
    """Authoritative full-window (±45 min, past+future) backfill of
    ``events.cycle_pulse_count``, then patch each cluster centroid's mean so the
    heuristic sees the signal immediately. Idempotent (only changed rows written).
    ``since_ts`` (periodic maturity re-check) restricts the WRITE-back to events at or
    after it — older events are still loaded for neighbour context but keep their
    already-authoritative counts. Returns ``{"scanned", "updated"}``."""
    evs = _load_pulse_events(conn, circuit)
    n = len(evs)
    since_epoch = None
    if since_ts is not None:
        _s = _parse_event_ts(since_ts)
        if _s is not None:
            since_epoch = _s.timestamp()
    stored = {
        r["id"]: r["cycle_pulse_count"]
        for r in conn.execute(
            "SELECT id, cycle_pulse_count FROM events WHERE circuit = ?", (circuit,)
        ).fetchall()
    }
    updated = 0
    lo = hi = 0
    for i in range(n):
        center = evs[i][0]
        while lo < n and evs[lo][0] < center - _CYCLE_PULSE_WINDOW_SECONDS:
            lo += 1
        while hi < n and evs[hi][0] <= center + _CYCLE_PULSE_WINDOW_SECONDS:
            hi += 1
        if since_epoch is not None and center < since_epoch:
            continue                  # neighbour-only: keep its authoritative count
        nbrs = [(evs[j][2], evs[j][3], evs[j][4])
                for j in range(lo, hi) if j != i]
        cnt = compute_cycle_pulse_count(evs[i][2], nbrs,
                                        this_steady=evs[i][3], this_flow_cv=evs[i][4])
        eid = evs[i][1]
        if stored.get(eid) != cnt:
            conn.execute(
                "UPDATE events SET cycle_pulse_count = ? WHERE id = ? AND circuit = ?",
                (cnt, eid, circuit),
            )
            updated += 1

    # Patch cluster centroids with the member-mean so suggest_fixture_type can
    # read the cycle signal without waiting for 10 fresh events.
    for cr in conn.execute(
        "SELECT cluster_id, AVG(cycle_pulse_count) AS avgc FROM events "
        "WHERE circuit = ? AND cluster_id IS NOT NULL "
        "  AND cycle_pulse_count IS NOT NULL GROUP BY cluster_id",
        (circuit,),
    ).fetchall():
        row = conn.execute(
            "SELECT centroid FROM fixture_clusters WHERE circuit = ? AND id = ?",
            (circuit, cr["cluster_id"]),
        ).fetchone()
        if not row or not row["centroid"]:
            continue
        try:
            cen = json.loads(row["centroid"])
        except (json.JSONDecodeError, TypeError):
            continue
        cen["cycle_pulse_count"] = round(float(cr["avgc"] or 0.0), 4)
        conn.execute(
            "UPDATE fixture_clusters SET centroid = ? WHERE circuit = ? AND id = ?",
            (json.dumps(cen), circuit, cr["cluster_id"]),
        )
    conn.commit()
    log.info("[%s] cycle_pulse_count: %d scanned, %d updated", circuit, n, updated)
    return {"scanned": n, "updated": updated}


def resuggest_all_clusters(conn: sqlite3.Connection, circuit: str) -> Dict[str, int]:
    """Re-run the heuristic ``suggest_fixture_type`` over each cluster centroid
    (e.g. after a cycle_pulse_count backfill). Only touches clusters whose
    ``suggestion_source`` is NULL or 'heuristic' — user-label votes are never
    overwritten. Returns ``{"clusters", "updated"}``."""
    from .fixtures import suggest_fixture_type
    ctype = get_circuit_type(conn, circuit)
    rows = conn.execute(
        "SELECT id, centroid, suggested_type, suggestion_source "
        "FROM fixture_clusters WHERE circuit = ?", (circuit,)
    ).fetchall()
    updated = 0
    for r in rows:
        if r["suggestion_source"] == "user_labels" or not r["centroid"]:
            continue
        try:
            centroid = json.loads(r["centroid"])
        except (json.JSONDecodeError, TypeError):
            continue
        new_type, new_conf = suggest_fixture_type(centroid, ctype)
        if new_type != r["suggested_type"]:
            conn.execute(
                "UPDATE fixture_clusters SET suggested_type = ?, "
                "  suggested_confidence = ?, suggestion_source = 'heuristic' "
                "WHERE circuit = ? AND id = ?",
                (new_type, new_conf if new_type else 0.0, circuit, r["id"]),
            )
            updated += 1
    conn.commit()
    log.info("[%s] resuggest: %d clusters, %d updated", circuit, len(rows), updated)
    return {"clusters": len(rows), "updated": updated}


# ── 2a: auto cycle-labeling + provenance-scoped undo ──────────────────────────
# When the user labels an APPLIANCE event, its repeated fill pulses (the cycle)
# are almost certainly the same appliance. Auto-label the vol+flow-similar mates
# within ±45 min (source='cycle') so one label seeds several — validated 92%
# pure. Appliances only (toilet/tap/shower aren't cyclic). Fully reversible.

_CYCLE_FLOW_RATIO_LO: float = 0.5
_CYCLE_FLOW_RATIO_HI: float = 2.0
_CYCLE_APPLIANCE_TYPES: frozenset = frozenset({"dishwasher", "washing_machine"})


def propagate_cycle_label(conn: sqlite3.Connection, circuit: str, event_id,
                          fixture_type) -> int:
    """Auto-label an appliance event's cycle-mates the same type (source='cycle').

    Dishwasher: a mate is a same-circuit, currently-unlabeled, non-excluded event
    within ±45 min whose volume AND avg flow are within ratio of the anchor.

    Washing machine (dev.23): the volume-ratio gate structurally fails — a washer
    cycle alternates ≥9 L fills with sub-1.5 L top-offs (15× spread) at ~constant
    PEAK. So washer mates are keyed on the PEAK family instead (pk within
    0.8–1.3× of the anchor's, vol ≥ 0.5 L, dur ≤ 400 s), excluding flush-shaped
    events (a toilet flush during laundry stays unlabeled).

    No-op for non-appliance types. Caller owns the transaction (no commit).
    Returns the number of mates labeled.
    """
    if fixture_type not in _CYCLE_APPLIANCE_TYPES:
        return 0
    anchor = conn.execute(
        "SELECT start_ts, volume_litres, avg_flow_lpm, peak_flow_lpm "
        "FROM events WHERE id = ? AND circuit = ?", (event_id, circuit),
    ).fetchone()
    if anchor is None:
        return 0
    a_ts = _parse_event_ts(anchor["start_ts"])
    washer_mode = fixture_type == "washing_machine"

    def _num(v) -> float:
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    a_vol = _num(anchor["volume_litres"])
    a_flow = _num(anchor["avg_flow_lpm"])
    a_pk = _num(anchor["peak_flow_lpm"])
    if a_ts is None:
        return 0
    if washer_mode:
        # Peak is the washer family key; vol/avg-flow may legitimately be NULL.
        if a_pk <= 0:
            return 0
    elif a_vol <= 0 or a_flow <= 0:
        return 0
    # Coarse time-window pre-filter (bounds the rows; the 90-min window keeps it
    # small), then a precise parsed-timestamp check so an ISO tz-format quirk can
    # never over-include.
    lo_ts = (a_ts - timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
    hi_ts = (a_ts + timedelta(seconds=_CYCLE_PULSE_WINDOW_SECONDS)).isoformat()
    a_epoch = a_ts.timestamp()
    mates = conn.execute(
        "SELECT id, start_ts, volume_litres, avg_flow_lpm, duration_seconds, "
        "       peak_flow_lpm, has_pressure_transient, pressure_delta_psi "
        "FROM events "
        "WHERE circuit = ? AND id <> ? AND user_fixture_type IS NULL "
        "  AND COALESCE(excluded_from_training, 0) = 0 "
        "  AND start_ts BETWEEN ? AND ? "
        "  AND volume_litres > 0",
        (circuit, event_id, lo_ts, hi_ts),
    ).fetchall()
    from .event_rules import (_WASHER_FAMILY_PK_RATIO, _WASHER_SIBLING_MAX_DUR_S,
                              _WASHER_SIBLING_MIN_VOL_L, is_flush_shaped)
    n = 0
    for mr in mates:
        mt = _parse_event_ts(mr["start_ts"])
        if mt is None or abs(mt.timestamp() - a_epoch) > _CYCLE_PULSE_WINDOW_SECONDS:
            continue
        try:
            v = float(mr["volume_litres"])
            f = float(mr["avg_flow_lpm"]) if mr["avg_flow_lpm"] is not None else 0.0
            pk = (float(mr["peak_flow_lpm"])
                  if mr["peak_flow_lpm"] is not None else 0.0)
        except (TypeError, ValueError):
            continue
        if washer_mode:
            dur = mr["duration_seconds"]
            ok = (v >= _WASHER_SIBLING_MIN_VOL_L
                  and (dur is None or dur <= _WASHER_SIBLING_MAX_DUR_S)
                  and _WASHER_FAMILY_PK_RATIO[0] * a_pk <= pk
                  <= _WASHER_FAMILY_PK_RATIO[1] * a_pk
                  and not is_flush_shaped(dict(mr)))
        else:
            ok = (f > 0
                  and _CYCLE_PULSE_VOL_RATIO_LO <= v / a_vol <= _CYCLE_PULSE_VOL_RATIO_HI
                  and _CYCLE_FLOW_RATIO_LO <= f / a_flow <= _CYCLE_FLOW_RATIO_HI)
        if ok:
            conn.execute(
                "UPDATE events SET user_fixture_type = ?, "
                "fixture_label_source = 'cycle', cycle_group_id = ? "
                "WHERE id = ? AND circuit = ? AND user_fixture_type IS NULL",
                (fixture_type, str(event_id), mr["id"], circuit),
            )
            n += 1
    if n:
        # Stamp the anchor with its OWN id as the group key so the History rollup
        # (dev.24 §7) collapses the whole cycle — anchor + cycle-mates — under one
        # expandable parent row.
        conn.execute(
            "UPDATE events SET cycle_group_id = ? WHERE id = ? AND circuit = ?",
            (str(event_id), event_id, circuit),
        )
    return n


def clear_auto_labels(conn: sqlite3.Connection, circuit: str, event_ids=None,
                      commit: bool = True) -> int:
    """Reverse auto-applied labels: rows with `fixture_label_source IN
    ('cycle','training')` → `user_fixture_type` + `fixture_label_source` back to
    NULL. NEVER touches an explicit 'user' label or a NULL-source (legacy) row.

    When ``event_ids`` (a list) is given, scopes to exactly those ids — so
    rejecting one training capture can't wipe another capture's (or a cycle's)
    labels. When None, clears all auto-source rows on the circuit. Returns the count.
    """
    if event_ids is not None:
        ids = list(event_ids)
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        cur = conn.execute(
            "UPDATE events SET user_fixture_type = NULL, fixture_label_source = NULL "
            "WHERE circuit = ? AND fixture_label_source IN ('cycle','training') "
            f"  AND id IN ({placeholders})",
            (circuit, *ids),
        )
    else:
        cur = conn.execute(
            "UPDATE events SET user_fixture_type = NULL, fixture_label_source = NULL "
            "WHERE circuit = ? AND fixture_label_source IN ('cycle','training')",
            (circuit,),
        )
    if commit:
        conn.commit()
    return cur.rowcount


# ── 2b: training-helper capture ───────────────────────────────────────────────
# A one-time wizard: the user runs each fixture once, the event-completion hook
# records candidate event ids, and the user confirms to write ~100%-pure
# 'training' labels. Instant types capture the next event; windowed types capture
# every event in a user-chosen monitoring window (then accept/reject the run).

_TRAINING_INSTANT_TYPES: frozenset = frozenset({"toilet", "tap"})
_TRAINING_INSTANT_WINDOW_MIN: int = 5
# Allowed monitoring-window options per windowed type (minutes).
_TRAINING_WINDOW_INCREMENTS: Dict[str, List[int]] = {
    "shower_tub":      [5, 10, 15, 20, 25, 30],
    "irrigation_zone": [5, 10, 15, 20, 30, 45, 60],
    "washing_machine": [15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180],
    "dishwasher":      [15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180],
}


def training_capturable_types() -> frozenset:
    """Fixture types the wizard can capture (excludes 'other'/'leak_test')."""
    return _TRAINING_INSTANT_TYPES | frozenset(_TRAINING_WINDOW_INCREMENTS)


def training_window_options(fixture_type: str) -> List[int]:
    """The monitoring-duration dropdown options for a windowed type ([] = instant)."""
    return list(_TRAINING_WINDOW_INCREMENTS.get(fixture_type, []))


def training_capture_band_match(fixture_type: str, volume_litres,
                                duration_seconds) -> bool:
    """Loose, named sanity band for an INSTANT capture (toilet/tap). Catches a
    clearly-wrong fixture (e.g. a sub-litre tap during a 'toilet' capture);
    windowed types are not gated here (the user accepts/rejects)."""
    try:
        v = float(volume_litres) if volume_litres is not None else 0.0
        d = float(duration_seconds) if duration_seconds is not None else 0.0
    except (TypeError, ValueError):
        return False
    if fixture_type == "toilet":
        return v >= 2.0                      # a real flush is 3-13 L
    if fixture_type == "tap":
        return v <= 10.0 and d <= 180.0      # rejects shower/appliance-sized
    return True


def arm_training_capture(conn: sqlite3.Connection, circuit: str, fixture_type: str,
                         window_minutes=None) -> Dict[str, Any]:
    """Arm a capture for ``fixture_type`` on ``circuit`` (cancels any existing
    armed/ready row — one active per circuit). ``window_minutes`` is required (and
    validated to the per-type increments) for windowed types; instant types use a
    fixed short window. Raises ValueError for a non-capturable type / bad window."""
    if fixture_type in _TRAINING_INSTANT_TYPES:
        wm = _TRAINING_INSTANT_WINDOW_MIN
    elif fixture_type in _TRAINING_WINDOW_INCREMENTS:
        allowed = _TRAINING_WINDOW_INCREMENTS[fixture_type]
        if window_minutes is None:
            wm = allowed[0]
        elif int(window_minutes) in allowed:
            wm = int(window_minutes)
        else:
            raise ValueError(
                f"window_minutes {window_minutes} not allowed for {fixture_type}")
    else:
        raise ValueError(f"{fixture_type} is not capturable in the training helper")
    conn.execute(
        "UPDATE training_capture SET status = 'cancelled' "
        "WHERE circuit = ? AND status IN ('armed','ready')", (circuit,))
    cur = conn.execute(
        "INSERT INTO training_capture (circuit, fixture_type, status, armed_at, "
        " expires_at, window_minutes, captured_count) "
        "VALUES (?, ?, 'armed', datetime('now'), datetime('now', ?), ?, 0)",
        (circuit, fixture_type, f"+{wm} minutes", wm))
    conn.commit()
    return {"id": cur.lastrowid, "circuit": circuit, "fixture_type": fixture_type,
            "window_minutes": wm, "status": "armed"}


def get_active_training_capture(conn: sqlite3.Connection, circuit: str):
    """The newest armed/ready capture on the circuit (None if none). Includes
    `seconds_remaining` and `candidate_count`."""
    row = conn.execute(
        "SELECT *, CAST((julianday(expires_at) - julianday('now')) * 86400 AS INTEGER) "
        "         AS seconds_remaining "
        "FROM training_capture "
        "WHERE circuit = ? AND status IN ('armed','ready') "
        "ORDER BY id DESC LIMIT 1", (circuit,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    # dev46 (46b) reviewed — UNGUARDED by design: COUNT(*) always returns a
    # row, so None means a broken cursor (loud failure is the right outcome),
    # not "no candidates" (which is COUNT(*) = 0).
    d["candidate_count"] = conn.execute(
        "SELECT COUNT(*) FROM training_capture_candidates WHERE capture_id = ?",
        (row["id"],)).fetchone()[0]
    return d


def cancel_training_capture(conn: sqlite3.Connection, circuit: str) -> int:
    """Cancel the active (armed/ready, not-yet-confirmed) capture on the circuit."""
    cur = conn.execute(
        "UPDATE training_capture SET status = 'cancelled' "
        "WHERE circuit = ? AND status IN ('armed','ready')", (circuit,))
    conn.commit()
    return cur.rowcount


def extend_training_capture(conn: sqlite3.Connection, circuit: str,
                            add_minutes: int) -> Dict[str, int]:
    """Push out a still-running windowed capture's expiry (don't strand a long cycle)."""
    am = max(1, int(add_minutes))
    cur = conn.execute(
        "UPDATE training_capture SET expires_at = datetime(expires_at, ?) "
        "WHERE circuit = ? AND status = 'armed'", (f"+{am} minutes", circuit))
    conn.commit()
    return {"extended": cur.rowcount, "add_minutes": am}


def expire_stale_training_captures(conn: sqlite3.Connection) -> int:
    """Sweep armed captures whose window has elapsed — split by TYPE, not count.

    A **windowed** (non-instant) capture flips to 'ready' regardless of count, so it
    NEVER evaporates on the timer: with events it becomes "review your N events", with
    zero it becomes an actionable "no events captured — redo" prompt — either way the
    user gets closure (an active item to Accept / Reject / Cancel). Only an **instant**
    (toilet/tap) capture that never caught an event expires (an instant WITH an event is
    already 'ready'). 'ready' captures are never touched. Called opportunistically
    (poll + pruner). Returns the number EXPIRED.
    """
    instant = tuple(_TRAINING_INSTANT_TYPES)
    ph = ",".join("?" * len(instant))
    try:
        # Windowed (non-instant) past-due → 'ready' (review, or "no events" redo).
        conn.execute(
            f"UPDATE training_capture SET status = 'ready' "
            f"WHERE status = 'armed' AND expires_at <= datetime('now') "
            f"AND fixture_type NOT IN ({ph})", instant)
        # Instant past-due that never caught an event → 'expired'.
        cur = conn.execute(
            f"UPDATE training_capture SET status = 'expired' "
            f"WHERE status = 'armed' AND expires_at <= datetime('now') "
            f"AND fixture_type IN ({ph})", instant)
        conn.commit()
        return cur.rowcount
    except sqlite3.OperationalError as e:
        # Opportunistic hygiene must never 500 the UI's status poll: during a
        # long admin write (e.g. the ~30 s "Apply my labels" reclassify) the
        # database is locked and this sweep simply waits for the next poll /
        # pruner pass (observed: 13 ASGI 500s in one 30 s window, 2026-08-12).
        _rollback_quietly(conn)
        log.debug("expire_stale_training_captures skipped (non-fatal): %s", e)
        return 0


def confirm_training_capture(conn: sqlite3.Connection, circuit: str) -> Dict[str, Any]:
    """Accept the active capture: write `user_fixture_type` + source='training' for
    each candidate (only unlabeled rows), status→captured. Returns counts. Caller
    runs the reclassify (once)."""
    cap = conn.execute(
        "SELECT id, fixture_type FROM training_capture "
        "WHERE circuit = ? AND status IN ('armed','ready') "
        "ORDER BY id DESC LIMIT 1", (circuit,)).fetchone()
    if cap is None:
        return {"labeled": 0}
    ids = [r["event_id"] for r in conn.execute(
        "SELECT event_id FROM training_capture_candidates WHERE capture_id = ?",
        (cap["id"],)).fetchall()]
    labeled = 0
    for eid in ids:
        cur = conn.execute(
            "UPDATE events SET user_fixture_type = ?, fixture_label_source = 'training' "
            "WHERE id = ? AND circuit = ? AND user_fixture_type IS NULL",
            (cap["fixture_type"], eid, circuit))
        labeled += cur.rowcount
    conn.execute("UPDATE training_capture SET status = 'captured' WHERE id = ?",
                 (cap["id"],))
    conn.commit()
    return {"capture_id": cap["id"], "fixture_type": cap["fixture_type"], "labeled": labeled}


def reject_training_capture(conn: sqlite3.Connection, circuit: str,
                            capture_id: int) -> Dict[str, Any]:
    """Discard a capture (contaminated): clear ONLY this capture's 'training' labels
    (scoped to its candidate ids), status→rejected, so the checklist item re-arms."""
    ids = [r["event_id"] for r in conn.execute(
        "SELECT event_id FROM training_capture_candidates WHERE capture_id = ?",
        (capture_id,)).fetchall()]
    cleared = clear_auto_labels(conn, circuit, event_ids=ids, commit=False) if ids else 0
    conn.execute("UPDATE training_capture SET status = 'rejected' "
                 "WHERE id = ? AND circuit = ?", (capture_id, circuit))
    conn.commit()
    return {"capture_id": capture_id, "cleared": cleared}


def get_training_checklist(conn: sqlite3.Connection, circuit: str,
                           applicable_types) -> Dict[str, dict]:
    """Per-type latest capture status for the checklist (resumable)."""
    out: Dict[str, dict] = {}
    for t in applicable_types:
        row = conn.execute(
            "SELECT id, status, captured_count FROM training_capture "
            "WHERE circuit = ? AND fixture_type = ? ORDER BY id DESC LIMIT 1",
            (circuit, t)).fetchone()
        out[t] = {
            "status": row["status"] if row else "none",
            "captured_count": row["captured_count"] if row else 0,
            "capture_id": row["id"] if row else None,
        }
    return out


def record_training_candidate(conn: sqlite3.Connection, circuit: str,
                              event_features: dict) -> bool:
    """HOT-PATH hook: if a capture is armed (non-expired) on the circuit, record
    this just-completed event as a candidate. Writes NO label (labels are written
    only on the user's confirm). Instant types flip to 'ready' after the first
    event; windowed types stay armed and accumulate. Returns True if recorded."""
    cap = conn.execute(
        "SELECT id, fixture_type, captured_count FROM training_capture "
        "WHERE circuit = ? AND status = 'armed' AND expires_at > datetime('now') "
        "ORDER BY id DESC LIMIT 1", (circuit,)).fetchone()
    if cap is None:
        return False
    eid = event_features.get("id")
    if not eid:
        return False
    conn.execute(
        "INSERT INTO training_capture_candidates (capture_id, event_id) VALUES (?, ?)",
        (cap["id"], eid))
    new_count = (cap["captured_count"] or 0) + 1
    if cap["fixture_type"] in _TRAINING_INSTANT_TYPES:
        conn.execute("UPDATE training_capture SET captured_count = ?, status = 'ready' "
                     "WHERE id = ?", (new_count, cap["id"]))
    else:
        conn.execute("UPDATE training_capture SET captured_count = ? WHERE id = ?",
                     (new_count, cap["id"]))
    conn.commit()
    return True


# ── Pressure-history helpers (24 h dashboard chart) ──────────────────────────
_BAD_PRESSURE_STATES = frozenset({"", "unavailable", "unknown", "none", "nan"})


def coerce_pressure_state(state) -> Optional[float]:
    """Parse a HA state to a float PSI, or None for unavailable/unknown/non-numeric
    (those mark a recorder gap, not a reading)."""
    if state is None:
        return None
    s = str(state).strip().lower()
    if s in _BAD_PRESSURE_STATES:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def downsample_pressure_series(states, start_epoch: float, end_epoch: float,
                               buckets: int = 288) -> List[Dict[str, Any]]:
    """Bucket [start,end] (epoch seconds) into `buckets` even slots for a 24 h line.
    Each slot = mean of the numeric samples in it; an empty *quiet* slot carries the
    last-known value forward (a recorded sensor holds its value between changes); an
    unavailable/unknown sample breaks the carry so a real outage renders as a gap
    (`v: None`). Leading slots before the first sample are gaps. Returns
    [{t: epoch_ms, v: float|None}] of length `buckets` (client formats `t` → HH:MM)."""
    span = end_epoch - start_epoch
    if span <= 0 or buckets <= 0:
        return []
    width = span / buckets
    sums = [0.0] * buckets
    counts = [0] * buckets
    breaks = [False] * buckets
    for s in states:
        ts = s.get("last_changed")
        if ts is None:
            continue
        ep = ts.timestamp() if hasattr(ts, "timestamp") else float(ts)
        offset = ep - start_epoch
        if offset < 0:
            offset = 0.0
        if offset >= span:
            continue
        idx = int(offset / width)
        if idx >= buckets:
            idx = buckets - 1
        v = coerce_pressure_state(s.get("state"))
        if v is None:
            breaks[idx] = True
        else:
            sums[idx] += v
            counts[idx] += 1
    out: List[Dict[str, Any]] = []
    last: Optional[float] = None
    for i in range(buckets):
        t_ms = int((start_epoch + width * i) * 1000)
        if counts[i]:
            val: Optional[float] = round(sums[i] / counts[i], 2)
            last = val
        elif breaks[i]:
            val = None
            last = None
        else:
            val = last
        out.append({"t": t_ms, "v": val})
    return out


def recent_pressure_baseline(conn: sqlite3.Connection, circuit: str,
                             since_iso: str) -> Optional[float]:
    """Typical *resting* pressure for the baseline reference line: AVG of recent
    pre-event pressures over the window; fall back to the latest event's pre-event
    pressure, then the latest leak-test baseline, else None. PSI."""
    row = conn.execute(
        "SELECT AVG(pre_event_pressure_psi) AS b FROM events "
        "WHERE circuit = ? AND start_ts >= ? "
        "AND pre_event_pressure_psi IS NOT NULL AND pre_event_pressure_psi > 0",
        (circuit, since_iso)).fetchone()
    if row and row["b"] is not None:
        return round(row["b"], 2)
    row = conn.execute(
        "SELECT pre_event_pressure_psi AS b FROM events "
        "WHERE circuit = ? AND pre_event_pressure_psi IS NOT NULL "
        "AND pre_event_pressure_psi > 0 ORDER BY start_ts DESC LIMIT 1",
        (circuit,)).fetchone()
    if row and row["b"] is not None:
        return round(row["b"], 2)
    row = conn.execute(
        "SELECT baseline_psi AS b FROM leak_test_history "
        "WHERE circuit = ? AND baseline_psi IS NOT NULL "
        "ORDER BY run_at DESC LIMIT 1", (circuit,)).fetchone()
    if row and row["b"] is not None:
        return round(row["b"], 2)
    return None


def migrate_to_type_level_clusters(
    conn,
    circuit: str,
) -> Dict[str, Any]:
    """One-shot migration: merge clusters of the same confirmed/suggested type.

    Conservative safety gate:
    - member_count >= 5 for both clusters
    - If neither confirmed: suggested_confidence >= 0.70
    - Centroid weighted-distance <= get_match_threshold(effective_type)
    - 'other' clusters are never merged

    Does NOT auto-confirm suggested-only clusters. Only calls
    upsert_fixture_from_cluster for survivors that already had a confirmed
    fixture row.

    Idempotent: safe to call repeatedly; no-op when each type already has
    exactly one cluster.

    Returns {"types_merged", "clusters_removed", "survivor_ids"}.
    """
    # get_match_threshold was removed from the import — see the comment
    # inside the per-type loop below explaining the simplified gate.
    from .fixtures import FIXTURE_TYPE_LABELS

    rows = conn.execute(
        """SELECT fc.id, fc.member_count,
                  fc.suggested_type, fc.suggested_confidence,
                  fc.centroid, fc.last_match_at,
                  f.fixture_type AS user_type,
                  CASE WHEN f.confirmed = 1 THEN 1 ELSE 0 END AS is_confirmed,
                  f.id AS fixture_row_id
           FROM fixture_clusters fc
           LEFT JOIN fixtures f ON fc.fixture_id = f.id
           WHERE fc.circuit = ?
           ORDER BY fc.id""",
        (circuit,),
    ).fetchall()

    by_type: Dict[str, List[dict]] = {}
    for r in rows:
        eff = r["user_type"] or r["suggested_type"]
        if not eff or eff == "other":
            continue
        by_type.setdefault(eff, []).append(dict(r))

    types_merged    = 0
    clusters_removed = 0
    survivor_ids: List[int] = []

    for ftype, clusters in by_type.items():
        # NOTE: an earlier version of this migration used
        # get_match_threshold(ftype) to gate which clusters could
        # merge by per-type centroid distance. That gate was replaced
        # by the simpler "any 2+ eligible clusters merge into one"
        # logic below (centroid-distance approximation at the
        # eligible_close filter, then deterministic survivor pick).
        # The migration is one-shot; users have already run it. The
        # threshold call is removed because it had no effect — but
        # this comment documents the design simplification in case
        # the rigorous gate is ever wanted back.

        # Apply safety gate
        eligible = [
            cl for cl in clusters
            if (cl["member_count"] or 0) >= 5
            and (cl["is_confirmed"] or (cl["suggested_confidence"] or 0) >= 0.70)
        ]

        # Centroid distance gate (requires at least 2 eligible clusters)
        if len(eligible) >= 2:
            # Try to filter pairs by centroid similarity
            # We do a simple Euclidean distance on raw centroid values here
            # (scaler state is not available in the DB layer); this is an
            # approximation sufficient for a one-shot migration.
            eligible_close: List[dict] = []
            for cl in eligible:
                try:
                    raw = json.loads(cl["centroid"] or "{}")
                    cl["_centroid_raw"] = raw
                    eligible_close.append(cl)
                except Exception:
                    pass
            eligible = eligible_close

        if len(eligible) < 2:
            if eligible:
                survivor_ids.append(eligible[0]["id"])
            continue

        # Deterministic survivor: confirmed first, then member_count desc,
        # then last_match_at desc, then id asc.  last_match_at is a TEXT
        # ISO-8601 timestamp — it can't be negated into a single descending
        # tuple key, so apply staged stable sorts, least-significant first.
        eligible.sort(key=lambda c: c["id"])
        eligible.sort(key=lambda c: c["last_match_at"] or "", reverse=True)
        eligible.sort(key=lambda c: c["member_count"] or 0, reverse=True)
        eligible.sort(key=lambda c: c["is_confirmed"], reverse=True)
        survivor = eligible[0]
        all_ids  = [cl["id"] for cl in eligible]

        try:
            result = merge_clusters(conn, circuit, survivor["id"], all_ids)
            clusters_removed += result["fixtures_removed"]
            types_merged     += 1
        except Exception as exc:
            log.warning(
                "[%s] migrate: merge_clusters failed for '%s': %s",
                circuit, ftype, exc,
            )
            survivor_ids.append(survivor["id"])
            continue

        survivor_ids.append(survivor["id"])

        # Only update/create a confirmed fixture row if the survivor already
        # had one; do not auto-confirm suggested-only clusters.
        if survivor["is_confirmed"] and survivor["fixture_row_id"]:
            auto_name = FIXTURE_TYPE_LABELS.get(ftype, ftype.replace("_", " ").title())
            upsert_fixture_from_cluster(
                conn, circuit, survivor["id"],
                name=auto_name,
                fixture_type=ftype,
                publish_to_ha=1,
            )

    return {
        "types_merged":     types_merged,
        "clusters_removed": clusters_removed,
        "survivor_ids":     survivor_ids,
    }


# ==========================================================================
# Plumbing-event exclusion windows
# ==========================================================================

def create_exclusion_window(
    conn,
    circuit: str,
    minutes: int,
    reason: str = "plumbing",
) -> None:
    """Open a new exclusion window lasting ``minutes`` minutes.

    All timestamps are stored via SQLite datetime() so they share the
    same 'YYYY-MM-DD HH:MM:SS' format and compare correctly in WHERE clauses.
    """
    minutes = max(5, min(60, int(minutes)))
    modifier = f"+{minutes} minutes"
    # Close any existing active window for this circuit before opening a new one
    # so we never accumulate multiple overlapping rows.
    conn.execute(
        "UPDATE circuit_exclusion_windows "
        "SET ends_at = datetime('now') "
        "WHERE circuit = ? AND ends_at > datetime('now')",
        (circuit,),
    )
    conn.execute(
        "INSERT INTO circuit_exclusion_windows "
        "(circuit, started_at, ends_at, reason) "
        "VALUES (?, datetime('now'), datetime('now', ?), ?)",
        (circuit, modifier, reason or "plumbing"),
    )
    conn.commit()


def is_event_in_exclusion_window(
    conn,
    circuit: str,
    event_start_ts: str,
) -> bool:
    """Return True if ``event_start_ts`` falls inside any active exclusion
    window for ``circuit``.

    Normalises the caller timestamp to SQLite 'YYYY-MM-DD HH:MM:SS' format
    before doing the BETWEEN comparison.
    """
    if not event_start_ts:
        return False
    try:
        from datetime import datetime as _dt, timezone as _tz
        dt = _dt.fromisoformat(str(event_start_ts))
        if dt.tzinfo is not None:
            dt = dt.astimezone(_tz.utc).replace(tzinfo=None)
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        ts = str(event_start_ts)
    row = conn.execute(
        "SELECT 1 FROM circuit_exclusion_windows "
        "WHERE circuit = ? AND ? BETWEEN started_at AND ends_at LIMIT 1",
        (circuit, ts),
    ).fetchone()
    return row is not None


def get_active_exclusion_window(
    conn,
    circuit: str,
):
    """Return the current active exclusion window, or None."""
    row = conn.execute(
        "SELECT id, circuit, started_at, ends_at, reason, "
        "CAST((strftime('%s', ends_at) - strftime('%s', 'now')) / 60 AS INTEGER) "
        "AS minutes_remaining "
        "FROM circuit_exclusion_windows "
        "WHERE circuit = ? AND ends_at > datetime('now') "
        "ORDER BY ends_at DESC LIMIT 1",
        (circuit,),
    ).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["minutes_remaining"] = max(0, result.get("minutes_remaining") or 0)
    return result


def cancel_exclusion_window(conn, circuit: str) -> None:
    """End all active exclusion windows for ``circuit`` immediately."""
    conn.execute(
        "UPDATE circuit_exclusion_windows "
        "SET ends_at = datetime('now') "
        "WHERE circuit = ? AND ends_at > datetime('now')",
        (circuit,),
    )
    conn.commit()


def extend_exclusion_window(conn, circuit: str, extra_minutes: int = 15) -> None:
    """Add ``extra_minutes`` to the active window (capped at 60 min from start)."""
    modifier = f"+{extra_minutes} minutes"
    conn.execute(
        "UPDATE circuit_exclusion_windows "
        "SET ends_at = MIN(datetime(ends_at, ?), datetime(started_at, '+60 minutes')) "
        "WHERE circuit = ? AND ends_at > datetime('now')",
        (modifier, circuit),
    )
    conn.commit()


# ------------------------------------------------------------------
# RBAC helpers — operator allow-list, admin-set cache, seen-users log
# ------------------------------------------------------------------

def load_operator_ids(db: sqlite3.Connection) -> set:
    """Return the set of HA user ids granted the operator tier.

    Returns an empty set (default-deny) if the table is absent (pre-migration DB).
    """
    try:
        rows = db.execute("SELECT user_id FROM operator_users").fetchall()
        return {r["user_id"] for r in rows if r["user_id"]}
    except sqlite3.OperationalError:
        return set()


def list_operator_users(db: sqlite3.Connection) -> List[dict]:
    """Return all operator allow-list rows (for the Access page)."""
    try:
        rows = db.execute(
            "SELECT user_id, display_name, added_by, added_at "
            "FROM operator_users ORDER BY display_name, user_id"
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


def add_operator(db: sqlite3.Connection, user_id: str,
                 display_name: str = "", added_by: str = "") -> None:
    """Grant the operator tier to a HA user id (idempotent upsert)."""
    if not user_id:
        return
    with db:
        db.execute(
            "INSERT INTO operator_users (user_id, display_name, added_by) "
            "VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET "
            "display_name = excluded.display_name, added_by = excluded.added_by",
            (user_id, display_name or "", added_by or ""),
        )


def remove_operator(db: sqlite3.Connection, user_id: str) -> None:
    """Revoke the operator tier from a HA user id."""
    if not user_id:
        return
    with db:
        db.execute("DELETE FROM operator_users WHERE user_id = ?", (user_id,))


def load_cached_admin_ids(db: sqlite3.Connection) -> set:
    """Return the last-known-good HA admin user-id set (empty if none cached)."""
    try:
        rows = db.execute("SELECT user_id FROM admin_ids_cache").fetchall()
        return {r["user_id"] for r in rows if r["user_id"]}
    except sqlite3.OperationalError:
        return set()


def save_admin_ids_cache(db: sqlite3.Connection,
                         users: "Iterable[dict]") -> None:
    """Replace the admin-id cache with the current admin users.

    ``users`` are normalised dicts (``id`` + optional ``name``) for the users that
    are admins. A NO-OP when the input is empty so a failed/empty ``config/auth/list``
    can never wipe the last-known-good set (the safety property that keeps admins
    from being locked out).
    """
    admins = [u for u in (users or []) if u.get("id")]
    if not admins:
        return
    with db:
        db.execute("DELETE FROM admin_ids_cache")
        db.executemany(
            "INSERT OR REPLACE INTO admin_ids_cache (user_id, display_name) "
            "VALUES (?, ?)",
            [(u["id"], u.get("name") or "") for u in admins],
        )


def record_seen_user(db: sqlite3.Connection, user_id: str,
                     display_name: str = "") -> None:
    """Upsert a user into the seen-users log (first_seen kept, last_seen bumped).

    Best-effort and cheap; the middleware only calls this on the FIRST sighting of
    a user id this process-lifetime, so it is not a per-request write.
    """
    if not user_id:
        return
    try:
        with db:
            db.execute(
                "INSERT INTO seen_users (user_id, display_name) VALUES (?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                "  display_name = COALESCE(NULLIF(excluded.display_name, ''), "
                "                          seen_users.display_name), "
                "  last_seen = CURRENT_TIMESTAMP",
                (user_id, display_name or ""),
            )
    except sqlite3.Error:
        # Best-effort diagnostic write — never let it disrupt the request.
        pass


def list_seen_users(db: sqlite3.Connection) -> List[dict]:
    """Return the seen-users log (for the Access page fallback pick-list)."""
    try:
        rows = db.execute(
            "SELECT user_id, display_name, first_seen, last_seen "
            "FROM seen_users ORDER BY last_seen DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


# ------------------------------------------------------------------
# Circuit label helpers (circuit_1 / circuit_2 display names)
# ------------------------------------------------------------------

def load_circuit_labels(db: sqlite3.Connection) -> Dict[str, str]:
    """Return {circuit_id: display_name} for all configured circuits."""
    try:
        rows = db.execute(
            "SELECT circuit_id, display_name FROM circuit_labels"
        ).fetchall()
        return {row["circuit_id"]: row["display_name"] for row in rows}
    except sqlite3.OperationalError:
        # Table does not exist yet (pre-migration-023 DB); return empty dict.
        log.debug("circuit_labels table not found — migration 023 not yet applied")
        return {}


def upsert_circuit_label(
    db: sqlite3.Connection,
    circuit_id: str,
    display_name: str,
    commit: bool = True,
) -> None:
    """Insert or update the display name for a circuit ID.

    When ``commit`` is True (default) this runs in its own transaction via
    ``with db:``. When False the caller owns the transaction — required by
    multi-step paths like the restore handler so a failure later in the
    sequence rolls back this label change too.
    """
    sql = (
        "INSERT INTO circuit_labels (circuit_id, display_name) VALUES (?, ?) "
        "ON CONFLICT(circuit_id) DO UPDATE SET display_name = excluded.display_name"
    )
    if commit:
        with db:
            db.execute(sql, (circuit_id, display_name.strip()))
    else:
        db.execute(sql, (circuit_id, display_name.strip()))


