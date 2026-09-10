-- ==========================================================================
-- WATER MONITOR - BASE SCHEMA (fresh-install DDL)
--
-- Run by database._create_schema() through sqlite3.Connection.executescript().
--
-- LOCATION IS LOAD-BEARING: this file MUST stay at water_monitor/app/
-- schema.sql. The add-on Dockerfile does `COPY app /opt/water_monitor/app`
-- and the repo has no .dockerignore, so a copy kept anywhere else passes
-- every local test and is simply missing from the container image.
--
-- -- executescript() semantics ---------------------------------------------
-- sqlite3's executescript() issues a COMMIT *before* it runs the script and
-- disregards the connection's isolation_level. Consequences:
--   * never call _create_schema() from inside an open transaction - any
--     uncommitted work on that connection gets committed out from under you;
--   * this file must NOT contain its own BEGIN; / COMMIT; - the caller
--     commits, and a stray BEGIN here would leave a transaction dangling.
--
-- -- this file is NOT "the schema" -----------------------------------------
-- It is the FRESH-INSTALL DDL and it is order-coupled to db_migrations.py:
--
--   1. database.init_db() -> _create_schema()   runs this file, and must
--      succeed against an EXISTING database at any upgradeable version
--   2. db_migrations.run_migrations()           ALTERs, backfills, indexes
--
-- Every CREATE TABLE here is IF NOT EXISTS, so step 1 is a near no-op on an
-- existing database: it does NOT add columns to a table that already exists.
-- Any statement here that references a column a migration adds therefore
-- runs against the OLD table and fails the boot. Hence the rule that
-- tests/test_schema_ddl_drift.py enforces mechanically:
--
--     AN INDEX ON A MIGRATION-ADDED COLUMN BELONGS IN THE MIGRATION,
--     NOT IN THIS FILE.
--
-- This file also DELIBERATELY OMITS objects that migrations create (partial
-- indexes, late tables, indexes on late columns). Do not "consolidate" them
-- back in - that is what breaks upgrades. The omissions are enumerated, with
-- a reason each, in that test's _MIGRATION_ONLY_OBJECTS allowlist.
--
-- The reverse also needs care: an index added ONLY here reaches fresh
-- installs and never reaches an existing one, because that database was built
-- by an older copy of this file and CREATE INDEX IF NOT EXISTS is not
-- retroactive. So every index here is pinned in the same test's _DDL_INDEXES;
-- to add one, put it in a migration first (that is what backfills existing
-- installs) and then list the name there.
-- ==========================================================================
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

-- NOTE: the legacy `csrf_tokens` table was dropped by migration 20260902.
-- It was created here and never touched again: the only other mention of the
-- name in the whole repo was EXPORT_EXCLUDED_TABLES (routers/backup.py), which
-- DELETEs from it inside a try/except that already treats "table absent in this
-- schema version" as normal. Nothing issued or verified a token against it —
-- CSRF has been stateless HMAC double-submit off csrf_server_secret.

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
    pulses_per_litre    REAL DEFAULT 396.0,
    -- Winterized (migration 20260809, dev46 46h). The circuit is deliberately
    -- drained for the season: its meter and transducer sit downstream of the
    -- shutoff and drain with it, so ~0 psi for months is EXPECTED, and
    -- drain-down day would otherwise look like a catastrophic pressure event.
    -- While set, the event detector skips the circuit, supply-regime sampling
    -- and the pump-regime nightly exclude it, and baselines / leak-test
    -- scheduling pause. Clearing it restores everything (with a brief grace so
    -- spring re-pressurisation does not alarm either).
    winterized          INTEGER DEFAULT 0
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
    reseed_in_progress  TEXT,
    -- dev46 (46k, migration 20260810) — max-age backstop for the verdict
    -- stamp. The stamp is only as good as the list of inputs baked into it;
    -- if one is ever missed, stale verdicts would persist silently. Forcing a
    -- full unstamped pass when this is old bounds that to days, not forever.
    last_full_reclassify_at TIMESTAMP
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

-- NOTE: idx_type_signatures_circuit (circuit) was dropped by migration
-- 20260902 — the PRIMARY KEY (circuit, fixture_type) already provides an
-- index whose leading column is `circuit`, so it served no query the PK
-- index did not. It was also created by migration 20260530; that copy is
-- gone too (a DDL-only deletion would have been a silent no-op).

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
    closed_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- dev50 (migration 20260814): event_id has no FK, so a reprocess left it
    -- dangling. Marked like overlap_audit — provenance, never deleted.
    stale_reason  TEXT,
    stale_at      TEXT
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
-- NOTE: idx_jobs_id (id) was dropped by migration 20260902. `id INTEGER
-- PRIMARY KEY AUTOINCREMENT` is a rowid alias, so lookups by id already use
-- the table's own rowid B-tree; the extra index only cost a write per job row.

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

-- NOTE: idx_clusters_circuit (circuit) was dropped by migration 20260902 —
-- PRIMARY KEY (circuit, id) already indexes `circuit` as its leading column.
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

-- NOTE: `cluster_sequences` (a Phase 2.2 placeholder that stayed empty) was
-- dropped by migration 20260902. It had exactly one mention in the repo — this
-- CREATE — so nothing read it, wrote it, exported it or restored it.

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
    -- (flow_onset_delay_seconds was here; dropped by migration 20260902 —
    --  never written by any code path, never read by any query.)
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
    -- dev56 — PINNED VERDICT (migration 20260818). A verdict reached from
    -- CROSS-event evidence that single-event re-derives cannot reproduce and
    -- therefore must not overturn. verdict_pin names the family ('overlap_duplicate'
    -- today; dev57 may add a user family), verdict_pin_veff is the effective
    -- volume the pin prescribes (0 for a full duplicate, the uncovered remainder
    -- for a partial one), verdict_pin_set_at when it was (re)derived. Preserved
    -- across re-imports like the bookkeeping columns; released by the overlap
    -- guard when the covering events disappear, or by deleting the row.
    verdict_pin                 TEXT,
    verdict_pin_veff            REAL,
    verdict_pin_set_at          TEXT,
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
    -- dev48 (migration 20260812): the rate a draw runs at once it is running.
    -- NULL, not 0 — a draw with no stored waveform has no plateau, and the
    -- model must read that as missing rather than as "ran at zero".
    flow_plateau_lpm                 REAL,
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
    -- User training-exclusion (migration 20260809, dev46 46f). "Keep my label,
    -- but do not train on this event." Set when a label is TRUE while the
    -- event's FEATURES describe a composite draw — without it the only way to
    -- keep such an event out of training was to lie about its label. DISTINCT
    -- from training_quarantine_reason above and NOT lifted by review: review
    -- is what SETS this one. Pool readers filter on it alongside quarantine.
    training_excluded_by_user        INTEGER DEFAULT 0,
    -- Per-channel signature spans (migration 20260809, dev46 46i). The real
    -- captured duration of each signature channel, so the event modal can draw
    -- an honest per-channel time axis instead of a proportional overlay.
    -- Forward-only: legacy rows stay NULL and keep the proportional render,
    -- because their true spans are unknowable (annotate-don't-modify).
    flow_sig_span_s                  REAL,
    pressure_sig_span_s              REAL,
    -- dev46 (46k, migration 20260810). WHICH inputs produced this row's
    -- stored verdict: classifier code version + this circuit's rule bands +
    -- its label pool. The boot reclassify scans only rows whose stamp differs
    -- from the current one, so a verdict that provably cannot have changed is
    -- not re-derived. NULL = never stamped = always a candidate, which is why
    -- no backfill is needed and why any doubt costs correctness nothing.
    -- Cleared explicitly by anything that rewrites a classification input on
    -- one event (see invalidate_verdict_stamps).
    verdict_stamp                    TEXT,
    -- dev50 auto-split memo (migration 20260814). What the hourly over-merge job
    -- DECIDED about this event, so a decision is made once instead of on every
    -- pass and every restart. The job's checked-set was in-memory only; harmless
    -- while it scanned 24 h, but it now scans the whole HA recorder window, and a
    -- re-check costs an HA history fetch. Outcome is one of 'split' /
    -- 'clean' / 'untrustworthy' / 'out_of_retention' — also a plain audit trail of
    -- why an event was left alone. NULL = never evaluated = a candidate, so no
    -- backfill is needed; a change to the split gate ships with a migration that
    -- clears these so every event is reconsidered once.
    split_evaluated_at               TEXT,
    split_evaluation_outcome         TEXT,
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
-- NOTE: idx_events_verdict_pin (dev56) is deliberately NOT here for the same
-- reason — verdict_pin arrives with migration 20260818, which creates it.
-- NOTE: idx_events_circuit_cluster (circuit, cluster_id) and idx_events_fixture
-- (fixture_id) follow the same convention and are created by migration
-- 20260902. cluster_id is the clustering layer's join key and had no index at
-- all; fixture_id is a declared FK to fixtures(id), so without a backing index
-- every fixture delete or merge made SQLite scan all of events.

-- ==========================================================================
-- HOURLY VOLUME (pre-aggregated for fast chart queries)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS hourly_volume (
    circuit         TEXT NOT NULL,
    hour_ts         TIMESTAMP NOT NULL,
    volume_litres   REAL DEFAULT 0,
    PRIMARY KEY (circuit, hour_ts)
);

-- NOTE: idx_hourly_volume_circuit_ts (circuit, hour_ts) was dropped by
-- migration 20260902 — it duplicated PRIMARY KEY (circuit, hour_ts) column
-- for column, and hourly_volume takes a write on every event.

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
    -- (50.0 for ESP captures — the firmware's 20 ms waveform_capture tick,
    -- NOT the 200 Hz ADC read loop; NULL for the event-driven software series,
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
    action          TEXT NOT NULL,   -- 'flagged' | 'reverted'
    -- dev50 (migration 20260814): event_id has no FK, so a reprocess left it
    -- dangling. Marked like overlap_audit — provenance, never deleted.
    stale_reason    TEXT,
    stale_at        TEXT
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

-- NOTE: idx_daily_summary_circuit_day (circuit, day) was dropped by migration
-- 20260902 — an exact duplicate of PRIMARY KEY (circuit, day).

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

-- dev51 (migration 20260815): the model referee's frozen benchmark and its
-- decision record.
--   referee_benchmark       — event ids of the pinned benchmark, per circuit.
--                             Imported once through Dev Tools; the ids never
--                             enter the repo (they are a record of when this
--                             household used water). Empty = the referee's
--                             benchmark leg abstains.
--   referee_benchmark_meta  — ONE row per circuit: the import's hash and the
--                             number of ids it asked for, so a retrain can say
--                             "165 requested, 151 matched the pool" and the
--                             ledger has one unambiguous hash to quote.
--   retrain_ledger          — every referee decision, durably. The jobs table
--                             prunes after two days, so "a run of rejections"
--                             was invisible; this is the record.
-- dev53 (migration 20260816): the add-on pins its own benchmark. A re-pin is
-- written as role='pending' and only becomes active at the next promotion, so
-- the leg is never dark; both sets are reserved from training meanwhile.
-- referee_benchmark_meta grows the provenance (source 'import'|'auto',
-- pinned_from_n = human labels at pin time), the pending slot, and the
-- Water Use prompt's dismissal record.
CREATE TABLE IF NOT EXISTS referee_benchmark (
    circuit      TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    source_hash  TEXT NOT NULL,
    imported_at  TEXT NOT NULL,
    role         TEXT NOT NULL DEFAULT 'active',
    PRIMARY KEY (circuit, event_id, role)
);
CREATE TABLE IF NOT EXISTS referee_benchmark_meta (
    circuit               TEXT PRIMARY KEY,
    source_hash           TEXT NOT NULL,
    requested_n           INTEGER NOT NULL,
    imported_at           TEXT NOT NULL,
    source                TEXT NOT NULL DEFAULT 'import',
    pinned_from_n         INTEGER,
    repin_dismissed_at    TEXT,
    repin_dismissed_keys  TEXT,
    pending_hash          TEXT,
    pending_pinned_at     TEXT,
    pending_pinned_from_n INTEGER,
    pending_trigger       TEXT,
    pending_reason        TEXT
);
CREATE TABLE IF NOT EXISTS retrain_ledger (
    id               INTEGER PRIMARY KEY,
    circuit          TEXT NOT NULL,
    decided_at       TEXT NOT NULL,
    trigger          TEXT NOT NULL,
    status           TEXT NOT NULL,
    swap             INTEGER NOT NULL,
    challenger_hash  TEXT,
    champion_hash    TEXT,
    reason           TEXT,
    benchmark_hash   TEXT,
    benchmark_n      INTEGER,
    detail_json      TEXT
);
CREATE INDEX IF NOT EXISTS idx_retrain_ledger_circuit_decided
    ON retrain_ledger (circuit, decided_at);

-- dev46 (46k) — verdict-stamp invalidation, enforced at the TABLE.
--
-- The stamp says "this row's stored verdict was derived from these inputs".
-- A global stamp cannot see a per-row edit, so any write that changes an
-- input the classifier reads must release that row. Doing it here rather
-- than at each call site is deliberate: those writes live in at least nine
-- places across feature_extractor and database, and a missed one produces a
-- SILENTLY stale verdict — the exact failure this stamp exists to prevent.
-- A trigger cannot be forgotten by a future writer.
--
-- Watched columns are classifier INPUTS only. matched_fixture_type,
-- matched_via, cycle_group_id, match_rejection_reason and verdict_stamp are
-- deliberately absent: they are the pass's OUTPUTS, and watching them would
-- have the pass erase the stamp it had just written.
CREATE TRIGGER IF NOT EXISTS trg_events_verdict_stamp_invalidate
AFTER UPDATE OF
    cycle_pulse_count, excluded_from_training, volume_litres,
    volume_litres_effective, duration_seconds, peak_flow_lpm,
    active_flow_segment_count, has_pressure_transient,
    flow_signature_json, pressure_signature_json,
    is_pressure_restoration_phantom, is_cross_talk, is_low_flow_dribble,
    user_ignored, training_excluded_by_user
ON events
FOR EACH ROW WHEN NEW.verdict_stamp IS NOT NULL
BEGIN
    UPDATE events SET verdict_stamp = NULL WHERE id = NEW.id;
END;


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
-- FIXTURE HEALTH (dev47 47i)
-- Classification adapts; these do not. A degrading fixture keeps producing
-- correctly-labelled events, so accuracy metrics stay clean while the home
-- loses water — the label schema conflates WHICH fixture with IS IT HEALTHY.
-- The baseline is frozen over an explicit window and moves only by an explicit
-- unlock with a reason code; a reference that drifted with the data would let
-- a failing fixture quietly redefine normal, which is the whole failure mode.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS fixture_baseline (
    circuit         TEXT NOT NULL,
    fixture_type    TEXT NOT NULL,
    baseline_hash   TEXT,
    pinned_at       TEXT,
    window_start    TEXT,
    window_end      TEXT,
    n_events        INTEGER,
    stats_json      TEXT,
    locked          INTEGER NOT NULL DEFAULT 1,
    unlocked_reason TEXT,               -- fixture_replaced | fixture_repaired | false_alarm
    unlocked_at     TEXT,
    PRIMARY KEY (circuit, fixture_type)
);

-- Append-only nightly observations: the evidence a health card is built from.
CREATE TABLE IF NOT EXISTS fixture_health_stat (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit      TEXT NOT NULL,
    fixture_type TEXT NOT NULL,
    as_of_day    TEXT NOT NULL,
    stats_json   TEXT,
    UNIQUE (circuit, fixture_type, as_of_day)
);

CREATE TABLE IF NOT EXISTS fixture_health_alert (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit      TEXT NOT NULL,
    fixture_type TEXT NOT NULL,
    signal       TEXT NOT NULL,
    opened_at    TEXT NOT NULL,
    resolved_at  TEXT,
    resolution   TEXT,
    detail_json  TEXT
);

CREATE INDEX IF NOT EXISTS idx_fixture_health_alert_open
    ON fixture_health_alert (circuit, fixture_type, resolved_at);
-- NOTE: idx_fixture_health_stat_day was dropped by migration 20260902 — it
-- duplicated the table's own UNIQUE (circuit, fixture_type, as_of_day)
-- constraint index column for column. It was also created by migration
-- 20260811; that copy is gone too.

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


-- ==========================================================================
-- EVENT INDEXES THAT USED TO COME FROM A PRE-BASELINE MIGRATION
-- Created by migrations 20260526 and 20260535, which are now below
-- _BASELINE_VERSION and no longer shipped. Every column they cover is declared
-- above, and the baseline guarantees any accepted database already has them, so
-- these are safe here — unlike idx_events_wf_claim / idx_events_verdict_pin,
-- which still come from a migration (see _ensure_wf_claim_index).
-- ==========================================================================
CREATE INDEX IF NOT EXISTS idx_events_degraded
    ON events (circuit, start_ts) WHERE degraded_supply = 1;
CREATE INDEX IF NOT EXISTS idx_events_training_labels
    ON events (circuit, user_fixture_type, excluded_from_training);
CREATE INDEX IF NOT EXISTS idx_events_unlabelled_reclassify
    ON events (circuit, user_fixture_type, matched_fixture_type);
