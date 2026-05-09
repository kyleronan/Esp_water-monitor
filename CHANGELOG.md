# Changelog

## [0.2.0-rc2] — 2026-05-08

### Bug Fixes

#### Event-table deduplication (migration 021)

Three independent bugs combined to produce many duplicate event rows in the
database — visible in Quick Restore backups as 8–9 identical rows per event
with different `id` values and `created_at` timestamps spanning several days.

**Root causes:**

1. **Pre-fix code generated UUID4 ids.** Before the `uuid5` fix in
   `feature_extractor.py`, every re-processing of the same raw event produced
   a fresh random id. `INSERT OR REPLACE` was keyed on `id` (PRIMARY KEY)
   only — no `UNIQUE` constraint on `(circuit, start_ts)` — so every import
   created a new row.

2. **`event_exists_near()` was broken.** The historical importer calls this
   to skip already-imported events. The implementation used SQLite's
   `datetime()` function which returns `'YYYY-MM-DD HH:MM:SS'` (space
   separator), while stored `start_ts` values use ISO 8601 `'T'` separator.
   ASCII `'T'` (84) > `' '` (32), so the upper-bound string comparison
   always failed and every event was re-imported on every catch-up cycle.

3. **Migration 015 was one-shot.** It deduped correctly but only ran once.
   Quick Restore used `INSERT OR REPLACE` keyed on `id`, so pre-fix backups
   re-introduced duplicates on restore, and migration 015 never ran again.

**Fixes in this release:**

- **Migration 021** — normalizes all `events.start_ts` / `end_ts` to UTC
  ISO 8601 (`+00:00`), recomputes UUID5 `id` values against the new UTC
  timestamps (prevents future `fixture_id` loss on re-import), clears
  `cluster_id` on dedup survivors so `backfill_unmatched` re-matches them,
  removes duplicate rows (keeps `MAX(rowid)` — newest insert), drops the
  superseded `idx_events_circuit_ts` index, and creates
  `UNIQUE INDEX idx_events_circuit_start_unique ON events (circuit, start_ts)`.
  Same UTC normalization applied to `hourly_volume.hour_ts`. Entire migration
  is wrapped in a transaction for atomicity.

- **`event_exists_near()`** — rewritten to compare in Unix epoch seconds via
  `CAST(strftime('%s', start_ts) AS INTEGER) BETWEEN lo AND hi`. This is
  robust against separator mismatch, mixed timezone offsets, and microsecond
  precision differences. Added `AND start_ts IS NOT NULL` guard.

- **`extract_features()`** — normalizes `event.start_ts` / `end_ts` to UTC
  before storing and before computing the UUID5 id. Same event expressed
  in any timezone now always produces the same id and the same stored string.

- **Startup dedup** — `dedup_events()` called in `orchestrator.py` after
  migrations as a safety net for any legacy data that slipped through. No-op
  on clean databases.

- **Quick Restore** — after importing events, `normalize_events_utc()` runs
  first (order matters), then `dedup_events()`. Export query now uses
  `ORDER BY rowid ASC` so the newest row for each `(circuit, start_ts)` is
  last in the JSON array and wins on `INSERT OR REPLACE`.

- **Test suite** — 12 new tests in `test_event_dedup.py` covering dedup
  semantics, UNIQUE constraint, `event_exists_near` correctness (including
  the regression test that fails on the old code), DST offset mismatch,
  `normalize_events_utc`, and the migration 021 end-to-end path.

#### Additional bug fixes (codebase audit)

- **`leak_test_scheduler.py` — `dir()` guard always True / missing column**
  (`BUG-01`): `'schedule' not in dir()` evaluates against the object's
  *attributes*, not local variables, so it was always `True` — the duration
  block never ran. `schedule["duration_minutes"]` then caused a `KeyError`
  because the `leak_test_schedules` table has no such column. Fixed: removed
  the bogus guard, fetch duration from the HA firmware entity instead, and
  initialize `result_str = "unknown"` before the poll loop so a timeout log
  message can't fail with `UnboundLocalError`.

- **`event_detector.py` — silent event loss on full queue** (`BUG-02`):
  `asyncio.create_task(queue.put(ev))` silently blocks (and leaks a task)
  when the queue is full, dropping the event with no log. Changed to
  `queue.put_nowait()` with an explicit `QueueFull` warning log.

- **`presence_watcher.py` — false away-mode at startup** (`BUG-03`):
  Python's `all([])` returns `True`, so the watcher enabled away mode at
  startup before HA had delivered any entity states. Added a guard: skip
  evaluation if no entity states are known yet.

- **`routers/backup.py` — partial restore wipes data permanently** (`BUG-04`):
  The Quick Restore loop deleted tables then re-inserted rows with only one
  `db.commit()` at the end. A failure mid-loop left the database in an
  inconsistent state with no rollback path. Wrapped the entire loop in
  `with db:` (atomic transaction).

- **`fixture_publisher.py` — SQLite thread-safety violation** (`BUG-05`):
  paho-mqtt's `_on_connect` callback runs on paho's background thread but
  called `_publish_all_confirmed_sync()` directly, reading `self._db` from
  the wrong thread. Moved the call to
  `loop.call_soon_threadsafe(_publish_all_confirmed_sync)` so it runs on the
  asyncio event loop thread that owns the connection.

- **`feature_extractor.py` — `hour_ts` format inconsistency** (`BUG-06`):
  `hour_ts` was stored via `.isoformat()` on a UTC-aware datetime, producing
  `'2026-05-03T17:00:00+00:00'`. All DB queries use SQLite's
  `strftime('%Y-%m-%dT%H:00:00', …)` which produces no timezone suffix,
  causing lexicographic comparisons to fail. Changed storage to
  `strftime('%Y-%m-%dT%H:00:00')`. Migration 021's `hourly_volume.hour_ts`
  normalization pass updated to use the same no-suffix format for consistency.

- **`routers/settings.py` — event loop blocked during prune** (`BUG-07`):
  `orch.data_pruner.prune_now()` runs synchronous SQLite `DELETE` statements
  that can take several seconds on large tables, blocking the asyncio event
  loop. Moved to `await loop.run_in_executor(None, prune_now)`.

- **`routers/setup.py` — OOM risk + unatomic restore** (`BUG-08`):
  `await file.read()` had no size limit — a multi-GB upload would exhaust
  memory. Added the same 50 MB cap as the main backup restore endpoint.
  The restore loop was also unprotected (same partial-failure risk as
  BUG-04). Wrapped in `with db:`. Added `normalize_events_utc()` +
  `dedup_events()` after events import (same as the main restore path).

#### Second-tier bug fixes and suspicious-pattern cleanup

- **`orchestrator.py` — zero sensitivity thresholds silently reset to preset**
  (`BUG-09`): `_get_sensitivity` used `row[x] or preset[x]` to merge DB
  values with preset defaults. `0.0` is falsy, so a user-set threshold of
  `0.0` (valid — disables a trigger) always reverted to the preset. Changed
  to `value if value is not None else preset[key]`.

- **`event_detector.py` — pressure recalculation skipped for zero-baseline
  systems** (`BUG-12`): `if ev.pre_event_pressure_psi` is falsy when baseline
  is 0.0, so min/delta pressure were never recomputed for unpressurised
  systems. Changed to `is not None`.

- **`main.py` — leaked DB connection after migrations** (`BUG-13`): `lifespan`
  opens a connection, runs migrations, then discards it without closing. The
  abandoned handle held a shared lock and prevented WAL checkpointing.
  Wrapped in `try/finally: _db.close()`.

- **`data_pruner.py` — stale daily summaries never recomputed** (`BUG-14`):
  `ds.computed_at < date(e.start_ts, '+1 day')` compares a full ISO timestamp
  string against a plain `YYYY-MM-DD` string. `'T' (84) > '-' (32)` in ASCII,
  so same-day rows were never considered stale. Wrapped `computed_at` in
  `date()` for a clean date-vs-date comparison.

- **`ha_client.py` — WebSocket recv loops hang forever on network stall**
  (`BUG-15`): `_subscribe_state_changed` and `ws_request` used unbounded
  `while True: await ws.recv()` loops with no timeout. A network stall during
  connection setup or a one-shot query would block the event loop indefinitely.
  Added `asyncio.wait_for(ws.recv(), timeout=15/30)` with descriptive
  `TimeoutError` messages.

- **`historical_importer.py` — import hangs on full event queue** (`BUG-16`):
  `await self._event_queue.put(raw)` blocks forever when the queue is full
  (identical pattern to BUG-02). Changed to `put_nowait` with a `QueueFull`
  warning; dropped events are re-attempted on the next catch-up cycle.

- **`feature_extractor.py` — anomaly alert branch permanently dead code**
  (`BUG-17`): `features.get("anomaly_score")` was always `None/absent` because
  nothing set it — the alert check after `_cluster_event` never fired.
  `_cluster_event` now derives `anomaly_score = 1.0 - match_confidence` from
  the cluster match result and stores it in `features`. Events rejected by
  type-gate or excluded from training do not set the score (they are not
  anomalous — they are simply not yet matched).

- **`training_manager.py` — full recalibration leaves stale confirmed
  centroids** (`BUG-19`): `trigger_full_recalibration` cleared events and
  volume data but not `fixture_clusters`. The in-memory DBSTREAM was reset by
  `start_calibration → reset_circuit`, leaving confirmed centroid rows in the
  DB with no in-memory counterpart. New events were then type-gate-rejected
  instead of matched. Added `("fixture_clusters", "circuit")` to the deletion
  list so the DB and engine state are always consistent after a full reset.

- **`routers/settings.py` — non-numeric retention form input raises 500**
  (`BUG-20`): bare `int(form.get("events_retain_years", 1))` raises
  `ValueError` on any non-numeric or empty submission. Added a local `_int()`
  helper that returns the default on conversion failure.

- **`database.py` — `update_home_profile` SQL injection via unvalidated keys**
  (`SUSP-02`): column names were interpolated directly into SQL from `kwargs`
  with no allowlist. Added `_HOME_PROFILE_COLUMNS` frozenset; unknown keys
  raise `ValueError` before reaching the DB layer.

- **`device_discovery.py` — deprecated `datetime.utcnow()`** (`SUSP-06`):
  replaced two `datetime.utcnow().isoformat()` calls with timezone-aware
  `datetime.now(timezone.utc).isoformat()`.

- **`routers/setup.py` — form circuit/role written to DB without validation**
  (`SUSP-07`): any form field containing `__` was split into `circuit__role`
  and written directly to `circuit_entity_map`. Added validation against
  `ROLE_PATTERNS` (the authoritative allowlist from `device_discovery.py`);
  unknown circuit or role pairs are logged and skipped.

- **`main.py` — `import re` on every request** (`SUSP-09`): `import re`
  was inside the middleware function body, re-executed on every HTTP request.
  Moved to module level (`import re as _re`).

---

## [0.2.0-rc1] — 2026-05-08

### New Features

#### Phase 2.1 — Labelling state + clean-start guarantee

- **Three-stage training lifecycle** — calibration now ends in a
  ``labelling`` review window (was: auto-promotion straight to ``live``).
  After training, the user confirms detected clusters on the Fixtures page,
  then explicitly clicks **Activate fixtures** to transition to ``live``.
  This was the missing state from the original training-manager design,
  deferred from Phase 1 and now reinstated. Anomaly detection is held off
  until activation so the live phase doesn't run against unconfirmed
  clusters.
- **Auto-activation safety net** — if a circuit sits in ``labelling`` for
  more than 7 days without user review (``LABELLING_AUTO_TIMEOUT_DAYS``),
  ``_check_progress`` auto-promotes it to ``live`` and sends an HA
  notification. Prevents the system being stuck waiting for the user to
  come back.
- **Clean-start guarantee** — ``start_calibration`` now deletes orphan
  clusters (``fixture_clusters`` rows with ``fixture_id IS NULL``) from
  any prior calibration cycle and calls ``cluster_engine.reset_circuit()``
  to flush the in-memory DBSTREAM and StandardScaler. Confirmed clusters
  (user-labelled fixtures) survive recalibration.
- **Recalibrate from labelling** — ``start_calibration`` accepts
  ``labelling`` as a valid source state, so a user reviewing clusters who
  doesn't like what they see can trigger recalibration directly without
  resetting to idle first.
- **Fixtures page UI** — per-circuit "Awaiting review" pill, a callout
  card with the **Activate fixtures** button, and adapted banner copy
  ("confirm below, then activate the circuit") when any circuit is in
  labelling. Settings page badge/pill render in amber for labelling.
- **Banner-count fix (carried into this release)** — the unreviewed-clusters
  banner now only counts clusters whose grid is actually rendered, fixing
  the contradiction where calibrating circuits with latent clusters showed
  "*N* clusters need review" with nothing visible to review.
- **Test suite** — 10 new tests in ``test_training_state.py`` covering
  orphan clearing, confirmed-cluster preservation, calibrating →
  labelling → live transitions, no-op activate from wrong states,
  auto-timeout firing at 8 days and not firing at 6 days,
  ``percent_complete=100`` for labelling, HA sensor publishing, and
  recalibrating from labelling. Total suite is now 33 tests.

#### Phase 2.1 — Type-aware fixture matching (Commits 1–4 complete)

- **Per-fixture-type variance profiles** — `fixtures.py` gains
  `FIXTURE_VARIANCE_PROFILES` and `FIXTURE_MATCH_THRESHOLDS` for all 23
  fixture types. Deterministic fixtures (toilet, ice maker, refrigerator)
  have tight thresholds (0.5–0.7); user-driven fixtures (shower, taps) use
  loose thresholds (1.8–2.8) with duration and volume as "float" features
  that are zeroed from the distance calculation; programme-driven appliances
  (washing machine, dishwasher) are loosest (2.5–3.0).
- **Type-aware match gate** — once a cluster is confirmed as a fixture type
  the engine uses per-type weighted Euclidean distance (anchor features
  amplified, float features ignored) against the stored centroid. Events that
  exceed the per-type threshold are rejected with reason
  ``'type_gate_rejected'`` and their ``cluster_id`` stays NULL so
  ``backfill_unmatched`` can retry them if the threshold is later relaxed.
  Unconfirmed clusters keep the existing global-threshold behaviour.
- **Live cache invalidation** — confirming or deleting a cluster on the
  Fixtures page immediately updates the in-memory type cache via
  ``notify_fixture_confirmed`` / ``notify_fixture_removed``. No restart
  required. The cache is also rebuilt from DB on every ``rebuild_from_db``
  call as a drift guard.
- **Rebuild-mapping tightened** — ``_rebuild_id_map_from_centroids`` now
  uses the per-type threshold (not 2× global) as the acceptance bound when
  re-attaching a river center to a confirmed DB cluster after a rebuild.
- **``events.match_rejection_reason``** — new TEXT column recording why an
  event has ``cluster_id IS NULL``: ``'features_missing'``,
  ``'no_centers'``, or ``'type_gate_rejected'``. Added via idempotent
  ``ALTER TABLE`` migration so existing databases are upgraded automatically
  on first start.
- **Schema bugfix** — ``fixture_clusters.centroid`` and
  ``.feature_std`` gained ``DEFAULT '{}'`` so the intermediate INSERT in
  ``_upsert_cluster`` no longer violates the NOT NULL constraint on fresh
  databases.
- **Test suite** — 17 unit tests in ``water_monitor/tests/`` covering the
  weighted-distance helper, type cache lifecycle, gate acceptance/rejection
  for toilet and shower, unconfirmed regression guard, schema-drift guard,
  multi-circuit isolation, and fail-open behaviour on corrupt centroids. Run
  with ``pytest water_monitor/tests/``. Requires ``pytest`` from
  ``requirements-dev.txt`` (not installed in the Docker image).

#### Phase 2.1 — Fixture Identification (Stages 1–2 complete)

- **Online clustering engine** — `cluster_engine.py` runs per-circuit
  `river.DBSTREAM` + `StandardScaler` (online, density-based, no fixed K).
  Every new water event is matched to a cluster immediately as it arrives.
  Startup replays the last 60 days of matched events to reconstruct
  in-memory state without pickling (see ADR 008).
- **9-feature event vectors** — `avg_flow_lpm`, `peak_flow_lpm`,
  `duration_seconds`, `volume_litres`, `pressure_delta_psi`,
  `has_pressure_transient`, `flow_variability`, `hour_sin`, `hour_cos`.
  Time-of-day is sin/cos encoded so midnight and 11 pm are adjacent in
  feature space.
- **Sequence context** — each event records `seconds_since_prev_event` and
  `prev_cluster_id`; the previous event gets `seconds_to_next_event` filled
  retroactively. Groundwork for Stage 3 cooccurrence boost.
- **Cluster confidence progression** — three levels persisted on
  `fixture_clusters.confidence_level`: preliminary (< 50 events), learning
  (50–200), confirmed (200+ or user-locked). See ADR 009.
- **Heuristic type suggestion** — `suggest_fixture_type` runs at event 1
  and every 10 events per cluster, updating `suggested_type` and
  `suggested_confidence`.
- **Fixtures page** — shows all clusters grouped by circuit with confidence
  pills, avg stats (unit-converted), and a confirm/name flow that creates a
  `fixtures` row and back-fills `events.fixture_id`. "Re-run clustering"
  rebuilds DBSTREAM state from the last 60 days.
- **Settings unit conversion for ESP device entities** — flow threshold and
  pressure threshold entities now display and accept values in the user's
  chosen units (gal/min, bar, etc.) and convert back to L/min / PSI before
  sending to HA/ESP.
- **Duplicate event prevention** — events use a deterministic
  `uuid5(circuit/start_ts)` ID so the same event can never be inserted twice.
  Migration 015 removes any existing duplicates on first run.
- **Migration 016** — adds `idx_events_fixture_id` and `idx_fixtures_circuit`
  indexes for Phase 2 query paths.

#### Design refresh

- Full visual refresh across all 7 pages (Dashboard, Device, History,
  Fixtures, Settings, Backup, Setup) — OKLCH colour tokens, consistent
  card/pill/button components, Settings sidebar navigation.

---

## [0.1.2] — 2026-05-03

### Removed
- **Water Budget & Cost** — Removed entirely. HA's built-in `utility_meter` integration
  provides a richer and better-maintained implementation. The three database columns
  (`monthly_budget_litres`, `water_cost_per_litre`, `water_cost_currency`) are dropped
  automatically by migration 012 on first start.

### Additional Bug Fixes (post-release)

- **`UnboundLocalError: cannot access local variable 'orch_ref'`** — In
  `IngressTemplates.TemplateResponse`, `orch_ref` was only assigned inside the
  CSRF cache-refresh block; when the cache was still warm the variable was never
  set and the unit context injection crashed. Fixed by hoisting the lookup before
  the cache block so it is always defined.
- **Backup restore failed with removed budget columns** — The setup wizard's own
  restore loop used raw column names from the backup JSON without schema validation.
  Old backups containing `monthly_budget_litres` / `water_cost_per_litre` /
  `water_cost_currency` caused an `OperationalError` because migration 012 had
  already dropped those columns. Fixed by applying the same `PRAGMA table_info`
  column-filtering used in the main backup restore route.
- **Units reverted to L/min after backup restore** — `_init_display_units` ran at
  startup (before the restore) and correctly detected `gal/min + psi`; the subsequent
  backup restore overwrote `home_profile.flow_unit` with the backup's schema-default
  `L/min`. Fixed by re-running `_init_display_units` (and invalidating the unit cache)
  immediately after the restore completes — the skip condition preserves any
  explicitly-chosen units from the backup while re-detecting when only defaults were
  stored.

### New Features

#### Display Unit Conversion
- **Unit selection step in setup wizard** — Step 4 of the setup wizard asks the user
  to confirm or change the auto-detected units before proceeding to home details.
  Applies to both new setup and backup restore paths. Units can still be changed
  at any time in Settings → Display Units.
- **Configurable flow and pressure units** — Dashboard, history, device page, leak test
  results, and HA push notifications all respect the user's chosen units
  - Flow rate and volume: L/min · gal/min · ft³/min · m³/min
  - Pressure: PSI · bar · kPa
- **HA unit system auto-detection** — On first run, queries `/api/config` and selects
  sensible defaults (US installs get gal/min + PSI; metric installs get L/min + bar).
  User overrides are preserved across restarts.
- **Re-detect from HA button** — Settings page lets users re-query HA at any time.
- **30-second result cache** — `load_unit_context` caches the DB read; invalidated
  immediately on save so the next page load reflects the change without delay.

#### Historical Event Import
- **Startup backfill** — On every restart, reconstructs events missed while the addon
  was offline (up to 10 days of HA recorder history).
- **Periodic catch-up** — Runs every 30 minutes to fill gaps from brief restarts.
- **Dual detection strategy** — `flow_pulse_onset` transitions as primary signal with
  15-second gap bridging; `flow_rate > 0.05 L/min` sustained readings as fallback.
- **Pressure fidelity** — Prefers `pressure_main` (2 Hz, 1.375 s smoothing) over
  `pressure_main_avg` (25 s smoothing) for historical pressure data.
- **Duplicate prevention** — Checks ±30 seconds before inserting; safe to re-run.
- **Concurrent query limit** — At most 2 simultaneous HA WebSocket history queries.

#### Cross-Circuit Valve State
- **`other_valve_open` event field** — Live state of every other circuit's valve is
  captured when each event starts. Main-circuit events with `other_valve_open = true`
  are almost certainly irrigation bleed-through — a direct binary feature for Phase 2.

#### Firmware Changes (`esp-water-shut-off-3_4.yaml`)
- **`pressure_main` / `pressure_irrigation` changed from `diagnostic` to normal** —
  HA recorder now logs them at 2 Hz. Used for historical import pressure fidelity and
  for the live dashboard reading (12× more responsive than the 25 s averaged sensor).

### Bug Fixes

#### Unit Conversion
- Hourly chart bars, total, and tooltip not multiplied by `vol_factor`
- `device.html` status strip and threshold labels were hardcoded `PSI` / `L/min`
- Leak test `baseline_psi` / `final_psi` not multiplied by `pressure_factor`
- Event table used fixed `%.2f`/`%.1f` format strings; now respects `*_decimals`
- Sensitivity threshold label hardcoded as `(PSI)`
- Alert push notifications always used PSI and L/min regardless of user units
- Auto-detect skip condition only checked `flow_unit`; manual `pressure_unit` change
  was overwritten on restart — now checks both columns against schema defaults
- Fallback pressure unit for unrecognised HA volume units was `psi`; changed to `bar`
- Pressure dropdown showed raw key `"psi"` instead of friendly label `"PSI"`
- Budget section not fully removed from dashboard template and route
- `load_unit_context` hit the DB on every 2-second poll (60+ reads/min)
- `_init_display_units` silently did nothing on fresh install (UPDATE on missing row)

#### Core
- Timezone mismatch in daily volume baseline (`period_ts` used UTC vs local midnight)
- Full recalibration left stale `daily_summary`, `import_state`, `volume_snapshots`
- Away mode calibration timer used per-loop 1-minute extension instead of true elapsed
  away duration; offline time was not accounted for
- PresenceWatcher created unbounded concurrent tasks on rapid entity state changes
- Data pruner `_wait_until_3am` was not DST-safe; now recalculates in 1-hour chunks
- Historical importer closed an ongoing event at `history[-1]` (could equal start);
  now closes at the original `query_end`
- Backup restore interpolated JSON column names directly into SQL (injection risk)
- No file size limit before parsing uploaded backup JSON (OOM risk on large files)
- `X-Ingress-Path` header embedded unescaped in setup-redirect HTML
- `start_calibration` rejected `"calibrating"` as starting state (broke backup restore)
- Pruner training fence used `> calibration_ends_at` protecting all pre-install history;
  now uses `BETWEEN started_at AND calibration_ends_at`
- Leak test could poll forever if firmware changed a terminal result string; now has a
  hard timeout with a clear warning log
- Daily volume showed 0 — baseline was set to `current_ha_value`, making delta zero
- Dashboard `| round()` Jinja2 filter crashed on string values from HA states
- `Unexpected token '&'` JS error on all pages — `tojson` filter returned plain `str`
  instead of `Markup`, allowing autoescape to corrupt JSON inside `<script>` blocks
- 500 on dashboard after setup — inline `from ..database` used double-dot path

### Performance and Reliability
- Long-event memory — pressure and flow readings downsampled after 120 s (keep every
  5th); a 2-hour irrigation run drops from ~290 k to ~35 k samples
- `get_write_lock()` exported from `database.py` for multi-step async write sequences

---

## [0.1.1] — 2026-05-03

### Bug Fixes
- Valve button shows correct state after live poll
- Leak test countdown uses actual configured duration
- Leak test results correctly written to database
- Abort leak test clears `is_running()` state immediately
- Valve shows correct Open/Close button during leak test
- Duplicate abort button removed from dashboard
- Settings page 500 — `SENSITIVITY_PRESETS` imported inside function
- Fault/trickle reset buttons had missing device prefix
- Setup redirect broken behind HA ingress proxy
- Firmware router import from wrong module
- `training_manager` None-guard missing on startup
- `asyncio.gather()` indentation error in orchestrator
- Volume display showed "0 LL" (unit suffix applied twice)
- Setup wizard `from __future__` mid-file SyntaxError

### New Features

#### Dashboard
- Live valve state polling every 5 seconds without page reload
- Safety fault confirmation dialog before valve open override
- Away mode banner

#### Leak Test
- Countdown timer with settle phase display
- Learned quiet hour scheduling from 60-day usage history
- Manual triggers start immediately; single-click abort

#### History Page
- Daily usage bar chart with anomaly overlay
- Range buttons: 30d / 6m / 1y / All / This month / This year / Year-over-year
- Custom date range filter

#### Settings
- Away / Vacation mode with calibration timer extension
- HA presence linking (person, device_tracker, input_boolean, alarm_control_panel)
- Mobile push notifications to `notify.mobile_app_*` services
- Data retention sliders (events and hourly volumes)
- Automatic weekly backup to `/share/water_monitor_backups/`
- Recalibration backup prompt

#### Backup and Restore
- Three-tier backup: Quick Restore JSON · History Archive SQLite · Full ZIP
- Setup wizard restore from backup (step 0)

#### Alerts
- AlertManager wires alert toggles to HA notifications and mobile push

#### Security
- CSRF protection on all state-changing form POSTs

#### ESP Firmware
- Fault reason text sensors with human-readable strings
- Six fault trigger types covered

---

## [0.1.0] — Initial release

- ESP32-S3 water monitor integration for Home Assistant
- Dual-circuit support (main + irrigation motorised ball valves)
- Real-time pressure and flow monitoring via ESPHome entities
- Setup wizard with automatic device and entity discovery
- Valve open/close control with live state updates
- Micro leak test scheduling and manual trigger
- Safety fault detection and reset
- Training/calibration state machine
- Basic dashboard with circuit status cards
- Settings page with sensitivity presets and alert configuration
- History page with leak test results and event log
