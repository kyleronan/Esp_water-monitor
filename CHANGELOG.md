# Changelog

## [Unreleased] — 0.2.0-dev

### New Features

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
