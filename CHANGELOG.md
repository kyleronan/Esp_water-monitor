# Changelog

## [0.3.1] — Unreleased

Smarter fixture labeling, anomaly surfacing, and a round of volume-accuracy
guardrails driven by a full audit of the add-on's stored events against raw
Home Assistant history. (Shipped incrementally as dev1–dev17 — dev7/dev8
landed without a version bump; per-build details are in git history.)

### New Features

- **Pump-aware supply profile — Phase 1 plumbing (dev21)** — homes pressurized
  by a pump (city booster or well pump) violate the static-supply assumptions
  behind the pressure detectors (seen live 2026-07-19: a booster-pump install
  turned static ~43 PSI into a 53–65 PSI recharge sawtooth and an event storm).
  The setup/settings supply question gains "City water with a booster pump"
  (`city_pump`), which also gets the well home's +7 calibration days. New
  resolver `config.pump_mode_effective` (per-circuit override → supply answer →
  banner-confirmed detection; a well IS a pump home, defaulting to the
  switch+tank profile at read time). Answer provenance (`supply_type_set_at`)
  is stamped only on real changes so future pump alerts can distinguish a
  post-feature answer from a migrated one, and moving off a pump supply
  disarms/unconfirms pump state. Detection, detector gating, and the
  pump-assisted leak tests build on this in later phases (migration 20260558).
- **Embedded-fixture (composite) detection** — sustained events (long showers)
  are scanned for draws superimposed on their baseline (a mid-shower toilet
  flush) and annotated "Contains: toilet ×2 (~9 L)" in the event modal.
  Annotate-only: parent volume and label never change. The classifier can also
  emit `other` for clearly multi-fixture events instead of leaving them blank,
  and the k-NN matcher now uses time-of-day (`app/composite_detector.py`;
  migration 20260548).
- **Dishwasher-cycle detector** — a chain of ≥3 small gentle fills within
  30 min is recognized as a dishwasher run, fixing concurrent
  washer+dishwasher overlaps that left fills unlabelled
  (`detect_dishwasher_cycles`).
- **Self-healing event hygiene, on by default** — the background pass that
  re-imports garbled events now also shrinks inflated single events (mostly-
  idle spans), reaches the `sparse_envelope` events it was built for, and is
  atomic: a failed re-import restores the deleted events verbatim, so a
  failure can never lose water. Anomaly-flagged events are never touched
  (migration 20260549).
- **Irrigation zone-switch cross-talk cleanup** — water-hammer transients from
  irrigation zone valves were logging tiny phantom main-circuit events. A
  pressure-swing-ratio discriminator (irrigation swing ÷ main swing ≥ 1.3) in
  the historical importer flags and zeroes them, with a frozen ≤1.5 L cap so a
  larger draw is never zeroed. Auditable via the new `cross_talk_audit` table
  and revertible (migration 20260550).
- **Anomalies finally surface** — flagged events were invisible before. Now:
  History `?filter=anomaly` view, reason badges (high use / estimated / large
  draw — review), an "Unusual event" modal section with Mark reviewed, a
  dashboard "Unusual events" card that never ages out, and `triggered_alert`
  is stamped when a notification actually goes out.
- **Volume guardrails** — a degraded event's envelope estimate is capped at
  max(1.5 × flow-integral, 2 L) (audit found a 2.86× inflation), and a
  would-be phantom carrying ≥10 L is kept and flagged for review instead of
  silently zeroed. Migration 20260551 restores already-zeroed large draws
  through the volume ledger.
- **Fingerprint label propagator** — a new match tier between rules and the
  k-NN: an event whose un-normalized stored waveform closely matches a
  user-labeled event inherits that label (self-calibrating threshold,
  user-labels-only library so no drift; `app/fingerprint_matcher.py`;
  migration 20260552). Applies only to events ≥ 2 L effective (dev20): the
  tier was validated on coarse-meter data whose sub-2 L draws never became
  events, and the first post-pulse-meter review overturned every fingerprint
  stamp — all were micro-draws. Below the floor the tier abstains and such
  events don't join the match library.
- **Two-option anomaly review** — "Mark reviewed" now records intent:
  ✓ Normal use (participates in future baseline refits) vs ❓ Don't recognize
  it (held out of every future refit so an unidentified draw can't widen
  "normal"). A later relabel supersedes the verdict (migration 20260553).
- **Rising-pressure phantom detector** — short flow bursts driven by a
  city-pressure rise were counted as real water. New per-event flow↔pressure
  correlation separates them (real demand pulls pressure down; a rise phantom
  tracks the ramp); fires only under 1 L / 120 s so it stays under the
  leak-detection suspect bar. Includes a one-time backfill from HA recorder
  history (migration 20260554).
- **History filter bar** — one bar covering every column: date, duration,
  avg flow, ΔP, and volume min/max sliders, plus fixture and note dropdowns.
  All filters run in SQL so the recency limit counts matching rows and old
  matches never vanish. The Shape column was removed; the sparkline moved
  into the Volume cell.
- **Toilet physics veto** — a toilet label from any tier (rule / fingerprint /
  k-NN / cluster inheritance) is dropped to "Other" when the event can't be a
  single cistern refill: volume floor, era-based flush cap from
  `home_profile.build_year` (EPA 1994 / 1982 / pre-1982 tiers, new Settings
  toggle), peak-flow floor, segment count. Applied at classify, backfill,
  History, and rollups (migration 20260555).
- **256-point signatures** — the stored flow/pressure shape signatures widen
  64 → 256 points so long events stop collapsing into rectangles (at 64 pts a
  45-min shower got one point per ~42 s; valve ramps and fill tapers vanished).
  Cluster per-dim weights rescaled so total shape weight is unchanged; every
  consumer already resamples on load, and migration 20260556 regenerates
  stored signatures from each event's hi-res waveform envelope where finer
  (measured ~4 s for 3.3k events; no-waveform rows keep their shorter sigs).
  Also adds `tools/validate_edge_signatures.py` — an offline LOO k-NN A/B
  harness for the proposed fixed-time onset/offset "edge signature" matcher
  block (validate-first; production wiring is a separate step).
- **Edge signatures in the k-NN matcher** — every event now stores fixed-TIME
  onset/offset shape vectors (32 cells × 1 s each end, absolute grid,
  zero-padded; `events.onset/offset_signature_json`). Unlike the proportional
  signatures, these align valve ramps and fill tapers across event durations —
  a new first matcher tier (`match_source='active_flow_edges'`) uses them when
  both the query and enough labelled neighbours carry them, falling back to
  the existing tiers otherwise. Config chosen by a two-round LOO sweep over
  344 labelled events (32×1 s beat 16-cell and 0.5 s-cell variants; the wider
  32 s window is what catches toilet fill-tapers): toilet recall 0.783→0.870,
  shower 0.878→0.927, tap 0.429→0.486. ESP-captured events recompute edges
  from the firmware array under the same quality gate as the signatures;
  migration 20260557 backfills historical events from their waveform
  envelopes (coarse envelopes smear onto the grid — the configuration the
  study validated). `tools/eval_knn_classifier.py` mirrors the new tier.

### Bug Fixes

- **Label-save lock contention** — rapid labelling stacked full background
  reclassifies and returned 500 (`database is locked`). Reclassifies are now
  debounced and yield the write lock between batches; label + cycle-mates
  still apply instantly.
- **Locked-baseline relabel gate** — a relabel no longer fires a full-history
  reclassify once the baseline is locked (`is_baseline_locked`); it still
  applies instantly and propagates to cycle-mates.
- **Importer no longer fabricates long events from noise** — a long
  pressure-dip envelope containing only trivial, individually-viable flow
  blips is no longer stitched into one bogus multi-minute event. The gate only
  removes empty spans between self-sufficient fragments, so it can never
  orphan flow or mask a leak.
- **Unusual-events Review list truncated** — the `?filter=anomaly` and
  `?filter=degraded` views filtered in Python after fetching the newest 100
  events, hiding older flagged events (card said 95, list showed 2). Both now
  filter in SQL. Reviewed anomalies also get a neutral "✓ Reviewed" pill
  instead of shouting "⚠ Unusual" forever.
- **Re-run Setup unlock didn't unlock** — the unlock endpoint cleared one
  setup-complete flag but the wizard guard read the other; it now clears
  both. Also fixed a FK crash (`fixtures` deleted before `events.fixture_id`
  was nulled) when re-running discovery on a DB with labelled events.

## [firmware 3.13.0] — 2026-07-04

Both circuits migrate from `pulse_counter` (windowed counting, quantized to
1.67 L/min steps on the new K≈72 oval-gear main meter) to `pulse_meter`
(edge-period timing): smooth traces and a 0.083 L/min low-flow floor.

- **Volume from pulse totals**, not rate integration (integration would
  overcount each event tail); NVS-backed accumulator, one-time meter reset on
  first 3.13 boot (HA long-term statistics survive).
- **Onset and pressure-drop gates re-keyed to last-pulse age** — onset latency
  improves ~41 ms → ~16–32 ms; the water-hammer false-positive window stays
  ~1 s instead of widening to the 10 s pulse timeout.
- **Valve-seal checks moved to a pulse-age-based 1 s interval block.**
- **Fixed pre-existing timer bugs** — trickle and seal accumulators counted
  publish ticks as seconds (a "90 min" dial fired at ~22.5 min; the fast seal
  check was inert). All wall-clock now; trickle default lowered 90 → 30 min to
  match the effective behavior users saw.
- Instant burst gains a 2-consecutive-sample guard; 100 µs PULSE-mode
  internal filter (bounce immunity under period timing).

## [0.3.0] — 2026-06-27

Role-based access. The panel is no longer all-or-nothing:

- **Three tiers** — **admin** (every HA administrator, auto-detected): full
  control; **operator** (granted on the new Settings → Access page): read-only
  plus main-valve open/close for emergencies; **viewer** (any other HA user):
  read-only Dashboard, History, Water Use, Device status.
- Roles derive from the Supervisor's `X-Remote-User-Id` ingress header
  (trustworthy because the add-on is ingress-only). Default-deny; enforcement
  is server-side at a single middleware chokepoint.
- Admin list cached last-known-good and refreshed every 10 min, so an HA
  hiccup can't downgrade a real admin; optional `bootstrap_admin_user_id`
  add-on option is the lockout escape hatch. Migration 20260547.

## [0.2.2] — 2026-06-21

### New Features

- **Flow calibration helper** — Settings → Flow Meter → "Calibrate…" derives
  the meter's true pulses/litre from a bucket or municipal-meter reference
  run, with method-aware sample gating and an editable suggested value
  (`routers/calibration.py`, `calibration_math.py`).
- **Runtime per-circuit flow-meter PPL** — pulses-per-litre is now an
  NVS-backed HA number entity per circuit (replaces compile-time
  `flow_k_factor`), so any meter works without a reflash. The add-on reads it
  as the single source of truth and derives each circuit's low-flow floor;
  changing it triggers a non-destructive re-baseline. Migration 20260546.
- **Degraded-supply guard** — events captured during pulsing municipal supply
  (chaotic paddlewheel readings) are detected via pressure-band
  autocorrelation, flagged `degraded_supply`, given an envelope-smoothed
  effective volume, and excluded from clustering. Surfaced in History, the
  dashboard, and a rate-limited notification.
- **Per-circuit valve type (2-port / 3-port)** — 3-port (drain-capable)
  circuits automatically skip the micro leak test, with the reason shown in
  the UI. Migration 20260527.

### Security

- **Per-session CSRF tokens** — replaced the shared per-process token with
  stateless HMAC double-submit; setup-wizard POSTs are no longer exempt.
- **Firmware credentials restored** — API encryption, OTA, fallback-AP and
  web-server auth re-enabled from `secrets.yaml`; a new release-check script
  fails the release if any are missing.

### Bug Fixes

- Degraded-supply autocorrelation normalisation corrected to a proper
  overlap-weighted Pearson correlation.
- Migration 20260526's `hourly_volume` rebuild made exception-safe via a
  temp-table swap with rollback.
- Catch-up importer no longer orphans events longer than the check interval —
  the checkpoint holds at a still-active flow start until the event closes.
- No-flow pressure phantoms that settled below the recovery line no longer
  block a circuit for hours (`_maybe_close_settled_noflow` closes them after
  60 s of settled, zero-flow state).

### Performance

- Nightly prune, waveform purge, and leak-test hour learning moved off the
  asyncio event loop via `run_in_executor`.

## [0.2.1] — 2026-05-24

Refinement of the 0.2.x fixture-identification line.

### New Features

- **ESP-side waveform capture** — firmware 3.7.0+ captures per-event flow and
  pressure waveforms on-device and streams them to the add-on; the feature
  extractor uses them when quality allows, falling back per-group to software
  values (migration 031).
- **Event detail modal** on History — relabel, ignore/restore, technical
  details, embedded waveform chart.
- **Auto dark mode** — follows `prefers-color-scheme` across all pages and
  charts; dark palette derived from the OKLCH light tokens.
- **Merge clusters UI** on the Fixtures page; **Basic/Advanced split** in
  Settings; **Re-run setup wizard** (Settings → Advanced) with backup prompt;
  toast notifications; accessible valve confirm modal; History summary strip;
  irrigation enable/disable toggle; historical-import option in setup.

### Changes

- Firmware v3.7+: waveform capture, 4 Hz flow publishes, valve-close
  re-trigger storm fixed.
- Clustering: 32-point pressure signature added (60 dims); weights retuned.
- Event detection: settled resting pressure drives the baseline;
  pressure-recovery END for pulsed events; default
  `pressure_drop_event_psi` 2.0 → 1.2 (migration 027).
- Propagation-delay scan reworked timestamp-based; leak-test scheduling uses
  local timezone; flow-stop lag reduced (2 s window, EMA removed).

### Bug Fixes

- Persistence: `insert_event` upserts, restore is atomic,
  `UNIQUE (circuit, start_ts)` enforced; importer no longer truncates active
  events; migration 028 collapses legacy duplicates.
- Pressure-surge and overnight-oscillation phantoms rejected.
- CSRF extended to PATCH/PUT/DELETE; `/setup` mutations locked post-setup.
- Volume baselines consistently UTC (fixes wrong totals on non-UTC servers);
  circuit-ID normalisation survives Supervisor restarts (migrations 023/024);
  assorted UI/XSS/race fixes.

### Hardware & Docs

- PCB v1.2a with KiCad project, Gerbers, BOM; complete hardware build guide
  with bring-up checklist and photo gallery; stylesheet cache-busting.

## [0.2.0] — 2026-05-10

- **Dashboard "Past 7 days" volume** replaces the calendar-week total
  (rolling window, no Monday drop to zero); volume-baseline key format
  unified to naive UTC.
- Add-on icon/logo and web UI favicon added.
- **Hardening pass across the add-on** (two audits): transactional writes in
  discovery/backup/restore, SQL-injection column allowlists, input-validation
  guards on settings/setup forms, UTC baseline fixes, fail-fast on migration
  errors, connection-leak and thread-safety fixes, weekly-volume SQL date fix.
- **Firmware 3.6.0**: false motor-fault on concurrent open/close fixed;
  pressure history buffer corrected to a true 5 s at 2 Hz; log and comment
  cleanups.

## [0.2.0-rc2] — 2026-05-08

- **Event-table deduplication (migration 021)** — three stacked bugs (random
  UUID4 ids, an `event_exists_near()` string-comparison bug that re-imported
  every event each cycle, and a one-shot dedup migration defeated by
  restores) produced 8–9 duplicate rows per event. Migration 021 normalizes
  timestamps to UTC ISO 8601, recomputes UUID5 ids, dedups, and adds
  `UNIQUE (circuit, start_ts)`; restore paths re-normalize and re-dedup.
- **Codebase audit fixes (BUG-01…BUG-20, SUSP-XX)** — a sweep covering silent
  event loss on full queues, false away-mode at startup, unatomic restores,
  SQLite thread-safety, WebSocket timeout guards, zero-threshold handling,
  dead anomaly-alert code, stale-summary comparisons, SQL-injection
  allowlists, and input-validation guards.

## [0.2.0-rc1] — 2026-05-08

Phase 2.1 — fixture identification:

- **Online clustering engine** (`cluster_engine.py`): per-circuit
  `river.DBSTREAM` + `StandardScaler`; 9-feature event vectors with sin/cos
  time-of-day; sequence context fields; confidence progression
  (preliminary / learning / confirmed).
- **Three-stage training lifecycle** — calibration now ends in a `labelling`
  review window; the user confirms clusters then explicitly activates, with a
  7-day auto-activation safety net. Clean-start guarantee clears orphan
  clusters; recalibration is possible directly from labelling.
- **Type-aware matching** — per-fixture-type variance profiles and thresholds
  for 23 types; type-gated events record `match_rejection_reason` so
  `backfill_unmatched` can retry them.
- **Fixtures page** — clusters grouped by circuit with confidence pills,
  confirm/name flow, re-run clustering.
- Deterministic `uuid5(circuit/start_ts)` event ids (migration 015 dedups);
  indexes for Phase 2 query paths (migration 016).
- Full visual refresh across all 7 pages (OKLCH tokens, consistent
  components, Settings sidebar).

## [0.1.2] — 2026-05-03

### Removed

- **Water Budget & Cost** — HA's `utility_meter` integration does it better;
  migration 012 drops the columns.

### New Features

- **Display unit conversion** — configurable flow/volume (L/min, gal/min,
  ft³/min, m³/min) and pressure (PSI, bar, kPa) units across the whole UI and
  notifications, with HA auto-detection, a setup-wizard confirmation step,
  and a re-detect button.
- **Historical event import** — startup backfill (up to 10 days) plus a
  30-minute periodic catch-up from HA recorder history, with duplicate
  prevention.
- **Cross-circuit valve state** — `other_valve_open` captured per event
  (irrigation bleed-through signal for Phase 2).
- Firmware 3.4: `pressure_main`/`pressure_irrigation` promoted from
  diagnostic so HA records them at 2 Hz.

### Bug Fixes

- A long tail of unit-conversion misses (charts, device page, leak tests,
  notifications, formatting) all now respect the user's chosen units.
- Core: UTC/local baseline mismatches, non-DST-safe pruner scheduling,
  restore-path SQL-injection and OOM guards, importer end-timestamp fix,
  away-mode timer accounting, and several template/JS crashes.
- Post-release: `orch_ref` UnboundLocalError, restores of pre-012 backups,
  and units reverting after a backup restore.

### Performance

- Long-event downsampling after 120 s (a 2-hour irrigation run drops from
  ~290 k to ~35 k samples).

## [0.1.1] — 2026-05-03

### New Features

- Dashboard: live valve-state polling, safety-fault confirm dialog, away-mode
  banner.
- Leak test: countdown with settle phase, learned quiet-hour scheduling,
  immediate manual trigger and abort.
- History: daily usage chart with anomaly overlay, range buttons, custom date
  filter.
- Settings: away/vacation mode, HA presence linking, mobile push, retention
  sliders, automatic weekly backups.
- Three-tier backup (Quick Restore JSON / History Archive / Full ZIP) with
  setup-wizard restore.
- AlertManager wiring, CSRF on state-changing POSTs, firmware fault-reason
  text sensors.

### Bug Fixes

- 13 fixes across valve state display, leak-test lifecycle, setup-wizard
  ingress redirects, unit-suffix doubling, and startup errors.

## [0.1.0] — Initial release

- ESP32-S3 water monitor integration for Home Assistant: dual-circuit
  (main + irrigation) motorised ball valves, real-time pressure and flow via
  ESPHome entities.
- Setup wizard with automatic device/entity discovery; valve control; micro
  leak test scheduling; safety fault detection and reset.
- Training/calibration state machine; dashboard, settings (sensitivity
  presets, alerts), and history pages.
