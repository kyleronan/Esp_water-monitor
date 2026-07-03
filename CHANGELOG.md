# Changelog

## [0.3.1-dev14] — 2026-07-03 — rising-pressure phantom detector + historical backfill

Short (3–50 s) flow bursts driven by a **city-pressure RISE** were being counted
as real water: climbing supply pressure pushes a slug through the turbine, and
the burst clears every existing detector (the phantom needs ≥ 120 s, dribble
needs flow < 1 L/min). Confirmed from raw HA history on the 07-02 14:01 event —
79 mL "real" while pressure climbed ~2 PSI and never dipped below rest.

The separator is the **flow↔pressure Pearson correlation** over the event:
real demand pulls pressure DOWN while flow runs (audited real draws −0.88 /
−0.24); a rise phantom's flow TRACKS the pressure ramp (+0.67). Validated over
the full 05-17→07-03 history against 551 labelled events — 0 specifically-
labelled real draws flagged at the shipped thresholds
(`tools/validate_rising_pressure_phantom.py` is the offline reference harness).

- **`events.flow_pressure_corr`** (migration 20260554): stored on every new
  event (index-binned pressure vs 1 Hz flow); recomputed from the firmware
  arrays when a quality-gated ESP waveform upgrades an event. Shown in the
  event modal under Technical details ("Flow–pressure corr.").
- **Detector** (`_detect_rising_pressure_phantom`): fires at corr ≥ 0.6 AND
  volume strictly < 1.0 L AND duration ≤ 120 s — all frozen; the volume cap
  keeps every zeroed event under detector_validation's `SUSPECT_ZERO_LITRES`
  leak bar, and corr = None (no pressure signal) can never fire. Verdict joins
  `_finalize_derived_verdicts` (priority phantom → rise → cross-talk →
  dribble), reusing `is_pressure_restoration_phantom` + the existing
  "👻 Not real use" presentation with distinct provenance
  `match_rejection_reason='rising_pressure_phantom'`. Leak-safe by
  construction: a leak is sustained flow + pressure DROP — the opposite sign.
- **Repair pass** `reprocess_rising_pressure_phantoms` (scan 3b of the
  exclusion reprocess): applies the verdict to stored corrs through the §2.5
  `apply_effective_volume` ledger chokepoint (hourly reversed, daily summaries
  recomputed). Manual Recompute carries the stored corr so the verdict
  survives (waveforms are gone at recompute time).
- **One-time backfill** (`rise_corr_backfill` worker): historical candidate
  events (short, small, un-labelled, un-flagged — the detector's exact scope;
  ~920 events ≈ 51 L on this home) get their correlation computed from HA
  recorder history (stored signatures were validated unusable — the [0,1]
  drop-clamp erases rises). Paced batches, fetch-error retry, and a
  `home_profile.rise_corr_backfill_done` stamp once a clean sweep finishes;
  uncomputable events stay counted.
- Flag-collision repair (Fix B) understands the new provenance; calibration
  exposes `RISE_PHANTOM_MIN_CORR` as a default WITHOUT fit bounds (frozen v1 —
  the nearest labelled real draw sat at +0.48, too close to hand a fit
  loosening rights).

## [0.3.1-dev13] — 2026-07-03 — two-option anomaly review: Normal use / Don't recognize it

"Mark reviewed" collapsed two opposite meanings — "that was normal" and
"I have no idea what that was" — into one bit, and an unrecognized draw
with a machine-matched label would still stretch the anomaly baseline's
idea of "normal" at the next retrain. Reviewing now records which one you
meant:

- **✓ Normal use** — confirmed legitimate (long shower, guests, filling
  something). Clears the dashboard card; participates in baseline refits
  as before.
- **❓ Don't recognize it** — you looked but couldn't place it. Clears the
  card too, but the event is **held out of every future anomaly-baseline
  refit** (`fit_usage_baselines` skips `review_verdict='unknown'`), so an
  unidentified draw can never widen a fixture envelope or the overall
  volume percentiles toward "normal". The row keeps an amber
  "❓ Reviewed — unrecognized" pill so it stays visible.
- A later **relabel clears the verdict** — identifying the draw supersedes
  "unknown" and readmits the event under its new label. Un-reviewing also
  wipes the verdict (a verdict can't outlive its review).
- **Migration 20260553**: `events.review_verdict` (TEXT, nullable). Events
  reviewed before verdicts existed stay NULL — their intent wasn't recorded
  and isn't invented; they keep the generic "✓ Reviewed — was unusual" pill
  and keep participating in refits (old behavior).
- `review_verdict` joins the user-intent columns preserved across
  re-imports/reprocess. PATCH API accepts `review_verdict`
  ('normal'/'unknown'/null); plain `user_reviewed` still works (legacy).
- The frozen-baseline invariant is untouched: verdicts change nothing live —
  'unknown' only gates which events the NEXT deliberate refit may learn from.
- Tests: 6 new in `test_anomaly_surfacing.py` (verdict semantics, relabel/
  un-review clearing, baseline hold-out both directions).
- **?filter=anomaly_unreviewed** — the dashboard card's Review link now
  opens exactly the events it counts (flagged AND awaiting review), so
  finished verdicts don't bury the open ones. The History banner toggles
  between "Only unreviewed" and "Show all unusual"; both variants keep the
  reach-past-the-recency-limit SQL semantics and the hide-not-real bypass.

## [0.3.1-dev12] — 2026-07-03 — fix: unusual-events Review list truncated by recency limit

- **Bug:** the dashboard "Unusual events" card counts every unreviewed
  flagged event (no time window), but History's `?filter=anomaly` view
  filtered in Python AFTER fetching only the newest 100 events — so any
  flagged event older than the 100th most recent row silently vanished
  from the very list meant to surface it (card said 95, Review showed 2).
  The `?filter=degraded` view had the same defect.
- **Fix:** both filters now live in `get_recent_events`' SQL WHERE
  (`flagged_only` / `degraded_only`), so the recency limit applies to
  *matching* events — the Review list shows the newest 100 flagged events
  regardless of how much unflagged use came after them. Date-range
  filtering composes with the flag filters unchanged.
- Tests: 3 regressions in `test_anomaly_surfacing.py` (flagged/degraded
  reach past the recency limit; date-range + flag AND).
- **Also fixed:** the History Note pill ignored review state — after "Mark
  reviewed" the row kept shouting "⚠ Unusual — high use" forever. Reviewed
  anomalies now render a neutral "✓ Reviewed — was unusual" pill (kept, not
  removed, so rows in the unusual-events view still say why they're there),
  and the mark-reviewed button swaps the pill in place without a reload.

## [0.3.1-dev11] — 2026-07-03 — fingerprint label propagator (waveform-match tier)

Phase 3 of the 2026-07 reality audit. An event's fully un-normalized stored
waveform — absolute-time flow + cumulative volume + pressure-drop-below-
baseline — acts as a fingerprint: when a new event's nearest neighbor among
the USER-labeled events is close enough, it inherits that label. Validated on
two backups of this home's data and re-verified with the shipped code:
**30% standalone coverage at 97% precision (LOO), dishwasher 81/81, and it
labels 63 of the 221 events the full production pipeline declined, at 94%.**

- **New `app/fingerprint_matcher.py`** (pure module): 64×4 s absolute-time
  grid over `event_waveforms` (flow_max + pressure_min + `pre_event_pressure_psi`);
  per-channel std scaling from the library itself; **self-calibrating
  threshold** = a percentile of the library's own NN-distance distribution
  (30th pctl; tightens to 15th below 100 labels — cold-start precision
  measured to dip otherwise). Guards: ≥10 labels overall, ≥5 per class;
  library = user-labeled events only (no fingerprint→fingerprint chaining, so
  no drift). CYCLE_ONLY types may inherit at the standard threshold — a
  whole-waveform match is dispositive, unlike the lone scalar k-NN vote that
  guard exists for (an extra tightening was tried: half the coverage, zero
  precision gain).
- **New match tier in both pipelines**, after rules/cycles, before the k-NN:
  the reclassify loop (fresh library per run) and the live `_cluster_event`
  path (TTL-cached library, invalidated on every label save). Stamped
  `matched_via='fingerprint'` via the standard chokepoint — auditable, and
  revertible like any auto label. Waveform-missing / tiny-library / any
  failure degrades silently to the k-NN. History shows a dedicated tooltip
  on fingerprint-matched fixtures.
- **Migration 20260552**: `home_profile.fingerprint_labeling_enabled`
  (default ON — shipped eval-gated). No backfill; the next reclassify pass
  spreads existing labels.
- Uses numpy (already in the container via river/dtaidistance), lazily
  imported.
- Deferred (noted, not shipped): raising the training wizard's capture goal
  to 5 per fixture would arm this tier on day 1 of a fresh install (the
  wizard's checklist semantics are built around one-capture-per-fixture; a
  UX pass, not a constant tweak).
- Tests: `test_fingerprint_matcher.py` (13) — fingerprint construction,
  library/threshold policy incl. maturity tightening, min-class guard,
  tier precedence in reclassify (stamps / declines / toggle-off), cache
  invalidation. Full suite **952 passed / 1 skipped**.

## [0.3.1-dev10] — 2026-07-03 — volume guardrails: envelope cap + never silently zero a big draw

Phase 2 of the 2026-07 reality audit. The audit measured two volume-estimation
defects against 45M raw HA readings: `pulsing_supply_envelope` inflating
**2.86×** (336 L claimed for 2 L real), and `pressure_restoration_phantom`
silently zeroing a real **141 L** draw through a NULL-metrics hole (legacy
events skip the frozen no-flow guards entirely).

- **Envelope cap (2a).** A degraded event's envelope estimate is now capped at
  `max(1.5 × flow-integral, 2 L)` (falls back to raw volume; no evidence → the
  estimate stands). The clamp is audited in `degraded_diagnostic_json`
  (`envelope_cap_applied`, uncapped value, cap base) so it's explainable.
  Because anomaly scoring reads the capped effective volume, the 336 L artifact
  can no longer trip a false "high volume" alert either.
- **Suppression-averted guard (2b).** A would-be phantom carrying
  ≥ 10 L of measured volume is never zeroed: volume KEPT, event excluded from
  training, and flagged `suppression_averted` (score 1.0, never authorises
  shut-off) — it lands in the new Unusual-events view as "Large draw — review".
  A manual classification or a real label resolves the review (marker cleared,
  relabel also counts as reviewed). Leak-safety: keeping volume can never mask
  a leak (only zeroing could), and trickle detection is firmware, independent
  of stored events.
- **Migration 20260551** adds `events.phantom_suppression_averted` and
  re-examines already-zeroed phantoms: large ones get their measured volume
  restored **through `apply_effective_volume`** (hourly ledger reversed and
  re-applied) + flagged for review. User-classified phantoms are never touched.
  On this home's data that restores the 141 L draw from 2026-05-26.
- Updated the two tests that pinned the old silent-zeroing on large events
  (248.7 L / 1089 L) to pin the new keep-and-flag behavior, with small-volume
  variants preserving the original regressions verbatim.
- Tests: `test_volume_guardrails.py` (12) — cap math incl. the audit's worst
  cases, averted guard both sides of the threshold, scorer short-circuit,
  ledger-correct migration backfill (idempotent), relabel-resolves-review.
  Full suite 938 passed / 1 skipped.
- **Display honesty (audit Phase 4, folded in):** a zeroed not-real-use event
  (phantom / cross-talk / dribble) no longer renders a medium/large sparkline
  next to its "0 gal" number — the tier's peak-flow dimension (pressure-window
  noise on those events) is suppressed so they draw at trickle height (246
  events affected). An `unknown`-size event now renders at small height
  instead of full-height large.

## [0.3.1-dev9] — 2026-07-03 — anomalies finally surface (History filter, reason badges, triage)

The 2026-07 reality audit found that anomaly detection worked but nothing ever
reached the user: 91 flagged events, zero visible anywhere, `triggered_alert`
a dead column, and the live notification pointed at a History view that didn't
exist. This release is Phase 1 of the audit plan — surfacing, not detection
changes.

- **History `?filter=anomaly`** — mirrors the degraded filter; shows every
  flagged event and deliberately bypasses the "hide not-real-use" toggle (an
  anomaly on a zeroed event is exactly what must not stay hidden).
- **Reason badges** split the one internal `flagged` bit into what it means to
  a homeowner: **⚠ Unusual — high use** (trustworthy measurement, genuinely
  big), **⚠ Unusual (estimated)** (the flag fired on a pulsing-supply
  ESTIMATE — the number itself may be wrong), and **⚠ Large draw — review**
  (reserved for Phase 2's phantom guard keep-and-flag).
- **Event modal triage** — flagged events get an "Unusual event" section with
  a plain-language explanation per reason and a **Mark reviewed** button
  (admin-gated, PATCH `user_reviewed`), which clears it from the dashboard.
- **Dashboard "Unusual events" card** — count of `flagged AND NOT
  user_reviewed` per circuit with a Review link into the filter. No time
  window: an unreviewed anomaly cannot silently age out. Passive UI — push
  behavior and its cooldown are unchanged.
- **`triggered_alert` wired** (was dead since the column was added):
  `AlertManager.fire()` now reports dispatched-vs-suppressed and
  `alert_unusual_usage` stamps the event, best-effort, only when a
  notification actually went out — a real audit trail of what notified.
- The unusual-usage push message now points at the Unusual events filter
  instead of the bare History page.
- Tests: `test_anomaly_surfacing.py` (10) — review patch, dispatch/suppress
  return, stamp-on-fire (incl. critical shut-off bypass), dashboard count
  shape. Full suite 924 passed / 1 skipped.
- (Bookkeeping: dev7/dev8 shipped in commits e3a7116/dd46c63 without a
  config/CHANGELOG bump — this entry jumps dev6 → dev9 to match.)

## [0.3.1-dev6] — 2026-06-28 — auto-hygiene reaches the inflated events it was built for

dev4 added a SHRINK path to the background hygiene pass to fix "the two-blips-welded-into-
20-minutes case" — but it never actually reached those events. They are flagged
`sparse_envelope` ("brief use, long idle tail"), which sets `excluded_from_training=1`, and
the candidate query screened out **every** excluded event (`excluded_from_training = 0`).
So the exact inflated singles the shrink logic targets were filtered out before the gate
ever saw them. This release fixes that.

- **The candidate filter now admits `sparse_envelope` events** while still benching the real
  artifact verdicts that `excluded_from_training = 0` used to cover. The clause became
  `(excluded_from_training = 0 OR match_rejection_reason = 'sparse_envelope')` plus explicit
  `is_pressure_restoration_phantom = 0 AND is_cross_talk = 0 AND is_low_flow_dribble = 0`.
- **Volume- and leak-neutral.** `sparse_envelope` keeps the event's raw volume (it only
  excludes from training), so admitting it carries no zeroed volume into reprocess. The three
  verdicts that *do* zero volume are explicitly re-gated — and an event that reads
  `sparse_envelope` yet *also* carries one of those flags is still benched.
- **Every prior guard intact.** User-labelled / classified / ignored, anomaly-`flagged`, and
  softener-brine events are still never candidates; the dry-run SPLIT/SHRINK gate still
  decides; reprocess is still exception-atomic (delete + verbatim restore through the volume
  ledger). A shrunk event becomes a short, mostly-active draw, so it can never re-qualify as
  `sparse_envelope` → no churn.
- **Adversarial-reviewed (ship, no holes).** The headline reassurance: running-toilet / trickle
  leak detection is a **firmware** binary-sensor, independent of the stored-event envelope this
  pass rewrites — so re-importing a historical event cannot silence a leak alarm. (Deferred,
  non-leak-safety: a durable HA-history outage mid-reprocess can re-attempt the same event on
  later passes — log-spam only, no water lost; optionally dampen later with a per-event
  `last_auto_split_attempt_ts` grace window.)

No schema change. 904 tests pass.

## [0.3.1-dev5] — 2026-06-28 — irrigation zone-switch cross-talk cleanup

When irrigation runs, every zone-valve switch fires a water-hammer transient through
the shared supply manifold that briefly spins the **main** flow impeller — logging a
flurry of tiny `~Tap` / `Other` main events (1–11 s, ≤~0.8 L) that aren't real
household water. A deep dive on the raw HA history (`water_history.db`) found a clean,
physically-grounded discriminator and this release acts on it.

- **The discriminator is the pressure-swing ratio `PiΔ / PmΔ`** — the irrigation-circuit
  pressure swing over the event window ÷ the main-circuit swing. A zone-switch transient
  originates on the irrigation branch, so its swing **dominates** (ratio ≥ 1.3, typically
  1.7–3.2); a genuine main draw pulls the **main** branch down (ratio ≤ ~1.1). Validated
  across **all 12 irrigation days** in the May–Jun 2026 history: it flags the zone-switch
  bursts and keeps every real draw, including a 123 L/25-min dawn shower and toilets that
  overlapped irrigation.
- **Reconciliation lives in the historical importer, not the live detector.** The live
  per-circuit detector only sees its own circuit's pressure (and `other_valve_open` is
  NULL on ~92 % of main events here — flaky valve entity). The importer can pull the
  irrigation pressure sensor from HA history, so a periodic pass (plus a one-time backfill
  over the recorder-retention window) computes `PiΔ`/`PmΔ` from one history batch and flags
  qualifying events. Self-healing + idempotent (candidates exclude already-flagged rows).
- **Safety-first gating.** A flag requires irrigation concurrently flowing **and** a hard,
  structurally-frozen volume cap (`≤ 1.5 L` — a larger draw is never zeroed, whatever the
  ratio) **and** a real main swing (`≥ 2.0 PSI`) **and** the ratio ≥ 1.3. The empirical
  danger band (small volume, real ΔP, ratio 1.0–1.6) holds exactly one event in 12 days.
- **Durable + calibration-safe.** Flagged events reuse `is_cross_talk` (volume zeroed,
  excluded from training, hidden from History) but carry a distinct
  `match_rejection_reason = "irrigation_cross_talk"` so they (a) never pollute the
  long-no-flow cross-talk calibration, and (b) survive a main-only reprocess —
  `_finalize_derived_verdicts` preserves the verdict instead of clearing it.
- **Auditable + reversible.** New `cross_talk_audit` table (migration **20260550**)
  records the pre-zero volume + pressure-swing evidence for every action before zeroing;
  `revert_irrigation_cross_talk` restores a false positive. The same migration enables
  `home_profile.hide_cross_talk_events` so these drop out of History.

No event-table schema change (audit is a new side table). 902 tests pass.

## [0.3.1-dev4] — 2026-06-28 — self-healing event hygiene (atomic, on by default)

Toward "you should never have to hit Reprocess by hand." The add-on already had a
background pass (`auto_split_merged_events`) that re-imports garbled events — it was
opt-in/off and only caught multi-draw merges. This release makes it safe to run for
real and turns it on.

- **Reprocess is now atomic.** It deletes a window's machine events then re-imports
  them from history; if the re-import fails (e.g. HA history briefly unavailable), the
  deleted events are now **restored verbatim** (re-inserted + hourly-volume re-applied),
  so a failure can never lose water. `import_range(strict=True)` surfaces a fetch
  failure instead of silently returning 0; `delete_events_in_range(capture=True)`
  snapshots the rows; `restore_deleted_events` rolls them back. Benefits the manual
  Reprocess button too.
- **The hygiene pass now also fixes inflated single events** — not just multi-draw
  merges. An event whose stored span is mostly idle (e.g. the two-blips-welded-into-
  20-minutes case) is re-imported and **shrunk** to its real use. The dry-run gate
  accepts a split (≥2 draws) *or* a shrink (1 draw ≥1 min shorter than stored); a clean
  event reconstructs to itself and is left alone (no churn).
- **On by default** (`home_profile.auto_split_enabled` now defaults 1; migration
  **20260549** backfills existing installs). Still dry-run-gated, label-preserving,
  loop-safe (re-imported draws are single-segment so they don't re-trigger), rate-
  limited, and runs in the periodic maturity pass. You can still turn it off in
  Settings.
- **Leak-safety guard (from an adversarial review):** the pass **never touches an
  anomaly-flagged event**. Splitting/shrinking a flagged event could strip its leak
  signal (an unusual long event becomes several individually-normal fragments) even
  though volume is preserved — so a flagged event is left exactly as-is; only
  unremarkable, un-flagged garbled events are auto-cleaned. (Known limitation,
  documented: reprocess is exception-atomic, not crash-atomic — a hard kill in the
  sub-second delete→import window isn't yet journalled; future work.)

No event-table schema change. 885 tests pass.

## [0.3.1-dev3] — 2026-06-28 — dishwasher-cycle detection + locked-baseline relabel gate

### A relabel no longer re-walks history once the baseline is locked

The classifier is **fit-once-at-activation, then hard-locked** — after the training/
labelling window closes, the reference is frozen and live events match against it.
But a label change was still firing a full-history background reclassify every time,
long after that window had closed. Fixed: the label-triggered reclassify is now
**skipped when the baseline is locked** (`training_state = 'live'` with no active
recalibration). A relabel still applies instantly and still propagates to the event's
own cycle-mates; it just no longer re-scans the whole history. The full reclassify
runs where it should — during the training window, at startup, and on explicit
recalibration. (New `is_baseline_locked`, mirroring the anomaly-shutoff state gate.)

### Dishwasher-cycle detection

The original complaint: a dishwasher running concurrent with the washer left its
gentle fills unlabelled (`(none)`). Root cause — the per-event dishwasher rule needs
`cycle_pulse_count ≥ 3`, but gentle fills fail the "fill-shaped" gate inside that
counter and sit at cpc=2, so they never qualify.

- **New dishwasher-cycle detector** (`detect_dishwasher_cycles`, companion to the
  washer detector): a chain of ≥3 small, gentle fills (≤3.5 L, peak ≤3.6, not
  flush-shaped) within 30 min of each other is a dishwasher run. It bypasses the
  fragile per-event shape gate and rolls the fills up under one History row. Wired
  into reclassify and the live path, after the washer/softener detectors (so a
  laundry top-off or brine pulse can't be re-read as a dishwasher). The live path's
  trailing retro-scan now re-stamps a cycle's earlier fills once the 3rd arrives, so a
  freshly-completing cycle doesn't leave its first fills sitting unlabelled.
- **Result** (audit backup, confidence-weighted harness): dishwasher recall
  **66% → 91%**, overall weighted type accuracy **0.73 → 0.83**, with no other class
  regressing (tap held; one labelled-tap reclassified to dishwasher). The 7:08 PM
  washer+dishwasher overlap now separates correctly.

### Importer no longer fabricates long events from noise

A long pressure-dip envelope that contained almost no flow was stitching unrelated
trivial blips into one bogus multi-minute event (observed: two ~0.3 L blips 20 min
apart fused into one 20-minute "event" with a flat, meaningless waveform). The importer
now **drops a pressure-dip bridge only when it is provably noise**: (a) long (≥5 min),
(b) trivial flow inside (<2 L), (c) there IS real flow inside, and (d) **every** flow
fragment inside already survives on its own (≥ the min-duration floor). So the gate only
ever removes the empty *span between* fragments that stand alone — it can never orphan,
lose, or under-count flow. A pure-pressure dip with no flow is kept (handled by the
phantom guard); a sub-floor fragment keeps its bridge. **Leak-safe by construction:** a
real draw or slow/intermittent leak is flow, so it's always a surviving flow period,
never only an empty envelope. (Hardened after a 3-lens adversarial leak-safety review;
volume integration is left-hold step, not trapezoidal, so a no-flow gap can't invent
phantom volume.)

No schema change. 880 tests pass (+6 dishwasher detector, +5 lock gate, +6 importer gate).

## [0.3.1-dev2] — 2026-06-28 — fix label-save lock contention

Follow-up to dev1: labelling/reprocessing an event while a background reclassify
was running still returned **500 (`database is locked`)**. Root cause: every label
save fired a full ~10s background reclassify, and rapid labelling stacked them so
the write lock was held almost continuously — the next save's own write timed out.

- **Debounced the background reclassify** — a burst of label saves now coalesces
  into a single reclassify ~8s after the last save, so active labelling no longer
  competes with a long-running reclassify (the user's label + cycle-mates are still
  applied synchronously and shown immediately).
- **The reclassify now yields the write lock** — a brief pause after each batched
  commit widens the release window so a waiting save acquires the lock within the
  busy-timeout instead of erroring (a bare commit re-acquires too fast to hand off).

No schema change. All 863 tests pass.

## [0.3.1-dev1] — 2026-06-28 — smarter fixture labeling

Acting on the full-record labeling audit: the classifier now recognises fixtures
hidden *inside* other events and can say "this is more than one thing." Every
change is gated by a confidence-weighted test harness scored against verified
ground truth (not the classifier's own past output), so accuracy can't silently
regress. Volume totals and leak-safety are untouched.

### New Features

- **Embedded-fixture (composite) detection.** A sustained event — a long shower —
  is now scanned for draws superimposed on its baseline (a toilet flushed
  mid-shower). The History event modal shows them as **"Contains: toilet ×2
  (~9 L)"**. Detection runs on the high-resolution waveform the add-on already
  stores; when the waveform is too coarse to resolve a draw it abstains rather
  than guess. **Annotate-only** — the parent event's volume and primary label are
  never changed (verified byte-identical), so this cannot affect water totals or
  leak detection.
- **The classifier can now emit `other`.** An event that abstained from every
  single-fixture rule but clearly contains a second draw is labelled `other`
  (composite) instead of being left blank — so genuinely multi-fixture events
  read as such instead of vanishing.
- **Time-of-day now informs the fixture matcher.** The k-NN signature matcher now
  considers when an event happened (cyclic hour-of-day), so fixtures that share a
  flow shape but run at different times — a daytime tap vs an evening dishwasher
  fill — separate better. Overall labeling accuracy +1 pt, tap recall +6, with no
  loss elsewhere. (Tested: letting time override the washer/dishwasher *rules* hurt
  accuracy, so the rules still own appliance grouping — only the residual k-NN gained
  the time signal.)

### Under the hood

- New `app/composite_detector.py` (pure, unit-tested): rolling-baseline excursion
  integration over the flow waveform, classing each embedded draw toilet- vs
  tap-sized.
- New `events.embedded_fixtures_json` column (schema migration **20260548**);
  populated by `recompute_embedded_fixtures` on the reclassify path. Metadata only.
  The scan is **incremental** (only un-annotated events) and the reclassify loop now
  **commits in batches**, so a label-save no longer waits on a full-history rescan and
  hits "database is locked" — the long single transaction was the lock that starved
  concurrent user writes.
- Time-of-day: `hour_sin`/`hour_cos` (already computed per event and used by the
  cluster engine) added to `_SIGNATURE_KNN_ACTIVE_FEATURES` at scale 0.35 (interior
  optimum of the confidence-weighted LOO sweep). No schema change — the columns
  already exist and are populated at feature extraction.
- New labeling test harness: `tools/eval_labeling.py` grades labels against a
  **confidence-weighted** ground-truth set (verified anchors > physics-consistent
  user labels > propagated/conflicting > over-applied `other`), with metrics for
  embedded recall, softener-regen recall + dawn-shower false positives, and
  washer-window recall. `tools/eval_knn_classifier.py` gained `--dump-predictions`
  to feed it. On the audit backup, embedded-toilet recall rose from 0.68 to 0.84
  and overall confidence-weighted type accuracy from 0.72 to 0.73 (tap recall
  +6), with no regression elsewhere.

### Evaluated and deferred (the harness said no)

Two audit follow-ups were prototyped against the same harness and **not shipped** —
the data didn't justify the risk:

- **Metered softener-regen model.** The existing schedule-anchored detector already
  catches 3 of 4 validated regens with **zero** false positives. The 4th is masked
  by a long dawn shower; a cumulative "regen-due" prior can't recover it, because the
  measured inter-regen soft-water (697 / 1019 / 980 gal) is too inconsistent for any
  threshold that wouldn't also misfire. Reliable counting of a shower-masked regen
  needs the softener's own meter signal — left as a known limitation.
- **Waveform shape in the type k-NN.** Adding an L2/DTW distance over the stored 32-pt
  flow signature moved leave-one-out accuracy by at most one event (noise): the k-NN
  residual is a small slice already resolved by the rule/cycle tiers, so shape had
  nothing to act on. Left off; documented.

## [0.3.0] — 2026-06-27

Role-based access. The add-on UI is no longer all-or-nothing: non-admin Home
Assistant users can now open the panel, with least-privilege roles enforced
server-side.

### New Features

- **Three access tiers (viewer / operator / admin).** `panel_admin` is now
  `false`, so any logged-in HA user can open the panel, but what they can do is
  gated by role:
  - **admin** — every HA administrator (detected automatically from HA's user
    list). Full control, exactly as before.
  - **operator** — a non-admin you explicitly add on the new **Settings →
    Access** page. Read-only **plus** opening/closing the main water valve — so a
    household member home alone can shut water off (or back on) in an emergency
    without an admin. No settings, backup, labeling, leak tests, or scheduling.
  - **viewer** — every other HA user. Read-only: Dashboard, History, Water Use,
    and live Device status.
- **Access management page (Settings → Access, admin-only)** lists HA users with
  their resolved role and a one-click operator grant/revoke.

### How it works / security

- Roles derive from the Supervisor's `X-Remote-User-Id` ingress header, which is
  trustworthy because the add-on already rejects any request not from the ingress
  proxy IP (it's ingress-only, no exposed port). Default-deny: an unknown user is
  a viewer.
- Enforcement is server-side at a single chokepoint in the ingress middleware (no
  mutating route can be missed); the admin-only routers (settings, backup, setup,
  calibration, training, access) additionally require admin on their GET pages.
  Templates merely hide controls a role can't use.
- Admins are cached (last-known-good) and refreshed from HA every 10 min, with an
  early refresh at startup; a transient HA hiccup can never downgrade a real admin
  to viewer. New optional `bootstrap_admin_user_id` add-on option is an escape
  hatch that always grants admin to one HA user id if the user-list lookup is ever
  unavailable.
- Schema migration **20260547** adds `operator_users`, `admin_ids_cache`, and
  `seen_users` (DDL only).

## [0.2.2] — 2026-06-21

A features + correctness + hardening release. Headlines: runtime per-circuit
flow-meter pulses-per-litre as a Home Assistant number entity (any meter, no
firmware edit) with a guided bucket / municipal-meter calibration helper;
a degraded-supply guard so events captured during pulsing municipal pressure
are flagged and stop poisoning clustering / hourly volume; a per-circuit
valve-type setting
(2-port / 3-port) so micro leak tests are correctly disabled on
drain-capable hardware; and a round of security and robustness work
covering CSRF, the autocorrelation primitive behind the supply detector,
and migration safety. Firmware credentials restored and a release-check
script added to keep them that way.

### New Features

- **Flow calibration helper (guided bucket / municipal-meter test)** — Settings → Flow Meter →
  "Calibrate…" walks the user through measuring the meter's TRUE pulses/litre: enter a known
  reference volume (a bucket, or the whole-house municipal-meter delta), run that fixture, and the
  add-on derives `new_ppl = current_ppl × (measured ÷ actual)` from the cumulative volume sensor
  (no firmware change; self-correcting even if the current PPL is far off). Runs pool by volume,
  with a method-aware sample gate (a >3% bucket correction needs ≥3 averaged fills to cancel
  fill error; a single ≥10-gal municipal run suffices) and a run-spread warning. Applying writes
  the firmware PPL entity; a small trim just re-scales the frozen anomaly thresholds (no relearn),
  a large change re-baselines. The deliberate test draw is suppressed from auto-shutoff and
  excluded from training/anomaly so it can't trip the valve or pollute the baseline. The
  result lists each run plus their average + range, and the suggested value is editable
  (re-gated on its own correction) before applying. New `routers/calibration.py` +
  `calibration_math.py`.
- **Runtime-configurable per-circuit flow meter (PPL)** — pulses-per-litre is now a
  per-circuit Home Assistant `number` entity (`Flow Meter PPL - <circuit>`), NVS-backed
  so it survives reboots and OTA updates. Set it to match any meter — turbine YF-B5 = 396,
  oval-gear ZJ-HSM-OFZATS-06 = 72, or any datasheet / bucket-tested value (replaces the old
  compile-time `flow_k_factor`). The per-circuit default is seeded at flash time by the
  `flow_ppl_main` / `flow_ppl_irr` substitutions, so a unit is correctly calibrated from first
  boot with no HA; `restore_value` then keeps the value across reboots and OTA updates, and it
  can be changed at runtime (HA / device web page) without a reflash. The firmware converts pulses→L/min
  from the live entity; the add-on reads it as the single source of truth (read-only in
  Settings) and derives each circuit's low-flow noise floor (60 ÷ ppl: ≈0.15 L/min at 396,
  ≈0.83 at 72). Changing the PPL forces a NON-destructive re-baseline of that circuit
  (auto-shutoff degrades to notify until the new baseline matures); historical event volumes
  are never recomputed. A coarse positive-displacement meter's small real draws are protected
  from the low-flow dribble-zeroing heuristic (which was tuned for the turbine). DB migration
  20260546 adds the `circuit_profile.pulses_per_litre` cache (default 396).
- **Degraded-supply guard** — when the municipal supply pulses, the
  paddlewheel flow sensor produces chaotic readings (forward and
  reverse pulses both count positive; brief zero-velocity transitions
  read as 0 L/min). Events captured during these conditions are now
  detected via pressure-band autocorrelation + flow-rectification
  signatures, flagged with `degraded_supply=1`, given an
  envelope-smoothed `volume_litres_effective` so daily totals stay
  sensible, and excluded from clustering. Surfaces in the History
  page with a filter, in the dashboard as a banner, and as a
  rate-limited HA notification.
- **Per-circuit valve type** — Setup wizard step 3b and Settings now
  ask whether each circuit is wired for a 2-port (standard) or 3-port
  (drain-capable, winterization-friendly) ball valve. 3-port circuits
  automatically skip the micro leak test (the drain port would always
  read as a constant leak); the schedule is preserved so switching
  back to 2-port resumes it. UI shows the reason on both the Device
  and Settings pages.

### Security

- **Per-session CSRF tokens** — the previous implementation cached a
  single token per process for an hour, so every browser session
  shared the same token. Replaced with stateless HMAC double-submit:
  a persistent server secret (generated once, never auto-rotated) +
  a per-browser `wm_session` cookie + `csrf_token = HMAC(secret,
  session_id)`. Setup-wizard POSTs are no longer CSRF-exempt (the
  earlier `startswith("/setup")` check let every `POST /setup/...`
  through with no token). Path-prefix exemptions replaced with
  exact-match for `/health` so `/health-anything` no longer slips
  past ingress checks.
- **Firmware credentials restored** — API encryption, OTA password,
  fallback-AP password and web-server auth are no longer commented
  out in the firmware YAML; all four read from `secrets.yaml` (see
  `firmware/secrets.yaml.example`). `dashboard_import` URL pinned
  to `@v3.10.0`. A new `scripts/check_firmware_release.py` script
  parses the YAML (not greps — commented examples don't fool it)
  and fails the release if any of those are missing or if the
  `dashboard_import` ref is mutable.

### Bug Fixes

- **Autocorrelation normalisation bug** — `_autocorr_at_lag` in the
  degraded-supply detector mismatched its numerator and denominator
  sample windows. The fix is a proper overlap-weighted Pearson
  correlation: each window centred by its own mean, normalised by
  `sqrt(var_head * var_tail)`, then scaled by sample overlap so the
  peak-pick prefers the fundamental over harmonics. The 12 existing
  degraded-supply tests still pass; 14 new tests in
  `test_autocorr.py` cover the primitive directly.
- **Migration rebuild safety** — the 20260526 migration's
  `hourly_volume` rebuild (DELETE + INSERT/SELECT from `events`) now
  uses a temp-table swap pattern wrapped in `try/except + rollback()`,
  so a Python-level exception mid-rebuild leaves the original table
  intact (previously safe only against process death, via SQLite's
  transaction durability). Eight new migration round-trip tests cover
  forward migration from every historical schema version,
  idempotency, rebuild correctness, and failure-injection.
- **Daily-summary UPSERT future-proofing** — `compute_daily_summary`
  now emits `ON CONFLICT … DO NOTHING` if the column-update list is
  ever empty (currently 12 columns; a future trim could make this a
  SQL syntax error otherwise).
- **Catch-up orphaned events longer than the check interval** — the
  periodic `historical_importer` catch-up advanced `last_check_ts` to
  `now` even while a flow period was still active, so an event longer
  than `CHECK_INTERVAL_MINUTES` had its start slide behind the checkpoint
  and could then only be recovered by a much-later startup backfill (a
  133-min irrigation run surfaced ~4 days late). `_import_range` now
  reports the trailing still-active flow start and folds it into
  `retry_from`, holding the checkpoint there until the event ends so the
  next catch-up after it closes reconstructs the full period. The overlap
  / UUID5 dedup keeps this from double-counting. New tests in
  `test_historical_importer_active_event.py`.
- **No-flow pressure phantoms blocked a circuit for hours** — a pure
  pressure transient whose pressure SETTLED below the recovery line (e.g.
  an irrigation zone solenoid nudging the steady pressure) never satisfied
  the pressure-recovery END and stayed open until the 6 h over-long
  watchdog; while open it blocked every new event on that circuit (the
  `_active_event is None` start gate), so the next irrigation run was
  missed by live detection. New `_maybe_close_settled_noflow`
  (`SETTLED_NOFLOW_CLOSE_S = 60 s`) closes such an event once pressure
  settles and flow has been zero ≥ 60 s; flow-triggered, pulsed, and
  ongoing draws are excluded so a real run is never cut short. New tests
  in `test_event_detector_pressure_recovery.py`.

### Performance

- **Async / blocking-SQLite audit** — `data_pruner.run` (nightly
  prune + daily-summary computation), `_run_waveform_purger` (daily
  DELETE), and `learn_best_hour` (60-day usage analysis for leak-test
  scheduling) are now offloaded via `loop.run_in_executor()` rather
  than running directly on the asyncio event loop. The orchestrator's
  `rebuild_from_db` / `backfill_unmatched` were already wrapped.

### Internal

- **Migration sequence** updated to schema version 20260527 (added
  `circuit_profile.valve_type` with a defensive backfill). Migration
  20260526 added 7 columns on `events`, the `event_waveforms` table,
  and rebuilt `hourly_volume` from events as the source of truth.
- **Set away mode** — `CASE WHEN ?` rewritten to explicit
  `CASE WHEN ? = 1` for clarity (same behaviour).
- **_enrich_from_waveform** docstring now lists the four A/B tracking
  fields it sets.
- **Firmware globals comment** stripped of stale 3.8.x slot+decimation
  history.

---

## [0.2.1] — 2026-05-24

A refinement release of the 0.2.x fixture-identification line. Headlines:
ESP-side waveform capture replaces software-only signatures, the event
detail modal makes individual events inspectable and re-labellable, auto
dark mode follows the OS, and the hardware documentation tree is now
complete (KiCad, BOM, bring-up checklist, build photos). Plus a long tail
of correctness work on the historical importer, propagation-delay scan,
clustering features, persistence, and CSRF coverage.

### New Features

- **Auto dark mode** — UI follows `prefers-color-scheme` automatically;
  no toggle, no setup. All pages, modals, toasts, status pills, and
  Chart.js charts switch live with the OS theme. The dark palette is
  derived from the existing OKLCH light tokens by lightness inversion,
  so brand identity is preserved.
- **ESP-side waveform capture and feature enrichment** — firmware 3.7.0+
  captures per-event flow and pressure waveforms on-device and streams
  them to the addon over a chunked HA event channel. The feature
  extractor uses them when overlap and quality are good enough, and
  falls back to the legacy software-derived values otherwise. Per-group
  fallback (metadata, start window, full window) so partial waveforms
  still help. Migration 031 adds four A/B tracking columns.
- **Event detail modal** on the History page — fixture re-assignment,
  ignore/restore, technical details (trigger type, peak flow, propagation
  delay, anomaly score), and an embedded waveform chart when ESP-side
  capture is available.
- **Merge clusters UI** on the Fixtures page — checkboxes on each
  cluster card plus a confirm page for combining fixtures the engine
  split apart.
- **Basic / Advanced split** in Settings — sidebar groups Advanced-only
  destinations behind a "Show Advanced" toggle persisted in localStorage.
  Per-circuit sub-tabs (Config / Detection / Advanced) replace the long
  scroll.
- **Re-run setup wizard** — Settings → Advanced → Re-run Setup
  unlocks the wizard after initial setup (e.g. for new hardware) and
  prompts to download a backup first. Setup re-locks automatically when
  the wizard completes.
- **Toast notification system** replacing inline status text.
- **Accessible valve confirm modal** replacing `window.confirm()` for
  open/close actions.
- **History page summary strip** showing event counts, pass/fail and
  totals at the top of the page.
- **Last automatic backup date** surfaced on the Backup page.
- **Irrigation enable/disable** toggle — hide the irrigation circuit
  from Dashboard and Device pages without removing it from
  configuration. Configured during setup (hardware selection step) and
  adjustable at any time in Settings → Home Profile. Backed by a new
  `circuit_profile.enabled` per-circuit flag that is forward-compatible
  with future multi-device setups.
- **Historical-import option** in the setup wizard — backfill events
  from HA history when first connecting to an existing device.

### Changes

- **Firmware bumped to v3.7+**. Per-event waveform capture; chunked
  streaming at native cadence; flow-rate publishes throttled to 4 Hz;
  valve-close re-trigger storm on fault fixed (main + irrigation);
  default logger level set to INFO.
- **Clustering**: 32-point pressure signature added to the feature
  vector (60 dims total). Feature weights retuned from F-ratio analysis
  on labelled events.
- **Event detection**: settled resting pressure now drives the event
  baseline; new pressure-transient shape features; zigzag algorithm for
  flow edges; medium sensitivity preset `pressure_drop_event_psi` default
  lowered 2.0 → 1.2 PSI (migration 027). Added pressure-recovery END
  for pulsed pressure-triggered events (e.g. fridge dispenser) so
  flow-pulse-onset flicker no longer fragments a single event.
- **Propagation-delay scan** reworked to be timestamp-based and
  observable in logs; under-estimation on gradual pressure drops fixed;
  pressure+flow composites now enriched with a real delay.
- **Leak test scheduling** now uses the local timezone instead of UTC,
  so the configured run hour matches your wall clock.
- **Flow-stop detection lag** reduced — EMA filter removed; sliding
  window dropped from 5 s to 2 s.

### Bug Fixes

- **Persistence**: `insert_event` is now an upsert, restore is atomic
  inside a single transaction, `UNIQUE (circuit, start_ts)` enforced.
  The historical importer no longer truncates active events on its
  next run (could lose multi-minute fixture activity). Migration 028
  collapses duplicate overlapping event rows left over from older
  builds.
- **Phantom events**: pressure-surge phantoms now rejected by both the
  live detector and the importer. Overnight pressure-oscillation
  phantoms eliminated via baseline-history requirements.
- **Security**: CSRF coverage tightened across all mutating methods
  (PATCH/PUT/DELETE, not just POST); `/setup` mutations locked
  post-setup so a stray `/setup/restore` can no longer wipe the live
  DB. Settings endpoint inputs hardened against malformed values.
- **Setup wizard could not be re-run** after initial setup — every
  `/setup/*` POST was silently redirected to the dashboard. Now shows
  a clear error banner pointing to Settings → Advanced → Re-run Setup,
  which deliberately re-opens the wizard after a confirmation step.
- **Reliability**: fire-and-forget tasks now tracked so failures
  surface; HA WebSocket reconnect is more robust (waveform onset bounds
  hardened, reconnect after setup wizard so new entity subscriptions
  activate immediately).
- **UI / UX**: setup progress bar `TypeError` on the "3b" sub-step
  fixed; dashboard chart legacy alias restored; XSS-via-fixture-label,
  valve modal race conditions, toast-timing edge cases and trickle
  status display all addressed (9 merge-readiness fixes).
- **Discovery**: circuit ID normalisation now survives Supervisor
  restarts (migration 024 patches stale `options.json`); reset-button,
  alert-switch and threshold roles added to role discovery; waveform
  role domains corrected (ESPHome text-sensor entities live in the
  `sensor.*` domain).
- **Database**: volume baselines now consistently UTC (fixes wrong
  daily / weekly totals on non-UTC servers); `update_data_retention`
  column allowlist prevents SQL injection via `**kwargs`.

### Hardware & Docs

- **PCB v1.2a** — design refresh with a new board image. KiCad project,
  Gerbers and BOM published under `docs/hardware/`.
- **Hardware build guide** now complete — overview, build order,
  pinout, bring-up checklist, and an 11-photo build gallery.
- **ESP32-S3-WROOM-1 pinout** diagram added to the hardware index.
- **Stylesheet cache-bust** — the `<link>` to `styles.css` now carries
  a `?v=0.2.1` query string so the UI picks up new CSS on upgrade
  without requiring a hard refresh.

### Internal

- Refactor: hardcoded circuit role names (`main`, `irrigation`)
  replaced with generic stable IDs (`circuit_1`, `circuit_2`) plus
  user-configurable display names. Migrations 023 / 024 carry existing
  installs across the rename.
- Shared restore logic extracted to `restore_utils.py` (used by both
  the setup-wizard restore and the Backup page).
- Migrations: 023 (circuit-ID rename), 024 (`options.json` patch),
  027 (sensitivity preset bump), 028 (overlap dedup), 031 (waveform
  A/B columns).
- Test suite expanded to 422 tests covering waveform enrichment,
  importer state-machine and 1 Hz resampling, propagation-delay
  scans, pressure-period detection, and discovery contracts.

---

## [0.2.0] — 2026-05-10

### Changes

- **Dashboard — "Past 7 days" volume** replaces the previous calendar-week
  (Monday-reset) total. The figure now reflects a rolling 7-day window,
  which is more useful day-to-day and avoids the sharp Monday drop to zero.
  Affects both the hourly-volume fallback path (SQL cutoff changed to
  `datetime('now', '-7 days')`) and the HA cumulative-sensor path (baseline
  key updated to midnight UTC 7 days ago). The volume baseline seeder in
  `_init_volume_baselines` was refactored to use a single naive-UTC key
  format throughout, fixing a key-format mismatch that would have silently
  broken daily totals on non-UTC servers.

- **Addon icon and logo** — `water_monitor/icon.png` (128 × 128) and
  `water_monitor/logo.png` (250 × 100) added. The generic puzzle-piece
  placeholder on the HA addon card and store detail page is now replaced by
  a proper branded icon.

- **Web UI favicon** — `favicon.ico` and `icon_32x32.png` added to the
  static assets folder. `base.html` now includes `<link rel="icon">` tags so
  the browser tab shows the addon icon instead of a blank page icon.

### Bug Fixes

#### Addon — robustness and correctness (second-pass audit)

- **`device_discovery.py` — `name_by_user` None crash in device search**:
  the exact-match path called `.lower()` on `name_by_user` without a None
  guard; the partial-match path already had one. Added `d.name_by_user and`
  guard for consistency.

- **`device_discovery.py` — `save_discovery` not transactional**: multiple
  `db.execute` calls (including FK-ordered deletes and circuit-entity inserts)
  were not wrapped in an explicit transaction. A mid-write failure left the
  DB in a partially-cleared state. Wrapped the entire function body in
  `with db:`.

- **`backup.py` — `tempfile.mktemp()` deprecated + connection leak**: the
  SQLite backup snapshot used the deprecated `mktemp()` (TOCTOU race
  possible). Replaced with `NamedTemporaryFile(delete=False)`. All three
  SQLite connections (`src_conn`, `mem_conn`, `disk_conn`) are now opened
  before a single `try/finally` block so none can leak if `backup()` raises.

- **`backup.py` — `QUICK_RESTORE_TABLES` missing two tables**:
  `fixture_ha_entity_map` and `fixture_daily_summary` were omitted, so
  HA entity mappings and daily fixture summaries were lost on a Quick Restore
  cycle. Both tables added to the list.

- **`database.py` — volume baselines used local time instead of UTC**:
  `compute_ha_daily_volume` and `compute_ha_weekly_volume` called
  `datetime.now()` (local) to compute the baseline `period_ts` key; SQLite's
  `'now'` is always UTC. On servers not in UTC this caused the key to miss
  the row seeded by `_init_volume_baselines`, falling back to a 0.0 baseline
  and showing the full cumulative sensor total as the daily/weekly figure.
  All baseline functions now use `datetime.now(timezone.utc)` with the
  `tzinfo` stripped before `isoformat()` to produce consistent naive-UTC
  keys throughout.

- **`database.py` — `update_data_retention` SQL column injection**:
  column names were interpolated from `**kwargs` without an allowlist. Added
  `_DATA_RETENTION_COLUMNS` frozenset; unknown keys raise `ValueError` before
  reaching the DB layer (mirrors the existing `_HOME_PROFILE_COLUMNS` pattern).

- **`settings.py` — `int(calibration_days)` raises 500 on bad input**:
  bare `int()` on a form value raises `ValueError` if the submission is
  non-numeric or empty. Added `try/except (ValueError, TypeError)` with
  fallback to 14 days.

- **`setup.py` — `int(cal_days)` raises 500 on non-numeric query param**:
  the setup-complete page renders `int(cal_days)` from a query parameter;
  a stale cache hit or crafted URL could pass a non-numeric string. Added
  `.isdigit()` guard with fallback to 14.

- **`main.py` — DB migration failure non-fatal**: a migration error was
  caught, logged at ERROR, and swallowed — the server started with a
  potentially broken schema. Now logs at CRITICAL and re-raises so the
  addon fails fast rather than running in a degraded state.

- **`orchestrator.py` — live-state cache used `setattr`**: per-circuit
  state was cached via `setattr(self, f"_live_state_{circuit}", ...)`.
  A circuit name colliding with an existing `Orchestrator` attribute would
  silently corrupt instance state. Replaced with a dedicated
  `_live_state_cache: Dict[str, Any] = {}`.

#### Addon — code quality (18-issue audit)

- **`device_discovery.py`** — removed duplicate `re.compile` call in
  `_find_entity_for_role`; replaced `__import__("datetime")` anti-pattern
  in `save_discovery` and `mark_setup_complete` with normal imports; added
  FK-safe delete ordering before `DELETE FROM fixtures` to prevent constraint
  violations on re-setup; added comments on regex fragility and suffix-list
  limitations.

- **`backup.py`** — wrapped `import_history_archive` multi-table writes in
  `with orch.db:` for atomicity; replaced `len(rows)` count (which included
  skipped duplicates) with before/after `COUNT(*)` for accurate inserted-row
  reporting; moved `arc.close()` to `finally`; removed dead `import io as _io`.

- **`settings.py`** — fixed `form.get("mqtt_publish_enabled")` truthy check:
  any non-empty string (including `"0"`) evaluated as `True`; changed to
  explicit `== "1"` comparison.

- **`setup.py`** — added `get_home_profile(orch.db) or {}` guard against
  `None` return before `dict()`; applied `quote_plus()` to all exception
  messages embedded in redirect URLs; added early-return guard when no
  restore options are selected.

- **`database.py`** — fixed `get_weekly_volume` SQL date expression that
  gave wrong results on Sunday and Monday (wrong `weekday 1` modifier);
  updated `_create_schema` `CREATE TABLE` statements to include columns
  added by migrations (self-documenting, fresh installs see full schema);
  added docstring to `dedup_events` explaining its post-restore role.

- **`orchestrator.py`** — unit-converted `volume_total` in the live-state
  dict using the same `vol_factor`/`vol_decimals` pattern as
  `volume_daily`/`volume_weekly`; removed startup `dedup_events()` call
  (migration 021's `UNIQUE` index prevents new duplicates at write time).

- **`historical_importer.py`** — `_rate_to_periods` now accepts an optional
  `query_end` parameter; an open flow period at the end of the query window
  is now closed at `query_end` rather than at the last reading timestamp,
  matching the `_onset_to_periods` behaviour.

- **`main.py`** — added inline comment explaining the `/setup` CSRF
  exemption trade-off.

#### Firmware — `esp-water-shut-off-3_6.yaml` (v3.6.0)

- **Spurious motor fault on concurrent close** (🔴): after `open_action`
  fires the relay and waits 15 s, it checks whether the open end-stop is
  inactive to detect a motor fault. If a `close_action` completed during
  that 15 s window the end-stop is naturally inactive — triggering a false
  fault. Fixed: the post-wait condition now checks
  `id(open_in_flight_*) && !id(open_end_stop_*).state` so a false fault
  can only fire if the open sequence is still genuinely in flight.

- **Pressure history buffer too small** (🔴): `pressure_history_main/irr`
  globals were 5-element `float[5]` arrays. `pressure_main` publishes at
  2 Hz (500 ms per sample), so the buffer held only 2.5 s of history, not
  the intended 5 s. Increased to `float[10]` (10 × 500 ms = 5 s); modulo
  index and initial value updated accordingly.

- **Close guard log message misleading** (🟡): fault-triggered close log
  said "valve is mid-travel opening" — incorrect when the guard is set after
  a failed close. Updated to "valve may be mid-travel or in recovery after a
  failed close".

- **Stale comments in `pressure_main_avg` and `pressure_main_fast`** (🟡/🔵):
  `pressure_main_avg` header comment still said "every 1 second", "1.25 s
  sliding window", "30-sample", "120 bytes" — all wrong after the 2 Hz
  refactor. Corrected to 500 ms / 50-sample / 25 s / 200 bytes.
  `pressure_main_fast` comment said "slower 1 Hz pressure_main" — corrected
  to 2 Hz.

---

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
