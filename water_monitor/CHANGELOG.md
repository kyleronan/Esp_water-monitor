# Changelog

## [0.3.1] — Unreleased

Fixture labeling that survives a change in water supply, full booster-pump
support, and a long run of accuracy work driven by audits of the add-on's
stored events against raw Home Assistant history. (Shipped incrementally as
dev1–dev46 — dev7/dev8 landed without a version bump; per-build detail is in
git history.)

### New Features

- **Winterized circuits** — mark a circuit as drained for the season and the
  add-on stops recording use, running leak tests, and learning pressure for
  it, instead of reading the empty line as a catastrophic pressure loss. Turn
  it on *before* draining; turning it off resumes with a short quiet period.
- **Help page** — what each button does and when to use it, organised by task.
  Covers the two easy-to-get-wrong orderings: set the winterized toggle before
  draining, and rebuild fixture grouping *before* re-fitting rules.
- **Keep a label, stop learning from the event** — a checkbox in event details
  separates "this label is right" from "learn from this shape", for correct
  labels over messy events (a dishwasher fill with a tap running across it).
  Previously the only way to exclude one was to mislabel it.
- **Nightly meter reconciliation** — every night the add-on compares the water
  it recorded against the flow meter's own running total for the same day and
  reports the gap. It is the only check measured against something the add-on
  did not produce, so it can catch events that were never recorded at all. It
  reports and never rewrites; gap direction is part of the finding.
- **Download a study snapshot** — a copy of the data stamped with the version
  it came from, for offline analysis. Cannot stall the app or run during a
  rebuild.
- **Leak-test line refills are tagged, not counted** — a scheduled test's
  valve reopen refills the pipes through the meter, which logged as an
  ordinary small draw. Events in a test's reopen window are now tagged
  "🔧 Leak test — line refill", removed from totals and from fixture learning,
  with their own Note filter. Unlike other not-real-use verdicts these stay
  **visible** — a refill that grows is worth noticing. Capped at 1.0 L per
  test, and a test that already detected water in use attributes nothing.
- **Overnight pump watch replaces "Too short to judge"** — ruling on pump
  behaviour needs ~3 recharge cycles, far longer than a deliberately short
  test window, so quiet-pump homes saw that message forever. Rows the
  in-window check can't rule on now answer from the nightly 3-hour pressure
  watch: "Quiet overnight" or "⚠ Pump busy overnight". In-window verdicts
  still win when the test itself could rule.
- **256-point signatures** — stored flow/pressure shape signatures widen
  64 → 256 points so long events stop collapsing into rectangles (at 64 pts a
  45-min shower got one point per ~42 s). Cluster weights rescaled so total
  shape weight is unchanged; migration 20260556 regenerates stored signatures
  from each event's hi-res waveform where finer.
- **Edge signatures in the k-NN matcher** — events store fixed-*time*
  onset/offset shape vectors (32 cells × 1 s each end;
  `events.onset/offset_signature_json`), which align valve ramps and fill
  tapers across differing durations. A new first matcher tier
  (`match_source='active_flow_edges'`) uses them when query and neighbours
  both carry them. Toilet recall 0.783→0.870, shower 0.878→0.927, tap
  0.429→0.486. Migration 20260557 backfills history.
- **Embedded-fixture (composite) detection** — sustained events are scanned
  for draws superimposed on their baseline (a mid-shower flush) and annotated
  "Contains: toilet ×2 (~9 L)". Annotate-only: parent volume and label never
  change. The classifier can also emit `other`, and the k-NN uses time-of-day
  (`app/composite_detector.py`; migration 20260548).
- **Dishwasher-cycle detector** — a chain of ≥3 small gentle fills within
  30 min is recognized as a dishwasher run, fixing concurrent
  washer+dishwasher overlaps that left fills unlabelled.
- **Fingerprint label propagator** — a match tier between rules and the k-NN:
  an event whose un-normalized stored waveform closely matches a user-labeled
  event inherits that label (self-calibrating threshold, user-labels-only
  library so no drift; `app/fingerprint_matcher.py`; migration 20260552).
  Applies only to events ≥2 L effective — it was validated on coarse-meter
  data whose sub-2 L draws never became events, and every sub-2 L stamp was
  later overturned.
- **Toilet physics veto** — a toilet label from any tier is dropped to "Other"
  when the event can't be a single cistern refill: volume floor, era-based
  flush cap from `home_profile.build_year` (EPA 1994 / 1982 / pre-1982 tiers,
  Settings toggle), peak-flow floor, segment count (migration 20260555).
- **Rising-pressure phantom detector** — short flow bursts driven by a
  city-pressure rise were counted as real water. A per-event flow↔pressure
  correlation separates them (real demand pulls pressure down; a rise phantom
  tracks the ramp); fires only under 1 L / 120 s so it stays below the
  leak-detection suspect bar. Includes a one-time backfill (migration
  20260554).
- **Anomalies surface** — flagged events were previously invisible. Now a
  History `?filter=anomaly` view, reason badges (high use / estimated / large
  draw), an "Unusual event" modal section with Mark reviewed, and a dashboard
  card that never ages out. **Two-option review** records intent: ✓ Normal use
  (feeds future baseline refits) vs ❓ Don't recognize it (held out of every
  refit, so an unidentified draw can't widen "normal"); a later relabel
  supersedes the verdict (migration 20260553).
- **History filter bar** — date, duration, avg flow, ΔP and volume min/max
  sliders plus fixture and note dropdowns, all pushed into SQL so the recency
  limit counts matching rows and old matches never vanish. The Shape column
  was removed and the sparkline moved into the Volume cell.
- **Self-healing event hygiene, on by default** — the background pass that
  re-imports garbled events also shrinks inflated single events, reaches the
  `sparse_envelope` events it was built for, and is atomic: a failed
  re-import restores the deleted events verbatim, so a failure can never lose
  water. Anomaly-flagged events are never touched (migration 20260549).
- **Irrigation zone-switch cross-talk cleanup** — zone-valve water hammer was
  logging tiny phantom main-circuit events. A pressure-swing-ratio
  discriminator (irrigation ÷ main ≥ 1.3) in the importer flags and zeroes
  them under a frozen ≤1.5 L cap, so a larger draw is never zeroed. Audited
  in `cross_talk_audit` and revertible (migration 20260550).
- **Volume guardrails** — a degraded event's envelope estimate is capped at
  max(1.5 × flow-integral, 2 L) (an audit found 2.86× inflation), and a
  would-be phantom carrying ≥10 L is kept and flagged for review rather than
  silently zeroed. Migration 20260551 restores already-zeroed large draws
  through the volume ledger.

### Booster-pump and supply-change support

A booster-pump install on 2026-07-19 turned a static ~43 PSI supply into a
53–65 PSI recharge sawtooth, invalidating the static-supply assumptions behind
the pressure detectors and moving every fixture's measured shape. This arc
makes the add-on survive that.

- **Pump-aware supply profile** — the setup/settings supply question gains
  "City water with a booster pump" (`city_pump`), with the well home's +7
  calibration days. New resolver `config.pump_mode_effective` (per-circuit
  override → supply answer → banner-confirmed detection). Answer provenance is
  stamped only on real changes; moving off a pump supply disarms pump state
  (migration 20260558).
- **Nightly pump-regime detection + confirmation banner** — a background
  worker analyzes each circuit's quiet-hour window with a study-validated
  cycling detector (`app/pump_regime_math.py`; 13/13 pre-install nights
  negative, 2/2 positive) and records per-night verdicts. Detection never
  silently changes behavior: a dashboard banner asks "Booster pump detected —
  is that right?" and only confirming activates pump mode.
- **Pump-recharge storm absorber** — on confirmed pump homes a `pump_recharge`
  artifact class claims the brief recharge slugs ("🔄 Pump top-up — not real
  use") and replaces the two detectors whose static-supply premises are false
  under a sawtooth, so a real draw coinciding with a recharge upswing can no
  longer be wrongly zeroed. Offline replay: 85–88% event-count reduction with
  zero labeled-real events lost.
- **Pump-mode live-detector gates** — in confirmed pump mode the detector stops
  opening *pressure*-initiated events while the supply is mid-sawtooth,
  killing the blip-opened wrapper events that swallowed real draws and
  double-counted their water. Flow starts are untouched and remain the primary
  trigger; firmware trickle detection is independent, so nothing here can mask
  a leak.
- **Pump-assisted leak detection** — the pump becomes a leak sensor. On
  detected nights the regime worker estimates leak rate from recharge cycles,
  shows a "💧 Leak watch — ~X gal/day" tile, and fires a notify-only
  `pump_leak` alert when the estimate holds ≥20 L/day for 3 consecutive nights
  or the cycle period shrinks >30% week-over-week. After each valve-closed
  leak test, a cross-check reports whether the *untested* circuit kept
  recharge-cycling — "the leak is on the other line, upstream of the valve, or
  in the pump's own check valve" (migration 20260559). A cross-check failure
  can never affect the test's own result.
- **Low-pressure alerts** — sustained low pressure while an irrigation zone
  runs warns that sprinkler heads may not pop up fully (with fill grace after
  zone start and after any ≥30% flow step). On armed pump homes, 5 minutes
  below the pump floor fires "pump may have lost power/faulted" — unless heavy
  flow is running, which branches to "pump can't keep up with demand". The
  floor gets a measured one-tap suggestion in Settings (migration 20260560).
- **Supply-pressure-aware classification** — recognition had quietly degraded
  (unmatched share of ≥2 L events rose 16%→35%) because both k-NN exemplars
  and frozen rule bands were fitted on old-pressure physics. Four pieces:
  `pre_event_pressure_psi` becomes a k-NN feature (scale 1.5 by LOO sweep,
  median-imputed so unknown pressure is distance-neutral, never a phantom
  "0 PSI" outlier); a supply-regime tracker samples idle-line pressure into
  daily medians and detects sustained shifts (migration 20260564); rule
  calibration is fitted once **per regime** (migration 20260565); and a
  dashboard banner plus Settings action run a one-tap re-fit and reclassify.
  Nothing adapts silently, and firmware leak/trickle safety is untouched.
- **Pressure-invariant matching rung** — a final ladder rung classifies on
  fixture *shape alone*, with every pressure-derived feature excluded. This is
  the durable answer to the failure that motivated the milestone: the pump
  moved toilet ΔP from 4.4 to 11.3 PSI at an unchanged 4.9 L flush, and 800+
  events went unnamed for twelve days. Trained on pre-pump labels and tested
  on post-pump events, it scores 0.750 accuracy / 0.856 coverage / 0.96 toilet
  recall against 0.250 / 0.423 / 0.15 for the pressure-drop tier. It runs
  **last**, so a home with a stable supply sees identical verdicts. Matches
  are recorded as `knn_invariant`; it abstains rather than guessing and cannot
  emit `other`.
- **Fixture grouping can be rebuilt after a supply change** — clustering died
  outright after the install (weekly assignment 75% → 0%, silently, because
  the startup replay reads only events that already carry an assignment). A
  new "Rebuild fixture grouping for current pressure" action re-seeds clusters
  from pump-era events, and an empty replay pool now logs a loud warning
  naming the fix. The rebuilt space excludes every pressure-derived feature —
  under a constant-pressure pump those describe the pump, not the fixture (the
  pressure-vs-flow² correlation fell 0.72 → 0.06 across the install). Measured
  0% → 100% of trainable pump-era events grouped. The feature mode is
  persisted (migration 20260568); pre-pump groupings are kept as history.
- **Anomaly baselines follow a supply change too** — regime recalibration was
  re-fitting classification rules but leaving usage baselines fitted on
  pre-pump usage, so a pre-pump toilet envelope flagged every normal post-pump
  flush. Recalibration now also re-freezes usage baselines and re-scores
  anomalies, fitting on the pinned pump-era anchor with per-type fallback to
  the all-time fit when the era is too thin — and the overall percentiles fall
  back below 30 era events, deliberately the same minimum the shut-off gate
  requires. Every freeze snapshots the previous state (migration 20260569,
  10 deep per circuit) so a bad refit is revertable.
- **Label re-import + calibration guardrails** — the History Archive import
  gains a **labels-only** mode that merges just the archive's user-labelled
  events, with features intact (blanking pressure columns looks conservative
  and is the opposite — a NULL becomes a fabricated "0 PSI drop"). Labelled
  pool 238 → 616. Two guardrails ship with it: a fit may now *tighten* a
  threshold freely but may only *loosen* it within a bounded factor of the
  shipped default (a percentile fit over fragment-polluted pools had produced
  a 2.13 L "big shower" floor against a 30 L default), and the do-no-harm gate
  now requires the frozen default to win by ≥2 events **and** 5% of the test
  set before a fit is dropped, instead of discarding fits on a coin-flip
  one-event difference.
- **Same-circuit overlap guard** — one circuit can only have one event at a
  time, so overlapping same-circuit events mean the same water was recorded
  twice. A guard at the event-write chokepoint zeroes a reconciling wrapper
  through the volume ledger with a "⧉ Duplicate — not extra water" verdict;
  user-labeled wrappers and ambiguous partial overlaps are kept and
  audit-flagged only, because over-counting beats silently dropping real
  water. Migration 20260561 sweeps history once (~3.7 kL recovered here);
  decisions land in `overlap_audit`.
- **Leak test rework (firmware 3.13.2 + add-on)** — driven by a controlled
  drip experiment. Firmware: a failed test no longer raises a safety fault
  (the water stays on — a fail is informational; the trickle detector still
  owns auto-shutoff); tests refuse to start while water is flowing and abort
  if the meter registers pulses before the valve seals; the settle now runs
  *after* the closed end stop instead of a flat 60 s that also covered travel;
  and the decay at threshold crossing is classified by fall **rate** (≥2 PSI/s
  → "demand detected"; measured: flapper 0.04 PSI/s, a real 214 mL/min leak
  0.37, a toilet flush 5–12). Add-on (migration 20260563): rows record the
  firmware's post-settle baseline instead of a pre-close read that folded the
  close transient into every drop; new settle-loss / monitored-window /
  threshold columns; an estimated leak rate from decay × per-circuit
  compliance rendered as gal/day; and a 120 s post-reopen watch that relabels
  a failure amber "Aborted — water in use" when the refill slug is ≥1 L.
  **Dismissible failures**: a reviewed, benign failure can be marked "Ignore"
  — amber rather than red, reversible, never altering the record (migration
  20260562).
- **Large backups no longer depend on browser uploads** — HA's ingress proxy
  rejects big uploads before the add-on sees them, failing exactly when a
  backup matters most. The add-on now maps `/share` and exchanges large files
  there in both directions, with strict filename validation. The upload path
  also fails clearly now instead of showing a cryptic "not valid JSON".
  Requires one add-on restart after updating for the mapping to appear.
- **Older examples carry less weight** — pattern matching prefers examples
  from the same supply era as the event being identified, judged by when the
  events happened rather than by today's date, so an answer doesn't drift.

### Performance and reliability

- **Database work is single-threaded** — the add-on shares one SQLite
  connection while database work was spread across a thread pool; two threads
  touching it at once corrupt each other mid-statement, which killed a
  fixture-grouping rebuild and broke the History page. All of it now goes
  through one worker, long startup work runs in slices so pages stay
  responsive, and a build-time check keeps it that way.
- **Usable seconds after a restart** — a restart used to lock the interface
  for ~3 minutes behind a spinner with no explanation (the notice that should
  have appeared could never fire, and the landing page wasn't covered by it).
  Both fixed; pages now return in ~20 seconds with the slow pass finishing in
  the background. Only Download Study Snapshot still waits, and says so.
- **Label re-derivation is incremental** — every restart re-derived labels for
  all ~5,400 unlabelled events and changed almost none. Each event now records
  which inputs produced its label, so a restart re-checks only what could have
  moved. Saving a label used to trigger a full pass too, which made the whole
  thing pointless in practice; a label now re-checks only the events it could
  plausibly affect. Measured: 80 s → under 1 s, and a restart after labelling
  85 s → 72 s. A new build or rules re-fit still forces a full re-check, but
  it drains in background slices, newest first (157 s of startup churn → 14 s).
- **History no longer starves when hidden artifacts dominate** — the "hide
  not-real-use" toggle was applied *after* the 100-row recency limit, so a
  burst of artifact events crowded real ones out of the page (~18 visible).
  The exclusion now lives in the SQL WHERE.
- **One recalibration at a time** — recalibration can wait minutes on other
  database work, and every click during the wait used to queue another full
  run. Extra clicks are now declined.
- **An interrupted rebuild says so** — a fixture-grouping rebuild cut short by
  a restart leaves the grouping half-finished; Settings now shows that, with
  its start time, next to the button that fixes it.

### Accuracy and data integrity

- **Full raw-sensor audit fixes** — the 2026-08-14 audit replayed all 6,124
  stored events against 91.6 M raw recorder readings; the fixes land app-side.
  The largest: 619 events shared a single ESP waveform capture with another
  event, invisible to the earlier repair because a shared capture usually
  overwrites both peaks with the same plausible value. A startup sweep now
  finds groups by array identity, keeps the claim on the event whose duration
  best matches the capture span, and de-enriches the rest — foreign peak/ΔP/
  delay cleared, contaminated signatures NULLed rather than relabelled, since
  "software" would launder a wrong shape under a trusted label (migration
  20260804). Alongside: time-of-day features computed in the home timezone
  instead of UTC (day-of-week was wrong on 30% of events; migration 20260801),
  `hydraulic_resistance` recomputed when ESP enrichment overwrites ΔP
  (20260803), `true_avg > peak` clamped at write time (20260802),
  `other_valve_open` able to record a confirmed "no", leak tests storing a
  sustained-drop figure from the full-resolution sensor, honest per-channel
  seconds axes on the waveform chart, an annotate-only
  `registration_est_litres` estimate for low-flow meter under-registration
  that touches no total, and daily summaries healing via a nightly dirty-day
  marker with no lookback limit (event counts were stale on 19% of days).
- **Measurement provenance moved into data** — a conformance review closed the
  remaining audit gaps. Cycle-detector outputs no longer feed rule fits at all
  (they are gated by the very bands being fit, so their labels could walk a
  band wider). Leak-test measurement gained a quality gate — minimum samples
  per phase, a measured noise floor, a sustainedness figure separating a held
  drop from a recovered dip by shape, and retained raw samples; when the
  measurement is indeterminate the leak-rate estimate is not computed at all
  rather than silently falling back to the raw two-point drop. The
  meter-registration correction curve moved out of code into a versioned
  `registration_curve` table, explicitly marked **unvalidated**, with every
  estimate stamped with the curve version that produced it and new
  `meter_anchor_points` / `utility_register_readings` tables holding the
  physical reference data it must eventually be anchored against (migration
  20260807; band table in `docs/audit_2026-08_registration_curve.md`).
- **Volume-accounting repairs** — driven by a full audit against raw history.
  The measurement chain is healthy (±1% per event); the errors were all
  downstream. A user-labelled 685 L draw counted as zero is restored — the
  History modal pre-checked its classification boxes from *automatic* flags
  and posted them back as manual verdicts on any save, so opening an event
  could silently promote an auto flag into a permanent one. The boxes now
  submit only when toggled, and a real fixture label reverses any zeroing
  verdict. The pulsing-supply estimator's 1.5× ceiling was itself the
  inflation (worst case: 2.01 L measured, 335.88 L reported) and is now 1.0×,
  with the two paths that bypassed it routed through. Overlap de-duplication
  stops dropping real water — a wrapper now keeps whatever its children don't
  account for (704.7 L of irrigation had been zeroed). Classification failures
  are recorded as `no_tier_matched` instead of leaving 933 events unmatched
  with no reason at all, which had made a 12-day cluster outage invisible.
- **The booster pump's ~1 Hz control ripple is no longer read as failing
  supply** — it satisfied every pulsing-supply gate while the meter stayed
  accurate, taking degraded events from 0 to 27–68/week. Exempted only inside
  the pump era, anchored to a pinned `pump_era_start` (migration 20260566)
  rather than live pump state, because pre-pump genuine pulsing occupies the
  same period band.
- **Bad dishwasher labels no longer teach the system** — the automatic
  "dishwasher cycle" label was wrong more often than right (9 of 19 correct
  before the August outage, 1 of 10 after the last re-seed), and because those
  labels are also training data the fitted dishwasher flow band had stretched
  3.75 → 8.32 L/min over three re-fits. Affected events are marked "don't
  learn from this" (migration 20260805) and skipped by every learning path;
  nothing about the events themselves changes, and reviewing one restores it
  immediately since a user label is ground truth. A later sweep extended the
  quarantine to all remaining unreviewed machine labels with no time bounds
  (migration 20260806).
- **The fake-dishwasher grouping rule is turned off** — the loose rule that
  invented dishwasher cycles out of chained faucet bursts now requires each
  candidate fill to look like a fill (flow variability ≤1.6, ≥40% of the draw
  at steady state). Validated against three months of reviews: keeps 8 of 9
  genuine pre-outage cycles and rejects the burst chains. This closes the last
  channel still minting bad labels into the learning pools daily.
- **Slow steady draws stop reading as toilets** — the flush rule accepted any
  peak above 5 L/min, but a real flush valve dumps at full line rate: all 90
  reviewed genuine flushes peak at ≥7.1 L/min while the mislabeled square
  draws sit at 5–7.5. The peak floor is now 7.5 L/min (keeps 89 of 90 genuine
  flushes; precision 0.81 → 0.87) and stays calibratable. This also explains
  why do-no-harm kept refusing the fitted toilet band three re-fits running —
  the band was being fit partly on those mislabeled draws.
- **Honest waveform timing** — flow and pressure are sampled at different
  rates and the event chart was stretching both to the same width. New events
  record each channel's real span so a pressure dip lines up with the flow
  that caused it, and the hi-res upgrade now runs whenever the capture can
  draw an honest axis (~93% of events) instead of being blocked by an
  always-true point-count check left over from 32-point signatures.

### Bug Fixes

- **Automatic fixture grouping died silently after a restart** — live cluster
  matching stopped cold while the database showed 30 healthy clusters and no
  error anywhere. The boot replay re-attached in-memory centers to stored
  clusters by *centroid proximity*, so any center whose best centroid drifted
  outside the bound stayed unmapped, and the frozen matcher then returned
  "success with no cluster" — events stored NULL with no rejection reason,
  indistinguishable from never having been evaluated. The id map is now
  grounded in the replayed rows' own stored assignments (majority vote per
  center), an unmapped center rejects explicitly with `unmapped_center`, and
  an empty map after replay logs a loud pointer at the Settings re-seed.
- **Rebuilds could corrupt the books** — a recalibration deleted provisional
  fixture groups while thousands of events still pointed at them (the climbing
  "orphaned events" warning), and a rebuild could crash mid-run when a page
  load touched the database. Now: deleting groups always unlinks their events
  in the same transaction; the rebuild runs in small chunks that share the
  database; events arriving mid-rebuild are matched against the finished
  model; and a rebuild that dies leaves a persistent "rerun required" marker
  that warns at every boot (migration 20260808).
- **Stale group links can be cleared, and the cleanup now sticks** — 1,776
  events stranded on deleted fixture groups had no reachable repair (the boot
  warning pointed at a banner that only appears for a different problem). The
  Water Use page now shows an amber banner with a one-click **Clear stale
  group links**. The first version of that repair rebuilt the in-memory
  matcher *without resetting it first*, so the poisoned group-id map survived
  underneath and the next matching pass re-created 1,736 of the links ninety
  seconds later; the repair now resets before replaying.
- **Pump top-ups no longer masquerade as tiny mystery draws** — with the pump
  holding the line, a small bypass leak bled pressure until the pump pushed a
  brief top-up slug, recorded as ~10 unclassifiable micro-events a day and
  ~1 L/day of fixture usage. The recharge detector gained a third signature: a
  pressure-triggered start whose drop is just the pump's restart deadband,
  reached at leak-decay speed with no demand-shaped correlation to veto it
  (migration 20260572 re-verdicts stored history once). The garbled waveforms
  behind them are fixed at source — the live detector seeded the flow curve
  with the pre-onset ramp but not the pressure curve, so every consumer that
  aligns them index-by-index read a time-shifted pressure trace.
- **The leak-watch banner reported leaks that had stopped** — the tile showed
  the most recent night *carrying an estimate* out of the last fourteen, with
  no test of how old it was, so it stated in the present tense that ~110 L/day
  "is leaking" for six days after the cause was serviced. The estimate must
  now come from one of the last three evaluated nights, and the banner gained
  a **Dismiss** button that acknowledges a reading rather than the feature — a
  later night with a fresh estimate re-shows it, so one click can never
  silence a leak still being measured. Separately, the banner now requires
  **two nights of evidence**: a softener regen once shrank the analyzer's
  quiet window and its pump cycling read as leak top-ups, announcing
  "~294 gal/day"; both historical false banners fail the two-night test and
  the real July incident passes it. Nightly estimates are also suppressed when
  the *other* circuit drew water inside the analysis window.
- **One "day", everywhere** — the same water was reported as four different
  daily totals. The History chart bucketed by UTC, so a "day" ran 6 PM to 6 PM
  local; the 24-hour chart is a rolling window and labelled its bars in UTC
  too. Daily rollups now cut at local midnight — the same instant the TODAY
  tile and HA's utility meter use — and the rolling chart reads in local time
  and says "rolling" in its subtitle. History is rebucketed once automatically
  on first start after the update; changing the HA timezone re-runs it.
- **A meter hiccup no longer erases the day** — the TODAY tile computes "meter
  now minus meter at midnight", so a lifetime counter that stepped *backwards*
  (a reboot losing its last flash write, a reflash, a stale republished value)
  was treated as a new starting point and discarded everything already used
  that day — a real 108 gal day showed 20.2. The day's volume is now carried
  across the reset the way HA's utility meter carries it, and the reset is
  logged rather than passing silently.
- **A wedged database writer now names itself** — one connection was observed
  holding a write transaction for 27+ minutes: every History save returned a
  raw 500 and nothing correlated the isolated warnings into "one wedged writer
  is blocking everything". Locked-write failures now feed a shared detector
  that escalates once per episode with restart guidance, and marking an event
  during an episode answers "database is busy — your change was NOT saved"
  instead of a traceback.
- **Regime recalibration outwaits a busy database** — pressing "Re-fit rules
  for current pressure" while a reclassify held the write lock failed after
  one 5-second timeout, and invisibly, since the job row that would surface
  the failure is created inside the locked work. It now retries every 20 s for
  up to 10 minutes; only lock errors retry.
- **Startup no longer crash-loops when a recalibration runs during it** — a
  heavy admin write during startup could hold the write lock past the 5 s busy
  timeout, crashing the supervised training task, which restarted every 5 s
  and collided again for as long as the admin job ran. Startup now waits the
  lock out; the defaults it writes are idempotent.
- **Imported events no longer carry the old install's cluster links** — cluster
  ids are local autoincrements, not portable, but the history import copied
  them verbatim: 272 rows pointed at clusters that don't exist here and 11
  collided with existing ids and silently joined the wrong clusters. Imports
  now strip cluster linkage (features are measurements and still travel
  intact; linkage is a local cache the post-merge backfill re-derives), which
  doubles as the repair for already-imported rows.
- **An ignored leak test no longer counts as a failure in the History summary**
  — the tile counted every non-"Passed" result as a red failure while the rows
  below showed amber "Failed — ignored" / "Stopped" / "Aborted" badges. Tile
  and badges now read one shared classifier, so they can't drift apart again.
- **Vetoed toilet matches say which physics test rejected them** — the log
  printed volume and flush cap, so an event turned down by the 2.8 L
  manufactured-flush floor logged as "vol=2.5 L, cap=30.5 L" and read like a
  passing event. It now names the condition that fired, with per-event lines
  at DEBUG behind an INFO summary per reclassify.
- **Unusual-events Review list was truncated** — the `?filter=anomaly` and
  `?filter=degraded` views filtered in Python after fetching the newest 100
  events, hiding older flagged ones (card said 95, list showed 2). Both now
  filter in SQL, and reviewed anomalies get a neutral "✓ Reviewed" pill
  instead of shouting "⚠ Unusual" forever.
- **Label-save lock contention** — rapid labelling stacked full background
  reclassifies and returned 500 (`database is locked`). Reclassifies are now
  debounced and yield the write lock between batches.
- **Locked-baseline relabel gate** — a relabel no longer fires a full-history
  reclassify once the baseline is locked (`is_baseline_locked`); it still
  applies instantly and propagates to cycle-mates.
- **Importer no longer fabricates long events from noise** — a long
  pressure-dip envelope containing only trivial, individually-viable flow
  blips is no longer stitched into one bogus multi-minute event. The gate only
  removes empty spans between self-sufficient fragments, so it can never
  orphan flow or mask a leak.
- **Re-run Setup unlock didn't unlock** — the unlock endpoint cleared one
  setup-complete flag but the wizard guard read the other; it now clears both.
  Also fixed a FK crash (`fixtures` deleted before `events.fixture_id` was
  nulled) when re-running discovery on a DB with labelled events.

### Investigated, not shipped

- **Refill-curve shape analysis for toilet look-alikes** — run against 151
  genuine flushes and 38 reviewed non-toilets. The expected signature (flow
  tailing off as the float rises) is not in the data; flushes show slightly
  *more* tail than the look-alikes. The best measure found would catch 6 of 38
  look-alikes while wrongly vetoing ~1 in 20 real flushes on the most-used
  fixture in the house. The peak floor above stays the only validated defence.
- **Tuning the 2-cluster collapse** — a 30-configuration offline sweep found
  no setting that escapes it. The fix is a feature-space redesign, queued
  deliberately rather than another hopeful re-seed.

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
