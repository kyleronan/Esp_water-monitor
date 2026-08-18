# Changelog

## [0.3.1] — Unreleased

Smarter fixture labeling, anomaly surfacing, and a round of volume-accuracy
guardrails driven by a full audit of the add-on's stored events against raw
Home Assistant history. (Shipped incrementally as dev1–dev46 — dev7/dev8
landed without a version bump; per-build details are in git history.)

### New Features

- **The database is now driven from one thread — dev46** — the add-on shares a
  single SQLite connection, and database work was spread across a pool of
  threads. Two of them touching it at the same moment corrupt each other
  mid-statement, which is what killed a fixture-grouping rebuild on 8/15 and
  broke the History page on 8/16. All of it now goes through one worker. Long
  startup work is done in slices so pages stay responsive instead of waiting
  behind it, and the pages that need that data say "still starting up" rather
  than hanging. A build-time check keeps it that way.

- **The add-on is usable seconds after a restart — dev46** — a restart used to
  lock the whole interface for about three minutes: opening it just showed a
  spinner on an empty page, with nothing to say why. Two things caused that.
  The notice that was supposed to appear could never actually appear, and the
  page you land on first was not covered by it at all. Both fixed. Underneath,
  the slow part — re-checking every event's fixture label — no longer holds
  anything up: pages come back in about 20 seconds and that work finishes in
  the background. The one thing that still waits is **Download Study Snapshot
  (.zip)**, which needs the background pass to finish and now says so.

- **It stops re-checking labels it already knows — dev46** — every restart
  re-derived a label for all 5,400-odd unlabelled events, and almost none of
  them ever changed: the same events were re-examined and re-rejected on every
  boot since late July, and the pile grows by roughly 50 a day with no upper
  limit. It stored the answer but never stored anything about whether that
  answer was still good. Each event now records which inputs produced its
  label, so a restart re-checks only what could genuinely have moved —
  everything else is left alone. A new build, a rules re-fit, or a label you
  save all still trigger a full re-check, which is exactly when you want one.
  Measured on the real database: 80 seconds down to under one.

- **Yesterday's water is checked against the meter — dev46** — every night the
  add-on now compares the water it recorded against the flow meter's own
  running total for the same day and reports any gap. This is the one check
  that measures its numbers against something it did not produce, so it can
  notice the failure nothing else can: events that were never recorded at all.
  It reports and never rewrites — a gap is evidence, and correcting history
  from a daily total would destroy it. Which direction the gap runs is part of
  the finding: more on the meter means water went unrecorded, more in the
  events means something was counted twice.

- **The flush look-alikes are a labelling job, not a detector job — dev46** —
  dev45 left the remaining look-alikes (fast squarish draws that match flush
  physics in every measurement) queued for "refill-curve shape analysis". That
  analysis has now been run against 151 genuine flushes and 38 reviewed
  non-toilets, and it does not work: the expected signature — a cistern's flow
  tailing off as the float rises — simply is not in the data, and flushes
  actually show slightly *more* tail than the look-alikes do. The best measure
  found would catch 6 of 38 look-alikes while wrongly vetoing about 1 in 20
  real flushes, on the most-used fixture in the house. Nothing shipped. The
  dev45 peak floor stays the only validated defence, and your own labels
  remain the fix for the stragglers.

- **Winterized circuits — dev46** — mark a circuit as drained for the season and
  the add-on stops recording water use, running leak tests, and learning
  pressure for it, instead of reading the empty line as a catastrophic pressure
  loss and alarming all winter. Turn it on *before* draining; turning it off
  resumes everything with a short quiet period so refilling doesn't set off an
  alarm.

- **Keep a label, stop learning from the event — dev46** — some events carry a
  correct label over a messy shape: a dishwasher fill with a tap running across
  it. Previously the only way to keep those out of the learning pool was to
  mislabel them. A checkbox in the event details now separates the two.

- **A Help page — dev46** — what each button does, when to use it, and what the
  warnings mean, organised by what you're trying to do. Includes the two things
  that are easy to get wrong: set the winterized toggle before draining, and
  rebuild fixture grouping *before* re-fitting rules.

- **Download a study snapshot — dev46** — a copy of the data stamped with the
  version it came from, for digging into a question offline. It cannot stall the
  app or run while a rebuild is in progress.

- **Older examples carry less weight — dev46** — the booster pump changed the
  shape of every fixture, so pattern matching now prefers examples from the same
  era as the event it is identifying. Judged by when the events happened, not by
  today's date, so an answer doesn't drift over time.

- **Honest waveform timing — dev46** — flow and pressure are sampled at
  different rates, and the event chart was stretching both to the same width.
  New events record each channel's real span so a pressure dip lines up with the
  flow that caused it.

- **An interrupted rebuild now says so — dev46** — a fixture-grouping rebuild
  cut short by a restart leaves the grouping half-finished. Settings now shows
  that, with the time it started, next to the button that fixes it.

- **One recalibration at a time — dev46** — recalibration waits out other
  database work for minutes, and every click during that wait used to queue
  another full run. Extra clicks are now declined.

- **Slow steady draws stop reading as toilets — dev45** — the flush-scan
  review found the "~ Toilet" suggestion landing on square, plateau-shaped
  draws that plainly weren't flushes (a 3-pulse 62 s tap run, back-to-back
  appliance fills, a two-notch draw). The hole: the flush rule accepted any
  peak above 5 L/min, but a real flush valve dumps at full line rate — every
  one of the 90 reviewed genuine flushes peaks at 7.1 L/min or higher, while
  the mislabeled slow draws sit at 5–7.5. The rule's peak floor is now 7.5
  L/min (validated against all 116 reviewed toilet claims: keeps 89 of 90
  genuine flushes, precision 0.81 → 0.87), and it stays calibratable so a
  per-pressure fit with your labels can adjust it — with the do-no-harm
  check arbitrating as always. This also explains why that check kept
  refusing the fitted toilet band three re-fits running: the band was being
  fit partly on those mislabeled square draws, and the check correctly
  preferred the defaults. Remaining look-alikes (fast squarish draws that
  match flush physics exactly) still need the refill-curve shape analysis —
  queued; your star-labels fix them individually meanwhile.

- **The waveform time axis finally shows up — dev45** — the event modal has
  quietly been showing every event on the axis-less, index-aligned signature
  overlay: the code that upgrades to the high-resolution view (with the real
  per-channel seconds axis added in dev38) refused to run whenever the
  hi-res capture had fewer points than the stored signature — a sensible
  rule when signatures were 32 points, and an always-true one after they
  widened to 256. The upgrade now runs whenever the capture can draw an
  honest time axis, which covers ~93 % of events (5,407 of 5,784 with a
  stored capture). The remaining ~377 signature-only events keep the
  proportional overlay — their per-channel time spans were never recorded,
  so an honest axis for them is queued as forward-only work. Also in dev45:
  the "Clear stale group links" button now refuses with a friendly message
  while the add-on is still starting up — clicking it mid-boot raced the
  startup's own replay of the event history, and whichever finished last
  owned the result. (The dev44 reset fix held in production; this makes the
  ordering deterministic instead of lucky.)

- **The stale-link repair now resets the matcher, so the cleanup sticks —
  dev44** — the first click of dev43's "Clear stale group links" cleared all
  1,787 stranded references, and ninety seconds later the startup's backfill
  quietly re-created 1,736 of them: the repair rebuilt the in-memory matcher
  WITHOUT resetting it first, and a rebuild on a live matcher layers new
  events onto the existing model without clearing its group-id map — the
  poisoned entry pointing at the deleted group survived underneath, and the
  very next matching pass wrote it back onto every event. The repair now
  resets the matcher before replaying (the same reset-then-replay order the
  rebuild action has always used), regression-tested with a deliberately
  poisoned map. Click the button once more after this update to clear the
  re-minted references for good.

- **The stale-group cleanup finally has a button — dev43** — dev42 stopped
  rebuilds from stranding events on deleted fixture groups, but the 1,776
  already-stranded events had no way to be cleaned up: the boot warning
  pointed at a "relink banner" that only ever appears for a different
  problem (a confirmed fixture losing its group), so the actual repair was
  unreachable outside a years-old migration. The Water Use page now shows
  an amber banner whenever stranded events exist — "N event(s) still point
  at fixture groups that no longer exist" — with a one-click **Clear stale
  group links** that unlinks them and rebuilds the in-memory matcher, so
  the deleted group ids can't be resurrected from the stranded events'
  votes (which is how the count kept climbing on its own). Admin-only,
  hidden when the count is zero.

- **The fake-dishwasher tap is turned off, and rebuilds can't corrupt the
  books anymore — dev42** — three fixes, all found chasing last night's
  numbers. First, the loose grouping rule that invented dishwasher cycles
  out of chained faucet bursts (measured wrong 9 times in 10 after the last
  re-seed) now requires each candidate fill to LOOK like a fill — steady,
  gentle flow (flow variability ≤ 1.6 and at least 40 % of the draw at
  steady state). Validated against three months of your reviews before
  shipping: it keeps 8 of 9 genuine pre-outage cycles and rejects the burst
  chains; this closes the last channel that was still minting bad labels
  into the learning pools daily. Second, the "Rebuild fixture grouping"
  action was quietly corrupting bookkeeping two ways: a recalibration
  deleted provisional fixture groups while thousands of events still
  pointed at them (the climbing "orphaned events" warning — 1,772 by last
  night — with live matching happily assigning MORE events to the deleted
  groups every day), and the rebuild itself could crash mid-run when a page
  load touched the database at the wrong moment (the 8/15 crash), leaving a
  half-rebuilt model with no trace. Now: deleting groups always unlinks
  their events in the same transaction; the rebuild runs in small chunks
  that share the database politely instead of fighting over it; events
  arriving mid-rebuild wait and are matched against the FINISHED model,
  never a half-built one; and a rebuild that dies leaves a persistent
  "rebuild incomplete — rerun required" marker that warns at every boot
  until a rerun succeeds (migration 20260808). Also recorded: the offline
  parameter study concluded the 2-cluster collapse cannot be fixed by
  tuning — no setting in a 30-configuration sweep escaped it — so the fix
  is a feature-space redesign, queued deliberately rather than another
  hopeful re-seed.

- **Measurement provenance lands in data, not code — dev41** — a conformance
  review of the dev38 audit fixes against the audit's own decisions closed
  the remaining gaps. Training hygiene: the dishwasher-label quarantine now
  covers ALL remaining unreviewed machine labels (migration 20260806, no
  time bounds — see the dev40 entry), and cycle-detector outputs no longer
  feed the rule fits at all — the cycle detectors are gated by the very
  bands being fit, so their labels can never again walk a band wider.
  Leak tests: the addon-side measurement grew a quality gate — minimum
  sample counts per phase, a measured noise floor (robust σ of detrended
  monitor samples; the firmware baseline is a 1.375 s-smoothed 0.01-psi
  read, so ripple, not quantization, is what the floor guards), a
  sustainedness figure that separates a held drop from a recovered dip by
  *shape*, the other circuit's valve state at monitor start, and the raw
  monitor samples retained on the row. When any of these makes the
  measurement indeterminate, the leak-rate estimate is not computed at all —
  it no longer silently falls back to the raw two-point drop — and the
  "transient dip" note stays silent; the firmware verdict is, as always,
  never touched. Provenance: `other_valve_open` now records when/how the
  valve state was established; `overlap_audit` stale marks carry a
  timestamp; new ESP rows warn if the firmware omits a waveform boot id
  (the 48-hour claim probe is load-bearing — boot ids are not persistent
  counters). And the meter-registration correction curve moved out of code
  into a versioned `registration_curve` table seeded from the audit
  analysis and explicitly marked **unvalidated** — every stored estimate is
  stamped with the curve version that produced it, new
  `meter_anchor_points` / `utility_register_readings` tables hold the
  physical reference data the curve must eventually be anchored against
  (the audit's band table is committed at
  `docs/audit_2026-08_registration_curve.md`), and the estimate stays
  annotate-only: no volume, no total, no historical row is ever recomputed.
  (Migration 20260807; the throttled-valve bucket test that flips the curve
  to "anchored" — or refutes it — is a physical action still owed.)

- **Bad dishwasher labels no longer teach the system — dev40** — a check of
  reviewed events on 2026-08-15 found the automatic "dishwasher cycle" label
  was wrong more often than right: 9 of 19 correct before the August outage,
  1 of 10 after the last re-seed, where short faucet bursts were being chained
  together into a fake fill-and-drain sequence. Because those labels are also
  what the add-on learns from, the fitted dishwasher flow band had already
  stretched 3.75 → 8.32 L/min over three re-fits — wrong labels making the
  next wrong label easier to accept. Affected events are now marked as "don't
  learn from this" (migration 20260805) and skipped by every learning path:
  the k-NN label pools, cluster suggestions, fixture signatures, rule fitting,
  and the usage baselines behind anomaly detection. Nothing about the events
  themselves changes — labels, verdicts and volumes are untouched, they still
  appear normally in History, and reviewing one restores it to the learning
  pool immediately, since your label is ground truth. Every unreviewed
  automatic dishwasher label is marked: the first pass (migration 20260805)
  covered the two measured windows, and the dev41 sweep (migration 20260806)
  extends it to everything else with no time bounds — the outage mid-window
  had originally been exempted pending re-grouping, but re-grouping touches
  cluster assignments, never labels, so the exemption protected nothing
  while those rows kept feeding the fits. The grouping rule that produced
  the bad labels is unchanged for now — this stops the damage spreading
  while that fix is validated.

  Two smaller items ride along. Cluster health now shows up in the log at
  rebuild time: a warning when the fixture groups have blurred into each other
  (the condition that preceded the dev39 outage — on 2026-08-15 no fixture
  group on the main circuit had its members agreeing more than 49 % on a
  single stored cluster), plus a count of how many
  distinct groups saw water in the last 48 hours. And the "Rebuild fixture
  grouping for current pressure" button no longer breaks when double-clicked:
  the rebuild takes about a minute with the page sitting there loading, so
  people click again, and the second run used to crash the first one
  mid-write. It now reports that a rebuild is already running.

- **Automatic fixture grouping no longer dies silently after a restart —
  dev39** — live cluster matching stopped cold on 2026-08-13 while the
  database showed 30 healthy clusters, no error anywhere. Root cause: the
  boot-time replay rebuilds the in-memory matcher from scratch and then
  re-attached its centers to the stored clusters by *centroid proximity
  under an acceptance bound* — any center whose best stored centroid
  drifted outside the bound (scaler statistics shift a little on every
  restart, and dev38's timezone feature rewrite shifted them a lot) stayed
  unmapped, and the frozen-circuit matcher then returned "success with no
  cluster": events stored NULL with no rejection reason, indistinguishable
  from never having been evaluated. Three fixes: the id map is now grounded
  in the replayed rows' **own stored cluster assignments** (majority vote
  per center — the database already knows which cluster each replayed event
  belongs to, so the link can't drift), with the old proximity method
  demoted to a fallback for vote-less centers; a match landing on an
  unmapped center now rejects explicitly with `unmapped_center` (stored on
  the event, warned in the log) instead of silently succeeding with
  nothing; and an empty id map after a replay logs a loud pointer at the
  Settings re-seed. Regression-tested with a restart simulation seeded
  with deliberately-garbage stored centroids — the vote-based map survives
  it, the old method couldn't.

- **Every fix from the full raw-sensor audit, in one release — dev38** — the
  2026-08-14 audit replayed all 6,124 stored events against 91.6 M raw HA
  recorder readings and the fixes land here, app-side only. The big one: 619
  events (August audit, 269 groups) shared a single ESP waveform capture with
  another event — byte-identical stored arrays with physically impossible
  duration mismatches, invisible to the dev37 repair because a shared capture
  usually overwrites *both* peaks with the same plausible value. A second
  startup sweep now finds groups by array identity, keeps the claim on the
  event whose duration best matches the capture span (no-winner escape hatch
  when the rightful owner is gone), and de-enriches the rest — foreign
  peak/ΔP/propagation-delay cleared with pre-repair audit copies, and the
  contaminated signatures NULLed rather than relabelled (they were regenerated
  from the foreign capture's arrays, so "software" would launder a wrong shape
  under a trusted label; migration 20260804 applies the same correction to the
  31 rows dev37 repaired). The claim ledger's NULL-boot_id hole — nine ESP
  rows in ten couldn't block a double claim — now falls back to a same-circuit
  48-hour probe. Alongside it: time-of-day features are computed in the home
  timezone instead of UTC (day-of-week was wrong on 30 % of events; a deferred
  boot task rewrites all stored rows once the zone is known, marker column via
  migration 20260801); `hydraulic_resistance` is recomputed whenever ESP
  enrichment overwrites ΔP (1,324 stale rows backfilled by 20260803);
  `true_avg > peak` is clamped at write time (825 impossible rows raised by
  20260802); `other_valve_open` can finally record a confirmed "no" (valve
  states are primed from HA at startup instead of waiting for a transition);
  leak tests store a sustained-drop figure — the median of the monitor
  window's tail from the full-resolution sensor, not one instantaneous
  0.5-psi-quantised read — with read timestamps, the leak-rate estimate uses
  it, and a firmware-Failed test whose sustained drop sits under half the
  threshold gets a "transient dip — recommend re-running" note (display only;
  the firmware verdict is never altered); the event modal's waveform draws
  flow and pressure on honest per-channel seconds axes (they were index-
  aligned across different cadences — misaligned on 18 % of events; new
  per-channel source metadata on `event_waveforms`); a new annotate-only
  `registration_est_litres` **estimates** true volume where the meter's
  low-flow under-registration bites (per audit cross-analysis, 1.5–2.5 L/min
  reads ~27 % low and 2.5–4 ~10 % low, relative to the meter's own
  ≥ 8 L/min band and pending validation against a low-flow reference test)
  without touching any total; daily
  summaries heal via a dirty-day marker drained nightly with no lookback
  limit (event counts were stale on 19 % of days — frozen post-day-end
  summaries plus reprocess re-imports); and `overlap_audit` rows whose events
  were reprocessed away or retention-pruned are marked stale instead of
  rendering blank "covering event" chips, with the restore-dedup path doing a
  real id remap. Validated by 13 new test files and a replay of the audit's
  own queries against the audited database copy.

- **Pump top-ups no longer masquerade as tiny mystery draws — dev37** — with
  the booster pump holding the line, the small bypass leak slowly bleeds
  pressure until the pump restarts and pushes a brief ~10–15 s slug of water
  to top the line back up. Around ten of these a day were being recorded as
  unclassifiable micro-events with garbled-looking waveforms, and about 1 L/day
  of pump top-up water was counted as fixture usage. Two fixes:
  the recharge detector gained a third signature — a pressure-triggered start
  whose drop is just the pump's restart deadband, reached at leak-decay speed
  (bench: demand pulls pressure down at 5–12 PSI/s, a leak at ~0.4) with no
  demand-shaped flow/pressure correlation to veto it — validated against the
  08-09 production export (71 events tagged across 22 days, none user-labeled,
  each under the frozen 0.6 L leak-safety cap; migration 20260572 re-verdicts
  stored history once). And the garbled waveforms themselves are fixed at the
  source: the live detector seeded the *flow* curve with the pre-onset ramp but
  not the *pressure* curve, so the two series started at different times and
  every consumer that lines them up index-by-index — the event modal's overlay
  and the software flow/pressure correlation — read a time-shifted pressure
  trace. Both series now share the same origin, which also repairs the
  correlation feature the phantom and recharge detectors depend on. (The
  "Reprocess event" button remains for genuinely garbled history, but these
  events no longer need it.)
  Also — dev37: the **Leak watch banner now requires two nights of evidence**.
  On 2026-08-10 it announced "~294 gal/day is leaking" from a single
  contaminated night: a water-softener regeneration shrank the analyzer's
  quiet window to 66 minutes and its pump cycling read as 27-second leak
  top-ups. A real leak regime cycles every night (the July incident detected
  on consecutive nights); one-off contamination — a softener regen, the 02:00
  irrigation program that caused the earlier "110 L/day" banner — shows up as
  a lone detected night surrounded by quiet ones. The banner now stays silent
  unless the evaluated night before the estimate also detected cycling; both
  historical false banners fail that test, the real July incident passes it.

- **One "day", everywhere — dev36** — the same water was being reported as four
  different daily totals. On 2026-08-06 the History chart said 100.1 gal, the
  dashboard's 24-hour chart said 108.4, Home Assistant's own card said 108, and
  the dashboard's **TODAY** tile said 20.2. None of them were lying; they were
  answering four different questions.
  The History chart bucketed days by **UTC**, so a "day" ran 6 PM to 6 PM local
  — last night's evening showers counted as today, and tonight's counted as
  tomorrow. The 24-hour chart is a *rolling* window rather than a day at all,
  and it labelled its bars in UTC too, so the 5 AM shower was drawn at "11:00".
  Daily rollups now cut at **your local midnight** — the same instant the TODAY
  tile and Home Assistant's utility meter use — and the rolling chart's axis
  reads in local clock time, with the word "rolling" in its subtitle so it
  isn't mistaken for a daily total. Existing history is rebucketed once,
  automatically, the first time the add-on starts after the update; day totals
  near the boundary will shift by design, because they were previously cut in
  the wrong place. Changing your Home Assistant timezone re-runs the rebuild.

- **A meter hiccup no longer erases the day — dev36** — the TODAY tile works out
  your usage as "meter now, minus what the meter read at midnight". If the ESP's
  lifetime counter ever stepped *backwards* — a reboot that loses the last
  flash write, a reflash, a stale value republished on reconnect — that was
  treated as a new starting point and **everything already used that day was
  thrown away**. That is the 20.2 above: a real 108 gal day, restarted in the
  evening. The 7-day tile never showed the problem, because its starting point
  sits far below any single day's reading.
  The day's volume is now carried across the reset the same way Home Assistant's
  utility meter carries it, so the two agree instead of drifting apart, and the
  reset is written to the log rather than passing silently. A period with no
  measured high-water mark still restarts from zero — under-reporting a day is
  recoverable, inventing water that never flowed is not.

- **The app's own leak test no longer shows up as water you used — dev35** — a
  scheduled leak test closes your main valve, and when it reopens the pipes
  refill. That refill runs through the meter, so it was logged as an ordinary
  little draw: on 2026-08-04 at 01:56, 9 seconds and 0.04 L with the pressure
  climbing back from 52 to 56 PSI. It isn't a tap or an appliance, but it also
  isn't a sensor error — the meter measured it correctly — so none of the
  existing "not real use" detectors could recognise it. The only thing that
  knows what it was is the scheduler that closed the valve, so that is now what
  labels it: events landing in a test's reopen window are tagged **"🔧 Leak
  test — line refill"**, their volume is removed from every total, and they are
  kept out of fixture learning.
  Unlike the other not-real-use verdicts these stay **visible** in History
  rather than being hidden — there is at most one a day, and a refill that
  suddenly gets bigger is worth noticing, since it reads out how much the
  isolated section is losing between tests. They have their own **Note filter**
  entry, and opening one explains what happened. If water really was running,
  relabelling still overrides the call and restores the volume.
  The tag also **takes over** from the older detectors: on the production
  history they had claimed 9 of 10 of these refills as "drip", "pressure-silent
  flow", or "pump top-up". The water removed is identical either way — what
  changes is that the event now says what actually happened.
  Safety comes from the test's own demand bar: **at most 1.0 L per test** can
  ever be attributed to a refill, and a test that already detected water in use
  attributes nothing at all. Verified against the full stored history — 10
  refills tagged (0.014–0.083 L), and three real toilet flushes that happened
  to fall inside a reopen window (3.9, 6.0 and 6.2 L) were all left untouched.

- **"Too short to judge" replaced by the overnight pump watch — dev35** — the
  Pump check column on leak tests needs ~3 recharge cycles inside the test
  window to rule, and at this pump's last confirmed pace that's ~24 minutes —
  while tests are deliberately kept to a few minutes, because a long
  valve-closed window invites false failures from household draws (icemaker,
  humidifier, someone up at night) and from thermal contraction after a heater
  cycle. So on a quiet pump every nightly test showed **"Too short to judge"**
  forever, reading like a complaint about the test. Now rows the in-window
  check can't rule on answer from the **nightly 3-hour pressure watch** that
  already runs in the same small hours — valve open, no isolation, and immune
  to decay-shaped thermal effects since it counts recharge *rises*, not slow
  decline: **"Quiet overnight"** (green) when that night's watch saw no
  cycling, **"⚠ Pump busy overnight"** (amber) when it did. Resolved at
  display time on purpose, because the same-night analysis lands hours *after*
  the 1 AM test — so the morning's result upgrades the row you look at over
  coffee. The in-window verdicts ("Leak elsewhere" / "Pump quiet") still win
  when the test itself could rule, and a night with no watch result shows a
  neutral "No pump verdict" whose tooltip finally says how long a window the
  in-window call would need — as an explanation, never as advice to run
  longer tests.

- **Fixture recognition now survives a change in water supply — dev34** — a new
  final rung in the matching ladder classifies on **fixture shape alone**, with
  every pressure-derived feature excluded from its feature set. This is the
  durable answer to the failure that motivated this whole milestone: installing
  a booster pump moved toilet pressure-drop from 4.4 to 11.3 PSI at an
  unchanged 4.9 L flush, and because every existing tier reads pressure, the
  home's post-pump events fell outside their learned bands and **800+ events
  went unnamed for twelve days**.
  Measured by training only on pre-pump labels and testing only on post-pump
  ones — the actual test of whether a tier survives a supply change:

  | tier | accuracy | coverage | toilet recall |
  |---|---|---|---|
  | existing (pressure-drop) | 0.250 | 0.423 | 0.15 |
  | existing (pressure-conditioned) | 0.375 | 0.731 | 0.46 |
  | **new (shape only)** | **0.750** | **0.856** | **0.96** |

  It runs **last**, after every existing tier has abstained, so a home whose
  supply hasn't changed sees identical verdicts — the tiers above carry more
  signal while pressure is stable, and leave-one-out on the full pool confirms
  it adds coverage without cost (0.927 → 0.947 coverage, 0.750 → 0.759
  accuracy, no class worse). Matches it makes are recorded as `knn_invariant`
  so its contribution stays separable in the data. It abstains on the classes
  it is weak at rather than guessing, and — a known limit worth stating — it
  cannot emit `other`, so a genuinely novel draw is left unnamed rather than
  forced into the nearest known fixture.

- **Large backups no longer depend on browser uploads — dev34** — Home
  Assistant's ingress proxy rejects big uploads before the add-on ever sees
  them, which surfaced as a cryptic "not valid JSON" error and fails exactly
  when a backup matters most: restoring years of history onto a rebuilt HA,
  where the history archive (or a full export) is tens of MB. The add-on now
  maps **`/share`** and exchanges large files there instead, in both
  directions: the Backup page can **save the Full Export to
  `/share/water_monitor/`** (where HA backups and Samba can pick it up), and
  the import dialog lists archives found there — a raw `.db` or a Full Export
  `.zip` both work (the database inside the zip is used), with the same
  merge semantics and labels-only option as the upload path. Filenames are
  validated strictly (bare names inside the share folder only). The upload
  path also fails clearly now: oversized files are stopped client-side with
  an explanation, and a proxy rejection shows the proxy's actual words. The
  import dialog additionally gained the **"Labelled events only"** checkbox
  the dev34 label re-import added server-side. Requires one add-on restart
  after updating for the `/share` mapping to appear.

- **Anomaly detection now follows a supply change too — dev34** — the regime
  recalibration re-fit the *classification* rules but left the *usage
  baselines* — the per-fixture envelopes and overall volume percentiles
  behind "unusual event" flags and the shut-off confidence gate — fit on
  pre-pump usage. Toilet fills shortened 2.6× under the pump, so a pre-pump
  toilet envelope flags every normal post-pump flush; the dashboard's
  "152 events didn't fit your home's usual pattern" is that failure at full
  size. The regime recalibration now also re-freezes the usage baselines and
  re-scores anomalies. The fit windows on the **pinned pump-era anchor** (the
  era, not the current regime, which a recenter can move), falling back per
  fixture type to the all-time fit when the era has too few events — a stale
  envelope beats none — and the overall percentiles fall back below 30 era
  events, deliberately the same minimum the shut-off gate requires: a window
  too thin to authorise a shut-off must not re-anchor the percentiles that
  feed one. Every freeze snapshots the previous state first (migration
  20260569, kept 10 deep per circuit) so a refit that lands badly is
  revertable. **After deploying, run "Re-fit rules for current pressure"
  once** — that single action now refreshes rules, baselines, and anomaly
  scores together, and should clear most of the standing "unusual events"
  backlog.

- **Fixture grouping can be rebuilt after a supply change — dev34** — the
  automatic fixture grouping (clustering) died outright after the pump
  install: live matching stopped when the features moved, and because the
  startup replay reads only events that already carry a cluster assignment,
  the replay pool drained until a restart rebuilt nothing — weekly assignment
  went 75% → 58% → 45% → 8% → **0%**, permanently and silently. A new
  **"Rebuild fixture grouping for current pressure"** action on the Settings
  calibration card re-seeds the clusters from pump-era events, and the empty
  replay pool now logs a loud warning naming the fix instead of failing
  quietly. The rebuilt space excludes **every pressure-derived feature** —
  under a constant-pressure pump those describe the pump, not the fixture
  (the pressure-vs-flow² correlation that makes pressure fixture evidence
  fell 0.72 → 0.06 across the install), and they were most of the problem:
  the pressure-shape block alone was 37% of the grouping distance and its
  class separation collapsed to near noise, while flow shape held. Measured
  on the production export: 0% → 100% of trainable pump-era events grouped,
  with softener and shower clusters cleanly separated. The feature mode is
  persisted (migration 20260568) so a restart rebuilds the same space.
  Pre-pump groupings are kept as history; nothing is deleted, leak protection
  is untouched, and the action is re-runnable — grouping quality improves as
  post-repair events accumulate, so it is worth re-running after a few weeks.

- **Label re-import + calibration guardrails — dev34** — the classifier's
  coverage (not its accuracy) collapsed after a fresh start discarded 486
  hand-made labels, so the History Archive import gains a **labels-only**
  mode that merges just the archive's user-labelled events. Rows arrive with
  features INTACT: blanking the pressure columns looks conservative and is
  the opposite, because `pressure_delta_psi` is a linear k-NN dimension where
  a NULL becomes a fabricated "0 PSI drop". Measured on the real archives:
  the labelled pool goes **238 → 616 (2.6×)** with zero id collisions,
  water-softener 1 → 12, washing machine 22 → 48; k-NN coverage rises
  0.910 → 0.927 with accuracy flat once the `other` class (which the
  classifier structurally cannot emit) is excluded. Collisions are counted
  and named rather than silently dropped.
  Two calibration guardrails ship with it. **(1) Bounded loosening.** The
  per-home rule fit could collapse a discriminative threshold to nothing: the
  2026-08-02 regime refit produced a "big shower" floor of **2.13 L** against
  a 30 L default (and a 14.6 s irrigation-zone floor against 240 s), because
  a percentile fit over a pool containing event fragments puts the p10 far
  below the class's real edge — and the absolute sanity bounds (0–600 L) are
  far too wide to catch it. A fit may now *tighten* a threshold freely but
  may only *loosen* it within a bounded factor of the shipped default, which
  encodes the physics of the class. This keeps the legitimate pump-driven
  dishwasher change (peak ceiling 3.6 → 7.59 L/min) and rejects the
  degenerate floors. **(2) A noise margin on do-no-harm.** The gate that
  protects a well-tuned home from a bad refit was discarding fits on a
  one-event held-out difference — that is a coin flip, and it left pre-pump
  toilet constants in force through a regime where toilet ΔP had risen 2.6×.
  The frozen default must now win by at least 2 events AND 5% of the test set
  before the fit is dropped; a fit that ships while slightly behind is logged
  as such. **After deploying, re-run the regime recalibration** — the sanity
  gate applies at fit time, so bands already frozen stay until refit.

### Fixes

- **An ignored leak test no longer counts as a failure in the History summary
  (dev35)** — the "Leak tests (last 20)" tile read "11 passed · 9 failed" while
  the rows below it showed amber "Failed — ignored", "Stopped" and "Aborted"
  badges: the tile counted *every* result that wasn't "Passed" as a red
  failure. It now counts the same verdict the badges show — only an unexplained
  failure is tallied as failed, ignored ones get their own amber "N ignored"
  count, and tests that never reached a verdict (aborted, timed out, stopped,
  not run) aren't counted either way. Both the tile and the row badges now read
  one shared classifier, so the number and the badges can't drift apart again.

- **A stuck database write lock now names itself, and event saves fail
  gracefully (dev34)** — one connection was observed holding an open write
  transaction for 27+ minutes: every save from the History page returned a
  raw "Error 500", the background samplers logged isolated warnings, and
  nothing correlated them into "one wedged writer is blocking everything".
  Locked-write failures across the app now feed a shared detector that
  escalates once per episode after failures span two minutes, with restart
  guidance in the log. Marking an event (Normal use / labels / checkboxes)
  during such an episode now answers with a clear "database is busy — your
  change was NOT saved, try again in a minute" instead of a 500 traceback.

- **The regime recalibration now outwaits a busy database instead of dying
  (dev34)** — pressing "Re-fit rules for current pressure" while a startup or
  import reclassify held the write lock failed after one 5-second busy
  timeout with "database is locked", and invisibly: the job row that would
  have surfaced the failure is created inside the locked work. Observed twice
  live. The recalibration now retries every 20 seconds for up to 10 minutes —
  a full reclassify runs minutes, so the budget is sized in minutes — and
  only lock errors retry; anything else still fails fast.

- **Imported events no longer carry the old install's cluster links (dev34)**
  — cluster ids are small local autoincrements, not portable, but the history
  import copied them verbatim. On the real label import that meant 272 rows
  pointing at clusters that don't exist here (the startup "orphaned events"
  warning) and 11 that collided with existing ids and silently joined the
  wrong clusters, polluting cluster state on every boot. Imports now strip
  cluster linkage from incoming events (features are measurements and still
  travel intact; linkage is a local cache the post-merge backfill re-derives)
  — and because re-importing an archive is otherwise a no-op, it doubles as
  the repair: archive rows that already exist locally get their stale
  linkage cleared. **If you imported labels on an earlier dev34 build,
  re-import the same archive once** — the result reports the cleared count
  and the orphan warning disappears on the next start.

- **The leak-watch banner no longer reports a leak that has stopped (dev34)** —
  and it can now be dismissed. The tile showed the most recent night *that
  carried an estimate* out of the last fourteen, with no test of how old that
  night was. When the 2026-07 pump cycling stopped after a valve service, the
  last night with a reading kept winning, and the dashboard went on stating in
  the present tense that ~110 L/day "is leaking somewhere" for six days — with
  a number that a data audit had by then attributed to an irrigation program
  running inside the analysis window, not to a leak. (dev33 stopped such nights
  from writing an estimate, but that only applies going forward; it left the
  already-stored reading on screen.) The estimate must now come from one of the
  **last three evaluated nights**, so three quiet nights clear it — which is
  what a completed repair looks like, and what the tile always claimed it would
  do. A **Dismiss** button covers the rest; it was the only home banner without
  one. Dismissal acknowledges a *reading*, not the feature: a later night with
  a fresh estimate re-shows the tile, so one click can never silence a leak
  that is still being measured, and the Home Assistant alert path is untouched.

- **Vetoed toilet matches now say which physics test rejected them (dev34)** —
  the log line printed the event volume and the home's flush cap, so an event
  turned down by the 2.8 L manufactured-flush floor logged as "vol=2.5 L,
  cap=30.5 L" and read like a passing event. It now names the condition that
  fired (floor, cap, peak flow, or segment count), and the per-event lines
  moved to DEBUG behind a one-line INFO summary per reclassify — the readable
  reasons made the recurring band diagnosable, and it turned out to be the
  veto *working*: the ~90 rejected 2.2–2.8 L events are the dishwasher's
  upper fill-pulse tail (of the user labels in that band, 7 are dishwasher
  and none are toilet; every one has a neighbouring event within 30 minutes,
  clustered at meal times). The 2.8 L floor is what keeps appliance pulses
  from being named flushes — it stays.

- **Startup no longer crash-loops when a recalibration runs during it (dev34)**
  — triggering a heavy admin write (regime recalibration, recompute) while the
  add-on is still starting can hold the SQLite write lock for tens of seconds,
  longer than the 5 s busy timeout. The resulting error crashed the whole
  supervised training task, which restarted every 5 s and collided again for as
  long as the admin job ran (observed live: ~8 cycles). Startup now waits the
  lock out — the defaults it writes are idempotent, and everything else in that
  window was already non-fatal. Recovery previously needed a manual restart.

- **Volume-accounting repairs + pump-ripple exemption — dev33** — driven by a
  full audit of every stored event against raw Home Assistant history
  (2026-08-02). The measurement chain is healthy (±1% per event, 98.1%
  coverage, two different meters agreeing to <1% on fixed-volume fixtures) —
  the errors were all downstream of it. **(1) A 685 L draw counted as zero
  is restored.** A user-labelled 78-minute, 8.7 L/min draw carried a manual
  "drip" verdict, so 2026-07-12 reported 588 L instead of 1,266 L. Root
  cause: the History modal pre-checked its classification boxes from the
  row's *automatic* flags and posted them back as manual verdicts on any
  save — so opening an event and changing anything could silently promote an
  auto flag into a permanent one. The checkboxes now only submit when you
  actually toggle one, **a real fixture label now reverses any zeroing
  verdict** (the UI has always said "relabel if wrong" — now that works for
  drip/phantom/cross-talk, not just irrigation cross-talk), and a one-shot
  repair restores past rows that carry a real label *and* real-water evidence
  (≥1 L above the meter's registration floor), listing anything ambiguous for
  you to relabel rather than guessing. **(2) Degraded events can no longer
  report more water than the meter measured.** The pulsing-supply estimator's
  1.5× ceiling was itself the inflation (+105 L live; worst historical case
  2.01 L measured, 335.88 L reported); it is now 1.0×, and the two paths that
  bypassed the cap entirely — the admin re-check sweep and the manual "supply
  pressure" checkbox — go through it. Trade named explicitly: a *partial*
  meter gap can no longer be corrected upward; a total one still is.
  **(3) The booster pump's ~1 Hz control ripple is no longer read as failing
  supply.** It satisfied every pulsing-supply gate while the meter stayed
  accurate, taking degraded events from 0 to 27–68/week — 121 events on the
  live database. Exempted only inside the *pump era*, anchored to a pinned
  `pump_era_start` (migration 20260566) rather than live pump state or the
  current supply regime, because pre-pump genuine pulsing occupies the same
  period band and either of those would re-flag everything on the next
  re-check. Rows freed by this get one fair pass at the pump-recharge
  detector, and the sweep now rebuilds each affected day's totals — it was
  the only one of five that didn't. **(4) Overlap de-duplication stops
  dropping real water.** A wrapper event zeroed as a duplicate although its
  children only began 42 minutes in cost 704.7 L of irrigation on 2026-07-25;
  a wrapper now keeps whatever its children don't account for, and nested /
  same-instant children are counted once (which is where a further 114.6 L of
  double-counting came from). **(5) Classification failures are recorded
  instead of silent** — 933 events had been unmatched with no reason at all,
  making a 12-day cluster outage invisible; unmatched-but-evaluated rows now
  read `no_tier_matched` (and the marker retracts when a later pass matches
  them, so a recovery is as visible as the outage). Also: nightly pump-leak
  estimates are suppressed for any night when the *other* circuit drew water
  inside the analysis window — that, not a fabricating estimator, is what
  produced the "110 L/day" reading on 2026-07-28 (it was the 02:00 irrigation
  program; the 7/26 estimate was correct, and the ~26 L/day loss it measured
  stopped the day after the valve service). Note for anyone reading leak-test
  rates: the system compliance constant (9.5 mL/PSI) reads ~1.5× low against
  measured pump bursts, so `est_leak_ml_min` values are lower bounds until it
  is recalibrated at the leak test's actual closing pressure.

- **Supply-pressure-aware fixture classification — dev32** — the 2026-07
  booster-pump install shifted resting pressure ~46→~54 PSI, sped up flows
  (~P^0.4 on peaks; toilets fill faster, so shorter durations at unchanged
  volume) and quietly degraded recognition: unmatched share of ≥2 L events
  rose 16%→35% because both the k-NN exemplars and the frozen rule bands
  were fitted on old-pressure physics. Four pieces: **(1)** starting supply
  pressure (`pre_event_pressure_psi`) is now a k-NN feature (linear, scale
  1.5 locked by LOO sweep — interior optimum, collapse below 1.0 proves
  it's signal not memorization; tap recall 0.65→0.75, dishwasher
  0.654→0.692, no class regressed), with missing/legacy-zero values
  median-imputed so an unknown pressure is distance-neutral, never a
  phantom "0 PSI" outlier — post-pump events now find post-pump neighbours
  automatically, and self-heal if the pump is ever removed. **(2)** A
  supply-regime tracker (migration 20260564) samples the detector's settled
  idle-line pressure every 10 min into daily medians and detects sustained
  shifts (3-of-4 evaluated days > 5 PSI, pump-detector-style hysteresis);
  first run bootstraps history from stored events — on the real backup it
  reconstructs the pump install at 2026-07-18 unaided. **(3)** Rule
  calibration is now fitted once PER REGIME (migration 20260565,
  `rule_calibration` keyed `(circuit, regime_id)`; the old row lives on as
  regime 0): live events use the current regime's bands, batch reclassify
  resolves each historical event's bands by its own regime, and the locked
  anti-drift baseline philosophy is preserved within each regime.
  **(4)** A dashboard banner ("Water pressure changed — recalibrate?") plus
  a Settings-card action run a one-tap `regime_shift` re-fit + reclassify
  since the shift, with a per-type labels-needed nudge (the dual gate
  still requires 5 explicit labels per type; do-no-harm still drops any
  fit that regresses held-out recall). Nothing adapts silently, and
  firmware leak/trickle safety is untouched.

- **Leak test rework — firmware 3.13.2 + dev31** — driven by a controlled
  drip experiment (2026-07-26: bench runs A/B/D with a measured 214 mL/min
  drip and mid-test toilet flushes). Firmware: a failed test no longer
  raises a safety fault (the water stays ON — a fail is informational; the
  trickle detector still owns auto-shutoff); tests refuse to start while
  water is flowing (result 11) and abort if the meter registers pulses
  before the valve seals (result 12); the settle runs AFTER the closed end
  stop instead of a flat 60 s that also covered travel (new "Leak Test
  Settle Time" number, default 5 s — run B lost 25 of its 27 PSI before
  the baseline existed); at the threshold crossing the decay is classified
  by fall RATE (≥2 PSI/s → result 9 "demand detected"; measured: flapper
  0.04 PSI/s, real 214 mL/min leak 0.37, toilet flush 5–12 — result 9 was
  previously unreachable because the 2 PSI leak threshold always tripped
  before the ~10 PSI burst rule); new "Leak Test Baseline" / "Leak Test
  Close Pressure" sensors expose what the test judged against. Add-on
  (migration 20260563): rows now record the firmware's post-settle baseline
  instead of a pre-close read that folded the close transient + settle loss
  into every drop (a bench row read 21.5 PSI where the monitored decay was
  ~3); new settle-loss / monitored-window / threshold columns; an estimated
  leak rate from decay × per-circuit compliance (mL per PSI of the isolated
  section — 9.5 measured on Main, `sensitivity_config.compliance_ml_psi`),
  rendered as gal/day; a 120 s post-reopen watch measures the refill slug
  (flapper ~0.04 L, pump recharge 0.3–0.5 L, toilet finishing its fill
  3–8 L) and ≥1 L relabels a failure amber "Aborted — water in use" with a
  non-alarm notification; cancelled tests now leave a "Stopped manually"
  row (previously vanished); manual-start pre-check defers when flow is
  live. Phase 5b honesty fix: a test window too short to hold ~3 pump
  recharge cycles now reads "Too short to judge" instead of a green "Pump
  quiet" (run A had two visible recharges and still said quiet); the
  "long enough" yardstick prefers the HOME's learned recharge period
  (`home_profile.pump_detect_period_s`) over the 60 s plausibility floor
  when the window shows no rises at all — field-found 2026-08-02, a
  5.3-minute test on a ~170 s pump earned a green "quiet" it hadn't
  watched long enough to claim (three cycles need 8.5 min).
- **Dismissible failed leak tests + observer-valve guard (dev30)** — a
  failed leak test the user has reviewed and judged benign (test
  interrupted by an add-on update, known coincident draw) can be marked
  "Ignore": it renders amber ("Failed — ignored") instead of red, is
  reversible, and never alters the record (migration 20260562). Also
  hardens the Phase 5b pump cross-check: the observer circuit's sensors
  only see the shared supply while its own valve is open, so if BOTH
  valves are closed the verdict is `not_applicable` — never a false
  "pump quiet / no leak anywhere".
- **Same-circuit overlap guard + one-shot cleanup (dev28)** — one circuit
  can only have one event at a time; overlapping same-circuit events mean
  the same water was recorded twice (live blip-opened "wrapper" events vs
  the importer's tight reconstruction — verified against raw meter flow).
  A guard at the single event-write chokepoint now resolves any overlap a
  new event creates: a wrapper whose span (≥70%) contains the other
  member(s) and whose volume reconciles with theirs (±40%) is zeroed
  through the volume ledger with a new "⧉ Duplicate — not extra water"
  verdict (`overlap_duplicate`); user-labeled wrappers and ambiguous
  partial overlaps are kept and audit-flagged only (over-count + flag
  beats silently dropping real water). Migration 20260561 sweeps existing
  history once (this home: 64 wrappers, ~3.7 kL of double-counted water
  recovered — dominated by a 2.5 h irrigation mega-wrapper whose zone
  events were also recorded individually). Every decision lands in the new
  `overlap_audit` table; the verdict is recompute-durable (same carve-out
  as irrigation cross-talk) and a user relabel restores the water.
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
- **Low-pressure alerts (dev27, pump plan Phase 6)** — 6a: while an
  irrigation zone is flowing, pressure sustained below a per-circuit floor
  (default 25 PSI) for 3 minutes alerts "sprinkler heads may not pop up
  fully" — with a fill grace after zone start AND after any ≥30% flow step
  (multi-zone transitions never hit zero flow), sustain reset on any
  recovery above the floor, and one alert per zone run. Not pump-gated
  (low pressure under load matters on any supply); the "pump may need
  attention" sentence renders only on pump homes. 6b: on ARMED vfd pump
  homes (post-feature supply answer or detected-night evidence — a stored
  pre-feature answer never arms an alert), pressure sustained 5 minutes
  below the pump's floor (user value, or 40 PSI read-time default) fires
  "pump may have lost power/faulted" — unless heavy flow is running, which
  branches to "pump can't keep up with demand" (a maxed-out VFD is not a
  dead pump). A recharge rise inside the window resets it (pump alive).
  The floor gets a measured one-tap suggestion in Settings (quiet-window
  cut-in − 5, from nightly min_psi — works on healthy homes too).
  Migration 20260560; two new alert toggles (Low Pressure While Running,
  Pump Health).
- **Pump-assisted leak detection (dev26, pump plan Phase 5)** — the pump is
  now a leak sensor. 5a: on detected nights the regime worker estimates the
  leak rate from recharge cycles (median slug × spacing, scaled by the
  street-meter calibration factor 1.9 — the home meter registers ~half of
  each slug), stores it in `pump_regime_nightly.est_leak_lpd`, shows a
  dashboard "💧 Leak watch — ~X gal/day" tile, and fires a notify-only
  `pump_leak` alert (new per-type toggle) when the estimate holds ≥20 L/day
  for 3 consecutive evaluated nights or the cycle period shrinks >30%
  week-over-week (leak growing). Transition-only — a persistent leak alerts
  once, not nightly. The copy teaches the valve-bisect that located the
  2026-07 zone-valve leak. 5b: after every valve-closed leak test (pump mode
  only), the add-on checks whether the UNTESTED circuit's pressure kept
  recharge-cycling while this line was isolated — "main passed, but the pump
  kept topping up → the leak is on the other line, upstream of the valve, or
  inside the pump's own check valve" — stored on `leak_test_history`
  (migration 20260559) and shown as a "Pump check" verdict pill in the Leak
  Test History table. A 5b failure can never affect the test's own result.
- **Pump-mode live-detector gates (dev25, Phase 4b)** — in confirmed vfd
  pump mode the live detector stops opening PRESSURE-initiated events while
  the supply is mid-sawtooth: a rolling 60 s peak-to-peak above an
  amplitude-derived gate (max(2.0, 0.15 × the nightly-measured band))
  suppresses pressure starts, killing the blip-opened "wrapper" events that
  swallowed real draws and double-counted their water (the 7/20 10:03
  duplicated flush). Flow starts are untouched and remain the primary
  trigger; firmware trickle detection is independent, so nothing here can
  mask a leak. The pressure-surge phantom rejection is widened to
  effectively-off in pump mode (a recharge upswing during a real event is
  exactly that pattern), and the historical rise-phantom /
  pressure-silent reprocess sweeps now skip pump-mode circuits so they
  can't re-apply verdicts the live path retired. The banner-confirm and
  supply-type routes reload detector gates immediately — no restart. The
  k-NN pressure-feature down-weight stays DEFERRED per the plan (the fit
  paths are era-agnostic and Phase 2c showed no material k-NN regression
  on the labeled set).
- **Pump-recharge storm absorber (dev24, pump plan Phase 4 — first half)** —
  on homes with CONFIRMED pump mode (vfd profile), a new `pump_recharge`
  artifact class claims the brief recharge slugs ("🔄 Pump top-up — not real
  use"): flow-during-rise (positive flow↔pressure correlation) or
  pressure-quiet blips ≤0.6 L / ≤60 s. It REPLACES the two detectors whose
  static-supply premises are false under a pump sawtooth
  (`rising_pressure_phantom`, `pressure_silent_flow`) — a real draw
  coinciding with a recharge upswing can no longer be wrongly zeroed
  (skipping them only ADDS events; leak-safe). The toilet rule's pressure-
  corroboration requirement is waived in pump mode (a flush's ΔP depends on
  where in the pump cycle it lands). Flag resolution: event paths read
  `pump_mode_effective` through a 60 s TTL cache invalidated by the banner/
  supply routes; everything is inert until the user confirms. Offline replay
  on the captured storm: 85–88% event-count reduction with zero labeled-real
  events lost. (Second half — pressure-start suppression during oscillation
  in the live detector — lands separately.)
- **Nightly pump-regime detection + confirmation banner (dev23, pump plan
  Phase 3)** — a supervised background worker analyzes each circuit's
  quiet-hour pressure/flow window from HA history with the study-validated
  cycling detector (`app/pump_regime_math.py`; offline study: 13/13
  pre-install nights negative incl. softener regens, 2/2 positive) and
  records per-night verdicts in `pump_regime_nightly`. Home flag uses
  2-of-3-evaluated-nights hysteresis (7 quiet nights to clear; skipped
  nights invisible) with ANY-circuit aggregation. Detection NEVER silently
  changes behavior: a dashboard banner asks "Booster pump detected — is
  that right?" and only confirming (or the supply-type answer) activates
  pump mode. Dismissals re-prompt only after 30+ evaluated nights of
  continued detection. Quiet-hour inference is shared with the leak-test
  scheduler (`learn_quiet_hour`). Street-meter calibration captured during
  the incident: the home meter registers ~half of each recharge slug
  (factor 1.9), feeding the Phase 5 leak estimator.
- **History no longer starves when hidden artifacts dominate (dev22)** — the
  "Hide not-real-use events" toggle was a post-filter applied AFTER the
  100-row recency limit, so a burst of artifact events (booster-pump recharge
  cycling) crowded real events out of the page (~18 visible). The exclusion
  now lives in the SQL WHERE, so the page always shows the most recent 100
  VISIBLE events; the "N hidden" badge counts hidden rows within the span the
  visible list covers (`get_recent_events(exclude_not_real=)` +
  `count_not_real_events`).
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
