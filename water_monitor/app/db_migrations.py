"""
Database schema version guard — squashed baseline 20260523.

Startup order (confirmed from main.py):
  1. database.py init_db() → _create_schema() creates all tables
  2. run_migrations() is called → verifies/stamps version

Fresh database: tables created by step 1 include all baseline columns
(including signature_source); this module stamps baseline version.

Old pre-squash database: startup fails fast. Delete the DB and restart.
"""
from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_BASELINE_VERSION: int = 20260523
# Version bumps:
#   20260524 — retired text-sensor waveform roles
#   20260525 — added UNIQUE(circuit, start_ts) on events (dedup first)
#   20260526 — degraded-supply guard: new event columns, event_waveforms
#              table, rebuild hourly_volume from events
#   20260527 — per-circuit valve_type column on circuit_profile
#   20260528 — Sprint A orphan repair: fixtures.cluster_backfill_needed
#              column + one-shot repair of orphaned cluster/fixture refs
#   20260529 — Sprint B label propagation: fixture_clusters.suggestion_source
#              column ('heuristic' | 'user_labels' | NULL)
#   20260530 — Sprint C signature matcher: fixture_type_signatures table +
#              events.matched_fixture_type column
#   20260531 — Sprint D taxonomy consolidation: 23 → 8 fixture types;
#              rewrites stored type strings in events, fixtures,
#              fixture_clusters, and clears fixture_type_signatures
#   20260532 — Sprint E pressure-restoration phantom guard:
#              events.is_pressure_restoration_phantom +
#              home_profile.hide_pressure_artifact_events columns; one-shot
#              reprocess zeros phantom volume + reverses hourly_volume
#   20260533 — Sprint F per-category Fixtures rollup: new category_publish
#              table (per-(circuit, fixture_type) HA publish gate). Seeded
#              from MIN(fixtures.publish_to_ha) so any existing off
#              preference carries over to the new category-level gate.
#   20260534 — Sprint H phantom misclassification fix + manual classification:
#              events.user_ignored + events.user_classified columns; one-shot
#              repair un-flags wrongly-flagged phantoms (delta>=2.0) and
#              restores their real volume to hourly_volume + daily_summary.
#   20260535 — Low-flow dribble exclusion: events.is_low_flow_dribble column
#              (non-zeroing training-exclusion flag for brief low-flow
#              trickles) + two indexes backing the label-training and
#              reclassify-backfill queries. Lightweight DDL only; the verdict
#              backfill runs from the startup / import / manual reprocess paths.
#   20260536 — Active-flow features: flow_integral_litres, active_flow_duration_
#              seconds, true_avg_flow_lpm, flow_on_ratio, active_flow_segment_
#              count, flow_cv_on_segments, integration_quality + the volume audit
#              columns (volume_litres_original, volume_recomputed_at). All
#              NULLABLE. Lightweight DDL only; the per-event recompute runs from
#              the startup / import / manual recompute paths after migration.
#   20260537 — Temporal appliance signal: events.cycle_pulse_count (nullable
#              INTEGER, count of similar-volume neighbours within ±45 min). DDL
#              only; the count backfill + cluster re-suggest run from the startup
#              / manual recompute paths after migration.
#   20260538 — Label provenance: events.fixture_label_source (nullable TEXT,
#              'user'/'cycle'/'training'; NULL = legacy/explicit). DDL only — no
#              backfill (NULL is the correct default for pre-existing labels).
#   20260539 — Training-helper capture (2b): training_capture +
#              training_capture_candidates tables. DDL only.
#   20260540 — Cross-talk event category: events.is_cross_talk +
#              home_profile.hide_cross_talk_events. DDL only; the flag backfill
#              runs from the startup / manual recompute paths after migration.
#   20260541 — Match provenance: events.matched_via (nullable TEXT — 'knn',
#              'washer_cycle', 'rule_toilet', 'rule_dishwasher', 'rule_shower',
#              'zone_default'; NULL = legacy/cluster). DDL only — no backfill
#              (NULL is correct for pre-existing matches; the rules tier and
#              reclassify stamp it going forward).
#   20260542 — dev.24: opt-in water-softener config (home_profile.has_water_softener
#              + softener_regen_start + softener_circuit) and the History cycle-rollup
#              grouping key (events.cycle_group_id). DDL only — no backfill (softener
#              off until enabled; cycle_group_id stamped by the next reclassify).
#   20260543 — Phase 2.3 anomaly response: sensitivity_config.anomaly_response
#              (TEXT DEFAULT 'notify') + sensitivity_config.baseline_anomaly_n
#              (INTEGER — event count behind the frozen percentiles, read by the
#              shut-off confidence gate). DDL only; no backfill (defaults are
#              correct — 'notify', and NULL n until the next activation freeze).
#   20260544 — Phase 3 §2 recorder volume reconciliation: events.volume_recorder_litres
#              (REAL — the firmware cumulative-sensor delta for the event; NULL = not
#              reconciled) + sensitivity_config.recorder_reconcile_auto (INTEGER DEFAULT 1
#              — per-circuit auto-correct vs flag-only toggle). DDL only; no backfill
#              (NULL/default are correct; reconciliation fills them going forward).
#   20260545 — dev.38: guarded auto-split opt-in (home_profile.auto_split_enabled)
#   20260546 — runtime per-circuit flow meter: circuit_profile.pulses_per_litre
#              (REAL DEFAULT 396.0) — add-on cache of the firmware PPL number
#              entity; the low-flow floor is derived (60 ÷ ppl). DDL only; the
#              DEFAULT is correct for existing rows (reference turbine).
#   20260547 — RBAC (viewer/operator/admin): operator_users (operator allow-list),
#              admin_ids_cache (last-known-good HA admin set), seen_users (Access
#              page fallback pick-list). DDL only — CREATE TABLE IF NOT EXISTS,
#              idempotent; no backfill (empty allow-list = everyone non-admin is a
#              viewer until promoted).
#   20260548 — composite labeling: events.embedded_fixtures_json (nullable TEXT —
#              JSON array of fixtures found superimposed on a sustained event's
#              waveform by composite_detector; metadata only, never alters volume
#              or the primary label). DDL only; the annotation backfill runs from
#              the startup / manual reprocess reclassify path after migration.
#   20260549 — dev.39: enable auto-hygiene by default — backfill
#              home_profile.auto_split_enabled = 1 (the background over-merged/inflated
#              event re-import, now safe to run by default: reprocess is atomic +
#              dry-run-gated). Value backfill only; no DDL (column exists since 20260545).
#   20260551 — 2026-07 audit Phase 2b: events.phantom_suppression_averted column +
#              one-time re-evaluation of already-zeroed LARGE phantom draws
#              (>= 10 L measured): volume restored through apply_effective_volume,
#              flagged 'suppression_averted' for review. User-classified phantoms
#              are never touched.
#   20260552 — 2026-07 audit Phase 3: home_profile.fingerprint_labeling_enabled
#              (DEFAULT 1) — the tight-fingerprint label-propagation tier toggle.
#              DDL only; the tier reads live labels, no backfill needed (the next
#              reclassify pass stamps matched_via='fingerprint' hits).
#   20260553 — events.review_verdict (TEXT: 'normal'/'unknown'/NULL) — two-option
#              anomaly triage. 'unknown' events are held out of anomaly-baseline
#              refits (fit_usage_baselines). DDL only; existing user_reviewed=1
#              rows keep NULL (= reviewed before verdicts existed).
#   20260554 — rising-pressure phantom detector (dev14): events.flow_pressure_corr
#              (REAL, nullable — Pearson r of flow vs index-binned pressure over the
#              event window; the rise-phantom discriminator) + home_profile.
#              rise_corr_backfill_done (one-shot stamp for the HA-history corr
#              backfill worker). DDL only; the column backfill is the
#              rise_corr_backfill worker, NOT a migration (needs HA fetches).
#   20260555 — toilet physics veto (dev17): home_profile.epa_flush_cap_enabled
#              (DEFAULT 1) — derive the veto's flush-volume ceiling from
#              build_year via the EPA/federal flush-standard eras. DDL only; the
#              veto applies at display/rollup/classify time, no backfill (the
#              next reclassify pass clears vetoed stored toilet matches).
#   20260556 — dev18: 256-pt signatures. No DDL; one-shot rebuild of stored
#              flow/pressure signatures from event_waveforms envelopes where the
#              envelope is finer than the stored signature (long events stop
#              collapsing to rectangles). No-waveform rows keep shorter sigs
#              (all consumers resample on load).
#   20260557 — dev19: edge signatures. events.onset_signature_json /
#              offset_signature_json (TEXT — 32×1 s fixed-time shape cells for
#              the k-NN edge tier) + one-shot backfill from every
#              event_waveforms envelope (the validated configuration).
#   20260562 — dev30: leak_test_history.user_dismissed — user-acknowledged
#              failed leak tests (benign causes: update interrupted the
#              test, known coincident draw) render amber instead of red.
#              Display-only flag; DDL only.
#   20260561 — dev28: overlap-guard cleanup (plan overlap-guard-invariant).
#              One-shot sweep over history for same-circuit overlapping
#              events (same water recorded twice — ~127 L in the 2026-07
#              pump incident): wrapper events whose span+volume reconcile
#              with their contained members are zeroed through the ledger
#              chokepoint with mrr='overlap_duplicate'; user-labeled and
#              ambiguous cases are audit-flagged only. overlap_audit table +
#              the (circuit, start_ts, end_ts) index ship via _create_schema
#              (new objects need no DDL migration); idempotent.
#   20260560 — dev27: pump plan Phase 6b. sensitivity_config.
#              pump_low_pressure_alert_psi (DEFAULT NULL — NULL resolves the
#              per-supply default at read time; only explicit user action
#              writes a value, which doubles as the arming rule's
#              "user-set floor" signal) + pump_regime_nightly.min_psi (the
#              quiet-window pressure floor ≈ pump cut-in — feeds the
#              suggested-floor hint). DDL only.
#   20260559 — dev26: pump plan Phase 5b. leak_test_history gains the
#              cross-circuit pump verdict columns (other_circuit_cycles,
#              other_circuit_period_s, pump_verdict) — during a valve-closed
#              leak test on circuit A, recharge cycling observed on the
#              UNTESTED circuit B means the leak is on the other line /
#              upstream / inside the pump's own check valve. DDL only.
#   20260558 — dev21: pump-aware detection Phase 1 (plan
#              yes-write-up-the-elegant-kettle). home_profile: pump_mode_detected
#              /_at, pump_detect_period_s, pump_mode_ack, pump_profile,
#              supply_type_set_at (answer provenance — the alert arming rule
#              must not trust pre-feature supply answers), pump_alert_armed_at
#              (persisted arming stamp). sensitivity_config: pump_mode
#              ('auto'|'on'|'off' per-circuit override), low_pressure_alert_psi
#              (irrigation under-load floor, default 25). DDL only, no backfill.
#   20260563 — leak test measures the right interval and reports a rate.
#              leak_test_history: closed_psi, settle_loss_psi, monitor_minutes,
#              threshold_psi, est_leak_ml_min, post_restore_volume_l,
#              draw_verdict; sensitivity_config.compliance_ml_psi (mL per PSI
#              of the isolated section, calibrated from the reopen refill).
#              baseline_psi was previously read BEFORE the valve closed, so
#              every row carried the close transient plus the settle-phase
#              loss. DDL only — historical rows cannot be corrected.
#   20260564 — supply-pressure regime tracking: supply_pressure_daily (daily
#              settled-pressure median/p10/p90 per circuit) + supply_regime
#              (discrete supply-band intervals; a booster-pump install or
#              removal opens a new regime instead of silently degrading
#              classification). Table-create only, no backfill — the tracker
#              worker bootstraps history from events.pre_event_pressure_psi
#              on first run.
#   20260565 — rule_calibration rebuilt with PRIMARY KEY (circuit, regime_id):
#              rule bands are fitted once PER SUPPLY REGIME. The existing row
#              is copied as regime_id=0 (legacy fallback), so behavior with no
#              regimes recorded is bit-identical to before.
#   20260566 — home_profile.pump_era_start: the PINNED start of this home's
#              booster-pump era. Retroactive pump-era sweeps (the VFD-ripple
#              exemption) gate on it instead of live pump state or the current
#              regime, so neither a gate flip nor a later supply transition can
#              re-flag events that were already exempted. DDL only; resolved
#              lazily by supply_regime.pump_era_start.
#   20260567 — home_profile.leak_watch_ack: the night a user dismissed on the
#              leak-watch tile ('dismissed:<night_date>'). The tile was the one
#              home banner with no dismiss control, and it had no age bound
#              either — it showed the newest night carrying an estimate out of
#              the last 14, so a single stale reading stayed on screen for two
#              weeks after the cycling stopped. DDL only.
#   20260568 — training_state.cluster_features_mode ('full' | 'pressure_blind'):
#              which feature space this circuit's cluster centers were seeded
#              in. Persisted because the startup replay must rebuild the SAME
#              space the centers were learned in — replaying pressure-blind
#              centers with pressure features on shifts every distance and
#              breaks the id-map rebuild. Set by the pump-era cluster re-seed.
#   20260569 — baseline_snapshot table: the frozen usage baseline + anomaly
#              percentiles as they stood before each freeze, so a regime refit
#              that lands badly is revertable. Table-create only.
#   20260570 — events.leak_test_id: provenance for the reopen-refill verdict
#              (the add-on's own leak test cycling the valve logs a short flow
#              burst that is neither fixture use nor a sensor phantom). DDL plus
#              a one-time backfill over leak_test_history.
#   20260571 — one day boundary. volume_snapshots.last_reading (high-water mark
#              per period, so a meter reset carries the period's volume over
#              instead of zeroing the dashboard's TODAY tile) plus
#              home_profile.daily_summary_tz (which zone daily_summary rows are
#              bucketed in — the rows themselves move from the UTC day to the
#              home-local day, rebuilt by the orchestrator once HA has answered
#              with the timezone). DDL only.
#   20260572 — sawtooth pump-recharge backfill: one-shot re-verdict of stored
#              pump-era events under the widened third prong of
#              _detect_pump_recharge (slow-decay pressure-triggered restart
#              slugs the 2026-08 micro-event audit surfaced). Data-only.
#   20260573 — waveform claim ledger + mis-attachment repair audit:
#              events.waveform_boot_id (completes the firmware-capture identity
#              so one capture can enrich only one event), the
#              *_pre_repair audit trio, wf_repair_at / wf_repair_verdict, and
#              idx_events_wf_claim. DDL only — the repair sweep itself runs as
#              the wf_repair_backfill worker after boot.
#   20260574 — pump_regime_nightly.window_start_ts / window_end_ts: the UTC
#              bounds of the analyzed quiet window, so the leak-watch banner
#              can say WHEN the cycling was observed ("between 1:05 and 2:11
#              AM") instead of the ambiguous "night of <date>". DDL only;
#              old rows stay NULL and the banner falls back to date-only copy.
#   20260801 — dev38 audit-fix DDL, all in one step: events.time_features_tz
#              (deferred local-time feature backfill marker) +
#              events.registration_est_litres (annotate-only meter-registration
#              estimate); event_waveforms per-channel source metadata
#              (flow/press _src_n, _src_hz) for an honest waveform time axis;
#              overlap_audit.stale_reason (dangling refs are MARKED, never
#              deleted); leak_test_history measurement-provenance columns
#              (baseline/final read timestamps, final_window_s,
#              sustained_drop_psi, monitor_started_at); daily_summary_dirty
#              table (days needing a summary recompute).
#   20260802 — data backfill: raise peak_flow_lpm to ceil(true_avg*1000)/1000
#              where true_avg_flow_lpm > peak_flow_lpm (825 physically
#              impossible software-sourced rows; live path now clamps too).
#   20260803 — data backfill: recompute hydraulic_resistance = ΔP/avg on
#              ESP-enriched rows (1,324 rows carried the pre-enrichment ΔP
#              ratio; the finalize/enrich paths now keep it current).
#   20260804 — data retro-fix: NULL the contaminated (foreign-draw) signatures
#              + signature_source on the 31 dev37 'misattached' rows the
#              repair sweep left labelled esp_* (their signature bytes came
#              from the mis-attached ESP capture; envelopes already deleted).
#   20260805 — dev40 training quarantine: events.training_quarantine_reason /
#              training_quarantined_at + backfill flagging unreviewed machine
#              dishwasher-cycle labels (pre-outage + post-reseed windows) out
#              of every training/exemplar pool (measured 9/19 and 1/10
#              precision on user reviews; the labels had widened the fitted
#              DW band 3.75→8.32 LPM). Labels/verdicts/volumes untouched.
#   20260806 — dev41 quarantine sweep: flag ALL remaining unreviewed machine
#              dishwasher-cycle labels (no time bounds — the 20260805
#              mid-window exemption protected nothing, since re-attribution
#              touches cluster ids, never labels; ~48 pre-July rows from the
#              same over-firing gate ride along). Distinct reason string
#              'dev40_precision_quarantine_sweep'; lift is reason-agnostic.
#   20260807 — dev41 conformance-review DDL: other_valve_open provenance,
#              registration_curve_version, leak-test measurement-quality
#              columns (sustainedness/status/noise/raw samples),
#              overlap_audit.stale_at, utility_register_readings,
#              meter_anchor_points, registration_curve (v1 seeded
#              'unvalidated' from the audit inversion).
#   20260808 — dev42: training_state.reseed_in_progress marker (F-C2) — a
#              crashed re-seed leaves it set; boot warns until a rerun.
#   20260809 — dev46: events.training_excluded_by_user (46f), events
#              flow_sig_span_s / pressure_sig_span_s (46i), and
#              circuit_profile.winterized (46h).
#   20260810 — dev46 (46k): events.verdict_stamp + training_state
#              .last_full_reclassify_at — lets the boot reclassify SKIP an
#   20260813 — dev49 (P0-4): mark daily_summary days that drifted from events
#               (markers only — the shipped drain does the recompute).
#   20260814 — dev50: events.split_evaluated_at / split_evaluation_outcome (the
#               over-merge job's decision memo) + stale_reason / stale_at on
#               anomaly_shutoff_log and cross_talk_audit.
#   20260815 — dev51: the model referee's tables — referee_benchmark +
#               referee_benchmark_meta (the pinned frozen benchmark, imported
#               once via Dev Tools; its ids never enter the repo) and
#               retrain_ledger (every referee decision, durably — the jobs
#               table prunes after two days).
#   20260812 — dev48: events.flow_plateau_lpm + waveform backfill.
#   20260811 — dev47 (47i): fixture health baselines, nightly stats and
#               alerts (fixture_baseline / fixture_health_stat /
#               fixture_health_alert).
#              event whose verdict provably cannot have changed.
#
# VERSION-NUMBER CONVENTION (2026-08-12, decided with the operator): from the
# next migration onward, versions are YYYYMM + a 2-digit per-month sequence —
# the NEXT one is 20260801 (August 2026, #01), then 20260802, and September
# rolls to 20260901. The historical 202605xx run reads the same way with the
# month stuck at 05 (it drifted into a plain sequence); everything stays
# strictly increasing, so stamped DBs walk forward unchanged. Never reuse or
# reorder a shipped number.
#
# EXCEPTION ON THE RECORD: 20260819 landed in SEPTEMBER but reused August's
# prefix. It stays as-is because it shipped and stamped live databases, and
# "never reuse or reorder a shipped number" outranks tidiness — renumbering it
# would make those DBs fail the _UPGRADEABLE_VERSIONS check below and be told to
# delete themselves. 20260901 followed it (September 2026, #01), 20260902 after
# that; THE NEXT MIGRATION IS 20260903.
_CURRENT_VERSION: int = 20260902
# Intermediate stepping-stone version for the dedup-then-unique-index
# migration. Existing DBs at this version have had their wf rows dropped
# but still need the unique index applied.
_VERSION_PRE_UNIQUE_INDEX: int = 20260524
# Intermediate stepping-stone for the degraded-supply migration. Existing
# DBs at this version have the unique index but lack the degraded columns.
_VERSION_PRE_DEGRADED: int = 20260525

# Roles removed when the firmware switched waveform delivery from 5 chunked
# text sensors to a single HA event (firmware 3.8.0). Old DBs may still carry
# circuit_entity_map rows for these — the migration deletes them so the
# discovery wizard doesn't display stale entries.
_RETIRED_WF_ROLES: tuple = (
    "wf_start_flow_sensor",
    "wf_start_pressure_sensor",
    "wf_full_flow_sensor",
    "wf_full_pressure_sensor",
    "wf_metadata_sensor",
)


def _drop_retired_wf_entity_map_rows(conn: sqlite3.Connection) -> None:
    placeholders = ",".join("?" * len(_RETIRED_WF_ROLES))
    cur = conn.execute(
        f"DELETE FROM circuit_entity_map WHERE role IN ({placeholders})",
        _RETIRED_WF_ROLES,
    )
    conn.commit()
    if cur.rowcount:
        log.info("Removed %d stale waveform text-sensor row(s) from circuit_entity_map",
                 cur.rowcount)


def _apply_unique_events_index(conn: sqlite3.Connection) -> None:
    """Add UNIQUE(circuit, start_ts) on events after deduping any historical
    duplicates left behind by older code paths.

    The dedup pass runs first because a CREATE UNIQUE INDEX would fail with
    IntegrityError if the existing data has duplicate (circuit, start_ts)
    pairs. dedup_events keeps the most recently inserted row (MAX rowid),
    clears stale cluster_id on contested groups, and recomputes UUID5 ids.
    Idempotent on its own.

    The non-unique idx_events_circuit_ts is also dropped — its sole purpose
    was the index range scan that the new unique index now serves.
    """
    # Import here so the module remains importable without database.py side
    # effects during test collection.
    from .database import dedup_events
    removed = dedup_events(conn, commit=False)
    if removed:
        log.info(
            "Migration: dedup_events removed %d duplicate row(s) before "
            "applying UNIQUE(circuit, start_ts)", removed,
        )
    conn.execute("DROP INDEX IF EXISTS idx_events_circuit_ts")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_circuit_start_unique "
        "ON events (circuit, start_ts)"
    )
    conn.commit()


def _apply_degraded_supply_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260526.

    Adds 7 new columns on events, creates idx_events_degraded, creates the
    event_waveforms table + index, backfills volume_litres_effective and
    hourly_volume_applied_* for existing rows, then REBUILDS hourly_volume
    from events as source of truth.

    Rebuild filter is INTENTIONALLY broad — every event with a positive
    volume contributes, including those with excluded_from_training=1.
    Clustering exclusion is NOT the same as volume exclusion; degraded
    events still count toward water-usage totals (with their estimated
    value).

    Idempotent on its own (column adds are guarded by _has_column; the
    backfill UPDATEs only touch rows with NULL/0 in the new fields).
    """
    new_cols = (
        ("degraded_supply",               "BOOLEAN DEFAULT 0"),
        ("volume_litres_estimated",       "REAL"),
        ("volume_litres_effective",       "REAL"),
        ("volume_estimation_method",      "TEXT DEFAULT 'raw'"),
        ("hourly_volume_applied_litres",  "REAL DEFAULT 0"),
        ("hourly_volume_applied_bucket",  "TEXT"),
        ("degraded_diagnostic_json",      "TEXT"),
    )
    _add_columns(conn, "events", new_cols)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_degraded "
        "ON events (circuit, start_ts) WHERE degraded_supply = 1"
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS event_waveforms (
            event_id              TEXT PRIMARY KEY
                                  REFERENCES events(id) ON DELETE CASCADE,
            flow_min_json         TEXT NOT NULL,
            flow_max_json         TEXT NOT NULL,
            pressure_min_json     TEXT NOT NULL,
            pressure_max_json     TEXT NOT NULL,
            duration_seconds      REAL NOT NULL,
            created_at            TEXT NOT NULL
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_waveforms_created "
        "ON event_waveforms (created_at)"
    )
    conn.commit()

    # Backfill effective volume + method for existing rows BEFORE the
    # rebuild reads from this column.
    conn.execute(
        "UPDATE events "
        "SET volume_litres_effective = COALESCE(volume_litres, 0), "
        "    volume_estimation_method = 'raw' "
        "WHERE volume_litres_effective IS NULL"
    )

    # Backfill applied bookkeeping so future re-imports subtract correctly.
    # hour_ts format must match _hour_bucket_for() in database.py:
    # '%Y-%m-%dT%H:00:00' UTC, no tz suffix.
    conn.execute(
        "UPDATE events "
        "SET hourly_volume_applied_litres = "
        "      COALESCE(volume_litres_effective, volume_litres, 0), "
        "    hourly_volume_applied_bucket = "
        "      strftime('%Y-%m-%dT%H:00:00', start_ts) "
        "WHERE hourly_volume_applied_bucket IS NULL"
    )
    conn.commit()

    # Rebuild hourly_volume from events as the source of truth.
    # CRITICAL: no excluded_from_training filter — degraded events still
    # count toward volume totals.
    #
    # Temp-table swap pattern: build the new rows into a TEMP table
    # first, only THEN clear hourly_volume and copy across. Anything that
    # raises before the final COMMIT is rolled back atomically, leaving
    # the original hourly_volume intact. If the process dies mid-rebuild,
    # SQLite's transaction durability does the same thing automatically.
    try:
        conn.execute(
            "CREATE TEMP TABLE hourly_volume_rebuild AS "
            "SELECT circuit, "
            "       strftime('%Y-%m-%dT%H:00:00', start_ts) AS hour_ts, "
            "       SUM(COALESCE(volume_litres_effective, volume_litres, 0)) "
            "         AS volume_litres "
            "FROM events "
            "WHERE start_ts IS NOT NULL "
            "GROUP BY circuit, strftime('%Y-%m-%dT%H:00:00', start_ts)"
        )
        conn.execute("DELETE FROM hourly_volume")
        conn.execute(
            "INSERT INTO hourly_volume (circuit, hour_ts, volume_litres) "
            "SELECT circuit, hour_ts, volume_litres "
            "FROM hourly_volume_rebuild"
        )
        conn.execute("DROP TABLE hourly_volume_rebuild")
        conn.commit()
    except Exception:
        conn.rollback()
        # Best-effort cleanup — DROP IF EXISTS so rerun is safe.
        try:
            conn.execute("DROP TABLE IF EXISTS hourly_volume_rebuild")
            conn.commit()
        except Exception:
            pass
        raise
    log.info("Migration 20260526: rebuilt hourly_volume from events; "
             "added 7 degraded-supply columns + event_waveforms table")


def _apply_valve_type_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260527.

    Adds circuit_profile.valve_type with DEFAULT '2_port'. Idempotent —
    column add guarded by _has_column. The DEFAULT clause on ADD COLUMN
    gives all existing rows the value automatically; a defensive backfill
    afterward handles any oddly migrated DB where the new column ended up
    NULL or empty.
    """
    _add_columns(conn, "circuit_profile",
                 (("valve_type", "TEXT DEFAULT '2_port'"),))
    # Defensive backfill — handles legacy / hand-altered rows.
    conn.execute(
        "UPDATE circuit_profile SET valve_type = '2_port' "
        "WHERE valve_type IS NULL OR valve_type = ''"
    )
    conn.commit()


def _apply_signature_matcher(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260530 — Sprint C signature matcher.

    Adds two artefacts:

      1. ``fixture_type_signatures`` table (per-circuit, per-fixture-type
         centroid learned from user-labelled events). The legacy
         ``fixture_signatures`` table (per-fixture, per-feature) was never
         populated by any code path; it's left in place for backup-restore
         compat but the matcher reads from the new table.

      2. ``events.matched_fixture_type`` column — populated when the
         signature matcher tags an event with a fixture_type, independent
         of cluster_id.

    Idempotent — both creates are guarded.
    """
    conn.execute(
        """CREATE TABLE IF NOT EXISTS fixture_type_signatures (
            circuit       TEXT NOT NULL,
            fixture_type  TEXT NOT NULL,
            centroid      TEXT NOT NULL DEFAULT '{}',
            member_count  INTEGER NOT NULL DEFAULT 0,
            created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (circuit, fixture_type)
        )"""
    )
    # (idx_type_signatures_circuit was created here; removed with migration
    #  20260902, which also DROPs it — PRIMARY KEY (circuit, fixture_type)
    #  already indexes `circuit` as its leading column. Removing it from
    #  database.py alone would have been a silent no-op, because this line
    #  re-created it on every forward walk.)
    _add_columns(conn, "events", (("matched_fixture_type", "TEXT"),))
    conn.commit()
    log.info("Migration 20260530: signature-matcher infrastructure ready")


def _apply_fixture_taxonomy_consolidation(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260531 — Sprint D taxonomy consolidation.

    Collapses the old 23-entry fixture type list down to 8 coarse types.
    Applies LEGACY_TYPE_REMAP (from fixtures.py) to every stored type string
    in four columns, then clears fixture_type_signatures so that centroids
    are rebuilt against the new type names on the next user label save.

    This is a pure data migration — no schema changes. Idempotent: old type
    strings no longer appear after the first run, so subsequent UPDATEs
    affect zero rows.
    """
    from .fixtures import LEGACY_TYPE_REMAP

    cols_tables = [
        ("events",           "user_fixture_type"),
        ("events",           "matched_fixture_type"),
        ("fixtures",         "fixture_type"),
        ("fixture_clusters", "suggested_type"),
    ]

    total_updated = 0
    for old_type, new_type in LEGACY_TYPE_REMAP.items():
        if old_type == new_type:
            continue  # nothing to rewrite
        for table, col in cols_tables:
            cur = conn.execute(
                f"UPDATE {table} SET {col} = ? WHERE {col} = ?",
                (new_type, old_type),
            )
            total_updated += cur.rowcount

    conn.commit()

    # Clear signatures — centroids were computed against old type strings
    # and will be rebuilt on the next label save or on demand.
    cur = conn.execute("DELETE FROM fixture_type_signatures")
    sig_count = cur.rowcount
    conn.commit()

    log.info(
        "Migration 20260531: taxonomy consolidation rewrote %d stored type "
        "value(s) across 4 columns; cleared %d fixture_type_signatures row(s)",
        total_updated, sig_count,
    )


def _apply_phantom_event_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260532 — Sprint E phantom guard.

    Adds two columns:
      1. ``events.is_pressure_restoration_phantom`` — flags events matching
         the long-duration + near-zero-pressure-drop fingerprint of a city-
         pressure-restoration artifact.
      2. ``home_profile.hide_pressure_artifact_events`` — backs the Settings
         toggle that hides flagged events from the History list.

    After the column adds, runs ``reprocess_pressure_restoration_phantoms``
    to retroactively flag existing events and reverse their hourly_volume
    contributions so historical daily totals shed the false volume.

    Idempotent — both column adds are guarded by ``_has_column``; the
    reprocess helper skips already-flagged events.
    """
    _add_columns(conn, "events",
                 (("is_pressure_restoration_phantom", "INTEGER DEFAULT 0"),))
    _add_columns(conn, "home_profile",
                 (("hide_pressure_artifact_events",
                   "INTEGER NOT NULL DEFAULT 0"),))
    conn.commit()

    # Lazy import — keeps this module importable without feature_extractor
    # side effects during test collection.
    from .feature_extractor import reprocess_pressure_restoration_phantoms
    result = reprocess_pressure_restoration_phantoms(conn)
    log.info(
        "Migration 20260532: phantom guard ready; flagged %d existing event(s)",
        result.get("flagged", 0),
    )


def _apply_category_publish_table(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260533 — Sprint F category rollup.

    Creates the ``category_publish`` table (per-(circuit, fixture_type) HA
    publish gate) and seeds it from existing confirmed fixtures so any
    previously-disabled HA entity stays disabled under the new model.

    Seeding rule: ``publish_to_ha = MIN(fixtures.publish_to_ha)`` across each
    (circuit, fixture_type). MIN, not MAX, so if the user previously
    silenced ANY fixture in a category, the new category gate starts off —
    we never surprise-republish an HA entity the user had disabled.

    Idempotent — CREATE IF NOT EXISTS guards the table; INSERT OR IGNORE
    guards the seed (subsequent runs leave existing rows alone).
    """
    conn.execute(
        """CREATE TABLE IF NOT EXISTS category_publish (
            circuit         TEXT NOT NULL,
            fixture_type    TEXT NOT NULL,
            publish_to_ha   INTEGER NOT NULL DEFAULT 1,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (circuit, fixture_type)
        )"""
    )
    conn.commit()

    # Seed from confirmed fixtures' per-fixture publish flags. MIN preserves
    # any explicit user "off" preference at the category level.
    cur = conn.execute(
        "INSERT OR IGNORE INTO category_publish "
        "  (circuit, fixture_type, publish_to_ha) "
        "SELECT circuit, fixture_type, COALESCE(MIN(publish_to_ha), 1) "
        "FROM fixtures "
        "WHERE confirmed = 1 "
        "  AND fixture_type IS NOT NULL "
        "  AND fixture_type != '' "
        "GROUP BY circuit, fixture_type"
    )
    seeded = cur.rowcount
    conn.commit()
    log.info(
        "Migration 20260533: category_publish table ready; seeded %d "
        "(circuit, fixture_type) row(s) from existing fixtures.publish_to_ha (MIN)",
        seeded,
    )


def _apply_manual_classification_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260534 — Sprint H.

    Adds ``events.user_ignored`` (explicit Ignore/Restore intent, split out of
    the now-derived ``excluded_from_training``) and ``events.user_classified``
    (lock bit guarding a manual classification). Then runs a one-shot repair
    of phantom misclassifications: a real event that got a stale
    ``is_pressure_restoration_phantom=1`` despite ``pressure_delta_psi >= 2.0``
    (e.g. a long shower flagged before its ESP-waveform pressure landed) is
    un-flagged and its real volume restored to hourly_volume + daily_summary.

    Idempotent — column adds guarded by ``_has_column``; the repair skips rows
    that are already consistent and rows the user has manually classified.
    """
    if _add_columns(conn, "events",
                    (("user_ignored", "INTEGER DEFAULT 0"),)):
        # Backfill the explicit Ignore intent from the legacy combined column:
        # rows excluded with no auto reason were excluded by a user Ignore.
        conn.execute(
            "UPDATE events SET user_ignored = 1 "
            "WHERE excluded_from_training = 1 "
            "  AND COALESCE(is_composite, 0) = 0 "
            "  AND COALESCE(degraded_supply, 0) = 0 "
            "  AND COALESCE(is_pressure_restoration_phantom, 0) = 0 "
            "  AND (match_rejection_reason IS NULL "
            "       OR match_rejection_reason = 'excluded_from_training')"
        )
    _add_columns(conn, "events",
                 (("user_classified", "INTEGER DEFAULT 0"),))
    conn.commit()

    from .database import repair_misflagged_phantom_events
    result = repair_misflagged_phantom_events(conn)
    log.info(
        "Migration 20260534: manual-classification columns ready; repaired %d "
        "misflagged phantom event(s), restored %.1f L to totals",
        result.get("repaired", 0), result.get("litres_restored", 0.0),
    )


def _apply_low_flow_dribble_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260535 — low-flow dribble exclusion.

    Adds ``events.is_low_flow_dribble`` (a verdict flag for brief low-flow /
    low-volume / near-zero-pressure trickles) plus two indexes that back the
    label-training and reclassify-backfill queries. (The verdict was originally
    non-zeroing; since 2026-06-19 it also zeroes volume_litres_effective — see
    feature_extractor._finalize_derived_verdicts. This DDL is unaffected.)

    LIGHTWEIGHT DDL ONLY. Unlike the phantom migration above, this does NOT
    import feature_extractor or run a data backfill — the per-event verdict is
    backfilled idempotently from the startup / import / manual reprocess paths
    (``reprocess_event_exclusion_verdicts``) AFTER migration completes, so this
    module stays free of heavy app-logic imports (avoids circular imports).

    Idempotent — the column add is guarded by ``_has_column`` and both indexes
    use ``CREATE INDEX IF NOT EXISTS``.
    """
    _add_columns(conn, "events",
                 (("is_low_flow_dribble", "INTEGER NOT NULL DEFAULT 0"),))
    # Indexes live here (not _create_schema) because idx_events_unlabelled_
    # reclassify references matched_fixture_type, which a baseline DB doesn't
    # have until _apply_signature_matcher runs earlier in the same upgrade.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_training_labels "
        "ON events (circuit, user_fixture_type, excluded_from_training)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_unlabelled_reclassify "
        "ON events (circuit, user_fixture_type, matched_fixture_type)"
    )
    conn.commit()
    log.info("Migration 20260535: low-flow dribble column + reclassify indexes ready")


_ACTIVE_FLOW_NEW_COLUMNS: tuple = (
    ("flow_integral_litres", "REAL"),
    ("active_flow_duration_seconds", "REAL"),
    ("true_avg_flow_lpm", "REAL"),
    ("flow_on_ratio", "REAL"),
    ("active_flow_segment_count", "INTEGER"),
    ("flow_cv_on_segments", "REAL"),
    ("integration_quality", "TEXT"),
    ("volume_litres_original", "REAL"),
    ("volume_recomputed_at", "TIMESTAMP"),
)


def _apply_active_flow_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260536 — active-flow features.

    Adds the timestamped-flow-integral feature columns + the volume-recompute
    audit columns, ALL NULLABLE (NULL = not yet backfilled, distinct from 0 =
    known no flow). LIGHTWEIGHT DDL ONLY — the per-event recompute (from raw HA
    flow history) runs from the startup / import / manual recompute paths after
    migration, so this stays free of heavy app-logic imports.

    Idempotent — each add is guarded by ``_has_column``.
    """
    _add_columns(conn, "events", _ACTIVE_FLOW_NEW_COLUMNS)
    conn.commit()
    log.info("Migration 20260536: active-flow feature columns ready")


_CYCLE_PULSE_NEW_COLUMNS: tuple = (
    ("cycle_pulse_count", "INTEGER"),
)


def _apply_cycle_pulse_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260537 — temporal appliance signal.

    Adds ``events.cycle_pulse_count`` (nullable INTEGER; NULL = not yet computed,
    0 = computed/no qualifying neighbours). LIGHTWEIGHT DDL ONLY — the count
    backfill (``database.recompute_cycle_pulse_counts``) + cluster re-suggest run
    from the startup / manual recompute paths after migration.

    Idempotent — each add is guarded by ``_has_column``.
    """
    _add_columns(conn, "events", _CYCLE_PULSE_NEW_COLUMNS)
    conn.commit()
    log.info("Migration 20260537: cycle_pulse_count column ready")


_LABEL_SOURCE_NEW_COLUMNS: tuple = (
    ("fixture_label_source", "TEXT"),
)


def _apply_label_source_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260538 — label provenance.

    Adds ``events.fixture_label_source`` (nullable TEXT; 'user'/'cycle'/'training';
    NULL = legacy/explicit). LIGHTWEIGHT DDL ONLY — no backfill (NULL is the correct
    default for pre-existing labels, and is protected like a 'user' label).

    Idempotent — each add is guarded by ``_has_column``.
    """
    _add_columns(conn, "events", _LABEL_SOURCE_NEW_COLUMNS)
    conn.commit()
    log.info("Migration 20260538: fixture_label_source column ready")


def _apply_training_capture_tables(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260539 — training-helper capture (2b).

    Creates ``training_capture`` + ``training_capture_candidates`` (+ indexes),
    all ``CREATE TABLE/INDEX IF NOT EXISTS`` so it is idempotent. DDL only.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS training_capture (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            circuit         TEXT NOT NULL,
            fixture_type    TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'armed',
            armed_at        TIMESTAMP NOT NULL,
            expires_at      TIMESTAMP NOT NULL,
            window_minutes  INTEGER,
            captured_count  INTEGER NOT NULL DEFAULT 0,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_training_capture_active "
                 "ON training_capture (circuit, status)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS training_capture_candidates (
            capture_id      INTEGER NOT NULL,
            event_id        TEXT NOT NULL,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_training_capture_candidates "
                 "ON training_capture_candidates (capture_id)")
    conn.commit()
    log.info("Migration 20260539: training_capture tables ready")


def _apply_cross_talk_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260540 — cross-talk event category.

    Adds ``events.is_cross_talk`` (INTEGER, default 0 — a long no-flow event whose
    pressure dropped because *another* circuit drew water: registered but no real
    flow through this meter, so it is zeroed + excluded like a phantom) and
    ``home_profile.hide_cross_talk_events`` (the Settings 'hide from History' toggle,
    mirroring ``hide_pressure_artifact_events``). DDL only; idempotent (each add is
    guarded by ``_has_column``). The flag backfill runs from the startup /
    manual-recompute paths.
    """
    _add_columns(conn, "events",
                 (("is_cross_talk", "INTEGER NOT NULL DEFAULT 0"),))
    _add_columns(conn, "home_profile",
                 (("hide_cross_talk_events", "INTEGER NOT NULL DEFAULT 0"),))
    conn.commit()
    log.info("Migration 20260540: cross-talk columns ready")


def _apply_matched_via_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260541 — match provenance (dev.23 rules tier).

    Adds ``events.matched_via`` (TEXT, nullable): how ``matched_fixture_type`` was
    produced — ``'knn'`` (signature k-NN), ``'washer_cycle'`` (anchor + same-peak
    family detector), ``'rule_toilet'``/``'rule_dishwasher'``/``'rule_shower'``
    (structural event rules), ``'zone_default'`` (zone-circuit fallback), or NULL
    (legacy / cluster-tier / user-labeled rows). Machine-derived — NOT in
    ``_EVENT_USER_COLUMNS`` (recomputed by every reclassify). DDL only; idempotent
    (guarded by ``_has_column``); no backfill — NULL is correct for existing rows.
    The live trailing washer scan is served by the existing
    ``idx_events_circuit_start_unique`` (circuit, start_ts) index, so no new index.
    """
    _add_columns(conn, "events", (("matched_via", "TEXT"),))
    conn.commit()
    log.info("Migration 20260541: matched_via column ready")


def _apply_dev24_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260542 — dev.24.

    Adds the opt-in water-softener config to ``home_profile``:
      • ``has_water_softener``   (INTEGER NOT NULL DEFAULT 0 — the setup opt-in)
      • ``softener_regen_start`` (TEXT, 'HH:MM' local — REQUIRED when enabled)
      • ``softener_circuit``     (TEXT — which circuit the softener is on; Main)
    and the History cycle-rollup grouping key to ``events``:
      • ``cycle_group_id``       (TEXT, nullable — washer anchor id / softener
        session id / dishwasher cycle anchor id; NULL = ungrouped singleton).
    DDL only; idempotent (each add guarded by ``_has_column``). No backfill —
    softener detection is off until enabled, and cycle_group_id is stamped by
    the next reclassify.
    """
    _add_columns(conn, "home_profile", (
        ("has_water_softener",   "INTEGER NOT NULL DEFAULT 0"),
        ("softener_regen_start", "TEXT"),
        ("softener_circuit",     "TEXT"),
    ))
    _add_columns(conn, "events", (("cycle_group_id", "TEXT"),))
    conn.commit()
    log.info("Migration 20260542: dev.24 columns ready")


def _apply_anomaly_response_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260543 — Phase 2.3 anomaly response.

    Adds two ``sensitivity_config`` columns:
      • ``anomaly_response`` (TEXT DEFAULT 'notify') — the graduated response level
        ('off' | 'notify' | 'notify_shutoff_severe' | 'shutoff_any').
      • ``baseline_anomaly_n`` (INTEGER) — event count behind the frozen volume
        percentiles, read by the shut-off confidence gate so a thin/default
        baseline can never close the valve.
    DDL only; idempotent (each add guarded by ``_has_column``). No backfill —
    'notify' is the correct default and ``baseline_anomaly_n`` is written by the
    next activation freeze (NULL until then → shut-off degrades to notify).
    """
    _add_columns(conn, "sensitivity_config", (
        ("anomaly_response",   "TEXT DEFAULT 'notify'"),
        ("baseline_anomaly_n", "INTEGER"),
    ))
    conn.commit()
    log.info("Migration 20260543: anomaly-response columns ready")


def _apply_recorder_reconcile_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260544 — Phase 3 §2 recorder volume reconciliation.

    Adds:
      • ``events.volume_recorder_litres`` (REAL) — the authoritative firmware cumulative
        volume-sensor delta for the event's window (NULL = not yet reconciled / sensor
        unavailable). The audit + what flag-mode review/apply uses.
      • ``sensitivity_config.recorder_reconcile_auto`` (INTEGER DEFAULT 1) — per-circuit
        toggle: 1 = auto-correct the volume from the recorder, 0 = flag-only.
    DDL only; idempotent (each add guarded by ``_has_column``). No backfill — NULL /
    default 1 are correct; the hourly reconcile pass fills them going forward.
    """
    _add_columns(conn, "events", (("volume_recorder_litres", "REAL"),))
    _add_columns(conn, "sensitivity_config",
                 (("recorder_reconcile_auto", "INTEGER DEFAULT 1"),))
    conn.commit()
    log.info("Migration 20260544: recorder-reconcile columns ready")


def _apply_dev38_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260545 — dev.38 guarded auto-split.

    Adds the opt-in flag ``home_profile.auto_split_enabled`` (INTEGER NOT NULL
    DEFAULT 0). DDL only; idempotent. OFF until the user enables it — the first
    automated, destructive split pass must be opt-in.
    """
    # home_profile is created by _create_schema before migrations in every real upgrade;
    # guard for minimal synthetic DBs that walk the chain without it.
    _add_columns(conn, "home_profile",
                 (("auto_split_enabled", "INTEGER NOT NULL DEFAULT 0"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260545: dev.38 auto-split flag ready")


def _apply_ppl_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260546 — runtime per-circuit flow meter.

    Adds circuit_profile.pulses_per_litre with DEFAULT 396.0 (reference turbine).
    This column is the add-on's CACHE of the firmware's runtime PPL number entity
    (firmware is the source of truth); the low-flow floor is derived as 60 ÷ ppl.
    Idempotent — column add guarded by _has_column; the DEFAULT gives existing
    rows 396.0 automatically, with a defensive backfill for legacy NULL/0 rows.
    """
    # circuit_profile is created by _create_schema before migrations in every real
    # upgrade; guard for minimal synthetic DBs that walk the chain without it.
    if _has_table(conn, "circuit_profile"):
        _add_columns(conn, "circuit_profile",
                     (("pulses_per_litre", "REAL DEFAULT 396.0"),))
        # Defensive backfill — handles legacy / hand-altered rows.
        conn.execute(
            "UPDATE circuit_profile SET pulses_per_litre = 396.0 "
            "WHERE pulses_per_litre IS NULL OR pulses_per_litre <= 0"
        )
    conn.commit()


def _apply_rbac_tables(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260547 — role-based access (RBAC).

    Creates the three RBAC tables (``operator_users``, ``admin_ids_cache``,
    ``seen_users``). DDL only, all ``CREATE TABLE IF NOT EXISTS`` so it is
    idempotent and a no-op on a fresh DB (where ``_create_schema`` already made
    them). No backfill — an empty operator allow-list means every non-admin HA
    user is a viewer until an admin promotes them on the Access page.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS operator_users (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            added_by      TEXT,
            added_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_ids_cache (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            cached_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_users (
            user_id       TEXT PRIMARY KEY,
            display_name  TEXT,
            first_seen    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    log.info("Migration 20260547: RBAC tables ready")


def _apply_embedded_fixtures_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260548 — composite (embedded-fixture) labeling.

    Adds ``events.embedded_fixtures_json`` (nullable TEXT). The reclassify path
    populates it with a JSON array of draws found superimposed on a sustained
    event's stored waveform (a toilet flushed mid-shower). Metadata ONLY — it
    never alters the parent event's volume or primary label. Idempotent — the
    column add is guarded by ``_has_column``; no backfill here (the annotation is
    written by ``recompute_embedded_fixtures`` on the next reclassify pass, which
    needs the event_waveforms rows the bare DDL step doesn't touch).
    """
    _add_columns(conn, "events", (("embedded_fixtures_json", "TEXT"),))
    conn.commit()
    log.info("Migration 20260548: embedded_fixtures_json column ready")


def _apply_auto_split_default(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260549 — enable auto-hygiene by default.

    Sets ``home_profile.auto_split_enabled = 1`` on existing rows so the background
    over-merged/inflated-event re-import runs without the user flipping a toggle. It is
    safe to default on as of dev.39: the reprocess is ATOMIC (a failed re-import restores
    the deleted events) and dry-run-gated, and user-labelled events are never touched.
    Value backfill only — the column has existed since 20260545; idempotent.
    """
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='home_profile'"
    ).fetchone()
    if has_table and _has_column(conn, "home_profile", "auto_split_enabled"):
        conn.execute(
            "UPDATE home_profile SET auto_split_enabled = 1 "
            "WHERE COALESCE(auto_split_enabled, 0) = 0")
    conn.commit()
    log.info("Migration 20260549: auto-hygiene enabled by default")


def _apply_cross_talk_audit_table(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260550 — irrigation zone-switch cross-talk.

    Creates ``cross_talk_audit`` — the evidence trail the importer's reconciliation
    pass (``historical_importer._reconcile_irrigation_cross_talk``) writes BEFORE it
    zeroes a main event it has identified as irrigation zone-switch cross-talk. One
    row per action records the pre-zero volume + the pressure-swing evidence so a
    false positive is auditable and reversible. DDL is ``CREATE TABLE IF NOT EXISTS``
    so it is idempotent and a no-op on a fresh DB (``_create_schema`` already made it).

    Also flips ``home_profile.hide_cross_talk_events`` on by default (value backfill,
    one-time): the user opted into hiding zone-switch cross-talk from History, and the
    same toggle already governs the long-no-flow cross-talk category. Guarded so it
    only flips an unset (0) value.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cross_talk_audit (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        TEXT NOT NULL,
            circuit         TEXT NOT NULL,
            reconciled_at   TEXT NOT NULL,
            interval_start  TEXT,
            interval_end    TEXT,
            main_delta_psi  REAL,
            other_delta_psi REAL,
            ratio           REAL,
            volume_litres   REAL,
            action          TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cross_talk_audit_event "
        "ON cross_talk_audit(event_id)")
    has_profile = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='home_profile'"
    ).fetchone()
    if has_profile and _has_column(conn, "home_profile", "hide_cross_talk_events"):
        conn.execute(
            "UPDATE home_profile SET hide_cross_talk_events = 1 "
            "WHERE COALESCE(hide_cross_talk_events, 0) = 0")
    conn.commit()
    log.info("Migration 20260550: cross_talk_audit table ready + cross-talk hidden")


def _apply_phantom_suppression_averted(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260551 — 2026-07 audit Phase 2b.

    1. Adds ``events.phantom_suppression_averted`` (guarded, idempotent).
    2. One-time re-evaluation of ALREADY-zeroed phantom events that carried a
       large measured volume (the silent-suppression case the audit found: a
       real 141 L draw zeroed to 0). Those events get their measured volume
       back — routed through ``apply_effective_volume`` so the hourly ledger
       reverses the zero and applies the restore correctly — and are flagged
       ``suppression_averted`` for review. Mirrors the live guard in
       ``_finalize_derived_verdicts`` (threshold `_PHANTOM_REVIEW_FLAG_LITRES`).
       User-classified phantoms are the user's decision — never touched.
    Leak-safety: restoring volume can never mask a leak (only zeroing could),
    and the firmware trickle sensor is independent of stored events anyway.
    """
    _add_columns(conn, "events",
                 (("phantom_suppression_averted", "INTEGER DEFAULT 0"),))
    # Backfill needs the modern event shape; a stub/ancient DB (pre-verdict
    # columns) has no phantom-zeroed rows to restore — DDL above is enough.
    for _needed in ("circuit", "start_ts", "volume_litres",
                    "volume_estimation_method", "user_classified", "flagged"):
        if not _has_column(conn, "events", _needed):
            conn.commit()
            log.info("Migration 20260551: column added; backfill skipped "
                     "(events table lacks %r)", _needed)
            return
    from .database import apply_effective_volume
    from .feature_extractor import _PHANTOM_REVIEW_FLAG_LITRES
    rows = conn.execute(
        "SELECT id, circuit, start_ts, volume_litres FROM events "
        "WHERE volume_estimation_method = 'pressure_restoration_phantom' "
        "  AND COALESCE(user_classified, 0) = 0 "
        "  AND COALESCE(volume_litres, 0) >= ?",
        (_PHANTOM_REVIEW_FLAG_LITRES,)).fetchall()
    for r in rows:
        vol = float(r["volume_litres"])
        conn.execute(
            "UPDATE events SET volume_litres_effective = ?, "
            "  volume_estimation_method = 'raw', "
            "  is_pressure_restoration_phantom = 0, "
            "  phantom_suppression_averted = 1, "
            "  flagged = 1, anomaly_type = 'suppression_averted', "
            "  anomaly_score = 1.0, match_rejection_reason = NULL, "
            "  excluded_from_training = 1 "
            "WHERE id = ?",
            (round(vol, 3), r["id"]))
        apply_effective_volume(conn, r["id"], r["circuit"], r["start_ts"], vol)
    conn.commit()
    log.info("Migration 20260551: phantom_suppression_averted ready; "
             "%d zeroed large draw(s) restored + flagged for review", len(rows))


def _apply_fingerprint_labeling_flag(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260552 — 2026-07 audit Phase 3.

    Adds ``home_profile.fingerprint_labeling_enabled`` (DEFAULT 1 — the tier
    shipped eval-gated at 96% measured precision). Guarded + idempotent; a
    stub DB without home_profile just gets the version stamp.
    """
    _add_columns(conn, "home_profile",
                 (("fingerprint_labeling_enabled",
                   "INTEGER NOT NULL DEFAULT 1"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260552: fingerprint_labeling_enabled ready (default ON)")


def _apply_review_verdict_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260553 — two-option anomaly triage.

    Adds ``events.review_verdict`` (TEXT, nullable): 'normal' — the user
    confirmed legitimate use; 'unknown' — the user looked but didn't
    recognise the draw (held out of anomaly-baseline refits so it can never
    teach "normal"); NULL — unreviewed, or reviewed before verdicts existed
    (existing user_reviewed=1 rows deliberately keep NULL — their intent
    wasn't recorded and must not be invented). Guarded + idempotent.
    """
    _add_columns(conn, "events", (("review_verdict", "TEXT"),))
    conn.commit()
    log.info("Migration 20260553: events.review_verdict ready")


def _apply_flow_pressure_corr(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260554 — rising-pressure phantom (dev14).

    Adds ``events.flow_pressure_corr`` (REAL, nullable): Pearson correlation of
    the event's flow readings against its index-binned pressure readings —
    strongly negative for real demand (flow pulls pressure DOWN), positive when
    a city-pressure RISE pushed a slug through the turbine (the rise phantom).
    NULL = not computed (pre-dev14 event, or waveforms too short).

    Adds ``home_profile.rise_corr_backfill_done`` (INTEGER NOT NULL DEFAULT 0):
    one-shot stamp for the backfill worker that computes the correlation for
    historical candidate events from HA history. DDL only — the backfill itself
    is a supervised worker (needs HA fetches), never a migration.
    Guarded + idempotent.
    """
    _add_columns(conn, "events", (("flow_pressure_corr", "REAL"),))
    _add_columns(conn, "home_profile",
                 (("rise_corr_backfill_done", "INTEGER NOT NULL DEFAULT 0"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260554: flow_pressure_corr + rise_corr_backfill_done ready")


def _apply_epa_flush_cap_flag(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260555 — toilet physics veto (dev17).

    Adds ``home_profile.epa_flush_cap_enabled`` (INTEGER NOT NULL DEFAULT 1):
    when 1, the toilet veto's flush-volume ceiling is derived from
    ``home_profile.build_year`` via the EPA/federal flush-standard eras
    (pre-1982 ≈ 7 gpf, 1982–1993 ≈ 3.5 gpf, 1994+ ≈ 1.6 gpf); when 0 the
    ceiling falls back to the pre-1982 bound. The 2.8 L floor and the
    single-refill shape veto are structural and unaffected by this flag.
    DDL only. Guarded + idempotent.
    """
    _add_columns(conn, "home_profile",
                 (("epa_flush_cap_enabled", "INTEGER NOT NULL DEFAULT 1"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260555: epa_flush_cap_enabled ready")


def _apply_sig256_rebuild(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260556 — 256-pt signatures (dev18).

    No DDL. One-shot data pass: regenerate stored flow/pressure signatures at
    the new SIGNATURE_POINTS (256) from each event's ``event_waveforms``
    envelope where the envelope is finer than the stored signature — long
    events stop rendering/clustering as rectangles. Events without a waveform
    row keep their shorter signatures (every consumer resamples on load).
    Idempotent (already-256 rows are skipped); measured ~seconds on a
    3.6k-event home. Guarded for stub DBs without the tables.
    """
    has_wf = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_waveforms'"
    ).fetchone()
    if has_wf:
        from .feature_extractor import rebuild_signatures_from_waveforms
        res = rebuild_signatures_from_waveforms(conn)
        log.info("Migration 20260556: signatures rebuilt at 256 pts "
                 "(%d flow / %d pressure of %d scanned)",
                 res["flow_upgraded"], res["pressure_upgraded"], res["scanned"])
    else:
        log.info("Migration 20260556: no event_waveforms table — nothing to rebuild")
    conn.commit()


def _apply_edge_signatures(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260557 — edge signatures (dev19).

    Adds ``events.onset_signature_json`` / ``offset_signature_json`` (TEXT,
    nullable): fixed-time onset/offset shape vectors (32 cells × 1 s) feeding
    the k-NN matcher's new edge tier. Then one-shot backfills them from every
    ``event_waveforms`` envelope (coarse envelopes smear onto the grid — the
    validated configuration). Guarded + idempotent.
    """
    _add_columns(conn, "events", (("onset_signature_json",  "TEXT"),
                                  ("offset_signature_json", "TEXT")))
    has_wf = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_waveforms'"
    ).fetchone()
    if has_wf:
        from .feature_extractor import rebuild_edge_signatures_from_waveforms
        res = rebuild_edge_signatures_from_waveforms(conn)
        log.info("Migration 20260557: edge signatures ready (%d/%d backfilled)",
                 res["edges_filled"], res["scanned"])
    else:
        log.info("Migration 20260557: edge-signature columns ready (no waveforms)")
    conn.commit()


def _apply_suggestion_source_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260529 — Sprint B label propagation.

    Adds ``fixture_clusters.suggestion_source`` (TEXT, nullable). Values:
    ``NULL`` (no suggestion yet), ``'heuristic'`` (set by the centroid
    feature-range rules in cluster_engine), ``'user_labels'`` (set by the
    majority-vote helper in ``database.recompute_cluster_suggestion_from_user_labels``).

    Backfill: clusters that already had a non-NULL ``suggested_type`` get
    ``suggestion_source = 'heuristic'`` — historically that's the only
    code path that could have set it. The new majority-vote helper hasn't
    run yet, so we know nothing in the DB is from user labels.

    Idempotent — column add is guarded by ``_has_column``; the backfill
    only touches rows where ``suggestion_source IS NULL``.
    """
    _add_columns(conn, "fixture_clusters", (("suggestion_source", "TEXT"),))
    conn.execute(
        "UPDATE fixture_clusters SET suggestion_source = 'heuristic' "
        "WHERE suggestion_source IS NULL AND suggested_type IS NOT NULL"
    )
    conn.commit()
    log.info("Migration 20260529: suggestion_source column added + backfilled")


def _apply_orphan_repair(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260528 — Sprint A orphan repair.

    Adds ``fixtures.cluster_backfill_needed`` (INTEGER DEFAULT 0) and
    runs a one-shot pass that:

      1. NULLs ``events.cluster_id`` where the referenced cluster row
         no longer exists (so the next backfill pass re-clusters them).
      2. Flags ``fixtures.cluster_backfill_needed = 1`` for confirmed
         fixtures that have no cluster pointing back at them — surfaces
         the relink banner on the Fixtures page.
      3. NULLs ``fixture_clusters.fixture_id`` where the referenced
         fixture row no longer exists.

    Idempotent — column add is guarded by ``_has_column``; the repair
    helper itself yields zero counts on a second invocation.
    """
    _add_columns(conn, "fixtures",
                 (("cluster_backfill_needed", "INTEGER DEFAULT 0"),))
    conn.commit()

    # Lazy import — keeps this module importable without database.py
    # side effects during test collection.
    from .database import find_orphaned_cluster_references
    counts = find_orphaned_cluster_references(conn, repair=True)
    total = sum(counts.values())
    if total:
        log.info(
            "Migration 20260528: orphan-repair fixed %d event(s), flagged "
            "%d unbacked fixture(s), nulled %d dangling cluster fixture_id(s)",
            counts["events_orphaned"],
            counts["fixtures_unbacked"],
            counts["clusters_dangling"],
        )
    else:
        log.info("Migration 20260528: orphan-repair found nothing to fix")


def _get_version(conn: sqlite3.Connection) -> int:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _schema_version (
            version INTEGER NOT NULL DEFAULT 0
        )""")
    row = conn.execute("SELECT version FROM _schema_version").fetchone()
    if not row:
        conn.execute("INSERT INTO _schema_version VALUES (0)")
        conn.commit()
        return 0
    return row[0]


def _set_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute("UPDATE _schema_version SET version = ?", (version,))
    conn.commit()


# ---------------------------------------------------------------------------
# Column lookups — one PRAGMA per table per schema change, not one per question.
# ---------------------------------------------------------------------------
# The chain asks `_has_column` 106 times on an ordinary boot (a DB already at
# the current version, re-verifying every column it must have), and every
# question used to build and throw away its own `PRAGMA table_info` result set —
# 122 rows of it for `events`, 160 µs a call. Measured on this schema:
#
#     ordinary boot   106 questions / 15 tables : 106 PRAGMAs -> 14
#     full walk from the baseline               : 259 PRAGMAs -> 40
#     the boot guard itself                     : 11.1 ms -> 1.65 ms
#
# `PRAGMA schema_version` costs 1.5 µs against those 160, which is what makes
# re-validating on every question affordable.
#
# ⛔ THE DANGER, WRITTEN DOWN: MIGRATIONS ADD COLUMNS AS THEY RUN. A cache that
# answers a stale "that column is missing" makes a later step re-run an ALTER
# that already happened, or run a backfill guarded on the column being new —
# i.e. it corrupts the schema in the middle of the chain, which is the one place
# in this codebase where a wrong answer costs the user their database. Two
# INDEPENDENT guards, either sufficient on its own:
#
#   1. `PRAGMA schema_version` is SQLite's own schema cookie. It increments on
#      every schema change — ALTER ADD/DROP COLUMN, CREATE/DROP TABLE or INDEX,
#      RENAME — and never on plain DML (verified on the 3.39 this ships with).
#      The snapshot carries the cookie it was read at and is dropped whole the
#      moment the cookie moves, so a change made by ANY code path — this module,
#      database.py, a helper nobody remembered — invalidates it. Nothing has to
#      remember to call an invalidate function; that is the point.
#   2. A snapshot may only ever answer TRUE. "Column is missing" always re-reads
#      the PRAGMA first, so the stale-False failure above is unreachable even if
#      guard 1 were wrong somewhere (a SQLite build that does not bump the
#      cookie, say). Missing-column answers therefore cost exactly what they
#      cost today; present-column answers — the every-boot case, where the
#      schema is already current — become a dict lookup.
#
# The snapshot holds ONE connection at a time, by strong reference, compared
# with `is`. Keying on `id(conn)` would be a correctness bug rather than a style
# one: sqlite3.Connection supports neither weak references nor attributes, ids
# are recycled once a connection is freed, and cookie values are small integers
# that collide readily across the many databases one test run builds.
_SNAPSHOT_LOCK = threading.Lock()
_COLUMN_SNAPSHOT: dict = {"conn": None, "cookie": None, "tables": {}}


def _schema_cookie(conn: sqlite3.Connection) -> Optional[int]:
    """SQLite's schema cookie, or None when it cannot be read (→ no caching)."""
    try:
        row = conn.execute("PRAGMA schema_version").fetchone()
    except sqlite3.Error:                   # pragma: no cover - defensive
        return None
    return int(row[0]) if row else None


def _table_columns(conn: sqlite3.Connection, table: str,
                   _force: bool = False) -> frozenset:
    """Column names of ``table`` — from the snapshot when it is still valid."""
    cookie = _schema_cookie(conn)
    with _SNAPSHOT_LOCK:
        snap = _COLUMN_SNAPSHOT
        if snap["conn"] is not conn or snap["cookie"] != cookie:
            snap["conn"], snap["cookie"], snap["tables"] = conn, cookie, {}
        cols = None if _force else snap["tables"].get(table)
        if cols is None:
            cols = frozenset(
                row[1] for row in conn.execute(f"PRAGMA table_info({table})"))
            if cookie is not None:
                snap["tables"][table] = cols
    return cols


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if column in _table_columns(conn, table):
        return True
    # Guard 2: never answer "missing" out of the snapshot.
    return column in _table_columns(conn, table, _force=True)


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def _add_columns(conn: sqlite3.Connection, table: str, columns,
                 *, if_table_exists: bool = False) -> list:
    """ADD COLUMN every ``(name, decl)`` pair ``table`` is missing.

    Returns the columns actually added, so a caller whose backfill must run
    only for a NEWLY added column can keep that coupling explicit.

    This is the preamble 48 migrations wrote out by hand — guard on
    ``_has_column``, ``ALTER TABLE … ADD COLUMN``, sometimes log it, sometimes
    not. Twenty of them looped, twenty-eight repeated the block per column.

    ``if_table_exists=True`` reproduces the ``sqlite_master`` probe the later
    migrations wrote in front of their adds. It is NOT the default, and the
    difference matters: a step that raises today on a missing table must keep
    raising, because the chain stamps the schema version only after every step
    RETURNS. A step that silently skipped instead would let the DB stamp itself
    current with the column absent, and the next boot's guard answers that with
    "Delete the database file and restart the add-on."
    """
    if if_table_exists and not _has_table(conn, table):
        return []
    added = []
    for col, decl in columns:
        if _has_column(conn, table, col):
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        log.info("Added %s.%s (%s)", table, col, decl)
        added.append(col)
    return added


# ---------------------------------------------------------------------------
# The waveform-claim index — the one index every tail migration re-adds.
# ---------------------------------------------------------------------------
_WF_CLAIM_INDEX_COLUMNS: tuple = ("circuit", "waveform_boot_id",
                                  "waveform_event_id")
_WF_CLAIM_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_events_wf_claim "
    "ON events (circuit, waveform_boot_id, waveform_event_id)"
)


def _ensure_wf_claim_index(conn: sqlite3.Connection) -> None:
    """Create ``idx_events_wf_claim`` when the columns it covers exist.

    ⛔ THIS INDEX MUST NOT MOVE INTO THE SCHEMA DDL SCRIPT. ⛔
    ``_create_schema`` / ``schema.sql`` runs against UPGRADE databases too, and
    it runs BEFORE any migration. The index covers ``events.waveform_boot_id``,
    a column that only arrives with migration 20260573, so an index statement in
    the DDL script executes against a pre-20260573 database that does not have
    the column yet — SQLite raises, and the add-on cannot boot. That is not
    hypothetical; it is the failure dev56 learned the hard way (see the matching
    NOTE beside the events DDL in database.py). The same argument applies to
    ``idx_events_verdict_pin`` (20260818) and to the two indexes 20260902 adds.

    Consequence: the index can only ever come from a migration, so 20260573
    creates it and every LAST migration since re-adds it belt-and-braces — a
    documented convention, not copy-paste. A database stamped at a version AFTER
    20260573 is built by the current schema script (which omits the index) and
    never walks back through 20260573, so without the re-add on the tail
    migration it would end the walk without the index and every claim lookup
    would table-scan ``events`` on add-on hardware.
    ``test_migrations_forward.py`` asserts the index after a walk that passes
    through 20260573.

    Plain, NOT unique: the live path writes events through a wide upsert, and a
    constraint violation there would abort event storage entirely. The
    check-first SELECT in ``_wf_already_claimed`` is the enforcement.

    NOTE for the schema-drift check: this index is present in a migration-built
    database and deliberately ABSENT from the schema DDL, so a drift test must
    carry an allowlist entry for ``idx_events_wf_claim`` (likewise
    ``idx_events_verdict_pin``, ``idx_events_circuit_cluster`` and
    ``idx_events_fixture``). Grep for ``_ensure_wf_claim_index``.

    Guarded on every indexed column: other migration tests exercise this chain
    against stub ``events`` tables carrying only the columns their own step
    needs, and an index over a missing column aborts the whole run. Idempotent
    (``IF NOT EXISTS``) and safe to call from any migration body.
    """
    if not _has_table(conn, "events"):
        return
    if not all(_has_column(conn, "events", c)
               for c in _WF_CLAIM_INDEX_COLUMNS):
        return
    conn.execute(_WF_CLAIM_INDEX_DDL)
    conn.commit()


_VERDICT_PIN_COLUMNS: tuple = (
    ("verdict_pin", "TEXT"),
    ("verdict_pin_veff", "REAL"),
    ("verdict_pin_set_at", "TEXT"),
)


def _ensure_verdict_pin_columns(conn: sqlite3.Connection) -> None:
    """Add the dev56 pin columns (+ their index) when absent. Idempotent.

    Called from the TOP of BOTH 20260817 and 20260818, and the ordering is
    load-bearing. 20260817 replays ``cleanup_all_overlaps`` over all history,
    and that resolver WRITES the pin — but the columns nominally arrive one
    migration later, so overlap_guard used to carry a parallel pre-pin code path
    (a column sniff, a narrower SELECT list and a duplicated UPDATE) purely to
    survive that one window. Creating the columns here closes the window: by the
    time any overlap code runs, the shape is the current shape, everywhere.

    ⛔ The obvious-looking alternative — SWAPPING 20260817 and 20260818 so the
    columns simply land first — is a BOOT-BREAKER, and must never be done.
    ``_run_migrations_impl`` selects ``[fn for v, fn in _MIGRATIONS if v >
    version]`` and then stamps ``_CURRENT_VERSION`` unconditionally. A database
    stamped exactly 20260817 is a REAL state (dev55 shipped as its own commits,
    ahead of dev56). After a swap that database would run only the re-sweep,
    never receive these three ALTERs, and still be stamped current — then fail
    every subsequent boot on the current-version guard with "Delete the database
    file." Shipped migration numbers do not move; an idempotent ensure-helper
    called from both steps reaches the same end state with no renumbering.

    Guarded for stub ``events`` tables (older migration tests build one with
    only the columns their own step needs), and the index is created only once
    ``circuit`` exists — same guard 20260802-04 use.
    """
    if not _has_table(conn, "events"):
        return
    _add_columns(conn, "events", _VERDICT_PIN_COLUMNS)
    if _has_column(conn, "events", "circuit"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_verdict_pin "
                     "ON events (circuit, verdict_pin)")
    conn.commit()


# ---------------------------------------------------------------------------
# Best-effort backfill failures — loud, and on the record.
# ---------------------------------------------------------------------------
# Several one-shot DATA repairs (20260570/72, 20260802/03/04) are deliberately
# best-effort: a backfill must never keep the add-on from booting. What was
# NOT deliberate is that they used to fail at log.warning and vanish — the
# chain still stamps _CURRENT_VERSION afterwards, and the `_missing_*`
# verifiers only check SCHEMA, so a DB reporting itself fully current could be
# one where the repair never ran and never will. These failures are now logged
# at ERROR with the migration id and recorded here, so "did the repair run?"
# is an answerable question instead of a guess.
#
# Created on demand (nothing reads it on the happy path, so _create_schema
# deliberately does not mirror it) and never itself allowed to break boot.
_MIGRATION_FAILURES_DDL = (
    "CREATE TABLE IF NOT EXISTS migration_failures ("
    " version INTEGER NOT NULL,"
    " step TEXT NOT NULL,"
    " error TEXT NOT NULL,"
    " first_failed_at TEXT NOT NULL,"
    " last_failed_at TEXT NOT NULL,"
    " failures INTEGER NOT NULL DEFAULT 1,"
    " PRIMARY KEY (version, step))"
)


def _record_migration_failure(
    conn: sqlite3.Connection,
    version: int,
    step: str,
    exc: BaseException,
) -> None:
    """Log a skipped best-effort migration step at ERROR and record it.

    Never raises: it is called from an except handler on the boot path, and a
    failure to record a failure must not escalate into a failure to boot.
    """
    log.error(
        "Migration %d: %s FAILED and was SKIPPED — the data repair it performs "
        "has NOT run and nothing re-runs it automatically (the schema version "
        "is still stamped current). Recorded in migration_failures. %s: %s",
        version, step, type(exc).__name__, exc, exc_info=True,
    )
    try:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn.execute(_MIGRATION_FAILURES_DDL)
        conn.execute(
            "INSERT INTO migration_failures "
            " (version, step, error, first_failed_at, last_failed_at, failures) "
            "VALUES (?, ?, ?, ?, ?, 1) "
            "ON CONFLICT(version, step) DO UPDATE SET "
            " error = excluded.error, last_failed_at = excluded.last_failed_at, "
            " failures = migration_failures.failures + 1",
            (int(version), str(step), f"{type(exc).__name__}: {exc}"[:500],
             now, now),
        )
        conn.commit()
    except Exception as rec_exc:  # pragma: no cover - defensive
        log.error("Migration %d: could not record the failure above: %s",
                  version, rec_exc)


# Required columns in the events table that only exist in the baseline schema.
# Checking multiple columns is more robust — an old DB might have some
# backfilled but not others.
_BASELINE_EVENT_COLUMNS: frozenset = frozenset({
    "signature_source",        # new in squash (never present in pre-squash DBs)
    "esp_waveform_used",       # added in migration 031
    "pressure_signature_json", # added in migration 029
    "waveform_overlap_score",  # added in migration 031
})

# Columns added by the 20260526 degraded-supply migration. Used to verify the
# migration has actually run on a DB claiming version 20260526 (catches the
# case where _schema_version was stamped without the migration applying).
_DEGRADED_EVENT_COLUMNS: frozenset = frozenset({
    "degraded_supply",
    "volume_litres_effective",
    "hourly_volume_applied_bucket",
})


def _missing_degraded_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _DEGRADED_EVENT_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260527 valve-type migration. Verified the same way
# as the degraded-supply columns — catches a DB whose _schema_version was
# stamped without the migration body running.
_VALVE_TYPE_COLUMNS: frozenset = frozenset({"valve_type"})


def _missing_valve_type_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _VALVE_TYPE_COLUMNS
        if not _has_column(conn, "circuit_profile", col)
    }


# Columns added by the 20260528 orphan-repair migration. Same verification
# pattern as above — catches a DB whose _schema_version was stamped without
# the migration body running.
_ORPHAN_REPAIR_COLUMNS: frozenset = frozenset({"cluster_backfill_needed"})


def _missing_orphan_repair_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _ORPHAN_REPAIR_COLUMNS
        if not _has_column(conn, "fixtures", col)
    }


# Columns added by the 20260529 suggestion-source migration (Sprint B).
_SUGGESTION_SOURCE_COLUMNS: frozenset = frozenset({"suggestion_source"})


def _missing_suggestion_source_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _SUGGESTION_SOURCE_COLUMNS
        if not _has_column(conn, "fixture_clusters", col)
    }


# Columns added by the 20260530 signature-matcher migration (Sprint C).
_SIGNATURE_MATCHER_COLUMNS: frozenset = frozenset({"matched_fixture_type"})


def _missing_signature_matcher_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _SIGNATURE_MATCHER_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Column added by the 20260532 phantom-guard migration (Sprint E) on events.
_PHANTOM_COLUMNS: frozenset = frozenset({"is_pressure_restoration_phantom"})


def _missing_phantom_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _PHANTOM_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Table added by the 20260533 category-publish migration (Sprint F).
# A "missing column" here is actually a missing TABLE check — the verifier
# treats the table's absence as a single missing-column-equivalent entry.
# Columns added by the 20260534 manual-classification migration (Sprint H).
_MANUAL_CLASSIFICATION_COLUMNS: frozenset = frozenset({"user_ignored", "user_classified"})


def _missing_manual_classification_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _MANUAL_CLASSIFICATION_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Column added by the 20260535 low-flow-dribble migration on events.
_LOW_FLOW_DRIBBLE_COLUMNS: frozenset = frozenset({"is_low_flow_dribble"})


def _missing_low_flow_dribble_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _LOW_FLOW_DRIBBLE_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260536 active-flow migration on events.
_ACTIVE_FLOW_COLUMNS: frozenset = frozenset(c for c, _ in _ACTIVE_FLOW_NEW_COLUMNS)


def _missing_active_flow_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _ACTIVE_FLOW_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260537 cycle-pulse migration on events.
_CYCLE_PULSE_COLUMNS: frozenset = frozenset(c for c, _ in _CYCLE_PULSE_NEW_COLUMNS)


def _missing_cycle_pulse_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _CYCLE_PULSE_COLUMNS
        if not _has_column(conn, "events", col)
    }


# Columns added by the 20260538 label-provenance migration on events.
_LABEL_SOURCE_COLUMNS: frozenset = frozenset(c for c, _ in _LABEL_SOURCE_NEW_COLUMNS)


def _missing_label_source_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        col for col in _LABEL_SOURCE_COLUMNS
        if not _has_column(conn, "events", col)
    }


def _missing_training_capture_table(conn: sqlite3.Connection) -> set[str]:
    """Return any of the 20260539 training-capture tables that are absent."""
    needed = {"training_capture", "training_capture_candidates"}
    present = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('training_capture','training_capture_candidates')"
        ).fetchall()
    }
    return needed - present


def _missing_cross_talk_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260540 cross-talk columns that are absent (events + home_profile)."""
    missing: set[str] = set()
    if not _has_column(conn, "events", "is_cross_talk"):
        missing.add("events.is_cross_talk")
    if not _has_column(conn, "home_profile", "hide_cross_talk_events"):
        missing.add("home_profile.hide_cross_talk_events")
    return missing


def _missing_matched_via_column(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260541 provenance column if absent from events."""
    if not _has_column(conn, "events", "matched_via"):
        return {"events.matched_via"}
    return set()


def _missing_dev24_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260542 dev.24 columns that are absent (home_profile + events).

    Spans TWO tables — without the events check a DB missing only
    ``events.cycle_group_id`` would pass and the rollup would fail at runtime.
    """
    missing: set[str] = set()
    for col in ("has_water_softener", "softener_regen_start", "softener_circuit"):
        if not _has_column(conn, "home_profile", col):
            missing.add(f"home_profile.{col}")
    if not _has_column(conn, "events", "cycle_group_id"):
        missing.add("events.cycle_group_id")
    return missing


def _missing_anomaly_response_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260543 anomaly-response columns absent from sensitivity_config."""
    return {
        f"sensitivity_config.{col}"
        for col in ("anomaly_response", "baseline_anomaly_n")
        if not _has_column(conn, "sensitivity_config", col)
    }


def _missing_recorder_reconcile_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260544 recorder-reconcile columns absent (events + sensitivity_config)."""
    missing: set[str] = set()
    if not _has_column(conn, "events", "volume_recorder_litres"):
        missing.add("events.volume_recorder_litres")
    if not _has_column(conn, "sensitivity_config", "recorder_reconcile_auto"):
        missing.add("sensitivity_config.recorder_reconcile_auto")
    return missing


def _missing_dev38_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260545 dev.38 auto-split flag if absent from home_profile."""
    if not _has_column(conn, "home_profile", "auto_split_enabled"):
        return {"home_profile.auto_split_enabled"}
    return set()


def _missing_ppl_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260546 pulses_per_litre column if absent from circuit_profile."""
    if not _has_column(conn, "circuit_profile", "pulses_per_litre"):
        return {"circuit_profile.pulses_per_litre"}
    return set()


def _missing_rbac_tables(conn: sqlite3.Connection) -> set[str]:
    """Return any of the 20260547 RBAC tables that are absent."""
    needed = {"operator_users", "admin_ids_cache", "seen_users"}
    present = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('operator_users','admin_ids_cache','seen_users')"
        ).fetchall()
    }
    return needed - present


def _missing_embedded_fixtures_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260548 embedded_fixtures_json column if absent from events."""
    if not _has_column(conn, "events", "embedded_fixtures_json"):
        return {"events.embedded_fixtures_json"}
    return set()


def _missing_cross_talk_audit_table(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260550 cross_talk_audit table if absent."""
    present = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cross_talk_audit'"
    ).fetchone()
    return set() if present else {"cross_talk_audit"}


def _missing_edge_signature_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260557 edge-signature columns absent from events."""
    return {
        f"events.{col}"
        for col in ("onset_signature_json", "offset_signature_json")
        if not _has_column(conn, "events", col)
    }


def _missing_flow_pressure_corr_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260554 rise-phantom columns absent (events + home_profile)."""
    missing: set[str] = set()
    if not _has_column(conn, "events", "flow_pressure_corr"):
        missing.add("events.flow_pressure_corr")
    if not _has_column(conn, "home_profile", "rise_corr_backfill_done"):
        missing.add("home_profile.rise_corr_backfill_done")
    return missing


def _missing_epa_flush_cap_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260555 epa_flush_cap_enabled column if absent."""
    if not _has_column(conn, "home_profile", "epa_flush_cap_enabled"):
        return {"home_profile.epa_flush_cap_enabled"}
    return set()


# 20260558 (dev21) — pump-aware detection Phase 1 columns. Single source for
# the apply fn AND the verifier so the two can never drift apart.
_PUMP_MODE_HOME_COLUMNS: tuple = (
    ("pump_mode_detected",    "INTEGER NOT NULL DEFAULT 0"),
    ("pump_mode_detected_at", "TEXT"),
    ("pump_detect_period_s",  "REAL"),
    ("pump_mode_ack",         "TEXT"),
    ("pump_profile",          "TEXT"),
    ("supply_type_set_at",    "TEXT"),
    ("pump_alert_armed_at",   "TEXT"),
)
_PUMP_MODE_SENS_COLUMNS: tuple = (
    ("pump_mode",              "TEXT NOT NULL DEFAULT 'auto'"),
    ("low_pressure_alert_psi", "REAL NOT NULL DEFAULT 25.0"),
)


def _apply_pump_mode_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260558 — dev21 pump-aware Phase 1.

    Home may be pressurized by a booster/well pump (2026-07-19 ESYBOX incident:
    recharge cycling violated every static-supply pressure assumption). Adds the
    profile/ack/provenance plumbing; detection + gating land in later phases.
    DDL only, guarded + idempotent; stub DBs just get the version stamp.
    NOTE: supply_type_set_at stays NULL for every migrated row by design — a
    pre-feature supply answer must never read as post-feature consent.
    """
    for table, cols in (("home_profile", _PUMP_MODE_HOME_COLUMNS),
                        ("sensitivity_config", _PUMP_MODE_SENS_COLUMNS)):
        _add_columns(conn, table, cols, if_table_exists=True)
    conn.commit()
    log.info("Migration 20260558: pump-mode columns ready (profile/ack/"
             "provenance plumbing; detection ships separately)")


# 20260559 (dev26) — Phase 5b cross-circuit leak-test verdict columns.
_LEAK_TEST_PUMP_COLUMNS: tuple = (
    ("other_circuit_cycles",   "INTEGER"),
    ("other_circuit_period_s", "REAL"),
    ("pump_verdict",           "TEXT"),
)


def _apply_leak_test_pump_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260559 — dev26 pump plan Phase 5b.
    Guarded + idempotent; stub DBs just get the version stamp."""
    _add_columns(conn, "leak_test_history", _LEAK_TEST_PUMP_COLUMNS,
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260559: leak-test pump-verdict columns ready")


def _missing_leak_test_pump_columns(conn: sqlite3.Connection) -> set[str]:
    return {f"leak_test_history.{col}"
            for col, _ddl in _LEAK_TEST_PUMP_COLUMNS
            if not _has_column(conn, "leak_test_history", col)}


# 20260562 (dev30) — dismissible failed leak tests.
def _apply_leak_test_dismissed_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260562. Guarded + idempotent."""
    _add_columns(conn, "leak_test_history",
                 (("user_dismissed", "INTEGER DEFAULT 0"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260562: leak-test dismissed flag ready")


def _missing_leak_test_dismissed_column(conn: sqlite3.Connection) -> set[str]:
    if not _has_column(conn, "leak_test_history", "user_dismissed"):
        return {"leak_test_history.user_dismissed"}
    return set()


# 20260563 — leak test measures the right interval, and reports a rate.
# baseline_psi was read BEFORE the valve closed, so every stored row carried
# the close transient plus the whole settle-phase loss (measured 2026-07-26:
# a row read 1.0 PSI where the monitored decay was 0.28, and another read
# 21.5 where it was ~3). Firmware 3.13.2 publishes the values the test
# actually judged against; these columns store them, plus the derived leak
# rate and the demand verdict.
_LEAK_TEST_MEASUREMENT_COLUMNS: tuple = (
    ("closed_psi",            "REAL"),     # pressure the instant the valve sealed
    ("settle_loss_psi",       "REAL"),     # closed_psi - baseline_psi
    ("monitor_minutes",       "REAL"),     # monitored window only
    ("threshold_psi",         "REAL"),     # leak_pressure_threshold at test time
    ("est_leak_ml_min",       "REAL"),     # decay rate x per-circuit compliance
    ("post_restore_volume_l", "REAL"),     # reopen slug
    ("draw_verdict",          "TEXT"),     # demand | clean | unavailable
)


def _apply_leak_test_measurement_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260563. Guarded + idempotent; DDL only.
    Historical rows keep their inflated pressures — no backfill is possible,
    the firmware never published what they were measured against."""
    _add_columns(conn, "leak_test_history", _LEAK_TEST_MEASUREMENT_COLUMNS,
                 if_table_exists=True)
    # Per-circuit compliance (mL per PSI of the isolated section) — converts
    # the decay rate into a leak rate. Seeded from the reopen refill; NULL
    # means "not yet calibrated" and the leak rate is simply not shown.
    _add_columns(conn, "sensitivity_config",
                 (("compliance_ml_psi", "REAL"),), if_table_exists=True)
    conn.commit()
    log.info("Migration 20260563: leak-test measurement columns ready")


def _missing_leak_test_measurement_columns(conn: sqlite3.Connection) -> set[str]:
    missing = {f"leak_test_history.{col}"
               for col, _ddl in _LEAK_TEST_MEASUREMENT_COLUMNS
               if not _has_column(conn, "leak_test_history", col)}
    if not _has_column(conn, "sensitivity_config", "compliance_ml_psi"):
        missing.add("sensitivity_config.compliance_ml_psi")
    return missing


# 20260561 (dev28) — one-shot overlap cleanup.
def _apply_overlap_cleanup(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260561 — resolve historical
    same-circuit event overlaps (idempotent: already-zeroed wrappers no-op
    and audit rows are INSERT OR IGNORE). Guarded for stub DBs.

    Calls ``_ensure_verdict_pin_columns`` first for the same reason 20260817
    does: this is the EARLIEST step that replays ``cleanup_all_overlaps``, and
    that resolver writes the dev56 pin. A database stamped below 20260561
    predates those columns entirely, so without the hoist the sweep would fail
    on `no such column: verdict_pin` in the middle of the chain. What it writes
    here is exactly what 20260818's TAG backfill would have written later, and
    that backfill is a no-op on rows already pinned."""
    _ensure_verdict_pin_columns(conn)
    has_events = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'"
    ).fetchone()
    has_audit = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND "
        "name='overlap_audit'").fetchone()
    if not (has_events and has_audit):
        conn.commit()
        return
    from .overlap_guard import cleanup_all_overlaps
    totals = cleanup_all_overlaps(conn, source="cleanup_migration")
    log.info("Migration 20260561: overlap cleanup done (%d group(s), "
             "%.1f L recovered)", totals["groups"],
             totals["litres_recovered"])


def _missing_overlap_audit_table(conn: sqlite3.Connection) -> set[str]:
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='overlap_audit'").fetchone():
        return {"overlap_audit (table)"}
    return set()


# 20260560 (dev27) — Phase 6b pump-failure alert columns.
def _apply_pump_low_pressure_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260560 — dev27 pump plan Phase 6b.
    Guarded + idempotent; stub DBs just get the version stamp."""
    _add_columns(conn, "sensitivity_config",
                 (("pump_low_pressure_alert_psi", "REAL"),),
                 if_table_exists=True)
    _add_columns(conn, "pump_regime_nightly", (("min_psi", "REAL"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260560: pump low-pressure alert columns ready")


def _missing_pump_low_pressure_columns(conn: sqlite3.Connection) -> set[str]:
    missing: set[str] = set()
    if not _has_column(conn, "sensitivity_config",
                       "pump_low_pressure_alert_psi"):
        missing.add("sensitivity_config.pump_low_pressure_alert_psi")
    if not _has_column(conn, "pump_regime_nightly", "min_psi"):
        missing.add("pump_regime_nightly.min_psi")
    return missing


# 20260564 — supply-pressure regime tracking tables.
def _apply_supply_regime_tables(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260564 — supply-pressure regime
    tracking (daily settled-pressure medians + discrete regime intervals).
    Table-create only; guarded + idempotent."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS supply_pressure_daily (
            circuit       TEXT NOT NULL,
            day_date      TEXT NOT NULL,
            sample_count  INTEGER NOT NULL,
            median_psi    REAL NOT NULL,
            p10_psi       REAL,
            p90_psi       REAL,
            source        TEXT NOT NULL DEFAULT 'settled',
            updated_at    TIMESTAMP,
            PRIMARY KEY (circuit, day_date)
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS supply_regime (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at    TEXT NOT NULL,
            ended_at      TEXT,
            center_psi    REAL NOT NULL,
            band_lo_psi   REAL,
            band_hi_psi   REAL,
            source        TEXT NOT NULL,
            detected_at   TEXT,
            confirmed_at  TEXT,
            dismissed_at  TEXT,
            note          TEXT
        )""")
    conn.commit()
    log.info("Migration 20260564: supply-pressure regime tables ready")


def _missing_supply_regime_tables(conn: sqlite3.Connection) -> set[str]:
    missing: set[str] = set()
    for tbl in ("supply_pressure_daily", "supply_regime"):
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                            "AND name=?", (tbl,)).fetchone():
            missing.add(tbl)
    return missing


# 20260566 — pinned pump-era anchor.
def _apply_pump_era_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260566 — `home_profile.pump_era_start`,
    the PINNED timestamp from which this home has run a booster pump.
    Retroactive pump-era sweeps resolve it once and read the stored value
    thereafter, so re-bootstrapping/merging supply regimes can never move a
    boundary that gates historical verdicts. DDL only; resolution happens
    lazily in supply_regime.pump_era_start. Guarded + idempotent."""
    _add_columns(conn, "home_profile", (("pump_era_start", "TEXT"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260566: pump_era_start column ready")


def _missing_pump_era_columns(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260566 shape. Table-guarded like its migration."""
    if (_has_table(conn, "home_profile")
            and not _has_column(conn, "home_profile", "pump_era_start")):
        return {"home_profile.pump_era_start"}
    return set()


# 20260567 — leak-watch tile dismissal.
def _apply_leak_watch_ack_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260567 — `home_profile.leak_watch_ack`,
    holding 'dismissed:<night_date>' for the newest night the user has
    acknowledged on the leak-watch tile. Dismissal is per-READING, not per-
    feature: a later night carrying a fresh estimate re-shows the tile, which
    is what keeps a real leak from being silenced by one click. DDL only;
    guarded + idempotent."""
    _add_columns(conn, "home_profile", (("leak_watch_ack", "TEXT"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260567: leak_watch_ack column ready")


def _missing_leak_watch_columns(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260567 shape. Table-guarded like its migration."""
    if (_has_table(conn, "home_profile")
            and not _has_column(conn, "home_profile", "leak_watch_ack")):
        return {"home_profile.leak_watch_ack"}
    return set()


# 20260568 — persisted cluster feature mode (pump-era re-seed).
def _apply_cluster_features_mode(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260568 —
    `training_state.cluster_features_mode`, the feature space this circuit's
    cluster centers live in ('full' default, 'pressure_blind' after the
    pump-era re-seed). DDL only; guarded + idempotent."""
    _add_columns(conn, "training_state",
                 (("cluster_features_mode", "TEXT DEFAULT 'full'"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260568: cluster_features_mode column ready")


def _missing_cluster_mode_columns(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260568 shape. Table-guarded like its migration."""
    if (_has_table(conn, "training_state")
            and not _has_column(conn, "training_state",
                                "cluster_features_mode")):
        return {"training_state.cluster_features_mode"}
    return set()


# 20260569 — baseline snapshots (dev34 B3).
def _apply_baseline_snapshot_table(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260569 — `baseline_snapshot`, the
    pre-freeze copies of the usage baseline + anomaly percentiles that make a
    regime refit revertable. Table-create only; the schema DDL is the source
    of truth. Guarded + idempotent."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS baseline_snapshot (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            circuit          TEXT NOT NULL,
            reason           TEXT,
            params           TEXT NOT NULL DEFAULT '{}',
            source           TEXT,
            locked_at        TIMESTAMP,
            sensitivity_json TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
    conn.commit()
    log.info("Migration 20260569: baseline_snapshot table ready")


# 20260570 — leak-test reopen refill provenance.
def _apply_leak_test_refill_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260570 — ``events.leak_test_id`` plus a
    one-time backfill of the reopen-refill verdict over ALL stored leak tests.

    The backfill runs the same idempotent reconcile the scheduler calls live, so
    historical refills get the same verdict as future ones (on the production
    export that is one ~0.04 L event per test night). Best-effort: a backfill
    failure must never block boot — the periodic reconcile retries it.
    """
    _add_columns(conn, "events", (("leak_test_id", "INTEGER"),))
    conn.commit()
    try:
        from .leak_test_refill import reconcile_leak_test_refills
        res = reconcile_leak_test_refills(conn, lookback_days=0, since=None)
        log.info("Migration 20260570: leak_test_id ready; backfill tagged "
                 "%d refill event(s) over %d test(s)",
                 res.get("tagged", 0), res.get("tests_scanned", 0))
    except Exception as e:
        # Loud + recorded: the periodic reconcile is what retries this, so a
        # silent skip here is indistinguishable from "there was nothing to tag".
        _record_migration_failure(conn, 20260570, "leak-test refill backfill", e)


def _missing_leak_test_refill_columns(conn: sqlite3.Connection) -> set[str]:
    if not _has_column(conn, "events", "leak_test_id"):
        return {"events.leak_test_id"}
    return set()


# 20260571 — one day boundary.
def _apply_local_day_boundary(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260571 — the columns behind the unified
    day boundary and the meter-reset carry-over.

    ``volume_snapshots.last_reading`` — high-water mark per period, so a meter
    reset carries the period's volume over instead of zeroing it.
    ``home_profile.daily_summary_tz`` — which timezone the stored daily_summary
    rows are bucketed in. NULL means "UTC-bucketed, pre-migration": the
    orchestrator rebuilds them once the HA timezone is known (it isn't here —
    migrations run before HA is reachable), then stamps this.

    DDL only; both are additive, idempotent, and skipped when the table is
    absent (the partial-schema fixtures the forward-walk tests build).
    """
    if _add_columns(conn, "volume_snapshots", (("last_reading", "REAL"),),
                    if_table_exists=True):
        # Seed the high-water mark at the baseline: a reset before the first
        # live read then carries 0 L, which is the old behaviour and never
        # invents water. Every subsequent read raises it to the true maximum.
        conn.execute("UPDATE volume_snapshots SET last_reading = ha_volume "
                     "WHERE last_reading IS NULL")
    _add_columns(conn, "home_profile", (("daily_summary_tz", "TEXT"),),
                 if_table_exists=True)
    conn.commit()
    log.info("Migration 20260571: local-day boundary columns ready "
             "(daily_summary rebuild deferred to tz detection)")


# 20260572 — sawtooth pump-recharge backfill.
def _apply_sawtooth_recharge_backfill(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260572 — one-shot re-verdict of stored
    pump-era events under the widened (sawtooth micro-cycle) prong of the
    pump-recharge detector. No DDL; data-only, idempotent, and best-effort —
    a backfill failure must never block boot (the degraded-reprocess sweep
    re-derives on its next pass)."""
    try:
        from .feature_extractor import backfill_sawtooth_pump_recharge
        res = backfill_sawtooth_pump_recharge(conn)
        log.info("Migration 20260572: sawtooth recharge backfill tagged "
                 "%d event(s)", res.get("tagged", 0))
    except Exception as e:
        _record_migration_failure(
            conn, 20260572, "sawtooth pump-recharge re-verdict backfill", e)


# 20260574 — leak-watch window bounds.
def _apply_regime_window_bounds(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260574 — ``pump_regime_nightly``
    window_start_ts / window_end_ts (UTC ISO bounds of the analyzed quiet
    window). Additive, idempotent, skipped when the table is absent (the
    partial-schema fixtures the forward-walk tests build)."""
    _add_columns(conn, "pump_regime_nightly",
                 (("window_start_ts", "TEXT"), ("window_end_ts", "TEXT")),
                 if_table_exists=True)
    _ensure_wf_claim_index(conn)
    conn.commit()
    log.info("Migration 20260574: regime window-bound columns ready")


def _missing_local_day_columns(conn: sqlite3.Connection) -> set[str]:
    missing = set()
    if (_has_table(conn, "volume_snapshots")
            and not _has_column(conn, "volume_snapshots", "last_reading")):
        missing.add("volume_snapshots.last_reading")
    if (_has_table(conn, "home_profile")
            and not _has_column(conn, "home_profile", "daily_summary_tz")):
        missing.add("home_profile.daily_summary_tz")
    return missing


# 20260801 — dev38 audit-fix columns (all DDL for the release in one step).
_202608_EVENT_COLUMNS = (
    ("time_features_tz", "TEXT"),
    ("registration_est_litres", "REAL"),
)
_202608_WAVEFORM_COLUMNS = (
    ("flow_src_n", "INTEGER"),
    ("press_src_n", "INTEGER"),
    ("flow_src_hz", "REAL"),
    ("press_src_hz", "REAL"),
)
_202608_LEAK_TEST_COLUMNS = (
    ("baseline_read_ts", "TEXT"),
    ("final_read_ts", "TEXT"),
    ("final_window_s", "REAL"),
    ("sustained_drop_psi", "REAL"),
    ("monitor_started_at", "TEXT"),
)


def _apply_dev38_audit_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260801 — every dev38 column in one DDL
    step (see the changelog block for the per-fix rationale):

      events.time_features_tz        — deferred-tz marker for the local-time
                                       feature backfill (20260571 pattern:
                                       migrations run before HA is reachable,
                                       so the rewrite happens at boot once
                                       the home zone is known)
      events.registration_est_litres — annotate-only meter-registration
                                       estimate; never feeds totals
      event_waveforms.*_src_n/_hz    — per-channel source metadata for an
                                       honest waveform time axis
      overlap_audit.stale_reason     — dangling-reference marking (rows are
                                       provenance and are never deleted)
      leak_test_history.*            — measurement provenance + sustained drop
      daily_summary_dirty            — days needing a summary recompute

    Additive, idempotent, tables-absent-safe (forward-walk fixtures)."""
    _add_columns(conn, "events", _202608_EVENT_COLUMNS, if_table_exists=True)
    _add_columns(conn, "event_waveforms", _202608_WAVEFORM_COLUMNS,
                 if_table_exists=True)
    _add_columns(conn, "leak_test_history", _202608_LEAK_TEST_COLUMNS,
                 if_table_exists=True)
    _add_columns(conn, "overlap_audit", (("stale_reason", "TEXT"),),
                 if_table_exists=True)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS daily_summary_dirty ("
        "circuit TEXT NOT NULL, day TEXT NOT NULL, "
        "PRIMARY KEY (circuit, day))")
    conn.commit()
    log.info("Migration 20260801: dev38 audit-fix columns ready")


# 20260802 — true_avg > peak consistency backfill.
def _apply_peak_consistency_backfill(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260802 — raise ``peak_flow_lpm`` to
    ``ceil(true_avg*1000)/1000`` wherever ``true_avg_flow_lpm`` exceeds it
    (physically impossible; 825 rows in the 2026-08 audit, all software-
    sourced — avg/peak come from ``flow_readings`` while true_avg comes from
    the timestamped ``flow_samples``). Matches the dev37 repair convention
    (ceil, never round down; never lower true_avg). Data-only, idempotent,
    best-effort — a failure must never block boot (the live write path now
    clamps at extract time, so the population cannot regrow)."""
    try:
        rows = conn.execute(
            "SELECT id, true_avg_flow_lpm FROM events "
            "WHERE true_avg_flow_lpm IS NOT NULL AND peak_flow_lpm IS NOT NULL "
            "AND true_avg_flow_lpm > peak_flow_lpm").fetchall()
        for r in rows:
            # Index access — the migration conn's row factory is not guaranteed.
            new_peak = math.ceil(float(r[1]) * 1000.0) / 1000.0
            conn.execute("UPDATE events SET peak_flow_lpm = ? WHERE id = ?",
                         (new_peak, r[0]))
        conn.commit()
        log.info("Migration 20260802: peak-consistency backfill raised "
                 "%d row(s)", len(rows))
    except Exception as e:
        # NOTE: nothing re-runs this one. The live write path stops the
        # population regrowing, but rows already carrying true_avg > peak stay
        # impossible until someone acts on this record.
        _record_migration_failure(
            conn, 20260802, "peak-consistency (true_avg > peak) backfill", e)


# 20260803 — stale hydraulic_resistance backfill.
def _apply_resistance_backfill(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260803 — recompute the stored
    ``hydraulic_resistance`` on ESP-enriched rows from the CURRENT ΔP.

    Pinned definition (identical to extract_features / the dev38 finalize
    recompute): ΔP / avg_flow_lpm, gated on avg >= 0.15 AND
    has_pressure_transient AND ΔP > 0. The 2026-08 audit found 1,324 rows
    whose ratio still reflected the pre-enrichment ΔP (resistance was
    computed before the ESP metadata overwrote pressure_delta_psi).

    Ordering vs the +240 s shared-capture sweep is safe by construction:
    rows the sweep has not yet de-enriched still carry the ESP ΔP, so
    ΔP/avg is CONSISTENT for them; the sweep then NULLs both ΔP and
    resistance on its losers. Data-only, idempotent, best-effort."""
    try:
        cur = conn.execute(
            "UPDATE events SET hydraulic_resistance = "
            "ROUND(pressure_delta_psi / avg_flow_lpm, 3) "
            "WHERE esp_waveform_used = 1 AND avg_flow_lpm >= 0.15 "
            "AND COALESCE(has_pressure_transient, 0) != 0 "
            "AND pressure_delta_psi > 0")
        conn.commit()
        log.info("Migration 20260803: resistance backfill updated %d row(s)",
                 cur.rowcount)
    except Exception as e:
        # Nothing re-runs this one either: the stale ΔP-derived resistance
        # stays on those rows and keeps feeding the classifier.
        _record_migration_failure(
            conn, 20260803, "hydraulic-resistance recompute backfill", e)


# 20260804 — retro-fix the dev37-repaired rows' contaminated signatures.
def _apply_misattached_signature_null(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260804 — NULL the signatures of rows
    the dev37 sweep marked ``misattached`` but left carrying esp-labelled
    signature bytes (31 rows in the 2026-08 audit).

    VERIFIED PROVENANCE: when an ESP capture is claimed, the flow/pressure/
    edge signatures are regenerated FROM the capture arrays
    (feature_extractor._enrich_from_waveform), so a mis-attached row's
    signature is a FOREIGN draw's shape. No HA-derived signature was ever
    persisted for these rows, and the dev37 sweep already deleted their
    envelopes — so the signatures are NULLed and signature_source is set
    NULL (NOT 'software', which would launder contaminated shape data under
    a trusted label). Data-only, idempotent."""
    try:
        cur = conn.execute(
            "UPDATE events SET flow_signature_json = NULL, "
            "pressure_signature_json = NULL, onset_signature_json = NULL, "
            "offset_signature_json = NULL, signature_source = NULL "
            "WHERE wf_repair_verdict = 'misattached' "
            "AND signature_source LIKE 'esp%'")
        conn.commit()
        log.info("Migration 20260804: nulled contaminated signatures on "
                 "%d misattached row(s)", cur.rowcount)
    except Exception as e:
        # Nothing re-runs this one: on failure, mis-attached rows keep a
        # FOREIGN draw's signature bytes under an 'esp' provenance label.
        _record_migration_failure(
            conn, 20260804, "mis-attached signature retro-fix", e)
    _ensure_wf_claim_index(conn)


# 20260805 — dev40 training quarantine for the over-firing dishwasher-cycle tier.
def _apply_training_quarantine(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260805 — annotate-don't-modify quarantine
    of machine dishwasher-cycle labels from every training/exemplar pool.

    The 2026-08-15 precision readout measured the dishwasher_cycle tier at
    9/19 on pre-outage user reviews and 1/10 post-reseed (faucet bursts chained
    into fake fill sequences), and the contaminated labels had already widened
    the fitted DW band 3.75 → 8.32 LPM across three calibration fits — a
    self-reinforcing loop. Until the grouping gate is fixed, unreviewed events
    carrying a machine dishwasher label (either matched_via='dishwasher_cycle'
    or a cycle-stamped user_fixture_type) are excluded from training pools via
    ``training_quarantine_reason`` — labels, verdicts and volumes untouched, so
    History display and the user's ability to review/relabel are unchanged, and
    a later review lifts the quarantine's effect by supplying real ground truth.

    Windows (UTC bounds of the audited Denver-local windows): the pre-outage
    calibration-source window [2026-07-01, 2026-07-22) and the post-reseed
    window [2026-08-13, open) — the 07-22..08-13 outage window is deliberately
    NOT flagged here: it is sequenced for re-attribution off the reseeded
    cluster model first. Idempotent (only NULL-reason rows are stamped)."""
    from datetime import datetime, timezone
    _add_columns(conn, "events", (("training_quarantine_reason", "TEXT"),
                                  ("training_quarantined_at",  "TEXT")))
    # The backfill reads columns older migrations add (a DB walking forward
    # from a mid-ladder version gains them earlier in the same run, but a
    # test-stripped schema may lack them) — without any of them no row can
    # carry a machine dishwasher-cycle label, so there is nothing to flag.
    if all(_has_column(conn, "events", c) for c in
           ("matched_via", "fixture_label_source", "user_reviewed",
            "user_fixture_type")):
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "UPDATE events SET training_quarantine_reason = 'dev40_precision_quarantine', "
            "       training_quarantined_at = ? "
            "WHERE training_quarantine_reason IS NULL "
            "  AND COALESCE(user_reviewed, 0) = 0 "
            "  AND (matched_via = 'dishwasher_cycle' "
            "       OR (fixture_label_source = 'cycle' "
            "           AND user_fixture_type = 'dishwasher')) "
            "  AND ((start_ts >= '2026-07-01T06:00' AND start_ts < '2026-07-22T06:00') "
            "       OR start_ts >= '2026-08-13T06:00')",
            (now,))
        log.info("Migration 20260805: training-quarantined %d unreviewed "
                 "dishwasher-cycle row(s)", cur.rowcount)
    else:
        log.info("Migration 20260805: label/provenance columns absent — "
                 "quarantine backfill skipped (nothing to flag)")
    conn.commit()
    _ensure_wf_claim_index(conn)


# 20260806 — dev41 quarantine sweep: ALL remaining unreviewed machine
# dishwasher labels, no time bounds.
def _apply_training_quarantine_sweep(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260806 — sweep the remaining unreviewed
    machine dishwasher-cycle labels into the training quarantine, with NO
    start_ts bounds.

    20260805 flagged two audited windows and deliberately exempted the
    07-22..08-13 outage mid-window, reasoning it was sequenced for
    re-attribution first — but re-attribution touches cluster ids, never
    labels, so the exemption protected nothing while the mid-window rows kept
    feeding every training pool (the post-quarantine refit still fit
    DW_MAX_PK ≈ 8.59 from them). Planning review also surfaced ~48 more
    unreviewed machine-DW rows predating 07-01, minted by the same over-firing
    gate under the older band. Rather than a third hand-written window, this
    sweep drops the window arithmetic entirely: every unreviewed machine
    dishwasher label still unflagged is quarantined.

    Distinct reason string ('dev40_precision_quarantine_sweep') gives real
    per-migration provenance; readers only test IS NULL, and the relabel lift
    (database.py set_user_fixture_type) clears the column unconditionally, so
    flagged rows lift identically regardless of reason. Idempotent (only
    NULL-reason rows are stamped); labels, verdicts and volumes untouched."""
    from datetime import datetime, timezone
    _add_columns(conn, "events", (("training_quarantine_reason", "TEXT"),
                                  ("training_quarantined_at",  "TEXT")))
    if all(_has_column(conn, "events", c) for c in
           ("matched_via", "fixture_label_source", "user_reviewed",
            "user_fixture_type")):
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "UPDATE events SET "
            "       training_quarantine_reason = 'dev40_precision_quarantine_sweep', "
            "       training_quarantined_at = ? "
            "WHERE training_quarantine_reason IS NULL "
            "  AND COALESCE(user_reviewed, 0) = 0 "
            "  AND (matched_via = 'dishwasher_cycle' "
            "       OR (fixture_label_source = 'cycle' "
            "           AND user_fixture_type = 'dishwasher'))",
            (now,))
        log.info("Migration 20260806: swept %d remaining unreviewed "
                 "dishwasher-cycle row(s) into the training quarantine",
                 cur.rowcount)
    else:
        log.info("Migration 20260806: label/provenance columns absent — "
                 "quarantine sweep skipped (nothing to flag)")
    conn.commit()
    _ensure_wf_claim_index(conn)


# 20260807 — dev41 conformance-review DDL (all in one step, dev38 pattern).
_DEV41_EVENT_COLUMNS = (
    ("other_valve_open_set_at", "TEXT"),
    ("other_valve_open_source", "TEXT"),
    ("registration_curve_version", "INTEGER"),
)
_DEV41_LEAK_TEST_COLUMNS = (
    ("sustainedness_psi", "REAL"),
    ("head_window_s", "REAL"),
    ("monitor_sample_count", "INTEGER"),
    ("sighting_latency_s", "REAL"),
    ("addon_measure_status", "TEXT"),
    ("addon_measure_reason", "TEXT"),
    ("other_valve_state", "TEXT"),
    ("measured_noise_psi", "REAL"),
    ("monitor_samples_json", "TEXT"),
)
# v1 registration curve — seeded from the audit's pressure-witness inversion
# (flow_integral historically carried these as the _REGISTRATION_RATIO code
# constants; dev41 moves them into data with provenance). Relative to the
# meter's own >=8 L/min band; 'unvalidated' until a low-flow anchor lands.
_DEV41_CURVE_V1 = (
    (8.0, None, 0.999),
    (4.0, 8.0, 0.941),
    (2.5, 4.0, 0.904),
    (1.5, 2.5, 0.732),
    (1.0, 1.5, 0.59),
)


def _apply_dev41_conformance_ddl(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260807 — dev41 conformance-review DDL:

      events.other_valve_open_set_at/_source — tri-state provenance (D6)
      events.registration_curve_version      — estimate provenance (E1)
      leak_test_history.*                    — addon-side measurement quality:
                                               sustainedness (shape), sample
                                               counts, sighting latency,
                                               indeterminate status+reason,
                                               noise floor, raw samples (B1-B4)
      overlap_audit.stale_at                 — when the stale mark landed (D5)
      utility_register_readings              — manual register pairs (item 7)
      meter_anchor_points                    — bucket/timed-fill anchors (D1)
      registration_curve                     — versioned correction curve,
                                               v1 seeded 'unvalidated' from
                                               the audit inversion (E1)

    Additive, idempotent, tables-absent-safe."""
    from datetime import datetime, timezone
    _add_columns(conn, "events", _DEV41_EVENT_COLUMNS, if_table_exists=True)
    _add_columns(conn, "leak_test_history", _DEV41_LEAK_TEST_COLUMNS,
                 if_table_exists=True)
    _add_columns(conn, "overlap_audit", (("stale_at", "TEXT"),),
                 if_table_exists=True)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS utility_register_readings ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "reading_value REAL NOT NULL, reading_ts TEXT NOT NULL, "
        "meter_serial TEXT, source TEXT, entered_by TEXT, notes TEXT, "
        "created_at TEXT)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS meter_anchor_points ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, circuit TEXT, "
        "flow_rate_lpm REAL, measured_volume_l REAL, "
        "reference_volume_l REAL, test_date TEXT, method TEXT, notes TEXT, "
        "created_at TEXT)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS registration_curve ("
        "curve_version INTEGER NOT NULL, band_lo_lpm REAL NOT NULL, "
        "band_hi_lpm REAL, ratio REAL NOT NULL, status TEXT NOT NULL, "
        "source TEXT, created_at TEXT, "
        "PRIMARY KEY (curve_version, band_lo_lpm))")
    # Seed curve v1 (idempotent via the PK).
    now = datetime.now(timezone.utc).isoformat()
    for lo, hi, ratio in _DEV41_CURVE_V1:
        conn.execute(
            "INSERT OR IGNORE INTO registration_curve "
            "(curve_version, band_lo_lpm, band_hi_lpm, ratio, status, "
            " source, created_at) VALUES (1, ?, ?, ?, 'unvalidated', "
            " 'audit_2026-08_pressure_witness_inversion', ?)",
            (lo, hi, ratio, now))
    conn.commit()
    _ensure_wf_claim_index(conn)
    log.info("Migration 20260807: dev41 conformance-review DDL ready")


# 20260808 — dev42: reseed completion marker (F-C2).
def _apply_reseed_marker_column(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260808 —
    ``training_state.reseed_in_progress``: ISO timestamp stamped when a
    cluster re-seed clears assignments, cleared only on success. A crash
    mid-replay leaves it set (the 2026-08-15 11:56 reseed crash stranded a
    part-cleared model with no persistent trace); boot and the post-rebuild
    health pass warn loudly until a rerun succeeds. Additive, idempotent."""
    _add_columns(conn, "training_state", (("reseed_in_progress", "TEXT"),),
                 if_table_exists=True)
    conn.commit()
    _ensure_wf_claim_index(conn)
    log.info("Migration 20260808: reseed-in-progress marker column ready")


# 20260809 — dev46: training-exclusion flag (46f), per-channel signature
# spans (46i), and the winterized-circuit flag (46h).
def _apply_dev46_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260809 — three additive flags.

    ``events.training_excluded_by_user`` (46f) — "keep my label, but don't
    train on this event". Four confirmed cases exist where a user label is
    TRUE while the event's features describe a composite draw; before this,
    the only way to keep such an event out of training was to lie about its
    label. Deliberately DISTINCT from training_quarantine_reason and NOT
    lifted by review — review is what SETS it.

    ``events.flow_sig_span_s`` / ``pressure_sig_span_s`` (46i) — the real
    captured span of each signature channel, so the event modal can draw an
    honest per-channel time axis. Forward-only: legacy rows stay NULL and
    keep the proportional overlay, because their spans are unknowable
    (annotate-don't-modify).

    ``circuit_profile.winterized`` (46h) — the circuit is deliberately
    drained for winter, so ~0 psi is EXPECTED rather than a catastrophic
    pressure event. Consumers pause detection, regime sampling, baselines
    and leak-test scheduling for the circuit while set.

    All additive and idempotent."""
    _add_columns(conn, "events",
                 (("training_excluded_by_user", "INTEGER DEFAULT 0"),
                  ("flow_sig_span_s",           "REAL"),
                  ("pressure_sig_span_s",       "REAL")),
                 if_table_exists=True)
    _add_columns(conn, "circuit_profile",
                 (("winterized", "INTEGER DEFAULT 0"),), if_table_exists=True)
    conn.commit()
    _ensure_wf_claim_index(conn)
    log.info("Migration 20260809: training-exclusion flag, signature spans "
             "and winterized flag ready")


# 20260810 — dev46 (46k): per-event verdict validity stamp.
# Must match the trigger's UPDATE OF list below — the guard checks these exist
# before creating it, and a mismatch would create a trigger that never fires.
_VERDICT_STAMP_WATCHED = (
    "cycle_pulse_count", "excluded_from_training", "volume_litres",
    "volume_litres_effective", "duration_seconds", "peak_flow_lpm",
    "active_flow_segment_count", "has_pressure_transient",
    "flow_signature_json", "pressure_signature_json",
    "is_pressure_restoration_phantom", "is_cross_talk",
    "is_low_flow_dribble", "user_ignored", "training_excluded_by_user",
)


def _apply_verdict_stamp(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260810 — make the boot reclassify skippable.

    ``events.verdict_stamp`` records WHICH inputs produced the row's stored
    verdict. The boot pass then scans only events whose stamp differs from the
    current one, instead of re-deriving every unlabelled event every boot.

    Why this is needed at all: the pass already stores its answer in
    matched_fixture_type, but stored nothing that said whether that answer was
    still valid — so it recomputed ~5,400 verdicts to discover that ~5,400 of
    them were unchanged. Measured 2026-08-17: 151.7 s per boot, growing by
    roughly 45-60 events/day as the unlabelled backlog grows, with no ceiling.

    NULL means "never stamped" and therefore always a candidate, so the
    migration needs no backfill: the first pass after upgrade stamps every row
    and behaves exactly as today.

    ``training_state.last_full_reclassify_at`` is the max-age backstop. If an
    input is ever left out of the stamp, staleness would otherwise be
    invisible; forcing a full pass when the last one is old bounds that to
    days rather than forever.

    Both additive and idempotent."""
    _add_columns(conn, "events", (("verdict_stamp", "TEXT"),),
                 if_table_exists=True)
    _add_columns(conn, "training_state",
                 (("last_full_reclassify_at", "TIMESTAMP"),),
                 if_table_exists=True)
    # The pass's candidate query filters circuit + user_fixture_type +
    # verdict_stamp; without this it degrades to a full scan of every event
    # on every boot, which is the cost this migration exists to remove.
    if (_has_table(conn, "events")
            and all(_has_column(conn, "events", c) for c in
                    ("circuit", "user_fixture_type", "verdict_stamp"))):
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_verdict_stamp "
            "ON events (circuit, user_fixture_type, verdict_stamp)")
    _ensure_wf_claim_index(conn)
    conn.commit()
    # Recreated (not IF NOT EXISTS alone) so that changing the watched-column
    # list in a later release replaces the old trigger instead of silently
    # keeping a stale one.
    # Every watched column must exist or the CREATE fails. A partial schema
    # (only synthetic test DBs in practice) simply gets no trigger — and the
    # runtime refuses to skip when the trigger is absent, so the optimisation
    # cannot engage without the mechanism that keeps it honest.
    if _has_table(conn, "events") and all(
            _has_column(conn, "events", c) for c in _VERDICT_STAMP_WATCHED):
        conn.execute("DROP TRIGGER IF EXISTS trg_events_verdict_stamp_invalidate")
        conn.executescript("""
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
""")
    conn.commit()
    log.info("Migration 20260810: per-event verdict stamp ready")


def _missing_202608_columns(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260801 DDL (current-version guard set)."""
    missing = set()
    if _has_table(conn, "events"):
        for col, _ in _202608_EVENT_COLUMNS:
            if not _has_column(conn, "events", col):
                missing.add(f"events.{col}")
    if _has_table(conn, "event_waveforms"):
        for col, _ in _202608_WAVEFORM_COLUMNS:
            if not _has_column(conn, "event_waveforms", col):
                missing.add(f"event_waveforms.{col}")
    if _has_table(conn, "leak_test_history"):
        for col, _ in _202608_LEAK_TEST_COLUMNS:
            if not _has_column(conn, "leak_test_history", col):
                missing.add(f"leak_test_history.{col}")
    if (_has_table(conn, "overlap_audit")
            and not _has_column(conn, "overlap_audit", "stale_reason")):
        missing.add("overlap_audit.stale_reason")
    if not _has_table(conn, "daily_summary_dirty"):
        missing.add("daily_summary_dirty")
    return missing


# 20260565 — rule_calibration keyed per (circuit, supply regime).
def _apply_regime_calibration(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260565 — rebuild rule_calibration with
    PRIMARY KEY (circuit, regime_id). The existing per-circuit row is copied
    as regime_id=0 (the legacy/pre-regime row, still the fallback when a
    regime has no fit of its own). Guarded + idempotent.

    ATOMICITY: SQLite's own DDL is transactional, but Python 3.11's sqlite3
    opens an implicit transaction only ahead of DML — a bare CREATE / DROP /
    ALTER autocommits one statement at a time, and the trailing conn.commit()
    below buys nothing. Before this was fixed, a crash mid-rebuild left the DB
    in a state no re-run could repair:

      * crash after the CREATE — ``rule_calibration_new`` already exists on the
        next boot, the unguarded CREATE raises, and the migration chain fails
        forever (the version is never stamped, so every boot retries and dies);
      * crash after the DROP — ``rule_calibration`` is GONE, the table-exists
        early-out below then declares success, the chain stamps itself current,
        and the user's calibration is permanently destroyed.

    The rebuild now runs inside an explicit ``BEGIN IMMEDIATE`` (the pattern
    _apply_referee_meta_columns uses in this same file), so it is all-or-nothing
    even against a hard kill. The two branches ahead of it repair a DB already
    left in either state by the old code.
    """
    if not _has_table(conn, "rule_calibration"):
        if (_has_table(conn, "rule_calibration_new")
                and _has_column(conn, "rule_calibration_new", "regime_id")):
            # Interrupted between DROP and RENAME by the pre-fix code: every
            # row survives in the scratch table, so finish the rename rather
            # than early-out and let the chain stamp over the loss.
            conn.execute(
                "ALTER TABLE rule_calibration_new RENAME TO rule_calibration")
            conn.commit()
            log.warning("Migration 20260565: recovered an interrupted "
                        "rule_calibration rebuild — the scratch table held the "
                        "rows and has been renamed into place")
            return
        conn.commit()
        return
    if _has_column(conn, "rule_calibration", "regime_id"):
        # Already migrated. Clear any scratch table an interrupted earlier
        # attempt left behind — otherwise a later re-run of the CREATE below
        # (or a hand repair) trips over it.
        if _has_table(conn, "rule_calibration_new"):
            conn.execute("DROP TABLE rule_calibration_new")
        conn.commit()
        return
    conn.commit()  # nothing pending — BEGIN IMMEDIATE must be the outer txn
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DROP TABLE IF EXISTS rule_calibration_new")
        conn.execute("""
            CREATE TABLE rule_calibration_new (
                circuit     TEXT NOT NULL,
                regime_id   INTEGER NOT NULL DEFAULT 0,
                params      TEXT NOT NULL DEFAULT '{}',
                report      TEXT,
                source      TEXT,
                locked_at   TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (circuit, regime_id)
            )""")
        conn.execute(
            "INSERT INTO rule_calibration_new "
            " (circuit, regime_id, params, report, source, locked_at, updated_at) "
            "SELECT circuit, 0, params, report, source, locked_at, updated_at "
            "FROM rule_calibration")
        conn.execute("DROP TABLE rule_calibration")
        conn.execute(
            "ALTER TABLE rule_calibration_new RENAME TO rule_calibration")
        conn.commit()
    except BaseException:
        # Includes KeyboardInterrupt: an operator Ctrl-C mid-rebuild must not
        # be the one crash window that leaves the table half-swapped.
        conn.rollback()
        raise
    log.info("Migration 20260565: rule_calibration keyed per (circuit, regime)")


def _missing_regime_calibration_columns(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260565 shape (current-version guard set).

    Table-guarded because the migration is: _apply_regime_calibration returns
    early when rule_calibration is absent, so demanding the column on a DB that
    has no such table would fail a boot the migration itself was happy with.
    """
    if (_has_table(conn, "rule_calibration")
            and not _has_column(conn, "rule_calibration", "regime_id")):
        return {"rule_calibration.regime_id"}
    return set()


def _missing_pump_mode_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the 20260558 pump-mode columns absent (home_profile +
    sensitivity_config)."""
    missing: set[str] = set()
    for table, cols in (("home_profile", _PUMP_MODE_HOME_COLUMNS),
                        ("sensitivity_config", _PUMP_MODE_SENS_COLUMNS)):
        for col, _ddl in cols:
            if not _has_column(conn, table, col):
                missing.add(f"{table}.{col}")
    return missing


# 20260573 — waveform claim ledger + mis-attachment repair audit.
_WF_CLAIM_COLUMNS: tuple = (
    # Completes the firmware-capture identity. The ESP event counter restarts
    # at every reboot, so event_id alone cannot identify a capture; boot_id is
    # a per-boot random_uint32() from the firmware. Together they are the claim
    # key enforcing one-capture-one-event (see _wf_already_claimed).
    ("waveform_boot_id", "INTEGER"),
    # Audit trail for the one-shot repair sweep (wf_repair_backfill), following
    # the volume_litres_original / volume_recomputed_at precedent: the corrupted
    # values are preserved, never silently overwritten.
    ("peak_flow_lpm_pre_repair", "REAL"),
    ("pressure_delta_psi_pre_repair", "REAL"),
    ("propagation_delay_ms_pre_repair", "REAL"),
    ("wf_repair_at", "TEXT"),
    # 'misattached' (capture provably belonged to another draw) or 'floor_only'
    # (cross-sensor disagreement; peak floored, provenance kept). The verdict
    # distribution is the empirical check on the mis-attachment diagnosis.
    ("wf_repair_verdict", "TEXT"),
)


def _apply_wf_claim_and_repair_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to version 20260573 — the waveform claim ledger and
    the repair-audit columns.

    DDL only, deliberately: the repair itself needs the event corpus and the
    cluster engine, which this module must not import (see the 20260535 note).
    ``wf_repair_backfill`` runs it as a supervised one-shot worker after boot.

    Background: ``_enrich_from_waveform`` overwrote peak_flow_lpm /
    pressure_delta_psi / propagation_delay_ms from whichever buffered capture
    best matched on DURATION, with nothing stopping two same-length draws from
    both claiming the same capture. On the 2026-08-09 production export that
    left 110 events with true_avg_flow_lpm > peak_flow_lpm — impossible for a
    single draw, since an average cannot exceed its own maximum.
    """
    if not _has_table(conn, "events"):
        return
    _add_columns(conn, "events", _WF_CLAIM_COLUMNS)
    _ensure_wf_claim_index(conn)
    conn.commit()
    log.info("Migration 20260573: waveform claim ledger + repair audit columns ready")


def _missing_wf_claim_columns(conn: sqlite3.Connection) -> set[str]:
    if not _has_table(conn, "events"):
        return set()
    return {
        f"events.{col}" for col, _ddl in _WF_CLAIM_COLUMNS
        if not _has_column(conn, "events", col)
    }


def _missing_baseline_columns(conn: sqlite3.Connection) -> set[str]:
    """Return the set of required baseline columns absent from the events table."""
    return {
        col for col in _BASELINE_EVENT_COLUMNS
        if not _has_column(conn, "events", col)
    }


def _log_schema_state(conn: sqlite3.Connection) -> None:
    """Emit a single INFO line summarising the current schema.

    Plan C-IQ-15 / C-IQ-22 (lightweight variant). Walks `sqlite_master`
    for user tables and reports each table's column count alongside
    the stamped schema version. A divergent DB (e.g. a partially
    restored backup, or a hand-edited database) will be loud in the
    logs without forcing a hard-fail boot abort — which the plan
    downgraded over dev-time false-alarm risk.

    Format chosen so the line is greppable but compact:
        Schema v=20260527  tables: events(56), fixtures(11), ...

    Best-effort: any SQL error here is swallowed so a deeply broken DB
    doesn't keep the addon from starting in the diagnose-and-restore
    path. The migration verification block above is the real guard.
    """
    try:
        version = _get_version(conn)
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' "
            "  AND name NOT LIKE 'sqlite_%' "
            "  AND name NOT LIKE '_schema_version' "
            "ORDER BY name"
        ).fetchall()
        parts = []
        for r in rows:
            tbl = r[0]
            try:
                cols = conn.execute(
                    f"PRAGMA table_info({tbl})"
                ).fetchall()
                parts.append(f"{tbl}({len(cols)})")
            except sqlite3.OperationalError:
                # Table dropped between SELECT and PRAGMA — rare.
                parts.append(f"{tbl}(?)")
        log.info(
            "Schema v=%d  tables: %s",
            version, ", ".join(parts) or "(none)",
        )
    except Exception as exc:
        # Schema diagnostic must never fail the boot. Log the error
        # itself at INFO so a developer running locally can spot it.
        log.info("Schema diagnostic failed (non-fatal): %s", exc)


# ── Ordered forward-migration chain ────────────────────────────────────────────
# (introduced_in_version, apply_fn): a DB stamped at version V already HAS every
# step with introduced <= V and needs exactly the steps with introduced > V,
# applied in this order. Every apply fn is idempotent, so the whole dispatch is
# one loop — adding a migration is ONE line here (plus bumping _CURRENT_VERSION),
# not an edit to ~28 hand-maintained per-version branches (the old ladder, where
# one missed branch stamped a DB current while silently missing a table).
# 20260811 — dev47 (47i): fixture health baselines, stats and alerts.
def _apply_fixture_health(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260811 — three tables for fixture health.

    dev47 separates two things the label schema conflates: WHICH fixture a
    draw came from, and whether that fixture is healthy. Classification stays
    adaptive (a leaking toilet is still a toilet, and the model may absorb its
    new shape); health is measured downstream against a reference that does
    NOT adapt. These tables hold that reference and its evidence.

    ``fixture_baseline`` — one frozen row per (circuit, fixture_type),
    pinned over an explicit window. ``locked`` defaults to 1 and only an
    explicit unlock with a reason code clears it, because a baseline that
    could drift with the data would detect nothing: a degrading flapper would
    simply redefine normal, which is exactly the failure this exists to catch.

    ``fixture_health_stat`` — append-only nightly observations. The series is
    the evidence a health card is built from; rewriting it would make an alarm
    unexplainable afterwards. UNIQUE on (circuit, fixture_type, as_of_day) so
    a re-run of the nightly job updates that day rather than duplicating it.

    ``fixture_health_alert`` — open/resolved alerts. The retrain reads the
    open set for holdout hygiene; it is NOT the detector.

    All three are additive and idempotent."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS fixture_baseline (
               circuit         TEXT NOT NULL,
               fixture_type    TEXT NOT NULL,
               baseline_hash   TEXT,
               pinned_at       TEXT,
               window_start    TEXT,
               window_end      TEXT,
               n_events        INTEGER,
               stats_json      TEXT,
               locked          INTEGER NOT NULL DEFAULT 1,
               unlocked_reason TEXT,
               unlocked_at     TEXT,
               PRIMARY KEY (circuit, fixture_type)
           )""")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS fixture_health_stat (
               id           INTEGER PRIMARY KEY AUTOINCREMENT,
               circuit      TEXT NOT NULL,
               fixture_type TEXT NOT NULL,
               as_of_day    TEXT NOT NULL,
               stats_json   TEXT,
               UNIQUE (circuit, fixture_type, as_of_day)
           )""")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS fixture_health_alert (
               id           INTEGER PRIMARY KEY AUTOINCREMENT,
               circuit      TEXT NOT NULL,
               fixture_type TEXT NOT NULL,
               signal       TEXT NOT NULL,
               opened_at    TEXT NOT NULL,
               resolved_at  TEXT,
               resolution   TEXT,
               detail_json  TEXT
           )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fixture_health_alert_open "
        "ON fixture_health_alert (circuit, fixture_type, resolved_at)")
    # (idx_fixture_health_stat_day was created here; removed with migration
    #  20260902, which also DROPs it — it duplicated the table's own
    #  UNIQUE (circuit, fixture_type, as_of_day) index column for column.
    #  Removing it from database.py alone would have been a silent no-op.)
    _ensure_wf_claim_index(conn)
    conn.commit()
    log.info("Migration 20260811: fixture health baselines ready")


# 20260812 — dev48: flow_plateau_lpm, the rate a draw runs at once running.
def _apply_flow_plateau(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260812 — one column, backfilled from waveforms.

    Neither stored flow number answers "how fast does this fixture actually
    run": the average is diluted by ramp and off-time, the peak is one sample.
    The plateau is the median of the flowing samples, so it describes the valve
    and the supply pressure rather than the length of the draw.

    Backfilled here rather than left to accumulate because the model can only
    learn from it where it exists, and the history is where the labels are. Only
    events with a stored waveform get a value; the rest stay NULL, which the
    model reads as missing rather than as zero. On the reference home that is
    about 60% coverage, and the feature bought +2.3 accuracy points (excluding
    'other') on exactly that subset.

    Additive and idempotent: the column is added only if absent, and the
    backfill only ever fills rows that are still NULL, so re-running cannot
    overwrite a value the live path has since computed."""
    import json

    from .feature_extractor import flow_plateau_lpm

    _add_columns(conn, "events", (("flow_plateau_lpm", "REAL"),))

    # A database old enough to be migrating from far back may not have reached
    # the waveform table yet — the forward-walk test migrates from every prior
    # version, and joining a table that does not exist there is fatal. Nothing
    # to backfill from is a normal state, not an error: the live path fills the
    # column going forward either way.
    if not _has_table(conn, "event_waveforms"):
        conn.commit()
        log.info("Migration 20260812: flow_plateau_lpm ready (no waveform "
                 "table yet — nothing to backfill)")
        return

    rows = conn.execute(
        "SELECT e.id, w.flow_max_json FROM events e "
        "  JOIN event_waveforms w ON w.event_id = e.id "
        " WHERE e.flow_plateau_lpm IS NULL AND w.flow_max_json IS NOT NULL"
    ).fetchall()
    filled = 0
    for row in rows:
        try:
            series = json.loads(row[1] or "[]")
        except (TypeError, ValueError):
            continue
        plateau = flow_plateau_lpm(series)
        if plateau is None:
            continue
        conn.execute("UPDATE events SET flow_plateau_lpm = ? WHERE id = ?",
                     (plateau, row[0]))
        filled += 1
    conn.commit()
    log.info("Migration 20260812: flow_plateau_lpm ready (%d of %d waveform "
             "row(s) backfilled)", filled, len(rows))

# 20260813 — dev49 (P0-4): mark days whose daily_summary drifted from `events`.
def _apply_daily_summary_drift_markers(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260813 — MARK drifted days, recompute nothing.

    ``mark_daily_summary_dirty`` had exactly one caller repo-wide while ~20
    paths route volume through ``apply_effective_volume``, so every reprocess,
    recompute, merge and overlap resolution left the cached day behind. dev49
    moved the mark into the chokepoint, which stops NEW drift — it does not
    repair the drift already recorded. Measured on the reference home:
    17 of 92 days disagreed with `events` by more than 0.5 L, 468.5 L in total,
    worst single day -93.2 L.

    WHY THIS IS NOT A HISTORICAL VOLUME RECOMPUTE (the standing invariant).
    ``daily_summary`` is a DERIVED CACHE over the event ledger. This migration
    writes nothing but ``daily_summary_dirty`` markers; the recompute is done
    later, by ``drain_daily_summary_dirty`` — already-shipped code with a
    no-lookback contract — and it re-derives each day from events that this
    migration does not touch. No event's ``volume_litres`` or
    ``volume_litres_effective`` changes here or afterwards. The invariant
    governs the ledger, and the ledger is untouched.

    Marking rather than recomputing inline also keeps the migration fast and
    interruptible: a marker left undrained is retried on the next pruner pass.

    Idempotent: INSERT OR IGNORE on the (circuit, day) primary key. Days are
    bucketed with ``local_day_of`` so the comparison matches how the summary
    was written; a day that has since been corrected simply fails the >0.5 L
    test and is not marked.
    """
    if not (_has_table(conn, "daily_summary")
            and _has_table(conn, "daily_summary_dirty")):
        conn.commit()
        log.info("Migration 20260813: no daily_summary yet — nothing to mark")
        return

    from .database import local_day_of

    # Bucket events by local day, the same way compute_daily_summary does.
    totals: dict = {}
    for r in conn.execute(
            "SELECT circuit, start_ts, "
            "       COALESCE(volume_litres_effective, volume_litres, 0) AS v "
            "FROM events WHERE start_ts IS NOT NULL"):
        day = local_day_of(r["start_ts"])
        if not day:
            continue
        key = (r["circuit"], day)
        totals[key] = totals.get(key, 0.0) + float(r["v"] or 0.0)

    marked = 0
    drift_litres = 0.0
    for row in conn.execute(
            "SELECT circuit, day, COALESCE(total_volume_litres, 0) AS t "
            "FROM daily_summary").fetchall():
        key = (row["circuit"], row["day"])
        delta = totals.get(key, 0.0) - float(row["t"] or 0.0)
        if abs(delta) > 0.5:
            conn.execute(
                "INSERT OR IGNORE INTO daily_summary_dirty (circuit, day) "
                "VALUES (?, ?)", (row["circuit"], row["day"]))
            marked += 1
            drift_litres += abs(delta)
    conn.commit()
    log.info("Migration 20260813: marked %d drifted day(s) for recompute "
             "(%.1f L total disagreement); the pruner's summary pass drains "
             "them", marked, drift_litres)


# 20260814 — dev50: auto-split memo + stale marks for the two audit tables whose
# event_id had no foreign key.
def _apply_auto_split_memo(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260814 — additive, idempotent, no backfill.

    Two dev50 changes make reprocess a CONTINUOUS background action rather than a
    rare manual one (the over-merge job now scans the whole HA recorder window
    instead of 24 h), and that turns two latent problems into ongoing ones:

    * ``events.split_evaluated_at`` / ``split_evaluation_outcome`` — the job's
      checked-set lived only in memory, so every restart re-evaluated the whole
      backlog, and post-dev50 each re-evaluation costs an HA history fetch. NULL
      means "never evaluated", so the columns need no backfill and every existing
      event is considered exactly once after this lands.
    * ``anomaly_shutoff_log`` / ``cross_talk_audit`` ``stale_reason`` + ``stale_at``
      — both carry an ``event_id`` with no FK and no cleanup, so each reprocess left
      them pointing at an id that no longer exists. Marked, never deleted, exactly
      as ``overlap_audit`` has been since 20260801: these rows are provenance (a
      shutoff that fired, a cross-talk verdict that was applied).
    """
    _add_columns(conn, "events", (("split_evaluated_at",       "TEXT"),
                                  ("split_evaluation_outcome", "TEXT")),
                 if_table_exists=True)
    for tbl in ("anomaly_shutoff_log", "cross_talk_audit"):
        _add_columns(conn, tbl, (("stale_reason", "TEXT"),
                                 ("stale_at",     "TEXT")),
                     if_table_exists=True)
    conn.commit()
    _ensure_wf_claim_index(conn)
    log.info("Migration 20260814: auto-split memo columns + audit stale marks ready")


# 20260815 — dev51: the model referee's benchmark + decision ledger.
_REFEREE_TABLES_DDL: tuple = (
    ("referee_benchmark",
     "CREATE TABLE IF NOT EXISTS referee_benchmark ("
     "circuit TEXT NOT NULL, event_id TEXT NOT NULL, "
     "source_hash TEXT NOT NULL, imported_at TEXT NOT NULL, "
     "PRIMARY KEY (circuit, event_id))"),
    ("referee_benchmark_meta",
     "CREATE TABLE IF NOT EXISTS referee_benchmark_meta ("
     "circuit TEXT PRIMARY KEY, source_hash TEXT NOT NULL, "
     "requested_n INTEGER NOT NULL, imported_at TEXT NOT NULL)"),
    ("retrain_ledger",
     "CREATE TABLE IF NOT EXISTS retrain_ledger ("
     "id INTEGER PRIMARY KEY, circuit TEXT NOT NULL, decided_at TEXT NOT NULL, "
     "trigger TEXT NOT NULL, status TEXT NOT NULL, swap INTEGER NOT NULL, "
     "challenger_hash TEXT, champion_hash TEXT, reason TEXT, "
     "benchmark_hash TEXT, benchmark_n INTEGER, detail_json TEXT)"),
)


def _apply_referee_tables(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260815 — three new tables, no backfill, idempotent.

    The referee (dev47) was designed with two legs: a pinned frozen benchmark
    as the primary guard and a recent labelled holdout as the secondary. In
    production the benchmark leg never ran — nothing wired the pinned file in —
    and the referee rejected the identical challenger night after night. dev51
    moves the benchmark INTO the database (``referee_benchmark`` + one
    ``referee_benchmark_meta`` row per circuit holding the import's hash and
    requested count), imported once through Dev Tools so the ids never enter
    the repo, and adds ``retrain_ledger`` because the jobs table prunes at two
    days and "a run of rejections" was therefore invisible. Empty tables are
    the correct state on a fresh install: the benchmark leg abstains, and an
    abstaining referee keeps the incumbent.
    """
    for _name, ddl in _REFEREE_TABLES_DDL:
        conn.execute(ddl)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_retrain_ledger_circuit_decided "
        "ON retrain_ledger (circuit, decided_at)")
    conn.commit()
    _ensure_wf_claim_index(conn)
    log.info("Migration 20260815: referee benchmark + retrain ledger tables ready")


def _missing_referee_tables(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260815 DDL (current-version guard set)."""
    return {name for name, _ in _REFEREE_TABLES_DDL if not _has_table(conn, name)}


# 20260816 — dev53: the add-on pins its own benchmark (provenance + pending slot).
_REFEREE_META_COLUMNS: tuple = (
    ("source", "TEXT NOT NULL DEFAULT 'import'"),
    ("pinned_from_n", "INTEGER"),
    ("repin_dismissed_at", "TEXT"),
    ("repin_dismissed_keys", "TEXT"),
    ("pending_hash", "TEXT"),
    ("pending_pinned_at", "TEXT"),
    ("pending_pinned_from_n", "INTEGER"),
    ("pending_trigger", "TEXT"),
    ("pending_reason", "TEXT"),
)


def _apply_referee_meta_columns(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260816 — additive, idempotent, no backfill.

    dev51 stored ONE hand-imported benchmark per circuit. dev53 lets the add-on
    pin its own and, deliberately, re-pin it. Two shape changes:

    * ``referee_benchmark`` gains ``role`` ('active' | 'pending') and its
      primary key becomes (circuit, event_id, role). A re-pin is written as
      pending and only takes over at the next promotion, so the benchmark leg
      is never dark; an event may sit in both sets across a handover. SQLite
      cannot alter a primary key, so this is a table rebuild that copies every
      existing row as 'active'.
    * ``referee_benchmark_meta`` gains provenance (``source``,
      ``pinned_from_n``), the pending slot, and the Water Use prompt's
      dismissal record. Existing import rows read as source='import' with an
      unknown ``pinned_from_n`` — the growth trigger simply cannot fire for
      them, which is correct: nothing knows what label count they were drawn
      from.
    """
    # a 20260814 DB runs 20260815 first, so the tables exist; be defensive anyway
    for _name, ddl in _REFEREE_TABLES_DDL:
        conn.execute(ddl)
    _add_columns(conn, "referee_benchmark_meta", _REFEREE_META_COLUMNS)
    if not _has_column(conn, "referee_benchmark", "role"):
        conn.execute("BEGIN")
        conn.execute(
            "CREATE TABLE referee_benchmark__dev53 ("
            "circuit TEXT NOT NULL, event_id TEXT NOT NULL, "
            "source_hash TEXT NOT NULL, imported_at TEXT NOT NULL, "
            "role TEXT NOT NULL DEFAULT 'active', "
            "PRIMARY KEY (circuit, event_id, role))")
        conn.execute(
            "INSERT INTO referee_benchmark__dev53 "
            "(circuit, event_id, source_hash, imported_at, role) "
            "SELECT circuit, event_id, source_hash, imported_at, 'active' "
            "FROM referee_benchmark")
        conn.execute("DROP TABLE referee_benchmark")
        conn.execute("ALTER TABLE referee_benchmark__dev53 RENAME TO referee_benchmark")
    conn.commit()
    _ensure_wf_claim_index(conn)
    log.info("Migration 20260816: referee benchmark provenance + pending slot ready")


def _missing_referee_meta_columns(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260816 shape (current-version guard set)."""
    missing: set[str] = set()
    if _has_table(conn, "referee_benchmark_meta"):
        missing |= {f"referee_benchmark_meta.{c}" for c, _ in _REFEREE_META_COLUMNS
                    if not _has_column(conn, "referee_benchmark_meta", c)}
    if _has_table(conn, "referee_benchmark") and not _has_column(
            conn, "referee_benchmark", "role"):
        missing.add("referee_benchmark.role")
    return missing


def _apply_overlap_resweep(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260817 — dev55. Re-run the same-circuit overlap
    sweep over all history.

    20260561 (dev28) ran this once. Since then the importer kept writing a long
    reconstructed parent on top of the live children inside it: each child is
    individually >= 3x shorter than the parent, so find_overlapping_event's
    "longer wins over short unlabeled stub" heal waved every one of them through
    one at a time. dev55 refuses those writes going forward; this clears what
    already accumulated. Measured on a 2026-09-05 copy of the live DB: 323
    overlap groups, 249 unresolved, of which 179 carry no user label and hold
    ~265 L of double-counted water — replaying the resolver's own policy
    de-duplicates 165 of them.

    Idempotent by contract: an already-zeroed wrapper is a no-op and audit rows
    are INSERT OR IGNORE. User-labelled wrappers get an audit row only and keep
    every litre. Guarded for stub DBs. No schema change of its OWN — but it does
    call ``_ensure_verdict_pin_columns`` first, because the resolver it replays
    writes the 20260818 pin columns and they must exist before it runs (see that
    helper: the columns are hoisted, the migration numbers are NOT swapped).
    """
    _ensure_verdict_pin_columns(conn)
    has_events = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'"
    ).fetchone()
    has_audit = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND "
        "name='overlap_audit'").fetchone()
    if not (has_events and has_audit):
        conn.commit()
        return
    from .overlap_guard import cleanup_all_overlaps
    totals = cleanup_all_overlaps(conn, source="cleanup_migration_dev55")
    log.info("Migration 20260817: overlap re-sweep done (%d group(s), "
             "%d wrapper(s) de-duplicated, %.1f L recovered, %d user-labelled "
             "flagged only)", totals["groups"], totals["wrappers_zeroed"],
             totals["litres_recovered"], totals["flag_only"])


def _apply_verdict_pin(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260818 — dev56. The PINNED VERDICT.

    Three columns on ``events`` (``verdict_pin``, ``verdict_pin_veff``,
    ``verdict_pin_set_at``) plus an index on (circuit, verdict_pin). Backfill is
    TAG ONLY: every row already carrying ``match_rejection_reason =
    'overlap_duplicate'`` gets the pin with ``verdict_pin_veff`` = its CURRENT
    effective volume — no litre is rewritten (a partial-remainder wrapper keeps its
    remainder). The phantom bit is cleared on those rows: wrappers are not
    phantoms — their water is real, merely counted by another row — and carrying
    the bit put them in the phantom repair's path and under the phantom pill. That
    is a one-time semantic change of the bit; the History surfaces key on the
    reason from dev56 on. Idempotent: guarded ALTERs, an UPDATE whose WHERE is
    empty on a second run.

    The DDL itself lives in ``_ensure_verdict_pin_columns``, which 20260817 also
    calls — the re-sweep replays a resolver that writes these columns. Only the
    TAG backfill below is unique to this step.
    """
    _ensure_verdict_pin_columns(conn)
    if not _has_table(conn, "events"):
        conn.commit()
        return
    if not all(_has_column(conn, "events", c) for c in
               ("match_rejection_reason", "volume_litres_effective",
                "is_pressure_restoration_phantom")):
        conn.commit()
        log.info("Migration 20260818: pinned verdict columns ready (stub schema — "
                 "no backfill)")
        return
    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc).isoformat()
    cur = conn.execute(
        "UPDATE events SET verdict_pin = 'overlap_duplicate', "
        "  verdict_pin_veff = COALESCE(volume_litres_effective, 0), "
        "  verdict_pin_set_at = ?, is_pressure_restoration_phantom = 0 "
        "WHERE match_rejection_reason = 'overlap_duplicate' "
        "  AND verdict_pin IS NULL", (now,))
    # dev56 — the 20260817 re-sweep ran before the direction rule (6322179)
    # existed and raised ten wrappers another verdict had zeroed; the guard's
    # UPDATE never touches is_cross_talk / is_low_flow_dribble, so two rows
    # were left saying "cross-talk" with 0.71 / 0.57 L still counted, and no
    # sweep re-derives cross-talk. Generic rule: a zeroing flag the operator
    # did not set means zero. The other eight rows' original verdicts are
    # unrecoverable (the phantom bit was overwritten); they stay as the
    # guard's own remainder (I-4: over-count-and-flag beats guessing).
    from .database import rezero_rows_with_zeroing_flag
    try:
        repaired = rezero_rows_with_zeroing_flag(conn)
    except sqlite3.Error as e:
        repaired = 0
        log.info("Migration 20260818: flag/volume consistency pass skipped: %s", e)
    conn.commit()
    _ensure_wf_claim_index(conn)
    log.info("Migration 20260818: pinned verdict columns ready (%d overlap "
             "wrapper(s) tagged, no volume rewritten; %d row(s) whose zeroing flag "
             "disagreed with their volume re-zeroed)", cur.rowcount or 0, repaired)


def _missing_verdict_pin_columns(conn: sqlite3.Connection) -> set[str]:
    """Verifier for the 20260818 shape (current-version guard set)."""
    if not _has_table(conn, "events"):
        return set()
    return {f"events.{c}" for c, _ in _VERDICT_PIN_COLUMNS
            if not _has_column(conn, "events", c)}


def _apply_wf_src_hz_correction(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260819 — correct the stored ESP capture rate.

    ``event_waveforms.flow_src_hz`` / ``press_src_hz`` record the fixed sample
    rate of an ESP-sourced series so a renderer can build an honest time axis.
    They were written as 200.0, which is the pressure ADC READ rate, not the
    capture rate: the firmware's waveform_capture interval is 20 ms (~50 Hz),
    stated in its own header. The add-on's matching constant is a function-local
    ``_SAMPLE_MS = 20`` in ``event_waveform.py`` (unit 7.3 moved it there); it
    is NOT importable from ``event_detector`` and never was module-level.

    Every ESP-sourced waveform therefore rendered on a 4x-compressed time axis —
    a 30 s capture drawn as 7.5 s. This rewrites the stored metadata; the sample
    arrays themselves were always correct and are untouched.

    Idempotent: only rows still holding exactly 200.0 are changed, and the write
    is value-scoped rather than blanket, so a genuinely different stored rate
    (none exist today) would survive. No schema change, so _create_schema needs
    no mirroring. Guarded for stub DBs.
    """
    if not _has_table(conn, "event_waveforms"):
        conn.commit()
        return
    cur = conn.execute(
        "UPDATE event_waveforms SET "
        "  flow_src_hz  = CASE WHEN flow_src_hz  = 200.0 THEN 50.0 ELSE flow_src_hz  END, "
        "  press_src_hz = CASE WHEN press_src_hz = 200.0 THEN 50.0 ELSE press_src_hz END "
        "WHERE flow_src_hz = 200.0 OR press_src_hz = 200.0"
    )
    conn.commit()
    log.info("Migration 20260819: corrected ESP capture rate 200 Hz -> 50 Hz "
             "on %d waveform row(s)", cur.rowcount or 0)



def _apply_drop_mqtt_schema(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260901 — remove the MQTT publisher's schema.

    MQTT was part of an original roadmap the operator is no longer pursuing
    (decided 2026-09-07). It had never worked on this install in any case:
    ``config.yaml`` declared no ``services:`` block, so the Supervisor answered
    the broker-credentials query with 403 and the publisher returned at
    ``status=not_configured`` without ever connecting.

    Drops, in order of how sure we are they are unused:

    * ``home_profile.mqtt_publish_enabled`` and
      ``home_profile.publish_fixtures_to_ha`` — zero readers even before the
      code removal; a repo-wide grep found only their DDL lines.
    * ``fixture_ha_entity_map`` — the audit's schema census found it had never
      held a row: a CREATE, one DELETE, and nothing else.
    * ``category_publish`` — backed the per-category "publish to HA" checkbox,
      which controlled only MQTT output.

    ``fixtures.publish_to_ha`` is deliberately LEFT ALONE. Unlike these it is
    written by the live confirm path (``upsert_fixture_from_cluster``) and read
    back at database.py's fixture rollup, so removing it is a change to that
    flow rather than to MQTT.

    Idempotent: DROP ... IF EXISTS, and each column is checked first. Column
    drops are wrapped because ``ALTER TABLE ... DROP COLUMN`` needs SQLite
    3.35+; on anything older the columns are simply left in place, which is
    harmless — nothing reads them. No data is migrated: every object here is
    either empty or write-only.

    NOTE: ``fixture_ha_entity_map`` was also removed from QUICK_RESTORE_TABLES
    (routers/backup.py) and RESTORABLE_TABLES (restore_utils.py) in the same
    commit. Those lists are NOT optional to update: the restore path runs
    ``DELETE FROM {tbl}`` for every name in the quick-restore list with no
    existence check, so a dropped-but-still-listed table makes Quick Restore
    fail outright.
    """
    for table in ("category_publish", "fixture_ha_entity_map"):
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        except sqlite3.Error as e:            # noqa: PERF203 — one per table
            log.warning("Migration 20260901: could not drop %s: %s", table, e)

    dropped = []
    for col in ("mqtt_publish_enabled", "publish_fixtures_to_ha"):
        if not _has_column(conn, "home_profile", col):
            continue
        try:
            conn.execute(f"ALTER TABLE home_profile DROP COLUMN {col}")
            dropped.append(col)
        except sqlite3.Error as e:
            # SQLite < 3.35 has no DROP COLUMN. Leaving the column costs
            # nothing: it has no reader, and _create_schema no longer emits it
            # so fresh installs never gain one.
            log.warning("Migration 20260901: leaving home_profile.%s in place "
                        "(%s)", col, e)

    conn.commit()
    log.info("Migration 20260901: MQTT schema removed "
             "(tables dropped, %d home_profile column(s) dropped: %s)",
             len(dropped), ", ".join(dropped) or "none")


# Objects removed by 20260902. Kept as module constants so the migration and
# its test name the same things.
_V20260902_DROP_TABLES: tuple = ("csrf_tokens", "cluster_sequences")
_V20260902_DROP_INDEXES: tuple = (
    "idx_hourly_volume_circuit_ts",     # == PRIMARY KEY (circuit, hour_ts)
    "idx_daily_summary_circuit_day",    # == PRIMARY KEY (circuit, day)
    "idx_clusters_circuit",             # prefix of PK (circuit, id)
    "idx_type_signatures_circuit",      # prefix of PK (circuit, fixture_type)
    "idx_jobs_id",                      # jobs.id is a rowid alias
    "idx_fixture_health_stat_day",      # == UNIQUE (circuit, fixture_type, as_of_day)
)


def _apply_dead_schema_and_event_indexes(conn: sqlite3.Connection) -> None:
    """Forward migration to 20260902 — drop dead schema, fix events' indexes.

    Three unrelated pieces of work that all need the same one migration.

    (1) DEAD OBJECTS. Two tables and one column with no reader and no writer
    anywhere in the repo:

    * ``csrf_tokens`` — a pre-HMAC leftover. CSRF has been stateless
      double-submit off ``csrf_server_secret`` for a long time; nothing ever
      issued or checked a row here. Its only other mention in the tree is
      ``EXPORT_EXCLUDED_TABLES`` in ``routers/backup.py``, which DELETEs from
      each listed table inside a ``try/except sqlite3.Error: continue`` whose
      own comment reads "table absent in this schema version" — so unlike the
      quick-restore allowlist (which has no existence check and WOULD 500),
      leaving the name there after the drop is exactly the handled case. That
      file is not touched here.
    * ``cluster_sequences`` — a Phase 2.2 placeholder that never gained a
      writer; a repo-wide grep found its CREATE and nothing else.
    * ``events.flow_onset_delay_seconds`` — never in the feature dict the live
      upsert builds its column list from, so dropping it cannot break event
      ingestion, and no query selects it.

    NOT dropped, though the audit proposed them:

    * ``zone_flow_history`` — it IS in ``RESTORABLE_TABLES`` (restore_utils.py)
      and ``HISTORY_ARCHIVE_TABLES`` (routers/backup.py), and data_pruner
      prunes it by ``recorded_at``. Never-populated is not the same as
      unreferenced.
    * ``events.propagation_delay_seconds`` — no reader in the add-on, but
      ``tools/audit/scripts/p4_fields.py`` SELECTs it by name against a copy of
      the live database, so dropping it would break the audit harness.

    (2) REDUNDANT INDEXES (``_V20260902_DROP_INDEXES``). Six non-UNIQUE indexes
    whose key is already indexed, column for column, by the table's own PRIMARY
    KEY / UNIQUE constraint (or, for ``jobs``, by the rowid the INTEGER PRIMARY
    KEY aliases). No ``ON CONFLICT`` target can depend on them — a conflict
    target must be UNIQUE — and every one of them cost a write on tables the
    live path writes to constantly. Two of the six were ALSO created inside
    earlier migrations (20260530, 20260811); those CREATE lines are removed in
    the same commit, because dropping here while a forward walk re-created them
    two steps earlier would have been a silent no-op.

    NOT dropped: ``idx_cross_talk_audit_event``. The audit listed it, but it is
    not redundant — ``cross_talk_audit`` has no other index on ``event_id``, and
    the reprocess delete loop in database.py runs
    ``UPDATE cross_talk_audit SET stale_reason = … WHERE event_id = ?`` once per
    deleted event. Dropping it would have turned that into a table scan per row.

    (3) THE TWO MISSING EVENTS INDEXES.

    * ``idx_events_circuit_cluster (circuit, cluster_id)`` — ``cluster_id`` is
      the join key of the whole clustering layer and had no index at all. Nearly
      every filter site pairs it with ``circuit`` (``WHERE circuit = ? AND
      cluster_id = ?``, ``… AND cluster_id IS NULL``), and cluster ids are only
      unique per circuit, so the composite is the right key rather than a bare
      one.
    * ``idx_events_fixture (fixture_id)`` — ``fixture_id`` is a declared FK to
      ``fixtures(id)`` with no ON DELETE action, so SQLite must check the child
      rows on every fixture delete or merge. Unindexed that was a full scan of
      ``events``; the FK check keys on ``fixture_id`` alone, so this index is
      deliberately NOT circuit-led.

    Both live here rather than in ``_create_schema`` for the reason the
    ``idx_events_wf_claim`` / ``idx_events_verdict_pin`` notes in database.py
    give: that DDL script also runs against upgrade databases, and an index
    statement there executes before any ALTER has run. Fresh installs still get
    them — the version==0 path runs this whole chain.

    Idempotent throughout: ``DROP … IF EXISTS``, ``CREATE INDEX IF NOT EXISTS``,
    and the column drop is checked with ``_has_column`` first and wrapped
    (``ALTER TABLE … DROP COLUMN`` needs SQLite 3.35+; on anything older the
    column is simply left in place, which is harmless now that ``_create_schema``
    no longer emits it). No data is migrated — every object removed here is
    empty or write-never-read.
    """
    for table in _V20260902_DROP_TABLES:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        except sqlite3.Error as e:            # noqa: PERF203 — one per table
            log.warning("Migration 20260902: could not drop table %s: %s",
                        table, e)

    for idx in _V20260902_DROP_INDEXES:
        try:
            conn.execute(f"DROP INDEX IF EXISTS {idx}")
        except sqlite3.Error as e:            # noqa: PERF203 — one per index
            log.warning("Migration 20260902: could not drop index %s: %s",
                        idx, e)

    dropped_col = False
    if _has_column(conn, "events", "flow_onset_delay_seconds"):
        try:
            conn.execute(
                "ALTER TABLE events DROP COLUMN flow_onset_delay_seconds")
            dropped_col = True
        except sqlite3.Error as e:
            log.warning("Migration 20260902: leaving "
                        "events.flow_onset_delay_seconds in place (%s)", e)

    if _has_table(conn, "events"):
        if all(_has_column(conn, "events", c) for c in ("circuit", "cluster_id")):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_circuit_cluster "
                "ON events (circuit, cluster_id)")
        if _has_column(conn, "events", "fixture_id"):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_fixture "
                "ON events (fixture_id)")
        _ensure_wf_claim_index(conn)

    conn.commit()
    log.info("Migration 20260902: dropped %d dead table(s) + %d redundant "
             "index(es)%s; events cluster_id/fixture_id now indexed",
             len(_V20260902_DROP_TABLES), len(_V20260902_DROP_INDEXES),
             " + events.flow_onset_delay_seconds" if dropped_col else "")


_MIGRATIONS: tuple = (
    (20260524, _drop_retired_wf_entity_map_rows),
    (20260525, _apply_unique_events_index),
    (20260526, _apply_degraded_supply_columns),
    (20260527, _apply_valve_type_column),
    (20260528, _apply_orphan_repair),
    (20260529, _apply_suggestion_source_column),
    (20260530, _apply_signature_matcher),
    (20260531, _apply_fixture_taxonomy_consolidation),
    (20260532, _apply_phantom_event_column),
    (20260533, _apply_category_publish_table),
    (20260534, _apply_manual_classification_columns),
    (20260535, _apply_low_flow_dribble_column),
    (20260536, _apply_active_flow_columns),
    (20260537, _apply_cycle_pulse_column),
    (20260538, _apply_label_source_column),
    (20260539, _apply_training_capture_tables),
    (20260540, _apply_cross_talk_columns),
    (20260541, _apply_matched_via_column),
    (20260542, _apply_dev24_columns),
    (20260543, _apply_anomaly_response_columns),
    (20260544, _apply_recorder_reconcile_columns),
    (20260545, _apply_dev38_columns),
    (20260546, _apply_ppl_column),
    (20260547, _apply_rbac_tables),
    (20260548, _apply_embedded_fixtures_column),
    (20260549, _apply_auto_split_default),
    (20260550, _apply_cross_talk_audit_table),
    (20260551, _apply_phantom_suppression_averted),
    (20260552, _apply_fingerprint_labeling_flag),
    (20260553, _apply_review_verdict_column),
    (20260554, _apply_flow_pressure_corr),
    (20260555, _apply_epa_flush_cap_flag),
    (20260556, _apply_sig256_rebuild),
    (20260557, _apply_edge_signatures),
    (20260558, _apply_pump_mode_columns),
    (20260559, _apply_leak_test_pump_columns),
    (20260560, _apply_pump_low_pressure_column),
    (20260561, _apply_overlap_cleanup),
    (20260562, _apply_leak_test_dismissed_column),
    (20260563, _apply_leak_test_measurement_columns),
    (20260564, _apply_supply_regime_tables),
    (20260565, _apply_regime_calibration),
    (20260566, _apply_pump_era_column),
    (20260567, _apply_leak_watch_ack_column),
    (20260568, _apply_cluster_features_mode),
    (20260569, _apply_baseline_snapshot_table),
    (20260570, _apply_leak_test_refill_column),
    (20260571, _apply_local_day_boundary),
    (20260572, _apply_sawtooth_recharge_backfill),
    (20260573, _apply_wf_claim_and_repair_columns),
    (20260574, _apply_regime_window_bounds),
    (20260801, _apply_dev38_audit_columns),
    (20260802, _apply_peak_consistency_backfill),
    (20260803, _apply_resistance_backfill),
    (20260804, _apply_misattached_signature_null),
    (20260805, _apply_training_quarantine),
    (20260806, _apply_training_quarantine_sweep),
    (20260807, _apply_dev41_conformance_ddl),
    (20260808, _apply_reseed_marker_column),
    (20260809, _apply_dev46_columns),
    (20260810, _apply_verdict_stamp),
    (20260811, _apply_fixture_health),
    (20260812, _apply_flow_plateau),
    (20260813, _apply_daily_summary_drift_markers),
    (20260814, _apply_auto_split_memo),
    (20260815, _apply_referee_tables),
    (20260816, _apply_referee_meta_columns),
    (20260817, _apply_overlap_resweep),
    (20260818, _apply_verdict_pin),
    (20260819, _apply_wf_src_hz_correction),
    (20260901, _apply_drop_mqtt_schema),
    (20260902, _apply_dead_schema_and_event_indexes),
)

# Versions a DB may legitimately be stamped with and still be upgradeable.
_UPGRADEABLE_VERSIONS: frozenset = frozenset(
    {_BASELINE_VERSION} | {v for v, _ in _MIGRATIONS})


def _run_migration_step(conn: sqlite3.Connection, version: int, fn) -> None:
    """Run one migration body and NAME IT in the log, before and after.

    Until dev59 the chain logged one summary line ("Database upgraded X → Y,
    N forward step(s)") and nothing else, so 26 of the 48 migrations were
    completely silent: any migration whose body logs nothing left no trace it
    had run. That makes crash resumption unauditable — after a boot that died
    mid-chain there was no way to say from the log which step was running when
    it died, and the chain deliberately re-runs every step it predates on the
    next boot (nothing is stamped until the end), so "did 20260807 already
    run?" could only be answered by inspecting the schema by hand.

    The BEFORE line is the load-bearing one: it is the last thing in the log
    when a step hangs or is killed. The AFTER line separates "crashed INSIDE
    20260807" from "finished 20260807 and crashed on the step after it".
    """
    log.info("Migration %d: running %s", version, fn.__name__)
    _t0 = time.monotonic()
    try:
        fn(conn)
    except BaseException:
        log.error(
            "Migration %d: %s RAISED after %.0f ms — the chain stops here and "
            "the schema version is NOT stamped, so the next boot re-runs from "
            "this step.",
            version, fn.__name__, (time.monotonic() - _t0) * 1000.0,
        )
        raise
    log.info("Migration %d: %s applied in %.0f ms",
             version, fn.__name__, (time.monotonic() - _t0) * 1000.0)


def run_migrations(
    conn: sqlite3.Connection,
    db_path: Optional[Path] = None,
) -> None:
    """
    Enforce baseline schema version. Called once at startup after init_db().

    CRITICAL: tables are already created by database.py before this is called.
    Version 0 is ambiguous — could be fresh DB OR old pre-squash DB without
    _schema_version. Distinguish by checking for ALL required baseline columns:
      - All present  → fresh DB created by current schema → stamp baseline
      - Any absent   → old pre-squash DB → fail fast

    Always emits the schema-state diagnostic line at the end (via the
    `finally` below), even when migration aborts with a RuntimeError —
    that way the supervisor logs show exactly what tables/columns the
    on-disk DB had at the moment things went wrong.
    """
    try:
        _run_migrations_impl(conn, db_path)
    finally:
        _log_schema_state(conn)


def _run_migrations_impl(
    conn: sqlite3.Connection,
    db_path: Optional[Path] = None,
) -> None:
    """Actual migration dispatch. Kept separate so run_migrations can
    log the schema state unconditionally via try/finally."""
    version = _get_version(conn)
    _db_hint = f" DB file: {db_path}" if db_path else ""

    if version == _CURRENT_VERSION:
        missing = (
            _missing_baseline_columns(conn)
            | _missing_degraded_columns(conn)
            | _missing_valve_type_columns(conn)
            | _missing_orphan_repair_columns(conn)
            | _missing_suggestion_source_columns(conn)
            | _missing_signature_matcher_columns(conn)
            | _missing_phantom_columns(conn)
            | _missing_manual_classification_columns(conn)
            | _missing_low_flow_dribble_columns(conn)
            | _missing_active_flow_columns(conn)
            | _missing_cycle_pulse_columns(conn)
            | _missing_label_source_columns(conn)
            | _missing_training_capture_table(conn)
            | _missing_cross_talk_columns(conn)
            | _missing_matched_via_column(conn)
            | _missing_dev24_columns(conn)
            | _missing_anomaly_response_columns(conn)
            | _missing_recorder_reconcile_columns(conn)
            | _missing_dev38_columns(conn)
            | _missing_ppl_columns(conn)
            | _missing_rbac_tables(conn)
            | _missing_embedded_fixtures_columns(conn)
            | _missing_cross_talk_audit_table(conn)
            | _missing_flow_pressure_corr_columns(conn)
            | _missing_epa_flush_cap_columns(conn)
            | _missing_edge_signature_columns(conn)
            | _missing_pump_mode_columns(conn)
            | _missing_leak_test_pump_columns(conn)
            | _missing_pump_low_pressure_columns(conn)
            | _missing_overlap_audit_table(conn)
            | _missing_leak_test_dismissed_column(conn)
            | _missing_leak_test_measurement_columns(conn)
            # 20260564-68 (dev32/33/34). These five verifiers were written with
            # their migrations and then never wired in here, so a DB stamped
            # CURRENT with any of those five bodies un-run passed this guard and
            # only failed later, at the first query against the missing column.
            | _missing_supply_regime_tables(conn)
            | _missing_regime_calibration_columns(conn)
            | _missing_pump_era_columns(conn)
            | _missing_leak_watch_columns(conn)
            | _missing_cluster_mode_columns(conn)
            | _missing_leak_test_refill_columns(conn)
            | _missing_local_day_columns(conn)
            | _missing_wf_claim_columns(conn)
            | _missing_202608_columns(conn)
            | _missing_referee_tables(conn)
            | _missing_referee_meta_columns(conn)
            | _missing_verdict_pin_columns(conn)
        )
        if missing:
            raise RuntimeError(
                "Database claims current schema version but is missing required "
                f"columns: {', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )
        log.debug("Database at schema version %d", _CURRENT_VERSION)
        return

    if version == 0:
        # Distinguish fresh DB from pre-squash DB via baseline columns.
        missing = _missing_baseline_columns(conn)
        if missing:
            raise RuntimeError(
                "Existing pre-squash database detected. Missing baseline columns: "
                f"{', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )
        # Fresh DB created by current _create_schema() — has all current
        # columns including the degraded-supply additions. Stamp at current.
        # Defensively verify the degraded columns are present too.
        missing_deg = _missing_degraded_columns(conn)
        if missing_deg:
            raise RuntimeError(
                "Fresh DB missing expected degraded-supply columns: "
                f"{', '.join(sorted(missing_deg))}. "
                f"Schema definition is out of sync.{_db_hint}"
            )
        # Run the chain anyway (idempotent, near-no-op on an empty DB): a few
        # steps create things _create_schema deliberately omits (partial
        # indexes, one-shot backfills). _apply_unique_events_index is skipped —
        # the fresh schema already ships the unique index and the dedup scan
        # would only rescan an empty table.
        for _v, _fn in _MIGRATIONS:
            if _fn is _apply_unique_events_index:
                log.info("Migration %d: skipped on a fresh DB — the schema "
                         "already ships the unique index", _v)
                continue
            _run_migration_step(conn, _v, _fn)
        _set_version(conn, _CURRENT_VERSION)
        log.info("New database — schema version %d applied", _CURRENT_VERSION)
        return

    if version > _CURRENT_VERSION:
        # A DOWNGRADE, not a corrupt DB: this database was written by a NEWER
        # add-on version. There is no backward chain, so we stop — but we must
        # never tell the user to delete a database that is perfectly intact and
        # simply ahead of us. Re-installing the newer version reads it fine.
        raise RuntimeError(
            f"Database schema version {version} is NEWER than this add-on "
            f"understands ({_CURRENT_VERSION}). The database was written by a "
            f"newer version of the add-on and CANNOT be downgraded. Your data "
            f"is intact — do NOT delete the database. Re-install the newer "
            f"add-on version to use it, or restore a backup taken at schema "
            f"version {_CURRENT_VERSION} or older.{_db_hint}"
        )

    if version not in _UPGRADEABLE_VERSIONS:
        if version < _BASELINE_VERSION:
            # Any version 1–31 (or an unknown pre-baseline stamp): old
            # incremental pre-squash DB, with no forward path from here.
            raise RuntimeError(
                f"Database schema version {version} is a pre-squash version. "
                f"Delete the database file and restart the add-on to create a "
                f"fresh schema. (Expected {_CURRENT_VERSION}, found "
                f"{version}.){_db_hint}"
            )
        # Between the baseline and current, but not a version this add-on ever
        # shipped (a dev build, or a hand-edited stamp). Unknown provenance, so
        # we refuse to guess which steps already ran — but the data is not
        # known-bad, so this is not a delete-the-database situation either.
        raise RuntimeError(
            f"Database schema version {version} is not a version this add-on "
            f"ever shipped (expected {_CURRENT_VERSION}, or one of the known "
            f"upgrade steps). The database was most likely written by a "
            f"development build. Your data is intact — do NOT delete the "
            f"database; restore a backup stamped at a released schema version, "
            f"or re-install the build that wrote it.{_db_hint}"
        )

    if version == _BASELINE_VERSION:
        # Baseline sanity check before the chain runs (the chain itself starts
        # with the drop-retired-waveform-roles step this version predates).
        missing = _missing_baseline_columns(conn)
        if missing:
            raise RuntimeError(
                "Database claims baseline schema version but is missing required "
                f"columns: {', '.join(sorted(missing))}. "
                f"Delete the database file and restart the add-on.{_db_hint}"
            )

    # Ordered chain: apply exactly the steps this version predates (see
    # _MIGRATIONS — every fn is idempotent, so a re-run after a mid-chain
    # crash is safe).
    steps = [(v, fn) for v, fn in _MIGRATIONS if v > version]
    log.info("Database upgrade %d → %d: %d forward step(s) to run — %s",
             version, _CURRENT_VERSION, len(steps),
             ", ".join(str(v) for v, _fn in steps))
    for _v, _fn in steps:
        _run_migration_step(conn, _v, _fn)
    _set_version(conn, _CURRENT_VERSION)
    log.info("Database upgraded %d → %d (%d forward step(s))",
             version, _CURRENT_VERSION, len(steps))
